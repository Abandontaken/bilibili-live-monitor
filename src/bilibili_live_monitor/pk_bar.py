"""只读历史PK消息，按录像时钟重放比分条；不连接生产网页。"""
import bisect
import json
import math
import sqlite3
import subprocess
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from .gift_assets import sha_file, write_json

COMMANDS = ('PK_INFO', 'PK_BATTLE_PRE_NEW', 'PK_BATTLE_START_NEW',
            'PK_BATTLE_PROCESS_NEW', 'PK_BATTLE_END', 'PK_BATTLE_VIDEO_PUNISH_BEGIN',
            'PK_BATTLE_PUNISH_END')


def epoch(value):
    date = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if date.tzinfo is None:
        raise ValueError('录像起点必须含时区')
    return date.timestamp()


def media_info(path):
    result = subprocess.run(['ffprobe', '-v', 'error', '-show_streams', '-show_format',
                             '-of', 'json', str(path)], capture_output=True, text=True,
                            check=True, timeout=30)
    return json.loads(result.stdout)


def bar_ratio(left, right, minimum=16):
    """网页同款可读宽度保护；不是原始票数百分比。"""
    if left == right == 0:
        return .5
    big, small = max(left, right), min(left, right)
    bound = 4 * len(str(small)) + minimum
    for index, threshold in enumerate((10, 100, 1000, 5000)):
        if big < threshold:
            bound += 25 - 5 * index + 5 * (1 - big / threshold)
            break
    ratio = math.floor(left / (left + right) * 100)
    return min(100-bound, max(bound, ratio)) / 100


def make_plan(database, room, pk_id, video, video_start, font, *, connection=None):
    origin = epoch(video_start)
    database, video, font = (Path(p).resolve(strict=True) for p in (database, video, font))
    info = media_info(video)
    videos = [s for s in info['streams'] if s['codec_type'] == 'video']
    if len(videos) != 1 or not any(s['codec_type'] == 'audio' for s in info['streams']):
        raise ValueError('需要单视频轨及音轨')
    duration = float(info['format']['duration'])
    if not 0 < duration <= 7200:
        raise ValueError('支持最多两小时的有限录像')
    if connection is not None and not 0 <= connection[0] < connection[1] <= duration:
        raise ValueError('连线区间须位于录像内')
    # 读时间范围有限且沿用room/time索引；消息中的pk_id再做精确过滤。
    lower = datetime.fromtimestamp(origin-900, timezone.utc).isoformat()
    upper = datetime.fromtimestamp(origin+duration+900, timezone.utc).isoformat()
    with sqlite3.connect(database.as_uri()+'?mode=ro', uri=True) as db:
        rows = db.execute('SELECT id,received_utc,command,data FROM raw_events WHERE room_id=? '
                          'AND received_utc>=? AND received_utc<? ORDER BY received_utc,id',
                          (str(room), lower, upper)).fetchall()
    state = {'status': 0, 'members': [], 'start': None, 'end': None,
             'punish_end': None, 'sprint': 10, 'sub_type': 0, 'final_end': None}
    snapshots = []
    for ident, received, command, body in rows:
        if command not in COMMANDS:
            continue
        packet = json.loads(body)
        data = packet.get('data', {})
        basic = data.get('pk_basic', {})
        if str(packet.get('pk_id', basic.get('pk_id', ''))) != str(pk_id):
            continue
        # 回放以收到消息的时刻更新，服务器时钟只用于deadline，不提前揭示结果。
        at = epoch(received)-origin
        if command == 'PK_INFO':
            state.update(status=int(basic['status']), members=data.get('members', []),
                         start=basic.get('start_time'), end=basic.get('end_time'),
                         punish_end=basic.get('punish_end_time'), sprint=basic.get('sprint_duration', 10),
                         sub_type=basic.get('sub_type', 0))
            if state['status'] == 101 and state['start'] is not None:
                state['start'] += (data.get('pk_play') or {}).get('pre_duration', 0)
            final = (data.get('pk_play') or {}).get('final_conf') or {}
            state['final_end'] = final.get('end_time') if final.get('switch') else None
        elif command == 'PK_BATTLE_PRE_NEW':
            state.update(status=101, start=packet.get('timestamp', epoch(received))+data['pre_timer'])
        elif command == 'PK_BATTLE_START_NEW':
            state.update(status=201, start=data['pk_start_time'], end=data['pk_frozen_time'])
        elif command in ('PK_BATTLE_PROCESS_NEW', 'PK_BATTLE_END', 'PK_BATTLE_VIDEO_PUNISH_BEGIN'):
            for key in ('init_info', 'match_info'):
                update = data.get(key, {})
                member = next((m for m in state['members'] if m['room_id'] == update.get('room_id')), None)
                if member is not None:
                    member.update({k: update[k] for k in ('votes', 'winner_type') if k in update})
            if command == 'PK_BATTLE_END':
                state['status'] = 401
            elif command == 'PK_BATTLE_VIDEO_PUNISH_BEGIN':
                state.update(status=601, punish_end=packet.get('timestamp', epoch(received))+
                             data['video_punish']['duration'])
        elif command == 'PK_BATTLE_PUNISH_END':
            state['status'] = 1001
        snapshot = json.loads(json.dumps(state))
        snapshot.update(at=at, raw_event_id=ident, command=command)
        snapshots.append(snapshot)
    if not snapshots or not any(len(s['members']) == 2 for s in snapshots):
        raise ValueError('找不到该PK的双方状态，不能猜测')
    for snapshot in snapshots:
        members = snapshot['members']
        if members and (len(members) != 2 or sum(str(m['room_id']) == str(room) for m in members) != 1):
            raise ValueError('目前仅支持包含目标房间的双人PK')
    return {'schema_version': 1, 'room': str(room), 'pk_id': str(pk_id),
            'video': str(video), 'video_sha256': sha_file(video), 'video_start': video_start,
            'font': str(font), 'font_sha256': sha_file(font), 'duration': duration,
            'width': videos[0]['width'], 'height': videos[0]['height'],
            'connection': connection, 'snapshots': snapshots,
            'policy': 'received-time replay; server deadlines; supplied connection bounds; reconstructed UI'}


def display_state(plan, seconds):
    snapshots = plan['snapshots']
    index = bisect.bisect_right([s['at'] for s in snapshots], seconds)-1
    state = snapshots[index] if index >= 0 else {'status': 0, 'members': []}
    now = epoch(plan['video_start'])+seconds
    status = state['status']
    stage, remaining = 'hidden', None
    deadline = None
    if status == 101:
        stage, deadline = 'preparing', state.get('start')
    elif status == 201:
        deadline = state.get('end')
        stage = 'battle' if deadline and now < deadline else 'settling'
        if deadline and 0 < deadline-now <= state.get('sprint', 10):
            stage = 'sprint'
    elif status == 301:
        deadline = state.get('final_end')
        stage = 'final' if deadline and now < deadline else 'settling'
    elif status == 304:
        stage = 'result'
    elif status in (401, 404, 501):
        stage = 'result'
    elif status in (601, 602):
        deadline = state.get('punish_end')
        stage = 'punishment' if deadline and now < deadline else 'result'
    if deadline is not None and stage in ('preparing', 'battle', 'sprint', 'punishment', 'final'):
        remaining = max(0, math.ceil(deadline-now))
    connection = plan.get('connection')
    if stage == 'hidden' and connection and connection[0] <= seconds < connection[1]:
        stage = 'connected'
    # sub_type=50 的开头15秒只显示巅峰赛标记，网页会隐藏倒计时。
    hide_clock = (stage == 'battle' and state.get('sub_type') == 50 and state.get('start') is not None
                  and now-state['start'] <= 15)
    members = state.get('members', [])
    left = next((m for m in members if str(m['room_id']) == plan['room']), {})
    right = next((m for m in members if str(m['room_id']) != plan['room']), {})
    return {'stage': stage, 'remaining': remaining, 'hide_clock': hide_clock,
            'left': left, 'right': right, 'raw_event_id': state.get('raw_event_id')}


@lru_cache(maxsize=8)
def load_font(path, size):
    return ImageFont.truetype(path, size)


def gradient(draw, left, right, bottom, start, end):
    for x in range(left, right):
        ratio = (x-left)/max(1, right-left-1)
        color = tuple(round(a+(b-a)*ratio) for a, b in zip(start, end))
        draw.line((x, 0, x, bottom), fill=(*color, 245))


def strip(plan, seconds):
    width = plan['width']
    scale = width/1600
    height = max(64, round(88*scale))
    image = Image.new('RGBA', (width, height))
    draw = ImageDraw.Draw(image)
    state = display_state(plan, seconds)
    stage = state['stage']
    if stage == 'hidden':
        return image
    font = load_font(plan['font'], max(16, round(25*scale)))
    small = load_font(plan['font'], max(14, round(20*scale)))
    if stage == 'connected':
        # 连线中来自操作者核对的录像区间，不声称是PK消息字段。
        draw.rounded_rectangle((width/2-95*scale, 0, width/2+95*scale, 42*scale),
                               radius=12*scale, fill=(24, 30, 45, 220))
        draw.text((width/2, 20*scale), '连线中', font=small, anchor='mm', fill='white')
        return image
    left, right = state['left'].get('votes', 0), state['right'].get('votes', 0)
    edge = round(width*bar_ratio(left, right))
    bar_height = round(34*scale)
    gradient(draw, 0, edge, bar_height, (195, 0, 70), (255, 83, 135))
    gradient(draw, edge, width, bar_height, (36, 203, 255), (0, 102, 222))
    draw.text((12*scale, bar_height/2), str(left), font=font, anchor='lm', fill='white', stroke_width=1)
    draw.text((width-12*scale, bar_height/2), str(right), font=font, anchor='rm', fill='white', stroke_width=1)
    draw.polygon([(edge-8*scale, 0), (edge+8*scale, 0), (edge, bar_height)], fill='white')
    labels = {'preparing': '准备中', 'battle': '巅峰赛', 'sprint': '最后冲刺',
              'settling': '结算中', 'result': 'PK结束', 'punishment': '惩罚', 'final': '决胜时刻'}
    label = labels[stage]
    if stage == 'result':
        winner = state['left'].get('is_winner')
        if winner == 1 or state['left'].get('winner_type', 0) in (2, 3):
            label = 'PK结束 · 我方胜'
        elif state['right'].get('is_winner') == 1 or state['right'].get('winner_type', 0) in (2, 3):
            label = 'PK结束 · 对方胜'
    if state['remaining'] is not None and not state['hide_clock']:
        n = state['remaining']
        label += f'  {n//60:02d}:{n%60:02d}'
    box_width = min(width, round(360*scale))
    draw.rounded_rectangle(((width-box_width)/2, bar_height, (width+box_width)/2, height-3),
                           radius=12*scale, fill=(36, 28, 49, 235))
    draw.text((width/2, (bar_height+height-3)/2), label, font=small, anchor='mm', fill='white')
    return image


def render(plan, output):
    if plan.get('schema_version') != 1:
        raise ValueError('未知计划格式')
    if sha_file(plan['video']) != plan['video_sha256'] or sha_file(plan['font']) != plan['font_sha256']:
        raise ValueError('录像或字体已改变')
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    write_json(output/'plan.json', plan)
    first = strip(plan, 0)
    width, height = first.size
    destination = output/'video.mp4'
    command = ['ffmpeg', '-nostdin', '-v', 'error', '-n', '-threads', '4', '-i', plan['video'],
               '-f', 'rawvideo', '-pixel_format', 'rgba', '-video_size', f'{width}x{height}',
               '-framerate', '10', '-i', 'pipe:0', '-filter_complex_threads', '2',
               '-filter_complex', '[0:v][1:v]overlay=0:0:shortest=1:format=auto,format=yuv420p[v]',
               '-map', '[v]', '-map', '0:a:0', '-c:v', 'libx265', '-preset', 'veryfast', '-crf', '22',
               '-x265-params', 'pools=8:frame-threads=3:log-level=error', '-tag:v', 'hvc1',
               '-r', '30', '-c:a', 'copy', '-t', str(plan['duration']), '-movflags', '+faststart', str(destination)]
    with (output/'render.log').open('wb') as log:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stderr=log)
        try:
            for frame in range(math.ceil(plan['duration']*10)):
                process.stdin.write(strip(plan, frame/10).tobytes())
            process.stdin.close()
            if process.wait(timeout=180):
                raise ValueError('视频渲染失败，见render.log')
        except BaseException:
            process.kill()
            process.wait()
            raise
    verify(output)
    return {'video': str(destination)}


def audio_hash(path):
    return subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-i', str(path), '-map', '0:a:0',
                           '-c', 'copy', '-f', 'hash', '-hash', 'sha256', '-'],
                          capture_output=True, text=True, check=True, timeout=60).stdout.strip()


def verify(output):
    output = Path(output)
    plan = json.loads((output/'plan.json').read_text())
    video = output/'video.mp4'
    info = media_info(video)
    stream = next(s for s in info['streams'] if s['codec_type'] == 'video')
    if ([stream['width'], stream['height']] != [plan['width'], plan['height']]
            or abs(float(info['format']['duration'])-plan['duration']) > .15
            or sha_file(plan['video']) != plan['video_sha256']):
        raise ValueError('时长、画幅或源文件校验失败')
    if audio_hash(video) != audio_hash(plan['video']):
        raise ValueError('音轨与来源不一致')
    subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-xerror', '-threads', '4',
                    '-i', str(video), '-f', 'null', '-'], check=True, timeout=300)
    result = {'verified': True, 'sha256': sha_file(video), 'duration': info['format']['duration'],
              'audio_identical': True, 'reconstructed_ui': True, 'visual_review_required': True}
    write_json(output/'verification.json', result)
    return result


def configure_parser(commands):
    parser = commands.add_parser('pk-bar', help='离线重放历史PK比分、阶段和倒计时')
    actions = parser.add_subparsers(dest='action', required=True)
    plan = actions.add_parser('plan')
    for name in ('database', 'room', 'pk-id', 'video', 'video-start', 'font', 'output'):
        plan.add_argument('--'+name, required=True)
    plan.add_argument('--connection-start', type=float)
    plan.add_argument('--connection-end', type=float)
    rendering = actions.add_parser('render')
    rendering.add_argument('--plan', required=True)
    rendering.add_argument('--output', required=True)
    check = actions.add_parser('verify')
    check.add_argument('--output', required=True)


def main(args):
    try:
        if args.action == 'plan':
            values = args.connection_start, args.connection_end
            if (values[0] is None) != (values[1] is None):
                raise ValueError('连线起止必须同时提供')
            plan = make_plan(args.database, args.room, args.pk_id, args.video, args.video_start,
                             args.font, connection=list(values) if values[0] is not None else None)
            path = Path(args.output)
            if path.exists():
                raise ValueError('计划文件已存在')
            write_json(path, plan)
            result = {'plan': str(path), 'snapshots': len(plan['snapshots'])}
        elif args.action == 'render':
            result = render(json.loads(Path(args.plan).read_text()), args.output)
        else:
            result = verify(args.output)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, KeyError, sqlite3.Error, subprocess.SubprocessError) as error:
        print(json.dumps({'error': str(error)}, ensure_ascii=False))
        return 1
