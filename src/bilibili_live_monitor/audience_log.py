"""按已观测 UID 导出单房间弹幕，不做身份消解。"""

import json
import re
import sqlite3
from bisect import bisect_right
from collections import defaultdict
from contextlib import closing
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

UID = re.compile(r'[1-9][0-9]*')


def _instant(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError('时间必须包含时区偏移')
    return result


def collect(database, archive, room_id, title, timezone='Asia/Shanghai'):
    tz = ZoneInfo(timezone)
    if str(archive.get('room_id', room_id)) != room_id:
        raise ValueError('归档房间与 --room-id 不一致')
    windows = sorted((_instant(row['start_utc']), _instant(row['updated_at']))
                     for row in archive['sessions'] if row['status'] == 'completed')
    if not windows or any(start >= end or (i and windows[i - 1][1] >= start)
                          for i, (start, end) in enumerate(windows)):
        raise ValueError('缺少已完成场次，或场次时间范围无效／重叠')
    starts = [start for start, _ in windows]

    def local(instant):
        return instant.astimezone(tz).isoformat()

    sessions = [dict(start=local(start), end=local(end)) for start, end in windows]
    people = {}
    missing_uid = outside = 0
    with closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only = ON')
        for row_id, when, kind, payload in db.execute('''
            SELECT id, at_utc, kind, data FROM timeline
            WHERE room_id = ? AND kind IN ('entry', 'danmaku', 'gift', 'like')
        ''', (room_id,)):
            instant = _instant(when)
            i = bisect_right(starts, instant) - 1
            if i < 0 or instant > windows[i][1]:
                outside += kind == 'danmaku'
                continue
            event = json.loads(payload)
            uid = str(event.get('sender_uid'))
            if not UID.fullmatch(uid) or event.get('identity_status') not in (None, 'uid'):
                missing_uid += kind == 'danmaku'
                continue
            names, cards = people.setdefault(uid, ({}, defaultdict(list)))
            messages = cards[i]
            name = event.get('author')
            name = name if isinstance(name, str) and name.strip() else None
            if name is not None:
                stamp = instant, row_id
                if name not in names or stamp > names[name]:
                    names[name] = stamp
            if kind == 'danmaku':
                text = event['text']
                if not isinstance(text, str):
                    raise ValueError(f'弹幕 {row_id} 的 text 不是字符串')
                messages.append((instant, row_id, name, text))

    records = []
    for uid, (names, cards) in people.items():
        count = sum(map(len, cards.values()))
        if count:
            records.append(dict(uid=uid, names=sorted(names, key=names.__getitem__, reverse=True),
                                message_count=count, sessions=[
                                    dict(session=i, messages=[dict(at=local(instant), name=name, text=text)
                                                              for instant, _, name, text in sorted(rows)])
                                    for i, rows in sorted(cards.items(), reverse=True)
                                ]))
    records.sort(key=lambda row: (-len(row['sessions']), -row['message_count'], row['uid']))
    return dict(schema_version=1, room_id=room_id, title=title, timezone=timezone,
                sessions=sessions, people=records, stats=dict(
                    message_count=sum(row['message_count'] for row in records),
                    unattributed_message_count=missing_uid, outside_session_message_count=outside))


def render_html(data):
    template = Path(__file__).with_name('audience_log.html').read_text(encoding='utf-8')
    payload = json.dumps(data, ensure_ascii=False, separators=(',', ':')).replace('<', '\\u003c')
    return template.replace('__AUDIENCE_DATA__', payload, 1)


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, mode='w', encoding='utf-8', delete=False) as file:
        file.write(text)
    Path(file.name).replace(path)


def configure_parser(commands):
    parser = commands.add_parser('audience-log', help='只读导出按 UID 分组的观众弹幕档案')
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--sessions', type=Path, required=True, help='已归档的单房间场次 JSON')
    parser.add_argument('--room-id', required=True)
    parser.add_argument('--title', default='观众弹幕档案')
    parser.add_argument('--timezone', default='Asia/Shanghai')
    parser.add_argument('--json-out', type=Path)
    parser.add_argument('--html-out', type=Path)
    parser.set_defaults(audience_log_parser=parser)


def main(args):
    parser = args.audience_log_parser
    if not UID.fullmatch(args.room_id):
        parser.error('--room-id 必须是正整数房号')
    outputs = [path for path in (args.json_out, args.html_out) if path is not None]
    if not outputs:
        parser.error('至少指定 --json-out 或 --html-out')
    paths = [args.database, args.sessions, *outputs]
    if len({path.resolve() for path in paths}) != len(paths):
        parser.error('输入与输出必须使用不同的路径')
    try:
        archive = json.loads(args.sessions.read_text(encoding='utf-8'))
        data = collect(args.database, archive, args.room_id, args.title, args.timezone)
        if args.json_out:
            _write(args.json_out, json.dumps(data, ensure_ascii=False, indent=2) + '\n')
        if args.html_out:
            _write(args.html_out, render_html(data))
    except (OSError, sqlite3.Error, KeyError, TypeError, ValueError, ZoneInfoNotFoundError) as error:
        parser.error(f'{type(error).__name__}: {error}')
    print(json.dumps(dict(sessions=len(data['sessions']), people=len(data['people']), **data['stats']),
                     ensure_ascii=False))
    return 0
