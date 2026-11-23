"""单场可恢复工作流：实时发布→停止AI→封口→离线ASR→最终静态页。"""
import argparse
import fcntl
import hashlib
import json
import os
import re
import shlex
import subprocess
import time
import urllib.request
from html.parser import HTMLParser
from pathlib import Path

from . import live_share_watch as watch
from .content_mode import content_ai_enabled
from .live_share import atomic_json, iso
from .live_share_control import (
    export_snapshot,
    load_config,
    read_session_source,
    source_at_cutoff,
    verify_snapshot,
)
from .session_ai import ai_control
from .session_budget import BudgetPaused, check_budget
from .session_offline import recording_files, run_offline


def _strip_cdn_beacon(match):
    class Script(HTMLParser):
        sources = ()

        def handle_starttag(self, tag, attrs):
            self.sources = tuple(value for key, value in attrs if tag == 'script' and key == 'src')

    parser = Script()
    parser.feed(match.group().decode())
    if (len(parser.sources) == 1 and parser.sources[0]
            and re.fullmatch(r'https://static\.cloudflareinsights\.com/beacon\.min\.js(?:[/?][^\s]*)?', parser.sources[0])):
        return b''
    return match.group()


def verify_remote(config):
    if config.get('local_only', False):
        return
    for name in ('index.html', 'snapshot.json'):
        local = (Path(config['workspace']) / 'public' / name).read_bytes()
        req = urllib.request.Request(config['url'] + name,
                                     headers={'Cache-Control': 'no-cache', 'User-Agent': 'bilibili-live-monitor/0.1'})
        with urllib.request.urlopen(req, timeout=20) as response:
            remote = response.read()
        if name == 'index.html':
            remote = re.sub(rb'<script\b[^>]*></script>\n?', _strip_cdn_beacon, remote)
        if hashlib.sha256(remote).digest() != hashlib.sha256(local).digest():
            raise RuntimeError('远端看板内容与本次本地产物不一致')


def stop_ai(config, session, *, children=False):
    if not session:
        return True
    children_stopped = True
    if children:
        for child in ai_control(config, 'children', session=session):
            children_stopped = stop_ai(config, child, children=True) and children_stopped
    if ai_control(config, 'status', session=session).get('type') in ('busy', 'retry'):
        ai_control(config, 'abort', session=session)
        return False
    return children_stopped


def ensure_ai(config, purpose):
    from .deployment import require_running
    require_running('crm' if purpose == 'audience_finalize' else 'sessions')
    check_budget(config)
    title = f'{purpose} {config["label"]} {config["workflow_key"]}'
    return ai_control(config, 'create', project=config['opencode_project'], title=title)['id']


def verify_local(config):
    report = verify_snapshot(config)
    if not report['valid']:
        raise RuntimeError('本地看板验收失败：' + ','.join(report['errors']))
    return report


def mark_attention(config):
    """只发布此前已验收的内容，并明确更新异常，不公开原始错误。"""
    workspace = Path(config['workspace'])
    saved = workspace / 'last_verified_snapshot.json'
    if not saved.is_file():
        return
    snapshot = json.loads(saved.read_text())
    snapshot['publication_status'] = 'attention'
    state = json.loads((workspace / 'state.json').read_text())
    if state.get('stop_intent'):
        snapshot['phase'] = 'finalizing'
    from .session_snapshot import write_snapshot
    write_snapshot(snapshot, workspace / 'public', navigation=workspace / 'navigation.json')
    watch.publish(config, workspace / 'public')


def advance(config, state, now):
    check_budget(config, state, now=now, publication=state.get('stage') == 'finalizing')
    workspace = Path(config['workspace'])
    config_path = workspace / 'config.json'
    phase = state.setdefault('stage', 'created')
    uses_ai = content_ai_enabled(config)
    talk_final = config.get('content_mode') == 'talk' and config.get('talk_finalize') is True
    if not uses_ai and (config.get('ai_session') or state.get('final_session')):
        for session in (config.get('ai_session'), None if talk_final else state.get('final_session')):
            if session and not stop_ai(config, session, children=True):
                return
        config.update(ai_enabled=False, ai_session=None)
        if not talk_final:
            state.pop('final_session', None)
        atomic_json(config_path, {k: v for k, v in config.items() if not k.startswith('_')})
    if not uses_ai and not talk_final and phase == 'offline_review':
        state['stage'] = phase = 'finalizing'
    if phase == 'created':
        if uses_ai and not state.get('ai_preflight'):
            ai_control(config, 'preflight', model=config['model'])
            state['ai_preflight'] = True
        if not config.get('local_only', False) and not state.get('remote_prepared'):
            host, directory = config['remote'].split(':', 1)
            subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', host,
                            'mkdir -p -- ' + shlex.quote(directory)],
                           check=True, capture_output=True, timeout=30)
            state['remote_prepared'] = True
        if uses_ai and not config.get('ai_session'):
            config.update(ai_session=ensure_ai(config, 'update_periodic'), ai_enabled=True)
            atomic_json(config_path, {k: v for k, v in config.items() if not k.startswith('_')})
        state['stage'] = 'live'
        return
    if phase == 'live':
        live_state = watch.cycle({**config, '_defer_publish': True}, workspace / 'state.json', 'once')
        if live_state.get('phase') == 'completed':
            # 采集终止不依赖最后一次页面构建成功；封口后统一重建、终审和验收。
            state.update(stage='stopping', cutoff=live_state['cutoff'],
                         finish_deadline=now + config.get('finish_timeout', 21600))
            return
        if live_state.get('last_error'):
            raise RuntimeError(live_state['last_error'])
        verify_local(config)
        atomic_json(workspace / 'last_verified_snapshot.json',
                    json.loads((workspace / 'public/snapshot.json').read_text()))
        if config.get('remote'):
            watch.publish(config, workspace / 'public')
        if not state.get('ready'):
            verify_remote(config)
            state['ready'] = True
        slot = int(now // 300)
        if uses_ai and state.get('ai_slot') != slot:
            state['ai_slot'] = slot
            # 提交之前落盘；响应丢失不重复派发同一轮。
            atomic_json(workspace / 'workflow.json', state)
            _, _, source = source_at_cutoff(config)
            watch.ai_once(config, live_state, source, now)
        return
    if phase == 'stopping':
        if uses_ai and not stop_ai(config, config.get('ai_session')):
            return
        state.update(stage='sealing', stable_since=now)
        return
    if phase == 'sealing':
        groups = recording_files(config, state['cutoff'])
        signature = sorted((str(p), p.stat().st_size, p.stat().st_mtime_ns)
                           for files in groups.values() for _, p in files)
        # JSON往返后tuple变list，保持同一表示以便恢复。
        signature = [list(item) for item in signature]
        if state.get('media_signature') != signature:
            state.update(media_signature=signature, stable_since=now)
            return
        platform = watch.request(config['api'] + '/api/platform')
        candidate = next((c for c in platform['candidates'] if c.get('room_id') == config.get('platform_room_id', config['room_id'])), {})
        source = next((s for s in platform['sources'] if s.get('room_id') == config['room_id']), {})
        same_live = candidate.get('live_status') == 1 and candidate.get('live_start') == config['live_start']
        if same_live or (source.get('status') != 'stopped' and candidate.get('live_start') == config['live_start']):
            return
        if now - state['stable_since'] >= config.get('seal_seconds', 300):
            state['stage'] = 'offline_asr'
        return
    if phase == 'offline_asr':
        from .session_offline import NoMediaError
        try:
            config['offline_segments'] = run_offline(config, state)
        except NoMediaError as error:
            state.update(stage='attention', media_status='no_media', last_error=str(error))
            return
        atomic_json(config_path, {k: v for k, v in config.items() if not k.startswith('_')})
        state['stage'] = 'offline_review' if uses_ai or talk_final else 'finalizing'
        return
    if phase == 'offline_review':
        if talk_final:
            from . import talk_finalize
            if not state.get('final_session'):
                state['final_session'] = ensure_ai(config, 'talk_finalize')
                return
            if not state.get('finalize_submitted'):
                plan = talk_finalize.prepare(config)
                if state.get('finalize_sending'):
                    if not ai_control(config, 'submitted', session=state['final_session'], marker='FINALIZE_PLAN:' + plan['id']):
                        raise RuntimeError('杂谈终审发送回执未确认，不重复提交')
                else:
                    def handoff():
                        state['finalize_sending'] = True
                        atomic_json(workspace / 'workflow.json', state)
                    from .session_finalize import SubmissionNotSent
                    try:
                        talk_finalize.submit(config, state['final_session'], on_handoff=handoff)
                    except SubmissionNotSent:
                        state.pop('finalize_sending', None)
                        atomic_json(workspace / 'workflow.json', state)
                        raise
                state.update(finalize_submitted=True, finalize_sending=False)
                atomic_json(workspace / 'workflow.json', state)
            state['finalize_progress'] = talk_finalize.progress(config)
            if talk_finalize.complete(config):
                state['stage'] = 'finalizing'
            else:
                from .finalize_lifecycle import supervise
                supervise(config, state, state['final_session'], workspace / 'talk_finalize', now)
            return
        if not state.get('final_session'):
            state['final_session'] = ensure_ai(config, 'finalize')
            return
        if config.get('finalize_parallel'):
            from . import session_finalize
            if not state.get('finalize_submitted'):
                plan = session_finalize.prepare(config)
                if state.get('finalize_sending'):
                    if not ai_control(config, 'submitted', session=state['final_session'], marker='FINALIZE_PLAN:' + plan['id']):
                        raise RuntimeError('总任务发送回执未确认，保留现场且不重复提交')
                else:
                    def handoff():
                        state['finalize_sending'] = True
                        atomic_json(workspace / 'workflow.json', state)
                    try:
                        session_finalize.submit(config, state['final_session'], on_handoff=handoff)
                    except session_finalize.SubmissionNotSent:
                        state.pop('finalize_sending', None)
                        atomic_json(workspace / 'workflow.json', state)
                        raise
                state.update(finalize_submitted=True, finalize_sending=False)
                atomic_json(workspace / 'workflow.json', state)
            state['finalize_progress'] = session_finalize.progress(config)
            if session_finalize.complete(config):
                session_finalize.finish(config)
                state['stage'] = 'finalizing'
            else:
                from .finalize_lifecycle import music_work, supervise
                root = workspace / 'finalize'
                actionable = (lambda: music_work(root)) if config.get('final_subagent') in (
                    'grok_build_cli', 'cursor_cli_grok') else None
                supervise(config, state, state['final_session'], root, now, actionable=actionable)
            return
        if ai_control(config, 'status', session=state['final_session']).get('type') in ('busy', 'retry'):
            return
        export_snapshot(config)
        report = verify_local(config)
        source = read_session_source(config)
        from .session_finalize import reviewed as is_reviewed
        from .transcript_corrections import correction_context
        corrections, _ = correction_context(workspace, source,
                                            warehouse_database=config.get('warehouse_database'))
        from .annotation_history import read_annotations
        reviews = read_annotations(workspace).get('reviews', [])
        reviewed = {r.get('seq'): r for r in reviews if isinstance(r, dict)}
        remaining = sum(s.get('is_final') and bool(s.get('text')) and not is_reviewed(
                         reviewed.get(s['seq']), {**s, **corrections.get(s['seq'], {})})
                         for s in source['segments'])
        state.update(unreviewed_windows=remaining, unpublished_songs=report['unpublished_songs'])
        if not remaining and not report['unpublished_songs']:
            state['stage'] = 'finalizing'
            return
        if state.get('review_batches', 0) >= config.get('final_review_batches', 40):
            raise ValueError('离线复核已到批次上限，尚有未审窗口')
        state['review_batches'] = state.get('review_batches', 0) + 1
        atomic_json(workspace / 'workflow.json', state)
        final_config = {**config, 'ai_session': state['final_session'],
                        'model': config.get('final_model', config['model'])}
        watch.ai_once(final_config, {'cutoff': state['cutoff']}, source, now, final=True)
        return
    if phase == 'finalizing':
        from .finalize_lifecycle import drain
        if config.get('content_mode') == 'talk':
            from .session_offline import verify_offline
            verify_offline(config, state['cutoff'])
        if talk_final:
            from . import talk_finalize
            if not drain(config, state, state.get('final_session'), now):
                return
            if not talk_finalize.complete(config):
                raise ValueError('杂谈精修尚未完整入库，不能发布')
        if uses_ai and not drain(config, state, state.get('final_session'), now,
                                  children=config.get('finalize_parallel', False)):
            return
        if state.get('publication_resume'):
            from .session_finalize import complete
            if (not complete(config) or state['publication_resume']['receipt_sha256']
                    != publication_receipt_digest(config)):
                raise ValueError('发布恢复的终审凭证已变更')
        if uses_ai and config.get('finalize_parallel'):
            from .session_finalize import finish
            finish(config)  # 所有写入者停止后，重验实际待发布内容。
        elif uses_ai:
            from .session_finalize import corrections
            if corrections(config)['needs_review']:
                raise ValueError('转写校正尚未完成复核，不能发布最终版')
        export_snapshot(config)
        verify_local(config)
        if config.get('final_timeline'):
            from .session_timeline import commit, verify
            state['final_timeline'] = commit(config, state['cutoff'])
            verify(config, state['cutoff'])
        if config.get('audience_crm_config'):
            check_budget(config, state)
            from .audience_finalize import advance as advance_audience
            if not advance_audience(config, state):
                return
        check_budget(config, state, publication=True)
        watch.publish(config, workspace / 'public')
        verify_remote(config)
        state['final_artifacts'] = {name: hashlib.sha256((workspace / name).read_bytes()).hexdigest()
                                   for name in ('public/index.html', 'public/snapshot.json', 'annotations.json', 'verified_songs.json')
                                   if (workspace / name).is_file()}
        state.update(stage='completed', completed_at=iso(now), last_error=None, ready=True)


def run(path, *, resume_publication=False):
    config = load_config(path)
    from .deployment import MaintenancePaused, check_task
    check_task(config)
    workspace = Path(config['workspace'])
    with (workspace / 'workflow.lock').open('a') as lock, (workspace / 'publisher.lock').open('a') as publisher:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(publisher, fcntl.LOCK_EX | fcntl.LOCK_NB)
        status_path = workspace / 'workflow.json'
        state = json.loads(status_path.read_text()) if status_path.exists() else {'stage': 'created'}
        if resume_publication:
            resume_paused_music_publication(config, state, time.time())
        publication_only = bool(state.get('publication_resume'))
        state['runner_runtime'] = {'pid': os.getpid(),
            'code_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'cursor_worker_sha256': hashlib.sha256(Path(__file__).with_name('cursor_finalize.py').read_bytes()).hexdigest(),
            'started_at': iso(time.time())}
        # 首次健康任务可能在AI预检完成前启动，先发布可读取的初始状态。
        atomic_json(status_path, state)
        next_health_check = 0
        while state['stage'] not in ('completed', 'attention'):
            from .deployment import paused
            if paused('sessions'):
                return 0  # 维护停止不更改业务阶段，也不发布attention页。
            started = time.time()
            config = load_config(path)
            if publication_only and (state['stage'] != 'finalizing'
                    or state['publication_resume']['config_sha256'] != publication_config_digest(config)):
                state.update(stage='attention', last_error='发布恢复配置已变更', updated_at=iso(started))
                atomic_json(status_path, state)
                break
            try:
                publication_ready(config, state, started)
                check_budget(config, state, now=started, publication=state.get('stage') == 'finalizing')
            except BudgetPaused:
                pause_budget(state, started)
                atomic_json(status_path, state)
                break
            if not publication_only and started >= next_health_check:
                from .session_health import ensure_scheduled
                try:
                    ensure_scheduled(config, delay=1)
                    state.pop('health_schedule_error', None)
                except Exception as error:
                    state['health_schedule_error'] = f'{type(error).__name__}: {error}'[:500]
                next_health_check = started + 300
            try:
                advance(config, state, started)
                if state['stage'] != 'attention':
                    state['last_error'] = None
                state['consecutive_errors'] = 0
            except MaintenancePaused:
                return 0
            except BudgetPaused:
                pause_budget(state, time.time())
            except Exception as error:
                state['last_error'] = f'{type(error).__name__}: {error}'[:500]
                state['consecutive_errors'] = state.get('consecutive_errors', 0) + 1
                if isinstance(error, ValueError) or state['consecutive_errors'] >= 10:
                    # 收尾失败亦先停本场专属AI，不重新打开实时调度。
                    stopped = True
                    for session in (config.get('ai_session'), state.get('final_session'), state.get('audience_session')):
                        if session:
                            try:
                                stopped = stop_ai(config, session, children=(session == state.get('audience_session')
                                                  or config.get('finalize_parallel', False)
                                                  or config.get('talk_finalize') is True)) and stopped
                            except Exception:
                                stopped = False
                    try:
                        from .cursor_finalize import stop_workers
                        stopped = stop_workers(config) and stopped
                    except Exception:
                        stopped = False
                    if stopped:
                        state['stage'] = 'attention'
                        if not publication_only:
                            try:
                                mark_attention(config)
                            except Exception as publication_error:
                                state['attention_publish_error'] = type(publication_error).__name__
            state['updated_at'] = iso(time.time())
            atomic_json(status_path, state)
            if state['stage'] not in ('completed', 'attention'):
                time.sleep(max(1, (60 if state['stage'] == 'live' or state.get('last_error') else 10) - (time.time() - started)))
        return 0 if state['stage'] == 'completed' else 1


def pause_budget(state, now):
    # 不abort协调会话、不停止Cursor、不发布异常页面；终态阻止manager/health自动恢复。
    state.setdefault('automation_paused', {'reason': 'finish_timeout', 'stage': state['stage'],
                     'paused_at': iso(now), 'finish_deadline': state.get('finish_deadline')})
    state.update(stage='attention', updated_at=iso(now),
                 last_error='离线收尾自动推进预算已用尽，保留现场等待人工处理')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--resume-publication', action='store_true', help='恢复已暂停且本地终审完成的音乐场次发布')
    args = parser.parse_args()
    return run(args.config.resolve(), resume_publication=args.resume_publication)


def publication_config_digest(config):
    return hashlib.sha256(json.dumps(config, sort_keys=True, default=str).encode()).hexdigest()


def publication_receipt_digest(config):
    return hashlib.sha256((Path(config['workspace']) / 'finalize' / 'completed.json').read_bytes()).hexdigest()


def resume_paused_music_publication(config, state, now):
    from .cursor_finalize import require_stopped
    from .deployment import require_running
    from .session_finalize import complete

    require_running('sessions')
    pause = state.get('automation_paused') or {}
    if (state.get('stage') != 'attention' or pause.get('reason') != 'finish_timeout'
            or pause.get('stage') not in ('offline_review', 'finalizing')
            or not config.get('finalize_parallel') or config.get('content_mode', 'music') != 'music'
            or config.get('audience_crm_config') or state.get('publication_resume')
            or state.get('finish_deadline', float('inf')) > now):
        raise ValueError('本场不符合一次性音乐发布恢复条件')
    require_stopped(config, 'finalize')
    if not complete(config):
        raise ValueError('缺少匹配当前输入的终审完成凭证')
    state['publication_resume'] = {'at': iso(now), 'previous_pause': state.pop('automation_paused'),
                                   'config_sha256': publication_config_digest(config),
                                   'receipt_sha256': publication_receipt_digest(config)}
    state.update(stage='finalizing', publication_deadline=now + 600, last_error=None, consecutive_errors=0)


def publication_ready(config, state, now):
    """有效完成凭证允许一次十分钟收尾，不允许新派发、重试或无限续期。"""
    if state.get('automation_paused') or state.get('stage') not in ('offline_review', 'finalizing'):
        return
    if now < state.get('finish_deadline', float('inf')):
        return
    if config.get('content_mode') == 'talk' and config.get('talk_finalize'):
        from .talk_finalize import complete
    elif config.get('finalize_parallel'):
        from .session_finalize import complete
    else:
        return
    if complete(config):
        state.setdefault('publication_deadline', now + 600)
        state['stage'] = 'finalizing'
        atomic_json(Path(config['workspace']) / 'workflow.json', state)


if __name__ == '__main__':
    raise SystemExit(main())
