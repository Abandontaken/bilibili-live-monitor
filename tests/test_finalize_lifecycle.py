import hashlib
import json
import os
from types import SimpleNamespace

import pytest

from bilibili_live_monitor import finalize_lifecycle as lifecycle
from bilibili_live_monitor import session_finalize, session_runner
from bilibili_live_monitor.audience_finalize import CURSOR_AUTHOR, actionable_work
from bilibili_live_monitor.session_budget import BudgetPaused, check_budget
from bilibili_live_monitor.session_offline import NoMediaError, exclude_empty, run_offline


def test_grace_survives_restart_and_never_stops_early(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(session_runner, 'stop_ai', lambda *a, **kw: calls.append(kw) or True)
    state = {}
    assert not lifecycle.drain({}, state, 'coordinator', 100)
    restored = json.loads(json.dumps(state))
    assert not lifecycle.drain({}, restored, 'coordinator', 219)
    assert not calls
    assert lifecycle.drain({}, restored, 'coordinator', 220)
    assert calls == [{'children': True}]


def test_idle_wake_uses_fixed_cwd_model_and_verifies_delivery(tmp_path, monkeypatch):
    calls = []
    config = {'workspace': str(tmp_path), 'model': 'synthetic/live', 'final_model': 'synthetic/final',
              'opencode_python': '/synthetic/python', 'opencode_cwd': '/synthetic/client'}
    control = {'status': {'type': 'idle'}, 'submitted': False}
    monkeypatch.setattr(lifecycle, 'ai_control', lambda c, op, **kw: control[op])
    monkeypatch.setattr(lifecycle.subprocess, 'run', lambda argv, **kw:
                        calls.append((argv, kw)) or SimpleNamespace(returncode=0, stdout='{}', stderr=''))
    state = {}
    lifecycle.supervise(config, state, 'same-session', tmp_path, 0)
    lifecycle.supervise(config, state, 'same-session', tmp_path, 300)
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert kwargs['cwd'] == config['opencode_cwd']
    assert argv[argv.index('--model') + 1] == 'synthetic/final'
    # ACK并非送达；丢失回执不重发，进程重启也从journal恢复。
    lifecycle.supervise(config, {}, 'same-session', tmp_path, 1000)
    assert len(calls) == 1
    assert json.loads((tmp_path / 'supervision.json').read_text())['delivery'] == 'unknown'
    control.update(submitted=True, status={'type': 'busy'})
    lifecycle.supervise(config, {}, 'same-session', tmp_path, 1001)
    assert len(calls) == 1
    assert json.loads((tmp_path / 'supervision.json').read_text())['delivery'] == 'verified_in_session'


def test_verified_completion_gets_bounded_publish_only_reserve(tmp_path, monkeypatch):
    config = {'workspace': str(tmp_path), 'finalize_parallel': True}
    state = {'stage': 'offline_review', 'finish_deadline': 1}
    monkeypatch.setattr(session_finalize, 'complete', lambda c: True)
    session_runner.publication_ready(config, state, 10)
    assert state['stage'] == 'finalizing' and state['publication_deadline'] == 610
    check_budget(config, state, now=609, publication=True)
    with pytest.raises(BudgetPaused):
        check_budget(config, state, now=11)  # 新派发仍不可用。
    session_runner.publication_ready(config, state, 609)
    assert state['publication_deadline'] == 610
    with pytest.raises(BudgetPaused):
        check_budget(config, state, now=610, publication=True)


@pytest.mark.parametrize('kind,key', [('talk_finalize', 'coordinator_watch'),
                                      ('audience_finalize', 'audience_watch')])
def test_resume_preserves_nonmusic_completion_contract(tmp_path, monkeypatch, kind, key):
    root = tmp_path / kind
    root.mkdir()
    config = {'workspace': str(tmp_path), 'model': 'synthetic/model',
              'opencode_python': '/synthetic/python', 'opencode_cwd': '/synthetic/client'}
    monkeypatch.setattr(lifecycle, 'ai_control', lambda *a, **kw: {'type': 'idle'})
    monkeypatch.setattr(lifecycle.subprocess, 'run', lambda *a, **kw:
                        SimpleNamespace(returncode=0, stdout='{}', stderr=''))
    state = {}
    lifecycle.supervise(config, state, 'same-session', root, 0, key=key)
    lifecycle.supervise(config, state, 'same-session', root, 600, key=key, actionable=lambda: ['work'])
    prompt = (root / 'resume_prompt.md').read_text()
    assert '禁止音乐merge/corrections' in prompt
    assert ('talk_finalize finish' if kind == 'talk_finalize' else 'audience_crm finish') in prompt


def test_crm_waiting_result_gets_wake_and_only_one_retry(tmp_path, monkeypatch):
    calls = []
    config = {'workspace': str(tmp_path), 'model': 'synthetic/model',
              'opencode_python': 'python', 'opencode_cwd': str(tmp_path)}
    monkeypatch.setattr(lifecycle, 'ai_control', lambda c, op, **kw:
                        True if op == 'submitted' else {'type': 'idle'})
    monkeypatch.setattr(lifecycle.subprocess, 'run', lambda *a, **kw:
                        calls.append(a) or SimpleNamespace(returncode=0, stdout='', stderr=''))
    state = {}
    lifecycle.supervise(config, state, 'same', tmp_path, 0, key='audience_watch')
    for now in (600, 1200, 1800, 2400):
        lifecycle.supervise(config, state, 'same', tmp_path, now, key='audience_watch', actionable=lambda: None)
    assert not calls
    for now in (3000, 3600, 4200, 4800):
        lifecycle.supervise(config, {}, 'same', tmp_path, now, key='audience_watch', actionable=lambda: ['review'])
    assert len(calls) == 2
    assert json.loads((tmp_path / 'supervision.json').read_text())['decision'] == 'action_retry_exhausted'
    lifecycle.supervise(config, {}, 'same', tmp_path, 5400, key='audience_watch', actionable=lambda: ['finish'])
    assert len(calls) == 3


def test_crm_full_concurrency_waits_until_a_candidate_is_ready(tmp_path):
    tasks = [{'result': str(tmp_path / f'{i}.result.json')} for i in range(3)]
    (tmp_path / 'plan.json').write_text(json.dumps({'id': 'plan', 'concurrency': 2, 'tasks': tasks}))
    workers = tmp_path / 'cursor_workers'
    workers.mkdir()
    for index in range(2):
        (workers / f'{index:03d}.json').write_text(json.dumps({
            'plan_id': 'plan', 'shard': f'{index:03d}', 'state': 'running'}))
    assert not actionable_work(tmp_path, 'plan', CURSOR_AUTHOR)
    (workers / '001.json').write_text(json.dumps({
        'plan_id': 'plan', 'shard': '001', 'state': 'awaiting_coordinator_review', 'exit_code': 0}))
    assert actionable_work(tmp_path, 'plan', CURSOR_AUTHOR)
    (tmp_path / '0.result.json').write_text('{}')
    (workers / '000.json').write_text(json.dumps({
        'plan_id': 'plan', 'backend': 'legacy_result', 'state': 'accepted'}))
    assert actionable_work(tmp_path, 'plan', CURSOR_AUTHOR) == ['review:1:None', 'start:2']
    assert not actionable_work(tmp_path, 'plan', 'native_worker')


def test_reserve_reaches_advance_without_reopening_dispatch(tmp_path, monkeypatch):
    config = {'workspace': str(tmp_path), 'finalize_parallel': True}
    state = {'stage': 'offline_review', 'finish_deadline': 1, 'final_session': 'coordinator'}
    monkeypatch.setattr(session_finalize, 'complete', lambda c: True)
    session_runner.publication_ready(config, state, 10)
    session_runner.advance(config, state, 10)
    assert state['stage'] == 'finalizing' and state['final_cleanup']['observed_complete_at'] == 10
    with pytest.raises(BudgetPaused):
        session_runner.ensure_ai(config, 'finalize')


def test_empty_source_preserved_gap_keeps_next_timestamp(tmp_path):
    empty = tmp_path / 'empty.mkv'
    empty.touch()
    os.utime(empty, (0, 0))
    row = {'path': str(empty), 'size': 0, 'mtime_ns': empty.stat().st_mtime_ns,
           'sha256': hashlib.sha256(b'').hexdigest(), 'wall_start': 100}
    valid = {'wall_start': 155, 'size': 42}
    manifest = {'cutoff': 1000, 'rows': [row, valid]}
    config = {'workspace': str(tmp_path), 'recording_segment_seconds': 300}
    exclude_empty(manifest, config)
    assert manifest['rows'] == [valid] and valid['wall_start'] == 155
    assert manifest['excluded_empty_sources'][0]['gap_end_utc'].endswith('02:35+00:00')
    assert empty.exists() and empty.stat().st_size == 0
    empty.write_bytes(b'changed')
    with pytest.raises(ValueError):
        exclude_empty(manifest, config)


def test_zero_duration_without_media_is_explicit_not_success(tmp_path):
    config = {'workspace': str(tmp_path), 'recording_root': str(tmp_path),
              'start_utc': '2030-01-01T00:00:00+00:00'}
    with pytest.raises(NoMediaError):
        run_offline(config, {'cutoff': 1})
    assert not (tmp_path / 'offline/completed.json').exists()
    assert (tmp_path / 'offline/manifest.json').exists()
