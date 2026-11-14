from pathlib import Path

import pytest

from bilibili_live_monitor import audience_watch, deployment, session_health, session_runner


def test_paused_watch_does_not_create_workspace(tmp_path, monkeypatch):
    guard = tmp_path / 'guard.json'
    deployment.atomic(guard, {'mode': 'paused'})
    monkeypatch.setenv('BILI_DEPLOYMENT_GUARD', str(guard))
    root = tmp_path / 'not-created'
    assert audience_watch.tick({'workspace': str(root)}) == {'status': 'maintenance_paused'}
    assert not root.exists()


def test_paused_health_never_calls_api_or_creates_lock(tmp_path, monkeypatch):
    guard = tmp_path / 'guard.json'
    deployment.atomic(guard, {'mode': 'paused'})
    monkeypatch.setenv('BILI_DEPLOYMENT_GUARD', str(guard))
    monkeypatch.setattr(session_health, 'request', lambda *a: (_ for _ in ()).throw(AssertionError('API called')))
    assert session_health.run({'workspace': str(tmp_path / 'absent')}, reschedule=True) == {'maintenance_paused': True}
    assert not (tmp_path / 'absent').exists()


def test_task_launch_uses_pin_not_parent_release(tmp_path, monkeypatch):
    monkeypatch.setenv('BILI_RELEASE_ROOT', '/parent/release')
    marker = tmp_path / 'deployment.json'
    binding = {'release': '/old/release', 'release_id': 'old', 'source_identity': 'hash',
               'runtime': {'python': '/old/venv/bin/python'}}
    deployment.atomic(marker, {'component': 'sessions', 'binding': binding})
    monkeypatch.setattr(deployment, 'verify', lambda p: {'source_identity': 'hash'})
    monkeypatch.setattr(deployment, 'validate_binding', lambda binding: binding)
    command, env = deployment.task_launch(tmp_path, 'bilibili_live_monitor.session_runner')
    assert command[0] == '/old/venv/bin/python'
    assert env['BILI_RELEASE_ROOT'] == '/old/release'
    assert env['PYTHONPATH'] == str(Path('/old/release/src'))


def test_paused_runner_keeps_business_state_and_never_cleans_up(tmp_path, monkeypatch):
    guard = tmp_path / 'guard.json'
    deployment.atomic(guard, {'mode': 'paused'})
    monkeypatch.setenv('BILI_DEPLOYMENT_GUARD', str(guard))
    original = {'stage': 'offline_review', 'finish_deadline': 123, 'final_session': 'existing-session'}
    deployment.atomic(tmp_path / 'workflow.json', original)
    monkeypatch.setattr(session_runner, 'load_config', lambda p: {'workspace': str(tmp_path)})
    def forbidden(*a, **kw):
        raise AssertionError('paused runner advanced or cleaned up')
    for name in ('advance', 'stop_ai', 'mark_attention'):
        monkeypatch.setattr(session_runner, name, forbidden)
    assert session_runner.run(tmp_path / 'config.json') == 0
    saved = deployment.read(tmp_path / 'workflow.json')
    assert all(saved[key] == value for key, value in original.items())
    assert 'automation_paused' not in saved


def test_new_session_pins_registry_target_not_capture(tmp_path, monkeypatch):
    monkeypatch.setenv('BILI_RELEASE_ROOT', '/capture/a')
    monkeypatch.setenv('BILI_DEPLOYMENT_REGISTRY', '/registry.json')
    calls = []
    monkeypatch.setattr(deployment, 'pin', lambda *args: calls.append(args))
    deployment.pin_current(tmp_path, new_task=True)
    assert calls == [('/registry.json', 'sessions', tmp_path / 'deployment.json')]
    with pytest.raises(RuntimeError, match='历史任务'):
        deployment.pin_current(tmp_path)


def test_runtime_changed_blocks_child_launch(tmp_path, monkeypatch):
    deployment.atomic(tmp_path / 'deployment.json', {'component': 'sessions', 'binding': {
        'release': '/release', 'source_identity': 'source', 'runtime': {
            'python': '/runtime/python', 'identity': 'old'}}})
    monkeypatch.setattr(deployment, 'verify', lambda p: {'source_identity': 'source'})
    monkeypatch.setattr(deployment, 'environment', lambda p: {'identity': 'changed'})
    with pytest.raises(ValueError, match='依赖已变化'):
        deployment.task_launch(tmp_path, 'example')


def test_mid_tick_crm_pause_does_not_mark_runner_failed(tmp_path, monkeypatch):
    deployment.atomic(tmp_path / 'workflow.json', {'stage': 'finalizing', 'final_session': 'original'})
    monkeypatch.setattr(session_runner, 'load_config', lambda p: {'workspace': str(tmp_path)})
    monkeypatch.setattr(session_health, 'ensure_scheduled', lambda *a, **k: None)
    def paused(*args):
        raise deployment.MaintenancePaused('crm paused')
    monkeypatch.setattr(session_runner, 'advance', paused)
    monkeypatch.setattr(session_runner, 'stop_ai', lambda *a, **k: pytest.fail('must not abort'))
    monkeypatch.setattr(session_runner, 'mark_attention', lambda *a: pytest.fail('must not publish'))
    assert session_runner.run(tmp_path / 'config.json') == 0
    state = deployment.read(tmp_path / 'workflow.json')
    assert state['stage'] == 'finalizing'
    assert state['final_session'] == 'original'
    assert 'consecutive_errors' not in state
