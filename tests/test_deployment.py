import os
import subprocess
import sys
from pathlib import Path

import pytest

from bilibili_live_monitor import deployment as d


@pytest.fixture
def release(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    subprocess.run(['git', 'init', '-q', '-b', 'master', str(source)], check=True)
    for name, text in {'src/bilibili_live_monitor/__init__.py': '',
                       'frontend/dist/index.html': 'first', 'pyproject.toml': '[project]\nname="test"'}.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    # No commits or user Git configuration needed for synthetic HEAD.
    tree = subprocess.check_output(['git', 'hash-object', '-t', 'tree', '--stdin'], input=b'', cwd=source).strip()
    env = {**os.environ, 'GIT_AUTHOR_NAME': 'Test', 'GIT_AUTHOR_EMAIL': 'test@example.invalid',
           'GIT_COMMITTER_NAME': 'Test', 'GIT_COMMITTER_EMAIL': 'test@example.invalid'}
    commit = subprocess.check_output(['git', 'commit-tree', tree.decode(), '-m', 'fixture'], cwd=source, env=env).strip()
    subprocess.run(['git', 'update-ref', 'HEAD', commit.decode()], cwd=source, check=True)
    result = d.build(source, tmp_path / 'releases', allow_uncommitted=True)
    return source, Path(result['path'])


def test_build_reuses_content_and_detects_tampering(release):
    source, path = release
    assert d.build(source, path.parent, allow_uncommitted=True)['path'] == str(path)
    target = path / 'frontend/dist/index.html'
    target.chmod(0o644)
    target.write_text('tampered')
    with pytest.raises(ValueError, match='改变'):
        d.verify(path)


def test_task_pin_survives_component_switch(release, tmp_path):
    source, first = release
    registry = tmp_path / 'registry.json'
    d.register(registry, 'sessions', first, sys.executable, allow_candidate=True)
    marker = tmp_path / 'task/deployment.json'
    pinned = d.pin(registry, 'sessions', marker)
    (source / 'frontend/dist/index.html').write_text('second')
    second = d.build(source, first.parent, allow_uncommitted=True)
    d.register(registry, 'sessions', second['path'], sys.executable, expected_revision=1, allow_candidate=True)
    assert d.select(registry, 'sessions')['release_id'] != pinned['release_id']
    assert d.select(registry, 'sessions', marker)['release_id'] == pinned['release_id']
    with pytest.raises(ValueError, match='已变化'):
        d.register(registry, 'sessions', first, sys.executable, expected_revision=1, allow_candidate=True)


def test_dry_run_never_pins_or_executes(release, tmp_path):
    _, path = release
    registry = tmp_path / 'registry.json'
    marker = tmp_path / 'task.json'
    d.register(registry, 'sessions', path, sys.executable, allow_candidate=True)
    result = d.run(registry, 'sessions', ['-m', 'synthetic'], task=marker, dry_run=True)
    assert result['executed'] is False and not marker.exists()
    assert not (tmp_path / 'launches').exists()


def test_drain_resumes_existing_pin_but_never_creates_one(release, tmp_path, monkeypatch):
    _, path = release
    registry = tmp_path / 'registry.json'
    marker = tmp_path / 'task.json'
    d.register(registry, 'sessions', path, sys.executable, allow_candidate=True)
    d.pin(registry, 'sessions', marker)
    guard = tmp_path / 'guard.json'
    d.atomic(guard, {'mode': 'drain'})
    monkeypatch.setenv('BILI_DEPLOYMENT_GUARD', str(guard))
    binding, argv, env = d.prepare_run(registry, 'sessions', ['-c', 'pass'], task=marker)
    assert binding['release'] == str(path)
    assert argv[0] == sys.executable
    assert env['BILI_TASK_PIN'] == str(marker)
    missing = tmp_path / 'missing.json'
    with pytest.raises(d.MaintenancePaused):
        d.prepare_run(registry, 'sessions', ['-c', 'pass'], task=missing)
    assert not missing.exists()


def test_guard_drain_allows_existing_but_blocks_new(tmp_path, monkeypatch):
    guard = tmp_path / 'guard.json'
    d.atomic(guard, {'mode': 'drain', 'components': {'capture': 'running'}})
    monkeypatch.setenv('BILI_DEPLOYMENT_GUARD', str(guard))
    assert not d.paused('sessions') and d.paused('sessions', new_work=True)
    assert not d.paused('capture', new_work=True)
    d.atomic(guard, {'mode': 'paused'})
    with pytest.raises(RuntimeError, match='maintenance_paused'):
        d.require_running('sessions')
    guard.unlink()
    with pytest.raises(FileNotFoundError):
        d.paused('sessions')


def test_child_binding_and_legacy_identity(tmp_path, monkeypatch):
    monkeypatch.setenv('BILI_RELEASE_ROOT', str(tmp_path / 'release'))
    monkeypatch.setenv('BILI_RELEASE_ID', 'release-test')
    monkeypatch.setenv('BILI_DEPLOYMENT_GUARD', str(tmp_path / 'guard.json'))
    env = d.child_environment()
    assert env['PYTHONPATH'] == str(tmp_path / 'release/src')
    assert env['BILI_RELEASE_ID'] == 'release-test'
    with pytest.raises(RuntimeError, match='未登记'):
        d.check_task({'workspace': str(tmp_path)})
    marker = {'schema': 1, 'component': 'sessions', 'binding': {
        'release': str(Path(d.__file__).resolve().parents[2]), 'runtime': {'python': sys.executable}}}
    d.atomic(tmp_path / 'deployment.json', marker)
    monkeypatch.setattr(d, 'validate_binding', lambda binding: binding)
    d.check_task({'workspace': str(tmp_path)})
    marker['binding']['runtime']['python'] = '/wrong/python'
    d.atomic(tmp_path / 'deployment.json', marker)
    with pytest.raises(RuntimeError, match='不匹配'):
        d.check_task({'workspace': str(tmp_path)})


@pytest.mark.parametrize('exception', ['BudgetPaused', 'MaintenancePaused'])
def test_component_process_preserves_pause_control_flow(release, tmp_path, monkeypatch, exception):
    from bilibili_live_monitor.session_budget import BudgetPaused
    source, first = release
    package = source / 'src/bilibili_live_monitor'
    (package / 'deployment.py').write_bytes(Path(d.__file__).read_bytes())
    (package / 'session_budget.py').write_text('class BudgetPaused(RuntimeError): pass\n')
    (package / 'probe.py').write_text(
        'from .deployment import MaintenancePaused\n'
        'from .session_budget import BudgetPaused\n'
        'def call(state):\n'
        ' state["child_checkpoint"] = 123\n'
        f' raise {exception}("paused")\n')
    built = d.build(source, first.parent, allow_uncommitted=True)
    registry = tmp_path / 'registry.json'
    d.register(registry, 'crm', built['path'], sys.executable, allow_candidate=True)
    monkeypatch.setenv('BILI_DEPLOYMENT_REGISTRY', str(registry))
    state = {'finish_deadline': 42}
    expected = BudgetPaused if exception == 'BudgetPaused' else d.MaintenancePaused
    with pytest.raises(expected):
        d.component_call('crm', tmp_path / 'crm.json', 'bilibili_live_monitor.probe',
                         'call', [state], new_task=True)
    if exception == 'BudgetPaused':
        assert state == {'finish_deadline': 42, 'child_checkpoint': 123}
