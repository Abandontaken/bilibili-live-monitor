import json
from pathlib import Path

import pytest
from test_cursor_finalize import setup

from bilibili_live_monitor import cursor_finalize as finalize
from bilibili_live_monitor import finalize_lifecycle as lifecycle
from bilibili_live_monitor.cursor_finalize import CURSOR_AUTHOR, frozen_author


@pytest.mark.parametrize(('stop', 'model', 'expected'), [
    ('end_turn', 'grok-4.7-build', True),
    ('max_turns', 'grok-4.7-build', False),
    ('end_turn', 'grok-4.7-build-fast', False),
])
def test_grok_result_checks_served_model_and_terminal_state(stop, model, expected):
    state = {}
    assert finalize.grok_receipt({'stopReason': stop, 'sessionId': 'test-session',
                                  'modelUsage': {model: {'modelCalls': 1}}}, state) is expected
    assert state['session_ids'] == ['test-session']
    assert state['observed_models'] == [model]


def test_crm_dispatch_uses_frozen_backend(tmp_path):
    root = tmp_path / 'audience_finalize'
    root.mkdir()
    marker = root / 'plan.json'
    config = {'workspace': str(tmp_path), 'final_subagent': finalize.GROK_BACKEND}
    marker.write_text('{}')
    (root / 'prompt.md').write_text('Launcher托管单片Cursor')
    assert finalize.shard_backend(config, 'audience_finalize') == finalize.BACKEND
    assert finalize.shard_backend(config, 'finalize') == finalize.GROK_BACKEND
    marker.write_text(json.dumps({'author_agent': finalize.GROK_BACKEND}))
    assert finalize.shard_backend(config, 'audience_finalize') == finalize.GROK_BACKEND


def test_grok_command_uses_native_cli_and_pins_model(tmp_path):
    command = finalize.cli_command({'backend': finalize.GROK_BACKEND, 'resume_session': 'test-id'},
                                   tmp_path / 'prompt.md', tmp_path)
    assert Path(command[0]).name == 'grok'
    assert command[command.index('--model') + 1] == 'grok-4.7'
    assert command[command.index('--resume') + 1] == 'test-id'
    assert command[command.index('--tools') + 1] == 'read_file,list_dir,grep'


def test_legacy_crm_route_not_guessed_after_switch(tmp_path):
    with pytest.raises(ValueError, match='无法确认'):
        frozen_author({}, tmp_path)
    prompt = tmp_path / 'prompt.md'
    prompt.write_text('例如使用task的general子代理处理input/result')
    assert frozen_author({}, tmp_path) == 'general'
    prompt.write_text('Launcher托管单片Cursor')
    assert frozen_author({}, tmp_path) == CURSOR_AUTHOR
    assert frozen_author({'author_agent': finalize.GROK_BACKEND}, tmp_path) == finalize.GROK_BACKEND


def test_native_crm_idle_wake_names_frozen_agent(tmp_path, monkeypatch):
    from subprocess import CompletedProcess

    monkeypatch.setattr(lifecycle, 'ai_control', lambda *_args, **_kw: {'type': 'idle'})
    monkeypatch.setattr(lifecycle.subprocess, 'run', lambda *_args, **_kw: CompletedProcess([], 0, '', ''))
    config = {'author_agent': 'general', 'model': 'example/coordinator',
              'opencode_python': 'python', 'opencode_cwd': str(tmp_path), 'workspace': str(tmp_path)}
    state = {'audience_watch': {'last_wake_at': 0, 'attempts': 0}}
    lifecycle.supervise(config, state, 'test-session', tmp_path, 600, key='audience_watch', actionable=lambda: ['finish'])
    prompt = (tmp_path / 'resume_prompt.md').read_text()
    assert '冻结的 general 子代理' in prompt
    assert 'plan.json指定的subagent' not in prompt


@pytest.mark.parametrize(('code', 'text', 'model', 'ready'), [
    (0, 'valid', 'grok-4.7-build', True),
    (1, 'valid', 'grok-4.7-build', False),
    (0, None, 'grok-4.7-build', False),
    (0, 'valid', 'grok-4.7-build-fast', False),
    (0, '{"reviews": []}', 'grok-4.7-build', False),
])
def test_worker_writes_candidate_only_for_valid_grok_run(tmp_path, monkeypatch, code, text, model, ready):
    config, plan, root = setup(tmp_path)
    inputs = json.loads((root / '000.input.json').read_text())
    if text == 'valid':
        text = json.dumps({'plan_id': plan['id'], 'reviews': [
            {'seq': w['seq'], 'fingerprint': w['fingerprint'], 'correction_id': w.get('correction_id'),
             'investigation_version': 2, 'label': 'speaking', 'music_source': 'none'}
            for w in inputs['windows'] if w['seq'] in inputs['target_seqs']]})
    config['final_subagent'] = finalize.GROK_BACKEND
    monkeypatch.setattr(finalize, 'post', lambda *_: {'pid': 123, 'diagnose_on_failure': False})
    monkeypatch.setattr(finalize, 'stop_group', lambda _: None)
    monkeypatch.setenv('XAI_API_KEY', 'fake-should-not-be-inherited')

    class AgentProcess:
        pid = 123
        returncode = code

        def __init__(self, command, *, stdout, env, **kwargs):
            assert Path(command[0]).name == 'grok'
            assert 'XAI_API_KEY' not in env
            stdout.write(json.dumps({'stopReason': 'end_turn', 'sessionId': 'synthetic',
                                     'modelUsage': {model: {}}, 'text': text}).encode())

        def wait(self, timeout):
            return self.returncode

    monkeypatch.setattr(finalize.subprocess, 'Popen', AgentProcess)
    first = finalize.launch(config, 'finalize', '000')
    result = finalize.worker(config, 'finalize', '000', first['token'])
    assert result['state'] == ('awaiting_coordinator_review' if ready else 'failed')
    assert (root / 'cursor_workers/000.result.json').exists() == ready
