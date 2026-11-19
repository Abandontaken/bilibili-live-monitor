import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from bilibili_live_monitor.app import create_app
from bilibili_live_monitor.automation import Automation
from bilibili_live_monitor.config import Settings
from bilibili_live_monitor.room_config import reload_plan
from bilibili_live_monitor.store import Store


def make(tmp_path, rooms=None):
    store = Store(tmp_path/'monitor.sqlite3')
    platform = SimpleNamespace(store=store, monitor=SimpleNamespace(browser=None))
    return Automation(platform, SimpleNamespace(), {'rooms': rooms or {}})


def test_new_room_defaults_off_and_persisted_switch_survives_restart(tmp_path):
    config = {'123456': {'label': '测试', 'intro': '欢迎'}}
    service = make(tmp_path, config)
    assert service.row('123456')['enabled'] is False
    service.row('123456').update(enabled=True, seen=['viewer'], last_sent=123)
    service.save()
    restored = Automation(service.platform, service.accounts, service.config)
    assert restored.row('123456')['enabled'] is True
    assert restored.row('123456')['seen'] == ['viewer']


@pytest.mark.asyncio
async def test_add_and_reload_preserve_session_and_do_not_duplicate_loop(tmp_path):
    service = make(tmp_path, {'123456': {'label': '旧', 'intro': '欢迎', 'welcome_enabled': True}})
    # A real blocking coroutine lets us check loop ownership.
    async def run():
        await asyncio.Event().wait()
    service.run = run
    service.row('123456').update(seen=['old'], session='old-session', last_sent=123)
    service.workflows.records['old'] = {'url': 'https://example.org/old/'}
    before = deepcopy(service.row('123456'))
    incoming = deepcopy(service.config)
    incoming['rooms']['654321'] = {'label': '新', 'intro': '欢迎'}
    result = await service.reload_rooms(incoming)
    loop = service.loop
    await service.reload_rooms(incoming)
    assert result['added'] == ['654321']
    assert service.loop is loop
    assert service.row('654321')['enabled'] is False
    assert service.row('123456') == before
    assert service.workflows.records['old']['url'].endswith('/old/')
    await service.close()


@pytest.mark.asyncio
async def test_explicit_disable_revokes_inflight_send(tmp_path):
    service = make(tmp_path, {'123456': {'label': '旧', 'intro': '欢迎', 'welcome_enabled': True}})
    service.start = AsyncMock()
    service.row('123456')
    task = asyncio.create_task(asyncio.Event().wait())
    service.tasks['welcome:123456'] = task
    incoming = deepcopy(service.config)
    incoming['rooms']['123456']['welcome_enabled'] = False
    await service.reload_rooms(incoming)
    assert task.cancelled()
    assert service.row('123456')['enabled'] is False
    assert service.row('123456')['revision'] == 1


@pytest.mark.asyncio
async def test_invalid_reload_changes_nothing(tmp_path):
    service = make(tmp_path, {'123456': {'label': '旧', 'intro': '欢迎'}})
    before = deepcopy(service.config)
    for mutate in (
        lambda c: c['rooms'].clear(),
        lambda c: c['rooms']['123456'].update(welcome_enabled='false'),
        lambda c: c.update(notification_command=['changed']),
        lambda c: c.update(session_workflow={'enabled': True}),
        lambda c: c['rooms']['123456'].update(greeting='{bad}{names}'),
    ):
        incoming = deepcopy(before)
        mutate(incoming)
        with pytest.raises(ValueError):
            await service.reload_rooms(incoming)
        assert service.config == before
        assert service.rooms == before['rooms']


@pytest.mark.asyncio
async def test_reload_updates_variants_and_preserves_saved_copy(tmp_path):
    service = make(tmp_path, {'123456': {'label': '旧', 'intro': '欢迎'}})
    service.accounts.summary = lambda: {}
    service.start = AsyncMock()
    incoming = deepcopy(service.config)
    incoming['rooms']['654321'] = {
        'label': '新', 'intro': '介绍',
        'welcome_variants': [{'id': 'first', 'info': '新介绍', 'cta': '新引导'}]}
    await service.reload_rooms(incoming)
    assert service.summary()['rooms'][1]['variant_count'] == 1
    assert service.variants['654321'][0]['id'] == 'first'
    await service.update_welcome_copy('654321', {
        **service.welcome_copy('654321'), 'intro': '界面保存', 'welcome_variants': []})
    await service.reload_rooms(incoming)
    assert service.welcome_copy('654321')['intro'] == '界面保存'
    assert service.variants['654321'] == []
    before = deepcopy(service.rooms)
    broken = deepcopy(incoming)
    broken['rooms']['123456']['welcome_variants'] = [{'id': 'bad'}]
    with pytest.raises(ValueError):
        await service.reload_rooms(broken)
    assert service.rooms == before
    service.store.close()


def test_only_future_publish_mapping_can_change():
    config = {'rooms': {}, 'session_workflow': {'enabled': False, 'publish': {'default': {}}}}
    incoming = deepcopy(config)
    incoming['session_workflow']['publish']['default']['url'] = 'https://example.org/new/'
    assert reload_plan(config, incoming) == incoming
    assert config['session_workflow']['publish']['default'] == {}


@pytest.mark.parametrize(('key', 'value'), [
    ('model', None), ('model', 1), ('model', 'bare-model'), ('model', '/model'),
    ('model', 'provider/'), ('model', ' provider/model'),
    ('final_model', None), ('final_model', 1), ('final_model', 'bare-model'),
    ('final_model', '/model'), ('final_model', 'provider/'), ('final_model', ' provider/model'),
    ('final_subagent', ''), ('final_subagent', None),
    ('final_concurrency', True), ('final_concurrency', 0), ('final_concurrency', 11),
])
@pytest.mark.asyncio
async def test_invalid_model_reload_is_atomic_even_when_disabled(tmp_path, key, value):
    service = make(tmp_path, {'123456': {'label': '测试', 'intro': '欢迎'}})
    service.start = AsyncMock()
    service.config['session_workflow'] = {'enabled': False}
    before = deepcopy(service.config)
    old_workflow = deepcopy(service.workflows.config)
    incoming = deepcopy(before)
    incoming['session_workflow'][key] = value
    with pytest.raises(ValueError):
        await service.reload_rooms(incoming)
    assert service.config == before
    assert service.workflows.config == old_workflow
    service.start.assert_not_called()
    service.store.close()


@pytest.mark.parametrize(('key', 'value'), [
    ('opencode_python', '/changed/python'), ('workspace_root', '/changed/sessions'),
    ('finish_timeout', 123), ('finalize_parallel', False), ('enabled', True),
])
def test_model_reload_does_not_allow_other_workflow_changes(key, value):
    before = {'rooms': {}, 'session_workflow': {'enabled': False}}
    incoming = deepcopy(before)
    incoming['session_workflow'].update(model='new/model', **{key: value})
    with pytest.raises(ValueError, match='需要重启'):
        reload_plan(before, incoming)


def test_reload_api_uses_fixed_local_file(tmp_path):
    path = tmp_path/'automation.json'
    path.write_text('{"rooms":{}}')
    app = create_app(Settings(data_dir=tmp_path, automation_config=str(path)))
    app.state.automation = SimpleNamespace(reload_rooms=AsyncMock(return_value={'accepted': True}))
    client = TestClient(app)
    assert client.post('/api/automation/reload').json() == {'accepted': True}
    path.write_text('{broken')
    assert client.post('/api/automation/reload').status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize('publish', [None, {'rooms': []}, {'default': []},
    {'rooms': {'123456': None}}, {'default': {'url': 123}}, {'default': {'remote': None}}])
async def test_nested_publish_types_fail_before_state_change(tmp_path, publish):
    service = make(tmp_path, {'123456': {'label': '测试', 'intro': '欢迎'}})
    service.config['session_workflow'] = {'enabled': False, 'publish': {}}
    before = deepcopy(service.config)
    incoming = deepcopy(before)
    incoming['session_workflow']['publish'] = publish
    with pytest.raises(ValueError):
        await service.reload_rooms(incoming)
    assert service.config == before
    assert service.rooms == before['rooms']


@pytest.mark.asyncio
async def test_capture_hot_reload_updates_runtime_settings_and_rejects_non_reloadable_fields(tmp_path, monkeypatch):
    import json

    from bilibili_live_monitor.accounts import Accounts
    from bilibili_live_monitor.config import load_settings
    from bilibili_live_monitor.monitor import Monitor
    from bilibili_live_monitor.platform import Candidate, Platform, Policy

    settings = Settings(
        data_dir=tmp_path,
        capture_backend='direct',
        max_rooms=30,
        direct_handshake_interval=1.5,
    )
    # 构造 Settings 后注入宿主干扰环境变量，验证 Automation 基线与 parse_capture_overrides 均不受污染
    monkeypatch.setenv('MAX_ROOMS', '999')
    monkeypatch.setenv('DIRECT_AUDIO_QN', '0')
    monkeypatch.setenv('BABYSITTER_INTERVAL_SECONDS', '-5')
    store = Store(tmp_path / 'monitor.sqlite3')
    monitor = Monitor(settings, store)
    platform = Platform(monitor)
    platform.import_candidates([
        Candidate(url='https://live.bilibili.com/123456'),
        Candidate(url='https://live.bilibili.com/654321'),
    ])
    platform.configure(Policy(
        enabled=True,
        target_rooms=2,
        mode='fixed',
        fixed_rooms=['https://live.bilibili.com/123456', 'https://live.bilibili.com/654321'],
        record_mode='audio',
    ))

    # 初始化时 capture.max_rooms 小于当前 target_rooms 必须拒绝
    too_small_init = {
        'profiles': {'anon': {'mode': 'anonymous', 'label': '匿名'}},
        'active_profile': 'anon',
        'rooms': {'123456': {'label': '测试主播', 'intro': '仅匿名监控', 'welcome_enabled': False}},
        'capture': {'capture_backend': 'direct', 'max_rooms': 1},
    }
    accounts = Accounts(settings, too_small_init)
    with pytest.raises(ValueError, match='target_rooms'):
        Automation(platform, accounts, too_small_init)
    assert settings.max_rooms == 30

    # 初始配置自带动态字段时，基线仍保留显式 Settings(max_rooms=30, direct_handshake_interval=1.5)
    base_config = {
        'profiles': {'anon': {'mode': 'anonymous', 'label': '匿名'}},
        'active_profile': 'anon',
        'rooms': {'123456': {'label': '测试主播', 'intro': '仅匿名监控', 'welcome_enabled': False}},
        'capture': {
            'capture_backend': 'direct',
            'record_mode': 'audio',
            'audio_format': 'opus',
            'audio_bitrate': '32k',
            'direct_handshake_interval': 3.0,
            'max_rooms': 50,
            'asr_enabled': False,
            'preview_enabled': False,
        },
    }
    service = Automation(platform, accounts, base_config)
    service.start = AsyncMock()
    assert settings.max_rooms == 50
    assert settings.direct_handshake_interval == 3.0
    assert monitor.direct.handshake_interval == 3.0

    updated = deepcopy(base_config)
    updated['capture'].update(
        direct_audio_qn=150,
        direct_video_qn=400,
        direct_handshake_interval=1.25,
        audio_bitrate='48k',
        recording_segment_seconds=120,
        max_rooms=60,
    )
    res = await service.reload_rooms(updated)
    assert res['accepted'] is True
    assert res['capture'] == {
        'direct_audio_qn': 150,
        'direct_video_qn': 400,
        'direct_handshake_interval': 1.25,
        'audio_bitrate': '48k',
        'recording_segment_seconds': 120,
        'max_rooms': 60,
    }
    assert settings.direct_audio_qn == 150
    assert settings.direct_video_qn == 400
    assert settings.direct_handshake_interval == 1.25
    assert monitor.direct.handshake_interval == 1.25
    assert settings.audio_bitrate == '48k'
    assert settings.recording_segment_seconds == 120
    assert settings.max_rooms == 60

    # 移除所有动态字段（含初始就存在的 max_rooms 与 direct_handshake_interval）时，回退至显式 Settings 基线
    stripped_cfg = deepcopy(base_config)
    for k in ('max_rooms', 'direct_handshake_interval', 'audio_bitrate'):
        stripped_cfg['capture'].pop(k, None)
    await service.reload_rooms(stripped_cfg)
    assert settings.direct_audio_qn == 80
    assert settings.direct_video_qn == 10000
    assert settings.max_rooms == 30
    assert settings.direct_handshake_interval == 1.5
    assert monitor.direct.handshake_interval == 1.5

    # 非热加载字段、越界值与小于当前 target_rooms 的 max_rooms 均原子拒绝
    snapshot = deepcopy(service.config)
    for bad_patch in (
        {'capture_backend': 'cdp'},
        {'record_mode': 'video'},
        {'asr_enabled': True},
        {'preview_enabled': True},
        {'audio_format': 'mka'},
        {'queue_size': 50},
        {'direct_audio_qn': 0},
        {'direct_audio_qn': True},
        {'audio_bitrate': 'invalid'},
        {'recording_segment_seconds': 5},
        {'max_rooms': 1},
    ):
        broken = deepcopy(stripped_cfg)
        broken['capture'].update(bad_patch)
        with pytest.raises(ValueError):
            await service.reload_rooms(broken)
        assert service.config == snapshot
        assert settings.direct_audio_qn == 80
        assert settings.max_rooms == 30
        assert monitor.direct.handshake_interval == 1.5

    await service.close()
    await platform.close()
    await monitor.close()
    store.close()

    # 验证 load_settings() 入口保留合法环境变量基线并忽略被 automation.capture 覆盖的脏环境变量
    auto_file = tmp_path / 'auto_env.json'
    auto_file.write_text(json.dumps({
        'profiles': {'anon': {'mode': 'anonymous', 'label': '匿名'}},
        'active_profile': 'anon',
        'rooms': {'123456': {'label': '测试主播', 'intro': '仅匿名监控', 'welcome_enabled': False}},
        'capture': {'capture_backend': 'direct', 'direct_audio_qn': 250, 'max_rooms': 80},
    }))
    monkeypatch.setenv('DATA_DIR', str(tmp_path / 'env_data'))
    monkeypatch.setenv('AUTOMATION_CONFIG', str(auto_file))
    monkeypatch.setenv('MAX_ROOMS', '40')
    monkeypatch.setenv('DIRECT_AUDIO_QN', '0')
    monkeypatch.setenv('BABYSITTER_INTERVAL_SECONDS', '300')
    loaded = load_settings()
    assert loaded.max_rooms == 80 and loaded.direct_audio_qn == 250
    store2 = Store(loaded.data_dir / 'monitor.sqlite3')
    mon2 = Monitor(loaded, store2)
    plat2 = Platform(mon2)
    acc2 = Accounts(loaded, json.loads(auto_file.read_text()))
    svc2 = Automation(plat2, acc2, json.loads(auto_file.read_text()))
    svc2.start = AsyncMock()
    await svc2.reload_rooms({
        'profiles': {'anon': {'mode': 'anonymous', 'label': '匿名'}},
        'active_profile': 'anon',
        'rooms': {'123456': {'label': '测试主播', 'intro': '仅匿名监控', 'welcome_enabled': False}},
        'capture': {'capture_backend': 'direct'},
    })
    assert loaded.max_rooms == 40
    assert loaded.direct_audio_qn == 80
    await svc2.close()
    await plat2.close()
    await mon2.close()
    store2.close()
