import asyncio
import base64
import hashlib
import json
import shutil
import sqlite3
import struct
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import brotli
import httpx
import pytest

from bilibili_live_monitor import (
    audience_crm,
    finalize_lifecycle,
    offline_asr,
    session_timeline,
    talk_finalize,
)
from bilibili_live_monitor.accounts import Accounts
from bilibili_live_monitor.app import create_app
from bilibili_live_monitor.automation import Automation
from bilibili_live_monitor.config import Settings
from bilibili_live_monitor.direct_client import (
    DirectClient,
    GuestIdentity,
    MinimalAsyncWebSocket,
    RoomNotLive,
    pack_bili_packet,
    sign_wbi,
)
from bilibili_live_monitor.finalize_context import danmaku_context
from bilibili_live_monitor.live_share import atomic_json, epoch
from bilibili_live_monitor.live_share_control import load_config
from bilibili_live_monitor.monitor import Monitor
from bilibili_live_monitor.platform import Candidate, Platform, Policy
from bilibili_live_monitor.recording import Recording, command
from bilibili_live_monitor.room_status import guard_snapshot, room_statuses
from bilibili_live_monitor.runtime import run_session
from bilibili_live_monitor.session_offline import recording_files, run_offline, verify_offline
from bilibili_live_monitor.store import Store
from bilibili_live_monitor.transcript_corrections import correction_context
from bilibili_live_monitor.warehouse import open_warehouse
from bilibili_live_monitor.warehouse_protocol import uid_crc32, uid_md5

SYNTHETIC_IMG_KEY = '7cd084941338484aae1ad9425b84077c'
SYNTHETIC_SUB_KEY = '4932caff0ff746eab6f01bf08b70ac45'
WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'


def _ws_server_frame(payload: bytes, *, opcode: int = 0x2, fin: bool = True) -> bytes:
    first = (0x80 if fin else 0x00) | (opcode & 0x0F)
    header = bytearray([first])
    length = len(payload)
    if length < 126:
        header.append(length)
    elif length < 65536:
        header.append(126)
        header.extend(struct.pack('>H', length))
    else:
        header.append(127)
        header.extend(struct.pack('>Q', length))
    return bytes(header) + payload


async def _ws_read_client_frame(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    b1, b2 = await reader.readexactly(2)
    opcode = b1 & 0x0F
    masked = bool(b2 & 0x80)
    assert masked, 'RFC 6455 客户端发送帧必须携带掩码'
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack('>H', await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack('>Q', await reader.readexactly(8))[0]
    mask_key = await reader.readexactly(4)
    raw = await reader.readexactly(length)
    data = bytes(b ^ mask_key[i & 3] for i, b in enumerate(raw))
    return opcode, data


async def _ws_accept_handshake(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> dict[str, str]:
    req_line = (await reader.readline()).decode('latin1').strip()
    assert req_line.startswith('GET /sub HTTP/1.1')
    headers: dict[str, str] = {}
    while True:
        line = await reader.readline()
        if line in (b'\r\n', b'\n', b''):
            break
        k, v = line.decode('latin1').strip().split(':', 1)
        headers[k.strip().lower()] = v.strip()
    accept = base64.b64encode(
        hashlib.sha1((headers['sec-websocket-key'] + WS_GUID).encode('ascii')).digest()
    ).decode('ascii')
    resp = (
        'HTTP/1.1 101 Switching Protocols\r\n'
        'Upgrade: websocket\r\n'
        'Connection: Upgrade\r\n'
        f'Sec-WebSocket-Accept: {accept}\r\n\r\n'
    )
    writer.write(resp.encode('ascii'))
    await writer.drain()
    return headers


def test_settings_defaults_and_direct_constraints(tmp_path):
    cdp_default = Settings(data_dir=tmp_path)
    assert cdp_default.capture_backend == 'cdp'
    assert cdp_default.audio_format == 'wav'

    direct_default = Settings(data_dir=tmp_path, capture_backend='direct')
    assert direct_default.capture_backend == 'direct'
    assert direct_default.audio_format == 'opus'
    assert direct_default.audio_bitrate == '32k'
    assert direct_default.direct_handshake_interval == 2.5

    with pytest.raises(ValueError, match='opus或mka'):
        Settings(data_dir=tmp_path, capture_backend='direct', audio_format='wav')
    with pytest.raises(ValueError, match='离线转写'):
        Settings(data_dir=tmp_path, capture_backend='direct', asr_enabled=True)
    with pytest.raises(ValueError, match='缩略图预览'):
        Settings(data_dir=tmp_path, capture_backend='direct', preview_enabled=True)
    with pytest.raises(ValueError, match='采集后端'):
        Settings(data_dir=tmp_path, capture_backend='invalid')


def test_wbi_signature_vector_and_special_char_stripping():
    signed = sign_wbi(
        {'id': 123456, 'type': 0, 'note': "a!b'c(d)e*f"},
        SYNTHETIC_IMG_KEY,
        SYNTHETIC_SUB_KEY,
        now=1700000000,
    )
    assert signed['wts'] == '1700000000'
    assert signed['note'] == 'abcdef'
    assert len(signed['w_rid']) == 32
    # 相同输入确定性一致，修改参数后签名变化
    same = sign_wbi(
        {'type': 0, 'id': 123456, 'note': 'abcdef'},
        SYNTHETIC_IMG_KEY,
        SYNTHETIC_SUB_KEY,
        now=1700000000,
    )
    other = sign_wbi({'id': 654321, 'type': 0}, SYNTHETIC_IMG_KEY, SYNTHETIC_SUB_KEY, now=1700000000)
    assert signed['w_rid'] == same['w_rid']
    assert signed['w_rid'] != other['w_rid']


@pytest.mark.asyncio
async def test_guest_cache_refresh_and_handshake_rate_limit():
    spi_calls = 0
    dm_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal spi_calls, dm_calls
        url = str(request.url)
        if '/x/frontend/finger/spi' in url:
            spi_calls += 1
            return httpx.Response(200, json={
                'code': 0,
                'data': {'b_3': f'synthetic-buvid3-{spi_calls}', 'b_4': 'synthetic-buvid4'},
            })
        if '/x/web-interface/nav' in url:
            return httpx.Response(200, json={
                'code': -101,
                'data': {
                    'isLogin': False,
                    'wbi_img': {
                        'img_url': f'https://i0.hdslb.com/bfs/wbi/{SYNTHETIC_IMG_KEY}.png',
                        'sub_url': f'https://i0.hdslb.com/bfs/wbi/{SYNTHETIC_SUB_KEY}.png',
                    },
                },
            })
        if '/getDanmuInfo' in url:
            dm_calls += 1
            if dm_calls == 1:
                return httpx.Response(200, json={'code': -352, 'message': 'stale wbi'})
            return httpx.Response(200, json={
                'code': 0,
                'data': {
                    'token': 'synthetic-token',
                    'host_list': [{'host': '127.0.0.1', 'wss_port': 2245}],
                },
            })
        raise AssertionError(f'unexpected url: {url}')

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = DirectClient(handshake_interval=0.08, http_client=http)
        g1 = await client.ensure_guest()
        g2 = await client.ensure_guest()
        assert g1 == g2 and spi_calls == 1

        # getDanmuInfo 首次遇 -352 自动强制刷新一次游客身份并成功恢复
        guest, token, hosts = await client.fetch_danmu_info(123456)
        assert spi_calls == 2 and dm_calls == 2
        assert guest.buvid3 == 'synthetic-buvid3-2'
        assert token == 'synthetic-token' and hosts[0]['wss_port'] == 2245

        # 验证全局建连限速锁严格串行间隔
        stamps: list[float] = []

        async def grab():
            await client.acquire_handshake_slot()
            stamps.append(time.monotonic())

        await asyncio.gather(grab(), grab(), grab())
        assert stamps[1] - stamps[0] >= 0.07
        assert stamps[2] - stamps[1] >= 0.07


@pytest.mark.asyncio
async def test_rfc6455_fragmentation_ping_pong_and_store_pipeline(tmp_path):
    pings_received_by_server: list[bytes] = []
    heartbeats_received = 0
    auth_received: dict = {}
    server_done = asyncio.Event()

    danmu_pkt = {
        'cmd': 'DANMU_MSG',
        'info': [
            [0, 1, 25, 16777215, 1893456005000, 0, 0, 'a1b2c3d4', 0, 0, 0, '', 0, '{}', '{}', {'user': {'base': {'name': '甲***'}}}],
            '合成直连弹幕测试',
            [0, '甲***', 0, 0, 0, 10000, 1, ''],
            [],
        ],
    }
    inner_op5 = pack_bili_packet(5, json.dumps(danmu_pkt, ensure_ascii=False).encode('utf-8'), protover=0)
    compressed_op5 = pack_bili_packet(5, brotli.compress(inner_op5), protover=3)

    async def handle_ws(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        nonlocal heartbeats_received, auth_received
        try:
            await _ws_accept_handshake(reader, writer)
            op, payload = await _ws_read_client_frame(reader)
            assert op == 0x2
            size, hdr, _, bop, _ = struct.unpack_from('>IHHII', payload, 0)
            assert bop == 7
            auth_received = json.loads(payload[hdr:size].decode('utf-8'))

            # 返回 Op8 code=0
            writer.write(_ws_server_frame(pack_bili_packet(8, b'{"code":0}')))
            await writer.drain()

            # 读取客户端立即发送的 Op2 心跳
            op2, hb_payload = await _ws_read_client_frame(reader)
            assert op2 == 0x2 and struct.unpack_from('>IHHII', hb_payload, 0)[3] == 2
            heartbeats_received += 1

            # 将 Brotli 压缩 Op5 拆成两片发送，并在中间穿插一个 RFC 6455 Ping 控制帧！
            mid = len(compressed_op5) // 2
            writer.write(_ws_server_frame(compressed_op5[:mid], opcode=0x2, fin=False))
            writer.write(_ws_server_frame(b'keepalive-ping', opcode=0x9, fin=True))
            writer.write(_ws_server_frame(compressed_op5[mid:], opcode=0x0, fin=True))
            await writer.drain()

            # 读取客户端自动回发的 Pong 控制帧
            pong_op, pong_data = await _ws_read_client_frame(reader)
            if pong_op == 0xA:
                pings_received_by_server.append(pong_data)

            # 发送服务端 Close 帧结束连接
            writer.write(_ws_server_frame(struct.pack('>H', 1000) + b'normal', opcode=0x8, fin=True))
            await writer.drain()
            server_done.set()
        finally:
            writer.close()

    server = await asyncio.start_server(handle_ws, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]

    def http_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if '/x/frontend/finger/spi' in url:
            return httpx.Response(200, json={'code': 0, 'data': {'b_3': 'guest-b3', 'b_4': 'guest-b4'}})
        if '/x/web-interface/nav' in url:
            return httpx.Response(200, json={
                'code': -101,
                'data': {
                    'isLogin': False,
                    'wbi_img': {
                        'img_url': f'https://i0.hdslb.com/bfs/wbi/{SYNTHETIC_IMG_KEY}.png',
                        'sub_url': f'https://i0.hdslb.com/bfs/wbi/{SYNTHETIC_SUB_KEY}.png',
                    },
                },
            })
        if '/getDanmuInfo' in url:
            return httpx.Response(200, json={
                'code': 0,
                'data': {
                    'token': 'tok-123',
                    'host_list': [{'host': '127.0.0.1', 'wss_port': port, 'use_tls': False}],
                },
            })
        raise AssertionError(url)

    store = Store(tmp_path / 'monitor.sqlite3')
    store.nickname_identity = True
    try:
        async with server, httpx.AsyncClient(transport=httpx.MockTransport(http_handler)) as http:
            client = DirectClient(handshake_interval=0.0, http_client=http)
            with pytest.raises(EOFError, match='code=1000'):
                await client.stream_danmaku(
                    123456,
                    lambda msg: store.live_message('123456', {**msg, 'session_id': 'gen1'}),
                    heartbeat_seconds=5.0,
                )
            await asyncio.wait_for(server_done.wait(), 2.0)

        assert auth_received == {
            'uid': 0,
            'roomid': 123456,
            'protover': 3,
            'platform': 'web',
            'type': 2,
            'buvid': 'guest-b3',
            'key': 'tok-123',
        }
        assert heartbeats_received == 1
        assert pings_received_by_server == [b'keepalive-ping']

        raw_count = store.db.execute("SELECT COUNT(*) FROM raw_events WHERE room_id='123456' AND command='DANMU_MSG'").fetchone()[0]
        assert raw_count == 1
        items = store.items('123456')
        assert len(items) == 1
        assert items[0]['text'] == '合成直连弹幕测试'
        assert items[0]['author'] == '甲***'
        assert items[0]['analysis_identity_basis'] == 'nickname'
        health = store.control('message_health', {})['123456']
        assert health['raw_events'] == 1 and health['state'] == 'disconnected'
    finally:
        store.close()


@pytest.mark.asyncio
async def test_short_room_id_mapping_status_guard_and_workflow_compatibility(tmp_path):
    def http_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if '/getRoomBaseInfo' in url:
            return httpx.Response(200, json={
                'code': 0,
                'data': {
                    'by_room_ids': {
                        '123456': {
                            'room_id': 123456,
                            'short_id': 654,
                            'uid': 999001,
                            'live_status': 1,
                            'live_time': '2030-01-01 08:00:00',
                            'title': '合成短号房间',
                        }
                    }
                },
            })
        if '/room_init' in url:
            return httpx.Response(200, json={
                'code': 0,
                'data': {
                    'room_id': 123456,
                    'short_id': 654,
                    'uid': 999001,
                    'live_status': 1,
                    'live_time': 1893456000,
                },
            })
        if '/Room/get_info' in url:
            return httpx.Response(200, json={
                'code': 0,
                'data': {
                    'room_id': 123456,
                    'short_id': 654,
                    'live_time': '2030-01-01 08:00:00',
                    'title': '合成短号房间',
                    'user_cover': 'https://example.org/cover.jpg',
                },
            })
        if '/guardTab/topList' in url:
            return httpx.Response(200, json={
                'code': 0,
                'data': {
                    'info': {'num': 1, 'page': 1},
                    'top3': [{
                        'guard_level': 3,
                        'uinfo': {'uid': 888001, 'base': {'name': '合成舰长', 'face': 'https://i0.hdslb.com/bfs/face/' + 'a' * 40 + '.jpg'}},
                    }],
                    'list': [],
                },
            })
        raise AssertionError(url)

    store = Store(tmp_path / 'monitor.sqlite3')
    settings = Settings(data_dir=tmp_path, capture_backend='direct', direct_handshake_interval=0.0)
    monitor = Monitor(settings, store)
    async with httpx.AsyncClient(transport=httpx.MockTransport(http_handler)) as http:
        monitor.direct = DirectClient(handshake_interval=0.0, http_client=http)
        # 免 CDP 查询状态与大航海快照
        statuses = await room_statuses(monitor.direct, [{'url': 'https://live.bilibili.com/654'}])
        assert statuses['654']['room_id'] == 123456
        assert statuses['123456']['live_time'] == '2030-01-01 08:00:00'

        snap = await guard_snapshot(monitor.direct, '123456', '999001')
        assert snap['complete'] is True and snap['reported_total'] == 1
        assert snap['members'][0]['sender_uid'] == '888001'
        assert snap['members'][0]['username'] == '合成舰长'

        # 验证 Platform + Monitor.add + SessionWorkflows 短号绑定且不开启实时 ASR
        async def fake_capture_direct(room, generation):
            await asyncio.sleep(10)

        monitor.capture_direct = fake_capture_direct
        platform = Platform(monitor)
        platform.import_candidates([Candidate(url='https://live.bilibili.com/123456')])
        platform.configure(Policy(enabled=True, target_rooms=1, mode='fixed',
                                  fixed_rooms=['https://live.bilibili.com/123456'], record_mode='none'))
        accounts = Accounts(settings, {'profiles': {'anon': {'mode': 'anonymous', 'label': '匿名'}}, 'active_profile': 'anon'})
        await accounts.start()
        automation = Automation(platform, accounts, {
            'rooms': {'123456': {'label': '合成主播', 'intro': '欢迎'}},
            'session_workflow': {
                'enabled': True,
                'workspace_root': str(tmp_path / 'sessions'),
                'opencode_python': '/synthetic/python',
                'opencode_cwd': '/synthetic',
                'opencode_project': '/synthetic/project',
                'warehouse_database': str(tmp_path / 'warehouse.sqlite3'),
                'model': 'qwen38/qwen3.8-27b',
                'final_model': 'qwen38/qwen3.8-27b',
                'final_subagent': 'general',
                'publish': {'default': {'remote': 'test:/share/', 'url': 'https://example.org/share/'}},
            },
        })
        await platform.refresh_statuses()
        candidate = platform.candidates['https://live.bilibili.com/123456']
        assert candidate['room_id'] == '123456'
        assert platform.capture_options(candidate)['asr_enabled'] is False

        await platform.launch(candidate)
        assert platform.active['https://live.bilibili.com/123456'] == '654'
        room_row = next(r for r in store.rooms() if r['room_id'] == '654')
        assert room_row['real_room_id'] == '123456'
        assert room_row['source_url'] == 'https://live.bilibili.com/123456'
        assert room_row['asr_enabled'] is False

        record = automation.workflows.prepare('123456', candidate)
        cfg_path = automation.workflows.initialize(record)
        cfg = load_config(cfg_path)
        assert cfg['room_id'] == '654'
        assert cfg['platform_room_id'] == '123456'
        assert cfg['recording_root'] == str(tmp_path / 'recordings/654')
        assert cfg['warehouse_database'] == str(tmp_path / 'warehouse.sqlite3')

        await monitor.stop('654')
        await automation.close()
        await platform.close()
    await monitor.close()
    store.close()


@pytest.mark.parametrize('live_status', [0, 2])
async def test_stream_offline_is_a_broadcast_state_even_with_replay_url(live_status):
    def handler(request):
        return httpx.Response(200, json={'code': 0, 'data': {
            'live_status': live_status, 'playurl_info': {'playurl': {'stream': []}},
        }})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        direct = DirectClient(http_client=http)
        direct.ensure_guest = AsyncMock(return_value=GuestIdentity('guest3', 'guest4', '', '', 0))
        with pytest.raises(RoomNotLive) as error:
            await direct.fetch_stream_url(123456)
        assert error.value.live_status == live_status


@pytest.mark.asyncio
async def test_direct_room_statuses_retries_once_on_transient_500_and_guard_classifies_errors(monkeypatch):
    from bilibili_live_monitor.rotation import RoomStatusRefused

    status_calls = 0
    guard_code = -500

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal status_calls
        url = str(request.url)
        if '/getRoomBaseInfo' in url:
            status_calls += 1
            if status_calls == 1:
                return httpx.Response(200, json={'code': -500, 'message': '服务调用超时'})
            return httpx.Response(200, json={
                'code': 0,
                'data': {'by_room_ids': {'123456': {'room_id': 123456, 'uid': 999001, 'live_status': 0}}},
            })
        if '/guardTab/topList' in url:
            return httpx.Response(200, json={'code': guard_code, 'message': 'err'})
        raise AssertionError(url)

    slept = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr('bilibili_live_monitor.direct_client.asyncio.sleep', fake_sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        direct = DirectClient(handshake_interval=0.0, http_client=http)
        result = await direct.room_statuses([{'url': 'https://live.bilibili.com/123456'}])
        assert status_calls == 2 and slept == [2.0]
        assert result['123456']['live_status'] == 0

        with pytest.raises(RuntimeError) as err_500:
            await direct.guard_snapshot('123456', '999001')
        assert not isinstance(err_500.value, RoomStatusRefused)

        guard_code = -412
        with pytest.raises(RoomStatusRefused):
            await direct.guard_snapshot('123456', '999001')


@pytest.mark.parametrize('live_status', [0, 2])
async def test_capture_direct_offline_does_not_exhaust_retry_budget(tmp_path, live_status):
    store = Store(tmp_path / 'monitor.sqlite3')
    monitor = Monitor(Settings(data_dir=tmp_path, capture_backend='direct'), store)
    monitor.direct.acquire_handshake_slot = AsyncMock()
    monitor.direct.fetch_danmu_info = AsyncMock(return_value=None)
    monitor.direct.fetch_stream_url = AsyncMock(side_effect=RoomNotLive(live_status))
    room = {'room_id': '123456', 'real_room_id': '123456', 'record_mode': 'audio',
            'reconnect_attempts': 19, 'status': 'capturing'}
    store.room(room)
    monitor.generations['123456'] = 'synthetic'
    task = asyncio.create_task(monitor.capture_direct(room, 'synthetic'))
    monitor.tasks['123456'] = task
    await asyncio.wait_for(task, 2)
    saved = store.rooms()[0]
    assert saved['status'] == 'stopped' and saved['error'] is None
    assert saved['reconnect_attempts'] == 0 and saved['next_retry_utc'] is None
    assert saved['capture_live_status'] == live_status
    assert monitor.direct.fetch_stream_url.await_count == 1 and not monitor.tasks
    await monitor.close()
    store.close()


@pytest.mark.asyncio
async def test_direct_mode_never_touches_chrome_or_interactive_actions(tmp_path, monkeypatch):
    settings = Settings(data_dir=tmp_path, capture_backend='direct')
    # Accounts 拒绝实名 profile
    with pytest.raises(ValueError, match='anonymous'):
        Accounts(settings, {'profiles': {'acc': {'mode': 'account', 'label': '实名'}}, 'active_profile': 'acc'})

    accounts = Accounts(settings, {'profiles': {'anon': {'mode': 'anonymous', 'label': '匿名'}}, 'active_profile': 'anon'})
    await accounts.start()
    assert accounts.process is None and accounts.anonymous is True
    browser_probe = SimpleNamespace(connect=AsyncMock(side_effect=AssertionError('direct 不得连接 CDP')))
    summary = await accounts.refresh(browser_probe)
    assert summary['mode'] == 'anonymous' and summary['logged_in'] is False

    # runtime.run_session 在 direct 模式下仅启动 backend，不检查或启动 Chrome
    owned_calls = []
    monkeypatch.setattr('bilibili_live_monitor.runtime.socket.create_connection',
                        lambda *a, **kw: (_ for _ in ()).throw(OSError('free')))
    monkeypatch.setattr('bilibili_live_monitor.runtime.run_owned',
                        lambda cmds, directory, deadline, **kw: owned_calls.append(cmds) or {'status': 'stopped'})
    run_session(settings, '2030-01-01T00:00:00+00:00')
    assert len(owned_calls) == 1
    assert [label for label, _ in owned_calls[0]] == ['backend']

    # FastAPI 接口在 direct 模式下拒绝启用 welcome 和 likes
    auto_cfg = tmp_path / 'automation.json'
    auto_cfg.write_text(json.dumps({
        'profiles': {'anon': {'mode': 'anonymous', 'label': '匿名'}},
        'active_profile': 'anon',
        'rooms': {'123456': {'label': '合成主播', 'intro': '欢迎'}},
    }))
    from fastapi.testclient import TestClient
    with TestClient(create_app(Settings(data_dir=tmp_path / 'app_data', capture_backend='direct',
                                        automation_config=str(auto_cfg)))) as client:
        r_welcome = client.put('/api/automation/rooms/123456', json={'enabled': True})
        assert r_welcome.status_code == 409 and 'direct' in r_welcome.json()['detail']
        r_likes = client.post('/api/automation/rooms/123456/likes', json={'count': 5, 'request_id': 'req1'})
        assert r_likes.status_code == 409 and 'direct' in r_likes.json()['detail']


def test_opus_command_builder(tmp_path):
    args = command(
        'https://cdn.bilivideo.com/live-bvc/123456.flv?token=synthetic',
        'https://live.bilibili.com/123456',
        'audio',
        tmp_path / 'seg-%05d.opus',
        300,
        audio_format='opus',
        audio_bitrate='32k',
    )
    assert args[args.index('-map') + 1] == '0:a:0'
    assert '-vn' in args
    assert args[args.index('-ac') + 1] == '1'
    assert args[args.index('-c:a') + 1] == 'libopus'
    assert args[args.index('-b:a') + 1] == '32k'
    assert args[args.index('-vbr') + 1] == 'on'
    assert args[args.index('-segment_format') + 1] == 'ogg'
    header_arg = args[args.index('-headers') + 1]
    assert 'Referer: https://live.bilibili.com/123456\r\n' in header_arg
    assert 'User-Agent:' in header_arg


@pytest.mark.skipif(not shutil.which('ffmpeg'), reason='需要 ffmpeg 验证真实 .opus 分段与离线转写链路')
@pytest.mark.asyncio
async def test_real_opus_segmented_recording_to_offline_asr_pipeline(tmp_path, monkeypatch):
    source_media = tmp_path / 'source.mkv'
    subprocess.run([
        'ffmpeg', '-v', 'error',
        '-f', 'lavfi', '-i', 'color=size=64x64:rate=5',
        '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000',
        '-t', '13', '-c:v', 'mpeg4', '-c:a', 'aac', str(source_media),
    ], check=True, timeout=20)

    settings = Settings(data_dir=tmp_path, capture_backend='direct', recording_segment_seconds=10)
    room = {'room_id': '123456', 'url': 'https://live.bilibili.com/123456', 'record_mode': 'audio'}
    recorder = Recording(settings, room)
    first_part_ts = int(recorder.prefix.split('-')[1][:4])  # 确保分段名合法
    assert first_part_ts >= 2026

    await recorder.start(str(source_media))
    await asyncio.wait_for(recorder.process.wait(), 15)
    await recorder.close()
    assert recorder.process.returncode == 0

    rec_dir = tmp_path / 'recordings' / '123456'
    opus_files = sorted(rec_dir.glob('*.opus'))
    assert len(opus_files) == 2
    snap = recorder.snapshot()
    assert snap['recording_files'] == 2
    assert snap['recording_bytes'] == sum(p.stat().st_size for p in opus_files)
    assert snap['recorded_seconds'] >= 12.5

    # 验证 offline_asr.probe 与 16kHz PCM 解码对 RFC 7845 48kHz 时间基 .opus 完全兼容
    total_probed = 0.0
    for f in opus_files:
        probed = offline_asr.probe(rec_dir, f)
        assert probed['sealed'] == 'sealed_readable'
        assert probed['audio_codec'] == 'opus'
        assert probed['duration_sec'] > 0
        total_probed += probed['duration_sec']
        pcm = offline_asr.decode_pcm(f)
        assert len(pcm) > 16000 * 4  # 至少 >1 秒 f32le 样本
    assert 12.8 <= total_probed <= 13.5

    # 验证 session_offline 识别 .opus 分片并跑通 run_offline + verify_offline 闭环
    stamp = recorder.prefix.split('-')[1]
    start_dt = f'{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}T{stamp[9:11]}:{stamp[11:13]}:{stamp[13:15]}+00:00'
    cutoff = epoch(start_dt) + 30
    workspace = tmp_path / 'session_ws'
    workspace.mkdir()
    session_cfg = {
        'workspace': str(workspace),
        'recording_root': str(rec_dir),
        'start_utc': start_dt,
        'recording_segment_seconds': 10,
        'room_id': '123456',
        'asr_python': 'synthetic',
        'asr_model': 'Qwen/Qwen3-ASR-1.7B',
    }
    groups = recording_files(session_cfg, cutoff)
    assert len(groups) == 1 and len(next(iter(groups.values()))) == 2

    class FakeWorker:
        def __init__(self, *a):
            self.model = 'Qwen/Qwen3-ASR-1.7B'

        def start(self):
            pass

        def close(self):
            pass

        def request(self, wav_path, req_id):
            assert Path(wav_path).stat().st_size > 44
            return {'text': '合成Opus离线转写文本', 'infer_ms': 12}

    monkeypatch.setattr(offline_asr, 'Worker', FakeWorker)
    seg_path = run_offline(session_cfg, {'cutoff': cutoff})
    session_cfg['offline_segments'] = seg_path
    marker = verify_offline(session_cfg, cutoff)
    assert marker['files'] == 2 and marker['segments'] == 2
    segments = json.loads(Path(seg_path).read_text())
    assert [s['text'] for s in segments] == ['合成Opus离线转写文本', '合成Opus离线转写文本']


def _make_danmu_raw_and_item(record_id: int, at_utc: str, masked_author: str, text: str,
                             uid_for_crc: int | None, face_hex: str | None = None, *, mystery: bool = False):
    crc_hex = uid_crc32(uid_for_crc) if uid_for_crc else '00000000'
    face_url = f'https://i0.hdslb.com/bfs/face/{face_hex}.jpg' if face_hex else 'https://i0.hdslb.com/bfs/face/member/noface.jpg'
    u15 = {'user': {'base': {'name': '神秘人' if mystery else masked_author, 'face': face_url, 'is_mystery': mystery}}}
    info = [
        [0, 1, 25, 16777215, int(epoch(at_utc) * 1000), 0, 0, crc_hex, 0, 0, 0, '', 0, '{}', '{}', u15],
        text,
        [0, '神秘人' if mystery else masked_author, 0, 0, 0, 10000, 1, ''],
        [],
    ]
    source_key = hashlib.sha256(json.dumps(info, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    raw_row = (at_utc, 'DANMU_MSG', {'cmd': 'DANMU_MSG', 'info': info})
    item_row = ('danmaku', at_utc, {
        '_record_id': record_id,
        'author': '神秘人' if mystery else masked_author,
        'text': text,
        'source_key': source_key,
        'sender_hidden': mystery,
        'sender_uid': None,
        'identity_status': 'unresolved',
    })
    return raw_row, item_row


def test_finalize_context_resolves_same_session_and_warehouse_names_and_passes_talk_finalize(tmp_path, monkeypatch):
    # 1. 创建源库 monitor.sqlite3 与只读数仓 warehouse.sqlite3
    monitor_db = tmp_path / 'monitor.sqlite3'
    with sqlite3.connect(monitor_db) as db:
        db.execute('CREATE TABLE sentinel(v TEXT)')
        db.execute("INSERT INTO sentinel VALUES('untouched')")

    wh_db = tmp_path / 'warehouse.sqlite3'
    uid_session = 100001  # 同场 ENTRY_EFFECT 露面：真名 "甲壳虫乐迷"
    uid_warehouse = 200002  # 仅数仓跨场露面：真名 "乙太旅行者"
    uid_face_only = 300003  # 改名老观众（仅唯一头像 + CRC32 匹配）：真名 "新名字老熟人"
    face_unique = 'b' * 40

    with open_warehouse(wh_db) as wdb:
        wdb.execute(
            '''INSERT INTO dim_users(uid, uid_md5, uid_crc32, latest_uname, first_char, latest_uname_ts,
               max_wealth_level, first_seen_ts, last_seen_ts) VALUES (?,?,?,?,?,?,?,?,?)''',
            (uid_warehouse, uid_md5(uid_warehouse), uid_crc32(uid_warehouse), '乙太旅行者', '乙', 100.0, 0, 100.0, 100.0),
        )
        wdb.execute(
            '''INSERT INTO dim_users(uid, uid_md5, uid_crc32, latest_uname, first_char, latest_uname_ts,
               max_wealth_level, first_seen_ts, last_seen_ts) VALUES (?,?,?,?,?,?,?,?,?)''',
            (uid_face_only, uid_md5(uid_face_only), uid_crc32(uid_face_only), '新名字老熟人', '新', 100.0, 0, 100.0, 100.0),
        )
        wdb.execute(
            'INSERT INTO dim_user_faces(face_sha1, uid, first_seen_ts, last_seen_ts) VALUES (?,?,?,?)',
            (face_unique, uid_face_only, 100.0, 100.0),
        )
        wdb.commit()
    wh_sha_before = hashlib.sha256(wh_db.read_bytes()).hexdigest()

    raw1, item1 = _make_danmu_raw_and_item(1, '2030-01-01T00:00:05Z', '甲***', '主播晚上好', uid_session)
    raw2, item2 = _make_danmu_raw_and_item(2, '2030-01-01T00:00:10Z', '乙***', '求讲一下合成器', uid_warehouse)
    raw3, item3 = _make_danmu_raw_and_item(3, '2030-01-01T00:00:15Z', '旧***', '我改名啦', uid_face_only, face_hex=face_unique)
    raw4, item4 = _make_danmu_raw_and_item(4, '2030-01-01T00:00:20Z', '神秘人', '隐藏发言', 400004, mystery=True)
    raw5, item5 = _make_danmu_raw_and_item(5, '2030-01-01T00:00:25Z', '丁***', '未露面新观众', 500005)

    entry_raw = ('2030-01-01T00:00:02Z', 'ENTRY_EFFECT', {
        'cmd': 'ENTRY_EFFECT',
        'data': {'uid': uid_session, 'copy_writing': '欢迎 <%甲壳虫乐迷%> 进入直播间'},
    })

    segments = [{
        'seq': 1,
        'start_utc': '2030-01-01T00:00:01Z',
        'end_utc': '2030-01-01T00:00:30Z',
        'text': '欢迎假壳虫乐迷和以太旅行者，还有新名字老熟人！',
        'is_final': True,
    }]
    source = {
        'raw': [entry_raw, raw1, raw2, raw3, raw4, raw5],
        'items': [item1, item2, item3, item4, item5],
        'segments': segments,
    }

    # 直接测试 danmaku_context 双层还原
    ctx = danmaku_context(source, epoch('2030-01-01T00:00:00Z'), epoch('2030-01-01T00:01:00Z'),
                          warehouse_database=wh_db)
    assert [row['name'] for row in ctx] == ['甲壳虫乐迷', '乙太旅行者', '新名字老熟人', '神秘人', '丁***']
    assert hashlib.sha256(wh_db.read_bytes()).hexdigest() == wh_sha_before

    # 测试 OpenCode 原生 subagent ('general' + 'qwen38/qwen3.8-27b') 下 talk_finalize 全链路闭环
    workspace = tmp_path / 'talk_ws'
    workspace.mkdir()
    config = {
        'workspace': str(workspace),
        '_config_path': str(workspace / 'config.json'),
        'database': str(monitor_db),
        'warehouse_database': str(wh_db),
        'room_id': '123456',
        'workflow_key': 'synthetic_talk_wf',
        'start_utc': '2030-01-01T00:00:00Z',
        'content_mode': 'talk',
        'talk_finalize': True,
        'model': 'qwen38/qwen3.8-27b',
        'final_model': 'qwen38/qwen3.8-27b',
        'final_subagent': 'general',
        'final_concurrency': 4,
        'opencode_python': '/synthetic/python',
        'opencode_cwd': str(tmp_path),
    }
    monkeypatch.setattr(talk_finalize, 'source_at_cutoff',
                        lambda cfg: ({}, epoch('2030-01-01T00:05:00Z'), source))

    dispatched = []
    monkeypatch.setattr('bilibili_live_monitor.session_finalize.subprocess.run',
                        lambda cmd, **kw: dispatched.append(cmd) or subprocess.CompletedProcess(cmd, 0, '{}', ''))
    talk_finalize.submit(config, 'opencode-session-1')
    assert dispatched[0][dispatched[0].index('--model') + 1] == 'qwen38/qwen3.8-27b'
    prompt_text = (workspace / 'talk_finalize/prompt.md').read_text()
    assert '再用task分发给general，最多同时4片。' in prompt_text
    assert 'cursor_finalize' not in prompt_text

    # 验证 OpenCode 模式下 supervise 续跑提示词不出现 Cursor accept/CLI回执
    monkeypatch.setattr(finalize_lifecycle, 'ai_control', lambda *a, **kw: {'type': 'idle'})
    monkeypatch.setattr(finalize_lifecycle.subprocess, 'run',
                        lambda *a, **kw: SimpleNamespace(returncode=0, stdout='{}', stderr=''))
    sup_state = {}
    finalize_lifecycle.supervise(config, sup_state, 'opencode-session-1', workspace / 'talk_finalize', 0)
    finalize_lifecycle.supervise(config, sup_state, 'opencode-session-1', workspace / 'talk_finalize', 300)
    resume_text = (workspace / 'talk_finalize/resume_prompt.md').read_text()
    assert 'cursor_finalize' not in resume_text and 'CLI回执' not in resume_text
    assert 'talk_finalize finish' in resume_text

    # 模拟 OpenCode 分片直接写入 000.result.json，引用还原后的真名作为纠错证据
    plan = json.loads((workspace / 'talk_finalize/plan.json').read_text())
    shard_in = json.loads(Path(plan['shards'][0]['input']).read_text())
    assert [m['name'] for m in shard_in['danmaku'][:3]] == ['甲壳虫乐迷', '乙太旅行者', '新名字老熟人']
    atomic_json(Path(plan['shards'][0]['result']), {
        'plan_id': plan['id'],
        'reviews': [{
            'seq': 1,
            'fingerprint': shard_in['windows'][0]['fingerprint'],
            'corrected_text': '欢迎甲壳虫乐迷和乙太旅行者，还有新名字老熟人！',
            'reason': '依据同场与数仓实名弹幕修正观众昵称同音字',
            'confidence': 'high',
            'evidence': [
                {'id': 'm:1', 'quote': '甲壳虫乐迷'},
                {'id': 'm:2', 'quote': '乙太旅行者'},
                {'id': 'm:3', 'quote': '新名字老熟人'},
            ],
        }],
    })

    # 模拟在 prepare 之后、finish/complete 之前，后台 warehouse sync 把原本未露面的 500005 也写进了数仓！
    with open_warehouse(wh_db) as wdb:
        wdb.execute(
            '''INSERT INTO dim_users(uid, uid_md5, uid_crc32, latest_uname, first_char, latest_uname_ts,
               max_wealth_level, first_seen_ts, last_seen_ts) VALUES (?,?,?,?,?,?,?,?,?)''',
            (500005, uid_md5(500005), uid_crc32(500005), '丁香花开', '丁', 200.0, 0, 200.0, 200.0),
        )
        wdb.commit()

    # 已冻结计划不受数仓事后更新干扰，顺利 finish 入库并通过 complete 校验！
    marker = talk_finalize.finish(config)
    assert marker['status'] == 'completed' and marker['rows'] == 1
    assert talk_finalize.complete(config) is True

    # 同时验证源数据库原始表未被修改，且篡改源弹幕仍会被 complete() 检出
    with sqlite3.connect(monitor_db) as db:
        assert db.execute('SELECT v FROM sentinel').fetchone()[0] == 'untouched'
    source['items'][1][2]['author'] = '被篡改***'
    assert talk_finalize.complete(config) is False
    source['items'][1][2]['author'] = '乙***'
    assert talk_finalize.complete(config) is True

    # 验证音乐终审 transcript_corrections 及下游消费者（session_timeline / audience_crm / config 自动回退）一致保留数仓昵称证据
    music_ws = tmp_path / 'music_ws'
    music_ws.mkdir()
    music_cfg = {
        'workspace': str(music_ws),
        'database': str(monitor_db),
        'warehouse_database': str(wh_db),
        'room_id': '123456',
        'workflow_key': 'synthetic_music_wf',
        'start_utc': '2030-01-01T00:00:00Z',
        'talk_finalize': False,
    }
    atomic_json(music_ws / 'config.json', music_cfg)
    atomic_json(music_ws / 'transcript_corrections.json', {
        'corrections': [{
            'seq': 1,
            'fingerprint': shard_in['windows'][0]['fingerprint'],
            'corrected_text': '欢迎甲壳虫乐迷和乙太旅行者，还有新名字老熟人！',
            'reason': '弹幕实名核对',
            'evidence': [{'id': 'm:2', 'quote': '乙太旅行者'}],
        }],
    })
    active, info = correction_context(music_ws, source)
    assert 1 in active and not info['rejected']

    cutoff_ts = epoch('2030-01-01T00:05:00Z')
    monkeypatch.setattr(session_timeline, 'source_at_cutoff',
                        lambda cfg: ({}, cutoff_ts, {**source, 'segments': [dict(s) for s in source['segments']]}))
    tl_rows = session_timeline.build(music_cfg, cutoff_ts)
    tl_seg = next(r for r in tl_rows if r['kind'] == 'transcript' and r['seq'] == 1)
    assert tl_seg['text'] == '欢迎甲壳虫乐迷和乙太旅行者，还有新名字老熟人！'
    assert tl_seg['correction_id'] == active[1]['correction_id']

    monkeypatch.setattr('bilibili_live_monitor.live_share_control.source_at_cutoff',
                        lambda cfg: ({}, cutoff_ts, {**source, 'segments': [dict(s) for s in source['segments']]}))
    _, _, crm_src = audience_crm.session_source(music_cfg)
    assert crm_src['segments'][0]['corrected_text'] == '欢迎甲壳虫乐迷和乙太旅行者，还有新名字老熟人！'
    assert crm_src['segments'][0]['correction_id'] == active[1]['correction_id']


@pytest.mark.asyncio
async def test_coalesced_bili_packets_op8_op5_and_op3_op5():
    def _make_danmu_op5(text: str, ts_ms: int) -> bytes:
        pkt = {
            'cmd': 'DANMU_MSG',
            'info': [
                [0, 1, 25, 16777215, ts_ms, 0, 0, 'a1b2c3d4'],
                text,
                [0, '甲***', 0, 0, 0, 10000, 1, ''],
                [],
            ],
        }
        inner = pack_bili_packet(5, json.dumps(pkt, ensure_ascii=False).encode('utf-8'), protover=0)
        return pack_bili_packet(5, brotli.compress(inner), protover=3)

    op8_plus_op5 = pack_bili_packet(8, b'{"code":0}') + _make_danmu_op5('合包弹幕一', 1893456001000)
    op3_plus_op5 = pack_bili_packet(3, struct.pack('>I', 42)) + _make_danmu_op5('合包弹幕二', 1893456002000)

    async def handle_ws(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            await _ws_accept_handshake(reader, writer)
            await _ws_read_client_frame(reader)  # Op7
            writer.write(_ws_server_frame(op8_plus_op5))
            await writer.drain()
            await _ws_read_client_frame(reader)  # 初始 Op2
            writer.write(_ws_server_frame(op3_plus_op5))
            writer.write(_ws_server_frame(struct.pack('>H', 1000), opcode=0x8))
            await writer.drain()
        finally:
            writer.close()

    server = await asyncio.start_server(handle_ws, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    received: list[dict] = []
    guest = GuestIdentity('b3', 'b4', SYNTHETIC_IMG_KEY, SYNTHETIC_SUB_KEY, time.monotonic())
    async with server:
        client = DirectClient(handshake_interval=0.0)
        with pytest.raises(EOFError):
            await client.stream_danmaku(
                123456,
                received.append,
                danmu_info=(guest, 'tok', [{'host': '127.0.0.1', 'wss_port': port, 'use_tls': False}]),
                heartbeat_seconds=10.0,
            )

    texts = [m['text'] for m in received if m.get('text') in ('合包弹幕一', '合包弹幕二')]
    assert texts == ['合包弹幕一', '合包弹幕二']


@pytest.mark.asyncio
async def test_ws_connect_rate_limit_covers_host_fallback_and_rfc6455_header_guards():
    connect_stamps: list[float] = []

    async def failing_open_connection(host, port, ssl=None):
        connect_stamps.append(time.monotonic())
        raise ConnectionRefusedError(f'synthetic host down {host}:{port}')

    guest = GuestIdentity('b3', 'b4', SYNTHETIC_IMG_KEY, SYNTHETIC_SUB_KEY, time.monotonic())
    client = DirectClient(handshake_interval=0.08, ws_connector=failing_open_connection)
    await client.acquire_handshake_slot()
    with pytest.raises(RuntimeError, match='弹幕服务器连接失败'):
        await client.stream_danmaku(
            123456,
            lambda _m: None,
            danmu_info=(
                guest,
                'tok',
                [
                    {'host': '127.0.0.1', 'wss_port': 2245, 'use_tls': False},
                    {'host': '127.0.0.2', 'wss_port': 2245, 'use_tls': False},
                ],
            ),
        )
    assert len(connect_stamps) == 2
    assert connect_stamps[1] - connect_stamps[0] >= 0.07

    class _DummyWriter:
        def write(self, _b):
            pass

        async def drain(self):
            pass

        def close(self):
            pass

        async def wait_closed(self):
            pass

    for bad_frame, match in [
        (bytes([0xC2, 0x02]) + b'hi', 'RSV'),
        (bytes([0x82, 0x82, 1, 2, 3, 4, 5, 6]), '掩码'),
        (bytes([0x83, 0x01, 0x00]), '操作码'),
        (bytes([0x09, 0x01, 0x00]), '控制帧'),
        (bytes([0x89, 126, 0x00, 126]) + b'x' * 126, '控制帧'),
    ]:
        reader = asyncio.StreamReader()
        reader.feed_data(bad_frame)
        reader.feed_eof()
        ws = MinimalAsyncWebSocket(reader, _DummyWriter())
        with pytest.raises(ValueError, match=match):
            await ws.recv_message()


@pytest.mark.asyncio
async def test_monitor_capture_direct_reconnects_then_halts_on_refusal(tmp_path):
    store = Store(tmp_path / 'monitor.sqlite3')
    settings = Settings(data_dir=tmp_path, capture_backend='direct', record_mode='none',
                        direct_handshake_interval=0.0, retry_delay=0.01, retry_max_delay=0.05)
    monitor = Monitor(settings, store)

    attempts = 0
    done = asyncio.Event()

    async def fake_resolve(url):
        return {
            'requested_id': '123456',
            'real_room_id': 123456,
            'short_id': 0,
            'capture_room_id': '123456',
            'url': 'https://live.bilibili.com/123456',
            'source_url': 'https://live.bilibili.com/123456',
            'anchor_uid': '999',
            'live_status': 1,
            'live_time': '2030-01-01 08:00:00',
            'title': '合成重连房间',
            'cover_url': '',
        }

    async def fake_fetch_danmu(rid):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError('transient network reset')
        done.set()
        raise RuntimeError('recording_refused: getDanmuInfo code=-412')

    monitor.direct.resolve_room = fake_resolve
    monitor.direct.fetch_danmu_info = fake_fetch_danmu

    await monitor.start()
    await monitor.add('123456', 5, record_mode='none')
    await asyncio.wait_for(done.wait(), 2.0)
    task = monitor.tasks.get('123456')
    if task:
        await asyncio.wait_for(task, 2.0)

    room_state = next(r for r in store.rooms() if r['room_id'] == '123456')
    assert attempts == 2
    assert room_state['status'] == 'error'
    assert room_state['reconnect_attempts'] == 2
    assert 'recording_refused' in room_state['error']
    assert room_state['next_retry_utc'] is None
    await monitor.close()
    store.close()
