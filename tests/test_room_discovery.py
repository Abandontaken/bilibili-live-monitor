"""扩房发现与分层滚雪球采样离线 CLI 单元测试（全部使用虚构房间号、虚构 UID 与 Mock HTTP 传输）。"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from bilibili_live_monitor.__main__ import main as cli_main
from bilibili_live_monitor.room_discovery import (
    OPUS_SNOWFLAKE_EPOCH_SEC,
    AuthFollowingsClient,
    GuestDiscoveryClient,
    classify_guard_tier,
    decode_opus_timestamp,
    detect_hygiene_flag,
    load_cookie_file,
    open_discovery_db,
    pull_followings,
    read_cdp_cookies_readonly,
    sample_plan,
    screen_candidates,
)
from bilibili_live_monitor.warehouse import open_warehouse
from bilibili_live_monitor.warehouse_protocol import uid_crc32, uid_md5

FAKE_IMG_KEY = '7cd084941338484aae1ad9425b84077c'
FAKE_SUB_KEY = '4932caff0ff746eab6f01bf08b70ac45'
NOW_TS = 1795000000


def _make_opus_id(ts_sec: int, low_bits: int = 42) -> str:
    return str(((int(ts_sec) - OPUS_SNOWFLAKE_EPOCH_SEC) << 32) | low_bits)


def _seed_synthetic_warehouse(wh_path: Path) -> None:
    conn = open_warehouse(wh_path)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO dim_rooms (room_id, anchor_uid, room_label, first_seen_ts, last_seen_ts)
                VALUES ('123456', 100001, '合成种子主播A', 1790000000, 1790003600)
                """
            )
            conn.execute(
                """
                INSERT INTO dim_sessions (
                    session_key, room_id, anchor_uid, room_label, source_instance,
                    source_db_path, start_ts, cutoff_ts, extractor_version, ingested_at_utc
                ) VALUES (
                    'sess_seed_001', '123456', 100001, '合成种子主播A', 'anonymous',
                    '/PRIVATE/monitor.sqlite3', 1790000000, 1790003600, 2, '2026-11-17T00:00:00Z'
                )
                """
            )
            # 包含种子主播本人(100001，即使荣耀等级高也必须自动排除) + 大/中/小 R 及低等级路人(<10)
            users = [
                (100001, '合成种子主播A', 48),
                (200001, '合成大哥一', 45),
                (200002, '合成大哥二', 41),
                (200003, '合成大哥三', 40),
                (300001, '合成中R一', 32),
                (300002, '合成中R二', 25),
                (400001, '合成小R一', 18),
                (400002, '合成小R二', 10),
                (500001, '合成低等级观众', 5),
                (987654321, 'SECRET_SMALL_ACCOUNT', 22),
            ]
            for uid, uname, wl in users:
                conn.execute(
                    """
                    INSERT INTO dim_users (
                        uid, uid_md5, uid_crc32, latest_uname, first_char,
                        max_wealth_level, first_seen_ts, last_seen_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, 1790000000, 1790003600)
                    """,
                    (uid, uid_md5(uid), uid_crc32(uid), uname, uname[0], wl),
                )
                conn.execute(
                    """
                    INSERT INTO fact_session_presence (
                        session_key, actor_key, room_id, resolved_uid,
                        first_event_ts, last_event_ts, danmaku_count, gift_event_count,
                        paid_milli_yuan, max_wealth_level
                    ) VALUES ('sess_seed_001', ?, '123456', ?, 1790000100, 1790000900, 3, 1, 1000, ?)
                    """,
                    (f'uid:{uid}', uid, wl),
                )
    finally:
        conn.close()


def test_opus_snowflake_and_helpers(tmp_path: Path) -> None:
    oid = _make_opus_id(NOW_TS - 3600)
    assert decode_opus_timestamp(oid) == NOW_TS - 3600
    assert decode_opus_timestamp('') is None
    assert decode_opus_timestamp(-5) is None
    assert decode_opus_timestamp(True) is None

    assert classify_guard_tier(0) == 'zero_0'
    assert classify_guard_tier(15) == 'tail_1_29'
    assert classify_guard_tier(30) == 'mid_30_199'
    assert classify_guard_tier(200) == 'head_200_plus'

    assert detect_hygiene_flag('晚安歌回', '普通虚拟主播') == 'clean'
    assert detect_hygiene_flag('深夜温暖点唱厅欢迎光临') == 'flagged:点唱厅'
    assert detect_hygiene_flag('助眠音疗 24h 无人播放') == 'flagged:24h'

    cookie_json = tmp_path / 'cookies.json'
    cookie_json.write_text(
        json.dumps([{'name': 'SESSDATA', 'value': 'syn_sess_val'}, {'name': 'bili_jct', 'value': 'syn_jct'}]),
        encoding='utf-8',
    )
    loaded = load_cookie_file(cookie_json)
    assert loaded == {'SESSDATA': 'syn_sess_val', 'bili_jct': 'syn_jct'}

    cookie_hdr = tmp_path / 'cookie.txt'
    cookie_hdr.write_text('Cookie: SESSDATA=hdr_sess; buvid3=b3_val', encoding='utf-8')
    assert load_cookie_file(cookie_hdr)['SESSDATA'] == 'hdr_sess'

    bad_cookie = tmp_path / 'no_sess.json'
    bad_cookie.write_text(json.dumps({'buvid3': 'only_guest'}), encoding='utf-8')
    with pytest.raises(ValueError, match='缺少 SESSDATA'):
        load_cookie_file(bad_cookie)


def test_db_isolation_guards(tmp_path: Path) -> None:
    wh_path = tmp_path / 'warehouse.sqlite3'
    _seed_synthetic_warehouse(wh_path)

    # 拒绝把已有数仓当成 discovery_db 可写打开
    with pytest.raises(ValueError, match='拒绝作为发现工作库'):
        open_discovery_db(wh_path)

    # 拒绝把采集源库当成 discovery_db 可写打开
    src_db = tmp_path / 'monitor.sqlite3'
    with sqlite3.connect(src_db) as c:
        c.execute('CREATE TABLE raw_events (id INTEGER PRIMARY KEY)')
    with pytest.raises(ValueError, match='拒绝作为发现工作库'):
        open_discovery_db(src_db)


def test_auth_client_url_whitelist_and_no_cookie_leak() -> None:
    seen_urls: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen_urls.append(str(req.url))
        return httpx.Response(200, json={'code': 0, 'data': {'isLogin': True}})

    transport = httpx.MockTransport(handler)
    raw_http = httpx.Client(transport=transport, follow_redirects=False)
    client = AuthFollowingsClient(
        {'SESSDATA': 'secret_token_999999', 'bili_jct': 'secret_csrf_888888'},
        auth_interval=0.0,
        http_client=raw_http,
    )
    try:
        res = client.get_json('https://api.bilibili.com/x/web-interface/nav')
        assert res['code'] == 0

        # 非白名单直播接口或空间接口一律拦截
        for forbidden in (
            'https://api.live.bilibili.com/room/v1/Room/get_info',
            'https://api.bilibili.com/x/polymer/web-dynamic/v1/opus/feed/space',
            'https://api.bilibili.com/x/relation/followings?vmid=200001',
            'http://api.bilibili.com/x/web-interface/nav',
        ):
            with pytest.raises(PermissionError, match='拦截非白名单'):
                client.get_json(forbidden)
        assert len(seen_urls) == 1
    finally:
        client.close()
        assert len(raw_http.cookies) == 0
        raw_http.close()


def test_cdp_cookie_extraction_never_closes_browser(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    class FakeUrlResp:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self) -> bytes:
            return json.dumps(
                {'webSocketDebuggerUrl': 'ws://127.0.0.1:19358/devtools/browser/syn', 'User-Agent': 'TestUA'}
            ).encode('utf-8')

    class FakeContext:
        def cookies(self, urls: list[str]) -> list[dict[str, str]]:
            events.append(f'cookies:{len(urls)}')
            return [{'name': 'SESSDATA', 'value': 'cdp_syn_sess'}]

        def new_page(self):
            raise AssertionError('严禁新建标签页')

    class FakeBrowser:
        contexts = [FakeContext()]

        def close(self) -> None:
            events.append('browser.close')
            raise AssertionError('严禁调用 browser.close()')

    class FakeChromium:
        def connect_over_cdp(self, url: str, timeout: int = 10000) -> FakeBrowser:
            events.append(f'connect:{url}')
            return FakeBrowser()

    class FakePW:
        chromium = FakeChromium()

        def stop(self) -> None:
            events.append('pw.stop')

    class FakeSyncPlaywrightCM:
        def start(self) -> FakePW:
            events.append('pw.start')
            return FakePW()

    monkeypatch.setattr('urllib.request.urlopen', lambda *a, **kw: FakeUrlResp())
    monkeypatch.setitem(
        sys.modules,
        'playwright.sync_api',
        type('FakeMod', (), {'sync_playwright': lambda: FakeSyncPlaywrightCM()}),
    )

    cookies, ua = read_cdp_cookies_readonly('http://127.0.0.1:19358')
    assert cookies == {'SESSDATA': 'cdp_syn_sess'}
    assert ua == 'TestUA'
    assert events == ['pw.start', 'connect:http://127.0.0.1:19358', 'cookies:2', 'pw.stop']

    with pytest.raises(ValueError, match='回环地址'):
        read_cdp_cookies_readonly('http://192.168.1.20:9358')


def test_pull_followings_dry_run_pagination_cap_privacy_and_resume(tmp_path: Path) -> None:
    wh_path = tmp_path / 'warehouse.sqlite3'
    disc_path = tmp_path / 'discovery.sqlite3'
    _seed_synthetic_warehouse(wh_path)

    # 1. dry-run 不创建 discovery.sqlite3、不发请求，且自动排除种子主播 100001 与 <10 级观众
    dry = pull_followings(warehouse_db=wh_path, discovery_db=disc_path, per_tier=2, seed=42, execute=False)
    assert dry['mode'] == 'dry_run'
    assert not disc_path.exists()
    assert dry['pool_counts'] == {'high_r': 3, 'mid_r': 2, 'low_r': 3}
    assert dry['planned_counts'] == {'high_r': 2, 'mid_r': 2, 'low_r': 2}
    assert dry['estimated_max_requests'] == 1 + 2 * 6

    # 2. execute 测试：
    # - 200001: total=350，pn=1 返回 50 条，pn=2 返回 50 条 -> 必须 2 页封顶（绝不请求 pn=3）
    # - 200002 / 200003: code=22115 隐私锁 -> 记录 hidden
    # - 300001: total=12，pn=1 返回 12 条 -> 1 页即停
    # - 其余返回公开 2 条
    requested_pages: list[tuple[int, int]] = []
    secret_sess = 'SENSITIVE_SESSDATA_DO_NOT_LEAK'
    secret_self_uid = '987654321'

    def handler(req: httpx.Request) -> httpx.Response:
        path = req.url.path
        if path == '/x/web-interface/nav':
            return httpx.Response(
                200,
                json={
                    'code': 0,
                    'data': {
                        'isLogin': True,
                        'mid': int(secret_self_uid),
                        'uname': 'SECRET_SMALL_ACCOUNT',
                    },
                },
            )
        assert path == '/x/relation/followings'
        qs = parse_qs(req.url.query.decode('utf-8'))
        vmid = int(qs['vmid'][0])
        pn = int(qs['pn'][0])
        assert pn <= 2, f'违反第三方 2 页封顶约束：请求了 pn={pn}'
        requested_pages.append((vmid, pn))

        if vmid in (200002, 200003):
            return httpx.Response(200, json={'code': 22115, 'message': '用户已设置隐私'})
        if vmid == 200001:
            base = (pn - 1) * 50 + 600000
            items = [
                {
                    'mid': base + i,
                    'uname': f'候选UP_{base + i}',
                    'mtime': 1794000000 + i,
                    'sign': '歌势虚拟主播',
                    'official_verify': {'type': -1, 'desc': ''},
                }
                for i in range(50)
            ]
            return httpx.Response(200, json={'code': 0, 'data': {'total': 350, 'list': items}})
        if vmid == 300001:
            items = [
                {'mid': 700001 + i, 'uname': f'候选_{i}', 'mtime': 1794000100, 'sign': ''}
                for i in range(12)
            ]
            return httpx.Response(200, json={'code': 0, 'data': {'total': 12, 'list': items}})
        return httpx.Response(
            200,
            json={
                'code': 0,
                'data': {
                    'total': 3,
                    'list': [
                        {'mid': 700001, 'uname': '共现主播甲', 'mtime': 1794000200, 'sign': ''},
                        {'mid': 700002, 'uname': '共现主播乙', 'mtime': 1794000300, 'sign': ''},
                        # 若观众关注了小号本人，必须静默剔除，禁止落盘
                        {'mid': int(secret_self_uid), 'uname': 'SECRET_SMALL_ACCOUNT', 'mtime': 1794000400, 'sign': ''},
                    ],
                },
            },
        )

    raw_http = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    auth = AuthFollowingsClient({'SESSDATA': secret_sess}, auth_interval=0.0, http_client=raw_http)
    try:
        res1 = pull_followings(
            warehouse_db=wh_path,
            discovery_db=disc_path,
            per_tier=3,
            seed=42,
            execute=True,
            auth_client=auth,
        )
    finally:
        auth.close()
        raw_http.close()

    assert res1['circuit_broken'] is False
    assert res1['tiers']['high_r']['public_count'] == 1
    assert res1['tiers']['high_r']['hidden_count'] == 2
    assert (200001, 1) in requested_pages and (200001, 2) in requested_pages
    assert (300001, 1) in requested_pages and (300001, 2) not in requested_pages

    # 验证输出 JSON 与 SQLite 中绝对不含 SESSDATA 或小号本人 UID/昵称
    serialized = json.dumps(res1, ensure_ascii=False)
    assert secret_sess not in serialized
    assert secret_self_uid not in serialized
    assert 'SECRET_SMALL_ACCOUNT' not in serialized
    db_bytes = disc_path.read_bytes()
    assert secret_sess.encode() not in db_bytes
    assert b'SECRET_SMALL_ACCOUNT' not in db_bytes
    with sqlite3.connect(disc_path) as c_chk:
        assert c_chk.execute(
            'SELECT COUNT(*) FROM ext_viewer_follow_status WHERE viewer_uid = ?', (int(secret_self_uid),)
        ).fetchone()[0] == 0
        assert c_chk.execute(
            'SELECT COUNT(*) FROM ext_viewer_followings WHERE viewer_uid = ? OR followed_uid = ?',
            (int(secret_self_uid), int(secret_self_uid)),
        ).fetchone()[0] == 0

    # 3. 幂等续跑：除小号本人外其余 7 位观众已是 public/hidden；传入 exclude_uids 排除小号后待拉取为 0
    dry2 = pull_followings(
        warehouse_db=wh_path,
        discovery_db=disc_path,
        per_tier=3,
        seed=42,
        exclude_uids=[secret_self_uid],
        execute=False,
    )
    assert dry2['total_planned_viewers'] == 0
    assert dry2['estimated_max_requests'] == 0


def test_pull_followings_circuit_breaker_preserves_progress(tmp_path: Path) -> None:
    wh_path = tmp_path / 'warehouse.sqlite3'
    disc_path = tmp_path / 'discovery.sqlite3'
    _seed_synthetic_warehouse(wh_path)

    call_seq = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal call_seq
        if req.url.path == '/x/web-interface/nav':
            return httpx.Response(200, json={'code': 0, 'data': {'isLogin': True}})
        call_seq += 1
        if call_seq == 1:
            return httpx.Response(
                200,
                json={'code': 0, 'data': {'total': 1, 'list': [{'mid': 800001, 'uname': '主播A', 'mtime': 1}]}},
            )
        # 第二个观众返回 -352 风控拦截，必须立即熔断停机，不再请求后续观众
        return httpx.Response(200, json={'code': -352, 'message': '风控校验失败'})

    raw_http = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    auth = AuthFollowingsClient({'SESSDATA': 'syn_sess'}, auth_interval=0.0, http_client=raw_http)
    try:
        res = pull_followings(
            warehouse_db=wh_path,
            discovery_db=disc_path,
            per_tier=2,
            seed=7,
            execute=True,
            auth_client=auth,
        )
    finally:
        auth.close()
        raw_http.close()

    assert res['circuit_broken'] is True
    assert res['circuit_break_reason'] == 'API_CODE_-352'
    assert call_seq == 2
    assert res['authenticated_requests'] == 3  # 1 nav + 1 ok + 1 circuit-break

    with sqlite3.connect(disc_path) as conn:
        saved = conn.execute('SELECT COUNT(*) FROM ext_viewer_follow_status').fetchone()[0]
        assert saved == 1


def test_screen_candidates_and_sample_plan_end_to_end(tmp_path: Path) -> None:
    wh_path = tmp_path / 'warehouse.sqlite3'
    disc_path = tmp_path / 'discovery.sqlite3'
    auto_cfg = tmp_path / 'automation.json'
    _seed_synthetic_warehouse(wh_path)
    auto_cfg.write_text(json.dumps({'rooms': {'654321': {'label': '已配置种子房'}}}), encoding='utf-8')

    # 预置发现库中的观众关注边（涵盖各类正反例候选账号）
    conn = open_discovery_db(disc_path, readonly_sources=[wh_path])
    with conn:
        edges = [
            # 100001: 已知种子主播 -> 应自动排除
            (200001, 100001, 'high_r', 1794000000, '合成种子主播A', None, None, ''),
            # 600001: 当前正在直播 (live_now)，虚拟主播/虚拟日常，260舰 (head_200_plus)
            (200001, 600001, 'high_r', 1794000001, '合成在播V', None, None, '每晚八点直播'),
            (300001, 600001, 'mid_r', 1794000002, '合成在播V', None, None, '每晚八点直播'),
            (400001, 600001, 'low_r', 1794000003, '合成在播V', None, None, '每晚八点直播'),
            # 600002: 下播状态，但有近 14 天直播回放合集 (live_replay)，虚拟主播/虚拟Singer，85舰 (mid_30_199)
            (200001, 600002, 'high_r', 1794000004, '合成回放歌势', None, None, ''),
            (300001, 600002, 'mid_r', 1794000005, '合成回放歌势', None, None, ''),
            # 600003: 下播且不留回放，但有近期直播预约 (live_reservation)，电台/唱见电台，12舰 (tail_1_29)
            (200002, 600003, 'high_r', 1794000006, '合成预约唱见', None, None, ''),
            (400001, 600003, 'low_r', 1794000007, '合成预约唱见', None, None, ''),
            # 600004: 不留回放歌势主播，无预约，但 guard=45 > 0 且近 3 天有 opus 动态 -> 必须召回 (guard_plus_recent_opus)
            # 同时有短房号 short_id=8804，验证 capture_room_id 优先采用短号
            (200001, 600004, 'high_r', 1794000008, '合成不留档歌势', None, None, '歌单见动态'),
            # 600005: 退网残余舰长反例 (guard=6 > 0，但最新 opus 在 90 天前) -> 必须判为 inactive
            (200001, 600005, 'high_r', 1794000009, '合成退网主播', None, None, ''),
            # 600006: 纯发动态无舰官号反例 (guard=0，近 2 天连发 opus，甚至带超远期 60 天后预约) -> 必须判为 inactive
            (300001, 600006, 'mid_r', 1794000010, '合成游戏官号', 1, '官方账号', ''),
            # 600007: 多人语音交友厅 (live_status=1，但标题含「交友厅」) -> 保留入库并标 hygiene_flag != 'clean'
            (400002, 600007, 'low_r', 1794000011, '某家族厅', None, None, '24h温暖陪伴'),
            # 600008: 未开通直播间普通 UP (room_id=0) -> 不进入候选房表
            (400002, 600008, 'low_r', 1794000012, '纯视频UP', None, None, ''),
            # 600009: 房间号命中 automation.json 中的 654321 -> 自动作为种子房跳过
            (200001, 600009, 'high_r', 1794000013, '已配置种子主播B', None, None, ''),
        ]
        conn.executemany(
            """
            INSERT INTO ext_viewer_followings (
                viewer_uid, followed_uid, wealth_tier, followed_mtime,
                followed_uname, official_type, official_desc, sign_text, synced_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1794900000)
            """,
            edges,
        )
    conn.close()

    rooms_catalog: dict[int, dict[str, Any]] = {
        600001: {
            'uid': 600001,
            'room_id': 2100001,
            'short_id': 0,
            'uname': '合成在播V',
            'title': '晚间杂谈歌回',
            'area_v2_parent_name': '虚拟主播',
            'area_v2_name': '虚拟日常',
            'live_status': 1,
        },
        600002: {
            'uid': 600002,
            'room_id': 2100002,
            'short_id': 0,
            'uname': '合成回放歌势',
            'title': '周三练歌房',
            'area_v2_parent_name': '虚拟主播',
            'area_v2_name': '虚拟Singer',
            'live_status': 0,
        },
        600003: {
            'uid': 600003,
            'room_id': 2100003,
            'short_id': 0,
            'uname': '合成预约唱见',
            'title': '周五弹唱',
            'area_v2_parent_name': '电台',
            'area_v2_name': '唱见电台',
            'live_status': 0,
        },
        600004: {
            'uid': 600004,
            'room_id': 2100004,
            'short_id': 8804,
            'uname': '合成不留档歌势',
            'title': '不留档清唱',
            'area_v2_parent_name': '虚拟主播',
            'area_v2_name': '虚拟Singer',
            'live_status': 0,
        },
        600005: {
            'uid': 600005,
            'room_id': 2100005,
            'short_id': 0,
            'uname': '合成退网主播',
            'title': '有缘再见',
            'area_v2_parent_name': '虚拟主播',
            'area_v2_name': '虚拟日常',
            'live_status': 0,
        },
        600006: {
            'uid': 600006,
            'room_id': 2100006,
            'short_id': 0,
            'uname': '合成游戏官号',
            'title': '前瞻特别节目',
            'area_v2_parent_name': '手游',
            'area_v2_name': '动作手游',
            'live_status': 0,
        },
        600007: {
            'uid': 600007,
            'room_id': 2100007,
            'short_id': 0,
            'uname': '某家族厅',
            'title': '心动交友厅主持在线',
            'area_v2_parent_name': '电台',
            'area_v2_name': '聊天电台',
            'live_status': 1,
        },
        600009: {
            'uid': 600009,
            'room_id': 654321,
            'short_id': 0,
            'uname': '已配置种子主播B',
            'title': '种子房',
            'area_v2_parent_name': '娱乐',
            'area_v2_name': '视频唱见',
            'live_status': 1,
        },
        # 外生跨赛道候选
        700001: {
            'uid': 700001,
            'room_id': 2200001,
            'short_id': 0,
            'uname': '外生单机主播一',
            'title': '魂系速通挑战',
            'area_v2_parent_name': '单机游戏',
            'area_v2_name': '主机游戏',
            'live_status': 1,
        },
        700002: {
            'uid': 700002,
            'room_id': 2200002,
            'short_id': 0,
            'uname': '外生娱乐舞见一',
            'title': '晚间随机舞蹈',
            'area_v2_parent_name': '娱乐',
            'area_v2_name': '舞见',
            'live_status': 1,
        },
    }

    guard_counts = {
        600001: 260,
        600002: 85,
        600003: 12,
        600004: 45,
        600005: 6,
        600006: 0,
        600007: 35,
        700001: 110,
        700002: 5,
    }

    opus_352_once = {'triggered': False}

    def guest_handler(req: httpx.Request) -> httpx.Response:
        assert 'SESSDATA' not in req.headers.get('cookie', '')
        url_obj = urlsplit(str(req.url))
        qs = parse_qs(url_obj.query)
        path = url_obj.path

        if path == '/x/frontend/finger/spi':
            return httpx.Response(200, json={'code': 0, 'data': {'b_3': 'SYN_BUVID3', 'b_4': 'SYN_BUVID4'}})
        if path == '/x/web-interface/nav':
            return httpx.Response(
                200,
                json={
                    'code': -101,
                    'data': {
                        'isLogin': False,
                        'wbi_img': {
                            'img_url': f'https://i0.hdslb.com/bfs/wbi/{FAKE_IMG_KEY}.png',
                            'sub_url': f'https://i0.hdslb.com/bfs/wbi/{FAKE_SUB_KEY}.png',
                        },
                    },
                },
            )
        if path == '/room/v1/Area/getList':
            return httpx.Response(
                200,
                json={
                    'code': 0,
                    'data': [
                        {'id': 9, 'name': '虚拟主播'},
                        {'id': 6, 'name': '单机游戏'},
                        {'id': 1, 'name': '娱乐'},
                    ],
                },
            )
        if path == '/room/v3/area/getRoomList':
            pid = int(qs['parent_area_id'][0])
            if pid == 9:
                # 600001 同时出现在内生与外生，应合并为 source_channel='both'
                return httpx.Response(
                    200,
                    json={'code': 0, 'data': {'list': [rooms_catalog[600001] | {'roomid': 2100001}]}},
                )
            if pid == 6:
                return httpx.Response(
                    200,
                    json={'code': 0, 'data': {'list': [rooms_catalog[700001] | {'roomid': 2200001}]}},
                )
            if pid == 1:
                return httpx.Response(
                    200,
                    json={'code': 0, 'data': {'list': [rooms_catalog[700002] | {'roomid': 2200002}]}},
                )
            return httpx.Response(200, json={'code': 0, 'data': {'list': []}})

        if path == '/room/v1/Room/get_status_info_by_uids':
            uids = [int(x) for x in qs.get('uids[]', [])]
            out = {str(u): rooms_catalog[u] for u in uids if u in rooms_catalog}
            return httpx.Response(200, json={'code': 0, 'data': out})

        if path == '/xlive/app-room/v2/guardTab/topList':
            ruid = int(qs['ruid'][0])
            return httpx.Response(
                200,
                json={'code': 0, 'data': {'info': {'num': guard_counts.get(ruid, 0), 'page': 1}, 'list': [], 'top3': []}},
            )

        if path == '/x/polymer/web-space/seasons_series_list':
            mid = int(qs['mid'][0])
            if mid == 600002:
                return httpx.Response(
                    200,
                    json={
                        'code': 0,
                        'data': {
                            'items_lists': {
                                'series_list': [
                                    {
                                        'meta': {'category': 2, 'name': '直播回放', 'series_id': 91, 'mtime': NOW_TS - 86400},
                                        'archives': [{'aid': 1, 'pubdate': NOW_TS - 2 * 86400}],
                                    }
                                ]
                            }
                        },
                    },
                )
            return httpx.Response(200, json={'code': 0, 'data': {'items_lists': {'series_list': []}}})

        if path == '/x/space/reservation':
            vmid = int(qs['vmid'][0])
            if vmid == 600003:
                return httpx.Response(
                    200,
                    json={'code': 0, 'data': [{'sid': 10, 'live_plan_start_time': NOW_TS + 2 * 86400}]},
                )
            if vmid == 600006:
                # 60 天后的超远期活动预约（即使今天刚创建 ctime=NOW_TS-60）也不应算近 14 天开播活跃
                return httpx.Response(
                    200,
                    json={
                        'code': 0,
                        'data': [{'sid': 11, 'live_plan_start_time': NOW_TS + 60 * 86400, 'ctime': NOW_TS - 60}],
                    },
                )
            return httpx.Response(200, json={'code': 0, 'data': []})

        if path == '/x/polymer/web-dynamic/v1/opus/feed/space':
            host_mid = int(qs['host_mid'][0])
            assert 'w_rid' in qs and 'wts' in qs
            if host_mid == 600004 and not opus_352_once['triggered']:
                opus_352_once['triggered'] = True
                return httpx.Response(200, json={'code': -352, 'message': 'need refresh wbi'})
            if host_mid == 600004:
                return httpx.Response(
                    200,
                    json={'code': 0, 'data': {'items': [{'opus_id': _make_opus_id(NOW_TS - 3 * 86400)}]}},
                )
            if host_mid == 600005:
                # 90 天前的旧动态 -> 退网主播不活跃
                return httpx.Response(
                    200,
                    json={'code': 0, 'data': {'items': [{'opus_id': _make_opus_id(NOW_TS - 90 * 86400)}]}},
                )
            if host_mid == 600006:
                return httpx.Response(
                    200,
                    json={'code': 0, 'data': {'items': [{'opus_id': _make_opus_id(NOW_TS - 86400)}]}},
                )
            return httpx.Response(200, json={'code': 0, 'data': {'items': []}})

        raise AssertionError(f'未预期的请求路径: {path}')

    raw_guest = httpx.Client(transport=httpx.MockTransport(guest_handler))
    guest = GuestDiscoveryClient(guest_interval=0.0, http_client=raw_guest)
    try:
        summary = screen_candidates(
            discovery_db=disc_path,
            warehouse_db=wh_path,
            automation_configs=[auto_cfg],
            max_endogenous=50,
            exogenous_per_area=2,
            parent_areas=['虚拟主播', '单机游戏', '娱乐'],
            active_days=14,
            now_ts=NOW_TS,
            guest_client=guest,
        )
    finally:
        guest.close()
        raw_guest.close()

    assert opus_352_once['triggered'] is True
    assert summary['skipped_seed_rooms'] == 1  # 600009 (room 654321)
    assert summary['source_channel_counts']['both'] == 1  # 600001
    assert summary['hygiene_summary'] == {'clean': 8, 'flagged': 1}
    assert summary['active_evidence_counts'] == {
        'live_now': 4,  # 600001, 600007(flagged), 700001, 700002
        'live_replay': 1,  # 600002
        'live_reservation': 1,  # 600003
        'guard_plus_recent_opus': 1,  # 600004 (不留回放歌势成功召回)
        'inactive': 2,  # 600005(退网残余舰长) + 600006(无舰官号)
    }

    with sqlite3.connect(disc_path) as check_conn:
        r_600004 = check_conn.execute(
            'SELECT capture_room_id, active_14d, active_evidence, guard_tier FROM dim_candidate_rooms WHERE anchor_uid = 600004'
        ).fetchone()
        assert r_600004 == ('8804', 1, 'guard_plus_recent_opus', 'mid_30_199')

        r_600005 = check_conn.execute(
            'SELECT active_14d, active_evidence FROM dim_candidate_rooms WHERE anchor_uid = 600005'
        ).fetchone()
        assert r_600005 == (0, 'inactive')

        r_600006 = check_conn.execute(
            'SELECT active_14d, active_evidence FROM dim_candidate_rooms WHERE anchor_uid = 600006'
        ).fetchone()
        assert r_600006 == (0, 'inactive')

        r_600007 = check_conn.execute(
            'SELECT hygiene_flag, active_14d FROM dim_candidate_rooms WHERE anchor_uid = 600007'
        ).fetchone()
        assert r_600007 == ('flagged:交友厅', 1)

    # 验证 sample_plan 拒绝将 --output 指向只读数仓、发现库、automation 配置或其他已有 SQLite 文件
    for colliding_out in (wh_path, disc_path, auto_cfg):
        with pytest.raises(ValueError):
            sample_plan(
                discovery_db=disc_path,
                warehouse_db=wh_path,
                automation_configs=[auto_cfg],
                target_count=5,
                output_path=colliding_out,
            )
    other_sqlite = tmp_path / 'other.sqlite3'
    with sqlite3.connect(other_sqlite) as c_oth:
        c_oth.execute('CREATE TABLE t (x INTEGER)')
    with pytest.raises(ValueError, match='SQLite'):
        sample_plan(discovery_db=disc_path, target_count=5, output_path=other_sqlite)

    # 验证第三阶段 sample-plan：子分区限额 (--max-per-subarea 1 会限制 虚拟主播/虚拟Singer 的 600002 与 600004 只选共现更高的 600002)
    plan_file = tmp_path / 'plan.json'
    plan1 = sample_plan(
        discovery_db=disc_path,
        warehouse_db=wh_path,
        automation_configs=[auto_cfg],
        target_count=5,
        endogenous_ratio=0.6,
        max_per_subarea=1,
        seed=20261117,
        output_path=plan_file,
    )
    plan2 = sample_plan(
        discovery_db=disc_path,
        warehouse_db=wh_path,
        automation_configs=[auto_cfg],
        target_count=5,
        endogenous_ratio=0.6,
        max_per_subarea=1,
        seed=20261117,
    )
    assert plan1 == plan2
    assert json.loads(plan_file.read_text(encoding='utf-8')) == plan1
    assert plan1['funnel']['excluded_inactive_count'] == 2
    assert plan1['funnel']['excluded_hygiene_flagged_count'] == 1
    assert plan1['funnel']['eligible_clean_active_count'] == 6  # 4 endo/both + 2 exo

    picked_uids = [r['anchor_uid'] for r in plan1['selected_rooms']]
    assert len(picked_uids) == 5
    # 600002 (共现=2) 与 600004 (共现=1) 同属 虚拟主播/虚拟Singer，max_per_subarea=1 下只选 600002
    assert 600002 in picked_uids
    assert 600004 not in picked_uids
    assert 700001 in picked_uids and 700002 in picked_uids
    assert 600007 not in picked_uids  # 交友厅被过滤


def test_guest_client_rejects_accidental_login() -> None:
    raw = httpx.Client()
    raw.cookies.set('SESSDATA', 'unexpected_login')
    try:
        with pytest.raises(PermissionError, match='登录态 Cookie'):
            GuestDiscoveryClient(http_client=raw)
    finally:
        raw.close()

    def logged_in_nav(req: httpx.Request) -> httpx.Response:
        if req.url.path == '/x/frontend/finger/spi':
            return httpx.Response(200, json={'code': 0, 'data': {'b_3': 'G_B3'}})
        return httpx.Response(200, json={'code': 0, 'data': {'isLogin': True}})

    raw2 = httpx.Client(transport=httpx.MockTransport(logged_in_nav))
    guest = GuestDiscoveryClient(guest_interval=0.0, http_client=raw2)
    try:
        with pytest.raises(PermissionError, match='isLogin=True'):
            guest.ensure_guest_wbi()
    finally:
        guest.close()
        raw2.close()


def test_cli_entrypoint_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    wh_path = tmp_path / 'warehouse.sqlite3'
    disc_path = tmp_path / 'discovery.sqlite3'
    _seed_synthetic_warehouse(wh_path)

    monkeypatch.setattr(
        sys,
        'argv',
        [
            'bili-live',
            'room-discovery',
            'pull-followings',
            '--warehouse-db',
            str(wh_path),
            '--db',
            str(disc_path),
            '--per-tier',
            '2',
        ],
    )
    rc = cli_main()
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out['mode'] == 'dry_run'
    assert out['total_planned_viewers'] == 6


@pytest.mark.parametrize(
    ('http_status', 'payload', 'expected_reason'),
    [
        (412, {}, 'HTTP_412'),
        (429, {}, 'HTTP_429'),
        (302, {}, 'HTTP_REDIRECT_302'),
        (200, {'code': -412, 'message': 'request blocked'}, 'API_CODE_-412'),
        (200, {'code': -101, 'message': '账号未登录'}, 'API_CODE_-101'),
    ],
)
def test_auth_client_rate_jitter_and_circuit_breaker_codes(
    http_status: int,
    payload: dict[str, Any],
    expected_reason: str,
) -> None:
    sleeps: list[float] = []
    calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, json={'code': 0, 'data': {'isLogin': True}})
        return httpx.Response(http_status, json=payload, headers={'Location': 'https://passport.bilibili.com'})

    raw = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    client = AuthFollowingsClient(
        {'SESSDATA': 'syn_sess'},
        auth_interval=2.5,
        http_client=raw,
        sleep_fn=sleeps.append,
    )
    try:
        r1 = client.get_json('https://api.bilibili.com/x/web-interface/nav')
        assert r1['code'] == 0
        client.get_json('https://api.bilibili.com/x/relation/followings', params={'vmid': 200001, 'pn': 1})
        assert client.circuit_broken is True
        assert client.circuit_break_reason == expected_reason
        assert len(sleeps) == 1
        # 2.5s * [0.92, 1.08] => [2.30s, 2.70s] (< 0.44 QPS)
        assert 2.25 <= sleeps[0] <= 2.75
        with pytest.raises(RuntimeError, match='风控熔断已激活'):
            client.get_json('https://api.bilibili.com/x/web-interface/nav')
    finally:
        client.close()
        raw.close()


def test_p2_mid_flight_circuit_break_and_series_archives_fallback(tmp_path: Path) -> None:
    wh_path = tmp_path / 'warehouse.sqlite3'
    disc_path = tmp_path / 'discovery.sqlite3'
    _seed_synthetic_warehouse(wh_path)

    # pn=1 返回满 50 条且 total=80，但在拉 pn=2 时遇到 HTTP 412 -> 不得把半截数据标为 public
    def auth_handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == '/x/web-interface/nav':
            return httpx.Response(200, json={'code': 0, 'data': {'isLogin': True}})
        qs = parse_qs(req.url.query.decode('utf-8'))
        pn = int(qs['pn'][0])
        if pn == 1:
            items = [{'mid': 900000 + i, 'uname': f'U{i}', 'mtime': 1} for i in range(50)]
            return httpx.Response(200, json={'code': 0, 'data': {'total': 80, 'list': items}})
        return httpx.Response(412, text='Precondition Failed')

    raw_auth = httpx.Client(transport=httpx.MockTransport(auth_handler), follow_redirects=False)
    auth = AuthFollowingsClient({'SESSDATA': 'syn_sess'}, auth_interval=0.0, http_client=raw_auth)
    try:
        res = pull_followings(
            warehouse_db=wh_path,
            discovery_db=disc_path,
            per_tier=1,
            seed=99,
            execute=True,
            auth_client=auth,
        )
    finally:
        auth.close()
        raw_auth.close()

    assert res['circuit_broken'] is True
    assert res['circuit_break_reason'] == 'HTTP_412'
    with sqlite3.connect(disc_path) as conn:
        assert conn.execute('SELECT COUNT(*) FROM ext_viewer_follow_status').fetchone()[0] == 0
        assert conn.execute('SELECT COUNT(*) FROM ext_viewer_followings').fetchone()[0] == 0

    # 测试 seasons_series_list 内联 archives 为空时自动回查 /x/series/archives 命中 live_replay
    with open_discovery_db(disc_path, readonly_sources=[wh_path]) as conn:
        conn.execute(
            """
            INSERT INTO ext_viewer_followings (
                viewer_uid, followed_uid, wealth_tier, followed_mtime,
                followed_uname, official_type, official_desc, sign_text, synced_ts
            ) VALUES (200001, 610001, 'high_r', 1794000000, '回放分页主播', NULL, NULL, '', 1794900000)
            """
        )

    def guest_handler(req: httpx.Request) -> httpx.Response:
        path = req.url.path
        if path == '/room/v1/Room/get_status_info_by_uids':
            return httpx.Response(
                200,
                json={
                    'code': 0,
                    'data': {
                        '610001': {
                            'uid': 610001,
                            'room_id': 2310001,
                            'short_id': 0,
                            'uname': '回放分页主播',
                            'title': '下播休息',
                            'area_v2_parent_name': '单机游戏',
                            'area_v2_name': '独立游戏',
                            'live_status': 0,
                        }
                    },
                },
            )
        if path == '/xlive/app-room/v2/guardTab/topList':
            return httpx.Response(200, json={'code': 0, 'data': {'info': {'num': 18, 'page': 1}, 'list': [], 'top3': []}})
        if path == '/x/polymer/web-space/seasons_series_list':
            return httpx.Response(
                200,
                json={
                    'code': 0,
                    'data': {
                        'items_lists': {
                            'series_list': [
                                {
                                    'meta': {'category': 2, 'name': '直播回放', 'series_id': 555, 'mtime': NOW_TS - 3600},
                                    'archives': [],
                                }
                            ]
                        }
                    },
                },
            )
        if path == '/x/series/archives':
            return httpx.Response(
                200,
                json={'code': 0, 'data': {'archives': [{'aid': 99, 'pubdate': NOW_TS - 7200}]}},
            )
        raise AssertionError(f'unexpected {path}')

    raw_guest = httpx.Client(transport=httpx.MockTransport(guest_handler))
    guest = GuestDiscoveryClient(guest_interval=0.0, http_client=raw_guest)
    try:
        sc = screen_candidates(
            discovery_db=disc_path,
            warehouse_db=wh_path,
            max_endogenous=10,
            exogenous_per_area=0,
            active_days=14,
            now_ts=NOW_TS,
            guest_client=guest,
        )
    finally:
        guest.close()
        raw_guest.close()

    assert sc['active_evidence_counts'] == {'live_replay': 1}

