"""扩房发现与分层滚雪球采样纯离线工具 CLI（pull-followings / screen-candidates / sample-plan）。"""
from __future__ import annotations

import argparse
import json
import random
import re
import sqlite3
import time
import urllib.request
from collections import Counter, defaultdict
from collections.abc import Iterable
from http.cookies import CookieError, SimpleCookie
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from .direct_client import USER_AGENT, sign_wbi
from .live_share import atomic_json
from .warehouse import assert_distinct_warehouse_path, open_readonly_sqlite

ALLOWED_AUTH_URLS = frozenset(
    {
        'https://api.bilibili.com/x/web-interface/nav',
        'https://api.bilibili.com/x/relation/followings',
    }
)

OPUS_SNOWFLAKE_EPOCH_SEC = 1498838400

DEFAULT_PARENT_AREAS = ('虚拟主播', '娱乐', '电台', '单机游戏', '网游')

FALLBACK_PARENT_AREA_IDS: dict[str, int] = {
    '娱乐': 1,
    '网游': 2,
    '手游': 3,
    '电台': 5,
    '单机游戏': 6,
    '虚拟主播': 9,
    '生活': 10,
    '知识': 11,
    '赛事': 13,
    '聊天室': 14,
    '互动玩法': 15,
    '购物': 16,
}

HYGIENE_PATTERN = re.compile(
    r'(点唱厅|交友厅|考核厅|语音厅|男模|女模|主持人|24小时|24h|轮播|无人值守|助眠挂机)',
    re.IGNORECASE,
)

_PROTECTED_DB_MARKERS = frozenset(
    {'raw_events', 'timeline', 'segments', 'guard_snapshots', 'dim_users', 'fact_events'}
)

DISCOVERY_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS ext_viewer_follow_status (
    viewer_uid     INTEGER PRIMARY KEY,
    wealth_tier    TEXT NOT NULL,
    wealth_level   INTEGER NOT NULL DEFAULT 0,
    status         TEXT NOT NULL,
    reported_total INTEGER,
    fetched_pages  INTEGER NOT NULL DEFAULT 0,
    fetched_count  INTEGER NOT NULL DEFAULT 0,
    error_code     INTEGER,
    synced_ts      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS ext_viewer_followings (
    viewer_uid     INTEGER NOT NULL,
    followed_uid   INTEGER NOT NULL,
    wealth_tier    TEXT NOT NULL,
    followed_mtime INTEGER,
    followed_uname TEXT,
    official_type  INTEGER,
    official_desc  TEXT,
    sign_text      TEXT,
    synced_ts      REAL NOT NULL,
    PRIMARY KEY (viewer_uid, followed_uid)
);
CREATE INDEX IF NOT EXISTS idx_ext_followings_target ON ext_viewer_followings(followed_uid);

CREATE TABLE IF NOT EXISTS dim_candidate_rooms (
    anchor_uid       INTEGER PRIMARY KEY,
    room_id          INTEGER NOT NULL,
    short_id         INTEGER NOT NULL DEFAULT 0,
    capture_room_id  TEXT NOT NULL,
    uname            TEXT,
    title            TEXT,
    parent_area      TEXT,
    sub_area         TEXT,
    live_status      INTEGER NOT NULL DEFAULT 0,
    source_channel   TEXT NOT NULL,
    co_follow_total  INTEGER NOT NULL DEFAULT 0,
    co_follow_high_r INTEGER NOT NULL DEFAULT 0,
    co_follow_mid_r  INTEGER NOT NULL DEFAULT 0,
    co_follow_low_r  INTEGER NOT NULL DEFAULT 0,
    guard_count      INTEGER NOT NULL DEFAULT 0,
    guard_tier       TEXT NOT NULL,
    hygiene_flag     TEXT NOT NULL,
    active_14d       INTEGER NOT NULL DEFAULT 0,
    active_evidence  TEXT NOT NULL,
    latest_activity_ts INTEGER,
    screened_ts      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cand_rooms_capture ON dim_candidate_rooms(capture_room_id);
"""


def _sqlite_objects(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")
    }


def open_discovery_db(path: Path, *, readonly_sources: Iterable[Path] = ()) -> sqlite3.Connection:
    """打开或初始化独立发现工作库，拒绝覆盖采集源库或身份数仓。"""
    db_path = Path(path)
    if readonly_sources:
        assert_distinct_warehouse_path(db_path, readonly_sources)
    if db_path.exists() and db_path.stat().st_size > 0:
        with open_readonly_sqlite(db_path) as probe:
            existing = _sqlite_objects(probe)
            conflict = existing & _PROTECTED_DB_MARKERS
            if conflict:
                raise ValueError(f'目标文件包含采集库或数仓表 {sorted(conflict)}，拒绝作为发现工作库可写打开：{db_path}')
    resolved = db_path.resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(resolved, timeout=60.0)
    conn.executescript(DISCOVERY_DDL)
    return conn


def decode_opus_timestamp(opus_id: str | int | None) -> int | None:
    """将 B 站图文动态 opus_id 按高 32 位雪花位移还原为 UTC 秒级时间戳。"""
    if opus_id is None or isinstance(opus_id, bool):
        return None
    try:
        oid = int(str(opus_id).strip())
    except (TypeError, ValueError):
        return None
    if oid <= 0:
        return None
    return (oid >> 32) + OPUS_SNOWFLAKE_EPOCH_SEC


def classify_guard_tier(guard_count: int) -> str:
    count = max(0, int(guard_count or 0))
    if count >= 200:
        return 'head_200_plus'
    if count >= 30:
        return 'mid_30_199'
    if count >= 1:
        return 'tail_1_29'
    return 'zero_0'


def classify_wealth_tier(wealth_level: int) -> str | None:
    wl = int(wealth_level or 0)
    if wl >= 40:
        return 'high_r'
    if 25 <= wl <= 39:
        return 'mid_r'
    if 10 <= wl <= 24:
        return 'low_r'
    return None


def detect_hygiene_flag(*texts: str | None) -> str:
    """检测多人语音厅、考核厅或 24h 无人轮播关键词，透明打标不黑盒删除。"""
    for raw in texts:
        if not raw:
            continue
        match = HYGIENE_PATTERN.search(str(raw))
        if match:
            return f'flagged:{match.group(1).lower()}'
    return 'clean'


def _parse_csv_ints(values: Iterable[str] | None) -> set[int]:
    result: set[int] = set()
    for item in values or ():
        for token in str(item).split(','):
            tok = token.strip()
            if not tok:
                continue
            val = int(tok)
            if val > 0:
                result.add(val)
    return result


def _parse_csv_strings(values: Iterable[str] | None) -> set[str]:
    result: set[str] = set()
    for item in values or ():
        for token in str(item).split(','):
            tok = token.strip()
            if tok:
                result.add(tok)
    return result


def collect_seed_exclusions(
    *,
    warehouse_db: Path | None = None,
    exclude_rooms: Iterable[str] | None = None,
    exclude_uids: Iterable[str] | None = None,
    automation_configs: Iterable[Path] | None = None,
) -> tuple[set[str], set[int]]:
    """汇总需要排除的已知种子房间号集合与主播 UID 集合。"""
    room_ids = _parse_csv_strings(exclude_rooms)
    anchor_uids = _parse_csv_ints(exclude_uids)

    for cfg_path in automation_configs or ():
        payload = json.loads(Path(cfg_path).read_text(encoding='utf-8'))
        rooms_obj = payload.get('rooms')
        if isinstance(rooms_obj, dict):
            for key, cfg in rooms_obj.items():
                if str(key).strip():
                    room_ids.add(str(key).strip())
                if isinstance(cfg, dict):
                    for r_field in ('room_id', 'platform_room_id', 'short_id'):
                        if cfg.get(r_field):
                            room_ids.add(str(cfg[r_field]).strip())
                    if cfg.get('anchor_uid') and str(cfg['anchor_uid']).isdigit():
                        auid = int(cfg['anchor_uid'])
                        if auid > 0:
                            anchor_uids.add(auid)

    if warehouse_db is not None:
        with open_readonly_sqlite(warehouse_db) as wh:
            objs = _sqlite_objects(wh)
            if 'dim_rooms' in objs:
                for rid, auid in wh.execute('SELECT room_id, anchor_uid FROM dim_rooms'):
                    if rid:
                        room_ids.add(str(rid).strip())
                    if auid and int(auid) > 0:
                        anchor_uids.add(int(auid))
            if 'dim_sessions' in objs:
                for rid, auid in wh.execute('SELECT DISTINCT room_id, anchor_uid FROM dim_sessions'):
                    if rid:
                        room_ids.add(str(rid).strip())
                    if auid and int(auid) > 0:
                        anchor_uids.add(int(auid))

    return room_ids, anchor_uids


def read_cdp_cookies_readonly(cdp_url: str) -> tuple[dict[str, str], str]:
    """只读连接本机 CDP 提取 bilibili Cookie 后立即断开驱动，绝不调用 browser.close() 或触碰标签页。"""
    parsed = urlsplit(cdp_url)
    if parsed.scheme not in ('http', 'https') or parsed.hostname not in ('127.0.0.1', 'localhost', '::1'):
        raise ValueError('出于安全边界约束，--cdp-url 仅允许连接本机回环地址')
    base = cdp_url.rstrip('/')
    with urllib.request.urlopen(f'{base}/json/version', timeout=5.0) as resp:
        ver_before = json.loads(resp.read().decode('utf-8'))

    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    try:
        browser = pw.chromium.connect_over_cdp(base, timeout=10000)
        if not browser.contexts:
            raise RuntimeError('CDP 实例中未找到活动浏览器上下文')
        raw_cookies = browser.contexts[0].cookies(
            ['https://www.bilibili.com', 'https://api.bilibili.com']
        )
    finally:
        # 仅停止本地 Playwright 协议连接，严禁调用 browser.close()
        pw.stop()

    with urllib.request.urlopen(f'{base}/json/version', timeout=5.0) as resp:
        ver_after = json.loads(resp.read().decode('utf-8'))
    if ver_before.get('webSocketDebuggerUrl') != ver_after.get('webSocketDebuggerUrl'):
        raise RuntimeError('CDP 只读提取后 webSocketDebuggerUrl 发生变化，拒绝继续')

    cookie_map = {
        str(c['name']): str(c['value'])
        for c in raw_cookies
        if isinstance(c, dict) and c.get('name') and c.get('value')
    }
    if not cookie_map.get('SESSDATA'):
        raise ValueError('CDP 浏览器上下文未包含有效登录态 Cookie（缺少 SESSDATA）')
    user_agent = str(ver_before.get('User-Agent') or USER_AGENT)
    return cookie_map, user_agent


def load_cookie_file(cookie_file: Path) -> dict[str, str]:
    """从本地 JSON（字典或 Playwright cookies 数组）或 Header 文本读取 Cookie 到内存，绝不回显原值。"""
    text = Path(cookie_file).read_text(encoding='utf-8').strip()
    if not text:
        raise ValueError('Cookie 文件为空')
    cookie_map: dict[str, str] = {}
    try:
        if text[0] in ('{', '['):
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                if isinstance(parsed.get('cookies'), list):
                    items = parsed['cookies']
                    for item in items:
                        if isinstance(item, dict) and item.get('name') and item.get('value'):
                            cookie_map[str(item['name'])] = str(item['value'])
                else:
                    for k, v in parsed.items():
                        if isinstance(v, (str, int, float)) and str(v):
                            cookie_map[str(k)] = str(v)
            elif isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict) and item.get('name') and item.get('value'):
                        cookie_map[str(item['name'])] = str(item['value'])
        else:
            header = text.split(':', 1)[1].strip() if text.lower().startswith('cookie:') else text
            jar = SimpleCookie()
            jar.load(header)
            for name, morsel in jar.items():
                if morsel.value:
                    cookie_map[name] = morsel.value
    except (json.JSONDecodeError, CookieError) as exc:
        raise ValueError('Cookie 文件格式无效，无法解析') from exc

    if not cookie_map.get('SESSDATA'):
        raise ValueError('Cookie 文件未包含有效登录态 Cookie（缺少 SESSDATA）')
    return cookie_map


class AuthFollowingsClient:
    """硬编码双 URL 白名单、带抖动限速与即时风控熔断的小号只读客户端。"""

    def __init__(
        self,
        cookies: dict[str, str],
        *,
        user_agent: str = USER_AGENT,
        auth_interval: float = 2.5,
        rng: random.Random | None = None,
        http_client: httpx.Client | None = None,
        sleep_fn=time.sleep,
    ) -> None:
        self.auth_interval = max(0.0, float(auth_interval))
        self._rng = rng or random.Random(20261117)
        self._sleep = sleep_fn
        self._external_http = http_client is not None
        self._client = http_client or httpx.Client(
            headers={
                'User-Agent': user_agent,
                'Referer': 'https://space.bilibili.com/',
                'Origin': 'https://space.bilibili.com',
            },
            timeout=15.0,
            follow_redirects=False,
        )
        for k, v in cookies.items():
            self._client.cookies.set(k, v)
        self._last_req_mono: float | None = None
        self.request_count = 0
        self.circuit_broken = False
        self.circuit_break_reason: str | None = None

    def close(self) -> None:
        self._client.cookies.clear()
        if not self._external_http:
            self._client.close()

    @staticmethod
    def _assert_whitelisted(url: str) -> None:
        parts = urlsplit(url)
        normalized = f'{parts.scheme}://{parts.netloc}{parts.path}'
        if parts.query or parts.fragment or normalized not in ALLOWED_AUTH_URLS:
            raise PermissionError(f'拦截非白名单带鉴权请求 URL: {normalized}')

    def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        referer: str | None = None,
    ) -> dict[str, Any]:
        self._assert_whitelisted(url)
        if self.circuit_broken:
            raise RuntimeError(f'风控熔断已激活，禁止继续请求: {self.circuit_break_reason}')

        if self._last_req_mono is not None and self.auth_interval > 0:
            # 默认 2.5s 时落在 [2.30s, 2.70s]，严格 < 0.44 QPS
            target_gap = self.auth_interval * self._rng.uniform(0.92, 1.08)
            elapsed = time.monotonic() - self._last_req_mono
            if elapsed < target_gap:
                self._sleep(target_gap - elapsed)

        headers = {'Referer': referer} if referer else None
        resp = self._client.get(url, params=params, headers=headers)
        self._last_req_mono = time.monotonic()
        self.request_count += 1

        if resp.status_code in (412, 429):
            self.circuit_broken = True
            self.circuit_break_reason = f'HTTP_{resp.status_code}'
            return {'_http_status': resp.status_code, 'code': -resp.status_code}
        if 300 <= resp.status_code < 400:
            self.circuit_broken = True
            self.circuit_break_reason = f'HTTP_REDIRECT_{resp.status_code}'
            return {'_http_status': resp.status_code, 'code': -resp.status_code}
        if resp.status_code != 200:
            return {'_http_status': resp.status_code, 'code': -resp.status_code}

        try:
            payload = resp.json()
        except Exception:
            return {'_http_status': resp.status_code, 'code': -1}
        if not isinstance(payload, dict):
            return {'_http_status': resp.status_code, 'code': -1}

        code = payload.get('code')
        if code in (-352, -412, -101, -509, 429):
            self.circuit_broken = True
            self.circuit_break_reason = f'API_CODE_{code}'
        return payload


class GuestDiscoveryClient:
    """全程无登录态的游客发现客户端：禁止携带 SESSDATA，按需获取游客 buvid3 与 WBI 签名。"""

    def __init__(
        self,
        *,
        guest_interval: float = 0.2,
        http_client: httpx.Client | None = None,
        sleep_fn=time.sleep,
    ) -> None:
        self.guest_interval = max(0.0, float(guest_interval))
        self._sleep = sleep_fn
        self._external_http = http_client is not None
        self._client = http_client or httpx.Client(
            headers={
                'User-Agent': USER_AGENT,
                'Referer': 'https://live.bilibili.com/',
            },
            timeout=15.0,
            follow_redirects=True,
        )
        if 'SESSDATA' in self._client.cookies or 'bili_jct' in self._client.cookies:
            raise PermissionError('游客客户端检测到登录态 Cookie，拒绝初始化')
        self._last_req_mono: float | None = None
        self._wbi_keys: tuple[str, str] | None = None
        self.request_count = 0

    def close(self) -> None:
        if not self._external_http:
            self._client.close()

    def _throttle(self) -> None:
        if self._last_req_mono is not None and self.guest_interval > 0:
            elapsed = time.monotonic() - self._last_req_mono
            if elapsed < self.guest_interval:
                self._sleep(self.guest_interval - elapsed)

    def get_json(
        self,
        url: str,
        *,
        params: Any = None,
        referer: str | None = None,
    ) -> dict[str, Any]:
        if 'SESSDATA' in self._client.cookies or 'bili_jct' in self._client.cookies:
            raise PermissionError('游客客户端运行期检测到登录态 Cookie 注入，拒绝请求')
        self._throttle()
        headers = {'Referer': referer} if referer else None
        resp = self._client.get(url, params=params, headers=headers)
        self._last_req_mono = time.monotonic()
        self.request_count += 1
        if resp.status_code in (412, 429):
            raise RuntimeError(f'游客接口触发频控 HTTP {resp.status_code}: {url}')
        if resp.status_code != 200:
            return {'_http_status': resp.status_code, 'code': -resp.status_code}
        try:
            payload = resp.json()
        except Exception:
            return {'_http_status': resp.status_code, 'code': -1}
        if not isinstance(payload, dict):
            return {'_http_status': resp.status_code, 'code': -1}
        if payload.get('code') == -412:
            raise RuntimeError(f'游客接口触发业务频控 code=-412: {url}')
        return payload

    def ensure_guest_wbi(self, *, force_refresh: bool = False) -> tuple[str, str]:
        if self._wbi_keys is not None and not force_refresh:
            return self._wbi_keys
        spi = self.get_json('https://api.bilibili.com/x/frontend/finger/spi')
        if spi.get('code') == 0 and isinstance(spi.get('data'), dict):
            b3 = str(spi['data'].get('b_3') or '').strip()
            b4 = str(spi['data'].get('b_4') or '').strip()
            if b3:
                self._client.cookies.set('buvid3', b3)
            if b4:
                self._client.cookies.set('buvid4', b4)

        nav = self.get_json('https://api.bilibili.com/x/web-interface/nav')
        nav_data = nav.get('data') if isinstance(nav.get('data'), dict) else {}
        if nav_data.get('isLogin') is True:
            raise PermissionError('游客客户端意外处于登录态（isLogin=True），立即中止')
        wbi_img = nav_data.get('wbi_img') if isinstance(nav_data.get('wbi_img'), dict) else {}
        img_key = str(wbi_img.get('img_url') or '').rsplit('/', 1)[-1].split('.', 1)[0]
        sub_key = str(wbi_img.get('sub_url') or '').rsplit('/', 1)[-1].split('.', 1)[0]
        if len(img_key) < 32 or len(sub_key) < 32:
            raise RuntimeError('游客 nav 接口未返回有效 WBI 密钥')
        self._wbi_keys = (img_key, sub_key)
        return self._wbi_keys


def _load_stratified_viewers(
    warehouse_db: Path,
    excluded_uids: set[int],
) -> dict[str, list[tuple[int, int]]]:
    """从只读数仓提取非主播活跃实名观众，按最高荣耀等级切分三层。"""
    tiers: dict[str, list[tuple[int, int]]] = {'high_r': [], 'mid_r': [], 'low_r': []}
    with open_readonly_sqlite(warehouse_db) as wh:
        objs = _sqlite_objects(wh)
        rows: list[tuple[int, int]] = []
        if 'fact_session_presence' in objs:
            has_presence = wh.execute('SELECT 1 FROM fact_session_presence LIMIT 1').fetchone() is not None
            if has_presence:
                sql = (
                    'SELECT p.resolved_uid, '
                    'MAX(MAX(COALESCE(p.max_wealth_level, 0), COALESCE(u.max_wealth_level, 0))) AS wl '
                    'FROM fact_session_presence p '
                    'LEFT JOIN dim_users u ON u.uid = p.resolved_uid '
                    'WHERE p.resolved_uid IS NOT NULL AND p.resolved_uid > 0 '
                    '  AND (p.danmaku_count > 0 OR p.gift_event_count > 0 OR p.paid_milli_yuan > 0) '
                    'GROUP BY p.resolved_uid '
                    'ORDER BY p.resolved_uid ASC'
                ) if 'dim_users' in objs else (
                    'SELECT p.resolved_uid, MAX(COALESCE(p.max_wealth_level, 0)) AS wl '
                    'FROM fact_session_presence p '
                    'WHERE p.resolved_uid IS NOT NULL AND p.resolved_uid > 0 '
                    '  AND (p.danmaku_count > 0 OR p.gift_event_count > 0 OR p.paid_milli_yuan > 0) '
                    'GROUP BY p.resolved_uid '
                    'ORDER BY p.resolved_uid ASC'
                )
                rows = [(int(u), int(w or 0)) for u, w in wh.execute(sql)]

        if not rows and 'v_resolved_events' in objs:
            has_events = wh.execute('SELECT 1 FROM fact_events LIMIT 1').fetchone() is not None
            if has_events:
                sql = (
                    "SELECT e.resolved_uid, "
                    "MAX(MAX(COALESCE(e.wealth_level, 0), COALESCE(u.max_wealth_level, 0))) AS wl "
                    "FROM v_resolved_events e "
                    "LEFT JOIN dim_users u ON u.uid = e.resolved_uid "
                    "WHERE e.resolved_uid IS NOT NULL AND e.resolved_uid > 0 "
                    "  AND e.kind IN ('danmaku', 'gift', 'guard', 'sc') "
                    "GROUP BY e.resolved_uid "
                    "ORDER BY e.resolved_uid ASC"
                )
                rows = [(int(u), int(w or 0)) for u, w in wh.execute(sql)]

        if not rows and 'dim_users' in objs:
            rows = [
                (int(u), int(w or 0))
                for u, w in wh.execute(
                    'SELECT uid, max_wealth_level FROM dim_users WHERE uid > 0 ORDER BY uid ASC'
                )
            ]

    for uid, wl in rows:
        if uid in excluded_uids:
            continue
        tier = classify_wealth_tier(wl)
        if tier is not None:
            tiers[tier].append((uid, wl))
    return tiers


def _load_completed_viewer_uids(discovery_db: Path) -> set[int]:
    if not Path(discovery_db).is_file():
        return set()
    with open_readonly_sqlite(discovery_db) as conn:
        objs = _sqlite_objects(conn)
        if 'ext_viewer_follow_status' not in objs:
            return set()
        return {
            int(r[0])
            for r in conn.execute(
                "SELECT viewer_uid FROM ext_viewer_follow_status WHERE status IN ('public', 'hidden')"
            )
        }


def pull_followings(
    *,
    warehouse_db: Path,
    discovery_db: Path,
    per_tier: int = 50,
    seed: int = 20261117,
    exclude_uids: Iterable[str] | None = None,
    refresh: bool = False,
    execute: bool = False,
    cdp_url: str | None = None,
    cookie_file: Path | None = None,
    auth_interval: float = 2.5,
    auth_client: AuthFollowingsClient | None = None,
) -> dict[str, Any]:
    """第一阶段：按荣耀等级分层抽样实名观众，默认 dry-run，显式 execute 才用小号只读拉取最多 2 页公开关注。"""
    if per_tier < 1:
        raise ValueError('--per-tier 必须 >= 1')
    assert_distinct_warehouse_path(discovery_db, [warehouse_db])

    _, excluded_uids = collect_seed_exclusions(
        warehouse_db=warehouse_db,
        exclude_uids=exclude_uids,
    )
    pools = _load_stratified_viewers(warehouse_db, excluded_uids)
    done_uids = set() if refresh else _load_completed_viewer_uids(discovery_db)

    rng = random.Random(seed)
    sampled_by_tier: dict[str, list[tuple[int, int]]] = {}
    pool_counts: dict[str, int] = {}
    skipped_done_counts: dict[str, int] = {}
    planned_counts: dict[str, int] = {}

    for tier in ('high_r', 'mid_r', 'low_r'):
        full_pool = pools[tier]
        pending_pool = [item for item in full_pool if item[0] not in done_uids]
        k = min(per_tier, len(pending_pool))
        chosen = rng.sample(pending_pool, k) if k > 0 else []
        sampled_by_tier[tier] = chosen
        pool_counts[tier] = len(full_pool)
        skipped_done_counts[tier] = len(full_pool) - len(pending_pool)
        planned_counts[tier] = len(chosen)

    total_planned = sum(planned_counts.values())
    # 1 次 nav 登录态核验 + 每位观众最多 2 页（B 站对第三方固定封顶前 100 条）
    est_max_requests = (1 + 2 * total_planned) if total_planned > 0 else 0
    est_max_seconds = round(max(0, est_max_requests - 1) * float(auth_interval), 1)

    if not execute:
        return {
            'mode': 'dry_run',
            'seed': seed,
            'per_tier': per_tier,
            'refresh': refresh,
            'excluded_anchor_uid_count': len(excluded_uids),
            'pool_counts': pool_counts,
            'skipped_already_done_counts': skipped_done_counts,
            'planned_counts': planned_counts,
            'total_planned_viewers': total_planned,
            'max_pages_per_viewer': 2,
            'estimated_max_requests': est_max_requests,
            'estimated_max_seconds': est_max_seconds,
        }

    owns_client = auth_client is None
    if auth_client is None:
        if bool(cdp_url) == bool(cookie_file):
            raise ValueError('--execute 模式必须且只能指定 --cdp-url 或 --cookie-file 其中之一')
        if cdp_url:
            cookies, ua = read_cdp_cookies_readonly(cdp_url)
        else:
            assert cookie_file is not None
            cookies = load_cookie_file(cookie_file)
            ua = USER_AGENT
        try:
            auth_client = AuthFollowingsClient(
                cookies,
                user_agent=ua,
                auth_interval=auth_interval,
                rng=random.Random(seed + 1),
            )
        finally:
            cookies.clear()

    started = time.monotonic()
    tier_results: dict[str, dict[str, Any]] = {
        t: {
            'planned': planned_counts[t],
            'processed': 0,
            'public_count': 0,
            'hidden_count': 0,
            'error_count': 0,
            'edges_upserted': 0,
        }
        for t in ('high_r', 'mid_r', 'low_r')
    }
    total_edges = 0

    conn = open_discovery_db(discovery_db, readonly_sources=[warehouse_db])
    self_uid = 0
    try:
        if total_planned > 0:
            nav = auth_client.get_json('https://api.bilibili.com/x/web-interface/nav')
            nav_data = nav.get('data') if isinstance(nav.get('data'), dict) else {}
            if auth_client.circuit_broken or nav.get('code') != 0 or nav_data.get('isLogin') is not True:
                reason = auth_client.circuit_break_reason or f"code={nav.get('code')},isLogin={bool(nav_data.get('isLogin'))}"
                raise RuntimeError(f'小号登录态预检未通过（{reason}），已中止请求')
            raw_mid = nav_data.get('mid')
            if isinstance(raw_mid, int) and not isinstance(raw_mid, bool) and raw_mid > 0:
                self_uid = raw_mid

        for tier in ('high_r', 'mid_r', 'low_r'):
            for viewer_uid, wealth_level in sampled_by_tier[tier]:
                if auth_client.circuit_broken:
                    break
                if self_uid > 0 and viewer_uid == self_uid:
                    continue
                p1 = auth_client.get_json(
                    'https://api.bilibili.com/x/relation/followings',
                    params={'vmid': viewer_uid, 'pn': 1, 'ps': 50, 'order': 'desc'},
                    referer=f'https://space.bilibili.com/{viewer_uid}/fans/follow',
                )
                if auth_client.circuit_broken:
                    break

                code = p1.get('code')
                now_ts = time.time()
                if code in (22115, 22013):
                    with conn:
                        conn.execute('DELETE FROM ext_viewer_followings WHERE viewer_uid = ?', (viewer_uid,))
                        conn.execute(
                            """
                            INSERT INTO ext_viewer_follow_status (
                                viewer_uid, wealth_tier, wealth_level, status,
                                reported_total, fetched_pages, fetched_count, error_code, synced_ts
                            ) VALUES (?, ?, ?, 'hidden', NULL, 0, 0, ?, ?)
                            ON CONFLICT(viewer_uid) DO UPDATE SET
                                wealth_tier = excluded.wealth_tier,
                                wealth_level = excluded.wealth_level,
                                status = excluded.status,
                                reported_total = NULL,
                                fetched_pages = 0,
                                fetched_count = 0,
                                error_code = excluded.error_code,
                                synced_ts = excluded.synced_ts
                            """,
                            (viewer_uid, tier, wealth_level, int(code), now_ts),
                        )
                    tier_results[tier]['processed'] += 1
                    tier_results[tier]['hidden_count'] += 1
                    continue

                if code != 0:
                    with conn:
                        conn.execute(
                            """
                            INSERT INTO ext_viewer_follow_status (
                                viewer_uid, wealth_tier, wealth_level, status,
                                reported_total, fetched_pages, fetched_count, error_code, synced_ts
                            ) VALUES (?, ?, ?, 'error', NULL, 0, 0, ?, ?)
                            ON CONFLICT(viewer_uid) DO UPDATE SET
                                wealth_tier = excluded.wealth_tier,
                                wealth_level = excluded.wealth_level,
                                status = excluded.status,
                                error_code = excluded.error_code,
                                synced_ts = excluded.synced_ts
                            """,
                            (viewer_uid, tier, wealth_level, int(code) if isinstance(code, int) else -1, now_ts),
                        )
                    tier_results[tier]['processed'] += 1
                    tier_results[tier]['error_count'] += 1
                    continue

                data1 = p1.get('data') if isinstance(p1.get('data'), dict) else {}
                reported_total = int(data1.get('total') or 0)
                items = list(data1.get('list') or [])
                fetched_pages = 1

                # 第三方查询服务端硬上限为 2 页（100 条），仅第 1 页满 50 且 total > 50 时再拉 pn=2，到 pn=2 即停
                if len(items) >= 50 and reported_total > 50:
                    p2 = auth_client.get_json(
                        'https://api.bilibili.com/x/relation/followings',
                        params={'vmid': viewer_uid, 'pn': 2, 'ps': 50, 'order': 'desc'},
                        referer=f'https://space.bilibili.com/{viewer_uid}/fans/follow',
                    )
                    if auth_client.circuit_broken:
                        # 半途熔断时不提交当前观众半截状态，留待恢复后完整重拉
                        break
                    p2_code = p2.get('code')
                    if p2_code != 0:
                        with conn:
                            conn.execute(
                                """
                                INSERT INTO ext_viewer_follow_status (
                                    viewer_uid, wealth_tier, wealth_level, status,
                                    reported_total, fetched_pages, fetched_count, error_code, synced_ts
                                ) VALUES (?, ?, ?, 'error', ?, 1, 0, ?, ?)
                                ON CONFLICT(viewer_uid) DO UPDATE SET
                                    wealth_tier = excluded.wealth_tier,
                                    wealth_level = excluded.wealth_level,
                                    status = excluded.status,
                                    error_code = excluded.error_code,
                                    synced_ts = excluded.synced_ts
                                """,
                                (
                                    viewer_uid,
                                    tier,
                                    wealth_level,
                                    reported_total,
                                    int(p2_code) if isinstance(p2_code, int) else -1,
                                    now_ts,
                                ),
                            )
                        tier_results[tier]['processed'] += 1
                        tier_results[tier]['error_count'] += 1
                        continue
                    if isinstance(p2.get('data'), dict):
                        items2 = list(p2['data'].get('list') or [])
                        items.extend(items2)
                        fetched_pages = 2

                dedup_edges: dict[int, tuple[Any, ...]] = {}
                for entry in items:
                    if not isinstance(entry, dict):
                        continue
                    f_uid = int(entry.get('mid') or 0)
                    if f_uid <= 0 or (self_uid > 0 and f_uid == self_uid) or f_uid in dedup_edges:
                        continue
                    ov = entry.get('official_verify') if isinstance(entry.get('official_verify'), dict) else {}
                    ov_type = ov.get('type') if isinstance(ov.get('type'), int) else None
                    ov_desc = str(ov.get('desc') or '').strip() or None
                    mtime_raw = entry.get('mtime')
                    mtime = int(mtime_raw) if isinstance(mtime_raw, (int, float)) else None
                    uname = str(entry.get('uname') or '').strip() or None
                    sign_txt = str(entry.get('sign') or '').strip() or None
                    dedup_edges[f_uid] = (
                        viewer_uid,
                        f_uid,
                        tier,
                        mtime,
                        uname,
                        ov_type,
                        ov_desc,
                        sign_txt,
                        now_ts,
                    )

                with conn:
                    conn.execute('DELETE FROM ext_viewer_followings WHERE viewer_uid = ?', (viewer_uid,))
                    if dedup_edges:
                        conn.executemany(
                            """
                            INSERT INTO ext_viewer_followings (
                                viewer_uid, followed_uid, wealth_tier, followed_mtime,
                                followed_uname, official_type, official_desc, sign_text, synced_ts
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            list(dedup_edges.values()),
                        )
                    conn.execute(
                        """
                        INSERT INTO ext_viewer_follow_status (
                            viewer_uid, wealth_tier, wealth_level, status,
                            reported_total, fetched_pages, fetched_count, error_code, synced_ts
                        ) VALUES (?, ?, ?, 'public', ?, ?, ?, NULL, ?)
                        ON CONFLICT(viewer_uid) DO UPDATE SET
                            wealth_tier = excluded.wealth_tier,
                            wealth_level = excluded.wealth_level,
                            status = excluded.status,
                            reported_total = excluded.reported_total,
                            fetched_pages = excluded.fetched_pages,
                            fetched_count = excluded.fetched_count,
                            error_code = NULL,
                            synced_ts = excluded.synced_ts
                        """,
                        (viewer_uid, tier, wealth_level, reported_total, fetched_pages, len(dedup_edges), now_ts),
                    )
                tier_results[tier]['processed'] += 1
                tier_results[tier]['public_count'] += 1
                tier_results[tier]['edges_upserted'] += len(dedup_edges)
                total_edges += len(dedup_edges)
    finally:
        conn.close()
        if owns_client:
            auth_client.close()

    for t_stat in tier_results.values():
        proc = t_stat['processed']
        t_stat['public_rate'] = round(t_stat['public_count'] / proc, 4) if proc else 0.0
        t_stat['hidden_rate'] = round(t_stat['hidden_count'] / proc, 4) if proc else 0.0

    elapsed_sec = round(time.monotonic() - started, 2)
    return {
        'mode': 'execute',
        'seed': seed,
        'per_tier': per_tier,
        'refresh': refresh,
        'tiers': tier_results,
        'total_edges_upserted': total_edges,
        'authenticated_requests': auth_client.request_count,
        'elapsed_seconds': elapsed_sec,
        'circuit_broken': auth_client.circuit_broken,
        'circuit_break_reason': auth_client.circuit_break_reason,
    }


def _fetch_batch_rooms_by_uids(
    guest: GuestDiscoveryClient,
    uids: list[int],
) -> dict[int, dict[str, Any]]:
    """按每批 25 个 UID 免登录调用 get_status_info_by_uids，返回 room_id > 0 的直播间字典。"""
    found: dict[int, dict[str, Any]] = {}
    for offset in range(0, len(uids), 25):
        chunk = uids[offset:offset + 25]
        resp = guest.get_json(
            'https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids',
            params=[('uids[]', u) for u in chunk],
        )
        data = resp.get('data')
        if resp.get('code') != 0 or not isinstance(data, dict):
            continue
        for k, info in data.items():
            if not isinstance(info, dict):
                continue
            uid_val = int(info.get('uid') or (int(k) if str(k).isdigit() else 0))
            room_id = int(info.get('room_id') or 0)
            if uid_val > 0 and room_id > 0:
                found[uid_val] = info
    return found


def _resolve_parent_area_ids(guest: GuestDiscoveryClient, area_names: list[str]) -> dict[str, int]:
    mapping = dict(FALLBACK_PARENT_AREA_IDS)
    resp = guest.get_json('https://api.live.bilibili.com/room/v1/Area/getList')
    if resp.get('code') == 0 and isinstance(resp.get('data'), list):
        for entry in resp['data']:
            if isinstance(entry, dict) and entry.get('name') and entry.get('id'):
                mapping[str(entry['name']).strip()] = int(entry['id'])
    resolved: dict[str, int] = {}
    for name in area_names:
        if name not in mapping:
            raise ValueError(f'未知直播一级大区名称：{name}')
        resolved[name] = mapping[name]
    return resolved


def _fetch_exogenous_area_candidates(
    guest: GuestDiscoveryClient,
    parent_areas: list[str],
    per_area: int,
    excluded_rooms: set[str],
    excluded_uids: set[int],
) -> dict[int, dict[str, Any]]:
    """免登录抓取各主力大区当前在播房间，打破单一歌势种子同温层。"""
    if per_area <= 0 or not parent_areas:
        return {}
    area_id_map = _resolve_parent_area_ids(guest, parent_areas)
    raw_by_uid: dict[int, dict[str, Any]] = {}

    for area_name in parent_areas:
        pid = area_id_map[area_name]
        collected_for_area = 0
        page = 1
        page_size = min(50, max(20, per_area * 2))
        while collected_for_area < per_area and page <= 3:
            resp = guest.get_json(
                'https://api.live.bilibili.com/room/v3/area/getRoomList',
                params={
                    'parent_area_id': pid,
                    'area_id': 0,
                    'sort_type': 'online',
                    'page': page,
                    'page_size': page_size,
                    'platform': 'web',
                },
            )
            rows = ((resp.get('data') or {}).get('list')) if isinstance(resp.get('data'), dict) else None
            if resp.get('code') != 0 or not isinstance(rows, list) or not rows:
                break
            for item in rows:
                if not isinstance(item, dict):
                    continue
                uid = int(item.get('uid') or 0)
                rid = int(item.get('roomid') or item.get('room_id') or 0)
                if uid <= 0 or rid <= 0:
                    continue
                if uid in excluded_uids or str(rid) in excluded_rooms or uid in raw_by_uid:
                    continue
                verify_obj = item.get('verify') if isinstance(item.get('verify'), dict) else {}
                raw_by_uid[uid] = {
                    'uid': uid,
                    'room_id': rid,
                    'short_id': 0,
                    'uname': str(item.get('uname') or ''),
                    'title': str(item.get('title') or ''),
                    'area_v2_parent_name': str(
                        item.get('area_v2_parent_name') or item.get('parent_name') or area_name
                    ),
                    'area_v2_name': str(item.get('area_v2_name') or item.get('area_name') or ''),
                    'live_status': 1,
                    'official_desc': str(verify_obj.get('desc') or ''),
                }
                collected_for_area += 1
                if collected_for_area >= per_area:
                    break
            if len(rows) < page_size:
                break
            page += 1

    # 批量补齐规范长短房号与标签信息
    enriched = _fetch_batch_rooms_by_uids(guest, list(raw_by_uid))
    result: dict[int, dict[str, Any]] = {}
    for uid, base in raw_by_uid.items():
        b_info = enriched.get(uid)
        if b_info:
            rid = int(b_info.get('room_id') or base['room_id'])
            sid = int(b_info.get('short_id') or 0)
            if str(rid) in excluded_rooms or (sid > 0 and str(sid) in excluded_rooms):
                continue
            merged = {**base, **b_info, 'room_id': rid, 'short_id': sid}
            result[uid] = merged
        else:
            result[uid] = base
    return result


def _check_streamer_activity_and_guard(
    guest: GuestDiscoveryClient,
    *,
    uid: int,
    room_id: int,
    live_status: int,
    now_ts: float,
    active_days: int,
) -> tuple[int, bool, str, int | None]:
    """免登录获取大航海在舰数并执行两级近 N 天开播活跃度核验。"""
    cutoff_ts = int(now_ts - active_days * 86400)

    guard_resp = guest.get_json(
        'https://api.live.bilibili.com/xlive/app-room/v2/guardTab/topList',
        params={'roomid': room_id, 'ruid': uid, 'page': 1, 'page_size': 1},
    )
    guard_info = ((guard_resp.get('data') or {}).get('info')) if isinstance(guard_resp.get('data'), dict) else {}
    guard_count = int((guard_info or {}).get('num') or 0) if guard_resp.get('code') == 0 else 0

    # 1. 当前正在开播
    if int(live_status or 0) == 1:
        return guard_count, True, 'live_now', int(now_ts)

    best_seen_ts: int | None = None

    # 2a. 第一级精确直播信号：空间直播回放合集（category == 2）
    series_resp = guest.get_json(
        'https://api.bilibili.com/x/polymer/web-space/seasons_series_list',
        params={'mid': uid, 'page_num': 1, 'page_size': 5},
        referer=f'https://space.bilibili.com/{uid}/lists',
    )
    items_lists = ((series_resp.get('data') or {}).get('items_lists')) if isinstance(series_resp.get('data'), dict) else {}
    series_list = (items_lists or {}).get('series_list') or []
    for s_item in series_list:
        if not isinstance(s_item, dict):
            continue
        meta = s_item.get('meta') if isinstance(s_item.get('meta'), dict) else {}
        if meta.get('category') != 2 and '直播回放' not in str(meta.get('name') or ''):
            continue
        archives = list(s_item.get('archives') or [])
        pub_times = [
            int(a.get('pubdate') or a.get('ctime') or 0)
            for a in archives
            if isinstance(a, dict) and (a.get('pubdate') or a.get('ctime'))
        ]
        if (not pub_times or max(pub_times) < cutoff_ts) and int(meta.get('mtime') or 0) >= cutoff_ts and meta.get('series_id'):
            arc_resp = guest.get_json(
                'https://api.bilibili.com/x/series/archives',
                params={
                    'mid': uid,
                    'series_id': meta['series_id'],
                    'only_normal': 'true',
                    'sort': 'desc',
                    'pn': 1,
                    'ps': 5,
                },
                referer=f'https://space.bilibili.com/{uid}/lists',
            )
            arc_list = ((arc_resp.get('data') or {}).get('archives')) if isinstance(arc_resp.get('data'), dict) else []
            pub_times.extend(
                int(a.get('pubdate') or a.get('ctime') or 0)
                for a in (arc_list or [])
                if isinstance(a, dict) and (a.get('pubdate') or a.get('ctime'))
            )
        if pub_times:
            latest_rep = max(pub_times)
            best_seen_ts = max(best_seen_ts or 0, latest_rep)
            if latest_rep >= cutoff_ts:
                return guard_count, True, 'live_replay', latest_rep

    # 2b. 第一级精确直播信号：空间近期或未来窗口内直播预约
    resv_resp = guest.get_json(
        'https://api.bilibili.com/x/space/reservation',
        params={'vmid': uid},
        referer=f'https://space.bilibili.com/{uid}',
    )
    resv_data = resv_resp.get('data') if isinstance(resv_resp.get('data'), list) else []
    horizon_ts = int(now_ts + active_days * 86400)
    resv_times: list[int] = []
    for item in resv_data:
        if not isinstance(item, dict):
            continue
        plan_ts = int(item.get('live_plan_start_time') or 0)
        ctime_ts = int(item.get('ctime') or 0)
        if plan_ts > 0:
            if cutoff_ts <= plan_ts <= horizon_ts:
                resv_times.append(plan_ts)
        elif cutoff_ts <= ctime_ts <= horizon_ts:
            resv_times.append(ctime_ts)
    if resv_times:
        latest_resv = max(resv_times)
        best_seen_ts = max(best_seen_ts or 0, latest_resv)
        return guard_count, True, 'live_reservation', latest_resv

    # 3. 第二级不留回放歌势主播兜底：必须同时满足 guard_count > 0 且近期有 opus 动态
    if guard_count > 0:
        for attempt in range(2):
            img_key, sub_key = guest.ensure_guest_wbi(force_refresh=attempt > 0)
            opus_resp = guest.get_json(
                'https://api.bilibili.com/x/polymer/web-dynamic/v1/opus/feed/space',
                params=sign_wbi({'host_mid': uid, 'page': 1}, img_key, sub_key, now=now_ts),
                referer=f'https://space.bilibili.com/{uid}/dynamic',
            )
            if opus_resp.get('code') == -352 and attempt == 0:
                continue
            opus_items = ((opus_resp.get('data') or {}).get('items')) if isinstance(opus_resp.get('data'), dict) else []
            opus_times = [
                ts
                for it in (opus_items or [])
                if isinstance(it, dict) and (ts := decode_opus_timestamp(it.get('opus_id'))) is not None
            ]
            if opus_times:
                latest_opus = max(opus_times)
                best_seen_ts = max(best_seen_ts or 0, latest_opus)
                if latest_opus >= cutoff_ts:
                    return guard_count, True, 'guard_plus_recent_opus', latest_opus
            break

    return guard_count, False, 'inactive', best_seen_ts


def screen_candidates(
    *,
    discovery_db: Path,
    warehouse_db: Path | None = None,
    exclude_rooms: Iterable[str] | None = None,
    exclude_uids: Iterable[str] | None = None,
    automation_configs: Iterable[Path] | None = None,
    max_endogenous: int = 300,
    exogenous_per_area: int = 15,
    parent_areas: Iterable[str] = DEFAULT_PARENT_AREAS,
    active_days: int = 14,
    guest_interval: float = 0.2,
    now_ts: float | None = None,
    guest_client: GuestDiscoveryClient | None = None,
) -> dict[str, Any]:
    """第二阶段：全程免登录筛出直播间、补齐跨赛道外生样本、清洗厅房轮播并核验两级活跃度与舰长档位。"""
    if max_endogenous < 0 or exogenous_per_area < 0:
        raise ValueError('--max-endogenous 与 --exogenous-per-area 不能为负数')
    if active_days < 1:
        raise ValueError('--active-days 必须 >= 1')

    readonly_sources = [warehouse_db] if warehouse_db is not None else []
    excluded_rooms, excluded_uids = collect_seed_exclusions(
        warehouse_db=warehouse_db,
        exclude_rooms=exclude_rooms,
        exclude_uids=exclude_uids,
        automation_configs=automation_configs,
    )
    area_list = [a.strip() for a in parent_areas if a and a.strip()]
    eval_now = float(time.time() if now_ts is None else now_ts)

    conn = open_discovery_db(discovery_db, readonly_sources=readonly_sources)
    owns_guest = guest_client is None
    guest = guest_client or GuestDiscoveryClient(guest_interval=guest_interval)
    started = time.monotonic()

    try:
        endo_meta: dict[int, dict[str, Any]] = {}
        if max_endogenous > 0:
            rows = conn.execute(
                """
                SELECT
                    followed_uid,
                    COUNT(DISTINCT viewer_uid) AS co_total,
                    COUNT(DISTINCT CASE WHEN wealth_tier = 'high_r' THEN viewer_uid END) AS co_high,
                    COUNT(DISTINCT CASE WHEN wealth_tier = 'mid_r' THEN viewer_uid END) AS co_mid,
                    COUNT(DISTINCT CASE WHEN wealth_tier = 'low_r' THEN viewer_uid END) AS co_low,
                    MAX(followed_uname) AS uname,
                    MAX(official_desc) AS official_desc,
                    MAX(sign_text) AS sign_text
                FROM ext_viewer_followings
                GROUP BY followed_uid
                ORDER BY co_total DESC, co_high DESC, followed_uid ASC
                """
            ).fetchall()
            for r in rows:
                f_uid = int(r[0])
                if f_uid <= 0 or f_uid in excluded_uids:
                    continue
                endo_meta[f_uid] = {
                    'co_total': int(r[1]),
                    'co_high': int(r[2]),
                    'co_mid': int(r[3]),
                    'co_low': int(r[4]),
                    'uname': r[5] or '',
                    'official_desc': r[6] or '',
                    'sign_text': r[7] or '',
                }
                if len(endo_meta) >= max_endogenous:
                    break

        endo_rooms = _fetch_batch_rooms_by_uids(guest, list(endo_meta)) if endo_meta else {}
        exo_rooms = (
            _fetch_exogenous_area_candidates(
                guest,
                area_list,
                exogenous_per_area,
                excluded_rooms,
                excluded_uids,
            )
            if exogenous_per_area > 0
            else {}
        )

        # 合并内生与外生候选
        merged_uids: list[int] = list(
            dict.fromkeys(list(endo_rooms.keys()) + list(exo_rooms.keys()))
        )

        source_counts: Counter[str] = Counter()
        hygiene_counts: Counter[str] = Counter()
        evidence_counts: Counter[str] = Counter()
        area_active_counts: Counter[str] = Counter()
        guard_tier_active_counts: Counter[str] = Counter()
        screened_count = 0
        skipped_seed_rooms = 0

        for uid in merged_uids:
            in_endo = uid in endo_rooms
            in_exo = uid in exo_rooms
            r_info = endo_rooms[uid] if in_endo else exo_rooms[uid]
            room_id = int(r_info.get('room_id') or 0)
            short_id = int(r_info.get('short_id') or 0)
            capture_id = str(short_id) if short_id > 0 else str(room_id)
            if room_id <= 0 or str(room_id) in excluded_rooms or capture_id in excluded_rooms:
                skipped_seed_rooms += 1
                continue

            source_channel = 'both' if (in_endo and in_exo) else ('endogenous' if in_endo else 'exogenous')
            em = endo_meta.get(uid, {})
            exo_m = exo_rooms.get(uid, {})
            uname = str(r_info.get('uname') or em.get('uname') or exo_m.get('uname') or '').strip()
            title = str(r_info.get('title') or exo_m.get('title') or '').strip()
            parent_area = str(r_info.get('area_v2_parent_name') or exo_m.get('area_v2_parent_name') or '').strip()
            sub_area = str(r_info.get('area_v2_name') or exo_m.get('area_v2_name') or '').strip()
            live_status = int(r_info.get('live_status') if r_info.get('live_status') is not None else 0)
            tags = str(r_info.get('tags') or r_info.get('tag_name') or '').strip()
            sign_text = str(em.get('sign_text') or '').strip()
            official_desc = str(em.get('official_desc') or exo_m.get('official_desc') or '').strip()

            hygiene_flag = detect_hygiene_flag(title, uname, sign_text, official_desc, tags)
            guard_count, is_active, evidence, latest_ts = _check_streamer_activity_and_guard(
                guest,
                uid=uid,
                room_id=room_id,
                live_status=live_status,
                now_ts=eval_now,
                active_days=active_days,
            )
            guard_tier = classify_guard_tier(guard_count)
            row_ts = time.time()

            with conn:
                conn.execute(
                    """
                    INSERT INTO dim_candidate_rooms (
                        anchor_uid, room_id, short_id, capture_room_id, uname, title,
                        parent_area, sub_area, live_status, source_channel,
                        co_follow_total, co_follow_high_r, co_follow_mid_r, co_follow_low_r,
                        guard_count, guard_tier, hygiene_flag, active_14d,
                        active_evidence, latest_activity_ts, screened_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(anchor_uid) DO UPDATE SET
                        room_id = excluded.room_id,
                        short_id = excluded.short_id,
                        capture_room_id = excluded.capture_room_id,
                        uname = excluded.uname,
                        title = excluded.title,
                        parent_area = excluded.parent_area,
                        sub_area = excluded.sub_area,
                        live_status = excluded.live_status,
                        source_channel = CASE
                            WHEN dim_candidate_rooms.source_channel != excluded.source_channel
                                 AND (dim_candidate_rooms.source_channel = 'both'
                                      OR (dim_candidate_rooms.source_channel IN ('endogenous', 'exogenous')
                                          AND excluded.source_channel IN ('endogenous', 'exogenous')))
                            THEN 'both'
                            ELSE excluded.source_channel
                        END,
                        co_follow_total = MAX(dim_candidate_rooms.co_follow_total, excluded.co_follow_total),
                        co_follow_high_r = MAX(dim_candidate_rooms.co_follow_high_r, excluded.co_follow_high_r),
                        co_follow_mid_r = MAX(dim_candidate_rooms.co_follow_mid_r, excluded.co_follow_mid_r),
                        co_follow_low_r = MAX(dim_candidate_rooms.co_follow_low_r, excluded.co_follow_low_r),
                        guard_count = excluded.guard_count,
                        guard_tier = excluded.guard_tier,
                        hygiene_flag = excluded.hygiene_flag,
                        active_14d = excluded.active_14d,
                        active_evidence = excluded.active_evidence,
                        latest_activity_ts = excluded.latest_activity_ts,
                        screened_ts = excluded.screened_ts
                    """,
                    (
                        uid,
                        room_id,
                        short_id,
                        capture_id,
                        uname,
                        title,
                        parent_area,
                        sub_area,
                        live_status,
                        source_channel,
                        int(em.get('co_total') or 0),
                        int(em.get('co_high') or 0),
                        int(em.get('co_mid') or 0),
                        int(em.get('co_low') or 0),
                        guard_count,
                        guard_tier,
                        hygiene_flag,
                        1 if is_active else 0,
                        evidence,
                        latest_ts,
                        row_ts,
                    ),
                )

            screened_count += 1
            source_counts[source_channel] += 1
            hygiene_counts['clean' if hygiene_flag == 'clean' else 'flagged'] += 1
            evidence_counts[evidence] += 1
            if is_active and hygiene_flag == 'clean':
                area_active_counts[parent_area or '未分类'] += 1
                guard_tier_active_counts[guard_tier] += 1
    finally:
        conn.close()
        if owns_guest:
            guest.close()

    return {
        'active_days': active_days,
        'endogenous_uids_probed': len(endo_meta),
        'endogenous_with_room': len(endo_rooms),
        'exogenous_with_room': len(exo_rooms),
        'skipped_seed_rooms': skipped_seed_rooms,
        'screened_candidates': screened_count,
        'source_channel_counts': dict(source_counts),
        'hygiene_summary': dict(hygiene_counts),
        'active_evidence_counts': dict(evidence_counts),
        'clean_active_by_parent_area': dict(area_active_counts),
        'clean_active_by_guard_tier': dict(guard_tier_active_counts),
        'guest_requests': guest.request_count,
        'elapsed_seconds': round(time.monotonic() - started, 2),
    }


def _round_robin_stratified_pick(
    candidates: list[dict[str, Any]],
    *,
    quota: int,
    prefer_cofollow: bool,
    max_per_subarea: int,
    subarea_counts: Counter[tuple[str, str]],
    selected_uids: set[int],
    rng: random.Random,
) -> list[dict[str, Any]]:
    if quota <= 0 or not candidates:
        return []

    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    decorated: list[tuple[float, dict[str, Any]]] = [(rng.random(), c) for c in candidates]
    if prefer_cofollow:
        decorated.sort(
            key=lambda pair: (
                -int(pair[1]['co_follow_total']),
                -int(pair[1]['co_follow_high_r']),
                pair[0],
                int(pair[1]['anchor_uid']),
            )
        )
    else:
        decorated.sort(key=lambda pair: (pair[0], int(pair[1]['anchor_uid'])))

    for _, cand in decorated:
        key = (cand['parent_area'] or '未分类', cand['guard_tier'])
        buckets[key].append(cand)

    strata = sorted(buckets.keys())
    rng.shuffle(strata)
    cursors = {s: 0 for s in strata}
    picked: list[dict[str, Any]] = []

    while len(picked) < quota:
        progressed = False
        for s in strata:
            pool = buckets[s]
            idx = cursors[s]
            while idx < len(pool):
                cand = pool[idx]
                idx += 1
                uid = int(cand['anchor_uid'])
                if uid in selected_uids:
                    continue
                sub_key = (cand['parent_area'] or '未分类', cand['sub_area'] or '未分类')
                if max_per_subarea > 0 and subarea_counts[sub_key] >= max_per_subarea:
                    continue
                selected_uids.add(uid)
                subarea_counts[sub_key] += 1
                picked.append(cand)
                progressed = True
                break
            cursors[s] = idx
            if len(picked) >= quota:
                break
        if not progressed:
            break

    return picked


def sample_plan(
    *,
    discovery_db: Path,
    warehouse_db: Path | None = None,
    exclude_rooms: Iterable[str] | None = None,
    exclude_uids: Iterable[str] | None = None,
    automation_configs: Iterable[Path] | None = None,
    target_count: int = 60,
    endogenous_ratio: float = 0.7,
    max_per_subarea: int = 8,
    seed: int = 20261117,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """第三阶段：纯本地只读确定性计算，按 (parent_area, guard_tier) 与内外生配额生成均衡扩房抽样计划。"""
    if target_count < 1:
        raise ValueError('--target-count 必须 >= 1')
    if not 0.0 <= endogenous_ratio <= 1.0:
        raise ValueError('--endogenous-ratio 必须在 [0.0, 1.0] 范围内')
    if max_per_subarea < 0:
        raise ValueError('--max-per-subarea 不能为负数')

    if output_path is not None:
        out_p = Path(output_path)
        protected_inputs: list[Path] = [Path(discovery_db)]
        if warehouse_db is not None:
            protected_inputs.append(Path(warehouse_db))
        protected_inputs.extend(Path(p) for p in (automation_configs or ()))
        assert_distinct_warehouse_path(out_p, protected_inputs)
        if out_p.is_file():
            with out_p.open('rb') as fh:
                if fh.read(16) == b'SQLite format 3\x00':
                    raise ValueError(f'--output 目标已存在且为 SQLite 数据库文件，拒绝覆盖：{out_p}')

    excluded_rooms, excluded_uids = collect_seed_exclusions(
        warehouse_db=warehouse_db,
        exclude_rooms=exclude_rooms,
        exclude_uids=exclude_uids,
        automation_configs=automation_configs,
    )

    with open_readonly_sqlite(discovery_db) as conn:
        objs = _sqlite_objects(conn)
        if 'dim_candidate_rooms' not in objs:
            raise ValueError(f'发现库中不存在 dim_candidate_rooms 表：{discovery_db}')
        cur = conn.execute(
            """
            SELECT
                anchor_uid, room_id, short_id, capture_room_id, uname, title,
                parent_area, sub_area, live_status, source_channel,
                co_follow_total, co_follow_high_r, co_follow_mid_r, co_follow_low_r,
                guard_count, guard_tier, hygiene_flag, active_14d,
                active_evidence, latest_activity_ts
            FROM dim_candidate_rooms
            ORDER BY anchor_uid ASC
            """
        )
        cols = [d[0] for d in cur.description]
        all_rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    excluded_seed_cnt = 0
    inactive_cnt = 0
    hygiene_filtered_cnt = 0
    eligible: list[dict[str, Any]] = []

    for row in all_rows:
        uid = int(row['anchor_uid'])
        rid = str(row['room_id'])
        sid = str(row['short_id']) if int(row['short_id'] or 0) > 0 else ''
        cap = str(row['capture_room_id'])
        if (
            uid in excluded_uids
            or rid in excluded_rooms
            or cap in excluded_rooms
            or (sid and sid in excluded_rooms)
        ):
            excluded_seed_cnt += 1
            continue
        if int(row['active_14d'] or 0) != 1:
            inactive_cnt += 1
            continue
        if row['hygiene_flag'] != 'clean':
            hygiene_filtered_cnt += 1
            continue
        eligible.append(row)

    endo_pool = [r for r in eligible if r['source_channel'] in ('endogenous', 'both')]
    exo_pool = [r for r in eligible if r['source_channel'] == 'exogenous']

    endo_quota = round(target_count * endogenous_ratio)
    exo_quota = target_count - endo_quota

    rng = random.Random(seed)
    subarea_counts: Counter[tuple[str, str]] = Counter()
    selected_uids: set[int] = set()

    picked_endo = _round_robin_stratified_pick(
        endo_pool,
        quota=endo_quota,
        prefer_cofollow=True,
        max_per_subarea=max_per_subarea,
        subarea_counts=subarea_counts,
        selected_uids=selected_uids,
        rng=rng,
    )
    picked_exo = _round_robin_stratified_pick(
        exo_pool,
        quota=exo_quota,
        prefer_cofollow=False,
        max_per_subarea=max_per_subarea,
        subarea_counts=subarea_counts,
        selected_uids=selected_uids,
        rng=rng,
    )

    selected = list(picked_endo) + list(picked_exo)
    # 若一侧池子人数不足配额，允许另一侧合格剩余候选在不突破子分区上限的前提下补齐总目标数
    shortfall = target_count - len(selected)
    if shortfall > 0:
        selected.extend(
            _round_robin_stratified_pick(
                endo_pool,
                quota=shortfall,
                prefer_cofollow=True,
                max_per_subarea=max_per_subarea,
                subarea_counts=subarea_counts,
                selected_uids=selected_uids,
                rng=rng,
            )
        )
    shortfall = target_count - len(selected)
    if shortfall > 0:
        selected.extend(
            _round_robin_stratified_pick(
                exo_pool,
                quota=shortfall,
                prefer_cofollow=False,
                max_per_subarea=max_per_subarea,
                subarea_counts=subarea_counts,
                selected_uids=selected_uids,
                rng=rng,
            )
        )

    by_channel = Counter(r['source_channel'] for r in selected)
    by_parent = Counter(r['parent_area'] or '未分类' for r in selected)
    by_subarea = Counter(f"{r['parent_area'] or '未分类'}/{r['sub_area'] or '未分类'}" for r in selected)
    by_guard = Counter(r['guard_tier'] for r in selected)
    by_stratum = Counter(f"{r['parent_area'] or '未分类'}|{r['guard_tier']}" for r in selected)

    plan_payload = {
        'parameters': {
            'target_count': target_count,
            'endogenous_ratio': endogenous_ratio,
            'endogenous_quota': endo_quota,
            'exogenous_quota': exo_quota,
            'max_per_subarea': max_per_subarea,
            'seed': seed,
        },
        'funnel': {
            'total_candidates_in_db': len(all_rows),
            'excluded_seed_count': excluded_seed_cnt,
            'excluded_inactive_count': inactive_cnt,
            'excluded_hygiene_flagged_count': hygiene_filtered_cnt,
            'eligible_clean_active_count': len(eligible),
            'eligible_endogenous_count': len(endo_pool),
            'eligible_exogenous_count': len(exo_pool),
            'selected_count': len(selected),
        },
        'distribution': {
            'by_source_channel': dict(by_channel),
            'by_parent_area': dict(by_parent),
            'by_sub_area': dict(by_subarea),
            'by_guard_tier': dict(by_guard),
            'by_area_and_guard_stratum': dict(by_stratum),
        },
        'selected_rooms': [
            {
                'capture_room_id': r['capture_room_id'],
                'room_id': int(r['room_id']),
                'short_id': int(r['short_id'] or 0),
                'anchor_uid': int(r['anchor_uid']),
                'uname': r['uname'],
                'title': r['title'],
                'parent_area': r['parent_area'],
                'sub_area': r['sub_area'],
                'guard_count': int(r['guard_count'] or 0),
                'guard_tier': r['guard_tier'],
                'source_channel': r['source_channel'],
                'co_follow_total': int(r['co_follow_total'] or 0),
                'co_follow_high_r': int(r['co_follow_high_r'] or 0),
                'active_evidence': r['active_evidence'],
            }
            for r in selected
        ],
    }

    if output_path is not None:
        atomic_json(Path(output_path), plan_payload)
    return plan_payload


def configure_parser(subparsers: argparse._SubParsersAction) -> None:
    rd = subparsers.add_parser('room-discovery', help='扩房发现与分层滚雪球采样离线工具')
    sub = rd.add_subparsers(dest='discovery_cmd', required=True)

    pull = sub.add_parser('pull-followings', help='按荣耀等级分层抽样观众并拉取公开关注前 2 页（默认 --dry-run）')
    pull.add_argument('--warehouse-db', type=Path, required=True, help='只读打开的身份消解数仓路径')
    pull.add_argument('--db', '--discovery-db', dest='db', type=Path, required=True, help='独立发现工作库 SQLite 路径')
    pull.add_argument('--per-tier', type=int, default=50, help='大/中/小 R 每层抽样人数（默认 50）')
    pull.add_argument('--seed', type=int, default=20261117, help='确定性随机种子')
    pull.add_argument('--exclude-uids', action='append', default=[], help='额外排除的 UID（逗号分隔或多次传入）')
    pull.add_argument('--refresh', action='store_true', help='忽略已完成状态重新拉取选中观众')
    pull.add_argument('--cdp-url', help='小号浏览器只读 CDP 地址（仅内存读取 Cookie 后立即断开）')
    pull.add_argument('--cookie-file', type=Path, help='本地 Cookie JSON 或 Header 文件路径（仅内存读取）')
    pull.add_argument('--auth-interval', type=float, default=2.5, help='带登录态请求基准间隔秒数（默认 2.5s）')
    mode = pull.add_mutually_exclusive_group()
    mode.add_argument('--dry-run', action='store_true', help='仅计算抽样与请求配额预演，不发起网络请求（默认）')
    mode.add_argument('--execute', action='store_true', help='显式执行小号只读关注拉取并逐人事务落盘')

    screen = sub.add_parser('screen-candidates', help='全程免登录筛房、两级近 14 天活跃度核验与跨赛道外生补齐')
    screen.add_argument('--db', '--discovery-db', dest='db', type=Path, required=True, help='发现工作库 SQLite 路径')
    screen.add_argument('--warehouse-db', type=Path, help='可选：只读读取数仓自动排除已知种子房与主播 UID')
    screen.add_argument('--exclude-rooms', action='append', default=[], help='排除的种子房号（逗号分隔或多次传入）')
    screen.add_argument('--exclude-uids', action='append', default=[], help='排除的种子主播 UID（逗号分隔或多次传入）')
    screen.add_argument(
        '--exclude-automation-config',
        action='append',
        type=Path,
        default=[],
        help='只读读取 automation.json 自动排除已配置的种子房间',
    )
    screen.add_argument('--max-endogenous', type=int, default=300, help='按共现度排序初筛的内生 UID 上限（默认 300）')
    screen.add_argument('--exogenous-per-area', type=int, default=15, help='每个主力大区补充抓取的外生在播样本数（默认 15）')
    screen.add_argument(
        '--parent-areas',
        default=','.join(DEFAULT_PARENT_AREAS),
        help='外生补齐覆盖的一级大区列表（逗号分隔）',
    )
    screen.add_argument('--active-days', type=int, default=14, help='近期活跃开播核验窗口天数（默认 14）')
    screen.add_argument('--guest-interval', type=float, default=0.2, help='游客接口请求间隔秒数（默认 0.2s）')

    plan = sub.add_parser('sample-plan', help='纯本地确定性计算内外生与 (大区, 舰长档位) 均衡分层抽样方案')
    plan.add_argument('--db', '--discovery-db', dest='db', type=Path, required=True, help='只读读取的发现工作库路径')
    plan.add_argument('--warehouse-db', type=Path, help='可选：只读读取数仓自动排除已知种子房与主播 UID')
    plan.add_argument('--exclude-rooms', action='append', default=[], help='排除的种子房号（逗号分隔或多次传入）')
    plan.add_argument('--exclude-uids', action='append', default=[], help='排除的种子主播 UID（逗号分隔或多次传入）')
    plan.add_argument(
        '--exclude-automation-config',
        action='append',
        type=Path,
        default=[],
        help='只读读取 automation.json 自动排除已配置的种子房间',
    )
    plan.add_argument('--target-count', type=int, default=60, help='目标抽样房间总数（默认 60）')
    plan.add_argument('--endogenous-ratio', type=float, default=0.7, help='内生共现候选目标占比（默认 0.7）')
    plan.add_argument('--max-per-subarea', type=int, default=8, help='单一二级子分区最大入选数（默认 8，0 为不限）')
    plan.add_argument('--seed', type=int, default=20261117, help='确定性抽样种子')
    plan.add_argument('--output', type=Path, help='可选：将抽样计划 JSON 原子写入文件供人工审阅')


def main(args: argparse.Namespace) -> int:
    if args.discovery_cmd == 'pull-followings':
        out = pull_followings(
            warehouse_db=args.warehouse_db,
            discovery_db=args.db,
            per_tier=args.per_tier,
            seed=args.seed,
            exclude_uids=args.exclude_uids,
            refresh=bool(args.refresh),
            execute=bool(args.execute),
            cdp_url=args.cdp_url,
            cookie_file=args.cookie_file,
            auth_interval=args.auth_interval,
        )
    elif args.discovery_cmd == 'screen-candidates':
        areas = [x.strip() for x in str(args.parent_areas or '').split(',') if x.strip()]
        out = screen_candidates(
            discovery_db=args.db,
            warehouse_db=args.warehouse_db,
            exclude_rooms=args.exclude_rooms,
            exclude_uids=args.exclude_uids,
            automation_configs=args.exclude_automation_config,
            max_endogenous=args.max_endogenous,
            exogenous_per_area=args.exogenous_per_area,
            parent_areas=areas,
            active_days=args.active_days,
            guest_interval=args.guest_interval,
        )
    elif args.discovery_cmd == 'sample-plan':
        out = sample_plan(
            discovery_db=args.db,
            warehouse_db=args.warehouse_db,
            exclude_rooms=args.exclude_rooms,
            exclude_uids=args.exclude_uids,
            automation_configs=args.exclude_automation_config,
            target_count=args.target_count,
            endogenous_ratio=args.endogenous_ratio,
            max_per_subarea=args.max_per_subarea,
            seed=args.seed,
            output_path=args.output,
        )
    else:
        raise ValueError(f'未知子命令：{args.discovery_cmd}')

    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0
