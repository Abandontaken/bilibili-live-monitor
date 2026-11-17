"""跨房全局身份消解与付费动机分析数仓（warehouse sync / backfill-pages / stats）。"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .annotation_history import read_annotations
from .live_share import fingerprint
from .live_share_control import load_config, read_session_source, scope_source
from .song_sources import verified_titles
from .time_utils import epoch, iso
from .transcript_corrections import correction_context
from .warehouse_protocol import (
    clean_uname,
    extract_guard_snapshot_identities,
    extract_raw_identities,
    extract_session_events,
    extract_timeline_identity,
    first_char,
    is_masked_name,
    pos_int,
    uid_crc32,
    uid_md5,
)

EXTRACTOR_VERSION = 1

WAREHOUSE_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta_source_cursors (
    source_name            TEXT PRIMARY KEY,
    db_path                TEXT NOT NULL,
    last_raw_event_id      INTEGER NOT NULL DEFAULT 0,
    last_guard_snapshot_id INTEGER NOT NULL DEFAULT 0,
    last_timeline_id       INTEGER NOT NULL DEFAULT 0,
    updated_at_utc         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_rooms (
    room_id           TEXT PRIMARY KEY,
    anchor_uid        INTEGER,
    room_label        TEXT,
    track_category    TEXT,
    allows_ext_medal  INTEGER NOT NULL DEFAULT 0,
    first_seen_ts     REAL NOT NULL,
    last_seen_ts      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_sessions (
    session_key       TEXT PRIMARY KEY,
    room_id           TEXT NOT NULL,
    anchor_uid        INTEGER,
    room_label        TEXT,
    track_category    TEXT,
    source_instance   TEXT NOT NULL,
    source_db_path    TEXT NOT NULL,
    workspace_path    TEXT,
    start_ts          REAL NOT NULL,
    cutoff_ts         REAL NOT NULL,
    transcript_basis  TEXT,
    extractor_version INTEGER NOT NULL,
    ingested_at_utc   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_room_time ON dim_sessions(room_id, start_ts);

CREATE TABLE IF NOT EXISTS dim_users (
    uid               INTEGER PRIMARY KEY,
    uid_md5           TEXT NOT NULL UNIQUE,
    uid_crc32         TEXT NOT NULL,
    latest_uname      TEXT,
    first_char        TEXT,
    latest_uname_ts   REAL,
    max_wealth_level  INTEGER NOT NULL DEFAULT 0,
    first_seen_ts     REAL NOT NULL,
    last_seen_ts      REAL NOT NULL,
    follow_status     TEXT DEFAULT 'pending',
    follow_synced_ts  REAL
);
CREATE INDEX IF NOT EXISTS idx_users_crc32 ON dim_users(uid_crc32);

CREATE TABLE IF NOT EXISTS dim_user_faces (
    face_sha1         TEXT NOT NULL,
    uid               INTEGER NOT NULL,
    first_seen_ts     REAL NOT NULL,
    last_seen_ts      REAL NOT NULL,
    PRIMARY KEY (face_sha1, uid)
);

CREATE VIEW IF NOT EXISTS v_unique_face_uid AS
SELECT face_sha1, MIN(uid) AS uid
FROM dim_user_faces
GROUP BY face_sha1
HAVING COUNT(DISTINCT uid) = 1;

CREATE TABLE IF NOT EXISTS fact_transcripts (
    session_key       TEXT NOT NULL REFERENCES dim_sessions(session_key) ON DELETE CASCADE,
    seq               INTEGER NOT NULL,
    start_ts          REAL NOT NULL,
    end_ts            REAL NOT NULL,
    activity_label    TEXT,
    music_source      TEXT,
    song_title        TEXT,
    text              TEXT NOT NULL,
    PRIMARY KEY (session_key, seq)
);
CREATE INDEX IF NOT EXISTS idx_transcripts_time ON fact_transcripts(session_key, start_ts, end_ts);

CREATE TABLE IF NOT EXISTS fact_events (
    session_key        TEXT NOT NULL REFERENCES dim_sessions(session_key) ON DELETE CASCADE,
    event_seq          INTEGER NOT NULL,
    room_id            TEXT NOT NULL,
    event_ts           REAL NOT NULL,
    time_source        TEXT NOT NULL,
    kind               TEXT NOT NULL,
    is_mystery         INTEGER NOT NULL DEFAULT 0,
    raw_uid            INTEGER,
    raw_uname          TEXT,
    clue_first_char    TEXT,
    clue_md5           TEXT,
    clue_face_sha1     TEXT,
    clue_crc32         TEXT,
    resolved_crc32_uid INTEGER,
    crc32_status       TEXT,
    inferred_uid       INTEGER,
    inference_method   TEXT,
    wealth_level       INTEGER,
    guard_level        INTEGER,
    medal_ruid         INTEGER,
    medal_room_id      TEXT,
    medal_name         TEXT,
    medal_level        INTEGER,
    gift_id            TEXT,
    gift_name          TEXT,
    blind_orig_name    TEXT,
    quantity           INTEGER,
    coin_type          TEXT,
    paid_milli_yuan    INTEGER DEFAULT 0,
    output_milli_yuan  INTEGER DEFAULT 0,
    content_text       TEXT,
    source_ref         TEXT,
    PRIMARY KEY (session_key, event_seq)
);
CREATE INDEX IF NOT EXISTS idx_events_room_time ON fact_events(room_id, event_ts);
CREATE INDEX IF NOT EXISTS idx_events_paid ON fact_events(paid_milli_yuan, event_ts) WHERE paid_milli_yuan > 0;
CREATE INDEX IF NOT EXISTS idx_events_md5 ON fact_events(clue_md5) WHERE clue_md5 IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_crc32 ON fact_events(room_id, clue_crc32) WHERE clue_crc32 IS NOT NULL;

CREATE VIEW IF NOT EXISTS v_resolved_events AS
SELECT
    e.*,
    CASE
        WHEN e.is_mystery = 1 THEN NULL
        ELSE COALESCE(e.raw_uid, u_md5.uid, u_face.uid, e.resolved_crc32_uid, e.inferred_uid)
    END AS resolved_uid,
    CASE
        WHEN e.is_mystery = 1 THEN 'mystery:' || e.session_key || ':' || e.event_seq
        WHEN COALESCE(e.raw_uid, u_md5.uid, u_face.uid, e.resolved_crc32_uid, e.inferred_uid) IS NOT NULL
            THEN 'uid:' || COALESCE(e.raw_uid, u_md5.uid, u_face.uid, e.resolved_crc32_uid, e.inferred_uid)
        WHEN e.clue_md5 IS NOT NULL THEN 'md5:' || e.clue_md5
        WHEN e.clue_crc32 IS NOT NULL
            THEN 'crc32:' || e.room_id || ':' || e.clue_crc32 || COALESCE(':' || e.clue_first_char, '')
        ELSE 'anon:' || e.session_key || ':' || e.event_seq
    END AS actor_key,
    CASE
        WHEN e.is_mystery = 1 THEN 'mystery'
        WHEN e.raw_uid IS NOT NULL THEN 'native_uid'
        WHEN u_md5.uid IS NOT NULL THEN 'global_md5'
        WHEN u_face.uid IS NOT NULL THEN 'unique_face'
        WHEN e.resolved_crc32_uid IS NOT NULL THEN 'crc32_first_char'
        WHEN e.inferred_uid IS NOT NULL THEN COALESCE(e.inference_method, 'inferred')
        WHEN e.crc32_status = 'ambiguous_crc32' THEN 'ambiguous_crc32'
        WHEN e.clue_md5 IS NOT NULL THEN 'unresolved_md5_pseudonym'
        ELSE 'unresolved'
    END AS resolution_basis
FROM fact_events e
LEFT JOIN dim_users u_md5 ON e.is_mystery = 0 AND e.raw_uid IS NULL AND e.clue_md5 = u_md5.uid_md5
LEFT JOIN v_unique_face_uid u_face ON e.is_mystery = 0 AND e.raw_uid IS NULL AND u_md5.uid IS NULL AND e.clue_face_sha1 = u_face.face_sha1;

CREATE TABLE IF NOT EXISTS fact_session_presence (
    session_key        TEXT NOT NULL REFERENCES dim_sessions(session_key) ON DELETE CASCADE,
    actor_key          TEXT NOT NULL,
    room_id            TEXT NOT NULL,
    resolved_uid       INTEGER,
    first_event_ts     REAL NOT NULL,
    last_event_ts      REAL NOT NULL,
    entry_count        INTEGER NOT NULL DEFAULT 0,
    danmaku_count      INTEGER NOT NULL DEFAULT 0,
    like_count         INTEGER NOT NULL DEFAULT 0,
    gift_event_count   INTEGER NOT NULL DEFAULT 0,
    paid_milli_yuan    INTEGER NOT NULL DEFAULT 0,
    output_milli_yuan  INTEGER NOT NULL DEFAULT 0,
    max_wealth_level   INTEGER NOT NULL DEFAULT 0,
    guard_level        INTEGER,
    medal_ruid         INTEGER,
    medal_room_id      TEXT,
    medal_name         TEXT,
    medal_level        INTEGER,
    PRIMARY KEY (session_key, actor_key)
);
CREATE INDEX IF NOT EXISTS idx_presence_uid ON fact_session_presence(resolved_uid) WHERE resolved_uid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_presence_room ON fact_session_presence(room_id, session_key);

CREATE TABLE IF NOT EXISTS ext_user_followings (
    uid               INTEGER NOT NULL REFERENCES dim_users(uid),
    target_mid        INTEGER NOT NULL,
    followed_mtime    INTEGER,
    PRIMARY KEY (uid, target_mid)
);

CREATE TABLE IF NOT EXISTS dim_up_profiles (
    mid               INTEGER PRIMARY KEY,
    uname             TEXT,
    official_verify   TEXT,
    top_partition     TEXT,
    updated_ts        REAL
);
"""

_IDENTITY_COMMANDS = (
    'ONLINE_RANK_V3',
    'ENTRY_EFFECT',
    'ENTRY_EFFECT_MUST_RECEIVE',
    'LIKE_INFO_V3_CLICK',
    'USER_TOAST_MSG',
    'USER_TOAST_MSG_V2',
    'SUPER_CHAT_MESSAGE',
    'PK_BATTLE_SETTLE_NEW',
    'GUARD_BUY',
    'SEND_GIFT',
    'SEND_GIFT_V2',
    'COMBO_SEND',
    'INTERACT_WORD',
    'INTERACT_WORD_V2',
)

_SOURCE_DB_MARKER_TABLES = frozenset({'raw_events', 'timeline', 'segments', 'guard_snapshots'})


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def open_readonly_sqlite(path: Path) -> sqlite3.Connection:
    """强制以 URI mode=ro + PRAGMA query_only=ON 只读打开数据库，绝不创建或写入。"""
    resolved = Path(path).resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f'只读数据库不存在：{resolved}')
    conn = sqlite3.connect(resolved.as_uri() + '?mode=ro', uri=True, timeout=30.0)
    conn.execute('PRAGMA query_only = ON')
    return conn


def assert_distinct_warehouse_path(warehouse_path: Path, source_paths: Iterable[Path]) -> None:
    """拒绝数仓路径与任何源数据库指向同一文件（含符号链接与硬链接别名）。"""
    wh_raw = Path(warehouse_path)
    wh_resolved = wh_raw.resolve()
    wh_exists = wh_raw.exists()
    for src in source_paths:
        src_raw = Path(src)
        src_resolved = src_raw.resolve()
        if wh_resolved == src_resolved:
            raise ValueError(f'数仓路径不得与源数据库相同：{src_raw}')
        if wh_exists and src_raw.exists() and os.path.samefile(wh_raw, src_raw):
            raise ValueError(f'数仓路径不得通过软硬链接指向源数据库：{src_raw}')


def collect_session_databases(session_roots: Iterable[Path]) -> list[Path]:
    """只读预扫场次根目录引用的全部源数据库路径，供打开数仓前做路径隔离校验。"""
    dbs: list[Path] = []
    seen: set[str] = set()
    for root in session_roots:
        root_path = Path(root)
        if not root_path.is_dir():
            raise FileNotFoundError(f'场次根目录不存在：{root_path}')
        for entry in sorted(root_path.iterdir()):
            if entry.is_symlink() or not entry.is_dir():
                continue
            cfg_path = entry / 'config.json'
            if not cfg_path.is_file():
                continue
            try:
                cfg = json.loads(cfg_path.read_text())
            except Exception:
                continue
            db_val = cfg.get('database')
            if isinstance(db_val, str) and db_val and db_val not in seen:
                seen.add(db_val)
                dbs.append(Path(db_val))
    return dbs


def open_warehouse(path: Path) -> sqlite3.Connection:
    resolved = Path(path).resolve()
    if Path(path).exists() and Path(path).stat().st_size > 0:
        with open_readonly_sqlite(path) as probe:
            tables = _source_tables(probe)
            if (tables & _SOURCE_DB_MARKER_TABLES) and 'dim_users' not in tables:
                raise ValueError(f'目标文件包含采集源数据库表结构，拒绝作为数仓可写打开：{path}')
    resolved.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(resolved, timeout=60.0)
    conn.executescript(WAREHOUSE_DDL)
    cols = {row[1] for row in conn.execute('PRAGMA table_info(dim_users)')}
    if 'latest_uname_ts' not in cols:
        conn.execute('ALTER TABLE dim_users ADD COLUMN latest_uname_ts REAL')
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


class _IdentityBatch:
    """在单批次内聚合 (uid, uname, face_sha1, wealth_level, ts)，独立追踪有效昵称观测时间戳。"""

    def __init__(self) -> None:
        self.users: dict[int, list[Any]] = {}
        self.faces: dict[tuple[str, int], list[float]] = {}
        self.room_anchors: dict[str, int] = {}
        self.room_times: dict[str, list[float]] = {}

    def add_room_touch(self, room_id: str | None, ts: float, anchor_uid: int | None = None) -> None:
        if not room_id:
            return
        rid = str(room_id)
        if anchor_uid and anchor_uid > 0:
            self.room_anchors[rid] = anchor_uid
        span = self.room_times.get(rid)
        if span is None:
            self.room_times[rid] = [ts, ts]
        else:
            if ts < span[0]:
                span[0] = ts
            if ts > span[1]:
                span[1] = ts

    def add(self, uid: int, uname: str | None, face_sha1: str | None, wealth_level: int, ts: float) -> None:
        if not uid or uid <= 0:
            return
        clean = clean_uname(uname) if uname and not is_masked_name(uname) else None
        fc = first_char(clean) if clean else None
        w = max(0, int(wealth_level or 0))
        cur = self.users.get(uid)
        if cur is None:
            # [latest_uname, first_char, latest_uname_ts, max_wealth, first_ts, last_ts]
            self.users[uid] = [clean, fc, ts if clean else None, w, ts, ts]
        else:
            if clean and (cur[2] is None or ts >= cur[2]):
                cur[0] = clean
                cur[1] = fc
                cur[2] = ts
            if w > cur[3]:
                cur[3] = w
            if ts < cur[4]:
                cur[4] = ts
            if ts > cur[5]:
                cur[5] = ts
        if face_sha1:
            fkey = (face_sha1, uid)
            fspan = self.faces.get(fkey)
            if fspan is None:
                self.faces[fkey] = [ts, ts]
            else:
                if ts < fspan[0]:
                    fspan[0] = ts
                if ts > fspan[1]:
                    fspan[1] = ts

    def flush(self, wh: sqlite3.Connection) -> tuple[int, int]:
        if self.users:
            wh.executemany(
                """
                INSERT INTO dim_users (
                    uid, uid_md5, uid_crc32, latest_uname, first_char, latest_uname_ts,
                    max_wealth_level, first_seen_ts, last_seen_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uid) DO UPDATE SET
                    latest_uname = CASE
                        WHEN excluded.latest_uname IS NOT NULL
                             AND (dim_users.latest_uname IS NULL
                                  OR dim_users.latest_uname_ts IS NULL
                                  OR excluded.latest_uname_ts >= dim_users.latest_uname_ts)
                        THEN excluded.latest_uname
                        ELSE dim_users.latest_uname
                    END,
                    first_char = CASE
                        WHEN excluded.first_char IS NOT NULL
                             AND (dim_users.first_char IS NULL
                                  OR dim_users.latest_uname_ts IS NULL
                                  OR excluded.latest_uname_ts >= dim_users.latest_uname_ts)
                        THEN excluded.first_char
                        ELSE dim_users.first_char
                    END,
                    latest_uname_ts = CASE
                        WHEN excluded.latest_uname IS NOT NULL
                             AND (dim_users.latest_uname_ts IS NULL
                                  OR excluded.latest_uname_ts >= dim_users.latest_uname_ts)
                        THEN excluded.latest_uname_ts
                        ELSE dim_users.latest_uname_ts
                    END,
                    max_wealth_level = MAX(dim_users.max_wealth_level, excluded.max_wealth_level),
                    first_seen_ts = MIN(dim_users.first_seen_ts, excluded.first_seen_ts),
                    last_seen_ts = MAX(dim_users.last_seen_ts, excluded.last_seen_ts)
                """,
                [
                    (uid, uid_md5(uid), uid_crc32(uid), rec[0], rec[1], rec[2], rec[3], rec[4], rec[5])
                    for uid, rec in self.users.items()
                ],
            )
        if self.faces:
            wh.executemany(
                """
                INSERT INTO dim_user_faces (face_sha1, uid, first_seen_ts, last_seen_ts)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(face_sha1, uid) DO UPDATE SET
                    first_seen_ts = MIN(dim_user_faces.first_seen_ts, excluded.first_seen_ts),
                    last_seen_ts = MAX(dim_user_faces.last_seen_ts, excluded.last_seen_ts)
                """,
                [(sha1, uid, span[0], span[1]) for (sha1, uid), span in self.faces.items()],
            )
        for rid, span in self.room_times.items():
            anchor = self.room_anchors.get(rid)
            wh.execute(
                """
                INSERT INTO dim_rooms (room_id, anchor_uid, first_seen_ts, last_seen_ts)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(room_id) DO UPDATE SET
                    anchor_uid = COALESCE(dim_rooms.anchor_uid, excluded.anchor_uid),
                    first_seen_ts = MIN(dim_rooms.first_seen_ts, excluded.first_seen_ts),
                    last_seen_ts = MAX(dim_rooms.last_seen_ts, excluded.last_seen_ts)
                """,
                (rid, anchor, span[0], span[1]),
            )
        return len(self.users), len(self.faces)


def _source_tables(src: sqlite3.Connection) -> set[str]:
    return {row[0] for row in src.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _scan_single_source(wh: sqlite3.Connection, source_name: str, db_path: Path) -> dict[str, Any]:
    resolved_db = db_path.resolve()
    cur_row = wh.execute(
        'SELECT db_path, last_raw_event_id, last_guard_snapshot_id, last_timeline_id '
        'FROM meta_source_cursors WHERE source_name = ?',
        (source_name,),
    ).fetchone()
    if cur_row is not None and Path(cur_row[0]).resolve() != resolved_db:
        raise ValueError(f'数据源 {source_name} 已绑定到其他数据库路径，禁止混用游标')
    last_raw_id, last_guard_id, last_tl_id = (cur_row[1], cur_row[2], cur_row[3]) if cur_row else (0, 0, 0)

    batch = _IdentityBatch()
    scanned_raw = 0
    scanned_guards = 0
    scanned_timeline = 0
    mode = 'raw_events'

    with open_readonly_sqlite(resolved_db) as src:
        src.execute('BEGIN')
        tables = _source_tables(src)

        if 'rooms' in tables:
            for (rdata_s,) in src.execute('SELECT data FROM rooms'):
                try:
                    rdata = json.loads(rdata_s)
                    rid = str(rdata.get('room_id') or '')
                    auid = pos_int(rdata.get('uid') or rdata.get('anchor_uid'))
                    ts = epoch(rdata['updated_at']) if rdata.get('updated_at') else time.time()
                    if rid:
                        batch.add_room_touch(rid, ts, auid)
                except Exception:
                    continue

        if 'raw_events' in tables:
            max_raw_id = src.execute('SELECT COALESCE(MAX(id), 0) FROM raw_events').fetchone()[0]
            if max_raw_id > last_raw_id:
                placeholders = ','.join('?' for _ in _IDENTITY_COMMANDS)
                sql = (
                    f'SELECT id, room_id, received_utc, command, data FROM raw_events '
                    f'WHERE id > ? AND id <= ? AND (command IN ({placeholders}) OR command LIKE \'DANMU_MSG%\') '
                    f'ORDER BY id'
                )
                cursor = src.execute(sql, (last_raw_id, max_raw_id, *_IDENTITY_COMMANDS))
                while rows := cursor.fetchmany(5000):
                    for row_id, room_id, rec_utc, command, data_s in rows:
                        scanned_raw += 1
                        cmd = str(command or '')
                        try:
                            ts = epoch(rec_utc)
                            packet = json.loads(data_s)
                        except Exception:
                            continue
                        if not isinstance(packet, dict):
                            continue
                        identities, anchor_uid = extract_raw_identities(cmd, packet)
                        if anchor_uid or identities:
                            batch.add_room_touch(room_id, ts, anchor_uid)
                        for uid, uname, face, wealth in identities:
                            batch.add(uid, uname, face, wealth, ts)
                last_raw_id = max_raw_id

            if 'guard_snapshots' in tables:
                max_guard_id = src.execute('SELECT COALESCE(MAX(id), 0) FROM guard_snapshots').fetchone()[0]
                if max_guard_id > last_guard_id:
                    gcur = src.execute(
                        'SELECT id, room_id, observed_utc, data FROM guard_snapshots '
                        'WHERE id > ? AND id <= ? ORDER BY id',
                        (last_guard_id, max_guard_id),
                    )
                    while grows := gcur.fetchmany(500):
                        for _, room_id, obs_utc, gdata_s in grows:
                            scanned_guards += 1
                            try:
                                ts = epoch(obs_utc)
                                snap = json.loads(gdata_s)
                            except Exception:
                                continue
                            identities, anchor_uid = extract_guard_snapshot_identities(snap)
                            batch.add_room_touch(room_id, ts, anchor_uid)
                            for uid, uname, face, wealth in identities:
                                batch.add(uid, uname, face, wealth, ts)
                    last_guard_id = max_guard_id
        elif 'timeline' in tables:
            mode = 'timeline_fallback'
            max_tl_id = src.execute('SELECT COALESCE(MAX(id), 0) FROM timeline').fetchone()[0]
            if max_tl_id > last_tl_id:
                tcur = src.execute(
                    "SELECT id, room_id, at_utc, data FROM timeline "
                    "WHERE id > ? AND id <= ? AND kind != 'transcript' "
                    "AND (json_extract(data, '$.sender_uid') IS NOT NULL "
                    "     OR json_extract(data, '$.gift.sender_uid') IS NOT NULL) "
                    "ORDER BY id",
                    (last_tl_id, max_tl_id),
                )
                while trows := tcur.fetchmany(5000):
                    for _, room_id, at_utc, tdata_s in trows:
                        scanned_timeline += 1
                        try:
                            ts = epoch(at_utc)
                            tdata = json.loads(tdata_s)
                        except Exception:
                            continue
                        extracted = extract_timeline_identity(tdata)
                        if extracted is not None:
                            uid, uname = extracted
                            batch.add_room_touch(room_id, ts)
                            batch.add(uid, uname, None, 0, ts)
                last_tl_id = max_tl_id

    with wh:
        touched_users, touched_faces = batch.flush(wh)
        wh.execute(
            """
            INSERT INTO meta_source_cursors (
                source_name, db_path, last_raw_event_id, last_guard_snapshot_id, last_timeline_id, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_name) DO UPDATE SET
                db_path = excluded.db_path,
                last_raw_event_id = excluded.last_raw_event_id,
                last_guard_snapshot_id = excluded.last_guard_snapshot_id,
                last_timeline_id = excluded.last_timeline_id,
                updated_at_utc = excluded.updated_at_utc
            """,
            (source_name, str(resolved_db), last_raw_id, last_guard_id, last_tl_id, utc_now_iso()),
        )

    return {
        'source_name': source_name,
        'mode': mode,
        'scanned_raw_events': scanned_raw,
        'scanned_guard_snapshots': scanned_guards,
        'scanned_timeline_rows': scanned_timeline,
        'touched_users': touched_users,
        'touched_faces': touched_faces,
        'last_raw_event_id': last_raw_id,
        'last_guard_snapshot_id': last_guard_id,
        'last_timeline_id': last_tl_id,
    }


def _compute_session_key(config: dict[str, Any]) -> str:
    wf_key = config.get('workflow_key')
    if isinstance(wf_key, str) and wf_key.strip():
        return wf_key.strip()
    raw = json.dumps(
        [str(Path(config['database']).resolve()), str(config['room_id']), str(config['start_utc'])],
        ensure_ascii=False,
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def _transcript_basis(config: dict[str, Any], has_verified_reviews: bool) -> str:
    mode = config.get('content_mode', 'music')
    workspace = Path(config['workspace'])
    if mode == 'talk':
        if config.get('talk_finalize'):
            from . import talk_finalize
            if talk_finalize.complete(config):
                return 'finalized_talk'
        return 'offline_asr' if config.get('offline_segments') else 'realtime_asr'
    if config.get('offline_segments'):
        if (workspace / 'finalize/completed.json').is_file() or has_verified_reviews:
            return 'finalized_music'
        return 'offline_asr'
    return 'realtime_asr'


def _ingest_completed_session(
    wh: sqlite3.Connection,
    warehouse_path: Path,
    session_dir: Path,
    source_by_db: dict[Path, str],
    force_reingest: bool,
) -> dict[str, Any]:
    wf_path = session_dir / 'workflow.json'
    cfg_path = session_dir / 'config.json'
    state_path = session_dir / 'state.json'
    if not (wf_path.is_file() and cfg_path.is_file() and state_path.is_file()):
        return {'status': 'skipped_incomplete'}

    workflow = json.loads(wf_path.read_text())
    if workflow.get('stage') != 'completed':
        return {'status': 'skipped_not_completed'}

    config = load_config(cfg_path)
    db_path = Path(config['database'])
    assert_distinct_warehouse_path(warehouse_path, [db_path])
    resolved_db = db_path.resolve()

    skey = _compute_session_key(config)
    existing = wh.execute(
        'SELECT extractor_version FROM dim_sessions WHERE session_key = ?', (skey,)
    ).fetchone()
    if existing is not None and existing[0] == EXTRACTOR_VERSION and not force_reingest:
        return {'status': 'skipped_already_ingested', 'session_key': skey}

    state_data = json.loads(state_path.read_text())
    start_ts = epoch(config['start_utc'])
    cutoff_ts = min(float(state_data.get('cutoff', state_data.get('last_live_at', start_ts))), float(config['deadline']))
    if cutoff_ts < start_ts:
        raise ValueError(f'场次 {skey} 的截止时间早于开播时间')

    source_instance = source_by_db.get(resolved_db) or ('anonymous' if config.get('local_only') else 'account')
    room_id = str(config['room_id'])
    platform_room_id = str(config.get('platform_room_id') or room_id)

    # 1. 提取终审语音与歌曲区间 (fact_transcripts)，兼容 talk_finalize 精修文本与无 raw_events 早期库
    session_source = scope_source(read_session_source(config), config['start_utc'], cutoff_ts)
    workspace = Path(config['workspace'])
    corrections, _ = correction_context(workspace, session_source)
    annotations = read_annotations(workspace)
    known_songs = set(config.get('approved_songs', [])) | set(verified_titles(workspace))
    reviews_by_seq: dict[int, dict[str, Any]] = {}
    for rev in annotations.get('reviews', []):
        if isinstance(rev, dict) and type(rev.get('seq')) is int:
            reviews_by_seq[rev['seq']] = rev

    transcript_rows: list[tuple[Any, ...]] = []
    has_verified_reviews = False
    for seg in session_source['segments']:
        if not (seg.get('is_final') and seg.get('text')):
            continue
        s_ts, e_ts = epoch(seg['start_utc']), epoch(seg['end_utc'])
        if not (start_ts <= s_ts <= e_ts <= cutoff_ts):
            continue
        seq = int(seg['seq'])
        corr = corrections.get(seq, {})
        seg_corr = seg.get('corrected_text')
        final_text = (
            corr.get('corrected_text')
            or (seg_corr if isinstance(seg_corr, str) and seg_corr.strip() else None)
            or seg['text']
        )
        rev = reviews_by_seq.get(seq)
        label_val = None
        msrc_val = None
        song_val = None
        if rev and rev.get('fingerprint') == fingerprint(seg):
            has_verified_reviews = True
            if rev.get('label') in ('singing', 'speaking', 'mixed', 'unknown'):
                label_val = rev['label']
            if rev.get('music_source') in ('probable_live', 'recording', 'uncertain', 'none'):
                msrc_val = rev['music_source']
            if isinstance(rev.get('song'), str) and rev['song'] in known_songs:
                song_val = rev['song']
        transcript_rows.append((skey, seq, s_ts, e_ts, label_val, msrc_val, song_val, final_text))

    # 2. 只读读取源库本场窗口事件，保留原始协议线索
    raw_rows: list[tuple[int, str, str, dict[str, Any]]] = []
    tl_rows: list[tuple[int, str, str, dict[str, Any]]] = []
    batch = _IdentityBatch()
    with open_readonly_sqlite(resolved_db) as src:
        src.execute('BEGIN')
        tables = _source_tables(src)
        lo_iso = iso(start_ts - 60.0)
        hi_iso = iso(cutoff_ts + 60.0)
        if 'raw_events' in tables:
            for rid_id, rec_utc, cmd, data_s in src.execute(
                'SELECT id, received_utc, command, data FROM raw_events '
                'WHERE room_id = ? AND received_utc >= ? AND received_utc <= ? ORDER BY id',
                (room_id, lo_iso, hi_iso),
            ):
                try:
                    pkt = json.loads(data_s)
                except Exception:
                    continue
                if isinstance(pkt, dict):
                    raw_rows.append((rid_id, rec_utc, str(cmd or ''), pkt))
                    identities, anchor_uid = extract_raw_identities(str(cmd or ''), pkt)
                    rec_ts = epoch(rec_utc)
                    if anchor_uid:
                        batch.add_room_touch(room_id, rec_ts, anchor_uid)
                    for uid, uname, face, wealth in identities:
                        batch.add(uid, uname, face, wealth, rec_ts)
        if not raw_rows and 'timeline' in tables:
            for tl_id, kind, at_utc, data_s in src.execute(
                'SELECT id, kind, at_utc, data FROM timeline '
                'WHERE room_id = ? AND at_utc >= ? AND at_utc <= ? ORDER BY id',
                (room_id, lo_iso, hi_iso),
            ):
                try:
                    tdata = json.loads(data_s)
                except Exception:
                    continue
                if isinstance(tdata, dict):
                    tl_rows.append((tl_id, str(kind), at_utc, tdata))
                    extracted = extract_timeline_identity(tdata)
                    if extracted is not None:
                        batch.add(extracted[0], extracted[1], None, 0, epoch(at_utc))

    norm_events = extract_session_events(room_id, start_ts, cutoff_ts, raw_rows, tl_rows)

    room_row = wh.execute('SELECT anchor_uid FROM dim_rooms WHERE room_id = ?', (room_id,)).fetchone()
    known_anchor = batch.room_anchors.get(room_id) or (room_row[0] if room_row and room_row[0] else None)
    observed_medal_ruids = {ev['medal_ruid'] for ev in norm_events if ev.get('medal_ruid')}
    allows_ext = 0
    if known_anchor and any(ruid != known_anchor for ruid in observed_medal_ruids):
        allows_ext = 1
    elif len(observed_medal_ruids) >= 2:
        allows_ext = 1
    elif any(
        ev.get('medal_room_id') and ev['medal_room_id'] not in (room_id, platform_room_id, '0')
        for ev in norm_events
    ):
        allows_ext = 1

    track_cat = 'virtual_music' if config.get('content_mode', 'music') == 'music' else 'virtual_talk'
    basis = _transcript_basis(config, has_verified_reviews=has_verified_reviews)
    now_str = utc_now_iso()

    with wh:
        batch.flush(wh)
        wh.execute(
            """
            INSERT INTO dim_rooms (room_id, anchor_uid, room_label, track_category, allows_ext_medal, first_seen_ts, last_seen_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(room_id) DO UPDATE SET
                anchor_uid = COALESCE(dim_rooms.anchor_uid, excluded.anchor_uid),
                room_label = COALESCE(excluded.room_label, dim_rooms.room_label),
                track_category = COALESCE(dim_rooms.track_category, excluded.track_category),
                allows_ext_medal = MAX(dim_rooms.allows_ext_medal, excluded.allows_ext_medal),
                first_seen_ts = MIN(dim_rooms.first_seen_ts, excluded.first_seen_ts),
                last_seen_ts = MAX(dim_rooms.last_seen_ts, excluded.last_seen_ts)
            """,
            (room_id, known_anchor, config.get('label'), track_cat, allows_ext, start_ts, cutoff_ts),
        )
        wh.execute('DELETE FROM fact_session_presence WHERE session_key = ?', (skey,))
        wh.execute('DELETE FROM fact_events WHERE session_key = ?', (skey,))
        wh.execute('DELETE FROM fact_transcripts WHERE session_key = ?', (skey,))
        wh.execute('DELETE FROM dim_sessions WHERE session_key = ?', (skey,))
        wh.execute(
            """
            INSERT INTO dim_sessions (
                session_key, room_id, anchor_uid, room_label, track_category, source_instance,
                source_db_path, workspace_path, start_ts, cutoff_ts, transcript_basis,
                extractor_version, ingested_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                skey, room_id, known_anchor, config.get('label'), track_cat, source_instance,
                str(resolved_db), str(workspace.resolve()), start_ts, cutoff_ts,
                basis, EXTRACTOR_VERSION, now_str,
            ),
        )
        if transcript_rows:
            wh.executemany(
                """
                INSERT INTO fact_transcripts (
                    session_key, seq, start_ts, end_ts, activity_label, music_source, song_title, text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                transcript_rows,
            )
        if norm_events:
            wh.executemany(
                """
                INSERT INTO fact_events (
                    session_key, event_seq, room_id, event_ts, time_source, kind,
                    is_mystery, raw_uid, raw_uname, clue_first_char, clue_md5, clue_face_sha1, clue_crc32,
                    inferred_uid, inference_method, wealth_level, guard_level,
                    medal_ruid, medal_room_id, medal_name, medal_level,
                    gift_id, gift_name, blind_orig_name, quantity, coin_type,
                    paid_milli_yuan, output_milli_yuan, content_text, source_ref
                ) VALUES (
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                """,
                [
                    (
                        skey, idx + 1, ev['room_id'], ev['event_ts'], ev['time_source'], ev['kind'],
                        ev['is_mystery'], ev['raw_uid'], ev['raw_uname'], ev['clue_first_char'],
                        ev['clue_md5'], ev['clue_face_sha1'], ev['clue_crc32'],
                        ev['inferred_uid'], ev['inference_method'], ev['wealth_level'], ev['guard_level'],
                        ev['medal_ruid'], ev['medal_room_id'], ev['medal_name'], ev['medal_level'],
                        ev['gift_id'], ev['gift_name'], ev['blind_orig_name'], ev['quantity'], ev['coin_type'],
                        ev['paid_milli_yuan'], ev['output_milli_yuan'], ev['content_text'], ev['source_ref'],
                    )
                    for idx, ev in enumerate(norm_events)
                ],
            )

    return {
        'status': 'ingested',
        'session_key': skey,
        'transcripts': len(transcript_rows),
        'events': len(norm_events),
    }


def _resolve_danmaku_crc32_and_presence(wh: sqlite3.Connection) -> dict[str, int]:
    """双向消解匿名弹幕 crc32+首字（含同场/同房共现消歧与碰撞保护），刷新 presence 并清除 dirty 标记。"""
    crc_candidates: dict[tuple[str, str], list[int]] = {}
    for uid, crc, fchar in wh.execute(
        'SELECT uid, uid_crc32, first_char FROM dim_users WHERE first_char IS NOT NULL'
    ):
        crc_candidates.setdefault((crc, fchar), []).append(int(uid))

    session_known: dict[str, set[int]] = {}
    room_known: dict[str, set[int]] = {}
    for skey, rid, ruid in wh.execute(
        """
        SELECT
            e.session_key,
            e.room_id,
            COALESCE(e.raw_uid, u_md5.uid, u_face.uid, e.inferred_uid) AS firm_uid
        FROM fact_events e
        LEFT JOIN dim_users u_md5 ON e.is_mystery = 0 AND e.raw_uid IS NULL AND e.clue_md5 = u_md5.uid_md5
        LEFT JOIN v_unique_face_uid u_face
            ON e.is_mystery = 0 AND e.raw_uid IS NULL AND u_md5.uid IS NULL AND e.clue_face_sha1 = u_face.face_sha1
        WHERE e.is_mystery = 0
          AND COALESCE(e.raw_uid, u_md5.uid, u_face.uid, e.inferred_uid) IS NOT NULL
        GROUP BY e.session_key, e.room_id, firm_uid
        """
    ):
        u_int = int(ruid)
        session_known.setdefault(skey, set()).add(u_int)
        room_known.setdefault(rid, set()).add(u_int)

    updates: list[tuple[int | None, str | None, str, str, str | None]] = []
    groups = wh.execute(
        """
        SELECT session_key, room_id, clue_crc32, clue_first_char,
               MAX(COALESCE(resolved_crc32_uid, -1)) AS cur_uid,
               MAX(COALESCE(crc32_status, '')) AS cur_status
        FROM fact_events
        WHERE kind = 'danmaku' AND is_mystery = 0 AND raw_uid IS NULL AND clue_crc32 IS NOT NULL
        GROUP BY session_key, room_id, clue_crc32, clue_first_char
        """
    ).fetchall()

    resolved_groups = 0
    ambiguous_groups = 0
    for skey, rid, crc, fchar, cur_uid, cur_status in groups:
        cands = crc_candidates.get((crc, fchar), []) if fchar else []
        new_uid: int | None = None
        new_status: str | None = None
        if len(cands) == 1:
            new_uid = cands[0]
            new_status = 'resolved'
            resolved_groups += 1
        elif len(cands) > 1:
            in_sess = [u for u in cands if u in session_known.get(skey, ())]
            if len(in_sess) == 1:
                new_uid = in_sess[0]
                new_status = 'resolved'
                resolved_groups += 1
            elif len(in_sess) > 1:
                new_uid = None
                new_status = 'ambiguous_crc32'
                ambiguous_groups += 1
            else:
                in_room = [u for u in cands if u in room_known.get(rid, ())]
                if len(in_room) == 1:
                    new_uid = in_room[0]
                    new_status = 'resolved'
                    resolved_groups += 1
                else:
                    new_uid = None
                    new_status = 'ambiguous_crc32'
                    ambiguous_groups += 1

        if (new_uid if new_uid is not None else -1) != cur_uid or (new_status or '') != cur_status:
            updates.append((new_uid, new_status, skey, crc, fchar))

    with wh:
        if updates:
            wh.executemany(
                """
                UPDATE fact_events
                SET resolved_crc32_uid = ?, crc32_status = ?
                WHERE session_key = ? AND kind = 'danmaku' AND is_mystery = 0
                  AND raw_uid IS NULL AND clue_crc32 = ? AND clue_first_char IS ?
                """,
                updates,
            )

        wh.execute('DELETE FROM fact_session_presence')
        wh.execute(
            """
            INSERT INTO fact_session_presence (
                session_key, actor_key, room_id, resolved_uid, first_event_ts, last_event_ts,
                entry_count, danmaku_count, like_count, gift_event_count,
                paid_milli_yuan, output_milli_yuan, max_wealth_level, guard_level,
                medal_ruid, medal_room_id, medal_name, medal_level
            )
            WITH resolved AS (
                SELECT * FROM v_resolved_events
            ),
            latest_medal AS (
                SELECT session_key, actor_key, medal_ruid, medal_room_id, medal_name, medal_level
                FROM (
                    SELECT session_key, actor_key, medal_ruid, medal_room_id, medal_name, medal_level,
                           ROW_NUMBER() OVER (
                               PARTITION BY session_key, actor_key
                               ORDER BY event_ts DESC, event_seq DESC
                           ) AS rn
                    FROM resolved
                    WHERE medal_level IS NOT NULL OR medal_name IS NOT NULL OR medal_ruid IS NOT NULL
                )
                WHERE rn = 1
            )
            SELECT
                r.session_key,
                r.actor_key,
                MIN(r.room_id) AS room_id,
                MAX(r.resolved_uid) AS resolved_uid,
                MIN(r.event_ts) AS first_event_ts,
                MAX(r.event_ts) AS last_event_ts,
                SUM(CASE WHEN r.kind = 'entry' THEN 1 ELSE 0 END) AS entry_count,
                SUM(CASE WHEN r.kind = 'danmaku' THEN 1 ELSE 0 END) AS danmaku_count,
                SUM(CASE WHEN r.kind = 'like' THEN 1 ELSE 0 END) AS like_count,
                SUM(CASE WHEN r.kind IN ('gift', 'guard', 'sc') THEN 1 ELSE 0 END) AS gift_event_count,
                SUM(COALESCE(r.paid_milli_yuan, 0)) AS paid_milli_yuan,
                SUM(COALESCE(r.output_milli_yuan, 0)) AS output_milli_yuan,
                COALESCE(MAX(r.wealth_level), 0) AS max_wealth_level,
                MIN(CASE WHEN r.guard_level IN (1, 2, 3) THEN r.guard_level END) AS guard_level,
                m.medal_ruid,
                m.medal_room_id,
                m.medal_name,
                m.medal_level
            FROM resolved r
            LEFT JOIN latest_medal m
                ON r.session_key = m.session_key AND r.actor_key = m.actor_key
            GROUP BY r.session_key, r.actor_key
            """
        )

    presence_rows = wh.execute('SELECT COUNT(*) FROM fact_session_presence').fetchone()[0]
    return {
        'updated_danmaku_groups': len(updates),
        'resolved_danmaku_groups': resolved_groups,
        'ambiguous_danmaku_groups': ambiguous_groups,
        'presence_rows': presence_rows,
    }


def sync_warehouse(
    warehouse_path: Path,
    sources: list[tuple[str, Path]],
    session_roots: list[Path] | None = None,
    reingest: set[str] | None = None,
) -> dict[str, Any]:
    """执行增量源库扫描、已完成场次标准化入库与跨房身份双向消解。"""
    started = time.monotonic()
    reingest_set = set(reingest or ())
    reingest_all = '*' in reingest_set

    # 打开数仓前先校验全部显式源库及场次引用的源库，严防误写任何源 monitor.sqlite3
    session_dbs = collect_session_databases(session_roots or [])
    assert_distinct_warehouse_path(warehouse_path, [*(p for _, p in sources), *session_dbs])

    source_by_db = {db_path.resolve(): name for name, db_path in sources}

    with open_warehouse(warehouse_path) as wh:
        existing_cursor_dbs = [
            Path(row[0]) for row in wh.execute('SELECT db_path FROM meta_source_cursors') if row[0]
        ]
        assert_distinct_warehouse_path(warehouse_path, existing_cursor_dbs)

        source_reports = []
        for name, db_path in sources:
            source_reports.append(_scan_single_source(wh, name, db_path))

        session_stats = {
            'ingested': 0,
            'skipped_already_ingested': 0,
            'skipped_not_completed': 0,
            'skipped_incomplete': 0,
            'ingested_transcripts': 0,
            'ingested_events': 0,
        }
        for root in session_roots or []:
            root_path = Path(root)
            for entry in sorted(root_path.iterdir()):
                if entry.is_symlink() or not entry.is_dir():
                    continue
                force = reingest_all or (entry.name in reingest_set)
                if not force and (entry / 'config.json').is_file():
                    try:
                        cfg_raw = json.loads((entry / 'config.json').read_text())
                        if isinstance(cfg_raw.get('workflow_key'), str) and cfg_raw['workflow_key'] in reingest_set:
                            force = True
                    except Exception:
                        pass
                res = _ingest_completed_session(
                    wh, warehouse_path, entry, source_by_db, force_reingest=force
                )
                status = res['status']
                session_stats[status] = session_stats.get(status, 0) + 1
                if status == 'ingested':
                    session_stats['ingested_transcripts'] += res['transcripts']
                    session_stats['ingested_events'] += res['events']

        any_growth = any(r['touched_users'] > 0 or r['touched_faces'] > 0 for r in source_reports)
        presence_stale = wh.execute(
            'SELECT EXISTS('
            '  SELECT 1 FROM v_resolved_events r '
            '  LEFT JOIN fact_session_presence p '
            '    ON r.session_key = p.session_key AND r.actor_key = p.actor_key '
            '  WHERE p.actor_key IS NULL LIMIT 1'
            ')'
        ).fetchone()[0]
        unindexed_danmaku = wh.execute(
            'SELECT EXISTS('
            '  SELECT 1 FROM fact_events e '
            '  JOIN dim_users u ON u.uid_crc32 = e.clue_crc32 AND u.first_char = e.clue_first_char '
            '  WHERE e.kind = \'danmaku\' AND e.is_mystery = 0 AND e.raw_uid IS NULL '
            '    AND e.resolved_crc32_uid IS NULL AND e.crc32_status IS NULL LIMIT 1'
            ')'
        ).fetchone()[0]
        if any_growth or session_stats['ingested'] > 0 or bool(reingest_set) or presence_stale or unindexed_danmaku:
            resolution = _resolve_danmaku_crc32_and_presence(wh)
        else:
            presence_count = wh.execute('SELECT COUNT(*) FROM fact_session_presence').fetchone()[0]
            resolution = {
                'updated_danmaku_groups': 0,
                'resolved_danmaku_groups': 0,
                'ambiguous_danmaku_groups': 0,
                'presence_rows': presence_count,
            }

        known_uids = wh.execute('SELECT COUNT(*) FROM dim_users').fetchone()[0]
        unique_faces = wh.execute('SELECT COUNT(*) FROM v_unique_face_uid').fetchone()[0]
        total_sessions = wh.execute('SELECT COUNT(*) FROM dim_sessions').fetchone()[0]

    return {
        'warehouse': str(Path(warehouse_path).resolve()),
        'elapsed_seconds': round(time.monotonic() - started, 3),
        'known_uids': known_uids,
        'unique_faces': unique_faces,
        'total_sessions': total_sessions,
        'sources': source_reports,
        'sessions': session_stats,
        'resolution': resolution,
    }


def _rate(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 6)


def warehouse_stats(warehouse_path: Path) -> dict[str, Any]:
    """只读输出全局身份字典规模、付费礼物金额分档还原率与弹幕荣耀等级分档消解率。"""
    with open_readonly_sqlite(warehouse_path) as wh:
        known_uids = wh.execute('SELECT COUNT(*) FROM dim_users').fetchone()[0]
        unique_faces = wh.execute('SELECT COUNT(*) FROM v_unique_face_uid').fetchone()[0]
        shared_faces = wh.execute(
            'SELECT COUNT(*) FROM (SELECT face_sha1 FROM dim_user_faces GROUP BY face_sha1 HAVING COUNT(DISTINCT uid) > 1)'
        ).fetchone()[0]
        ingested_sessions = wh.execute('SELECT COUNT(*) FROM dim_sessions').fetchone()[0]
        known_rooms = wh.execute('SELECT COUNT(*) FROM dim_rooms').fetchone()[0]

        def gift_tier_summary(extra_where: str = '') -> dict[str, Any]:
            tiers = ('<10元', '10-200元', '>=200元')
            data = {
                t: {'events': 0, 'resolved_events': 0, 'paid_milli': 0, 'resolved_paid_milli': 0}
                for t in (*tiers, 'total')
            }
            rows = wh.execute(
                f"""
                SELECT
                    CASE
                        WHEN paid_milli_yuan < 10000 THEN '<10元'
                        WHEN paid_milli_yuan < 200000 THEN '10-200元'
                        ELSE '>=200元'
                    END AS tier,
                    COUNT(*) AS cnt,
                    SUM(CASE WHEN resolved_uid IS NOT NULL THEN 1 ELSE 0 END) AS res_cnt,
                    SUM(paid_milli_yuan) AS paid_sum,
                    SUM(CASE WHEN resolved_uid IS NOT NULL THEN paid_milli_yuan ELSE 0 END) AS res_paid_sum
                FROM v_resolved_events
                WHERE kind IN ('gift', 'guard', 'sc') AND paid_milli_yuan > 0 {extra_where}
                GROUP BY tier
                """
            ).fetchall()
            for tier, cnt, res_cnt, paid_sum, res_paid_sum in rows:
                for target in (tier, 'total'):
                    data[target]['events'] += cnt or 0
                    data[target]['resolved_events'] += res_cnt or 0
                    data[target]['paid_milli'] += paid_sum or 0
                    data[target]['resolved_paid_milli'] += res_paid_sum or 0
            out: dict[str, Any] = {}
            for key in (*tiers, 'total'):
                d = data[key]
                out[key] = {
                    'events': d['events'],
                    'resolved_events': d['resolved_events'],
                    'event_resolution_rate': _rate(d['resolved_events'], d['events']),
                    'paid_yuan': round(d['paid_milli'] / 1000.0, 3),
                    'resolved_paid_yuan': round(d['resolved_paid_milli'] / 1000.0, 3),
                    'paid_resolution_rate': _rate(d['resolved_paid_milli'], d['paid_milli']),
                }
            return out

        def danmaku_tier_summary(extra_where: str = '') -> dict[str, Any]:
            tiers = ('0', '1-9', '10-24', '25-39', '40+')
            data = {
                t: {'events': 0, 'resolved_events': 0, 'ambiguous_events': 0}
                for t in (*tiers, 'total')
            }
            rows = wh.execute(
                f"""
                SELECT
                    CASE
                        WHEN COALESCE(wealth_level, 0) <= 0 THEN '0'
                        WHEN wealth_level <= 9 THEN '1-9'
                        WHEN wealth_level <= 24 THEN '10-24'
                        WHEN wealth_level <= 39 THEN '25-39'
                        ELSE '40+'
                    END AS tier,
                    COUNT(*) AS cnt,
                    SUM(CASE WHEN resolved_uid IS NOT NULL THEN 1 ELSE 0 END) AS res_cnt,
                    SUM(CASE WHEN crc32_status = 'ambiguous_crc32' THEN 1 ELSE 0 END) AS amb_cnt
                FROM v_resolved_events
                WHERE kind = 'danmaku' {extra_where}
                GROUP BY tier
                """
            ).fetchall()
            for tier, cnt, res_cnt, amb_cnt in rows:
                for target in (tier, 'total'):
                    data[target]['events'] += cnt or 0
                    data[target]['resolved_events'] += res_cnt or 0
                    data[target]['ambiguous_events'] += amb_cnt or 0
            out: dict[str, Any] = {}
            for key in (*tiers, 'total'):
                d = data[key]
                out[key] = {
                    'events': d['events'],
                    'resolved_events': d['resolved_events'],
                    'ambiguous_events': d['ambiguous_events'],
                    'resolution_rate': _rate(d['resolved_events'], d['events']),
                }
            return out

        basis_breakdown = dict(
            wh.execute(
                """
                SELECT resolution_basis, COUNT(*)
                FROM v_resolved_events
                WHERE kind IN ('gift', 'guard', 'sc') AND paid_milli_yuan > 0
                GROUP BY resolution_basis
                """
            ).fetchall()
        )

        anonymous_sessions_filter = (
            "AND session_key IN (SELECT session_key FROM dim_sessions WHERE source_instance = 'anonymous')"
        )
        return {
            'known_uids': known_uids,
            'unique_faces': unique_faces,
            'shared_faces': shared_faces,
            'known_rooms': known_rooms,
            'ingested_sessions': ingested_sessions,
            'paid_gifts': gift_tier_summary(),
            'paid_gifts_anonymous_sessions': gift_tier_summary(anonymous_sessions_filter),
            'paid_gift_resolution_basis': basis_breakdown,
            'danmaku': danmaku_tier_summary(),
            'danmaku_anonymous_sessions': danmaku_tier_summary(anonymous_sessions_filter),
        }


def _parse_source_spec(spec: str) -> tuple[str, Path]:
    if '=' not in spec:
        raise argparse.ArgumentTypeError('源库参数须为 <name>=<monitor.sqlite3> 格式')
    name, raw_path = spec.split('=', 1)
    name = name.strip()
    if not name or not raw_path.strip():
        raise argparse.ArgumentTypeError('源库名称与路径均不能为空')
    return name, Path(raw_path.strip())


def configure_parser(commands) -> argparse.ArgumentParser:
    parser = commands.add_parser('warehouse', help='跨房全局身份消解数仓与本地匿名网页看板差量回填')
    sub = parser.add_subparsers(dest='warehouse_command', required=True)

    sync = sub.add_parser('sync', help='只读增量同步源库字典并将已完成场次标准化入库')
    sync.add_argument('--warehouse', type=Path, required=True, help='本地数仓 SQLite 路径')
    sync.add_argument('--source', type=_parse_source_spec, action='append', default=[],
                      help='源数据库映射 <name>=<monitor.sqlite3>，可重复指定')
    sync.add_argument('--session-root', type=Path, action='append', default=[],
                      help='场次工作区根目录，可重复指定')
    sync.add_argument('--reingest', action='append', default=[],
                      help='强制事务重入指定 session_key（传 * 重入全部已完成场次）')

    backfill = sub.add_parser('backfill-pages', help='差量回填本地已完成匿名场次看板的送礼身份与去重人数')
    backfill.add_argument('--warehouse', type=Path, required=True, help='本地数仓 SQLite 路径')
    backfill.add_argument('--session-root', type=Path, action='append', required=True,
                          help='场次工作区根目录，可重复指定')
    mode = backfill.add_mutually_exclusive_group()
    mode.add_argument('--dry-run', action='store_true', help='仅只读预演差量变更，不写任何文件（默认）')
    mode.add_argument('--execute', action='store_true', help='获取场次锁并原子写回有变化的本地看板产物')

    stats = sub.add_parser('stats', help='输出数仓全局统计摘要与分档消解率 JSON')
    stats.add_argument('--warehouse', type=Path, required=True, help='本地数仓 SQLite 路径')
    return parser


def main(args) -> int:
    try:
        if args.warehouse_command == 'sync':
            result = sync_warehouse(
                args.warehouse,
                sources=args.source,
                session_roots=args.session_root,
                reingest=set(args.reingest or ()),
            )
        elif args.warehouse_command == 'backfill-pages':
            from .warehouse_backfill import backfill_local_pages
            result = backfill_local_pages(
                args.warehouse,
                session_roots=args.session_root,
                execute=bool(args.execute),
            )
        else:
            result = warehouse_stats(args.warehouse)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (ValueError, TypeError, KeyError, OSError, sqlite3.Error) as error:
        print(json.dumps({'error': f'{type(error).__name__}: {error}'}, ensure_ascii=False))
        return 1
