"""按场次和时间截取弹幕，只作为转写纠错的只读旁证；支持同场原始事件与只读数仓匿名真名叠加。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from .time_utils import epoch
from .warehouse_protocol import (
    _HEX8_RE,
    _extract_danmaku_u15_user,
    _extract_uinfo_dict,
    clean_uname,
    extract_raw_identities,
    extract_timeline_identity,
    first_char,
    is_masked_name,
    pos_int,
    uid_crc32,
)


class _ReadonlyIdentityPool:
    """只读实名消解池：严格遵循 CRC32+首字 与 全局唯一头像 防撞规则。"""

    def __init__(self) -> None:
        self.uname_by_uid: dict[int, str] = {}
        self.by_crc32: dict[str, set[int]] = {}
        self.by_face: dict[str, set[int]] = {}

    def add(self, uid: int, uname: str | None, face_sha1: str | None, *, prefer_existing_name: bool = False) -> None:
        if not uid or uid <= 0:
            return
        cleaned = clean_uname(uname) if uname and not is_masked_name(uname) else None
        if cleaned:
            if not prefer_existing_name or uid not in self.uname_by_uid:
                self.uname_by_uid[uid] = cleaned
            self.by_crc32.setdefault(uid_crc32(uid), set()).add(uid)
        if face_sha1:
            self.by_face.setdefault(face_sha1, set()).add(uid)

    def resolve_danmaku(
        self,
        clue_crc32: str | None,
        clue_first_char: str | None,
        clue_face_sha1: str | None,
    ) -> str | None:
        if clue_crc32:
            candidates = [
                uid for uid in self.by_crc32.get(clue_crc32, ())
                if uid in self.uname_by_uid
                and (clue_first_char is None or first_char(self.uname_by_uid[uid]) == clue_first_char)
            ]
            if len(candidates) == 1:
                return self.uname_by_uid[candidates[0]]
            if len(candidates) > 1 and clue_face_sha1:
                face_uids = self.by_face.get(clue_face_sha1, set())
                if len(face_uids) == 1:
                    disambiguated = [uid for uid in candidates if uid in face_uids]
                    if len(disambiguated) == 1:
                        return self.uname_by_uid[disambiguated[0]]
                return None

        if clue_face_sha1:
            face_uids = self.by_face.get(clue_face_sha1, set())
            if len(face_uids) == 1:
                uid = next(iter(face_uids))
                uname = self.uname_by_uid.get(uid)
                if uname:
                    if clue_crc32 is not None:
                        if uid_crc32(uid) == clue_crc32:
                            return uname
                    elif clue_first_char is None or first_char(uname) == clue_first_char:
                        return uname
        return None


def _extract_danmu_clues(source: dict[str, Any]) -> tuple[_ReadonlyIdentityPool, dict[str, tuple[str | None, str | None, str | None]]]:
    pool = _ReadonlyIdentityPool()
    danmu_clues: dict[str, tuple[str | None, str | None, str | None]] = {}

    for _, command, packet in source.get('raw', []):
        if not isinstance(packet, dict):
            continue
        cmd = str(command or packet.get('cmd') or '')
        identities, _ = extract_raw_identities(cmd, packet)
        for uid, uname, face_sha1, _ in identities:
            pool.add(uid, uname, face_sha1)

        if cmd.startswith('DANMU_MSG'):
            info = packet.get('info')
            if not isinstance(info, list) or len(info) < 3 or not isinstance(info[0], list) or len(info[0]) < 5:
                continue
            u15_user = _extract_danmaku_u15_user(info)
            u_uid, u_name, u_face, _, u_mys = _extract_uinfo_dict(u15_user)
            if u_mys:
                continue
            user_arr = info[2] if isinstance(info[2], list) else []
            uid = pos_int(user_arr[0] if user_arr else 0) or u_uid
            uname = clean_uname(user_arr[1] if len(user_arr) > 1 else None) or u_name
            if uid is not None and uname and not is_masked_name(uname):
                pool.add(uid, uname, u_face)
                continue
            raw_crc = str(info[0][7]).strip().lower() if len(info[0]) > 7 and info[0][7] is not None else ''
            crc_val = raw_crc if _HEX8_RE.fullmatch(raw_crc) and raw_crc != '00000000' else None
            f_char = first_char(uname)
            if crc_val or u_face:
                source_key = hashlib.sha256(
                    json.dumps(info, ensure_ascii=False, sort_keys=True).encode()
                ).hexdigest()
                danmu_clues.setdefault(source_key, (crc_val, f_char, u_face))

    for _, _, data in source.get('items', []):
        if isinstance(data, dict):
            tl_ident = extract_timeline_identity(data)
            if tl_ident is not None:
                pool.add(tl_ident[0], tl_ident[1], None)

    return pool, danmu_clues


def _augment_from_warehouse(
    pool: _ReadonlyIdentityPool,
    warehouse_database: str | Path,
    needed_crc32: set[str],
    needed_faces: set[str],
) -> None:
    wh_path = Path(warehouse_database)
    if not wh_path.is_file() or (not needed_crc32 and not needed_faces):
        return
    try:
        conn = sqlite3.connect(wh_path.resolve().as_uri() + '?mode=ro', uri=True, timeout=15.0)
    except sqlite3.Error:
        return
    try:
        conn.execute('PRAGMA query_only = ON')
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")
        }
        if 'dim_users' not in tables:
            return
        crc_list = sorted(needed_crc32)
        for offset in range(0, len(crc_list), 400):
            chunk = crc_list[offset:offset + 400]
            placeholders = ','.join('?' for _ in chunk)
            rows = conn.execute(
                f'SELECT uid, latest_uname FROM dim_users WHERE uid_crc32 IN ({placeholders}) AND latest_uname IS NOT NULL',
                chunk,
            ).fetchall()
            for uid, uname in rows:
                if isinstance(uid, int) and uid > 0:
                    pool.add(uid, uname, None, prefer_existing_name=True)

        if 'dim_user_faces' in tables and needed_faces:
            face_list = sorted(needed_faces)
            for offset in range(0, len(face_list), 400):
                chunk = face_list[offset:offset + 400]
                placeholders = ','.join('?' for _ in chunk)
                rows = conn.execute(
                    f'''SELECT f.face_sha1, f.uid, u.latest_uname
                        FROM dim_user_faces f
                        LEFT JOIN dim_users u ON f.uid = u.uid
                        WHERE f.face_sha1 IN ({placeholders})''',
                    chunk,
                ).fetchall()
                for face_sha1, uid, uname in rows:
                    if isinstance(face_sha1, str) and isinstance(uid, int) and uid > 0:
                        pool.add(uid, uname, face_sha1, prefer_existing_name=True)
    except sqlite3.Error:
        return
    finally:
        conn.close()


def resolve_danmaku_authors(
    source: dict[str, Any],
    *,
    lower: float | None = None,
    upper: float | None = None,
    warehouse_database: str | Path | None = None,
    frozen_warehouse_overrides: dict[str, list[str]] | None = None,
) -> tuple[dict[str, str], dict[str, list[str]]]:
    """只读消解弹幕打码昵称。

    返回 (resolved_by_msg_id, warehouse_overrides_used)：
    - resolved_by_msg_id: {'m:<id>': '解出真名'}
    - warehouse_overrides_used: {'m:<id>': ['原打码名', '解出真名']}（仅记录依赖外部数仓解出的条目，供冻结计划固化）
    """
    candidates: list[tuple[str, str, str | None]] = []
    for kind, at, data in source.get('items', []):
        if kind != 'danmaku' or not isinstance(data, dict) or data.get('_record_id') is None:
            continue
        if lower is not None and upper is not None and not (lower <= epoch(at) <= upper):
            continue
        author = str(data.get('author') or '')
        if author and author != '神秘人' and not data.get('sender_hidden') and is_masked_name(author):
            msg_id = 'm:' + str(data['_record_id'])
            candidates.append((msg_id, author, data.get('source_key')))

    if not candidates:
        return {}, {}

    pool, danmu_clues = _extract_danmu_clues(source)
    resolved: dict[str, str] = {}
    unresolved: list[tuple[str, str, tuple[str | None, str | None, str | None]]] = []

    for msg_id, author, source_key in candidates:
        clues = danmu_clues.get(source_key or '')
        if not clues:
            continue
        hit = pool.resolve_danmaku(*clues)
        if hit:
            resolved[msg_id] = hit
        else:
            unresolved.append((msg_id, author, clues))

    warehouse_used: dict[str, list[str]] = {}
    if not unresolved:
        return resolved, warehouse_used

    if frozen_warehouse_overrides is not None:
        for msg_id, author, _ in unresolved:
            pair = frozen_warehouse_overrides.get(msg_id)
            if (
                isinstance(pair, list)
                and len(pair) == 2
                and pair[0] == author
                and isinstance(pair[1], str)
                and pair[1]
            ):
                resolved[msg_id] = pair[1]
                warehouse_used[msg_id] = [author, pair[1]]
        return resolved, warehouse_used

    if warehouse_database:
        needed_crc32 = {c[0] for _, _, c in unresolved if c[0]}
        needed_faces = {c[2] for _, _, c in unresolved if c[2]}
        _augment_from_warehouse(pool, warehouse_database, needed_crc32, needed_faces)
        for msg_id, author, clues in unresolved:
            hit = pool.resolve_danmaku(*clues)
            if hit:
                resolved[msg_id] = hit
                warehouse_used[msg_id] = [author, hit]

    return resolved, warehouse_used


def danmaku_context(
    source: dict[str, Any],
    lower: float,
    upper: float,
    *,
    warehouse_database: str | Path | None = None,
    frozen_warehouse_overrides: dict[str, list[str]] | None = None,
) -> list[dict[str, str]]:
    resolved, _ = resolve_danmaku_authors(
        source,
        lower=lower,
        upper=upper,
        warehouse_database=warehouse_database,
        frozen_warehouse_overrides=frozen_warehouse_overrides,
    )
    rows: list[dict[str, str]] = []
    for kind, at, data in source.get('items', []):
        if kind == 'danmaku' and data.get('_record_id') is not None and lower <= epoch(at) <= upper:
            msg_id = 'm:' + str(data['_record_id'])
            rows.append({
                'id': msg_id,
                'at': at,
                'name': resolved.get(msg_id, data.get('author', '')),
                'text': data.get('text', ''),
            })
    return rows
