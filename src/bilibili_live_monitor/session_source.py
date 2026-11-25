"""只读载入单场原始记录并裁剪时间范围，不生成页面或触发任务。"""
import json
import sqlite3
from pathlib import Path

from .time_utils import epoch


def scope_source(source: dict, start: str, end: float) -> dict:
    lower = epoch(start)
    return {
        'raw': [row for row in source['raw'] if lower <= epoch(row[0]) <= end],
        'items': [row for row in source['items'] if lower <= epoch(row[1]) <= end],
        'segments': [s for s in source['segments']
                     if lower <= epoch(s['start_utc']) <= epoch(s['end_utc']) <= end],
    }


def _in_clause(values: tuple[str, ...]) -> str:
    if not values:
        raise ValueError('过滤集合不能为空')
    return ','.join('?' for _ in values)


def read_source(
    database: Path,
    room_id: str,
    start: str,
    end: str = '9999',
    *,
    raw_commands: tuple[str, ...] | None = None,
    optional_raw_commands: tuple[str, ...] = (),
    item_json_kinds: tuple[str, ...] | None = None,
) -> dict:
    """读取[start, end)的记录；可选仅解码指定command/kind的大JSON载荷以加速归档聚合。"""
    with sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'raw_events' not in tables:
            raw = []
        elif raw_commands is None:
            raw = [(at, cmd, json.loads(data)) for at, cmd, data in db.execute(
                'SELECT received_utc,command,data FROM raw_events WHERE room_id=? AND received_utc>=? AND received_utc<? '
                'ORDER BY id', (room_id, start, end))]
        else:
            # 顺着(room_id, received_utc)索引顺序扫描，再按id用Timsort恢复精确插入序，避免SQLite临时B-Tree排序。
            decoded = (*raw_commands, *optional_raw_commands)
            placeholders = _in_clause(decoded)
            raw_rows = db.execute(
                f'SELECT id,received_utc,command,CASE WHEN command IN ({placeholders}) THEN data END '
                'FROM raw_events WHERE room_id=? AND received_utc>=? AND received_utc<?',
                (*decoded, room_id, start, end),
            ).fetchall()
            raw_rows.sort(key=lambda r: r[0])
            raw = []
            for _, at, cmd, data in raw_rows:
                try:
                    packet = json.loads(data) if data is not None else {}
                except (ValueError, TypeError):
                    if cmd not in optional_raw_commands:
                        raise
                    packet = {}
                raw.append((at, cmd, packet))

        if 'timeline' not in tables:
            items = []
        elif item_json_kinds is None:
            items = [(kind, at, {**json.loads(data), '_record_id': ident}) for ident, kind, at, data in db.execute(
                'SELECT id,kind,at_utc,data FROM timeline WHERE room_id=? AND at_utc>=? AND at_utc<? ORDER BY id',
                (room_id, start, end))]
        else:
            placeholders = _in_clause(item_json_kinds)
            item_rows = db.execute(
                f'SELECT id,kind,at_utc,CASE WHEN kind IN ({placeholders}) THEN data END '
                'FROM timeline WHERE room_id=? AND at_utc>=? AND at_utc<?',
                (*item_json_kinds, room_id, start, end),
            ).fetchall()
            item_rows.sort(key=lambda r: r[0])
            items = [
                (kind, at, ({**json.loads(data), '_record_id': ident} if data is not None else {'_record_id': ident}))
                for ident, kind, at, data in item_rows
            ]

        segments = [json.loads(data) for data, in db.execute(
            "SELECT data FROM segments WHERE room_id=? AND json_extract(data,'$.start_utc')>=? "
            "AND json_extract(data,'$.start_utc')<? ORDER BY seq", (room_id, start, end))] if 'segments' in tables else []
    from .gift_value import gift_text
    for kind, _, item in items:
        if kind == 'gift' and isinstance(item.get('gift'), dict):
            item['text'] = gift_text(item['gift'], item.get('text', ''))
    return {'raw': raw, 'items': items, 'segments': segments}
