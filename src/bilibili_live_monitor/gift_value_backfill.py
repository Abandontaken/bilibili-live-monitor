"""只重算已有快照的礼物派生字段，不触发采集、ASR、AI或发布。"""
import json
import sqlite3
from copy import deepcopy
from pathlib import Path
from urllib.parse import urlsplit

from .activity_summary import gift_observations, summarize_gifters
from .gift_value import VALUE_BASIS, value_fields
from .time_utils import epoch, iso


def totals(events):
    return {'gift_count': sum(row[1] or 0 for row in events),
            'gift_count_unknown': sum(row[1] is None for row in events),
            'gift_amount_yuan': sum(row[2] or 0 for row in events) / 1000,
            'gift_amount_unknown': sum(row[2] is None for row in events)}


def refresh_snapshot(snapshot, source):
    """source须是同房间完整时间窗；保留场次、分析、导航与全部非礼物字段。"""
    result = deepcopy(snapshot)
    lower, cutoff = epoch(snapshot['session_start']), epoch(snapshot['updated_at'])
    scoped = {**source, 'items': [row for row in source['items'] if lower <= epoch(row[1]) <= cutoff]}
    events = gift_observations(scoped)
    timed = gift_observations(scoped, event_time_only=True)
    old_count = snapshot.get('session_summary', {}).get('gift_count')
    if old_count is not None and old_count != totals(events)['gift_count']:
        raise ValueError('回填礼物数量与原快照不一致，需核查原子事件范围')
    result.setdefault('session_summary', {}).update(totals(events))
    result['gift_summary'] = summarize_gifters(scoped)
    for row in result.get('activity_summary', []):
        if row.get('time_unknown'):
            selected = [event for event in timed if event[0] is None]
        else:
            start, end = epoch(row['start']), epoch(row.get('window_end', row['end']))
            selected = [event for event in timed if event[0] is not None and
                        (start <= event[0] < end or start <= event[0] == cutoff == end)]
        row.update(totals(selected))
    if result.get('activity_reconciliation'):
        # 私有展示扩展可选存在；这里只更新已有分区金额，不改变分段定义。
        rows = result.get('activity_summary', [])
        sums = {'total': 0, 'covered': 0, 'unclassified': 0, 'overlap': 0}
        unknown = {'total': 0, 'covered': 0, 'unclassified': 0}
        missing = 0
        for at, _, value, _, _ in events:
            matches = sum(epoch(row['start']) <= at < epoch(row.get('window_end', row['end'])) or
                          epoch(row['start']) <= at == cutoff == epoch(row.get('window_end', row['end']))
                          for row in rows if not row.get('time_unknown'))
            group = 'covered' if matches else 'unclassified'
            missing += not matches
            if value is None:
                unknown['total'] += 1
                unknown[group] += 1
            else:
                sums['total'] += value
                sums[group] += value
                sums['overlap'] += value * max(0, matches - 1)
        result['activity_reconciliation'].update({key + '_amount_yuan': value / 1000 for key, value in sums.items()})
        result['activity_reconciliation'].update({key + '_amount_unknown': value for key, value in unknown.items()})
        result['activity_reconciliation']['unclassified_gift_events'] = missing
    result['gift_value_basis'] = VALUE_BASIS
    return value_fields(result)


def refresh_archive_values(rows, config):
    """重估公开汇总的金额；其他人工审核指标保留，原始汇总文件不改。"""
    windows = {}
    for root in (config.get('sessions_root'), config.get('historical_root')):
        if not root or not Path(root).is_dir():
            continue
        for directory in Path(root).iterdir():
            cfg_path, snap_path = directory / 'config.json', directory / 'public/snapshot.json'
            if not cfg_path.is_file() or not snap_path.is_file():
                continue
            cfg, snap = json.loads(cfg_path.read_text()), json.loads(snap_path.read_text())
            if str(cfg.get('room_id')) == str(config['room_id']):
                windows[urlsplit(cfg.get('url', '')).path] = (snap['session_start'], snap['updated_at'])
    result = deepcopy(rows)
    with sqlite3.connect(Path(config['database']).resolve().as_uri() + '?mode=ro', uri=True) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        for row in result:
            fallback = windows.get(urlsplit(row.get('url', '')).path, (None, None))
            start, end = row.get('start_utc') or fallback[0], row.get('updated_at') or fallback[1]
            metrics = row['metrics']
            metrics.update(amount=None, amount_nonself=None, paid_donors_nonself=None)
            row['gift_value_basis'] = VALUE_BASIS
            if not start or not end:
                continue
            records = db.execute("SELECT kind,at_utc,data FROM timeline WHERE room_id=? AND kind='gift' "
                                 'AND at_utc>=? AND at_utc<=? ORDER BY id',
                                 (str(config['room_id']), iso(epoch(start)), iso(epoch(end)))).fetchall()
            source = {'items': [(kind, at, json.loads(data)) for kind, at, data in records]}
            events = gift_observations(source, platform_uid=True)
            if not events:
                continue
            if any(event[2] is None for event in events):
                continue
            metrics['amount'] = sum(event[2] for event in events) / 1000
            excluded = set(map(str, config.get('exclude_account_uids', [])))
            others = [event for event in events if str(event[3]) not in excluded]
            metrics['amount_nonself'] = sum(event[2] for event in others) / 1000
            if all(not event[2] or event[3] is not None for event in others):
                metrics['paid_donors_nonself'] = len({str(event[3]) for event in others if event[2] > 0})
    return result


def audience_value_map(material, config):
    """只读重算既有CRM事件的展示金额，不改变冻结material、引用或AI状态。"""
    from .audience_crm import digest, identity
    from .gift_value import gift_value
    if not config.get('database'):
        return None
    room, values = str(config['room_id']), {}
    with sqlite3.connect(Path(config['database']).resolve().as_uri() + '?mode=ro', uri=True) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        for session in material['sessions']:
            records = db.execute("SELECT id,at_utc,data FROM timeline WHERE room_id=? AND kind='gift' "
                                 'AND at_utc>=? AND at_utc<=? ORDER BY id',
                                 (room, iso(epoch(session['start'])), iso(epoch(session['end'])))).fetchall()
            for ident, at, payload in records:
                data = {**json.loads(payload), '_record_id': ident}
                uid = identity(data)
                if uid is None:
                    continue
                gift = data.get('gift') or {}
                family = str(gift.get('event_type', 'gift'))
                family = 'SEND_GIFT' if family.startswith('SEND_GIFT') else family
                event = gift.get('event_id')
                key = ('gift', uid, family, str(event), gift.get('batch_index', 0)) if event else (
                    'gift', uid, data.get('source_key') or ident)
                values[digest([room, session['id'], key])[:32]] = gift_value(gift)
    return values


def refresh_detail_values(detail, config):
    """旧静态详情的金额适配；保留曲线、内容与下播后结算字段。"""
    result = deepcopy(detail)
    snapshot = {'session_start': detail['start'], 'updated_at': detail['end'],
                'session_summary': detail['summary'], 'activity_summary': deepcopy(detail['segments'])}
    for row in snapshot['activity_summary']:
        for key in ('start', 'end', 'window_end'):
            if type(row.get(key)) in (int, float):
                row[key] = iso(row[key])
    closing = []
    with sqlite3.connect(Path(config['database']).resolve().as_uri() + '?mode=ro', uri=True) as db:
        db.execute('PRAGMA query_only=ON')
        records = db.execute("SELECT kind,at_utc,data FROM timeline WHERE room_id=? AND kind='gift' "
                             'AND at_utc>=? AND at_utc<=? ORDER BY id',
                             (str(config['room_id']), iso(epoch(detail['start'])), iso(epoch(detail['end'])))).fetchall()
        if detail.get('closing_amount') is not None:
            after = db.execute("SELECT kind,at_utc,data FROM timeline WHERE room_id=? AND kind='gift' "
                               'AND at_utc>? AND at_utc<=? ORDER BY id',
                               (str(config['room_id']), iso(epoch(detail['end'])), iso(epoch(detail['end']) + 60))).fetchall()
            closing = [(kind, at, json.loads(data)) for kind, at, data in after
                       if json.loads(data).get('gift', {}).get('event_type') == 'GUARD_BUY'
                       and json.loads(data)['gift'].get('guard_price_gold') == round(detail['closing_amount'] * 1000)]
            if len(closing) != 1:
                raise ValueError('无法唯一核实历史下播后舰长结算')
            snapshot['session_summary'] = dict(snapshot['session_summary'])
            snapshot['session_summary']['gift_count'] -= closing[0][2]['gift']['num']
    source = {'items': [(kind, at, json.loads(data)) for kind, at, data in records]}
    fresh = refresh_snapshot(snapshot, source)
    if closing:
        live_value = fresh['session_summary']['gift_amount_yuan']
        fresh['session_summary']['gift_amount_yuan'] += detail['closing_amount']
        fresh['session_summary']['gift_count'] += closing[0][2]['gift']['num']
        fresh['session_summary']['ordinary_gift_amount_yuan'] = live_value
        fresh['session_summary']['guard_amount_yuan'] = detail['closing_amount']
        fresh['gift_summary'] = summarize_gifters({'items': source['items'] + closing})
        value_fields(fresh)
    result['summary'] = fresh['session_summary']
    result['gift_summary'] = fresh['gift_summary']
    for old, new in zip(result['segments'], fresh['activity_summary']):
        old.update({key: value for key, value in new.items() if key.startswith('gift_')})
    result['gift_value_basis'] = VALUE_BASIS
    return result
