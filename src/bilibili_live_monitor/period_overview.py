"""滚动周期摘要与可选送礼占比；输出不含账号身份或配置。"""
import math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .gift_value import value_fields


def period_overview(days, *, now=None, share_amounts=None, share_label=None, unassigned_dates=()):
    today = (now or datetime.now(ZoneInfo('Asia/Shanghai'))).astimezone(ZoneInfo('Asia/Shanghai')).date()
    result = []
    for count in (7, 30):
        start = today - timedelta(days=count - 1)
        selected = [row for row in days if start.isoformat() <= row['date'] <= today.isoformat()
                    and row.get('has_message_observations', row.get('has_observations'))]
        known = [row for row in selected if row.get('gift_amount_yuan') is not None]
        total = round(sum(row['gift_amount_yuan'] for row in known), 3) if known else None
        unknown = sum(row.get('gift_amount_unknown', 0) for row in selected)
        entries = [row['entries'] for row in selected if row.get('entries') is not None]
        item = {'days': count, 'start': start.isoformat(), 'end': today.isoformat(),
                'observed_days': len(entries), 'entries_daily_mean': sum(entries) / len(entries) if entries else None,
                'gift_amount_yuan': total, 'gift_amount_unknown': unknown}
        item.update(fans_club_period(days, start.isoformat(), today.isoformat()))
        if share_label is not None:
            amounts = [(share_amounts or {}).get(row['date'], 0) for row in selected]
            own = sum(amount or 0 for amount in amounts)
            incomplete = unknown or None in amounts or any(row['date'] in unassigned_dates for row in selected)
            item['gift_share'] = _gift_share(share_label, own, total, incomplete)
        result.append(item)
    return value_fields(result)


def fans_club_period(days, start, end):
    """窗口内按天收盘的粉丝团人数；变化以窗口前最后一个收盘值为基线，缺基线时用窗口内首个收盘值。"""
    closes = sorted((row['date'], row['fans_club_close']) for row in days
                    if type(row.get('fans_club_close')) is int and row['date'] <= end)
    inside = [(date, value) for date, value in closes if date >= start]
    if not inside:
        return {'fans_club_latest': None, 'fans_club_latest_date': None, 'fans_club_daily_mean': None,
                'fans_club_change': None, 'fans_club_change_since': None}
    before = [(date, value) for date, value in closes if date < start]
    base = before[-1] if before else inside[0] if len(inside) > 1 else None
    return {'fans_club_latest': inside[-1][1], 'fans_club_latest_date': inside[-1][0],
            'fans_club_daily_mean': sum(value for _, value in inside) / len(inside),
            'fans_club_change': inside[-1][1] - base[1] if base else None,
            'fans_club_change_since': base[0] if base else None}


def _gift_share(label, own, total, incomplete):
    percentage = own / total * 100 if total and total > 0 else None
    result = {'label': label, 'percentage': None if incomplete else percentage}
    if incomplete and percentage is not None:
        result['confirmed_percentage'] = percentage
    return result


def overall_share(days, amounts, label, *, unassigned_dates=()):
    observed = [row for row in days if row.get('has_observations')]
    total = sum(row.get('gift_amount_yuan') or 0 for row in observed)
    unknown = any(row.get('gift_amount_unknown') for row in observed)
    own = sum(amount or 0 for amount in amounts.values())
    incomplete = unknown or None in amounts.values() or any(row['date'] in unassigned_dates for row in observed)
    return _gift_share(label, own, total, incomplete)


def summary_gift_share(rows, label):
    """总体沿用已审核场次金额，避免遗漏只有历史汇总的场次。"""
    # 调用者须确认 amount_nonself 排除的账号集合与当前指定对象一致。
    metrics_rows = [row.get('metrics', {}) for row in rows]
    covered = []
    incomplete = False
    partial_sessions = 0
    for metrics in metrics_rows:
        total, nonself = metrics.get('amount'), metrics.get('amount_nonself')
        partial = total is None or nonself is None
        if partial:
            total, nonself = metrics.get('amount_priced'), metrics.get('amount_nonself_priced')
        if not all(type(value) in (int, float) and math.isfinite(value) for value in (total, nonself)):
            continue
        if not 0 <= nonself <= total:
            continue
        covered.append((total, nonself))
        partial_sessions += partial
        incomplete |= partial or ('paid_donors_nonself' in metrics and metrics['paid_donors_nonself'] is None)
    result = _gift_share(label, sum(a - b for a, b in covered), sum(a for a, _ in covered),
                         incomplete or len(covered) != len(metrics_rows))
    if 'confirmed_percentage' in result and len(covered) != len(metrics_rows):
        result.update(covered_sessions=len(covered), total_sessions=len(metrics_rows))
    if 'confirmed_percentage' in result and partial_sessions:
        result['partial_sessions'] = partial_sessions
    return result
