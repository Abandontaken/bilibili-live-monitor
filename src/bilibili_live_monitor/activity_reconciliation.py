"""区分分段已覆盖与待归类礼物，按事件去重核对整场金额。"""
from .activity_summary import gift_observations
from .live_share import epoch


def reconcile_activity(source, rows):
    windows = [(epoch(row['start']), epoch(row['window_end'])) for row in rows
               if row.get('kind') in ('song', 'talk') and not row.get('time_unknown')]
    cutoff = max((epoch(row['window_end']) for row in rows), default=None)
    totals = {'total': 0, 'covered': 0, 'unclassified': 0, 'overlap': 0}
    unknown = {'total': 0, 'covered': 0, 'unclassified': 0}
    unclassified_events = 0
    for at, _, amount, _, _ in gift_observations(source, event_time_only=True):
        matches = sum(start <= at < end or at == cutoff == end for start, end in windows) if at is not None else 0
        group = 'covered' if matches else 'unclassified'
        if not matches:
            unclassified_events += 1
        if amount is None:
            unknown['total'] += 1
            unknown[group] += 1
            continue
        totals['total'] += amount
        totals[group] += amount
        totals['overlap'] += amount * max(0, matches - 1)
    return {**{key + '_amount_yuan': value / 1000 for key, value in totals.items()},
            **{key + '_amount_unknown': value for key, value in unknown.items()},
            'unclassified_gift_events': unclassified_events}


def render_reconciliation(data):
    def money(key):
        return f'¥{data[key + "_amount_yuan"]:,.2f}' + ('+' if data.get(key + '_amount_unknown') else '')

    overlap = data.get('overlap_amount_yuan', 0)
    note = (f'相邻分段的30秒尾窗另有 ¥{overlap:,.2f} 重复计入，分段行不能直接相加。' if overlap
            else '下方仅列已归类分段，不代表整场全部礼物。')
    return ('<div id="activity-reconciliation" style="margin:12px 0;padding:14px 16px;'
            'border:1px solid #dbe4ec;border-radius:10px;background:#f8fafc;line-height:1.8">'
            f'<strong>整场礼物 {money("total")} = 分段已覆盖 {money("covered")} + 待归类 {money("unclassified")}</strong>'
            f'<p style="margin:4px 0 0">{note} 待归类包含尚未完成有效分类的时段，不能直接计为杂谈。'
            '金额为平台标价，非主播实收；“+”表示另有金额未知的事件。</p></div>')
