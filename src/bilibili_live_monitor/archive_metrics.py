"""场次指标独立计算；尚无观测的字段为未知，不阻止场次展示。"""
import math
from bisect import bisect_left, bisect_right

from .activity_summary import gift_observations, nonnegative_integer
from .time_utils import epoch


def prepare_archive_source(source):
    """一次性预解析同房间时间轴与指标序列，供多场次二分切片复用。"""
    raw_epochs = []
    series = {'ONLINE_RANK_COUNT': [], 'WATCHED_CHANGE': [], 'ROOM_REAL_TIME_MESSAGE_UPDATE': []}
    fields = {'ONLINE_RANK_COUNT': 'online_count', 'WATCHED_CHANGE': 'num', 'ROOM_REAL_TIME_MESSAGE_UPDATE': 'fans'}
    for at, command, packet in source.get('raw', ()):
        stamp = epoch(at)
        raw_epochs.append(stamp)
        if command in series and isinstance(packet, dict):
            data = packet.get('data')
            field = fields[command]
            if isinstance(data, dict) and nonnegative_integer(data.get(field)) is not None:
                series[command].append((stamp, data[field]))
    raw_epochs.sort()
    for rows in series.values():
        rows.sort()

    item_epochs = []
    gift_entries = []
    for index, (kind, at, item) in enumerate(source.get('items', ())):
        stamp = epoch(at)
        item_epochs.append(stamp)
        if kind == 'gift':
            gift_entries.append((stamp, index, (kind, at, item)))
    item_epochs.sort()
    gift_entries.sort(key=lambda r: (r[0], r[1]))
    gift_epochs = [r[0] for r in gift_entries]

    segments = []
    for segment in source.get('segments', ()):
        segments.append((epoch(segment['start_utc']), epoch(segment['end_utc'])))

    return {
        'raw_epochs': raw_epochs,
        'online': series['ONLINE_RANK_COUNT'],
        'online_epochs': [r[0] for r in series['ONLINE_RANK_COUNT']],
        'watched': series['WATCHED_CHANGE'],
        'watched_epochs': [r[0] for r in series['WATCHED_CHANGE']],
        'fans': series['ROOM_REAL_TIME_MESSAGE_UPDATE'],
        'fans_epochs': [r[0] for r in series['ROOM_REAL_TIME_MESSAGE_UPDATE']],
        'item_epochs': item_epochs,
        'gift_entries': gift_entries,
        'gift_epochs': gift_epochs,
        'segments': segments,
    }


def _slice_series(rows, epochs, lower, upper):
    left = bisect_left(epochs, lower)
    right = bisect_right(epochs, upper)
    return rows[left:right]


def _has_point_in_closed_range(epochs, lower, upper):
    idx = bisect_left(epochs, lower)
    return idx < len(epochs) and epochs[idx] <= upper


def aggregate_prepared_metrics(prepared, lower, upper, excluded_uids=None):
    if not (math.isfinite(lower) and math.isfinite(upper) and lower <= upper):
        raise ValueError('invalid_metric_time_range')
    online = _slice_series(prepared['online'], prepared['online_epochs'], lower, upper)
    watched = _slice_series(prepared['watched'], prepared['watched_epochs'], lower, upper)
    fans = _slice_series(prepared['fans'], prepared['fans_epochs'], lower, upper)
    weighted = seconds = 0
    for index, (at, value) in enumerate(online):
        end = online[index + 1][0] if index + 1 < len(online) else upper
        duration = max(0, end - at)
        weighted += value * duration
        seconds += duration

    g_left = bisect_left(prepared['gift_epochs'], lower)
    g_right = bisect_right(prepared['gift_epochs'], upper)
    selected_gifts = sorted(prepared['gift_entries'][g_left:g_right], key=lambda r: r[1])
    gift_items = [r[2] for r in selected_gifts]
    gifts = gift_observations({'items': gift_items}, platform_uid=True) if gift_items else []

    observed = (
        _has_point_in_closed_range(prepared['raw_epochs'], lower, upper)
        or _has_point_in_closed_range(prepared['item_epochs'], lower, upper)
        or any(lower < seg_end and seg_start < upper for seg_start, seg_end in prepared['segments'])
    )
    known_amount = observed and all(gift[2] is not None for gift in gifts)
    amount = round(sum(gift[2] for gift in gifts) / 1000, 2) if known_amount else None
    others = donors = None
    if excluded_uids is not None and known_amount and all(not gift[2] or gift[3] is not None for gift in gifts):
        excluded = set(map(str, excluded_uids))
        selected = [gift for gift in gifts if str(gift[3]) not in excluded]
        others = round(sum(gift[2] for gift in selected) / 1000, 2)
        donors = len({str(gift[3]) for gift in selected if gift[2] > 0})
    return {'online_mean': weighted / seconds if seconds else None,
            'online_peak': max((value for _, value in online), default=None),
            'watched_last': watched[-1][1] if watched else None,
            'followers_net': fans[-1][1] - fans[0][1] if len(fans) >= 2 else None,
            'amount': amount, 'amount_nonself': others, 'paid_donors_nonself': donors}


def aggregate_metrics(source, lower, upper, excluded_uids=None):
    """读取同房间来源，仅使用闭区间内观测；排除名单未配置时不计算归属指标。"""
    if not (math.isfinite(lower) and math.isfinite(upper) and lower <= upper):
        raise ValueError('invalid_metric_time_range')
    return aggregate_prepared_metrics(prepare_archive_source(source), lower, upper, excluded_uids=excluded_uids)
