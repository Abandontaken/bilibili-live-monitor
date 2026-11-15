from bilibili_live_monitor.activity_reconciliation import reconcile_activity, render_reconciliation


def gift(at, amount, event):
    return ('gift', at, {'source_key': event, 'gift': {'event_type': 'SEND_GIFT',
        'event_id': event, 'coin_type': 'gold', 'total_coin': amount, 'num': 1,
        'event_at_utc': at, 'time_source': 'event'}})


def test_missing_intervals_and_overlaps_reconcile_without_double_counting():
    source = {'items': [gift('2026-01-01T00:00:05Z', 3000000, 'a'),
                        gift('2026-01-01T00:00:15Z', 3000000, 'b'),
                        gift('2026-01-01T00:00:25Z', 1754000, 'c')]}
    rows = [{'kind': 'talk', 'start': '2026-01-01T00:00:00Z', 'window_end': '2026-01-01T00:00:10Z'},
            {'kind': 'song', 'start': '2026-01-01T00:00:04Z', 'window_end': '2026-01-01T00:00:09Z'}]
    data = reconcile_activity(source, rows)
    assert data['total_amount_yuan'] == 7754
    assert data['covered_amount_yuan'] == 3000
    assert data['unclassified_amount_yuan'] == 4754
    assert data['overlap_amount_yuan'] == 3000
    assert data['unclassified_gift_events'] == 2
    assert '分段行不能直接相加' in render_reconciliation(data)


def test_duplicate_and_unknown_amounts_are_not_hidden():
    item = gift('2026-01-01T00:00:05Z', 0, 'a')
    unknown = gift('2026-01-01T00:00:15Z', None, 'b')
    data = reconcile_activity({'items': [item, item, unknown]}, [])
    assert data['unclassified_gift_events'] == 2
    assert data['total_amount_unknown'] == 1
    assert data['unclassified_amount_unknown'] == 1
    assert '¥0.00+' in render_reconciliation(data)


def test_unclassified_and_unknown_times_are_not_covered():
    item = gift('2026-01-01T00:00:05Z', 10000, 'a')
    row = {'kind': 'unclassified', 'start': '2026-01-01T00:00:00Z',
           'window_end': '2026-01-01T00:00:10Z'}
    data = reconcile_activity({'items': [item]}, [row])
    assert data['covered_amount_yuan'] == 0
    assert data['unclassified_amount_yuan'] == 10
    item[2]['gift']['time_source'] = 'received'
    data = reconcile_activity({'items': [item]}, [{**row, 'kind': 'talk'}, {**row, 'time_unknown': True}])
    assert data['covered_amount_yuan'] == data['overlap_amount_yuan'] == 0
    assert data['unclassified_amount_yuan'] == 10


def test_final_cutoff_event_matches_summary():
    item = gift('2026-01-01T00:00:10Z', 10000, 'a')
    rows = [{'kind': 'talk', 'start': '2026-01-01T00:00:00Z', 'window_end': '2026-01-01T00:00:10Z'}]
    assert reconcile_activity({'items': [item]}, rows)['covered_amount_yuan'] == 10
