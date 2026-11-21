import json
import sqlite3
from datetime import datetime, timezone

import pytest

from bilibili_live_monitor.warehouse_backfill import (
    _load_blind_box_assignments,
    overlay_session_gift_identities,
)
from bilibili_live_monitor.warehouse_protocol import extract_session_events

START = 1_800_000_000


def _row(seq, offset, command, data):
    return seq, datetime.fromtimestamp(START + offset, timezone.utc).isoformat(), command, {'data': data}


def _rank(seq, offset, score, *, other=30):
    return _row(seq, offset, 'ONLINE_RANK_V3', {'online_list': [
        {'uid': 42, 'uname': '灰头', 'score': str(score)},
        {'uid': 43, 'uname': '灰尾', 'score': str(other)},
    ]})


def _gift(seq, offset, price, *, uid=0, tid=None):
    return _row(seq, offset, 'SEND_GIFT', {
        'uid': uid, 'uname': '灰***', 'giftName': '测试结果', 'num': 1,
        'coin_type': 'gold', 'price': price, 'total_coin': price,
        'tid': tid or f'test-{seq}', 'timestamp': START + offset,
        'blind_gift': {'original_gift_name': '测试盲盒', 'original_gift_price': 15000},
    })


def _extract(rows, cutoff=60):
    return [e for e in extract_session_events('1', START, START + cutoff, rows) if e['kind'] == 'gift']


def test_split_rank_updates_match_face_value_without_changing_paid_cost():
    gifts = _extract([
        _rank(1, 1, 100), _gift(2, 10, 40000), _rank(3, 11, 500),
        _gift(4, 13, 9000), _rank(5, 16, 590),
    ])
    assert [(g['inferred_uid'], g['inference_method']) for g in gifts] == [(42, 'blind_box_rank_window')] * 2
    assert [(g['paid_milli_yuan'], g['output_milli_yuan']) for g in gifts] == [(15000, 40000), (15000, 9000)]


@pytest.mark.parametrize('after,other,cutoff', [(250, 30, 60), (500, 430, 60), (500, 30, 10), (500, 31, 60)])
def test_blind_box_rejects_wrong_amount_ambiguity_and_out_of_session_frames(after, other, cutoff):
    gift, = _extract([_rank(1, 1, 100), _gift(2, 10, 40000), _rank(3, 12, after, other=other)], cutoff)
    assert gift['inferred_uid'] is None


def test_native_identity_is_preserved_and_blocks_contaminated_window():
    gifts = _extract([
        _rank(1, 1, 100), _gift(2, 10, 40000), _gift(3, 11, 40000, uid=42), _rank(4, 12, 500),
    ])
    assert gifts[0]['inferred_uid'] is None
    assert gifts[1]['raw_uid'] == 42


def test_competing_score_before_competitor_drops_off_board_blocks_attribution():
    tail = _rank(5, 22, 590)
    tail[3]['data']['online_list'].pop()
    gifts = _extract([
        _rank(1, 1, 100), _gift(2, 10, 9000), _rank(3, 15, 100, other=120),
        _gift(4, 20, 40000), tail,
    ])
    assert all(g['inferred_uid'] is None for g in gifts)


def test_duplicate_transaction_does_not_create_an_inferred_gift():
    gifts = _extract([
        _rank(1, 1, 100), _gift(2, 10, 40000, tid='repeat'),
        _gift(3, 11, 40000, tid='repeat'), _rank(4, 12, 500),
    ])
    assert len(gifts) == 1
    assert gifts[0]['inferred_uid'] is None


@pytest.mark.parametrize('case', ['preexisting_jump', 'reset', 'bad_packet', 'unknown_face', 'missing_box_name', 'early_total'])
def test_unclean_window_is_not_assigned(case):
    gift = _gift(4, 20, 40000)
    rows = [_rank(1, 1, 100)]
    after = 500
    if case == 'preexisting_jump':
        rows.append(_rank(2, 19.5, 500))
    elif case == 'reset':
        rows.append(_rank(2, 19.5, 0))
    elif case == 'bad_packet':
        rows.append(_row(2, 19.5, 'SEND_GIFT_V2', {'pb': 'bad'}))
    elif case == 'unknown_face':
        other = _gift(2, 19, 9000)
        other[3]['data']['uname'] = '花***'
        del other[3]['data']['price']
        rows.append(other)
    elif case == 'early_total':
        rows.extend([_gift(2, 10, 9000), _rank(3, 15, 590)])
        after = 590
    else:
        del gift[3]['data']['blind_gift']['original_gift_name']
        after = 250
    rows.extend([gift, _rank(5, 21, after)])
    assert all(g['inferred_uid'] is None for g in _extract(rows))


def test_page_overlay_loads_same_assignment_from_warehouse(tmp_path):
    source_db, warehouse = tmp_path / 'source.db', tmp_path / 'warehouse.db'
    _, at, cmd, packet = _gift(2, 20, 40000)
    with sqlite3.connect(source_db) as db:
        db.execute('CREATE TABLE raw_events(id, room_id, received_utc, command, data)')
        db.execute('INSERT INTO raw_events VALUES (2, ?, ?, ?, ?)', ('1', at, cmd, json.dumps(packet)))
    with sqlite3.connect(warehouse) as db:
        db.execute('CREATE TABLE dim_sessions(session_key, source_db_path, start_ts, cutoff_ts)')
        db.execute('CREATE TABLE v_resolved_events(session_key, source_ref, resolved_uid, resolution_basis)')
        db.execute('INSERT INTO dim_sessions VALUES (?, ?, ?, ?)', ('s1', str(source_db.resolve()), START, START + 60))
        db.execute("INSERT INTO v_resolved_events VALUES ('s1', 'raw:2:0', 42, 'blind_box_rank_window')")
    config = {'workflow_key': 's1', 'database': str(source_db), 'room_id': '1',
              'start_utc': datetime.fromtimestamp(START, timezone.utc).isoformat()}
    assert _load_blind_box_assignments(warehouse, config, START + 25) is None
    identities = _load_blind_box_assignments(warehouse, config, START + 60)
    assert identities == {('test-2', 0): 42}
    item = {'author': '灰***', 'gift': {'event_id': 'test-2', 'event_type': 'SEND_GIFT'}}
    source = {'raw': [(at, cmd, packet)], 'items': [('gift', at, item)]}
    result, _ = overlay_session_gift_identities(source, {42: '灰头'}, {}, {}, identities)
    assert result['items'][0][2]['sender_uid'] == '42'
    assert 'sender_uid' not in item
