from datetime import datetime, timezone

import pytest

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


def test_duplicate_transaction_does_not_create_an_inferred_gift():
    gifts = _extract([
        _rank(1, 1, 100), _gift(2, 10, 40000, tid='repeat'),
        _gift(3, 11, 40000, tid='repeat'), _rank(4, 12, 500),
    ])
    assert len(gifts) == 1
    assert gifts[0]['inferred_uid'] is None
