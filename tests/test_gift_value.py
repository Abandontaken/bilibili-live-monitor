from copy import deepcopy

import pytest

from bilibili_live_monitor.activity_summary import gift_observations, summarize_gifters, summarize_session
from bilibili_live_monitor.gift_value import gift_text, gift_value, value_fields


def batch():
    rows = []
    for index, (name, price, count) in enumerate((('合成稀有礼物', 2233000, 1), ('合成抱枕', 16000, 3), ('合成糖', 9000, 6))):
        gift = {'event_type': 'SEND_GIFT_V2', 'gift_name': name, 'price': price, 'num': count,
                'total_coin': 15000 * count, 'coin_type': 'gold', 'sender_uid': '123',
                'event_id': 'synthetic-order', 'batch_index': index,
                'blind_gift': {'original_gift_price': 15000}}
        rows.append(('gift', '2026-01-01T12:00:00Z', {'author': '合成观众', 'gift': gift}))
    return {'items': rows + rows, 'raw': [], 'segments': []}


def test_mixed_batch_values_quantity_dedup_and_gifter_totals_agree():
    source = batch()
    before = deepcopy(source)
    assert [r[2] for r in gift_observations(source)] == [2233000, 48000, 54000]
    assert summarize_session(source)['gift_amount_yuan'] == 2335
    assert summarize_session(source)['gift_count'] == 10
    assert summarize_gifters(source)[0]['amount_yuan'] == 2335
    result = value_fields(summarize_session(source))
    assert result['gift_value_yuan'] == 2335 and result['gift_value_basis'] == 'gift-output-v1'
    assert source == before
    text = gift_text(source['items'][1][2]['gift'], '旧投入文本')
    assert '¥48.00' in text and '45.00' not in text and '旧投入' not in text


@pytest.mark.parametrize('patch', [{'price': None}, {'price': True}, {'price': -1}, {'num': None}, {'num': 0}])
def test_blind_unknown_result_never_falls_back_to_cost(patch):
    gift = {**batch()['items'][0][2]['gift'], **patch}
    assert gift_value(gift) is None


def test_zero_silver_guard_and_sc_units():
    gift = batch()['items'][0][2]['gift']
    assert gift_value({**gift, 'price': 0}) == 0
    assert gift_value({**gift, 'coin_type': 'silver'}) == 0
    assert gift_value({'event_type': 'GUARD_BUY', 'num': 3, 'guard_price_gold': 198000}) == 198000
    assert gift_value({'event_type': 'SUPER_CHAT_MESSAGE', 'price_yuan': 30}) == 30000


def test_backfill_preserves_content_identity_time_and_original_data():
    from bilibili_live_monitor.gift_value_backfill import refresh_snapshot
    snapshot = {'session_start': '2026-01-01T11:00:00Z', 'updated_at': '2026-01-01T13:00:00Z',
                'phase': 'completed', 'activity': [{'song': '合成歌曲'}], 'series': {'fans': [[1, 2]]},
                'session_summary': {'followers_change': 3, 'gift_amount_yuan': 150},
                'activity_summary': [{'start': '2026-01-01T11:00:00Z', 'end': '2026-01-01T13:00:00Z',
                                      'gift_amount_yuan': 150, 'followers_change': 3}]}
    source = batch()
    for _, _, item in source['items']:
        item['gift']['event_at_utc'] = '2026-01-01T12:00:00Z'
    before = deepcopy(snapshot)
    result = refresh_snapshot(snapshot, source)
    assert result['session_summary']['gift_value_yuan'] == 2335
    assert result['activity_summary'][0]['gift_value_yuan'] == 2335
    assert result['gift_summary'][0]['amount_yuan'] == 2335
    for key in ('phase', 'session_start', 'updated_at', 'activity', 'series'):
        assert result[key] == snapshot[key]
    assert snapshot == before
    assert refresh_snapshot(result, source) == result


def test_empty_blind_metadata_is_still_blind_and_bad_quantity_sanitizes_old_text():
    gift = {'event_type': 'SEND_GIFT', 'gift_name': '合成盲盒结果', 'num': 1,
            'coin_type': 'gold', 'blind_gift': {}, 'total_coin': 45000}
    assert gift_value(gift) is None
    assert '未知' in gift_text({**gift, 'num': None}, '盲盒花费 450 电池')
    assert '450' not in gift_text({**gift, 'num': None}, '盲盒花费 450 电池')
    from bilibili_live_monitor.danmaku import gift_message
    item = gift_message({'cmd': 'SEND_GIFT', 'data': {'giftId': 1, 'giftName': '合成结果',
                         'num': 1, 'blind_gift': {}, 'price': None, 'total_coin': 45000, 'coin_type': 'gold'}})
    assert item['gift']['blind_gift'] == {}
    assert gift_value(item['gift']) is None and '45.00' not in item['text']


def test_legacy_gifter_amount_is_not_relabelled():
    from bilibili_live_monitor.activity_summary import render_gifters
    page = render_gifters([{'name': '合成观众', 'gift_count': 1, 'gift_count_unknown': 0,
                           'amount_yuan': 15, 'amount_unknown': 0}])
    assert '15.00' not in page and '<td>—</td>' in page


def test_snapshot_cutoff_gift_matches_reconciliation():
    from bilibili_live_monitor.gift_value_backfill import refresh_snapshot
    source = batch()
    snapshot = {'session_start': '2026-01-01T11:00:00Z', 'updated_at': '2026-01-01T12:00:00Z',
                'session_summary': {'gift_count': 10}, 'activity_reconciliation': {'total_amount_yuan': 150},
                'activity_summary': [{'start': '2026-01-01T11:00:00Z', 'end': '2026-01-01T12:00:00Z'}]}
    for _, _, item in source['items']:
        item['gift']['event_at_utc'] = '2026-01-01T12:00:00Z'
    result = refresh_snapshot(snapshot, source)
    assert result['activity_summary'][0]['gift_value_yuan'] == 2335
    assert result['activity_reconciliation']['covered_amount_yuan'] == 2335
    assert result['activity_reconciliation']['unclassified_amount_yuan'] == 0


def test_display_upgrade_preserves_legacy_fallback_source_key():
    import hashlib
    import json

    from bilibili_live_monitor.danmaku import gift_message
    packet = {'cmd': 'SEND_GIFT', 'data': {'giftId': 1, 'giftName': '合成结果', 'num': 2,
              'price': 40000, 'total_coin': 30000, 'coin_type': 'gold', 'timestamp': 1792700000,
              'blind_gift': {'original_gift_price': 15000}, 'uname': '合成观众'}}
    message = gift_message(packet)
    legacy_text = '盲盒结果：合成结果 ×2 · 结果标价 30000 平台金币 · 原始盲盒价格原值 15000（未换算）'
    legacy_fingerprint = json.dumps({'at_utc': message['gift']['event_at_utc'], 'author': message['author'],
                                    'gift': message['gift'], 'text': legacy_text}, ensure_ascii=False, sort_keys=True)
    expected = 'SEND_GIFT:' + hashlib.sha256(legacy_fingerprint.encode()).hexdigest()
    assert message['source_key'] == expected
    old = {**message, 'text': legacy_text}
    source = {'items': [('gift', message['at_utc'], old), ('gift', message['at_utc'], message)]}
    assert summarize_session({**source, 'raw': []})['gift_amount_yuan'] == 80


def test_crm_revalues_frozen_history_by_event_identity_without_mutating_it(tmp_path):
    import json
    import sqlite3

    from bilibili_live_monitor.audience_crm import digest
    from bilibili_live_monitor.gift_value_backfill import audience_value_map, refresh_archive_values
    gift = batch()['items'][0][2]['gift']
    at = '2026-01-01T12:00:00+00:00'
    item = {'sender_uid': '123', 'gift': gift}
    path = tmp_path / 'monitor.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE timeline(id INTEGER,room_id TEXT,kind TEXT,at_utc TEXT,data TEXT)')
        db.execute('INSERT INTO timeline VALUES(1,?,?,?,?)', ('456', 'gift', at, json.dumps(item)))
    material = {'sessions': [{'id': 'one', 'start': '2026-01-01T11:00:00Z', 'end': '2026-01-01T13:00:00Z'}]}
    before = deepcopy(material)
    config = {'database': str(path), 'room_id': '456', 'exclude_account_uids': ['123']}
    values = audience_value_map(material, config)
    event = digest(['456', 'one', ('gift', '123', 'SEND_GIFT', 'synthetic-order', 0)])[:32]
    assert values[event] == 2233000 and material == before
    rows = [{'start_utc': material['sessions'][0]['start'], 'updated_at': material['sessions'][0]['end'],
             'metrics': {'amount': 15, 'online_mean': 10}}]
    refreshed = refresh_archive_values(rows, config)
    assert refreshed[0]['metrics'] == {'amount': 2233, 'amount_nonself': 0, 'paid_donors_nonself': 0, 'online_mean': 10}
    assert rows[0]['metrics']['amount'] == 15
    no_records = refresh_archive_values([{**rows[0], 'start_utc': '2026-01-02T11:00:00Z',
                                         'updated_at': '2026-01-02T13:00:00Z', 'metrics': {'amount': 0}}], config)
    assert no_records[0]['metrics']['amount'] is None


def test_same_crm_material_can_publish_new_immutable_render_version(tmp_path, monkeypatch):
    from bilibili_live_monitor import audience_pages
    content = ['old']
    def fake_build(config):
        from pathlib import Path
        (Path(config['output_dir']) / 'index.html').write_text(content[0])
        return {'plan_id': 'same-material', 'files': ['index.html']}
    monkeypatch.setattr(audience_pages, '_build', fake_build)
    config = {'output_dir': str(tmp_path / 'public')}
    first = audience_pages.build(config)
    content[0] = 'new-output-value'
    second = audience_pages.build(config)
    assert first['plan_id'] == second['plan_id'] and first['directory'] != second['directory']
    assert audience_pages.build(config)['directory'] == second['directory']
    assert (tmp_path / 'public/index.html').read_text() == 'new-output-value'
