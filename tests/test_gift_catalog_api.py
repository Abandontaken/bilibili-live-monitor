import pytest

from bilibili_live_monitor.gift_assets import normalize_catalog, select_effect


def test_api_catalog_quantity_tier_and_ordinary_fallback():
    payload = {'data': {'list': [{'id': 123, 'effect_id': 7, 'count_map': [
        {'num': 1, 'effect_id': 0}, {'num': 10, 'effect_id': 8}]}]},
        'effects': [{'effectId': n, 'webMP4': f'https://example.test/{n}.mp4',
                     'webMP4Json': f'https://example.test/{n}.json'} for n in (7, 8)]}
    catalog = normalize_catalog(payload)
    assert select_effect(catalog, 123, 1)[1]['effectId'] == 7
    assert select_effect(catalog, 123, 10)[1]['effectId'] == 8
    assert 'effectId' not in payload['data']['list'][0]


def test_api_without_effect_resources_cannot_claim_renderable():
    catalog = normalize_catalog({'data': {'list': [{'id': 123, 'effect_id': 7}]}})
    with pytest.raises(ValueError, match='没有可唯一定位'):
        select_effect(catalog, 123, 1)


def test_conflicting_versions_are_not_silently_combined():
    catalog = normalize_catalog({'gifts': [{'id': 123, 'effectId': 7},
                                           {'id': 123, 'effectId': 8}], 'effects': []})
    with pytest.raises(ValueError, match='不能唯一定位礼物'):
        select_effect(catalog, 123, 1)
