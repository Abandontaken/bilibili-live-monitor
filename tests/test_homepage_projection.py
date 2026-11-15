import json

from bilibili_live_monitor.dashboard_gateway import presentation_aliases, room_snapshot


def test_frozen_identity_hides_handoff_and_counts_tasks_without_touching_inputs(tmp_path):
    folder = tmp_path / ('a' * 24)
    folder.mkdir()
    (folder / 'config.json').write_text(json.dumps({'room_id': '123456', 'platform_room_id': '654321'}))
    instance = {'id': 'guest', 'label': '匿名', 'session_root': str(tmp_path), 'hidden_rooms': ['222222'],
                'archive': {'rooms': {'123456': '合成主播'}}}
    instance['_room_aliases'] = presentation_aliases(instance)
    platform = {'policy': {'enabled': True, 'fixed_rooms': ['https://live.bilibili.com/654321',
                 'https://live.bilibili.com/222222']}, 'candidates': [
                 {'room_id': rid, 'url': 'https://live.bilibili.com/' + rid, 'live_status': 0}
                 for rid in ('654321', '222222')]}
    original = {'rooms': [{'room_id': '123456', 'status': 'stopped', 'error': 'old', 'audio_error': 'old'},
                          {'room_id': '654321', 'status': 'waiting'},
                          {'room_id': '222222', 'status': 'stopped'}]}
    result = room_snapshot(original, platform, {}, instance)
    assert result['fixed_task_count'] == 1 and result['raw_fixed_count'] == 2
    assert len(result['rooms']) == 1
    assert result['rooms'][0]['home_url'] == 'rooms/123456/'
    assert result['rooms'][0]['current_issue'] is False
    assert original['rooms'][0]['error'] == 'old'
    # 同房在另一个有效实例仍是独立任务。
    assert room_snapshot(original, platform, {}, {**instance, 'id': 'other'})['fixed_task_count'] == 1


def test_current_audio_problem_counts_once_but_idle_errors_do_not():
    platform = {'policy': {'enabled': True, 'fixed_rooms': ['https://live.bilibili.com/123456']},
                'candidates': [{'room_id': '123456', 'url': 'https://live.bilibili.com/123456', 'live_status': 1}]}
    state = {'rooms': [{'room_id': '123456', 'status': 'capturing', 'audio_error': 'current', 'error': 'old'}]}
    result = room_snapshot(state, platform, {}, {'id': 'one', 'label': '测试'})
    assert result['rooms'][0]['current_issue'] is True
    state['rooms'][0].update(status='stopped')
    assert room_snapshot(state, platform, {}, {'id': 'one', 'label': '测试'})['rooms'][0]['current_issue'] is False


def test_conflicting_identity_is_not_guessed(tmp_path):
    for index, capture in enumerate(('123456', '234567')):
        folder = tmp_path / str(index)
        folder.mkdir()
        (folder / 'config.json').write_text(json.dumps({'room_id': capture, 'platform_room_id': '654321'}))
    assert presentation_aliases({'session_root': str(tmp_path)}) == {}


def test_overview_reuses_calendar_period_and_cache_but_invalidates_at_midnight(tmp_path, monkeypatch):
    from datetime import datetime

    from fastapi.testclient import TestClient

    from bilibili_live_monitor import dashboard_gateway, room_archive

    calls = []
    period = {'days': 7, 'start': '2030-12-26', 'end': '2031-01-01', 'observed_days': 2,
              'entries_daily_mean': 0, 'gift_value_yuan': None, 'fans_club_change': -5}
    def aggregate(*args, **kwargs):
        calls.append(True)
        return {'periods': [period, {'days': 30}], 'sessions': [], 'days': [], 'summaries': []}
    monkeypatch.setattr(room_archive, 'archive_data', aggregate)
    config = {'instances': [{'id': 'one', 'label': '合成', 'url': 'http://127.0.0.1:8765',
              'session_root': str(tmp_path), 'archive': {'database': 'unused', 'rooms': {'123456': '合成'}}}]}
    with TestClient(dashboard_gateway.create_gateway(config)) as client:
        path = '/instances/one/api/rooms/123456/archive'
        assert client.get(path + '?overview=true').json()['periods'] == [period]
        assert client.get(path + '?overview=true').json()['periods'] == [period]
        assert len(calls) == 1
        assert client.get(path + '?overview=true&trend=true').status_code == 400
        class Tomorrow:
            @staticmethod
            def now(tz):
                return datetime(2099, 1, 1, tzinfo=tz)
        monkeypatch.setattr(dashboard_gateway, 'datetime', Tomorrow)
        assert client.get(path + '?overview=true').json()['periods'] == [period]
        assert len(calls) == 2
