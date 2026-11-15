import json
import sqlite3

import pytest

from bilibili_live_monitor import pk_bar


def test_stage_deadlines_and_connection():
    origin = pk_bar.epoch('2030-01-01T00:00:00Z')
    base = {'members': [], 'start': origin+10, 'end': origin+310,
            'punish_end': origin+373, 'sprint': 10, 'sub_type': 50}
    plan = {'video_start': '2030-01-01T00:00:00Z', 'room': '123', 'connection': [0, 500],
            'snapshots': [{**base, 'at': 1, 'status': 101}, {**base, 'at': 10, 'status': 201},
                          {**base, 'at': 313, 'status': 601}, {**base, 'at': 375, 'status': 1001}]}
    assert pk_bar.display_state(plan, 0)['stage'] == 'connected'
    assert pk_bar.display_state(plan, 5)['remaining'] == 5
    assert pk_bar.display_state(plan, 12)['hide_clock']
    assert not pk_bar.display_state(plan, 26)['hide_clock']
    assert pk_bar.display_state(plan, 301)['stage'] == 'sprint'
    assert pk_bar.display_state(plan, 310)['stage'] == 'settling'
    assert pk_bar.display_state(plan, 315)['remaining'] == 58
    assert pk_bar.display_state(plan, 376)['stage'] == 'connected'
    assert pk_bar.display_state(plan, 501)['stage'] == 'hidden'


def test_zero_and_extreme_scores_are_readable():
    assert pk_bar.bar_ratio(0, 0) == .5
    assert .16 <= pk_bar.bar_ratio(1, 1000000) < .5
    assert .5 < pk_bar.bar_ratio(1000000, 1) <= .84
    assert pk_bar.bar_ratio(1000, 1000) == .5


def test_final_disabled_expired_and_result():
    origin = pk_bar.epoch('2030-01-01T00:00:00Z')
    state = {'at': 0, 'status': 301, 'members': [], 'final_end': None}
    plan = {'video_start': '2030-01-01T00:00:00Z', 'room': '123',
            'connection': [0, 100], 'snapshots': [state]}
    assert pk_bar.display_state(plan, 1)['stage'] == 'settling'
    state['final_end'] = origin+10
    assert pk_bar.display_state(plan, 1)['stage'] == 'final'
    assert pk_bar.display_state(plan, 10)['stage'] == 'settling'
    state['status'] = 304
    assert pk_bar.display_state(plan, 1)['stage'] == 'result'
    assert pk_bar.display_state(plan, 20)['remaining'] is None


def test_snapshot_ownership_and_no_future_score(tmp_path, monkeypatch):
    db = tmp_path/'source.sqlite3'
    video = tmp_path/'source.mp4'
    font = tmp_path/'font.ttf'
    video.write_bytes(b'video')
    font.write_bytes(b'font')
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE raw_events(id INTEGER,room_id TEXT,received_utc TEXT,command TEXT,data TEXT)')
        basic = {'pk_id': 77, 'status': 201, 'start_time': 1893456000,
                 'end_time': 1893456300, 'sub_type': 50}
        packet = {'data': {'pk_basic': basic, 'members': [
            {'room_id': 123, 'votes': 1}, {'room_id': 456, 'votes': 2}]}}
        conn.execute('INSERT INTO raw_events VALUES(1,?,?,?,?)',
                     ('123', '2030-01-01T00:00:00+00:00', 'PK_INFO', json.dumps(packet)))
        update = {'pk_id': 77, 'data': {'init_info': {'room_id': 456, 'votes': 200},
                                      'match_info': {'room_id': 123, 'votes': 100}}}
        conn.execute('INSERT INTO raw_events VALUES(2,?,?,?,?)',
                     ('123', '2030-01-01T00:00:05+00:00', 'PK_BATTLE_PROCESS_NEW', json.dumps(update)))
    monkeypatch.setattr(pk_bar, 'media_info', lambda _: {'format': {'duration': '20'}, 'streams': [
        {'codec_type': 'video', 'width': 1600, 'height': 1280}, {'codec_type': 'audio'}]})
    plan = pk_bar.make_plan(db, '123', '77', video, '2030-01-01T00:00:00Z', font)
    assert plan['snapshots'][0]['members'][0]['votes'] == 1
    assert pk_bar.display_state(plan, 4.9)['left']['votes'] == 1
    assert pk_bar.display_state(plan, 5)['left']['votes'] == 100
    assert pk_bar.display_state(plan, 5)['right']['votes'] == 200
    with pytest.raises(ValueError, match='找不到'):
        pk_bar.make_plan(db, '123', '78', video, '2030-01-01T00:00:00Z', font)
