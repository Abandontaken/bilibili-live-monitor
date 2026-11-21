import argparse
import hashlib
import json
import sqlite3
from contextlib import closing

import pytest

from bilibili_live_monitor.audience_log import collect, configure_parser, main

ROOM = '123456'
UID = '9007199254740993'
TEXT = '原文 </script> & 原样保留'


def session(start, end, status='completed'):
    return dict(start_utc=f'2030-01-01T{start}Z', updated_at=f'2030-01-01T{end}Z', status=status)


@pytest.fixture
def sources(tmp_path):
    db = tmp_path / 'monitor.sqlite3'
    archive = tmp_path / 'sessions.json'
    archive.write_text(json.dumps({'room_id': ROOM, 'sessions': [
        session('04:00:00', '05:00:00', 'live'), session('02:00:00', '03:00:00'),
        session('00:00:00', '01:00:00'),
    ]}))
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute('CREATE TABLE timeline (id INTEGER PRIMARY KEY, room_id, at_utc, kind, data)')
        rows = [
            ('00:04:00', UID, '旧名', 'danmaku', TEXT, ROOM, 'segment-a'),
            ('00:00:00', UID, '旧名', 'danmaku', '开场', ROOM, 'segment-a'),
            ('01:00:00', UID, '新名', 'danmaku', '收场', ROOM, 'segment-b'),
            ('02:00:00', UID, '新名', 'entry', None, ROOM, 'segment-c'),
            ('00:10:00', '888', '新名', 'danmaku', '同名不同人', ROOM, 'segment-a'),
            ('00:15:00', '889', '潜水观众', 'entry', None, ROOM, 'segment-a'),
            ('00:20:00', None, '未知观众', 'danmaku', '无 UID', ROOM, 'segment-a'),
            ('01:01:00', UID, '新名', 'danmaku', '场外', ROOM, 'segment-b'),
            ('04:01:00', UID, '新名', 'danmaku', '未完场', ROOM, 'segment-d'),
            ('00:40:00', UID, '另一房昵称', 'danmaku', '另一房', '654321', 'segment-e'),
        ]
        conn.executemany('INSERT INTO timeline (room_id, at_utc, kind, data) VALUES (?,?,?,?)', [
            (room, f'2030-01-01T{time}Z', kind,
             json.dumps(dict(sender_uid=uid, author=name, text=text, session_id=segment)))
            for time, uid, name, kind, text, room, segment in rows
        ])
    return db, archive


def run_cli(*argv):
    parser = argparse.ArgumentParser()
    configure_parser(parser.add_subparsers(dest='command', required=True))
    return main(parser.parse_args(['audience-log', *map(str, argv)]))


def test_export_uses_archive_boundaries_and_observed_uids(sources, tmp_path, capsys):
    db, archive = sources
    before = hashlib.sha256(db.read_bytes()).digest()
    json_out, html = tmp_path / 'log.json', tmp_path / 'index.html'
    assert run_cli('--database', db, '--sessions', archive, '--room-id', ROOM,
                   '--json-out', json_out, '--html-out', html) == 0
    data = json.loads(json_out.read_text())
    assert data['schema_version'] == 1
    assert len(data['sessions']) == 2
    assert [p['uid'] for p in data['people']] == [UID, '888']
    person = data['people'][0]
    assert person['names'] == ['新名', '旧名']
    assert person['sessions'][0] == {'session': 1, 'messages': []}
    messages = person['sessions'][1]['messages']
    assert [m['text'] for m in messages] == ['开场', TEXT, '收场']
    assert messages[-1]['at'] == '2030-01-01T09:00:00+08:00'
    assert data['stats'] == {
        'message_count': 4, 'unattributed_message_count': 1, 'outside_session_message_count': 2,
    }
    assert json.loads(capsys.readouterr().out)['people'] == 2
    embedded = html.read_text().split('id="payload">', 1)[1].split('</script>', 1)[0]
    assert TEXT not in embedded and json.loads(embedded) == data
    assert hashlib.sha256(db.read_bytes()).digest() == before


def test_refuses_input_overwrite(sources):
    db, archive = sources
    with pytest.raises(SystemExit) as error:
        run_cli('--database', db, '--sessions', archive, '--room-id', ROOM, '--html-out', db)
    assert error.value.code == 2


def test_rejects_overlapping_or_wrong_room_and_reports_empty(sources):
    db, _ = sources
    with pytest.raises(ValueError, match='重叠'):
        collect(db, {'sessions': [session('00:00:00', '01:00:00'), session('01:00:00', '02:00:00')]},
                ROOM, '示例')
    with pytest.raises(ValueError, match='不一致'):
        collect(db, {'room_id': '654321'}, ROOM, '示例')
    data = collect(db, {'sessions': [session('09:00:00', '10:00:00')]}, ROOM, '示例')
    assert data['people'] == []
    assert data['stats']['message_count'] == 0
