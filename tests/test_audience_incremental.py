import copy
import json
from pathlib import Path

import pytest

from bilibili_live_monitor.audience_crm import collect, finish, prepare


def scene(sid, day, items=(), text='今天聊一个无关的话题'):
    at = f'2026-01-{day:02d}T00:01:00+00:00'
    return {'id': sid, 'room_id': '10001', 'label': sid, 'url': 'https://example.com/session/',
            'start': f'2026-01-{day:02d}T00:00:00+00:00',
            'end': f'2026-01-{day:02d}T01:00:00+00:00',
            'source': {'raw': [], 'segments': [{'seq': day, 'is_final': True, 'text': text,
                       'start_utc': at, 'end_utc': at}],
                       'items': [(kind, at, data) for kind, data in items]}}


def event(kind, text='想听《示例曲》'):
    data = {'sender_uid': '101', 'author': '独特昵称', 'text': text}
    if kind == 'gift':
        data['gift'] = {'sender_uid': '101', 'event_id': 'new-gift', 'gift_name': '礼物', 'num': 1}
    return kind, data


def baseline(tmp_path, agentic=True):
    cfg = {'room_id': '10001', 'scope': 'example', 'label': '示例主播', 'secret': 'x' * 48,
           'workspace': str(tmp_path / 'private'), 'agentic_sources': agentic}
    one = scene('one', 1, [event('danmaku')])
    material = collect(cfg, [one])
    directory, plan = prepare(cfg, material)
    person = next(iter(material['people'].values()))
    own = next(e for e in person['evidence'] if e['speaker'] == 'viewer')
    state = {'person_id': person['id'], 'revision': person['revision'],
             'overview': [{'text': '曾点示例曲', 'refs': [{'id': own['id'], 'quote': own['text']}]}],
             'preferences': [], 'attention': [], 'promises': [], 'actions': [], 'interactions': []}
    Path(plan['tasks'][0]['result']).write_text(json.dumps(state))
    finish(cfg, directory)
    return cfg, one, state


@pytest.mark.parametrize('agentic', [True, False])
@pytest.mark.parametrize('kind', ['entry', 'like', None])
def test_irrelevant_new_session_preserves_history_and_finishes(tmp_path, kind, agentic):
    cfg, one, old = baseline(tmp_path, agentic)
    two = scene('two', 2, [event(kind)] if kind else [])
    material = collect(cfg, [one, two])
    directory, plan = prepare(cfg, material)
    task = plan['tasks'][0]
    assert task['reused'] and task['reuse_reason'] in ('no_relevant_interaction', 'unchanged_revision')
    assert plan['selection_summary']['updates'] == 0
    retained = json.loads(Path(task['result']).read_text())
    assert {k: v for k, v in retained.items() if k != 'revision'} == {
        k: v for k, v in old.items() if k != 'revision'}
    finish(cfg, directory)


@pytest.mark.parametrize('kind', ['danmaku', 'gift'])
def test_current_interaction_needs_analysis(tmp_path, kind):
    cfg, one, _ = baseline(tmp_path)
    directory, plan = prepare(cfg, collect(cfg, [one, scene('two', 2, [event(kind)])]))
    assert not plan['tasks'][0]['reused']
    assert 'viewer_interaction' in plan['tasks'][0]['update_reasons']
    with pytest.raises(FileNotFoundError):
        finish(cfg, directory)


def test_named_absent_viewer_and_pending_song_recalled(tmp_path):
    cfg, one, _ = baseline(tmp_path)
    _, plan = prepare(cfg, collect(cfg, [one, scene('two', 2, text='独特昵称，今天给你补唱。')]))
    assert not plan['tasks'][0]['reused']
    assert 'named_candidate' in plan['tasks'][0]['update_reasons']


def test_changed_unrelated_sources_preserve_accepted_evidence_snapshot(tmp_path):
    cfg, one, _ = baseline(tmp_path)
    corrected = copy.deepcopy(one)
    corrected['source']['segments'][0]['text'] = '旧转写校正，但与这位观众无关'
    material = collect(cfg, [corrected, scene('two', 2)])
    _, plan = prepare(cfg, material)
    assert plan['tasks'][0]['reused']
    corrected['source']['items'][0][2]['text'] = '原引用已经不存在'
    material = collect(cfg, [corrected, scene('two', 2)])
    directory, plan = prepare(cfg, material)
    assert plan['tasks'][0]['reused']
    assert any(e.get('retained_from_plan') for p in material['people'].values() for e in p['evidence'])
    finish(cfg, directory)


def test_untracked_historical_viewer_is_not_backfilled_without_new_interaction(tmp_path):
    cfg, one, _ = baseline(tmp_path)
    earlier = scene('earlier', 3, [('danmaku', {'sender_uid': '202', 'author': '另一个昵称', 'text': '历史互动'})])
    material = collect({**cfg, '_update_sessions': ['two']}, [one, scene('two', 2), earlier])
    directory, plan = prepare(cfg, material)
    assert len(plan['tasks']) == 1
    assert plan['selection_summary']['not_analyzed'] == 1
    finish(cfg, directory)


def test_pending_song_followup_without_nickname_is_candidate(tmp_path):
    cfg, one, _ = baseline(tmp_path)
    pointer = Path(cfg['workspace']) / 'current.json'
    previous = json.loads(pointer.read_text())
    pid = next(iter(previous['states']))
    previous['states'][pid]['promises'] = []
    # Candidate retrieval itself does not assert fulfillment or manufacture a state.
    from bilibili_live_monitor.audience_crm import update_reasons
    person = collect(cfg, [one])['people'][pid]
    material = collect(cfg, [one, scene('two', 2, text='答应的《示例曲》学会了，今天补上。')])
    reasons = update_reasons(cfg, person, material, {'two'}, {
        'promises': [{'status': 'open', 'text': '补唱《示例曲》'}]})
    assert reasons == ['promise_candidate']


def test_explicit_current_scope_does_not_treat_all_history_as_new(tmp_path):
    cfg, one, _ = baseline(tmp_path)
    two = scene('two', 2, [event('entry')])
    material = collect(cfg, [one, two])
    material['update_sessions'] = ['two']
    _, plan = prepare(cfg, material)
    assert plan['tasks'][0]['reused']


def test_retained_result_cannot_silently_drop_history(tmp_path):
    cfg, one, _ = baseline(tmp_path)
    directory, plan = prepare(cfg, collect(cfg, [one, scene('two', 2)]))
    assert plan['tasks'][0]['reused']
    result = Path(plan['tasks'][0]['result'])
    data = json.loads(result.read_text())
    # Existing validator validates citation structure, but reused files must also
    # retain the prior accepted content at finish, not merely be valid empty JSON.
    data['overview'] = []
    result.write_text(json.dumps(data))
    with pytest.raises(ValueError, match='复用'):
        finish(cfg, directory)


def test_unaccepted_legacy_plan_cannot_finish(tmp_path):
    cfg, one, _ = baseline(tmp_path)
    material = collect(cfg, [one, scene('two', 2, [event('danmaku')])])
    material.pop('update_policy')
    directory, _ = prepare(cfg, material)
    with pytest.raises(ValueError, match='重新准备'):
        finish(cfg, directory)
