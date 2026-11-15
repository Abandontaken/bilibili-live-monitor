import json

from bilibili_live_monitor.annotation_history import read_annotations


def test_batch_replacement_preserves_old_reviews_and_new_revision_wins(tmp_path):
    path = tmp_path / 'annotations.json'
    path.write_text(json.dumps({'reviews': [{'seq': 1, 'fingerprint': 'first', 'label': 'speaking'}],
                                'completed_followups': ['keep']}))
    assert len(read_annotations(tmp_path)['reviews']) == 1
    path.write_text(json.dumps({'reviews': [{'seq': 2, 'fingerprint': 'second', 'label': 'singing'}],
                                'completed_followups': ['latest']}))
    result = read_annotations(tmp_path)
    assert [row['seq'] for row in result['reviews']] == [1, 2]
    assert result['completed_followups'] == ['latest']
    assert len(json.loads(path.read_text())['reviews']) == 1
    path.write_text(json.dumps({'reviews': [{'seq': 1, 'fingerprint': 'corrected', 'label': 'singing'}]}))
    assert read_annotations(tmp_path)['reviews'][0]['fingerprint'] == 'corrected'


def test_missing_current_file_retains_history(tmp_path):
    (tmp_path / 'annotation_history.json').write_text(json.dumps({'reviews': [{'seq': 1, 'fingerprint': 'f'}]}))
    assert read_annotations(tmp_path)['reviews'] == [{'seq': 1, 'fingerprint': 'f'}]
