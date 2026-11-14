"""显式保留原片，不影响其他已授权周期清理；无需真实删除媒体。"""
from pathlib import Path

import pytest

from bilibili_live_monitor import archive_daily as daily
from bilibili_live_monitor import recording_archive as archive


@pytest.mark.parametrize('permanent', [False, True])
@pytest.mark.parametrize('kind', ['file', 'directory', 'broken-symlink'])
def test_retention_blocks_all_cleanup_before_media_calls(tmp_path, monkeypatch, permanent, kind):
    source = tmp_path / 'capture.mkv'
    source.write_bytes(b'original')
    marker = Path(str(source) + '.archive-retain')
    if kind == 'directory':
        marker.mkdir()
    elif kind == 'broken-symlink':
        marker.symlink_to(tmp_path / 'absent')
    else:
        marker.write_text('not valid JSON: still retained')
    monkeypatch.setattr(archive, 'check_source', lambda *a: pytest.fail('media check must not run'))
    monkeypatch.setattr(archive, 'command', lambda *a, **k: pytest.fail('must not invoke trash'))
    with pytest.raises(ValueError, match='显式保留'):
        archive.cleanup({'source': str(source)}, permanent=permanent)
    assert source.read_bytes() == b'original'


def test_natural_periodic_does_not_reschedule_retained_archived_original(tmp_path, monkeypatch):
    source = tmp_path / 'capture.mkv'
    source.write_bytes(b'original')
    Path(str(source) + '.archive-retain').write_text('retain')
    monkeypatch.setattr(daily, 'plan', lambda roots: [{'source': str(source), 'action': 'archived'}])
    plan = daily.run([tmp_path], trash_originals=True, workers=5)
    assert plan['planned'] == 0


def test_process_new_archive_preserves_marked_source_even_with_trash_flag(tmp_path, monkeypatch):
    source = tmp_path / 'capture.mkv'
    source.write_bytes(b'original-media')
    Path(str(source) + '.archive-retain').write_text('retain')
    target = tmp_path / 'archive.mp4'
    target.write_bytes(b'mp4')
    item = {'source': str(source), 'action': 'encode', 'source_stat': {'size': 14}, 'legacy_row': {}}
    monkeypatch.setattr(archive, 'encode', lambda *a: {'status': 'archived'})
    monkeypatch.setattr(daily, 'archived_input', lambda row: target)
    monkeypatch.setattr(archive, 'cleanup', lambda *a, **k: pytest.fail('marked source must not be cleaned'))
    result = daily.process(item, (), trash_originals=True)
    assert result['status'] == 'archived' and result['original_retained']
    assert source.is_file()


def test_unmarked_archived_original_keeps_existing_cleanup_plan(tmp_path, monkeypatch):
    source = tmp_path / 'capture.mkv'
    source.write_bytes(b'original')
    item = {'source': str(source), 'action': 'archived', 'source_stat': {'size': 8}}
    monkeypatch.setattr(daily, 'plan', lambda roots: [item])
    plan = daily.run([tmp_path], trash_originals=True, workers=5)
    assert plan['planned'] == 1


def test_hold_added_during_validation_blocks_final_cleanup(tmp_path, monkeypatch):
    source = tmp_path / 'capture.mkv'
    source.write_bytes(b'original')
    stat = archive.file_stat(source)
    monkeypatch.setattr(archive, 'check_source', lambda *a: None)
    monkeypatch.setattr(archive, 'archive_receipt', lambda *a: {'source_stat': stat, 'source_info': {'timestamp_max_adjustment_us': 0}})
    monkeypatch.setattr(archive, 'verify_output', lambda *a: Path(str(source) + '.archive-retain').write_text('retain'))
    monkeypatch.setattr(archive, 'command', lambda *a, **k: pytest.fail('late hold must prevent Trash'))
    with pytest.raises(ValueError, match='显式保留'):
        archive.cleanup({'source': str(source)})
    assert source.is_file()
