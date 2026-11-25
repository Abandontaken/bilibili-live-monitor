import json

import pytest

from bilibili_live_monitor import warehouse_remote_backfill as backfill


def test_partial_upload_retries_without_overwriting_unrelated_remote(tmp_path, monkeypatch):
    cfg_path = tmp_path / 'config.json'
    cfg_path.write_text('{}')
    public = tmp_path / 'public'
    public.mkdir()
    config = {'remote': 'example:/published/', 'url': 'https://example.org/published/'}
    remote = {}
    for name in backfill.FILES:
        remote[name] = b'old'
        (public / name).write_bytes(b'old')
    transaction = {'target': list(config.values()), 'config_hash': backfill._hash(b'{}'),
                   'before': {name: backfill._hash(b'old') for name in backfill.FILES},
                   'files': {name: 'new' for name in backfill.FILES}}
    pending = tmp_path / 'identity_remote_pending.json'
    backfill._write_pending(pending, transaction)
    monkeypatch.setattr(backfill, 'read_remote_artifact', lambda _, name: remote[name])
    monkeypatch.setattr(backfill, 'verify_snapshot', lambda *a, **kw: {'valid': True, 'unpublished_songs': []})

    def upload(_, staging):
        remote['index.html'] = (staging / 'index.html').read_bytes()
        raise OSError('interrupted')

    monkeypatch.setattr(backfill, 'publish', upload)
    with pytest.raises(OSError):
        backfill._apply(config, {}, pending, transaction)
    assert pending.is_file() and (public / 'index.html').read_bytes() == b'old'
    remote['snapshot.json'] = b'newer operator change'
    with pytest.raises(ValueError, match='远端页面'):
        backfill._apply(config, {}, pending, transaction)
    remote['snapshot.json'] = b'new'  # Transport actually finished; retry can commit without another upload.
    backfill._apply(config, {}, pending, transaction)
    assert not pending.exists()
    assert all((public / name).read_bytes() == b'new' for name in backfill.FILES)
    assert json.loads((tmp_path / 'workflow.json').read_text())['final_artifacts']['public/index.html']


def test_invalid_candidate_never_reaches_publisher(tmp_path, monkeypatch):
    (tmp_path / 'config.json').write_text('{}')
    public = tmp_path / 'public'
    public.mkdir()
    for name in backfill.FILES:
        (public / name).write_bytes(b'old')
    config = {'remote': 'example:/published/', 'url': 'https://example.org/published/'}
    transaction = {'target': list(config.values()), 'config_hash': backfill._hash(b'{}'),
                   'before': {name: backfill._hash(b'old') for name in backfill.FILES},
                   'files': {name: 'unverified' for name in backfill.FILES}}
    monkeypatch.setattr(backfill, 'read_remote_artifact', lambda *_: b'old')
    monkeypatch.setattr(backfill, 'verify_snapshot', lambda *a, **kw: {'valid': False})
    monkeypatch.setattr(backfill, 'publish', lambda *_: pytest.fail('must not publish'))
    with pytest.raises(ValueError, match='本地校验'):
        backfill._apply(config, {}, tmp_path / 'identity_remote_pending.json', transaction)
    assert all((public / name).read_bytes() == b'old' for name in backfill.FILES)
