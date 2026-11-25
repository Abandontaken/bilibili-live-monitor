import json
import subprocess
import sys

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

    def upload(_, tx):
        remote['index.html'] = tx['files']['index.html'].encode()
        raise OSError('interrupted')

    monkeypatch.setattr(backfill, '_publish', upload)
    with pytest.raises(OSError):
        backfill._apply(config, {}, pending, transaction)
    assert pending.is_file() and (public / 'index.html').read_bytes() == b'old'
    remote['snapshot.json'] = b'newer operator change'
    with pytest.raises(ValueError, match='远端页面'):
        backfill._apply(config, {}, pending, transaction)
    remote['snapshot.json'] = b'old'
    monkeypatch.setattr(backfill, '_publish', lambda _, tx: remote.update({k: v.encode() for k, v in tx['files'].items()}))
    monkeypatch.setattr(backfill, 'verify_remote', lambda _: None)
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
    monkeypatch.setattr(backfill, '_publish', lambda *_: pytest.fail('must not publish'))
    with pytest.raises(ValueError, match='本地校验'):
        backfill._apply(config, {}, tmp_path / 'identity_remote_pending.json', transaction)
    assert all((public / name).read_bytes() == b'old' for name in backfill.FILES)


def test_origin_installer_refuses_changed_peer_before_replacing_either_file(tmp_path):
    tmp_path = tmp_path.resolve()
    (tmp_path / 'index.html').write_bytes(b'old')
    (tmp_path / 'snapshot.json').write_bytes(b'changed')
    tx = {'before': {name: backfill._hash(b'old') for name in backfill.FILES},
          'files': {name: 'new' for name in backfill.FILES}}
    args = [sys.executable, '-c', backfill._REMOTE_INSTALL, str(tmp_path)]
    payload = json.dumps(tx).encode()
    assert subprocess.run(args, input=payload, capture_output=True, check=False).returncode != 0
    assert (tmp_path / 'index.html').read_bytes() == b'old'
    (tmp_path / 'index.html').write_bytes(b'new')
    (tmp_path / 'snapshot.json').write_bytes(b'old')
    subprocess.run(args, input=payload, capture_output=True, check=True)
    assert all((tmp_path / name).read_bytes() == b'new' for name in backfill.FILES)


def test_origin_installer_refuses_ancestor_alias(tmp_path):
    real = tmp_path.resolve() / 'real'
    real.mkdir()
    directory = real / 'page'
    directory.mkdir()
    for name in backfill.FILES:
        (directory / name).write_bytes(b'old')
    alias = tmp_path.resolve() / 'alias'
    alias.symlink_to(real)
    tx = {'before': {name: backfill._hash(b'old') for name in backfill.FILES},
          'files': {name: 'new' for name in backfill.FILES}}
    proc = subprocess.run([sys.executable, '-c', backfill._REMOTE_INSTALL, str(alias / 'page')],
                          input=json.dumps(tx).encode(), capture_output=True, check=False)
    assert proc.returncode != 0 and b'noncanonical origin' in proc.stderr
    assert all((directory / name).read_bytes() == b'old' for name in backfill.FILES)


def test_html_only_changes_activity_user_cell():
    page = ('<nav><a href="/existing">back</a></nav><section id="activity-summary">'
            '<table><tbody><tr><td>08:00:00–08:01:00</td><td><a href="/song">song</a></td><td>0</td>'
            '<td>3</td><td>0+</td><td>10</td><td></td></tr></tbody></table></section>')
    before = {'gift_summary': [], 'activity_summary': [{'gift_users': 0, 'gift_users_unknown_events': 3,
              'kind': 'song', 'title': 'song', 'start': '2026-01-01T00:00:00+00:00',
              'end': '2026-01-01T00:01:00+00:00'}]}
    assert backfill._update_html(page, before, [], [{'gift_users': 1}]) == page.replace('<td>0+</td>', '<td>1</td>')
    with pytest.raises(ValueError, match='区间身份'):
        backfill._update_html(page.replace('>song<', '>other<'), before, [], [{'gift_users': 1}])
