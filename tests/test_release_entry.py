import importlib.util
import sys
from pathlib import Path

from bilibili_live_monitor import deployment


def test_stable_entry_uses_pinned_release(tmp_path, monkeypatch, capsys):
    path = Path(__file__).resolve().parents[1] / 'scripts/release_entry.py'
    spec = importlib.util.spec_from_file_location('release_entry', path)
    entry = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(entry)
    marker = tmp_path / 'pin.json'
    deployment.atomic(marker, {'component': 'sessions', 'binding': {'release': '/fixed/release'}})
    seen = []
    monkeypatch.setattr(deployment, 'run', lambda *a, **kw: seen.append((a, kw)) or {'executed': False})
    monkeypatch.setattr(sys, 'argv', [str(path), '--registry', str(tmp_path / 'missing-registry'),
                                    '--component', 'sessions', '--task', str(marker), '--dry-run',
                                    '--', '-m', 'synthetic'])
    previous = list(sys.path)
    try:
        entry.main()
        assert sys.path[0] == '/fixed/release/src'
        assert seen[0][1]['task'] == marker
        assert seen[0][0][2] == ['-m', 'synthetic']
        assert 'false' in capsys.readouterr().out
    finally:
        sys.path[:] = previous
