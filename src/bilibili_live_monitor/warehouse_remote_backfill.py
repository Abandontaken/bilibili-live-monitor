"""Opt-in identity backfill for already published single-session pages."""
import fcntl
import hashlib
import json
import os
import tempfile
from collections import Counter
from contextlib import ExitStack
from pathlib import Path

from .live_share import atomic_json
from .live_share_control import load_config, source_at_cutoff, verify_snapshot
from .live_share_watch import publish
from .session_runner import read_remote_artifact, verify_remote
from .session_snapshot import write_snapshot
from .warehouse_backfill import (
    _atomic_write_bytes,
    _compute_final_artifacts,
    _load_blind_box_assignments,
    _load_identity_dictionary,
    _recompute_summaries,
    assert_distinct_warehouse_path,
    overlay_session_gift_identities,
)

FILES = ('index.html', 'snapshot.json')


def _hash(data):
    return hashlib.sha256(data).hexdigest()


def _write_pending(path, data):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as temp:
        try:
            temp.write(json.dumps(data, ensure_ascii=False).encode())
            temp.flush()
            os.fsync(temp.fileno())
            os.replace(temp.name, path)
        finally:
            Path(temp.name).unlink(missing_ok=True)


def _apply(config, workflow, pending, transaction):
    workspace = pending.parent
    public = workspace / 'public'
    if transaction['target'] != [config['remote'], config['url']]:
        raise ValueError('发布地址已变化，不能自动续跑')
    if transaction['config_hash'] != _hash((workspace / 'config.json').read_bytes()):
        raise ValueError('配置已变化，不能自动续跑')
    remote_changed = False
    for name in FILES:
        old = transaction['before'][name]
        new = _hash(transaction['files'][name].encode())
        if _hash((public / name).read_bytes()) not in {old, new}:
            raise ValueError('页面在回填后另有修改，不能自动覆盖')
        remote = _hash(read_remote_artifact(config, name))
        if remote not in {old, new}:
            raise ValueError('远端页面已另有修改，不能自动覆盖')
        remote_changed |= remote != new
    with tempfile.TemporaryDirectory(dir=workspace) as directory:
        staged_workspace = Path(directory)
        staged = staged_workspace / 'public'
        staged.mkdir()
        for name in FILES:
            (staged / name).write_bytes(transaction['files'][name].encode())
        report = verify_snapshot(config, public_dir=staged)
        if not report['valid'] or report['unpublished_songs']:
            raise ValueError('候选页面未通过本地校验')
        if remote_changed:
            publish(config, staged)
            verify_remote({**config, 'workspace': str(staged_workspace)})
    for name in FILES:
        _atomic_write_bytes(public / name, transaction['files'][name].encode())
    workflow['final_artifacts'] = _compute_final_artifacts(workspace)
    atomic_json(workspace / 'workflow.json', workflow)
    pending.unlink()


def _session(workspace, warehouse, dictionary, execute):
    cfg_path = workspace / 'config.json'
    wf_path = workspace / 'workflow.json'
    if not cfg_path.is_file() or not wf_path.is_file():
        return 'skipped_missing_files'
    with ExitStack() as stack:
        if execute:
            try:
                for name in ('workflow.lock', 'publisher.lock'):
                    fd = os.open(workspace / name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
                    stack.callback(os.close, fd)
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 'skipped_locked'
        workflow = json.loads(wf_path.read_text())
        if workflow.get('stage') != 'completed':
            return 'skipped_not_completed'
        config = load_config(cfg_path)
        if config.get('local_only') is not False or not config.get('remote') or not config.get('url'):
            return 'skipped_not_remote'
        public = workspace / 'public'
        pending = workspace / 'identity_remote_pending.json'
        paths = [cfg_path, wf_path, pending, public, *(public / name for name in FILES)]
        if Path(config['workspace']).resolve() != workspace.resolve() or any(p.is_symlink() for p in paths):
            return 'skipped_workspace_mismatch'
        if not all((public / name).is_file() for name in FILES):
            return 'skipped_missing_files'
        if pending.exists():
            if not execute:
                return 'pending_publication'
            _apply(config, workflow, pending, json.loads(pending.read_text()))
            return 'published'
        assert_distinct_warehouse_path(warehouse, [Path(config['database'])])
        _, cutoff, source = source_at_cutoff(config)
        assignments = _load_blind_box_assignments(warehouse, config, cutoff)
        if assignments is None:
            return 'skipped_warehouse_window_mismatch'
        snapshot = json.loads((public / 'snapshot.json').read_text())
        resolved, _ = overlay_session_gift_identities(source, *dictionary(), assignments)
        previous = snapshot['gift_summary']
        # A remote page may use identities already visible in its own gift table.
        # Do not publish new cross-room names or stable hash fragments.
        allowed_names = {row['name'] for row in previous}
        resolved['items'] = [
            new if new[0] != 'gift' or new[2].get('author') in allowed_names else old
            for old, new in zip(source['items'], resolved['items'], strict=True)
        ]
        gifts, activity = _recompute_summaries(snapshot, resolved, cutoff)
        if {row['name'] for row in gifts} - allowed_names:
            return 'skipped_new_public_names'
        for key in ('gift_count', 'gift_count_unknown', 'value_yuan', 'amount_unknown'):
            if round(sum(row.get(key, 0) for row in gifts), 3) != round(sum(row.get(key, 0) for row in previous), 3):
                return 'skipped_accounting_mismatch'
        # Only take identity metrics from the recomputation; preserve existing time/amount rules.
        activity = [{**old, **{key: new[key] for key in ('gift_users', 'gift_users_unknown_events')}}
                    for old, new in zip(snapshot['activity_summary'], activity, strict=True)]
        if previous == gifts and snapshot['activity_summary'] == activity:
            return 'unchanged'
        if not execute:
            return 'would_publish'
        before = {name: _hash((public / name).read_bytes()) for name in FILES}
        snapshot.update(gift_summary=gifts, activity_summary=activity)
        with tempfile.TemporaryDirectory(dir=workspace) as directory:
            staging = Path(directory)
            write_snapshot(snapshot, staging, navigation=workspace / 'navigation.json', temporary_suffix='.html.tmp')
            files = {name: (staging / name).read_bytes().decode() for name in FILES}
        transaction = {'target': [config['remote'], config['url']], 'before': before, 'files': files,
                       'config_hash': _hash(cfg_path.read_bytes())}
        _write_pending(pending, transaction)
        _apply(config, workflow, pending, transaction)
        return 'published'


def backfill_remote_pages(warehouse, roots, *, execute=False):
    """Scan existing roots; dry-run makes no writes or remote requests."""
    counts = Counter()
    failures = []
    dictionary = None

    def load_dictionary():
        nonlocal dictionary
        if dictionary is None:
            dictionary = _load_identity_dictionary(warehouse)
        return dictionary

    for root in sorted({root.resolve() for root in roots}):
        for entry in sorted(root.iterdir()):
            if entry.is_symlink() or not entry.is_dir():
                continue
            try:
                # Avoid taking locks on local-only sessions in the remote pass.
                cfg = entry / 'config.json'
                if not cfg.is_file() or cfg.is_symlink() or json.loads(cfg.read_text()).get('local_only') is not False:
                    continue
                counts[_session(entry, warehouse, load_dictionary, execute)] += 1
            except Exception as exc:
                counts['failed'] += 1
                failures.append({'session': entry.name, 'reason': type(exc).__name__})
    return {'counts': dict(counts), 'failures': failures}
