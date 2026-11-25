"""Opt-in identity backfill for already published single-session pages."""
import fcntl
import hashlib
import json
import os
import re
import shlex
import subprocess
import tempfile
from collections import Counter
from contextlib import ExitStack
from pathlib import Path

from . import warehouse_backfill as local_backfill
from .activity_summary import render_gifters
from .live_share import atomic_json
from .live_share_control import load_config, source_at_cutoff, verify_snapshot
from .session_runner import read_remote_artifact, verify_remote
from .warehouse_backfill import (
    _atomic_write_bytes,
    _compute_final_artifacts,
    _load_blind_box_assignments,
    _recompute_summaries,
    assert_distinct_warehouse_path,
    overlay_session_gift_identities,
)

FILES = ('index.html', 'snapshot.json')
ACCOUNTING = ('gift_count', 'gift_count_unknown', 'gift_amount_yuan', 'gift_amount_unknown')

# Compare at the write destination under a directory lock, not just at the CDN.
_REMOTE_INSTALL = '''
import fcntl, hashlib, json, os, pathlib, sys, tempfile
root = pathlib.Path(sys.argv[1])
tx = json.load(sys.stdin)
fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
fcntl.flock(fd, fcntl.LOCK_EX)
pending = []
try:
    for name in ('index.html', 'snapshot.json'):
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise ValueError('invalid artifact')
        data = tx['files'][name].encode()
        current = hashlib.sha256(path.read_bytes()).hexdigest()
        new = hashlib.sha256(data).hexdigest()
        if current not in {tx['before'][name], new}:
            raise ValueError('destination changed')
        if current == new:
            continue
        with tempfile.NamedTemporaryFile(dir=root, delete=False) as temp:
            pending.append((temp.name, path))
            os.fchmod(temp.fileno(), path.stat().st_mode & 0o777)
            temp.write(data)
            temp.flush()
            os.fsync(temp.fileno())
    for temp, path in pending:
        os.replace(temp, path)
finally:
    for temp, _ in pending:
        pathlib.Path(temp).unlink(missing_ok=True)
    os.close(fd)
'''


def _publish(config, transaction):
    host, directory = config['remote'].split(':', 1)
    subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '--', host,
                    shlex.join(['python3', '-c', _REMOTE_INSTALL, directory.rstrip('/')])],
                   input=json.dumps({key: transaction[key] for key in ('before', 'files')}).encode(),
                   capture_output=True, check=True, timeout=45)


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
    for name in FILES:
        old = transaction['before'][name]
        new = _hash(transaction['files'][name].encode())
        if _hash((public / name).read_bytes()) not in {old, new}:
            raise ValueError('页面在回填后另有修改，不能自动覆盖')
        remote = _hash(read_remote_artifact(config, name))
        if remote not in {old, new}:
            raise ValueError('远端页面已另有修改，不能自动覆盖')
    with tempfile.TemporaryDirectory(dir=workspace) as directory:
        staged_workspace = Path(directory)
        staged = staged_workspace / 'public'
        staged.mkdir()
        for name in FILES:
            (staged / name).write_bytes(transaction['files'][name].encode())
        report = verify_snapshot(config, public_dir=staged, validation_only=True)
        if not report['valid']:
            raise ValueError('候选页面未通过本地校验')
        _publish(config, transaction)
        verify_remote({**config, 'workspace': str(staged_workspace)})
    for name in FILES:
        _atomic_write_bytes(public / name, transaction['files'][name].encode())
    workflow['final_artifacts'] = _compute_final_artifacts(workspace)
    atomic_json(workspace / 'workflow.json', workflow)
    pending.unlink()


def _replace_section(page, section_id, transform):
    pattern = re.compile(r'(<section id="' + section_id + r'">).*?</section>', re.S)
    result, count = pattern.subn(lambda match: transform(match.group()), page)
    if count != 1:
        raise ValueError('页面汇总区块不存在或重复')
    return result


def _users(row):
    return ('—' if row.get('gift_users') is None else
            str(row['gift_users']) + ('+' if row.get('gift_users_unknown_events') else ''))


def _update_html(page, previous, gifts, activity):
    if previous['gift_summary'] != gifts:
        page = _replace_section(page, 'gift-summary', lambda _: render_gifters(gifts))
    if previous['activity_summary'] == activity or previous.get('content_mode') == 'talk':
        return page

    def update(section):
        table = re.search(r'<tbody>(.*?)</tbody>', section, re.S)
        if table is None:
            raise ValueError('页面缺少活动表')
        rows = iter(zip(previous['activity_summary'], activity, strict=True))

        def row(match):
            before, after = next(rows)
            cells = list(re.finditer(r'<td(?: [^>]*)?>(.*?)</td>', match[0], re.S))
            if len(cells) != 7 or cells[4][1] != _users(before):
                raise ValueError('页面和快照的区间人数不一致')
            lo, hi = cells[4].span(1)
            return match[0][:lo] + _users(after) + match[0][hi:]

        body = re.sub(r'<tr>.*?</tr>', row, table[1], flags=re.S)
        if next(rows, None) is not None:
            raise ValueError('页面和快照的区间数量不一致')
        lo, hi = table.span(1)
        return section[:lo] + body + section[hi:]

    return _replace_section(page, 'activity-summary', update)


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
        for old, new in zip(snapshot['activity_summary'], activity, strict=True):
            if any(old.get(key) != new.get(key) for key in ACCOUNTING):
                return 'skipped_accounting_mismatch'
        # Only take identity metrics from the recomputation; preserve existing time/amount rules.
        activity = [{**old, **{key: new[key] for key in ('gift_users', 'gift_users_unknown_events')}}
                    for old, new in zip(snapshot['activity_summary'], activity, strict=True)]
        if previous == gifts and snapshot['activity_summary'] == activity:
            return 'unchanged'
        if not execute:
            return 'would_publish'
        before = {name: _hash((public / name).read_bytes()) for name in FILES}
        files = {'index.html': _update_html((public / 'index.html').read_bytes().decode(), snapshot, gifts, activity),
                 'snapshot.json': json.dumps({**snapshot, 'gift_summary': gifts, 'activity_summary': activity},
                                             ensure_ascii=False, separators=(',', ':'))}
        transaction = {'target': [config['remote'], config['url']], 'before': before, 'files': files,
                       'config_hash': _hash(cfg_path.read_bytes())}
        with tempfile.TemporaryDirectory(dir=workspace) as directory:
            for name, text in files.items():
                (Path(directory) / name).write_bytes(text.encode())
            if not verify_snapshot(config, public_dir=Path(directory), validation_only=True)['valid']:
                raise ValueError('候选页面未通过本地校验')
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
            reader = (getattr(local_backfill, '_load_full_identity_dictionary', None)
                      or local_backfill._load_identity_dictionary)
            dictionary = reader(warehouse)[:3]
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
