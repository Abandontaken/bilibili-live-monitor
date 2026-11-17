"""本地匿名网页看板差量身份回填：严守只读内存叠加、范围限定、待跟进冻结、差量触发与全链路回滚五条硬边界。"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import time
from copy import deepcopy
from pathlib import Path
from typing import Any

from .activity_summary import gift_observations, summarize_gifters
from .gift_value import value_fields
from .live_share import atomic_json
from .live_share_control import load_config, source_at_cutoff, verify_snapshot
from .session_snapshot import write_snapshot
from .time_utils import epoch
from .warehouse import (
    assert_distinct_warehouse_path,
    collect_session_databases,
    open_readonly_sqlite,
    utc_now_iso,
)
from .warehouse_protocol import (
    clean_uname,
    extract_raw_gift_clues,
    is_masked_name,
    pos_int,
)

_ARTIFACT_NAMES = ('public/index.html', 'public/snapshot.json', 'annotations.json', 'verified_songs.json')


def _load_identity_dictionary(warehouse_path: Path) -> tuple[dict[int, str | None], dict[str, int], dict[str, int]]:
    users_by_uid: dict[int, str | None] = {}
    uid_by_md5: dict[str, int] = {}
    uid_by_unique_face: dict[str, int] = {}
    with open_readonly_sqlite(warehouse_path) as wh:
        wh.execute('BEGIN')
        for uid, md5_val, uname in wh.execute('SELECT uid, uid_md5, latest_uname FROM dim_users'):
            u_int = int(uid)
            clean = clean_uname(uname) if uname and not is_masked_name(uname) else None
            users_by_uid[u_int] = clean
            if isinstance(md5_val, str) and md5_val:
                uid_by_md5[md5_val] = u_int
        for sha1, uid in wh.execute('SELECT face_sha1, uid FROM v_unique_face_uid'):
            if isinstance(sha1, str) and sha1:
                uid_by_unique_face[sha1] = int(uid)
    return users_by_uid, uid_by_md5, uid_by_unique_face


def overlay_session_gift_identities(
    source: dict[str, Any],
    users_by_uid: dict[int, str | None],
    uid_by_md5: dict[str, int],
    uid_by_unique_face: dict[str, int],
) -> tuple[dict[str, Any], dict[str, int]]:
    """在内存副本上用全局身份字典消解本场礼物事件，绝不改动源数据。"""
    clues = extract_raw_gift_clues(source.get('raw', []))
    by_tid_batch: dict[tuple[str, int], dict[str, Any]] = clues['by_tid_batch']
    unused_fallbacks = [g for g in clues['all_gifts'] if not g.get('tid')]

    session_plain_names: dict[int, str] = {}
    for _, _, item in source.get('items', []):
        if not isinstance(item, dict) or item.get('sender_hidden') or item.get('identity_status') == 'unresolved':
            continue
        gift_part = item.get('gift') if isinstance(item.get('gift'), dict) else {}
        uid = pos_int(item.get('sender_uid')) or pos_int(gift_part.get('sender_uid'))
        author = clean_uname(item.get('author'))
        if uid and author and not is_masked_name(author):
            session_plain_names[uid] = author

    counters = {'resolved_uid_gifts': 0, 'md5_pseudonym_gifts': 0, 'untouched_gifts': 0}
    new_items: list[tuple[str, str, dict[str, Any]]] = []

    for kind, at_utc, raw_data in source.get('items', []):
        if kind != 'gift' or not isinstance(raw_data, dict) or not isinstance(raw_data.get('gift'), dict):
            new_items.append((kind, at_utc, raw_data))
            continue

        data = deepcopy(raw_data)
        gift = data['gift']
        if data.get('sender_hidden'):
            counters['untouched_gifts'] += 1
            new_items.append((kind, at_utc, data))
            continue

        ev_type = str(gift.get('event_type') or '')
        native_uid = (
            None if data.get('identity_status') == 'unresolved'
            else (pos_int(gift.get('sender_uid')) or pos_int(data.get('sender_uid')))
        )
        clue: dict[str, Any] | None = None
        if ev_type in ('SEND_GIFT', 'SEND_GIFT_V2'):
            ev_id = gift.get('event_id')
            b_idx = int(gift.get('batch_index', 0) or 0)
            if ev_id:
                clue = by_tid_batch.get((str(ev_id), b_idx))
            elif unused_fallbacks:
                ev_at = gift.get('event_at_utc') or at_utc
                ev_ts = epoch(ev_at)
                gid = str(gift.get('gift_id') or '')
                num = pos_int(gift.get('num')) or 1
                for idx, cand in enumerate(unused_fallbacks):
                    if (cand['gift_id'] == gid and cand['quantity'] == num
                            and abs(cand['event_ts'] - ev_ts) <= 2.0):
                        clue = unused_fallbacks.pop(idx)
                        break

        if clue and clue.get('is_mystery'):
            counters['untouched_gifts'] += 1
            new_items.append((kind, at_utc, data))
            continue

        clue_md5 = clue.get('clue_md5') if clue else None
        clue_face = clue.get('clue_face_sha1') if clue else None
        inferred_uid = clue.get('inferred_uid') if clue else None
        raw_uid = native_uid or (clue.get('raw_uid') if clue else None)

        resolved_uid = (
            raw_uid
            or (uid_by_md5.get(clue_md5) if clue_md5 else None)
            or (uid_by_unique_face.get(clue_face) if clue_face else None)
            or inferred_uid
        )

        if resolved_uid is not None:
            display_name = (
                users_by_uid.get(resolved_uid)
                or session_plain_names.get(resolved_uid)
                or (clean_uname(clue.get('raw_uname')) if clue and not is_masked_name(clue.get('raw_uname')) else None)
                or clean_uname(data.get('author'))
                or '未命名观众'
            )
            uid_str = str(resolved_uid)
            data.pop('analysis_identity_basis', None)
            data['sender_uid'] = uid_str
            gift['sender_uid'] = uid_str
            data['identity_status'] = 'uid'
            data['sender_hidden'] = False
            data['author'] = display_name
            counters['resolved_uid_gifts'] += 1
        elif clue_md5:
            short_tag = f'#{clue_md5[:4]}'
            base_name = clean_uname(data.get('author')) or '未命名观众'
            display_name = base_name if base_name.endswith(short_tag) else f'{base_name} {short_tag}'
            pseudo_id = f'md5:{clue_md5}'
            data.pop('analysis_identity_basis', None)
            data['sender_uid'] = pseudo_id
            gift['sender_uid'] = pseudo_id
            data['identity_status'] = 'unresolved_md5'
            data['sender_hidden'] = False
            data['author'] = display_name
            counters['md5_pseudonym_gifts'] += 1
        else:
            counters['untouched_gifts'] += 1

        new_items.append((kind, at_utc, data))

    return {**source, 'items': new_items}, counters


def _recompute_summaries(
    existing_snapshot: dict[str, Any],
    resolved_source: dict[str, Any],
    cutoff: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """仅重算 gift_summary 与 activity_summary（含单曲去重送礼人数），不动 followups 及其他快照字段。"""
    new_gift_summary = summarize_gifters(resolved_source)
    timed = [item[:4] for item in gift_observations(resolved_source, event_time_only=True)]
    new_activity_summary = deepcopy(existing_snapshot.get('activity_summary', []))

    for row in new_activity_summary:
        if row.get('time_unknown'):
            selected = [(cnt, amt, uid) for at, cnt, amt, uid in timed if at is None]
        else:
            start = epoch(row['start'])
            observed_end = epoch(row.get('window_end', row['end']))
            end = epoch(row['end'])
            selected = [
                (cnt, amt, uid)
                for at, cnt, amt, uid in timed
                if at is not None and (start <= at < observed_end or (at == cutoff == end and start <= at))
            ]
        row.update(
            gift_count=sum(cnt for cnt, _, _ in selected if cnt is not None),
            gift_count_unknown=sum(cnt is None for cnt, _, _ in selected),
            gift_users=len({str(uid) for _, _, uid in selected if uid}),
            gift_users_unknown_events=sum(not uid for _, _, uid in selected),
            gift_amount_yuan=sum(amt for _, amt, _ in selected if amt is not None) / 1000,
            gift_amount_unknown=sum(amt is None for _, amt, _ in selected),
        )
        value_fields(row)

    return new_gift_summary, new_activity_summary


def _compute_final_artifacts(workspace: Path) -> dict[str, str]:
    return {
        name: hashlib.sha256((workspace / name).read_bytes()).hexdigest()
        for name in _ARTIFACT_NAMES
        if (workspace / name).is_file()
    }


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    tmp = path.with_suffix(path.suffix + '.atomic.tmp')
    with tmp.open('wb') as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    tmp.replace(path)


def _process_single_session(
    session_dir: Path,
    warehouse_path: Path,
    users_by_uid: dict[int, str | None],
    uid_by_md5: dict[str, int],
    uid_by_unique_face: dict[str, int],
    execute: bool,
) -> dict[str, Any]:
    wf_path = session_dir / 'workflow.json'
    cfg_path = session_dir / 'config.json'
    snap_path = session_dir / 'public/snapshot.json'
    html_path = session_dir / 'public/index.html'
    state_path = session_dir / 'state.json'

    if not (wf_path.is_file() and cfg_path.is_file()):
        return {'status': 'skipped_missing_files'}

    workflow = json.loads(wf_path.read_text())
    if workflow.get('stage') != 'completed':
        return {'status': 'skipped_not_completed'}

    raw_cfg = json.loads(cfg_path.read_text())
    if raw_cfg.get('local_only') is not True:
        return {'status': 'skipped_not_local_only'}

    if not (snap_path.is_file() and html_path.is_file() and state_path.is_file()):
        return {'status': 'skipped_missing_files'}

    config = load_config(cfg_path)
    assert_distinct_warehouse_path(warehouse_path, [Path(config['database'])])
    workspace = Path(config['workspace'])

    _, cutoff, source = source_at_cutoff(config)
    existing_snapshot = json.loads(snap_path.read_text())

    resolved_source, counters = overlay_session_gift_identities(
        source, users_by_uid, uid_by_md5, uid_by_unique_face
    )
    new_gift_summary, new_activity_summary = _recompute_summaries(existing_snapshot, resolved_source, cutoff)

    expected_count = (existing_snapshot.get('session_summary') or {}).get('gift_count')
    actual_count = sum(row['gift_count'] for row in new_gift_summary)
    if expected_count is not None and actual_count != expected_count:
        raise ValueError(f'回填后礼物件数不守恒：期望 {expected_count}，实际 {actual_count}')

    if (new_gift_summary == existing_snapshot.get('gift_summary')
            and new_activity_summary == existing_snapshot.get('activity_summary')):
        return {'status': 'unchanged', 'session': session_dir.name}

    skey = str(config.get('workflow_key') or session_dir.name)
    change_detail = {
        'session_key': skey,
        'room_id': str(config['room_id']),
        'gift_summary_rows_before': len(existing_snapshot.get('gift_summary', [])),
        'gift_summary_rows_after': len(new_gift_summary),
        **counters,
    }

    if not execute:
        return {'status': 'would_update', **change_detail}

    with (workspace / 'workflow.lock').open('a') as wf_lock, (workspace / 'publisher.lock').open('a') as pub_lock:
        try:
            fcntl.flock(wf_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(pub_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'status': 'skipped_locked', 'session_key': skey}

        old_wf_bytes = wf_path.read_bytes()
        wf_locked = json.loads(old_wf_bytes)
        cfg_locked = json.loads(cfg_path.read_text())
        if wf_locked.get('stage') != 'completed' or cfg_locked.get('local_only') is not True:
            return {'status': 'skipped_state_changed', 'session_key': skey}

        old_snap_bytes = snap_path.read_bytes()
        old_html_bytes = html_path.read_bytes()
        history_path = workspace / 'identity_backfill_history.jsonl'
        old_history_bytes = history_path.read_bytes() if history_path.is_file() else None
        locked_snapshot = json.loads(old_snap_bytes)

        locked_config = load_config(cfg_path)
        _, locked_cutoff, locked_source = source_at_cutoff(locked_config)
        locked_resolved, locked_counters = overlay_session_gift_identities(
            locked_source, users_by_uid, uid_by_md5, uid_by_unique_face
        )
        locked_gift_summary, locked_activity_summary = _recompute_summaries(
            locked_snapshot, locked_resolved, locked_cutoff
        )
        if (locked_gift_summary == locked_snapshot.get('gift_summary')
                and locked_activity_summary == locked_snapshot.get('activity_summary')):
            return {'status': 'unchanged', 'session': session_dir.name}

        before_sha = {
            'public/snapshot.json': hashlib.sha256(old_snap_bytes).hexdigest(),
            'public/index.html': hashlib.sha256(old_html_bytes).hexdigest(),
        }

        public_dir = workspace / 'public'
        try:
            updated_snapshot = deepcopy(locked_snapshot)
            updated_snapshot['gift_summary'] = locked_gift_summary
            updated_snapshot['activity_summary'] = locked_activity_summary
            write_snapshot(
                updated_snapshot,
                public_dir,
                navigation=workspace / 'navigation.json',
                temporary_suffix='.html.tmp',
            )

            verification = verify_snapshot(locked_config)
            if not verification.get('valid'):
                raise ValueError(f"回填快照未通过原生校验：{verification.get('errors')}")

            after_sha = {
                'public/snapshot.json': hashlib.sha256(snap_path.read_bytes()).hexdigest(),
                'public/index.html': hashlib.sha256(html_path.read_bytes()).hexdigest(),
            }

            audit_entry = {
                'updated_at_utc': utc_now_iso(),
                'session_key': skey,
                'room_id': str(config['room_id']),
                'before_sha256': before_sha,
                'after_sha256': after_sha,
                'gift_summary_rows_before': len(locked_snapshot.get('gift_summary', [])),
                'gift_summary_rows_after': len(locked_gift_summary),
                **locked_counters,
            }
            encoded_line = (json.dumps(audit_entry, ensure_ascii=False, separators=(',', ':')) + '\n').encode('utf-8')
            _atomic_write_bytes(history_path, (old_history_bytes or b'') + encoded_line)

            wf_locked['final_artifacts'] = _compute_final_artifacts(workspace)
            atomic_json(wf_path, wf_locked)
        except BaseException as exc:
            # 全链路异常恢复：任何中间步骤失败均还原页面、审计日志与 workflow.json
            for tmp_candidate in (
                public_dir / 'index.html.tmp',
                public_dir / 'snapshot.json.tmp',
                history_path.with_suffix(history_path.suffix + '.atomic.tmp'),
                wf_path.with_suffix(wf_path.suffix + '.tmp'),
            ):
                tmp_candidate.unlink(missing_ok=True)
            _atomic_write_bytes(snap_path, old_snap_bytes)
            _atomic_write_bytes(html_path, old_html_bytes)
            if old_history_bytes is None:
                history_path.unlink(missing_ok=True)
            else:
                _atomic_write_bytes(history_path, old_history_bytes)
            _atomic_write_bytes(wf_path, old_wf_bytes)
            if isinstance(exc, ValueError) and '已自动回滚' in str(exc):
                raise
            raise ValueError(f'回填执行失败，已自动回滚：{type(exc).__name__}: {exc}') from exc

        return {'status': 'updated', **audit_entry}


def backfill_local_pages(
    warehouse_path: Path,
    session_roots: list[Path],
    *,
    execute: bool = False,
) -> dict[str, Any]:
    """扫描场次根目录，默认 dry-run 预演，仅在 execute=True 时对有变化的纯本地已完成场次原子回填。"""
    started = time.monotonic()
    session_dbs = collect_session_databases(session_roots)
    assert_distinct_warehouse_path(warehouse_path, session_dbs)
    users_by_uid, uid_by_md5, uid_by_unique_face = _load_identity_dictionary(warehouse_path)

    counts = {
        'updated': 0,
        'would_update': 0,
        'unchanged': 0,
        'skipped_not_completed': 0,
        'skipped_not_local_only': 0,
        'skipped_missing_files': 0,
        'skipped_locked': 0,
    }
    changed_sessions: list[dict[str, Any]] = []

    for root in session_roots:
        root_path = Path(root)
        for entry in sorted(root_path.iterdir()):
            if entry.is_symlink() or not entry.is_dir():
                continue
            outcome = _process_single_session(
                entry, warehouse_path, users_by_uid, uid_by_md5, uid_by_unique_face, execute=execute
            )
            st = outcome['status']
            counts[st] = counts.get(st, 0) + 1
            if st in ('updated', 'would_update'):
                changed_sessions.append(outcome)

    return {
        'mode': 'execute' if execute else 'dry_run',
        'warehouse': str(Path(warehouse_path).resolve()),
        'elapsed_seconds': round(time.monotonic() - started, 3),
        'dictionary_known_uids': len(users_by_uid),
        'dictionary_unique_faces': len(uid_by_unique_face),
        'counts': counts,
        'changed_sessions': changed_sessions,
    }
