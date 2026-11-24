"""终审生命周期：程序接续idle协调、完成后有界排空；不依赖agent临时定时脚本。"""
import json
import subprocess
from pathlib import Path

from .live_share import atomic_json
from .session_ai import ai_control

GRACE_SECONDS = 120
WAKE_INTERVAL = 300
MAX_WAKES = 3
CRM_WAKE_INTERVAL = 600
CRM_MAX_WAKES = 10
CRM_RETRY_INTERVAL = 900


def music_work(root):
    from .cursor_finalize import group_exists, maintenance_adapter_stopped

    plan = json.loads((root / 'plan.json').read_text())
    runs = root / 'cursor_workers'
    ready, dispatchable, active, accepted = [], [], 0, 0
    for shard in plan['shards']:
        name = Path(shard['input']).name.removesuffix('.input.json')
        receipt = runs / f'{name}.json'
        result = Path(shard['result']).is_file()
        if not receipt.exists():
            if result:
                ready.append(f'review:{name}:legacy')
            elif not (runs / 'stop.json').exists():
                dispatchable.append(f'start:{name}')
            continue
        worker = json.loads(receipt.read_text())
        if worker.get('plan_id') != plan['id'] or worker.get('shard', name) != name:
            continue
        status, token = worker.get('state'), worker.get('token')
        if result and (status == 'accepted' or (root / f'{name}.reviewed-acceptance.json').is_file()):
            accepted += 1
        elif status == 'awaiting_coordinator_review' and (runs / f'{name}.result.json').is_file():
            ready.append(f'review:{name}:{token}')
        elif (not result and not (runs / 'stop.json').exists()
              and ((status in ('failed', 'interrupted') and len(list((runs / 'history' / name).glob('*'))) < 2
                    and not (worker.get('pgid') and group_exists(worker['pgid'])))
                   or (status == 'maintenance_paused_before_start' and maintenance_adapter_stopped(worker)))):
            dispatchable.append(f'retry:{name}:{token}')
        elif status in ('submitting', 'starting', 'running', 'cleanup_failed'):
            active += 1
    if accepted == len(plan['shards']):
        ready.append(f'finish:{plan["id"]}')
    return ready, dispatchable, max(0, min(10, plan['concurrency']) - active)


def drain(config, state, session, now, *, key='final_cleanup', children=True):
    from .session_runner import stop_ai
    if not session:
        return True
    record = state.setdefault(key, {'observed_complete_at': now})
    if now - record['observed_complete_at'] < GRACE_SECONDS:
        return False  # 包括子会话在内的停止操作必须等宽限结束。
    return stop_ai(config, session, children=children)


def supervise(config, state, session, root, now, *, key='coordinator_watch', actionable=None):
    """锁由runner/watch持有。先落发送意图；结果不明只对账，绝不盲目重投。"""
    from .session_budget import check_budget
    if not session:
        return
    root = Path(root)
    watch = state.setdefault(key, {'last_wake_at': now, 'attempts': 0})
    journal = root / 'supervision.json'
    if journal.exists():
        watch = json.loads(journal.read_text())
        state[key] = watch
    if watch.get('pending_marker'):
        if not ai_control(config, 'submitted', session=session, marker=watch['pending_marker']):
            watch['delivery'] = 'unknown'
            atomic_json(journal, watch)
            return
        watch.pop('pending_marker')
        watch['delivery'] = 'verified_in_session'
        atomic_json(journal, watch)
    crm = key == 'audience_watch'
    by_action = actionable is not None
    interval = CRM_WAKE_INTERVAL if crm else WAKE_INTERVAL
    if now - watch['last_wake_at'] < interval:
        return
    if now - watch.get('last_check_at', -float('inf')) < (interval if crm else 60):
        return
    watch['last_check_at'] = now
    ready, dispatchable, capacity = actionable() if by_action else ([], [], 0)
    if by_action and not (ready or (capacity and dispatchable)):
        watch['decision'] = 'waiting_for_actionable_work'
        atomic_json(journal, watch)
        return
    status = ai_control(config, 'status', session=session).get('type')
    watch['observed_status'] = status
    if ((crm or by_action) and status != 'idle') or status in ('busy', 'retry'):
        atomic_json(journal, watch)
        return
    check_budget(config, state)
    if (crm or not by_action) and watch['attempts'] >= (CRM_MAX_WAKES if crm else MAX_WAKES):
        watch['delivery'] = 'wake_limit_reached'
        atomic_json(journal, watch)
        return
    if by_action:
        action_wakes = watch.get('action_wakes', {})
        actions = ready + (dispatchable if capacity else [])

        def can_wake(action):
            wakes = action_wakes.get(action, [])
            return len(wakes) < 2 and (not wakes or now - wakes[-1] >= CRM_RETRY_INTERVAL)

        eligible = [action for action in ready if can_wake(action)]
        eligible += [action for action in dispatchable if can_wake(action)][:capacity]
        if not eligible:
            watch['decision'] = ('action_retry_exhausted' if all(len(action_wakes.get(a, [])) >= 2 for a in actions)
                                 else 'action_retry_backoff')
            atomic_json(journal, watch)
            return
        watch['decision'] = 'action_retry' if all(a in action_wakes for a in eligible) else 'action_ready'
        for action in eligible:
            action_wakes[action] = [*action_wakes.get(action, []), now]
        watch['action_wakes'] = action_wakes
    marker = f'FINALIZE_RESUME:{root.name}:{watch["attempts"] + 1}'
    finish_steps = ('逐人核对引用归属和previous保留，按原audience_crm finish合同收尾；禁止音乐merge/corrections'
                    if key == 'audience_watch' else
                    '仅按原talk_finalize finish合同收尾；禁止音乐merge/corrections'
                    if root.name == 'talk_finalize' else '按原合同merge/校正重审/finish')
    uses_cli = (
        (key == 'audience_watch' and config.get('author_agent') in ('cursor_grok_cli', 'grok_build_cli'))
        or (key != 'audience_watch' and config.get('final_subagent') in ('cursor_cli_grok', 'grok_build_cli'))
    )
    author = config.get('author_agent')
    review_line = (
        f'沿用原输入、原模型、已有结果和CLI回执。\n检查分片进度，有候选则实质语义审核后accept；'
        f'有空位则按原合同补派未启动片或处理明确可重试的失败片；全部通过后{finish_steps}。'
        if uses_cli else
        f'检查既有结果和冻结输入；原分片由冻结的 {author} 子代理负责，不重派可能仍在写入的分片；全部通过后{finish_steps}。'
        if key == 'audience_watch' and author else
        f'执行路由未知，不得派发新分片；如既有结果全部通过则{finish_steps}。'
        if key == 'audience_watch' else
        f'沿用原输入、原模型和已有分片结果。\n检查分片进度，未完成分片通过task派发给plan.json指定的subagent写入对应result路径，核验覆盖与指纹后{finish_steps}。'
    )
    if by_action and uses_cli:
        review_line += ('\n本次仅处理以下到期待办（末段 token 用于区分尝试，分片序号补零）：'
                        + ', '.join(eligible) + '。不派发或重试清单以外的分片，不重试回执不明的派发。')
    prompt = root / 'resume_prompt.md'
    prompt.write_text(f'''{marker}
继续原终审计划 {root / 'plan.json'}。读取本目录原prompt.md及instructions（如有），{review_line}不要重跑成功片，不手写completed，不改原稿、边界或URL。
程序负责回访，不创建定时任务；busy时无需自行排timer。证据不足保留未知，不等待操作者作可逆的技术判断。真实不可恢复阻塞明确报告。
''')
    watch.update(last_wake_at=now, attempts=watch['attempts'] + 1,
                 pending_marker=marker, delivery='sending')
    atomic_json(journal, watch)
    try:
        result = subprocess.run([config['opencode_python'], '-m', 'opencode_skill', 'append',
            '--session-id', session, '--prompt-file', str(prompt), '--model', config.get('final_model') or config['model'],
            '--agent', 'build', '--send-timeout', '10', '--json'], cwd=config['opencode_cwd'],
            capture_output=True, text=True, timeout=35)
    except (OSError, subprocess.TimeoutExpired) as error:
        watch.update(delivery='unknown', error=type(error).__name__)
        atomic_json(journal, watch)
        return
    (root / 'resume_handoff.log').write_text(result.stdout + result.stderr)
    # ACK只代表交接，下一轮读取同会话marker核验送达。
    watch['delivery'] = 'handoff_accepted' if result.returncode == 0 else 'unknown'
    atomic_json(journal, watch)
