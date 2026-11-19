"""终审生命周期：程序接续idle协调、完成后有界排空；不依赖agent临时定时脚本。"""
import json
import subprocess
from pathlib import Path

from .live_share import atomic_json
from .session_ai import ai_control

GRACE_SECONDS = 120
WAKE_INTERVAL = 300
MAX_WAKES = 3


def drain(config, state, session, now, *, key='final_cleanup', children=True):
    from .session_runner import stop_ai
    if not session:
        return True
    record = state.setdefault(key, {'observed_complete_at': now})
    if now - record['observed_complete_at'] < GRACE_SECONDS:
        return False  # 包括子会话在内的停止操作必须等宽限结束。
    return stop_ai(config, session, children=children)


def supervise(config, state, session, root, now, *, key='coordinator_watch'):
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
    if now - watch['last_wake_at'] < WAKE_INTERVAL:
        return
    if now - watch.get('last_check_at', -float('inf')) < 60:
        return
    watch['last_check_at'] = now
    status = ai_control(config, 'status', session=session).get('type')
    watch['observed_status'] = status
    if status in ('busy', 'retry'):
        atomic_json(journal, watch)
        return
    check_budget(config, state)
    if watch['attempts'] >= MAX_WAKES:
        watch['delivery'] = 'wake_limit_reached'
        atomic_json(journal, watch)
        return
    marker = f'FINALIZE_RESUME:{root.name}:{watch["attempts"] + 1}'
    finish_steps = ('逐人核对引用归属和previous保留，按原audience_crm finish合同收尾；禁止音乐merge/corrections'
                    if key == 'audience_watch' else
                    '仅按原talk_finalize finish合同收尾；禁止音乐merge/corrections'
                    if root.name == 'talk_finalize' else '按原合同merge/校正重审/finish')
    uses_cli = (
        (key == 'audience_watch' and config.get('author_agent') in ('cursor_grok_cli', 'grok_build_cli'))
        or (key != 'audience_watch' and config.get('final_subagent') in ('cursor_cli_grok', 'grok_build_cli'))
    )
    review_line = (
        f'沿用原输入、原模型、已有结果和CLI回执。\n检查分片进度，有候选则实质语义审核后accept；全部通过后{finish_steps}。'
        if uses_cli else
        f'沿用原输入、原模型和已有分片结果。\n检查分片进度，未完成分片通过task派发给plan.json指定的subagent写入对应result路径，核验覆盖与指纹后{finish_steps}。'
    )
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
