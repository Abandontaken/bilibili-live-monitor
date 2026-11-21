"""场次结束后的观众状态更新；独立总会话用指定子代理处理按人分片。"""
import json
import shlex
import time
from pathlib import Path

from . import audience_crm as crm
from .cursor_finalize import CURSOR_AUTHOR, GROK_BACKEND, frozen_author, maintenance_adapter_stopped
from .live_share import atomic_json


def cursor_prompt(session, config, directory, prompt):
    """保留CRM内容与总协调模型，只替换按人的执行后端。"""
    if config['author_agent'] not in (CURSOR_AUTHOR, GROK_BACKEND):
        return prompt
    old = f"仅对reused=false（本场有新互动、新提名或来源变更）的任务使用task的{config['author_agent']}子代理处理input/result，最多同时10个；不可静默改模型或创建下一级代理。"
    prompt = prompt.replace(old, '仅对reused=false的任务使用Cursor Grok CLI查证；不使用OpenCode task，不创建下一级代理。')
    prompt = prompt.replace('使用OpenCode工具自主查证后续', '使用CLI文件读取/检索工具自主查证后续')
    prompt = prompt.replace('或排定时任务。', '或安排本任务监督以外的定时任务。')
    from .deployment import task_command
    command = task_command(session, 'bilibili_live_monitor.cursor_finalize', component='crm') + ' ' + shlex.join([
        'start', '--config', session['_config_path'], '--kind', 'audience_finalize'])
    rendered = prompt + f'''
总协调仍由本OpenCode会话完成，先创建可见todo并按人更新状态、CLI session及验收结果；总协调模型保持原指定配置，不改成分片模型。
plan.tasks按列表位置从0编号，补零至少3位，例如000；编号对应同一person_id，不把person_id当数字序号。
对每个非reused任务调用 `{command} --shard 000`，替换实际编号。Launcher托管单片Cursor，固定grok-4.7-high、每片45分钟、最多10并发；同批启动所有就绪片，不把长CLI直接塞进短bash，不用sleep占满工具预算。
逐片日志/回执在{directory / 'cursor_workers'}；stream init含CLI session_id。文件存在、日志增长或进程存活都不等于完成，须核对退出0及最终result/success/is_error=false。
Cursor只写候选，总协调逐项查证引用归属、未决承诺与previous保留，再把start改为accept（其他参数不变）完成程序验证及转正。reused=true的结果由原程序验证并直接保留；已有旧结果无Cursor回执时先做同样语义审核再accept，记录legacy_result，不伪造CLI回执、不重跑。
明确failed/interrupted且无旧写入者时可把start改为retry，最多两次且优先同CLI UUID、同模型；发送不明先查回执与进程，不盲目重投或手工改状态。
等待全部分片实际退出并验收，仍按原合同统一audience_crm finish，保留冻结材料、来源查证、逐字引用校验与current指针事务，不运行音乐终审的merge/corrections，不发布不通知。
程序负责同会话回访，不创建临时脚本或定时任务；不能用idle代替完成。超过本场预算或不可恢复时如实报告失败。
'''
    if config['author_agent'] == GROK_BACKEND:
        rendered = (rendered.replace('Cursor Grok CLI', 'Grok Build CLI')
                    .replace('单片Cursor', '单片Grok Build CLI').replace('grok-4.7-high', 'grok-4.7')
                    .replace('stream init含CLI session_id', '终态JSON含CLI sessionId')
                    .replace('最终result/success/is_error=false', '终态end_turn及实际模型grok-4.7-build')
                    .replace('Cursor只写', 'Grok Build CLI只写').replace('无Cursor回执', '无CLI回执'))
    return rendered

def load_settings(session):
    path = Path(session['audience_crm_config'])
    config = json.loads(path.read_text())
    if config['room_id'] != session['room_id']:
        raise ValueError('本场CRM配置属于其他主播')
    agent = config.get('author_agent', 'grok' if config.get('agentic_sources') else 'ollama_glm_5_3_flash')
    if not isinstance(agent, str) or not agent.strip():
        raise ValueError('author_agent须为非空agent名称')
    config['author_agent'] = agent
    model = config.get('coordinator_model')
    if model is not None and (not isinstance(model, str) or '/' not in model or not model.strip()):
        raise ValueError('coordinator_model须为provider/model')
    return config, path


def accepted_ready(session):
    path = Path(session['workspace']) / 'audience_finalize/plan.json'
    if not path.exists():
        return False
    saved = json.loads(path.read_text())
    directory = Path(saved['run'])
    if not (directory / 'accepted.json').exists():
        return False
    config, _ = load_settings(session)
    crm.verify_accepted(config, directory, saved['plan_id'])
    return True


def actionable_work(directory, plan_id, author):
    """Identify ready coordinator actions from the frozen plan and worker receipts."""
    plan = json.loads((directory / 'plan.json').read_text())
    if plan['id'] != plan_id:
        raise ValueError('观众终审计划身份不符')
    waiting, review, retry = [], [], []
    active = 0
    incomplete = 0
    cli = author in (CURSOR_AUTHOR, GROK_BACKEND)
    for index, task in enumerate(plan['tasks']):
        if task.get('reused'):
            continue
        result = Path(task['result'])
        receipt = directory / 'cursor_workers' / f'{index:03d}.json'
        if cli and receipt.is_file():
            item = json.loads(receipt.read_text())
            legacy = item.get('backend') == 'legacy_result' and item.get('state') == 'accepted'
            if item.get('plan_id') != plan_id or (not legacy and item.get('shard') != f'{index:03d}'):
                raise ValueError('观众终审分片回执身份不符')
            status = item.get('state')
            if status == 'accepted' and result.is_file():
                continue
            incomplete += 1
            if status == 'awaiting_coordinator_review' and item.get('exit_code') == 0:
                review.append(f'review:{index}:{item.get("token")}')
            elif status in ('failed', 'interrupted') and not result.exists():
                history = receipt.parent / 'history' / receipt.stem
                if not history.exists() or len(list(history.iterdir())) < 2:
                    retry.append(f'retry:{index}:{item.get("token")}')
            elif status == 'maintenance_paused_before_start' and maintenance_adapter_stopped(item):
                retry.append(f'retry:{index}:{item.get("token")}')
            elif status in ('submitting', 'starting', 'running', 'cleanup_failed'):
                active += 1
        elif result.is_file():
            if cli:
                incomplete += 1
                review.append(f'review:{index}:legacy')
        else:
            incomplete += 1
            waiting.append(index)
    capacity = max(0, min(10, plan.get('concurrency', 10)) - active)
    stopped = cli and (directory / 'cursor_workers' / 'stop.json').exists()
    # A native child may still be writing even if its parent is idle; a missing
    # file is not proof that it is safe to redispatch without a worker receipt.
    dispatchable = (retry + [f'start:{index}' for index in waiting])[:capacity] if cli and not stopped else []
    return [*review, *dispatchable, *(['finish'] if not incomplete else [])]


def advance(session, state):
    """返回True才允许本场收尾；失败不改旧档案，不重复提交不确定的任务。"""
    if not session.get('audience_crm_config'):
        return True
    from .deployment import require_running
    require_running('crm')
    from .session_ai import ai_control
    from .session_budget import check_budget
    from .session_finalize import dispatch
    from .session_runner import ensure_ai
    config, path = load_settings(session)
    if config.get('coordinator_model'):
        session = {**session, 'model': config['coordinator_model'], 'final_model': config['coordinator_model']}
    elif session.get('model'):
        session = {**session, 'final_model': session['model']}
    state_path = Path(session.get('audience_state_path') or Path(session['workspace']) / 'workflow.json')
    root = Path(session['workspace']) / 'audience_finalize'
    ready = accepted_ready(session)
    check_budget(session, state, publication=ready)
    from .deployment import component_call
    pin_path = Path(session.get('deployment_pin') or root / 'deployment.json')
    session = {**session, 'deployment_pin': str(pin_path)}
    forwarded = component_call('crm', pin_path, __name__, 'advance', [session, state],
                               new_task=not (root / 'plan.json').exists())
    if forwarded is not None:
        state.clear()
        state.update(forwarded['arguments'][1])
        return forwarded['result']
    root.mkdir(exist_ok=True)
    marker = root / 'plan.json'
    if not marker.exists():
        config['_include_session'] = session['_config_path']
        directory, plan = crm.prepare(config, crm.load_material(config))
        atomic_json(marker, {'run': str(directory), 'plan_id': plan['id'],
                            'budget_state_path': str(state_path),
                            'author_agent': config['author_agent']})
    saved = json.loads(marker.read_text())
    if saved.get('budget_state_path') != str(state_path):
        saved['budget_state_path'] = str(state_path)
        atomic_json(marker, saved)
    directory = Path(saved['run'])
    accepted = directory / 'accepted.json'
    check_budget(session, state, publication=accepted.exists())
    if (not accepted.exists()
            and json.loads((directory / 'material.json').read_text()).get('update_policy')
            != 'meaningful-interaction-v1'):
        raise ValueError('旧未完成CRM计划须按有效互动规则重新准备，不可继续派发')
    if accepted.exists():
        crm.verify_accepted(config, directory, saved['plan_id'])
        from .finalize_lifecycle import drain
        if not drain(session, state, state.get('audience_session'), time.time(), key='audience_cleanup'):
            return False
        from .cursor_finalize import require_run_stopped
        require_run_stopped(directory)
        if config.get('publish_on_finalize') and not session.get('local_only', False):
            check_budget(session, state, publication=True)
            from .audience_publish import publish
            result = publish(config)
        else:
            from .audience_pages import build
            result = build(config)
        state['audience_crm'] = {'plan_id': result['plan_id'], 'people': result['people'],
                                 'published': result.get('status') == 'published'}
        return True
    config['author_agent'] = frozen_author(saved, root)
    if not state.get('audience_session'):
        state['audience_session'] = ensure_ai(session, 'audience_finalize')
        atomic_json(state_path, state)
    ai_session = state['audience_session']
    tag = 'AUDIENCE_PLAN:' + saved['plan_id']
    if state.get('audience_dispatch_uncertain'):
        if ai_control(session, 'submitted', session=ai_session, marker=tag):
            state.pop('audience_dispatch_uncertain', None)
            state['audience_submitted'] = True
        else:
            raise ValueError('CRM提交结果不确定，须核查原会话，不自动重发')
    if not state.get('audience_submitted'):
        from .deployment import task_command
        command = task_command(session, 'bilibili_live_monitor.audience_crm', component='crm') + ' ' + shlex.join([
            'finish', '--config', str(path), '--run', str(directory)])
        prompt = f'''{tag}
你负责此场CRM总协调，不改代码、不发布、不通知。读取{directory / 'plan.json'}及{directory / 'instructions.txt'}。
plan.json中reused=true的任务已由程序校验复用上一版result，直接保留无需重跑；仅对reused=false（本场有新互动、新提名或来源变更）的任务使用task的{config['author_agent']}子代理处理input/result，最多同时10个；不可静默改模型或创建下一级代理。
每个待更新任务读取自己的input与其中source_manifest指向的完整本房间来源，使用OpenCode工具自主查证后续；在保留previous有效历史信息的基础上增量合入新场变化，只写对应result，不改来源。没有source_manifest时仅使用原有input证据。
不能把JSON通过当作查证完成。对reused=false的任务须实际查证且校验通过；重点追查“未见回应/待核实”的点歌及首次入团送歌兑现情况，不把可从源文件查到的工作转交主播。
总任务重点抽查未完成承诺、已跨场兑现及同名歧义：一条引用真实不等于结论成立；必须核对双方上下文，不能挪用其他观众的回应。
全部完成后统一运行：{command}。finish失败须修复原结果，不伪造空结果通过，不修改计划或输入。
成功后报告完成数与未决问题。若指定子代理不可用，报告阻塞，不fallback。不能访问凭据、控制采集、发送直播消息或排定时任务。
'''

        def handoff():
            state['audience_dispatch_uncertain'] = True
            atomic_json(state_path, state)

        dispatch(session, ai_session, cursor_prompt(session, config, directory, prompt), root, handoff)
        state.pop('audience_dispatch_uncertain', None)
        state['audience_submitted'] = True
    from .finalize_lifecycle import supervise
    supervise({**session, 'author_agent': config['author_agent']},
              state, ai_session, root, time.time(), key='audience_watch',
              actionable=lambda: actionable_work(directory, saved['plan_id'], config['author_agent']))
    return False
