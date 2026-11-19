"""事件触发的短驻静音浏览器开播打卡助手，复用既有已登录CDP实例且绝不关闭浏览器。"""
import argparse
import asyncio
import fcntl
import json
import os
import random
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from playwright.async_api import async_playwright

from .browser import MUTE_SCRIPT, close_page, room_url
from .welcome import send_chat, text_length

# Automation.send_notification 子进程硬上限为 90s：
# 业务执行封顶 64s + 取消收敛 2s + 保活页 3s + 双级关页 11.5s + 空白页撤离 2s + Playwright 断连 3s = 85.5s（另留 4.5s 启动与输出裕量）。
MAX_EXECUTION_BUDGET_SECONDS = 64.0
CANCEL_DRAIN_SECONDS = 2.0
ANCHOR_CREATE_BUDGET_SECONDS = 1.5
CLOSE_PAGE_BUDGET_SECONDS = 11.5
EVACUATE_BUDGET_SECONDS = 2.0
PW_STOP_RESERVE_SECONDS = 3.0
PREFLIGHT_BUDGET_SECONDS = 10.0
SEND_CHAT_BUDGET_SECONDS = 16.0
NAV_URL = 'https://api.bilibili.com/x/web-interface/nav'
ROOM_INFO_URL = 'https://api.live.bilibili.com/room/v1/Room/get_info'
ROOM_USER_INFO_URL = 'https://api.live.bilibili.com/xlive/web-room/v1/index/getInfoByUser'
ANCHOR_URL = 'about:blank#live-monitor-anchor'
ALLOWED_CONFIG_KEYS = frozenset({
    'room_id',
    'cdp_url',
    'expected_uid',
    'message',
    'messages',
    'state_path',
    'timezone',
    'once_per_day',
    'min_interval_seconds',
    'jitter_seconds',
    'linger_seconds',
    'page_timeout_ms',
    'max_message_length',
})


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_loopback_cdp(cdp_url):
    if not isinstance(cdp_url, str) or not cdp_url.strip():
        raise ValueError('cdp_url必须是非空字符串')
    parsed = urlsplit(cdp_url.strip())
    if parsed.scheme not in ('http', 'https', 'ws', 'wss'):
        raise ValueError('cdp_url协议必须是http/https/ws/wss')
    if parsed.username is not None or parsed.password is not None:
        raise ValueError('cdp_url不得包含凭据')
    if parsed.hostname not in ('127.0.0.1', 'localhost', '::1'):
        raise ValueError('cdp_url仅允许本机回环地址')
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError('cdp_url端口无效') from error
    if port is None or not (1 <= port <= 65535):
        raise ValueError('cdp_url必须显式指定有效端口')
    return cdp_url.strip()


def load_checkin_config(path):
    config_path = Path(path).expanduser().resolve()
    raw = json.loads(config_path.read_text(encoding='utf-8'))
    if not isinstance(raw, dict):
        raise ValueError('打卡配置必须是JSON对象')
    unknown = sorted(set(raw) - ALLOWED_CONFIG_KEYS)
    if unknown:
        raise ValueError(f'打卡配置包含未知字段: {", ".join(unknown)}')

    raw_room = str(raw.get('room_id') or '').strip()
    canonical_url = room_url(raw_room)
    rid = canonical_url.rsplit('/', 1)[-1]
    if rid != raw_room or not rid.isdigit():
        raise ValueError('room_id必须是正整数房间号字符串')

    cdp_url = _validate_loopback_cdp(raw.get('cdp_url'))

    expected_uid = str(raw.get('expected_uid') or '').strip()
    if not expected_uid.isdigit() or int(expected_uid) <= 0:
        raise ValueError('expected_uid必须是正整数UID字符串')

    has_single = 'message' in raw
    has_multi = 'messages' in raw
    if has_single == has_multi:
        raise ValueError('打卡配置必须且只能提供message或messages之一')

    max_len = raw.get('max_message_length', 80)
    if type(max_len) is not int or not (1 <= max_len <= 200):
        raise ValueError('max_message_length必须是1到200之间的整数')

    candidates = [raw['message']] if has_single else raw['messages']
    if not isinstance(candidates, list) or not candidates:
        raise ValueError('打卡文案列表不能为空')
    messages = []
    for item in candidates:
        if not isinstance(item, str) or not item.strip():
            raise ValueError('打卡文案必须是非空字符串')
        if any(ch in item for ch in '\r\n\t\x00') or item != item.strip():
            raise ValueError('打卡文案不得包含控制字符或首尾空白')
        if text_length(item) > max_len:
            raise ValueError(f'打卡文案宽度({text_length(item)})超出上限({max_len})')
        messages.append(item)

    raw_state = raw.get('state_path')
    if not isinstance(raw_state, str) or not raw_state.strip():
        raise ValueError('state_path必须是非空路径字符串')
    state_candidate = Path(raw_state.strip()).expanduser()
    state_path = (state_candidate if state_candidate.is_absolute()
                  else (config_path.parent / state_candidate)).resolve()

    tz_name = raw.get('timezone', 'Asia/Shanghai')
    if not isinstance(tz_name, str) or not tz_name.strip():
        raise ValueError('timezone必须是非空时区名称')
    try:
        ZoneInfo(tz_name)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f'未知时区: {tz_name}') from error

    once_per_day = raw.get('once_per_day', True)
    if type(once_per_day) is not bool:
        raise ValueError('once_per_day必须是布尔值')

    min_interval = raw.get('min_interval_seconds', 21600.0)
    if not _is_number(min_interval) or min_interval < 0:
        raise ValueError('min_interval_seconds必须是非负数字')

    jitter_raw = raw.get('jitter_seconds', [3.0, 8.0])
    if (not isinstance(jitter_raw, list) or len(jitter_raw) != 2
            or not all(_is_number(v) for v in jitter_raw)
            or jitter_raw[0] < 0 or jitter_raw[0] > jitter_raw[1]):
        raise ValueError('jitter_seconds必须是形如[min, max]且0<=min<=max的二元数组')

    linger_seconds = raw.get('linger_seconds', 10.0)
    if not _is_number(linger_seconds) or not (0 <= linger_seconds <= 30):
        raise ValueError('linger_seconds必须在0到30秒之间')

    page_timeout_ms = raw.get('page_timeout_ms', 20000)
    if type(page_timeout_ms) is not int or not (3000 <= page_timeout_ms <= 30000):
        raise ValueError('page_timeout_ms必须是3000到30000之间的整数毫秒')

    worst_case = (
        float(jitter_raw[1])
        + PREFLIGHT_BUDGET_SECONDS
        + page_timeout_ms / 1000.0
        + SEND_CHAT_BUDGET_SECONDS
        + float(linger_seconds)
    )
    if worst_case > MAX_EXECUTION_BUDGET_SECONDS:
        raise ValueError(
            f'最坏端到端执行预算({worst_case:.1f}s)超出安全上限({MAX_EXECUTION_BUDGET_SECONDS:.0f}s)，请调小等待参数'
        )

    return {
        'room_id': rid,
        'cdp_url': cdp_url,
        'expected_uid': expected_uid,
        'messages': messages,
        'max_message_length': max_len,
        'state_path': state_path,
        'timezone': tz_name,
        'once_per_day': once_per_day,
        'min_interval_seconds': float(min_interval),
        'jitter_seconds': (float(jitter_raw[0]), float(jitter_raw[1])),
        'linger_seconds': float(linger_seconds),
        'page_timeout_ms': page_timeout_ms,
    }


def save_state(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f'.{path.name}.', dir=path.parent)
    try:
        payload = (json.dumps(data, ensure_ascii=False, indent=2) + '\n').encode('utf-8')
        with os.fdopen(fd, 'wb') as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def load_state(path: Path):
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise ValueError('打卡状态文件格式损坏')
    return data


def normalize_live_start(value):
    text = str(value or '').strip()
    if not text or text.startswith('0000-00-00'):
        return ''
    try:
        parsed = datetime.strptime(text, '%Y-%m-%d %H:%M:%S')
        return parsed.replace(tzinfo=ZoneInfo('Asia/Shanghai')).astimezone(timezone.utc).isoformat()
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).isoformat()
    except ValueError:
        pass
    return text


def evaluate_cooldown(config, state, *, now, live_start):
    if live_start and state.get('last_live_start') == live_start:
        return 'same_live_start'
    tz = ZoneInfo(config['timezone'])
    local_date = datetime.fromtimestamp(now, tz).date().isoformat()
    if config['once_per_day'] and state.get('last_sent_local_date') == local_date:
        return 'already_sent_today'
    last_epoch = state.get('last_sent_epoch')
    if config['min_interval_seconds'] > 0 and _is_number(last_epoch):
        if (now - float(last_epoch)) < config['min_interval_seconds']:
            return 'min_interval_cooldown'
    return None


def choose_checkin_message(messages, last_message=None, *, rng=random):
    if len(messages) == 1:
        return messages[0]
    filtered = [item for item in messages if item != last_message]
    return rng.choice(filtered or messages)


async def _retire_temp_page(context, page, close_page_fn):
    """关闭临时直播页；单页、关页失败或关页超时后，单独执行有界空白页导航脱离直播间。"""
    others = [p for p in context.pages if p is not page and not p.is_closed()]
    if not others:
        try:
            anchor = await asyncio.wait_for(
                context.new_page(), timeout=ANCHOR_CREATE_BUDGET_SECONDS
            )
            await asyncio.wait_for(
                anchor.goto(ANCHOR_URL, wait_until='domcontentloaded', timeout=1400),
                timeout=ANCHOR_CREATE_BUDGET_SECONDS,
            )
            others = [anchor]
        except Exception:
            await asyncio.wait_for(
                page.goto(ANCHOR_URL, wait_until='domcontentloaded', timeout=1800),
                timeout=EVACUATE_BUDGET_SECONDS,
            )
            if page.url != ANCHOR_URL:
                raise RuntimeError('无法新建保活页且未能将临时页退回空白页')
            return {'closed': False, 'evacuated': True}
    close_err = None
    try:
        await asyncio.wait_for(close_page_fn(page), timeout=CLOSE_PAGE_BUDGET_SECONDS)
        if not page.is_closed():
            close_err = RuntimeError('临时打卡标签页未能确认关闭')
    except Exception as err:
        close_err = err
    if close_err is None:
        return {'closed': True, 'evacuated': True}
    evacuated = False
    try:
        if not page.is_closed():
            await asyncio.wait_for(
                page.goto(ANCHOR_URL, wait_until='domcontentloaded', timeout=1800),
                timeout=EVACUATE_BUDGET_SECONDS,
            )
            evacuated = page.url == ANCHOR_URL
        else:
            evacuated = True
    except Exception:
        evacuated = False
    close_err.evacuated = evacuated
    raise close_err


async def _await_bounded_send(send_coro, timeout_seconds):
    task = asyncio.create_task(send_coro)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout_seconds)
    except (asyncio.TimeoutError, asyncio.CancelledError) as interrupt_err:
        if not task.done():
            task.cancel()
        done, _ = await asyncio.wait({task}, timeout=CANCEL_DRAIN_SECONDS)
        if done:
            try:
                return await task
            except BaseException as inner:
                attempted = getattr(inner, 'send_attempted', None)
                interrupt_err.send_attempted = False if attempted is False else True
                interrupt_err.send_diagnostics = getattr(inner, 'send_diagnostics', [])
        else:
            interrupt_err.send_attempted = True
            interrupt_err.send_diagnostics = [{'step': 'cancel_drain_timeout'}]
        raise interrupt_err


async def _cdp_preflight(context, config, *, timeout_ms_fn):
    nav_resp = await context.request.get(NAV_URL, timeout=min(6000, timeout_ms_fn()))
    if nav_resp.status != 200:
        raise RuntimeError(f'登录态接口返回异常HTTP {nav_resp.status}')
    nav_body = await nav_resp.json()
    nav_data = (nav_body.get('data') if isinstance(nav_body, dict)
                and isinstance(nav_body.get('data'), dict) else {})
    is_login = isinstance(nav_body, dict) and nav_body.get('code') == 0 and nav_data.get('isLogin') is True
    actual_uid = str(nav_data.get('mid') or '')
    if not is_login or actual_uid != config['expected_uid']:
        raise RuntimeError('当前浏览器登录账号与expected_uid不符，拒绝进房打卡')

    room_resp = await context.request.get(
        f"{ROOM_INFO_URL}?room_id={config['room_id']}", timeout=min(6000, timeout_ms_fn())
    )
    if room_resp.status != 200:
        raise RuntimeError(f'房间状态接口返回异常HTTP {room_resp.status}')
    room_body = await room_resp.json()
    if not isinstance(room_body, dict) or room_body.get('code') != 0 or not isinstance(room_body.get('data'), dict):
        code = room_body.get('code') if isinstance(room_body, dict) else 'invalid'
        raise RuntimeError(f'房间状态查询失败：code={code}')
    room_data = room_body['data']
    canonical_id = str(room_data.get('room_id') or '')
    if canonical_id != config['room_id']:
        raise RuntimeError('房间状态返回规范房号与打卡配置不符')
    return {
        'canonical_room_id': canonical_id,
        'live_status': int(room_data.get('live_status') or 0),
        'api_live_start': normalize_live_start(room_data.get('live_time')),
    }


async def run_checkin(config, payload, *, dry_run=False, playwright_factory=async_playwright,
                      send_chat_fn=send_chat, close_page_fn=close_page,
                      sleep_fn=asyncio.sleep, now_fn=time.time, monotonic_fn=time.monotonic,
                      rng=random):
    if not isinstance(payload, dict):
        raise ValueError('通知事件JSON必须是对象')

    payload_room = str(payload.get('room_id') or '').strip()
    if (payload_room or not dry_run) and payload_room != config['room_id']:
        raise ValueError('通知事件room_id与打卡配置不符')

    event = str(payload.get('event') or '').strip()
    if not dry_run and event != 'live':
        return {
            'id': f'skipped:{event or "unknown"}',
            'status': 'skipped',
            'reason': 'not_live_event',
            'room_id': config['room_id'],
            'event': event,
        }

    state_path = config['state_path']
    tz = ZoneInfo(config['timezone'])
    payload_live_start = normalize_live_start(payload.get('live_start'))
    overall_deadline = monotonic_fn() + MAX_EXECUTION_BUDGET_SECONDS

    def remaining_budget_ms(stage_label='执行打卡'):
        rem_s = overall_deadline - monotonic_fn()
        if rem_s <= 0:
            raise TimeoutError(f'{stage_label}超出总执行预算')
        return max(500, int(rem_s * 1000))

    if dry_run:
        state = load_state(state_path)
        if state.get('status') in ('sending', 'ambiguous'):
            raise RuntimeError('存在未确认的打卡发送状态，需人工核验后再恢复')
        now = float(now_fn())
        skip_reason = evaluate_cooldown(config, state, now=now, live_start=payload_live_start)
        message = choose_checkin_message(config['messages'], state.get('last_message'), rng=rng)
        pw_cm = playwright_factory()
        pw = await asyncio.wait_for(pw_cm.__aenter__(), timeout=remaining_budget_ms('启动Playwright') / 1000.0)
        try:
            browser = await pw.chromium.connect_over_cdp(
                config['cdp_url'], timeout=min(8000, remaining_budget_ms('CDP连接'))
            )
            if not browser.contexts:
                raise RuntimeError('CDP浏览器不存在可用上下文')
            context = browser.contexts[0]
            room_info = await _cdp_preflight(
                context, config, timeout_ms_fn=lambda: remaining_budget_ms('进房前预检')
            )
            danmu_length = None
            user_resp = await context.request.get(
                f"{ROOM_USER_INFO_URL}?room_id={room_info['canonical_room_id']}",
                timeout=min(6000, remaining_budget_ms('弹幕上限预检')),
            )
            if user_resp.status == 200:
                user_body = await user_resp.json()
                if isinstance(user_body, dict) and user_body.get('code') == 0 and isinstance(user_body.get('data'), dict):
                    prop = user_body['data'].get('property') or {}
                    danmu = prop.get('danmu') if isinstance(prop, dict) else {}
                    limit_val = danmu.get('length') if isinstance(danmu, dict) else None
                    if isinstance(limit_val, int) and limit_val > 0:
                        danmu_length = limit_val
                        widest = max(text_length(item) for item in config['messages'])
                        if widest > danmu_length:
                            raise RuntimeError(f'打卡文案最大宽度({widest})超出房间弹幕上限({danmu_length})')
        finally:
            try:
                await asyncio.wait_for(pw_cm.__aexit__(None, None, None), timeout=PW_STOP_RESERVE_SECONDS)
            except Exception:
                pass
        return {
            'id': f"dry_run:{config['room_id']}",
            'status': 'dry_run',
            'room_id': config['room_id'],
            'uid_verified': True,
            'live_status': room_info['live_status'],
            'danmu_length': danmu_length,
            'message_width': text_length(message),
            'would_skip': skip_reason is not None,
            'skip_reason': skip_reason,
            'state_status': state.get('status', 'idle'),
        }

    state_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = state_path.with_name(state_path.name + '.lock')
    with lock_path.open('a', encoding='utf-8') as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError('同房间已有打卡进程正在执行') from error

        state = load_state(state_path)
        if state.get('status') in ('sending', 'ambiguous'):
            raise RuntimeError('存在未确认的打卡发送状态，需人工核验后再恢复')

        now = float(now_fn())
        skip_reason = evaluate_cooldown(config, state, now=now, live_start=payload_live_start)
        if skip_reason is not None:
            return {
                'id': f'skipped:{skip_reason}',
                'status': 'skipped',
                'reason': skip_reason,
                'room_id': config['room_id'],
            }

        message = choose_checkin_message(config['messages'], state.get('last_message'), rng=rng)
        j_min, j_max = config['jitter_seconds']
        jitter = rng.uniform(j_min, j_max) if j_max > 0 else 0.0
        if jitter > 0:
            await sleep_fn(jitter)

        pw_cm = playwright_factory()
        pw = await asyncio.wait_for(
            pw_cm.__aenter__(), timeout=min(5.0, remaining_budget_ms('启动Playwright') / 1000.0)
        )
        try:
            browser = await pw.chromium.connect_over_cdp(
                config['cdp_url'], timeout=min(8000, remaining_budget_ms('CDP连接'))
            )
            if not browser.contexts:
                raise RuntimeError('CDP浏览器不存在可用上下文')
            context = browser.contexts[0]
            room_info = await _cdp_preflight(
                context, config, timeout_ms_fn=lambda: remaining_budget_ms('进房前预检')
            )
            if room_info['live_status'] != 1:
                return {
                    'id': 'skipped:room_not_live',
                    'status': 'skipped',
                    'reason': 'room_not_live',
                    'room_id': config['room_id'],
                    'live_status': room_info['live_status'],
                }

            effective_live_start = room_info['api_live_start'] or payload_live_start
            recheck_reason = evaluate_cooldown(
                config, state, now=float(now_fn()), live_start=effective_live_start
            )
            if recheck_reason is not None:
                return {
                    'id': f'skipped:{recheck_reason}',
                    'status': 'skipped',
                    'reason': recheck_reason,
                    'room_id': config['room_id'],
                }

            page_deadline = min(overall_deadline - 2.0,
                                monotonic_fn() + config['page_timeout_ms'] / 1000.0)

            def remaining_page_ms():
                remaining = int((page_deadline - monotonic_fn()) * 1000)
                if remaining <= 0:
                    raise TimeoutError('打卡页面加载超时')
                return max(500, remaining)

            page = await asyncio.wait_for(context.new_page(), timeout=remaining_page_ms() / 1000.0)
            sent_epoch = None
            ack = None
            done_state = None
            cleanup_error = None
            send_entered = False
            cleanup_evacuated = True
            try:
                try:
                    await asyncio.wait_for(
                        page.add_init_script(MUTE_SCRIPT),
                        timeout=remaining_page_ms() / 1000.0,
                    )
                    response = await page.goto(
                        f"https://live.bilibili.com/{config['room_id']}",
                        wait_until='domcontentloaded',
                        timeout=remaining_page_ms(),
                    )
                    if response and response.status in (403, 412, 429):
                        raise RuntimeError(f'直播页拒绝访问：HTTP {response.status}')
                    await page.locator('.chat-input:visible').wait_for(
                        state='visible', timeout=remaining_page_ms()
                    )
                    await page.locator('button.send-btn:visible').wait_for(
                        state='visible', timeout=remaining_page_ms()
                    )

                    send_budget = min(SEND_CHAT_BUDGET_SECONDS, overall_deadline - monotonic_fn())
                    if send_budget <= 1.0:
                        raise TimeoutError('发送前剩余执行预算不足')

                    save_state(state_path, {
                        **state,
                        'status': 'sending',
                        'room_id': config['room_id'],
                        'attempt_started_at': float(now_fn()),
                        'pending_message': message,
                        'pending_live_start': effective_live_start,
                    })
                    send_entered = True
                    ack = await _await_bounded_send(
                        send_chat_fn(page, message, room_id=config['room_id']),
                        send_budget,
                    )
                except BaseException as error:
                    err_epoch = float(now_fn())
                    attempted_attr = getattr(error, 'send_attempted', None)
                    was_attempted = (attempted_attr is not False) if send_entered else bool(attempted_attr)
                    if was_attempted:
                        err_local_date = datetime.fromtimestamp(err_epoch, tz).date().isoformat()
                        save_state(state_path, {
                            **state,
                            'status': 'ambiguous',
                            'room_id': config['room_id'],
                            'ambiguous_at': err_epoch,
                            'last_sent_epoch': err_epoch,
                            'last_sent_local_date': err_local_date,
                            'last_live_start': effective_live_start or state.get('last_live_start', ''),
                            'pending_message': message,
                            'last_error': f'{type(error).__name__}: {error}',
                            'send_diagnostics': getattr(error, 'send_diagnostics', []),
                        })
                    elif state_path.exists() or state:
                        restored = {
                            **state,
                            'status': 'idle',
                            'room_id': config['room_id'],
                            'last_error': f'{type(error).__name__}: {error}',
                            'last_error_at': err_epoch,
                        }
                        for transient in ('pending_message', 'pending_live_start', 'attempt_started_at'):
                            restored.pop(transient, None)
                        save_state(state_path, restored)
                    raise

                sent_epoch = float(now_fn())
                sent_local_date = datetime.fromtimestamp(sent_epoch, tz).date().isoformat()
                history = list(state.get('history') or [])[-49:]
                history.append({
                    'sent_epoch': sent_epoch,
                    'local_date': sent_local_date,
                    'live_start': effective_live_start,
                    'message': message,
                })
                done_state = {
                    'status': 'idle',
                    'room_id': config['room_id'],
                    'last_sent_epoch': sent_epoch,
                    'last_sent_local_date': sent_local_date,
                    'last_live_start': effective_live_start,
                    'last_message': message,
                    'last_error': None,
                    'history': history,
                }
                save_state(state_path, done_state)
                linger_budget = min(config['linger_seconds'], max(0.0, overall_deadline - monotonic_fn()))
                if linger_budget > 0:
                    try:
                        await sleep_fn(linger_budget)
                    except Exception:
                        pass
            finally:
                try:
                    await _retire_temp_page(context, page, close_page_fn)
                except Exception as error:
                    cleanup_error = f'{type(error).__name__}: {error}'
                    cleanup_evacuated = bool(getattr(error, 'evacuated', False))
                    if done_state is not None:
                        save_state(state_path, {
                            **done_state,
                            'last_cleanup_error': cleanup_error,
                            'last_cleanup_evacuated': cleanup_evacuated,
                            'last_cleanup_error_at': float(now_fn()),
                        })
                    else:
                        raise
        finally:
            try:
                await asyncio.wait_for(pw_cm.__aexit__(None, None, None), timeout=PW_STOP_RESERVE_SECONDS)
            except Exception:
                pass

    receipt = {
        'id': f"checkin:{config['room_id']}:{int(sent_epoch)}",
        'status': 'sent',
        'room_id': config['room_id'],
        'sent_epoch': sent_epoch,
        'local_date': datetime.fromtimestamp(sent_epoch, tz).date().isoformat(),
        'http_status': ack['http_status'],
        'code': ack['code'],
        'cleanup_failed': cleanup_error is not None,
    }
    if cleanup_error is not None:
        receipt['cleanup_error'] = cleanup_error
        receipt['cleanup_evacuated'] = cleanup_evacuated
    return receipt


def _read_event_payload(args, config):
    if args.event_json is not None:
        return json.loads(args.event_json)
    if args.dry_run and sys.stdin.isatty():
        return {'event': 'live', 'room_id': config['room_id']}
    raw = sys.stdin.read().strip()
    if not raw:
        if args.dry_run:
            return {'event': 'live', 'room_id': config['room_id']}
        raise ValueError('缺少通知事件JSON输入；预检请加 --dry-run')
    return json.loads(raw)


def configure_parser(subparsers):
    checkin = subparsers.add_parser('checkin', help='开播事件触发的短驻静音浏览器打卡助手')
    _add_arguments(checkin)
    return checkin


def _add_arguments(parser):
    parser.add_argument('--config', required=True, type=Path, help='私有打卡JSON配置路径')
    parser.add_argument('--dry-run', action='store_true', help='只读核验CDP登录UID与字数上限，不打开房间页也不写入状态')
    parser.add_argument('--event-json', help='显式传入单次通知JSON（默认从stdin读取Automation通知载荷）')


def main(args=None):
    if args is None:
        parser = argparse.ArgumentParser(description='B站开播短驻静音打卡助手')
        _add_arguments(parser)
        args = parser.parse_args()
    try:
        config = load_checkin_config(args.config)
        payload = _read_event_payload(args, config)
        result = asyncio.run(run_checkin(config, payload, dry_run=args.dry_run))
    except Exception as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
