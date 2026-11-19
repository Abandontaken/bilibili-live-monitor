import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from bilibili_live_monitor import __main__ as cli_main
from bilibili_live_monitor import live_checkin
from bilibili_live_monitor.browser import MUTE_SCRIPT


class _FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def json(self):
        return self._payload


class _FakeLocator:
    def __init__(self, page, selector):
        self._page = page
        self._selector = selector

    async def wait_for(self, *, state='visible', timeout=0):
        self._page.waited_selectors.append((self._selector, state, timeout))

    async def count(self):
        return 1

    async def input_value(self):
        return self._page.draft_value


class _FakePage:
    def __init__(self, url='about:blank', *, goto_status=200):
        self.url = url
        self._closed = False
        self._goto_status = goto_status
        self.draft_value = ''
        self.init_scripts = []
        self.goto_calls = []
        self.waited_selectors = []

    def is_closed(self):
        return self._closed

    async def close(self):
        self._closed = True

    async def add_init_script(self, script):
        self.init_scripts.append(script)

    async def goto(self, url, *, wait_until='load', timeout=0):
        self.url = url
        self.goto_calls.append((url, wait_until, timeout))
        return _FakeResponse({}, status=self._goto_status)

    def locator(self, selector):
        return _FakeLocator(self, selector)


class _FakeRequestAPI:
    def __init__(self, handlers):
        self._handlers = handlers
        self.calls = []

    async def get(self, url, *, timeout=0):
        self.calls.append((url, timeout))
        for prefix, response in self._handlers.items():
            if url.startswith(prefix):
                if callable(response):
                    return response(url)
                return response
        raise AssertionError(f'unexpected URL: {url}')


class _FakeContext:
    def __init__(self, handlers, *, existing_pages=None, goto_status=200):
        self.request = _FakeRequestAPI(handlers)
        self.pages = list(existing_pages) if existing_pages is not None else [_FakePage('about:blank#live-monitor-anchor')]
        self.created_pages = []
        self._goto_status = goto_status

    async def new_page(self):
        page = _FakePage(goto_status=self._goto_status)
        self.pages.append(page)
        self.created_pages.append(page)
        return page


class _FakeBrowser:
    def __init__(self, context):
        self.contexts = [context] if context is not None else []
        self.close_called = False

    async def close(self):
        self.close_called = True
        raise AssertionError('browser.close() must never be called on shared CDP browser')


class _FakePlaywright:
    def __init__(self, browser):
        self.browser = browser
        self.stopped = False
        self.connect_calls = []
        self.chromium = self

    async def connect_over_cdp(self, cdp_url, *, timeout=0):
        self.connect_calls.append((cdp_url, timeout))
        return self.browser

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.stopped = True
        return False


def _write_config(tmp_path, **overrides):
    data = {
        'room_id': '123456',
        'cdp_url': 'http://127.0.0.1:9222',
        'expected_uid': '900001',
        'message': '主播好！我是AI实验打卡，不算比赛～',
        'state_path': str(tmp_path / 'checkin_state.json'),
        'timezone': 'Asia/Shanghai',
        'once_per_day': True,
        'min_interval_seconds': 21600,
        'jitter_seconds': [3.0, 5.0],
        'linger_seconds': 10.0,
        'page_timeout_ms': 20000,
    }
    data.update(overrides)
    cfg_path = tmp_path / 'checkin.json'
    cfg_path.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
    return cfg_path


def _default_handlers(*, uid='900001', is_login=True, room_id=123456, live_status=1,
                      live_time='2026-11-18 20:00:00', danmu_length=80):
    return {
        live_checkin.NAV_URL: _FakeResponse({
            'code': 0 if is_login else -101,
            'data': {'isLogin': is_login, 'mid': int(uid) if uid else 0},
        }),
        live_checkin.ROOM_INFO_URL: _FakeResponse({
            'code': 0,
            'data': {
                'room_id': room_id,
                'short_id': 0,
                'live_status': live_status,
                'live_time': live_time,
            },
        }),
        live_checkin.ROOM_USER_INFO_URL: _FakeResponse({
            'code': 0,
            'data': {'property': {'danmu': {'length': danmu_length}}},
        }),
    }


def test_load_checkin_config_validates_loopback_uid_copy_and_budget(tmp_path):
    cfg = live_checkin.load_checkin_config(_write_config(tmp_path))
    assert cfg['room_id'] == '123456'
    assert cfg['expected_uid'] == '900001'
    assert cfg['messages'] == ['主播好！我是AI实验打卡，不算比赛～']
    worst_total = (
        live_checkin.MAX_EXECUTION_BUDGET_SECONDS
        + live_checkin.CANCEL_DRAIN_SECONDS
        + 2 * live_checkin.ANCHOR_CREATE_BUDGET_SECONDS
        + live_checkin.CLOSE_PAGE_BUDGET_SECONDS
        + live_checkin.EVACUATE_BUDGET_SECONDS
        + live_checkin.PW_STOP_RESERVE_SECONDS
    )
    assert worst_total <= 86.0

    with pytest.raises(ValueError, match='本机回环地址'):
        live_checkin.load_checkin_config(_write_config(tmp_path, cdp_url='http://192.168.1.10:9222'))

    with pytest.raises(ValueError, match='不得包含凭据'):
        live_checkin.load_checkin_config(_write_config(tmp_path, cdp_url='http://user:pw@127.0.0.1:9222'))

    with pytest.raises(ValueError, match='expected_uid'):
        live_checkin.load_checkin_config(_write_config(tmp_path, expected_uid=''))

    with pytest.raises(ValueError, match='未知字段'):
        live_checkin.load_checkin_config(_write_config(tmp_path, typo_field=1))

    with pytest.raises(ValueError, match='超出上限'):
        live_checkin.load_checkin_config(_write_config(tmp_path, message='哈' * 45))

    with pytest.raises(ValueError, match='最坏端到端执行预算'):
        live_checkin.load_checkin_config(
            _write_config(tmp_path, jitter_seconds=[10.0, 20.0], page_timeout_ms=25000, linger_seconds=10.0)
        )


@pytest.mark.asyncio
async def test_non_live_events_skip_immediately_without_cdp_or_state_mutation(tmp_path):
    cfg = live_checkin.load_checkin_config(_write_config(tmp_path))

    def forbidden_pw():
        raise AssertionError('CDP should not be contacted on non-live events')

    for evt in ('offline', 'dashboard'):
        receipt = await live_checkin.run_checkin(
            cfg,
            {'event': evt, 'room_id': '123456'},
            playwright_factory=forbidden_pw,
        )
        assert receipt['status'] == 'skipped'
        assert receipt['reason'] == 'not_live_event'
        assert receipt['id'] == f'skipped:{evt}'
    assert not cfg['state_path'].exists()


@pytest.mark.asyncio
async def test_uid_mismatch_aborts_before_opening_any_tab(tmp_path):
    cfg = live_checkin.load_checkin_config(_write_config(tmp_path))
    ctx = _FakeContext(_default_handlers(uid='777777'))
    pw = _FakePlaywright(_FakeBrowser(ctx))

    async def noop_sleep(_seconds):
        return None

    with pytest.raises(RuntimeError, match='expected_uid不符'):
        await live_checkin.run_checkin(
            cfg,
            {'event': 'live', 'room_id': '123456', 'live_start': '2026-11-18T12:00:00+00:00'},
            playwright_factory=lambda: pw,
            sleep_fn=noop_sleep,
        )
    assert ctx.created_pages == []
    assert pw.stopped is True
    assert pw.browser.close_called is False


@pytest.mark.asyncio
async def test_room_offline_at_preflight_skips_without_opening_tab(tmp_path):
    cfg = live_checkin.load_checkin_config(_write_config(tmp_path))
    ctx = _FakeContext(_default_handlers(live_status=0, live_time='0000-00-00 00:00:00'))
    pw = _FakePlaywright(_FakeBrowser(ctx))

    async def noop_sleep(_seconds):
        return None

    receipt = await live_checkin.run_checkin(
        cfg,
        {'event': 'live', 'room_id': '123456'},
        playwright_factory=lambda: pw,
        sleep_fn=noop_sleep,
    )
    assert receipt['status'] == 'skipped'
    assert receipt['reason'] == 'room_not_live'
    assert ctx.created_pages == []
    assert pw.stopped is True


@pytest.mark.asyncio
async def test_successful_ephemeral_send_mutes_lingers_closes_only_temp_tab_and_deduplicates(tmp_path):
    cfg = live_checkin.load_checkin_config(_write_config(tmp_path))
    existing_live_page = _FakePage('https://live.bilibili.com/654321')
    anchor_page = _FakePage('about:blank#live-monitor-anchor')
    ctx = _FakeContext(_default_handlers(), existing_pages=[anchor_page, existing_live_page])
    pw = _FakePlaywright(_FakeBrowser(ctx))
    sleeps = []
    sent_calls = []
    closed_pages = []
    clock = [1795003200.0]  # 2026-11-18 20:00:00 Asia/Shanghai

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    async def fake_send_chat(page, text, *, room_id):
        # State must already be marked 'sending' before network send!
        disk_state = json.loads(cfg['state_path'].read_text(encoding='utf-8'))
        assert disk_state['status'] == 'sending'
        assert page.init_scripts == [MUTE_SCRIPT]
        sent_calls.append((page.url, text, room_id))
        return {'http_status': 200, 'code': 0, 'room_id': room_id, 'message': text}

    async def fake_close_page(page):
        closed_pages.append(page)
        await page.close()

    receipt = await live_checkin.run_checkin(
        cfg,
        {'event': 'live', 'room_id': '123456', 'live_start': '2026-11-18T12:00:00+00:00'},
        playwright_factory=lambda: pw,
        send_chat_fn=fake_send_chat,
        close_page_fn=fake_close_page,
        sleep_fn=fake_sleep,
        now_fn=lambda: clock[0],
        monotonic_fn=lambda: clock[0],
    )
    assert receipt['status'] == 'sent'
    assert sent_calls == [('https://live.bilibili.com/123456', '主播好！我是AI实验打卡，不算比赛～', '123456')]
    assert len(sleeps) == 2
    assert 3.0 <= sleeps[0] <= 5.0
    assert sleeps[1] == 10.0
    assert len(ctx.created_pages) == 1
    assert closed_pages == [ctx.created_pages[0]]
    assert existing_live_page.is_closed() is False
    assert anchor_page.is_closed() is False
    assert pw.browser.close_called is False
    assert pw.stopped is True

    saved = json.loads(cfg['state_path'].read_text(encoding='utf-8'))
    assert saved['status'] == 'idle'
    assert saved['last_sent_local_date'] == '2026-11-18'
    assert saved['last_live_start'] == '2026-11-18T12:00:00+00:00'

    # Reconnect same session -> skipped via same_live_start without touching CDP
    def forbidden_pw():
        raise AssertionError('cooldown skip must not open CDP')

    dup_same_session = await live_checkin.run_checkin(
        cfg,
        {'event': 'live', 'room_id': '123456', 'live_start': '2026-11-18T12:00:00+00:00'},
        playwright_factory=forbidden_pw,
        now_fn=lambda: clock[0] + 60,
    )
    assert dup_same_session['status'] == 'skipped'
    assert dup_same_session['reason'] == 'same_live_start'

    # Second broadcast on same Shanghai calendar day -> skipped via already_sent_today
    dup_same_day = await live_checkin.run_checkin(
        cfg,
        {'event': 'live', 'room_id': '123456', 'live_start': '2026-11-18T15:00:00+00:00'},
        playwright_factory=forbidden_pw,
        now_fn=lambda: clock[0] + 7200,
    )
    assert dup_same_day['status'] == 'skipped'
    assert dup_same_day['reason'] == 'already_sent_today'

    # Cross-midnight within 6h min_interval -> skipped via min_interval_cooldown
    # 2026-11-18 23:50 CST -> 1795017000.0; +1800s -> 2026-11-19 00:20 CST
    saved['last_sent_epoch'] = 1795017000.0
    saved['last_sent_local_date'] = '2026-11-18'
    cfg['state_path'].write_text(json.dumps(saved), encoding='utf-8')
    dup_cross_midnight = await live_checkin.run_checkin(
        cfg,
        {'event': 'live', 'room_id': '123456', 'live_start': '2026-11-18T16:20:00+00:00'},
        playwright_factory=forbidden_pw,
        now_fn=lambda: 1795018800.0,
    )
    assert dup_cross_midnight['status'] == 'skipped'
    assert dup_cross_midnight['reason'] == 'min_interval_cooldown'


@pytest.mark.asyncio
async def test_pre_send_error_recovers_to_idle_while_mid_send_error_locks_ambiguous(tmp_path):
    cfg = live_checkin.load_checkin_config(_write_config(tmp_path, jitter_seconds=[0.0, 0.0], linger_seconds=0.0))

    async def pre_send_fail(_page, _text, *, room_id):
        err = RuntimeError('输入框含草稿')
        err.send_attempted = False
        raise err

    ctx1 = _FakeContext(_default_handlers())
    with pytest.raises(RuntimeError, match='输入框含草稿'):
        await live_checkin.run_checkin(
            cfg,
            {'event': 'live', 'room_id': '123456'},
            playwright_factory=lambda: _FakePlaywright(_FakeBrowser(ctx1)),
            send_chat_fn=pre_send_fail,
        )
    state1 = json.loads(cfg['state_path'].read_text(encoding='utf-8'))
    assert state1['status'] == 'idle'
    assert ctx1.created_pages[0].is_closed() is True

    # Now simulate a mid-send timeout where /msg/send was already dispatched
    async def mid_send_fail(_page, _text, *, room_id):
        err = TimeoutError('等待回执超时')
        err.send_attempted = True
        err.send_diagnostics = [{'path': '/msg/send', 'allowed': True}]
        raise err

    ctx2 = _FakeContext(_default_handlers())
    with pytest.raises(TimeoutError, match='等待回执超时'):
        await live_checkin.run_checkin(
            cfg,
            {'event': 'live', 'room_id': '123456'},
            playwright_factory=lambda: _FakePlaywright(_FakeBrowser(ctx2)),
            send_chat_fn=mid_send_fail,
        )
    state2 = json.loads(cfg['state_path'].read_text(encoding='utf-8'))
    assert state2['status'] == 'ambiguous'
    assert ctx2.created_pages[0].is_closed() is True

    # Subsequent run must refuse while status == 'ambiguous'
    with pytest.raises(RuntimeError, match='未确认的打卡发送状态'):
        await live_checkin.run_checkin(
            cfg,
            {'event': 'live', 'room_id': '123456'},
            playwright_factory=lambda: pytest.fail('must not connect when ambiguous'),
        )


@pytest.mark.asyncio
async def test_dry_run_verifies_uid_and_danmu_limit_without_creating_tab_or_state_file(tmp_path):
    cfg = live_checkin.load_checkin_config(_write_config(tmp_path))
    ctx = _FakeContext(_default_handlers(danmu_length=40))
    pw = _FakePlaywright(_FakeBrowser(ctx))

    result = await live_checkin.run_checkin(
        cfg,
        {'event': 'live', 'room_id': '123456'},
        dry_run=True,
        playwright_factory=lambda: pw,
    )
    assert result['status'] == 'dry_run'
    assert result['uid_verified'] is True
    assert result['danmu_length'] == 40
    assert result['message_width'] == 34
    assert ctx.created_pages == []
    assert not cfg['state_path'].exists()

    # If room danmaku limit is 20 (< 34), dry-run must fail!
    ctx_tight = _FakeContext(_default_handlers(danmu_length=20))
    with pytest.raises(RuntimeError, match='超出房间弹幕上限'):
        await live_checkin.run_checkin(
            cfg,
            {'event': 'live', 'room_id': '123456'},
            dry_run=True,
            playwright_factory=lambda: _FakePlaywright(_FakeBrowser(ctx_tight)),
        )


def test_cli_stdin_contract_matches_automation_send_notification(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    src_root = str(Path(live_checkin.__file__).resolve().parents[1])
    proc = subprocess.run(
        [sys.executable, '-m', 'bilibili_live_monitor.live_checkin', '--config', str(cfg_path)],
        input=json.dumps({'id': 'n1', 'room_id': '123456', 'event': 'offline'}),
        text=True,
        capture_output=True,
        env={**os.environ, 'PYTHONPATH': src_root},
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    receipt = json.loads(proc.stdout)
    assert receipt['id'] == 'skipped:offline'
    assert receipt['status'] == 'skipped'

    # Verify top-level __main__.py checkin subcommand wiring
    monkeypatch.setattr(
        sys,
        'argv',
        ['bili-live', 'checkin', '--config', str(cfg_path), '--event-json', '{"room_id":"123456","event":"dashboard"}'],
    )
    assert cli_main.main() == 0

    # Empty stdin without --dry-run must exit non-zero instead of firing accidentally
    monkeypatch.setattr(sys, 'stdin', io.StringIO(''))
    rc = live_checkin.main(
        type('Args', (), {'config': cfg_path, 'dry_run': False, 'event_json': None})()
    )
    assert rc == 1


@pytest.mark.asyncio
async def test_sole_page_creates_anchor_before_closing_and_messages_rotate(tmp_path):
    cfg_path = _write_config(
        tmp_path,
        messages=['打卡文案A', '打卡文案B'],
        jitter_seconds=[0.0, 0.0],
        linger_seconds=0.0,
    )
    raw = json.loads(cfg_path.read_text(encoding='utf-8'))
    raw.pop('message', None)
    cfg_path.write_text(json.dumps(raw, ensure_ascii=False), encoding='utf-8')
    cfg = live_checkin.load_checkin_config(cfg_path)
    live_checkin.save_state(cfg['state_path'], {'status': 'idle', 'last_message': '打卡文案A'})

    ctx = _FakeContext(_default_handlers(), existing_pages=[])
    sent = []

    async def fake_send_chat(_page, text, *, room_id):
        sent.append(text)
        return {'http_status': 200, 'code': 0, 'room_id': room_id, 'message': text}

    receipt = await live_checkin.run_checkin(
        cfg,
        {'event': 'live', 'room_id': '123456', 'live_start': '2026-11-18T12:00:00+00:00'},
        playwright_factory=lambda: _FakePlaywright(_FakeBrowser(ctx)),
        send_chat_fn=fake_send_chat,
    )
    assert receipt['status'] == 'sent'
    assert sent == ['打卡文案B']
    # First created page is the room page (closed); second is the keepalive anchor (open)
    assert len(ctx.created_pages) == 2
    assert ctx.created_pages[0].is_closed() is True
    assert ctx.created_pages[1].is_closed() is False
    assert ctx.created_pages[1].url == 'about:blank#live-monitor-anchor'


@pytest.mark.asyncio
async def test_send_timeout_preserves_ambiguous_lock_when_cancelled(tmp_path, monkeypatch):
    import asyncio as _asyncio

    monkeypatch.setattr(live_checkin, 'SEND_CHAT_BUDGET_SECONDS', 1.2)
    cfg = live_checkin.load_checkin_config(
        _write_config(tmp_path, jitter_seconds=[0.0, 0.0], linger_seconds=0.0, page_timeout_ms=5000)
    )
    ctx = _FakeContext(_default_handlers())

    async def hung_send_after_dispatch(_page, _text, *, room_id):
        try:
            await _asyncio.sleep(10)
        except _asyncio.CancelledError as err:
            err.send_attempted = True
            err.send_diagnostics = [{'path': '/msg/send', 'allowed': True}]
            raise

    with pytest.raises(TimeoutError):
        await live_checkin.run_checkin(
            cfg,
            {'event': 'live', 'room_id': '123456'},
            playwright_factory=lambda: _FakePlaywright(_FakeBrowser(ctx)),
            send_chat_fn=hung_send_after_dispatch,
        )
    saved = json.loads(cfg['state_path'].read_text(encoding='utf-8'))
    assert saved['status'] == 'ambiguous'
    assert saved['send_diagnostics'] == [{'path': '/msg/send', 'allowed': True}]
    assert ctx.created_pages[0].is_closed() is True


@pytest.mark.asyncio
async def test_post_send_close_failure_preserves_sent_ledger_and_flags_receipt(tmp_path):
    cfg = live_checkin.load_checkin_config(
        _write_config(tmp_path, jitter_seconds=[0.0, 0.0], linger_seconds=0.0)
    )
    ctx = _FakeContext(_default_handlers())

    async def fake_send_chat(_page, text, *, room_id):
        return {'http_status': 200, 'code': 0, 'room_id': room_id, 'message': text}

    async def failing_close_page(_page):
        raise RuntimeError('CDP Page.close failed')

    receipt = await live_checkin.run_checkin(
        cfg,
        {'event': 'live', 'room_id': '123456', 'live_start': '2026-11-18T12:00:00+00:00'},
        playwright_factory=lambda: _FakePlaywright(_FakeBrowser(ctx)),
        send_chat_fn=fake_send_chat,
        close_page_fn=failing_close_page,
    )
    assert receipt['status'] == 'sent'
    assert receipt['cleanup_failed'] is True
    assert 'CDP Page.close failed' in receipt['cleanup_error']
    # Even when close_page failed, fallback navigated the page out of the live room to the blank anchor!
    assert ctx.created_pages[0].url == 'about:blank#live-monitor-anchor'
    saved = json.loads(cfg['state_path'].read_text(encoding='utf-8'))
    assert saved['status'] == 'idle'
    assert saved['last_live_start'] == '2026-11-18T12:00:00+00:00'
    assert 'CDP Page.close failed' in saved['last_cleanup_error']


@pytest.mark.asyncio
async def test_sole_page_anchor_creation_failure_parks_temp_page_without_closing_last_window(tmp_path):
    cfg = live_checkin.load_checkin_config(
        _write_config(tmp_path, jitter_seconds=[0.0, 0.0], linger_seconds=0.0)
    )

    class _AnchorFailContext(_FakeContext):
        async def new_page(self):
            if len(self.created_pages) >= 1:
                raise RuntimeError('cannot open second tab')
            return await super().new_page()

    ctx = _AnchorFailContext(_default_handlers(), existing_pages=[])

    async def fake_send_chat(_page, text, *, room_id):
        return {'http_status': 200, 'code': 0, 'room_id': room_id, 'message': text}

    receipt = await live_checkin.run_checkin(
        cfg,
        {'event': 'live', 'room_id': '123456'},
        playwright_factory=lambda: _FakePlaywright(_FakeBrowser(ctx)),
        send_chat_fn=fake_send_chat,
    )
    assert receipt['status'] == 'sent'
    assert receipt['cleanup_failed'] is False
    # Sole page was NOT closed; instead it navigated back to the silent blank anchor URL
    assert len(ctx.created_pages) == 1
    assert ctx.created_pages[0].is_closed() is False
    assert ctx.created_pages[0].url == 'about:blank#live-monitor-anchor'


@pytest.mark.asyncio
async def test_caller_cancellation_preserves_pre_vs_post_dispatch_state(tmp_path):
    import asyncio as _asyncio

    cfg = live_checkin.load_checkin_config(
        _write_config(tmp_path, jitter_seconds=[0.0, 0.0], linger_seconds=0.0)
    )
    entered_pre = _asyncio.Event()

    async def pre_dispatch_cancel(_page, _text, *, room_id):
        entered_pre.set()
        try:
            await _asyncio.sleep(10)
        except _asyncio.CancelledError as err:
            err.send_attempted = False
            raise

    task1 = _asyncio.create_task(
        live_checkin.run_checkin(
            cfg,
            {'event': 'live', 'room_id': '123456'},
            playwright_factory=lambda: _FakePlaywright(_FakeBrowser(_FakeContext(_default_handlers()))),
            send_chat_fn=pre_dispatch_cancel,
        )
    )
    await entered_pre.wait()
    task1.cancel()
    with pytest.raises(_asyncio.CancelledError):
        await task1
    assert json.loads(cfg['state_path'].read_text(encoding='utf-8'))['status'] == 'idle'

    entered_post = _asyncio.Event()

    async def post_dispatch_cancel(_page, _text, *, room_id):
        entered_post.set()
        try:
            await _asyncio.sleep(10)
        except _asyncio.CancelledError:
            # Bare CancelledError without send_attempted=False must default to ambiguous!
            raise

    task2 = _asyncio.create_task(
        live_checkin.run_checkin(
            cfg,
            {'event': 'live', 'room_id': '123456'},
            playwright_factory=lambda: _FakePlaywright(_FakeBrowser(_FakeContext(_default_handlers()))),
            send_chat_fn=post_dispatch_cancel,
        )
    )
    await entered_post.wait()
    task2.cancel()
    with pytest.raises(_asyncio.CancelledError):
        await task2
    assert json.loads(cfg['state_path'].read_text(encoding='utf-8'))['status'] == 'ambiguous'


@pytest.mark.asyncio
async def test_hung_send_cleanup_is_bounded_by_cancel_drain_and_locks_ambiguous(tmp_path, monkeypatch):
    import asyncio as _asyncio

    monkeypatch.setattr(live_checkin, 'SEND_CHAT_BUDGET_SECONDS', 1.1)
    monkeypatch.setattr(live_checkin, 'CANCEL_DRAIN_SECONDS', 0.2)
    cfg = live_checkin.load_checkin_config(
        _write_config(tmp_path, jitter_seconds=[0.0, 0.0], linger_seconds=0.0, page_timeout_ms=5000)
    )
    release_hung = _asyncio.Event()

    async def hung_cleanup_send(_page, _text, *, room_id):
        try:
            await _asyncio.sleep(10)
        finally:
            await release_hung.wait()

    with pytest.raises(TimeoutError):
        await live_checkin.run_checkin(
            cfg,
            {'event': 'live', 'room_id': '123456'},
            playwright_factory=lambda: _FakePlaywright(_FakeBrowser(_FakeContext(_default_handlers()))),
            send_chat_fn=hung_cleanup_send,
        )
    saved = json.loads(cfg['state_path'].read_text(encoding='utf-8'))
    assert saved['status'] == 'ambiguous'
    assert saved['send_diagnostics'] == [{'step': 'cancel_drain_timeout'}]
    release_hung.set()


@pytest.mark.asyncio
async def test_real_welcome_send_chat_pre_send_draft_error_recovers_to_idle(tmp_path):
    cfg = live_checkin.load_checkin_config(
        _write_config(tmp_path, jitter_seconds=[0.0, 0.0], linger_seconds=0.0)
    )
    ctx = _FakeContext(_default_handlers())
    orig_new_page = ctx.new_page

    async def new_page_with_draft():
        page = await orig_new_page()
        page.draft_value = '未发送草稿'
        return page

    ctx.new_page = new_page_with_draft
    # Uses default send_chat_fn=welcome.send_chat!
    with pytest.raises(RuntimeError, match='含已有草稿'):
        await live_checkin.run_checkin(
            cfg,
            {'event': 'live', 'room_id': '123456'},
            playwright_factory=lambda: _FakePlaywright(_FakeBrowser(ctx)),
        )
    saved = json.loads(cfg['state_path'].read_text(encoding='utf-8'))
    assert saved['status'] == 'idle'


@pytest.mark.asyncio
async def test_close_page_timeout_still_runs_independent_blank_evacuation(tmp_path, monkeypatch):
    import asyncio as _asyncio

    monkeypatch.setattr(live_checkin, 'CLOSE_PAGE_BUDGET_SECONDS', 0.15)
    cfg = live_checkin.load_checkin_config(
        _write_config(tmp_path, jitter_seconds=[0.0, 0.0], linger_seconds=0.0)
    )
    ctx = _FakeContext(_default_handlers())

    async def fake_send_chat(_page, text, *, room_id):
        return {'http_status': 200, 'code': 0, 'room_id': room_id, 'message': text}

    async def slow_close_page(_page):
        await _asyncio.sleep(5)

    receipt = await live_checkin.run_checkin(
        cfg,
        {'event': 'live', 'room_id': '123456', 'live_start': '2026-11-18T12:00:00+00:00'},
        playwright_factory=lambda: _FakePlaywright(_FakeBrowser(ctx)),
        send_chat_fn=fake_send_chat,
        close_page_fn=slow_close_page,
    )
    assert receipt['status'] == 'sent'
    assert receipt['cleanup_failed'] is True
    assert receipt['cleanup_evacuated'] is True
    assert ctx.created_pages[0].url == 'about:blank#live-monitor-anchor'




