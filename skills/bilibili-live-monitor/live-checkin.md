# 开播事件驱动的短驻静音打卡助手

## 适用场景与定位

`bili-live checkin`（或 `python -m bilibili_live_monitor.live_checkin`）用于在不改动匿名直连采集（`CAPTURE_BACKEND=direct`）与常驻名额的前提下，复用本机已登录的小号 Chrome（CDP），在目标房间确认开播时短驻打开静音临时标签页发送一条打卡弹幕，停留数秒后关闭该临时标签页并断开 Playwright 连接。

适用前提：
- 仅在用户明确授权目标房间号、小号 UID 与精确打卡文案后配置启用。
- 复用 `Automation` 逐房 `notification_command` 的 `stdin` JSON / `stdout` JSON 回执契约，通过 `POST /api/automation/reload` 热加载接入，采集进程零重启。
- 对非开播事件（`offline`、`dashboard` 等）在无锁路径毫秒级返回 `{"id": "skipped:...", "status": "skipped"}` 并以 `exit 0` 退出。

## 安全门禁与幂等防重

1. **回环地址与分阶段超时预算**：`cdp_url` 仅接受 `127.0.0.1`、`localhost` 或 `::1` 且禁止携带凭据；业务阶段共享统一单调时钟预算（`jitter_max + 10s(预检) + page_timeout_ms / 1000 + 16s(发送) + linger_seconds <= 64s`），并独立分配 2 秒取消收敛、3 秒单页保活、11.5 秒双级关页、2 秒空白页撤离与 3 秒 Playwright 断连预算（累计最坏 `85.5s`，预留 4.5 秒进程启动与 JSON 回执余量，对齐 `Automation.send_notification` 90 秒上限）。
2. **进房前双接口核验（零误露面）**：在调用 `context.new_page()` 打开任何直播间页面之前，先通过 `context.request.get` 核验：
   - `https://api.bilibili.com/x/web-interface/nav` 必须返回 `isLogin == true` 且 `str(mid) == expected_uid`。若当前浏览器切到了大号或未登录，立即报错中止，绝不打开房间触发进房特效。
   - `https://api.live.bilibili.com/room/v1/Room/get_info?room_id=<room_id>` 必须确认规范房号一致且 `live_status == 1`，若已下播则直接返回 `skipped:room_not_live`。
3. **短驻静音页与浏览器保护**：仅新建独立临时页，在 `goto` 前通过 `add_init_script(MUTE_SCRIPT)` 强制静音，复用 `welcome.send_chat` 的输入框草稿检查、Service Worker 临时绕过与 `/msg/send` 字段精确拦截；停留 `linger_seconds` 后在 `finally` 中仅关闭该临时页（若为最后一张页面则先补齐 `about:blank#live-monitor-anchor` 保活页），通过 `pw.stop()` 断开本地连接，严禁调用 `browser.close()`。
4. **多重防重与未决锁定**：基于 `fcntl.flock` 文件锁与原子落盘状态文件（`state_path`），同时执行同 `live_start` 去重、按 `timezone` 自然日每日最多一次（`once_per_day: true`）以及最小时间间隔冷却（默认 `min_interval_seconds: 21600`）。发送前先落盘 `status: "sending"`，若 `/msg/send` 已发出但回执超时或业务码非零则持久锁定为 `status: "ambiguous"`，需人工核验后方可恢复；进房或发送前的确定性未外发错误则自动恢复为 `status: "idle"`。

## 私有配置与接入示例

以下 `/PRIVATE/...`、房间号 `123456` 与小号 UID `900001` 均为虚构占位示例，真实配置与状态文件只存放在 gitignored 私有目录中：

```json
{
  "room_id": "123456",
  "cdp_url": "http://127.0.0.1:9222",
  "expected_uid": "900001",
  "message": "主播好！我是AI实验打卡，不算比赛～",
  "state_path": "/PRIVATE/checkin_123456_state.json",
  "timezone": "Asia/Shanghai",
  "once_per_day": true,
  "min_interval_seconds": 21600,
  "jitter_seconds": [3.0, 8.0],
  "linger_seconds": 10.0,
  "page_timeout_ms": 20000
}
```

需要多条文案相邻场次不重复轮换时，可将 `"message"` 替换为非空字符串数组 `"messages"`。

### 1. 上线前只读预检（`--dry-run`）

```bash
bili-live checkin --config /PRIVATE/checkin_123456.json --dry-run
```

`--dry-run` 会连接本机 CDP 核验 `expected_uid`、房间规范房号与当前账号在该房间的弹幕字数上限，检查本地状态账本是否处于未决锁定或当日冷却，全程不打开任何标签页、不发送弹幕、不改写状态文件。

### 2. 挂载逐房 `notification_command` 并热加载

在匿名或实名实例的私有 `automation.json` 中，为目标房间单独配置 `notification_command`（可配合 `release_entry.py` 绑定独立 `checkin` 组件）：

```json
{
  "rooms": {
    "123456": {
      "label": "示例主播",
      "intro": "仅匿名监控，不发送欢迎。",
      "welcome_enabled": false,
      "content_mode": "talk",
      "record_mode": "audio",
      "local_only": true,
      "notification_command": [
        "/PRIVATE/venv/bin/python",
        "-m",
        "bilibili_live_monitor.live_checkin",
        "--config",
        "/PRIVATE/checkin_123456.json"
      ],
      "followup_rules": {}
    }
  }
}
```

保存后执行热加载使配置立即对下一次开播生效，无需重启采集服务：

```bash
curl -fsS -X POST http://127.0.0.1:8766/api/automation/reload
```
