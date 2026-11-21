# 观众弹幕档案

当用户要按 UID 回看单个直播间的曾用昵称和逐场弹幕时使用。它是原文索引，不生成画像、关系判断或总结，也不做匿名身份消解。

## 输入与输出

`--database` 指向采集库 `monitor.sqlite3`，只读 `timeline`；`--room-id` 使用采集库里的房号。`--sessions` 指向可信的同房归档，例如 `{"room_id":"123456","sessions":[{"status":"completed","start_utc":"2030-01-01T00:00:00Z","updated_at":"2030-01-01T02:00:00Z"}]}`。已有单房场次索引可以没有顶层 `room_id`，此时先核对其来源。只使用 `completed` 场次，两端均含在场内。不要用采集连接 ID 分场：同场重连可能产生多个 ID。

```bash
bili-live audience-log --database "$MONITOR_DB" --sessions "$ROOM_ARCHIVE" \
  --room-id 123456 --title '示例房间 · 观众弹幕档案' \
  --json-out .local/audience-log/audience.json --html-out .local/audience-log/index.html
```

任选一种输出即可。JSON `schema_version: 1` 包含 `room_id`、`title`、`timezone`、`sessions`（`start` / `end`）、`people`（`uid` 字符串、按最后观测倒序的 `names`、`message_count`、倒序的 `sessions`）及 `stats`。个人场次的 `session` 是全局场次数组的零基下标，`messages` 按事件时间排序，每条含 `at`、`name`（可为空）、`text`。时间带偏移，默认显示北京时间，可用 `--timezone` 换成其他 IANA 时区。网页内嵌同一份数据，可以直接从磁盘打开，左侧滚动自动加载，右侧可一键展开或收起全部。

## 验收与边界

- 用户列表只包含在所选范围至少有一条可归属弹幕的 UID。同名不同 UID 不合并；同一 UID 的多个实录昵称均保留。已入选的人如果某场只有进房、点赞或礼物记录，显示「无弹幕」，不要把未观察到的场次当作缺席。
- 核对 CLI 打印的场次、人数和弹幕量；`unattributed_message_count` 和 `outside_session_message_count` 分别记录场内无法归属、场次区间外的弹幕。抽查旧昵称搜索、场次边界和弹幕原文；空结果应如实呈现。
- 真实房间参数见使用者的私有 overlay。JSON 和网页都包含真实 UID 和原文，默认留在私有目录。本命令不发布，也不更改归档主页；若需要远程访问，单独确认访问控制和数据范围。`noindex` 不能替代鉴权。
