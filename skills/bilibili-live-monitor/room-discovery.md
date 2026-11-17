# 扩房发现与分层滚雪球采样

## 适用场景与定位

`bili-live room-discovery` 用于从已有跨房身份数仓出发，按观众全站荣耀等级分层抽样拉取公开关注列表，结合免登录跨赛道候选补充、近 14 天两级开播活跃度核验与舰长规模分档，产出均衡的扩房候选方案，供人工审阅后批量导入 `automation.json`。

整个工具链纯离线运行，不触碰常驻采集进程或看板网关：
- 身份数仓 `--warehouse-db` 始终以 `?mode=ro` 与 `PRAGMA query_only=ON` 只读打开，发现中间状态与候选表独立写入 `--db`（`discovery.sqlite3`），对数仓与采集库零锁争用。
- 带登录态的关注列表拉取默认处于 `--dry-run` 预演模式，仅在显式传入 `--execute` 时运行；阶段二筛房核验与阶段三抽样计划全程免登录（阶段三零网络请求）。

## 协议约束与方法论依据

### 1. 为什么不能用外房粉丝牌扩房
实测表明，约 85% 的直播间开启了「仅展示本房间粉丝勋章」，平台服务端在下发 `DANMU_MSG`、`ENTRY_EFFECT` 与 `LIKE_INFO_V3_CLICK` 报文前已直接清空外房 `medal` 字段。若依赖弹幕流中的外房粉丝牌做滚雪球扩房，样本会完全倒向少数未开启勋章过滤的种子房受众圈。无偏扩房必须从数仓解出的全量活跃实名观众出发，按荣耀等级分层拉取公开关注关系。

### 2. `/x/relation/followings` 第三方分页上限与隐私锁码
- 未登录请求该接口固定返回 `code = -101`（账号未登录），必须使用单个独立小号登录态查询。
- 当查询第三方用户（`vmid` 非本人）时，无论传入 `order=desc`、`order_type=attention`（最常访问）还是 `order=asc`，服务端均按最近关注时间倒序返回完全相同的前 2 页（每页 50 条，上限 100 条），`pn >= 3` 固定返回空列表 `[]`。因此每位公开观众最多只需请求 `pn=1..2` 两页（第 1 页不足 50 条或 `total <= 50` 时 1 页即停）。
- 当目标观众隐藏关注列表时，接口返回 `code = 22115`（或 `22013`）。程序将其记入 `ext_viewer_follow_status`（`status = 'hidden'`），后续续跑自动跳过，不重复消耗请求配额。

### 3. 下播后 `live_time` 清零陷阱与两级 14 天活跃度判据
主播一旦下播（`live_status` 为 `0` 或 `2`），`Room/get_info` 与 `getRoomBaseInfo` 中的 `live_time` 会立即归零为 `"0000-00-00 00:00:00"`，旧版网页录播列表接口已下线（HTTP 404），房间历史弹幕与七日榜在未开播状态下亦常为空。

为兼顾留档主播与不留回放的歌势/虚拟主播，同时剔除退网残余舰长账号与不直播的纯动态官号，`screen-candidates` 采用两级近 14 天活跃度判据（满足任一即记 `active_14d = 1` 并记录 `active_evidence`）：
1. **当前在播（`live_now`）**：`live_status == 1`。
2. **第一级精确直播信号（`live_replay` / `live_reservation`）**：
   - 空间合集接口 `/x/polymer/web-space/seasons_series_list?mid=<uid>` 存在直播回放合集（`category == 2` 或名称含「直播回放」），且最新回放稿件发布时间落在近 14 天窗口内；
   - 或空间预约接口 `/x/space/reservation?vmid=<uid>` 存在近 14 天内或未来 14 天内的直播预约。
3. **第二级不留回放主播兜底（`guard_plus_recent_opus`）**：必须**同时满足**当前大航海在舰人数 `guard_count > 0`（来自免登录 `xlive/app-room/v2/guardTab/topList` 的 `info.num`），且游客 WBI 查询 `/x/polymer/web-dynamic/v1/opus/feed/space?host_mid=<uid>` 存在近期动态满足雪花时间戳公式：
   $$\text{opus\_ts} = (\text{int}(\text{opus\_id}) \gg 32) + 1498838400 \ge \text{now} - 14 \times 86400$$
   双重条件可召回不留直播回放但在空间发布歌单、排班表或下播打卡的活跃主播，同时排除 `guard_count == 0` 的纯发动态官号与无近期动态的退网残余舰长号。

### 4. 多人公会厅与轮播透明标记
`screen-candidates` 对房间标题、昵称、简介、认证说明与标签检测多人厅房及无人轮播特征词（`点唱厅`、`交友厅`、`考核厅`、`语音厅`、`男模`、`女模`、`主持人`、`24小时`、`24h`、`轮播`、`无人值守`、`助眠挂机`），命中记录写入 `dim_candidate_rooms.hygiene_flag`（如 `flagged:点唱厅`，未命中记 `clean`），保留在表中供审计，不在入库阶段静默删除。

## 小号 Cookie 安全红线

1. **纯内存读取与无损断开**：`--cdp-url` 仅允许连接本机回环地址，通过 Playwright 只读提取 `bilibili.com` 域 Cookie 到内存后立即调用 `pw.stop()` 断开本地连接，严禁调用 `browser.close()` 或打开、关闭、刷新任何标签页；也可通过 `--cookie-file` 读取本地私有 Cookie JSON 或 Header 文件。
2. **零落盘与零脱敏遗漏**：`SESSDATA`、`bili_jct` 以及小号本人的 UID、昵称严禁写入 SQLite、JSON 输出、终端日志或异常消息，请求结束后立即清空内存 Cookie 容器。
3. **双 URL 硬白名单与物理隔离**：携带登录态的客户端禁用重定向跟随，且硬编码仅允许请求 `https://api.bilibili.com/x/web-interface/nav` 与 `https://api.bilibili.com/x/relation/followings`。后续批量查房、分区抓取、大航海统计、回放预约与动态核验全部走独立免登录游客客户端（若检测到登录态立即报错中止）。
4. **低频抖动与即时熔断**：带登录态请求默认基准间隔 `--auth-interval 2.5` 秒并叠加随机抖动（低于 `0.44 QPS`）。一旦遇到 HTTP `412`/`429` 或业务码 `-352`/`-412`/`-101`，立即触发熔断停机并保留每位已完成观众的落盘进度，绝不循环重试。

## 三步命令用法

以下路径 `/PRIVATE/...`、房间号 `123456` / `654321` 与 UID `100001` 均为虚构占位示例：

```bash
# 第一步（预演）：从只读数仓按大R(>=40)/中R(25..39)/小R(10..24)分层抽样，估算请求配额与耗时
bili-live room-discovery pull-followings \
  --warehouse-db /PRIVATE/live_warehouse.sqlite3 \
  --db /PRIVATE/room_discovery.sqlite3 \
  --per-tier 50 \
  --seed 20261117 \
  --dry-run

# 第一步（执行）：只读提取本机小号浏览器 Cookie 拉取公开关注前 2 页，支持随时中断续跑
bili-live room-discovery pull-followings \
  --warehouse-db /PRIVATE/live_warehouse.sqlite3 \
  --db /PRIVATE/room_discovery.sqlite3 \
  --per-tier 50 \
  --seed 20261117 \
  --cdp-url http://127.0.0.1:9358 \
  --auth-interval 2.5 \
  --execute

# 第二步（全程免登录）：内生共现筛房 + 外生跨赛道补齐 + 厅房标记 + 两级 14 天活跃度与舰长分档
bili-live room-discovery screen-candidates \
  --db /PRIVATE/room_discovery.sqlite3 \
  --warehouse-db /PRIVATE/live_warehouse.sqlite3 \
  --exclude-automation-config /PRIVATE/automation.json \
  --max-endogenous 300 \
  --exogenous-per-area 15 \
  --parent-areas 虚拟主播,娱乐,电台,单机游戏,网游 \
  --active-days 14

# 第三步（纯本地零网络）：按 70% 内生 + 30% 外生及 (一级大区, 舰长档位) 均衡导出 60 房抽样计划
bili-live room-discovery sample-plan \
  --db /PRIVATE/room_discovery.sqlite3 \
  --warehouse-db /PRIVATE/live_warehouse.sqlite3 \
  --exclude-automation-config /PRIVATE/automation.json \
  --exclude-rooms 123456,654321 \
  --exclude-uids 100001 \
  --target-count 60 \
  --endogenous-ratio 0.7 \
  --max-per-subarea 8 \
  --seed 20261117 \
  --output /PRIVATE/expansion_plan.json
```

## 发现库核心表速查（`room_discovery.sqlite3`）

- `ext_viewer_follow_status`：记录已抽样观众 `viewer_uid`、荣耀分层 `wealth_tier`（`high_r` / `mid_r` / `low_r`）、可见状态 `status`（`public` / `hidden` / `error`）、声明关注总数 `reported_total` 与实际拉取条数 `fetched_count`。
- `ext_viewer_followings`：记录公开观众与被关注账号的有向边 `(viewer_uid, followed_uid)`、关注时间 `followed_mtime` 及被关注账号昵称、认证说明与签名摘要。
- `dim_candidate_rooms`：记录候选主播 `anchor_uid`、规范房号 `room_id`、短号 `short_id`、推荐采集主键 `capture_room_id`、一级/二级分区、来源通道 `source_channel`（`endogenous` / `exogenous` / `both`）、各层共现关注人数、在舰数 `guard_count`、舰长档位 `guard_tier`（`head_200_plus` / `mid_30_199` / `tail_1_29` / `zero_0`）、清洗标记 `hygiene_flag`、活跃布尔值 `active_14d` 与证据枚举 `active_evidence`。
