# 数据库解析与口径防坑指南

本文档面向读取 `live_warehouse.sqlite3` 或 `monitor.sqlite3` 的分析脚本与 AI 操作者，定义双库职责边界、核心表与原始报文路径、八类常见口径误判及标准只读 SQL 模板。文中路径 `/PRIVATE/...`、房间号 `123456` / `654321` 与 UID `880001` 均为虚构示例。

## 一、双库定位与优先查询原则

所有离线统计、用户画像与跨房分析均须以只读方式打开数据库（SQLite URI 附加 `?mode=ro` 并执行 `PRAGMA query_only = ON`），严禁在分析脚本中向源采集库执行写入或建表操作。

| 查询目标 | 优先读取的数据库 | 原因与边界 |
|---|---|---|
| 跨房观众身份消解、串门重合度、付费去重人数 | `live_warehouse.sqlite3` | `v_resolved_events` 已晚绑定明文 UID、连击无盐 MD5、唯一自定义头像 SHA1、匿名弹幕 CRC32+首字及榜单跳变推断，自动去重成对广播。 |
| 单笔付费前后语音/歌曲归因、同屏在线与 PK/连麦上下文 | `live_warehouse.sqlite3` | `fact_transcripts`（优先采用终审修订文本）、`fact_room_metrics` 与 `fact_cohost_intervals` 已按统一秒级时间戳落表并建立复合索引。 |
| 大/中/小 R 全站氪金分层、纵向养成轨迹、沉默大哥对照 | `live_warehouse.sqlite3` | `dim_users.max_wealth_level` 与 `fact_session_presence` 已完成跨场荣耀等级与人×场次行为聚合。 |
| 尚未下播收尾（未达 `completed`）的实时场次监控 | `monitor.sqlite3` | 数仓默认仅摄取已完成收尾的场次；直播进行中的实时弹幕与流式 ASR 需读取源库 `timeline` 与 `segments`。 |
| 新增未知 WebSocket 命令调试、离线特效与原始报文逐字节核验 | `monitor.sqlite3` | 源库 `raw_events` 原样保留全量 WebSocket 业务报文，供协议扩展排障与特效订单绑定回查。 |

分析前若近期有新下播场次或新增明文曝光，先执行一次幂等增量同步 `bili-live warehouse sync`（用法见 [跨房全局身份消解数仓](warehouse.md)），再针对 `live_warehouse.sqlite3` 运行查询。

## 二、核心表与协议字段速查字典

### 2.1 数仓核心表与视图（`live_warehouse.sqlite3`）

所有时间戳列统一为 UTC Unix 秒（`REAL` 或分钟整点 `INTEGER`）；所有金额列统一为千分元整数（`INTEGER`，`1000 = 1 元人民币`）。

- **`v_resolved_events`（基于 `fact_events` 的晚绑定消解视图，事件明细查询主入口）**
  - 时间与分类：`session_key`、`event_seq`、`room_id`、`event_ts`（UTC 秒）、`time_source`（`'event'` 协议自带时间戳 / `'received'` 本机接收时钟）、`kind`（`'gift'` / `'guard'` / `'sc'` / `'danmaku'` / `'entry'` / `'like'`）。
  - 身份消解结果：`resolved_uid`（解出的正整数 UID；神秘人或未解出时为 `NULL`）、`actor_key`（统一聚合标识：`uid:<uid>`、`md5:<32hex>`、`crc32:<room_id>:<8hex>:<首字>`、`mystery:...` 或 `anon:...`）、`resolution_basis`（消解依据：`'native_uid'`、`'global_md5'`、`'crc32_first_char'`、`'unique_face'`、`'rank_delta'`、`'ambiguous_crc32'`、`'unresolved_md5_pseudonym'`、`'mystery'`、`'unresolved'`）。
  - 原始协议线索：`is_mystery`（`1` 为开启神秘人隐藏）、`raw_uid`、`raw_uname`、`clue_first_char`、`clue_md5`（32 位小写无盐 MD5）、`clue_face_sha1`（40 位自定义头像 SHA1；弹幕在 `raw_uid` 与 `resolved_crc32_uid` 未命中时，若存在匹配的 `clue_crc32` 双哈希或无 `clue_crc32` 时首字无冲突，则通过 `v_unique_face_uid` 自动认领，仍未解出时保留该列供同场或跨场盲盒与弹幕关联）、`clue_crc32`（8 位小写 CRC32）。
  - 财富与勋章快照：`wealth_level`（当刻全站荣耀等级）、`guard_level`（`1`=总督，`2`=提督，`3`=舰长）、`medal_ruid`（所戴勋章归属主播 UID）、`medal_room_id`、`medal_name`、`medal_level`。
  - 付费与内容载荷：`paid_milli_yuan`（消费者实付千分元；银瓜子恒为 `0`，盲盒取原盒投入价×数量，大航海优先取 `USER_TOAST_MSG_V2` 实付折扣价）、`output_milli_yuan`（开出或展示面值千分元，对齐 `gift-output-v1`）、`coin_type`（`'gold'` / `'silver'`）、`gift_id`、`gift_name`、`blind_orig_name`、`quantity`、`content_text`（弹幕正文或 SC 留言）、`source_ref`（如 `raw:1024:0`）。
- **`dim_users` 与 `v_unique_face_uid`（全局跨房用户字典）**
  - `dim_users`：`uid`（主键）、`uid_md5`、`uid_crc32`、`latest_uname`、`first_char`、`max_wealth_level`（跨房历史最高荣耀等级）、`first_seen_ts`、`last_seen_ts`。
  - `dim_rooms`：`room_id`（规范长房号主键）、`anchor_uid`（本房主播 UID）、`short_room_id`（B 站靓号短房号，无短号时为 `NULL`）、`room_label`（主播名称）、`track_category`（赛道分类，如 `'virtual_music'` / `'virtual_gaming'` / `'virtual_talk'`，未分类为 `NULL`）、`allows_ext_medal`（`1`=实测允许展示外房粉丝牌，`0`=服务端已剥离外房牌或未观测到外房牌）。
  - `v_unique_face_uid`：从 `dim_user_faces` 聚合筛出全局仅绑定唯一 1 个 `uid` 的 `face_sha1`。
- **`fact_transcripts`（终审语音与演唱分段表）**
  - 主键 `(session_key, seq)`；`start_ts`、`end_ts`（UTC 秒）；`activity_label`（`'singing'` / `'speaking'` / `'mixed'` / `'unknown'`）；`music_source`（`'probable_live'` 现场演唱 / `'recording'` 播放录音 / `'uncertain'` / `'none'`）；`song_title`（已核验曲名）；`text`（优先取音乐或杂谈终审精修后的 `corrected_text`）。
- **`fact_session_presence`（人×场次汇总宽表）**
  - 主键 `(session_key, actor_key)`；记录单场内该观众的 `resolved_uid`、`first_event_ts`、`last_event_ts`、`entry_count`、`danmaku_count`、`like_count`、`gift_event_count`、`paid_milli_yuan`、`output_milli_yuan`、`max_wealth_level`、`guard_level` 及场内末次非空勋章 `medal_ruid` / `medal_name` / `medal_level`。
- **`fact_room_metrics`（每分钟房间状态时间序列）**
  - 主键 `(session_key, minute_ts)`，其中 `minute_ts = int(ts // 60) * 60`（UTC 整分秒戳），取该分钟桶内最新有效观测：`online_count`（`ONLINE_RANK_COUNT.data.online_count` 同屏高能在线人数）、`watched_count`（`WATCHED_CHANGE.data.num` 累计看过人次）、`fans_count`（`ROOM_REAL_TIME_MESSAGE_UPDATE.data.fans` 关注数）、`fans_club_count`（`data.fans_club` 粉丝团人数，缺测保留 `NULL`）。
- **`fact_cohost_intervals`（PK 对战、惩罚与语音连麦状态区间表）**
  - 主键 `(session_key, interval_seq)`；`kind` 取 `'pk'`（PK 准备与正式拉票对战期）、`'pk_punish'`（PK 惩罚连麦互动期）、`'voice_join'`（语音连麦在麦通话期）或 `'voice_join_queue'`（排麦等待期）；`start_ts`、`end_ts` 为裁剪至本场 `[start_ts, cutoff_ts]` 内的 UTC 秒；`battle_id` 记录 `pk_id` 或连麦 `channel`；`opponent_uid`、`opponent_room_id`、`opponent_uname` 记录对手主播或连麦对象；`raw_summary_json` 保存 `battle_start_ts`、`sprint_start_ts`（最后读秒冲刺起点）、`sprint_duration`、双方 `votes`、`self_winner` 及触发命令列表。

### 2.2 源库 `raw_events` 原始协议字段路径速查（`monitor.sqlite3`）

回查 `raw_events.data`（JSON 文本）时，五类高频业务报文的精确提取路径如下：

- **`DANMU_MSG`（顶层 `info` 数组）**
  - `info[0][4]`：弹幕发送时间戳（毫秒整数）。
  - `info[0][7]`：发送者 UID 的无盐 `CRC32` 8 位十六进制字符串（神秘人时为 `'00000000'`）。
  - `info[0][15]`：扩展字典（或 JSON 字符串），`user.base.face`（或同层 `extra` JSON 内的 `user.base.face`）在未登录采集下约 98.6% 携带未脱敏真实头像 URL；`user.base.is_mystery` 标识神秘人。
  - `info[1]`：弹幕文本内容。
  - `info[2]`：`[uid, uname, ...]`；未登录态下 `info[2][0]` 为 `0`，`info[2][1]` 为保留首字的打码昵称（如 `'星***'`）。
  - `info[3]`：粉丝牌数组 `[medal_level, medal_name, anchor_uname, anchor_roomid, ..., guard_level(下标10), is_lighted(下标11), anchor_ruid(下标12)]`；未佩戴或遭本房过滤时为空数组 `[]`。
  - `info[4]`：`[ul_level, ...]` 用户全站经验等级 `UL`。
  - `info[7]`：本房大航海等级（`0`/`1`/`2`/`3`）。
  - `info[16]`：`[wealth_level, ...]` 全站荣耀等级（未登录态下明文保留）。
- **`SEND_GIFT_V2`（`data.pb` Base64 编码 Protobuf，对应 `warehouse_protocol.py` 中 `WareGift`）**
  - 顶层：`1=uid`、`2=uname`、`3=face`（头像 URL）、`4=batch_combo_id`、`8=medal_info`（`1=target_id` 主播 UID、`5=medal_level`、`6=medal_name`、`11=guard_level`、`12=anchor_roomid`）、`19=is_mystery`。
  - `9=blind_gift`（盲盒结构）：`2=original_gift_id`、`3=original_gift_name`、`6=original_gift_price`（盲盒原盒单价，千分元/金瓜子）。
  - `10=items`（repeated 原子结果明细）：`1=gift_id`、`2=gift_name`、`3=num`、`5=price`（单价或盲盒开出结果单价）、`7=total_coin`、`8=coin_type`（`'gold'`/`'silver'`）、`9=tid`（订单号）、`10=timestamp`、`12=batch_combo_id`（格式 `batch:gift:combo_id:<32位MD5(uid)或实名UID>:<ruid>:<gift_id>:<ts>`）。
  - `15=sender`（发送者 `uinfo`）：`1=uid`、`2.1=base.name`、`2.2=base.face`（自定义头像 URL）、`2.4=base.is_mystery`、`3=medal`（`1=name`、`2=level`、`10=ruid`、`11=guard_level`）、`4.1=wealth.level`、`6.1=guard.level`。
- **`ONLINE_RANK_V3`（`data.pb` Base64 Protobuf `WareRank` 或同结构 JSON）**
  - `1=rank_type`（如 `'gold-rank'`）、`2=list`、`3=online_list`。即使在未登录态下，榜单内 `RankUser` 仍下发明文 `1=uid`、`2=face`、`3=score`（本场贡献值字符串，`1 贡献值 = 100 金瓜子 = 0.1 元`）、`4=uname`、`6=guard_level` 及 `8=uinfo`（`8.4.1=wealth.level`）。
- **`ENTRY_EFFECT` / `ENTRY_EFFECT_MUST_RECEIVE`（JSON `data`）**
  - 未登录态下仍下发明文 `data.uid`、`data.target_id`（本房主播 UID）、`data.privilege_type`（航海等级）、`data.copy_writing`（含 `<%完整昵称%>`）与 `data.uinfo`（`uid`、`base.name`、`base.face`、`wealth.level`、`medal`）。
- **`LIKE_INFO_V3_CLICK`（JSON `data`）**
  - 未登录态下仍下发明文 `data.uid`、`data.uname`、`data.uinfo`（`uid`、`base.name`、`base.face`、`wealth.level`、`medal`、`guard.level`）与 `data.fans_medal`。

## 三、八大必防数据解释陷阱

### 陷阱 1：服务端外房粉丝牌剥离
多数直播间开启了仅展示本房勋章开关，平台服务端在下发弹幕、入场与点赞报文前已直接清空外房 `medal` 字段，并非前端遮罩隐藏。当 `dim_rooms.allows_ext_medal = 0` 时，严禁把观众 `medal_ruid IS NULL` 判读为专一单推人或无外房牌新粉；跨房偏好只能依据多房真实共现记录（`fact_session_presence` 跨 `room_id` 聚合）或外部关注列表 `ext_user_followings` 判断。

### 陷阱 2：短房号与规范长房号不一致
老号段直播间的采集配置键 `room_id` 常为页面短号（如 `123456`），而粉丝勋章 `anchor_roomid` 与 `PK_INFO.data.members[].room_id` 返回平台规范长房号（如 `12345600`）。判断观众是否佩戴本房勋章时，严禁直接用 `medal_room_id = room_id` 比对，一律比对勋章归属主播 UID：`medal_ruid = dim_rooms.anchor_uid`。

### 陷阱 3：盲盒双套金额账本与银瓜子混淆
免费礼物（`coin_type = 'silver'`）不产生真金白银消费；盲盒礼物同时存在投入成本与开出面值两套不同账本。分析消费者真实付费意愿、客单价与大中小 R 消费额时，一律使用 `paid_milli_yuan`（盲盒按 `original_gift_price * quantity` 计，银瓜子为 `0`）；分析直播间开盲盒盈亏刺激、节目效果或对齐前端看板展示总额时，使用 `output_milli_yuan`。两套金额列严禁混加或相互替代。

### 陷阱 4：`UL` 用户等级注水与 `wealth_level` 荣耀等级区分
`DANMU_MSG` 中 `info[4][0]` 的用户等级 `UL` 受早年免费银瓜子与挂机时长影响，高 `UL` 并不代表高付费能力。划分大/中/小 R 全站氪金底盘时，一律使用 `wealth_level`（单事件快照）或 `dim_users.max_wealth_level` / `fact_session_presence.max_wealth_level`（全站荣耀等级）。

### 陷阱 5：直播间人数三口径混淆
旧版房间状态接口的 `online` 字段为平台加权人气值，并非人数；`WATCHED_CHANGE.data.num`（对应 `fact_room_metrics.watched_count`）是本场开播以来的累计看过人次，单调递增不随观众离开回落；衡量当刻同屏真实高能在线人数只看 `ONLINE_RANK_COUNT.data.online_count`（对应 `fact_room_metrics.online_count`）。

### 陷阱 6：成对广播重复记账
原始 `raw_events` 中，连击送礼会同时收到逐次原子 `SEND_GIFT`/`SEND_GIFT_V2` 与累计汇总 `COMBO_SEND`；开通或续费大航海会成对收到 `GUARD_BUY`（固定原价，如舰长 `198000` 千分元）与 `USER_TOAST_MSG`/`USER_TOAST_MSG_V2`（含续费实付价，如 `138000` 千分元）。直接对 `raw_events` 求和会导致礼物件数与流水翻倍，统计送礼次数、人数与金额必须查询已完成订单级去重与实付对齐的 `fact_events` / `v_resolved_events`。

### 陷阱 7：公共装扮头像与多人撞图
`/bfs/garb/` 官方装扮头像、`/bfs/baselabs/` 装扮图、`noface` 默认灰头以及热门动漫网图会被多名无关观众同时使用。严禁直接拿原始头像 URL 做 1:1 身份合并；基于头像认领 UID 必须走已剔除公共前缀且满足全局唯一约束（`COUNT(DISTINCT uid) = 1`）的 `v_unique_face_uid`，未解出事件按 `clue_face_sha1` 做同场盲盒与弹幕探索关联时也须排除 `dim_user_faces` 中已知多人共用的哈希。

### 陷阱 8：接收时钟偏差与 PK/连麦多人混音边界
`time_source = 'received'` 的事件（如 `LIKE_INFO_V3_CLICK`、`ENTRY_EFFECT`）仅记录本机收到 WebSocket 帧的时刻，不可与 `time_source = 'event'` 的弹幕或礼物做亚秒级先后因果排序。同时 `fact_transcripts` 来自房间混流音频转写，不含全局说话人分离；凡落入 `fact_cohost_intervals` 中 `kind IN ('pk', 'pk_punish', 'voice_join')` 区间的转写文本，均可能混有对手主播或连麦观众的说话声，分析主播个人话术时必须联表识别并标注多说话人窗口。

## 四、标准 SQL 查询范式示例

### 范式 1：拉取单笔付费前后 `[-180s, +120s]` 双边原话、本人弹幕、逐行 PK/连麦状态与当刻在线人数切片

每条转写与弹幕分别按自身发生时间窗口匹配 `fact_cohost_intervals`，`pay_cohost_modes` 单独反映付款发生当刻所处的 PK（含读秒冲刺 `sprint`）或连麦状态：

```sql
-- 目标付费事件：session_key = 'sess_demo_123456', event_seq = 42
WITH target_pay AS (
    SELECT
        session_key,
        event_seq,
        room_id,
        event_ts AS pay_ts,
        actor_key,
        resolved_uid,
        paid_milli_yuan / 1000.0 AS paid_yuan,
        output_milli_yuan / 1000.0 AS output_yuan,
        gift_name,
        blind_orig_name,
        wealth_level
    FROM v_resolved_events
    WHERE session_key = 'sess_demo_123456'
      AND event_seq = 42
),
pay_ctx AS (
    SELECT
        (
            SELECT m.online_count
            FROM fact_room_metrics m
            WHERE m.session_key = p.session_key
              AND m.minute_ts = CAST(p.pay_ts / 60 AS INTEGER) * 60
        ) AS pay_online_count,
        (
            SELECT GROUP_CONCAT(
                c.kind || CASE
                    WHEN c.kind = 'pk'
                     AND json_extract(c.raw_summary_json, '$.sprint_start_ts') IS NOT NULL
                     AND p.pay_ts BETWEEN json_extract(c.raw_summary_json, '$.sprint_start_ts') AND c.end_ts
                    THEN '(sprint)'
                    ELSE ''
                END,
                ','
            )
            FROM fact_cohost_intervals c
            WHERE c.session_key = p.session_key
              AND c.kind IN ('pk', 'pk_punish', 'voice_join')
              AND p.pay_ts BETWEEN c.start_ts AND c.end_ts
        ) AS pay_cohost_modes
    FROM target_pay p
)
SELECT
    'transcript' AS item_type,
    ROUND(t.start_ts - p.pay_ts, 1) AS rel_sec,
    COALESCE(t.activity_label, 'unknown') || COALESCE(':' || t.song_title, '') AS meta,
    t.text AS content,
    ctx.pay_online_count,
    ctx.pay_cohost_modes,
    (
        SELECT GROUP_CONCAT(
            c.kind || CASE
                WHEN c.kind = 'pk'
                 AND json_extract(c.raw_summary_json, '$.sprint_start_ts') IS NOT NULL
                 AND t.end_ts >= json_extract(c.raw_summary_json, '$.sprint_start_ts')
                 AND t.start_ts <= c.end_ts
                THEN '(sprint)'
                ELSE ''
            END,
            ','
        )
        FROM fact_cohost_intervals c
        WHERE c.session_key = t.session_key
          AND c.kind IN ('pk', 'pk_punish', 'voice_join')
          AND c.start_ts <= t.end_ts
          AND c.end_ts >= t.start_ts
    ) AS row_cohost_modes
FROM target_pay p
CROSS JOIN pay_ctx ctx
JOIN fact_transcripts t
  ON t.session_key = p.session_key
 AND t.end_ts >= p.pay_ts - 180.0
 AND t.start_ts <= p.pay_ts + 120.0

UNION ALL

SELECT
    'self_danmaku' AS item_type,
    ROUND(e.event_ts - p.pay_ts, 1) AS rel_sec,
    e.resolution_basis AS meta,
    e.content_text AS content,
    ctx.pay_online_count,
    ctx.pay_cohost_modes,
    (
        SELECT GROUP_CONCAT(
            c.kind || CASE
                WHEN c.kind = 'pk'
                 AND json_extract(c.raw_summary_json, '$.sprint_start_ts') IS NOT NULL
                 AND e.event_ts BETWEEN json_extract(c.raw_summary_json, '$.sprint_start_ts') AND c.end_ts
                THEN '(sprint)'
                ELSE ''
            END,
            ','
        )
        FROM fact_cohost_intervals c
        WHERE c.session_key = e.session_key
          AND c.kind IN ('pk', 'pk_punish', 'voice_join')
          AND e.event_ts BETWEEN c.start_ts AND c.end_ts
    ) AS row_cohost_modes
FROM target_pay p
CROSS JOIN pay_ctx ctx
JOIN v_resolved_events e
  ON e.session_key = p.session_key
 AND e.kind = 'danmaku'
 AND e.event_ts BETWEEN p.pay_ts - 180.0 AND p.pay_ts + 120.0
 AND (
     e.actor_key = p.actor_key
     OR (
         p.resolved_uid IS NULL
         AND e.resolved_uid IS NULL
         AND p.actor_key NOT LIKE 'mystery:%'
         AND e.clue_face_sha1 IS NOT NULL
         AND e.clue_face_sha1 = (
             SELECT clue_face_sha1 FROM fact_events
             WHERE session_key = p.session_key AND event_seq = p.event_seq
         )
         AND e.clue_face_sha1 NOT IN (
             SELECT face_sha1 FROM dim_user_faces GROUP BY face_sha1 HAVING COUNT(DISTINCT uid) > 1
         )
     )
 )
ORDER BY rel_sec ASC, item_type ASC;
```

### 范式 2：筛选荣耀 `>= 35` 级、场内首末事件跨度 `>= 300s` 但 `paid_milli_yuan = 0` 的沉默大哥样本

直播弹幕流不含退房事件，`last_event_ts - first_event_ts` 衡量观众在本场首末次可见事件（进房、弹幕、点赞或免费礼）之间的时间跨度，作为驻留时间筛选的可观测代理指标（若观众中途退房后再次进房，`entry_count > 1` 可辅助区分重复短访）：

```sql
SELECT
    p.session_key,
    p.room_id,
    s.room_label,
    p.resolved_uid,
    u.latest_uname,
    MAX(p.max_wealth_level, COALESCE(u.max_wealth_level, 0)) AS wealth_level,
    ROUND(p.last_event_ts - p.first_event_ts, 1) AS observed_span_sec,
    p.entry_count,
    p.danmaku_count,
    p.like_count,
    CASE
        WHEN r.anchor_uid IS NOT NULL AND p.medal_ruid = r.anchor_uid THEN 1
        ELSE 0
    END AS wears_self_room_medal
FROM fact_session_presence p
JOIN dim_sessions s ON s.session_key = p.session_key
LEFT JOIN dim_rooms r ON r.room_id = p.room_id
LEFT JOIN dim_users u ON u.uid = p.resolved_uid
WHERE MAX(p.max_wealth_level, COALESCE(u.max_wealth_level, 0)) >= 35
  AND p.paid_milli_yuan = 0
  AND (p.last_event_ts - p.first_event_ts) >= 300.0
ORDER BY wealth_level DESC, observed_span_sec DESC;
```

### 范式 3：统计两个直播间的真实跨房重合观众与各自实付金额

```sql
WITH room_actors AS (
    SELECT
        room_id,
        actor_key,
        MAX(resolved_uid) AS resolved_uid,
        COUNT(DISTINCT session_key) AS sessions_seen,
        SUM(danmaku_count) AS danmaku_total,
        SUM(paid_milli_yuan) AS paid_milli_total
    FROM fact_session_presence
    WHERE room_id IN ('123456', '654321')
      AND (resolved_uid IS NOT NULL OR actor_key LIKE 'md5:%')
    GROUP BY room_id, actor_key
)
SELECT
    a.actor_key,
    COALESCE(a.resolved_uid, b.resolved_uid) AS uid,
    a.sessions_seen AS sessions_in_123456,
    ROUND(a.paid_milli_total / 1000.0, 2) AS paid_yuan_123456,
    b.sessions_seen AS sessions_in_654321,
    ROUND(b.paid_milli_total / 1000.0, 2) AS paid_yuan_654321
FROM room_actors a
JOIN room_actors b
  ON a.actor_key = b.actor_key
WHERE a.room_id = '123456'
  AND b.room_id = '654321'
ORDER BY (a.paid_milli_total + b.paid_milli_total) DESC;
```
