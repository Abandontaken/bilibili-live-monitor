# 跨房全局身份消解数仓与本地匿名看板回填

## 定位与边界

`bili-live warehouse` 提供纯离线、按需批处理的跨房身份消解数仓与本地匿名网页看板差量回填，不需要改动或重启常驻采集进程：

- **源数据库严格只读**：所有源 `monitor.sqlite3` 均以 URI `?mode=ro` 与 `PRAGMA query_only=ON` 打开，绝不回写源库或创建表。
- **原始线索保留与查询晚绑定**：入库时保留每条互动事件的原始协议线索（明文 `raw_uid`、礼物连击无盐 `clue_md5`、用户上传头像 `clue_face_sha1`、匿名弹幕 `clue_crc32` 与首字），在查询视图 `v_resolved_events` 与增量同步阶段与全局字典 `dim_users` 双向关联。某位观众先在房间 `123456` 匿名发弹幕与送礼、数日后在房间 `654321` 点赞或进榜露出明文身份时，下一次 `sync` 会自动认领历史记录并刷新 `fact_session_presence`。
- **身份归属与防碰撞护栏**：
  1. `native_uid`：明文正整数 UID 直接写入 `dim_users` 并计算 `uid_md5` 与 `uid_crc32`；
  2. `global_md5`：`batch_combo_id` 中的 128 位无盐 `MD5(uid)` 跨房精确关联；
  3. `unique_face`：仅提取 `/bfs/face/<40位hex>.`，排除 `noface` 与 `/bfs/garb/` 官方装扮；一旦观测到 2 个及以上不同 UID 共用同一头像哈希，`v_unique_face_uid` 自动排除该头像；
  4. `crc32_first_char`：匿名弹幕优先匹配 `uid_crc32` 与首字（多候选碰撞时按同场、同房已确凿露面记录消歧，无法唯一确定则置空并标 `ambiguous_crc32`）；若 `crc32` 未命中，允许通过 `info[0][15]` 提取的唯一头像 `v_unique_face_uid`（有 `clue_crc32` 时要求 CRC32 一致以兼顾改名用户与防撞图，无 `clue_crc32` 时要求首字无冲突）辅助认领，仍未解出时保留 `clue_face_sha1` 供盲盒与弹幕关联；
  5. `rank_delta`：对无上述线索的单笔非盲盒金瓜子礼物，仅当随后 15 秒内同房 `ONLINE_RANK_V3` 恰有唯一 1 名榜单观众贡献值增量精确吻合且窗口内无其他金瓜子干扰时，单笔记录 `inferred_uid`。
   6. `blind_box_rank_window`：盲盒不用实付匹配榜分。对同房同场、身份缺失、交易未重复的连续盲盒包，用开出面值总和匹配已在榜账号的唯一积分增量；首字只用于排除冲突。缺帧、干扰或多解时不归属。认领不改变开盒实付、礼物面值或房间总额。历史场次按审核后的清单显式 `--reingest <session_key>`，然后刷新受影响的本地看板。
- **房间状态与 PK/连麦上下文沉淀**：场次入库同步构建每分钟房间状态序列 `fact_room_metrics`（同屏在线 `online_count`、累计看过 `watched_count`、关注数 `fans_count`、粉丝团 `fans_club_count`）与多说话人状态区间 `fact_cohost_intervals`（`pk`、`pk_punish`、`voice_join`、`voice_join_queue`）；已有 v1 数仓执行 `sync` 时自动平滑升级表结构与视图并重入旧版本场次。完整表字典、原始报文路径、八大口径防坑规则与标准 SQL 模板见 [数据库解析与口径防坑指南](data-interpretation.md)。

## 命令用法

以下路径与房间号均为公开文档占位示例，实际运行数据存放在私有 `.local/` 目录：

```bash
# 1. 增量扫描源库字典并将已完成场次标准化入库（天然幂等；可选传入外部房间台账归并短长房号并写入赛道分类）
bili-live warehouse sync \
  --warehouse /PRIVATE/warehouse.sqlite3 \
  --source account=/PRIVATE/account/monitor.sqlite3 \
  --source anonymous=/PRIVATE/anonymous/monitor.sqlite3 \
  --session-root /PRIVATE/anonymous/live_sessions \
  --room-registry /PRIVATE/room_registry.json

# 单场解析升级或重验时，可按 workflow_key 事务重入（传 * 重入全部已完成场次）
bili-live warehouse sync \
  --warehouse /PRIVATE/warehouse.sqlite3 \
  --source anonymous=/PRIVATE/anonymous/monitor.sqlite3 \
  --session-root /PRIVATE/anonymous/live_sessions \
  --reingest 0123456789abcdef01234567

# 2. 查看全局字典规模、付费礼物金额分档消解率与弹幕荣耀等级分档消解率
bili-live warehouse stats --warehouse /PRIVATE/warehouse.sqlite3

# 3. 预演本地已完成匿名场次看板差量回填（默认 --dry-run，零磁盘写入）
bili-live warehouse backfill-pages \
  --warehouse /PRIVATE/warehouse.sqlite3 \
  --session-root /PRIVATE/anonymous/live_sessions \
  --dry-run

# 确认预演结果无误后，显式加 --execute 原子写回有变化的本地看板
bili-live warehouse backfill-pages \
  --warehouse /PRIVATE/warehouse.sqlite3 \
  --session-root /PRIVATE/anonymous/live_sessions \
  --execute
```

## 本地匿名看板回填五条硬边界

1. **范围限定**：仅处理 `workflow.json` 为 `stage == "completed"` **且** `config.json` 为 `local_only == true` 的场次；直播中、收尾中或带远端发布的场次一律跳过。
2. **只读内存叠加**：回填时只读读取源库与数仓，在内存副本上合并同一已解出 `uid` 的送礼汇总（展示最新非打码昵称），并将尚未露真身但带独立 `clue_md5` 的同名打码送礼人拆分为 `某*** #a1b2` 短后缀，避免不同观众按同首字误并。
3. **不伪造历史待跟进**：仅更新 `gift_summary`（每人送礼汇总）与 `activity_summary`（含单曲去重送礼人数 `gift_users`），原场已封口的 `followups`（灯牌与盲盒点歌跟进）保持不变。
4. **差量触发**：重算结果与磁盘现有 `public/snapshot.json` 一致时记为 `unchanged` 跳过，不触碰锁或静态文件。
5. **锁保护、原生校验与留痕回滚**：`--execute` 非阻塞获取 `workflow.lock` 与 `publisher.lock`，原子写出 `public/index.html` 与 `public/snapshot.json` 后强制运行 `verify_snapshot` 校验金额守恒与零隐私字段泄漏；校验失败立即原子回滚旧字节，通过后在场次目录追加 `identity_backfill_history.jsonl` 审计摘要并更新 `workflow.json` 的 `final_artifacts`。

## 已发布场次回填

`bili-live warehouse backfill-remote-pages --warehouse /PRIVATE/warehouse.sqlite3 --session-root /PRIVATE/live_sessions` 只读预演。`--execute` 处理已完成且已有远端地址的场次，只发布该场原有的 HTML 和 JSON。只允许使用本场送礼表已展示的名字，不公开新的跨房名字或哈希后缀。时间、礼物总计、点歌跟进和其他场次指标不变。先校验候选，再发布并读回两文件，最后更新本地。若传输中断，私有 `identity_remote_pending.json` 保留可重试候选；本地、远端或发布目标出现第三种版本时停止并报告失败。
