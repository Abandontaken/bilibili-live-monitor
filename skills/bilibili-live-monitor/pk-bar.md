# 离线 PK 条回放

从只读 SQLite 的 `PK_INFO` 和 `PK_BATTLE_*` 原始消息生成可审计计划，再叠加到已准备的固定画幅视频。仅支持双人 PK；不会连接、修改或重启直播采集。

```bash
bili-live pk-bar plan --database /PRIVATE/monitor.sqlite3 --room 123456 \
  --pk-id 789 --video /PRIVATE/base.mp4 --video-start 2030-01-01T12:00:00+08:00 \
  --font /PRIVATE/font.ttf --connection-start 0 --connection-end 600 \
  --output /PRIVATE/pk-plan.json
bili-live pk-bar render --plan /PRIVATE/pk-plan.json --output /PRIVATE/new-render
bili-live pk-bar verify --output /PRIVATE/new-render
```

- `video-start` 是视频第零秒对应的带时区墙钟，须由调用者核对。`connection-start/end` 是人工确认的视频相对秒数，仅用于非比赛期间的“连线中”；不能把 PK 结束当成断开连线。
- 消息按本地接收时间更新；倒计时用服务器绝对截止时间。阶段包括准备、对战、最后冲刺、等待结算、结果、惩罚、结束后连线。原始消息缺失时不猜比分。
- `PK_INFO` 是本实现主要状态源。准备状态的 `start_time + pre_duration`、正式状态的 `end_time`、惩罚状态的 `punish_end_time` 分开使用；巅峰赛子类型50开头15秒隐藏倒计时。
- 比分条采用网页可读宽度规则，0:0各半，极端比分保留弱方最小宽度。该版本为重建界面：渐变、分数、阶段文字和倒计时；未复刻网页 SVGA 表情、滚动数字或粒子动画，不声称历史像素级一致。
- 左侧绑定目标房间，右侧绑定对方。分数直接取平台 `votes`，不由礼物金额反算。结果依据胜负字段，不以录像截断时最后比分猜胜负。
- 计划固定视频、字体SHA与原始消息ID。输出新目录包含计划、H.265视频、日志和验收凭证。核验时长、画幅、完整解码、音轨逐包内容哈希与源文件SHA；还须人工检查起止、状态切换、位置和字体。
- 输出含真实房间及本机路径，必须留在私有目录；勿提交真实消息或计划。
