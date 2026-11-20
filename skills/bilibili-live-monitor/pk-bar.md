# 离线 PK 条回放

从只读 SQLite 的 `PK_INFO` 和 `PK_BATTLE_*` 原始消息生成可审计计划，再叠加到已准备的固定画幅视频。仅支持双人 PK；不会连接、修改或重启直播采集。

```bash
bili-live pk-bar plan --database /PRIVATE/monitor.sqlite3 --room 123456 \
  --pk-id 789 --video /PRIVATE/base.mp4 --video-start 2030-01-01T12:00:00+08:00 \
  --font /PRIVATE/font.ttf --connection-start 0 --connection-end 600 \
  --style-assets /PRIVATE/verified-style \
  --score-font /PRIVATE/score-bold-italic.ttf --timer-font /PRIVATE/timer-regular.ttf \
  --output /PRIVATE/pk-plan.json
bili-live pk-bar render --plan /PRIVATE/pk-plan.json --output /PRIVATE/new-render
bili-live pk-bar verify --output /PRIVATE/new-render
```

用户要求沿用已认可的 PK 动画版本时，先查历史会话和那版 `plan.json`，复用其中 `style_assets`、`font`、`score_font`、`timer_font` 并核对对应 SHA256。**省略 `--style-assets` 会走基础渐变条，不包含原网页流动箭头、交界光效和胜负表情；基础条不能作为动画版的等价替代。** 沿用既有 CLI 参数即可，不需要新建渲染框架或修改默认基础模式。

每个连续片段从未烧录旧 PK 条的输入重新渲染，按该片段第零秒的墙钟生成计划，再拼接；不要把新样式盖在旧比分条上。验收至少查看相隔数帧的顶部画面，确认箭头相位、光效、比分字形和倒计时底图；适用时再查冲刺和结果阶段。最终总片末端固定30fps。只修视觉时，优先复用前一版总片已验证的原音轨，逐包哈希校验一致；音频不应随更换 PK 样式而漂移。

- `video-start` 是视频第零秒对应的带时区墙钟，须由调用者核对。`connection-start/end` 是人工确认的视频相对秒数，仅用于非比赛期间的“连线中”；不能把 PK 结束当成断开连线。
- 消息按本地接收时间更新；倒计时用服务器绝对截止时间。阶段包括准备、对战、最后冲刺、等待结算、结果、惩罚、结束后连线。原始消息缺失时不猜比分。
- `PK_INFO` 是本实现主要状态源。准备状态的 `start_time + pre_duration`、正式状态的 `end_time`、惩罚状态的 `punish_end_time` 分开使用；巅峰赛子类型50开头15秒隐藏倒计时。
- 比分条采用网页可读宽度规则，0:0各半，极端比分保留弱方最小宽度。默认重建渐变、分数、阶段文字和倒计时；不声称历史像素级一致。
- `plan --style-assets /PRIVATE/style` 可加载离线网页纹理与SVGA透明帧，以30fps重放反向箭头、交界光效、结果表情及分数滚动。目录包含`left/right/normal/sprint/chaos.png`和`manifest.json`；manifest的`light/win/lose/draw`各含`size`、`fps`、`frames`，帧名为`light_000.png`等。先从有权使用的网页素材离线导出，渲染不联网；素材不随公开仓库分发。素材文件名与内容绑定SHA。
- `--score-font`与`--timer-font`分别指定比分和倒计时字体并固定SHA。比分按网页16px粗斜体规格，倒计时14px、冲刺16px；字体文件由调用者提供，须选对应字重和字形的实际字体。
- 左侧绑定目标房间，右侧绑定对方。分数直接取平台 `votes`，不由礼物金额反算。结果依据胜负字段，不以录像截断时最后比分猜胜负。
- 计划固定视频、字体SHA与原始消息ID。输出新目录包含计划、H.265视频、日志和验收凭证。核验时长、画幅、完整解码、音轨逐包内容哈希与源文件SHA；还须人工检查起止、状态切换、位置和字体。
- 输出含真实房间及本机路径，必须留在私有目录；勿提交真实消息或计划。
