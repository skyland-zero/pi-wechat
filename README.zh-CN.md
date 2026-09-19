# pi-wechat

[English](./README.md) | 简体中文

`pi-wechat` 是一个给 [pi](https://github.com/badlogic/pi-mono) 使用的 TypeScript 扩展，用来把微信 iLink Bot 的消息桥接到 pi 会话里。

它可以：

- 通过二维码登录微信 iLink Bot
- 在后台长轮询微信消息
- 把每条微信消息注入到当前 pi 会话
- 等整个 agent loop 完成后，把最终回复发回微信
- 在 pi 工作期间同步微信输入态

## 这是什么

这个项目是一个 pi 扩展，不是独立运行的聊天机器人进程。

桥接方式是把微信消息直接送进“当前 pi 会话”。这样实现更贴近 pi 的扩展模型，也更简单，但代价是这个会话会承载所有桥接上下文。

推荐用法：

- 为微信桥接单独开一个 pi 会话
- 不要在同一个会话里混用本地终端聊天和实时微信流量

当前能力：

- 稳定的文本消息桥接
- 登录凭证持久化 + 自动备份
- getUpdates 游标持久化（重启后继续，不重复投递）
- 会话过期冷却、轮询退避与频控退避
- 出站消息节流
- 输入态支持（typing_ticket 带 TTL）

当前限制：

- 图片、语音、视频、文件消息目前会转成占位文本
- 这不是多用户路由服务
- 目前不会为每个微信会话自动拆分独立的 pi session
- 微信侧限流较紧：约 **7 条消息 / 5 分钟**，且为**该 bot 账号全局共享**配额
- 不支持群聊（协议只声明私聊）
- 主动推送能力受限，长时间无互动时通道可能被服务端断开

## 安装

使用 pi 从下面两种来源之一安装扩展。

### 方式 A：从 npm 安装

```bash
pi install npm:pi-wechat
```

### 方式 B：从 GitHub 安装

```bash
pi install git:github.com/yangyang0507/pi-wechat
```

### 重新加载 pi 资源

如果 pi 已经启动：

```text
/reload
```

## 快速开始

在 pi 里执行：

```text
/wechat-login
/wechat-start
```

然后：

1. 用微信扫描 pi 中显示的二维码
2. 在手机上确认登录
3. 从微信给 bot 发消息
4. 等 pi 完整跑完 agent loop
5. 在微信里收到最终回复

## 使用教程

### 登录

执行：

```text
/wechat-login
```

扩展会请求微信 iLink Bot 的二维码，在 pi 界面里渲染出来，并等待你确认登录。

登录过程会处理协议的全部扫码状态：

| 状态 | 扩展行为 |
| --- | --- |
| `scaned` | 提示「请在手机上确认登录」 |
| `need_verifycode` | 弹出输入框，要求输入手机微信显示的数字配对码 |
| `verify_code_blocked` | 提示输入错误并自动刷新二维码 |
| `expired` | 自动刷新二维码（最多 3 次） |
| `scaned_but_redirect` | 按服务端下发的 `redirect_host` 切换接入点 |
| `binded_redirect` | 提示「已绑定过此机器人」，不再重复绑定 |

整个登录流程有 8 分钟超时，避免无限等待。

本地状态文件（均在 `~/.pi-wechat/`）：

```text
credentials.json       当前凭证
credentials.json.bak   凭证自动备份（每次覆盖前生成）
tokens.json            历史 token（用于识别"已绑定"状态）
sync.json              getUpdates 游标（用于重启续传）
```

如果要强制重新扫码：

```text
/wechat-login --force
```

`--force` 会忽略本地凭证，并且**不携带本地 token 列表**，从而让服务端下发全新二维码。
不带 `--force` 时，如果服务端判断该 bot 已绑定到本端，会返回 `binded_redirect` 而不是静默卡住。

> ⚠️ 微信侧目前**没有解绑入口**。如果 `credentials.json` 和备份都丢失，而服务端仍认为该 bot 已绑定，
> 重新扫码可能无法完成。请勿随意删除 `~/.pi-wechat/`。

### 启动桥接

执行：

```text
/wechat-start
```

这会启动长轮询循环。收到的微信消息会按顺序排队，再逐条注入当前 pi 会话。

### 停止桥接

执行：

```text
/wechat-stop
```

这会停止轮询，并清空内存中的桥接状态。

### 查看状态

执行：

```text
/wechat-status
```

可以看到桥接是否运行、凭证是否已加载、通道是否处于冷却期、当前是否有排队消息等信息。

### 清除本地凭证

执行：

```text
/wechat-logout
```

这会停止桥接，并删除 `credentials.json` 与 `sync.json`。

注意：**备份与历史 token 会保留**。保留历史 token 是有意为之——服务端需要它才能识别
「该 bot 已绑定到本端」，否则下次登录会卡在服务端拒绝签发二维码的状态。

## Slash Commands

- `/wechat-login` - 二维码登录（已有凭证时直接加载）
- `/wechat-login --force` - 强制重新扫码，忽略本地凭证与绑定记录
- `/wechat-start` - 启动桥接
- `/wechat-stop` - 停止桥接
- `/wechat-status` - 查看桥接状态（含冷却剩余时间）
- `/wechat-logout` - 删除凭证并停止桥接（保留备份）

## 通道限制与冷却

微信侧对 iLink 通道有频率限制，扩展已内置相应保护：

| 场景 | 服务端/扩展行为 |
| --- | --- |
| 频率限制（`ret: -2`） | 约 7 条 / 5 分钟。扩展出站节流为最小 5 秒间隔，命中后自动退避 30 秒重试一次 |
| 会话过期（`errcode: -14`） | 扩展将通道**冷却 1 小时**，而不是立即重登，避免反复触发风控 |
| 长轮询超时 | 视为正常控制流，直接进入下一轮，不计为错误 |
| 轮询失败 | 重试间隔 2 秒，连续 3 次失败后退避 30 秒 |
| 长轮询时长 | 跟随服务端下发的 `longpolling_timeout_ms` |

长回复会被切成多条消息（每片 2000 字符），**每片都计入频率配额**。如果需要发送大量长回复，
请留意这 7 条 / 5 分钟的限制。

## 回复是怎么发回微信的

当一条微信消息到来时：

1. 扩展从 iLink Bot API 收到消息
2. 消息正文作为用户消息注入 pi
3. 微信回复约束通过隐藏的 `before_agent_start` system prompt 注入
4. pi 跑完整个 agent loop，必要时可以调用工具
5. 回复按下面的策略投递回微信

### 投递策略

| 层级 | 行为 | 开关 |
| --- | --- | --- |
| **L1 块级投递**（默认） | 每条 assistant 消息完成时立即发出其正文，对应官方「按顺序发送模型在多步工具调用之间完成的文本块」 | 默认开启 |
| **L2 工具进度** | 工具执行时发 `TOOL_CALL_START` / `TOOL_CALL_RESULT` 进度消息（只含工具名与状态） | 默认开启 |
| **L3 块流式** | token 级累积，达 800 字或空闲 12 秒即发一块 | `PI_WECHAT_STREAM=1`，**默认关闭** |
| 兜底 | `agent_end` 时只补发**尚未投递的残余**，不会重复发送 | 默认开启 |

`agent_end` 仍不用 `turn_end`，因为 assistant 中途调用工具时，`turn_end` 容易把中间结果过早发回微信。

### 注入时机：只认 `agent_settled`

`agent_end` 只代表本轮底层运行结束。之后 pi 仍可能自动重试、自动压缩后重试，
或继续执行已排队的跟进消息——此时仍然处于流式状态。

因此下一条微信消息只在 `agent_settled`（无重试/压缩/跟进残留）后注入。
此外每次注入都带 `deliverAs: 'followUp'`，即使发生竞态也只会排队，不会报错丢失消息。

> 如果你看到 `Agent is already processing. Specify streamingBehavior ('steer' or 'followUp')`，
> 说明扩展版本过旧（≤ 8a8f7b5）。更新即可。

### L2 只读工具过滤

一次 agent 轮次里 `read` / `grep` 这类调用可能十几次，全发会直接吃满频率配额。
以下工具**不发**进度消息：`read`、`grep`、`find`、`ls`、`ffgrep`、`fffind`、`glob`。

### 为什么 L3 默认关闭

官方 openclaw-weixin 同样把块流式默认关闭：它声明了 `blockStreaming` 能力，
但派发时硬编码 `disableBlockStreaming: true`（后续 PR 改为可配置，默认仍为 off）。
原因是微信侧频率配额很紧（约 7 条 / 5 分钟），且上游存在「块回复不早于工具执行」的已知问题。

开启后阈值取 OpenClaw core chunker 的 800 字，而不是插件里声明却未生效的 200 字。

## 思考内容不会发到微信

pi 的 assistant 内容是 `(TextContent | ThinkingContent | ToolCall)[]`，扩展在出站前做三重防护：

| 形态 | 处理 |
| --- | --- |
| 独立 thinking block（含 `thinkingSignature` 加密载荷） | 只提取 `type === "text"` 的部分，thinking 与 toolCall 一律排除 |
| 流式 `thinking_start` / `thinking_delta` / `thinking_end` 事件 | 只监听 `text_delta`，思考事件全部忽略 |
| 被 provider 混进正文首部的推理 | 文本层兜底剥离 `<thinking>` / `<reasoning>` / ```` ```thinking ```` 等结构化标记 |

**所有投递路径都经过同一个净化入口**，不存在绕过的分支。

> ⚠️ pi 的 `hideThinkingBlock` 设置**只影响终端显示，对微信桥接无效**。
> 不要依赖它来防止思考内容外泄。

另外，工具进度消息只包含工具名、调用 ID 和状态，**绝不包含工具入参或结果内容**。

## 开发

安装依赖：

```bash
npm install
```

执行基础校验（类型检查 + 扩展加载）：

```bash
npm run check
```

只做类型检查：

```bash
npm run typecheck
```

## 发布

如果后面要发布到 npm：

```bash
npm run check
npm login
npm publish --access public
```

## 调试日志

默认情况下，扩展会尽量减少 UI 噪音；如果 pi 已经能显示通知，就不会额外输出大量 console 日志。

如果需要打开桥接调试日志：

```bash
PI_WECHAT_DEBUG=1 pi
```

如果需要打开 L3 块流式（默认关闭，注意频率配额）：

```bash
PI_WECHAT_STREAM=1 pi
```

## 参考

- pi 扩展运行时：[badlogic/pi-mono](https://github.com/badlogic/pi-mono)
- 微信官方插件（协议对齐参考）：[Tencent/openclaw-weixin](https://github.com/Tencent/openclaw-weixin)
- 微信协议 SDK 参考：[epiral/weixin-bot](https://github.com/epiral/weixin-bot)
- Agent 桥接设计参考：[wong2/weixin-agent-sdk](https://github.com/wong2/weixin-agent-sdk)
