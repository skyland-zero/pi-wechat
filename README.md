# pi-wechat

English | [简体中文](./README.zh-CN.md)

`pi-wechat` is a TypeScript extension for [pi](https://github.com/badlogic/pi-mono) that bridges WeChat iLink Bot conversations into a pi session.

It lets you:

- log in to WeChat iLink Bot with a QR code
- long-poll incoming WeChat messages in the background
- inject each incoming message into the current pi session
- send the final assistant reply from the completed agent loop back to WeChat
- keep WeChat typing state in sync while pi is working

## What This Extension Is

This project is a pi extension, not a standalone chatbot process.

The bridge works by feeding WeChat messages into the current pi session. That design keeps the implementation small and aligned with pi's extension model, but it also means the session becomes the shared context for all bridged messages.

Recommended usage:

- run the bridge in a dedicated pi session
- avoid mixing manual terminal prompts and live WeChat traffic in the same session

Current scope:

- stable text message bridge
- login persistence with automatic credential backup
- getUpdates cursor persistence (resumes after restart, no duplicate delivery)
- session-expiry cooldown, poll backoff, and rate-limit backoff
- outbound message throttling
- typing indicator support (typing_ticket with TTL)

Current limitations:

- image, voice, video, and file messages are converted to placeholder text
- this is not a multi-user routing server
- this does not isolate each WeChat conversation into a separate pi session
- the WeChat side rate-limits aggressively: roughly **7 messages / 5 minutes**, shared across **all clients of that bot account**
- group chats are not supported (the protocol declares direct chats only)
- proactive delivery is limited; an idle channel may be silently disconnected by the server

## Install

Install the extension with pi using one of these package sources.

### Option A: install from npm

```bash
pi install npm:pi-wechat
```

### Option B: install from GitHub

```bash
pi install git:github.com/yangyang0507/pi-wechat
```

### Reload pi resources

If pi is already running:

```text
/reload
```

## Quick Start

Inside pi:

```text
/wechat-login
/wechat-start
```

Then:

1. scan the QR code in pi with WeChat
2. confirm the login on your phone
3. send a message to the bot from WeChat
4. wait for pi to finish the full agent loop
5. receive the final assistant text reply back in WeChat

## Usage Tutorial

### Login

Run:

```text
/wechat-login
```

The extension fetches a WeChat iLink Bot QR code, renders it in the pi UI, and waits for confirmation.

Credentials are stored locally at:

```text
~/.pi-wechat/credentials.json
```

To force a fresh login:

```text
/wechat-login --force
```

`--force` ignores local credentials and **omits the local token list**, so the server issues a brand-new
QR code. Without `--force`, if the server decides the bot is already bound to this machine it returns
`binded_redirect` instead of hanging silently.

The login flow handles every protocol status: `scaned`, `need_verifycode` (prompts for the numeric
pairing code shown on your phone), `verify_code_blocked`, `expired` (auto-refresh, up to 3 times),
`scaned_but_redirect` (switches to the server-provided `redirect_host`), and `binded_redirect`.
The whole flow times out after 8 minutes.

Local state files (all under `~/.pi-wechat/`):

```text
credentials.json       current credentials
credentials.json.bak   automatic backup, written before each overwrite
tokens.json            token history, used to detect the "already bound" state
sync.json              getUpdates cursor, used to resume after restarts
```

> ⚠️ The WeChat side currently offers **no way to unbind**. If `credentials.json` and its backup are both
> lost while the server still considers the bot bound, re-scanning may fail permanently. Do not delete
> `~/.pi-wechat/` casually.

### Start the bridge

Run:

```text
/wechat-start
```

This starts the long-poll loop. Incoming WeChat messages are queued and injected into the current pi session one by one.

### Stop the bridge

Run:

```text
/wechat-stop
```

This stops polling and clears in-memory bridge state.

### Check bridge status

Run:

```text
/wechat-status
```

This shows whether the bridge is running, whether credentials are loaded, whether the channel is in a cooldown period, and whether there are queued messages.

### Clear saved credentials

Run:

```text
/wechat-logout
```

This stops the bridge and removes `credentials.json` and `sync.json`.

The backup and token history are **kept on purpose**: the server needs the token history to recognize
that the bot is already bound to this machine, otherwise the next login can stall because the server
refuses to issue a new QR code.

## Slash Commands

- `/wechat-login` - log in with QR code (loads cached credentials when present)
- `/wechat-login --force` - force a fresh QR login, ignoring cached credentials and the binding record
- `/wechat-start` - start polling and bridging messages
- `/wechat-stop` - stop the bridge
- `/wechat-status` - show bridge state, including remaining cooldown
- `/wechat-logout` - remove saved credentials and stop the bridge (keeps the backup)

## Channel Limits and Cooldowns

The WeChat side rate-limits the iLink channel. The extension builds in matching protections:

| Situation | Server / extension behavior |
| --- | --- |
| Rate limit (`ret: -2`) | About 7 messages / 5 minutes. The extension throttles outbound sends to a 5s minimum interval and backs off 30s before one retry |
| Stale session (`errcode: -14`) | The extension **cools the channel down for 1 hour** instead of re-logging in immediately, avoiding repeated risk-control triggers |
| Long-poll timeout | Treated as normal control flow and retried immediately, not counted as an error |
| Poll failure | Retry after 2s; back off 30s after 3 consecutive failures |
| Long-poll duration | Follows the server-provided `longpolling_timeout_ms` |

Long replies are split into multiple messages (2000 characters each), and **every chunk consumes rate-limit quota**.
Keep the 7 / 5 minutes limit in mind when sending many long replies.

## How Replies Work

When a WeChat message arrives:

1. the extension receives it from the iLink Bot API
2. the message text is injected into pi as a user message
3. bridge-specific reply instructions are added through the hidden `before_agent_start` system prompt layer
4. pi runs the full agent loop, including tools if needed
5. the reply is delivered back to WeChat using the strategy below

### Delivery strategy

| Layer | Behavior | Switch |
| --- | --- | --- |
| **L1 block replies** (default) | Each completed assistant message is sent immediately, mirroring the official plugin's "send the text blocks completed between multi-step tool calls in order" | On by default |
| **L2 tool progress** | `TOOL_CALL_START` / `TOOL_CALL_RESULT` progress messages (tool name and status only) | On by default |
| **L3 block streaming** | Token-level coalescing; flushes at 800 chars or 12s idle | `PI_WECHAT_STREAM=1`, **off by default** |
| Safety net | `agent_end` only sends the **not-yet-delivered remainder**, never a duplicate | On by default |

`agent_end` is still used instead of `turn_end`, because `turn_end` can prematurely send intermediate results when the assistant calls tools.

### L2 read-only tool filter

A single agent turn can call `read` / `grep` a dozen times; reporting all of them would burn the rate-limit quota.
These tools are **not** reported: `read`, `grep`, `find`, `ls`, `ffgrep`, `fffind`, `glob`.

### Why L3 is off by default

The official openclaw-weixin plugin also leaves block streaming off: it declares the `blockStreaming`
capability but hard-codes `disableBlockStreaming: true` at dispatch time (a later PR made it configurable,
still defaulting to off). The reasons are WeChat's tight rate-limit quota (roughly 7 messages / 5 minutes)
and an upstream bug where block replies are not delivered before tool execution.

When enabled, the threshold follows the OpenClaw core chunker (800 chars), not the plugin's declared-but-ineffective 200.

## Thinking Content Is Never Sent to WeChat

pi's assistant content is `(TextContent | ThinkingContent | ToolCall)[]`. The extension applies three layers of protection before anything leaves the machine:

| Form | Handling |
| --- | --- |
| Separate thinking block (including the `thinkingSignature` encrypted payload) | Only `type === "text"` parts are extracted; thinking and toolCall are always excluded |
| Streaming `thinking_start` / `thinking_delta` / `thinking_end` events | Only `text_delta` is consumed; thinking events are ignored entirely |
| Reasoning a provider folds into the start of plain text | A text-level fallback strips structural markers such as `<thinking>`, `<reasoning>`, and ```` ```thinking ```` |

**Every delivery path goes through the same sanitizer**, so there is no bypassing branch.

> ⚠️ pi's `hideThinkingBlock` setting **only affects the terminal display and does nothing for the WeChat bridge**.
> Do not rely on it to keep thinking content private.

Tool progress messages contain only the tool name, call ID, and status — **never tool inputs or results**.

## Development

Install dependencies:

```bash
npm install
```

Run the extension check (type check + extension load):

```bash
npm run check
```

Type check only:

```bash
npm run typecheck
```

## Publish

Once you are ready to distribute it through npm:

```bash
npm run check
npm login
npm publish --access public
```

## Debug Logging

By default, the extension keeps UI noise low and avoids extra console logging when pi UI notifications are available.

To enable bridge debug logs:

```bash
PI_WECHAT_DEBUG=1 pi
```

To enable L3 block streaming (off by default, mind the rate-limit quota):

```bash
PI_WECHAT_STREAM=1 pi
```

## References

- pi extension runtime: [badlogic/pi-mono](https://github.com/badlogic/pi-mono)
- WeChat official plugin (protocol alignment reference): [Tencent/openclaw-weixin](https://github.com/Tencent/openclaw-weixin)
- WeChat protocol SDK reference: [epiral/weixin-bot](https://github.com/epiral/weixin-bot)
- Agent bridge design reference: [wong2/weixin-agent-sdk](https://github.com/wong2/weixin-agent-sdk)
