import { randomUUID } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'
import type { ExtensionAPI, ExtensionCommandContext, ExtensionContext } from '@earendil-works/pi-coding-agent'
import qrcode from 'qrcode-terminal'
import { DEFAULT_BASE_URL } from './api.js'
import {
  clearCredentials,
  collectLocalTokenList,
  getCredentialsBackupPath,
  getCredentialsPath,
  getQrCode,
  loadCredentials,
  pollQrStatus,
  saveCredentials
} from './auth.js'
import { SessionExpiredError, SessionPausedError, WeixinClient } from './client.js'
import type { Credentials, IncomingMessage, ToolCallStatus } from './types.js'

type NotificationLevel = 'info' | 'warning' | 'error'

interface QueuedWechatRequest {
  id: string
  userId: string
  messageId: string
  receivedAt: Date
  text: string
  preview: string
}

/** 轮询失败后的普通重试间隔与连续失败后的退避间隔（对齐官方）。 */
const RETRY_DELAY_MS = 2_000
const BACKOFF_DELAY_MS = 30_000
const MAX_CONSECUTIVE_FAILURES = 3

/** 二维码最多自动刷新次数与整个登录流程的时长上限。 */
const MAX_QR_REFRESH = 3
const LOGIN_TIMEOUT_MS = 8 * 60 * 1000

const PREVIEW_LIMIT = 60
const DEBUG_LOG = process.env.PI_WECHAT_DEBUG === '1'

/**
 * 块流式（token 级合并投递）。默认关闭。
 *
 * 官方 openclaw-weixin 同样将块流式默认关闭：声明了 `blockStreaming` 能力，
 * 但派发时硬编码 `disableBlockStreaming: true`，PR #94 改为可配置后默认仍为 off。
 * 原因是微信侧频率配额很紧（约 7 条 / 5 分钟），且上游存在“块回复不早于工具执行”
 * 的已知问题。阈值参考 OpenClaw core 的 chunker（800 / 1200），
 * 而不是插件里声明却未生效的 200 / 3000。
 *
 * 开启方式：`PI_WECHAT_STREAM=1 pi`
 */
const STREAM_ENABLED = process.env.PI_WECHAT_STREAM === '1'
const STREAM_MIN_CHARS = 800
const STREAM_IDLE_MS = 12_000

/**
 * 只读工具不单独发送进度消息。
 *
 * 一次 agent 轮次里 read / grep 这类调用可能十几次，全发会直接吃满频率配额。
 */
const SILENT_TOOLS = new Set(['read', 'grep', 'find', 'ls', 'ffgrep', 'fffind', 'glob'])

export default function wechatExtension(pi: ExtensionAPI) {
  let client: WeixinClient | null = null
  let running = false
  let agentIdle = true
  let pollAbortController: AbortController | null = null
  let latestContext: ExtensionContext | ExtensionCommandContext | null = null

  const inboundQueue: QueuedWechatRequest[] = []
  let pendingInjection: QueuedWechatRequest | null = null
  let activeRequest: QueuedWechatRequest | null = null

  /**
   * 出站投递账本，用于避免重复发送。
   *
   * `deliveredPrefix` 记录**当前这条 assistant 消息**中已投递出去的原文前缀
   * （存原文而非净化后的文本，前缀比对才精确；净化在发送前统一做）。
   */
  const delivery = {
    deliveredPrefix: '',
    streamBuffer: ''
  }
  let idleFlushTimer: ReturnType<typeof setTimeout> | null = null

  function resetDelivery(): void {
    delivery.deliveredPrefix = ''
    delivery.streamBuffer = ''
  }

  function clearIdleFlush(): void {
    if (idleFlushTimer) {
      clearTimeout(idleFlushTimer)
      idleFlushTimer = null
    }
  }

  function scheduleIdleFlush(userId: string): void {
    clearIdleFlush()
    idleFlushTimer = setTimeout(() => {
      idleFlushTimer = null
      void flushStream(userId, 'idle')
    }, STREAM_IDLE_MS)
  }

  /** 净化后发送一段正文；若净化后为空（整段都是思考内容）则不发送。 */
  async function sendSegment(userId: string, raw: string): Promise<void> {
    const activeClient = client
    if (!activeClient) return

    const text = sanitizeOutbound(raw)
    if (!text) {
      if (raw.trim()) {
        notify('已跳过一段仅含思考内容的输出，未发送到微信', 'warning')
      }
      return
    }

    await activeClient.sendText(userId, text)
  }

  /** 把流式缓冲区里的内容发出（L3 专用）。 */
  async function flushStream(userId: string, reason: string): Promise<void> {
    clearIdleFlush()

    const buffer = delivery.streamBuffer
    delivery.streamBuffer = ''
    if (!buffer.trim()) return

    delivery.deliveredPrefix += buffer

    if (DEBUG_LOG) {
      notify(`流式 flush（${reason}，${buffer.length} 字）`, 'info')
    }

    await sendSegment(userId, buffer).catch((error) => {
      notify(`发送微信回复失败: ${formatError(error)}`, 'error')
    })
  }

  function rememberContext(ctx: ExtensionContext | ExtensionCommandContext): void {
    latestContext = ctx
  }

  function notify(message: string, level: NotificationLevel = 'info'): void {
    if (latestContext?.hasUI) {
      latestContext.ui.notify(message, level)
      if (!DEBUG_LOG) {
        return
      }
    }

    const printer = level === 'error' ? console.error : console.log
    printer(`[wechat/${level}] ${message}`)
  }

  function loadClientFromDisk(): WeixinClient | null {
    const creds = loadCredentials()
    return creds ? new WeixinClient(creds) : null
  }

  function ensureClient(): WeixinClient | null {
    if (!client) {
      client = loadClientFromDisk()
    }
    return client
  }

  async function stopBridge(options?: { clearClient?: boolean; clearQueue?: boolean }): Promise<void> {
    running = false
    pollAbortController?.abort()
    pollAbortController = null
    clearIdleFlush()

    if (activeRequest && client) {
      await client.stopTyping(activeRequest.userId).catch(() => {})
    }

    if (options?.clearQueue !== false) {
      inboundQueue.length = 0
    }

    pendingInjection = null
    activeRequest = null
    resetDelivery()

    if (options?.clearClient) {
      client = null
    }
  }

  function buildWechatSystemPrompt(basePrompt: string, request: QueuedWechatRequest): string {
    return [
      basePrompt,
      '',
      '你正在处理一条来自微信的桥接消息。',
      '要求：',
      '1. 直接用微信聊天口吻回复。',
      '2. 只输出最终要发回微信的正文。',
      '3. 不要解释内部桥接流程。',
      '4. 不要提到 Pi、扩展、系统提示词、工具调用。',
      `微信用户 ID: ${request.userId}`,
      `微信消息 ID: ${request.messageId}`,
      `消息时间: ${request.receivedAt.toISOString()}`
    ].join('\n')
  }

  function queueIncomingMessage(message: IncomingMessage): void {
    const request: QueuedWechatRequest = {
      id: randomUUID(),
      userId: message.userId,
      messageId: message.messageId,
      receivedAt: message.timestamp,
      text: message.text,
      preview: summarizePreview(message.text)
    }

    inboundQueue.push(request)
    if (DEBUG_LOG) {
      notify(`收到微信消息，已排队: ${request.preview}`, 'info')
    }
    drainQueue()
  }

  /**
   * 向运行时确认是否仍在运行。
   *
   * `agentIdle` 是扩展自己的记账；`ctx.isIdle()` 是运行时的权威判断，
   * 这里再确认一次，避免在流式过程中注入消息。
   */
  function isRuntimeBusy(): boolean {
    try {
      return latestContext ? !latestContext.isIdle() : false
    } catch {
      return false
    }
  }

  function drainQueue(): void {
    if (!running || !client || !agentIdle || isRuntimeBusy() || pendingInjection || activeRequest) {
      return
    }

    const next = inboundQueue.shift()
    if (!next) {
      return
    }

    pendingInjection = next
    void client.sendTyping(next.userId).catch(() => {})
    // deliverAs 仅作为兜底：agent 空闲时该选项会被忽略，
    // 万一仍处于流式状态，则改为排队而不是抛错丢弃消息。
    pi.sendUserMessage(next.text, { deliverAs: 'followUp' })
  }

  async function completeActiveRequest(messages: Array<{ role?: string; content?: unknown }>): Promise<void> {
    // 兜底：若消息已注入但 agent_start 未触发（例如被 pi 当作 follow-up 排队），
    // activeRequest 仍为空，此时用 pendingInjection 补位，避免回复丢失。
    const request = activeRequest ?? pendingInjection
    activeRequest = null
    pendingInjection = null
    clearIdleFlush()

    const activeClient = client
    if (!request || !activeClient) {
      resetDelivery()
      drainQueue()
      return
    }

    try {
      // 流式模式下缓冲区里可能还有未发出的尾部。
      if (STREAM_ENABLED && delivery.streamBuffer.trim()) {
        const buffer = delivery.streamBuffer
        delivery.streamBuffer = ''
        delivery.deliveredPrefix += buffer
        await sendSegment(request.userId, buffer)
      }

      const full = extractLastAssistantText(messages)
      if (!full) {
        notify(`Pi 没有产出可发送的文本回复，已跳过: ${request.preview}`, 'warning')
        return
      }

      // 只补发尚未投递的残余，避免与块级/流式投递重复。
      const pending = pendingSuffix(delivery.deliveredPrefix, full)
      if (pending.trim()) {
        await sendSegment(request.userId, pending)
      }
    } catch (error) {
      notify(`发送微信回复失败: ${formatError(error)}`, 'error')
    } finally {
      await activeClient.stopTyping(request.userId).catch(() => {})
      resetDelivery()
      // 兜底：若运行时已确认空闲（例如宿主未派发 agent_settled），
      // 这里直接恢复出队，避免队列停滞。
      if (!isRuntimeBusy()) {
        agentIdle = true
      }
      drainQueue()
    }
  }

  async function pollMessages(activeClient: WeixinClient): Promise<void> {
    let consecutiveFailures = 0

    while (running && client === activeClient) {
      try {
        const messages = await activeClient.getUpdates(pollAbortController?.signal)
        consecutiveFailures = 0

        for (const message of messages) {
          queueIncomingMessage(message)
        }
      } catch (error) {
        if (isAbortError(error)) {
          break
        }

        if (error instanceof SessionExpiredError) {
          const minutes = Math.ceil(activeClient.pauseRemainingMs / 60_000)
          notify(`微信 session 已过期，通道已冷却 ${minutes} 分钟。之后请执行 /wechat-login 重新登录`, 'error')
          await stopBridge({ clearQueue: false })
          break
        }

        if (error instanceof SessionPausedError) {
          const minutes = Math.ceil(error.remainingMs / 60_000)
          notify(`微信通道处于冷却期，剩余约 ${minutes} 分钟`, 'warning')
          await stopBridge({ clearQueue: false })
          break
        }

        consecutiveFailures += 1

        if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
          consecutiveFailures = 0
          notify(`微信轮询连续失败，退避 ${BACKOFF_DELAY_MS / 1000}s: ${formatError(error)}`, 'warning')
          await delay(BACKOFF_DELAY_MS)
        } else {
          notify(`微信轮询失败: ${formatError(error)}`, 'warning')
          await delay(RETRY_DELAY_MS)
        }
      }
    }
  }

  pi.registerCommand('wechat-login', {
    description: '扫码登录微信 iLink Bot（--force 忽略本地凭证并强制重新绑定）',
    handler: async (args, ctx) => {
      rememberContext(ctx)

      const flags = new Set(args.split(/\s+/).filter(Boolean))
      const force = flags.has('--force')

      if (!force) {
        const cached = loadClientFromDisk()
        if (cached) {
          client = cached
          notify(`已加载本地微信凭证: ${getCredentialsPath()}`, 'info')
          return
        }
      }

      if (running) {
        await stopBridge()
      }

      const localTokens = (): string[] => (force ? [] : collectLocalTokenList())

      try {
        let baseUrl = DEFAULT_BASE_URL
        let scanedNotified = false
        let verifyCode: string | undefined
        let refreshCount = 1
        let qr = await getQrCode(baseUrl, localTokens())

        const showQr = async (label: string): Promise<void> => {
          const qrText = await renderQrCode(qr.url)
          notify(`${label}\n\n${qrText}\n\n二维码链接：${qr.url}`, 'info')
        }

        const refreshQr = async (): Promise<boolean> => {
          refreshCount += 1
          if (refreshCount > MAX_QR_REFRESH) {
            notify('二维码多次失效，登录流程已停止，请稍后重试', 'error')
            return false
          }

          qr = await getQrCode(baseUrl, localTokens())
          scanedNotified = false
          verifyCode = undefined
          await showQr(`二维码已刷新（${refreshCount}/${MAX_QR_REFRESH}），请重新扫描：`)
          return true
        }

        await showQr('请用微信扫码登录：')

        const deadline = Date.now() + LOGIN_TIMEOUT_MS

        while (Date.now() < deadline) {
          const result = await pollQrStatus(qr.token, baseUrl, verifyCode)

          switch (result.status) {
            case 'wait':
              break

            case 'scaned':
              verifyCode = undefined
              if (!scanedNotified) {
                notify('已扫码，请在手机上确认登录', 'info')
                scanedNotified = true
              }
              break

            case 'need_verifycode': {
              const input = ctx.hasUI
                ? await ctx.ui.input(
                    verifyCode
                      ? '数字不匹配，请重新输入手机微信显示的数字'
                      : '请输入手机微信显示的数字以继续登录'
                  )
                : undefined
              const code = input?.trim()
              if (!code) {
                notify('该账号需要数字配对码才能登录，已取消。可稍后重新执行 /wechat-login', 'error')
                return
              }
              verifyCode = code
              // 立即重新查询，不再等待。
              continue
            }

            case 'verify_code_blocked':
              notify('配对码多次输入错误，正在刷新二维码…', 'error')
              verifyCode = undefined
              if (!(await refreshQr())) return
              continue

            case 'expired':
              notify('二维码已过期，正在刷新…', 'warning')
              if (!(await refreshQr())) return
              continue

            case 'binded_redirect':
              notify(
                [
                  '该微信已绑定过此机器人，无需重复登录。',
                  `本地凭证: ${getCredentialsPath()}`,
                  `凭证备份: ${getCredentialsBackupPath()}`,
                  '如果凭证已丢失且无法恢复，微信侧目前没有解绑入口，',
                  '只能等待腾讯提供服务端解绑能力。'
                ].join('\n'),
                'warning'
              )
              return

            case 'scaned_but_redirect':
              if (result.redirect_host) {
                baseUrl = `https://${result.redirect_host}`
                notify(`已切换微信接入点: ${result.redirect_host}`, 'info')
              }
              break

            case 'confirmed': {
              if (!result.ilink_bot_id) {
                notify('登录失败：服务端未返回 ilink_bot_id', 'error')
                return
              }

              const credentials: Credentials = {
                token: result.bot_token ?? '',
                baseUrl: result.baseurl || baseUrl,
                accountId: result.ilink_bot_id,
                userId: result.ilink_user_id ?? ''
              }

              saveCredentials(credentials)
              client = new WeixinClient(credentials)
              notify('微信登录成功', 'info')
              return
            }
          }
        }

        notify('登录超时，请重新执行 /wechat-login', 'error')
      } catch (error) {
        notify(`微信登录失败: ${formatError(error)}`, 'error')
      }
    }
  })

  pi.registerCommand('wechat-start', {
    description: '启动微信消息桥接',
    handler: async (_args, ctx) => {
      rememberContext(ctx)

      const activeClient = ensureClient()
      if (!activeClient) {
        notify('未找到微信凭证，请先执行 /wechat-login', 'error')
        return
      }

      if (running) {
        notify('微信桥接已经在运行', 'info')
        return
      }

      if (activeClient.isPaused) {
        const minutes = Math.ceil(activeClient.pauseRemainingMs / 60_000)
        notify(`微信通道处于冷却期（剩余约 ${minutes} 分钟），请稍后再试或重新执行 /wechat-login`, 'error')
        return
      }

      running = true
      pollAbortController = new AbortController()
      notify('微信桥接已启动', 'info')
      drainQueue()

      void pollMessages(activeClient).finally(() => {
        if (pollAbortController?.signal.aborted) {
          pollAbortController = null
        }
      })
    }
  })

  pi.registerCommand('wechat-stop', {
    description: '停止微信消息桥接',
    handler: async (_args, ctx) => {
      rememberContext(ctx)
      await stopBridge()
      notify('微信桥接已停止', 'info')
    }
  })

  pi.registerCommand('wechat-logout', {
    description: '清除微信凭证并停止桥接',
    handler: async (_args, ctx) => {
      rememberContext(ctx)
      await stopBridge({ clearClient: true })
      clearCredentials()
      notify(
        [
          `已清除微信凭证: ${getCredentialsPath()}`,
          `备份保留在: ${getCredentialsBackupPath()}`,
          '（本地绑定记录已保留，用于下次登录时识别"已绑定"状态）'
        ].join('\n'),
        'info'
      )
    }
  })

  pi.registerCommand('wechat-status', {
    description: '查看微信桥接状态',
    handler: async (_args, ctx) => {
      rememberContext(ctx)

      const activeClient = client ?? loadClientFromDisk()
      const paused = activeClient?.isPaused ?? false
      const lines = [
        `运行状态: ${running ? 'running' : 'stopped'}`,
        `凭证状态: ${activeClient ? 'ready' : 'missing'}`,
        `通道冷却: ${paused ? `剩余约 ${Math.ceil((activeClient?.pauseRemainingMs ?? 0) / 60_000)} 分钟` : '正常'}`,
        `账号 ID: ${activeClient?.accountId ?? '-'}`,
        `用户 ID: ${activeClient?.userId ?? '-'}`,
        `排队消息: ${inboundQueue.length}`,
        `等待注入: ${pendingInjection ? pendingInjection.preview : '-'}`,
        `处理中: ${activeRequest ? activeRequest.preview : '-'}`,
        `凭证路径: ${getCredentialsPath()}`,
        `凭证备份: ${getCredentialsBackupPath()}`
      ]

      notify(lines.join('\n'), 'info')
    }
  })

  pi.on('session_start', async (_event, ctx) => {
    rememberContext(ctx)
    client ??= loadClientFromDisk()
  })

  /** 每条 assistant 消息开始，重置投递账本。 */
  pi.on('message_start', async (event, ctx) => {
    rememberContext(ctx)
    if ((event.message as { role?: string } | undefined)?.role !== 'assistant') {
      return
    }
    resetDelivery()
  })

  /**
   * L3：token 级块流式（默认关闭）。
   * 只接受 `text_delta`，`thinking_start` / `thinking_delta` / `thinking_end` 一律忽略。
   */
  pi.on('message_update', async (event, ctx) => {
    if (!STREAM_ENABLED) return

    const request = activeRequest ?? pendingInjection
    if (!request || !client) return

    const streamEvent = event.assistantMessageEvent as { type?: string; delta?: string } | undefined
    if (streamEvent?.type !== 'text_delta' || typeof streamEvent.delta !== 'string') {
      return
    }

    delivery.streamBuffer += streamEvent.delta

    if (delivery.streamBuffer.length >= STREAM_MIN_CHARS) {
      await flushStream(request.userId, 'threshold')
    } else {
      scheduleIdleFlush(request.userId)
    }
  })

  /**
   * L1：块级投递。每条 assistant 消息完成即发出其正文（仅新增部分），
   * 对应官方“按顺序发送模型在多步工具调用之间完成的文本块”的行为。
   */
  pi.on('message_end', async (event, ctx) => {
    rememberContext(ctx)

    const request = activeRequest ?? pendingInjection
    const message = event.message as { role?: string; content?: unknown } | undefined
    if (!request || !client || message?.role !== 'assistant') {
      return
    }

    if (STREAM_ENABLED) {
      await flushStream(request.userId, 'message_end')
      return
    }

    const text = extractTextParts(message.content)
    const pending = pendingSuffix(delivery.deliveredPrefix, text)
    delivery.deliveredPrefix = text

    if (!pending.trim()) return

    await sendSegment(request.userId, pending).catch((error) => {
      notify(`发送微信回复失败: ${formatError(error)}`, 'error')
    })
  })

  /** L2：工具调用开始进度（只发工具名，绝不发入参）。 */
  pi.on('tool_call', async (event, ctx) => {
    rememberContext(ctx)

    const request = activeRequest ?? pendingInjection
    const activeClient = client
    if (!request || !activeClient) return

    // 工具执行前先把已缓冲的正文发出，保证微信端顺序正确。
    if (STREAM_ENABLED) {
      await flushStream(request.userId, 'tool_boundary')
    }

    const toolName = (event as { toolName?: string }).toolName ?? 'tool'
    if (SILENT_TOOLS.has(toolName)) return

    await activeClient
      .sendToolCallStart(request.userId, toolName, (event as { toolCallId?: string }).toolCallId)
      .catch((error) => {
        if (DEBUG_LOG) notify(`发送工具进度失败: ${formatError(error)}`, 'warning')
      })
  })

  /** L2：工具调用结束进度（只发工具名与状态，不发结果）。 */
  pi.on('tool_result', async (event, ctx) => {
    rememberContext(ctx)

    const request = activeRequest ?? pendingInjection
    const activeClient = client
    if (!request || !activeClient) return

    const toolName = (event as { toolName?: string }).toolName ?? 'tool'
    if (SILENT_TOOLS.has(toolName)) return

    const status: ToolCallStatus = (event as { isError?: boolean }).isError ? 'failed' : 'completed'

    await activeClient
      .sendToolCallResult(request.userId, toolName, status, (event as { toolCallId?: string }).toolCallId)
      .catch((error) => {
        if (DEBUG_LOG) notify(`发送工具进度失败: ${formatError(error)}`, 'warning')
      })
  })

  pi.on('before_agent_start', async (event, ctx) => {
    rememberContext(ctx)

    const request = pendingInjection ?? activeRequest
    if (!request) {
      return
    }

    return {
      systemPrompt: buildWechatSystemPrompt(event.systemPrompt, request)
    }
  })

  pi.on('agent_start', async (_event, ctx) => {
    rememberContext(ctx)
    agentIdle = false

    if (pendingInjection) {
      activeRequest = pendingInjection
      pendingInjection = null
    }
  })

  pi.on('agent_end', async (event, ctx) => {
    rememberContext(ctx)
    // 注意：这里不把 agentIdle 置为 true。agent_end 之后 pi 仍可能
    // 自动重试、自动压缩后重试，或继续执行已排队的跟进消息，
    // 真正的空闲信号是 agent_settled。
    await completeActiveRequest(event.messages as Array<{ role?: string; content?: unknown }>)
  })

  /** agent 已完全稳定（无重试/压缩/排队跟进），此时才允许注入下一条消息。 */
  pi.on('agent_settled', async (_event, ctx) => {
    rememberContext(ctx)
    agentIdle = true
    drainQueue()
  })

  pi.on('session_shutdown', async (_event, ctx) => {
    rememberContext(ctx)
    await stopBridge()
  })
}

async function renderQrCode(url: string): Promise<string> {
  return new Promise((resolve) => {
    qrcode.generate(url, { small: true }, (code) => resolve(code))
  })
}

interface TextPart {
  type: 'text'
  text: string
}

/**
 * 只提取真正的正文文本。
 *
 * pi 的 assistant content 是 `(TextContent | ThinkingContent | ToolCall)[]`，
 * 这里按 `type === 'text'` 过滤，从而排除：
 * - `ThinkingContent`（含 `thinkingSignature` 加密载荷 / `redacted` 标记）
 * - `ToolCall`（含工具入参）
 */
function extractTextParts(content: unknown): string {
  if (!Array.isArray(content)) {
    return ''
  }

  return content
    .filter((part): part is TextPart => {
      if (typeof part !== 'object' || part === null) return false
      const candidate = part as { type?: unknown; text?: unknown }
      return candidate.type === 'text' && typeof candidate.text === 'string'
    })
    .map((part) => part.text)
    .join('\n')
    .trim()
}

/**
 * 文本层兜底：剥离被 provider 混进正文的推理内容。
 *
 * pi 正常把思考放在独立的 `ThinkingContent` 里，但部分 provider/模型会把推理
 * 直接写进正文（pi 自己的 `requiresThinkingAsText` 注释也提到模型会 mimic
 * thinking 标签，所以官方刻意不生成标签）。这里只剥离明确的结构化标记，
 * 不做会误伤正常正文的模糊匹配。
 */
const THINKING_PATTERNS: readonly RegExp[] = [
  /<thinking>[\s\S]*?<\/thinking>/gi,
  /<reasoning>[\s\S]*?<\/reasoning>/gi,
  /<thought>[\s\S]*?<\/thought>/gi,
  /<analysis>[\s\S]*?<\/analysis>/gi,
  /```(?:thinking|reasoning|thought|analysis)[^\n]*\n[\s\S]*?```/gi,
  /<\/?(?:thinking|reasoning|thought|analysis)[^>]*>/gi,
  // 未闭合的开标签：直接截断到结尾
  /<(?:thinking|reasoning|thought|analysis)>[\s\S]*$/gi
]

/** 正文最开头的推理引导行（仅在后面跟有空行时才视为推理段）。 */
const LEADING_THINKING_RE =
  /^(?:thinking process|reasoning process|思考过程|推理过程|让我想想|let me think)[ \t]*[:：][ \t]*\r?\n/i

/** 引导行后允许的推理段最大长度，超过则判定为正常正文。 */
const LEADING_THINKING_MAX_CHARS = 2_000

/**
 * 剥离出现在正文最开头的推理段。
 *
 * 只有在能找到一个空行边界、且长度在限制内时才剥离；
 * 否则原样返回，避免误删正常正文。
 */
function stripLeadingThinkingBlock(text: string): string {
  const match = LEADING_THINKING_RE.exec(text)
  if (!match) return text

  const rest = text.slice(match[0].length)
  const boundary = rest.search(/\r?\n[ \t]*\r?\n/)
  if (boundary < 0 || boundary > LEADING_THINKING_MAX_CHARS) {
    return text
  }

  return rest.slice(boundary).replace(/^\s+/, '')
}

function stripThinkingText(text: string): string {
  let output = stripLeadingThinkingBlock(text)
  for (const pattern of THINKING_PATTERNS) {
    output = output.replace(pattern, '')
  }
  return output.replace(/\n{3,}/g, '\n\n').trim()
}

/** 出站内容的唯一净化入口。所有投递路径都必须经过它。 */
function sanitizeOutbound(text: string): string {
  return stripThinkingText(text).trim()
}

/**
 * 计算“尚未投递的残余”。
 *
 * - `full` 以已投递前缀开头：同一条消息在增长，只发尾部。
 * - 已投递前缀比 `full` 更长且包含它：已覆盖，不重复发送。
 * - 其余情况（例如下一条 assistant 消息）：视为新内容，整体发送。
 */
function pendingSuffix(deliveredPrefix: string, full: string): string {
  if (!deliveredPrefix) return full
  if (full.startsWith(deliveredPrefix)) return full.slice(deliveredPrefix.length)
  if (deliveredPrefix.startsWith(full)) return ''
  return full
}

/** 取最后一条含正文的 assistant 消息。 */
function extractLastAssistantText(
  messages: Array<{ role?: string; content?: unknown }>
): string | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index]
    if (message?.role !== 'assistant') continue

    const text = extractTextParts(message.content)
    if (text) return text
  }

  return null
}

function summarizePreview(text: string): string {
  const normalized = text.replace(/\s+/g, ' ').trim()
  if (normalized.length <= PREVIEW_LIMIT) {
    return normalized
  }

  return `${normalized.slice(0, PREVIEW_LIMIT - 1)}…`
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError'
}
