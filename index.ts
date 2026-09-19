import { randomUUID } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'
import type { ExtensionAPI, ExtensionCommandContext, ExtensionContext } from '@earendil-works/pi-coding-agent'
import qrcode from 'qrcode-terminal'
import { DEFAULT_BASE_URL, validateBaseUrl } from './api.js'
import {
  clearCredentials,
  clearPauseUntil,
  collectLocalTokenList,
  getCredentialsBackupPath,
  getCredentialsPath,
  getQrCode,
  loadCredentials,
  pollQrStatus,
  saveCredentials
} from './auth.js'
import {
  CursorPersistenceError,
  SessionExpiredError,
  SessionPausedError,
  WeixinClient
} from './client.js'
import {
  addInboxRecord,
  clearInbox,
  InboxPersistenceError,
  OutboxPersistenceError,
  loadPendingInbox,
  markInboxActive,
  markInboxPending,
  removeInboxRecord
} from './queue.js'
import type { Credentials, IncomingMessage, ToolCallStatus } from './types.js'

type NotificationLevel = 'info' | 'warning' | 'error'

class QueueBackpressureError extends Error {
  constructor() {
    super('INBOUND_QUEUE_FULL')
    this.name = 'QueueBackpressureError'
  }
}

interface QueuedWechatRequest {
  id: string
  accountId: string
  userId: string
  messageId: string
  receivedAt: Date
  contextToken: string
  text: string
  attempts: number
}

/** 轮询失败后的普通重试间隔与连续失败后的退避间隔（对齐官方）。 */
const RETRY_DELAY_MS = 2_000
const BACKOFF_DELAY_MS = 30_000
const MAX_CONSECUTIVE_FAILURES = 3

/** 二维码最多自动刷新次数与整个登录流程的时长上限。 */
const MAX_QR_REFRESH = 3
const LOGIN_TIMEOUT_MS = 8 * 60 * 1000

const MAX_INBOUND_QUEUE = 100
const MAX_TOTAL_INBOUND_QUEUE = 500
const MAX_INBOUND_TEXT_CHARS = 50_000
const MAX_SEEN_MESSAGE_IDS = 2_000
const MAX_DELIVERY_RETRIES = 3
const DEBUG_LOG = process.env.PI_WECHAT_DEBUG === '1'
const CONFIGURED_ALLOWED_USERS = new Set(parseUserList(process.env.PI_WECHAT_ALLOWED_USERS))
const ALLOW_SHARED_SESSION = process.env.PI_WECHAT_ALLOW_SHARED_SESSION === '1'
const ALLOW_ACCOUNT_SWITCH = process.env.PI_WECHAT_ALLOW_ACCOUNT_SWITCH === '1'

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
const INTERMEDIATE_ENABLED = STREAM_ENABLED || process.env.PI_WECHAT_INTERMEDIATE === '1'
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
  let pollLoopPromise: Promise<void> | null = null
  let lifecycleEpoch = 0
  let latestContext: ExtensionContext | ExtensionCommandContext | null = null
  let loginGeneration = 0
  let loginInProgress: Promise<void> | null = null
  let bridgeStopping = false
  let inboxRestoredForAccount: string | null = null
  let sessionAccountId: string | null = null
  let sharedSessionWarningShown = false

  const inboundQueue: QueuedWechatRequest[] = []
  const seenMessageIds = new Map<string, true>()
  let pendingInjection: QueuedWechatRequest | null = null
  let activeRequest: QueuedWechatRequest | null = null
  let completingRequest: QueuedWechatRequest | null = null
  let completionInProgress = false
  let lastAgentMessages: Array<{ role?: string; content?: unknown }> | null = null

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
  let injectionWatchdog: ReturnType<typeof setTimeout> | null = null

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

  function clearInjectionWatchdog(): void {
    if (injectionWatchdog) {
      clearTimeout(injectionWatchdog)
      injectionWatchdog = null
    }
  }

  function scheduleInjectionWatchdog(request: QueuedWechatRequest): void {
    clearInjectionWatchdog()
    injectionWatchdog = setTimeout(() => {
      injectionWatchdog = null
      if (pendingInjection?.id !== request.id || isRuntimeBusy()) return

      pendingInjection = null
      inboundQueue.unshift(request)
      notify(`微信消息未能注入 Pi，已退回队列（messageId=${request.messageId}）`, 'error')
      if (running && !isRuntimeBusy()) {
        setTimeout(() => {
          if (running) drainQueue()
        }, RETRY_DELAY_MS)
      }
    }, 1_500)
  }

  function scheduleIdleFlush(request: QueuedWechatRequest): void {
    clearIdleFlush()
    idleFlushTimer = setTimeout(() => {
      idleFlushTimer = null
      void flushStream(request, 'idle')
    }, STREAM_IDLE_MS)
  }

  /** 净化后发送一段正文；若净化后为空（整段都是思考内容）则不发送。 */
  async function sendSegment(request: QueuedWechatRequest, raw: string): Promise<boolean> {
    const activeClient = client
    if (!activeClient || activeClient.accountId !== request.accountId) return false

    const text = sanitizeOutbound(raw)
    if (!text) {
      if (raw.trim()) {
        notify('已跳过一段仅含思考内容的输出，未发送到微信', 'warning')
      }
      return false
    }

    // WeixinClient 在单个 message/client_id 内完成重试。
    // 这里不能再重试整个 sendText：长文本可能已经成功发送了前几片，
    // 从头重试会把已投递片段重复发给用户。
    await activeClient.sendText(request.userId, text, request.contextToken, (chunk) => {
      delivery.deliveredPrefix += chunk
    })
    return true
  }

  /** 把流式缓冲区里的内容发出（L3 专用）。 */
  async function flushStream(request: QueuedWechatRequest, reason: string): Promise<void> {
    clearIdleFlush()

    const buffer = delivery.streamBuffer
    const prefixBefore = delivery.deliveredPrefix
    delivery.streamBuffer = ''
    if (!buffer.trim()) return
    const normalizedBuffer = sanitizeOutbound(buffer)
    if (!normalizedBuffer) return

    if (DEBUG_LOG) {
      notify(`流式 flush（${reason}，${normalizedBuffer.length} 字）`, 'info')
    }

    try {
      const sent = await sendSegment(request, normalizedBuffer)
      if (!sent) {
        delivery.streamBuffer = pendingSuffix(
          delivery.deliveredPrefix,
          prefixBefore + normalizedBuffer
        ) + delivery.streamBuffer
      }
    } catch (error) {
      // 保留尚未投递的尾部，避免把已经成功发送的前几片再次入队。
      delivery.streamBuffer = pendingSuffix(
        delivery.deliveredPrefix,
        prefixBefore + normalizedBuffer
      ) + delivery.streamBuffer
      notify(`发送微信回复失败: ${formatError(error)}`, 'error')
    }
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
    if (!client || client.isClosed) {
      client = loadClientFromDisk()
      if (!client) inboxRestoredForAccount = null
    }
    if (client) rememberSessionAccount(client.accountId)
    restoreInbox()
    return client
  }

  function rememberSessionAccount(accountId: string): void {
    if (!sessionAccountId) sessionAccountId = accountId
  }

  function restoreInbox(): void {
    const activeClient = client
    if (!activeClient || inboxRestoredForAccount === activeClient.accountId) return

    try {
      const records = loadPendingInbox(activeClient.accountId)
      let restoredAll = true
      for (const record of records) {
        if (!isAllowedUser(record.userId, activeClient)) {
          // Configured users are retained when shared-session mode is not
          // explicitly enabled, so correcting the configuration cannot lose them.
          if (!(CONFIGURED_ALLOWED_USERS.size > 1
            && CONFIGURED_ALLOWED_USERS.has(record.userId)
            && !ALLOW_SHARED_SESSION)) {
            removeInboxRecord(activeClient.accountId, record.id)
          }
          continue
        }
        if (queuedCountForAccount(activeClient.accountId) >= MAX_INBOUND_QUEUE
          || inboundQueue.length >= MAX_TOTAL_INBOUND_QUEUE) {
          notify(`持久化微信队列超过上限（每账号 ${MAX_INBOUND_QUEUE}，总计 ${MAX_TOTAL_INBOUND_QUEUE}），剩余消息暂不恢复`, 'warning')
          restoredAll = false
          break
        }
        const messageKey = makeMessageKey(record.accountId, record.userId, record.messageId)
        if (seenMessageIds.has(messageKey)) continue
        seenMessageIds.set(messageKey, true)
        inboundQueue.push({
          id: record.id,
          accountId: record.accountId,
          userId: record.userId,
          messageId: record.messageId,
          receivedAt: new Date(record.receivedAt),
          contextToken: record.contextToken,
          text: record.text,
          attempts: record.attempts
        })
      }
      inboxRestoredForAccount = restoredAll ? activeClient.accountId : null
      if (running) drainQueue()
    } catch (error) {
      notify(`恢复微信持久化队列失败: ${formatError(error)}`, 'error')
    }
  }

  async function stopBridge(options?: {
    clearClient?: boolean
    clearQueue?: boolean
    waitForPoll?: boolean
    invalidateClient?: boolean
  }): Promise<void> {
    running = false
    const stopEpoch = ++lifecycleEpoch
    const oldPoll = pollLoopPromise
    const oldClient = client
    const oldActiveRequest = activeRequest
    if (options?.invalidateClient) {
      oldClient?.close()
    }
    pollAbortController?.abort()
    pollAbortController = null

    if (options?.waitForPoll !== false && oldPoll) {
      await oldPoll.catch(() => {})
    }
    // A concurrent start/login/logout has installed a newer lifecycle. Do not
    // let this older stop operation tear down the new client or queue.
    if (lifecycleEpoch !== stopEpoch) return

    clearIdleFlush()
    clearInjectionWatchdog()

    if (oldActiveRequest && oldClient) {
      await oldClient.stopTyping(oldActiveRequest.userId, oldActiveRequest.contextToken).catch(() => {})
      if (lifecycleEpoch !== stopEpoch) return
    }

    if (options?.clearQueue !== false) {
      client?.rollbackUncommittedCursor()
      if (client) {
        try {
          clearInbox(client.accountId)
        } catch (error) {
          notify(`清理微信入站队列失败: ${formatError(error)}`, 'error')
        }
        try {
          client.clearPendingOutbound()
        } catch (error) {
          notify(`清理微信出站队列失败: ${formatError(error)}`, 'error')
        }
      }
      inboundQueue.length = 0
      seenMessageIds.clear()
    }

    if (options?.clearQueue === false) {
      if (options.invalidateClient && completingRequest) inboundQueue.unshift(completingRequest)
      if (activeRequest) inboundQueue.unshift(activeRequest)
      if (pendingInjection) inboundQueue.unshift(pendingInjection)
    }
    pendingInjection = null
    activeRequest = null
    completingRequest = null
    completionInProgress = false
    lastAgentMessages = null
    resetDelivery()

    if (options?.clearClient) {
      client = null
      inboxRestoredForAccount = null
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

  function queueIncomingMessage(activeClient: WeixinClient, message: IncomingMessage): boolean {
    const messageKey = makeMessageKey(activeClient.accountId, message.userId, message.messageId)
    if (seenMessageIds.has(messageKey)) {
      return false
    }
    if (!isAllowedUser(message.userId, activeClient)) {
      rememberMessageId(messageKey)
      if (DEBUG_LOG) {
        notify(`忽略未授权微信用户: ${message.userId}`, 'warning')
      }
      return false
    }

    if (queuedCountForAccount(activeClient.accountId) >= MAX_INBOUND_QUEUE
      || inboundQueue.length >= MAX_TOTAL_INBOUND_QUEUE) {
      throw new QueueBackpressureError()
    }

    const text = message.text.length > MAX_INBOUND_TEXT_CHARS
      ? `${message.text.slice(0, MAX_INBOUND_TEXT_CHARS)}\n[消息过长，后续内容已截断]`
      : message.text
    const request: QueuedWechatRequest = {
      id: randomUUID(),
      accountId: activeClient.accountId,
      userId: message.userId,
      messageId: message.messageId,
      receivedAt: message.timestamp,
      contextToken: message.contextToken,
      text,
      attempts: 0
    }

    const persisted = addInboxRecord({
      id: request.id,
      accountId: activeClient.accountId,
      userId: request.userId,
      messageId: request.messageId,
      contextToken: request.contextToken,
      text: request.text,
      receivedAt: request.receivedAt.getTime(),
      attempts: 0
    })
    rememberMessageId(messageKey)
    if (!persisted) return false

    activeClient.rememberContext(message.raw)
    inboundQueue.push(request)
    if (DEBUG_LOG) {
      notify(`收到微信消息，已排队（messageId=${request.messageId}）`, 'info')
    }
    drainQueue()
    return true
  }

  function isAllowedUser(userId: string, activeClient = client): boolean {
    if (CONFIGURED_ALLOWED_USERS.size > 1 && !ALLOW_SHARED_SESSION) {
      if (!sharedSessionWarningShown) {
        notify(
          '检测到多个微信白名单用户；为避免共享 Pi 上下文，默认拒绝。请只保留一个用户，或显式设置 PI_WECHAT_ALLOW_SHARED_SESSION=1',
          'error'
        )
        sharedSessionWarningShown = true
      }
      return false
    }
    if (CONFIGURED_ALLOWED_USERS.size > 0) {
      return CONFIGURED_ALLOWED_USERS.has(userId)
    }
    return Boolean(activeClient?.userId && activeClient.userId === userId)
  }

  function makeMessageKey(accountId: string, userId: string, messageId: string): string {
    return JSON.stringify([accountId, userId, messageId])
  }

  function queuedCountForAccount(accountId: string): number {
    return inboundQueue.reduce((count, request) => count + (request.accountId === accountId ? 1 : 0), 0)
  }

  function rememberMessageId(messageKey: string): void {
    seenMessageIds.delete(messageKey)
    seenMessageIds.set(messageKey, true)
    while (seenMessageIds.size > MAX_SEEN_MESSAGE_IDS) {
      const oldest = seenMessageIds.keys().next().value
      if (oldest === undefined) break
      seenMessageIds.delete(oldest)
    }
  }

  /**
   * 向运行时确认是否仍在运行。
   *
   * `agentIdle` 是扩展自己的记账；`ctx.isIdle()` 是运行时的权威判断，
   * 这里再确认一次，避免在流式过程中注入消息。
   */
  function isRuntimeBusy(): boolean {
    try {
      return latestContext ? !latestContext.isIdle() : true
    } catch {
      return true
    }
  }

  function drainQueue(): void {
    const activeClient = client
    if (!running || !activeClient || activeClient.isPaused || !agentIdle || completionInProgress || isRuntimeBusy() || pendingInjection || activeRequest) {
      return
    }

    const nextIndex = inboundQueue.findIndex((request) => request.accountId === activeClient.accountId)
    if (nextIndex < 0) {
      return
    }
    const [next] = inboundQueue.splice(nextIndex, 1)
    if (!next) {
      return
    }

    pendingInjection = next
    void activeClient.sendTyping(next.userId, next.contextToken).catch(() => {})
    // deliverAs 仅作为兜底：agent 空闲时该选项会被忽略，
    // 万一仍处于流式状态，则改为排队而不是抛错丢弃消息。
    const rollbackInjection = (error: unknown): void => {
      // sendUserMessage 可能在 before_agent_start 前因竞态同步或异步失败。
      // 如果不回滚 pendingInjection，这条消息会永久卡在“等待注入”状态。
      if (pendingInjection?.id === next.id) {
        clearInjectionWatchdog()
        pendingInjection = null
        inboundQueue.unshift(next)
      }
      notify(`微信消息注入 Pi 失败，已保留在队列: ${formatError(error)}`, 'error')
      if (running && !isRuntimeBusy()) {
        setTimeout(() => {
          if (running) drainQueue()
        }, RETRY_DELAY_MS)
      }
    }

    try {
      const result = (pi.sendUserMessage as unknown as (
        text: string,
        options: { deliverAs: 'followUp' }
      ) => unknown)(next.text, { deliverAs: 'followUp' })
      if (result && typeof (result as { then?: unknown }).then === 'function') {
        void Promise.resolve(result).catch(rollbackInjection)
      }
    } catch (error) {
      rollbackInjection(error)
    }
    if (pendingInjection?.id === next.id) {
      scheduleInjectionWatchdog(next)
    }
  }

  async function completeActiveRequest(messages: Array<{ role?: string; content?: unknown }>): Promise<void> {
    // 只有已经在 agent_start/message_start 中确认开始处理的请求才能收尾；
    // pendingInjection 可能只是一次尚未成功的注入，不能拿本地 run 的消息冒充回复。
    const request = activeRequest
    const completionEpoch = lifecycleEpoch
    completionInProgress = true
    completingRequest = request
    activeRequest = null
    clearIdleFlush()

    const activeClient = client
    if (!request || !activeClient) {
      resetDelivery()
      completionInProgress = false
      if (!isRuntimeBusy()) {
        agentIdle = true
      }
      restoreInbox()
      drainQueue()
      return
    }

    let deliverySucceeded = false

    try {
      // 流式模式下缓冲区里可能还有未发出的尾部。
      if (STREAM_ENABLED && delivery.streamBuffer.trim()) {
        await flushStream(request, 'settled')
      }

      const rawFull = extractLastAssistantText(messages)
      const full = rawFull ? sanitizeOutbound(rawFull) : ''
      if (!full) {
        notify(`Pi 没有产出可发送的文本回复（messageId=${request.messageId}）`, 'warning')
        deliverySucceeded = await sendSegment(
          request,
          '抱歉，本次处理没有生成可发送的文本回复，请稍后重试。'
        )
        return
      }

      // 只补发尚未投递的残余，避免与块级/流式投递重复。
      const pending = pendingSuffix(delivery.deliveredPrefix, full)
      if (!pending.trim()) {
        deliverySucceeded = true
      } else {
        try {
          deliverySucceeded = await sendSegment(request, pending)
        } catch (firstError) {
          // sendText reports chunk progress through delivery.deliveredPrefix;
          // retry only the unsent suffix, never the already delivered prefix.
          const remaining = pendingSuffix(delivery.deliveredPrefix, full)
          if (!remaining.trim()) {
            deliverySucceeded = true
          } else {
            try {
              deliverySucceeded = await sendSegment(request, remaining)
            } catch {
              throw firstError
            }
          }
        }
      }
    } catch (error) {
      notify(`发送微信回复失败: ${formatError(error)}`, 'error')
    } finally {
      if (activeClient.accountId === request.accountId) {
        await activeClient.stopTyping(request.userId, request.contextToken).catch(() => {})
      }
      const sameClient = client === activeClient
        && !activeClient.isClosed
        && activeClient.accountId === request.accountId
      const sameLifecycle = lifecycleEpoch === completionEpoch && sameClient
      try {
        if (deliverySucceeded) {
          // The inbox record belongs to the request's original account. It is
          // safe to acknowledge it even if a concurrent login installed another
          // client; do not requeue an already delivered reply.
          removeInboxRecord(request.accountId, request.id)
        } else if (sameClient) {
          if (request.attempts < MAX_DELIVERY_RETRIES) {
            const retryRequest = { ...request, attempts: request.attempts + 1 }
            markInboxPending(request.accountId, request.id, retryRequest.attempts)
            if (queuedCountForAccount(request.accountId) < MAX_INBOUND_QUEUE
              && inboundQueue.length < MAX_TOTAL_INBOUND_QUEUE) {
              inboundQueue.unshift(retryRequest)
            } else {
              inboxRestoredForAccount = null
            }
            notify(
              `微信回复发送失败，已保留消息待重试（messageId=${request.messageId}，第 ${retryRequest.attempts} 次）`,
              'error'
            )
          } else {
            removeInboxRecord(request.accountId, request.id)
            notify(`微信回复多次失败，已停止重试（messageId=${request.messageId}）`, 'error')
          }
        } else if (client && !client.isClosed
          && !inboundQueue.some((item) => item.id === request.id)) {
          // A login/account switch happened while the old agent was settling.
          // Keep the request, but drainQueue will only use it when its account is active again.
          inboundQueue.unshift(request)
        }
      } catch (error) {
        notify(`更新微信消息投递状态失败: ${formatError(error)}`, 'error')
      }
      resetDelivery()
      completionInProgress = false
      completingRequest = null
      // 兜底：若运行时已确认空闲（例如宿主未派发 agent_settled），
      // 这里直接恢复出队，避免队列停滞。
      if (!isRuntimeBusy()) {
        agentIdle = true
      }
      if (sameLifecycle || (sameClient && running)) {
        restoreInbox()
        drainQueue()
      }
    }
  }

  async function pollMessages(
    activeClient: WeixinClient,
    signal: AbortSignal,
    epoch: number
  ): Promise<void> {
    let consecutiveFailures = 0

    while (running && lifecycleEpoch === epoch && client === activeClient && !signal.aborted) {
      try {
        const messages = await activeClient.getUpdates(signal)
        if (!running || lifecycleEpoch !== epoch || client !== activeClient || signal.aborted) {
          activeClient.rollbackUncommittedCursor()
          break
        }
        consecutiveFailures = 0
        if (DEBUG_LOG && messages.length > 0) {
          notify(`轮询收到 ${messages.length} 条可处理消息，正在持久化入站队列`, 'info')
        }

        for (const message of messages) {
          queueIncomingMessage(activeClient, message)
        }
        activeClient.commitCursor()
      } catch (error) {
        if (signal.aborted || lifecycleEpoch !== epoch || client !== activeClient) {
          activeClient.rollbackUncommittedCursor()
          break
        }
        activeClient.rollbackUncommittedCursor()
        if (isAbortError(error)) {
          break
        }

        if (error instanceof QueueBackpressureError) {
          notify(`微信消息队列已满（${MAX_INBOUND_QUEUE}），已暂停轮询以避免丢消息`, 'error')
          await stopBridge({ clearQueue: false, waitForPoll: false })
          break
        }

        if (error instanceof InboxPersistenceError) {
          notify(`微信入站队列无法持久化，已暂停轮询以避免丢消息: ${formatError(error)}`, 'error')
          await stopBridge({ clearQueue: false, waitForPoll: false })
          break
        }

        if (error instanceof CursorPersistenceError) {
          notify('微信游标无法持久化，已停止轮询以避免丢消息；请检查 ~/.pi-wechat 的权限', 'error')
          await stopBridge({ clearQueue: false, waitForPoll: false })
          break
        }

        if (error instanceof SessionExpiredError) {
          const minutes = Math.ceil(activeClient.pauseRemainingMs / 60_000)
          notify(`微信 session 已过期，通道已冷却 ${minutes} 分钟。之后请执行 /wechat-login 重新登录`, 'error')
          await stopBridge({ clearQueue: false, waitForPoll: false })
          break
        }

        if (error instanceof SessionPausedError) {
          const minutes = Math.ceil(error.remainingMs / 60_000)
          notify(`微信通道处于冷却期，剩余约 ${minutes} 分钟`, 'warning')
          await stopBridge({ clearQueue: false, waitForPoll: false })
          break
        }

        consecutiveFailures += 1

        if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
          consecutiveFailures = 0
          notify(`微信轮询连续失败，退避 ${BACKOFF_DELAY_MS / 1000}s: ${formatError(error)}`, 'warning')
          await delayWithAbort(BACKOFF_DELAY_MS, signal)
        } else {
          notify(`微信轮询失败: ${formatError(error)}`, 'warning')
          await delayWithAbort(RETRY_DELAY_MS, signal)
        }
      }
    }
  }

  pi.registerCommand('wechat-login', {
    description: '扫码登录微信 iLink Bot（--force 忽略本地凭证并强制重新绑定）',
    handler: async (args, ctx) => {
      rememberContext(ctx)
      if (loginInProgress) {
        notify('已有微信登录流程正在进行，请等待其结束', 'warning')
        return
      }

      let releaseLogin!: () => void
      loginInProgress = new Promise<void>((resolve) => {
        releaseLogin = resolve
      })

      const flags = new Set(args.split(/\s+/).filter(Boolean))
      const force = flags.has('--force')
      const currentLogin = ++loginGeneration
      const isCurrentLogin = (): boolean => loginGeneration === currentLogin

      try {
      // 运行中的桥接不能被一个“加载本地凭证”的 login 命令替换 client。
      // 否则旧轮询会因 client 身份不一致退出，而 running 仍保持 true。
      if (running) {
        if (!force) {
          notify('微信桥接已经在运行；如需重新登录请使用 /wechat-login --force', 'warning')
          return
        }
        await stopBridge({ clearQueue: false, invalidateClient: true })
      }

      if (!force) {
        const cached = loadClientFromDisk()
        if (cached) {
          if (client && client !== cached) client.close()
          client = cached
          rememberSessionAccount(client.accountId)
          restoreInbox()
          notify(`已加载本地微信凭证: ${getCredentialsPath()}`, 'info')
          return
        }
      }

      const localTokens = (): string[] => (force ? [] : collectLocalTokenList())

      try {
        let baseUrl = DEFAULT_BASE_URL
        let scanedNotified = false
        let verifyCode: string | undefined
        let refreshCount = 1
        let qr = await getQrCode(baseUrl, localTokens())
        if (!isCurrentLogin()) return

        const showQr = async (label: string): Promise<void> => {
          if (!isCurrentLogin()) return
          const qrText = await renderQrCode(qr.url)
          if (isCurrentLogin()) {
            notify(`${label}\n\n${qrText}\n\n二维码链接：${qr.url}`, 'info')
          }
        }

        const refreshQr = async (): Promise<boolean> => {
          if (!isCurrentLogin()) return false
          refreshCount += 1
          if (refreshCount > MAX_QR_REFRESH) {
            notify('二维码多次失效，登录流程已停止，请稍后重试', 'error')
            return false
          }

          qr = await getQrCode(baseUrl, localTokens())
          if (!isCurrentLogin()) return false
          scanedNotified = false
          verifyCode = undefined
          await showQr(`二维码已刷新（${refreshCount}/${MAX_QR_REFRESH}），请重新扫描：`)
          return isCurrentLogin()
        }

        await showQr('请用微信扫码登录：')

        const deadline = Date.now() + LOGIN_TIMEOUT_MS

        while (Date.now() < deadline) {
          if (!isCurrentLogin()) return
          const result = await pollQrStatus(qr.token, baseUrl, verifyCode)
          if (!isCurrentLogin()) return

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
              if (!isCurrentLogin()) return
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
                try {
                  const rawHost = result.redirect_host.trim()
                  baseUrl = validateBaseUrl(
                    rawHost.includes('://') ? rawHost : `https://${rawHost}`
                  )
                } catch (error) {
                  notify(`登录失败：服务端下发了不安全的接入点 (${formatError(error)})`, 'error')
                  return
                }
                notify(`已切换微信接入点: ${result.redirect_host}`, 'info')
              }
              break

            case 'confirmed': {
              if (!isCurrentLogin()) return
              if (!result.ilink_bot_id || !result.bot_token?.trim() || !result.ilink_user_id?.trim()) {
                notify('登录失败：服务端缺少 bot_token、ilink_bot_id 或 ilink_user_id', 'error')
                return
              }

              let credentialsBaseUrl: string
              try {
                credentialsBaseUrl = validateBaseUrl(result.baseurl || baseUrl)
              } catch (error) {
                notify(`登录失败：服务端下发了不安全的 baseurl (${formatError(error)})`, 'error')
                return
              }

              const credentials: Credentials = {
                token: result.bot_token.trim(),
                baseUrl: credentialsBaseUrl,
                accountId: result.ilink_bot_id.trim(),
                userId: result.ilink_user_id.trim()
              }

              if (sessionAccountId
                && sessionAccountId !== credentials.accountId
                && !ALLOW_ACCOUNT_SWITCH) {
                notify(
                  '当前 Pi 会话已经绑定其他微信账号；请在新 Pi 会话中登录，或显式设置 PI_WECHAT_ALLOW_ACCOUNT_SWITCH=1',
                  'error'
                )
                return
              }

              saveCredentials(credentials)
              // 显式扫码获得了新 token，允许该新 session 重新开始；
              // 普通 reload/load cached credentials 仍会遵守持久化冷却。
              clearPauseUntil(credentials.accountId)
              if (client) client.close()
              client = new WeixinClient(credentials)
              rememberSessionAccount(client.accountId)
              restoreInbox()
              notify('微信登录成功', 'info')
              return
            }
          }
        }

        notify('登录超时，请重新执行 /wechat-login', 'error')
        } catch (error) {
          if (isCurrentLogin()) {
            notify(`微信登录失败: ${formatError(error)}`, 'error')
          }
        }
      } finally {
        if (force && isCurrentLogin() && client?.isClosed) {
          client = loadClientFromDisk()
          if (client) rememberSessionAccount(client.accountId)
          restoreInbox()
        }
        releaseLogin()
        loginInProgress = null
      }
    }
  })

  pi.registerCommand('wechat-start', {
    description: '启动微信消息桥接',
    handler: async (_args, ctx) => {
      rememberContext(ctx)
      if (loginInProgress) {
        notify('微信登录流程正在进行，请登录完成后再启动桥接', 'warning')
        return
      }
      if (bridgeStopping) {
        notify('微信桥接正在停止，请稍后再启动', 'warning')
        return
      }
      if (running) {
        drainQueue()
        notify('微信桥接已经在运行', 'info')
        return
      }

      const waitEpoch = lifecycleEpoch
      const oldPoll = pollLoopPromise
      if (oldPoll) {
        await oldPoll.catch(() => {})
      }
      if (bridgeStopping || lifecycleEpoch !== waitEpoch) {
        return
      }

      const activeClient = ensureClient()
      if (!activeClient) {
        notify('未找到微信凭证，请先执行 /wechat-login', 'error')
        return
      }
      if (CONFIGURED_ALLOWED_USERS.size > 1 && !ALLOW_SHARED_SESSION) {
        notify(
          '多个白名单用户会共享当前 Pi 上下文；请只保留一个用户，或设置 PI_WECHAT_ALLOW_SHARED_SESSION=1 后再启动',
          'error'
        )
        return
      }

      if (running) {
        drainQueue()
        notify('微信桥接已经在运行', 'info')
        return
      }

      if (activeClient.isPaused) {
        const minutes = Math.ceil(activeClient.pauseRemainingMs / 60_000)
        notify(`微信通道处于冷却期（剩余约 ${minutes} 分钟），请稍后再试或重新执行 /wechat-login`, 'error')
        return
      }

      const startEpoch = lifecycleEpoch
      try {
        await activeClient.flushPendingOutbound()
      } catch (error) {
        if (error instanceof OutboxPersistenceError) {
          notify(`恢复微信出站队列失败，仍将启动轮询: ${formatError(error)}`, 'error')
        } else {
          notify(`重发微信待发送消息失败，仍将启动轮询: ${formatError(error)}`, 'warning')
        }
      }

      if (lifecycleEpoch !== startEpoch || client !== activeClient || activeClient.isClosed) {
        return
      }
      if (activeClient.isPaused) {
        notify(`微信通道进入冷却期（剩余约 ${Math.ceil(activeClient.pauseRemainingMs / 60_000)} 分钟）`, 'error')
        return
      }

      running = true
      const epoch = ++lifecycleEpoch
      const pollController = new AbortController()
      pollAbortController = pollController
      notify('微信桥接已启动', 'info')
      drainQueue()

      const pollPromise = pollMessages(activeClient, pollController.signal, epoch)
      pollLoopPromise = pollPromise
      void pollPromise
        .catch((error) => {
          if (!isAbortError(error)) {
            notify(`微信轮询已停止: ${formatError(error)}`, 'error')
          }
        })
        .finally(() => {
          // 只清理属于当前轮询实例的状态，避免旧轮询覆盖新轮询。
          if (pollLoopPromise === pollPromise) pollLoopPromise = null
          if (pollAbortController !== pollController) return
          pollAbortController = null
          if (running && lifecycleEpoch === epoch && client === activeClient && !pollController.signal.aborted) {
            running = false
            notify('微信轮询已停止，请执行 /wechat-start 重试', 'error')
          }
        })
    }
  })

  pi.registerCommand('wechat-stop', {
    description: '停止微信消息桥接',
    handler: async (_args, ctx) => {
      rememberContext(ctx)
      if (bridgeStopping) return
      bridgeStopping = true
      loginGeneration += 1
      try {
        await stopBridge({ clearQueue: false })
      } finally {
        bridgeStopping = false
      }
      notify('微信桥接已停止（待处理消息已保留）', 'info')
    }
  })

  pi.registerCommand('wechat-logout', {
    description: '清除微信凭证并停止桥接',
    handler: async (_args, ctx) => {
      rememberContext(ctx)
      if (bridgeStopping) return
      bridgeStopping = true
      loginGeneration += 1
      try {
        await stopBridge({ clearClient: true, invalidateClient: true })
      } finally {
        bridgeStopping = false
      }
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

      const activeClient = ensureClient()
      const paused = activeClient?.isPaused ?? false
      const lines = [
        `运行状态: ${running ? 'running' : 'stopped'}`,
        `凭证状态: ${activeClient ? 'ready' : 'missing'}`,
        `通道冷却: ${paused ? `剩余约 ${Math.ceil((activeClient?.pauseRemainingMs ?? 0) / 60_000)} 分钟` : '正常'}`,
        `账号 ID: ${activeClient?.accountId ?? '-'}`,
        `用户 ID: ${activeClient?.userId ?? '-'}`,
        `排队消息: ${inboundQueue.length}`,
        `等待注入: ${pendingInjection ? `messageId=${pendingInjection.messageId}` : '-'}`,
        `处理中: ${activeRequest ? `messageId=${activeRequest.messageId}` : '-'}`,
        `凭证路径: ${getCredentialsPath()}`,
        `凭证备份: ${getCredentialsBackupPath()}`
      ]

      notify(lines.join('\n'), 'info')
    }
  })

  pi.on('session_start', async (_event, ctx) => {
    rememberContext(ctx)
    client ??= loadClientFromDisk()
    if (client) rememberSessionAccount(client.accountId)
    restoreInbox()
  })

  // 不允许本地输入和微信请求共享同一个 activeRequest，避免把本地回复误发到微信。
  // Extension 注入的消息必须放行。
  pi.on('input', async (event, ctx) => {
    rememberContext(ctx)
    if (event.source !== 'extension' && (activeRequest || pendingInjection || completionInProgress)) {
      notify('当前正在处理微信消息，请等待处理完成后再输入本地消息', 'warning')
      return { action: 'handled' as const }
    }
    return { action: 'continue' as const }
  })

  /** 每条 assistant 消息开始，重置投递账本。 */
  pi.on('message_start', async (event, ctx) => {
    rememberContext(ctx)
    const message = event.message as { role?: string; content?: unknown } | undefined

    // follow-up 消息可能在同一个低层 agent run 中处理，不一定再次触发
    // agent_start；以实际的 user message 事件确认 pendingInjection 已开始。
    if (message?.role === 'user' && pendingInjection) {
      const text = extractTextParts(message.content)
      if (text === pendingInjection.text) {
        clearInjectionWatchdog()
        activeRequest = pendingInjection
        pendingInjection = null
        try {
          if (client?.accountId === activeRequest.accountId) {
            markInboxActive(client.accountId, activeRequest.id)
          }
        } catch (error) {
          notify(`标记微信消息处理中失败: ${formatError(error)}`, 'error')
        }
      }
    }

    if (message?.role !== 'assistant') {
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

    const request = activeRequest
    if (!request || !client || client.accountId !== request.accountId) return

    const streamEvent = event.assistantMessageEvent as { type?: string; delta?: string } | undefined
    if (streamEvent?.type !== 'text_delta' || typeof streamEvent.delta !== 'string') {
      return
    }

    delivery.streamBuffer += streamEvent.delta

    if (delivery.streamBuffer.length >= STREAM_MIN_CHARS) {
      await flushStream(request, 'threshold')
    } else {
      scheduleIdleFlush(request)
    }
  })

  /**
   * L1：块级投递。每条 assistant 消息完成即发出其正文（仅新增部分），
   * 对应官方“按顺序发送模型在多步工具调用之间完成的文本块”的行为。
   */
  pi.on('message_end', async (event, ctx) => {
    rememberContext(ctx)

    // Only a text-correlated active request may receive assistant output.
    // pendingInjection is reserved for before_agent_start and must not let a
    // local command or another extension leak its output to WeChat.
    const request = activeRequest
    const message = event.message as { role?: string; content?: unknown } | undefined
    if (!request || !client || client.accountId !== request.accountId || message?.role !== 'assistant') {
      return
    }

    if (!INTERMEDIATE_ENABLED) {
      return
    }

    if (STREAM_ENABLED) {
      await flushStream(request, 'message_end')
      return
    }

    const text = sanitizeOutbound(extractTextParts(message.content))
    const pending = pendingSuffix(delivery.deliveredPrefix, text)

    if (!pending.trim()) {
      if (text.length >= delivery.deliveredPrefix.length) {
        delivery.deliveredPrefix = text
      }
      return
    }

    try {
      if (await sendSegment(request, pending)) {
        // 只有真正发送成功后才更新投递账本；失败时 agent_end/settled 仍会重试。
        delivery.deliveredPrefix = text
      }
    } catch (error) {
      notify(`发送微信回复失败: ${formatError(error)}`, 'error')
    }
  })

  /** L2：工具调用开始进度（只发工具名，绝不发入参）。 */
  pi.on('tool_call', async (event, ctx) => {
    rememberContext(ctx)

    const request = activeRequest
    const activeClient = client
    if (!request || !activeClient || activeClient.accountId !== request.accountId) return

    // 工具执行前先把已缓冲的正文发出，保证微信端顺序正确。
    if (STREAM_ENABLED) {
      await flushStream(request, 'tool_boundary')
    }

    const toolName = (event as { toolName?: string }).toolName ?? 'tool'
    if (SILENT_TOOLS.has(toolName)) return

    await activeClient
      .sendToolCallStart(
        request.userId,
        toolName,
        (event as { toolCallId?: string }).toolCallId,
        request.contextToken
      )
      .catch((error) => {
        if (DEBUG_LOG) notify(`发送工具进度失败: ${formatError(error)}`, 'warning')
      })
  })

  /** L2：工具调用结束进度（只发工具名与状态，不发结果）。 */
  pi.on('tool_result', async (event, ctx) => {
    rememberContext(ctx)

    const request = activeRequest
    const activeClient = client
    if (!request || !activeClient || activeClient.accountId !== request.accountId) return

    const toolName = (event as { toolName?: string }).toolName ?? 'tool'
    if (SILENT_TOOLS.has(toolName)) return

    const status: ToolCallStatus = (event as { isError?: boolean }).isError ? 'failed' : 'completed'

    await activeClient
      .sendToolCallResult(
        request.userId,
        toolName,
        status,
        (event as { toolCallId?: string }).toolCallId,
        request.contextToken
      )
      .catch((error) => {
        if (DEBUG_LOG) notify(`发送工具进度失败: ${formatError(error)}`, 'warning')
      })
  })

  pi.on('before_agent_start', async (event, ctx) => {
    rememberContext(ctx)

    const request = pendingInjection ?? activeRequest
    const prompt = (event as { prompt?: unknown }).prompt
    if (!request
      || !client
      || client.accountId !== request.accountId
      || typeof prompt !== 'string'
      || prompt.trim() !== request.text.trim()) {
      return
    }

    return {
      systemPrompt: buildWechatSystemPrompt(event.systemPrompt, request)
    }
  })

  pi.on('agent_start', async (_event, ctx) => {
    rememberContext(ctx)
    agentIdle = false
    // Do not promote pendingInjection here. Other extensions or slash commands
    // can start an agent without consuming our exact injected user message;
    // message_start performs the text-correlated promotion below.
  })

  pi.on('agent_end', async (event, ctx) => {
    rememberContext(ctx)
    // agent_end 只代表一个底层 run 结束。Pi 可能随后自动重试、压缩后重试，
    // 因此不能在这里清空 activeRequest；只保存最近一轮的消息，等 settled 再收尾。
    const requestInFlight = activeRequest !== null
    lastAgentMessages = requestInFlight
      ? event.messages as Array<{ role?: string; content?: unknown }>
      : null

    // 仅在 agent_settled 中收尾；agent_end 之后宿主可能还会自动重试、压缩或
    // 处理 follow-up，不能因为 ctx.isIdle() 暂时为真就提前发送中间结果。
  })

  /** agent 已完全稳定（无重试/压缩/排队跟进），此时才允许注入下一条消息。 */
  pi.on('agent_settled', async (_event, ctx) => {
    rememberContext(ctx)
    // 先锁住出队，直到最终回复发送完成；否则 sendSegment 的 await 期间
    // 新微信消息可能抢先启动下一轮并覆盖 activeRequest/delivery。
    agentIdle = false

    const messages = lastAgentMessages
    const requestInFlight = activeRequest !== null
    if (pendingInjection && !requestInFlight) {
      clearInjectionWatchdog()
      inboundQueue.unshift(pendingInjection)
      pendingInjection = null
    }
    lastAgentMessages = null
    if (requestInFlight) {
      await completeActiveRequest(messages ?? [])
    } else {
      agentIdle = true
      drainQueue()
    }
  })

  pi.on('session_shutdown', async (_event, ctx) => {
    rememberContext(ctx)
    loginGeneration += 1
    await stopBridge({ clearQueue: false, invalidateClient: true })
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
  if (typeof content === 'string') {
    return content.trim()
  }
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

function parseUserList(value: string | undefined): string[] {
  if (!value) return []
  return [...new Set(value.split(',').map((item) => item.trim()).filter(Boolean))]
}

function formatError(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error)
  const safe = message.replace(/[\u0000-\u001f\u007f]/g, ' ').trim()
  return safe.length <= 240 ? safe : `${safe.slice(0, 239)}…`
}

async function delayWithAbort(milliseconds: number, signal: AbortSignal): Promise<void> {
  try {
    await delay(milliseconds, undefined, { signal })
  } catch (error) {
    if (!isAbortError(error)) throw error
  }
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError'
}
