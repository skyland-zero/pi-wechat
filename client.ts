import { setTimeout as delay } from 'node:timers/promises'
import {
  ApiError,
  ERR_RATE_LIMIT,
  ERR_SESSION_EXPIRED,
  LONG_POLL_TIMEOUT_MS,
  buildItemMessage,
  buildTextMessage,
  buildToolCallResultItem,
  buildToolCallStartItem,
  getConfig,
  getUpdates,
  sendMessage,
  sendTyping
} from './api.js'
import {
  loadCursor,
  loadPauseUntil,
  saveCursor,
  savePauseUntil
} from './auth.js'
import {
  addOutboxRecord,
  clearOutbox,
  listOutboxRecords,
  removeOutboxRecord,
  type DurableOutboundRecord
} from './queue.js'
import {
  MessageItemType,
  MessageType,
  type Credentials,
  type IncomingMessage,
  type IncomingMessageType,
  type MessageItem,
  type SendMessageReq,
  type ToolCallStatus,
  type WeixinMessage
} from './types.js'

/** 单条文本分片上限。 */
const MAX_TEXT_CHUNK = 4_000

/** `-14`（token 失效）后的全局冷却时长，官方同为 1 小时。 */
const SESSION_PAUSE_MS = 60 * 60 * 1000

/** `-2`（频控）的退避时长；服务端窗口约 5 分钟。 */
const RATE_LIMIT_BACKOFF_MS = 30_000
const MAX_SEND_ATTEMPTS = 3

/** 出站消息最小间隔：分片发送时每片都计入服务端频率配额。 */
const MIN_SEND_INTERVAL_MS = 5_000

/** 每个 client 最多保留的用户上下文，避免恶意用户耗尽内存。 */
const MAX_CONTEXT_USERS = 256
const MAX_CONTEXT_TOKEN_LENGTH = 4_096
const MAX_USER_ID_LENGTH = 512
const MAX_MESSAGE_ID_LENGTH = 512

/** typing_ticket 有效期约 24 小时，留出余量后刷新。 */
const TYPING_TICKET_TTL_MS = 24 * 60 * 60 * 1000
const TYPING_TICKET_MIN_REFRESH_MS = 22 * 60 * 60 * 1000
const TYPING_TICKET_RETRY_MS = 2_000
const TYPING_TICKET_MAX_RETRY_MS = 60 * 60 * 1000

export class SessionExpiredError extends Error {
  constructor() {
    super('SESSION_EXPIRED')
    this.name = 'SessionExpiredError'
  }
}

/** 通道处于 `-14` 冷却期时抛出。 */
export class SessionPausedError extends Error {
  readonly remainingMs: number

  constructor(remainingMs: number) {
    super(`SESSION_PAUSED:${Math.ceil(remainingMs / 60_000)}min`)
    this.name = 'SessionPausedError'
    this.remainingMs = remainingMs
  }
}

export class CursorPersistenceError extends Error {
  constructor() {
    super('CURSOR_PERSISTENCE_FAILED')
    this.name = 'CursorPersistenceError'
  }
}

interface TypingTicketCache {
  ticket: string
  contextToken: string
  expiresAt: number
}

export class WeixinClient {
  private readonly token: string
  private readonly typingTickets = new Map<string, TypingTicketCache>()
  private readonly typingTicketFailures = new Map<string, { nextFetchAt: number; retryDelayMs: number }>()
  private readonly contextTokens = new Map<string, string>()
  private readonly outboundAbortController = new AbortController()
  private closed = false
  private baseUrl: string
  private cursor: string
  private persistedCursor: string
  private pendingCursor: string | null = null
  private pausedUntil = 0
  private lastSendAt = 0
  private nextPollTimeoutMs = LONG_POLL_TIMEOUT_MS
  /** All outbound messages share one FIFO queue and one rate limiter. */
  private sendQueue: Promise<void> = Promise.resolve()
  private pendingOutboundFlush: Promise<void> | null = null

  constructor(private readonly credentials: Credentials) {
    this.baseUrl = credentials.baseUrl
    this.token = credentials.token
    // 游标落盘后重启可续传，避免重复投递或漏消息。
    this.cursor = loadCursor(credentials.accountId)
    this.persistedCursor = this.cursor
    this.pausedUntil = loadPauseUntil(credentials.accountId)
  }

  close(): void {
    this.closed = true
    this.outboundAbortController.abort()
  }

  clearPendingOutbound(): void {
    clearOutbox(this.credentials.accountId)
  }

  async flushPendingOutbound(): Promise<void> {
    if (this.pendingOutboundFlush) return this.pendingOutboundFlush

    const flush = (async () => {
      for (const record of listOutboxRecords(this.credentials.accountId)) {
        await this.enqueueRecord(record)
      }
    })()
    this.pendingOutboundFlush = flush
    try {
      await flush
    } finally {
      if (this.pendingOutboundFlush === flush) this.pendingOutboundFlush = null
    }
  }

  get isClosed(): boolean {
    return this.closed
  }

  get accountId(): string {
    return this.credentials.accountId
  }

  get userId(): string {
    return this.credentials.userId
  }

  get isPaused(): boolean {
    if (this.pausedUntil > 0 && Date.now() >= this.pausedUntil) {
      this.pausedUntil = 0
    }
    return Date.now() < this.pausedUntil
  }

  get pauseRemainingMs(): number {
    return Math.max(0, this.pausedUntil - Date.now())
  }

  async getUpdates(signal?: AbortSignal): Promise<IncomingMessage[]> {
    this.assertActive()

    let response
    try {
      response = await getUpdates(this.baseUrl, this.token, this.cursor, this.nextPollTimeoutMs, signal)
    } catch (error) {
      if (isSessionExpired(error)) {
        this.pauseSession()
        throw new SessionExpiredError()
      }
      throw error
    }

    const serverTimeout = response.longpolling_timeout_ms
    if (typeof serverTimeout === 'number' && Number.isFinite(serverTimeout) && serverTimeout > 0) {
      this.nextPollTimeoutMs = Math.min(serverTimeout, 120_000)
    }

    const nextCursor = response.get_updates_buf
    if (typeof nextCursor === 'string' && nextCursor && nextCursor !== this.cursor) {
      // Only advance in memory here. The caller commits after every response
      // has been normalized and accepted into its queue.
      this.cursor = nextCursor
      this.pendingCursor = nextCursor
    }

    const incoming: IncomingMessage[] = []

    for (const raw of response.msgs ?? []) {
      const normalized = this.normalizeIncomingMessage(raw)
      if (!normalized) continue
      incoming.push(normalized)
    }

    return incoming
  }

  /** Persist the newest cursor after the caller has accepted the response. */
  commitCursor(): void {
    if (this.pendingCursor === null) return
    if (!saveCursor(this.credentials.accountId, this.pendingCursor)) {
      throw new CursorPersistenceError()
    }
    this.persistedCursor = this.pendingCursor
    this.pendingCursor = null
  }

  /** Rewind an uncommitted response before discarding its in-memory messages. */
  rollbackUncommittedCursor(): void {
    if (this.pendingCursor === null) return
    this.cursor = this.persistedCursor
    this.pendingCursor = null
  }

  async sendText(
    userId: string,
    text: string,
    contextToken?: string,
    onChunkSent?: (chunk: string) => void
  ): Promise<void> {
    this.assertActive()

    const resolvedContextToken = this.requireContextToken(userId, contextToken)
    const message = text.trim()
    if (!message) {
      throw new Error('Message text cannot be empty')
    }

    for (const chunk of chunkText(message, MAX_TEXT_CHUNK)) {
      await this.sendItem(buildTextMessage(userId, resolvedContextToken, chunk))
      onChunkSent?.(chunk)
    }
  }

  /**
   * 发送工具调用进度。只携带工具名与调用 ID，
   * **绝不携带 `event.input`**——入参可能包含路径、密钥、命令全文。
   */
  async sendToolCallStart(
    userId: string,
    toolName: string,
    toolCallId?: string,
    contextToken?: string
  ): Promise<void> {
    this.assertActive()
    const resolvedContextToken = this.requireContextToken(userId, contextToken)
    await this.sendItem(
      buildItemMessage(userId, resolvedContextToken, buildToolCallStartItem(toolName, toolCallId))
    )
  }

  /** 发送工具调用结束进度。只携带工具名与状态，**不携带结果内容**。 */
  async sendToolCallResult(
    userId: string,
    toolName: string,
    status: ToolCallStatus,
    toolCallId?: string,
    contextToken?: string
  ): Promise<void> {
    this.assertActive()
    const resolvedContextToken = this.requireContextToken(userId, contextToken)
    await this.sendItem(
      buildItemMessage(userId, resolvedContextToken, buildToolCallResultItem(toolName, status, toolCallId))
    )
  }

  private requireContextToken(userId: string, explicitContextToken?: string): string {
    const contextToken = explicitContextToken?.trim() || this.contextTokens.get(userId)
    if (!contextToken || contextToken.length > MAX_CONTEXT_TOKEN_LENGTH) {
      throw new Error(`Invalid context token for user ${userId}`)
    }

    // Touch the entry so the map behaves like a small LRU cache.
    this.contextTokens.delete(userId)
    this.contextTokens.set(userId, contextToken)
    return contextToken
  }

  /** 统一的出站发送通道：FIFO + 节流 + 频控退避重试。 */
  private sendItem(msg: SendMessageReq['msg']): Promise<void> {
    this.assertActive()
    const record = addOutboxRecord({
      accountId: this.credentials.accountId,
      userId: msg.to_user_id,
      contextToken: msg.context_token,
      msg,
      createdAt: Date.now()
    })
    return this.enqueueRecord(record)
  }

  private enqueueRecord(record: DurableOutboundRecord): Promise<void> {
    const operation = this.sendQueue.then(() => this.sendItemNow(record))
    // Keep the queue usable after a failed item while returning the failure to
    // the caller that owns this item.
    this.sendQueue = operation.catch(() => {})
    return operation
  }

  private async sendItemNow(record: DurableOutboundRecord): Promise<void> {
    const { msg } = record
    for (let attempt = 1; attempt <= MAX_SEND_ATTEMPTS; attempt += 1) {
      this.assertActive()
      await this.throttle()
      this.assertActive()

      try {
        await sendMessage(this.baseUrl, this.token, msg, this.outboundAbortController.signal)
        try {
          removeOutboxRecord(this.credentials.accountId, record.id)
        } catch {
          // The API has acknowledged the stable client_id. Keep the record for
          // crash recovery rather than sending the same logical message again.
        }
        this.lastSendAt = Date.now()
        return
      } catch (error) {
        this.lastSendAt = Date.now()
        if (isSessionExpired(error)) {
          this.pauseSession()
          throw new SessionExpiredError()
        }
        if (attempt >= MAX_SEND_ATTEMPTS || !isRetryableSendError(error)) {
          throw error
        }

        const wait = isRateLimited(error)
          ? RATE_LIMIT_BACKOFF_MS
          : Math.min(2_000 * 2 ** (attempt - 1), RATE_LIMIT_BACKOFF_MS)
        await delay(wait)
      }
    }
  }

  private async throttle(): Promise<void> {
    const wait = MIN_SEND_INTERVAL_MS - (Date.now() - this.lastSendAt)
    if (wait > 0) {
      await delay(wait)
    }
  }

  private enqueueRateLimited<T>(operation: () => Promise<T>): Promise<T> {
    const queued = this.sendQueue.then(async () => {
      this.assertActive()
      await this.throttle()
      this.assertActive()
      try {
        return await operation()
      } finally {
        this.lastSendAt = Date.now()
      }
    })
    this.sendQueue = queued.then(() => {}, () => {})
    return queued
  }

  async sendTyping(userId: string, contextToken?: string): Promise<void> {
    if (this.closed || this.isPaused) return
    const ticket = await this.getTypingTicket(userId, contextToken)
    if (!ticket) return

    await this.enqueueRateLimited(async () => {
      try {
        await sendTyping(this.baseUrl, this.token, userId, ticket, 1, this.outboundAbortController.signal)
      } catch (error) {
        if (isSessionExpired(error)) {
          this.pauseSession()
        }
        throw error
      }
    })
  }

  async stopTyping(userId: string, contextToken?: string): Promise<void> {
    if (this.closed || this.isPaused) return
    const ticket = await this.getTypingTicket(userId, contextToken)
    if (!ticket) return

    await this.enqueueRateLimited(async () => {
      try {
        await sendTyping(this.baseUrl, this.token, userId, ticket, 2, this.outboundAbortController.signal)
      } catch (error) {
        if (isSessionExpired(error)) {
          this.pauseSession()
        }
        throw error
      }
    })
  }

  rememberContext(message: WeixinMessage): void {
    const userId = message.message_type === MessageType.USER ? message.from_user_id : message.to_user_id
    const contextToken = typeof message.context_token === 'string' ? message.context_token.trim() : ''
    if (!userId || !contextToken
      || userId.length > MAX_USER_ID_LENGTH
      || contextToken.length > MAX_CONTEXT_TOKEN_LENGTH) return

    this.contextTokens.delete(userId)
    this.contextTokens.set(userId, contextToken)
    while (this.contextTokens.size > MAX_CONTEXT_USERS) {
      const oldest = this.contextTokens.keys().next().value
      if (oldest === undefined) break
      this.contextTokens.delete(oldest)
      this.typingTickets.delete(oldest)
      this.typingTicketFailures.delete(oldest)
    }
  }

  private normalizeIncomingMessage(message: WeixinMessage): IncomingMessage | null {
    if (!message || message.message_type !== MessageType.USER) {
      return null
    }
    if (typeof message.from_user_id !== 'string'
      || !message.from_user_id.trim()
      || message.from_user_id.length > MAX_USER_ID_LENGTH) {
      return null
    }
    if (typeof message.message_id !== 'string' && typeof message.message_id !== 'number') {
      return null
    }
    const messageId = String(message.message_id).trim()
    if (!messageId || messageId.length > MAX_MESSAGE_ID_LENGTH) {
      return null
    }
    if (typeof message.context_token !== 'string'
      || !message.context_token.trim()
      || message.context_token.length > MAX_CONTEXT_TOKEN_LENGTH) {
      return null
    }
    if (!Number.isFinite(message.create_time_ms) || !Array.isArray(message.item_list)) {
      return null
    }

    const type = detectType(message.item_list)
    return {
      messageId,
      userId: message.from_user_id.trim(),
      text: extractText(message.item_list, type),
      type,
      raw: message,
      contextToken: message.context_token.trim(),
      timestamp: new Date(message.create_time_ms)
    }
  }

  private assertActive(): void {
    if (this.closed) {
      throw new Error('WECHAT_CLIENT_CLOSED')
    }
    if (this.isPaused) {
      throw new SessionPausedError(this.pauseRemainingMs)
    }
  }

  private pauseSession(): void {
    this.pausedUntil = Date.now() + SESSION_PAUSE_MS
    savePauseUntil(this.credentials.accountId, this.pausedUntil)
  }

  private async getTypingTicket(userId: string, explicitContextToken?: string): Promise<string | null> {
    const contextToken = explicitContextToken?.trim() || this.contextTokens.get(userId)
    if (!contextToken || contextToken.length > MAX_CONTEXT_TOKEN_LENGTH) return null

    const cached = this.typingTickets.get(userId)
    if (cached && cached.contextToken === contextToken && Date.now() < cached.expiresAt) {
      this.typingTickets.delete(userId)
      this.typingTickets.set(userId, cached)
      return cached.ticket
    }

    const failure = this.typingTicketFailures.get(userId)
    if (failure && Date.now() < failure.nextFetchAt) {
      return cached?.contextToken === contextToken ? cached.ticket : null
    }

    try {
      const config = await getConfig(
        this.baseUrl,
        this.token,
        userId,
        contextToken,
        this.outboundAbortController.signal
      )
      const ticket = config.typing_ticket?.trim()

      if (!ticket) {
        this.scheduleTypingRetry(userId)
        return cached?.contextToken === contextToken ? cached.ticket : null
      }

      this.typingTickets.delete(userId)
      this.typingTickets.set(userId, {
        ticket,
        contextToken,
        expiresAt: Date.now() + TYPING_TICKET_MIN_REFRESH_MS
          + Math.random() * (TYPING_TICKET_TTL_MS - TYPING_TICKET_MIN_REFRESH_MS)
      })
      this.typingTicketFailures.delete(userId)
      return ticket
    } catch (error) {
      if (isSessionExpired(error)) {
        this.pauseSession()
      }
      // typing_ticket 只影响“正在输入”提示，失败不应影响消息收发。
      this.scheduleTypingRetry(userId)
      return cached?.contextToken === contextToken ? cached.ticket : null
    }
  }

  private scheduleTypingRetry(userId: string): void {
    const previous = this.typingTicketFailures.get(userId)?.retryDelayMs ?? TYPING_TICKET_RETRY_MS
    const nextDelay = Math.min(previous * 2, TYPING_TICKET_MAX_RETRY_MS)
    this.typingTicketFailures.delete(userId)
    this.typingTicketFailures.set(userId, {
      nextFetchAt: Date.now() + nextDelay,
      retryDelayMs: nextDelay
    })
  }
}

function detectType(items: MessageItem[]): IncomingMessageType {
  for (const item of items) {
    if (!item || typeof item !== 'object') continue
    switch (item.type) {
      case MessageItemType.TEXT:
        return 'text'
      case MessageItemType.IMAGE:
        return 'image'
      case MessageItemType.VOICE:
        return 'voice'
      case MessageItemType.FILE:
        return 'file'
      case MessageItemType.VIDEO:
        return 'video'
    }
  }

  return 'text'
}

function extractText(items: MessageItem[], type: IncomingMessageType): string {
  const text = items
    .filter((item) => item && item.type === MessageItemType.TEXT && item.text_item?.text)
    .map((item) => item.text_item?.text?.trim())
    .filter(Boolean)
    .join('\n')

  if (text) {
    return text
  }

  switch (type) {
    case 'image':
      return '[用户发送了一张图片，当前扩展尚未下载图片内容。]'
    case 'voice':
      return items.find((item) => item?.type === MessageItemType.VOICE)?.voice_item?.text?.trim()
        || '[用户发送了一条语音，当前扩展尚未转写完整音频。]'
    case 'file':
      return `[用户发送了文件：${items.find((item) => item?.type === MessageItemType.FILE)?.file_item?.file_name || '未命名文件'}]`
    case 'video':
      return '[用户发送了一段视频，当前扩展尚未提取视频内容。]'
    default:
      return '[收到一条空文本消息]'
  }
}

export function chunkText(text: string, maxLength: number): string[] {
  const chunks: string[] = []
  let remaining = text

  while (remaining.length > maxLength) {
    let splitAt = remaining.lastIndexOf('\n', maxLength)
    if (splitAt < maxLength / 2) {
      splitAt = remaining.lastIndexOf(' ', maxLength)
    }
    if (splitAt < maxLength / 2) {
      splitAt = maxLength
    }

    chunks.push(remaining.slice(0, splitAt))
    remaining = remaining.slice(splitAt)
  }

  if (remaining) {
    chunks.push(remaining)
  }

  return chunks
}

function isSessionExpired(error: unknown): boolean {
  return error instanceof ApiError
    && (error.code === ERR_SESSION_EXPIRED || error.status === 401)
}

function isRateLimited(error: unknown): boolean {
  return (error instanceof ApiError && error.code === ERR_RATE_LIMIT)
    || (error instanceof ApiError && error.status === 429)
}

function isRetryableSendError(error: unknown): boolean {
  if (isRateLimited(error)) return true
  if (error instanceof ApiError) {
    return error.status === 408 || error.status >= 500
  }
  return error instanceof TypeError
    || (error instanceof Error && (error.name === 'TimeoutError' || error.name === 'FetchError'))
}
