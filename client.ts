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
import { loadCursor, saveCursor } from './auth.js'
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

/**
 * 单条文本分片上限。
 *
 * 取官方 openclaw-weixin 声明的 `outbound.textChunkLimit`（4000）。
 * 协议下发字段中没有最大长度，这个值属于客户端兼容策略。
 */
const MAX_TEXT_CHUNK = 4_000

/** `-14`（token 失效）后的全局冷却时长，官方同为 1 小时。 */
const SESSION_PAUSE_MS = 60 * 60 * 1000

/** `-2`（频控）的退避时长；服务端窗口约 5 分钟。 */
const RATE_LIMIT_BACKOFF_MS = 30_000

/** 出站消息最小间隔：分片发送时每片都计入服务端频率配额。 */
const MIN_SEND_INTERVAL_MS = 5_000

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

export class WeixinClient {
  private readonly token: string
  private readonly typingTickets = new Map<string, { ticket: string; expiresAt: number }>()
  private readonly typingTicketFailures = new Map<string, { nextFetchAt: number; retryDelayMs: number }>()
  private readonly contextTokens = new Map<string, string>()
  private baseUrl: string
  private cursor: string
  private pausedUntil = 0
  private lastSendAt = 0
  private nextPollTimeoutMs = LONG_POLL_TIMEOUT_MS

  constructor(private readonly credentials: Credentials) {
    this.baseUrl = credentials.baseUrl
    this.token = credentials.token
    // 游标落盘后重启可续传，避免重复投递或漏消息。
    this.cursor = loadCursor(credentials.accountId)
  }

  get accountId(): string {
    return this.credentials.accountId
  }

  get userId(): string {
    return this.credentials.userId
  }

  get isPaused(): boolean {
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
        // 冷却而非立刻重登，避免反复触发风控。
        this.pausedUntil = Date.now() + SESSION_PAUSE_MS
        throw new SessionExpiredError()
      }
      throw error
    }

    const serverTimeout = response.longpolling_timeout_ms
    if (typeof serverTimeout === 'number' && serverTimeout > 0) {
      this.nextPollTimeoutMs = serverTimeout
    }

    const nextCursor = response.get_updates_buf
    if (nextCursor) {
      this.cursor = nextCursor
      saveCursor(this.credentials.accountId, nextCursor)
    }

    const incoming: IncomingMessage[] = []

    for (const raw of response.msgs ?? []) {
      this.rememberContext(raw)
      const normalized = this.normalizeIncomingMessage(raw)
      if (normalized) {
        incoming.push(normalized)
      }
    }

    return incoming
  }

  async sendText(userId: string, text: string): Promise<void> {
    this.assertActive()

    const contextToken = this.requireContextToken(userId)
    const message = text.trim()
    if (!message) {
      throw new Error('Message text cannot be empty')
    }

    for (const chunk of chunkText(message, MAX_TEXT_CHUNK)) {
      await this.sendItem(buildTextMessage(userId, contextToken, chunk))
    }
  }

  /**
   * 发送工具调用进度。只携带工具名与调用 ID，
   * **绝不携带 `event.input`**——入参可能包含路径、密钥、命令全文。
   */
  async sendToolCallStart(userId: string, toolName: string, toolCallId?: string): Promise<void> {
    this.assertActive()
    const contextToken = this.requireContextToken(userId)
    await this.sendItem(
      buildItemMessage(userId, contextToken, buildToolCallStartItem(toolName, toolCallId))
    )
  }

  /** 发送工具调用结束进度。只携带工具名与状态，**不携带结果内容**。 */
  async sendToolCallResult(
    userId: string,
    toolName: string,
    status: ToolCallStatus,
    toolCallId?: string
  ): Promise<void> {
    this.assertActive()
    const contextToken = this.requireContextToken(userId)
    await this.sendItem(
      buildItemMessage(userId, contextToken, buildToolCallResultItem(toolName, status, toolCallId))
    )
  }

  private requireContextToken(userId: string): string {
    const contextToken = this.contextTokens.get(userId)
    if (!contextToken) {
      throw new Error(`No cached context token for user ${userId}`)
    }
    return contextToken
  }

  /** 统一的出站发送通道：节流 + 频控退避重试。 */
  private async sendItem(msg: SendMessageReq['msg']): Promise<void> {
    await this.throttle()

    try {
      await sendMessage(this.baseUrl, this.token, msg)
    } catch (error) {
      if (!isRateLimited(error)) throw error
      // 命中频控：退避后重试一次，仍失败则交给上层提示用户。
      await delay(RATE_LIMIT_BACKOFF_MS)
      await sendMessage(this.baseUrl, this.token, msg)
    } finally {
      this.lastSendAt = Date.now()
    }
  }

  private async throttle(): Promise<void> {
    const wait = MIN_SEND_INTERVAL_MS - (Date.now() - this.lastSendAt)
    if (wait > 0) {
      await delay(wait)
    }
  }

  async sendTyping(userId: string): Promise<void> {
    if (this.isPaused) return
    const ticket = await this.getTypingTicket(userId)
    if (!ticket) return
    await sendTyping(this.baseUrl, this.token, userId, ticket, 1)
  }

  async stopTyping(userId: string): Promise<void> {
    if (this.isPaused) return
    const ticket = await this.getTypingTicket(userId)
    if (!ticket) return
    await sendTyping(this.baseUrl, this.token, userId, ticket, 2)
  }

  rememberContext(message: WeixinMessage): void {
    const userId = message.message_type === MessageType.USER ? message.from_user_id : message.to_user_id
    if (userId && message.context_token) {
      this.contextTokens.set(userId, message.context_token)
    }
  }

  private normalizeIncomingMessage(message: WeixinMessage): IncomingMessage | null {
    if (message.message_type !== MessageType.USER) {
      return null
    }

    const type = detectType(message.item_list)
    return {
      messageId: String(message.message_id),
      userId: message.from_user_id,
      text: extractText(message.item_list, type),
      type,
      raw: message,
      contextToken: message.context_token,
      timestamp: new Date(message.create_time_ms)
    }
  }

  private assertActive(): void {
    if (this.isPaused) {
      throw new SessionPausedError(this.pauseRemainingMs)
    }
  }

  private async getTypingTicket(userId: string): Promise<string | null> {
    const cached = this.typingTickets.get(userId)
    if (cached && Date.now() < cached.expiresAt) {
      return cached.ticket
    }

    const failure = this.typingTicketFailures.get(userId)
    if (failure && Date.now() < failure.nextFetchAt) {
      return cached?.ticket ?? null
    }

    const contextToken = this.contextTokens.get(userId)
    if (!contextToken) {
      return null
    }

    try {
      const config = await getConfig(this.baseUrl, this.token, userId, contextToken)
      const ticket = config.typing_ticket?.trim()

      if (!ticket) {
        this.scheduleTypingRetry(userId)
        return cached?.ticket ?? null
      }

      this.typingTickets.set(userId, {
        ticket,
        expiresAt: Date.now() + TYPING_TICKET_MIN_REFRESH_MS
          + Math.random() * (TYPING_TICKET_TTL_MS - TYPING_TICKET_MIN_REFRESH_MS)
      })
      this.typingTicketFailures.delete(userId)
      return ticket
    } catch {
      // typing_ticket 只影响"正在输入"提示，失败不应影响消息收发。
      this.scheduleTypingRetry(userId)
      return cached?.ticket ?? null
    }
  }

  private scheduleTypingRetry(userId: string): void {
    const previous = this.typingTicketFailures.get(userId)?.retryDelayMs ?? TYPING_TICKET_RETRY_MS
    const nextDelay = Math.min(previous * 2, TYPING_TICKET_MAX_RETRY_MS)
    this.typingTicketFailures.set(userId, {
      nextFetchAt: Date.now() + nextDelay,
      retryDelayMs: nextDelay
    })
  }
}

function detectType(items: MessageItem[]): IncomingMessageType {
  for (const item of items) {
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
    .filter((item) => item.type === MessageItemType.TEXT && item.text_item?.text)
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
      return items.find((item) => item.type === MessageItemType.VOICE)?.voice_item?.text?.trim()
        || '[用户发送了一条语音，当前扩展尚未转写完整音频。]'
    case 'file':
      return `[用户发送了文件：${items.find((item) => item.type === MessageItemType.FILE)?.file_item?.file_name || '未命名文件'}]`
    case 'video':
      return '[用户发送了一段视频，当前扩展尚未提取视频内容。]'
    default:
      return '[收到一条空文本消息]'
  }
}

function chunkText(text: string, maxLength: number): string[] {
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

    chunks.push(remaining.slice(0, splitAt).trim())
    remaining = remaining.slice(splitAt).trim()
  }

  if (remaining) {
    chunks.push(remaining)
  }

  return chunks
}

function isSessionExpired(error: unknown): boolean {
  return error instanceof ApiError && error.code === ERR_SESSION_EXPIRED
}

function isRateLimited(error: unknown): boolean {
  return error instanceof ApiError && error.code === ERR_RATE_LIMIT
}
