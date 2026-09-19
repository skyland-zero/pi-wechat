import { randomBytes, randomUUID } from 'node:crypto'
import {
  MessageItemType,
  MessageState,
  MessageType,
  type BaseInfo,
  type GetConfigResp,
  type GetUpdatesReq,
  type GetUpdatesResp,
  type MessageItem,
  type SendMessageReq,
  type SendTypingReq,
  type ToolCallStatus
} from './types.js'

export const DEFAULT_BASE_URL = 'https://ilinkai.weixin.qq.com'

/**
 * 通道版本与客户端标识。
 *
 * 这些字段随每个请求上报，但经验上服务端不对它们做门槛：
 * 官方插件曾长期在生产环境带着空的 `iLink-App-Id` / `0` 版本的 `iLink-App-ClientVersion` 正常运行
 * （见其 CHANGELOG 的修复记录），因此本扩展采用"诚实标识"策略——
 * 不冒充 OpenClaw，只声明自己是谁。
 *
 * `channel_version` / `iLink-App-ClientVersion` 保持为已验证可用的 1.0.0；
 * 若日后服务端开始按版本门控，两者需一起上调。
 */
export const CHANNEL_VERSION = '1.0.0'
/** 来自官方 package.json 的 `ilink_appid`，标识接入的应用类型为 bot。 */
const ILINK_APP_ID = 'bot'
/** 0x00MMNNPP 编码：major<<16 | minor<<8 | patch，与 channel_version 保持一致。 */
const ILINK_APP_CLIENT_VERSION = String((1 << 16) | (0 << 8) | 0)
/** UA 风格的自声明标识，格式为 `name/version`。不伪装成 OpenClaw。 */
const BOT_AGENT = 'PiWechat/0.1.0'

/** 服务端返回的协议错误码。 */
export const ERR_RATE_LIMIT = -2
export const ERR_SESSION_EXPIRED = -14

/** getUpdates 长轮询默认保持时长；服务端可通过 longpolling_timeout_ms 覆盖。 */
export const LONG_POLL_TIMEOUT_MS = 35_000
const DEFAULT_API_TIMEOUT_MS = 15_000
const CONFIG_TIMEOUT_MS = 10_000
const QR_REQUEST_TIMEOUT_MS = 35_000

export interface QrCodeResponse {
  qrcode: string
  qrcode_img_content: string
}

/** 扫码登录的全部协议状态。 */
export type QrStatus =
  | 'wait'
  | 'scaned'
  | 'need_verifycode'
  | 'verify_code_blocked'
  | 'binded_redirect'
  | 'scaned_but_redirect'
  | 'confirmed'
  | 'expired'

export interface QrStatusResponse {
  status: QrStatus
  bot_token?: string
  ilink_bot_id?: string
  ilink_user_id?: string
  /** 登录成功时下发的业务基座地址。 */
  baseurl?: string
  /** `scaned_but_redirect` 时下发的接入点主机名，需要切换轮询地址。 */
  redirect_host?: string
}

export class ApiError extends Error {
  readonly status: number
  readonly code?: number
  readonly payload?: unknown

  constructor(message: string, options: { status: number; code?: number; payload?: unknown }) {
    super(message)
    this.name = 'ApiError'
    this.status = options.status
    this.code = options.code
    this.payload = options.payload
  }
}

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, '')
}

function buildBaseInfo(): BaseInfo {
  return {
    channel_version: CHANNEL_VERSION,
    bot_agent: BOT_AGENT
  }
}

/** X-WECHAT-UIN：随机 uint32 -> 十进制字符串 -> base64。 */
export function randomWechatUin(): string {
  const value = randomBytes(4).readUInt32BE(0)
  return Buffer.from(String(value), 'utf8').toString('base64')
}

/**
 * 构造请求头。token 为空时不下发 Authorization（二维码接口不需要鉴权）。
 * 官方同样是"AuthorizationType 恒定、Authorization 按需"的组合。
 */
export function buildHeaders(token?: string): Record<string, string> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    AuthorizationType: 'ilink_bot_token',
    'X-WECHAT-UIN': randomWechatUin(),
    'iLink-App-Id': ILINK_APP_ID,
    'iLink-App-ClientVersion': ILINK_APP_CLIENT_VERSION
  }

  const trimmed = token?.trim()
  if (trimmed) {
    headers.Authorization = `Bearer ${trimmed}`
  }

  return headers
}

/** 只保留 token 前缀与长度，避免凭证进入日志。 */
export function redactToken(token?: string): string {
  if (!token) return '(none)'
  if (token.length <= 6) return `****(len=${token.length})`
  return `${token.slice(0, 6)}…(len=${token.length})`
}

/** 脱敏 JSON 请求/响应体中的敏感字段并截断。 */
export function redactBody(body: string, maxLen = 200): string {
  const redacted = body.replace(
    /"(context_token|bot_token|token|authorization)"\s*:\s*"[^"]*"/gi,
    '"$1":"<redacted>"'
  )
  return redacted.length <= maxLen ? redacted : `${redacted.slice(0, maxLen)}…(len=${redacted.length})`
}

/** 去掉 URL 的 query，避免签名/token 进入日志。 */
export function redactUrl(rawUrl: string): string {
  try {
    const url = new URL(rawUrl)
    return `${url.origin}${url.pathname}`
  } catch {
    return rawUrl.slice(0, 80)
  }
}

/** 是否为超时中断（区别于调用方主动 abort）。 */
export function isTimeoutError(error: unknown): boolean {
  return error instanceof Error && (error.name === 'TimeoutError' || error.name === 'AbortError')
}

async function parseJsonResponse<T>(response: Response, label: string): Promise<T> {
  const text = await response.text()
  const payload = text ? JSON.parse(text) as T : ({} as T)

  if (!response.ok) {
    const body = payload as { errmsg?: string; errcode?: number } | null
    throw new ApiError(body?.errmsg ?? `${label} failed with HTTP ${response.status}`, {
      status: response.status,
      code: body?.errcode,
      payload
    })
  }

  const body = payload as { ret?: number; errcode?: number; errmsg?: string } | null
  const ret = body?.ret
  const errcode = body?.errcode
  const failed =
    (typeof ret === 'number' && ret !== 0) ||
    (typeof errcode === 'number' && errcode !== 0)

  if (failed) {
    // 优先采用非零的 errcode：部分接口会返回 ret=0 但 errcode=-14。
    const code = typeof errcode === 'number' && errcode !== 0 ? errcode : ret
    throw new ApiError(body?.errmsg ?? `${label} failed`, {
      status: response.status,
      code,
      payload
    })
  }

  return payload
}

interface RequestOptions {
  baseUrl: string
  endpoint: string
  token?: string
  timeoutMs?: number
  signal?: AbortSignal
  label?: string
}

async function apiPost<T>(options: RequestOptions & { body: unknown }): Promise<T> {
  const { baseUrl, endpoint, body, token, timeoutMs, signal, label } = options
  const url = new URL(endpoint, `${normalizeBaseUrl(baseUrl)}/`)

  const timeoutSignal = timeoutMs ? AbortSignal.timeout(timeoutMs) : undefined
  const requestSignal = signal && timeoutSignal
    ? AbortSignal.any([signal, timeoutSignal])
    : signal ?? timeoutSignal

  const response = await fetch(url, {
    method: 'POST',
    headers: buildHeaders(token),
    body: JSON.stringify(body),
    ...(requestSignal ? { signal: requestSignal } : {})
  })

  return parseJsonResponse<T>(response, label ?? endpoint)
}

async function apiGet<T>(options: Omit<RequestOptions, 'body'>): Promise<T> {
  const { baseUrl, endpoint, token, timeoutMs, signal, label } = options
  const url = new URL(endpoint, `${normalizeBaseUrl(baseUrl)}/`)

  const timeoutSignal = timeoutMs ? AbortSignal.timeout(timeoutMs) : undefined
  const requestSignal = signal && timeoutSignal
    ? AbortSignal.any([signal, timeoutSignal])
    : signal ?? timeoutSignal

  const response = await fetch(url, {
    method: 'GET',
    headers: buildHeaders(token),
    ...(requestSignal ? { signal: requestSignal } : {})
  })

  return parseJsonResponse<T>(response, label ?? endpoint)
}

/**
 * 长轮询拉取消息。
 *
 * 客户端超时属于长轮询的正常控制流：此时返回空响应，调用方直接进入下一轮，
 * 不应把它当作错误处理（官方行为一致）。
 */
export async function getUpdates(
  baseUrl: string,
  token: string,
  cursor: string,
  timeoutMs = LONG_POLL_TIMEOUT_MS,
  signal?: AbortSignal
): Promise<GetUpdatesResp> {
  const body: GetUpdatesReq = {
    get_updates_buf: cursor,
    base_info: buildBaseInfo()
  }

  try {
    return await apiPost<GetUpdatesResp>({
      baseUrl,
      endpoint: '/ilink/bot/getupdates',
      body,
      token,
      timeoutMs,
      signal,
      label: 'getUpdates'
    })
  } catch (error) {
    // 调用方主动中断 -> 向上抛出，让轮询循环退出。
    if (signal?.aborted) {
      throw error
    }
    if (isTimeoutError(error)) {
      return { ret: 0, msgs: [], get_updates_buf: cursor }
    }
    throw error
  }
}

export async function sendMessage(
  baseUrl: string,
  token: string,
  msg: SendMessageReq['msg']
): Promise<Record<string, unknown>> {
  return apiPost<Record<string, unknown>>({
    baseUrl,
    endpoint: '/ilink/bot/sendmessage',
    body: { msg, base_info: buildBaseInfo() },
    token,
    timeoutMs: DEFAULT_API_TIMEOUT_MS,
    label: 'sendMessage'
  })
}

export async function getConfig(
  baseUrl: string,
  token: string,
  userId: string,
  contextToken: string
): Promise<GetConfigResp> {
  return apiPost<GetConfigResp>({
    baseUrl,
    endpoint: '/ilink/bot/getconfig',
    body: {
      ilink_user_id: userId,
      context_token: contextToken,
      base_info: buildBaseInfo()
    },
    token,
    timeoutMs: CONFIG_TIMEOUT_MS,
    label: 'getConfig'
  })
}

export async function sendTyping(
  baseUrl: string,
  token: string,
  userId: string,
  ticket: string,
  status: SendTypingReq['status']
): Promise<Record<string, unknown>> {
  const body: SendTypingReq = {
    ilink_user_id: userId,
    typing_ticket: ticket,
    status,
    base_info: buildBaseInfo()
  }

  return apiPost<Record<string, unknown>>({
    baseUrl,
    endpoint: '/ilink/bot/sendtyping',
    body,
    token,
    timeoutMs: CONFIG_TIMEOUT_MS,
    label: 'sendTyping'
  })
}

/**
 * 获取登录二维码。
 *
 * 官方 2.x 使用 POST，并在 body 中携带本地已保存的 bot token 列表：
 * 服务端据此识别"已绑定到本端"的账号并返回 `binded_redirect`，
 * 避免为同一个 bot 重复创建会话。
 */
export async function fetchQrCode(
  baseUrl: string = DEFAULT_BASE_URL,
  localTokenList: string[] = []
): Promise<QrCodeResponse> {
  return apiPost<QrCodeResponse>({
    baseUrl,
    endpoint: '/ilink/bot/get_bot_qrcode?bot_type=3',
    body: { local_token_list: localTokenList.slice(0, 10) },
    timeoutMs: QR_REQUEST_TIMEOUT_MS,
    label: 'fetchQrCode'
  })
}

/** 轮询扫码状态；`verifyCode` 用于 `need_verifycode` 场景下的数字配对码。 */
export async function getQrCodeStatus(
  qrcode: string,
  baseUrl: string = DEFAULT_BASE_URL,
  verifyCode?: string
): Promise<QrStatusResponse> {
  const params = new URLSearchParams({ qrcode })
  if (verifyCode) {
    params.set('verify_code', verifyCode)
  }

  return apiGet<QrStatusResponse>({
    baseUrl,
    endpoint: `/ilink/bot/get_qrcode_status?${params.toString()}`,
    timeoutMs: LONG_POLL_TIMEOUT_MS,
    label: 'getQrCodeStatus'
  })
}

export function buildTextMessage(
  userId: string,
  contextToken: string,
  text: string
): SendMessageReq['msg'] {
  return buildItemMessage(userId, contextToken, {
    type: MessageItemType.TEXT,
    text_item: { text }
  })
}

/**
 * 构造只携带单个 item 的消息。
 *
 * 官方 openclaw-weixin 对每个 item 都单独发一条请求（`item_list` 长度为 1），
 * 本扩展保持同样约束。
 */
export function buildItemMessage(
  userId: string,
  contextToken: string,
  item: MessageItem
): SendMessageReq['msg'] {
  return {
    from_user_id: '',
    to_user_id: userId,
    client_id: randomUUID(),
    message_type: MessageType.BOT,
    message_state: MessageState.FINISH,
    context_token: contextToken,
    item_list: [item]
  }
}

/** 工具调用开始。只携带工具名与调用 ID，**不携带入参**。 */
export function buildToolCallStartItem(toolName: string, toolCallId?: string): MessageItem {
  return {
    type: MessageItemType.TOOL_CALL_START,
    create_time_ms: Date.now(),
    is_completed: false,
    tool_call_start_item: {
      tool_name: toolName,
      ...(toolCallId ? { tool_call_id: toolCallId } : {})
    }
  }
}

/** 工具调用结束。只携带工具名、调用 ID 与状态，**不携带结果内容**。 */
export function buildToolCallResultItem(
  toolName: string,
  status: ToolCallStatus,
  toolCallId?: string
): MessageItem {
  return {
    type: MessageItemType.TOOL_CALL_RESULT,
    create_time_ms: Date.now(),
    is_completed: true,
    tool_call_result_item: {
      tool_name: toolName,
      ...(toolCallId ? { tool_call_id: toolCallId } : {}),
      status
    }
  }
}
