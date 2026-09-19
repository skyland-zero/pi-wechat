import { randomUUID } from 'node:crypto'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import {
  DEFAULT_BASE_URL,
  fetchQrCode,
  getQrCodeStatus,
  isTimeoutError,
  validateBaseUrl,
  type QrStatusResponse
} from './api.js'
import type { Credentials } from './types.js'

const CREDS_DIR = path.join(os.homedir(), '.pi-wechat')
const CREDS_FILE = path.join(CREDS_DIR, 'credentials.json')
const CREDS_BACKUP_FILE = path.join(CREDS_DIR, 'credentials.json.bak')
const TOKENS_FILE = path.join(CREDS_DIR, 'tokens.json')
const SYNC_FILE = path.join(CREDS_DIR, 'sync.json')
const PAUSE_FILE = path.join(CREDS_DIR, 'pause.json')

/** 上报给服务端的本地 token 数量上限。 */
const TOKEN_HISTORY_LIMIT = 10
const MAX_CREDENTIAL_FIELD_LENGTH = 4096

export function getCredentialsPath(): string {
  return CREDS_FILE
}

export function getCredentialsBackupPath(): string {
  return CREDS_BACKUP_FILE
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

/** 校验磁盘上的凭证，避免把任意 JSON 当成可用 client。 */
export function validateCredentials(value: unknown): Credentials | null {
  if (!isRecord(value)) return null

  const token = typeof value.token === 'string' ? value.token.trim() : ''
  const accountId = typeof value.accountId === 'string' ? value.accountId.trim() : ''
  const userId = typeof value.userId === 'string' ? value.userId.trim() : ''
  if (!token || !accountId || !userId) return null
  if ([token, accountId, userId].some((value) =>
    value.length > MAX_CREDENTIAL_FIELD_LENGTH || /[\u0000-\u001f\u007f]/.test(value)
  )) {
    return null
  }

  let baseUrl: string
  try {
    baseUrl = validateBaseUrl(value.baseUrl)
  } catch {
    return null
  }

  return {
    token,
    baseUrl,
    accountId,
    userId,
    ...(typeof value.savedAt === 'string' ? { savedAt: value.savedAt } : {})
  }
}

export function loadCredentials(): Credentials | null {
  try {
    const parsed = JSON.parse(fs.readFileSync(CREDS_FILE, 'utf-8')) as unknown
    return validateCredentials(parsed)
  } catch {
    return null
  }
}

function ensureCredentialsDir(): void {
  fs.mkdirSync(CREDS_DIR, { recursive: true, mode: 0o700 })
  // `mode` only applies when creating a directory. Repair permissions from
  // older versions as well, where the directory may have been more open.
  try {
    fs.chmodSync(CREDS_DIR, 0o700)
  } catch {
    // Best effort on platforms without POSIX permissions.
  }
}

function atomicWrite(filePath: string, content: string): void {
  ensureCredentialsDir()
  const tempPath = `${filePath}.${process.pid}.${randomUUID()}.tmp`

  try {
    fs.writeFileSync(tempPath, content, { encoding: 'utf-8', mode: 0o600, flag: 'wx' })
    try {
      fs.chmodSync(tempPath, 0o600)
    } catch {
      // Best effort on platforms without POSIX permissions.
    }
    fs.renameSync(tempPath, filePath)
    try {
      fs.chmodSync(filePath, 0o600)
    } catch {
      // Best effort on platforms without POSIX permissions.
    }
  } finally {
    try {
      fs.unlinkSync(tempPath)
    } catch {
      // Ignore an already-renamed temporary file.
    }
  }
}

/**
 * 在覆盖凭证前留一份备份。
 *
 * 微信侧目前没有解绑入口：一旦本地 token 丢失而服务端仍认为该 bot 已绑定，
 * 重新扫码可能永久失败。备份是这种情况下唯一的自救手段。
 */
function backupCredentials(): void {
  if (!fs.existsSync(CREDS_FILE)) return

  const tempPath = `${CREDS_BACKUP_FILE}.${process.pid}.${randomUUID()}.tmp`
  try {
    ensureCredentialsDir()
    fs.copyFileSync(CREDS_FILE, tempPath)
    try {
      fs.chmodSync(tempPath, 0o600)
    } catch {
      // Best effort on platforms without POSIX permissions.
    }
    fs.renameSync(tempPath, CREDS_BACKUP_FILE)
    try {
      fs.chmodSync(CREDS_BACKUP_FILE, 0o600)
    } catch {
      // Best effort on platforms without POSIX permissions.
    }
  } catch {
    // 备份失败不应阻断登录流程。
  } finally {
    try {
      fs.unlinkSync(tempPath)
    } catch {
      // Ignore an already-renamed temporary file.
    }
  }
}

export function saveCredentials(creds: Credentials): void {
  const normalized = validateCredentials(creds)
  if (!normalized) {
    throw new Error('Invalid WeChat credentials')
  }

  ensureCredentialsDir()
  backupCredentials()
  atomicWrite(
    CREDS_FILE,
    JSON.stringify(
      {
        ...normalized,
        savedAt: new Date().toISOString()
      },
      null,
      2
    )
  )
  rememberToken(normalized.token)
}

export function clearCredentials(): void {
  const current = loadCredentials()
  try {
    fs.unlinkSync(CREDS_FILE)
  } catch {
    // Ignore missing credentials.
  }
  clearCursor()
  if (current) clearPauseUntil(current.accountId)
}

function loadTokenHistory(): string[] {
  try {
    const data = JSON.parse(fs.readFileSync(TOKENS_FILE, 'utf-8')) as { tokens?: unknown }
    if (!Array.isArray(data.tokens)) return []
    return data.tokens.filter((value): value is string => typeof value === 'string' && !!value.trim())
  } catch {
    return []
  }
}

function rememberToken(token: string): void {
  const trimmed = token.trim()
  if (!trimmed) return

  const next = [trimmed, ...loadTokenHistory().filter((value) => value !== trimmed)]
    .slice(0, TOKEN_HISTORY_LIMIT)

  try {
    atomicWrite(TOKENS_FILE, JSON.stringify({ tokens: next }, null, 2))
  } catch {
    // 记录失败不影响登录结果。
  }
}

/**
 * 本地已保存的 bot token 列表，用于二维码请求的 `local_token_list`。
 *
 * 有意保留历史 token：服务端需要它才能判断"该 bot 已绑定到本端"，
 * 进而返回 `binded_redirect`，否则登录会卡在服务端拒绝新二维码的状态。
 */
export function collectLocalTokenList(): string[] {
  const current = loadCredentials()?.token?.trim()
  const all = current ? [current, ...loadTokenHistory()] : loadTokenHistory()
  return [...new Set(all)].filter(Boolean).slice(0, TOKEN_HISTORY_LIMIT)
}

/** 读取 getUpdates 游标；账号变更时旧游标失效。 */
export function loadCursor(accountId: string): string {
  try {
    const data = JSON.parse(fs.readFileSync(SYNC_FILE, 'utf-8')) as {
      accountId?: unknown
      cursor?: unknown
    }
    if (data.accountId !== accountId) return ''
    return typeof data.cursor === 'string' ? data.cursor : ''
  } catch {
    return ''
  }
}

export function saveCursor(accountId: string, cursor: string): boolean {
  if (!accountId || typeof cursor !== 'string') return false

  try {
    atomicWrite(
      SYNC_FILE,
      JSON.stringify({ accountId, cursor, updatedAt: new Date().toISOString() })
    )
    return true
  } catch {
    // 调用方必须把 false 当作持久化失败并暂停轮询，不能确认该批消息。
    return false
  }
}

function clearCursor(): void {
  try {
    fs.unlinkSync(SYNC_FILE)
  } catch {
    // Ignore missing cursor state.
  }
}

interface PauseState {
  accountId: string
  pausedUntil: number
}

/** 持久化 session 冷却，避免重新加载 client 绕过 -14 冷却。 */
export function loadPauseUntil(accountId: string): number {
  try {
    const data = JSON.parse(fs.readFileSync(PAUSE_FILE, 'utf-8')) as Partial<PauseState>
    if (data.accountId !== accountId || typeof data.pausedUntil !== 'number') return 0
    return Number.isFinite(data.pausedUntil) && data.pausedUntil > Date.now() ? data.pausedUntil : 0
  } catch {
    return 0
  }
}

export function savePauseUntil(accountId: string, pausedUntil: number): boolean {
  if (!accountId || !Number.isFinite(pausedUntil)) return false

  try {
    atomicWrite(PAUSE_FILE, JSON.stringify({ accountId, pausedUntil }))
    return true
  } catch {
    // 冷却落盘失败不应阻断当前进程的冷却。
    return false
  }
}

export function clearPauseUntil(accountId: string): void {
  try {
    const data = JSON.parse(fs.readFileSync(PAUSE_FILE, 'utf-8')) as Partial<PauseState>
    if (data.accountId === accountId) {
      fs.unlinkSync(PAUSE_FILE)
    }
  } catch {
    // Ignore missing or malformed pause state.
  }
}

export async function getQrCode(
  baseUrl: string = DEFAULT_BASE_URL,
  localTokenList: string[] = []
): Promise<{ url: string; token: string }> {
  const response = await fetchQrCode(baseUrl, localTokenList)
  if (typeof response.qrcode !== 'string' || !response.qrcode.trim()) {
    throw new Error('QR response did not include qrcode')
  }
  if (typeof response.qrcode_img_content !== 'string' || !response.qrcode_img_content.trim()) {
    throw new Error('QR response did not include qrcode_img_content')
  }

  return {
    url: response.qrcode_img_content,
    token: response.qrcode
  }
}

/**
 * 轮询扫码状态。
 *
 * 服务端会 hold 住请求（长轮询），客户端超时属于正常控制流，
 * 视为 `wait` 继续下一轮，而不是抛出错误。
 */
export async function pollQrStatus(
  qrcode: string,
  baseUrl: string = DEFAULT_BASE_URL,
  verifyCode?: string
): Promise<QrStatusResponse> {
  try {
    const result = await getQrCodeStatus(qrcode, baseUrl, verifyCode)
    const knownStatuses = new Set([
      'wait',
      'scaned',
      'need_verifycode',
      'verify_code_blocked',
      'binded_redirect',
      'scaned_but_redirect',
      'confirmed',
      'expired'
    ])
    if (!knownStatuses.has(result.status)) {
      throw new Error(`Unknown QR status: ${String(result.status)}`)
    }
    return result
  } catch (error) {
    if (isTimeoutError(error)) {
      return { status: 'wait' }
    }
    throw error
  }
}
