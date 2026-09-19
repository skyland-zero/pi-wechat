import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import {
  DEFAULT_BASE_URL,
  fetchQrCode,
  getQrCodeStatus,
  isTimeoutError,
  type QrStatusResponse
} from './api.js'
import type { Credentials } from './types.js'

const CREDS_DIR = path.join(os.homedir(), '.pi-wechat')
const CREDS_FILE = path.join(CREDS_DIR, 'credentials.json')
const CREDS_BACKUP_FILE = path.join(CREDS_DIR, 'credentials.json.bak')
const TOKENS_FILE = path.join(CREDS_DIR, 'tokens.json')
const SYNC_FILE = path.join(CREDS_DIR, 'sync.json')

/** 上报给服务端的本地 token 数量上限。 */
const TOKEN_HISTORY_LIMIT = 10

export function getCredentialsPath(): string {
  return CREDS_FILE
}

export function getCredentialsBackupPath(): string {
  return CREDS_BACKUP_FILE
}

export function loadCredentials(): Credentials | null {
  try {
    const parsed = JSON.parse(fs.readFileSync(CREDS_FILE, 'utf-8')) as Credentials
    return parsed?.token ? parsed : null
  } catch {
    return null
  }
}

/**
 * 在覆盖凭证前留一份备份。
 *
 * 微信侧目前没有解绑入口：一旦本地 token 丢失而服务端仍认为该 bot 已绑定，
 * 重新扫码可能永久失败。备份是这种情况下唯一的自救手段。
 */
function backupCredentials(): void {
  try {
    if (fs.existsSync(CREDS_FILE)) {
      fs.copyFileSync(CREDS_FILE, CREDS_BACKUP_FILE)
    }
  } catch {
    // 备份失败不应阻断登录流程。
  }
}

export function saveCredentials(creds: Credentials): void {
  fs.mkdirSync(CREDS_DIR, { recursive: true })
  backupCredentials()
  fs.writeFileSync(
    CREDS_FILE,
    JSON.stringify(
      {
        ...creds,
        savedAt: new Date().toISOString()
      },
      null,
      2
    ),
    { mode: 0o600 }
  )
  rememberToken(creds.token)
}

export function clearCredentials(): void {
  try {
    fs.unlinkSync(CREDS_FILE)
  } catch {
    // Ignore missing credentials.
  }
  clearCursor()
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
    fs.mkdirSync(CREDS_DIR, { recursive: true })
    fs.writeFileSync(TOKENS_FILE, JSON.stringify({ tokens: next }, null, 2), { mode: 0o600 })
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
      accountId?: string
      cursor?: string
    }
    if (data.accountId !== accountId) return ''
    return typeof data.cursor === 'string' ? data.cursor : ''
  } catch {
    return ''
  }
}

export function saveCursor(accountId: string, cursor: string): void {
  try {
    fs.mkdirSync(CREDS_DIR, { recursive: true })
    fs.writeFileSync(
      SYNC_FILE,
      JSON.stringify({ accountId, cursor, updatedAt: new Date().toISOString() }),
      { mode: 0o600 }
    )
  } catch {
    // 游标落盘失败只影响重启后的续传，不应中断轮询。
  }
}

function clearCursor(): void {
  try {
    fs.unlinkSync(SYNC_FILE)
  } catch {
    // Ignore missing cursor state.
  }
}

export async function getQrCode(
  baseUrl: string = DEFAULT_BASE_URL,
  localTokenList: string[] = []
): Promise<{ url: string; token: string }> {
  const response = await fetchQrCode(baseUrl, localTokenList)
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
    return await getQrCodeStatus(qrcode, baseUrl, verifyCode)
  } catch (error) {
    if (isTimeoutError(error)) {
      return { status: 'wait' }
    }
    throw error
  }
}
