import { randomUUID } from 'node:crypto'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import type { SendMessageReq } from './types.js'

const STATE_DIR = path.join(os.homedir(), '.pi-wechat')
const INBOX_FILE = path.join(STATE_DIR, 'inbox.json')
const OUTBOX_FILE = path.join(STATE_DIR, 'outbox.json')
const MAX_INBOX_RECORDS = 2_000
const MAX_INBOX_BYTES = 10 * 1024 * 1024
const MAX_INBOX_TEXT_LENGTH = 50_000
const MAX_CONTEXT_TOKEN_LENGTH = 4_096
const MAX_ID_LENGTH = 512
const MAX_OUTBOX_RECORDS = 1_000
const MAX_OUTBOX_BYTES = 10 * 1024 * 1024

export interface DurableOutboundRecord {
  id: string
  accountId: string
  userId: string
  contextToken: string
  msg: SendMessageReq['msg']
  createdAt: number
}

export interface DurableInboxRecord {
  id: string
  accountId: string
  userId: string
  messageId: string
  contextToken: string
  text: string
  receivedAt: number
  attempts: number
  status: 'pending' | 'active'
}

export class InboxPersistenceError extends Error {
  constructor(message = 'INBOX_PERSISTENCE_FAILED') {
    super(message)
    this.name = 'InboxPersistenceError'
  }
}

export class OutboxPersistenceError extends Error {
  constructor(message = 'OUTBOX_PERSISTENCE_FAILED') {
    super(message)
    this.name = 'OutboxPersistenceError'
  }
}

function ensureStateDir(): void {
  fs.mkdirSync(STATE_DIR, { recursive: true, mode: 0o700 })
  try {
    fs.chmodSync(STATE_DIR, 0o700)
  } catch {
    // Best effort on platforms without POSIX permissions.
  }
}

function writeAtomic(content: string, filePath: string): void {
  const tempPath = `${filePath}.${process.pid}.${randomUUID()}.tmp`
  try {
    ensureStateDir()
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
  } catch (error) {
    throw new InboxPersistenceError(error instanceof Error ? error.message : String(error))
  } finally {
    try {
      fs.unlinkSync(tempPath)
    } catch {
      // Ignore an already-renamed temporary file.
    }
  }
}

function readRecords(): DurableInboxRecord[] {
  try {
    const parsed = JSON.parse(fs.readFileSync(INBOX_FILE, 'utf-8')) as { records?: unknown }
    if (!Array.isArray(parsed.records) || !parsed.records.every(isInboxRecord)) {
      throw new InboxPersistenceError('INVALID_INBOX_SCHEMA')
    }

    return parsed.records.slice(-MAX_INBOX_RECORDS).map((record) => ({
      ...record,
      attempts: record.attempts ?? 0
    }))
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return []
    throw new InboxPersistenceError(error instanceof Error ? error.message : String(error))
  }
}

function isInboxRecord(value: unknown): value is DurableInboxRecord {
  if (typeof value !== 'object' || value === null) return false
  const record = value as Partial<DurableInboxRecord>
  return typeof record.id === 'string'
    && record.id.length <= MAX_ID_LENGTH
    && typeof record.accountId === 'string'
    && record.accountId.length <= MAX_ID_LENGTH
    && typeof record.userId === 'string'
    && record.userId.length <= MAX_ID_LENGTH
    && typeof record.messageId === 'string'
    && record.messageId.length <= MAX_ID_LENGTH
    && typeof record.contextToken === 'string'
    && record.contextToken.length <= MAX_CONTEXT_TOKEN_LENGTH
    && typeof record.text === 'string'
    && record.text.length <= MAX_INBOX_TEXT_LENGTH
    && Number.isFinite(record.receivedAt)
    && (record.attempts === undefined
      || (Number.isInteger(record.attempts) && record.attempts >= 0 && record.attempts <= 10))
    && (record.status === 'pending' || record.status === 'active')
}

function saveRecords(records: DurableInboxRecord[]): void {
  const content = JSON.stringify({ version: 1, records }, null, 2)
  if (Buffer.byteLength(content, 'utf8') > MAX_INBOX_BYTES) {
    throw new InboxPersistenceError('INBOX_TOO_LARGE')
  }
  writeAtomic(content, INBOX_FILE)
}

/**
 * Add an inbound message before the API cursor is committed.
 * Returns false for a durable duplicate, true for a newly persisted record.
 */
export function addInboxRecord(record: Omit<DurableInboxRecord, 'id' | 'status'> & Partial<Pick<DurableInboxRecord, 'id'>>): boolean {
  const records = readRecords()
  const duplicate = records.some((item) =>
    item.accountId === record.accountId
    && item.userId === record.userId
    && item.messageId === record.messageId
  )
  if (duplicate) return false
  if (records.length >= MAX_INBOX_RECORDS) {
    throw new InboxPersistenceError('INBOX_FULL')
  }

  records.push({
    ...record,
    id: record.id ?? randomUUID(),
    attempts: record.attempts ?? 0,
    status: 'pending'
  })
  saveRecords(records)
  return true
}

export function loadPendingInbox(accountId: string): DurableInboxRecord[] {
  const records = readRecords()
  let changed = false
  const pending = records
    .filter((record) => record.accountId === accountId)
    .map((record) => {
      if (record.status !== 'active') return record
      changed = true
      return { ...record, status: 'pending' as const }
    })

  if (changed) {
    saveRecords(records.map((record) => {
      if (record.accountId !== accountId || record.status !== 'active') return record
      return { ...record, status: 'pending' as const }
    }))
  }

  return pending
}

export function markInboxActive(accountId: string, id: string): void {
  updateRecord(accountId, id, (record) => ({ ...record, status: 'active' }))
}

export function markInboxPending(accountId: string, id: string, attempts: number): void {
  updateRecord(accountId, id, (record) => ({
    ...record,
    attempts: Math.max(0, Math.min(10, Math.trunc(attempts))),
    status: 'pending'
  }))
}

export function removeInboxRecord(accountId: string, id: string): void {
  const records = readRecords()
  const next = records.filter((record) => !(record.accountId === accountId && record.id === id))
  if (next.length !== records.length) saveRecords(next)
}

export function clearInbox(accountId: string): void {
  const records = readRecords()
  const next = records.filter((record) => record.accountId !== accountId)
  if (next.length !== records.length) saveRecords(next)
}

function updateRecord(
  accountId: string,
  id: string,
  update: (record: DurableInboxRecord) => DurableInboxRecord
): void {
  const records = readRecords()
  let changed = false
  const next = records.map((record) => {
    if (record.accountId !== accountId || record.id !== id) return record
    changed = true
    return update(record)
  })
  if (changed) saveRecords(next)
}

function readOutboxRecords(): DurableOutboundRecord[] {
  try {
    const parsed = JSON.parse(fs.readFileSync(OUTBOX_FILE, 'utf-8')) as { records?: unknown }
    if (!Array.isArray(parsed.records) || !parsed.records.every(isOutboxRecord)) {
      throw new OutboxPersistenceError('INVALID_OUTBOX_SCHEMA')
    }
    return parsed.records.slice(-MAX_OUTBOX_RECORDS)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return []
    throw new OutboxPersistenceError(error instanceof Error ? error.message : String(error))
  }
}

function isOutboxRecord(value: unknown): value is DurableOutboundRecord {
  if (typeof value !== 'object' || value === null) return false
  const record = value as Partial<DurableOutboundRecord>
  const msg = record.msg as Partial<SendMessageReq['msg']> | undefined
  return typeof record.id === 'string'
    && record.id.length <= MAX_ID_LENGTH
    && typeof record.accountId === 'string'
    && record.accountId.length <= MAX_ID_LENGTH
    && typeof record.userId === 'string'
    && record.userId.length <= MAX_ID_LENGTH
    && typeof record.contextToken === 'string'
    && record.contextToken.length <= MAX_CONTEXT_TOKEN_LENGTH
    && Number.isFinite(record.createdAt)
    && typeof msg === 'object'
    && msg !== null
    && typeof msg.client_id === 'string'
    && msg.client_id.length <= MAX_ID_LENGTH
    && typeof msg.to_user_id === 'string'
    && Array.isArray(msg.item_list)
}

function saveOutboxRecords(records: DurableOutboundRecord[]): void {
  const content = JSON.stringify({ version: 1, records }, null, 2)
  if (Buffer.byteLength(content, 'utf8') > MAX_OUTBOX_BYTES) {
    throw new OutboxPersistenceError('OUTBOX_TOO_LARGE')
  }
  try {
    writeAtomic(content, OUTBOX_FILE)
  } catch (error) {
    throw new OutboxPersistenceError(error instanceof Error ? error.message : String(error))
  }
}

export function addOutboxRecord(
  record: Omit<DurableOutboundRecord, 'id'> & Partial<Pick<DurableOutboundRecord, 'id'>>
): DurableOutboundRecord {
  const records = readOutboxRecords()
  if (records.length >= MAX_OUTBOX_RECORDS) {
    throw new OutboxPersistenceError('OUTBOX_FULL')
  }
  const next: DurableOutboundRecord = {
    ...record,
    id: record.id ?? randomUUID()
  }
  records.push(next)
  saveOutboxRecords(records)
  return next
}

export function listOutboxRecords(accountId: string): DurableOutboundRecord[] {
  return readOutboxRecords()
    .filter((record) => record.accountId === accountId)
    .sort((left, right) => left.createdAt - right.createdAt)
}

export function removeOutboxRecord(accountId: string, id: string): void {
  const records = readOutboxRecords()
  const next = records.filter((record) => !(record.accountId === accountId && record.id === id))
  if (next.length !== records.length) saveOutboxRecords(next)
}

export function clearOutbox(accountId: string): void {
  const records = readOutboxRecords()
  const next = records.filter((record) => record.accountId !== accountId)
  if (next.length !== records.length) saveOutboxRecords(next)
}
