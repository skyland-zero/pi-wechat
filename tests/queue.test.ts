import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'

// queue.ts resolves its state directory from the home directory at import time;
// run the persistence test in an isolated child process to avoid touching the
// developer's real ~/.pi-wechat state.
test('durable inbox/outbox records survive and stay account-scoped', () => {
  const home = mkdtempSync(path.join(os.tmpdir(), 'pi-wechat-queue-'))
  try {
    const script = `
      const assert = (await import('node:assert/strict')).default
      const q = await import('./queue.ts')
      const base = {
        accountId: 'account-a', userId: 'user-a', messageId: 'message-1',
        contextToken: 'context-a', text: 'hello', receivedAt: Date.now(), attempts: 0
      }
      assert.equal(q.addInboxRecord(base), true)
      assert.equal(q.addInboxRecord(base), false)
      assert.equal(q.loadPendingInbox('account-a').length, 1)
      q.markInboxActive('account-a', q.loadPendingInbox('account-a')[0].id)
      assert.equal(q.loadPendingInbox('account-a').length, 1)
      assert.equal(q.addInboxRecord({ ...base, accountId: 'account-b', messageId: 'message-2' }), true)
      q.addOutboxRecord({
        accountId: 'account-a', userId: 'user-a', contextToken: 'context-a',
        createdAt: Date.now(), msg: {
          to_user_id: 'user-a', context_token: 'context-a', client_id: 'client-1',
          item_list: [{ type: 1, text_item: { text: 'reply' } }]
        }
      })
      assert.equal(q.listOutboxRecords('account-a').length, 1)
      assert.equal(q.listOutboxRecords('account-b').length, 0)
      q.clearOutbox('account-a')
      assert.equal(q.listOutboxRecords('account-a').length, 0)
      q.clearInbox('account-a')
      assert.equal(q.loadPendingInbox('account-a').length, 0)
      assert.equal(q.loadPendingInbox('account-b').length, 1)
    `
    execFileSync(process.execPath, ['--import', 'tsx/esm', '--eval', script], {
      cwd: process.cwd(),
      env: { ...process.env, HOME: home, USERPROFILE: home },
      stdio: 'pipe'
    })
  } finally {
    rmSync(home, { recursive: true, force: true })
  }
})
