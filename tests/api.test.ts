import assert from 'node:assert/strict'
import test from 'node:test'
import { buildHeaders, validateBaseUrl } from '../api.js'

test('validateBaseUrl accepts the default WeChat endpoint', () => {
  assert.equal(validateBaseUrl('https://ilinkai.weixin.qq.com/'), 'https://ilinkai.weixin.qq.com')
})

test('validateBaseUrl rejects unsafe or unapproved endpoints', () => {
  for (const value of [
    'http://ilinkai.weixin.qq.com',
    'https://ilinkai.weixin.qq.com/?token=secret',
    'https://user:pass@ilinkai.weixin.qq.com',
    'https://127.0.0.1',
    'https://attacker.example.com'
  ]) {
    assert.throws(() => validateBaseUrl(value))
  }
})

test('buildHeaders only sends Authorization for a non-empty token', () => {
  assert.equal(buildHeaders('').Authorization, undefined)
  assert.equal(buildHeaders(' token ').Authorization, 'Bearer token')
})
