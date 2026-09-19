import assert from 'node:assert/strict'
import test from 'node:test'
import { chunkText } from '../client.js'

test('chunkText preserves content while respecting the limit', () => {
  const input = 'a'.repeat(4) + '\n' + 'b'.repeat(4)
  const chunks = chunkText(input, 5)
  assert.deepEqual(chunks.join(''), input)
  assert.ok(chunks.every((chunk) => chunk.length <= 5))
})

test('chunkText does not emit empty chunks', () => {
  assert.deepEqual(chunkText('hello', 5), ['hello'])
  assert.deepEqual(chunkText('', 5), [])
})
