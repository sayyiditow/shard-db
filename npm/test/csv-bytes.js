'use strict'
const assert = require('node:assert/strict')
const decode = require('../csv-bytes')
function encode(bytes) {
  return Buffer.concat(Array.from(bytes, byte => byte === 92 ? Buffer.from('\\\\') :
    byte < 32 || byte === 127 ? Buffer.from('\\u00' + byte.toString(16).padStart(2, '0')) : Buffer.from([byte])))
}
let state = 20261027
for (let caseNo = 0; caseNo < 2000; caseNo++) {
  const bytes = Buffer.alloc(caseNo % 513)
  for (let i = 0; i < bytes.length; i++) {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0
    bytes[i] = state >>> 24
  }
  assert.deepEqual(decode(encode(bytes)), bytes)
}
const everyByte = Buffer.from(Array.from({ length: 256 }, (_, i) => i))
assert.deepEqual(decode(encode(everyByte)), everyByte)
assert.deepEqual(decode('\\u0000'), Buffer.from([0]))
assert.deepEqual(decode('\\\\u0000'), Buffer.from('\\u0000'))
assert.deepEqual(decode('é'), Buffer.from('é'))
for (const bad of ['\\', '\\z', '\\u00gg', '\\u1234', '\\u000']) assert.throws(() => decode(bad), SyntaxError)
assert.throws(() => decode(1), TypeError)
console.log('CSV byte decoder: 2000 seeded round trips, all 256 bytes, NUL/literal distinction and invalid escapes pass')
