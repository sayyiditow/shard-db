'use strict'

// Exact numerics and long strings through the embedded binding. db.query()
// resolves to the raw response text; these assertions compare that text, never
// a JSON.parse()d double, because parsing is exactly where precision is lost.

const assert = require('node:assert/strict')
const os = require('os')
const path = require('path')
const fs = require('fs')
const ShardDb = require('..')

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'shard-db-precision-'))
const db = new ShardDb(tmp)

const q = async body => db.query(body)
const field = (raw, name) => {
  const m = new RegExp(`"${name}":("(?:[^"\\\\]|\\\\.)*"|[^,}\\]\\s]+)`).exec(raw)
  assert.ok(m, `field ${name} present in ${raw.slice(0, 120)}`)
  return m[1]
}

;(async () => {
  assert.equal(JSON.parse(await q({ mode: 'create-object', dir: 'fin', object: 'ledger',
    splits: 8, max_key: 32,
    fields: ['exact:numeric:26,10', 'bare:numeric', 'memo:varchar'] })).status, 'created')

  const exact = '1234567890123456.1234567890'
  const bare = '-98765432109876543210.123456789012345678'
  const memo = 'x'.repeat(100000)
  const ins = JSON.parse(await q({ mode: 'insert', dir: 'fin', object: 'ledger', key: 'k',
    value: { exact, bare, memo } }))
  assert.ok(!ins.error, 'insert succeeds')

  const raw = await q({ mode: 'get', dir: 'fin', object: 'ledger', key: 'k' })
  assert.equal(field(raw, 'exact'), exact, 'constrained decimal is the exact digit string')
  assert.equal(field(raw, 'bare'), bare, 'bare numeric is the exact digit string')
  assert.equal(field(raw, 'memo'), JSON.stringify(memo), '100000-byte varchar is complete')

  // The hazard this test exists to document: a plain JSON.parse turns the
  // decimal into a double and drops digits. Clients must keep the raw text.
  const parsed = JSON.parse(raw)
  assert.notEqual(String(parsed.exact), exact, 'JSON.parse() is lossy for this value (expected)')
  assert.equal(typeof parsed.exact, 'number')

  // Rejections leave nothing behind.
  const bad = JSON.parse(await q({ mode: 'insert', dir: 'fin', object: 'ledger', key: 'bad',
    value: { exact: '12345678901234567890123456.1' } }))
  assert.ok(bad.error, 'a decimal that cannot fit numeric:26,10 is rejected')
  assert.equal(JSON.parse(await q({ mode: 'exists', dir: 'fin', object: 'ledger', key: 'bad' })), false)

  console.log('npm precision: exact decimal, bare numeric and 100000-byte varchar round trip as raw text')
})().catch(e => { console.error(e); process.exitCode = 1 }).finally(() => {
  db.close()
  fs.rmSync(tmp, { recursive: true, force: true })
})
