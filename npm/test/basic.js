'use strict'

const os   = require('os')
const path = require('path')
const fs   = require('fs')
const ShardDb = require('..')

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'shard-db-test-'))

let passed = 0
let failed = 0

function assert(cond, msg) {
  if (cond) { console.log('ok -', msg); passed++ }
  else       { console.error('not ok -', msg); failed++ }
}

let db
try {
  // 1. Open
  db = new ShardDb(tmp)
  assert(db !== null, 'open returns instance')

  // 2. Create object
  const createResp = JSON.parse(db.query(JSON.stringify({
    mode: 'create-object',
    dir: 'test',
    object: 'items',
    splits: 8,
    max_key: 64,
    fields: ['title:varchar:128']
  })))
  assert(!createResp.error, 'create-object succeeds')

  // 3. Insert
  const insertResp = JSON.parse(db.query(JSON.stringify({
    mode: 'insert',
    dir: 'test',
    object: 'items',
    key: 'k1',
    value: { title: 'hello world' }
  })))
  assert(!insertResp.error, 'insert succeeds')

  // 4. Get
  const getResp = JSON.parse(db.query(JSON.stringify({
    mode: 'get',
    dir: 'test',
    object: 'items',
    key: 'k1'
  })))
  assert(getResp.title === 'hello world', 'get returns correct value')

  // 5. Count
  const countResp = JSON.parse(db.query(JSON.stringify({
    mode: 'count',
    dir: 'test',
    object: 'items'
  })))
  assert(countResp === 1, 'count returns 1')

  // 6. Delete
  const delResp = JSON.parse(db.query(JSON.stringify({
    mode: 'delete',
    dir: 'test',
    object: 'items',
    key: 'k1'
  })))
  assert(!delResp.error, 'delete succeeds')

  // 7. Count after delete
  const count2 = JSON.parse(db.query(JSON.stringify({
    mode: 'count',
    dir: 'test',
    object: 'items'
  })))
  assert(count2 === 0, 'count is 0 after delete')

  // 8. Object form — query() accepts a plain object (no JSON.stringify needed)
  const insertObj = JSON.parse(db.query({
    mode: 'insert',
    dir: 'test',
    object: 'items',
    key: 'k2',
    value: { title: 'object form' }
  }))
  assert(!insertObj.error, 'object-form insert succeeds')

  const fetchObj = JSON.parse(db.query({
    mode: 'get',
    dir: 'test',
    object: 'items',
    key: 'k2'
  }))
  assert(fetchObj.title === 'object form', 'object-form get returns correct value')

} finally {
  if (db) db.close()
  fs.rmSync(tmp, { recursive: true, force: true })
}

console.log(`\n1..${passed + failed}`)
console.log(`# ${passed} passed, ${failed} failed`)
if (failed > 0) process.exit(1)
