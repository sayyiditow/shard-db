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
;(async () => {
  // 1. Open
  db = new ShardDb(tmp)
  assert(db !== null, 'open returns instance')

  // 2. Create object
  const createResp = JSON.parse(await db.query(JSON.stringify({
    mode: 'create-object',
    dir: 'test',
    object: 'items',
    splits: 8,
    max_key: 64,
    fields: ['title:varchar:128']
  })))
  assert(!createResp.error, 'create-object succeeds')

  // 3. Insert
  const insertResp = JSON.parse(await db.query(JSON.stringify({
    mode: 'insert',
    dir: 'test',
    object: 'items',
    key: 'k1',
    value: { title: 'hello world' }
  })))
  assert(!insertResp.error, 'insert succeeds')

  // 4. Get
  const getResp = JSON.parse(await db.query(JSON.stringify({
    mode: 'get',
    dir: 'test',
    object: 'items',
    key: 'k1'
  })))
  assert(getResp.title === 'hello world', 'get returns correct value')

  // 5. Count
  const countResp = JSON.parse(await db.query(JSON.stringify({
    mode: 'count',
    dir: 'test',
    object: 'items'
  })))
  assert(countResp === 1, 'count returns 1')

  // 6. Delete
  const delResp = JSON.parse(await db.query(JSON.stringify({
    mode: 'delete',
    dir: 'test',
    object: 'items',
    key: 'k1'
  })))
  assert(!delResp.error, 'delete succeeds')

  // 7. Count after delete
  const count2 = JSON.parse(await db.query(JSON.stringify({
    mode: 'count',
    dir: 'test',
    object: 'items'
  })))
  assert(count2 === 0, 'count is 0 after delete')

  // 8. Object form — query() accepts a plain object (no JSON.stringify needed)
  const insertObj = JSON.parse(await db.query({
    mode: 'insert',
    dir: 'test',
    object: 'items',
    key: 'k2',
    value: { title: 'object form' }
  }))
  assert(!insertObj.error, 'object-form insert succeeds')

  const fetchObj = JSON.parse(await db.query({
    mode: 'get',
    dir: 'test',
    object: 'items',
    key: 'k2'
  }))
  assert(fetchObj.title === 'object form', 'object-form get returns correct value')

  // 10. update
  const updResp = JSON.parse(await db.query({ mode: 'update', dir: 'test', object: 'items', key: 'k2', value: { title: 'updated' } }))
  assert(!updResp.error, 'update succeeds')
  const getUpd = JSON.parse(await db.query({ mode: 'get', dir: 'test', object: 'items', key: 'k2' }))
  assert(getUpd.title === 'updated', 'update persisted correct value')

  // 11. exists — present and absent
  const ex1 = JSON.parse(await db.query({ mode: 'exists', dir: 'test', object: 'items', key: 'k2' }))
  assert(ex1 === true, 'exists returns true for present key')
  const ex2 = JSON.parse(await db.query({ mode: 'exists', dir: 'test', object: 'items', key: 'missing' }))
  assert(ex2 === false, 'exists returns false for absent key')

  // 12. bulk-insert
  const bulkIns = JSON.parse(await db.query({ mode: 'bulk-insert', dir: 'test', object: 'items', records: [
    { key: 'k3', value: { title: 'three' } },
    { key: 'k4', value: { title: 'four'  } }
  ]}))
  assert(!bulkIns.error, 'bulk-insert succeeds')

  // 13. find
  const found = JSON.parse(await db.query({ mode: 'find', dir: 'test', object: 'items', criteria: {}, limit: 10 }))
  assert(Array.isArray(found) && found.length === 3, 'find returns all 3 records')

  // 14. keys
  const keysList = JSON.parse(await db.query({ mode: 'keys', dir: 'test', object: 'items', limit: 10 }))
  assert(Array.isArray(keysList) && keysList.length === 3, 'keys returns 3 keys')

  // 15. fetch
  const fetched = JSON.parse(await db.query({ mode: 'fetch', dir: 'test', object: 'items', limit: 10 }))
  assert(Array.isArray(fetched.results) && fetched.results.length === 3, 'fetch returns 3 records')

  // 16. bulk-delete
  const bulkDel = JSON.parse(await db.query({ mode: 'bulk-delete', dir: 'test', object: 'items', keys: ['k3', 'k4'] }))
  assert(!bulkDel.error, 'bulk-delete succeeds')
  const countAfterBulk = JSON.parse(await db.query({ mode: 'count', dir: 'test', object: 'items' }))
  assert(countAfterBulk === 1, 'count is 1 after bulk-delete')

  // 17. aggregate — separate object with numeric field
  const createScores = JSON.parse(await db.query({ mode: 'create-object', dir: 'test', object: 'scores', splits: 8, max_key: 64, fields: ['score:int'] }))
  assert(!createScores.error, 'create-object scores succeeds')
  await db.query({ mode: 'bulk-insert', dir: 'test', object: 'scores', records: [
    { key: 's1', value: { score: 10 } },
    { key: 's2', value: { score: 20 } },
    { key: 's3', value: { score: 30 } }
  ]})
  const agg = JSON.parse(await db.query({ mode: 'aggregate', dir: 'test', object: 'scores', aggregates: [{ fn: 'sum', field: 'score', alias: 'total' }] }))
  assert(agg && agg.total === 60, 'aggregate sum returns correct total')

  // 18. list-objects
  const objs = JSON.parse(await db.query({ mode: 'list-objects', dir: 'test' }))
  assert(Array.isArray(objs.objects) && objs.objects.includes('items'), 'list-objects includes items')

  // 19. describe-object
  const desc = JSON.parse(await db.query({ mode: 'describe-object', dir: 'test', object: 'items' }))
  assert(desc && !desc.error, 'describe-object returns schema')

  // 20. drop-object
  const drop = JSON.parse(await db.query({ mode: 'drop-object', dir: 'test', object: 'scores' }))
  assert(!drop.error, 'drop-object succeeds')
  const objsAfterDrop = JSON.parse(await db.query({ mode: 'list-objects', dir: 'test' }))
  assert(!objsAfterDrop.objects.includes('scores'), 'dropped object no longer listed')

})().catch(e => { console.error(e); failed++ }).finally(() => {
  if (db) db.close()
  fs.rmSync(tmp, { recursive: true, force: true })
  console.log(`\n1..${passed + failed}`)
  console.log(`# ${passed} passed, ${failed} failed`)
  if (failed > 0) process.exit(1)
})
