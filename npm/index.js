'use strict'

const LOG_TYPES = Object.freeze(['', 'error', 'warn', 'info', 'debug', 'audit', 'slow'])

const binding = require('node-gyp-build')(__dirname)

class ShardDb {
  constructor(dbRoot) {
    if (typeof dbRoot !== 'string' || !dbRoot)
      throw new TypeError('dbRoot must be a non-empty string')
    this._handle = binding.open(dbRoot)
  }

  query(bodyOrJson) {
    const json = (typeof bodyOrJson === 'object' && bodyOrJson !== null)
      ? JSON.stringify(bodyOrJson)
      : bodyOrJson
    if (typeof json !== 'string')
      throw new TypeError('query() argument must be a QueryBody object or a JSON string')
    return binding.query(this._handle, json)
  }

  close() {
    binding.close(this._handle)
  }

  setLogHandler(fn) {
    binding.setLogHandler(this._handle, fn ?? null)
  }
}

ShardDb.LOG_TYPES = LOG_TYPES
module.exports = ShardDb
