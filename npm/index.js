'use strict'

const binding = require('node-gyp-build')(__dirname)

class ShardDb {
  constructor(dbRoot) {
    if (typeof dbRoot !== 'string' || !dbRoot)
      throw new TypeError('dbRoot must be a non-empty string')
    this._handle = binding.open(dbRoot)
  }

  query(json) {
    if (typeof json !== 'string')
      throw new TypeError('json must be a string')
    return binding.query(this._handle, json)
  }

  close() {
    binding.close(this._handle)
  }
}

module.exports = ShardDb
