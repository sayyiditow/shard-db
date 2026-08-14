export = ShardDb

declare class ShardDb {
  /** Open a shard-db data directory for in-process use. */
  constructor(dbRoot: string)

  /**
   * Execute a query asynchronously.
   * Accepts a typed QueryBody object (recommended — enables autocomplete)
   * or a raw JSON string (backward compatible).
   * Returns a Promise that resolves to the JSON response string. Parse with JSON.parse().
   * Multiple concurrent queries are safe — shard-db's worker pool handles parallelism.
   */
  query(body: ShardDb.QueryBody): Promise<string>
  query(json: string): Promise<string>

  /**
   * Register a callback to receive log events (errors, warnings, slow queries, etc.).
   * The callback fires on the JS thread after each query Promise resolves.
   * Multiple queries may be in flight concurrently; the callback is called once
   * per completed query, in completion order.
   * Pass null to unregister.
   *
   * Log types: 1=error  2=warn  3=info  4=debug  5=audit  6=slow
   * msg is a pre-formatted string: "YYYY-MM-DD HH:MM:SS LEVEL [subsystem] text\n"
   *
   * Note: handler is set after construction; startup logs during new ShardDb()
   * are emitted before the handler is registered and will not be delivered.
   */
  setLogHandler(fn: ShardDb.LogHandler | null): void

  /**
   * Repair corrupted kf (key-file) entries by rescanning all segment files.
   * Idempotent — objects with clean kf return `{"status":"ok","repaired":0}` immediately.
   * Resolves to `{"status":"ok","repaired":N}` where N is the number of entries fixed,
   * or `{"error":"..."}` on failure.
   * Call once per object after upgrading from a release prior to the compact-kf-fix.
   */
  rebuildKf(dir: string, object: string): Promise<string>

  /** Close the database and release all resources. */
  close(): void
}

declare namespace ShardDb {
  /**
   * Log event type passed to the setLogHandler callback.
   * 1=error  2=warn  3=info  4=debug  5=audit  6=slow-query
   */
  type LogType = 'error' | 'warn' | 'info' | 'debug' | 'audit' | 'slow'

  /**
   * Map from numeric C type (1–6) to LogType string.
   * Index by the numeric type: ShardDb.LOG_TYPES[type] → LogType.
   */
  const LOG_TYPES: readonly ['', 'error', 'warn', 'info', 'debug', 'audit', 'slow']

  /** Callback signature for setLogHandler. type is a raw integer (1–6); use LOG_TYPES[type] to get the string name. */
  type LogHandler = (type: number, msg: string) => void

  /** Arbitrary field criteria — keys are schema field names, values are
   *  scalars (exact match) or operator objects e.g. { gt: 100 }.
   *  Also accepts an array of { field, op, value } filter objects. */
  type Criteria = Record<string, unknown> | unknown[]

  /** Field projection — comma-separated string or array, both accepted on the wire. */
  type Fields = string | string[]

  interface Aggregate {
    fn: 'sum' | 'avg' | 'min' | 'max' | 'count'
    /** Required for sum/avg/min/max; omit for count() over all rows. */
    field?: string
    alias?: string
  }

  interface Join {
    object: string
    local: string
    /** "key" for a primary-key lookup, or any indexed field on the remote object. */
    remote: string
    /** Column prefix in the output. Defaults to the remote object name. */
    as?: string
    type?: 'inner' | 'left'
    fields?: string[]
  }

  /** One record in bulk-insert/bulk-update array form. */
  interface BulkRecord {
    key: string
    value: Record<string, unknown>
    /** Per-record CAS guard (bulk-update array form only). */
    if?: Criteria
  }

  type QueryBody =
    // ── CRUD ──────────────────────────────────────────────────────────────
    | { mode: 'get'
        dir: string; object: string
        /** Single-key form. */
        key?: string
        /** Multi-key form — response is {key: value | null}. */
        keys?: string[]
        fields?: Fields }

    | { mode: 'exists'
        dir: string; object: string
        key?: string
        keys?: string[] }

    | { mode: 'insert'
        dir: string; object: string
        /** Omit when the object has auto_key configured — the server generates one. */
        key?: string
        value: Record<string, unknown>
        /** CAS guard — fail if the key already exists. Not combinable with an omitted key. */
        if_not_exists?: boolean
        /** CAS guard — only insert/upsert if current value (if any) matches. */
        if?: Criteria }

    | { mode: 'update'
        dir: string; object: string; key: string
        value: Record<string, unknown>
        /** CAS guard — only update if current value matches. */
        if?: Criteria }

    | { mode: 'delete'
        dir: string; object: string; key: string
        /** CAS guard — only delete if current value matches. */
        if?: Criteria }

    | { mode: 'not-exists'
        dir: string; object: string
        keys: string[] }

    // ── Bulk ──────────────────────────────────────────────────────────────
    | { mode: 'bulk-insert'
        dir: string; object: string
        /** Dict form ({key: value}), array form, or omit and pass `file`. */
        records?: Record<string, Record<string, unknown>> | BulkRecord[]
        file?: string
        /** Skip (don't overwrite) keys that already exist; response includes "skipped". */
        if_not_exists?: boolean }

    | { mode: 'bulk-insert-delimited'
        dir: string; object: string
        file?: string
        data?: string
        /** Single character; default '|'. */
        delimiter?: string }

    | { mode: 'bulk-delete'
        dir: string; object: string
        /** By key list. */
        keys?: string[]
        /** By criteria — mutually exclusive with `keys`. */
        criteria?: Criteria
        file?: string
        limit?: number
        dry_run?: boolean }

    | { mode: 'bulk-update'
        dir: string; object: string
        /** Criteria-driven mass update. */
        criteria?: Criteria
        value?: Record<string, unknown>
        /** Per-key partial update — dict or array form. Mutually exclusive with criteria/value. */
        records?: Record<string, Record<string, unknown>> | BulkRecord[]
        file?: string
        limit?: number
        dry_run?: boolean }

    | { mode: 'bulk-update-delimited'
        dir: string; object: string
        file?: string
        data?: string
        delimiter?: string }

    // ── Query ─────────────────────────────────────────────────────────────
    | { mode: 'find'
        dir: string; object: string
        criteria?: Criteria
        limit?: number
        offset?: number
        fields?: Fields
        /** Skip these keys from results. Comma-separated string or array. */
        excludedKeys?: Fields
        order_by?: string
        order?: 'asc' | 'desc'
        /** Resume cursor from a previous paginated response. null = first page. */
        cursor?: Record<string, unknown> | null
        /** Return full match count alongside the page. */
        total?: boolean
        /** Response format — 'dict' returns a {key: value} object instead of an array. Ignored (forced tabular) when `join` is present. */
        format?: 'rows' | 'dict'
        join?: Join[]
        explain?: boolean }

    | { mode: 'fetch'
        dir: string; object: string
        offset?: number; limit?: number
        fields?: Fields
        format?: 'dict' }

    | { mode: 'count'
        dir: string; object: string
        criteria?: Criteria }

    | { mode: 'aggregate'
        dir: string; object: string
        aggregates: Aggregate[]
        group_by?: string[]
        criteria?: Criteria
        /** Same shape as criteria, but fields are aggregate aliases (or group-by fields). */
        having?: Criteria
        order_by?: string
        order?: 'asc' | 'desc'
        limit?: number
        explain?: boolean }

    | { mode: 'keys'
        dir: string; object: string
        offset?: number; limit?: number }

    // ── Schema ────────────────────────────────────────────────────────────
    | { mode: 'create-object'
        dir: string; object: string
        splits?: number; max_key?: number
        fields: string[]
        indexes?: string[]
        /** Server-generated keys: "uuid" or "seq(<name>)". Immutable once set. */
        auto_key?: string }

    | { mode: 'drop-object'
        dir: string; object: string }

    | { mode: 'add-field'
        dir: string; object: string
        fields: string[] }

    | { mode: 'edit-field'
        dir: string; object: string
        fields: string[]
        /** Required to change an existing enum value at a position rather than append. */
        allow_rename?: boolean
        dry_run?: boolean }

    | { mode: 'rename-field'
        dir: string; object: string
        old: string; new: string }

    | { mode: 'remove-field'
        dir: string; object: string
        fields: string[] }

    | { mode: 'add-index'
        dir: string; object: string
        /** Single field/spec (e.g. "email", "status:bitmap", "a+b"). */
        field?: string
        /** Multiple fields/specs, built in one shard scan. */
        fields?: string[]
        /** Rebuild even if the index already exists. */
        force?: boolean }

    | { mode: 'remove-index'
        dir: string; object: string
        /** Exact stored index name (e.g. "email", "body:trigram"). */
        field?: string
        fields?: string[] }

    | { mode: 'estimate-index'
        dir: string; object: string
        spec: string }

    // ── Maintenance ───────────────────────────────────────────────────────
    | { mode: 'truncate'
        dir: string; object: string }

    | { mode: 'vacuum'
        dir: string; object: string
        /** Full rebuild: drops tombstoned fields, shrinks slot_size. */
        compact?: boolean
        /** Full rebuild with a new shard count; also triggers reindex. */
        splits?: number }

    | { mode: 'recount'
        dir: string; object: string }

    | { mode: 'backup'
        dir: string; object: string }

    | { mode: 'restore'
        dir: string; object: string
        /** Timestamp directory name under <obj>/backup/. */
        from: string
        /** Required if the live object isn't empty. */
        force?: boolean }

    | { mode: 'sequence'
        dir: string; object: string
        name: string
        action: string
        batch?: number }

    // ── Files ─────────────────────────────────────────────────────────────
    | { mode: 'put-file'
        dir: string; object: string
        filename?: string
        /** Base64-encoded bytes (remote-safe form). */
        data?: string
        /** Server-local path (admin fast path) — mutually exclusive with filename/data. */
        path?: string
        if_not_exists?: boolean }

    | { mode: 'get-file'
        dir: string; object: string; filename: string }

    | { mode: 'get-file-path'
        dir: string; object: string; filename: string }

    | { mode: 'delete-file'
        dir: string; object: string; filename: string }

    | { mode: 'list-files'
        dir: string; object: string
        pattern?: string
        match?: 'prefix' | 'suffix' | 'contains' | 'glob'
        /** Legacy alias for pattern + match:"prefix". */
        prefix?: string
        offset?: number; limit?: number }

    // ── Diagnostics ───────────────────────────────────────────────────────
    | { mode: 'stats'; format?: 'table' }

    | { mode: 'stats-prom' }

    | { mode: 'shard-stats'
        dir?: string; object?: string
        format?: 'table' }

    | { mode: 'vacuum-check' }

    | { mode: 'db-dirs' }

    | { mode: 'size'
        dir: string; object: string }

    | { mode: 'orphaned'
        dir: string; object: string }

    // ── Catalog ───────────────────────────────────────────────────────────
    | { mode: 'list-objects'
        dir: string }

    | { mode: 'describe-object'
        dir: string; object: string }

    // ── Auth administration (server-admin scope) ────────────────────────────
    | { mode: 'add-token'
        token: string
        dir?: string; object?: string
        perm?: 'r' | 'rw' | 'rwx' }

    | { mode: 'remove-token'
        token?: string
        fingerprint?: string }

    | { mode: 'list-tokens' }

    | { mode: 'add-dir'
        dir: string }

    | { mode: 'remove-dir'
        dir: string
        check_empty?: boolean }

    | { mode: 'add-ip'
        ip: string }

    | { mode: 'remove-ip'
        ip: string }

    | { mode: 'list-ips' }
}
