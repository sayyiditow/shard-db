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
   * Migrate one object from fixed-slot to variable-length segment format.
   * Idempotent — safe to call on already-migrated objects (returns immediately).
   * Resolves to `{"status":"ok","migrated":true}` on success,
   * `{"status":"ok","migrated":false}` if already variable-length,
   * or `{"error":"..."}` on failure.
   * Called automatically during construction; use this for explicit per-object control.
   */
  migrate(dir: string, object: string): Promise<string>

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

  interface Aggregate {
    fn: 'sum' | 'avg' | 'min' | 'max' | 'count'
    field: string
    alias?: string
  }

  type QueryBody =
    // ── CRUD ──────────────────────────────────────────────────────────────
    | { mode: 'get'
        dir: string; object: string; key: string
        fields?: string[] }

    | { mode: 'exists'
        dir: string; object: string; key: string }

    | { mode: 'insert'
        dir: string; object: string; key: string
        value: Record<string, unknown> }

    | { mode: 'delete'
        dir: string; object: string; key: string }

    | { mode: 'bulk-insert'
        dir: string; object: string
        records: Array<{ key: string; value: Record<string, unknown> }> }

    | { mode: 'bulk-delete'
        dir: string; object: string
        keys: string[] }

    // ── Query ─────────────────────────────────────────────────────────────
    | { mode: 'find'
        dir: string; object: string
        criteria?: Criteria
        limit?: number
        offset?: number
        order_by?: string
		order?: 'asc' | 'desc'
        /** Resume cursor from a previous paginated response. null = first page. */
        cursor?: Record<string, unknown> | null
        /** Return full match count alongside the page. */
        total?: boolean
        fields?: string[]
        /** Response format — 'dict' returns a {key: value} object instead of an array. */
        format?: 'dict' }

    | { mode: 'count'
        dir: string; object: string
        criteria?: Criteria }

    | { mode: 'aggregate'
        dir: string; object: string
        aggregates: Aggregate[]
        group_by?: string
        criteria?: Criteria
        having?: Criteria }

    | { mode: 'keys'
        dir: string; object: string
        offset?: number; limit?: number }

    | { mode: 'fetch'
        dir: string; object: string
        offset?: number; limit?: number
        fields?: string[] }

    // ── Schema ────────────────────────────────────────────────────────────
    | { mode: 'create-object'
        dir: string; object: string
        splits: number; max_key: number
        fields: string[]
        indexes?: string[] }

    | { mode: 'add-dir'
        dir: string }

    | { mode: 'add-index'
        dir: string; object: string
        fields: string[] }

    | { mode: 'remove-index'
        dir: string; object: string
        fields: string[] }

    // ── Maintenance ───────────────────────────────────────────────────────
    | { mode: 'truncate'
        dir: string; object: string }

    | { mode: 'vacuum'
        dir: string; object: string }

    | { mode: 'migrate'
        dir: string; object: string }

    | { mode: 'stats' }

    // ── Single-record update ──────────────────────────────────────────────
    | { mode: 'update'
        dir: string; object: string; key: string
        value: Record<string, unknown>
        /** CAS guard — only update if current value matches. */
        if?: Record<string, unknown> }

    // ── Object lifecycle ──────────────────────────────────────────────────
    | { mode: 'drop-object'
        dir: string; object: string }

    // ── Catalog ───────────────────────────────────────────────────────────
    | { mode: 'list-objects'
        dir: string }

    | { mode: 'describe-object'
        dir: string; object: string }
}
