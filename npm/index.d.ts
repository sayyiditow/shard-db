export = ShardDb

declare class ShardDb {
  /** Open a shard-db data directory for in-process use. */
  constructor(dbRoot: string)

  /**
   * Execute a query synchronously.
   * Accepts a typed QueryBody object (recommended — enables autocomplete)
   * or a raw JSON string (backward compatible).
   * Returns the JSON response string. Parse with JSON.parse().
   * Thread-safe: multiple threads may call concurrently on the same instance.
   */
  query(body: ShardDb.QueryBody): string
  query(json: string): string

  /** Close the database and release all resources. */
  close(): void
}

declare namespace ShardDb {
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
