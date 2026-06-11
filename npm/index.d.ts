export = ShardDb

declare class ShardDb {
  /** Open a shard-db data directory for in-process use. */
  constructor(dbRoot: string)

  /**
   * Execute a JSON query string synchronously.
   * Returns the JSON response string.
   * Thread-safe: multiple threads may call concurrently on the same instance.
   */
  query(json: string): string

  /** Close the database and release all resources. */
  close(): void
}
