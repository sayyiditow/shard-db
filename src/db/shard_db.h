#ifndef SHARD_DB_H
#define SHARD_DB_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ShardDb ShardDb;

/* Open a shard-db data directory for in-process use.
   db_root must be an existing, writable directory path.
   Returns NULL on error, including failure to start required background
   infrastructure. Only one instance may be open at a time (V1); sequential
   open-close-open use in the same process is supported. */
ShardDb *shard_db_open(const char *db_root);

/* Execute a JSON query string and return the JSON response in *out.
   The caller must free *out with shard_db_free_result().
   Thread-safe: multiple threads may call concurrently on the same handle.
   Returns 0 on success, -1 on allocation failure. */
int shard_db_query(ShardDb *db, const char *json, char **out, size_t *out_len);

/* Free a result buffer returned by shard_db_query. */
void shard_db_free_result(char *out);

/* Stop and join internal maintenance threads, flush/shut down caches and
   pools, then free the instance. */
void shard_db_close(ShardDb *db);

/* Log event type constants passed to the shard_db_set_log_handler callback. */
#define SHARD_DB_LOG_ERROR 1  /* internal errors */
#define SHARD_DB_LOG_WARN  2  /* warnings */
#define SHARD_DB_LOG_INFO  3  /* general info */
#define SHARD_DB_LOG_DEBUG 4  /* verbose debug */
#define SHARD_DB_LOG_AUDIT 5  /* auth / write audit trail */
#define SHARD_DB_LOG_SLOW  6  /* slow-query threshold crossed */

/* Register a process-wide log handler for embedded use. shard-db allows
   only one ShardDb instance per process (V1), so this is a single
   process-wide slot, not a per-handle setting — it may be called any time,
   including before shard_db_open() returns. Registering before
   shard_db_open() is the only way to observe startup-migration
   diagnostics, which can fire before the caller holds a ShardDb* to pass
   to shard_db_set_log_handler().
   fn is called synchronously on the emitting thread for every log event.
   Pass NULL to unregister. The handler must be thread-safe — shard-db
   parallel workers can emit log events from threads other than the caller.
   shard_db_open() never writes log files; if no handler is registered,
   events print to stderr as a last resort instead of being written to
   disk unasked or silently dropped.
   msg is a pre-formatted, newline-terminated string:
     "YYYY-MM-DD HH:MM:SS LEVEL [subsystem] text\n" */
void shard_db_set_log_handler_global(
    void (*fn)(int type, const char *msg, void *userdata),
    void *userdata);

/* Per-handle convenience wrapper over shard_db_set_log_handler_global() —
   db is accepted for API compatibility (there is only ever one handler
   slot per process) but otherwise unused. */
void shard_db_set_log_handler(ShardDb *db,
    void (*fn)(int type, const char *msg, void *userdata),
    void *userdata);

#ifdef __cplusplus
}
#endif

#endif /* SHARD_DB_H */
