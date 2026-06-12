#ifndef SHARD_DB_H
#define SHARD_DB_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ShardDb ShardDb;

/* Open a shard-db data directory for in-process use.
   db_root must be an existing, writable directory path.
   Returns NULL on error. Only one instance per process is allowed (V1). */
ShardDb *shard_db_open(const char *db_root);

/* Execute a JSON query string and return the JSON response in *out.
   The caller must free *out with shard_db_free_result().
   Thread-safe: multiple threads may call concurrently on the same handle.
   Returns 0 on success, -1 on allocation failure. */
int shard_db_query(ShardDb *db, const char *json, char **out, size_t *out_len);

/* Free a result buffer returned by shard_db_query. */
void shard_db_free_result(char *out);

/* Close the instance and free all resources. */
void shard_db_close(ShardDb *db);

#ifdef __cplusplus
}
#endif

#endif /* SHARD_DB_H */
