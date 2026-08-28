# Embedded mode

shard-db can run **in-process** inside your application — no daemon, no TCP
socket, no separate process.  You open a data directory, run queries
synchronously, and close when you're done.  All the same storage, indexes,
and query capabilities are available; the only things missing are the TCP
server and its config knobs (`PORT`, `TLS_*`, `THREADS`, etc.).

## When to use it

| Use daemon mode | Use embedded mode |
|---|---|
| Multiple applications share one database | Single application owns the data |
| Remote access required | All access is in-process |
| Multi-language clients (Python, Go, …) | C, C++, or Node.js / Bun |
| Hot reload / live config changes | Restart is acceptable |

## C API

Include `shard_db.h` (exported alongside `libshard-db.a` during `./build.sh`):

```c
#include "shard_db.h"
```

### Open

```c
ShardDb *db = shard_db_open("/path/to/data");
if (!db) { /* check stderr */ }
```

`db_root` must be an existing, writable directory. One instance may be open at
a time (single-instance guard enforced with an atomic flag); after close, the
same process may open another instance. Open returns `NULL` if required
background infrastructure cannot be started.

### Query

```c
char  *out     = NULL;
size_t out_len = 0;

int rc = shard_db_query(db,
    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\"}",
    &out, &out_len);

if (rc == 0) {
    printf("%.*s\n", (int)out_len, out);   /* e.g. 42 */
    shard_db_free_result(out);
}
```

`shard_db_query` is **thread-safe** — multiple threads may call it concurrently
on the same handle.  Returns 0 on success, -1 on allocation failure.  The
response is the same JSON string the daemon would return over TCP; parse it
with any JSON library.

Always free the result with `shard_db_free_result(out)`.

### Close

```c
shard_db_close(db);
```

Stops and joins auto-vacuum, auto-reshard, and any explicitly enabled async
warmup before shutting down the CPU/I/O pools and flushing caches. After this call the pointer is invalid. A maintenance operation already
in progress may therefore extend close latency, but it cannot race cache or
mutex teardown.

### Log handler

By default embedded mode is **silent** — no log files, no ring buffer.
Register a callback to receive events:

```c
void my_log(int type, const char *msg, void *userdata) {
    /* msg: "YYYY-MM-DD HH:MM:SS LEVEL [subsystem] text\n" */
    if (type <= SHARD_DB_LOG_WARN)
        fprintf(stderr, "[shard-db] %s", msg);
}

shard_db_set_log_handler(db, my_log, NULL);
```

Log type constants:

| Constant | Value | When |
|---|---|---|
| `SHARD_DB_LOG_ERROR` | 1 | Internal errors |
| `SHARD_DB_LOG_WARN`  | 2 | Warnings |
| `SHARD_DB_LOG_INFO`  | 3 | General info |
| `SHARD_DB_LOG_DEBUG` | 4 | Verbose debug |
| `SHARD_DB_LOG_AUDIT` | 5 | Auth / write audit trail |
| `SHARD_DB_LOG_SLOW`  | 6 | Slow-query threshold crossed |

**Thread safety:** the handler is called from whichever thread emits the
event — including shard-db's internal worker threads.  The handler must be
thread-safe.

**Timing:** set the handler after `shard_db_open` returns.  Log events that
occur during `open` itself (schema load, recovery) are emitted before the
handler is registered and will not be delivered. Background maintenance events
emitted afterward are delivered normally.

## Configuration

shard-db reads `db.env` from the process's **current working directory** on
`shard_db_open`, if the file exists.  `DB_ROOT` inside the file is
ignored — the `db_root` argument to `shard_db_open` is always authoritative.

Useful knobs for embedded mode:

```bash
# db.env  (place in your app's working directory)
SLOW_QUERY_MS=200       # default 500 — log queries slower than this
GLOBAL_LIMIT=10000      # default 100000 — max records returned per query
FCACHE_MAX=4096         # default 4096 — entries each for kfcache/segcache; derives bt_cache/bm_cache at /4
TOKEN_CAP=1024          # default 1024 — token table buckets
LOG_LEVEL=3             # default 3 — 1=ERROR 2=WARN 3=INFO 4=DEBUG
# WARMUP=async           # embedded default is off unless explicitly configured
```

Durability is synchronous, not periodic. Indexed/bulk mutations pass through
window barriers (M/A/I/K/T/C marker cycle) that fsync before returning.
Cached mmap mutations outside a window are marked dirty and flushed with a
blocking `MS_SYNC` write at cache-eviction time; a failed flush restores the
dirty state for retry on the next eviction attempt and is reported through the
log callback. This does not provide WAL semantics or complete directory-entry
metadata durability.

Daemon warmup still defaults to `async`. Embedded mode deliberately defaults
warmup to `off`; explicit `WARMUP=off`, `sync`, or `async` is honored. All
asynchronous maintenance threads are joined by `shard_db_close`.

If no `db.env` is present all settings stay at their compiled defaults.
The file is only read once at open time; changing it after `shard_db_open`
has no effect until the next open.

## Linking

`./build.sh` produces `build/bin/libshard-db.a` and `build/bin/shard_db.h`.

```makefile
CFLAGS  += -I/path/to/build/bin
LDFLAGS += -L/path/to/build/bin -lshard-db -lpthread
# On Linux also: -lm
# With TLS enabled: -lssl -lcrypto
```

## Limitations

- **One instance open at a time.** A second `shard_db_open` call returns NULL
  until the first is closed; sequential open → close → open is supported.
- **No TCP server.**  Daemon config knobs that relate to the TCP layer
  (`PORT`, `TLS_*`, `THREADS`, `WORKERS`, `MAX_REQUEST_SIZE`) are parsed
  from `db.env` if present but have no effect.  `MAX_CONCURRENT_QUERIES`
  does not cap concurrency in embedded mode but does influence the
  per-query memory budget auto-tune.
- **Startup logs not delivered.**  Events emitted during `shard_db_open`
  occur before the handler is registered.  If you need to capture them, start
  the daemon, redirect its log to your log pipeline, and use the TCP API
  instead.
