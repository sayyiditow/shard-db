# Error-Logging Audit Remediation — Implementation Plan

> **For agentic workers:** This plan is executed OUTSIDE the Claude family per
> this repo's `CLAUDE.md` standing exception — a non-Claude model (Gemini,
> GPT, ...) runs it task-by-task on a fresh branch off `main`. Do NOT commit;
> leave the working tree dirty for a Sonnet review pass of the raw `git diff`
> once all tasks are done. If a quoted anchor below isn't found **exactly**
> in the current source, STOP and write `PLAN_NOTES.md` describing the
> mismatch — do not guess or reinterpret the surrounding code. If you hit a
> decision this plan doesn't cover, stop and ask — do not improvise.

**Goal:** Add `LOG_ERROR`/`LOG_WARN` calls (or, for `util.c` only,
`fprintf(stderr, ...)`) at every site in `src/db/*.c` that was flagged by the
2026-07-11 audit as an unexpected/system-level failure returned silently
(`return -1;` / `return NULL;`) with no corresponding entry in `$LOG_DIR`.
Purely additive — no behavior, return values, or client-visible responses
change anywhere in this plan, **with one documented exception**: Task 1
Steps 1–2 reorder *when* `log_init()` starts (moving it earlier in
`cmd_server`, before TLS init) and add `log_shutdown()` calls on the TLS
preflight failure paths so their `LOG_ERROR` lines are guaranteed flushed
before process exit. This changes internal startup sequencing and adds a
bounded flush delay on already-fatal paths, but does not change any client-
visible response, return value, or exit code — see Task 1's "Note on Steps
1–2" for the fork-safety rationale.

**Architecture:** Each task covers one source file (or, for three small
audit groupings, a 2–3 file cluster) and inserts a log call immediately
before each flagged `return` statement, using the subsystem tag and message
already drafted during the audit. No new functions, no new files, no schema
or wire-protocol changes. `tls.c` additionally needs `#include "log.h"`
(currently absent) and a one-parameter signature change to its existing
`tls_log_err()` helper so it can log at either `LOG_LVL_ERROR` or
`LOG_LVL_WARN` depending on call site.

**Tech Stack:** C (existing codebase conventions only). Logging via the
macros in `src/db/log.h`: `LOG_ERROR(sub, fmt, ...)`, `LOG_WARN(sub, fmt,
...)` → `log_msg_sub(level, subsystem, fmt, ...)`. Subsystem tags used below:
`LOG_SUB_TLS`, `LOG_SUB_SERVER`, `LOG_SUB_QUERY`, `LOG_SUB_SLOTCASK`,
`LOG_SUB_REINDEX`, `LOG_SUB_TRIGRAM`, `LOG_SUB_BTREE`, `LOG_SUB_BITMAP`,
`LOG_SUB_CONFIG`, `LOG_SUB_WARMUP`.

## Global Constraints

- **No behavior changes.** Every edit in this plan adds a log call and
  nothing else. Do not alter conditions, return values, error messages sent
  to clients (`OUT(...)`), or control flow.
- **No per-site regression tests.** There is no fault-injection framework in
  this codebase (approved by the human during design — see
  `docs/superpowers/specs/2026-07-11-error-logging-audit-design.md`).
  Definition of done for every task is: clean build, full suite unchanged,
  plus one manual spot-check (below).
- **Build:** `SKIP_TESTS=1 ./build.sh` (per this repo's `CLAUDE.md`).
- **Test:** `./build/bin/shard-db-test run-all` (per this repo's
  `CLAUDE.md`). Must show the same pass count as the pre-change baseline —
  run it once before Task 1 to record that baseline.
- **`util.c` exception:** `fuzz/fuzz_json.c` and `fuzz/fuzz_b64.c` link
  `src/db/util.c` standalone with no `config.c` and no stubs for
  `log_msg_sub`/`log_audit_sub`. Any `LOG_*` macro reference anywhere in
  `util.c` — even in unreached code — breaks that link (undefined
  reference at link time, unconditionally, regardless of runtime
  reachability). `util.c`'s existing `mkdirp` function already hit this and
  uses `fprintf(stderr, "mkdirp: ...");` instead of `LOG_*` — Task 10 (below)
  follows that exact precedent for all `util.c` sites. Do not use `LOG_*` in
  `util.c` under any circumstances. Note this means `util.c`'s new sites are
  best-effort in production: in daemon mode `stderr` is `/dev/null`, so
  these lines are only visible in foreground (`./shard-db server`) runs or
  the fuzz harnesses. This is acceptable because most `util.c` failure
  paths that matter in daemon mode already get a second, `LOG_*`-backed
  message from their caller (e.g. `auto_key_generate` in `server.c`, Task 2,
  and the `gen_uuid4_raw`/`gen_uuid4_batch` entropy helpers in `config.c`,
  Task 8) — `util.c`'s own `fprintf` is a debugging aid, not the production
  signal.
- **Spot-check protocol (per task):** after build + full suite pass, force
  one real failure covered by that task (e.g. `chmod 000` a file the code
  tries to `fopen`, or point a path at `/nonexistent`), issue the request
  that exercises it, and `tail` the relevant `$LOG_DIR/YYYY-MM-DD-error.log`
  (for `LOG_ERROR`) or `$LOG_DIR/YYYY-MM-DD-info.log` (for `LOG_WARN` —
  `open_log_for_level()` in `config.c` routes ERROR to `-error.log` and
  everything else, including WARN, to `-info.log`; there is no `-warn.log`
  file) to confirm the new line appears with the right subsystem tag and
  message. Undo the forced condition afterward (e.g. `chmod` back).
- **Commit:** do not commit. Leave the tree dirty for review, per this
  repo's `CLAUDE.md` execution-mode exception.

## Out of scope (do NOT act on these — follow-up items only)

These issues surfaced during the audit but are structurally different from
"add a log line to an existing silent return" (they involve a silent
`continue`/no-check with no `return -1`/`return NULL` statement to anchor
on, or a discarded return value at multiple call sites) and were explicitly
excluded from this plan's scope:

1. **`resolve_bitmaps` (`src/db/index.c`, ~line 2674–2690):** the per-shard
   kf `open()`/`fstat()`/`mmap()` loop silently `continue`s past failures
   with no log and no `return` — invisible to a `return`-anchored audit.
2. **`copy_file` (`src/db/query_maint.c`, lines ~286/288, used by
   `backup`/similar):** 3 call sites discard its return value entirely, so
   even after Task 11 adds logging inside `copy_file` itself, callers won't
   surface the failure to the client.
3. **`bt_cache_init` (`src/db/btree.c`, line 381):** `calloc(bt_cache_slots,
   sizeof(BtCacheEntry))` return value is never checked at all — no `if`,
   no `return`, nothing; a failure here is a straight NULL-deref the next
   time any code touches `bt_cache[i]`. No `return -1`/`NULL` to anchor on,
   and fixing it means adding a check where none exists today (a
   behavior change, not additive logging) — out of scope for this plan.
4. **`bt_split_leaf` (`src/db/btree.c`, lines 1370 and 1373):** both calls
   to `leaf_rebuild()` discard its `int` return value. Task 9 adds logging
   *inside* `leaf_rebuild` itself at its own failure site, but — same
   pattern as `copy_file` above — these two callers still won't know
   `leaf_rebuild` failed.
5. **Registry silent-drop consistency sweep:** several `*_shard_worker_v2`
   functions across `query_bulk.c`/`query_find.c`/`query_aggregate.c` treat
   a failed `slotcask_registry_get()` as an ordinary "no work for this
   shard" outcome — the caller's aggregate error/skip counters can't
   distinguish "OOM/registry failure" from "nothing matched." Tasks 4 and 5
   add the missing `LOG_WARN` at each individual site (per the findings
   report), which makes the failure visible in `$LOG_DIR`, but a client
   watching only the wire-protocol response still sees an ordinary count.
   A follow-up correctness pass could thread a distinct error signal
   through to the client; that's a larger, non-additive change and is
   explicitly not this plan's job.

## Task ordering rationale

`tls.c` is Task 1 regardless of its raw flagged-site count (22) because it
is "the single biggest practical gap found in the entire audit": once the
daemon daemonizes, `stderr` is redirected to `/dev/null`. Pure additive
logging inside `tls.c` alone is not sufficient to close that gap, though —
`cmd_server` (`server.c`) calls `tls_server_init()` before `log_init()`
starts the log writer thread, so even with `LOG_*` calls added throughout
`tls.c`, a daemonized start with a bad cert/key would still leave zero
trace: `stderr` is already `/dev/null` and the log writer hasn't started
yet. Task 1 therefore also reorders `log_init()` ahead of TLS init inside
`cmd_server` (Steps 1–2) and adds logging to the 3 TLS preflight checks
that live in `server.c` rather than `tls.c` — see the Task 1 Interfaces
note for why that reorder is safe. With that fix, startup TLS misconfig
(preflight checks and `tls_server_init`'s own OpenSSL failures alike) and
runtime handshake failures (`tls_accept`/`tls_connect`, always reached
after `log_init()`) are both covered. Every other task is ordered by
flagged site count, descending, per the findings report's priority
ranking.

---

### Task 1: `src/db/tls.c` (+ `src/db/server.c` startup-ordering fix) — add `#include "log.h"` + refactor `tls_log_err()` + reorder `log_init()` + 22 sites

**Files:**
- Modify: `src/db/tls.c`
- Modify: `src/db/server.c` (Steps 1–2 only — reorders `log_init()` ahead of
  TLS init inside `cmd_server` and adds 3 `LOG_ERROR` calls to the TLS
  preflight checks. Task 2 covers `server.c`'s other 20 flagged sites
  separately; it runs after this task in the same working tree, so its
  anchors assume Steps 1–2 below have already landed.)

**Interfaces:**
- Produces: `tls_log_err(LogLevel level, const char *what)` — the existing
  helper gains a leading `LogLevel level` parameter. All 16 existing call
  sites inside `tls.c` must be updated in the same edit (there are no
  external callers — `tls_log_err` is `static`).

**Note on Steps 1–2 (the `server.c` reorder):** this is the one place in
the whole plan that isn't purely-additive logging — `log_init()`'s call
site moves earlier in `cmd_server`. It is still behavior-preserving from
the client's/operator's perspective: no return value, exit code,
`OUT(...)` message, or `fprintf(stderr, ...)` message changes anywhere.
All that changes is *when* the log writer thread starts. This is safe
specifically because the new call site is **after** `fork()`/`setsid()`
(inside the same `if (daemonize) { ... }` block's post-fork code path) —
starting the writer thread any earlier, before `fork()`, would be unsafe:
`fork()` only carries the calling thread into the child, so the writer
thread would not exist in the daemonized child, and if it happened to
hold `g_log_lock` at fork time the child would deadlock on it forever. Do
not move `log_init()` to any point before the one shown in Step 1 below.

- [ ] **Step 1: Move `log_init()` ahead of TLS init in `cmd_server`, and log the 3 TLS preflight failures**

Locate (exact current text):

```c
    /* Record the running PID inside the lock file for operator visibility
       (lsof / cat .shard-db.lock). Not load-bearing — the lock is what
       enforces exclusion. */
    char pidbuf[32];
    int pidlen = snprintf(pidbuf, sizeof(pidbuf), "%d\n", (int)getpid());
    if (ftruncate(lock_fd, 0) == 0) {
        ssize_t _ignored = pwrite(lock_fd, pidbuf, pidlen, 0);
        (void)_ignored;
    }

    /* TLS init — if enabled, refuse to start without readable cert/key.
       Done before bind/listen so a misconfig fails fast and visibly. */
    if (g_tls_enable) {
        if (g_tls_cert[0] == '\0' || g_tls_key[0] == '\0') {
            fprintf(stderr, "Error: TLS_ENABLE=1 but TLS_CERT and/or TLS_KEY not set in db.env\n");
            return 1;
        }
        if (access(g_tls_cert, R_OK) != 0) {
            fprintf(stderr, "Error: TLS_CERT not readable: %s (%s)\n", g_tls_cert, strerror(errno));
            return 1;
        }
        if (access(g_tls_key, R_OK) != 0) {
            fprintf(stderr, "Error: TLS_KEY not readable: %s (%s)\n", g_tls_key, strerror(errno));
            return 1;
        }
        if (tls_server_init(g_tls_cert, g_tls_key) != 0) {
            fprintf(stderr, "Error: TLS context init failed (see preceding tls: ... message)\n");
            return 1;
        }
    }
```

Replace with:

```c
    /* Record the running PID inside the lock file for operator visibility
       (lsof / cat .shard-db.lock). Not load-bearing — the lock is what
       enforces exclusion. */
    char pidbuf[32];
    int pidlen = snprintf(pidbuf, sizeof(pidbuf), "%d\n", (int)getpid());
    if (ftruncate(lock_fd, 0) == 0) {
        ssize_t _ignored = pwrite(lock_fd, pidbuf, pidlen, 0);
        (void)_ignored;
    }

    /* Start the log writer now, before TLS init. This is the earliest safe
       point post-fork: fork() only carries the calling thread into the
       child, so the writer thread must not exist before fork() runs
       (already happened above, in the `if (daemonize)` block). TLS
       misconfig is the most common day-1 startup failure; previously
       log_init() ran after TLS init and after socket bind/listen, so a
       daemonized start with a bad cert/key left zero trace anywhere —
       stderr was already /dev/null and the log writer hadn't started. */
    log_init(db_root);

    /* TLS init — if enabled, refuse to start without readable cert/key.
       Done before bind/listen so a misconfig fails fast and visibly. */
    if (g_tls_enable) {
        if (g_tls_cert[0] == '\0' || g_tls_key[0] == '\0') {
            LOG_ERROR(LOG_SUB_TLS, "cmd_server: TLS_ENABLE=1 but TLS_CERT and/or TLS_KEY not set in db.env");
            fprintf(stderr, "Error: TLS_ENABLE=1 but TLS_CERT and/or TLS_KEY not set in db.env\n");
            log_shutdown();
            return 1;
        }
        if (access(g_tls_cert, R_OK) != 0) {
            LOG_ERROR(LOG_SUB_TLS, "cmd_server: TLS_CERT not readable: %s (%s)", g_tls_cert, strerror(errno));
            fprintf(stderr, "Error: TLS_CERT not readable: %s (%s)\n", g_tls_cert, strerror(errno));
            log_shutdown();
            return 1;
        }
        if (access(g_tls_key, R_OK) != 0) {
            LOG_ERROR(LOG_SUB_TLS, "cmd_server: TLS_KEY not readable: %s (%s)", g_tls_key, strerror(errno));
            fprintf(stderr, "Error: TLS_KEY not readable: %s (%s)\n", g_tls_key, strerror(errno));
            log_shutdown();
            return 1;
        }
        if (tls_server_init(g_tls_cert, g_tls_key) != 0) {
            LOG_ERROR(LOG_SUB_TLS, "cmd_server: TLS context init failed (see preceding tls_* log line)");
            fprintf(stderr, "Error: TLS context init failed (see preceding tls: ... message)\n");
            log_shutdown();
            return 1;
        }
    }
```

> **Note for the executing model:** `log_shutdown()` is added on each of
> these 4 early-exit paths (but not inside `tls_server_init`/`tls.c`
> itself — see the Step 5 note). `LOG_*` only enqueues onto an in-memory
> ring buffer; a background writer thread drains it to disk
> asynchronously. Without a call that joins that thread before the
> process exits, a `LOG_ERROR` immediately followed by `return 1` (which
> unwinds to `main()` returning, which calls `exit()`) races the writer
> thread — the process can exit before the just-enqueued line ever
> reaches `$LOG_DIR`, silently defeating the whole point of this reorder.
> `log_shutdown()` (`config.c`) sets the running flag to 0, signals the
> writer's condvar, and `pthread_join`s it — so every enqueued line,
> including the one from `LOG_ERROR` two lines above, is guaranteed
> flushed before `log_shutdown()` returns. This costs one flush's worth of
> latency (sub-millisecond in practice) on an already-fatal startup path,
> so it isn't a performance concern.

- [ ] **Step 2: Remove the now-duplicate `log_init()` call further down in `cmd_server`**

Locate (exact current text):

```c
    log_init(db_root);
    write_pid_file(db_root, port);
```

Replace with:

```c
    write_pid_file(db_root, port);
```

> **Note for the executing model:** `log_init(db_root);` appears exactly
> once in `server.c` before Step 1 is applied. After Step 1, that same
> line of source now appears twice — once at its new location (inside the
> block Step 1 just replaced) and once at its original location (this
> step). Step 2 removes the original, now-redundant call. Apply Step 1
> before Step 2, in order.

- [ ] **Step 3: Add the `log.h` include**

Locate (top of file, exact current text):

```c
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "tls.h"

#include <errno.h>
```

Replace with:

```c
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "tls.h"
#include "log.h"

#include <errno.h>
```

- [ ] **Step 4: Refactor `tls_log_err()` to take a `LogLevel` and log through both `stderr` and `$LOG_DIR`**

Locate (exact current text):

```c
static void tls_log_err(const char *what) {
    unsigned long e = ERR_get_error();
    char buf[256] = {0};
    if (e) ERR_error_string_n(e, buf, sizeof(buf));
    fprintf(stderr, "tls: %s%s%s\n", what,
            buf[0] ? ": " : "",
            buf[0] ? buf : "");
}
```

Replace with:

```c
static void tls_log_err(LogLevel level, const char *what) {
    unsigned long e = ERR_get_error();
    char buf[256] = {0};
    if (e) ERR_error_string_n(e, buf, sizeof(buf));
    fprintf(stderr, "tls: %s%s%s\n", what,
            buf[0] ? ": " : "",
            buf[0] ? buf : "");
    if (level == LOG_LVL_ERROR)
        LOG_ERROR(LOG_SUB_TLS, "%s%s%s", what, buf[0] ? ": " : "", buf[0] ? buf : "");
    else
        LOG_WARN(LOG_SUB_TLS, "%s%s%s", what, buf[0] ? ": " : "", buf[0] ? buf : "");
}
```

- [ ] **Step 5: Update `tls_server_init` — 6 `tls_log_err` calls, all `LOG_LVL_ERROR`**

Locate (exact current text):

```c
    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) { tls_log_err("SSL_CTX_new(server)"); return -1; }

    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        tls_log_err("set_min_proto_version(1.3)"); SSL_CTX_free(ctx); return -1;
    }
    if (SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION) != 1) {
        tls_log_err("set_max_proto_version(1.3)"); SSL_CTX_free(ctx); return -1;
    }

    if (SSL_CTX_use_certificate_chain_file(ctx, cert_path) != 1) {
        tls_log_err("use_certificate_chain_file"); SSL_CTX_free(ctx); return -1;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, key_path, SSL_FILETYPE_PEM) != 1) {
        tls_log_err("use_PrivateKey_file"); SSL_CTX_free(ctx); return -1;
    }
    if (SSL_CTX_check_private_key(ctx) != 1) {
        tls_log_err("check_private_key (cert and key do not match)"); SSL_CTX_free(ctx); return -1;
    }
```

Replace with:

```c
    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) { tls_log_err(LOG_LVL_ERROR, "SSL_CTX_new(server)"); return -1; }

    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        tls_log_err(LOG_LVL_ERROR, "set_min_proto_version(1.3)"); SSL_CTX_free(ctx); return -1;
    }
    if (SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION) != 1) {
        tls_log_err(LOG_LVL_ERROR, "set_max_proto_version(1.3)"); SSL_CTX_free(ctx); return -1;
    }

    if (SSL_CTX_use_certificate_chain_file(ctx, cert_path) != 1) {
        tls_log_err(LOG_LVL_ERROR, "use_certificate_chain_file"); SSL_CTX_free(ctx); return -1;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, key_path, SSL_FILETYPE_PEM) != 1) {
        tls_log_err(LOG_LVL_ERROR, "use_PrivateKey_file"); SSL_CTX_free(ctx); return -1;
    }
    if (SSL_CTX_check_private_key(ctx) != 1) {
        tls_log_err(LOG_LVL_ERROR, "check_private_key (cert and key do not match)"); SSL_CTX_free(ctx); return -1;
    }
```

- [ ] **Step 6: Update `tls_client_init` — 4 `tls_log_err` calls, all `LOG_LVL_ERROR`**

Locate (exact current text):

```c
    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) { tls_log_err("SSL_CTX_new(client)"); return -1; }

    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        tls_log_err("set_min_proto_version(1.3)"); SSL_CTX_free(ctx); return -1;
    }

    if (skip_verify) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
    } else {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
        if (ca_path && ca_path[0]) {
            if (SSL_CTX_load_verify_locations(ctx, ca_path, NULL) != 1) {
                tls_log_err("load_verify_locations"); SSL_CTX_free(ctx); return -1;
            }
        } else {
            /* Fall back to OS default trust store. */
            if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
                tls_log_err("set_default_verify_paths"); SSL_CTX_free(ctx); return -1;
            }
        }
    }
```

Replace with:

```c
    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) { tls_log_err(LOG_LVL_ERROR, "SSL_CTX_new(client)"); return -1; }

    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        tls_log_err(LOG_LVL_ERROR, "set_min_proto_version(1.3)"); SSL_CTX_free(ctx); return -1;
    }

    if (skip_verify) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
    } else {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
        if (ca_path && ca_path[0]) {
            if (SSL_CTX_load_verify_locations(ctx, ca_path, NULL) != 1) {
                tls_log_err(LOG_LVL_ERROR, "load_verify_locations"); SSL_CTX_free(ctx); return -1;
            }
        } else {
            /* Fall back to OS default trust store. */
            if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
                tls_log_err(LOG_LVL_ERROR, "set_default_verify_paths"); SSL_CTX_free(ctx); return -1;
            }
        }
    }
```

- [ ] **Step 7: Update `tls_accept` — add a standalone `LOG_ERROR` for the uninitialized-context guard, update 2 `tls_log_err` calls to `LOG_LVL_ERROR`, and 1 to `LOG_LVL_WARN`**

Locate (exact current text):

```c
SSL *tls_accept(int fd) {
    if (!g_tls_server_ctx) return NULL;
    SSL *ssl = SSL_new(g_tls_server_ctx);
    if (!ssl) { tls_log_err("SSL_new(accept)"); return NULL; }
    if (SSL_set_fd(ssl, fd) != 1) { tls_log_err("SSL_set_fd(accept)"); SSL_free(ssl); return NULL; }
    /* Blocking handshake — server worker is blocking I/O via fgets/fwrite. */
    if (SSL_accept(ssl) != 1) {
        tls_log_err("SSL_accept");
        SSL_free(ssl);
        return NULL;
    }
    return ssl;
}
```

Replace with:

```c
SSL *tls_accept(int fd) {
    if (!g_tls_server_ctx) {
        LOG_ERROR(LOG_SUB_TLS, "tls_accept: called for fd=%d but g_tls_server_ctx is NULL (server TLS not initialised)", fd);
        return NULL;
    }
    SSL *ssl = SSL_new(g_tls_server_ctx);
    if (!ssl) { tls_log_err(LOG_LVL_ERROR, "SSL_new(accept)"); return NULL; }
    if (SSL_set_fd(ssl, fd) != 1) { tls_log_err(LOG_LVL_ERROR, "SSL_set_fd(accept)"); SSL_free(ssl); return NULL; }
    /* Blocking handshake — server worker is blocking I/O via fgets/fwrite. */
    if (SSL_accept(ssl) != 1) {
        tls_log_err(LOG_LVL_WARN, "SSL_accept");
        SSL_free(ssl);
        return NULL;
    }
    return ssl;
}
```

- [ ] **Step 8: Update `tls_connect` — same pattern as `tls_accept`**

Locate (exact current text):

```c
SSL *tls_connect(int fd, const char *server_name) {
    if (!g_tls_client_ctx) return NULL;
    SSL *ssl = SSL_new(g_tls_client_ctx);
    if (!ssl) { tls_log_err("SSL_new(connect)"); return NULL; }
    if (SSL_set_fd(ssl, fd) != 1) { tls_log_err("SSL_set_fd(connect)"); SSL_free(ssl); return NULL; }
    if (server_name && server_name[0]) {
        SSL_set_tlsext_host_name(ssl, server_name);
        SSL_set1_host(ssl, server_name);
    }
    if (SSL_connect(ssl) != 1) {
        tls_log_err("SSL_connect");
        SSL_free(ssl);
        return NULL;
    }
    return ssl;
}
```

Replace with:

```c
SSL *tls_connect(int fd, const char *server_name) {
    if (!g_tls_client_ctx) {
        LOG_ERROR(LOG_SUB_TLS, "tls_connect: called for fd=%d but g_tls_client_ctx is NULL (client TLS not initialised)", fd);
        return NULL;
    }
    SSL *ssl = SSL_new(g_tls_client_ctx);
    if (!ssl) { tls_log_err(LOG_LVL_ERROR, "SSL_new(connect)"); return NULL; }
    if (SSL_set_fd(ssl, fd) != 1) { tls_log_err(LOG_LVL_ERROR, "SSL_set_fd(connect)"); SSL_free(ssl); return NULL; }
    if (server_name && server_name[0]) {
        SSL_set_tlsext_host_name(ssl, server_name);
        SSL_set1_host(ssl, server_name);
    }
    if (SSL_connect(ssl) != 1) {
        tls_log_err(LOG_LVL_WARN, "SSL_connect");
        SSL_free(ssl);
        return NULL;
    }
    return ssl;
}
```

- [ ] **Step 9: Add standalone `LOG_WARN` calls in the macOS cookie read/write functions**

Locate (exact current text):

```c
static int tls_cookie_read_apple(void *cookie, char *buf, int size) {
    int n = SSL_read((SSL *)cookie, buf, size);
    if (n > 0) return n;
    int err = SSL_get_error((SSL *)cookie, n);
    if (err == SSL_ERROR_ZERO_RETURN) return 0;
    if (err == SSL_ERROR_SYSCALL && n == 0) return 0;
    errno = EIO; return -1;
}

static int tls_cookie_write_apple(void *cookie, const char *buf, int size) {
    int n = SSL_write((SSL *)cookie, buf, size);
    if (n > 0) return n;
    errno = EIO; return -1;
}
```

Replace with:

```c
static int tls_cookie_read_apple(void *cookie, char *buf, int size) {
    int n = SSL_read((SSL *)cookie, buf, size);
    if (n > 0) return n;
    int err = SSL_get_error((SSL *)cookie, n);
    if (err == SSL_ERROR_ZERO_RETURN) return 0;
    if (err == SSL_ERROR_SYSCALL && n == 0) return 0;
    LOG_WARN(LOG_SUB_TLS, "tls_cookie_read_apple: SSL_read failed, SSL_get_error=%d", err);
    errno = EIO; return -1;
}

static int tls_cookie_write_apple(void *cookie, const char *buf, int size) {
    int n = SSL_write((SSL *)cookie, buf, size);
    if (n > 0) return n;
    LOG_WARN(LOG_SUB_TLS, "tls_cookie_write_apple: SSL_write failed, n=%d", n);
    errno = EIO; return -1;
}
```

- [ ] **Step 10: Add standalone `LOG_WARN` calls in the Linux/glibc cookie read/write functions**

Locate (exact current text):

```c
static ssize_t tls_cookie_read(void *cookie, char *buf, size_t size) {
    int n = SSL_read((SSL *)cookie, buf, size > INT_MAX ? INT_MAX : (int)size);
    if (n > 0) return n;
    int err = SSL_get_error((SSL *)cookie, n);
    if (err == SSL_ERROR_ZERO_RETURN) return 0;
    if (err == SSL_ERROR_SYSCALL && n == 0) return 0;
    errno = EIO; return -1;
}

static ssize_t tls_cookie_write(void *cookie, const char *buf, size_t size) {
    int n = SSL_write((SSL *)cookie, buf, size > INT_MAX ? INT_MAX : (int)size);
    if (n > 0) return n;
    errno = EIO; return -1;
}
```

Replace with:

```c
static ssize_t tls_cookie_read(void *cookie, char *buf, size_t size) {
    int n = SSL_read((SSL *)cookie, buf, size > INT_MAX ? INT_MAX : (int)size);
    if (n > 0) return n;
    int err = SSL_get_error((SSL *)cookie, n);
    if (err == SSL_ERROR_ZERO_RETURN) return 0;
    if (err == SSL_ERROR_SYSCALL && n == 0) return 0;
    LOG_WARN(LOG_SUB_TLS, "tls_cookie_read: SSL_read failed, SSL_get_error=%d", err);
    errno = EIO; return -1;
}

static ssize_t tls_cookie_write(void *cookie, const char *buf, size_t size) {
    int n = SSL_write((SSL *)cookie, buf, size > INT_MAX ? INT_MAX : (int)size);
    if (n > 0) return n;
    LOG_WARN(LOG_SUB_TLS, "tls_cookie_write: SSL_write failed, n=%d", n);
    errno = EIO; return -1;
}
```

- [ ] **Step 11: Build**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: clean build, no new warnings.

- [ ] **Step 12: Run full test suite, compare to baseline**

Run: `./build/bin/shard-db-test run-all`
Expected: identical pass/fail counts to the pre-Task-1 baseline recorded in
the Global Constraints step.

- [ ] **Step 13: Manual spot-check (two parts — preflight path and deep OpenSSL path)**

**Part A — preflight check (validates Step 1's new `LOG_ERROR` calls):**
Start the daemon in background mode (`./shard-db start`, not `./shard-db
server` foreground) with `TLS_ENABLE=1` in `db.env` pointing `TLS_CERT` at
a nonexistent path, e.g. `TLS_CERT=/nonexistent/cert.pem`. Expected: daemon
fails to start (as before — no behavior change to the client-visible
outcome), AND `$LOG_DIR/<today>-error.log` now contains a line from
`LOG_SUB_TLS` reading `cmd_server: TLS_CERT not readable: /nonexistent/cert.pem (...)`.
Before Step 1, this scenario produced no log line at all — it died at
`fprintf(stderr, ...)` before `log_init()` ever ran; confirm that gap is
now closed.

**Part B — deep OpenSSL path (validates Task 1's `tls_server_init`
refactor in Step 5):** the preflight `access()` check only catches a
missing/unreadable file, not invalid *content* — a bad PEM still passes
`access()` and reaches `tls_server_init`, which is the actual `tls.c` code
this task modifies. Create a readable-but-invalid cert file, e.g.:
```bash
echo "not a real certificate" > /tmp/bad-cert.pem
```
Point `TLS_CERT=/tmp/bad-cert.pem` (with `TLS_KEY` left at a valid key
file) and `./shard-db start` again. Expected: daemon fails to start, AND
`$LOG_DIR/<today>-error.log` contains a line from `LOG_SUB_TLS` reading
`use_certificate_chain_file` followed by whatever detail string OpenSSL's
`ERR_error_string_n` produced for the parse failure (if any) — this is the
line Step 5's refactored `tls_log_err` call now produces (see `tls_log_err`'s
format string: `what` + optional `": " + <openssl detail>`; it never
includes the cert *path* — only the OpenSSL error string, if one was set).
It only reaches disk because Step 1 moved `log_init()` ahead of
`tls_server_init()`.

Restore a valid TLS config (or `TLS_ENABLE=0`) afterward, and remove
`/tmp/bad-cert.pem`.

- [ ] **Step 14: Leave uncommitted**

Do not run `git add`/`git commit`. Move to Task 2.

---

### Task 2: `src/db/server.c` — 20 sites (15 ERROR, 5 WARN)

**Files:**
- Modify: `src/db/server.c`

**Interfaces:** none (additive logging only).

- [ ] **Step 1: `auto_key_normalize` — add `LOG_ERROR` at its 2 malloc-OOM sites**

Locate (exact current text):

```c
static int auto_key_normalize(const Schema *sc, const char *key,
                              char **out_buf, size_t *out_len) {
    if (sc->auto_key == AK_UUID) {
        if (!key || !key[0]) {
            OUT("{\"error\":\"Missing key for auto_key=uuid object\"}\n");
            return -1;
        }
        uint8_t bin[16];
        if (parse_uuid_string(key, bin) < 0) {
            OUT("{\"error\":\"Invalid key for auto_key=uuid: must be 36-char dashed UUID\"}\n");
            return -1;
        }
        char *buf = malloc(16);
        if (!buf) { OUT("{\"error\":\"oom\"}\n"); return -1; }
        memcpy(buf, bin, 16);
        *out_buf = buf; *out_len = 16;
        return 0;
    }
    if (sc->auto_key == AK_SEQ) {
        if (!key || !key[0]) {
            OUT("{\"error\":\"Missing key for auto_key=seq object\"}\n");
            return -1;
        }
        int64_t v;
        if (parse_seq_key(key, &v) < 0) {
            OUT("{\"error\":\"Invalid key for auto_key=seq(...): must be strict decimal int64\"}\n");
            return -1;
        }
        char *buf = malloc(8);
        if (!buf) { OUT("{\"error\":\"oom\"}\n"); return -1; }
        for (int i = 7; i >= 0; i--) {
            buf[i] = (char)(v & 0xFF);
            v >>= 8;
        }
        *out_buf = buf; *out_len = 8;
        return 0;
    }
    /* AK_NONE — verbatim. */
    if (!key) { OUT("{\"error\":\"Missing key\"}\n"); return -1; }
    *out_buf = strdup(key);
    *out_len = strlen(key);
    return 0;
}
```

Replace with:

```c
static int auto_key_normalize(const Schema *sc, const char *key,
                              char **out_buf, size_t *out_len) {
    if (sc->auto_key == AK_UUID) {
        if (!key || !key[0]) {
            OUT("{\"error\":\"Missing key for auto_key=uuid object\"}\n");
            return -1;
        }
        uint8_t bin[16];
        if (parse_uuid_string(key, bin) < 0) {
            OUT("{\"error\":\"Invalid key for auto_key=uuid: must be 36-char dashed UUID\"}\n");
            return -1;
        }
        char *buf = malloc(16);
        if (!buf) {
            LOG_ERROR(LOG_SUB_SERVER, "auto_key_normalize: malloc(16) failed for AK_UUID key");
            OUT("{\"error\":\"oom\"}\n"); return -1;
        }
        memcpy(buf, bin, 16);
        *out_buf = buf; *out_len = 16;
        return 0;
    }
    if (sc->auto_key == AK_SEQ) {
        if (!key || !key[0]) {
            OUT("{\"error\":\"Missing key for auto_key=seq object\"}\n");
            return -1;
        }
        int64_t v;
        if (parse_seq_key(key, &v) < 0) {
            OUT("{\"error\":\"Invalid key for auto_key=seq(...): must be strict decimal int64\"}\n");
            return -1;
        }
        char *buf = malloc(8);
        if (!buf) {
            LOG_ERROR(LOG_SUB_SERVER, "auto_key_normalize: malloc(8) failed for AK_SEQ key");
            OUT("{\"error\":\"oom\"}\n"); return -1;
        }
        for (int i = 7; i >= 0; i--) {
            buf[i] = (char)(v & 0xFF);
            v >>= 8;
        }
        *out_buf = buf; *out_len = 8;
        return 0;
    }
    /* AK_NONE — verbatim. */
    if (!key) { OUT("{\"error\":\"Missing key\"}\n"); return -1; }
    *out_buf = strdup(key);
    *out_len = strlen(key);
    return 0;
}
```

- [ ] **Step 2: `auto_key_generate` — add `LOG_ERROR` at all 5 of its failure sites**

Locate (exact current text):

```c
static int auto_key_generate(const Schema *sc, const char *db_root,
                              const char *object,
                              char **out_buf, size_t *out_len) {
    if (sc->auto_key == AK_UUID) {
        char *buf = malloc(16);
        if (!buf) { OUT("{\"error\":\"oom\"}\n"); return -1; }
        if (gen_uuid4_raw((uint8_t *)buf) != 0) {
            free(buf);
            OUT("{\"error\":\"random source unavailable (uuid key generation failed)\"}\n");
            return -1;
        }
        *out_buf = buf; *out_len = 16;
        return 0;
    }
    if (sc->auto_key == AK_SEQ) {
        long long v = seq_next_val(db_root, object, sc->auto_key_seq_name);
        if (v < 0) {
            OUT("{\"error\":\"sequence_next failed for [%s]\"}\n", sc->auto_key_seq_name);
            return -1;
        }
        char *buf = malloc(8);
        if (!buf) { OUT("{\"error\":\"oom\"}\n"); return -1; }
        int64_t vb = v;
        for (int i = 7; i >= 0; i--) { buf[i] = (char)(vb & 0xFF); vb >>= 8; }
        *out_buf = buf; *out_len = 8;
        return 0;
    }
    OUT("{\"error\":\"auto_key_generate called on AK_NONE object\"}\n");
    return -1;
}
```

Replace with:

```c
static int auto_key_generate(const Schema *sc, const char *db_root,
                              const char *object,
                              char **out_buf, size_t *out_len) {
    if (sc->auto_key == AK_UUID) {
        char *buf = malloc(16);
        if (!buf) {
            LOG_ERROR(LOG_SUB_SERVER, "auto_key_generate: malloc(16) failed for object [%s]", object);
            OUT("{\"error\":\"oom\"}\n"); return -1;
        }
        if (gen_uuid4_raw((uint8_t *)buf) != 0) {
            free(buf);
            LOG_ERROR(LOG_SUB_SERVER, "auto_key_generate: gen_uuid4_raw failed (random source unavailable) for object [%s]", object);
            OUT("{\"error\":\"random source unavailable (uuid key generation failed)\"}\n");
            return -1;
        }
        *out_buf = buf; *out_len = 16;
        return 0;
    }
    if (sc->auto_key == AK_SEQ) {
        long long v = seq_next_val(db_root, object, sc->auto_key_seq_name);
        if (v < 0) {
            LOG_ERROR(LOG_SUB_SERVER, "auto_key_generate: seq_next_val failed for object [%s] seq=[%s]", object, sc->auto_key_seq_name);
            OUT("{\"error\":\"sequence_next failed for [%s]\"}\n", sc->auto_key_seq_name);
            return -1;
        }
        char *buf = malloc(8);
        if (!buf) {
            LOG_ERROR(LOG_SUB_SERVER, "auto_key_generate: malloc(8) failed for object [%s]", object);
            OUT("{\"error\":\"oom\"}\n"); return -1;
        }
        int64_t vb = v;
        for (int i = 7; i >= 0; i--) { buf[i] = (char)(vb & 0xFF); vb >>= 8; }
        *out_buf = buf; *out_len = 8;
        return 0;
    }
    LOG_ERROR(LOG_SUB_SERVER, "auto_key_generate: called on AK_NONE object [%s] (internal invariant violation)", object);
    OUT("{\"error\":\"auto_key_generate called on AK_NONE object\"}\n");
    return -1;
}
```

- [ ] **Step 3: `warmup_object_open` — add `LOG_WARN` when `load_schema` returns invalid splits**

Locate (exact current text):

```c
    /* schema cache populate (load_schema is internally cached) */
    Schema sch = load_schema(eff, obj);
    if (sch.splits <= 0) return NULL;
```

Replace with:

```c
    /* schema cache populate (load_schema is internally cached) */
    Schema sch = load_schema(eff, obj);
    if (sch.splits <= 0) {
        LOG_WARN(LOG_SUB_WARMUP, "warmup_object_open: load_schema returned invalid splits=%d for %s/%s; skipping warmup", sch.splits, dir, obj);
        return NULL;
    }
```

- [ ] **Step 4: `warmup_touch_file` — add `LOG_WARN` on `open()` failure**

Locate (exact current text):

```c
static int warmup_touch_file(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
```

Replace with:

```c
static int warmup_touch_file(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        LOG_WARN(LOG_SUB_WARMUP, "warmup_touch_file: open(%s) failed: errno=%d (%s)", path, errno, strerror(errno));
        return -1;
    }
```

- [ ] **Step 5: `warmup_kf_task_fn` — add `LOG_WARN` when `kfcache_acquire` fails**

Locate (exact current text):

```c
    char kf_path[PATH_MAX];
    slotcask_kf_path(kf_path, sizeof(kf_path), t->sdb->data_dir, t->shard_idx);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, t->sdb->slots_per_shard, 0) != 0) return NULL;
```

Replace with:

```c
    char kf_path[PATH_MAX];
    slotcask_kf_path(kf_path, sizeof(kf_path), t->sdb->data_dir, t->shard_idx);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, t->sdb->slots_per_shard, 0) != 0) {
        LOG_WARN(LOG_SUB_WARMUP, "warmup_kf_task_fn: kfcache_acquire failed for kf_path=%s shard_idx=%d", kf_path, t->shard_idx);
        return NULL;
    }
```

- [ ] **Step 6: `validate_metadata` — add `LOG_ERROR` when the schema-entries `realloc` fails**

Locate (exact current text):

```c
            if (schema_count >= schema_cap) {
                schema_cap = schema_cap ? schema_cap * 2 : 32;
                SchemaEntry *t = realloc(schema_entries,
                                         (size_t)schema_cap * sizeof(SchemaEntry));
                if (!t) { free(schema_entries); fclose(sf); return -1; }
                schema_entries = t;
            }
```

Replace with:

```c
            if (schema_count >= schema_cap) {
                schema_cap = schema_cap ? schema_cap * 2 : 32;
                SchemaEntry *t = realloc(schema_entries,
                                         (size_t)schema_cap * sizeof(SchemaEntry));
                if (!t) {
                    LOG_ERROR(LOG_SUB_SERVER, "validate_metadata: realloc failed growing schema_entries to cap=%d", schema_cap);
                    free(schema_entries); fclose(sf); return -1;
                }
                schema_entries = t;
            }
```

- [ ] **Step 7: `client_connect` — add `LOG_ERROR`/`LOG_WARN` at 3 of its failure sites (leave the `connect()` failure at SKIP-benign, unchanged — its caller already prints a friendly "Cannot connect to port" message)**

Locate (exact current text):

```c
static int client_connect(int port, ClientConn *c) {
    c->fd = -1; c->ssl = NULL;
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) return -1;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(port);
    if (connect(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sfd); return -1;
    }
    c->fd = sfd;
    if (g_db && g_tls_enable) {
        if (ensure_tls_client_ctx() != 0) { close(sfd); c->fd = -1; return -1; }
        const char *server_name = getenv("TLS_SERVER_NAME");
        if (!server_name || !*server_name) server_name = "localhost";
        SSL *ssl = tls_connect(sfd, server_name);
        if (!ssl) { close(sfd); c->fd = -1; return -1; }
        c->ssl = ssl;
    }
    return 0;
}
```

Replace with:

```c
static int client_connect(int port, ClientConn *c) {
    c->fd = -1; c->ssl = NULL;
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) {
        LOG_ERROR(LOG_SUB_SERVER, "client_connect: socket() failed: errno=%d (%s)", errno, strerror(errno));
        return -1;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(port);
    if (connect(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sfd); return -1;
    }
    c->fd = sfd;
    if (g_db && g_tls_enable) {
        if (ensure_tls_client_ctx() != 0) {
            LOG_ERROR(LOG_SUB_SERVER, "client_connect: ensure_tls_client_ctx failed for port=%d", port);
            close(sfd); c->fd = -1; return -1;
        }
        const char *server_name = getenv("TLS_SERVER_NAME");
        if (!server_name || !*server_name) server_name = "localhost";
        SSL *ssl = tls_connect(sfd, server_name);
        if (!ssl) {
            LOG_WARN(LOG_SUB_SERVER, "client_connect: tls_connect failed for port=%d server_name=%s", port, server_name);
            close(sfd); c->fd = -1; return -1;
        }
        c->ssl = ssl;
    }
    return 0;
}
```

- [ ] **Step 8: `write_all` — add `LOG_ERROR` at both failure sites**

Locate (exact current text):

```c
static int write_all(int fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t w = 0;
    while (w < len) {
        ssize_t n = write(fd, p + w, len - w);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        if (n == 0) return -1;
        w += (size_t)n;
    }
    return 0;
}
```

Replace with:

```c
static int write_all(int fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t w = 0;
    while (w < len) {
        ssize_t n = write(fd, p + w, len - w);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR(LOG_SUB_SERVER, "write_all: write(fd=%d) failed: errno=%d (%s)", fd, errno, strerror(errno));
            return -1;
        }
        if (n == 0) {
            LOG_ERROR(LOG_SUB_SERVER, "write_all: write(fd=%d) returned 0 with %zu bytes remaining", fd, len - w);
            return -1;
        }
        w += (size_t)n;
    }
    return 0;
}
```

- [ ] **Step 9: `query_collect` — add `LOG_WARN` on send failure and `LOG_ERROR` at the malloc + both realloc failure sites (leave the `client_connect` failure at the top unchanged — SKIP-benign, root cause already covered by Step 7)**

Locate (exact current text):

```c
static int query_collect(int port, const char *json, size_t json_len, char **out, size_t *out_len) {
    ClientConn cc;
    if (client_connect(port, &cc) != 0) return -1;

    if (client_send_all(&cc, json, json_len) != 0 ||
        client_send_all(&cc, "\n", 1) != 0) {
        client_close(&cc); return -1;
    }

    size_t cap = 8192, len = 0;
    char *buf = malloc(cap);
    if (!buf) { client_close(&cc); return -1; }

    char rbuf[8192];
    ssize_t n;
    while ((n = client_recv(&cc, rbuf, sizeof(rbuf))) > 0) {
        for (ssize_t j = 0; j < n; j++) {
            if (rbuf[j] == '\0') {
                if (len + j > cap) {
                    while (cap < len + j) cap *= 2;
                    char *nb = realloc(buf, cap);
                    if (!nb) { free(buf); client_close(&cc); return -1; }
                    buf = nb;
                }
                memcpy(buf + len, rbuf, j);
                len += j;
                client_close(&cc);
                *out = buf; *out_len = len;
                return 0;
            }
        }
        if (len + (size_t)n > cap) {
            while (cap < len + (size_t)n) cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); client_close(&cc); return -1; }
            buf = nb;
        }
        memcpy(buf + len, rbuf, n);
        len += (size_t)n;
    }
    client_close(&cc);
    /* EOF with no \0 sentinel — still return what we got. */
    *out = buf; *out_len = len;
    return 0;
}
```

Replace with:

```c
static int query_collect(int port, const char *json, size_t json_len, char **out, size_t *out_len) {
    ClientConn cc;
    if (client_connect(port, &cc) != 0) return -1;

    if (client_send_all(&cc, json, json_len) != 0 ||
        client_send_all(&cc, "\n", 1) != 0) {
        LOG_WARN(LOG_SUB_SERVER, "query_collect: client_send_all failed for port=%d json_len=%zu", port, json_len);
        client_close(&cc); return -1;
    }

    size_t cap = 8192, len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        LOG_ERROR(LOG_SUB_SERVER, "query_collect: malloc(%zu) failed", cap);
        client_close(&cc); return -1;
    }

    char rbuf[8192];
    ssize_t n;
    while ((n = client_recv(&cc, rbuf, sizeof(rbuf))) > 0) {
        for (ssize_t j = 0; j < n; j++) {
            if (rbuf[j] == '\0') {
                if (len + j > cap) {
                    while (cap < len + j) cap *= 2;
                    char *nb = realloc(buf, cap);
                    if (!nb) {
                        LOG_ERROR(LOG_SUB_SERVER, "query_collect: realloc(%zu) failed while framing response (len=%zu)", cap, len);
                        free(buf); client_close(&cc); return -1;
                    }
                    buf = nb;
                }
                memcpy(buf + len, rbuf, j);
                len += j;
                client_close(&cc);
                *out = buf; *out_len = len;
                return 0;
            }
        }
        if (len + (size_t)n > cap) {
            while (cap < len + (size_t)n) cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) {
                LOG_ERROR(LOG_SUB_SERVER, "query_collect: realloc(%zu) failed while accumulating response (len=%zu)", cap, len);
                free(buf); client_close(&cc); return -1;
            }
            buf = nb;
        }
        memcpy(buf + len, rbuf, n);
        len += (size_t)n;
    }
    client_close(&cc);
    /* EOF with no \0 sentinel — still return what we got. */
    *out = buf; *out_len = len;
    return 0;
}
```

- [ ] **Step 10: Build**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: clean build, no new warnings.

- [ ] **Step 11: Run full test suite**

Run: `./build/bin/shard-db-test run-all`
Expected: identical pass/fail counts to baseline.

- [ ] **Step 12: Manual spot-check**

Stop the daemon. Pick any existing indexed field's `.idx` shard file under
`<db_root>/<dir>/<obj>/indexes/<field>/*.idx` (create an object with
`add-index` first if none exists) and `chmod 000` it. **Set `WARMUP=sync`
explicitly in `db.env` for this spot-check** — the default is `async`
(`embedded.c`: `warmup_mode` defaults to `"async"`), which runs warmup on a
detached background thread; grepping the log immediately after `./shard-db
start` returns would race that thread and could false-negative before it
has reached the chmod'd file. `WARMUP=sync` makes `cmd_server` block until
warmup finishes before it returns, eliminating the race. Start the daemon
(`./shard-db start`). Expected: the daemon still starts successfully (a
warmup miss is non-fatal, unchanged behavior), AND
`$LOG_DIR/<today>-info.log` contains a `LOG_SUB_WARMUP` `WARN` line from
Step 4 reading `warmup_touch_file: open(<path>) failed: errno=13 (Permission denied)`
naming the exact `.idx` path. (If you'd rather leave `WARMUP=async`, poll
`$LOG_DIR/<today>-info.log` for the pre-existing `LOG_SUB_WARMUP` `WARMUP
done: ...` line — server.c:2822 — before grepping for the WARN line, since
that line marks warmup-thread completion.) `chmod 644` the file back
afterward and restart the daemon to confirm the warmup completes cleanly
again (check `$LOG_DIR/<today>-info.log` for the `LOG_SUB_WARMUP` `WARMUP
done: ...` line that already exists in the codebase today, unaffected by
this task).

- [ ] **Step 13: Leave uncommitted**

Do not run `git add`/`git commit`. Move to Task 3.

---

### Task 3: `src/db/query.c` — 39 sites (21 ERROR, 18 WARN)

**Files:**
- Modify: `src/db/query.c`

**Interfaces:** none (additive logging only). All sites use `LOG_SUB_QUERY`
except the four `slotcask_registry_get` sites, which use `LOG_SUB_SLOTCASK`
(matching the convention already used elsewhere in this codebase for that
specific failure).

- [ ] **Step 1: `collect_hash_cb` — add `LOG_WARN` on buffer-cap exceeded (leave the preceding `collect_cap` early-out unchanged — SKIP-benign, caller-supplied limit)**

Locate (exact current text):

```c
    if (cc->collect_cap > 0 && idx >= (size_t)cc->collect_cap) return -1;
    if (idx >= cc->cap) {
        __atomic_store_n(&cc->budget_exceeded, 1, __ATOMIC_RELAXED);
        return -1;
    }
```

Replace with:

```c
    if (cc->collect_cap > 0 && idx >= (size_t)cc->collect_cap) return -1;
    if (idx >= cc->cap) {
        LOG_WARN(LOG_SUB_QUERY, "collect_hash_cb: query buffer cap exceeded (cap=%zu)", cc->cap);
        __atomic_store_n(&cc->budget_exceeded, 1, __ATOMIC_RELAXED);
        return -1;
    }
```

- [ ] **Step 2: `stream_keyset_cb` — add `LOG_WARN` on keyset-full**

Locate (exact current text):

```c
    if (keyset_insert(sk->ks, hash16) < 0) {
        atomic_store_explicit(&sk->full, 1, memory_order_relaxed);
        return -1; /* keyset full — abort walk; caller falls back. */
    }
```

Replace with:

```c
    if (keyset_insert(sk->ks, hash16) < 0) {
        LOG_WARN(LOG_SUB_QUERY, "stream_keyset_cb: keyset capacity exhausted, aborting walk");
        atomic_store_explicit(&sk->full, 1, memory_order_relaxed);
        return -1; /* keyset full — abort walk; caller falls back. */
    }
```

- [ ] **Step 3: `batch_buf_init` — add `LOG_ERROR` on `calloc` failure**

Locate (exact current text):

```c
    b->pending = calloc(b->pending_cap, 16);
    if (!b->pending) return -1;
```

Replace with:

```c
    b->pending = calloc(b->pending_cap, 16);
    if (!b->pending) {
        LOG_ERROR(LOG_SUB_QUERY, "batch_buf_init: calloc failed for pending_cap=%zu", b->pending_cap);
        return -1;
    }
```

- [ ] **Step 4: `intersect_probe_cb` — add `LOG_WARN` on destination keyset capacity exhausted**

Locate (exact current text):

```c
    if (keyset_contains(p->running, hash16)) {
        if (keyset_insert(p->out, hash16) < 0) return -1;
    }
```

Replace with:

```c
    if (keyset_contains(p->running, hash16)) {
        if (keyset_insert(p->out, hash16) < 0) {
            LOG_WARN(LOG_SUB_QUERY, "intersect_probe_cb: destination keyset capacity exhausted");
            return -1;
        }
    }
```

- [ ] **Step 5: `intersect_collect_cb` — add `LOG_WARN` on keyset overflow**

Locate (exact current text):

```c
    if (query_deadline_tick(c->deadline, &c->dl_counter)) return -1;
    if (keyset_insert(c->ks, hash16) < 0) { c->overflowed = 1; return -1; }
```

Replace with:

```c
    if (query_deadline_tick(c->deadline, &c->dl_counter)) return -1;
    if (keyset_insert(c->ks, hash16) < 0) {
        LOG_WARN(LOG_SUB_QUERY, "intersect_collect_cb: keyset capacity exhausted, marking overflow");
        c->overflowed = 1; return -1;
    }
```

- [ ] **Step 6: `bm_intersect_shard_worker` — add `LOG_ERROR` at the invalid-`n_leaves` invariant check and the `resolve_idx_field` failure on the `OP_EQUAL` pre-encode loop**

Locate (exact current text):

```c
static void *bm_intersect_shard_worker(void *raw) {
    BmIntersectShardArg *a = (BmIntersectShardArg *)raw;
    a->count = 0;
    if (a->n_leaves < 2 || a->n_leaves > MAX_INTERSECT_LEAVES) return NULL;

    /* Pre-encode all leaf values */
    uint8_t  vals[MAX_INTERSECT_LEAVES][1024];
    size_t   vlens[MAX_INTERSECT_LEAVES];
    for (int i = 0; i < a->n_leaves; i++) {
        const TypedField *tf = resolve_idx_field(a->ts, a->leaves[i]->field);
        if (!tf) return NULL;
```

Replace with:

```c
static void *bm_intersect_shard_worker(void *raw) {
    BmIntersectShardArg *a = (BmIntersectShardArg *)raw;
    a->count = 0;
    if (a->n_leaves < 2 || a->n_leaves > MAX_INTERSECT_LEAVES) {
        LOG_ERROR(LOG_SUB_QUERY, "bm_intersect_shard_worker: invalid n_leaves=%d (expected 2..%d) — caller should have validated this", a->n_leaves, MAX_INTERSECT_LEAVES);
        return NULL;
    }

    /* Pre-encode all leaf values */
    uint8_t  vals[MAX_INTERSECT_LEAVES][1024];
    size_t   vlens[MAX_INTERSECT_LEAVES];
    for (int i = 0; i < a->n_leaves; i++) {
        const TypedField *tf = resolve_idx_field(a->ts, a->leaves[i]->field);
        if (!tf) {
            LOG_ERROR(LOG_SUB_QUERY, "bm_intersect_shard_worker: resolve_idx_field failed for already-validated indexed field '%s'", a->leaves[i]->field);
            return NULL;
        }
```

- [ ] **Step 7: `build_keyset_from_bitmap` (generic dict-scan path, `leaf->op != OP_EQUAL && leaf->op != OP_IN` branch) — add `LOG_WARN`/`LOG_ERROR` at its 3 flagged sites**

Locate (exact current text):

```c
        SlotcaskDb *sdb_g = slotcask_registry_get(db_root, object, &info_g);
        if (!sdb_g) return NULL;

        size_t tot_g = bm_popcount_generic_for_crit(db_root, object,
                                                     leaf->field, splits,
                                                     (SearchCriterion *)leaf, tf);

        size_t ks_bytes_est_g = (tot_g ? tot_g : 1024)
                                * 2 * (sizeof(uint8_t[16]) + sizeof(uint32_t));
        if (ks_bytes_est_g > g_query_buffer_max_bytes) return NULL;

        KeySet *ks_g = keyset_new(tot_g > 0 ? tot_g : 1024);
        if (!ks_g) return NULL;
```

Replace with:

```c
        SlotcaskDb *sdb_g = slotcask_registry_get(db_root, object, &info_g);
        if (!sdb_g) {
            LOG_WARN(LOG_SUB_SLOTCASK, "build_keyset_from_bitmap: slotcask_registry_get failed for %s/%s", db_root, object);
            return NULL;
        }

        size_t tot_g = bm_popcount_generic_for_crit(db_root, object,
                                                     leaf->field, splits,
                                                     (SearchCriterion *)leaf, tf);

        size_t ks_bytes_est_g = (tot_g ? tot_g : 1024)
                                * 2 * (sizeof(uint8_t[16]) + sizeof(uint32_t));
        if (ks_bytes_est_g > g_query_buffer_max_bytes) {
            LOG_WARN(LOG_SUB_QUERY, "build_keyset_from_bitmap: estimated keyset size %zu exceeds query buffer cap %zu", ks_bytes_est_g, g_query_buffer_max_bytes);
            return NULL;
        }

        KeySet *ks_g = keyset_new(tot_g > 0 ? tot_g : 1024);
        if (!ks_g) {
            LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_bitmap: keyset_new(%zu) failed", tot_g > 0 ? tot_g : 1024);
            return NULL;
        }
```

- [ ] **Step 8: `build_keyset_from_bitmap` (eq/IN path) — add `LOG_ERROR` on the `vals`/`vlens` calloc failure**

Locate (exact current text):

```c
    uint8_t (*vals)[1024] = calloc((size_t)n_vals, sizeof(*vals));
    size_t *vlens = calloc((size_t)n_vals, sizeof(*vlens));
    if (!vals || !vlens) { free(vals); free(vlens); return NULL; }
```

Replace with:

```c
    uint8_t (*vals)[1024] = calloc((size_t)n_vals, sizeof(*vals));
    size_t *vlens = calloc((size_t)n_vals, sizeof(*vlens));
    if (!vals || !vlens) {
        LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_bitmap: calloc failed for n_vals=%d", n_vals);
        free(vals); free(vlens); return NULL;
    }
```

- [ ] **Step 9: `build_keyset_from_bitmap` (eq/IN path) — add `LOG_WARN` on `slotcask_registry_get` failure**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { free(vals); free(vlens); return NULL; }

    /* Pass A: sum bm_count across every (shard × value). Each bm_count
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "build_keyset_from_bitmap: slotcask_registry_get failed for %s/%s", db_root, object);
        free(vals); free(vlens); return NULL;
    }

    /* Pass A: sum bm_count across every (shard × value). Each bm_count
```

- [ ] **Step 10: `build_keyset_from_bitmap` (eq/IN path) — add `LOG_WARN`/`LOG_ERROR` on the budget check and `keyset_new` failure**

Locate (exact current text):

```c
    if (ks_bytes_est > g_query_buffer_max_bytes) { free(vals); free(vlens); return NULL; }

    KeySet *ks = keyset_new(total_matches > 0 ? total_matches : 1024);
    if (!ks) { free(vals); free(vlens); return NULL; }
```

Replace with:

```c
    if (ks_bytes_est > g_query_buffer_max_bytes) {
        LOG_WARN(LOG_SUB_QUERY, "build_keyset_from_bitmap: estimated keyset size %zu exceeds query buffer cap %zu", ks_bytes_est, g_query_buffer_max_bytes);
        free(vals); free(vlens); return NULL;
    }

    KeySet *ks = keyset_new(total_matches > 0 ? total_matches : 1024);
    if (!ks) {
        LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_bitmap: keyset_new(%zu) failed", total_matches > 0 ? total_matches : 1024);
        free(vals); free(vlens); return NULL;
    }
```

- [ ] **Step 11: `build_keyset_bitmap_complement` — add `LOG_WARN`/`LOG_ERROR` at its 3 flagged sites**

Locate (exact current text):

```c
    if (ks_bytes_est > g_query_buffer_max_bytes) return NULL;

    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return NULL;
```

Replace with:

```c
    if (ks_bytes_est > g_query_buffer_max_bytes) {
        LOG_WARN(LOG_SUB_QUERY, "build_keyset_bitmap_complement: estimated keyset size %zu exceeds query buffer cap %zu", ks_bytes_est, g_query_buffer_max_bytes);
        return NULL;
    }

    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "build_keyset_bitmap_complement: slotcask_registry_get failed for %s/%s", db_root, object);
        return NULL;
    }
```

Locate (exact current text):

```c
    KeySet *ks = keyset_new(comp_hint > 0 ? comp_hint : 1024);
    if (!ks) return NULL;
```

Replace with:

```c
    KeySet *ks = keyset_new(comp_hint > 0 ? comp_hint : 1024);
    if (!ks) {
        LOG_ERROR(LOG_SUB_QUERY, "build_keyset_bitmap_complement: keyset_new(%zu) failed", comp_hint > 0 ? comp_hint : 1024);
        return NULL;
    }
```

- [ ] **Step 12: `build_smaller_bitmap_keyset` — add `LOG_ERROR` on the `tvals`/`tvlens` calloc failure**

Locate (exact current text):

```c
    uint8_t (*tvals)[1024] = calloc((size_t)n_vals, sizeof(*tvals));
    size_t *tvlens = calloc((size_t)n_vals, sizeof(*tvlens));
    if (!tvals || !tvlens) { free(tvals); free(tvlens); return NULL; }
```

Replace with:

```c
    uint8_t (*tvals)[1024] = calloc((size_t)n_vals, sizeof(*tvals));
    size_t *tvlens = calloc((size_t)n_vals, sizeof(*tvlens));
    if (!tvals || !tvlens) {
        LOG_ERROR(LOG_SUB_QUERY, "build_smaller_bitmap_keyset: calloc failed for n_vals=%d", n_vals);
        free(tvals); free(tvlens); return NULL;
    }
```

- [ ] **Step 13: `keyset_pairwise_intersect` — add `LOG_ERROR` on `keyset_new` failure**

Locate (exact current text):

```c
    KeySet *out = keyset_new(keyset_size(small));
    if (!out) return NULL;
```

Replace with:

```c
    KeySet *out = keyset_new(keyset_size(small));
    if (!out) {
        LOG_ERROR(LOG_SUB_QUERY, "keyset_pairwise_intersect: keyset_new(%zu) failed", keyset_size(small));
        return NULL;
    }
```

- [ ] **Step 14: `tg_intersect_streaming` — add `LOG_ERROR` on `keyset_new` failure**

Locate (exact current text):

```c
    KeySet *next = keyset_new(keyset_size(running));
    if (!next) { keyset_free(running); return NULL; }
```

Replace with:

```c
    KeySet *next = keyset_new(keyset_size(running));
    if (!next) {
        LOG_ERROR(LOG_SUB_QUERY, "tg_intersect_streaming: keyset_new(%zu) failed", keyset_size(running));
        keyset_free(running); return NULL;
    }
```

- [ ] **Step 15: `build_keyset_from_trigram` — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
    TgEntry *order = malloc(n * sizeof(TgEntry));
    if (!order) return NULL;
```

Replace with:

```c
    TgEntry *order = malloc(n * sizeof(TgEntry));
    if (!order) {
        LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_trigram: malloc failed for n=%d TgEntry", n);
        return NULL;
    }
```

Locate (exact current text):

```c
    KeySet *acc = keyset_new(order[0].count);
    if (!acc) { free(order); return NULL; }
```

Replace with:

```c
    KeySet *acc = keyset_new(order[0].count);
    if (!acc) {
        LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_trigram: keyset_new(%zu) failed", (size_t)order[0].count);
        free(order); return NULL;
    }
```

- [ ] **Step 16: `build_keyset_from_leaf` (tiered allocation) — add `LOG_ERROR` on `keyset_new` failure**

Locate (exact current text):

```c
        KeySet *ks = keyset_new(hints[t]);
        if (!ks) return NULL;
```

Replace with:

```c
        KeySet *ks = keyset_new(hints[t]);
        if (!ks) {
            LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_leaf: keyset_new(%zu) failed (tier %d)", hints[t], t);
            return NULL;
        }
```

- [ ] **Step 17: `intersect_all_cb` — add `LOG_WARN` on keyset capacity exhausted**

Locate (exact current text):

```c
    if (keyset_insert(ic->out, hash) < 0) return -1;
```

Replace with:

```c
    if (keyset_insert(ic->out, hash) < 0) {
        LOG_WARN(LOG_SUB_QUERY, "intersect_all_cb: destination keyset capacity exhausted");
        return -1;
    }
```

- [ ] **Step 18: `intersect_indexed_leaves` — add `LOG_ERROR` at its 3 flagged sites**

Locate (exact current text):

```c
    KeySet **per_leaf = calloc((size_t)n, sizeof(KeySet *));
    LeafBuildArg *args = calloc((size_t)n, sizeof(LeafBuildArg));
    if (!per_leaf || !args) { free(per_leaf); free(args); return NULL; }
```

Replace with:

```c
    KeySet **per_leaf = calloc((size_t)n, sizeof(KeySet *));
    LeafBuildArg *args = calloc((size_t)n, sizeof(LeafBuildArg));
    if (!per_leaf || !args) {
        LOG_ERROR(LOG_SUB_QUERY, "intersect_indexed_leaves: calloc failed for n=%d leaves", n);
        free(per_leaf); free(args); return NULL;
    }
```

Locate (exact current text):

```c
    KeySet *result = keyset_new(keyset_size(per_leaf[smallest_i]));
    if (!result) {
        for (int i = 0; i < n; i++) keyset_free(per_leaf[i]);
        free(per_leaf); free(args); return NULL;
    }
```

Replace with:

```c
    KeySet *result = keyset_new(keyset_size(per_leaf[smallest_i]));
    if (!result) {
        LOG_ERROR(LOG_SUB_QUERY, "intersect_indexed_leaves: keyset_new(%zu) failed", keyset_size(per_leaf[smallest_i]));
        for (int i = 0; i < n; i++) keyset_free(per_leaf[i]);
        free(per_leaf); free(args); return NULL;
    }
```

Locate (exact current text):

```c
        KeySet *next = keyset_new(cur);
        if (!next) { keyset_free(running); return NULL; }
```

Replace with:

```c
        KeySet *next = keyset_new(cur);
        if (!next) {
            LOG_ERROR(LOG_SUB_QUERY, "intersect_indexed_leaves: keyset_new(%zu) failed (serial path)", cur);
            keyset_free(running); return NULL;
        }
```

> **Note for the executing model:** the exact free-list in the `per_leaf`/`args` and `result` blocks above must match what's currently in `src/db/query.c` at those two sites — copy the surrounding loop/cleanup code verbatim from the live file if it differs from what's shown here (e.g. loop variable name), and only insert the `LOG_ERROR` call plus braces. Do not change any existing cleanup logic.

- [ ] **Step 19: `keyset_to_batch_cb` — add `LOG_ERROR` on the should-never-happen capacity check**

Locate (exact current text):

```c
    if (kc->count >= kc->cap) return -1; /* shouldn't happen */
```

Replace with:

```c
    if (kc->count >= kc->cap) {
        LOG_ERROR(LOG_SUB_QUERY, "keyset_to_batch_cb: count >= cap (%zu) — internal invariant violation", kc->cap);
        return -1; /* shouldn't happen */
    }
```

- [ ] **Step 20: `keyset_to_collected_hashes` — add `LOG_ERROR` on `malloc` failure**

Locate (exact current text):

```c
    CollectedHash *entries = malloc(cap * sizeof(CollectedHash));
    if (!entries) return -1;
```

Replace with:

```c
    CollectedHash *entries = malloc(cap * sizeof(CollectedHash));
    if (!entries) {
        LOG_ERROR(LOG_SUB_QUERY, "keyset_to_collected_hashes: malloc failed for cap=%zu", cap);
        return -1;
    }
```

- [ ] **Step 21: `or_collect_cb` — add `LOG_WARN` on keyset capacity exhausted**

Locate (exact current text):

```c
    if (keyset_insert(w->ks, hash16) < 0) return -1;
```

Replace with:

```c
    if (keyset_insert(w->ks, hash16) < 0) {
        LOG_WARN(LOG_SUB_QUERY, "or_collect_cb: destination keyset capacity exhausted");
        return -1;
    }
```

> **Note for the executing model:** `keyset_insert(sk->ks, ...)` (Step 2), `keyset_insert(p->out, ...)` (Step 4), `keyset_insert(c->ks, ...)` (Step 5), `keyset_insert(ic->out, ...)` (Step 17), and `keyset_insert(w->ks, ...)` (this step) are five textually-similar but distinct call sites in five different functions. Apply each edit only inside the named function — use `grep -n` to confirm you are editing the correct occurrence if the surrounding function isn't obvious from context.

- [ ] **Step 22: `build_or_keyset` — add `LOG_WARN`/`LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
    if (ks_bytes > g_query_buffer_max_bytes) {
        if (out_budget_exceeded) *out_budget_exceeded = 1;
        return NULL;
    }
```

Replace with:

```c
    if (ks_bytes > g_query_buffer_max_bytes) {
        LOG_WARN(LOG_SUB_QUERY, "build_or_keyset: estimated keyset size %zu exceeds query buffer cap %zu", ks_bytes, g_query_buffer_max_bytes);
        if (out_budget_exceeded) *out_budget_exceeded = 1;
        return NULL;
    }
```

Locate (exact current text):

```c
    KeySet *ks = keyset_new(est_total);
    if (!ks) return NULL;
```

Replace with:

```c
    KeySet *ks = keyset_new(est_total);
    if (!ks) {
        LOG_ERROR(LOG_SUB_QUERY, "build_or_keyset: keyset_new(%zu) failed", est_total);
        return NULL;
    }
```

> **Note for the executing model:** `keyset_new(...)` failure checks appear at many sites across this file (Steps 7, 10, 11, 13, 14, 15, 16, 18, 22). Each is textually similar (`KeySet *X = keyset_new(Y); if (!X) ...`) but in a different function with different variable names and cleanup requirements — apply each edit only at the specific site identified by its surrounding function name, never with a blind find-and-replace-all.

- [ ] **Step 23: `cmd_count` — add `LOG_WARN` on query buffer cap exceeded**

Locate (exact current text):

```c
    if (cc.budget_exceeded) {
        OUT(QUERY_BUFFER_ERR);
        collect_ctx_destroy(&cc);
        return -1;
    }
```

Replace with:

```c
    if (cc.budget_exceeded) {
        LOG_WARN(LOG_SUB_QUERY, "cmd_count: query buffer cap exceeded");
        OUT(QUERY_BUFFER_ERR);
        collect_ctx_destroy(&cc);
        return -1;
    }
```

- [ ] **Step 24: `d2_build_hash_map` — add `LOG_ERROR` on `malloc` failure**

Locate (exact current text):

```c
    D2HashIdxEntry *map = malloc(n * sizeof(D2HashIdxEntry));
    if (!map) return NULL;
```

Replace with:

```c
    D2HashIdxEntry *map = malloc(n * sizeof(D2HashIdxEntry));
    if (!map) {
        LOG_ERROR(LOG_SUB_QUERY, "d2_build_hash_map: malloc failed for n=%d entries", n);
        return NULL;
    }
```

- [ ] **Step 25: `cursor_fetch_worker` — add `LOG_WARN`/`LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(ctx->db_root, ctx->object, &info);
    if (!sdb) return NULL;
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(ctx->db_root, ctx->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "cursor_fetch_worker: slotcask_registry_get failed for %s/%s", ctx->db_root, ctx->object);
        return NULL;
    }
```

Locate (exact current text):

```c
    uint8_t (*hashes)[16] = malloc((size_t)ctx->entry_count * sizeof(*hashes));
    if (!hashes) return NULL;
```

Replace with:

```c
    uint8_t (*hashes)[16] = malloc((size_t)ctx->entry_count * sizeof(*hashes));
    if (!hashes) {
        LOG_ERROR(LOG_SUB_QUERY, "cursor_fetch_worker: malloc failed for entry_count=%d", ctx->entry_count);
        return NULL;
    }
```

- [ ] **Step 26: `cmd_find_do` (ordered-sort path) — add `LOG_WARN` on query buffer cap exceeded**

Locate (exact current text): copy the full `if (oc.budget_exceeded) { ... }` block verbatim from `src/db/query.c` around line 7856 — it frees each row in `oc.rows`, frees `oc.rows` itself, destroys `oc.lock`, emits `QUERY_BUFFER_ERR`, frees `excluded` and `joins`, and returns `-1`. Insert one line as the first statement inside that `if` block:

```c
        LOG_WARN(LOG_SUB_QUERY, "cmd_find_do: query buffer cap exceeded during ordered sort/fetch (object=%s)", object);
```

immediately after the opening `{` of `if (oc.budget_exceeded) {`, before the existing cleanup statements. Do not alter any of the existing cleanup/free logic.

- [ ] **Step 27: `cmd_find_do` (`rc == -2` path) — add `LOG_WARN` on query deadline/buffer failure**

Locate (exact current text): copy the full `if (rc == -2) { ... }` block verbatim from `src/db/query.c` around line 8099 — it frees `excluded` and `joins`, emits `QUERY_BUFFER_ERR`, and returns `-1`. Insert one line as the first statement inside that `if` block:

```c
        LOG_WARN(LOG_SUB_QUERY, "cmd_find_do: scan_shards returned rc=-2 (query buffer cap exceeded, object=%s)", object);
```

immediately after the opening `{` of `if (rc == -2) {`, before the existing cleanup statements. Do not alter any of the existing cleanup/free logic.

- [ ] **Step 28: `cmd_find_do` (FP_UNION path) — add `LOG_WARN` on query buffer cap exceeded**

Locate (exact current text): copy the full `if (budget_exceeded) { ... }` block verbatim from `src/db/query.c` around line 8118 — it frees `excluded` and `joins`, emits `QUERY_BUFFER_ERR`, and returns `-1`. Insert one line as the first statement inside that `if` block:

```c
        LOG_WARN(LOG_SUB_QUERY, "cmd_find_do: query buffer cap exceeded building FP_UNION keyset (object=%s)", object);
```

immediately after the opening `{` of `if (budget_exceeded) {`, before the existing cleanup statements. Do not alter any of the existing cleanup/free logic.

- [ ] **Step 29: Build**

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: clean build, no new warnings.

- [ ] **Step 30: Full suite**

```bash
./build/bin/shard-db-test run-all
```

Expected: identical pass/fail counts to the Task 1 baseline — no regressions, no new failures.

- [ ] **Step 31: Manual spot-check**

Pick one reachable site — e.g. force a query-buffer-cap breach by setting a very low `QUERY_BUFFER_MB` (e.g. `QUERY_BUFFER_MB=1`) in `db.env`, restart the daemon, and run a `find`/`count` against an object with enough rows to exceed 1 MB of intermediate buffer. Confirm the client receives the existing `QUERY_BUFFER_ERR` response AND a matching `LOG_WARN` line appears in `$LOG_DIR` (e.g. `cmd_count: query buffer cap exceeded` or `build_or_keyset: estimated keyset size ... exceeds query buffer cap ...`). Restore `QUERY_BUFFER_MB` to its original value and restart the daemon afterward.

- [ ] **Step 32: Leave uncommitted**

Do not run `git add`/`git commit`. Move to Task 4.

---

### Task 4: `src/db/query_schema.c` + `src/db/query_bulk.c` + `src/db/query_find.c` — 29 sites (19 ERROR, 10 WARN)

**Files:**
- Modify: `src/db/query_schema.c`
- Modify: `src/db/query_bulk.c`
- Modify: `src/db/query_find.c`

**Interfaces:** none (additive logging only). Subsystem tags: `LOG_SUB_CONFIG`
for `schema.conf`/`fields.conf` rewrite failures (matches the tag already
used for config-file I/O elsewhere in this codebase), `LOG_SUB_QUERY` for
allocation/keyset/deadline sites, `LOG_SUB_SLOTCASK` for
`slotcask_registry_get` failures.

#### query_schema.c (2 sites)

- [ ] **Step 1: `rewrite_fields_conf_for_edit` — add `LOG_ERROR` on both `fopen` failures and the `rename` failure**

Locate (exact current text):

```c
    FILE *fin = fopen(fpath, "r");
    if (!fin) return -1;
```

Replace with:

```c
    FILE *fin = fopen(fpath, "r");
    if (!fin) {
        LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: fopen %s failed: %s", fpath, strerror(errno));
        return -1;
    }
```

> **Note for the executing model:** if `src/db/query_schema.c` has a second, separate `fopen(fpath_new, "w")` failure check near this one (per the audit, both were originally flagged at the same site pair), apply the equivalent `LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: fopen %s failed: %s", fpath_new, strerror(errno));` pattern there too — confirm the exact variable name (`fpath_new` vs. `fout_path` etc.) against the live source before editing.

Locate (exact current text):

```c
    if (rename(fpath_new, fpath) != 0) {
```

Replace with:

```c
    if (rename(fpath_new, fpath) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: rename %s -> %s failed: %s", fpath_new, fpath, strerror(errno));
```

> **Note for the executing model:** insert the `LOG_ERROR` as the first statement inside the existing `if (rename(...) != 0) { ... }` block — do not alter any of the existing cleanup/error-response logic that follows it in that block.

#### query_bulk.c (19 sites: 11 ERROR, 8 WARN)

- [ ] **Step 2: `idx_build_field_worker` — add `LOG_ERROR` at its 2 flagged alloc-failure sites**

Locate (exact current text): the 3-way `counts`/`offsets`/`parted` allocation-failure check inside `idx_build_field_worker` (around line 57 per the audit). Copy the exact current `if (!counts || !offsets || !parted) { ... return -1; }` (or equivalent free/cleanup) block verbatim from `src/db/query_bulk.c`, and insert as its first statement:

```c
        LOG_ERROR(LOG_SUB_QUERY, "idx_build_field_worker: alloc failed for field %s (new_count=%zu)", fa->field, fa->new_count);
```

Locate (exact current text): the `calloc(idx_n, sizeof(size_t))` failure check for `cursor` (around line 64). Copy the exact current `if (!cursor) { ... return -1; }` block verbatim, and insert as its first statement:

```c
        LOG_ERROR(LOG_SUB_QUERY, "idx_build_field_worker: calloc cursor failed (idx_n=%d)", idx_n);
```

- [ ] **Step 3: `arena_new` — add `LOG_ERROR` at its 2 flagged alloc-failure sites**

Locate (exact current text):

```c
    BulkArena *a = malloc(sizeof(BulkArena));
    if (!a) return NULL;
```

Replace with:

```c
    BulkArena *a = malloc(sizeof(BulkArena));
    if (!a) {
        LOG_ERROR(LOG_SUB_QUERY, "arena_new: malloc BulkArena header failed");
        return NULL;
    }
```

Locate (exact current text): the `malloc(cap)` failure check for the arena base buffer (around line 159). Copy the exact current `if (!a->base) { ... return NULL; }` (or equivalent, possibly freeing `a` first) block verbatim, and insert as its first statement:

```c
        LOG_ERROR(LOG_SUB_QUERY, "arena_new: malloc %zu bytes failed", cap);
```

- [ ] **Step 4: `build_shard_worker_map` — add `LOG_ERROR` on alloc failure**

Locate (exact current text): the `worker_shards`/`s2w` allocation-failure check (around line 293). Copy the exact current `if (...) { ... return ...; }` block verbatim from `src/db/query_bulk.c`, and insert as its first statement:

```c
    LOG_ERROR(LOG_SUB_QUERY, "build_shard_worker_map: malloc failed (splits=%d, nw=%d)", splits, nw);
```

- [ ] **Step 5: `bulk_insert_shard_worker_v2` — add `LOG_ERROR` on the 6-way alloc failure that silently drops records**

Locate (exact current text): the 6-way `batch`/`ctxs`/`kf_shards`/`counts`/`offsets`/`cursors` allocation-failure check (around line 611) that increments `sw->errors` without logging. Copy the exact current failure block verbatim, and insert as its first statement:

```c
        LOG_ERROR(LOG_SUB_QUERY, "bulk_insert_shard_worker_v2: alloc failed, dropping %zu records for shard=%d", sw->count, sw->shard_id);
```

> **Note for the executing model:** this function already has a good-pattern example immediately above at the `slotcask_registry_get` failure site (around line 599) — `LOG_ERROR(LOG_SUB_SLOTCASK, "INSERT_DROP shard=%d ...")`. That site is ALREADY-LOGGED; do not duplicate or modify it. Only the 6-way alloc-failure block below it needs the new `LOG_ERROR` call.

- [ ] **Step 6: `bulk_del_shard_worker_v2` — add `LOG_WARN` on `slotcask_registry_get` failure and `LOG_ERROR` on the alloc failure**

Locate (exact current text): the `slotcask_registry_get` failure check (around line 2266). Copy the exact current `if (!sdb) { ... }` block verbatim from `src/db/query_bulk.c`, and insert as its first statement:

```c
        LOG_WARN(LOG_SUB_SLOTCASK, "bulk_del_shard_worker_v2: slotcask_registry_get failed for %s/%s", sw->db_root, sw->object);
```

Locate (exact current text): the `batch`/`ctxs` allocation-failure check (around line 2274) that drops `sw->key_count` deletes with no error counter even incremented. Copy the exact current failure block verbatim, and insert as its first statement:

```c
        LOG_ERROR(LOG_SUB_QUERY, "bulk_del_shard_worker_v2: alloc failed, dropping %d deletes", sw->key_count);
```

- [ ] **Step 7: `bulk_upd_shard_worker_v2` — add `LOG_WARN` on `slotcask_registry_get` failure and `LOG_ERROR` on the alloc failure**

Locate (exact current text): the `slotcask_registry_get` failure check (around line 2817). Copy the exact current `if (!sdb) { ... }` block verbatim from `src/db/query_bulk.c`, and insert as its first statement:

```c
        LOG_WARN(LOG_SUB_SLOTCASK, "bulk_upd_shard_worker_v2: slotcask_registry_get failed for %s/%s", w->db_root, w->object);
```

Locate (exact current text): the `batch`/`ctxs`/`scratch` allocation-failure check (around line 2829) that sets `w->skipped += w->count`. Copy the exact current failure block verbatim, and insert as its first statement:

```c
        LOG_ERROR(LOG_SUB_QUERY, "bulk_upd_shard_worker_v2: alloc failed, skipping %d updates", w->count);
```

- [ ] **Step 8: Bulk-update orchestrator (post-scan) — add `LOG_WARN` on deadline timeout and query buffer cap exceeded**

Locate (exact current text):

```c
    if (dl.timed_out) {
        OUT("{\"error\":\"query_timeout\"}\n");
```

Replace with:

```c
    if (dl.timed_out) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-update: query deadline exceeded while matching criteria");
        OUT("{\"error\":\"query_timeout\"}\n");
```

Locate (exact current text): the adjacent `ctx.budget_exceeded` check (around line 2939) that emits `QUERY_BUFFER_ERR`. Copy the exact current `if (ctx.budget_exceeded) { ... }` block verbatim from `src/db/query_bulk.c`, and insert as its first statement:

```c
        LOG_WARN(LOG_SUB_QUERY, "bulk-update: query buffer cap exceeded while matching criteria");
```

- [ ] **Step 9: `bulk_upd_delim_shard_worker_v2` — add `LOG_WARN` on `slotcask_registry_get` failure and `LOG_ERROR` on the alloc failure**

Locate (exact current text): the `slotcask_registry_get` failure check (around line 3232). Copy the exact current `if (!sdb) { ... }` block verbatim from `src/db/query_bulk.c`, and insert as its first statement:

```c
        LOG_WARN(LOG_SUB_SLOTCASK, "bulk_upd_delim_shard_worker_v2: slotcask_registry_get failed for %s/%s", w->db_root, w->object);
```

Locate (exact current text): the 3-way allocation-failure check (around line 3239, CSV-delimited bulk update) that sets `w->skipped += w->count`. Copy the exact current failure block verbatim, and insert as its first statement:

```c
        LOG_ERROR(LOG_SUB_QUERY, "bulk_upd_delim_shard_worker_v2: alloc failed, skipping %d updates", w->count);
```

- [ ] **Step 10: `bulk_upd_json_shard_worker_v2` — add `LOG_WARN` on `slotcask_registry_get` failure and `LOG_ERROR` on the alloc failure**

Locate (exact current text): the `slotcask_registry_get` failure check (around line 3694). Copy the exact current `if (!sdb) { ... }` block verbatim from `src/db/query_bulk.c`, and insert as its first statement:

```c
        LOG_WARN(LOG_SUB_SLOTCASK, "bulk_upd_json_shard_worker_v2: slotcask_registry_get failed for %s/%s", w->db_root, w->object);
```

Locate (exact current text): the 3-way allocation-failure check (around line 3699, JSON-patch bulk update) that sets `w->skipped += w->count`. Copy the exact current failure block verbatim, and insert as its first statement:

```c
        LOG_ERROR(LOG_SUB_QUERY, "bulk_upd_json_shard_worker_v2: alloc failed, skipping %d updates", w->count);
```

- [ ] **Step 11: Bulk-delete-by-criteria orchestrator — add `LOG_WARN` on deadline timeout and query buffer cap exceeded**

Locate (exact current text):

```c
    if (dl.timed_out) {
        OUT("{\"error\":\"query_timeout\"}\n");
```

Replace with:

```c
    if (dl.timed_out) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-delete: query deadline exceeded while matching criteria");
        OUT("{\"error\":\"query_timeout\"}\n");
```

> **Note for the executing model:** `query_bulk.c` has TWO textually-identical `if (dl.timed_out) { OUT("{\"error\":\"query_timeout\"}\n"); ...` blocks — one in the bulk-update orchestrator (Step 8, around line 2932) and one here in the bulk-delete-by-criteria orchestrator (around line 4318). Apply the correct message text (`"bulk-update: ..."` vs. `"bulk-delete: ..."`) to each, and confirm via surrounding function context (which orchestrator you're in) before editing — do not use a blind find-and-replace-all across the file.

Locate (exact current text): the adjacent `ctx.budget_exceeded` check (around line 4325) that emits `QUERY_BUFFER_ERR`. Copy the exact current `if (ctx.budget_exceeded) { ... }` block verbatim from `src/db/query_bulk.c`, and insert as its first statement:

```c
        LOG_WARN(LOG_SUB_QUERY, "bulk-delete: query buffer cap exceeded while matching criteria");
```

- [ ] **Step 12: `bulk_del_crit_shard_worker` — add `LOG_ERROR` on the alloc failure**

Locate (exact current text): the `batch`/`ctxs` allocation-failure check (around line 4178) that sets `w->skipped = w->count`. Copy the exact current failure block verbatim from `src/db/query_bulk.c`, and insert as its first statement:

```c
        LOG_ERROR(LOG_SUB_QUERY, "bulk_del_crit_shard_worker: alloc failed, skipping %d deletes", w->count);
```

#### query_find.c (8 sites: 6 ERROR, 2 WARN)

- [ ] **Step 13: `json_escape_field` — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
char *json_escape_field(char *v) {
    if (!v) return NULL;
    size_t len = strlen(v);
    char *esc = malloc(len * 6 + 1);
    if (!esc) { free(v); return NULL; }
    int n = json_escape_into(esc, len * 6 + 1, v, len);
    free(v);
    if (n < 0) { free(esc); return NULL; }
    esc[n] = '\0';
    return esc;
}
```

Replace with:

```c
char *json_escape_field(char *v) {
    if (!v) return NULL;
    size_t len = strlen(v);
    char *esc = malloc(len * 6 + 1);
    if (!esc) {
        LOG_ERROR(LOG_SUB_QUERY, "json_escape_field: malloc %zu bytes failed", len * 6 + 1);
        free(v); return NULL;
    }
    int n = json_escape_into(esc, len * 6 + 1, v, len);
    free(v);
    if (n < 0) {
        LOG_ERROR(LOG_SUB_QUERY, "json_escape_field: json_escape_into overflowed a %zu-byte buffer", len * 6 + 1);
        free(esc); return NULL;
    }
    esc[n] = '\0';
    return esc;
}
```

- [ ] **Step 14: `json_escape_const` — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
char *json_escape_const(const char *v) {
    if (!v) return NULL;
    size_t len = strlen(v);
    char *esc = malloc(len * 6 + 1);
    if (!esc) return NULL;
    int n = json_escape_into(esc, len * 6 + 1, v, len);
    if (n < 0) { free(esc); return NULL; }
```

Replace with:

```c
char *json_escape_const(const char *v) {
    if (!v) return NULL;
    size_t len = strlen(v);
    char *esc = malloc(len * 6 + 1);
    if (!esc) {
        LOG_ERROR(LOG_SUB_QUERY, "json_escape_const: malloc %zu bytes failed", len * 6 + 1);
        return NULL;
    }
    int n = json_escape_into(esc, len * 6 + 1, v, len);
    if (n < 0) {
        LOG_ERROR(LOG_SUB_QUERY, "json_escape_const: json_escape_into overflowed a %zu-byte buffer", len * 6 + 1);
        free(esc); return NULL;
    }
```

- [ ] **Step 15: `scan_dispatch` — add `LOG_WARN` on `slotcask_registry_get` failure**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return -1;
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "scan_dispatch: slotcask_registry_get failed for %s/%s", db_root, object);
        return -1;
    }
```

- [ ] **Step 16: `read_record_ref` — add `LOG_WARN` on `slotcask_registry_get` failure**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return -1;
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "read_record_ref: slotcask_registry_get failed for %s/%s", db_root, object);
        return -1;
    }
```

> **Note for the executing model:** Steps 15 and 16 are textually-identical `if (!sdb) return -1;` sites in two different functions (`scan_dispatch` around line 442, `read_record_ref` around line 487). Confirm which function you're in from surrounding context before editing — do not blind find-and-replace-all.

- [ ] **Step 17: `update_schema_conf_splits_streams` — add `LOG_ERROR` at its 2 flagged `fopen` failure sites**

Locate (exact current text):

```c
    FILE *fin = fopen(conf, "r");
    if (!fin) return -1;
    int lockfd = fileno(fin);
    flock(lockfd, LOCK_EX);

    FILE *fout = fopen(tmp, "w");
    if (!fout) { flock(lockfd, LOCK_UN); fclose(fin); return -1; }
```

Replace with:

```c
    FILE *fin = fopen(conf, "r");
    if (!fin) {
        LOG_ERROR(LOG_SUB_CONFIG, "update_schema_conf_splits_streams: fopen %s failed: %s", conf, strerror(errno));
        return -1;
    }
    int lockfd = fileno(fin);
    flock(lockfd, LOCK_EX);

    FILE *fout = fopen(tmp, "w");
    if (!fout) {
        LOG_ERROR(LOG_SUB_CONFIG, "update_schema_conf_splits_streams: fopen %s failed: %s", tmp, strerror(errno));
        flock(lockfd, LOCK_UN); fclose(fin); return -1;
    }
```

> **Note for the executing model:** `query_find.c` has a second, independent read-modify-write path over `schema.conf` besides `query_maint.c`'s `ensure_schema_conf_line` (covered in Task 11). Both are being given equivalent `LOG_ERROR(LOG_SUB_CONFIG, ...)` treatment for consistency — this is intentional, not duplication.

- [ ] **Step 18: Build**

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: clean build, no new warnings.

- [ ] **Step 19: Full suite**

```bash
./build/bin/shard-db-test run-all
```

Expected: identical pass/fail counts to the Task 1 baseline — no regressions, no new failures.

- [ ] **Step 20: Manual spot-check**

Force a real failure for one site per file:
- `query_schema.c`: run `edit-field` on an object whose `fields.conf` is on a read-only filesystem or whose directory permissions block write access; confirm `LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: ...")` appears in `$LOG_DIR`.
- `query_bulk.c`: run a `bulk-insert`/`bulk-update`/`bulk-delete` against a nonexistent `dir`/`object` pair (or one where `slotcask_registry_get` would fail) and confirm the corresponding `LOG_WARN(LOG_SUB_SLOTCASK, ...)` appears; separately, set a very low `QUERY_BUFFER_MB` and run a criteria-based bulk-update/bulk-delete large enough to trip the budget check, confirming `LOG_WARN(LOG_SUB_QUERY, "bulk-update: query buffer cap exceeded...")` (or `bulk-delete:`) appears.
- `query_find.c`: same read-only-filesystem trick against `schema.conf` (or its containing directory) while triggering a splits/streams update path; confirm `LOG_ERROR(LOG_SUB_CONFIG, "update_schema_conf_splits_streams: ...")` appears.

Undo any forced read-only/permission changes afterward.

- [ ] **Step 21: Leave uncommitted**

Do not run `git add`/`git commit`. Move to Task 5.

---

### Task 5: `src/db/query_aggregate.c` — 27 sites (18 ERROR, 9 WARN)

**Files:**
- Modify: `src/db/query_aggregate.c`

**Interfaces:** none (additive logging only). All sites use `LOG_SUB_QUERY`
except the `shard_agg_worker` `slotcask_registry_get` site, which uses
`LOG_SUB_SLOTCASK`.

`src/db/query_join.c` is audited and confirmed to have **zero** flagged
sites — its 9 `return -1;` sites are all either client-input validation
already reported via `OUT("{\"error\":...}")`, or the codebase's
`-1`-halts-the-walk btree-callback control-flow convention in
`join_bt_first_cb`. No task needed for that file.

- [ ] **Step 1: `agg_arena_alloc` — add `LOG_ERROR` on `malloc` failure**

Locate (exact current text):

```c
    AggArenaSlab *s = malloc(sizeof(AggArenaSlab) + new_cap);
    if (!s) return NULL;
```

Replace with:

```c
    AggArenaSlab *s = malloc(sizeof(AggArenaSlab) + new_cap);
    if (!s) {
        LOG_ERROR(LOG_SUB_QUERY, "agg_arena_alloc: malloc %zu bytes failed", new_cap);
        return NULL;
    }
```

- [ ] **Step 2: `topn_heap_new` — add `LOG_ERROR` at its 2 flagged alloc-failure sites**

Locate (exact current text):

```c
    TopNHeap *h = calloc(1, sizeof(TopNHeap));
    if (!h) return NULL;
    h->entries = calloc((size_t)cap + 1, sizeof(TopNHeapEntry));
    if (!h->entries) { free(h); return NULL; }
```

Replace with:

```c
    TopNHeap *h = calloc(1, sizeof(TopNHeap));
    if (!h) {
        LOG_ERROR(LOG_SUB_QUERY, "topn_heap_new: calloc TopNHeap failed (cap=%d)", cap);
        return NULL;
    }
    h->entries = calloc((size_t)cap + 1, sizeof(TopNHeapEntry));
    if (!h->entries) {
        LOG_ERROR(LOG_SUB_QUERY, "topn_heap_new: calloc %d entries failed", cap);
        free(h); return NULL;
    }
```

- [ ] **Step 3: `topn_walk_cb` — add `LOG_ERROR` on `malloc` failure for `current_key`**

Locate (exact current text):

```c
        c->current_key = malloc(enc_val_len + 1);
        if (!c->current_key) return -1;
```

Replace with:

```c
        c->current_key = malloc(enc_val_len + 1);
        if (!c->current_key) {
            LOG_ERROR(LOG_SUB_QUERY, "topn_walk_cb: malloc %zu bytes failed", enc_val_len + 1);
            return -1;
        }
```

- [ ] **Step 4: top-N group-by driver — add `LOG_WARN` on deadline timeout, and `LOG_ERROR` on the 7-way `calloc` failure in the drain path**

Locate (exact current text):

```c
    if (dl->timed_out) {
        topn_heap_destroy(heap);
        return -1;
    }
```

Replace with:

```c
    if (dl->timed_out) {
        LOG_WARN(LOG_SUB_QUERY, "top-N group-by aggregate: query deadline exceeded");
        topn_heap_destroy(heap);
        return -1;
    }
```

Locate (exact current text):

```c
    double  *maxs    = calloc((size_t)n_out + 1, sizeof(double));
    if (!metrics || !gkeys || !gklens || !counts || !sums || !mins || !maxs) {
        free(metrics); free(gkeys); free(gklens);
        free(counts); free(sums); free(mins); free(maxs);
        topn_heap_destroy(heap);
        return -1;
```

Replace with:

```c
    double  *maxs    = calloc((size_t)n_out + 1, sizeof(double));
    if (!metrics || !gkeys || !gklens || !counts || !sums || !mins || !maxs) {
        LOG_ERROR(LOG_SUB_QUERY, "top-N drain: calloc failed for n_out=%d", n_out);
        free(metrics); free(gkeys); free(gklens);
        free(counts); free(sums); free(mins); free(maxs);
        topn_heap_destroy(heap);
        return -1;
```

- [ ] **Step 5: `hbk_init` — add `LOG_ERROR` on `calloc` failure**

Locate (exact current text):

```c
    m->entries = calloc(cap, sizeof(HashBktEntry));
    if (!m->entries) {
        m->cap = m->mask = 0;
        return -1;
    }
```

Replace with:

```c
    m->entries = calloc(cap, sizeof(HashBktEntry));
    if (!m->entries) {
        LOG_ERROR(LOG_SUB_QUERY, "hbk_init: calloc cap=%zu failed", cap);
        m->cap = m->mask = 0;
        return -1;
    }
```

- [ ] **Step 6: `hsm_init` — add `LOG_ERROR` on `entries`/`arena` alloc failure**

Locate (exact current text):

```c
    m->entries = calloc(cap, sizeof(HashStrEntry));
    if (arena_hint < 1024) arena_hint = 1024;
    m->arena = malloc(arena_hint);
    if (!m->entries || !m->arena) {
        free(m->entries); m->entries = NULL;
        free(m->arena); m->arena = NULL;
        m->cap = m->mask = m->arena_used = m->arena_cap = 0;
        return -1;
    }
```

Replace with:

```c
    m->entries = calloc(cap, sizeof(HashStrEntry));
    if (arena_hint < 1024) arena_hint = 1024;
    m->arena = malloc(arena_hint);
    if (!m->entries || !m->arena) {
        LOG_ERROR(LOG_SUB_QUERY, "hsm_init: alloc failed (cap=%zu, arena_hint=%zu)", cap, arena_hint);
        free(m->entries); m->entries = NULL;
        free(m->arena); m->arena = NULL;
        m->cap = m->mask = m->arena_used = m->arena_cap = 0;
        return -1;
    }
```

- [ ] **Step 7: `hsm_insert` — add `LOG_ERROR` on `realloc` failure**

Locate (exact current text):

```c
        char *na = realloc(m->arena, new_cap);
        if (!na) return -1;
```

Replace with:

```c
        char *na = realloc(m->arena, new_cap);
        if (!na) {
            LOG_ERROR(LOG_SUB_QUERY, "hsm_insert: realloc arena to %zu failed", new_cap);
            return -1;
        }
```

- [ ] **Step 8: `wfc_worker` — add `LOG_ERROR` on `btree_range_iter_open` failure**

Locate (exact current text):

```c
    BtRangeIter *it = btree_range_iter_open(
        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, w->desc);
    if (!it) return NULL;
```

Replace with:

```c
    BtRangeIter *it = btree_range_iter_open(
        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, w->desc);
    if (!it) {
        LOG_ERROR(LOG_SUB_QUERY, "wfc_worker: btree_range_iter_open %s failed", idx_path);
        return NULL;
    }
```

- [ ] **Step 9: `awc_shard_worker` — add `LOG_ERROR` on `btree_range_iter_open` failure**

Locate (exact current text):

```c
static void *awc_shard_worker(void *raw) {
    AwcShardArg *a = (AwcShardArg *)raw;
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path),
                   a->db_root, a->object, a->fld, a->shard_id);
    BtRangeIter *it = btree_range_iter_open(
        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) return NULL;
```

Replace with:

```c
static void *awc_shard_worker(void *raw) {
    AwcShardArg *a = (AwcShardArg *)raw;
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path),
                   a->db_root, a->object, a->fld, a->shard_id);
    BtRangeIter *it = btree_range_iter_open(
        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) {
        LOG_ERROR(LOG_SUB_QUERY, "awc_shard_worker: btree_range_iter_open %s failed", idx_path);
        return NULL;
    }
```

> **Note for the executing model:** Steps 8, 9, 13, 14, 16 all share the same `BtRangeIter *it = btree_range_iter_open(...); if (!it) return NULL;` shape but in five different functions (`wfc_worker`, `awc_shard_worker`, `sec_map_build_worker`, `igb_pass1_worker`, `agg_single_shard_worker`). Apply each edit only inside the named function, using the surrounding code shown in each step's Locate block to disambiguate — do not blind find-and-replace-all.

- [ ] **Step 10: `agg_find_or_create` — add `LOG_ERROR` on `agg_ht_lazy_init` OOM, and `LOG_WARN` on the bucket-bytes budget cap**

Locate (exact current text):

```c
    /* Lazy-init on first insert so an empty AggCtx pays no allocation. */
    if (!ctx->ht && agg_ht_lazy_init(ctx) != 0) {
        ctx->budget_exceeded = 1;
        return NULL;
    }
```

Replace with:

```c
    /* Lazy-init on first insert so an empty AggCtx pays no allocation. */
    if (!ctx->ht && agg_ht_lazy_init(ctx) != 0) {
        LOG_ERROR(LOG_SUB_QUERY, "agg_find_or_create: agg_ht_lazy_init failed (OOM), reported to client as buffer-exceeded");
        ctx->budget_exceeded = 1;
        return NULL;
    }
```

Locate (exact current text):

```c
        if (prev + bucket_bytes > g_query_buffer_max_bytes) {
            atomic_fetch_sub_explicit(ctx->shared_buffer_bytes,
                                      bucket_bytes, memory_order_relaxed);
            ctx->budget_exceeded = 1;
            return NULL;
        }
```

Replace with:

```c
        if (prev + bucket_bytes > g_query_buffer_max_bytes) {
            LOG_WARN(LOG_SUB_QUERY, "agg_find_or_create: query buffer cap exceeded at bucket_bytes=%zu (prev=%zu, cap=%zu)", bucket_bytes, prev, g_query_buffer_max_bytes);
            atomic_fetch_sub_explicit(ctx->shared_buffer_bytes,
                                      bucket_bytes, memory_order_relaxed);
            ctx->budget_exceeded = 1;
            return NULL;
        }
```

> **Note for the executing model:** these two sites are the pair called out in the audit as "conflates a real resource cap with an OOM under the same client-visible `budget_exceeded` flag." Both get log lines added, but neither changes the client-visible response — this is intentional (see design doc: no behavior changes).

- [ ] **Step 11: `shard_agg_worker` — add `LOG_WARN` on `slotcask_registry_get` failure, and `LOG_ERROR` on `malloc` failure for `hashes`**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(sa->db_root, sa->object, &info);
    if (!sdb) return NULL;

    /* Extract hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sa->entry_count * sizeof(*hashes));
    if (!hashes) return NULL;
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(sa->db_root, sa->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "shard_agg_worker: slotcask_registry_get failed for %s/%s", sa->db_root, sa->object);
        return NULL;
    }

    /* Extract hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sa->entry_count * sizeof(*hashes));
    if (!hashes) {
        LOG_ERROR(LOG_SUB_QUERY, "shard_agg_worker: malloc %d hashes failed", sa->entry_count);
        return NULL;
    }
```

- [ ] **Step 12: `sec_map_build_worker` — add `LOG_ERROR` on `btree_range_iter_open` failure**

Locate (exact current text):

```c
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), w->db_root, w->object,
                   w->gfield_s, w->shard_id);
    BtRangeIter *it = btree_range_iter_open(idx_path, "", 0, 0,
                                             "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) return NULL;
    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(w->dl, &w->dl_counter)) {
```

Replace with:

```c
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), w->db_root, w->object,
                   w->gfield_s, w->shard_id);
    BtRangeIter *it = btree_range_iter_open(idx_path, "", 0, 0,
                                             "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) {
        LOG_ERROR(LOG_SUB_QUERY, "sec_map_build_worker: btree_range_iter_open %s failed", idx_path);
        return NULL;
    }
    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(w->dl, &w->dl_counter)) {
```

- [ ] **Step 13: `igb_pass1_worker` — add `LOG_ERROR` on `btree_range_iter_open` failure**

Locate (exact current text):

```c
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), w->db_root, w->object,
                   w->gfield, w->shard_id);
    BtRangeIter *it = btree_range_iter_open(idx_path, "", 0, 0,
                                             "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) return NULL;
    char prev_enc[64]; size_t prev_enc_len = 0;
```

Replace with:

```c
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), w->db_root, w->object,
                   w->gfield, w->shard_id);
    BtRangeIter *it = btree_range_iter_open(idx_path, "", 0, 0,
                                             "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) {
        LOG_ERROR(LOG_SUB_QUERY, "igb_pass1_worker: btree_range_iter_open %s failed", idx_path);
        return NULL;
    }
    char prev_enc[64]; size_t prev_enc_len = 0;
```

- [ ] **Step 14: `bktarr_push` — add `LOG_ERROR` on `realloc` failure**

Locate (exact current text):

```c
        AggBucket **nb = realloc(a->arr, (size_t)new_cap * sizeof(AggBucket *));
        if (!nb) return -1;
```

Replace with:

```c
        AggBucket **nb = realloc(a->arr, (size_t)new_cap * sizeof(AggBucket *));
        if (!nb) {
            LOG_ERROR(LOG_SUB_QUERY, "bktarr_push: realloc to cap=%d failed", new_cap);
            return -1;
        }
```

- [ ] **Step 15: `agg_single_shard_worker` (MIN/MAX path) — add `LOG_ERROR` on `btree_range_iter_open` failure**

Locate (exact current text):

```c
    BtRangeIter *it = btree_range_iter_open(
        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, a->desc);
    if (!it) return NULL;

    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        double v;
```

Replace with:

```c
    BtRangeIter *it = btree_range_iter_open(
        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, a->desc);
    if (!it) {
        LOG_ERROR(LOG_SUB_QUERY, "agg_single_shard_worker: btree_range_iter_open failed");
        return NULL;
    }

    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        double v;
```

- [ ] **Step 16: `cmd_aggregate_do` (single-field-fast-path, after `wfc_worker` fan-out) — add `LOG_WARN` on deadline timeout**

Locate (exact current text):

```c
                free(wargs);
                if (dl.timed_out) {
                    OUT("{\"error\":\"query_timeout\"}\n");
                    return -1;
                }
```

Replace with:

```c
                free(wargs);
                if (dl.timed_out) {
                    LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query deadline exceeded (single-shard fast path)");
                    OUT("{\"error\":\"query_timeout\"}\n");
                    return -1;
                }
```

- [ ] **Step 17: `cmd_aggregate_do` (NEQ split-plan path, position-check) — add `LOG_WARN` on deadline timeout**

Locate (exact current text):

```c
            btree_dispatch(db_root, object, pos.field, sch.splits,
                           &pos, pos_tf, idx_count_cb, &ic);
            if (dl.timed_out) {
                OUT("{\"error\":\"query_timeout\"}\n");
                agg_free(&ctx); return -1;
            }
```

Replace with:

```c
            btree_dispatch(db_root, object, pos.field, sch.splits,
                           &pos, pos_tf, idx_count_cb, &ic);
            if (dl.timed_out) {
                LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query deadline exceeded (NEQ split-plan position check)");
                OUT("{\"error\":\"query_timeout\"}\n");
                agg_free(&ctx); return -1;
            }
```

- [ ] **Step 18: `cmd_aggregate_do` (NEQ split-plan, `ctx_full` clone) — add `LOG_WARN` on deadline timeout and query buffer cap exceeded**

Locate (exact current text):

```c
        if (dl.timed_out) {
            OUT("{\"error\":\"query_timeout\"}\n");
            agg_ctx_free_local(&ctx_full);
            agg_free(&ctx);
            return -1;
        }
        if (ctx.budget_exceeded || ctx_full.budget_exceeded) {
            OUT(QUERY_BUFFER_ERR);
            agg_ctx_free_local(&ctx_full);
            agg_free(&ctx);
            return -1;
```

Replace with:

```c
        if (dl.timed_out) {
            LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query deadline exceeded (NEQ split-plan)");
            OUT("{\"error\":\"query_timeout\"}\n");
            agg_ctx_free_local(&ctx_full);
            agg_free(&ctx);
            return -1;
        }
        if (ctx.budget_exceeded || ctx_full.budget_exceeded) {
            LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query buffer cap exceeded (NEQ split-plan)");
            OUT(QUERY_BUFFER_ERR);
            agg_ctx_free_local(&ctx_full);
            agg_free(&ctx);
            return -1;
```

- [ ] **Step 19: `cmd_aggregate_do` (AWC per-shard fan-out setup) — add `LOG_ERROR` on the 6-way `calloc` failure**

Locate (exact current text):

```c
                    if (!wargs || !w_counts || !w_sums || !w_mins ||
                        !w_maxs || !w_present) {
                        free(wargs); free(w_counts); free(w_sums);
                        free(w_mins); free(w_maxs); free(w_present);
                        keyset_free(crit_ks);
                        OUT(QUERY_BUFFER_ERR);
                        return -1;
                    }
```

Replace with:

```c
                    if (!wargs || !w_counts || !w_sums || !w_mins ||
                        !w_maxs || !w_present) {
                        LOG_ERROR(LOG_SUB_QUERY, "cmd_aggregate_do: calloc failed for n_idx=%d shard workers", n_idx);
                        free(wargs); free(w_counts); free(w_sums);
                        free(w_mins); free(w_maxs); free(w_present);
                        keyset_free(crit_ks);
                        OUT(QUERY_BUFFER_ERR);
                        return -1;
                    }
```

- [ ] **Step 20: `cmd_aggregate_do` (final `agg_run_plan` result check) — add `LOG_WARN` on deadline timeout and query buffer cap exceeded**

Locate (exact current text):

```c
    } else if (agg_run_plan(&ctx, tree, db_root, object, &sch) != 0) {
        if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
        else if (ctx.budget_exceeded) OUT(QUERY_BUFFER_ERR);
        agg_free(&ctx);
        return -1;
    }
```

Replace with:

```c
    } else if (agg_run_plan(&ctx, tree, db_root, object, &sch) != 0) {
        if (dl.timed_out) {
            LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query deadline exceeded");
            OUT("{\"error\":\"query_timeout\"}\n");
        } else if (ctx.budget_exceeded) {
            LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query buffer cap exceeded");
            OUT(QUERY_BUFFER_ERR);
        }
        agg_free(&ctx);
        return -1;
    }
```

- [ ] **Step 21: `cmd_aggregate_tree` — add `LOG_ERROR` on `calloc` failure for `AggSpec[]`**

Locate (exact current text):

```c
    AggSpec *specs = calloc(naggs, sizeof(AggSpec));
    if (!specs) {
        OUT("{\"error\":\"out of memory\"}\n");
        return -1;
    }
```

Replace with:

```c
    AggSpec *specs = calloc(naggs, sizeof(AggSpec));
    if (!specs) {
        LOG_ERROR(LOG_SUB_QUERY, "cmd_aggregate_tree: calloc %d AggSpec failed", naggs);
        OUT("{\"error\":\"out of memory\"}\n");
        return -1;
    }
```

- [ ] **Step 22: Build**

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: clean build, no new warnings.

- [ ] **Step 23: Full suite**

```bash
./build/bin/shard-db-test run-all
```

Expected: identical pass/fail counts to the Task 1 baseline — no regressions, no new failures.

- [ ] **Step 24: Manual spot-check**

Set a very low `QUERY_BUFFER_MB` (e.g. `QUERY_BUFFER_MB=1`) in `db.env`, restart the daemon, and run an `aggregate` with `group_by` against an object with enough distinct groups to exceed 1 MB of intermediate hash-map/bucket storage. Confirm the client receives the existing `QUERY_BUFFER_ERR` response AND a matching `LOG_WARN` line appears in `$LOG_DIR` (e.g. `agg_find_or_create: query buffer cap exceeded ...` or `cmd_aggregate_do: query buffer cap exceeded`). Restore `QUERY_BUFFER_MB` and restart the daemon afterward.

- [ ] **Step 25: Leave uncommitted**

Do not run `git add`/`git commit`. Move to Task 6.

---

### Task 6: `src/db/index.c` — 25 sites (24 ERROR, 1 WARN)

**Files:**
- Modify: `src/db/index.c`

**Interfaces:** none (additive logging only). Subsystem tags per site:
`LOG_SUB_REINDEX` (spill-file helpers, merge/enumerate/multi-field-worker
plumbing), `LOG_SUB_TRIGRAM` (`build_trigram_pass`), `LOG_SUB_BTREE`
(`build_btree_streaming`), `LOG_SUB_BITMAP` (bitmap-rebuild path). All four
already exist in `LogSubsystem` in `src/db/log.h` — no enum changes needed
for this task.

**Out of scope (do not touch):** `resolve_bitmaps`'s per-shard
`open()`/`fstat()`/`mmap()` loop (~line 2674–2690) silently `continue`s
past failures with no `return` statement to anchor a Locate/Replace block
on — it's a real gap but structurally outside this plan's return-statement
scope; `kf_probe_slot`'s `!k->map` guard (line 2643) is the downstream
symptom of that same gap and is SKIP-benign here for the same reason. Both
are noted as follow-ups in the audit findings report, not tasks.

- [ ] **Step 1: `spill_writer_open` — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
static int spill_writer_open(SpillWriter *sw, const char *path) {
    sw->fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (sw->fd < 0) return -1;
    sw->wbuf = malloc(SPILL_WRITE_BUF_BYTES);
    if (!sw->wbuf) { close(sw->fd); sw->fd = -1; return -1; }
```

Replace with:

```c
static int spill_writer_open(SpillWriter *sw, const char *path) {
    sw->fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (sw->fd < 0) {
        LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_open: open failed for %s: %s", path, strerror(errno));
        return -1;
    }
    sw->wbuf = malloc(SPILL_WRITE_BUF_BYTES);
    if (!sw->wbuf) {
        LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_open: malloc(%d) failed for spill write buffer (%s)", SPILL_WRITE_BUF_BYTES, path);
        close(sw->fd); sw->fd = -1; return -1;
    }
```

- [ ] **Step 2: `spill_writer_drain` — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
    const uint8_t *p = sw->wbuf;
    size_t left = sw->wbuf_used;
    while (left > 0) {
        ssize_t n = write(sw->fd, p, left);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        if (n == 0) return -1;
        p += n; left -= (size_t)n;
```

Replace with:

```c
    const uint8_t *p = sw->wbuf;
    size_t left = sw->wbuf_used;
    while (left > 0) {
        ssize_t n = write(sw->fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_drain: write failed on fd %d: %s", sw->fd, strerror(errno));
            return -1;
        }
        if (n == 0) {
            LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_drain: write returned 0 (disk full?) on fd %d", sw->fd);
            return -1;
        }
        p += n; left -= (size_t)n;
```

- [ ] **Step 3: `spill_writer_put` (oversize-chunk direct write) — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
            /* Direct write for oversize chunk. */
            const uint8_t *p = data; size_t left = len;
            while (left > 0) {
                ssize_t n = write(sw->fd, p, left);
                if (n < 0) { if (errno == EINTR) continue; return -1; }
                if (n == 0) return -1;
                p += n; left -= (size_t)n;
            }
```

Replace with:

```c
            /* Direct write for oversize chunk. */
            const uint8_t *p = data; size_t left = len;
            while (left > 0) {
                ssize_t n = write(sw->fd, p, left);
                if (n < 0) {
                    if (errno == EINTR) continue;
                    LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_put: oversize direct write failed on fd %d (len=%zu): %s", sw->fd, len, strerror(errno));
                    return -1;
                }
                if (n == 0) {
                    LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_put: oversize direct write returned 0 on fd %d (len=%zu)", sw->fd, len);
                    return -1;
                }
                p += n; left -= (size_t)n;
            }
```

- [ ] **Step 4: `spill_writer_write_run` — add `LOG_WARN` on the 4GB body-size invariant violation**

Locate (exact current text):

```c
    if (body > 0xFFFFFFFFULL) return -1;  /* shouldn't happen with sane buffer caps */
    uint32_t body_u32 = (uint32_t)body;
```

Replace with:

```c
    if (body > 0xFFFFFFFFULL) {
        LOG_WARN(LOG_SUB_REINDEX, "spill_writer_write_run: run body size %llu exceeds 4GB cap (count=%u); dropping run", (unsigned long long)body, count);
        return -1;  /* shouldn't happen with sane buffer caps */
    }
    uint32_t body_u32 = (uint32_t)body;
```

- [ ] **Step 5: `spill_run_fill` — add `LOG_ERROR` on `pread` failure/unexpected EOF**

Locate (exact current text):

```c
    ssize_t n = pread(r->fd, r->buf, want, r->pos);
    if (n <= 0) { r->eof = 1; return -1; }
```

Replace with:

```c
    ssize_t n = pread(r->fd, r->buf, want, r->pos);
    if (n <= 0) {
        LOG_ERROR(LOG_SUB_REINDEX, "spill_run_fill: pread failed on fd %d at offset %lld (want=%zu): %s", r->fd, (long long)r->pos, want, strerror(errno));
        r->eof = 1; return -1;
    }
```

- [ ] **Step 6: `spill_run_take` — add `LOG_ERROR` on unexpected EOF mid-entry**

Locate (exact current text):

```c
    uint8_t *o = out;
    while (len > 0) {
        if (r->buf_off >= r->buf_used) {
            if (spill_run_fill(r) != 0) return -1;
            if (r->buf_off >= r->buf_used) return -1;  /* eof mid-entry */
        }
```

Replace with:

```c
    uint8_t *o = out;
    while (len > 0) {
        if (r->buf_off >= r->buf_used) {
            if (spill_run_fill(r) != 0) return -1;
            if (r->buf_off >= r->buf_used) {
                LOG_ERROR(LOG_SUB_REINDEX, "spill_run_take: unexpected EOF mid-entry on fd %d (wanted %zu more bytes)", r->fd, len);
                return -1;  /* eof mid-entry */
            }
        }
```

> **Note for the executing model:** `spill_run_fill(r) != 0` on the line above is pure propagation of Step 5's already-logged failure — do not add a second log line there, only at the `eof mid-entry` site.

- [ ] **Step 7: `spill_run_advance` — add `LOG_ERROR` on corrupt spill entry (`vlen` overflows peek buffer)**

Locate (exact current text):

```c
static int spill_run_advance(SpillRunReader *r) {
    if (r->entries_remaining == 0) { r->has_entry = 0; r->eof = 1; return 0; }
    uint16_t vlen;
    if (spill_run_take(r, &vlen, sizeof(vlen)) != 0) { r->has_entry = 0; return -1; }
    if (vlen > sizeof(r->value)) { r->has_entry = 0; return -1; }
    if (spill_run_take(r, r->value, vlen)           != 0) { r->has_entry = 0; return -1; }
    if (spill_run_take(r, r->hash,  BT_HASH_SIZE)   != 0) { r->has_entry = 0; return -1; }
```

Replace with:

```c
static int spill_run_advance(SpillRunReader *r) {
    if (r->entries_remaining == 0) { r->has_entry = 0; r->eof = 1; return 0; }
    uint16_t vlen;
    if (spill_run_take(r, &vlen, sizeof(vlen)) != 0) { r->has_entry = 0; return -1; }
    if (vlen > sizeof(r->value)) {
        LOG_ERROR(LOG_SUB_REINDEX, "spill_run_advance: corrupt spill entry on fd %d: vlen=%u exceeds buffer size %zu", r->fd, vlen, sizeof(r->value));
        r->has_entry = 0; return -1;
    }
    if (spill_run_take(r, r->value, vlen)           != 0) { r->has_entry = 0; return -1; }
    if (spill_run_take(r, r->hash,  BT_HASH_SIZE)   != 0) { r->has_entry = 0; return -1; }
```

> **Note for the executing model:** the two `spill_run_take(...) != 0` checks in this block are pure propagation of Step 6's already-logged failure — only the `vlen > sizeof(r->value)` corruption check gets a new log line.

- [ ] **Step 8: `merge_spills_into_index` — add `LOG_ERROR` at its 2 flagged alloc-failure sites**

Locate (exact current text):

```c
    int *fds = calloc((size_t)n_kf, sizeof(int));
    if (!fds) return -1;
    for (int w = 0; w < n_kf; w++) fds[w] = -1;
```

Replace with:

```c
    int *fds = calloc((size_t)n_kf, sizeof(int));
    if (!fds) {
        LOG_ERROR(LOG_SUB_REINDEX, "merge_spills_into_index: calloc failed for %d fd slots (%s/%s/%s shard %d)", n_kf, db_root, object, field, shard);
        return -1;
    }
    for (int w = 0; w < n_kf; w++) fds[w] = -1;
```

Locate (exact current text):

```c
    SpillRunReader *readers = malloc(reader_cap * sizeof(SpillRunReader));
    if (!readers) { free(fds); return -1; }
```

Replace with:

```c
    SpillRunReader *readers = malloc(reader_cap * sizeof(SpillRunReader));
    if (!readers) {
        LOG_ERROR(LOG_SUB_REINDEX, "merge_spills_into_index: malloc failed for %zu spill run readers (%s/%s/%s shard %d)", reader_cap, db_root, object, field, shard);
        free(fds); return -1;
    }
```

- [ ] **Step 9: `build_trigram_pass` — add `LOG_ERROR` on `slotcask_registry_get` failure**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return -1;

    LOG_WARN(LOG_SUB_TRIGRAM, "BUILD-TRIGRAM %s/%s/%s: segment-sequential scan",
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_TRIGRAM, "build_trigram_pass: slotcask_registry_get failed for %s/%s", db_root, object);
        return -1;
    }

    LOG_WARN(LOG_SUB_TRIGRAM, "BUILD-TRIGRAM %s/%s/%s: segment-sequential scan",
```

> **Note for the executing model:** this function already has an existing `LOG_WARN(LOG_SUB_TRIGRAM, "BUILD-TRIGRAM ...")` a few lines below the `slotcask_registry_get` check — that one is ALREADY-LOGGED and unrelated (it's an informational progress log, not an error path); do not modify it. Only the `if (!sdb)` failure branch above it is new.

- [ ] **Step 10: `build_btree_streaming` — add `LOG_ERROR` on `slotcask_registry_get` failure**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return -1;

    MFFieldDesc d;
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_BTREE, "build_btree_streaming: slotcask_registry_get failed for %s/%s/%s", db_root, object, field);
        return -1;
    }

    MFFieldDesc d;
```

- [ ] **Step 11: `bm_shard_walk_worker` — add `LOG_ERROR` on `bm_open` failure (silent stale-bitmap gap)**

Locate (exact current text):

```c
static void *bm_shard_walk_worker(void *arg) {
    BmShardWalkArg *a = (BmShardWalkArg *)arg;
    BitmapShard *bm = bm_open(a->path, a->slots_per_shard, 0, 0, 0,
                              1 /* writer: reindex bm_set's */);
    if (!bm) return NULL;
```

Replace with:

```c
static void *bm_shard_walk_worker(void *arg) {
    BmShardWalkArg *a = (BmShardWalkArg *)arg;
    BitmapShard *bm = bm_open(a->path, a->slots_per_shard, 0, 0, 0,
                              1 /* writer: reindex bm_set's */);
    if (!bm) {
        LOG_ERROR(LOG_SUB_BITMAP, "bm_shard_walk_worker: bm_open failed for %s (kf_shard=%d); bitmap left stale for this shard", a->path, a->kf_shard);
        return NULL;
    }
```

- [ ] **Step 12: `build_bitmap_pass` — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return -1;

    /* Parallel kf-shard walks: each worker opens its own .bm (paths are
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_BITMAP, "build_bitmap_pass: slotcask_registry_get failed for %s/%s/%s", db_root, object, field);
        return -1;
    }

    /* Parallel kf-shard walks: each worker opens its own .bm (paths are
```

Locate (exact current text):

```c
    BmShardWalkArg *args = malloc((size_t)sch->splits * sizeof(BmShardWalkArg));
    if (!args) return -1;
```

Replace with:

```c
    BmShardWalkArg *args = malloc((size_t)sch->splits * sizeof(BmShardWalkArg));
    if (!args) {
        LOG_ERROR(LOG_SUB_BITMAP, "build_bitmap_pass: malloc failed for %d BmShardWalkArg entries (%s/%s/%s)", sch->splits, db_root, object, field);
        return -1;
    }
```

- [ ] **Step 13: `mf_worker_field_alloc` — add `LOG_ERROR` at its 2 flagged sites**

> **Note for the executing model:** these two sites use `LOG_SUB_REINDEX`,
> not `LOG_SUB_TRIGRAM`. The original findings draft tagged this function
> `LOG_SUB_TRIGRAM`, but `mf_worker_field_alloc` allocates buffers for
> *any* indexed field type — `d->type == STREAM_TRIGRAM` is only one of
> several branches inside it (see the `est` sizing logic a few lines above
> the anchor below). `LOG_SUB_REINDEX` is the correct tag here per this
> task's own Interfaces block ("multi-field-worker plumbing").

Locate (exact current text):

```c
    f->flush_offsets = calloc((size_t)idx_n, sizeof(size_t));
    f->flush_cursors = calloc((size_t)idx_n, sizeof(size_t));
    if (!f->pairs || !f->arena || !f->flush_out ||
        !f->flush_counts || !f->flush_offsets || !f->flush_cursors)
        return -1;
    f->spill_writers = calloc((size_t)idx_n, sizeof(SpillWriter));
    if (!f->spill_writers) return -1;
```

Replace with:

```c
    f->flush_offsets = calloc((size_t)idx_n, sizeof(size_t));
    f->flush_cursors = calloc((size_t)idx_n, sizeof(size_t));
    if (!f->pairs || !f->arena || !f->flush_out ||
        !f->flush_counts || !f->flush_offsets || !f->flush_cursors) {
        LOG_ERROR(LOG_SUB_REINDEX, "mf_worker_field_alloc: allocation failed for field %s (cap=%zu, idx_n=%d)", d->name, cap, idx_n);
        return -1;
    }
    f->spill_writers = calloc((size_t)idx_n, sizeof(SpillWriter));
    if (!f->spill_writers) {
        LOG_ERROR(LOG_SUB_REINDEX, "mf_worker_field_alloc: calloc failed for %d spill_writers (field %s)", idx_n, d->name);
        return -1;
    }
```

> **Note for the executing model:** confirm the exact variable name used for the field's byte-capacity (`cap` above) against the live source at this site — the audit's draft log text assumes a local named `cap` is in scope; if the actual variable is named differently, use the correct name.

- [ ] **Step 14: `enumerate_segments` — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
static SegRef *enumerate_segments(const char *data_dir, int n_streams, int *out_n) {
    size_t cap = 256, n = 0;
    SegRef *segs = malloc(cap * sizeof(SegRef));
    if (!segs) { *out_n = 0; return NULL; }
```

Replace with:

```c
static SegRef *enumerate_segments(const char *data_dir, int n_streams, int *out_n) {
    size_t cap = 256, n = 0;
    SegRef *segs = malloc(cap * sizeof(SegRef));
    if (!segs) {
        LOG_ERROR(LOG_SUB_REINDEX, "enumerate_segments: malloc failed for %zu SegRef entries (%s)", cap, data_dir);
        *out_n = 0; return NULL;
    }
```

Locate (exact current text):

```c
                SegRef *t = realloc(segs, cap * sizeof(SegRef));
                if (!t) { closedir(d); free(segs); *out_n = 0; return NULL; }
```

Replace with:

```c
                SegRef *t = realloc(segs, cap * sizeof(SegRef));
                if (!t) {
                    LOG_ERROR(LOG_SUB_REINDEX, "enumerate_segments: realloc failed growing to %zu SegRef entries (%s)", cap, data_dir);
                    closedir(d); free(segs); *out_n = 0; return NULL;
                }
```

- [ ] **Step 15: `resolve_bitmaps` — add `LOG_ERROR` on `calloc` failure for `kf`**

Locate (exact current text):

```c
    KfMap *kf = calloc((size_t)splits, sizeof(KfMap));
    if (!kf) return -1;
```

Replace with:

```c
    KfMap *kf = calloc((size_t)splits, sizeof(KfMap));
    if (!kf) {
        LOG_ERROR(LOG_SUB_BITMAP, "resolve_bitmaps: calloc failed for %d KfMap entries (%s/%s)", splits, db_root, object);
        return -1;
    }
```

- [ ] **Step 16: `seg_seq_build_spills` — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
    SegScanWorker *workers = calloc((size_t)P, sizeof(SegScanWorker));
    if (!workers) { free(segs); return -1; }
```

Replace with:

```c
    SegScanWorker *workers = calloc((size_t)P, sizeof(SegScanWorker));
    if (!workers) {
        LOG_ERROR(LOG_SUB_REINDEX, "seg_seq_build_spills: calloc failed for %d SegScanWorker entries (%s/%s)", P, db_root, object);
        free(segs); return -1;
    }
```

Locate (exact current text):

```c
    if (!alloc_ok) {
        for (int w = 0; w < P; w++) {
            if (workers[w].fields) {
                for (int fi = 0; fi < n_fields; fi++)
                    mf_worker_field_free_spill(&workers[w].fields[fi], idx_n);
                free(workers[w].fields);
            }
            free(workers[w].bm_writers);
        }
        free(workers); free(segs);
        return -1;
    }
```

Replace with:

```c
    if (!alloc_ok) {
        LOG_ERROR(LOG_SUB_REINDEX, "seg_seq_build_spills: per-worker buffer allocation failed (%s/%s, %d workers, %d fields)", db_root, object, P, n_fields);
        for (int w = 0; w < P; w++) {
            if (workers[w].fields) {
                for (int fi = 0; fi < n_fields; fi++)
                    mf_worker_field_free_spill(&workers[w].fields[fi], idx_n);
                free(workers[w].fields);
            }
            free(workers[w].bm_writers);
        }
        free(workers); free(segs);
        return -1;
    }
```

- [ ] **Step 17: `build_indexes_streaming_multi` — add `LOG_ERROR` on `slotcask_registry_get` failure**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return -1;

    return seg_seq_build_spills(db_root, object, sch, ts, sdb, descs, n_fields);
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_REINDEX, "build_indexes_streaming_multi: slotcask_registry_get failed for %s/%s", db_root, object);
        return -1;
    }

    return seg_seq_build_spills(db_root, object, sch, ts, sdb, descs, n_fields);
```

> **Note for the executing model:** `index.c` has FIVE occurrences of the shape `SlotcaskDb *sdb = slotcask_registry_get(...); if (!sdb) return -1;` across this task (Steps 9, 10, 12, 17, plus the pre-existing one at `build_trigram_pass` covered in Step 9) — each in a different function with a different subsystem tag and message. Apply each only at its named function; use `grep -n` to disambiguate if unsure.

- [ ] **Step 18: Build**

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: clean build, no new warnings. If any `LOG_SUB_*` tag used above doesn't exist yet in `src/db/log.h`, add it as a new enum value before this step (see Interfaces note above).

- [ ] **Step 19: Full suite**

```bash
./build/bin/shard-db-test run-all
```

Expected: identical pass/fail counts to the Task 1 baseline — no regressions, no new failures.

- [ ] **Step 20: Manual spot-check**

Force a real failure: run `./shard-db add-index <dir> <obj> <field>` (or `reindex`) against an object while the `data/kf/` directory (or a specific spill/temp path) is on a read-only filesystem or has restrictive permissions, so a spill-writer `open()`/`malloc()` fails. Confirm the command reports failure AND a matching `LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_open: ...")` (or the nearest reachable site) line appears in `$LOG_DIR`. Restore permissions afterward.

- [ ] **Step 21: Leave uncommitted**

Do not run `git add`/`git commit`. Move to Task 7.

---

### Task 7: `src/db/storage.c` — 22 sites (20 ERROR, 2 WARN)

**Files:**
- Modify: `src/db/storage.c`

**Interfaces:** none (additive logging only). All sites use `LOG_SUB_SLOTCASK`
— the tag already used elsewhere in this file for storage/ucache-adjacent
code.

- [ ] **Step 1: `ucache_ensure` (read-only open path) — add `LOG_ERROR` at its 3 flagged sites**

Locate (exact current text):

```c
        /* Read-only: open existing file */
        fd = open(path, O_RDWR);
        if (fd < 0) { pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        struct stat st;
        if (fstat(fd, &st) < 0 || st.st_size == 0) { close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        slots_per_shard = shard_init_or_read_header(fd, 0);
        if (slots_per_shard == 0) { close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        sz = st.st_size;
    } else {
```

Replace with:

```c
        /* Read-only: open existing file */
        fd = open(path, O_RDWR);
        if (fd < 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_ensure %s: open(O_RDWR) failed: %s", path, strerror(errno));
            pthread_mutex_unlock(&g_ucache_table_mutex); return -1;
        }
        struct stat st;
        if (fstat(fd, &st) < 0 || st.st_size == 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_ensure %s: fstat failed or file empty: %s", path, strerror(errno));
            close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1;
        }
        slots_per_shard = shard_init_or_read_header(fd, 0);
        if (slots_per_shard == 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_ensure %s: shard_init_or_read_header failed (corrupt/short header)", path);
            close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1;
        }
        sz = st.st_size;
    } else {
```

- [ ] **Step 2: `ucache_ensure` (write/create path) — add `LOG_ERROR` at its 3 flagged sites**

Locate (exact current text):

```c
        /* Write: create file if needed, write header + ftruncate */
        mkdirp(dirname_of(path));
        fd = open(path, O_RDWR | O_CREAT, 0644);
        if (fd < 0) { pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        slots_per_shard = shard_init_or_read_header(fd, slot_size_for_create);
        if (slots_per_shard == 0) { close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        struct stat st;
        if (fstat(fd, &st) < 0) { close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        sz = st.st_size;
    }
```

Replace with:

```c
        /* Write: create file if needed, write header + ftruncate */
        mkdirp(dirname_of(path));
        fd = open(path, O_RDWR | O_CREAT, 0644);
        if (fd < 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_ensure %s: open(O_RDWR|O_CREAT) failed: %s", path, strerror(errno));
            pthread_mutex_unlock(&g_ucache_table_mutex); return -1;
        }
        slots_per_shard = shard_init_or_read_header(fd, slot_size_for_create);
        if (slots_per_shard == 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_ensure %s: shard_init_or_read_header failed during create (slot_size=%d)", path, slot_size_for_create);
            close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1;
        }
        struct stat st;
        if (fstat(fd, &st) < 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_ensure %s: fstat failed: %s", path, strerror(errno));
            close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1;
        }
        sz = st.st_size;
    }
```

- [ ] **Step 3: `ucache_ensure` (LRU eviction search exhausted) — add `LOG_WARN`**

Locate (exact current text):

```c
        } else {
            close(fd);
            pthread_mutex_unlock(&g_ucache_table_mutex);
            return -1;
        }
    }
```

Replace with:

```c
        } else {
            LOG_WARN(LOG_SUB_SLOTCASK, "ucache_ensure %s: LRU eviction exhausted (no evictable entry found, g_ucache_count=%d)", path, g_ucache_count);
            close(fd);
            pthread_mutex_unlock(&g_ucache_table_mutex);
            return -1;
        }
    }
```

> **Note for the executing model:** this `else` branch is inside the LRU-eviction-search `if (lru >= 0) { ... } else { ... }` block (the `if` arm evicts a victim entry; the `else` arm is this failure path) — confirm you're editing the eviction-search else-branch and not a different `else { ... return -1; }` shape elsewhere in the file.

- [ ] **Step 4: `ucache_ensure` — add `LOG_ERROR` on `mmap_with_hints` failure**

Locate (exact current text):

```c
    e->map = mmap_with_hints(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (e->map == MAP_FAILED) { e->map = NULL; close(fd); e->fd = -1; pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
```

Replace with:

```c
    e->map = mmap_with_hints(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (e->map == MAP_FAILED) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_ensure %s: mmap failed (size=%zu): %s", path, sz, strerror(errno));
        e->map = NULL; close(fd); e->fd = -1; pthread_mutex_unlock(&g_ucache_table_mutex); return -1;
    }
```

- [ ] **Step 5: `ucache_grow_to` — add `LOG_ERROR` on the `target_slots` power-of-2 invariant violation**

Locate (exact current text):

```c
    if (!g_ucache) return -1;
    /* target_slots must be a power of 2. */
    if (target_slots == 0 || (target_slots & (target_slots - 1)) != 0) return -1;
```

Replace with:

```c
    if (!g_ucache) return -1;
    /* target_slots must be a power of 2. */
    if (target_slots == 0 || (target_slots & (target_slots - 1)) != 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_grow_to %s: target_slots %u is not a power of 2", path, target_slots);
        return -1;
    }
```

> **Note for the executing model:** the `if (!g_ucache) return -1;` guard immediately above is SKIP-benign (defensive not-yet-initialized guard) — do not add a log there, only at the power-of-2 check.

- [ ] **Step 6: `ucache_grow_to` — add `LOG_ERROR` on the corrupt/missing shard header check**

Locate (exact current text):

```c
    ShardHeader *old_hdr = (ShardHeader *)e->map;
    if (!old_hdr || old_hdr->magic != SHARD_MAGIC) {
        pthread_rwlock_unlock(&e->rwlock);
        return -1;
    }
```

Replace with:

```c
    ShardHeader *old_hdr = (ShardHeader *)e->map;
    if (!old_hdr || old_hdr->magic != SHARD_MAGIC) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_grow_to %s: bad or missing shard header (magic=0x%x, expected 0x%x)", path, old_hdr ? old_hdr->magic : 0, SHARD_MAGIC);
        pthread_rwlock_unlock(&e->rwlock);
        return -1;
    }
```

- [ ] **Step 7: `ucache_grow_to` — add `LOG_ERROR` on `open`/`ftruncate` failures for the `.new` grow-target file**

Locate (exact current text):

```c
    int nfd = open(new_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (nfd < 0) { pthread_rwlock_unlock(&e->rwlock); return -1; }

    size_t new_size = shard_file_size(new_slots, slot_size);
    if (ftruncate(nfd, new_size) < 0) {
        close(nfd); unlink(new_path); pthread_rwlock_unlock(&e->rwlock); return -1;
    }
```

Replace with:

```c
    int nfd = open(new_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (nfd < 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_grow_to %s: open(%s, O_CREAT|O_TRUNC) failed: %s", path, new_path, strerror(errno));
        pthread_rwlock_unlock(&e->rwlock); return -1;
    }

    size_t new_size = shard_file_size(new_slots, slot_size);
    if (ftruncate(nfd, new_size) < 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_grow_to %s: ftruncate(%s, %zu) failed: %s", path, new_path, new_size, strerror(errno));
        close(nfd); unlink(new_path); pthread_rwlock_unlock(&e->rwlock); return -1;
    }
```

- [ ] **Step 8: `ucache_grow_to` — add `LOG_ERROR` on `mmap` failure for the new grow-target file**

Locate (exact current text): the `mmap(NULL, new_size, ...)` failure check on `nfd` (around line 473). Copy the exact current `if (nmap == MAP_FAILED) { ... return -1; }` (or equivalent) block verbatim from `src/db/storage.c`, and insert as its first statement:

```c
        LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_grow_to %s: mmap(%s, %zu) failed: %s", path, new_path, new_size, strerror(errno));
```

- [ ] **Step 9: `ucache_grow_to` — add `LOG_ERROR` on `rename` failure completing the atomic swap**

Locate (exact current text):

```c
    if (rename(new_path, path) != 0) {
        munmap(nmap, new_size);
        close(nfd);
        unlink(new_path);
        pthread_rwlock_unlock(&e->rwlock);
        return -1;
    }
```

Replace with:

```c
    if (rename(new_path, path) != 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_grow_to %s: rename(%s, %s) failed: %s", path, new_path, path, strerror(errno));
        munmap(nmap, new_size);
        close(nfd);
        unlink(new_path);
        pthread_rwlock_unlock(&e->rwlock);
        return -1;
    }
```

- [ ] **Step 10: `ucache_grow_shard` — add `LOG_ERROR` on corrupt in-memory state (`slots_per_shard == 0`)**

Locate (exact current text):

```c
    int slot = ucache_ensure(path, slot_size);
    if (slot < 0) return -1;
    uint32_t observed = g_ucache[slot].slots_per_shard;
    if (observed == 0) return -1;
    return ucache_grow_to(path, observed * 2, slot_size);
}
```

Replace with:

```c
    int slot = ucache_ensure(path, slot_size);
    if (slot < 0) return -1;
    uint32_t observed = g_ucache[slot].slots_per_shard;
    if (observed == 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_grow_shard %s: cache entry has slots_per_shard=0 (corrupt state)", path);
        return -1;
    }
    return ucache_grow_to(path, observed * 2, slot_size);
}
```

> **Note for the executing model:** `if (slot < 0) return -1;` here is pure propagation of `ucache_ensure`'s already-logged failure (Steps 1–4) — do not add a log there, only at the `observed == 0` corruption check.

- [ ] **Step 11: `resolve_counts` — add `LOG_ERROR` on `slotcask_registry_get` failure**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(eff_root, bare_obj, &info);
    if (!sdb) { *out_live = 0; *out_deleted = 0; return -1; }
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(eff_root, bare_obj, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "resolve_counts %s/%s: slotcask_registry_get failed", eff_root, bare_obj);
        *out_live = 0; *out_deleted = 0; return -1;
    }
```

- [ ] **Step 12: `counts_cache_get` — add `LOG_WARN` on counts-cache table full**

Locate (exact current text):

```c
        /* Cache full — fall back to direct file I/O via a NULL return.
           Callers handle this gracefully. */
        pthread_mutex_unlock(&g_counts_lock);
        return NULL;
    }
```

Replace with:

```c
        /* Cache full — fall back to direct file I/O via a NULL return.
           Callers handle this gracefully. */
        LOG_WARN(LOG_SUB_SLOTCASK, "counts_cache_get %s: cache full (COUNTS_CACHE_BUCKETS exhausted), falling back to direct file I/O", path);
        pthread_mutex_unlock(&g_counts_lock);
        return NULL;
    }
```

- [ ] **Step 13: `parse_multi_key` — add `LOG_ERROR` on `malloc` failure**

Locate (exact current text):

```c
    char *wire = malloc(klen + 1);
    if (!wire) return -1;
```

Replace with:

```c
    char *wire = malloc(klen + 1);
    if (!wire) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "parse_multi_key: malloc(%zu) failed for wire key buffer", klen + 1);
        return -1;
    }
```

- [ ] **Step 14: `multi_exists_shard_worker` — add `LOG_ERROR` at its 2 flagged sites (both mask real failures as "not found")**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) return NULL;

    SlotcaskBulkRec *batch = calloc(sw->count, sizeof(SlotcaskBulkRec));
    if (!batch) return NULL;
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_exists_shard_worker %s/%s: slotcask_registry_get failed, %d keys reported as not-found", sw->db_root, sw->object, sw->count);
        return NULL;
    }

    SlotcaskBulkRec *batch = calloc(sw->count, sizeof(SlotcaskBulkRec));
    if (!batch) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_exists_shard_worker %s/%s: calloc(%d) failed for batch, keys reported as not-found", sw->db_root, sw->object, sw->count);
        return NULL;
    }
```

- [ ] **Step 15: `multi_get_shard_worker` — add `LOG_ERROR` at its 2 flagged sites (both mask real failures as "missing")**

Locate (exact current text):

```c
    SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) return NULL;

    /* Extract pre-computed hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sw->count * sizeof(*hashes));
    if (!hashes) return NULL;
```

Replace with:

```c
    SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_get_shard_worker %s/%s: slotcask_registry_get failed, %d keys reported as missing", sw->db_root, sw->object, sw->count);
        return NULL;
    }

    /* Extract pre-computed hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sw->count * sizeof(*hashes));
    if (!hashes) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_get_shard_worker %s/%s: malloc(%d * 16) failed for hashes array", sw->db_root, sw->object, sw->count);
        return NULL;
    }
```

> **Note for the executing model:** Steps 14 and 15 share the identical `SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info); if (!sdb) return NULL;` shape in two different functions (`multi_exists_shard_worker`, `multi_get_shard_worker`) — apply each only inside its named function.

- [ ] **Step 16: Build**

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: clean build, no new warnings.

- [ ] **Step 17: Full suite**

```bash
./build/bin/shard-db-test run-all
```

Expected: identical pass/fail counts to the Task 1 baseline — no regressions, no new failures.

- [ ] **Step 18: Manual spot-check**

Force a real failure: create an object, then make its kf shard file (`data/kf/000.kf`) read-only or move it aside so a subsequent grow (`ucache_grow_to`, triggered by inserting past the 50% load-factor threshold) fails to `open()`/`ftruncate()` the `.new` grow-target file. Confirm the operation fails AND a matching `LOG_ERROR(LOG_SUB_SLOTCASK, "ucache_grow_to ...")` line appears in `$LOG_DIR`. Restore the file/permissions afterward.

- [ ] **Step 19: Leave uncommitted**

Do not run `git add`/`git commit`. Move to Task 8.

---

### Task 8: `src/db/config.c` — 20 sites (16 ERROR, 4 WARN)

**Files:**
- Modify: `src/db/config.c`

**Interfaces:** none (additive logging only). All sites use `LOG_SUB_CONFIG`.

- [ ] **Step 1: `dirs_add` — add `LOG_ERROR` on hash-table capacity exhaustion**

Locate (exact current text):

```c
    if (free_slot < 0) { pthread_mutex_unlock(&g_dirs_lock); return -1; }
```

Replace with:

```c
    if (free_slot < 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "dirs_add: no free slot for dir [%s]; DIRS_BUCKETS (%d) exhausted", dir, DIRS_BUCKETS);
        pthread_mutex_unlock(&g_dirs_lock); return -1;
    }
```

- [ ] **Step 2: `gen_uuid4_raw` — add `LOG_ERROR` on entropy-source failure**

Locate (exact current text):

```c
int gen_uuid4_raw(uint8_t out[16]) {
    if (fill_random(out, 16) != 0) {
        memset(out, 0, 16);   /* defense in depth — callers must check rc */
        return -1;
    }
```

Replace with:

```c
int gen_uuid4_raw(uint8_t out[16]) {
    if (fill_random(out, 16) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "gen_uuid4_raw: fill_random(16) failed: %s", strerror(errno));
        memset(out, 0, 16);   /* defense in depth — callers must check rc */
        return -1;
    }
```

- [ ] **Step 3: `gen_uuid4_batch` — add `LOG_ERROR` on entropy-source failure**

Locate (exact current text):

```c
int gen_uuid4_batch(uint8_t *out, size_t n) {
    if (n == 0) return 0;
    if (n > SIZE_MAX / 16) return -1;
    if (fill_random(out, n * 16) != 0) return -1;
```

Replace with:

```c
int gen_uuid4_batch(uint8_t *out, size_t n) {
    if (n == 0) return 0;
    if (n > SIZE_MAX / 16) return -1;
    if (fill_random(out, n * 16) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "gen_uuid4_batch: fill_random(%zu) failed: %s", n * 16, strerror(errno));
        return -1;
    }
```

> **Note for the executing model:** `if (n > SIZE_MAX / 16) return -1;` is SKIP-benign (caller-supplied overflow guard) — do not add a log there, only at the `fill_random` failure.

- [ ] **Step 4: `seq_next_val` — add `LOG_ERROR` on sequence lock-file open failure**

Locate (exact current text):

```c
    int lockfd = open(lock_path, O_RDWR | O_CREAT, 0644);
    if (lockfd < 0) return -1;
    flock(lockfd, LOCK_EX);

    long long val = 0;
    FILE *f = fopen(seq_path, "r");
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    val++;
    f = fopen(seq_path, "w");
```

Replace with:

```c
    int lockfd = open(lock_path, O_RDWR | O_CREAT, 0644);
    if (lockfd < 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "seq_next_val: open(%s): %s", lock_path, strerror(errno));
        return -1;
    }
    flock(lockfd, LOCK_EX);

    long long val = 0;
    FILE *f = fopen(seq_path, "r");
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    val++;
    f = fopen(seq_path, "w");
```

> **Note for the executing model:** `seq_next_val` and `seq_next_val_batch` (next step) both open `lock_path` with the identical `open(lock_path, O_RDWR | O_CREAT, 0644); if (lockfd < 0) return -1;` shape — apply this step's edit only to `seq_next_val` (the single-value function, no `n` parameter), and Step 5's edit only to `seq_next_val_batch` (takes an `n` parameter).

- [ ] **Step 5: `seq_next_val_batch` — add `LOG_ERROR` on sequence lock-file open failure and on corrupt/overflowing counter**

Locate (exact current text):

```c
    int lockfd = open(lock_path, O_RDWR | O_CREAT, 0644);
    if (lockfd < 0) return -1;
    flock(lockfd, LOCK_EX);

    long long val = 0;
    FILE *f = fopen(seq_path, "r");
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    if (val < 0 || (long long)n > LLONG_MAX - val - 1) {
        flock(lockfd, LOCK_UN);
        close(lockfd);
        return -1;
    }
```

Replace with:

```c
    int lockfd = open(lock_path, O_RDWR | O_CREAT, 0644);
    if (lockfd < 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "seq_next_val_batch: open(%s): %s", lock_path, strerror(errno));
        return -1;
    }
    flock(lockfd, LOCK_EX);

    long long val = 0;
    FILE *f = fopen(seq_path, "r");
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    if (val < 0 || (long long)n > LLONG_MAX - val - 1) {
        LOG_ERROR(LOG_SUB_CONFIG, "seq_next_val_batch: sequence [%s] corrupt or exhausted (val=%lld, n=%d)", seq_name, val, n);
        flock(lockfd, LOCK_UN);
        close(lockfd);
        return -1;
    }
```

> **Note for the executing model:** `if (n <= 0) return -1;` earlier in `seq_next_val_batch` (not shown in this anchor) is SKIP-benign (caller-supplied invalid batch size) — do not log it.

- [ ] **Step 6: `format_wire_key` — add `LOG_WARN` at all 3 branches (AK_UUID, AK_SEQ, AK_NONE)**

Locate (exact current text):

```c
    if (sc && sc->auto_key == AK_UUID) {
        if (klen != 16 || outcap < 37) return -1;
        format_uuid_string((const uint8_t *)key, out);
        return 36;
    }
    if (sc && sc->auto_key == AK_SEQ) {
        if (klen != 8 || outcap < 24) return -1;
        int64_t v = 0;
        for (int i = 0; i < 8; i++)
            v = (v << 8) | (uint8_t)key[i];
        format_seq_key(v, out);
        return (int)strlen(out);
    }
    /* AK_NONE: verbatim copy (key is a regular string). */
    if (klen + 1 > outcap) return -1;
    memcpy(out, key, klen);
    out[klen] = '\0';
    return (int)klen;
}
```

Replace with:

```c
    if (sc && sc->auto_key == AK_UUID) {
        if (klen != 16 || outcap < 37) {
            LOG_WARN(LOG_SUB_CONFIG, "format_wire_key: AK_UUID key length mismatch (klen=%zu, outcap=%zu)", klen, outcap);
            return -1;
        }
        format_uuid_string((const uint8_t *)key, out);
        return 36;
    }
    if (sc && sc->auto_key == AK_SEQ) {
        if (klen != 8 || outcap < 24) {
            LOG_WARN(LOG_SUB_CONFIG, "format_wire_key: AK_SEQ key length mismatch (klen=%zu, outcap=%zu)", klen, outcap);
            return -1;
        }
        int64_t v = 0;
        for (int i = 0; i < 8; i++)
            v = (v << 8) | (uint8_t)key[i];
        format_seq_key(v, out);
        return (int)strlen(out);
    }
    /* AK_NONE: verbatim copy (key is a regular string). */
    if (klen + 1 > outcap) {
        LOG_WARN(LOG_SUB_CONFIG, "format_wire_key: output buffer too small for key (klen=%zu, outcap=%zu)", klen, outcap);
        return -1;
    }
    memcpy(out, key, klen);
    out[klen] = '\0';
    return (int)klen;
}
```

- [ ] **Step 7: `decode_field_to_buf` (FT_VARCHAR case) — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
        if (len == 0) return 0;
        if (buflen < 4) return -1;  /* "" + NUL */
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3,
                                   (const char *)(data + 2), (size_t)len);
        if (esc < 0) return -1;
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';   /* caller renders with %s */
        return 2 + esc;
    }
    case FT_LONG: {
```

Replace with:

```c
        if (len == 0) return 0;
        if (buflen < 4) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: buffer too small for field [%s] (buflen=%d, need>=4)", f->name, buflen);
            return -1;  /* "" + NUL */
        }
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3,
                                   (const char *)(data + 2), (size_t)len);
        if (esc < 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: json_escape_into overflow for field [%s] (len=%d, buflen=%d)", f->name, len, buflen);
            return -1;
        }
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';   /* caller renders with %s */
        return 2 + esc;
    }
    case FT_LONG: {
```

- [ ] **Step 8: `decode_field_to_buf` (FT_ENUM case) — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
        /* Escape per RFC 8259 — enum value strings are user-supplied
           at create-object, may carry quotes/backslashes/control chars.
           Same buffer-sizing contract as FT_VARCHAR. */
        if (buflen < 4) return -1;
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3, s, strlen(s));
        if (esc < 0) return -1;
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    default:
        return 0;
    }
}
```

Replace with:

```c
        /* Escape per RFC 8259 — enum value strings are user-supplied
           at create-object, may carry quotes/backslashes/control chars.
           Same buffer-sizing contract as FT_VARCHAR. */
        if (buflen < 4) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: buffer too small for enum field [%s] (buflen=%d, need>=4)", f->name, buflen);
            return -1;
        }
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3, s, strlen(s));
        if (esc < 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: json_escape_into overflow for enum field [%s] value [%s]", f->name, s);
            return -1;
        }
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    default:
        return 0;
    }
}
```

> **Note for the executing model:** Steps 7 and 8 both contain the shape `if (buflen < 4) return -1;` followed by a `json_escape_into` call and `if (esc < 0) return -1;` — Step 7 is inside the `FT_VARCHAR` case (its buffer source is `data + 2`), Step 8 is inside the `FT_ENUM` case (its buffer source is `s`, a `const char *` enum value string, and it's preceded by the RFC 8259 comment). Apply each edit only to its matching case block.

- [ ] **Step 9: `typed_get_field_str` (FT_DATETIMEMS case) — add `LOG_ERROR` on `malloc(18)` failure**

Locate (exact current text):

```c
        char *out = malloc(18);
        if (!out) return NULL;
        snprintf(out, 18, "%08d%02d%02d%02d%03d", dv, hh, mm, ss, fff);
        return out;
    }
    case FT_TIME: {
```

Replace with:

```c
        char *out = malloc(18);
        if (!out) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: malloc(18) failed for field [%s]", f->name);
            return NULL;
        }
        snprintf(out, 18, "%08d%02d%02d%02d%03d", dv, hh, mm, ss, fff);
        return out;
    }
    case FT_TIME: {
```

- [ ] **Step 10: `typed_get_field_str` (FT_IPV4 case) — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
    case FT_IPV4: {
        const uint8_t *ip = src + f->offset;
        if (ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0)
            return NULL;
        char *out = malloc(INET_ADDRSTRLEN);
        if (!out) return NULL;
        if (!inet_ntop(AF_INET, ip, out, INET_ADDRSTRLEN)) {
            free(out);
            return NULL;
        }
        return out;
    }
    case FT_IPV6: {
        const uint8_t *ip = src + f->offset;
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (ip[bi] != 0) { allzero = 0; break; }
        if (allzero) return NULL;
        char *out = malloc(INET6_ADDRSTRLEN);
        if (!out) return NULL;
        if (!inet_ntop(AF_INET6, ip, out, INET6_ADDRSTRLEN)) {
            free(out);
            return NULL;
        }
        return out;
    }
```

Replace with:

```c
    case FT_IPV4: {
        const uint8_t *ip = src + f->offset;
        if (ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0)
            return NULL;
        char *out = malloc(INET_ADDRSTRLEN);
        if (!out) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: malloc(INET_ADDRSTRLEN) failed for field [%s]", f->name);
            return NULL;
        }
        if (!inet_ntop(AF_INET, ip, out, INET_ADDRSTRLEN)) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: inet_ntop(AF_INET) failed for field [%s]: %s", f->name, strerror(errno));
            free(out);
            return NULL;
        }
        return out;
    }
    case FT_IPV6: {
        const uint8_t *ip = src + f->offset;
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (ip[bi] != 0) { allzero = 0; break; }
        if (allzero) return NULL;
        char *out = malloc(INET6_ADDRSTRLEN);
        if (!out) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: malloc(INET6_ADDRSTRLEN) failed for field [%s]", f->name);
            return NULL;
        }
        if (!inet_ntop(AF_INET6, ip, out, INET6_ADDRSTRLEN)) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: inet_ntop(AF_INET6) failed for field [%s]: %s", f->name, strerror(errno));
            free(out);
            return NULL;
        }
        return out;
    }
```

> **Note for the executing model:** this single Locate/Replace block covers both the FT_IPV4 and FT_IPV6 cases (4 sites total: 2 `malloc` checks, 2 `inet_ntop` checks) in one contiguous span — apply it as one edit.

- [ ] **Step 11: `replace_tokens` — add `LOG_WARN` on output-buffer overflow**

Locate (exact current text):

```c
        if (pos + srclen + 2 >= outcap) return -1; /* overflow */
```

Replace with:

```c
        if (pos + srclen + 2 >= outcap) {
            LOG_WARN(LOG_SUB_CONFIG, "replace_tokens: output would overflow (in=[%s], old=[%s], new=[%s], outcap=%zu)", in, old_name, new_name, outcap);
            return -1; /* overflow */
        }
```

> **Note for the executing model:** `replace_tokens` takes its input string as a parameter — confirm the parameter's name in the live function signature (the audit drafted it as `in`) and use that exact name in the log call.

- [ ] **Step 12: `rename_indexes_for_field` — add `LOG_ERROR` on `index.conf.new` open failure**

Locate (exact current text):

```c
    FILE *nf = fopen(newconf_path, "w");
    if (!nf) { fclose(f); return -1; }
```

Replace with:

```c
    FILE *nf = fopen(newconf_path, "w");
    if (!nf) {
        LOG_ERROR(LOG_SUB_CONFIG, "rename_indexes_for_field: fopen(%s, \"w\"): %s", newconf_path, strerror(errno));
        fclose(f); return -1;
    }
```

- [ ] **Step 13: Build**

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: clean build, no new warnings.

- [ ] **Step 14: Full suite**

```bash
./build/bin/shard-db-test run-all
```

Expected: identical pass/fail counts to the Task 1 baseline — no regressions, no new failures.

- [ ] **Step 15: Manual spot-check**

Force a real failure: create an object with `"auto_key":"seq(myseq)"`, then make `metadata/sequences/myseq.lock` unreadable/unwritable (e.g. remove write perms on its parent dir) so `seq_next_val`'s `open(lock_path, ...)` fails on the next insert. Confirm the insert fails AND a matching `LOG_ERROR(LOG_SUB_CONFIG, "seq_next_val: open(...)")` line appears in `$LOG_DIR`. Restore permissions afterward.

- [ ] **Step 16: Leave uncommitted**

Do not run `git add`/`git commit`. Move to Task 9.

---

### Task 9: `src/db/btree.c` — 19 sites (16 ERROR, 3 WARN)

**Files:**
- Modify: `src/db/btree.c`

**Interfaces:** none (additive logging only). All sites use `LOG_SUB_BTREE`
(confirmed present in `src/db/log.h`, already used by `index.c`).

- [ ] **Step 1: `bt_cache_probe` — add `LOG_WARN` on cache table full**

Locate (exact current text):

```c
            *out_found = 1;
            return s;
        }
    }
    *out_found = 0;
    return -1;
}
```

Replace with:

```c
            *out_found = 1;
            return s;
        }
    }
    *out_found = 0;
    LOG_WARN(LOG_SUB_BTREE, "bt_cache_probe %s: table full after probing all %d slots, falling back to uncached mapping", path, bt_cache_slots);
    return -1;
}
```

> **Note for the executing model:** confirm `bt_cache_probe`'s path parameter name in the live signature (the audit drafted it as `path`) before using it in the log call.

- [ ] **Step 2: `bt_open_file` — add `LOG_ERROR`/`LOG_WARN` at its 6 flagged sites**

Locate (exact current text):

```c
        mkdirp(dirname_of(path));
        fd = open(path, O_RDWR | O_CREAT, 0644);
    } else {
        fd = open(path, O_RDWR);
    }
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) < 0) { close(fd); return -1; }

    int fresh = 0;
    size_t sz;
    if (st.st_size == 0) {
        if (!writer) { close(fd); return -1; }
        size_t init_size = (size_t)bt_page_size * 2;
        if (ftruncate(fd, init_size) < 0) { close(fd); return -1; }
        sz = init_size;
        fresh = 1;
    } else {
        sz = (size_t)st.st_size;
    }

    uint8_t *map = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) { close(fd); return -1; }
    madvise(map, sz, MADV_RANDOM);
```

Replace with:

```c
        mkdirp(dirname_of(path));
        fd = open(path, O_RDWR | O_CREAT, 0644);
    } else {
        fd = open(path, O_RDWR);
    }
    if (fd < 0) {
        LOG_ERROR(LOG_SUB_BTREE, "bt_open_file %s: open failed (writer=%d): %s", path, writer, strerror(errno));
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        LOG_ERROR(LOG_SUB_BTREE, "bt_open_file %s: fstat failed: %s", path, strerror(errno));
        close(fd); return -1;
    }

    int fresh = 0;
    size_t sz;
    if (st.st_size == 0) {
        if (!writer) {
            LOG_WARN(LOG_SUB_BTREE, "bt_open_file %s: reader found zero-size file (never initialized by a writer)", path);
            close(fd); return -1;
        }
        size_t init_size = (size_t)bt_page_size * 2;
        if (ftruncate(fd, init_size) < 0) {
            LOG_ERROR(LOG_SUB_BTREE, "bt_open_file %s: ftruncate to %zu failed: %s", path, init_size, strerror(errno));
            close(fd); return -1;
        }
        sz = init_size;
        fresh = 1;
    } else {
        sz = (size_t)st.st_size;
    }

    uint8_t *map = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        LOG_ERROR(LOG_SUB_BTREE, "bt_open_file %s: mmap(%zu) failed: %s", path, sz, strerror(errno));
        close(fd); return -1;
    }
    madvise(map, sz, MADV_RANDOM);
```

- [ ] **Step 3: `bt_open_file` — add `LOG_ERROR` alongside the existing `fprintf(stderr, ...)` on magic mismatch**

Locate (exact current text):

```c
            fprintf(stderr,
                "btree: rejecting %s format at %s — run ./migrate to reindex\n",
                which, path);
            munmap(map, sz);
            close(fd);
            return -1;
        }
    }
```

Replace with:

```c
            fprintf(stderr,
                "btree: rejecting %s format at %s — run ./migrate to reindex\n",
                which, path);
            LOG_ERROR(LOG_SUB_BTREE, "bt_open_file %s: rejecting %s format — run ./migrate to reindex", path, which);
            munmap(map, sz);
            close(fd);
            return -1;
        }
    }
```

- [ ] **Step 4: `leaf_rebuild` — add `LOG_ERROR` on page-overflow (records don't fit)**

Locate (exact current text):

```c
    size_t slots_end = sizeof(BtPageHeader) + (size_t)count * sizeof(uint16_t);
    if (slots_end + data_bytes > (size_t)bt_page_size) return -1;
```

Replace with:

```c
    size_t slots_end = sizeof(BtPageHeader) + (size_t)count * sizeof(uint16_t);
    if (slots_end + data_bytes > (size_t)bt_page_size) {
        LOG_ERROR(LOG_SUB_BTREE, "leaf_rebuild: %d records need %zu bytes, page holds %d (slots_end=%zu) — overflow", count, data_bytes, bt_page_size, slots_end);
        return -1;
    }
```

- [ ] **Step 5: `leaf_decode_value_at` — add `LOG_ERROR` on out-of-range `slot_idx` (detection site — do not duplicate at propagation call sites)**

Locate (exact current text):

```c
static int leaf_decode_value_at(uint8_t *page, int slot_idx,
                                char *out_buf, size_t *out_len) {
    BtPageHeader *ph = (BtPageHeader *)page;
    if (slot_idx < 0 || slot_idx >= (int)ph->count) return -1;
```

Replace with:

```c
static int leaf_decode_value_at(uint8_t *page, int slot_idx,
                                char *out_buf, size_t *out_len) {
    BtPageHeader *ph = (BtPageHeader *)page;
    if (slot_idx < 0 || slot_idx >= (int)ph->count) {
        LOG_ERROR(LOG_SUB_BTREE, "leaf_decode_value_at: slot_idx=%d out of range [0,%u)", slot_idx, ph->count);
        return -1;
    }
```

> **Note for the executing model:** do NOT add logging at any of `leaf_decode_value_at`'s call sites (in `page_insert_at_leaf`, at roughly 5 separate `if (leaf_decode_value_at(...) != 0) return -1;` spots) — those are pure propagation of this one detection point and would multiply a single failure into 5-6 near-identical log lines.

- [ ] **Step 6: `page_insert_at_leaf` — add `LOG_ERROR` on out-of-range `pos` (leave the adjacent `vlen` check untouched)**

Locate (exact current text):

```c
    if (pos < 0 || pos > N) return -1;
    if (vlen > BT_MAX_VAL_LEN) return -1;
```

Replace with:

```c
    if (pos < 0 || pos > N) {
        LOG_ERROR(LOG_SUB_BTREE, "page_insert_at_leaf: pos=%d out of range [0,%d]", pos, N);
        return -1;
    }
    if (vlen > BT_MAX_VAL_LEN) return -1;
```

> **Note for the executing model:** `if (vlen > BT_MAX_VAL_LEN) return -1;` is SKIP-benign (redundant defense-in-depth check — callers already reject oversized values before calling in) — do not add a log there.

- [ ] **Step 7: `page_insert_at_leaf` (anchor-crossing loop) — add `LOG_ERROR` on patch-array overflow**

Locate (exact current text):

```c
    for (int s = first_anchor; s <= N; s += K) {
        if (n + 2 > LPI_MAX_PATCHES) return -1; /* shouldn't trip at K=16 */
```

Replace with:

```c
    for (int s = first_anchor; s <= N; s += K) {
        if (n + 2 > LPI_MAX_PATCHES) {
            LOG_ERROR(LOG_SUB_BTREE, "page_insert_at_leaf: patch array overflow (n=%d, cap=%d) inserting at pos=%d", n, LPI_MAX_PATCHES, pos);
            return -1; /* shouldn't trip at K=16 */
        }
```

- [ ] **Step 8: `iter_init_desc_leaves` — add `LOG_ERROR` on `malloc` failure**

Locate (exact current text):

```c
    it->desc_leaves = malloc(sizeof(uint32_t));
    if (!it->desc_leaves) return -1;
```

Replace with:

```c
    it->desc_leaves = malloc(sizeof(uint32_t));
    if (!it->desc_leaves) {
        LOG_ERROR(LOG_SUB_BTREE, "iter_init_desc_leaves: malloc(%zu) failed for desc cursor", sizeof(uint32_t));
        return -1;
    }
```

- [ ] **Step 9: `btree_range_iter_open` — add `LOG_ERROR` on `calloc` failure (leave the `bt_acquire` propagation site untouched)**

Locate (exact current text):

```c
    BtRangeIter *it = calloc(1, sizeof(*it));
    if (!it) return NULL;
    if (bt_acquire(&it->bt, path, 0) != 0) { free(it); return NULL; }
```

Replace with:

```c
    BtRangeIter *it = calloc(1, sizeof(*it));
    if (!it) {
        LOG_ERROR(LOG_SUB_BTREE, "btree_range_iter_open %s: calloc(BtRangeIter) failed", path);
        return NULL;
    }
    if (bt_acquire(&it->bt, path, 0) != 0) { free(it); return NULL; }
```

> **Note for the executing model:** `if (bt_acquire(&it->bt, path, 0) != 0) { free(it); return NULL; }` is SKIP-benign here (propagation of `bt_open_file`'s already-logged failures via `bt_acquire`) — do not add a log there.

- [ ] **Step 10: `bt_stream_build_open` — add `LOG_ERROR` at its 2 flagged sites (leave the `bt_acquire` propagation untouched)**

Locate (exact current text):

```c
    BtStreamBuilder *b = calloc(1, sizeof(*b));
    if (!b) return NULL;
    if (bt_acquire(&b->bt, path, 1) != 0) { free(b); return NULL; }

    b->leaf_cap = 256;
    b->leaf_ids = malloc(b->leaf_cap * sizeof(uint32_t));
    if (!b->leaf_ids) { bt_release(&b->bt); free(b); return NULL; }
```

Replace with:

```c
    BtStreamBuilder *b = calloc(1, sizeof(*b));
    if (!b) {
        LOG_ERROR(LOG_SUB_BTREE, "bt_stream_build_open %s: calloc(BtStreamBuilder) failed", path);
        return NULL;
    }
    if (bt_acquire(&b->bt, path, 1) != 0) { free(b); return NULL; }

    b->leaf_cap = 256;
    b->leaf_ids = malloc(b->leaf_cap * sizeof(uint32_t));
    if (!b->leaf_ids) {
        LOG_ERROR(LOG_SUB_BTREE, "bt_stream_build_open %s: malloc(leaf_ids, cap=%zu) failed", path, b->leaf_cap);
        bt_release(&b->bt); free(b); return NULL;
    }
```

- [ ] **Step 11: `bt_stream_build_add` — add `LOG_ERROR` on `realloc` failure growing the leaf-id array**

Locate (exact current text):

```c
        b->cur_leaf = new_leaf;
        if (b->leaf_count >= b->leaf_cap) {
            size_t new_cap = b->leaf_cap * 2;
            uint32_t *t = realloc(b->leaf_ids, new_cap * sizeof(uint32_t));
            if (!t) { b->fatal = 1; return -1; }
            b->leaf_ids = t;
            b->leaf_cap = new_cap;
        }
```

Replace with:

```c
        b->cur_leaf = new_leaf;
        if (b->leaf_count >= b->leaf_cap) {
            size_t new_cap = b->leaf_cap * 2;
            uint32_t *t = realloc(b->leaf_ids, new_cap * sizeof(uint32_t));
            if (!t) {
                LOG_ERROR(LOG_SUB_BTREE, "bt_stream_build_add: realloc(leaf_ids, cap=%zu) failed — builder marked fatal", new_cap);
                b->fatal = 1; return -1;
            }
            b->leaf_ids = t;
            b->leaf_cap = new_cap;
        }
```

- [ ] **Step 12: `bt_stream_build_finish` — add `LOG_ERROR` on `malloc` failure building an internal-node level**

Locate (exact current text):

```c
        uint32_t *parent_ids = malloc(parent_cap * sizeof(uint32_t));
        if (!parent_ids) {
            if (child_ids != b->leaf_ids) free(child_ids);
            bt_release(&b->bt);
            free(b->leaf_ids);
            free(b);
            return -1;
        }
```

Replace with:

```c
        uint32_t *parent_ids = malloc(parent_cap * sizeof(uint32_t));
        if (!parent_ids) {
            LOG_ERROR(LOG_SUB_BTREE, "bt_stream_build_finish: malloc(parent_ids, cap=%zu) failed", parent_cap);
            if (child_ids != b->leaf_ids) free(child_ids);
            bt_release(&b->bt);
            free(b->leaf_ids);
            free(b);
            return -1;
        }
```

- [ ] **Step 13: `bt_extract_all` — add `LOG_ERROR` on extraction-buffer `malloc` failure**

Locate (exact current text):

```c
    BtEntry *entries = malloc(cap * sizeof(BtEntry));
    if (!entries) { bt_release(&bt); return NULL; }
```

Replace with:

```c
    BtEntry *entries = malloc(cap * sizeof(BtEntry));
    if (!entries) {
        LOG_ERROR(LOG_SUB_BTREE, "bt_extract_all %s: malloc(entries, cap=%zu) failed", path, cap);
        bt_release(&bt); return NULL;
    }
```

- [ ] **Step 14: `bt_merge_lock_for` — add `LOG_WARN` on merge-lock table exhaustion**

Locate (exact current text):

```c
            pthread_mutex_unlock(&g_bt_merge_table_lock);
            return &g_bt_merge_locks[slot].mutex;
        }
    }
    pthread_mutex_unlock(&g_bt_merge_table_lock);
    return NULL;
}
```

Replace with:

```c
            pthread_mutex_unlock(&g_bt_merge_table_lock);
            return &g_bt_merge_locks[slot].mutex;
        }
    }
    pthread_mutex_unlock(&g_bt_merge_table_lock);
    LOG_WARN(LOG_SUB_BTREE, "bt_merge_lock_for %s: all %d merge-lock buckets in use — proceeding WITHOUT per-path serialization", path, BT_MERGE_LOCK_BUCKETS);
    return NULL;
}
```

> **Note for the executing model:** confirm `bt_merge_lock_for`'s path parameter name in the live signature before using it in the log call.

- [ ] **Step 15: Build**

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: clean build, no new warnings.

- [ ] **Step 16: Full suite**

```bash
./build/bin/shard-db-test run-all
```

Expected: identical pass/fail counts to the Task 1 baseline — no regressions, no new failures.

- [ ] **Step 17: Manual spot-check**

Force a real failure: create an indexed object, then make its index shard file (`<obj>/indexes/<field>/000.idx`) read-only or corrupt its magic bytes so `bt_open_file` rejects it. Confirm the operation fails AND a matching `LOG_ERROR(LOG_SUB_BTREE, "bt_open_file ...")` line appears in `$LOG_DIR`. Restore the file afterward.

- [ ] **Step 18: Leave uncommitted**

Do not run `git add`/`git commit`. Move to Task 10.

---

### Task 10: `src/db/keyset.c` + `src/db/objlock.c` + `src/db/util.c` — 16 sites (14 ERROR, 2 WARN)

**Files:**
- Modify: `src/db/keyset.c`
- Modify: `src/db/objlock.c`
- Modify: `src/db/util.c`

**Interfaces:** none (additive logging only).

**Constraint — `util.c` fuzz-harness linkage:** `util.c` is linked standalone
(no `config.c`, no `log_msg_sub`/`log_audit_sub` symbols) into two libFuzzer
harnesses, `fuzz/fuzz_json.c` and `fuzz/fuzz_b64.c` (see `fuzz/build.sh`).
Any `LOG_ERROR`/`LOG_WARN` call added anywhere in `util.c` — even in a
function the fuzzer never calls at runtime — produces an **undefined
reference to `log_msg_sub`** at link time for those two harnesses, because
the symbol reference exists in the compiled object regardless of whether
the code path executes. `util.c`'s existing `mkdirp` function already
documents and follows the correct pattern: emit to `fprintf(stderr, ...)`
instead of `LOG_*`, with a comment explaining why. All new `util.c` sites in
this task follow that same `fprintf(stderr, ...)` pattern — do not use
`LOG_ERROR`/`LOG_WARN` in `util.c`. `keyset.c` and `objlock.c` have no such
constraint (they aren't linked into any fuzz harness) and use normal
`LOG_*` calls.

- [ ] **Step 1: `keyset.c` `keyset_new` — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
KeySet *keyset_new(size_t capacity_hint) {
    size_t cap = next_pow2(capacity_hint * 2);
    KeySet *ks = calloc(1, sizeof(KeySet));
    if (!ks) return NULL;
    ks->cap = cap;
    ks->mask = cap - 1;
    ks->keys  = calloc(cap, sizeof(*ks->keys));
    ks->state = calloc(cap, sizeof(*ks->state));
    atomic_init(&ks->n, 0);
    if (!ks->keys || !ks->state) {
        free(ks->keys); free(ks->state); free(ks);
        return NULL;
    }
    return ks;
}
```

Replace with:

```c
KeySet *keyset_new(size_t capacity_hint) {
    size_t cap = next_pow2(capacity_hint * 2);
    KeySet *ks = calloc(1, sizeof(KeySet));
    if (!ks) {
        LOG_ERROR(LOG_SUB_PLANNER, "keyset_new: calloc failed for KeySet struct (capacity_hint=%zu)", capacity_hint);
        return NULL;
    }
    ks->cap = cap;
    ks->mask = cap - 1;
    ks->keys  = calloc(cap, sizeof(*ks->keys));
    ks->state = calloc(cap, sizeof(*ks->state));
    atomic_init(&ks->n, 0);
    if (!ks->keys || !ks->state) {
        LOG_ERROR(LOG_SUB_PLANNER, "keyset_new: calloc failed for keys/state arrays (cap=%zu)", cap);
        free(ks->keys); free(ks->state); free(ks);
        return NULL;
    }
    return ks;
}
```

- [ ] **Step 2: `keyset.c` `keyset_insert` — add `LOG_WARN` on hash table full**

Locate (exact current text):

```c
        /* Collision — linear probe. */
    }
    return -1; /* full */
}
```

Replace with:

```c
        /* Collision — linear probe. */
    }
    LOG_WARN(LOG_SUB_PLANNER, "keyset_insert: hash table full (cap=%zu), falling back to full scan", ks->cap);
    return -1; /* full */
}
```

> **Note for the executing model:** `if (!ks) return -1;` at the top of `keyset_insert` is SKIP-benign (defensive param guard, symptom of an already-logged upstream `keyset_new` failure) — do not add a log there.

- [ ] **Step 3: `objlock.c` `get_lock` — add `LOG_ERROR` on lock table exhaustion**

Locate (exact current text):

```c
            atomic_store_explicit(&g_objlocks[slot].used, 1, memory_order_release);
            pthread_mutex_unlock(&g_objlock_table_lock);
            return &g_objlocks[slot].rwlock;
        }
    }
    pthread_mutex_unlock(&g_objlock_table_lock);
    return NULL;
}
```

Replace with:

```c
            atomic_store_explicit(&g_objlocks[slot].used, 1, memory_order_release);
            pthread_mutex_unlock(&g_objlock_table_lock);
            return &g_objlocks[slot].rwlock;
        }
    }
    pthread_mutex_unlock(&g_objlock_table_lock);
    LOG_ERROR(LOG_SUB_SERVER, "objlock get_lock: table full (%d buckets), object '%s' will run WITHOUT rwlock protection", OBJLOCK_BUCKETS, key);
    return NULL;
}
```

> **Note for the executing model:** all four callers (`objlock_rdlock`/`objlock_rdunlock`/`objlock_wrlock`/`objlock_wrunlock`) do `if (l) pthread_rwlock_*(l);` — on `NULL` they silently skip locking entirely, so this is a real concurrency-correctness hazard (unprotected concurrent writes/rebuild), not just a diagnostic nicety. `key` is the local 512-byte buffer built earlier in `get_lock` from `db_root`/`object` — confirm its exact name in the live function body before using it in the log call.

- [ ] **Step 4: `util.c` `fill_random` — add `fprintf(stderr, ...)` at its 2 flagged sites (NOT `LOG_*` — see Constraint above)**

Locate (exact current text):

```c
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    off = 0;
    while (off < n) {
        ssize_t r = read(fd, p + off, n - off);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) { close(fd); return -1; }
        off += (size_t)r;
    }
    close(fd);
```

Replace with:

```c
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        fprintf(stderr, "fill_random: getentropy exhausted and open(/dev/urandom) failed: %s\n", strerror(errno));
        return -1;
    }
    off = 0;
    while (off < n) {
        ssize_t r = read(fd, p + off, n - off);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) {
            fprintf(stderr, "fill_random: read(/dev/urandom) failed or hit EOF: %s\n", r < 0 ? strerror(errno) : "EOF");
            close(fd); return -1;
        }
        off += (size_t)r;
    }
    close(fd);
```

- [ ] **Step 5: `util.c` `read_file` — add `fprintf(stderr, ...)` at its 4 flagged sites (NOT `LOG_*`)**

Locate (exact current text):

```c
char *read_file(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return NULL; }
    long len = ftell(f);
    if (len < 0) { fclose(f); return NULL; }
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)len + 1);
    if (!buf) { fclose(f); return NULL; }
```

Replace with:

```c
char *read_file(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) {
        fprintf(stderr, "read_file: fseek(SEEK_END) failed on '%s': %s\n", path, strerror(errno));
        fclose(f); return NULL;
    }
    long len = ftell(f);
    if (len < 0) {
        fprintf(stderr, "read_file: ftell failed on '%s': %s\n", path, strerror(errno));
        fclose(f); return NULL;
    }
    if (fseek(f, 0, SEEK_SET) != 0) {
        fprintf(stderr, "read_file: fseek(SEEK_SET) failed on '%s': %s\n", path, strerror(errno));
        fclose(f); return NULL;
    }
    char *buf = malloc((size_t)len + 1);
    if (!buf) {
        fprintf(stderr, "read_file: malloc(%zu) failed for '%s'\n", (size_t)len + 1, path);
        fclose(f); return NULL;
    }
```

> **Note for the executing model:** `if (!f) return NULL;` (the `fopen` failure) is SKIP-benign — the file may legitimately not exist (e.g. probing for an optional config file) — do not add a log there, only at the 4 sites shown above.

- [ ] **Step 6: `util.c` `json_parse_object` — add `fprintf(stderr, ...)` on field-count overflow (NOT `LOG_*`)**

Locate (exact current text):

```c
        size_t vlen = p - val_start;
        if (out->n >= JSON_OBJ_MAX_FIELDS) {
            /* Too many fields for our fixed-size bucket. Abort; caller can
               still fall back to the legacy per-field walker if this ever
               fires in practice. */
            return -1;
        }
```

Replace with:

```c
        size_t vlen = p - val_start;
        if (out->n >= JSON_OBJ_MAX_FIELDS) {
            /* Too many fields for our fixed-size bucket. Abort; caller can
               still fall back to the legacy per-field walker if this ever
               fires in practice. */
            fprintf(stderr, "json_parse_object: field count exceeds JSON_OBJ_MAX_FIELDS=%d, aborting parse\n", JSON_OBJ_MAX_FIELDS);
            return -1;
        }
```

> **Note for the executing model:** this site is directly reachable by any oversized client JSON payload the server receives, not just the fuzzer — it stays `fprintf(stderr, ...)` regardless, per the file-wide `util.c` constraint, since the symbol-linkage issue applies to the whole translation unit.

- [ ] **Step 7: `util.c` `json_unescape_string` — add `fprintf(stderr, ...)` on `malloc` failure (NOT `LOG_*`)**

Locate (exact current text):

```c
    if (!out_buf || !out_len) return -1;
    /* Worst case: every escape decodes to fewer bytes than its source,
       so in_len + 1 is a safe upper bound for the output buffer. */
    char *out = malloc(in_len + 1);
    if (!out) return -1;
```

Replace with:

```c
    if (!out_buf || !out_len) return -1;
    /* Worst case: every escape decodes to fewer bytes than its source,
       so in_len + 1 is a safe upper bound for the output buffer. */
    char *out = malloc(in_len + 1);
    if (!out) {
        fprintf(stderr, "json_unescape_string: malloc(%zu) failed\n", in_len + 1);
        return -1;
    }
```

> **Note for the executing model:** `if (!out_buf || !out_len) return -1;` is SKIP-benign (defensive param guard) — do not log it.

- [ ] **Step 8: `util.c` `json_obj_strdup` — add `fprintf(stderr, ...)` on `malloc` failure (NOT `LOG_*`)**

Locate (exact current text):

```c
char *json_obj_strdup(const JsonObj *o, const char *key) {
    const char *v; size_t vl;
    if (!json_obj_unquoted(o, key, &v, &vl)) return NULL;
    char *s = malloc(vl + 1);
    if (!s) return NULL;
    memcpy(s, v, vl); s[vl] = '\0';
    return s;
}
```

Replace with:

```c
char *json_obj_strdup(const JsonObj *o, const char *key) {
    const char *v; size_t vl;
    if (!json_obj_unquoted(o, key, &v, &vl)) return NULL;
    char *s = malloc(vl + 1);
    if (!s) {
        fprintf(stderr, "json_obj_strdup: malloc(%zu) failed for key '%s'\n", vl + 1, key);
        return NULL;
    }
    memcpy(s, v, vl); s[vl] = '\0';
    return s;
}
```

- [ ] **Step 9: `util.c` `json_obj_strdup_raw` — add `fprintf(stderr, ...)` on `malloc` failure (NOT `LOG_*`)**

Locate (exact current text):

```c
char *json_obj_strdup_raw(const JsonObj *o, const char *key) {
    const char *v; size_t vl;
    if (!json_obj_get(o, key, &v, &vl)) return NULL;
    char *s = malloc(vl + 1);
    if (!s) return NULL;
    memcpy(s, v, vl); s[vl] = '\0';
    return s;
}
```

Replace with:

```c
char *json_obj_strdup_raw(const JsonObj *o, const char *key) {
    const char *v; size_t vl;
    if (!json_obj_get(o, key, &v, &vl)) return NULL;
    char *s = malloc(vl + 1);
    if (!s) {
        fprintf(stderr, "json_obj_strdup_raw: malloc(%zu) failed for key '%s'\n", vl + 1, key);
        return NULL;
    }
    memcpy(s, v, vl); s[vl] = '\0';
    return s;
}
```

> **Note for the executing model:** Steps 8 and 9 both contain the identical shape `char *s = malloc(vl + 1); if (!s) return NULL;` in two different functions (`json_obj_strdup`, `json_obj_strdup_raw`) — apply each edit only inside its named function.

- [ ] **Step 10: `util.c` `json_obj_string_or_array` — add `fprintf(stderr, ...)` at its 2 flagged sites (NOT `LOG_*`)**

Locate (exact current text):

```c
    if (!json_obj_get(o, key, &v, &vl) || vl == 0) return NULL;

    /* Plain string: strip surrounding quotes if present. */
    if (vl >= 2 && v[0] == '"' && v[vl - 1] == '"') {
        char *out = malloc(vl - 1);
        if (!out) return NULL;
        memcpy(out, v + 1, vl - 2);
        out[vl - 2] = '\0';
        return out;
    }
    if (v[0] != '[') {
        char *out = malloc(vl + 1);
        if (!out) return NULL;
        memcpy(out, v, vl); out[vl] = '\0';
        return out;
    }
```

Replace with:

```c
    if (!json_obj_get(o, key, &v, &vl) || vl == 0) return NULL;

    /* Plain string: strip surrounding quotes if present. */
    if (vl >= 2 && v[0] == '"' && v[vl - 1] == '"') {
        char *out = malloc(vl - 1);
        if (!out) {
            fprintf(stderr, "json_obj_string_or_array: malloc(%zu) failed for key '%s'\n", vl - 1, key);
            return NULL;
        }
        memcpy(out, v + 1, vl - 2);
        out[vl - 2] = '\0';
        return out;
    }
    if (v[0] != '[') {
        char *out = malloc(vl + 1);
        if (!out) {
            fprintf(stderr, "json_obj_string_or_array: malloc(%zu) failed for key '%s'\n", vl + 1, key);
            return NULL;
        }
        memcpy(out, v, vl); out[vl] = '\0';
        return out;
    }
```

- [ ] **Step 11: Build**

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: clean build, no new warnings. Also rebuild the fuzz harnesses to confirm no link errors were introduced:

```bash
cd fuzz && ./build.sh && cd ..
```

Expected: `fuzz_json` and `fuzz_b64` both link successfully (no `undefined reference to log_msg_sub`).

- [ ] **Step 12: Full suite**

```bash
./build/bin/shard-db-test run-all
```

Expected: identical pass/fail counts to the Task 1 baseline — no regressions, no new failures.

- [ ] **Step 13: Manual spot-check**

Force a real failure: send a `find`/`count` query with a criteria JSON containing more than `JSON_OBJ_MAX_FIELDS` top-level fields, so `json_parse_object` hits its overflow guard. Confirm the query fails AND a matching `json_parse_object: field count exceeds JSON_OBJ_MAX_FIELDS=...` line appears on the server's stderr (this one is intentionally `fprintf(stderr, ...)`, not `$LOG_DIR` — per the Constraint above). Separately, for the `keyset.c`/`objlock.c` sites (which do use `LOG_*`), force a lock-table-full condition is impractical to trigger manually at `OBJLOCK_BUCKETS` scale — code-review the `get_lock` diff instead to confirm the `LOG_ERROR` call compiles and is reachable.

- [ ] **Step 14: Leave uncommitted**

Do not run `git add`/`git commit`. Move to Task 11.

---

### Task 11: `src/db/query_plan.c` + `src/db/query_maint.c` — 12 sites (12 ERROR)

**Files:**
- Modify: `src/db/query_maint.c`
- Modify: `src/db/query_plan.c`

**Interfaces:** none (additive logging only). `query_maint.c` sites use
`LOG_SUB_CONFIG` (matches the existing convention for on-disk config/data-file
I/O failures during schema/maintenance ops elsewhere in the codebase).
`query_plan.c` sites use `LOG_SUB_QUERY` (these are all OOM failures while
parsing a single request's criteria JSON into a `CriteriaNode` tree —
request-scoped, not index/plan-selection logic).

**Note on scope:** `copy_file`'s callers (`cprf`, `cmd_backup`, `cmd_restore`)
discard its `int` return value entirely — that caller-side bug is explicitly
**out of scope** for this task (see Global Constraints). This task only adds
logging *inside* `copy_file` at its two `open()` failure sites, so the
failure is at least visible in `$LOG_DIR` even though the caller doesn't
act on the return code.

- [ ] **Step 1: `query_maint.c` `copy_file` — add `LOG_ERROR` at its 2 flagged sites**

Locate (exact current text):

```c
static int copy_file(const char *src, const char *dst, mode_t mode) {
    int sfd = open(src, O_RDONLY);
    if (sfd < 0) return -1;
    int dfd = open(dst, O_WRONLY | O_CREAT | O_TRUNC, mode & 0777);
    if (dfd < 0) { close(sfd); return -1; }
```

Replace with:

```c
static int copy_file(const char *src, const char *dst, mode_t mode) {
    int sfd = open(src, O_RDONLY);
    if (sfd < 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "copy_file: open(%s) failed: %s", src, strerror(errno));
        return -1;
    }
    int dfd = open(dst, O_WRONLY | O_CREAT | O_TRUNC, mode & 0777);
    if (dfd < 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "copy_file: open(%s) failed: %s", dst, strerror(errno));
        close(sfd); return -1;
    }
```

- [ ] **Step 2: `query_maint.c` `ensure_schema_conf_line` — add `LOG_ERROR` on `schema.conf` open failure**

Locate (exact current text):

```c
    FILE *f = fopen(conf, "a+");
    if (!f) return -1;
    int lockfd = fileno(f);
    flock(lockfd, LOCK_EX);
```

Replace with:

```c
    FILE *f = fopen(conf, "a+");
    if (!f) {
        LOG_ERROR(LOG_SUB_CONFIG, "ensure_schema_conf_line: fopen(%s) failed: %s", conf, strerror(errno));
        return -1;
    }
    int lockfd = fileno(f);
    flock(lockfd, LOCK_EX);
```

- [ ] **Step 3: `query_plan.c` `cnode_append` — add `LOG_ERROR` on `realloc` failure**

Locate (exact current text):

```c
static int cnode_append(CriteriaNode *parent, CriteriaNode *child) {
    CriteriaNode **nc = realloc(parent->children,
                                (parent->n_children + 1) * sizeof(CriteriaNode *));
    if (!nc) return -1;
```

Replace with:

```c
static int cnode_append(CriteriaNode *parent, CriteriaNode *child) {
    CriteriaNode **nc = realloc(parent->children,
                                (parent->n_children + 1) * sizeof(CriteriaNode *));
    if (!nc) {
        LOG_ERROR(LOG_SUB_QUERY, "cnode_append: realloc(%zu bytes) failed", (parent->n_children + 1) * sizeof(CriteriaNode *));
        return -1;
    }
```

- [ ] **Step 4: `query_plan.c` `parse_tree_array` — add `LOG_ERROR` on element-buffer `malloc` failure**

Locate (exact current text):

```c
        const char *obj_start = p;
        const char *obj_end = json_skip_value(p);
        size_t obj_len = obj_end - obj_start;
        char *obj_buf = malloc(obj_len + 1);
        if (!obj_buf) { if (err) *err = "out of memory"; return -1; }
```

Replace with:

```c
        const char *obj_start = p;
        const char *obj_end = json_skip_value(p);
        size_t obj_len = obj_end - obj_start;
        char *obj_buf = malloc(obj_len + 1);
        if (!obj_buf) {
            LOG_ERROR(LOG_SUB_QUERY, "parse_tree_array: malloc(%zu) failed", obj_len + 1);
            if (err) *err = "out of memory"; return -1;
        }
```

- [ ] **Step 5: `query_plan.c` `parse_tree_element` — add `LOG_ERROR` at its 2 `cnode_new` OOM sites**

Locate (exact current text):

```c
        free(or_arr); free(and_arr);
        return n;
    }

    CriteriaNode *n = cnode_new(CNODE_LEAF);
    if (!n) { if (err) *err = "out of memory"; return NULL; }
    if (parse_one_criterion(obj_buf, &n->leaf) != 0) {
```

Replace with:

```c
        free(or_arr); free(and_arr);
        return n;
    }

    CriteriaNode *n = cnode_new(CNODE_LEAF);
    if (!n) {
        LOG_ERROR(LOG_SUB_QUERY, "parse_tree_element: calloc(sizeof(CriteriaNode)) failed for leaf");
        if (err) *err = "out of memory"; return NULL;
    }
    if (parse_one_criterion(obj_buf, &n->leaf) != 0) {
```

> **Note for the executing model:** `parse_tree_element` has an earlier `CriteriaNode *n = cnode_new(or_arr ? CNODE_OR : CNODE_AND);` branch (for the `or`/`and` case, not shown in this anchor — it's above the `free(or_arr); free(and_arr); return n;` line) with its own `if (!n) { if (err) *err = "out of memory"; return NULL; }` check. Locate that branch separately and add:
> ```c
> LOG_ERROR(LOG_SUB_QUERY, "parse_tree_element: calloc(sizeof(CriteriaNode)) failed for %s node", or_arr ? "OR" : "AND");
> ```
> as its first statement, matching the same pattern as the leaf case above. This gives 2 total log call additions in this function.

- [ ] **Step 6: `query_plan.c` `parse_criteria_tree` (array-form root) — add `LOG_ERROR` on `cnode_new(CNODE_AND)` OOM**

Locate (exact current text):

```c
    if (*p == '[') {
        CriteriaNode *root = cnode_new(CNODE_AND);
        if (!root) { if (err) *err = "out of memory"; return NULL; }
        if (parse_tree_array(p, root, 0, err) != 0) {
```

Replace with:

```c
    if (*p == '[') {
        CriteriaNode *root = cnode_new(CNODE_AND);
        if (!root) {
            LOG_ERROR(LOG_SUB_QUERY, "parse_criteria_tree: calloc(sizeof(CriteriaNode)) failed for AND root");
            if (err) *err = "out of memory"; return NULL;
        }
        if (parse_tree_array(p, root, 0, err) != 0) {
```

> **Note for the executing model:** `parse_criteria_tree` has **three** distinct `CriteriaNode *root = cnode_new(CNODE_AND);` / `CriteriaNode *n = cnode_new(...)` call sites across its array-form, object-form, and simple-equality-form branches — this step, Step 7, and Step 8 each target a different one. Match by the surrounding code shown in each Locate block, not just the `cnode_new` call text, which repeats.

- [ ] **Step 7: `query_plan.c` `parse_criteria_tree` (object-form `or`/`and`, and single-leaf-field forms) — add `LOG_ERROR` at both OOM sites**

Locate (exact current text):

```c
        if (or_arr || and_arr) {
            CriteriaNode *n = cnode_new(or_arr ? CNODE_OR : CNODE_AND);
            if (!n) { free(or_arr); free(and_arr); if (err) *err = "out of memory"; return NULL; }
            if (parse_tree_array(or_arr ? or_arr : and_arr, n, 0, err) != 0) {
```

Replace with:

```c
        if (or_arr || and_arr) {
            CriteriaNode *n = cnode_new(or_arr ? CNODE_OR : CNODE_AND);
            if (!n) {
                LOG_ERROR(LOG_SUB_QUERY, "parse_criteria_tree: calloc(sizeof(CriteriaNode)) failed for %s node", or_arr ? "OR" : "AND");
                free(or_arr); free(and_arr); if (err) *err = "out of memory"; return NULL;
            }
            if (parse_tree_array(or_arr ? or_arr : and_arr, n, 0, err) != 0) {
```

Then locate (exact current text):

```c
        const char *field_v; size_t field_vl;
        if (json_obj_get(&pobj, "field", &field_v, &field_vl)) {
            CriteriaNode *n = cnode_new(CNODE_LEAF);
            if (!n) { if (err) *err = "out of memory"; return NULL; }
            if (parse_one_criterion(p, &n->leaf) != 0) {
```

Replace with:

```c
        const char *field_v; size_t field_vl;
        if (json_obj_get(&pobj, "field", &field_v, &field_vl)) {
            CriteriaNode *n = cnode_new(CNODE_LEAF);
            if (!n) {
                LOG_ERROR(LOG_SUB_QUERY, "parse_criteria_tree: calloc(sizeof(CriteriaNode)) failed for leaf");
                if (err) *err = "out of memory"; return NULL;
            }
            if (parse_one_criterion(p, &n->leaf) != 0) {
```

- [ ] **Step 8: `query_plan.c` `parse_criteria_tree` (simple-equality-form root and per-pair leaf) — add `LOG_ERROR` at both OOM sites**

Locate (exact current text):

```c
        /* Simple-equality form `{"k1":"v1","k2":"v2"}` — backward compat.
           Parse k:v pairs as EQ leaves under an implicit AND root. */
        CriteriaNode *root = cnode_new(CNODE_AND);
        if (!root) { if (err) *err = "out of memory"; return NULL; }
        p++;
```

Replace with:

```c
        /* Simple-equality form `{"k1":"v1","k2":"v2"}` — backward compat.
           Parse k:v pairs as EQ leaves under an implicit AND root. */
        CriteriaNode *root = cnode_new(CNODE_AND);
        if (!root) {
            LOG_ERROR(LOG_SUB_QUERY, "parse_criteria_tree: calloc(sizeof(CriteriaNode)) failed for AND root (simple-equality form)");
            if (err) *err = "out of memory"; return NULL;
        }
        p++;
```

Then locate (exact current text):

```c
            CriteriaNode *leaf = cnode_new(CNODE_LEAF);
            if (!leaf) { free_criteria_tree(root); if (err) *err = "out of memory"; return NULL; }
            if (flen > 255) flen = 255;
```

Replace with:

```c
            CriteriaNode *leaf = cnode_new(CNODE_LEAF);
            if (!leaf) {
                LOG_ERROR(LOG_SUB_QUERY, "parse_criteria_tree: calloc(sizeof(CriteriaNode)) failed for simple-equality leaf");
                free_criteria_tree(root); if (err) *err = "out of memory"; return NULL;
            }
            if (flen > 255) flen = 255;
```

> **Note for the executing model:** Steps 6, 7, and 8 together touch **6** distinct `cnode_new(...)` OOM checks spread across `parse_criteria_tree`'s four branches (array-form, object-form-or/and, object-form-single-leaf, simple-equality-form-root, simple-equality-form-per-pair-leaf). Each Locate block's surrounding code (the comment above it, the sibling variable names, the branch it's nested in) is unique — use that surrounding context to confirm you're editing the right one, since the `if (!X) { ... "out of memory" ... return NULL; }` shape repeats almost verbatim across all of them.

- [ ] **Step 9: Build**

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: clean build, no new warnings.

- [ ] **Step 10: Full suite**

```bash
./build/bin/shard-db-test run-all
```

Expected: identical pass/fail counts to the Task 1 baseline — no regressions, no new failures.

- [ ] **Step 11: Manual spot-check**

Force a real failure: run `./shard-db backup <dir> <obj>` (or trigger `cmd_restore`) against an object whose `metadata/fields.conf` source file has its read permission removed, so `copy_file`'s `open(src, O_RDONLY)` fails. Confirm the backup/restore completes with a truncated/incomplete copy AND a matching `LOG_ERROR(LOG_SUB_CONFIG, "copy_file: open(...) failed")` line appears in `$LOG_DIR`. Restore permissions afterward.

- [ ] **Step 12: Leave uncommitted**

Do not run `git add`/`git commit`. Move to the final gate below.

---

## Final gate (after all 11 tasks)

- [ ] **Final Step 1: Full suite, one more time**

```bash
./build/bin/shard-db-test run-all
```

Expected: identical pass/fail counts to the Task 1 baseline. This is a
last cumulative check across all 11 tasks' changes together, on top of
the per-task runs already done.

- [ ] **Final Step 2: Inventory the added log calls, per file**

A single global magic number is not reliable here and must NOT be used:
Task 1's TLS call sites are flagged per-call-site in the findings report,
but Step 5 routes them all through one shared `tls_log_err` helper, so the
literal `LOG_ERROR(`/`LOG_WARN(` text appears far fewer times in the diff
than the flagged-site count would suggest. Task 1 also adds 4 `LOG_ERROR`
calls in `server.c` (the TLS preflight checks, Step 1) that are net-new
relative to the 251-site findings count, since they didn't exist as silent
sites in the original audit — they're a byproduct of the `log_init()`
reorder, not a flagged-and-fixed site. Counting per file, and reconciling
each file against its own task's step list in this plan, avoids both
distortions.

```bash
git diff --unified=0 -- src/db | grep -o '^diff --git a/\(src/db/[a-zA-Z_]*\.c\)' | sed 's#^diff --git a/##' | sort -u | while read -r f; do
    n=$(git diff --unified=0 -- "$f" | grep -c '^\+.*\(LOG_ERROR\|LOG_WARN\)(')
    echo "$f: $n"
done
git diff --unified=0 -- src/db/util.c | grep -c '^\+.*fprintf(stderr'
```

Expected shape, not exact totals: every file touched by Tasks 2–11 (all
except `tls.c`) should show a `LOG_ERROR`/`LOG_WARN` count reasonably close
to that task's own "N sites" figure in its task header — walk each file's
count against the corresponding task's numbered steps in this plan and
confirm every step that adds a `LOG_ERROR`/`LOG_WARN` call is accounted
for; a handful of sites sharing one wrapper function (as already noted for
`tls_log_err`, and possible elsewhere — check each task's steps for a
"shared helper" pattern) will legitimately produce fewer literal
occurrences than sites. `server.c`'s count should be Task 2's per-step
total **plus 4** (the Task 1 Step 1 TLS preflight `LOG_ERROR`s). `tls.c`'s
count reflects Step 5's single `tls_log_err` definition (2 literal
`LOG_ERROR`/`LOG_WARN` calls inside the helper) rather than the number of
call sites that route through it. The `util.c` `fprintf(stderr, ...)`
count should be **12** (the pre-existing `mkdirp` call is unchanged by
this plan, so it won't show up as an added `+` line).

If a file's count looks wrong after walking it against that file's task
steps (a step was skipped, or a call was written with different macro
text than the plan specified), write `PLAN_NOTES.md` naming the specific
file, task, and step and stop. Do not write `PLAN_NOTES.md` over a raw
number mismatch alone without first doing that per-step walk — the whole
point of this per-file breakdown is that raw counts are expected to
deviate from the findings report's per-site tallies in known, explainable
ways.

- [ ] **Final Step 3: Leave the tree dirty**

Do not run `git add`/`git commit`/`git push`. The plan is now fully
executed. A human (Sonnet) reviews the raw `git diff` from here — per
this repo's `CLAUDE.md` execution-mode exception, that review, plus the
human's explicit go-ahead, are required before anything is committed.

---
