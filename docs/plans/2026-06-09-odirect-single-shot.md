# Plan: O_DIRECT single-shot fast path + raise default buffer to 32 MB

**Date:** 2026-06-09
**Branch:** `feat/odirect-single-shot`

## Problem

At splits=256 / 25M records each shard is ~14 MB. With the current 4 MB
default buffer, every `seg_scan_o_direct*` call performs:
- 4 pread calls per shard (14 MB / 4 MB)
- 1 `pthread_create` + `pthread_join` per shard
- 2 × 4 MB buffer allocations (8 MB per scan)

The prefetch worker is useful when a file spans multiple buffers. For a 14 MB
shard with a 32 MB buffer the whole file lands in buf[0] and the worker has
nothing to do — it exits immediately after the kickoff signals EOF. Spawning
the thread is pure overhead at that point.

## Fix

Two coordinated changes:

1. **Raise `ODIRECT_BUF_SIZE_DEFAULT` from 4 MB to 32 MB.** Matches the
   design comment in the header ("32 MB, reduced syscall rate on modern NVMe").
   At splits≤256 every shard now fits in one read.

2. **Single-shot path in `DbCtx`**: when the file fits in buf[0] (i.e.
   `file_size ≤ odirect_buf_size`), skip buf[1] allocation, skip
   `pthread_create`/`dbctx_kickoff`, and make `dbctx_swap` return 0
   immediately. The main scan loop is unchanged — the inner `goto next_chunk`
   path calls `dbctx_swap` once; single-shot returns 0 (EOF) so the loop
   exits cleanly. `dbctx_destroy` skips `pthread_join` when single-shot.

   Memory per scan: `round_up(file_size, 4096)` bytes (≈14 MB for a 14 MB
   shard) instead of 2 × 32 MB = 64 MB.

Applied to `seg_scan_o_direct` and `seg_scan_o_direct_match`.
`btree_leaf_scan_o_direct` is left unchanged (btree files can exceed 32 MB).

## Execution rules

- Branch off `main`: `git checkout -b feat/odirect-single-shot`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Never claim a step passed without showing the real output.
- If a quoted anchor is not found exactly, stop and write `PLAN_NOTES.md`.

---

## Task 1 — `src/db/io_direct.h`: raise default buffer size

### Anchor:
```c
#define ODIRECT_BUF_SIZE_DEFAULT  (4 * 1024 * 1024)   /* 4 MB fallback          */
```

### Replacement:
```c
#define ODIRECT_BUF_SIZE_DEFAULT  (32 * 1024 * 1024)  /* 32 MB: fits one shard at splits≤256 */
```

---

## Task 2 — `src/db/io_direct.c`: add `single_shot` field to `DbCtx`

### Anchor:
```c
    int               err;           /* errno from worker I/O failure          */
} DbCtx;
```

### Replacement:
```c
    int               err;           /* errno from worker I/O failure          */
    int               single_shot;   /* 1 = file fits in buf[0]; no thread     */
} DbCtx;
```

---

## Task 3 — `src/db/io_direct.c`: add `single_shot` parameter to `dbctx_init`

### Anchor (entire function):
```c
static int dbctx_init(DbCtx *c, int fd, off_t file_size)
{
    memset(c, 0, sizeof(*c));
    c->fd        = fd;
    c->file_size = file_size;
    c->active    = 0;
    c->inactive  = 1;
    c->state     = DBS_IDLE;

    c->buf[0] = od_alloc_buf();
    c->buf[1] = od_alloc_buf();
    if (!c->buf[0] || !c->buf[1]) {
        free(c->buf[0]); free(c->buf[1]);
        return -ENOMEM;
    }

    pthread_mutex_init(&c->lock, NULL);
    pthread_cond_init(&c->prefetch_needed, NULL);
    pthread_cond_init(&c->prefetch_done,   NULL);

    /* Fill buf[0] synchronously. */
    off_t  fsz  = file_size;
    size_t want = (fsz >= (off_t)odirect_buf_size) ? odirect_buf_size : (size_t)fsz;
    size_t wanta = (want + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
    if (wanta > odirect_buf_size) wanta = odirect_buf_size;

    ssize_t got = pread(fd, c->buf[0], wanta, 0);
    if (got < 0) {
        int e = errno;
        free(c->buf[0]); free(c->buf[1]);
        pthread_mutex_destroy(&c->lock);
        pthread_cond_destroy(&c->prefetch_needed);
        pthread_cond_destroy(&c->prefetch_done);
        return -e;
    }
    c->active_len = got;
    c->next_off   = (off_t)got;    /* worker starts from here */
    return 0;
}
```

### Replacement:
```c
static int dbctx_init(DbCtx *c, int fd, off_t file_size, int single_shot)
{
    memset(c, 0, sizeof(*c));
    c->fd          = fd;
    c->file_size   = file_size;
    c->active      = 0;
    c->inactive    = 1;
    c->state       = DBS_IDLE;
    c->single_shot = single_shot;

    if (single_shot) {
        /* Allocate exactly what we need — no second buffer. */
        size_t exact = ((size_t)file_size + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (posix_memalign(&c->buf[0], ODIRECT_ALIGN, exact) != 0)
            return -ENOMEM;
        c->buf[1] = NULL;
    } else {
        c->buf[0] = od_alloc_buf();
        c->buf[1] = od_alloc_buf();
        if (!c->buf[0] || !c->buf[1]) {
            free(c->buf[0]); free(c->buf[1]);
            return -ENOMEM;
        }
    }

    pthread_mutex_init(&c->lock, NULL);
    pthread_cond_init(&c->prefetch_needed, NULL);
    pthread_cond_init(&c->prefetch_done,   NULL);

    /* Fill buf[0] synchronously. */
    off_t  fsz  = file_size;
    size_t wanta;
    if (single_shot) {
        wanta = ((size_t)fsz + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
    } else {
        size_t want = (fsz >= (off_t)odirect_buf_size) ? odirect_buf_size : (size_t)fsz;
        wanta = (want + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (wanta > odirect_buf_size) wanta = odirect_buf_size;
    }

    ssize_t got = pread(fd, c->buf[0], wanta, 0);
    if (got < 0) {
        int e = errno;
        free(c->buf[0]); free(c->buf[1]);
        pthread_mutex_destroy(&c->lock);
        pthread_cond_destroy(&c->prefetch_needed);
        pthread_cond_destroy(&c->prefetch_done);
        return -e;
    }
    c->active_len = got;
    c->next_off   = (off_t)got;
    return 0;
}
```

---

## Task 4 — `src/db/io_direct.c`: `dbctx_swap` — return 0 immediately when single_shot

### Anchor:
```c
static ssize_t dbctx_swap(DbCtx *c)
{
    pthread_mutex_lock(&c->lock);
```

### Replacement:
```c
static ssize_t dbctx_swap(DbCtx *c)
{
    /* Single-shot: whole file was in buf[0]; no worker, no more data. */
    if (c->single_shot) return 0;

    pthread_mutex_lock(&c->lock);
```

---

## Task 5 — `src/db/io_direct.c`: `dbctx_destroy` — skip pthread_join when single_shot

### Anchor:
```c
static void dbctx_destroy(DbCtx *c, pthread_t worker_tid)
{
    pthread_mutex_lock(&c->lock);
    c->worker_quit  = 1;
    c->work_pending = 1;   /* ensure worker unblocks from its wait */
```

### Replacement:
```c
static void dbctx_destroy(DbCtx *c, pthread_t worker_tid)
{
    if (!c->single_shot) {
        pthread_mutex_lock(&c->lock);
        c->worker_quit  = 1;
        c->work_pending = 1;   /* ensure worker unblocks from its wait */
```

Now find the end of `dbctx_destroy`. The function ends with:

### Anchor (closing of dbctx_destroy — find the lines that free buffers and destroy condvars):
```c
    pthread_join(worker_tid, NULL);
    free(c->buf[0]); free(c->buf[1]);
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->prefetch_needed);
    pthread_cond_destroy(&c->prefetch_done);
}
```

### Replacement:
```c
    pthread_join(worker_tid, NULL);
    } /* end if (!single_shot) */
    free(c->buf[0]); free(c->buf[1]); /* buf[1] is NULL for single_shot — free(NULL) is safe */
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->prefetch_needed);
    pthread_cond_destroy(&c->prefetch_done);
}
```

---

## Task 6 — `src/db/io_direct.c`: `seg_scan_o_direct` — single-shot fast path

### Anchor (the stat + early-open block at the top of the function):
```c
    struct stat st;
    if (stat(seg_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;
    if (file_size == 0) return 0;

    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size);
```

### Replacement:
```c
    if (odirect_buf_size == 0) odirect_init_buf_size();

    struct stat st;
    if (stat(seg_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;
    if (file_size == 0) return 0;

    /* Single-shot when the shard fits in one read — no prefetch thread needed. */
    int single_shot = (file_size <= (off_t)odirect_buf_size);

    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size, single_shot);
```

### Anchor (pthread_create block in seg_scan_o_direct):
```c
    pthread_t worker_tid;
    if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
        int e = errno;
        free(carry);
        free(dc.buf[0]); free(dc.buf[1]);
        pthread_mutex_destroy(&dc.lock);
        pthread_cond_destroy(&dc.prefetch_needed);
        pthread_cond_destroy(&dc.prefetch_done);
        close(fd);
        return -e;
    }

    /* Kick off the first async prefetch of buf[1]. */
    dbctx_kickoff(&dc);
```

### Replacement:
```c
    pthread_t worker_tid = (pthread_t)0;
    if (!single_shot) {
        if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
            int e = errno;
            free(carry);
            free(dc.buf[0]); free(dc.buf[1]);
            pthread_mutex_destroy(&dc.lock);
            pthread_cond_destroy(&dc.prefetch_needed);
            pthread_cond_destroy(&dc.prefetch_done);
            close(fd);
            return -e;
        }
        /* Kick off the first async prefetch of buf[1]. */
        dbctx_kickoff(&dc);
    }
```

---

## Task 7 — `src/db/io_direct.c`: `seg_scan_o_direct_match` — single-shot fast path

### Anchor (top of seg_scan_o_direct_match, stat + open block):
```c
    struct stat st;
    if (stat(seg_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;
    if (file_size == 0) { *out_count = 0; return 0; }
    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size);
```

### Replacement:
```c
    if (odirect_buf_size == 0) odirect_init_buf_size();

    struct stat st;
    if (stat(seg_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;
    if (file_size == 0) { *out_count = 0; return 0; }

    int single_shot = (file_size <= (off_t)odirect_buf_size);

    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size, single_shot);
```

### Anchor (pthread_create block in seg_scan_o_direct_match):
```c
    pthread_t worker_tid;
    if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
        int e = errno;
        free(carry);
        free(dc.buf[0]); free(dc.buf[1]);
        pthread_mutex_destroy(&dc.lock);
        pthread_cond_destroy(&dc.prefetch_needed);
        pthread_cond_destroy(&dc.prefetch_done);
        close(fd);
        return -e;
    }
    dbctx_kickoff(&dc);
```

### Replacement:
```c
    pthread_t worker_tid = (pthread_t)0;
    if (!single_shot) {
        if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
            int e = errno;
            free(carry);
            free(dc.buf[0]); free(dc.buf[1]);
            pthread_mutex_destroy(&dc.lock);
            pthread_cond_destroy(&dc.prefetch_needed);
            pthread_cond_destroy(&dc.prefetch_done);
            close(fd);
            return -e;
        }
        dbctx_kickoff(&dc);
    }
```

---

## Task 8 — Build

```
SKIP_TESTS=1 ./build.sh
```

Expected: clean compile, no warnings.

---

## Task 9 — New test: `src/test/cases/test_odirect_single_shot.c`

Verifies that O_DIRECT scans return correct results (no data loss/corruption
from the single-shot path) on objects of various record counts.

```c
/* src/test/cases/test_odirect_single_shot.c
 * Correctness test for the O_DIRECT single-shot fast path:
 * full scans on non-indexed fields must return the right results
 * regardless of whether the shard fits in one buffer.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int count_json_key(const char *resp, const char *key) {
    if (!resp) return 0;
    int n = 0; const char *p = resp;
    while ((p = strstr(p, key))) { n++; p += strlen(key); }
    return n;
}

static int test_odirect_single_shot_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"oss\"}", &resp);
    free(resp); resp = NULL;

    /* Object with non-indexed int field. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"oss\",\"object\":\"items\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[{\"name\":\"score\",\"type\":\"int\"},{\"name\":\"tag\",\"type\":\"varchar\",\"size\":8}]}",
        &resp); free(resp); resp = NULL;

    /* Insert 200 records: score 1..200, tag "even" or "odd". */
    for (int i = 1; i <= 200; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"oss\",\"object\":\"items\","
            "\"key\":\"k%03d\",\"value\":{\"score\":%d,\"tag\":\"%s\"}}",
            i, i, (i % 2 == 0) ? "even" : "odd");
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* Full scan: count all records. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"oss\",\"object\":\"items\"}",
        &resp);
    ASSERT_CONTAINS(resp, "200", "count == 200");
    free(resp); resp = NULL;

    /* Non-indexed filter: find all even scores (100 records). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"oss\",\"object\":\"items\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"even\"}],"
        "\"limit\":200}",
        &resp);
    ASSERT_NOT_NULL(resp, "find even response");
    {
        int n = count_json_key(resp, "\"key\":\"k");
        ASSERT_TRUE(n == 100, "find tag=even returns 100 records");
    }
    free(resp); resp = NULL;

    /* Non-indexed range: score > 150 (50 records). */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"oss\",\"object\":\"items\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"150\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "50", "count score>150 == 50");
    free(resp); resp = NULL;

    /* Verify no records are duplicated: find all, check total. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"oss\",\"object\":\"items\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"gte\",\"value\":\"1\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "200", "full criteria scan == 200");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-odirect-single-shot", test_odirect_single_shot_run)
```

---

## Task 10 — Run full test suite

```
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` where N ≥ previous count + new assertions.

Leave all changes **uncommitted**. Report the exact test output line.
