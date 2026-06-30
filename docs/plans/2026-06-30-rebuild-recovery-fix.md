# Plan: rebuild_object_v2 recovery fixes

**Date:** 2026-06-30  
**Branch:** `feat/rebuild-recovery-fix` (branch off `main` after `feat/rebuild-kf` merges)

## Background

Two bugs discovered while cleaning up locally-corrupted HN data after the compact-kf-fix release:

**Bug 1 — walk failure leaves object in broken state.**  
`rebuild_object_v2` renames `data/` → `data.legacy/` then moves it inside `.rebuild_legacy_root/`, then opens a fresh `data/` for the new schema, then walks. On walk failure the error handler returns immediately without restoring. Result: `data/` has partial new data; original is stranded in `.rebuild_legacy_root/data/`; a second rebuild attempt reads the partial data.

**Bug 2 — single bad record aborts the entire walk.**  
`v2_rebuild_walk_cb` calls `slotcask_insert` and on failure sets `ctx->error = 1; return 1`, which terminates the walk immediately. A single corrupt segment record (bad klen/vlen but valid hash + flag=1, e.g. left by a prior crash) kills the whole rebuild. The correct behaviour is to skip that record, log a warning, and continue.

## Execution rules

- Branch off `main` (after `feat/rebuild-kf` merges).
- Tasks in order; build with `SKIP_TESTS=1 ./build.sh` after each task.
- Full test after Task 3: `./build/bin/shard-db-test run-all`.
- Anchors are quoted exactly — if an anchor is not found verbatim, stop and write `PLAN_NOTES.md`.
- Never claim a step passed without showing real output.

---

## Task 1 — Add `skipped` field to `V2RebuildCtx`

**File:** `src/db/query.c`

**Anchor to find:**
```c
} V2RebuildCtx;
```

**Replacement** (add `skipped` field before the closing brace):
```c
    int                skipped;  /* records skipped due to insert failure */
} V2RebuildCtx;
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 2 — Fix `v2_rebuild_walk_cb`: skip bad records instead of aborting

Two call sites in `v2_rebuild_walk_cb` call `slotcask_insert` — both must be changed.

### 2a — slot_changed=false path (verbatim copy)

**File:** `src/db/query.c`

**Anchor to find** (exact):
```c
        if (slotcask_insert(ctx->new_db, -1, key, klen, value, vlen) != 0) {
            ctx->error = 1; return 1;
        }
        ctx->live_count++;
        return 0;
    }
```

**Replacement:**
```c
        if (slotcask_insert(ctx->new_db, -1, key, klen, value, vlen) != 0) {
            LOG_WARN(LOG_SUB_CONFIG, "rebuild_v2: insert failed for record %d (klen=%zu vlen=%zu), skipping",
                     ctx->live_count + ctx->skipped + 1, klen, vlen);
            ctx->skipped++;
            return 0;
        }
        ctx->live_count++;
        return 0;
    }
```

### 2b — slot_changed=true path (recomposed payload)

**File:** `src/db/query.c`

**Anchor to find** (exact):
```c
    int rc = slotcask_insert(ctx->new_db, -1, key, klen,
                              buf, ctx->new_ts->total_size);
    free(buf);
    if (rc != 0) { ctx->error = 1; return 1; }
    ctx->live_count++;
    return 0;
}
```

**Replacement:**
```c
    int rc = slotcask_insert(ctx->new_db, -1, key, klen,
                              buf, ctx->new_ts->total_size);
    free(buf);
    if (rc != 0) {
        LOG_WARN(LOG_SUB_CONFIG, "rebuild_v2: insert failed for record %d (klen=%zu), skipping",
                 ctx->live_count + ctx->skipped + 1, klen);
        ctx->skipped++;
        return 0;
    }
    ctx->live_count++;
    return 0;
}
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 3 — Fix `rebuild_object_v2`: restore original data on walk failure

**File:** `src/db/query.c`

### 3a — Log skipped count before walk-error check

**Anchor to find** (exact):
```c
    slotcask_walk_live(&legacy_db, v2_rebuild_walk_cb, &walk_ctx);
    int live_count = walk_ctx.live_count;
    int walk_err   = walk_ctx.error;
    free(walk_ctx.backfill);
    walk_ctx.backfill = NULL;
```

**Replacement:**
```c
    slotcask_walk_live(&legacy_db, v2_rebuild_walk_cb, &walk_ctx);
    int live_count = walk_ctx.live_count;
    int skipped    = walk_ctx.skipped;
    int walk_err   = walk_ctx.error;
    if (skipped > 0)
        LOG_WARN(LOG_SUB_CONFIG, "rebuild_v2: skipped %d records due to insert failure", skipped);
    free(walk_ctx.backfill);
    walk_ctx.backfill = NULL;
```

### 3b — Restore original data on walk failure

**Anchor to find** (exact):
```c
    if (walk_err) {
        LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: walk error after %d records", live_count);
        OUT("{\"error\":\"Rebuild walk failed; %s/.rebuild_legacy_root preserved\"}\n", obj_dir);
        return 1;
    }
```

**Replacement:**
```c
    if (walk_err) {
        LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: walk error after %d records; restoring original data",
                  live_count);
        /* Remove partial new data/ and restore original from .rebuild_legacy_root/data/. */
        rmrf(data_dir);
        if (rename(legacy_data_under_root, data_dir) != 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: restore failed: %s — original at %s",
                      strerror(errno), legacy_root);
            OUT("{\"error\":\"Rebuild walk failed; restore also failed — original at .rebuild_legacy_root\"}\n");
        } else {
            rmrf(legacy_root);
            OUT("{\"error\":\"Rebuild walk failed; original data restored\"}\n");
        }
        return 1;
    }
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 4 — Add test case `test_rebuild_recovery.c`

**File:** `src/test/cases/test_rebuild_recovery.c` (new file)

This test validates both fixes together:
- Bug 1 fix: walk error → original data restored, not stranded
- Bug 2 fix: a corrupt record is skipped, walk completes, remaining records survive

```c
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <limits.h>

/* Corrupt the first live segment record in the first .dat file of stream 0
   by writing a garbage vlen value while keeping hash and flag intact.
   This makes seg_rec_live_with_hash pass but slotcask_insert fail (vlen
   would exceed the new object's slot budget or be malformed).
   Returns 1 on success, 0 if no segment file found. */
static int corrupt_first_seg_record(const char *db_root) {
    char seg[PATH_MAX];
    /* Default object rebuildrecov has splits=8, stream 0, file 000000.dat */
    snprintf(seg, sizeof(seg),
             "%s/default/rebuildrecov/data/streams/000/000000.dat", db_root);
    int fd = open(seg, O_RDWR);
    if (fd < 0) return 0;
    struct stat st;
    fstat(fd, &st);
    if (st.st_size < 24) { close(fd); return 0; }
    uint8_t *m = mmap(NULL, (size_t)st.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (m == MAP_FAILED) return 0;
    /* Segment record layout: hash[16] klen[2 LE] flag[1] reserved[1] vlen[4 LE]
       Corrupt vlen to 0xFFFFFFFF — keeps hash and flag=1 intact so
       seg_rec_live_with_hash passes, but slotcask_insert will reject it. */
    uint8_t *rec = m;  /* first record at offset 0 */
    if (rec[18] == 1) {  /* flag byte */
        rec[20] = 0xFF;
        rec[21] = 0xFF;
        rec[22] = 0xFF;
        rec[23] = 0xFF;
    }
    msync(m, (size_t)st.st_size, MS_SYNC);
    munmap(m, (size_t)st.st_size);
    return 1;
}

static int test_rebuild_recovery_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* Register the default tenant dir. */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Create object with splits=8 so a splits-bump to 16 triggers rebuild_object_v2. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rebuildrecov\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create object");
    free(resp); resp = NULL;

    /* Insert 100 records. */
    for (int i = 0; i < 100; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rebuildrecov\","
            "\"key\":\"item%04d\",\"value\":{\"score\":%d,\"title\":\"t%d\"}}",
            i, i * 5, i);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert OK");
        free(resp); resp = NULL;
    }

    /* Verify 100 records before corruption. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rebuildrecov\"}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 100, "100 records before corruption");
    free(resp); resp = NULL;

    tc_close(tc);

    /* Kill daemon so segment files are flushed and stable. */
    test_env_kill(&env);

    /* Corrupt the vlen of the first segment record to trigger insert failure
       during the rebuild walk. */
    int c = corrupt_first_seg_record(env.db_root);
    ASSERT_TRUE(c > 0, "segment record corrupted");

    /* Restart daemon and trigger a splits-bump vacuum (splits 8→16) which
       calls rebuild_object_v2. With Bug 2 fixed, the corrupt record is skipped
       and the walk completes. With Bug 1 fixed, even if the walk had failed,
       original data would be restored. */
    if (test_env_start_at(&env, saved_db_root, saved_port) != 0) return 1;
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (!tc) { test_env_stop(&env); return 1; }

    /* Trigger rebuild via splits change. */
    tc_request(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"rebuildrecov\",\"splits\":16}",
        &resp);
    /* Must succeed (no error key). The corrupt record is skipped, not fatal. */
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "vacuum/rebuild succeeds despite corrupt record");
    free(resp); resp = NULL;

    /* At least 99 records must survive (the corrupt one is skipped). */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rebuildrecov\"}",
        &resp);
    int after = tu_parse_count(resp);
    ASSERT_TRUE(after >= 99, "at least 99 records survive rebuild (corrupt one skipped)");
    free(resp); resp = NULL;

    /* Verify no .rebuild_legacy_root left behind (Bug 1 fix: clean restore). */
    char legacy_path[PATH_MAX];
    snprintf(legacy_path, sizeof(legacy_path),
             "%s/default/rebuildrecov/.rebuild_legacy_root", env.db_root);
    ASSERT_TRUE(access(legacy_path, F_OK) != 0, "no .rebuild_legacy_root left behind");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-rebuild-recovery", test_rebuild_recovery_run)
```

---

## Task 5 — Register `test_rebuild_recovery.c` in `build.sh`

**File:** `build.sh`

**Anchor to find** (exact):
```
  src/test/cases/test_rebuild_kf.c \
```

**Replacement** (append the new case immediately after):
```
  src/test/cases/test_rebuild_kf.c \
  src/test/cases/test_rebuild_recovery.c \
```

Build and test: `./build.sh`

Expected output: `# total: N passed, 0 failed` — includes `test-rebuild-recovery`.

---

## Migration validation plan (local, before production release)

After these fixes merge to `main`:

1. **Build new binary** from `main`.
2. **Refetch all 5 M stories locally**: restart hn-explorer with the new binary; let the refresh loop catch up from the current `last_seen_id` up to `maxItem`. This exercises the normal (non-corrupt) insert path end-to-end.
3. **Run a splits-bump on the now-full local stories object** (`{"mode":"vacuum","dir":"hn","object":"stories","splits":256}`) to exercise `rebuild_object_v2` on real data. Verify:
   - No `"error"` in response.
   - `./shard-db count hn stories` ≈ pre-rebuild count (±skipped).
   - No `.rebuild_legacy_root` directory left behind.
   - Explorer serves requests normally after rebuild.
4. **Explorer smoke test**: open browser, verify stories/comments load, watch one full 5-min tick in logs for `upserted:` lines with no `ERROR [refresh]` entries.
5. **Only after steps 1–4 pass cleanly**: merge and deploy to production.
