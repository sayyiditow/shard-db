# Plan: Fix idx cache key collision across tenants

**Date:** 2026-06-09
**Branch:** `feat/idx-cache-tenant-key`
**Goal:** `IdxCache` currently keys entries by `object` name only. Two tenants with
an identically-named object share one idx cache entry, so the second tenant to
load sees the first's index list. `invalidate_idx_cache` is similarly blind to
the tenant.

## Execution rules

- Branch off `main`: `git checkout -b feat/idx-cache-tenant-key`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Never claim a step passed without showing the real output.
- If a quoted anchor is not found exactly, stop and write `PLAN_NOTES.md`.

---

## Task 1 — `src/db/config.c`: widen `IdxCache.name` to 512 bytes

### Anchor:
```c
struct IdxCache {
    char name[256];
```

### Replacement:
```c
struct IdxCache {
    char name[512];
```

---

## Task 2 — `src/db/config.c`: key `idx_cache_ensure` on `db_root:object`

### Anchor (entire function body, lines ~1060–1072):
```c
static int idx_cache_ensure(const char *db_root, const char *object) {
    uint32_t hash = str_hash(object);

    pthread_mutex_lock(&g_idx_lock);
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (hash + i) % IDX_BUCKETS;
        if (!g_idx_cache[slot].used) break;
        if (strcmp(g_idx_cache[slot].name, object) == 0) {
            pthread_mutex_unlock(&g_idx_lock);
            return slot;
        }
    }
    pthread_mutex_unlock(&g_idx_lock);
```

### Replacement:
```c
static int idx_cache_ensure(const char *db_root, const char *object) {
    char key[512];
    snprintf(key, sizeof(key), "%s:%s", db_root, object);
    uint32_t hash = str_hash(key);

    pthread_mutex_lock(&g_idx_lock);
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (hash + i) % IDX_BUCKETS;
        if (!g_idx_cache[slot].used) break;
        if (strcmp(g_idx_cache[slot].name, key) == 0) {
            pthread_mutex_unlock(&g_idx_lock);
            return slot;
        }
    }
    pthread_mutex_unlock(&g_idx_lock);
```

---

## Task 3 — `src/db/config.c`: fix slow-path insert in `idx_cache_ensure`

### Anchor:
```c
    pthread_mutex_lock(&g_idx_lock);
    int return_slot = -1;
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (hash + i) % IDX_BUCKETS;
        if (g_idx_cache[slot].used && strcmp(g_idx_cache[slot].name, object) == 0) {
            return_slot = slot;
            break;
        }
        if (!g_idx_cache[slot].used) {
            strncpy(g_idx_cache[slot].name, object, 255);
            g_idx_cache[slot].name[255] = '\0';
```

### Replacement:
```c
    pthread_mutex_lock(&g_idx_lock);
    int return_slot = -1;
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (hash + i) % IDX_BUCKETS;
        if (g_idx_cache[slot].used && strcmp(g_idx_cache[slot].name, key) == 0) {
            return_slot = slot;
            break;
        }
        if (!g_idx_cache[slot].used) {
            strncpy(g_idx_cache[slot].name, key, 511);
            g_idx_cache[slot].name[511] = '\0';
```

---

## Task 4 — `src/db/config.c`: add `db_root` parameter to `invalidate_idx_cache`

### Anchor:
```c
void invalidate_idx_cache(const char *object) {
    uint32_t idx = str_hash(object) % IDX_BUCKETS;
    pthread_mutex_lock(&g_idx_lock);
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (idx + i) % IDX_BUCKETS;
        if (!g_idx_cache[slot].used) break;
        if (strcmp(g_idx_cache[slot].name, object) == 0) {
            g_idx_cache[slot].used = 0;
            break;
        }
    }
    pthread_mutex_unlock(&g_idx_lock);
}
```

### Replacement:
```c
void invalidate_idx_cache(const char *db_root, const char *object) {
    char key[512];
    snprintf(key, sizeof(key), "%s:%s", db_root, object);
    uint32_t idx = str_hash(key) % IDX_BUCKETS;
    pthread_mutex_lock(&g_idx_lock);
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (idx + i) % IDX_BUCKETS;
        if (!g_idx_cache[slot].used) break;
        if (strcmp(g_idx_cache[slot].name, key) == 0) {
            g_idx_cache[slot].used = 0;
            break;
        }
    }
    pthread_mutex_unlock(&g_idx_lock);
}
```

---

## Task 5 — `src/db/types.h`: update declaration

### Anchor:
```c
void invalidate_idx_cache(const char *object);
```

### Replacement:
```c
void invalidate_idx_cache(const char *db_root, const char *object);
```

---

## Task 6 — `src/db/index.c`: update 4 call sites

### Site A — after `fprintf(af, "%s\n", canon); fclose(af); }` (add-index single-field path, ~line 1184):

Anchor:
```c
        if (af) { fprintf(af, "%s\n", canon); fclose(af); }
    }

    invalidate_idx_cache(object);
    uint64_t duration_ms = now_ms() - t_start;
    int records = get_live_count(db_root, object);
    if (records < 0) records = 0;
    OUT("{\"status\":\"indexed\",\"field\":\"%s\",\"records\":%d,\"duration_ms\":%llu}\n",
```

Replacement (change only the `invalidate_idx_cache` line):
```c
        if (af) { fprintf(af, "%s\n", canon); fclose(af); }
    }

    invalidate_idx_cache(db_root, object);
    uint64_t duration_ms = now_ms() - t_start;
    int records = get_live_count(db_root, object);
    if (records < 0) records = 0;
    OUT("{\"status\":\"indexed\",\"field\":\"%s\",\"records\":%d,\"duration_ms\":%llu}\n",
```

### Site B — add-indexes multi-field path (~line 2361):

Anchor:
```c
                if (af) { fprintf(af, "%s\n", canon); fclose(af); }
            }
        }
    }

    invalidate_idx_cache(object);
    uint64_t duration_ms = now_ms() - t_start;
    int records = get_live_count(db_root, object);
    if (records < 0) records = 0;
    /* Response semantics:
```

Replacement:
```c
                if (af) { fprintf(af, "%s\n", canon); fclose(af); }
            }
        }
    }

    invalidate_idx_cache(db_root, object);
    uint64_t duration_ms = now_ms() - t_start;
    int records = get_live_count(db_root, object);
    if (records < 0) records = 0;
    /* Response semantics:
```

### Site C — remove-index single-field path (~line 2480):

Anchor:
```c
    unlink_index_by_line(db_root, object, matched_line, sch.splits);
    invalidate_idx_cache(object);

    LOG_AUDIT(LOG_SUB_INDEX, "REMOVE-INDEX %s/%s: %s", db_root, object, field);
```

Replacement:
```c
    unlink_index_by_line(db_root, object, matched_line, sch.splits);
    invalidate_idx_cache(db_root, object);

    LOG_AUDIT(LOG_SUB_INDEX, "REMOVE-INDEX %s/%s: %s", db_root, object, field);
```

### Site D — remove-index bulk path (~line 2562):

Anchor:
```c
    invalidate_idx_cache(object);
    LOG_AUDIT(LOG_SUB_INDEX, "REMOVE-INDEX %s/%s: %d removed, %d not_indexed", db_root, object, removed, missing);
```

Replacement:
```c
    invalidate_idx_cache(db_root, object);
    LOG_AUDIT(LOG_SUB_INDEX, "REMOVE-INDEX %s/%s: %d removed, %d not_indexed", db_root, object, removed, missing);
```

---

## Task 7 — `src/db/config.c`: update 2 call sites

### Site E — remove-field path (~line 3319):

Anchor:
```c
    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(object);

    LOG_AUDIT(LOG_SUB_CONFIG, "REMOVE-FIELD %s/%s: %d fields tombstoned, %d indexes dropped",
```

Replacement:
```c
    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(db_root, object);

    LOG_AUDIT(LOG_SUB_CONFIG, "REMOVE-FIELD %s/%s: %d fields tombstoned, %d indexes dropped",
```

### Site F — rename-field path (~line 3407):

Anchor:
```c
    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(object);

    LOG_AUDIT(LOG_SUB_CONFIG, "RENAME-FIELD %s/%s: %s -> %s", db_root, object, old_name, new_name);
```

Replacement:
```c
    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(db_root, object);

    LOG_AUDIT(LOG_SUB_CONFIG, "RENAME-FIELD %s/%s: %s -> %s", db_root, object, old_name, new_name);
```

---

## Task 8 — Build

```
SKIP_TESTS=1 ./build.sh
```

Expected: clean compile, no warnings.

---

## Task 9 — New test: `src/test/cases/test_idx_cache_tenants.c`

Create this file:

```c
/* src/test/cases/test_idx_cache_tenants.c
 * Verify that two tenants with identically-named objects have independent
 * idx cache entries — neither pollutes the other on index add/remove.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>

static int test_idx_cache_tenants_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* Two tenants, same object name "products". */
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"ict_alpha\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"ict_beta\"}", &resp);
    free(resp); resp = NULL;

    /* alpha/products — index on "price" */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"ict_alpha\",\"object\":\"products\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[{\"name\":\"price\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"varchar\",\"size\":32}],"
        "\"indexes\":[\"price\"]}",
        &resp); free(resp); resp = NULL;

    /* beta/products — index on "name" */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"ict_beta\",\"object\":\"products\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[{\"name\":\"price\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"varchar\",\"size\":32}],"
        "\"indexes\":[\"name\"]}",
        &resp); free(resp); resp = NULL;

    /* Verify alpha sees "price" index, NOT "name" */
    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"ict_alpha\",\"object\":\"products\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"price\"", "alpha/products indexes contain price");
    ASSERT_TRUE(strstr(resp, "\"name\"") == NULL ||
                strstr(resp, "\"indexes\"") == NULL ||
                /* name appears only in fields, not indexes section */
                (strstr(strstr(resp, "\"indexes\""), "\"price\"") != NULL),
                "alpha/products index list contains price");
    free(resp); resp = NULL;

    /* Verify beta sees "name" index */
    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"ict_beta\",\"object\":\"products\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"name\"", "beta/products describe returns name");
    free(resp); resp = NULL;

    /* Add a second index to alpha — must NOT appear in beta's cache. */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"ict_alpha\",\"object\":\"products\",\"field\":\"name\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\"", "add-index name on alpha succeeded");
    free(resp); resp = NULL;

    /* Beta should still only have "name" index (not an additional "price"). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"ict_beta\",\"object\":\"products\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"x\"}],"
        "\"limit\":1}",
        &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL,
                "find on beta/products by name (indexed) succeeds without error");
    free(resp); resp = NULL;

    /* Remove index from alpha — must NOT corrupt beta's cache. */
    tc_request(tc,
        "{\"mode\":\"remove-index\",\"dir\":\"ict_alpha\",\"object\":\"products\",\"field\":\"name\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"removed\"", "remove-index name from alpha");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"ict_beta\",\"object\":\"products\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"x\"}],"
        "\"limit\":1}",
        &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL,
                "find on beta/products by name still works after alpha remove-index");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-idx-cache-tenants", test_idx_cache_tenants_run)
```

---

## Task 10 — Run full test suite

```
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` where N ≥ previous count + assertions in the new test.

Leave all changes **uncommitted**. Report the exact test output line.
