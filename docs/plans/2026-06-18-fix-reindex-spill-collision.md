# Plan: Fix reindex spill-directory collision when field has both btree and trigram indexes

**Date:** 2026-06-18
**Author:** Claude Sonnet 4.6

---

## Execution rules (read before touching any file)

1. Branch off `main`: `git checkout -b fix/reindex-spill-collision`
2. Execute tasks in order. Do not skip.
3. Build after each task: `SKIP_TESTS=1 ./build.sh`
4. Full test suite after all tasks: `./build/bin/shard-db-test run-all`
5. Never claim a step passed without pasting the real terminal output.
6. **Anchor rule**: every insertion site is identified by a quoted string that must appear verbatim in the file. If the anchor is not found exactly, STOP and write `PLAN_NOTES.md` at the repo root. Do not guess or reinterpret.
7. Leave all work **uncommitted**. The user will commit after review.

---

## Root cause

`reindex_object` builds the `MFFieldDesc` array from `index.conf`. For any entry it calls `parse_index_spec`, which strips the type suffix — `"title:trigram"` yields `ps.name = "title"`. The name is then stored verbatim into `d->name` (line 3383). A plain `"title"` btree entry also produces `d->name = "title"`.

Both fields therefore share the same spill directory:

```
<db_root>/<object>/indexes/title/.spill/
```

In `seg_scan_worker`, each field opens its per-shard spill files with `O_CREAT | O_TRUNC`:

```c
snprintf(path, sizeof(path), "%s/w%d_s%d.bin", spill_dir, w->worker_idx, s);
spill_writer_open(&f->spill_writers[s], path);   /* O_CREAT | O_TRUNC */
```

When fi=0 (btree-title) opens `w0_s0.bin` and then fi=1 (trigram-title) opens the **same path**, the second `open()` call truncates the first writer's file to zero while leaving both fds pointing at offset 0. Both writers then interleave bytes into the file concurrently, producing a completely corrupt spill file.

Phase 2 `merge_spills_into_index` reads the corrupt file, hits an invalid run header on the very first `pread`, and breaks out of its loop with `reader_count == 0`. It then calls `bt_stream_build_finish` on an empty stream, writing an empty B+ tree. Both the btree-title and trigram-title indexes come out empty. The same collision can occur for any two index types that share a field name (e.g. `score` btree + `score:bitmap` — though those auto-promote and are unlikely to coexist in practice).

**Fix**: append the field-descriptor array index `fi` to the spill directory name at all four construction sites. `fi` is already in scope at every site and guarantees uniqueness across all entries in `descs[]` regardless of field name or type.

---

## Files modified

| File | What changes |
|---|---|
| `src/db/index.c` | Append `_%d fi` to spill_dir at 4 `snprintf` sites |
| `src/test/cases/test_reindex_spill_collision.c` | New test case |
| `src/test/test_registry.c` | Register the new test case |

---

## Task 1 — Fix the four spill-directory construction sites in `src/db/index.c`

### 1a. `seg_scan_worker` — Phase 1 writer

**Anchor** (unique because of `w->` prefix on all three args):

```
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill",
                 w->db_root, w->object, w->descs[fi].name);
        if (w->descs[fi].type == MF_BITMAP) {
```

Replace with:

```c
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                 w->db_root, w->object, w->descs[fi].name, fi);
        if (w->descs[fi].type == MF_BITMAP) {
```

### 1b. `seg_seq_build_spills` — initial `mkdirp` setup loop

**Anchor** (unique because of `mkdirp(spill_dir)` immediately after):

```
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill",
                 db_root, object, descs[fi].name);
        mkdirp(spill_dir);
```

Replace with:

```c
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                 db_root, object, descs[fi].name, fi);
        mkdirp(spill_dir);
```

### 1c. `seg_seq_build_spills` — Phase 2a merge

**Anchor** (unique because of `MergeShardArg *margs = calloc` immediately after):

```
        char spill_dir[PATH_MAX];
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill",
                 db_root, object, descs[fi].name);
        MergeShardArg *margs = calloc((size_t)idx_n, sizeof(MergeShardArg));
```

Replace with:

```c
        char spill_dir[PATH_MAX];
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                 db_root, object, descs[fi].name, fi);
        MergeShardArg *margs = calloc((size_t)idx_n, sizeof(MergeShardArg));
```

### 1d. `resolve_bitmaps` — Phase 2b bitmap spill reader

**Anchor** (unique because of `snprintf(path, sizeof(path), "%s/bmw%d.bin"` after):

```
        char spill_dir[PATH_MAX];
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill",
                 db_root, object, descs[fi].name);

        for (int w = 0; w < P; w++) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/bmw%d.bin", spill_dir, w);
```

Replace with:

```c
        char spill_dir[PATH_MAX];
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                 db_root, object, descs[fi].name, fi);

        for (int w = 0; w < P; w++) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/bmw%d.bin", spill_dir, w);
```

### 1e. Build check

```bash
SKIP_TESTS=1 ./build.sh
```

Paste output. Must succeed with no errors or warnings.

---

## Task 2 — New test case: `test_reindex_spill_collision.c`

Create the file `src/test/cases/test_reindex_spill_collision.c` with the following exact content:

```c
/* src/test/cases/test_reindex_spill_collision.c
 * Regression test: reindex must produce correct btree and trigram indexes
 * when the same field has both a btree and a trigram index in index.conf.
 * Before the fix, both shared the spill directory name "indexes/<field>/.spill",
 * causing O_CREAT|O_TRUNC to corrupt each other's spill files. The trigram
 * (and btree) index came out empty. After the fix, each field descriptor uses
 * its own ".spill_<fi>" directory, preventing the collision.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int do_count(TestClient *tc, const char *dir, const char *obj,
                    const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"criteria\":%s}", dir, obj, crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_reindex_spill_collision_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;

    /* Create object with a varchar title field; no indexes yet. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"articles\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[{\"name\":\"title\",\"type\":\"varchar\",\"size\":200}]}",
        &resp);
    free(resp); resp = NULL;

    /* Insert records — 5 contain "honda", 10 do not. */
    const char *titles[] = {
        "Honda announces new EV platform",
        "Honda wins Le Mans prototype class",
        "Toyota beats Honda in sales",
        "The best motorcycles of 2026",
        "Honda recall affects 200k vehicles",
        "Apple releases new MacBook",
        "OpenAI launches GPT-5",
        "PostgreSQL 17 performance review",
        "Rust 2.0 stabilized",
        "Linux kernel 7.0 released",
        "Microsoft acquires startup",
        "Google search algorithm update",
        "Amazon AWS outage post-mortem",
        "Meta VR headset review",
        "SpaceX starship test flight",
    };
    int honda_count = 0;
    for (int i = 0; i < 15; i++) {
        char req[512];
        /* JSON-escape the title (these are all ASCII, no escaping needed). */
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"articles\","
            "\"key\":\"k%02d\",\"value\":{\"title\":\"%s\"}}",
            i, titles[i]);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
        if (strstr(titles[i], "Honda") || strstr(titles[i], "honda")) honda_count++;
    }
    /* honda_count == 3 (case-sensitive insert; trigram search is case-insensitive,
       all 3 "Honda" titles match icontains "honda"). */

    /* Add btree index on title (for eq / starts_with queries). */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"articles\","
        "\"field\":\"title\"}",
        &resp);
    free(resp); resp = NULL;

    /* Add trigram index on title (for icontains / contains queries). */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"articles\","
        "\"field\":\"title:trigram\"}",
        &resp);
    free(resp); resp = NULL;

    /* Baseline: verify both indexes work before reindex. */
    int pre_tg = do_count(tc, "d", "articles",
        "[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"honda\"}]");
    ASSERT_INT_EQ(pre_tg, 3, "pre-reindex trigram icontains honda");

    int pre_bt = do_count(tc, "d", "articles",
        "[{\"field\":\"title\",\"op\":\"starts\",\"value\":\"Honda\"}]");
    ASSERT_INT_EQ(pre_bt, 3, "pre-reindex btree starts Honda");

    /* Run reindex — this is the regression point. With the bug, both title
       indexes shared the same spill directory and corrupted each other. */
    tc_request(tc,
        "{\"mode\":\"reindex\",\"dir\":\"d\",\"object\":\"articles\"}",
        &resp);
    free(resp); resp = NULL;

    /* After reindex: trigram must still return 3 for icontains "honda". */
    int post_tg = do_count(tc, "d", "articles",
        "[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"honda\"}]");
    ASSERT_INT_EQ(post_tg, 3, "post-reindex trigram icontains honda");

    /* After reindex: btree must still return 3 for starts "Honda". */
    int post_bt = do_count(tc, "d", "articles",
        "[{\"field\":\"title\",\"op\":\"starts\",\"value\":\"Honda\"}]");
    ASSERT_INT_EQ(post_bt, 3, "post-reindex btree starts Honda");

    /* After reindex: full icontains find must return 3 records. */
    char *find_resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"honda\"}],"
        "\"limit\":10}",
        &find_resp);
    /* Result is a JSON array; count '[' separators is not reliable — just
       verify it is an array and non-empty. */
    ASSERT_NOT_NULL(find_resp, "post-reindex find response non-null");
    int is_array = find_resp && find_resp[0] == '[';
    ASSERT_INT_EQ(is_array, 1, "post-reindex find returns array");
    /* Count the number of objects in the array by counting top-level '{'. */
    int obj_count = 0;
    int depth = 0;
    for (const char *p = find_resp; *p; p++) {
        if (*p == '[' || *p == '{') depth++;
        else if (*p == ']' || *p == '}') depth--;
        if (*p == '{' && depth == 2) obj_count++;
    }
    ASSERT_INT_EQ(obj_count, 3, "post-reindex find returns 3 honda records");
    free(find_resp);

    tc_disconnect(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER(test-reindex-spill-collision, test_reindex_spill_collision_run);
```

### 2a. Build check

```bash
SKIP_TESTS=1 ./build.sh
```

Paste output. Must succeed.

---

## Task 3 — Register the test in `src/test/test_registry.c`

**Anchor** (find this exact string — it is the block of `TEST_DECLARE` lines near the end of the existing declarations; pick the last one present and append after it; if unsure, grep for `TEST_DECLARE` and use the final occurrence):

```
/* AUTO-GENERATED list ends here — add new TEST_DECLARE lines above */
```

If that sentinel comment is not present, use this anchor instead — the closing brace of the last `TEST_DECLARE` block (search for the pattern and confirm only one match):

```bash
grep -n "TEST_DECLARE\|test_registry" src/test/test_registry.c | tail -5
```

Paste the grep output and use the last `TEST_DECLARE` line as the anchor. Insert:

```c
TEST_DECLARE(test-reindex-spill-collision);
```

immediately after the last existing `TEST_DECLARE` line.

### 3a. Build check

```bash
SKIP_TESTS=1 ./build.sh
```

Paste output. Must succeed.

---

## Task 4 — Verification

### 4a. Run the new test alone first

```bash
./build/bin/shard-db-test run test-reindex-spill-collision
```

Required: `# test-reindex-spill-collision: N passed, 0 failed`. Paste full output.

### 4b. Full test suite

```bash
./build/bin/shard-db-test run-all
```

Required: `# total: N passed, 0 failed` where N ≥ 209. Paste the last 5 lines.

---

## Correctness invariants

1. **`fi` is the position in `descs[]`** — the same array is passed to `seg_seq_build_spills`, `seg_scan_worker` (via `w->descs`), and `resolve_bitmaps`. The loop variable `fi` in each function iterates the same positions, so `.spill_0`, `.spill_1`, … refer consistently to the same field across Phase 1 and Phase 2.

2. **No other code reads or constructs spill directory paths** — confirmed by `grep -n '\.spill' src/db/index.c` returning exactly 4 `snprintf` lines plus 1 `margs[s].spill_dir = spill_dir` assignment (which uses the already-corrected local variable).

3. **`rmdir(spill_dir)` cleanup is automatic** — the cleanup calls use the same local `spill_dir` variable in scope, so they delete the correctly-named `_<fi>` directories without any additional changes.

4. **Single-field `add-index` is unaffected** — `build_trigram_pass` and `build_btree_pass` construct their own spill paths independently and are not part of the `descs[]` multi-field flow. They are not modified by this plan.

5. **`fi == 0` is valid** — `.spill_0` is a legal directory name. The suffix `_0`, `_1`, etc. is consistent and unambiguous.

---

## Edge cases with required behavior

| Case | Required behavior |
|---|---|
| Two fields with the same name AND same type (e.g., two btree entries for `title` in a corrupt index.conf) | Each gets its own `.spill_<fi>`. Both merge correctly into separate output paths. The underlying btree files share the same `.idx` path and the second merge overwrites the first — this was already broken before this fix and is not made worse. |
| Single field, single type (normal case) | `.spill_0` is used. No collision possible. Identical output to before. |
| Object with 0 btree/trigram fields (bitmap only) | `resolve_bitmaps` loop uses `fi` for its spill dir. No btree/trigram spill dirs are created. |
| Stale `.spill_<fi>` directories from a crashed prior reindex | `mkdirp` is idempotent. Leftover spill files from a prior run are overwritten by `O_CREAT|O_TRUNC` as before. |

---

## What is NOT changed

- `build_trigram_pass` / `build_btree_pass` (single-field `add-index` path): not affected.
- `MFFieldDesc` struct: no new fields added.
- Output B+ tree / bitmap file paths (`tg_build_path`, `build_idx_path`, `bm_build_path`): use `descs[fi].name` (field name only) — unchanged.
- `merge_spills_into_index` signature: `spill_dir` is already a parameter, not recomputed internally.
