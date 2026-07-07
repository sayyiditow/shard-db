# Coverity stack-overflow / memory-corruption fixes

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/coverity-stack-overflow main`.
- Do the three tasks below **in order**. Each is independent of the others; nothing later depends on earlier code, but doing them in order keeps the diff easy to review incrementally.
- Build with `SKIP_TESTS=1 ./build.sh` after each task to catch compile errors early. Run the full suite only after all three tasks are done: `./build/bin/shard-db-test run-all`.
- Every insertion/edit below is anchored on **quoted exact text** from the current source. If a quoted anchor is not found verbatim in the file, STOP — do not guess, do not reinterpret, do not "fix it forward." Instead write `docs/plans/PLAN_NOTES.md` describing exactly what you searched for and what you found instead, and stop working on this plan.
- Leave all work **uncommitted** when done. Do not run `git add`, `git commit`, `git push`, or open a PR — that happens outside this workflow, after human review.
- After the last task, run `./build/bin/shard-db-test run-all --filter coverity` and also the three specific new test names given below, and paste the **real terminal output** (not a paraphrase) showing `# total: N passed, 0 failed` before considering this plan done. If any test fails, do not modify the test to make it pass — the test encodes the bug; a failing test after your fix means the fix is incomplete or wrong. Stop and report.

## Background

Three related Coverity findings — CID 1696413, CID 1696446, and CID 1696463/1696458 — are all instances of the same broader defect class: a fixed-size stack buffer is filled using a length that was not actually validated against the buffer's capacity. Full triage context: `docs/coverity-triage-2026-07.md`.

All three are being fixed in one plan because they're small, mechanically similar, and the highest-severity items from the triage.

---

## Task 1 — `encode_criterion_value` unbounded memcpy (CID 1696413)

### The bug

`src/db/query.c` defines:

```c
/* Encode a criterion value for index lookup. If tf is NULL (composite index
   or unknown field), returns the text as raw bytes. Otherwise emits
   memcmp-sortable bytes matching what the write side stored. Output written
   into caller's buf; *out_len set. */
void encode_criterion_value(const TypedField *tf,
                                   const char *val, size_t vlen,
                                   uint8_t *buf, size_t *out_len) {
    if (!tf) {
        memcpy(buf, val, vlen);
        *out_len = vlen;
        return;
    }
    encode_field_for_index(tf, val, vlen, buf, out_len);
}
```

When `tf` is non-NULL, `encode_field_for_index` bounds its own output to `tf->size` bytes (a typed field's fixed on-disk width, always well under 1024). But when `tf` is NULL — which happens whenever the criterion's field name isn't a resolvable typed field, e.g. `resolve_idx_field()` returns NULL for composite-index field names (`strchr(field, '+')`) or fields absent from the typed schema — this function does a **raw, unbounded `memcpy(buf, val, vlen)`**, where `vlen` is `strlen()` of a criterion value that arrived over the wire with no length cap.

Every call site passes a **fixed-size stack buffer** sized 1024, 1032, or `1024+8` bytes (verified across all 40+ call sites in `query.c`, `query_plan.c`, `query_aggregate.c` — the smallest is exactly 1024 bytes, e.g. `uint8_t val[1024]` in `query_plan.c`). A criterion value longer than 1024 bytes routed through the `tf == NULL` path overflows the caller's stack buffer. This is reachable through the public `find`/`count`/`aggregate` JSON query protocol whenever a query's seed/order/composite field doesn't resolve to a typed field — e.g. `find_via_composite_prefix` (query.c) calls `encode_criterion_value(seed_tf_sv, seed->value, strlen(seed->value), buf_lo_sv, &len_lo_sv)` at line 1981, where `buf_lo_sv` is `uint8_t buf_lo_sv[1024 + 8]` and `seed_tf_sv` can be NULL.

### The fix

Clamp the raw-copy branch to the smallest guaranteed buffer size across every call site (1024 bytes). This is a single, self-contained fix inside the function — no call site changes needed, since every caller's buffer is already >= 1024 bytes.

In `src/db/query.c`, find this exact block:

```c
void encode_criterion_value(const TypedField *tf,
                                   const char *val, size_t vlen,
                                   uint8_t *buf, size_t *out_len) {
    if (!tf) {
        memcpy(buf, val, vlen);
        *out_len = vlen;
        return;
    }
    encode_field_for_index(tf, val, vlen, buf, out_len);
}
```

Replace it with:

```c
void encode_criterion_value(const TypedField *tf,
                                   const char *val, size_t vlen,
                                   uint8_t *buf, size_t *out_len) {
    if (!tf) {
        /* No typed field to bound the copy by (composite-index field name,
           or a field absent from the typed schema) — vlen here is a raw
           user-supplied criterion value with no upper bound from the wire
           protocol. Every call site's buf is a fixed-size stack buffer of
           at least 1024 bytes (verified across all call sites in query.c /
           query_plan.c / query_aggregate.c); clamp to that so an
           over-length value can never overflow the smallest of them
           (CID 1696413). */
        size_t n = vlen < 1024 ? vlen : 1024;
        memcpy(buf, val, n);
        *out_len = n;
        return;
    }
    encode_field_for_index(tf, val, vlen, buf, out_len);
}
```

### Regression test

Add a new file `src/test/cases/test_coverity_encode_criterion_overflow.c`:

```c
/* src/test/cases/test_coverity_encode_criterion_overflow.c
 * CID 1696413: encode_criterion_value's tf==NULL branch did an unbounded
 * memcpy(buf, val, vlen) with no relation to the caller's actual buffer
 * size. Every real call site passes a fixed >=1024-byte stack buffer;
 * a criterion value longer than that overflowed it. This test calls the
 * function directly (linked into shard-db-test alongside the rest of
 * src/db/*.c — see build.sh) with a heap buffer immediately followed by
 * a canary region, so any write past the declared size is caught
 * regardless of stack layout. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <stdlib.h>
#include <string.h>

/* Declared non-static in query.c; no header change needed for this fix. */
extern void encode_criterion_value(const TypedField *tf,
                                    const char *val, size_t vlen,
                                    uint8_t *buf, size_t *out_len);

static int test_coverity_encode_criterion_overflow_run(void) {
    const size_t BUF_SZ = 1024;
    const size_t CANARY_SZ = 256;
    uint8_t *region = malloc(BUF_SZ + CANARY_SZ);
    ASSERT_NOT_NULL(region, "alloc region");
    if (!region) return 1;
    memset(region + BUF_SZ, 0xAB, CANARY_SZ);

    /* Value far longer than any real call site's buffer. */
    size_t vlen = 4000;
    char *val = malloc(vlen + 1);
    ASSERT_NOT_NULL(val, "alloc val");
    if (!val) { free(region); return 1; }
    memset(val, 'x', vlen);
    val[vlen] = '\0';

    size_t out_len = 0;
    encode_criterion_value(NULL, val, vlen, region, &out_len);

    ASSERT_TRUE(out_len <= BUF_SZ, "out_len clamped to buffer size");

    int canary_intact = 1;
    for (size_t i = 0; i < CANARY_SZ; i++) {
        if (region[BUF_SZ + i] != 0xAB) { canary_intact = 0; break; }
    }
    ASSERT_TRUE(canary_intact, "no write past the declared 1024-byte buffer");

    /* Sanity: a short value still round-trips exactly (no regression on
       the common case). */
    memset(region, 0, BUF_SZ);
    out_len = 0;
    encode_criterion_value(NULL, "hello", 5, region, &out_len);
    ASSERT_EQ_INT((int)out_len, 5, "short value: out_len exact");
    ASSERT_TRUE(memcmp(region, "hello", 5) == 0, "short value: bytes exact");

    free(val);
    free(region);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-coverity-encode-criterion-overflow", test_coverity_encode_criterion_overflow_run)
```

In `build.sh`, find this exact line (the last test-case file before the `src/db/*.c` sources begin in the `shard-db-test` link command):

```
    src/test/cases/test_secure_random_keys.c \
```

Replace it with:

```
    src/test/cases/test_secure_random_keys.c \
    src/test/cases/test_coverity_encode_criterion_overflow.c \
    src/test/cases/test_coverity_group_by_overflow.c \
    src/test/cases/test_coverity_join_buf_overflow.c \
```

(This single edit registers all three new test files added by this plan — Tasks 1, 2, and 3 — so it only needs to be done once, during Task 1.)

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-coverity-encode-criterion-overflow`. Paste the real output.

---

## Task 2 — `cmd_aggregate_tree`'s `group_by_buf` overflow (CID 1696446)

### The bug

`src/db/query_aggregate.c`, inside `cmd_aggregate_tree`:

```c
    /* Convert group_by_csv to group_by_json (JSON array) */
    char group_by_buf[4096] = "[";
    int gpos = 1;
    if (group_by_csv && group_by_csv[0]) {
        const char *p = group_by_csv;
        while (*p) {
            while (*p == ' ' || *p == '\t') p++;
            if (!*p) break;
            if (gpos > 1 && gpos < (int)sizeof(group_by_buf) - 1)
                group_by_buf[gpos++] = ',';
            group_by_buf[gpos++] = '"';
            while (*p && *p != ',') {
                if (gpos >= (int)sizeof(group_by_buf) - 2) break;
                group_by_buf[gpos++] = *p++;
            }
            group_by_buf[gpos++] = '"';
            if (*p == ',') p++;
        }
    }
    group_by_buf[gpos] = ']';
```

The inner per-character copy loop is bounds-checked (`if (gpos >= sizeof(group_by_buf) - 2) break;`), but the two quote writes surrounding it (`group_by_buf[gpos++] = '"'`, before and after the inner loop) and the final `group_by_buf[gpos] = ']'` are **not**. Once `gpos` reaches the buffer's end from accumulated fields, this is what happens on the next outer-loop iteration: the inner while-loop's bound check fires immediately (`gpos >= sizeof-2` is already true), so it `break`s *before* consuming any input character for that field — `p` does not advance. The trailing `if (*p == ',') p++;` doesn't fire either, since `p` isn't pointing at a comma. So the outer `while (*p)` loop spins forever on the same input position, and the two unconditional quote writes each outer-loop iteration keep incrementing `gpos` and writing to `group_by_buf[gpos]` — past the end of the 4096-byte stack array — indefinitely, until the process crashes.

This function is a **general-purpose, externally-declared entry point** (`int cmd_aggregate_tree(...)`, declared in `types.h`), reachable today from the NQL `aggregate` command (`server.c`, `NQL_AGGREGATE` case) with `group_by_csv` sourced from `NqlCommand.group_by`, itself capped at 1023 bytes (`nql.h`: `char group_by[1024]`) via a single bounded `snprintf` in `nql.c`. Given that cap, the worst-case output for today's NQL wire grammar stays under 4096 bytes, so this exact crash is not reachable through the current NQL wire protocol — but `cmd_aggregate_tree` has no such guarantee for any other caller (embedded-mode C API consumers can call it directly with an arbitrary-length string, and a future NQL grammar change could lift or bypass the 1024 cap). The function's own contract is broken regardless of who currently respects the cap upstream, so it's fixed here as a real defect in the function itself, not solely as a live wire-reachable exploit.

### The fix

Extract the CSV→JSON conversion into its own small, bounds-correct, independently testable function, matching the already-correct sibling pattern used a few lines below for `having_buf` in the same function (every write there is gated on `n > 0 && n < remain` before advancing the position).

In `src/db/query_aggregate.c`, find this exact block (immediately preceding `int cmd_aggregate_tree(...)`):

```c
int cmd_aggregate_tree(const char *db_root, const char *object,
                       CriteriaNode *criteria_tree,
                       const NqlAggSpec *aggs, int naggs,
                       const char *group_by_csv,
                       CriteriaNode *having_tree,
                       const char *order_by, int order_desc, int limit,
                       const char *format, const char *delimiter, int want_total) {
```

Insert the new helper function directly **before** it (i.e. immediately above this line), so the new text becomes:

```c
/* Convert a CSV field list ("a,b,c") into a JSON string array
   ("["a","b","c"]") into out[0..out_sz). Every byte offset is
   bounds-checked *before* the write that would produce it — the
   original inline version instead wrote its closing quote/comma/bracket
   unconditionally after a bounds-checked copy loop, which let a long
   enough group_by_csv drive an unbounded write loop past the end of a
   fixed 4096-byte stack buffer (CID 1696446). Fields beyond what fits
   are silently dropped rather than truncated mid-name, matching the
   sibling having_buf conversion just below this function's caller. */
static void group_by_csv_to_json(const char *csv, char *out, size_t out_sz) {
    if (out_sz == 0) return;
    if (out_sz < 3) { out[0] = '\0'; return; }
    out[0] = '[';
    int pos = 1;
    int first = 1;
    if (csv && csv[0]) {
        const char *p = csv;
        while (*p) {
            while (*p == ' ' || *p == '\t') p++;
            if (!*p) break;
            const char *start = p;
            while (*p && *p != ',') p++;
            int flen = (int)(p - start);
            int remain = (int)out_sz - pos;
            if (remain <= 0) break;
            int n = snprintf(out + pos, (size_t)remain, "%s\"%.*s\"",
                             first ? "" : ",", flen, start);
            if (n < 0 || n >= remain) break;   /* would truncate — stop here */
            pos += n;
            first = 0;
            if (*p == ',') p++;
        }
    }
    if ((size_t)pos + 2 <= out_sz) {
        out[pos++] = ']';
        out[pos] = '\0';
    } else {
        out[out_sz - 2] = ']';
        out[out_sz - 1] = '\0';
    }
}

int cmd_aggregate_tree(const char *db_root, const char *object,
                       CriteriaNode *criteria_tree,
                       const NqlAggSpec *aggs, int naggs,
                       const char *group_by_csv,
                       CriteriaNode *having_tree,
                       const char *order_by, int order_desc, int limit,
                       const char *format, const char *delimiter, int want_total) {
```

Next, still in `src/db/query_aggregate.c`, find this exact block (the call site inside `cmd_aggregate_tree`):

```c
    /* Convert group_by_csv to group_by_json (JSON array) */
    char group_by_buf[4096] = "[";
    int gpos = 1;
    if (group_by_csv && group_by_csv[0]) {
        const char *p = group_by_csv;
        while (*p) {
            while (*p == ' ' || *p == '\t') p++;
            if (!*p) break;
            if (gpos > 1 && gpos < (int)sizeof(group_by_buf) - 1)
                group_by_buf[gpos++] = ',';
            group_by_buf[gpos++] = '"';
            while (*p && *p != ',') {
                if (gpos >= (int)sizeof(group_by_buf) - 2) break;
                group_by_buf[gpos++] = *p++;
            }
            group_by_buf[gpos++] = '"';
            if (*p == ',') p++;
        }
    }
    group_by_buf[gpos] = ']';
```

Replace it with:

```c
    /* Convert group_by_csv to group_by_json (JSON array) */
    char group_by_buf[4096];
    group_by_csv_to_json(group_by_csv, group_by_buf, sizeof(group_by_buf));
```

Finally, expose `group_by_csv_to_json` for the test to call directly. In `src/db/query_internal.h`, find this exact line:

```c
void encode_criterion_value(const TypedField *tf,
```

Insert the new declaration directly **before** it:

```c
void group_by_csv_to_json(const char *csv, char *out, size_t out_sz);
void encode_criterion_value(const TypedField *tf,
```

And back in `src/db/query_aggregate.c`, drop the `static` keyword from the helper you just added — find:

```c
static void group_by_csv_to_json(const char *csv, char *out, size_t out_sz) {
```

Replace with:

```c
void group_by_csv_to_json(const char *csv, char *out, size_t out_sz) {
```

### Regression test

Add a new file `src/test/cases/test_coverity_group_by_overflow.c`:

```c
/* src/test/cases/test_coverity_group_by_overflow.c
 * CID 1696446: cmd_aggregate_tree's inline CSV->JSON group_by conversion
 * wrote its closing quote/bracket unconditionally once the bounds-checked
 * inner copy loop had already saturated the 4096-byte buffer, turning
 * into an unbounded write loop. Now extracted into group_by_csv_to_json()
 * (query_aggregate.c, declared in query_internal.h) — this test calls it
 * directly with a CSV far longer than any single NQL group_by clause
 * (NqlCommand.group_by is capped at 1024 bytes upstream) to prove the
 * function itself is safe regardless of caller. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "query_internal.h"
#include <stdlib.h>
#include <string.h>

static int test_coverity_group_by_overflow_run(void) {
    const size_t BUF_SZ = 4096;
    const size_t CANARY_SZ = 256;
    char *region = malloc(BUF_SZ + CANARY_SZ);
    ASSERT_NOT_NULL(region, "alloc region");
    if (!region) return 1;
    memset(region + BUF_SZ, 0xAB, CANARY_SZ);

    /* 3000 single-character field names — far more than the ~500 that
       would fit if this were bounded by the NQL wire cap, guaranteed to
       have driven the old code's unbounded write loop. */
    size_t n_fields = 3000;
    size_t csv_cap = n_fields * 2 + 1;
    char *csv = malloc(csv_cap);
    ASSERT_NOT_NULL(csv, "alloc csv");
    if (!csv) { free(region); return 1; }
    size_t cp = 0;
    for (size_t i = 0; i < n_fields; i++) {
        if (i > 0) csv[cp++] = ',';
        csv[cp++] = 'x';
    }
    csv[cp] = '\0';

    group_by_csv_to_json(csv, region, BUF_SZ);

    int canary_intact = 1;
    for (size_t i = 0; i < CANARY_SZ; i++) {
        if ((unsigned char)region[BUF_SZ + i] != 0xAB) { canary_intact = 0; break; }
    }
    ASSERT_TRUE(canary_intact, "no write past the declared 4096-byte buffer");

    /* Output must still be NUL-terminated, well-formed JSON (starts '[',
       ends ']') even though most fields were necessarily dropped. */
    size_t out_len = strlen(region);
    ASSERT_TRUE(out_len > 0 && out_len < BUF_SZ, "output is NUL-terminated within bounds");
    ASSERT_TRUE(region[0] == '[', "output starts with [");
    ASSERT_TRUE(region[out_len - 1] == ']', "output ends with ]");

    /* Sanity: a normal short CSV still round-trips exactly. */
    char small[64];
    group_by_csv_to_json("a,b,c", small, sizeof(small));
    ASSERT_TRUE(strcmp(small, "[\"a\",\"b\",\"c\"]") == 0, "small CSV converts exactly");

    free(csv);
    free(region);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-coverity-group-by-overflow", test_coverity_group_by_overflow_run)
```

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-coverity-group-by-overflow`. Paste the real output.

---

## Task 3 — `buf_join_values` / `buf_driver_values` unclamped `snprintf` return (CID 1696463, CID 1696458)

### The bug

`src/db/query_join.c` builds tabular JSON join rows using a chain of helper functions that all follow the pattern `pos += snprintf(buf + pos, bufsz - pos, ...)`. `snprintf` returns the number of bytes it *would* have written if the buffer were large enough — not the number actually written. Once any single call in the chain truncates, `pos` becomes permanently larger than what was actually written, and every subsequent `buf + pos` in that call becomes an out-of-bounds pointer while `bufsz - pos` (unsigned arithmetic, `size_t - int` with `pos > bufsz`) wraps around to a huge value — handing the *next* `snprintf` a valid-looking but wildly oversized bound and an out-of-bounds destination pointer.

Three functions in this file have the pattern; the third is the one that ultimately overflows because its final write is unconditional:

```c
static int buf_field_value(const TypedField *tf, const uint8_t *field_ptr,
                           char *buf, size_t bufsz) {
    if (!tf) return snprintf(buf, bufsz, "null");
    char tmp[512];
    int n;
    switch (tf->type) {
    case FT_VARCHAR: {
        int len = varchar_eff_len(field_ptr, tf->size);
        if (len == 0) return snprintf(buf, bufsz, "\"\"");
        /* Escape per RFC 8259 — varchar content is opaque bytes,
           may contain " \ or control chars. Caller sizes buf for
           up to 6 * len + 2 worst case. NUL-terminate so callers
           that pass the buffer to printf-%s read a bounded string. */
        if (bufsz < 4) return -1;
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, bufsz - 3,
                                    (const char *)(field_ptr + 2),
                                    (size_t)len);
        if (esc < 0) return -1;
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    case FT_DATE:
    case FT_DATETIME:
    case FT_DATETIMEMS:
    case FT_IPV4:
    case FT_IPV6:
    case FT_ENUM:
        /* Enum's display string is a JSON string (quoted, escaped).
           DATE/DATETIME are also strings on the wire. */
        n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
        if (n <= 0) return snprintf(buf, bufsz, "null");
        return snprintf(buf, bufsz, "\"%s\"", tmp);
    default:
        n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
        if (n <= 0) return snprintf(buf, bufsz, "null");
        return snprintf(buf, bufsz, "%s", tmp);
    }
}

/* Write one join's contribution (,val,val,...) to buf. remote_raw NULL → nulls. */
int buf_join_values(const JoinSpec *j, const uint8_t *remote_raw,
                           char *buf, size_t bufsz) {
    int pos = 0;
    if (j->include_remote_key) {
        /* v1: emit null — local field gives the value; extend later if needed */
        pos += snprintf(buf + pos, bufsz - pos, ",null");
    }
    for (int k = 0; k < j->proj_count; k++) {
        if (pos >= (int)bufsz - 1) break;
        pos += snprintf(buf + pos, bufsz - pos, ",");
        if (!remote_raw || !j->proj_tfs[k])
            pos += snprintf(buf + pos, bufsz - pos, "null");
        else
            pos += buf_field_value(j->proj_tfs[k],
                                   remote_raw + j->proj_tfs[k]->offset,
                                   buf + pos, bufsz - pos);
    }
    return pos;
}

/* Write driver-side row values (,val,val,...) after the key. */
int buf_driver_values(const uint8_t *driver_raw, FieldSchema *driver_fs,
                             const char **driver_proj, int driver_proj_count,
                             char *buf, size_t bufsz) {
    int pos = 0;
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++) {
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf(buf + pos, bufsz - pos, ",");
            if (driver_fs && driver_fs->ts) {
                int idx = typed_field_index(driver_fs->ts, driver_proj[i]);
                if (idx >= 0) {
                    pos += buf_field_value(&driver_fs->ts->fields[idx],
                                           driver_raw + driver_fs->ts->fields[idx].offset,
                                           buf + pos, bufsz - pos);
                    continue;
                }
            }
            pos += snprintf(buf + pos, bufsz - pos, "null");
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf(buf + pos, bufsz - pos, ",");
            pos += buf_field_value(&driver_fs->ts->fields[i],
                                   driver_raw + driver_fs->ts->fields[i].offset,
                                   buf + pos, bufsz - pos);
        }
    }
    return pos;
}
```

The caller, `adv_search_cb` (same file), compounds the effect across both functions and then does an **unconditional** final write:

```c
                } else if (sc->njoins > 0) {
                    /* Tabular JSON row: [driver.key, driver fields..., join1 fields..., ...] */
                    char row[16384];
                    int pos = snprintf(row, sizeof(row), "%s[\"%s\"",
                                       sc->printed ? "," : "", key);
                    pos += buf_driver_values((const uint8_t *)raw, sc->fs,
                                             sc->proj_count > 0 ? sc->proj_fields : NULL,
                                             sc->proj_count,
                                             row + pos, sizeof(row) - pos);
                    for (int i = 0; i < sc->njoins && pos < (int)sizeof(row) - 2; i++)
                        pos += buf_join_values(&sc->joins[i], join_raws[i],
                                               row + pos, sizeof(row) - pos);
                    snprintf(row + pos, sizeof(row) - pos, "]");
                    OUT("%s", row);
```

If a projected field's rendered value is long enough to overflow the shrinking remaining space, `buf_field_value`'s raw `snprintf` return inflates `pos` beyond `sizeof(row)`. The very next `snprintf(row + pos, sizeof(row) - pos, "]")` then computes `row + pos` (a pointer past the 16384-byte stack array) and `sizeof(row) - pos` (a `size_t` underflow to a huge value, since `pos > sizeof(row)`), and writes 2 bytes at that out-of-bounds address — a real stack write past the buffer's end, reachable through any joined `find` query (tabular JSON format) with sufficiently large field content.

### The fix

Introduce one small wrapper, `snprintf_bounded`, that always returns the number of bytes *actually written or that would fit* (never more than `bufsz - 1`), and use it everywhere in this file that currently accumulates a raw `snprintf`/formatting return value into a running buffer offset. This preserves the existing invariant the `having_buf` code elsewhere in the codebase already relies on (a clamped return value can never desync `pos` from the buffer), applied consistently here.

In `src/db/query_join.c`, find this exact block (the file's includes):

```c
#include "types.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include <math.h>
#include <dirent.h>
```

Replace it with:

```c
#include "types.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include <math.h>
#include <dirent.h>
#include <stdarg.h>
```

Next, find this exact block (the comment immediately before `buf_field_value`):

```c
/* Write one field value as a JSON token (number/"string"/null) into buf.
   Returns bytes written (>= 0). Safe for worker threads. */
static int buf_field_value(const TypedField *tf, const uint8_t *field_ptr,
                           char *buf, size_t bufsz) {
    if (!tf) return snprintf(buf, bufsz, "null");
```

Replace it with:

```c
/* snprintf() reports the length it *would* write even when the
   destination is too small to hold it. Accumulating that raw return
   value into a running buffer offset (`pos += snprintf(buf+pos,
   bufsz-pos, ...)`) lets a single truncation permanently desync pos
   from the buffer: the next call in the chain computes `buf + pos`
   past the end of the buffer, and `bufsz - pos` (size_t - int, with
   pos > bufsz) wraps around to a huge unsigned size — both feed an
   out-of-bounds write to the following snprintf. This wrapper clamps
   to what was actually written / would fit, so pos can never exceed
   bufsz - 1 (CID 1696463, CID 1696458). */
static int snprintf_bounded(char *buf, size_t bufsz, const char *fmt, ...) {
    if (bufsz == 0) return 0;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, bufsz, fmt, ap);
    va_end(ap);
    if (n < 0) return 0;
    return (n >= (int)bufsz) ? (int)bufsz - 1 : n;
}

/* Write one field value as a JSON token (number/"string"/null) into buf.
   Returns bytes written; always < bufsz, so accumulating this into a
   running offset can never overrun the caller's buffer (see
   snprintf_bounded above). Safe for worker threads. */
static int buf_field_value(const TypedField *tf, const uint8_t *field_ptr,
                           char *buf, size_t bufsz) {
    if (!tf) return snprintf_bounded(buf, bufsz, "null");
```

Next, find this exact block (the rest of `buf_field_value`, continuing from `FT_VARCHAR`):

```c
    char tmp[512];
    int n;
    switch (tf->type) {
    case FT_VARCHAR: {
        int len = varchar_eff_len(field_ptr, tf->size);
        if (len == 0) return snprintf(buf, bufsz, "\"\"");
        /* Escape per RFC 8259 — varchar content is opaque bytes,
           may contain " \ or control chars. Caller sizes buf for
           up to 6 * len + 2 worst case. NUL-terminate so callers
           that pass the buffer to printf-%s read a bounded string. */
        if (bufsz < 4) return -1;
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, bufsz - 3,
                                    (const char *)(field_ptr + 2),
                                    (size_t)len);
        if (esc < 0) return -1;
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    case FT_DATE:
    case FT_DATETIME:
    case FT_DATETIMEMS:
    case FT_IPV4:
    case FT_IPV6:
    case FT_ENUM:
        /* Enum's display string is a JSON string (quoted, escaped).
           DATE/DATETIME are also strings on the wire. */
        n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
        if (n <= 0) return snprintf(buf, bufsz, "null");
        return snprintf(buf, bufsz, "\"%s\"", tmp);
    default:
        n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
        if (n <= 0) return snprintf(buf, bufsz, "null");
        return snprintf(buf, bufsz, "%s", tmp);
    }
}
```

Replace it with:

```c
    char tmp[512];
    int n;
    switch (tf->type) {
    case FT_VARCHAR: {
        int len = varchar_eff_len(field_ptr, tf->size);
        if (len == 0) return snprintf_bounded(buf, bufsz, "\"\"");
        /* Escape per RFC 8259 — varchar content is opaque bytes,
           may contain " \ or control chars. Caller sizes buf for
           up to 6 * len + 2 worst case. NUL-terminate so callers
           that pass the buffer to printf-%s read a bounded string.
           Returns 0 (not -1) when it doesn't fit, so a caller doing
           pos += buf_field_value(...) never has pos move backwards. */
        if (bufsz < 4) return 0;
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, bufsz - 3,
                                    (const char *)(field_ptr + 2),
                                    (size_t)len);
        if (esc < 0) return 0;
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    case FT_DATE:
    case FT_DATETIME:
    case FT_DATETIMEMS:
    case FT_IPV4:
    case FT_IPV6:
    case FT_ENUM:
        /* Enum's display string is a JSON string (quoted, escaped).
           DATE/DATETIME are also strings on the wire. */
        n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
        if (n <= 0) return snprintf_bounded(buf, bufsz, "null");
        return snprintf_bounded(buf, bufsz, "\"%s\"", tmp);
    default:
        n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
        if (n <= 0) return snprintf_bounded(buf, bufsz, "null");
        return snprintf_bounded(buf, bufsz, "%s", tmp);
    }
}
```

Next, find this exact block (`buf_join_values`):

```c
/* Write one join's contribution (,val,val,...) to buf. remote_raw NULL → nulls. */
int buf_join_values(const JoinSpec *j, const uint8_t *remote_raw,
                           char *buf, size_t bufsz) {
    int pos = 0;
    if (j->include_remote_key) {
        /* v1: emit null — local field gives the value; extend later if needed */
        pos += snprintf(buf + pos, bufsz - pos, ",null");
    }
    for (int k = 0; k < j->proj_count; k++) {
        if (pos >= (int)bufsz - 1) break;
        pos += snprintf(buf + pos, bufsz - pos, ",");
        if (!remote_raw || !j->proj_tfs[k])
            pos += snprintf(buf + pos, bufsz - pos, "null");
        else
            pos += buf_field_value(j->proj_tfs[k],
                                   remote_raw + j->proj_tfs[k]->offset,
                                   buf + pos, bufsz - pos);
    }
    return pos;
}
```

Replace it with:

```c
/* Write one join's contribution (,val,val,...) to buf. remote_raw NULL → nulls.
   Returns bytes written; always < bufsz (see snprintf_bounded above). */
int buf_join_values(const JoinSpec *j, const uint8_t *remote_raw,
                           char *buf, size_t bufsz) {
    int pos = 0;
    if (j->include_remote_key) {
        /* v1: emit null — local field gives the value; extend later if needed */
        pos += snprintf_bounded(buf + pos, bufsz - pos, ",null");
    }
    for (int k = 0; k < j->proj_count; k++) {
        if (pos >= (int)bufsz - 1) break;
        pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
        if (!remote_raw || !j->proj_tfs[k])
            pos += snprintf_bounded(buf + pos, bufsz - pos, "null");
        else
            pos += buf_field_value(j->proj_tfs[k],
                                   remote_raw + j->proj_tfs[k]->offset,
                                   buf + pos, bufsz - pos);
    }
    return pos;
}
```

Next, find this exact block (`buf_driver_values`):

```c
/* Write driver-side row values (,val,val,...) after the key. */
int buf_driver_values(const uint8_t *driver_raw, FieldSchema *driver_fs,
                             const char **driver_proj, int driver_proj_count,
                             char *buf, size_t bufsz) {
    int pos = 0;
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++) {
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf(buf + pos, bufsz - pos, ",");
            if (driver_fs && driver_fs->ts) {
                int idx = typed_field_index(driver_fs->ts, driver_proj[i]);
                if (idx >= 0) {
                    pos += buf_field_value(&driver_fs->ts->fields[idx],
                                           driver_raw + driver_fs->ts->fields[idx].offset,
                                           buf + pos, bufsz - pos);
                    continue;
                }
            }
            pos += snprintf(buf + pos, bufsz - pos, "null");
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf(buf + pos, bufsz - pos, ",");
            pos += buf_field_value(&driver_fs->ts->fields[i],
                                   driver_raw + driver_fs->ts->fields[i].offset,
                                   buf + pos, bufsz - pos);
        }
    }
    return pos;
}
```

Replace it with:

```c
/* Write driver-side row values (,val,val,...) after the key.
   Returns bytes written; always < bufsz (see snprintf_bounded above). */
int buf_driver_values(const uint8_t *driver_raw, FieldSchema *driver_fs,
                             const char **driver_proj, int driver_proj_count,
                             char *buf, size_t bufsz) {
    int pos = 0;
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++) {
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
            if (driver_fs && driver_fs->ts) {
                int idx = typed_field_index(driver_fs->ts, driver_proj[i]);
                if (idx >= 0) {
                    pos += buf_field_value(&driver_fs->ts->fields[idx],
                                           driver_raw + driver_fs->ts->fields[idx].offset,
                                           buf + pos, bufsz - pos);
                    continue;
                }
            }
            pos += snprintf_bounded(buf + pos, bufsz - pos, "null");
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
            pos += buf_field_value(&driver_fs->ts->fields[i],
                                   driver_raw + driver_fs->ts->fields[i].offset,
                                   buf + pos, bufsz - pos);
        }
    }
    return pos;
}
```

Finally, find this exact block (the caller in `adv_search_cb`):

```c
                } else if (sc->njoins > 0) {
                    /* Tabular JSON row: [driver.key, driver fields..., join1 fields..., ...] */
                    char row[16384];
                    int pos = snprintf(row, sizeof(row), "%s[\"%s\"",
                                       sc->printed ? "," : "", key);
                    pos += buf_driver_values((const uint8_t *)raw, sc->fs,
                                             sc->proj_count > 0 ? sc->proj_fields : NULL,
                                             sc->proj_count,
                                             row + pos, sizeof(row) - pos);
                    for (int i = 0; i < sc->njoins && pos < (int)sizeof(row) - 2; i++)
                        pos += buf_join_values(&sc->joins[i], join_raws[i],
                                               row + pos, sizeof(row) - pos);
                    snprintf(row + pos, sizeof(row) - pos, "]");
                    OUT("%s", row);
```

Replace it with:

```c
                } else if (sc->njoins > 0) {
                    /* Tabular JSON row: [driver.key, driver fields..., join1 fields..., ...] */
                    char row[16384];
                    int pos = snprintf_bounded(row, sizeof(row), "%s[\"%s\"",
                                       sc->printed ? "," : "", key);
                    pos += buf_driver_values((const uint8_t *)raw, sc->fs,
                                             sc->proj_count > 0 ? sc->proj_fields : NULL,
                                             sc->proj_count,
                                             row + pos, sizeof(row) - pos);
                    for (int i = 0; i < sc->njoins && pos < (int)sizeof(row) - 2; i++)
                        pos += buf_join_values(&sc->joins[i], join_raws[i],
                                               row + pos, sizeof(row) - pos);
                    snprintf_bounded(row + pos, sizeof(row) - pos, "]");
                    OUT("%s", row);
```

### Regression test

Add a new file `src/test/cases/test_coverity_join_buf_overflow.c`:

```c
/* src/test/cases/test_coverity_join_buf_overflow.c
 * CID 1696463 / CID 1696458: buf_join_values / buf_driver_values (and the
 * buf_field_value they call) accumulated snprintf's unclamped "would have
 * written" return value into a running buffer offset, so once one field's
 * rendered value didn't fit, `pos` desynced from the buffer and later
 * calls computed an out-of-bounds `buf + pos` with an underflowed,
 * huge `bufsz - pos`. This test calls buf_driver_values directly with a
 * deliberately tiny destination buffer (immediately followed by a canary
 * region) and several typed fields whose rendered values don't fit, to
 * force exactly the truncation chain that used to desync pos. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "query_internal.h"
#include <stdlib.h>
#include <string.h>

extern int buf_driver_values(const uint8_t *driver_raw, FieldSchema *driver_fs,
                             const char **driver_proj, int driver_proj_count,
                             char *buf, size_t bufsz);

static int test_coverity_join_buf_overflow_run(void) {
    /* Build a typed schema with several long-ish varchar fields so their
       rendered JSON values are each bigger than the tiny buffer below. */
    TypedSchema ts;
    memset(&ts, 0, sizeof(ts));
    ts.nfields = 4;
    int off = 0;
    for (int i = 0; i < ts.nfields; i++) {
        TypedField *f = &ts.fields[i];
        memset(f, 0, sizeof(*f));
        snprintf(f->name, sizeof(f->name), "f%d", i);
        f->type = FT_VARCHAR;
        f->size = 64; /* on-disk: 2-byte length prefix + 62 content bytes */
        f->offset = off;
        off += f->size;
    }
    ts.total_size = off;

    FieldSchema fs;
    memset(&fs, 0, sizeof(fs));
    fs.ts = &ts;

    /* Build a raw record buffer with each field filled to near its cap
       with distinct content, so JSON-escaping renders a value clearly
       longer than the destination buffer used below. */
    uint8_t *raw = calloc(1, (size_t)ts.total_size);
    ASSERT_NOT_NULL(raw, "alloc raw record");
    if (!raw) return 1;
    for (int i = 0; i < ts.nfields; i++) {
        TypedField *f = &ts.fields[i];
        int content_len = f->size - 2;
        uint8_t *p = raw + f->offset;
        p[0] = (uint8_t)((content_len >> 8) & 0xff);
        p[1] = (uint8_t)(content_len & 0xff);
        memset(p + 2, 'A' + i, (size_t)content_len);
    }

    /* Destination buffer tiny enough that not all 4 fields fit, followed
       by a canary region — any OOB write past bufsz shows up here. */
    const size_t BUF_SZ = 40;
    const size_t CANARY_SZ = 256;
    char *region = malloc(BUF_SZ + CANARY_SZ);
    ASSERT_NOT_NULL(region, "alloc region");
    if (!region) { free(raw); return 1; }
    memset(region + BUF_SZ, 0xAB, CANARY_SZ);

    int pos = buf_driver_values(raw, &fs, NULL, 0, region, BUF_SZ);

    ASSERT_TRUE(pos >= 0 && (size_t)pos < BUF_SZ, "returned pos stays within bufsz");

    int canary_intact = 1;
    for (size_t i = 0; i < CANARY_SZ; i++) {
        if ((unsigned char)region[BUF_SZ + i] != 0xAB) { canary_intact = 0; break; }
    }
    ASSERT_TRUE(canary_intact, "no write past the declared buffer size");

    free(raw);
    free(region);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-coverity-join-buf-overflow", test_coverity_join_buf_overflow_run)
```

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-coverity-join-buf-overflow`. Paste the real output.

---

## Final verification

After all three tasks:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-coverity-encode-criterion-overflow
./build/bin/shard-db-test run test-coverity-group-by-overflow
./build/bin/shard-db-test run test-coverity-join-buf-overflow
./build/bin/shard-db-test run-all
```

Paste the real terminal output of the final `run-all`, ending in `# total: N passed, 0 failed`, before considering this plan complete. Leave the branch uncommitted for review.
