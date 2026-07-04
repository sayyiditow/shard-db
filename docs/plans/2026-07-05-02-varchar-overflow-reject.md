# Fix: over-length varchar values are silently truncated — reject with an error instead

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/varchar-overflow-reject`.
- Task 1 (failing test) is built and run first; paste its failure output.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run test-varchar-overflow`, then `run-all`.
- Anchors are quoted exact text. If one is not found exactly, STOP and write
  `PLAN_NOTES.md`. Do not guess. Never claim a pass without real output.
- Leave work uncommitted.

## Background

`encode_field_len` (config.c, FT_VARCHAR branch) clamps over-length content to
fit the field: `if ((int)vlen > content_max) vlen = content_max;` — then the
insert proceeds. No error, no signal. Truncated values then fail downstream
uniqueness/validation with a confusing "insert failed" and no hint that
truncation was the cause (memory notes #6029, #6966).

**Decision (user, 2026-07-05):** reject the insert with an explicit error. No
silent truncation, no truncated-flag.

## Write-path encode entry points (verified 2026-07-05)

- Plain `typed_encode` — **0 callers**, ignore.
- `typed_encode_defaults` (config.c) — the JSON `insert` and JSON `bulk-insert`
  path. Already returns **-2** with a message in `err_buf` for strict-enum
  rejection, which `cmd_insert_v2` surfaces as `{"error":"..."}`. We add varchar
  overflow to the same channel. **(Task 2)**
- Direct `encode_field_len` loop in the CSV/delimited bulk encoder
  (`query.c` ~2489 / ~2496). **(Task 3)**

The content cap for a varchar field is `f->size - 2` (2-byte length prefix).

---

## Task 1 — Failing test

Create `src/test/cases/test_varchar_overflow.c`:

```c
/* src/test/cases/test_varchar_overflow.c
 * Over-length varchar values must be rejected, not silently truncated. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>

static int test_varchar_overflow_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vo\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vo\",\"object\":\"t\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"name:varchar:8\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create obj"); free(resp); resp = NULL;

    /* 8-byte cap; send 20 bytes → must be rejected, not truncated+stored. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vo\",\"object\":\"t\",\"key\":\"k1\","
        "\"value\":{\"name\":\"abcdefghijklmnopqrst\"}}", &resp);
    ASSERT_CONTAINS(resp, "exceeds max", "over-length insert rejected"); free(resp); resp = NULL;

    /* The record must NOT exist (reject means no write). */
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"vo\",\"object\":\"t\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "false", "rejected insert left no record"); free(resp); resp = NULL;

    /* Exactly-at-cap value still succeeds. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vo\",\"object\":\"t\",\"key\":\"k2\","
        "\"value\":{\"name\":\"abcdefgh\"}}", &resp);
    ASSERT_CONTAINS(resp, "inserted", "at-cap insert accepted"); free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varchar-overflow", test_varchar_overflow_run)
```

Register in `build.sh` — anchor on this exact line:

```
    src/test/cases/test_auto_key.c \
```

Insert immediately after:

```
    src/test/cases/test_varchar_overflow.c \
```

> If plan 01 already added its test file after this same anchor, add this line
> after **that** one instead — order among test files does not matter.

Build + run. It MUST fail (currently truncates+stores). Paste output.

---

## Task 2 — Reject in `typed_encode_defaults` (JSON insert + bulk)

In `src/db/config.c`, anchor on this exact block inside `typed_encode_defaults`:

```c
                    if (ts->fields[i].type == FT_VARCHAR) {
                        char *unesc = NULL; size_t unesc_len = 0;
                        if (json_unescape_string(ev, el, &unesc, &unesc_len) == 0) {
                            encode_field_len(&ts->fields[i], unesc, unesc_len,
                                              out + ts->fields[i].offset);
                            free(unesc);
                        } else {
                            encode_field_len(&ts->fields[i], ev, el,
                                              out + ts->fields[i].offset);
                        }
                    } else {
```

Replace it **entirely** with:

```c
                    if (ts->fields[i].type == FT_VARCHAR) {
                        char *unesc = NULL; size_t unesc_len = 0;
                        int did_unesc =
                            (json_unescape_string(ev, el, &unesc, &unesc_len) == 0);
                        size_t stored_len = did_unesc ? unesc_len : el;
                        int content_max = ts->fields[i].size - 2;
                        if ((int)stored_len > content_max) {
                            if (err_buf && err_buf_size > 0)
                                snprintf(err_buf, err_buf_size,
                                    "value for field '%s' is %zu bytes; exceeds max %d for varchar",
                                    ts->fields[i].name, stored_len, content_max);
                            if (did_unesc) free(unesc);
                            return -2;
                        }
                        if (did_unesc) {
                            encode_field_len(&ts->fields[i], unesc, unesc_len,
                                              out + ts->fields[i].offset);
                            free(unesc);
                        } else {
                            encode_field_len(&ts->fields[i], ev, el,
                                              out + ts->fields[i].offset);
                        }
                    } else {
```

`cmd_insert_v2` already turns a `-2` return into `{"error":"<err_buf>"}` and
frees the buffer without writing — no other call-site change needed for the
JSON path.

---

## Task 3 — Reject in the CSV/delimited bulk encoder

In `src/db/query.c`, anchor on this exact block:

```c
        if (!has_tombstones) {
            for (int i = 0; i < active_count && i < nvals; i++) {
                if (vals[i].len > 0)
                    encode_field_len(&ts->fields[i], vals[i].ptr, vals[i].len,
                                     payload + ts->fields[i].offset);
            }
        } else {
            for (int i = 0; i < active_count && i < nvals; i++) {
                int fi = active_indices[i];
                if (vals[i].len > 0)
                    encode_field_len(&ts->fields[fi], vals[i].ptr, vals[i].len,
                                     payload + ts->fields[fi].offset);
            }
        }
```

Add an overflow guard by replacing it with (note the two `if (vals[i].len > 0)`
gains a length check that `continue`s past a bad row after flagging it):

```c
        int row_overflow = 0;
        if (!has_tombstones) {
            for (int i = 0; i < active_count && i < nvals; i++) {
                if (vals[i].len == 0) continue;
                if (ts->fields[i].type == FT_VARCHAR &&
                    (int)vals[i].len > ts->fields[i].size - 2) { row_overflow = 1; break; }
                encode_field_len(&ts->fields[i], vals[i].ptr, vals[i].len,
                                 payload + ts->fields[i].offset);
            }
        } else {
            for (int i = 0; i < active_count && i < nvals; i++) {
                int fi = active_indices[i];
                if (vals[i].len == 0) continue;
                if (ts->fields[fi].type == FT_VARCHAR &&
                    (int)vals[i].len > ts->fields[fi].size - 2) { row_overflow = 1; break; }
                encode_field_len(&ts->fields[fi], vals[i].ptr, vals[i].len,
                                 payload + ts->fields[fi].offset);
            }
        }
        if (row_overflow) {
            /* Skip this row rather than storing a truncated value. The bulk
               path reports per-row outcomes via its skipped/failed counter;
               do NOT append this record to `records`. */
            continue;
        }
```

> **IMPORTANT for the executor:** the exact loop that follows this block appends
> the record to `records[]` and there is a surrounding `for` over input rows.
> Confirm that `continue;` here lands in that per-row loop (so the bad row is
> skipped and the batch proceeds). If the enclosing control flow is NOT a simple
> per-row `for`/`while` where `continue` skips to the next row, STOP and write
> `PLAN_NOTES.md` — do not guess. The CSV bulk error-reporting contract (how
> skipped rows surface to the client) must be preserved; if unclear, note it and
> stop.

---

## Task 4 — Verify update path (read-only check, may add a follow-up note)

The partial-`update` path (`storage.c`, `cmd_update` around the `slotcask_get`
+ `memcpy(new_buf, old_val, old_vlen)`) applies only client-provided fields via
`encode_field`. Grep for `encode_field(` in that function. If an over-length
varchar can reach it, the same truncation applies on update.

- If update routes through `typed_encode_defaults`: already covered by Task 2 —
  note that in the branch's final message.
- If update encodes fields directly: add the same `size - 2` guard there, or (if
  the fix is non-trivial) record it in `PLAN_NOTES.md` as a scoped follow-up and
  leave update as-is. Do **not** expand scope silently.

---

## Task 5 — Verify

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-varchar-overflow   # must pass
./build/bin/shard-db-test run-all
```

Paste both. Suite must end `# total: N passed, 0 failed`. Leave uncommitted.

## Invariants

- The at-cap length (`stored_len == content_max`) is **accepted**; only strictly
  greater is rejected.
- The order-by sort-key cap at `config.c:1705` (`if (vlen > cap) vlen = cap;`) is
  a transient comparison key, not stored data — **leave it truncating**. Do not
  touch it.

---

## Task 6 — Fix regression in Task 3's bulk reject path (query.c)

**Found in review of the executed Task 3 diff.** The `row_overflow` block added
in `bulk_ins_delim_run` (query.c, inside the per-row loop around line 2486)
diverges from every other reject path in the same loop: it does not increment
`errors` and does not free `wire_for_record`.

Concretely (current code, already in the working tree):

```c
        if (row_overflow) {
            /* Skip this row rather than storing a truncated value. The bulk
               path reports per-row outcomes via its skipped/failed counter;
               do NOT append this record to `records`. */
            continue;
        }
```

Two bugs:
1. **Leak**: in auto-key mode, `wire_for_record` was heap-allocated a few lines
   earlier (`char *wbuf = malloc(klen + 1); ... wire_for_record = wbuf;` around
   line 2399-2401) and is never freed on this path. Compare the existing reject
   3 lines above it: `free(wbuf); errors++; continue;` (line ~2416-2418).
2. **Silent drop**: `errors` is not incremented, so if this is the only bad row
   in the batch, the final response is `{"inserted":N,"skipped":...}` with no
   `errors` field and no `"error"` key at all — the client cannot tell a row
   was dropped. This is the same silent-failure class this whole plan exists to
   eliminate, just moved from "truncated silently" to "dropped silently".

Fix — replace the block with:

```c
        if (row_overflow) {
            /* Skip this row rather than storing a truncated value. Count it
               like every other per-row reject in this loop so the client's
               errors/skipped tally reflects the drop. */
            free(wire_for_record);
            errors++;
            continue;
        }
```

`free(NULL)` is a no-op, so this is safe when `wire_for_record` was never set
(AK_NONE objects).

Build + run `./build/bin/shard-db-test run test-varchar-overflow` and `run-all`
after this change (folded into Task 8 below — no separate build step needed if
you do Tasks 6, 7, 8 in one pass).

---

## Task 7 — Reject in the update path (`storage.c`, `cmd_update_v2`)

Confirmed by grep (`encode_field(` in storage.c): the partial-update path
applies client-supplied fields via `encode_field`, which wraps
`encode_field_len` with `strlen()` and has no return value — no error channel
exists today, and no length check happens before the `memcpy`-equivalent write
inside `encode_field_len`. This is the same clamp-not-reject bug as the insert
path, on `cmd_update_v2`.

In `src/db/storage.c`, anchor on this exact block (inside `cmd_update_v2`):

```c
    for (int i = 0; i < ts->nfields; i++) {
        if (field_vals[i]) {
            if (!ts->fields[i].removed)
                encode_field(&ts->fields[i], field_vals[i],
                             new_buf + ts->fields[i].offset);
            free(field_vals[i]);
        }
    }
```

Replace it **entirely** with:

```c
    for (int i = 0; i < ts->nfields; i++) {
        if (field_vals[i]) {
            if (!ts->fields[i].removed) {
                if (ts->fields[i].type == FT_VARCHAR) {
                    int content_max = ts->fields[i].size - 2;
                    size_t vlen = strlen(field_vals[i]);
                    if ((int)vlen > content_max) {
                        char err[256];
                        snprintf(err, sizeof(err),
                            "value for field '%s' is %zu bytes; exceeds max %d for varchar",
                            ts->fields[i].name, vlen, content_max);
                        free(field_vals[i]);
                        for (int j = i + 1; j < ts->nfields; j++) free(field_vals[j]);
                        free(new_buf);
                        OUT("{\"error\":\"%s\"}\n", err);
                        return 1;
                    }
                }
                encode_field(&ts->fields[i], field_vals[i],
                             new_buf + ts->fields[i].offset);
            }
            free(field_vals[i]);
        }
    }
```

Notes for the executor:
- This early-`return 1` happens **before** `new_buf` is handed to
  `slotcask_upsert_with_hooks` — no partial write occurs, matching the
  insert-path invariant (reject means no write).
- The loop frees `field_vals[j]` for every remaining `j > i` before returning,
  since the normal per-iteration `free(field_vals[i])` at the bottom of the
  loop is skipped by the early return. Double-check no other cleanup
  (`crit`/`old_val`/etc.) is needed at this point in the function — `old_val`
  is already freed by this point (line ~1636, before this loop), and `crit`
  /`ncrit` are parsed **after** this loop (~line 1701-1703), so nothing else
  is live yet. If that ordering has changed since this plan was written, STOP
  and write `PLAN_NOTES.md`.
- `OUT(...)` is the existing response macro used elsewhere in this same
  function (e.g. `OUT("{\"error\":\"Not found\"}\n");` a few lines above) —
  no new include or macro needed.

Add test coverage to `src/test/cases/test_varchar_overflow.c`, appended before
`tc_close(tc);`:

```c
    /* Update path: over-length varchar must also be rejected, not truncated. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"vo\",\"object\":\"t\",\"key\":\"k2\","
        "\"value\":{\"name\":\"abcdefghijklmnopqrst\"}}", &resp);
    ASSERT_CONTAINS(resp, "exceeds max", "over-length update rejected"); free(resp); resp = NULL;

    /* k2's original at-cap value must be unchanged (reject means no write). */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vo\",\"object\":\"t\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "abcdefgh", "rejected update left original value intact"); free(resp); resp = NULL;
```

---

## Task 8 — Verify (supersedes Task 5)

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-varchar-overflow   # must pass, now 7 assertions
./build/bin/shard-db-test run-all
```

Paste both. Suite must end `# total: N passed, 0 failed`. Leave uncommitted.
