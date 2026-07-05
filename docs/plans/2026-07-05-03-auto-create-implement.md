# Implement `:auto_create` properly (stamp on first insert, preserve on update)

## Execution rules (read first)

- Branch off `main`: `git checkout -b feat/auto-create-implement`.
- Task 1 (failing test) first; paste its failure output before the fix.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run test-auto-create`, then `run-all`.
- Anchors are quoted exact text. Anchor not found exactly → STOP, write
  `PLAN_NOTES.md`. Never claim a pass without real output. Leave uncommitted.

## Background

`DK_AUTO_CREATE` parses, appears in `describe-object` as
`"default":"auto_create"`, but no insert path ever writes it — the field stays
zero forever. Only `DK_AUTO_UPDATE` has runtime code. (Memory notes #6492, #6965.)

**Decision (user, 2026-07-05):** implement it for real. Semantics:

- **First insert** of a key → stamp `now()` into the auto_create field.
- **Any later write** (update, or re-insert/upsert of an existing key) →
  **preserve** the stored create-time; never overwrite it.
- A record that predates the field (added post-hoc) has an unknown create-time →
  leave it blank (consistent with backfill's "stays zero").

## Why this is implementable (verified 2026-07-05)

`cmd_insert_v2` is an upsert. `typed_encode_defaults` leaves auto_create fields
blank, so a re-insert would zero the create-time. The write path *does* know
existence — `v2_insert_pre_commit` receives `old` + `is_update` — but it fires
**after** the value is written to a segment, too late to change the stored bytes
(`slotcask.h`: "pre_commit_fn fires AFTER the new record is written"). `check_fn`
fires before the write but only sees `old`, not a mutable value buffer.

So the correct hook is a **guarded pre-fetch** in `cmd_insert_v2`: when (and only
when) the schema declares an auto_create field, `slotcask_get` the key before the
upsert and either preserve (update) or stamp (insert) into `typed_buf`. Objects
without an auto_create field never pay the extra read.

> **Scope:** Phase 1 (Tasks 1–4) makes single-record `insert` correct. Phase 2
> (Task 5) extends to the bulk paths and is **gated** — the executor implements
> it only if the bulk workers already expose the prior record; otherwise it
> stops and reports. Do not rearchitect the bulk engine under this plan.

---

## Task 1 — Failing test

Create `src/test/cases/test_auto_create.c`:

```c
/* src/test/cases/test_auto_create.c
 * :auto_create stamps now() on first insert and preserves it on re-insert. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>

/* Extract the decimal value of "field":<digits> from a JSON response. */
static char *extract_field_num(const char *resp, const char *field) {
    if (!resp) return NULL;
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", field);
    const char *p = strstr(resp, pat);
    if (!p) return NULL;
    p += strlen(pat);
    const char *s = p;
    while (*p && (*p == '-' || (*p >= '0' && *p <= '9'))) p++;
    size_t n = (size_t)(p - s);
    if (n == 0) return NULL;
    char *out = malloc(n + 1);
    memcpy(out, s, n); out[n] = '\0';
    return out;
}

static int test_auto_create_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"ac\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"ac\",\"object\":\"t\",\"splits\":16,"
        "\"max_key\":16,\"fields\":[\"data:varchar:16\",\"created:timestamp:auto_create\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create obj"); free(resp); resp = NULL;

    /* First insert: created must be stamped (non-zero epoch ms). */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k1\","
        "\"value\":{\"data\":\"a\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k1\"}", &resp);
    char *c1 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(c1, "created present after insert");
    ASSERT_TRUE(c1 && strcmp(c1, "0") != 0, "created stamped non-zero on insert");
    free(resp); resp = NULL;

    /* Re-insert (upsert) same key with new data: created must be PRESERVED. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k1\","
        "\"value\":{\"data\":\"b\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"data\":\"b\"", "data updated on re-insert");
    char *c2 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(c2, "created present after re-insert");
    ASSERT_TRUE(c1 && c2 && strcmp(c1, c2) == 0, "created preserved across re-insert");
    free(resp); resp = NULL;
    free(c1); free(c2);

    /* Explicit auto_create value in the JSON must be overwritten by the
       server stamp on a FRESH insert too — this is the case that guards
       against ever dropping the "redundant" re-stamp in the !existed
       branch (see review note in this plan's Task 3). */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k2\","
        "\"value\":{\"data\":\"c\",\"created\":9999999}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k2\"}", &resp);
    char *c3 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(c3, "created present on client-supplied insert");
    ASSERT_TRUE(c3 && strcmp(c3, "0") != 0, "client-supplied created overwritten (non-zero)");
    ASSERT_TRUE(c3 && strcmp(c3, "9999999") != 0, "client-supplied created value not preserved");
    free(resp); resp = NULL;
    free(c3);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-create", test_auto_create_run)
```

Register in `build.sh` after the `src/test/cases/test_auto_key.c \` anchor
(same anchor as plans 01/02 — order among test lines is irrelevant):

```
    src/test/cases/test_auto_create.c \
```

Build + run — MUST fail (`created` currently stays 0). Paste output.

---

## Task 2 — Add the `auto_now_str` helper

In `src/db/storage.c`, anchor on this exact line:

```c
static int cmd_insert_v2(const char *db_root, const char *object,
```

Insert **immediately before** it:

```c
/* Produce the "now" string for an auto_create / auto_update timestamp field in
   the form its type expects. buf must be >= 24 bytes.
     FT_TIMESTAMP  — Unix epoch ms (decimal)
     FT_DATETIMEMS — yyyyMMddHHmmssSSS
     FT_DATE       — yyyyMMdd
     other (FT_DATETIME / fallback) — yyyyMMddHHmmss */
static void auto_now_str(const TypedField *f, char *buf, size_t bufsz) {
    if (f->type == FT_TIMESTAMP) {
        struct timespec tsn; clock_gettime(CLOCK_REALTIME, &tsn);
        long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
        snprintf(buf, bufsz, "%lld", ms);
    } else if (f->type == FT_DATETIMEMS) {
        struct timespec tsn; clock_gettime(CLOCK_REALTIME, &tsn);
        time_t nowsec = tsn.tv_sec; struct tm tm; localtime_r(&nowsec, &tm);
        int msec = (int)(tsn.tv_nsec / 1000000L);
        snprintf(buf, bufsz, "%04d%02d%02d%02d%02d%02d%03d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
    } else {
        time_t now = time(NULL); struct tm tm; localtime_r(&now, &tm);
        if (f->type == FT_DATE)
            snprintf(buf, bufsz, "%04d%02d%02d",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
        else
            snprintf(buf, bufsz, "%04d%02d%02d%02d%02d%02d",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                     tm.tm_hour, tm.tm_min, tm.tm_sec);
    }
}

```

---

## Task 3 — Guarded pre-fetch in `cmd_insert_v2`

In `src/db/storage.c`, anchor on this exact block:

```c
    /* Wire up compact trim for VARIABLE-format typed objects. */
    if (sdb->format == SLOTCASK_FORMAT_VARIABLE && !sdb->trim_fn) {
        sdb->trim_fn  = schema_trim_fn;
        sdb->trim_ctx = (void *)ts;
    }
```

Insert **immediately after** it:

```c

    /* auto_create: stamp now() on first insert only; preserve the stored value
       on any update / re-insert. This path is an upsert, so a re-insert of an
       existing key would otherwise zero the create-time (typed_encode_defaults
       leaves DK_AUTO_CREATE fields blank). We consult the prior record — but
       only when the schema actually declares an auto_create field, so ordinary
       objects pay nothing. */
    {
        int has_ac = 0;
        for (int i = 0; i < ts->nfields; i++)
            if (!ts->fields[i].removed &&
                ts->fields[i].default_kind == DK_AUTO_CREATE) { has_ac = 1; break; }
        if (has_ac) {
            void *ac_old = NULL; size_t ac_old_vlen = 0;
            int existed = (slotcask_get(sdb, key, klen, &ac_old, &ac_old_vlen) == 0);
            for (int i = 0; i < ts->nfields; i++) {
                if (ts->fields[i].removed ||
                    ts->fields[i].default_kind != DK_AUTO_CREATE) continue;
                size_t off = (size_t)ts->fields[i].offset;
                size_t w   = (size_t)ts->fields[i].size;
                if (existed && ac_old && ac_old_vlen >= off + w) {
                    memcpy(typed_buf + off, (uint8_t *)ac_old + off, w);
                } else if (!existed) {
                    /* Re-stamp unconditionally, even though typed_encode_defaults
                       already stamped now() for a client-omitted field. Do NOT
                       "optimize" this away: if the client explicitly supplied a
                       value for this field, typed_encode_defaults wrote THAT
                       value (seen[i]=1 skips generate_default), and this is the
                       only place that overwrites it — removing this branch lets
                       a client-supplied auto_create value survive a fresh
                       insert, which the field's contract forbids. The extra
                       clock_gettime on fresh inserts is negligible. */
                    char tbuf[24];
                    auto_now_str(&ts->fields[i], tbuf, sizeof(tbuf));
                    encode_field(&ts->fields[i], tbuf, typed_buf + off);
                }
                /* existed but old too short (field added post-hoc): leave blank. */
            }
            free(ac_old);
        }
    }
```

> Uses the same `slotcask_get(sdb, key, klen, &out, &outlen) == 0` contract as
> the update path elsewhere in this file. Both the fast-insert and upsert
> branches below run after this block, so both are covered.

---

## Task 4 — Verify single-record path

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-auto-create   # must pass
./build/bin/shard-db-test run-all
```

Paste both. Must end `# total: N passed, 0 failed`.

---

## Task 5 — Bulk paths (GATED — investigate before implementing)

`insert` is now correct. Bulk-insert (`bulk-insert`, JSON and CSV) is also an
upsert and must honor the same semantics. The bulk timestamp stamping for
`DK_AUTO_UPDATE` lives inline in the per-record workers around **`query.c` ~3400,
~3902, and ~4352** (search `default_kind == DK_AUTO_UPDATE`).

**Before writing any bulk code**, confirm at each of those sites whether the
worker has access to the prior record (an `old` / `SlotcaskOldRecord` pointer)
and an `is_update` flag at the point the value buffer is still mutable:

- **If yes** at a site: add a sibling branch — `if (... == DK_AUTO_CREATE)` — that
  on **insert** stamps `auto_now_str(...)` (mirroring the auto_update block) and
  on **update** copies the field's bytes from `old` into the new buffer. Add a
  bulk variant of the Task-1 test (create object with auto_create, `bulk-insert`
  two keys, re-`bulk-insert` one with changed data, assert created preserved).

- **If no** (the worker builds the value without the prior record, like the CSV
  worker at ~3900 which has no `old`): **STOP.** Write `PLAN_NOTES.md` naming the
  site(s) that lack `old`, and leave bulk auto_create unimplemented for this
  branch. Wiring the prior record into those workers is a separate design change
  the user must approve — do not attempt it here.

Whatever the outcome, state clearly in the branch's final message which bulk
paths now support auto_create and which are deferred.

---

## Invariants / notes

- The single-record `update` (partial) path already `memcpy`s the old value into
  the new buffer before applying provided fields, so it preserves create-time
  naturally — no change needed there. Verify by inspection; note it in the final
  message.
- Concurrent inserts of the same new key can each stamp their own `now()` (they
  share the object read-lock; slotcask serializes the write). Both values are
  valid creation instants within a few ms — acceptable, not a correctness bug.
- Do **not** remove the `describe-object` serialization of `auto_create`; it is
  now truthful.
