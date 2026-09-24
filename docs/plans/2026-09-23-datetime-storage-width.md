# Plan: widen `datetime` storage to 7 bytes (revision 2)

Revision 2, 2026-09-23. Supersedes the original draft after a verification
review. Owner design decision: **startup auto-migration, running after the
version gate, shared by embedded and server modes.**

Prior art: commit `40589f6` (2026-08-06, "automate migration on startup")
introduced `shard_db_startup_migrate()`; commit `497e497` (2026.08.1 gating
refactor) removed it. This plan reinstates that approach on top of the
current strict gate. The old `src/test/cases/test_startup_auto_migration.c`
(223 lines, same commit) is additional prior art for the migration test.

## Goal

Preserve every valid `datetime` second from `00:00:00` through `23:59:59` on
insert, read, indexing, criteria evaluation, ordering, and aggregate output —
and rewrite existing 6-byte objects to the 7-byte layout at startup.

## Root cause

`encode_field_len()` (the record-field encoder, `src/db/config.c:1656`)
computes seconds-of-day and truncates it to 16 bits at both of these sites:

- `src/db/config.c:1743` — record encoder,
  `FT_DATETIME` case: `uint16_t t = (uint16_t)(hh * 3600 + mm * 60 + ss);`
- `src/db/config.c:2009` — index-key encoder inside
  `encode_field_for_index()`: identical wrap-cast.

Seconds-of-day ranges over 0..86399, which needs 17 bits; a 2-byte time
field holds 65,536 patterns — pigeonhole, no encoding can fix it while
keeping second precision. 65535 s = `18:12:15` exactly, so ~24% of every day
(`18:12:16`–`23:59:59`) wraps silently: `23:59:59` (86399) persists as
`05:47:43` (20863), `18:12:16` (65536) persists as `00:00:00`. The decoder
is faithful to stored bytes, so round-trip tests below the boundary pass —
that is how this survived since the type was introduced.

The **criteria side has the same wrap**: `parse_datetime()` at
`src/db/query_plan.c:49` stores time-of-day in a `uint16_t`, so literal
evening values in `eq`/`range`/`between`/`in` criteria were also truncated
before comparison (`src/db/query_plan.c:377-382`, `:524-533`).

The fix widens the calendar `datetime` payload from 6 to 7 bytes
(`int32` date + 3-byte big-endian seconds). This matches the in-codebase
precedent: `FT_TIME` has stored seconds-since-midnight as exactly this
3-byte BE form since 2026-05-11 (`src/db/config.c:1768`, commit `02f3cbb`).
`docs/concepts/typed-records.md:51` documented the old layout as
"`HHmmss` (uint16 BE packed)" — a format that was never implementable; the
doc fix corrects the description, not just the width.

The decoder cannot recover values already truncated before migration; a
successful migration preserves the value the old bytes represent.

## Design decisions (owner-approved)

1. **Startup migration after the version gate.** `shard_db_version_decide()`
   (`src/db/util.c:773`) gains a `SHARD_DB_VERSION_MIGRATE` outcome for
   populated roots with `required ≤ disk < current`. Both startup paths —
   `shard_db_open` (`src/db/embedded.c:711`) and `cmd_server`
   (`src/db/server.c:3496`) — run `shard_db_startup_migrate()` when the
   decision is MIGRATE, then stamp. Both modes therefore migrate
   identically.
2. **Version bump; the minimum required source version does not change.**
   `SHARD_DB_VERSION` `"2026.09.1"` → `"2026.09.2"` in
   `src/db/version.h:6`. `SHARD_DB_REQUIRED_SOURCE_VERSION` stays
   `"2026.08.2"`: the MIGRATE band is `[REQUIRED, current)`, and the floor
   is that band's lower edge — a 2026.08.2 root upgrades directly to
   2026.09.2 and is migrated at first start, so the floor stays put for
   this release. The schema.conf engine-version slot stays the literal `2`
   (this migration is governed by the root `.version` file, not the v1/v2
   engine slot). Behavior change to note in the changelog: a populated
   `2026.08.2` root previously took the `STAMP` fast path (accept without
   rewriting); it now migrates like any other old root.
   **Forward policy (next release, not this one):** the release after
   2026.09.2 raises `SHARD_DB_REQUIRED_SOURCE_VERSION` to `"2026.09.2"`,
   closing the migration window — pre-migration roots (`.version` below
   2026.09.2) become `TOO_OLD` and must pass through 2026.09.2 first.
   Because the band is `[REQUIRED, current)`, that policy requires only
   the one-constant bump (and optionally retiring the startup migration
   once nothing below the floor can reach it); no new logic. Release notes
   must state this upgrade window to operators (task 8).
3. **Migration = `rebuild_object_v2` with explicit old/new schemas.** The
   rebuild engine already takes `old_ts`/`new_ts` pairs and recomposes
   record bytes field-by-field (`src/db/query_find.c:962`); per-field size
   changes already route through `field_needs_transform()` /
   `transform_field_value()` (`src/db/query_find.c:803` / `:708`) — the
   same machinery `edit-field` uses for varchar/int/enum resizes. The
   datetime widen is a new case there, not new machinery.
4. **Idempotency and failure.** Each object migrates inside the existing
   crash-safe `RebuildTxn` protocol (`src/db/objlock.c`). Rerun safety
   deliberately does NOT rely on txn markers (`rebuild_object_v2` cleans
   the committed marker right after `rebuild_txn_commit`,
   `query_find.c:1161`; startup recovery deletes leftover `.done` state,
   `objlock.c:736-741`) and NOT on an independently renamed marker file
   either — an independently renamed file has no rollback, so a crash
   after the rename but before commit would leave it over authoritative
   6-byte data. The marker instead lives INSIDE `fields.conf` as a
   final `#datetime_7byte` comment line, appended by the migration's
   `apply_metadata` finalize callback. It inherits fields.conf's exact
   protection with zero txn changes: `rebuild_txn_begin` snapshots
   `fields.conf` to `fields.conf.rollback` before the rebuild starts
   (`objlock.c:476`), `rebuild_txn_abort` restores that snapshot
   (marker erased, old data restored), and commit publishes the
   appended marker together with the widened data. `load_typed_schema`
   skips `#` lines and v2's fields.conf stager preserves them
   (`query_find.c:1106`), so the comment is inert to every reader. A
   rerun scans `fields.conf` for the marker line and skips marked
   objects. Index consistency: the migration sets
   `indexes_may_change = 1` and rebuilds indexes via the
   `rebuild_indexes` finalize callback (`selective_reindex_dirty`)
   BEFORE commit. The flag's role is rollback-only — `rebuild_txn_abort`
   runs a full `reindex_object_checked` (`objlock.c:550`) to resync
   indexes with the restored old data; commit itself never reindexes
   (it is a single rename, `objlock.c:503-510`). Success path: one
   selective reindex; failure path: attempted selective plus the
   abort-time full reindex. No duplication on either path. Any
   migration failure refuses startup with the root left unchanged for
   retry.
5. **Semantics preserved by the migration**: the wire format stays the
   14-digit `yyyyMMddHHmmss` string; `datetimems` (uint32 ms) is untouched;
   already-truncated values keep their stored (wrapped) meaning — the
   migration never pretends to restore lost seconds; deleted (tombstoned)
   records are dropped by the rebuild exactly as `vacuum`/`add-field`
   behave today; non-datetime objects are not rewritten.

## Byte-layout contract

```c
/* OLD (6 bytes): [0..3] BE int32 yyyyMMdd | [4..5] BE uint16 seconds-of-day */
/* NEW (7 bytes): [0..3] BE int32 yyyyMMdd | [4..6] BE seconds-of-day      */

/* Widen transform: zero-extend the 2-byte seconds into 3 bytes. */
dst[0..3] = src[0..3];
dst[4]    = 0;
dst[5]    = src[4];
dst[6]    = src[5];
```

Encoder (both sites), replacing the wrap-cast:

```c
uint32_t secs = (uint32_t)hh * 3600u + (uint32_t)mm * 60u + (uint32_t)ss;
out[4] = (uint8_t)(secs >> 16);
out[5] = (uint8_t)(secs >> 8);
out[6] = (uint8_t)secs;
```

Index keys stay memcmp-chronological (flipped date + BE seconds). The
datetime sort-key formula `date * 100000LL + seconds`
(`src/db/query_plan.c:870-871`) remains valid: 86399 < 100000. 7 bytes is
still fixed-width, so composite-index cursor pagination requirements are
unaffected. The all-zero sentinel (`d == 0 && t == 0`) stays all-zero under
widening.

## Consumer inventory (verified by codebase search)

Every site below is edited in task 3; anchors were verified 2026-09-23.

| Site | Anchor |
|---|---|
| Width contract (validation) | `src/db/query_schema.c:721` — in `validate_field_type()`: `if (strcmp(type, "datetime") == 0) return 6;` |
| Width assignment (parser) | `src/db/config.c:1409-1410` — `f->type = FT_DATETIME;` / `f->size = 6;` in `parse_field_type` |
| Record encoder | `src/db/config.c:1730` case, wrap-cast at `:1743` |
| Index encoder | `src/db/config.c:1995` case in `encode_field_for_index`, wrap-cast at `:2009`, `*out_len = 6;` at `:2010` |
| Raw key encoder | `src/db/config.c:2176` case in `typed_field_to_index_key`: `memcpy(out, src, 6);` / `*out_len = 6;` |
| JSON decoder | `src/db/config.c:3007` — `uint16_t t = ((uint16_t)data[4] << 8) | data[5];` |
| Field-string decoder | `src/db/config.c:3277` — `uint16_t t = ((uint16_t)d[4] << 8) | d[5];` |
| Enum comment | `src/db/types.h:137` — `FT_DATETIME,    /* datetime — 6 bytes packed ... */` |
| Index-layout comment | `src/db/config.c:1886` — `datetime — 4 bytes BE flipped-int32 date + 2 bytes BE uint16 time` |
| Criteria parse | `src/db/query_plan.c:49` — `parse_datetime` signature `uint16_t *out_time` |
| Criteria compile | `src/db/query_plan.c:377-382` — `int32_t d; uint16_t t;` + `cc->t1 = t;` / `cc->t2 = t;` |
| Criteria IN | `src/db/query_plan.c:524-533` — `int32_t d; uint16_t t;` in the IN loop |
| Sort key | `src/db/query_plan.c:869-871` — `ld_be_u16(a + 4)` / `ld_be_u16(b + 4)` |
| Typed match | `src/db/query_plan.c:1045` — `uint16_t t = ld_be_u16(p + 4);` and `cc->t1` at `:1048` |
| BE loader helper | `src/db/query_internal.h:235` — add `ld_be_u24` next to `ld_be_u16` |
| Aggregate zero-check | `src/db/query_aggregate.c:235` — `ld_be_i32(p) == 0 && ld_be_u16(p + 4) == 0` |
| Aggregate loads | `src/db/query_aggregate.c:1082`, `:2014` — `uint16_t t = ld_be_u16(p + 4);` |
| Aggregate packed loads | `src/db/query_aggregate.c:1238`, `:1405` — `uint16_t t = ((uint16_t)p[4] << 8) | (uint16_t)p[5];` and `:1304` — `uint16_t t = ((uint16_t)p[4] << 8) | p[5];` |
| Join sizing | `src/db/query_join.c:349` — `case FT_DATETIME:` field-value sizing |
| Compiled-criteria time fields | the struct holding `t1`/`t2` (locate from the `cc->t1 = t;` anchor above); widen to `uint32_t` |

**Verified no-change sites** (do not touch; reasons checked):

- `typed_encode_trim_len` (`config.c:3203`) — derives lengths from
  `f->offset`/`f->size`, so the parser change covers it. Corollary: after
  the parser flips to 7, old 6-byte objects would misparse — the version
  gate is the only guard, which is why migration is mandatory before any
  old object is touched.
- `type_desc.c:19` — datetime row carries `0` (no fixed size in that table).
- `auto_now_str` (`storage.c:920`), the auto_update stamping site
  (`storage.c:1300`), `gen_datetime_now` (`config.c`) — emit the unchanged
  14-digit wire string.
- `query.c` / `query_find.c` match paths — no `FT_DATETIME` byte access
  outside the rebuild engine (which gets the transform in task 5).
- `src/cli/`, benches — wire format unchanged.
- `src/db/bitmap.c:388` `return 6;` — bool fastpath encoding, unrelated
  to datetime.
- The only other hardcoded datetime width is `validate_field_type`
  (row above); a `rg -n "return 6;" src/db/*.c` sweep confirms no
  others.

## Tasks

### 1. Unit red: encoder boundary vectors

In `src/test/cases/test_config_encode.c`, replace:

```c
    TypedField f_dt = make_f(FT_DATETIME, 6, 0);
    memset(out, 0, 64); encode_field_len(&f_dt, "20260513123000", 14, out);
    ASSERT_TRUE(out[0] != 0 || out[4] != 0, "datetime packed");
```

with:

```c
    TypedField f_dt = make_f(FT_DATETIME, 7, 0);
    /* 12:30:00 → 45000 = 0x00AFC8 */
    memset(out, 0, 64); encode_field_len(&f_dt, "20260513123000", 14, out);
    ASSERT_EQ_INT(out[4], 0x00, "datetime secs b2");
    ASSERT_EQ_INT(out[5], 0xAF, "datetime secs b1");
    ASSERT_EQ_INT(out[6], 0xC8, "datetime secs b0");
    /* 18:12:15 → 65535 = old uint16 max — the last instant the old
       encoder could represent. */
    memset(out, 0, 64); encode_field_len(&f_dt, "20240102181215", 14, out);
    ASSERT_EQ_INT(out[4], 0x00, "181215 b2");
    ASSERT_EQ_INT(out[5], 0xFF, "181215 b1");
    ASSERT_EQ_INT(out[6], 0xFF, "181215 b0");
    /* 18:12:16 → 65536 — first instant the old encoder wrapped to
       00:00:00. */
    memset(out, 0, 64); encode_field_len(&f_dt, "20240102181216", 14, out);
    ASSERT_EQ_INT(out[4], 0x01, "181216 b2");
    ASSERT_EQ_INT(out[5], 0x00, "181216 b1");
    ASSERT_EQ_INT(out[6], 0x00, "181216 b0");
    /* 23:59:59 → 86399 = 0x01517F — old encoder wrapped to 05:47:43. */
    memset(out, 0, 64); encode_field_len(&f_dt, "20240102235959", 14, out);
    ASSERT_EQ_INT(out[4], 0x01, "235959 b2");
    ASSERT_EQ_INT(out[5], 0x51, "235959 b1");
    ASSERT_EQ_INT(out[6], 0x7F, "235959 b0");
```

Also sweep the test tree for other datetime-width fixtures
(`rg -n "FT_DATETIME, 6|FT_DATETIME,6" src/test/`) and update to 7.

**Red step:** run `./build/bin/shard-db-test run test-config-encode` on the
base implementation and paste the failure — the old encoder leaves byte 6
zero and wraps `181216`/`235959`.

### 2. Integration red: full-day datetime behavior

New case `src/test/cases/test_datetime_evening.c`, modeled on
`test_all_field_types.c:1-40` (same includes, `TestEnv`/`TestClient`
pattern, `TEST_REGISTER`):

```c
/* src/test/cases/test_datetime_evening.c
 * datetime must round-trip the full day, including values at and past
 * the legacy 18:12:15 wrap boundary. */
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

static int do_count(TestClient *tc, const char *obj, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":%s}", obj, crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_datetime_evening_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"test_dt_eve\",\"fields\":[\"v:datetime\"],"
        "\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-index\",\"dir\":\"default\","
        "\"object\":\"test_dt_eve\",\"field\":\"v\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"test_dt_eve\",\"records\":["
        "{\"key\":\"early\",\"value\":{\"v\":\"20240102004744\"}},"
        "{\"key\":\"noon\",\"value\":{\"v\":\"20240102120000\"}},"
        "{\"key\":\"last_ok\",\"value\":{\"v\":\"20240102181215\"}},"
        "{\"key\":\"first_wrap\",\"value\":{\"v\":\"20240102181216\"}},"
        "{\"key\":\"eod\",\"value\":{\"v\":\"20240102235959\"}}]}",
        &resp); free(resp); resp = NULL;

    /* Exact round-trip of every stored value, incl. past-boundary ones. */
    const char *keys[] = {"early", "noon", "last_ok", "first_wrap", "eod"};
    const char *vals[] = {"20240102004744", "20240102120000",
                          "20240102181215", "20240102181216",
                          "20240102235959"};
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":"
            "\"test_dt_eve\",\"key\":\"%s\"}", keys[i]);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(strstr(resp ? resp : "", vals[i]), vals[i]);
        free(resp); resp = NULL;
    }

    /* Indexed equality on a past-boundary value — verify the returned
       key, not just the count: the literal and first_wrap's stored time
       are the only 18:12:16 values, so assert early/noon are absent. */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\","
        "\"object\":\"test_dt_eve\",\"criteria\":[{\"field\":\"v\","
        "\"op\":\"eq\",\"value\":\"20240102181216\"}]}", &resp);
    ASSERT_NOT_NULL(strstr(resp ? resp : "", "\"first_wrap\""),
                    "eq 181216 returns first_wrap");
    ASSERT_TRUE(strstr(resp ? resp : "", "\"early\"") == NULL,
                "eq 181216 does not return early");
    free(resp); resp = NULL;

    /* Criteria-literal wrap detector: no record exists at 19:00:00
       (68400 s), but the old uint16 parser truncates the literal to
       68400 - 65536 = 2864 s = 00:47:44 — exactly the "early" record. */
    ASSERT_EQ_INT(do_count(tc, "test_dt_eve",
        "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"20240102190000\"}]"),
        0, "eq 19:00:00 does not match the 00:47:44 record");

    /* Late-day range: everything from 18:12:16 onward. */
    ASSERT_EQ_INT(do_count(tc, "test_dt_eve",
        "[{\"field\":\"v\",\"op\":\"gte\",\"value\":\"20240102181216\"}]"),
        2, "late-day gte");
    ASSERT_EQ_INT(do_count(tc, "test_dt_eve",
        "[{\"field\":\"v\",\"op\":\"between\",\"value\":\"20240102181216\","
        "\"value2\":\"20240102235959\"}]"), 2, "evening between");

    /* Complete ascending order — the full sequence, not a substring
       probe: early(00:47:44) < noon < last_ok(18:12:15) <
       first_wrap(18:12:16) < eod(23:59:59). Under the old encoder the
       stored seconds wrap and this order is impossible. */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\","
        "\"object\":\"test_dt_eve\",\"order_by\":\"v\","
        "\"criteria\":[{\"field\":\"v\",\"op\":\"exists\"}]}", &resp);
    {
        const char *p        = resp ? resp : "";
        const char *p_early  = strstr(p, "20240102004744");
        const char *p_noon   = strstr(p, "20240102120000");
        const char *p_last   = strstr(p, "20240102181215");
        const char *p_wrap   = strstr(p, "20240102181216");
        const char *p_eod    = strstr(p, "20240102235959");
        ASSERT_NOT_NULL(p_early, "early in ordered find");
        ASSERT_TRUE(p_early < p_noon && p_noon < p_last &&
                    p_last < p_wrap && p_wrap < p_eod,
                    "full ascending datetime order");
    }
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-datetime-evening", test_datetime_evening_run)
```

**Red step:** run `./build/bin/shard-db-test run test-datetime-evening` on
base and paste the failures. Counts below are empirically confirmed on
the pristine base binary against this exact 5-record fixture: the
`first_wrap`/`eod` round-trips return `20240102000000` / `20240102054743`
(wrapped storage); `gte 181216` matches all **5** records — the literal
wraps to 00:00:00, so every same-date record satisfies it; `between`
returns **3** (`early`, `first_wrap`, `eod` inside the wrapped bounds);
`eq 19:00:00` wrongly matches `early` (returns 1); and the full
ascending-order assertion fails (wrapped order differs, and the
unwrapped evening strings are absent entirely).

### 3. Width flip across all consumers

Make each edit at its inventoried anchor (table above):

1. Width, both sites: `src/db/query_schema.c:721` in `validate_field_type`
   — `if (strcmp(type, "datetime") == 0) return 6;` → `return 7;` — and
   `src/db/config.c:1409-1410` in `parse_field_type` — `f->size = 6;` →
   `f->size = 7;`.
2. Record encoder + index encoder: replace both `uint16_t t =
   (uint16_t)(hh * 3600 + mm * 60 + ss); out[4] = (t >> 8) & 0xFF;
   out[5] = t & 0xFF;` blocks with the 3-byte contract snippet; index
   encoder sets `*out_len = 7;`.
3. `typed_field_to_index_key`: `memcpy(out, src, 6); out[0] ^= 0x80;
   *out_len = 6;` → `memcpy(out, src, 7); out[0] ^= 0x80; *out_len = 7;`
   (records are always 7 bytes on this binary).
4. Both decoders: load seconds as
   `uint32_t t = ((uint32_t)data[4] << 16) | ((uint32_t)data[5] << 8) |
   data[6];` (and the `d[]` variant).
5. `ld_be_u24` helper in `query_internal.h` next to `ld_be_u16`:

   ```c
   static inline uint32_t ld_be_u24(const uint8_t *p) {
       return ((uint32_t)p[0] << 16) | ((uint32_t)p[1] << 8) |
              (uint32_t)p[2];
   }
   ```

6. `parse_datetime`: `uint16_t *out_time` → `uint32_t *out_time`; all
   callers (`cc->t1`/`cc->t2` compile, IN loop) use `uint32_t t`; widen the
   compiled-criteria struct fields `t1`/`t2` to `uint32_t`.
7. Sort key + typed match + all five aggregate load sites → `ld_be_u24(p +
   4)` / the 3-byte packed load. Join sizing case → 7.
8. Comments: `types.h:137` and `config.c:1886` describe 7 bytes
   (`4B flipped-int32 date + 3B BE seconds-of-day`).

**Green step:** tasks 1–2 pass. Then `SKIP_TESTS=1 ./build.sh` and
`./build/bin/shard-db-test run-all` — expect unrelated failures only in
migration-era cases (none exist yet on base).

### 4. Version-gate red: decision matrix

First add the enum value only (no behavior change), in
`src/db/version.h`, inside `enum ShardDbVersionDecision`:

```c
    SHARD_DB_VERSION_MIGRATE = 2,
```

New case `src/test/cases/test_version_gate.c` (unit case, no daemon —
follow an existing process-local unit case's skeleton):

```c
static int test_version_gate_run(void) {
    const char *cur  = "2026.09.2";
    const char *reqd = "2026.08.2";
    /* Populated old roots migrate. */
    ASSERT_EQ_INT(shard_db_version_decide("2026.09.1", 1, 0, cur, reqd),
                  SHARD_DB_VERSION_MIGRATE, "prev release migrates");
    ASSERT_EQ_INT(shard_db_version_decide("2026.08.2", 1, 0, cur, reqd),
                  SHARD_DB_VERSION_MIGRATE, "required release migrates");
    ASSERT_EQ_INT(shard_db_version_decide("2026.09.2", 1, 0, cur, reqd),
                  SHARD_DB_VERSION_NOOP, "current noop");
    ASSERT_EQ_INT(shard_db_version_decide("2026.08.1", 1, 0, cur, reqd),
                  SHARD_DB_VERSION_TOO_OLD, "pre-required too old");
    ASSERT_EQ_INT(shard_db_version_decide("2026.10.1", 1, 0, cur, reqd),
                  SHARD_DB_VERSION_DOWNGRADE, "newer disk refuses");
    /* Empty-root bootstrap unchanged. */
    ASSERT_EQ_INT(shard_db_version_decide(NULL, 0, 1, cur, reqd),
                  SHARD_DB_VERSION_STAMP, "empty stamps");
    ASSERT_EQ_INT(shard_db_version_decide("garbage", 1, 0, cur, reqd),
                  SHARD_DB_VERSION_INVALID, "malformed invalid");
    return 0;
}
TEST_REGISTER("test-version-gate", test_version_gate_run)
```

**Red step:** compile (enum now exists) and run on the unchanged
`shard_db_version_decide` — the two MIGRATE assertions fail: on base,
`decide("2026.09.1", current="2026.09.2")` returns
`SHARD_DB_VERSION_TOO_OLD` (the `disk == required → STAMP` special case
only matches 2026.08.2 exactly), and `decide("2026.08.2", …)` returns
`SHARD_DB_VERSION_STAMP`. Paste output.

### 5. Decision + version bump + transform implementations

1. **Version bump — must land before task 6.** The migration fixture
   writes `.version = 2026.09.1` and boots the real daemon, whose gate
   compares the file against the compiled `SHARD_DB_VERSION`; with the
   macro un-bumped the decision is `NOOP` and the migration path never
   runs. In `src/db/version.h:6`:
   `#define SHARD_DB_VERSION "2026.09.1"` →
   `#define SHARD_DB_VERSION "2026.09.2"`
   (`SHARD_DB_REQUIRED_SOURCE_VERSION` stays `"2026.08.2"`).
2. `shard_db_version_decide` (`src/db/util.c:793-799`) — replace the tail:

   ```c
       int current_cmp = shard_db_version_compare(disk_version, current_version);
       if (current_cmp > 0) return SHARD_DB_VERSION_DOWNGRADE;
       if (current_cmp == 0) return SHARD_DB_VERSION_NOOP;
       if (shard_db_version_compare(disk_version, required_source_version) >= 0)
           return SHARD_DB_VERSION_MIGRATE;
       return SHARD_DB_VERSION_TOO_OLD;
   ```

   (This retires the `disk == required → STAMP` special case: old roots now
   migrate, which is the point.)

3. Widen transform in `transform_field_value`
   (`src/db/query_find.c:708`) — add before the `default:` case:

   ```c
       case FT_DATETIME:
           /* 6 → 7 widen: zero-extend 2-byte BE seconds-of-day into 3
              bytes. Values already truncated by the old encoder keep
              their stored meaning. */
           if (old_f->size == 6 && new_f->size == 7) {
               memcpy(dst, src, 4);
               dst[4] = 0;
               dst[5] = src[4];
               dst[6] = src[5];
               return;
           }
           memcpy(dst, src, old_f->size < new_f->size ? old_f->size
                                                      : new_f->size);
           return;
   ```

   `field_needs_transform` already returns 1 on any size change, so no
   edit there. Update the function-head comment block (`varchar shrink …
   other types — same size required`) to list the datetime case.

### 6. Migration red: old-layout fixture end-to-end

New case `src/test/cases/test_datetime_migration.c`. It hand-builds a
6-byte datetime object exactly as the old binary would have stored it, then
boots the stack and lets startup migration rewrite it.

Fixture craft (before the daemon starts): create
`<root>/default/migrate_dt/` with `fields.conf` containing `v:datetime\n`,
append `default:migrate_dt:8:16:2:1\n` to `<root>/schema.conf`, and insert
four records by opening slotcask directly at the **old** geometry —
`slot_size = (24 + max_key + max_value + 7) & ~7` — the `+7` IS the
round-up, so (24 + 16 + 6) = 46 → **48** (56 would round twice), splits 8,
streams 1 — via `slotcask_open` + `slotcask_insert_with_hooks`, with
hand-encoded values
(key → 6 stored bytes = BE `20240102` + BE u16):

| key | stored seconds | meaning under old binary |
|---|---|---|
| `noon` | 45000 (`0xAFC0`) | `20240102120000` |
| `last_ok` | 65535 (`0xFFFF`) | `20240102181215` |
| `first_wrap` | 0 | inserted as `181216`, wrapped to `00:00:00` |
| `eod` | 20863 (`0x517F`) | inserted as `235959`, wrapped to `05:47:43` |

Write `2026.09.1\n` into `<root>/.version`, then start the daemon
(`test_env_start` or the harness's prepare-then-start API — see
PLAN_NOTES below) and assert:

1. `get` returns `20240102120000`, `20240102181215`, `20240102000000`,
   `20240102054743` respectively — **stored meaning preserved, nothing
   "restored"**.
2. A fresh insert of `20240102235959` round-trips exactly (full day now
   writable); late-day `gte 20240102181216` counts **only** the new
   insert = 1 — the widened `first_wrap` keeps its stored meaning
   (`20240102000000`, below 18:12:16, per assertion 1).
3. `<root>/.version` now reads `2026.09.2`.
4. `<obj_dir>/fields.conf` ends with the `#datetime_7byte` marker line —
   appended inside the txn window and protected by the same rollback
   copy as the rest of fields.conf (`objlock.c:476`).
5. Stop and restart the daemon on the same root: still `NOOP`, values
   unchanged (the rerun scans fields.conf for the marker and skips —
   no double-widen).
6. A control object without datetime fields in the same root is readable
   throughout.

**Red step:** run this on the tree after tasks 3–5 — 7-byte parser live,
`SHARD_DB_VERSION_MIGRATE` implemented in `shard_db_version_decide`, but
task 7's caller wiring not yet present. The gate decides `MIGRATE`, the
unwired callers ignore the new positive code, and the daemon boots on the
old root, misreading the hand-built 6-byte records through the 7-byte
parser: the value assertions fail with garbage datetimes and assertion 3
(`.version` reads `2026.09.2`) fails with the old stamp. (On the pristine
base binary the 6-byte parser would read the fixture correctly; the
fixture exists to fail the migration-aware tree.) Paste output.

PLAN_NOTES item: if `fixtures.h` only offers start-and-connect in one step,
add a prepare-root phase to the harness (boot the daemon *after* the
fixture craft) and record the API choice in `PLAN_NOTES.md`.

### 7. Startup migration implementation

1. `src/db/query_schema.c` — export the edit-field index-finalize
   machinery for the migration (it is a TU static today):

   ```c
   /* Exported for shard_db_startup_migrate (embedded.c): selective
      reindex of fields whose encoded keys changed, runnable inside the
      rebuild txn window — the same machinery edit-field's
      RebuildFinalizeOps drives via edit_finalize_indexes. */
   int migration_reindex_dirty(const char *db_root, const char *object,
                               const char (*dirty_names)[256],
                               int n_dirty, int *out_rebuilt,
                               int *out_skipped) {
       return selective_reindex_dirty(db_root, object, dirty_names,
                                      n_dirty, out_rebuilt, out_skipped);
   }
   ```

   Declare it in `src/db/query_internal.h` beside `RebuildFinalizeOps`
   (anchor: `} RebuildFinalizeOps;`).

2. `src/db/embedded.c` — new `shard_db_startup_migrate(db_root,
   disk_version)` (same file so the static `startup_schema_line` /
   `StartupSchemaEntry` schema.conf reader is reusable). Per object:

   - Load the typed schema (mirror `rebuild_object`'s argument
     convention, `src/db/query_find.c:1206`: composite
     `<dir>/<object>`). No `FT_DATETIME` field → skip.
   - **Rerun-skip marker check**: if `<obj_dir>/fields.conf` contains a
     line starting `#datetime_7byte` → skip (the previous start
     committed this object but crashed before stamping the root).
   - `fields.conf` needs no separate staging: v2's own stage block
     (`query_find.c:1095-1126`) copies the original content verbatim
     when `n_added == 0` and `drop_tombstoned == 0`, comment lines
     included. The migration only appends the marker (below).
   - Build `old_ts`: copy of the loaded schema with every `FT_DATETIME`
     field forced to `size = 6`, offsets and `total_size` recomputed
     cumulatively. Build `new_ts` exactly like `rebuild_object` does
     (`src/db/query_find.c:1236-1267`), sizes as parsed (7). Identity
     `new_to_old`; `old_sch`/`new_sch` geometry per the `rebuild_object`
     recipe (`slot_size = (24 + max_key + max_value + 7) & ~7`, floor
     32). `dirty_names` = the datetime field names.
   - Finalize callbacks — the same inside-the-txn-window hooks
     `edit-field` uses (`RebuildFinalizeOps`, `query_internal.h:120`),
     so the marker and the rebuilt indexes publish atomically with the
     widened data at `rebuild_txn_commit`:

     ```c
     typedef struct {
         const char *db_root;          /* composite-object convention */
         const char *object;
         char        obj_dir[PATH_MAX];
         const char (*dirty_names)[256];
         int         n_dirty;
     } MigrationFinalizeCtx;

     static int migration_apply_metadata(void *ctx_) {
         MigrationFinalizeCtx *c = ctx_;
         char fpath[PATH_MAX];
         snprintf(fpath, sizeof(fpath), "%s/fields.conf", c->obj_dir);
         /* Append the rerun-skip marker. The append is protected by the
            same rollback copy as the rest of fields.conf (snapshotted
            at rebuild_txn_begin, objlock.c:476): abort restores the
            snapshot — marker erased, old data back — and only a
            successful commit publishes it. */
         FILE *f = fopen(fpath, "a");
         if (!f) return -1;
         if (fputs("#datetime_7byte\n", f) < 0 || fflush(f) != 0 ||
             fsync(fileno(f)) != 0 || fclose(f) != 0) return -1;
         return 0;
     }

     static int migration_rebuild_indexes(void *ctx_, int *out_rebuilt) {
         MigrationFinalizeCtx *c = ctx_;
         int skipped = 0;
         return migration_reindex_dirty(c->db_root, c->object,
                                        c->dirty_names, c->n_dirty,
                                        out_rebuilt, &skipped);
     }

     /* Explicit on every field: indexes_may_change selects the
        abort-time full reindex (objlock.c:550) — it does NOT trigger a
        commit-time reindex (commit is a single rename). */
     RebuildFinalizeOps finalize = {
         .apply_metadata     = migration_apply_metadata,
         .rebuild_indexes    = migration_rebuild_indexes,
         .ctx                = &finalize_ctx,
         .indexes_may_change = 1,
     };
     ```
   - `slot_changed = 1`; call `rebuild_object_v2(db_root, object,
     &old_sch, old_ts, &new_sch, &new_ts, new_to_old, slot_changed,
     0 /*splits unchanged*/, 0, NULL, 0, &finalize)`. All index
     rebuilding rides the finalize callback **before** commit
     (`query_find.c:1150-1156`); if anything fails,
     `rebuild_txn_abort` restores fields.conf from the begin-time
     rollback copy — erasing the marker — restores the old data dir,
     and, because `indexes_may_change = 1`, runs a full
     `reindex_object_checked` so indexes match the restored old data
     (`objlock.c:550`; commit itself never reindexes). Count
     bookkeeping (`reset_deleted_count`/`set_count`) is v2's own
     success path (`query_find.c:1162-1164`). `OUT()` diagnostics go to
     stdout at startup (`types.h:447`) — acceptable.
   - Any failure → free the ctx and return −1 (caller refuses startup;
     `.version` stays old; next start retries — marked objects skip).

3. Wire both call sites. In `src/db/embedded.c` after the
   `shard_db_validate_before_stamp` block and before the stamp:

   ```c
       if (version_decision == SHARD_DB_VERSION_MIGRATE &&
           shard_db_startup_migrate(db_root, disk_version) != 0) {
           fprintf(stderr,
                   "shard_db_open: refusing to open: startup migration from "
                   "%s to %s failed; root unchanged, safe to retry\n",
                   disk_version, SHARD_DB_VERSION);
           g_shard_db_instance = NULL;
           g_db = NULL;
           db_cleanup_before_pools(db);
           db_root_lock_release(&lock_fd);
           atomic_store(&g_instance_open, 0);
           return NULL;
       }
   ```

   and change `int stamp_required = version_decision ==
   SHARD_DB_VERSION_STAMP;` to `... || version_decision ==
   SHARD_DB_VERSION_MIGRATE;`. Mirror both edits in `src/db/server.c`
   (`cmd_server`, anchors: the identical `stamp_required` line and the
   stamp block after `shard_db_validate_before_stamp`), where failure
   releases the lock and returns 1. Declare `shard_db_startup_migrate` in
   `src/db/types.h` beside the version-check decls (anchor at
   `types.h:1388`, "Read-only compatibility check").

**Green step:** tasks 4 and 6 pass.

### 8. Docs

- `docs/concepts/typed-records.md:51` — datetime row → `7` bytes:
  "`yyyyMMdd` (int32 BE) + seconds-of-day (3-byte BE, `0..86399`). Wire
  format `yyyyMMddHHmmss`. Widened from 6 bytes in 2026.09.2; startup
  migration rewrites old objects."
- `AGENTS.md` — typed-record table row; rewrite the 2026.09.1 gating
  paragraph to describe the migrate band (`required ≤ disk < current` →
  startup migration, then stamp).
- `docs/reference/changelog.md` + `docs/release-notes/2026.09.2.md` — the
  format change, the migration, the `2026.08.2` STAMP→MIGRATE behavior
  change, and the operator-facing upgrade window: 2026.08.2 / 2026.09.1
  roots migrate automatically on first 2026.09.2 start; from the following
  release `SHARD_DB_REQUIRED_SOURCE_VERSION` rises to `"2026.09.2"` and
  pre-migration roots are refused — upgrade through 2026.09.2 first.
- Sweep: `rg -n "6 bytes|uint16" docs/ | rg -i datetime` and fix strays.

### 9. Verification (all mandatory — no conditional gates)

1. Paste base-branch red output and post-fix green output for tasks 1, 2,
   4, and 6.
2. `SKIP_TESTS=1 ./build.sh`; fresh `./build/bin/shard-db-test run-all`.
3. The diff touches locks, shared/cached state, object lifetimes, and
   startup/background behavior — so both dynamic-safety gates run, three
   consecutive clean times each:
   - `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, then 3×
     `./build/bin/shard-db-test run-all`
   - `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`, then 3×
     `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
     ./build/bin/shard-db-test run-all --jobs 2`
4. Leave the work **uncommitted** for the raw-diff review, per the
   standing exception.

## Out of scope

- `datetimems` (uint32 ms already covers the full day), `date`, `time`,
  `timestamp`.
- Restoring already-truncated historic values (impossible — the original
  seconds are gone).
- Datetime validation, timezone, or calendar semantics changes.
- `edit-field` datetime resize exposure to users (the transform supports
  it internally; no CLI/wire surface is added).
