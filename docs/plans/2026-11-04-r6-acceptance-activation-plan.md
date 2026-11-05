# R6 — activation, application acceptance, docs/clients, final gates (2026-11-04, execution-ready)

> **For the executing agent.** Read `AGENTS.md` and `CORE-PROCESS.md` first. This plan is carried out literally, task by task.
> Every code block below was compiled and run against the base commit in a scratch worktree before being written here
> (results are recorded next to each block). Nothing in this plan is implemented in the repo yet.
>
> **Execution rules (embedded).**
> 1. Base: `feat/r5b-packed-recompose` @ `5e8564b` (R5b, committed locally, **unpushed**). Create `feat/r6-acceptance`
>    **from it** (deliberate deviation from "branch off the default branch": R6 cannot exist without R5/R5b, and neither is in
>    `main`). Leave all work **uncommitted** (AGENTS.md standing exception); do not commit, push, merge or open a PR.
> 2. Do tasks in order. Paste real command output into `docs/plans/2026-11-04-r6-evidence.md`; never claim a step passed without it.
> 3. If a quoted anchor is not found **exactly once**, write the mismatch to `PLAN_NOTES-r6.md` (a git-excluded `PLAN_NOTES.md` already
>    exists — do not overwrite it) and **halt the whole run immediately**. If you hit a decision the plan does not cover, stop and ask.
> 4. Never weaken a test. A test that fails then passes on rerun is a bug until root-caused.
> 5. Build `SKIP_TESTS=1 ./build.sh`. Single case `./build/bin/shard-db-test run <name>`. Normal full suite **only**
>    `./build/bin/shard-db-test run-all --jobs 4`. `/tmp` is tmpfs: after **every** run
>    `find /tmp -maxdepth 1 -name 'shard-db-*' -exec rm -rf {} +` (plain `rm -rf /tmp/shard-db-*` aborts under zsh on an empty glob).
>    Never `pkill -f`; use `pgrep -a shard-db` then `kill <pid>`. **No benches.** Note `ls` is aliased to long format in this shell —
>    use `find`/`command ls` in scripts.
> 6. **Sanitizers run exactly once (Task 8), after all fixes** — never between rounds.
> 7. Do not touch `docs/plans/2026-10-19-durability-certification-rig.md` (Task 9 verifies its SHA-256).

---

## 0. Scope, state and decisions

R6 = "explicit activation, finance application acceptance, documentation/clients, final blind review and branch-final safety
gates" (recovery plan, *Execution consolidation*). Packed is already the default (R5 flip) and `SHARD_DB_VERSION` is already
`2026.11.1`; **activation therefore needs no code switch** — R6 proves it with tests and documents it.

### 0.1 Decisions (defaults apply unless the human overrides before execution)

| # | Question | Default |
|---|---|---|
| D-R6-1 | PR shape for the unpushed stack (`fix/type-correctness-recovery` → R5b → R6, ~256 files) | One PR from one integration branch; Task 9 only reports readiness. |
| D-R6-2 | npm client: fix native async-handle lifetime concern (R1 note)? | **No.** Record it only. (R6 *does* fix `npm/binding.gyp`, a build-list omission, not a lifecycle change — see F-2.) |
| D-R6-3 | Version | Keep `2026.11.1` (unreleased). |
| D-R6-4 | Defects found during execution | Fix if simple (CORE-PROCESS "never paper over"); otherwise stop and ask. |
| D-R6-5 | Bitmap staleness after keyfile growth (F-1 below): fix in R6, or separate plan? | **Separate plan.** R6 ships the passing no-growth equivalence test and links the write-up. |

### 0.2 Findings made while verifying this plan (all reproduced)

| # | Finding | Disposition |
|---|---|---|
| F-1 | Bitmap indexes silently under-count (762 of 1714 rows) after kf shards grow during inserts; btree indexes fine; `reindex` repairs; **identical on legacy layout**, so pre-existing. Root cause: kf resplit renumbers slots, bitmap bits are slot numbers and are not remapped. Full write-up + deterministic repro: `docs/plans/2026-11-04-bitmap-stale-after-resplit.md` (already in the tree). | D-R6-5. Task 2's test deliberately avoids the growth seam. |
| F-2 | `npm/binding.gyp` omits 11 engine sources added since the last npm release (`agg_exact decimal_order decnum packed_csv_command packed_record scalar_grammar schema_wire typed_bulk_insert typed_index typed_read_command typed_row`). The addon links but dies on first use: `undefined symbol: typed_field_specs_parse`. The committed `npm/prebuilds/linux-x64` is also stale (basic.js fails 1 of 28 against it; passes 28/28 against a rebuilt addon). | Fixed in Task 4 (RED proven). |
| F-3 | Legacy `numeric:N` (implicit scale 2) rendered a whole value as `100`; after migration (`numeric:N,2`) it renders `100.00`. Numerically equal, textually different. | Documented in Task 5; pinned by the Task 3 test. |
| F-4 | `docs/concepts/typed-records.md` still documents `varchar` as `N+2` bytes / max 65535 and `numeric` as 8-byte int64 with "P informational"; `docs/reference/limits.md` says varchar max 65 535; `AGENTS.md` says segment records are "padded to `slot_size`". All stale for packed objects. | Task 5. |
| F-5 | R2 blind-review P1s (mixed-bitmap count bypassing verification; parallel find capping candidates before verification): the Task 2 equivalence test (bitmap AND/OR btree-decimal, count vs find paged at 1000, oracle computed in C) passes on this stack, including after bulk-delete. | Closed by evidence; record in Task 0.4. |

### 0.3 Edge cases / invariants

- Decimal comparisons are **textual**; no assertion routes a decimal through `double`.
- Request helpers interpolate only fixture-owned digits/letters; do not reuse them for arbitrary strings.
- Reopen tests use `test_env_stop_keep` + `test_env_start_at`; plain `test_env_stop` deletes the data.
- All new tests are `--jobs 4`-safe: own daemon/port/tmpdir, no env-var/global mutation, bounded /tmp use (acceptance case peaks at
  ~4 MiB varchar ×2 + 150 KiB decimal; record `du -sh` of its tmp dir in evidence if > 100 MB).
- R6 changes **no** on-disk or wire format; the only production-adjacent change is `npm/binding.gyp` (a build list).
- Consumers/call sites: no signature changes → none to inventory.

---

## Task 0 — Preflight (no code)

0. Add `PLAN_NOTES-r6.md` to the local exclude: `echo '/PLAN_NOTES-r6.md' >> .git/info/exclude`.
1. `git status --short` shows nothing except `?? docs/plans/2026-11-04-*.md` (the plan files; never stage them). Git-excluded files (`PLAN_NOTES.md`, `PLAN_NOTES-r6.md`, `R2-UNSTAGED-DIFF.patch`) and ignored `*.o` may exist;
   `git log --oneline -1` starts with `5e8564b`. Then `git checkout -b feat/r6-acceptance`.
2. `SKIP_TESTS=1 ./build.sh` (zero warnings: `... 2>&1 | grep -i warning` empty) and `./build/bin/shard-db-test run-all --jobs 4`.
   Expected baseline: 2403488 passed, 0 failed, 598 cases (R5b end state; match the numbers, not the exact wording). Paste the totals line. Anything else is a base
   problem — **stop and report**. Clear `/tmp/shard-db-*`.
3. Record `./build/bin/shard-db-test list | wc -l` (598).
4. Work-state items "R2 blind review" are closed by Task 2's `test-predicate-equivalence` (see F-5); no separate action.

## Task 1 — Application acceptance fixture

**Goal:** one daemon-backed case covering the finance shapes end to end on the default (packed) layout, including a clean restart.

1. Register the case in `build.sh`. Anchor (exactly once):
   `    src/test/cases/test_scalar_conversion_safety.c \`
   Insert immediately after it, preserving the backslash:
```
    src/test/cases/test_type_correctness_acceptance.c \
```
2. **RED first.** Before creating the files, run `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-type-correctness-acceptance`
   — it must fail with an unknown-case/missing-source build error (paste it; this proves the registration is what makes the case run).
3. Create `src/test/cases/test_type_correctness_acceptance.c` with exactly this content (139 assertions; verified green on the base):

```c
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

static char *type_accept_request(TestClient *tc, const char *request) {
    char *response = NULL;
    int rc = tc_request(tc, request, &response);
    ASSERT_EQ_INT(rc, 0, "acceptance transport succeeds");
    ASSERT_NOT_NULL(response, "acceptance response exists");
    return response;
}

static int type_accept_number(const char *response, const char *field,
                              const char *expected) {
    char key[80];
    int n = snprintf(key, sizeof(key), "\"%s\":", field);
    if (!response || n < 0 || (size_t)n >= sizeof(key)) return 0;
    const char *p = strstr(response, key);
    if (!p) return 0;
    p += (size_t)n;
    while (isspace((unsigned char)*p)) p++;
    size_t len = strcspn(p, ",}] \t\r\n");
    return len == strlen(expected) && memcmp(p, expected, len) == 0;
}

static int type_accept_create(TestClient *tc, const char *request) {
    char *response = type_accept_request(tc, request);
    int created = response && strstr(response, "\"error\"") == NULL &&
                  strstr(response, "created") != NULL;
    ASSERT_TRUE(created, "acceptance object created");
    free(response);
    return created;
}

static void type_accept_insert_literal(TestClient *tc, const char *object,
                                       const char *key, const char *field,
                                       const char *literal, int reject) {
    size_t cap = strlen(literal) + 512;
    char *request = malloc(cap);
    ASSERT_NOT_NULL(request, "acceptance request allocation");
    if (!request) return;
    /* Inputs to this helper are fixture-owned digits/ASCII letters only. */
    int n = snprintf(request, cap,
        "{\"mode\":\"insert\",\"dir\":\"type_accept\","
        "\"object\":\"%s\",\"key\":\"%s\","
        "\"value\":{\"%s\":\"%s\"}}", object, key, field, literal);
    ASSERT_TRUE(n >= 0 && (size_t)n < cap, "acceptance request fits");
    if (n >= 0 && (size_t)n < cap) {
        char *response = type_accept_request(tc, request);
        ASSERT_TRUE(response &&
            ((strstr(response, "\"error\"") != NULL) == reject),
            "acceptance insert validation result");
        free(response);
    }
    free(request);
}


static char *type_accept_get(TestClient *tc, const char *object, const char *key) {
    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"type_accept\",\"object\":\"%s\",\"key\":\"%s\"}",
        object, key);
    return type_accept_request(tc, req);
}

static void type_accept_expect_layout(TestClient *tc, const char *object,
                                      const char *layout) {
    char req[256], want[64];
    snprintf(req, sizeof(req),
        "{\"mode\":\"describe-object\",\"dir\":\"type_accept\",\"object\":\"%s\"}",
        object);
    snprintf(want, sizeof(want), "\"declaration_layout\":\"%s\"", layout);
    char *response = type_accept_request(tc, req);
    ASSERT_CONTAINS(response, want, "object has the expected declaration layout");
    free(response);
}

static void type_accept_expect_count(TestClient *tc, const char *object,
                                     int expected, const char *desc) {
    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"type_accept\",\"object\":\"%s\"}", object);
    char *response = type_accept_request(tc, req);
    ASSERT_EQ_INT(tu_parse_count(response), expected, desc);
    free(response);
}

/* Everything that must survive a clean stop/start of the same root. */
#define TA_WHOLE 131072
#define TA_FRACTION 16383
#define TA_VCEIL 4194304

static void type_accept_verify_persisted(TestClient *tc, const char *phase) {
    char desc[96];
    char *response = type_accept_get(tc, "fixed", "exact");
    snprintf(desc, sizeof(desc), "%s: constrained decimal exact", phase);
    ASSERT_TRUE(type_accept_number(response, "n", "1234567890123456.1234567890"), desc);
    free(response);

    response = type_accept_get(tc, "bare", "small");
    snprintf(desc, sizeof(desc), "%s: bare numeric scale kept", phase);
    ASSERT_TRUE(type_accept_number(response, "n", "1.2300"), desc);
    free(response);

    size_t length = (size_t)TA_WHOLE + 1 + TA_FRACTION;
    char *expected = malloc(length + 1);
    ASSERT_NOT_NULL(expected, "maximum decimal allocation");
    if (expected) {
        memset(expected, '9', TA_WHOLE);
        expected[TA_WHOLE] = '.';
        memset(expected + TA_WHOLE + 1, '1', TA_FRACTION);
        expected[length] = '\0';
        response = type_accept_get(tc, "bare", "maximum");
        snprintf(desc, sizeof(desc), "%s: maximum finite decimal exact", phase);
        ASSERT_TRUE(type_accept_number(response, "n", expected), desc);
        free(response);
        free(expected);
    }

    char *want = malloc(TA_VCEIL + 1);
    ASSERT_NOT_NULL(want, "varchar ceiling allocation");
    if (want) {
        memset(want, 'a', TA_VCEIL);
        want[TA_VCEIL] = '\0';
        response = type_accept_get(tc, "strings", "at_max");
        const char *pv = response ? strstr(response, "\"v\":\"") : NULL;
        snprintf(desc, sizeof(desc), "%s: 4 MiB varchar exact", phase);
        ASSERT_TRUE(pv && memcmp(pv + 5, want, TA_VCEIL) == 0 &&
                    pv[5 + TA_VCEIL] == '"', desc);
        free(response);
        free(want);
    }
}

static int test_type_correctness_acceptance_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "acceptance connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *response = type_accept_request(tc,
        "{\"mode\":\"add-dir\",\"dir\":\"type_accept\"}");
    free(response);

    /* Negative control: the harness can tell legacy from packed, so the
       "packed" assertions below cannot pass vacuously. TEST_BUILD only. */
    int control = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"control\",\"splits\":8,\"max_key\":16,"
        "\"declaration_layout\":\"legacy\",\"fields\":[\"i:int\"]}");
    if (control) type_accept_expect_layout(tc, "control", "legacy");

    int fixed = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"fixed\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"n:numeric:26,10\",\"f:double\",\"i:int\"],"
        "\"indexes\":[\"n\"]}");
    if (fixed) {
        type_accept_expect_layout(tc, "fixed", "packed");
        type_accept_insert_literal(tc, "fixed", "bad", "i", "2147483648", 1);
        type_accept_expect_count(tc, "fixed", 0, "rejected integer creates no record");
        response = type_accept_request(tc,
            "{\"mode\":\"insert\",\"dir\":\"type_accept\","
            "\"object\":\"fixed\",\"key\":\"exact\",\"value\":{"
            "\"n\":\"1234567890123456.1234567890\","
            "\"f\":\"1.2345678901234567\"}}");
        ASSERT_TRUE(response && !strstr(response, "\"error\""),
                    "exact constrained decimal insert succeeds");
        free(response);
        /* adjacent last-place neighbour and a negative counterpart */
        response = type_accept_request(tc,
            "{\"mode\":\"insert\",\"dir\":\"type_accept\","
            "\"object\":\"fixed\",\"key\":\"next\",\"value\":{"
            "\"n\":\"1234567890123456.1234567891\"}}");
        ASSERT_TRUE(response && !strstr(response, "\"error\""), "adjacent decimal insert");
        free(response);
        response = type_accept_request(tc,
            "{\"mode\":\"insert\",\"dir\":\"type_accept\","
            "\"object\":\"fixed\",\"key\":\"neg\",\"value\":{"
            "\"n\":\"-1234567890123456.1234567890\"}}");
        ASSERT_TRUE(response && !strstr(response, "\"error\""), "negative decimal insert");
        free(response);

        response = type_accept_get(tc, "fixed", "exact");
        ASSERT_TRUE(type_accept_number(response, "n", "1234567890123456.1234567890"),
                    "constrained decimal retains value and scale");
        ASSERT_TRUE(type_accept_number(response, "f", "1.2345678901234567"),
                    "double output retains stored value");
        free(response);

        /* indexed equality must separate last-place neighbours exactly */
        response = type_accept_request(tc,
            "{\"mode\":\"count\",\"dir\":\"type_accept\",\"object\":\"fixed\","
            "\"criteria\":[{\"field\":\"n\",\"op\":\"eq\","
            "\"value\":\"1234567890123456.1234567890\"}]}");
        ASSERT_EQ_INT(tu_parse_count(response), 1, "indexed eq matches exactly one decimal");
        free(response);
        response = type_accept_request(tc,
            "{\"mode\":\"count\",\"dir\":\"type_accept\",\"object\":\"fixed\","
            "\"criteria\":[{\"field\":\"n\",\"op\":\"gt\","
            "\"value\":\"1234567890123456.1234567890\"}]}");
        ASSERT_EQ_INT(tu_parse_count(response), 1, "indexed gt sees only the adjacent decimal");
        free(response);
        response = type_accept_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"type_accept\",\"object\":\"fixed\","
            "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"n\",\"alias\":\"s\"}],"
            "\"criteria\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"0\"}]}");
        ASSERT_CONTAINS(response, "2469135780246912.2469135781",
                        "exact decimal sum of the two positive rows");
        free(response);
    }

    int bare = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"bare\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"n:numeric\",\"after:varchar:8\"]}");
    if (bare) {
        type_accept_expect_layout(tc, "bare", "packed");
        type_accept_insert_literal(tc, "bare", "small", "n", "1.2300", 0);
        response = type_accept_request(tc,
            "{\"mode\":\"update\",\"dir\":\"type_accept\","
            "\"object\":\"bare\",\"key\":\"small\","
            "\"value\":{\"after\":\"tail\"}}");
        ASSERT_TRUE(response && !strstr(response, "\"error\""),
                    "update following field succeeds");
        free(response);
        response = type_accept_get(tc, "bare", "small");
        ASSERT_TRUE(type_accept_number(response, "n", "1.2300"),
                    "bare numeric scale survives following-field update");
        ASSERT_CONTAINS(response, "\"after\":\"tail\"", "following field survives");
        free(response);

        size_t length = (size_t)TA_WHOLE + 1 + TA_FRACTION;
        char *value = malloc(length + 1);
        ASSERT_NOT_NULL(value, "maximum decimal allocation");
        if (value) {
            memset(value, '9', TA_WHOLE);
            value[TA_WHOLE] = '.';
            memset(value + TA_WHOLE + 1, '1', TA_FRACTION);
            value[length] = '\0';
            type_accept_insert_literal(tc, "bare", "maximum", "n", value, 0);
            response = type_accept_get(tc, "bare", "maximum");
            ASSERT_TRUE(type_accept_number(response, "n", value),
                        "full PostgreSQL finite digit range round trips");
            free(response);
            /* one more integer digit must be rejected and leave no record */
            char *over = malloc(length + 2);
            ASSERT_NOT_NULL(over, "over-limit decimal allocation");
            if (over) {
                over[0] = '9';
                memcpy(over + 1, value, length + 1);
                type_accept_insert_literal(tc, "bare", "over_whole", "n", over, 1);
                free(over);
            }
            free(value);
        }
        type_accept_expect_count(tc, "bare", 2, "rejected decimals create no records");
    }

    int strings = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"strings\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"v:varchar\",\"after:int\"]}");
    if (strings) {
        type_accept_expect_layout(tc, "strings", "packed");
        type_accept_insert_literal(tc, "strings", "small", "v", "abc", 0);
        char *value = malloc((size_t)TA_VCEIL + 2);
        ASSERT_NOT_NULL(value, "varchar boundary allocation");
        if (value) {
            memset(value, 'a', TA_VCEIL); value[TA_VCEIL] = '\0';
            type_accept_insert_literal(tc, "strings", "at_max", "v", value, 0);
            value[TA_VCEIL] = 'a'; value[TA_VCEIL + 1] = '\0';
            type_accept_insert_literal(tc, "strings", "over_max", "v", value, 1);
            free(value);
        }
        type_accept_expect_count(tc, "strings", 2, "ceiling+1 varchar creates no record");
    }

    type_accept_verify_persisted(tc, "live");

    /* clean stop and restart of the same root: nothing may change */
    tc_close(tc);
    char root[sizeof(env.db_root)];
    snprintf(root, sizeof(root), "%s", env.db_root);
    int port = env.port;
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(test_env_start_at(&env, root, port), 0, "restart on the same root");
    tc = tc_connect(&(TestClientCfg){ .port = port, .io_timeout_ms = 60000 });
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (tc) {
        type_accept_expect_layout(tc, "fixed", "packed");
        type_accept_verify_persisted(tc, "reopened");
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-type-correctness-acceptance",
              test_type_correctness_acceptance_run)
```

4. Run `./build/bin/shard-db-test run test-type-correctness-acceptance` → expect `139 passed, 0 failed`. Record wall time
   (`time`) and peak RSS (`/usr/bin/time -v`) in evidence.
5. **Mutation proofs (required; both were run during planning and failed as stated).** Apply each, rebuild, run, paste the failure, revert, rebuild, re-run green:
   - (a) `sed -i 's/#define TA_VCEIL 4194304/#define TA_VCEIL 4194305/' src/test/cases/test_type_correctness_acceptance.c`
     → expect 4 failures: `acceptance insert validation result`, `ceiling+1 varchar creates no record`, `live: 4 MiB varchar exact`, `reopened: 4 MiB varchar exact`.
   - (b) change `type_accept_expect_layout(tc, "control", "legacy")` to `... "packed")` → expect exactly 1 failure: `object has the expected declaration layout`.
   (The permanent `control` legacy object means the "packed" assertions cannot pass vacuously.)

## Task 2 — Route matrix: finance routes + predicate equivalence

Parent plan Phase 9 requires the full route matrix. Coverage was mapped against existing cases; the gaps are filled by two new cases.

**Matrix → existing case (verified by `grep`; paste the same greps into evidence):**

| Matrix row | Covered by |
|---|---|
| Decimal core / rounding / limits | `test_decnum_core.c`, `test_packed_record_codec.c` (both reference 73744 / 4194304) |
| Packed storage, physical length | `test_packed_record_storage.c`, `test_packed_record_codec.c` |
| Writes: single/bulk/computed/guards | `test_single_write_integration`, `test_typed_bulk_insert`, `test_typed_bulk_update`, `test_computed_update`, `test_r1_routes` |
| Reads: projection/CSV | `test_packed_json_projection`, `test_packed_named_projection`, `test_packed_csv`, `test_packed_csv_commands` |
| Aggregates | `test_agg_exact_core`, `test_agg_packed_routes`, `test_aggregate_type_matrix` |
| Scalars (date/bool/uuid/ip/enum) | `test_scalar_grammar_core`, `test_scalar_grammar_routes` |
| Schema mutations | `test_packed_recompose`, `test_legacy_recompose`, `test_edit_field*`, `test_not_null_*` |
| Migration / lifecycle | `test_layout_manifest` (migration, refusals, crash points, backup/restore), `test_null_migration`, `test_version_gate*` |
| **Bulk → exact read-back, indexed cursor order ASC/DESC, delete + compact, backup/restore, restart (finance shapes)** | **NEW `test_finance_routes.c`** |
| **Mixed bitmap + btree-decimal criteria: count vs find vs oracle, after delete** | **NEW `test_predicate_equivalence.c`** |

0. **Do not rebuild until both files exist** (the registration below references both). Register both cases in `build.sh`: immediately after the line `    src/test/cases/test_type_correctness_acceptance.c \` (added in Task 1) insert
```
    src/test/cases/test_finance_routes.c \
    src/test/cases/test_predicate_equivalence.c \
```
   (Order of work: steps 0, 1, 3 — create both files — then 2, 4. The first build happens only after step 3.)
1. Create `src/test/cases/test_finance_routes.c` (2 462 assertions; verified green):

```c
/* R6 route matrix for the finance shapes on packed storage: bulk insert,
   exact per-row read-back (including bare-numeric scale), indexed cursor
   order in both directions, bulk delete + vacuum compact, exact aggregate,
   backup/restore and a clean restart. Textual comparison throughout. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FR_N 600

typedef struct {
    long long scaled;     /* amt * 10^10 */
    char amt[48];
    char bare[32];
    char cur[32];
} FrRow;

static FrRow fr_rows[FR_N];

static void fr_build(void) {
    for (int i = 0; i < FR_N; i++) {
        FrRow *r = &fr_rows[i];
        int n = i + 1;
        long long ip = (n % 7 == 0) ? -(long long)n : n;
        long long frac = ((long long)n * 7919LL) % 10000000000LL;
        snprintf(r->amt, sizeof(r->amt), "%s%lld.%010lld", ip < 0 ? "-" : "",
                 ip < 0 ? -ip : ip, frac);
        r->scaled = (ip < 0 ? -1 : 1) * ((ip < 0 ? -ip : ip) * 10000000000LL + frac);
        int scale = n % 5;
        if (scale == 0) snprintf(r->bare, sizeof(r->bare), "%d", n);
        else {
            char f[8];
            memset(f, '5', (size_t)scale); f[scale] = '\0';
            snprintf(r->bare, sizeof(r->bare), "%d.%s", n, f);
        }
        snprintf(r->cur, sizeof(r->cur), "%d.%04d", n, (n * 37) % 10000);
    }
}

static char *fr_req(TestClient *tc, const char *json) {
    char *resp = NULL;
    int rc = tc_request(tc, json, &resp);
    ASSERT_EQ_INT(rc, 0, "finance transport");
    return resp;
}

static int fr_has(const char *resp, const char *key, const char *expected) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = resp ? strstr(resp, pat) : NULL;
    if (!p) return 0;
    p += strlen(pat);
    size_t len = strcspn(p, ",}] \t\r\n");
    return len == strlen(expected) && memcmp(p, expected, len) == 0;
}

static int fr_cmp_scaled(const void *a, const void *b) {
    const FrRow *x = *(const FrRow *const *)a, *y = *(const FrRow *const *)b;
    return (x->scaled > y->scaled) - (x->scaled < y->scaled);
}

static void fr_verify_all_rows(TestClient *tc, const char *phase, int skip_deleted) {
    int bad = 0, seen = 0;
    for (int i = 0; i < FR_N; i++) {
        int deleted = skip_deleted && ((i + 1) % 3 == 0);
        char req[160];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"fin\",\"object\":\"ledger\",\"key\":\"r%04d\"}", i + 1);
        char *resp = fr_req(tc, req);
        if (deleted) {
            if (resp && !strstr(resp, "\"error\"") && strstr(resp, "\"amt\"")) bad++;
        } else {
            seen++;
            if (!resp || !fr_has(resp, "amt", fr_rows[i].amt) ||
                !fr_has(resp, "bare", fr_rows[i].bare) ||
                !fr_has(resp, "cur", fr_rows[i].cur) || !strstr(resp, "note-")) bad++;
        }
        free(resp);
    }
    char desc[96];
    snprintf(desc, sizeof(desc), "%s: every row exact as text (%d checked)", phase, seen);
    ASSERT_EQ_INT(bad, 0, desc);
}

static void fr_expected_sum(int skip_deleted, char *out, size_t cap) {
    __int128 total = 0;
    for (int i = 0; i < FR_N; i++)
        if (!(skip_deleted && ((i + 1) % 3 == 0))) total += fr_rows[i].scaled;
    int neg = total < 0;
    if (neg) total = -total;
    long long whole = (long long)(total / 10000000000LL);
    long long frac = (long long)(total % 10000000000LL);
    snprintf(out, cap, "%s%lld.%010lld", neg ? "-" : "", whole, frac);
}

static void fr_verify_aggregate(TestClient *tc, const char *phase, int skip_deleted, int expect_count) {
    char want[64], desc[96];
    fr_expected_sum(skip_deleted, want, sizeof(want));
    char *resp = fr_req(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"fin\",\"object\":\"ledger\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"amt\",\"alias\":\"s\"}]}");
    snprintf(desc, sizeof(desc), "%s: exact decimal sum %s", phase, want);
    ASSERT_CONTAINS(resp, want, desc);
    free(resp);
    resp = fr_req(tc, "{\"mode\":\"count\",\"dir\":\"fin\",\"object\":\"ledger\"}");
    snprintf(desc, sizeof(desc), "%s: record count", phase);
    ASSERT_EQ_INT(tu_parse_count(resp), expect_count, desc);
    free(resp);
}

/* Walks the keyset cursor to the end and returns the keys in delivery order. */
static int fr_walk_cursor(TestClient *tc, const char *order, char keys[][8], int cap) {
    int n = 0;
    char cursor[256] = "null";
    for (int page = 0; page < 20; page++) {
        char req[640];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"fin\",\"object\":\"ledger\","
            "\"criteria\":[{\"field\":\"amt\",\"op\":\"exists\"}],\"order_by\":\"amt\",\"order\":\"%s\",\"limit\":100,\"cursor\":%s}", order, cursor);
        char *resp = fr_req(tc, req);
        if (!resp || strstr(resp, "\"error\"")) { free(resp); return -1; }

        const char *c = strstr(resp, "\"cursor\":");
        const char *p = resp;
        /* rows come before the trailing cursor object, which has its own "key" */
        while ((p = strstr(p, "\"key\":\"r")) != NULL && (!c || p < c)) {
            p += 7;
            if (n < cap) { memcpy(keys[n], p, 5); keys[n][5] = '\0'; }
            n++;
        }
        if (!c) { free(resp); break; }
        c += 9;
        if (strncmp(c, "null", 4) == 0) { free(resp); break; }
        const char *e = strchr(c, '}');
        if (!e) { free(resp); return -1; }
        size_t len = (size_t)(e - c) + 1;
        if (len >= sizeof(cursor)) { free(resp); return -1; }
        memcpy(cursor, c, len); cursor[len] = '\0';
        free(resp);
    }
    return n;
}

static int test_finance_routes_run(void) {
    fr_build();
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *r = fr_req(tc, "{\"mode\":\"add-dir\",\"dir\":\"fin\"}");
    free(r);
    r = fr_req(tc,
        "{\"mode\":\"create-object\",\"dir\":\"fin\",\"object\":\"ledger\",\"splits\":8,"
        "\"max_key\":16,\"fields\":[\"amt:numeric:26,10\",\"bare:numeric\",\"cur:currency\","
        "\"note:varchar\"],\"indexes\":[\"amt\"]}");
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create ledger");
    free(r);

    size_t cap = (size_t)FR_N * 220 + 256;
    char *bulk = malloc(cap);
    ASSERT_NOT_NULL(bulk, "bulk buffer");
    if (!bulk) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = (size_t)snprintf(bulk, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"fin\",\"object\":\"ledger\",\"records\":[");
    for (int i = 0; i < FR_N; i++)
        len += (size_t)snprintf(bulk + len, cap - len,
            "%s{\"key\":\"r%04d\",\"value\":{\"amt\":\"%s\",\"bare\":\"%s\",\"cur\":\"%s\","
            "\"note\":\"note-%d\"}}", i ? "," : "", i + 1, fr_rows[i].amt,
            fr_rows[i].bare, fr_rows[i].cur, i + 1);
    snprintf(bulk + len, cap - len, "]}");
    r = fr_req(tc, bulk);
    free(bulk);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "bulk-insert accepted");
    free(r);

    fr_verify_all_rows(tc, "after bulk insert", 0);
    fr_verify_aggregate(tc, "after bulk insert", 0, FR_N);

    /* indexed cursor order must equal exact numeric order, ASC and DESC */
    const FrRow *sorted[FR_N];
    for (int i = 0; i < FR_N; i++) sorted[i] = &fr_rows[i];
    qsort(sorted, FR_N, sizeof(sorted[0]), fr_cmp_scaled);
    static char keys[FR_N + 8][8];
    int n = fr_walk_cursor(tc, "asc", keys, FR_N + 8);
    ASSERT_EQ_INT(n, FR_N, "ASC cursor delivers every row once");
    int ordered = n == FR_N;
    for (int i = 0; ordered && i < FR_N; i++) {
        char want[8];
        snprintf(want, sizeof(want), "r%04d", (int)(sorted[i] - fr_rows) + 1);
        if (strcmp(keys[i], want) != 0) ordered = 0;
    }
    ASSERT_TRUE(ordered, "ASC cursor order equals exact decimal order");
    n = fr_walk_cursor(tc, "desc", keys, FR_N + 8);
    ASSERT_EQ_INT(n, FR_N, "DESC cursor delivers every row once");
    ordered = n == FR_N;
    for (int i = 0; ordered && i < FR_N; i++) {
        char want[8];
        snprintf(want, sizeof(want), "r%04d", (int)(sorted[FR_N - 1 - i] - fr_rows) + 1);
        if (strcmp(keys[i], want) != 0) ordered = 0;
    }
    ASSERT_TRUE(ordered, "DESC cursor order equals exact decimal order");

    /* delete every third record, compact, re-verify through every route */
    char del[FR_N * 12 + 128];
    size_t dl = (size_t)snprintf(del, sizeof(del),
        "{\"mode\":\"bulk-delete\",\"dir\":\"fin\",\"object\":\"ledger\",\"keys\":[");
    int first = 1;
    for (int i = 2; i < FR_N; i += 3) {
        dl += (size_t)snprintf(del + dl, sizeof(del) - dl, "%s\"r%04d\"", first ? "" : ",", i + 1);
        first = 0;
    }
    snprintf(del + dl, sizeof(del) - dl, "]}");
    r = fr_req(tc, del);
    ASSERT_CONTAINS(r, "\"deleted\":200", "bulk-delete removed 200 records");
    free(r);
    r = fr_req(tc, "{\"mode\":\"vacuum\",\"dir\":\"fin\",\"object\":\"ledger\",\"compact\":true}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "vacuum compact succeeds");
    free(r);
    fr_verify_all_rows(tc, "after delete + compact", 1);
    fr_verify_aggregate(tc, "after delete + compact", 1, FR_N - 200);
    r = fr_req(tc, "{\"mode\":\"orphaned\",\"dir\":\"fin\",\"object\":\"ledger\"}");
    ASSERT_EQ_INT(tu_parse_count(r), 0, "compact leaves no orphaned records");
    free(r);
    {
        char req[400];
        snprintf(req, sizeof(req),
            "{\"mode\":\"count\",\"dir\":\"fin\",\"object\":\"ledger\","
            "\"criteria\":[{\"field\":\"amt\",\"op\":\"eq\",\"value\":\"%s\"}]}", fr_rows[0].amt);
        r = fr_req(tc, req);
        ASSERT_EQ_INT(tu_parse_count(r), 1, "indexed eq on a surviving exact decimal");
        free(r);
        snprintf(req, sizeof(req),
            "{\"mode\":\"count\",\"dir\":\"fin\",\"object\":\"ledger\","
            "\"criteria\":[{\"field\":\"amt\",\"op\":\"eq\",\"value\":\"%s\"}]}", fr_rows[2].amt);
        r = fr_req(tc, req);
        ASSERT_EQ_INT(tu_parse_count(r), 0, "indexed eq on a deleted decimal finds nothing");
        free(r);
    }

    /* backup, damage, restore */
    r = fr_req(tc, "{\"mode\":\"backup\",\"dir\":\"fin\",\"object\":\"ledger\"}");
    char stamp[64] = {0};
    const char *bp = r ? strstr(r, "/backup/") : NULL;
    if (bp) sscanf(bp + 8, "%63[^\"]", stamp);
    ASSERT_TRUE(stamp[0] != '\0', "backup id parsed");
    free(r);
    r = fr_req(tc, "{\"mode\":\"bulk-delete\",\"dir\":\"fin\",\"object\":\"ledger\","
                   "\"keys\":[\"r0001\",\"r0002\"]}");
    free(r);
    {
        char req[320];
        snprintf(req, sizeof(req),
            "{\"mode\":\"restore\",\"dir\":\"fin\",\"object\":\"ledger\",\"from\":\"%s\",\"force\":true}", stamp);
        r = fr_req(tc, req);
        ASSERT_CONTAINS(r, "\"status\":\"restored\"", "restore succeeds");
        free(r);
    }
    fr_verify_all_rows(tc, "after restore", 1);
    fr_verify_aggregate(tc, "after restore", 1, FR_N - 200);

    /* clean restart of the same root */
    tc_close(tc);
    char root[sizeof(env.db_root)];
    snprintf(root, sizeof(root), "%s", env.db_root);
    int port = env.port;
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(test_env_start_at(&env, root, port), 0, "restart on the same root");
    tc = tc_connect(&(TestClientCfg){ .port = port, .io_timeout_ms = 60000 });
    ASSERT_NOT_NULL(tc, "reconnect");
    if (tc) {
        fr_verify_all_rows(tc, "after restart", 1);
        fr_verify_aggregate(tc, "after restart", 1, FR_N - 200);
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-finance-routes", test_finance_routes_run)
```

2. Run → `test-finance-routes: 2462 passed, 0 failed`. **Mutation proof (run during planning, failed as stated):** change the comparator body
   `return (x->scaled > y->scaled) - (x->scaled < y->scaled);` to `return (x->scaled < y->scaled) - (x->scaled > y->scaled);` → expect exactly
   `ASC cursor order equals exact decimal order` and `DESC cursor order equals exact decimal order` to fail; revert and re-run green.
3. Create `src/test/cases/test_predicate_equivalence.c` (31 assertions; verified green). It intentionally does **not** use the kf-tier seam (see F-1):

```c
/* R6: count, find and a C-computed oracle must agree for mixed bitmap +
   btree-decimal criteria on a packed object (primary-leaf, intersect, keyset
   union and scan routes). Guards the R2 review items: candidate verification
   after a bitmap/btree primary, and candidate caps before verification. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PE_N 4000

typedef struct { long long amt; int flag; int kind; int qty; int live; } PeRow; /* amt scaled by 1e4 */
static PeRow pe_rows[PE_N];
static const char *const pe_kind[] = { "a", "b", "c" };

typedef int (*PeMatch)(const PeRow *);
static int pe_m_and(const PeRow *r)   { return r->flag && r->amt >= 1000000 && r->amt <= 2500000; }
static int pe_m_or(const PeRow *r)    { return r->flag || r->amt > 3500000; }
static int pe_m_neq(const PeRow *r)   { return r->flag && r->amt != 1234500; }
static int pe_m_kind(const PeRow *r)  { return r->kind == 1 && r->flag && r->qty < 5; }
static int pe_m_none(const PeRow *r)  { return r->flag && r->amt < -9000000; }
static int pe_m_orand(const PeRow *r) { return (r->flag || r->kind == 2) && r->amt >= 0 && r->amt < 500000; }

static int pe_oracle(PeMatch m) {
    int n = 0;
    for (int i = 0; i < PE_N; i++) n += (pe_rows[i].live && m(&pe_rows[i])) ? 1 : 0;
    return n;
}

static int pe_find_total(TestClient *tc, const char *criteria) {
    int total = 0;
    for (int off = 0;; off += 1000) {
        char req[1024];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"pe\",\"object\":\"t\",\"criteria\":%s,"
            "\"offset\":%d,\"limit\":1000,\"fields\":[\"qty\"]}", criteria, off);
        char *r = NULL;
        if (tc_request(tc, req, &r) != 0 || !r || strstr(r, "\"error\"")) { free(r); return -1; }
        int got = 0;
        for (const char *p = r; (p = strstr(p, "\"key\":\"")) != NULL; p += 7) got++;
        free(r);
        total += got;
        if (got < 1000) break;
    }
    return total;
}

static void pe_check(TestClient *tc, const char *name, const char *criteria, PeMatch m) {
    char req[1024], desc[160];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"pe\",\"object\":\"t\",\"criteria\":%s}", criteria);
    char *r = NULL;
    int rc = tc_request(tc, req, &r);
    int counted = rc == 0 ? tu_parse_count(r) : -1;
    free(r);
    int want = pe_oracle(m);
    snprintf(desc, sizeof(desc), "%s: count equals the oracle (%d)", name, want);
    ASSERT_EQ_INT(counted, want, desc);
    snprintf(desc, sizeof(desc), "%s: find returns exactly the oracle rows", name);
    ASSERT_EQ_INT(pe_find_total(tc, criteria), want, desc);
}

static void pe_run_all(TestClient *tc, const char *phase) {
    char tag[96];
#define PE_CASE(label, crit, fn) \
    do { snprintf(tag, sizeof(tag), "%s/%s", phase, label); pe_check(tc, tag, crit, fn); } while (0)
    PE_CASE("bitmap AND decimal between",
        "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"amt\",\"op\":\"between\",\"value\":[\"100.0000\",\"250.0000\"]}]", pe_m_and);
    PE_CASE("bitmap OR decimal gt",
        "[{\"or\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"amt\",\"op\":\"gt\",\"value\":\"350.0000\"}]}]", pe_m_or);
    PE_CASE("bitmap AND decimal neq",
        "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"amt\",\"op\":\"neq\",\"value\":\"123.4500\"}]", pe_m_neq);
    PE_CASE("enum bitmap AND bool bitmap AND int range",
        "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"},"
        "{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"qty\",\"op\":\"lt\",\"value\":\"5\"}]", pe_m_kind);
    PE_CASE("empty result",
        "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"-900.0000\"}]", pe_m_none);
    PE_CASE("OR sub-tree under AND",
        "[{\"or\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"c\"}]},"
        "{\"field\":\"amt\",\"op\":\"gte\",\"value\":\"0\"},"
        "{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"50.0000\"}]", pe_m_orand);
#undef PE_CASE
}

static int test_predicate_equivalence_run(void) {
    for (int i = 0; i < PE_N; i++) {
        int n = i + 1;
        pe_rows[i].amt = ((long long)(n * 7919) % 8000000) - 4000000; /* -400..400 at 4 dp */
        pe_rows[i].flag = (n % 3 == 0) || (n % 7 == 0);
        pe_rows[i].kind = n % 3;
        pe_rows[i].qty = n % 11;
        pe_rows[i].live = 1;
    }
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    /* NOTE: no kf-tier seam here -- see docs/plans/2026-11-04-bitmap-stale-after-resplit.md */
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *r = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"pe\"}", &r); free(r); r = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"pe\",\"object\":\"t\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"amt:numeric:18,4\",\"flag:bool\",\"kind:enum(a,b,c)\",\"qty:int\"],"
        "\"indexes\":[\"amt\",\"flag:bitmap\",\"kind:bitmap\",\"qty\"]}", &r);
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create object");
    free(r);
    for (int base = 0; base < PE_N; base += 1000) {
        size_t cap = 1000 * 160 + 256;
        char *bulk = malloc(cap);
        if (!bulk) return 1;
        size_t len = (size_t)snprintf(bulk, cap,
            "{\"mode\":\"bulk-insert\",\"dir\":\"pe\",\"object\":\"t\",\"records\":[");
        for (int j = 0; j < 1000; j++) {
            const PeRow *row = &pe_rows[base + j];
            long long a = row->amt < 0 ? -row->amt : row->amt;
            len += (size_t)snprintf(bulk + len, cap - len,
                "%s{\"key\":\"e%05d\",\"value\":{\"amt\":\"%s%lld.%04lld\",\"flag\":%s,"
                "\"kind\":\"%s\",\"qty\":%d}}", j ? "," : "", base + j + 1,
                row->amt < 0 ? "-" : "", a / 10000, a % 10000,
                row->flag ? "true" : "false", pe_kind[row->kind], row->qty);
        }
        snprintf(bulk + len, cap - len, "]}");
        r = NULL;
        tc_request(tc, bulk, &r);
        ASSERT_TRUE(r && !strstr(r, "\"error\""), "bulk-insert chunk");
        free(r);
        free(bulk);
    }
    pe_run_all(tc, "loaded");

    /* delete a slice; every route must still agree with the oracle */
    char del[4096];
    size_t dl = (size_t)snprintf(del, sizeof(del),
        "{\"mode\":\"bulk-delete\",\"dir\":\"pe\",\"object\":\"t\",\"keys\":[");
    for (int i = 0; i < 300; i++) {
        int idx = i * 7 + 1;                       /* deterministic spread */
        dl += (size_t)snprintf(del + dl, sizeof(del) - dl, "%s\"e%05d\"", i ? "," : "", idx);
        pe_rows[idx - 1].live = 0;
    }
    snprintf(del + dl, sizeof(del) - dl, "]}");
    r = NULL;
    tc_request(tc, del, &r);
    ASSERT_CONTAINS(r, "\"deleted\":300", "bulk-delete removed the slice");
    free(r);
    pe_run_all(tc, "after-delete");
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-predicate-equivalence", test_predicate_equivalence_run)
```

4. Run → `31 passed, 0 failed`. **Mutation proof:** change `r->amt <= 2500000` to `r->amt <= 2400000` in `pe_m_and` → expect failures in
   `bitmap AND decimal between` (count and find, both phases); revert.
5. Do not add the growth-seam variant: it fails today (F-1) and is owned by the follow-up plan. Link
   `docs/plans/2026-11-04-bitmap-stale-after-resplit.md` from the release note's "Known issues" (Task 5).

## Task 3 — Migration acceptance (finance-shaped root)

Existing R5 cases already assert, in `test_layout_manifest.c`: (a) populated legacy root migrates to `layout=packed` and re-stamps
(`test-layout-migration-converts-legacy`); (b) `numeric:N`→`numeric:N,2` rewrite and explicit P,S kept (`lm_assert_migrated`); (c) pending marker →
refusal, marker preserved (`test-layout-migration-refusals` b); (d) crash at each of 7 phases then restart finishes
(`test-layout-migration-crash-points`); (e) unclean root and unfit value refused (refusals a, c); (f) downgrade refused
(`test-version-gate-2026-11`). Paste this mapping into evidence. The single gap is a finance-shaped, multi-index, bitmap-bearing root
compared **textually** against a pre-migration snapshot. Add it to the same file (it reuses the file's static helpers).

1. **RED:** run `./build/bin/shard-db-test run test-layout-migration-finance-acceptance` → expect "unknown test" (paste).
2. Append this block to the **end** of `src/test/cases/test_layout_manifest.c` (anchor: the last line of the file is
   `TEST_REGISTER("test-layout-startup-agreement", test_layout_startup_agreement)`; the block starts on the line after it). Verified: 21 assertions pass.

```c

/* ---- R6: finance-shaped migration acceptance ---------------------------
   A populated legacy root (decimal, currency, scale-2 legacy numeric, date,
   bool, varchar, three btree indexes + a bitmap) is stopped cleanly, aged to
   2026.10.2, and migrated by the first start of this binary. Every value is
   compared as text against a pre-migration snapshot and every index route is
   compared against its pre-migration answer. */
#define LM_FIN_ROWS 3000
#define LM_FIN_CHUNK 1000

static int lm_cmp_lines(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/* Splits `text` in place into lines, sorts them and rejoins into a new string. */
static char *lm_sorted_lines(const char *text) {
    if (!text) return NULL;
    char *copy = strdup(text);
    size_t cap = 1024, n = 0;
    char **lines = malloc(cap * sizeof(*lines));
    if (!copy || !lines) { free(copy); free(lines); return NULL; }
    for (char *s = strtok(copy, "\n"); s; s = strtok(NULL, "\n")) {
        if (n == cap) {
            cap *= 2;
            char **grown = realloc(lines, cap * sizeof(*lines));
            if (!grown) { free(copy); free(lines); return NULL; }
            lines = grown;
        }
        lines[n++] = s;
    }
    qsort(lines, n, sizeof(*lines), lm_cmp_lines);
    char *out = malloc(strlen(text) + n + 2);
    if (out) {
        out[0] = '\0';
        for (size_t i = 0; i < n; i++) { strcat(out, lines[i]); strcat(out, "\n"); }
    }
    free(copy); free(lines);
    return out;
}

static char *lm_fin_csv(TestClient *tc) {
    char *r = NULL;
    if (tc_request(tc,
        "{\"mode\":\"fetch\",\"dir\":\"lay\",\"object\":\"fin\",\"offset\":0,"
        "\"limit\":100000,\"format\":\"csv\"}", &r) != 0) return NULL;
    char *sorted = lm_sorted_lines(r);
    free(r);
    return sorted;
}

typedef struct { int qty7, amt_range, bool_true, date_range; char sum[64]; } LmFinProbe;

static int lm_fin_count(TestClient *tc, const char *criteria) {
    char req[400];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"lay\",\"object\":\"fin\",\"criteria\":%s}", criteria);
    char *r = NULL;
    int rc = tc_request(tc, req, &r);
    int n = rc == 0 ? tu_parse_count(r) : -1;
    free(r);
    return n;
}

static void lm_fin_probe(TestClient *tc, LmFinProbe *p) {
    p->qty7 = lm_fin_count(tc, "[{\"field\":\"qty\",\"op\":\"eq\",\"value\":\"7\"}]");
    p->amt_range = lm_fin_count(tc,
        "[{\"field\":\"amt\",\"op\":\"between\",\"value\":[\"100.0000\",\"250.5000\"]}]");
    p->bool_true = lm_fin_count(tc, "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]");
    p->date_range = lm_fin_count(tc,
        "[{\"field\":\"d\",\"op\":\"between\",\"value\":[\"20260101\",\"20260630\"]}]");
    p->sum[0] = '\0';
    char *r = NULL;
    if (tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"lay\",\"object\":\"fin\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"amt\",\"alias\":\"s\"}]}", &r) == 0 && r) {
        const char *s = strstr(r, "\"s\":");
        if (s) { s += 4; size_t l = strcspn(s, ",}] \r\n"); if (l < sizeof(p->sum)) { memcpy(p->sum, s, l); p->sum[l] = '\0'; } }
    }
    free(r);
}

static int lm_build_finance(LmRoot *m, TestEnv *env) {
    TestClient *tc = lm_start_client(m, env);
    if (!tc) return -1;
    lm_add_dir(tc);
    char *r = NULL;
    int ok = tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"fin\",\"splits\":8,"
        "\"max_key\":16,\"declaration_layout\":\"legacy\","
        "\"fields\":[\"amt:numeric:18,4\",\"cur:currency\",\"scaled2:numeric:12\","
        "\"note:varchar:40\",\"qty:int\",\"d:date\",\"flag:bool\"],"
        "\"indexes\":[\"qty\",\"amt\",\"d\",\"flag:bitmap\"]}", &r) == 0 &&
        r && strstr(r, "\"status\":\"created\"");
    free(r);
    r = NULL;
    ok = ok && tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"zs\",\"splits\":8,"
        "\"max_key\":16,\"declaration_layout\":\"legacy\",\"fields\":[\"v:numeric:12\"]}", &r) == 0 &&
        r && strstr(r, "\"status\":\"created\"");
    free(r);
    r = NULL;
    ok = ok && tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"zs\",\"key\":\"w\","
        "\"value\":{\"v\":\"100\"}}", &r) == 0 && r && strstr(r, "\"status\":\"inserted\"");
    free(r);
    for (int chunk = 0; ok && chunk < LM_FIN_ROWS / LM_FIN_CHUNK; chunk++) {
        size_t cap = (size_t)LM_FIN_CHUNK * 220 + 256, len;
        char *bulk = malloc(cap);
        if (!bulk) { ok = 0; break; }
        len = (size_t)snprintf(bulk, cap,
            "{\"mode\":\"bulk-insert\",\"dir\":\"lay\",\"object\":\"fin\",\"records\":[");
        for (int j = 0; j < LM_FIN_CHUNK; j++) {
            int i = chunk * LM_FIN_CHUNK + j + 1;
            long long whole = (i % 11 == 0) ? -(long long)i : i;
            len += (size_t)snprintf(bulk + len, cap - len,
                "%s{\"key\":\"f%05d\",\"value\":{\"amt\":\"%s%lld.%04d\",\"cur\":\"%d.%04d\","
                "\"scaled2\":\"%d.%02d\",\"note\":\"memo %d\",\"qty\":%d,"
                "\"d\":\"2026%02d%02d\",\"flag\":%s}}",
                j ? "," : "", i, whole < 0 ? "-" : "", whole < 0 ? -whole : whole,
                (i * 37) % 10000, i, (i * 91) % 10000, i, 1 + (i * 13) % 99, i, i % 13,
                1 + i % 12, 1 + i % 28, i % 3 == 0 ? "true" : "false");
        }
        snprintf(bulk + len, cap - len, "]}");
        r = NULL;
        ok = tc_request(tc, bulk, &r) == 0 && r && !strstr(r, "\"error\"");
        free(r);
        free(bulk);
    }
    tc_close(tc);
    test_env_stop_keep(env);
    return ok ? 0 : -1;
}

static int test_layout_migration_finance_acceptance(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build_finance(&m, &env), 0, "build legacy finance root");

    /* snapshot while still legacy: restart on the old layout, read, stop cleanly */
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "legacy restart for the snapshot");
    char *before = NULL; LmFinProbe pre = {0};
    if (tc) {
        char *r = NULL;
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"fin\"}", &r);
        ASSERT_CONTAINS(r, "\"declaration_layout\":\"legacy\"", "fixture is legacy before migration");
        free(r);
        before = lm_fin_csv(tc);
        lm_fin_probe(tc, &pre);
        r = NULL;
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"zs\",\"key\":\"w\"}", &r);
        ASSERT_CONTAINS(r, "\"v\":100}", "legacy renders a whole scale-2 value without decimals");
        free(r);
        tc_close(tc);
        test_env_stop_keep(&env);
    }
    ASSERT_TRUE(before && strlen(before) > (size_t)LM_FIN_ROWS * 20, "snapshot captured");
    ASSERT_TRUE(pre.qty7 > 0 && pre.amt_range > 0 && pre.bool_true == LM_FIN_ROWS / 3 &&
                pre.date_range > 0 && pre.sum[0], "pre-migration probes are non-trivial");

    lm_make_pre_migration(&m);
    tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "first start migrates");
    if (tc) {
        char *r = NULL;
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"fin\"}", &r);
        ASSERT_CONTAINS(r, "\"declaration_layout\":\"packed\"", "object is packed after migration");
        free(r);
        r = NULL;
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"zs\",\"key\":\"w\"}", &r);
        ASSERT_CONTAINS(r, "\"v\":100.00}",
                        "documented display change: migrated numeric:N renders its declared scale (100 -> 100.00)");
        free(r);
        char *after = lm_fin_csv(tc);
        ASSERT_TRUE(before && after && strcmp(before, after) == 0,
                    "every row is textually identical to the pre-migration snapshot");
        free(after);
        LmFinProbe post = {0};
        lm_fin_probe(tc, &post);
        ASSERT_EQ_INT(post.qty7, pre.qty7, "btree eq answer unchanged");
        ASSERT_EQ_INT(post.amt_range, pre.amt_range, "decimal range answer unchanged");
        ASSERT_EQ_INT(post.bool_true, pre.bool_true, "bitmap answer unchanged");
        ASSERT_EQ_INT(post.date_range, pre.date_range, "date range answer unchanged");
        ASSERT_EQ_STR(post.sum, pre.sum, "exact decimal sum unchanged");
        r = NULL;
        tc_request(tc, "{\"mode\":\"count\",\"dir\":\"lay\",\"object\":\"fin\"}", &r);
        ASSERT_EQ_INT(tu_parse_count(r), LM_FIN_ROWS, "record count unchanged");
        free(r);
        tc_close(tc);
        test_env_stop_keep(&env);
    }
    free(before);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-layout-migration-finance-acceptance", test_layout_migration_finance_acceptance)
```

3. Rebuild; run → `test-layout-migration-finance-acceptance: 21 passed, 0 failed`. Also run the whole file's cases:
   `./build/bin/shard-db-test run-all --filter layout-` (expect 0 failed).
4. **Mutation proof:** in `lm_build_finance`, change `1 + (i * 13) % 99` back to `(i * 13) % 100` → the textual-identity assertion must fail
   (row `f00100`: legacy `100`, packed `100.00` — finding F-3); revert.

## Task 4 — npm client: build list, precision test, docs

1. **RED (F-2).** From `npm/`: `npm install` is not needed (reuse existing `node_modules`; if absent run `npm ci`). Build the addon
   with the *current* list: `npx node-gyp rebuild --nodedir=<cached node headers dir>` (the headers for the local node are under
   `~/.cache/node-gyp/<version>`; if absent run plain `npx node-gyp rebuild`, which downloads them). Create `npm/test/precision.js` (step 3) and run
   `node test/precision.js` → expect the process to die with
   `symbol lookup error: .../shard_db.node: undefined symbol: typed_field_specs_parse`. Paste it.
2. Edit `npm/binding.gyp`. Anchor (exactly once): `        "../src/db/bitmap.c",\n` — insert immediately after it:
```
        "../src/db/agg_exact.c",
        "../src/db/decimal_order.c",
        "../src/db/decnum.c",
        "../src/db/packed_csv_command.c",
        "../src/db/packed_record.c",
        "../src/db/scalar_grammar.c",
        "../src/db/schema_wire.c",
        "../src/db/typed_bulk_insert.c",
        "../src/db/typed_index.c",
        "../src/db/typed_read_command.c",
        "../src/db/typed_row.c",
```
   Verify nothing else is missing: `grep -o 'src/db/[a-z_0-9]*\.c' build.sh | sort -u` minus `npm/binding.gyp`'s list must be exactly
   `src/db/main.c src/db/test_control.c src/db/tls.c` (CLI entry, test seam, TLS — correctly excluded from the embedded addon).
3. Create `npm/test/precision.js` (verified: passes against the rebuilt addon):

```js
'use strict'

// Exact numerics and long strings through the embedded binding. db.query()
// resolves to the raw response text; these assertions compare that text, never
// a JSON.parse()d double, because parsing is exactly where precision is lost.

const assert = require('node:assert/strict')
const os = require('os')
const path = require('path')
const fs = require('fs')
const ShardDb = require('..')

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'shard-db-precision-'))
const db = new ShardDb(tmp)

const q = async body => db.query(body)
const field = (raw, name) => {
  const m = new RegExp(`"${name}":("(?:[^"\\\\]|\\\\.)*"|[^,}\\]\\s]+)`).exec(raw)
  assert.ok(m, `field ${name} present in ${raw.slice(0, 120)}`)
  return m[1]
}

;(async () => {
  assert.equal(JSON.parse(await q({ mode: 'create-object', dir: 'fin', object: 'ledger',
    splits: 8, max_key: 32,
    fields: ['exact:numeric:26,10', 'bare:numeric', 'memo:varchar'] })).status, 'created')

  const exact = '1234567890123456.1234567890'
  const bare = '-98765432109876543210.123456789012345678'
  const memo = 'x'.repeat(100000)
  const ins = JSON.parse(await q({ mode: 'insert', dir: 'fin', object: 'ledger', key: 'k',
    value: { exact, bare, memo } }))
  assert.ok(!ins.error, 'insert succeeds')

  const raw = await q({ mode: 'get', dir: 'fin', object: 'ledger', key: 'k' })
  assert.equal(field(raw, 'exact'), exact, 'constrained decimal is the exact digit string')
  assert.equal(field(raw, 'bare'), bare, 'bare numeric is the exact digit string')
  assert.equal(field(raw, 'memo'), JSON.stringify(memo), '100000-byte varchar is complete')

  // The hazard this test exists to document: a plain JSON.parse turns the
  // decimal into a double and drops digits. Clients must keep the raw text.
  const parsed = JSON.parse(raw)
  assert.notEqual(String(parsed.exact), exact, 'JSON.parse() is lossy for this value (expected)')
  assert.equal(typeof parsed.exact, 'number')

  // Rejections leave nothing behind.
  const bad = JSON.parse(await q({ mode: 'insert', dir: 'fin', object: 'ledger', key: 'bad',
    value: { exact: '12345678901234567890123456.1' } }))
  assert.ok(bad.error, 'a decimal that cannot fit numeric:26,10 is rejected')
  assert.equal(JSON.parse(await q({ mode: 'exists', dir: 'fin', object: 'ledger', key: 'bad' })), false)

  console.log('npm precision: exact decimal, bare numeric and 100000-byte varchar round trip as raw text')
})().catch(e => { console.error(e); process.exitCode = 1 }).finally(() => {
  db.close()
  fs.rmSync(tmp, { recursive: true, force: true })
})
```

4. Rebuild (`npx node-gyp rebuild ...`) and run: `node test/precision.js` (exit 0, prints the `npm precision:` line), `node test/basic.js`
   (expect `# 28 passed, 0 failed`), `node test/csv-bytes.js` (passes). Paste all three.
5. `npm/package.json`: replace the exact line `    "test": "bun test/basic.js"` with
   `    "test": "bun test/basic.js && bun test/csv-bytes.js && bun test/precision.js"`. (Only `node` was used for verification; run the
   same three files with `bun` if available and paste, otherwise state "bun not run".)
6. `npm/README.md`: after the exact paragraph
   `` `query()` returns a `Promise<string>` — queries run on worker threads so the event loop is never blocked. ``
   insert:

```markdown
### Exact numerics and long strings

`query()` resolves to the **raw response text**. `numeric` and `currency` values are emitted as exact JSON number tokens
(`1234567890123456.1234567890`, `-98765432109876543210.123456789012345678`) and bare `varchar` values can be up to 4 MiB.
`JSON.parse()` turns a number token into an IEEE-754 double and silently drops digits, so for money:

- read the token from the raw text (or use a parser with a lossless number/BigInt hook) instead of `JSON.parse(...)`, or
- project the field as a string where your client allows it.

`npm/test/precision.js` demonstrates the exact round trip and asserts that `JSON.parse()` is lossy for a 26-digit decimal.
```

7. Prebuilds: `npm/prebuilds/linux-x64` is a stale binary artefact (F-2). **Do not regenerate or commit it in R6**; list it under
   "Needs human" in the hand-back (prebuilds are produced by the release process, `npm run prebuild`).
8. Record in evidence (no change, per D-R6-2): the Node async-handle lifetime concern from R1 stays open. `src/cli/` (`shard-cli`)
   speaks the wire protocol only and links no engine code; no change.

## Task 5 — Documentation sync (exact edits)

Each edit: open the file, find the quoted text, confirm it occurs exactly once, replace.

**5.1 `AGENTS.md`** — Segment-files bullet. Replace the exact text
`+ key bytes + value bytes, padded to `slot_size`. This is what the sequential reindex scan reads via `seg_scan_o_direct`.`
with
`+ key bytes + value bytes, at the record's **actual size** rounded up to 8 bytes (variable-length records); a rewrite may reuse a larger tombstoned slot from the stream's size-bucketed free pool, leaving zero-filled padding. This is what the sequential reindex scan reads via `seg_scan_o_direct`.`
Then replace the exact text `Driven by fields.conf. One slot = sum of field sizes (fixed). Every` (it ends a line; the next line starts with `record carries a per-record null bitmap`, keep that intact) with
`Driven by fields.conf. New objects (and every object migrated by 2026.11.1) use the **packed layout**: fixed-width scalars plus an 8-byte descriptor per varchar/numeric field whose payload is stored at its actual size (varchar up to 4 MiB; numeric:P,S exact up to P=1000; bare numeric up to 131 072 integer + 16 383 fractional digits). The widths in the table below are the legacy fixed widths. Every`

**5.2 `docs/concepts/storage-model.md`** — already describes variable-length records; confirm with `grep -n "padded to" docs/concepts/storage-model.md`
(expect no match) and make no change. Paste the grep.

**5.3 `docs/concepts/typed-records.md`** — replace the **entire table line** beginning `| `varchar` | `name:varchar:N` (N = 1..65535) |` with:
`| `varchar` | `name:varchar:N` (N = 1..4194304) or bare `name:varchar` | descriptor + content | Packed objects store an 8-byte descriptor plus the content bytes at their **actual length**; the cap is N bytes (bare = 4 MiB, 4 194 304). Legacy objects keep `N + 2` bytes (`[uint16 BE length][content]`, N ≤ 65535). |`
and replace the **entire line** beginning `| `numeric` | `price:numeric:P,S` | 8 |` with:
`| `numeric` | `price:numeric:P,S` (P ≤ 1000, S −1000..1000) or bare `price:numeric` | descriptor + digits | Packed objects store an exact decimal (8-byte descriptor + 16-byte header + ⌈digits/2⌉ coefficient bytes), parsed exactly (no float) and rounded half-away-from-zero to the declared scale on writes and query literals; bare `numeric` keeps each value's own scale (up to 131 072 integer + 16 383 fractional digits); P is enforced. A value whose integer part exceeds P−S digits is rejected. Legacy objects store a scaled int64 (8 bytes, S ≤ 18). |`
Leave the `currency` row as is (still `numeric:19,4`).

**5.4 `docs/reference/limits.md`** — replace the exact row `| varchar content | 65 535 bytes | uint16 length prefix. |` with the three rows:
```
| varchar content | 4 194 304 bytes (4 MiB) | Packed layout (default). Bare `varchar` and `varchar:N` up to 4 MiB; ceiling+1 is rejected with no record written. Legacy-layout objects: 65 535 (uint16 length prefix). |
| numeric precision / scale | P ≤ 1000, S −1000..1000 | `numeric:P,S` on packed objects. Legacy: S ≤ 18, value must fit a scaled int64. |
| bare numeric digits | 131 072 integer + 16 383 fractional | Bare `numeric` on packed objects; one more digit in either part is rejected. |
```

**5.5 `docs/release-notes/2026.11.1.md`** — append after the last bullet (the one beginning `- `add-field`, `edit-field` and `vacuum compact` work on packed-layout`):
```
- **Packed layout is the default.** New objects are packed; the first start of this release migrates every existing object. `varchar` now reaches 4 MiB and `numeric` is an exact decimal (constrained `numeric:P,S` up to P=1000, or bare `numeric` with per-value scale). See [typed records](../concepts/typed-records.md).
- **Compatibility — `numeric:N` changes meaning for new objects.** It used to mean scale 2; for a *new* object `numeric:N` is now scale 0. Migration rewrites existing `numeric:N` to `numeric:N,2`, so stored values are unchanged.
- **Compatibility — display of whole values on migrated `numeric:N`.** A legacy scale-2 field rendered the whole value `100` as `100`; after migration it renders its declared scale, `100.00`. The numeric value is identical; clients that compare the text must normalise.
- **Compatibility — strict scalar grammars (R4), exact aggregates (R3), exact predicates/indexes (R2), complete ordinary writes/reads (R1).** Malformed `bool`/`date`/`time`/`uuid`/`ip`/`enum` input and decimals that do not fit are rejected instead of coerced; aggregates over decimals are exact. Details: [changelog](../reference/changelog.md).
- **Known issues.** Bitmap-indexed fields can under-count after a keyfile shard has grown during inserts until `reindex` runs ([write-up](../plans/2026-11-04-bitmap-stale-after-resplit.md)); this predates the type work. The Node binding's async-handle lifetime concern recorded in R1 is unchanged. `JSON.parse()` on a response loses precision for wide decimals — see the npm README.
```
**5.6 `docs/reference/changelog.md`** — under `## Unreleased`, add after the existing R5b paragraph a paragraph headed
`**2026.11.1 — acceptance (R6).**` that restates the four compatibility bullets above in one paragraph and names the new cases
`test-type-correctness-acceptance`, `test-finance-routes`, `test-predicate-equivalence`, `test-layout-migration-finance-acceptance`.

**5.7 Spot-check (read-only unless a mismatch is found; report each in evidence):** `docs/query-protocol/{bulk,cas,find,aggregate,joins}.md`
against the behaviour lists in `docs/plans/2026-10-28-r1-execution-evidence.md` (R1) and the compatibility section of
`docs/plans/2026-10-30-r3-exact-aggregates-plan.md` (R3). Edit only a genuine contradiction; each edit is quoted in evidence.

**5.8 Superseded R5 doc hunks.** Do **not** apply Task 12's "not supported on packed" hunks from
`docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md`. In that file, delete the section that begins with the exact heading line
`## FOLLOW-UP NOTE — R5b (must land before R6; not part of this plan)` through the line before `## Open items handed to the human`, and
add one line under the plan's title: `> Executed; R5b and R6 landed on top of it. Superseded hunks were not applied.`
Run `git status --short docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md` first: if it shows an unrelated uncommitted edit
that is not yours, **stop and ask** (earlier sessions left one).

**5.9 Gate:** `grep -rn "not supported on packed" src docs --include='*.c' --include='*.h' --include='*.md' | grep -v '^docs/plans/'` must be empty.

## Task 6 — Hygiene / definition-of-done pre-checks

- `SKIP_TESTS=1 ./build.sh 2>&1 | grep -iE 'warning|error'` empty.
- `git diff --check` clean. `git diff -U0 | grep -E '^\+.*(TODO|FIXME)'` empty (the plan's own docs may mention the words in prose; check code files only).
- `git status --short` shows only: `build.sh`, 4 test sources (3 new + `test_layout_manifest.c`), `npm/{binding.gyp,package.json,README.md,test/precision.js}`,
  `AGENTS.md`, the docs listed in Task 5, `docs/plans/2026-11-04-*.md`, `docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md`. Nothing under `src/db/`.
- No new dependency (state "none" in evidence; `package.json` gains no entry).

## Task 7 — Normal full suite (fresh)

`SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all --jobs 4`. Expected: `0 failed`, cases = 598 + 3 new + 1 new (migration) = **602**, assertions
= 2 403 488 + 139 + 2 462 + 31 + 21 = **2 406 141**. Paste the totals line; clear `/tmp/shard-db-*`. A different count means a case failed to register — find out why before continuing.

## Task 8 — Sanitizer gate (ONCE; discharges CORE-PROCESS "Definition of done")

The stack touches locks, kfcache/bitmap cache, object lifetimes and the rebuild-transaction state machine, so the gate covers the whole stack.

```sh
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all          # x3, fresh runs, default all-core parallelism
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2   # x3, fresh runs
```

No `halt_on_error=0`, no suppression files. Clear `/tmp/shard-db-*` after every run. Watch specifically (R5b flagged these, never LSan-checked):
enum-list frees on `cmd_edit_fields` early returns, `TypedPrepared` owners in the packed converter, `rebuild_txn_*` staging. The 4 MiB / 148 KiB-decimal acceptance
case is the heaviest new one under TSan (`--jobs 2`); if it trips the runner watchdog (default 1200 s in TSan), record it and ask — do not shrink the case.
Any finding: root-cause, fix, then **rerun both gates from the top** (a fix invalidates earlier runs). Paste the exit status of all six runs.

## Task 9 — Final blind review and hand-back

1. Spawn an independent reviewer that has **not** seen this plan. Give it only the repo and the diff scope `git diff main...HEAD -- src/db` (the whole unpushed stack) plus
   the R6 delta; ask for ranked findings on concurrency, on-disk/wire format, migration crash safety, breaking changes (CORE-PROCESS review checklist). Fix and re-review.
2. `sha256sum docs/plans/2026-10-19-durability-certification-rig.md` must equal `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
3. Leave everything uncommitted. Report: diff stat, evidence path, suite totals, six sanitizer exit codes, review findings and dispositions.
4. **Needs human** (list explicitly): (i) authorise commit/push/PR/merge and choose the PR shape (D-R6-1); (ii) decide F-1 (bitmap staleness) — fix plan or accept;
   (iii) regenerate/publish npm prebuilds via the release process (F-2); (iv) run the benches; (v) the durability certification rig plan that is scheduled after this work; that plan file is git-excluded locally and will not be part of the PR diff.
   Stop here.
