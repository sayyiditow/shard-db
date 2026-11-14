# Type correctness acceptance fixture and verification matrix

## Primary goal

Provide one concrete finance-application acceptance case for wide decimals,
full-range bare numeric and bare varchar, plus a route matrix that prevents
calling the work complete after only a successful get.

## Phase 0 — Freeze desired public behavior before production changes

**How this serves the primary goal:** the original failure and both requested
bare types have exact value/length assertions, independent of binary floats.

This source is a proposed final acceptance case, not an implemented fix. It
uses verified fixture APIs and contains every function in full. The code is
syntax-checked during planning; daemon-backed execution is not claimed here.
Register it in the real suite when the required features exist, or temporarily
compile/run it to record baseline red before removing it from intermediate
green-suite builds. Partition stage-specific tests into their own complete
cases as each executable production plan is written; do not leave an expected
failing final acceptance test in every intermediate full-suite gate.

New source: `src/test/cases/test_type_correctness_acceptance.c`. The entire file
is the block below. Build-list insertion anchor is the exact line
`src/test/cases/test_config_encode.c` with its existing continuation backslash;
insert the new source immediately after it, preserving the list syntax.

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

    int fixed = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"fixed\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"n:numeric:26,10\",\"f:double\",\"i:int\"]}");
    if (fixed) {
        type_accept_insert_literal(tc, "fixed", "bad", "i", "2147483648", 1);
        response = type_accept_request(tc,
            "{\"mode\":\"exists\",\"dir\":\"type_accept\","
            "\"object\":\"fixed\",\"key\":\"bad\"}");
        const char *p = response;
        if (p) while (isspace((unsigned char)*p)) p++;
        ASSERT_TRUE(p && strncmp(p, "false", 5) == 0 &&
            p[5 + strspn(p + 5, " \t\r\n")] == '\0',
            "rejected integer creates no record");
        free(response);
        response = type_accept_request(tc,
            "{\"mode\":\"insert\",\"dir\":\"type_accept\","
            "\"object\":\"fixed\",\"key\":\"exact\",\"value\":{"
            "\"n\":\"1234567890123456.1234567890\","
            "\"f\":\"1.2345678901234567\"}}");
        ASSERT_TRUE(response && !strstr(response, "\"error\""),
                    "exact constrained decimal insert succeeds");
        free(response);
        response = type_accept_request(tc,
            "{\"mode\":\"get\",\"dir\":\"type_accept\","
            "\"object\":\"fixed\",\"key\":\"exact\"}");
        ASSERT_TRUE(type_accept_number(response, "n",
                    "1234567890123456.1234567890"),
                    "constrained decimal retains value and scale");
        ASSERT_TRUE(type_accept_number(response, "f", "1.2345678901234567"),
                    "double output retains stored value");
        free(response);
    }

    int bare = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"bare\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"n:numeric\",\"after:varchar:8\"]}");
    if (bare) {
        type_accept_insert_literal(tc, "bare", "small", "n", "1.2300", 0);
        response = type_accept_request(tc,
            "{\"mode\":\"update\",\"dir\":\"type_accept\","
            "\"object\":\"bare\",\"key\":\"small\","
            "\"value\":{\"after\":\"tail\"}}");
        ASSERT_TRUE(response && !strstr(response, "\"error\""),
                    "update following field succeeds");
        free(response);
        response = type_accept_request(tc,
            "{\"mode\":\"get\",\"dir\":\"type_accept\","
            "\"object\":\"bare\",\"key\":\"small\"}");
        ASSERT_TRUE(type_accept_number(response, "n", "1.2300"),
                    "bare numeric scale survives following-field update");
        ASSERT_CONTAINS(response, "\"after\":\"tail\"", "following field survives");
        free(response);

        const size_t whole = 131072, fraction = 16383;
        size_t length = whole + 1 + fraction;
        char *value = malloc(length + 1);
        ASSERT_NOT_NULL(value, "maximum decimal allocation");
        if (value) {
            memset(value, '9', whole);
            value[whole] = '.';
            memset(value + whole + 1, '1', fraction);
            value[length] = '\0';
            type_accept_insert_literal(tc, "bare", "maximum", "n", value, 0);
            response = type_accept_request(tc,
                "{\"mode\":\"get\",\"dir\":\"type_accept\","
                "\"object\":\"bare\",\"key\":\"maximum\"}");
            ASSERT_TRUE(type_accept_number(response, "n", value),
                        "full PostgreSQL finite digit range round trips");
            free(response); free(value);
        }
    }

    int varchar = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"strings\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"v:varchar\",\"after:int\"]}");
    if (varchar) {
        type_accept_insert_literal(tc, "strings", "small", "v", "abc", 0);
        /* Bare varchar ceiling: 4 MiB (4,194,304 content bytes).
           64 KiB-era values are ordinary values now, not boundaries. */
        const size_t vceil = 4194304;
        char *value = malloc(vceil + 2);
        ASSERT_NOT_NULL(value, "varchar boundary allocation");
        if (value) {
            memset(value, 'a', vceil); value[vceil] = '\0';
            type_accept_insert_literal(tc, "strings", "at_max", "v", value, 0);
            char *resp = type_accept_request(tc,
                "{\"mode\":\"get\",\"dir\":\"type_accept\","
                "\"object\":\"strings\",\"key\":\"at_max\"}");
            const char *pv = resp ? strstr(resp, "\"v\":\"") : NULL;
            ASSERT_NOT_NULL(pv, "ceiling varchar rendered");
            pv += 5;
            ASSERT_TRUE(pv && memcmp(pv, value, vceil) == 0 &&
                        pv[vceil] == '"',
                        "ceiling varchar round trips without truncation");
            free(resp);
            value[vceil] = 'a'; value[vceil + 1] = '\0';
            type_accept_insert_literal(tc, "strings", "over_max", "v", value, 1);
            free(value);
        }
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-type-correctness-acceptance",
              test_type_correctness_acceptance_run)
```

## Final acceptance execution

**How this serves the primary goal:** verifies the actual protocol response
without parsing exact decimal tokens through double.

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-type-correctness-acceptance
rtk proxy ./build/bin/shard-db-test run-all
```

Expected base-branch failures: integer overflow accepted / record exists, wrong
constrained numeric value, double output clipped, bare numeric schema rejected,
bare varchar schema rejected. Compile/run with the future fixes and capture
green; temporarily reverse the relevant production fixes while retaining
regressions to prove red as required by CORE-PROCESS.md.

The maximum finite number test is deliberately **131072 digits before plus
16383 after** in one value. A 1000-digit or int128-only implementation must
not satisfy it. Assertions compare full token lengths and bytes, not a rounded
float or a prefix substring. All input bytes are fixture-owned safe digits /
letters; this helper is not an example for interpolating arbitrary JSON strings.

This smoke fixture does not itself prove physical packing, every typed input
route, index equivalence, exact aggregates, migration, or reopen behavior.
Those are separate mandatory tests below and must have complete implementations
in the respective child code plans before execution is approved.

## Required phase-specific matrix

| Dimension | Required cases |
| --- | --- |
| Scalar boundaries | exact min/max and +/- one; INT64_MIN; byte -1/256; fractional/exponent/junk integer input; escaped scalar strings |
| Float | finite extremes, true underflow, nonzero subnormals, double-rounding-sensitive binary32, signed zero, nonfinite input/result rejection, native float-to-double schema widening |
| Decimal | numeric26 original and negative; adjacent last-place digits; currency; precision overflow after rounding; negative scale; scale > P; display zeros; huge exponent rejection |
| Bare numeric | differing scales in same column; full finite range; integer/fraction limits independently +1; request/memory errors; small middle-position value |
| Varchar | bare default 4 MiB (4,194,304); explicit bounds; empty/NULL; ceiling accepted, ceiling+1 rejected; 64 KiB values accepted as ordinary; UTF-8 byte limits; escapes/NUL; two strings before/after numeric |
| Writes | insert/upsert/update, JSON/delimited bulk insert/update, per-row error counts, criteria update, defaults/backfill, $incr/$copy, CAS/object guards, return_record |
| Reject preservation | old value, counts and every index remain unchanged; mixed bulk peers retain documented partial-success semantics |
| Reads | get/multi, find/fetch, projected/full, rows/dict/CSV, joins and index-only reads |
| Predicates | eq/ne/ranges/between/in/not_in, field comparisons, valid/invalid bounds and members, CAS guards, all indexed and scan routes |
| Index | exact sign/exponent/scale order; canonical equal decimals; actual key budget including NULL/composite bytes; oversized index value fails without omission/mutation; build failure preserves prior index |
| Cursor | ASC/DESC, exact ties, NULL transitions, composite bounds/resume or explicit documented rejection for unsupported shapes |
| Aggregates | exact integer/decimal totals, cancellation, large adjacent min/max, avg rounding, group equal scales, NULL, worker merge, having, top-N, leaf/scan/parallel paths |
| Physical packing | inspect actual encoded length and segment vlen for 1 vs large numeric, abc vs max varchar, first/middle/last position, two variable fields, update and vacuum; assert `24 + ceil(coefficient_digits/2)` decimal bytes (37 for 25/26 digits), `8 + content_bytes` varchar bytes, a 73744-byte maximum decimal payload and 4194304-byte maximum varchar payload, counting shared headers/alignment separately; verify the Task 0.3 maximum-schema table and tight actual-size packing |
| Schema | create/add/edit/remove/rename, defaults and enum membership, shrink refusal, scale rounding/carry, float widening, export/import logical capacity and numeric P/S |
| Legacy | old eight-byte numeric; old numeric:2 interpretation; informational P violations; old string layout; no reinterpretation before transaction; explicit upgrade failures |
| Lifecycle | keep-root reopen, backup/restore, vacuum/reindex/reshard, pending marker replay and interrupted rebuild; downgrade refusal |
| Memory | dynamic payload sizing, checked size arithmetic, field-count maxima, no 65537-byte zero-buffer misuse, no stack-sized max numeric array, deterministic cap errors |

For reopen use `test_env_stop_keep`, copy the db_root and port, then
`test_env_start_at`; do not call test_env_stop first because it deletes data.
Use isolated TestEnv fixtures or cleaned process-local objects. Every fixture
source/registration hunk belongs in the approved code plan, not executor
improvisation. Avoid process-global mutable environment, fixed ports and order
dependence. No benchmark or separate certification-rig implementation is part
of this fixture.
