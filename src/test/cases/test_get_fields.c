/* src/test/cases/test_get_fields.c
 * Finding 7 regression: single-key `get` with a `fields` projection must
 * read the actual (v2/slotcask) record and return a bare filtered dict,
 * matching plain `get`'s documented response shape — not the dead v1
 * ucache path, which always reported "Not found".
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
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

/* Extract the value of a `"key":"..."` field from a JSON response.
   Returns a malloc'd copy of the value (caller frees) or NULL if not
   found. Strict: assumes the value is a JSON string, not a number.
   Copied from test_auto_key.c:26-41 -- this codebase's test cases don't
   share code across files. */
static char *extract_key_field(const char *resp) {
    const char *p = SAFE_STRSTR(resp, "\"key\":\"");
    if (!p) return NULL;
    p += 7;
    const char *end = strchr(p, '"');
    if (!end) return NULL;
    size_t n = (size_t)(end - p);
    char *out = malloc(n + 1);
    if (!out) return NULL;
    memcpy(out, p, n);
    out[n] = '\0';
    return out;
}

static int test_get_fields_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gf\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\",\"age:int\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gf\",\"key\":\"k1\","
        "\"value\":{\"name\":\"Alice\",\"age\":30}}", &resp);
    free(resp); resp = NULL;

    /* Plain get (control) — must already work and return a bare dict. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gf\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "plain get returns the record");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"error\""), "plain get has no error");
    free(resp); resp = NULL;

    /* get + fields — the bug: must return the record, projected, as a bare
       dict (no {"key":...,"value":{...}} wrapper), not "Not found". */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gf\",\"key\":\"k1\","
        "\"fields\":[\"name\"]}", &resp);
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"error\""), "get+fields does not report Not found");
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "get+fields returns the projected field");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"age\""), "get+fields excludes unrequested fields");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"key\":\"k1\""), "get+fields is a bare dict, no key wrapper");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"value\":{"), "get+fields is a bare dict, no value wrapper");
    free(resp); resp = NULL;

    /* Multi-field projection. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gf\",\"key\":\"k1\","
        "\"fields\":[\"name\",\"age\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "multi-field: name present");
    ASSERT_CONTAINS(resp, "\"age\":30", "multi-field: age is a JSON number");
    free(resp); resp = NULL;

    /* Every typed projection value must be emitted according to its JSON
       type.  This exercises the shared typed_get_field_str → projection
       emitter seam, including the numeric default-branch types and enum. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gf_types\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"long_v:long\",\"timestamp_v:timestamp\",\"int_v:int\","
        "\"short_v:short\",\"double_v:double\",\"float_v:float\",\"byte_v:byte\","
        "\"numeric_v:numeric:2\",\"enum_v:enum(red,green)\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gf_types\",\"key\":\"k1\","
        "\"value\":{\"long_v\":\"1234567890123\",\"timestamp_v\":\"1700000000000\","
        "\"int_v\":\"-42\",\"short_v\":\"-7\",\"double_v\":\"3.25\","
        "\"float_v\":\"2.5\",\"byte_v\":\"255\",\"numeric_v\":\"12.34\","
        "\"enum_v\":\"red\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "typed projection fixture inserted");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"gf_types\","
        "\"criteria\":[],\"fields\":\"long_v,timestamp_v,int_v,short_v,double_v,float_v,byte_v,numeric_v,enum_v\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"long_v\":1234567890123", "find projection: long is a JSON number");
    ASSERT_CONTAINS(resp, "\"timestamp_v\":1700000000000", "find projection: timestamp is a JSON number");
    ASSERT_CONTAINS(resp, "\"int_v\":-42", "find projection: int is a JSON number");
    ASSERT_CONTAINS(resp, "\"short_v\":-7", "find projection: short is a JSON number");
    ASSERT_CONTAINS(resp, "\"double_v\":3.25", "find projection: double is a JSON number");
    ASSERT_CONTAINS(resp, "\"float_v\":2.5", "find projection: float is a JSON number");
    ASSERT_CONTAINS(resp, "\"byte_v\":255", "find projection: byte is a JSON number");
    ASSERT_CONTAINS(resp, "\"numeric_v\":12.34", "find projection: numeric is a JSON number");
    ASSERT_CONTAINS(resp, "\"enum_v\":\"red\"", "find projection: enum is an uncorrupted JSON string");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"int_v\":\"-42\""), "find projection: int is not a JSON string");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"enum_v\":\"\\\"red\\\"\""), "find projection: enum has no embedded quotes");
    free(resp); resp = NULL;

    /* Dict and fetch projections use the same typed field extraction contract. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"gf_types\","
        "\"criteria\":[],\"format\":\"dict\",\"fields\":\"int_v,enum_v\"}", &resp);
    ASSERT_CONTAINS(resp, "\"int_v\":-42", "dict projection: int is a JSON number");
    ASSERT_CONTAINS(resp, "\"enum_v\":\"red\"", "dict projection: enum is a JSON string");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"gf_types\","
        "\"criteria\":[],\"format\":\"rows\",\"fields\":[\"int_v\",\"enum_v\"]}", &resp);
    ASSERT_CONTAINS(resp, "[\"k1\",-42,\"red\"]", "rows projection: numeric is a number and enum is a string");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"gf_types\","
        "\"limit\":1,\"fields\":[\"numeric_v\",\"enum_v\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"numeric_v\":12.34", "fetch projection: numeric is a JSON number");
    ASSERT_CONTAINS(resp, "\"enum_v\":\"red\"", "fetch projection: enum is a JSON string");
    free(resp); resp = NULL;

    /* Missing key with fields — must still report Not found, not crash. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gf\",\"key\":\"nope\","
        "\"fields\":[\"name\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "missing key with fields still errors");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-get-fields", test_get_fields_run)

/* Auto-key regression (Task 1 addendum): the fields branch never called
   auto_key_normalize before this fix, so a `get`+`fields` request against
   an auto_key=uuid object using the server-rendered dashed-UUID key would
   silently fail to find the record even though it exists. */
static int test_get_fields_auto_key_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gfauto\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"],\"auto_key\":\"uuid\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gfauto\","
        "\"value\":{\"name\":\"Bob\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "auto-key omit-key insert succeeds");
    char *uuid = extract_key_field(resp);
    ASSERT_NOT_NULL(uuid, "insert response carries generated key");
    ASSERT_TRUE(uuid && strlen(uuid) == 36, "generated key is a 36-char dashed uuid");
    free(resp); resp = NULL;

    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gfauto\",\"key\":\"%s\","
        "\"fields\":[\"name\"]}", uuid ? uuid : "");
    tc_request(tc, req, &resp);
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"error\""), "get+fields on auto-key object succeeds");
    ASSERT_CONTAINS(resp, "\"name\":\"Bob\"", "get+fields on auto-key object returns the projected field");
    free(resp); resp = NULL;
    free(uuid);

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-get-fields-auto-key", test_get_fields_auto_key_run)

/* decode_field composite-field hardening regression (Task 2.5): two
   varchar fields whose combined decoded length exceeds decode_field's
   4096-byte concatenation buffer must not corrupt the stack when
   requested as a composite "f1+f2" projection field. Run this case under
   AddressSanitizer (or equivalent) locally to actually observe the
   pre-fix stack-buffer-overflow -- a plain build may not visibly crash
   every run even though the memory corruption is real. */
static int test_get_fields_composite_overflow_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gfbig\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"f1:varchar:3000\",\"f2:varchar:3000\"]}", &resp);
    free(resp); resp = NULL;

    char *big1 = malloc(2900); memset(big1, 'a', 2899); big1[2899] = '\0';
    char *big2 = malloc(2900); memset(big2, 'b', 2899); big2[2899] = '\0';
    char *ins = malloc(6200);
    snprintf(ins, 6200,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gfbig\",\"key\":\"k1\","
        "\"value\":{\"f1\":\"%s\",\"f2\":\"%s\"}}", big1, big2);
    tc_request(tc, ins, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "large composite-source insert succeeds");
    free(resp); resp = NULL;
    free(ins); free(big1); free(big2);

    /* Combined f1+f2 length (5798 bytes) exceeds the old fixed 4096-byte
       cat[] buffer -- pre-fix this overflows the stack. Task 2.5 replaces
       that fixed buffer with one that grows dynamically, so the fix must
       preserve the FULL concatenation, not truncate it: decode_field's
       composite path also feeds composite criteria matching
       (query_plan.c:884), ordering keys (query.c:5495), and aggregate
       grouping (query_aggregate.c:2025) -- silent truncation there would
       silently collapse distinct values or corrupt sort/group order, not
       just degrade a display projection. Assert no error AND the exact
       expected value, not just "a response came back". */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gfbig\",\"key\":\"k1\","
        "\"fields\":[\"f1+f2\"]}", &resp);
    ASSERT_NOT_NULL(resp, "get+fields composite overflow returns a response");
    ASSERT_TRUE(resp != NULL && !SAFE_STRSTR(resp, "\"error\""),
        "get+fields composite overflow does not error");
    if (resp) {
        char expected[5799];
        memset(expected, 'a', 2899);
        memset(expected + 2899, 'b', 2899);
        expected[5798] = '\0';
        ASSERT_CONTAINS(resp, expected,
            "composite value is the full untruncated 5798-byte concatenation (no data loss)");
    }
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-get-fields-composite-overflow", test_get_fields_composite_overflow_run)

/* Skipped when running as root, since root bypasses file permission checks. */
/* Startup now validates every materialized object before accepting
   connections. Keep this regression focused on that fail-closed contract:
   chmod the kf shard while no process has it open, then require a fresh
   daemon process to refuse the database rather than serving a partial
   object. */
static int test_startup_refuses_unreadable_kf_count_run(void) {
    if (geteuid() == 0) {
        TAP_DIAG("# skipping: running as root, chmod-based open-failure "
                 "injection does not apply\n");
        return 0;
    }

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* name is NOT indexed -- every op on it (including "contains" below)
       forces a full scan_dispatch scan rather than an index-driven plan,
       regardless of which operator is used. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gfnotopen\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:32\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gfnotopen\",\"key\":\"k1\","
        "\"value\":{\"name\":\"Alice\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);

    /* Stop (keeping db_root/port -- test_env_stop would rm -rf the tree),
       revoke read/write on the kf shard while no process holds it open,
       then restart fresh at the same db_root/port so the registry cache
       from the insert above is gone. Save db_root/port to independent
       locals first: test_env_start_at's signature takes db_root as a
       plain `const char *`, and passing env.db_root back in as that
       argument while the function's own snprintf writes into
       env->db_root is a same-buffer overlap (restrict-qualifier
       violation) -- undefined behavior on paper even though it happens
       to be an identity copy in practice on this glibc. */
    char saved_db_root[PATH_MAX];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;
    test_env_stop_keep(&env);

    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/default/gfnotopen/data/kf/000.kf", saved_db_root);
    ASSERT_TRUE(chmod(kf_path, 0) == 0, "revoke kf shard permissions");

    int start_rc = test_env_start_at(&env, saved_db_root, saved_port);
    ASSERT_TRUE(start_rc != 0,
        "daemon refuses restart when a materialized KF shard is unreadable");
    if (start_rc == 0) test_env_stop(&env);
    chmod(kf_path, 0644);
    return 0;
}

TEST_REGISTER("test-startup-refuses-unreadable-kf-count",
              test_startup_refuses_unreadable_kf_count_run)
