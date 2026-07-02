/* src/test/cases/test_schema_export.c
 * Port of tests/test-schema-export.sh — export-schema / import-schema
 * round-trip. Both are CLI-only (argv-form), so we drive them via
 * system() with CWD=<base> so the CLI picks up the fixture's db.env.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>




static void base_of(const char *db_root, char *out, size_t out_sz) {
    const char *slash = strrchr(db_root, '/');
    if (!slash || slash == db_root) { out[0] = '\0'; return; }
    size_t n = (size_t)(slash - db_root);
    if (n + 1 > out_sz) { out[0] = '\0'; return; }
    memcpy(out, db_root, n); out[n] = '\0';
}

static int test_schema_export_run(void) {
    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) { ASSERT_TRUE(0, "shard-db not found"); return 1; }

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char base[256]; base_of(env.db_root, base, sizeof(base));

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    /* Two tenants. */
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"migtest\"}", &resp); free(resp); resp = NULL;

    /* Three objects across two tenants, mixing types. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"mig_users\","
        "\"splits\":16,\"max_key\":32,"
        "\"fields\":[\"name:varchar:40\",\"age:int\",\"active:bool\","
                    "\"created_at:datetime\"],"
        "\"indexes\":[\"age\",\"name\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"mig_orders\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\",\"amount:numeric:18,2\","
                    "\"region:varchar:16\"],"
        "\"indexes\":[\"status\",\"status+region\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"migtest\",\"object\":\"mig_events\","
        "\"splits\":16,\"max_key\":24,"
        "\"fields\":[\"kind:varchar:24\",\"ts:datetime\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    ASSERT_TRUE(1, "3 objects created across 2 tenants");

    /* Insert one record into each. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"mig_users\","
                   "\"key\":\"u1\",\"value\":{\"name\":\"alice\",\"age\":30,\"active\":true,"
                                            "\"created_at\":\"20260427120000\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"mig_orders\","
                   "\"key\":\"o1\",\"value\":{\"status\":\"paid\",\"amount\":\"123.45\","
                                            "\"region\":\"us\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"migtest\",\"object\":\"mig_events\","
                   "\"key\":\"e1\",\"value\":{\"kind\":\"login\","
                                            "\"ts\":\"20260427121500\"}}", &resp);
    free(resp); resp = NULL;

    /* === EXPORT === */
    char out_path[300];
    snprintf(out_path, sizeof(out_path), "%s/manifest.json", base);
    unlink(out_path);
    char *out = tu_capture_cmd("cd %s && %s export-schema '%s' 2>&1",
                            base, shard_db_abs, out_path);
    ASSERT_TRUE(out && strstr(out, "exported") != NULL, "export-schema reported success");
    free(out);

    struct stat st;
    ASSERT_TRUE(stat(out_path, &st) == 0 && st.st_size > 0,
                "manifest file is non-empty");
    char *manifest = tu_read_file(out_path);
    ASSERT_NOT_NULL(manifest, "manifest read");
    if (!manifest) { tc_close(tc); test_env_stop(&env); return 1; }

    ASSERT_TRUE(strstr(manifest, "\"version\"") == NULL,
                "manifest has no version field (unused by import, removed 2026.07.1)");
    ASSERT_CONTAINS(manifest, "\"dirs\"", "manifest has dirs[]");
    ASSERT_CONTAINS(manifest, "\"objects\"", "manifest has objects[]");
    ASSERT_CONTAINS(manifest, "\"default\"", "default tenant present");
    ASSERT_CONTAINS(manifest, "\"migtest\"", "migtest tenant present");
    ASSERT_CONTAINS(manifest, "\"object\":\"mig_users\"", "mig_users entry");
    ASSERT_CONTAINS(manifest, "\"object\":\"mig_orders\"", "mig_orders entry");
    ASSERT_CONTAINS(manifest, "\"object\":\"mig_events\"", "mig_events entry");
    ASSERT_CONTAINS(manifest, "\"name:varchar:40\"", "varchar:40 (content size)");
    ASSERT_CONTAINS(manifest, "\"amount:numeric:18,2\"", "numeric scale preserved");
    ASSERT_CONTAINS(manifest, "\"age:int\"", "int simple");
    ASSERT_CONTAINS(manifest, "\"active:bool\"", "bool simple");
    ASSERT_CONTAINS(manifest, "\"created_at:datetime\"", "datetime simple");
    ASSERT_CONTAINS(manifest, "\"status+region\"", "composite index exported");
    ASSERT_CONTAINS(manifest, "\"splits\":16", "mig_users splits=16");
    ASSERT_CONTAINS(manifest, "\"max_key\":32", "mig_users max_key=32");
    ASSERT_TRUE(strstr(manifest, "\"alice\"") == NULL, "no record bodies");
    ASSERT_TRUE(strstr(manifest, "record_count") == NULL, "no record_count field");
    free(manifest);

    /* === STDOUT FORM === */
    char *stdout_man = tu_capture_cmd("cd %s && %s export-schema 2>/dev/null",
                                   base, shard_db_abs);
    ASSERT_TRUE(stdout_man && strstr(stdout_man, "\"version\"") == NULL,
                "stdout export has no version field");
    ASSERT_TRUE(stdout_man && strstr(stdout_man, "\"mig_users\"") != NULL,
                "stdout export contains mig_users");
    free(stdout_man);

    /* === WIPE + IMPORT ROUND-TRIP === */
    tc_request(tc, "{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"mig_users\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"mig_orders\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"drop-object\",\"dir\":\"migtest\",\"object\":\"mig_events\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"list-objects\",\"dir\":\"default\"}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"mig_users\"") == NULL,
                "wipe: mig_users gone from default");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"list-objects\",\"dir\":\"migtest\"}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"mig_events\"") == NULL,
                "wipe: mig_events gone from migtest");
    free(resp); resp = NULL;

    out = tu_capture_cmd("cd %s && %s import-schema '%s' --if-not-exists 2>&1",
                      base, shard_db_abs, out_path);
    ASSERT_TRUE(out && strstr(out, "created=3") != NULL, "import: created=3");
    ASSERT_TRUE(out && strstr(out, "failed=0") != NULL, "import: failed=0");
    free(out);

    /* describe-object verifies schemas. */
    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"mig_users\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"splits\":16", "mig_users splits restored");
    ASSERT_CONTAINS(resp, "\"name\":\"name\"", "mig_users name field restored");
    ASSERT_CONTAINS(resp, "\"age\"", "mig_users age index restored");
    ASSERT_CONTAINS(resp, "\"size\":42", "mig_users name varchar size=42 (on-disk)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"mig_orders\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"scale\":2", "mig_orders numeric scale=2");
    ASSERT_CONTAINS(resp, "\"status+region\"", "mig_orders composite idx");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"migtest\",\"object\":\"mig_events\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"kind\"", "mig_events kind field restored");
    free(resp); resp = NULL;

    /* Imported objects start empty. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"mig_users\"}", &resp);
    ASSERT_CONTAINS(resp, "0", "mig_users empty after import");
    free(resp); resp = NULL;

    /* === --if-not-exists IS IDEMPOTENT === */
    out = tu_capture_cmd("cd %s && %s import-schema '%s' --if-not-exists 2>&1",
                      base, shard_db_abs, out_path);
    ASSERT_TRUE(out && strstr(out, "created=0") != NULL, "rerun: created=0");
    ASSERT_TRUE(out && strstr(out, "failed=0") != NULL, "rerun: failed=0");
    free(out);

    /* Without --if-not-exists, re-import should report created=0. */
    out = tu_capture_cmd("cd %s && %s import-schema '%s' 2>&1",
                      base, shard_db_abs, out_path);
    ASSERT_TRUE(out && strstr(out, "created=0") != NULL, "no-flag rerun: created=0");
    free(out);

    /* === ERROR HANDLING === */
    out = tu_capture_cmd("cd %s && %s import-schema /tmp/shard-db_does_not_exist.json 2>&1",
                      base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "cannot open") != NULL,
                "missing manifest reports error");
    free(out);

    char bad_manifest[300];
    snprintf(bad_manifest, sizeof(bad_manifest), "%s/bad_manifest.json", base);
    FILE *bf = fopen(bad_manifest, "w");
    if (bf) { fputs("{\"version\":\"x\"}", bf); fclose(bf); }
    out = tu_capture_cmd("cd %s && %s import-schema '%s' 2>&1",
                      base, shard_db_abs, bad_manifest);
    ASSERT_TRUE(out && strstr(out, "objects") != NULL,
                "no-objects manifest reports error");
    free(out);

    unlink(out_path);
    unlink(bad_manifest);
    (void)tu_run_cmd; /* unused if not needed; kept for future cleanup helpers */
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-schema-export", test_schema_export_run)
