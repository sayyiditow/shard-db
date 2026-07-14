/* src/test/cases/test_restore.c
 * Backup → mutate → restore round-trip. Verifies the live data tree
 * is replaced by the backup snapshot, that ucache invalidation lets
 * subsequent reads see the restored state, and that the safety guards
 * (missing backup, non-empty live tree without --force) fire.
 */
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
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* Pull "<key>":"<value>" out of a JSON-ish response. Stops on first '"'. */
static int extract_str(const char *resp, const char *key, char *out, size_t out_sz) {
    if (!resp) return 0;
    char needle[64]; snprintf(needle, sizeof(needle), "\"%s\":\"", key);
    const char *p = SAFE_STRSTR(resp, needle);
    if (!p) return 0;
    p += strlen(needle);
    const char *q = strchr(p, '"');
    if (!q) return 0;
    size_t n = (size_t)(q - p);
    if (n + 1 > out_sz) n = out_sz - 1;
    memcpy(out, p, n); out[n] = '\0';
    return 1;
}

/* Trailing path component (basename) of a slash-separated path. */
static const char *path_tail(const char *p) {
    const char *t = strrchr(p, '/');
    return t ? t + 1 : p;
}

static int test_restore_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rest\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"],\"indexes\":[\"name\"]}",
        &resp); free(resp); resp = NULL;

    /* Seed 5 records — small enough to verify each by GET. */
    for (int i = 1; i <= 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rest\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"n%d\"}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* Backup. Pull the timestamp out of the response path. */
    tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"rest\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"backed_up\"", "backup succeeded");
    char path[256];
    if (!extract_str(resp, "path", path, sizeof(path))) {
        ASSERT_TRUE(0, "backup path extracted");
        free(resp); tc_close(tc); test_env_stop(&env); return 1;
    }
    free(resp); resp = NULL;
    char ts[64];
    snprintf(ts, sizeof(ts), "%s", path_tail(path));

    /* Refuse: live tree is non-empty. */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\","
            "\"from\":\"%s\"}", ts);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\"", "restore refuses non-empty live tree");
        ASSERT_CONTAINS(resp, "force=true", "error mentions force=true workaround");
        free(resp); resp = NULL;
    }

    /* Now mutate live state: delete k3, change k1 → "MUTATED". After
       restore we expect k3 back and k1 == "n1". */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"rest\",\"key\":\"k3\"}",
                   &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"rest\","
        "\"key\":\"k1\",\"value\":{\"name\":\"MUTATED\"}}", &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rest\",\"key\":\"k1\"}",
                   &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"MUTATED\"", "k1 mutated pre-restore");
    free(resp); resp = NULL;

    /* Restore with force=true. */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\","
            "\"from\":\"%s\",\"force\":true}", ts);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "restore status");
        ASSERT_CONTAINS(resp, "\"object\":\"rest\"", "restore echoes object");
        free(resp); resp = NULL;
    }

    /* Verify all 5 records reappear with their original values. */
    for (int i = 1; i <= 5; i++) {
        char req[256], want[64];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rest\",\"key\":\"k%d\"}", i);
        tc_request(tc, req, &resp);
        snprintf(want, sizeof(want), "\"name\":\"n%d\"", i);
        char desc[64]; snprintf(desc, sizeof(desc), "k%d restored to n%d", i, i);
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, want) != NULL, desc);
        free(resp); resp = NULL;
    }

    /* Indexed search still works post-restore (btree mappings refreshed). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rest\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"n3\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k3\"", "indexed search post-restore returns k3");
    free(resp); resp = NULL;

    /* Error: bogus timestamp. */
    tc_request(tc,
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\","
        "\"from\":\"19990101_000000\",\"force\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"backup not found", "bogus from rejected");
    free(resp); resp = NULL;

    /* Error: traversal in from. */
    tc_request(tc,
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\","
        "\"from\":\"../../../etc\",\"force\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid from", "traversal rejected");
    free(resp); resp = NULL;

    /* Error: missing from. */
    tc_request(tc,
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"from is required\"", "missing from rejected");
    free(resp); resp = NULL;

    /* Verify backup contains the schema files we promised to capture. */
    char fields_bak[400];
    snprintf(fields_bak, sizeof(fields_bak),
             "%s/default/rest/backup/%s/fields.conf", env.db_root, ts);
    {
        struct stat fst;
        ASSERT_TRUE(stat(fields_bak, &fst) == 0 && fst.st_size > 0,
                    "backup includes fields.conf");
    }
    char meta_bak[400];
    snprintf(meta_bak, sizeof(meta_bak),
             "%s/default/rest/backup/%s/object.json", env.db_root, ts);
    {
        FILE *f = fopen(meta_bak, "r");
        ASSERT_NOT_NULL(f, "backup includes object.json");
        if (f) {
            char buf[256] = {0};
            fread(buf, 1, sizeof(buf) - 1, f);
            fclose(f);
            ASSERT_TRUE(strstr(buf, "\"splits\":16") != NULL,
                        "object.json carries splits");
            ASSERT_TRUE(strstr(buf, "\"max_key\":16") != NULL,
                        "object.json carries max_key");
        }
    }

    /* Schema-recovery path: simulate the "operator rm'd the metadata"
       failure mode. Stop the daemon-side caches by deleting the live
       fields.conf + the schema.conf line, then restore should put them
       back from the backup. */
    {
        /* Live fields.conf path. */
        char live_fields[400];
        snprintf(live_fields, sizeof(live_fields),
                 "%s/default/rest/fields.conf", env.db_root);
        unlink(live_fields);

        /* Strip the rest line out of schema.conf. */
        char schema_path[400], tmp_path[400];
        snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", env.db_root);
        snprintf(tmp_path, sizeof(tmp_path), "%s/schema.conf.test", env.db_root);
        FILE *fin = fopen(schema_path, "r");
        FILE *fout = fopen(tmp_path, "w");
        if (fin && fout) {
            char line[512];
            while (fgets(line, sizeof(line), fin))
                if (strncmp(line, "default:rest:", 13) != 0) fputs(line, fout);
        }
        if (fin) fclose(fin);
        if (fout) fclose(fout);
        rename(tmp_path, schema_path);
    }

    /* Now run restore — should recreate fields.conf + the schema.conf line
       from backup metadata. */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\","
            "\"from\":\"%s\",\"force\":true}", ts);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"restored\"",
                        "restore succeeds even with schema files deleted");
        free(resp); resp = NULL;
    }

    /* Reads work again — proves schema.conf line + fields.conf were recreated. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rest\",\"key\":\"k3\"}",
                   &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"n3\"", "post-recovery read works");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-restore", test_restore_run)
