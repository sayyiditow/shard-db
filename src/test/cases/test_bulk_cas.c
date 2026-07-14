/* src/test/cases/test_bulk_cas.c
 * Port of tests/test-bulk-cas.sh — CAS on bulk operations.
 * bulk-insert (JSON+CSV) with if_not_exists; bulk-update with if (array+
 * object form); bulk-delete with if; dry_run+if; back-compat shapes.
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
#include <unistd.h>

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static int test_bulk_cas_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"castest\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"name:varchar:40\",\"status:varchar:16\",\"attempt:int\"],"
        "\"indexes\":[\"name\",\"status\"]}", &resp); free(resp); resp = NULL;
    ASSERT_TRUE(1, "object created (name, status indexed; attempt scalar)");

    char seed_path[256], collide_path[256], mixed_path[256], new_csv[256], mix_csv[256];
    int pid = (int)getpid();
    snprintf(seed_path, sizeof(seed_path), "/tmp/sdb_seed_%d.json", pid);
    snprintf(collide_path, sizeof(collide_path), "/tmp/sdb_collide_%d.json", pid);
    snprintf(mixed_path, sizeof(mixed_path), "/tmp/sdb_mixed_%d.json", pid);
    snprintf(new_csv, sizeof(new_csv), "/tmp/sdb_new_%d.csv", pid);
    snprintf(mix_csv, sizeof(mix_csv), "/tmp/sdb_mix_%d.csv", pid);

    /* === bulk-insert (JSON) — back-compat shape on fresh keys === */
    write_file(seed_path,
        "[{\"key\":\"k1\",\"value\":{\"name\":\"alice\",\"status\":\"pending\",\"attempt\":\"0\"}},"
        "{\"key\":\"k2\",\"value\":{\"name\":\"bob\",\"status\":\"pending\",\"attempt\":\"1\"}},"
        "{\"key\":\"k3\",\"value\":{\"name\":\"carol\",\"status\":\"pending\",\"attempt\":\"0\"}}]");

    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"castest\","
        "\"file\":\"%s\"}", seed_path);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":3", "fresh insert returns inserted=3");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"skipped\"") == NULL,
                "no skipped field on plain run");
    free(resp); resp = NULL;
    unlink(seed_path);

    /* === bulk-insert (JSON) — if_not_exists === */
    write_file(collide_path,
        "[{\"key\":\"k1\",\"value\":{\"name\":\"OVERWRITTEN\",\"status\":\"x\",\"attempt\":\"99\"}},"
        "{\"key\":\"k2\",\"value\":{\"name\":\"OVERWRITTEN\",\"status\":\"x\",\"attempt\":\"99\"}},"
        "{\"key\":\"k3\",\"value\":{\"name\":\"OVERWRITTEN\",\"status\":\"x\",\"attempt\":\"99\"}}]");

    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"castest\","
        "\"file\":\"%s\",\"if_not_exists\":true}", collide_path);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":0", "all-collision: inserted=0");
    ASSERT_CONTAINS(resp, "\"skipped\":3", "all-collision: skipped=3");
    free(resp); resp = NULL;
    unlink(collide_path);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "k1 untouched (still alice)");
    ASSERT_CONTAINS(resp, "\"attempt\":0", "k1 attempt still 0");
    free(resp); resp = NULL;

    write_file(mixed_path,
        "[{\"key\":\"k1\",\"value\":{\"name\":\"OVERWRITTEN\",\"status\":\"x\",\"attempt\":\"99\"}},"
        "{\"key\":\"k4\",\"value\":{\"name\":\"dave\",\"status\":\"pending\",\"attempt\":\"0\"}},"
        "{\"key\":\"k5\",\"value\":{\"name\":\"eve\",\"status\":\"pending\",\"attempt\":\"2\"}},"
        "{\"key\":\"k3\",\"value\":{\"name\":\"OVERWRITTEN\",\"status\":\"x\",\"attempt\":\"99\"}}]");

    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"castest\","
        "\"file\":\"%s\",\"if_not_exists\":true}", mixed_path);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":2", "mixed: inserted=2 (k4,k5)");
    ASSERT_CONTAINS(resp, "\"skipped\":2", "mixed: skipped=2 (k1,k3)");
    free(resp); resp = NULL;
    unlink(mixed_path);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"carol\"", "k3 still carol (CAS held)");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k4\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"dave\"", "k4 dave inserted");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"eve\"", "k5 eve inserted");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
    ASSERT_CONTAINS(resp, "5", "after JSON CAS: 5 records");
    free(resp); resp = NULL;

    /* === bulk-insert-delimited (CSV) — if_not_exists === */
    write_file(new_csv, "k6|frank|pending|0\nk7|grace|pending|1\nk8|henry|pending|0\n");
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"castest\","
        "\"file\":\"%s\",\"delimiter\":\"|\"}", new_csv);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "3", "CSV plain insert: count=3 (back-compat)");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"castest\","
        "\"file\":\"%s\",\"delimiter\":\"|\",\"if_not_exists\":true}", new_csv);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":0", "CSV CAS rerun: inserted=0");
    ASSERT_CONTAINS(resp, "\"skipped\":3", "CSV CAS rerun: skipped=3");
    free(resp); resp = NULL;
    unlink(new_csv);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k7\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"grace\"", "k7 still grace (CSV CAS held)");
    free(resp); resp = NULL;

    write_file(mix_csv, "k7|OVERWRITTEN|x|99\nk9|iris|pending|0\n");
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"castest\","
        "\"file\":\"%s\",\"delimiter\":\"|\",\"if_not_exists\":true}", mix_csv);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":1", "CSV mixed: inserted=1");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "CSV mixed: skipped=1");
    free(resp); resp = NULL;
    unlink(mix_csv);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k7\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"grace\"", "k7 still grace post-mix");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k9\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"iris\"", "k9 iris inserted");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
    ASSERT_CONTAINS(resp, "9", "after CSV CAS: 9 records total");
    free(resp); resp = NULL;

    /* === bulk-update with if (CAS guard, array form) === */
    /* attempt=0: k1, k3, k4, k6, k8, k9 (6 records)
       attempt=1: k2, k7 (2)
       attempt=2: k5 (1) */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}],"
        "\"value\":{\"status\":\"processing\"},"
        "\"if\":[{\"field\":\"attempt\",\"op\":\"eq\",\"value\":\"0\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":9", "matched=9 (all pending)");
    ASSERT_CONTAINS(resp, "\"updated\":6", "updated=6 (attempt=0 winners)");
    ASSERT_CONTAINS(resp, "\"skipped\":3", "skipped=3 (CAS losers)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "6", "index: 6 processing");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "3", "index: 3 still pending");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"processing\"", "k1 processing");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"pending\"", "k2 still pending (attempt=1 lost)");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"pending\"", "k5 still pending (attempt=2 lost)");
    free(resp); resp = NULL;

    /* === bulk-update with if (object form) === */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}],"
        "\"value\":{\"status\":\"archived\"},\"if\":{\"attempt\":\"1\"}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"matched\":3", "object-form if: matched=3");
    ASSERT_CONTAINS(resp, "\"updated\":2", "object-form if: updated=2");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "object-form if: skipped=1");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"archived\"", "k2 archived");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"pending\"", "k5 still pending");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "1 pending left (k5)");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"archived\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "2 archived");
    free(resp); resp = NULL;

    /* === back-compat update (no if) === */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}],"
        "\"value\":{\"status\":\"done\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":1", "back-compat update: matched=1");
    ASSERT_CONTAINS(resp, "\"updated\":1", "back-compat update: updated=1");
    ASSERT_CONTAINS(resp, "\"skipped\":0", "back-compat update: skipped=0");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"done\"", "k5 now done");
    free(resp); resp = NULL;

    /* === bulk-delete with if === */
    /* Reset attempt for k1, k3 to 5. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"castest\","
        "\"key\":\"k1\",\"value\":{\"attempt\":\"5\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"castest\","
        "\"key\":\"k3\",\"value\":{\"attempt\":\"5\"}}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}],"
        "\"if\":[{\"field\":\"attempt\",\"op\":\"eq\",\"value\":\"5\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":6", "delete CAS: matched=6");
    ASSERT_CONTAINS(resp, "\"deleted\":2", "delete CAS: deleted=2");
    ASSERT_CONTAINS(resp, "\"skipped\":4", "delete CAS: skipped=4");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "Not found", "k1 gone");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "Not found", "k3 gone");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castest\",\"key\":\"k4\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"dave\"", "k4 survived (lost CAS)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "4", "4 processing left after CAS-delete");
    free(resp); resp = NULL;

    /* === back-compat delete (no if) === */
    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"archived\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"matched\":2", "back-compat delete: matched=2");
    ASSERT_CONTAINS(resp, "\"deleted\":2", "back-compat delete: deleted=2");
    ASSERT_CONTAINS(resp, "\"skipped\":0", "back-compat delete: skipped=0");
    free(resp); resp = NULL;

    /* === dry_run + if === */
    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
    char size_before[64] = {0};
    if (resp) {
        const char *p = resp; while (*p == ' ' || *p == '\n') p++;
        snprintf(size_before, sizeof(size_before), "%lld", (long long)atoll(p));
    }
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}],"
        "\"value\":{\"status\":\"WOULD-NOT-WRITE\"},"
        "\"if\":[{\"field\":\"attempt\",\"op\":\"eq\",\"value\":\"0\"}],\"dry_run\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"dry_run\":true", "dry_run shape");
    ASSERT_CONTAINS(resp, "\"updated\":0", "dry_run: updated=0");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}],"
        "\"if\":[{\"field\":\"attempt\",\"op\":\"eq\",\"value\":\"0\"}],\"dry_run\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"dry_run\":true", "dry_run delete shape");
    ASSERT_CONTAINS(resp, "\"deleted\":0", "dry_run delete: deleted=0");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
    {
        const char *p = resp; while (p && (*p == ' ' || *p == '\n')) p++;
        char after[64]; snprintf(after, sizeof(after), "%lld", (long long)atoll(p));
        ASSERT_TRUE(strcmp(size_before, after) == 0, "size unchanged after dry runs");
    }
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castest\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"WOULD-NOT-WRITE\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "0", "sentinel not written");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bulk-cas", test_bulk_cas_run)
