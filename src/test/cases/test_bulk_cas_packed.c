/* Packed copy of test_bulk_cas.c (class K original stays legacy until T3).
 * bulk-insert JSON/CSV with if_not_exists, bulk-delete with if, back-compat
 * delete, dry-run delete and the size/sentinel checks are copied. The three
 * criteria bulk-updates are replaced with equivalent per-key updates; their
 * matched/skipped CAS counts and the dry-run criteria update are dropped.
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

static int test_bulk_cas_packed_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"default\",\"object\":\"castestp\","
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
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"castestp\","
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
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"file\":\"%s\",\"if_not_exists\":true}", collide_path);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":0", "all-collision: inserted=0");
    ASSERT_CONTAINS(resp, "\"skipped\":3", "all-collision: skipped=3");
    free(resp); resp = NULL;
    unlink(collide_path);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "k1 untouched (still alice)");
    ASSERT_CONTAINS(resp, "\"attempt\":0", "k1 attempt still 0");
    free(resp); resp = NULL;

    write_file(mixed_path,
        "[{\"key\":\"k1\",\"value\":{\"name\":\"OVERWRITTEN\",\"status\":\"x\",\"attempt\":\"99\"}},"
        "{\"key\":\"k4\",\"value\":{\"name\":\"dave\",\"status\":\"pending\",\"attempt\":\"0\"}},"
        "{\"key\":\"k5\",\"value\":{\"name\":\"eve\",\"status\":\"pending\",\"attempt\":\"2\"}},"
        "{\"key\":\"k3\",\"value\":{\"name\":\"OVERWRITTEN\",\"status\":\"x\",\"attempt\":\"99\"}}]");

    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"file\":\"%s\",\"if_not_exists\":true}", mixed_path);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":2", "mixed: inserted=2 (k4,k5)");
    ASSERT_CONTAINS(resp, "\"skipped\":2", "mixed: skipped=2 (k1,k3)");
    free(resp); resp = NULL;
    unlink(mixed_path);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"carol\"", "k3 still carol (CAS held)");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k4\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"dave\"", "k4 dave inserted");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"eve\"", "k5 eve inserted");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castestp\"}", &resp);
    ASSERT_CONTAINS(resp, "5", "after JSON CAS: 5 records");
    free(resp); resp = NULL;

    /* === bulk-insert-delimited (CSV) — if_not_exists === */
    write_file(new_csv, "k6|frank|pending|0\nk7|grace|pending|1\nk8|henry|pending|0\n");
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"file\":\"%s\",\"delimiter\":\"|\"}", new_csv);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "3", "CSV plain insert: count=3 (back-compat)");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"file\":\"%s\",\"delimiter\":\"|\",\"if_not_exists\":true}", new_csv);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":0", "CSV CAS rerun: inserted=0");
    ASSERT_CONTAINS(resp, "\"skipped\":3", "CSV CAS rerun: skipped=3");
    free(resp); resp = NULL;
    unlink(new_csv);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k7\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"grace\"", "k7 still grace (CSV CAS held)");
    free(resp); resp = NULL;

    write_file(mix_csv, "k7|OVERWRITTEN|x|99\nk9|iris|pending|0\n");
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"file\":\"%s\",\"delimiter\":\"|\",\"if_not_exists\":true}", mix_csv);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":1", "CSV mixed: inserted=1");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "CSV mixed: skipped=1");
    free(resp); resp = NULL;
    unlink(mix_csv);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k7\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"grace\"", "k7 still grace post-mix");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k9\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"iris\"", "k9 iris inserted");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castestp\"}", &resp);
    ASSERT_CONTAINS(resp, "9", "after CSV CAS: 9 records total");
    free(resp); resp = NULL;

    /* Criteria bulk-update + `if` replaced with equivalent per-key updates:
       packed refuses criteria updates and bulk conditions. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"castestp\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"processing\"}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"processing\"}},"
        "{\"key\":\"k4\",\"value\":{\"status\":\"processing\"}},"
        "{\"key\":\"k6\",\"value\":{\"status\":\"processing\"}},"
        "{\"key\":\"k8\",\"value\":{\"status\":\"processing\"}},"
        "{\"key\":\"k9\",\"value\":{\"status\":\"processing\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"updated\":6", "per-key replacement: 6 processing");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "6", "index: 6 processing");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "3", "index: 3 still pending");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"processing\"", "k1 processing");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"pending\"", "k2 still pending (attempt=1 lost)");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"pending\"", "k5 still pending (attempt=2 lost)");
    free(resp); resp = NULL;

    /* Object-form criteria update replaced with per-key updates (k2, k7). */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"castestp\",\"records\":["
        "{\"key\":\"k2\",\"value\":{\"status\":\"archived\"}},"
        "{\"key\":\"k7\",\"value\":{\"status\":\"archived\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"updated\":2", "per-key replacement: 2 archived");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"archived\"", "k2 archived");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"pending\"", "k5 still pending");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "1 pending left (k5)");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"archived\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "2 archived");
    free(resp); resp = NULL;

    /* Back-compat criteria update replaced with a per-key update (k5). */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"castestp\",\"records\":["
        "{\"key\":\"k5\",\"value\":{\"status\":\"done\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"updated\":1", "per-key replacement: k5 done");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"done\"", "k5 now done");
    free(resp); resp = NULL;

    /* === bulk-delete with if === */
    /* Reset attempt for k1, k3 to 5. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"key\":\"k1\",\"value\":{\"attempt\":\"5\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"key\":\"k3\",\"value\":{\"attempt\":\"5\"}}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}],"
        "\"if\":[{\"field\":\"attempt\",\"op\":\"eq\",\"value\":\"5\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":6", "delete CAS: matched=6");
    ASSERT_CONTAINS(resp, "\"deleted\":2", "delete CAS: deleted=2");
    ASSERT_CONTAINS(resp, "\"skipped\":4", "delete CAS: skipped=4");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "Not found", "k1 gone");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "Not found", "k3 gone");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"castestp\",\"key\":\"k4\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"dave\"", "k4 survived (lost CAS)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "4", "4 processing left after CAS-delete");
    free(resp); resp = NULL;

    /* === back-compat delete (no if) === */
    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"archived\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"matched\":2", "back-compat delete: matched=2");
    ASSERT_CONTAINS(resp, "\"deleted\":2", "back-compat delete: deleted=2");
    ASSERT_CONTAINS(resp, "\"skipped\":0", "back-compat delete: skipped=0");
    free(resp); resp = NULL;

    /* === dry_run + if === */
    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"castestp\"}", &resp);
    char size_before[64] = {0};
    if (resp) {
        const char *p = resp; while (*p == ' ' || *p == '\n') p++;
        snprintf(size_before, sizeof(size_before), "%lld", (long long)atoll(p));
    }
    free(resp); resp = NULL;

    /* Dry-run criteria bulk-update dropped: packed refuses criteria updates
       (pinned by test-packed-bulk-update-criteria-refused). */

    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}],"
        "\"if\":[{\"field\":\"attempt\",\"op\":\"eq\",\"value\":\"0\"}],\"dry_run\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"dry_run\":true", "dry_run delete shape");
    ASSERT_CONTAINS(resp, "\"deleted\":0", "dry_run delete: deleted=0");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"castestp\"}", &resp);
    {
        const char *p = resp; while (p && (*p == ' ' || *p == '\n')) p++;
        char after[64]; snprintf(after, sizeof(after), "%lld",
                             p ? (long long)atoll(p) : 0LL);
        ASSERT_TRUE(strcmp(size_before, after) == 0, "size unchanged after dry runs");
    }
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castestp\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"WOULD-NOT-WRITE\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "0", "sentinel not written");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bulk-cas-packed", test_bulk_cas_packed_run)
