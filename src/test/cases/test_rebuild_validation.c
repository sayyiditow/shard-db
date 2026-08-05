#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <limits.h>

/* Corrupt one live kf entry's file_id to 0xFFFF across all shards of
   default/rebuildvalidation.  Returns 1 on success, 0 if nothing found. */
static int corrupt_kf_file_id(const char *db_root) {
    for (int shard = 0; shard < 8; shard++) {
        char kfp[PATH_MAX];
        snprintf(kfp, sizeof(kfp),
                 "%s/default/rebuildvalidation/data/kf/%03d.kf", db_root, shard);
        int fd = open(kfp, O_RDWR);
        if (fd < 0) continue;
        struct stat st;
        fstat(fd, &st);
        if ((size_t)st.st_size < 24 + 24) { close(fd); continue; }
        uint8_t *m = mmap(NULL, (size_t)st.st_size, PROT_READ|PROT_WRITE,
                          MAP_SHARED, fd, 0);
        close(fd);
        if (m == MAP_FAILED) continue;

        /* Header is 24 bytes; each entry is 24 bytes.
           Entry layout: hash[16] flag[1] stream_id[1] file_id[2 LE] offset[4] */
        int found = 0;
        size_t cap = ((size_t)st.st_size - 24) / 24;
        for (size_t i = 0; i < cap; i++) {
            uint8_t *e = m + 24 + i * 24;
            if (e[16] == 1) { /* flag=1 (live) */
                e[18] = 0xFF; /* file_id low byte  */
                e[19] = 0xFF; /* file_id high byte */
                found = 1;
                break;
            }
        }
        msync(m, (size_t)st.st_size, MS_SYNC);
        munmap(m, (size_t)st.st_size);
        if (found) return 1;
    }
    return 0;
}

static int test_rebuild_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Create object with splits=8 and one record. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rebuildvalidation\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create object");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rebuildvalidation\","
        "\"key\":\"item0000\",\"value\":{\"score\":42,\"title\":\"hello\"}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert one record");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rebuildvalidation\"}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 1, "one record before corruption");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env);

    /* Corrupt one live kf entry's file_id while daemon is down. */
    int c = corrupt_kf_file_id(saved_db_root);
    ASSERT_TRUE(c > 0, "kf entry corrupted");

    if (test_env_start_at(&env, saved_db_root, saved_port) != 0) return 1;
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (!tc) { test_env_stop(&env); return 1; }

    /* Rebuild via splits change 8 → 16 must abort with invalid kf reference. */
    tc_request(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"rebuildvalidation\","
        "\"splits\":16}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"Rebuild aborted: 1 invalid live kf reference; original data restored\"",
                    "rebuild rejects invalid kf entry");
    free(resp); resp = NULL;

    /* Original data restored but kf is still corrupted: get fails because
       the kf entry points to file_id=0xFFFF (no such segment). Count reads
       from kf headers (total=1, deleted=0) and shows 1. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rebuildvalidation\"}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 1, "count unchanged after rebuild abort");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rebuildvalidation\",\"key\":\"item0000\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "item0000 not readable (kf still corrupted)");
    free(resp); resp = NULL;

    /* Schema must still be eight splits. */
    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"rebuildvalidation\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"splits\":8", "schema still has eight splits");
    free(resp); resp = NULL;

    /* No rebuild-transaction artifacts left behind. */
    char txn_active[PATH_MAX], txn_done[PATH_MAX], txn_preparing[PATH_MAX];
    snprintf(txn_active, sizeof(txn_active),
             "%s/default/rebuildvalidation/.rebuild_txn.active", env.db_root);
    snprintf(txn_done, sizeof(txn_done),
             "%s/default/rebuildvalidation/.rebuild_txn.done", env.db_root);
    snprintf(txn_preparing, sizeof(txn_preparing),
             "%s/default/rebuildvalidation/.rebuild_txn.preparing", env.db_root);
    ASSERT_TRUE(access(txn_active, F_OK) != 0, "no .rebuild_txn.active left behind");
    ASSERT_TRUE(access(txn_done, F_OK) != 0, "no .rebuild_txn.done left behind");
    ASSERT_TRUE(access(txn_preparing, F_OK) != 0, "no .rebuild_txn.preparing left behind");

    /* rebuild-kf mode must be rejected. */
    tc_request(tc,
        "{\"mode\":\"rebuild-kf\",\"dir\":\"default\",\"object\":\"rebuildvalidation\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"Unknown mode: rebuild-kf\"",
                    "removed rebuild-kf mode is rejected");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-rebuild-validation", test_rebuild_validation_run)
