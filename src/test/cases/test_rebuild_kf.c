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

/* Corrupt kf shard 0 of default/rebuildtest by setting file_id=0xFFFF for
   the first `n` live entries found.  Returns the number corrupted. */
static int corrupt_kf_entries(const char *db_root, int n) {
    char kfp[PATH_MAX];
    snprintf(kfp, sizeof(kfp), "%s/default/rebuildtest/data/kf/000.kf", db_root);
    int fd = open(kfp, O_RDWR);
    if (fd < 0) return 0;
    struct stat st;
    fstat(fd, &st);
    uint8_t *m = mmap(NULL, (size_t)st.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (m == MAP_FAILED) return 0;

    /* Header is 24 bytes; each entry is 24 bytes.
       Entry layout: hash[16] flag[1] stream_id[1] file_id[2 LE] offset[4] */
    int corrupted = 0;
    size_t cap = ((size_t)st.st_size - 24) / 24;
    for (size_t i = 0; i < cap && corrupted < n; i++) {
        uint8_t *e = m + 24 + i * 24;
        if (e[16] == 1) { /* flag=1 (live) */
            e[18] = 0xFF; /* file_id low byte  */
            e[19] = 0xFF; /* file_id high byte */
            corrupted++;
        }
    }
    msync(m, (size_t)st.st_size, MS_SYNC);
    munmap(m, (size_t)st.st_size);
    return corrupted;
}

static int test_rebuild_kf_run(void) {
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

    /* Register the default tenant dir. */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Create VARIABLE-format object under default dir. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rebuildtest\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"karma:int\",\"username:varchar:32\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create object");
    free(resp); resp = NULL;

    /* Insert 200 records. */
    for (int i = 0; i < 200; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rebuildtest\","
            "\"key\":\"user%04d\",\"value\":{\"karma\":%d,\"username\":\"u%d\"}}",
            i, i * 10, i);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert OK");
        free(resp); resp = NULL;
    }

    /* Confirm 200 records. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rebuildtest\"}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 200, "200 records after insert");
    free(resp); resp = NULL;

    tc_close(tc);

    /* Corrupt 50 kf entries directly on disk (daemon still running). */
    int c = corrupt_kf_entries(env.db_root, 50);
    ASSERT_TRUE(c > 0, "at least 1 kf entry corrupted");

    /* Kill daemon so kf is re-read from disk on restart. */
    test_env_kill(&env);

    if (test_env_start_at(&env, saved_db_root, saved_port) != 0) return 1;
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (!tc) { test_env_stop(&env); return 1; }

    /* At least some gets should fail — corrupted file_id=0xFFFF causes
       verify_stored_key to fail to open the segment. */
    int errors = 0;
    for (int i = 0; i < 200; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rebuildtest\","
            "\"key\":\"user%04d\"}", i);
        tc_request(tc, req, &resp);
        if (resp && strstr(resp, "\"error\"")) errors++;
        free(resp); resp = NULL;
    }
    ASSERT_TRUE(errors > 0, "some gets fail after kf corruption");

    /* Run rebuild-kf. */
    tc_request(tc,
        "{\"mode\":\"rebuild-kf\",\"dir\":\"default\",\"object\":\"rebuildtest\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"ok\"", "rebuild-kf returns ok");
    /* Parse repaired count: find "repaired": and atoi the number after it. */
    const char *rp = resp ? strstr(resp, "\"repaired\":") : NULL;
    ASSERT_TRUE(rp && atoi(rp + 11) > 0, "repaired > 0");
    free(resp); resp = NULL;

    /* All 200 records should now be accessible. */
    int ok = 0;
    for (int i = 0; i < 200; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rebuildtest\","
            "\"key\":\"user%04d\"}", i);
        tc_request(tc, req, &resp);
        if (resp && strstr(resp, "\"karma\"")) ok++;
        free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(ok, 200, "all 200 records readable after rebuild-kf");

    /* Idempotency: second rebuild-kf must report repaired=0. */
    tc_request(tc,
        "{\"mode\":\"rebuild-kf\",\"dir\":\"default\",\"object\":\"rebuildtest\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"repaired\":0", "second rebuild-kf is idempotent");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-rebuild-kf", test_rebuild_kf_run)
