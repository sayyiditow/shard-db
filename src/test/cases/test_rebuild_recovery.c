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

/* Corrupt the vlen of the first flag=1 segment record in stream 0 of
   rebuildrecov so slotcask_insert rejects it during the walk.
   Returns 1 on success, 0 if nothing found. */
static int corrupt_first_seg_record(const char *db_root) {
    char seg[PATH_MAX];
    snprintf(seg, sizeof(seg),
             "%s/default/rebuildrecov/data/streams/000/000000.dat", db_root);
    int fd = open(seg, O_RDWR);
    if (fd < 0) return 0;
    struct stat st;
    fstat(fd, &st);
    if (st.st_size < 24) { close(fd); return 0; }
    uint8_t *m = mmap(NULL, (size_t)st.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (m == MAP_FAILED) return 0;
    /* Segment record: hash[16] klen[2 LE] flag[1] reserved[1] vlen[4 LE]
       Corrupt vlen to 0xFFFFFFFF — keeps hash and flag intact so the
       record appears live but the payload is malformed, causing
       slotcask_insert to fail (vlen vastly exceeds slot budget). */
    int found = 0;
    size_t pos = 0;
    while (pos + 24 <= (size_t)st.st_size) {
        uint8_t *rec = m + pos;
        if (rec[18] == 1) {  /* flag=1 (live) */
            rec[20] = 0xFF;
            rec[21] = 0xFF;
            rec[22] = 0xFF;
            rec[23] = 0xFF;
            found = 1;
            break;
        }
        pos += 24;
    }
    msync(m, (size_t)st.st_size, MS_SYNC);
    munmap(m, (size_t)st.st_size);
    return found;
}

static int test_rebuild_recovery_run(void) {
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

    /* splits=8 so a bump to 16 triggers rebuild_object_v2. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rebuildrecov\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create object");
    free(resp); resp = NULL;

    for (int i = 0; i < 100; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rebuildrecov\","
            "\"key\":\"item%04d\",\"value\":{\"score\":%d,\"title\":\"t%d\"}}",
            i, i * 5, i);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert OK");
        free(resp); resp = NULL;
    }

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rebuildrecov\"}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 100, "100 records before corruption");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env);

    /* Corrupt one segment record while daemon is down. */
    int c = corrupt_first_seg_record(env.db_root);
    ASSERT_TRUE(c > 0, "segment record corrupted");

    if (test_env_start_at(&env, saved_db_root, saved_port) != 0) return 1;
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (!tc) { test_env_stop(&env); return 1; }

    /* Trigger rebuild_object_v2 via splits change 8 → 16.
       Bug 2 fix: corrupt record is skipped, walk completes — no error. */
    tc_request(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"rebuildrecov\",\"splits\":16}",
        &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""),
                "vacuum/rebuild succeeds despite corrupt segment record");
    free(resp); resp = NULL;

    /* At least 99 records must survive (the corrupt record is skipped). */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rebuildrecov\"}",
        &resp);
    int after = tu_parse_count(resp);
    ASSERT_TRUE(after >= 99, ">=99 records survive rebuild");
    free(resp); resp = NULL;

    /* Bug 1 fix: no .rebuild_legacy_root left behind after successful walk. */
    char legacy_path[PATH_MAX];
    snprintf(legacy_path, sizeof(legacy_path),
             "%s/default/rebuildrecov/.rebuild_legacy_root", env.db_root);
    ASSERT_TRUE(access(legacy_path, F_OK) != 0, "no .rebuild_legacy_root left behind");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-rebuild-recovery", test_rebuild_recovery_run)
