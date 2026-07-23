/* src/test/cases/test_coverity_disk_corruption_segments.c
 *
 * CID 1696451: reindex_seg_cb (index.c) trusted an on-disk segment
 * record's klen field without validating it against the FIXED-format
 * slot_size before using it in pointer arithmetic. A corrupted klen could
 * push the value pointer out of bounds.
 *
 * CID 1696428 / CID 1696427: mf_append_field (index.c, streaming reindex
 * path) and tg_estimate_cb (query_maint.c, estimate-index path) both
 * trusted an on-disk varchar length prefix without clamping it to the
 * field's actual declared size before calling tg_extract_distinct.
 *
 * Pattern: spin up a real daemon, create an object + insert real records,
 * stop the daemon, directly corrupt specific on-disk bytes, restart the
 * daemon at the same db_root/port, and confirm the vulnerable operation
 * (reindex / estimate-index) completes without crashing the daemon.
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
#include <fcntl.h>
#include <unistd.h>

/* --- CID 1696451: corrupt a FIXED-format segment record's on-disk klen. */
static int test_coverity_seg_klen_corruption_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-seg-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon spawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"segrec\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: segrec");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"segrec\","
        "\"key\":\"k1\",\"value\":{\"v\":\"hello\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    /* Locate the segment data file: <db_root>/d/segrec/data/streams/000/000000.dat */
    char seg_path[600];
    snprintf(seg_path, sizeof(seg_path), "%s/d/segrec/data/streams/000/000000.dat", db_root);
    int fd = open(seg_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open segment file for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    /* Segment record header: 16B hash + 2B klen + 1B flag + 1B reserved +
       4B vlen, at file offset 0 for the first record. Corrupt klen
       (offset 16, 2 bytes) to an oversized value. */
    uint16_t bad_klen = 60000;
    ssize_t wr = pwrite(fd, &bad_klen, sizeof(bad_klen), 16);
    ASSERT_EQ_INT((int)wr, (int)sizeof(bad_klen), "corrupt klen write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Trigger reindex_seg_cb via the streaming reindex path. Must not
       crash the daemon regardless of what it returns. */
    tc_request(tc, "{\"mode\":\"reindex\",\"dir\":\"d\",\"object\":\"segrec\"}", &resp);
    free(resp); resp = NULL;

    /* Daemon must still be alive and responsive afterward. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"segrec\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon survives corrupted-klen reindex");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

/* --- CID 1696428: corrupt an on-disk varchar length prefix ahead of a
   streaming reindex's trigram field extraction (mf_append_field). */
static int test_coverity_reindex_trigram_overflow_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-tg1-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon spawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"tgobj\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"bio:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: tgobj");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"tgobj\","
        "\"key\":\"k1\",\"value\":{\"bio\":\"hello world\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    /* Corrupt the varchar length prefix inside the stored value. Record
       layout: 16B hash + 2B klen + 1B flag + 1B reserved + 4B vlen (24B
       header) + key bytes + value bytes. key = "k1" (2 bytes). The
       "bio" field's on-disk encoding starts with its own 2-byte length
       prefix. Corrupt that prefix to a value far larger than the field's
       declared size (32 bytes → 30 content bytes max). */
    char seg_path[600];
    snprintf(seg_path, sizeof(seg_path), "%s/d/tgobj/data/streams/000/000000.dat", db_root);
    int fd = open(seg_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open segment file for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    size_t value_off = 24 + 2; /* header + klen("k1") */
    uint16_t bad_len = 60000;
    ssize_t wr = pwrite(fd, &bad_len, sizeof(bad_len), (off_t)value_off);
    ASSERT_EQ_INT((int)wr, (int)sizeof(bad_len), "corrupt varchar len write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"tgobj\","
        "\"fields\":[\"bio:trigram\"],\"force\":true}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"tgobj\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon survives corrupted-varchar-len trigram build");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

/* --- CID 1696427: same class of bug via the estimate-index JSON mode
   (tg_estimate_cb in query_maint.c), which samples records directly
   rather than going through the streaming reindex path. */
static int test_coverity_estimate_index_overflow_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-tg2-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon spawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"tgobj2\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"bio:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: tgobj2");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"tgobj2\","
        "\"key\":\"k1\",\"value\":{\"bio\":\"hello world\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    char seg_path[600];
    snprintf(seg_path, sizeof(seg_path), "%s/d/tgobj2/data/streams/000/000000.dat", db_root);
    int fd = open(seg_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open segment file for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    size_t value_off = 24 + 2;
    uint16_t bad_len = 60000;
    ssize_t wr = pwrite(fd, &bad_len, sizeof(bad_len), (off_t)value_off);
    ASSERT_EQ_INT((int)wr, (int)sizeof(bad_len), "corrupt varchar len write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    tc_request(tc,
        "{\"mode\":\"estimate-index\",\"dir\":\"d\",\"object\":\"tgobj2\","
        "\"spec\":\"bio:trigram\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon survives corrupted-varchar-len estimate-index");
    free(resp); resp = NULL;

    /* Follow-up request confirms the daemon process itself is still alive. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"tgobj2\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon still responsive after estimate-index");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

TEST_REGISTER("test-coverity-seg-klen-corruption", test_coverity_seg_klen_corruption_run);
TEST_REGISTER("test-coverity-reindex-trigram-overflow", test_coverity_reindex_trigram_overflow_run);
TEST_REGISTER("test-coverity-estimate-index-overflow", test_coverity_estimate_index_overflow_run);
