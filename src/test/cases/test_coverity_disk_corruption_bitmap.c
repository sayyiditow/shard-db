/* src/test/cases/test_coverity_disk_corruption_bitmap.c
 *
 * CID 1696403: bm_dict_used_bytes (bitmap.c) walked the on-disk
 * dictionary using n_values (an on-disk header field) as the loop bound,
 * with no check that each entry's [len][bytes] pair actually stayed
 * within the mapped region before bm_dict_add's next write.
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
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

static int find_first_bm_shard(const char *dir_path, char *out, size_t out_sz) {
    DIR *d = opendir(dir_path);
    if (!d) return -1;
    struct dirent *de;
    int found = -1;
    while ((de = readdir(d)) != NULL) {
        size_t nlen = strlen(de->d_name);
        if (nlen < 3) continue;
        if (strcmp(de->d_name + nlen - 3, ".bm") != 0) continue;
        snprintf(out, out_sz, "%s/%s", dir_path, de->d_name);
        found = 0;
        break;
    }
    closedir(d);
    return found;
}

static int test_coverity_bitmap_nvalues_corruption_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-bm-%d", (int)getpid());
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
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"bmobj\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"cat:varchar:16\"],\"indexes\":[\"cat:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: bmobj");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"bmobj\","
        "\"key\":\"k1\",\"value\":{\"cat\":\"red\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    char bm_dir[600];
    snprintf(bm_dir, sizeof(bm_dir), "%s/d/bmobj/indexes/cat", db_root);
    char bm_path[600];
    int rc = find_first_bm_shard(bm_dir, bm_path, sizeof(bm_path));
    ASSERT_EQ_INT(rc, 0, "found a bitmap shard file");
    if (rc != 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    int fd = open(bm_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open bitmap shard for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    /* Corrupt n_values (header offset 12, 4 bytes) to a huge value, far
       larger than the dictionary region actually holds. */
    uint32_t bad_n_values = 0x0FFFFFFFu;
    ssize_t wr = pwrite(fd, &bad_n_values, sizeof(bad_n_values), 12);
    ASSERT_EQ_INT((int)wr, (int)sizeof(bad_n_values), "corrupt n_values write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Insert a second record with a NEW distinct value — this calls
       bm_dict_add, which calls bm_dict_used_bytes using the corrupted
       n_values as its loop bound. Must not crash the daemon. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"bmobj\","
        "\"key\":\"k2\",\"value\":{\"cat\":\"blue\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"bmobj\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon survives corrupted n_values during bm_dict_add");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

TEST_REGISTER("test-coverity-bitmap-nvalues-corruption", test_coverity_bitmap_nvalues_corruption_run);
