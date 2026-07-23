/* src/test/cases/test_coverity_disk_corruption_btree.c
 *
 * CID 1696448 / CID 1696465: bt_page() call sites in iter_init_desc_leaves
 * and btree_walk_all_values followed on-disk page-chain pointers
 * (next_leaf / child pointers) with no bound against the file's actual
 * page_count and no cycle guard. A corrupted next_leaf pointing back at
 * its own page (a 1-cycle) would spin the descending iterator forever
 * pre-fix.
 *
 * CID 1696431: btree_decode_leaves_in_range (io_direct.c) trusted an
 * on-disk leaf page's `count` field as a loop bound for slot-table
 * iteration with no clamp against how many slots can actually fit in one
 * page.
 *
 * Discovers the on-disk .idx file at runtime (rather than hand-computing
 * hash routing) by reading each shard file's BtFileHeader.entry_count
 * until a non-empty shard is found.
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

#define BT_PAGE_SIZE 4096

/* Read a BtFileHeader's entry_count (offset 16, 8 bytes) from an .idx
   file. Returns 0 on read failure (treated as "empty"). */
static uint64_t read_entry_count(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return 0;
    uint64_t ec = 0;
    ssize_t n = pread(fd, &ec, sizeof(ec), 16);
    close(fd);
    if (n != (ssize_t)sizeof(ec)) return 0;
    return ec;
}

/* Scan <idx_dir> for the first NNN.idx shard whose entry_count > 0.
   Writes the full path into out. Returns 0 on success, -1 if none found. */
static int find_nonempty_idx_shard(const char *idx_dir, char *out, size_t out_sz) {
    DIR *d = opendir(idx_dir);
    if (!d) return -1;
    struct dirent *de;
    int found = -1;
    while ((de = readdir(d)) != NULL) {
        size_t nlen = strlen(de->d_name);
        if (nlen < 4) continue;
        if (strcmp(de->d_name + nlen - 4, ".idx") != 0) continue;
        char candidate[600];
        snprintf(candidate, sizeof(candidate), "%s/%s", idx_dir, de->d_name);
        if (read_entry_count(candidate) > 0) {
            snprintf(out, out_sz, "%s", candidate);
            found = 0;
            break;
        }
    }
    closedir(d);
    return found;
}

static int test_coverity_btree_nextleaf_cycle_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-bt-%d", (int)getpid());
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
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"btobj\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"n:int\"],"
        "\"indexes\":[\"n\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: btobj");
    free(resp); resp = NULL;

    for (int i = 0; i < 200; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"btobj\","
            "\"key\":\"k%03d\",\"value\":{\"n\":%d}}", i, i);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    char idx_dir[600];
    snprintf(idx_dir, sizeof(idx_dir), "%s/d/btobj/indexes/n", db_root);
    char idx_path[600];
    int rc = find_nonempty_idx_shard(idx_dir, idx_path, sizeof(idx_path));
    ASSERT_EQ_INT(rc, 0, "found a non-empty btree index shard");
    if (rc != 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    int fd = open(idx_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open idx shard for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    uint32_t root_page = 0;
    pread(fd, &root_page, sizeof(root_page), 4);
    ASSERT_TRUE(root_page > 0, "root_page discovered");

    /* Walk from the root page to the first leaf page (page_type == 1) so
       we corrupt an actual leaf's next_leaf, not an internal page's. */
    uint32_t page_id = root_page;
    uint32_t page_type = 0;
    for (int hop = 0; hop < 32; hop++) {
        off_t page_off = (off_t)page_id * BT_PAGE_SIZE;
        pread(fd, &page_type, sizeof(page_type), page_off + 0);
        if (page_type == 1) break;
        /* Internal page: descend via the first entry's child pointer.
           Entry layout is not needed byte-precise here — if this object's
           200 records fit in a single leaf (root_page IS the leaf), the
           loop above already broke out with page_type==1 on the first
           iteration, which is the common case for this dataset size and
           avoids needing internal-page entry parsing at all. */
        break;
    }
    ASSERT_EQ_INT((int)page_type, 1, "located a leaf page");

    /* Corrupt next_leaf (page-relative offset 8) to point at this same
       page — a 1-cycle that would spin the pre-fix descending iterator
       forever. */
    off_t page_off = (off_t)page_id * BT_PAGE_SIZE;
    uint32_t self_cycle = page_id;
    ssize_t wr = pwrite(fd, &self_cycle, sizeof(self_cycle), page_off + 8);
    ASSERT_EQ_INT((int)wr, (int)sizeof(self_cycle), "corrupt next_leaf write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    cfg.io_timeout_ms = 10000; /* short timeout: pre-fix this would hang */
    tc_close(tc);
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Descending order_by drives iter_init_desc_leaves through the
       corrupted next_leaf pointer. Must return (not hang) and must not
       crash the daemon. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d\",\"object\":\"btobj\","
        "\"criteria\":[],\"order_by\":{\"field\":\"n\",\"dir\":\"desc\"},"
        "\"limit\":5}", &resp);
    ASSERT_NOT_NULL(resp, "descending find returns instead of hanging");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"btobj\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon still responsive after cycle corruption");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

/* --- CID 1696431: corrupt a leaf page's `count` field to an oversized
   value ahead of a full-index O_DIRECT leaf scan (vacuum triggers one). */
static int test_coverity_btree_leafcount_overflow_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-bt2-%d", (int)getpid());
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
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"btobj2\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"n:int\"],"
        "\"indexes\":[\"n\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: btobj2");
    free(resp); resp = NULL;

    for (int i = 0; i < 50; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"btobj2\","
            "\"key\":\"k%03d\",\"value\":{\"n\":%d}}", i, i);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    char idx_dir[600];
    snprintf(idx_dir, sizeof(idx_dir), "%s/d/btobj2/indexes/n", db_root);
    char idx_path[600];
    int rc = find_nonempty_idx_shard(idx_dir, idx_path, sizeof(idx_path));
    ASSERT_EQ_INT(rc, 0, "found a non-empty btree index shard");
    if (rc != 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    int fd = open(idx_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open idx shard for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    uint32_t root_page = 0;
    pread(fd, &root_page, sizeof(root_page), 4);
    ASSERT_TRUE(root_page > 0, "root_page discovered");

    /* Corrupt the root/leaf page's count field (page-relative offset 4)
       to a huge value. With 50 records, splits:8, index_splits_for(8)=2,
       the leaf almost certainly fits in one page (root IS the leaf). */
    off_t page_off = (off_t)root_page * BT_PAGE_SIZE;
    uint32_t bad_count = 0xFFFFFF00u;
    ssize_t wr = pwrite(fd, &bad_count, sizeof(bad_count), page_off + 4);
    ASSERT_EQ_INT((int)wr, (int)sizeof(bad_count), "corrupt leaf count write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    /* vacuum triggers a full-index O_DIRECT leaf scan via
       btree_decode_leaves_in_range. Must not crash the daemon. */
    tc_request(tc, "{\"mode\":\"vacuum\",\"dir\":\"d\",\"object\":\"btobj2\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"btobj2\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon survives corrupted leaf count during vacuum");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

TEST_REGISTER("test-coverity-btree-nextleaf-cycle", test_coverity_btree_nextleaf_cycle_run);
TEST_REGISTER("test-coverity-btree-leafcount-overflow", test_coverity_btree_leafcount_overflow_run);
