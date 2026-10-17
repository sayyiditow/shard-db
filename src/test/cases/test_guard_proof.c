#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "../../db/btree.h"
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

static int guard_proof_count(const char *v, size_t n,
                              const uint8_t *hash, void *ctx) {
    (void)v; (void)n; (void)hash;
    (*(int *)ctx)++;
    return 0;
}
static int guard_proof_cap(const char *v, size_t n,
                            const uint8_t *hash, void *ctx) {
    (void)v; (void)n; (void)hash; (void)ctx;
    return -1;
}
static int test_guard_proof_run(void) {
    char path[] = "/tmp/shard-guard-proof-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_TRUE(fd >= 0, "unique temporary index path");
    if (fd < 0) return 1;
    close(fd); unlink(path);
    int count = 0;
    ASSERT_EQ_INT(btree_guard_eq(path, "a", 1, guard_proof_count, &count),
                  -1, "missing index is unproven");
    uint8_t hash[16] = {1};
    ASSERT_EQ_INT(btree_insert(path, "a", 1, hash), 0, "seed index");
    ASSERT_EQ_INT(btree_guard_eq(path, "a", 1, guard_proof_count, &count),
                  0, "complete equality enumeration");
    ASSERT_EQ_INT(count, 1, "one candidate");
    ASSERT_EQ_INT(btree_guard_eq(path, "a", 1, guard_proof_cap, NULL),
                  -1, "candidate cap cannot prove absence");
    btree_cache_invalidate(path);
    fd = open(path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open private index for corruption");
    if (fd >= 0) {
        BtFileHeader h;
        ssize_t got = pread(fd, &h, sizeof(h), 0);
        ASSERT_TRUE(got == (ssize_t)sizeof(h), "read header");
        if (got == (ssize_t)sizeof(h)) {
            uint32_t bad = UINT32_MAX;
            off_t off = (off_t)h.root_page * bt_page_size +
                        (off_t)offsetof(BtPageHeader, count);
            ASSERT_TRUE(pwrite(fd, &bad, sizeof(bad), off) == sizeof(bad),
                        "corrupt slot directory count");
            count = 0;
            ASSERT_EQ_INT(btree_guard_eq(path, "a", 1, guard_proof_count, &count),
                          -1, "corrupt page cannot prove absence");
        }
        close(fd);
    }
    btree_cache_invalidate(path);
    unlink(path);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-guard-proof", test_guard_proof_run)
