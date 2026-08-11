/* Direct characterization test for btree_walk_ordered_ranges.
 * Creates real temporary btree files, inserts known entries, then exercises
 * the unified range-set walker with explicit BtOrderedRangeSpec arrays.
 * Covers: globally merged ASC/DESC, different bounds per range, early stop,
 * release/resume, and forced reopen failure retirement. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "btree.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>

#define RANGE_TEST_DIR "/tmp/shard-db-range-test-XXXXXX"

typedef struct {
    int count;
    char values[64][BT_MAX_VAL_LEN];
    size_t vlens[64];
    uint8_t hashes[64][BT_HASH_SIZE];
} CollectCtx;

static void cleanup_range_files(const char *dir,
                                const char *path_a,
                                const char *path_b) {
    btree_cache_invalidate(path_a);
    btree_cache_invalidate(path_b);
    unlink(path_a);
    unlink(path_b);
    rmdir(dir);
}

static int collect_cb(const char *value, size_t vlen,
                      const uint8_t *hash, BtOrderedWalkHandle *h, void *ctx) {
    (void)h;
    CollectCtx *c = ctx;
    if (c->count >= 64) return -1;
    size_t copy = vlen > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vlen;
    memcpy(c->values[c->count], value, copy);
    c->vlens[c->count] = copy;
    memcpy(c->hashes[c->count], hash, BT_HASH_SIZE);
    c->count++;
    return 0;
}

static int early_stop_cb(const char *value, size_t vlen,
                         const uint8_t *hash, BtOrderedWalkHandle *h, void *ctx) {
    (void)h;
    CollectCtx *c = ctx;
    if (c->count >= 2) return -1;  /* stop after 2 entries */
    size_t copy = vlen > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vlen;
    memcpy(c->values[c->count], value, copy);
    c->vlens[c->count] = copy;
    memcpy(c->hashes[c->count], hash, BT_HASH_SIZE);
    c->count++;
    return 0;
}

/* Test: globally merged ascending output from two btree files */
static int test_range_merged_asc(void) {
    char dir[] = RANGE_TEST_DIR;
    ASSERT_NOT_NULL(mkdtemp(dir), "create temp dir");

    char path_a[PATH_MAX], path_b[PATH_MAX];
    snprintf(path_a, sizeof(path_a), "%s/a.idx", dir);
    snprintf(path_b, sizeof(path_b), "%s/b.idx", dir);

    /* Insert entries: a.idx gets values "01"-"05", b.idx gets "03"-"07" */
    for (int i = 1; i <= 5; i++) {
        char val[8];
        snprintf(val, sizeof(val), "%02d", i);
        uint8_t hash[16] = {0}; hash[0] = (uint8_t)i;
        ASSERT_EQ_INT(btree_insert(path_a, val, strlen(val), hash), 0,
                      "insert into a.idx");
    }
    for (int i = 3; i <= 7; i++) {
        char val[8];
        snprintf(val, sizeof(val), "%02d", i);
        uint8_t hash[16] = {0}; hash[0] = (uint8_t)(i + 100);
        ASSERT_EQ_INT(btree_insert(path_b, val, strlen(val), hash), 0,
                      "insert into b.idx");
    }

    BtOrderedRangeSpec ranges[2] = {
        { .path = path_a, .min_val = "", .min_len = 0, .min_exclusive = 0,
          .max_val = "\xff", .max_len = 1, .max_exclusive = 0, .tie_id = 0 },
        { .path = path_b, .min_val = "", .min_len = 0, .min_exclusive = 0,
          .max_val = "\xff", .max_len = 1, .max_exclusive = 0, .tie_id = 1 },
    };

    CollectCtx ctx = {0};
    btree_walk_ordered_ranges(ranges, 2, 0, collect_cb, &ctx);

    /* Should get 10 entries total: 01-05 from a, 03-07 from b (with
       different hashes, so they're distinct entries). */
    ASSERT_EQ_INT(ctx.count, 10, "merged ASC returns 10 entries");
    /* Verify global order */
    for (int i = 1; i < ctx.count; i++) {
        int cmp = memcmp(ctx.values[i-1], ctx.values[i],
                         ctx.vlens[i-1] < ctx.vlens[i] ? ctx.vlens[i-1] : ctx.vlens[i]);
        ASSERT_TRUE(cmp <= 0, "entries in ASC order");
    }

    cleanup_range_files(dir, path_a, path_b);
    return 0;
}

/* Test: globally merged descending output */
static int test_range_merged_desc(void) {
    char dir[] = RANGE_TEST_DIR;
    ASSERT_NOT_NULL(mkdtemp(dir), "create temp dir");

    char path_a[PATH_MAX], path_b[PATH_MAX];
    snprintf(path_a, sizeof(path_a), "%s/a.idx", dir);
    snprintf(path_b, sizeof(path_b), "%s/b.idx", dir);

    for (int i = 1; i <= 4; i++) {
        char val[8];
        snprintf(val, sizeof(val), "%02d", i);
        uint8_t hash[16] = {0}; hash[0] = (uint8_t)i;
        btree_insert(path_a, val, strlen(val), hash);
    }
    for (int i = 3; i <= 6; i++) {
        char val[8];
        snprintf(val, sizeof(val), "%02d", i);
        uint8_t hash[16] = {0}; hash[0] = (uint8_t)(i + 100);
        btree_insert(path_b, val, strlen(val), hash);
    }

    BtOrderedRangeSpec ranges[2] = {
        { .path = path_a, .min_val = "", .min_len = 0, .min_exclusive = 0,
          .max_val = "\xff", .max_len = 1, .max_exclusive = 0, .tie_id = 0 },
        { .path = path_b, .min_val = "", .min_len = 0, .min_exclusive = 0,
          .max_val = "\xff", .max_len = 1, .max_exclusive = 0, .tie_id = 1 },
    };

    CollectCtx ctx = {0};
    btree_walk_ordered_ranges(ranges, 2, 1, collect_cb, &ctx);

    /* 01-04 from a, 03-06 from b = 01,02,03(a),03(b),04,05,06 = 7 unique,
       DESC: 06, 05, 04, 04, 03(b), 03(a), 02, 01 = 8 with duplicates */
    ASSERT_EQ_INT(ctx.count, 8, "merged DESC returns 8 entries");
    for (int i = 1; i < ctx.count; i++) {
        size_t m = ctx.vlens[i-1] < ctx.vlens[i] ? ctx.vlens[i-1] : ctx.vlens[i];
        int cmp = memcmp(ctx.values[i-1], ctx.values[i], m);
        if (cmp == 0) cmp = (ctx.vlens[i-1] < ctx.vlens[i]) ? -1 :
                             (ctx.vlens[i-1] > ctx.vlens[i]) ? 1 : 0;
        ASSERT_TRUE(cmp >= 0, "entries in DESC order");
    }

    cleanup_range_files(dir, path_a, path_b);
    return 0;
}

/* Test: different lower/upper ranges per cursor */
static int test_range_different_bounds(void) {
    char dir[] = RANGE_TEST_DIR;
    ASSERT_NOT_NULL(mkdtemp(dir), "create temp dir");

    char path_a[PATH_MAX], path_b[PATH_MAX];
    snprintf(path_a, sizeof(path_a), "%s/a.idx", dir);
    snprintf(path_b, sizeof(path_b), "%s/b.idx", dir);

    for (int i = 1; i <= 10; i++) {
        char val[8];
        snprintf(val, sizeof(val), "%02d", i);
        uint8_t hash[16] = {0}; hash[0] = (uint8_t)i;
        btree_insert(path_a, val, strlen(val), hash);
        btree_insert(path_b, val, strlen(val), hash);
    }

    /* Range A: (03, 07) exclusive — should get 04, 05, 06 */
    /* Range B: [06, 10] inclusive — should get 06, 07, 08, 09, 10 */
    BtOrderedRangeSpec ranges[2] = {
        { .path = path_a, .min_val = "03", .min_len = 2, .min_exclusive = 1,
          .max_val = "07", .max_len = 2, .max_exclusive = 1, .tie_id = 0 },
        { .path = path_b, .min_val = "06", .min_len = 2, .min_exclusive = 0,
          .max_val = "10", .max_len = 2, .max_exclusive = 0, .tie_id = 1 },
    };

    CollectCtx ctx = {0};
    btree_walk_ordered_ranges(ranges, 2, 0, collect_cb, &ctx);

    /* 04,05,06 from A (exclusive bounds); 06,07,08,09,10 from B; merged: 04,05,06(a),06(b),07,08,09,10 */
    ASSERT_EQ_INT(ctx.count, 8, "different bounds returns 8 entries");
    for (int i = 1; i < ctx.count; i++) {
        size_t m = ctx.vlens[i-1] < ctx.vlens[i] ? ctx.vlens[i-1] : ctx.vlens[i];
        int cmp = memcmp(ctx.values[i-1], ctx.values[i], m);
        if (cmp == 0) cmp = (ctx.vlens[i-1] < ctx.vlens[i]) ? -1 :
                             (ctx.vlens[i-1] > ctx.vlens[i]) ? 1 : 0;
        ASSERT_TRUE(cmp <= 0, "entries globally ordered");
    }

    cleanup_range_files(dir, path_a, path_b);
    return 0;
}

/* Test: early callback stop */
static int test_range_early_stop(void) {
    char dir[] = RANGE_TEST_DIR;
    ASSERT_NOT_NULL(mkdtemp(dir), "create temp dir");

    char path_a[PATH_MAX], path_b[PATH_MAX];
    snprintf(path_a, sizeof(path_a), "%s/a.idx", dir);
    snprintf(path_b, sizeof(path_b), "%s/b.idx", dir);

    for (int i = 1; i <= 5; i++) {
        char val[8];
        snprintf(val, sizeof(val), "%02d", i);
        uint8_t hash[16] = {0}; hash[0] = (uint8_t)i;
        btree_insert(path_a, val, strlen(val), hash);
        btree_insert(path_b, val, strlen(val), hash);
    }

    BtOrderedRangeSpec ranges[2] = {
        { .path = path_a, .min_val = "", .min_len = 0, .min_exclusive = 0,
          .max_val = "\xff", .max_len = 1, .max_exclusive = 0, .tie_id = 0 },
        { .path = path_b, .min_val = "", .min_len = 0, .min_exclusive = 0,
          .max_val = "\xff", .max_len = 1, .max_exclusive = 0, .tie_id = 1 },
    };

    CollectCtx ctx = {0};
    btree_walk_ordered_ranges(ranges, 2, 0, early_stop_cb, &ctx);

    ASSERT_EQ_INT(ctx.count, 2, "early stop returns exactly 2 entries");
    /* 01(a), 01(b) are the first two globally */
    ASSERT_EQ_INT(ctx.vlens[0], 2, "first entry is 2 bytes");
    ASSERT_EQ_INT(ctx.vlens[1], 2, "second entry is 2 bytes");

    cleanup_range_files(dir, path_a, path_b);
    return 0;
}

/* Test: release/resume — callback releases, walk resumes from correct position */
typedef struct {
    int call_count;
    int release_on;     /* release on this call number */
    int resume_val;     /* resume from this value */
    CollectCtx *out;
} ReleaseResumeCtx;

static int release_resume_cb(const char *value, size_t vlen,
                             const uint8_t *hash, BtOrderedWalkHandle *h,
                             void *ctx) {
    ReleaseResumeCtx *c = ctx;
    c->call_count++;
    if (c->call_count == c->release_on) {
        btree_ordered_walk_release_for_blocking(h);
        /* After release, the walk will reopen from the resume point.
           We still need to record this entry and continue. */
    }
    if (c->out->count >= 64) return -1;
    size_t copy = vlen > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vlen;
    memcpy(c->out->values[c->out->count], value, copy);
    c->out->vlens[c->out->count] = copy;
    memcpy(c->out->hashes[c->out->count], hash, BT_HASH_SIZE);
    c->out->count++;
    return 0;
}

static int test_range_release_resume(void) {
    char dir[] = RANGE_TEST_DIR;
    ASSERT_NOT_NULL(mkdtemp(dir), "create temp dir");

    char path_a[PATH_MAX], path_b[PATH_MAX];
    snprintf(path_a, sizeof(path_a), "%s/a.idx", dir);
    snprintf(path_b, sizeof(path_b), "%s/b.idx", dir);

    for (int i = 1; i <= 10; i++) {
        char val[8];
        snprintf(val, sizeof(val), "%02d", i);
        uint8_t hash[16] = {0}; hash[0] = (uint8_t)i;
        btree_insert(path_a, val, strlen(val), hash);
        btree_insert(path_b, val, strlen(val), hash);
    }

    BtOrderedRangeSpec ranges[2] = {
        { .path = path_a, .min_val = "", .min_len = 0, .min_exclusive = 0,
          .max_val = "\xff", .max_len = 1, .max_exclusive = 0, .tie_id = 0 },
        { .path = path_b, .min_val = "", .min_len = 0, .min_exclusive = 0,
          .max_val = "\xff", .max_len = 1, .max_exclusive = 0, .tie_id = 1 },
    };

    CollectCtx collect = {0};
    ReleaseResumeCtx rctx = { .release_on = 5, .out = &collect };
    btree_walk_ordered_ranges(ranges, 2, 0, release_resume_cb, &rctx);

    /* Release/resume must neither lose nor duplicate either range. */
    ASSERT_EQ_INT(collect.count, 20,
                  "release/resume delivers every entry exactly once");

    /* Verify entries are still in order */
    for (int i = 1; i < collect.count; i++) {
        size_t m = collect.vlens[i-1] < collect.vlens[i] ?
                   collect.vlens[i-1] : collect.vlens[i];
        int cmp = memcmp(collect.values[i-1], collect.values[i], m);
        if (cmp == 0) cmp = (collect.vlens[i-1] < collect.vlens[i]) ? -1 :
                             (collect.vlens[i-1] > collect.vlens[i]) ? 1 : 0;
        ASSERT_TRUE(cmp <= 0, "entries globally ordered after release/resume");
    }

    for (int value = 1; value <= 10; value++) {
        char expected[8];
        snprintf(expected, sizeof(expected), "%02d", value);
        ASSERT_TRUE(collect.vlens[(value - 1) * 2] == 2 &&
                    memcmp(collect.values[(value - 1) * 2], expected, 2) == 0 &&
                    collect.vlens[(value - 1) * 2 + 1] == 2 &&
                    memcmp(collect.values[(value - 1) * 2 + 1], expected, 2) == 0,
                    "each range contributes one entry for every value");
    }

    cleanup_range_files(dir, path_a, path_b);
    return 0;
}

/* Test: failed reopen retirement — when one cursor fails to reopen, it stays
   retired and other ranges continue normally. Uses btree_test_fail_next_range_open_shard
   to force a reopen failure. */
#ifdef TEST_BUILD
typedef struct {
    CollectCtx collected;
    int released;
} FailedReopenCtx;

static int failed_reopen_cb(const char *value, size_t vlen,
                            const uint8_t *hash,
                            BtOrderedWalkHandle *handle,
                            void *ctx) {
    FailedReopenCtx *failure = ctx;
    int rc = collect_cb(value, vlen, hash, handle, &failure->collected);
    if (rc < 0) return rc;
    if (!failure->released) {
        failure->released = 1;
        btree_test_fail_next_range_open_shard(1);
        btree_ordered_walk_release_for_blocking(handle);
    }
    return 0;
}

static int test_range_failed_reopen_retirement(void) {
    char dir[] = RANGE_TEST_DIR;
    ASSERT_NOT_NULL(mkdtemp(dir), "create temp dir");

    char path_a[PATH_MAX], path_b[PATH_MAX];
    snprintf(path_a, sizeof(path_a), "%s/000.idx", dir);
    snprintf(path_b, sizeof(path_b), "%s/001.idx", dir);

    /* Cursor 0 survives with F=1,F=3. Cursor 1 owns F=2 and is forced to
       fail its reopen after the callback releases both iterators at F=1. */
    uint8_t h01a[16] = {0}; h01a[0] = 0x01;
    uint8_t h02b[16] = {0}; h02b[0] = 0x02;
    uint8_t h03a[16] = {0}; h03a[0] = 0x03;
    ASSERT_EQ_INT(btree_insert(path_a, "01", 2, h01a), 0,
                  "insert F=1 on surviving range");
    ASSERT_EQ_INT(btree_insert(path_a, "03", 2, h03a), 0,
                  "insert F=3 on surviving range");
    ASSERT_EQ_INT(btree_insert(path_b, "02", 2, h02b), 0,
                  "insert F=2 on failed-reopen range");

    BtOrderedRangeSpec ranges[2] = {
        { .path = path_a, .min_val = "", .min_len = 0, .min_exclusive = 0,
          .max_val = "\xff", .max_len = 1, .max_exclusive = 0, .tie_id = 0 },
        { .path = path_b, .min_val = "", .min_len = 0, .min_exclusive = 0,
          .max_val = "\xff", .max_len = 1, .max_exclusive = 0, .tie_id = 1 },
    };

    FailedReopenCtx ctx = {0};
    btree_walk_ordered_ranges(ranges, 2, 0, failed_reopen_cb, &ctx);
    btree_test_fail_next_range_open_shard(-1);

    ASSERT_TRUE(ctx.released, "callback releases iterators before reopen");
    ASSERT_EQ_INT(ctx.collected.count, 2,
                  "failed range stays retired while surviving range continues");
    ASSERT_TRUE(ctx.collected.vlens[0] == 2 &&
                memcmp(ctx.collected.values[0], "01", 2) == 0 &&
                ctx.collected.vlens[1] == 2 &&
                memcmp(ctx.collected.values[1], "03", 2) == 0,
                "failed reopen yields F=1,F=3 without stale F=2");

    cleanup_range_files(dir, path_a, path_b);
    return 0;
}
#endif

TEST_REGISTER("test-range-merged-asc", test_range_merged_asc);
TEST_REGISTER("test-range-merged-desc", test_range_merged_desc);
TEST_REGISTER("test-range-different-bounds", test_range_different_bounds);
TEST_REGISTER("test-range-early-stop", test_range_early_stop);
TEST_REGISTER("test-range-release-resume", test_range_release_resume);
#ifdef TEST_BUILD
TEST_REGISTER("test-range-failed-reopen-retirement", test_range_failed_reopen_retirement);
#endif
