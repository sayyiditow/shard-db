#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "btree.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_bt_count;

static int bt_count_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)v; (void)vl; (void)h; (void)ctx;
    g_bt_count++;
    return 0;
}

static int test_btree_run(void) {
    const char *path = "/tmp/shard-db-btree-test.idx";
    unlink(path);

    uint8_t h1[16], h2[16], h3[16], h4[16], h5[16];
    memset(h1, 0, 16); h1[0] = 1;
    memset(h2, 0, 16); h2[0] = 2;
    memset(h3, 0, 16); h3[0] = 3;
    memset(h4, 0, 16); h4[0] = 4;
    memset(h5, 0, 16); h5[0] = 5;

    btree_insert(path, "Charlie", 7, h3);
    btree_insert(path, "Alice", 5, h1);
    btree_insert(path, "Eve", 3, h5);
    btree_insert(path, "Bob", 3, h2);
    btree_insert(path, "Dave", 4, h4);

    g_bt_count = 0;
    btree_search(path, "Bob", 3, bt_count_cb, NULL);
    ASSERT_EQ_INT(g_bt_count, 1, "search Bob");

    g_bt_count = 0;
    btree_search(path, "Nobody", 6, bt_count_cb, NULL);
    ASSERT_EQ_INT(g_bt_count, 0, "search missing");

    g_bt_count = 0;
    btree_range(path, "Bob", 3, "Dave", 4, bt_count_cb, NULL);
    ASSERT_EQ_INT(g_bt_count, 3, "range Bob..Dave incl");

    g_bt_count = 0;
    btree_range_ex(path, "Bob", 3, 1, "Dave", 4, 0, bt_count_cb, NULL);
    ASSERT_EQ_INT(g_bt_count, 2, "range_ex Bob excl..Dave incl");

    g_bt_count = 0;
    btree_range_desc_ex(path, "Bob", 3, 0, "Eve", 3, 0, bt_count_cb, NULL);
    ASSERT_EQ_INT(g_bt_count, 4, "range_desc_ex Bob..Eve");

    btree_delete(path, "Charlie", 7, h3);
    g_bt_count = 0;
    btree_search(path, "Charlie", 7, bt_count_cb, NULL);
    ASSERT_EQ_INT(g_bt_count, 0, "after delete");

    btree_insert(path, "Alice", 5, h1);
    g_bt_count = 0;
    btree_search(path, "Alice", 5, bt_count_cb, NULL);
    ASSERT_EQ_INT(g_bt_count, 1, "dup Alice still 1");

    unlink(path);
    int nbulk = 500;
    BtEntry *entries = malloc((size_t)nbulk * sizeof(BtEntry));
    for (int i = 0; i < nbulk; i++) {
        entries[i].value = malloc(32);
        int vlen = snprintf((char *)entries[i].value, 32, "key_%05d", i);
        entries[i].vlen = (size_t)vlen;
        memset(entries[i].hash, 0, 16);
        memcpy(entries[i].hash, &i, sizeof(int));
    }
    btree_bulk_build(path, entries, (size_t)nbulk);
    g_bt_count = 0;
    btree_search(path, "key_00250", 9, bt_count_cb, NULL);
    ASSERT_EQ_INT(g_bt_count, 1, "bulk search key_00250");

    g_bt_count = 0;
    btree_range(path, "key_00100", 9, "key_00105", 9, bt_count_cb, NULL);
    ASSERT_EQ_INT(g_bt_count, 6, "bulk range 100..105");

    /* DESC range-iter seek correctness — covers the 2026.05.x fix that
       replaced last_leaf_page → walk-left-leaf-by-leaf with proper
       root→leaf descent to max_val.  Previously, a DESC walk with a
       low max_val on a tall tree would touch every leaf rightward of
       the target before reaching the in-range leaves.  Here we verify
       only the result count: the seek-perf assertion lives in the
       bench, but if the descent navigates incorrectly we'd see wrong
       counts or missing rows. */
    {
        /* DESC iterator over [key_00100, key_00105].  Expect 6 rows in
           descending order: 105, 104, 103, 102, 101, 100. */
        BtRangeIter *it = btree_range_iter_open(path,
                                                "key_00100", 9, 0,
                                                "key_00105", 9, 0,
                                                1 /* desc */);
        ASSERT_NOT_NULL(it, "desc iter opened");
        if (it) {
            const char *prev_v = NULL;
            size_t prev_vl = 0;
            int n = 0, ordered = 1;
            const char *v; size_t vl; const uint8_t *h;
            while (btree_range_iter_next(it, &v, &vl, &h)) {
                if (prev_v) {
                    size_t m = prev_vl < vl ? prev_vl : vl;
                    if (memcmp(v, prev_v, m) > 0) ordered = 0;
                }
                prev_v = v; prev_vl = vl;
                n++;
            }
            ASSERT_EQ_INT(n, 6, "desc range 100..105 count");
            ASSERT_TRUE(ordered, "desc range emits in descending order");
            btree_range_iter_close(it);
        }

        /* DESC seek to a low max_val on the 500-entry tree.  Without
           the fix, the seek starts at the rightmost leaf
           (key_00499...) and walks leftward through ~10 leaves to
           reach the in-range zone.  With the fix, descent lands
           directly at the target leaf — same result count, far fewer
           leaf reads (proven by bench). */
        it = btree_range_iter_open(path,
                                   "key_00000", 9, 0,
                                   "key_00010", 9, 0,
                                   1 /* desc */);
        ASSERT_NOT_NULL(it, "desc seek-to-low iter opened");
        if (it) {
            int n = 0;
            const char *v; size_t vl; const uint8_t *h;
            while (btree_range_iter_next(it, &v, &vl, &h)) n++;
            ASSERT_EQ_INT(n, 11, "desc seek-to-low yields keys 00..10");
            btree_range_iter_close(it);
        }

        /* Empty-result DESC walk: bounds entirely below all stored
           keys.  Bug-shape repro: this is what the Cogito-with-zero-
           comments query looks like at the iterator level. */
        it = btree_range_iter_open(path,
                                   "aaa", 3, 0,
                                   "aaz", 3, 0,
                                   1 /* desc */);
        ASSERT_NOT_NULL(it, "desc empty-range iter opened");
        if (it) {
            int n = 0;
            const char *v; size_t vl; const uint8_t *h;
            while (btree_range_iter_next(it, &v, &vl, &h)) n++;
            ASSERT_EQ_INT(n, 0, "desc empty-range yields nothing");
            btree_range_iter_close(it);
        }
    }

    for (int i = 0; i < nbulk; i++) free((char *)entries[i].value);
    free(entries);
    unlink(path);
    bt_cache_shutdown();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-btree", test_btree_run)
