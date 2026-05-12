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

    for (int i = 0; i < nbulk; i++) free((char *)entries[i].value);
    free(entries);
    unlink(path);
    bt_cache_shutdown();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-btree", test_btree_run)
