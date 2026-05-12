#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <string.h>

static void fill_hash(uint8_t *h, int id) {
    memset(h, 0, 16);
    memcpy(h, &id, sizeof(int));
}

static int cb_count;
static int cb_stop;
static int iter_cb(const uint8_t hash[16], void *ctx) {
    (void)hash; (void)ctx;
    cb_count++;
    return cb_stop ? 1 : 0;
}

static int test_keyset_run(void) {
    KeySet *ks = keyset_new(8);
    ASSERT_NOT_NULL(ks, "keyset_new");
    ASSERT_EQ_INT((int)keyset_size(ks), 0, "empty size");

    uint8_t h1[16], h2[16], h3[16], h4[16];
    fill_hash(h1, 1); fill_hash(h2, 2); fill_hash(h3, 3); fill_hash(h4, 4);

    ASSERT_EQ_INT(keyset_insert(ks, h1), 1, "insert h1");
    ASSERT_EQ_INT((int)keyset_size(ks), 1, "size after 1");
    ASSERT_EQ_INT(keyset_insert(ks, h2), 1, "insert h2");
    ASSERT_EQ_INT((int)keyset_size(ks), 2, "size after 2");

    ASSERT_EQ_INT(keyset_insert(ks, h1), 0, "dup returns 0");
    ASSERT_EQ_INT((int)keyset_size(ks), 2, "size unchanged after dup");
    ASSERT_TRUE(keyset_contains(ks, h1), "contains h1");
    ASSERT_TRUE(keyset_contains(ks, h2), "contains h2");
    ASSERT_TRUE(!keyset_contains(ks, h3), "not contains h3");

    ASSERT_EQ_INT(keyset_insert(ks, h3), 1, "insert h3");
    cb_count = 0; cb_stop = 0;
    keyset_iter(ks, iter_cb, NULL);
    ASSERT_EQ_INT(cb_count, 3, "iter 3 entries");

    cb_count = 0; cb_stop = 1;
    keyset_iter(ks, iter_cb, NULL);
    ASSERT_EQ_INT(cb_count, 1, "iter stops early");
    keyset_free(ks);

    ks = keyset_new(0);
    ASSERT_NOT_NULL(ks, "keyset_new tiny");
    fill_hash(h1, 42);
    ASSERT_EQ_INT(keyset_insert(ks, h1), 1, "insert into tiny");
    keyset_free(ks);

    ks = keyset_new(1024);
    for (int i = 0; i < 1000; i++) {
        uint8_t h[16]; fill_hash(h, i);
        keyset_insert(ks, h);
    }
    ASSERT_EQ_INT((int)keyset_size(ks), 1000, "1000 inserts");
    for (int i = 0; i < 1000; i++) {
        uint8_t h[16]; fill_hash(h, i);
        ASSERT_TRUE(keyset_contains(ks, h), "bulk contains");
    }
    fill_hash(h1, 9999);
    ASSERT_TRUE(!keyset_contains(ks, h1), "bulk missing");
    keyset_free(ks);

    ASSERT_TRUE(!keyset_contains(NULL, h1), "contains NULL safe");
    ASSERT_EQ_INT(keyset_insert(NULL, h1), -1, "insert NULL safe");
    ASSERT_EQ_INT((int)keyset_size(NULL), 0, "size NULL safe");
    cb_count = 0;
    keyset_iter(NULL, iter_cb, NULL);
    ASSERT_EQ_INT(cb_count, 0, "iter NULL safe");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-keyset", test_keyset_run)
