/* src/test/cases/test_btree_bulk_merge_tie_duplicate.c
 *
 * Regression for btree_bulk_merge's rebuild-path merge loop: on an exact
 * (value,hash) tie between the on-disk snapshot and the incoming batch,
 * the old code took the existing-side copy but never advanced the
 * new-side cursor, so the tied entry from new_entries survived to be
 * appended a second time by the trailing drain loop — a physical
 * duplicate leaf entry. This seeds a tree, then merges a batch containing
 * one entry that exactly duplicates an already-seeded entry (same value
 * AND hash) plus one genuinely new entry, and asserts the duplicated
 * entry is present exactly once.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "btree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_count;
static int count_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)v; (void)vl; (void)h; (void)ctx;
    g_count++;
    return 0;
}
static int search_count(const char *path, const char *value, size_t vlen) {
    g_count = 0;
    btree_search(path, value, vlen, count_cb, NULL);
    return g_count;
}

#define SEED_COUNT 5

static int test_btree_bulk_merge_tie_duplicate_run(void) {
    char tmpl[] = "/tmp/shard-db-bulk-merge-tie-XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd < 0) { ASSERT_TRUE(0, "mkstemp"); return 1; }
    close(fd);
    unlink(tmpl);
    char path[sizeof(tmpl) + 4];
    snprintf(path, sizeof(path), "%s.idx", tmpl);
    unlink(path);

    BtEntry seed[SEED_COUNT];
    char seed_vals[SEED_COUNT][32];
    for (int i = 0; i < SEED_COUNT; i++) {
        int vlen = snprintf(seed_vals[i], sizeof(seed_vals[i]), "seed_%02d", i);
        seed[i].value = seed_vals[i];
        seed[i].vlen = (size_t)vlen;
        memset(seed[i].hash, 0, BT_HASH_SIZE);
        memcpy(seed[i].hash, &i, sizeof(int));
    }
    ASSERT_EQ_INT(btree_bulk_merge(path, seed, SEED_COUNT), 0, "seed rebuild");
    ASSERT_EQ_INT(search_count(path, seed_vals[2], seed[2].vlen), 1,
        "seed entry present once before merge");

    /* new_entries: one EXACT duplicate of seed[2] (same value + hash),
       plus one genuinely new entry. bt_cmp_entry sorts by (value,hash),
       so after qsort the duplicate is new_entries[0] or [1] depending on
       value ordering — either way it ties exactly with seed[2] during
       the merge. */
    char dup_val[32];
    memcpy(dup_val, seed_vals[2], sizeof(dup_val));
    char new_val[32];
    int new_vlen = snprintf(new_val, sizeof(new_val), "zzz_new");

    BtEntry batch[2];
    batch[0].value = dup_val;
    batch[0].vlen = seed[2].vlen;
    memcpy(batch[0].hash, seed[2].hash, BT_HASH_SIZE);
    batch[1].value = new_val;
    batch[1].vlen = (size_t)new_vlen;
    memset(batch[1].hash, 0, BT_HASH_SIZE);
    batch[1].hash[0] = 0xAB;

    ASSERT_EQ_INT(btree_bulk_merge(path, batch, 2), 0, "merge with tie");

    ASSERT_EQ_INT(search_count(path, seed_vals[2], seed[2].vlen), 1,
        "tied entry present exactly once after merge, not duplicated");
    ASSERT_EQ_INT(search_count(path, new_val, (size_t)new_vlen), 1,
        "genuinely new entry present exactly once after merge");
    for (int i = 0; i < SEED_COUNT; i++) {
        if (i == 2) continue;
        ASSERT_EQ_INT(search_count(path, seed_vals[i], seed[i].vlen), 1,
            "untouched seed entry still present exactly once");
    }

    unlink(path);
    bt_cache_shutdown();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-btree-bulk-merge-tie-duplicate", test_btree_bulk_merge_tie_duplicate_run)
