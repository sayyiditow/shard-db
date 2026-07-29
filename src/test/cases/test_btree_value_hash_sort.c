/* Covers BTRH (value, hash) sort invariants:
 *   - btree_insert + btree_delete on duplicate-value clusters
 *   - btree_bulk_build sorts ties by hash
 *   - btree_bulk_merge folds new + existing entries in (value, hash) order
 *   - leaf compaction path picks the right slot after re-encoding
 *
 * The pre-BTRH bug class these guard: value-only ordering left hashes in
 * insertion order, so btree_delete's value+hash bsearch landed at the
 * wrong slot and silently no-op'd. Each case below would fail before the
 * (value, hash) sort fix.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "btree.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_count;
static int count_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)v; (void)vl; (void)h; (void)ctx;
    g_count++;
    return 0;
}

/* Deletes one entry from a duplicate-value cluster (3 entries, all
   value "paid", three different hashes). Pre-BTRH bug: when the inserted
   hashes happen to be larger than the just-inserted neighbor, the leaf
   ended up unsorted by hash and the delete landed at the wrong slot. */
static int test_dup_value_delete(const char *path) {
    /* All six subtests below share one path with a single trailing
       bt_cache_shutdown(). bt_cache keys entries by path string with no
       inode check (see server.c's mode_is_write comment), so a bare
       unlink() here would leave the previous subtest's still-mmap'd
       entry live in cache; the next write would land on that stale
       mapping instead of a fresh file. Must invalidate before unlink. */
    btree_cache_invalidate(path);
    unlink(path);
    uint8_t h[3][16] = {{0}};
    /* Deliberately non-monotonic to expose ordering bugs: hash[1] is the
       SMALLEST in memcmp terms but inserted second. */
    h[0][0] = 0x10;
    h[1][0] = 0x05;
    h[2][0] = 0x20;

    btree_insert(path, "paid", 4, h[0]);
    btree_insert(path, "paid", 4, h[1]);
    btree_insert(path, "paid", 4, h[2]);

    g_count = 0;
    btree_range(path, "paid", 4, "paid", 4, count_cb, NULL);
    ASSERT_EQ_INT(g_count, 3, "3 paid entries before delete");

    btree_delete(path, "paid", 4, h[1]);
    g_count = 0;
    btree_range(path, "paid", 4, "paid", 4, count_cb, NULL);
    ASSERT_EQ_INT(g_count, 2, "2 paid entries after deleting middle-by-hash");

    btree_delete(path, "paid", 4, h[0]);
    g_count = 0;
    btree_range(path, "paid", 4, "paid", 4, count_cb, NULL);
    ASSERT_EQ_INT(g_count, 1, "1 paid entry after deleting another");

    btree_delete(path, "paid", 4, h[2]);
    g_count = 0;
    btree_range(path, "paid", 4, "paid", 4, count_cb, NULL);
    ASSERT_EQ_INT(g_count, 0, "0 paid entries after deleting last");

    unlink(path);
    return 0;
}

/* Mirrors test_slotcask_v2_bulk's upsert pattern in miniature: mixed
   values + delete one entry from a duplicate-value cluster + insert into a
   different cluster. Pre-BTRH the delete left a ghost. */
static int test_mixed_value_upsert(const char *path) {
    btree_cache_invalidate(path);
    unlink(path);
    uint8_t h[10][16] = {{0}};
    for (int i = 0; i < 10; i++) h[i][0] = (uint8_t)(i * 17 + 3);

    btree_insert(path, "paid", 4, h[0]);
    btree_insert(path, "paid", 4, h[1]);
    btree_insert(path, "pending", 7, h[2]);
    btree_insert(path, "paid", 4, h[3]);
    btree_insert(path, "refunded", 8, h[4]);
    btree_insert(path, "pending", 7, h[5]);

    btree_delete(path, "paid", 4, h[1]);
    btree_insert(path, "refunded", 8, h[1]);

    g_count = 0;
    btree_range(path, "paid", 4, "paid", 4, count_cb, NULL);
    ASSERT_EQ_INT(g_count, 2, "paid count after upsert");

    g_count = 0;
    btree_range(path, "pending", 7, "pending", 7, count_cb, NULL);
    ASSERT_EQ_INT(g_count, 2, "pending count");

    g_count = 0;
    btree_range(path, "refunded", 8, "refunded", 8, count_cb, NULL);
    ASSERT_EQ_INT(g_count, 2, "refunded count");

    unlink(path);
    return 0;
}

/* btree_bulk_merge must order ties by hash. Pre-BTRH the merge step
   sorted by value only, leaving hashes in input order; subsequent
   btree_delete failed. btree_bulk_merge runs the canonical sort path
   (cmp_btentry_fn in index.c + bt_cmp_entry in btree.c), so feeding it
   value-only-grouped input is the right way to exercise the fix. */
static int test_bulk_build_sort(const char *path) {
    btree_cache_invalidate(path);
    unlink(path);
    enum { N = 64 };
    BtEntry *entries = malloc(N * sizeof(BtEntry));
    for (int i = 0; i < N; i++) {
        entries[i].value = strdup(i < N/2 ? "alpha" : "beta");
        entries[i].vlen = (size_t)(i < N/2 ? 5 : 4);
        /* Hashes intentionally permuted (i * 31 mod 251) so input order
           does NOT match hash order — the merge sort must re-tie-break. */
        memset(entries[i].hash, 0, 16);
        entries[i].hash[0] = (uint8_t)((i * 31) % 251);
        entries[i].hash[1] = (uint8_t)i;
    }
    btree_bulk_merge(path, entries, N);

    /* All N entries reachable. */
    g_count = 0;
    btree_range(path, "alpha", 5, "alpha", 5, count_cb, NULL);
    ASSERT_EQ_INT(g_count, N/2, "bulk_build alpha cluster reachable");
    g_count = 0;
    btree_range(path, "beta", 4, "beta", 4, count_cb, NULL);
    ASSERT_EQ_INT(g_count, N/2, "bulk_build beta cluster reachable");

    /* Every entry deletable by (value, hash). Bug condition: pre-BTRH
       bulk_build left a duplicate-value cluster unsorted by hash, so
       btree_delete's bsearch missed half the entries. */
    int del_ok = 0;
    for (int i = 0; i < N; i++) {
        btree_delete(path, entries[i].value, entries[i].vlen, entries[i].hash);
    }
    g_count = 0;
    btree_range(path, "alpha", 5, "alpha", 5, count_cb, NULL);
    del_ok += (g_count == 0);
    g_count = 0;
    btree_range(path, "beta", 4, "beta", 4, count_cb, NULL);
    del_ok += (g_count == 0);
    ASSERT_EQ_INT(del_ok, 2, "every bulk_build entry deletable after re-sort");

    for (int i = 0; i < N; i++) free((char *)entries[i].value);
    free(entries);
    unlink(path);
    return 0;
}

/* Diagnosis: exercise the bulk merge adaptive branch's in-place insertion
   primitive with the same dense duplicate-value distribution as secondary
   indexes. This has no daemon, cache publication, or request concurrency. */
static int test_dense_duplicate_batch_insert(const char *path) {
    enum { N = 20000, BATCH = 64 };
    btree_cache_invalidate(path);
    unlink(path);
    BtEntry *entries = calloc(BATCH, sizeof(*entries));
    ASSERT_NOT_NULL(entries, "allocate dense insert batch");
    if (!entries) return 1;
    for (int base = 0; base < N; base += BATCH) {
        int n = N - base < BATCH ? N - base : BATCH;
        for (int i = 0; i < n; i++) {
            int id = base + i;
            entries[i].value = (id % 4 == 0) ? "paid" : (id % 4 == 1) ? "pending" :
                               (id % 4 == 2) ? "refunded" : "cancelled";
            entries[i].vlen = strlen(entries[i].value);
            memset(entries[i].hash, 0, BT_HASH_SIZE);
            uint32_t mixed = (uint32_t)id * 2654435761u;
            memcpy(entries[i].hash, &mixed, sizeof(mixed));
        }
        ASSERT_EQ_INT(btree_insert_batch(path, entries, (size_t)n), 0,
                      "dense duplicate batch insert succeeds");
    }
    g_count = 0;
    btree_range(path, "", 0, "\xff\xff\xff\xff", 4, count_cb, NULL);
    ASSERT_EQ_INT(g_count, N, "dense duplicate point inserts retain exactly N entries");
    free(entries);
    unlink(path);
    return 0;
}

/* Many duplicate-value entries to force a leaf split + internal-page
   (value, hash) separators. Then random-order deletes — every one must
   land. Pre-BTRH: split promoted only a value, so descent for
   (paid, H_X) routed to the wrong side of the tree about half the time. */
static int test_split_propagates_hash(const char *path) {
    btree_cache_invalidate(path);
    unlink(path);
    enum { N = 800 }; /* enough to force a multi-leaf tree */
    uint8_t hashes[N][16];
    for (int i = 0; i < N; i++) {
        memset(hashes[i], 0, 16);
        /* Spread across the full 16-byte range so leaves split cleanly. */
        hashes[i][0] = (uint8_t)((i * 73) & 0xFF);
        hashes[i][1] = (uint8_t)((i * 251) & 0xFF);
        hashes[i][2] = (uint8_t)(i & 0xFF);
        hashes[i][3] = (uint8_t)((i >> 8) & 0xFF);
        btree_insert(path, "dup", 3, hashes[i]);
    }

    g_count = 0;
    btree_range(path, "dup", 3, "dup", 3, count_cb, NULL);
    ASSERT_EQ_INT(g_count, N, "all duplicate-value entries reachable");

    /* Delete every other entry — must succeed without leaving ghosts. */
    int deleted = 0;
    for (int i = 0; i < N; i += 2) {
        btree_delete(path, "dup", 3, hashes[i]);
        deleted++;
    }
    g_count = 0;
    btree_range(path, "dup", 3, "dup", 3, count_cb, NULL);
    ASSERT_EQ_INT(g_count, N - deleted, "half deleted, half remain");

    /* The other half still deletes cleanly through the split tree. */
    for (int i = 1; i < N; i += 2) {
        btree_delete(path, "dup", 3, hashes[i]);
    }
    g_count = 0;
    btree_range(path, "dup", 3, "dup", 3, count_cb, NULL);
    ASSERT_EQ_INT(g_count, 0, "everything deleted across split leaves");

    unlink(path);
    return 0;
}

/* btree_bulk_merge merges a new sorted batch with an existing tree's
   on-disk order. Pre-BTRH the merge used val_cmp only; ties between
   existing and new entries with the same value preserved input order
   instead of hash order, breaking (value, hash) sort and silently
   failing later deletes. */
static int test_bulk_merge_value_hash(const char *path) {
    btree_cache_invalidate(path);
    unlink(path);
    BtEntry batch1[3];
    for (int i = 0; i < 3; i++) {
        batch1[i].value = strdup("k");
        batch1[i].vlen = 1;
        memset(batch1[i].hash, 0, 16);
        batch1[i].hash[0] = (uint8_t)(0x10 + i * 0x10); /* 10, 20, 30 */
    }
    btree_bulk_build(path, batch1, 3);

    BtEntry batch2[3];
    for (int i = 0; i < 3; i++) {
        batch2[i].value = strdup("k");
        batch2[i].vlen = 1;
        memset(batch2[i].hash, 0, 16);
        /* Interleaving hashes that sort between batch1's entries. */
        batch2[i].hash[0] = (uint8_t)(0x18 + i * 0x10); /* 18, 28, 38 */
    }
    btree_bulk_merge(path, batch2, 3);

    g_count = 0;
    btree_range(path, "k", 1, "k", 1, count_cb, NULL);
    ASSERT_EQ_INT(g_count, 6, "merge: 3 + 3 reachable");

    /* Every entry must still be deletable individually. */
    int ok = 1;
    for (int i = 0; i < 3; i++) {
        btree_delete(path, batch1[i].value, batch1[i].vlen, batch1[i].hash);
    }
    for (int i = 0; i < 3; i++) {
        btree_delete(path, batch2[i].value, batch2[i].vlen, batch2[i].hash);
    }
    g_count = 0;
    btree_range(path, "k", 1, "k", 1, count_cb, NULL);
    if (g_count != 0) ok = 0;
    ASSERT_EQ_INT(ok, 1, "merge: every entry deletable post-merge");

    for (int i = 0; i < 3; i++) { free((char *)batch1[i].value); free((char *)batch2[i].value); }
    unlink(path);
    return 0;
}

static int test_btree_value_hash_sort_run(void) {
    const char *path = "/tmp/shard-db-btree-vh-test.idx";
    test_dup_value_delete(path);
    test_mixed_value_upsert(path);
    test_bulk_build_sort(path);
    test_dense_duplicate_batch_insert(path);
    test_split_propagates_hash(path);
    test_bulk_merge_value_hash(path);
    bt_cache_shutdown();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-btree-value-hash-sort", test_btree_value_hash_sort_run)
