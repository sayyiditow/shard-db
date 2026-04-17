#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "btree.h"

static int result_count = 0;
static void count_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    result_count++;
}
static void print_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    printf("  value='%.*s' hash=%02x%02x...\n", (int)vl, v, h[0], h[1]);
    result_count++;
}

int main() {
    const char *path = "/tmp/btree_test.idx";
    unlink(path);

    uint8_t h1[16] = {1}; uint8_t h2[16] = {2}; uint8_t h3[16] = {3};
    uint8_t h4[16] = {4}; uint8_t h5[16] = {5};

    /* Test 1: Basic insert + search */
    printf("=== insert 5 records ===\n");
    btree_insert(path, "Charlie", 7, h3);
    btree_insert(path, "Alice", 5, h1);
    btree_insert(path, "Eve", 3, h5);
    btree_insert(path, "Bob", 3, h2);
    btree_insert(path, "Dave", 4, h4);

    printf("=== search Bob ===\n");
    result_count = 0;
    btree_search(path, "Bob", 3, print_cb, NULL);
    printf("Found: %d (expected 1)\n", result_count);

    /* Test 2: Range */
    printf("=== range Bob..Dave ===\n");
    result_count = 0;
    btree_range(path, "Bob", 3, "Dave", 4, print_cb, NULL);
    printf("Found: %d (expected 3: Bob, Charlie, Dave)\n", result_count);

    /* Test 3: Delete */
    printf("=== delete Charlie ===\n");
    btree_delete(path, "Charlie", 7, h3);
    result_count = 0;
    btree_search(path, "Charlie", 7, count_cb, NULL);
    printf("After delete: %d (expected 0)\n", result_count);

    /* Test 4: Duplicate insert */
    printf("=== duplicate insert ===\n");
    btree_insert(path, "Alice", 5, h1);
    result_count = 0;
    btree_search(path, "Alice", 5, count_cb, NULL);
    printf("After dup: %d (expected 1)\n", result_count);

    /* Test 5: Bulk insert (force page splits) */
    printf("=== bulk: 10000 records ===\n");
    unlink(path);
    for (int i = 0; i < 10000; i++) {
        char val[32];
        int vlen = snprintf(val, sizeof(val), "key_%05d", i);
        uint8_t hash[16];
        memset(hash, 0, 16);
        memcpy(hash, &i, sizeof(int));
        btree_insert(path, val, vlen, hash);
    }
    result_count = 0;
    btree_search(path, "key_05000", 9, count_cb, NULL);
    printf("Search key_05000: %d (expected 1)\n", result_count);

    result_count = 0;
    btree_range(path, "key_01000", 9, "key_01010", 9, count_cb, NULL);
    printf("Range key_01000..key_01010: %d (expected 11)\n", result_count);

    /* Test 6: Bulk build */
    printf("=== bulk build 10000 ===\n");
    unlink(path);
    BtEntry *entries = malloc(10000 * sizeof(BtEntry));
    for (int i = 0; i < 10000; i++) {
        char *val = malloc(32);
        int vlen = snprintf(val, 32, "key_%05d", i);
        entries[i].value = val;
        entries[i].vlen = vlen;
        memset(entries[i].hash, 0, 16);
        memcpy(entries[i].hash, &i, sizeof(int));
    }
    btree_bulk_build(path, entries, 10000);

    result_count = 0;
    btree_search(path, "key_05000", 9, count_cb, NULL);
    printf("Bulk search key_05000: %d (expected 1)\n", result_count);

    result_count = 0;
    btree_range(path, "key_09990", 9, "key_09999", 9, count_cb, NULL);
    printf("Bulk range 09990..09999: %d (expected 10)\n", result_count);

    for (int i = 0; i < 10000; i++) free((char *)entries[i].value);
    free(entries);

    unlink(path);
    printf("=== ALL DONE ===\n");
    return 0;
}
