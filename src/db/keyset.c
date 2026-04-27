#include "types.h"
#include <stdatomic.h>

/* Open-addressed hash table of 16-byte xxh128 record-key hashes.

   Concurrency model:
   - Inserts are lock-free via CAS on the per-bucket `state` word.
   - Capacity is fixed at construction; callers size up-front from the sum of
     estimated B+ tree range sizes (x2 to keep load factor ≤ 0.5).
   - No resize. If the set fills, keyset_insert returns -1 and the caller
     falls back to a full scan. This is a planner-time decision.

   Bucket state machine:
     0 (empty)   -> 1 (filling)   via CAS
     1 (filling) -> 2 (filled)    via plain store after memcpy(keys[b], hash)
     2 (filled)                   terminal

   Readers (keyset_contains, keyset_iter) treat `filling` as "skip this bucket
   for now" — they won't miss inserts because those either come from the same
   thread (serial observer) or complete before the join barrier that precedes
   a read. */

static size_t next_pow2(size_t x) {
    size_t p = 1;
    while (p < x) p <<= 1;
    return p < 8 ? 8 : p;
}

/* Hash the 16-byte xxh128 to a bucket index. The hash is already high-entropy,
   so we just splice the low + high halves together and mask. */
static inline uint64_t bucket_hash(const uint8_t h[16]) {
    uint64_t lo, hi;
    memcpy(&lo, h, 8);
    memcpy(&hi, h + 8, 8);
    return lo ^ hi;
}

KeySet *keyset_new(size_t capacity_hint) {
    size_t cap = next_pow2(capacity_hint * 2);
    KeySet *ks = calloc(1, sizeof(KeySet));
    if (!ks) return NULL;
    ks->cap = cap;
    ks->mask = cap - 1;
    ks->keys  = calloc(cap, sizeof(*ks->keys));
    ks->state = calloc(cap, sizeof(*ks->state));
    atomic_init(&ks->n, 0);
    if (!ks->keys || !ks->state) {
        free(ks->keys); free(ks->state); free(ks);
        return NULL;
    }
    return ks;
}

void keyset_free(KeySet *ks) {
    if (!ks) return;
    free(ks->keys);
    free(ks->state);
    free(ks);
}

int keyset_insert(KeySet *ks, const uint8_t hash[16]) {
    if (!ks) return -1;
    size_t start = bucket_hash(hash) & ks->mask;
    for (size_t probe = 0; probe < ks->cap; probe++) {
        size_t b = (start + probe) & ks->mask;
        uint32_t s = atomic_load_explicit((_Atomic uint32_t *)&ks->state[b],
                                          memory_order_acquire);
        if (s == 0) {
            uint32_t expect = 0;
            if (atomic_compare_exchange_strong_explicit(
                    (_Atomic uint32_t *)&ks->state[b], &expect, 1,
                    memory_order_acq_rel, memory_order_acquire)) {
                memcpy(ks->keys[b], hash, 16);
                atomic_store_explicit((_Atomic uint32_t *)&ks->state[b], 2,
                                      memory_order_release);
                atomic_fetch_add_explicit(&ks->n, 1, memory_order_relaxed);
                return 1;
            }
            /* CAS lost — another thread took this slot. Re-read state. */
            s = atomic_load_explicit((_Atomic uint32_t *)&ks->state[b],
                                     memory_order_acquire);
        }
        if (s == 1) {
            /* Another thread filling this slot. Wait briefly for filled state
               so we can compare keys; cheaper than probing past. */
            while (atomic_load_explicit((_Atomic uint32_t *)&ks->state[b],
                                        memory_order_acquire) == 1) { /* spin */ }
            s = 2;
        }
        if (s == 2 && memcmp(ks->keys[b], hash, 16) == 0) {
            return 0; /* duplicate */
        }
        /* Collision — linear probe. */
    }
    return -1; /* full */
}

int keyset_contains(const KeySet *ks, const uint8_t hash[16]) {
    if (!ks) return 0;
    size_t start = bucket_hash(hash) & ks->mask;
    for (size_t probe = 0; probe < ks->cap; probe++) {
        size_t b = (start + probe) & ks->mask;
        uint32_t s = atomic_load_explicit((_Atomic uint32_t *)&ks->state[b],
                                          memory_order_acquire);
        if (s == 0) return 0;
        if (s == 2 && memcmp(ks->keys[b], hash, 16) == 0) return 1;
    }
    return 0;
}

size_t keyset_size(const KeySet *ks) {
    if (!ks) return 0;
    return atomic_load_explicit(&ks->n, memory_order_acquire);
}

void keyset_iter(const KeySet *ks,
                 int (*cb)(const uint8_t hash[16], void *ctx),
                 void *ctx) {
    if (!ks || !cb) return;
    for (size_t b = 0; b < ks->cap; b++) {
        uint32_t s = atomic_load_explicit((_Atomic uint32_t *)&ks->state[b],
                                          memory_order_acquire);
        if (s == 2) {
            if (cb(ks->keys[b], ctx) != 0) return;
        }
    }
}
