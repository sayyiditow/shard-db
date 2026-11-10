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

/* Hash the 16-byte xxh128 to a bucket index. The hash is already high-entropy,
   so we just splice the low + high halves together and mask. */
static inline uint64_t bucket_hash(const uint8_t h[16]) {
    uint64_t lo, hi;
    memcpy(&lo, h, 8);
    memcpy(&hi, h + 8, 8);
    return lo ^ hi;
}

/* Capacity for a hint: minimum eight, next power of two of twice the hint,
 * with checked doubling. 0 or -1/EOVERFLOW. */
static int keyset_capacity_for(size_t hint, size_t *out_cap) {
    if (hint > SIZE_MAX / 2) { errno = EOVERFLOW; return -1; }
    size_t want = hint * 2;
    size_t cap = 8;
    while (cap < want) {
        if (cap > SIZE_MAX / 2) { errno = EOVERFLOW; return -1; }
        cap *= 2;
    }
    *out_cap = cap;
    return 0;
}

/* Slot capacity for a hint (shared with keyset_new's allocator rule):
 * minimum eight, next power of two of twice the hint. 0 or -1/EOVERFLOW.
 * Exposed so callers can compare preferred/ordinary capacity classes
 * without a second formula. */
int keyset_capacity_hint_slots(size_t hint, size_t *out_slots) {
    return keyset_capacity_for(hint, out_slots);
}

/* Exact allocation charge of the KeySet keyset_new(hint) builds: the object
 * plus its keys and state arrays at the shared capacity. 0 or
 * -1/EOVERFLOW. Budget checks must use this, never a second formula. */
int keyset_allocation_bytes(size_t hint, size_t *out) {
    size_t cap;
    if (keyset_capacity_for(hint, &cap) != 0) return -1;
    size_t per = sizeof(uint8_t[16]) + sizeof(uint32_t);
    if (cap > (SIZE_MAX - sizeof(KeySet)) / per) { errno = EOVERFLOW; return -1; }
    *out = sizeof(KeySet) + cap * per;
    return 0;
}

#ifdef TEST_BUILD
/* Thread-local one-shot allocation fault for the budgeted constructor:
 * exercises reservation rollback without global malloc interception. */
static __thread int keyset_test_fail_alloc;
void keyset_test_fail_budgeted_alloc_next(int on) { keyset_test_fail_alloc = on; }
#endif

KeySet *keyset_new(size_t capacity_hint) {
    size_t cap;
    if (keyset_capacity_for(capacity_hint, &cap) != 0) {
        LOG_ERROR(LOG_SUB_PLANNER, "keyset_new: capacity overflow (capacity_hint=%zu)", capacity_hint);
        return NULL;
    }
    KeySet *ks = calloc(1, sizeof(KeySet));
    if (!ks) {
        LOG_ERROR(LOG_SUB_PLANNER, "keyset_new: calloc failed for KeySet struct (capacity_hint=%zu)", capacity_hint);
        return NULL;
    }
    ks->cap = cap;
    ks->mask = cap - 1;
    ks->keys  = calloc(cap, sizeof(*ks->keys));
    ks->state = calloc(cap, sizeof(*ks->state));
    atomic_init(&ks->n, 0);
    if (!ks->keys || !ks->state) {
        LOG_ERROR(LOG_SUB_PLANNER, "keyset_new: calloc failed for keys/state arrays (cap=%zu)", cap);
        free(ks->keys); free(ks->state); free(ks);
        errno = ENOMEM;
        return NULL;
    }
    return ks;
}

/* ─── Request-owned budget (prepared bitmap find sources) ───
 * keyset_budget_reserve atomically charges `bytes` against the request
 * budget (0, or -1/ENOBUFS without charging); keyset_budget_release returns
 * it. A KeySet built by keyset_new_budgeted carries its budget and exact
 * charge so keyset_free releases it exactly once; moving the set between
 * owners never re-charges. The budget must outlive every set charged to it
 * and every worker that can still reserve from it. */
int keyset_budget_reserve(KeySetBudget *budget, size_t bytes) {
    if (!budget || bytes == 0) return 0;
    size_t used = atomic_load_explicit(&budget->used, memory_order_acquire);
    for (;;) {
        if (bytes > budget->limit || used > budget->limit - bytes) {
            errno = ENOBUFS;
            return -1;
        }
        if (atomic_compare_exchange_weak_explicit(&budget->used, &used,
                                                  used + bytes,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire))
            break;
    }
    size_t now = used + bytes;
    size_t peak = atomic_load_explicit(&budget->peak, memory_order_acquire);
    while (peak < now &&
           !atomic_compare_exchange_weak_explicit(&budget->peak, &peak, now,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire)) {
    }
    return 0;
}

void keyset_budget_release(KeySetBudget *budget, size_t bytes) {
    if (!budget || bytes == 0) return;
    atomic_fetch_sub_explicit(&budget->used, bytes, memory_order_acq_rel);
}

KeySet *keyset_new_budgeted(size_t capacity_hint, KeySetBudget *budget) {
    if (!budget) return keyset_new(capacity_hint);
    size_t charge = 0;
    if (keyset_allocation_bytes(capacity_hint, &charge) != 0) {
        errno = ENOBUFS;
        return NULL;
    }
    if (keyset_budget_reserve(budget, charge) != 0) return NULL;
    KeySet *ks = NULL;
#ifdef TEST_BUILD
    if (keyset_test_fail_alloc) {
        keyset_test_fail_alloc = 0;
        errno = ENOMEM;
    } else
#endif
    ks = keyset_new(capacity_hint);
    if (!ks) {
        int saved = errno ? errno : ENOMEM;
        keyset_budget_release(budget, charge);
        errno = saved;
        return NULL;
    }
    ks->budget = budget;
    ks->budget_charge = charge;
    return ks;
}

void keyset_free(KeySet *ks) {
    if (!ks) return;
    keyset_budget_release(ks->budget, ks->budget_charge);
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
    LOG_WARN(LOG_SUB_PLANNER, "keyset_insert: hash table full (cap=%zu), falling back to full scan", ks->cap);
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
