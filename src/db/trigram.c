/* Trigram helpers — see trigram.h for the API contract.
 *
 * Two functions:
 *   tg_extract_distinct — lowercase + sliding-window + dedup
 *   tg_build_path       — <db_root>/<object>/indexes/<field>/NNN.tg
 *
 * Dedup uses a 24-bit packed key (b0<<16 | b1<<8 | b2) in a small
 * stack hash table. Worst case the table is 8192 slots × 4 bytes = 32K,
 * well within the 8MB thread stack `db_thread_create` sets up.
 */

#include "trigram.h"

#include <stdio.h>
#include <limits.h>   /* PATH_MAX — portable, works on both Linux and macOS */

/* Fibonacci hashing constant — spreads 24-bit keys across power-of-two
 * tables evenly. 0x9E3779B9 = floor(2^32 / phi). */
#define TG_HASH_GOLDEN 0x9E3779B9u

/* Power-of-two table slots; sized for TG_MAX_DISTINCT (4096) at ~50%
 * load. Keep the mask trick (`& (slots-1)`) — replacing with `%` slows
 * the inner loop. */
#define TG_DEDUP_SLOTS 8192
#define TG_DEDUP_MASK  (TG_DEDUP_SLOTS - 1)
#define TG_EMPTY       0xFFFFFFFFu  /* sentinel; valid keys are <= 0xFFFFFF */

static inline uint8_t tg_tolower(uint8_t c) {
    return (c >= 'A' && c <= 'Z') ? (uint8_t)(c + 32) : c;
}

size_t tg_extract_distinct(const uint8_t *value, size_t vlen,
                           uint8_t (*out)[3], size_t out_cap) {
    if (vlen < 3 || out_cap == 0) return 0;

    uint32_t seen[TG_DEDUP_SLOTS];
    for (size_t i = 0; i < TG_DEDUP_SLOTS; i++) seen[i] = TG_EMPTY;

    size_t written = 0;
    for (size_t i = 0; i + 2 < vlen; i++) {
        uint8_t b0 = tg_tolower(value[i]);
        uint8_t b1 = tg_tolower(value[i + 1]);
        uint8_t b2 = tg_tolower(value[i + 2]);
        uint32_t key = ((uint32_t)b0 << 16) | ((uint32_t)b1 << 8) | b2;

        size_t h = (key * TG_HASH_GOLDEN) & TG_DEDUP_MASK;
        while (seen[h] != TG_EMPTY && seen[h] != key) {
            h = (h + 1) & TG_DEDUP_MASK;
        }
        if (seen[h] == key) continue;  /* already emitted for this record */
        seen[h] = key;

        out[written][0] = b0;
        out[written][1] = b1;
        out[written][2] = b2;
        written++;
        if (written >= out_cap) break;
    }
    return written;
}

void tg_build_path(char *out, size_t outlen,
                   const char *db_root, const char *object,
                   const char *field, int shard_idx) {
    snprintf(out, outlen, "%s/%s/indexes/%s/%03x.tg",
             db_root, object, field, shard_idx);
}
