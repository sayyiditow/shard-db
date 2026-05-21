#ifndef SHARD_DB_TRIGRAM_H
#define SHARD_DB_TRIGRAM_H

#include <stddef.h>
#include <stdint.h>

/* Trigram index helpers. Implementation notes in trigram.c.
 *
 * Storage shape: `.tg` files use the same BTRH btree format as `.idx`,
 * just at a different path so a field can carry both btree and trigram
 * indexes simultaneously (bt_cache is path-keyed). Each leaf entry is
 * (3-byte lowercase trigram, 16-byte record xxh128). The posting list
 * for a trigram is the range of leaf entries sharing that 3-byte key.
 *
 * See [[trigram-impl-map]] for the design.
 */

/* Per-record cap on distinct trigrams. Long varchars saturate around
 * ~3000 distinct trigrams (most 3-char windows have already appeared);
 * 4096 leaves headroom without runaway memory. Hit and we stop — every
 * trigram beyond the cap is silently dropped from the index. */
#define TG_MAX_DISTINCT 4096

/* Lowercase a UTF-8/ASCII varchar and emit every distinct 3-char window
 * into `out[0..n)`. `out` must hold at least `out_cap` slots of 3 bytes
 * each. Returns the count of distinct trigrams written (capped at
 * out_cap; <3-byte inputs return 0).
 *
 * Folding is ASCII-only: 'A'..'Z' → 'a'..'z'. Non-ASCII bytes pass
 * through unchanged — sufficient for English-leaning corpora and
 * fastpath-cheap. A future i18n pass can extend this without changing
 * the on-disk format (everything stays bytewise).
 *
 * Used at insert/delete time (CRUD hook in update_idx_fn) and at query
 * time (extract trigrams from the search pattern). */
size_t tg_extract_distinct(const uint8_t *value, size_t vlen,
                           uint8_t (*out)[3], size_t out_cap);

/* Build the on-disk path to a trigram shard. Mirrors bm_build_path.
 *
 *   <db_root>/<object>/indexes/<field>/<NNN>.tg
 *
 * `out` must hold at least PATH_MAX bytes. `shard_idx` is 0..MAX_SPLITS-1,
 * formatted as 3 hex digits to match every other on-disk shard name
 * (kf/seg/idx/bm). */
void tg_build_path(char *out, size_t outlen,
                   const char *db_root, const char *object,
                   const char *field, int shard_idx);

#endif
