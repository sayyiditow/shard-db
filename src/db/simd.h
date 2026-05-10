/* src/db/simd.h
 *
 * SIMD-accelerated string primitives. Each function checks CPU support at
 * runtime (__builtin_cpu_supports) and dispatches to the AVX2 implementation
 * when available, falling back to libc otherwise. Build does not require
 * -march=native; AVX2 paths are guarded by __attribute__((target("avx2"))).
 *
 * Used in scan-path hotspots:
 *   - simd_memmem: bio substring search (CONTAINS, LIKE %x%, regex anchors)
 *   - simd_memcmp_eq: 16/32-byte equality checks (eq_field varchar)
 */
#ifndef SHARD_DB_SIMD_H
#define SHARD_DB_SIMD_H

#include <stddef.h>
#include <stdint.h>

/* Substring search. Returns pointer to first occurrence of needle within
   haystack, or NULL. Same contract as glibc memmem(). AVX2 path uses the
   "first byte + last byte" filter (cmpeq + movemask + verify) which beats
   glibc's two-way search by 3-5x on ASCII haystacks for needles 3-32B long.
   For nlen <= 1 or hlen < nlen the fallback is taken (no AVX2 startup cost). */
const void *simd_memmem(const void *haystack, size_t hlen,
                        const void *needle,   size_t nlen);

/* Optional explicit AVX2 dispatch — exposed so call-sites that already know
   AVX2 is hot can skip the per-call cpu-supports check. Most callers should
   use simd_memmem(). */
const void *simd_memmem_avx2(const void *haystack, size_t hlen,
                             const void *needle,   size_t nlen);

/* Case-insensitive substring search for ASCII (A-Z folded to a-z). needle_lc
   must already be lowercased by the caller (compile_one does this for
   ICONTAINS/ILIKE). Returns pointer to first match or NULL. AVX2 path uses
   the same first/last-byte filter as simd_memmem but lowercases each loaded
   byte via vectorised ASCII fold. ~5-10× faster than the naive byte loop
   for ASCII-heavy haystacks. */
const void *simd_memcasemem(const void *haystack, size_t hlen,
                            const void *needle_lc, size_t nlen);

const void *simd_memcasemem_avx2(const void *haystack, size_t hlen,
                                 const void *needle_lc, size_t nlen);

#endif
