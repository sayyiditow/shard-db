/* src/db/simd.c — AVX2-accelerated string primitives, runtime-dispatched.
 *
 * Build does not require -march flag; AVX2 functions use
 * __attribute__((target("avx2"))) so the compiler emits AVX2 code only
 * for those functions while the rest of the binary stays SSE2-baseline.
 * simd_memmem() checks __builtin_cpu_supports("avx2") once per process
 * and routes accordingly.
 *
 * On x86_64 without AVX2 (rare on hardware made after 2013), all calls
 * fall through to glibc memmem.
 */

#define _GNU_SOURCE
#include "simd.h"
#include <string.h>

#ifdef __x86_64__
#include <immintrin.h>
#endif

/* Cached CPU capability check. Initialised on first call; safe to race
   (idempotent set, single-byte write). */
static int g_have_avx2_cached = -1;

static inline int have_avx2(void) {
    int v = __atomic_load_n(&g_have_avx2_cached, __ATOMIC_RELAXED);
    if (v < 0) {
#ifdef __x86_64__
        v = __builtin_cpu_supports("avx2") ? 1 : 0;
#else
        v = 0;
#endif
        __atomic_store_n(&g_have_avx2_cached, v, __ATOMIC_RELAXED);
    }
    return v;
}

#ifdef __x86_64__

/* AVX2 substring search using the "first byte + last byte" filter.
 * For each 32-byte block of haystack:
 *   - Load 32 bytes at i (block_first) and 32 bytes at i + nlen - 1 (block_last)
 *   - Compare each byte against needle[0] and needle[nlen-1] respectively
 *   - AND the masks: candidate positions are where BOTH match
 *   - For each candidate, verify the middle bytes via memcmp
 * Roughly 5-10x faster than glibc memmem for ASCII haystacks and 3-32B needles.
 *
 * The 1-byte and 2-byte needle paths are fast already in glibc (memchr-based);
 * we only take the AVX2 path for nlen >= 3 to keep the code simple.
 */
__attribute__((target("avx2")))
const void *simd_memmem_avx2(const void *haystack, size_t hlen,
                             const void *needle,   size_t nlen) {
    if (nlen == 0) return haystack;
    if (hlen < nlen) return NULL;
    if (nlen == 1) return memchr(haystack, *(const uint8_t *)needle, hlen);
    if (nlen == 2) return memmem(haystack, hlen, needle, nlen);
    /* For very long needles the per-candidate verify dominates; fall through. */
    if (nlen > 256) return memmem(haystack, hlen, needle, nlen);

    const uint8_t *hay = (const uint8_t *)haystack;
    const uint8_t *ned = (const uint8_t *)needle;

    const __m256i first = _mm256_set1_epi8((char)ned[0]);
    const __m256i last  = _mm256_set1_epi8((char)ned[nlen - 1]);

    /* We need 32 bytes for block_first + (nlen-1) more bytes to read
       block_last at i + nlen - 1. So loop while i + nlen + 31 <= hlen. */
    size_t i = 0;
    while (i + nlen + 31 < hlen + 1) {
        __m256i block_first = _mm256_loadu_si256((const __m256i *)(hay + i));
        __m256i block_last  = _mm256_loadu_si256((const __m256i *)(hay + i + nlen - 1));

        __m256i eq_first = _mm256_cmpeq_epi8(first, block_first);
        __m256i eq_last  = _mm256_cmpeq_epi8(last,  block_last);
        uint32_t mask    = (uint32_t)_mm256_movemask_epi8(_mm256_and_si256(eq_first, eq_last));

        while (mask) {
            int bit = __builtin_ctz(mask);
            /* Verify the middle (bytes 1..nlen-2). For nlen==2 this loop is
               skipped above; for nlen>=3 we always have at least 1 middle byte. */
            if (nlen <= 2 ||
                memcmp(hay + i + bit + 1, ned + 1, nlen - 2) == 0) {
                return hay + i + bit;
            }
            mask &= mask - 1;
        }
        i += 32;
    }

    /* Tail: less than 32 bytes left to scan. Hand off to glibc, which is
       fine for the small remainder. */
    if (i < hlen) {
        return memmem(hay + i, hlen - i, ned, nlen);
    }
    return NULL;
}

#else  /* !__x86_64__ */

const void *simd_memmem_avx2(const void *haystack, size_t hlen,
                             const void *needle,   size_t nlen) {
    return memmem(haystack, hlen, needle, nlen);
}

#endif

const void *simd_memmem(const void *haystack, size_t hlen,
                        const void *needle,   size_t nlen) {
    /* glibc memmem returns haystack on empty needle; macOS libc returns
       NULL. Normalise so callers (and tests) see one behaviour. */
    if (nlen == 0) return haystack;
    if (have_avx2()) return simd_memmem_avx2(haystack, hlen, needle, nlen);
    return memmem(haystack, hlen, needle, nlen);
}

/* Naive ASCII case-insensitive memmem — fallback for non-AVX2 CPUs and
   AVX2 tail. Lowercases haystack bytes inline; needle is already lc. */
static const void *memcasemem_naive(const void *haystack, size_t hlen,
                                     const void *needle_lc, size_t nlen) {
    if (nlen == 0) return haystack;
    if (hlen < nlen) return NULL;
    const uint8_t *h = (const uint8_t *)haystack;
    const uint8_t *n = (const uint8_t *)needle_lc;
    for (size_t i = 0; i + nlen <= hlen; i++) {
        size_t j = 0;
        for (; j < nlen; j++) {
            uint8_t c = h[i + j];
            if (c >= 'A' && c <= 'Z') c = (uint8_t)(c + 32);
            if (c != n[j]) break;
        }
        if (j == nlen) return h + i;
    }
    return NULL;
}

#ifdef __x86_64__
/* AVX2 case-insensitive substring search.
 *
 * Vectorised ASCII tolower per loaded block:
 *   is_upper = (v >= 'A') & (v <= 'Z')   // signed compare, ASCII range
 *   v_lc     = v + (is_upper & 0x20)
 *
 * Then same first-byte+last-byte filter as simd_memmem_avx2, but compare
 * the lowercased haystack bytes against the (already-lowercased) needle.
 *
 * Verify-on-candidate is scalar — only fires for first+last-byte coincidence
 * (rare for random text), so per-record cost is dominated by the vectorised
 * filter. ~5-10× faster than naive memcasemem on ASCII haystacks.
 */
__attribute__((target("avx2")))
static inline __m256i avx2_ascii_tolower(__m256i v) {
    /* Signed compare works because A=0x41 .. Z=0x5A are positive small;
       non-ASCII (high bit set) reads as negative and never falls in [A,Z]. */
    __m256i ge_A = _mm256_cmpgt_epi8(v, _mm256_set1_epi8('A' - 1));
    __m256i le_Z = _mm256_cmpgt_epi8(_mm256_set1_epi8('Z' + 1), v);
    __m256i is_upper = _mm256_and_si256(ge_A, le_Z);
    __m256i delta = _mm256_and_si256(is_upper, _mm256_set1_epi8(0x20));
    return _mm256_add_epi8(v, delta);
}

__attribute__((target("avx2")))
const void *simd_memcasemem_avx2(const void *haystack, size_t hlen,
                                  const void *needle_lc, size_t nlen) {
    if (nlen == 0) return haystack;
    if (hlen < nlen) return NULL;
    if (nlen == 1) {
        /* Search for either case of the single character. */
        const uint8_t *h = (const uint8_t *)haystack;
        uint8_t lc = ((const uint8_t *)needle_lc)[0];
        uint8_t uc = (lc >= 'a' && lc <= 'z') ? (uint8_t)(lc - 32) : lc;
        for (size_t i = 0; i < hlen; i++) {
            if (h[i] == lc || h[i] == uc) return h + i;
        }
        return NULL;
    }
    if (nlen > 256) return memcasemem_naive(haystack, hlen, needle_lc, nlen);

    const uint8_t *hay = (const uint8_t *)haystack;
    const uint8_t *ned = (const uint8_t *)needle_lc;
    const __m256i first_lc = _mm256_set1_epi8((char)ned[0]);
    const __m256i last_lc  = _mm256_set1_epi8((char)ned[nlen - 1]);

    size_t i = 0;
    while (i + nlen + 31 < hlen + 1) {
        __m256i b1 = _mm256_loadu_si256((const __m256i *)(hay + i));
        __m256i b2 = _mm256_loadu_si256((const __m256i *)(hay + i + nlen - 1));
        __m256i b1_lc = avx2_ascii_tolower(b1);
        __m256i b2_lc = avx2_ascii_tolower(b2);

        __m256i eq_first = _mm256_cmpeq_epi8(first_lc, b1_lc);
        __m256i eq_last  = _mm256_cmpeq_epi8(last_lc,  b2_lc);
        uint32_t mask    = (uint32_t)_mm256_movemask_epi8(_mm256_and_si256(eq_first, eq_last));

        while (mask) {
            int bit = __builtin_ctz(mask);
            /* Verify middle bytes case-insensitively. Scalar loop —
               only runs on first+last filter survivors (rare). */
            int matches = 1;
            for (size_t j = 1; j + 1 < nlen; j++) {
                uint8_t c = hay[i + bit + j];
                if (c >= 'A' && c <= 'Z') c = (uint8_t)(c + 32);
                if (c != ned[j]) { matches = 0; break; }
            }
            if (matches) return hay + i + bit;
            mask &= mask - 1;
        }
        i += 32;
    }

    if (i < hlen) {
        return memcasemem_naive(hay + i, hlen - i, ned, nlen);
    }
    return NULL;
}

#else  /* !__x86_64__ */

const void *simd_memcasemem_avx2(const void *haystack, size_t hlen,
                                  const void *needle_lc, size_t nlen) {
    return memcasemem_naive(haystack, hlen, needle_lc, nlen);
}

#endif

const void *simd_memcasemem(const void *haystack, size_t hlen,
                            const void *needle_lc, size_t nlen) {
    if (have_avx2()) return simd_memcasemem_avx2(haystack, hlen, needle_lc, nlen);
    return memcasemem_naive(haystack, hlen, needle_lc, nlen);
}
