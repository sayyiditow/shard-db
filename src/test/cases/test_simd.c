#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "simd.h"
#include <string.h>

static int test_simd_run(void) {
    const char *haystack = "the quick brown fox jumps over the lazy dog";

    ASSERT_NOT_NULL(simd_memmem(haystack, strlen(haystack), "quick", 5), "memmem quick");
    ASSERT_NOT_NULL(simd_memmem(haystack, strlen(haystack), "dog", 3), "memmem dog");
    ASSERT_NOT_NULL(simd_memmem(haystack, strlen(haystack), "the", 3), "memmem the first");
    ASSERT_TRUE(simd_memmem(haystack, 3, "the", 3) == haystack, "memmem exact prefix");
    ASSERT_TRUE(simd_memmem(haystack, strlen(haystack), "xyz", 3) == NULL, "memmem absent");
    ASSERT_TRUE(simd_memmem(haystack, strlen(haystack), "", 0) == haystack, "memmem empty needle");
    ASSERT_TRUE(simd_memmem(haystack, 5, "quick", 5) == NULL, "memmem hlen < nlen");

    const char *single = "abc";
    ASSERT_NOT_NULL(simd_memmem(single, 3, "b", 1), "memmem nlen=1");
    ASSERT_TRUE(simd_memmem(single, 3, "d", 1) == NULL, "memmem nlen=1 miss");
    ASSERT_NOT_NULL(simd_memmem(single, 3, "bc", 2), "memmem nlen=2");
    ASSERT_TRUE(simd_memmem(single, 3, "bd", 2) == NULL, "memmem nlen=2 miss");

    const char *tail_hay = "aaaaab";
    ASSERT_TRUE(simd_memmem(tail_hay, strlen(tail_hay), "ab", 2) == tail_hay + 4, "memmem tail");

    const char *case_hay = "Hello World";
    ASSERT_NOT_NULL(simd_memcasemem(case_hay, strlen(case_hay), "hello", 5), "memcasemem hello");
    ASSERT_NOT_NULL(simd_memcasemem(case_hay, strlen(case_hay), "world", 5), "memcasemem world");
    ASSERT_TRUE(simd_memcasemem(case_hay, strlen(case_hay), "xyz", 3) == NULL, "memcasemem absent");
    ASSERT_TRUE(simd_memcasemem(case_hay, strlen(case_hay), "", 0) == case_hay, "memcasemem empty needle");
    ASSERT_TRUE(simd_memcasemem(case_hay, 4, "hello", 5) == NULL, "memcasemem hlen < nlen");

    ASSERT_NOT_NULL(simd_memcasemem("AbC", 3, "abc", 3), "memcasemem AbC/abc");
    ASSERT_NOT_NULL(simd_memcasemem("ABC", 3, "abc", 3), "memcasemem ABC/abc");
    ASSERT_NOT_NULL(simd_memcasemem("abc", 3, "abc", 3), "memcasemem abc/abc");

    ASSERT_NOT_NULL(simd_memcasemem("a", 1, "a", 1), "memcasemem single char");
    ASSERT_NOT_NULL(simd_memcasemem("A", 1, "a", 1), "memcasemem A matches a");
    ASSERT_TRUE(simd_memcasemem("b", 1, "a", 1) == NULL, "memcasemem single char miss");

    ASSERT_NOT_NULL(simd_memmem_avx2(haystack, strlen(haystack), "quick", 5), "avx2 quick");
    ASSERT_TRUE(simd_memmem_avx2(haystack, strlen(haystack), "xyz", 3) == NULL, "avx2 absent");
    ASSERT_NOT_NULL(simd_memcasemem_avx2(case_hay, strlen(case_hay), "hello", 5), "avx2 ci hello");
    ASSERT_TRUE(simd_memcasemem_avx2(case_hay, strlen(case_hay), "xyz", 3) == NULL, "avx2 ci absent");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-simd", test_simd_run)
