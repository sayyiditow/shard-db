/* src/test/test_assert.h
 *
 * TAP-style assertion macros. Each ASSERT_* increments a per-test
 * counter and prints "ok N - desc" or "not ok N - desc \n# expected …".
 * The runner aggregates pass/fail counts across all tests in a suite.
 *
 * Designed for E2E shape: each test is a function returning int (0 = ok,
 * non-zero = fail). The macros reference a thread-local TestCtx that the
 * runner sets before invoking the test function.
 */
#ifndef TEST_ASSERT_H
#define TEST_ASSERT_H
#include <stdio.h>
#include <string.h>

typedef struct {
    int test_num;       /* 1-based counter within current case */
    int passed;
    int failed;
    const char *name;
} TestCtx;

extern __thread TestCtx *t_ctx;

#define _TAP_OK(desc)   do { t_ctx->test_num++; t_ctx->passed++; \
    printf("ok %d - %s\n", t_ctx->test_num, (desc)); } while (0)

#define _TAP_FAIL(desc, fmt, ...) do { t_ctx->test_num++; t_ctx->failed++; \
    printf("not ok %d - %s\n#   " fmt "\n", t_ctx->test_num, (desc), ##__VA_ARGS__); } while (0)

#define ASSERT_TRUE(cond, desc) \
    do { if (cond) _TAP_OK(desc); else _TAP_FAIL(desc, "assertion failed: %s", #cond); } while (0)

#define ASSERT_EQ_INT(actual, expected, desc) \
    do { long long _a = (long long)(actual), _e = (long long)(expected); \
         if (_a == _e) _TAP_OK(desc); \
         else _TAP_FAIL(desc, "expected %lld got %lld", _e, _a); } while (0)

#define ASSERT_EQ_STR(actual, expected, desc) \
    do { const char *_a = (actual), *_e = (expected); \
         if (_a && _e && strcmp(_a, _e) == 0) _TAP_OK(desc); \
         else _TAP_FAIL(desc, "expected '%s' got '%s'", _e ? _e : "(null)", _a ? _a : "(null)"); } while (0)

#define ASSERT_CONTAINS(haystack, needle, desc) \
    do { const char *_h = (haystack), *_n = (needle); \
         if (_h && _n && strstr(_h, _n)) _TAP_OK(desc); \
         else _TAP_FAIL(desc, "'%s' not found in '%s'", _n ? _n : "(null)", _h ? _h : "(null)"); } while (0)

#define ASSERT_NOT_NULL(ptr, desc) \
    do { if (ptr) _TAP_OK(desc); else _TAP_FAIL(desc, "got NULL"); } while (0)

#endif
