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
    FILE *out;           /* NULL = write to stdout (the normal path).
                            Kept as an optional override for focused
                            in-process test helpers. */
} TestCtx;

extern __thread TestCtx *t_ctx;

#define _TAP_OUT (t_ctx->out ? t_ctx->out : stdout)

#define _TAP_OK(desc)   do { t_ctx->test_num++; t_ctx->passed++; \
    fprintf(_TAP_OUT, "ok %d - %s\n", t_ctx->test_num, (desc)); } while (0)

#define _TAP_FAIL(desc, fmt, ...) do { t_ctx->test_num++; t_ctx->failed++; \
    fprintf(_TAP_OUT, "not ok %d - %s\n#   " fmt "\n", t_ctx->test_num, (desc), ##__VA_ARGS__); } while (0)

/* For diagnostic/skip/progress lines emitted by test bodies outside the
   ASSERT_* macros. Routes through the same stream as TAP output so
   parallel run-all's per-test buffering never interleaves these with
   another concurrently-running test's lines. */
#define TAP_DIAG(fmt, ...) fprintf(_TAP_OUT, fmt, ##__VA_ARGS__)

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

/* NULL-safe strstr for test bodies that call strstr(resp, ...) directly
   (outside an ASSERT_* macro) after a non-fatal ASSERT_NOT_NULL/ASSERT_TRUE
   on resp. Those asserts record a failure but never stop execution, so a
   request that genuinely fails under load (timeout/connection reset,
   tc_recv leaves *out_response NULL) would otherwise crash the very next
   line instead of just leaving the already-recorded assertion failure as
   the only symptom. */
#define SAFE_STRSTR(h, n) ((h) ? strstr((h), (n)) : NULL)

#endif
