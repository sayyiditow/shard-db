/* src/test/cases/test_nql_filter_parse_error_leak.c
 * Regression: a NQL filter whose IN-list fails to parse part-way (a bad
 * token after some values were already collected) must free the partial
 * list. The partially built `in_values` array and its strdup'ed members
 * were dropped on the error path: ~152 bytes leaked per request, wire
 * reachable and unbounded. In-process: no daemon needed.
 *
 * Detection: glibc mallinfo2() heap growth over many failing parses
 * (without the fix ~150 bytes x N). mallinfo2 does not see the sanitizer
 * heap, so the growth check is compiled out under ASan (the pool runner's
 * children _exit() and skip LSan's atexit check, so this case is only a
 * regression guard in the normal build; the asan gate still exercises the
 * parse path for memory errors).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern CriteriaNode *nql_parse_filter(const char *src, char *err_out,
                                      size_t err_sz);

static int test_nql_filter_parse_error_leak_run(void) {
    char err[256];
    /* Fails inside the list: two values collected, then a stray quote. */
    const char *bad = "indext in[ sed erert f\"";
    CriteriaNode *n = nql_parse_filter(bad, err, sizeof err);
    ASSERT_TRUE(n == NULL, "malformed list is rejected");

    const int N = 4000;
    for (int i = 0; i < 64; i++) (void)nql_parse_filter(bad, err, sizeof err);
#if defined(__SANITIZE_ADDRESS__)
    return t_ctx->failed > 0 ? 1 : 0;
#endif
    size_t before = mallinfo2().uordblks;
    for (int i = 0; i < N; i++) {
        CriteriaNode *r = nql_parse_filter(bad, err, sizeof err);
        if (r) free_criteria_tree(r);
    }
    size_t after = mallinfo2().uordblks;
    char msg[160];
    snprintf(msg, sizeof msg,
             "no heap growth over %d failing parses (grew %zu bytes)", N,
             after > before ? after - before : 0);
    ASSERT_TRUE(after < before + 64 * 1024, msg);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-filter-parse-error-leak", test_nql_filter_parse_error_leak_run)
