/* src/test/cases/test_query_plan_in_oom.c
 *
 * Deterministic realloc-failure coverage for parse_one_criterion's three
 * IN-list grow sites (Coverity CID 1699832). The list starts at
 * in_cap=64, so 66 elements force exactly one grow; arming
 * query_plan_test_set_fail_grow(1) fails that grow, and every input form
 * (quoted array, unquoted array, bare CSV) must surface as a clean parse
 * error (-1 / NULL / 0 count) with no crash and no partial list.
 * Pure parser unit test — no ShardDb needed.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "../db/types.h"
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

extern void query_plan_test_set_fail_grow(int fail_n);

#define IN_ELEMS 66

/* Appends at buf+off, clamping the returned offset to cap so a later
   call's `cap - off` can never underflow. */
static size_t safe_append(char *buf, size_t cap, size_t off, const char *fmt, ...) {
    if (off >= cap) return cap;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf + off, cap - off, fmt, ap);
    va_end(ap);
    if (n < 0) return off;
    size_t written = (size_t)n;
    return written >= cap - off ? cap : off + written;
}

static int run_one(const char *label, const char *value_form) {
    char json[8192];
    size_t off = 0;
    off = safe_append(json, sizeof(json), off,
        "[{\"field\":\"f\",\"op\":\"in\",\"value\":%s}]", value_form);

    SearchCriterion *crit = NULL;
    int n = -1;

    query_plan_test_set_fail_grow(0);
    ASSERT_EQ_INT(parse_criteria_json(json, &crit, &n), 0, "control parse succeeds");
    ASSERT_EQ_INT(n, 1, "control criterion count");
    if (n == 1 && crit) {
        ASSERT_EQ_INT(crit[0].in_count, IN_ELEMS, "control IN element count");
        free_criteria(crit, n);
    }

    query_plan_test_set_fail_grow(1);
    crit = NULL; n = -1;
    ASSERT_EQ_INT(parse_criteria_json(json, &crit, &n), -1,
                  "grow failure returns error");
    ASSERT_TRUE(crit == NULL && n == 0, "failed parse leaves no partial list");
    return 0;
}

static int test_query_plan_in_oom_run(void) {
    char quoted[4096], unquoted[4096], bare[4096];
    size_t off = 0;

    /* Form 1: JSON array of quoted elements  -> quoted grow site. */
    off = 0;
    off = safe_append(quoted, sizeof(quoted), off, "[");
    for (int i = 0; i < IN_ELEMS; i++)
        off = safe_append(quoted, sizeof(quoted), off,
            "%s\"e%d\"", i ? "," : "", i);
    off = safe_append(quoted, sizeof(quoted), off, "]");

    /* Form 2: JSON array of bare (unquoted) elements -> bare grow site. */
    off = 0;
    off = safe_append(unquoted, sizeof(unquoted), off, "[");
    for (int i = 0; i < IN_ELEMS; i++)
        off = safe_append(unquoted, sizeof(unquoted), off,
            "%se%d", i ? "," : "", i);
    off = safe_append(unquoted, sizeof(unquoted), off, "]");

    /* Form 3: bare CSV string value -> comma-token grow site. */
    off = 0;
    off = safe_append(bare, sizeof(bare), off, "\"");
    for (int i = 0; i < IN_ELEMS; i++)
        off = safe_append(bare, sizeof(bare), off,
            "%se%d", i ? "," : "", i);
    off = safe_append(bare, sizeof(bare), off, "\"");

    if (run_one("quoted-array form", quoted) != 0) return 1;
    if (run_one("unquoted-array form", unquoted) != 0) return 1;
    if (run_one("bare-CSV form", bare) != 0) return 1;
    return 0;
}

TEST_REGISTER("test-query-plan-in-oom", test_query_plan_in_oom_run)