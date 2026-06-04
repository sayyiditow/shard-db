/* src/test/test_runner.c */
#include "test_runner.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef TEST_BUILD
extern void test_reset_caches(void);
#endif

__thread TestCtx *t_ctx = NULL;

static TestCaseEntry *g_head = NULL;

void test_register(TestCaseEntry *entry) {
    /* Insert at end so registration order matches source order. */
    if (!g_head) { g_head = entry; return; }
    TestCaseEntry *p = g_head;
    while (p->next) p = p->next;
    p->next = entry;
}

int test_count(void) {
    int n = 0;
    for (TestCaseEntry *p = g_head; p; p = p->next) n++;
    return n;
}

const TestCaseEntry *test_first(void) { return g_head; }

static int run_case(const TestCaseEntry *tc) {
    TestCtx ctx = { .name = tc->name };
    t_ctx = &ctx;
    printf("# %s\n", tc->name);
    int rc = tc->fn();
    if (rc != 0 && ctx.failed == 0) ctx.failed = 1; /* fn signalled fail without assert */
    printf("# %s: %d passed, %d failed\n", tc->name, ctx.passed, ctx.failed);
    t_ctx = NULL;
    return ctx.failed;
}

int test_run_one(const char *name) {
    for (TestCaseEntry *p = g_head; p; p = p->next) {
        if (strcmp(p->name, name) == 0) return run_case(p);
    }
    fprintf(stderr, "no test named '%s'\n", name);
    return 1;
}

int test_run_all(const char *filter) {
    int total_fail = 0, ran = 0, total_passed = 0;
    for (TestCaseEntry *p = g_head; p; p = p->next) {
        if (filter && !strstr(p->name, filter)) continue;
        TestCtx ctx = { .name = p->name };
        t_ctx = &ctx;
        printf("# %s\n", p->name);
        int rc = p->fn();
        if (rc != 0 && ctx.failed == 0) ctx.failed = 1;
        total_fail += ctx.failed;
        total_passed += ctx.passed;
        ran++;
        printf("# %s: %d passed, %d failed\n", p->name, ctx.passed, ctx.failed);
        t_ctx = NULL;
#ifdef TEST_BUILD
        test_reset_caches();
#endif
    }
    printf("1..%d\n", ran);
    printf("# total: %d passed, %d failed across %d cases\n",
           total_passed, total_fail, ran);
    return total_fail;
}
