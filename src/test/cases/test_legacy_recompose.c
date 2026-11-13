/* src/test/cases/test_legacy_recompose.c — legacy-layout counterparts of the
 * recompose behaviours that the main edit/add-field/vacuum cases now exercise
 * on packed objects. Legacy objects are still creatable (declaration_layout
 * "legacy") and keep their own semantics: truncating numeric rescale,
 * zero-fill add-field backfill, "varchar shrink" pre-flight text. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "bappend.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct { TestEnv env; TestClient *tc; } Lr;

static int lr_open(Lr *p) {
    memset(p, 0, sizeof(*p));
    if (test_env_start(&p->env) != 0) return -1;
    TestClientCfg cfg = { .port = p->env.port, .io_timeout_ms = 60000 };
    p->tc = tc_connect(&cfg);
    if (!p->tc) { test_env_stop(&p->env); return -1; }
    char *r = NULL;
    tc_request(p->tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &r);
    free(r);
    return 0;
}
static void lr_close(Lr *p) {
    if (p->tc) tc_close(p->tc);
    test_env_stop(&p->env);
}
static char *lr_q(Lr *p, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static char *lr_q(Lr *p, const char *fmt, ...) {
    char req[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(req, sizeof(req), fmt, ap);
    va_end(ap);
    char *resp = NULL;
    if (tc_request(p->tc, req, &resp) != 0) { free(resp); return NULL; }
    return resp;
}
static void lr_expect(char *resp, const char *needle, const char *desc) {
    ASSERT_CONTAINS(resp, needle, desc);
    free(resp);
}
static void lr_create(Lr *p, const char *fields, const char *indexes) {
    lr_expect(lr_q(p,
        "{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\","
        "\"object\":\"o\",\"splits\":8,\"max_key\":16,\"fields\":%s,\"indexes\":%s}",
        fields, indexes), "\"status\":\"created\"", "create legacy object");
}

static int test_legacy_numeric_truncates_and_varchar_shrink(void) {
    Lr p;
    if (lr_open(&p) != 0) return 1;
    lr_create(&p, "[\"balance:numeric:18,4\",\"bio:varchar:64\"]", "[]");
    lr_expect(lr_q(&p, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"p\","
                       "\"value\":{\"balance\":\"3.1499\",\"bio\":\"short\"}}"), "inserted", "insert p");
    lr_expect(lr_q(&p, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"n\","
                       "\"value\":{\"balance\":\"-3.1499\",\"bio\":\"a much longer bio value\"}}"), "inserted", "insert n");
    lr_expect(lr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\","
                       "\"fields\":[\"balance:numeric:18,2\"]}"), "\"status\":\"edited\"", "scale-down");
    lr_expect(lr_q(&p, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"p\"}"),
              "\"balance\":3.14", "legacy scale-down truncates toward zero (positive)");
    lr_expect(lr_q(&p, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"n\"}"),
              "\"balance\":-3.14", "legacy scale-down truncates toward zero (negative)");
    char *r = lr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\","
                       "\"fields\":[\"bio:varchar:5\"]}");
    ASSERT_CONTAINS(r, "\"error\"", "varchar shrink below a live value is refused");
    ASSERT_CONTAINS(r, "varchar shrink", "legacy pre-flight reason text");
    free(r);
    lr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_legacy_add_field_zero_fill_and_compact(void) {
    Lr p;
    if (lr_open(&p) != 0) return 1;
    lr_create(&p, "[\"name:varchar:16\",\"gone:int\"]", "[]");
    lr_expect(lr_q(&p, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"k1\","
                       "\"value\":{\"name\":\"alice\",\"gone\":7}}"), "inserted", "insert k1");
    lr_expect(lr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\","
                       "\"fields\":[\"age:int\",\"email:varchar:16\",\"lit:int:default=9\"]}"),
              "\"status\":\"rebuilt\"", "add-field");
    char *g = lr_q(&p, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"k1\"}");
    ASSERT_CONTAINS(g, "\"age\":0", "legacy add-field zero-fills ints");
    ASSERT_CONTAINS(g, "\"email\":\"\"", "legacy add-field renders empty varchar");
    ASSERT_CONTAINS(g, "\"lit\":9", "legacy add-field applies the literal default");
    free(g);
    lr_expect(lr_q(&p, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"gone\"]}"),
              "\"status\"", "remove-field");
    lr_expect(lr_q(&p, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true}"),
              "\"status\"", "compact");
    g = lr_q(&p, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"k1\"}");
    ASSERT_CONTAINS(g, "\"name\":\"alice\"", "row survives legacy compact");
    ASSERT_TRUE(g && !strstr(g, "\"gone\""), "tombstoned field dropped by legacy compact");
    free(g);
    lr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Legacy twin of test-packed-bitmap-collision-growth: the bitmap rebuild
   callback and the post-recompose bitmap rebuild are shared by both layouts. */
#define LR_BM_ROWS 6000
static int test_legacy_bitmap_collision_growth(void) {
    Lr p;
    if (lr_open(&p) != 0) return 1;
    ASSERT_EQ_INT(test_env_ctl_set_kf_seams(&p.env, 64, 0), 0, "shrink the kf tier to force growth");
    lr_create(&p, "[\"kind:varchar:8\",\"kind2:varchar:8\",\"a:int\"]", "[\"kind:bitmap\"]");
    size_t cap = (size_t)LR_BM_ROWS * 96 + 256;
    char *req = malloc(cap);
    ASSERT_NOT_NULL(req, "alloc bulk request");
    if (!req) { lr_close(&p); return 1; }
    size_t n = (size_t)snprintf(req, cap, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"o\",\"records\":{");
    for (int i = 0; i < LR_BM_ROWS; i++)
        n = tappendf(req, cap, n, "%s\"r%05d\":{\"kind\":\"c%d\",\"kind2\":\"c%d\",\"a\":%d}",
                              i ? "," : "", i, i % 4, i % 4, i);
    snprintf(req + n, cap - n, "}}");
    char *r = NULL;
    tc_request(p.tc, req, &r);
    free(req);
    ASSERT_CONTAINS(r, "inserted", "bulk insert");
    free(r);
    for (int i = 0; i < LR_BM_ROWS; i += 3) {
        char *d = lr_q(&p, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"r%05d\"}", i);
        free(d);
    }
    const char *steps[] = {
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"o\",\"field\":\"kind:bitmap\",\"force\":true}",
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"z:int\"]}",
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true}",
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"a:long\"]}" };
    for (int s = -1; s < 4; s++) {
        if (s >= 0) lr_expect(lr_q(&p, "%s", steps[s]), "\"status\"", "recompose step");
        for (int c = 0; c < 4; c++) {
            char *b = lr_q(&p, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"o\",\"criteria\":[{\"field\":\"kind2\",\"op\":\"eq\",\"value\":\"c%d\"}]}", c);
            int nb = b ? tu_parse_count(b) : -2;
            free(b);
            char *r = lr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\",\"format\":\"dict\",\"limit\":100000,"
                               "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"c%d\"}]}", c);
            char hit[24];
            snprintf(hit, sizeof(hit), "\"kind\":\"c%d\"", c);
            int matching = 0, any = 0;
            for (const char *q = r; r && (q = strstr(q, hit)); q += strlen(hit)) matching++;
            for (const char *q = r; r && (q = strstr(q, "\"kind\":\"")); q += 8) any++;
            free(r);
            ASSERT_TRUE(nb > 0 && matching == nb && any == nb, "legacy bitmap find returns exactly the matching rows");
        }
    }
    lr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-legacy-numeric-truncates-varchar-shrink", test_legacy_numeric_truncates_and_varchar_shrink)
TEST_REGISTER("test-legacy-add-field-zero-fill-compact", test_legacy_add_field_zero_fill_and_compact)
TEST_REGISTER("test-legacy-bitmap-collision-growth", test_legacy_bitmap_collision_growth)
