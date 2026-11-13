/* src/test/cases/test_packed_recompose.c — R5b: add-field, edit-field and
 * vacuum compact on packed-layout objects. Each case owns its daemon. */
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
#include <unistd.h>

typedef struct { TestEnv env; TestClient *tc; } Pr;

static int pr_open(Pr *p) {
    memset(p, 0, sizeof(*p));
    if (test_env_start(&p->env) != 0) return -1;
    TestClientCfg cfg = { .port = p->env.port, .io_timeout_ms = 300000 };
    /* 300s: schema-mutation recomposes (add-field/edit-field/compact
       over 6000 rows) provably exceed the old 60s under ASan/TSan on
       loaded CI runners; an expired read then desyncs the shared
       connection (late response read as the next response) and
       cascades into false bitmap-mismatch failures. Only binds on
       slow hosts; the run-all watchdog still bounds genuine hangs. */
    p->tc = tc_connect(&cfg);
    if (!p->tc) { test_env_stop(&p->env); return -1; }
    char *r = NULL;
    tc_request(p->tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &r);
    free(r);
    return 0;
}
static void pr_close(Pr *p) {
    if (p->tc) tc_close(p->tc);
    test_env_stop(&p->env);
}
static char *pr_q(Pr *p, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static char *pr_q(Pr *p, const char *fmt, ...) {
    char req[32768];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(req, sizeof(req), fmt, ap);
    va_end(ap);
    char *resp = NULL;
    if (tc_request(p->tc, req, &resp) != 0) { free(resp); return NULL; }
    return resp;
}
/* Expect `needle` in the response; frees it. */
static void pr_expect(char *resp, const char *needle, const char *desc) {
    ASSERT_CONTAINS(resp, needle, desc);
    free(resp);
}
static void pr_refused(char *resp, const char *desc) {
    ASSERT_CONTAINS(resp, "\"error\"", desc);
    free(resp);
}
static void pr_create(Pr *p, const char *obj, const char *fields, const char *indexes) {
    char *r = pr_q(p,
        "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":8,\"max_key\":16,\"fields\":%s,\"indexes\":%s}",
        obj, fields, indexes);
    pr_expect(r, "\"status\":\"created\"", "create packed object");
}
static void pr_ins(Pr *p, const char *obj, const char *key, const char *value) {
    char *r = pr_q(p,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"%s\",\"value\":%s}",
        obj, key, value);
    pr_expect(r, "\"status\":\"inserted\"", "insert");
}
static char *pr_get(Pr *p, const char *obj, const char *key) {
    return pr_q(p, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"%s\"}", obj, key);
}
static char *pr_conf(Pr *p, const char *obj) {
    char path[512];
    snprintf(path, sizeof(path), "%s/default/%s/fields.conf", p->env.db_root, obj);
    return tu_read_file(path);
}
static int pr_count(Pr *p, const char *obj) {
    char *r = pr_q(p, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"}", obj);
    int n = r ? tu_parse_count(r) : -1;
    free(r);
    return n;
}
static int pr_txn_residue(Pr *p, const char *obj) {
    const char *names[] = {".rebuild_txn.preparing", ".rebuild_txn.active", ".rebuild_txn.done",
                           ".rebuild_legacy_root", "data.legacy", "fields.conf.new"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        char path[600];
        snprintf(path, sizeof(path), "%s/default/%s/%s", p->env.db_root, obj, names[i]);
        if (tu_file_exists(path)) return 1;
    }
    return 0;
}
/* get(before) must equal get(after) once `tail` (e.g. ,"f9":0) is spliced before the closing brace. */
static void pr_same_plus(const char *before, const char *after, const char *tail, const char *desc) {
    ASSERT_NOT_NULL(before, desc);
    ASSERT_NOT_NULL(after, desc);
    if (!before || !after) return;
    char exp[4096];
    size_t n = strlen(before);
    while (n && (before[n - 1] == '\n' || before[n - 1] == ' ')) n--;
    ASSERT_TRUE(n > 1 && before[n - 1] == '}', "bare object response");
    snprintf(exp, sizeof(exp), "%.*s%s}", (int)(n - 1), before, tail);
    ASSERT_CONTAINS(after, exp, desc);
}

/* ---- add-field ------------------------------------------------------ */

static int test_packed_add_field(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    /* 8 fields: adding the 9th grows the null-bitmap prefix from 2 to 3 bytes. */
    pr_create(&p, "o",
        "[\"f1:int\",\"f2:int\",\"f3:int\",\"f4:int\",\"f5:int\",\"f6:int\",\"f7:varchar:10\",\"f8:numeric:8,2\"]",
        "[\"f1\"]");
    pr_ins(&p, "o", "k1", "{\"f1\":1,\"f7\":\"hi\",\"f8\":\"1.50\"}");
    pr_ins(&p, "o", "k2", "{\"f1\":2,\"f7\":null,\"f8\":null}");   /* explicit NULLs must survive */
    char *b1 = pr_get(&p, "o", "k1"), *b2 = pr_get(&p, "o", "k2");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"f9:int\"]}"),
              "\"status\":\"rebuilt\"", "add-field succeeds on a packed object");
    char *a1 = pr_get(&p, "o", "k1"), *a2 = pr_get(&p, "o", "k2");
    pr_same_plus(b1, a1, ",\"f9\":null", "old row k1 intact, new nullable field without default reads NULL (A-1)");
    pr_same_plus(b2, a2, ",\"f9\":null", "NULLs preserved across the bitmap resize");
    free(b1); free(b2); free(a1); free(a2);
    pr_expect(pr_q(&p, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"k3\","
                       "\"value\":{\"f1\":3,\"f9\":9}}"), "\"status\":\"inserted\"", "9-field insert works");
    char *c = pr_conf(&p, "o");
    ASSERT_TRUE(c && strstr(c, "f9:int"), "fields.conf has the appended line");
    free(c);
    ASSERT_EQ_INT(pr_count(&p, "o"), 3, "record count");
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]}"),
              "k2", "existing index still answers after add-field");
    ASSERT_TRUE(!pr_txn_residue(&p, "o"), "no transaction residue");
    char sc[512];
    snprintf(sc, sizeof(sc), "%s/schema.conf", p.env.db_root);
    char *conf = tu_read_file(sc);
    ASSERT_TRUE(conf && strstr(conf, "layout=packed"), "layout token preserved");
    free(conf);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_add_field_grammar(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"a:int\"]", "[]");
    pr_ins(&p, "o", "k1", "{\"a\":1}");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\","
                       "\"fields\":[\"note:varchar\",\"amt:numeric\",\"price:currency\",\"n:numeric:10,3\",\"w:numeric:12\"]}"),
              "\"status\":\"rebuilt\"", "bare varchar/numeric, currency, numeric(10,3), numeric:12 (scale 0)");
    char *g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "\"note\":null") && strstr(g, "\"amt\":null") && strstr(g, "\"w\":null"),
                "default-less nullable bare varchar/numerics read NULL on old rows (A-1)");
    free(g);
    /* A-1: a declared default backfills old rows; NOT NULL without a default is refused. */
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"lvl:int:default=7\"]}"),
              "\"status\":\"rebuilt\"", "add-field with a default");
    g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "\"lvl\":7"), "old row backfilled with the declared default");
    free(g);
    char *pre_nn = pr_conf(&p, "o");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"nn:int:not_null\"]}"),
               "NOT NULL without a default is refused when rows exist");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"nn:int:not_null\"]}"),
              "NOT NULL and has no default", "refusal names the cause");
    char *post_nn = pr_conf(&p, "o");
    ASSERT_TRUE(pre_nn && post_nn && strcmp(pre_nn, post_nn) == 0, "fields.conf unchanged after the refusal");
    free(pre_nn); free(post_nn);
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"nn2:int:default=3:not_null\"]}"),
              "\"status\":\"rebuilt\"", "NOT NULL with a default is accepted");
    char *before = pr_conf(&p, "o");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"x:varchar:0\"]}"), "varchar:0 refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"x:numeric:0\"]}"), "numeric:0 refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"x:varchar:4194305\"]}"), "varchar:4194305 refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"a:int\"]}"), "duplicate refused");
    char *after = pr_conf(&p, "o");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0, "refusals leave fields.conf untouched");
    free(before); free(after);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_add_field_defaults(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"a:int\"]", "[]");
    char key[16], val[64];
    for (int i = 0; i < 40; i++) {
        snprintf(key, sizeof(key), "k%02d", i);
        snprintf(val, sizeof(val), "{\"a\":%d}", i);
        pr_ins(&p, "o", key, val);
    }
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":["
        "\"lit:int:default=7\",\"flag:bool:default=true\",\"txt:varchar:10:default=hey\","
        "\"rid:long:default=seq(rid)\",\"tid:varchar:36:default=uuid()\",\"u:uuid:default=uuid()\","
        "\"nonce:varchar:8:default=random(4)\",\"ca:timestamp:auto_create\"]}"),
        "\"status\":\"rebuilt\"", "add-field with every default kind");
    /* literal */
    char *g = pr_get(&p, "o", "k00");
    ASSERT_TRUE(g && strstr(g, "\"lit\":7") && strstr(g, "\"txt\":\"hey\""), "literal defaults stamped");
    free(g);
    /* seq: 40 distinct values 1..40, contiguous; next insert resumes at 41 */
    long sum = 0;
    for (int i = 0; i < 40; i++) {
        snprintf(key, sizeof(key), "k%02d", i);
        g = pr_get(&p, "o", key);
        const char *s = g ? strstr(g, "\"rid\":") : NULL;
        ASSERT_TRUE(s != NULL, "rid present");
        if (s) sum += atol(s + 6);
        free(g);
    }
    ASSERT_EQ_INT(sum, 40L * 41L / 2L, "seq values are exactly 1..40 (order = shard scan order, documented)");
    pr_expect(pr_q(&p, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"new\",\"value\":{\"a\":99}}"),
              "\"status\":\"inserted\"", "insert after backfill");
    g = pr_get(&p, "o", "new");
    ASSERT_TRUE(g && strstr(g, "\"rid\":41"), "sequence resumes at start+live_count");
    free(g);
    /* uuid / random uniqueness */
    char *g0 = pr_get(&p, "o", "k00"), *g1 = pr_get(&p, "o", "k01");
    const char *t0 = g0 ? strstr(g0, "\"tid\":\"") : NULL, *t1 = g1 ? strstr(g1, "\"tid\":\"") : NULL;
    ASSERT_TRUE(t0 && t1 && strncmp(t0, t1, 7 + 36) != 0 && t0[7 + 8] == '-', "uuid() differs per row, dashed form");
    const char *n0 = g0 ? strstr(g0, "\"nonce\":\"") : NULL, *n1 = g1 ? strstr(g1, "\"nonce\":\"") : NULL;
    ASSERT_TRUE(n0 && n1 && n0[9 + 8] == '"' && strncmp(n0, n1, 9 + 8) != 0, "random(4) = 8 hex chars, differs per row");
    free(g0); free(g1);
    /* A-3 refusals: nothing changes */
    char *before = pr_conf(&p, "o");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"r:varchar:8:default=random(5)\"]}"), "random(5) into varchar:8 refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"r:int:default=random(2)\"]}"), "random into int refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"r:varchar:20:default=uuid()\"]}"), "uuid() into varchar:20 refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"r:int:default=abc\"]}"), "unstorable literal refused");
    char *after = pr_conf(&p, "o");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0, "refusals leave fields.conf untouched");
    ASSERT_TRUE(!pr_txn_residue(&p, "o"), "no transaction residue");
    free(before); free(after);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_add_field_atomic(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"a:int\"]", "[\"a\"]");
    char key[16], val[64];
    for (int i = 0; i < 300; i++) {
        snprintf(key, sizeof(key), "k%03d", i);
        snprintf(val, sizeof(val), "{\"a\":%d}", i);
        pr_ins(&p, "o", key, val);
    }
    char *snap[300];
    for (int i = 0; i < 300; i++) { snprintf(key, sizeof(key), "k%03d", i); snap[i] = pr_get(&p, "o", key); }
    char *conf0 = pr_conf(&p, "o");
    /* 300 sequence values cannot fit a signed byte: the range's LAST value fails the probe
       after reservation but before any row is written. */
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"s:byte:default=seq(sq)\"]}"),
               "seq overflowing a byte refuses the whole add");
    char *conf1 = pr_conf(&p, "o");
    ASSERT_TRUE(conf0 && conf1 && strcmp(conf0, conf1) == 0, "fields.conf unchanged");
    ASSERT_EQ_INT(pr_count(&p, "o"), 300, "count unchanged");
    int same = 1;
    for (int i = 0; i < 300; i++) {
        snprintf(key, sizeof(key), "k%03d", i);
        char *now = pr_get(&p, "o", key);
        if (!now || !snap[i] || strcmp(now, snap[i]) != 0) same = 0;
        free(now); free(snap[i]);
    }
    ASSERT_TRUE(same, "every row byte-identical after the refused add");
    ASSERT_TRUE(!pr_txn_residue(&p, "o"), "transaction fully rolled back");
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"7\"}]}"), "k007", "index still consistent");
    free(conf0); free(conf1);
    /* limits: 255 fields + 1 ok, one more refused (tombstones count toward 256) */
    char fields[8192] = "[";
    size_t off = 1;
    for (int i = 0; i < 255; i++) off = tappendf(fields, sizeof(fields), off, "%s\"c%d:int\"", i ? "," : "", i);
    snprintf(fields + off, sizeof(fields) - off, "]");
    pr_create(&p, "wide", fields, "[]");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"wide\",\"fields\":[\"c255:int\"]}"),
              "\"status\":\"rebuilt\"", "256th field accepted");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"wide\",\"fields\":[\"c256:int\"]}"),
               "257th field refused");
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- edit-field ----------------------------------------------------- */

static int test_packed_edit_field_varchar(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"name:varchar:10\",\"b:varchar\"]", "[\"name\"]");
    pr_ins(&p, "o", "k1", "{\"name\":\"abcdefghij\",\"b\":\"x\"}");
    pr_ins(&p, "o", "k2", "{\"name\":\"zz\",\"b\":null}");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"name:varchar:40\"]}"),
              "\"rebuilt\":true", "grow rebuilds");
    char *g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "abcdefghij"), "value intact after grow");
    free(g);
    char *before = pr_conf(&p, "o");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"name:varchar:5\"]}"),
              "Pre-flight failed on field [name]", "shrink below an existing value refused by the pre-flight");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"dry_run\":true,\"fields\":[\"name:varchar:5\"]}"),
              "Pre-flight failed on field [name]", "dry_run predicts the same refusal");
    char *after = pr_conf(&p, "o");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0, "refused shrink changed nothing");
    free(before); free(after);
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"name:varchar:10\"]}"),
              "\"rebuilt\":true", "shrink to exactly the longest value is allowed");
    /* bare <-> bounded, every direction */
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"name:varchar\"]}"),
              "\"status\":\"edited\"", "bounded -> bare");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"b:varchar:3\"]}"),
              "\"status\":\"edited\"", "bare -> bounded when all rows fit");
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"name:varchar:3\"]}"),
               "bare -> too-small bounded refused");
    ASSERT_EQ_INT(pr_count(&p, "o"), 2, "count");
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"zz\"}]}"), "k2", "varchar index rebuilt and consistent");
    /* metadata-only: default change, no rebuild */
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"b:varchar:3:default=q\"]}"),
              "\"rebuilt\":false", "default-only edit is metadata only");
    ASSERT_TRUE(!pr_txn_residue(&p, "o"), "no residue");
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_edit_field_numeric(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    const char *rows[][2] = { {"k1", "1.0050"}, {"k2", "-1.0050"}, {"k3", "2.5000"}, {"k4", "99999999.9999"} };
    pr_create(&p, "o", "[\"a:numeric:12,4\"]", "[\"a\"]");
    pr_create(&p, "ov", "[\"a:numeric:12,4\"]", "[\"a\"]");
    for (int i = 0; i < 4; i++) {
        char v[64];
        snprintf(v, sizeof(v), "{\"a\":\"%s\"}", rows[i][1]);
        pr_ins(&p, "o", rows[i][0], v);
        pr_ins(&p, "ov", rows[i][0], v);
    }
    /* D-1: scale reduction rounds half away from zero; the integer part still fits (9 digits <= 10) */
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"a:numeric:12,2\"]}"),
              "\"rebuilt\":true", "numeric(12,4) -> numeric(12,2)");
    char *g;
    g = pr_get(&p, "o", "k1"); ASSERT_TRUE(g && strstr(g, "1.01") && !strstr(g, "1.0050"), "1.0050 rounds to 1.01"); free(g);
    g = pr_get(&p, "o", "k2"); ASSERT_TRUE(g && strstr(g, "-1.01"), "-1.0050 rounds to -1.01 (ties away from zero)"); free(g);
    g = pr_get(&p, "o", "k3"); ASSERT_TRUE(g && strstr(g, "2.50"), "2.5000 -> 2.50"); free(g);
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"1.01\"}]}"), "k1", "index rebuilt: eq on the rounded value");
    /* all-or-nothing: 99999999.9999 rounds to 100000000.00 = 9 integer digits > 10-2 */
    char *conf0 = pr_conf(&p, "ov");
    char *dry = pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ov\",\"dry_run\":true,\"fields\":[\"a:numeric:10,2\"]}");
    pr_expect(dry, "Pre-flight failed on field [a]", "dry_run predicts the overflow refusal");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ov\",\"fields\":[\"a:numeric:10,2\"]}"),
              "Pre-flight failed on field [a]", "overflow after rounding refused by the pre-flight");
    char *conf1 = pr_conf(&p, "ov");
    ASSERT_TRUE(conf0 && conf1 && strcmp(conf0, conf1) == 0, "fields.conf unchanged");
    free(conf0); free(conf1);
    for (int i = 0; i < 4; i++) {
        g = pr_get(&p, "ov", rows[i][0]);
        ASSERT_TRUE(g && strstr(g, rows[i][1]), "every row (including the ones that would have fit) is unchanged");
        free(g);
    }
    /* bare <-> bounded and currency */
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ov\",\"fields\":[\"a:numeric\"]}"),
              "\"status\":\"edited\"", "bounded -> bare");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ov\",\"fields\":[\"a:currency\"]}"),
              "\"status\":\"edited\"", "bare -> currency (numeric(19,4))");
    ASSERT_EQ_INT(pr_count(&p, "ov"), 4, "count");
    ASSERT_TRUE(!pr_txn_residue(&p, "ov") && !pr_txn_residue(&p, "o"), "no residue");
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_edit_field_fixed(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    char enums[4096] = "[\"i:int\",\"s:short\",\"f:float\",\"e:enum(a,b,c)\",\"big:enum(";
    size_t off = strlen(enums);
    for (int i = 0; i < 256; i++) off = tappendf(enums, sizeof(enums), off, "%sv%d", i ? "," : "", i);
    snprintf(enums + off, sizeof(enums) - off, ")\"]");
    pr_create(&p, "o", enums, "[\"i\"]");
    pr_ins(&p, "o", "k1", "{\"i\":-200,\"s\":300,\"f\":3.25,\"e\":\"c\",\"big\":\"v255\"}");
    pr_ins(&p, "o", "k2", "{\"i\":70000,\"s\":null,\"f\":null,\"e\":null,\"big\":null}");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"i:long\",\"f:double\"]}"),
              "\"rebuilt\":true", "int->long, float->double");
    char *g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "\"i\":-200") && strstr(g, "3.25"), "negatives sign-extend; float widens exactly");
    free(g);
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"i:short\"]}"),
               "narrow refused: 70000 does not fit a short");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"s:int\"]}"),
              "\"rebuilt\":true", "short->int");
    /* enum: append within width = metadata only; rename needs allow_rename */
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"e:enum(a,b,c,d)\"]}"),
              "\"rebuilt\":false", "enum append (same width) is metadata only");
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"e:enum(x,b,c,d)\"]}"),
               "rename without allow_rename refused");
    /* enum 256 -> 257 values: 1B -> 2B widen rebuild */
    char wide[4096] = "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"big:enum(";
    off = strlen(wide);
    for (int i = 0; i < 257; i++) off = tappendf(wide, sizeof(wide), off, "%sv%d", i ? "," : "", i);
    snprintf(wide + off, sizeof(wide) - off, ")\"]}");
    pr_expect(pr_q(&p, "%s", wide), "\"rebuilt\":true", "enum 1B -> 2B widen rebuilds");
    g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "\"big\":\"v255\"") && strstr(g, "\"e\":\"c\""), "enum indexes preserved through the widen");
    free(g);
    g = pr_get(&p, "o", "k2");
    ASSERT_TRUE(g != NULL, "NULL row still readable");
    free(g);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Bitmap indexes address data by kf slot; a rebuild renumbers slots. Prove they stay correct.
   A failure here is a PRE-EXISTING engine bug exposed by R5b (the legacy path is identical): report it, do not paper over it. */
static int pr_cnt(Pr *p, const char *field, const char *value) {
    char *r = pr_q(p, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"o\",\"criteria\":[{\"field\":\"%s\",\"op\":\"eq\",\"value\":\"%s\"}]}", field, value);
    int n = r ? tu_parse_count(r) : -2;
    free(r);
    return n;
}
static int test_packed_bitmap_after_rebuild(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"flag:bool\",\"c:enum(red,green,blue)\",\"a:int\"]", "[\"flag\",\"c\"]");
    char key[16], val[96];
    for (int i = 0; i < 30; i++) {
        snprintf(key, sizeof(key), "k%02d", i);
        snprintf(val, sizeof(val), "{\"flag\":%s,\"c\":\"%s\",\"a\":%d}", i % 3 ? "false" : "true", i % 2 ? "green" : "blue", i);
        pr_ins(&p, "o", key, val);
    }
    for (int i = 0; i < 10; i++) {
        snprintf(key, sizeof(key), "k%02d", i);
        pr_expect(pr_q(&p, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"%s\"}", key), "deleted", "delete");
    }
    int base_flag = pr_cnt(&p, "flag", "true"), base_c = pr_cnt(&p, "c", "blue");
    const char *ops[] = {
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"z:int\"]}",
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true}",
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"a:long\"]}" };
    for (int o = 0; o < 3; o++) {
        pr_expect(pr_q(&p, "%s", ops[o]), "\"status\"", "recompose");
        ASSERT_EQ_INT(pr_cnt(&p, "flag", "true"), base_flag, "bool bitmap count stable after recompose");
        ASSERT_EQ_INT(pr_cnt(&p, "c", "blue"), base_c, "enum bitmap count stable after recompose");
    }
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Finding 1 (blind review): bitmap bits are kf slot numbers, and a recompose
   re-inserts rows into a fresh keyfile. The kf tier is shrunk to 64 slots per
   shard so 6000 rows force repeated in-place growth (~73% load, many probe
   collisions) and every third row is deleted so probe chains differ after the
   rebuild. A popcount alone cannot see a misplaced bit, so each check resolves
   the bitmap's slots back to records with `find` and requires that every row
   returned really has the queried value and that none is missing. */
#define PR_BM_ROWS 6000
static int pr_occurrences(const char *hay, const char *needle) {
    int n = 0;
    for (const char *p = hay; hay && (p = strstr(p, needle)); p += strlen(needle)) n++;
    return n;
}
static int pr_bm_check(Pr *p, const char *when) {
    int bad = 0;
    for (int c = 0; c < 4; c++) {
        char v[8], hit[24];
        snprintf(v, sizeof(v), "c%d", c);
        snprintf(hit, sizeof(hit), "\"kind\":\"%s\"", v);
        int via_scan = pr_cnt(p, "kind2", v);
        char *r = pr_q(p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\",\"format\":\"dict\",\"limit\":100000,"
                          "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"%s\"}]}", v);
        int matching = pr_occurrences(r, hit);
        int any = pr_occurrences(r, "\"kind\":\"");
        free(r);
        if (matching != via_scan || any != via_scan || via_scan <= 0) {
            fprintf(stderr, "# bitmap find mismatch %s %s: matching=%d any=%d scan=%d\n", when, v, matching, any, via_scan);
            bad++;
        }
    }
    ASSERT_EQ_INT(bad, 0, when);
    return bad;
}
static int test_packed_bitmap_collision_growth(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    ASSERT_EQ_INT(test_env_ctl_set_kf_seams(&p.env, 64, 0), 0, "shrink the kf tier to force growth");
    pr_create(&p, "o", "[\"kind:varchar:8\",\"kind2:varchar:8\",\"a:int\"]", "[\"kind:bitmap\"]");
    size_t cap = (size_t)PR_BM_ROWS * 96 + 256;
    char *req = malloc(cap);
    ASSERT_NOT_NULL(req, "alloc bulk request");
    if (!req) { pr_close(&p); return 1; }
    size_t n = (size_t)snprintf(req, cap, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"o\",\"records\":{");
    for (int i = 0; i < PR_BM_ROWS; i++)
        n = tappendf(req, cap, n, "%s\"r%05d\":{\"kind\":\"c%d\",\"kind2\":\"c%d\",\"a\":%d}",
                              i ? "," : "", i, i % 4, i % 4, i);
    snprintf(req + n, cap - n, "}}");
    char *r = NULL;
    tc_request(p.tc, req, &r);
    free(req);
    ASSERT_CONTAINS(r, "inserted", "bulk insert 6000 rows");
    free(r);
    for (int i = 0; i < PR_BM_ROWS; i += 3) {
        char k[16];
        snprintf(k, sizeof(k), "r%05d", i);
        char *d = pr_q(&p, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"%s\"}", k);
        free(d);
    }
    pr_bm_check(&p, "baseline bitmap == scan");
    pr_expect(pr_q(&p, "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"o\",\"field\":\"kind:bitmap\",\"force\":true}"), "\"status\"", "forced bitmap rebuild");
    pr_bm_check(&p, "bitmap == scan after forced bitmap rebuild");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"z:int\"]}"), "\"status\"", "add-field");
    pr_bm_check(&p, "bitmap == scan after add-field");
    pr_expect(pr_q(&p, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true}"), "\"status\"", "compact");
    pr_bm_check(&p, "bitmap == scan after compact");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"a:long\"]}"), "\"status\"", "edit-field");
    pr_bm_check(&p, "bitmap == scan after edit-field");
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Multi-field edit (n_edits > 1), tombstone in the middle, numeric NULL, allow_rename success,
   add-field with a :not_null field on an object that already has a tombstone. */
static int test_packed_edit_field_tombstone_multi(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"a:int\",\"gone:int\",\"n:numeric:10,4\",\"s:varchar:10\",\"e:enum(x,y)\"]", "[]");
    pr_ins(&p, "o", "k1", "{\"a\":1,\"gone\":5,\"n\":\"1.2345\",\"s\":\"hi\",\"e\":\"y\"}");
    pr_ins(&p, "o", "k2", "{\"a\":2,\"n\":null,\"s\":null,\"e\":null}");
    pr_expect(pr_q(&p, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"gone\"]}"), "\"status\":\"removed\"", "tombstone");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"allow_rename\":true,\"fields\":["
                       "\"a:long\",\"n:numeric:10,2\",\"s:varchar:40\",\"e:enum(x,z)\"]}"),
              "\"rebuilt\":true", "multi-field edit incl. rename");
    char *g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "1.23") && strstr(g, "\"e\":\"z\"") && !strstr(g, "\"gone\""), "values converted, rename relabels, tombstone stays hidden");
    free(g);
    g = pr_get(&p, "o", "k2");
    ASSERT_TRUE(g && !strstr(g, "\"error\""), "NULL numeric row readable");
    free(g);
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"gone:int\"]}"), "tombstoned field cannot be edited");
    /* A-1: :not_null with rows present needs a usable default; with one, the
       add succeeds next to the tombstone (the walk backfills the default). */
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"nn:int:default=5:not_null\",\"t:varchar:5\"]}"),
              "\"status\":\"rebuilt\"", "add-field with a defaulted :not_null field next to a tombstone");
    char *c = pr_conf(&p, "o");
    ASSERT_TRUE(c && strstr(c, "gone:int:removed") && strstr(c, "nn:int:default=5:not_null"), "tombstone line kept, new line appended");
    free(c);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_edit_field_gates(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"a:int\",\"b:varchar:10\"]", "[]");
    pr_ins(&p, "o", "k1", "{\"a\":1,\"b\":\"x\"}");
    pr_ins(&p, "o", "k2", "{\"a\":2,\"b\":null}");
    /* packed-aware NOT NULL gate */
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"b:varchar:10:not_null\"]}"),
               "adding :not_null over a NULL row is refused");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"a:int:not_null\"]}"),
              "\"status\":\"edited\"", "adding :not_null when no NULL exists succeeds");
    /* A-3 on the edit path: computed defaults are probed before the edit applies. */
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"b:varchar:10:default=random(16)\"]}"),
               "random(N) whose hex exceeds the cap is refused on edit");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"b:varchar:10:default=random(4)\"]}"),
              "\"rebuilt\":false", "random(N) that fits is a metadata-only edit");
    char *c = pr_conf(&p, "o");
    ASSERT_TRUE(c && strstr(c, "a:int:not_null"), "fields.conf carries the constraint");
    free(c);
    /* dry_run: reports, changes nothing */
    char *before = pr_conf(&p, "o");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"dry_run\":true,\"fields\":[\"b:varchar:50\"]}"),
              "\"dry_run\":true", "dry_run success");
    char *after = pr_conf(&p, "o");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0, "dry_run wrote nothing");
    free(before); free(after);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- vacuum compact ------------------------------------------------- */

static int test_packed_compact(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    /* 9 fields -> 8 after compact: the null-bitmap prefix shrinks 3 -> 2 bytes */
    pr_create(&p, "o",
        "[\"a:int\",\"b:varchar:20\",\"c:numeric:10,2\",\"d:bool\",\"e:int\",\"f:int\",\"g:int\",\"h:int\",\"i:int\"]",
        "[\"b\"]");
    char key[16], val[128];
    for (int i = 0; i < 6; i++) {
        snprintf(key, sizeof(key), "k%d", i);
        snprintf(val, sizeof(val), "{\"a\":%d,\"b\":\"v%d\",\"c\":\"1.50\",\"d\":true,\"e\":%d}", i, i, i);
        pr_ins(&p, "o", key, val);
    }
    pr_ins(&p, "o", "kn", "{\"a\":9,\"b\":null}");
    char *r0 = pr_q(&p, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true}");
    ASSERT_CONTAINS(r0, "\"status\":\"rebuilt\"", "compact with no tombstones still rebuilds");
    const char *s0 = r0 ? strstr(r0, "\"slot_size\":") : NULL;
    int slot_before = s0 ? atoi(s0 + 12) : -1;
    free(r0);
    pr_expect(pr_q(&p, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"k2\"}"), "deleted", "delete k2");
    pr_expect(pr_q(&p, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"k3\"}"), "deleted", "delete k3");
    char *orph = pr_q(&p, "{\"mode\":\"orphaned\",\"dir\":\"default\",\"object\":\"o\"}");
    ASSERT_TRUE(orph && atoi(orph) >= 2, "two deleted records pending");
    free(orph);
    pr_expect(pr_q(&p, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"c\"]}"),
              "\"status\":\"removed\"", "tombstone c");
    char *r1 = pr_q(&p, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true}");
    ASSERT_CONTAINS(r1, "\"compact\":true", "compact reports compact");
    const char *s1 = r1 ? strstr(r1, "\"slot_size\":") : NULL;
    ASSERT_TRUE(s1 && atoi(s1 + 12) < slot_before, "slot_size shrinks when a field is dropped");
    free(r1);
    char *conf = pr_conf(&p, "o");
    ASSERT_TRUE(conf && !strstr(conf, ":removed") && !strstr(conf, "c:numeric"), "tombstoned line dropped from fields.conf");
    free(conf);
    orph = pr_q(&p, "{\"mode\":\"orphaned\",\"dir\":\"default\",\"object\":\"o\"}");
    ASSERT_EQ_INT(orph ? atoi(orph) : -1, 0, "deleted-record space reclaimed");
    free(orph);
    ASSERT_EQ_INT(pr_count(&p, "o"), 5, "live rows preserved");
    char *g = pr_get(&p, "o", "k4");
    ASSERT_TRUE(g && strstr(g, "\"b\":\"v4\"") && strstr(g, "\"e\":4") && !strstr(g, "\"c\""), "row intact, dropped field gone");
    free(g);
    g = pr_get(&p, "o", "kn");
    ASSERT_TRUE(g != NULL && !strstr(g, "\"error\""), "NULL row survives the bitmap shrink");
    free(g);
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"v4\"}]}"), "k4", "index preserved and consistent");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"c:int\"]}"),
              "\"status\":\"rebuilt\"", "a name freed by compact can be re-added");
    /* compact + splits in one pass */
    pr_expect(pr_q(&p, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true,\"splits\":16}"),
              "\"splits\":16", "compact + splits");
    char sc[512];
    snprintf(sc, sizeof(sc), "%s/schema.conf", p.env.db_root);
    char *sconf = tu_read_file(sc);
    ASSERT_TRUE(sconf && strstr(sconf, "default:o:16:") && strstr(sconf, "layout=packed"), "splits changed, layout token preserved");
    free(sconf);
    ASSERT_EQ_INT(pr_count(&p, "o"), 5, "count after compact+splits");
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"v1\"}]}"), "k1", "index consistent after reshard");
    ASSERT_TRUE(!pr_txn_residue(&p, "o"), "no residue");
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A-4 regression: a LIVE field whose default literal contains ":removed" mid-line
   must survive compaction on both layouts. */
static int test_compact_removed_literal(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    const char *layouts[] = { "packed", "legacy" };
    for (int L = 0; L < 2; L++) {
        char obj[16];
        snprintf(obj, sizeof(obj), "o_%s", layouts[L]);
        char *r = pr_q(&p,
            "{\"mode\":\"create-object\",\"declaration_layout\":\"%s\",\"dir\":\"default\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"note:varchar:20:default=a:removed:x\",\"gone:int\"]}",
            layouts[L], obj);
        pr_expect(r, "\"status\":\"created\"", "create");
        pr_ins(&p, obj, "k1", "{\"note\":\"hello\",\"gone\":1}");
        pr_expect(pr_q(&p, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"%s\",\"fields\":[\"gone\"]}", obj),
                  "\"status\":\"removed\"", "tombstone gone");
        pr_expect(pr_q(&p, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"%s\",\"compact\":true}", obj),
                  "\"status\":\"rebuilt\"", "compact");
        char *conf = pr_conf(&p, obj);
        ASSERT_TRUE(conf && strstr(conf, "note:varchar:20:default=a:removed:x") && !strstr(conf, "gone:"),
                    "live line kept verbatim, tombstoned line dropped");
        free(conf);
        char *g = pr_get(&p, obj, "k1");
        ASSERT_TRUE(g && strstr(g, "\"note\":\"hello\""), "row readable");
        free(g);
    }
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Finding 7 (blind review): a live field whose default literal merely contains
   ":removed" must not be mistaken for a tombstone by remove-field, rename-field
   or edit-field (only an exact trailing ":removed" is the marker). */
static int test_removed_literal_remove_rename_edit(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    const char *layouts[] = { "packed", "legacy" };
    for (int L = 0; L < 2; L++) {
        char obj[16];
        snprintf(obj, sizeof(obj), "o_%s", layouts[L]);
        pr_expect(pr_q(&p,
            "{\"mode\":\"create-object\",\"declaration_layout\":\"%s\",\"dir\":\"default\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"note:varchar:20:default=a:removed:x\",\"other:int\",\"n2:varchar:20:default=b:removed:y\"]}",
            layouts[L], obj), "\"status\":\"created\"", "create");
        pr_ins(&p, obj, "k1", "{\"note\":\"hello\",\"other\":1,\"n2\":\"w\"}");
        pr_expect(pr_q(&p, "{\"mode\":\"rename-field\",\"dir\":\"default\",\"object\":\"%s\",\"old\":\"other\",\"new\":\"n2x\"}", obj),
                  "\"status\":\"renamed\"", "rename succeeds beside a ':removed' literal");
        pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"%s\",\"fields\":[\"note:varchar:30:default=c:removed:z\"]}", obj),
                  "\"status\":\"edited\"", "edit-field accepts a ':removed' literal in a default");
        pr_expect(pr_q(&p, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"%s\",\"fields\":[\"n2\"]}", obj),
                  "\"status\":\"removed\"", "remove-field tombstones a live field with a ':removed' literal");
        char *conf = pr_conf(&p, obj);
        ASSERT_TRUE(conf && strstr(conf, "n2:varchar:20:default=b:removed:y:removed") &&
                    strstr(conf, "note:varchar:30:default=c:removed:z"), "only the removed field is tombstoned");
        free(conf);
        pr_refused(pr_q(&p, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"%s\",\"fields\":[\"n2\"]}", obj),
                   "a genuinely removed field is still 'already removed'");
    }
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-packed-add-field", test_packed_add_field)
TEST_REGISTER("test-packed-add-field-grammar", test_packed_add_field_grammar)
TEST_REGISTER("test-packed-add-field-defaults", test_packed_add_field_defaults)
TEST_REGISTER("test-packed-add-field-atomic", test_packed_add_field_atomic)
TEST_REGISTER("test-packed-edit-field-varchar", test_packed_edit_field_varchar)
TEST_REGISTER("test-packed-edit-field-numeric", test_packed_edit_field_numeric)
TEST_REGISTER("test-packed-edit-field-fixed", test_packed_edit_field_fixed)
TEST_REGISTER("test-packed-edit-field-gates", test_packed_edit_field_gates)
TEST_REGISTER("test-packed-bitmap-after-rebuild", test_packed_bitmap_after_rebuild)
TEST_REGISTER("test-packed-bitmap-collision-growth", test_packed_bitmap_collision_growth)
TEST_REGISTER("test-packed-edit-field-tombstone-multi", test_packed_edit_field_tombstone_multi)
TEST_REGISTER("test-packed-compact", test_packed_compact)
TEST_REGISTER("test-compact-removed-literal", test_compact_removed_literal)
TEST_REGISTER("test-removed-literal-remove-rename-edit", test_removed_literal_remove_rename_edit)
