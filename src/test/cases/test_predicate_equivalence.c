/* R6: count, find and a C-computed oracle must agree for mixed bitmap +
   btree-decimal criteria on a packed object (primary-leaf, intersect, keyset
   union and scan routes). Guards the R2 review items: candidate verification
   after a bitmap/btree primary, and candidate caps before verification. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PE_N 4000

typedef struct { long long amt; int flag; int kind; int qty; int live; } PeRow; /* amt scaled by 1e4 */
static PeRow pe_rows[PE_N];
static const char *const pe_kind[] = { "a", "b", "c" };

typedef int (*PeMatch)(const PeRow *);
static int pe_m_and(const PeRow *r)   { return r->flag && r->amt >= 1000000 && r->amt <= 2500000; }
static int pe_m_or(const PeRow *r)    { return r->flag || r->amt > 3500000; }
static int pe_m_neq(const PeRow *r)   { return r->flag && r->amt != 1234500; }
static int pe_m_kind(const PeRow *r)  { return r->kind == 1 && r->flag && r->qty < 5; }
static int pe_m_none(const PeRow *r)  { return r->flag && r->amt < -9000000; }
static int pe_m_orand(const PeRow *r) { return (r->flag || r->kind == 2) && r->amt >= 0 && r->amt < 500000; }

static int pe_oracle(PeMatch m) {
    int n = 0;
    for (int i = 0; i < PE_N; i++) n += (pe_rows[i].live && m(&pe_rows[i])) ? 1 : 0;
    return n;
}

static int pe_find_total(TestClient *tc, const char *criteria) {
    int total = 0;
    for (int off = 0;; off += 1000) {
        char req[1024];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"pe\",\"object\":\"t\",\"criteria\":%s,"
            "\"offset\":%d,\"limit\":1000,\"fields\":[\"qty\"]}", criteria, off);
        char *r = NULL;
        if (tc_request(tc, req, &r) != 0 || !r || strstr(r, "\"error\"")) { free(r); return -1; }
        int got = 0;
        for (const char *p = r; (p = strstr(p, "\"key\":\"")) != NULL; p += 7) got++;
        free(r);
        total += got;
        if (got < 1000) break;
    }
    return total;
}

static void pe_check(TestClient *tc, const char *name, const char *criteria, PeMatch m) {
    char req[1024], desc[160];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"pe\",\"object\":\"t\",\"criteria\":%s}", criteria);
    char *r = NULL;
    int rc = tc_request(tc, req, &r);
    int counted = rc == 0 ? tu_parse_count(r) : -1;
    free(r);
    int want = pe_oracle(m);
    snprintf(desc, sizeof(desc), "%s: count equals the oracle (%d)", name, want);
    ASSERT_EQ_INT(counted, want, desc);
    snprintf(desc, sizeof(desc), "%s: find returns exactly the oracle rows", name);
    ASSERT_EQ_INT(pe_find_total(tc, criteria), want, desc);
}

static void pe_run_all(TestClient *tc, const char *phase) {
    char tag[96];
#define PE_CASE(label, crit, fn) \
    do { snprintf(tag, sizeof(tag), "%s/%s", phase, label); pe_check(tc, tag, crit, fn); } while (0)
    PE_CASE("bitmap AND decimal between",
        "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"amt\",\"op\":\"between\",\"value\":[\"100.0000\",\"250.0000\"]}]", pe_m_and);
    PE_CASE("bitmap OR decimal gt",
        "[{\"or\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"amt\",\"op\":\"gt\",\"value\":\"350.0000\"}]}]", pe_m_or);
    PE_CASE("bitmap AND decimal neq",
        "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"amt\",\"op\":\"neq\",\"value\":\"123.4500\"}]", pe_m_neq);
    PE_CASE("enum bitmap AND bool bitmap AND int range",
        "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"},"
        "{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"qty\",\"op\":\"lt\",\"value\":\"5\"}]", pe_m_kind);
    PE_CASE("empty result (no row qualifies)",
        "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"-900.0000\"}]", pe_m_none);
    PE_CASE("OR sub-tree under AND",
        "[{\"or\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
        "{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"c\"}]},"
        "{\"field\":\"amt\",\"op\":\"gte\",\"value\":\"0\"},"
        "{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"50.0000\"}]", pe_m_orand);
#undef PE_CASE
}

static int test_predicate_equivalence_run(void) {
    for (int i = 0; i < PE_N; i++) {
        int n = i + 1;
        pe_rows[i].amt = ((long long)(n * 7919) % 8000000) - 4000000; /* -400..400 at 4 dp */
        pe_rows[i].flag = (n % 3 == 0) || (n % 7 == 0);
        pe_rows[i].kind = n % 3;
        pe_rows[i].qty = n % 11;
        pe_rows[i].live = 1;
    }
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    ASSERT_EQ_INT(test_env_ctl_set_kf_seams(&env, 64, 0), 0, "shrink the kf tier to force growth");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *r = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"pe\"}", &r);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "add-dir");
    free(r); r = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"pe\",\"object\":\"t\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"amt:numeric:18,4\",\"flag:bool\",\"kind:enum(a,b,c)\",\"qty:int\"],"
        "\"indexes\":[\"amt\",\"flag:bitmap\",\"kind:bitmap\",\"qty\"]}", &r);
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create object");
    free(r);
    for (int base = 0; base < PE_N; base += 1000) {
        size_t cap = 1000 * 160 + 256;
        char *bulk = malloc(cap);
        ASSERT_NOT_NULL(bulk, "bulk buffer");
        if (!bulk) { tc_close(tc); test_env_stop(&env); return 1; }
        size_t len = (size_t)snprintf(bulk, cap,
            "{\"mode\":\"bulk-insert\",\"dir\":\"pe\",\"object\":\"t\",\"records\":[");
        for (int j = 0; j < 1000; j++) {
            const PeRow *row = &pe_rows[base + j];
            long long a = row->amt < 0 ? -row->amt : row->amt;
            len += (size_t)snprintf(bulk + len, cap - len,
                "%s{\"key\":\"e%05d\",\"value\":{\"amt\":\"%s%lld.%04lld\",\"flag\":%s,"
                "\"kind\":\"%s\",\"qty\":%d}}", j ? "," : "", base + j + 1,
                row->amt < 0 ? "-" : "", a / 10000, a % 10000,
                row->flag ? "true" : "false", pe_kind[row->kind], row->qty);
        }
        snprintf(bulk + len, cap - len, "]}");
        r = NULL;
        tc_request(tc, bulk, &r);
        ASSERT_TRUE(r && !strstr(r, "\"error\""), "bulk-insert chunk");
        free(r);
        free(bulk);
    }
    pe_run_all(tc, "loaded");

    /* delete a slice; every route must still agree with the oracle */
    char del[4096];
    size_t dl = (size_t)snprintf(del, sizeof(del),
        "{\"mode\":\"bulk-delete\",\"dir\":\"pe\",\"object\":\"t\",\"keys\":[");
    for (int i = 0; i < 300; i++) {
        int idx = i * 7 + 1;                       /* deterministic spread */
        dl += (size_t)snprintf(del + dl, sizeof(del) - dl, "%s\"e%05d\"", i ? "," : "", idx);
        pe_rows[idx - 1].live = 0;
    }
    snprintf(del + dl, sizeof(del) - dl, "]}");
    r = NULL;
    tc_request(tc, del, &r);
    ASSERT_CONTAINS(r, "\"deleted\":300", "bulk-delete removed the slice");
    free(r);
    pe_run_all(tc, "after-delete");
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-predicate-equivalence", test_predicate_equivalence_run)
