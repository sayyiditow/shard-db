/* Exact aggregate states against the independent oracle: random + literal
   vectors under every partition/merge order, overflow, non-finite, budget,
   allocation faults, comparison, rendering and ordered-key decoding. */
#include "test_runner.h"
#include "test_assert.h"
#include "../db/agg_exact.h"
#include "../db/decimal_order.h"
#include "agg_exact_vectors.h"
#include <errno.h>
#include <float.h>
#include <stdlib.h>
#include <string.h>

static agg_i128 ae_raw(const char *t) {           /* "-12.50" -> -1250 */
    agg_i128 v = 0; int neg = 0;
    for (const char *p = t; *p; p++) {
        if (*p == '-') neg = 1;
        else if (*p != '.') v = v * 10 + (*p - '0');
    }
    return neg ? -v : v;
}

static void ae_render(const AggNum *n, const AggNumSpec *sp, enum AggFn fn,
                      int64_t cnt, char *out, size_t cap) {
    AggValue v;
    ASSERT_EQ_INT(agg_num_value(n, sp, fn, cnt, &v), 0, "value");
    char *t = agg_value_render(&v);
    ASSERT_NOT_NULL(t, "render");
    snprintf(out, cap, "%s", t ? t : "");
    free(t);
    agg_value_release(&v);
}

static int ae_vectors(void) {
    size_t nv = sizeof AGG_VECS / sizeof *AGG_VECS;
    static const int parts_list[] = { 1, 2, 3, 5 };
    for (size_t vi = 0; vi < nv; vi++) {
        const typeof(AGG_VECS[0]) *V = &AGG_VECS[vi];
        AggNumSpec sp = { .cls = V->cls == 'I' ? AGC_INT : AGC_DEC,
                          .scale = V->cls == 'I' ? V->scale : 0 };
        for (size_t pi = 0; pi < sizeof parts_list / sizeof *parts_list; pi++) {
            int parts = parts_list[pi];
            for (int reverse = 0; reverse < 2; reverse++) {
                _Atomic size_t bytes = 0;
                AggEnv env; agg_env_init(&env, &bytes, (size_t)1 << 30);
                AggNum num[5]; int64_t cnt[5];
                memset(num, 0, sizeof num); memset(cnt, 0, sizeof cnt);
                for (int i = 0; i < V->n; i++) {
                    int p = i % parts;
                    if (V->cls == 'I') {
                        ASSERT_EQ_INT(agg_num_offer_int(&num[p], ae_raw(V->v[i])), 0, "offer int");
                    } else {
                        dec_t d; dec_init(&d);
                        ASSERT_EQ_INT(dec_parse(V->v[i], strlen(V->v[i]), &d, NULL, 0), DEC_OK, "parse");
                        ASSERT_EQ_INT(agg_num_offer_decimal(&num[p], &env, &d), 0, "offer dec");
                        dec_free(&d);
                    }
                    cnt[p]++;
                }
                AggNum total; int64_t tc = 0; memset(&total, 0, sizeof total);
                for (int k = 0; k < parts; k++) {
                    int p = reverse ? parts - 1 - k : k;
                    ASSERT_EQ_INT(agg_num_merge(&total, &num[p], sp.cls, &env), 0, "merge");
                    tc += cnt[p];
                    agg_num_release(&num[p], sp.cls, &env);
                }
                char got[512];
                ae_render(&total, &sp, AGG_SUM, tc, got, sizeof got);
                ASSERT_EQ_STR(got, V->sum, "sum");
                ae_render(&total, &sp, AGG_AVG, tc, got, sizeof got);
                ASSERT_EQ_STR(got, V->avg, "avg");
                ae_render(&total, &sp, AGG_MIN, tc, got, sizeof got);
                ASSERT_EQ_STR(got, V->mn, "min");
                ae_render(&total, &sp, AGG_MAX, tc, got, sizeof got);
                ASSERT_EQ_STR(got, V->mx, "max");
                agg_num_release(&total, sp.cls, &env);
                ASSERT_EQ_INT((long)atomic_load(&bytes), 0, "budget returns to zero (no leak)");
                agg_env_free(&env);
            }
        }
    }
    return 0;
}

static int ae_limits(void) {
    AggNum n; memset(&n, 0, sizeof n);
    agg_i128 big = ((agg_i128)1 << 126);
    ASSERT_EQ_INT(agg_num_offer_int(&n, big), 0, "2^126 fits");
    errno = 0;
    ASSERT_EQ_INT(agg_num_offer_int(&n, big), -1, "overflow is an error");
    ASSERT_EQ_INT(errno, ERANGE, "ERANGE");

    AggNum f; memset(&f, 0, sizeof f);
    ASSERT_EQ_INT(agg_num_offer_double(&f, DBL_MAX), 0, "finite");
    ASSERT_EQ_INT(agg_num_offer_double(&f, DBL_MAX), 0, "finite input: defer unused sum overflow");
    AggNumSpec fsp = { .cls = AGC_FLT, .type = FT_DOUBLE };
    AggValue fv;
    ASSERT_EQ_INT(agg_num_value(&f, &fsp, AGG_MIN, 2, &fv), 0, "min survives sum overflow");
    ASSERT_TRUE(fv.f == DBL_MAX, "exact finite min"); agg_value_release(&fv);
    ASSERT_EQ_INT(agg_num_value(&f, &fsp, AGG_MAX, 2, &fv), 0, "max survives sum overflow");
    ASSERT_TRUE(fv.f == DBL_MAX, "exact finite max"); agg_value_release(&fv);
    errno = 0;
    ASSERT_EQ_INT(agg_num_value(&f, &fsp, AGG_SUM, 2, &fv), -1, "requested sum rejects overflow");
    ASSERT_EQ_INT(errno, ERANGE, "ERANGE sum"); agg_value_release(&fv);
    errno = 0;
    ASSERT_EQ_INT(agg_num_value(&f, &fsp, AGG_AVG, 2, &fv), -1, "requested avg rejects overflow");
    ASSERT_EQ_INT(errno, ERANGE, "ERANGE avg"); agg_value_release(&fv);
    AggNum fa = {0}, fb = {0};
    ASSERT_EQ_INT(agg_num_offer_double(&fa, DBL_MAX), 0, "merge left");
    ASSERT_EQ_INT(agg_num_offer_double(&fb, DBL_MAX), 0, "merge right");
    ASSERT_EQ_INT(agg_num_merge(&fa, &fb, AGC_FLT, NULL), 0, "merge defers sum overflow");
    ASSERT_EQ_INT(agg_num_value(&fa, &fsp, AGG_MIN, 2, &fv), 0, "merged min survives");
    ASSERT_TRUE(fv.f == DBL_MAX, "merged min"); agg_value_release(&fv);
    errno = 0;
    ASSERT_EQ_INT(agg_num_value(&fa, &fsp, AGG_SUM, 2, &fv), -1, "merged requested sum fails");
    ASSERT_EQ_INT(errno, ERANGE, "merged ERANGE"); agg_value_release(&fv);

    /* budget rejection leaves the counter untouched and the state valid */
    _Atomic size_t bytes = 0;
    AggEnv env; agg_env_init(&env, &bytes, 8);
    AggNum d; memset(&d, 0, sizeof d);
    dec_t v; dec_init(&v);
    ASSERT_EQ_INT(dec_parse("123456789012345678901234567890", 30, &v, NULL, 0), DEC_OK, "parse");
    errno = 0;
    ASSERT_EQ_INT(agg_num_offer_decimal(&d, &env, &v), -1, "budget rejects");
    ASSERT_EQ_INT(errno, ENOBUFS, "ENOBUFS");
    ASSERT_EQ_INT((long)atomic_load(&bytes), 0, "no charge leaked");
    agg_num_release(&d, AGC_DEC, &env);
    agg_env_free(&env);

    /* allocation-fault injection: Nth guarded allocation fails, state intact */
    agg_env_init(&env, NULL, 0);
    memset(&d, 0, sizeof d);
    agg_exact_test_set_fail_alloc(1);
    errno = 0;
    ASSERT_EQ_INT(agg_num_offer_decimal(&d, &env, &v), -1, "alloc fault");
    ASSERT_EQ_INT(errno, ENOMEM, "ENOMEM");
    agg_exact_test_set_fail_alloc(0);
    ASSERT_EQ_INT(agg_num_offer_decimal(&d, &env, &v), 0, "recovers after fault");
    agg_num_release(&d, AGC_DEC, &env);
    agg_env_free(&env);
    dec_free(&v);
    return 0;
}

static int ae_compare_render(void) {
    /* exact comparison to numeric literals; scale-insensitive for decimals */
    AggValue a; agg_value_init(&a);
    a.cls = AGC_INT; a.present = 1; a.i = 1250; a.scale = 2;          /* 12.50 */
    int c = 9;
    ASSERT_EQ_INT(agg_value_cmp_text(&a, "12.5", &c), 0, "cmp ok");
    ASSERT_EQ_INT(c, 0, "12.50 == 12.5");
    ASSERT_EQ_INT(agg_value_cmp_text(&a, "12.51", &c), 0, "cmp ok");
    ASSERT_EQ_INT(c, -1, "12.50 < 12.51");
    errno = 0;
    ASSERT_EQ_INT(agg_value_cmp_text(&a, "abc", &c), -1, "non-number rejected");
    ASSERT_EQ_INT(errno, EINVAL, "EINVAL");
    /* adjacent values above 2^53 stay distinct */
    agg_value_init(&a);
    a.cls = AGC_INT; a.present = 1; a.i = 9007199254740993LL;
    ASSERT_EQ_INT(agg_value_cmp_text(&a, "9007199254740992", &c), 0, "cmp ok");
    ASSERT_EQ_INT(c, 1, "…993 > …992 exactly");
    /* shortest round-trip floats */
    char *t = agg_render_double(0.1);
    ASSERT_EQ_STR(t, "0.1", "0.1"); free(t);
    t = agg_render_double(0.30000000000000004);
    ASSERT_EQ_STR(t, "0.30000000000000004", "17 digits when needed"); free(t);
    errno = 0;
    ASSERT_TRUE(agg_render_double(1.0 / 0.0) == NULL && errno == ERANGE, "inf is an error");
    return 0;
}

static int ae_keys_labels(void) {
    /* ordered decimal key: tier 0 + decimal_order payload */
    AggNumSpec bounded = { .cls = AGC_DEC, .dscale = 4, .type = FT_NUMERIC };
    AggNumSpec bare = { .cls = AGC_DEC, .bare = 1, .type = FT_NUMERIC };
    uint8_t key[600]; size_t pl = 0;
    dec_t d; dec_init(&d);
    ASSERT_EQ_INT(dec_parse("2", 1, &d, NULL, 0), DEC_OK, "parse");
    ASSERT_EQ_INT(decimal_order_into(&d, key + 1, sizeof key - 1, &pl), 0, "encode");
    key[0] = 0x00;
    AggEnv env; agg_env_init(&env, NULL, 0);
    AggNum n; memset(&n, 0, sizeof n);
    errno = 0;
    ASSERT_EQ_INT(agg_num_offer_key(&bare, key, pl + 1, &n, &env), -1, "bare key value use is gated");
    ASSERT_EQ_INT(errno, ENOTSUP, "ENOTSUP");
    ASSERT_EQ_INT(agg_num_offer_key(&bounded, key, pl + 1, &n, &env), 1, "bounded decodes");
    char got[64];
    ae_render(&n, &bounded, AGG_MAX, 1, got, sizeof got);
    ASSERT_EQ_STR(got, "2.0000", "bounded key renders at the declared scale");
    key[0] = 0x01;
    ASSERT_EQ_INT(agg_num_offer_key(&bounded, key, pl + 1, &n, &env), 0, "NULL tier skipped");
    agg_num_release(&n, AGC_DEC, &env);

    char label[64];
    int len = agg_dec_label(&bounded, key + 1, pl, 1, label, sizeof label, &env);
    ASSERT_EQ_INT(len, 6, "length"); ASSERT_EQ_STR(label, "2.0000", "bounded label");
    len = agg_dec_label(&bare, key + 1, pl, 1, label, sizeof label, &env);
    ASSERT_EQ_INT(len, 1, "length"); ASSERT_EQ_STR(label, "2", "bare label is canonical");
    ASSERT_EQ_INT((int)strlen(label), len, "returned length equals strlen");
    errno = 0;
    ASSERT_EQ_INT(agg_dec_label(&bounded, key + 1, pl, 1, label, 3, &env), -1, "small buffer");
    ASSERT_EQ_INT(errno, ENAMETOOLONG, "ENAMETOOLONG");
    agg_env_free(&env);
    dec_free(&d);
    return 0;
}

/* AggEnv scratch contract: env.val is a worker-private dec_t that
   agg_env_free must release and reset — a freed env carries no digits.
   (The WFC worker call site frees each warg's env; leaks there are only
   LSan-visible and are covered by the branch's sanitizer gate.) */
static int ae_env_contract(void) {
    AggEnv env;
    agg_env_init(&env, NULL, 0);
    ASSERT_TRUE(env.val.digits == NULL, "env scratch starts zeroed");
    /* Parse straight into the scratch: allocates env.val's digit storage
       the way the worker-path decode does. */
    ASSERT_EQ_INT(dec_parse("123.45", 6, &env.val, NULL, 0), DEC_OK,
                  "parse into env scratch");
    ASSERT_TRUE(env.val.digits != NULL, "scratch holds digits after use");
    agg_env_free(&env);
    ASSERT_TRUE(env.val.digits == NULL, "agg_env_free releases and resets env.val");
    /* double free must be safe (workers may be torn down twice on early exit) */
    agg_env_free(&env);
    return 0;
}

/* Sort-direction state is per-row (no file-static shared across queries):
   the TEST_BUILD seam sorts two arrays with opposite directions back to
   back through the real comparator. */
static int ae_sort_dirs(void) {
    extern int agg_test_sort_dirs(void) __attribute__((weak));
    if (!agg_test_sort_dirs) return 0;   /* seam absent: nothing to assert */
    ASSERT_EQ_INT(agg_test_sort_dirs(), 0, "per-row sort direction");
    return 0;
}

static int ae_run(void) {
    return ae_vectors() | ae_limits() | ae_compare_render() | ae_keys_labels() |
           ae_env_contract() | ae_sort_dirs();
}
TEST_REGISTER("test-agg-exact-core", ae_run)
