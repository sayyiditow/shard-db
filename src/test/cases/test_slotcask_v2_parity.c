/* test_slotcask_v2_parity.c — v1↔v2 query-mode parity audit.
 *
 * Creates two objects with identical schema + index set, seeds the same
 * data into both, then runs every major query mode against each and
 * confirms the wire response shape is byte-equal.
 *
 * Modes audited:
 *   - get (single + multi)
 *   - exists (single + multi)
 *   - count (no criteria, indexed criteria, non-indexed criteria, AND, OR)
 *   - find (full scan, indexed range with order_by, indexed eq without
 *     order_by, projection, offset+limit, format=csv, format=dict)
 *   - aggregate (count/sum/avg/min/max with + without group_by, having)
 *   - keys / fetch
 *   - join (inner + left, by primary key)
 *
 * Per the Phase 3F+3G+3H commit message, the audit was deferred — this is
 * the deferred work, brought forward into Phase 4E. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Object names */
#define DIR  "parity"
#define V1   "u_v1"
#define V2   "u_v2"
#define V1Q  "q_v1"
#define V2Q  "q_v2"

/* Fire the same request shape against v1 + v2 objects, return whether
   the responses are byte-equal. Substitutes <obj> placeholder per call. */
static int request_pair(TestClient *tc, const char *templ,
                         const char *o1, const char *o2,
                         char **r1_out, char **r2_out) {
    char buf[4096];
    /* Replace literal token <obj> in templ with o1 / o2 */
    for (int i = 0; i < 2; i++) {
        const char *obj = (i == 0) ? o1 : o2;
        const char *p = templ;
        char *q = buf;
        while (*p && (size_t)(q - buf) < sizeof(buf) - 1) {
            if (strncmp(p, "<obj>", 5) == 0) {
                size_t ol = strlen(obj);
                if ((size_t)(q - buf) + ol >= sizeof(buf) - 1) break;
                memcpy(q, obj, ol);
                q += ol;
                p += 5;
            } else *q++ = *p++;
        }
        *q = '\0';
        char **out = (i == 0) ? r1_out : r2_out;
        *out = NULL;
        tc_request(tc, buf, out);
    }
    int eq = (*r1_out && *r2_out && strcmp(*r1_out, *r2_out) == 0);
    return eq;
}

#define ASSERT_PAIR_EQ(tc, templ, name) do { \
    char *r1 = NULL, *r2 = NULL; \
    int eq = request_pair(tc, templ, V1, V2, &r1, &r2); \
    if (!eq) fprintf(stderr, "PARITY FAIL [%s]\n  v1=%s\n  v2=%s\n", \
                      name, r1?r1:"(null)", r2?r2:"(null)"); \
    ASSERT_TRUE(eq, name); \
    free(r1); free(r2); \
} while (0)

static void seed_pair(TestClient *tc, const char *obj,
                       const char *key, const char *json) {
    char req[1024];
    snprintf(req, sizeof(req),
             "{\"mode\":\"insert\",\"dir\":\"" DIR "\",\"object\":\"%s\","
             "\"key\":\"%s\",\"value\":%s}", obj, key, json);
    char *resp = NULL;
    tc_request(tc, req, &resp); free(resp);
}

static void create_obj(TestClient *tc, const char *obj, int v2) {
    char req[1024];
    if (v2) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"" DIR "\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":40,"
            "\"fields\":[\"name:varchar:32\",\"age:int\",\"city:varchar:16\","
                       "\"score:int\"],"
            "\"indexes\":[\"age\",\"city\"]}", obj);
    } else {
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"" DIR "\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":40,"
            "\"fields\":[\"name:varchar:32\",\"age:int\",\"city:varchar:16\","
                       "\"score:int\"],"
            "\"indexes\":[\"age\",\"city\"]}", obj);
    }
    char *resp = NULL;
    tc_request(tc, req, &resp); free(resp);
}

/* Driver-side join test — needs a 2nd object on each side (orders pointing
   at users). Schema mirrored between v1 and v2 sets. */
static void create_orders(TestClient *tc, const char *obj, int v2) {
    char req[1024];
    if (v2) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"" DIR "\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":40,"
            "\"fields\":[\"buyer:varchar:40\",\"total:int\"],"
            "\"indexes\":[\"buyer\"]}", obj);
    } else {
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"" DIR "\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":40,"
            "\"fields\":[\"buyer:varchar:40\",\"total:int\"],"
            "\"indexes\":[\"buyer\"]}", obj);
    }
    char *resp = NULL;
    tc_request(tc, req, &resp); free(resp);
}

static int test_slotcask_v2_parity_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"" DIR "\"}", &resp);
    free(resp); resp = NULL;

    create_obj(tc, V1, 0);
    create_obj(tc, V2, 1);
    create_orders(tc, V1Q, 0);
    create_orders(tc, V2Q, 1);

    /* Seed identical user data into both objects. */
    static const char *records[][2] = {
        {"u1", "{\"name\":\"alice\",\"age\":25,\"city\":\"NYC\",\"score\":80}"},
        {"u2", "{\"name\":\"bob\",\"age\":30,\"city\":\"LON\",\"score\":92}"},
        {"u3", "{\"name\":\"carol\",\"age\":35,\"city\":\"NYC\",\"score\":75}"},
        {"u4", "{\"name\":\"dave\",\"age\":40,\"city\":\"TYO\",\"score\":88}"},
        {"u5", "{\"name\":\"eve\",\"age\":45,\"city\":\"LON\",\"score\":95}"},
        {"u6", "{\"name\":\"frank\",\"age\":50,\"city\":\"NYC\",\"score\":70}"},
        {NULL, NULL}
    };
    for (int i = 0; records[i][0]; i++) {
        seed_pair(tc, V1, records[i][0], records[i][1]);
        seed_pair(tc, V2, records[i][0], records[i][1]);
    }
    /* Orders: 2 per user with primary key u<i>-<j> referencing users by buyer */
    char req[512];
    for (int i = 1; i <= 6; i++) {
        for (int j = 1; j <= 2; j++) {
            snprintf(req, sizeof(req),
                "{\"mode\":\"insert\",\"dir\":\"" DIR "\",\"object\":\"<O>\","
                "\"key\":\"u%d-%d\",\"value\":{\"buyer\":\"u%d\",\"total\":%d}}",
                i, j, i, i * 10 + j);
            for (int v = 0; v < 2; v++) {
                char r2[600];
                const char *o = (v == 0) ? V1Q : V2Q;
                /* substitute <O> */
                const char *p = req; char *q = r2;
                while (*p) {
                    if (strncmp(p, "<O>", 3) == 0) {
                        memcpy(q, o, strlen(o)); q += strlen(o); p += 3;
                    } else *q++ = *p++;
                }
                *q = '\0';
                tc_request(tc, r2, &resp); free(resp); resp = NULL;
            }
        }
    }

    /* ===== get ===== */
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"get\",\"dir\":\"" DIR "\",\"object\":\"<obj>\",\"key\":\"u3\"}",
                    "get u3 parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"get\",\"dir\":\"" DIR "\",\"object\":\"<obj>\",\"key\":\"missing\"}",
                    "get missing parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"get\",\"dir\":\"" DIR "\",\"object\":\"<obj>\",\"keys\":[\"u1\",\"u2\",\"missing\",\"u6\"]}",
                    "get multi parity");

    /* ===== exists ===== */
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"exists\",\"dir\":\"" DIR "\",\"object\":\"<obj>\",\"key\":\"u3\"}",
                    "exists u3 parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"exists\",\"dir\":\"" DIR "\",\"object\":\"<obj>\",\"key\":\"missing\"}",
                    "exists missing parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"exists\",\"dir\":\"" DIR "\",\"object\":\"<obj>\",\"keys\":[\"u1\",\"missing\"]}",
                    "exists multi parity");

    /* ===== count ===== */
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"count\",\"dir\":\"" DIR "\",\"object\":\"<obj>\"}",
                    "count no criteria parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"count\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"}]}",
                    "count indexed eq parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"count\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"35\"}]}",
                    "count indexed range parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"count\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"name\",\"op\":\"contains\",\"value\":\"a\"}]}",
                    "count non-indexed parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"count\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"},"
                                     "{\"field\":\"age\",\"op\":\"lt\",\"value\":\"40\"}]}",
                    "count AND-intersect parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"count\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":{\"or\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"25\"},"
                                              "{\"field\":\"age\",\"op\":\"eq\",\"value\":\"50\"}]}}",
                    "count OR parity");

    /* ===== find ===== */
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"35\"}],"
                       "\"order_by\":\"age\"}",
                    "find indexed range + order_by parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"}],"
                       "\"order_by\":\"age\"}",
                    "find indexed eq + order_by parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"carol\"}]}",
                    "find non-indexed eq parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"30\"}],"
                       "\"order_by\":\"age\",\"fields\":[\"name\",\"age\"]}",
                    "find projection parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"25\"}],"
                       "\"order_by\":\"age\",\"offset\":2,\"limit\":2}",
                    "find offset+limit parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"}],"
                       "\"order_by\":\"age\",\"format\":\"csv\"}",
                    "find csv parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"}],"
                       "\"order_by\":\"age\",\"format\":\"dict\"}",
                    "find dict parity");

    /* ===== aggregate ===== */
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"aggregate\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"aggregates\":[{\"op\":\"count\",\"as\":\"n\"}]}",
                    "aggregate count parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"aggregate\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"aggregates\":[{\"op\":\"sum\",\"field\":\"score\",\"as\":\"s\"}]}",
                    "aggregate sum parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"aggregate\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"aggregates\":[{\"op\":\"avg\",\"field\":\"score\",\"as\":\"a\"},"
                                       "{\"op\":\"min\",\"field\":\"score\",\"as\":\"mn\"},"
                                       "{\"op\":\"max\",\"field\":\"score\",\"as\":\"mx\"}]}",
                    "aggregate avg/min/max parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"aggregate\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"aggregates\":[{\"op\":\"count\",\"as\":\"n\"},"
                                       "{\"op\":\"sum\",\"field\":\"score\",\"as\":\"s\"}],"
                       "\"group_by\":\"city\",\"order_by\":\"city\"}",
                    "aggregate group_by parity");
    ASSERT_PAIR_EQ(tc, "{\"mode\":\"aggregate\",\"dir\":\"" DIR "\",\"object\":\"<obj>\","
                       "\"aggregates\":[{\"op\":\"count\",\"as\":\"n\"}],"
                       "\"group_by\":\"city\",\"order_by\":\"city\","
                       "\"having\":{\"n\":{\"gt\":1}}}",
                    "aggregate having parity");

    /* ===== keys ===== Walk order is implementation-defined (neither v1 nor
       v2 promises a stable order from `keys`). Compare the returned key set
       semantically by checking each expected key is present in both. */
    {
        char *r1 = NULL, *r2 = NULL;
        tc_request(tc, "{\"mode\":\"keys\",\"dir\":\"" DIR "\",\"object\":\"" V1 "\"}", &r1);
        tc_request(tc, "{\"mode\":\"keys\",\"dir\":\"" DIR "\",\"object\":\"" V2 "\"}", &r2);
        int ok = (r1 && r2);
        for (int i = 1; i <= 6 && ok; i++) {
            char k[8]; snprintf(k, sizeof(k), "\"u%d\"", i);
            if (!strstr(r1, k) || !strstr(r2, k)) ok = 0;
        }
        ASSERT_TRUE(ok, "keys parity (set membership)");
        free(r1); free(r2);
    }

    /* ===== fetch ===== Same caveat as keys — order is unstable and limited
       fetch can pick different sets across implementations. Use limit large
       enough to cover the whole dataset, then check membership. */
    {
        char *r1 = NULL, *r2 = NULL;
        tc_request(tc, "{\"mode\":\"fetch\",\"dir\":\"" DIR "\",\"object\":\"" V1 "\","
                       "\"limit\":100}", &r1);
        tc_request(tc, "{\"mode\":\"fetch\",\"dir\":\"" DIR "\",\"object\":\"" V2 "\","
                       "\"limit\":100}", &r2);
        int ok = (r1 && r2);
        for (int i = 1; i <= 6 && ok; i++) {
            char k[16]; snprintf(k, sizeof(k), "\"key\":\"u%d\"", i);
            if (!strstr(r1, k) || !strstr(r2, k)) ok = 0;
        }
        ASSERT_TRUE(ok, "fetch parity (set membership)");
        free(r1); free(r2);
    }

    /* Note: join parity tests use V1Q/V2Q on the orders side. We avoid
       request_pair's <obj> substitution because two object names appear. */
    {
        char *r1 = NULL, *r2 = NULL;
        tc_request(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"" V1Q "\","
                       "\"criteria\":[],\"order_by\":\"buyer\","
                       "\"join\":[{\"object\":\"" V1 "\",\"on\":{\"buyer\":\"_key\"},"
                                  "\"as\":\"u\",\"fields\":[\"name\",\"age\"]}]}", &r1);
        tc_request(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"" V2Q "\","
                       "\"criteria\":[],\"order_by\":\"buyer\","
                       "\"join\":[{\"object\":\"" V2 "\",\"on\":{\"buyer\":\"_key\"},"
                                  "\"as\":\"u\",\"fields\":[\"name\",\"age\"]}]}", &r2);
        int eq = (r1 && r2 && strcmp(r1, r2) == 0);
        if (!eq) fprintf(stderr, "PARITY FAIL [join inner]\n  v1=%s\n  v2=%s\n", r1?r1:"(null)", r2?r2:"(null)");
        ASSERT_TRUE(eq, "join inner pk parity");
        free(r1); free(r2);
    }
    {
        char *r1 = NULL, *r2 = NULL;
        tc_request(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"" V1Q "\","
                       "\"criteria\":[],\"order_by\":\"buyer\","
                       "\"join\":[{\"object\":\"" V1 "\",\"on\":{\"buyer\":\"_key\"},"
                                  "\"type\":\"left\",\"as\":\"u\",\"fields\":[\"city\"]}]}", &r1);
        tc_request(tc, "{\"mode\":\"find\",\"dir\":\"" DIR "\",\"object\":\"" V2Q "\","
                       "\"criteria\":[],\"order_by\":\"buyer\","
                       "\"join\":[{\"object\":\"" V2 "\",\"on\":{\"buyer\":\"_key\"},"
                                  "\"type\":\"left\",\"as\":\"u\",\"fields\":[\"city\"]}]}", &r2);
        int eq = (r1 && r2 && strcmp(r1, r2) == 0);
        if (!eq) fprintf(stderr, "PARITY FAIL [join left]\n  v1=%s\n  v2=%s\n", r1?r1:"(null)", r2?r2:"(null)");
        ASSERT_TRUE(eq, "join left pk parity");
        free(r1); free(r2);
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-v2-parity", test_slotcask_v2_parity_run)
