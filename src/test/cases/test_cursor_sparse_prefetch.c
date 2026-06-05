/* Sparse bitmap prefetch heuristic: for bitmap seeds, the walk cost scales
 * as want × N² / K² (sparsity factor N/K) instead of want × N / K. The
 * crossover is K³ < want × N². In the test hook limit=0 so want=1, giving
 * crossover K³ < N². e.g. type=rare k=700 out of N=20000: K³=343M < 400M=N²
 * → sort. Without this, sparse bitmap seeds incorrectly choose walk, taking
 * 20s+ scanning past non-matching order-index entries.
 *
 * Strategy: use OP_EQUAL on a selective-but-sparse bitmap value with NO
 * covering composite to exercise the D2/D3 fork. The quadratic formula
 * (k²=490K > want*N=20K) says walk; the sparsity formula (k³=343M <
 * want*N²=400M) overrides to sort. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* Plan-path introspection hook. */
extern const char *plan_filter_kind_for_test(
    const char *db_root, const char *object,
    const char *criteria_json, const char *order_by, int fetching,
    char *out_field, size_t fsz, char *out_order, size_t osz,
    int *out_total_cheap);

static const char *order_of(TestEnv *env, const char *obj,
                            const char *crit, const char *order_by) {
    static char order[32];
    char field[64] = {0}; int cheap = -1;
    order[0] = '\0';
    plan_filter_kind_for_test(env->db_root, obj, crit, order_by, 1,
                              field, sizeof(field), order, sizeof(order), &cheap);
    return order;
}

static int test_cursor_sparse_prefetch(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"sp\"}", &resp); free(resp); resp=NULL;

    /* type: bitmap; score: single btree (drivable order_by). NO type+score
     * composite — OP_EQUAL on type must fall through to the D2/D3 fork. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"sp\",\"object\":\"spobj\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:varchar:10\",\"score:int\"],"
        "\"indexes\":[\"type:bitmap\",\"score\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create spobj"); free(resp); resp=NULL;

    /* N=20000 total, with K=700 "rare" making the bitmap seed sparse in
       the order-by index (test hook uses limit=0 → want=1). The quadratic:
         K² = 490000  >  want*N = 1*20000 = 20000  → walk
       But the sparsity formula:
         K³ = 343000000  <  want*N² = 1*400000000 = 400000000  → sort */
    int N = 20000;
    int rare_K = 700;
    for (int i = 0; i < N; i++) {
        char req[256];
        const char *type = (i < rare_K) ? "rare" : "common";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"sp\",\"object\":\"spobj\",\"key\":\"k%05d\","
            "\"value\":{\"type\":\"%s\",\"score\":%d}}", i, type, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    /* SPARSE BITMAP: type=rare (K=700) with order_by score.
     * Quadratic formula says walk, but K³ < want*N² overrides to sort. */
    ASSERT_EQ_STR(order_of(&env, "sp/spobj",
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"rare\"}]", "score"),
        "sort",
        "sparse bitmap type=rare + order score → sort (sparsity override)");

    /* BROAD BITMAP: type=common (K=19300) with order_by score.
     * K³ ≈ 7.2T > want*N² = 400M → walk (sparsity formula agrees with quadratic). */
    ASSERT_EQ_STR(order_of(&env, "sp/spobj",
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"common\"}]", "score"),
        "walk",
        "broad bitmap type=common + order score → walk (no sparsity)");

    /* End-to-end correctness: find top-2 rare by score desc (highest scores
       are the last-inserted rare rows: scores 699, 698, ...) */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"sp\",\"object\":\"spobj\","
        "\"criteria\":[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"rare\"}],"
        "\"order_by\":\"score\",\"order\":\"desc\",\"limit\":2}", &resp);
    ASSERT_CONTAINS(resp, "\"score\":699", "top rare score 699");
    ASSERT_CONTAINS(resp, "\"score\":698", "2nd rare score 698");
    ASSERT_EQ_INT(
        (int)(strstr(resp, "\"score\":699") ? 1 : 0) +
        (int)(strstr(resp, "\"score\":698") ? 1 : 0), 2,
        "top-2 rare scores present");
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-cursor-sparse-prefetch", test_cursor_sparse_prefetch)
