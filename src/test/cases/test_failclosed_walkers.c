/* Regression: index walkers that treated an unreadable shard as an empty
 * one. Every route below used to answer 200 with a short (or empty)
 * result when one shard of its index could not be opened; each must now
 * fail closed — an error object, or (once rows are on the wire) an
 * unterminated response — never a healthy-looking short answer.
 *
 *   1. Trigram contains: find / count / aggregate  (tg_posting_count,
 *      tg_intersect_streaming, the seed collect in build_keyset_from_trigram_b)
 *   2. Trigram starts_with find (A3 leading-gram walk)
 *   3. Full-leaf O_DIRECT scan (count with contains on a btree-only field)
 *   4. Ordered k-way walks: order_by + cursor find (btree_walk_ordered_ranges
 *      dropped a shard whose range-iterator open failed)
 *
 * Faults: the TEST_BUILD walk seam (.idx and .tg shard walks and the
 * O_DIRECT leaf scan) and the range-open seam (iterator opens). Shard 0
 * of index_splits_for(8) == 2 idx shards is armed; the 300-row fixture
 * spreads postings across both, so every route touches shard 0.
 *
 * Oracle: a healthy control query first, then the armed query must either
 * contain an error object or be unterminated (a closed short page can pass
 * for success). */
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

#define NROWS 300
#define OBJ "fcw"

/* A response fails closed when it ends in a complete error object (the last
   non-empty line starts with {"error"} — for finds that is the closed
   envelope followed by the error line — or when it is unterminated (no
   closing bracket/brace/number: a truncated body cannot pass for success).
   A response that merely CONTAINS "error" somewhere inside a body, or an
   error line glued to an open envelope, is not accepted. */
static int failed_closed(const char *resp) {
    if (!resp || !*resp) return 1;      /* connection dropped / nothing */
    size_t n = strlen(resp);
    while (n > 0 && (resp[n - 1] == '\n' || resp[n - 1] == ' ')) n--;
    if (n == 0) return 1;
    size_t ls = n;
    while (ls > 0 && resp[ls - 1] != '\n') ls--;
    if (n - ls >= 8 && strncmp(resp + ls, "{\"error\"", 8) == 0) {
        /* a complete error object; anything before it must itself be a
           closed envelope (or nothing) */
        if (ls == 0) return 1;
        size_t b = ls;
        while (b > 0 && (resp[b - 1] == '\n' || resp[b - 1] == ' ')) b--;
        char c = b ? resp[b - 1] : ']';
        return c == ']' || c == '}';
    }
    char last = resp[n - 1];
    return !(last == ']' || last == '}' || (last >= '0' && last <= '9'));
}

static char *req(TestClient *tc, const char *json) {
    char *resp = NULL;
    tc_request(tc, json, &resp);
    return resp;
}

static void check_route(TestClient *tc, TestEnv *env, int use_range_seam,
                        const char *name, const char *json) {
    char msg[200];
    char *healthy = req(tc, json);
    snprintf(msg, sizeof(msg), "%s: healthy control answers without error", name);
    ASSERT_TRUE(healthy && !strstr(healthy, "\"error\"") && *healthy, msg);
    free(healthy);

    if (use_range_seam) test_env_ctl_set_bt_range_open_fail(env, 0);
    else test_env_ctl_set_bt_walk_open_fail(env, 0);
    char *armed = req(tc, json);
    if (use_range_seam) test_env_ctl_set_bt_range_open_fail(env, -1);
    else test_env_ctl_set_bt_walk_open_fail(env, -1);

    snprintf(msg, sizeof(msg),
             "%s: shard open failure fails closed (resp=%.100s)", name,
             armed ? armed : "(null)");
    ASSERT_TRUE(failed_closed(armed), msg);
    free(armed);

    healthy = req(tc, json);
    snprintf(msg, sizeof(msg), "%s: seam disarmed, healthy again", name);
    ASSERT_TRUE(healthy && !strstr(healthy, "\"error\""), msg);
    free(healthy);
}


/* The range-open seam is one-shot and a query may open shard 0 several
   times (selectivity estimate, then the ordered walk). Try skip = 0..3: an
   armed run must be byte-identical to the healthy answer (the fault hit an
   estimate that is allowed to ignore it) or fail closed — never a closed,
   different (short) answer — and at least one skip must reach the walk. */
static void check_route_nth(TestClient *tc, TestEnv *env, const char *name,
                            const char *json) {
    char msg[200];
    char *healthy = req(tc, json);
    snprintf(msg, sizeof(msg), "%s: healthy control answers without error", name);
    ASSERT_TRUE(healthy && !strstr(healthy, "\"error\"") && *healthy, msg);
    int reached = 0;
    for (int skip = 0; skip < 4; skip++) {
        test_env_ctl_set_bt_range_open_fail_nth(env, 0, skip);
        char *armed = req(tc, json);
        test_env_ctl_set_bt_range_open_fail(env, -1);
        int closed_short = armed && !failed_closed(armed) && healthy &&
                           strcmp(armed, healthy) != 0;
        snprintf(msg, sizeof(msg),
                 "%s: skip=%d never a closed short answer (resp=%.80s)",
                 name, skip, armed ? armed : "(null)");
        ASSERT_TRUE(!closed_short, msg);
        if (failed_closed(armed)) reached = 1;
        free(armed);
    }
    snprintf(msg, sizeof(msg), "%s: some skip reached the walk and failed closed", name);
    ASSERT_TRUE(reached, msg);
    free(healthy);
}

static int test_failclosed_walkers_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = req(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    free(resp);
    resp = req(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"" OBJ
        "\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"s:varchar:24\",\"n:int\",\"z:varchar:16\"],"
        "\"indexes\":[\"s:trigram\",\"n\",\"z\"]}");
    ASSERT_CONTAINS(resp, "\"created\"", "object created");
    free(resp);

    size_t cap = (size_t)NROWS * 100 + 200;
    char *body = malloc(cap);
    ASSERT_NOT_NULL(body, "alloc body");
    if (!body) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t off = (size_t)snprintf(body, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"" OBJ
        "\",\"records\":[");
    for (int i = 0; i < NROWS; i++)
        off += (size_t)snprintf(body + off, cap - off,
            "%s{\"key\":\"k%03d\",\"value\":{\"s\":\"alpha%03d\","
            "\"n\":%d,\"z\":\"zed%03d\"}}", i ? "," : "", i, i, i, i);
    snprintf(body + off, cap - off, "]}");
    resp = req(tc, body);
    free(body);
    ASSERT_TRUE(resp && strstr(resp, "\"inserted\":300") != NULL,
                "fixture inserted");
    free(resp);

    check_route(tc, &env, 0, "trigram contains find",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"" OBJ "\","
        "\"criteria\":[{\"field\":\"s\",\"op\":\"contains\","
        "\"value\":\"lpha1\"}],\"limit\":50}");
    check_route(tc, &env, 0, "trigram contains count",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"" OBJ "\","
        "\"criteria\":[{\"field\":\"s\",\"op\":\"contains\","
        "\"value\":\"lpha1\"}]}");
    check_route(tc, &env, 0, "trigram contains aggregate",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"" OBJ "\","
        "\"criteria\":[{\"field\":\"s\",\"op\":\"contains\","
        "\"value\":\"lpha1\"}],\"aggregates\":[{\"fn\":\"sum\","
        "\"field\":\"n\",\"alias\":\"t\"}]}");
    check_route(tc, &env, 0, "trigram starts_with find",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"" OBJ "\","
        "\"criteria\":[{\"field\":\"s\",\"op\":\"starts_with\","
        "\"value\":\"alp\"}],\"limit\":50}");
    check_route(tc, &env, 0, "O_DIRECT full-leaf scan count (contains)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"" OBJ "\","
        "\"criteria\":[{\"field\":\"z\",\"op\":\"contains\","
        "\"value\":\"ed01\"}]}");
    check_route_nth(tc, &env, "ordered walk: order_by + cursor find",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"" OBJ "\","
        "\"criteria\":[{\"field\":\"n\",\"op\":\"gte\",\"value\":0}],"
        "\"order_by\":\"n\",\"cursor\":null,\"limit\":50}");
    check_route_nth(tc, &env, "ordered walk: order_by offset find",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"" OBJ "\","
        "\"criteria\":[{\"field\":\"n\",\"op\":\"gte\",\"value\":0}],"
        "\"order_by\":\"n\",\"limit\":50}");

    /* ===== A shard a writer is still creating reads as EMPTY, not as an
     * error: index shards are created lazily, and between the creator's
     * open(O_CREAT) and its bt_init_file a concurrent reader sees either a
     * zero-size file or a sized file with an all-zero (magic 0) header.
     * Both must answer exactly like an absent shard (no rows, no error)
     * on the btree walks and the O_DIRECT full-leaf scan. */
    resp = req(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"fcw2\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"n:int\",\"z:varchar:16\"],"
        "\"indexes\":[\"n\",\"z\"]}");
    ASSERT_CONTAINS(resp, "\"created\"", "fcw2 created");
    free(resp);
    for (int field = 0; field < 2; field++) {
        char dir[512], p0[560], p1[560];
        snprintf(dir, sizeof(dir), "%s/default/fcw2/indexes/%s",
                 env.db_root, field ? "z" : "n");
        snprintf(p0, sizeof(p0), "%s/000.idx", dir);
        snprintf(p1, sizeof(p1), "%s/001.idx", dir);
        char mk[1200];
        snprintf(mk, sizeof(mk), "mkdir -p '%s'", dir);
        ASSERT_EQ_INT(system(mk), 0, "index dir created");
        FILE *f0 = fopen(p0, "wb");           /* zero-size: mid-create */
        if (f0) fclose(f0);
        FILE *f1 = fopen(p1, "wb");           /* sized, magic still 0 */
        if (f1) {
            static const char zeros[16384] = {0};
            fwrite(zeros, 1, sizeof(zeros), f1);
            fclose(f1);
        }
    }
    static const char *const uninit_q[] = {
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fcw2\","
        "\"criteria\":[{\"field\":\"n\",\"op\":\"eq\",\"value\":5}],\"limit\":10}",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"fcw2\","
        "\"criteria\":[{\"field\":\"n\",\"op\":\"gte\",\"value\":0}]}",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"fcw2\","
        "\"criteria\":[{\"field\":\"z\",\"op\":\"contains\",\"value\":\"ed\"}]}",
    };
    for (size_t q = 0; q < sizeof(uninit_q) / sizeof(uninit_q[0]); q++) {
        resp = req(tc, uninit_q[q]);
        char msg[160];
        snprintf(msg, sizeof(msg),
                 "uninitialized shard #%zu reads as empty, not an error "
                 "(resp=%.60s)", q, resp ? resp : "(null)");
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""), msg);
        free(resp);
    }

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-failclosed-walkers", test_failclosed_walkers_run)
