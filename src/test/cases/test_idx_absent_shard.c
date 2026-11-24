/* Regression (round-4 review, M3 follow-up): an index shard file that was
 * NEVER CREATED must read as an EMPTY shard on every fail-closed route —
 * and a REAL open failure must still fail closed, on the wire shape the
 * find contract prescribes.
 *
 * Index shards are created lazily by the first write that routes a record
 * to them (routing = hash16 of the record key), so a shard nothing ever
 * routed to has no file at all and its open() fails with ENOENT.
 *
 * Part A — absent = empty. The round-4 M3 change made three walkers report
 * a failed btree acquire as a hard error and swept this normal state up
 * with it (121 suite cases went red on the first full run):
 *
 *   1. btree_search / btree_range_ex (the hot eq/range shard walks): -1 on
 *      ANY acquire failure. Their fan-out deliberately visits every shard
 *      including never-created ones, so every indexed eq / range / IN /
 *      between / neq count on a small object failed.
 *   2. chunk_walk_worker (D1 composite-exact + streaming finds): NULL
 *      from btree_range_iter_open on the absent shard became
 *      "index storage error".
 *   3. wfc_worker (single MIN/MAX with criteria): same, surfaced as
 *      "aggregate storage access failed".
 *
 * The other fail-closed consumers of btree_range_iter_open already guard
 * with a stat-first check ("ENOENT is a legitimate empty shard"); these
 * three had none. Fix: the rule lives once in btree.c — a reader acquire
 * that fails with ENOENT is an empty target (the walkers return 0; the
 * range iterator hands back an exhausted iterator).
 *
 * Part B — the other half of the M3 contract, which had no test of its
 * own: a REAL open failure on an EXISTING shard must fail closed on the
 * walker legs — never a silently short answer. The shard-walker seam
 * (test_env_ctl_set_bt_walk_open_fail) fails every btree_search /
 * btree_range_ex open of one shard's .idx with EIO; the fixture's only
 * record lives in that shard, so a silent skip answers 0 / no rows.
 *
 * Part C — route matrix. Until M3 the dispatch's -1 return was
 * unreachable (only a criterion-encode failure produced it), so the
 * hard-failure branch of every keyset-builder call site was dead code.
 * M3 makes it reachable, so no route may now answer a silent short page.
 * Under an armed failure each route must answer one of:
 *   - byte-identical to its unarmed response (the route does not depend
 *     on the failed walk — the unarmed response is the oracle);
 *   - a closed envelope followed by an error object;
 *   - an unterminated response.
 * The last two are both fail-closed, and both are prescribed:
 * docs/query-protocol/find.md ("Bitmap-source errors…", "Unordered
 * indexed pages") keeps a failure AFTER the response has started
 * unterminated on purpose — a closed short page can pass for success, a
 * truncated one cannot — while a budget refusal and the two round-4
 * exits (M2 intersect build, M3 composite-exact) close a still-bare
 * envelope. A closed page that differs from the oracle without an error
 * object is the only outcome this part rejects.
 *
 * Part D — the mid-stream half of that contract on a round-4 exit. The
 * M3 composite-exact exit closes a bare envelope and renders an error
 * object; that must stop the moment a row is on the wire. With more
 * matching entries than one chunk batch, an iterator-open failure on a
 * LATER open of a shard fires after that shard's first batch was flushed,
 * so rows are already out: the response must then be unterminated.
 *
 * Fixture: splits 8 -> 2 idx shards; ONE record, so exactly one shard
 * file exists per index and the other is absent. The test reads which
 * shard number exists straight off the scratch tree (never assumes the
 * hash) and asserts the precondition, so it cannot pass vacuously.
 */
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
#include <sys/stat.h>

#define NIDX 2 /* index_splits_for(8) */
#define ND1REC 20000 /* Part D fixture: > one chunk batch (<= 4096) per shard */

typedef struct {
    const char *label;
    const char *crit;
} CritCase;

/* Every indexed operator family; each names the shard walker it drives.
 * The fixture's one record matches every criterion, so the answer is 1. */
static const CritCase COUNTS[] = {
    { "eq (btree_search)",           "[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}]" },
    { "gte (btree_range)",           "[{\"field\":\"a\",\"op\":\"gte\",\"value\":1}]" },
    { "lt (btree_range, open low)",  "[{\"field\":\"a\",\"op\":\"lt\",\"value\":2}]" },
    { "between (btree_range)",       "[{\"field\":\"a\",\"op\":\"between\",\"value\":1,\"value2\":1}]" },
    { "gt (btree_range_ex)",         "[{\"field\":\"a\",\"op\":\"gt\",\"value\":0}]" },
    { "in (btree_search per value)", "[{\"field\":\"a\",\"op\":\"in\",\"value\":\"1,2\"}]" },
    { "neq (btree_range_ex x2)",     "[{\"field\":\"a\",\"op\":\"neq\",\"value\":5}]" },
    { "two indexed leaves (AND)",
      "[{\"field\":\"a\",\"op\":\"gte\",\"value\":1},"
      "{\"field\":\"b\",\"op\":\"lte\",\"value\":5}]" },
    { "OR union",
      "[{\"or\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":1},"
      "{\"field\":\"b\",\"op\":\"eq\",\"value\":1}]}]" },
};
#define NCOUNTS ((int)(sizeof(COUNTS) / sizeof(COUNTS[0])))

static const CritCase FINDS[] = {
    { "find eq (default)", "[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}]" },
    { "find gte (rows)",   "[{\"field\":\"a\",\"op\":\"gte\",\"value\":1}]" },
    { "find lt (csv)",     "[{\"field\":\"a\",\"op\":\"lt\",\"value\":2}]" },
};
#define NFINDS ((int)(sizeof(FINDS) / sizeof(FINDS[0])))

/* Part C request shapes on abs_ix (a, b, c ints; indexes a, b, a+b). */
#define CRIT_A_EQ  "[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}]"
#define CRIT_A_GTE "[{\"field\":\"a\",\"op\":\"gte\",\"value\":1}]"
#define CRIT_C_EQ  "[{\"field\":\"c\",\"op\":\"eq\",\"value\":1}]"   /* not indexed */
#define CRIT_AND   "[{\"field\":\"a\",\"op\":\"gte\",\"value\":1}," \
                   "{\"field\":\"b\",\"op\":\"lte\",\"value\":5}]"
#define CRIT_OR    "[{\"or\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}," \
                   "{\"field\":\"b\",\"op\":\"eq\",\"value\":1}]}]"
#define FIND(crit, tail) \
    "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"abs_ix\",\"criteria\":" crit tail "}"
#define AGG(crit, aggs, tail) \
    "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"abs_ix\",\"criteria\":" crit \
    ",\"aggregates\":" aggs tail "}"
#define AGG_N "[{\"fn\":\"count\",\"field\":\"b\",\"alias\":\"n\"}]"

typedef struct {
    const char *label;
    const char *req;
    int csv;      /* response body is CSV text, not a JSON document */
} Shape;

static const Shape SHAPES[] = {
    { "find default eq",             FIND(CRIT_A_EQ, ",\"limit\":10}"), 0 },
    { "find default gte",            FIND(CRIT_A_GTE, ",\"limit\":10}"), 0 },
    { "find default eq + total",     FIND(CRIT_A_EQ, ",\"limit\":10,\"total\":true}"), 0 },
    { "find default AND",            FIND(CRIT_AND, ",\"limit\":10}"), 0 },
    { "find default OR",             FIND(CRIT_OR, ",\"limit\":10}"), 0 },
    { "find eq order_by a",          FIND(CRIT_A_EQ, ",\"limit\":10,\"order_by\":\"a\"}"), 0 },
    { "find gte order_by c",         FIND(CRIT_A_GTE, ",\"limit\":10,\"order_by\":\"c\"}"), 0 },
    { "find eq cursor",              FIND(CRIT_A_EQ, ",\"limit\":10,\"order_by\":\"a\",\"cursor\":null}"), 0 },
    { "find rows eq",                FIND(CRIT_A_EQ, ",\"limit\":10,\"format\":\"rows\"}"), 0 },
    { "find rows gte",               FIND(CRIT_A_GTE, ",\"limit\":10,\"format\":\"rows\"}"), 0 },
    { "find rows eq + total",        FIND(CRIT_A_EQ, ",\"limit\":10,\"format\":\"rows\",\"total\":true}"), 0 },
    { "find rows AND",               FIND(CRIT_AND, ",\"limit\":10,\"format\":\"rows\"}"), 0 },
    { "find rows OR",                FIND(CRIT_OR, ",\"limit\":10,\"format\":\"rows\"}"), 0 },
    { "find csv eq",                 FIND(CRIT_A_EQ, ",\"limit\":10,\"format\":\"csv\"}"), 1 },
    { "find csv gte",                FIND(CRIT_A_GTE, ",\"limit\":10,\"format\":\"csv\"}"), 1 },
    { "find dict eq",                FIND(CRIT_A_EQ, ",\"limit\":10,\"format\":\"dict\"}"), 0 },
    { "find dict gte",               FIND(CRIT_A_GTE, ",\"limit\":10,\"format\":\"dict\"}"), 0 },
    { "agg count b",                 AGG(CRIT_A_EQ, AGG_N, "}"), 0 },
    { "agg sum b",                   AGG(CRIT_A_GTE, "[{\"fn\":\"sum\",\"field\":\"b\",\"alias\":\"s\"}]", "}"), 0 },
    { "agg avg b",                   AGG(CRIT_A_GTE, "[{\"fn\":\"avg\",\"field\":\"b\",\"alias\":\"v\"}]", "}"), 0 },
    { "agg min b",                   AGG(CRIT_A_GTE, "[{\"fn\":\"min\",\"field\":\"b\",\"alias\":\"m\"}]", "}"), 0 },
    { "agg max b",                   AGG(CRIT_A_GTE, "[{\"fn\":\"max\",\"field\":\"b\",\"alias\":\"m\"}]", "}"), 0 },
    { "agg group_by b",              AGG(CRIT_A_EQ, AGG_N, ",\"group_by\":[\"b\"]}"), 0 },
    { "agg group_by a,b",            AGG(CRIT_A_EQ, AGG_N, ",\"group_by\":[\"a\",\"b\"]}"), 0 },
    { "agg having",                  AGG(CRIT_A_EQ, AGG_N, ",\"group_by\":[\"b\"],\"having\":{\"field\":\"n\",\"op\":\"gt\",\"value\":0}}"), 0 },
    { "agg OR criteria",             AGG(CRIT_OR, AGG_N, "}"), 0 },
    { "agg min b csv",               AGG(CRIT_A_GTE, "[{\"fn\":\"min\",\"field\":\"b\",\"alias\":\"m\"}]", ",\"format\":\"csv\"}"), 1 },
};
#define NSHAPES ((int)(sizeof(SHAPES) / sizeof(SHAPES[0])))

static int idx_shard_exists(TestEnv *env, const char *obj,
                            const char *field, int s) {
    char path[640];
    snprintf(path, sizeof(path), "%s/default/%s/indexes/%s/%03d.idx",
             env->db_root, obj, field, s);
    struct stat st;
    return stat(path, &st) == 0;
}

/* The one shard file that exists for an index, or -1 unless exactly one
 * of the object's NIDX shards is present. */
static int existing_shard(TestEnv *env, const char *obj, const char *field) {
    int found = -1, n = 0;
    for (int s = 0; s < NIDX; s++)
        if (idx_shard_exists(env, obj, field, s)) { found = s; n++; }
    return n == 1 ? found : -1;
}

static int count_keys(const char *resp) {
    int n = 0;
    const char *p = resp;
    while (p && (p = strstr(p, "k0001")) != NULL) { n++; p += 5; }
    return n;
}

static int is_error_resp(const char *resp) {
    return resp && SAFE_STRSTR(resp, "\"error\"") != NULL;
}

/* Unclosed brackets/braces outside string literals at the end of the
 * response (0 = every document closed; -1 = a string left open). The
 * response may be several concatenated documents (envelope + error
 * object) — only the balance at the end matters. */
static int json_end_depth(const char *s) {
    int depth = 0, instr = 0;
    for (; s && *s; s++) {
        if (instr) {
            if (*s == '\\' && s[1]) s++;
            else if (*s == '"') instr = 0;
            continue;
        }
        if (*s == '"') instr = 1;
        else if (*s == '{' || *s == '[') depth++;
        else if (*s == '}' || *s == ']') depth--;
    }
    return instr ? -1 : depth;
}

static char *ask(TestClient *tc, const char *req) {
    char *resp = NULL;
    tc_request(tc, req, &resp);
    return resp;
}

static void create_object(TestClient *tc, const char *obj,
                          const char *fields, const char *indexes) {
    char req[512];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"default\","
             "\"object\":\"%s\",\"splits\":8,\"max_key\":16,"
             "\"fields\":[%s],\"indexes\":[%s]}", obj, fields, indexes);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "object created");
    free(resp);
}

static void insert_one(TestClient *tc, const char *obj, const char *key,
                       const char *value) {
    char req[512];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\",\"value\":%s}", obj, key, value);
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && !is_error_resp(resp), "record inserted");
    free(resp);
}

static void run_counts(TestClient *tc, int fail_closed, const char *tag) {
    for (int i = 0; i < NCOUNTS; i++) {
        char req[768];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"default\","
                 "\"object\":\"abs_ix\",\"criteria\":%s}", COUNTS[i].crit);
        char *resp = ask(tc, req);
        int is_err = is_error_resp(resp);
        int val = (resp && !is_err) ? atoi(resp) : -1;
        TAP_DIAG("# %s count[%d] %s -> %s\n", tag, i, COUNTS[i].label,
                 resp ? resp : "(none)");
        if (!fail_closed)
            /* Part A: absent shard = empty; the record is still found. */
            ASSERT_EQ_INT(is_err ? -1 : val, 1, COUNTS[i].label);
        else
            /* Part B: the record's own shard cannot be opened, so the
               answer is either a fail-closed error or (if the route
               recovers by another path) the correct one — never a silent
               short count. */
            ASSERT_TRUE(is_err || val == 1, COUNTS[i].label);
        free(resp);
    }
}

static void run_finds(TestClient *tc, int fail_closed, const char *tag) {
    static const char *fmt[NFINDS] = { NULL, "rows", "csv" };
    for (int i = 0; i < NFINDS; i++) {
        char req[768];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\","
                 "\"object\":\"abs_ix\",\"criteria\":%s,\"limit\":10%s%s%s}",
                 FINDS[i].crit, fmt[i] ? ",\"format\":\"" : "",
                 fmt[i] ? fmt[i] : "", fmt[i] ? "\"" : "");
        char *resp = ask(tc, req);
        int is_err = is_error_resp(resp);
        int rows = is_err ? -1 : count_keys(resp);
        /* A streamed find that fails after the prefix is unterminated by
           contract (find.md) — fail-closed without an error object. */
        int cut = !is_err && resp && fmt[i] && strcmp(fmt[i], "csv") != 0 &&
                  json_end_depth(resp) > 0;
        TAP_DIAG("# %s find[%d] %s -> %s\n", tag, i, FINDS[i].label,
                 resp ? resp : "(none)");
        if (!fail_closed)
            ASSERT_EQ_INT(is_err ? -1 : rows, 1, FINDS[i].label);
        else
            ASSERT_TRUE(is_err || cut || rows == 1, FINDS[i].label);
        free(resp);
    }
}

/* Single MIN / MAX with a criterion on another field (wfc route). */
static void run_wfc(TestClient *tc, const char *tag) {
    static const char *fns[] = { "min", "max" };
    for (int i = 0; i < 2; i++) {
        char req[768];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"aggregate\",\"dir\":\"default\","
                 "\"object\":\"abs_ix\",\"criteria\":[{\"field\":\"a\","
                 "\"op\":\"eq\",\"value\":1}],\"aggregates\":[{\"fn\":\"%s\","
                 "\"field\":\"b\",\"alias\":\"m\"}]}", fns[i]);
        char *resp = ask(tc, req);
        TAP_DIAG("# %s wfc %s -> %s\n", tag, fns[i], resp ? resp : "(none)");
        ASSERT_TRUE(resp && !is_error_resp(resp),
                    "wfc: absent shard is not an aggregate error");
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"m\":1") != NULL,
                    "wfc: min/max still sees the record");
        free(resp);
    }
}

/* D1 composite-exact find: exact prefix on a+b, standalone c present so
 * the exact plan has a primary leaf (see test_wfc_chunk_open_fail). */
static void run_d1(TestClient *tc, const char *tag) {
    char *resp = ask(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"abs_d1\","
        "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":1},"
        "{\"field\":\"b\",\"op\":\"eq\",\"value\":1},"
        "{\"field\":\"c\",\"op\":\"gte\",\"value\":0}],"
        "\"limit\":10,\"total\":true}");
    TAP_DIAG("# %s d1 -> %s\n", tag, resp ? resp : "(none)");
    ASSERT_TRUE(resp && !is_error_resp(resp),
                "d1: absent shard is not a storage error");
    ASSERT_EQ_INT(resp ? count_keys(resp) : -1, 1, "d1: returns the record");
    free(resp);
}

/* Part C oracle for one request: under the armed failure the response is
 * either byte-identical to the unarmed one, or fail-closed — a closed
 * envelope plus an error object, or unterminated (the documented shape
 * once the response has started). A closed page that differs without an
 * error object is a silent short answer. */
static void check_shape(const Shape *s, const char *base, const char *armed) {
    int same = armed && base && strcmp(armed, base) == 0;
    int is_err = is_error_resp(armed);
    int depth = (s->csv || !armed) ? 0 : json_end_depth(armed);
    int ok;
    const char *cls;
    if (!armed) { ok = 0; cls = "(no response)"; }
    else if (same) { ok = 1; cls = "identical"; }
    else if (s->csv) {                 /* text body: only an error line closes it */
        ok = is_err; cls = is_err ? "fail-closed error" : "OTHER";
    } else if (depth > 0 && !is_err) { ok = 1; cls = "unterminated (mid-stream)"; }
    else if (depth == 0 && is_err) { ok = 1; cls = "fail-closed error"; }
    else { ok = 0; cls = "OTHER"; }
    TAP_DIAG("# C %-30s -> %s depth=%d\n", s->label, cls, depth);
    if (!ok && armed)
        TAP_DIAG("# C   bad response: %s\n", armed);
    ASSERT_TRUE(ok, s->label);
}

/* Composite-exact find over the many-row fixture (Part D). */
static char *d1_many(TestClient *tc) {
    return ask(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"abs_d1m\","
        "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":1},"
        "{\"field\":\"b\",\"op\":\"eq\",\"value\":1},"
        "{\"field\":\"c\",\"op\":\"gte\",\"value\":0}],"
        "\"limit\":40000,\"total\":true}");
}

static int count_k0(const char *resp) {
    int n = 0;
    const char *p = resp;
    while (p && (p = strstr(p, "\"k0")) != NULL) { n++; p += 3; }
    return n;
}

/* Part D: fail the (skip+1)-th open of one shard and classify. The rows
   of a chunked fan-out interleave by timing, so a closed response is
   compared with the baseline by ROW COUNT, not bytes. A closed page must
   carry the baseline's rows, or be a bare envelope plus an error object
   (nothing emitted yet). Once rows are out the only accepted shape is
   unterminated. Returns 1 when rows were on the wire. */
static int d1_classify(TestClient *tc, TestEnv *env, int base_rows,
                       int shard, int skip) {
    char label[96];
    snprintf(label, sizeof(label), "D1 reopen failure (shard %d, skip %d)",
             shard, skip);
    ASSERT_EQ_INT(test_env_ctl_set_bt_range_open_fail_nth(env, shard, skip),
                  0, label);
    char *armed = d1_many(tc);
    test_env_ctl_set_bt_range_open_fail(env, -1);
    int is_err = is_error_resp(armed);
    int depth = armed ? json_end_depth(armed) : -1;
    int rows = count_k0(armed);
    int complete = depth == 0 && !is_err && rows == base_rows;
    int ok = complete || (depth > 0 && !is_err) ||
             (depth == 0 && is_err && rows == 0);
    TAP_DIAG("# D shard %d skip %d -> %s rows=%d depth=%d err=%d\n", shard, skip,
             complete ? "complete" : depth > 0 ? "unterminated" :
             is_err ? "closed + error" : "OTHER", rows, depth, is_err);
    if (!ok && armed)
        TAP_DIAG("# D   bad response tail: ...%s\n",
                 armed + (strlen(armed) > 160 ? strlen(armed) - 160 : 0));
    ASSERT_TRUE(ok, label);
    free(armed);
    return rows > 0 && !complete;
}

static int test_idx_absent_shard_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    free(ask(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}"));

    static const char *ABC = "\"a:int\",\"b:int\",\"c:int\"";
    create_object(tc, "abs_ix", ABC, "\"a\",\"b\",\"a+b\"");
    create_object(tc, "abs_d1", ABC, "\"a+b\",\"c\"");
    insert_one(tc, "abs_ix", "k0001", "{\"a\":1,\"b\":1,\"c\":1}");
    insert_one(tc, "abs_d1", "k0001", "{\"a\":1,\"b\":1,\"c\":1}");

    /* Precondition: exactly one shard file per index, the other absent —
       and (routing is the record key's hash) all of abs_ix's indexes put
       it in the same numbered shard. */
    int sh = existing_shard(&env, "abs_ix", "a");
    ASSERT_TRUE(sh == 0 || sh == 1,
                "precondition: exactly one of the 2 'a' shards exists");
    TAP_DIAG("# precondition: existing shard = %d\n", sh);
    ASSERT_EQ_INT(existing_shard(&env, "abs_ix", "b"), sh,
                  "precondition: 'b' shard in the same slot");
    ASSERT_EQ_INT(existing_shard(&env, "abs_ix", "a+b"), sh,
                  "precondition: 'a+b' shard in the same slot");
    ASSERT_TRUE(existing_shard(&env, "abs_d1", "a+b") == 0 ||
                existing_shard(&env, "abs_d1", "a+b") == 1,
                "precondition: abs_d1 has one 'a+b' shard");

    /* ===== Part A: a never-created shard reads as empty ===== */
    run_counts(tc, 0, "A");
    run_finds(tc, 0, "A");
    run_wfc(tc, "A");
    run_d1(tc, "A");

    /* Cursor pagination (k-way merge across the shards) — pins that the
       ordered walk treats the absent shard as empty too. */
    resp = ask(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"abs_ix\","
        "\"criteria\":[{\"field\":\"a\",\"op\":\"gte\",\"value\":0}],"
        "\"order_by\":\"a\",\"order\":\"asc\",\"limit\":10,\"cursor\":null}");
    TAP_DIAG("# A cursor -> %s\n", resp ? resp : "(none)");
    ASSERT_TRUE(resp && !is_error_resp(resp),
                "cursor: absent shard is not an error");
    ASSERT_EQ_INT(resp ? count_keys(resp) : -1, 1, "cursor: returns the record");
    free(resp); resp = NULL;

    /* ===== Part B: a REAL open failure on the existing shard fails
       closed on the walker legs (never a silent short answer) ===== */
    ASSERT_EQ_INT(test_env_ctl_set_bt_walk_open_fail(&env, sh), 0,
                  "arm the walker open failure on the record's shard");
    run_counts(tc, 1, "B");
    run_finds(tc, 1, "B");
    ASSERT_EQ_INT(test_env_ctl_set_bt_walk_open_fail(&env, -1), 0,
                  "disarm the walker open failure");
    run_counts(tc, 0, "B-restored");
    run_finds(tc, 0, "B-restored");

    /* ===== Part C: route matrix ===== */
    /* Join twins. By primary key: key "1" mirrors the record's c. By an
       indexed remote field: one object per attempt so the test can see
       which numbered shard the twin's single record landed in — it must
       be the armed one, or the lookup would never touch a failed walk. */
    create_object(tc, "abs_j2", "\"tag:varchar:8\"", "");
    insert_one(tc, "abs_j2", "1", "{\"tag\":\"t1\"}");
    char jt[32] = "";
    for (int i = 0; i < 16 && !jt[0]; i++) {
        char obj[32], key[16];
        snprintf(obj, sizeof(obj), "absjt%d", i);
        snprintf(key, sizeof(key), "jk%d", i);
        create_object(tc, obj, "\"ref:int\",\"tag:varchar:8\"", "\"ref\"");
        insert_one(tc, obj, key, "{\"ref\":1,\"tag\":\"t1\"}");
        if (sh >= 0 && existing_shard(&env, obj, "ref") == sh)
            snprintf(jt, sizeof(jt), "%s", obj);
    }
    ASSERT_TRUE(jt[0] != 0, "precondition: a join twin in the armed shard");

    Shape shapes[NSHAPES + 4];
    memcpy(shapes, SHAPES, sizeof(SHAPES));
    int nsh = NSHAPES;
    static char join_key[512], join_field[512], join_field2[512], join_left[512];
    snprintf(join_key, sizeof(join_key),
             FIND(CRIT_A_EQ, ",\"limit\":10,\"join\":[{\"object\":\"abs_j2\","
                 "\"local\":\"c\",\"remote\":\"key\",\"as\":\"j2\","
                 "\"type\":\"inner\",\"fields\":[\"tag\"]}]}"));
    snprintf(join_field, sizeof(join_field),
             FIND(CRIT_A_EQ, ",\"limit\":10,\"join\":[{\"object\":\"%s\","
                 "\"local\":\"a\",\"remote\":\"ref\",\"as\":\"j3\","
                 "\"type\":\"inner\",\"fields\":[\"tag\"]}]}"), jt);
    /* The driver on a NON-indexed field (full scan), so the remote
       lookup is the only walk of the armed shard: this is the join
       lookup's own failure handling, inner and left. */
#define JOIN_JT "{\"object\":\"%s\",\"local\":\"a\",\"remote\":\"ref\"," \
               "\"as\":\"j3\",\"type\":\"%s\",\"fields\":[\"tag\"]}"
    snprintf(join_field2, sizeof(join_field2),
             FIND(CRIT_C_EQ, ",\"limit\":10,\"join\":[" JOIN_JT "]}"), jt, "inner");
    snprintf(join_left, sizeof(join_left),
             FIND(CRIT_C_EQ, ",\"limit\":10,\"join\":[" JOIN_JT "]}"), jt, "left");
    shapes[nsh++] = (Shape){ "find join by key", join_key, 0 };
    shapes[nsh++] = (Shape){ "find join by indexed field", join_field, 0 };
    shapes[nsh++] = (Shape){ "scan driver, join by field", join_field2, 0 };
    shapes[nsh++] = (Shape){ "scan driver, left join by field", join_left, 0 };

    /* Pass 1: unarmed baselines (the oracle). A baseline that is itself
       an error would make its shape pass vacuously — fail loudly. */
    char *base[NSHAPES + 4];
    for (int i = 0; i < nsh; i++) {
        base[i] = ask(tc, shapes[i].req);
        TAP_DIAG("# C base %-26s -> %s\n", shapes[i].label,
                 base[i] ? base[i] : "(none)");
        ASSERT_TRUE(base[i] && !is_error_resp(base[i]), shapes[i].label);
    }
    /* Pass 2: armed. */
    ASSERT_EQ_INT(test_env_ctl_set_bt_walk_open_fail(&env, sh), 0,
                  "arm the walker open failure (route matrix)");
    for (int i = 0; i < nsh; i++) {
        char *armed = ask(tc, shapes[i].req);
        check_shape(&shapes[i], base[i], armed);
        free(armed);
    }
    ASSERT_EQ_INT(test_env_ctl_set_bt_walk_open_fail(&env, -1), 0,
                  "disarm the walker open failure (route matrix)");
    /* Destructive shapes run on twin objects so the fixture survives:
       abs_b<k>_0 is the unarmed baseline, abs_b<k>_1 the armed attempt.
       The seam is toggled around each request so the baseline and the
       state check below are both unarmed. */
    static const char *BDEL =
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}]}";
    static const char *BUPD =
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}],"
        "\"value\":{\"b\":2}}";
    for (int k = 0; k < 2; k++) {
        const char *tmpl = k == 0 ? BDEL : BUPD;
        char o0[32], o1[32], req[512];
        snprintf(o0, sizeof(o0), "abs_b%d_0", k);
        snprintf(o1, sizeof(o1), "abs_b%d_1", k);
        create_object(tc, o0, ABC, "\"a\"");
        create_object(tc, o1, ABC, "\"a\"");
        insert_one(tc, o0, "k0001", "{\"a\":1,\"b\":1,\"c\":1}");
        insert_one(tc, o1, "k0001", "{\"a\":1,\"b\":1,\"c\":1}");
        snprintf(req, sizeof(req), tmpl, o0);
        char *b0 = ask(tc, req);
        test_env_ctl_set_bt_walk_open_fail(&env, sh);
        snprintf(req, sizeof(req), tmpl, o1);
        char *b1 = ask(tc, req);
        test_env_ctl_set_bt_walk_open_fail(&env, -1);
        Shape s = { k == 0 ? "bulk-delete by criteria" : "bulk-update by criteria",
                    tmpl, 0 };
        check_shape(&s, b0, b1);
        /* State must agree with what the armed attempt reported: an error
           means nothing was applied; an applied answer means it was. */
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
                 "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}]}",
                 o1);
        char *after = ask(tc, req);
        int expect = is_error_resp(b1) ? 1 : 0;
        ASSERT_EQ_INT(after ? atoi(after) : -1, expect,
                      "bulk: stored state matches the armed answer");
        free(b0); free(b1); free(after);
    }
    for (int i = 0; i < nsh; i++) free(base[i]);

    /* ===== Part D: the composite-exact exit keeps the mid-stream contract =====
       More matching entries than one chunk batch per shard (the batch cap
       is at most 4096), so each shard worker reopens its iterator after
       its first batch was flushed. Failing a LATER open of a shard hits
       after rows are out. */
    create_object(tc, "abs_d1m", ABC, "\"a+b\",\"c\"");
    {
        enum { NCHUNK = 2500 };
        char *body = malloc((size_t)NCHUNK * 64 + 256);
        ASSERT_NOT_NULL(body, "alloc d1m body");
        if (body) {
            for (int b = 0; b < ND1REC; b += NCHUNK) {
                size_t off = (size_t)snprintf(body, 32, "[");
                for (int i = b; i < b + NCHUNK; i++)
                    off += (size_t)snprintf(body + off, 64,
                        "%s{\"key\":\"k%05d\",\"value\":{\"a\":1,\"b\":1,\"c\":%d}}",
                        i > b ? "," : "", i, i);
                snprintf(body + off, 8, "]");
                char *req = malloc(off + 200);
                ASSERT_NOT_NULL(req, "alloc d1m request");
                if (!req) break;
                snprintf(req, off + 200,
                         "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                         "\"object\":\"abs_d1m\",\"records\":%s}", body);
                char *r = ask(tc, req);
                ASSERT_TRUE(r && !is_error_resp(r), "d1m bulk insert");
                free(r); free(req);
            }
        }
        free(body);
    }
    char *d1_base = d1_many(tc);
    ASSERT_TRUE(d1_base && !is_error_resp(d1_base), "d1m baseline acked");
    /* The test env clamps limit (GLOBAL_LIMIT), so the page is a slice of
       the fixture — but a large one: far more than one chunk batch per
       shard, which is what forces the reopens. */
    int d1_rows = count_k0(d1_base);
    TAP_DIAG("# D baseline rows=%d tail=...%s\n", d1_rows,
             d1_base + (strlen(d1_base) > 60 ? strlen(d1_base) - 60 : 0));
    ASSERT_TRUE(d1_rows > 2 * 4096, "d1m baseline page spans several batches");
    ASSERT_EQ_INT(d1_base ? json_end_depth(d1_base) : -1, 0,
                  "d1m baseline is closed");
    {
        int mid = 0;
        for (int s = 0; s < NIDX; s++)
            for (int skip = 0; skip < 4; skip++)
                mid |= d1_classify(tc, &env, d1_rows, s, skip);
        ASSERT_TRUE(mid, "D: a later open failure was exercised after rows");
    }
    free(d1_base);

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-idx-absent-shard", test_idx_absent_shard_run)
