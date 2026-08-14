/* src/test/cases/test_bitmap_index.c
 *
 * Bitmap index — Phase 1 (wire + schema acceptance). These assertions
 * only cover the create-object validator and the index.conf round-trip;
 * the runtime bitmap behaviour (CRUD hooks, query planner, reindex)
 * lands in later phases and gets its own assertions appended to this
 * same file.
 *
 * Spec: [[index-types-roadmap]] / [[bitmap-impl-map]].
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "bitmap.h"
#include "slotcask.h"
#include "types.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

/* Convenience: the framework only provides ASSERT_TRUE / EQ_* / CONTAINS /
   NOT_NULL. Spelling NOT_CONTAINS as a wrapper keeps the test prose
   readable without dragging a new macro into the shared header. */
#define ASSERT_NOT_CONTAINS(haystack, needle, desc) \
    ASSERT_TRUE(strstr((haystack), (needle)) == NULL, (desc))

static int g_walk_count;
static uint32_t g_walk_last_slot;
static int walk_count_cb(uint32_t slot, void *ctx) {
    (void)ctx;
    g_walk_count++;
    g_walk_last_slot = slot;
    return 0;
}

/* Direct bitmap.c API exercise — no daemon, no fixtures. Runs before the
   wire-layer block so a primitive-level bug surfaces with a clean stack
   trace rather than a confusing daemon response mismatch. */
static int run_unit_assertions(void) {
    const char *path = "/tmp/shard-db-test-bm-unit.bm";
    unlink(path);

    /* === Bool fast-path: 256 slots, 2 hardcoded values. === */
    BitmapShard *bm = bm_open(path, 256, 1, 1, 0, 1);
    ASSERT_NOT_NULL(bm, "bm_open: bool create");
    if (!bm) return 1;

    ASSERT_EQ_INT((int)bm_slots(bm), 256, "slots = 256");
    ASSERT_EQ_INT((int)bm_n_values(bm), 2, "bool dict has 2 values");
    ASSERT_EQ_INT((int)bm_stride(bm), 32, "stride = 256/8 = 32 bytes");

    uint8_t v_true[1]  = { 0x01 };
    uint8_t v_false[1] = { 0x00 };

    /* All bits start at zero. */
    ASSERT_EQ_INT(bm_test(bm, v_true,  1, 5),  0, "slot 5 not yet true");
    ASSERT_EQ_INT(bm_test(bm, v_false, 1, 5),  0, "slot 5 not yet false");
    ASSERT_EQ_INT((int)bm_count(bm, v_true, 1), 0, "true count = 0");

    /* Set 3 truthy bits, 2 falsy. */
    ASSERT_EQ_INT(bm_set(bm, v_true,  1,  5), 0, "set true[5]");
    ASSERT_EQ_INT(bm_set(bm, v_true,  1, 17), 0, "set true[17]");
    ASSERT_EQ_INT(bm_set(bm, v_true,  1, 31), 0, "set true[31]");
    ASSERT_EQ_INT(bm_set(bm, v_false, 1,  3), 0, "set false[3]");
    ASSERT_EQ_INT(bm_set(bm, v_false, 1,  4), 0, "set false[4]");

    ASSERT_EQ_INT(bm_test(bm, v_true,  1,  5), 1, "test true[5]");
    ASSERT_EQ_INT(bm_test(bm, v_true,  1, 17), 1, "test true[17]");
    ASSERT_EQ_INT(bm_test(bm, v_true,  1, 18), 0, "test true[18] (unset)");
    ASSERT_EQ_INT(bm_test(bm, v_false, 1,  3), 1, "test false[3]");
    ASSERT_EQ_INT(bm_test(bm, v_false, 1,  5), 0, "test false[5] (unset)");

    ASSERT_EQ_INT((int)bm_count(bm, v_true,  1), 3, "true popcount = 3");
    ASSERT_EQ_INT((int)bm_count(bm, v_false, 1), 2, "false popcount = 2");

    /* Walk returns slots in ascending order. */
    g_walk_count = 0;
    bm_walk(bm, v_true, 1, walk_count_cb, NULL);
    ASSERT_EQ_INT(g_walk_count, 3, "walk true visited 3");
    ASSERT_EQ_INT((int)g_walk_last_slot, 31, "walk true last slot 31");

    /* Clear one bit and re-count. */
    ASSERT_EQ_INT(bm_clear(bm, v_true, 1, 17), 0, "clear true[17]");
    ASSERT_EQ_INT(bm_test(bm, v_true, 1, 17), 0, "cleared bit is 0");
    ASSERT_EQ_INT((int)bm_count(bm, v_true, 1), 2, "true popcount after clear = 2");

    /* Grow to 1024 slots — stride should grow to 128 bytes, existing bits
       preserved, new high slots all zero. */
    ASSERT_EQ_INT(bm_grow(bm, 1024), 0, "bm_grow 256→1024");
    ASSERT_EQ_INT((int)bm_slots(bm), 1024, "post-grow slots = 1024");
    ASSERT_EQ_INT((int)bm_stride(bm), 128, "post-grow stride = 128");
    ASSERT_EQ_INT(bm_test(bm, v_true,  1,  5), 1, "post-grow: true[5] preserved");
    ASSERT_EQ_INT(bm_test(bm, v_true,  1, 31), 1, "post-grow: true[31] preserved");
    ASSERT_EQ_INT(bm_test(bm, v_false, 1,  3), 1, "post-grow: false[3] preserved");
    ASSERT_EQ_INT(bm_test(bm, v_true,  1, 800), 0, "post-grow: new slot 800 zero");
    ASSERT_EQ_INT((int)bm_count(bm, v_true, 1), 2, "post-grow popcount unchanged");

    /* Use a newly-grown slot. */
    ASSERT_EQ_INT(bm_set(bm, v_true, 1, 999), 0, "set true[999] in grown region");
    ASSERT_EQ_INT((int)bm_count(bm, v_true, 1), 3, "popcount after high slot");

    /* Reject out-of-bounds slot. */
    ASSERT_EQ_INT(bm_set(bm, v_true, 1, 1024), -1, "set OOB rejected");
    ASSERT_EQ_INT(bm_set(bm, v_true, 1, 99999), -1, "set far-OOB rejected");

    /* Bool fast-path rejects unknown values. */
    uint8_t v_two[1] = { 0x02 };
    ASSERT_EQ_INT(bm_set(bm, v_two, 1, 0), -1, "bool fast-path rejects 0x02");

    bm_close(bm);

    /* === Persistence across close/reopen. === */
    bm = bm_open(path, 1024, 0, 0, 0, 0);
    ASSERT_NOT_NULL(bm, "bm_open: reopen no-create");
    if (bm) {
        ASSERT_EQ_INT((int)bm_slots(bm), 1024, "persisted slots");
        ASSERT_EQ_INT((int)bm_stride(bm), 128, "persisted stride");
        ASSERT_EQ_INT(bm_test(bm, v_true,  1,   5), 1, "persisted true[5]");
        ASSERT_EQ_INT(bm_test(bm, v_true,  1, 999), 1, "persisted true[999]");
        ASSERT_EQ_INT(bm_test(bm, v_false, 1,   3), 1, "persisted false[3]");
        ASSERT_EQ_INT((int)bm_count(bm, v_true, 1), 3, "persisted true count = 3");
        bm_close(bm);
    }
    unlink(path);

    /* === Varchar enum (no fast-path): dict grows on demand. === */
    bm = bm_open(path, 64, 1, 0, 0, 1);
    ASSERT_NOT_NULL(bm, "bm_open: varchar enum create");
    if (!bm) return 1;
    ASSERT_EQ_INT((int)bm_n_values(bm), 0, "varchar starts empty");

    ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"story", 5, 0), 0, "set story[0]");
    ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"story", 5, 1), 0, "set story[1]");
    ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"comment", 7, 2), 0, "set comment[2]");
    ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"job",    3, 3), 0, "set job[3]");
    ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"comment", 7, 4), 0, "set comment[4]");

    ASSERT_EQ_INT((int)bm_n_values(bm), 3, "varchar dict has 3 distinct values");
    ASSERT_EQ_INT((int)bm_count(bm, (const uint8_t *)"story",   5), 2, "story count");
    ASSERT_EQ_INT((int)bm_count(bm, (const uint8_t *)"comment", 7), 2, "comment count");
    ASSERT_EQ_INT((int)bm_count(bm, (const uint8_t *)"job",     3), 1, "job count");
    ASSERT_EQ_INT((int)bm_count(bm, (const uint8_t *)"poll",    4), 0, "poll absent = 0");

    /* Persistence after dict growth. */
    bm_close(bm);
    bm = bm_open(path, 64, 0, 0, 0, 0);
    ASSERT_NOT_NULL(bm, "reopen after dict-grow");
    if (bm) {
        ASSERT_EQ_INT((int)bm_n_values(bm), 3, "persisted dict size");
        ASSERT_EQ_INT(bm_test(bm, (const uint8_t *)"comment", 7, 4), 1, "persisted comment[4]");
        ASSERT_EQ_INT(bm_test(bm, (const uint8_t *)"story",   5, 0), 1, "persisted story[0]");
        bm_close(bm);
    }
    unlink(path);

    /* === Cardinality cap: bitmap refuses past BM_MAX_VALUES. The user
           contract is "bitmap is for bool + low-cardinality enums" —
           anyone declaring bitmap on a field with thousands of distinct
           values is mis-indexing and should reach for btree. === */
    bm = bm_open(path, 64, 1, 0, 0, 1);
    ASSERT_NOT_NULL(bm, "bm_open: cap test");
    if (bm) {
        /* Fill the dict to exactly BM_DEFAULT_MAX_VALUES distinct
           values (the cap baked into a default-create file). */
        char val[16];
        int ok_until_cap = 1;
        for (uint32_t i = 0; i < BM_DEFAULT_MAX_VALUES; i++) {
            int n = snprintf(val, sizeof(val), "v%u", i);
            if (bm_set(bm, (const uint8_t *)val, (size_t)n, 0) != 0) {
                ok_until_cap = 0;
                break;
            }
        }
        ASSERT_TRUE(ok_until_cap, "first BM_DEFAULT_MAX_VALUES values all succeed");
        ASSERT_EQ_INT((int)bm_n_values(bm), (int)BM_DEFAULT_MAX_VALUES,
                      "dict at exactly the default cap");

        /* The 257th distinct value is refused. */
        int rc = bm_set(bm, (const uint8_t *)"over", 4, 0);
        ASSERT_EQ_INT(rc, -1, "bm_set refuses past cap");
        ASSERT_EQ_INT((int)bm_n_values(bm), (int)BM_DEFAULT_MAX_VALUES,
                      "dict size unchanged after refusal");

        /* But existing values still work — the cap doesn't poison the
           shard, it just blocks growth. */
        ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"v0", 2, 1), 0,
                      "existing value v0 still settable post-cap");
        ASSERT_EQ_INT((int)bm_count(bm, (const uint8_t *)"v0", 2), 2,
                      "v0 has 2 bits set");
        bm_close(bm);
    }
    unlink(path);

    /* === Per-file cap override: declare a custom cap at create. === */
    bm = bm_open(path, 64, 1, 0, 4, 1);   /* cap = 4 */
    ASSERT_NOT_NULL(bm, "bm_open: cap-override create");
    if (bm) {
        ASSERT_EQ_INT((int)bm_max_values(bm), 4, "header has custom cap=4");
        ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"a", 1, 0), 0, "v1 ok");
        ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"b", 1, 1), 0, "v2 ok");
        ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"c", 1, 2), 0, "v3 ok");
        ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"d", 1, 3), 0, "v4 ok (cap)");
        ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"e", 1, 4), -1, "v5 refused at cap=4");
        ASSERT_EQ_INT((int)bm_n_values(bm), 4, "dict at custom cap exactly");

        /* Persistence of the custom cap. */
        bm_close(bm);
        bm = bm_open(path, 64, 0, 0, 0, 0);  /* arg ignored on existing file */
        ASSERT_NOT_NULL(bm, "reopen with cap baked in");
        if (bm) {
            ASSERT_EQ_INT((int)bm_max_values(bm), 4, "cap survives reopen");
            ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"e", 1, 5), -1,
                          "still refused after reopen");
            bm_close(bm);
        }
    }
    unlink(path);
    return 0;
}

typedef struct {
    TestClientCfg cfg;
    int idx;
    int rc; /* 0 = accepted, 1 = rejected (cap error), -1 = connect/IO failure */
} ConcCapWorkerCtx;

/* One thread = one connection = one single-record insert of a distinct
   value into the same (single-shard, bitmap-capped) object, fired
   concurrently with every other worker so the cap check/commit races for
   real instead of being serialized by the test driver. */
static void *conc_cap_worker(void *arg) {
    ConcCapWorkerCtx *w = (ConcCapWorkerCtx *)arg;
    TestClient *tc = tc_connect(&w->cfg);
    if (!tc) { w->rc = -1; return NULL; }
    char req[256], *resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"conccap\","
        "\"key\":\"cc%d\",\"value\":{\"v\":\"ccv%d\"}}", w->idx, w->idx);
    if (tc_request(tc, req, &resp) != 0) { w->rc = -1; tc_close(tc); return NULL; }
    w->rc = (resp && SAFE_STRSTR(resp, "\"error\"")) ? 1 : 0;
    free(resp);
    tc_close(tc);
    return NULL;
}

/* Fills keys[0..n) with distinct "cc<N>" keys that all hash to
   target_shard under `splits` — mirrors the bitmap dict cap's actual
   scoping (per field, per kf-shard), so the cap-boundary race below is
   deterministic instead of relying on natural pigeonhole collisions. */
static int cc_pick_same_shard_keys(int splits, int target_shard, int n, int *out_idx) {
    int found = 0;
    for (int cand = 0; cand < 100000 && found < n; cand++) {
        char key[32];
        snprintf(key, sizeof(key), "cc%d", cand);
        uint8_t hash16[16];
        compute_hash_raw(key, strlen(key), hash16);
        if (compute_record_shard(hash16, splits) == target_shard)
            out_idx[found++] = cand;
    }
    return found == n ? 0 : -1;
}

typedef struct {
    uint64_t bucket;             /* (kf shard, initial probe slot) */
    int candidate;
} ProbeCollisionCandidate;

static int probe_collision_cmp(const void *a, const void *b) {
    const ProbeCollisionCandidate *pa = a, *pb = b;
    return (pa->bucket > pb->bucket) - (pa->bucket < pb->bucket);
}

/* Find four keys with exactly the same initial kf probe position.  A
   same-shard-only fixture would not expose the reservation-hole bug: the
   rejected middle record must occupy the same probe chain as a surviving
   later record.  Sorting a modest candidate sample finds a quadruple by the
   birthday effect without a slow brute-force search for one fixed slot. */
static int pick_four_same_probe_keys(int splits, char out_keys[][32]) {
    enum { NCANDIDATES = 500000 };
    ProbeCollisionCandidate *candidates = malloc(sizeof(*candidates) * NCANDIDATES);
    if (!candidates) return -1;
    size_t slots = slotcask_default_slots_for_splits(splits);
    for (int cand = 0; cand < NCANDIDATES; cand++) {
        char key[32];
        uint8_t hash16[16];
        snprintf(key, sizeof(key), "sc%06d", cand);
        compute_hash_raw(key, strlen(key), hash16);
        candidates[cand].bucket = ((uint64_t)(unsigned)compute_record_shard(hash16, splits) << 32) |
                                  (uint64_t)kf_slot_for(hash16, slots);
        candidates[cand].candidate = cand;
    }
    qsort(candidates, NCANDIDATES, sizeof(*candidates), probe_collision_cmp);
    int rc = -1;
    for (int i = 0; i + 3 < NCANDIDATES; i++) {
        if (candidates[i].bucket != candidates[i + 3].bucket) continue;
        for (int k = 0; k < 4; k++)
            snprintf(out_keys[k], 32, "sc%06d", candidates[i + k].candidate);
        rc = 0;
        break;
    }
    free(candidates);
    return rc;
}

static int test_bitmap_index_run(void) {
    /* Phase 2 unit assertions: direct bitmap.c API, no daemon. */
    if (run_unit_assertions() != 0) return 1;

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"t\"}", &resp);
    ASSERT_CONTAINS(resp, "\"dir\":\"t\"", "add-dir t");
    free(resp); resp = NULL;

    /* === Happy path: explicit :bitmap / :trigram + composite btree === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"a\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":["
          "\"name:varchar:32\",\"score:int\","
          "\"dead:bool\",\"deleted:bool\","
          "\"type:varchar:16\",\"text:varchar:1024\"],"
        "\"indexes\":["
          "\"name\","                /* legacy bare → btree */
          "\"score\","
          "\"type:bitmap\","         /* opt-in varchar enum */
          "\"text:trigram\","
          "\"name+score\"]}",         /* composite stays btree */
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create object 'a'");
    free(resp); resp = NULL;

    /* describe-object surfaces the canonicalised :type strings + the
       auto-defaulted bitmap entries for both bool fields. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"a\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\"",          "describe: btree name");
    ASSERT_CONTAINS(resp, "\"type:bitmap\"",   "describe: explicit varchar bitmap");
    ASSERT_CONTAINS(resp, "\"text:trigram\"",  "describe: explicit varchar trigram");
    ASSERT_CONTAINS(resp, "\"name+score\"",    "describe: composite btree");
    ASSERT_CONTAINS(resp, "\"dead:bitmap\"",   "describe: auto-bitmap on dead");
    ASSERT_CONTAINS(resp, "\"deleted:bitmap\"","describe: auto-bitmap on deleted");
    free(resp); resp = NULL;

    /* === Error: bitmap on non-bool/non-varchar === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"err_int_bm\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"n:int\"],\"indexes\":[\"n:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "bitmap on int → error");
    ASSERT_CONTAINS(resp, "bitmap index requires", "error mentions bitmap contract");
    free(resp); resp = NULL;

    /* === Error: trigram on non-varchar === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"err_int_tg\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"n:int\"],\"indexes\":[\"n:trigram\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "trigram on int → error");
    ASSERT_CONTAINS(resp, "trigram index requires varchar", "error mentions trigram contract");
    free(resp); resp = NULL;

    /* === Error: composite + non-btree type === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"err_composite\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:bool\",\"b:bool\"],\"indexes\":[\"a+b:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "composite bitmap rejected");
    ASSERT_CONTAINS(resp, "composite indexes are btree-only", "error explains restriction");
    free(resp); resp = NULL;

    /* === Auto-default: bool fields with NO indexes declared still get bitmap === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"bools_only\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:bool\",\"b:bool\",\"c:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "bools_only created");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"bools_only\"}", &resp);
    ASSERT_CONTAINS(resp, "\"a:bitmap\"", "implicit bitmap on a");
    ASSERT_CONTAINS(resp, "\"b:bitmap\"", "implicit bitmap on b");
    /* `c:int` must NOT be indexed since user didn't ask for it. `c`
       legitimately appears in the fields section, so scope the check
       to the substring after `"indexes":[`. */
    {
        const char *idx_section = SAFE_STRSTR(resp, "\"indexes\":[");
        ASSERT_NOT_NULL((void *)idx_section, "indexes section present");
        if (idx_section) {
            const char *idx_end = strchr(idx_section, ']');
            char idx_buf[1024] = {0};
            if (idx_end && (size_t)(idx_end - idx_section) < sizeof(idx_buf)) {
                memcpy(idx_buf, idx_section, idx_end - idx_section);
            }
            ASSERT_TRUE(strstr(idx_buf, "\"c\"") == NULL && strstr(idx_buf, "\"c:") == NULL,
                        "c (int) NOT in indexes list");
        }
    }
    free(resp); resp = NULL;

    /* === Legacy / back-compat: bare names still mean btree, no implicit type. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"legacy\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"k:varchar:32\",\"s:int\"],"
        "\"indexes\":[\"k\",\"s\",\"k+s\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "legacy bare-name indexes");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"legacy\"}", &resp);
    /* No :type suffix on any entry — describes round-trip exactly what the
       user typed. */
    ASSERT_NOT_CONTAINS(resp, ":btree",   "no implicit :btree decoration");
    ASSERT_NOT_CONTAINS(resp, ":bitmap",  "no spurious bitmap");
    ASSERT_NOT_CONTAINS(resp, ":trigram", "no spurious trigram");
    free(resp); resp = NULL;

    /* === Explicit :bitmap on bool overrides the auto-default contract
           (no duplicate entry written). === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"explicit_bool_bm\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\"],\"indexes\":[\"flag:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "explicit bool bitmap created");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"explicit_bool_bm\"}", &resp);
    /* Substring count: "flag:bitmap" appears exactly once. We can't count
       directly via ASSERT_CONTAINS, but absence of a second one is
       confirmed by the field appearing exactly once. Sniff for both
       the canonical form and that no bare "flag" appears separately. */
    ASSERT_CONTAINS(resp, "\"flag:bitmap\"", "flag has bitmap");
    /* Check no `"flag",` or `"flag"]` (separate bare entry) follows the
       bitmap one. This is a regex-ish sniff. */
    {
        const char *first = SAFE_STRSTR(resp, "\"flag:bitmap\"");
        if (first) {
            /* Look for another "flag" anywhere AFTER first that isn't part of
               our :bitmap entry's surrounding braces. */
            const char *after = first + strlen("\"flag:bitmap\"");
            ASSERT_TRUE(strstr(after, "\"flag\"") == NULL,
                        "no second bare 'flag' entry");
            ASSERT_TRUE(strstr(after, "\"flag:bitmap\"") == NULL,
                        "no duplicate 'flag:bitmap' entry");
        }
    }
    free(resp); resp = NULL;

    /* === Bool listed in indexes WITHOUT :type auto-becomes bitmap.
           The escape hatch is explicit :btree. Same rule will apply to
           enum once that type lands. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"bool_listed\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\"],\"indexes\":[\"flag\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create bool_listed");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"bool_listed\"}", &resp);
    ASSERT_CONTAINS(resp, "\"flag:bitmap\"", "bool with bare 'flag' becomes bitmap");
    free(resp); resp = NULL;

    /* Explicit :btree on bool → respected (escape hatch). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"bool_btree\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\"],\"indexes\":[\"flag:btree\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create bool_btree");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"bool_btree\"}", &resp);
    ASSERT_NOT_CONTAINS(resp, "\"flag:bitmap\"", "explicit :btree NOT promoted to bitmap");
    free(resp); resp = NULL;

    /* === End-to-end: insert records, verify bits land in the bitmap shard
           files. Uses bm_open() directly to inspect the on-disk state the
           daemon wrote. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"e2e\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"label:varchar:16\"],"
        "\"indexes\":[\"label:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create e2e (bool auto + label:bitmap)");
    free(resp); resp = NULL;

    /* Insert 6 records with varied (flag, label) so each bool value gets
       some bits and the varchar enum has 3 distinct labels. */
    const struct { const char *k; const char *flag; const char *label; } rows[] = {
        { "k1", "true",  "alpha"   },
        { "k2", "false", "alpha"   },
        { "k3", "true",  "beta"    },
        { "k4", "false", "beta"    },
        { "k5", "true",  "gamma"   },
        { "k6", "true",  "gamma"   },
    };
    for (size_t i = 0; i < sizeof(rows)/sizeof(rows[0]); i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"e2e\","
            "\"key\":\"%s\",\"value\":{\"flag\":%s,\"label\":\"%s\"}}",
            rows[i].k, rows[i].flag, rows[i].label);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert e2e row");
        free(resp); resp = NULL;
    }

    /* Sum popcounts across all 8 data shards. Each bitmap shard file
       lives at <db_root>/t/e2e/indexes/<field>/<NNN>.bm. */
    uint32_t flag_true_count = 0, flag_false_count = 0;
    uint32_t alpha_count = 0, beta_count = 0, gamma_count = 0;
    for (int s = 0; s < 8; s++) {
        char bp[1024];
        snprintf(bp, sizeof(bp), "%s/t/e2e/indexes/flag/%03x.bm",
                 env.db_root, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
        if (bm) {
            uint8_t t = 0x01, f = 0x00;
            flag_true_count  += bm_count(bm, &t, 1);
            flag_false_count += bm_count(bm, &f, 1);
            bm_close(bm);
        }

        snprintf(bp, sizeof(bp), "%s/t/e2e/indexes/label/%03x.bm",
                 env.db_root, s);
        bm = bm_open(bp, 0, 0, 0, 0, 0);
        if (bm) {
            alpha_count += bm_count(bm, (const uint8_t *)"alpha", 5);
            beta_count  += bm_count(bm, (const uint8_t *)"beta",  4);
            gamma_count += bm_count(bm, (const uint8_t *)"gamma", 5);
            bm_close(bm);
        }
    }
    ASSERT_EQ_INT((int)flag_true_count,  4, "bitmap: 4 true bits set across shards");
    ASSERT_EQ_INT((int)flag_false_count, 2, "bitmap: 2 false bits set across shards");
    ASSERT_EQ_INT((int)alpha_count,      2, "bitmap: 2 alpha bits set");
    ASSERT_EQ_INT((int)beta_count,       2, "bitmap: 2 beta bits set");
    ASSERT_EQ_INT((int)gamma_count,      2, "bitmap: 2 gamma bits set");

    /* === Update one record's flag (true→false). Bit should move. === */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"t\",\"object\":\"e2e\","
        "\"key\":\"k1\",\"value\":{\"flag\":false,\"label\":\"alpha\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "update k1 flag true→false");
    free(resp); resp = NULL;

    flag_true_count = 0; flag_false_count = 0;
    for (int s = 0; s < 8; s++) {
        char bp[1024];
        snprintf(bp, sizeof(bp), "%s/t/e2e/indexes/flag/%03x.bm", env.db_root, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
        if (bm) {
            uint8_t t = 0x01, f = 0x00;
            flag_true_count  += bm_count(bm, &t, 1);
            flag_false_count += bm_count(bm, &f, 1);
            bm_close(bm);
        }
    }
    ASSERT_EQ_INT((int)flag_true_count,  3, "post-update: 3 true bits (was 4)");
    ASSERT_EQ_INT((int)flag_false_count, 3, "post-update: 3 false bits (was 2)");

    /* === Delete one record. Its bits should clear. === */
    tc_request(tc,
        "{\"mode\":\"delete\",\"dir\":\"t\",\"object\":\"e2e\",\"key\":\"k5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "delete k5");
    free(resp); resp = NULL;

    flag_true_count = 0; gamma_count = 0;
    for (int s = 0; s < 8; s++) {
        char bp[1024];
        snprintf(bp, sizeof(bp), "%s/t/e2e/indexes/flag/%03x.bm", env.db_root, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
        if (bm) {
            uint8_t t = 0x01;
            flag_true_count += bm_count(bm, &t, 1);
            bm_close(bm);
        }
        snprintf(bp, sizeof(bp), "%s/t/e2e/indexes/label/%03x.bm", env.db_root, s);
        bm = bm_open(bp, 0, 0, 0, 0, 0);
        if (bm) {
            gamma_count += bm_count(bm, (const uint8_t *)"gamma", 5);
            bm_close(bm);
        }
    }
    ASSERT_EQ_INT((int)flag_true_count, 2, "post-delete: 2 true bits (k5 cleared)");
    ASSERT_EQ_INT((int)gamma_count,     1, "post-delete: 1 gamma bit (was 2)");

    /* === Phase 5 reindex: rebuilds bitmap from existing records. Create
           an object, insert records, manually wipe the .bm file, run
           reindex via the CLI-equivalent query, verify the bitmap is
           repopulated to match the inserted state. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"reix\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"label:varchar:16\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create reix");
    free(resp); resp = NULL;

    /* Insert 5 records: 3 flag=true, 2 flag=false. */
    const char *reix_keys[] = { "r1", "r2", "r3", "r4", "r5" };
    const char *reix_flags[] = { "true", "true", "false", "true", "false" };
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"reix\","
            "\"key\":\"%s\",\"value\":{\"flag\":%s,\"label\":\"l%d\"}}",
            reix_keys[i], reix_flags[i], i);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }

    /* Sanity: bitmap should have 3 true + 2 false right after inserts. */
    {
        uint32_t pre_true = 0, pre_false = 0;
        for (int s = 0; s < 8; s++) {
            char bp[1024];
            snprintf(bp, sizeof(bp), "%s/t/reix/indexes/flag/%03x.bm", env.db_root, s);
            BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
            if (bm) {
                uint8_t t = 0x01, f = 0x00;
                pre_true  += bm_count(bm, &t, 1);
                pre_false += bm_count(bm, &f, 1);
                bm_close(bm);
            }
        }
        ASSERT_EQ_INT((int)pre_true,  3, "reix pre-wipe: 3 true bits");
        ASSERT_EQ_INT((int)pre_false, 2, "reix pre-wipe: 2 false bits");
    }

    /* Wipe all .bm files for `flag` to simulate "index missing", then
       run reindex via add-index force=true (which rebuilds). */
    for (int s = 0; s < 8; s++) {
        char bp[1024];
        snprintf(bp, sizeof(bp), "%s/t/reix/indexes/flag/%03x.bm", env.db_root, s);
        unlink(bp);
    }
    /* Verify all wiped. */
    {
        uint32_t wiped_true = 0;
        for (int s = 0; s < 8; s++) {
            char bp[1024];
            snprintf(bp, sizeof(bp), "%s/t/reix/indexes/flag/%03x.bm", env.db_root, s);
            BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
            if (bm) {
                uint8_t t = 0x01;
                wiped_true += bm_count(bm, &t, 1);
                bm_close(bm);
            }
        }
        ASSERT_EQ_INT((int)wiped_true, 0, "post-wipe: no true bits anywhere");
    }

    /* Rebuild via add-index force. */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"t\",\"object\":\"reix\","
        "\"fields\":[\"flag:bitmap\"],\"force\":\"true\"}", &resp);
    /* Either status:added or status:ok depending on the path taken. */
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "add-index force succeeded");
    free(resp); resp = NULL;

    /* Confirm bits repopulated. */
    {
        uint32_t post_true = 0, post_false = 0;
        for (int s = 0; s < 8; s++) {
            char bp[1024];
            snprintf(bp, sizeof(bp), "%s/t/reix/indexes/flag/%03x.bm", env.db_root, s);
            BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
            if (bm) {
                uint8_t t = 0x01, f = 0x00;
                post_true  += bm_count(bm, &t, 1);
                post_false += bm_count(bm, &f, 1);
                bm_close(bm);
            }
        }
        ASSERT_EQ_INT((int)post_true,  3, "reindex: 3 true bits restored");
        ASSERT_EQ_INT((int)post_false, 2, "reindex: 2 false bits restored");
    }

    /* === Phase 5b: mode:reindex rebuilds the bitmap via the unified
           segment-sequential scan + resolve_bitmaps (kf hash→slot join),
           a DIFFERENT path than add-index/build_bitmap_pass above. Wipe,
           reindex via mode, then verify both the raw bits AND a bitmap-
           driven query resolve to the correct records. === */
    for (int s = 0; s < 8; s++) {
        char bp[1024];
        snprintf(bp, sizeof(bp), "%s/t/reix/indexes/flag/%03x.bm", env.db_root, s);
        unlink(bp);
    }
    tc_request(tc,
        "{\"mode\":\"reindex\",\"dir\":\"t\",\"object\":\"reix\"}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "mode:reindex succeeded");
    free(resp); resp = NULL;

    {
        uint32_t post_true = 0, post_false = 0;
        for (int s = 0; s < 8; s++) {
            char bp[1024];
            snprintf(bp, sizeof(bp), "%s/t/reix/indexes/flag/%03x.bm", env.db_root, s);
            BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
            if (bm) {
                uint8_t t = 0x01, f = 0x00;
                post_true  += bm_count(bm, &t, 1);
                post_false += bm_count(bm, &f, 1);
                bm_close(bm);
            }
        }
        ASSERT_EQ_INT((int)post_true,  3, "mode:reindex: 3 true bits restored");
        ASSERT_EQ_INT((int)post_false, 2, "mode:reindex: 2 false bits restored");
    }

    /* Bitmap-driven query must return the right records — proves the
       resolved slots map back to the correct keys through the planner. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"reix\","
        "\"criteria\":{\"flag\":true}}", &resp);
    ASSERT_CONTAINS(resp, "3", "mode:reindex: count flag=true == 3");
    free(resp); resp = NULL;

    /* === Phase 3.2 auto-grow: bitmap_update must extend the bitmap when
           a write lands at a slot beyond the current stride. Direct
           bitmap.c API exercise (no daemon) — create a tiny bitmap then
           call bm_set at slot > current_slots to confirm grow fires. */
    {
        const char *gp = "/tmp/shard-db-test-bm-autogrow.bm";
        unlink(gp);
        BitmapShard *gbm = bm_open(gp, 64, 1, 1, 0, 1);
        ASSERT_NOT_NULL(gbm, "autogrow: create at slots=64");
        if (gbm) {
            /* Simulate the bitmap_update auto-grow path manually: detect
               OOB slot, grow, then set. */
            uint8_t v_true[1] = { 0x01 };
            uint32_t target_slot = 200;
            if (target_slot >= bm_slots(gbm)) {
                uint32_t grown = 1;
                while (grown < target_slot + 1) grown <<= 1;
                ASSERT_EQ_INT(bm_grow(gbm, grown), 0, "autogrow: bm_grow returns 0");
                ASSERT_EQ_INT((int)bm_slots(gbm), 256, "autogrow: slots = 256 (rounded up)");
            }
            ASSERT_EQ_INT(bm_set(gbm, v_true, 1, target_slot), 0,
                          "autogrow: set bit at high slot ok");
            ASSERT_EQ_INT(bm_test(gbm, v_true, 1, target_slot), 1,
                          "autogrow: bit reads back set");
            bm_close(gbm);
        }
        unlink(gp);
    }

    /* === Phase 4: read-side planner uses the bitmap. Find / count / find-with-AND
           on a bitmap-indexed bool field must return the same results as
           the equivalent full-scan query. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"qry\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"name:varchar:8\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create qry");
    free(resp); resp = NULL;

    /* Insert: 6 records, 4 with flag=true, 2 with flag=false. Specific
       names so we can validate AND-with-btree later if needed. */
    const struct { const char *k; const char *flag; const char *name; } qrows[] = {
        { "q1", "true",  "alice" },
        { "q2", "false", "bob"   },
        { "q3", "true",  "carol" },
        { "q4", "true",  "alice" },
        { "q5", "false", "alice" },
        { "q6", "true",  "dave"  },
    };
    for (size_t i = 0; i < sizeof(qrows)/sizeof(qrows[0]); i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"qry\","
            "\"key\":\"%s\",\"value\":{\"flag\":%s,\"name\":\"%s\"}}",
            qrows[i].k, qrows[i].flag, qrows[i].name);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "qry row inserted");
        free(resp); resp = NULL;
    }

    /* count(flag=true) MUST be 4. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"qry\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "4", "count flag=true via bitmap planner");
    free(resp); resp = NULL;

    /* count(flag=false) MUST be 2. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"qry\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "count flag=false via bitmap planner");
    free(resp); resp = NULL;

    /* find(flag=true) returns all 4 keys. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"t\",\"object\":\"qry\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"fields\":[\"name\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"q1\"", "find flag=true: q1");
    ASSERT_CONTAINS(resp, "\"q3\"", "find flag=true: q3");
    ASSERT_CONTAINS(resp, "\"q4\"", "find flag=true: q4");
    ASSERT_CONTAINS(resp, "\"q6\"", "find flag=true: q6");
    ASSERT_NOT_CONTAINS(resp, "\"q2\"", "find flag=true: q2 absent");
    ASSERT_NOT_CONTAINS(resp, "\"q5\"", "find flag=true: q5 absent");
    free(resp); resp = NULL;

    /* === Bitmap cap override `:bitmap(N)` round-trips through create-object
           + describe-object + index.conf. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"capover\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"region:varchar:8\"],"
        "\"indexes\":[\"region:bitmap(1024)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create with bitmap(1024)");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"capover\"}", &resp);
    ASSERT_CONTAINS(resp, "\"region:bitmap(1024)\"", "describe surfaces cap");
    free(resp); resp = NULL;

    /* Reject cap out of [2, BM_HARD_CEILING]. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"cap_lo\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"k:varchar:8\"],\"indexes\":[\"k:bitmap(1)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "bitmap cap=1 rejected");
    ASSERT_CONTAINS(resp, "out of range", "error mentions range");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"cap_hi\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"k:varchar:8\"],\"indexes\":[\"k:bitmap(99999)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "bitmap cap=99999 rejected");
    ASSERT_CONTAINS(resp, "out of range", "error mentions range");
    free(resp); resp = NULL;

    /* === Cap-aware CRUD: a custom :bitmap(N) cap must be respected at
           insert time. Default cap (256) gets exercised by the e2e block
           above; this block hits the override path. We create with
           bitmap(4), insert 4 distinct values (all succeed), and confirm
           the on-disk header carries cap=4 (the file was materialised at
           create-object with the right value). The insert-past-cap
           refusal lands in a richer test once Phase 4 surfaces the
           error path through the wire — for now we verify the header
           is correctly stamped, which is what CRUD reads on each open. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"capcrud\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"col:varchar:8\"],\"indexes\":[\"col:bitmap(4)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create capcrud bitmap(4)");
    free(resp); resp = NULL;

    /* Open shard 0 directly and confirm the header has cap=4. The file
       was pre-materialised in cmd_create_object so every data shard
       carries the same cap. */
    {
        char bp[1024];
        snprintf(bp, sizeof(bp), "%s/t/capcrud/indexes/col/000.bm", env.db_root);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
        ASSERT_NOT_NULL(bm, "pre-materialised bitmap shard present");
        if (bm) {
            ASSERT_EQ_INT((int)bm_max_values(bm), 4, "header carries cap=4");
            bm_close(bm);
        }
    }

    /* Insert 4 records with 4 distinct labels — all four succeed. */
    const char *capcrud_labels[] = { "red", "blu", "grn", "ylw" };
    for (int i = 0; i < 4; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"capcrud\","
            "\"key\":\"k%d\",\"value\":{\"col\":\"%s\"}}", i, capcrud_labels[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "capcrud row inserted");
        free(resp); resp = NULL;
    }

    /* Inspect: the 4 distinct labels landed somewhere across the 8 shards.
       Sum n_values across shards == 4 distinct varchar values seen total. */
    int total_n_values = 0;
    int total_set_bits = 0;
    for (int s = 0; s < 8; s++) {
        char bp[1024];
        snprintf(bp, sizeof(bp), "%s/t/capcrud/indexes/col/%03x.bm", env.db_root, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
        if (bm) {
            total_n_values += (int)bm_n_values(bm);
            for (int i = 0; i < 4; i++) {
                total_set_bits += (int)bm_count(
                    bm, (const uint8_t *)capcrud_labels[i],
                    strlen(capcrud_labels[i]));
            }
            bm_close(bm);
        }
    }
    /* Each label lives in whichever shard the key hashes into, so the
       n_values total isn't strictly 4 — distinct-per-shard ≤ 4. But the
       total set bits across shards == 4 (one per insert). */
    ASSERT_EQ_INT(total_set_bits, 4, "4 bits set across all shards");
    ASSERT_TRUE(total_n_values >= 1 && total_n_values <= 4,
                "per-shard dict size within cap");

    /* === Bulk-insert maintains bitmap shards. Insert 10 records via
           one bulk-insert call; verify the on-disk bit counts match. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create bulk (auto-bitmap on flag)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"t\",\"object\":\"bulk\",\"records\":["
          "{\"key\":\"b1\",\"value\":{\"flag\":true}},"
          "{\"key\":\"b2\",\"value\":{\"flag\":true}},"
          "{\"key\":\"b3\",\"value\":{\"flag\":true}},"
          "{\"key\":\"b4\",\"value\":{\"flag\":false}},"
          "{\"key\":\"b5\",\"value\":{\"flag\":false}},"
          "{\"key\":\"b6\",\"value\":{\"flag\":true}},"
          "{\"key\":\"b7\",\"value\":{\"flag\":true}},"
          "{\"key\":\"b8\",\"value\":{\"flag\":false}},"
          "{\"key\":\"b9\",\"value\":{\"flag\":true}},"
          "{\"key\":\"b10\",\"value\":{\"flag\":false}}"
        "]}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":10", "bulk inserted 10");
    free(resp); resp = NULL;

    uint32_t bulk_true = 0, bulk_false = 0;
    for (int s = 0; s < 8; s++) {
        char bp[1024];
        snprintf(bp, sizeof(bp), "%s/t/bulk/indexes/flag/%03x.bm", env.db_root, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
        if (bm) {
            uint8_t t = 0x01, f = 0x00;
            bulk_true  += bm_count(bm, &t, 1);
            bulk_false += bm_count(bm, &f, 1);
            bm_close(bm);
        }
    }
    ASSERT_EQ_INT((int)bulk_true,  6, "bulk: 6 true bits across all shards");
    ASSERT_EQ_INT((int)bulk_false, 4, "bulk: 4 false bits across all shards");

    /* === Operator coverage on bitmap-indexed bool field.
           btree parity: every applicable op routes through the bitmap
           index, never falls to a full data-shard scan. eq/IN go through
           the popcount fast path; everything else (NEQ via subtraction,
           range, len_*) goes through the dict-scan dispatch.
           Schema: 10 records, flag=true ×6, flag=false ×4. === */

    /* count eq */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}", &resp);
    ASSERT_CONTAINS(resp, "6", "count flag=true → 6");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}]}", &resp);
    ASSERT_CONTAINS(resp, "4", "count flag=false → 4");
    free(resp); resp = NULL;

    /* count neq — via the negation subtraction shortcut on bitmap.
       Was the 956ms regression on the 25M bench before [[feedback-btree-pattern-reference]]. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"neq\",\"value\":\"true\"}]}", &resp);
    ASSERT_CONTAINS(resp, "4", "count flag!=true → 4 (neq shortcut)");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"neq\",\"value\":\"false\"}]}", &resp);
    ASSERT_CONTAINS(resp, "6", "count flag!=false → 6 (neq shortcut)");
    free(resp); resp = NULL;

    /* count IN — single-value uses the per-value popcount fast path. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"in\",\"value\":[\"true\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "6", "count flag in [true] → 6");
    free(resp); resp = NULL;
    /* count IN both → whole-domain shortcut returns live_count (10). */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"in\",\"value\":[\"true\",\"false\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "10", "count flag in [true,false] → 10");
    free(resp); resp = NULL;

    /* count NOT_IN — single-value via op_invert → IN. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"not_in\",\"value\":[\"true\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "4", "count flag not_in [true] → 4");
    free(resp); resp = NULL;

    /* count range on bool — exercises the generic dict-scan path.
       bool false < true; the dict-scan iterates the 2-value dict,
       runs match_criterion against each decoded value, sums bitmaps. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"gt\",\"value\":\"false\"}]}", &resp);
    ASSERT_CONTAINS(resp, "6", "count flag > false → 6 (dict-scan range)");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"lt\",\"value\":\"true\"}]}", &resp);
    ASSERT_CONTAINS(resp, "4", "count flag < true → 4 (dict-scan range)");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"gte\",\"value\":\"true\"}]}", &resp);
    ASSERT_CONTAINS(resp, "6", "count flag >= true → 6 (dict-scan range)");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"lte\",\"value\":\"false\"}]}", &resp);
    ASSERT_CONTAINS(resp, "4", "count flag <= false → 4 (dict-scan range)");
    free(resp); resp = NULL;

    /* count exists on typed bool — already shortcut to live_count at
       the planner level; assert it stays right. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"exists\"}]}", &resp);
    ASSERT_CONTAINS(resp, "10", "count flag exists → 10");
    free(resp); resp = NULL;

    /* find via NEQ — routes through the keyset-from-bitmap path. */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"neq\",\"value\":\"true\"}],"
        "\"limit\":100}", &resp);
    /* All 4 false-flag records must appear. Their keys are b4, b5, b8, b10. */
    ASSERT_CONTAINS(resp, "\"b4\"",  "find flag!=true returns b4");
    ASSERT_CONTAINS(resp, "\"b5\"",  "find flag!=true returns b5");
    ASSERT_CONTAINS(resp, "\"b8\"",  "find flag!=true returns b8");
    ASSERT_CONTAINS(resp, "\"b10\"", "find flag!=true returns b10");
    free(resp); resp = NULL;

    /* find via range — same set should appear via generic dict-scan. */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"t\",\"object\":\"bulk\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"lt\",\"value\":\"true\"}],"
        "\"limit\":100}", &resp);
    ASSERT_CONTAINS(resp, "\"b4\"",  "find flag<true returns b4 (range via dict)");
    ASSERT_CONTAINS(resp, "\"b5\"",  "find flag<true returns b5 (range via dict)");
    ASSERT_CONTAINS(resp, "\"b8\"",  "find flag<true returns b8 (range via dict)");
    ASSERT_CONTAINS(resp, "\"b10\"", "find flag<true returns b10 (range via dict)");
    free(resp); resp = NULL;

    /* === 5th distinct value past the per-file cap on the SAME data
           shard must fail. The cap is per-shard (not global), so the
           insert needs to target a shard that already has 4 distinct
           values. We can't easily predict which shard the test keys
           hash into, so create a fresh narrow-cap bitmap with splits=8
           and use a fixed-shard placement trick: pick keys that all
           fall in the same xxh-derived shard. As a more robust
           shortcut for the assertion, drop to a single-shard object
           (splits=8 still) and stuff 5 distinct values targeting
           shard 0 until we trip the cap. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"capovf\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:4\"],\"indexes\":[\"v:bitmap(2)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create capovf bitmap(2)");
    free(resp); resp = NULL;

    /* Use sequential keys k0..k2 with 3 distinct values; let the
       insert path tell us when the cap is hit. The 5th insert across
       ALL shards is fine because the cap is per-shard; what we test
       is "if any shard accumulates >cap distinct values, that insert
       fails". A 3-distinct varchar spread of 3 inserts only fails if
       all three land in the same shard — possible at splits=8 with
       enough attempts. Practical assertion: try 24 inserts of 24
       distinct values; some MUST collide and trip the cap. */
    int saw_cap_error = 0;
    int actionable_msg = 0;
    int rejected_k = -1;
    for (int k = 0; k < 24; k++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"capovf\","
            "\"key\":\"k%d\",\"value\":{\"v\":\"v%d\"}}", k, k);
        tc_request(tc, req, &resp);
        if (resp && SAFE_STRSTR(resp, "\"error\"")) {
            saw_cap_error = 1;
            rejected_k = k;
            if (SAFE_STRSTR(resp, "bitmap index on field 'v' exceeded") &&
                SAFE_STRSTR(resp, "or switch to btree")) {
                actionable_msg = 1;
            }
            free(resp); resp = NULL;
            break;
        }
        free(resp); resp = NULL;
    }
    ASSERT_TRUE(saw_cap_error,
                "bitmap(2) cap eventually trips across 24 distinct values");
    ASSERT_TRUE(actionable_msg,
                "cap-exceeded error names the field + suggests btree");

    /* === Daemon survives repeated cap rejections (the original bug: this
           used to abort() the whole daemon once kf became durable before
           pre_commit ran). Re-hit the *same* over-cap shard a few more
           times with the exact key/value that already tripped it — new
           distinct values would just as likely land on a different,
           not-yet-full shard (bitmap dict cap is scoped per (field,
           kf-shard), not per object), so retrying the identical pair is
           the only way to deterministically keep hitting a full shard.
           Prove: the daemon keeps answering, the rejected key never
           landed, and an update to an already-accepted key/value in that
           same shard's dict still works afterward (dict/lock state for
           the capped shard isn't corrupted). */
    if (saw_cap_error && rejected_k >= 0) {
        char rereq[256];
        snprintf(rereq, sizeof(rereq),
            "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"capovf\","
            "\"key\":\"k%d\",\"value\":{\"v\":\"v%d\"}}", rejected_k, rejected_k);
        int repeat_cap_errors = 0;
        for (int i = 0; i < 5; i++) {
            tc_request(tc, rereq, &resp);
            if (resp && SAFE_STRSTR(resp, "\"error\"")) repeat_cap_errors++;
            free(resp); resp = NULL;
        }
        ASSERT_EQ_INT(repeat_cap_errors, 5,
                    "daemon still alive and consistently rejecting the same "
                    "over-cap insert on retry");

        char existsreq[256];
        snprintf(existsreq, sizeof(existsreq),
            "{\"mode\":\"exists\",\"dir\":\"t\",\"object\":\"capovf\",\"key\":\"k%d\"}",
            rejected_k);
        tc_request(tc, existsreq, &resp);
        ASSERT_CONTAINS(resp, "false", "rejected key never landed (exists:false)");
        free(resp); resp = NULL;

        /* Key k0's own shard already has v0 in its dict (k0 was among the
           first successful inserts in the loop above); updating k0 back
           to v0 doesn't add a new distinct value, so it must still
           succeed even though that same key's kf-shard may or may not be
           the one that tripped the cap — proves the dict/lock state for
           an already-accepted entry keeps working after a cap rejection
           elsewhere. */
        tc_request(tc,
            "{\"mode\":\"update\",\"dir\":\"t\",\"object\":\"capovf\","
            "\"key\":\"k0\",\"value\":{\"v\":\"v0\"}}", &resp);
        ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""),
                    "update of an already-accepted key/value still succeeds "
                    "after a cap rejection");
        free(resp); resp = NULL;
    }

    /* === Bulk-window partial rejection: one bulk-insert call carrying
           enough distinct values that a bitmap(2) shard cap trips partway
           through the window. Exercises v2_bulk_ins_prepare_window /
           v2_bulk_ins_apply_window (the two-phase bulk fix) — asserts the
           rejected record(s) alone get dropped, every other record in the
           same window still commits (both before and after the rejection
           point), and the daemon/dict/lock state survive intact. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"bulkcapovf\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:32\"],\"indexes\":[\"v:bitmap(2)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create bulkcapovf bitmap(2)");
    free(resp); resp = NULL;

    {
        /* 20 distinct values across 8 shards, cap=2/shard: pigeonhole
           guarantees at least one shard exceeds its cap within the window
           (avg 2.5 distinct values/shard > cap 2). */
        char req[2048];
        int off = snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"t\",\"object\":\"bulkcapovf\",\"records\":[");
        for (int k = 0; k < 20 && (size_t)off < sizeof(req); k++) {
            off += snprintf(req + off, sizeof(req) - (size_t)off,
                "%s{\"key\":\"bc%d\",\"value\":{\"v\":\"bcv%d\"}}", k ? "," : "", k, k);
        }
        if ((size_t)off < sizeof(req)) snprintf(req + off, sizeof(req) - (size_t)off, "]}");
        tc_request(tc, req, &resp);
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"errors\":") &&
                    !SAFE_STRSTR(resp, "\"inserted\":20"),
                    "bulk window: at least one record rejected by the bitmap cap");
        free(resp); resp = NULL;
    }

    int bulkcap_existing = 0;
    int bulkcap_exists[20];
    int first_rejected = -1;
    int survivor_after_rejection = -1;
    for (int k = 0; k < 20; k++) {
        char req[128];
        snprintf(req, sizeof(req),
            "{\"mode\":\"exists\",\"dir\":\"t\",\"object\":\"bulkcapovf\",\"key\":\"bc%d\"}", k);
        tc_request(tc, req, &resp);
        bulkcap_exists[k] = (resp && SAFE_STRSTR(resp, "true")) ? 1 : 0;
        if (bulkcap_exists[k]) bulkcap_existing++;
        else if (first_rejected < 0) first_rejected = k;
        free(resp); resp = NULL;
    }
    ASSERT_TRUE(bulkcap_existing > 0 && bulkcap_existing < 20,
                "bulk window: surviving records (before AND after the rejected "
                "one) committed, only the capped record(s) dropped");
    ASSERT_TRUE(first_rejected >= 0,
                "bulk window: at least one specific record identified as rejected "
                "(exists:false)");
    if (first_rejected >= 0) {
        for (int k = first_rejected + 1; k < 20; k++) {
            if (bulkcap_exists[k]) { survivor_after_rejection = k; break; }
        }
        ASSERT_TRUE(survivor_after_rejection >= 0,
                    "bulk window: a record positioned AFTER the first rejected "
                    "record still committed — a rejection mid-window does not "
                    "abort or drop the rest of the window");
    }

    /* A stronger form of the partial-rejection case: all four records
       begin at the same kf probe slot. Two distinct values fill bitmap(2),
       the third is rejected, and the final record shares an accepted value
       and must still commit.  Before reservation re-planning, that final
       entry could be published after the rejected record's reserved slot,
       making it unreachable to normal open-address lookup. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"bulkcapprobe\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:32\"],\"indexes\":[\"v:bitmap(2)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                    "create probe-collision bitmap(2) fixture");
    free(resp); resp = NULL;

    {
        char keys[4][32];
        ASSERT_EQ_INT(pick_four_same_probe_keys(8, keys), 0,
                      "find four keys with one identical initial kf probe slot");
        char req[768];
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"t\",\"object\":\"bulkcapprobe\","
            "\"records\":[{\"key\":\"%s\",\"value\":{\"v\":\"kept\"}},"
            "{\"key\":\"%s\",\"value\":{\"v\":\"also-kept\"}},"
            "{\"key\":\"%s\",\"value\":{\"v\":\"rejected\"}},"
            "{\"key\":\"%s\",\"value\":{\"v\":\"kept\"}}]}",
            keys[0], keys[1], keys[2], keys[3]);
        tc_request(tc, req, &resp);
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"errors\":"),
                    "colliding bulk window rejects only the cap-exceeding record");
        free(resp); resp = NULL;

        for (int k = 0; k < 4; k++) {
            char exists_req[192];
            snprintf(exists_req, sizeof(exists_req),
                "{\"mode\":\"exists\",\"dir\":\"t\",\"object\":\"bulkcapprobe\",\"key\":\"%s\"}",
                keys[k]);
            tc_request(tc, exists_req, &resp);
            ASSERT_TRUE(resp && (k == 2 ? SAFE_STRSTR(resp, "true") == NULL
                                         : SAFE_STRSTR(resp, "true") != NULL),
                        k == 2 ? "rejected colliding key is absent"
                               : "surviving colliding key is reachable through its kf probe chain");
            free(resp); resp = NULL;
        }
    }

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"bulkcapovf\","
        "\"key\":\"bcpost\",\"value\":{\"v\":\"bcv0\"}}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""),
                "daemon alive: post-bulk-window-rejection insert still succeeds");
    free(resp); resp = NULL;

    {
        char saved_root[256];
        int  saved_port = env.port;
        snprintf(saved_root, sizeof(saved_root), "%s", env.db_root);
        tc_close(tc);
        tc = NULL;
        test_env_stop_keep(&env);

        ASSERT_EQ_INT(test_env_start_at(&env, saved_root, saved_port), 0,
                      "restart after bulk-window partial rejection");
        tc = tc_connect(&cfg);
        ASSERT_NOT_NULL(tc, "reconnect after bulk-window restart");
        if (tc) {
            int post_existing = 0;
            for (int k = 0; k < 20; k++) {
                char req[128];
                snprintf(req, sizeof(req),
                    "{\"mode\":\"exists\",\"dir\":\"t\",\"object\":\"bulkcapovf\",\"key\":\"bc%d\"}", k);
                tc_request(tc, req, &resp);
                if (!resp || SAFE_STRSTR(resp, "\"error\""))
                    TAP_DIAG("# bulkcapovf restart exists response for bc%d: %s\n",
                             k, resp ? resp : "(null)");
                ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""),
                            "bulkcapovf restart exists response");
                if (resp && SAFE_STRSTR(resp, "true")) post_existing++;
                free(resp); resp = NULL;
            }
            ASSERT_EQ_INT(post_existing, bulkcap_existing,
                          "bulk-window survivors unchanged across restart "
                          "(no orphaned marker left to replay)");
        }
    }

    /* === Concurrent cap-boundary: many connections race to insert distinct
           values into the same single-shard bitmap(CAP) object at once, all
           firing before any of them completes. Exercises the cap
           check-then-commit path under real cross-connection concurrency —
           the per-(field, kf-shard) dict cap must land on exactly CAP
           accepted values, never more (a race letting two threads both pass
           the cap check before either commits) and never fewer (a race
           incorrectly rejecting a value that had room). === */
    {
        const int conc_splits = 8;
        const int conc_cap = 8;
        const int conc_n = 20;
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"conccap\","
            "\"splits\":%d,\"max_key\":16,"
            "\"fields\":[\"v:varchar:32\"],\"indexes\":[\"v:bitmap(%d)\"]}",
            conc_splits, conc_cap);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                        "create conccap: bitmap(8) for concurrent race");
        free(resp); resp = NULL;

        int key_idx[20];
        ASSERT_EQ_INT(cc_pick_same_shard_keys(conc_splits, 0, conc_n, key_idx), 0,
                      "found 20 keys all hashing to the same kf shard");

        pthread_t threads[20];
        ConcCapWorkerCtx ctxs[20];
        for (int i = 0; i < conc_n; i++) {
            ctxs[i].cfg = cfg;
            ctxs[i].idx = key_idx[i];
            ctxs[i].rc = -2;
            ASSERT_EQ_INT(pthread_create(&threads[i], NULL, conc_cap_worker, &ctxs[i]), 0,
                          "spawn concurrent cap-boundary insert thread");
        }
        for (int i = 0; i < conc_n; i++) pthread_join(threads[i], NULL);

        int accepted = 0, rejected = 0, errored = 0;
        for (int i = 0; i < conc_n; i++) {
            if (ctxs[i].rc == 0) accepted++;
            else if (ctxs[i].rc == 1) rejected++;
            else errored++;
        }
        ASSERT_EQ_INT(errored, 0,
                      "all concurrent cap-boundary requests completed (no connect/IO errors)");
        ASSERT_EQ_INT(accepted, conc_cap,
                      "concurrent race: exactly the shard's cap worth of distinct "
                      "values accepted, no more and no fewer");
        ASSERT_EQ_INT(rejected, conc_n - conc_cap,
                      "concurrent race: every value beyond the cap rejected");

        int exists_count = 0;
        for (int i = 0; i < conc_n; i++) {
            char ereq[128];
            snprintf(ereq, sizeof(ereq),
                "{\"mode\":\"exists\",\"dir\":\"t\",\"object\":\"conccap\",\"key\":\"cc%d\"}",
                key_idx[i]);
            tc_request(tc, ereq, &resp);
            if (resp && SAFE_STRSTR(resp, "true")) exists_count++;
            free(resp); resp = NULL;
        }
        ASSERT_EQ_INT(exists_count, conc_cap,
                      "on-disk state matches: exactly CAP keys durable after the race");
    }

    /* === Restart: schema + index.conf survive across daemon stop/start.
           No `test_env_restart` helper — compose it from stop_keep + start_at,
           which is what the framework gives us. The fresh daemon should
           read index.conf from disk and surface the exact same line set. */
    {
        char saved_root[256];
        int  saved_port = env.port;
        snprintf(saved_root, sizeof(saved_root), "%s", env.db_root);
        tc_close(tc);
        tc = NULL;
        test_env_stop_keep(&env);

        TestEnv env2 = {0};
        ASSERT_EQ_INT(test_env_start_at(&env2, saved_root, saved_port), 0,
                      "daemon restart at same db_root + port");
        TestClient *tc2 = tc_connect(&cfg);
        ASSERT_NOT_NULL(tc2, "reconnect after restart");
        if (tc2) {
            tc_request(tc2, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"a\"}", &resp);
            ASSERT_CONTAINS(resp, "\"type:bitmap\"",   "post-restart: bitmap preserved");
            ASSERT_CONTAINS(resp, "\"text:trigram\"",  "post-restart: trigram preserved");
            ASSERT_CONTAINS(resp, "\"dead:bitmap\"",   "post-restart: auto-bitmap preserved");
            free(resp); resp = NULL;
            tc_close(tc2);
        }
        test_env_stop(&env2);
    }
    if (tc) tc_close(tc);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ── Post-review regression (2026-08-01 corrective plan, Task 1) ──
 *
 * Bitmap counterpart of the btree bounded-deadlock regression: retain a
 * reader handle on A, publish a replacement of A, acquire/use B while A
 * remains retained, then prove a post-publication A acquire sees the new
 * bitmap only. Pre-fix: bm_publish_replace holds the global publication
 * gate and blocks on A's cache-entry rwlock while bm_open(B) blocks on
 * the gate — the child hangs and the parent kills it at the bound.
 */
#define PUB_BM_SLOTS 256

typedef struct {
    const char *target;
    const char *tmp;
    ShardDb    *db;
    int         rc;
    atomic_int *done;
} PubBmArgs;

static void *pub_bm_publish_thread_main(void *arg) {
    PubBmArgs *a = arg;
    g_db = a->db;
    BitmapShard *bm = bm_open(a->tmp, PUB_BM_SLOTS, 1, 0, 0, 1);
    if (!bm) { a->rc = -1; atomic_store(a->done, 1); return NULL; }
    for (uint32_t s = 0; s < 5; s++)
        bm_set(bm, (const uint8_t *)"new", 3, s);
    a->rc = bm_sync(bm) != 0 ? -1 : 0;
    bm_close(bm);
    if (a->rc == 0) a->rc = bm_publish_replace(a->target, a->tmp);
    atomic_store(a->done, 1);
    return NULL;
}

static int run_bm_publish_deadlock_child(const char *base) {
    bm_cache_shutdown();
    bm_cache_init(16);

    char path_a[PATH_MAX], path_b[PATH_MAX], path_tmp[PATH_MAX];
    snprintf(path_a, sizeof(path_a), "%s/pub_a.bm", base);
    snprintf(path_b, sizeof(path_b), "%s/pub_b.bm", base);
    snprintf(path_tmp, sizeof(path_tmp), "%s/pub_tmp.bm", base);

    BitmapShard *bm = bm_open(path_a, PUB_BM_SLOTS, 1, 0, 0, 1);
    if (!bm) _exit(2);
    for (uint32_t s = 0; s < 5; s++)
        bm_set(bm, (const uint8_t *)"old", 3, s);
    if (bm_sync(bm) != 0) _exit(2);
    bm_close(bm);

    bm = bm_open(path_b, PUB_BM_SLOTS, 1, 0, 0, 1);
    if (!bm) _exit(2);
    for (uint32_t s = 0; s < 3; s++)
        bm_set(bm, (const uint8_t *)"bval", 4, s);
    if (bm_sync(bm) != 0) _exit(2);
    bm_close(bm);

    ShardDb *db = g_db;
    snprintf(db->durability_test_pause_phase,
             sizeof(db->durability_test_pause_phase), "%s",
             "bm-publish-before-rename");
    db->durability_test_pause_ms = 2000;

    /* Retain a real rdlock on A's cache entry. */
    BitmapShard *retained = bm_open(path_a, 0, 0, 0, 0, 0);
    if (!retained) _exit(3);

    atomic_int pub_done = 0;
    PubBmArgs margs = { .target = path_a, .tmp = path_tmp, .db = db,
                        .rc = -999, .done = &pub_done };
    pthread_t pub_tid;
    pthread_create(&pub_tid, NULL, pub_bm_publish_thread_main, &margs);

    int marker_seen = 0;
    {
        char marker[PATH_MAX];
        snprintf(marker, sizeof(marker), "%s/.durability-test-%s.active",
                 base, "bm-publish-before-rename");
        for (int tick = 0; tick < 50; tick++) {
            struct stat st;
            if (stat(marker, &st) == 0) { marker_seen = 1; break; }
            usleep(100 * 1000);
        }
    }

    /* Must complete before releasing A. Pre-fix this blocks forever on the
       global publication gate. */
    BitmapShard *b2 = bm_open(path_b, 0, 0, 0, 0, 0);
    if (!b2) _exit(4);
    if (bm_count(b2, (const uint8_t *)"bval", 4) != 3) _exit(5);
    bm_close(b2);

    /* Release A's retained rdlock, then let publication finish. */
    bm_close(retained);

    if (marker_seen) {
        char marker[PATH_MAX];
        snprintf(marker, sizeof(marker), "%s/.durability-test-%s.active",
                 base, "bm-publish-before-rename");
        for (int tick = 0; tick < 50; tick++) {
            struct stat st;
            if (stat(marker, &st) != 0) break;
            usleep(100 * 1000);
            if (tick == 49) _exit(6);
        }
    }

    for (int waited = 0; !atomic_load(&pub_done); waited++) {
        if (waited >= 100) _exit(7);
        usleep(100 * 1000);
    }
    pthread_join(pub_tid, NULL);
    if (margs.rc != BM_PUBLISH_OK) _exit(8);

    /* Post-publication A acquire sees the new bitmap only. */
    BitmapShard *a2 = bm_open(path_a, 0, 0, 0, 0, 0);
    if (!a2) _exit(9);
    if (bm_n_values(a2) != 1) _exit(10);
    if (bm_test(a2, (const uint8_t *)"new", 3, 0) != 1) _exit(11);
    if (bm_test(a2, (const uint8_t *)"old", 3, 0) != 0) _exit(12);
    bm_close(a2);

    db->durability_test_pause_ms = 0;
    db->durability_test_pause_phase[0] = '\0';
    _exit(0);
}

static int test_bm_publish_generation_run(void) {
    char base[] = "/tmp/shard-db-bm-publish-XXXXXX";
    if (!mkdtemp(base)) { ASSERT_TRUE(0, "mkdtemp"); return 1; }

    pid_t pid = fork();
    if (pid < 0) {
        ASSERT_TRUE(0, "fork");
        char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
        return 1;
    }
    if (pid == 0) run_bm_publish_deadlock_child(base); /* never returns */

    int status = 0;
    int wait_rc = 0;
    for (int tick = 0; tick < 300; tick++) {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) { wait_rc = 1; break; }
        if (r < 0) break;
        usleep(100000);
    }
    if (wait_rc != 1) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
    }

    ASSERT_EQ_INT(wait_rc, 1,
                  "bitmap bounded-deadlock child exits before the 30-second bound");
    if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
        TAP_DIAG("# bitmap deadlock child exited %d (2=seed failed, 3=retain "
                  "open failed, 4=B never acquired, 5=B contents wrong, "
                  "6=marker never cleared, 7=publisher did not finish, "
                  "8=publisher failed, 9=post-publication A open failed, "
                  "10-12=post-publication A contents wrong)\n",
                  WEXITSTATUS(status));
    ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0,
        "bitmap acquire of B completes while A is retained mid-publication");

    char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bm_checked_discard_flush_failure_run(void) {
    char root[] = "/tmp/shard-db-bm-discard-XXXXXX";
    if (!mkdtemp(root)) {
        ASSERT_TRUE(0, "mkdtemp bitmap discard root");
        return 1;
    }
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/dirty.bm", root);
    bm_cache_shutdown();
    bm_cache_init(16);
    BitmapShard *bm = bm_open(path, 256, 1, 0, 0, 1);
    ASSERT_NOT_NULL(bm, "open bitmap for checked discard");
    if (bm) {
        ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"dirty", 5, 7), 0,
                      "mark cached bitmap dirty");
        bm_close(bm);
    }
    durability_test_msync_reset();
    durability_test_msync_fail_next(1, EIO);
    errno = 0;
    ASSERT_EQ_INT(bm_cache_invalidate_checked(path), -1,
                  "checked discard propagates dirty flush failure");
    ASSERT_EQ_INT(errno, EIO,
                  "checked discard preserves dirty flush errno");
    durability_test_msync_reset();
    ASSERT_EQ_INT(bm_cache_invalidate_checked(path), 0,
                  "checked discard succeeds after injection reset");
    bm_cache_shutdown();
    unlink(path);
    rmdir(root);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bitmap-index", test_bitmap_index_run)
TEST_REGISTER("test-bm-publish-generation", test_bm_publish_generation_run)
TEST_REGISTER("test-bm-checked-discard-flush-failure",
              test_bm_checked_discard_flush_failure_run)
