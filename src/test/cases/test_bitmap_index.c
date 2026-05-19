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
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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
    BitmapShard *bm = bm_open(path, 256, 1, 1, 0);
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
    bm = bm_open(path, 1024, 0, 0, 0);
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
    bm = bm_open(path, 64, 1, 0, 0);
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
    bm = bm_open(path, 64, 0, 0, 0);
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
    bm = bm_open(path, 64, 1, 0, 0);
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
    bm = bm_open(path, 64, 1, 0, 4);   /* cap = 4 */
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
        bm = bm_open(path, 64, 0, 0, 0);  /* arg ignored on existing file */
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
        const char *idx_section = strstr(resp, "\"indexes\":[");
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
        const char *first = strstr(resp, "\"flag:bitmap\"");
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

TEST_REGISTER("test-bitmap-index", test_bitmap_index_run)
