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
#include <stdlib.h>
#include <string.h>

/* Convenience: the framework only provides ASSERT_TRUE / EQ_* / CONTAINS /
   NOT_NULL. Spelling NOT_CONTAINS as a wrapper keeps the test prose
   readable without dragging a new macro into the shared header. */
#define ASSERT_NOT_CONTAINS(haystack, needle, desc) \
    ASSERT_TRUE(strstr((haystack), (needle)) == NULL, (desc))

static int test_bitmap_index_run(void) {
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
