/* test_slotcask_v2_wire.c — Phase-2C E2E test for storage_version=2.
 *
 * Spawns a daemon and exercises the full v2 wire-protocol round-trip for
 * single-record CRUD. Confirms cmd_get/exists/insert/delete/update dispatch
 * correctly to the slotcask engine when storage_version=2 is set.
 *
 * Out of scope (Phase 3): query-layer dispatch — find/count/aggregate over
 * v2 still walks the legacy probe-into-slot path and returns []. The
 * pre_commit index hook IS being fired (validated in test-slotcask-cas);
 * the find layer just doesn't know to fetch v2 records yet.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>

static int test_slotcask_v2_wire_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* ===== Setup ===== */
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"sctest\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"sctest\",\"object\":\"users\","
        "\"splits\":8,\"max_key\":40,"
        "\"fields\":[\"name:varchar:64\",\"age:int\",\"email:varchar:80\",\"active:bool\"],"
        "\"indexes\":[\"age\",\"email\"]}", &resp);
    free(resp); resp = NULL;

    /* ===== insert + get + exists ===== */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u1\","
                   "\"value\":{\"name\":\"alice\",\"age\":30,\"email\":\"a@x\",\"active\":true}}",
                   &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "v2 insert (fresh)");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "v2 get returns name");
    ASSERT_CONTAINS(resp, "\"age\":30", "v2 get returns age");
    ASSERT_CONTAINS(resp, "\"email\":\"a@x\"", "v2 get returns email");
    ASSERT_CONTAINS(resp, "\"active\":true", "v2 get returns active");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "true", "v2 exists=true on present key");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"absent\"}", &resp);
    ASSERT_CONTAINS(resp, "false", "v2 exists=false on missing key");
    free(resp); resp = NULL;

    /* ===== upsert via the same insert mode ===== */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u1\","
                   "\"value\":{\"name\":\"alice\",\"age\":31,\"email\":\"a@x\",\"active\":true}}",
                   &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "v2 upsert reports updated");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"age\":31", "post-upsert age=31");
    free(resp); resp = NULL;

    /* ===== partial update — wire format uses "value" for the partial JSON ===== */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u1\",\"value\":{\"age\":32}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "v2 partial update");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"age\":32", "partial update changed age");
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "partial update preserved name");
    ASSERT_CONTAINS(resp, "\"email\":\"a@x\"", "partial update preserved email");
    free(resp); resp = NULL;

    /* ===== CAS: if_not_exists ===== */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u_new\",\"if_not_exists\":true,"
                   "\"value\":{\"name\":\"dave\",\"age\":50,\"email\":\"d@x\",\"active\":true}}",
                   &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "if_not_exists on missing key inserts");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u_new\",\"if_not_exists\":true,"
                   "\"value\":{\"name\":\"NEVER\",\"age\":99,\"email\":\"n@x\",\"active\":false}}",
                   &resp);
    ASSERT_CONTAINS(resp, "condition_not_met",
                    "if_not_exists on present key → condition_not_met");
    ASSERT_CONTAINS(resp, "\"current\":{",
                    "condition_not_met carries current record");
    ASSERT_CONTAINS(resp, "\"name\":\"dave\"",
                    "current shows existing dave, not NEVER");
    free(resp); resp = NULL;

    /* ===== CAS: if_json criteria (array form per docs/query-protocol/cas.md) ===== */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u_new\","
                   "\"if\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"WRONG\"}],"
                   "\"value\":{\"name\":\"x\",\"age\":1,\"email\":\"x@x\",\"active\":true}}",
                   &resp);
    ASSERT_CONTAINS(resp, "condition_not_met",
                    "if criteria mismatch rejects update");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u_new\","
                   "\"if\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"dave\"}],"
                   "\"value\":{\"name\":\"dave\",\"age\":51,\"email\":\"d@x\",\"active\":true}}",
                   &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"",
                    "if criteria match allows update");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u_new\"}", &resp);
    ASSERT_CONTAINS(resp, "\"age\":51", "if-criteria-matched update applied");
    free(resp); resp = NULL;

    /* ===== CAS update via update mode ===== */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u_new\",\"value\":{\"age\":52},"
                   "\"if\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"99\"}]}",
                   &resp);
    ASSERT_CONTAINS(resp, "condition_not_met",
                    "update with mismatched if → condition_not_met");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u_new\",\"value\":{\"age\":52},"
                   "\"if\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"51\"}]}",
                   &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"",
                    "update with matching if → updated");
    free(resp); resp = NULL;

    /* ===== delete + verify gone ===== */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u_new\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "v2 delete u_new");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u_new\"}", &resp);
    ASSERT_CONTAINS(resp, "false", "u_new gone after delete");
    free(resp); resp = NULL;

    /* ===== delete CAS rejection ===== */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u1\","
                   "\"if\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"99\"}]}", &resp);
    ASSERT_CONTAINS(resp, "condition_not_met",
                    "delete with non-matching criteria → condition_not_met");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "true", "u1 still present after rejected delete");
    free(resp); resp = NULL;

    /* ===== drop-object cleans the registry ===== */
    tc_request(tc, "{\"mode\":\"drop-object\",\"dir\":\"sctest\",\"object\":\"users\"}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"dropped\"", "drop-object on v2 succeeds");
    free(resp); resp = NULL;

    /* Recreate same name — proves registry was invalidated. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"sctest\",\"object\":\"users\","
        "\"splits\":8,\"max_key\":40,"
        "\"fields\":[\"name:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                    "recreate after drop succeeds (registry invalidated)");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"fresh\",\"value\":{\"name\":\"after-drop\"}}",
                   &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "post-drop insert works");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sctest\",\"object\":\"users\","
                   "\"key\":\"fresh\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"after-drop\"", "post-drop get works");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-v2-wire", test_slotcask_v2_wire_run)
