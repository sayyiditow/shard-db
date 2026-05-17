/* src/test/cases/test_auto_key.c
 *
 * Auto-key feature — server-generated keys at insert time on objects
 * created with auto_key=uuid or auto_key=seq(<name>). Covers:
 *   - create validation (max_key floors per mode)
 *   - omit-key insert generates per mode + returns rendered key
 *   - provided-key insert upserts (exists → update, else → insert)
 *   - get / delete / exists by the rendered key
 *   - cross-restart schema persistence (auto_key= token survives)
 *   - update / delete error paths on missing key
 *   - bulk-insert auto-gen mixed with provided keys deferred (TODO)
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
#include <unistd.h>

/* Extract the value of a `"key":"..."` field from a JSON response.
   Returns a malloc'd copy of the value (caller frees) or NULL if not
   found. Strict: assumes the value is a JSON string, not a number. */
static char *extract_key_field(const char *resp) {
    const char *p = strstr(resp, "\"key\":\"");
    if (!p) return NULL;
    p += 7;
    const char *end = strchr(p, '"');
    if (!end) return NULL;
    size_t n = (size_t)(end - p);
    char *out = malloc(n + 1);
    if (!out) return NULL;
    memcpy(out, p, n);
    out[n] = '\0';
    return out;
}

static int test_auto_key_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* === 1. create validation ============================================= */

    /* uuid + max_key=16 OK */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"users\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"],\"auto_key\":\"uuid\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "uuid create OK");
    free(resp); resp = NULL;

    /* uuid + max_key=15 refused */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_small\","
        "\"splits\":8,\"max_key\":15,"
        "\"fields\":[\"name:varchar:32\"],\"auto_key\":\"uuid\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "uuid max_key<16 rejected");
    ASSERT_CONTAINS(resp, "requires max_key>=16", "uuid floor reason");
    free(resp); resp = NULL;

    /* seq + max_key=8 OK */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"orders\","
        "\"splits\":8,\"max_key\":8,"
        "\"fields\":[\"amount:int\"],\"auto_key\":\"seq(orders_id)\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "seq create OK");
    free(resp); resp = NULL;

    /* sequence file should have been pre-initialised at 0. */
    char path[400];
    snprintf(path, sizeof(path),
             "%s/default/orders/metadata/sequences/orders_id", env.db_root);
    struct stat st;
    ASSERT_TRUE(stat(path, &st) == 0, "seq file pre-initialised");

    /* seq + max_key=7 refused */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"o_small\","
        "\"splits\":8,\"max_key\":7,"
        "\"fields\":[\"amount:int\"],\"auto_key\":\"seq(other)\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "seq max_key<8 rejected");
    ASSERT_CONTAINS(resp, "requires max_key>=8", "seq floor reason");
    free(resp); resp = NULL;

    /* invalid mode refused */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bad\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"x:int\"],\"auto_key\":\"foo\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "bad auto_key mode rejected");
    free(resp); resp = NULL;

    /* === 2. uuid omit-key insert → server generates ======================= */

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"users\","
        "\"value\":{\"name\":\"Alice\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "uuid omit-key inserted");
    char *uuid_alice = extract_key_field(resp);
    ASSERT_NOT_NULL(uuid_alice, "uuid response carries key");
    ASSERT_TRUE(uuid_alice && strlen(uuid_alice) == 36, "uuid is 36 chars dashed");
    free(resp); resp = NULL;

    /* GET by that exact uuid string */
    char buf[512];
    snprintf(buf, sizeof(buf),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"%s\"}",
        uuid_alice);
    tc_request(tc, buf, &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "GET by generated uuid");
    free(resp); resp = NULL;

    /* Second omit-key insert produces a different uuid */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"users\","
        "\"value\":{\"name\":\"Bob\"}}", &resp);
    char *uuid_bob = extract_key_field(resp);
    ASSERT_NOT_NULL(uuid_bob, "second uuid response carries key");
    ASSERT_TRUE(uuid_bob && uuid_alice && strcmp(uuid_alice, uuid_bob) != 0,
                "two omit-key inserts get different uuids");
    free(resp); resp = NULL;

    /* === 3. uuid provided-key upsert ====================================== */

    const char *manual_uuid = "11111111-2222-4333-8444-555566667777";
    snprintf(buf, sizeof(buf),
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"users\","
        "\"key\":\"%s\",\"value\":{\"name\":\"Carol\"}}",
        manual_uuid);
    tc_request(tc, buf, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "provided uuid insert");
    char *got_key = extract_key_field(resp);
    ASSERT_TRUE(got_key && strcmp(got_key, manual_uuid) == 0,
                "provided uuid echoed verbatim");
    free(got_key); free(resp); resp = NULL;

    /* Repeat with same key → upsert update */
    snprintf(buf, sizeof(buf),
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"users\","
        "\"key\":\"%s\",\"value\":{\"name\":\"Carol2\"}}",
        manual_uuid);
    tc_request(tc, buf, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "same uuid → upsert update");
    free(resp); resp = NULL;

    /* GET reflects the update */
    snprintf(buf, sizeof(buf),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"%s\"}",
        manual_uuid);
    tc_request(tc, buf, &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Carol2\"", "upsert wrote the new value");
    free(resp); resp = NULL;

    /* === 4. uuid invalid provided key ===================================== */

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"users\","
        "\"key\":\"not-a-uuid\",\"value\":{\"name\":\"X\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "malformed uuid rejected");
    ASSERT_CONTAINS(resp, "36-char dashed UUID", "uuid format hint");
    free(resp); resp = NULL;

    /* === 5. seq omit-key insert → 1, 2, 3 ================================= */

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"value\":{\"amount\":100}}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"1\"", "first seq key is 1");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"value\":{\"amount\":200}}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"2\"", "second seq key is 2");
    free(resp); resp = NULL;

    /* GET by decimal string */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"orders\",\"key\":\"1\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"amount\":100", "GET seq by decimal");
    free(resp); resp = NULL;

    /* Provided seq key (decimal) — upsert */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"key\":\"500\",\"value\":{\"amount\":555}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "manual seq key insert");
    ASSERT_CONTAINS(resp, "\"key\":\"500\"", "manual seq key echoed");
    free(resp); resp = NULL;

    /* Manual key does NOT advance the seq — next auto-gen is 3, not 501 */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"value\":{\"amount\":300}}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"3\"", "seq watermark unaffected by manual insert");
    free(resp); resp = NULL;

    /* Malformed seq key rejected (leading zeros) */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"key\":\"007\",\"value\":{\"amount\":7}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "leading-zero seq key rejected");
    free(resp); resp = NULL;

    /* Malformed seq key rejected (non-numeric) */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"key\":\"abc\",\"value\":{\"amount\":1}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "non-numeric seq key rejected");
    free(resp); resp = NULL;

    /* === 6. CAS — if_not_exists with provided key ========================= */

    /* Existing key 500 + if_not_exists:true → skipped (condition_not_met) */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"key\":\"500\",\"value\":{\"amount\":999},\"if_not_exists\":\"true\"}",
        &resp);
    ASSERT_CONTAINS(resp, "condition_not_met", "if_not_exists skips existing");
    free(resp); resp = NULL;

    /* Absent key 600 + if_not_exists → inserts */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"key\":\"600\",\"value\":{\"amount\":600},\"if_not_exists\":\"true\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "if_not_exists inserts absent");
    free(resp); resp = NULL;

    /* === 7. omit-key + if predicate rejected at parse time ================ */

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"value\":{\"amount\":1},\"if\":[{\"field\":\"amount\",\"op\":\"eq\",\"value\":1}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "if + omit-key rejected");
    ASSERT_CONTAINS(resp, "if predicate", "if reason");
    free(resp); resp = NULL;

    /* === 8. update / delete require key on auto-key objects =============== */

    /* update with no key → error */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"users\","
        "\"value\":{\"name\":\"NoKey\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "update no-key rejected");
    free(resp); resp = NULL;

    /* update with valid uuid works */
    snprintf(buf, sizeof(buf),
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"users\","
        "\"key\":\"%s\",\"value\":{\"name\":\"Alice2\"}}", uuid_alice);
    tc_request(tc, buf, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "update by uuid OK");
    free(resp); resp = NULL;

    /* delete by uuid */
    snprintf(buf, sizeof(buf),
        "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"%s\"}",
        uuid_bob);
    tc_request(tc, buf, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "delete by uuid");
    free(resp); resp = NULL;

    /* exists by uuid */
    snprintf(buf, sizeof(buf),
        "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"%s\"}",
        uuid_alice);
    tc_request(tc, buf, &resp);
    ASSERT_CONTAINS(resp, "true", "exists by uuid");
    free(resp); resp = NULL;

    /* === 9. AK_NONE regression ============================================ */

    /* Old-style object — no auto_key. Insert with no key still errors. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"old\","
        "\"splits\":8,\"max_key\":32,\"fields\":[\"name:varchar:32\"]}",
        &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"old\","
        "\"value\":{\"name\":\"X\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "AK_NONE omit-key still errors");
    free(resp); resp = NULL;

    /* AK_NONE provided-key insert still works as today. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"old\","
        "\"key\":\"k1\",\"value\":{\"name\":\"Y\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "AK_NONE provided-key OK");
    ASSERT_CONTAINS(resp, "\"key\":\"k1\"", "AK_NONE key echoed");
    free(resp); resp = NULL;

    /* === 10. find on auto-key object renders keys ========================= */

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\","
        "\"criteria\":[{\"field\":\"amount\",\"op\":\"eq\",\"value\":100}]}",
        &resp);
    /* find returns array of records; key in each entry should render as
       a decimal string. The match for amount=100 should yield key "1". */
    ASSERT_CONTAINS(resp, "\"key\":\"1\"", "find renders seq key as decimal");
    free(resp); resp = NULL;

    /* === 11. bulk-insert auto-key (seq) =================================== */

    /* 3 records, all omit-key → batch-allocated sequential keys. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"records\":[{\"value\":{\"amount\":1000}},"
                     "{\"value\":{\"amount\":1001}},"
                     "{\"value\":{\"amount\":1002}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"bulk-inserted\"", "bulk-insert auto-seq status");
    ASSERT_CONTAINS(resp, "\"count\":3", "bulk-insert auto-seq count");
    /* Next 3 auto-keys are 4,5,6 (seq was at 3 after the earlier inserts) */
    ASSERT_CONTAINS(resp, "\"keys\":[\"4\",\"5\",\"6\"]", "bulk-insert seq batch keys");
    free(resp); resp = NULL;

    /* Mixed: 1 provided + 2 auto-gen */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"records\":[{\"key\":\"800\",\"value\":{\"amount\":800}},"
                     "{\"value\":{\"amount\":2000}},"
                     "{\"value\":{\"amount\":2001}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"keys\":[\"800\",\"7\",\"8\"]", "bulk-insert mixed: provided + auto-gen");
    free(resp); resp = NULL;

    /* Bad provided key fails whole batch pre-flight. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"orders\","
        "\"records\":[{\"value\":{\"amount\":1}},"
                     "{\"key\":\"abc\",\"value\":{\"amount\":2}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "bulk validation rejects bad key");
    ASSERT_CONTAINS(resp, "record 1", "bulk error names failing index");
    free(resp); resp = NULL;

    /* === 12. bulk-insert auto-key (uuid) ================================== */

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"users\","
        "\"records\":[{\"value\":{\"name\":\"P\"}},"
                     "{\"value\":{\"name\":\"Q\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"bulk-inserted\"", "bulk-insert auto-uuid status");
    ASSERT_CONTAINS(resp, "\"count\":2", "bulk-insert auto-uuid count");
    /* Both keys should be 36-char dashed uuids — substring check on "-" pattern. */
    /* Loose check: response should contain at least one dashed-uuid pattern. */
    ASSERT_TRUE(resp && strstr(resp, "-4") != NULL,
                "bulk-insert response contains v4 marker (-4)");
    free(resp); resp = NULL;

    /* === 13. multi-key get/exists/not-exists on auto-key objects ========== */

    /* get multi by rendered seq keys — both "1" and "2" exist, "999" doesn't */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"orders\","
        "\"keys\":[\"1\",\"2\",\"999\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"1\":", "multi-get auto-seq: 1 echoed");
    ASSERT_CONTAINS(resp, "\"2\":", "multi-get auto-seq: 2 echoed");
    ASSERT_CONTAINS(resp, "\"999\":null", "multi-get missing key → null");
    free(resp); resp = NULL;

    /* exists multi by rendered keys */
    tc_request(tc,
        "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"orders\","
        "\"keys\":[\"1\",\"999\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"1\":true", "multi-exists auto-seq: 1 true");
    ASSERT_CONTAINS(resp, "\"999\":false", "multi-exists auto-seq: 999 false");
    free(resp); resp = NULL;

    /* not-exists returns missing keys */
    tc_request(tc,
        "{\"mode\":\"not-exists\",\"dir\":\"default\",\"object\":\"orders\","
        "\"keys\":[\"1\",\"999\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"999\"", "not-exists: 999 in output");
    ASSERT_TRUE(resp && strstr(resp, "\"1\"") == NULL,
                "not-exists: existing key 1 omitted");
    free(resp); resp = NULL;

    /* uuid multi-get by rendered uuid */
    snprintf(buf, sizeof(buf),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"users\","
        "\"keys\":[\"%s\",\"00000000-0000-4000-8000-000000000000\"]}",
        uuid_alice);
    tc_request(tc, buf, &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Alice2\"", "multi-get auto-uuid: alice value");
    ASSERT_CONTAINS(resp, "00000000-0000-4000-8000-000000000000\":null",
                    "multi-get auto-uuid: missing uuid → null");
    free(resp); resp = NULL;

    /* === 14. bulk-insert-delimited auto-key ============================== */

    /* Seq mode: 3 rows, all empty first column → batch-gen */
    tc_request(tc,
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"orders\","
        "\"delimiter\":\",\","
        "\"data\":\",5000\\n,5001\\n,5002\\n\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"bulk-inserted\"", "delim auto-seq status");
    ASSERT_CONTAINS(resp, "\"count\":3", "delim auto-seq count");
    /* Seq watermark after earlier inserts is around 8 — the next 3 are 9,10,11 */
    ASSERT_CONTAINS(resp, "\"9\"", "delim auto-seq batch first key");
    ASSERT_CONTAINS(resp, "\"11\"", "delim auto-seq batch last key");
    free(resp); resp = NULL;

    /* Mixed: 1 provided + 2 empty (auto-gen). Provided key is upsert. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"orders\","
        "\"delimiter\":\",\","
        "\"data\":\"900,9000\\n,9001\\n,9002\\n\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"bulk-inserted\"", "delim mixed status");
    ASSERT_CONTAINS(resp, "\"900\"", "delim mixed: provided key echoed");
    free(resp); resp = NULL;

    /* Malformed provided key (non-decimal on seq mode) refused. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"orders\","
        "\"delimiter\":\",\","
        "\"data\":\"abc,1\\n,2\\n\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "delim refuses bad seq wire key");
    free(resp); resp = NULL;

    /* UUID mode delimited: 2 rows, both omit-key */
    tc_request(tc,
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"users\","
        "\"delimiter\":\",\","
        "\"data\":\",DelimP\\n,DelimQ\\n\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"bulk-inserted\"", "delim auto-uuid status");
    ASSERT_CONTAINS(resp, "\"count\":2", "delim auto-uuid count");
    ASSERT_TRUE(resp && strstr(resp, "-4") != NULL,
                "delim auto-uuid: v4 marker present");
    free(resp); resp = NULL;

    /* === 15. bulk-insert seq collision → strict insert errors ============= */

    /* Fresh object — the earlier `orders` tests are polluted with records
       and a churned-up sequence counter; we need known starting state. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"collide\","
        "\"splits\":8,\"max_key\":8,"
        "\"fields\":[\"amount:int\"],\"auto_key\":\"seq(collide_seq)\"}", &resp);
    free(resp); resp = NULL;

    /* Manually insert at "3" — the seq is at 0, so the next auto-gen of
       5 records will hit 1, 2, 3, 4, 5 and collide at 3. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"collide\","
        "\"key\":\"3\",\"value\":{\"amount\":42}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "manual seq key 3 inserted");
    free(resp); resp = NULL;

    /* Bulk-insert 5 omit-key records. "3" collides with the manual
       record; with per-record CAS, that one slot is condition_not_met
       and the other 4 insert. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"collide\","
        "\"records\":[{\"value\":{\"amount\":100}},"
                     "{\"value\":{\"amount\":200}},"
                     "{\"value\":{\"amount\":300}},"
                     "{\"value\":{\"amount\":400}},"
                     "{\"value\":{\"amount\":500}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"skipped\":1", "bulk seq collision: 1 skipped");
    ASSERT_CONTAINS(resp, "\"count\":4", "bulk seq collision: 4 inserted");
    free(resp); resp = NULL;

    /* The manually-inserted record at "3" must still have amount=42, not 300. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"collide\",\"key\":\"3\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"amount\":42", "manual record at 3 NOT overwritten");
    free(resp); resp = NULL;

    /* Records at 1, 2, 4, 5 should exist (the non-colliding auto-gens). */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"collide\",\"key\":\"4\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"amount\":400", "non-colliding auto-gen record present");
    free(resp); resp = NULL;

    free(uuid_alice); free(uuid_bob);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-key", test_auto_key_run)
