/* src/test/cases/test_agg_empty_avg_order.c
 *
 * order_by on an aggregate alias with an EMPTY group in the set: the empty
 * group's avg is a DEC value while populated siblings sort through the same
 * comparison, so a class/payload mismatch here used to dereference NULL in
 * dec_cmp and take the daemon down. Every direction, every numeric fn, plus
 * long/short avg variants, must answer and leave the daemon alive. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/shard_db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static ShardDb *ao_db;
static char *ao_req(const char *json) {
    char *r = NULL;
    tu_pdb_request(ao_db, json, &r);
    return r;
}
static int ao_group_count(const char *resp) {
    if (!resp) return 0;
    int n = 0;
    const char *p = resp;
    while ((p = strstr(p, "{\"g\":")) != NULL) { n++; p++; }
    return n;
}
/* order_by <alias> in both directions must answer with three groups and a
   live daemon afterwards. */
static void ao_ordered(const char *obj, const char *aggs, const char *alias,
                       const char *msg) {
    char req[512];
    const char *dirs[2] = { "asc", "desc" };
    for (int d = 0; d < 2; d++) {
        snprintf(req, sizeof req,
            "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
            "\"aggregates\":[%s],\"group_by\":[\"g\"],"
            "\"order_by\":\"%s\",\"order\":\"%s\"}", obj, aggs, alias, dirs[d]);
        char *r = ao_req(req);
        ASSERT_NOT_NULL(r, msg);
        if (r) {
            ASSERT_TRUE(!strstr(r, "\"error\""), msg);
            ASSERT_EQ_INT(ao_group_count(r), 3, msg);
            shard_db_free_result(r);
        }
        char *c = ao_req("{\"mode\":\"count\",\"dir\":\"default\","
                         "\"object\":\"t\"}");
        ASSERT_NOT_NULL(c, "daemon alive after ordered aggregate");
        ASSERT_TRUE(c && !strstr(c, "\"error\""), "daemon alive after ordered aggregate");
        shard_db_free_result(c);
    }
}

static int ao_run(void) {
    ao_db = test_get_process_db();
    ASSERT_NOT_NULL(ao_db, "process db");
    if (!ao_db) return 1;
    char *r = ao_req("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);

    /* int: x=5, y=NULL (empty avg), z=7 */
    r = ao_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"t\","
               "\"splits\":8,\"max_key\":16,"
               "\"fields\":[\"g:varchar:10\",\"v:int\",\"lv:long\",\"sv:short\"],"
               "\"indexes\":[\"v\",\"lv\",\"sv\"]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "object created");
    shard_db_free_result(r);
    r = ao_req("{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"t\",\"records\":["
               "{\"key\":\"r1\",\"value\":{\"g\":\"x\",\"v\":5,\"lv\":5000000000,\"sv\":50}},"
               "{\"key\":\"r2\",\"value\":{\"g\":\"y\",\"v\":null,\"lv\":null,\"sv\":null}},"
               "{\"key\":\"r3\",\"value\":{\"g\":\"z\",\"v\":7,\"lv\":7000000000,\"sv\":70}}]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "rows inserted");
    shard_db_free_result(r);

    ao_ordered("t", "{\"fn\":\"avg\",\"field\":\"v\",\"alias\":\"a\"}", "a",
               "ordered avg with empty group");
    ao_ordered("t", "{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"}", "mn",
               "ordered min with empty group");
    ao_ordered("t", "{\"fn\":\"max\",\"field\":\"v\",\"alias\":\"mx\"}", "mx",
               "ordered max with empty group");
    ao_ordered("t", "{\"fn\":\"sum\",\"field\":\"v\",\"alias\":\"s\"}", "s",
               "ordered sum with empty group");
    ao_ordered("t", "{\"fn\":\"avg\",\"field\":\"lv\",\"alias\":\"a\"}", "a",
               "ordered long avg with empty group");
    ao_ordered("t", "{\"fn\":\"avg\",\"field\":\"sv\",\"alias\":\"a\"}", "a",
               "ordered short avg with empty group");

    /* CSV format: same ordered route, D3 empty-avg cell, daemon alive. */
    r = ao_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"t\","
               "\"aggregates\":[{\"fn\":\"avg\",\"field\":\"v\",\"alias\":\"a\"}],"
               "\"group_by\":[\"g\"],\"order_by\":\"a\",\"order\":\"asc\","
               "\"format\":\"csv\"}");
    ASSERT_NOT_NULL(r, "csv ordered avg with empty group");
    if (r) {
        ASSERT_TRUE(!strstr(r, "\"error\""), "csv ordered avg with empty group");
        ASSERT_TRUE(strstr(r, "\n") && strstr(r, ","), "csv ordered avg emitted rows");
        shard_db_free_result(r);
    }
    /* NQL path (TCP dispatch only — shard_db_query is JSON): the same
       NULL-only fixture against a fresh daemon, ordered by the avg alias. */
    {
        TestEnv nenv = {0};
        TestClient *ntc = NULL;
        if (test_env_start(&nenv) == 0) {
            TestClientCfg ncfg = { .port = nenv.port };
            ntc = tc_connect(&ncfg);
        }
        ASSERT_NOT_NULL(ntc, "nql daemon client");
        if (ntc) {
            const char *seq[] = {
                "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
                "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"t\","
                "\"splits\":8,\"max_key\":16,"
                "\"fields\":[\"g:varchar:10\",\"v:int\"],\"indexes\":[\"v\"]}",
                "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"t\",\"records\":["
                "{\"key\":\"r1\",\"value\":{\"g\":\"x\",\"v\":5}},"
                "{\"key\":\"r2\",\"value\":{\"g\":\"y\",\"v\":null}},"
                "{\"key\":\"r3\",\"value\":{\"g\":\"z\",\"v\":7}}]}",
            };
            int ok = 1;
            for (size_t i = 0; i < sizeof seq / sizeof *seq && ok; i++) {
                char *nr = NULL;
                if (tc_request(ntc, seq[i], &nr) != 0 || !nr ||
                    strstr(nr, "\"error\"")) ok = 0;
                free(nr);
            }
            ASSERT_TRUE(ok, "nql fixture ready");
            char *nr = NULL;
            if (tc_request(ntc, "aggregate default t avg(v) --group-by g",
                           &nr) == 0) {
                ASSERT_NOT_NULL(nr, "nql avg with NULL-only group");
                if (nr) {
                    /* NQL validates order_by against fields/aliases and
                       rejects an aggregate alias there, so the alias sort
                       itself is JSON-route-only; here the NULL-only group
                       must flow through NQL with avg rendered as null. */
                    ASSERT_TRUE(!strstr(nr, "\"error\""), "nql avg with NULL-only group");
                    ASSERT_TRUE(strstr(nr, "\"g\":\"y\"") &&
                                strstr(nr, "null"),
                                "nql NULL-only group answers with null avg");
                    free(nr);
                }
            }
            tc_close(ntc);
        }
        test_env_stop(&nenv);
    }

    tu_pdb_drop_object(ao_db, "default", "t");
    return 0;
}
TEST_REGISTER("test-agg-empty-avg-order", ao_run)
