/* src/test/cases/test_agg_ht_resize.c
 *
 * group_by over more than AGG_HT_INIT (256) distinct non-integer keys: the
 * bucket table doubles mid-query, and every chain must be re-routed by the
 * SAME length-based hash that lookups use. A NUL-terminated rehash of the
 * tagged key (whose first byte is the 0x00 NULL tag) sends every bucket to
 * one slot, lookups then miss, and the same group is emitted several
 * times with split counts. Totals stay right, which is why only the group
 * count and per-group counts expose it. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/shard_db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define AHR_GROUPS 3000
#define AHR_PER    3

static ShardDb *ahr_db;
static char *ahr_req(const char *json) {
    char *r = NULL;
    tu_pdb_request(ahr_db, json, &r);
    return r;
}

static int ahr_count(const char *s, const char *needle) {
    int n = 0;
    for (const char *p = s; (p = strstr(p, needle)) != NULL; p++) n++;
    return n;
}

static void ahr_fill(const char *obj, int varchar_group) {
    char req[256];
    snprintf(req, sizeof req,
             "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
             "\"splits\":8,\"max_key\":16,\"fields\":[\"g:%s\",\"v:int\"]}",
             obj, varchar_group ? "varchar:12" : "double");
    char *r = ahr_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "object created");
    shard_db_free_result(r);
    for (int base = 0; base < AHR_GROUPS * AHR_PER; base += 3000) {
        char *buf = malloc(3000 * 96 + 128);
        ASSERT_NOT_NULL(buf, "batch buffer");
        if (!buf) return;
        int n = snprintf(buf, 128, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                         "\"object\":\"%s\",\"records\":[", obj);
        for (int i = base; i < base + 3000; i++) {
            int g = i % AHR_GROUPS;
            n += varchar_group
                ? snprintf(buf + n, 96, "%s{\"key\":\"k%d\",\"value\":{\"g\":\"grp%d\",\"v\":1}}",
                           i > base ? "," : "", i, g)
                : snprintf(buf + n, 96, "%s{\"key\":\"k%d\",\"value\":{\"g\":%d.5,\"v\":1}}",
                           i > base ? "," : "", i, g);
        }
        snprintf(buf + n, 8, "]}");
        r = ahr_req(buf);
        ASSERT_TRUE(r && !strstr(r, "\"error\""), "rows inserted");
        shard_db_free_result(r);
        free(buf);
    }
}

static void ahr_check(const char *obj, const char *msg) {
    char req[320];
    snprintf(req, sizeof req,
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"group_by\":[\"g\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],"
             "\"limit\":100000}", obj);
    char *r = ahr_req(req);
    ASSERT_NOT_NULL(r, msg);
    if (!r) return;
    ASSERT_TRUE(!strstr(r, "\"error\""), msg);
    ASSERT_EQ_INT(ahr_count(r, "{\"g\":"), AHR_GROUPS, msg);
    ASSERT_EQ_INT(ahr_count(r, "\"c\":3}"), AHR_GROUPS, msg);
    shard_db_free_result(r);
}

static int ahr_run(void) {
    ahr_db = test_get_process_db();
    ASSERT_NOT_NULL(ahr_db, "process db");
    if (!ahr_db) return 1;
    char *r = ahr_req("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);

    ahr_fill("ahrv", 1);
    ahr_check("ahrv", "varchar group_by past a table resize: each group once, full count");
    tu_pdb_drop_object(ahr_db, "default", "ahrv");

    ahr_fill("ahrd", 0);
    ahr_check("ahrd", "double group_by past a table resize: each group once, full count");
    tu_pdb_drop_object(ahr_db, "default", "ahrd");
    return 0;
}
TEST_REGISTER("test-agg-ht-resize", ahr_run)
