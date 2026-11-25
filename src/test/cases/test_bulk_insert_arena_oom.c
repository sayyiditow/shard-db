/* src/test/cases/test_bulk_insert_arena_oom.c
 * AUD-BULK-OOM: bulk-insert and bulk-insert-delimited used arena_new /
 * arena_alloc results unchecked, so an allocation failure crashed the
 * daemon (or, on a record-array realloc failure, reported {"inserted":0}
 * as if nothing was wrong). The Nth arena allocation is failed through
 * bulk_arena_test_set_fail(N): every site must answer a clean "out of
 * memory" error and write nothing. In-process (process-local ShardDb).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/shard_db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void bulk_arena_test_set_fail(int n);

static int test_bulk_insert_arena_oom(void) {
    ShardDb *db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;
    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    shard_db_free_result(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"bao_t\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:64\"]}", &resp);
    shard_db_free_result(resp); resp = NULL;

    /* ~600 KB of records so the 256 KiB first arena must grow. */
    size_t n_rec = 6000;
    size_t cap = n_rec * 80 + 256;
    char *json = malloc(cap), *csv = malloc(cap);
    ASSERT_TRUE(json && csv, "buffers");
    if (!json || !csv) { free(json); free(csv); return 1; }
    size_t jo = 0, co = 0;
    jo += (size_t)snprintf(json + jo, cap - jo, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bao_t\",\"records\":{");
    co += (size_t)snprintf(csv + co, cap - co, "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bao_t\",\"data\":\"");
    for (size_t i = 0; i < n_rec; i++) {
        jo += (size_t)snprintf(json + jo, cap - jo, "%s\"k%06zu\":{\"v\":\"value-%zu\"}", i ? "," : "", i, i);
        co += (size_t)snprintf(csv + co, cap - co, "c%06zu,cv-%zu\\n", i, i);
    }
    snprintf(json + jo, cap - jo, "}}");
    snprintf(csv + co, cap - co, "\",\"delimiter\":\",\"}");

    char *reqs[2] = { json, csv };
    const char *names[2] = { "json", "delimited" };
    for (int f = 0; f < 2; f++) {
        for (int n = 1; n <= 2; n++) {
            bulk_arena_test_set_fail(n);
            tu_pdb_request(db, reqs[f], &resp);
            bulk_arena_test_set_fail(0);
            char msg[120];
            snprintf(msg, sizeof(msg), "%s: allocation #%d failure gives a clean out-of-memory error", names[f], n);
            ASSERT_TRUE(resp && strstr(resp, "out of memory"), msg);
            if (resp && !strstr(resp, "out of memory")) fprintf(stderr, "# got: %.200s\n", resp);
            shard_db_free_result(resp); resp = NULL;
        }
        /* Nothing was written by the failed attempts. */
        tu_pdb_request(db, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"bao_t\"}", &resp);
        ASSERT_TRUE(resp && strcmp(resp, "0\n") == 0 || (resp && resp[0] == '0'), "failed attempts wrote nothing");
        shard_db_free_result(resp); resp = NULL;
        /* Control: no injection inserts every record. */
        tu_pdb_request(db, reqs[f], &resp);
        ASSERT_TRUE(resp && strstr(resp, "\"inserted\":6000"), "control inserts all records");
        shard_db_free_result(resp); resp = NULL;
        tu_pdb_request(db, "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"bao_t\"}", &resp);
        shard_db_free_result(resp); resp = NULL;
    }
    free(json); free(csv);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bulk-insert-arena-oom", test_bulk_insert_arena_oom)
