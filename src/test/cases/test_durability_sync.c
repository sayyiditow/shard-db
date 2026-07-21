#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"

#include <glob.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    int kf;
    int seg;
    int bt;
    int bm;
    int failed;
} TickTotals;

static void sleep_ms(int ms) {
    struct timespec delay = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&delay, NULL);
}

static void scan_info_logs(const char *logs_dir, TickTotals *totals) {
    char pattern[512];
    snprintf(pattern, sizeof(pattern), "%s/*-info.log", logs_dir);
    glob_t files;
    if (glob(pattern, 0, NULL, &files) != 0) return;
    memset(totals, 0, sizeof(*totals));
    for (size_t i = 0; i < files.gl_pathc; i++) {
        FILE *f = fopen(files.gl_pathv[i], "r");
        if (!f) continue;
        char line[4096];
        while (fgets(line, sizeof(line), f)) {
            char *tick = strstr(line, "DURABILITY-SYNC tick:");
            int kf, seg, bt, bm, failed, skipped, escalated;
            unsigned long elapsed;
            if (tick && sscanf(tick,
                    "DURABILITY-SYNC tick: kf=%d seg=%d bt=%d bm=%d "
                    "failed=%d skipped=%d escalated=%d in %lums",
                    &kf, &seg, &bt, &bm, &failed, &skipped, &escalated,
                    &elapsed) == 8) {
                totals->kf += kf;
                totals->seg += seg;
                totals->bt += bt;
                totals->bm += bm;
                totals->failed += failed;
            }
        }
        fclose(f);
    }
    globfree(&files);
}

static int test_durability_sync_run(void) {
    char base[256], db_root[300], logs_dir[300], env_path[300];
    snprintf(base, sizeof(base), "/tmp/shard-db-dsync-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);
    mkdir(logs_dir, 0755);

    int port = test_pick_port();
    ASSERT_TRUE(port > 0, "pick daemon port");
    if (port <= 0) return 1;

    FILE *envf = fopen(env_path, "w");
    ASSERT_NOT_NULL(envf, "create durability db.env");
    if (!envf) return 1;
    fprintf(envf,
            "DB_ROOT=%s\nPORT=%d\nTIMEOUT=0\nLOG_DIR=%s\nLOG_LEVEL=3\n"
            "THREADS=2\nWORKERS=4\nIO_THREADS=4\n"
            "MAX_CONCURRENT_QUERIES=8\nFCACHE_MAX=4096\nTLS_ENABLE=0\n"
            "WARMUP=off\nDURABILITY_SYNC_MS=100\n",
            db_root, port, logs_dir);
    fclose(envf);

    TestEnv env = {0};
    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0,
                  "start daemon with periodic durability sync");
    if (env.daemon_pid <= 0) {
        tu_run_cmd("rm -rf %s", base);
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 5000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect durability daemon");
    if (!tc) {
        test_env_stop_keep(&env);
        tu_run_cmd("rm -rf %s", base);
        return 1;
    }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"durable\",\"splits\":8,\"max_key\":32,"
        "\"fields\":[\"v:int\",\"flag:bool\"],"
        "\"indexes\":[\"v\",\"flag:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create indexed object");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\","
        "\"object\":\"durable\",\"key\":\"k1\","
        "\"value\":{\"v\":7,\"flag\":true}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "mutate every mapped cache");
    free(resp); resp = NULL;

    TickTotals totals = {0};
    int all_synced = 0;
    for (int waited = 0; waited < 2000; waited += 50) {
        scan_info_logs(logs_dir, &totals);
        if (totals.kf > 0 && totals.seg > 0 && totals.bt > 0 &&
            totals.bm > 0) {
            all_synced = 1;
            break;
        }
        sleep_ms(50);
    }
    ASSERT_TRUE(all_synced,
                "periodic ticks synchronize kf, segment, btree, and bitmap caches");
    ASSERT_EQ_INT(totals.failed, 0, "periodic durability ticks have no failures");

    TickTotals before_bulk = totals;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"durable\",\"records\":["
        "{\"key\":\"k2\",\"value\":{\"v\":8,\"flag\":false}},"
        "{\"key\":\"k3\",\"value\":{\"v\":9,\"flag\":true}}]}",
        &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""),
                "bulk mmap mutation completes without index/cache error");
    free(resp); resp = NULL;
    int bulk_synced = 0;
    for (int waited = 0; waited < 2000; waited += 50) {
        scan_info_logs(logs_dir, &totals);
        if (totals.seg > before_bulk.seg && totals.bt > before_bulk.bt) {
            bulk_synced = 1;
            break;
        }
        sleep_ms(50);
    }
    ASSERT_TRUE(bulk_synced,
                "grouped bulk segment and btree mutations are dirty-tracked");

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"durable\","
        "\"criteria\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"7\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "btree-indexed record remains queryable");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"durable\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\","
        "\"value\":\"true\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "bitmap-indexed record remains queryable");
    free(resp);

    tc_close(tc);
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed ? 1 : 0;
}

TEST_REGISTER("test-durability-sync", test_durability_sync_run)
