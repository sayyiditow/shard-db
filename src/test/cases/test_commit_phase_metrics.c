/* Red on base: g_db->commit_windows_total & friends do not exist yet. After
   Task 1 of docs/plans/2026-09-04-bulk-commit-throughput-and-durability.md:
   a plain (unindexed) bulk upsert must run >= 1 commit window and publish
   >= 1 marker, proving the M/C instrumentation wraps real work. */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static void expect(int cond, const char *what) {
    ASSERT_TRUE(cond, what);
}

static int test_commit_phase_metrics_run(void) {
    slotcask_init(64, 64);
    char base[] = "/tmp/shard-db-phase-metrics-XXXXXX";
    if (!mkdtemp(base)) return 1;
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", base);
    snprintf(k, sizeof(k), "%s/data/kf", base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return 1;

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    if (slotcask_open(&db, base, 8, 1, 64) != 0) return 1;
    db.bulk_commit_window = 16;

    /* The commit-phase counters live on the process-global ShardDb the
       runner initialises (same object test-bulk-commit-window-config
       pokes g_db->bulk_commit_window on). */
    uint64_t w0  = __atomic_load_n(&g_db->commit_windows_total, __ATOMIC_RELAXED);
    uint64_t m0  = __atomic_load_n(&g_db->commit_marker_publish_count, __ATOMIC_RELAXED);
    uint64_t mu0 = __atomic_load_n(&g_db->commit_marker_publish_us_total, __ATOMIC_RELAXED);
    uint64_t su0 = __atomic_load_n(&g_db->commit_segment_sync_us_total, __ATOMIC_RELAXED);
    uint64_t cu0 = __atomic_load_n(&g_db->commit_marker_clear_us_total, __ATOMIC_RELAXED);

    static char keys[8][16], vals[8][16];
    SlotcaskBulkRec batch[8];
    memset(batch, 0, sizeof(batch));
    for (int i = 0; i < 8; i++) {
        snprintf(keys[i], sizeof(keys[i]), "pm-key-%02d", i);
        snprintf(vals[i], sizeof(vals[i]), "pm-val-%02d", i);
        batch[i].key   = keys[i];
        batch[i].klen  = strlen(keys[i]);
        batch[i].value = vals[i];
        batch[i].vlen  = strlen(vals[i]);
        batch[i].status = 0;
        batch[i].was_update = 0;
    }
    SlotcaskBulkOpts opts = {0};   /* no indexed fields: hooks not required */
    int rc = slotcask_bulk_upsert_in_kfshard(&db, 0, batch, 8, &opts);
    expect(rc == 0, "bulk upsert succeeds");
    for (int i = 0; i < 8; i++)
        expect(batch[i].status == 0, "record committed");

    uint64_t w1  = __atomic_load_n(&g_db->commit_windows_total, __ATOMIC_RELAXED);
    uint64_t m1  = __atomic_load_n(&g_db->commit_marker_publish_count, __ATOMIC_RELAXED);
    uint64_t mu1 = __atomic_load_n(&g_db->commit_marker_publish_us_total, __ATOMIC_RELAXED);
    uint64_t su1 = __atomic_load_n(&g_db->commit_segment_sync_us_total, __ATOMIC_RELAXED);
    uint64_t cu1 = __atomic_load_n(&g_db->commit_marker_clear_us_total, __ATOMIC_RELAXED);
    expect(w1 > w0,  "commit_windows_total advanced");
    expect(m1 > m0,  "commit_marker_publish_count advanced");
    expect(mu1 >= mu0, "marker_publish_us_total present (grows or stays)");
    expect(su1 >= su0, "segment_sync_us_total present (grows or stays)");
    expect(cu1 >= cu0, "marker_clear_us_total present (grows or stays)");

    slotcask_close(&db);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    slotcask_shutdown();
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-commit-phase-metrics", test_commit_phase_metrics_run)
