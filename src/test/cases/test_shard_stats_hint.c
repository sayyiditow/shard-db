/* src/test/cases/test_shard_stats_hint.c
 * cmd_shard_stats's reshard hint must come from the same
 * reshard_target_for_count() lookup auto-reshard uses — a total-live-count
 * decision, not a per-shard average/max. The separate skew hint
 * (max_records > 4 * min_records) is a different, still-valid signal and
 * must keep firing on its own.
 *
 * Live-count fabrication: writes directly into each kf shard's on-disk
 * header (offset 8, 8 bytes, the `total` field — magic u32@0, version
 * u32@4, total u64@8, deleted u64@16, see SlotcaskKfHeader in
 * src/db/slotcask.h) after create-object has created the (real, empty)
 * kf shard files. Safe here because cmd_shard_stats() (src/db/
 * query_maint.c) reads each shard header with its own open()+pread()+
 * close() directly against the kf file, not through the mmap'd kfcache
 * — so it observes the external pwrite() via the OS page cache with no
 * staleness window.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

/* Writes `total` into a kf shard's header at offset 8 (the `total`
   field). Leaves magic/version/deleted untouched. */
static int fabricate_kf_total(const char *kf_path, uint64_t total) {
    int fd = open(kf_path, O_WRONLY);
    if (fd < 0) return -1;
    ssize_t n = pwrite(fd, &total, sizeof(total), 8);
    close(fd);
    return (n == (ssize_t)sizeof(total)) ? 0 : -1;
}

static int test_shard_stats_hint_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;

    /* Object 1: under-split. splits=8, fabricate shard 0's live count to
       2,000,000 (falls in the 1M-10M band -> target=16 > 8). Hint must
       recommend splits=16. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"grown\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/default/grown/data/kf/000.kf", env.db_root);
    ASSERT_EQ_INT(fabricate_kf_total(kf_path, 2000000ULL), 0,
                  "fabricate shard 0 total=2,000,000 on 'grown'");

    tc_request(tc, "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"grown\"}", &resp);
    ASSERT_CONTAINS(resp, "\"hint\"", "grown: hint present (target=16 > splits=8)");
    ASSERT_CONTAINS(resp, "splits=16", "grown: hint recommends splits=16");
    free(resp); resp = NULL;

    /* Object 2: already correctly sized. splits=64, live stays tiny (a
       few real inserts) -> target=8 <= 64, no hint at all. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"sized\","
        "\"splits\":64,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    for (int i = 1; i <= 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sized\","
            "\"key\":\"k%d\",\"value\":{\"v\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    tc_request(tc, "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"sized\"}", &resp);
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"hint\""), "sized: no hint (target=8 <= splits=64, no skew)");
    free(resp); resp = NULL;

    /* Object 3: skew hint must still fire on its own, independent of the
       reshard-target check. splits=8, all 8 shards fabricated nonzero so
       min_records > 0; shard 7 is 10x every other shard (> 4x skew
       threshold). Total stays well under 1M so target=8 <= splits=8 -
       the reshard-target branch must NOT also fire here. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"skewed\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    for (int i = 0; i < 8; i++) {
        char shard_kf[PATH_MAX];
        snprintf(shard_kf, sizeof(shard_kf), "%s/default/skewed/data/kf/%03d.kf", env.db_root, i);
        uint64_t total = (i == 7) ? 1000ULL : 100ULL;
        ASSERT_EQ_INT(fabricate_kf_total(shard_kf, total), 0, "fabricate skewed shard total");
    }
    tc_request(tc, "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"skewed\"}", &resp);
    ASSERT_CONTAINS(resp, "skewed", "skewed: skew hint fires");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "splits="), "skewed: no reshard-target hint (target=8 <= splits=8)");
    free(resp); resp = NULL;

    /* Object 4: 'maxed' — already at MAX_SPLITS (4096), with average
       records/shard past the old 1M degradation threshold, but total
       live count (~4.5B) sits inside the 1B-5B band, whose target (2048)
       is already <= splits (4096) — so the reshard-target branch does
       NOT fire, and this object needs the *separate* MAX_SPLITS
       degradation hint to fire instead. Before this task's fix, that
       fallback only checked `total_records >= 10000000000ULL` (10B), so
       this 4.5B case got no hint at all — the exact gap this task
       closes by reusing the already-computed `rps` (records/shard)
       signal instead of a second hardcoded threshold. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"maxed\","
        "\"splits\":4096,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    for (int i = 0; i < 4096; i++) {
        char shard_kf[PATH_MAX];
        snprintf(shard_kf, sizeof(shard_kf), "%s/default/maxed/data/kf/%03d.kf", env.db_root, i);
        ASSERT_EQ_INT(fabricate_kf_total(shard_kf, 1100000ULL), 0,
                      "fabricate maxed shard total=1,100,000 (avg rps > 1M)");
    }
    tc_request(tc, "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"maxed\"}", &resp);
    ASSERT_CONTAINS(resp, "\"hint\"", "maxed: MAX_SPLITS degradation hint present");
    ASSERT_CONTAINS(resp, "MAX_SPLITS", "maxed: hint mentions MAX_SPLITS, not a splits=N reshard target");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-shard-stats-hint", test_shard_stats_hint_run)
