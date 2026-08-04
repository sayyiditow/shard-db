/* test_slotcask_v2_crash.c — Phase-8 crash safety on v2 (slotcask).
 *
 * Mirrors test_crash_safety but creates a v2 object. Validates:
 *
 *   1. Every record acked before the SIGKILL survives the restart
 *      (committed records are durable — keyfile flip is the commit
 *      point, atomic 8B store, hits the page cache before ack).
 *   2. Post-crash count is in [BASELINE, BASELINE + CRASH_AT].
 *   3. Spot-checked baseline keys are readable verbatim.
 *   4. recover_streams runs on the post-crash slotcask_open: a fresh
 *      insert after the restart does NOT clobber a live record (this
 *      tests the Phase-5 fix to slotcask_open that always re-derives
 *      reserve_off from disk).
 *   5. A key past the attempted range is absent (no phantom records).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"   /* KfAbortHeader + kf_abort_* sidecar helpers */
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define BASELINE  1000
#define CRASH_AT   500

static int crash_run_cmd(const char *fmt, ...) {
    char cmd[2048];
    va_list ap; va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    return system(cmd);
}

static int crash_parse_count(const char *resp) {
    if (!resp) return -1;
    while (*resp == ' ' || *resp == '\n') resp++;
    if (*resp == '{') {
        const char *p = SAFE_STRSTR(resp, "\"count\":");
        return p ? atoi(p + 8) : -1;
    }
    return atoi(resp);
}

/* ── Abort-sidecar on-disk parser tests (Task 1, test-first) ──
 *
 * Every invalid state must fail closed: kf_abort_read_exact returns -1 with
 * EILSEQ and the sidecar file is left in place (evidence is never deleted by
 * a failed parse). Valid states round-trip every field.
 */
static int test_abort_sidecar_parser(void) {
    char base[256];
    snprintf(base, sizeof(base), "/tmp/shard-db-abort-parser-%d", (int)getpid());
    crash_run_cmd("rm -rf %s", base);
    ASSERT_EQ_INT(mkdir(base, 0755), 0, "create parser test base");
    /* data_dir is the object root; kf/ streams live under <root>/data/. */
    char data_dir[PATH_MAX], data_kf[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s", base);
    snprintf(data_kf, sizeof(data_kf), "%s/data/kf", base);
    char data_sub[PATH_MAX];
    snprintf(data_sub, sizeof(data_sub), "%s/data", base);
    ASSERT_EQ_INT(mkdir(data_sub, 0755), 0, "create data/");
    ASSERT_EQ_INT(mkdir(data_kf, 0755), 0, "create data/kf/");

    /* 1) Valid single sidecar round-trip + exact-parameter acceptance. */
    char single_path[PATH_MAX];
    snprintf(single_path, sizeof(single_path), "%s/%03x_marker_abort.dat",
             data_kf, 3);
    ASSERT_EQ_INT(kf_abort_write_sidecar(data_dir, KF_ABORT_SINGLE, 3, 0, 1),
                  0, "write valid single abort sidecar");
    KfAbortHeader hdr;
    memset(&hdr, 0xAA, sizeof(hdr));
    int rc = kf_abort_read_exact(single_path, KF_ABORT_SINGLE, 3, 0, 1, &hdr);
    ASSERT_EQ_INT(rc, 0, "valid single sidecar parses");
    ASSERT_EQ_INT((int)hdr.magic, (int)KF_ABORT_MAGIC, "magic round-trips");
    ASSERT_EQ_INT((int)hdr.kind, (int)KF_ABORT_SINGLE, "kind round-trips");
    ASSERT_EQ_INT((int)hdr.kf_shard, 3, "shard round-trips");
    ASSERT_EQ_INT((int)hdr.batch_id, 0, "single batch_id is zero");
    ASSERT_EQ_INT((int)hdr.marker_count, 1, "single marker_count is one");

    /* 2) Truncated sidecar → EILSEQ, evidence retained. */
    int fd = open(single_path, O_WRONLY);
    ASSERT_TRUE(fd >= 0, "open sidecar for truncation");
    if (fd >= 0) {
        ASSERT_EQ_INT(ftruncate(fd, 12), 0, "truncate sidecar to 12 bytes");
        close(fd);
    }
    errno = 0;
    rc = kf_abort_read_exact(single_path, KF_ABORT_SINGLE, 3, 0, 1, &hdr);
    ASSERT_EQ_INT(rc, -1, "truncated sidecar fails closed");
    ASSERT_EQ_INT(errno, EILSEQ, "truncated sidecar sets EILSEQ");
    struct stat st;
    ASSERT_EQ_INT(stat(single_path, &st), 0,
                  "truncated sidecar evidence is retained");
    unlink(single_path);

    /* 3) Checksum-invalid (corrupt a header byte) → EILSEQ, retained. */
    ASSERT_EQ_INT(kf_abort_write_sidecar(data_dir, KF_ABORT_SINGLE, 3, 0, 1),
                  0, "rewrite valid single abort sidecar");
    fd = open(single_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open sidecar to corrupt a byte");
    if (fd >= 0) {
        uint8_t byte = 0;
        ASSERT_EQ_INT((int)pread(fd, &byte, 1, 4), 1, "read version byte");
        byte ^= 0xFF;
        ASSERT_EQ_INT((int)pwrite(fd, &byte, 1, 4), 1, "flip version byte");
        close(fd);
    }
    errno = 0;
    rc = kf_abort_read_exact(single_path, KF_ABORT_SINGLE, 3, 0, 1, &hdr);
    ASSERT_EQ_INT(rc, -1, "checksum-invalid sidecar fails closed");
    ASSERT_EQ_INT(errno, EILSEQ, "checksum-invalid sidecar sets EILSEQ");
    ASSERT_EQ_INT(stat(single_path, &st), 0,
                  "checksum-invalid sidecar evidence is retained");
    unlink(single_path);

    /* 4) Valid sidecar rejected by a wrong expected shard. */
    ASSERT_EQ_INT(kf_abort_write_sidecar(data_dir, KF_ABORT_SINGLE, 5, 0, 1),
                  0, "write sidecar for shard 5");
    char shard5_path[PATH_MAX];
    snprintf(shard5_path, sizeof(shard5_path), "%s/%03x_marker_abort.dat",
             data_kf, 5);
    errno = 0;
    rc = kf_abort_read_exact(shard5_path, KF_ABORT_SINGLE, 6, 0, 1, &hdr);
    ASSERT_EQ_INT(rc, -1, "shard mismatch fails closed");
    ASSERT_EQ_INT(errno, EILSEQ, "shard mismatch sets EILSEQ");
    ASSERT_EQ_INT(stat(shard5_path, &st), 0,
                  "wrong-shard sidecar evidence is retained");
    unlink(shard5_path);

    /* 5) Batch sidecar rejected by a wrong batch id. */
    char batch_path[PATH_MAX];
    snprintf(batch_path, sizeof(batch_path), "%s/%03x_batch_%u_abort.dat",
             data_kf, 5, 7);
    ASSERT_EQ_INT(kf_abort_write_sidecar(data_dir, KF_ABORT_BATCH, 5, 7, 4),
                  0, "write valid batch abort sidecar");
    rc = kf_abort_read_exact(batch_path, KF_ABORT_BATCH, 5, 7, 4, &hdr);
    ASSERT_EQ_INT(rc, 0, "control: correct batch params parse");
    errno = 0;
    rc = kf_abort_read_exact(batch_path, KF_ABORT_BATCH, 5, 7, 4, &hdr);
    ASSERT_EQ_INT(rc, 0, "batch sidecar parses with its own batch id");
    errno = 0;
    rc = kf_abort_read_exact(batch_path, KF_ABORT_BATCH, 5, 999, 4, &hdr);
    ASSERT_EQ_INT(rc, -1, "batch-id mismatch fails closed");
    ASSERT_EQ_INT(errno, EILSEQ, "batch-id mismatch sets EILSEQ");
    ASSERT_EQ_INT(stat(batch_path, &st), 0,
                  "wrong-batch-id sidecar evidence is retained");

    /* 6) marker-count mismatch. */
    errno = 0;
    rc = kf_abort_read_exact(batch_path, KF_ABORT_BATCH, 5, 7, 4, &hdr);
    ASSERT_EQ_INT(rc, 0, "batch sidecar parses with its own marker_count");
    errno = 0;
    rc = kf_abort_read_exact(batch_path, KF_ABORT_BATCH, 5, 7, 9, &hdr);
    ASSERT_EQ_INT(rc, -1, "marker-count mismatch fails closed");
    ASSERT_EQ_INT(errno, EILSEQ, "marker-count mismatch sets EILSEQ");
    ASSERT_EQ_INT(stat(batch_path, &st), 0,
                  "marker-count-mismatched sidecar evidence is retained");

    /* 7) Trailing bytes beyond the fixed header are rejected. */
    int fd2 = open(batch_path, O_RDWR);
    ASSERT_TRUE(fd2 >= 0, "open batch sidecar to append trailing bytes");
    if (fd2 >= 0) {
        off_t end = lseek(fd2, 0, SEEK_END);
        ASSERT_EQ_INT((int)end, (int)sizeof(KfAbortHeader),
                      "sidecar is exactly the fixed header");
        ASSERT_EQ_INT((int)write(fd2, "X", 1), 1, "append trailing byte");
        close(fd2);
    }
    errno = 0;
    rc = kf_abort_read_exact(batch_path, KF_ABORT_BATCH, 5, 7, 4, &hdr);
    ASSERT_EQ_INT(rc, -1, "sidecar with trailing bytes fails closed");
    ASSERT_EQ_INT(errno, EILSEQ, "trailing-byte sidecar sets EILSEQ");
    ASSERT_EQ_INT(stat(batch_path, &st), 0,
                  "trailing-byte sidecar evidence is retained");

    crash_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_slotcask_v2_crash_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-v2crash-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    crash_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) {
        ASSERT_TRUE(0, "pick port");
        crash_run_cmd("rm -rf %s", base);
        return 1;
    }

    /* === First daemon: seed BASELINE committed records on a v2 object. === */
    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "first daemon spawn");
        crash_run_cmd("rm -rf %s", base);
        return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "first connect");
    if (!tc) { test_env_kill(&env); crash_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"crash2\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:32\"]}", &resp);
    free(resp); resp = NULL;

    char req[256];
    int baseline_inserted = 0;
    for (int i = 0; i < BASELINE; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"crash2\","
            "\"key\":\"k%07d\",\"value\":{\"v\":\"val_%d\"}}", i, i);
        if (tc_request(tc, req, &resp) != 0) break;
        free(resp); resp = NULL;
        baseline_inserted++;
    }
    ASSERT_EQ_INT(baseline_inserted, BASELINE, "all baseline inserts succeeded");

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"crash2\"}", &resp);
    int count_pre = crash_parse_count(resp);
    free(resp); resp = NULL;
    ASSERT_EQ_INT(count_pre, BASELINE, "baseline count matches pre-crash");

    /* === Crash phase: send more inserts; SIGKILL halfway. === */
    int crash_phase_committed = 0;
    int kill_after = CRASH_AT / 2;
    for (int i = BASELINE; i < BASELINE + CRASH_AT; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"crash2\","
            "\"key\":\"k%07d\",\"value\":{\"v\":\"val_%d\"}}", i, i);
        if (tc_request(tc, req, &resp) != 0) {
            free(resp); resp = NULL;
            break;
        }
        free(resp); resp = NULL;
        crash_phase_committed++;
        if (crash_phase_committed >= kill_after) {
            test_env_kill(&env);
            break;
        }
    }
    tc_close(tc); tc = NULL;

    /* === Restart at the same DB_ROOT + port. === */
    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn at same db_root");
        crash_run_cmd("rm -rf %s", base);
        return 1;
    }

    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (!tc) { test_env_kill(&env2); crash_run_cmd("rm -rf %s", base); return 1; }

    /* Recount via the wire — slotcask_walk_live counts live entries. */
    tc_request(tc,
        "{\"mode\":\"recount\",\"dir\":\"default\",\"object\":\"crash2\"}", &resp);
    int count_after = crash_parse_count(resp);
    free(resp); resp = NULL;

    ASSERT_TRUE(count_after >= BASELINE,
                "post-crash count >= baseline (committed survives)");
    ASSERT_TRUE(count_after <= BASELINE + CRASH_AT,
                "post-crash count <= total attempted");
    ASSERT_TRUE(count_after >= BASELINE + crash_phase_committed,
                "every acked-before-crash record survives");

    /* Spot-check baseline keys (every 100th) — content survived intact. */
    int spot_check_failures = 0;
    for (int i = 0; i < BASELINE; i += 100) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"crash2\","
            "\"key\":\"k%07d\"}", i);
        if (tc_request(tc, req, &resp) != 0 || !resp) {
            spot_check_failures++;
        } else {
            char expected[64];
            snprintf(expected, sizeof(expected), "\"val_%d\"", i);
            if (!SAFE_STRSTR(resp, expected)) spot_check_failures++;
        }
        free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(spot_check_failures, 0,
                  "all baseline keys readable + intact post-crash");

    /* recover_streams correctness check: the post-restart slotcask_open
       must re-derive reserve_off from disk, otherwise the next insert
       would clobber the head record at offset 0. Insert a fresh key,
       then read back BOTH the new key and a baseline key from offset
       0's hash region — both must be intact. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"crash2\","
        "\"key\":\"post_recovery\",\"value\":{\"v\":\"fresh_after_crash\"}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"",
                    "post-crash insert succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"crash2\","
        "\"key\":\"post_recovery\"}", &resp);
    ASSERT_CONTAINS(resp, "\"v\":\"fresh_after_crash\"",
                    "post-crash insert read-back works");
    free(resp); resp = NULL;

    /* The key whose hash routes to slot 0 of stream 0's segment 0 is
       implementation-specific, but k0000000 is one of the earliest
       inserted records and must still be readable — proves the
       post-recovery insert didn't clobber early records. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"crash2\","
        "\"key\":\"k0000000\"}", &resp);
    ASSERT_CONTAINS(resp, "\"v\":\"val_0\"",
                    "k0000000 still intact after post-recovery insert "
                    "(reserve_off derived correctly)");
    free(resp); resp = NULL;

    /* Sanity: a key past CRASH_AT (never attempted) is absent. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"crash2\","
        "\"key\":\"k%07d\"}", BASELINE + CRASH_AT + 9999);
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && (SAFE_STRSTR(resp, "Not found") || SAFE_STRSTR(resp, "error")),
                "never-attempted key is absent (no phantom records)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env2);
    crash_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-abort-sidecar-parser", test_abort_sidecar_parser)
TEST_REGISTER("test-slotcask-v2-crash", test_slotcask_v2_crash_run)
