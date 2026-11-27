/* Regression: an insert window that rejects a record AFTER
 * its kf slot was reserved (ordinary bitmap-cap rejection; faults are a
 * second route) writes an inert tombstone with NO counter bookkeeping.
 * A later insert reusing that flag==2 slot commits with hdr->deleted--
 * (the planner cannot tell an inert tombstone from a real one), which
 * underflows `deleted` to UINT64_MAX when the shard's deleted was 0 —
 * and resolve_counts' object-level clamp then reports count = 0 for a
 * fully intact object. Fix: (a) book inert tombstones at write
 * (total++/deleted++, once, durably), (b) non-wrapping reuse guard.
 *
 * Case A (wire, deterministic): v:bitmap(4) + 80 records over 6
 *   distinct values → the cap rejects some records after reservation
 *   (others survive); per-shard header counters are pinned right after
 *   the bulk-insert (sum(total) == 80, sum(deleted) == errors — the
 *   booking itself), re-insert each rejected key with an allowed value
 *   (reusing its tombstone) and pin sum(deleted) back to 0. Assert
 *   count == live model, all keys readable, orphaned >= 0, and every
 *   kf shard header has total >= deleted (no wrap). RED on base.
 *
 * Case B (reuse guard in isolation): craft a NEVER-COUNTED inert
 *   tombstone directly (flag=2 slot + header total=0, deleted=0 —
 *   the state a rejected reservation left before tombstones were booked,
 *   i.e. what a legacy on-disk root can still hold), reopen on a FRESH
 *   free port, re-insert that key; assert no wrap and live exact.
 *   RED on base (wrap).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <errno.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define OBJ_A "kfub_a"
#define OBJ_B "kfub_b"
#define NRECS_A 80
#define NDISTINCT 6   /* > bitmap(4) cap */
#define NSHARDS 8

static uint64_t rd_le64(const uint8_t *p) {
    return (uint64_t)p[0] | (uint64_t)p[1] << 8 | (uint64_t)p[2] << 16 |
           (uint64_t)p[3] << 24 | (uint64_t)p[4] << 32 |
           (uint64_t)p[5] << 40 | (uint64_t)p[6] << 48 |
           (uint64_t)p[7] << 56;
}

/* Read one kf shard header. Returns 0 ok (total/deleted out),
 * -1 file missing. Layout: magic u32, version u32, total u64,
 * deleted u64 — 24 bytes, little-endian (packed struct). */
static int read_kf_header(const char *path, uint32_t *magic,
                          uint64_t *total, uint64_t *deleted) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    uint8_t buf[24];
    ssize_t r = pread(fd, buf, sizeof(buf), 0);
    close(fd);
    if (r != (ssize_t)sizeof(buf)) return -1;
    *magic = (uint32_t)buf[0] | (uint32_t)buf[1] << 8 |
             (uint32_t)buf[2] << 16 | (uint32_t)buf[3] << 24;
    *total = rd_le64(buf + 8);
    *deleted = rd_le64(buf + 16);
    return 0;
}

static void kf_path(char *out, size_t cap, const char *db_root,
                    const char *obj, int shard) {
    snprintf(out, cap, "%s/default/%s/data/kf/%03d.kf", db_root, obj,
             shard);
}

/* Insert one record (single-key insert). */
static void insert_one(TestClient *tc, const char *obj, const char *key,
                       const char *vjson) {
    char req[512];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\",\"value\":%s}", obj, key, vjson);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "insert response");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "insert accepted");
    free(resp);
}

static int test_kf_deleted_underflow_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* ===== Case A: bitmap-cap rejection → inert tombstone → reuse wrap */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"" OBJ_A "\",\"splits\":" "8" ",\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\"],"
        "\"indexes\":[\"v:bitmap(4)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "case A object created");
    free(resp); resp = NULL;

    /* 80 records over 6 distinct v values: each shard sees ~10 records
     * and ~5-6 distinct values, so the 4-value cap rejects some
     * records (after kf reservation) while their window siblings
     * survive. */
    const size_t reclen = 64;
    char *body = malloc((size_t)NRECS_A * reclen + 16);
    ASSERT_NOT_NULL(body, "alloc case A body");
    size_t off = 0;
    off += (size_t)snprintf(body + off, 8, "[");
    for (int i = 0; i < NRECS_A; i++) {
        off += (size_t)snprintf(body + off, reclen,
            "%s{\"key\":\"k%03d\",\"value\":{\"v\":\"v%d\"}}",
            i ? "," : "", i, i % NDISTINCT);
    }
    off += (size_t)snprintf(body + off, 8, "]");
    char *req = malloc(off + 160);
    ASSERT_NOT_NULL(req, "alloc case A request");
    snprintf(req, off + 160,
             "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
             "\"object\":\"" OBJ_A "\",\"records\":%s}", body);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "case A bulk response");
    int n_ins = -1, n_err = -1;
    if (resp) {
        const char *q = SAFE_STRSTR(resp, "\"inserted\":");
        if (q) n_ins = atoi(q + strlen("\"inserted\":"));
        q = SAFE_STRSTR(resp, "\"errors\":");
        if (q) n_err = atoi(q + strlen("\"errors\":"));
    }
    char msg[128];
    snprintf(msg, sizeof(msg),
             "case A: cap rejected some records (inserted=%d errors=%d)",
             n_ins, n_err);
    ASSERT_TRUE(n_ins > 0 && n_err > 0, msg);
    free(resp); resp = NULL;
    free(body);   /* req stays allocated: reused by every later probe */
    /* Case A assumes every shard accepted at least one record: a shard
       whose whole window was rejected contributes rejected keys whose
       hashes may collide with kept records' probe chains elsewhere.
       With 80 records over 8 shards and a 4-value cap rejecting only
       part of each window, that holds; a future change to the
       rejection distribution must revisit it. */
    if (n_ins <= 0 || n_err <= 0) { free(req); test_env_stop(&env); return 1; }

    /* Pin the booking itself on the per-shard kf headers right after the
     * bulk-insert: every record reserved a slot (sum(total) == NRECS_A)
     * and every rejected reservation is booked as lived-and-died
     * (sum(deleted) == n_err). On base the rejected tombstones are never
     * counted, so sum(deleted) reads 0. */
    {
        uint64_t sum_total = 0, sum_deleted = 0;
        for (int s = 0; s < NSHARDS; s++) {
            char path[512];
            kf_path(path, sizeof(path), env.db_root, OBJ_A, s);
            uint32_t magic;
            uint64_t total, deleted;
            if (read_kf_header(path, &magic, &total, &deleted) != 0)
                continue;   /* lazy-created: shard never written */
            sum_total += total;
            sum_deleted += deleted;
        }
        snprintf(msg, sizeof(msg),
                 "case A: booking pinned (sum total=%llu == %d, "
                 "sum deleted=%llu == %d)",
                 (unsigned long long)sum_total, NRECS_A,
                 (unsigned long long)sum_deleted, n_err);
        ASSERT_TRUE((long)sum_total == NRECS_A &&
                    (long)sum_deleted == n_err, msg);
    }

    /* Identify rejected keys (absent from the store) and re-insert each
     * with an allowed value — each reuse commits with hdr->deleted--
     * on base. */
    int n_missing = 0;
    for (int i = 0; i < NRECS_A; i++) {
        char key[16];
        snprintf(key, sizeof(key), "k%03d", i);
        snprintf(req, 512,
                 "{\"mode\":\"exists\",\"dir\":\"default\","
                 "\"object\":\"" OBJ_A "\",\"key\":\"%s\"}", key);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case A: exists probe");
        int absent = resp && SAFE_STRSTR(resp, "false") != NULL;
        free(resp); resp = NULL;
        if (absent) {
            /* the shard's own bitmap dict may hold 4 of the 6 values and
               not include v0 — try each value until one is accepted */
            int done = 0;
            for (int v = 0; v < NDISTINCT && !done; v++) {
                char vj[32];
                snprintf(vj, sizeof(vj), "{\"v\":\"v%d\"}", v);
                char ireq[512];
                char *ir = NULL;
                snprintf(ireq, sizeof(ireq),
                         "{\"mode\":\"insert\",\"dir\":\"default\","
                         "\"object\":\"%s\",\"key\":\"%s\","
                         "\"value\":%s}", OBJ_A, key, vj);
                tc_request(tc, ireq, &ir);
                if (ir && SAFE_STRSTR(ir, "\"error\"") == NULL) done = 1;
                free(ir);
            }
            ASSERT_EQ_INT(done, 1, "case A: rejected key re-inserted");
            n_missing++;
        }
    }
    snprintf(msg, sizeof(msg),
             "case A: rejected keys found and re-inserted (%d/%d)",
             n_missing, n_err);
    ASSERT_EQ_INT(n_missing, n_err, msg);

    /* Every re-insert reused its booked tombstone: the deleted counter
     * must return to 0 (each reuse commits with hdr->deleted--). */
    {
        uint64_t sum_deleted = 0;
        for (int s = 0; s < NSHARDS; s++) {
            char path[512];
            kf_path(path, sizeof(path), env.db_root, OBJ_A, s);
            uint32_t magic;
            uint64_t total, deleted;
            if (read_kf_header(path, &magic, &total, &deleted) != 0)
                continue;
            sum_deleted += deleted;
        }
        snprintf(msg, sizeof(msg),
                 "case A: deleted back to 0 after the re-inserts (%llu)",
                 (unsigned long long)sum_deleted);
        ASSERT_TRUE(sum_deleted == 0, msg);
    }

    /* Live model (probe-tolerant): every key that reads back true now.
     * On the fixed build this is inserted + re-inserted (all of them);
     * the assertion pins count == that model either way. */
    long live_model = 0;
    for (int i = 0; i < NRECS_A; i++) {
        char key[16];
        snprintf(key, sizeof(key), "k%03d", i);
        snprintf(req, 512,
                 "{\"mode\":\"exists\",\"dir\":\"default\","
                 "\"object\":\"" OBJ_A "\",\"key\":\"%s\"}", key);
        tc_request(tc, req, &resp);
        snprintf(msg, sizeof(msg), "case A: key %s readable", key);
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "true") != NULL, msg);
        if (resp && SAFE_STRSTR(resp, "true")) live_model++;
        free(resp); resp = NULL;
    }

    /* count == live model. On base the wrapped shard clamps the whole
     * object to 0. */
    snprintf(req, 512,
             "{\"mode\":\"count\",\"dir\":\"default\","
             "\"object\":\"" OBJ_A "\"}");
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "case A count response");
    snprintf(msg, sizeof(msg), "case A: count == live model (%ld)",
             live_model);
    ASSERT_TRUE(resp && isdigit((unsigned char)resp[0]) && atol(resp) == live_model, msg);
    free(resp); resp = NULL;

    /* orphaned >= 0 (base returns -1: (int) of the wrapped u64). */
    snprintf(req, 512,
             "{\"mode\":\"orphaned\",\"dir\":\"default\","
             "\"object\":\"" OBJ_A "\"}");
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "case A orphaned response");
    snprintf(msg, sizeof(msg), "case A: orphaned >= 0");
    ASSERT_TRUE(resp && isdigit((unsigned char)resp[0]) && atol(resp) >= 0, msg);
    free(resp); resp = NULL;

    /* Per-shard kf headers: total >= deleted (no wrap). */
    for (int s = 0; s < NSHARDS; s++) {
        char path[512];
        kf_path(path, sizeof(path), env.db_root, OBJ_A, s);
        uint32_t magic;
        uint64_t total, deleted;
        if (read_kf_header(path, &magic, &total, &deleted) != 0)
            continue;   /* lazy-created: shard never written */
        snprintf(msg, sizeof(msg),
                 "case A: shard %03d header total(%llu) >= deleted(%llu)",
                 s, (unsigned long long)total,
                 (unsigned long long)deleted);
        ASSERT_TRUE(total >= deleted, msg);
    }

    /* ===== Case B: crafted NEVER-COUNTED inert tombstone + reuse guard
     * (isolates fix part (b) from part (a)). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"" OBJ_B "\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"t:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "case B object created");
    free(resp); resp = NULL;

    insert_one(tc, OBJ_B, "kA", "{\"t\":1}");

    /* Stop the daemon, keep the root: craft the inert tombstone by
     * writing the kf slot flag directly (pattern of
     * test_slotcask_grave_run_frontier.c). With one record inserted,
     * exactly one entry across all shards holds flag==1 — tombstone it
     * and zero header total so the shard mirrors the state a rejected
     * reservation leaves: flag==2 slot, total=0, deleted=0. */
    tc_close(tc);   /* the client object would otherwise leak across the
                     * restart (test_env_stop does not free clients) */
    test_env_stop_keep(&env);
    {
        int fixed = 0;
        for (int s = 0; s < NSHARDS && !fixed; s++) {
            char path[512];
            kf_path(path, sizeof(path), env.db_root, OBJ_B, s);
            int fd = open(path, O_RDWR);
            if (fd < 0) continue;
            uint8_t hdr[24];
            if (pread(fd, hdr, 24, 0) != 24) { close(fd); continue; }
            uint32_t magic = (uint32_t)hdr[0] | (uint32_t)hdr[1] << 8 |
                             (uint32_t)hdr[2] << 16 |
                             (uint32_t)hdr[3] << 24;
            uint64_t total = rd_le64(hdr + 8);
            if (magic != 0x31464B53u || total != 1) { close(fd); continue; }
            /* find the single live entry and tombstone it */
            off_t sz = lseek(fd, 0, SEEK_END);
            if (sz < 24) { close(fd); continue; }
            uint64_t cap = ((uint64_t)sz - 24) / 24;
            for (uint64_t e = 0; e < cap; e++) {
                uint8_t ent[24];
                if (pread(fd, ent, 24, 24 + (off_t)e * 24) != 24) break;
                if (ent[16] == 1) {   /* kf entry flag byte (hash[16] then flag) */
                    uint8_t two = 2;
                    ASSERT_EQ_INT((int)pwrite(fd, &two, 1,
                                              24 + (off_t)e * 24 + 16),
                                  1, "case B: slot tombstoned");
                    uint64_t zero = 0;
                    ASSERT_EQ_INT((int)pwrite(fd, &zero, 8, 8), 8,
                                  "case B: header total zeroed");
                    fixed = 1;
                    break;
                }
            }
            close(fd);
        }
        ASSERT_EQ_INT(fixed, 1, "case B: crafted the inert tombstone");
    }

    /* Reopen on a FRESH free port (the old port's listener may not be
     * re-bindable yet) and re-insert the same key: its hash probes to
     * the same slot, the planner sees flag==2 (reused_tomb) and commits
     * with hdr->deleted-- — base wraps deleted to UINT64_MAX (deleted
     * was 0); post-fix the non-wrapping guard does hdr->total++ instead
     * and live stays exact. */
    int reopen_port = test_pick_port();
    ASSERT_TRUE(reopen_port > 0, "case B: picked a fresh free port");
    if (test_env_start_at(&env, env.db_root, reopen_port) != 0) {
        printf("reopen failed\n");
        free(req);
        test_env_stop(&env);
        return 1;
    }
    TestClientCfg cfg2 = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc2 = tc_connect(&cfg2);
    ASSERT_NOT_NULL(tc2, "case B reconnect");
    if (!tc2) { free(req); test_env_stop(&env); return 1; }
    tc = tc2;

    /* sanity after reopen: the corrupted key reads as absent (its slot
     * is a tombstone) and count is 0 — the header reads total=0,
     * deleted=0, so live = total - deleted = 0 */
    snprintf(req, 512,
             "{\"mode\":\"exists\",\"dir\":\"default\","
             "\"object\":\"" OBJ_B "\",\"key\":\"kA\"}");
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "false") != NULL,
                "case B: kA absent after the crafted tombstone");
    free(resp); resp = NULL;

    insert_one(tc, OBJ_B, "kA", "{\"t\":2}");

    snprintf(req, 512,
             "{\"mode\":\"count\",\"dir\":\"default\","
             "\"object\":\"" OBJ_B "\"}");
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "case B count response");
    ASSERT_TRUE(resp && isdigit((unsigned char)resp[0]) && atol(resp) == 1,
                "case B: count == 1 (live exact, no wrap)");
    free(resp); resp = NULL;

    for (int s = 0; s < NSHARDS; s++) {
        char path[512];
        kf_path(path, sizeof(path), env.db_root, OBJ_B, s);
        uint32_t magic;
        uint64_t total, deleted;
        if (read_kf_header(path, &magic, &total, &deleted) != 0)
            continue;
        snprintf(msg, sizeof(msg),
                 "case B: shard %03d header total(%llu) >= deleted(%llu)",
                 s, (unsigned long long)total,
                 (unsigned long long)deleted);
        ASSERT_TRUE(total >= deleted, msg);
    }

    tc_close(tc);
    free(req);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-kf-deleted-underflow", test_kf_deleted_underflow_run)
