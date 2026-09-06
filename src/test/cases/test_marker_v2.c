/* Task 4 — marker format V2 (16 B header + count × 32 B KfMarkerSlot,
 * exact-size validation, 1 ≤ count ≤ 16384, v1 refused).
 * docs/plans/2026-09-05-request-level-commit-batching.md.
 *
 * Red on base: the exact-path test accessors
 * (kf_batch_marker_read_path_for_test and the path-taking
 * kf_batch_marker_corrupt_first_kf_slot_for_test) and V2 support do not
 * exist yet (compile-red on base).
 *
 * Parser cases run through the test-only exact-path wrapper — no request
 * coordinator needed. Gate-integration cases drive a real retained
 * marker through the deferred request's commit-flush failure and then a
 * follow-up single-record writer (the gate is the only replay consumer
 * in-process). Marker surgery never happens while a live request owns
 * the shard gate: the requesting call has returned (gates released)
 * before any file is touched.
 */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "shard_db_internal.h"
#include "shard_test_ctl.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

/* Local V1 disk fixture (the production typedef is deleted by Task 4;
 * only this test-local definition may carry the name). */
typedef struct __attribute__((packed)) {
    KfMarkerSlot slot;
    uint8_t  hash[16];
    uint16_t klen;
    uint16_t old_vlen;
    uint16_t new_vlen;
} BatchMarkerEntry;                     /* 54 bytes — the V1 layout */

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
    uint32_t reserved;
} Mv2Header;                            /* same 16 B shape in V1 and V2 */

enum { MV2_MAGIC = 0x4B464D32u };       /* "KFM2" */

static void mv2_cleanup(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    system(cmd);
}

static int mv2_write_file(const char *path, const void *bytes, size_t len) {
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    if (len && fwrite(bytes, 1, len, f) != len) { fclose(f); return -1; }
    if (fclose(f) != 0) return -1;
    return 0;
}

/* Build a V2 marker blob: header + count slots, checksums valid. */
static int mv2_build_v2(uint32_t version, uint32_t count, uint32_t reserved,
                        uint8_t op, uint32_t kf_slot, uint8_t **out,
                        size_t *out_len) {
    size_t len = sizeof(Mv2Header) + (size_t)count * 32;
    uint8_t *buf = calloc(1, len);
    if (!buf) return -1;
    Mv2Header hdr = { MV2_MAGIC, version, count, reserved };
    memcpy(buf, &hdr, sizeof(hdr));
    for (uint32_t i = 0; i < count; i++) {
        KfMarkerSlot slot;
        memset(&slot, 0, sizeof(slot));
        slot.magic = 0x4B464D32u;
        slot.op = op;
        slot.kf_slot = kf_slot;
        slot.new_offset = 0x1000 + i;
        slot.new_file_id = 3;
        slot.new_stream_id = 1;
        slot.checksum = 0;
        slot.checksum = XXH32(&slot, offsetof(KfMarkerSlot, checksum), 0);
        memcpy(buf + sizeof(Mv2Header) + (size_t)i * 32, &slot, 32);
    }
    *out = buf;
    *out_len = len;
    return 0;
}

/* Build a fully valid V1 marker (header v1 + one 54 B entry + zero-length
 * spans, correct checksum) so on base the same file parses successfully. */
static int mv2_build_v1(uint8_t **out, size_t *out_len) {
    size_t len = sizeof(Mv2Header) + sizeof(BatchMarkerEntry);
    uint8_t *buf = calloc(1, len);
    if (!buf) return -1;
    Mv2Header hdr = { MV2_MAGIC, 1, 1, 0 };
    memcpy(buf, &hdr, sizeof(hdr));
    BatchMarkerEntry e;
    memset(&e, 0, sizeof(e));
    e.slot.magic = 0x4B464D32u;
    e.slot.op = 1;                      /* UPSERT */
    e.slot.kf_slot = 0xFFFFFFFFu;
    e.slot.new_offset = 0x1000;
    e.slot.new_file_id = 3;
    e.slot.new_stream_id = 1;
    e.slot.checksum = 0;
    e.slot.checksum = XXH32(&e, sizeof(e), 0);
    memcpy(buf + sizeof(Mv2Header), &e, sizeof(e));
    *out = buf;
    *out_len = len;
    return 0;
}

/* ── parser cases (exact-path test wrapper, no coordinator) ─────────── */

static int mv2_parser_cases(void) {
    char base[] = "/tmp/shard-db-marker-v2-XXXXXX";
    if (!mkdtemp(base)) return 1;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/marker.dat", base);

    KfMarkerSlot slots[4];
    size_t count = 0;
    uint8_t *buf = NULL;
    size_t len = 0;

    /* 1. golden: valid V2, count=1 → parses; slot round-trips. */
    ASSERT_EQ_INT(mv2_build_v2(2, 1, 0, 1, 0xFFFFFFFFu, &buf, &len), 0,
                  "build golden V2");
    ASSERT_EQ_INT(mv2_write_file(path, buf, len), 0, "write golden V2");
    free(buf); buf = NULL;
    errno = 0;
    ASSERT_EQ_INT(kf_batch_marker_read_path_for_test(path, slots, 4, &count),
                  0, "golden V2 parses");
    ASSERT_EQ_INT(count, 1, "golden V2 count");
    ASSERT_EQ_INT((int)slots[0].op, 1, "golden V2 op");
    ASSERT_EQ_INT((int)slots[0].new_stream_id, 1, "golden V2 stream");

    /* 2. trailing byte → fail closed. */
    ASSERT_EQ_INT(mv2_write_file(path, NULL, 0), 0, "truncate for append");
    ASSERT_EQ_INT(mv2_build_v2(2, 1, 0, 1, 0xFFFFFFFFu, &buf, &len), 0, "b");
    ASSERT_EQ_INT(mv2_write_file(path, buf, len), 0, "write for append");
    free(buf); buf = NULL;
    { FILE *f = fopen(path, "ab"); ASSERT_TRUE(f != NULL, "append open");
      if (f) { fputc(0x41, f); fclose(f); } }
    ASSERT_EQ_INT(kf_batch_marker_read_path_for_test(path, slots, 4, &count),
                  -1, "trailing byte rejected");

    /* 3. truncated to 16+(count−1)×32 → fail closed. */
    ASSERT_EQ_INT(mv2_build_v2(2, 2, 0, 1, 0xFFFFFFFFu, &buf, &len), 0, "c");
    ASSERT_EQ_INT(mv2_write_file(path, buf, len - 32), 0, "write truncated");
    free(buf); buf = NULL;
    ASSERT_EQ_INT(kf_batch_marker_read_path_for_test(path, slots, 4, &count),
                  -1, "truncated rejected");

    /* 4. count = 0 → fail closed. */
    ASSERT_EQ_INT(mv2_build_v2(2, 0, 0, 1, 0xFFFFFFFFu, &buf, &len), 0, "d");
    ASSERT_EQ_INT(mv2_write_file(path, buf, len), 0, "write count0");
    free(buf); buf = NULL;
    ASSERT_EQ_INT(kf_batch_marker_read_path_for_test(path, slots, 4, &count),
                  -1, "count=0 rejected");

    /* 5. count = 16385 with a size-matching file → fail closed. */
    {
        uint32_t big = 16385;
        size_t blen = sizeof(Mv2Header) + (size_t)big * 32;
        uint8_t *bb = calloc(1, blen);
        ASSERT_TRUE(bb != NULL, "alloc 16385-slot file");
        if (bb) {
            Mv2Header hdr = { MV2_MAGIC, 2, big, 0 };
            memcpy(bb, &hdr, sizeof(hdr));
            ASSERT_EQ_INT(mv2_write_file(path, bb, blen), 0,
                          "write count16385");
            free(bb);
        }
    }
    ASSERT_EQ_INT(kf_batch_marker_read_path_for_test(path, slots, 4, &count),
                  -1, "count=16385 rejected");

    /* 6. bad checksum (flip a bit in slot 0 new_offset) → fail closed. */
    ASSERT_EQ_INT(mv2_build_v2(2, 1, 0, 1, 0xFFFFFFFFu, &buf, &len), 0, "f");
    buf[sizeof(Mv2Header) + offsetof(KfMarkerSlot, new_offset)] ^= 0x01;
    ASSERT_EQ_INT(mv2_write_file(path, buf, len), 0, "write badsum");
    free(buf); buf = NULL;
    ASSERT_EQ_INT(kf_batch_marker_read_path_for_test(path, slots, 4, &count),
                  -1, "bad checksum rejected");

    /* 7. invalid op (op = 7, checksum fixed up) → fail closed. */
    ASSERT_EQ_INT(mv2_build_v2(2, 1, 0, 7, 0xFFFFFFFFu, &buf, &len), 0, "g");
    {
        KfMarkerSlot s;
        memcpy(&s, buf + sizeof(Mv2Header), sizeof(s));
        s.checksum = 0;
        s.checksum = XXH32(&s, offsetof(KfMarkerSlot, checksum), 0);
        memcpy(buf + sizeof(Mv2Header), &s, sizeof(s));
    }
    ASSERT_EQ_INT(mv2_write_file(path, buf, len), 0, "write badop");
    free(buf); buf = NULL;
    ASSERT_EQ_INT(kf_batch_marker_read_path_for_test(path, slots, 4, &count),
                  -1, "invalid op rejected");

    /* 8. nonzero header reserved → fail closed. */
    ASSERT_EQ_INT(mv2_build_v2(2, 1, 0xBEEF, 1, 0xFFFFFFFFu, &buf, &len), 0,
                  "h");
    ASSERT_EQ_INT(mv2_write_file(path, buf, len), 0, "write reserved");
    free(buf); buf = NULL;
    ASSERT_EQ_INT(kf_batch_marker_read_path_for_test(path, slots, 4, &count),
                  -1, "nonzero reserved rejected");

    /* 9. v1 refused: a valid V1 marker must be refused with ENOTSUP
       (red on base: the same file parses as V1 there). */
    ASSERT_EQ_INT(mv2_build_v1(&buf, &len), 0, "build V1 fixture");
    ASSERT_EQ_INT(mv2_write_file(path, buf, len), 0, "write V1");
    free(buf); buf = NULL;
    errno = 0;
    ASSERT_EQ_INT(kf_batch_marker_read_path_for_test(path, slots, 4, &count),
                  -1, "v1 refused");
    ASSERT_EQ_INT(errno, ENOTSUP, "v1 refusal errno ENOTSUP");

    mv2_cleanup(base);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ── gate-integration cases (real retained V2 marker + follow-up
 *    single-record writer) ───────────────────────────────────────────── */

typedef struct {
    SlotcaskDb db;
    char base[PATH_MAX];
} Mv2Db;

static int mv2_db_open(Mv2Db *w) {
    slotcask_init(64, 64);
    char b[] = "/tmp/shard-db-marker-v2-gate-XXXXXX";
    if (!mkdtemp(b)) return -1;
    snprintf(w->base, sizeof(w->base), "%s", b);
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", w->base);
    snprintf(k, sizeof(k), "%s/data/kf", w->base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return -1;
    memset(&w->db, 0, sizeof(w->db));
    if (slotcask_open(&w->db, w->base, 8, 1, 64) != 0) return -1;
    shard_test_ctl_reset();
    return 0;
}

static void mv2_db_close(Mv2Db *w) {
    slotcask_close(&w->db);
    mv2_cleanup(w->base);
    slotcask_shutdown();
    shard_test_ctl_reset();
}

static int mv2_marker_count(const char *base, char paths[][PATH_MAX], int max) {
    char kdir[PATH_MAX];
    snprintf(kdir, sizeof(kdir), "%s/data/kf", base);
    DIR *d = opendir(kdir);
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        size_t L = strlen(e->d_name);
        if (L > 11 && strcmp(e->d_name + L - 11, "_marker.dat") == 0) {
            if (n < max) snprintf(paths[n], PATH_MAX, "%s/%s", kdir, e->d_name);
            n++;
        }
    }
    closedir(d);
    return n;
}

static int mv2_shard_of(const char *key) {
    uint8_t h[16];
    compute_hash_raw(key, strlen(key), h);
    return compute_record_shard(h, 8);
}

/* No-op window hooks (state NULL). */
static int mv2_prepare(SlotcaskBulkRec *recs, const size_t *active,
                       size_t nactive, void *ctx, void **out_window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    *out_window_state = NULL;
    return 0;
}
static int mv2_apply(SlotcaskBulkRec *recs, const size_t *active,
                     size_t nactive, void *ctx, void *window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx; (void)window_state;
    return 0;
}
static void mv2_terminal(void *ctx, void *window_state) {
    (void)ctx; (void)window_state;
}

/* Run a one-record deferred request on shard 0 with the commit flush
 * failing, leaving its V2 marker retained. Returns 0 on the expected
 * EINPROGRESS failure; the single record's key is provided by the
 * caller and must hash to shard 0. */
static int mv2_retain_one_marker(Mv2Db *w, const char *key, const char *val) {
    char keys[1][24];
    char vals[1][24];
    SlotcaskBulkRec recs[1];
    snprintf(keys[0], sizeof(keys[0]), "%s", key);
    snprintf(vals[0], sizeof(vals[0]), "%s", val);
    memset(&recs[0], 0, sizeof(recs[0]));
    recs[0].key = keys[0]; recs[0].klen = strlen(keys[0]);
    recs[0].value = vals[0]; recs[0].vlen = strlen(vals[0]);

    SlotcaskBulkOpts opts;
    memset(&opts, 0, sizeof(opts));
    opts.has_indexed_fields = 1;
    opts.prepare_window = mv2_prepare;
    opts.apply_window = mv2_apply;
    opts.commit_done = mv2_terminal;
    opts.release_window = mv2_terminal;
    opts.abort_window = mv2_terminal;

    SlotcaskBulkShardInput in;
    memset(&in, 0, sizeof(in));
    in.kf_shard_id = 0;
    in.recs = recs; in.nrecs = 1;
    in.kind = SLOTCASK_BULK_INPUT_UPSERT;
    in.opts.upsert = opts;

    shard_test_ctl_reset();
    g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
    g_shard_test_fail_occurrence = 1;
    g_shard_test_fail_sticky = 1;
    int rc = slotcask_bulk_request_execute(&w->db, &in, 1);
    g_shard_test_fail_phase = -1;
    g_shard_test_fail_occurrence = 0;
    g_shard_test_fail_sticky = 0;
    shard_test_ctl_reset();
    return rc;
}

/* Flip one bit inside slot 0's checksum-covered region (new_offset),
 * leaving the stored checksum untouched → the reader must fail closed. */
static int mv2_flip_bit_bad_checksum(const char *path) {
    int fd = open(path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return -1;
    size_t off = sizeof(Mv2Header) + offsetof(KfMarkerSlot, new_offset);
    uint8_t byte;
    ssize_t nr = pread(fd, &byte, 1, (off_t)off);
    if (nr != 1) { close(fd); return -1; }
    byte ^= 0x01;
    ssize_t nw = pwrite(fd, &byte, 1, (off_t)off);
    int rc = (nw == 1 && fsync(fd) == 0) ? 0 : -1;
    close(fd);
    return rc;
}

static int mv2_gate_cases(void) {
    Mv2Db w;
    ASSERT_EQ_INT(mv2_db_open(&w), 0, "open marker-v2 gate db");
    if (t_ctx->failed) return 1;

    /* Probe keys onto shard 0. */
    char k1[24], k2[24], fk[24], fv[24];
    int a = 0;
    for (;; a++) { snprintf(k1, sizeof(k1), "mv2-a-%d", a);
                   if (mv2_shard_of(k1) == 0) break; }
    a = 0;
    for (;; a++) { snprintf(k2, sizeof(k2), "mv2-b-%d", a);
                   if (mv2_shard_of(k2) == 0) break; }

    /* Positive control: retained golden V2 marker → follow-up write's
       gate replays it, clears it, record readable. */
    ASSERT_EQ_INT(mv2_retain_one_marker(&w, k1, "val1"), -1,
                  "retained marker request fails");
    ASSERT_EQ_INT(errno, EINPROGRESS, "retention errno");
    char paths[4][PATH_MAX];
    ASSERT_EQ_INT(mv2_marker_count(w.base, paths, 4), 1, "one marker");

    /* Validate the retained file through the exact-path reader. */
    KfMarkerSlot slots[4];
    size_t count = 0;
    ASSERT_EQ_INT(kf_batch_marker_read_path_for_test(paths[0], slots, 4,
                                                     &count), 0,
                  "retained marker parses as V2");
    ASSERT_EQ_INT(count, 1, "retained marker count");

    SlotcaskUpsertOpts so;
    memset(&so, 0, sizeof(so));
    snprintf(fk, sizeof(fk), "mv2-follow-%d", a++);
    snprintf(fv, sizeof(fv), "fv");
    for (;; a++) { snprintf(fk, sizeof(fk), "mv2-follow-%d", a);
                   if (mv2_shard_of(fk) == 0) break; }
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                             fv, strlen(fv), &so, NULL), 0,
                  "golden follow-up write succeeds");
    ASSERT_EQ_INT(mv2_marker_count(w.base, paths, 4), 0, "golden cleared");
    {
        void *v = NULL; size_t vl = 0;
        ASSERT_EQ_INT(slotcask_get(&w.db, k1, strlen(k1), &v, &vl), 0,
                      "replayed record visible");
        int ok = v && vl == 4 && memcmp(v, "val1", 4) == 0;
        free(v);
        ASSERT_TRUE(ok, "replayed record content");
    }

    /* Corrupt-marker cases: retained marker mutated on disk → the
       follow-up write's gate fails closed and the marker remains.
       Case A: bad checksum — flip a bit in slot 0's new_offset WITHOUT
       recomputing the checksum. (The finalize wave already applied the kf
       mutation — only its sync failed — so a checksum-valid but
       semantically-bent slot would replay idempotently and converge; a
       checksum mismatch must fail closed at the reader.) */
    ASSERT_EQ_INT(mv2_retain_one_marker(&w, k2, "val2"), -1,
                  "second retained marker request fails");
    ASSERT_EQ_INT(mv2_marker_count(w.base, paths, 4), 1,
                  "re-scan: the new retained marker has a new exact path");
    ASSERT_EQ_INT(mv2_flip_bit_bad_checksum(paths[0]), 0,
                  "flip checksum-covered byte");
    for (;; a++) { snprintf(fk, sizeof(fk), "mv2-follow-%d", a);
                   if (mv2_shard_of(fk) == 0) break; }
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                             fv, strlen(fv), &so, NULL), -1,
                  "corrupt marker: gate fails closed");
    ASSERT_EQ_INT(mv2_marker_count(w.base, paths, 4), 1,
                  "corrupt marker retained");

    /* Case B: trailing byte → fails closed. */
    {
        FILE *f = fopen(paths[0], "ab");
        ASSERT_TRUE(f != NULL, "reopen retained marker");
        if (f) { fputc(0x41, f); fclose(f); }
    }
    for (;; a++) { snprintf(fk, sizeof(fk), "mv2-follow-%d", a);
                   if (mv2_shard_of(fk) == 0) break; }
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                             fv, strlen(fv), &so, NULL), -1,
                  "trailing-byte marker: gate fails closed");

    /* Case C: truncated → fails closed. */
    {
        FILE *f = fopen(paths[0], "r+b");
        ASSERT_TRUE(f != NULL, "reopen for truncate");
        if (f) { fseek(f, 16 + 31, SEEK_SET); ftruncate(fileno(f),
                                                        16 + 31); fclose(f); }
    }
    for (;; a++) { snprintf(fk, sizeof(fk), "mv2-follow-%d", a);
                   if (mv2_shard_of(fk) == 0) break; }
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                             fv, strlen(fv), &so, NULL), -1,
                  "truncated marker: gate fails closed");
    ASSERT_EQ_INT(mv2_marker_count(w.base, paths, 4), 1,
                  "mutated marker still retained");

    mv2_db_close(&w);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_marker_v2_run(void) {
    if (mv2_parser_cases() != 0) return 1;
    return mv2_gate_cases();
}
TEST_REGISTER("test-marker-v2", test_marker_v2_run)
