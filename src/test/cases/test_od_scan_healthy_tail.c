/* Regression/guards for seg_scan_o_direct + the hole-aware wide resync
 * (seg_resync_wide_fd) — docs/plans/2026-11-17-kf-grave-frontier-fix.md,
 * Task 5 (separate case). Uses Task 4a's od_test_resync_bytes_* counter.
 *
 * Sub-cases and their base expectations:
 *   (i)   healthy-tail read bound — self-calibrating guard, PASSES on
 *         base; fails a wide search that reads through holes when only
 *         ~8 KiB is allocated (mutation: force trusted = 0).
 *   (ii)  record behind a hole — FAILS on base (record not delivered).
 *   (iii) grave larger than the scan chunk (resume-past-grave) — guard,
 *         PASSES on base; proven only by mutating the resume rule to
 *         `target` (child then livelocks and dies to alarm(10)).
 *   (iv)  extent larger than buf_sz (one-slot overlap rule) — FAILS on
 *         base; mutation `bytes_read` (no overlap) misses the record.
 *   (v)   (a) grave ending exactly at EOF across a chunk boundary —
 *         coverage: delivers the live record once on base and fixed
 *         (both reach it; base via bounded skip-graves-and-end, fixed
 *         via resume == file_size). (b) Linux only: hole-punched
 *         spanning record behind a desync — FAILS on base.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include "io_direct.h"
#include "types.h"   /* compute_hash_raw */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>
#if defined(__linux__)
#include <linux/falloc.h>
#endif

#define TEST_SEG_BYTES (512u * 1024u)
#define CHUNK_MIN (1024UL * 1024UL)          /* 1 MiB minimum scan chunk */
#define REC(kl, vl) (((size_t)24 + (kl) + (vl) + 7u) & ~(size_t)7u)

/* ── raw-record crafting helpers (style of
 *    test_coverity_seg_scan_varlen_overflow.c) ─────────────────────── */

/* header = hash16 (xxh128 of key) | klen u16 | flag u8 | hdr2 u8 |
   vlen u32 (all fields native-endian, matching the engine's memcpy
   loads), then key, then value, padded to 8. */
static size_t build_rec(uint8_t *out, const char *key, uint16_t klen,
                        const void *val, uint32_t vlen, uint8_t flag) {
    size_t rs = REC(klen, vlen);
    memset(out, 0, rs);
    compute_hash_raw(key, klen, out);
    memcpy(out + 16, &klen, 2);
    out[18] = flag;
    out[19] = 0;
    memcpy(out + 20, &vlen, 4);
    memcpy(out + 24, key, klen);
    if (val && vlen) memcpy(out + 24 + klen, val, vlen);
    return rs;
}

static int pwrite_rec(int fd, off_t off, const char *key, uint16_t klen,
                      const void *val, uint32_t vlen, uint8_t flag) {
    size_t rs = REC(klen, vlen);
    uint8_t *buf = malloc(rs);
    if (!buf) return -1;
    build_rec(buf, key, klen, val, vlen, flag);
    ssize_t w = pwrite(fd, buf, rs, off);
    free(buf);
    return w == (ssize_t)rs ? 0 : -1;
}

/* ── scan callback: count deliveries per 16-byte key ───────────────── */

struct key_counter {
    const char (*keys)[17];
    size_t nkeys;
    size_t count[8];
};

static int count_cb(const uint8_t *rec, size_t vlen,
                    const uint8_t hash16[16], void *raw) {
    (void)vlen; (void)hash16;
    struct key_counter *c = (struct key_counter *)raw;
    for (size_t i = 0; i < c->nkeys && i < 8; i++) {
        if (memcmp(rec + 24, c->keys[i], 16) == 0) {
            c->count[i]++;
            break;
        }
    }
    return 0;
}

/* Fresh scratch dir helper (rm -rf before and after is the caller's job). */
static void fresh_dir(char *dir, size_t cap, const char *tag) {
    snprintf(dir, cap, "/tmp/shard-db-od-healthy-tail-%s-%d", tag,
             (int)getpid());
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", dir);
    system(cmd);
    mkdir(dir, 0755);
}

static void rm_path(const char *p) {
    char cmd[600];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", p);
    system(cmd);
}

/* ── (i) healthy-tail read bound (self-calibrating; PASSES on base) ── */

static void case_i_bound(void) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), "i");
    slotcask_test_set_seg_max_bytes(TEST_SEG_BYTES);
    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    if (slotcask_open(&db, dir, 8, 1, 8192) != 0) {
        ASSERT_TRUE(0, "(i) open");
        slotcask_test_set_seg_max_bytes(0);
        rm_path(dir);
        return;
    }
    char v[100];
    memset(v, 'a', sizeof(v));
    static const char keys[3][17] = {
        "od-i-a-01234567", "od-i-b-01234567", "od-i-c-01234567"
    };
    for (size_t k = 0; k < 3; k++)
        ASSERT_EQ_INT(slotcask_insert_with_hooks(&db, 0, keys[k], 16, v,
                                      sizeof(v), NULL, NULL), 0,
                      "(i) insert small live record");
    slotcask_close(&db);

    char path[512];
    snprintf(path, sizeof(path), "%s/data/streams/000/000000.dat", dir);
    struct stat st;
    ASSERT_EQ_INT(stat(path, &st), 0, "(i) stat segment");
    off_t allocated = (off_t)st.st_blocks * 512;

    int pfd = open(path, O_RDONLY);
    ASSERT_TRUE(pfd >= 0, "(i) open segment read-only");
    int trusted = pfd >= 0 ? od_seek_data_trusted(pfd, path) : 0;

    struct key_counter kc;
    memset(&kc, 0, sizeof(kc));
    kc.keys = keys;
    kc.nkeys = 3;
    od_test_resync_bytes_reset();
    int rc = seg_scan_o_direct(path, 8192, count_cb, &kc);
    ASSERT_EQ_INT(rc, 0, "(i) scan succeeds on a healthy tail");
    for (size_t k = 0; k < 3; k++)
        ASSERT_EQ_INT((int)kc.count[k], 1, "(i) each record delivered exactly once");

    if (trusted) {
        uint64_t bytes = od_test_resync_bytes_read();
        /* Bound: the resync search may read the data extent (<= allocated
           here) plus up to 4 slot-size pads of slop. Fails a wide search
           that reads through holes when only ~8 KiB is allocated. */
        ASSERT_TRUE(bytes <= (uint64_t)allocated + 4ull * 8192ull,
                    "(i) resync read bound (allocated + 4 slots)");
        if (pfd >= 0) close(pfd);
    } else {
        if (pfd >= 0) close(pfd);
        TAP_DIAG("# SKIP (i) read bound: SEEK_DATA not trusted here\n");
    }

    slotcask_test_set_seg_max_bytes(0);
    rm_path(dir);
}

/* ── (ii) record behind a hole (FAILS on base) ──────────────────────── */

static void case_ii_behind_hole(void) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), "ii");
    slotcask_test_set_seg_max_bytes(TEST_SEG_BYTES);
    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    if (slotcask_open(&db, dir, 8, 1, 8192) != 0) {
        ASSERT_TRUE(0, "(ii) open");
        slotcask_test_set_seg_max_bytes(0);
        rm_path(dir);
        return;
    }
    static const char keys[2][17] = { "od-ii-a-01234567", "od-ii-b-01234567" };
    char va[10], vb[20];
    memset(va, 'A', sizeof(va));
    memset(vb, 'B', sizeof(vb));
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db, 0, keys[0], 16, va,
                                  sizeof(va), NULL, NULL), 0, "(ii) insert A");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db, 0, keys[1], 16, vb,
                                  sizeof(vb), NULL, NULL), 0, "(ii) insert B");
    slotcask_close(&db);

    /* A lands at 0 (rec 56), B at 56 (rec 64). Copy B's record (valid
       hash) to 256 KiB: 8-aligned, well past any bounded window, behind
       a hole larger than 64 KiB. */
    char path[512];
    snprintf(path, sizeof(path), "%s/data/streams/000/000000.dat", dir);
    const off_t ISLAND = (off_t)256 * 1024;
    {
        int fd = open(path, O_RDWR);
        ASSERT_TRUE(fd >= 0, "(ii) open segment");
        if (fd >= 0) {
            uint8_t rec[REC(16, 20)];
            ASSERT_EQ_INT((int)pread(fd, rec, sizeof(rec), REC(16, 10)),
                          (int)sizeof(rec), "(ii) read B's record bytes");
            ASSERT_EQ_INT((int)pwrite(fd, rec, sizeof(rec), ISLAND),
                          (int)sizeof(rec), "(ii) pwrite island at 256 KiB");
            close(fd);
        }
    }

    struct key_counter kc;
    memset(&kc, 0, sizeof(kc));
    kc.keys = keys;
    kc.nkeys = 2;
    int rc = seg_scan_o_direct(path, 8192, count_cb, &kc);
    ASSERT_EQ_INT(rc, 0, "(ii) scan succeeds");
    ASSERT_EQ_INT((int)kc.count[0], 1, "(ii) A delivered once");
    ASSERT_EQ_INT((int)kc.count[1], 2,
                  "(ii) B delivered twice (original + island copy)");

    slotcask_test_set_seg_max_bytes(0);
    rm_path(dir);
}

/* ── (iii) grave larger than the scan chunk (livelock proof; PASSES
 *    on base) ────────────────────────────────────────────────────────── */

static int case_iii_child(const char *path) {
    struct key_counter kc;
    memset(&kc, 0, sizeof(kc));
    static const char live[17] = "od-iii-l-0123456";
    kc.keys = &live;
    kc.nkeys = 1;
    alarm(10);
    int rc = seg_scan_o_direct(path, 2u * 1024 * 1024, count_cb, &kc);
    _exit(rc == 0 && kc.count[0] == 1 ? 0 : 1);
}

static void case_iii_big_grave(void) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), "iii");
    char path[512];
    snprintf(path, sizeof(path), "%s/000000.dat", dir);

    /* One hash-valid flag=0 content grave of 1.5 MiB (larger than the
       1 MiB chunk), then a live record after it. slot_size 2 MiB keeps
       the grave struct-ok. */
    const size_t GRAVE_REC = 1536UL * 1024;              /* 1.5 MiB, 8-aligned */
    const uint32_t G_VLEN = (uint32_t)(GRAVE_REC - REC(16, 0)); /* 1536K-40 */
    int fd = open(path, O_RDWR | O_CREAT, 0600);
    ASSERT_TRUE(fd >= 0, "(iii) open crafted file");
    if (fd < 0) { rm_path(dir); return; }
    ASSERT_EQ_INT(pwrite_rec(fd, 0, "od-iii-g-0123456", 16, NULL, G_VLEN, 0),
                  0, "(iii) write 1.5 MiB grave");
    ASSERT_EQ_INT(pwrite_rec(fd, (off_t)GRAVE_REC, "od-iii-l-0123456", 16,
                             NULL, 100, 1),
                  0, "(iii) write live record after the grave");
    close(fd);

    size_t old_buf = atomic_load(&odirect_buf_size);
    atomic_store(&odirect_buf_size, CHUNK_MIN);

    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) case_iii_child(path);   /* alarm(10) inside */

    int wst = 0;
    ASSERT_EQ_INT((int)waitpid(pid, &wst, 0), (int)pid, "(iii) waitpid");
    if (WIFSIGNALED(wst)) {
        ASSERT_TRUE(0, "(iii) scan child killed by signal (livelock suspected)");
    } else {
        ASSERT_EQ_INT(WEXITSTATUS(wst), 0,
                      "(iii) scan completes, live record delivered exactly once");
    }

    atomic_store(&odirect_buf_size, old_buf);
    rm_path(dir);
}

/* ── (iv) extent larger than buf_sz (overlap rule; FAILS on base) ───── */

static void case_iv_big_extent(void) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), "iv");
    char path[512];
    snprintf(path, sizeof(path), "%s/000000.dat", dir);

    /* Live record at 0 (desync at 144), then a real 3 MiB pwrite of
       ZEROS (one data extent), then a hash-valid live record straddling
       the wide-search buffer end:
         off0   = (desync + max_slot_size) rounded to 8 = 144 + 8192
         buf_sz = max(2 * max_slot_size, 1 MiB) = 1 MiB
         rec at off0 + buf_sz - 64. */
    const size_t MAX_SLOT = 8192;
    const off_t DESYNC = REC(16, 100);                     /* 144 */
    const off_t OFF0 = ((DESYNC + (off_t)MAX_SLOT) + 7) & ~(off_t)7;
    const size_t BUF_SZ = (2 * MAX_SLOT > CHUNK_MIN) ? 2 * MAX_SLOT : CHUNK_MIN;
    const off_t REC1_OFF = OFF0 + (off_t)BUF_SZ - 64;
    const off_t ZERO_END = OFF0 + (off_t)3 * 1024 * 1024;

    int fd = open(path, O_RDWR | O_CREAT, 0600);
    ASSERT_TRUE(fd >= 0, "(iv) open crafted file");
    if (fd < 0) { rm_path(dir); return; }
    ASSERT_EQ_INT(pwrite_rec(fd, 0, "od-iv-a-01234567", 16, NULL, 100, 1),
                  0, "(iv) write first live record");
    {
        char *zeros = calloc(1, 1024 * 1024);
        ASSERT_NOT_NULL(zeros, "(iv) alloc zero block");
        if (zeros) {
            off_t w = OFF0;
            while (w < ZERO_END) {
                size_t chunk = ZERO_END - w > (off_t)(1024 * 1024)
                                   ? 1024 * 1024 : (size_t)(ZERO_END - w);
                ASSERT_EQ_INT((int)pwrite(fd, zeros, chunk, w), (int)chunk,
                              "(iv) write zero extent");
                w += (off_t)chunk;
            }
            free(zeros);
        }
    }
    /* 8000-byte zero value: rec 8040 <= 8192, and a header never
       misparses inside the value region (zeros parse as zero-fill). */
    {
        char zv[8000];
        memset(zv, 0, sizeof(zv));
        ASSERT_EQ_INT(pwrite_rec(fd, REC1_OFF, "od-iv-b-01234567", 16, zv,
                                 sizeof(zv), 1),
                      0, "(iv) write straddling live record");
    }
    close(fd);

    struct key_counter kc;
    memset(&kc, 0, sizeof(kc));
    static const char keys[2][17] = { "od-iv-a-01234567", "od-iv-b-01234567" };
    kc.keys = keys;
    kc.nkeys = 2;
    int rc = seg_scan_o_direct(path, MAX_SLOT, count_cb, &kc);
    ASSERT_EQ_INT(rc, 0, "(iv) scan succeeds (zero-fill desync is a tail)");
    ASSERT_EQ_INT((int)kc.count[0], 1, "(iv) first record delivered once");
    ASSERT_EQ_INT((int)kc.count[1], 1,
                  "(iv) record straddling the wide-search buffer end delivered");

    rm_path(dir);
}

/* ── (v)(a) grave ending exactly at EOF across a chunk boundary ─────── */

static void case_va_grave_at_eof(void) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), "va");
    char path[512];
    snprintf(path, sizeof(path), "%s/000000.dat", dir);

    /* 2 MiB file, chunk 1 MiB. Live record at 0; the last record is a
       hash-valid grave whose HEADER sits 16 bytes before the first
       chunk boundary and which ends exactly at file_size. */
    const off_t FILE_SIZE = (off_t)2 * 1024 * 1024;
    const off_t GRAVE_OFF = (off_t)1024 * 1024 - 16;
    const uint32_t G_VLEN = (uint32_t)((FILE_SIZE - GRAVE_OFF) - REC(16, 0));
    int fd = open(path, O_RDWR | O_CREAT, 0600);
    ASSERT_TRUE(fd >= 0, "(v)(a) open crafted file");
    if (fd < 0) { rm_path(dir); return; }
    ASSERT_EQ_INT(pwrite_rec(fd, 0, "od-va-l-0123456", 16, NULL, 100, 1),
                  0, "(v)(a) write live record");
    ASSERT_EQ_INT(ftruncate(fd, FILE_SIZE), 0, "(v)(a) extend file to 2 MiB");
    ASSERT_EQ_INT(pwrite_rec(fd, GRAVE_OFF, "od-va-g-0123456", 16, NULL,
                             G_VLEN, 0),
                  0, "(v)(a) write grave header at chunk end");
    close(fd);

    size_t old_buf = atomic_load(&odirect_buf_size);
    atomic_store(&odirect_buf_size, CHUNK_MIN);

    struct key_counter kc;
    memset(&kc, 0, sizeof(kc));
    static const char live[17] = "od-va-l-0123456";
    kc.keys = &live;
    kc.nkeys = 1;
    int rc = seg_scan_o_direct(path, 2u * 1024 * 1024, count_cb, &kc);
    ASSERT_EQ_INT(rc, 0, "(v)(a) scan ends cleanly (resume == file_size)");
    ASSERT_EQ_INT((int)kc.count[0], 1,
                  "(v)(a) live record delivered exactly once");

    atomic_store(&odirect_buf_size, old_buf);
    rm_path(dir);
}

/* ── (v)(b) hole-punched spanning record behind a desync (Linux only;
 *    FAILS on base) ──────────────────────────────────────────────────── */

static void case_vb_spanning_hole(void) {
#if defined(__linux__)
    char dir[256];
    fresh_dir(dir, sizeof(dir), "vb");
    char path[512];
    snprintf(path, sizeof(path), "%s/000000.dat", dir);

    /* Live record at 0 (delivered before the desync), then the
       scenario-9-style spanning record (key 16, 16000-byte value, zero
       middle pages punched out) placed > 2 MiB past the desync so base's
       bounded window cannot reach it: base fails with the record not
       delivered; the fixed scan delivers it (max_slot_size zero padding
       makes struct_ok pass across the hole). */
    const off_t R_OFF = (off_t)4 * 1024 * 1024;
    int fd = open(path, O_RDWR | O_CREAT, 0600);
    ASSERT_TRUE(fd >= 0, "(v)(b) open crafted file");
    if (fd < 0) { rm_path(dir); return; }
    ASSERT_EQ_INT(pwrite_rec(fd, 0, "od-vb-l-0123456", 16, NULL, 100, 1),
                  0, "(v)(b) write live record before the desync");
    {
        char *val = malloc(16000);
        ASSERT_NOT_NULL(val, "(v)(b) alloc island value");
        if (!val) { close(fd); rm_path(dir); return; }
        memset(val, 0, 16000);
        memset(val, 'R', 64);
        memset(val + 16000 - 64, 'r', 64);
        int rc = pwrite_rec(fd, R_OFF, "od-vb-r-0123456", 16, val, 16000, 1);
        free(val);
        ASSERT_EQ_INT(rc, 0, "(v)(b) write spanning record at 4 MiB");
    }
    int frc = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                        R_OFF + 4096, 8192);
    if (frc != 0 &&
        (errno == EOPNOTSUPP || errno == ENOTTY || errno == EINVAL)) {
        TAP_DIAG("# SKIP (v)(b): fallocate(PUNCH_HOLE) unsupported (%s)\n",
                 strerror(errno));
        close(fd);
        rm_path(dir);
        return;
    }
    ASSERT_EQ_INT(frc, 0, "(v)(b) punch hole inside the spanning record");
    close(fd);

    size_t old_buf = atomic_load(&odirect_buf_size);
    atomic_store(&odirect_buf_size, CHUNK_MIN);

    struct key_counter kc;
    memset(&kc, 0, sizeof(kc));
    static const char keys[2][17] = { "od-vb-l-0123456", "od-vb-r-0123456" };
    kc.keys = keys;
    kc.nkeys = 2;
    int rc = seg_scan_o_direct(path, 16384, count_cb, &kc);
    ASSERT_EQ_INT(rc, 0, "(v)(b) scan succeeds");
    ASSERT_EQ_INT((int)kc.count[0], 1, "(v)(b) pre-desync record delivered once");
    ASSERT_EQ_INT((int)kc.count[1], 1,
                  "(v)(b) hole-punched spanning record delivered");

    atomic_store(&odirect_buf_size, old_buf);
    rm_path(dir);
#else
    TAP_DIAG("# SKIP (v)(b): requires Linux fallocate(PUNCH_HOLE)\n");
#endif
}

/* ── (vi) pread I/O error in the wide search fails closed, no double free ── */

static void case_vi_wide_io_error(void) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), "vi");
    char path[512];
    snprintf(path, sizeof(path), "%s/000000.dat", dir);

    int fd = open(path, O_RDWR | O_CREAT, 0600);
    ASSERT_TRUE(fd >= 0, "(vi) open file");
    if (fd < 0) { rm_path(dir); return; }
    {
        /* Dense zeros: a real data extent so the search issues a pread
           (an all-hole file would answer ENXIO without reading). */
        char *z = calloc(1, 1024 * 1024);
        ASSERT_NOT_NULL(z, "(vi) alloc");
        if (z) {
            ASSERT_EQ_INT((int)pwrite(fd, z, 1024 * 1024, 0), 1024 * 1024,
                          "(vi) write dense zeros");
            free(z);
        }
    }
    close(fd);

    /* Write-only fd: every pread fails with EBADF (non-EINTR). Before the
       fix the error path freed the buffer twice (glibc abort). */
    int wfd = open(path, O_WRONLY);
    ASSERT_TRUE(wfd >= 0, "(vi) open write-only");
    if (wfd >= 0) {
        off_t out = 0;
        errno = 0;
        int rc = seg_resync_wide_fd(wfd, path, 1024 * 1024, 8192, 144, &out);
        ASSERT_EQ_INT(rc, -1, "(vi) pread error returns -1 (no crash)");
        ASSERT_EQ_INT(errno, EBADF, "(vi) errno preserved");
        close(wfd);
    }
    rm_path(dir);
}

/* ── (vii) a definitive untrusted SEEK_DATA verdict is cached ────────── */

static void case_vii_untrusted_cached(void) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), "vii");
    char path[512];
    snprintf(path, sizeof(path), "%s/000000.dat", dir);
    int fd = open(path, O_RDWR | O_CREAT, 0600);
    ASSERT_TRUE(fd >= 0, "(vii) open file");
    if (fd < 0) { rm_path(dir); return; }

    od_test_probe_cache_reset();
    od_test_force_probe_untrusted(1);
    int a = od_seek_data_trusted(fd, path);
    int b = od_seek_data_trusted(fd, path);
    od_test_force_probe_untrusted(0);
    ASSERT_EQ_INT(a, 0, "(vii) forced verdict is untrusted");
    ASSERT_EQ_INT(b, 0, "(vii) cached verdict stays untrusted");
    ASSERT_EQ_INT((int)od_test_probe_count(), 1,
                  "(vii) untrusted verdict probed once, then cached");
    od_test_probe_cache_reset();   /* do not leak the fake verdict */
    close(fd);
    rm_path(dir);
}

static int test_od_scan_healthy_tail_run(void) {
    slotcask_init(64, 64);
    case_i_bound();
    case_ii_behind_hole();
    case_iii_big_grave();
    case_iv_big_extent();
    case_va_grave_at_eof();
    case_vb_spanning_hole();
    case_vi_wide_io_error();
    case_vii_untrusted_cached();
    slotcask_test_set_seg_max_bytes(0);
    slotcask_shutdown();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-od-scan-healthy-tail", test_od_scan_healthy_tail_run)
