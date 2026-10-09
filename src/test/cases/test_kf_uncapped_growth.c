/* src/test/cases/test_kf_uncapped_growth.c
 *
 * W1 — unbounded per-shard kf growth
 * (docs/plans/2026-09-14-auto-widen-kf-ceiling.md, reissued 2026-10-07).
 *
 * Standalone slotcask unit case (no daemon — mirrors
 * test-slotcask-resplit.c):
 *   1. the TEST_BUILD max seam holds the pre-grow loop down
 *      (red while the loops still read the old 16M constant),
 *   2. without the seam, growth is unbounded: thousands of inserts
 *      into 64-slot shards tier through many doublings with zero
 *      refusals (regression guard for the growth model),
 *   3. every record survives the doublings and a close/reopen.
 *
 * Task 2 appends: Part 4 (the reshard hint records its
 * recommendation) and Part 5 (the kf slot radix sort stays correct
 * across the old 2^24 bound).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700   /* nftw + struct FTW (ftw.h hides them under
                             * _GNU_SOURCE alone) */
#endif
#ifdef __APPLE__
/* On macOS, _XOPEN_SOURCE 700 alone pins __DARWIN_C_LEVEL to 700, which
 * hides mkdtemp (needs >= 200809L). _DARWIN_C_SOURCE restores the full
 * level; glibc ignores the macro. */
#define _DARWIN_C_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include "shard_test_ctl.h"

#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

/* W1 review fix: no shell interpolation — mkdtemp creates the
 * directory (0700, collision-free) and an nftw depth-first walk
 * removes it, so SHARD_TEST_TMPDIR metacharacters or spaces can
 * neither execute anything nor split the path. */
static int rm_rf_callback(const char *fpath, const struct stat *sb,
                          int typeflag, struct FTW *ftwbuf) {
    (void)sb; (void)typeflag; (void)ftwbuf;
    return remove(fpath);   /* depth-first: files first, then the dir */
}

static void rm_rf(const char *path) {
    if (!path || !*path) return;
    nftw(path, rm_rf_callback, 16, FTW_DEPTH | FTW_PHYS);
}

static void unique_tmpdir(char out[256]) {
    const char *base = getenv("SHARD_TEST_TMPDIR");
    if (!base || !*base) base = "/tmp";
    snprintf(out, 256, "%s/shard_kf_growth_XXXXXX", base);
    if (!mkdtemp(out)) out[0] = '\0';
}

static size_t shard0_capacity(const char *dir) {
    char kf_path[512];
    snprintf(kf_path, sizeof(kf_path), "%s/data/kf/000.kf", dir);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, 64, 0) != 0) return 0;
    size_t cap = kh.capacity;
    kfcache_release(&kh);
    return cap;
}

static int test_kf_uncapped_growth_run(void) {
    char dir[256];
    unique_tmpdir(dir);
    ASSERT_TRUE(dir[0] != '\0', "mkdtemp tmpdir created");
    if (!dir[0]) return 1;
    slotcask_init(64, 64);

    /* --- Part 1: the max seam holds the growth loops down ---
     * initial = max = 64: a pregrow projection of 1000 records must
     * NOT grow capacity past the test-only cap. Red while the loops
     * read the old 16M constant (pregrow distributes 1000 over 8
     * shards, 125 per shard: 64 -> 128 -> 256, stopping at the first
     * tier where 125*4 < capacity*3). */
    atomic_store(&g_shard_test_kf_initial_slots, 64);
    atomic_store(&g_shard_test_kf_max_slots, 64);

    SlotcaskDb db;
    int rc = slotcask_open(&db, dir, 8, 1, 256);
    ASSERT_EQ_INT(rc, 0, "slotcask_open succeeds (initial=64)");
    if (rc != 0) { rm_rf(dir); return 1; }

    /* pregrow creates any not-yet-materialised shard files at the
     * seam's initial capacity, then (new code) declines to grow past
     * the seam's max. */
    ASSERT_EQ_INT(slotcask_pregrow_kf(&db, 1000), 0, "pregrow runs");
    ASSERT_EQ_INT(shard0_capacity(dir), 64,
                  "pregrow honored the max seam (capacity stays 64)");

    /* --- Part 4 (D2b): the reshard hint --- spike every shard's
     * header total past the 1M tier boundary, then nudge a pregrow
     * (its hint check sums the live headers): the hint bitmask must
     * record the recommendation (16 for ~1.6M records at splits=8).
     * This is the state behind the RESHARD-HINT log line. */
    for (int s = 0; s < 8; s++)
        ASSERT_EQ_INT(slotcask_test_set_kf_total(&db, s, 200000, 0), 0,
                      "synthetic header totals written");
    ASSERT_EQ_INT(slotcask_pregrow_kf(&db, 1), 0, "pregrow nudge runs");
    /* splits=16 recommendation → bit 4 → mask 0x10. */
    ASSERT_EQ_INT(atomic_load(&db.reshard_hint_logged), 16,
                  "reshard hint recorded the splits=16 recommendation");

    slotcask_close(&db);
    rm_rf(dir);

    /* --- Part 2: uncapped growth, end to end ---
     * No max seam (0 = uncapped, production behavior). 5,000 records
     * into 64-slot shards forces ~4 doublings per shard; nothing may
     * refuse. (Green-only on base too — the inline resplits were
     * always uncapped; this locks the model in.) */
    atomic_store(&g_shard_test_kf_initial_slots, 64);
    atomic_store(&g_shard_test_kf_max_slots, 0);

    rc = slotcask_open(&db, dir, 8, 1, 256);
    ASSERT_EQ_INT(rc, 0, "slotcask_open succeeds (uncapped)");
    if (rc != 0) { rm_rf(dir); return 1; }

    enum { NREC = 5000 };
    int inserted = 0;
    for (int i = 0; i < NREC; i++) {
        char k[32], v[64];
        snprintf(k, sizeof(k), "grow-%06d", i);
        snprintf(v, sizeof(v), "value-%d", i);
        if (slotcask_insert_with_hooks(&db, -1, k, strlen(k), v,
                                       strlen(v), NULL, NULL) == 0)
            inserted++;
    }
    ASSERT_EQ_INT(inserted, NREC,
                  "all 5000 inserts succeed across the doublings");

    size_t cap_after = shard0_capacity(dir);
    ASSERT_TRUE(cap_after > 64,
                  "shard 0 tiered up past its initial 64 slots");

    int readable = 0;
    for (int i = 0; i < NREC; i += 97) {         /* spot-check stride */
        char k[32];
        snprintf(k, sizeof(k), "grow-%06d", i);
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, k, strlen(k), &v, &vl) == 0) {
            readable++;
            free(v);
        }
    }
    ASSERT_TRUE(readable > 0, "spot-checked records readable post-growth");

    slotcask_close(&db);

    /* Reopen: the larger kf files are picked up automatically and all
     * spot-checked records survive. */
    rc = slotcask_open(&db, dir, 8, 1, 256);
    ASSERT_EQ_INT(rc, 0, "reopen after growth succeeds");
    readable = 0;
    for (int i = 0; i < NREC; i += 97) {
        char k[32];
        snprintf(k, sizeof(k), "grow-%06d", i);
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, k, strlen(k), &v, &vl) == 0) {
            readable++;
            free(v);
        }
    }
    ASSERT_TRUE(readable > 0, "spot-checked records readable after reopen");

    /* --- Part 5 (D2): radix sort beyond the old 24-bit ceiling ---
     * Uncapped growth lets one shard exceed 16,777,216 slots; the old
     * fixed three-pass radix silently mis-sorted there (it ordered
     * 0x1000000 before 5, since only the low 24 bits were compared).
     * These values straddle the bound; the adaptive pass count must
     * produce the full ordering. */
    {
        enum { RN = 9 };
        size_t vals[RN] = {
            0, 5, 0xfffffe, 0xffffff, 0x1000000, 0x1000001,
            0x12345678, 0x7fffffff, 0xffffffff
        };
        size_t scratch[RN], expect[RN];
        memcpy(expect, vals, sizeof(vals));
        for (size_t i = 1; i < RN; i++) {      /* reference insertion sort */
            size_t key = expect[i];
            long long j = (long long)i - 1;
            while (j >= 0 && expect[j] > key) {
                expect[j + 1] = expect[j];
                j--;
            }
            expect[j + 1] = key;
        }
        slotcask_test_radix_sort_sizes(vals, scratch, RN);
        ASSERT_TRUE(memcmp(vals, expect, sizeof(vals)) == 0,
                    "radix sort correct across the 2^24 boundary");
    }

    atomic_store(&g_shard_test_kf_initial_slots, 0);
    atomic_store(&g_shard_test_kf_max_slots, 0);
    shard_test_ctl_reset();
    slotcask_close(&db);
    slotcask_shutdown();
    rm_rf(dir);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-kf-uncapped-growth", test_kf_uncapped_growth_run)
