/* src/test/audit/audit_repro_resplit_msync.c
 *
 * Audit repro for AUD-KF-RESPLIT-SYNC (docs/plans/2026-11-25-audit-storage-
 * k-reentry.md, Phase 2 area 1 part 2). kfcache_resplit_locked ignores the
 * result of the Phase C msync(MS_SYNC) of the rebuilt keyfile and renames it
 * over the live one anyway. This case makes exactly that msync fail (EIO)
 * and asserts the contract every other sync in the engine follows: the
 * resplit is abandoned with the old keyfile still in place.
 *
 * Expected today: FAILS (the keyfile grows despite the failed sync). It
 * passes once the Phase 4 fix lands. Audit case: excluded from default
 * run-all (TEST_REGISTER_AUDIT); run with --include-audit.
 *
 * Mechanism: a test-binary msync() interposer (executable symbols win over
 * libc) that fails the first msync of a rebuilt keyfile. The resplit's
 * msync is identified by shape: MS_SYNC, length over two pages and not a
 * page multiple (per-slot syncs are page-aligned multiples). Skipped under
 * ASan/TSan, whose runtimes own msync.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "fixtures.h"
#include "shard_test_ctl.h"

#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

#if !defined(__SANITIZE_ADDRESS__) && !defined(__SANITIZE_THREAD__)
static _Atomic int g_rs_arm;    /* fail the next big msync */
static _Atomic int g_rs_hits;   /* big msyncs failed */

int msync(void *addr, size_t len, int flags) {
    static int (*real)(void *, size_t, int);
    if (!real) real = (int (*)(void *, size_t, int))dlsym(RTLD_NEXT, "msync");
    if ((flags & MS_SYNC) && len > 8192 && (len % 4096) != 0 &&
        atomic_load(&g_rs_arm) &&
        atomic_exchange(&g_rs_arm, 0)) {
        atomic_fetch_add(&g_rs_hits, 1);
        errno = EIO;
        return -1;
    }
    return real(addr, len, flags);
}

static int rs_upsert(SlotcaskDb *db, uint64_t key) {
    char v[8] = "resplit";
    SlotcaskBulkRec rec;
    memset(&rec, 0, sizeof(rec));
    rec.key = &key; rec.klen = sizeof(key);
    rec.value = v;  rec.vlen = 7;
    SlotcaskBulkOpts o;
    memset(&o, 0, sizeof(o));
    return slotcask_bulk_upsert_in_kfshard(db, 0, &rec, 1, &o);
}

static off_t rs_size(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 ? st.st_size : -1;
}

static int audit_repro_resplit_msync_run(void) {
    SlotcaskDb db;
    char base[] = "/tmp/shard-db-rs-test-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", base);
    snprintf(k, sizeof(k), "%s/data/kf", base);
    ASSERT_TRUE(mkdir(d, 0755) == 0 && mkdir(k, 0755) == 0, "mkdir");
    slotcask_init(64, 64);
    shard_test_ctl_reset();
    atomic_store(&g_shard_test_kf_initial_slots, 256);
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, base, 1, 1, 64), 0, "open db");
    if (t_ctx->failed) goto out;

    char kf[PATH_MAX];
    kf_path_for(kf, base, 0);
    atomic_store(&g_rs_hits, 0);
    atomic_store(&g_rs_arm, 1);
    off_t before = rs_size(kf);
    int inserted = 0;
    for (uint64_t key = 1; key <= 600 && atomic_load(&g_rs_hits) == 0; key++) {
        if (rs_upsert(&db, key) == 0) inserted++;
    }
    atomic_store(&g_rs_arm, 0);
    off_t after = rs_size(kf);

    ASSERT_EQ_INT(atomic_load(&g_rs_hits), 1,
                  "the rebuilt keyfile's msync was reached and failed once");
    char msg[200];
    snprintf(msg, sizeof(msg),
             "resplit abandoned after a failed keyfile msync (size before=%lld "
             "after=%lld, %d records inserted)",
             (long long)before, (long long)after, inserted);
    ASSERT_TRUE(after == before, msg);
out:
    slotcask_close(&db);
    {
        char cmd[PATH_MAX + 16];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
        if (system(cmd) != 0) { /* best effort */ }
    }
    slotcask_shutdown();
    shard_test_ctl_reset();
    return t_ctx->failed > 0 ? 1 : 0;
}
#else
static int audit_repro_resplit_msync_run(void) {
    /* Sanitizer runtimes own msync; this repro runs in the normal build. */
    return 0;
}
#endif

TEST_REGISTER_AUDIT("audit-repro-resplit-msync", audit_repro_resplit_msync_run)
