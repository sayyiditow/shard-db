#include "types.h"
#include "slotcask.h"
#include "trigram.h"
#include "bitmap.h"
#include "io_direct.h"   /* od_seek_data_trusted — sparse copy gate (Task 11) */
#include <dirent.h>
#ifdef TEST_BUILD
#include "shard_test_ctl.h"
#endif

/* ========== SIZE ========== */

static void dir_du(const char *path, int64_t *acc) {
    DIR *d = opendir(path);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.' &&
            (e->d_name[1] == '\0' ||
             (e->d_name[1] == '.' && e->d_name[2] == '\0'))) continue;
        char sub[PATH_MAX];
        snprintf(sub, sizeof(sub), "%s/%s", path, e->d_name);
        struct stat st;
        if (lstat(sub, &st) != 0) continue;
        if (S_ISREG(st.st_mode))
            *acc += (int64_t)st.st_blocks * 512;
        else if (S_ISDIR(st.st_mode))
            dir_du(sub, acc);
    }
    closedir(d);
}

int cmd_size(const char *db_root, const char *object) {
    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);
    int64_t bytes = 0;
    dir_du(obj_dir, &bytes);
    OUT("%lld\n", (long long)bytes);
    return 0;
}

int cmd_orphaned(const char *db_root, const char *object) {
    OUT("%d\n", get_deleted_count(db_root, object));
    return 0;
}

/* ========== ESTIMATE-INDEX ==========
 * Sample N records, project on-disk index size for a hypothetical
 * trigram index on the given field. Lets operators budget honestly
 * before committing to `add-index foo:trigram` — see
 * [[trigram-impl-map]] phase 5. Wire: spec like "body:trigram". */

#define TG_ESTIMATE_SAMPLE 1024     /* sample size; trades accuracy vs ms cost */
#define TG_BYTES_PER_ENTRY 20       /* btree leaf cost: 16B hash + ~4B encoded key */

typedef struct {
    int                field_index;
    TypedSchema       *ts;
    size_t             sampled;
    size_t             distinct_sum;     /* Σ distinct trigrams per record */
    size_t             max_sample;
} TgEstimateCtx;

static int tg_estimate_cb(uint32_t slot, const uint8_t hash16[16],
                          const void *key, size_t klen,
                          const void *value, size_t vlen,
                          uint8_t hdr2, void *ctx) {
    (void)slot; (void)hash16; (void)key; (void)klen;
    TgEstimateCtx *c = (TgEstimateCtx *)ctx;
    if (c->sampled >= c->max_sample) return -1;
    const TypedField *f = &c->ts->fields[c->field_index];
    const uint8_t *vbase = ((size_t)f->offset + (size_t)f->size > vlen)
        ? g_zero_field_65537 : (const uint8_t *)value + f->offset;
    uint16_t actual_len = ((uint16_t)vbase[0] << 8) | (uint16_t)vbase[1];
    /* actual_len is an on-disk length prefix; clamp it to the field's
       actual declared content size before reading past vbase + 2
       (CID 1696427), same pattern as mf_append_field in index.c. */
    size_t max_content = f->size > 2 ? (size_t)f->size - 2 : 0;
    if ((size_t)actual_len > max_content) actual_len = (uint16_t)max_content;
    if (actual_len > 0) {
        uint8_t trigrams[TG_MAX_DISTINCT][3];
        size_t n = tg_extract_distinct(vbase + 2, actual_len,
                                       trigrams, TG_MAX_DISTINCT);
        c->distinct_sum += n;
    }
    c->sampled++;
    return 0;
}

int cmd_estimate_index(const char *db_root, const char *object,
                        const char *spec) {
    /* Parse `<field>:<type>`. Only `trigram` is supported in this
       release — `btree` and `bitmap` would need their own projection
       formulas; the on-disk size models differ. */
    if (!spec || !*spec) {
        OUT("{\"error\":\"missing spec; expected <field>:trigram\"}\n");
        return 1;
    }
    const char *colon = strrchr(spec, ':');
    if (!colon || strcmp(colon + 1, "trigram") != 0) {
        OUT("{\"error\":\"only :trigram supported (got '%s')\"}\n", spec);
        return 1;
    }
    char field[256] = {0};
    size_t flen = (size_t)(colon - spec);
    if (flen == 0 || flen >= sizeof(field)) {
        OUT("{\"error\":\"invalid field name in spec\"}\n");
        return 1;
    }
    memcpy(field, spec, flen);

    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"object has no typed schema\"}\n");
        return 1;
    }
    int fi = typed_field_index(ts, field);
    if (fi < 0) {
        OUT("{\"error\":\"field '%s' not in object\"}\n", field);
        return 1;
    }
    if (ts->fields[fi].type != FT_VARCHAR) {
        OUT("{\"error\":\"trigram requires varchar; field '%s' is type %d\"}\n",
            field, ts->fields[fi].type);
        return 1;
    }

    Schema sch = load_schema(db_root, object);
    int live = get_live_count(db_root, object);
    if (live <= 0) {
        OUT("{\"records\":0,\"sample_size\":0,\"avg_distinct_trigrams\":0,"
            "\"estimated_entries\":0,\"estimated_disk_bytes\":0}\n");
        return 0;
    }

    SlotcaskSchemaInfo info = {
        .splits = sch.splits, .slot_size = sch.slot_size, .streams = sch.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        OUT("{\"error\":\"slotcask open failed\"}\n");
        return 1;
    }

    /* Walk shards in order, accumulating up to TG_ESTIMATE_SAMPLE
       records. Stops early once the cap is hit. Each shard's kf handle is
       acquired locally: this sampler never touches a bitmap, so there is
       no cross-cache order to establish, only the walker's now-required
       pre-acquired handle. */
    TgEstimateCtx c = {
        .field_index = fi, .ts = ts,
        .sampled = 0, .distinct_sum = 0, .max_sample = TG_ESTIMATE_SAMPLE,
    };
    for (int s = 0; s < sch.splits && c.sampled < c.max_sample; s++) {
        char kf_path[PATH_MAX];
        slotcask_kf_path(kf_path, sizeof(kf_path), sdb->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, sdb->slots_per_shard, 0) != 0)
            continue;
        slotcask_walk_one_shard_slots_locked(sdb, s, &kh, tg_estimate_cb, &c);
        kfcache_release(&kh);
    }

    double avg_distinct = c.sampled > 0
        ? (double)c.distinct_sum / (double)c.sampled : 0.0;
    long long est_entries = (long long)((double)live * avg_distinct);
    long long est_disk = est_entries * TG_BYTES_PER_ENTRY;

    OUT("{\"records\":%d,\"sample_size\":%zu,"
        "\"avg_distinct_trigrams\":%.1f,"
        "\"estimated_entries\":%lld,"
        "\"estimated_disk_bytes\":%lld}\n",
        live, c.sampled, avg_distinct, est_entries, est_disk);
    return 0;
}
int cmd_vacuum(const char *db_root, const char *object,
               int compact, int new_splits) {
    Schema sch = load_schema(db_root, object);
    if (!sch.splits) {
        OUT("{\"error\":\"object not found\"}\n");
        return 1;
    }

    /* v2 streams-mismatch detection: if the host's CPU count has changed
       since create-object (e.g. machine upgraded, container resized, or
       schema was hand-edited), the on-disk stream count diverges from
       slotcask_streams_for_nproc(). The fix is a full rebuild — same
       infrastructure as a splits change. The check folds into both flag
       paths so a single `vacuum` invocation always converges streams. */
    int new_streams = 0;
    int derived = slotcask_streams_for_nproc();
    if (derived != sch.streams && derived > 0)
        new_streams = derived;

    /* Heavy path: --compact, --splits, or streams mismatch — all converge
       through rebuild_object. */
    if (compact || new_splits > 0 || new_streams > 0) {
        return rebuild_object(db_root, object, new_splits, compact,
                               NULL, 0, new_streams);
    }

    /* v2 light path: streams already match, no flags. The snake-game pool
       reuses tombstoned slots inline for new writes, but a delete-heavy /
       no-write workload accumulates sparse non-active seg files. Direction-C
       compaction pair-merges those: live records of the sparsest non-active
       seg get migrated into a denser non-active seg's tombstone holes, then
       the now-empty donor file is unlinked. The active seg is never touched
       so concurrent appends after vacuum return are unaffected. */
    SlotcaskSchemaInfo info = {
        .splits = sch.splits, .slot_size = sch.slot_size,
        .streams = sch.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        OUT("{\"error\":\"object not open\"}\n");
        return 1;
    }
    int dropped = 0, skipped = 0;
    if (slotcask_compact_segs_ex(sdb, &dropped, &skipped) != 0) {
        OUT("{\"error\":\"segment compaction failed\"}\n");
        return 1;
    }
    /* kf-compact: rebuild each shard to drop tombstones so kf->deleted
       returns to 0 (kf-derived counts model — there's no separate
       counts file to lie via anymore). */
    int kf_skipped = 0;
    if (slotcask_compact_kf(sdb, &kf_skipped) != 0) {
        OUT("{\"error\":\"keyfile compaction failed\"}\n");
        return 1;
    }
    reset_deleted_count(db_root, object);  /* v1 only; no-op for v2 */
    if (skipped || kf_skipped)
        OUT("{\"status\":\"vacuumed\",\"cleaned\":%d%s%s}\n", dropped,
            skipped ? ",\"segments_skipped\":\"pending-markers\"" : "",
            kf_skipped ? ",\"kf_skipped\":\"pending-markers\"" : "");
    else
        OUT("{\"status\":\"vacuumed\",\"cleaned\":%d}\n", dropped);
    return 0;
}
int reshard_target_for_count(long long live) {
    if (live >= 5000000000LL)  return 4096;  /* 5B+ (ceiling, no further auto action) */
    if (live >= 1000000000LL)  return 2048;  /* 1B-5B */
    if (live >= 200000000LL)   return 1024;  /* 200M-1B */
    if (live >= 50000000LL)    return 256;   /* 50M-200M */
    if (live >= 10000000LL)    return 64;    /* 10M-50M */
    if (live >= 1000000LL)     return 16;    /* 1M-10M */
    return 8;                                 /* up to 1M */
}
/* ========== SEQUENCES ========== */

/* Atomic counter per object, stored in $DB_ROOT/<object>/metadata/sequences/<name> */
int cmd_sequence(const char *db_root, const char *object,
                        const char *seq_name, const char *action, int batch_size) {
    char seq_dir[PATH_MAX], seq_path[PATH_MAX], lock_path[PATH_MAX];
    snprintf(seq_dir, sizeof(seq_dir), "%s/%s/metadata/sequences", db_root, object);
    mkdirp(seq_dir);
    snprintf(seq_path, sizeof(seq_path), "%s/%s", seq_dir, seq_name);
    snprintf(lock_path, sizeof(lock_path), "%s/%s.lock", seq_dir, seq_name);

    if (strcmp(action, "current") == 0) {
        long long val = 0;
        FILE *f = fopen(seq_path, "r");
        if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
        OUT("{\"sequence\":\"%s\",\"value\":%lld}\n", seq_name, val);
        return 0;
    }

    if (strcmp(action, "reset") == 0) {
        if (seq_state_reset(db_root, object, seq_name) != 0) {
            OUT("{\"error\":\"sequence reset failed: %s\"}\n", strerror(errno));
            return 1;
        }
        OUT("{\"sequence\":\"%s\",\"value\":0}\n", seq_name);
        return 0;
    }

    if (strcmp(action, "next") == 0 || strcmp(action, "next-batch") == 0) {
        int lockfd = open(lock_path, O_RDWR | O_CREAT, 0644);
        if (lockfd < 0) {
            OUT("{\"error\":\"sequence: open(%s) failed: %s\"}\n", lock_path, strerror(errno));
            return 1;
        }
        flock(lockfd, LOCK_EX);

        long long val = 0;
        FILE *f = fopen(seq_path, "r");
        if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }

        if (batch_size <= 1) {
            val++;
            f = fopen(seq_path, "w");
            if (!f) {
                flock(lockfd, LOCK_UN); close(lockfd);
                OUT("{\"error\":\"sequence: fopen(%s) failed: %s\"}\n", seq_path, strerror(errno));
                return 1;
            }
            fprintf(f, "%lld\n", val);
            fclose(f);
            flock(lockfd, LOCK_UN);
            close(lockfd);
            OUT("{\"sequence\":\"%s\",\"value\":%lld}\n", seq_name, val);
        } else {
            long long start = val + 1;
            val += batch_size;
            f = fopen(seq_path, "w");
            if (!f) {
                flock(lockfd, LOCK_UN); close(lockfd);
                OUT("{\"error\":\"sequence: fopen(%s) failed: %s\"}\n", seq_path, strerror(errno));
                return 1;
            }
            fprintf(f, "%lld\n", val);
            fclose(f);
            flock(lockfd, LOCK_UN);
            close(lockfd);
            OUT("{\"sequence\":\"%s\",\"start\":%lld,\"end\":%lld,\"count\":%d}\n",
                   seq_name, start, val, batch_size);
        }
        return 0;
    }

    OUT("{\"error\":\"Unknown sequence action: %s\"}\n", action);
    return 1;
}

/* ========== BACKUP ========== */

/* Copy one regular file. Preserves mode bits. Sparse-preserving
   (2026.11.2, docs/plans/2026-11-17-kf-grave-frontier-fix.md Task 11):
   the copy walks the source with lseek(SEEK_DATA/SEEK_HOLE) and copies
   only data extents, finishing with ftruncate — a dense copy made every
   restored segment fully allocated, so the hole-aware wide resync then
   pays a full-segment scan per stream on open (~155 ms per 8-stream
   object vs ~4 ms sparse). Regression case: test-backup-copy-sparse.
   Returns 0 on success. */

/* Copy every data extent of src into dst at the same offsets (both fds
   open). EINTR-safe pread/pwrite loops throughout. Returns 0 on
   success, -1 with errno on the first failure. NOTE: the extent lseeks
   move the source fd's offset — a caller falling back to a sequential
   read() loop must lseek it back to 0 first (pwrite never moves the
   destination fd's offset). */
static int copy_pwrite_all(int sfd, int dfd, off_t size) {
    size_t bufsz = 1024 * 1024;
    char *buf = malloc(bufsz);
    if (!buf) { errno = ENOMEM; return -1; }
    off_t off = 0;
    int rc = 0;
    while (off < size) {
        off_t d = lseek(sfd, off, SEEK_DATA);
        if (d < 0) {
            if (errno == ENXIO) break;   /* nothing but hole ahead: done */
            rc = -1;
            break;
        }
        if (d >= size) break;
        off_t h = lseek(sfd, d, SEEK_HOLE);
        if (h <= d) { rc = -1; break; }  /* must advance */
        if (h > size) h = size;
        off_t p = d;
        while (p < h) {
            size_t want = (h - p > (off_t)bufsz) ? bufsz : (size_t)(h - p);
            size_t got = 0;
            while (got < want) {
                ssize_t n = pread(sfd, buf + got, want - got, p + (off_t)got);
                if (n < 0) { if (errno == EINTR) continue; rc = -1; break; }
                if (n == 0) break;       /* EOF: source shrank under us */
                got += (size_t)n;
            }
            if (rc == 0 && got == 0) { errno = EIO; rc = -1; }  /* source shrank: fail, never pad */
            if (rc != 0) break;
            size_t written = 0;
            while (written < got) {
                ssize_t w = pwrite(dfd, buf + written, got - written,
                                   p + (off_t)written);
                if (w < 0) { if (errno == EINTR) continue; rc = -1; break; }
                written += (size_t)w;
            }
            if (rc != 0) break;
            p += (off_t)got;
        }
        if (rc != 0) break;
        off = h;
    }
    free(buf);
    return rc;
}

/* Sparse-preserving copy of [0, size). Gated on od_seek_data_trusted so
   a dirty unflushed page is never skipped; unusable (SEEK_DATA
   untrusted on this filesystem) or failed -> -1 and the caller falls
   back to the dense loop. */
#ifdef TEST_BUILD
_Atomic int g_restore_test_fault_mask;
_Atomic int g_restore_test_swap_rename_at;
static int restore_test_fault(int bit) {
    int m = atomic_load(&g_restore_test_fault_mask);
    while (m & bit)
        if (atomic_compare_exchange_weak(&g_restore_test_fault_mask, &m, m & ~bit)) {
            errno = EIO; return 1;
        }
    return 0;
}
static int restore_test_swap_rename_fault(size_t i) {
    if ((int)i != atomic_load(&g_restore_test_swap_rename_at)) return 0;
    return restore_test_fault(SHARD_TEST_RF_SWAP_RENAME);
}
static pthread_mutex_t g_restore_park_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_restore_park_site;
static shard_db_test_gate_fn g_restore_park_fn;
static void *g_restore_park_ctx;
void shard_db_test_set_restore_park_hook(int site, shard_db_test_gate_fn fn, void *ctx) {
    pthread_mutex_lock(&g_restore_park_lock);
    g_restore_park_site = fn ? site : 0; g_restore_park_fn = fn; g_restore_park_ctx = ctx;
    pthread_mutex_unlock(&g_restore_park_lock);
}
static void restore_test_park(int site) {   /* one-shot, mirrors shard_db_test_guard_park */
    shard_db_test_gate_fn fn = NULL; void *ctx = NULL;
    pthread_mutex_lock(&g_restore_park_lock);
    if (g_restore_park_fn && g_restore_park_site == site) {
        fn = g_restore_park_fn; ctx = g_restore_park_ctx;
        g_restore_park_fn = NULL; g_restore_park_ctx = NULL; g_restore_park_site = 0;
    }
    pthread_mutex_unlock(&g_restore_park_lock);
    if (fn) fn(ctx);
}
#else
#define restore_test_fault(bit) 0
#define restore_test_swap_rename_fault(i) 0
#define restore_test_park(site) ((void)0)
#endif

#ifdef TEST_BUILD
static int g_copy_test_partial_fail = 0;
void maint_test_copy_partial_fail(int on) { g_copy_test_partial_fail = on; }
#endif

static int copy_file_sparse(int sfd, int dfd, const char *src, off_t size) {
    if (!od_seek_data_trusted(sfd, src)) return -1;
#ifdef TEST_BUILD
    if (g_copy_test_partial_fail) {
        /* Simulate a sparse copy that wrote more than the dense pass will:
           the fallback must truncate it away. */
        static const char junk[4096] = {'X'};
        (void)pwrite(dfd, junk, sizeof(junk), 0);
        (void)pwrite(dfd, junk, sizeof(junk), size + 8192);
        return -1;
    }
#endif
    if (copy_pwrite_all(sfd, dfd, size) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "copy_file: sparse extent copy of %s failed: %s",
                  src, strerror(errno));
        return -1;
    }
    if (ftruncate(dfd, size) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "copy_file: ftruncate of %s copy failed: %s",
                  src, strerror(errno));
        return -1;
    }
    return 0;
}

static int copy_file_raw(const char *src, const char *dst, mode_t mode) {
    int sfd = open(src, O_RDONLY);
    if (sfd < 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "copy_file: open(%s) failed: %s", src, strerror(errno));
        return -1;
    }
    int dfd = open(dst, O_WRONLY | O_CREAT | O_TRUNC, mode & 0777);
    if (dfd < 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "copy_file: open(%s) failed: %s", dst, strerror(errno));
        close(sfd); return -1;
    }
    struct stat st;
    if (fstat(sfd, &st) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "copy_file: fstat(%s) failed: %s", src, strerror(errno));
        close(sfd); close(dfd);
        return -1;
    }
    if (st.st_size > 0 && copy_file_sparse(sfd, dfd, src, st.st_size) == 0) {
        close(sfd);
        close(dfd);
        return 0;
    }
    if (st.st_size == 0) {
        /* Nothing to copy; O_TRUNC already left dst empty. */
        close(sfd);
        close(dfd);
        return 0;
    }
    /* Sparse path unusable (SEEK_DATA untrusted) or failed mid-copy:
       dense fallback. The extent lseeks moved the source offset, so
       rewind both fds before the sequential loop. */
    (void)lseek(sfd, 0, SEEK_SET);
    (void)lseek(dfd, 0, SEEK_SET);
    /* A failed sparse attempt may have left partial data; drop it so no
       stale tail survives if the source is shorter on this pass. */
    if (ftruncate(dfd, 0) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "copy_file: ftruncate(%s) failed: %s", dst, strerror(errno));
        close(sfd); close(dfd);
        return -1;
    }
    char buf[64 * 1024];
    ssize_t n;
    int rc = 0;
    while ((n = read(sfd, buf, sizeof(buf))) > 0) {
        size_t total = (size_t)n;     /* n > 0 here; bounded by sizeof(buf) */
        size_t off = 0;
        while (off < total) {
            /* Loop guard `off < total` (both size_t) makes total-off > 0
               and well within size_t range — the only writer to off is
               `off += (size_t)w` where w >= 0 (the w<0 path goto-dones).
               Coverity's INTEGER_OVERFLOW heuristic chains taintedness
               from read()'s ssize_t return, so it doesn't trust the
               cast+invariant chain. The arithmetic is safe by construction.
               coverity[overflow_sink] total - off is unsigned-positive */
            ssize_t w = write(dfd, buf + off, total - off);
            if (w < 0) { rc = -1; goto done; }
            off += (size_t)w;
        }
    }
    if (n < 0) rc = -1;
done:
    close(sfd);
    close(dfd);
    return rc;
}

/* Copy one file and make the copy durable: a "backed up" / "restored"
   answer must survive power loss (AUD-BACKUP-SILENT). */
static int copy_file(const char *src, const char *dst, mode_t mode) {
    if (copy_file_raw(src, dst, mode) != 0) return -1;
    return fsync_file_path(dst);
}

#ifdef TEST_BUILD
/* TEST_BUILD wrapper over the internal copy_file (the backup/restore
   file copy) so test-backup-copy-sparse can exercise the copy
   directly. */
void maint_test_copy_file(const char *src, const char *dst) {
    (void)copy_file(src, dst, 0600);
}
#endif

/* Recursively copy a directory tree. No shell, no path interpolation —
   every path is built component-wise, so the source-object name (which
   originates from a JSON request) cannot inject shell metacharacters.
   Replaces the previous `system("cp -r ...")` invocations that CodeQL
   flagged as "uncontrolled data used in OS command". */
static int cprf(const char *src, const char *dst) {
    /* TOCTOU-safe: open by path once, then fstat the fd and fdopendir the
       same fd. O_NOFOLLOW rejects symlinks at the open boundary (skipped,
       the documented contract). A missing source means there is nothing to
       copy (returns 0); every other failure returns -1 and stops. */
    int fd = open(src, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) return (errno == ENOENT || errno == ELOOP) ? 0 : -1;
    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return -1; }

    int rc = 0;
    if (S_ISDIR(st.st_mode)) {
        mkdirp(dst);
        struct stat ds;
        if (stat(dst, &ds) != 0 || !S_ISDIR(ds.st_mode)) { close(fd); return -1; }
        DIR *d = fdopendir(fd);          /* takes ownership of fd */
        if (!d) { close(fd); return -1; }
        struct dirent *e;
        while (rc == 0 && (e = readdir(d))) {
            if (e->d_name[0] == '.' &&
                (e->d_name[1] == '\0' ||
                 (e->d_name[1] == '.' && e->d_name[2] == '\0')))
                continue;
            /* Crash-orphaned SEEK_DATA probe file (od_seek_data_trusted):
               never copy it into a backup. */
            if (strncmp(e->d_name, ".sdprobe-", 9) == 0) continue;
            char child_src[PATH_MAX], child_dst[PATH_MAX];
            if (snprintf(child_src, sizeof(child_src), "%s/%s", src, e->d_name) >= (int)sizeof(child_src) ||
                snprintf(child_dst, sizeof(child_dst), "%s/%s", dst, e->d_name) >= (int)sizeof(child_dst)) {
                errno = ENAMETOOLONG;
                rc = -1;
                break;
            }
            if (cprf(child_src, child_dst) != 0) rc = -1;
        }
        if (closedir(d) != 0 && rc == 0) rc = -1;   /* closes the underlying fd */
        if (rc == 0 && fsync_dir(dst) != 0) rc = -1;
    } else if (S_ISREG(st.st_mode)) {
        close(fd);                        /* copy_file opens its own fd */
        rc = copy_file(src, dst, st.st_mode);
    } else {
        close(fd);
    }
    /* Symlinks, fifos, devices etc. are ignored — backup targets are
       always shard-db's own regular files / directories. */
    return rc;
}

int cmd_backup(const char *db_root, const char *object) {
    char src_dir[PATH_MAX], bak_dir[PATH_MAX], bak_parent[PATH_MAX];
    snprintf(src_dir, sizeof(src_dir), "%s/%s", db_root, object);
    struct stat st;
    if (stat(src_dir, &st) != 0) {
        fprintf(stderr, "Error: Object [%s] not found\n", object);
        return 1;
    }

    /* Create a uniquely named, timestamped backup directory: two backups in
       the same second must never merge into one directory. */
    time_t now = time(NULL);
    struct tm tbuf; struct tm *t = localtime_r(&now, &tbuf);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y%m%d_%H%M%S", t);
    snprintf(bak_parent, sizeof(bak_parent), "%s/%s/backup", db_root, object);
    mkdirp(bak_parent);
    int made = 0;
    for (int n = 0; n < 1000 && !made; n++) {
        if (n == 0) snprintf(bak_dir, sizeof(bak_dir), "%s/%s", bak_parent, ts);
        else        snprintf(bak_dir, sizeof(bak_dir), "%s/%s_%d", bak_parent, ts, n);
        if (mkdir(bak_dir, 0755) == 0) made = 1;
        else if (errno != EEXIST) break;
    }
    if (!made) {
        OUT("{\"error\":\"backup failed: cannot create backup directory: %s\"}\n", strerror(errno));
        return 1;
    }

    /* Any failed step removes the partial backup and reports the error
       instead of claiming success (AUD-BACKUP-SILENT). */
    const char *failed = NULL;
    int saved_errno = 0;

    /* Recursively copy data/, indexes/, and metadata/ (files/ and
       tokens.conf follow below). v2 stores kf shards + segments under
       the same data/ umbrella, so a single cprf("data") captures both
       layouts. */
    Schema sch = load_schema(db_root, object);
    char src_sub[PATH_MAX], dst_sub[PATH_MAX];
    const char *subs[] = { "data", "indexes", "metadata" };
    for (size_t i = 0; i < sizeof(subs) / sizeof(subs[0]) && !failed; i++) {
        snprintf(src_sub, sizeof(src_sub), "%s/%s", src_dir, subs[i]);
        snprintf(dst_sub, sizeof(dst_sub), "%s/%s", bak_dir, subs[i]);
        if (cprf(src_sub, dst_sub) != 0) { failed = subs[i]; saved_errno = errno; }
    }

    /* files/ (put-file store) is part of the object; copy it as-is. cprf
       returns 0 when the source does not exist. */
    if (!failed) {
        snprintf(src_sub, sizeof(src_sub), "%s/files", src_dir);
        snprintf(dst_sub, sizeof(dst_sub), "%s/files", bak_dir);
        if (cprf(src_sub, dst_sub) != 0) { failed = "files"; saved_errno = errno; }
    }

    /* The object's access tokens travel with it. They are secrets: the copy
       is 0600 regardless of the source mode. A missing file is fine. */
    if (!failed) {
        snprintf(src_sub, sizeof(src_sub), "%s/tokens.conf", src_dir);
        snprintf(dst_sub, sizeof(dst_sub), "%s/tokens.conf", bak_dir);
        if (access(src_sub, F_OK) == 0 && copy_file(src_sub, dst_sub, 0600) != 0) {
            failed = "tokens.conf"; saved_errno = errno;
        }
    }

    /* Schema capture — make the backup directory self-contained. fields.conf
       has the typed-field layout; object.json carries splits + max_key (which
       live in the global schema.conf, not under <obj>/). Without these,
       cmd_restore couldn't recreate the object on a fresh DB. */
    if (!failed) {
        char src_fields[PATH_MAX], dst_fields[PATH_MAX];
        snprintf(src_fields, sizeof(src_fields), "%s/fields.conf", src_dir);
        snprintf(dst_fields, sizeof(dst_fields), "%s/fields.conf", bak_dir);
        if (stat(src_fields, &st) == 0 &&
            copy_file(src_fields, dst_fields, st.st_mode) != 0) {
            failed = "fields.conf"; saved_errno = errno;
        }
    }

    if (!failed) {
        char meta_path[PATH_MAX];
        snprintf(meta_path, sizeof(meta_path), "%s/object.json", bak_dir);
        FILE *mf = fopen(meta_path, "w");
        int ok = mf != NULL;
        if (mf) {
            /* storage_version slot kept in the backup metadata for forward
               compatibility — every object the daemon writes is v2. */
            ok = fprintf(mf, "{\"splits\":%d,\"max_key\":%d,\"storage_version\":2,\"streams\":%d,\"layout\":\"packed\",\"files\":1}\n",
                         sch.splits, sch.max_key, sch.streams) > 0;
            if (fflush(mf) != 0) ok = 0;
            if (ok && fsync(fileno(mf)) != 0) ok = 0;
            if (fclose(mf) != 0) ok = 0;
        }
        if (!ok) { failed = "object.json"; saved_errno = errno; }
    }
    if (!failed && (fsync_dir(bak_dir) != 0 || fsync_dir(bak_parent) != 0)) {
        failed = "directory sync"; saved_errno = errno;
    }
    if (failed) {
        rmrf(bak_dir);
        OUT("{\"error\":\"backup failed at %s: %s\"}\n", failed,
            strerror(saved_errno ? saved_errno : EIO));
        return 1;
    }

    OUT("{\"status\":\"backed_up\",\"path\":\"%s\"}\n", bak_dir);
    return 0;
}

/* Parse object.json written by cmd_backup. splits + max_key are required;
   storage_version + streams are optional (older backups predate them).
   Returns 1 on success. Defaults out_storage_version to 1 / out_streams
   to 0 when missing, matching legacy semantics. */
static int parse_object_meta(const char *path, int *out_splits, int *out_max_key,
                              int *out_storage_version, int *out_streams, int *out_packed) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char buf[256] = {0};
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    int read_err = ferror(f);
    fclose(f);
    if (read_err) return 0;
    buf[n] = '\0';
    int splits = -1, max_key = -1, sv = 1, streams = 0;
    const char *p = strstr(buf, "\"splits\":");
    if (p) splits = atoi(p + strlen("\"splits\":"));
    p = strstr(buf, "\"max_key\":");
    if (p) max_key = atoi(p + strlen("\"max_key\":"));
    p = strstr(buf, "\"storage_version\":");
    if (p) sv = atoi(p + strlen("\"storage_version\":"));
    p = strstr(buf, "\"streams\":");
    if (p) streams = atoi(p + strlen("\"streams\":"));
    int packed = strstr(buf, "\"layout\":\"packed\"") != NULL;
    if (splits < 0 || max_key < 0) return 0;
    *out_splits = splits;
    *out_max_key = max_key;
    if (out_storage_version) *out_storage_version = sv;
    if (out_streams)         *out_streams = streams;
    if (out_packed)          *out_packed = packed;
    return 1;
}

/* Scan `f` for a line starting with `prefix`. 1 found, 0 not found, -1
   read error. */
static int schema_conf_scan(FILE *f, const char *prefix, int pfxlen) {
    rewind(f);
    char line[512];
    while (fgets(line, sizeof(line), f))
        if (strncmp(line, prefix, (size_t)pfxlen) == 0) return 1;
    return ferror(f) ? -1 : 0;
}

/* 1 when the object's schema.conf line exists, 0 when the file or the line
   is absent, -1 on a read error (errno set). */
static int schema_conf_line_present(const char *db_root, const char *object) {
    const char *dir = db_root + strlen(g_db_root);
    if (*dir == '/') dir++;

    char conf[PATH_MAX], prefix[512];
    snprintf(conf, sizeof(conf), "%s/schema.conf", g_db_root);
    int pfxlen = snprintf(prefix, sizeof(prefix), "%s:%s:", dir, object);
    if (pfxlen <= 0 || pfxlen >= (int)sizeof(prefix)) { errno = ENAMETOOLONG; return -1; }

    FILE *f = fopen(conf, "r");
    if (!f) return errno == ENOENT ? 0 : -1;
    int lockfd = fileno(f);
    if (flock(lockfd, LOCK_SH) != 0) {
        int e = errno; fclose(f); errno = e; return -1;
    }
    int rc = schema_conf_scan(f, prefix, pfxlen);
    flock(lockfd, LOCK_UN);
    if (fclose(f) != 0 && rc >= 0) rc = -1;
    return rc;
}

/* Append a `dir:object:splits:max_key:2:streams:layout=packed` line to
   schema.conf if it isn't already there. Caller holds objlock_wrlock.
   Returns 0 on success, -1 on IO failure (errno preserved). */
static int ensure_schema_conf_line(const char *db_root, const char *object,
                                   int splits, int max_key, int streams) {
    const char *dir = db_root + strlen(g_db_root);
    if (*dir == '/') dir++;

    char conf[PATH_MAX];
    snprintf(conf, sizeof(conf), "%s/schema.conf", g_db_root);

    char prefix[512];
    int pfxlen = snprintf(prefix, sizeof(prefix), "%s:%s:", dir, object);
    if (pfxlen <= 0 || pfxlen >= (int)sizeof(prefix)) { errno = ENAMETOOLONG; return -1; }

    FILE *f = fopen(conf, "a+");
    if (!f) {
        LOG_ERROR(LOG_SUB_CONFIG, "ensure_schema_conf_line: fopen(%s) failed: %s", conf, strerror(errno));
        return -1;
    }
    int lockfd = fileno(f);
    if (flock(lockfd, LOCK_EX) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "ensure_schema_conf_line: flock(%s) failed: %s", conf, strerror(errno));
        int e = errno;
        fclose(f);
        errno = e;
        return -1;
    }
    int found = schema_conf_scan(f, prefix, pfxlen);
    if (found < 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "ensure_schema_conf_line: read(%s) failed: %s", conf, strerror(errno));
        int e = errno;
        flock(lockfd, LOCK_UN);
        fclose(f);
        errno = e;
        return -1;
    }
    int append_failed = 0;
    if (!found) {
        if (fseek(f, 0, SEEK_END) != 0) append_failed = 1;
        else if (fprintf(f, "%s%d:%d:2:%d:layout=packed\n", prefix, splits,
                         max_key, streams) < 0) append_failed = 1;
        else if (fflush(f) != 0) append_failed = 1;
        else if (fsync(fileno(f)) != 0) append_failed = 1;
        if (append_failed) {
            LOG_ERROR(LOG_SUB_CONFIG, "ensure_schema_conf_line: append(%s) failed: %s", conf, strerror(errno));
            int e = errno;
            flock(lockfd, LOCK_UN);
            fclose(f);
            errno = e;
            return -1;
        }
    }
    flock(lockfd, LOCK_UN);
    if (fclose(f) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "ensure_schema_conf_line: fclose(%s) failed: %s", conf, strerror(errno));
        return -1;
    }
    if (!found && fsync_dir(g_db_root) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "ensure_schema_conf_line: fsync_dir(%s) failed: %s",
                  g_db_root, strerror(errno));
        return -1;
    }
    return 0;
}

/* 1 when the backup's object.json carries "files":1 (written by backups
   taken with files/ support); 0 for older backups or unreadable manifests. */
static int backup_manifest_has_files(const char *meta_path) {
    FILE *f = fopen(meta_path, "r");
    if (!f) return 0;
    char buf[256] = {0};
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    int err = ferror(f);
    fclose(f);
    if (err) return 0;
    buf[n] = '\0';
    return strstr(buf, "\"files\":1") != NULL;
}

#define RESTORE_STAGE_PREFIX ".restore-"

/* Startup/leftover recovery for the stage-then-swap restore. A staging
   directory without a SWAP marker never touched the live object (delete it).
   One with the marker means a crash during the swap: roll every subtree
   back to its pre-restore state (the backup is intact, so the operator can
   run the restore again), then delete the staging directory. */
static const char *const RESTORE_SWAP_NAMES[] =
    { "data", "indexes", "metadata", "files", "fields.conf" };
#define RESTORE_NSWAP (sizeof(RESTORE_SWAP_NAMES) / sizeof(RESTORE_SWAP_NAMES[0]))

/* State-driven rollback (I2/I3/I6): for each name with old/<name>, move the
   live entry (a restored copy) to rb/<name>, then old/<name> back. Idempotent
   from any crash prefix. 0 = rolled back durably (or nothing to roll back);
   -1 = stopped (errno set). *progress is set to 1 when this call renamed
   anything (caller may pass NULL). */
static int restore_rollback_swap(const char *obj_dir, const char *stage, int *progress) {
    char rb[PATH_MAX], old[PATH_MAX];
    int moved_trash = 0;
    if (progress) *progress = 0;
    if (snprintf(rb, sizeof(rb), "%s/rb", stage) >= (int)sizeof(rb) ||
        snprintf(old, sizeof(old), "%s/old", stage) >= (int)sizeof(old)) {
        errno = ENAMETOOLONG; return -1;
    }
    struct stat st;
    /* No old/ at all: nothing was ever moved aside, or a cleanup already
       emptied it (table rows 4, 11): nothing to roll back, no syncs. */
    if (lstat(old, &st) != 0) return errno == ENOENT ? 0 : -1;
    if (mkdir(rb, 0755) != 0) {
        if (errno != EEXIST) return -1;
        if (lstat(rb, &st) != 0) return -1;
        if (!S_ISDIR(st.st_mode)) { errno = ENOTDIR; return -1; }
    }
    for (size_t k = RESTORE_NSWAP; k-- > 0; ) {
        const char *name = RESTORE_SWAP_NAMES[k];
        char aside[PATH_MAX], live[PATH_MAX], trash[PATH_MAX];
        if (snprintf(aside, sizeof(aside), "%s/%s", old, name) >= (int)sizeof(aside) ||
            snprintf(live, sizeof(live), "%s/%s", obj_dir, name) >= (int)sizeof(live) ||
            snprintf(trash, sizeof(trash), "%s/%s", rb, name) >= (int)sizeof(trash)) {
            errno = ENAMETOOLONG; return -1;
        }
        if (lstat(aside, &st) != 0) { if (errno == ENOENT) continue; return -1; }
        if (lstat(live, &st) == 0) {
            if (lstat(trash, &st) == 0) rmrf(trash);      /* only ever a restored copy */
            else if (errno != ENOENT) return -1;
            if (rename(live, trash) != 0) return -1;
            moved_trash = 1;
            if (progress) *progress = 1;
        } else if (errno != ENOENT) {
            return -1;
        }
        if (restore_test_fault(SHARD_TEST_RF_ROLLBACK_RENAME) || rename(aside, live) != 0)
            return -1;
        if (progress) *progress = 1;
    }
    if (restore_test_fault(SHARD_TEST_RF_ROLLBACK_DIRSYNC) || fsync_dir(obj_dir) != 0 ||
        fsync_dir(old) != 0 || (moved_trash && fsync_dir(rb) != 0))
        return -1;
    return 0;
}

/* I6: make "no SWAP" durable before deleting a rolled-back (or never-swapped)
   stage, so a crash mid-delete cannot leave SWAP behind a vanished old/.
   On failure the stage is kept: it holds SWAP and no names in old/, which
   the next rollback treats as a no-op. */
static void restore_uncommit_and_remove(const char *stage) {
    char mark[PATH_MAX];
    if (snprintf(mark, sizeof(mark), "%s/SWAP", stage) >= (int)sizeof(mark)) return;
    if (unlink(mark) != 0 && errno != ENOENT) {
        LOG_WARN(LOG_SUB_CONFIG, "RESTORE %s: cannot remove SWAP (%s); staging kept", stage, strerror(errno));
        return;
    }
    if (fsync_dir(stage) != 0) {
        LOG_WARN(LOG_SUB_CONFIG, "RESTORE %s: cannot sync the SWAP removal (%s); staging kept", stage, strerror(errno));
        return;
    }
    rmrf(stage);
}

static void restore_free_names(char **names, size_t n) {
    for (size_t i = 0; i < n; i++) free(names[i]);
    free(names);
}

/* 1 = at least one interrupted swap rolled back; 0 = nothing to roll back;
   -1 = a SWAP stage could not be rolled back (kept for the next attempt and
   the operator, I5). Stages without SWAP are deleted (before SWAP: nothing
   moved; after commit: old/ is the superseded tree). */
static int restore_recover_impl(const char *obj_dir, int *out_progress) {
    if (out_progress) *out_progress = 0;
    DIR *d = opendir(obj_dir);
    if (!d) {
        if (errno == ENOENT || errno == ENOTDIR) return 0;
        LOG_ERROR(LOG_SUB_CONFIG, "RESTORE-RECOVER %s: cannot open: %s", obj_dir, strerror(errno));
        return -1;
    }
    char **names = NULL;
    size_t n = 0, cap = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (strncmp(e->d_name, RESTORE_STAGE_PREFIX,
                    strlen(RESTORE_STAGE_PREFIX)) != 0) continue;
        if (n == cap) {
            size_t ncap = cap ? cap * 2 : 8;
            char **t = realloc(names, ncap * sizeof(*t));
            if (!t) {
                LOG_ERROR(LOG_SUB_CONFIG, "RESTORE-RECOVER %s: out of memory", obj_dir);
                restore_free_names(names, n);
                closedir(d);
                errno = ENOMEM;
                return -1;
            }
            names = t; cap = ncap;
        }
        names[n] = strdup(e->d_name);
        if (!names[n]) {
            LOG_ERROR(LOG_SUB_CONFIG, "RESTORE-RECOVER %s: out of memory", obj_dir);
            restore_free_names(names, n);
            closedir(d);
            errno = ENOMEM;
            return -1;
        }
        n++;
    }
    closedir(d);

    int failed = 0, rolled_back = 0, removed = 0, first_errno = 0;
    for (size_t i = 0; i < n; i++) {
        char stage[PATH_MAX], mark[PATH_MAX];
        snprintf(stage, sizeof(stage), "%s/%s", obj_dir, names[i]);
        snprintf(mark, sizeof(mark), "%s/SWAP", stage);
        struct stat st;
        if (lstat(stage, &st) != 0) {
            if (errno == ENOENT) continue;
            if (!failed) first_errno = errno;
            failed = 1;
            continue;
        }
        if (!S_ISDIR(st.st_mode)) {
            /* A stray regular file that merely shares the prefix: neither a
               stage to fail on nor one to delete. */
            LOG_WARN(LOG_SUB_CONFIG, "RESTORE-RECOVER %s: skipping non-directory %s",
                     obj_dir, stage);
            continue;
        }
        int have_swap = 0;
        if (lstat(mark, &st) == 0) {
            have_swap = 1;
        } else if (errno != ENOENT) {
            if (!failed) first_errno = errno;
            failed = 1;
            continue;
        }
        if (have_swap) {
            int progress = 0;
            if (restore_rollback_swap(obj_dir, stage, &progress) != 0) {
                int rerrno = errno;
                LOG_ERROR(LOG_SUB_CONFIG,
                          "RESTORE-RECOVER %s: interrupted restore %s could not be rolled back: %s; staging kept (see docs/operations/backup.md)",
                          obj_dir, stage, strerror(rerrno));
                if (!failed) first_errno = rerrno;
                failed = 1;
                if (progress && out_progress) *out_progress = 1;
                continue;
            }
            LOG_WARN(LOG_SUB_CONFIG, "RESTORE: rolled back an interrupted restore in %s; run restore again", obj_dir);
            rolled_back = 1;
            restore_uncommit_and_remove(stage);
            removed = 1;
        } else {
            rmrf(stage);
            removed = 1;
        }
    }
    restore_free_names(names, n);
    if (removed) (void)fsync_dir(obj_dir);
    if (failed) {
        errno = first_errno ? first_errno : EIO;
        return -1;
    }
    return rolled_back;
}

int maint_restore_swap_recover(const char *obj_dir) { return restore_recover_impl(obj_dir, NULL); }

/* Unique stage suffix per restore call (the pid alone can repeat after a
   restart while a leftover stage is still on disk). */
static _Atomic unsigned g_restore_stage_seq;

/* Drop every cached mapping of the object's files: before the swap so no
   reader can pin the old inodes, and after it so the next reader sees the
   restored ones. The btree page cache holds an open fd+mmap per
   <field>/<NNN>.idx, so walk indexes/ explicitly (same pattern as
   index.c::reindex_clean_legacy). */
static void restore_invalidate_all(const char *db_root, const char *object) {
    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);
    invalidate_idx_cache(db_root, object);
    invalidate_schema_caches(db_root, object);
    {
        /* Walk indexes/<field>/ trees and drop btree cache entries for each
           per-shard .idx file. opendir() distinguishes dir vs file-or-other
           in one syscall (returns NULL with ENOTDIR for non-dirs), so we
           skip the explicit stat() pre-check (Coverity TOCTOU CID-1692482:
           between stat-says-dir and opendir, a symlink swap could redirect
           to an attacker-controlled path). For paths where opendir fails,
           call btree_cache_invalidate directly — the cache is keyed on
           path strings with no file I/O, so a phantom path or non-regular
           file at that location is harmless (the lookup just misses). */
        char idx_root[PATH_MAX];
        snprintf(idx_root, sizeof(idx_root), "%s/indexes", obj_dir);
        DIR *d = opendir(idx_root);
        if (d) {
            struct dirent *e;
            while ((e = readdir(d))) {
                if (e->d_name[0] == '.') continue;
                char field_dir[PATH_MAX];
                snprintf(field_dir, sizeof(field_dir), "%s/%s", idx_root, e->d_name);
                DIR *sd = opendir(field_dir);
                if (sd) {
                    struct dirent *se;
                    while ((se = readdir(sd))) {
                        if (se->d_name[0] == '.') continue;
                        char sp[PATH_MAX];
                        snprintf(sp, sizeof(sp), "%s/%s", field_dir, se->d_name);
                        btree_cache_invalidate(sp);
                        /* A restore replaces the index files: without the
                           bitmap entry dropped, a stale mmap could serve
                           pre-restore bits (D8: no inode check without a
                           publish-generation change). */
                        bm_cache_invalidate(sp);
                    }
                    closedir(sd);
                } else {
                    /* Non-dir at this path (legacy single-file layout, or
                       a stray) — invalidate cache by direct path. */
                    btree_cache_invalidate(field_dir);
                    bm_cache_invalidate(field_dir);
                }
            }
            closedir(d);
        }
    }
    counts_invalidate(db_root, object);
    slotcask_registry_invalidate(db_root, object);
}

/* After a failed fsync/rename the on-disk state is unknowable; crash-and-
   recover is the only path that never serves a half-swapped object (D2).
   Logging is asynchronous and the daemon's stderr is /dev/null, so write the
   message synchronously to the log file and stderr before aborting. */
static void restore_fail_stop(const char *obj_dir, const char *step, int err, int rb_err)
    __attribute__((noreturn));
static void restore_fail_stop(const char *obj_dir, const char *step, int err, int rb_err) {
    char msg[PATH_MAX * 2 + 256];
    snprintf(msg, sizeof(msg),
             "RESTORE %s: %s failed (%s) and the rollback could not be completed (%s); "
             "aborting, startup recovery finishes the rollback",
             obj_dir, step, strerror(err ? err : EIO), strerror(rb_err ? rb_err : EIO));
    LOG_ERROR(LOG_SUB_CONFIG, "%s", msg);
    if (g_log_dir[0]) {
        FILE *lf = open_log_for_level(1);
        if (lf) { fprintf(lf, "%s\n", msg); fclose(lf); }
    }
    fprintf(stderr, "%s\n", msg);
    fflush(stderr);
    abort();
}

/* After a durable SWAP: roll back, clean up, reply. Never returns on a failed
   rollback (D2). */
static int restore_abandon(const char *db_root, const char *object, const char *obj_dir,
                           const char *stage, const char *step, int err) {
    if (restore_rollback_swap(obj_dir, stage, NULL) != 0)
        restore_fail_stop(obj_dir, step, err, errno);
    restore_uncommit_and_remove(stage);   /* I6: never rmrf a stage holding SWAP */
    (void)fsync_dir(obj_dir);
    restore_invalidate_all(db_root, object);
    objlock_wrunlock(db_root, object);
    OUT("{\"error\":\"restore failed at %s: %s; rolled back, the live object was not changed\"}\n",
        step, strerror(err ? err : EIO));
    return 1;
}

/* ========== RESTORE ==========
   Symmetric to backup: copies data/ + indexes/ + metadata/ + fields.conf
   (plus files/ when the backup's object.json carries "files":1) from
   `<obj>/backup/<from>` into a staging directory, then swaps each entry in
   (<name> → <stage>/old/<name>, <stage>/new/<name> → live) and commits by
   durably removing the SWAP marker. A crash before the commit rolls back at
   the next start; after it the restored data stays (B1). The schema.conf
   line (and a live-absent fields.conf) is installed only after the commit.
   The backup's tokens.conf is not applied here — the JSON dispatcher merges
   those tokens into the running store after cmd_restore succeeds.
   Refuses if any live state would conflict, unless force=1. Holds the
   object's write lock for the whole operation; invalidates the slotcask
   registry + bt/bitmap caches + idx cache + schema caches + counts around
   the swap so the next reader sees the new mappings. */
int cmd_restore(const char *db_root, const char *object,
                const char *from, int force) {
    if (!from || !*from) { OUT("{\"error\":\"from is required\"}\n"); return 1; }
    if (strchr(from, '/') || strstr(from, "..")) {
        OUT("{\"error\":\"invalid from (no slashes or '..' allowed)\"}\n");
        return 1;
    }

    char obj_dir[PATH_MAX], src_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);
    snprintf(src_dir, sizeof(src_dir), "%s/backup/%s", obj_dir, from);
    struct stat st;
    if (stat(obj_dir, &st) != 0) {
        OUT("{\"error\":\"object not found\"}\n"); return 1;
    }
    if (stat(src_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        OUT("{\"error\":\"backup not found: %s\"}\n", from); return 1;
    }

    if (objlock_wrlock(db_root, object) != 0) {
        OUT("{\"error\":\"objlock_wrlock failed for restore\"}\n");
        return 1;
    }

    /* Finish or refuse an earlier interrupted restore before anything else
       looks at this object (I4/I5). A rollback that failed after moving
       something may leave the live tree part-rolled-back, so this call must
       not serve on: fail-stop and let startup finish it. */
    int prog = 0;
    int rec = restore_recover_impl(obj_dir, &prog);
    if (rec < 0) {
        if (prog)
            restore_fail_stop(obj_dir, "recovery of an earlier interrupted restore",
                              errno, errno);
        objlock_wrunlock(db_root, object);
        OUT("{\"error\":\"restore refused: an earlier interrupted restore of this object could not be rolled back; see the error log\"}\n");
        return 1;
    }
    if (rec > 0)
        restore_invalidate_all(db_root, object);

    /* Does the backup govern files/? (meta_src is declared later, so a
       dedicated path variable here.) */
    char fmeta[PATH_MAX];
    snprintf(fmeta, sizeof(fmeta), "%s/object.json", src_dir);
    int bak_files = backup_manifest_has_files(fmeta);

    const char *subs[] = { "data", "indexes", "metadata" };
    char src_sub[PATH_MAX], dst_sub[PATH_MAX];

    /* Refuse if any live subdir is non-empty, unless force=1.
       opendir() returns NULL with ENOTDIR for non-dirs and ENOENT for
       missing paths; both mean "not a populated dir we need to refuse",
       so we skip the explicit stat() + S_ISDIR pre-check (TOCTOU between
       stat-says-dir and opendir-actually-opens). */
    if (!force) {
        for (size_t i = 0; i < sizeof(subs) / sizeof(subs[0]); i++) {
            snprintf(dst_sub, sizeof(dst_sub), "%s/%s", obj_dir, subs[i]);
            DIR *d = opendir(dst_sub);
            if (d) {
                struct dirent *e; int empty = 1;
                while ((e = readdir(d))) {
                    if (e->d_name[0] == '.' &&
                        (e->d_name[1] == '\0' ||
                         (e->d_name[1] == '.' && e->d_name[2] == '\0'))) continue;
                    empty = 0; break;
                }
                closedir(d);
                if (!empty) {
                    objlock_wrunlock(db_root, object);
                    OUT("{\"error\":\"%s/ is not empty (use force=true to overwrite)\"}\n",
                        subs[i]);
                    return 1;
                }
            }
        }
    }

    /* Same refusal for files/ when the backup governs it. This runs before
       the later layout/schema/fields.conf refusals, so in the rare state
       where only files/ is non-empty it answers the files error first, and
       nothing is mutated before it. */
    if (bak_files && !force) {
        char files_probe[PATH_MAX];
        snprintf(files_probe, sizeof(files_probe), "%s/files", obj_dir);
        DIR *d = opendir(files_probe);
        if (d) {
            struct dirent *e; int empty = 1;
            while ((e = readdir(d))) {
                if (e->d_name[0] == '.' &&
                    (e->d_name[1] == '\0' ||
                     (e->d_name[1] == '.' && e->d_name[2] == '\0'))) continue;
                empty = 0; break;
            }
            closedir(d);
            if (!empty) {
                objlock_wrunlock(db_root, object);
                OUT("{\"error\":\"files/ is not empty (use force=true to overwrite)\"}\n");
                return 1;
            }
        }
    }

    /* Schema reconciliation. If the backup carries object.json, cross-check
       it against the live schema.conf entry. Mismatch → refuse without
       force. A missing live entry is not appended here: the line is
       installed only after the commit (T3), so a failure before it leaves
       schema.conf untouched. Backups from before this feature have no
       object.json — accept and skip. */
    char meta_src[PATH_MAX];
    snprintf(meta_src, sizeof(meta_src), "%s/object.json", src_dir);
    int bak_splits = 0, bak_max_key = 0, bak_sv = 1, bak_streams = 0, bak_packed = 0;
    int have_meta = parse_object_meta(meta_src, &bak_splits, &bak_max_key,
                                       &bak_sv, &bak_streams, &bak_packed);
    if (!have_meta) {
        /* No layout evidence: after the layout migration every live object
           is packed, so unknown bytes must never be copied over it. */
        objlock_wrunlock(db_root, object);
        OUT("{\"error\":\"backup has no layout metadata (object.json); restore refused\"}\n");
        return 1;
    }
    if (!bak_packed || bak_sv != 2) {
        /* Every object this binary writes is packed v2. A backup taken by a
           pre-layout binary carries no packed manifest (or an older storage
           version); converting it is no longer supported. */
        objlock_wrunlock(db_root, object);
        OUT("{\"error\":\"backup is not a packed v2 object; restore with shard-db 2026.11.2 first\"}\n");
        return 1;
    }
    {
        Schema live = load_schema(db_root, object);
        if (live.splits > 0 && (live.splits != bak_splits || live.max_key != bak_max_key) && !force) {
            objlock_wrunlock(db_root, object);
            OUT("{\"error\":\"schema mismatch: live splits=%d max_key=%d, "
                "backup splits=%d max_key=%d (use force=true to overwrite)\"}\n",
                live.splits, live.max_key, bak_splits, bak_max_key);
            return 1;
        }
    }

    /* fields.conf moves with the data (D5). The backup copy is opened first
       and fstat'ed (TOCTOU: stat-then-open could be redirected by a symlink
       swap). A live regular file is swapped with the data; an absent live
       file is installed from the stage right after the commit; anything
       else is refused before any mutation. */
    char fields_src[PATH_MAX], fields_dst[PATH_MAX];
    snprintf(fields_src, sizeof(fields_src), "%s/fields.conf", src_dir);
    snprintf(fields_dst, sizeof(fields_dst), "%s/fields.conf", obj_dir);
    enum { FIELDS_NONE = 0, FIELDS_INSTALL, FIELDS_SWAP } fields_mode = FIELDS_NONE;
    mode_t fields_mode_bits = 0644;
    {
        FILE *fa_pre = fopen(fields_src, "re");
        if (fa_pre) {
            struct stat sa;
            if (fstat(fileno(fa_pre), &sa) == 0 && S_ISREG(sa.st_mode)) {
                struct stat sb;
                if (lstat(fields_dst, &sb) != 0) {
                    if (errno != ENOENT) {
                        fclose(fa_pre);
                        objlock_wrunlock(db_root, object);
                        OUT("{\"error\":\"live fields.conf is not a regular file; restore refused\"}\n");
                        return 1;
                    }
                    fields_mode = FIELDS_INSTALL;
                } else if (S_ISREG(sb.st_mode)) {
                    fields_mode = FIELDS_SWAP;
                } else {
                    fclose(fa_pre);
                    objlock_wrunlock(db_root, object);
                    OUT("{\"error\":\"live fields.conf is not a regular file; restore refused\"}\n");
                    return 1;
                }
                fields_mode_bits = sa.st_mode;
                if (fields_mode == FIELDS_SWAP && !force) {
                    /* Compare contents — same size + same bytes = identical. */
                    int same = 0;
                    FILE *fb_pre = fopen(fields_dst, "re");
                    if (fb_pre) {
                        if (sb.st_size == sa.st_size) {
                            same = 1;
                            int ca, cb;
                            while ((ca = fgetc(fa_pre)) != EOF && (cb = fgetc(fb_pre)) != EOF) {
                                if (ca != cb) { same = 0; break; }
                            }
                        }
                        fclose(fb_pre);
                    }
                    if (!same) {
                        fclose(fa_pre);
                        objlock_wrunlock(db_root, object);
                        OUT("{\"error\":\"fields.conf differs between live and backup "
                            "(use force=true to overwrite)\"}\n");
                        return 1;
                    }
                }
            }
            fclose(fa_pre);
        }
    }

    /* The schema.conf line is installed only after the commit. Refuse now if
       a missing line could not be reconstructed validly from the manifest
       (AUD-P3-6 on this path). */
    int line_present = schema_conf_line_present(db_root, object);
    if (line_present < 0) {
        objlock_wrunlock(db_root, object);
        OUT("{\"error\":\"cannot read schema.conf: %s; restore refused\"}\n", strerror(errno));
        return 1;
    }
    if (line_present == 0 &&
        (bak_sv != 2 || bak_streams < 1 || bak_streams > SLOTCASK_MAX_STREAMS)) {
        objlock_wrunlock(db_root, object);
        OUT("{\"error\":\"backup manifest (object.json) lacks a valid storage_version/streams; cannot recreate the schema.conf line; restore refused\"}\n");
        return 1;
    }

    /* Drop caches first so readers can't pin the old mappings while we swap. */
    restore_invalidate_all(db_root, object);

    /* Stage, then swap (AUD-BACKUP-SILENT). The backup is copied into a
       staging directory first; the live object is not touched until every
       copy succeeded and was fsynced. The swap then renames each live
       entry aside and the staged one in; the durable SWAP removal is the
       commit point, and startup recovery rolls back every earlier prefix. */
    char stage[PATH_MAX], stage_new[PATH_MAX], stage_old[PATH_MAX], swap_mark[PATH_MAX];
    snprintf(stage, sizeof(stage), "%s/%s%d-%u", obj_dir, RESTORE_STAGE_PREFIX,
             (int)getpid(), atomic_fetch_add(&g_restore_stage_seq, 1) + 1);
    snprintf(stage_new, sizeof(stage_new), "%s/new", stage);
    snprintf(stage_old, sizeof(stage_old), "%s/old", stage);
    snprintf(swap_mark, sizeof(swap_mark), "%s/SWAP", stage);
    char stage_new_fields[PATH_MAX];
    snprintf(stage_new_fields, sizeof(stage_new_fields), "%s/fields.conf", stage_new);

    /* Every name a swap may move, in swap order. */
    const char *names[6];
    size_t nnames = 0;
    names[nnames++] = "data";
    names[nnames++] = "indexes";
    names[nnames++] = "metadata";
    if (bak_files) names[nnames++] = "files";
    /* SWAP moves the file with the data; INSTALL is renamed in after the
       commit and must not be part of the swap set. */
    if (fields_mode == FIELDS_SWAP) names[nnames++] = "fields.conf";   /* last */

    /* One exit for every pre-SWAP failure: nothing was moved, so undo the
       placeholders this call made and drop the stage (a kept stage can only
       hold an empty SWAP marker with no names in old/, table rows 3-4). */
    const char *pfail = NULL;
    int perrno = 0;
    unsigned made_mask = 0;
    int stage_made = mkdir(stage, 0755) == 0;
    if (!stage_made) {
        pfail = "staging directory"; perrno = errno;
    } else if (mkdir(stage_new, 0755) != 0 || mkdir(stage_old, 0755) != 0) {
        pfail = "staging directory"; perrno = errno;
    }
    if (!pfail && restore_test_fault(SHARD_TEST_RF_STAGE_COPY)) {
        pfail = "staging copy"; perrno = EIO;
    }
    for (size_t i = 0; !pfail && i < nnames; i++) {
        if (strcmp(names[i], "fields.conf") == 0) continue;   /* explicit copy below */
        char sdst[PATH_MAX];
        snprintf(src_sub, sizeof(src_sub), "%s/%s", src_dir, names[i]);
        snprintf(sdst, sizeof(sdst), "%s/%s", stage_new, names[i]);
        if (stat(src_sub, &st) == 0 && cprf(src_sub, sdst) != 0) {
            pfail = names[i]; perrno = errno;
        }
    }
    if (!pfail && fields_mode != FIELDS_NONE) {
        char fstage[PATH_MAX];
        snprintf(fstage, sizeof(fstage), "%s/fields.conf", stage_new);
        if (copy_file(fields_src, fstage, fields_mode_bits) != 0) {
            pfail = "fields.conf"; perrno = errno;
        }
    }
    if (!pfail && (fsync_dir(stage_new) != 0 || fsync_dir(stage) != 0)) {
        pfail = "staging sync"; perrno = errno;
    }
    /* I2: every swapped name must exist live before SWAP becomes durable.
       An absent live directory gets an inert, empty placeholder. */
    if (!pfail) {
        int made_any = 0;
        for (size_t i = 0; i < nnames; i++) {
            if (strcmp(names[i], "fields.conf") == 0) continue;
            char live[PATH_MAX];
            snprintf(live, sizeof(live), "%s/%s", obj_dir, names[i]);
            if (lstat(live, &st) == 0) continue;
            if (errno != ENOENT) { pfail = "directory preparation"; perrno = errno; break; }
            if (mkdir(live, 0755) != 0) {
                pfail = "directory preparation"; perrno = errno; break;
            }
            made_mask |= 1u << i;
            made_any = 1;
        }
        if (!pfail && made_any && fsync_dir(obj_dir) != 0) {
            pfail = "directory preparation"; perrno = errno;
        }
    }
    /* SWAP durable before the first rename. */
    if (!pfail) {
        int mfd = open(swap_mark, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int ok = mfd >= 0 && fsync(mfd) == 0;
        if (mfd >= 0 && close(mfd) != 0) ok = 0;
        if (ok && fsync_dir(stage) != 0) ok = 0;
        if (!ok) { pfail = "swap marker"; perrno = errno; }
    }
    if (pfail) {
        for (size_t i = 0; i < nnames; i++) {
            if (!(made_mask & (1u << i))) continue;
            char live[PATH_MAX];
            snprintf(live, sizeof(live), "%s/%s", obj_dir, names[i]);
            (void)rmdir(live);
        }
        if (stage_made) restore_uncommit_and_remove(stage);
        objlock_wrunlock(db_root, object);
        OUT("{\"error\":\"restore failed at %s: %s; the live object was not changed\"}\n",
            pfail, strerror(perrno ? perrno : EIO));
        return 1;
    }

    /* Forward swap. */
    const char *swap_step = NULL;
    int swap_err = 0;
    for (size_t i = 0; i < nnames && !swap_step; i++) {
        char live[PATH_MAX], aside[PATH_MAX], fresh[PATH_MAX];
        snprintf(live, sizeof(live), "%s/%s", obj_dir, names[i]);
        snprintf(aside, sizeof(aside), "%s/%s", stage_old, names[i]);
        snprintf(fresh, sizeof(fresh), "%s/%s", stage_new, names[i]);
        if (rename(live, aside) != 0) {
            swap_err = errno; swap_step = names[i]; break;
        }
        if (lstat(fresh, &st) == 0) {
            if (restore_test_swap_rename_fault(i) || rename(fresh, live) != 0) {
                swap_err = errno; swap_step = names[i]; break;
            }
        } else if (errno != ENOENT) {
            swap_err = errno; swap_step = names[i]; break;
        }
    }
    /* The restored indexes/ replaces this object's flag/marker state: re-arm
       the escape regardless of the outcome (the backup may hold the flag). */
    if (slotcask_bmfix_disabled_exists(obj_dir) != 0)
        atomic_store(&g_bmfix_any_disabled, 1);
    if (swap_step) {
        char step[64];
        snprintf(step, sizeof(step), "swap of %s", swap_step);
        return restore_abandon(db_root, object, obj_dir, stage, step, swap_err);
    }

    restore_test_park(SHARD_TEST_RESTORE_PARK_SWAPPED);

    /* Commit: (1) the renames durable in both directories, (2) the SWAP
       removal durable. Only then is the restored tree the object. */
    if (restore_test_fault(SHARD_TEST_RF_COMMIT_DIRSYNC) || fsync_dir(obj_dir) != 0 ||
        fsync_dir(stage_old) != 0 || fsync_dir(stage_new) != 0)
        return restore_abandon(db_root, object, obj_dir, stage, "directory sync", errno);
    if (unlink(swap_mark) != 0 && errno != ENOENT)
        return restore_abandon(db_root, object, obj_dir, stage, "swap marker removal", errno);
    if (restore_test_fault(SHARD_TEST_RF_UNSWAP_DIRSYNC) || fsync_dir(stage) != 0) {
        int e = errno;
        /* Removal not known durable either way: put SWAP back durably before
           rolling back, or a crash mid-rollback would let recovery delete old/
           as a committed leftover (table row 7b). */
        int ok = 0;
        if (!restore_test_fault(SHARD_TEST_RF_SWAP_RECREATE)) {
            int mfd = open(swap_mark, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            ok = mfd >= 0 && fsync(mfd) == 0;
            if (mfd >= 0 && close(mfd) != 0) ok = 0;
            if (ok && fsync_dir(stage) != 0) ok = 0;
        }
        if (!ok) restore_fail_stop(obj_dir, "swap marker sync", e, errno);
        return restore_abandon(db_root, object, obj_dir, stage, "swap marker sync", e);
    }
    /* COMMITTED */
    /* A missing fields.conf / schema.conf line is installed only now: the
       data is durable, so a failure here is reported instead of rolled back
       (D6 — a re-run with force is idempotent). */
    const char *install_fail = NULL;
    int ierr = 0;
    if (fields_mode == FIELDS_INSTALL &&
        (rename(stage_new_fields, fields_dst) != 0 || fsync_dir(obj_dir) != 0)) {
        install_fail = "fields.conf"; ierr = errno;
    }
    if ((restore_test_fault(SHARD_TEST_RF_SCHEMA_INSTALL) ||
         ensure_schema_conf_line(db_root, object, bak_splits, bak_max_key,
                                 bak_streams) != 0) && !install_fail) {
        install_fail = "the schema.conf line"; ierr = errno;
    }

    restore_invalidate_all(db_root, object);

    if (slotcask_bmfix_disabled_exists(obj_dir) != 0) {
        LOG_WARN(LOG_SUB_CONFIG,
                 "RESTORE %s/%s: restored object has bitmap planning disabled; run reindex",
                 db_root, object);
    }

    restore_test_park(SHARD_TEST_RESTORE_PARK_COMMITTED);
    rmrf(stage);
    (void)fsync_dir(obj_dir);

    objlock_wrunlock(db_root, object);

    if (install_fail) {
        OUT("{\"error\":\"restore committed but %s could not be installed: %s; the backup data is live; re-run restore with force=true\"}\n",
            install_fail, strerror(ierr ? ierr : EIO));
        return 1;
    }
    OUT("{\"status\":\"restored\",\"object\":\"%s\",\"from\":\"%s\"}\n", object, from);
    return 0;
}

/* ========== RECOUNT ========== */

/* shard-stats: walk every keyfile shard under data/kf/, read its
   SlotcaskKfHeader, and report slots/records/load plus a hint when splits
   may be too low. Cheap — reads only the 24-byte header per shard.
   as_table=1 emits ASCII table; as_table=0 emits JSON. */
int cmd_shard_stats(const char *db_root, const char *object, int as_table) {
    Schema sch = load_schema(db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    typedef struct { int shard_id; uint32_t slots; uint32_t records; uint64_t file_bytes; } Row;
    int cap = sch.splits > 0 ? sch.splits : 16;
    Row *rows = calloc(cap, sizeof(Row));
    if (!rows) { OUT("{\"error\":\"oom\"}\n"); return 1; }
    int nrows = 0;

    /* v2 layout: walk data/kf/NNN.kf. Each file is [24B SlotcaskKfHeader]
       [N × 24B SlotcaskKfEntry]. The header carries `total` (live +
       tombstoned) and `deleted` (tombstones); live = total - deleted,
       so we don't need to scan the entry array at all — one pread of
       the header per shard is enough. */
    char kf_dir[PATH_MAX];
    snprintf(kf_dir, sizeof(kf_dir), "%s/kf", data_dir);
    DIR *d1 = opendir(kf_dir);
    if (!d1) { OUT("{\"error\":\"Object [%s] has no kf data\"}\n", object); free(rows); return 1; }
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (e1->d_name[0] == '.') continue;
        size_t nl = strlen(e1->d_name);
        if (nl < 4 || strcmp(e1->d_name + nl - 3, ".kf") != 0) continue;
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", kf_dir, e1->d_name);
        /* Lock-free open+pread+close — same rationale as
           slotcask_sum_kf_totals (slotcask.c): the cache path's
           per-shard rwlock serialises against in-flight writers, which
           is the wrong trade-off for an admin diagnostic that wants
           best-effort reads.  Header reads tolerate one-update tears. */
        int fd = open(path, O_RDONLY);
        if (fd < 0) continue;
        struct stat st; if (fstat(fd, &st) < 0) { close(fd); continue; }
        uint32_t slots = 0, live = 0;
        if ((uint64_t)st.st_size >= SLOTCASK_KF_HDR_SIZE) {
            slots = (uint32_t)(((uint64_t)st.st_size - SLOTCASK_KF_HDR_SIZE)
                               / sizeof(SlotcaskKfEntry));
            SlotcaskKfHeader hdr;
            if (pread(fd, &hdr, sizeof(hdr), 0) == (ssize_t)sizeof(hdr)
                && hdr.magic == SLOTCASK_KF_MAGIC) {
                live = (uint32_t)(hdr.total - hdr.deleted);
            }
        }
        close(fd);
        int sid = (int)strtol(e1->d_name, NULL, 10);
        if (nrows >= cap) {
            cap *= 2;
            Row *t = xrealloc_or_free(rows, cap * sizeof(*t));
            if (!t) { rows = NULL; nrows = 0; break; }
            rows = t;
        }
        rows[nrows++] = (Row){ sid, slots, live, (uint64_t)st.st_size };
    }
    closedir(d1);

    /* Sort by shard_id */
    for (int i = 1; i < nrows; i++) {
        Row k = rows[i]; int j = i - 1;
        while (j >= 0 && rows[j].shard_id > k.shard_id) { rows[j+1] = rows[j]; j--; }
        rows[j+1] = k;
    }

    uint64_t total_records = 0, total_bytes = 0;
    uint32_t max_slots = 0, max_records = 0, min_records = UINT32_MAX;
    for (int i = 0; i < nrows; i++) {
        total_records += rows[i].records;
        total_bytes += rows[i].file_bytes;
        if (rows[i].slots > max_slots) max_slots = rows[i].slots;
        if (rows[i].records > max_records) max_records = rows[i].records;
        if (rows[i].records < min_records) min_records = rows[i].records;
    }

    /* `grows` = log2(max_slots / initial), initial from
       slotcask_default_slots_for_splits(). */
    uint32_t initial = (uint32_t)slotcask_default_slots_for_splits(sch.splits);
    int grows = 0;
    for (uint32_t s = max_slots; initial > 0 && s > initial; s >>= 1) grows++;

    /* Hint: single source of truth with auto-reshard — reuse
       reshard_target_for_count() (src/db/query_maint.c, added for
       auto_reshard_thread()) instead of an independent per-shard
       average/max threshold, so shard-stats and the automatic nightly
       sweep never disagree about whether an object needs a bigger
       `splits`. Stack-local hint_buf (not static): cmd_shard_stats can
       run concurrently across server workers for different objects. */
    const char *hint = NULL;
    char hint_buf[160];
    double avg_load = 0.0;
    uint64_t rps = 0;
    if (nrows > 0) {
        avg_load = (double)total_records / ((double)max_slots * nrows);
        rps = total_records / (uint64_t)nrows;
        int target = reshard_target_for_count((long long)total_records);
        if (target > sch.splits) {
            snprintf(hint_buf, sizeof(hint_buf),
                     "live records (%lu) recommend splits=%d (currently %d) — re-split with vacuum --splits=%d",
                     (unsigned long)total_records, target, sch.splits, target);
            hint = hint_buf;
        } else if (sch.splits >= MAX_SPLITS && rps > 1000000ULL) {
            hint = "at MAX_SPLITS with >1M records/shard — performance may degrade; consider partitioning across objects";
        } else if (min_records > 0 && max_records > min_records * 4) {
            hint = "shard load is skewed — check key distribution";
        }
    }

    /* Keyfile-flavored field names — kf shards aren't data shards. */
    const char *count_key = "keyfiles";
    const char *list_key  = "keyfiles";
    const char *row_key   = "keyfile";

    if (as_table) {
        OUT("splits=%d %s=%d total_records=%lu total_bytes=%lu avg_rec_per_shard=%lu max_grows=%d avg_load=%.3f\n",
            sch.splits, count_key, nrows, (unsigned long)total_records, (unsigned long)total_bytes,
            (unsigned long)rps, grows, avg_load);
        if (nrows != sch.splits)
            OUT("warn: %s (%d) ≠ splits (%d) — partial vacuum/reshard or missing kf files?\n",
                count_key, nrows, sch.splits);
        OUT("  %-8s %-10s %-10s %-8s %-14s\n", row_key, "slots", "records", "load", "bytes");
        for (int i = 0; i < nrows; i++) {
            double load = rows[i].slots ? (double)rows[i].records / (double)rows[i].slots : 0.0;
            OUT("  %-8d %-10u %-10u %-8.3f %-14lu\n",
                rows[i].shard_id, rows[i].slots, rows[i].records, load,
                (unsigned long)rows[i].file_bytes);
        }
        if (hint) OUT("hint: %s\n", hint);
    } else {
        OUT("{\"splits\":%d,\"%s\":%d,\"total_records\":%lu,\"total_bytes\":%lu,\"%s\":[",
            sch.splits, count_key, nrows, (unsigned long)total_records,
            (unsigned long)total_bytes, list_key);
        for (int i = 0; i < nrows; i++) {
            double load = rows[i].slots ? (double)rows[i].records / (double)rows[i].slots : 0.0;
            OUT("%s{\"%s\":%d,\"slots\":%u,\"records\":%u,\"load\":%.3f,\"bytes\":%lu}",
                i ? "," : "", row_key, rows[i].shard_id, rows[i].slots, rows[i].records,
                load, (unsigned long)rows[i].file_bytes);
        }
        OUT("],\"avg_rec_per_shard\":%lu,\"max_grows\":%d", (unsigned long)rps, grows);
        if (hint) OUT(",\"hint\":\"%s\"", hint);
        OUT("}\n");
    }
    free(rows);
    return 0;
}

/* recount: report current live-record count. For v2 this is kf-derived
   (sum per-shard kf headers — total - deleted = live), so the result
   matches what get_live_count returns and `set_count` is a no-op. The
   command is functionally redundant on v2 since the kf headers are
   already authoritative; kept as a fast diagnostic / parity wrapper.
    v1 still has the full text-counts file write path below. */
int cmd_recount(const char *db_root, const char *object) {
    Schema sch = load_schema(db_root, object);
    if (!sch.splits) {
        OUT("{\"error\":\"object not found\"}\n");
        return 1;
    }

    SlotcaskSchemaInfo info = {
        .splits = sch.splits, .slot_size = sch.slot_size,
        .streams = sch.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        OUT("{\"error\":\"object not open\"}\n");
        return 1;
    }
    uint64_t total_hdr = 0, deleted_hdr = 0;
    if (slotcask_sum_kf_totals(sdb, &total_hdr, &deleted_hdr) != 0) {
        OUT("{\"error\":\"recount failed\"}\n");
        return 1;
    }
    int live = (int)(total_hdr > deleted_hdr ? total_hdr - deleted_hdr : 0);
    OUT("{\"count\":%d}\n", live);
    return 0;
}

/* ========== TRUNCATE ========== */

int cmd_truncate(const char *db_root, const char *object) {
    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);
    struct stat st;
    if (stat(obj_dir, &st) != 0) {
        fprintf(stderr, "Error: Object [%s] not found\n", object);
        return 1;
    }
    invalidate_idx_cache(db_root, object);
    counts_invalidate(db_root, object);

    /* Drop the cached slotcask handle so the rmrf below doesn't tug on
       live mmaps. The data/ wipe further down handles the actual file
       removal — kf + stream files all live under data/. */
    slotcask_registry_invalidate(db_root, object);

    /* Only delete data, metadata, and index data — preserve config files */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/data", obj_dir);
    rmrf(path);
    snprintf(path, sizeof(path), "%s/metadata", obj_dir);
    rmrf(path);
    /* Delete index .idx files but keep index.conf */
    snprintf(path, sizeof(path), "%s/indexes", obj_dir);
    DIR *d = opendir(path);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            size_t nlen = strlen(e->d_name);
            if (nlen > 4 && strcmp(e->d_name + nlen - 4, ".idx") == 0) {
                char fp[PATH_MAX];
                snprintf(fp, sizeof(fp), "%s/%s", path, e->d_name);
                unlink(fp);
            }
        }
        closedir(d);
    }
    /* Recreate data and metadata dirs */
    snprintf(path, sizeof(path), "%s/data", obj_dir);
    mkdirp(path);
    snprintf(path, sizeof(path), "%s/metadata", obj_dir);
    mkdirp(path);

    set_count(db_root, object, 0);
    LOG_AUDIT(LOG_SUB_CONFIG, "TRUNCATE %s", object);
    OUT("{\"status\":\"truncated\",\"object\":\"%s\"}\n", object);
    return 0;
}
