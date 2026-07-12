#include "types.h"
#include "slotcask.h"
#include "trigram.h"
#include <dirent.h>

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
                          void *ctx) {
    (void)slot; (void)hash16; (void)key; (void)klen; (void)vlen;
    TgEstimateCtx *c = (TgEstimateCtx *)ctx;
    if (c->sampled >= c->max_sample) return -1;
    const TypedField *f = &c->ts->fields[c->field_index];
    const uint8_t *vbase = (const uint8_t *)value + f->offset;
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
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        OUT("{\"error\":\"slotcask open failed\"}\n");
        return 1;
    }

    /* Walk shards in order, accumulating up to TG_ESTIMATE_SAMPLE
       records. Stops early once the cap is hit. */
    TgEstimateCtx c = {
        .field_index = fi, .ts = ts,
        .sampled = 0, .distinct_sum = 0, .max_sample = TG_ESTIMATE_SAMPLE,
    };
    for (int s = 0; s < sch.splits && c.sampled < c.max_sample; s++) {
        slotcask_walk_one_shard_slots(sdb, s, tg_estimate_cb, &c);
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
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    int dropped = 0;
    if (sdb) {
        (void)slotcask_compact_segs(sdb, &dropped);
        /* kf-compact: rebuild each shard to drop tombstones so kf->deleted
           returns to 0 (kf-derived counts model — there's no separate
           counts file to lie via anymore). */
        (void)slotcask_compact_kf(sdb);
    }
    reset_deleted_count(db_root, object);  /* v1 only; no-op for v2 */
    OUT("{\"status\":\"vacuumed\",\"cleaned\":%d}\n", dropped);
    return 0;
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
        FILE *f = fopen(seq_path, "w");
        if (f) { fprintf(f, "0\n"); fclose(f); }
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

/* Copy one regular file. Preserves mode bits. Returns 0 on success. */
static int copy_file(const char *src, const char *dst, mode_t mode) {
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

/* Recursively copy a directory tree. No shell, no path interpolation —
   every path is built component-wise, so the source-object name (which
   originates from a JSON request) cannot inject shell metacharacters.
   Replaces the previous `system("cp -r ...")` invocations that CodeQL
   flagged as "uncontrolled data used in OS command". */
static void cprf(const char *src, const char *dst) {
    /* TOCTOU-safe: open by path once, then fstat the fd and fdopendir the
       same fd. Avoids the classic lstat-then-opendir race where a symlink
       swap between the two calls would steer the recursive copy at a
       different filesystem object. O_NOFOLLOW rejects symlinks at the
       open boundary, which is consistent with the existing "skip symlinks"
       contract documented below. */
    int fd = open(src, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) return;
    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return; }

    if (S_ISDIR(st.st_mode)) {
        mkdirp(dst);
        DIR *d = fdopendir(fd);          /* takes ownership of fd */
        if (!d) { close(fd); return; }
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.' &&
                (e->d_name[1] == '\0' ||
                 (e->d_name[1] == '.' && e->d_name[2] == '\0')))
                continue;
            char child_src[PATH_MAX], child_dst[PATH_MAX];
            snprintf(child_src, sizeof(child_src), "%s/%s", src, e->d_name);
            snprintf(child_dst, sizeof(child_dst), "%s/%s", dst, e->d_name);
            cprf(child_src, child_dst);
        }
        closedir(d);                      /* closes the underlying fd */
    } else if (S_ISREG(st.st_mode)) {
        close(fd);                        /* copy_file opens its own fd */
        copy_file(src, dst, st.st_mode);
    } else {
        close(fd);
    }
    /* Symlinks, fifos, devices etc. are ignored — backup targets are
       always shard-db's own regular files / directories. */
}

int cmd_backup(const char *db_root, const char *object) {
    char src_dir[PATH_MAX], bak_dir[PATH_MAX];
    snprintf(src_dir, sizeof(src_dir), "%s/%s", db_root, object);
    struct stat st;
    if (stat(src_dir, &st) != 0) {
        fprintf(stderr, "Error: Object [%s] not found\n", object);
        return 1;
    }

    /* Create timestamped backup directory */
    time_t now = time(NULL);
    struct tm tbuf; struct tm *t = localtime_r(&now, &tbuf);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y%m%d_%H%M%S", t);
    snprintf(bak_dir, sizeof(bak_dir), "%s/%s/backup/%s", db_root, object, ts);
    mkdirp(bak_dir);

    /* Recursively copy data/, indexes/, and metadata/. v2 stores
       kf shards + segments under the same data/ umbrella, so a single
       cprf("data") captures both layouts. */
    Schema sch = load_schema(db_root, object);
    char src_sub[PATH_MAX], dst_sub[PATH_MAX];
    const char *subs[] = { "data", "indexes", "metadata" };
    for (size_t i = 0; i < sizeof(subs) / sizeof(subs[0]); i++) {
        snprintf(src_sub, sizeof(src_sub), "%s/%s", src_dir, subs[i]);
        snprintf(dst_sub, sizeof(dst_sub), "%s/%s", bak_dir, subs[i]);
        cprf(src_sub, dst_sub);
    }

    /* Schema capture — make the backup directory self-contained. fields.conf
       has the typed-field layout; object.json carries splits + max_key (which
       live in the global schema.conf, not under <obj>/). Without these,
       cmd_restore couldn't recreate the object on a fresh DB. */
    char src_fields[PATH_MAX], dst_fields[PATH_MAX];
    snprintf(src_fields, sizeof(src_fields), "%s/fields.conf", src_dir);
    snprintf(dst_fields, sizeof(dst_fields), "%s/fields.conf", bak_dir);
    if (stat(src_fields, &st) == 0) copy_file(src_fields, dst_fields, st.st_mode);

    char meta_path[PATH_MAX];
    snprintf(meta_path, sizeof(meta_path), "%s/object.json", bak_dir);
    FILE *mf = fopen(meta_path, "w");
    if (mf) {
        /* storage_version slot kept in the backup metadata for forward
           compatibility — every object the daemon writes is v2. */
        fprintf(mf, "{\"splits\":%d,\"max_key\":%d,\"storage_version\":2,\"streams\":%d}\n",
                sch.splits, sch.max_key, sch.streams);
        fclose(mf);
    }

    OUT("{\"status\":\"backed_up\",\"path\":\"%s\"}\n", bak_dir);
    return 0;
}

/* Parse object.json written by cmd_backup. splits + max_key are required;
   storage_version + streams are optional (older backups predate them).
   Returns 1 on success. Defaults out_storage_version to 1 / out_streams
   to 0 when missing, matching legacy semantics. */
static int parse_object_meta(const char *path, int *out_splits, int *out_max_key,
                              int *out_storage_version, int *out_streams) {
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
    if (splits < 0 || max_key < 0) return 0;
    *out_splits = splits;
    *out_max_key = max_key;
    if (out_storage_version) *out_storage_version = sv;
    if (out_streams)         *out_streams = streams;
    return 1;
}

/* Append a `dir:object:splits:max_key[:storage_version:streams]` line to
   schema.conf if it isn't already there. Caller holds objlock_wrlock.
   storage_version=0 omits the trailing fields (legacy v1 form).
   Returns 0 on success, -1 on IO failure. */
static int ensure_schema_conf_line(const char *db_root, const char *object,
                                   int splits, int max_key,
                                   int storage_version, int streams) {
    const char *dir = db_root + strlen(g_db_root);
    if (*dir == '/') dir++;

    char conf[PATH_MAX];
    snprintf(conf, sizeof(conf), "%s/schema.conf", g_db_root);

    char prefix[512];
    int pfxlen = snprintf(prefix, sizeof(prefix), "%s:%s:", dir, object);

    FILE *f = fopen(conf, "a+");
    if (!f) {
        LOG_ERROR(LOG_SUB_CONFIG, "ensure_schema_conf_line: fopen(%s) failed: %s", conf, strerror(errno));
        return -1;
    }
    int lockfd = fileno(f);
    flock(lockfd, LOCK_EX);

    rewind(f);
    char line[512];
    int found = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, prefix, pfxlen) == 0) { found = 1; break; }
    }
    if (!found) {
        fseek(f, 0, SEEK_END);
        if (storage_version >= 2)
            fprintf(f, "%s%d:%d:%d:%d\n", prefix, splits, max_key,
                    storage_version, streams);
        else
            fprintf(f, "%s%d:%d\n", prefix, splits, max_key);
    }
    flock(lockfd, LOCK_UN);
    fclose(f);
    return 0;
}

/* ========== RESTORE ==========
   Symmetric to backup: copies data/ + indexes/ + metadata/ + fields.conf
   from `<obj>/backup/<from>` over the live tree, and ensures the
   schema.conf line is in place from object.json.
   Refuses if any live state would conflict, unless force=1. Holds the
   object's write lock for the whole operation; invalidates ucache + bt
   cache + idx cache + schema caches before the swap so the next reader
   sees the new mappings. */
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

    objlock_wrlock(db_root, object);

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

    /* Schema reconciliation. If the backup carries object.json, cross-check
       it against the live schema.conf entry. Mismatch → refuse without
       force; missing live entry → append (the object exists on disk but
       got orphaned from schema.conf, exactly the failure mode this
       protects against). Backups from before this feature have no
       object.json — accept and skip. */
    char meta_src[PATH_MAX];
    snprintf(meta_src, sizeof(meta_src), "%s/object.json", src_dir);
    int bak_splits = 0, bak_max_key = 0, bak_sv = 1, bak_streams = 0;
    int have_meta = parse_object_meta(meta_src, &bak_splits, &bak_max_key,
                                       &bak_sv, &bak_streams);
    if (have_meta) {
        Schema live = load_schema(db_root, object);
        if (live.splits > 0 && (live.splits != bak_splits || live.max_key != bak_max_key) && !force) {
            objlock_wrunlock(db_root, object);
            OUT("{\"error\":\"schema mismatch: live splits=%d max_key=%d, "
                "backup splits=%d max_key=%d (use force=true to overwrite)\"}\n",
                live.splits, live.max_key, bak_splits, bak_max_key);
            return 1;
        }
        if (ensure_schema_conf_line(db_root, object, bak_splits, bak_max_key,
                                     bak_sv, bak_streams) != 0) {
            objlock_wrunlock(db_root, object);
            OUT("{\"error\":\"failed to update schema.conf\"}\n");
            return 1;
        }
    }

    /* fields.conf reconciliation. Same logic as schema: if the backup has
       a fields.conf, cross-check against live. Differs → force-required;
       missing live → install from backup. */
    char fields_src[PATH_MAX], fields_dst[PATH_MAX];
    snprintf(fields_src, sizeof(fields_src), "%s/fields.conf", src_dir);
    snprintf(fields_dst, sizeof(fields_dst), "%s/fields.conf", obj_dir);
    /* Open-then-fstat instead of stat-then-open. The earlier pattern was a
       TOCTOU: between the stat() and fopen(), a malicious symlink swap
       could redirect us to /etc/passwd or similar. Opening first and
       fstat'ing the resulting fd makes the type/size check authoritative
       on the actual stream we're going to read. */
    FILE *fa_pre = fopen(fields_src, "re");
    if (fa_pre) {
        struct stat sa;
        if (fstat(fileno(fa_pre), &sa) == 0 && S_ISREG(sa.st_mode)) {
            FILE *fb_pre = fopen(fields_dst, "re");
            int dst_present = 0;
            struct stat sb;
            if (fb_pre && fstat(fileno(fb_pre), &sb) == 0 && S_ISREG(sb.st_mode))
                dst_present = 1;
            if (dst_present && !force) {
                /* Compare contents — same size + same bytes = identical. */
                int same = 0;
                if (sb.st_size == sa.st_size) {
                    same = 1;
                    int ca, cb;
                    while ((ca = fgetc(fa_pre)) != EOF && (cb = fgetc(fb_pre)) != EOF) {
                        if (ca != cb) { same = 0; break; }
                    }
                }
                if (fb_pre) fclose(fb_pre);
                fclose(fa_pre);
                if (!same) {
                    objlock_wrunlock(db_root, object);
                    OUT("{\"error\":\"fields.conf differs between live and backup "
                        "(use force=true to overwrite)\"}\n");
                    return 1;
                }
            } else {
                if (fb_pre) fclose(fb_pre);
                fclose(fa_pre);
            }
            copy_file(fields_src, fields_dst, sa.st_mode);
        } else {
            fclose(fa_pre);
        }
    }

    /* Drop caches first so readers can't pin the old mappings while we swap.
       fcache_invalidate handles data shards; the btree page cache holds open
       fd+mmap per <field>/<NNN>.idx, so walk indexes/ explicitly (same
       pattern as index.c::reindex_clean_legacy). */
    fcache_invalidate(obj_dir);
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
                    }
                    closedir(sd);
                } else {
                    /* Non-dir at this path (legacy single-file layout, or
                       a stray) — invalidate cache by direct path. */
                    btree_cache_invalidate(field_dir);
                }
            }
            closedir(d);
        }
    }

    /* v2: drop slotcask registry first so the upcoming wipe doesn't
       tug on live mmaps. The data/ wipe + cprf in the next loop
       handles the kf + stream files (they live under data/). */
    if (have_meta && bak_sv == 2) {
        slotcask_registry_invalidate(db_root, object);
    }

    /* Wipe live subtrees, then copy from backup. */
    for (size_t i = 0; i < sizeof(subs) / sizeof(subs[0]); i++) {
        snprintf(dst_sub, sizeof(dst_sub), "%s/%s", obj_dir, subs[i]);
        rmrf(dst_sub);
        snprintf(src_sub, sizeof(src_sub), "%s/%s", src_dir, subs[i]);
        if (stat(src_sub, &st) == 0) cprf(src_sub, dst_sub);
    }

    objlock_wrunlock(db_root, object);

    OUT("{\"status\":\"restored\",\"object\":\"%s\",\"from\":\"%s\"}\n", object, from);
    return 0;
}

/* ========== RECOUNT ========== */

/* shard-stats: walk every shard file under data/, read each ShardHeader, report slots/records/load
   plus a hint when splits may be too low. Cheap — reads only 32B per shard.
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

    /* `grows` = log2(max_slots / initial). v1 uses INITIAL_SLOTS=256; v2 uses
       the splits-tier initial from slotcask_default_slots_for_splits(). */
    uint32_t initial = (uint32_t)slotcask_default_slots_for_splits(sch.splits);
    int grows = 0;
    for (uint32_t s = max_slots; initial > 0 && s > initial; s >>= 1) grows++;

    /* Hint: in v1, sizing is driven by records-per-shard (sweet spot 78K-200K).
       In v2 the kf auto-resplits, so high recs/shard ≠ broken — but it does mean
       the kf paid inline doubling cost. Same advice ("vacuum --splits=N") fits
       both: a higher `splits` up-front avoids resplit work. */
    const char *hint = NULL;
    double avg_load = 0.0;
    uint64_t rps = 0;
    if (nrows > 0) {
        avg_load = (double)total_records / ((double)max_slots * nrows);
        rps = total_records / (uint64_t)nrows;
        if (rps > 1000000ULL) {
            hint = (sch.splits < MAX_SPLITS)
                ? "records-per-shard past sweet spot (>1M) — re-split with vacuum --splits=N"
                : "at MAX_SPLITS with >1M records/shard — performance may degrade; consider partitioning across objects";
        } else if (rps > 500000ULL && sch.splits < MAX_SPLITS) {
            hint = "records-per-shard approaching upper band (>500K) — consider vacuum --splits=N";
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
int cmd_rebuild_kf(const char *db_root, const char *object) {
    Schema sch = load_schema(db_root, object);
    if (!sch.splits) { OUT("{\"error\":\"object not found\"}\n"); return 1; }
    SlotcaskSchemaInfo info = {
        .splits = sch.splits, .slot_size = sch.slot_size,
        .streams = sch.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"object not open\"}\n"); return 1; }
    int repaired = slotcask_rebuild_kf(sdb);
    if (repaired < 0) { OUT("{\"error\":\"rebuild-kf failed (oom)\"}\n"); return 1; }
    OUT("{\"status\":\"ok\",\"repaired\":%d}\n", repaired);
    return 0;
}

int cmd_recount(const char *db_root, const char *object) {
    Schema sch = load_schema(db_root, object);

    SlotcaskSchemaInfo info = {
        .splits = sch.splits, .slot_size = sch.slot_size,
        .streams = sch.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    uint64_t total_hdr = 0, deleted_hdr = 0;
    if (sdb) slotcask_sum_kf_totals(sdb, &total_hdr, &deleted_hdr);
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
    fcache_invalidate(obj_dir);
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
