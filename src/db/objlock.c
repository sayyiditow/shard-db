#include "types.h"
#include "slotcask.h"
#include "query_internal.h"

/* ========== Per-object rwlock ==========
   Coordinates normal access (shared) vs. rebuild (exclusive).
   - Writers AND readers (insert/delete/update/bulk/get/find/exists/count/...):
     rdlock. Every mode that can reach slotcask_registry_get() needs this,
     not just writes: the registry hands out a raw SlotcaskDb* with no
     reference counting, and slotcask_registry_invalidate() (called by
     rebuild/vacuum below, under wrlock) frees that struct outright. Without
     rdlock here, a concurrent reader's registry_get() can race the free and
     dereference a dangling pointer — this is a genuine use-after-free, not
     just a data race on inert bytes.
   - Rebuild (vacuum/add-field/compact/...): wrlock — blocks all readers and
     writers for the duration, so it's safe to invalidate + free the
     registry entry once it holds the lock.

   This is orthogonal to slotcask's own read-side retry loop, which
   validates hash+key after a slot read and retries on a concurrent move
   (MAP_SHARED gives a live view of slot *contents* within a still-open
   SlotcaskDb). That mechanism handles in-object data races and needs no
   lock. It does nothing for, and was never meant to cover, the
   SlotcaskDb struct itself being freed out from under a reader — that's
   what this lock is for.

   Entries live for process lifetime — no eviction needed. Objects are
   created rarely and the pthread_rwlock_t memory is tiny. Safe to reuse
   the same entry if an object is truncated and recreated with the same
   name.
*/

/* ObjLockEntry + OBJLOCK_BUCKETS moved to shard_db_internal.h;
   g_objlocks, g_objlock_table_lock moved to ShardDb struct */

static uint32_t obj_str_hash(const char *s) {
    return (uint32_t)XXH3_64bits(s, strlen(s));
}

void objlock_init(void) {
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;
    for (int i = 0; i < OBJLOCK_BUCKETS; i++) {
        atomic_init(&g_objlocks[i].used, 0);
        g_objlocks[i].name[0] = '\0';
    }
}

/* Find or create the rwlock for a given object. Returns NULL only if
   the table is completely full (OBJLOCK_BUCKETS objects), which would
   mean thousands of distinct objects — not a realistic scenario. */
static pthread_rwlock_t *get_lock(const char *db_root, const char *object) {
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;
    char key[512];
    snprintf(key, sizeof(key), "%s:%s", db_root, object);
    uint32_t idx = obj_str_hash(key) % OBJLOCK_BUCKETS;

    /* Fast path: lockless probe for existing entry. acquire-load on
       `used` pairs with the slow-path release-store so strcmp on
       `name` reads a coherent snapshot. */
    for (int i = 0; i < OBJLOCK_BUCKETS; i++) {
        int slot = (idx + i) % OBJLOCK_BUCKETS;
        if (!atomic_load_explicit(&g_objlocks[slot].used, memory_order_acquire))
            break;
        if (strcmp(g_objlocks[slot].name, key) == 0)
            return &g_objlocks[slot].rwlock;
    }

    /* Slow path: take table lock, re-probe, and insert if still missing */
    pthread_mutex_lock(&g_objlock_table_lock);
    for (int i = 0; i < OBJLOCK_BUCKETS; i++) {
        int slot = (idx + i) % OBJLOCK_BUCKETS;
        int u = atomic_load_explicit(&g_objlocks[slot].used,
                                      memory_order_relaxed);
        if (u && strcmp(g_objlocks[slot].name, key) == 0) {
            pthread_mutex_unlock(&g_objlock_table_lock);
            return &g_objlocks[slot].rwlock;
        }
        if (!u) {
            strncpy(g_objlocks[slot].name, key, sizeof(g_objlocks[slot].name) - 1);
            g_objlocks[slot].name[sizeof(g_objlocks[slot].name) - 1] = '\0';
            /* Default-attribute (reader-preferring) rwlock: objlock's API
               permits recursive read locks, which
               PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP does not support
               safely (see docs/plans/2026-07-29-cache-rwlock-writer-preference.md),
               so this stays unchanged even where the four file caches switch
               to writer-preferring. */
            pthread_rwlock_init(&g_objlocks[slot].rwlock, NULL);
            /* Release ordering: name + rwlock_init complete before
               any concurrent fast-path acquire-load sees used==1. */
            atomic_store_explicit(&g_objlocks[slot].used, 1, memory_order_release);
            pthread_mutex_unlock(&g_objlock_table_lock);
            return &g_objlocks[slot].rwlock;
        }
    }
    pthread_mutex_unlock(&g_objlock_table_lock);
    LOG_ERROR(LOG_SUB_SERVER, "objlock get_lock: table full (%d buckets), object '%s' will run WITHOUT rwlock protection", OBJLOCK_BUCKETS, key);
    return NULL;
}

void objlock_rdlock(const char *db_root, const char *object) {
    pthread_rwlock_t *l = get_lock(db_root, object);
    if (l) pthread_rwlock_rdlock(l);
}

void objlock_rdunlock(const char *db_root, const char *object) {
    pthread_rwlock_t *l = get_lock(db_root, object);
    if (l) pthread_rwlock_unlock(l);
}

void objlock_wrlock(const char *db_root, const char *object) {
    pthread_rwlock_t *l = get_lock(db_root, object);
    if (l) pthread_rwlock_wrlock(l);
}

void objlock_wrunlock(const char *db_root, const char *object) {
    pthread_rwlock_t *l = get_lock(db_root, object);
    if (l) pthread_rwlock_unlock(l);
}

int db_root_lock_acquire(const char *db_root, int *out_fd) {
    if (!db_root || !out_fd) return -1;
    *out_fd = -1;
    char lockpath[PATH_MAX];
    int n = snprintf(lockpath, sizeof(lockpath), "%s/.shard-db.lock", db_root);
    if (n < 0 || n >= (int)sizeof(lockpath)) {
        fprintf(stderr, "shard-db: DB root lock path is too long\n");
        return -1;
    }
    int fd = open(lockpath, O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "shard-db: cannot open DB root lock %s: %s\n",
                lockpath, strerror(errno));
        return -1;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        fprintf(stderr,
                "shard-db: DB root %s is already open by another process "
                "(lock held on %s). Stop it first with './shard-db stop'.\n",
                db_root, lockpath);
        close(fd);
        return -1;
    }
    *out_fd = fd;
    return 0;
}

void db_root_lock_release(int *fd) {
    if (!fd || *fd < 0) return;
    (void)flock(*fd, LOCK_UN);
    close(*fd);
    *fd = -1;
}

/* ========== Rebuild transaction ========== */

struct RebuildTxn {
    char db_root[PATH_MAX];
    char object[256];
    char obj_dir[PATH_MAX];
    char preparing[PATH_MAX];
    char active[PATH_MAX];
    char done[PATH_MAX];
    char data_dir[PATH_MAX];
    char legacy_data[PATH_MAX];
    char fields_path[PATH_MAX];
    char fields_rollback[PATH_MAX];
    int old_splits;
    int old_streams;
    int indexes_may_change;
};

static int path_join2(char *out, size_t cap, const char *a, const char *b) {
    int n = snprintf(out, cap, "%s/%s", a, b);
    return n >= 0 && (size_t)n < cap ? 0 : -1;
}

static RebuildTxn *rebuild_txn_alloc(const char *db_root,
                                     const char *object) {
    if (!db_root || !object || !object[0] || strlen(object) >= 256) return NULL;
    RebuildTxn *txn = calloc(1, sizeof(*txn));
    if (!txn) return NULL;
    if (snprintf(txn->db_root, sizeof(txn->db_root), "%s", db_root) >=
            (int)sizeof(txn->db_root) ||
        snprintf(txn->object, sizeof(txn->object), "%s", object) >=
            (int)sizeof(txn->object) ||
        path_join2(txn->obj_dir, sizeof(txn->obj_dir), db_root, object) != 0 ||
        path_join2(txn->preparing, sizeof(txn->preparing), txn->obj_dir,
                   ".rebuild_txn.preparing") != 0 ||
        path_join2(txn->active, sizeof(txn->active), txn->obj_dir,
                   ".rebuild_txn.active") != 0 ||
        path_join2(txn->done, sizeof(txn->done), txn->obj_dir,
                   ".rebuild_txn.done") != 0 ||
        path_join2(txn->data_dir, sizeof(txn->data_dir), txn->obj_dir,
                   "data") != 0 ||
        path_join2(txn->legacy_data, sizeof(txn->legacy_data), txn->active,
                   "data") != 0 ||
        path_join2(txn->fields_path, sizeof(txn->fields_path), txn->obj_dir,
                   "fields.conf") != 0 ||
        path_join2(txn->fields_rollback, sizeof(txn->fields_rollback),
                   txn->active, "fields.conf.rollback") != 0) {
        free(txn);
        return NULL;
    }
    return txn;
}

static int verify_absent(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0 && errno == ENOENT) return 0;
    if (errno != ENOENT)
        LOG_ERROR(LOG_SUB_SLOTCASK, "rebuild txn: %s was not removed", path);
    return -1;
}

static int remove_non_authoritative_dir(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return errno == ENOENT ? 0 : -1;
    if (!S_ISDIR(st.st_mode)) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "rebuild txn: refusing non-directory transaction path %s",
                  path);
        return -1;
    }
    rmrf(path);
    return verify_absent(path);
}

static int copy_regular_file_atomic(const char *src, const char *dst,
                                    const char *tmp) {
    int in = open(src, O_RDONLY | O_NOFOLLOW);
    if (in < 0) return -1;
    struct stat st;
    if (fstat(in, &st) != 0 || !S_ISREG(st.st_mode)) { close(in); return -1; }
    int out = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, st.st_mode & 0777);
    if (out < 0) { close(in); return -1; }
    char buf[16384];
    int rc = 0;
    for (;;) {
        ssize_t nr = read(in, buf, sizeof(buf));
        if (nr == 0) break;
        if (nr < 0) { if (errno == EINTR) continue; rc = -1; break; }
        ssize_t off = 0;
        while (off < nr) {
            ssize_t nw = write(out, buf + off, (size_t)(nr - off));
            if (nw < 0) {
                if (errno == EINTR) continue;
                rc = -1;
                break;
            }
            if (nw == 0) { rc = -1; break; }
            off += nw;
        }
        if (rc != 0) break;
    }
    if (close(in) != 0) rc = -1;
    if (close(out) != 0) rc = -1;
    if (rc == 0 && rename(tmp, dst) != 0) rc = -1;
    if (rc != 0) unlink(tmp);
    return rc;
}

static int write_meta(RebuildTxn *txn) {
    char meta[PATH_MAX], tmp[PATH_MAX];
    if (path_join2(meta, sizeof(meta), txn->preparing, "meta") != 0 ||
        path_join2(tmp, sizeof(tmp), txn->preparing, "meta.tmp") != 0)
        return -1;
    FILE *f = fopen(tmp, "w");
    if (!f) return -1;
    int rc = fprintf(f,
                     "version=1\nold_splits=%d\nold_streams=%d\n"
                     "indexes_may_change=%d\n",
                     txn->old_splits, txn->old_streams,
                     txn->indexes_may_change ? 1 : 0) < 0 ? -1 : 0;
    if (fclose(f) != 0) rc = -1;
    if (rc == 0 && rename(tmp, meta) != 0) rc = -1;
    if (rc != 0) unlink(tmp);
    return rc;
}

static int valid_old_splits(int n) {
    return n > 0 && n <= 4096 && (n & (n - 1)) == 0;
}

static int parse_meta(RebuildTxn *txn) {
    char path[PATH_MAX];
    if (path_join2(path, sizeof(path), txn->active, "meta") != 0) return -1;
    int fd = open(path, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0 ||
        st.st_size > 1024) {
        close(fd);
        return -1;
    }
    FILE *f = fdopen(fd, "r");
    if (!f) {
        close(fd);
        return -1;
    }
    int seen_version = 0, seen_splits = 0, seen_streams = 0, seen_indexes = 0;
    int version = 0, splits = 0, streams = 0, indexes = -1;
    int bad = 0;
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        if (!strchr(line, '\n')) { bad = 1; break; }
        line[strcspn(line, "\r\n")] = '\0';
        char extra;
        if (sscanf(line, "version=%d%c", &version, &extra) == 1) {
            if (seen_version++) bad = 1;
        } else if (sscanf(line, "old_splits=%d%c", &splits, &extra) == 1) {
            if (seen_splits++) bad = 1;
        } else if (sscanf(line, "old_streams=%d%c", &streams, &extra) == 1) {
            if (seen_streams++) bad = 1;
        } else if (sscanf(line, "indexes_may_change=%d%c", &indexes,
                          &extra) == 1) {
            if (seen_indexes++) bad = 1;
        } else {
            bad = 1;
        }
        if (bad) break;
    }
    if (ferror(f)) bad = 1;
    fclose(f);
    if (bad || seen_version != 1 || seen_splits != 1 || seen_streams != 1 ||
        seen_indexes != 1 || version != 1 || !valid_old_splits(splits) ||
        streams <= 0 || (indexes != 0 && indexes != 1)) return -1;
    txn->old_splits = splits;
    txn->old_streams = streams;
    txn->indexes_may_change = indexes;
    return 0;
}

RebuildTxn *rebuild_txn_begin(const char *db_root, const char *object,
                              int old_splits, int old_streams,
                              int indexes_may_change) {
    if (!valid_old_splits(old_splits) || old_streams <= 0) return NULL;
    RebuildTxn *txn = rebuild_txn_alloc(db_root, object);
    if (!txn) return NULL;
    txn->old_splits = old_splits;
    txn->old_streams = old_streams;
    txn->indexes_may_change = indexes_may_change ? 1 : 0;

    struct stat st;
    if (lstat(txn->active, &st) == 0 || errno != ENOENT ||
        remove_non_authoritative_dir(txn->preparing) != 0 ||
        remove_non_authoritative_dir(txn->done) != 0 ||
        mkdir(txn->preparing, 0755) != 0) {
        rebuild_txn_free(txn);
        return NULL;
    }
    char rollback[PATH_MAX], rollback_tmp[PATH_MAX];
    if (path_join2(rollback, sizeof(rollback), txn->preparing,
                   "fields.conf.rollback") != 0 ||
        path_join2(rollback_tmp, sizeof(rollback_tmp), txn->preparing,
                   "fields.conf.rollback.tmp") != 0 ||
        copy_regular_file_atomic(txn->fields_path, rollback, rollback_tmp) != 0 ||
        write_meta(txn) != 0 ||
        rename(txn->preparing, txn->active) != 0) {
        (void)remove_non_authoritative_dir(txn->preparing);
        rebuild_txn_free(txn);
        return NULL;
    }
    slotcask_registry_invalidate(db_root, object);
    if (rename(txn->data_dir, txn->legacy_data) != 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "rebuild txn: cannot stage data for %s/%s: %s",
                  db_root, object, strerror(errno));
        (void)rebuild_txn_abort(txn);
        rebuild_txn_free(txn);
        return NULL;
    }
    return txn;
}

const char *rebuild_txn_legacy_root(const RebuildTxn *txn) {
    return txn ? txn->active : NULL;
}

int rebuild_txn_commit(RebuildTxn *txn) {
    if (!txn) return -1;
    if (rename(txn->active, txn->done) != 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "rebuild txn: commit rename failed: %s",
                  strerror(errno));
        return -1;
    }
    return 0;
}

int rebuild_txn_abort(RebuildTxn *txn) {
    if (!txn) return -1;
    struct stat active_st, backup_st, live_st, fields_st;
    if (lstat(txn->active, &active_st) != 0 || !S_ISDIR(active_st.st_mode) ||
        lstat(txn->fields_rollback, &fields_st) != 0 ||
        !S_ISREG(fields_st.st_mode)) return -1;

    int has_backup = lstat(txn->legacy_data, &backup_st) == 0;
    if (!has_backup && errno != ENOENT) return -1;
    int has_live = lstat(txn->data_dir, &live_st) == 0;
    if (!has_live && errno != ENOENT) return -1;
    if ((has_backup && !S_ISDIR(backup_st.st_mode)) ||
        (has_live && !S_ISDIR(live_st.st_mode))) return -1;

    slotcask_registry_invalidate(txn->db_root, txn->object);
    if (has_backup) {
        if (has_live) {
            rmrf(txn->data_dir);
            if (verify_absent(txn->data_dir) != 0) return -1;
        }
        if (rename(txn->legacy_data, txn->data_dir) != 0) return -1;
    } else if (!has_live) {
        return -1;
    }

    char fields_tmp[PATH_MAX];
    if (snprintf(fields_tmp, sizeof(fields_tmp), "%s.rollback.tmp.%d",
                 txn->fields_path, (int)getpid()) >= (int)sizeof(fields_tmp) ||
        copy_regular_file_atomic(txn->fields_rollback, txn->fields_path,
                                 fields_tmp) != 0) return -1;
    invalidate_schema_caches(txn->db_root, txn->object);
    if (update_schema_conf_splits_streams(txn->db_root, txn->object,
                                          txn->old_splits,
                                          txn->old_streams) != 0) return -1;
    invalidate_schema_caches(txn->db_root, txn->object);
    invalidate_idx_cache(txn->db_root, txn->object);
    slotcask_registry_invalidate(txn->db_root, txn->object);
    if (txn->indexes_may_change) {
        int ignored = 0;
        if (reindex_object_checked(txn->db_root, txn->object, 0,
                                   &ignored) != 0) return -1;
    }
    slotcask_registry_invalidate(txn->db_root, txn->object);
    if (rename(txn->active, txn->done) != 0) return -1;
    rebuild_txn_cleanup_committed(txn);
    return verify_absent(txn->done);
}

void rebuild_txn_cleanup_committed(RebuildTxn *txn) {
    if (!txn) return;
    struct stat st;
    if (lstat(txn->done, &st) != 0) return;
    if (!S_ISDIR(st.st_mode)) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "rebuild txn: refusing non-directory done path %s", txn->done);
        return;
    }
    rmrf(txn->done);
    if (verify_absent(txn->done) != 0)
        LOG_ERROR(LOG_SUB_SLOTCASK, "rebuild txn: committed cleanup failed for %s",
                  txn->done);
}

void rebuild_txn_free(RebuildTxn *txn) {
    free(txn);
}

/* ========== Rebuild crash recovery ==========
   On server startup, walk all tenant dirs and remove any leftover
   rebuild artifacts. A partial *.new indicates the rebuild crashed
   before the atomic swap — it must be rerun. */

static int recovery_path_state(const char *path, struct stat *st) {
    if (lstat(path, st) == 0) return 1;
    if (errno == ENOENT) return 0;
    LOG_ERROR(LOG_SUB_SLOTCASK, "RECOVERY: lstat(%s) failed: %s",
              path, strerror(errno));
    return -1;
}

static int recover_unambiguous_legacy_layout(const char *obj_dir) {
    char data_dir[PATH_MAX], legacy_dir[PATH_MAX], legacy_root[PATH_MAX],
         legacy_data[PATH_MAX];
    if (snprintf(data_dir, sizeof(data_dir), "%s/data", obj_dir) >=
            (int)sizeof(data_dir) ||
        snprintf(legacy_dir, sizeof(legacy_dir), "%s/data.legacy", obj_dir) >=
            (int)sizeof(legacy_dir) ||
        snprintf(legacy_root, sizeof(legacy_root),
                 "%s/.rebuild_legacy_root", obj_dir) >=
            (int)sizeof(legacy_root) ||
        snprintf(legacy_data, sizeof(legacy_data), "%s/data", legacy_root) >=
            (int)sizeof(legacy_data)) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "RECOVERY: object path too long under %s", obj_dir);
        return -1;
    }

    struct stat data_st, legacy_st, legacy_root_st, legacy_data_st;
    int has_data = recovery_path_state(data_dir, &data_st);
    int has_legacy = recovery_path_state(legacy_dir, &legacy_st);
    int has_legacy_root = recovery_path_state(legacy_root, &legacy_root_st);
    int has_legacy_data = recovery_path_state(legacy_data, &legacy_data_st);
    if (has_data < 0 || has_legacy < 0 || has_legacy_root < 0 ||
        has_legacy_data < 0) return -1;

    if ((has_data && (has_legacy || has_legacy_data)) ||
        (has_legacy && has_legacy_data)) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "RECOVERY: ambiguous rebuild layout under %s; preserving all copies",
                  obj_dir);
        return -1;
    }
    if ((has_data && !S_ISDIR(data_st.st_mode)) ||
        (has_legacy && !S_ISDIR(legacy_st.st_mode)) ||
        (has_legacy_root && !S_ISDIR(legacy_root_st.st_mode)) ||
        (has_legacy_data && !S_ISDIR(legacy_data_st.st_mode))) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "RECOVERY: rebuild artifact under %s is not a directory",
                  obj_dir);
        return -1;
    }

    if (has_legacy && !has_data) {
        if (rename(legacy_dir, data_dir) != 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "RECOVERY: failed to restore %s from %s: %s",
                      data_dir, legacy_dir, strerror(errno));
            return -1;
        }
        if (has_legacy_root && rmdir(legacy_root) != 0 && errno != ENOENT) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "RECOVERY: unexpected contents in legacy root %s",
                      legacy_root);
            return -1;
        }
        LOG_WARN(LOG_SUB_SLOTCASK,
                 "RECOVERY restored %s from data.legacy", data_dir);
        return 0;
    }

    if (!has_legacy && !has_data && has_legacy_data) {
        if (!has_legacy_root) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "RECOVERY: refusing malformed legacy rebuild root %s",
                      legacy_root);
            return -1;
        }
        if (rename(legacy_data, data_dir) != 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "RECOVERY: failed to restore %s from %s: %s",
                      data_dir, legacy_data, strerror(errno));
            return -1;
        }
        rmrf(legacy_root);
        struct stat verify_st;
        if (lstat(legacy_root, &verify_st) == 0 || errno != ENOENT) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "RECOVERY: failed to remove consumed legacy root %s",
                      legacy_root);
            return -1;
        }
        LOG_WARN(LOG_SUB_SLOTCASK,
                 "RECOVERY restored %s from .rebuild_legacy_root", data_dir);
        return 0;
    }

    if (has_legacy_root && !has_legacy_data) {
        if (!has_data) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "RECOVERY: empty legacy root but no live data under %s",
                      obj_dir);
            return -1;
        }
        if (rmdir(legacy_root) != 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "RECOVERY: legacy root %s is not an empty shell: %s",
                      legacy_root, strerror(errno));
            return -1;
        }
    }
    return 0;
}

static int recover_transaction_layout(const char *obj_dir) {
    const char *slash = strrchr(obj_dir, '/');
    if (!slash || slash == obj_dir || !slash[1]) return -1;
    char eff_root[PATH_MAX];
    size_t root_len = (size_t)(slash - obj_dir);
    if (root_len >= sizeof(eff_root)) return -1;
    memcpy(eff_root, obj_dir, root_len);
    eff_root[root_len] = '\0';
    RebuildTxn *txn = rebuild_txn_alloc(eff_root, slash + 1);
    if (!txn) return -1;

    struct stat prep_st, active_st, done_st;
    int has_prep = recovery_path_state(txn->preparing, &prep_st);
    int has_active = recovery_path_state(txn->active, &active_st);
    int has_done = recovery_path_state(txn->done, &done_st);
    if (has_prep < 0 || has_active < 0 || has_done < 0) {
        rebuild_txn_free(txn);
        return -1;
    }
    int states = has_prep + has_active + has_done;
    if (states > 1) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "RECOVERY: multiple rebuild transaction states under %s; preserving all",
                  obj_dir);
        rebuild_txn_free(txn);
        return -1;
    }
    if ((has_prep && !S_ISDIR(prep_st.st_mode)) ||
        (has_active && !S_ISDIR(active_st.st_mode)) ||
        (has_done && !S_ISDIR(done_st.st_mode))) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "RECOVERY: transaction state under %s is not a directory",
                  obj_dir);
        rebuild_txn_free(txn);
        return -1;
    }

    int rc = 0;
    if (has_prep) {
        rc = remove_non_authoritative_dir(txn->preparing);
    } else if (has_done) {
        rebuild_txn_cleanup_committed(txn);
        rc = verify_absent(txn->done);
    } else if (has_active) {
        if (parse_meta(txn) != 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "RECOVERY: invalid active rebuild metadata under %s; preserving it",
                      obj_dir);
            rc = -1;
        } else {
            rc = rebuild_txn_abort(txn);
            if (rc != 0)
                LOG_ERROR(LOG_SUB_SLOTCASK,
                          "RECOVERY: rollback failed under %s; preserving active state",
                          obj_dir);
        }
    }
    rebuild_txn_free(txn);
    return rc;
}

static int recover_one_object(const char *obj_dir) {
    if (recover_transaction_layout(obj_dir) != 0) return -1;
    if (recover_unambiguous_legacy_layout(obj_dir) != 0) return -1;

    char path[PATH_MAX];
    const char *artifacts[] = {
        "data.new",
        "data.old",
        "indexes.new",
        "indexes.old",
        "fields.conf.new",
        "fields.conf.old",
        "schema.conf.new",
        "schema.conf.old",
        NULL
    };
    for (int i = 0; artifacts[i]; i++) {
        snprintf(path, sizeof(path), "%s/%s", obj_dir, artifacts[i]);
        struct stat st;
        if (stat(path, &st) == 0) {
            rmrf(path);
            LOG_WARN(LOG_SUB_SLOTCASK, "RECOVERY cleaned up %s", path);
        }
    }
    return 0;
}

int rebuild_recovery(const char *db_root) {
    /* Walk every tenant dir, then every object inside. */
    pthread_mutex_lock(&g_dirs_lock);
    char dirs_copy[DIRS_BUCKETS][256];
    int dirs_used_copy[DIRS_BUCKETS];
    int total = g_dirs_count;
    memcpy(dirs_copy, g_dirs, sizeof(dirs_copy));
    memcpy(dirs_used_copy, g_dirs_used, sizeof(dirs_used_copy));
    pthread_mutex_unlock(&g_dirs_lock);

    (void)total;
    int errors = 0;
    for (int b = 0; b < DIRS_BUCKETS; b++) {
        if (!dirs_used_copy[b]) continue;

        char eff_root[PATH_MAX];
        build_effective_root(eff_root, sizeof(eff_root), dirs_copy[b]);

        DIR *d = opendir(eff_root);
        if (!d) continue;
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            char obj_dir[PATH_MAX];
            snprintf(obj_dir, sizeof(obj_dir), "%s/%s", eff_root, e->d_name);
            struct stat st;
            if (stat(obj_dir, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
            if (recover_one_object(obj_dir) != 0) errors++;
        }
        closedir(d);
    }
    return errors == 0 ? 0 : -1;
}
