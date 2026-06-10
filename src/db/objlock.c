#include "types.h"

/* ========== Per-object rwlock ==========
   Coordinates normal writes (shared) vs. rebuild (exclusive).
   - Writers (insert/delete/update/bulk/add-index/truncate): rdlock
   - Rebuild (vacuum/add-field): wrlock — blocks all writers for the duration
   - Reads (get/find/search/range): do NOT take this lock (MAP_PRIVATE
     gives snapshot isolation)

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
#ifdef PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP
            {
                pthread_rwlockattr_t attr;
                pthread_rwlockattr_init(&attr);
                pthread_rwlockattr_setkind_np(&attr,
                    PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
                pthread_rwlock_init(&g_objlocks[slot].rwlock, &attr);
                pthread_rwlockattr_destroy(&attr);
            }
#else
            pthread_rwlock_init(&g_objlocks[slot].rwlock, NULL);
#endif
            /* Release ordering: name + rwlock_init complete before
               any concurrent fast-path acquire-load sees used==1. */
            atomic_store_explicit(&g_objlocks[slot].used, 1, memory_order_release);
            pthread_mutex_unlock(&g_objlock_table_lock);
            return &g_objlocks[slot].rwlock;
        }
    }
    pthread_mutex_unlock(&g_objlock_table_lock);
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

/* ========== Rebuild crash recovery ==========
   On server startup, walk all tenant dirs and remove any leftover
   rebuild artifacts. A partial *.new indicates the rebuild crashed
   before the atomic swap — it must be rerun. */

static void recover_one_object(const char *obj_dir) {
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
}

void rebuild_recovery(const char *db_root) {
    /* Walk every tenant dir, then every object inside. */
    pthread_mutex_lock(&g_dirs_lock);
    char dirs_copy[DIRS_BUCKETS][256];
    int dirs_used_copy[DIRS_BUCKETS];
    int total = g_dirs_count;
    memcpy(dirs_copy, g_dirs, sizeof(dirs_copy));
    memcpy(dirs_used_copy, g_dirs_used, sizeof(dirs_used_copy));
    pthread_mutex_unlock(&g_dirs_lock);

    (void)total;
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
            recover_one_object(obj_dir);
        }
        closedir(d);
    }
}
