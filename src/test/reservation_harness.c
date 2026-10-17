#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "shard_db.h"
#include "types.h"
#include "slotcask.h"
#include "shard_db_internal.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define NEED(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); return 1; } } while (0)
static void tick(void) { struct timespec t = {0, 1000000}; nanosleep(&t, NULL); }
static int wait_count(atomic_int *p, int want) {
    uint64_t until = now_ms_coarse() + 5000;
    while (atomic_load(p) < want && now_ms_coarse() < until) tick();
    return atomic_load(p) >= want;
}
static int remove_entry(const char *p, const struct stat *st, int type, struct FTW *f) {
    (void)st; (void)type; (void)f; return remove(p);
}
typedef struct { SlotcaskDb db; ShardDb *owner; int object_locked; char base[PATH_MAX]; } Fixture;
static int open_fixture(Fixture *f) {
    snprintf(f->base, sizeof(f->base), "/tmp/shard-reservation-XXXXXX");
    if (!mkdtemp(f->base) || chdir(f->base)) return -1;
    f->owner = shard_db_open_internal(f->base);
    if (!f->owner) return -1;
    g_shard_db_instance = f->owner;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/data", f->base);
    if (mkdir(path, 0755)) return -1;
    snprintf(path, sizeof(path), "%s/data/kf", f->base);
    if (mkdir(path, 0755)) return -1;
    if (slotcask_open(&f->db, f->base, 8, 1, 64)) return -1;
    if (objlock_rdlock(f->base, "raw")) return -1;
    f->object_locked = 1;
    return 0;
}
static void close_fixture(Fixture *f) {
    slotcask_close(&f->db);
    if (f->object_locked) objlock_rdunlock(f->base, "raw");
    shard_db_close(f->owner);
    chdir("/");
    nftw(f->base, remove_entry, 32, FTW_DEPTH | FTW_PHYS);
}
static void shard_key(char out[32], int shard) {
    for (unsigned i = 0; ; i++) {
        snprintf(out, 32, "key-%u", i);
        uint8_t hash[16]; compute_hash_raw(out, strlen(out), hash);
        if (compute_record_shard(hash, 8) == shard) return;
    }
}
static int put(SlotcaskDb *db, const char *key, const char *value) {
    SlotcaskUpsertOpts opts = {0}; SlotcaskUpsertResult result = {0};
    int rc = slotcask_upsert_with_hooks(db, -1, key, strlen(key), value,
                                        strlen(value), &opts, &result);
    free(result.current_value); return rc;
}
static int count_row(const uint8_t h[16], const void *k, size_t kn,
                      const void *v, size_t vn, uint8_t hdr, void *ctx) {
    (void)h; (void)k; (void)kn; (void)v; (void)vn; (void)hdr;
    (*(int *)ctx)++; return 0;
}
static int lifecycle(void) {
    Fixture f = {0}; NEED(open_fixture(&f) == 0);
    SlotcaskWriteReservation r = {0}, nested = {0}; SlotcaskDb other = {0};
    NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    NEED(slotcask_write_reservation_held(&f.db));
    NEED(!slotcask_write_reservation_held(&other));
    NEED(slotcask_write_reservation_begin(&f.db, &nested) == -1);
    NEED(put(&f.db, "borrow", "value") == 0); /* nested gate borrowing */
    void *value = NULL; size_t len = 0;
    NEED(slotcask_get_ex(&f.db, "borrow", 6, &value, &len, NULL) == 0);
    NEED(len == 5 && !memcmp(value, "value", 5)); free(value);
    int rows = 0; NEED(slotcask_walk_live_strict(&f.db, count_row, &rows) == 0);
    NEED(rows == 1);
    slotcask_write_reservation_end(&r);
    NEED(!slotcask_write_reservation_held(&f.db));
    slotcask_write_reservation_end(&r); /* inactive cleanup is harmless */
    for (int i = 0; i < f.db.num_shards; i++) {
        NEED(pthread_mutex_trylock(&f.db.writer_gates[i]) == 0);
        pthread_mutex_unlock(&f.db.writer_gates[i]);
    }
    NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    slotcask_write_reservation_end(&r);
    close_fixture(&f); return 0;
}

typedef struct {
    SlotcaskDb *db;
    int io;
    atomic_int *waiting;
    atomic_int *done;
} Blocker;
static void *block_gate(void *p) {
    Blocker *b = p;
    atomic_fetch_add(b->waiting, 1);
    pthread_mutex_lock(&b->db->writer_gates[0]);
    pthread_mutex_unlock(&b->db->writer_gates[0]);
    atomic_fetch_add(b->done, 1); return NULL;
}
static void *dispatch_blockers(void *p) {
    Blocker *b = p;
    Blocker jobs[2] = {*b, *b};
    if (b->io) parallel_for_io(block_gate, jobs, 2, sizeof(jobs[0]));
    else parallel_for(block_gate, jobs, 2, sizeof(jobs[0]));
    return NULL;
}
typedef struct { pthread_t owner; atomic_int *count, *wrong; } Probe;
static void *probe(void *p) {
    Probe *a = p;
    if (!pthread_equal(pthread_self(), a->owner)) atomic_store(a->wrong, 1);
    atomic_fetch_add(a->count, 1); return NULL;
}
static void *nested_io(void *p) {
    Probe jobs[2] = {*(Probe *)p, *(Probe *)p};
    probe(p); parallel_for_io(probe, jobs, 2, sizeof(jobs[0])); return NULL;
}
static void *nested_cpu(void *p) {
    Probe jobs[2] = {*(Probe *)p, *(Probe *)p};
    probe(p); parallel_for(probe, jobs, 2, sizeof(jobs[0])); return NULL;
}
static int saturation(void) {
    Fixture f = {0}; NEED(open_fixture(&f) == 0);
    parallel_pool_init(2); parallel_io_pool_init(2);
    NEED(put(&f.db, "seed", "seed") == 0);
    SlotcaskWriteReservation r = {0};
    NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    atomic_int waiting = 0, done = 0, count = 0, wrong = 0;
    Blocker cpu = {&f.db, 0, &waiting, &done}, io = {&f.db, 1, &waiting, &done};
    pthread_t cpu_thread, io_thread;
    NEED(!pthread_create(&cpu_thread, NULL, dispatch_blockers, &cpu));
    NEED(!pthread_create(&io_thread, NULL, dispatch_blockers, &io));
    NEED(wait_count(&waiting, 4)); /* every worker is waiting on our gate */
    Probe jobs[2] = {{pthread_self(), &count, &wrong}, {pthread_self(), &count, &wrong}};
    parallel_for(nested_io, jobs, 2, sizeof(jobs[0]));
    parallel_for_io(nested_cpu, jobs, 2, sizeof(jobs[0]));
    NEED(atomic_load(&count) == 12 && !atomic_load(&wrong));
    int rows = 0;
    NEED(slotcask_walk_live_strict(&f.db, count_row, &rows) == 0 && rows == 1);
    NEED(put(&f.db, "target", "target") == 0);
    NEED(atomic_load(&done) == 0);
    slotcask_write_reservation_end(&r);
    pthread_join(cpu_thread, NULL); pthread_join(io_thread, NULL);
    NEED(atomic_load(&done) == 4);
    parallel_io_pool_shutdown(); parallel_pool_shutdown();
    close_fixture(&f); return 0;
}

typedef struct { SlotcaskDb *db; atomic_int started, acquired; } Pending;
static void *reserve_later(void *p) {
    Pending *a = p; SlotcaskWriteReservation r = {0};
    atomic_store(&a->started, 1);
    if (!slotcask_write_reservation_begin(a->db, &r)) {
        atomic_store(&a->acquired, 1); slotcask_write_reservation_end(&r);
    }
    return NULL;
}
static int partial(void) {
    Fixture f = {0}; NEED(open_fixture(&f) == 0);
    pthread_mutex_lock(&f.db.writer_gates[1]);
    Pending p = {.db = &f.db}; pthread_t thread;
    NEED(!pthread_create(&thread, NULL, reserve_later, &p));
    NEED(wait_count(&p.started, 1));
    struct timespec delay = {0, 100000000}; nanosleep(&delay, NULL);
    /* A blocking ascending acquisition retains gate 0 forever here.
       The release/retry design lets this older writer use it. */
    int got = 0;
    for (int i = 0; i < 1000; i++) {
        if (!pthread_mutex_trylock(&f.db.writer_gates[0])) { got = 1; break; }
        tick();
    }
    NEED(got && !atomic_load(&p.acquired));
    pthread_mutex_unlock(&f.db.writer_gates[0]);
    pthread_mutex_unlock(&f.db.writer_gates[1]);
    pthread_join(thread, NULL); NEED(atomic_load(&p.acquired));
    close_fixture(&f); return 0;
}

static int prepare(SlotcaskBulkRec *r, const size_t *a, size_t n, void *ctx, void **state) {
    (void)r; (void)a; (void)n; (void)ctx; *state = NULL; return 0;
}
static int fail_apply(SlotcaskBulkRec *r, const size_t *a, size_t n, void *ctx, void *state) {
    (void)r; (void)a; (void)n; (void)ctx; (void)state; return -1;
}
static void terminal(void *ctx, void *state) { (void)ctx; (void)state; }
static int recovery_diff(const char *root, const char *object, int shard, uint32_t slot,
                          const uint8_t *hash, const uint8_t *old, size_t oldn, uint8_t oldh,
                          const uint8_t *newv, size_t newn, uint8_t newh, char *err, size_t cap) {
    (void)root; (void)object; (void)shard; (void)slot; (void)hash;
    (void)old; (void)oldn; (void)oldh; (void)newv; (void)newn; (void)newh;
    (void)err; (void)cap; return 0;
}
static int corrupt_marker(Fixture *f) {
    char dir[PATH_MAX], path[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s/data/kf", f->base);
    DIR *d = opendir(dir); if (!d) return -1;
    struct dirent *e; int result = -1;
    while ((e = readdir(d))) {
        if (!strstr(e->d_name, "_marker.dat")) continue;
        snprintf(path, sizeof(path), "%s/%s", dir, e->d_name);
        int fd = open(path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0) break;
        off_t off = 16 + offsetof(KfMarkerSlot, new_offset);
        uint8_t byte;
        if (pread(fd, &byte, 1, off) == 1) {
            byte ^= 1;
            if (pwrite(fd, &byte, 1, off) == 1 && !fsync(fd)) result = 0;
        }
        close(fd); break;
    }
    closedir(d); return result;
}
static int recovery(int corrupt) {
    Fixture f = {0}; NEED(open_fixture(&f) == 0);
    char pending[32], target[32]; shard_key(pending, 1); shard_key(target, 0);
    SlotcaskBulkRec rec = {.key = pending, .klen = strlen(pending), .value = "pending", .vlen = 7};
    SlotcaskBulkOpts opts = {.has_indexed_fields = 1, .prepare_window = prepare,
        .apply_window = fail_apply, .commit_done = terminal,
        .release_window = terminal, .abort_window = terminal};
    SlotcaskBulkShardInput input = {.kf_shard_id = 1, .recs = &rec, .nrecs = 1,
        .kind = SLOTCASK_BULK_INPUT_UPSERT, .opts.upsert = opts};
    NEED(slotcask_bulk_request_execute(&f.db, &input, 1) == -1);
    NEED(input.rc == -2 && errno == EINPROGRESS);
    NEED(object_has_pending_markers(f.base) == 1);
    if (corrupt) NEED(corrupt_marker(&f) == 0);
    RecoveryIndexDiffFn saved = g_recovery_index_diff_fn;
    g_recovery_index_diff_fn = recovery_diff; /* isolated executable only */
    SlotcaskWriteReservation r = {0}; NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    int replayed = 0;
    int rc = marker_recovery_sweep_object(f.base, f.base, "raw", &replayed);
    if (corrupt) {
        NEED(rc == -1 && object_has_pending_markers(f.base) == 1);
        void *value = NULL; size_t len = 0;
        NEED(slotcask_get_ex(&f.db, target, strlen(target), &value, &len, NULL) != 0);
        free(value);
    } else {
        NEED(rc == 0 && replayed == 1 && object_has_pending_markers(f.base) == 0);
        int rows = 0;
        NEED(slotcask_walk_live_strict(&f.db, count_row, &rows) == 0 && rows == 1);
        NEED(put(&f.db, target, "target") == 0);
    }
    slotcask_write_reservation_end(&r);
    NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    slotcask_write_reservation_end(&r);
    g_recovery_index_diff_fn = saved;
    close_fixture(&f); return 0;
}

typedef struct { const char *root; atomic_int started, done; } SchemaWait;
static void *schema_writer(void *p) {
    g_db = g_shard_db_instance;
    SchemaWait *a = p; atomic_store(&a->started, 1);
    if (!objlock_wrlock(a->root, "raw")) {
        atomic_store(&a->done, 1); objlock_wrunlock(a->root, "raw");
    }
    return NULL;
}
static int schema_wait(void) {
    Fixture f = {0}; NEED(open_fixture(&f) == 0);
    SlotcaskWriteReservation r = {0}; NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    NEED(objlock_rdlock(f.base, "other") == 0);
    objlock_rdunlock(f.base, "other");
    SchemaWait a = {.root = f.base}; pthread_t thread;
    NEED(!pthread_create(&thread, NULL, schema_writer, &a)); NEED(wait_count(&a.started, 1));
    struct timespec delay = {0, 100000000}; nanosleep(&delay, NULL);
    NEED(!atomic_load(&a.done));
    slotcask_write_reservation_end(&r);
    NEED(!atomic_load(&a.done)); /* shared objlock still prevents schema mutation */
    objlock_rdunlock(f.base, "raw"); f.object_locked = 0;
    pthread_join(thread, NULL); NEED(atomic_load(&a.done));
    close_fixture(&f); return 0;
}
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    if (!strcmp(argv[1], "lifecycle")) return lifecycle();
    if (!strcmp(argv[1], "saturation")) return saturation();
    if (!strcmp(argv[1], "partial")) return partial();
    if (!strcmp(argv[1], "recovery")) return recovery(0);
    if (!strcmp(argv[1], "corrupt")) return recovery(1);
    if (!strcmp(argv[1], "schema")) return schema_wait();
    return 2;
}
