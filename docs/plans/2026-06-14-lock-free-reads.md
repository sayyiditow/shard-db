# Plan: Lock-Free Reads via Per-Object Slot Refs with Generation Counters

**Date:** 2026-06-14  
**Author:** Claude Sonnet 4.6

---

## Execution rules (read before touching any file)

1. Branch off `main`: `git checkout -b feat/lock-free-reads`
2. Execute tasks in order, top to bottom. Do not skip.
3. Build after each task (not just at the end): `SKIP_TESTS=1 ./build.sh`
4. Full test suite after all tasks: `./build/bin/shard-db-test run-all`
5. Never claim a step passed without pasting the real terminal output.
6. **Anchor rule**: every insertion site is identified by a quoted string that must appear verbatim in the file. If the anchor string is not found exactly, STOP and write `PLAN_NOTES.md` at the repo root describing which anchor failed. Do not guess or reinterpret.
7. Leave all work **uncommitted**. The user will commit after review.
8. Run the baseline bench **before** Task 1 and the final bench **after** Task 6, pasting both outputs.

---

## Background and goals

Three global mutexes fire on every read in the hot path:

| Cache | Mutex | Location | Fired by |
|---|---|---|---|
| kfcache | `g_kfcache_lock` (`g_db->kfcache_lock`) | `shard_db_internal.h` | `kfcache_acquire()` in `slotcask.c` |
| segcache | `g_segcache_lock` (`g_db->segcache_lock`) | `shard_db_internal.h` | `segcache_acquire()` in `slotcask.c` |
| ucache | `g_ucache_table_mutex` (`g_db->ucache_table_mutex`) | `shard_db_internal.h` | `ucache_ensure()` in `storage.c` |

The fix: add a generation counter (`_Atomic uint64_t gen`) to each cache entry struct so that callers who already know which cache slot holds their entry can validate it with a single atomic load — no table lock required on the warm path.

For kfcache and segcache (accessed via `SlotcaskDb`), store per-object `SlotRef` arrays in `SlotcaskDb` so `slotcask_get` / `slotcask_exists` / `slotcask_bulk_lookup_in_kfshard` can skip the table probe on cache hits. For ucache (v1 shard scan path), the ucache is already effectively lock-free on reads since `fcache_get_read` returns immediately after `ucache_ensure` — but `ucache_ensure` takes the global mutex even on hits. Adding a `SlotRef` array to the `Schema`-level cache would require wiring it through query.c callers; this is deferred to a follow-on (see Task 5, which is scoped to a struct addition only, no caller wiring, so the build stays green).

**Net effect**: on a warm cache, `slotcask_get` / `slotcask_exists` / `slotcask_bulk_lookup_in_kfshard` skip `pthread_mutex_lock(&g_kfcache_lock)` and `pthread_mutex_lock(&g_segcache_lock)` on every call. These are the two hottest paths for point reads.

---

## Files modified

| File | What changes |
|---|---|
| `src/db/shard_db_internal.h` | Add `_Atomic uint64_t gen` to `KfCacheEntry` and `SegCacheEntry` |
| `src/db/types.h` | Add `typedef SlotRef`; add `_Atomic uint64_t gen` to `UCacheEntry` |
| `src/db/slotcask.h` | Add `SlotRef *kf_slot_refs`, `SlotRef **seg_slot_refs`, `int *seg_slot_caps` to `SlotcaskDb` |
| `src/db/slotcask.c` | Increment gen in drop functions; populate refs at open; add `kfcache_acquire_direct` and `segcache_acquire_direct`; replace calls in `slotcask_get`, `slotcask_exists`, `slotcask_bulk_lookup_in_kfshard` |
| `src/db/storage.c` | Increment gen in ucache eviction |

---

## Baseline bench (run BEFORE Task 1)

```bash
./build/bin/shard-db-bench bench-kv-parallel
```

Paste the full output here before proceeding.

---

## Task 1 — Add `SlotRef` type and generation fields

### 1a. Add `SlotRef` typedef to `types.h`

**File:** `src/db/types.h`

**Anchor** (find this exact string in the file):

```
typedef struct UCacheEntry {
```

Insert **immediately before** that line:

```c
/* A lightweight cache reference: cache-slot index + generation counter.
   Validated with a single atomic load — no table lock needed on warm hits.
   slot == -1 means "not yet populated" (safe initial value after calloc/memset). */
typedef struct SlotRef {
    int      slot;
    uint64_t gen;
} SlotRef;

```

**Note:** the struct must carry the tag `SlotRef` (`typedef struct SlotRef { ... } SlotRef;`), not an anonymous struct (`typedef struct { ... } SlotRef;`). `src/db/slotcask.h` does not include `types.h` and is itself included by `src/db/slotcask.c` *before* `types.h` (the only one of the six consumers of `slotcask.h` with that include order) — `slotcask.h` needs `SlotRef` to exist before `types.h` has necessarily been parsed, via the forward declaration added in step 1a-bis below. A forward declaration of an anonymous-struct typedef is not legal C; tagging the struct makes `typedef struct SlotRef SlotRef;` a valid, compatible redeclaration under C11 (duplicate typedefs to the same type are permitted).

### 1a-bis. Forward-declare `SlotRef` in `slotcask.h`

**File:** `src/db/slotcask.h`

**Why this step exists**: `slotcask.h` does not include `types.h` (confirmed: its current includes are `<stddef.h>`, `<stdint.h>`, `<stdatomic.h>`, `<pthread.h>`, `<limits.h>`, `<sys/param.h>`). Task 2a adds `SlotRef`-typed fields to `SlotcaskDb` in this same header, so `SlotRef` must be visible here. Most consumers of `slotcask.h` (`embedded.c`, `index.c`, `storage.c`, `server.c`, `query.c`) include `types.h` before `slotcask.h`, so `SlotRef` would already be defined by the time `slotcask.h` is parsed in those files — but `slotcask.c` itself includes `slotcask.h` (line 18) *before* `types.h` (line 19), so without this forward declaration, `SlotRef` is an undefined type when `slotcask.h`'s `SlotcaskDb` fields are parsed in that one file, and the build fails.

**Anchor** (find this exact string — confirmed unique, appears once in the file):

```
#include <sys/param.h>
```

Insert **immediately after** that line:

```c
#include <sys/param.h>

typedef struct SlotRef SlotRef;   /* full definition in types.h; only the
                                      tag is needed here so slotcask.h can
                                      declare SlotRef-typed fields before
                                      types.h is necessarily included
                                      (slotcask.c includes this header
                                      before types.h) */
```

This relies on Task 1a's `SlotRef` typedef using a named struct tag (`typedef struct SlotRef { ... } SlotRef;`), not an anonymous struct — C11 permits a duplicate `typedef` redeclaration to the same type, so this forward declaration and the full definition in `types.h` coexist without conflict regardless of which header is included first in any given `.c` file.

### 1b. Add `_Atomic uint64_t gen` to `UCacheEntry` in `types.h`

**File:** `src/db/types.h`

**Anchor** (find this exact string):

```
    uint64_t last_access;   /* monotonic counter for LRU eviction */
```

Replace with:

```c
    uint64_t last_access;   /* monotonic counter for LRU eviction */
    _Atomic uint64_t gen;   /* incremented on eviction; SlotRef validation */
```

### 1c. Add `_Atomic uint64_t gen` to `KfCacheEntry` in `shard_db_internal.h`

**File:** `src/db/shard_db_internal.h`

**Anchor** (find this exact string):

```
/* slotcask.c — kfcache */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *base;
    size_t   map_size;
    size_t   capacity;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
} KfCacheEntry;
```

Replace with:

```c
/* slotcask.c — kfcache */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *base;
    size_t   map_size;
    size_t   capacity;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
    _Atomic uint64_t gen;   /* incremented on eviction; SlotRef validation */
} KfCacheEntry;
```

### 1d. Add `_Atomic uint64_t gen` to `SegCacheEntry` in `shard_db_internal.h`

**File:** `src/db/shard_db_internal.h`

**Anchor** (find this exact string):

```
/* slotcask.c — segcache */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
} SegCacheEntry;
```

Replace with:

```c
/* slotcask.c — segcache */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
    _Atomic uint64_t gen;   /* incremented on eviction; SlotRef validation */
} SegCacheEntry;
```

### 1e. Increment `gen` in `kfcache_drop_slot` in `slotcask.c`

**File:** `src/db/slotcask.c`

**Anchor** (find this exact string):

```
static void kfcache_drop_slot(int slot) {
    KfCacheEntry *e = &g_kfcache[slot];
    if (!e->used) return;
    if (e->base && e->map_size > 0) msync(e->base, e->map_size, MS_ASYNC);
    if (e->base) munmap(e->base, e->map_size);
    if (e->fd >= 0) close(e->fd);
    e->base = NULL;
    e->fd = -1;
    e->map_size = 0;
    e->capacity = 0;
    e->used = 0;
    e->path[0] = '\0';
    g_kfcache_count--;
}
```

Replace with:

```c
static void kfcache_drop_slot(int slot) {
    KfCacheEntry *e = &g_kfcache[slot];
    if (!e->used) return;
    if (e->base && e->map_size > 0) msync(e->base, e->map_size, MS_ASYNC);
    if (e->base) munmap(e->base, e->map_size);
    if (e->fd >= 0) close(e->fd);
    e->base = NULL;
    e->fd = -1;
    e->map_size = 0;
    e->capacity = 0;
    e->used = 0;
    e->path[0] = '\0';
    /* Increment gen under g_kfcache_lock (caller always holds it).
       Any SlotRef pointing at this slot will fail its gen check after
       this store, forcing the slow-path re-probe. */
    atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
    g_kfcache_count--;
}
```

### 1f. Increment `gen` in `segcache_drop_slot` in `slotcask.c`

**File:** `src/db/slotcask.c`

**Anchor** (find this exact string):

```
static void segcache_drop_slot(int slot) {
    SegCacheEntry *e = &g_segcache[slot];
    if (!e->used) return;
    if (e->map && e->map_size > 0) msync(e->map, e->map_size, MS_ASYNC);
    if (e->map) munmap(e->map, e->map_size);
    if (e->fd >= 0) close(e->fd);
    e->map = NULL;
    e->fd = -1;
    e->map_size = 0;
    e->used = 0;
    e->path[0] = '\0';
    g_segcache_count--;
}
```

Replace with:

```c
static void segcache_drop_slot(int slot) {
    SegCacheEntry *e = &g_segcache[slot];
    if (!e->used) return;
    if (e->map && e->map_size > 0) msync(e->map, e->map_size, MS_ASYNC);
    if (e->map) munmap(e->map, e->map_size);
    if (e->fd >= 0) close(e->fd);
    e->map = NULL;
    e->fd = -1;
    e->map_size = 0;
    e->used = 0;
    e->path[0] = '\0';
    /* Increment gen under g_segcache_lock (caller always holds it).
       Any SlotRef pointing at this slot will fail its gen check, forcing
       the slow-path re-probe. */
    atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
    g_segcache_count--;
}
```

### 1g. Increment `gen` in `kfcache_invalidate_prefix` in `slotcask.c`

The `kfcache_invalidate_prefix` function evicts entries by clearing `e->used` atomically. It does NOT call `kfcache_drop_slot`. We must also increment gen there so slot refs into those slots are invalidated.

**Anchor** (find this exact string):

```
        if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
            strncmp(e->path, prefix, pl) == 0) {
            if (e->base && e->map_size > 0) msync(e->base, e->map_size, MS_ASYNC);
            if (e->base) munmap(e->base, e->map_size);
            if (e->fd >= 0) close(e->fd);
            e->base = NULL;
            e->fd = -1;
            e->map_size = 0;
            e->capacity = 0;
            e->path[0] = '\0';
            __atomic_store_n(&e->used, 0, __ATOMIC_RELEASE);
            __sync_fetch_and_sub(&g_kfcache_count, 1);
        }
        pthread_rwlock_unlock(&e->rwlock);
    }
}
```

Replace with:

```c
        if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
            strncmp(e->path, prefix, pl) == 0) {
            if (e->base && e->map_size > 0) msync(e->base, e->map_size, MS_ASYNC);
            if (e->base) munmap(e->base, e->map_size);
            if (e->fd >= 0) close(e->fd);
            e->base = NULL;
            e->fd = -1;
            e->map_size = 0;
            e->capacity = 0;
            e->path[0] = '\0';
            atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
            __atomic_store_n(&e->used, 0, __ATOMIC_RELEASE);
            __sync_fetch_and_sub(&g_kfcache_count, 1);
        }
        pthread_rwlock_unlock(&e->rwlock);
    }
}
```

### 1h. Increment `gen` in `segcache_invalidate_prefix` in `slotcask.c`

**Anchor** (find this exact string):

```
        if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
            strncmp(e->path, prefix, pl) == 0) {
            if (e->map && e->map_size > 0) msync(e->map, e->map_size, MS_ASYNC);
            if (e->map) munmap(e->map, e->map_size);
            if (e->fd >= 0) close(e->fd);
            e->map = NULL;
            e->fd = -1;
            e->map_size = 0;
            e->path[0] = '\0';
            __atomic_store_n(&e->used, 0, __ATOMIC_RELEASE);
            __sync_fetch_and_sub(&g_segcache_count, 1);
        }
        pthread_rwlock_unlock(&e->rwlock);
    }
}
```

Replace with:

```c
        if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
            strncmp(e->path, prefix, pl) == 0) {
            if (e->map && e->map_size > 0) msync(e->map, e->map_size, MS_ASYNC);
            if (e->map) munmap(e->map, e->map_size);
            if (e->fd >= 0) close(e->fd);
            e->map = NULL;
            e->fd = -1;
            e->map_size = 0;
            e->path[0] = '\0';
            atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
            __atomic_store_n(&e->used, 0, __ATOMIC_RELEASE);
            __sync_fetch_and_sub(&g_segcache_count, 1);
        }
        pthread_rwlock_unlock(&e->rwlock);
    }
}
```

### 1i. Increment `gen` in ucache eviction in `storage.c`

**File:** `src/db/storage.c`

**Anchor** (find this exact string):

```
        if (lru >= 0) {
            UCacheEntry *victim = &g_ucache[lru];
            if (victim->map) { msync(victim->map, victim->map_size, MS_ASYNC); munmap(victim->map, victim->map_size); }
            if (victim->fd >= 0) close(victim->fd);
            if (victim->slot_bits) free(victim->slot_bits);
            victim->map = NULL; victim->fd = -1; victim->slot_bits = NULL;
            victim->used = 0; victim->path[0] = '\0';
            victim->map_size = 0; victim->dirty = 0; victim->max_dirty_slot = -1;
            g_ucache_count--;
            slot = lru;
        } else {
```

Replace with:

```c
        if (lru >= 0) {
            UCacheEntry *victim = &g_ucache[lru];
            if (victim->map) { msync(victim->map, victim->map_size, MS_ASYNC); munmap(victim->map, victim->map_size); }
            if (victim->fd >= 0) close(victim->fd);
            if (victim->slot_bits) free(victim->slot_bits);
            victim->map = NULL; victim->fd = -1; victim->slot_bits = NULL;
            victim->used = 0; victim->path[0] = '\0';
            victim->map_size = 0; victim->dirty = 0; victim->max_dirty_slot = -1;
            /* Invalidate any SlotRef pointing at this slot. */
            atomic_fetch_add_explicit(&victim->gen, 1, memory_order_release);
            g_ucache_count--;
            slot = lru;
        } else {
```

### 1j. Build check

```bash
SKIP_TESTS=1 ./build.sh
```

Paste output. Must succeed with no errors.

---

## Task 2 — Per-object kf slot refs in `SlotcaskDb`

### 2a. Add fields to `SlotcaskDb` in `slotcask.h`

**File:** `src/db/slotcask.h`

**Anchor** (find this exact string):

```
typedef struct SlotcaskDb {
    char    data_dir[PATH_MAX];
    int     num_shards;
    int     num_streams;
    int     slot_size;       /* fixed; set at open time from schema or arg */
    size_t  slots_per_shard; /* per-shard kf capacity floor; individual
                                shards may have grown larger via auto-resplit */

    SlotcaskStream *streams;
} SlotcaskDb;
```

Replace with:

```c
typedef struct SlotcaskDb {
    char    data_dir[PATH_MAX];
    int     num_shards;
    int     num_streams;
    int     slot_size;       /* fixed; set at open time from schema or arg */
    size_t  slots_per_shard; /* per-shard kf capacity floor; individual
                                shards may have grown larger via auto-resplit */

    SlotcaskStream *streams;

    /* Per-shard kf slot refs — populated at slotcask_open time, updated
       on gen mismatch. Array of num_shards entries; slot==-1 means not
       yet cached (safe initial value since the array is calloc'd). */
    SlotRef *kf_slot_refs;

    /* Per-stream segment slot refs. seg_slot_refs[stream_id] is an array
       of seg_slot_caps[stream_id] entries indexed by file_id.
       Allocated lazily (NULL until first segcache hit for that stream).
       seg_slot_caps[stream_id] is the allocated capacity of that array. */
    SlotRef **seg_slot_refs;
    int      *seg_slot_caps;
} SlotcaskDb;
```

### 2b. Populate `kf_slot_refs` at open time in `slotcask.c`

The allocation and initial population happens after the parallel kf-open loop in `slotcask_open`. The kfcache already has every shard installed by that point (the parallel init loop calls `kfcache_acquire` for every shard); we just need to record the slot+gen for each.

**File:** `src/db/slotcask.c`

**Anchor** (find this exact string):

```
    for (int i = 0; i < num_shards; i++) {
        if (!open_args[i].ok) { free(open_args); goto fail; }
    }
```

Replace with:

```c
    for (int i = 0; i < num_shards; i++) {
        if (!open_args[i].ok) { free(open_args); goto fail; }
    }

    /* Populate per-shard kf slot refs so the hot read path can skip the
       table mutex on cache hits. The kfcache already has all shards
       installed from the parallel init above; we re-acquire each one as
       a reader (rdlock, no table mutation) solely to record (slot, gen). */
    db->kf_slot_refs = calloc((size_t)num_shards, sizeof(SlotRef));
    if (!db->kf_slot_refs) { free(open_args); goto fail; }
    for (int i = 0; i < num_shards; i++) db->kf_slot_refs[i].slot = -1;
    for (int i = 0; i < num_shards; i++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, data_dir, i);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) == 0) {
            if (kh.slot >= 0) {
                db->kf_slot_refs[i].slot = kh.slot;
                db->kf_slot_refs[i].gen  =
                    atomic_load_explicit(&g_kfcache[kh.slot].gen,
                                         memory_order_acquire);
            }
            kfcache_release(&kh);
        }
    }

    /* Allocate per-stream segment slot ref arrays (all start NULL / cap 0). */
    db->seg_slot_refs = calloc((size_t)num_streams, sizeof(SlotRef *));
    db->seg_slot_caps = calloc((size_t)num_streams, sizeof(int));
    if (!db->seg_slot_refs || !db->seg_slot_caps) { free(open_args); goto fail; }
```

### 2c. Free `kf_slot_refs` and `seg_slot_refs` in `slotcask_close`

**Anchor** (find this exact string):

```
void slotcask_close(SlotcaskDb *db) {
    if (db->streams) {
        for (int i = 0; i < db->num_streams; i++) {
            pthread_mutex_destroy(&db->streams[i].rotation_lock);
            pthread_mutex_destroy(&db->streams[i].pool_lock);
            free(db->streams[i].free_slots);
        }
        free(db->streams);
    }
    remove_dirty_marker(db);
    memset(db, 0, sizeof(*db));
}
```

Replace with:

```c
void slotcask_close(SlotcaskDb *db) {
    if (db->streams) {
        for (int i = 0; i < db->num_streams; i++) {
            pthread_mutex_destroy(&db->streams[i].rotation_lock);
            pthread_mutex_destroy(&db->streams[i].pool_lock);
            free(db->streams[i].free_slots);
        }
        free(db->streams);
    }
    free(db->kf_slot_refs);
    if (db->seg_slot_refs) {
        for (int i = 0; i < db->num_streams; i++)
            free(db->seg_slot_refs[i]);
        free(db->seg_slot_refs);
    }
    free(db->seg_slot_caps);
    remove_dirty_marker(db);
    memset(db, 0, sizeof(*db));
}
```

### 2d. Build check

```bash
SKIP_TESTS=1 ./build.sh
```

Paste output. Must succeed with no errors.

---

## Task 3 — Fast path in kfcache: `kfcache_acquire_direct`

### 3a. Add `kfcache_acquire_direct` declaration to `slotcask.h`

**File:** `src/db/slotcask.h`

**Anchor** (find this exact string):

```
int  kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                     size_t slots_capacity, int writer);
void kfcache_release(SlotcaskKfHandle *h);
```

Replace with:

```c
int  kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                     size_t slots_capacity, int writer);
void kfcache_release(SlotcaskKfHandle *h);

/* Fast-path acquire for read-only callers that hold a SlotRef.
   On gen match: takes rdlock and returns 0 without touching the table mutex.
   On gen mismatch (eviction since last open): falls through to kfcache_acquire,
   then updates *ref with the new (slot, gen). Always passes writer=0.
   db and kf_shard_id are used only on the slow path to refresh *ref. */
int  kfcache_acquire_direct(SlotcaskKfHandle *h, SlotRef *ref,
                             const char *path, size_t slots_capacity,
                             struct SlotcaskDb *db, int kf_shard_id);
```

Note: `struct SlotcaskDb` (forward-declared) is used here because `SlotcaskDb` is defined later in the same header. The full typedef is visible to callers that include the header in order, but the forward-declaration keeps the prototype valid within the header itself. If the compiler complains about the forward declaration within the same header, replace `struct SlotcaskDb *db` with `void *db` in the declaration only (the definition in `.c` uses the full type).

### 3b. Add `kfcache_acquire_direct` definition to `slotcask.c`

**File:** `src/db/slotcask.c`

**Anchor** (find this exact string):

```
void kfcache_release(SlotcaskKfHandle *h) {
    if (h->slot >= 0) {
        pthread_rwlock_unlock(&g_kfcache[h->slot].rwlock);
    } else if (h->hdr) {
        /* Uncached fallback. */
        munmap((void *)h->hdr, h->map_size);
        if (h->fd >= 0) close(h->fd);
    }
    h->slot = -1;
    h->fd = -1;
    h->hdr = NULL;
    h->map = NULL;
    h->map_size = 0;
    h->capacity = 0;
}
```

Insert immediately **after** the closing brace of `kfcache_release` (i.e., after that block):

```c
/* Fast-path kfcache acquire for read-only callers that hold a SlotRef.
 *
 * Warm hit (common case, no lock):
 *   1. Load ref->slot — skip if -1 (not yet populated).
 *   2. Atomic-load e->gen and compare with ref->gen.
 *   3. If equal: take per-slot rdlock, verify identity (path match + used),
 *      fill handle, return 0. Total cost: 1 atomic load + 1 rdlock.
 *
 * Cold/evicted (uncommon):
 *   Fall through to kfcache_acquire (existing slow path). On success,
 *   update ref->slot and ref->gen so the next call is a warm hit.
 *
 * writer must be 0 — this function is for read-only callers only.
 * db and kf_shard_id are accepted but only used to update ref on the
 * slow path (so the caller's stored ref stays current after a miss).
 */
int kfcache_acquire_direct(SlotcaskKfHandle *h, SlotRef *ref,
                            const char *path, size_t slots_capacity,
                            SlotcaskDb *db, int kf_shard_id) {
    (void)db;          /* used only to make the signature future-proof */
    (void)kf_shard_id; /* same */

    if (ref && ref->slot >= 0) {
        int s = ref->slot;
        KfCacheEntry *e = &g_kfcache[s];
        uint64_t cur_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
        if (cur_gen == ref->gen) {
            /* Gen matches — slot should still hold our entry.
               Take rdlock and verify identity before returning. */
            pthread_rwlock_rdlock(&e->rwlock);
            if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
                strcmp(e->path, path) == 0) {
                /* Warm hit confirmed. */
                h->slot = s;
                h->writer = 0;
                kf_handle_from_entry(h, e);
                return 0;
            }
            /* Identity check failed (concurrent eviction between gen-check
               and rdlock). Drop lock and fall through to slow path. */
            pthread_rwlock_unlock(&e->rwlock);
        }
    }

    /* Slow path: standard kfcache_acquire, then refresh the SlotRef. */
    int rc = kfcache_acquire(h, path, slots_capacity, 0);
    if (rc == 0 && ref && h->slot >= 0) {
        ref->slot = h->slot;
        ref->gen  = atomic_load_explicit(&g_kfcache[h->slot].gen,
                                          memory_order_acquire);
    }
    return rc;
}
```

### 3c. Replace `kfcache_acquire` with `kfcache_acquire_direct` in `slotcask_get`

**File:** `src/db/slotcask.c`

**Anchor** (find this exact string):

```
int slotcask_get(SlotcaskDb *db,
                 const void *key, size_t klen,
                 void **val_out, size_t *vlen_out) {
    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);

    for (int attempt = 0; attempt < SLOTCASK_GET_MAX_RETRIES; attempt++) {
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) return -1;
```

Replace with:

```c
int slotcask_get(SlotcaskDb *db,
                 const void *key, size_t klen,
                 void **val_out, size_t *vlen_out) {
    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[sid_kf] : NULL;

    for (int attempt = 0; attempt < SLOTCASK_GET_MAX_RETRIES; attempt++) {
        SlotcaskKfHandle kh;
        if (kfcache_acquire_direct(&kh, kf_ref, kf_path,
                                    db->slots_per_shard, db, sid_kf) != 0) return -1;
```

### 3d. Replace `kfcache_acquire` in `slotcask_exists`

**Anchor** (find this exact string):

```
int slotcask_exists(SlotcaskDb *db, const void *key, size_t klen) {
    if (klen > UINT16_MAX) return -1;
    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) return -1;
```

Replace with:

```c
int slotcask_exists(SlotcaskDb *db, const void *key, size_t klen) {
    if (klen > UINT16_MAX) return -1;
    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[sid_kf] : NULL;
    if (kfcache_acquire_direct(&kh, kf_ref, kf_path,
                                db->slots_per_shard, db, sid_kf) != 0) return -1;
```

### 3e. Replace `kfcache_acquire` in `slotcask_bulk_lookup_in_kfshard`

**Anchor** (find this exact string):

```
int slotcask_bulk_lookup_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                      SlotcaskBulkRec *recs, size_t n) {
    if (n == 0) return 0;

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) return -1;
```

Replace with:

```c
int slotcask_bulk_lookup_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                      SlotcaskBulkRec *recs, size_t n) {
    if (n == 0) return 0;

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[kf_shard_id] : NULL;
    if (kfcache_acquire_direct(&kh, kf_ref, kf_path,
                                db->slots_per_shard, db, kf_shard_id) != 0) return -1;
```

### 3f. Build check

```bash
SKIP_TESTS=1 ./build.sh
```

Paste output. Must succeed with no errors.

---

## Task 4 — Fast path in segcache: `segcache_acquire_direct`

### 4a. Add `segcache_acquire_direct` declaration to `slotcask.h`

**File:** `src/db/slotcask.h`

**Anchor** (find this exact string):

```
int  segcache_acquire(SlotcaskSegHandle *h, const char *path,
                      int create, int writer);
void segcache_release(SlotcaskSegHandle *h);
```

Replace with:

```c
int  segcache_acquire(SlotcaskSegHandle *h, const char *path,
                      int create, int writer);
void segcache_release(SlotcaskSegHandle *h);

/* Fast-path acquire for read-only callers that hold a SlotRef.
   On gen match: takes rdlock and returns 0 without touching g_segcache_lock.
   On gen mismatch: falls through to segcache_acquire and updates *ref.
   create must be 0 (read paths only). writer must be 0. */
int  segcache_acquire_direct(SlotcaskSegHandle *h, SlotRef *ref,
                              const char *path);
```

### 4b. Add `segcache_acquire_direct` definition to `slotcask.c`

**File:** `src/db/slotcask.c`

**Anchor** (find this exact string):

```
void segcache_release(SlotcaskSegHandle *h) {
    if (h->slot >= 0) {
        pthread_rwlock_unlock(&g_segcache[h->slot].rwlock);
    } else if (h->map) {
        munmap(h->map, h->map_size);
        if (h->fd >= 0) close(h->fd);
    }
    h->slot = -1;
    h->fd = -1;
    h->map = NULL;
    h->map_size = 0;
}
```

Insert immediately **after** the closing brace of `segcache_release`:

```c
/* Fast-path segcache acquire for read-only callers that hold a SlotRef.
 *
 * Warm hit (common case):
 *   1. Atomic-load e->gen; compare with ref->gen.
 *   2. If equal: take per-slot rdlock, verify identity, fill handle.
 *      No g_segcache_lock touched. Cost: 1 atomic load + 1 rdlock.
 *
 * Cold/evicted: fall through to segcache_acquire(create=0, writer=0),
 * then update *ref.
 *
 * IMPORTANT: ref may be NULL (if the per-stream array is not yet
 * allocated, or if file_id >= seg_slot_caps[stream_id]). In that case
 * we fall straight through to the slow path without crashing.
 */
int segcache_acquire_direct(SlotcaskSegHandle *h, SlotRef *ref,
                             const char *path) {
    if (ref && ref->slot >= 0) {
        int s = ref->slot;
        SegCacheEntry *e = &g_segcache[s];
        uint64_t cur_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
        if (cur_gen == ref->gen) {
            pthread_rwlock_rdlock(&e->rwlock);
            if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
                strcmp(e->path, path) == 0) {
                /* Warm hit confirmed. */
                h->slot = s;
                h->writer = 0;
                h->fd = e->fd;
                h->map = e->map;
                h->map_size = e->map_size;
                return 0;
            }
            pthread_rwlock_unlock(&e->rwlock);
        }
    }

    /* Slow path. */
    int rc = segcache_acquire(h, path, 0, 0);
    if (rc == 0 && ref && h->slot >= 0) {
        ref->slot = h->slot;
        ref->gen  = atomic_load_explicit(&g_segcache[h->slot].gen,
                                          memory_order_acquire);
    }
    return rc;
}
```

### 4c. Add a helper to get/grow the seg_slot_refs array for a (stream, file_id) pair

This helper is called on the slow path (after `segcache_acquire` returns) and on any path that learns the file_id at runtime. It grows `db->seg_slot_refs[stream_id]` as needed and returns a pointer to the right `SlotRef`, or NULL on OOM.

**File:** `src/db/slotcask.c`

**Anchor** (find this exact string — place this immediately before `segcache_acquire_direct`):

```
/* Fast-path segcache acquire for read-only callers that hold a SlotRef.
```

Insert immediately **before** that anchor line:

```c
/* Return a pointer to db->seg_slot_refs[stream_id][file_id], growing the
   per-stream array if file_id is past the current capacity.  Returns NULL
   on OOM (caller falls back to slow path).  Guarded by the stream's existing
   pool_lock so concurrent read threads racing on the same stream_id cannot
   race on the realloc. */
static SlotRef *seg_ref_for(SlotcaskDb *db, int stream_id, uint32_t file_id) {
    if (!db->seg_slot_refs || !db->seg_slot_caps) return NULL;
    if (stream_id < 0 || stream_id >= db->num_streams) return NULL;
    pthread_mutex_lock(&db->streams[stream_id].pool_lock);
    int cap = db->seg_slot_caps[stream_id];
    if ((int)file_id >= cap) {
        int new_cap = cap ? cap * 2 : 4;
        while (new_cap <= (int)file_id) new_cap *= 2;
        SlotRef *arr = realloc(db->seg_slot_refs[stream_id],
                                (size_t)new_cap * sizeof(SlotRef));
        if (!arr) {
            pthread_mutex_unlock(&db->streams[stream_id].pool_lock);
            return NULL;
        }
        /* Zero-init the new portion (slot = 0, gen = 0 is NOT "invalid"
           because slot 0 is a valid slot.  We distinguish "not yet populated"
           by initialising slot to -1 in the new entries. */
        for (int i = cap; i < new_cap; i++) arr[i].slot = -1;
        db->seg_slot_refs[stream_id] = arr;
        db->seg_slot_caps[stream_id] = new_cap;
    }
    SlotRef *result = &db->seg_slot_refs[stream_id][file_id];
    pthread_mutex_unlock(&db->streams[stream_id].pool_lock);
    return result;
}

```

**Note:** this is the only definition of `seg_ref_for` in the plan — the pool_lock guard is included here directly (earlier drafts of this plan showed an unguarded version here and a guarded "mandatory fix" version later in the Correctness invariants section; those have been consolidated into this single guarded definition to avoid an executor applying the unguarded one and missing the later fix).

### 4d. Replace `segcache_acquire` with `segcache_acquire_direct` in `slotcask_get`

**File:** `src/db/slotcask.c`

**Anchor** (find this exact string inside `slotcask_get`):

```
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, stream_id, file_id);
        SlotcaskSegHandle sh;
        if (segcache_acquire(&sh, path, 0, 0) != 0) return -1;
```

Replace with:

```c
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, stream_id, file_id);
        SlotcaskSegHandle sh;
        SlotRef *seg_ref = seg_ref_for(db, stream_id, file_id);
        if (segcache_acquire_direct(&sh, seg_ref, path) != 0) return -1;
```

### 4e. Replace `segcache_acquire` calls in `slotcask_bulk_lookup_in_kfshard`

In `slotcask_bulk_lookup_in_kfshard`, segment files are accessed inside the Phase 2 loop that groups records by (sid, fid). Each group opens one `segcache_acquire`. Replace those.

**Anchor note**: the 4-line tail of this block (`char path[PATH_MAX]; ... if (segcache_acquire(&h, path, 0, 0) != 0) {`) also appears verbatim as a prefix inside `slotcask_bulk_delete_in_kfshard`'s tombstone loop (different function, different variable name `tomb_idx` instead of `vidx`), so the anchor below includes three lines of preceding loop context (`vidx[run_end]`) to make it unique to `slotcask_bulk_lookup_in_kfshard`. Confirm only one match exists for the full anchor before editing.

**Anchor** (find this exact string inside `slotcask_bulk_lookup_in_kfshard`):

```
            while (run_end < vcount &&
                   st[vidx[run_end]].sid == sid &&
                   st[vidx[run_end]].fid == fid)
                run_end++;
            char path[PATH_MAX];
            seg_path_for(path, db->data_dir, sid, fid);
            SlotcaskSegHandle h;
            if (segcache_acquire(&h, path, 0, 0) != 0) {
```

Replace with:

```c
            while (run_end < vcount &&
                   st[vidx[run_end]].sid == sid &&
                   st[vidx[run_end]].fid == fid)
                run_end++;
            char path[PATH_MAX];
            seg_path_for(path, db->data_dir, sid, fid);
            SlotcaskSegHandle h;
            SlotRef *seg_ref = seg_ref_for(db, (int)sid, (uint32_t)fid);
            if (segcache_acquire_direct(&h, seg_ref, path) != 0) {
```

### 4f. Populate seg_slot_refs at slotcask_open for file_id 0 of each stream

At open time, every stream has `active_file_id` set by `recover_streams`. We prime the seg ref for file_id 0 (which is always created eagerly). For file_id > 0, refs are populated lazily on first `segcache_acquire_direct` slow path.

**Anchor** (find this exact string in `slotcask_open`):

```
    /* Eagerly create file_000 in each stream so the first append doesn't
       race the create path through the cache. */
    for (int i = 0; i < num_streams; i++) {
        char path[PATH_MAX];
        seg_path_for(path, data_dir, i, 0);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 1, 1) != 0) goto fail;
        segcache_release(&h);
    }
```

Replace with:

```c
    /* Eagerly create file_000 in each stream so the first append doesn't
       race the create path through the cache. */
    for (int i = 0; i < num_streams; i++) {
        char path[PATH_MAX];
        seg_path_for(path, data_dir, i, 0);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 1, 1) != 0) goto fail;
        /* Prime the seg slot ref for file_id 0 so point reads hit the
           fast path immediately after open. */
        if (db->seg_slot_refs && h.slot >= 0) {
            SlotRef *ref = seg_ref_for(db, i, 0);
            if (ref) {
                ref->slot = h.slot;
                ref->gen  = atomic_load_explicit(&g_segcache[h.slot].gen,
                                                  memory_order_acquire);
            }
        }
        segcache_release(&h);
    }
```

### 4g. Build check

```bash
SKIP_TESTS=1 ./build.sh
```

Paste output. Must succeed with no errors.

---

## Task 5 — ucache `gen` field infrastructure (struct only, no caller wiring)

The ucache gen field was already added to `UCacheEntry` in Task 1h. This task confirms the field is compile-safe and that `fcache_invalidate` (which also evicts entries) also increments gen, closing the same gap as Tasks 1g/1h closed for kfcache/segcache.

### 5a. Find and update `fcache_invalidate` in `storage.c`

**Anchor** (find this exact string in `storage.c`):

```c
        e->map_size = 0;
        e->slots_per_shard = 0;
        e->dirty = 0;
        e->max_dirty_slot = -1;
        e->used = 0;
        e->path[0] = '\0';
        g_ucache_count--;
```

Replace with:

```c
        e->map_size = 0;
        e->slots_per_shard = 0;
        e->dirty = 0;
        e->max_dirty_slot = -1;
        atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
        e->used = 0;
        e->path[0] = '\0';
        g_ucache_count--;
```

The gen increment goes before `e->used = 0` so that any concurrent reader who observes `used==0` immediately after cannot see a stale gen. The edit happens under `e->rwlock` wrlock (confirmed in the surrounding function body), consistent with the pattern in Tasks 1g/1h.

### 5b. Build check

```bash
SKIP_TESTS=1 ./build.sh
```

Paste output. Must succeed with no errors.

---

## Task 6 — Verification

### 6a. Full test suite

```bash
./build/bin/shard-db-test run-all
```

Required output (last line): `# total: N passed, 0 failed` where N ≥ 34.

Paste the complete output.

### 6b. Post-fix bench

```bash
./build/bin/shard-db-bench bench-kv-parallel
```

Paste the full output and compare with the baseline from before Task 1.

---

## Correctness invariants

The following must hold after this change:

1. **gen is incremented under the table lock or under the entry rwlock (wrlock)**. Never incremented bare. This is satisfied because:
   - `kfcache_drop_slot` is always called under `g_kfcache_lock`.
   - `segcache_drop_slot` is always called under `g_segcache_lock`.
   - `kfcache_invalidate_prefix` / `segcache_invalidate_prefix` increment gen under the per-entry wrlock (which is even stricter than the table lock, since they evict by wrlock-then-clear).
   - `ucache_ensure` eviction happens under `g_ucache_table_mutex`.
   - `fcache_invalidate` eviction happens under the appropriate lock (confirmed in Task 5a).

2. **gen is read with `memory_order_acquire`**. Writers store with `memory_order_release` (via `atomic_fetch_add_explicit(..., memory_order_release)`). The acquire/release pair ensures the caller that observes a matching gen also observes the fully-installed entry (path, fd, map, used).

3. **Identity re-check after rdlock**. Even after a gen match, both `kfcache_acquire_direct` and `segcache_acquire_direct` re-verify `e->used && strcmp(e->path, path) == 0` after taking the rdlock. This closes the TOCTOU window between "gen matched" and "rdlock taken": if the slot was evicted and re-used for a different path in that gap, the identity check fails and we fall back to the slow path. This is the same pattern as the existing retry loop in `kfcache_acquire`.

4. **Slow path fallback always works**. Both `kfcache_acquire_direct` and `segcache_acquire_direct` call the existing acquire functions on any mismatch. The gen and slot in the `SlotRef` are updated after every slow-path hit, so the next call is a warm hit.

5. **Write paths are not affected**. All write paths (`slotcask_insert`, `slotcask_update`, `slotcask_delete`, `slotcask_upsert_with_hooks`, `slotcask_bulk_update`) continue to call `kfcache_acquire(writer=1)`, which goes through the full table-lock path. The `SlotRef` arrays are per-object and are written only by the open-init code (single-threaded at open time) and by the slow path of the direct-acquire functions (serialised by the per-slot rdlock on the update). Write operations on `kf_slot_refs[sid_kf]` from different threads are benign because: (a) each read thread writes `kf_slot_refs[sid_kf]` for the shard it owns in a given request; (b) in the worst case two threads race to update the same ref from the slow path and one overwrites the other — both write the same logical value (same slot, same gen at that instant) so the race is data-race-free only if the struct is `_Atomic`. If the compiler warns about non-atomic access to `SlotRef` fields, wrap the ref-update behind a `__atomic_store` or accept the benign overwrite as an optimistic update (both values are observably equivalent at that point in time). This plan does NOT require atomicity on `SlotRef` struct fields themselves; the correctness guarantee is provided by the generation counter on the cache entry, not by atomic access to the ref.

   **If the build emits data-race warnings or ThreadSanitizer errors on `kf_slot_refs[i].slot` or `.gen`**: use `_Atomic` on the `SlotRef` fields or protect the ref-update with a per-shard spinlock. Do not silence the warning with pragmas.

6. **`seg_slot_refs` growth is thread-safe across concurrent readers on the same stream**. The `seg_ref_for` helper (Task 4c) reallocates the per-stream array, so concurrent point-read threads calling `seg_ref_for` for the same `stream_id` would otherwise be a write-write race on `db->seg_slot_refs[stream_id]`. Task 4c's definition of `seg_ref_for` already guards the whole read-check-realloc-write sequence with `pthread_mutex_lock(&db->streams[stream_id].pool_lock)` (released before every return, including the OOM path) — there is no separate or alternate unguarded version anywhere in this plan; do not strip the lock back out.

---

## Edge cases with required behavior

| Case | Required behavior |
|---|---|
| `kf_slot_refs` is NULL (calloc failed at open) | `kf_ref` pointer passed to `kfcache_acquire_direct` is NULL; function's `if (ref && ref->slot >= 0)` check short-circuits to slow path. No crash. |
| `seg_slot_refs` is NULL | `seg_ref_for` returns NULL; `segcache_acquire_direct` receives NULL ref; falls through to slow path. No crash. |
| file_id 0 not yet primed (race at open) | `kfcache_acquire_direct` returns from slow path with ref updated. Next call hits warm path. |
| LRU evicts a kf shard while a point-read has matched the gen but not yet taken the rdlock | `kfcache_acquire_direct` takes rdlock, sees `e->used == 0` or path mismatch, unlocks, falls to slow path. Correct. |
| kfcache_resplit_locked changes the mmap of an entry | The entry stays at the same cache slot but the mmap pointer changes. The gen does NOT change (resplit does not call `kfcache_drop_slot` — it replaces the mmap in place under the entry's wrlock). The SlotRef gen remains valid; after resplit, callers take rdlock and see the updated `e->base`. This is correct because the gen counter is about slot eviction, not mmap replacement. No change needed. |
| `seg_ref_for` returns a valid pointer but the caller updates `.slot` and `.gen` concurrently from two threads | Both threads write the same logical value after a slow-path hit (same slot, same gen at that moment). The pool_lock guards the realloc growth; the post-realloc individual SlotRef field writes are not guarded. This is safe if SlotRef field widths fit in registers (int + uint64_t on x86_64 are individually load/store atomic by hardware alignment guarantees), but is technically a C data race. TODO: make `SlotRef` fields `_Atomic` or always update under the pool_lock. |

---

## What is NOT changed

- Write paths (`slotcask_insert`, `slotcask_update`, `slotcask_delete`, `upsert_with_hooks`, `bulk_update`): all continue to use the existing `kfcache_acquire(writer=1)` full slow path.
- `ucache` call sites in `query.c` (`fcache_get_read`, `scan_shards`, `recount_worker`): not wired to SlotRef yet. The `gen` field on `UCacheEntry` is available for a future plan that threads `SlotRef *ucache_slot_refs` through `scan_shards`.
- The `btcache` and `bmcache` (B+ tree and bitmap caches): not in scope.
- TLS, server dispatch, auth paths: not affected.
