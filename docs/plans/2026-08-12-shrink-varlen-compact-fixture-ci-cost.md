# Shrink the varlen-compact test fixture's CI cost

## Root cause

`varlen_compact_fixture_build()` (`src/test/cases/varlen_compact_fixture.h`)
is shared by four test cases:

- `test-varlen-compact-recipient-resync`
- `test-varlen-compact-donor-resync`
- `test-varlen-compact-stat-resync`
- `test-compact-crash-recipient-sync` / `test-compact-crash-kf-repoint`
  (via `run_crash_test()` in `test_varlen_compact_crash_mid_migration.c`)

To build its donor/recipient compaction scenario it must force two segment
*rotations* on stream 0. Rotation is driven by `SLOTCASK_SEG_MAX_BYTES`
(`src/db/slotcask.h:43`), hardcoded to 128 MB with no override anywhere in
the codebase (confirmed by grep — the only two non-comment references
outside `slotcask.c` are the fixture's own `key_cap` sizing computation and
nothing else). With `VARLEN_FIXTURE_VALUE_LEN = 8000` bytes per filler
record, each fixture build therefore writes **250+ MB** of real data through
`slotcask_insert()` before it can even begin the test it's actually
checking.

This is the confirmed root cause of three distinct CI symptoms, verified
this session:

1. **ASan job** (`.github/workflows/sanitizers.yml`, `timeout-minutes: 35`):
   successive multi-minute fixture builds across the `*-resync` cases
   accumulate past the job budget → hard "operation was canceled"
   mid-run, not a real test failure. Locally, the identical fixture build
   completes in ~1s on a fast 16-core box — the CI-only slowness is a
   function of GH Actions' shared/weak runners plus ASan's per-access
   instrumentation, not a deadlock (ruled out via local reproduction:
   `run-all --jobs 2 --filter "compact"` passes all 80 assertions in
   ~3.3s, including the two TSan-CI-failing cases).
2. **Coverage job** (`.github/workflows/codecov.yml`, `timeout-minutes: 25`,
   the tightest budget of the three): identical shape, less headroom.
3. **TSan job** (`.github/workflows/tsan.yml`, `timeout-minutes: 150`, so
   the *suite* finishes and reports real "not ok" failures rather than
   being cancelled): `test-compact-crash-recipient-sync` and
   `test-compact-crash-kf-repoint` call `wait_marker(tmpdir, phase, 1,
   20000)` (`test_varlen_compact_crash_mid_migration.c:200`) to detect the
   crash-injection pause *after* the forked child finishes building the
   250+ MB fixture. TSan's documented "~10-15x" overhead (see the comment
   in `tsan.yml`) pushes that fixture build itself past the fixed 20s
   marker-wait window on a slow runner, producing a genuine (not flaky —
   structurally expected under load) "crash pause marker was reached"
   assertion failure. This is a different failure shape from the ASan/
   coverage timeout-cancellation, but the same root cause.

This is pre-existing on `main` (confirmed via `gh run list --branch main`
showing the same FAIL/cancelled pattern on recent pushes), not introduced
by this branch, but it is what's currently blocking this branch's CI.

**Interaction with `docs/plans/2026-08-07-varlen-default-and-fixed-removal.md`**:
that plan removes the *standalone* full-repack engine (`slotcask_compact()`,
`compact_stream_worker`, `CmpSegMap`/`CmpStreamMaps`/`CmpStreamArg`) but
explicitly *retains* `slotcask_compact_segs()` — the function all four slow
test cases above actually call — "for lightweight sparse-segment merging...
preserve their current crash/resynchronization protocol." It also explicitly
keeps these five test files (`test_varlen_compact_recipient_resync.c`,
`_donor_resync.c`, `_stat_resync.c`, `_crash_mid_migration.c`,
`_donor_preserved_on_desync.c`), only adapting them later to construct
VARIABLE fixtures directly instead of migrating from FIXED first, "while
preserving their original resync/crash assertions." So this plan's fix
remains needed after that one lands. Confirmed via grep that no current test
exercises `slotcask_compact()`'s success path at all (only its
nonexistent-object error path, in `test_dispatch_leak_paths.c`) — it is
already effectively dead code relative to this fix, so **`compact_stream_worker`
is deliberately left untouched below**: routing its `SLOTCASK_SEG_MAX_BYTES`
usages through the new accessor would be wasted effort on code the other
plan deletes outright.

## Fix

Add a `TEST_BUILD`-only override for the segment rotation/allocation
threshold, mirroring the existing `slotcask_test_set_after_old_hook` /
`segcache_test_force_identity_mismatches` seam pattern already in
`slotcask.h`/`slotcask.c`. The fixture sets it to 512 KiB before building —
enough to hold ~65 of the 8000-byte filler records per "segment" (well
above the minimum needed to avoid degenerate single-record segments) — so
forcing the same two rotations costs ~1.5 MB of writes instead of 250+ MB
(~170x reduction), while exercising **exactly the same code paths**
(`seg_open_file`, `append_reserve_n`/`append_reserve_single_varlen`
rotation, `slotcask_compact_segs`'s donor/recipient repack, kf repoint) at
the same logical file-count/rotation-count shape the fixture's docstring
describes (file 0 / file 1 / file 2). Nothing about *what* is tested
changes — only how many bytes it costs to set the scenario up.

Confirmed via `grep -n "TEST_BUILD" build.sh`: `shard-db-test` is always
built with `-DTEST_BUILD` regardless of `$MODE_CFLAGS`, so the override is
active under plain, ASan, TSan, and coverage builds alike — production
(non-test) builds never define `TEST_BUILD` and keep the compiled-in 128 MB
constant via a macro fallback, so **zero behavior change in production**.

This diff touches segment lifecycle code (`seg_open_file`, rotation
reservation, compaction repack) that's exercised by concurrent production
paths, so per `AGENTS.md`'s standing exception it must be validated locally
under both `BUILD_MODE=asan` and `BUILD_MODE=tsan` before being called
done, even though the new override itself is TEST_BUILD-gated.

## Task 1 — Add the test-only segment-size override

**File:** `src/db/slotcask.h`

Locate this anchor (existing TEST_BUILD hook block):

```c
#ifdef TEST_BUILD
void segcache_test_force_identity_mismatches(int count);
int  segcache_test_identity_mismatches_remaining(void);
#endif

/* ============================================================ Per-stream pool */
```

Replace with:

```c
#ifdef TEST_BUILD
void segcache_test_force_identity_mismatches(int count);
int  segcache_test_identity_mismatches_remaining(void);
#endif

/* Test-only override for the segment rotation/allocation threshold.
   Crash/compaction fixtures need multiple segment rotations to exercise
   donor/recipient scenarios; at the production 128 MB threshold that means
   writing 250+ MB of filler per test, which starves CI's sanitizer
   time/timeout budgets (see
   docs/plans/2026-08-12-shrink-varlen-compact-fixture-ci-cost.md). 0 means
   "use the compiled-in default". Never present in production builds; no
   production code path reads or writes this override. */
#ifdef TEST_BUILD
size_t slotcask_seg_max_bytes(void);
void slotcask_test_set_seg_max_bytes(size_t bytes);
#else
#define slotcask_seg_max_bytes() SLOTCASK_SEG_MAX_BYTES
#endif

/* ============================================================ Per-stream pool */
```

**File:** `src/db/slotcask.c`

Locate this anchor (end of the existing `g_after_old_*` TEST_BUILD hook
block, immediately before `segcache_init`):

```c
void slotcask_test_after_old(int under_kf_wrlock) {
    slotcask_test_after_old_fn fn;
    void *ctx;
    pthread_mutex_lock(&g_after_old_lock);
    fn = g_after_old_fn;
    ctx = g_after_old_ctx;
    g_after_old_fn = NULL;
    g_after_old_ctx = NULL;
    pthread_mutex_unlock(&g_after_old_lock);
    if (fn) fn(under_kf_wrlock, ctx);
}
#endif

void segcache_init(int cap) {
```

Replace with:

```c
void slotcask_test_after_old(int under_kf_wrlock) {
    slotcask_test_after_old_fn fn;
    void *ctx;
    pthread_mutex_lock(&g_after_old_lock);
    fn = g_after_old_fn;
    ctx = g_after_old_ctx;
    g_after_old_fn = NULL;
    g_after_old_ctx = NULL;
    pthread_mutex_unlock(&g_after_old_lock);
    if (fn) fn(under_kf_wrlock, ctx);
}
#endif

#ifdef TEST_BUILD
static _Atomic size_t g_slotcask_test_seg_max_bytes = 0;

size_t slotcask_seg_max_bytes(void) {
    size_t v = atomic_load_explicit(&g_slotcask_test_seg_max_bytes,
                                    memory_order_acquire);
    return v ? v : SLOTCASK_SEG_MAX_BYTES;
}

void slotcask_test_set_seg_max_bytes(size_t bytes) {
    atomic_store_explicit(&g_slotcask_test_seg_max_bytes, bytes,
                          memory_order_release);
}
#endif

void segcache_init(int cap) {
```

No test-first step here: this task only adds a new, currently-unused
seam (mirrors the existing hooks, which also landed without a dedicated
unit test — they're proven by the tests that consume them, added in
Task 4). Build after this task to confirm it compiles:
`SKIP_TESTS=1 ./build.sh`.

## Task 2 — Route the real segment-size call sites through the accessor

**File:** `src/db/slotcask.c`

11 of the 15 non-comment usages of `SLOTCASK_SEG_MAX_BYTES` in this file
must switch to `slotcask_seg_max_bytes()` so the override actually takes
effect everywhere the four target tests decide a segment's capacity:
`seg_open_file` (2a, 5 sites — shared open/create path used by every
`slotcask_open`/reopen), `append_reserve_n`/`append_reserve_single_varlen`
(2b, 2 sites — the rotation decision every insert goes through), and the
single-threaded donor/recipient repack inside `slotcask_compact_segs`
(2c, 4 sites — the function all four tests call).

The remaining 4 sites live in `compact_stream_worker`, which belongs
exclusively to the standalone `slotcask_compact()` full-repacker being
deleted by `docs/plans/2026-08-07-varlen-default-and-fixed-removal.md`
(see the note above) and isn't exercised by any test today. Leave those 4
sites on the raw `SLOTCASK_SEG_MAX_BYTES` macro — do not touch
`compact_stream_worker` in this plan.

(The macro itself, and its use in `varlen_compact_fixture.h`'s `key_cap`
sizing, are handled separately — Task 3.)

### 2a — `seg_open_file`

Anchor:

```c
/* Open + ftruncate to SLOTCASK_SEG_MAX_BYTES (sparse) + mmap MAP_SHARED. */
static int seg_open_file(const char *path, int create,
                         int *out_fd, uint8_t **out_map, size_t *out_size,
                         dev_t *out_dev, ino_t *out_ino) {
    int fd;
    if (create) {
        char dir[PATH_MAX];
        snprintf(dir, sizeof(dir), "%s", path);
        char *slash = strrchr(dir, '/');
        if (slash) { *slash = 0; mkdirp_local(dir); }
        fd = open(path, O_RDWR | O_CREAT, 0644);
    } else {
        fd = open(path, O_RDWR);
    }
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) < 0) { close(fd); return -1; }
    if ((size_t)st.st_size < SLOTCASK_SEG_MAX_BYTES) {
        if (!create) { close(fd); return -1; }
        if (ftruncate(fd, (off_t)SLOTCASK_SEG_MAX_BYTES) < 0) {
            close(fd); return -1;
        }
    }
    void *m = mmap(NULL, SLOTCASK_SEG_MAX_BYTES, PROT_READ | PROT_WRITE,
                   MAP_SHARED, fd, 0);
    if (m == MAP_FAILED) { close(fd); return -1; }
    /* Transparent huge pages hint — segments are 128 MB sparse files
       walked sequentially during scans and randomly during point reads.
       2 MB hugepages (vs 4 KB) cut TLB entries by 500× over the
       working set. Kernel ignores if THP is off; no functional impact. */
    SHARD_MADV_HUGEPAGE(m, SLOTCASK_SEG_MAX_BYTES);
    *out_fd = fd;
    *out_map = (uint8_t *)m;
    *out_size = SLOTCASK_SEG_MAX_BYTES;
    *out_dev = st.st_dev;
    *out_ino = st.st_ino;
    return 0;
}
```

Replace with:

```c
/* Open + ftruncate to slotcask_seg_max_bytes() (sparse) + mmap MAP_SHARED. */
static int seg_open_file(const char *path, int create,
                         int *out_fd, uint8_t **out_map, size_t *out_size,
                         dev_t *out_dev, ino_t *out_ino) {
    int fd;
    if (create) {
        char dir[PATH_MAX];
        snprintf(dir, sizeof(dir), "%s", path);
        char *slash = strrchr(dir, '/');
        if (slash) { *slash = 0; mkdirp_local(dir); }
        fd = open(path, O_RDWR | O_CREAT, 0644);
    } else {
        fd = open(path, O_RDWR);
    }
    if (fd < 0) return -1;

    size_t seg_max = slotcask_seg_max_bytes();
    struct stat st;
    if (fstat(fd, &st) < 0) { close(fd); return -1; }
    if ((size_t)st.st_size < seg_max) {
        if (!create) { close(fd); return -1; }
        if (ftruncate(fd, (off_t)seg_max) < 0) {
            close(fd); return -1;
        }
    }
    void *m = mmap(NULL, seg_max, PROT_READ | PROT_WRITE,
                   MAP_SHARED, fd, 0);
    if (m == MAP_FAILED) { close(fd); return -1; }
    /* Transparent huge pages hint — segments are 128 MB sparse files
       walked sequentially during scans and randomly during point reads.
       2 MB hugepages (vs 4 KB) cut TLB entries by 500× over the
       working set. Kernel ignores if THP is off; no functional impact. */
    SHARD_MADV_HUGEPAGE(m, seg_max);
    *out_fd = fd;
    *out_map = (uint8_t *)m;
    *out_size = seg_max;
    *out_dev = st.st_dev;
    *out_ino = st.st_ino;
    return 0;
}
```

### 2b — `append_reserve_n` and `append_reserve_single_varlen`

Anchor:

```c
static int append_reserve_n(SlotcaskDb *db, SlotcaskStream *p,
                            size_t n, uint32_t *file_id_out,
                            uint32_t *offsets_out) {
    pthread_mutex_lock(&p->rotation_lock);
    size_t need = n * (size_t)db->slot_size;
    if (p->reserve_off + need > SLOTCASK_SEG_MAX_BYTES) {
        /* Rotate. */
        p->active_file_id++;
        p->reserve_off = 0;
    }
    *file_id_out = p->active_file_id;
    for (size_t i = 0; i < n; i++) {
        offsets_out[i] = (uint32_t)(p->reserve_off + i * (size_t)db->slot_size);
    }
    p->reserve_off += need;
    pthread_mutex_unlock(&p->rotation_lock);
    return 0;
}

/* Reserve a single variable-length slot. rec_size includes the header
   + key + value + alignment padding. Rotates if not enough space in the
   active segment. Returns 0 on success, -1 on error. */
static int append_reserve_single_varlen(SlotcaskDb *db, SlotcaskStream *p,
                                         size_t rec_size,
                                         uint32_t *file_id_out,
                                         uint32_t *offset_out) {
    (void)db;
    pthread_mutex_lock(&p->rotation_lock);
    if (p->reserve_off + rec_size > SLOTCASK_SEG_MAX_BYTES) {
        p->active_file_id++;
        p->reserve_off = 0;
    }
    *file_id_out = p->active_file_id;
    *offset_out = (uint32_t)(p->reserve_off);
    p->reserve_off += rec_size;
    pthread_mutex_unlock(&p->rotation_lock);
    return 0;
}
```

Replace with:

```c
static int append_reserve_n(SlotcaskDb *db, SlotcaskStream *p,
                            size_t n, uint32_t *file_id_out,
                            uint32_t *offsets_out) {
    pthread_mutex_lock(&p->rotation_lock);
    size_t need = n * (size_t)db->slot_size;
    if (p->reserve_off + need > slotcask_seg_max_bytes()) {
        /* Rotate. */
        p->active_file_id++;
        p->reserve_off = 0;
    }
    *file_id_out = p->active_file_id;
    for (size_t i = 0; i < n; i++) {
        offsets_out[i] = (uint32_t)(p->reserve_off + i * (size_t)db->slot_size);
    }
    p->reserve_off += need;
    pthread_mutex_unlock(&p->rotation_lock);
    return 0;
}

/* Reserve a single variable-length slot. rec_size includes the header
   + key + value + alignment padding. Rotates if not enough space in the
   active segment. Returns 0 on success, -1 on error. */
static int append_reserve_single_varlen(SlotcaskDb *db, SlotcaskStream *p,
                                         size_t rec_size,
                                         uint32_t *file_id_out,
                                         uint32_t *offset_out) {
    (void)db;
    pthread_mutex_lock(&p->rotation_lock);
    if (p->reserve_off + rec_size > slotcask_seg_max_bytes()) {
        p->active_file_id++;
        p->reserve_off = 0;
    }
    *file_id_out = p->active_file_id;
    *offset_out = (uint32_t)(p->reserve_off);
    p->reserve_off += rec_size;
    pthread_mutex_unlock(&p->rotation_lock);
    return 0;
}
```

### 2c — single-threaded compact repack (donor/recipient dest sizing)

Anchor:

```c
            /* Open dest if needed or rotate when full. */
            if (!dest[sid].base ||
                dest_off[sid] + rec_size > SLOTCASK_SEG_MAX_BYTES) {
                if (dest[sid].base) {
                    size_t used = dest_off[sid];
                    munmap(dest[sid].base, dest[sid].alloc);
                    dest[sid].base = NULL;
                    if (ftruncate(dest[sid].fd, (off_t)used) < 0) {
                        close(dest[sid].fd);
                        dest[sid].fd = -1;
                        kfcache_release(&kh);
                        goto fail;
                    }
                    close(dest[sid].fd);
                    dest[sid].fd   = -1;
                    dest_fid[sid]++;
                    dest_off[sid] = 0;
                }
                char np[PATH_MAX];
                seg_path_for(np, db->data_dir, sid, dest_fid[sid]);
                { char d2[PATH_MAX]; snprintf(d2, sizeof(d2), "%s", np);
                  char *sl = strrchr(d2, '/'); if (sl) { *sl = '\0'; mkdirp_local(d2); } }
                int fd = open(np, O_RDWR | O_CREAT | O_TRUNC, 0644);
                if (fd < 0) { kfcache_release(&kh); goto fail; }
                if (ftruncate(fd, (off_t)SLOTCASK_SEG_MAX_BYTES) < 0)
                    { close(fd); kfcache_release(&kh); goto fail; }
                void *dm = mmap(NULL, SLOTCASK_SEG_MAX_BYTES,
                                PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                if (dm == MAP_FAILED)
                    { close(fd); kfcache_release(&kh); goto fail; }
                dest[sid].base  = (uint8_t *)dm;
                dest[sid].alloc = SLOTCASK_SEG_MAX_BYTES;
                dest[sid].fd    = fd;
            }
```

Replace with:

```c
            /* Open dest if needed or rotate when full. */
            size_t seg_max = slotcask_seg_max_bytes();
            if (!dest[sid].base ||
                dest_off[sid] + rec_size > seg_max) {
                if (dest[sid].base) {
                    size_t used = dest_off[sid];
                    munmap(dest[sid].base, dest[sid].alloc);
                    dest[sid].base = NULL;
                    if (ftruncate(dest[sid].fd, (off_t)used) < 0) {
                        close(dest[sid].fd);
                        dest[sid].fd = -1;
                        kfcache_release(&kh);
                        goto fail;
                    }
                    close(dest[sid].fd);
                    dest[sid].fd   = -1;
                    dest_fid[sid]++;
                    dest_off[sid] = 0;
                }
                char np[PATH_MAX];
                seg_path_for(np, db->data_dir, sid, dest_fid[sid]);
                { char d2[PATH_MAX]; snprintf(d2, sizeof(d2), "%s", np);
                  char *sl = strrchr(d2, '/'); if (sl) { *sl = '\0'; mkdirp_local(d2); } }
                int fd = open(np, O_RDWR | O_CREAT | O_TRUNC, 0644);
                if (fd < 0) { kfcache_release(&kh); goto fail; }
                if (ftruncate(fd, (off_t)seg_max) < 0)
                    { close(fd); kfcache_release(&kh); goto fail; }
                void *dm = mmap(NULL, seg_max,
                                PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                if (dm == MAP_FAILED)
                    { close(fd); kfcache_release(&kh); goto fail; }
                dest[sid].base  = (uint8_t *)dm;
                dest[sid].alloc = seg_max;
                dest[sid].fd    = fd;
            }
```

`seg_max` is declared once per loop iteration here (cheap, matches the
existing style of computing per-iteration locals like `rec_size` just
above this block) rather than hoisting it out, since the surrounding loop
structure (quoted anchor) doesn't currently have a place to hoist to
without a wider, riskier restructure.

Build after this task: `SKIP_TESTS=1 ./build.sh`. Grep to confirm exactly
the 4 expected sites remain on the raw macro (all inside
`compact_stream_worker`, left untouched per the note in Task 2's intro) plus
the `#define` and doc-comments: `grep -n SLOTCASK_SEG_MAX_BYTES src/db/slotcask.c`.

## Task 3 — Shrink the fixture and thread the override through the 4 tests

**File:** `src/test/cases/varlen_compact_fixture.h`

Anchor:

```c
#define VARLEN_FIXTURE_VALUE_LEN 8000u
```

Replace with:

```c
#define VARLEN_FIXTURE_VALUE_LEN 8000u

/* Test-only segment rotation threshold used while building this fixture.
   Callers must call slotcask_test_set_seg_max_bytes(VARLEN_FIXTURE_TEST_SEG_BYTES)
   before slotcask_open()/this function, and reset it to 0 once the whole
   test (including any later reopen/compact) is done. ~65 records of
   VARLEN_FIXTURE_VALUE_LEN fit per "segment" here — well clear of a
   degenerate single-record segment — so forcing the fixture's two
   rotations costs ~1.5 MB instead of the production 128 MB default's
   250+ MB, without changing which code paths or how many
   files/rotations are exercised. */
#define VARLEN_FIXTURE_TEST_SEG_BYTES (512u * 1024u)
```

Anchor:

```c
    size_t rec_size = (24u + 10u + VARLEN_FIXTURE_VALUE_LEN + 7u) & ~7u;
    size_t key_cap = (size_t)(SLOTCASK_SEG_MAX_BYTES / rec_size) + 4u;
```

Replace with:

```c
    size_t rec_size = (24u + 10u + VARLEN_FIXTURE_VALUE_LEN + 7u) & ~7u;
    size_t key_cap = (size_t)(slotcask_seg_max_bytes() / rec_size) + 4u;
```

**File:** `src/test/cases/test_varlen_compact_recipient_resync.c`

Anchor:

```c
    slotcask_init(64, 64);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192);
```

Replace with:

```c
    slotcask_init(64, 64);
    slotcask_test_set_seg_max_bytes(VARLEN_FIXTURE_TEST_SEG_BYTES);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192);
```

Anchor:

```c
    slotcask_close(&db);
    slotcask_shutdown();

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-recipient-resync", test_varlen_compact_recipient_resync_run)
```

Replace with:

```c
    slotcask_close(&db);
    slotcask_test_set_seg_max_bytes(0);
    slotcask_shutdown();

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-recipient-resync", test_varlen_compact_recipient_resync_run)
```

**File:** `src/test/cases/test_varlen_compact_donor_resync.c`

Anchor:

```c
    slotcask_init(64, 64);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192);
```

Replace with:

```c
    slotcask_init(64, 64);
    slotcask_test_set_seg_max_bytes(VARLEN_FIXTURE_TEST_SEG_BYTES);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192);
```

Anchor:

```c
    slotcask_close(&db);
    slotcask_shutdown();

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-donor-resync", test_varlen_compact_donor_resync_run)
```

Replace with:

```c
    slotcask_close(&db);
    slotcask_test_set_seg_max_bytes(0);
    slotcask_shutdown();

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-donor-resync", test_varlen_compact_donor_resync_run)
```

**File:** `src/test/cases/test_varlen_compact_stat_resync.c`

Anchor:

```c
    slotcask_init(64, 64);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192); /* single stream: deterministic file layout */
```

Replace with:

```c
    slotcask_init(64, 64);
    slotcask_test_set_seg_max_bytes(VARLEN_FIXTURE_TEST_SEG_BYTES);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192); /* single stream: deterministic file layout */
```

Anchor:

```c
    slotcask_close(&db);
    slotcask_shutdown();

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-stat-resync", test_varlen_compact_stat_resync_run)
```

Replace with:

```c
    slotcask_close(&db);
    slotcask_test_set_seg_max_bytes(0);
    slotcask_shutdown();

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-stat-resync", test_varlen_compact_stat_resync_run)
```

## Task 4 — Thread the override through `run_crash_test` (4 return paths)

**File:** `src/test/cases/test_varlen_compact_crash_mid_migration.c`

This function has multiple early returns; the override must be set once
before the `fork()` (so the forked child inherits it, and the parent's own
later reopen at Phase 2 sees the same segment size the child used to
create the files) and reset on **every** return path so it can't leak into
whatever test runs next in the same process under `run-all --jobs 1`.

Anchor:

```c
static int run_crash_test(const char *phase) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-compact-crash-%s-XXXXXX", phase);
    if (!mkdtemp(tmpdir)) {
        ASSERT_TRUE(0, "mkdtemp");
        return 1;
    }

    /* Phase 1: build fixture, trigger compaction with pause, SIGKILL. */
    pid_t pid = fork();
    ASSERT_TRUE(pid >= 0, "fork");
    if (pid < 0) { rmdir(tmpdir); return 1; }

    if (pid == 0) {
        compact_child_main(tmpdir, phase);
        _exit(99); /* unreachable */
    }
```

Replace with:

```c
static int run_crash_test(const char *phase) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-compact-crash-%s-XXXXXX", phase);
    if (!mkdtemp(tmpdir)) {
        ASSERT_TRUE(0, "mkdtemp");
        return 1;
    }

    /* Small test-only segment size: the production 128 MB default would
       need 250+ MB of filler writes to force the rotation this fixture
       requires, which starves CI's sanitizer time/timeout budgets (see
       docs/plans/2026-08-12-shrink-varlen-compact-fixture-ci-cost.md).
       Set before fork() so the child inherits it and the parent's own
       Phase-2 reopen below sees the same segment size the child used to
       create the files. Reset on every return path so it never leaks
       into the next test in a sequential --jobs 1 run. */
    slotcask_test_set_seg_max_bytes(VARLEN_FIXTURE_TEST_SEG_BYTES);

    /* Phase 1: build fixture, trigger compaction with pause, SIGKILL. */
    pid_t pid = fork();
    ASSERT_TRUE(pid >= 0, "fork");
    if (pid < 0) {
        slotcask_test_set_seg_max_bytes(0);
        rmdir(tmpdir);
        return 1;
    }

    if (pid == 0) {
        compact_child_main(tmpdir, phase);
        _exit(99); /* unreachable */
    }
```

Anchor:

```c
        TAP_DIAG("# phase %s: marker not reached (child rc=%d)\n", phase, child_rc);
        return 1;
    }
```

Replace with:

```c
        TAP_DIAG("# phase %s: marker not reached (child rc=%d)\n", phase, child_rc);
        slotcask_test_set_seg_max_bytes(0);
        return 1;
    }
```

Anchor:

```c
    int rc = slotcask_open(&db, tmpdir, 8, 1, 8192);
    ASSERT_EQ_INT(rc, 0, "reopen database after simulated crash");
    if (rc != 0) { slotcask_shutdown(); rmdir(tmpdir); return 1; }
```

Replace with:

```c
    int rc = slotcask_open(&db, tmpdir, 8, 1, 8192);
    ASSERT_EQ_INT(rc, 0, "reopen database after simulated crash");
    if (rc != 0) {
        slotcask_test_set_seg_max_bytes(0);
        slotcask_shutdown();
        rmdir(tmpdir);
        return 1;
    }
```

Anchor:

```c
    slotcask_close(&db);
    slotcask_shutdown();

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", tmpdir);
    system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_compact_crash_recipient_sync_run(void) {
```

Replace with:

```c
    slotcask_close(&db);
    slotcask_test_set_seg_max_bytes(0);
    slotcask_shutdown();

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", tmpdir);
    system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_compact_crash_recipient_sync_run(void) {
```

## Task 5 — Validate

Build and run the affected tests, confirming they still pass and now run
far faster (proving the fixture still exercises the same rotation/donor/
recipient scenario, just cheaply):

```bash
SKIP_TESTS=1 ./build.sh
time ./build/bin/shard-db-test run-all --jobs 2 --filter "compact"
```

All 6 cases (`test-varlen-compact-recipient-resync`, `-donor-resync`,
`-stat-resync`, `test-compact-crash-recipient-sync`, `-kf-repoint`, plus
any other case matching "compact") must pass, and wall time should drop
from the previous local baseline of ~3.3s to a small fraction of that
(paste actual before/after timing in the review).

Then run the full suite once to confirm no other case depended on the old
fixture size:

```bash
./build/bin/shard-db-test run-all
```

Then the repo's mandatory dynamic-safety gate, since this diff touches
segment lifecycle code (`seg_open_file`, rotation reservation, compaction
repack) used by concurrent production paths:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
    ./build/bin/shard-db-test run-all --jobs 2 --filter "compact"

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
    ./build/bin/shard-db-test run-all --jobs 1 --filter "compact"
```

Both must be clean (no new sanitizer findings, all TAP assertions pass).
Paste the real output for both runs in the review — this is the direct
stand-in for a "before/after" regression proof here: the *bug* being fixed
is CI-only slowness/timeout that doesn't reproduce locally, so the
proof-of-fix is the fixture completing correctly at drastically reduced
cost under the same sanitizer instrumentation that CI uses, not a
local repro-then-fix of a functional defect.

## Out of scope (tracked separately, not fixed by this plan)

- `test-parallel-index-integrity` hang on plain (non-sanitizer) Linux
  x86_64 — not yet reproduced or root-caused.
- ARM `after-metadata` rebuild-pause failures and the macOS cascade
  failures (`not ok 106-110`) — plausibly the same class of runner-
  resource-starvation issue, not yet confirmed to share this root cause.
- cppcheck's `query.c:5917` uninitialized-variable finding and related
  `nullPointer`/`ignoredReturnValue` warnings — unrelated code path, not
  investigated at the source level yet.

## Execution rules

- Branch off `main` (or continue on the current branch
  `fix/rebuild-txn-recovery-cleanup-cascade` if the human confirms that's
  where this should land — confirm before starting).
- Do tasks in order (1 → 5); Task 2 depends on Task 1's accessor existing,
  Task 3/4 depend on Task 2's call sites already routing through it (so
  the override actually has an effect when Task 3/4's tests set it).
- Build/test commands for this repo: `SKIP_TESTS=1 ./build.sh` to build,
  `./build/bin/shard-db-test run-all[-filter]`/`run <name>` to test, per
  `AGENTS.md`.
- Per this repo's standing execution-mode exception, leave the diff
  **uncommitted** when done — the reviewing agent and human review the raw
  `git diff` before anything is committed.
- If a quoted anchor isn't found exactly as written, stop immediately,
  write `PLAN_NOTES.md` describing the mismatch, and halt the entire run
  — do not guess, reinterpret, or continue to any other task.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.
