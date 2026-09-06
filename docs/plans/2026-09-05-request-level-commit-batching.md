# Plan: request-level commit batching (two-epoch waves, per-shard writer gates, marker V2)

Date: 2026-09-05 (revision 7 — locks the round-5 protocol: per-shard
writer-admission gates held request-wide by the coordinator, phase-local kf
locks, opaque per-window hook state, MarkerRef gate once per shard in the
stage pass, request-level K/A/I/T durability, marker V2 with exact-size
validation)
Status: awaiting human review
Baseline: `main` at `bc1dede` (merge of `perf/bulk-commit-batched-sync`)
Previous plan: `docs/plans/2026-09-04-bulk-commit-throughput-and-durability.md` (merged)

## Goal

Remove per-window durability barrier overhead. On the reference disk
(~6.9 ms per fsync-class wait) a 1M-record kv-parallel insert executes
~9,700 durable ops (256 windows × ~38) and a 100k × 14-index insert ~8,400.
Two-epoch wave batching collapses these to three request-level flush passes
plus one fsync per marker file: kv-parallel ~600–700 ops (~10×), indexed
insert ~250 index ops (~5–10×). Marker V2 removes write amplification and
the 64 MiB marker-size ceiling for large-record schemas.

## Decisions of record

| Decision | Value |
|---|---|
| Writer exclusion | per-kf-shard `writer_gate` mutex, acquired request-wide by the coordinator (ascending order), released reverse after the commit flush; ordinary writers take it around their mutation |
| Request concurrency | requests on disjoint shards run concurrently; requests sharing a shard serialize on that shard's gate; single writes to untouched shards proceed; readers never touch the gate |
| kf rwlock | phase-local only: every wave task acquires and releases it within the same task (a POSIX rwlock must not cross threads) |
| Failure scope | shard- and window-scoped predicates: `may_publish(shard) = !stage_failed[s] && payload_rc == 0`; `may_finalize(window) = window.published && marker_dir_rc == 0` |
| Hook protocol | prepare/apply/commit_done/abort/release gain opaque per-window `window_state`; slotcask.c stores it in ReqWindow opaquely |
| Marker publish primitives | deferred coordinator calls `marker_publish_file_atomic` and batches the dir fsync; legacy/single coordinator calls `marker_publish_and_sync_dir` immediately |
| Marker V2 | 16 B header + `count` × 32 B `KfMarkerSlot`, exact-size validated, `1 ≤ count ≤ 16384`; v1 refused per policy (a) |
| Legacy durability | `marker_publish_and_sync_dir` retains the immediate directory fsync for single-record and non-deferred paths |
| Single-record path | unchanged behavior; takes `writer_gate[shard]` around its mutation |
| Sanitizer gate | per AGENTS.md: local all-core ASan ×3 and TSan ×3, no `--jobs` |

Out of scope: group commit across concurrent single-record requests; marker
compression; async barriers.

## Root causes

1. **Latency amplification**: six barrier groups per window, paid redundantly
   across windows that touch identical files.
2. **Write amplification**: V1 markers duplicate key+old+new per record only
   so the parser can checksum them; replay re-derives everything from the
   segment records named by `KfMarkerSlot` (`slotcask.c:1006-1009`).
3. **Marker-size ceiling**: the reader fails closed on markers > 64 MiB
   (`slotcask.c:1023-1026`); a 4,096-record window of 64 KiB values produces
   ~268 MB → a crash bricks startup for large-record schemas. V2's worst
   case is 16 + 16384×32 = **524,304 B**.
4. **Stream dir-entry durability hole (pre-existing on main)**:
   `seg_open_file` creates files with `O_CREAT` and nothing fsyncs
   `data/streams/NNN`.

## Invariants

- **I1**: index mutations durable before the marker that would re-apply them
  is cleared (the commit flush precedes every marker clear).
- **I2**: segment payload bytes durable before any marker that references
  them is durable (epoch-1 flush for P-wave records; **immediate pre-marker
  sync** for M-phase-fallback records — D5).
- **I3**: stream directory entries of created/rotated segment files durable
  before any marker references the file (Task 2: immediate fsync at
  creation).
- **I4**: marker dir entries durable (one `fsync(data/kf dir)` after the
  publish wave) before any window mutation (A/I/K/T) runs.
- **I5**: a touched shard is exclusively reserved against mutating writers
  from before its gate replay until after its batched clear — enforced by
  the per-shard `writer_gate`, held by the coordinator for the whole
  request. Single writers to the shard queue on the same gate.
- **Fail closed**: any failure → affected markers retained (or never
  published) → forward replay at next open. Shard-/window-scoped predicates
  per the state machine in Task 3a.

Known trade (documented, accepted): single writers to a touched shard stall
for the deferred request's span (the coordinator holds that shard's gate).
Readers of touched shards never stall on the gate; between waves they see
coherent states only (old committed record before finalize, new committed
record after; flag=0 staged payloads are invisible to primary-key reads).
This is not request-level snapshot isolation: multi-shard requests may be
observed shard-by-shard, and secondary-index scans keep the current
per-mutation visibility contract. The feature changes durability batching,
not the database's documented atomicity/isolation boundary.

## Execution rules

- Branch `perf/request-level-commit-batching` off `main`. Execution order is
  Task 1 → Task 2 → write the Task 3f + Task 4 tests red → implement Tasks
  3 and 4 as one compile-green protocol unit → Task 5. The coordinator,
  exact-path publisher, opaque plan state, and V2 serializer intentionally
  form one unit; do not stop at a knowingly uncompilable intermediate
  checkpoint merely to separate their headings. All work remains
  uncommitted through review.
- Build `SKIP_TESTS=1 ./build.sh`; tests `./build/bin/shard-db-test run[-all]`.
- Sanitizer gate per AGENTS.md: local all-core `BUILD_MODE=asan` ×3 and
  `BUILD_MODE=tsan` ×3 full-suite runs. If full-parallel sanitizer runs trip
  the 180 s watchdog on this box (observed 2026-09-04), halt and report to
  the human — do not self-modify the invocation.
- Leave work uncommitted for raw-diff review. The human runs benches.
- Quoted anchor not found exactly → write `PLAN_NOTES.md`, halt the run.

---

## Task 1 — parallel window index flush (fix 2)

**Root cause.** The window flush seam calls `index_sync_record_fields` once
per pair with `nfields=1` (serial branch): up to 112 sequential fdatasyncs
to 112 distinct files.

**Test-first.** No new behavioral test — parallel issue is
latency-unobservable in-suite; failure semantics stay guarded by
`SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_I)`, `index_test_should_fail_after_success`,
and the durability suites. (Documented deviation.)

**Add — `src/db/index.c`**, after `index_sync_record_fields` (anchor: its
final lines `free(args); if (rc != 0) errno = saved_err ? saved_err : EIO; return rc; }`):

```c
/* ── Window-flush issuer ────────────────────────────────────────────────
   The bulk window flush seam hands over fdatasync targets that are all
   distinct files: issue them through parallel_for (same contract as
   index_sync_record_fields for nfields > 1) instead of one blocking wait
   per file. Any failure → -1 with errno set to the first failing
   flush's errno (or EIO). */
typedef struct {
    const char *path;
    int rc;
    int err;
} PathSyncArg;

static void *path_sync_thread_fn(void *p) {
    PathSyncArg *a = (PathSyncArg *)p;
    a->rc = btree_sync_path(a->path);
    a->err = a->rc != 0 ? errno : 0;
    return NULL;
}

int index_sync_path_set(const char *const *paths, size_t n) {
    if (n == 0) return 0;
    if (!paths || n > (size_t)INT_MAX) { errno = EINVAL; return -1; }
    PathSyncArg *args = malloc(n * sizeof(*args));
    if (!args) {
        int rc = 0, saved_err = 0;
        for (size_t i = 0; i < n; i++)
            if (btree_sync_path(paths[i]) != 0) {
                if (!saved_err) saved_err = errno;
                rc = -1;
            }
        if (rc != 0) errno = saved_err ? saved_err : EIO;
        return rc;
    }
    for (size_t i = 0; i < n; i++) {
        args[i].path = paths[i];
        args[i].rc = 0;
        args[i].err = 0;
    }
    if (n == 1)
        path_sync_thread_fn(&args[0]);
    else
        parallel_for(path_sync_thread_fn, args, n, sizeof(*args));
    int rc = 0, saved_err = 0;
    for (size_t i = 0; i < n; i++)
        if (args[i].rc != 0) {
            rc = -1;
            if (!saved_err) saved_err = args[i].err;
        }
    free(args);
    if (rc != 0) errno = saved_err ? saved_err : EIO;
    return rc;
}
```

**Declare — `src/db/types.h`**, after `bitmap_sync_shard_path`:

```c
/* Fdatasync a set of distinct index-file paths in parallel (bulk window
   flush seam). Paths must be distinct files; any failure → -1. */
int index_sync_path_set(const char *const *paths, size_t n);
```

**Rewire — `src/db/slotcask.c`**, `bulk_apply_and_sync_indexes_locked`:
replace the flush block (anchor: the block opening `uint64_t t0i = now_us();`
below the comment `/* one durable sync per touched (field, idx shard): dedupe,
then flush */`) with:

```c
    {
        uint64_t t0i = now_us();
        /* Bitmap files: cache-handle sync, serial (bounded by bitmap
           fields × kf shards; the bm cache serialises writers per file). */
        for (size_t i = 0; i < plan->touch.n; i++) {
            const IdxTouch *t = &plan->touch.v[i];
            if (t->type != IT_BITMAP) continue;
            if (bitmap_sync_shard_path(eff_root, object, t->field,
                                       t->idx_shard,
                                       txn->db->num_shards) != 0)
                return -1;
            __atomic_add_fetch(&g_commit_index_sync_ops_total, 1,
                               __ATOMIC_RELAXED);
        }
        /* btree/trigram pairs: distinct files → parallel issue. */
        size_t npaths = 0;
        for (size_t i = 0; i < plan->touch.n; i++)
            if (plan->touch.v[i].type != IT_BITMAP) npaths++;
        if (npaths > 0) {
            char *path_buf = malloc(npaths * PATH_MAX);
            const char **paths = malloc(npaths * sizeof(*paths));
            if (!path_buf || !paths) {
                free(path_buf); free(paths);
                errno = ENOMEM;
                return -1;
            }
            size_t w = 0;
            for (size_t i = 0; i < plan->touch.n; i++) {
                const IdxTouch *t = &plan->touch.v[i];
                if (t->type == IT_BITMAP) continue;
                int shard = idx_shard_for_hash(t->hash16,
                                               txn->db->num_shards);
                if (t->type == IT_TRIGRAM)
                    tg_build_path(path_buf + w * PATH_MAX, PATH_MAX,
                                  eff_root, object, t->field, shard);
                else
                    build_idx_path(path_buf + w * PATH_MAX, PATH_MAX,
                                   eff_root, object, t->field, shard);
                paths[w] = path_buf + w * PATH_MAX;
                w++;
            }
            int frc = index_sync_path_set(paths, w);
            free(path_buf);
            free(paths);
            if (frc != 0) return -1;
            __atomic_add_fetch(&g_commit_index_sync_ops_total,
                               (uint64_t)w, __ATOMIC_RELAXED);
        }
        commit_phase_us_record(&g_commit_index_sync_us_total, t0i);
    }
```

**Verify**: build clean; `run-all`; `test-bulk-idx-sync-batching`,
`test-bulk-idx-types-batching`, `test-single-op-index-sync` green.

---

## Task 2 — stream directory durability at the segment seam (every path)

**Root cause.** `seg_open_file` creates segment files with `O_RDWR | O_CREAT`
+ `mkdirp_local(dir)`; nothing fsyncs `data/streams/NNN`. A crash can lose a
newly created file's directory entry while a durable marker references it.
This is a main-branch bug for single-record writes too.

**Test-first.** Crash-observable only; no in-suite red/green (documented
deviation). Guarded by `test-durability-sigkill-marker-after-write-recovers`
staying green.

**Change — `src/db/slotcask.c`, `seg_open_file`.** Replace:

```c
    int fd;
    if (create) {
        char dir[PATH_MAX];
        int dn = snprintf(dir, sizeof(dir), "%s", path);
        if (dn < 0 || (size_t)dn >= sizeof(dir)) {
            errno = ENAMETOOLONG;
            return -1;
        }
        char *slash = strrchr(dir, '/');
        if (!slash) { errno = EINVAL; return -1; }
        *slash = 0;
        mkdirp_local(dir);
        fd = open(path, O_RDWR | O_CREAT, 0644);
    } else {
        fd = open(path, O_RDWR);
    }
    if (fd < 0) return -1;
```

with:

```c
    int fd;
    if (create) {
        char dir[PATH_MAX];
        snprintf(dir, sizeof(dir), "%s", path);
        char *slash = strrchr(dir, '/');
        if (slash) { *slash = 0; mkdirp_local(dir); }
        fd = open(path, O_RDWR | O_CREAT, 0644);
        if (fd >= 0) {
            /* I3: a freshly created segment file's directory entry must be
               durable before any marker can reference the file. Creation is
               rare (first touch / rotation), so an immediate dir fsync is
               cheap and covers every writer path — single-record, legacy
               bulk, and deferred requests alike. A pre-existing empty file
               is indistinguishable from a new one; the extra fsync is
               harmless. */
            struct stat dst;
            if (fstat(fd, &dst) != 0) {
                close(fd);
                return -1;
            }
            if (dst.st_size == 0 && fsync_dir(dir) != 0) {
                close(fd);
                return -1;
            }
        }
    } else {
        fd = open(path, O_RDWR);
    }
    if (fd < 0) return -1;
```

If `fsync_dir` is declared after `seg_open_file` in this file, move its
forward declaration above. Any other mismatch → PLAN_NOTES.

**Verify**: build clean; SIGKILL recovery suite; `run-all`.

---

## Task 3 — two-epoch coordinator (fix 1 + L3, invariants I1–I5)

**Root cause.** Per-window barrier groups (P sync, marker publish+dir, A
sync, I sync, K msync, T sync, C dir sync) are paid by every window and,
because hash routing spreads records evenly, redundantly across windows
that touch the same files.

**Design (locked, review round 4).** Two locks with separate jobs:

- **`writer_gate[shard]`** — a plain per-shard admission mutex used only by
  mutating writers. The deferred request's coordinator acquires the gates
  of every touched shard (ascending) and holds them for the entire
  request; ordinary writers lock the gate for their target shard around
  their mutation; readers never touch it.
- **the existing kf rwlock** — protects actual shard access; acquired and
  released *phase-locally* by each wave task (a POSIX rwlock must not be
  acquired by one thread and released by another, and pool threads may
  differ between waves).

Waves (same phase for every touched shard, concurrently, then join):

```
coordinator acquires touched writer gates (ascending shard order)
  stage wave    : per shard — kf wrlock → gate replay → kf wrlock release
                  → stage payloads (collect p_locs, no sync)
  join → payload flush (request-wide merged dedup, one pass)     [I2]
  publish wave  : per shard — kf wrlock → per window: plan → D5 sync →
                  marker_publish_file_atomic → kf wrlock release
  join → ONE fsync(data/kf dir)                                  [I4]
  finalize wave : per shard — kf wrlock → per published window:
                  A → I(apply) → K → T (syncs deferred) → kf wrlock release
  join → commit flush: merged index flush [I1] + merged A/T sync
                 + batched unlink of converged markers
                 + ONE fsync(data/kf dir)
coordinator releases touched writer gates (reverse order)
```

**Reader visibility between waves** (intentional and safe): after the stage
wave readers see the old committed record (staged payloads carry flag=0 and
are invisible); after the publish wave readers still see the old record
(markers are invisible metadata); during a shard's finalize, readers of
that shard wait on its kf wrlock; after the finalize wave readers see the
new committed record.

**Why the kf rwlock is not held across waves**: pool threads may differ
between waves, and a POSIX rwlock must not be acquired by one thread and
released by another. Holding it would also stall readers through the
payload and marker-dir fsyncs. The writer gate is what prevents another
*writer* from mutating the shard between waves; readers between waves see
only the coherent states above.

**Required concurrency behavior** (each is asserted by a test in 3e):
requests on disjoint shards of one object run concurrently; requests
sharing a shard serialize only on that shard; a single write to an
untouched shard proceeds during a deferred request; a single write to a
touched shard waits for gate release; readers never wait on writer gates;
readers of touched shards see only coherent old/new records; gates are
acquired in ascending shard order (no deadlock).

### 3a. Exact predicates and state machine

```c
/* per shard */ may_publish[s] = !stage_failed_by_shard[s] && payload_rc == 0;
/* per window */ may_finalize[w] = w.published && marker_dir_rc == 0;
```

Rules (verbatim policy):

- A stage failure prevents only that shard from publishing.
- A request-wide payload-flush failure prevents every marker publication.
- One window's marker-publish failure must not prevent successfully
  published windows from finalizing.
- The aggregate `publish_rc` affects the request result, not the eligibility
  of successful windows.
- A marker-directory fsync failure prevents all published windows from
  finalizing (their directory entries are not known durable) — published
  markers remain on disk for replay.
- The commit flush always runs after the finalize join, to handle every
  window that reached finalize.
- The aggregate request status is computed after cleanup; it must not be
  used to skip required convergence work.

State machine:

| Event | Handling |
|---|---|
| Payload flush failure | no publish wave; no markers exist; request −1; staged data inert (flag=0) |
| Per-shard stage failure | that shard publishes nothing (`stage_failed_by_shard[s]`); other shards proceed |
| Marker publish failure (one window) | that window is not registered as published; successful windows finalize normally; `publish_rc` fails the request result |
| Marker-dir fsync failure | finalize wave skipped entirely; published markers retained → replay at next open |
| Finalize failure (one window) | retry A/I/K/T idempotently under the same phase-local kf lock; a converged retry joins the common commit barrier, never clears early; retry failure → marker retained, request EINPROGRESS |
| Commit flush failure | converged markers NOT cleared (retained) → replay at next open; request EINPROGRESS |
| Follow-up writer after a failed request | writer gates were released → the next writer's gate + gate replay handles retained markers before planning |

### 3b. Exact marker identity and publication primitives

Nonce-bearing names make `(shard,batch_id)` insufficient identity. Replace
the ID-only scan/reconstruction with an exact-path reference throughout
gate replay, startup recovery, tests, and clear:

```c
typedef struct {
    int      kf_shard;
    uint32_t batch_id;
    uint64_t nonce;                 /* zero only for the legacy filename */
    char     path[PATH_MAX];         /* exact path returned by readdir */
} MarkerRef;

static int marker_ref_cmp(const void *ap, const void *bp) {
    const MarkerRef *a = ap, *b = bp;
    if (a->batch_id != b->batch_id)
        return (a->batch_id > b->batch_id) - (a->batch_id < b->batch_id);
    if (a->nonce != b->nonce)
        return (a->nonce > b->nonce) - (a->nonce < b->nonce);
    return strcmp(a->path, b->path);
}

static int marker_ref_from_name(const char *kf_dir, const char *name,
                                MarkerRef *out) {
    int used = 0;
    unsigned shard = 0, batch = 0;
    unsigned long long nonce = 0;
    int ok = sscanf(name, "%x_batch_%u_%16llx_marker.dat%n",
                    &shard, &batch, &nonce, &used) == 3 &&
             used == (int)strlen(name);
    if (!ok) {
        used = 0;
        ok = sscanf(name, "%x_batch_%u_marker.dat%n",
                    &shard, &batch, &used) == 2 &&
             used == (int)strlen(name);
        nonce = 0;
    }
    if (!ok || shard >= MAX_SPLITS) return 1;
    int n = snprintf(out->path, sizeof(out->path), "%s/%s", kf_dir, name);
    if (n < 0 || (size_t)n >= sizeof(out->path)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    out->kf_shard = (int)shard;
    out->batch_id = (uint32_t)batch;
    out->nonce = (uint64_t)nonce;
    return 0;
}

static int marker_tmp_name_valid(const char *name) {
    int used = 0;
    unsigned shard = 0, batch = 0, pid = 0;
    unsigned long long nonce = 0, tmpnonce = 0;
    if (sscanf(name, "%x_batch_%u_%16llx_marker.dat.tmp.%u.%llu%n",
               &shard, &batch, &nonce, &pid, &tmpnonce, &used) == 5 &&
        used == (int)strlen(name))
        return 1;
    used = 0;
    return sscanf(name, "%x_batch_%u_marker.dat.tmp.%u.%llu%n",
                  &shard, &batch, &pid, &tmpnonce, &used) == 4 &&
           used == (int)strlen(name);
}

static int marker_refs_scan(const char *kf_dir, int wanted_shard,
                            int cleanup_temps,
                            MarkerRef **out_refs, size_t *out_n) {
    DIR *d = NULL;
    MarkerRef *refs = NULL;
    size_t n = 0, cap = 0;
    int rc = -1;
    *out_refs = NULL;
    *out_n = 0;
    d = opendir(kf_dir);
    if (!d) return errno == ENOENT ? 0 : -1;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        MarkerRef ref;
        int prc = marker_ref_from_name(kf_dir, de->d_name, &ref);
        if (prc < 0) goto out;
        if (prc > 0) {
            if (marker_tmp_name_valid(de->d_name)) {
                if (!cleanup_temps) continue;
                char tmp_path[PATH_MAX];
                int tn = snprintf(tmp_path, sizeof(tmp_path), "%s/%s",
                                  kf_dir, de->d_name);
                if (tn < 0 || (size_t)tn >= sizeof(tmp_path)) {
                    errno = ENAMETOOLONG;
                    goto out;
                }
                if (unlink(tmp_path) != 0 && errno != ENOENT) goto out;
                continue;
            }
            if (strstr(de->d_name, "_marker.dat") != NULL) {
                errno = EILSEQ;
                goto out;
            }
            continue;
        }
        if (wanted_shard >= 0 && ref.kf_shard != wanted_shard) continue;
        if (n == cap) {
            size_t next = cap ? cap * 2 : 8;
            if (next < cap || next > SIZE_MAX / sizeof(*refs)) {
                errno = EOVERFLOW;
                goto out;
            }
            MarkerRef *grown = realloc(refs, next * sizeof(*refs));
            if (!grown) goto out;
            refs = grown;
            cap = next;
        }
        refs[n++] = ref;
    }
    if (n > 1) qsort(refs, n, sizeof(*refs), marker_ref_cmp);
    for (size_t i = 1; i < n; i++) {
        if (refs[i - 1].kf_shard == refs[i].kf_shard &&
            refs[i - 1].batch_id == refs[i].batch_id &&
            refs[i - 1].nonce == refs[i].nonce) {
            errno = EILSEQ;
            goto out;
        }
    }
    *out_refs = refs;
    *out_n = n;
    refs = NULL;
    rc = 0;
out:
    free(refs);
    closedir(d);
    return rc;
}

static int kfm2_unlink_by_path(const char *path) {
    if (unlink(path) == 0 || errno == ENOENT) return 0;
    return -1;
}

static int kfm2_clear_by_path_sync(const char *path) {
    char dir[PATH_MAX];
    int n = snprintf(dir, sizeof(dir), "%s", path);
    if (n < 0 || (size_t)n >= sizeof(dir)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    char *slash = strrchr(dir, '/');
    if (!slash) { errno = EINVAL; return -1; }
    *slash = '\0';
    if (kfm2_unlink_by_path(path) != 0) return -1;
    return fsync_dir(dir);
}

static int kfm2_read_batch_marker(const char *path,
                                  KfMarkerSlot **out_slots,
                                  size_t *out_count);

static int kf_batch_marker_gate_refs(int kf_shard, SlotcaskKfHandle *kh,
                                     const char *data_dir) {
    char kf_dir[PATH_MAX], eff_root[PATH_MAX], object[256];
    MarkerRef *refs = NULL;
    size_t nrefs = 0;
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);
    split_data_dir(data_dir, eff_root, sizeof(eff_root),
                   object, sizeof(object));
    if (marker_refs_scan(kf_dir, kf_shard, 1, &refs, &nrefs) != 0)
        return -1;
    int rc = 0;
    for (size_t i = 0; i < nrefs && rc == 0; i++) {
        KfMarkerSlot *slots = NULL;
        size_t count = 0;
        int mrc = kfm2_read_batch_marker(refs[i].path, &slots, &count);
        if (mrc == 1) continue;
        if (mrc != 0) { rc = -1; break; }
        for (size_t j = 0; j < count && rc == 0; j++)
            if (kf_marker_replay_entry_locked(eff_root, object, data_dir,
                                              kf_shard, kh,
                                              &slots[j]) != 0)
                rc = -1;
        free(slots);
        if (rc == 0 && kfm2_clear_by_path_sync(refs[i].path) != 0)
            rc = -1;
    }
    free(refs);
    return rc;
}
```

This replaces both ID-only `readdir` loops. It keeps the exact path,
filters by `wanted_shard` when nonnegative, and fails closed on duplicate
identity. Unrecognised marker-namespace files still fail closed;
recognised `.tmp.*` files are inert pre-M debris and the scan removes them
before returning refs (an unlink error fails the scan). Gate/startup replay calls
`kfm2_read_batch_marker(ref.path, ...)` and
`kfm2_clear_by_path_sync(ref.path)`—it never reconstructs a filename.
`kf_shard_marker_gate` becomes a one-line call to this refs-based gate.
Startup scans all refs with `wanted_shard = -1`, groups adjacent refs by
shard, takes that shard's kf wrlock, and executes the same replay/clear
body; it never reduces a reference back to a numeric batch ID.

The startup sweep's batch portion is exactly:

```c
MarkerRef *refs = NULL;
size_t nrefs = 0;
if (marker_refs_scan(kf_dir, -1, 1, &refs, &nrefs) != 0) return -1;
for (size_t i = 0; i < nrefs; ) {
    int shard = refs[i].kf_shard;
    size_t j = i + 1;
    while (j < nrefs && refs[j].kf_shard == shard) j++;
    if (out_replayed) *out_replayed += (int)(j - i);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, data_dir, shard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, 0, 1) != 0) { rc = -1; break; }
    for (size_t k = i; k < j && rc == 0; k++) {
        KfMarkerSlot *slots = NULL;
        size_t count = 0;
        int mrc = kfm2_read_batch_marker(refs[k].path, &slots, &count);
        if (mrc == 1) continue;
        if (mrc != 0) { rc = -1; break; }
        for (size_t x = 0; x < count && rc == 0; x++)
            if (kf_marker_replay_entry_locked(eff_root, object_name,
                                              data_dir, shard, &kh,
                                              &slots[x]) != 0)
                rc = -1;
        free(slots);
        if (rc == 0 && kfm2_clear_by_path_sync(refs[k].path) != 0)
            rc = -1;
    }
    kfcache_release(&kh);
    if (rc != 0) break;
    i = j;
}
free(refs);
```

Replace `object_has_pending_markers`'s legacy-name `sscanf` loop with:

```c
MarkerRef *refs = NULL;
size_t nrefs = 0;
int rc = marker_refs_scan(kf_dir, -1, 0, &refs, &nrefs);
free(refs);
return rc == 0 ? (nrefs != 0) : -1;
```

Thus graceful shutdown also recognizes nonce-bearing markers and cannot
incorrectly write clean-open evidence while a deferred intent remains.

Split `marker_publish_atomic` into these two contracts and use the exact
output path as the request window identity:

```c
/* 0 after file fsync + atomic no-replace publication; 1 means the final
   path exists but temp cleanup was incomplete; -1 means no final path was
   installed. link() prevents even a nonce collision from overwriting a
   retained marker. */
static int marker_publish_file_atomic(const char *kf_dir,
                                      const char *final_name,
                                      const void *bytes, size_t len,
                                      char out_path[PATH_MAX]) {
    char tmp_path[PATH_MAX], final_path[PATH_MAX];
    const uint8_t *p = bytes;
    size_t left = len;
    int fd = -1;
    if (marker_make_unique_paths(kf_dir, final_name, tmp_path,
                                 sizeof(tmp_path), final_path,
                                 sizeof(final_path)) != 0)
        return -1;
    fd = open(tmp_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return -1;
    while (left) {
        ssize_t nw = write(fd, p, left);
        if (nw < 0 && errno == EINTR) continue;
        if (nw <= 0) goto fail;
        p += (size_t)nw;
        left -= (size_t)nw;
    }
    if (fsync(fd) != 0) goto fail;
    if (close(fd) != 0) { fd = -1; goto fail; }
    fd = -1;
    int on = snprintf(out_path, PATH_MAX, "%s", final_path);
    if (on < 0 || on >= PATH_MAX) { errno = ENAMETOOLONG; goto fail; }
    if (link(tmp_path, final_path) != 0) goto fail;
    if (unlink(tmp_path) != 0) return 1;
    return 0;
fail:
    if (fd >= 0) close(fd);
    unlink(tmp_path);
    return -1;
}

/* Legacy tri-state: 0 durable, 1 published but dir durability unknown,
   -1 definitely pre-publish. */
static int marker_publish_and_sync_dir(const char *kf_dir,
                                       const char *final_name,
                                       const void *bytes, size_t len,
                                       char out_path[PATH_MAX]) {
    int rc = marker_publish_file_atomic(kf_dir, final_name, bytes, len,
                                        out_path);
    if (rc < 0) return -1;
    return fsync_dir(kf_dir) == 0 ? 0 : 1;
}
```

`final_name` is always a basename produced by
`%03x_batch_%u_%016llx_marker.dat`; `rw->marker_path` is only the output
buffer. A caller must never pass `rw->marker_path` as `final_name`. This
also replaces stale comments that say publication uses `rename`; the final
link is no-replace and therefore cannot destroy an older retained intent.

### 3c. Structures, per-shard gates, request state

**`SlotcaskDb` gains** (anchor: the `pthread_mutex_t trim_init_lock;` member
in `src/db/slotcask.h`):

```c
    pthread_mutex_t *writer_gates;     /* [num_shards] per-kf-shard writer
                                          admission; writers only; readers
                                          never take it                  */
    size_t           writer_gates_inited;
```

Add after `fsync_dir` in `slotcask.c`:

```c
static int writer_gates_init(SlotcaskDb *db) {
    db->writer_gates = calloc((size_t)db->num_shards,
                              sizeof(*db->writer_gates));
    if (!db->writer_gates) return -1;
    for (int s = 0; s < db->num_shards; s++) {
        int rc = pthread_mutex_init(&db->writer_gates[s], NULL);
        if (rc != 0) {
            errno = rc;
            while (db->writer_gates_inited > 0) {
                db->writer_gates_inited--;
                pthread_mutex_destroy(
                    &db->writer_gates[db->writer_gates_inited]);
            }
            free(db->writer_gates);
            db->writer_gates = NULL;
            return -1;
        }
        db->writer_gates_inited++;
    }
    return 0;
}

static void writer_gates_destroy(SlotcaskDb *db) {
    while (db->writer_gates_inited > 0) {
        db->writer_gates_inited--;
        pthread_mutex_destroy(&db->writer_gates[db->writer_gates_inited]);
    }
    free(db->writer_gates);
    db->writer_gates = NULL;
}

static void writer_gate_lock(SlotcaskDb *db, int kf_shard) {
    pthread_mutex_lock(&db->writer_gates[kf_shard]);
}
static void writer_gate_unlock(SlotcaskDb *db, int kf_shard) {
    pthread_mutex_unlock(&db->writer_gates[kf_shard]);
}
```

In `slotcask_open`, immediately after the successful `trim_init_lock`
initialisation, insert:

```c
    if (writer_gates_init(db) != 0) {
        pthread_mutex_destroy(&db->trim_init_lock);
        return -1;
    }
```

Insert `writer_gates_destroy(db);` immediately before
`pthread_mutex_destroy(&db->trim_init_lock);` in both the `fail:` cleanup
and `slotcask_close`. `writer_gates_inited` makes every partial-open path
safe; no uninitialised gate is destroyed.

**Hook API and public input** go in `src/db/slotcask.h`; **ReqWindow,
ReqShard, and SlotcaskBulkRequest** go in `src/db/slotcask.c` after the
existing bulk-plan types:

```c
/* Replace both upsert and delete window-hook typedef groups with these
   state-bearing signatures. prepare owns *out_window_state on success;
   exactly one terminal callback consumes it. */
typedef int (*slotcask_bulk_prepare_window_fn)(
    SlotcaskBulkRec *recs, const size_t *active, size_t nactive,
    void *ctx, void **out_window_state);
typedef int (*slotcask_bulk_apply_window_fn)(
    SlotcaskBulkRec *recs, const size_t *active, size_t nactive,
    void *ctx, void *window_state);
typedef void (*slotcask_bulk_terminal_window_fn)(void *ctx,
                                                  void *window_state);

/* SlotcaskBulkOpts and SlotcaskBulkDeleteOpts both use these five fields.
   Delete no longer has a structurally different terminal-hook contract. */
slotcask_bulk_prepare_window_fn  prepare_window;
slotcask_bulk_apply_window_fn    apply_window;
slotcask_bulk_terminal_window_fn commit_done;
slotcask_bulk_terminal_window_fn release_window;
slotcask_bulk_terminal_window_fn abort_window;

/* One deferred bulk request spanning an entire cmd_bulk_* call. The
 * coordinator holds the writer gate of every touched shard for the whole
 * request (ascending acquire, reverse release); ordinary writers to those
 * shards block on the gate; readers never touch it. Per-shard transaction
 * state persists across the coordinator's phase joins. */
struct ReqWindow {
    BulkWindowPlan  plan;              /* owned; moved from the stack      */
    char            marker_path[PATH_MAX];   /* exact path, preserved     */
    SegLoc         *a_locs; size_t na, cap_a; /* activation bytes (defer) */
    SegLoc         *t_locs; size_t nt, cap_t; /* tombstone bytes (defer)  */
    size_t         *kf_slots; size_t nkf, cap_kf;
    int             kf_header_changed;
    void           *hook_state;        /* opaque per-window hook state     */
    int             published;         /* marker file exists               */
    int             converged;         /* A/I/K/T completed                */
    int             unlink_succeeded;  /* unlink/ENOENT completed          */
    int             cleared;           /* unlink made durable by dir fsync */
    int             hooks_staged;
    struct {
        slotcask_bulk_terminal_window_fn commit_done;
        slotcask_bulk_terminal_window_fn release_window;
        slotcask_bulk_terminal_window_fn abort_window;
        void *ctx;
    } hooks;
};

typedef struct {
    int        used;
    int        kf_shard_id;
    int        stage_failed;
    /* Request-owned options (by value — worker-stack opts would dangle). */
    BulkMutationKind        kind;
    union {
        SlotcaskBulkOpts       upsert;
        SlotcaskBulkDeleteOpts delete_;
    } opts;
    BulkMutationShard shard;                  /* persisted executor state */
    BulkMutationTxn  txn;                     /* persisted across waves    */
    int              has_txn;
    SlotcaskBulkRec *recs; size_t nrecs;      /* caller-owned batch slice  */
    SegLoc    *p_locs; size_t np, cap_p;      /* payload bytes (epoch 1)   */
    ReqWindow *windows; size_t nwindows, cap_windows;
    int        failed;                 /* EINPROGRESS / unreplayed         */
} ReqShard;

typedef struct SlotcaskBulkRequest SlotcaskBulkRequest;
struct SlotcaskBulkRequest {
    SlotcaskDb *db;
    ReqShard   *shards;                /* [num_shards], indexed by kf id   */
    int         num_shards;
    char        kf_dir[PATH_MAX];
    uint64_t    nonce;                 /* per-request marker uniqueness    */
    int        *touched;               /* [ntouched] ascending, deduped    */
    size_t      ntouched;
    int         any_published;
    int         payload_rc, publish_rc, marker_dir_rc, finalize_rc, commit_rc;
    int         any_failed;            /* any retained/unreplayed window   */
};

typedef struct {
    int               kf_shard_id;
    SlotcaskBulkRec  *recs;
    size_t            nrecs;
    enum { SLOTCASK_BULK_INPUT_UPSERT = 1,
           SLOTCASK_BULK_INPUT_DELETE = 2 } kind;
    union {
        SlotcaskBulkOpts       upsert;
        SlotcaskBulkDeleteOpts delete_;
    } opts;                         /* copied by value by query_bulk */
    int rc;                         /* per-shard aggregate result */
} SlotcaskBulkShardInput;

/* The only new public operation. It is synchronous: input records, option
   pointer targets, hook contexts, and arenas remain owned by the caller and
   valid until it returns. Request/phase state stays private to slotcask.c. */
int slotcask_bulk_request_execute(SlotcaskDb *db,
                                  SlotcaskBulkShardInput *inputs,
                                  size_t ninputs);
```

Place `SlotcaskBulkShardInput` and the execute declaration after the full
`SlotcaskBulkDeleteOpts` definition, so both union members are complete.
The hook typedefs precede both option structs. Do not expose any request
state or phase helper in the header.

Add `struct SlotcaskBulkRequest *req;` to the complete `BulkMutationTxn`
definition in `slotcask.c`. Keep `ReqWindow`, `ReqShard`, and the request
definition and all `begin/stage/publish/finalize/flush/end` helpers private
and `static` in `slotcask.c`; `slotcask.h` exposes only
`SlotcaskBulkShardInput` and `slotcask_bulk_request_execute`. This is the
coordinator module's seam: callers provide owned shard inputs and receive
statuses without learning its phase state.

(`BulkMutationKind` is the existing enum with `BULK_MUTATION_UPSERT` /
`BULK_MUTATION_DELETE`; the tag selects which union member is valid. The
options are stored BY VALUE: every pointer they contain is a
function pointer (static lifetime), the worker hook context (the
per-worker scratch struct, alive until the cmd_bulk site's cleanup — and
`slotcask_bulk_request_end` runs BEFORE that cleanup at every call site),
or a worker-level out-parameter. Documented lifetime: request end.)

**Gate helpers** (`src/db/slotcask.c`):

```c
static void writer_gate_lock(SlotcaskDb *db, int kf_shard) {
    pthread_mutex_lock(&db->writer_gates[kf_shard]);
}
static void writer_gate_unlock(SlotcaskDb *db, int kf_shard) {
    pthread_mutex_unlock(&db->writer_gates[kf_shard]);
}
```

At the later allocation anchor `db->streams = calloc(...)`, replace the
existing direct-return check:

```c
    if (!db->streams) return -1;
```

with:

```c
    if (!db->streams) goto fail;
```

This is required because the writer gates now exist by that point. Every
failure after `writer_gates_init` must pass through `fail:`; no later
`return -1` may bypass `writer_gates_destroy`.

**begin/end** (complete):

```c
static SlotcaskBulkRequest *slotcask_bulk_request_begin(
        SlotcaskDb *db, const int *touched_shards, size_t ntouched) {
    if (!db || !touched_shards || ntouched == 0) return NULL;
    SlotcaskBulkRequest *req = calloc(1, sizeof(*req));
    if (!req) return NULL;
    req->db = db;
    req->num_shards = db->num_shards;
    req->nonce = now_us() ^
                 (__atomic_add_fetch(&g_marker_nonce_seq, 1,
                                     __ATOMIC_RELAXED) << 32);
    req->shards = calloc((size_t)db->num_shards, sizeof(*req->shards));
    if (!req->shards) { free(req); return NULL; }
    if (ntouched > SIZE_MAX / sizeof(*req->touched)) {
        free(req->shards); free(req); errno = EOVERFLOW; return NULL;
    }
    req->touched = malloc(ntouched * sizeof(*req->touched));
    if (!req->touched) { free(req->shards); free(req); return NULL; }
    /* Ascending touched-shard copy (dedup: the caller's map is already
       per-shard, but be defensive). Gate acquisition is ascending; release
       is reverse — no deadlock (each ordinary writer holds ONE gate). */
    for (size_t i = 0; i < ntouched; i++) {
        int s = touched_shards[i];
        if (s < 0 || s >= db->num_shards) {
            free(req->touched); free(req->shards); free(req);
            errno = EINVAL;
            return NULL;
        }
        int dup = 0;
        for (size_t k = 0; k < req->ntouched; k++)
            if (req->touched[k] == s) { dup = 1; break; }
        if (dup) continue;
        /* Insertion into the sorted prefix (standard insertion step). */
        size_t pos = req->ntouched++;
        while (pos > 0 && req->touched[pos - 1] > s) {
            req->touched[pos] = req->touched[pos - 1];
            pos--;
        }
        req->touched[pos] = s;
    }

    int kn = snprintf(req->kf_dir, sizeof(req->kf_dir),
                      "%s/data/kf", db->data_dir);
    if (kn < 0 || (size_t)kn >= sizeof(req->kf_dir)) {
        free(req->touched); free(req->shards); free(req);
        errno = ENAMETOOLONG;
        return NULL;
    }
    /* Acquire touched writer gates in ascending order. Ordinary writers
       take exactly one gate, so no acquisition cycle is possible. */
    for (size_t k = 0; k < req->ntouched; k++)
        writer_gate_lock(db, req->touched[k]);
    return req;
}

static void slotcask_bulk_request_end(SlotcaskBulkRequest *req) {
    if (!req) return;
    SlotcaskDb *db = req->db;
    for (int s = 0; s < req->num_shards; s++) {
        ReqShard *rs = &req->shards[s];
        if (rs->has_txn && rs->shard.st) {
            for (size_t i = 0; i < rs->shard.nrecs; i++) {
                free(rs->shard.st[i].old_buf);
                rs->shard.st[i].old_buf = NULL;
            }
            bulk_mutation_txn_free_state(&rs->txn);
        }
        for (size_t i = 0; i < rs->nwindows; i++) {
            ReqWindow *rw = &rs->windows[i];
            /* Retained (EINPROGRESS) windows keep their marker for
               gate/startup replay; their opaque hook state is released
               through the path's release hook. */
            if (rw->hooks_staged) {
                if (rw->hooks.release_window)
                    rw->hooks.release_window(rw->hooks.ctx,
                                             rw->hook_state);
                rw->hooks_staged = 0;
                rw->hook_state = NULL;
            }
            bulk_window_plan_destroy(&rw->plan);
            free(rw->a_locs); free(rw->t_locs); free(rw->kf_slots);
        }
        free(rs->windows);
        free(rs->p_locs);
    }
    /* Same coordinator thread that acquired the gates releases them after
       terminal hook and request-state cleanup has finished. */
    for (size_t k = req->ntouched; k > 0; k--)
        writer_gate_unlock(db, req->touched[k - 1]);
    free(req->shards);
    free(req->touched);
    free(req);
}
```

(`rs->kind` selects `rs->opts.upsert` or `rs->opts.delete_`; both are
request-owned by-value copies. `req->touched` is allocated before the
sorted insertion loop.)

### 3d'. M-phase fallback flag (D5 prerequisite, review round 4 P1-2)

`bulk_plan_window_locked`'s OLD-derived branch stages NEW bytes
synchronously (real code, `slotcask.c`, anchor:
`/* OLD-derived records never went through the P wave: stage NEW
synchronously here so M still covers a durable payload. */`) but sets
neither `st->needs_write` nor `st->staged_in_wave`. The plan's D5
pre-marker sync (`bulk_sync_fallback_payloads`) filters on
`needs_write && !staged_in_wave`, so without a flag change it collects
**nothing** for these records and their bytes go unsynced under a durable
marker — I2 violated for every `auto_create` object and OLD-derived
update. Fix — after the successful `bulk_stage_single_pending` call
(anchor: `r->slot_capacity = cap;`), add:

```c
                s->needs_write = 1;   /* staged bytes owe a D5 sync */
```

With this flag, `bulk_sync_fallback_payloads`'s filter
(`needs_write && !staged_in_wave`) collects exactly the fallback-staged
records: P-wave records carry `staged_in_wave = 1` and are already
covered by the epoch-1 flush; legacy-mode durability is unaffected (the
legacy A pass syncs from plan-entry locations regardless of
`needs_write`). The flag is not consulted after the finalize join.

### 3d. Tagged-union options, per-shard persistence, phase entry points

**Source order:** keep legacy `bulk_commit_one_kf_window` and add the
deferred publish/finalize leaf functions immediately after it (all
primitives they call are already above it). Keep `BulkStageWork` and
`bulk_stage_one_shard` in their current order. Insert request begin/end,
phase entry points, phase-worker structs, flush helpers, and the public coordinator immediately after
`bulk_mutation_txn_free_state`, with static prototypes at the start of that
block. This guarantees `BulkStageWork`, the leaf stage worker, comparators,
and transaction cleanup are declared before use; do not paste the snippets
in prose order if that would create implicit declarations.

`ReqShard` gains `BulkMutationKind kind;` plus the `opts` union shown
above. The stage entry point copies exactly one member:

```c
    rs->kind = ups ? BULK_MUTATION_UPSERT : BULK_MUTATION_DELETE;
    if (ups)  rs->opts.upsert  = *ups;
    if (dels) rs->opts.delete_ = *dels;
```

so no pointer into worker-stack options survives the stage call. Every
later phase reads `&rs->opts.upsert` / `&rs->opts.delete_` (request-owned
storage, valid through `request_end`).

**Phase entry points** (complete):

```c
static int request_owns_shard(const SlotcaskBulkRequest *req, int shard) {
    size_t lo = 0, hi = req->ntouched;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int cur = req->touched[mid];
        if (cur == shard) return 1;
        if (cur < shard) lo = mid + 1;
        else hi = mid;
    }
    return 0;
}

/* Wave 1 per shard: gate replay + stage; P sync deferred into p_locs. */
static int slotcask_bulk_stage_shard(SlotcaskBulkRequest *req,
                                     int kf_shard_id,
                                     SlotcaskBulkRec *recs, size_t n,
                                     const SlotcaskBulkOpts *ups,
                                     const SlotcaskBulkDeleteOpts *dels) {
    if (!req || kf_shard_id < 0 || kf_shard_id >= req->num_shards ||
        !request_owns_shard(req, kf_shard_id) || !recs || n == 0 ||
        (!!ups == !!dels)) {
        errno = EINVAL;
        return -1;
    }
    ReqShard *rs = &req->shards[kf_shard_id];
    if (rs->used) { errno = EALREADY; return -1; }
    rs->used = 1;
    rs->kf_shard_id = kf_shard_id;
    rs->recs = recs;
    rs->nrecs = n;
    rs->kind = ups ? BULK_MUTATION_UPSERT : BULK_MUTATION_DELETE;
    if (ups)  rs->opts.upsert  = *ups;
    if (dels) rs->opts.delete_ = *dels;

    memset(&rs->shard, 0, sizeof(rs->shard));
    rs->shard.kf_shard_id = kf_shard_id;
    rs->shard.recs = recs;
    rs->shard.nrecs = n;
    rs->shard.kind = rs->kind;
    if (ups) rs->shard.upsert_opts = &rs->opts.upsert;
    else rs->shard.delete_opts = &rs->opts.delete_;

    /* Phase-local kf handle: acquired and released within this task (a
       POSIX rwlock must not cross threads). ReqShard never stores it. */
    SlotcaskKfHandle kh;
    if (kf_shard_acquire(&kh, req->db, kf_shard_id, 1) != 0) {
        rs->stage_failed = 1;
        for (size_t i = 0; i < n; i++) recs[i].status = -1;
        return -1;
    }

    /* Gate retained markers once, before staging: replay converges any
       prior request's retained state; v1/corrupt markers fail closed. */
    if (kf_batch_marker_gate_refs(kf_shard_id, &kh,
                                  req->db->data_dir) != 0) {
        /* Fail closed, preserving the gate's errno: EILSEQ for a corrupt
           or v1 marker (not retryable), I/O errors for transient faults.
           Overwriting with EINPROGRESS would misreport corruption as
           retryable. */
        kfcache_release(&kh);
        rs->stage_failed = 1;
        for (size_t i = 0; i < n; i++) recs[i].status = -1;
        return -1;
    }
    kfcache_release(&kh);

    /* Per-shard transaction assembly — the verified preludes of
       slotcask_bulk_upsert_in_kfshard / slotcask_bulk_delete_in_kfshard,
       with the transaction PERSISTED in the ReqShard so the coordinator's
       phase waves operate on it across joins. */
    memset(&rs->txn, 0, sizeof(rs->txn));
    rs->txn.db = req->db;
    rs->txn.shards = &rs->shard;
    rs->txn.nshards = 1;
    rs->txn.window_cap = req->db->bulk_commit_window > 0
                       ? (size_t)req->db->bulk_commit_window
                       : (size_t)4096;
    if (rs->kind == BULK_MUTATION_UPSERT) {
        rs->txn.upsert_opts = &rs->opts.upsert;
    } else {
        rs->txn.delete_opts = &rs->opts.delete_;
    }
    rs->txn.req = req;
    atomic_init(&rs->txn.cancelled, 0);
    rs->has_txn = 1;

    /* This function already runs in the coordinator's parallel stage wave.
       Do not call bulk_stage_payload_wave (that would nest the executor).
       Allocate the one shard's state and invoke its leaf worker inline. */
    rs->shard.st = calloc(n, sizeof(*rs->shard.st));
    if (!rs->shard.st) {
        rs->stage_failed = 1;
        for (size_t i = 0; i < n; i++) recs[i].status = -1;
        return -1;
    }
    BulkStageWork work = { .txn = &rs->txn, .shard_idx = 0 };
    bulk_stage_one_shard(&work);
    int rc = atomic_load_explicit(&rs->txn.cancelled,
                                  memory_order_acquire) || rs->shard.rc != 0
           ? -1 : 0;
    if (rc != 0) {
        rs->stage_failed = 1;
        for (size_t i = 0; i < n; i++)
            if (recs[i].status == 0) recs[i].status = -1;
    }
    return rc;
}
```

The by-value options contain function pointers, scalar flags,
`bulk_hook_ctx`, and `out_durability_degraded`. Task 3d moves every batch,
scratch slab, arena, hook context, and durability result into its
call-site work object and frees them only after the synchronous request
interface returns. Therefore every copied pointer has an explicit
request-long lifetime. The 4096 fallback is the existing value used by
both legacy bulk entry points.

In `bulk_stage_one_shard`, retain the existing location construction but
delete its in-loop `st[i].staged_in_wave = 1`; replace the P sync tail with
this branch (forward-declare
`segloc_vec_append` above the stage worker):

```c
    qsort(locs, n, sizeof(*locs), segloc_cmp);
    if (txn->req) {
        ReqShard *rs = &txn->req->shards[shard->kf_shard_id];
        if (segloc_vec_append(&rs->p_locs, &rs->np, &rs->cap_p,
                              locs, n) != 0) {
            free(locs);
            atomic_store_explicit(&txn->cancelled, 1,
                                  memory_order_release);
            shard->rc = -1;
            return NULL;
        }
        for (size_t i = 0; i < shard->nrecs; i++)
            if (recs[i].status == 0 && st[i].needs_write)
                st[i].staged_in_wave = 1;
    } else {
        if (bulk_seg_apply_and_sync(txn->db, locs, n, 0, 0) != 0) {
            free(locs);
            atomic_store_explicit(&txn->cancelled, 1,
                                  memory_order_release);
            shard->rc = -1;
            return NULL;
        }
        for (size_t i = 0; i < shard->nrecs; i++)
            if (recs[i].status == 0 && st[i].needs_write)
                st[i].staged_in_wave = 1;
        if (SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_P)) {
            free(locs);
            atomic_store_explicit(&txn->cancelled, 1,
                                  memory_order_release);
            shard->rc = -1;
            return NULL;
        }
    }
    free(locs);
```

The deferred branch performs no durability syscall and never fires the P
sync injector; the request payload barrier owns both. A vector-allocation
failure marks the shard failed before any marker can publish.

**Publish / finalize entry points** (complete):

```c
static int bulk_publish_one_kf_window(BulkMutationTxn *txn,
                                      BulkMutationShard *shard,
                                      SlotcaskKfHandle *kh,
                                      size_t begin, size_t end,
                                      ReqWindow *rw);
static int bulk_finalize_one_kf_window(BulkMutationTxn *txn,
                                       BulkMutationShard *shard,
                                       SlotcaskKfHandle *kh,
                                       ReqWindow *rw);

static void req_window_capture_hooks(ReqWindow *rw,
                                     const BulkMutationShard *shard) {
    if (shard->kind == BULK_MUTATION_UPSERT && shard->upsert_opts) {
        rw->hooks.commit_done = shard->upsert_opts->commit_done;
        rw->hooks.release_window = shard->upsert_opts->release_window;
        rw->hooks.abort_window = shard->upsert_opts->abort_window;
        rw->hooks.ctx = shard->upsert_opts->bulk_hook_ctx;
    } else if (shard->delete_opts) {
        rw->hooks.commit_done = shard->delete_opts->commit_done;
        rw->hooks.release_window = shard->delete_opts->release_window;
        rw->hooks.abort_window = shard->delete_opts->abort_window;
        rw->hooks.ctx = shard->delete_opts->bulk_hook_ctx;
    }
}

static ReqWindow *req_window_append(ReqShard *rs) {
    if (rs->nwindows == rs->cap_windows) {
        size_t cap = rs->cap_windows ? rs->cap_windows * 2 : 4;
        if (cap < rs->cap_windows || cap > SIZE_MAX / sizeof(*rs->windows)) {
            errno = EOVERFLOW;
            return NULL;
        }
        ReqWindow *v = realloc(rs->windows, cap * sizeof(*v));
        if (!v) return NULL;
        rs->windows = v;
        rs->cap_windows = cap;
    }
    ReqWindow *rw = &rs->windows[rs->nwindows++];
    memset(rw, 0, sizeof(*rw));
    return rw;
}

static void bulk_window_plan_move(BulkWindowPlan *dst,
                                  BulkWindowPlan *src) {
    *dst = *src;
    memset(src, 0, sizeof(*src));
}

static int slotcask_bulk_publish_shard(SlotcaskBulkRequest *req,
                                       int kf_shard_id) {
    if (!req || kf_shard_id < 0 || kf_shard_id >= req->num_shards ||
        !request_owns_shard(req, kf_shard_id)) {
        errno = EINVAL;
        return -1;
    }
    ReqShard *rs = &req->shards[kf_shard_id];
    if (!rs->has_txn || rs->stage_failed || req->payload_rc != 0) return 0;
    SlotcaskKfHandle kh;
    if (kf_shard_acquire(&kh, req->db, kf_shard_id, 1) != 0) return -1;
    int rc = 0;
    while (rs->shard.cursor < rs->shard.nrecs) {
        size_t begin = rs->shard.cursor;
        size_t end = begin + rs->txn.window_cap;
        if (end > rs->shard.nrecs) end = rs->shard.nrecs;
        ReqWindow *rw = req_window_append(rs);
        if (!rw) { rc = -1; break; }
        if (bulk_publish_one_kf_window(&rs->txn, &rs->shard, &kh,
                                       begin, end, rw) != 0)
            rc = -1; /* this window aborted pre-M; later windows may proceed */
        rs->shard.cursor = end;
    }
    kfcache_release(&kh);
    return rc;
}

static int slotcask_bulk_finalize_shard(SlotcaskBulkRequest *req,
                                        int kf_shard_id) {
    if (!req || kf_shard_id < 0 || kf_shard_id >= req->num_shards ||
        !request_owns_shard(req, kf_shard_id)) {
        errno = EINVAL;
        return -1;
    }
    ReqShard *rs = &req->shards[kf_shard_id];
    if (!rs->has_txn || req->marker_dir_rc != 0) return 0;
    SlotcaskKfHandle kh;
    if (kf_shard_acquire(&kh, req->db, kf_shard_id, 1) != 0) return -1;
    int rc = 0;
    for (size_t i = 0; i < rs->nwindows; i++) {
        ReqWindow *rw = &rs->windows[i];
        if (!rw->published) continue;
        if (bulk_finalize_one_kf_window(&rs->txn, &rs->shard,
                                        &kh, rw) != 0) {
            /* Forward retry remains deferred. It may make mutations
               idempotently, but it never clears before the common barrier. */
            if (bulk_finalize_one_kf_window(&rs->txn, &rs->shard,
                                            &kh, rw) != 0) {
                rs->failed = 1;
                rc = -1;
            }
        }
    }
    kfcache_release(&kh);
    return rc;
}
```

**Coordinator and joins** (complete; insert after the phase entry points):

```c
static int slotcask_bulk_request_flush_payloads(SlotcaskBulkRequest *req);
static int slotcask_bulk_request_flush_marker_dir(SlotcaskBulkRequest *req);
static int slotcask_bulk_request_flush_commit(SlotcaskBulkRequest *req);

typedef struct {
    SlotcaskBulkRequest   *req;
    SlotcaskBulkShardInput *in;
    int phase;
    int rc;
    int err;
} ReqPhaseArg;

enum { REQ_STAGE = 1, REQ_PUBLISH = 2, REQ_FINALIZE = 3 };

static void *req_phase_worker(void *raw) {
    ReqPhaseArg *a = raw;
    SlotcaskBulkShardInput *in = a->in;
    if (a->phase == REQ_STAGE) {
        const SlotcaskBulkOpts *up =
            in->kind == SLOTCASK_BULK_INPUT_UPSERT ? &in->opts.upsert : NULL;
        const SlotcaskBulkDeleteOpts *del =
            in->kind == SLOTCASK_BULK_INPUT_DELETE ? &in->opts.delete_ : NULL;
        a->rc = slotcask_bulk_stage_shard(a->req, in->kf_shard_id,
                                           in->recs, in->nrecs, up, del);
    } else if (a->phase == REQ_PUBLISH) {
        a->rc = slotcask_bulk_publish_shard(a->req, in->kf_shard_id);
    } else {
        a->rc = slotcask_bulk_finalize_shard(a->req, in->kf_shard_id);
    }
    a->err = a->rc == 0 ? 0 : errno;
    return NULL;
}

static int req_run_phase(SlotcaskBulkRequest *req,
                         SlotcaskBulkShardInput *inputs, size_t ninputs,
                         int phase) {
    ReqPhaseArg *args = calloc(ninputs, sizeof(*args));
    if (!args) return -1;
    for (size_t i = 0; i < ninputs; i++) {
        args[i].req = req;
        args[i].in = &inputs[i];
        args[i].phase = phase;
    }
    parallel_for_io(req_phase_worker, args, (int)ninputs, sizeof(*args));
    int rc = 0, saved = 0;
    for (size_t i = 0; i < ninputs; i++) {
        if (args[i].rc != 0) {
            inputs[i].rc = -1;
            rc = -1;
            if (!saved) saved = args[i].err;
        }
    }
    free(args);
    if (rc != 0) errno = saved ? saved : EIO;
    return rc;
}

static void req_mark_durability_degraded(SlotcaskBulkRequest *req) {
    for (size_t i = 0; i < req->ntouched; i++) {
        ReqShard *rs = &req->shards[req->touched[i]];
        int retained = 0;
        for (size_t w = 0; w < rs->nwindows; w++)
            retained |= rs->windows[w].published && !rs->windows[w].cleared;
        if (!retained) continue;
        int *out = rs->kind == BULK_MUTATION_UPSERT
                 ? rs->opts.upsert.out_durability_degraded
                 : rs->opts.delete_.out_durability_degraded;
        if (out) *out = 1;
    }
}

int slotcask_bulk_request_execute(SlotcaskDb *db,
                                  SlotcaskBulkShardInput *inputs,
                                  size_t ninputs) {
    if (!db || !inputs || ninputs == 0 || ninputs > (size_t)INT_MAX) {
        errno = EINVAL;
        return -1;
    }
    int *touched = malloc(ninputs * sizeof(*touched));
    if (!touched) return -1;
    for (size_t i = 0; i < ninputs; i++) {
        if (inputs[i].kf_shard_id < 0 ||
            inputs[i].kf_shard_id >= db->num_shards ||
            !inputs[i].recs || inputs[i].nrecs == 0 ||
            (inputs[i].kind != SLOTCASK_BULK_INPUT_UPSERT &&
             inputs[i].kind != SLOTCASK_BULK_INPUT_DELETE)) {
            free(touched);
            errno = EINVAL;
            return -1;
        }
        inputs[i].rc = 0;
        int *degraded = inputs[i].kind == SLOTCASK_BULK_INPUT_UPSERT
                      ? inputs[i].opts.upsert.out_durability_degraded
                      : inputs[i].opts.delete_.out_durability_degraded;
        if (degraded) *degraded = 0;
        touched[i] = inputs[i].kf_shard_id;
        for (size_t j = 0; j < i; j++) {
            if (inputs[j].kf_shard_id == inputs[i].kf_shard_id) {
                free(touched);
                errno = EINVAL;
                return -1;
            }
        }
    }
    SlotcaskBulkRequest *req =
        slotcask_bulk_request_begin(db, touched, ninputs);
    free(touched);
    if (!req) return -1;

    int stage_rc = req_run_phase(req, inputs, ninputs, REQ_STAGE);
    req->payload_rc = slotcask_bulk_request_flush_payloads(req);
    if (req->payload_rc != 0) {
        for (size_t si = 0; si < ninputs; si++)
            for (size_t ri = 0; ri < inputs[si].nrecs; ri++)
                if (inputs[si].recs[ri].status == 0)
                    inputs[si].recs[ri].status = -1;
    }
    /* Every phase is joined even after failure. The phase helpers enforce
       the shard/window predicates and become no-ops when ineligible. */
    req->publish_rc = req_run_phase(req, inputs, ninputs, REQ_PUBLISH);
    req->any_published = 0;           /* coordinator-only post-join merge */
    for (size_t si = 0; si < req->ntouched; si++) {
        ReqShard *rs = &req->shards[req->touched[si]];
        for (size_t wi = 0; wi < rs->nwindows; wi++)
            req->any_published |= rs->windows[wi].published;
    }
    req->marker_dir_rc = slotcask_bulk_request_flush_marker_dir(req);
    if (req->any_published) {
        SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_REQ_PUBLISHED);
        durability_test_pause(req->db->data_dir, "req-published");
    }
    req->finalize_rc = req_run_phase(req, inputs, ninputs, REQ_FINALIZE);
    req->any_failed = 0;              /* coordinator-only post-join merge */
    for (size_t si = 0; si < req->ntouched; si++)
        req->any_failed |= req->shards[req->touched[si]].failed;
    req->commit_rc = slotcask_bulk_request_flush_commit(req);

    int pending = req->any_failed ||
                  (req->any_published &&
                   (req->marker_dir_rc != 0 || req->finalize_rc != 0 ||
                    req->commit_rc != 0));
    int failed = stage_rc != 0 || req->payload_rc != 0 ||
                 req->publish_rc != 0 || req->marker_dir_rc != 0 ||
                 req->finalize_rc != 0 || req->commit_rc != 0;
    int saved = pending ? EINPROGRESS : (failed ? (errno ? errno : EIO) : 0);
    if (pending) {
        req_mark_durability_degraded(req);
        for (size_t i = 0; i < ninputs; i++) {
            ReqShard *rs = &req->shards[inputs[i].kf_shard_id];
            for (size_t w = 0; w < rs->nwindows; w++)
                if (rs->windows[w].published && !rs->windows[w].cleared) {
                    inputs[i].rc = -2;
                    break;
                }
        }
    }
    slotcask_bulk_request_end(req);   /* releases gates on this same thread */
    if (failed) { errno = saved; return -1; }
    return 0;
}
```

Duplicate input shard IDs are rejected before `begin` (one persistent
`ReqShard` is the single writer for a shard); query callers merge all work
for a shard into one input. `parallel_for_io` is a join, including inline
execution, and no worker waits for another worker.

**Deferred window leaves.** Do not delete or route legacy/single callers
away from `bulk_commit_one_kf_window`; it retains its per-window immediate
durability behavior. Add the following request-only leaves beside it:

```c
/* Publish pass, per window: plan (P/lookup/reserve/value_compute) →
   D5 sync → M via marker_publish_file_atomic. Marker retained; the
   window's hook_state (from prepare_window) is stored in its ReqWindow. */
static int bulk_publish_one_kf_window(BulkMutationTxn *txn,
                                      BulkMutationShard *shard,
                                      SlotcaskKfHandle *kh,
                                      size_t begin, size_t end,
                                      ReqWindow *rw);
/* Finalize pass, per published window: A → I(apply) → K → T with
   every sync deferred into request-owned collections. Failure is retried
   idempotently but NEVER clears here; a converged retry joins the common
   request durability barrier. */
static int bulk_finalize_one_kf_window(BulkMutationTxn *txn,
                                       BulkMutationShard *shard,
                                       SlotcaskKfHandle *kh,
                                       ReqWindow *rw);

static int bulk_build_window_marker_v2(const BulkWindowPlan *plan,
                                       uint8_t **out_buf,
                                       size_t *out_len);

static int bulk_sync_fallback_payloads(BulkMutationTxn *txn,
                                       BulkMutationShard *shard,
                                       size_t begin, size_t end) {
    SlotcaskBulkRec *recs = shard->recs;
    SegLoc *locs = calloc(end - begin, sizeof(*locs));
    size_t n = 0;
    if (!locs) return -1;
    for (size_t i = begin; i < end; i++) {
        SlotcaskBulkState *st = &shard->st[i];
        if (recs[i].status != 0 || st->staged_in_wave || !st->needs_write)
            continue;
        locs[n++] = (SegLoc){ .sid = st->target_stream,
                             .fid = st->target_fid,
                             .off = st->target_off };
    }
    qsort(locs, n, sizeof(*locs), segloc_cmp);
    int rc = n ? bulk_seg_apply_and_sync(txn->db, locs, n, 0, 0) : 0;
    if (rc == 0)
        for (size_t i = begin; i < end; i++) {
            SlotcaskBulkState *st = &shard->st[i];
            if (recs[i].status == 0 && !st->staged_in_wave && st->needs_write)
                st->staged_in_wave = 1;
        }
    free(locs);
    return rc;
}

static void req_window_abort_pre_marker(ReqWindow *rw) {
    if (rw->hooks_staged && rw->hooks.abort_window)
        rw->hooks.abort_window(rw->hooks.ctx, rw->hook_state);
    rw->hooks_staged = 0;
    rw->hook_state = NULL;
}

static int bulk_publish_one_kf_window(BulkMutationTxn *txn,
                                      BulkMutationShard *shard,
                                      SlotcaskKfHandle *kh,
                                      size_t begin, size_t end,
                                      ReqWindow *rw) {
    BulkWindowPlan plan = {0};
    uint8_t *buf = NULL;
    size_t len = 0;
    int rc = -1;
    if (bulk_plan_window_locked(txn, shard, begin, end, kh, &plan) != 0) {
        req_window_capture_hooks(rw, shard);
        rw->hook_state = plan.hook_state;
        rw->hooks_staged = plan.hooks_staged;
        plan.hook_state = NULL;
        plan.hooks_staged = 0;
        req_window_abort_pre_marker(rw);
        goto out;
    }
    bulk_window_plan_move(&rw->plan, &plan);
    req_window_capture_hooks(rw, shard);
    rw->hook_state = rw->plan.hook_state;
    rw->hooks_staged = rw->plan.hooks_staged;
    rw->plan.hook_state = NULL;
    rw->plan.hooks_staged = 0;
    if (rw->plan.nactive == 0) {
        req_window_abort_pre_marker(rw);
        rc = 0;
        goto out;
    }
    if (bulk_sync_fallback_payloads(txn, shard, begin, end) != 0)
        goto out;
    if (bulk_build_window_marker_v2(&rw->plan, &buf, &len) != 0)
        goto out;
    char final_name[128];
    int nn = snprintf(final_name, sizeof(final_name),
                      "%03x_batch_%u_%016llx_marker.dat",
                      (unsigned)shard->kf_shard_id,
                      (unsigned)rw->plan.batch_id,
                      (unsigned long long)txn->req->nonce);
    if (nn < 0 || (size_t)nn >= sizeof(final_name)) {
        errno = ENAMETOOLONG;
        goto out;
    }
    uint64_t t0m = now_us();
    int prc = marker_publish_file_atomic(txn->req->kf_dir, final_name,
                                         buf, len, rw->marker_path);
    commit_phase_us_record(&g_commit_marker_publish_us_total, t0m);
    if (prc < 0) goto out;
    rw->published = 1;
    __atomic_add_fetch(&g_commit_marker_publish_count, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&g_commit_windows_total, 1, __ATOMIC_RELAXED);
    rc = 0;                         /* prc==1 is installed and retained */
out:
    free(buf);
    bulk_window_plan_destroy(&plan); /* moved source is zero */
    if (rc != 0 && !rw->published) {
        req_window_abort_pre_marker(rw);
        for (size_t i = begin; i < end; i++)
            if (shard->recs[i].status == 0) shard->recs[i].status = -1;
    }
    return rc;
}

static int bulk_finalize_one_kf_window(BulkMutationTxn *txn,
                                       BulkMutationShard *shard,
                                       SlotcaskKfHandle *kh,
                                       ReqWindow *rw) {
    (void)shard;
    rw->plan.req_window = rw;
    if (bulk_activate_new_payloads_locked(txn, &rw->plan) != 0) return -1;
    if (bulk_apply_and_sync_indexes_locked(txn, &rw->plan) != 0) return -1;
    if (bulk_apply_and_sync_kf_locked(txn, kh, &rw->plan) != 0) return -1;
    if (bulk_tombstone_old_payloads_locked(txn, &rw->plan) != 0) return -1;
    rw->converged = 1;
    return 0;
}
```

The functions above are authoritative for `txn->req != NULL` only.
Preserve the monolithic legacy function and all current per-record status
assignments/test-pause calls inside the shared planning/apply primitives.

- **D5** is `bulk_sync_fallback_payloads` above. It marks
  `staged_in_wave` only after the sync succeeds, so a failed sync can be
  retried and can never precede a durable marker. Its filter
  (`staged_in_wave || !needs_write → skip`) only sees fallback-staged
  records if the M-phase fallback flags them — see the hunk below.
- **D5 fallback flag** — `bulk_plan_window_locked`'s OLD-derived branch
  stages NEW bytes via `bulk_stage_single_pending` but, on main today,
  sets neither `st->needs_write` nor `st->staged_in_wave`, so the D5
  filter above would skip every computed record (I2 violated for every
  `auto_create` object and OLD-derived update). Change — in that
  fallback, after a successful `bulk_stage_single_pending` call, add:

```c
                s->needs_write = 1;   /* staged bytes owe the D5 sync */
```

  With the flag set, the D5 filter collects exactly the fallback-staged
  records: P-wave records carry `staged_in_wave = 1` (already covered by
  the request payload flush) and are skipped; legacy mode is unaffected
  (its A pass syncs from plan-entry locations regardless of
  `needs_write`).

- **A/T deferral** — do not skip the store half of
  `bulk_seg_apply_and_sync`: A currently performs the flag=1 stores inside
  that combined helper. Add these complete helpers after `segloc_cmp`:

```c
static int segloc_vec_append(SegLoc **dst, size_t *n, size_t *cap,
                             const SegLoc *src, size_t add) {
    if (add == 0) return 0;
    if (*n > SIZE_MAX - add) { errno = EOVERFLOW; return -1; }
    size_t need = *n + add;
    if (need > *cap) {
        size_t nc = *cap ? *cap : 16;
        while (nc < need) {
            if (nc > SIZE_MAX / 2) { errno = EOVERFLOW; return -1; }
            nc *= 2;
        }
        if (nc > SIZE_MAX / sizeof(**dst)) { errno = EOVERFLOW; return -1; }
        SegLoc *v = realloc(*dst, nc * sizeof(*v));
        if (!v) return -1;
        *dst = v;
        *cap = nc;
    }
    memcpy(*dst + *n, src, add * sizeof(*src));
    *n = need;
    return 0;
}

static int size_add_checked(size_t *total, size_t add) {
    if (*total > SIZE_MAX - add) { errno = EOVERFLOW; return -1; }
    *total += add;
    return 0;
}

static int size_vec_append(size_t **dst, size_t *n, size_t *cap,
                           const size_t *src, size_t add) {
    if (add == 0) return 0;
    if (*n > SIZE_MAX - add) { errno = EOVERFLOW; return -1; }
    size_t need = *n + add;
    if (need > *cap) {
        size_t nc = *cap ? *cap : 16;
        while (nc < need) {
            if (nc > SIZE_MAX / 2) { errno = EOVERFLOW; return -1; }
            nc *= 2;
        }
        if (nc > SIZE_MAX / sizeof(**dst)) { errno = EOVERFLOW; return -1; }
        size_t *v = realloc(*dst, nc * sizeof(*v));
        if (!v) return -1;
        *dst = v;
        *cap = nc;
    }
    memcpy(*dst + *n, src, add * sizeof(*src));
    *n = need;
    return 0;
}

/* Apply segment flags without a durability wait. `locs` is sorted. */
static int bulk_seg_apply_flags(SlotcaskDb *db, const SegLoc *locs,
                                size_t n, uint8_t flag) {
    size_t i = 0;
    while (i < n) {
        size_t j = i + 1;
        while (j < n && locs[j].sid == locs[i].sid &&
               locs[j].fid == locs[i].fid) j++;
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, locs[i].sid, locs[i].fid);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
        for (size_t k = i; k < j; k++)
            __atomic_store_n(&h.map[locs[k].off + 18], flag,
                             __ATOMIC_RELEASE);
        segcache_release(&h);
        i = j;
    }
    return 0;
}
```

In `bulk_activate_new_payloads_locked`, replace its call to
`bulk_seg_apply_and_sync` with this complete branch:

```c
    if (txn->req) {
        ReqWindow *rw = plan->req_window;
        if (!rw || bulk_seg_apply_flags(txn->db, locs, n, 1) != 0 ||
            segloc_vec_append(&rw->a_locs, &rw->na, &rw->cap_a,
                              locs, n) != 0)
            rc = -1;
        else
            rc = 0;
    } else {
        rc = bulk_seg_apply_and_sync(txn->db, locs, n, 1, 1);
    }
```

Add `ReqWindow *req_window;` to `BulkWindowPlan` and set it to `rw` before
finalize/retry. In `bulk_tombstone_old_payloads_locked`, keep every
`slotcask_tombstone_mark` call; replace only its final sync with:

```c
    if (txn->req) {
        ReqWindow *rw = plan->req_window;
        rc = rw ? segloc_vec_append(&rw->t_locs, &rw->nt, &rw->cap_t,
                                    locs, n) : -1;
    } else {
        rc = bulk_seg_apply_and_sync(txn->db, locs, n, 0, 0);
    }
```

The A/T `SHARD_TEST_NOTE_SYNC` checks run only in the legacy branch; their
deferred checks move to `slotcask_bulk_request_flush_commit`.
- **I-phase deferral** — `bulk_apply_and_sync_indexes_locked` skips its
  flush block when `txn->req != NULL`; the touch set stays in
  `rw->plan.touch` for the request merge.
  Replace `int record_touches = txn->window_cap > 1;` with
  `int record_touches = txn->req != NULL || txn->window_cap > 1;` so every
  deferred window records its durability targets even in a test/configured
  one-record window. Restore `tls_idx_touch = NULL` through one cleanup
  label on every apply-hook return; no error path may leave the TLS pointer
  referencing a retained plan.
- **K-phase deferral** — retain every kf mutation in
  `bulk_apply_and_sync_kf_locked`, but replace its final sync block with:

```c
    if (txn->req) {
        ReqWindow *rw = plan->req_window;
        if (!rw) { errno = EINVAL; return -1; }
        if (size_vec_append(&rw->kf_slots, &rw->nkf, &rw->cap_kf,
                            plan->kf_slots, plan->nkf_slots) != 0)
            return -1;
        rw->kf_header_changed |= plan->kf_header_changed;
        return 0;
    }
    int rc = kfcache_sync_slots_locked(kh, plan->kf_slots,
                                       plan->nkf_slots,
                                       plan->kf_header_changed);
    if (rc == 0 && plan->nactive > 0 &&
        SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_K)) rc = -1;
    return rc;
```

The request commit barrier below merges these vectors and performs one kf
sync per dirty shard.

**Request flushes** (complete):

```c
static int slotcask_bulk_request_flush_payloads(SlotcaskBulkRequest *req) {
    /* Merge every shard's payload locations request-wide, dedupe, and
       flush in ONE pass — shared stream files are written once. */
    size_t total = 0;
    for (int s = 0; s < req->num_shards; s++)
        if (size_add_checked(&total, req->shards[s].np) != 0) return -1;
    if (total == 0) return 0;
    if (total > SIZE_MAX / sizeof(SegLoc)) { errno = EOVERFLOW; return -1; }
    SegLoc *all = malloc(total * sizeof(*all));
    if (!all) return -1;
    size_t n = 0;
    for (int s = 0; s < req->num_shards; s++) {
        memcpy(all + n, req->shards[s].p_locs,
               req->shards[s].np * sizeof(*all));
        n += req->shards[s].np;
    }
    qsort(all, n, sizeof(*all), segloc_cmp);
    size_t w = 0;
    for (size_t i = 0; i < n; i++)
        if (w == 0 || segloc_cmp(&all[w - 1], &all[i]) != 0) all[w++] = all[i];
    uint64_t t0 = now_us();
    int rc = bulk_seg_apply_and_sync(req->db, all, w, 0, 0);
    commit_phase_us_record(&g_commit_segment_sync_us_total, t0);
    free(all);
    for (int s = 0; s < req->num_shards; s++) req->shards[s].np = 0;
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_P)) rc = -1;
    return rc;
}

static int slotcask_bulk_request_flush_marker_dir(SlotcaskBulkRequest *req) {
    if (!req->any_published) return 0;
    int rc = fsync_dir(req->kf_dir);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_M)) rc = -1;
    return rc;
}

typedef struct {
    SlotcaskBulkRequest *req;
    int shard_id;
    int rc;
    int err;
} ReqKfSyncArg;

static void *req_kf_sync_worker(void *raw) {
    ReqKfSyncArg *a = raw;
    ReqShard *rs = &a->req->shards[a->shard_id];
    size_t total = 0;
    int header_changed = 0;
    for (size_t i = 0; i < rs->nwindows; i++) {
        ReqWindow *rw = &rs->windows[i];
        if (!rw->converged) continue;
        if (size_add_checked(&total, rw->nkf) != 0) {
            a->rc = -1; a->err = errno; return NULL;
        }
        header_changed |= rw->kf_header_changed;
    }
    if (total == 0 && !header_changed) return NULL;
    if (total > SIZE_MAX / sizeof(size_t)) {
        a->rc = -1; a->err = EOVERFLOW; return NULL;
    }
    size_t *slots = total ? malloc(total * sizeof(*slots)) : NULL;
    if (total && !slots) { a->rc = -1; a->err = ENOMEM; return NULL; }
    size_t n = 0;
    for (size_t i = 0; i < rs->nwindows; i++) {
        ReqWindow *rw = &rs->windows[i];
        if (!rw->converged) continue;
        if (rw->nkf > 0) {
            memcpy(slots + n, rw->kf_slots, rw->nkf * sizeof(*slots));
            n += rw->nkf;
        }
    }
    if (n > 1) qsort(slots, n, sizeof(*slots), size_cmp);
    size_t w = 0;
    for (size_t i = 0; i < n; i++)
        if (w == 0 || slots[w - 1] != slots[i]) slots[w++] = slots[i];
    SlotcaskKfHandle kh;
    if (kf_shard_acquire(&kh, a->req->db, a->shard_id, 1) != 0) {
        a->rc = -1; a->err = errno; free(slots); return NULL;
    }
    a->rc = kfcache_sync_slots_locked(&kh, slots, w, header_changed);
    a->err = a->rc == 0 ? 0 : errno;
    kfcache_release(&kh);
    free(slots);
    return NULL;
}

static int req_flush_kf(SlotcaskBulkRequest *req) {
    ReqKfSyncArg *args = calloc(req->ntouched, sizeof(*args));
    if (!args) return -1;
    for (size_t i = 0; i < req->ntouched; i++) {
        args[i].req = req;
        args[i].shard_id = req->touched[i];
    }
    parallel_for_io(req_kf_sync_worker, args, (int)req->ntouched,
                    sizeof(*args));
    int rc = 0, saved = 0;
    for (size_t i = 0; i < req->ntouched; i++) {
        if (args[i].rc == 0) continue;
        rc = -1;
        if (!saved) saved = args[i].err;
    }
    free(args);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_K)) rc = -1;
    if (rc != 0) errno = saved ? saved : EIO;
    return rc;
}

static int slotcask_bulk_request_flush_commit(SlotcaskBulkRequest *req) {
    /* 1. Index flush: merge converged windows' touch sets request-wide,
          dedupe, issue through Task 1's parallel issuer; bitmaps serial. */
    size_t total = 0;
    for (int s = 0; s < req->num_shards; s++)
        for (size_t i = 0; i < req->shards[s].nwindows; i++)
            if (req->shards[s].windows[i].converged)
                if (size_add_checked(
                        &total,
                        req->shards[s].windows[i].plan.touch.n) != 0)
                    return -1;
    if (total > 0) {
        uint64_t t0i = now_us();
        if (total > SIZE_MAX / sizeof(IdxTouch)) {
            errno = EOVERFLOW;
            return -1;
        }
        IdxTouch *all = malloc(total * sizeof(*all));
        if (!all) return -1;
        size_t n = 0;
        char eff_root[PATH_MAX], object[256];
        split_data_dir(req->db->data_dir, eff_root, sizeof(eff_root),
                       object, sizeof(object));
        for (int s = 0; s < req->num_shards; s++)
            for (size_t i = 0; i < req->shards[s].nwindows; i++) {
                ReqWindow *rw = &req->shards[s].windows[i];
                if (!rw->converged) continue;
                memcpy(all + n, rw->plan.touch.v,
                       rw->plan.touch.n * sizeof(*all));
                n += rw->plan.touch.n;
            }
        qsort(all, n, sizeof(*all), idx_touch_cmp);
        size_t w = 0;
        for (size_t i = 0; i < n; i++)
            if (w == 0 || idx_touch_cmp(&all[w - 1], &all[i]) != 0)
                all[w++] = all[i];
        n = w;   /* deduplicated count drives everything below */
        size_t npaths = 0;
        for (size_t i = 0; i < n; i++)
            if (all[i].type != IT_BITMAP) npaths++;
        if (npaths > 0) {
            char *path_buf = malloc(npaths * PATH_MAX);
            const char **paths = malloc(npaths * sizeof(*paths));
            if (!path_buf || !paths) {
                free(all); free(path_buf); free(paths);
                return -1;
            }
            size_t w2 = 0;
            for (size_t i = 0; i < n; i++) {
                if (all[i].type == IT_BITMAP) continue;
                int shard = idx_shard_for_hash(all[i].hash16,
                                               req->num_shards);
                if (all[i].type == IT_TRIGRAM)
                    tg_build_path(path_buf + w2 * PATH_MAX, PATH_MAX,
                                  eff_root, object, all[i].field, shard);
                else
                    build_idx_path(path_buf + w2 * PATH_MAX, PATH_MAX,
                                   eff_root, object, all[i].field, shard);
                paths[w2] = path_buf + w2 * PATH_MAX;
                w2++;
            }
            int frc = index_sync_path_set(paths, npaths);
            free(path_buf); free(paths);
            if (frc != 0) { free(all); return -1; }
            __atomic_add_fetch(&g_commit_index_sync_ops_total,
                               (uint64_t)npaths, __ATOMIC_RELAXED);
        }
        for (size_t i = 0; i < n; i++) {
            if (all[i].type != IT_BITMAP) continue;
            char bpath[1024];
            bitmap_shard_path(bpath, sizeof(bpath), eff_root, object,
                              all[i].field, all[i].idx_shard);
            BitmapShard *bm = bm_open(bpath, 0, 0, 0, 0, 1);
            if (!bm) {
                if (errno != ENOENT) { free(all); return -1; }
                continue;
            }
            int brc = bm_sync(bm);
            bm_close(bm);
            if (brc != 0) { free(all); return -1; }
            __atomic_add_fetch(&g_commit_index_sync_ops_total, 1,
                               __ATOMIC_RELAXED);
        }
        free(all);
        commit_phase_us_record(&g_commit_index_sync_us_total, t0i);
    }
    /* 2. K barrier: one mmap durability wait per dirty kf shard. */
    if (req_flush_kf(req) != 0) return -1;

    /* 3. Segment barriers: activation + tombstone bytes, request-wide
          dedupe, one pass each. Locations belong to ReqWindow. */
    for (int pass = 0; pass < 2; pass++) {
        size_t total = 0;
        for (int s = 0; s < req->num_shards; s++)
            for (size_t i = 0; i < req->shards[s].nwindows; i++)
                if (size_add_checked(
                        &total,
                        pass == 0 ? req->shards[s].windows[i].na
                                  : req->shards[s].windows[i].nt) != 0)
                    return -1;
        if (total == 0) continue;
        if (total > SIZE_MAX / sizeof(SegLoc)) {
            errno = EOVERFLOW;
            return -1;
        }
        SegLoc *all = malloc(total * sizeof(*all));
        if (!all) return -1;
        size_t n = 0;
        for (int s = 0; s < req->num_shards; s++)
            for (size_t i = 0; i < req->shards[s].nwindows; i++) {
                ReqWindow *rw = &req->shards[s].windows[i];
                if (pass == 0) {
                    memcpy(all + n, rw->a_locs, rw->na * sizeof(*all));
                    n += rw->na;
                } else {
                    memcpy(all + n, rw->t_locs, rw->nt * sizeof(*all));
                    n += rw->nt;
                }
            }
        qsort(all, n, sizeof(*all), segloc_cmp);
        size_t w = 0;
        for (size_t i = 0; i < n; i++)
            if (w == 0 || segloc_cmp(&all[w - 1], &all[i]) != 0) all[w++] = all[i];
        uint64_t t0s = now_us();
        int rc = bulk_seg_apply_and_sync(req->db, all, w, 0, 0);
        commit_phase_us_record(&g_commit_segment_sync_us_total, t0s);
        free(all);
        if (rc != 0) return -1;
    }
    if (SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_A)) return -1;
    if (SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_T)) return -1;
    /* 4. Batched clear: unlink every converged window's marker via its
          preserved path, then ONE dir fsync. Failed (non-converged)
          windows stay retained. */
    int any_unlinked = 0;
    int unlink_rc = 0;
    int unlink_errno = 0;
    for (int s = 0; s < req->num_shards; s++) {
        for (size_t i = 0; i < req->shards[s].nwindows; i++) {
            ReqWindow *rw = &req->shards[s].windows[i];
            if (!rw->published || !rw->converged || rw->cleared) continue;
            if (kfm2_unlink_by_path(rw->marker_path) != 0) {
                if (!unlink_errno) unlink_errno = errno;
                unlink_rc = -1;
                continue; /* still fsync every successful unlink */
            }
            rw->unlink_succeeded = 1;
            any_unlinked = 1;
        }
    }
    int dir_rc = 0;
    uint64_t t0c = now_us();
    if (any_unlinked && fsync_dir(req->kf_dir) != 0) dir_rc = -1;
    if (any_unlinked)
        commit_phase_us_record(&g_commit_marker_clear_us_total, t0c);
    if (any_unlinked && dir_rc == 0 &&
        SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_C)) dir_rc = -1;
    if (dir_rc != 0) return -1; /* no reclaim: a marker may survive crash */

    /* 5. Directory-durable clears: reclaim OLD capacity, then transfer
          terminal ownership. This is the only deferred commit_done site. */
    for (int s = 0; s < req->num_shards; s++) {
        for (size_t i = 0; i < req->shards[s].nwindows; i++) {
            ReqWindow *rw = &req->shards[s].windows[i];
            if (!rw->unlink_succeeded || rw->cleared) continue;
            rw->cleared = 1;
            bulk_reclaim_old_payloads_locked(&req->shards[s].txn,
                                              &rw->plan);
            if (rw->hooks_staged && rw->hooks.commit_done)
                rw->hooks.commit_done(rw->hooks.ctx, rw->hook_state);
            rw->hooks_staged = 0;
            rw->hook_state = NULL;
        }
    }
    if (unlink_rc != 0) errno = unlink_errno ? unlink_errno : EIO;
    return unlink_rc;
}
```

**IdxTouch failure sticky bit (review round 4, finding 6)** — `IdxTouchSet`
gains `int failed;` (`src/db/slotcask.c`, anchor: the `IdxTouchSet`
typedef); `idx_touch_record` sets `s->failed = 1` on the overflow/OOM
return paths instead of returning silently; and
`bulk_apply_and_sync_indexes_locked` fails before declaring the window
converged:

```c
    if (plan->touch.failed) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "window flush: touch set lost entries (OOM); failing "
                  "window before marker clear");
        return -1;
    }
```

placed immediately after the last index apply/`idx_touch_record` call and
before either the legacy per-window flush or the deferred request flush.
The check is unconditional: a lost touch is a durability error on both
paths. Legacy callers therefore fail the window instead of silently
clearing a marker whose index file may not have been synced.

### 3e. Call-site/consumer inventory (complete)

**All non-request mutation modes take the same gate.** Centralise this in
the legacy coordinator rather than duplicating it in five public wrappers.
Replace `slotcask_bulk_mutation_transaction` with this complete body (the
deferred coordinator never calls it, so it cannot self-deadlock):

```c
static int int_cmp(const void *ap, const void *bp) {
    int a = *(const int *)ap, b = *(const int *)bp;
    return (a > b) - (a < b);
}

static int slotcask_bulk_mutation_transaction(BulkMutationTxn *txn) {
    int *ids = NULL;
    size_t nids = 0;
    int rc = -1;
    if (!txn || !txn->db || !txn->shards || txn->nshards == 0) {
        errno = EINVAL;
        return -1;
    }
    ids = malloc(txn->nshards * sizeof(*ids));
    if (!ids) return -1;
    for (size_t i = 0; i < txn->nshards; i++)
        ids[nids++] = txn->shards[i].kf_shard_id;
    qsort(ids, nids, sizeof(*ids), int_cmp);
    size_t w = 0;
    for (size_t i = 0; i < nids; i++)
        if (w == 0 || ids[w - 1] != ids[i]) ids[w++] = ids[i];
    nids = w;
    for (size_t i = 0; i < nids; i++) writer_gate_lock(txn->db, ids[i]);

    if (bulk_stage_payload_wave(txn) != 0) goto out;
    if (bulk_commit_kf_windows_wave(txn) != 0) goto out;
    rc = bulk_finish_status(txn);
out:
    bulk_mutation_txn_free_state(txn);
    for (size_t i = nids; i > 0; i--)
        writer_gate_unlock(txn->db, ids[i - 1]);
    free(ids);
    return rc;
}
```

This covers single upsert/insert/delete adapters and the legacy per-shard
bulk entry points because they all route through this transaction. Audit
the repository for every direct mutating kf-handle acquisition; the only
additional runtime writer is pre-grow. Wrap its whole critical section:

```c
static void *slotcask_pregrow_worker(void *raw) {
    SlotcaskPregrowArg *a = raw;
    char kf_path[PATH_MAX];
    writer_gate_lock(a->db, a->kf_shard_id);
    kf_path_for(kf_path, a->db->data_dir, a->kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, a->db->slots_per_shard, 1) != 0) {
        writer_gate_unlock(a->db, a->kf_shard_id);
        return NULL;
    }
    if (kh.hdr) {
        uint64_t projected = kh.hdr->total + (uint64_t)a->add_records;
        while (kh.capacity < SLOTCASK_MAX_SLOTS_PER_SHARD &&
               projected * 4 >= (uint64_t)kh.capacity * 3) {
            if (kfcache_resplit_locked(&kh, kh.capacity * 2) != 0) break;
        }
    }
    kfcache_release(&kh);
    writer_gate_unlock(a->db, a->kf_shard_id);
    return NULL;
}
```

Bulk call sites must invoke `slotcask_pregrow_kf` before
`slotcask_bulk_request_execute`, never after request gates are acquired.
Startup materialisation and object-exclusive maintenance remain outside
the runtime writer-gate protocol. The direct-write-handle audit must classify
`slotcask_open_kf_worker` and startup marker recovery as pre-serving,
`slotcask_compact_segs` / `slotcask_compact_kf` as caller-held
`objlock_wrlock`, and `slotcask_test_set_kf_total` as test-only. These are
the only exemptions found by the baseline search; any new production hit
is a PLAN_NOTES halt until it either takes `writer_gate[shard]` or proves
object-exclusive/pre-serving execution.

- **Hook typedefs + implementations** (window-state protocol):
  `SlotcaskBulkOpts`'s five hook typedefs and every implementation —
  insert (`v2_bulk_ins_prepare_window` allocates/fills
  `BulkInsWindowState`; `apply_window` consumes it; `commit_done` /
  `release_window` / `abort_window` free it), update structured
  (`v2_bulk_upd_noop_prepare` → state NULL; apply builds args inline),
  delimited, JSON, delete key-list, delete criteria, and the
  single-record adapter (`upsert_adapter_prepare_window/apply_window` —
  state NULL, scratch stays in `actx` as today). slotcask.c stores and
  returns the pointer opaquely.

  In `bulk_plan_window_locked`, both prepare branches use the same complete
  ownership pattern (shown for upsert; substitute delete opts verbatim):

  ```c
  void *window_state = NULL;
  if (uo->prepare_window(recs, plan->active, n, uo->bulk_hook_ctx,
                         &window_state) != 0)
      goto hard_fail;                 /* failed prepare self-cleans */
  plan->hook_state = window_state;
  plan->hooks_staged = 1;
  ```

  In `bulk_apply_and_sync_indexes_locked`, replace both apply calls with:

  ```c
  void *window_state = plan->req_window
                     ? plan->req_window->hook_state
                     : plan->hook_state;
  rc = opts->apply_window(shard->recs, plan->active, plan->nactive,
                          opts->bulk_hook_ctx, window_state);
  ```

  where `opts` is the correctly tagged upsert/delete member—never a cast.
  The legacy `bulk_commit_one_kf_window` terminal block passes
  `(bulk_hook_ctx, plan.hook_state)` to exactly one of abort/commit/release,
  then sets `plan.hooks_staged = 0; plan.hook_state = NULL;` before plan
  destruction. Deferred publication moves those fields into `ReqWindow`
  as shown above.
- **Gate-covered runtime entries**: `slotcask_upsert_with_hooks`,
  `slotcask_insert_with_hooks`, `slotcask_delete_with_hooks`, both legacy
  bulk entries (through `slotcask_bulk_mutation_transaction`), pre-grow
  (explicit worker wrapper above), and all six new request call sites.

**Six `query_bulk.c` callers become preparation + one request.** Add this
shared, complete tail helper:

```c
static int bulk_execute_prepared(SlotcaskDb *sdb,
                                 SlotcaskBulkShardInput *inputs,
                                 size_t ninputs, size_t pregrow_count) {
    if (ninputs == 0) return 0;
    /* Pre-grow owns the same gates, so it must finish before request_begin. */
    if (pregrow_count > 0 &&
        slotcask_pregrow_kf(sdb, pregrow_count) != 0)
        return -1;
    return slotcask_bulk_request_execute(sdb, inputs, ninputs);
}
```

At each listed anchor, split the existing worker at its
`slotcask_bulk_{upsert,delete}_in_kfshard` call. The parallel worker keeps
all parsing, record construction, option construction, arenas, scratch,
and hook context, but replaces the mutation call with a by-value input:

```c
    work->input.kf_shard_id = work->shard_id;
    work->input.recs = work->batch;
    work->input.nrecs = work->batch_n;
    work->input.kind = SLOTCASK_BULK_INPUT_UPSERT; /* DELETE at delete sites */
    work->input.opts.upsert = work->opts;    /* .delete_ at delete sites */
    /* Delete-site variant (identical shape, delete union member):
       work->input.kind = SLOTCASK_BULK_INPUT_DELETE;
       work->input.opts.delete_ = work->delete_opts; */
```

After the preparation `parallel_for_io` joins, the command thread compacts
nonempty inputs into ascending unique shard order, calls
`bulk_execute_prepared` exactly once, copies `input.rc`/record statuses to
the response, and only then destroys each work object. The work object owns
`batch`, per-record user contexts, update arena, parsed JSON/delimited
buffers, hook scratch, and `out_durability_degraded`; therefore every
pointer inside the by-value option remains valid through the synchronous
request. Cleanup is a single command-level label after execute, including
all early errors.

Apply that hunk at all six anchors (no per-worker mutation call remains):

1. insert trio worker around `query_bulk.c:1055` / the call near 1084;
2. key-list delete around the call near 2698;
3. structured update around 3370/3388;
4. delimited update around 3858/3876;
5. JSON update around 4419/4437;
6. criteria delete around the call near 5275.

The insert command's existing pre-grow calls near 1570/2306 move to the
command-level `bulk_execute_prepared(..., pregrow_count)` call and are
removed from any worker. Add a post-edit assertion to Task 3 verification:
`rg "slotcask_bulk_(upsert|delete)_in_kfshard" src/db/query_bulk.c` returns
zero production call sites. Single-record adapters remain legacy callers
and are gate-covered by `slotcask_bulk_mutation_transaction`.
- **`kf_shard_marker_gate` + per-id helpers** → replaced by
  `kf_batch_marker_gate_refs` + the replay loop + `kfm2_unlink_by_path` /
  `kfm2_clear_by_path_sync` from §3b. Call sites: the deferred
  stage entry (once per shard) and startup recovery (unchanged contract —
  same replay primitive over refs).
- **`marker_publish_atomic`** → replaced by
  `marker_publish_file_atomic` (deferred requests) and
  `marker_publish_and_sync_dir` (legacy/single). Call sites: the publish
  half of the split window function (deferred) and the legacy publish
  path (unchanged behavior).
- **`BatchMarkerEntry`** → deleted; reader returns `KfMarkerSlot *`;
  planning composition uses `KfMarkerSlot` directly (Task 4 inventory
  has the full consumer list).

### 3f. Test-first — `src/test/cases/test_request_flush_batching.c` (new)

In-process (scaffold mirrors `test-commit-phase-metrics`; `rt_marker_scan`
lifted verbatim from `test_window_release_routes.c`). Extend
`ShardTestPhase` with `SHARD_TEST_PHASE_REQ_PUBLISHED` immediately before
`SHARD_TEST_PHASE_COUNT`. The coordinator calls
`SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_REQ_PUBLISHED)` after the marker
directory barrier and before finalize; the test arms
`g_shard_test_pause_phase`, waits on `g_shard_test_pause_hits`, and releases
with `g_shard_test_pause_release`, exactly through the existing TEST_BUILD
atomics. Keep `durability_test_pause(..., "req-published")` beside it for
cross-process crash tests.

1. **Multi-window retention, marker-file inspection only** (red on base:
   API/stat compile-red): `db.bulk_commit_window = 16`; deferred request,
   40 records with an indexed field on shard 0 → 3 windows. Arm the
   `req-published` pause; on pause assert `rt_marker_scan(base) == 3` —
   **marker files only: no `slotcask_get`, no same-shard write during the
   pause** (the coordinator holds that shard's writer gate — such a write
   would block forever); release; the waves complete; `rt_marker_scan ==
   0`; `commit_windows_total` increases by 3 while each existing
   segment/index/marker-clear timing counter records the request-level
   barrier rather than three per-window barriers; records readable after
   the join.
2. **Payload-flush failure**: arm the msync injector to fail the epoch-1
   flush → request −1; no markers on disk; records not committed.
3. **Commit-flush failure → retained → gate replay**: inject a commit-flush
   failure (existing injector) → request EINPROGRESS, writer gates
   released, shard wrlocks released; markers retained. Then corrupt /
   leave-golden a retained marker and run a follow-up single-record write
   on the shard: golden marker → gate accepts, replay converges, record
   visible; corrupt marker → gate fails closed (−1) with the marker
   retained.
4. **D5 computed payload**: object with an `auto_create` field, deferred
   insert, force commit-flush failure → reopen → replay converges → the
   computed field value is intact.
5. **Post-M failure semantics**: force one window's first finalize attempt
   to fail, let its idempotent retry converge, and assert its marker remains
   present until the common commit barrier. If both attempts fail, assert
   the marker is retained after request end and the request reports
   EINPROGRESS.
6. **Concurrency** (controlled pause/hook evidence — "both completed" is
   not proof):
   (a) two simultaneous multi-shard deferred requests on disjoint shards
   demonstrably overlap: pause request A after its publish wave; start B
   on disjoint shards; assert B completes (its own records readable)
   **while A is still paused**; release A; both converge;
   (b) two deferred requests sharing a shard serialize: while A is paused
   mid-request on shard S, a second request targeting S does not complete
   its stage wave (its worker is blocked on the gate); after A ends, B
   completes;
   (c) an ordinary single write to an untouched shard completes while a
   deferred request is paused;
   (d) an ordinary single write to a touched shard starts but does not
   complete while the deferred request is paused on that shard's gate, and
   completes after release;
   (e) primary-key readers during the pause observe only the old committed
   record or the new committed record — never a partially written record.
   Secondary-index scans retain main's per-mutation visibility semantics;
   this plan does not claim request snapshots or make readers take gates.
   Run the touched/untouched admission assertion as a table over single
   upsert, insert-only, delete, both legacy bulk entry points, and
   `slotcask_pregrow_kf`; this is the regression guard that the gate
   inventory is exhaustive, not merely correct for one write API.
7. **Inline/limited-pool execution**: run the deferred request with
   more shards than I/O-pool workers from inside one outer
   `parallel_for_io` task. `parallel.c`'s existing `t_in_pool_worker` branch
   forces every nested request phase inline; the request must complete
   without deadlock. A second fixture starts the pool with two workers and
   uses at least eight shard inputs, proving phase joins do not require one
   resident waiter per shard.

**Additional guards**: the five durability suites,
`test-window-release-routes`, `test-commit-phase-metrics`,
`test-bulk-idx-sync-batching`, `test-bulk-idx-types-batching`,
`test-single-op-index-sync`, `run-all`.

**Verify**: build clean; new tests + guards; `run-all`.

---

## Task 4 — marker format V2 (span trim, exact size, v1 refusal)

**Root cause.** V1 entries carry `hash[16]` + `klen` + `old_vlen` +
`new_vlen` solely to checksum spans replay never reads
(`slotcask.c:1006-1009`); marker size is O(record bytes), tripping the
64 MiB reader ceiling for large-value schemas.

**Format.** `16 B BatchMarkerHeader{magic, version=2, count, reserved=0}` +
`count` × 32 B `KfMarkerSlot`. `1 ≤ count ≤ 16384` (window ceiling) → exact
maximum 16 + 16384×32 = **524,304 B**; the 64 MiB magic cap is replaced by
this structural bound. V1 refused per policy (a).

The in-memory planner type is deliberately not the disk type:

```c
typedef struct ReqWindow ReqWindow;    /* completed by Task 3 */

typedef struct {
    KfMarkerSlot slot;                 /* the only bytes serialized in V2 */
    uint8_t      hash[16];             /* transient index/K planning data */
} BulkPlanEntry;
```

Change `BulkWindowPlan.entries` to `BulkPlanEntry *`. The planner keeps
populating `.hash` and `.slot`, while the writer serializes only `.slot`.
Delete the old `BatchMarkerEntry` V1 disk typedef and static assert; do not
reuse that name for request memory. `BulkWindowPlan` also carries
`char marker_path[PATH_MAX]`, `void *hook_state`, and `int hooks_staged`.
The legacy publisher writes the exact installed path into
`plan.marker_path`, and `bulk_clear_window_marker_locked` becomes
`kfm2_clear_by_path_sync(plan->marker_path)` rather than reconstructing an
ID-only name. The prepare hook writes state into the latter fields, the
move transfers it to `ReqWindow`, and the source is zeroed so exactly one
terminal callback owns it.

V2 formatting is independent of directory batching. Task 3's deferred
publish half calls `marker_publish_file_atomic(..., rw->marker_path)`;
the unchanged legacy/single-window coordinator calls
`marker_publish_and_sync_dir` immediately. Marker version never selects
the durability primitive.

**Test-first — `src/test/cases/test_marker_v2.c` (new).** Implement the
parser cases before Task 4 using a test-only exact-path wrapper around
`kfm2_read_batch_marker`; they need no request coordinator and are red on
base because V2 is unsupported. Write the gate integration cases red in
the same test-first checkpoint, then make them green with the combined
Task 3+4 implementation: create a real retained marker by injecting the request
commit-flush failure, wait for EINPROGRESS/gate release, locate it through
the `MarkerRef` scan, and drive validation through a follow-up writer.
Marker surgery never happens while a live request owns the shard gate.
Cases:

1. **golden**: untouched V2 marker → follow-up write's gate succeeds,
   replay converges, marker cleared, records readable (positive control).
2. **trailing byte** (append 1 B) → gate fails closed (−1), marker
   retained.
3. **truncated** (file cut to `16+(count−1)×32`) → fails closed.
4. **count = 0** → fails closed.
5. **count = 16385** with a size-matching file → fails closed.
6. **bad checksum** (flip a bit in slot 0 `new_offset`) → fails closed.
7. **invalid op** (`op` = 7) → fails closed.
8. **nonzero reserved** → fails closed.
9. **v1 refused (red on base)**: synthesize a valid **V1** marker (V1
   header + one 54 B entry, correct V1 checksums) → gate refuses with the
   v1 log line; on base the same marker replays successfully, so the
   refusal assertion fails.

Corrupt-marker cases expect the follow-up write to return an error (gate
fail closed) and the marker to remain on disk. The separate multi-window
test in Task 3f is the only test using `req-published`; it inspects names
and counts only, releases the pause, and never mutates marker contents.

**Changes — `src/db/slotcask.c`.**

4a. **Writer** (`bulk_publish_window_marker_locked`, anchor: the span-append
region `buf_append(... r->key ...)`, `s->old_buf`, `r->value`, and
`XXH32(buf + at, len - at, 0)`): extract the bytes-only serializer below.
The deferred publish half in Task 3 owns naming/publication and the legacy
window calls the serializer followed by
`marker_publish_and_sync_dir(kf_dir, final_name, buf, len,
plan->marker_path)`.
Place the serializer immediately before the legacy publisher; Task 3 later
reuses it from the split deferred publisher through the static prototype
shown there.

```c
static int bulk_build_window_marker_v2(const BulkWindowPlan *plan,
                                       uint8_t **out_buf,
                                       size_t *out_len) {
    *out_buf = NULL;
    *out_len = 0;
    if (!plan || plan->nactive == 0 || plan->nactive > 16384) {
        errno = EINVAL;
        return -1;
    }
    BatchMarkerHeader hdr = {
        .magic = KF_BATCH_MARKER_MAGIC,
        .version = KF_BATCH_MARKER_VERSION,
        .count = (uint32_t)plan->nactive,
        .reserved = 0,
    };
    size_t len = sizeof(hdr) + (size_t)plan->nactive * sizeof(KfMarkerSlot);
    uint8_t *buf = malloc(len);
    if (!buf) return -1;
    memcpy(buf, &hdr, sizeof(hdr));
    for (size_t i = 0; i < plan->nactive; i++) {
        KfMarkerSlot slot = plan->entries[i].slot;
        slot.checksum = 0;
        slot.checksum = XXH32(&slot, offsetof(KfMarkerSlot, checksum), 0);
        memcpy(buf + sizeof(hdr) + i * sizeof(slot), &slot, sizeof(slot));
    }
    *out_buf = buf;
    *out_len = len;
    return 0;
}
```

The V1 entry array and all span appends are deleted. No boolean "skip
fsync" parameter is introduced: the distinct publication functions in
Task 3 preserve the durability contract at their call sites. Both callers
free the returned buffer on every exit.

4b. **Reader** (`kfm2_read_batch_marker`): replace the whole function
(anchor: `static int kfm2_read_batch_marker`) with:

```c
static int kfm2_read_batch_marker(const char *path,
                                  KfMarkerSlot **out_slots,
                                  size_t *out_count) {
    struct stat st;
    BatchMarkerHeader hdr;
    KfMarkerSlot *slots = NULL;
    int fd = -1, rc = -1;
    if (!path || !out_slots || !out_count) { errno = EINVAL; return -1; }
    *out_slots = NULL;
    *out_count = 0;
    fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return errno == ENOENT ? 1 : -1;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
        st.st_size < (off_t)sizeof(hdr))
        goto out;
    ssize_t hn;
    do { hn = pread(fd, &hdr, sizeof(hdr), 0); }
    while (hn < 0 && errno == EINTR);
    if (hn != (ssize_t)sizeof(hdr)) { errno = EILSEQ; goto out; }
    if (hdr.magic != KF_BATCH_MARKER_MAGIC) goto out;
    if (hdr.version < KF_BATCH_MARKER_VERSION) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "marker %s: format v%u predates this binary (v%u); "
                  "all writes must be durable before upgrading",
                  path, (unsigned)hdr.version,
                  (unsigned)KF_BATCH_MARKER_VERSION);
        errno = ENOTSUP;
        goto out;
    }
    if (hdr.version != KF_BATCH_MARKER_VERSION || hdr.reserved != 0 ||
        hdr.count == 0 || hdr.count > 16384) {
        errno = EILSEQ;
        goto out;
    }
    uint64_t expect = (uint64_t)sizeof(hdr) +
                      (uint64_t)hdr.count * sizeof(*slots);
    if (expect > (uint64_t)INT64_MAX || (uint64_t)st.st_size != expect) {
        errno = EILSEQ;
        goto out;
    }
    slots = calloc((size_t)hdr.count, sizeof(*slots));
    if (!slots) goto out;
    size_t need = (size_t)hdr.count * sizeof(*slots), got = 0;
    while (got < need) {
        ssize_t nr = pread(fd, (uint8_t *)slots + got, need - got,
                           (off_t)sizeof(hdr) + (off_t)got);
        if (nr < 0 && errno == EINTR) continue;
        if (nr <= 0) { errno = EILSEQ; goto out; }
        got += (size_t)nr;
    }
    for (uint32_t i = 0; i < hdr.count; i++) {
        KfMarkerSlot check;
        memcpy(&check, &slots[i], sizeof(check));
        uint32_t stored = check.checksum;
        check.checksum = 0;
        if (!kf_marker_op_valid(&check) ||
            stored != XXH32(&check,
                            offsetof(KfMarkerSlot, checksum), 0)) {
            errno = EILSEQ;
            goto out;
        }
    }
    *out_slots = slots;
    *out_count = (size_t)hdr.count;
    slots = NULL;
    rc = 0;
out:
    free(slots);
    if (fd >= 0) close(fd);
    return rc;
}
```

The header and slots use positional reads, so consuming the header cannot
shift the slot read. Count and exact file size are checked before
allocation. The span walk and all per-entry span allocation/copy code are
deleted.

Replace the batch-id-based test reader with an exact-path wrapper and
declare it in `shard_db_internal.h`:

```c
int kf_batch_marker_read_path_for_test(const char *path,
                                       KfMarkerSlot *slots_out,
                                       size_t max_slots,
                                       size_t *out_count) {
    if (!path || !out_count) { errno = EINVAL; return -1; }
    KfMarkerSlot *slots = NULL;
    size_t count = 0;
    int rc = kfm2_read_batch_marker(path, &slots, &count);
    if (rc != 0) return rc;
    size_t copy = count < max_slots ? count : max_slots;
    if (copy && !slots_out) { free(slots); errno = EINVAL; return -1; }
    if (copy) memcpy(slots_out, slots, copy * sizeof(*slots));
    *out_count = count;
    free(slots);
    return 0;
}
```

The corruption helper likewise takes an exact path:

```c
int kf_batch_marker_corrupt_first_kf_slot_for_test(
        const char *path, uint32_t bad_kf_slot, int *out_has_old) {
    KfMarkerSlot *slots = NULL;
    size_t count = 0;
    if (kfm2_read_batch_marker(path, &slots, &count) != 0) return -1;
    if (count == 0) { free(slots); errno = EILSEQ; return -1; }
    KfMarkerSlot slot = slots[0];
    free(slots);
    if (out_has_old) *out_has_old = slot.has_old;
    slot.kf_slot = bad_kf_slot;
    slot.checksum = 0;
    slot.checksum = XXH32(&slot, offsetof(KfMarkerSlot, checksum), 0);
    int fd = open(path, O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return -1;
    ssize_t nw;
    do { nw = pwrite(fd, &slot, sizeof(slot),
                     (off_t)sizeof(BatchMarkerHeader)); }
    while (nw < 0 && errno == EINTR);
    int rc = nw == (ssize_t)sizeof(slot) && fsync(fd) == 0 ? 0 : -1;
    int saved = errno;
    close(fd);
    if (rc != 0) errno = saved ? saved : EIO;
    return rc;
}
```

It never accepts shard/batch IDs.

4c. **Version constant**: `enum { KF_BATCH_MARKER_VERSION = 2 };` (anchor:
`enum { KF_BATCH_MARKER_VERSION = 1 };`). Delete `BatchMarkerEntry` and its
static-assert.

4d. **Consumer enumeration** (complete list — every reference to
`BatchMarkerEntry` / the reader's return type):
- `kfm2_read_batch_marker` (reader → returns `KfMarkerSlot *`),
- `kf_batch_marker_gate_refs`'s replay loop (`slots[j]`),
- startup recovery's `MarkerRef` replay loop,
- `BulkWindowPlan` (`BatchMarkerEntry *` → `BulkPlanEntry *`) and every
  planner/finalizer use of `.slot` / `.hash`,
- `bulk_build_window_marker_v2` (writer serializes `.slot` only),
- `src/test/cases/test_marker_v2.c` (new; constructs bytes directly),
- `src/test/cases/test_durability_ordering.c` and any helper that
  synthesizes or inspects markers. Update test accessors to locate a unique
  exact `MarkerRef`; no helper reconstructs `<shard>_<batch>` paths.

Post-edit assertions: `rg "BatchMarkerEntry" src/` finds only the local V1
fixture typedef in `test_marker_v2.c`; `rg "kf_batch_marker_path\(" src/`
finds no production replay/clear call; every V2 test helper accepts an
exact path or a `MarkerRef`.

**Verify**: `run test-marker-v2`; five durability suites; `run-all`.

---

## Task 5 — docs, gates, handoff

1. `docs/concepts/concurrency.md`: two-epoch lifecycle (publish pass →
   barrier 1 → finalize pass → barrier 2 → batched clear), the five
   invariants (I1–I5), the writer-gate trade (single writers to touched
   shards stall for the request span; readers never blocked; untouched
   shards unaffected), the crash-point table (stage / after barrier 1 /
   after publish / in finalize / after barrier 2 / after clear), V2 format
   + v1-refusal upgrade note and per-shard writer-admission gates. The
   existing object rwlock remains schema/maintenance exclusion only;
   normal data mutations do not take it.
2. `docs/getting-started/configuration.md`: `BULK_COMMIT_WINDOW` row — the
   window bounds marker size (16 + cap × 32 B) and replay granularity, not
   barrier boundaries.
3. `docs/reference/changelog.md`: Unreleased entry — two-epoch request
   batching (op-count math), marker V2 (trim, exact-size validation, cap
   fix, v1-refusal upgrade note), nonce marker names, per-shard
   writer-admission gates (no object-wide write serialization), stream-dir
   durability fix, parallel index flush, and the request-level semantics
   of the existing commit phase counters (no new stats keys).
4. AGENTS.md: storage-model bullet — marker format V2 sentence. No
   sanitizer invocation changes.
5. Gates per AGENTS.md: full suite fresh, then local all-core
   `BUILD_MODE=asan` ×3 and `BUILD_MODE=tsan` ×3 full-suite runs.
6. Leave everything uncommitted; handoff lists base commit, diff stat, gate
   outputs, and the disk-backed bench protocol
   (`SHARD_TEST_TMPDIR=/var/tmp`, branch vs main A/B).

## Risk register

- **Writer-gate stalls** — single writers to touched shards stall for the
  request span; readers never blocked; untouched shards unaffected.
  `BULK_COMMIT_WINDOW` and request chunking remain the levers.
- **Gate-order deadlock** — impossible by construction: the coordinator
  acquires ascending and releases reverse; ordinary writers hold exactly
  one gate.
- **Wave dispatch shares the IO pool** — waves run via parallel_for_io on
  the shared IO pool. Wave tasks never take writer gates and their kf
  locks are phase-local, so gate-blocked single writers occupy server
  connection threads and cannot starve the pool: the pool self-drains and
  the request's waves always make progress. Residual: wave tasks may
  queue behind unrelated IO work (bounded by task completion), and if a
  cmd handler ever runs on the IO pool itself, `t_in_pool_worker` forces
  every phase inline — correct, but the request then executes
  per-shard-serialized. Both covered by the 3e inline/limited-pool tests.
- **Gate replay semantics** — retained markers are gated+replayed once per
  shard at stage-wave start (under the held kf wrlock), so no follow-up
  writer can observe half-replayed state mid-request.
- **NOTE_SYNC relocation** — phase-note call sites move with the barriers;
  durability tests asserting old placements are updated in-task with the
  failure pasted.
- **V2 refusal on unclean upgrade** — policy (a), deliberate; the log line
  is the operator runbook; dual-read remains the documented fallback.
