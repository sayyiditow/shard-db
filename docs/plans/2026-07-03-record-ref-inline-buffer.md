# Perf: avoid malloc/free per record in read_record_ref for records that fit inline

## Execution rules (read first)

- Branch off `main`: `git checkout -b perf/record-ref-inline-buffer`.
- Build with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- **Never claim the task passed without pasting the real command output.** "# total: N passed, 0 failed" from the actual test binary is the only acceptable evidence it's done.
- Every edit below is located by **quoted anchor text**, not line numbers. If an anchor is not found character-for-character, **stop immediately** and write `docs/plans/PLAN_NOTES.md` describing what you searched for and what you found instead — do not guess.
- Do all three tasks — they only compile/behave correctly together (task 1 adds the struct field tasks 2 and 3 depend on).
- Leave all changes **uncommitted** on the branch when done.

## Background

`v2_record_capture_cb` (`src/db/query.c`) — the callback `read_record_ref` uses to fetch one record by hash — does `r->v2_buf = malloc(klen + vlen + 1)` on every call. `read_record_ref` has 9+ call sites across indexed `find`/`count`/joins/CAS (confirmed: `query.c:9227`, `11105`, `11753`, `16099`, `16249`, `17531`, `18959`, `19542`, `23585`), so a query touching many records via an index pays one malloc + one free per record just for this fetch step.

**Why a shared/reused buffer (arena) would be unsafe here, and why an inline struct field is the right fix instead:** every call site follows a `read_record_ref(...) → use rr.key/rr.val → release_record_ref(&rr)` pattern *before* moving to the next record (verified directly at `query.c:16098-16114`) — so lifetime-wise a reusable buffer would be fine *if* it were safe to share across every possible caller. But `RecordRef` is a plain stack local at every call site (`RecordRef rr;`), used from many different, not-fully-audited code paths (joins, CAS, KeySet iteration) — a shared thread-local or global reusable buffer would silently corrupt data the moment any single call site (present or future) holds one `RecordRef` "live" while making a *second*, *nested* `read_record_ref` call on the same thread (e.g. a join fetching a driver record and, mid-use, fetching a related record). Auditing every current and future call site for that invariant isn't a one-time cost — it becomes a standing constraint on the whole codebase.

The fix that avoids that risk entirely: embed a fixed-size buffer **inside the `RecordRef` struct itself**. Since `RecordRef` already lives on each call site's own stack frame (not shared, not persisted across calls), an inline buffer field has the exact same lifetime and scoping as the struct itself — zero cross-call state, zero nesting risk, works correctly for every current and future call site with no auditing required. This mirrors an existing pattern already proven in this exact file: `query.c:16105-16107` uses `uint8_t stk[2048]; ... : malloc(...)` — try a fixed on-stack buffer first, fall back to malloc only when the record doesn't fit. `RecordRef` gets the same treatment, sized to match that existing convention.

Records that don't fit in the inline buffer still fall back to `malloc()` exactly as today — this is a pure improvement for the common case, never a regression for the uncommon (large-record) case.

## Task 1 — Add the inline buffer field to RecordRef

### File: `src/db/types.h`

Find this exact block:

```c
/* Indexed record fetch: layout-agnostic dispatch for hash-based lookups.
   v1 path holds an FcacheRead handle; v2 holds a malloc'd copy of the
   record. Either way, key + val point into a contiguous buffer with
   layout `[key bytes][val bytes]`, matching v1 Zone B. Caller must
   call release_record_ref to free both lifetimes. */
typedef struct {
    FcacheRead     fc;        /* v1: kept open to keep mmap alive; .map=NULL on v2 */
    uint8_t       *v2_buf;    /* v2: malloc'd; NULL on v1 */
    const uint8_t *key;
    size_t         klen;
    const uint8_t *val;
    size_t         vlen;
} RecordRef;
```

Replace it with:

```c
/* Indexed record fetch: layout-agnostic dispatch for hash-based lookups.
   v1 path holds an FcacheRead handle; v2 holds a copy of the record in
   inline_buf (fits) or a malloc'd fallback (too large for inline_buf).
   Either way, key + val point into a contiguous buffer with layout
   `[key bytes][val bytes]`, matching v1 Zone B. Caller must call
   release_record_ref to free both lifetimes. */
typedef struct {
    FcacheRead     fc;        /* v1: kept open to keep mmap alive; .map=NULL on v2 */
    uint8_t       *v2_buf;    /* v2: points at inline_buf (common case) or a
                                  malloc'd fallback (record too large for
                                  inline_buf); NULL on v1 */
    uint8_t        inline_buf[2048]; /* v2 fast path — avoids malloc/free for
                                         records that fit; same size convention
                                         as the stk[2048] pattern in query.c's
                                         KeySet-fallback record collection */
    const uint8_t *key;
    size_t         klen;
    const uint8_t *val;
    size_t         vlen;
} RecordRef;
```

## Task 2 — Use the inline buffer in v2_record_capture_cb

### File: `src/db/query.c`

Find this exact block:

```c
static int v2_record_capture_cb(const uint8_t hash[16],
                                 const void *key, size_t klen,
                                 const void *value, size_t vlen,
                                 void *ctx) {
    (void)hash;
    RecordRef *r = (RecordRef *)ctx;
    r->v2_buf = malloc(klen + vlen + 1);
    if (!r->v2_buf) return 1;
    memcpy(r->v2_buf, key, klen);
    if (vlen) memcpy(r->v2_buf + klen, value, vlen);
    r->v2_buf[klen + vlen] = 0;
    r->key = r->v2_buf;
    r->klen = klen;
    r->val = r->v2_buf + klen;
    r->vlen = vlen;
    return 1;  /* found; stop */
}
```

Replace it with:

```c
static int v2_record_capture_cb(const uint8_t hash[16],
                                 const void *key, size_t klen,
                                 const void *value, size_t vlen,
                                 void *ctx) {
    (void)hash;
    RecordRef *r = (RecordRef *)ctx;
    size_t total = klen + vlen + 1;
    r->v2_buf = (total <= sizeof(r->inline_buf)) ? r->inline_buf : malloc(total);
    if (!r->v2_buf) return 1;
    memcpy(r->v2_buf, key, klen);
    if (vlen) memcpy(r->v2_buf + klen, value, vlen);
    r->v2_buf[klen + vlen] = 0;
    r->key = r->v2_buf;
    r->klen = klen;
    r->val = r->v2_buf + klen;
    r->vlen = vlen;
    return 1;  /* found; stop */
}
```

## Task 3 — Only free() when the fallback path was actually used

### File: `src/db/query.c`

Find this exact block:

```c
void release_record_ref(RecordRef *r) {
    if (!r) return;
    if (r->fc.map) { fcache_release(r->fc); r->fc.map = NULL; }
    if (r->v2_buf) { free(r->v2_buf); r->v2_buf = NULL; }
    r->key = r->val = NULL;
    r->klen = r->vlen = 0;
}
```

Replace it with:

```c
void release_record_ref(RecordRef *r) {
    if (!r) return;
    if (r->fc.map) { fcache_release(r->fc); r->fc.map = NULL; }
    /* Only free the malloc'd fallback — inline_buf is part of the
       caller's own RecordRef and needs no explicit release. */
    if (r->v2_buf && r->v2_buf != r->inline_buf) free(r->v2_buf);
    r->v2_buf = NULL;
    r->key = r->val = NULL;
    r->klen = r->vlen = 0;
}
```

### Invariant this preserves

`read_record_ref` still `memset(out, 0, sizeof(*out))`s the whole `RecordRef` (including `inline_buf`) at the start of every call — this is now a slightly larger memset (~2 KB) but still far cheaper than the heap allocation it replaces for the common case, and behavior is unchanged from today either way. Records too large for `inline_buf` fall back to `malloc()` exactly as before — no change in behavior or correctness for that case, only for the (now faster) common case.

## Verification

1. `SKIP_TESTS=1 ./build.sh` — must complete with no compile errors.
2. `./build/bin/shard-db-test run-all` — paste the real output; must show `# total: N passed, 0 failed` with N equal to the pre-change total (4891).

Do not report this plan as complete without pasting the actual output of step 2.
