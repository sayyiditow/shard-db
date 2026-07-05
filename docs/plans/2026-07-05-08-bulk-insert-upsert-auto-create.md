# Plan: fix auto_create on bulk-insert-as-upsert

## Execution rules (read first)

- Execute on the existing `feat/auto-create-implement` branch (do NOT
  create a new branch) — this plan is part 2 of that same feature, stacked
  on top of the already-executed, uncommitted single-insert/update/
  bulk-update work. Confirm you're on that branch before starting; if it
  doesn't exist or has already been committed/merged, STOP and check with
  the user before proceeding.
- Do tasks in order. Each task is self-contained; do not reorder.
- Build with `SKIP_TESTS=1 ./build.sh`. Test with
  `./build/bin/shard-db-test run-all` (or `run <name>` for a single case).
- Every insertion/edit below locates its site by **quoted anchor text**, not
  line numbers — another model may be working concurrently on a separate
  branch, so line numbers drift. If a quoted anchor is not found exactly
  (whitespace, wording, anything), STOP. Do not guess or reinterpret. Write
  `PLAN_NOTES.md` describing exactly what you found instead and why it
  doesn't match, then stop and wait for the user.
- Never claim a step passed without pasting the real command output.
- Leave the work **uncommitted** when done. Do not `git add`/`commit`/`push`/
  open a PR — that happens outside this session, by the user.

## Background

`feat/auto-create-implement` fixed `DK_AUTO_CREATE` (the `:auto_create`
field-default, "server datetime, INSERT only") for:

- single-record insert (`cmd_insert_v2`, storage.c) — stamps `now()` on a
  genuinely fresh key, preserves the stored value across re-insert/upsert of
  an existing key, and unconditionally overwrites any client-supplied value
  on a fresh insert (contract: auto_create is server-controlled at creation
  time only).
- single-record update (`cmd_update_v2`) — already correct by construction,
  no change needed (it starts from a `memcpy` of the old record and only
  overwrites explicitly-named fields).
- bulk-update (3 sites: JSON, delimited/CSV, and the shared `value_compute`
  machinery) — already correct by construction, same reasoning as
  single-record update. Verified by inspection, no change needed.
- The user separately decided: update / bulk-update **may** let a client
  explicitly overwrite an auto_create field (normal update semantics — no
  guard needed there).

**What's still wrong:** bulk-insert (`bulk_ins_run` for JSON,
`bulk_ins_delim_run` for delimited/CSV — both funnel into the same phase-2
write path, `bulk_insert_shard_worker_v2` in query.c) is *also* an upsert
(an existing key gets overwritten, not rejected). When a client bulk-inserts
a key that already exists and omits the auto_create field from the payload,
`typed_encode_defaults` (called during phase 1, before any existence check)
stamps a fresh `now()` into that field — exactly the bug Task 3 fixed for
single-insert, but still present on the bulk path. Bulk-insert of an
existing key silently resets its `created` timestamp.

## Why this doesn't need a `slotcask_get` probe

The single-insert fix (Task 3 in the prior plan) called `slotcask_get`
directly because `cmd_insert_v2` had no other way to learn whether the key
already existed. Bulk-insert is different: it goes through
`slotcask_bulk_upsert_in_kfshard`, which is itself an upsert primitive — it
already probes the kf shard to decide insert-vs-overwrite, and it already
supports a hook that runs **before** the record is written to the segment
and receives the prior record for free:

- `SlotcaskBulkOpts.value_compute` (`slotcask_bulk_value_fn`, declared
  `src/db/slotcask.h:496`) fires in the primitive's "Phase 1c", strictly
  before the segment `memcpy` that durably writes the record ("Phase 3").
  Setting `.value_compute` alone is enough to force the primitive to read
  the OLD record for us: `needs_old = opts->pre_commit_needs_old ||
  opts->value_compute != NULL` (`slotcask.c:4009`). There is no second flag
  to remember — forgetting one can't leave `old` NULL while `value_compute`
  still runs.
- This is the exact mechanism the three bulk-update `value_compute`
  callbacks (`v2_bulk_upd_value_compute` et al., query.c ~3387/~3873/~4340)
  already use to preserve everything the client didn't explicitly name.
  Bulk-insert just never adopted it because, until now, it never needed to
  look at the old record.
- By contrast, `pre_commit` (used today by bulk-insert for index
  maintenance, `v2_bulk_ins_pre_commit_bulk`) fires in "Phase 4", **after**
  the segment write already happened (`seg_record_emit`,
  `slotcask.c:1708-1731`) — mutating the record buffer there has no effect
  on what's persisted. That's a dead end; do not attempt to extend
  `pre_commit` for this.

So the fix is: give bulk-insert a `value_compute` hook, gated behind a
`has_ac` check computed once per shard-worker (mirroring the single-insert
fix's gate), so schemas without an auto_create field never set
`.value_compute` and pay zero extra cost — no probe, no extra read, no
change to the existing fast path.

## Side effect on the delimited (CSV) bulk-insert path — read before Task 2

`bulk_ins_delim_run` and `bulk_ins_run` (JSON) both dispatch through the
same phase-2 entry point, `bulk_insert_shard_worker_v2`. The CSV path does
**not** call `typed_encode_defaults` at all — it encodes only the columns
present in each line directly via `encode_field_len`, leaving any field the
client's row leaves empty as zero bytes (from the initial
`memset(payload, 0, ts->total_size)`). This means CSV bulk-insert currently
never stamps `:auto_create` (or any other default-kind field) at all — a
separate, pre-existing gap, out of scope for this plan.

Because the fix in this plan lives in the shared worker, it will also
start correctly stamping `now()` for CSV-inserted auto_create fields on a
genuinely fresh key (there's no "old" to preserve, so the `!old` branch
fires and stamps), and correctly preserving the old value on a CSV
upsert-of-existing-key. This is a strict improvement (zero → correct) and
low-risk since it reuses the identical code path already verified for
JSON — but it IS a behavior change for the CSV path, so Task 4 adds an
explicit test for it and the task list calls it out by name. Do not treat
this as scope creep to silently skip; keep the CSV test in the task list so
the behavior change is deliberately verified, not incidental.

---

## Task 1 — expose `auto_now_str` across translation units

`auto_now_str` currently lives as a `static` helper in `src/db/storage.c`
(added by the single-insert fix). query.c needs to call it too.

**File: `src/db/storage.c`**

Find this exact anchor:

```c
/* Produce the "now" string for an auto_create / auto_update timestamp field in
   the form its type expects. buf must be >= 24 bytes.
     FT_TIMESTAMP  — Unix epoch ms (decimal)
     FT_DATETIMEMS — yyyyMMddHHmmssSSS
     FT_DATE       — yyyyMMdd
     other (FT_DATETIME / fallback) — yyyyMMddHHmmss */
static void auto_now_str(const TypedField *f, char *buf, size_t bufsz) {
```

Replace with (drop `static` only — body unchanged):

```c
/* Produce the "now" string for an auto_create / auto_update timestamp field in
   the form its type expects. buf must be >= 24 bytes.
     FT_TIMESTAMP  — Unix epoch ms (decimal)
     FT_DATETIMEMS — yyyyMMddHHmmssSSS
     FT_DATE       — yyyyMMdd
     other (FT_DATETIME / fallback) — yyyyMMddHHmmss */
void auto_now_str(const TypedField *f, char *buf, size_t bufsz) {
```

**File: `src/db/types.h`**

Find this exact anchor:

```c
void encode_field(const TypedField *f, const char *val, uint8_t *out);
void encode_field_len(const TypedField *f, const char *val, size_t vlen, uint8_t *out);
```

Replace with:

```c
void encode_field(const TypedField *f, const char *val, uint8_t *out);
void encode_field_len(const TypedField *f, const char *val, size_t vlen, uint8_t *out);
/* Produce the "now" string for an auto_create / auto_update timestamp field
   in the form its type expects (buf must be >= 24 bytes). Shared between
   storage.c's single-record insert/update paths and query.c's bulk-insert
   value_compute hook. */
void auto_now_str(const TypedField *f, char *buf, size_t bufsz);
```

Build after this task (`SKIP_TESTS=1 ./build.sh`) and confirm it still
compiles — this task is a pure visibility change, no behavior change.

---

## Task 2 — add the `value_compute` hook to bulk-insert

**File: `src/db/query.c`**

Find this exact anchor (the `V2BulkInsCtx` struct, already existing):

```c
typedef struct {
    BulkInsShardWork *sw;
    BulkInsRecord    *rec;
    /* Per-worker arena for OLD index-key extraction during update upserts.
       nidx slots × INDEX_KEY_MAX bytes, reused across every record in this
       worker's kf-shard slice. pre_commits fire serially under the kf
       wrlock so reuse is safe. NEW keys can't share the arena — they're
       queued into sw->idx_pairs[fi] and consumed by btree_bulk_merge
       after the pre_commit returns. */
    uint8_t          *old_arena;
    size_t            old_arena_slot;
} V2BulkInsCtx;
```

Leave this struct unchanged (no new fields needed — the callback below
reaches the schema via `ctx->sw->ts`, exactly like the existing
`v2_bulk_ins_pre_commit_bulk` reaches `ctx->sw` today). Immediately after
this struct's closing `} V2BulkInsCtx;` line, insert:

```c

/* value_compute hook: corrects :auto_create fields before the segment
   write happens (Phase 1c of slotcask_bulk_upsert_in_kfshard — strictly
   before the record is persisted, unlike pre_commit which fires after).
   Only installed when the schema actually declares an auto_create field
   (see has_ac gate in bulk_insert_shard_worker_v2), so ordinary bulk
   inserts never pay for this.

   rec->value already holds the fully-encoded payload from phase 1
   (typed_encode_defaults for the JSON path; direct encode_field_len for
   the delimited/CSV path) — that encode had no way to know whether this
   key already exists, so any auto_create field in it is either a fresh
   now() stamp (client omitted it) or whatever the client explicitly
   supplied. This hook is the only place that corrects it:
     - key already existed (old->value != NULL): the field must NOT change
       on an upsert — restore the original bytes from the old record.
     - key is a genuine fresh insert (old == NULL): re-stamp now()
       unconditionally, even though phase 1 may already have stamped it,
       to overwrite any client-supplied override (mirrors cmd_insert_v2's
       Task 3 fix in storage.c — same contract, same reasoning). */
static int v2_bulk_ins_ac_value_compute(const SlotcaskOldRecord *old,
                                         SlotcaskBulkRec *rec) {
    V2BulkInsCtx *ctx = (V2BulkInsCtx *)rec->user_ctx;
    const TypedSchema *ts = ctx->sw->ts;
    uint8_t *buf = (uint8_t *)rec->value;

    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed ||
            ts->fields[i].default_kind != DK_AUTO_CREATE) continue;
        size_t off = (size_t)ts->fields[i].offset;
        size_t w   = (size_t)ts->fields[i].size;
        if (old && old->value && old->vlen >= off + w) {
            memcpy(buf + off, old->value + off, w);
        } else if (!old) {
            char tbuf[24];
            auto_now_str(&ts->fields[i], tbuf, sizeof(tbuf));
            encode_field(&ts->fields[i], tbuf, buf + off);
        }
        /* existed but old record too short (field added post-hoc): leave
           whatever phase 1 already encoded, same as the single-insert
           fix's equivalent case. */
    }
    return 0;
}
```

Now find this exact anchor (inside `bulk_insert_shard_worker_v2`):

```c
    SlotcaskBulkOpts opts = {
        .if_not_exists        = sw->if_not_exists,
        .pre_commit           = v2_bulk_ins_pre_commit_bulk,
        /* OLD value only needed when there are indexes to update; otherwise
           the hook returns immediately. Tells the primitive to skip the
           per-record read_record_value on UPDATE. */
        .pre_commit_needs_old = sw->nidx > 0,
    };
```

Replace with:

```c
    /* has_ac gate: only wire up the auto_create value_compute hook (and
       the OLD-record read it implies) when the schema actually declares
       an auto_create field. Ordinary bulk inserts pay nothing — this is
       the same has_ac gate the single-insert fix uses in
       storage.c's cmd_insert_v2. */
    int has_ac = 0;
    for (int i = 0; i < sw->ts->nfields; i++) {
        if (!sw->ts->fields[i].removed &&
            sw->ts->fields[i].default_kind == DK_AUTO_CREATE) { has_ac = 1; break; }
    }

    SlotcaskBulkOpts opts = {
        .if_not_exists        = sw->if_not_exists,
        .pre_commit           = v2_bulk_ins_pre_commit_bulk,
        /* OLD value only needed when there are indexes to update; otherwise
           the hook returns immediately. Tells the primitive to skip the
           per-record read_record_value on UPDATE. */
        .pre_commit_needs_old = sw->nidx > 0,
        .value_compute        = has_ac ? v2_bulk_ins_ac_value_compute : NULL,
    };
```

---

## Task 3 — build

Run `SKIP_TESTS=1 ./build.sh`. Paste the real build output. It must finish
with no errors. If there are warnings about unused `auto_now_str` in
storage.c or similar, STOP and write `PLAN_NOTES.md` — do not silence
warnings by adding casts/attributes not specified here.

---

## Task 4 — tests

**File: `src/test/cases/test_auto_create.c`**

Find this exact anchor (the end of the file, before the registration line):

```c
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-create", test_auto_create_run)
```

Replace with (adds bulk-insert-JSON-upsert, bulk-insert-JSON-fresh-override,
and bulk-insert-delimited-upsert coverage; keeps the existing close/return):

```c
    /* Bulk-insert (JSON) used as upsert: re-inserting an existing key via
       bulk-insert must preserve auto_create, exactly like single insert. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk1\","
        "\"value\":{\"data\":\"x\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk1\"}", &resp);
    char *bc1 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(bc1, "bulk: created present after initial insert");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"ac\",\"object\":\"t\","
        "\"records\":[{\"key\":\"bk1\",\"value\":{\"data\":\"y\"}}]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"data\":\"y\"", "bulk: data updated on bulk-insert upsert");
    char *bc2 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(bc2, "bulk: created present after bulk-insert upsert");
    ASSERT_TRUE(bc1 && bc2 && strcmp(bc1, bc2) == 0,
                "bulk: created preserved across bulk-insert upsert");
    free(resp); resp = NULL;

    /* Bulk-insert (JSON) fresh key with a client-supplied auto_create value:
       must be overwritten, same contract as single insert. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"ac\",\"object\":\"t\","
        "\"records\":[{\"key\":\"bk2\",\"value\":{\"data\":\"z\",\"created\":9999999}}]}",
        &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk2\"}", &resp);
    char *bc3 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(bc3, "bulk: created present on fresh bulk-insert");
    ASSERT_TRUE(bc3 && strcmp(bc3, "9999999") != 0,
                "bulk: client-supplied created overwritten on fresh bulk-insert");
    free(resp); resp = NULL;
    free(bc1); free(bc2); free(bc3);

    /* Bulk-insert-delimited (CSV) used as upsert: same preservation
       contract. Columns are key|data|created (created column is ignored —
       CSV bulk-insert never applied default_kind fields before this fix
       and still doesn't accept a client override for auto_create; the
       column here is a placeholder empty value). Field order in
       fields.conc is data, created — first CSV column after key is
       "data". */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk3\","
        "\"value\":{\"data\":\"p\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk3\"}", &resp);
    char *bc4 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(bc4, "csv: created present after initial insert");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"ac\",\"object\":\"t\","
        "\"delimiter\":\"|\",\"data\":\"bk3|q|\\n\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"data\":\"q\"", "csv: data updated on delimited upsert");
    char *bc5 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(bc5, "csv: created present after delimited upsert");
    ASSERT_TRUE(bc4 && bc5 && strcmp(bc4, bc5) == 0,
                "csv: created preserved across delimited upsert");
    free(resp); resp = NULL;
    free(bc4); free(bc5);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-create", test_auto_create_run)
```

Before writing this task, verify the exact wire field name for
`bulk-insert`'s inline-JSON-array parameter (some call sites in this
codebase use `"data"`, others `"records"` — check `docs/query-protocol/
bulk.md` and/or `cmd_bulk_insert_string` in query.c for the actual accepted
key name) and the exact wire field name for `bulk-insert-delimited`'s
inline-string parameter (may be `"data"` or something else — check
`docs/query-protocol/bulk.md`). If either name in the snippet above doesn't
match what the server actually accepts, fix the JSON in this task to use
the correct key — this is a mechanical correction (wire protocol field
name), not a design decision, so make it and note in your final summary
which name you used and why.

Run:

```
./build/bin/shard-db-test run test-auto-create
```

Paste the real output. Expect all assertions to pass (0 failed).

---

## Task 5 — full suite

Run `./build/bin/shard-db-test run-all`. Paste the real
`# total: N passed, 0 failed` line. If anything fails, investigate — do not
report success without the literal output.

---

## Task 6 — supersedes: none

This is the final task. Leave the work uncommitted. Report back:
- Whether Task 1-2's anchors were found exactly as quoted (or what
  `PLAN_NOTES.md` you had to write if not).
- The full build output from Task 3.
- The full test output from Tasks 4 and 5.
- Which wire field names you used for `bulk-insert` / `bulk-insert-delimited`
  inline data in Task 4's test, and where you confirmed them.
