# Phase 3B.2 — actual-size owning write callback

## Primary goal

Allow exact-decimal and large-varchar updates to produce and retain only actual
NEW bytes without a schema-maximum scratch allocation, while existing applications
continue working. This child prepares the storage constructor seam; it does not
yet make public packed objects usable or complete Phase 3B.

**Status:** Tasks 0–3 executed and raw-diff reviewed; cleanup and commit authorized. Packed storage remains inactive.
**Revision 2 (handoff cleanup):** allow the saved review document during Task 0,
and state the lifecycle-test coverage boundary explicitly. Embedded code/tests,
anchors, checkpoints and block hashes are unchanged from the verified draft.
**Planner/reviewer:** GPT-6.1. **Executor credit:** GLM 5.3, confirmed by the human for this commit.
**Base:** 47f5b0ab35f956355d9f9db63056906f610eb797 on
fix/phase1-safe-scalar-conversions (committed Phase 3B.1).

## Scope and rationale

upsert_adapter_value_compute still mallocs slot_size − 24 − key before the old
constructor runs: roughly 1 GiB for a roughly 2 KiB record at 256 bare varchars.
Changing the field composer alone cannot remove it. Add an optional owning
constructor beside the compatible old callback; reject both set together.
Only optional single-record transport plumbing changes. No storage.c producer changes.

This proposes a bounded prerequisite within 3B: private typed-row adapter →
owning transport hook → later single/bulk consumers. It is not approval inferred
from the earlier split. Bulk conversion also depends on legacy criteria/index
readers and deferred-buffer lifetimes; a callback-only patch cannot make those
routes packed-safe. There is no explicit-schema public bulk API, and mutating
a cache-owned schema to invent one in a test would add a lifecycle defect.

No loader/create-object binding, metadata/version, persisted format, cap,
migration, SDK/wire change, public activation or global selector. Extending
internal SlotcaskUpsertOpts requires rebuilding all linked targets; there is no
claim of binary compatibility for separately built internal-header callers.

## Execution rules

1. Obtain approval, continue the pinned feature branch, preserve earlier
   commits and leave all work unstaged/uncommitted. No branch/reset, commit,
   push, PR or merge. This plan does not authorize activation.
2. Read AGENTS.md/CORE-PROCESS.md. All shell commands use rtk. Build all targets
   with rtk proxy env SKIP_TESTS=1 ./build.sh; never run benchmarks.
3. Tasks 0–3 in order. Capture runtime RED before Task 2. Header-only optional
   fields make the test compile against unchanged storage implementation.
   A compiler failure is not RED. Never weaken assertions or invent edits.
4. Anchors match exactly once; block hashes/checkpoints bind literal stages.
   On mismatch, unexpected warnings/failure or uncovered decision, preserve
   state, write PLAN_NOTES.md and halt the entire run. Only human/planner
   amendment permits resume. Driver preflights a whole stage before writing.
5. Raw logs/scripts/probes/binaries stay in a unique /tmp directory. Reviewed
   source/tests/docs and concise evidence are the only commit candidates.
6. Protected docs/plans/2026-10-19-durability-certification-rig.md: hash only,
   never read/edit/move/stage/incorporate/execute. Required SHA256:
   fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.
7. No sanitizers here. Branch-series pre-push ASan+UBSan/TSan gates remain
   after all fixes, three full runs each (TSan --jobs 2). Normal full suite
   is mandatory after final code edits. No rerun-until-green.
8. Stop for plan-aware and fresh blind raw-diff review after verification.

## Task 0 — Documentation discovery and checkpoint

**How this serves the primary goal:** proves the current ownership/transaction
contracts before adding a constructor mode.

Read the actual sources; graph results are a map with stale line locations.

| Allowed API / pattern | Pinned source |
|---|---|
| Old buffer constructor; locked OLD; no reentry | slotcask.h:501–519 |
| Option/context/generation; zero-initialized callers | slotcask.h:559–610 |
| Adapter buffer survives transaction then free | slotcask.c:8541–8590,8652–8895 |
| Current segment maximum and padded actual size | slotcask.h:43,207–212; slotcask.c:4021–4051 |
| Actual owning typed update | typed_row.h:75–105; typed_row.c:468–504 |
| Explicit-schema physical fixtures | test_packed_record_storage.c:109–221; test_typed_row_adapter.c:10–30 |
| Correct precommit signature includes int is_update | slotcask.h:522–524 |
| Correct trim signature begins const void * | slotcask.h:254 |
| Process-local context initialization | test_runner.c and embedded.c:test_init_process_db |
| Scratch-only context setup/cleanup | embedded.c:shard_db_open_internal/shard_db_close (probe's local extern matches the definition) |
| Public CAS/computed/return_record contracts for later consumers | docs/query-protocol/cas.md:173–204 |
| Packing/deferred lifetimes | docs/concepts/storage-model.md; typed-row-adapter.md |

File names above are under src/db or src/test/cases as applicable.
Separate fact-gatherers inspected single and bulk writes; planner verified
signatures directly. One production old-hook assignment exists: storage.c:2311,
v2_update_new_from_old at :1862. No old signature/producer changes here. The
legacy composer still has single-update, three bulk and legacy adapter callers.

~~~sh
rtk proxy git rev-parse HEAD
rtk proxy git branch --show-current
rtk git status --short
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
rtk proxy sha256sum docs/plans/2026-10-25-phase3b2-owning-write-hook-plan.md
~~~

Match base/branch/protected hash. Only this plan, the Phase 3 roadmap index,
docs/plans/2026-10-25-phase3b2-blind-review.md and the protected untracked plan
may pre-exist as planning changes; no implementation diff. Record plan
hash and match human handoff if supplied. Establish local daemon socket
capability before full-suite execution; use the authorized capable mode for
EPERM restrictions, not a knowingly failing functional run.

Anti-pattern guards: no invented size callback, double constructor/default
generation, OLD fetched outside lock, cached-schema mutation, or assumed bulk
buffer owner. Source/consumer inventory is explicit below, not executor homework.

## Contract

| Case | Required result |
|---|---|
| Old callback only / no constructor | Existing behavior |
| Both set | -1/EINVAL before either callback/mutation |
| require-existing/check/absent OLD | Existing soft policy; no constructor call |
| Owning success | Once; actual final allocated bytes, non-NULL even for length zero |
| Failure with partial allocation | Free once; -1 hard, preserve errno or EINVAL fallback |
| NULL success output | -1/EINVAL |
| uint32/slot/segment limit incl actual key/header/8-byte padding | -1/EFBIG before reservation/precommit |
| Legacy trimmer installed | Neither supplied input nor owning output is trimmed |
| Transaction/hook completion or rejection | Accepted buffer stays live through hooks, then freed once |
| Generation | opts.record_hdr2; existing transport validation |

The callback must not alias OLD or caller/schema storage. Generic transport
checks physical fit/ownership, not schema semantics; producer owns semantic
validation. Supplied value/vlen are ignored; output is final bytes. A future
legacy owning producer must trim itself. Existing check is soft; existing
precommit refusal remains a hard storage failure. This hook needs OLD and
does not generate new-key defaults.

## Task 1 — Header contract and runtime RED

**How this serves the primary goal:** demonstrates missing dispatch and lost
actual bytes before storage implementation changes.

~~~sh
P3B2_TMP=$(rtk proxy mktemp -d /tmp/p3b2-exec-XXXXXX)
rtk proxy python3 - docs/plans/2026-10-25-phase3b2-owning-write-hook-plan.md "$P3B2_TMP/apply.py" <<'PY'
import re,sys
from pathlib import Path
b=dict(re.findall(r"(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)",Path(sys.argv[1]).read_text()))
Path(sys.argv[2]).write_text(b["driver"])
PY
rtk proxy python3 "$P3B2_TMP/apply.py" docs/plans/2026-10-25-phase3b2-owning-write-hook-plan.md 1
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-owning-write-hooks
~~~

Capture raw outputs/exit codes under P3B2_TMP. Zero build warnings. Runtime RED
must show ignored owning hook, zero callback calls, lost/empty NEW and wrong
error/trim behavior. Existing legacy callback checks pass. Reference counts
in the ledger are measured, not an excuse to weaken tests or a fabricated
forecast; investigate any different failure mechanism.

Stage 1 applies e1/e2 header hunks, e3 test registration and creates the full
tests block. Old implementation is unchanged.

## Task 2 — Own actual NEW through the transaction

**How this serves the primary goal:** removes the schema-max allocation from
the optional path while preserving lock scope and abort behavior.

~~~sh
rtk proxy python3 "$P3B2_TMP/apply.py" docs/plans/2026-10-25-phase3b2-owning-write-hook-plan.md 2
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-owning-write-hooks
rtk proxy ./build/bin/shard-db-test run-all
~~~

e4 stores owning-constructor errno; e5 is a complete adapter replacement;
e6/e7 replace both complete wrappers. Preserve existing old-constructor
logic/cleanup. Either constructor must set value_rewrites_payload so the
Q wave cannot stage an empty placeholder before NEW is constructed.

Zero warnings, focused/full suite green. Tests prove exact numeric(26,10)
digits, real store/get/reopen lengths/generation, compact output under the
1 GiB theoretical schema, nonNULL empty value, hard/soft distinctions and
unchanged counts/OLD on failures. Reservation is checked via the stream cursor
under rotation_lock, not sparse file size. This is physical helper coverage,
not public packed command/index acceptance.

## Task 3 — Ownership/OOM probe, docs and evidence

**How this serves the primary goal:** tests cleanup and bounded allocations
independently of successful round trips.

~~~sh
rtk proxy python3 - docs/plans/2026-10-25-phase3b2-owning-write-hook-plan.md "$P3B2_TMP/probe.c" <<'PY'
import re,sys
from pathlib import Path
b=dict(re.findall(r"(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)",Path(sys.argv[1]).read_text()))
Path(sys.argv[2]).write_text(b["probe"])
PY
rtk proxy gcc -std=gnu11 -O1 -g -fno-lto -Wall -Wextra -Werror -Wno-unused-parameter -Wno-address-of-packed-member -Wno-format-truncation -Wno-unused-result -Isrc/db -c src/db/slotcask.c -o "$P3B2_TMP/slotcask-probe.o"
rtk proxy gcc -std=gnu11 -O1 -g -Wall -Wextra -Werror -Isrc/db "$P3B2_TMP/probe.c" "$P3B2_TMP/slotcask-probe.o" build/bin/libshard-db.a -Wl,--wrap=malloc,--wrap=calloc,--wrap=realloc,--wrap=free -pthread -lssl -lcrypto -lm -o "$P3B2_TMP/probe"
rtk proxy "$P3B2_TMP/probe"
rtk proxy python3 "$P3B2_TMP/apply.py" docs/plans/2026-10-25-phase3b2-owning-write-hook-plan.md 3
rtk proxy git diff --check
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
~~~

Compile slotcask separately without LTO to preserve allocation wrapping.
Do not disable LTO on the final link: the archive requires its plugin.
Ordinary cc only. The probe initializes a real ShardDb context before using
Slotcask, closes it afterward, and adds no production test seam.

All eight cases pass: success, partial-error output, injected malloc failure,
invalid NULL success, segment overflow, precommit refusal, errno-zero fallback,
uint32 overflow. Exactly one call and one free per allocated result, zero
frees when no result exists, live buffer inside precommit, OLD/generation
preserved, and no operation allocation over 8 MiB under a roughly 1 GiB
schema. This is a scoped allocation-bound test, not an engine-wide OOM sweep
or benchmark.

Stage 3 creates owning-write-hooks.md and links it from typed-row-adapter.md.
Write docs/plans/2026-10-25-phase3b2-owning-hook-evidence.md with concise actual
identity/commands/output/exit codes, RED/GREEN/full/probe results and protected
start/end hashes. Do not claim pending gates or commit raw logs/probe binaries.

Final census: owning-hook use only in typedef/options/wrappers/new tests;
no storage.c/query_bulk.c/loader/SDK production assignment. Expected code:
build.sh, slotcask.h, slotcask.c, new test; docs: two concepts and evidence.
Review tracked/untracked files and stop uncommitted.

## Remaining 3B obligations

| Owner | Required work not delivered here |
|---|---|
| Single command child | storage.c total-size inserts/updates; defaults/auto-create transfer under correct lock; return_record actual length/generation; real key/segment gates |
| Bulk child | BulkInsRecord vlen/hdr2; JSON/CSV arenas; AC recomposition; three update slab owners through deferred finish; per-row accounting |
| Readers/indexes | Legacy criteria/CAS/builders must not interpret packed descriptors; no cached-schema test mutation |
| Rebuild/closure | Actual-size rebuild/carry/backfill; fresh complete census; route/index/count/failure tests |
| 3C, 4–6 and activation | Metadata/publication/replay, write semantics, exact criteria/index/aggregates and application acceptance; explicit activation approval |

Bulk inventory: payloads query_bulk.c:1600/2355, worker fixed vlen/hdr2:1204,
AC:450, constructors:3396/4207/4826, count×total_size slabs:3682/4279/4902,
finish frees:3749/4332/4955. Criteria and per-row JSON CAS inspect legacy OLD;
index hooks do likewise. Existing arena, CSV heap-cell copy and shared-patch
collection have unchecked OOM paths; JSON insert handles only encoder -2.
These are required audit/fix points when that bulk child replaces those same
regions, not issues claimed solved by this transport seam.

## Appendix A — Immutable driver, checkpoints and exact quoted edits

Old/new blocks are complete hunks/functions. Driver validates checkpoints
(including absence of new files), block hashes and unique anchors before
writing any stage. Keep previously approved blocks literal.

<!-- block:driver -->
```python
#!/usr/bin/env python3
import hashlib,json,re,sys
from pathlib import Path
def digest(data):return hashlib.sha256(data).hexdigest()
def main():
    if len(sys.argv)!=3:raise ValueError("usage: apply.py PLAN STAGE")
    plan=Path(sys.argv[1]).read_text()
    pairs=re.findall(r"(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)",plan)
    blocks=dict(pairs)
    if len(blocks)!=len(pairs):raise ValueError("duplicate block IDs")
    for name,wanted in json.loads(blocks["checksums"]).items():
        if digest(blocks[name].encode())!=wanted:raise ValueError("block checksum mismatch: "+name)
    stage=int(sys.argv[2])
    if stage not in (1,2,3):raise ValueError("stage must be 1, 2 or 3")
    root=Path.cwd().resolve();checks=json.loads(blocks["checkpoints"])
    for rel,wanted in checks[str(stage-1)].items():
        path=root/rel
        if path.is_symlink():raise ValueError("symlink not allowed: "+rel)
        got=digest(path.read_bytes()) if path.exists() else None
        if got!=wanted:raise ValueError("checkpoint mismatch: "+rel)
    pending={}
    for e in json.loads(blocks["edits"]):
        if e["stage"]!=stage:continue
        rel=e["path"];path=root/rel
        if not path.resolve().is_relative_to(root):raise ValueError("path outside root")
        text=pending.get(rel,path.read_text());old,new=blocks[e["old"]],blocks[e["new"]]
        if text.count(old)!=1:raise ValueError("anchor not unique: "+e["old"]+" in "+rel)
        pending[rel]=text.replace(old,new)
    creates={1:("src/test/cases/test_owning_write_hooks.c","tests"),3:("docs/concepts/owning-write-hooks.md","hook-doc")}
    if stage in creates:
        rel,name=creates[stage]
        if (root/rel).exists():raise ValueError("new file already exists: "+rel)
        pending[rel]=blocks[name]
    for rel,text in pending.items():
        if digest(text.encode())!=checks[str(stage)][rel]:raise ValueError("planned result mismatch: "+rel)
    for rel,text in pending.items():(root/rel).write_text(text)
    print("stage",stage,":",len(pending),"files applied via unique anchors")
try:main()
except Exception as exc:
    note=Path("PLAN_NOTES.md")
    if not note.exists():note.write_text("Phase 3B.2 halted\n\n"+str(exc)+"\n")
    print("HALT:",exc,file=sys.stderr);sys.exit(1)

```

<!-- block:edits -->
```json
[
  {
    "stage": 1,
    "path": "src/db/slotcask.h",
    "old": "e1-old",
    "new": "e1-new"
  },
  {
    "stage": 1,
    "path": "src/db/slotcask.h",
    "old": "e2-old",
    "new": "e2-new"
  },
  {
    "stage": 1,
    "path": "build.sh",
    "old": "e3-old",
    "new": "e3-new"
  },
  {
    "stage": 2,
    "path": "src/db/slotcask.c",
    "old": "e4-old",
    "new": "e4-new"
  },
  {
    "stage": 2,
    "path": "src/db/slotcask.c",
    "old": "e5-old",
    "new": "e5-new"
  },
  {
    "stage": 2,
    "path": "src/db/slotcask.c",
    "old": "e6-old",
    "new": "e6-new"
  },
  {
    "stage": 2,
    "path": "src/db/slotcask.c",
    "old": "e7-old",
    "new": "e7-new"
  },
  {
    "stage": 3,
    "path": "docs/concepts/typed-row-adapter.md",
    "old": "e8-old",
    "new": "e8-new"
  }
]
```

<!-- block:checkpoints -->
```json
{
  "0": {
    "build.sh": "928e78fd8ab597ce1965d60e60f973e5d36605daac7aaf31a9500dac7039dc7b",
    "docs/concepts/owning-write-hooks.md": null,
    "docs/concepts/typed-row-adapter.md": "860838ce218b01a938a58dd85a18fd52e5e064437c184c0070559f9dde8b5da6",
    "src/db/slotcask.c": "3c87f3f18ad3f0471fb2b593ca1521048ef8d93aa7cd9cdda7bd5e02b80ab5b8",
    "src/db/slotcask.h": "2bb0a9d5a47b97b325c3152ce1096dd66e1fdecc47a650f2cb246a3c7a1496b8",
    "src/test/cases/test_owning_write_hooks.c": null
  },
  "1": {
    "build.sh": "28681cc82cb5c9e858b333708fd69637f4b5fc5198e25c5eff76af6972df95b3",
    "docs/concepts/owning-write-hooks.md": null,
    "docs/concepts/typed-row-adapter.md": "860838ce218b01a938a58dd85a18fd52e5e064437c184c0070559f9dde8b5da6",
    "src/db/slotcask.c": "3c87f3f18ad3f0471fb2b593ca1521048ef8d93aa7cd9cdda7bd5e02b80ab5b8",
    "src/db/slotcask.h": "2cf265d1fa9629a89e08670af2d6401bd3206cfc2b22fb91fb03f43683f128c6",
    "src/test/cases/test_owning_write_hooks.c": "8b156410510b5e2bc3f585091519410ed7083ae402f0bc003dfec020a0822201"
  },
  "2": {
    "build.sh": "28681cc82cb5c9e858b333708fd69637f4b5fc5198e25c5eff76af6972df95b3",
    "docs/concepts/owning-write-hooks.md": null,
    "docs/concepts/typed-row-adapter.md": "860838ce218b01a938a58dd85a18fd52e5e064437c184c0070559f9dde8b5da6",
    "src/db/slotcask.c": "6836a15715f4ac46808d4acb718f30b14dadde8943242825dbb18270a236fbd0",
    "src/db/slotcask.h": "2cf265d1fa9629a89e08670af2d6401bd3206cfc2b22fb91fb03f43683f128c6",
    "src/test/cases/test_owning_write_hooks.c": "8b156410510b5e2bc3f585091519410ed7083ae402f0bc003dfec020a0822201"
  },
  "3": {
    "build.sh": "28681cc82cb5c9e858b333708fd69637f4b5fc5198e25c5eff76af6972df95b3",
    "docs/concepts/owning-write-hooks.md": "d0c71eef5de383c0fe6bf1e591645b770401dd75bb513e8189a61d4e975678c4",
    "docs/concepts/typed-row-adapter.md": "fb551e31a3b59cda085db556556e4da7fc6cdc32d72c8527e07e3ab04a15cd64",
    "src/db/slotcask.c": "6836a15715f4ac46808d4acb718f30b14dadde8943242825dbb18270a236fbd0",
    "src/db/slotcask.h": "2cf265d1fa9629a89e08670af2d6401bd3206cfc2b22fb91fb03f43683f128c6",
    "src/test/cases/test_owning_write_hooks.c": "8b156410510b5e2bc3f585091519410ed7083ae402f0bc003dfec020a0822201"
  }
}
```

### Edit 1: stage 1, src/db/slotcask.h

Old:

<!-- block:e1-old -->
```text
/* Return 0 to commit; non-zero to abort. NULL = always commit. */
typedef int (*slotcask_pre_commit_fn)
```

New:

<!-- block:e1-new -->
```text
/* Optional owning alternative to new_from_old. The callback executes once
   against lock-protected OLD, after the existing condition gates and before
   segment reservation. It must not perform network I/O or database reentry.
   Slotcask initializes *out_value=NULL and *out_vlen=0. On success return 0
   with a non-NULL malloc/free-compatible buffer, including a zero-length value.
   On every return Slotcask owns any non-NULL *out_value: it frees rejected
   partial output and retains accepted output until the transaction and all
   hooks finish. Output must not alias OLD, schema storage or caller input.
   The buffer contains final actual bytes: neither input nor output is passed
   to trim_fn in this mode. The supplied value/vlen are ignored. Generation
   remains opts.record_hdr2; a constructor cannot change it.
   Nonzero return is a hard error, not condition_not_met. Set errno for a
   specific cause; zero errno falls back to EINVAL. Invalid success output is
   EINVAL; physical slot/uint32/segment overflow is EFBIG, checked before write.
   Missing OLD and condition rejections do not invoke the constructor.
   new_from_old and new_from_old_alloc are mutually exclusive.
   This internal hook does not activate packed schemas or convert consumers. */
typedef int (*slotcask_new_from_old_alloc_fn)(const SlotcaskOldRecord *old,
                                             uint8_t **out_value,
                                             size_t *out_vlen, void *ctx);

/* Return 0 to commit; non-zero to abort. NULL = always commit. */
typedef int (*slotcask_pre_commit_fn)
```

### Edit 2: stage 1, src/db/slotcask.h

Old:

<!-- block:e2-old -->
```text
    slotcask_new_from_old_fn  new_from_old;
    void                     *new_from_old_ctx;
```

New:

<!-- block:e2-new -->
```text
    slotcask_new_from_old_fn  new_from_old;
    void                     *new_from_old_ctx;
    /* Owning actual-sized alternative; see slotcask_new_from_old_alloc_fn.
       Existing callers leave both fields NULL. No persisted layout change. */
    slotcask_new_from_old_alloc_fn new_from_old_alloc;
    void                         *new_from_old_alloc_ctx;
```

### Edit 3: stage 1, build.sh

Old:

<!-- block:e3-old -->
```text
    src/test/cases/test_typed_row_adapter.c \

```

New:

<!-- block:e3-new -->
```text
    src/test/cases/test_typed_row_adapter.c \
    src/test/cases/test_owning_write_hooks.c \

```

### Edit 4: stage 2, src/db/slotcask.c

Old:

<!-- block:e4-old -->
```text
    int hard_error;
} UpsertAdapterCtx;
```

New:

<!-- block:e4-new -->
```text
    int hard_error;
    /* Only owning-constructor failures pin errno across transaction cleanup. */
    int owned_errno;
} UpsertAdapterCtx;
```

### Edit 5: stage 2, src/db/slotcask.c

Old:

<!-- block:e5-old -->
```text
static int upsert_adapter_value_compute(const SlotcaskOldRecord *old,
                                        SlotcaskBulkRec *rec) {
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)rec->user_ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;

    if (uo->check && uo->check(old, uo->check_ctx) == 0) return -1;
    if (!uo->new_from_old) return 0;
    if (!old) return -1;               /* new_from_old requires an existing record */

    size_t out_capacity = (size_t)actx->db->slot_size - 24 - rec->klen;
    uint8_t *buf = malloc(out_capacity ? out_capacity : 1);
    if (!buf) return -1;
    size_t out_vlen = 0;
    if (uo->new_from_old(old, buf, out_capacity, &out_vlen,
                         uo->new_from_old_ctx) != 0 ||
        out_vlen > out_capacity) {
        free(buf);
        actx->hard_error = 1;
        return -1;
    }
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&actx->db->trim_fn,
                                                  memory_order_acquire);
    if (trim_fn) out_vlen = trim_fn(buf, out_vlen, actx->db->trim_ctx);
    if ((size_t)24 + rec->klen + out_vlen > (size_t)actx->db->slot_size) {
        free(buf);
        actx->hard_error = 1;
        return -1;
    }
    actx->callback_buf = buf;
    rec->value = buf;
    rec->vlen = out_vlen;
    return 0;
}
```

New:

<!-- block:e5-new -->
```text
static int upsert_adapter_value_compute(const SlotcaskOldRecord *old,
                                        SlotcaskBulkRec *rec) {
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)rec->user_ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;

    if (uo->check && uo->check(old, uo->check_ctx) == 0) return -1;
    if (!uo->new_from_old && !uo->new_from_old_alloc) return 0;
    if (!old) return -1;               /* both constructors require OLD */

    if (uo->new_from_old_alloc) {
        uint8_t *buf = NULL;
        size_t out_vlen = 0;
        errno = 0;
        int rc = uo->new_from_old_alloc(old, &buf, &out_vlen,
                                         uo->new_from_old_alloc_ctx);
        int cause = 0;
        if (rc != 0) cause = errno ? errno : EINVAL;
        else if (!buf) cause = EINVAL;
        else if (out_vlen > UINT32_MAX ||
                 out_vlen > SIZE_MAX - 24 - rec->klen - 7) cause = EFBIG;
        else {
            size_t physical = slotcask_record_size_varlen(rec->klen, out_vlen);
            if (actx->db->slot_size < 0 ||
                physical > (size_t)actx->db->slot_size ||
                physical > slotcask_seg_max_bytes()) cause = EFBIG;
        }
        if (cause) {
            free(buf);
            actx->hard_error = 1;
            actx->owned_errno = cause;
            errno = cause;
            return -1;
        }
        actx->callback_buf = buf;
        rec->value = buf;
        rec->vlen = out_vlen;
        return 0;
    }

    size_t out_capacity = (size_t)actx->db->slot_size - 24 - rec->klen;
    uint8_t *buf = malloc(out_capacity ? out_capacity : 1);
    if (!buf) return -1;
    size_t out_vlen = 0;
    if (uo->new_from_old(old, buf, out_capacity, &out_vlen,
                         uo->new_from_old_ctx) != 0 ||
        out_vlen > out_capacity) {
        free(buf);
        actx->hard_error = 1;
        return -1;
    }
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&actx->db->trim_fn,
                                                  memory_order_acquire);
    if (trim_fn) out_vlen = trim_fn(buf, out_vlen, actx->db->trim_ctx);
    if ((size_t)24 + rec->klen + out_vlen > (size_t)actx->db->slot_size) {
        free(buf);
        actx->hard_error = 1;
        return -1;
    }
    actx->callback_buf = buf;
    rec->value = buf;
    rec->vlen = out_vlen;
    return 0;
}
```

### Edit 6: stage 2, src/db/slotcask.c

Old:

<!-- block:e6-old -->
```text
int slotcask_upsert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result) {
    if (result) {
        result->was_update = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
        result->current_hdr2 = 0;
        result->current_hdr2 = 0;
    }
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;

    /* Trim value to field boundary for compact varlen storage. */
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn)
        vlen = trim_fn(value, vlen, db->trim_ctx);
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;

    /* Fail loud on a missed two-phase migration: an indexed object with a
       legacy pre_commit alongside only one half of the prepare/apply pair
       (or with prepare/apply set but the other missing) would silently run
       with an incomplete hook set. This applies to require_existing=1
       (update-only) callers too — an update-resolved record's durable
       index write is just as pre-marker if it lives in a bare pre_commit
       instead of being staged there and applied post-marker by
       prepare/apply. */
    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* kf_shard is a pure hash-routing constant — known before the
       transaction starts, unlike kf_slot which segment reservation only
       resolves mid-transaction. Publish it now so any caller hook that
       reads it via a ctx struct (e.g. pre_commit, which fires before slot
       resolution) sees the real value instead of a stale/zero default. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    UpsertAdapterCtx actx = { .uo = opts, .db = db, .callback_buf = NULL };
    SlotcaskBulkOpts bopts = {0};
    bopts.if_not_exists = opts->if_not_exists;
    bopts.require_existing = opts->require_existing;
    bopts.pre_commit = upsert_adapter_pre_commit;
    /* Single-record call: always fetch OLD when the key is found, so
       check/pre_commit/new_from_old (any of which may dereference it) always
       see it — the batch-amortised skip this flag otherwise enables doesn't
       apply to a window_cap=1 transaction. */
    bopts.pre_commit_needs_old = 1;
    bopts.value_compute = (opts->check || opts->new_from_old)
                          ? upsert_adapter_value_compute : NULL;
    bopts.value_rewrites_payload = opts->new_from_old != NULL;
    bopts.abort_window = upsert_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;
    /* prepare_window/apply_window are only meaningful together (two-phase);
       the coordinator only runs the window-hook path when both are
       non-NULL. Applies to both fresh-insert and update-resolved records —
       see the doc comment on upsert_adapter_prepare_window/apply_window. */
    if (opts->prepare_commit && opts->apply_commit) {
        bopts.prepare_window = upsert_adapter_prepare_window;
        bopts.apply_window = upsert_adapter_apply_window;
    }

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen; rec.value = value; rec.vlen = vlen;
    rec.hdr2 = opts ? opts->record_hdr2 : 0;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.upsert_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    free(actx.callback_buf);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) {
        /* Hard failure: kf_acquire, staging I/O, or a post-marker window
           that didn't converge synchronously (coordinator already set
           errno=EINPROGRESS for that case). */
        return -1;
    }
    if (rec.status == -2 && !actx.hard_error) {
        if (result) {
            result->was_update = rec.was_update;
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            uint8_t cv_hdr2 = 0;
            if (slotcask_get_ex(db, key, klen, &cv, &cvl, &cv_hdr2) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
                result->current_hdr2 = cv_hdr2;
            }
        }
        return -2;
    }
    if (rec.status != 0) return -1;

    if (result) {
        result->was_update = rec.was_update;
        result->condition_not_met = 0;
    }
    return 0;
}
```

New:

<!-- block:e6-new -->
```text
int slotcask_upsert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result) {
    if (result) {
        result->was_update = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
        result->current_hdr2 = 0;
        result->current_hdr2 = 0;
    }
    if (opts && opts->new_from_old_alloc) {
        value = NULL;
        vlen = 0;
    }
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;

    /* Trim value to field boundary for compact varlen storage. */
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn && !(opts && opts->new_from_old_alloc))
        vlen = trim_fn(value, vlen, db->trim_ctx);
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;
    if (opts->new_from_old && opts->new_from_old_alloc) {
        errno = EINVAL;
        return -1;
    }

    /* Fail loud on a missed two-phase migration: an indexed object with a
       legacy pre_commit alongside only one half of the prepare/apply pair
       (or with prepare/apply set but the other missing) would silently run
       with an incomplete hook set. This applies to require_existing=1
       (update-only) callers too — an update-resolved record's durable
       index write is just as pre-marker if it lives in a bare pre_commit
       instead of being staged there and applied post-marker by
       prepare/apply. */
    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* kf_shard is a pure hash-routing constant — known before the
       transaction starts, unlike kf_slot which segment reservation only
       resolves mid-transaction. Publish it now so any caller hook that
       reads it via a ctx struct (e.g. pre_commit, which fires before slot
       resolution) sees the real value instead of a stale/zero default. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    UpsertAdapterCtx actx = { .uo = opts, .db = db, .callback_buf = NULL };
    SlotcaskBulkOpts bopts = {0};
    bopts.if_not_exists = opts->if_not_exists;
    bopts.require_existing = opts->require_existing;
    bopts.pre_commit = upsert_adapter_pre_commit;
    /* Single-record call: always fetch OLD when the key is found, so
       check/pre_commit/new_from_old (any of which may dereference it) always
       see it — the batch-amortised skip this flag otherwise enables doesn't
       apply to a window_cap=1 transaction. */
    bopts.pre_commit_needs_old = 1;
    bopts.value_compute = (opts->check || opts->new_from_old || opts->new_from_old_alloc)
                          ? upsert_adapter_value_compute : NULL;
    bopts.value_rewrites_payload = opts->new_from_old != NULL ||
                                   opts->new_from_old_alloc != NULL;
    bopts.abort_window = upsert_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;
    /* prepare_window/apply_window are only meaningful together (two-phase);
       the coordinator only runs the window-hook path when both are
       non-NULL. Applies to both fresh-insert and update-resolved records —
       see the doc comment on upsert_adapter_prepare_window/apply_window. */
    if (opts->prepare_commit && opts->apply_commit) {
        bopts.prepare_window = upsert_adapter_prepare_window;
        bopts.apply_window = upsert_adapter_apply_window;
    }

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen; rec.value = value; rec.vlen = vlen;
    rec.hdr2 = opts ? opts->record_hdr2 : 0;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.upsert_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    free(actx.callback_buf);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) {
        /* Hard failure: kf_acquire, staging I/O, or a post-marker window
           that didn't converge synchronously (coordinator already set
           errno=EINPROGRESS for that case). */
        return -1;
    }
    if (rec.status == -2 && !actx.hard_error) {
        if (result) {
            result->was_update = rec.was_update;
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            uint8_t cv_hdr2 = 0;
            if (slotcask_get_ex(db, key, klen, &cv, &cvl, &cv_hdr2) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
                result->current_hdr2 = cv_hdr2;
            }
        }
        return -2;
    }
    if (rec.status != 0) {
        if (actx.owned_errno) errno = actx.owned_errno;
        return -1;
    }

    if (result) {
        result->was_update = rec.was_update;
        result->condition_not_met = 0;
    }
    return 0;
}
```

### Edit 7: stage 2, src/db/slotcask.c

Old:

<!-- block:e7-old -->
```text
int slotcask_insert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result) {
    if (result) {
        result->was_update = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
        result->current_hdr2 = 0;
        result->current_hdr2 = 0;
    }
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;

    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn)
        vlen = trim_fn(value, vlen, db->trim_ctx);
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;

    /* require_existing is incompatible with INSERT-only semantics; the
       caller should route through slotcask_upsert_with_hooks for that. */
    if (opts->require_existing) return -1;

    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* See slotcask_upsert_with_hooks's identical comment: kf_shard is known
       before the transaction starts, so publish it now for hooks (e.g.
       pre_commit) that fire before slot resolution. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    UpsertAdapterCtx actx = { .uo = opts, .db = db, .callback_buf = NULL };
    SlotcaskBulkOpts bopts = {0};
    /* Insert-only: always reject an existing key regardless of what the
       caller passed — the one contract difference from the upsert path. */
    bopts.if_not_exists = 1;
    bopts.pre_commit = upsert_adapter_pre_commit;
    bopts.pre_commit_needs_old = 1;
    bopts.value_compute = (opts->check || opts->new_from_old)
                          ? upsert_adapter_value_compute : NULL;
    bopts.value_rewrites_payload = opts->new_from_old != NULL;
    bopts.abort_window = upsert_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;
    if (opts->prepare_commit && opts->apply_commit) {
        bopts.prepare_window = upsert_adapter_prepare_window;
        bopts.apply_window = upsert_adapter_apply_window;
    }

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen; rec.value = value; rec.vlen = vlen;
    rec.hdr2 = opts ? opts->record_hdr2 : 0;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.upsert_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    free(actx.callback_buf);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) {
        /* Hard failure: kf_acquire, staging I/O, or a post-marker window
           that didn't converge synchronously (coordinator already set
           errno=EINPROGRESS for that case). */
        return -1;
    }
    if (rec.status == -2 && !actx.hard_error) {
        if (result) {
            result->was_update = rec.was_update;
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            uint8_t cv_hdr2 = 0;
            if (slotcask_get_ex(db, key, klen, &cv, &cvl, &cv_hdr2) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
                result->current_hdr2 = cv_hdr2;
            }
        }
        return -2;
    }
    if (rec.status != 0) return -1;

    if (result) {
        result->was_update = rec.was_update;
        result->condition_not_met = 0;
    }
    return 0;
}
```

New:

<!-- block:e7-new -->
```text
int slotcask_insert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result) {
    if (result) {
        result->was_update = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
        result->current_hdr2 = 0;
        result->current_hdr2 = 0;
    }
    if (opts && opts->new_from_old_alloc) {
        value = NULL;
        vlen = 0;
    }
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;

    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn && !(opts && opts->new_from_old_alloc))
        vlen = trim_fn(value, vlen, db->trim_ctx);
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;
    if (opts->new_from_old && opts->new_from_old_alloc) {
        errno = EINVAL;
        return -1;
    }

    /* require_existing is incompatible with INSERT-only semantics; the
       caller should route through slotcask_upsert_with_hooks for that. */
    if (opts->require_existing) return -1;

    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* See slotcask_upsert_with_hooks's identical comment: kf_shard is known
       before the transaction starts, so publish it now for hooks (e.g.
       pre_commit) that fire before slot resolution. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    UpsertAdapterCtx actx = { .uo = opts, .db = db, .callback_buf = NULL };
    SlotcaskBulkOpts bopts = {0};
    /* Insert-only: always reject an existing key regardless of what the
       caller passed — the one contract difference from the upsert path. */
    bopts.if_not_exists = 1;
    bopts.pre_commit = upsert_adapter_pre_commit;
    bopts.pre_commit_needs_old = 1;
    bopts.value_compute = (opts->check || opts->new_from_old || opts->new_from_old_alloc)
                          ? upsert_adapter_value_compute : NULL;
    bopts.value_rewrites_payload = opts->new_from_old != NULL ||
                                   opts->new_from_old_alloc != NULL;
    bopts.abort_window = upsert_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;
    if (opts->prepare_commit && opts->apply_commit) {
        bopts.prepare_window = upsert_adapter_prepare_window;
        bopts.apply_window = upsert_adapter_apply_window;
    }

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen; rec.value = value; rec.vlen = vlen;
    rec.hdr2 = opts ? opts->record_hdr2 : 0;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.upsert_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    free(actx.callback_buf);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) {
        /* Hard failure: kf_acquire, staging I/O, or a post-marker window
           that didn't converge synchronously (coordinator already set
           errno=EINPROGRESS for that case). */
        return -1;
    }
    if (rec.status == -2 && !actx.hard_error) {
        if (result) {
            result->was_update = rec.was_update;
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            uint8_t cv_hdr2 = 0;
            if (slotcask_get_ex(db, key, klen, &cv, &cvl, &cv_hdr2) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
                result->current_hdr2 = cv_hdr2;
            }
        }
        return -2;
    }
    if (rec.status != 0) {
        if (actx.owned_errno) errno = actx.owned_errno;
        return -1;
    }

    if (result) {
        result->was_update = rec.was_update;
        result->condition_not_met = 0;
    }
    return 0;
}
```

### Edit 8: stage 3, docs/concepts/typed-row-adapter.md

Old:

<!-- block:e8-old -->
```text
preserves schema bytes and ENOMEM on temporary enum-parser allocation failure.

```

New:

<!-- block:e8-new -->
```text
preserves schema bytes and ENOMEM on temporary enum-parser allocation failure.

The [owning write hook](owning-write-hooks.md) supplies a transport-level
actual-size NEW buffer lifetime for future integrations. It is optional and
has no production caller yet. Its presence does not convert public commands,
bulk row ownership, return_record, indexes or criteria to packed storage.

```

## Appendix B — Complete regression case

<!-- block:tests -->
```c
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "typed_row.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

typedef struct {
    TypedSchema *ts;
    SlotcaskDb db;
    char root[64];
    int opened;
} OwnFixture;
typedef struct {
    TypedSchema *ts;
    TypedInput inputs[MAX_FIELDS];
    int mode, calls, pre_calls, reject_pre, check_calls, allow;
    size_t fake_len, produced;
    const uint8_t *pending;
    int observed;
} OwnCtx;
static int own_remove(const char *p,const struct stat *st,int kind,struct FTW *f){
    (void)st;(void)kind;(void)f;return remove(p);
}
static void own_close(OwnFixture *f){
    if(f->opened)slotcask_close(&f->db);
    if(f->ts){for(int i=0;i<f->ts->nfields;i++)free_enum_values(&f->ts->fields[i]);free(f->ts);}
    slotcask_shutdown();
    if(f->root[0])ASSERT_EQ_INT(nftw(f->root,own_remove,16,FTW_DEPTH|FTW_PHYS),0,"own fixture cleanup");
}
static int own_open(OwnFixture *f,int maximum){
    memset(f,0,sizeof(*f));
    f->ts=calloc(1,sizeof(*f->ts));ASSERT_NOT_NULL(f->ts,"private schema");if(!f->ts)return -1;
    f->ts->typed=1;f->ts->nfields=maximum?MAX_FIELDS:2;
    const char *tokens[MAX_FIELDS];
    for(int i=0;i<f->ts->nfields;i++){
        tokens[i]=maximum||i?"varchar":"numeric:26,10";
        f->ts->fields[i].type=maximum||i?FT_VARCHAR:FT_NUMERIC;
        snprintf(f->ts->fields[i].name,sizeof(f->ts->fields[i].name),"f%d",i);
        f->ts->fields[i].name_len=(int)strlen(f->ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(f->ts,tokens,(size_t)f->ts->nfields,NULL,0);
    ASSERT_EQ_INT(rc,0,"bind explicit schema");if(rc)return -1;
    size_t bound=0;dec_status st=pkr_slot_bound(&f->ts->packed,1024,&bound);
    ASSERT_EQ_INT(st,DEC_OK,"slot bound");if(st!=DEC_OK)return -1;
    strcpy(f->root,"/tmp/shard-own-hook-XXXXXX");
    char *root=mkdtemp(f->root);ASSERT_NOT_NULL(root,"isolated owning object");if(!root){f->root[0]=0;return -1;}
    slotcask_init(64,64);
    rc=slotcask_open(&f->db,f->root,1,1,(int)bound);f->opened=rc==0;
    ASSERT_EQ_INT(rc,0,"open owning object");return rc;
}
static int own_seed(OwnFixture *f,const char *key){
    uint8_t *b=NULL;size_t n=0;char err[256];
    const char *json=f->ts->nfields==2?"{\"f0\":0,\"f1\":\"old\"}":"{\"f127\":\"tiny\"}";
    int rc=typed_encode_alloc(f->ts,json,1,NULL,NULL,&b,&n,err,sizeof(err));
    ASSERT_EQ_INT(rc,0,"actual seed encoding");if(rc)return -1;
    SlotcaskUpsertOpts o={0};o.record_hdr2=PKR_FLAGS;
    rc=slotcask_upsert_with_hooks(&f->db,-1,key,strlen(key),b,n,&o,NULL);
    ASSERT_EQ_INT(rc,0,"seed physical record");free(b);return rc;
}
static int own_make(const SlotcaskOldRecord *old,uint8_t **out,size_t *n,void *ptr){
    OwnCtx *c=ptr;c->calls++;
    if(c->mode==1){*out=malloc(1);*n=1;errno=EINVAL;return -1;}
    if(c->mode==2){errno=ENOMEM;return -1;}
    if(c->mode==3)return 0;
    if(c->mode==4){*out=malloc(1);*n=c->fake_len;return *out?0:-1;}
    if(c->mode==5){*out=malloc(1);*n=0;return *out?0:-1;}
    char err[256];
    int rc=typed_update_alloc(c->ts,old,c->inputs,0,out,n,err,sizeof(err));
    if(!rc){c->pending=*out;c->produced=*n;}
    return rc;
}
static int own_legacy(const SlotcaskOldRecord *old,uint8_t *out,size_t cap,size_t *n,void *ptr){
    OwnCtx *c=ptr;c->calls++;
    if(!old||old->vlen>cap)return -1;
    if(old->vlen)memcpy(out,old->value,old->vlen);
    *n=old->vlen;return 0;
}
static int own_check(const SlotcaskOldRecord *old,void *ptr){
    OwnCtx *c=ptr;c->check_calls++;return old&&c->allow;
}
static int own_pre(const SlotcaskOldRecord *old,const uint8_t *b,size_t n,int was_update,void *ptr){
    (void)was_update;OwnCtx *c=ptr;c->pre_calls++;
    if(old&&b&&b==c->pending&&n==c->produced)c->observed=1;
    return c->reject_pre;
}
static size_t own_trim(const void *b,size_t n,void *ptr){
    int *calls=ptr;(*calls)++;(void)b;return n;
}
static SlotcaskUpsertOpts own_opts(OwnCtx *c){
    SlotcaskUpsertOpts o={0};o.require_existing=1;o.record_hdr2=PKR_FLAGS;
    o.new_from_old_alloc=own_make;o.new_from_old_alloc_ctx=c;return o;
}
static void own_result_free(SlotcaskUpsertResult *r){free(r->current_value);}
static void own_assert_seed(OwnFixture *f,const char *key){
    void *b=NULL;size_t n=0;uint8_t h=0;
    ASSERT_EQ_INT(slotcask_get_ex(&f->db,key,strlen(key),&b,&n,&h),0,"OLD remains readable");
    ASSERT_EQ_INT(h,PKR_FLAGS,"OLD generation preserved");
    if(b){
        size_t len=0;char *s=typed_packed_field_text(f->ts,b,n,h,f->ts->nfields==2?1:127,&len);
        ASSERT_NOT_NULL(s,"OLD field text");
        if(s){ASSERT_EQ_STR(s,f->ts->nfields==2?"old":"tiny","OLD preserved on rejection");free(s);}
        free(b);
    }
}
static uint64_t own_segment_size(OwnFixture *f){
    SlotcaskStream *p=&f->db.streams[0];
    pthread_mutex_lock(&p->rotation_lock);
    uint64_t n=((uint64_t)p->active_file_id<<32)|p->reserve_off;
    pthread_mutex_unlock(&p->rotation_lock);return n;
}
static void own_success(void){
    OwnFixture f;if(own_open(&f,0)){own_close(&f);return;}
    if(own_seed(&f,"a")){own_close(&f);return;}
    OwnCtx c={0};c.ts=f.ts;
    c.inputs[0]=(TypedInput){"1234567890123456.1234567890",27,1,0,0};
    c.inputs[1]=(TypedInput){"new",3,1,0,0};
    SlotcaskUpsertOpts o=own_opts(&c);o.pre_commit=own_pre;o.pre_commit_ctx=&c;
    int trims=0;f.db.trim_ctx=&trims;atomic_store_explicit(&f.db.trim_fn,own_trim,memory_order_release);
    const char ignored[]="ignored";
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,ignored,sizeof(ignored),&o,NULL),0,"owned update");
    ASSERT_EQ_INT(c.calls,1,"constructor exactly once");ASSERT_EQ_INT(trims,0,"owned bytes bypass legacy trimmer");
    ASSERT_EQ_INT(c.pre_calls,1,"precommit once");ASSERT_EQ_INT(c.observed,1,"owned buffer alive through hook");
    atomic_store_explicit(&f.db.trim_fn,NULL,memory_order_release);
    void *b=NULL;size_t n=0;uint8_t h=0;
    ASSERT_EQ_INT(slotcask_get_ex(&f.db,"a",1,&b,&n,&h),0,"owned result get");
    ASSERT_EQ_INT(n,c.produced,"stored actual result size");ASSERT_EQ_INT(h,PKR_FLAGS,"owned generation");
    if(b){
        char *s=typed_packed_decode(f.ts,b,n,h,1024);
        ASSERT_NOT_NULL(s,"owned result render");
        if(s){ASSERT_CONTAINS(s,"1234567890123456.1234567890","exact finance digits");ASSERT_CONTAINS(s,"\"f1\":\"new\"","new varchar");free(s);}
        free(b);
    }
    int slot_size=f.db.slot_size;slotcask_close(&f.db);f.opened=0;
    int rc=slotcask_open(&f.db,f.root,1,1,slot_size);f.opened=rc==0;
    ASSERT_EQ_INT(rc,0,"owning record reopens");
    if(!rc){b=NULL;n=0;h=0;ASSERT_EQ_INT(slotcask_get_ex(&f.db,"a",1,&b,&n,&h),0,"reopened owned value");
        ASSERT_EQ_INT(h,PKR_FLAGS,"reopened generation");ASSERT_EQ_INT(n,c.produced,"reopened actual bytes");free(b);}
    own_close(&f);
}
static void own_rejections(void){
    OwnFixture f;if(own_open(&f,0)){own_close(&f);return;}
    if(own_seed(&f,"a")){own_close(&f);return;}
    for(int mode=1;mode<=4;mode++){
        if(own_seed(&f,"a"))break;
        OwnCtx c={0};c.ts=f.ts;c.mode=mode;c.fake_len=(size_t)f.db.slot_size;
        SlotcaskUpsertOpts o=own_opts(&c);o.pre_commit=own_pre;o.pre_commit_ctx=&c;
        uint64_t total=0,deleted=0,after_total=0,after_deleted=0;
        ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&total,&deleted),0,"before rejection counts");
        uint64_t before=own_segment_size(&f);SlotcaskUpsertResult r={0};errno=0;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,&r),-1,"owned constructor rejection is hard");
        ASSERT_EQ_INT(errno,mode==2?ENOMEM:mode==4?EFBIG:EINVAL,"constructor failure errno");
        ASSERT_EQ_INT(r.condition_not_met,0,"hard failure is not CAS failure");
        own_result_free(&r);ASSERT_EQ_INT(c.calls,1,"rejecting constructor once");
        ASSERT_EQ_INT(c.pre_calls,0,"reject before precommit");
        ASSERT_EQ_INT(own_segment_size(&f),before,"no segment growth on constructor rejection");
        ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&after_total,&after_deleted),0,"after rejection counts");
        ASSERT_TRUE(total==after_total&&deleted==after_deleted,"counts unchanged on constructor failure");
        own_assert_seed(&f,"a");
    }
    OwnCtx c={0};c.ts=f.ts;SlotcaskUpsertOpts o=own_opts(&c);
    o.check=own_check;o.check_ctx=&c;c.allow=0;SlotcaskUpsertResult r={0};
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,&r),-2,"check rejects softly");
    ASSERT_EQ_INT(c.check_calls,1,"check ran once");ASSERT_EQ_INT(c.calls,0,"check rejection precedes allocation");own_result_free(&r);
    o.check=NULL;o.require_existing=1;memset(&r,0,sizeof(r));
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"missing",7,NULL,0,&o,&r),-2,"missing OLD soft rejection");
    ASSERT_EQ_INT(c.calls,0,"missing OLD does not allocate");own_result_free(&r);
    o.require_existing=0;memset(&r,0,sizeof(r));
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&f.db,-1,"missing",7,NULL,0,&o,&r),-2,"insert-only constructor needs OLD");
    ASSERT_EQ_INT(c.calls,0,"fresh insert does not construct from absent OLD");own_result_free(&r);
    ASSERT_EQ_INT(slotcask_exists(&f.db,"missing",7),0,"absent key remains absent");
    o.new_from_old=own_legacy;o.new_from_old_ctx=&c;errno=0;
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,NULL),-1,"two constructors reject upsert");
    ASSERT_EQ_INT(errno,EINVAL,"upsert conflicting options errno");errno=0;
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&f.db,-1,"z",1,NULL,0,&o,NULL),-1,"two constructors reject insert");
    ASSERT_EQ_INT(errno,EINVAL,"insert conflicting options errno");ASSERT_EQ_INT(c.calls,0,"conflict invokes neither constructor");
    own_assert_seed(&f,"a");
    c.mode=0;c.reject_pre=1;c.inputs[1]=(TypedInput){"reject",6,1,0,0};
    o=own_opts(&c);o.pre_commit=own_pre;o.pre_commit_ctx=&c;memset(&r,0,sizeof(r));
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,&r),-1,"precommit refusal remains hard");
    ASSERT_EQ_INT(c.observed,1,"partial window sees live owned buffer");own_result_free(&r);own_assert_seed(&f,"a");
    own_close(&f);
}
static void own_maximum(void){
    OwnFixture f;if(own_open(&f,1)){own_close(&f);return;}
    ASSERT_TRUE(f.db.slot_size>1000000000,"theoretical slot exceeds 1GB");
    if(own_seed(&f,"a")){own_close(&f);return;}
    OwnCtx c={0};c.ts=f.ts;c.inputs[127]=(TypedInput){"longer",6,1,0,0};
    SlotcaskUpsertOpts o=own_opts(&c);
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,NULL),0,"small owned NEW in maximum schema");
    ASSERT_EQ_INT(c.calls,1,"maximum-schema constructor once");ASSERT_TRUE(c.produced<4096,"actual NEW stays compact");
    void *b=NULL;size_t n=0;uint8_t h=0;
    ASSERT_EQ_INT(slotcask_get_ex(&f.db,"a",1,&b,&n,&h),0,"maximum-schema read");ASSERT_EQ_INT(n,c.produced,"maximum-schema actual vlen");
    if(b){TypedRowView v;TypedRowSpan span;
        int rc=typed_row_open(f.ts,b,n,h,&v);ASSERT_EQ_INT(rc,0,"maximum-schema view");
        if(!rc){ASSERT_EQ_INT(typed_row_span(&v,127,&span),0,"maximum-schema changed span");ASSERT_EQ_INT(span.len,6,"new content length");
            if(span.len==6&&span.data)ASSERT_EQ_INT(memcmp(span.data,"longer",6),0,"new bytes");}free(b);}
    own_seed(&f,"a");c.mode=4;c.fake_len=(size_t)SLOTCASK_SEG_MAX_BYTES;
    SlotcaskUpsertResult r={0};errno=0;uint64_t before=own_segment_size(&f);
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,&r),-1,"actual record over segment max rejects");
    ASSERT_EQ_INT(errno,EFBIG,"segment ceiling errno");ASSERT_EQ_INT(r.condition_not_met,0,"segment limit is hard");
    ASSERT_EQ_INT(own_segment_size(&f),before,"segment overflow cannot reserve");own_result_free(&r);own_assert_seed(&f,"a");
    own_close(&f);
}
static void own_empty_and_legacy(void){
    char root[]="/tmp/shard-own-empty-XXXXXX";char *made=mkdtemp(root);
    ASSERT_NOT_NULL(made,"empty fixture");if(!made)return;
    slotcask_init(64,64);SlotcaskDb db;int rc=slotcask_open(&db,root,1,1,128);
    ASSERT_EQ_INT(rc,0,"small legacy object");
    if(!rc){
        SlotcaskUpsertOpts plain={0};ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"a",1,"old",3,&plain,NULL),0,"legacy seed");
        OwnCtx c={0};SlotcaskUpsertOpts o={0};o.require_existing=1;o.new_from_old=own_legacy;o.new_from_old_ctx=&c;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db,-1,"a",1,NULL,0,&o,NULL),0,"legacy callback still works");
        ASSERT_EQ_INT(c.calls,1,"legacy callback once");
        c.mode=5;c.calls=0;o.new_from_old=NULL;o.new_from_old_alloc=own_make;o.new_from_old_alloc_ctx=&c;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db,-1,"a",1,NULL,0,&o,NULL),0,"allocated empty value succeeds");
        ASSERT_EQ_INT(c.calls,1,"empty constructor once");
        void *b=NULL;size_t n=99;uint8_t h=99;ASSERT_EQ_INT(slotcask_get_ex(&db,"a",1,&b,&n,&h),0,"empty get");
        ASSERT_EQ_INT(n,0,"empty length exact");ASSERT_EQ_INT(h,0,"empty legacy generation");free(b);slotcask_close(&db);
    }
    slotcask_shutdown();ASSERT_EQ_INT(nftw(root,own_remove,16,FTW_DEPTH|FTW_PHYS),0,"empty fixture cleanup");
}
static int test_owning_write_hooks_run(void){
    own_success();own_rejections();own_maximum();own_empty_and_legacy();
    return t_ctx->failed?1:0;
}
TEST_REGISTER("test-owning-write-hooks",test_owning_write_hooks_run)

```

## Appendix C — Scratch-only allocation probe

<!-- block:probe -->
```c
#define _XOPEN_SOURCE 700
#include "types.h"
#include "slotcask.h"
#include "packed_record.h"
#include "shard_db.h"
#include <errno.h>
#include <ftw.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

ShardDb *shard_db_open_internal(const char *db_root);
void *__real_malloc(size_t);
void *__real_calloc(size_t,size_t);
void *__real_realloc(void *,size_t);
void __real_free(void *);
static _Atomic int tracking, oversized, released;
static _Atomic size_t largest;
static _Atomic(void *) pending;
static _Thread_local int fail_malloc;
static int allowed(size_t n){
    if(!atomic_load_explicit(&tracking,memory_order_relaxed))return 1;
    size_t prev=atomic_load_explicit(&largest,memory_order_relaxed);
    while(n>prev&&!atomic_compare_exchange_weak_explicit(&largest,&prev,n,memory_order_relaxed,memory_order_relaxed)){}
    if(n>8u*1024*1024){atomic_fetch_add(&oversized,1);errno=ENOMEM;return 0;}
    return 1;
}
void *__wrap_malloc(size_t n){
    if(fail_malloc){fail_malloc=0;errno=ENOMEM;return NULL;}
    return allowed(n)?__real_malloc(n):NULL;
}
void *__wrap_calloc(size_t a,size_t b){
    if(a&&b>SIZE_MAX/a){errno=ENOMEM;return NULL;}
    return allowed(a*b)?__real_calloc(a,b):NULL;
}
void *__wrap_realloc(void *p,size_t n){return allowed(n)?__real_realloc(p,n):NULL;}
void __wrap_free(void *p){
    if(p){
        void *expect=p;
        if(atomic_compare_exchange_strong(&pending,&expect,NULL))atomic_fetch_add(&released,1);
    }
    __real_free(p);
}
typedef struct {const uint8_t *value;size_t len;int mode,calls,hooks,observed;} Probe;
static int make(const SlotcaskOldRecord *old,uint8_t **out,size_t *n,void *ptr){
    Probe *p=ptr;p->calls++;
    if(!old){errno=EINVAL;return -1;}
    if(p->mode==3)return 0;
    if(p->mode==2)fail_malloc=1;
    *n=p->mode==4?(size_t)SLOTCASK_SEG_MAX_BYTES:p->mode==7?(size_t)UINT32_MAX+1:p->len;
    size_t alloc=(p->mode==1||p->mode==4||p->mode==6||p->mode==7)?1:p->len;
    *out=malloc(alloc?alloc:1);
    if(!*out)return -1;
    atomic_store(&pending,*out);
    if(p->mode==1||p->mode==6){errno=p->mode==1?EINVAL:0;return -1;}
    if(p->mode!=4&&p->mode!=7)memcpy(*out,p->value,p->len);
    return 0;
}
static int pre(const SlotcaskOldRecord *old,const uint8_t *b,size_t n,int was_update,void *ptr){
    (void)was_update;Probe *p=ptr;p->hooks++;
    if(old&&b==atomic_load(&pending)&&n==p->len&&!memcmp(b,p->value,n))p->observed=1;
    return p->mode==5;
}
static int rm(const char *p,const struct stat *s,int k,struct FTW *w){
    (void)s;(void)k;(void)w;return remove(p);
}
#define MUST(x) do{if(!(x)){fprintf(stderr,"probe failed line %d: %s errno=%d\n",__LINE__,#x,errno);exit(1);}}while(0)
int main(void){
    PkrField fields[PKR_MAX_FIELDS];PkrSchema schema;
    MUST(pkr_field_parse("varchar",&fields[0])==DEC_OK);
    for(int i=1;i<PKR_MAX_FIELDS;i++)fields[i]=fields[0];
    MUST(pkr_schema_build(fields,PKR_MAX_FIELDS,&schema)==DEC_OK);
    PkrCell cells[PKR_MAX_FIELDS];
    for(int i=0;i<PKR_MAX_FIELDS;i++)cells[i]=(PkrCell){NULL,0,1};
    cells[127]=(PkrCell){(const uint8_t*)"tiny",4,0};
    size_t n=0,bound=0;MUST(pkr_measure(&schema,cells,PKR_MAX_FIELDS,&n)==DEC_OK);
    uint8_t *value=malloc(n);MUST(value);
    MUST(pkr_compose(&schema,cells,PKR_MAX_FIELDS,value,n,&n)==DEC_OK);
    MUST(pkr_slot_bound(&schema,1024,&bound)==DEC_OK);MUST(bound>1000000000);
    char root[]="/tmp/shard-owned-probe-XXXXXX";MUST(mkdtemp(root));
    ShardDb *owner=shard_db_open_internal(root);MUST(owner);
    SlotcaskDb db;MUST(slotcask_open(&db,root,1,1,(int)bound)==0);
    SlotcaskUpsertOpts seed={0};seed.record_hdr2=PKR_FLAGS;
    MUST(slotcask_upsert_with_hooks(&db,-1,"a",1,value,n,&seed,NULL)==0);
    for(int mode=0;mode<=7;mode++){
        Probe p={value,n,mode,0,0,0};SlotcaskUpsertOpts o={0};
        o.require_existing=1;o.record_hdr2=PKR_FLAGS;
        o.new_from_old_alloc=make;o.new_from_old_alloc_ctx=&p;o.pre_commit=pre;o.pre_commit_ctx=&p;
        atomic_store(&released,0);atomic_store(&oversized,0);atomic_store(&largest,0);atomic_store(&pending,NULL);
        SlotcaskUpsertResult result={0};errno=0;atomic_store(&tracking,1);
        int rc=slotcask_upsert_with_hooks(&db,-1,"a",1,NULL,0,&o,&result);
        int cause=errno;atomic_store(&tracking,0);
        MUST(p.calls==1);MUST(atomic_load(&oversized)==0);MUST(!atomic_load(&pending));
        MUST(atomic_load(&released)==(mode==2||mode==3?0:1));
        if(mode==0){MUST(rc==0);MUST(p.hooks==1&&p.observed);}
        else if(mode==5){MUST(rc==-1);MUST(p.hooks==1&&p.observed);}
        else{
            MUST(rc==-1);MUST(!result.condition_not_met);MUST(p.hooks==0);
            MUST(cause==(mode==2?ENOMEM:mode==4||mode==7?EFBIG:EINVAL));
        }
        free(result.current_value);
        void *got=NULL;size_t gotn=0;uint8_t hdr2=0;
        MUST(slotcask_get_ex(&db,"a",1,&got,&gotn,&hdr2)==0);
        MUST(gotn==n&&hdr2==PKR_FLAGS&&!memcmp(got,value,n));free(got);
        printf("case %d: calls=1 owned-frees=%d hooks=%d max-allocation=%zu, OLD/result intact\n",
               mode,atomic_load(&released),p.hooks,atomic_load(&largest));
    }
    slotcask_close(&db);shard_db_close(owner);free(value);
    MUST(nftw(root,rm,16,FTW_DEPTH|FTW_PHYS)==0);
    puts("8 ownership/OOM/limit cases pass; 1GB schema bound caused no allocation over 8MiB");
    return 0;
}

```

## Appendix D — Complete concept document

<!-- block:hook-doc -->
```markdown
# Owning NEW-from-OLD write hook

SlotcaskUpsertOpts.new_from_old_alloc is an optional internal alternative to
the existing buffer-based new_from_old. The constructor returns actual-length
malloc/free-compatible bytes instead of the adapter allocating a theoretical
slot maximum. Both options together fail with EINVAL before either callback;
old callbacks and ordinary callers retain existing behavior.

The callback receives current OLD under the kf-shard write lock, after condition
gates, once before reservation/pre-commit. Missing OLD and rejected conditions
do not invoke it. It is not a fresh-insert default generator. It must not
re-enter the database or perform network I/O. Nonzero return is a hard error:
set errno for a cause; otherwise Slotcask supplies EINVAL.

Slotcask initializes output pointer/length to NULL/0 and takes ownership of
every non-NULL pointer returned, including a partial result on failure.
Success requires an allocated buffer even for zero bytes. Output must not
alias OLD or caller/schema storage. Accepted output survives the synchronous
transaction hooks; rejected and completed buffers are freed exactly once.
Durable marker recovery uses segment bytes, not freed callback storage.

The output contains final bytes. Supplied value/vlen are ignored, and neither
input nor output is passed to the registry's legacy trim callback in this mode.
A future legacy owning producer must trim its own final bytes first. Packed
producers return actual composed bytes. Generation comes from record_hdr2.

Before reservation, Slotcask checks uint32 value length and 8-byte-aligned
physical bytes including actual key and the 24-byte header against both slot
bound and current segment maximum. Overflow is EFBIG; invalid success output
is EINVAL. Constructor errno survives cleanup. Existing condition and
pre-commit failure policies remain unchanged.

There is no production constructor caller yet. This hook does not activate
packed schemas, change persisted formats, migrate objects, or complete single
and bulk command conversion. Explicit-schema tests exercise real physical
updates; public legacy tests remain the compatibility gate. Later write/read,
index, metadata and activation plans retain their own obligations.

```

## Appendix E — SHA256 block manifest

<!-- block:checksums -->
```json
{
  "e1-old": "e6fb2be7fbf429c8bf9ac49928b7e1092ef31aceafb31f828f8aefea3bd32b30",
  "e1-new": "62111cf510b7e3350e9a7ac41e2f209075898b8ed2173932e61630c15e9e95a2",
  "e2-old": "9e96b9af0bf87e86c48ba90f1db0c3799b34d12e24696a1d379b9ceac80e0242",
  "e2-new": "3e65c881f8aa66d547a7f036b58dd6f708ba4f08484e2fc0b33907dfa54d5b22",
  "e3-old": "9195f500a1014636951144e518d62771febf0c2b8998049c699f454411859a4c",
  "e3-new": "569118e0cc87c825bdac18278cdad0983e0d9fe140e556e33cdaf966ff9fb5ef",
  "e4-old": "4c2873c5411c895dc73afb076331710e88585b2063799a6c9cf3dd412e7d8883",
  "e4-new": "1059bc66dbd3e6d960bd17291399b469a46d219998e15bb3a466c0340c586511",
  "e5-old": "5a029fe7d94cbd26fef6404d2d0c3c69e17a05971a9b18764568bf2243d839c2",
  "e5-new": "565ed8602a11448ea5e4379f8a5c63b6c4c7976f87d68c9bb852e93f1d2cb08b",
  "e6-old": "72019db049137612f61f55dd2719f11eb016de3c5dabfc3b46b61b34bbd10a60",
  "e6-new": "f271c48e42c2de0b511b875ea2783ad6be80ad11ade5fa22cafe8aabde58c1c7",
  "e7-old": "cb29f7bedf9f749a5df9e2ad13ae0ca33d15d1d0bffd9c45421b25ade21921b9",
  "e7-new": "a37ea9668bc5b8091190f3e8aca879c29f3478027e11f2f5910a2dd58f566b41",
  "tests": "8b156410510b5e2bc3f585091519410ed7083ae402f0bc003dfec020a0822201",
  "edits": "cf2c7384743171ebd63d43ce068411f9c6dddc78ea79d8cc406bd14dc2875d7b",
  "e8-old": "d9ef52fd5fdc7d60ae07faad1d6773a1533ec68b87455781e0cd234bca99550e",
  "e8-new": "fd2fecd48a9d1507f051300e6844a6005e7760e7c490ba839f53c2bf62a950d3",
  "probe": "d2dfadc3555cc5c984ff06f1a707cb5cac12837bee3cd7f1fd2efed8ad7e16e4",
  "hook-doc": "d0c71eef5de383c0fe6bf1e591645b770401dd75bb513e8189a61d4e975678c4",
  "driver": "ddfdece13cd2e0b82562becab60a40fd4e8d21f22cc7a8b88d50a4304f7743dd",
  "checkpoints": "c216130fa5c4629ff7a47d220d6646c2a71bdb954ca9d8e0f50f9a4f4b3a90a1"
}
```


## Appendix F — Planning-time verification, not production execution

2026-10-25, base 47f5b0ab35f956355d9f9db63056906f610eb797. All proposed
source changes and runtime tests were confined to a git-archive copy under
/tmp/p3b2-plan-1d6t0ipo/tree. No production source, branch, index or commit was
changed. This evidence informs review; it does not replace the executor's own
RED-first run after human approval.

| Gate on final proposed code/test blocks | Actual | Exit |
|---|---|---|
| Task 1 normal full build at exact checkpoint 1 | zero warnings/errors | 0 |
| Task 1 focused runtime RED | 99 passed / 42 failed | 42 |
| Task 2 literal driver application | all four quoted replacements unique; checkpoint verified | 0 |
| Task 2 normal full build | zero warnings/errors | 0 |
| Task 2 focused GREEN | 153 passed / 0 failed | 0 |
| Task 2 normal full suite, socket-capable execution | 19225 passed / 0 failed across 520 cases | 0 |
| Task 3 separate-module compilation and final probe | all 8 cases pass | 0 |
| Task 3 literal doc stage | both files applied at checkpoint 3 | 0 |
| Fresh literal driver stages 1/2/3 from a second pristine archive | all six output files byte-identical to the compiled/tested copy | 0 |

Actual final runner summaries:
~~~text
# test-owning-write-hooks: 99 passed, 42 failed
# test-owning-write-hooks: 153 passed, 0 failed
1..520
# total: 19225 passed, 0 failed across 520 cases
8 ownership/OOM/limit cases pass; 1GB schema bound caused no allocation over 8MiB
~~~

The probe's largest observed operation allocation was 49152 bytes on the first
case and 2085 bytes on later cases. Those exact small counts are observations,
not portable performance gates; the probe's explicit 8 MiB cap is the check.
It verifies the owning output lifecycle, not every allocation in the engine.

Disclosed draft corrections before the final proof: initial test callbacks
omitted precommit's is_update argument and used the wrong trim pointer type;
compile errors were corrected against real headers. The first scratch
expectation incorrectly classified precommit refusal as soft; source and
behavior establish a hard -1, now pinned strictly. A standalone probe initially
used Slotcask without g_db; a GDB backtrace identified context initialization,
which is now explicit. Disabling LTO at the final link prevented archive plugin
loading; only the separately compiled module now disables it. No assertion was
weakened to conceal a production failure. Final RED was recaptured with the
corrected tests before reapplying the implementation; all final gates above
use those exact blocks. No sanitizers or benchmarks were run.

Independent blind raw-patch inspection, without the plan, found no actionable
correctness issue in dispatch, locking, ownership, errors, trim, bounds,
generation or test assumptions. Separate read-only fact checking verified all
23 unique block IDs, every block hash, all anchors and checkpoint maps against
the pinned source. It caught one over-narrow scope sentence, corrected to
optional single-record transport plumbing. Neither reviewer wrote the plan.
This does not constitute human approval or replace the post-execution raw-diff
review boundary.

Scratch raw evidence remains outside version control under the artifacts
subdirectory: red-final-build.log, red-final.log, green-final-build.log,
green-final.log, full-suite.log and probe-final.log. The document embeds all
code/commands, so none of these temporary paths is an execution dependency.
Protected hash was checked at planning start/end by hash only and remains exact.

## Appendix G — Final independent review and execution boundary

2026-10-25: a new reviewer received only the proposed raw patch and repository
standards, without this plan or prior reviews. A separate reviewer checked the
plan/specification and literal execution. Standards/blind correctness: zero
actionable findings. Specification/literal execution: zero actionable findings.
Both passes are recorded in 2026-10-25-phase3b2-blind-review.md.

The blind pass was static. Existing focused/probe tests observe owning-buffer
lifetime through precommit and transaction completion/rejection; they do not
inject owning-hook prepare/apply/post-marker failures or kill/reopen a pending
owning-hook marker. Source inspection confirms the established synchronous
transaction/replay cleanup retains the same adapter owner. Do not represent
that inspection or existing legacy full-suite coverage as a new owning-hook
crash test. Forced replay and public packed/indexed route acceptance remain
required later, as listed under remaining 3B/3C obligations.

The only handoff amendment is planning prose: Task 0 permits this saved review
file so a literal executor does not halt on an expected planning artifact.
All implementation/test/probe blocks and checkpoints remain byte-identical.
No production source changes or additional runtime gates were needed for this
prose-only amendment. No Git writes, sanitizers or benchmarks occurred.

## Execution closure — 2026-10-26

Tasks 0–3 are executed. The normal full suite passed 19225 assertions across
520 cases; focused 153/0; all eight ownership/OOM/limit probe cases pass.
Execution evidence: 2026-10-25-phase3b2-owning-hook-evidence.md.
Independent executed raw-diff review: 2026-10-26-phase3b2-raw-diff-review.md;
zero unresolved Standards/correctness and Spec/compliance findings.

The executed/approved plan hash was
c57de89e09d213694d16818877fc895b7259037263df1e5fe6c528ee6413f7af.
This closure updates planning status only; code/test/probe blocks, anchors and
checkpoints are unchanged. Production command/bulk conversion, activation,
remaining 3B work and pre-push branch-series sanitizers remain pending.
