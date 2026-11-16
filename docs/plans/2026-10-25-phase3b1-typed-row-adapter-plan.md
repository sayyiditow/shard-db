# Phase 3B.1 — inactive typed-row adapter

## Primary goal

Preserve every finance-decimal digit and varchar byte in the next write/read integrations while keeping existing applications working. This child prepares one checked, actual-size row interface; it does **not** yet unblock the finance application or complete Phase 3B.

**Status:** Revision 3 complete and independently re-reviewed. The user authorized cleanup and commit; packed storage remains inactive.
Appendix H is the current fix procedure; Appendix G preserves Revision 2 evidence. Original plan SHA256 was
`f110c99b7181f8d13ca072004bb54e3e1199a9bf0641db108ff994fa8aba0419`.

**Revision 2 changes:** enum TYPE tokens reject trailing declaration text;
true enum parse allocation failures preserve ENOMEM; fixed scalar failures now
establish errno at the new adapter boundary; legacy encode rejects NULL JSON
before delegation; legacy encode/update/span/OLD failures return deterministic
errors with outputs clear. The 33 new regression assertions were introduced
before these fixes and captured 201 passed / 13 failed against the previous
candidate, including the isolated NULL-input crash. The additional scratch
binder probe injects all enum-parser allocations. No production source changed.
**Planner/reviewer:** GPT-6.1. **Original executor:** GLM 5.3. GPT-6.1 executed the user-authorized enum fix in Appendix H.
**Base:** `b9cea93fd7e314eeb7505675e7d2212da850e184`, branch `fix/phase1-safe-scalar-conversions` (reviewed and committed Phase 3A).

## Scope and proposed sequencing

The approved split remains 3A codec/physical storage, 3B consumer integration, 3C metadata/publication. The 3B audit found 31 legacy record-view calls, six direct index-encoder calls, seven field-string calls, four dense update composers, three bulk-update layouts, and schema-sized insert/rebuild buffers. Combining them all in one executable plan would repeat the rejected oversized plan. This document proposes the first independently reviewable child **within** 3B.

Approval approves **3B.1 only**. Production writes, readers, indexes/joins/maintenance and rebuilds remain later 3B children and mandatory closure gates. Do not call this “3B complete,” activate a schema or replace public working numeric operations with refusals.

The deliverable is an internal module and real explicit-schema tests. Existing loader/create-object, storage/update callbacks, all query/index/bulk/rebuild functions and public SDK entry points keep their code and behavior. No persisted metadata/version, migration, release bump, cap change, callback change or global selector is introduced. Bound packed schemas only reach the new private adapters and codec; passing one to old raw-offset APIs remains unsafe and is forbidden until the owning consumer is converted.

## Execution rules

1. Continue the exact feature-branch checkpoint above; this authorized roadmap continuation does not branch off main or discard earlier commits. No Git writes: do not stage, commit, push, open a PR or merge.
2. Read AGENTS.md and CORE-PROCESS.md. All shell commands use `rtk`. Build with `rtk proxy env SKIP_TESTS=1 ./build.sh`; test with the compiled runner. Build all targets, including bench; never execute benchmarks.
3. Fresh execution: execute Tasks 0–3, then Appendix H, then Task 4. Resume from completed Revision 2: execute Appendix H only, including its final full-suite/probe gates. Capture each RED before the next source change. Introduce the final tests once; never weaken them. Transitional RED is intentional; final focused and normal full suite must pass.
4. Each quoted anchor must match exactly once; whole-file stage replacements require byte-identical prior blocks. On any anchor/checkpoint, unexpected build/warning/failure or unplanned decision: preserve state, write PLAN_NOTES.md with exact commands/output, halt the entire run. Resumption requires a human/planner amendment. Do not guess, reinterpret or continue unrelated tasks.
5. No sanitizers in this child. The AGENTS.md branch-series gate remains after **all** fixes, immediately before push: ASan+UBSan three normal full runs, TSan three full runs with --jobs 2 and its required options. The normal full suite is mandatory here. No benchmark/performance claims.
6. Keep extracted scripts, raw logs, probes and scratch probe binaries in a unique /tmp directory. Normal ./build.sh outputs remain in their usual ignored repository/build paths; never stage those build products. Commit candidates contain reviewed code/tests/docs only. The execution evidence is a concise Markdown ledger of actual outputs, not raw log files or debug hooks.
7. Protected `docs/plans/2026-10-19-durability-certification-rig.md`: hash only; never read/edit/move/stage/incorporate/execute. Required SHA256 `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`. Preserve existing plans/reviews.
8. Stop uncommitted/unstaged after final verification for plan-aware and independent raw-diff review. Plan approval does not authorize committing or activation.

## Task 0 — documentation discovery and checkpoint

**How this serves the primary goal:** binds work to real interfaces and keeps unsupported packed data away from applications.

Test-first: establish untouched source/checkpoint and protected hash before creating files. Approved planning-file changes and the protected untracked file are recorded separately; implementation sources must be clean.

```bash
rtk git rev-parse HEAD
rtk git branch --show-current
rtk git status --short
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
```

Read this child, the Phase 3 index and parent recovery plan; docs/concepts/typed-records.md and packed-record-codec.md; docs/query-protocol/bulk.md and cas.md; packed_record.h and decnum.h. Read the quoted source functions below. Do not reuse the withdrawn draft or read the protected file.

Discovery used independent reader, write/update and schema/test source investigations at this base. Allowed APIs and copy-ready patterns:

| Source | Pattern / contract |
|---|---|
| packed_record.h | field_parse/fixed, schema_build, view/span, measure/compose, record_fits; immutable borrowed spans, actual destination capacity |
| decnum.h | initialized distinct parse/precision outputs; payload_len/pack/unpack, text_len/to_text; exact arithmetic only |
| config.c typed_encode / typed_encode_defaults | zero-omission differs from default-omission; explicit NULL overrides default |
| config.c four shared helpers below | existing scalar/sentinel/default/render semantics; decode strings exactly once before sentinel validation |
| storage.c update_compose_new_from_old | legacy carry copies available bytes then zero pads; read-side whole-field zero substitution is separate |
| test_scalar_routes_checked.c scalar_routes_lowlevel | calloc explicit TypedSchema, names/prefix and parsed fields; no public metadata/global switch |
| test_null_legacy_rebuild.c | trimmed integer zero remains value; calendar sentinel becomes NULL in the old composer |
| build.sh | five separate source lists and explicit test registration |

Do not invent a declared parse_default_modifier API (none exists in a header), use json_skip_value as a validator, treat descriptors as scalar bytes, or add an environment/global activation seam.

### Signature/consumer and lifecycle inventory

No existing parameter/return signature changes. Four existing static config helpers become declared **internal** global functions, bodies otherwise byte-identical. Existing callers stay in config.c:

- encode_json_field_checked: typed_encode, typed_encode_defaults.
- sentinel_span_parses: the same two encoders.
- generate_default: typed_encode_defaults.
- decode_field_to_buf: typed_decode, typed_decode_stream, typed_get_field_str (two scalar branches).

New typed_row.c is their only new caller. They are not added to installed shard_db.h; internal static-library symbols become visible. New typed_row APIs have test callers only in this child. No external TypedSchema layout consumer is known/promised.

TypedSchema gains a trailing by-value PkrSchema, without new owned pointers. Existing local constructions in load_typed_schema, rebuild_object_v2 and edit-field memset the entire struct. Cache entries are zero-created and copy complete schemas. Hand-built tests scalar_routes_checked, coverity_join_buf_overflow, null_review_fixes, variable_length and not_null_writes calloc/memset/{0}. Existing enum ownership/destruction stays unchanged.

Verified x86_64 sizes: TypedSchema 154656 bytes (previously 147472), PkrSchema 7184, TypedPrepared 8464. The schema addition across TYPED_BUCKETS=256 costs 1839104 bytes (~1.75 MiB) per database context, including legacy-only contexts. This is an explicit proposed cost for approval, not a performance claim.

### Frozen adapter contract

- Bind already parsed names/defaults using TYPE-only tokens; validate everything before mutation. Retain bare/constrained/currency metadata. Zero packed.nfields means legacy. Packed variable field.size is the 8-byte directory entry, not maximum content.
- Reject schema/record generation mismatches both ways before field access; clear failed views. Keep NULL, empty varchar, decimal zero and removed slots distinct.
- Prepared objects own changed bytes and borrow OLD/schema/enum bytes immutably. Failed setters preserve cell/owner/seen/byte count. Defaults/stamp are incremental: discard on failure. Valid owned output arguments clear NULL/0 on error. Initialize before use, free idempotently, never alias outputs/inputs.
- Generate defaults once and retain them; measuring/composition never reruns sequence/random. No schema-max packed record allocation. Preparation cannot know actual key fit; future writers must call pkr_record_fits with actual key before mutation.
- Decimal limits: 131072 integer / 16383 fractional digits; separate dec_t precision outputs; negative declared S follows Phase 2/3A and never becomes a negative stored display scale.
- Validate complete JSON, including ignored values, at most 64 nested containers. Decode names before matching. Known values scalar; every duplicate validates, last valid occurrence wins. Unknown fields do not consume a fixed collector. TAB/CR delimiters are not field content. Byte varchar permits embedded NUL; other fixed scalar policies remain their existing policies.
- Render exact escaped JSON/name lengths before output allocation, budget includes terminating NUL. Decimal validation/render workspace is separately digit-bounded, not charged as output bytes. Length-aware raw field results preserve binary NUL.

## Task 1 — complete tests and stubs, capture RED

**How this serves the primary goal:** exposes digit/byte/NULL and actual-size failures before implementation.

Test-first: apply driver stage `1` only. It adds the entire final test, internal header, schema member, helper linkage, five build lists and unsupported stubs. Lifecycle cleanup makes RED safe; no binding/view/encoding support exists yet. Each of 13 quoted edits must match exactly once. The real test-runner link list is included; bench gains dependencies because the module uses decnum/packed_record (build only).

```bash
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-typed-row-adapter
```

Build must be zero-warning; focused run must be RED for unsupported binder/legacy adapters, not a compiler/link/harness failure. Preserve unfiltered output and exit code. A stub is not an oracle. See Appendix G (Revision 2) for the binding measured counts; unexplained deviations require halt, not assertion changes.

## Task 2 — schema binding, generation views, legacy materialization

**How this serves the primary goal:** supplies validated spans instead of guessing whether a descriptor is scalar bytes.

Test-first: Task 1 RED must exist. Driver stage `2` replaces the exact stub file with the complete `row-views` block. Header/tests must remain byte-identical. Binder/view/materialization code follows packed_record's validation and preserves the historical read/carry difference.

```bash
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-typed-row-adapter
```

Build must be zero-warning. Binder/legacy span checks become green; preparation/rendering stay RED. Preserve this second RED before stage 3.

## Task 3 — prepare once, actual-size encode/update/transfer, bounded render

**How this serves the primary goal:** prepares exact bytes and lengths without repeated generators or maximum buffers.

Test-first: retain Task 2 RED and unchanged tests. Driver stage `3` replaces the exact view-stage file with complete `row-core`. Every helper, parser, encoder and cleanup function is supplied; no executor-designed integration is required.

```bash
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-typed-row-adapter
rtk proxy ./build/bin/shard-db-test run-all
```

Require zero warnings, focused GREEN and normal full-suite GREEN. Daemon tests need localhost sockets: if sandbox startup is denied with EPERM, record it and use the host's authorized socket-capable mode for the same command. Never skip daemon cases, weaken assertions or rerun a functional failure until green.

## Task 4 — allocation/default probes, documentation, raw-diff hand-back

**How this serves the primary goal:** verifies bounded ownership/side effects and keeps remaining activation gates explicit.

Test-first: the committed case already covers canaries, caps, reused outputs, maximum digits, NUL, strict JSON/name/whitespace, and automatic timestamps. The scratch probe independently checks allocation failures; any defect requires planner amendment and RED-first committed regression before repair, not an unplanned executor change.

Extract probe-c into the unique task /tmp directory. Compile the **actual module** separately without LTO before linking the normal library. Linking only the LTO archive may inline generate_default and bypass its wrapper; that would make the count probe invalid. This is scratch instrumentation, no production seam or final-build flag change.

```bash
rtk proxy python3 "$P3B_TASK_TMP/apply.py" "$P3B_PLAN" extract probe-c "$P3B_TASK_TMP/probe.c"
rtk proxy gcc -std=gnu11 -O1 -g -fno-lto -Wall -Wextra -Werror -Isrc/db -c src/db/typed_row.c -o "$P3B_TASK_TMP/module.o"
rtk proxy gcc -std=gnu11 -O1 -g -Wall -Wextra -Werror -Isrc/db "$P3B_TASK_TMP/probe.c" "$P3B_TASK_TMP/module.o" build/bin/libshard-db.a -Wl,--wrap=malloc,--wrap=calloc,--wrap=realloc,--wrap=free,--wrap=generate_default -pthread -lssl -lcrypto -lm -o "$P3B_TASK_TMP/probe"
rtk proxy "$P3B_TASK_TMP/probe"
```

Fail each observed allocation in encode/update/render/auto-create transfer and enum schema binding. Enum binding failure must preserve the entire schema and report ENOMEM. Require error propagation, cleared owned output and release of every tracked allocation. This scoped malloc/calloc/realloc bookkeeping is not the sanitizer gate. Require zero output allocation for 4 MiB varchar with an 8-byte render budget. Wrapped sequence/random each run once; repeated defaults/measure/compose must not rerun them. This does not certify durable sequences or actual production callbacks.

Apply driver stage `4`: create the complete adapter doc and update codec intro. Doc-only changes follow the successful normal full suite; any later code/test change requires fresh focused/full verification.

Final implementation inventory: build.sh, types.h, config.c; new typed_row.h, typed_row.c, test_typed_row_adapter.c; concept docs typed-row-adapter.md and packed-record-codec.md. Write concise `docs/plans/2026-10-25-phase3b1-typed-row-evidence.md` with actual checkpoint/hash/commands/RED/GREEN/exits/warnings/probe/full-suite results and deviations. Preserve plan/index and existing documents. No raw logs/probe sources/debug caches are commit candidates.

```bash
rtk git diff --check
rtk git diff --stat
rtk git status --short
rtk rg -n 'typed_schema_bind_packed' src/db
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
```

Binder census in src/db may show declaration/definition only; no loader/create-object caller. Config changes are include plus removal of static from four unchanged helpers. No storage/query/index/bulk/rebuild file may differ. Stop uncommitted for raw-diff review; do not start the next child.

## Remaining gates — not completed here

| Owner | Required work |
|---|---|
| Next 3B write child | Actual-size owning NEW-from-OLD callback, prepared once; single insert/update/return_record; JSON/CSV bulk arenas and all three update slabs; per-row ownership/tallies; actual key/segment gate before mutation; packed computed updates deferred safely to Phase 4 |
| Later 3B readers/indexes | All legacy views/raw offsets; projection/CSV length channels, criteria/scans, builders/bitmap/trigram, joins/sorts and maintenance; explicit errors for private unsupported numeric semantics, never silent misses/zeros |
| Later 3B rebuilds | Actual-size schema rebuild/removal/carry/backfill; reserved sequence ranges consumed once; no implicit migration or historical auto timestamp rewriting; Phase 8 declaration-edit boundary |
| 3B closure | Fresh complete census, no schema-max packed temporary, corrupt-row error propagation, integrated explicit-schema routes/index/count preservation and failure tests |
| 3C | Authoritative identity/metadata, checked publication, strict loader/startup/downgrade gates, describe/export fidelity and forced KFM2 replay/crash evidence; still inactive |
| Phases 4–6 / activation | Public numeric/varchar grammar/default/write completion, exact criteria/index/aggregates and application acceptance before explicit approved activation |

Checkpoint's 31 view calls span config(3), index(5), query(4), aggregate(4), bulk(1), find(1), join(4), query_plan(2), schema(2), storage(5). This child converts **none**. Index-key encoder callers: index's single/composite/payload builders, query's record wrapper, join's local composite key, storage's trigram preparation. Field-string callers: query cursor, four find projection/CSV helpers, two join driver helpers. Dense composer callers: v2_update_new_from_old and three bulk-update callbacks. Future plans repeat source search rather than assuming these historical counts remain exhaustive.

## Planning-time evidence and review

Validation was performed in an isolated git archive of the base, never production sources. Planning evidence is not execution/approval evidence. Appendix F records the historical Revision 1 replay; Appendix G records the current Revision 2 exact-block replay. The executor must capture its own outputs again.

Independent prototype review found binary JSON decoding, decoded sentinel validation, failed-set seen mutation, pre-budget allocation, escaped-name matching, malformed ignored JSON and TAB/CR token-span defects. Final named regressions cover them. A separate 108-prefix guard-page probe checked truncated parser inputs. No sanitizer/benchmark/Git writes occurred; these checks are not proof for every future consumer or corrupt caller.

## Appendix A — mechanical extraction/application

Blocks have unique IDs. The single separator newline before a closing fence is not data; any intentional trailing newline remains captured. Use this driver and whole-source checks, not approximate line numbers or retyping.

```bash
P3B_PLAN="$PWD/docs/plans/2026-10-25-phase3b1-typed-row-adapter-plan.md"
P3B_TASK_TMP=$(rtk proxy mktemp -d /tmp/shard-p3b1-exec-XXXXXX)
rtk proxy python3 - "$P3B_PLAN" "$P3B_TASK_TMP/apply.py" <<'PY'
import re
import sys
from pathlib import Path
text = Path(sys.argv[1]).read_text()
match = re.search(r"(?ms)^<!-- block:driver -->\n```python\n(.*?)\n```(?=\n|$)", text)
if not match:
    raise SystemExit("HALT: missing driver block")
Path(sys.argv[2]).write_text(match.group(1))
PY
```

Run only the corresponding task stage after its prior gates, **never these together**:

```bash
rtk proxy python3 "$P3B_TASK_TMP/apply.py" "$P3B_PLAN" 1
rtk proxy python3 "$P3B_TASK_TMP/apply.py" "$P3B_PLAN" 2
rtk proxy python3 "$P3B_TASK_TMP/apply.py" "$P3B_PLAN" 3
rtk proxy python3 "$P3B_TASK_TMP/apply.py" "$P3B_PLAN" 4
```

<!-- block:driver -->
```python
import json
import re
import sys
from pathlib import Path

plan = Path(sys.argv[1]).read_text()
blocks = {}
for match in re.finditer(r"(?ms)^<!-- block:([a-z0-9-]+) -->\n```[^\n]*\n(.*?)\n```(?=\n|$)", plan):
    name, content = match.groups()
    if name in blocks:
        raise SystemExit("duplicate block: " + name)
    blocks[name] = content
stage = sys.argv[2]
if stage == "extract":
    Path(sys.argv[4]).write_text(blocks[sys.argv[3]])
    raise SystemExit(0)
root = Path.cwd()
changes = {}
def replace(path, old, new):
    original = changes.get(path, (root/path).read_text())
    if original.count(old) != 1:
        raise SystemExit("HALT: anchor count for " + path + " = " + str(original.count(old)))
    changes[path] = original.replace(old, new, 1)
def create(path, content):
    if (root/path).exists():
        raise SystemExit("HALT: new path exists: " + path)
    changes[path] = content
if stage == "1":
    for edit in json.loads(blocks["edits"]):
        replace(edit["path"], blocks[edit["old"]], blocks[edit["new"]])
    create("src/db/typed_row.h", blocks["row-header"])
    create("src/db/typed_row.c", blocks["row-stub"])
    create("src/test/cases/test_typed_row_adapter.c", blocks["row-tests"])
elif stage in ("2", "3"):
    for path, name in (("src/db/typed_row.h", "row-header"),
                       ("src/test/cases/test_typed_row_adapter.c", "row-tests")):
        if (root/path).read_text() != blocks[name]:
            raise SystemExit("HALT: stage input differs: " + path)
    before, after = ("row-stub", "row-views") if stage == "2" else ("row-views", "row-core")
    path = "src/db/typed_row.c"
    if (root/path).read_text() != blocks[before]:
        raise SystemExit("HALT: entire source checkpoint differs: " + path)
    changes[path] = blocks[after]
elif stage == "4":
    create("docs/concepts/typed-row-adapter.md", blocks["row-doc"])
    replace("docs/concepts/packed-record-codec.md", blocks["codec-doc-old"], blocks["codec-doc-new"])
else:
    raise SystemExit("unknown stage: " + stage)
# Preflight all files before changing any file. A write error still requires a halt.
for path, content in changes.items():
    (root/path).write_text(content)
    print("wrote " + path)

```

## Appendix B — all existing-file edits for stage 1

<!-- block:edits -->
```json
[
  {
    "path": "src/db/types.h",
    "old": "edit-1-old",
    "new": "edit-1-new"
  },
  {
    "path": "src/db/types.h",
    "old": "edit-2-old",
    "new": "edit-2-new"
  },
  {
    "path": "src/db/config.c",
    "old": "edit-3-old",
    "new": "edit-3-new"
  },
  {
    "path": "src/db/config.c",
    "old": "edit-4-old",
    "new": "edit-4-new"
  },
  {
    "path": "src/db/config.c",
    "old": "edit-5-old",
    "new": "edit-5-new"
  },
  {
    "path": "src/db/config.c",
    "old": "edit-6-old",
    "new": "edit-6-new"
  },
  {
    "path": "src/db/config.c",
    "old": "edit-7-old",
    "new": "edit-7-new"
  },
  {
    "path": "build.sh",
    "old": "edit-8-old",
    "new": "edit-8-new"
  },
  {
    "path": "build.sh",
    "old": "edit-9-old",
    "new": "edit-9-new"
  },
  {
    "path": "build.sh",
    "old": "edit-10-old",
    "new": "edit-10-new"
  },
  {
    "path": "build.sh",
    "old": "edit-11-old",
    "new": "edit-11-new"
  },
  {
    "path": "build.sh",
    "old": "edit-12-old",
    "new": "edit-12-new"
  },
  {
    "path": "build.sh",
    "old": "edit-13-old",
    "new": "edit-13-new"
  }
]
```

### Edit 1: `src/db/types.h` — exact old anchor and complete replacement

<!-- block:edit-1-old -->
```c
#include <stdint.h>
```

<!-- block:edit-1-new -->
```c
#include <stdint.h>
#include "packed_record.h"
```

### Edit 2: `src/db/types.h` — exact old anchor and complete replacement

<!-- block:edit-2-old -->
```c
                           and every hand-built TypedSchema must set it. */
} TypedSchema;
```

<!-- block:edit-2-new -->
```c
                           and every hand-built TypedSchema must set it. */
    /* By-value private codec metadata. Zero means legacy. Existing loaders
       never bind packed layouts in Phase 3B. Schemas are immutable in use. */
    PkrSchema packed;
} TypedSchema;
```

### Edit 3: `src/db/config.c` — exact old anchor and complete replacement

<!-- block:edit-3-old -->
```c
#include "types.h"
```

<!-- block:edit-3-new -->
```c
#include "types.h"
#include "typed_row.h"
```

### Edit 4: `src/db/config.c` — exact old anchor and complete replacement

<!-- block:edit-4-old -->
```c
static int encode_json_field_checked(const TypedField *f, const char *val,
                                      size_t vlen, int was_quoted,
                                      uint8_t *out, char *err, size_t errsz) {
    if (!was_quoted || !typed_json_string_type(f->type))
        return encode_field_checked(f, val, vlen, out, err, errsz);
    char *decoded = NULL;
    size_t decoded_len = 0;
    errno = 0;
    if (json_unescape_cstring(val, vlen, &decoded, &decoded_len) != 0) {
        int decode_errno = errno;
        if (err && errsz)
            snprintf(err, errsz, decode_errno == ENOMEM
                     ? "out of memory decoding JSON value for field '%s'"
                     : "malformed JSON escape in value for field '%s'",
                     f->name);
        errno = decode_errno;
        return -1;
    }
    int rc = encode_field_checked(f, decoded, decoded_len, out, err, errsz);
    free(decoded);
    return rc;
}
```

<!-- block:edit-4-new -->
```c
int encode_json_field_checked(const TypedField *f, const char *val,
                                      size_t vlen, int was_quoted,
                                      uint8_t *out, char *err, size_t errsz) {
    if (!was_quoted || !typed_json_string_type(f->type))
        return encode_field_checked(f, val, vlen, out, err, errsz);
    char *decoded = NULL;
    size_t decoded_len = 0;
    errno = 0;
    if (json_unescape_cstring(val, vlen, &decoded, &decoded_len) != 0) {
        int decode_errno = errno;
        if (err && errsz)
            snprintf(err, errsz, decode_errno == ENOMEM
                     ? "out of memory decoding JSON value for field '%s'"
                     : "malformed JSON escape in value for field '%s'",
                     f->name);
        errno = decode_errno;
        return -1;
    }
    int rc = encode_field_checked(f, decoded, decoded_len, out, err, errsz);
    free(decoded);
    return rc;
}
```

### Edit 5: `src/db/config.c` — exact old anchor and complete replacement

<!-- block:edit-5-old -->
```c
static int sentinel_span_parses(enum FieldType t, const char *val, size_t vlen) {
    switch (t) {
    case FT_IPV4: {
        char ipbuf[16]; uint8_t sink[4];
        size_t n = vlen < sizeof(ipbuf) - 1 ? vlen : sizeof(ipbuf) - 1;
        memcpy(ipbuf, val, n); ipbuf[n] = '\0';
        return n > 0 && inet_pton(AF_INET, ipbuf, sink) == 1;
    }
    case FT_IPV6: {
        char ipbuf[46]; uint8_t sink[16];
        size_t n = vlen < sizeof(ipbuf) - 1 ? vlen : sizeof(ipbuf) - 1;
        memcpy(ipbuf, val, n); ipbuf[n] = '\0';
        return n > 0 && inet_pton(AF_INET6, ipbuf, sink) == 1;
    }
    case FT_UUID: {
        int pos = 0;
        for (size_t i = 0; i < vlen && pos < 32; i++) {
            char c = val[i];
            if (c == '-') continue;
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
                  (c >= 'A' && c <= 'F'))) return 0;
            pos++;
        }
        return pos == 32;
    }
    default:
        return 1;
    }
}
```

<!-- block:edit-5-new -->
```c
int sentinel_span_parses(enum FieldType t, const char *val, size_t vlen) {
    switch (t) {
    case FT_IPV4: {
        char ipbuf[16]; uint8_t sink[4];
        size_t n = vlen < sizeof(ipbuf) - 1 ? vlen : sizeof(ipbuf) - 1;
        memcpy(ipbuf, val, n); ipbuf[n] = '\0';
        return n > 0 && inet_pton(AF_INET, ipbuf, sink) == 1;
    }
    case FT_IPV6: {
        char ipbuf[46]; uint8_t sink[16];
        size_t n = vlen < sizeof(ipbuf) - 1 ? vlen : sizeof(ipbuf) - 1;
        memcpy(ipbuf, val, n); ipbuf[n] = '\0';
        return n > 0 && inet_pton(AF_INET6, ipbuf, sink) == 1;
    }
    case FT_UUID: {
        int pos = 0;
        for (size_t i = 0; i < vlen && pos < 32; i++) {
            char c = val[i];
            if (c == '-') continue;
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
                  (c >= 'A' && c <= 'F'))) return 0;
            pos++;
        }
        return pos == 32;
    }
    default:
        return 1;
    }
}
```

### Edit 6: `src/db/config.c` — exact old anchor and complete replacement

<!-- block:edit-6-old -->
```c
static const char *generate_default(const TypedField *tf, char *gen_buf, size_t bufsz,
                                    const char *db_root, const char *object) {
    switch (tf->default_kind) {
    case DK_NONE:
        return NULL;
    case DK_LITERAL:
        return tf->default_val;
    case DK_AUTO_CREATE:
    case DK_AUTO_UPDATE:
        if (tf->type == FT_TIMESTAMP)
            gen_timestamp_now(gen_buf, bufsz);
        else if (tf->type == FT_DATETIMEMS)
            gen_datetimems_now(gen_buf, bufsz);
        else if (tf->type == FT_DATETIME)
            gen_datetime_now(gen_buf, bufsz);
        else if (tf->type == FT_DATE)
            gen_date_now(gen_buf, bufsz);
        else
            gen_datetime_now(gen_buf, bufsz); /* fallback for varchar etc. */
        return gen_buf;
    case DK_SEQ:
        if (!db_root || !object) return NULL;
        { long long v = seq_next_val(db_root, object, tf->default_val);
          if (v < 0) return NULL;
          snprintf(gen_buf, bufsz, "%lld", v);
          return gen_buf; }
    case DK_UUID:
        gen_uuid4(gen_buf, bufsz);
        return gen_buf[0] ? gen_buf : NULL;
    case DK_RANDOM:
        { int n = atoi(tf->default_val);
          gen_random_hex(n, gen_buf, bufsz);
          return gen_buf[0] ? gen_buf : NULL; }
    }
    return NULL;
}
```

<!-- block:edit-6-new -->
```c
const char *generate_default(const TypedField *tf, char *gen_buf, size_t bufsz,
                                    const char *db_root, const char *object) {
    switch (tf->default_kind) {
    case DK_NONE:
        return NULL;
    case DK_LITERAL:
        return tf->default_val;
    case DK_AUTO_CREATE:
    case DK_AUTO_UPDATE:
        if (tf->type == FT_TIMESTAMP)
            gen_timestamp_now(gen_buf, bufsz);
        else if (tf->type == FT_DATETIMEMS)
            gen_datetimems_now(gen_buf, bufsz);
        else if (tf->type == FT_DATETIME)
            gen_datetime_now(gen_buf, bufsz);
        else if (tf->type == FT_DATE)
            gen_date_now(gen_buf, bufsz);
        else
            gen_datetime_now(gen_buf, bufsz); /* fallback for varchar etc. */
        return gen_buf;
    case DK_SEQ:
        if (!db_root || !object) return NULL;
        { long long v = seq_next_val(db_root, object, tf->default_val);
          if (v < 0) return NULL;
          snprintf(gen_buf, bufsz, "%lld", v);
          return gen_buf; }
    case DK_UUID:
        gen_uuid4(gen_buf, bufsz);
        return gen_buf[0] ? gen_buf : NULL;
    case DK_RANDOM:
        { int n = atoi(tf->default_val);
          gen_random_hex(n, gen_buf, bufsz);
          return gen_buf[0] ? gen_buf : NULL; }
    }
    return NULL;
}
```

### Edit 7: `src/db/config.c` — exact old anchor and complete replacement

<!-- block:edit-7-old -->
```c
static int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                               char *buf, int buflen, int render_zeros) {
    switch (f->type) {
    case FT_VARCHAR: {
        /* Layout: [uint16 BE length][content]. Content bytes are
           opaque (UTF-8 text in practice but the engine doesn't
           enforce it) so we MUST escape per RFC 8259 — raw quotes /
           backslashes / control chars in HN comment bodies broke
           the response JSON before this. Caller's buflen needs
           room for up to 6 * len + 2 (quotes); decode_field_to_buf
           callers must size accordingly for FT_VARCHAR — see
           varchar_decode_capacity(). */
        int len = ((int)data[0] << 8) | (int)data[1];
        int content_max = f->size - 2;
        if (content_max < 0) content_max = 0;
        if (len > content_max) len = content_max;  /* defensive */
        if (len == 0) {
            if (!render_zeros) return 0;
            buf[0] = '"'; buf[1] = '"'; buf[2] = '\0';
            return 2;
        }
        if (buflen < 4) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: buffer too small for field [%s] (buflen=%d, need>=4)", f->name, buflen);
            return -1;  /* "" + NUL */
        }
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3,
                                   (const char *)(data + 2), (size_t)len);
        if (esc < 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: json_escape_into overflow for field [%s] (len=%d, buflen=%d)", f->name, len, buflen);
            return -1;
        }
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';   /* caller renders with %s */
        return 2 + esc;
    }
    case FT_LONG: {
        uint64_t u = ((uint64_t)data[0] << 56) | ((uint64_t)data[1] << 48) |
                     ((uint64_t)data[2] << 40) | ((uint64_t)data[3] << 32) |
                     ((uint64_t)data[4] << 24) | ((uint64_t)data[5] << 16) |
                     ((uint64_t)data[6] << 8) | data[7];
        int64_t v = (int64_t)u;   /* unsigned accumulate: shifting a signed
                                     value into its sign bit was UB for
                                     negative fields (strict gate) */
        if (v == 0) { /* skip zero — legacy only */
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "0");
        }
        return snprintf(buf, buflen, "%lld", (long long)v);
    }
    case FT_TIMESTAMP: {
        /* 0 is a valid timestamp (1970-01-01T00:00:00Z), so don't skip
           it — but uninitialised-storage zero looks the same as a real
           epoch-zero record, and there's no out-of-band sentinel to
           tell them apart. We match FT_LONG's skip-on-zero behaviour
           since uninit-zero is overwhelmingly the common case (a
           record using `auto_create` will always be > 0; an explicit
           0 epoch-ms is exotic enough that the existing emit-as-empty
           is acceptable). Callers needing strict "0 is valid" can
           inspect raw bytes via typed_get_field_str. */
        uint64_t u = ((uint64_t)data[0] << 56) | ((uint64_t)data[1] << 48) |
                     ((uint64_t)data[2] << 40) | ((uint64_t)data[3] << 32) |
                     ((uint64_t)data[4] << 24) | ((uint64_t)data[5] << 16) |
                     ((uint64_t)data[6] << 8) | data[7];
        int64_t v = (int64_t)u;   /* see FT_LONG: unsigned accumulate */
        if (v == 0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "0");
        }
        return snprintf(buf, buflen, "%lld", (long long)v);
    }
    case FT_INT: {
        uint32_t u = ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) |
                     ((uint32_t)data[2] << 8) | data[3];
        int32_t v = (int32_t)u;   /* see FT_LONG: unsigned accumulate */
        return snprintf(buf, buflen, "%d", v);
    }
    case FT_SHORT: {
        int16_t v = ((int16_t)data[0] << 8) | data[1];
        return snprintf(buf, buflen, "%d", v);
    }
    case FT_DOUBLE: {
        double v;
        memcpy(&v, data, 8);
        if (v == 0.0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "0");
        }
        /* 17 significant digits: every double round-trips exactly. */
        return snprintf(buf, buflen, "%.17g", v);
    }
    case FT_FLOAT: {
        float v;
        memcpy(&v, data, 4);
        if (v == 0.0f) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "0");
        }
        /* 9 significant digits: every float round-trips exactly, parsed
           directly as binary32. */
        return snprintf(buf, buflen, "%.9g", (double)v);
    }
    case FT_BOOL: {
        return snprintf(buf, buflen, "%s", data[0] ? "true" : "false");
    }
    case FT_BYTE: {
        return snprintf(buf, buflen, "%u", data[0]);
    }
    case FT_NUMERIC: {
        uint64_t u = ((uint64_t)data[0] << 56) | ((uint64_t)data[1] << 48) |
                     ((uint64_t)data[2] << 40) | ((uint64_t)data[3] << 32) |
                     ((uint64_t)data[4] << 24) | ((uint64_t)data[5] << 16) |
                     ((uint64_t)data[6] << 8) | data[7];
        int64_t v = (int64_t)u;   /* see FT_LONG: unsigned accumulate (UB gate) */
        if (v == 0) return snprintf(buf, buflen, "0");
        int64_t scale = f->numeric_scale_mult;
        if (scale <= 0)   /* unsupported legacy scale: raw integer fallback */
            return snprintf(buf, buflen, "%lld", (long long)v);
        /* Magnitude in unsigned arithmetic — negating INT64_MIN is UB
           and rendered "--9223372036854775808" at scale 0. */
        uint64_t m = (uint64_t)v;
        int neg = v < 0;
        if (neg) m = 0u - m;
        uint64_t whole = m / (uint64_t)scale;
        uint64_t frac = m % (uint64_t)scale;
        if (frac == 0)
            return snprintf(buf, buflen, "%s%llu", neg ? "-" : "",
                            (unsigned long long)whole);
        return snprintf(buf, buflen, "%s%llu.%0*llu", neg ? "-" : "",
                        (unsigned long long)whole, f->numeric_scale,
                        (unsigned long long)frac);
    }
    case FT_DATE: {
        uint32_t u = ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) |
                     ((uint32_t)data[2] << 8) | data[3];
        int32_t v = (int32_t)u;   /* see FT_LONG: unsigned accumulate (UB gate) */
        if (v == 0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"00000000\"");
        }
        return snprintf(buf, buflen, "\"%08d\"", v); /* "yyyyMMdd" */
    }
    case FT_DATETIME: {
        uint32_t ud = ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) |
                      ((uint32_t)data[2] << 8) | data[3];
        int32_t d = (int32_t)ud;   /* see FT_LONG: unsigned accumulate (UB gate) */
        uint32_t t = ((uint32_t)data[4] << 16) |
                     ((uint32_t)data[5] << 8) | data[6];
        if (d == 0 && t == 0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"00000000000000\"");
        }
        int hh = t / 3600;
        int mm = (t % 3600) / 60;
        int ss = t % 60;
        return snprintf(buf, buflen, "\"%08d%02d%02d%02d\"", d, hh, mm, ss); /* "yyyyMMddHHmmss" */
    }
    case FT_DATETIMEMS: {
        uint32_t ud = ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) |
                      ((uint32_t)data[2] << 8) | data[3];
        int32_t d = (int32_t)ud;   /* see FT_LONG: unsigned accumulate (UB gate) */
        uint32_t ms = ((uint32_t)data[4] << 24) | ((uint32_t)data[5] << 16) |
                      ((uint32_t)data[6] << 8) | data[7];
        if (d == 0 && ms == 0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"00000000000000000\"");
        }
        int hh = ms / 3600000;
        int mm = (ms % 3600000) / 60000;
        int ss = (ms % 60000) / 1000;
        int fff = ms % 1000;
        return snprintf(buf, buflen, "\"%08d%02d%02d%02d%03d\"", d, hh, mm, ss, fff);
    }
    case FT_TIME: {
        uint32_t secs = ((uint32_t)data[0] << 16) | ((uint32_t)data[1] << 8) | data[2];
        if (secs == 0 && data[0]==0 && data[1]==0 && data[2]==0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"00:00:00\"");
        }
        int hh = secs / 3600;
        int mm = (secs % 3600) / 60;
        int ss = secs % 60;
        return snprintf(buf, buflen, "\"%02d:%02d:%02d\"", hh, mm, ss); /* "HH:MM:SS" */
    }
    case FT_UUID: {
        /* Emit JSON-quoted canonical form ("xxxxxxxx-...-xxxxxxxxxxxx").
           All-zero bytes = unset; skip empty. */
        const uint8_t *b = data;
        if (uuid_is_zero(b)) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen,
                            "\"00000000-0000-0000-0000-000000000000\"");
        }
        if (buflen < 39) return 0; /* 36 hex + 2 quotes + NUL */
        char inner[37];
        uuid_format_canonical(inner, sizeof(inner), b);
        return snprintf(buf, buflen, "\"%s\"", inner);
    }
    case FT_IPV4: {
        if (data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"0.0.0.0\"");
        }
        char ipstr[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, data, ipstr, sizeof(ipstr)))
            return 0;
        return snprintf(buf, buflen, "\"%s\"", ipstr);
    }
    case FT_IPV6: {
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (data[bi] != 0) { allzero = 0; break; }
        if (allzero) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"::\"");
        }
        char ipstr[INET6_ADDRSTRLEN];
        if (!inet_ntop(AF_INET6, data, ipstr, sizeof(ipstr)))
            return 0;
        return snprintf(buf, buflen, "\"%s\"", ipstr);
    }
    case FT_ENUM: {
        /* Stored bytes are the byte index (1 or 2 BE). Always emit
           (byte 0 is a legit enum index, not "unset"). Output is a
           JSON-quoted string from enum_values[idx]. Out-of-range
           index (data corruption) → empty string. */
        if (!f->enum_values || f->n_enum_values <= 0) return 0;
        int idx = (f->enum_width == 2)
                    ? (int)(((uint16_t)data[0] << 8) | (uint16_t)data[1])
                    : (int)data[0];
        if (idx < 0 || idx >= f->n_enum_values) return snprintf(buf, buflen, "\"\"");
        const char *s = f->enum_values[idx];
        if (!s) return snprintf(buf, buflen, "\"\"");
        /* Escape per RFC 8259 — enum value strings are user-supplied
           at create-object, may carry quotes/backslashes/control chars.
           Same buffer-sizing contract as FT_VARCHAR. */
        if (buflen < 4) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: buffer too small for enum field [%s] (buflen=%d, need>=4)", f->name, buflen);
            return -1;
        }
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3, s, strlen(s));
        if (esc < 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: json_escape_into overflow for enum field [%s] value [%s]", f->name, s);
            return -1;
        }
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    default:
        return 0;
    }
}
```

<!-- block:edit-7-new -->
```c
int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                               char *buf, int buflen, int render_zeros) {
    switch (f->type) {
    case FT_VARCHAR: {
        /* Layout: [uint16 BE length][content]. Content bytes are
           opaque (UTF-8 text in practice but the engine doesn't
           enforce it) so we MUST escape per RFC 8259 — raw quotes /
           backslashes / control chars in HN comment bodies broke
           the response JSON before this. Caller's buflen needs
           room for up to 6 * len + 2 (quotes); decode_field_to_buf
           callers must size accordingly for FT_VARCHAR — see
           varchar_decode_capacity(). */
        int len = ((int)data[0] << 8) | (int)data[1];
        int content_max = f->size - 2;
        if (content_max < 0) content_max = 0;
        if (len > content_max) len = content_max;  /* defensive */
        if (len == 0) {
            if (!render_zeros) return 0;
            buf[0] = '"'; buf[1] = '"'; buf[2] = '\0';
            return 2;
        }
        if (buflen < 4) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: buffer too small for field [%s] (buflen=%d, need>=4)", f->name, buflen);
            return -1;  /* "" + NUL */
        }
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3,
                                   (const char *)(data + 2), (size_t)len);
        if (esc < 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: json_escape_into overflow for field [%s] (len=%d, buflen=%d)", f->name, len, buflen);
            return -1;
        }
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';   /* caller renders with %s */
        return 2 + esc;
    }
    case FT_LONG: {
        uint64_t u = ((uint64_t)data[0] << 56) | ((uint64_t)data[1] << 48) |
                     ((uint64_t)data[2] << 40) | ((uint64_t)data[3] << 32) |
                     ((uint64_t)data[4] << 24) | ((uint64_t)data[5] << 16) |
                     ((uint64_t)data[6] << 8) | data[7];
        int64_t v = (int64_t)u;   /* unsigned accumulate: shifting a signed
                                     value into its sign bit was UB for
                                     negative fields (strict gate) */
        if (v == 0) { /* skip zero — legacy only */
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "0");
        }
        return snprintf(buf, buflen, "%lld", (long long)v);
    }
    case FT_TIMESTAMP: {
        /* 0 is a valid timestamp (1970-01-01T00:00:00Z), so don't skip
           it — but uninitialised-storage zero looks the same as a real
           epoch-zero record, and there's no out-of-band sentinel to
           tell them apart. We match FT_LONG's skip-on-zero behaviour
           since uninit-zero is overwhelmingly the common case (a
           record using `auto_create` will always be > 0; an explicit
           0 epoch-ms is exotic enough that the existing emit-as-empty
           is acceptable). Callers needing strict "0 is valid" can
           inspect raw bytes via typed_get_field_str. */
        uint64_t u = ((uint64_t)data[0] << 56) | ((uint64_t)data[1] << 48) |
                     ((uint64_t)data[2] << 40) | ((uint64_t)data[3] << 32) |
                     ((uint64_t)data[4] << 24) | ((uint64_t)data[5] << 16) |
                     ((uint64_t)data[6] << 8) | data[7];
        int64_t v = (int64_t)u;   /* see FT_LONG: unsigned accumulate */
        if (v == 0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "0");
        }
        return snprintf(buf, buflen, "%lld", (long long)v);
    }
    case FT_INT: {
        uint32_t u = ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) |
                     ((uint32_t)data[2] << 8) | data[3];
        int32_t v = (int32_t)u;   /* see FT_LONG: unsigned accumulate */
        return snprintf(buf, buflen, "%d", v);
    }
    case FT_SHORT: {
        int16_t v = ((int16_t)data[0] << 8) | data[1];
        return snprintf(buf, buflen, "%d", v);
    }
    case FT_DOUBLE: {
        double v;
        memcpy(&v, data, 8);
        if (v == 0.0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "0");
        }
        /* 17 significant digits: every double round-trips exactly. */
        return snprintf(buf, buflen, "%.17g", v);
    }
    case FT_FLOAT: {
        float v;
        memcpy(&v, data, 4);
        if (v == 0.0f) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "0");
        }
        /* 9 significant digits: every float round-trips exactly, parsed
           directly as binary32. */
        return snprintf(buf, buflen, "%.9g", (double)v);
    }
    case FT_BOOL: {
        return snprintf(buf, buflen, "%s", data[0] ? "true" : "false");
    }
    case FT_BYTE: {
        return snprintf(buf, buflen, "%u", data[0]);
    }
    case FT_NUMERIC: {
        uint64_t u = ((uint64_t)data[0] << 56) | ((uint64_t)data[1] << 48) |
                     ((uint64_t)data[2] << 40) | ((uint64_t)data[3] << 32) |
                     ((uint64_t)data[4] << 24) | ((uint64_t)data[5] << 16) |
                     ((uint64_t)data[6] << 8) | data[7];
        int64_t v = (int64_t)u;   /* see FT_LONG: unsigned accumulate (UB gate) */
        if (v == 0) return snprintf(buf, buflen, "0");
        int64_t scale = f->numeric_scale_mult;
        if (scale <= 0)   /* unsupported legacy scale: raw integer fallback */
            return snprintf(buf, buflen, "%lld", (long long)v);
        /* Magnitude in unsigned arithmetic — negating INT64_MIN is UB
           and rendered "--9223372036854775808" at scale 0. */
        uint64_t m = (uint64_t)v;
        int neg = v < 0;
        if (neg) m = 0u - m;
        uint64_t whole = m / (uint64_t)scale;
        uint64_t frac = m % (uint64_t)scale;
        if (frac == 0)
            return snprintf(buf, buflen, "%s%llu", neg ? "-" : "",
                            (unsigned long long)whole);
        return snprintf(buf, buflen, "%s%llu.%0*llu", neg ? "-" : "",
                        (unsigned long long)whole, f->numeric_scale,
                        (unsigned long long)frac);
    }
    case FT_DATE: {
        uint32_t u = ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) |
                     ((uint32_t)data[2] << 8) | data[3];
        int32_t v = (int32_t)u;   /* see FT_LONG: unsigned accumulate (UB gate) */
        if (v == 0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"00000000\"");
        }
        return snprintf(buf, buflen, "\"%08d\"", v); /* "yyyyMMdd" */
    }
    case FT_DATETIME: {
        uint32_t ud = ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) |
                      ((uint32_t)data[2] << 8) | data[3];
        int32_t d = (int32_t)ud;   /* see FT_LONG: unsigned accumulate (UB gate) */
        uint32_t t = ((uint32_t)data[4] << 16) |
                     ((uint32_t)data[5] << 8) | data[6];
        if (d == 0 && t == 0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"00000000000000\"");
        }
        int hh = t / 3600;
        int mm = (t % 3600) / 60;
        int ss = t % 60;
        return snprintf(buf, buflen, "\"%08d%02d%02d%02d\"", d, hh, mm, ss); /* "yyyyMMddHHmmss" */
    }
    case FT_DATETIMEMS: {
        uint32_t ud = ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) |
                      ((uint32_t)data[2] << 8) | data[3];
        int32_t d = (int32_t)ud;   /* see FT_LONG: unsigned accumulate (UB gate) */
        uint32_t ms = ((uint32_t)data[4] << 24) | ((uint32_t)data[5] << 16) |
                      ((uint32_t)data[6] << 8) | data[7];
        if (d == 0 && ms == 0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"00000000000000000\"");
        }
        int hh = ms / 3600000;
        int mm = (ms % 3600000) / 60000;
        int ss = (ms % 60000) / 1000;
        int fff = ms % 1000;
        return snprintf(buf, buflen, "\"%08d%02d%02d%02d%03d\"", d, hh, mm, ss, fff);
    }
    case FT_TIME: {
        uint32_t secs = ((uint32_t)data[0] << 16) | ((uint32_t)data[1] << 8) | data[2];
        if (secs == 0 && data[0]==0 && data[1]==0 && data[2]==0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"00:00:00\"");
        }
        int hh = secs / 3600;
        int mm = (secs % 3600) / 60;
        int ss = secs % 60;
        return snprintf(buf, buflen, "\"%02d:%02d:%02d\"", hh, mm, ss); /* "HH:MM:SS" */
    }
    case FT_UUID: {
        /* Emit JSON-quoted canonical form ("xxxxxxxx-...-xxxxxxxxxxxx").
           All-zero bytes = unset; skip empty. */
        const uint8_t *b = data;
        if (uuid_is_zero(b)) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen,
                            "\"00000000-0000-0000-0000-000000000000\"");
        }
        if (buflen < 39) return 0; /* 36 hex + 2 quotes + NUL */
        char inner[37];
        uuid_format_canonical(inner, sizeof(inner), b);
        return snprintf(buf, buflen, "\"%s\"", inner);
    }
    case FT_IPV4: {
        if (data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 0) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"0.0.0.0\"");
        }
        char ipstr[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, data, ipstr, sizeof(ipstr)))
            return 0;
        return snprintf(buf, buflen, "\"%s\"", ipstr);
    }
    case FT_IPV6: {
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (data[bi] != 0) { allzero = 0; break; }
        if (allzero) {
            if (!render_zeros) return 0;
            return snprintf(buf, buflen, "\"::\"");
        }
        char ipstr[INET6_ADDRSTRLEN];
        if (!inet_ntop(AF_INET6, data, ipstr, sizeof(ipstr)))
            return 0;
        return snprintf(buf, buflen, "\"%s\"", ipstr);
    }
    case FT_ENUM: {
        /* Stored bytes are the byte index (1 or 2 BE). Always emit
           (byte 0 is a legit enum index, not "unset"). Output is a
           JSON-quoted string from enum_values[idx]. Out-of-range
           index (data corruption) → empty string. */
        if (!f->enum_values || f->n_enum_values <= 0) return 0;
        int idx = (f->enum_width == 2)
                    ? (int)(((uint16_t)data[0] << 8) | (uint16_t)data[1])
                    : (int)data[0];
        if (idx < 0 || idx >= f->n_enum_values) return snprintf(buf, buflen, "\"\"");
        const char *s = f->enum_values[idx];
        if (!s) return snprintf(buf, buflen, "\"\"");
        /* Escape per RFC 8259 — enum value strings are user-supplied
           at create-object, may carry quotes/backslashes/control chars.
           Same buffer-sizing contract as FT_VARCHAR. */
        if (buflen < 4) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: buffer too small for enum field [%s] (buflen=%d, need>=4)", f->name, buflen);
            return -1;
        }
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3, s, strlen(s));
        if (esc < 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: json_escape_into overflow for enum field [%s] value [%s]", f->name, s);
            return -1;
        }
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    default:
        return 0;
    }
}
```

### Edit 8: `build.sh` — exact old anchor and complete replacement

<!-- block:edit-8-old -->
```bash
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto

```

<!-- block:edit-8-new -->
```bash
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto

```

### Edit 9: `build.sh` — exact old anchor and complete replacement

<!-- block:edit-9-old -->
```bash
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/type_desc.c \

```

<!-- block:edit-9-new -->
```bash
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/type_desc.c \

```

### Edit 10: `build.sh` — exact old anchor and complete replacement

<!-- block:edit-10-old -->
```bash
    src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto

```

<!-- block:edit-10-new -->
```bash
    src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto

```

### Edit 11: `build.sh` — exact old anchor and complete replacement

<!-- block:edit-11-old -->
```bash
    src/db/decnum.c \
    src/db/packed_record.c \
    src/test/test_client.c \

```

<!-- block:edit-11-new -->
```bash
    src/db/decnum.c \
    src/db/packed_record.c \
    src/db/typed_row.c \
    src/test/test_client.c \

```

### Edit 12: `build.sh` — exact old anchor and complete replacement

<!-- block:edit-12-old -->
```bash
gcc $MODE_CFLAGS -o shard-db-bench \
    src/bench/shard-db-bench.c \
    src/bench/bench_common.c \
    src/bench/bench_stats.c \
    src/bench/bench_table.c \
    src/bench/bench_kv.c \
    src/bench/bench_kv_parallel.c \
    src/bench/bench_grow.c \
    src/bench/bench_invoice.c \
    src/bench/bench_parallel.c \
    src/bench/bench_queries.c \
    src/bench/bench_cache_pollution.c \
    src/bench/bench_joins.c \
    src/bench/bench_incremental.c \
    src/bench/bench_btree.c \
    src/bench/bench_bitmap_vs_btree.c \
    src/bench/bench_trigram_sizing.c \
    src/test/test_client.c \
    src/test/test_runner.c \
    src/test/fixtures.c \
    src/db/util.c \
    src/db/durability.c \
    src/db/durability_epoch.c \
    src/db/parallel.c \
    src/db/storage.c \
    src/db/index.c \
    src/db/keyset.c \
    src/db/btree.c \
    src/db/bitmap.c \
    src/db/trigram.c \
    src/db/objlock.c \
    src/db/tls.c \
    src/db/slotcask.c \
    src/db/simd.c \
    src/db/io_direct.c \
    src/db/query.c \
    src/db/query_aggregate.c \
    src/db/query_join.c \
    src/db/query_plan.c \
    src/db/query_maint.c \
    src/db/query_schema.c \
    src/db/query_bulk.c \
    src/db/query_find.c \
    src/db/server.c \
    src/db/config.c
```

<!-- block:edit-12-new -->
```bash
gcc $MODE_CFLAGS -o shard-db-bench \
    src/bench/shard-db-bench.c \
    src/bench/bench_common.c \
    src/bench/bench_stats.c \
    src/bench/bench_table.c \
    src/bench/bench_kv.c \
    src/bench/bench_kv_parallel.c \
    src/bench/bench_grow.c \
    src/bench/bench_invoice.c \
    src/bench/bench_parallel.c \
    src/bench/bench_queries.c \
    src/bench/bench_cache_pollution.c \
    src/bench/bench_joins.c \
    src/bench/bench_incremental.c \
    src/bench/bench_btree.c \
    src/bench/bench_bitmap_vs_btree.c \
    src/bench/bench_trigram_sizing.c \
    src/test/test_client.c \
    src/test/test_runner.c \
    src/test/fixtures.c \
    src/db/util.c \
    src/db/durability.c \
    src/db/durability_epoch.c \
    src/db/parallel.c \
    src/db/storage.c \
    src/db/index.c \
    src/db/keyset.c \
    src/db/btree.c \
    src/db/bitmap.c \
    src/db/trigram.c \
    src/db/objlock.c \
    src/db/tls.c \
    src/db/slotcask.c \
    src/db/simd.c \
    src/db/io_direct.c \
    src/db/query.c \
    src/db/query_aggregate.c \
    src/db/query_join.c \
    src/db/query_plan.c \
    src/db/query_maint.c \
    src/db/query_schema.c \
    src/db/query_bulk.c \
    src/db/query_find.c \
    src/db/server.c \
    src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c
```

### Edit 13: `build.sh` — exact old anchor and complete replacement

<!-- block:edit-13-old -->
```bash
    src/test/cases/test_packed_record_storage.c \
```

<!-- block:edit-13-new -->
```bash
    src/test/cases/test_packed_record_storage.c \
    src/test/cases/test_typed_row_adapter.c \
```

## Appendix C — complete new header, tests and staged module files

<!-- block:row-header -->
```c
#ifndef TYPED_ROW_H
#define TYPED_ROW_H
#include "types.h"
#include "slotcask.h"

/* Internal prepared schema/record adapters; never publish an object layout.
 * No loader/create-object calls the binder in this child. Bound packed schemas
 * may only reach these adapters/packed_record APIs until consumers are converted;
 * old typed_* and raw-offset APIs cannot interpret packed fields.
 * Schemas, enum strings and OLD bytes remain immutable while views/prepared
 * cells borrow them. Initialize prepared objects before use; free is idempotent.
 * Outputs and inputs must not alias. Owned output arguments must be valid;
 * callers release prior owned outputs before reuse. A successful *_alloc or
 * field/decode result belongs to the caller and is released with free(). Valid
 * owned output arguments are reset to NULL/0 on failure; field NULL is indicated
 * by a NULL return with errno=0. Other failures set errno.
 * set() is failure-atomic (cell, owner, seen, byte count). defaults()/stamp()
 * are incremental: discard/free the prepared object on failure; generators are
 * never rolled back. zero()/old() preserve prior state on validation failure,
 * then replace it; later errors leave an empty free-safe object. Allocation,
 * measurement and composition never rerun defaults/sequence/random generation.
 * Actual lengths do not prove row/key fit. The write integration must call
 * pkr_record_fits with the actual key and unchanged segment cap before mutation.
 * Legacy spans keep bytes actually present; read() substitutes whole-field zero
 * for incomplete legacy fields, copy_legacy() carries partial bytes then pads.
 * The packed renderer's budget counts JSON output bytes including the NUL;
 * decimal validation/render workspaces remain independently digit-bounded.
 * JSON collection validates the complete document (including ignored fields),
 * accepts at most 64 nested containers, decodes names and byte strings, rejects
 * object/array values for known scalar fields, validates each duplicate, and
 * retains its last valid occurrence. Unknown fields are ignored after validation.
 */
typedef struct {
    const TypedSchema *schema;
    const uint8_t *value, *base, *bitmap;
    size_t len, region;
    int bitmap_bytes;
    uint8_t hdr2;
    PkrView packed;
} TypedRowView;
typedef struct {
    const uint8_t *data;
    size_t len;
    int is_null, complete;
} TypedRowSpan;
typedef struct {
    const char *data;
    size_t len;
    uint8_t supplied, is_null, quoted;
} TypedInput;
typedef struct {
    const TypedSchema *schema;
    PkrCell cells[MAX_FIELDS];
    uint8_t *owned[MAX_FIELDS];
    uint8_t seen[MAX_FIELDS];
    size_t bytes;
} TypedPrepared;

/* The schema's fields/names/defaults are already parsed. Only TYPE tokens
   are accepted. Success binds an in-memory codec; failure preserves schema.
   Legacy cache loaders do not call this function in Phase 3B. */
int typed_schema_bind_packed(TypedSchema *ts, const char *const *types,
                              size_t count, char *err, size_t errsz);
uint8_t typed_schema_hdr2(const TypedSchema *ts);
int typed_row_open(const TypedSchema *ts, const uint8_t *value, size_t len,
                    uint8_t hdr2, TypedRowView *out);
int typed_row_span(const TypedRowView *view, size_t index, TypedRowSpan *out);
/* Explicit legacy materialization contracts: read requires a complete field;
   carry copies available bytes into an exact-width zeroed destination. */
int typed_span_read(const TypedSchema *ts, size_t index,
                      const TypedRowSpan *span, const uint8_t **out);
int typed_span_copy_legacy(const TypedSchema *ts, size_t index,
                             const TypedRowSpan *span, uint8_t *out, size_t cap);

void typed_prepared_init(TypedPrepared *p);
void typed_prepared_free(TypedPrepared *p);
int typed_prepared_zero(const TypedSchema *ts, TypedPrepared *p,
                         char *err, size_t errsz);
int typed_prepared_old(const TypedSchema *ts, const SlotcaskOldRecord *old,
                        TypedPrepared *p, char *err, size_t errsz);
int typed_prepared_set(TypedPrepared *p, size_t index, const TypedInput *input,
                        char *err, size_t errsz);
int typed_prepared_defaults(TypedPrepared *p, const char *root,
                             const char *object, char *err, size_t errsz);
int typed_prepared_stamp(TypedPrepared *p, enum DefaultKind kind,
                          char *err, size_t errsz);
int typed_prepared_compose(const TypedPrepared *p, uint8_t *out, size_t cap,
                            size_t *used, char *err, size_t errsz);
int typed_prepared_alloc(const TypedPrepared *p, uint8_t **out, size_t *len,
                          char *err, size_t errsz);
int typed_encode_alloc(const TypedSchema *ts, const char *json, int defaults,
                         const char *root, const char *object,
                         uint8_t **out, size_t *len, char *err, size_t errsz);
int typed_encode_packed_into(const TypedSchema *ts, const char *json, int defaults,
                              const char *root, const char *object,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz);
int typed_update_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                         const TypedInput *inputs, int stamp,
                         uint8_t **out, size_t *len, char *err, size_t errsz);
int typed_transfer_ac_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                             const uint8_t *value, size_t vlen,
                             uint8_t **out, size_t *len,
                             char *err, size_t errsz);
char *typed_packed_decode(const TypedSchema *ts, const uint8_t *value, size_t len,
                           uint8_t hdr2, size_t budget);
char *typed_packed_field_text(const TypedSchema *ts, const uint8_t *value,
                               size_t len, uint8_t hdr2, size_t index,
                               size_t *text_len);

/* Existing scalar semantics, shared internally by the adapter. */
int encode_json_field_checked(const TypedField *f, const char *text, size_t len,
                                int quoted, uint8_t *out, char *err, size_t errsz);
int sentinel_span_parses(enum FieldType type, const char *value, size_t len);
const char *generate_default(const TypedField *f, char *buf, size_t cap,
                              const char *root, const char *object);
int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                         char *buf, int cap, int render_zeros);
#endif

```

<!-- block:row-tests -->
```c
#include "test_runner.h"
#include "test_assert.h"
#include "typed_row.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>

static TypedSchema *schema_new(const char *const *types,size_t n){
    TypedSchema *ts=calloc(1,sizeof(*ts));if(!ts)return NULL;
    ts->typed=1;ts->nfields=(int)n;ts->nullmap_prefix=1+typed_nullmap_bytes((int)n);
    for(size_t i=0;i<n;i++){
        if(!strncmp(types[i],"varchar",7))ts->fields[i].type=FT_VARCHAR;
        else if(!strncmp(types[i],"numeric",7)||!strcmp(types[i],"currency"))
            ts->fields[i].type=FT_NUMERIC;
        else parse_field_type(types[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%zu",i);
        ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    if(typed_schema_bind_packed(ts,types,n,NULL,0)){
        for(size_t i=0;i<n;i++)free_enum_values(&ts->fields[i]);
        free(ts);return NULL;
    }
    return ts;
}
static void schema_free(TypedSchema *ts){
    if(!ts)return;
    for(int i=0;i<ts->nfields;i++)free_enum_values(&ts->fields[i]);
    free(ts);
}
static void encoding(void){
    const char *types[]={"numeric:26,10","varchar","int","currency","numeric:2,-2"};
    TypedSchema *ts=schema_new(types,5);ASSERT_NOT_NULL(ts,"explicit packed schema");
    if(!ts)return;
    uint8_t *out=NULL;size_t n=0;char err[256];
    const char *json="{\"f4\":9949,\"f1\":\"abc\",\"f0\":\"1234567890123456.1234567890\",\"f2\":7,\"f3\":\"12.34565\"}";
    ASSERT_EQ_INT(typed_encode_alloc(ts,json,1,NULL,NULL,&out,&n,err,sizeof(err)),0,"reverse JSON schema-order encoding");
    ASSERT_TRUE(n<(size_t)ts->total_size,"actual allocation below maximum");
    if(out){
        char *text=typed_packed_decode(ts,out,n,PKR_FLAGS,1024*1024);
        ASSERT_NOT_NULL(text,"exact packed render");
        if(text){
            ASSERT_CONTAINS(text,"1234567890123456.1234567890","full decimal value");
            ASSERT_CONTAINS(text,"\"f1\":\"abc\"","raw varchar rendered");
            ASSERT_CONTAINS(text,"\"f2\":7","fixed int directory");
            ASSERT_CONTAINS(text,"\"f3\":12.3457","currency precision");
            ASSERT_CONTAINS(text,"\"f4\":9900","negative scale");
            free(text);
        }
        TypedRowView view;ASSERT_EQ_INT(typed_row_open(ts,out,n,1,&view),-1,"packed schema rejects legacy generation");
        ASSERT_EQ_INT(typed_row_open(ts,out,n,PKR_FLAGS,&view),0,"packed schema view");
        TypedRowSpan span;ASSERT_EQ_INT(typed_row_span(&view,1,&span),0,"varchar span");
        ASSERT_EQ_INT(span.len,3,"content span no uint16 prefix");
        if(span.data)ASSERT_EQ_INT(memcmp(span.data,"abc",3),0,"content exact");
        ASSERT_EQ_INT(typed_schema_hdr2(ts),PKR_FLAGS,"packed output generation");
        errno=0;text=typed_packed_decode(ts,out,n,PKR_FLAGS,8);
        ASSERT_TRUE(!text&&errno==ENOMEM,"output budget error");
        free(out);
    }
    out=(void*)1;n=999;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"\"}",1,NULL,NULL,&out,&n,err,sizeof(err)),-1,"empty numeric rejects");
    ASSERT_TRUE(!out&&!n,"failed owned output reset");
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":0,\"f1\":\"\"}",1,NULL,NULL,&out,&n,err,sizeof(err)),0,"zero empty null distinct");
    if(out){
        TypedRowView v;TypedRowSpan a,b,c;ASSERT_EQ_INT(typed_row_open(ts,out,n,PKR_FLAGS,&v),0,"zero empty view");
        ASSERT_EQ_INT(typed_row_span(&v,0,&a),0,"zero span");ASSERT_TRUE(!a.is_null&&a.len>=16,"zero actual decimal payload");
        ASSERT_EQ_INT(typed_row_span(&v,1,&b),0,"empty span");ASSERT_TRUE(!b.is_null&&b.len==0&&b.data,"present empty");
        ASSERT_EQ_INT(typed_row_span(&v,2,&c),0,"omitted span");ASSERT_TRUE(c.is_null,"defaults insertion omission null");
        free(out);
    }
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f2\":1,\"f2\":2}",1,NULL,NULL,&out,&n,err,sizeof(err)),0,"last valid duplicate wins");
    if(out){
        size_t len=0;char *s=typed_packed_field_text(ts,out,n,PKR_FLAGS,2,&len);
        ASSERT_NOT_NULL(s,"duplicate display");if(s){ASSERT_EQ_STR(s,"2","duplicate result");free(s);}
        free(out);
    }
    ts->fields[0].default_kind=DK_LITERAL;strcpy(ts->fields[0].default_val,"1.25");
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{}",1,NULL,NULL,&out,&n,err,sizeof(err)),0,"literal default");
    if(out){
        size_t len=0;char *s=typed_packed_field_text(ts,out,n,PKR_FLAGS,0,&len);
        ASSERT_NOT_NULL(s,"default text");if(s){ASSERT_EQ_STR(s,"1.2500000000","default scale exact");free(s);}free(out);
    }
    ts->fields[1].not_null=1;out=(void*)1;n=99;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{}",1,NULL,NULL,&out,&n,err,sizeof(err)),-1,"omission not null");
    ASSERT_TRUE(!out&&!n,"not-null failure outputs reset");
    ts->fields[1].not_null=0;
    schema_free(ts);
}
static void updates_and_reuse(void){
    const char *types[]={"varchar","numeric:3,1","int"};
    TypedSchema *ts=schema_new(types,3);ASSERT_NOT_NULL(ts,"update schema");if(!ts)return;
    uint8_t *old=NULL;size_t oldn=0;char err[256];
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"old\",\"f1\":\"1.25\",\"f2\":7}",1,NULL,NULL,&old,&oldn,err,sizeof(err)),0,"update seed");
    if(!old){schema_free(ts);return;}
    SlotcaskOldRecord record={old,oldn,PKR_FLAGS};
    TypedInput inputs[MAX_FIELDS]={0};inputs[0]=(TypedInput){"much longer",11,1,0,0};
    uint8_t *next=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_update_alloc(ts,&record,inputs,0,&next,&n,err,sizeof(err)),0,"growing update actual allocation");
    if(next){
        char *text=typed_packed_decode(ts,next,n,PKR_FLAGS,4096);
        ASSERT_NOT_NULL(text,"updated text");if(text){
            ASSERT_CONTAINS(text,"much longer","new varchar");
            ASSERT_CONTAINS(text,"\"f1\":1.3","omitted exact decimal carried");
            ASSERT_CONTAINS(text,"\"f2\":7","omitted fixed carried");free(text);}
        free(next);
    }
    inputs[1]=(TypedInput){"99.95",5,1,0,0};next=(void*)1;n=99;
    ASSERT_EQ_INT(typed_update_alloc(ts,&record,inputs,0,&next,&n,err,sizeof(err)),-1,"precision overflow rejects update");
    ASSERT_TRUE(!next&&!n,"failed update owned output clear");
    char *before=typed_packed_decode(ts,old,oldn,PKR_FLAGS,4096);
    ASSERT_NOT_NULL(before,"old still decodes");if(before){
        ASSERT_CONTAINS(before,"\"f0\":\"old\"","failed update did not change OLD");free(before);}
    TypedPrepared p;typed_prepared_init(&p);
    ASSERT_EQ_INT(typed_prepared_old(ts,&record,&p,err,sizeof(err)),0,"borrow OLD prepared");
    size_t cap=p.bytes;uint8_t *buf=malloc(cap+8);ASSERT_NOT_NULL(buf,"actual capacity canary");
    if(buf){memset(buf,0xA5,cap+8);size_t used=99;
        ASSERT_EQ_INT(typed_prepared_compose(&p,buf,cap-1,&used,err,sizeof(err)),-1,"short destination");
        ASSERT_EQ_INT(used,0,"short used reset");int same=1;
        for(size_t i=0;i<cap+8;i++)if(buf[i]!=0xA5)same=0;
        ASSERT_TRUE(same,"failed compose untouched");
        ASSERT_EQ_INT(typed_prepared_compose(&p,buf,cap,&used,err,sizeof(err)),0,"actual capacity accepts");
        same=1;for(size_t i=cap;i<cap+8;i++)if(buf[i]!=0xA5)same=0;
        ASSERT_TRUE(same,"tail canary untouched");free(buf);}
    typed_prepared_free(&p);free(old);schema_free(ts);
}
static void legacy_views(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"legacy schema");if(!ts)return;
    ts->typed=1;ts->nfields=2;ts->nullmap_prefix=2;ts->total_size=10;
    parse_field_type("int",&ts->fields[0]);ts->fields[0].offset=0;
    parse_field_type("date",&ts->fields[1]);ts->fields[1].offset=4;
    uint8_t trimmed[]={1};TypedRowView view;
    ASSERT_EQ_INT(typed_row_open(ts,trimmed,sizeof(trimmed),0,&view),0,"legacy trimmed generation");
    TypedRowSpan span;ASSERT_EQ_INT(typed_row_span(&view,0,&span),0,"legacy partial span");
    ASSERT_EQ_INT(span.len,1,"actual stored partial bytes");
    const uint8_t *read=NULL;ASSERT_EQ_INT(typed_span_read(ts,0,&span,&read),0,"legacy reader zero substitute");
    if(read)ASSERT_EQ_INT(read[0],0,"reader requires whole field");
    uint8_t carried[4]={0xA5,0xA5,0xA5,0xA5};ASSERT_EQ_INT(typed_span_copy_legacy(ts,0,&span,carried,sizeof(carried)),0,"legacy carry materialization");
    ASSERT_EQ_INT(carried[0],1,"carry preserves partial leading byte");
    ASSERT_EQ_INT(carried[1],0,"carry zero fills suffix");
    ASSERT_EQ_INT(typed_row_open(ts,trimmed,sizeof(trimmed),PKR_FLAGS,&view),-1,"legacy schema rejects packed generation");
    free(ts);
}
static void maximum_and_binary(void){
    const char *types[MAX_FIELDS];for(int i=0;i<MAX_FIELDS;i++)types[i]="varchar";
    TypedSchema *ts=schema_new(types,MAX_FIELDS);ASSERT_NOT_NULL(ts,"maximum packed schema");if(!ts)return;
    ASSERT_EQ_INT(ts->total_size,1073743905,"schema maximum remains bound");
    TypedPrepared p;typed_prepared_init(&p);char err[256];
    ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"maximum schema prepare");
    uint8_t binary[]={'a',0,'b'};TypedInput in={(const char*)binary,sizeof(binary),1,0,0};
    ASSERT_EQ_INT(typed_prepared_set(&p,127,&in,err,sizeof(err)),0,"raw CSV binary span");
    uint8_t *out=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_prepared_alloc(&p,&out,&n,err,sizeof(err)),0,"maximum schema actual allocation");
    ASSERT_EQ_INT(n,2084,"actual descriptor directory plus three bytes");
    if(out){char *s=typed_packed_decode(ts,out,n,PKR_FLAGS,16384);
        ASSERT_NOT_NULL(s,"binary JSON full byte span");if(s){ASSERT_CONTAINS(s,"a\\u0000b","NUL not silently dropped");free(s);}free(out);}
    typed_prepared_free(&p);schema_free(ts);
}

static void review_regressions(void){
    char err[256];const char *types[]={"varchar","ipv4","uuid"};
    TypedSchema *ts=schema_new(types,3);ASSERT_NOT_NULL(ts,"review schema");if(!ts)return;
    uint8_t *out=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"a\\u0000b\",\"f1\":\"\\u0031.2.3.4\",\"f2\":\"\\u00300000000-0000-0000-0000-000000000001\"}",1,NULL,NULL,&out,&n,err,sizeof(err)),0,"decode string before sentinel validation");
    if(out){
        char *text=typed_packed_decode(ts,out,n,PKR_FLAGS,4096);
        ASSERT_NOT_NULL(text,"escaped binary render");if(text){
            ASSERT_CONTAINS(text,"a\\u0000b","binary JSON round trip");
            ASSERT_CONTAINS(text,"1.2.3.4","escaped IP stays non-null");
            ASSERT_CONTAINS(text,"00000000-0000-0000-0000-000000000001","escaped UUID stays non-null");
            size_t exact=strlen(text)+1;
            char *fit=typed_packed_decode(ts,out,n,PKR_FLAGS,exact);
            ASSERT_NOT_NULL(fit,"exact rendering budget");free(fit);
            errno=0;fit=typed_packed_decode(ts,out,n,PKR_FLAGS,exact-1);
            ASSERT_TRUE(!fit&&errno==ENOMEM,"one short output budget");free(fit);free(text);
        }
        size_t rawlen=0;char *raw=typed_packed_field_text(ts,out,n,PKR_FLAGS,0,&rawlen);
        ASSERT_NOT_NULL(raw,"binary projection owns span");
        if(raw){ASSERT_EQ_INT(rawlen,3,"binary projection length");
            ASSERT_TRUE(raw[0]=='a'&&raw[1]==0&&raw[2]=='b',"projection retains NUL");free(raw);}
        for(int i=0;i<3;i++)ts->fields[i].removed=1;
        errno=0;text=typed_packed_decode(ts,out,n,PKR_FLAGS,0);
        ASSERT_TRUE(!text&&errno==ENOMEM,"empty object respects budget");
        text=typed_packed_decode(ts,out,n,PKR_FLAGS,3);
        ASSERT_NOT_NULL(text,"empty object exact budget");if(text){ASSERT_EQ_STR(text,"{}","removed fields omitted");free(text);}
        free(out);for(int i=0;i<3;i++)ts->fields[i].removed=0;
    }
    ts->fields[0].default_kind=DK_LITERAL;strcpy(ts->fields[0].default_val,"fallback");
    TypedPrepared p;typed_prepared_init(&p);
    ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"prepare failed-set fixture");
    TypedInput bad={"\\q",2,1,0,1};
    ASSERT_EQ_INT(typed_prepared_set(&p,0,&bad,err,sizeof(err)),-1,"invalid escape fails assignment");
    ASSERT_EQ_INT(p.seen[0],0,"failed setter preserves seen");
    ASSERT_EQ_INT(typed_prepared_defaults(&p,NULL,NULL,err,sizeof(err)),0,"defaults after rejected set");
    ASSERT_EQ_INT(typed_prepared_alloc(&p,&out,&n,err,sizeof(err)),0,"compose after rejected set");
    if(out){size_t nn=0;char *text=typed_packed_field_text(ts,out,n,PKR_FLAGS,0,&nn);
        ASSERT_NOT_NULL(text,"default not suppressed");if(text){ASSERT_EQ_STR(text,"fallback","failed set leaves default eligible");free(text);}free(out);}
    typed_prepared_free(&p);
    strcpy(ts->fields[0].name,"x\"\\\n");ts->fields[0].name_len=4;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{}",0,NULL,NULL,&out,&n,err,sizeof(err)),0,"escaped field-name fixture");
    if(out){char *text=typed_packed_decode(ts,out,n,PKR_FLAGS,4096);
        ASSERT_NOT_NULL(text,"escaped name renderer");if(text){ASSERT_CONTAINS(text,"\"x\\\"\\\\\\n\":","field name escaped");free(text);}free(out);}
    schema_free(ts);
}

static void schema_and_failure_contracts(void){
    const char *types[]={"numeric","varchar:3","int"};char err[256];
    TypedSchema *ts=schema_new(types,3);ASSERT_NOT_NULL(ts,"failure schema");if(!ts)return;
    TypedSchema *snapshot=malloc(sizeof(*snapshot));ASSERT_NOT_NULL(snapshot,"schema snapshot");
    if(snapshot){memcpy(snapshot,ts,sizeof(*snapshot));
        const char *bad[]={"numeric","varchar:4194305","int"};
        ASSERT_EQ_INT(typed_schema_bind_packed(ts,bad,3,err,sizeof(err)),-1,"bad declaration rejects");
        ASSERT_EQ_INT(memcmp(snapshot,ts,sizeof(*ts)),0,"binding failure preserves schema");free(snapshot);}
    uint8_t *row=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f1\":\"abcd\"}",0,NULL,NULL,&row,&n,err,sizeof(err)),-1,"varchar cap no truncation");
    ASSERT_TRUE(!row&&!n,"cap error owned outputs clear");
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":1,\"f0\":\"bad\"}",0,NULL,NULL,&row,&n,err,sizeof(err)),-1,"every duplicate must validate");
    ASSERT_TRUE(!row&&!n,"duplicate failure outputs clear");
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"unknown\":{\"ignored\":[1,2]},\"f2\":3}",0,NULL,NULL,&row,&n,err,sizeof(err)),0,"unknown nested value ignored");
    if(row){
        TypedRowView v;ASSERT_EQ_INT(typed_row_open(ts,row,n,PKR_FLAGS,&v),0,"valid row before corruption");
        size_t descriptor=(size_t)ts->packed.prefix+ts->packed.fields[1].offset;
        uint8_t saved=row[descriptor];row[descriptor]=0xFF;
        ASSERT_EQ_INT(typed_row_open(ts,row,n,PKR_FLAGS,&v),-1,"bad descriptor rejected");
        ASSERT_TRUE(!v.schema&&!v.value,"invalid view cleared");
        row[descriptor]=saved;
        TypedPrepared prep;typed_prepared_init(&prep);SlotcaskOldRecord old={row,n,PKR_FLAGS};
        ASSERT_EQ_INT(typed_prepared_old(ts,&old,&prep,err,sizeof(err)),0,"reuse old fixture");
        TypedInput invalid={"bad",3,1,0,0};
        ASSERT_EQ_INT(typed_prepared_set(&prep,0,&invalid,err,sizeof(err)),-1,"failed setter retains cell");
        uint8_t *after=NULL;size_t an=0;
        ASSERT_EQ_INT(typed_prepared_alloc(&prep,&after,&an,err,sizeof(err)),0,"retained cells still compose");
        if(after){ASSERT_EQ_INT(an,n,"failed setter retained size");
            ASSERT_EQ_INT(memcmp(after,row,n),0,"failed setter retained bytes");free(after);}
        typed_prepared_free(&prep);typed_prepared_free(&prep);
        ASSERT_TRUE(!prep.schema&&!prep.bytes,"idempotent prepared free");free(row);
    }
    schema_free(ts);
}
static void decimal_limits_and_types(void){
    const char *types[]={"numeric","enum(red,blue)","float","double","timestamp"};
    TypedSchema *ts=schema_new(types,5);ASSERT_NOT_NULL(ts,"bare decimal schema");if(!ts)return;
    size_t digits=131072+1+16383;char *number=malloc(digits+1);ASSERT_NOT_NULL(number,"maximum legal numeric text");
    if(number){memset(number,'9',131072);number[131072]='.';memset(number+131073,'1',16383);number[digits]=0;
        TypedPrepared p;typed_prepared_init(&p);char err[256];
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"prepare maximum decimal");
        TypedInput in={number,digits,1,0,0};
        ASSERT_EQ_INT(typed_prepared_set(&p,0,&in,err,sizeof(err)),0,"full PostgreSQL-sized bare numeric accepted");
        uint8_t *row=NULL;size_t n=0;
        ASSERT_EQ_INT(typed_prepared_alloc(&p,&row,&n,err,sizeof(err)),0,"maximum numeric actual payload");
        if(row){size_t got=0;char *text=typed_packed_field_text(ts,row,n,PKR_FLAGS,0,&got);
            ASSERT_NOT_NULL(text,"maximum numeric exact text");if(text){ASSERT_EQ_INT(got,digits,"maximum text length");
                ASSERT_EQ_INT(memcmp(text,number,digits),0,"all maximum digits survive");free(text);}free(row);}
        typed_prepared_free(&p);free(number);
    }
    char err[256];uint8_t *row=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"1.0e2\",\"f1\":\"blue\",\"f2\":1.23456776,\"f3\":0.10000000000000001,\"f4\":0}",1,NULL,NULL,&row,&n,err,sizeof(err)),0,"fixed scalars share checked core");
    if(row){char *text=typed_packed_decode(ts,row,n,PKR_FLAGS,4096);ASSERT_NOT_NULL(text,"fixed and exponent render");
        if(text){ASSERT_CONTAINS(text,"\"f0\":100","exponent canonical scale");
            ASSERT_CONTAINS(text,"\"f1\":\"blue\"","enum directory value");
            ASSERT_CONTAINS(text,"1.23456776","float nine digits");
            ASSERT_CONTAINS(text,"0.10000000000000001","double seventeen digits");
            ASSERT_CONTAINS(text,"\"f4\":0","zero timestamp is value under bitmap");free(text);}free(row);}
    schema_free(ts);
}

static void json_contracts(void){
    const char *types[]={"varchar"};char err[256];TypedSchema *ts=schema_new(types,1);
    ASSERT_NOT_NULL(ts,"JSON schema");if(!ts)return;
    const char *bad[]={"{\"f0\":NaN}","{\"f0\":[1,,2]}","{\"unknown\":{\"x\":}}",
        "{\"unknown\":\"\\q\"}","{\"f0\":\"raw\nnewline\"}","{\"f0\":01}",
        "{\"f0\":1.}","{\"f0\":1e}","{\"f0\":true,}","{} trailing",
        "{\"f0\":{\"$copy\":\"f0\"}}","{\"f0\":[1,2]}"};
    for(size_t i=0;i<sizeof(bad)/sizeof(bad[0]);i++){
        uint8_t *row=(void*)1;size_t n=99;
        ASSERT_EQ_INT(typed_encode_alloc(ts,bad[i],1,NULL,NULL,&row,&n,err,sizeof(err)),-1,"malformed or non-scalar known value rejects");
        ASSERT_TRUE(!row&&!n,"JSON failure clears owned outputs");
    }
    uint8_t *row=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"\\u00660\":\"value\"}",1,NULL,NULL,&row,&n,err,sizeof(err)),0,"escaped key matches schema");
    if(row){char *text=typed_packed_decode(ts,row,n,PKR_FLAGS,1024);ASSERT_NOT_NULL(text,"escaped key render");
        if(text){ASSERT_EQ_STR(text,"{\"f0\":\"value\"}","decoded key retains supplied value");free(text);}free(row);}

    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":null\t}",1,NULL,NULL,&row,&n,err,sizeof(err)),0,"tab after primitive null accepts");
    if(row){size_t got=99;errno=123;char *text=typed_packed_field_text(ts,row,n,PKR_FLAGS,0,&got);
        ASSERT_TRUE(!text&&got==0&&errno==0,"tab-terminated null stays NULL");free(text);free(row);}
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":123\r}",1,NULL,NULL,&row,&n,err,sizeof(err)),0,"CR after primitive accepts");
    if(row){size_t got=0;char *text=typed_packed_field_text(ts,row,n,PKR_FLAGS,0,&got);
        ASSERT_NOT_NULL(text,"primitive text with CR delimiter");if(text){ASSERT_EQ_STR(text,"123","JSON whitespace not field content");free(text);}free(row);}
    strcpy(ts->fields[0].name,"x\"y");ts->fields[0].name_len=3;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"x\\\"y\":\"value\"}",1,NULL,NULL,&row,&n,err,sizeof(err)),0,"escaped name consumes own JSON");
    if(row){char *text=typed_packed_decode(ts,row,n,PKR_FLAGS,1024);ASSERT_NOT_NULL(text,"round-trip escaped name");
        if(text){uint8_t *round=NULL;size_t rn=0;
            ASSERT_EQ_INT(typed_encode_alloc(ts,text,1,NULL,NULL,&round,&rn,err,sizeof(err)),0,"rendered JSON can be read");
            if(round){ASSERT_EQ_INT(rn,n,"round-trip name size");ASSERT_EQ_INT(memcmp(round,row,n),0,"round-trip name bytes");free(round);}free(text);}free(row);}
    char depth[512];size_t pos=0;memcpy(depth,"{\"unknown\":",11);pos=11;
    for(int i=0;i<63;i++)depth[pos++]='[';
    depth[pos++]='0';
    for(int i=0;i<63;i++)depth[pos++]=']';
    depth[pos++]='}';depth[pos]=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,depth,1,NULL,NULL,&row,&n,err,sizeof(err)),0,"64-container JSON depth accepts");free(row);
    memmove(depth+12,depth+11,pos-11+1);depth[11]='[';pos++;
    depth[pos-1]=']';depth[pos++]='}';depth[pos]=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,depth,1,NULL,NULL,&row,&n,err,sizeof(err)),-1,"65-container JSON depth rejects");
    schema_free(ts);
}

static void automatic_fields(void){
    const char *types[]={"varchar","timestamp","int"};char err[256];
    TypedSchema *ts=schema_new(types,3);ASSERT_NOT_NULL(ts,"automatic field schema");if(!ts)return;
    ts->fields[0].default_kind=DK_AUTO_CREATE;ts->fields[1].default_kind=DK_AUTO_UPDATE;
    uint8_t *old=NULL,*incoming=NULL;size_t oldn=0,incomingn=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"born\",\"f1\":1,\"f2\":5}",0,NULL,NULL,&old,&oldn,err,sizeof(err)),0,"automatic OLD seed");
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"overwrite\",\"f1\":2,\"f2\":6}",0,NULL,NULL,&incoming,&incomingn,err,sizeof(err)),0,"automatic incoming seed");
    if(old&&incoming){
        SlotcaskOldRecord record={old,oldn,PKR_FLAGS};uint8_t *out=NULL;size_t n=0;
        ASSERT_EQ_INT(typed_transfer_ac_alloc(ts,&record,incoming,incomingn,&out,&n,err,sizeof(err)),0,"auto-create carries OLD before composition");
        if(out){size_t len=0;char *text=typed_packed_field_text(ts,out,n,PKR_FLAGS,0,&len);
            ASSERT_NOT_NULL(text,"carried auto-create text");if(text){ASSERT_EQ_STR(text,"born","supplied overwrite ignored");free(text);}free(out);}
        ASSERT_EQ_INT(typed_transfer_ac_alloc(ts,NULL,incoming,incomingn,&out,&n,err,sizeof(err)),0,"fresh auto-create stamp actual size");
        if(out){size_t len=0;char *text=typed_packed_field_text(ts,out,n,PKR_FLAGS,0,&len);
            ASSERT_NOT_NULL(text,"fresh auto-create text");if(text){ASSERT_EQ_INT(len,14,"varchar auto timestamp canonical length");free(text);}free(out);}
        ASSERT_EQ_INT(typed_update_alloc(ts,&record,NULL,1,&out,&n,err,sizeof(err)),0,"auto-update stamp before compose");
        if(out){size_t len=0;char *text=typed_packed_field_text(ts,out,n,PKR_FLAGS,1,&len);
            ASSERT_NOT_NULL(text,"auto-update timestamp text");if(text){ASSERT_TRUE(strtoll(text,NULL,10)>1,"timestamp refreshed without clock deadline");free(text);}free(out);}
        TypedPrepared prep;typed_prepared_init(&prep);
        ASSERT_EQ_INT(typed_prepared_old(ts,&record,&prep,err,sizeof(err)),0,"stamp validation fixture");
        ASSERT_EQ_INT(typed_prepared_stamp(&prep,DK_SEQ,err,sizeof(err)),-1,"timestamp API refuses arbitrary generator kinds");
        typed_prepared_free(&prep);
    }
    free(old);free(incoming);schema_free(ts);
}

static void blind_review_boundaries(void){
    char err[256];uint8_t *out=NULL;size_t len=0;
    const char *types[]={"int"};TypedSchema *ts=schema_new(types,1);
    ASSERT_NOT_NULL(ts,"blind packed scalar schema");
    if(ts){
        TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"blind prepare scalar");
        TypedInput bad={"bad",3,1,0,0};errno=0;
        ASSERT_EQ_INT(typed_prepared_set(&p,0,&bad,err,sizeof(err)),-1,"invalid packed scalar fails");
        ASSERT_EQ_INT(errno,EINVAL,"packed scalar failure sets errno");
        ASSERT_EQ_INT(p.seen[0],0,"packed scalar failure retains seen");
        typed_prepared_free(&p);schema_free(ts);
    }
    const char *enums[]={"enum(red,blue)"};ts=schema_new(enums,1);
    ASSERT_NOT_NULL(ts,"blind enum schema");
    if(ts){
        TypedSchema *before=malloc(sizeof(*before));ASSERT_NOT_NULL(before,"blind enum snapshot");
        if(before){memcpy(before,ts,sizeof(*before));
            const char *bad[]={"enum(red,blue):default=bad","enum(red,blue):auto_update","enum(red,blue)junk"};
            for(size_t i=0;i<sizeof(bad)/sizeof(bad[0]);i++){
                const char *token[]={bad[i]};errno=ENOMEM;
                ASSERT_EQ_INT(typed_schema_bind_packed(ts,token,1,err,sizeof(err)),-1,"enum suffix rejected");
                ASSERT_EQ_INT(errno,EINVAL,"enum syntax failure is deterministic");
                ASSERT_EQ_INT(memcmp(ts,before,sizeof(*ts)),0,"enum suffix failure preserves schema");
            }free(before);
        }schema_free(ts);
    }
    ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"blind legacy schema");if(!ts)return;
    ts->typed=1;ts->nfields=1;ts->nullmap_prefix=2;ts->total_size=6;
    parse_field_type("int",&ts->fields[0]);strcpy(ts->fields[0].name,"f0");ts->fields[0].name_len=2;
    for(int defaults=0;defaults<=1;defaults++){
        out=(void*)1;len=99;errno=ENOMEM;
        ASSERT_TRUE(typed_encode_alloc(ts,"{\"f0\":\"bad\"}",defaults,NULL,NULL,&out,&len,err,sizeof(err))<0,"legacy scalar encoding fails");
        ASSERT_TRUE(!out&&!len,"legacy encode failure clears owned output");
        ASSERT_EQ_INT(errno,EINVAL,"legacy scalar encoding sets fresh errno");
    }
    uint8_t bytes[]={1,0,0,0,0,1};SlotcaskOldRecord old={bytes,sizeof(bytes),1};
    TypedInput in[MAX_FIELDS]={0};in[0]=(TypedInput){"bad",3,1,0,0};errno=0;
    ASSERT_EQ_INT(typed_update_alloc(ts,&old,in,0,&out,&len,err,sizeof(err)),-1,"legacy scalar update fails");
    ASSERT_TRUE(!out&&!len,"legacy update failure clears output");
    ASSERT_EQ_INT(errno,EINVAL,"legacy scalar update sets errno");
    in[0]=(TypedInput){NULL,1,1,0,0};errno=0;
    ASSERT_EQ_INT(typed_update_alloc(ts,&old,in,0,&out,&len,err,sizeof(err)),-1,"invalid legacy span fails");
    ASSERT_EQ_INT(errno,EINVAL,"invalid legacy span sets errno");
    uint8_t broken[]={0};SlotcaskOldRecord malformed={broken,sizeof(broken),1};errno=0;
    ASSERT_EQ_INT(typed_update_alloc(ts,&malformed,NULL,0,&out,&len,err,sizeof(err)),-1,"malformed legacy OLD fails");
    ASSERT_TRUE(!out&&!len,"malformed OLD clears output");
    ASSERT_EQ_INT(errno,EILSEQ,"malformed OLD reports corruption");
    /* Keep a bad-input crash isolated from the runner. No global fixture or
       environment is changed; the child touches only this inherited schema. */
    pid_t child=fork();
    if(child!=0)ASSERT_TRUE(child>=0,"fork NULL JSON boundary probe");
    if(child==0){
        alarm(5);
        for(int defaults=0;defaults<=1;defaults++){
            uint8_t *value=(void*)1;size_t n=99;errno=0;
            int rc=typed_encode_alloc(ts,NULL,defaults,NULL,NULL,&value,&n,NULL,0);
            if(rc>=0||errno!=EINVAL||value||n)_exit(1);
        }
        _exit(0);
    }
    if(child>0){int status=0;pid_t got=waitpid(child,&status,0);
        ASSERT_TRUE(got==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0,"NULL JSON rejects without crashing in both modes");}
    free(ts);
}
static int test_typed_row_adapter_run(void){
    encoding();updates_and_reuse();legacy_views();maximum_and_binary();review_regressions();schema_and_failure_contracts();decimal_limits_and_types();json_contracts();automatic_fields();blind_review_boundaries();
    return t_ctx->failed?1:0;
}
TEST_REGISTER("test-typed-row-adapter",test_typed_row_adapter_run)

```

### Stage 1 entire module — unsupported stubs

<!-- block:row-stub -->
```c
#include "typed_row.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

int typed_schema_bind_packed(TypedSchema *ts, const char *const *types,
                              size_t count, char *err, size_t errsz) {
    (void)ts;
    (void)types;
    (void)count;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
uint8_t typed_schema_hdr2(const TypedSchema *ts) {
    (void)ts;
    errno=ENOSYS;
    return 0;
}
int typed_row_open(const TypedSchema *ts, const uint8_t *value, size_t len,
                    uint8_t hdr2, TypedRowView *out) {
    (void)ts;
    (void)value;
    (void)len;
    (void)hdr2;
    (void)out;
    if(out)memset(out,0,sizeof(*out));
    errno=ENOSYS;
    return -1;
}
int typed_row_span(const TypedRowView *view, size_t index, TypedRowSpan *out) {
    (void)view;
    (void)index;
    (void)out;
    if(out)memset(out,0,sizeof(*out));
    errno=ENOSYS;
    return -1;
}
int typed_span_read(const TypedSchema *ts, size_t index,
                      const TypedRowSpan *span, const uint8_t **out) {
    (void)ts;
    (void)index;
    (void)span;
    (void)out;
    if(out)*out=NULL;
    errno=ENOSYS;
    return -1;
}
int typed_span_copy_legacy(const TypedSchema *ts, size_t index,
                             const TypedRowSpan *span, uint8_t *out, size_t cap) {
    (void)ts;
    (void)index;
    (void)span;
    (void)out;
    (void)cap;
    errno=ENOSYS;
    return -1;
}
void typed_prepared_init(TypedPrepared *p){if(p)memset(p,0,sizeof(*p));}
void typed_prepared_free(TypedPrepared *p){
    if(!p)return;
    for(size_t i=0;i<MAX_FIELDS;i++)free(p->owned[i]);
    memset(p,0,sizeof(*p));
}
int typed_prepared_zero(const TypedSchema *ts, TypedPrepared *p,
                         char *err, size_t errsz) {
    (void)ts;
    (void)p;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_old(const TypedSchema *ts, const SlotcaskOldRecord *old,
                        TypedPrepared *p, char *err, size_t errsz) {
    (void)ts;
    (void)old;
    (void)p;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_set(TypedPrepared *p, size_t index, const TypedInput *input,
                        char *err, size_t errsz) {
    (void)p;
    (void)index;
    (void)input;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_defaults(TypedPrepared *p, const char *root,
                             const char *object, char *err, size_t errsz) {
    (void)p;
    (void)root;
    (void)object;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_stamp(TypedPrepared *p, enum DefaultKind kind,
                          char *err, size_t errsz) {
    (void)p;
    (void)kind;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_compose(const TypedPrepared *p, uint8_t *out, size_t cap,
                            size_t *used, char *err, size_t errsz) {
    (void)p;
    (void)out;
    (void)cap;
    (void)used;
    (void)err;
    (void)errsz;
    if(used)*used=0;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_alloc(const TypedPrepared *p, uint8_t **out, size_t *len,
                          char *err, size_t errsz) {
    (void)p;
    (void)out;
    (void)len;
    (void)err;
    (void)errsz;
    if(out)*out=NULL;
    if(len)*len=0;
    errno=ENOSYS;
    return -1;
}
int typed_encode_alloc(const TypedSchema *ts, const char *json, int defaults,
                         const char *root, const char *object,
                         uint8_t **out, size_t *len, char *err, size_t errsz) {
    (void)ts;
    (void)json;
    (void)defaults;
    (void)root;
    (void)object;
    (void)out;
    (void)len;
    (void)err;
    (void)errsz;
    if(out)*out=NULL;
    if(len)*len=0;
    errno=ENOSYS;
    return -1;
}
int typed_encode_packed_into(const TypedSchema *ts, const char *json, int defaults,
                              const char *root, const char *object,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz) {
    (void)ts;
    (void)json;
    (void)defaults;
    (void)root;
    (void)object;
    (void)out;
    (void)cap;
    (void)used;
    (void)err;
    (void)errsz;
    if(used)*used=0;
    errno=ENOSYS;
    return -1;
}
int typed_update_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                         const TypedInput *inputs, int stamp,
                         uint8_t **out, size_t *len, char *err, size_t errsz) {
    (void)ts;
    (void)old;
    (void)inputs;
    (void)stamp;
    (void)out;
    (void)len;
    (void)err;
    (void)errsz;
    if(out)*out=NULL;
    if(len)*len=0;
    errno=ENOSYS;
    return -1;
}
int typed_transfer_ac_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                             const uint8_t *value, size_t vlen,
                             uint8_t **out, size_t *len,
                             char *err, size_t errsz) {
    (void)ts;
    (void)old;
    (void)value;
    (void)vlen;
    (void)out;
    (void)len;
    (void)err;
    (void)errsz;
    if(out)*out=NULL;
    if(len)*len=0;
    errno=ENOSYS;
    return -1;
}
char *typed_packed_decode(const TypedSchema *ts, const uint8_t *value, size_t len,
                           uint8_t hdr2, size_t budget) {
    (void)ts;
    (void)value;
    (void)len;
    (void)hdr2;
    (void)budget;
    errno=ENOSYS;
    return NULL;
}
char *typed_packed_field_text(const TypedSchema *ts, const uint8_t *value,
                               size_t len, uint8_t hdr2, size_t index,
                               size_t *text_len) {
    (void)ts;
    (void)value;
    (void)len;
    (void)hdr2;
    (void)index;
    (void)text_len;
    if(text_len)*text_len=0;
    errno=ENOSYS;
    return NULL;
}

```

### Stage 2 entire module — schema/views with preparation stubs

<!-- block:row-views -->
```c
#include "typed_row.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

static int row_error(char *err, size_t cap, int code, const char *message) {
    if (err && cap) snprintf(err, cap, "%s", message);
    errno = code; return -1;
}
static int packed_schema_ok(const TypedSchema *ts) {
    if (!ts || !ts->typed || ts->nfields < 1 || ts->nfields > MAX_FIELDS ||
        ts->packed.nfields != (uint32_t)ts->nfields ||
        ts->packed.prefix != (uint32_t)ts->nullmap_prefix ||
        ts->packed.maximum != (uint32_t)ts->total_size) return 0;
    for (int i=0;i<ts->nfields;i++) {
        const PkrField *p=&ts->packed.fields[i];const TypedField *f=&ts->fields[i];
        if (f->name_len<1 || f->name_len>=(int)sizeof(f->name) ||
            strnlen(f->name,sizeof(f->name))!=(size_t)f->name_len ||
            f->offset<0 || f->size<0 || (uint32_t)f->offset!=p->offset ||
            (uint32_t)f->size!=p->width ||
            (p->kind==PKR_VARCHAR && f->type!=FT_VARCHAR) ||
            (p->kind==PKR_NUMERIC && f->type!=FT_NUMERIC) ||
            (p->kind==PKR_FIXED && (f->type==FT_VARCHAR||f->type==FT_NUMERIC)))
            return 0;
    }
    return 1;
}
int typed_schema_bind_packed(TypedSchema *ts,const char *const *types,
                              size_t n,char *err,size_t errsz) {
    if (!ts||!types||!ts->typed||n<1||n>MAX_FIELDS||n!=(size_t)ts->nfields)
        return row_error(err,errsz,EINVAL,"invalid packed schema");
    PkrField fields[MAX_FIELDS];enum FieldType kinds[MAX_FIELDS];
    for(size_t i=0;i<n;i++){
        const TypedField *field=&ts->fields[i];
        if(field->name_len<1||field->name_len>=(int)sizeof(field->name)||
           strnlen(field->name,sizeof(field->name))!=(size_t)field->name_len)
            return row_error(err,errsz,EINVAL,"invalid field name");
        if(!types[i])return row_error(err,errsz,EINVAL,"missing type token");
        if(!strncmp(types[i],"varchar",7)||!strncmp(types[i],"numeric",7)||
           !strcmp(types[i],"currency")){
            if(pkr_field_parse(types[i],&fields[i])!=DEC_OK)
                return row_error(err,errsz,EINVAL,"invalid packed declaration");
            kinds[i]=fields[i].kind==PKR_VARCHAR?FT_VARCHAR:FT_NUMERIC;
            if(ts->fields[i].type!=FT_NONE&&ts->fields[i].type!=kinds[i])
                return row_error(err,errsz,EINVAL,"packed logical type mismatch");
        }else{
            if(!strncmp(types[i],"enum(",5)){
                const char *close=strrchr(types[i],')');
                if(!close||close[1])
                    return row_error(err,errsz,EINVAL,"enum token contains trailing declaration text");
            }
            TypedField check={0};errno=0;parse_field_type(types[i],&check);
            int parse_errno=errno;
            int good=check.type!=FT_NONE&&check.type==ts->fields[i].type&&
                check.size==ts->fields[i].size&&check.size>=1&&check.size<=16;
            if(check.type==FT_ENUM){
                good=good&&check.n_enum_values==ts->fields[i].n_enum_values;
                for(int k=0;good&&k<check.n_enum_values;k++)
                    good=!strcmp(check.enum_values[k],ts->fields[i].enum_values[k]);
            }
            free_enum_values(&check);
            if(!good||pkr_field_fixed((uint32_t)ts->fields[i].size,&fields[i])!=DEC_OK)
                return row_error(err,errsz,parse_errno==ENOMEM?ENOMEM:EINVAL,"invalid fixed declaration");
            kinds[i]=ts->fields[i].type;
        }
    }
    PkrSchema next;
    if(pkr_schema_build(fields,n,&next)!=DEC_OK)
        return row_error(err,errsz,ERANGE,"packed schema exceeds bounds");
    for(size_t i=0;i<n;i++){
        ts->fields[i].type=kinds[i];ts->fields[i].offset=(int)next.fields[i].offset;
        ts->fields[i].size=(int)next.fields[i].width;
        if(kinds[i]==FT_NUMERIC){
            ts->fields[i].numeric_scale=next.fields[i].scale;
            ts->fields[i].numeric_scale_mult=0;
        }
    }
    ts->packed=next;ts->nullmap_prefix=(int)next.prefix;
    ts->total_size=(int)next.maximum;return 0;
}
uint8_t typed_schema_hdr2(const TypedSchema *ts) {
    return ts&&ts->packed.nfields?PKR_FLAGS:SREC_FLAG_NULLMAP;
}
int typed_row_open(const TypedSchema *ts,const uint8_t *value,size_t len,
                    uint8_t hdr2,TypedRowView *out) {
    if(!out){errno=EINVAL;return -1;}memset(out,0,sizeof(*out));
    if(!ts||!ts->typed||ts->nfields<1||ts->nfields>MAX_FIELDS||!value){
        errno=EINVAL;return -1;}
    TypedRowView v={0};v.schema=ts;v.value=value;v.len=len;v.hdr2=hdr2;
    if(ts->packed.nfields){
        if(!packed_schema_ok(ts)){errno=EILSEQ;return -1;}
        dec_status st=pkr_view(&ts->packed,value,len,hdr2,&v.packed);
        if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EILSEQ;return -1;}
        v.base=value+ts->packed.prefix;v.region=len-ts->packed.prefix;
        v.bitmap=value+1;v.bitmap_bytes=(int)ts->packed.prefix-1;
    }else{
        if(typed_record_view(value,len,hdr2,&v.base,&v.bitmap,
                              &v.bitmap_bytes,&v.region)!=0){
            errno=EILSEQ;return -1;}
    }
    *out=v;return 0;
}
int typed_row_span(const TypedRowView *v,size_t index,TypedRowSpan *out) {
    if(!out){errno=EINVAL;return -1;}memset(out,0,sizeof(*out));
    if(!v||!v->schema||index>=(size_t)v->schema->nfields){errno=EINVAL;return -1;}
    if(v->schema->packed.nfields){
        PkrSpan p;if(pkr_span(&v->packed,index,&p)!=DEC_OK){errno=EILSEQ;return -1;}
        out->data=p.data;out->len=p.len;out->is_null=p.state==PKR_NULL;
        out->complete=1;return 0;
    }
    const TypedField *f=&v->schema->fields[index];
    if(f->offset<0||f->size<1||f->size>65537){errno=EILSEQ;return -1;}
    out->is_null=typed_field_null(v->bitmap,v->bitmap_bytes,(int)index);
    size_t off=(size_t)f->offset,cap=(size_t)f->size;
    if(off<v->region){
        out->len=v->region-off;if(out->len>cap)out->len=cap;
        out->data=v->base+off;
    }
    out->complete=out->len==cap;return 0;
}
int typed_span_read(const TypedSchema *ts,size_t index,const TypedRowSpan *s,
                      const uint8_t **out) {
    static const uint8_t zero[65537];
    if(!out){errno=EINVAL;return -1;}*out=NULL;
    if(!ts||!s||index>=(size_t)ts->nfields||s->is_null){errno=EINVAL;return -1;}
    if(ts->packed.nfields){if(!s->complete){errno=EILSEQ;return -1;}*out=s->data;}
    else *out=s->complete?s->data:zero;
    return 0;
}
int typed_span_copy_legacy(const TypedSchema *ts,size_t index,
                             const TypedRowSpan *s,uint8_t *out,size_t cap){
    if(!ts||ts->packed.nfields||index>=(size_t)ts->nfields||!s||!out||
       ts->fields[index].size<1||cap<(size_t)ts->fields[index].size){
        errno=EINVAL;return -1;}
    size_t width=(size_t)ts->fields[index].size;
    if(s->len>width||(!s->data&&s->len)){errno=EILSEQ;return -1;}
    memset(out,0,width);if(s->len)memcpy(out,s->data,s->len);return 0;
}
void typed_prepared_init(TypedPrepared *p){if(p)memset(p,0,sizeof(*p));}
void typed_prepared_free(TypedPrepared *p){
    if(!p)return;
    for(size_t i=0;i<MAX_FIELDS;i++)free(p->owned[i]);
    memset(p,0,sizeof(*p));
}
int typed_prepared_zero(const TypedSchema *ts, TypedPrepared *p,
                         char *err, size_t errsz) {
    (void)ts;
    (void)p;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_old(const TypedSchema *ts, const SlotcaskOldRecord *old,
                        TypedPrepared *p, char *err, size_t errsz) {
    (void)ts;
    (void)old;
    (void)p;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_set(TypedPrepared *p, size_t index, const TypedInput *input,
                        char *err, size_t errsz) {
    (void)p;
    (void)index;
    (void)input;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_defaults(TypedPrepared *p, const char *root,
                             const char *object, char *err, size_t errsz) {
    (void)p;
    (void)root;
    (void)object;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_stamp(TypedPrepared *p, enum DefaultKind kind,
                          char *err, size_t errsz) {
    (void)p;
    (void)kind;
    (void)err;
    (void)errsz;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_compose(const TypedPrepared *p, uint8_t *out, size_t cap,
                            size_t *used, char *err, size_t errsz) {
    (void)p;
    (void)out;
    (void)cap;
    (void)used;
    (void)err;
    (void)errsz;
    if(used)*used=0;
    errno=ENOSYS;
    return -1;
}
int typed_prepared_alloc(const TypedPrepared *p, uint8_t **out, size_t *len,
                          char *err, size_t errsz) {
    (void)p;
    (void)out;
    (void)len;
    (void)err;
    (void)errsz;
    if(out)*out=NULL;
    if(len)*len=0;
    errno=ENOSYS;
    return -1;
}
int typed_encode_alloc(const TypedSchema *ts, const char *json, int defaults,
                         const char *root, const char *object,
                         uint8_t **out, size_t *len, char *err, size_t errsz) {
    (void)ts;
    (void)json;
    (void)defaults;
    (void)root;
    (void)object;
    (void)out;
    (void)len;
    (void)err;
    (void)errsz;
    if(out)*out=NULL;
    if(len)*len=0;
    errno=ENOSYS;
    return -1;
}
int typed_encode_packed_into(const TypedSchema *ts, const char *json, int defaults,
                              const char *root, const char *object,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz) {
    (void)ts;
    (void)json;
    (void)defaults;
    (void)root;
    (void)object;
    (void)out;
    (void)cap;
    (void)used;
    (void)err;
    (void)errsz;
    if(used)*used=0;
    errno=ENOSYS;
    return -1;
}
int typed_update_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                         const TypedInput *inputs, int stamp,
                         uint8_t **out, size_t *len, char *err, size_t errsz) {
    (void)ts;
    (void)old;
    (void)inputs;
    (void)stamp;
    (void)out;
    (void)len;
    (void)err;
    (void)errsz;
    if(out)*out=NULL;
    if(len)*len=0;
    errno=ENOSYS;
    return -1;
}
int typed_transfer_ac_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                             const uint8_t *value, size_t vlen,
                             uint8_t **out, size_t *len,
                             char *err, size_t errsz) {
    (void)ts;
    (void)old;
    (void)value;
    (void)vlen;
    (void)out;
    (void)len;
    (void)err;
    (void)errsz;
    if(out)*out=NULL;
    if(len)*len=0;
    errno=ENOSYS;
    return -1;
}
char *typed_packed_decode(const TypedSchema *ts, const uint8_t *value, size_t len,
                           uint8_t hdr2, size_t budget) {
    (void)ts;
    (void)value;
    (void)len;
    (void)hdr2;
    (void)budget;
    errno=ENOSYS;
    return NULL;
}
char *typed_packed_field_text(const TypedSchema *ts, const uint8_t *value,
                               size_t len, uint8_t hdr2, size_t index,
                               size_t *text_len) {
    (void)ts;
    (void)value;
    (void)len;
    (void)hdr2;
    (void)index;
    (void)text_len;
    if(text_len)*text_len=0;
    errno=ENOSYS;
    return NULL;
}

```

### Stage 3 entire module — final implementation

<!-- block:row-core -->
```c
#include "typed_row.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

static int row_error(char *err, size_t cap, int code, const char *message) {
    if (err && cap) snprintf(err, cap, "%s", message);
    errno = code; return -1;
}
static int packed_schema_ok(const TypedSchema *ts) {
    if (!ts || !ts->typed || ts->nfields < 1 || ts->nfields > MAX_FIELDS ||
        ts->packed.nfields != (uint32_t)ts->nfields ||
        ts->packed.prefix != (uint32_t)ts->nullmap_prefix ||
        ts->packed.maximum != (uint32_t)ts->total_size) return 0;
    for (int i=0;i<ts->nfields;i++) {
        const PkrField *p=&ts->packed.fields[i];const TypedField *f=&ts->fields[i];
        if (f->name_len<1 || f->name_len>=(int)sizeof(f->name) ||
            strnlen(f->name,sizeof(f->name))!=(size_t)f->name_len ||
            f->offset<0 || f->size<0 || (uint32_t)f->offset!=p->offset ||
            (uint32_t)f->size!=p->width ||
            (p->kind==PKR_VARCHAR && f->type!=FT_VARCHAR) ||
            (p->kind==PKR_NUMERIC && f->type!=FT_NUMERIC) ||
            (p->kind==PKR_FIXED && (f->type==FT_VARCHAR||f->type==FT_NUMERIC)))
            return 0;
    }
    return 1;
}
int typed_schema_bind_packed(TypedSchema *ts,const char *const *types,
                              size_t n,char *err,size_t errsz) {
    if (!ts||!types||!ts->typed||n<1||n>MAX_FIELDS||n!=(size_t)ts->nfields)
        return row_error(err,errsz,EINVAL,"invalid packed schema");
    PkrField fields[MAX_FIELDS];enum FieldType kinds[MAX_FIELDS];
    for(size_t i=0;i<n;i++){
        const TypedField *field=&ts->fields[i];
        if(field->name_len<1||field->name_len>=(int)sizeof(field->name)||
           strnlen(field->name,sizeof(field->name))!=(size_t)field->name_len)
            return row_error(err,errsz,EINVAL,"invalid field name");
        if(!types[i])return row_error(err,errsz,EINVAL,"missing type token");
        if(!strncmp(types[i],"varchar",7)||!strncmp(types[i],"numeric",7)||
           !strcmp(types[i],"currency")){
            if(pkr_field_parse(types[i],&fields[i])!=DEC_OK)
                return row_error(err,errsz,EINVAL,"invalid packed declaration");
            kinds[i]=fields[i].kind==PKR_VARCHAR?FT_VARCHAR:FT_NUMERIC;
            if(ts->fields[i].type!=FT_NONE&&ts->fields[i].type!=kinds[i])
                return row_error(err,errsz,EINVAL,"packed logical type mismatch");
        }else{
            if(!strncmp(types[i],"enum(",5)){
                const char *close=strrchr(types[i],')');
                if(!close||close[1])
                    return row_error(err,errsz,EINVAL,"enum token contains trailing declaration text");
            }
            TypedField check={0};errno=0;parse_field_type(types[i],&check);
            int parse_errno=errno;
            int good=check.type!=FT_NONE&&check.type==ts->fields[i].type&&
                check.size==ts->fields[i].size&&check.size>=1&&check.size<=16;
            if(check.type==FT_ENUM){
                good=good&&check.n_enum_values==ts->fields[i].n_enum_values;
                for(int k=0;good&&k<check.n_enum_values;k++)
                    good=!strcmp(check.enum_values[k],ts->fields[i].enum_values[k]);
            }
            free_enum_values(&check);
            if(!good||pkr_field_fixed((uint32_t)ts->fields[i].size,&fields[i])!=DEC_OK)
                return row_error(err,errsz,parse_errno==ENOMEM?ENOMEM:EINVAL,"invalid fixed declaration");
            kinds[i]=ts->fields[i].type;
        }
    }
    PkrSchema next;
    if(pkr_schema_build(fields,n,&next)!=DEC_OK)
        return row_error(err,errsz,ERANGE,"packed schema exceeds bounds");
    for(size_t i=0;i<n;i++){
        ts->fields[i].type=kinds[i];ts->fields[i].offset=(int)next.fields[i].offset;
        ts->fields[i].size=(int)next.fields[i].width;
        if(kinds[i]==FT_NUMERIC){
            ts->fields[i].numeric_scale=next.fields[i].scale;
            ts->fields[i].numeric_scale_mult=0;
        }
    }
    ts->packed=next;ts->nullmap_prefix=(int)next.prefix;
    ts->total_size=(int)next.maximum;return 0;
}
uint8_t typed_schema_hdr2(const TypedSchema *ts) {
    return ts&&ts->packed.nfields?PKR_FLAGS:SREC_FLAG_NULLMAP;
}
int typed_row_open(const TypedSchema *ts,const uint8_t *value,size_t len,
                    uint8_t hdr2,TypedRowView *out) {
    if(!out){errno=EINVAL;return -1;}memset(out,0,sizeof(*out));
    if(!ts||!ts->typed||ts->nfields<1||ts->nfields>MAX_FIELDS||!value){
        errno=EINVAL;return -1;}
    TypedRowView v={0};v.schema=ts;v.value=value;v.len=len;v.hdr2=hdr2;
    if(ts->packed.nfields){
        if(!packed_schema_ok(ts)){errno=EILSEQ;return -1;}
        dec_status st=pkr_view(&ts->packed,value,len,hdr2,&v.packed);
        if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EILSEQ;return -1;}
        v.base=value+ts->packed.prefix;v.region=len-ts->packed.prefix;
        v.bitmap=value+1;v.bitmap_bytes=(int)ts->packed.prefix-1;
    }else{
        if(typed_record_view(value,len,hdr2,&v.base,&v.bitmap,
                              &v.bitmap_bytes,&v.region)!=0){
            errno=EILSEQ;return -1;}
    }
    *out=v;return 0;
}
int typed_row_span(const TypedRowView *v,size_t index,TypedRowSpan *out) {
    if(!out){errno=EINVAL;return -1;}memset(out,0,sizeof(*out));
    if(!v||!v->schema||index>=(size_t)v->schema->nfields){errno=EINVAL;return -1;}
    if(v->schema->packed.nfields){
        PkrSpan p;if(pkr_span(&v->packed,index,&p)!=DEC_OK){errno=EILSEQ;return -1;}
        out->data=p.data;out->len=p.len;out->is_null=p.state==PKR_NULL;
        out->complete=1;return 0;
    }
    const TypedField *f=&v->schema->fields[index];
    if(f->offset<0||f->size<1||f->size>65537){errno=EILSEQ;return -1;}
    out->is_null=typed_field_null(v->bitmap,v->bitmap_bytes,(int)index);
    size_t off=(size_t)f->offset,cap=(size_t)f->size;
    if(off<v->region){
        out->len=v->region-off;if(out->len>cap)out->len=cap;
        out->data=v->base+off;
    }
    out->complete=out->len==cap;return 0;
}
int typed_span_read(const TypedSchema *ts,size_t index,const TypedRowSpan *s,
                      const uint8_t **out) {
    static const uint8_t zero[65537];
    if(!out){errno=EINVAL;return -1;}*out=NULL;
    if(!ts||!s||index>=(size_t)ts->nfields||s->is_null){errno=EINVAL;return -1;}
    if(ts->packed.nfields){if(!s->complete){errno=EILSEQ;return -1;}*out=s->data;}
    else *out=s->complete?s->data:zero;
    return 0;
}
int typed_span_copy_legacy(const TypedSchema *ts,size_t index,
                             const TypedRowSpan *s,uint8_t *out,size_t cap){
    if(!ts||ts->packed.nfields||index>=(size_t)ts->nfields||!s||!out||
       ts->fields[index].size<1||cap<(size_t)ts->fields[index].size){
        errno=EINVAL;return -1;}
    size_t width=(size_t)ts->fields[index].size;
    if(s->len>width||(!s->data&&s->len)){errno=EILSEQ;return -1;}
    memset(out,0,width);if(s->len)memcpy(out,s->data,s->len);return 0;
}
void typed_prepared_init(TypedPrepared *p){if(p)memset(p,0,sizeof(*p));}
void typed_prepared_free(TypedPrepared *p){
    if(!p)return;
    for(size_t i=0;i<MAX_FIELDS;i++)free(p->owned[i]);
    memset(p,0,sizeof(*p));
}
static int prepared_check(const TypedPrepared *p,char *err,size_t cap){
    if(!p||!packed_schema_ok(p->schema))
        return row_error(err,cap,EINVAL,"invalid prepared schema");
    return 0;
}
/* Legacy scalar helpers report status/text but do not promise errno for
   validation failures. Establish the new adapter boundary's error contract. */
static int row_encode_fixed(const TypedField *field,const char *text,size_t len,
                              int quoted,uint8_t *out,char *err,size_t cap){
    errno=0;
    int rc=encode_json_field_checked(field,text,len,quoted,out,err,cap);
    if(rc)errno=errno==ENOMEM?ENOMEM:EINVAL;
    return rc;
}
static int replace_cell(TypedPrepared *p,size_t i,uint8_t *owner,
                          const uint8_t *data,size_t n,int is_null,
                          char *err,size_t cap){
    const PkrField *f=&p->schema->packed.fields[i];
    size_t prior=p->cells[i].is_null||f->kind==PKR_FIXED?0:p->cells[i].len;
    size_t added=is_null||f->kind==PKR_FIXED?0:n;
    if(p->bytes<prior||added>SLOTCASK_SEG_MAX_BYTES-24||
       p->bytes-prior>SLOTCASK_SEG_MAX_BYTES-24-added){
        free(owner);return row_error(err,cap,EOVERFLOW,"record exceeds segment capacity");}
    free(p->owned[i]);p->owned[i]=owner;p->cells[i]=(PkrCell){data,n,(uint8_t)is_null};
    p->bytes=p->bytes-prior+added;return 0;
}
int typed_prepared_set(TypedPrepared *p,size_t i,const TypedInput *in,
                        char *err,size_t cap){
    if(prepared_check(p,err,cap)||!in||i>=(size_t)p->schema->nfields)
        return row_error(err,cap,EINVAL,"invalid field input");
    if(!in->supplied)return 0;
    const TypedField *f=&p->schema->fields[i];const PkrField *pf=&p->schema->packed.fields[i];
    if(f->removed)return 0;
    if(in->is_null){
        if(f->not_null){fmt_not_null_err(f,err,cap);errno=EINVAL;return -1;}
        int rc=replace_cell(p,i,NULL,NULL,0,1,err,cap);
        if(!rc)p->seen[i]=1;
        return rc;
    }
    if(!in->data&&in->len)return row_error(err,cap,EINVAL,"invalid value span");
    const char *text=in->data?in->data:"";size_t n=in->len;char *decoded=NULL;
    int string_type=f->type==FT_VARCHAR||f->type==FT_NUMERIC||
        f->type==FT_ENUM||f->type==FT_DATE||f->type==FT_DATETIME||
        f->type==FT_DATETIMEMS||f->type==FT_TIME||f->type==FT_UUID||
        f->type==FT_IPV4||f->type==FT_IPV6;
    int was_quoted=in->quoted;
    if(was_quoted&&string_type){
        errno=0;
        int rc=f->type==FT_VARCHAR?json_unescape_string(text,n,&decoded,&n):
            json_unescape_cstring(text,n,&decoded,&n);
        if(rc)return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"malformed JSON string");
        text=decoded;was_quoted=0;
    }
    uint8_t *bytes=NULL;size_t used=0;int rc=-1;
    if(pf->kind==PKR_VARCHAR){
        if(n>pf->payload_max){row_error(err,cap,ERANGE,"varchar content exceeds declaration");goto done;}
        bytes=malloc(n?n:1);if(!bytes){row_error(err,cap,ENOMEM,"oom");goto done;}
        if(n)memcpy(bytes,text,n);
        used=n;
    }else if(pf->kind==PKR_NUMERIC){
        dec_t a,b;dec_init(&a);dec_init(&b);
        dec_status st=dec_parse(text,n,&a,err,cap);const dec_t *value=&a;
        if(st==DEC_OK&&!pf->bare){
            st=dec_apply_precision(&a,pf->precision,pf->scale,&b,err,cap);value=&b;}
        if(st==DEC_OK){
            used=dec_payload_len(value);
            if(used>pf->payload_max)st=DEC_ERR_RANGE;
            else{bytes=malloc(used);if(!bytes)st=DEC_ERR_NOMEM;
                else st=dec_pack(value,bytes,used,&used);}
        }
        dec_free(&b);dec_free(&a);
        if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EINVAL;goto done;}
    }else{
        if(!sentinel_span_parses(f->type,text,n)){
            if(f->not_null){fmt_not_null_err(f,err,cap);errno=EINVAL;goto done;}
            rc=replace_cell(p,i,NULL,NULL,0,1,err,cap);goto done;
        }
        bytes=calloc(1,pf->width);if(!bytes){row_error(err,cap,ENOMEM,"oom");goto done;}
        if(row_encode_fixed(f,text,n,was_quoted,bytes,err,cap)!=0)goto done;
        used=pf->width;
    }
    rc=replace_cell(p,i,bytes,bytes,used,0,err,cap);bytes=NULL;
done:if(!rc)p->seen[i]=1;
    int saved_errno=errno;
    free(bytes);free(decoded);
    if(rc)errno=saved_errno;
    return rc;
}
int typed_prepared_zero(const TypedSchema *ts,TypedPrepared *p,char *err,size_t cap){
    if(!p||!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid packed schema");
    typed_prepared_free(p);p->schema=ts;p->bytes=ts->packed.prefix+ts->packed.directory;
    for(int i=0;i<ts->nfields;i++)p->cells[i]=(PkrCell){NULL,0,1};
    for(int i=0;i<ts->nfields;i++){
        const TypedField *f=&ts->fields[i];if(f->removed)continue;
        const PkrField *pf=&ts->packed.fields[i];
        if(pf->kind==PKR_FIXED){
            uint8_t *z=calloc(1,pf->width);if(!z){typed_prepared_free(p);return row_error(err,cap,ENOMEM,"oom");}
            if(replace_cell(p,(size_t)i,z,z,pf->width,0,err,cap))goto fail;
        }else{
            TypedInput in={pf->kind==PKR_NUMERIC?"0":"",pf->kind==PKR_NUMERIC?1:0,1,0,0};
            if(typed_prepared_set(p,(size_t)i,&in,err,cap))goto fail;
        }
    }
    memset(p->seen,0,sizeof(p->seen));return 0;
fail:typed_prepared_free(p);return -1;
}
int typed_prepared_old(const TypedSchema *ts,const SlotcaskOldRecord *old,
                        TypedPrepared *p,char *err,size_t cap){
    if(!p||!old||!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid OLD");
    TypedRowView v;if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&v))
        return row_error(err,cap,errno,"invalid OLD record");
    typed_prepared_free(p);p->schema=ts;p->bytes=ts->packed.prefix+ts->packed.directory;
    for(int i=0;i<ts->nfields;i++){
        TypedRowSpan sp;if(typed_row_span(&v,(size_t)i,&sp))goto fail;
        int null=sp.is_null||ts->fields[i].removed;
        p->cells[i]=(PkrCell){null?NULL:sp.data,null?0:sp.len,(uint8_t)null};
        if(!null&&ts->packed.fields[i].kind!=PKR_FIXED)p->bytes+=sp.len;
    }
    if(p->bytes>SLOTCASK_SEG_MAX_BYTES-24){
        typed_prepared_free(p);
        return row_error(err,cap,EOVERFLOW,"record exceeds segment capacity");
    }
    return 0;
fail:typed_prepared_free(p);return row_error(err,cap,EILSEQ,"invalid OLD field");
}
int typed_prepared_defaults(TypedPrepared *p,const char *root,const char *object,
                             char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];if(f->removed||p->seen[i])continue;
        TypedInput in={NULL,0,1,1,0};char generated[256];
        if(f->default_kind!=DK_NONE){
            in.data=generate_default(f,generated,sizeof(generated),root,object);
            if(!in.data)return row_error(err,cap,EIO,"default generator failed");
            in.len=strlen(in.data);in.is_null=0;
        }
        if(typed_prepared_set(p,(size_t)i,&in,err,cap))return -1;
    }return 0;
}
int typed_prepared_stamp(TypedPrepared *p,enum DefaultKind kind,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    if(kind!=DK_AUTO_CREATE&&kind!=DK_AUTO_UPDATE)
        return row_error(err,cap,EINVAL,"invalid timestamp kind");
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];
        if(f->removed||f->default_kind!=kind)continue;
        char b[256];const char *v=generate_default(f,b,sizeof(b),NULL,NULL);
        if(!v)return row_error(err,cap,EIO,"automatic timestamp failed");
        TypedInput in={v,strlen(v),1,0,0};
        if(typed_prepared_set(p,(size_t)i,&in,err,cap))return -1;
    }return 0;
}
static int prepared_valid(const TypedPrepared *p,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    for(int i=0;i<p->schema->nfields;i++){
        if(!p->schema->fields[i].removed&&p->schema->fields[i].not_null&&p->cells[i].is_null){
            fmt_not_null_err(&p->schema->fields[i],err,cap);errno=EINVAL;return -1;}
    }return 0;
}
int typed_prepared_compose(const TypedPrepared *p,uint8_t *out,size_t cap,
                            size_t *used,char *err,size_t errsz){
    if(used)*used=0;
    if(prepared_valid(p,err,errsz))return -1;
    dec_status st=pkr_compose(&p->schema->packed,p->cells,(size_t)p->schema->nfields,out,cap,used);
    if(st!=DEC_OK)return row_error(err,errsz,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"record composition failed");
    return 0;
}
int typed_prepared_alloc(const TypedPrepared *p,uint8_t **out,size_t *len,
                          char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(prepared_valid(p,err,cap))return -1;
    size_t need=0;dec_status st=pkr_measure(&p->schema->packed,p->cells,(size_t)p->schema->nfields,&need);
    if(st!=DEC_OK)return row_error(err,cap,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"record sizing failed");
    uint8_t *buf=malloc(need);if(!buf)return row_error(err,cap,ENOMEM,"oom");
    size_t used=0;if(typed_prepared_compose(p,buf,need,&used,err,cap)){free(buf);return -1;}
    *out=buf;*len=used;return 0;
}

/* Validate syntax before span collection. json_skip_value locates spans but
   deliberately is not a validator. Depth is bounded without global state. */
static const char *row_json_space(const char *p){
    while(*p==' '||*p=='\t'||*p=='\r'||*p=='\n')p++;
    return p;
}
static int row_json_string(const char **cursor){
    const unsigned char *p=(const unsigned char*)*cursor;
    if(*p++!='"')return -1;
    while(*p&&*p!='"'){
        if(*p<0x20)return -1;
        if(*p++!='\\')continue;
        unsigned char c=*p++;
        if(!c)return -1;
        if(c=='u'){
            for(int i=0;i<4;i++){
                unsigned char h=*p++;
                if(!((h>='0'&&h<='9')||(h>='a'&&h<='f')||(h>='A'&&h<='F')))return -1;
            }
        }else if(c!='"'&&c!='\\'&&c!='/'&&c!='b'&&c!='f'&&c!='n'&&c!='r'&&c!='t')return -1;
    }
    if(*p!='"')return -1;
    *cursor=(const char*)p+1;return 0;
}
static int row_json_value(const char **cursor,unsigned depth){
    const char *p=row_json_space(*cursor);
    if(*p=='"'){
        if(row_json_string(&p))return -1;
    }else if(*p=='{'||*p=='['){
        if(depth>=64)return -1;
        int object=*p=='{';char end=object?'}':']';p=row_json_space(p+1);
        if(*p!=end)for(;;){
            if(object){
                if(row_json_string(&p))return -1;
                p=row_json_space(p);if(*p++!=':')return -1;
            }
            if(row_json_value(&p,depth+1))return -1;
            p=row_json_space(p);if(*p==end)break;
            if(*p++!=',')return -1;
            p=row_json_space(p);
        }
        p++;
    }else if(!strncmp(p,"null",4))p+=4;
    else if(!strncmp(p,"true",4))p+=4;
    else if(!strncmp(p,"false",5))p+=5;
    else{
        if(*p=='-')p++;
        if(*p=='0')p++;
        else{
            if(*p<'1'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
        if(*p=='.'){
            p++;if(*p<'0'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
        if(*p=='e'||*p=='E'){
            p++;if(*p=='+'||*p=='-')p++;
            if(*p<'0'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
    }
    *cursor=p;return 0;
}
static int prepare_json(const TypedSchema *ts,const char *json,int defaults,
                          const char *root,const char *object,TypedPrepared *out,
                          char *err,size_t cap){
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    const char *valid=json;
    if(row_json_value(&valid,0)||*row_json_space(valid))
        return row_error(err,cap,EINVAL,"invalid record JSON");
    if(typed_prepared_zero(ts,out,err,cap))return -1;
    const char *p=row_json_space(json);if(*p++!='{')goto bad;
    int first=1;
    while(1){
        p=row_json_space(p);if(*p=='}'){p++;break;}
        if(!first){if(*p++!=',')goto bad;p=row_json_space(p);}
        if(*p!='"')goto bad;
        const char *key=p+1,*after=p;
        if(row_json_string(&after))goto bad;
        size_t kn=(size_t)(after-key-1);p=row_json_space(after);
        if(*p++!=':')goto bad;
        p=row_json_space(p);
        const char *v=p;
        if(row_json_value(&p,0))goto bad;
        size_t n=(size_t)(p-v);char *name=NULL;size_t name_len=0;
        /* Longer encoded keys cannot equal a <=255-byte schema name. */
        if(kn<=6*(sizeof(ts->fields[0].name)-1)){
            errno=0;
            if(json_unescape_string(key,kn,&name,&name_len)){
                row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid field name");goto fail;
            }
        }
        for(int i=0;name&&i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];
            if(f->removed||name_len!=(size_t)f->name_len||memcmp(f->name,name,name_len))continue;
            int quoted=n>=2&&v[0]=='"'&&v[n-1]=='"';
            if(!quoted&&(v[0]=='{'||v[0]=='[')){free(name);goto bad;}
            TypedInput in={quoted?v+1:v,quoted?n-2:n,1,
                (uint8_t)(!quoted&&json_span_is_null(v,n)),(uint8_t)quoted};
            if(typed_prepared_set(out,(size_t)i,&in,err,cap)){free(name);goto fail;}
            break;
        }
        free(name);first=0;
    }
    if(*row_json_space(p))goto bad;
    if(defaults&&typed_prepared_defaults(out,root,object,err,cap))goto fail;
    return 0;
bad:row_error(err,cap,EINVAL,"invalid record JSON");
fail:typed_prepared_free(out);return -1;
}
int typed_encode_packed_into(const TypedSchema *ts,const char *json,int defaults,
                              const char *root,const char *object,uint8_t *out,
                              size_t cap,size_t *used,char *err,size_t errsz){
    if(used)*used=0;
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,err,errsz);
    if(!rc)rc=typed_prepared_compose(&p,out,cap,used,err,errsz);
    typed_prepared_free(&p);return rc;
}
int typed_encode_alloc(const TypedSchema *ts,const char *json,int defaults,
                         const char *root,const char *object,uint8_t **out,
                         size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(!ts||!ts->typed||ts->total_size<1)
        return row_error(err,cap,EINVAL,"invalid typed schema");
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    if(!ts->packed.nfields){
        uint8_t *b=malloc((size_t)ts->total_size);if(!b)return row_error(err,cap,ENOMEM,"oom");
        errno=0;
        int rc=defaults?typed_encode_defaults(ts,json,b,ts->total_size,root,object,err,cap):
            typed_encode(ts,json,b,ts->total_size,err,cap);
        if(rc<0){int cause=errno==ENOMEM?ENOMEM:EINVAL;free(b);errno=cause;return rc;}
        *out=b;*len=(size_t)rc;return 0;
    }
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
int typed_update_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
                         const TypedInput *inputs,int stamp,uint8_t **out,
                         size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(!ts||!old)return row_error(err,cap,EINVAL,"invalid update");
    if(!ts->packed.nfields){
        if(ts->total_size<1)return row_error(err,cap,EINVAL,"invalid schema");
        uint8_t *b=malloc((size_t)ts->total_size);if(!b)return row_error(err,cap,ENOMEM,"oom");
        if(update_compose_new_from_old(ts,old,b,NULL,NULL,err,cap)){
            free(b);return row_error(err,cap,EILSEQ,"invalid OLD record");
        }
        if(inputs)for(int i=0;i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];const TypedInput *in=&inputs[i];
            if(f->removed||!in->supplied)continue;
            uint8_t *dst=b+ts->nullmap_prefix+f->offset;
            if(in->is_null){
                if(f->not_null){fmt_not_null_err(f,err,cap);free(b);errno=EINVAL;return -1;}
                memset(dst,0,(size_t)f->size);typed_set_null(b+1,ts->nullmap_prefix-1,i);
            }else{
                if(in->len&&!in->data){free(b);return row_error(err,cap,EINVAL,"invalid value span");}
                if(row_encode_fixed(f,in->data?in->data:"",in->len,in->quoted,dst,err,cap)){
                    int cause=errno;free(b);errno=cause;return -1;
                }
                typed_clear_null(b+1,ts->nullmap_prefix-1,i);
            }
        }
        if(stamp)update_stamp_auto_update_fields(ts,b);
        *out=b;*len=(size_t)ts->total_size;return 0;
    }
    TypedPrepared p;typed_prepared_init(&p);
    int rc=typed_prepared_old(ts,old,&p,err,cap);
    if(!rc&&inputs)for(int i=0;i<ts->nfields;i++){
        if(typed_prepared_set(&p,(size_t)i,&inputs[i],err,cap)){rc=-1;break;}}
    if(!rc&&stamp)rc=typed_prepared_stamp(&p,DK_AUTO_UPDATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
int typed_transfer_ac_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
                             const uint8_t *value,size_t vlen,uint8_t **out,
                             size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid packed schema");
    SlotcaskOldRecord current={value,vlen,PKR_FLAGS};TypedPrepared p;typed_prepared_init(&p);
    if(typed_prepared_old(ts,&current,&p,err,cap))return -1;
    int rc=0;
    if(old&&old->value){
        TypedRowView view;if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view))rc=-1;
        for(int i=0;!rc&&i<ts->nfields;i++){
            if(ts->fields[i].removed||ts->fields[i].default_kind!=DK_AUTO_CREATE)continue;
            TypedRowSpan span;if(typed_row_span(&view,(size_t)i,&span)){rc=-1;break;}
            rc=replace_cell(&p,(size_t)i,NULL,span.data,span.len,span.is_null,err,cap);
        }
    }else rc=typed_prepared_stamp(&p,DK_AUTO_CREATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

/* One bounded field description at a time; no varchar-sized scratch buffer.
   Packed-view validation and decimal workspace are separate from output budget. */
typedef struct {
    const char *data;
    size_t len;
    int quoted, numeric;
    char fixed[512];
    dec_t decimal;
} RowText;
static void row_text_free(RowText *text){dec_free(&text->decimal);}
static int row_text_open(const TypedRowView *view,size_t index,RowText *text){
    memset(text,0,sizeof(*text));dec_init(&text->decimal);
    TypedRowSpan span;if(typed_row_span(view,index,&span))return -1;
    const TypedField *f=&view->schema->fields[index];
    const PkrField *pf=&view->schema->packed.fields[index];
    if(span.is_null||f->removed){text->data="null";text->len=4;return 0;}
    if(pf->kind==PKR_VARCHAR){
        text->data=(const char*)span.data;text->len=span.len;text->quoted=1;return 0;
    }
    if(pf->kind==PKR_NUMERIC){
        dec_status st=dec_unpack(span.data,span.len,&text->decimal);
        if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EILSEQ;return -1;}
        size_t cap=dec_text_len(&text->decimal);
        if(!cap){errno=EILSEQ;return -1;}
        text->numeric=1;text->len=cap-1;return 0;
    }
    if(f->type==FT_ENUM){
        int i=f->enum_width==2?(int)(((uint16_t)span.data[0]<<8)|span.data[1]):span.data[0];
        /* Keep the legacy renderer's corrupt-enum empty-string policy. */
        text->data=i>=0&&i<f->n_enum_values&&f->enum_values&&f->enum_values[i]?
            f->enum_values[i]:"";
        text->len=strlen(text->data);text->quoted=1;return 0;
    }
    int n=decode_field_to_buf(f,span.data,text->fixed,sizeof(text->fixed),1);
    if(n<1||(size_t)n>=sizeof(text->fixed)){errno=EILSEQ;return -1;}
    text->data=text->fixed;text->len=(size_t)n;
    /* These fixed string renderings contain only their canonical ASCII. */
    if(n>=2&&text->fixed[0]=='"'&&text->fixed[n-1]=='"'){
        text->data++;text->len-=2;text->quoted=1;
    }
    return 0;
}
static int quoted_length(const char *data,size_t n,size_t *out){
    size_t len=2;
    for(size_t i=0;i<n;i++){
        unsigned char c=(unsigned char)data[i];
        size_t add=(c=='"'||c=='\\'||c=='\b'||c=='\f'||c=='\n'||c=='\r'||c=='\t')?2:
            c<0x20?6:1;
        if(len>SIZE_MAX-add){errno=EOVERFLOW;return -1;}len+=add;
    }
    *out=len;return 0;
}
static int quote_into(const char *data,size_t n,char *out,size_t cap,size_t *used){
    size_t need=0;if(quoted_length(data,n,&need))return -1;
    if(need>cap){errno=EOVERFLOW;return -1;}
    out[0]='"';int count=json_escape_into(out+1,need-2,data,n);
    if(count<0||(size_t)count!=need-2){errno=EOVERFLOW;return -1;}
    out[need-1]='"';*used=need;return 0;
}
static int row_text_length(const RowText *text,int json,size_t *out){
    if(json&&text->quoted)return quoted_length(text->data,text->len,out);
    *out=text->len;return 0;
}
static int row_text_write(const RowText *text,int json,char *out,size_t cap,size_t *used){
    *used=0;
    if(text->numeric){
        dec_status st=dec_to_text(&text->decimal,out,cap,used);
        if(st!=DEC_OK){errno=EOVERFLOW;return -1;}return 0;
    }
    if(json&&text->quoted)return quote_into(text->data,text->len,out,cap,used);
    if(text->len>cap){errno=EOVERFLOW;return -1;}
    if(text->len)memcpy(out,text->data,text->len);
    *used=text->len;return 0;
}
char *typed_packed_field_text(const TypedSchema *ts,const uint8_t *value,size_t len,
                               uint8_t hdr2,size_t index,size_t *text_len){
    if(text_len)*text_len=0;
    if(!text_len||!packed_schema_ok(ts)||index>=(size_t)ts->nfields){errno=EINVAL;return NULL;}
    TypedRowView view;TypedRowSpan span;
    if(typed_row_open(ts,value,len,hdr2,&view)||typed_row_span(&view,index,&span))return NULL;
    if(span.is_null||ts->fields[index].removed){errno=0;return NULL;}
    RowText text;char *out=NULL;
    if(row_text_open(&view,index,&text)){row_text_free(&text);return NULL;}
    if(text.len==SIZE_MAX){errno=EOVERFLOW;goto done;}
    out=malloc(text.len+1);if(!out){errno=ENOMEM;goto done;}
    size_t used=0;
    if(row_text_write(&text,0,out,text.len+1,&used)){free(out);out=NULL;goto done;}
    out[used]=0;*text_len=used;
done:row_text_free(&text);return out;
}
char *typed_packed_decode(const TypedSchema *ts,const uint8_t *value,size_t len,
                           uint8_t hdr2,size_t budget){
    /* Budget is exact JSON output bytes including the terminating NUL.
       Fail before any output-sized allocation, including for empty objects. */
    if(budget<3){errno=ENOMEM;return NULL;}
    if(!packed_schema_ok(ts)){errno=EINVAL;return NULL;}
    TypedRowView view;if(typed_row_open(ts,value,len,hdr2,&view))return NULL;
    size_t need=3;int first=1;
    for(int i=0;i<ts->nfields;i++){
        const TypedField *f=&ts->fields[i];if(f->removed)continue;
        size_t name=0,part=0;RowText text;
        if(quoted_length(f->name,(size_t)f->name_len,&name))return NULL;
        int rc=row_text_open(&view,(size_t)i,&text);
        if(!rc)rc=row_text_length(&text,1,&part);
        row_text_free(&text);if(rc)return NULL;
        size_t sep=first?1:2;
        if(name>SIZE_MAX-sep||part>SIZE_MAX-name-sep||need>SIZE_MAX-name-part-sep){
            errno=EOVERFLOW;return NULL;
        }
        need+=name+part+sep;
        if(need>budget){errno=ENOMEM;return NULL;}first=0;
    }
    char *out=malloc(need);if(!out){errno=ENOMEM;return NULL;}
    size_t pos=0;out[pos++]='{';first=1;
    for(int i=0;i<ts->nfields;i++){
        const TypedField *f=&ts->fields[i];if(f->removed)continue;
        if(!first)out[pos++]=',';
        first=0;size_t used=0;
        if(quote_into(f->name,(size_t)f->name_len,out+pos,need-pos,&used))goto fail;
        pos+=used;out[pos++]=':';
        RowText text;int rc=row_text_open(&view,(size_t)i,&text);
        if(!rc)rc=row_text_write(&text,1,out+pos,need-pos,&used);
        row_text_free(&text);if(rc)goto fail;
        pos+=used;
    }
    out[pos++]='}';out[pos]=0;
    if(pos+1!=need){errno=EILSEQ;goto fail;}return out;
fail:free(out);return NULL;
}

```

## Appendix D — scratch probe and complete stage-4 documentation

<!-- block:probe-c -->
```c
#include "typed_row.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void *__real_malloc(size_t);
void *__real_calloc(size_t,size_t);
void *__real_realloc(void*,size_t);
void __real_free(void*);
static void *live[8192];
static size_t calls,fail_at,alive,largest;
static int armed;
static void track(void *p,size_t n){
    if(!p||!armed)return;
    for(size_t i=0;i<8192;i++)if(!live[i]){live[i]=p;alive++;if(n>largest)largest=n;return;}
    abort();
}
static void untrack(void *p){
    if(!p)return;
    for(size_t i=0;i<8192;i++)if(live[i]==p){live[i]=NULL;alive--;return;}
}
static int fail(size_t n){
    if(!armed)return 0;
    calls++;if(n>largest)largest=n;
    if(calls==fail_at){errno=ENOMEM;return 1;}return 0;
}
void *__wrap_malloc(size_t n){if(fail(n))return NULL;void *p=__real_malloc(n);track(p,n);return p;}
void *__wrap_calloc(size_t n,size_t z){
    if(z&&n>SIZE_MAX/z){errno=ENOMEM;return NULL;}
    if(fail(n*z))return NULL;
    void *p=__real_calloc(n,z);track(p,n*z);return p;
}
void *__wrap_realloc(void *old,size_t n){
    if(fail(n))return NULL;
    void *p=__real_realloc(old,n);if(p||!n){untrack(old);track(p,n);}return p;
}
void __wrap_free(void *p){untrack(p);__real_free(p);}
static void begin(size_t at){if(alive)abort();calls=0;fail_at=at;largest=0;armed=1;}
static size_t finish(void){armed=0;if(alive){fprintf(stderr,"leaked allocations: %zu\n",alive);exit(1);}return calls;}
static TypedSchema *make_schema(const char *const *types,size_t n){
    TypedSchema *ts=calloc(1,sizeof(*ts));if(!ts)abort();ts->typed=1;ts->nfields=(int)n;
    for(size_t i=0;i<n;i++){
        if(!strncmp(types[i],"varchar",7))ts->fields[i].type=FT_VARCHAR;
        else if(!strncmp(types[i],"numeric",7))ts->fields[i].type=FT_NUMERIC;
        else parse_field_type(types[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%zu",i);
        ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    if(typed_schema_bind_packed(ts,types,n,NULL,0))abort();
    return ts;
}
static size_t do_case(int which,size_t at,const TypedSchema *ts,const SlotcaskOldRecord *old){
    char err[256];uint8_t *out=(void*)1;size_t len=99;char *text=NULL;int rc;
    TypedInput in[MAX_FIELDS]={0};in[0]=(TypedInput){"larger text",11,1,0,0};
    begin(at);
    if(which==0)rc=typed_encode_alloc(ts,"{\"f0\":\"a\\u0000b\",\"f1\":\"12345.6789\",\"f2\":9}",1,NULL,NULL,&out,&len,err,sizeof(err));
    else if(which==1)rc=typed_update_alloc(ts,old,in,0,&out,&len,err,sizeof(err));
    else if(which==2){text=typed_packed_decode(ts,old->value,old->vlen,old->hdr2,4096);rc=text?0:-1;out=NULL;len=0;}
    else rc=typed_transfer_ac_alloc(ts,old,old->value,old->vlen,&out,&len,err,sizeof(err));
    if(at&&calls>=at){
        if(rc>=0||out||len||text||errno!=ENOMEM){fprintf(stderr,"case %d failure %zu not propagated\n",which,at);exit(1);}
    }else if(rc){fprintf(stderr,"case %d baseline failed %s\n",which,err);exit(1);}
    free(out);free(text);return finish();
}

static size_t bind_case(TypedSchema *ts,size_t at){
    TypedSchema before=*ts;const char *tokens[]={"enum(red,blue)"};char err[256];
    begin(at);errno=0;
    int rc=typed_schema_bind_packed(ts,tokens,1,err,sizeof(err));
    if(at&&calls>=at){
        if(rc>=0||errno!=ENOMEM||memcmp(ts,&before,sizeof(*ts))){
            fprintf(stderr,"binder OOM %zu not atomic or not ENOMEM\n",at);exit(1);
        }
    }else if(rc){fprintf(stderr,"binder baseline failed\n");exit(1);}
    return finish();
}
static size_t generator_calls;
const char *__wrap_generate_default(const TypedField *f,char *buf,size_t cap,const char *root,const char *obj){
    (void)buf;(void)cap;(void)root;(void)obj;generator_calls++;
    if(f->default_kind==DK_SEQ)return "7";
    if(f->default_kind==DK_RANDOM)return "abc";
    return f->default_val;
}
int main(void){
    const char *types[]={"varchar","numeric:26,10","int"};TypedSchema *ts=make_schema(types,3);
    uint8_t *old=NULL;size_t len=0;char err[256];
    if(typed_encode_alloc(ts,"{\"f0\":\"old\",\"f1\":\"1.25\",\"f2\":3}",1,NULL,NULL,&old,&len,err,sizeof(err)))return 1;
    SlotcaskOldRecord record={old,len,PKR_FLAGS};
    for(int which=0;which<4;which++){
        size_t count=do_case(which,0,ts,&record);
        for(size_t i=1;i<=count;i++)do_case(which,i,ts,&record);
        printf("OOM case %d: %zu allocation failures propagated, no tracked leaks\n",which,count);
    }
    free(old);free(ts);
    const char *enum_types[]={"enum(red,blue)"};ts=make_schema(enum_types,1);
    size_t binder_calls=bind_case(ts,0);
    for(size_t i=1;i<=binder_calls;i++)bind_case(ts,i);
    printf("binder: %zu allocation failures preserve schema and ENOMEM\n",binder_calls);
    free_enum_values(&ts->fields[0]);free(ts);
    const char *one[]={"varchar"};ts=make_schema(one,1);
    TypedPrepared prep;typed_prepared_init(&prep);
    if(typed_prepared_zero(ts,&prep,err,sizeof(err)))return 1;
    char *large=malloc(PKR_VARCHAR_MAX);if(!large)return 1;memset(large,'x',PKR_VARCHAR_MAX);
    TypedInput input={large,PKR_VARCHAR_MAX,1,0,0};
    if(typed_prepared_set(&prep,0,&input,err,sizeof(err)))return 1;
    uint8_t *row=NULL;size_t rn=0;if(typed_prepared_alloc(&prep,&row,&rn,err,sizeof(err)))return 1;
    begin(0);errno=0;char *text=typed_packed_decode(ts,row,rn,PKR_FLAGS,8);
    if(text||errno!=ENOMEM||calls||largest){fprintf(stderr,"budget allocated %zu bytes\n",largest);return 1;}
    finish();puts("4 MiB varchar with 8-byte budget: 0 output allocations");
    typed_prepared_free(&prep);free(row);free(large);
    const char *gen_types[]={"int","varchar"};free(ts);ts=make_schema(gen_types,2);
    ts->fields[0].default_kind=DK_SEQ;ts->fields[1].default_kind=DK_RANDOM;
    typed_prepared_init(&prep);generator_calls=0;
    if(typed_prepared_zero(ts,&prep,err,sizeof(err))||typed_prepared_defaults(&prep,"root","obj",err,sizeof(err)))return 1;
    if(generator_calls!=2)return 1;
    for(int i=0;i<3;i++){
        if(typed_prepared_defaults(&prep,"root","obj",err,sizeof(err)))return 1;
        size_t actual=0;if(pkr_measure(&ts->packed,prep.cells,2,&actual)!=DEC_OK)return 1;
        uint8_t *bytes=NULL;size_t n=0;if(typed_prepared_alloc(&prep,&bytes,&n,err,sizeof(err)))return 1;
        free(bytes);
    }
    if(generator_calls!=2)return 1;
    puts("sequence/random: 2 calls for 2 defaults, unchanged by repeated defaults/measure/compose");
    typed_prepared_free(&prep);free(ts);
    printf("sizeof(TypedSchema)=%zu sizeof(PkrSchema)=%zu sizeof(TypedPrepared)=%zu\n",sizeof(TypedSchema),sizeof(PkrSchema),sizeof(TypedPrepared));
    return 0;
}

```

<!-- block:row-doc -->
```markdown
# Inactive typed-row adapter

The internal `typed_row` module connects an explicitly bound in-memory schema
with the packed-record codec. It preserves exact decimal text and arbitrary
varchar bytes, including embedded NUL, through length-aware APIs. Existing
loaders, objects, commands and SDK entry points still use the legacy format.
This preparation does not activate packed storage or migrate any object.

`typed_schema_bind_packed` accepts already parsed names/defaults and type-only
tokens. It binds a by-value `PkrSchema` after validating the entire declaration;
failure preserves the schema. An all-zero packed member means legacy. Bound
packed schemas must only reach the new adapters and codec until each old
consumer has been converted. Passing one to a legacy raw-offset API is unsafe.
The ordinary loader does not call this binder.

`typed_row_open` rejects schema/record generation mismatches before returning
borrowed field spans. Packed varchar spans contain content, not a uint16 prefix;
packed numeric spans contain decnum payloads, not scaled int64. Legacy spans
retain actual stored length. Legacy reading substitutes whole-field zero for
incomplete fields; legacy update carry copies available bytes then zero pads.
These two historical behaviors remain separate operations.

A `TypedPrepared` owns changed field bytes and borrows unchanged OLD bytes.
Schema, enum strings and borrowed records must remain immutable and alive until
composition completes. Initialize before use; free is idempotent. A failed
field assignment preserves its previous cell, owner, seen marker and byte count.
Defaults and timestamp passes are incremental; discard the prepared value on
failure. Sequence/random defaults are generated once and retained. Measuring,
allocating or composing a prepared row never regenerates them.

The allocating APIs return actual-length malloc/free-compatible buffers. The
nonallocating composer receives actual destination capacity and leaves its bytes
untouched on failure. Neither uses a schema-maximum row allocation for packed
schemas. Legacy adapters retain the existing fixed-row allocation. Writers must
still check `pkr_record_fits` with the actual key before mutation; value preparation
alone cannot know the final physical fit.

Packed JSON collection validates the complete document, including ignored values, with
a 64-container nesting bound. Field names are JSON-decoded before matching.
Unknown fields are ignored after validation. Every supplied occurrence of a
known field must validate; the last valid duplicate wins. Known field values
must be scalar. Quoted varchar is decoded as a byte span; numeric and fixed
string scalars retain their existing checked conversion policies. This is not a
calendar, boolean, UUID or enum semantics overhaul.

`typed_packed_field_text` returns a length alongside owned bytes; strlen cannot
represent a binary varchar. NULL/removed fields return NULL with errno zero.
`typed_packed_decode` validates one record view, measures exact escaped JSON,
checks its output-byte budget (including NUL), then allocates one output buffer.
A tiny budget does not cause a large varchar copy or escape allocation. Decimal
validation/render workspaces are separately bounded by the decimal limits.

The schema extension costs 7184 bytes per TypedSchema on the verified x86_64
build (about 1.75 MiB across 256 cache slots). Prepared objects cost 8464 stack
bytes plus actual changed-field allocations. These are measured layout costs,
not throughput claims. No benchmark has been run.

This child does not convert production write/read consumers. Subsequent Phase
3B children must integrate actual-size callback ownership, single/bulk writes,
projections, scans, indexes, joins, maintenance and rebuilds, with explicit
errors for private unsupported numeric semantics until Phases 5/6. Public
activation, authoritative metadata and durable publication remain later work.

New adapter boundaries reject NULL JSON before legacy delegation and establish
failure errno even when existing scalar helpers return status/text only. Invalid
scalar/span inputs report EINVAL, malformed OLD reports EILSEQ, and allocation
failures report ENOMEM. Enum type binding rejects trailing declaration text and
preserves schema bytes and ENOMEM on temporary enum-parser allocation failure.

```

<!-- block:codec-doc-old -->
```markdown
The internal packed_record module prepares future exact numeric/varchar records.
It has no production typed-schema adapter or public activation path. Existing
objects, declarations and commands keep their current behavior. Do not manually
put packed values into a public object.
```

<!-- block:codec-doc-new -->
```markdown
The internal packed_record module prepares future exact numeric/varchar records.
The inactive [typed-row adapter](typed-row-adapter.md) binds explicit in-memory
schemas for later consumer integration. No loader/create-object or public
activation path uses packed storage. Existing objects, declarations and commands
keep their current behavior. Do not manually put packed values into a public object.
```

## Appendix E — immutable block checksums

| Block | SHA256 |
|---|---|
| row-header | `4d19ffa97c167f4953fbf20f6acbb6a471fab80bcc1db641718fb8bd8b9744b0` |
| row-tests | `26727e7b70d2f728036bed6c6817d05f6fc44a17644ea90b0f2031d9c04baa4c` |
| row-stub | `1a6a4d87bde84094f44e3c10e7de039128e4d3a457ea08396623fc330e2e9c52` |
| row-views | `0c803fcfcb972217da99932d232b90fbbff80e176acc333646f305d9f31e12a0` |
| row-core | `4aa9038941595e2e26e8566835324387d52700aaf8f7d52f945e254130265fab` |
| probe-c | `02c7a615f94a33067ef83cce4a3b15e468939de44b006751ef49ae34a6dbc0b4` |


## Appendix F — historical Revision 1 planning ledger (2026-10-25)

This section records Revision 1, before the later blind-review corrections. Its counts and source-byte identity are historical; Appendix G is authoritative for Revision 2.

Base archive: b9cea93fd7e314eeb7505675e7d2212da850e184. Revision 1 replay tree:
`/tmp/phase3b-plan-hqmc1qc9/literal-replay`. All source/header/test/doc
blocks in that tree were compared byte-for-byte with Revision 1 at that time;
they are not the current Revision 2 blocks.
The stage-2 and stage-3 replacements and stage-4 docs were applied by the
extracted driver. Stage-1 edits were additionally replayed independently by
the mechanical reviewer: every old anchor occurred exactly once.

| Command / stage | Actual result | Exit |
|---|---|---|
| Task 1 `rtk proxy env SKIP_TESTS=1 ./build.sh` | zero warnings/errors | 0 |
| Task 1 `rtk proxy ./build/bin/shard-db-test run test-typed-row-adapter` | **2 passed, 15 failed**; unsupported binder/materialization, no crash/link failure | 15 |
| Task 2 same build | zero warnings/errors | 0 |
| Task 2 same focused run | **59 passed, 26 failed**; views/materialization green, preparation/render still RED | 26 |
| Task 3 same build | zero warnings/errors | 0 |
| Task 3 same focused run | **181 passed, 0 failed** | 0 |
| Task 3 `rtk proxy ./build/bin/shard-db-test run-all` | **18997 passed, 0 failed across 519 cases** | 0 |
| Task 4 exact probe compilation with separate non-LTO module | `-Wall -Wextra -Werror`, no warnings/errors | 0 |
| Task 4 probe | encode 21 / update 11 / decode 8 / transfer 13 allocation failures propagated, no tracked leaks | 0 |

The full suite used the socket-capable execution mode because its daemon
cases require localhost sockets. It ran once against the frozen final code
and test file; no failed functional run was retried into success. An earlier
prototype full suite (before additional tests) also passed, but is not used
as the final gate.

Final probe stdout (expected injected-allocation stderr from the existing
JSON helper is retained in the raw scratch log):

```text
OOM case 0: 21 allocation failures propagated, no tracked leaks
OOM case 1: 11 allocation failures propagated, no tracked leaks
OOM case 2: 8 allocation failures propagated, no tracked leaks
OOM case 3: 13 allocation failures propagated, no tracked leaks
4 MiB varchar with 8-byte budget: 0 output allocations
sequence/random: 2 calls for 2 defaults, unchanged by repeated defaults/measure/compose
sizeof(TypedSchema)=154656 sizeof(PkrSchema)=7184 sizeof(TypedPrepared)=8464
```

Independent mechanical check: literal Appendix A bootstrap succeeds; 37 unique
blocks; all six checksum rows match; all 13 anchors unique at the pinned base;
stages 1–4 apply; all 21 module functions supplied at every stage; all five
source lists plus test registration present; four config bodies unchanged
except linkage. Review found two document issues, now resolved: missing actual
ledger (this appendix), and the need to qualify the /tmp rule as scratch probe
binaries rather than normal build products.

Independent code review confirmed the final escaped-key, strict ignored-JSON
and TAB/CR fixes, and found no further concrete issue within this child scope.
It checked parser boundary behavior, allocation ownership and cleanup; its
validator guard-page probe exercised 108 NUL-terminated input prefixes. That
review does not certify the future write/read consumers left out of this child.

Planning corrections are disclosed: the first generator-count probe linked
only the LTO archive, allowing inlining to bypass its wrapper and invoke the
real sequence helper in an uninitialized standalone process. That probe was
invalid and failed; its empty sequence-lock fixture was removed. The final
non-LTO-module probe above wraps the helper and passes. An intermediate RED
stub build also exposed an uninitialized test destination; it now starts with
an explicit canary, and the frozen RED sequence was rerun from the start.
Neither failed prototype is counted as final verification.

Raw logs remain outside version control under `/tmp/phase3b-plan-hqmc1qc9/`:
`frozen1-build.log`, `frozen1-red.log`, `frozen2-build.log`, `frozen2-red.log`,
`frozen3-build.log`, `frozen3-green.log`, `frozen3-full-suite.log`,
`final-probe.log`. These temporary paths are convenience evidence, never an
execution dependency: all required sources/scripts are embedded above.

No production source edit, sanitizer, benchmark, commit, staging, push or
activation occurred. The protected hash was checked without reading contents
and remained exact. Only this proposed child and its work-index update belong
to the planning diff.

## Appendix G — authoritative Revision 2 verification (2026-10-25)

The fresh blind and contract reviews are recorded in
[the review report](2026-10-25-phase3b1-blind-review.md). Both reviewers rechecked
the fixes with focused probes and found no remaining concrete issue in their
reviewed scope. Production sources remain untouched; all compilation/execution
below occurred in scratch copies.

Regression proof against the previous candidate: normal zero-warning build,
then the expanded case returned **201 passed / 13 failed, exit 13**. Failures
reproduced stale/absent errno, accepted enum suffixes and the isolated NULL-JSON
crash. A supplemental allocation probe linked against the preserved unfixed
module returned exit 1 at the first enum-parser allocation failure (EINVAL
instead of ENOMEM); linking the repaired module passes all three injections.

The complete revised plan was then replayed from a fresh archive of
`b9cea93fd7e314eeb7505675e7d2212da850e184` at `/tmp/p3b1-r2-replay`, applying
stages 1–4 using the document's extracted driver. Header, final module, test
and concept document were compared byte-for-byte with the current blocks.
All 37 block IDs remain unique; all six Appendix E checksums match. Every
stage-1 anchor matched exactly once, and no source was retyped.

| Gate / actual command | Actual result | Exit |
|---|---|---|
| Task 1 `rtk proxy env SKIP_TESTS=1 ./build.sh` | zero warnings/errors | 0 |
| Task 1 `rtk proxy ./build/bin/shard-db-test run test-typed-row-adapter` | **13 passed / 23 failed**, expected unsupported stubs | 23 |
| Task 2 same build | zero warnings/errors | 0 |
| Task 2 same focused run | **84 passed / 34 failed**, expected preparation/render stubs | 34 |
| Task 3 same build | zero warnings/errors | 0 |
| Task 3 same focused run | **214 passed / 0 failed** | 0 |
| Task 3 `rtk proxy ./build/bin/shard-db-test run-all` | **19030 passed / 0 failed across 519 cases** | 0 |
| Task 4 separate-module and probe compilation | `-O1`, no LTO for module, `-Wall -Wextra -Werror`, no warnings/errors | 0 |
| Task 4 exact extracted probe | all 56 injected allocation failures propagate; ownership/error/budget/default checks pass | 0 |
| Task 4 exact documentation stage | both concept-document changes apply | 0 |

Final probe stdout (expected injected-failure stderr from the existing JSON
helper remains in the raw scratch log):

```text
OOM case 0: 21 allocation failures propagated, no tracked leaks
OOM case 1: 11 allocation failures propagated, no tracked leaks
OOM case 2: 8 allocation failures propagated, no tracked leaks
OOM case 3: 13 allocation failures propagated, no tracked leaks
binder: 3 allocation failures preserve schema and ENOMEM
4 MiB varchar with 8-byte budget: 0 output allocations
sequence/random: 2 calls for 2 defaults, unchanged by repeated defaults/measure/compose
sizeof(TypedSchema)=154656 sizeof(PkrSchema)=7184 sizeof(TypedPrepared)=8464
```

The normal full suite ran once against the final revised code/test file in a
socket-capable execution mode for daemon cases. No functional failure was
rerun into success. No sanitizers or benchmarks were run; their timing remains
unchanged. Later edits only synchronized documentation and this ledger.

Raw evidence is outside version control under `/tmp/p3b1-blind-review/`:
`review-red-build.log`, `review-red.log`, `r2-task1-build.log`,
`r2-task1-red.log`, `r2-task2-build.log`, `r2-task2-red.log`,
`r2-task3-build.log`, `r2-task3-green.log`, `r2-full-suite.log`,
`probe-r2-unfixed.log`, `final-r2-probe.log`. These temporary paths are not
execution dependencies. All required files/commands remain embedded here.

Only planning/index/review documentation changed in the production working
tree. HEAD remains b9cea93fd7e314eeb7505675e7d2212da850e184. The protected
certification checksum remains exact; contents were never read. No staging,
commit, push, PR, migration, metadata publication or packed activation occurred.


## Appendix H — Revision 3: checked enum membership after decoding

The user authorized the planner to amend and execute this small raw-review fix.
Base is the completed, uncommitted Revision 2 output at HEAD b9cea93.
Root cause: the packed collector delegates fixed encoding to a legacy helper
which assumes its caller has already checked enum membership. An unknown value
therefore becomes member zero, and an invalid early duplicate can be hidden
by a later valid member. Validate nonempty decoded spans before allocation or
cell replacement through existing enum_value_index; no new helper export.
The same setter covers packed JSON, direct set, defaults, updates and stamps.
No signature, format, legacy route or activation change. Empty spans retain
legacy member-zero behavior; explicit NULL retains null handling.

### H1 — Introduce regressions, build and capture RED before the fix

Replace this unique runner anchor in src/test/cases/test_typed_row_adapter.c:

<!-- block:enum-tests-old -->
```c
static int test_typed_row_adapter_run(void){
    encoding();updates_and_reuse();legacy_views();maximum_and_binary();review_regressions();schema_and_failure_contracts();decimal_limits_and_types();json_contracts();automatic_fields();blind_review_boundaries();
    return t_ctx->failed?1:0;
}
```

with the complete regression function and runner:

<!-- block:enum-tests-new -->
```c
static void enum_membership_regressions(void){
    const char *types[]={"enum(red,blue)"};
    TypedSchema *ts=schema_new(types,1);ASSERT_NOT_NULL(ts,"enum membership schema");if(!ts)return;
    char err[256];uint8_t *out=NULL;size_t n=0;
    const char *bad[]={"{\"f0\":\"bogus\"}","{\"f0\":\"bogus\",\"f0\":\"blue\"}","{\"f0\":true}"};
    for(size_t i=0;i<sizeof(bad)/sizeof(bad[0]);i++){
        out=(void*)1;n=99;errno=0;
        ASSERT_EQ_INT(typed_encode_alloc(ts,bad[i],1,NULL,NULL,&out,&n,err,sizeof(err)),-1,"unknown enum rejects before coercion or duplicate replacement");
        ASSERT_EQ_INT(errno,EINVAL,"unknown enum is invalid input");
        ASSERT_TRUE(!out&&!n,"unknown enum clears owned outputs");
        free(out&&out!=(void*)1?out:NULL);
    }
    const char *good[]={"{\"f0\":\"\\u0062lue\"}","{\"f0\":\"\"}","{\"f0\":null}"};
    const char *expected[]={"{\"f0\":\"blue\"}","{\"f0\":\"red\"}","{\"f0\":null}"};
    for(size_t i=0;i<sizeof(good)/sizeof(good[0]);i++){
        out=NULL;n=0;
        ASSERT_EQ_INT(typed_encode_alloc(ts,good[i],1,NULL,NULL,&out,&n,err,sizeof(err)),0,"escaped member empty and null retain contracts");
        if(out){
            char *text=typed_packed_decode(ts,out,n,PKR_FLAGS,1024);
            ASSERT_NOT_NULL(text,"accepted enum renders");
            if(text){ASSERT_EQ_STR(text,expected[i],"accepted enum value exact");free(text);}
            free(out);
        }
    }
    TypedPrepared p;typed_prepared_init(&p);
    ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"enum prepared zero");
    TypedInput valid={"blue",4,1,0,0},invalid={"bogus",5,1,0,0};
    ASSERT_EQ_INT(typed_prepared_set(&p,0,&valid,err,sizeof(err)),0,"enum set valid member");
    PkrCell cell=p.cells[0];uint8_t *owner=p.owned[0],seen=p.seen[0];size_t bytes=p.bytes;
    uint8_t saved[2]={0};ASSERT_TRUE(cell.len<=sizeof(saved),"enum snapshot bounded");
    if(cell.len<=sizeof(saved)&&cell.data)memcpy(saved,cell.data,cell.len);
    errno=0;
    ASSERT_EQ_INT(typed_prepared_set(&p,0,&invalid,err,sizeof(err)),-1,"direct unknown enum rejects");
    ASSERT_EQ_INT(errno,EINVAL,"direct rejection errno");
    ASSERT_TRUE(p.cells[0].data==cell.data&&p.cells[0].len==cell.len&&p.cells[0].is_null==cell.is_null,"rejected setter preserves cell");
    ASSERT_TRUE(p.owned[0]==owner&&p.seen[0]==seen&&p.bytes==bytes,"rejected setter preserves ownership seen and size");
    ASSERT_TRUE(p.cells[0].len==cell.len&&cell.len<=sizeof(saved)&&!memcmp(p.cells[0].data,saved,cell.len),"rejected setter preserves bytes");
    typed_prepared_free(&p);
    ts->fields[0].default_kind=DK_LITERAL;strcpy(ts->fields[0].default_val,"blue");
    ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"enum zero for default eligibility");
    ASSERT_EQ_INT(typed_prepared_set(&p,0,&invalid,err,sizeof(err)),-1,"invalid supplied enum does not count as seen");
    ASSERT_EQ_INT(p.seen[0],0,"failed setter leaves omission eligible");
    ASSERT_EQ_INT(typed_prepared_defaults(&p,NULL,NULL,err,sizeof(err)),0,"default after rejected setter");
    out=NULL;n=0;
    ASSERT_EQ_INT(typed_prepared_alloc(&p,&out,&n,err,sizeof(err)),0,"compose default enum");
    if(out){
        char *text=typed_packed_decode(ts,out,n,PKR_FLAGS,1024);
        ASSERT_NOT_NULL(text,"default enum renders");
        if(text){ASSERT_EQ_STR(text,"{\"f0\":\"blue\"}","unseen default applied");free(text);}
        SlotcaskOldRecord old={out,n,PKR_FLAGS};TypedInput inputs[MAX_FIELDS]={0};inputs[0]=invalid;
        uint8_t *next=(void*)1;size_t nextn=99;errno=0;
        ASSERT_EQ_INT(typed_update_alloc(ts,&old,inputs,0,&next,&nextn,err,sizeof(err)),-1,"unknown enum rejects update");
        ASSERT_EQ_INT(errno,EINVAL,"enum update invalid input");
        ASSERT_TRUE(!next&&!nextn,"enum update clears outputs");
        free(next&&next!=(void*)1?next:NULL);
        text=typed_packed_decode(ts,out,n,PKR_FLAGS,1024);
        ASSERT_NOT_NULL(text,"OLD remains readable");
        if(text){ASSERT_EQ_STR(text,"{\"f0\":\"blue\"}","failed enum update preserves OLD");free(text);}
        free(out);
    }
    typed_prepared_free(&p);
    strcpy(ts->fields[0].default_val,"bogus");out=(void*)1;n=99;errno=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{}",1,NULL,NULL,&out,&n,err,sizeof(err)),-1,"unknown enum default rejects");
    ASSERT_EQ_INT(errno,EINVAL,"invalid enum default errno");
    ASSERT_TRUE(!out&&!n,"invalid enum default clears outputs");
    free(out&&out!=(void*)1?out:NULL);
    schema_free(ts);
}

static int test_typed_row_adapter_run(void){
    encoding();updates_and_reuse();legacy_views();maximum_and_binary();review_regressions();schema_and_failure_contracts();decimal_limits_and_types();json_contracts();automatic_fields();blind_review_boundaries();enum_membership_regressions();
    return t_ctx->failed?1:0;
}
```

Run rtk proxy env SKIP_TESTS=1 ./build.sh, then
rtk proxy ./build/bin/shard-db-test run test-typed-row-adapter.
The build must be warning-free and the new unknown-value assertions must fail
against Revision 2; capture actual counts/output, never weaken assertions.
All original 214 assertions remain. RED counts are measured, not forecast gates.

### H2 — Apply the membership guard

In src/db/typed_row.c, replace the unique anchor:

<!-- block:enum-fix-old -->
```c
    }else{
        if(!sentinel_span_parses(f->type,text,n)){
```

with:

<!-- block:enum-fix-new -->
```c
    }else{
        if(f->type==FT_ENUM&&n&&enum_value_index(f,text,n)<0){
            if(err&&cap)snprintf(err,cap,"unknown enum value for field '%s'",f->name);
            errno=EINVAL;goto done;
        }
        if(!sentinel_span_parses(f->type,text,n)){
```

The guard runs after one JSON unescape; common done cleanup frees decoded text
and preserves EINVAL. The existing prepared cell, owner, seen and byte count
remain unchanged on rejection. Existing row-core/row-tests blocks and Appendix E
checksums intentionally retain the reviewed Revision 2 stage inputs. A fresh
execution applies H1/H2 after Task 3; never replace those inputs before staging.

### H3 — Verify, review and stop uncommitted

Rebuild warning-free, run focused GREEN, normal full suite, and the exact
Appendix probe with typed_row.c compiled separately without LTO. All tests and
all 56 allocation injections must pass. Capture actual commands/counts in the
evidence ledger and add a measured Revision 3 result here. Re-review the guard
and regressions independently. Keep packed storage inactive, protected file
hash-only and unchanged, and all scratch files in /tmp/p3b1-enum-fix.
No sanitizers, benchmarks, staging, commits or pushes. Stop for review/cleanup.


### H3 measured results — 2026-10-25

GPT-6.1 amended and executed this user-authorized fix; GLM 5.3 remains the
original R2 executor. These are fix-round measurements, not a claim of a new
stage-1/2 replay. Both builds completed with zero warnings/errors.

| Gate | Actual result | Exit |
|---|---|---|
| H1 RED focused case | 232 passed / 24 failed | 24 |
| H2/H3 GREEN focused case | 256 passed / 0 failed | 0 |
| H3 normal full suite | 19072 passed / 0 failed across 519 cases | 0 |
| H3 separately compiled allocation probe | 56/56 injections pass, no tracked leaks | 0 |

The blind reviewer independently rebuilt the original reproduction probe:
all three invalid enum cases now reject with EINVAL and NULL output. No
remaining actionable issue was found. The plan-aware reviewer confirmed exact
H1/H2 source/test correspondence, coherent fresh/resume order and no scope
drift. Automated comparison verified both files are byte-identical to frozen
R2 blocks plus their H edit; all six original block hashes remain valid.
Protected SHA256 remains
fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135
(hash-only). No sanitizer, benchmark, staging, commit or push occurred.
Packed storage remains inactive. Scratch evidence: /tmp/p3b1-enum-fix/.
