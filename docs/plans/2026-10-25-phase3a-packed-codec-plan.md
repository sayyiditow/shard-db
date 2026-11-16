# Phase 3A — inactive packed-record codec and physical transport

## Primary goal

Prepare exact, compact storage for finance values, including numeric:26,10 and
PostgreSQL-sized bare numeric, without changing existing application behavior
while integrations are incomplete.

## Status and approved scope

2026-10-25. Proposed executable child plan, **awaiting approval**.
The user approved splitting Phase 3 into 3A/3B/3C and keeping packed storage
inactive until ready. That split is not approval to execute this code.

3A adds a codec over the committed decimal core, raw physical transport support,
and real record tests. It does not change TypedSchema, load_typed_schema,
create-object, fields.conf, schema.conf, public writes, queries, indexes,
aggregates, defaults, or legacy trim behavior. There is no public flag,
environment switch, automatic migration, or application-visible numeric fix.
Existing and newly created public objects retain their current behavior.

Raw Slotcask tests explicitly use generation3 in private objects. Legacy typed
views still reject packed bytes; normal schema/startup readers still require
version2. 3B integrates consumers; 3C proves metadata publication/downgrade safety.
Activation additionally waits for required Phase 4–6 integrations, a complete
activation plan and explicit approval.

Baseline: fix/phase1-safe-scalar-conversions at
8c0bdf6259d2dc33e540e28e82b9f8010750047d. Continue the reviewed branch series
rather than branch from main and lose Phases 1–2. No git mutation is authorized
by this plan; stop uncommitted for raw-diff review after approved execution.

Protected later plan: docs/plans/2026-10-19-durability-certification-rig.md.
Hash only; never read/edit/move/stage/incorporate/execute.
SHA256 fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.

## Phase 0 — documentation discovery and identity

**How this serves the primary goal:** uses real APIs and preserves existing
application behavior instead of making assumptions about fixtures/storage.

Read AGENTS.md, CORE-PROCESS.md, docs/concepts/typed-records.md,
docs/concepts/storage-model.md and the Phase 3 section of the parent recovery
plan. Protected contents are excluded from all discovery.

Verified allowed interfaces:
- decnum.h: initialized dec_t lifecycle, separate input/output for
  dec_apply_precision, dec_pack/unpack and existing dec_status values.
- test_runner.h/test_assert.h: TEST_REGISTER, thread-local t_ctx, two-argument
  ASSERT_NOT_NULL; guard failed allocations. Isolated raw Slotcask pattern
  follows test_marker_v2.c. No invented TestClient or fixture interface.
- slotcask.h: init/open/close/shutdown, insert/upsert/delete_with_hooks,
  get_ex, exists, walk_live_strict, per-shard bulk and request_execute.
- seg_scan_varlen.h: structural header validation before record traversal.
- build.sh: four module lists: daemon, embedded library, test server, runner.
  Runner's decnum entry is near its start; explicit test registration is
  separate. Bench has no codec caller and gets no source insertion.
- Schema identity remains 3C: global-root schema.conf is authoritative;
  load_typed_schema receives tenant root. Never use substring object lookup.
- Existing typed_record_view is not schema-aware packed validation. It remains
  unchanged here; broadening it alone would expose descriptors as values.

## Mechanism, invariants and consumer inventory

Root causes addressed:
1. Fixed-width numeric cannot hold the exact coefficient. Packed decimals use
   the committed core, with distinct parsed/rounded values to honor its alias
   guard; using the same decimal would always fail constrained assignment.
2. A shared tail cursor corrupts fixed entries, and JSON-order payloads violate
   schema-order validation. Positional cells are measured first; fixed bytes
   occupy directory entries and variable payloads follow declaration order.
3. Schema maximum is a validity bound, not an allocation request. Composition
   takes actual capacity, clears/copies only actual bytes, and validates before
   writing. Tests allocate measured rows, not schema maxima.
4. Physical emitters mask PACKED away. Merely broadening the mask silently
   accepts unknown bits and can emit an unscannable packed-only flag. All
   incoming upserts are prevalidated before staging; compute callbacks are checked
   again before fallback emission; emitters preserve the
   validated byte. Scanner and writer share exact permitted generations.

Frozen contracts:
- Prefix 1+ceil(nfields/8), then field directory. Fixed entries retain checked
  binary widths. Packed entries are uint32 BE offset+length, 8 bytes total;
  offsets are relative to directory start. Payloads are contiguous in schema
  order. No gaps, overlap, trailing garbage, hidden NULL payload or bitmap junk.
- NULL=set bit+zero entry. Empty varchar=clear bit+zero descriptor. Numeric zero
  has a real canonical payload; explicit empty numeric is invalid.
- Phase 2 numeric payload unchanged. Bare limits131072 integer/16383 fractional
  digits; constrained P1..1000,S-1000..1000; currency numeric:19,4 identity.
  Declared negative S stays metadata; stored display scale is nonnegative.
- Bare varchar cap4194304 content bytes; bounded N1..4194304. Parser receives
  TYPE ONLY, never name:type, defaults, modifiers or JSON.
- Fixed cells already contain validated binary. Outputs do not overlap inputs
  or one another; compose also detects output/payload overlap. Failure used=0
  and bytes untouched; failed view cleared. Spans borrow immutable validated
  views/schema. No owned global state/cache is introduced.
- measure has no record-sized allocation. Numeric validation may allocate
  decimal limbs; there is no allocation-free or measured-performance claim.
- Transport accepts 0/1/3 only. Single/direct-bulk transaction and deferred
  request_execute prevalidate every NEW upsert before writing any member.
  Deletes ignore unused NEW generation. Existing synchronous caller-ownership
  rules forbid modifying records while the call runs.
- PkrSchema means packed; its view rejects 0/1/2/unknown. This is not a legacy
  decoder. 3B must enforce both schema/record mismatch directions.
- Actual rows remain bounded by the existing 128 MiB segment ceiling. No cap is
  raised. Schema maxima may be larger while small rows fit. Public over-segment
  error/policy remains a reviewed 3B/3C deliverable before activation.

Maximum arithmetic, header 24/max_key 1024/align8:
| Schema/value | Value bytes | Slot bound |
|---|---:|---:|
|256 bare varchar, all maximum|1073743905|1073744960|
|256 bare numeric, all maximum|18880545|18881600|
|one bare varchar maximum|4194314|4195368|
|256 bare varchar, one maximum payload/others NULL|4196385 actual|4197440|
Last two unaligned physical sizes:4195362/4197433. A256-varchar row with one
four-byte value/others NULL is2085 value bytes; actual key1 gives2112 physical
bytes. Full26-digit numeric occupies16+13=29payload bytes+8descriptor=37
before shared bitmap. Constrained capacity16+ceil(P/2) excludes implicit
decimal zeros. No 1 GiB buffer is needed to test maximum-schema arithmetic.

Affected consumers, exhaustive for this child's changes:
- Ten new pkr_* exports/private helpers; callers only in two new test TUs.
  No production typed adapter. Later adapters need their own complete inventory.
- New SREC constants/predicate: structural scanner; both raw emitters; incoming
  validation in slotcask_bulk_mutation_transaction and request_execute.
  Single insert/upsert and direct per-shard bulk reach the transaction.
  Deferred/merged pipelines originate in request_execute after prevalidation.
  After value_compute, bulk_plan_window_locked revalidates NEW generation
  before fallback staging. Compaction's emitter copies structurally validated records. Pending payloads
  use the second emitter. Public signatures remain unchanged.
- Shared structural scan/recovery accepts the known bit. KFM2 byte-copy replay
  is layout-neutral but pending replay/typed-index correctness is NOT proven
  here; those remain 3C gates. Header comments document raw generations.
- Four build lists+two test entries. No existing test weakened/skipped/replaced.
  TypedField/TypedSchema, config.c views, schema loaders and activation unchanged.

## Execution rules

Do tasks in order only after explicit approval. Keep raw logs/probes/binaries
under a fresh /tmp directory, out of version control. Do not stage/commit/push,
run benches/sanitizers, modify other plans, touch protected contents or begin
3B/3C. Branch-series sanitizer gates stay immediately before eventual push.

Every quoted old block below must match exactly once. Otherwise write
PLAN_NOTES.md with identity, anchor and output, and halt the ENTIRE run.
Unexpected warnings/results, uncovered design decisions or assumptions also
halt. Only planner/human amendment can resume; do not improvise or quietly
retry until green.

Capture RED first with stubs/tests against unchanged transport. Only then apply
the implementation. This red-first order replaces CORE's temporary-revert
example. Builds use rtk proxy env SKIP_TESTS=1 ./build.sh; tests use
rtk proxy ./build/bin/shard-db-test run[-all]. Full normal suite is mandatory
after the complete change; no partial suite substitutes for it.

## Task 0 — baseline and protection

**How this serves the primary goal:** ties the plan to the reviewed Phase 2 state.

Record outputs outside version control. Planning files may be untracked;
tracked production must be clean. On hash mismatch halt without reading it.

```sh
rtk git status --short
rtk git rev-parse HEAD
rtk git branch --show-current
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
```

Read Phase 0 references before editing. Task 1 supplies failing tests before
production behavior changes.

The full suite needs local TCP sockets for isolated test daemons. Check this
prerequisite before building or capturing RED:

```sh
rtk proxy python3 - <<'PY_SOCKET'
import socket
with socket.socket() as test_socket:
    test_socket.bind(("127.0.0.1", 0))
print("local TCP socket preflight: OK")
PY_SOCKET
```

If sandbox policy denies socket creation/bind with EPERM, record the failure
and use the tool's normal permission/approval facility for local test access.
Proceed only after this preflight passes in the same execution environment
used for daemon tests. This documented environment repair does not require a
plan amendment. Do not disable tests, change assertions or silently retry test
failures. If socket access remains unavailable, stop and report that blocker.

## Task 1 — real tests and explicit RED stubs

**How this serves the primary goal:** exposes missing codec and lost/unchecked
physical generation independently.

Create these four files. Stubs return DEC_ERR_INTERNAL after output reset.
Physical regressions run before codec prerequisites so a stub failure cannot
hide the physical-generation RED.

### Task1a — src/db/packed_record.h

Complete file body; SHA256 181abc577b8baa6a51206816cc33fd19e2ac347f3570a2016a04315880a6468a:

```c
/* Inactive packed-record codec. No JSON, schema I/O, global state or activation. */
#ifndef PACKED_RECORD_H
#define PACKED_RECORD_H
#include <stddef.h>
#include <stdint.h>
#include "decnum.h"
#define PKR_MAX_FIELDS 256
#define PKR_VARCHAR_MAX UINT32_C(4194304)
#define PKR_DESCRIPTOR_BYTES 8
#define PKR_FLAGS 0x03

typedef enum { PKR_FIXED = 1, PKR_VARCHAR = 2, PKR_NUMERIC = 3 } PkrKind;
typedef struct {
    PkrKind kind;
    uint32_t width, payload_max, offset;
    int32_t precision, scale;
    uint8_t bare, currency;
} PkrField;
typedef struct {
    PkrField fields[PKR_MAX_FIELDS];
    uint32_t nfields, prefix, directory, maximum;
} PkrSchema;
typedef enum { PKR_INVALID = 0, PKR_NULL = 1, PKR_PRESENT = 2 } PkrState;
typedef struct { const uint8_t *data; size_t len; uint8_t is_null; } PkrCell;
typedef struct {
    const PkrSchema *schema;
    const uint8_t *value;
    size_t len;
} PkrView;
typedef struct { const uint8_t *data; size_t len; PkrState state; } PkrSpan;

/* All outputs must not overlap inputs or one another. Fields and schemas own no allocations.
 * Constructors clear their output on failure. Type parser accepts TYPE ONLY:
 * varchar[:N], numeric[:P[,S]], currency. Other types use fixed(width),
 * where width is the already-validated existing binary width (1..16).
 * P/S, bare/bounded mode and currency identity stay explicit metadata. */
dec_status pkr_field_parse(const char *type, PkrField *out);
dec_status pkr_field_fixed(uint32_t width, PkrField *out);
dec_status pkr_schema_build(const PkrField *fields, size_t n, PkrSchema *out);
/* Prepare exact numeric payloads. Never aliases dec_apply_precision inputs.
 * Empty text is a syntax error. On failure *used=0 and buffer is unchanged. */
dec_status pkr_numeric_encode(const PkrField *field, const char *text, size_t n,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz);
/* Positional cells are in declaration order, not JSON order. Fixed cells
 * contain already-checked binary bytes; variable cells contain raw varchar
 * bytes or exact numeric payloads. NULL requires data=NULL,len=0. Removed
 * fields retain a slot; later adapters supply NULL when recomposing them.
 * Schema/value and view buffers remain immutable while borrowed spans live.
 * measure allocates no record-sized memory. Numeric validation uses decnum.
 * compose validates and measures before writes; cap is ACTUAL capacity.
 * Failure leaves *used=0 and output bytes untouched. Output must not overlap
 * any cell payload; overlap is detected and rejected. */
dec_status pkr_measure(const PkrSchema *schema, const PkrCell *cells, size_t n,
                       size_t *used);
dec_status pkr_compose(const PkrSchema *schema, const PkrCell *cells, size_t n,
                       uint8_t *out, size_t cap, size_t *used);
/* Strict packed-generation view: flags must equal PKR_FLAGS, bitmap width
 * must match schema, offsets are contiguous/in order, no trailing payload,
 * NULL entries are zero, and all numeric payloads satisfy their declaration.
 * A view is cleared on failure; spans are borrowed, including non-NULL data
 * for present-empty varchar. pkr_span requires an unchanged validated view. */
dec_status pkr_view(const PkrSchema *schema, const uint8_t *value, size_t n,
                    uint8_t flags, PkrView *out);
dec_status pkr_span(const PkrView *view, size_t index, PkrSpan *out);
/* Maximum slot bound and actual-row segment gate. Both include existing
 * 24-byte header, actual key bytes, align-8 and floor 32. No cap is changed.
 * Requests/queries remain separately bounded by their existing callers. */
dec_status pkr_slot_bound(const PkrSchema *schema, size_t max_key, size_t *out);
dec_status pkr_record_fits(const PkrSchema *schema, size_t actual, size_t key,
                           size_t segment_max, size_t *record_bytes);
#endif
```

### Task1b — src/db/packed_record.c, RED stub

Complete file body; SHA256 79ead8b01246f2707c817d7630a6ee7c8a9b192f7ab49be81f008575ac73f8c9:

```c
/* Phase 3A RED stub: replace this entire file in Task 2. */
#include "packed_record.h"
#include <string.h>

dec_status pkr_field_parse(const char *type, PkrField *out) {
    (void)type;
    if (out) memset(out, 0, sizeof(*out));
    (void)out;
    return DEC_ERR_INTERNAL;
}

dec_status pkr_field_fixed(uint32_t width, PkrField *out) {
    (void)width;
    if (out) memset(out, 0, sizeof(*out));
    (void)out;
    return DEC_ERR_INTERNAL;
}

dec_status pkr_schema_build(const PkrField *fields, size_t n, PkrSchema *out) {
    (void)fields;
    (void)n;
    if (out) memset(out, 0, sizeof(*out));
    (void)out;
    return DEC_ERR_INTERNAL;
}

dec_status pkr_numeric_encode(const PkrField *field, const char *text, size_t n,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz) {
    (void)field;
    (void)text;
    (void)n;
    (void)out;
    (void)cap;
    if (used) *used = 0;
    (void)used;
    (void)err;
    (void)errsz;
    return DEC_ERR_INTERNAL;
}

dec_status pkr_measure(const PkrSchema *schema, const PkrCell *cells, size_t n,
                       size_t *used) {
    (void)schema;
    (void)cells;
    (void)n;
    if (used) *used = 0;
    (void)used;
    return DEC_ERR_INTERNAL;
}

dec_status pkr_compose(const PkrSchema *schema, const PkrCell *cells, size_t n,
                       uint8_t *out, size_t cap, size_t *used) {
    (void)schema;
    (void)cells;
    (void)n;
    (void)out;
    (void)cap;
    if (used) *used = 0;
    (void)used;
    return DEC_ERR_INTERNAL;
}

dec_status pkr_view(const PkrSchema *schema, const uint8_t *value, size_t n,
                    uint8_t flags, PkrView *out) {
    (void)schema;
    (void)value;
    (void)n;
    (void)flags;
    if (out) memset(out, 0, sizeof(*out));
    (void)out;
    return DEC_ERR_INTERNAL;
}

dec_status pkr_span(const PkrView *view, size_t index, PkrSpan *out) {
    (void)view;
    (void)index;
    if (out) memset(out, 0, sizeof(*out));
    (void)out;
    return DEC_ERR_INTERNAL;
}

dec_status pkr_slot_bound(const PkrSchema *schema, size_t max_key, size_t *out) {
    (void)schema;
    (void)max_key;
    if (out) *out = 0;
    (void)out;
    return DEC_ERR_INTERNAL;
}

dec_status pkr_record_fits(const PkrSchema *schema, size_t actual, size_t key,
                           size_t segment_max, size_t *record_bytes) {
    (void)schema;
    (void)actual;
    (void)key;
    (void)segment_max;
    if (record_bytes) *record_bytes = 0;
    (void)record_bytes;
    return DEC_ERR_INTERNAL;
}
```

### Task1c — src/test/cases/test_packed_record_codec.c

Complete file body; SHA256 c51ba08f9e2acdfb931c9ff3ff38d0b15caf4c0d4b4039e854158d9cc46f181a:

```c
#include "test_runner.h"
#include "test_assert.h"
#include "packed_record.h"
#include "types.h"
#include <stdlib.h>
#include <string.h>
#include <limits.h>
_Static_assert(PKR_MAX_FIELDS == MAX_FIELDS, "packed field-count contract");

static void numeric_case(const char *decl, const char *text, dec_status expected,
                          const char *render) {
    PkrField f; memset(&f, 0, sizeof(f));
    dec_status st = pkr_field_parse(decl, &f);
    ASSERT_EQ_INT(st, DEC_OK, "numeric declaration");
    if (st != DEC_OK) return;
    uint8_t bytes[1024]; memset(bytes, 0xA5, sizeof(bytes));
    size_t used = 99; char err[256];
    st = pkr_numeric_encode(&f, text, strlen(text), bytes, sizeof(bytes), &used,
                             err, sizeof(err));
    ASSERT_EQ_INT(st, expected, "exact numeric encoding status");
    if (st != DEC_OK) {
        ASSERT_EQ_INT(used, 0, "numeric failure clears length");
        ASSERT_EQ_INT(bytes[0], 0xA5, "numeric failure preserves buffer");
        return;
    }
    dec_t d; dec_init(&d); char out[2048]; size_t outn = 0;
    st = dec_unpack(bytes, used, &d);
    ASSERT_EQ_INT(st, DEC_OK, "numeric payload independently decodes");
    if (st == DEC_OK) {
        ASSERT_EQ_INT(dec_to_text(&d, out, sizeof(out), &outn), DEC_OK,
                      "numeric renders");
        ASSERT_EQ_STR(out, render, "exact numeric text");
    }
    dec_free(&d);
}
static void declarations(void) {
    const char *valid[] = {"varchar", "varchar:1", "varchar:1000000",
        "varchar:1000001", "varchar:4194304", "numeric", "numeric:26,10",
        "numeric:3,5", "numeric:4,-2", "numeric:1000,-1000", "currency",
        "numeric:1,+2", "numeric:00000000000000000001"};
    for (size_t i = 0; i < sizeof(valid)/sizeof(valid[0]); i++) {
        PkrField f; memset(&f, 0xA5, sizeof(f));
        ASSERT_EQ_INT(pkr_field_parse(valid[i], &f), DEC_OK, valid[i]);
    }
    const char *invalid[] = {"", "n:numeric", "varchar:0", "varchar:4194305",
        "varchar:-1", "varchar:12garbage", "numeric:0", "numeric:1001",
        "numeric:1,1001", "numeric:1,-1001", "numeric:1,", "numeric:1,2x",
        "numeric:+1", "numeric:9999999999999999999999999999999999999",
        "varchar:9999999999999999999999999999999999999"};
    for (size_t i = 0; i < sizeof(invalid)/sizeof(invalid[0]); i++) {
        PkrField f; memset(&f, 0xA5, sizeof(f));
        ASSERT_EQ_INT(pkr_field_parse(invalid[i], &f), DEC_ERR_INVALID, invalid[i]);
        ASSERT_EQ_INT(f.kind, 0, "invalid declaration clears metadata");
    }
    PkrField a, b;
    ASSERT_EQ_INT(pkr_field_parse(NULL, &a), DEC_ERR_INVALID, "NULL declaration");
    ASSERT_EQ_INT(pkr_field_parse("numeric", NULL), DEC_ERR_INVALID, "NULL field output");
    ASSERT_EQ_INT(pkr_field_fixed(0, &a), DEC_ERR_INVALID, "zero fixed width");
    ASSERT_EQ_INT(pkr_field_fixed(17, &a), DEC_ERR_INVALID, "oversized fixed width");
    ASSERT_EQ_INT(pkr_field_fixed(4, &a), DEC_OK, "four fixed bytes");
    ASSERT_EQ_INT(pkr_field_parse("numeric", &a), DEC_OK, "bare numeric metadata");
    ASSERT_EQ_INT(a.bare, 1, "bare numeric retained");
    ASSERT_EQ_INT(a.precision, -1, "bare precision marker");
    ASSERT_EQ_INT(a.payload_max, DEC_PAYLOAD_MAX, "bare numeric cap");
    ASSERT_EQ_INT(pkr_field_parse("numeric:1,1000", &a), DEC_OK, "S>P declaration");
    ASSERT_EQ_INT(a.payload_max, 17, "canonical S>P coefficient cap is P digits");
    ASSERT_EQ_INT(pkr_field_parse("numeric:2,-2", &a), DEC_OK, "negative scale");
    ASSERT_EQ_INT(a.payload_max, 17, "negative scale implicit zeros are not coefficient digits");
    ASSERT_EQ_INT(pkr_field_parse("currency", &a), DEC_OK, "currency metadata");
    ASSERT_EQ_INT(a.currency, 1, "currency identity retained");
    ASSERT_EQ_INT(pkr_field_parse("numeric:19,4", &b), DEC_OK, "same numeric contract");
    ASSERT_EQ_INT(b.currency, 0, "explicit numeric identity distinct");
    numeric_case("numeric:26,10", "1234567890123456.1234567890", DEC_OK,
                  "1234567890123456.1234567890");
    numeric_case("numeric:3,1", "1.25", DEC_OK, "1.3");
    numeric_case("numeric:3,1", "-1.25", DEC_OK, "-1.3");
    numeric_case("numeric:3,1", "99.95", DEC_ERR_PRECISION, NULL);
    numeric_case("numeric:3,5", "0.00999", DEC_OK, "0.00999");
    numeric_case("numeric:2,-2", "9949", DEC_OK, "9900");
    numeric_case("numeric:2,-2", "9950", DEC_ERR_PRECISION, NULL);
    numeric_case("currency", "12.34565", DEC_OK, "12.3457");
    numeric_case("numeric", "1.2300", DEC_OK, "1.2300");
    numeric_case("numeric", "1.0e2", DEC_OK, "100");
    numeric_case("numeric", "", DEC_ERR_SYNTAX, NULL);
    numeric_case("numeric", "NaN", DEC_ERR_SYNTAX, NULL);
}
static void mixed_and_capacity(void) {
    uint8_t integer[] = {0,0,0,7}, text[] = {'a','b'}, num[64]; size_t nn = 0;
    PkrField fnum;
    if (pkr_field_parse("numeric", &fnum) != DEC_OK) {
        ASSERT_TRUE(0, "mixed test prerequisite numeric parser"); return;
    }
    ASSERT_EQ_INT(pkr_numeric_encode(&fnum, "1.50", 4, num, sizeof(num), &nn,
                                     NULL, 0), DEC_OK, "mixed numeric payload");
    for (int pos = 0; pos < 3; pos++) {
        PkrField fields[3]; PkrCell cells[3];
        for (int i = 0; i < 3; i++) {
            if (i == pos) {
                pkr_field_fixed(4, &fields[i]);
                cells[i] = (PkrCell){integer,4,0};
            } else if (i == (pos+1)%3) {
                pkr_field_parse("varchar:8", &fields[i]);
                cells[i] = (PkrCell){text,2,0};
            } else { fields[i] = fnum; cells[i] = (PkrCell){num,nn,0}; }
        }
        PkrSchema s; dec_status st = pkr_schema_build(fields,3,&s);
        ASSERT_EQ_INT(st,DEC_OK,"mixed schema"); if (st != DEC_OK) continue;
        size_t need = 0; st = pkr_measure(&s,cells,3,&need);
        ASSERT_EQ_INT(st,DEC_OK,"measure mixed"); if (st != DEC_OK) continue;
        ASSERT_EQ_INT(need,2+4+8+8+2+nn,"actual includes only real payloads");
        uint8_t *out = malloc(need+16); ASSERT_NOT_NULL(out,"exact capacity plus canary");
        if (!out) continue;
        memset(out,0xA5,need+16); size_t used = 99;
        ASSERT_EQ_INT(pkr_compose(&s,cells,3,out,need-1,&used),DEC_ERR_TRUNCATED,
                      "one byte short");
        ASSERT_EQ_INT(used,0,"short failure length");
        int intact=1;for(size_t k=0;k<need+16;k++)if(out[k]!=0xA5)intact=0;
        ASSERT_TRUE(intact,"failed composition did not write");
        st=pkr_compose(&s,cells,3,out,need,&used);
        ASSERT_EQ_INT(st,DEC_OK,"actual capacity accepted below schema maximum");
        ASSERT_EQ_INT(used,need,"stored length exact");
        int canary=1;for(size_t k=need;k<need+16;k++)if(out[k]!=0xA5)canary=0;
        ASSERT_TRUE(canary,"no write after actual capacity");
        PkrView v;
        ASSERT_EQ_INT(pkr_view(&s,out,used,PKR_FLAGS,&v),DEC_OK,"composed view");
        for(size_t i=0;i<3;i++) {
            PkrSpan sp; st=pkr_span(&v,i,&sp);
            ASSERT_EQ_INT(st,DEC_OK,"mixed span");
            ASSERT_EQ_INT(sp.state,PKR_PRESENT,"mixed present");
            ASSERT_EQ_INT(sp.len,cells[i].len,"span length");
            if(st==DEC_OK&&sp.data&&sp.len==cells[i].len)
                ASSERT_EQ_INT(memcmp(sp.data,cells[i].data,sp.len),0,"exact bytes incl fixed int 7");
        }
        free(out);
    }
}
static void validation(void) {
    PkrField fields[2];pkr_field_parse("varchar:8",&fields[0]);
    pkr_field_parse("varchar:8",&fields[1]);PkrSchema s;
    if(pkr_schema_build(fields,2,&s)!=DEC_OK){ASSERT_TRUE(0,"view prerequisite");return;}
    const uint8_t a[]="abcd",b[]="wxyz";
    PkrCell cells[2]={{a,4,0},{b,4,0}};uint8_t good[64],bad[64];size_t used=0;
    dec_status st=pkr_compose(&s,cells,2,good,sizeof(good),&used);
    ASSERT_EQ_INT(st,DEC_OK,"validation seed");if(st!=DEC_OK)return;
    for(int test=0;test<12;test++) {
        memcpy(bad,good,used);uint8_t *d=bad+s.prefix;size_t n=used;
        switch(test) {
        case 0:d[3]=4;break;                /* inside directory */
        case 1:d[7]=5;break;                /* overlap */
        case 2:d[11]=21;break;              /* gap */
        case 3:d[3]=20;d[11]=16;break;      /* reverse order */
        case 4:d[0]=d[1]=d[2]=d[3]=255;break;
        case 5:d[7]=9;break;                /* field cap */
        case 6:d[3]=0;break;
        case 7:d[7]=0;break;                /* nonzero off, zero len */
        case 8:bad[1]|=1;break;             /* NULL with live payload */
        case 9:bad[0]=0;break;
        case 10:bad[1]|=0x80;break;         /* noncanonical bitmap padding */
        default:bad[n++]=0;break;           /* trailing bytes */
        }
        PkrView v;memset(&v,0xA5,sizeof(v));
        ASSERT_EQ_INT(pkr_view(&s,bad,n,PKR_FLAGS,&v),DEC_ERR_PAYLOAD,
                      "malformed record rejected");
        ASSERT_TRUE(!v.schema&&!v.value&&!v.len,"failed view cleared");
    }
    uint8_t flags[]={0,1,2,0xFF};
    for(size_t i=0;i<sizeof(flags);i++) {
        PkrView v;ASSERT_EQ_INT(pkr_view(&s,good,used,flags[i],&v),DEC_ERR_PAYLOAD,
                                "incompatible generation rejected");
    }
    PkrView v;ASSERT_EQ_INT(pkr_view(&s,good,used-1,PKR_FLAGS,&v),DEC_ERR_PAYLOAD,
                            "truncation rejected");
    size_t n=0;
    PkrCell alias[2]={{good+18,4,0},{b,4,0}};
    ASSERT_EQ_INT(pkr_compose(&s,alias,2,good,sizeof(good),&n),DEC_ERR_INVALID,
                  "overlapping output and payload rejected");
    cells[0]=(PkrCell){NULL,0,1};cells[1]=(PkrCell){NULL,0,0};
    ASSERT_EQ_INT(pkr_compose(&s,cells,2,good,sizeof(good),&used),DEC_OK,
                  "NULL and empty compose");
    ASSERT_EQ_INT(pkr_view(&s,good,used,PKR_FLAGS,&v),DEC_OK,"NULL/empty view");
    PkrSpan sp;
    ASSERT_EQ_INT(pkr_span(&v,0,&sp),DEC_OK,"NULL span");
    ASSERT_TRUE(sp.state==PKR_NULL&&!sp.data&&!sp.len,"NULL distinct");
    ASSERT_EQ_INT(pkr_span(&v,1,&sp),DEC_OK,"empty span");
    ASSERT_TRUE(sp.state==PKR_PRESENT&&sp.data&&sp.len==0,"empty present non-NULL pointer");
    ASSERT_EQ_INT(pkr_span(&v,2,&sp),DEC_ERR_INVALID,"bad index");
    ASSERT_EQ_INT(sp.state,PKR_INVALID,"failed span clear");
    s.fields[0].offset++;
    ASSERT_EQ_INT(pkr_measure(&s,cells,2,&used),DEC_ERR_INVALID,"tampered schema rejected");
}
static void numeric_cells(void) {
    PkrField f;PkrSchema s;uint8_t pay[64],record[128];size_t n=0,used=0;
    if(pkr_field_parse("numeric",&f)!=DEC_OK||pkr_schema_build(&f,1,&s)!=DEC_OK){
        ASSERT_TRUE(0,"numeric cell prerequisite");return;
    }
    ASSERT_EQ_INT(pkr_numeric_encode(&f,"0",1,pay,sizeof(pay),&n,NULL,0),DEC_OK,"zero encodes");
    ASSERT_EQ_INT(n,16,"numeric zero has header");
    PkrCell c={pay,n,0};
    ASSERT_EQ_INT(pkr_compose(&s,&c,1,record,sizeof(record),&used),DEC_OK,"zero compose");
    PkrView v;ASSERT_EQ_INT(pkr_view(&s,record,used,PKR_FLAGS,&v),DEC_OK,"zero view");
    PkrSpan sp;ASSERT_EQ_INT(pkr_span(&v,0,&sp),DEC_OK,"zero span");
    ASSERT_TRUE(sp.state==PKR_PRESENT&&sp.len==16,"zero not NULL or empty");
    c=(PkrCell){NULL,0,0};
    ASSERT_EQ_INT(pkr_measure(&s,&c,1,&used),DEC_ERR_PAYLOAD,"empty numeric payload rejected");
    pay[0]=99;c=(PkrCell){pay,n,0};
    ASSERT_EQ_INT(pkr_measure(&s,&c,1,&used),DEC_ERR_PAYLOAD,"invalid numeric rejected in sizing");
    ASSERT_EQ_INT(pkr_numeric_encode(&f,"1.25",4,pay,sizeof(pay),&n,NULL,0),DEC_OK,"bare1.25");
    ASSERT_EQ_INT(pkr_field_parse("numeric:3,1",&f),DEC_OK,"constrained cell schema");
    ASSERT_EQ_INT(pkr_schema_build(&f,1,&s),DEC_OK,"constrained schema");c=(PkrCell){pay,n,0};
    ASSERT_EQ_INT(pkr_measure(&s,&c,1,&used),DEC_ERR_PAYLOAD,"non-assigned constrained payload rejected");
}
static void largest_numeric(void) {
    PkrField f;PkrSchema schema;
    if(pkr_field_parse("numeric",&f)!=DEC_OK||pkr_schema_build(&f,1,&schema)!=DEC_OK){
        ASSERT_TRUE(0,"largest numeric prerequisite");return;
    }
    size_t chars=DEC_MAX_INT_DIGITS+1+DEC_MAX_FRAC_DIGITS;
    char *text=malloc(chars+1);uint8_t *payload=malloc(DEC_PAYLOAD_MAX);
    ASSERT_NOT_NULL(text,"147455-digit input");ASSERT_NOT_NULL(payload,"73744-byte payload");
    if(!text||!payload){free(text);free(payload);return;}
    memset(text,'9',chars);text[DEC_MAX_INT_DIGITS]='.';text[chars]='\0';size_t n=0;
    dec_status st=pkr_numeric_encode(&f,text,chars,payload,DEC_PAYLOAD_MAX,&n,NULL,0);
    ASSERT_EQ_INT(st,DEC_OK,"entire agreed numeric domain");
    ASSERT_EQ_INT(n,DEC_PAYLOAD_MAX,"max numeric payload exact");
    if(st==DEC_OK){PkrCell c={payload,n,0};size_t need=0;
        ASSERT_EQ_INT(pkr_measure(&schema,&c,1,&need),DEC_OK,"max numeric sizing");
        uint8_t *record=malloc(need);ASSERT_NOT_NULL(record,"max numeric actual row");
        if(record){size_t used=0;ASSERT_EQ_INT(pkr_compose(&schema,&c,1,record,need,&used),DEC_OK,
                                             "max numeric compose");
            PkrView v;ASSERT_EQ_INT(pkr_view(&schema,record,used,PKR_FLAGS,&v),DEC_OK,
                                    "max numeric validated view");
            free(record);}}
    free(payload);free(text);
}
static void maxima(void) {
    PkrField fields[PKR_MAX_FIELDS];PkrSchema s;PkrCell cells[PKR_MAX_FIELDS];
    PkrField f;if(pkr_field_parse("varchar",&f)!=DEC_OK){ASSERT_TRUE(0,"max prerequisite");return;}
    for(size_t i=0;i<PKR_MAX_FIELDS;i++){fields[i]=f;cells[i]=(PkrCell){NULL,0,1};}
    ASSERT_EQ_INT(pkr_schema_build(fields,PKR_MAX_FIELDS,&s),DEC_OK,"256 varchar schema");
    ASSERT_EQ_INT(s.maximum,1073743905,"4MiB maximum value bound");
    size_t slot=0;ASSERT_EQ_INT(pkr_slot_bound(&s,1024,&slot),DEC_OK,"maximum slot bound");
    ASSERT_EQ_INT(slot,1073744960,"maximum aligned slot");
    cells[127]=(PkrCell){(const uint8_t*)"tiny",4,0};size_t used=0;
    ASSERT_EQ_INT(pkr_measure(&s,cells,PKR_MAX_FIELDS,&used),DEC_OK,"maximum schema tiny measure");
    ASSERT_EQ_INT(used,2085,"small row no padding to maximum");
    uint8_t *out=malloc(used+1);ASSERT_NOT_NULL(out,"allocate actual2085 not1GiB");
    if(out){out[used]=0xA5;size_t n=0;
        ASSERT_EQ_INT(pkr_compose(&s,cells,PKR_MAX_FIELDS,out,used,&n),DEC_OK,"max schema exact output");
        ASSERT_EQ_INT(out[used],0xA5,"maxschema canary");
        PkrView v;ASSERT_EQ_INT(pkr_view(&s,out,n,PKR_FLAGS,&v),DEC_OK,"maxschema validates");free(out);}
    size_t bytes=0;
    ASSERT_EQ_INT(pkr_record_fits(&s,used,1,4096,&bytes),DEC_OK,"tiny record fits segment");
    ASSERT_EQ_INT(pkr_record_fits(&s,s.maximum,1024,128u*1024u*1024u,&bytes),DEC_ERR_RANGE,
                  "maximum theoretical row exceeds segment");
    ASSERT_EQ_INT(bytes,0,"segment rejection clears size");
    ASSERT_EQ_INT(pkr_record_fits(&s,used,1,32,&bytes),DEC_ERR_RANGE,"actual capacity gate");
    ASSERT_EQ_INT(pkr_slot_bound(&s,SIZE_MAX,&bytes),DEC_ERR_RANGE,"key overflow rejected");
    pkr_field_parse("numeric",&f);for(size_t i=0;i<PKR_MAX_FIELDS;i++)fields[i]=f;
    ASSERT_EQ_INT(pkr_schema_build(fields,PKR_MAX_FIELDS,&s),DEC_OK,"maxnumeric schema");
    ASSERT_EQ_INT(s.maximum,18880545,"maxnumeric bound");
    ASSERT_EQ_INT(pkr_slot_bound(&s,1024,&bytes),DEC_OK,"maxnumeric slot");
    ASSERT_EQ_INT(bytes,18881600,"maxnumeric aligned slot");
    ASSERT_EQ_INT(pkr_schema_build(fields,0,&s),DEC_ERR_INVALID,"zero fields rejected");
    ASSERT_EQ_INT(s.nfields,0,"failed schema cleared");
    ASSERT_EQ_INT(pkr_schema_build(fields,PKR_MAX_FIELDS+1,&s),DEC_ERR_INVALID,"too many fields");
}
static int test_packed_record_codec_run(void) {
    declarations();mixed_and_capacity();validation();numeric_cells();largest_numeric();maxima();
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-packed-record-codec",test_packed_record_codec_run)
```

### Task1d — src/test/cases/test_packed_record_storage.c

Complete file body; SHA256 fd65c281b06a60cdb8a2e7004897decaff2d14c2c937fc98808d955601bc3d73:

```c
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "seg_scan_varlen.h"
#include "packed_record.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <ftw.h>
#include <errno.h>

static int remove_entry(const char *path, const struct stat *st, int kind,
                         struct FTW *state) {
    (void)st; (void)kind; (void)state; return remove(path);
}
typedef struct { const PkrSchema *schema; int rows, bad; } Walk;
static int check_row(const uint8_t hash[16], const void *key, size_t klen,
                      const void *value, size_t vlen, uint8_t hdr2, void *ctx) {
    (void)hash;(void)key;(void)klen;
    Walk *w=ctx;PkrView v;
    if(hdr2!=PKR_FLAGS||pkr_view(w->schema,value,vlen,hdr2,&v)!=DEC_OK)w->bad++;
    w->rows++;return 0;
}
/* This independent regression exercises the old engine before any codec
   implementation exists: bit preservation must really be RED, not hidden
   behind an unimplemented schema prerequisite. */
static int invalid_generation_compute(const SlotcaskOldRecord *old,
                                       SlotcaskBulkRec *rec) {
    (void)old;
    rec->hdr2 = 2; /* A callback may rewrite NEW; packed-only is invalid. */
    return 0;
}
static void generation_transport(void) {
    uint8_t fake[40]={0};uint16_t keylen=1;uint32_t vlen=11;
    memcpy(fake+16,&keylen,2);fake[18]=1;memcpy(fake+20,&vlen,4);
    uint8_t flags[]={0,1,2,3,4};int expected[]={1,1,0,1,0};
    for(size_t i=0;i<sizeof(flags);i++){
        size_t recn=0;uint8_t flag=0,hdr2=0;uint16_t k=0;uint32_t n=0;fake[19]=flags[i];
        ASSERT_EQ_INT(seg_scan_varlen_struct_ok(fake,sizeof(fake),0,64,&recn,&flag,&k,&n,&hdr2),
                      expected[i],"known generation combinations only");
    }
    char root[]="/tmp/shard-packed-flag-XXXXXX";char *created=mkdtemp(root);
    ASSERT_NOT_NULL(created,"flag regression isolated object");if(!created)return;
    slotcask_init(64,64);SlotcaskDb db;
    int rc=slotcask_open(&db,root,1,1,64);ASSERT_EQ_INT(rc,0,"flag object opens");
    if(rc==0){uint8_t value[11]={1,0,0,0,0,8,0,0,0,1,'x'};
        SlotcaskUpsertOpts opts={0};opts.record_hdr2=PKR_FLAGS;
        ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"a",1,value,sizeof(value),&opts,NULL),0,
                      "flag regression writes raw future payload");
        void *got=NULL;size_t n=0;uint8_t hdr2=0;
        ASSERT_EQ_INT(slotcask_get_ex(&db,"a",1,&got,&n,&hdr2),0,"flag regression reads");
        ASSERT_EQ_INT(n,sizeof(value),"raw future vlen");
        ASSERT_EQ_INT(hdr2,PKR_FLAGS,"packed bit survives physical emitter");free(got);
        uint8_t bad_flags[]={2,0x80,0x83};
        for(size_t i=0;i<sizeof(bad_flags);i++){
            opts.record_hdr2=bad_flags[i];errno=0;
            ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"z",1,value,sizeof(value),&opts,NULL),-1,
                          "invalid generation insert rejected");
            ASSERT_EQ_INT(errno,EINVAL,"invalid insert flags reported");
            errno=0;
            ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db,-1,"a",1,value,sizeof(value),&opts,NULL),-1,
                          "invalid generation update rejected");
            ASSERT_EQ_INT(errno,EINVAL,"invalid update flags reported");
            SlotcaskBulkRec recs[2]={0};recs[0].key="b";recs[1].key="c";
            for(int j=0;j<2;j++){recs[j].klen=1;recs[j].value=value;
                recs[j].vlen=sizeof(value);recs[j].hdr2=j?bad_flags[i]:PKR_FLAGS;}
            SlotcaskBulkOpts bulk={0};errno=0;
            ASSERT_EQ_INT(slotcask_bulk_upsert_in_kfshard(&db,0,recs,2,&bulk),-1,
                          "direct batch invalid generation rejected before any row");
            ASSERT_EQ_INT(errno,EINVAL,"invalid direct batch flags reported");
            SlotcaskBulkShardInput input={0};input.kf_shard_id=0;
            input.recs=recs;input.nrecs=2;input.kind=SLOTCASK_BULK_INPUT_UPSERT;errno=0;
            ASSERT_EQ_INT(slotcask_bulk_request_execute(&db,&input,1),-1,
                          "pipeline batch invalid generation rejected before any row");
            ASSERT_EQ_INT(errno,EINVAL,"invalid pipeline flags reported");
        }
        for(int pipeline=0;pipeline<2;pipeline++){
            for(int existing=0;existing<2;existing++){
                SlotcaskBulkRec rec={0};rec.key=existing?"a":(pipeline?"g":"f");
                rec.klen=1;rec.value=value;rec.vlen=sizeof(value);rec.hdr2=PKR_FLAGS;
                SlotcaskBulkOpts bulk={0};bulk.value_compute=invalid_generation_compute;
                bulk.value_rewrites_payload=1;
                int result;
                if(pipeline){SlotcaskBulkShardInput input={0};input.kf_shard_id=0;
                    input.recs=&rec;input.nrecs=1;input.kind=SLOTCASK_BULK_INPUT_UPSERT;
                    input.opts.upsert=bulk;
                    result=slotcask_bulk_request_execute(&db,&input,1);
                }else result=slotcask_bulk_upsert_in_kfshard(&db,0,&rec,1,&bulk);
                ASSERT_EQ_INT(result,-1,"computed invalid generation fails window");
            }
        }
        ASSERT_EQ_INT(slotcask_exists(&db,"f",1),0,"computed direct insert absent");
        ASSERT_EQ_INT(slotcask_exists(&db,"g",1),0,"computed pipeline insert absent");
        ASSERT_EQ_INT(slotcask_exists(&db,"z",1),0,"rejected insert absent");
        ASSERT_EQ_INT(slotcask_exists(&db,"b",1),0,"valid batch prefix not written");
        ASSERT_EQ_INT(slotcask_exists(&db,"c",1),0,"invalid batch suffix not written");
        got=NULL;n=0;hdr2=0;
        ASSERT_EQ_INT(slotcask_get_ex(&db,"a",1,&got,&n,&hdr2),0,"original survives rejected update");
        ASSERT_EQ_INT(hdr2,PKR_FLAGS,"rejected updates preserve generation");free(got);
        slotcask_close(&db);}
    slotcask_shutdown();ASSERT_EQ_INT(nftw(root,remove_entry,16,FTW_DEPTH|FTW_PHYS),0,
                                     "flag fixture removed");
}
static int test_packed_record_storage_run(void) {
    generation_transport();
    PkrField field,fields[PKR_MAX_FIELDS];PkrSchema schema;
    ASSERT_EQ_INT(pkr_field_parse("varchar",&field),DEC_OK,"physical field");
    for(size_t i=0;i<PKR_MAX_FIELDS;i++)fields[i]=field;
    dec_status st=pkr_schema_build(fields,PKR_MAX_FIELDS,&schema);
    ASSERT_EQ_INT(st,DEC_OK,"physical maximum schema");if(st!=DEC_OK)return 1;
    PkrCell cells[PKR_MAX_FIELDS];
    for(size_t i=0;i<PKR_MAX_FIELDS;i++)cells[i]=(PkrCell){NULL,0,1};
    cells[127]=(PkrCell){(const uint8_t*)"tiny",4,0};size_t n=0;
    ASSERT_EQ_INT(pkr_measure(&schema,cells,PKR_MAX_FIELDS,&n),DEC_OK,"physical actual sizing");
    ASSERT_EQ_INT(n,2085,"actual tiny value in1GiB-schema");
    uint8_t *value=malloc(n);ASSERT_NOT_NULL(value,"allocate actual row");
    if(!value)return 1;
    size_t used=0;
    st=pkr_compose(&schema,cells,PKR_MAX_FIELDS,value,n,&used);
    ASSERT_EQ_INT(st,DEC_OK,"physical compose");if(st!=DEC_OK){free(value);return 1;}
    size_t bound=0,record_size=0;
    ASSERT_EQ_INT(pkr_slot_bound(&schema,1024,&bound),DEC_OK,"maximum slot size");
    ASSERT_EQ_INT(pkr_record_fits(&schema,n,1,SLOTCASK_SEG_MAX_BYTES,&record_size),DEC_OK,
                  "actual record fits unchanged segment ceiling");
    char root[]="/tmp/shard-packed-record-XXXXXX";
    char *created=mkdtemp(root);ASSERT_NOT_NULL(created,"isolated physical object");
    if(!created){free(value);return 1;}
    slotcask_init(64,64);SlotcaskDb db;
    int rc=slotcask_open(&db,root,1,1,(int)bound);
    ASSERT_EQ_INT(rc,0,"open huge-bound object without huge row allocation");
    if(rc){free(value);slotcask_shutdown();nftw(root,remove_entry,16,FTW_DEPTH|FTW_PHYS);return 1;}
    SlotcaskUpsertOpts opts={0};opts.record_hdr2=PKR_FLAGS;
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"a",1,value,n,&opts,NULL),0,"first packed row");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"b",1,value,n,&opts,NULL),0,"second packed row");
    void *got=NULL;size_t gotn=0;uint8_t hdr2=0;
    rc=slotcask_get_ex(&db,"a",1,&got,&gotn,&hdr2);
    ASSERT_EQ_INT(rc,0,"raw packed read");ASSERT_EQ_INT(gotn,n,"raw length actual");
    ASSERT_EQ_INT(hdr2,PKR_FLAGS,"generation bit preserved by emitter");
    if(rc==0&&got&&gotn==n)ASSERT_EQ_INT(memcmp(got,value,n),0,"physical bytes exact");
    free(got);
    char path[PATH_MAX];snprintf(path,sizeof(path),"%s/data/streams/000/000000.dat",root);
    int fd=open(path,O_RDONLY);ASSERT_TRUE(fd>=0,"raw segment opens");
    if(fd>=0){
        uint8_t header[24];ssize_t count=pread(fd,header,sizeof(header),0);
        ASSERT_EQ_INT(count,24,"first header");
        if(count==24){uint32_t disk_n=0;memcpy(&disk_n,header+20,4);
            ASSERT_EQ_INT(disk_n,n,"on-disk vlen is actual not maximum");
            ASSERT_EQ_INT(header[19],PKR_FLAGS,"on-disk hdr2");}
        count=pread(fd,header,sizeof(header),(off_t)record_size);
        ASSERT_EQ_INT(count,24,"second header at actual aligned offset");
        if(count==24){uint32_t disk_n=0;memcpy(&disk_n,header+20,4);
            ASSERT_EQ_INT(disk_n,n,"second actual vlen");
            ASSERT_EQ_INT(header[18],1,"second row live at compact offset");}
        close(fd);
    }
    Walk w={&schema,0,0};
    ASSERT_EQ_INT(slotcask_walk_live_strict(&db,check_row,&w),0,"strict walk accepts packed bit");
    ASSERT_EQ_INT(w.rows,2,"strict walk sees both packed rows");
    ASSERT_EQ_INT(w.bad,0,"strict walker bytes validate");

    uint8_t *large=malloc(PKR_VARCHAR_MAX);ASSERT_NOT_NULL(large,"4MiB content allocation");
    if(large){
        memset(large,'A',PKR_VARCHAR_MAX);cells[127]=(PkrCell){large,PKR_VARCHAR_MAX,0};
        size_t large_n=0;ASSERT_EQ_INT(pkr_measure(&schema,cells,PKR_MAX_FIELDS,&large_n),DEC_OK,
                                      "maximum field actual measure");
        uint8_t *record=malloc(large_n);ASSERT_NOT_NULL(record,"4MiB actual record");
        if(record){
            st=pkr_compose(&schema,cells,PKR_MAX_FIELDS,record,large_n,&used);
            ASSERT_EQ_INT(st,DEC_OK,"4MiB field compose");
            if(st==DEC_OK){
                ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"c",1,record,used,&opts,NULL),0,
                              "maximum field stored");
                got=NULL;gotn=0;hdr2=0;rc=slotcask_get_ex(&db,"c",1,&got,&gotn,&hdr2);
                ASSERT_EQ_INT(rc,0,"maximum field get");ASSERT_EQ_INT(gotn,large_n,"maximum actual bytes");
                if(rc==0&&got){PkrView view;PkrSpan span;
                    st=pkr_view(&schema,got,gotn,hdr2,&view);
                    ASSERT_EQ_INT(st,DEC_OK,"maximum physical view");
                    if(st==DEC_OK){ASSERT_EQ_INT(pkr_span(&view,127,&span),DEC_OK,"maximum physical span");
                        ASSERT_EQ_INT(span.len,PKR_VARCHAR_MAX,"full4MiB length");
                        if(span.data&&span.len==PKR_VARCHAR_MAX)
                            ASSERT_EQ_INT(memcmp(span.data,large,PKR_VARCHAR_MAX),0,"all4MiB bytes intact");}}
                free(got);
            }
            free(record);
        }
        free(large);
    }
    /* Reuse a large hole twice; the second small row uses its split leftover. */
    ASSERT_EQ_INT(slotcask_delete_with_hooks(&db,"c",1,NULL,NULL),0,"delete large donor");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"d",1,value,n,&opts,NULL),0,"reuse donor head");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"e",1,value,n,&opts,NULL),0,"reuse split leftover");
    fd=open(path,O_RDONLY);ASSERT_TRUE(fd>=0,"reused segment opens");
    if(fd>=0){
        for(int i=0;i<2;i++){
            uint8_t header[25];off_t at=(off_t)((2+(size_t)i)*record_size);
            ssize_t count=pread(fd,header,sizeof(header),at);
            ASSERT_EQ_INT(count,25,"small reuse at exact compact offset");
            if(count==25){uint32_t disk_n=0;memcpy(&disk_n,header+20,4);
                ASSERT_EQ_INT(header[18],1,"reused record live");
                ASSERT_EQ_INT(header[19],PKR_FLAGS,"reused generation preserved");
                ASSERT_EQ_INT(disk_n,n,"reused extent is actual length");
                ASSERT_EQ_INT(header[24],i?'e':'d',"donor head then leftover reused");}
        }
        close(fd);
    }
    slotcask_close(&db);
    rc=slotcask_open(&db,root,1,1,(int)bound);
    ASSERT_EQ_INT(rc,0,"reopen recovers packed physical records");
    if(rc==0){got=NULL;gotn=0;hdr2=0;
        rc=slotcask_get_ex(&db,"a",1,&got,&gotn,&hdr2);
        ASSERT_EQ_INT(rc,0,"read after reopen");ASSERT_EQ_INT(gotn,n,"reopen actual size");
        ASSERT_EQ_INT(hdr2,PKR_FLAGS,"reopen generation");free(got);slotcask_close(&db);}
    slotcask_shutdown();free(value);
    ASSERT_EQ_INT(nftw(root,remove_entry,16,FTW_DEPTH|FTW_PHYS),0,"physical fixture removed");
    return t_ctx->failed?1:0;
}
TEST_REGISTER("test-packed-record-storage",test_packed_record_storage_run)
```

### Task1e — exact build wiring

#### build.sh — exact replacement 1

Find this entire block exactly once:

```sh
#        shrinks the binary by eliminating dead code visible only across files).
# strip: remove symbol/debug tables from the shipped binary (~25K cut). Skipped
#        for sanitizer/debug builds — symbols are needed for readable stack traces.
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db

# libshard-db.a — embedded mode static library (all daemon sources except main.c)
LIB_SRCS="src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c \
          src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c \
          src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c \
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/type_desc.c \
          src/db/nql.c src/db/embedded.c"
LIB_OBJS=""
mkdir -p build/bin build/obj
```

Replace with:

```sh
#        shrinks the binary by eliminating dead code visible only across files).
# strip: remove symbol/debug tables from the shipped binary (~25K cut). Skipped
#        for sanitizer/debug builds — symbols are needed for readable stack traces.
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db

# libshard-db.a — embedded mode static library (all daemon sources except main.c)
LIB_SRCS="src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c \
          src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c \
          src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c \
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/type_desc.c \
          src/db/nql.c src/db/embedded.c"
LIB_OBJS=""
mkdir -p build/bin build/obj
```

#### build.sh — exact replacement 2

Find this entire block exactly once:

```sh
# TCP cases reach the deterministic seam; the production shard-db binary and
# its flags are never built from TEST_BUILD.
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test-server \
    src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db-test-server

# shard-db-test — TAP-style C test runner. Links daemon's JSON helpers
```

Replace with:

```sh
# TCP cases reach the deterministic seam; the production shard-db binary and
# its flags are never built from TEST_BUILD.
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test-server \
    src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db-test-server

# shard-db-test — TAP-style C test runner. Links daemon's JSON helpers
```

#### build.sh — exact replacement 3

Find this entire block exactly once:

```sh
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test \
    src/test/shard-db-test.c \
    src/db/decnum.c \
    src/test/test_client.c \
    src/test/test_runner.c \
    src/test/fixtures.c \
```

Replace with:

```sh
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test \
    src/test/shard-db-test.c \
    src/db/decnum.c \
    src/db/packed_record.c \
    src/test/test_client.c \
    src/test/test_runner.c \
    src/test/fixtures.c \
```

#### build.sh — exact replacement 4

Find this entire block exactly once:

```sh
    src/test/cases/test_scalar_routes_checked.c \
    src/test/cases/test_scalar_csv_update.c \
    src/test/cases/test_decnum_core.c \
    src/test/cases/test_version_gate.c \
    src/test/cases/test_datetime_evening.c \
    src/test/cases/test_error_paths.c \
```

Replace with:

```sh
    src/test/cases/test_scalar_routes_checked.c \
    src/test/cases/test_scalar_csv_update.c \
    src/test/cases/test_decnum_core.c \
    src/test/cases/test_packed_record_codec.c \
    src/test/cases/test_packed_record_storage.c \
    src/test/cases/test_version_gate.c \
    src/test/cases/test_datetime_evening.c \
    src/test/cases/test_error_paths.c \
```

### Task1f — capture RED

Build succeeds without compiler warnings. Run both new cases, capture nonzero
exit codes and complete output before applying any Task2 code.

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-record-codec
rtk proxy ./build/bin/shard-db-test run test-packed-record-storage
```

Measured codec RED: 16 passed / 61 failed (exit 61). Physical RED before the final
callback assertions was 11/32 (exit 32); the callback assertions separately gave
96/7 against an otherwise-green candidate. Final stub-stage physical count is
forecast 11/38, not a measured or binding count. Capture actual output: both new
cases must fail for the named missing-feature mechanisms; unexpected classes
still halt. Stubs return status 10. Transport masks 3 to 1, the scanner rejects 3,
and invalid flags are accepted
and mutate the fixture. Do not weaken assertions.

Earlier full RED, before adding invalid-write assertions:18471/65 across518;
all516 pre-existing cases green. That applies to the earlier test revision,
not a full RED claim for the final test file. Executor may run final full RED:
all old cases must remain green and only new cases may fail. Its final aggregate
count has not been measured and must not be presented as measured evidence.

## Task 2 — bounded codec and checked physical transport

**How this serves the primary goal:** supplies a safe storage foundation while
applications continue through existing supported paths.

RED is Task1f. Replace the entire stub file identified by this unique first-line
anchor with the complete module below; never append under duplicate stubs.

```c
/* Phase 3A RED stub: replace this entire file in Task 2. */
```

### Task2a — src/db/packed_record.c, entire replacement

Complete file body; SHA256 8f883cf3944b2eedb9b51a4e74b0b4c7643a3f316083cc521b5db1825696ab0d:

```c
#include "packed_record.h"
#include <limits.h>
#include <string.h>

static uint32_t get32(const uint8_t *p) {
    return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 |
           (uint32_t)p[2] << 8 | p[3];
}
static void put32(uint8_t *p, uint32_t n) {
    p[0] = (uint8_t)(n >> 24); p[1] = (uint8_t)(n >> 16);
    p[2] = (uint8_t)(n >> 8); p[3] = (uint8_t)n;
}
static int uint_token(const char **s, uint32_t limit, uint32_t *out) {
    const char *p = *s;
    if (*p < '0' || *p > '9') return 0;
    uint32_t n = 0;
    do {
        uint32_t d = (uint32_t)(*p - '0');
        if (n > limit / 10 || (n == limit / 10 && d > limit % 10)) return 0;
        n = n * 10 + d; p++;
    } while (*p >= '0' && *p <= '9');
    *s = p; *out = n; return 1;
}
static uint32_t numeric_cap(int32_t precision) {
    /* Canonical coefficient strips zero padding even for negative S or S>P;
       a constrained value needs at most P significant coefficient digits. */
    return precision < 0 ? DEC_PAYLOAD_MAX :
           DEC_PAYLOAD_HEADER + ((uint32_t)precision + 1) / 2;
}
static int field_ok(const PkrField *f) {
    if (!f || f->bare > 1 || f->currency > 1) return 0;
    if (f->kind == PKR_FIXED)
        return f->width >= 1 && f->width <= 16 && !f->payload_max &&
               !f->precision && !f->scale && !f->bare && !f->currency;
    if (f->width != PKR_DESCRIPTOR_BYTES) return 0;
    if (f->kind == PKR_VARCHAR)
        return f->payload_max >= 1 && f->payload_max <= PKR_VARCHAR_MAX &&
               (!f->bare || f->payload_max == PKR_VARCHAR_MAX) &&
               !f->precision && !f->scale && !f->currency;
    if (f->kind != PKR_NUMERIC) return 0;
    if (f->bare)
        return f->precision == -1 && !f->scale && !f->currency &&
               f->payload_max == DEC_PAYLOAD_MAX;
    return f->precision >= 1 && f->precision <= DEC_MAX_PRECISION &&
           f->scale >= DEC_MIN_SCALE && f->scale <= DEC_MAX_SCALE &&
           (!f->currency || (f->precision == 19 && f->scale == 4)) &&
           f->payload_max == numeric_cap(f->precision);
}

dec_status pkr_field_fixed(uint32_t width, PkrField *out) {
    if (!out) return DEC_ERR_INVALID;
    memset(out, 0, sizeof(*out));
    if (!width || width > 16) return DEC_ERR_INVALID;
    out->kind = PKR_FIXED; out->width = width; return DEC_OK;
}
dec_status pkr_field_parse(const char *type, PkrField *out) {
    if (!out) return DEC_ERR_INVALID;
    memset(out, 0, sizeof(*out));
    if (!type) return DEC_ERR_INVALID;
    PkrField f = {0}; f.width = PKR_DESCRIPTOR_BYTES;
    if (!strcmp(type, "varchar")) {
        f.kind = PKR_VARCHAR; f.bare = 1; f.payload_max = PKR_VARCHAR_MAX;
    } else if (!strncmp(type, "varchar:", 8)) {
        const char *p = type + 8; uint32_t n;
        if (!uint_token(&p, PKR_VARCHAR_MAX, &n) || *p || !n)
            return DEC_ERR_INVALID;
        f.kind = PKR_VARCHAR; f.payload_max = n;
    } else if (!strcmp(type, "numeric")) {
        f.kind = PKR_NUMERIC; f.bare = 1; f.precision = -1;
        f.payload_max = DEC_PAYLOAD_MAX;
    } else if (!strcmp(type, "currency")) {
        f.kind = PKR_NUMERIC; f.precision = 19; f.scale = 4; f.currency = 1;
        f.payload_max = numeric_cap(f.precision);
    } else if (!strncmp(type, "numeric:", 8)) {
        const char *p = type + 8; uint32_t precision, scale = 0; int neg = 0;
        if (!uint_token(&p, DEC_MAX_PRECISION, &precision) || !precision)
            return DEC_ERR_INVALID;
        if (*p == ',') {
            p++;
            if (*p == '-' || *p == '+') { neg = *p == '-'; p++; }
            if (!uint_token(&p, DEC_MAX_SCALE, &scale)) return DEC_ERR_INVALID;
        }
        if (*p) return DEC_ERR_INVALID;
        f.kind = PKR_NUMERIC; f.precision = (int32_t)precision;
        f.scale = neg ? -(int32_t)scale : (int32_t)scale;
        f.payload_max = numeric_cap(f.precision);
    } else return DEC_ERR_INVALID;
    *out = f; return DEC_OK;
}

dec_status pkr_schema_build(const PkrField *fields, size_t n, PkrSchema *out) {
    if (!out) return DEC_ERR_INVALID;
    /* Local assembly permits rebuilding from out->fields without mutation
       while validation is in progress. No field owns dynamic memory. */
    PkrSchema next = {0};
    if (!fields || !n || n > PKR_MAX_FIELDS) {
        memset(out, 0, sizeof(*out)); return DEC_ERR_INVALID;
    }
    uint64_t directory = 0, payloads = 0;
    for (size_t i = 0; i < n; i++) {
        if (!field_ok(&fields[i])) {
            memset(out, 0, sizeof(*out)); return DEC_ERR_INVALID;
        }
        next.fields[i] = fields[i];
        next.fields[i].offset = (uint32_t)directory;
        directory += fields[i].width; payloads += fields[i].payload_max;
    }
    uint64_t prefix = 1 + (n + 7) / 8, maximum = prefix + directory + payloads;
    if (maximum > INT32_MAX) {
        memset(out, 0, sizeof(*out)); return DEC_ERR_RANGE;
    }
    next.nfields = (uint32_t)n; next.prefix = (uint32_t)prefix;
    next.directory = (uint32_t)directory; next.maximum = (uint32_t)maximum;
    *out = next; return DEC_OK;
}
static int schema_ok(const PkrSchema *s) {
    if (!s || !s->nfields || s->nfields > PKR_MAX_FIELDS ||
        s->prefix != 1 + (s->nfields + 7) / 8) return 0;
    uint64_t dir = 0, payload = 0;
    for (size_t i = 0; i < s->nfields; i++) {
        const PkrField *f = &s->fields[i];
        if (!field_ok(f) || f->offset != dir) return 0;
        dir += f->width; payload += f->payload_max;
    }
    return dir == s->directory && s->prefix + dir + payload <= INT32_MAX &&
           s->maximum == s->prefix + dir + payload;
}

dec_status pkr_numeric_encode(const PkrField *f, const char *text, size_t n,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz) {
    if (err && errsz) err[0] = '\0';
    if (!used) return DEC_ERR_INVALID;
    *used = 0;
    if (!field_ok(f) || f->kind != PKR_NUMERIC || !text || !out)
        return DEC_ERR_INVALID;
    dec_t parsed, rounded; dec_init(&parsed); dec_init(&rounded);
    dec_status st = dec_parse(text, n, &parsed, err, errsz);
    const dec_t *value = &parsed;
    if (st == DEC_OK && !f->bare) {
        st = dec_apply_precision(&parsed, f->precision, f->scale,
                                 &rounded, err, errsz);
        value = &rounded;
    }
    if (st == DEC_OK && dec_payload_len(value) > f->payload_max) st = DEC_ERR_RANGE;
    if (st == DEC_OK) st = dec_pack(value, out, cap, used);
    dec_free(&rounded); dec_free(&parsed); return st;
}
static dec_status numeric_payload(const PkrField *f, const uint8_t *p, size_t n) {
    dec_t d, r; dec_init(&d); dec_init(&r);
    dec_status st = dec_unpack(p, n, &d);
    if (st == DEC_OK && !f->bare) {
        st = dec_apply_precision(&d, f->precision, f->scale, &r, NULL, 0);
        if (st == DEC_OK && (dec_cmp(&d, &r) || d.dscale != r.dscale))
            st = DEC_ERR_PAYLOAD;
    }
    dec_free(&r); dec_free(&d);
    if (st == DEC_OK || st == DEC_ERR_NOMEM) return st;
    return DEC_ERR_PAYLOAD;
}
static dec_status cell_ok(const PkrField *f, const PkrCell *c) {
    if (c->is_null > 1) return DEC_ERR_INVALID;
    if (c->is_null) return (!c->data && !c->len) ? DEC_OK : DEC_ERR_INVALID;
    if (f->kind == PKR_FIXED)
        return (c->data && c->len == f->width) ? DEC_OK : DEC_ERR_PAYLOAD;
    if (c->len > f->payload_max) return DEC_ERR_RANGE;
    if (c->len && !c->data) return DEC_ERR_INVALID;
    if (f->kind == PKR_NUMERIC) return numeric_payload(f, c->data, c->len);
    return DEC_OK;
}
dec_status pkr_measure(const PkrSchema *s, const PkrCell *cells, size_t n,
                       size_t *used) {
    if (!used) return DEC_ERR_INVALID;
    *used = 0;
    if (!schema_ok(s) || !cells || n != s->nfields) return DEC_ERR_INVALID;
    size_t total = s->prefix + s->directory;
    for (size_t i = 0; i < n; i++) {
        dec_status st = cell_ok(&s->fields[i], &cells[i]);
        if (st != DEC_OK) return st;
        if (!cells[i].is_null && s->fields[i].kind != PKR_FIXED)
            total += cells[i].len;
    }
    if (total > s->maximum) return DEC_ERR_RANGE;
    *used = total; return DEC_OK;
}
static int overlap(const void *a, size_t an, const void *b, size_t bn) {
    if (!an || !bn) return 0;
    uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
    if (an > UINTPTR_MAX - x || bn > UINTPTR_MAX - y) return 1;
    return x < y + bn && y < x + an;
}
dec_status pkr_compose(const PkrSchema *s, const PkrCell *cells, size_t n,
                       uint8_t *out, size_t cap, size_t *used) {
    if (!used) return DEC_ERR_INVALID;
    *used = 0;
    if (!out) return DEC_ERR_INVALID;
    size_t need = 0; dec_status st = pkr_measure(s, cells, n, &need);
    if (st != DEC_OK) return st;
    if (cap < need) return DEC_ERR_TRUNCATED;
    for (size_t i = 0; i < n; i++)
        if (overlap(out, need, cells[i].data, cells[i].len)) return DEC_ERR_INVALID;
    /* No operations after this point allocate or can fail. */
    memset(out, 0, s->prefix + s->directory);
    out[0] = (uint8_t)(s->prefix - 1);
    uint8_t *dir = out + s->prefix;
    size_t cursor = s->directory;
    for (size_t i = 0; i < n; i++) {
        const PkrField *f = &s->fields[i]; const PkrCell *c = &cells[i];
        if (c->is_null) { out[1 + i / 8] |= (uint8_t)(1u << (i % 8)); continue; }
        if (f->kind == PKR_FIXED) memcpy(dir + f->offset, c->data, f->width);
        else if (c->len) {
            put32(dir + f->offset, (uint32_t)cursor);
            put32(dir + f->offset + 4, (uint32_t)c->len);
            memcpy(dir + cursor, c->data, c->len); cursor += c->len;
        }
    }
    *used = need; return DEC_OK;
}

dec_status pkr_view(const PkrSchema *s, const uint8_t *v, size_t n,
                    uint8_t flags, PkrView *out) {
    if (!out) return DEC_ERR_INVALID;
    memset(out, 0, sizeof(*out));
    if (!schema_ok(s) || !v) return DEC_ERR_INVALID;
    if (flags != PKR_FLAGS || n < (size_t)s->prefix + s->directory ||
        n > s->maximum || v[0] != s->prefix - 1) return DEC_ERR_PAYLOAD;
    unsigned rem = s->nfields % 8;
    if (rem && (v[s->prefix - 1] & (uint8_t)(0xFFu << rem))) return DEC_ERR_PAYLOAD;
    const uint8_t *dir = v + s->prefix;
    size_t cursor = s->directory, region = n - s->prefix;
    for (size_t i = 0; i < s->nfields; i++) {
        const PkrField *f = &s->fields[i];
        int is_null = !!(v[1 + i / 8] & (1u << (i % 8)));
        const uint8_t *entry = dir + f->offset;
        if (f->kind == PKR_FIXED) {
            if (is_null) for (size_t j = 0; j < f->width; j++)
                if (entry[j]) return DEC_ERR_PAYLOAD;
            continue;
        }
        uint32_t off = get32(entry), len = get32(entry + 4);
        if (!len) {
            if (off || (!is_null && f->kind == PKR_NUMERIC)) return DEC_ERR_PAYLOAD;
            continue;
        }
        if (is_null || len > f->payload_max || off != cursor ||
            cursor > region || len > region - cursor) return DEC_ERR_PAYLOAD;
        if (f->kind == PKR_NUMERIC) {
            dec_status st = numeric_payload(f, dir + off, len);
            if (st != DEC_OK) return st;
        }
        cursor += len;
    }
    if (cursor != region) return DEC_ERR_PAYLOAD;
    out->schema = s; out->value = v; out->len = n; return DEC_OK;
}
dec_status pkr_span(const PkrView *v, size_t index, PkrSpan *out) {
    if (!out) return DEC_ERR_INVALID;
    memset(out, 0, sizeof(*out));
    if (!v || !schema_ok(v->schema) || !v->value || index >= v->schema->nfields)
        return DEC_ERR_INVALID;
    const PkrSchema *s = v->schema; const PkrField *f = &s->fields[index];
    if (v->value[1 + index / 8] & (1u << (index % 8))) {
        out->state = PKR_NULL; return DEC_OK;
    }
    const uint8_t *dir = v->value + s->prefix;
    const uint8_t *entry = dir + f->offset;
    out->state = PKR_PRESENT;
    if (f->kind == PKR_FIXED) { out->data = entry; out->len = f->width; }
    else {
        out->len = get32(entry + 4);
        out->data = dir + (out->len ? get32(entry) : s->directory);
    }
    return DEC_OK;
}
static dec_status record_bytes(size_t value, size_t key, size_t *out) {
    if (!out) return DEC_ERR_INVALID;
    *out = 0;
    if (!key || key > 1024 || value > SIZE_MAX - 24 - key - 7) return DEC_ERR_RANGE;
    size_t bytes = (24 + key + value + 7) & ~(size_t)7;
    *out = bytes < 32 ? 32 : bytes; return DEC_OK;
}
dec_status pkr_slot_bound(const PkrSchema *s, size_t key, size_t *out) {
    if (!out) return DEC_ERR_INVALID;
    *out = 0;
    if (!schema_ok(s)) return DEC_ERR_INVALID;
    return record_bytes(s->maximum, key, out);
}
dec_status pkr_record_fits(const PkrSchema *s, size_t actual, size_t key,
                           size_t segment_max, size_t *out) {
    if (!out) return DEC_ERR_INVALID;
    *out = 0;
    if (!schema_ok(s)) return DEC_ERR_INVALID;
    if (actual < (size_t)s->prefix + s->directory || actual > s->maximum)
        return DEC_ERR_RANGE;
    size_t bytes = 0; dec_status st = record_bytes(actual, key, &bytes);
    if (st != DEC_OK) return st;
    if (bytes > segment_max) return DEC_ERR_RANGE;
    *out = bytes; return DEC_OK;
}
```

### Task2b — exact physical transport changes

Apply as one dependency group, then build. Keep legacy typed/schema readers
unchanged. No public packed object is created.

#### src/db/types.h — exact replacement 1

Find this entire block exactly once:

```c

/* ===== Null bitmap record support (2026.10.2) ===== */

/* Segment record header byte 19 (reserved): bit 0 = value region carries
   a field-null bitmap ([u8 bm_nbytes][bits...]) ahead of the field bytes.
   Bits 1-7 must be zero on disk; unknown bits fail closed. */
#define SREC_FLAG_NULLMAP 0x01

static inline int typed_nullmap_bytes(int nfields) {
    return (nfields + 7) / 8;
```

Replace with:

```c

/* ===== Null bitmap record support (2026.10.2) ===== */

/* Segment record generation flags. The generic transport recognizes
   nullmap (bit 0) and packed-with-nullmap (bits 0|1). Other bits and
   packed-without-nullmap are invalid. Recognizing transport flags does
   not activate packed object loading: legacy typed_record_view and the
   schema version gate remain unchanged until their approved slices. */
#define SREC_FLAG_NULLMAP 0x01
#define SREC_FLAG_PACKED  0x02
#define SREC_KNOWN_FLAGS  (SREC_FLAG_NULLMAP | SREC_FLAG_PACKED)

/* Physical transport accepts legacy, nullmap, or packed+nullmap only. */
static inline int srec_flags_valid(uint8_t flags) {
    return flags == 0 || flags == SREC_FLAG_NULLMAP ||
           flags == SREC_KNOWN_FLAGS;
}

static inline int typed_nullmap_bytes(int nfields) {
    return (nfields + 7) / 8;
```

#### src/db/seg_scan_varlen.h — exact replacement 1

Find this entire block exactly once:

```c

/* Validate the record header at map[pos]: flag <= 2, header + rec_size
   fits within map_size, and rec_size itself <= max_slot_size. Byte 19
   (generation) must carry only SREC_FLAG_NULLMAP — unknown bits fail
   closed. Does NOT verify the hash. max_slot_size is the owning object's
   cached SlotcaskDb.slot_size; it is deliberately not the segment file
   cap. out_hdr2 may be NULL (caller doesn't need the flag). Returns 1
```

Replace with:

```c

/* Validate the record header at map[pos]: flag <= 2, header + rec_size
   fits within map_size, and rec_size itself <= max_slot_size. Byte 19
   (generation) must be 0, NULLMAP, or PACKED|NULLMAP — other values fail
   closed. Does NOT verify the hash. max_slot_size is the owning object's
   cached SlotcaskDb.slot_size; it is deliberately not the segment file
   cap. out_hdr2 may be NULL (caller doesn't need the flag). Returns 1
```

#### src/db/seg_scan_varlen.h — exact replacement 2

Find this entire block exactly once:

```c
    uint8_t flag = rec[18];
    if (flag > 2) return 0;
    uint8_t hdr2 = rec[19];
    if (hdr2 & ~SREC_FLAG_NULLMAP) return 0;
    uint16_t klen;
    uint32_t vlen;
    memcpy(&klen, rec + 16, 2);
```

Replace with:

```c
    uint8_t flag = rec[18];
    if (flag > 2) return 0;
    uint8_t hdr2 = rec[19];
    if (!srec_flags_valid(hdr2)) return 0;
    uint16_t klen;
    uint32_t vlen;
    memcpy(&klen, rec + 16, 2);
```

#### src/db/slotcask.c — exact replacement 1

Find this entire block exactly once:

```c
       relaxed ordering for the initial 0 — readers that see 0 simply skip
       the record (no acquire needed). */
    __atomic_store_n(&dst[18], 0, __ATOMIC_RELEASE);
    /* Byte 19 carries SREC_FLAG_NULLMAP (null-bitmap generation). Written
       before the flag release-store, so a live record always has its
       generation byte visible. */
    dst[19] = (uint8_t)(hdr2 & SREC_FLAG_NULLMAP);
    uint32_t v32 = (uint32_t)vlen;
    memcpy(dst + 20, &v32, 4);
    memcpy(dst + 24, key, klen);
```

Replace with:

```c
       relaxed ordering for the initial 0 — readers that see 0 simply skip
       the record (no acquire needed). */
    __atomic_store_n(&dst[18], 0, __ATOMIC_RELEASE);
    /* Publish the known generation flags with the record payload. The
       object/schema dispatch remains separately gated. */
    dst[19] = hdr2;
    uint32_t v32 = (uint32_t)vlen;
    memcpy(dst + 20, &v32, 4);
    memcpy(dst + 24, key, klen);
```

#### src/db/slotcask.c — exact replacement 2

Find this entire block exactly once:

```c
    __atomic_store_n(&dst[18], 0, __ATOMIC_RELEASE);
    /* Generation byte rides the staged payload; the activation flag-flip
       publishes both together. */
    dst[19] = (uint8_t)(hdr2 & SREC_FLAG_NULLMAP);
    uint32_t v32 = (uint32_t)vlen;
    memcpy(dst + 20, &v32, 4);
    memcpy(dst + 24, key, klen);
```

Replace with:

```c
    __atomic_store_n(&dst[18], 0, __ATOMIC_RELEASE);
    /* Generation byte rides the staged payload; the activation flag-flip
       publishes both together. */
    dst[19] = hdr2;
    uint32_t v32 = (uint32_t)vlen;
    memcpy(dst + 20, &v32, 4);
    memcpy(dst + 24, key, klen);
```

#### src/db/slotcask.c — exact replacement 3

Find this entire block exactly once:

```c
    }

    if (shard->kind == BULK_MUTATION_UPSERT) {
        /* Fallback staging is batched after the planning loop: these
           records could not ride the P wave (their NEW bytes depend on
           OLD), but they stage exactly like it — grouped by stream,
```

Replace with:

```c
    }

    if (shard->kind == BULK_MUTATION_UPSERT) {
        /* value_compute can replace NEW metadata after entry validation.
           Reject its invalid generation before fallback payload emission. */
        for (size_t i = begin; i < end; i++) {
            if (recs[i].status == 0 && !srec_flags_valid(recs[i].hdr2)) {
                recs[i].status = -1;
                st[i].needs_write = 0;
                errno = EINVAL;
                goto hard_fail;
            }
        }
        /* Fallback staging is batched after the planning loop: these
           records could not ride the P wave (their NEW bytes depend on
           OLD), but they stage exactly like it — grouped by stream,
```

#### src/db/slotcask.c — exact replacement 4

Find this entire block exactly once:

```c
            errno = EINVAL;
            return -1;
        }
        inputs[i].rc = 0;
        inputs[i].error_no = 0;
        int *degraded = inputs[i].kind == SLOTCASK_BULK_INPUT_UPSERT
```

Replace with:

```c
            errno = EINVAL;
            return -1;
        }
        if (inputs[i].kind == SLOTCASK_BULK_INPUT_UPSERT) {
            for (size_t j = 0; j < inputs[i].nrecs; j++) {
                if (!srec_flags_valid(inputs[i].recs[j].hdr2)) {
                    free(touched);
                    errno = EINVAL;
                    return -1;
                }
            }
        }
        inputs[i].rc = 0;
        inputs[i].error_no = 0;
        int *degraded = inputs[i].kind == SLOTCASK_BULK_INPUT_UPSERT
```

#### src/db/slotcask.c — exact replacement 5

Find this entire block exactly once:

```c
    if (!txn || !txn->db || !txn->shards || txn->nshards == 0) {
        errno = EINVAL;
        return -1;
    }
    ids = malloc(txn->nshards * sizeof(*ids));
    if (!ids) return -1;
```

Replace with:

```c
    if (!txn || !txn->db || !txn->shards || txn->nshards == 0) {
        errno = EINVAL;
        return -1;
    }
    /* Validate every generation before any shard can stage payloads. */
    for (size_t i = 0; i < txn->nshards; i++) {
        const BulkMutationShard *shard = &txn->shards[i];
        if (shard->kind != BULK_MUTATION_UPSERT) continue;
        if (!shard->recs) { errno = EINVAL; return -1; }
        for (size_t j = 0; j < shard->nrecs; j++) {
            if (!srec_flags_valid(shard->recs[j].hdr2)) {
                errno = EINVAL;
                return -1;
            }
        }
    }
    ids = malloc(txn->nshards * sizeof(*ids));
    if (!ids) return -1;
```

#### src/db/slotcask.h — exact replacement 1

Find this entire block exactly once:

```c
                 void **val_out, size_t *vlen_out);

/* Same read, also surfacing the record's generation byte (segment header
   byte 19: SREC_FLAG_NULLMAP or 0). out_hdr2 may be NULL. */
int slotcask_get_ex(SlotcaskDb *db,
                    const void *key, size_t klen,
                    void **val_out, size_t *vlen_out,
```

Replace with:

```c
                 void **val_out, size_t *vlen_out);

/* Same read, also surfacing the record's generation byte (segment header
   byte 19: 0, NULLMAP, or PACKED|NULLMAP). out_hdr2 may be NULL. */
int slotcask_get_ex(SlotcaskDb *db,
                    const void *key, size_t klen,
                    void **val_out, size_t *vlen_out,
```

#### src/db/slotcask.h — exact replacement 2

Find this entire block exactly once:

```c
       converged synchronously. The caller must return the distinct pending
       outcome; it must not report a successful mutation. */
    int                      *out_durability_degraded;
    /* Segment header byte 19 for the NEW record (SREC_FLAG_NULLMAP when
       the value was produced by the nullmap encoders). Zero-init = legacy
       generation. The bulk primitive copies it to the record header. */
    uint8_t                   record_hdr2;
} SlotcaskUpsertOpts;
```

Replace with:

```c
       converged synchronously. The caller must return the distinct pending
       outcome; it must not report a successful mutation. */
    int                      *out_durability_degraded;
    /* Segment header byte 19 for the NEW record: 0 for legacy, NULLMAP
       for current typed encoders, NULLMAP|PACKED only for validated packed
       payloads. The codec is inactive in public object creation; recognizing
       transport flags does not publish or activate a packed schema. */
    uint8_t                   record_hdr2;
} SlotcaskUpsertOpts;
```

#### src/db/slotcask.h — exact replacement 3

Find this entire block exactly once:

```c
       Lifetime: caller owns the buffer; must outlive the bulk call. */
    const void *old_value;
    size_t      old_vlen;
    /* Segment header byte 19 for the NEW record (SREC_FLAG_NULLMAP or 0)
       and for a caller-provided old_value. Zero-init = legacy. */
    uint8_t     hdr2;
    uint8_t     old_hdr2;
```

Replace with:

```c
       Lifetime: caller owns the buffer; must outlive the bulk call. */
    const void *old_value;
    size_t      old_vlen;
    /* Segment header byte 19 for the NEW record (0, NULLMAP, or PACKED|NULLMAP)
       and for a caller-provided old_value. Zero-init = legacy. */
    uint8_t     hdr2;
    uint8_t     old_hdr2;
```

#### src/db/slotcask.h — exact replacement 4

Find this entire block exactly once:

```c
 */

/* cb returns 0 to continue, 1 to stop the walk early. hdr2 = segment
   record header byte 19 (SREC_FLAG_NULLMAP generation flag, 0 = legacy). */
typedef int (*SlotcaskScanCb)(const uint8_t hash16[16],
                               const void *key, size_t klen,
                               const void *value, size_t vlen,
```

Replace with:

```c
 */

/* cb returns 0 to continue, 1 to stop the walk early. hdr2 = segment
   record header byte 19 (generation flags: 0, NULLMAP, or PACKED|NULLMAP). */
typedef int (*SlotcaskScanCb)(const uint8_t hash16[16],
                               const void *key, size_t klen,
                               const void *value, size_t vlen,
```

### Task2c — GREEN and full regression

Capture real output; investigate any failure without silent retries.

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-record-codec
rtk proxy ./build/bin/shard-db-test run test-packed-record-storage
rtk proxy ./build/bin/shard-db-test run-all
```

Focused GREEN:codec 268/0,storage 103/0. The physical test asserts exact segment
vlen/offsets, all 4 MiB bytes, reopen/strict walk, maximum-schema actual buffers,
donor-head reuse and split-leftover reuse. Reopen is not pending-KFM2 replay.
Full suite must be green with all pre-existing assertions unchanged.

## Task 3 — independent checks, documentation and review

**How this serves the primary goal:** independently checks decimal assignment
and failure behavior, and defines the inactive interface for later planners.

Task1 captured RED. These validation probes never choose expected results from
the C implementation. Create these files under /tmp/p3a-check (or a fresh
equivalent) only, never in version control.

Create the scratch directory before writing either file:

```sh
rtk proxy mkdir -p /tmp/p3a-check
```

### Scratch numeric_oracle.py

```python
import ctypes,random,sys
from decimal import Decimal,localcontext,ROUND_HALF_UP
class Field(ctypes.Structure):
    _fields_=[('kind',ctypes.c_int),('width',ctypes.c_uint32),('payload_max',ctypes.c_uint32),('offset',ctypes.c_uint32),('precision',ctypes.c_int32),('scale',ctypes.c_int32),('bare',ctypes.c_uint8),('currency',ctypes.c_uint8)]
class D(ctypes.Structure):
    _fields_=[('weight',ctypes.c_int32),('dscale',ctypes.c_int32),('sign',ctypes.c_int),('ndigits',ctypes.c_uint16),('cap',ctypes.c_uint16),('digits',ctypes.POINTER(ctypes.c_uint16))]
l=ctypes.CDLL(sys.argv[1]);l.pkr_field_parse.argtypes=[ctypes.c_char_p,ctypes.POINTER(Field)];l.pkr_numeric_encode.argtypes=[ctypes.POINTER(Field),ctypes.c_char_p,ctypes.c_size_t,ctypes.c_void_p,ctypes.c_size_t,ctypes.POINTER(ctypes.c_size_t),ctypes.c_void_p,ctypes.c_size_t];l.dec_init.argtypes=[ctypes.POINTER(D)];l.dec_free.argtypes=[ctypes.POINTER(D)];l.dec_unpack.argtypes=[ctypes.c_void_p,ctypes.c_size_t,ctypes.POINTER(D)];l.dec_to_text.argtypes=[ctypes.POINTER(D),ctypes.c_void_p,ctypes.c_size_t,ctypes.POINTER(ctypes.c_size_t)]
rng=random.Random(20261025)
with localcontext() as ctx:
    ctx.prec=5000
    for i in range(3000):
        precision=rng.randint(1,1000);scale=rng.randint(-1000,1000)
        decl=f'numeric:{precision},{scale}';coef=rng.randrange(10**rng.randint(1,45));exponent=rng.randint(-200,200)
        text=('-' if rng.randrange(2) else '')+str(coef)+'e'+str(exponent)
        a=Decimal(text);q=a.quantize(Decimal(1).scaleb(-scale),rounding=ROUND_HALF_UP)
        fits=q==0 or abs(q)<Decimal(1).scaleb(precision-scale)
        f=Field();assert l.pkr_field_parse(decl.encode(),ctypes.byref(f))==0
        out=ctypes.create_string_buffer(1024);used=ctypes.c_size_t(999);raw=text.encode();rc=l.pkr_numeric_encode(ctypes.byref(f),raw,len(raw),out,len(out),ctypes.byref(used),None,0)
        assert rc==(0 if fits else 4),(i,decl,text,rc,fits)
        if not fits:assert used.value==0;continue
        d=D();l.dec_init(ctypes.byref(d));assert l.dec_unpack(out,used.value,ctypes.byref(d))==0
        buf=ctypes.create_string_buffer(5000);n=ctypes.c_size_t();assert l.dec_to_text(ctypes.byref(d),buf,len(buf),ctypes.byref(n))==0
        expected=format(abs(q) if q==0 else q,'f') if scale>=0 else format(q.quantize(Decimal(1)),'f')
        if q==0 and expected.startswith('-'):expected=expected[1:]
        assert buf.value.decode()==expected,(i,decl,text,buf.value,expected)
        assert used.value<=f.payload_max;l.dec_free(ctypes.byref(d))
print('3000 passed, 0 failed; independent Decimal assignment/round/render/cap oracle, seed20261025')
```

### Scratch oom_probe.c

```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
static size_t calls, fail_at;
static void *pmalloc(size_t n) { if (++calls == fail_at) return NULL; return malloc(n); }
static void *prealloc(void *p,size_t n) { if (++calls == fail_at) return NULL; return realloc(p,n); }
#define malloc pmalloc
#define realloc prealloc
#include "decnum.c"
#include "packed_record.c"
#undef malloc
#undef realloc
static int untouched(const uint8_t *p,size_t n) { for(size_t i=0;i<n;i++)if(p[i]!=0xA5)return 0;return 1; }
int main(void) {
 PkrField f;PkrSchema s;assert(pkr_field_parse("numeric:3,1",&f)==DEC_OK);
 assert(pkr_schema_build(&f,1,&s)==DEC_OK);
 uint8_t payload[64],record[64],out[64];size_t payn=0,recn=0,n=0;
 calls=0;fail_at=0;assert(pkr_numeric_encode(&f,"1.25",4,payload,sizeof(payload),&payn,NULL,0)==DEC_OK);
 size_t enc_alloc=calls;PkrCell c={payload,payn,0};
 calls=0;assert(pkr_compose(&s,&c,1,record,sizeof(record),&recn)==DEC_OK);size_t compose_alloc=calls;
 PkrView v;calls=0;assert(pkr_view(&s,record,recn,PKR_FLAGS,&v)==DEC_OK);size_t view_alloc=calls;
 assert(enc_alloc && compose_alloc && view_alloc);
 for(size_t i=1;i<=enc_alloc;i++) {calls=0;fail_at=i;memset(out,0xA5,sizeof(out));n=99;
  assert(pkr_numeric_encode(&f,"1.25",4,out,sizeof(out),&n,NULL,0)==DEC_ERR_NOMEM);
  assert(n==0&&untouched(out,sizeof(out)));}
 for(size_t i=1;i<=compose_alloc;i++) {calls=0;fail_at=i;memset(out,0xA5,sizeof(out));n=99;
  assert(pkr_compose(&s,&c,1,out,sizeof(out),&n)==DEC_ERR_NOMEM);
  assert(n==0&&untouched(out,sizeof(out)));}
 for(size_t i=1;i<=view_alloc;i++) {calls=0;fail_at=i;memset(&v,0xA5,sizeof(v));
  assert(pkr_view(&s,record,recn,PKR_FLAGS,&v)==DEC_ERR_NOMEM);
  assert(!v.schema&&!v.value&&!v.len);}
 printf("encode=%zu compose=%zu view=%zu allocations; every injected failure preserves output contracts\n",enc_alloc,compose_alloc,view_alloc);
 return 0;
}
```

Run ordinary compilation from repo root. Decimal uses explicit high precision
and ties-away rounding. No sanitizer/performance gate is run.

```sh
rtk proxy cc -O1 -Wall -Wextra -Werror -shared -fPIC -Isrc/db src/db/packed_record.c src/db/decnum.c -o /tmp/p3a-check/libpacked.so
rtk proxy python3 /tmp/p3a-check/numeric_oracle.py /tmp/p3a-check/libpacked.so
rtk proxy cc -O1 -Wall -Wextra -Werror -Isrc/db /tmp/p3a-check/oom_probe.c -o /tmp/p3a-check/oom_probe
rtk proxy /tmp/p3a-check/oom_probe
```

Expected 3000/0(seed20261025); encode=3 compose=3 view=3 allocations and every
injected failure preserves output contracts.

### Task3a — documentation

Create docs/concepts/packed-record-codec.md with this complete body:

```markdown
# Inactive packed-record codec

The internal packed_record module prepares future exact numeric/varchar records.
It has no production typed-schema adapter or public activation path. Existing
objects, declarations and commands keep their current behavior. Do not manually
put packed values into a public object.

The codec takes immutable schema-order cells. Fixed binary values stay in the
directory; variable fields use an eight-byte big-endian offset/length descriptor
and contiguous actual-size payloads. A null bitmap distinguishes NULL, empty
varchar and decimal zero. Numeric bytes use decnum's exact packed format.

pkr_measure computes actual value bytes; pkr_compose validates before writing
and receives actual destination capacity. No schema-maximum record buffer is
needed. Constructors/views clear outputs on failure; composition and numeric
encoding leave destination bytes untouched and report used=0 on failure.
Inputs, schemas, views and outputs follow the nonoverlap/borrowed-lifetime
contract in src/db/packed_record.h.

Raw transport accepts generation bytes0,1,3. Generation3 means PACKED+NULLMAP.
Unknown flags and PACKED alone are rejected before incoming upserts stage any
rows. This transport support does not activate packed schemas.

Bare varchar has a codec limit of4MiB. Bare numeric retains decnum's finite
PostgreSQL-sized limits. Actual rows remain subject to the existing segment
limit; a large schema maximum is not a promise that all maximum fields fit
together in one segment.

Consumer integration, authoritative layout metadata, durable publication,
downgrade protection and public activation require later reviewed plans.
No migration or application-level numeric capability is delivered by this
internal preparatory module alone.
```

### Task3b — evidence and hand-back

Write concise actual command/output evidence to
docs/plans/2026-10-25-phase3a-packed-codec-evidence.md with identity, changed-file
inventory, discrepancies, and protected start/end hashes. Distinguish execution
from planning-time checks. Raw logs/probes/binaries/debug files stay in /tmp.


```sh
rtk git diff --check
rtk git diff --stat
rtk git status --short
rtk proxy rg -n 'pkr_|PACKED|SREC_KNOWN_FLAGS|srec_flags_valid' src/db/packed_record.h src/db/packed_record.c src/db/types.h src/db/slotcask.c src/db/seg_scan_varlen.h
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
```

Execution manifest:build.sh; new packed_record.h/.c; types.h,seg_scan_varlen.h,
slotcask.c/.h; two new test files; new internal codec doc and concise evidence.
Preserve existing plans. config.c,storage.c,query*.c,index.c,embedded.c and
existing tests remain unchanged.

Stop uncommitted/unstaged for plan-aware and independent blind raw-diff review.
Do not start 3B, activate, commit/push/open PR. Passing scratch replay is not
human approval.

## Planning-time evidence and limits

Scratch /tmp/phase3-repair-8mj00p4h/checkout came from git archive of the exact
baseline without the protected untracked file. Candidate applied only there;
tracked production source in the shared workspace remains untouched.

Measured:

- Original stub build:exit0,no compiler warnings.
- Earlier full RED revision:18471/65,518 cases, 516 existing cases individually green.
- Codec stub RED:16/61; pre-callback physical stub RED:11/32. Final physical
  stub-stage count 11/38 is an unmeasured forecast, not evidence.
- Complete candidate build:exit0,no compiler warnings.
- Final focused GREEN:codec 268/0,storage 103/0.
- Independent Python Decimal:3000/0.
- Deterministic OOM:encode/compose/view each3 sites; every failure output correct.
- Independent blind numeric probe:9332 roundtrips and 3418 clean assignment failures.
- Review found invalid-write masking and missing reuse/splitting coverage;
  repaired and independently re-reviewed with no remaining findings. The later
  computed-generation guard and window cleanup were also independently reviewed.
- Final full normal suite:18816 passed / 0 failed across 518 cases; exit 0.
- Callback repair RED against otherwise-green candidate:96/7; invalid computed
  NEW flags reached fallback emission and mutated fresh/existing records.

No livePG,sanitizers,benches,activation,migration,git staging/commit claimed.
Every final code block is byte-checked against scratch; executor still proves
RED/GREEN on the real checkout after approval.

A sandbox-only full-suite attempt failed 7272/337 because socket creation was
denied (a direct socket probe returned EPERM); it is not code verification.
The socket-enabled full suite is recorded separately above.

A scratch orchestration error started a duplicate build during another build.
Duplicate was interrupted. The overlapping log is not clean-build evidence;
the later isolated complete candidate build and test runs are the evidence.

## Fresh blind-review pass — 2026-10-25

A second review round used fresh reviewers without planning history. Two
reviewers inspected the extracted candidate and real repository sources
without reading this plan or prior reviews; a separate reviewer checked plan
executability. No actionable code or execution blocker was found.

Independent ordinary codec probes passed 10,000 random mixed-schema roundtrips
and 19,462 constrained numeric encode/compose/view roundtrips. Transport review
covered entry/coordinator validation, callback changes before fallback,
failure cleanup and replay/compaction consumers by source inspection.

All 16 replacements still matched exactly once; complete code-block hashes
and extracted candidate bytes matched. The physical test does not directly
exercise pending-marker replay, actual segment compaction or concurrent
packed writes; those remain later integration gates, not claimed coverage.

Only prose formatting and the known local-socket environment prerequisite
were amended in this review. No proposed code bytes changed, so the final
18,816/0 full-suite evidence remains applicable.

## Next slice boundary

After approved 3A execution and raw-diff review, prepare the complete 3B child
against its exact resulting HEAD. The Phase 3 index carries all original
findings and mandatory 3B/3C acceptance. They are not yet executable plans;
approval here never waives their reviews.
