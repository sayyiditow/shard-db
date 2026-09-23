# Aggregate type correctness and temporal units

## Status

Proposed plan, revised 2026-09-22 after a code-verification review pass (every
anchor and claim below was checked against the source; two behavioral probes
were run against a locally built binary — see "Baseline evidence").

Two decisions were applied as recommended defaults during revision and are
flagged here for the human reviewer to override before execution:

1. **Task 5 was replaced.** The previous Task 5 (datetimems criteria
   multiplier `100000000LL` → `1000000000LL`) was arithmetically wrong: the
   stored `FT_DATETIMEMS` sub-field is true ms-of-day (0..86399999), not the
   `HHmmssfff` digit tail, and the criteria planner composes both sides of
   every comparison with the same multiplier — its range comparisons are
   self-consistent and correct. The replacement Task 5 fixes a *verified*
   criteria defect in the same territory: `IN`/`NOT_IN` on `datetime` and
   `datetimems` matches nothing / everything (probe evidence below).
2. **`count(time)`/`count(uuid)` doctrine.** Encoded-zero = missing: a
   midnight `00:00:00` time and an all-zero uuid are excluded from
   `count(field)`, matching what `exists` (`match_typed` OP_EXISTS) and `get`
   already do for those types. This extends the plan's calendar doctrine to
   the two remaining types that have an encoded unset marker. `enum` byte
   index 0 is a real value and counts.

No implementation, build, test, branch, or commit work is included in this
plan.

## Problem statement

Aggregate execution currently has multiple type-conversion paths. Their
supported-type sets are not identical, unsupported values collapse to numeric
zero, zero-valued numeric fields are sometimes discarded, temporal aggregate
values do not use the same representation as `get`, and the record-scan and
indexed group-by paths disagree about zero-valued group keys.

Root causes (all verified against the source on 2026-09-22):

1. `typed_field_to_double()` (`src/db/query_aggregate.c`, record path) has no
   `FT_FLOAT` or `FT_TIMESTAMP` arms. This breaks the record-scan, grouped,
   keyset, and fetch-backed aggregate paths for those two types.
2. `decode_index_key_to_double()` (`src/db/query_aggregate.c`, indexed path)
   has `FT_FLOAT` but no `FT_TIMESTAMP` arm. The indexed fast path silently
   produces an empty aggregate for indexed timestamps.
3. Temporal aggregates emit ad-hoc doubles (`date*1e6+secs`,
   `date*1e8+ms_of_day`) that are neither the canonical wire strings `get`
   emits nor exact (a 17-digit `datetimems` value exceeds double's 2^53 exact
   range, and ms-of-day is not the `HHmmssfff` digit tail, so *no* numeric
   multiplier produces the canonical value). Calendar aggregates must emit
   the exact canonical strings instead.
4. Out of scope after verification: the criteria planner's *range*
   comparisons for calendar types are self-consistent (bounds and record
   values compose with the same multiplier; `datetime` uses `date*1e5+secs`,
   `datetimems` uses `date*1e8+ms_of_day`, both overflow-safe and
   order-preserving). The real criteria-side temporal defect is the missing
   `IN`/`NOT_IN` in-list — fixed in Task 5, not here.
5. The aggregate scanner treats zero as absent for several numeric types.
   This corrupts `avg`, `min`, and `max` whenever zero is a real stored value
   (e.g. values `[10, 0]`: today `avg` = 10/1; correct is 10/2).
6. Varchar aggregation uses `atof()` for all non-empty strings. Non-numeric
   text becomes zero. The protocol ([docs/query-protocol/aggregate.md]) lists
   `min`/`max` as "Numeric or varchar" without defining varchar ordering, and
   does not document numeric coercion for varchar at all.
7. Unsupported types return "no value," and the final aggregate renderer
   converts "no value" to `0`, making unsupported operations indistinguishable
   from a real zero result.
8. The record-scan group-by path drops numeric-zero group keys:
   `typed_field_to_buf_raw()` hard-skips zero for long/int/short/double/float,
   so a `0` group collapses into the empty-string group. The indexed group-by
   paths already pass `skip_zero=0` to `decode_idx_to_buf()` and emit `"0"`,
   and the integer-key fast path (`typed_field_to_raw`) already emits full
   width for `v == 0` — the paths disagree with each other.

## Baseline evidence

Probes run against a local build of the base branch (scratch daemon, tmpdir,
free port; object `ev` with two rows: `k1 = 20260101…`, `k2 = 20260102…`):

| Query | Expected | Indexed | Unindexed |
|---|---|---|---|
| `eq` datetime `20260101000000` | 1 | 1 | 1 |
| `in` datetime [both rows] | 2 | **0** | **0** |
| `not_in` datetime [row 1] | 1 | **2** | **2** |
| `eq` datetimems `20260101000000000` | 1 | 1 | n/a |
| `in` datetimems [both rows] | 2 | **0** | **0** |
| `not_in` datetimems [row 1] | 1 | **2** | n/a |

Indexed and unindexed routes are broken identically: the defect is in the
shared per-record typed matcher, not in candidate selection.

## Decided output and operation contract

Calendar fields retain their existing exact string representation (the same
strings `get` emits — see `config.c` render arms):

- `date`: string `yyyyMMdd` (8 digits)
- `datetime`: string `yyyyMMddHHmmss` (14 digits)
- `datetimems`: string `yyyyMMddHHmmssfff` (17 digits)
- `timestamp`: JSON number containing Unix epoch milliseconds, unchanged from
  the current read contract

This is not a JSON syntax limitation. The calendar values remain strings
because their existing wire representation is textual and the 17-digit
`datetimems` value cannot be represented exactly by common IEEE-754-based
clients. `timestamp` remains numeric because its epoch-millisecond read
behavior is working and is not being changed.

**Unset markers.** Each type's encoded unset marker, as already defined by
`get` and `exists`: empty varchar content; all-zero encoded `date`,
`datetime`, `datetimems`; midnight time (`00:00:00`, all-zero 3 bytes);
all-zero uuid. `enum` index 0 is a real value, not a marker. Numeric zero is
a real value for every numeric-capable type.

Operation policy:

- `count`: all concrete field types, counting present values only — records
  whose field value is not the type's unset marker. Fixed-width numeric
  fields (including `bool`/`byte`) count every record, including zero values.
  `count(*)`/`count()` counts all records. Composite/legacy field expressions
  are not concrete fields and are rejected with the explicit unsupported-type
  error rather than entering the old string fallback.
- `sum`/`avg`: `int`, `long`, `short`, `float`, `double`, `numeric`, `bool`,
  `byte`, and `timestamp`; `timestamp` returns a JSON number. `date`,
  `datetime`, and `datetimems` reject `sum`/`avg`.
- `min`/`max`: numeric types plus lexicographic `varchar`; `date`,
  `datetime`, and `datetimems` return exact canonical strings; `timestamp`
  remains a JSON number.
- `bool` and `byte`: accepted by `sum`/`avg`/`min`/`max` with numeric `0/1`
  behavior; documented and tested explicitly.
- `time`, `uuid`, `enum`, `ipv4`, `ipv6`: reject non-count aggregates with an
  explicit unsupported-type error.
- String aggregate results from `varchar`, `date`, `datetime`, and
  `datetimems` support lexicographic `having` comparisons and `order_by`,
  using the same length-aware byte-order comparison as `min`/`max`.
- A text `min`/`max` with no present values emits JSON `null` and an empty
  CSV cell. `having` treats that null result as non-matching (every having
  leaf on it fails, including `neq`/`not_in`).
- Numeric zero is a present value everywhere: `sum`, `avg`, `min`, `max`,
  `count`, and group keys. The scan path must produce the same groups as the
  indexed path (integer-key and string-key paths agree).
- `get`'s separate legacy behavior of omitting zero-valued `long`/
  `timestamp`/`time`/`uuid` fields from read output is explicitly out of
  scope; Task 6 documents the distinction in one sentence.

## Execution rules

- Branch from the default branch using a `fix/<short-name>` branch before
  execution.
- Execute tasks in order.
- Leave all changes uncommitted for raw-diff review, per this repository's
  standing exception.
- If any quoted anchor is not found exactly, write `PLAN_NOTES.md` describing
  the mismatch and stop execution immediately.
- Do not weaken or skip a regression test. Task 7 sanctions specific
  expectation updates to existing tests that pin the *old, defective*
  contract; those updates are contract changes, not weakening. Any existing
  test that fails and is not covered by Task 7's table is a stop-and-ask,
  not an edit.
- Build/test commands: `SKIP_TESTS=1 ./build.sh`;
  `./build/bin/shard-db-test run[-all]`.

## Task 1 — Add the aggregate type-matrix regression test first

### Test-first step

Add a new isolated C test case before changing production code. Run it on the
base branch and capture the expected failures (listed under "Expected red
assertions" below). Temporarily revert any later fix, rerun this test, and
preserve both the red and green outputs in the execution report.

### Test location

Create `src/test/cases/test_aggregate_type_matrix.c`, following the existing
`TestEnv`/`TestClient` pattern anchored by the exact text
`static int test_datetimems_run(void)` in `src/test/cases/test_datetimems.c`
and the exact text `static int test_float_field_type_run(void)` in
`src/test/cases/test_float_field_type.c`.

### Required fixture

Create one object containing separate fields for every supported user type:

```c
"fields":[
  "v:varchar:32",
  "i:int",
  "l:long",
  "s:short",
  "d:double",
  "f:float",
  "b:bool",
  "by:byte",
  "n:numeric:12,2",
  "da:date",
  "dt:datetime",
  "dms:datetimems",
  "tm:time",
  "ts:timestamp",
  "u:uuid",
  "e:enum(red,blue)",
  "ip4:ipv4",
  "ip6:ipv6"
]
```

(The `enum(...)` literal form matches existing fixtures, e.g.
`src/test/cases/test_enum.c:40`.)

Records — insert at least:

- rows carrying distinct varchar values `"2"` and `"10"` (lexicographic
  min/max ordering differs from numeric ordering), a numeric-looking varchar
  (`"12.5"`), a quote/backslash-containing varchar (`a"b\c`), and a non-ASCII
  varchar (`ünïcode`);
- one row with negative numeric values and one with valid numeric zeros
  (`i:0`, `l:0`, `s:0`, `d:0`, `f:0`, `b:0`, `by:0`, `n:"0.00"`, `ts:0`);
- one row with all temporal/binary fields omitted from the insert JSON so
  their stored bytes are the all-zero unset markers (`da`, `dt`, `dms`, `tm`,
  `u`), while its varchar is non-empty.

Index at least `f`, `ts`, `dt`, `dms`, `v`, and one ordinary numeric field
(`i`). Keep the fixture local to the test environment; do not use fixed
ports, global paths, or process-wide mutable state.

### Assertions

The test must exercise each aggregate through both a no-criteria indexed path
and a criteria-driven record-scan path (criteria on a *non-indexed* field)
where applicable, and through both public aggregate entry points — the JSON
`mode:"aggregate"` path (`cmd_aggregate`) and the NQL text path
(`cmd_aggregate_tree`), which tests drive by sending a raw NQL line such as
`aggregate <dir> <obj> sum(i) --group-by v` as the request (see
`src/test/cases/test_nql_agg_filter_flag.c:47-50` for the pattern). JSON-only
where the NQL grammar cannot express the spec (composite field references,
JSON-array `in` lists).

- `count(*)` and `count(field)` for every concrete user type.
- `count` present-value doctrine: `count(v)` excludes the empty varchar;
  `count(da)`, `count(dt)`, `count(dms)` exclude the all-zero row;
  `count(tm)` excludes midnight `00:00:00`; `count(u)` excludes the all-zero
  uuid; `count(i)`, `count(b)`, `count(by)`, `count(ts)` include zero values
  (equal to total row count).
- `count` for a composite field expression (e.g. `"i+v"`) must return the
  explicit `aggregate count unsupported for field type composite` error
  through both public aggregate entry points; likewise `sum` and `min` on a
  composite field.
- `sum`, `avg`, `min`, and `max` for `int`, `long`, `short`, `double`,
  `float`, `numeric`, and `timestamp`.
- `sum`, `avg`, `min`, and `max` for `bool` and `byte`, asserting numeric
  `0/1` values and zero preservation.
- `min` and `max` for `date`, `datetime`, and `datetimems`, asserting exact
  quoted strings `yyyyMMdd`, `yyyyMMddHHmmss`, and `yyyyMMddHHmmssfff`.
- Rejection of `sum` and `avg` for `date`, `datetime`, and `datetimems`.
- Zero-sensitive `avg`, `min`, and `max` cases (the `[10, 0]` average case
  among them).
- Lexicographic `min`/`max` for varchar: `min(v) = "10"`, `max(v) = "2"`
  (strings, quoted).
- `sum`/`avg` on varchar must return the explicit unsupported-type error,
  not `0`.
- Non-count aggregates on `time`, `uuid`, `enum`, IPv4, and IPv6 must return
  the explicit unsupported-type error.
- String aggregate aliases must work in `having` and `order_by` with
  lexicographic comparison for varchar and calendar values.
- String group-by fields must also work in `having` with lexicographic
  comparison, because `having` accepts aggregate aliases and group-by fields.
- Empty textual aggregates (criteria matching no rows) must render as JSON
  `null`, an empty CSV cell, and must fail every `having` comparison,
  including `neq`/`not_in`.
- Varchar min/max with the quote/backslash and non-ASCII values must produce
  valid JSON (escaped correctly) and exact lexicographic ordering; grouped
  ordering by the textual group-by field `v` and by a textual calendar
  aggregate must exercise the non-heap sorter.
- Grouping by an indexed and a non-indexed numeric field must preserve a zero
  group as the literal numeric value `0`, distinct from an empty/missing key;
  exercise both the integer-key fast path and the string-key path, and
  verify the indexed and scan paths produce identical group sets.
- `timestamp` aggregates must preserve epoch-millisecond values at
  approximately `1.789e12` and remain JSON numbers; `ts:0` participates in
  `sum`/`count` (not skipped).
- JSON assertions must verify type as well as value: timestamp/numeric
  results are unquoted numbers, while varchar/date/datetime/datetimems
  min/max results are quoted strings.

### Expected red assertions (base branch)

Running the matrix on the base branch must fail, for these reasons: missing
`FT_TIMESTAMP`/`FT_FLOAT` arms (scan paths); indexed timestamp aggregates
returning empty; calendar min/max returning numbers instead of quoted
canonical strings; zero-valued numerics excluded from `sum`/`avg`/`min`/
`max`/`count` and the zero group collapsing; varchar `sum`/`avg` returning
`0` instead of an error; unsupported types returning `0` instead of errors;
composite `count` falling through to the string fallback instead of erroring.

### Verification

Build the base branch first with `SKIP_TESTS=1 ./build.sh`, then run
`./build/bin/shard-db-test run test-aggregate-type-matrix`. It must compile
and fail assertions for the reasons listed above before Task 2 begins.

## Task 2 — Centralize aggregate value classification and conversion

### Test-first step

Use the failing matrix from Task 1 as the red test. Add no production change
until the timestamp, float scan, zero, varchar, and unsupported-type
assertions have been observed failing for their expected reasons.

### Production anchor

Add the classification enum and replace the converter immediately before the
exact text
`static int typed_field_to_double(const TypedField *f, const uint8_t *p, double *out) {`
and replace that complete function.

```c
typedef enum {
    AGG_VALUE_UNSUPPORTED = -1,
    AGG_VALUE_MISSING = 0,
    AGG_VALUE_PRESENT = 1
} AggValueStatus;
```

Complete replacement of `typed_field_to_double` (signature gains an
`available` byte-count so the converter can distinguish a real stored zero
from truncated field data):

```c
/* Classify + convert a typed field to a numeric aggregate value.
   AGG_VALUE_PRESENT for a valid stored value (including numeric zero),
   AGG_VALUE_MISSING when the record does not carry the field (`available`
   smaller than the type's encoded width) and for varchar with empty
   content, AGG_VALUE_UNSUPPORTED for types that never aggregate
   numerically (varchar, calendar, time, uuid, enum, ipv4/ipv6).
   Zero is a real value for numeric-capable types: a stored 0 reaches
   sum/avg/min/max instead of being treated as absent. */
static AggValueStatus typed_field_to_double(const TypedField *f,
                                            const uint8_t *p,
                                            size_t available,
                                            double *out) {
    size_t need;
    switch (f->type) {
    case FT_LONG:
    case FT_TIMESTAMP: need = 8; break;
    case FT_INT:       need = 4; break;
    case FT_SHORT:     need = 2; break;
    case FT_DOUBLE:    need = 8; break;
    case FT_FLOAT:     need = 4; break;
    case FT_NUMERIC:   need = 8; break;
    case FT_BOOL:
    case FT_BYTE:      need = 1; break;
    default: return AGG_VALUE_UNSUPPORTED;  /* varchar/calendar/binary */
    }
    if (available < need) return AGG_VALUE_MISSING;
    switch (f->type) {
    case FT_LONG:
    case FT_TIMESTAMP: *out = (double)ld_be_i64(p); return AGG_VALUE_PRESENT;
    case FT_INT:       *out = (double)ld_be_i32(p); return AGG_VALUE_PRESENT;
    case FT_SHORT:     *out = (double)ld_be_i16(p); return AGG_VALUE_PRESENT;
    case FT_DOUBLE: { double v; memcpy(&v, p, 8); *out = v;
                      return AGG_VALUE_PRESENT; }
    case FT_FLOAT:   { float v;  memcpy(&v, p, 4); *out = (double)v;
                      return AGG_VALUE_PRESENT; }
    case FT_NUMERIC:
        *out = (double)ld_be_i64(p) / (double)f->numeric_scale_mult;
        return AGG_VALUE_PRESENT;
    case FT_BOOL:      *out = (double)(p[0] ? 1 : 0); return AGG_VALUE_PRESENT;
    case FT_BYTE:      *out = (double)p[0];           return AGG_VALUE_PRESENT;
    default:           return AGG_VALUE_UNSUPPORTED;
    }
}
```

(`FT_TIMESTAMP`'s record encoding is the same signed BE int64 as `FT_LONG` —
see the `FT_LONG`/`FT_TIMESTAMP` shared-arm comment in `config.c`.)

Separate complete helpers for exact text extraction of the calendar types,
with the same `available` rule (buffers need 9/15/18 bytes; the canonical
strings are produced by re-splitting the stored fields exactly as the
`config.c` read arms do — the stored `datetimems` uint32 is true ms-of-day,
which is why the string is built from `ms / 3600000` splits and *not* from a
decimal multiplier):

```c
/* Calendar → canonical wire text (identical to what `get` emits).
   AGG_VALUE_MISSING when truncated or when the encoded value is the
   all-zero unset marker. */
static AggValueStatus typed_date_to_text(const TypedField *f,
                                         const uint8_t *p, size_t available,
                                         char *out, size_t *out_len) {
    (void)f;
    if (available < 4) return AGG_VALUE_MISSING;
    int32_t v = ld_be_i32(p);
    if (v == 0) return AGG_VALUE_MISSING;
    *out_len = (size_t)snprintf(out, 9, "%08d", v);
    return AGG_VALUE_PRESENT;
}

static AggValueStatus typed_datetime_to_text(const TypedField *f,
                                             const uint8_t *p,
                                             size_t available,
                                             char *out, size_t *out_len) {
    (void)f;
    if (available < 6) return AGG_VALUE_MISSING;
    int32_t d = ld_be_i32(p);
    uint16_t t = ld_be_u16(p + 4);
    if (d == 0 && t == 0) return AGG_VALUE_MISSING;
    *out_len = (size_t)snprintf(out, 15, "%08d%02d%02d%02d",
                                d, t / 3600, (t % 3600) / 60, t % 60);
    return AGG_VALUE_PRESENT;
}

static AggValueStatus typed_datetimems_to_text(const TypedField *f,
                                               const uint8_t *p,
                                               size_t available,
                                               char *out, size_t *out_len) {
    (void)f;
    if (available < 8) return AGG_VALUE_MISSING;
    int32_t d = ld_be_i32(p);
    uint32_t ms = ld_be_u32(p + 4);
    if (d == 0 && ms == 0) return AGG_VALUE_MISSING;
    *out_len = (size_t)snprintf(out, 18, "%08d%02d%02d%02d%03d",
                                d, ms / 3600000, (ms % 3600000) / 60000,
                                (ms % 60000) / 1000, ms % 1000);
    return AGG_VALUE_PRESENT;
}
```

Every caller of `typed_field_to_double` must compare the result explicitly
with `AGG_VALUE_PRESENT`; it must not use the old truthiness form
`if (!typed_field_to_double(...))`, because `AGG_VALUE_UNSUPPORTED` is
negative and therefore truthy in C. The three record-path call sites
(`query_aggregate.c` — the fetch-backed walker, `agg_scan_cb`, and the
keyset feeder) pass `available` as the actual remaining record length:
`(size_t)hdr->value_len > tf->offset ? (size_t)hdr->value_len - tf->offset : 0`
— in particular, when the current `g_zero_field_65537` truncation fallback
fires, callers must pass `available = 0` (missing), never the fallback
buffer with an implicit full length. This eliminates today's fake zeros for
truncated `bool`/`byte`/`numeric` fields. Empty varchar is
`AGG_VALUE_MISSING` (content length 0); a fixed-width field containing
all-zero bytes is `AGG_VALUE_PRESENT` when its type permits zero.

Introduce a tagged result model for values that can be emitted or compared:

```c
typedef enum {
    AGG_RESULT_NUMBER,
    AGG_RESULT_TEXT
} AggResultKind;

typedef struct {
    AggResultKind kind;
    int present;           /* 0 = missing/null, 1 = value present */
    double number;
    const char *text;
    size_t text_len;
} AggResult;
```

`present` is mandatory; `kind` must never be used to infer presence. A
present textual result may have `text_len == 0`, while an empty aggregate has
`present == 0` and renders as JSON `null` / an empty CSV cell. Preserve this
bit through worker merges, `having`, ordering, and final rendering.

Use this model for numeric and textual aggregate values in `agg_bucket_value`,
`agg_having_match`, `agg_having_match_tree`, aggregate ordering callbacks,
indexed min/max walkers, grouped renderers, JSON output, and CSV output.
Numeric comparisons remain numeric; text comparisons use the length-aware
byte comparator. Textual values stored in `AggAccum`, worker-local state,
merged state, or sort state must be copied into the aggregate arena; no
result may retain a pointer to a stack buffer, mmap/index leaf buffer, or
record buffer.

Unsupported-type handling must not reuse the existing `0` fallback. Add an
explicit aggregate field validator — complete code:

```c
/* Aggregate × type capability matrix. Shared by cmd_aggregate and
   cmd_aggregate_tree through cmd_aggregate_do, so every entry path
   rejects before scanning, index walking, or the single-spec fast path.
   Returns 0 when every spec names a concrete field type its function
   supports; -1 with err filled for the first rejected spec. */
static const char *agg_fn_name(enum AggFn fn) {
    switch (fn) {
    case AGG_COUNT: return "count";
    case AGG_SUM:   return "sum";
    case AGG_AVG:   return "avg";
    case AGG_MIN:   return "min";
    case AGG_MAX:   return "max";
    }
    return "?";
}

static const char *agg_ft_name(enum FieldType t) {
    switch (t) {
    case FT_VARCHAR:    return "varchar";
    case FT_LONG:       return "long";
    case FT_INT:        return "int";
    case FT_SHORT:      return "short";
    case FT_DOUBLE:     return "double";
    case FT_FLOAT:      return "float";
    case FT_BOOL:       return "bool";
    case FT_BYTE:       return "byte";
    case FT_NUMERIC:    return "numeric";
    case FT_DATE:       return "date";
    case FT_DATETIME:   return "datetime";
    case FT_DATETIMEMS: return "datetimems";
    case FT_TIME:       return "time";
    case FT_TIMESTAMP:  return "timestamp";
    case FT_UUID:       return "uuid";
    case FT_ENUM:       return "enum";
    case FT_IPV4:       return "ipv4";
    case FT_IPV6:       return "ipv6";
    default:            return "unknown";
    }
}

static int agg_validate_spec_types(const TypedSchema *ts,
                                   const AggSpec *specs, int nspecs,
                                   char *err, size_t errsz) {
    for (int i = 0; i < nspecs; i++) {
        const AggSpec *s = &specs[i];
        if (s->fn == AGG_COUNT && s->field[0] == '\0')
            continue;   /* count(*) / count() carry no field — always allowed */

        /* Composite ("a+b") or unresolved legacy references are not one
           concrete TypedField — reject for every function, count included. */
        if (!ts || s->field[0] == '\0' || strchr(s->field, '+')) {
            snprintf(err, errsz,
                     "aggregate %s unsupported for field type composite",
                     agg_fn_name(s->fn));
            return -1;
        }
        int fi = typed_field_index(ts, s->field);
        if (fi < 0)
            continue;   /* unknown field: existing unknown-field error path */

        enum FieldType t = ts->fields[fi].type;
        int ok;
        switch (s->fn) {
        case AGG_SUM:
        case AGG_AVG:
            ok = (t == FT_LONG || t == FT_TIMESTAMP || t == FT_INT ||
                  t == FT_SHORT || t == FT_DOUBLE || t == FT_FLOAT ||
                  t == FT_NUMERIC || t == FT_BOOL || t == FT_BYTE);
            break;
        case AGG_MIN:
        case AGG_MAX:
            ok = (t == FT_LONG || t == FT_TIMESTAMP || t == FT_INT ||
                  t == FT_SHORT || t == FT_DOUBLE || t == FT_FLOAT ||
                  t == FT_NUMERIC || t == FT_BOOL || t == FT_BYTE ||
                  t == FT_VARCHAR || t == FT_DATE ||
                  t == FT_DATETIME || t == FT_DATETIMEMS);
            break;
        case AGG_COUNT:
        default:
            ok = 1;   /* every concrete type is countable */
            break;
        }
        if (!ok) {
            snprintf(err, errsz,
                     "aggregate %s unsupported for field type %s",
                     agg_fn_name(s->fn), agg_ft_name(t));
            return -1;
        }
    }
    return 0;
}
```

Hook it into `cmd_aggregate_do` immediately after the block that resolves
spec `TypedField`s for count specs — anchor text:
`/* Resolve TypedField for COUNT specs too — agg_scan_cb's`. On failure,
emit `{"error":"<err>"}` and return before any planner branch, scan, or fast
path runs. (The old in-scan anchor `/* count(*) / count() carry no field — skip. */`
remains the scanner's own count shortcut; validation now happens earlier.)

The stable error shape is:

```json
{"error":"aggregate <function> unsupported for field type <type>"}
```

The validator must reject calendar `sum`/`avg`, varchar `sum`/`avg`, and all
non-count operations for `time`, `uuid`, `enum`, `ipv4`, and `ipv6`. It must
be shared by both `cmd_aggregate` and `cmd_aggregate_tree` through their
common `cmd_aggregate_do` path.

## Task 3 — Make indexed decoding use the same canonical conversion

### Test-first step

Keep the Task 1 matrix red for indexed timestamp and temporal cases. Run the
indexed cases separately and capture their failures before editing the
indexed decoder.

### Call-site inventory (record here; update every one or justify)

- `typed_field_to_double` — `query_aggregate.c`:1607 (fetch-backed walker),
  :2169 (`agg_scan_cb`), :3245 (keyset feeder). Signature + result-compare
  updates in Task 2.
- `decode_index_key_to_double` — `query_aggregate.c`:644, :842, :1723,
  :1787, :2687, :3117, :3158, :3382, :3619, :3743, :5831. All switch to
  `== AGG_VALUE_PRESENT` / `!= AGG_VALUE_PRESENT`; calendar min/max callers
  (:1723, :1787, :3117, :3158, :3382, :3619, :3743 as applicable) route
  calendar fields to `decode_index_key_to_text` instead.
- `typed_field_to_buf_raw` — `query_aggregate.c`:2085 (`agg_scan_cb` group
  keys), `query_join.c`:222, :356, :360, :514. Signature gains `keep_zero`;
  join sites pass `0` (behavior unchanged), the aggregate site passes `1`.
- `decode_idx_to_buf` — `query_aggregate.c`:2869, :2948, :2953, :5026,
  :5120, :5212, :5719, :5724; `query.c`:281, :328, :838, :1745, :1866,
  :3158. Already caller-controlled via `skip_zero`; aggregate group-by sites
  already pass `0` and emit `"0"` — no change needed beyond verifying zero
  groups render. `query.c` sites are find-path output rendering; untouched.

### Production anchor — numeric decoder

Replace the complete function beginning with the exact text
`static int decode_index_key_to_double(const TypedField *f,` in
`src/db/query_aggregate.c` with a status-returning decoder. New signature:

```c
static AggValueStatus decode_index_key_to_double(const TypedField *f,
                                                 const uint8_t *p, size_t plen,
                                                 double *out);
```

Arms (inverting the existing `encode_field_for_index` transforms — the
on-disk index encoding does not change):

```c
    case FT_LONG:
    case FT_TIMESTAMP: {
        if (plen < 8) return AGG_VALUE_MISSING;
        uint64_t u = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                     ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                     ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                     ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
        int64_t v = (int64_t)(u ^ (1ULL << 63));   /* signed BE, top-bit flip */
        *out = (double)v; return AGG_VALUE_PRESENT;
    }
```

`FT_INT` (4 bytes, `^ 0x80000000u`), `FT_SHORT` (2 bytes, `^ 0x8000u`),
`FT_DOUBLE`/`FT_FLOAT` (IEEE-754 total-order inverse: sign-bit flip or
bitwise-NOT as in the current arms) keep their existing decode transforms
but drop every `if (v == 0) return 0;` skip — zero is
`AGG_VALUE_PRESENT`. `FT_NUMERIC` likewise (divide by
`f->numeric_scale_mult`). `FT_BOOL`/`FT_BYTE` unchanged but status-typed.
`FT_DATE`, `FT_DATETIME`, `FT_DATETIMEMS`, `FT_TIME`, `FT_UUID`, `FT_IPV4`,
`FT_IPV6`, `FT_VARCHAR`, and `default` return `AGG_VALUE_UNSUPPORTED`
(calendar/binary types never aggregate numerically; varchar has its own
path; calendar min/max uses the text decoder below).

### Production anchor — calendar text decoder

New complete function (same file, next to the numeric decoder). It inverts
the index transforms (date/datetime date-part top-bit flip; time-of-day
parts stored raw) and formats the canonical wire strings:

```c
/* Indexed calendar text decoder: btree leaf bytes → canonical wire
   string, identical to the record-path helpers and to `get`.
   AGG_VALUE_MISSING when truncated or all-zero (unset);
   AGG_VALUE_UNSUPPORTED for non-calendar types. */
static AggValueStatus decode_index_key_to_text(const TypedField *f,
                                               const uint8_t *p, size_t plen,
                                               char *out, size_t *out_len) {
    switch (f->type) {
    case FT_DATE: {
        if (plen < 4) return AGG_VALUE_MISSING;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t v = (int32_t)(u ^ 0x80000000u);
        if (v == 0) return AGG_VALUE_MISSING;
        *out_len = (size_t)snprintf(out, 9, "%08d", v);
        return AGG_VALUE_PRESENT;
    }
    case FT_DATETIME: {
        if (plen < 6) return AGG_VALUE_MISSING;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        uint16_t t = ((uint16_t)p[4] << 8) | (uint16_t)p[5];
        if (d == 0 && t == 0) return AGG_VALUE_MISSING;
        *out_len = (size_t)snprintf(out, 15, "%08d%02d%02d%02d",
                                    d, t / 3600, (t % 3600) / 60, t % 60);
        return AGG_VALUE_PRESENT;
    }
    case FT_DATETIMEMS: {
        if (plen < 8) return AGG_VALUE_MISSING;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        uint32_t ms = ((uint32_t)p[4] << 24) | ((uint32_t)p[5] << 16) |
                      ((uint32_t)p[6] << 8)  |  (uint32_t)p[7];
        if (d == 0 && ms == 0) return AGG_VALUE_MISSING;
        *out_len = (size_t)snprintf(out, 18, "%08d%02d%02d%02d%03d",
                                    d, ms / 3600000, (ms % 3600000) / 60000,
                                    (ms % 60000) / 1000, ms % 1000);
        return AGG_VALUE_PRESENT;
    }
    default:
        return AGG_VALUE_UNSUPPORTED;
    }
}
```

Do not change index ordering or on-disk encoding; only reverse the existing
encoding.

### Production anchor — group-key zero policy on the scan path

`typed_field_to_buf_raw` (anchor:
`int typed_field_to_buf_raw(const TypedField *f, const uint8_t *p,`) gains a
final `int keep_zero` parameter, mirroring `decode_idx_to_buf`'s
`skip_zero`:

- signature becomes
  `int typed_field_to_buf_raw(const TypedField *f, const uint8_t *p, char *buf, size_t bufsz, int keep_zero)`
  (update the declaration in `src/db/query_internal.h` to match);
- in the `FT_LONG`, `FT_INT`, `FT_SHORT`, `FT_DOUBLE`, and `FT_FLOAT` arms,
  `if (v == 0) return 0;` becomes `if (!keep_zero && v == 0) return 0;`;
- all other arms are byte-identical (bool/byte/numeric already render zero;
  calendar/time/uuid/ipv4/ipv6/varchar keep their unset-marker skips
  regardless of `keep_zero` — missing is missing, not zero).

Call sites:

- `query_join.c`:222, :356, :360, :514 pass `0` — join key-building and
  projection output behavior is unchanged (joins and `get` treat zero as
  missing today; that contract is not touched by this plan).
- `query_aggregate.c` `agg_scan_cb` group-key site (anchor: the call
  `typed_field_to_buf_raw(gtf, fp,` inside the `ctx->group_tfs[i]` branch)
  becomes truncation-safe and zero-preserving:

```c
        if (ctx->group_tfs[i]) {
            const TypedField *gtf = ctx->group_tfs[i];
            gbuf[i][0] = '\0';
            if ((size_t)gtf->offset + (size_t)gtf->size > (size_t)hdr->value_len) {
                /* truncated field — missing group value, not a fake zero */
            } else {
                typed_field_to_buf_raw(gtf, raw + gtf->offset,
                                       gbuf[i], sizeof(gbuf[i]), 1);
            }
        } else {
```

Zero group keys now render `"0"` on the scan path, matching the indexed
paths (which already pass `skip_zero = 0`) and the integer-key path (which
already emits full width for `v == 0`). The matrix's integer-path vs
string-path vs indexed-path group-equality assertions verify agreement.

### Routing

Update every indexed fast-path eligibility check so calendar `min`/`max`
routes through `decode_index_key_to_text`, calendar `sum`/`avg` is rejected
during validation (Task 2), numeric fields route through the numeric
decoder, and varchar `min`/`max` compares raw indexed bytes (leaf bytes ARE
the raw varchar content — see the `decode_idx_to_buf` header comment — so
byte order is lexicographic order). The single-spec fast-path gate (anchor:
`if (fi >= 0 && fs.ts->fields[fi].type != FT_VARCHAR &&`) becomes
fn-dependent: allow varchar and calendar for `AGG_MIN`/`AGG_MAX` (via the
text/byte paths), keep the numeric decoder for everything else. Merely
adding a decoder without routing callers to it is insufficient: every
call-site listed in the inventory must be updated in this task, and the
executor must re-verify the inventory (line numbers may have drifted) with a
final grep recorded in the execution report.

## Task 4 — Implement explicit varchar min/max semantics

### Test-first step

Run the varchar assertions from Task 1 against the unchanged implementation
and capture the numeric-coercion/zero result. This test must fail before
production changes.

### Production anchor

Update the aggregate scanner loop beginning with the exact text `double v;`
immediately after the `if (ctx->specs[i].fn == AGG_COUNT)` block in
`agg_scan_cb`.

Add one generic textual aggregate state for varchar and calendar min/max
values to `AggAccum` at the exact member block containing
`double sum;`, `double min;`, and `double max;` — exact before/after:

```c
typedef struct {
    double sum;
    double min;
    double max;
    int64_t count;
} AggAccum;
```

becomes:

```c
typedef struct {
    double sum;
    double min;
    double max;
    int64_t count;
    char *text_min;        /* arena-owned; NULL = no textual value yet */
    size_t text_min_len;
    char *text_max;
    size_t text_max_len;
    int text_present;      /* 1 once any textual value was seen */
} AggAccum;
```

The lengths are mandatory: do not recover them with `strlen()` during
comparison, merge, rendering, or sorting. The task must update every
complete lifecycle site for `AggAccum`: initialization, bucket creation
(`agg_find_or_create`'s memset-zero is fine — NULL/0 is the empty state),
worker cloning, worker merge (both the single-context merge and the
per-worker fold), arena ownership (text copied into the arena via the
existing arena allocator — never a raw `malloc` that bypasses the
arena/cleanup model), eviction, teardown, JSON output, CSV output, `having`,
and `order_by`. When a merged-from bucket's text wins the comparison, copy
its bytes into the destination arena.

For varchar:

- `min` and `max` compare raw content bytes using length-aware comparison
  (memcmp to the shorter length, then length decides — the same order as
  btree byte order).
- `sum` and `avg` are rejected during validation (Task 2).
- Empty varchar values are excluded from all field aggregates, matching
  `count(varchar)`.
- Every allocated text state is released through the existing aggregate
  bucket/arena teardown path; no per-record leak is acceptable.

String `having` comparisons must explicitly support `eq`, `neq`, `lt`, `lte`,
`gt`, `gte`, `between`, `in`, and `not_in`, using the same length-aware byte
comparator as varchar criteria. Numeric-only or unrelated operators must
return the explicit unsupported aggregate error.

When a `having` leaf names a group-by field rather than an aggregate alias,
resolve its value from `AggBucket.group_vals` and apply the same typed
numeric or length-aware textual comparison. This applies to varchar and
calendar group-by fields as well as numeric group-by fields; an unknown
field or an unsupported operator must fail validation rather than silently
compare `0`. Having validation happens before scanning: because each
spec's result kind (number vs text) is statically known from
fn + field type, and each having field resolves to an alias or a group-by
field with a concrete type, the Task 2 validator pass also validates every
having leaf's operator against that kind.

For calendar fields, the text state stores the exact canonical output string
and compares it bytewise. Because all three formats are fixed-width and
zero-padded, bytewise order equals chronological order.

Update every aggregate renderer and `having`/`order_by` value reader that
currently assumes all min/max values are doubles. Exact before/after for the
`agg_bucket_value` anchor (note the three spaces after `case AGG_MIN:` /
`case AGG_MAX:`):

```c
                case AGG_MIN:   return a->count > 0 ? a->min : 0.0;
                case AGG_MAX:   return a->count > 0 ? a->max : 0.0;
```

These double-returning readers become `AggResult`-returning (or gain
text-aware siblings); the aggregate sort callbacks and every final output
switch beginning with `case AGG_COUNT: snprintf(vbuf` (three sites:
grouped-JSON, NEQ-split, and whole-table renderers) become kind-aware:
numeric aggregates render through the existing `fmt_double` path unquoted;
textual aggregates render quoted through the existing JSON escape helper;
`present == 0` textual aggregates render JSON `null` (unquoted) and an empty
CSV cell. CSV output emits raw text through the existing CSV escaping
helper.

All textual JSON aggregate values must pass through the existing JSON escape
helper before emission. This includes quotes, backslashes, control
characters, embedded non-ASCII bytes, and empty strings. The matrix's
quote/backslash and non-ASCII assertions verify valid JSON and exact min/max
ordering.

When a textual aggregate has no present value, render JSON `null` and an
empty CSV cell. Do not render `0` or `""`. Numeric aggregates retain their
existing empty-result behavior unless the existing protocol is changed
explicitly (it is not).

The existing numeric top-N heap stores a `double` metric. Disable that
optimization whenever `order_by` resolves to a textual aggregate or to a
textual group-by field (varchar, date, datetime, or datetimems), and route
the query through the full result sort. Do not cast textual values to
numeric metrics. An absent textual value compares equal to the empty string
for ordering. The matrix's grouped tests order by a varchar group-by field
and by a calendar aggregate; both must exercise the non-heap sorter and
verify lexicographic order.

## Task 5 — Fix datetime/datetimems `IN`/`NOT_IN` criteria

### Root cause (verified)

`compile_one`'s `IN`/`NOT_IN` pre-parse switch (`src/db/query_plan.c`,
anchor: `/* IN/NOT_IN list pre-parsing (numerics only — varchar uses raw strings) */`)
has no `FT_DATETIME` or `FT_DATETIMEMS` case — both fall into the raw-string
`default`, which never populates `cc->in_i64`. The per-record matchers for
both types then call `cmp_op_i64(v, q1, q2, cc->op, NULL, 0, cc)` — with a
NULL in-list — so `OP_IN` compares against nothing (every record fails) and
`OP_NOT_IN` compares against nothing (every record passes). Baseline probe:
see "Baseline evidence" — wrong on both indexed and unindexed routes,
identically, because both converge on the same matcher.

The composition multiplier for each type must be the same one the type's
range comparisons already use (`datetime`: `date * 100000LL + seconds_of_day`
— max 86399 < 10^5; `datetimems`: `date * 100000000LL + ms_of_day` — max
86399999 < 10^8). Do not introduce any other multiplier.

### Test-first step

Create `src/test/cases/test_temporal_in_criteria.c` (TestEnv/TestClient
pattern): object with indexed `d:datetime` and `dm:datetimems` plus
unindexed `du:datetime` and `dmu:datetimems`; two rows (20260101…,
20260102…). Assert, for each of the four fields: `eq` control = 1, `in`
[both values] = 2, `not_in` [first value] = 1, `in` [nonexistent value] = 0,
via `count` and via `find` row counts. Run on the base branch: every `in`
assertion must fail (0 ≠ 2 / 0) and every `not_in` assertion must fail
(2 ≠ 1) for the expected reason. Register with
`TEST_REGISTER("test-temporal-in-criteria", test_temporal_in_criteria_run)`.

### Production anchors

In the IN pre-parse switch, immediately after the `case FT_DATE:` block
(anchor: `cc->in_i64[i] = (int64_t)parse_date_i32(c->in_values[i]);`),
insert:

```c
        case FT_DATETIME:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++) {
                int32_t d; uint16_t t;
                parse_datetime(c->in_values[i], &d, &t);
                cc->in_i64[i] = (int64_t)d * 100000LL + (int64_t)t;
            }
            break;
        case FT_DATETIMEMS:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++) {
                int32_t d; uint32_t ms;
                parse_datetimems(c->in_values[i], &d, &ms);
                cc->in_i64[i] = (int64_t)d * 100000000LL + (int64_t)ms;
            }
            break;
```

Update the now-stale `default:` comment (it names DATETIME as a raw-string
type) to name VARCHAR only.

In `match_typed`, the `FT_DATETIME` and `FT_DATETIMEMS` cases each end with

```c
            return cmp_op_i64(v, q1, q2, cc->op, NULL, 0, cc);
```

which becomes, in both cases:

```c
            return cmp_op_i64(v, q1, q2, cc->op, cc->in_i64, cc->in_count, cc);
```

### Verification

Re-run `test-temporal-in-criteria`: green. Re-run `test-datetimems` and
`test-timestamp` (adjacent temporal cases): still green. If any temporal-IN
route (indexed/unindexed × count/find × datetime/datetimems) does not turn
green with exactly this fix, stop and write `PLAN_NOTES.md` — do not add
unplanned planner changes.

## Task 6 — Documentation, changelog, and error contract

### Test-first step

Run the explicit unsupported-type assertions before updating documentation.
The test output must prove that unsupported operations now return errors
rather than numeric zero.

### Documentation anchors

Update the aggregate table beginning with the exact text
`| Function | Needs \`field\` | Notes |` in
`docs/query-protocol/aggregate.md`.

Document:

- all supported numeric types (including `float`, `bool`, `byte`,
  `timestamp`);
- varchar lexicographic `min`/`max` only;
- unsupported aggregate/type combinations and their error shape
  (`{"error":"aggregate <function> unsupported for field type <type>"}`);
- exact calendar aggregate representations: `date` → `yyyyMMdd`,
  `datetime` → `yyyyMMddHHmmss`, `datetimems` → `yyyyMMddHHmmssfff` (quoted
  strings, matching `get`);
- `timestamp` remains a JSON number containing epoch milliseconds;
- `sum`/`avg` are rejected for calendar fields;
- zero-valued numeric fields participate in all numeric aggregates and form
  real `"0"` group keys; the per-type unset markers (empty varchar, all-zero
  calendar, midnight time, all-zero uuid) are "missing" for `count(field)`
  and min/max — one sentence noting `get`'s separate omit-zero read behavior
  is unchanged and out of scope;
- textual `having` and `order_by` use lexicographic comparison for varchar
  and calendar aggregate results;
- empty textual aggregates render as `null` / empty CSV cell.

Add an entry to `docs/reference/changelog.md` describing the behavior change.
Flag the breaking change explicitly in the execution report: wire-visible
changes are (a) calendar `min`/`max` now quoted strings instead of numbers,
(b) `sum`/`avg` on varchar/calendar return errors instead of `0`,
(c) non-count aggregates on `time`/`uuid`/`enum`/`ipv4`/`ipv6` return errors
instead of `0`, (d) empty textual aggregates return `null` instead of `0`,
(e) zero-valued numeric groups now appear. Clients that branched on the old
numeric output must migrate.

## Task 7 — Reconcile existing tests that pin the old contract

The following existing test pins the old (defective) contract and is
sanctioned for exactly this update — nothing else in it may change:

- `src/test/cases/test_agg_leaf_only_walk.c` — the two `birthday` (date)
  min/max assertions use a numeric-prefix helper (`agg_value_eq`) that
  cannot match the new quoted-string output. Replace the two assertion lines
  with string-presence checks:

```c
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"m\":\"20000102\"") != NULL,
                "min birthday = \"20000102\" (quoted canonical string)");
```

and

```c
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"M\":\"20001101\"") != NULL,
                "max birthday = \"20001101\" (quoted canonical string)");
```

and update the file-header comment line describing birthday accordingly.

- `src/test/cases/test_agg_int_groupby_multi.c` — its CSV assertions pin the
  old display of zero-valued integer group fields as empty cells. Update them
  to `0` because zero-valued numeric group keys are now wire-visible values.
Verified survivors (checked against their fixtures/assertions — no changes
needed): `test_agg_walk_fetch_check` (varchar `min(name)` asserts key
presence only), `test_agg_varchar_groupby_sum` (aggregates a numeric field),
`test_field_vs_field` (`sum` over `int`), `test_count_varchar_field` (no
zero-value pins), `test_agg_input_validation`, and the remaining
aggregate-adjacent cases listed by
`rg -l '"mode":"aggregate' src/test/cases/` — all aggregate numeric fields
or assert only key presence.

Policy: after Tasks 2–6, run all aggregate-adjacent cases
(`./build/bin/shard-db-test run-all --filter agg` plus
`--filter aggregate`). Any failure NOT explained by the table above is a
stop-and-ask: write `PLAN_NOTES.md` with the failing assertion and the old
contract it pins; do not edit it under this plan.

## Task 8 — Verification gates

After all fixes:

1. Temporarily revert the production changes while retaining the tests; run
   `./build/bin/shard-db-test run test-aggregate-type-matrix` and
   `./build/bin/shard-db-test run test-temporal-in-criteria` and capture the
   expected red results (the matrix fails for the Task 1 reasons; the IN
   test fails with `in` = 0 and `not_in` = all).
2. Re-apply the changes and run both tests; capture the green results.
3. Build with `SKIP_TESTS=1 ./build.sh`.
4. Run the focused matrix, the IN test, and all aggregate-adjacent existing
   cases.
5. Run the complete suite with `./build/bin/shard-db-test run-all`.
6. This changes aggregate worker state and adds text accumulator ownership,
   so run the mandatory dynamic-safety gates from `AGENTS.md`: ASan/UBSan
   build plus three fresh full-suite runs. TSan remains a required human
   follow-up in this workspace and must use `--jobs 2` for each full-suite
   run; it is intentionally not run during this execution.
7. Inspect the raw `git diff` for protocol changes, error consistency,
   memory ownership, and accidental on-disk/index-format changes (none are
   expected — index encoding is only ever *read*, never written, by this
   plan).
8. Leave the work uncommitted for human and reviewer inspection.

## Expected changed files

- `src/test/cases/test_aggregate_type_matrix.c` (new)
- `src/test/cases/test_temporal_in_criteria.c` (new)
- `src/db/query_aggregate.c`
- `src/db/query_plan.c`
- `src/db/query_join.c` (four `typed_field_to_buf_raw` call sites: new
  `keep_zero` argument, behavior unchanged)
- `src/db/query_internal.h` (`typed_field_to_buf_raw` declaration)
- `src/test/cases/test_agg_leaf_only_walk.c` (Task 7 sanctioned updates)
- `src/test/cases/test_agg_int_groupby_multi.c` (Task 7 sanctioned update)
- `build.sh` (registers the two new regression cases)
- `docs/query-protocol/aggregate.md`
- `docs/reference/changelog.md`
- `AGENTS.md` (documents the workstation TSan `--jobs 2` requirement)

No index encoding, storage layout, dependency, or migration change is
expected. `src/test/cases/test_datetimems.c` is explicitly NOT changed (its
`dtms_to_ordinal` helper composes ordinals consistently with the engine and
stays as-is).
