# Phase 1 — Safe existing scalar conversions and floating output (executable child plan)

Status: **Phase 1 is applied on the uncommitted implementation branch;
review corrections are authorized and in verification.** No commit/push
or later phase is implied. The original fifth-revision body below is the
historical execution baseline, not the current code where an amendment
overrides it.

**Current review-correction amendments (2026-10-22):**

- [Scalar JSON routes / exact-once decoding](2026-10-22-phase1-scalar-routes-amendment.md)
- [Allocation-free textual index encoding](2026-10-22-phase1-index-oom-amendment.md)
- [Complete CSV update cell validation](2026-10-22-phase1-csv-amendment.md)
- [Legacy non-finite computed-copy coverage](2026-10-22-phase1-legacy-copy-amendment.md)

These amendments explicitly supersede the old span_to_cstr/zero-on-OOM
instructions, varchar-only JSON decoding, strlen(cellbuf) update span, and
the claim that non-finite $copy sources cannot occur after upgrade. Old
roots may retain non-finite bytes; write rejection only prevents new ones.
Prior scratch and baseline red/green results remain historical evidence;
current-checkout correction evidence is recorded in the amendments and
evidence ledger.

Parent: [type-correctness recovery plan](2026-10-19-type-correctness-recovery-plan.md)
(Phase 1). Phase 0 evidence:
[phase-0 evidence ledger](2026-10-20-type-correctness-phase0-evidence.md).
Written 2026-10-20 against checkout `1dd1888`.

**Fifth revision, 2026-10-22.** Closes the aggregate gaps from R4: the scratch integration now applies and inventories every aggregate numeric hunk, both aggregate float renderers use 9/17 digits, and dedicated public-route plus direct-helper regressions capture red before those changes. Earlier fourth-revision green results covered only a partial integration; the new evidence addendum supersedes that completeness claim.

**Fourth revision, same day.** The third review compiled the document's
own blocks against real sources and found four more issues; all are
resolved here, and the complete integrated patch is compiled (and the
regression case executed red-then-green) on a scratch copy of the full
repository before this revision is delivered — see the header of the
execution order for the scratch evidence paths.

1. Textual index encoders still truncated: the integer cases used
   `cbuf[16]`/`cbuf[8]` windows, the float/double cases `cbuf[48]` —
   a 20-char leading-zero int stored 1 but its lookup key encoded 0,
   and a 50-char float/double literal stored 1e10 while its key dropped
   the exponent. Fixed: every in-scope scalar text→key conversion now
   parses the complete span (`span_to_cstr` + the same library call the
   scan-side compiler uses — `strtoll`/`strtod`/`strtof`; numeric keeps
   `conv_numeric_text`), so indexed and scan routes agree on every
   literal writes accept. The FT_DOUBLE "no parity gap" claim was also
   wrong (same function, truncated input) and is withdrawn.
2. `parse_full_i64` kept a raw 20-character cap, rejecting valid
   leading-zero longs like `0000000000000000000001`. Fixed: validate
   the whole span, strip leading zeros, and bound only significant
   digits (≤19) with the sign-aware magnitude check.
3. Integration contradictions removed: the stale parenthetical claiming
   `typed_type_name` lives in config.c is gone (type_desc.c is the one
   placement); the aggregate division sites at query_aggregate.c:1311
   and :2060 were attributed to each other's functions — 1311 is
   `decode_index_key_to_double`, 2060 is `typed_field_to_double`;
   the scope paragraph and consumer inventory now explicitly include
   the `encode_field_for_index` successful-literal parity work in
   Phase 1 instead of deferring it.
4. The execution order now captures red FIRST — the regression case is
   added to unchanged production code and its failures recorded against
   the real baseline — and only then are Tasks 1-6 applied for green.
   The temporary-revert procedure is gone.

**Third revision, same day.** The second review confirmed the compile and
parity fixes but found five more issues; all are resolved here:

1. Textual index-key encoders still converted numeric/float literals
   through `atof`/`(float)atof` (config.c:2264-2270 and the
   `encode_field_for_index` float case), so exactly-written values could
   fail indexed lookups; numeric whitespace and the float
   double-rounding gap also broke lookup parity. Fixed: numeric
   lookup keys use `conv_numeric_text` (which now strips outer
   whitespace itself), float lookup keys and the float criterion
   compiles parse with `strtof` directly — the matcher casts the
   criterion to float (query_plan.c:997), so a strtof-parsed criterion
   equals the strtof-parsed stored value bit-for-bit.
2. Scale-zero `INT64_MIN` decoded as `--9223372036854775808` (invalid
   JSON): the renderers negated `INT64_MIN`, which is undefined behavior
   (and aborts under the strict UBSan build). All three numeric
   renderers now form the magnitude in unsigned arithmetic.
3. Two integrated hunks still failed compilation: the
   `typed_field_to_buf_raw` guard re-declared `scale` (the existing
   declaration is kept), and the `update_compose_new_from_old`
   definition replacement ended with `;` instead of ` {`. Corrected.
4. The regression used the wrong bulk request field (the wire field is
   `records`, server.c:1938) and the wrong tally assertion (the
   response carries `"errors":N`, bulk.md:71), and expected `1.23456777`
   where the binary32 value of 1.2345678 renders `1.23456776` under
   `%.9g` — exactly as Phase 0's types-audit output already showed.
   Corrected, and the route-shape hedge paragraph is gone.
5. `0e19` (any all-zero digit string with an exponent) and leading-zero
   forms such as `0000000000000000000001` were rejected.
   `conv_numeric_text` now short-circuits all-zero digit strings to 0
   regardless of exponent and counts only significant digits against
   the 19-digit magnitude budget.

**Second revision, same day.** Human review of the first revision found
eight issues; all are resolved here:

1. Core and test did not compile — fixed (missing `<math.h>`/`<stdarg.h>`,
   a non-variadic error formatter called with format arguments, a bogus
   conditional in it, and 1-arg `ASSERT_NOT_NULL` calls); every code block
   in this revision was extracted from this document and passed
   `cc -fsyntax-only` against the real headers before delivery.
2. Exact writes disagreed with the unchanged `atof`-based criterion parser
   (`0.145` at scale 2 stored `0.15`, but `eq "0.145"` still computed 14) —
   fixed by a new criteria-parity task: `parse_numeric_i64` now uses the
   same exact converter as writes. Full criteria overhaul remains Phase 5.
3. `6e-100` at scale 2 rounded to `0.01` — the round-up test read the
   first dropped digit even when the entire value was smaller than half a
   unit in the last kept place. Fixed: round up only when the digit string
   is at least as long as the dropped run.
4. Blanket whitespace trimming changed varchar contents and could select a
   different enum member — trimming is now applied to the scalar numeric
   types only; varchar/enum are byte-exact.
5. The legacy-scale fallback set `numeric_scale_mult = 0` but guarded only
   two of the division sites — all five `numeric_scale_mult` division/
   multiplication consumers are now guarded with exact hunks.
6. Scale-zero numerics and the minimum signed scaled value (`INT64_MIN`
   scaled) were rejected — the scale guard now admits 0, and the magnitude
   bound is sign-aware (`2^63` for negatives).
7. Integration edits were incomplete and coverage thin — the bulk JSON
   path is documented as needing no edit (it already counts `-2` per row),
   the typed_encode test-caller hunks are exact, and the regression case
   now covers bulk per-row errors, update preservation, defaults
   rejection, criteria parity, scale-0/INT64_MIN/`6e-100` numerics,
   varchar byte-exactness and scalar trimming.
8. The Phase 0 ledger's composite-index budget conclusions were wrong
   (tier bytes are per-component, not per-key) — the ledger and the
   parent's index-feasibility paragraph are corrected; see those files.

**Scope boundary (verified against the parent):** this plan changes no
on-disk format and no wire shape except float/double *text precision*
(documented below). It deliberately extends into two query-side
surfaces for **successful-literal lookup parity** — exact writes must be
findable by their own literals on every route: `query_plan.c`'s numeric
and float criterion compilation, and the in-scope scalar text→key
conversions inside `encode_field_for_index` (Task 3b). The index key
*format* is unchanged; Phase 5 retains the packed-decimal key codec,
invalid-literal erroring policy, remaining operators, and its full
P=1..1000 feasibility work. If execution discovers a hunk that cannot
land without a format change, execution halts per the parent's rules.

## Root causes (Phase 0, public-route evidence)

- `encode_field_len` (config.c:1824) parses with `atoi`/`strtoll`
  ignoring `errno`/`endptr`, narrows by cast, and parses numeric/float/
  double through `atof` (double) with an unchecked int64 cast
  (config.c:2002-2010).
- `transform_field_value` (query_find.c:805-814) widens float→double by
  reinterpreting native-order stored float bytes as big-endian and
  emitting big-endian double bytes; decoders read native bytes.
- Output decoders render `%g` (6 significant digits).
- `parse_field_type` (config.c:1516-1522) drops numeric P and parses S
  with `atoi`; `validate_field_type_clean` (query_schema.c:1022) accepts
  any `numeric:` suffix unchecked; the `10^scale` loop overflows for
  S > 18 (config.c:1577-1584).
- `parse_numeric_i64` (query_plan.c:81) is the same `atof`-and-cast
  formula as the old encoder — write/query parity exists today only
  because both round through double identically. Phase 1 must move both
  sides.

## Consumer inventory (complete, Phase 0-verified; dispositions)

`encode_field_len` / `encode_field` / `encode_field_checked` /
`conv_numeric_text` / `parse_numeric_i64` callers:

| Site | Kind of value | Phase 1 disposition |
| --- | --- | --- |
| config.c:2548, 2552, 2556 (`typed_encode`) | client JSON values | convert to checked |
| config.c:3127, 3131 (`typed_encode_defaults` values) | client JSON values | convert to checked |
| config.c:3173 (`typed_encode_defaults` literal defaults) | client-declared literal | convert to checked |
| query_bulk.c:1601 (`typed_encode_defaults`, bulk JSON insert) | client values | **no edit**: already passes `NULL, 0` and counts `-2` as a per-row error (`errors++; continue`) — the new `-2` sources flow through unchanged |
| query_bulk.c:2370, 2380 (bulk insert delimited) | client cells | convert to checked (per-row error) |
| query_bulk.c:4238 (bulk upd delim compute) | client cells | convert to checked (skip row) |
| query_bulk.c:4855 (bulk upd JSON compute) | client values | convert to checked (skip row) |
| storage.c:1488 (`update_compose_new_from_old`) | client update values | convert to checked |
| query_find.c:941 (rebuild literal-default backfill) | client-declared literal | convert to checked (fail rebuild) |
| query_plan.c:356, 357, 507 (`parse_numeric_i64`) | criterion literals | convert to exact (Task 3) — parity with writes |
| storage.c:1446, 1554; query_find.c:952, 971, 989 | internally generated (auto_create/auto_update stamps, seq, random, uuid) | keep void `encode_field` shim — generator output is valid by construction |
| index.c:1255, 1290, 1638, 1682; query.c:379, 8099; query_join.c:298; query_plan.c:3108 (`encode_field_for_index`, text→key) | criterion/join/rebuild literal keys | convert the in-scope scalar conversions to complete-span, scan-identical parsing (Task 3b: `strtoll`/`strtod`/`strtof` via `span_to_cstr`; numeric via `conv_numeric_text`); the stored-bytes path `typed_field_to_index_key` is already exact (config.c:2330-2338) and untouched |
| test_config_encode.c (13 sites) | valid literals | unchanged — none pin out-of-range conversions for the Phase 1 type set |
| query_plan.c:352-353, 371, 501 (int/short/byte criterion `strtoll`/`atoi`) | criterion literals | unchanged, with reasoning: every *writable* integer value round-trips exactly through `strtoll` (write rejects what strtoll would mangle), so no saved value can fail its lookup; strict criterion rejection arrives with the Phase 5 planner overhaul |

`update_compose_new_from_old` callers (signature gains err params):
storage.c:1921, query_bulk.c:3407, 4212, 4829. `typed_encode` callers:
tests only (test_not_null_writes.c:190, 192, 196); production inserts go
through `typed_encode_defaults`.

`numeric_scale_mult` consumers — the complete list, all dispositioned:
config.c:1580/1582 (compute — Task 4 fallback), config.c:2005/2267
(encode — superseded by the checked boundary), config.c:3296
(`decode_field_to_buf` — guarded), query_aggregate.c:1110
(`typed_field_to_buf_raw` — guarded), query_aggregate.c:1311
(`typed_field_to_double` — guarded), query_aggregate.c:1519
(`decode_idx_to_buf` — guarded), query_aggregate.c:2060
(`decode_index_key_to_double` — guarded), query_plan.c:356/357/507
(criterion — mult-free after Task 3), query_schema.c:769 (edit-field
carries the parsed value; downstream sites guarded).

## Task 1 — Checked conversion core (config.c)

First, extend the include block. Anchor: `#include "types.h"` at the top
of config.c (the first of the three local includes):

```c
#include "types.h"
#include "slotcask.h"
#include "bitmap.h"
#include <math.h>
#include <stdarg.h>
```

(adding the last two lines; `isfinite` and `va_list` are otherwise
undeclared — this was a compile error in the first revision).

Then insert the following complete block immediately **before** the
unique anchor:

```c
void encode_field_len(const TypedField *f, const char *val, size_t vlen,
                      uint8_t *out) {
```

```c
/* ---- Phase 1 checked conversion boundary ----
   Length-aware, full-input parsers with explicit syntax/range status and
   field-specific error text, shared by every value-carrying caller (see
   the inventory). The void encoders below remain as the legacy shim for
   internally generated values only. */

static int is_ascii_space(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

/* NUL-terminated copy of the complete span for lookup-key parsing: no
   truncating stack window may narrow what a criterion can say. Callers
   convert the copy with the same library call the scan-side criterion
   compiler uses, so indexed and scan routes agree on every literal
   writes accept. Returns NULL on OOM (callers then use 0 — the scan
   side cannot allocate either). */
static char *span_to_cstr(const char *val, size_t vlen) {
    char *s = malloc(vlen + 1);
    if (!s) return NULL;
    memcpy(s, val, vlen);
    s[vlen] = '\0';
    return s;
}

static void fmt_conv_err(char *err, size_t errsz, const TypedField *f,
                         const char *fmt, ...) {
    if (err && errsz > 0) {
        va_list ap;
        va_start(ap, fmt);
        int n = snprintf(err, errsz,
                         "value for field '%s' is not a valid %s: ",
                         f->name, typed_type_name(f->type));
        if (n > 0 && (size_t)n < errsz)
            vsnprintf(err + n, errsz - (size_t)n, fmt, ap);
        va_end(ap);
    }
}

/* Parse the whole span as optional-sign decimal digits (no fraction, no
   exponent, no other bytes). Leading zeros are valid at any length and
   do not count toward the magnitude budget — "0000000000000000000001"
   is 1. Only significant digits are bounded (≤19), with the sign-aware
   maximum. 0 = ok, -1 = syntax, -2 = int64 range. */
static int parse_full_i64(const char *val, size_t vlen, int64_t *out) {
    if (vlen == 0) return -1;
    size_t i = 0;
    int neg = 0;
    if (val[0] == '+' || val[0] == '-') { neg = (val[0] == '-'); i = 1; }
    if (i >= vlen) return -1;
    for (size_t k = i; k < vlen; k++)
        if (val[k] < '0' || val[k] > '9') return -1;
    while (i + 1 < vlen && val[i] == '0') i++;   /* leading zeros */
    size_t ndig = vlen - i;                      /* significant digits */
    const uint64_t maxmag = neg ? 9223372036854775808ull
                                : 9223372036854775807ull;
    if (ndig > 19) return -2;
    uint64_t mag = 0;
    for (; i < vlen; i++) {
        uint64_t d = (uint64_t)(val[i] - '0');
        if (mag > (maxmag - d) / 10) return -2;
        mag = mag * 10 + d;
    }
    /* mag <= 2^63; the negative path wraps 2^63 to its two's-complement
       int64 bit pattern (unsigned arithmetic is defined; the cast is
       implementation-defined and two's complement on all targets). */
    *out = neg ? (int64_t)(0u - mag) : (int64_t)mag;
    return 0;
}

static int conv_i16(const TypedField *f, const char *val, size_t vlen,
                    uint8_t *out, char *err, size_t errsz) {
    int64_t v;
    int rc = parse_full_i64(val, vlen, &v);
    if (rc == -1) {
        fmt_conv_err(err, errsz, f, "expected a whole decimal number");
        return -1;
    }
    if (rc == -2 || v < -32768 || v > 32767) {
        fmt_conv_err(err, errsz, f, "out of short range (-32768..32767)");
        return -1;
    }
    int16_t s = (int16_t)v;
    out[0] = (uint8_t)(((uint16_t)s >> 8) & 0xFF);
    out[1] = (uint8_t)s;
    return 0;
}

static int conv_i32(const TypedField *f, const char *val, size_t vlen,
                    uint8_t *out, char *err, size_t errsz) {
    int64_t v;
    int rc = parse_full_i64(val, vlen, &v);
    if (rc == -1) {
        fmt_conv_err(err, errsz, f, "expected a whole decimal number");
        return -1;
    }
    if (rc == -2 || v < -2147483648LL || v > 2147483647LL) {
        fmt_conv_err(err, errsz, f,
                     "out of int range (-2147483648..2147483647)");
        return -1;
    }
    int32_t s = (int32_t)v;
    out[0] = (uint8_t)(((uint32_t)s >> 24) & 0xFF);
    out[1] = (uint8_t)(((uint32_t)s >> 16) & 0xFF);
    out[2] = (uint8_t)(((uint32_t)s >> 8) & 0xFF);
    out[3] = (uint8_t)s;
    return 0;
}

static int conv_i64(const TypedField *f, const char *val, size_t vlen,
                    uint8_t *out, char *err, size_t errsz) {
    int64_t v;
    int rc = parse_full_i64(val, vlen, &v);
    if (rc == -1) {
        fmt_conv_err(err, errsz, f, "expected a whole decimal number");
        return -1;
    }
    if (rc == -2) {
        fmt_conv_err(err, errsz, f,
                     "out of signed 64-bit range (-9223372036854775808.."
                     "9223372036854775807)");
        return -1;
    }
    uint64_t u = (uint64_t)v;
    for (int b = 7; b >= 0; b--) { out[b] = (uint8_t)(u & 0xFF); u >>= 8; }
    return 0;
}

static int conv_u8(const TypedField *f, const char *val, size_t vlen,
                   uint8_t *out, char *err, size_t errsz) {
    int64_t v;
    int rc = parse_full_i64(val, vlen, &v);
    if (rc == -1) {
        fmt_conv_err(err, errsz, f, "expected a whole decimal number");
        return -1;
    }
    if (rc == -2 || v < 0 || v > 255) {
        fmt_conv_err(err, errsz, f, "out of byte range (0..255)");
        return -1;
    }
    out[0] = (uint8_t)v;
    return 0;
}

/* Finite binary floating parse via strtof/strtod (direct binary32 for
   float — no double round-trip). Rejects: junk suffixes, hex float
   syntax, non-finite spellings (inf/nan and overflow), and nonzero
   values that underflow to zero. Representable subnormals are accepted
   (ERANGE with a nonzero result). */
static int conv_f64(const TypedField *f, const char *val, size_t vlen,
                    uint8_t *out, char *err, size_t errsz) {
    char cbuf[64];
    if (vlen == 0 || vlen >= sizeof(cbuf)) {
        fmt_conv_err(err, errsz, f, "empty or over-long numeric text");
        return -1;
    }
    if (memchr(val, 'x', vlen) || memchr(val, 'X', vlen)) {
        fmt_conv_err(err, errsz, f, "hex float syntax is not accepted");
        return -1;
    }
    memcpy(cbuf, val, vlen);
    cbuf[vlen] = '\0';
    errno = 0;
    char *end = NULL;
    double v = strtod(cbuf, &end);
    if (end != cbuf + vlen) {
        fmt_conv_err(err, errsz, f, "trailing characters after number");
        return -1;
    }
    if (!isfinite(v)) {
        fmt_conv_err(err, errsz, f,
                     "only finite values are accepted (no inf/nan)");
        return -1;
    }
    if (errno == ERANGE && v == 0.0) {
        fmt_conv_err(err, errsz, f,
                     "nonzero value underflows to zero in double");
        return -1;
    }
    memcpy(out, &v, 8);
    return 0;
}

static int conv_f32(const TypedField *f, const char *val, size_t vlen,
                    uint8_t *out, char *err, size_t errsz) {
    char cbuf[64];
    if (vlen == 0 || vlen >= sizeof(cbuf)) {
        fmt_conv_err(err, errsz, f, "empty or over-long numeric text");
        return -1;
    }
    if (memchr(val, 'x', vlen) || memchr(val, 'X', vlen)) {
        fmt_conv_err(err, errsz, f, "hex float syntax is not accepted");
        return -1;
    }
    memcpy(cbuf, val, vlen);
    cbuf[vlen] = '\0';
    errno = 0;
    char *end = NULL;
    float v = strtof(cbuf, &end);
    if (end != cbuf + vlen) {
        fmt_conv_err(err, errsz, f, "trailing characters after number");
        return -1;
    }
    if (!isfinite((double)v)) {
        fmt_conv_err(err, errsz, f,
                     "only finite values are accepted (no inf/nan)");
        return -1;
    }
    if (errno == ERANGE && v == 0.0f) {
        fmt_conv_err(err, errsz, f,
                     "nonzero value underflows to zero in float");
        return -1;
    }
    memcpy(out, &v, 4);
    return 0;
}

/* Exact decimal text -> scaled int64 (value × 10^scale), ties away from
   zero, no double anywhere. Non-static: shared with the query_plan
   criterion compiler so a saved value always matches its own literal
   (write/query parity). 0 = ok, -1 = invalid syntax/unsupported field
   scale, -2 = out of range for the field's int64 scaled storage.
   Scale 0 is valid (mult 1). The magnitude bound is sign-aware:
   negatives may reach 2^63 (INT64_MIN). */
int conv_numeric_text(const TypedField *f, const char *val, size_t vlen,
                      int64_t *out, char *err, size_t errsz) {
    if (f->numeric_scale < 0 || f->numeric_scale > 18 ||
        f->numeric_scale_mult <= 0) {
        fmt_conv_err(err, errsz, f,
                     "field has an unsupported legacy scale; the object "
                     "needs a scale edit before writes");
        return -1;
    }
    /* Write/query/index literal parity: strip one outer ASCII
       whitespace run exactly as the write boundary does, so criterion
       text like " 0.145" matches a value written as "0.145". */
    while (vlen > 0 && is_ascii_space(val[0])) { val++; vlen--; }
    while (vlen > 0 && is_ascii_space(val[vlen - 1])) vlen--;
    size_t i = 0;
    int neg = 0;
    if (vlen && (val[0] == '+' || val[0] == '-')) { neg = (val[0]=='-'); i = 1; }
    size_t int_start = i;
    while (i < vlen && val[i] >= '0' && val[i] <= '9') i++;
    size_t int_len = i - int_start;
    size_t frac_len = 0;
    if (i < vlen && val[i] == '.') {
        i++;
        size_t fs = i;
        while (i < vlen && val[i] >= '0' && val[i] <= '9') i++;
        frac_len = i - fs;
    }
    long expv = 0;
    if (i < vlen && (val[i] == 'e' || val[i] == 'E')) {
        i++;
        int eneg = 0;
        if (i < vlen && (val[i] == '+' || val[i] == '-')) {
            eneg = (val[i] == '-'); i++;
        }
        size_t es = i;
        while (i < vlen && val[i] >= '0' && val[i] <= '9') i++;
        size_t el = i - es;
        if (el == 0 || el > 6) {
            fmt_conv_err(err, errsz, f, "exponent out of supported range");
            return -1;
        }
        for (size_t k = 0; k < el; k++) expv = expv * 10 + (val[es+k]-'0');
        if (eneg) expv = -expv;
        if (expv > 400000 || expv < -400000) {
            fmt_conv_err(err, errsz, f, "exponent out of supported range");
            return -1;
        }
    }
    if (i != vlen || (int_len + frac_len) == 0) {
        fmt_conv_err(err, errsz, f, "expected a decimal number");
        return -1;
    }
    if (memchr(val, 0, vlen)) {
        fmt_conv_err(err, errsz, f, "embedded NUL byte");
        return -1;
    }
    const uint64_t maxmag = neg ? 9223372036854775808ull      /* INT64_MIN */
                                : 9223372036854775807ull;
    const char *digits_int = val + int_start;
    const char *digits_frac = val + int_start + int_len + 1; /* past '.' */
    size_t total = int_len + frac_len;
    /* All-zero digits: the value is 0 whatever the exponent says (0e19
       is a valid zero), and leading zeros do not count toward the
       magnitude budget (0000000000000000000001 is 1). */
    size_t lz = 0;
    while (lz < total && (lz < int_len ? digits_int[lz]
                                       : digits_frac[lz - int_len]) == '0')
        lz++;
    if (lz == total) { *out = 0; return 0; }
    size_t sig = total - lz;
    /* stored = ±D × 10^te, te = scale - frac_len + exp; round the
       dropped tail half-away-from-zero. The round-up test is safe only
       when at least the dropped run's leading position exists in D:
       for total < k_drop the whole value is below half of the last kept
       place and rounds to zero (6e-100 at scale 2 → 0, not 0.01). */
    long te = (long)f->numeric_scale - (long)frac_len + expv;
    if (te >= 0) {
        if (sig > 19 || te > 18) {
            fmt_conv_err(err, errsz, f,
                         "out of range for numeric scale %d (stored as a "
                         "64-bit scaled integer)", f->numeric_scale);
            return -2;
        }
        uint64_t mag = 0;
        for (size_t k = 0; k < total; k++) {
            char c = (k < int_len) ? digits_int[k] : digits_frac[k - int_len];
            uint64_t d = (uint64_t)(c - '0');
            if (mag > (maxmag - d) / 10) {
                fmt_conv_err(err, errsz, f,
                             "out of range for numeric scale %d (stored as "
                             "a 64-bit scaled integer)", f->numeric_scale);
                return -2;
            }
            mag = mag * 10 + d;
        }
        for (long k = 0; k < te; k++) {
            if (mag > maxmag / 10) {
                fmt_conv_err(err, errsz, f,
                             "out of range for numeric scale %d",
                             f->numeric_scale);
                return -2;
            }
            mag *= 10;
        }
        *out = neg ? (int64_t)(0u - mag) : (int64_t)mag;
        return 0;
    }
    long k_drop = -te;   /* digit positions dropped by rounding */
    size_t q_len = (total > (size_t)k_drop) ? total - (size_t)k_drop : 0;
    uint64_t mag = 0;
    for (size_t k = 0; k < q_len; k++) {
        char c = (k < int_len) ? digits_int[k] : digits_frac[k - int_len];
        uint64_t d = (uint64_t)(c - '0');
        if (mag > (maxmag - d) / 10) {
            fmt_conv_err(err, errsz, f,
                         "out of range for numeric scale %d",
                         f->numeric_scale);
            return -2;
        }
        mag = mag * 10 + d;
    }
    int round_up = 0;
    if (total >= (size_t)k_drop) {
        size_t di = total - (size_t)k_drop;   /* first dropped digit */
        char c = (di < int_len) ? digits_int[di] : digits_frac[di - int_len];
        round_up = (c >= '5');   /* r >= 5*10^(k-1) ⟺ 2r ≥ 10^k */
    }
    if (round_up) {
        if (mag >= maxmag) {
            fmt_conv_err(err, errsz, f,
                         "out of range for numeric scale %d",
                         f->numeric_scale);
            return -2;
        }
        mag++;
    }
    *out = neg ? (int64_t)(0u - mag) : (int64_t)mag;
    return 0;
}

static int conv_numeric(const TypedField *f, const char *val, size_t vlen,
                        uint8_t *out, char *err, size_t errsz) {
    int64_t v;
    if (conv_numeric_text(f, val, vlen, &v, err, errsz) != 0) return -1;
    uint64_t u = (uint64_t)v;
    for (int b = 7; b >= 0; b--) { out[b] = (uint8_t)(u & 0xFF); u >>= 8; }
    return 0;
}

int encode_field_checked(const TypedField *f, const char *val, size_t vlen,
                         uint8_t *out, char *err, size_t errsz) {
    if (err && errsz > 0) err[0] = '\0';
    if (!val) vlen = 0;
    /* Task 0.2 grammar: strip one outer ASCII whitespace run for the
       scalar numeric types ONLY — varchar, enum and every other type is
       byte-exact (trimming would rewrite stored content and could select
       a different enum member). Embedded whitespace inside scalars is
       rejected by the parsers. */
    switch (f->type) {
    case FT_SHORT: case FT_INT: case FT_LONG: case FT_TIMESTAMP:
    case FT_BYTE: case FT_NUMERIC: case FT_DOUBLE: case FT_FLOAT:
        while (vlen > 0 && is_ascii_space(val[0])) { val++; vlen--; }
        while (vlen > 0 && is_ascii_space(val[vlen - 1])) vlen--;
        break;
    default:
        break;
    }
    if (vlen == 0) {
        /* Historic empty/NULL span semantics (zero-filled field); the
           Task 0.2 "empty string is an error for scalars" tightening
           lands with the Phase 7 grammar layer. */
        memset(out, 0, f->size);
        return 0;
    }
    switch (f->type) {
    case FT_SHORT:
        return conv_i16(f, val, vlen, out, err, errsz);
    case FT_INT:
        return conv_i32(f, val, vlen, out, err, errsz);
    case FT_LONG:
    case FT_TIMESTAMP:
        return conv_i64(f, val, vlen, out, err, errsz);
    case FT_BYTE:
        return conv_u8(f, val, vlen, out, err, errsz);
    case FT_NUMERIC:
        return conv_numeric(f, val, vlen, out, err, errsz);
    case FT_DOUBLE:
        return conv_f64(f, val, vlen, out, err, errsz);
    case FT_FLOAT:
        return conv_f32(f, val, vlen, out, err, errsz);
    case FT_VARCHAR:
        /* Defense in depth: the wire surfaces already reject overlength;
           the shim below still truncates for internal callers. */
        if ((int)vlen > f->size - 2) {
            fmt_conv_err(err, errsz, f,
                         "%zu bytes exceeds max %d", vlen, f->size - 2);
            return -1;
        }
        break;
    default:
        break;   /* bool/date/time/uuid/… keep Phase 7 semantics for now */
    }
    encode_field_len(f, val, vlen, out);
    return 0;
}

/* ---- End Phase 1 checked conversion boundary ---- */

```

`g_type_desc` is `static` to type_desc.c, so the name accessor is defined
there, not in config.c. In **src/db/type_desc.c**, immediately after the
existing accessor's closing brace (anchor
`const TypeDescriptor *type_desc(enum FieldType t) {` through
`return &g_type_desc[t];\n}`):

```c
const char *typed_type_name(enum FieldType t) {
    const TypeDescriptor *d = type_desc(t);
    return d ? d->name : "?";
}
```

Add the three exported declarations to types.h immediately after the
anchor `void encode_field(const TypedField *f, const char *val, uint8_t *out);`:

```c
/* Phase 1 checked conversion boundary: returns 0 on success, -1 on
   validation failure with a field-specific message in err (if non-NULL).
   `out` is only written on success. Outer ASCII whitespace is stripped
   for the scalar numeric types only; varchar/enum are byte-exact. */
int encode_field_checked(const TypedField *f, const char *val, size_t vlen,
                         uint8_t *out, char *err, size_t errsz);
/* Exact decimal text -> scaled int64 for the field's declared scale,
   ties away from zero; shared by the write path and the query_plan
   criterion compiler (write/query parity). 0 = ok, -1 = invalid,
   -2 = out of range. */
int conv_numeric_text(const TypedField *f, const char *val, size_t vlen,
                      int64_t *out, char *err, size_t errsz);
const char *typed_type_name(enum FieldType t);
```

(`typed_type_name` is defined in **src/db/type_desc.c** per the hunk
above — that is the only placement; config.c cannot see the file-static
`g_type_desc`.)

## Task 2 — Convert the value-carrying callers

### 2a. typed_encode (config.c)

Change the signature and prologue. Anchor:
`int typed_encode(const TypedSchema *ts, const char *json, uint8_t *out, int out_size) {`
followed by `    if (!ts || !ts->typed || out_size < ts->total_size) return -1;`
and `    memset(out, 0, ts->total_size);` — replace those three lines with:

```c
int typed_encode(const TypedSchema *ts, const char *json, uint8_t *out,
                 int out_size, char *err_buf, size_t err_buf_size) {
    if (!ts || !ts->typed || out_size < ts->total_size) return -1;
    memset(out, 0, ts->total_size);
    if (err_buf && err_buf_size > 0) err_buf[0] = '\0';
```

Convert the three encode sites inside typed_encode to checked calls that
`return -2` on failure (message already in err_buf — the same contract as
the function's existing not_null/sentinel `-2`):

Site 1 — the varchar unescape success path (anchor `encode_field_len(&ts->fields[i], unesc, unesc_len,` followed by `` out + ts->nullmap_prefix + ts->fields[i].offset);`` and `free(unesc);`):

```c
                        if (encode_field_checked(&ts->fields[i], unesc, unesc_len,
                                                  out + ts->nullmap_prefix + ts->fields[i].offset,
                                                  err_buf, err_buf_size) != 0) {
                            free(unesc);
                            return -2;
                        }
                        free(unesc);
```

Site 2 — the varchar unescape-failure fallback (anchor `encode_field_len(&ts->fields[i], ev, el,` in the `} else {` branch):

```c
                        if (encode_field_checked(&ts->fields[i], ev, el,
                                                  out + ts->nullmap_prefix + ts->fields[i].offset,
                                                  err_buf, err_buf_size) != 0)
                            return -2;
```

Site 3 — the non-varchar else branch immediately after (anchor `encode_field_len(&ts->fields[i], ev, el,` in the `} else {` after the varchar block's closing brace):

```c
                } else {
                    if (encode_field_checked(&ts->fields[i], ev, el,
                                              out + ts->nullmap_prefix + ts->fields[i].offset,
                                              err_buf, err_buf_size) != 0)
                        return -2;
                }
```

Update the declaration (types.h:957, anchor `int typed_encode(const TypedSchema *ts, const char *json, uint8_t *out, int out_size);`):

```c
int typed_encode(const TypedSchema *ts, const char *json, uint8_t *out,
                 int out_size, char *err_buf, size_t err_buf_size);
```

and the three test call sites in test_not_null_writes.c — lines 190, 192,
196, each currently `typed_encode(&tsu, …, out, sizeof(out))` — append
`, NULL, 0` to the argument list, keeping the expected `-2` results and
their assertion messages unchanged.

### 2b. typed_encode_defaults (config.c)

Site 1 — varchar unescape success path (anchor `encode_field_len(&ts->fields[i], unesc, unesc_len,` at the block ending `free(unesc);`):

```c
                        if (encode_field_checked(&ts->fields[i], unesc, unesc_len,
                                                  out + ts->nullmap_prefix + ts->fields[i].offset,
                                                  err_buf, err_buf_size) != 0) {
                            free(unesc);
                            return -2;
                        }
                        free(unesc);
```

Site 2 — non-varchar else branch (anchor `encode_field_len(&ts->fields[i], ev, el,` before `}` and `seen[i] = 1;`):

```c
                        if (encode_field_checked(&ts->fields[i], ev, el,
                                                  out + ts->nullmap_prefix + ts->fields[i].offset,
                                                  err_buf, err_buf_size) != 0)
                            return -2;
```

Site 3 — the literal-default in the defaults pass (anchor `encode_field(&ts->fields[i], dv,` before `out + ts->nullmap_prefix + ts->fields[i].offset);` at the function tail):

```c
        if (encode_field_checked(&ts->fields[i], dv, strlen(dv),
                                  out + ts->nullmap_prefix + ts->fields[i].offset,
                                  err_buf, err_buf_size) != 0)
            return -2;
```

(Generated defaults — seq/uuid/random/auto_* — also arrive as `dv`, but
those spellings come from `generate_default` and always parse; a generator
output that fails the checked parser is a bug and refusing the write is
the honest outcome, matching the existing "default generator failed"
`-2` contract directly above. The bulk JSON insert path — query_bulk.c:1601
— already calls `typed_encode_defaults(..., NULL, 0)` and counts `-2` as
a per-row error (`errors++; continue`), so the new rejection flows through
with no further edit there.)

### 2c. update_compose_new_from_old (storage.c) + all four callers

Change the definition's signature (storage.c:1454 anchor
`int update_compose_new_from_old(const TypedSchema *ts,` through
`const uint8_t *field_null) {`) to — note the definition ends with
`) {`, not `);`:

```c
int update_compose_new_from_old(const TypedSchema *ts,
                                const SlotcaskOldRecord *old,
                                uint8_t *out_buf,
                                char *const *field_vals,
                                const uint8_t *field_null,
                                char *err, size_t errsz) {
```

The separate declaration in slotcask.h:474 gets the identical parameter
list but ends with `char *err, size_t errsz);`.
Convert the client-value site (anchor
`encode_field(f, field_vals[i], dst);   /* bit already clear */`):

```c
            if (encode_field_checked(f, field_vals[i], strlen(field_vals[i]),
                                     dst, err, errsz) != 0)
                return -2;   /* field-specific message already in err */
```

Update all four call sites. storage.c:1921 (anchor
`if (update_compose_new_from_old(c->idx_ts, old, out_value, values, nulls) != 0) {`
plus its two-line body through the closing `}`):

```c
    int compose_rc = update_compose_new_from_old(c->idx_ts, old, out_value,
                                                 values, nulls,
                                                 c->err_buf, sizeof(c->err_buf));
    if (compose_rc == -2)
        goto reject;                     /* validation message in err_buf */
    if (compose_rc != 0) {
        snprintf(c->err_buf, sizeof(c->err_buf), "corrupt record header on update target");
        goto reject;
    }
```

query_bulk.c:3407 (anchor
`if (update_compose_new_from_old(w->ts, old, new_buf,\n                                    ctx->field_vals, ctx->field_null) != 0)`):

```c
    if (update_compose_new_from_old(w->ts, old, new_buf,
                                    ctx->field_vals, ctx->field_null,
                                    NULL, 0) != 0)
        return -1;
```

query_bulk.c:4212 and :4829 (anchors
`if (update_compose_new_from_old(w->ts, old, new_buf, NULL, NULL) != 0)` —
two occurrences): append `, NULL, 0`:

```c
    if (update_compose_new_from_old(w->ts, old, new_buf, NULL, NULL,
                                    NULL, 0) != 0)
        return -1;
```

(bulk value-compute `-1` is the existing per-record skip; no response
shape change.)

### 2d. Bulk insert delimited (query_bulk.c:2364-2392)

Replace both inner encode calls (anchors
`encode_field_len(&ts->fields[i], vals[i].ptr, vals[i].len,` and
`encode_field_len(&ts->fields[fi], vals[i].ptr, vals[i].len,` — each a
two-line call ending at `ts->fields[i].offset);` / `ts->fields[fi].offset);`)
with checked calls that reuse the existing per-row reject flag:

```c
                if (encode_field_checked(&ts->fields[i], vals[i].ptr, vals[i].len,
                                          payload + ts->nullmap_prefix +
                                          ts->fields[i].offset,
                                          NULL, 0) != 0) { row_overflow = 1; break; }
```

```c
                if (encode_field_checked(&ts->fields[fi], vals[i].ptr, vals[i].len,
                                          payload + ts->nullmap_prefix +
                                          ts->fields[fi].offset,
                                          NULL, 0) != 0) { row_overflow = 1; break; }
```

(The `row_overflow` flag already drives `errors++; continue` — the
documented per-row skip contract. The varchar pre-checks immediately above
each site stay: redundant but harmless; removing them is out of scope.)

### 2e. Bulk update computes (query_bulk.c:4238, 4855)

Anchor `encode_field(&w->ts->fields[tidx], cellbuf,` (two-line call):

```c
                if (encode_field_checked(&w->ts->fields[tidx], cellbuf,
                                          strlen(cellbuf),
                                          new_buf + w->ts->nullmap_prefix +
                                          w->ts->fields[tidx].offset,
                                          NULL, 0) != 0) {
                    if (cellbuf != tmpcell) free(cellbuf);
                    return -1;   /* per-record skip, existing tally */
                }
```

Anchor `encode_field(&w->ts->fields[tidx], json_rec->field_values[i],`:

```c
            if (encode_field_checked(&w->ts->fields[tidx], json_rec->field_values[i],
                                      strlen(json_rec->field_values[i]),
                                      new_buf + w->ts->nullmap_prefix +
                                      w->ts->fields[tidx].offset,
                                      NULL, 0) != 0)
                return -1;   /* per-record skip, existing tally */
```

### 2f. Rebuild literal-default backfill (query_find.c:941)

Anchor `encode_field(nf, nf->default_val, buf + ctx->new_ts->nullmap_prefix + nf->offset);`:

```c
                if (encode_field_checked(nf, nf->default_val,
                                          strlen(nf->default_val),
                                          buf + ctx->new_ts->nullmap_prefix + nf->offset,
                                          NULL, 0) != 0) {
                    LOG_ERROR(LOG_SUB_CONFIG,
                              "rebuild_v2: declared default for field [%s] "
                              "is not a valid %s value; refusing rebuild",
                              nf->name, typed_type_name(nf->type));
                    free(buf);
                    ctx->error = 1;
                    return 1;   /* rebuild aborts; old object stays live */
                }
```

The neighboring DK_SEQ/DK_RANDOM/DK_UUID backfill sites (952, 971, 989)
stay on the void shim — generated values.

## Task 3 — Criterion literal parity (query_plan.c)

Without this task, exact writes break equality lookups: `0.145` at scale 2
stores exactly `15` (= 0.15) while `parse_numeric_i64`'s double formula
yields 14 for the same literal. Replace the function at the anchor
`static int64_t parse_numeric_i64(const char *s, int64_t mul) {` plus its
two-line body:

```c
/* Exact decimal literal → int64 scaled by the field's declared scale,
   shared with the write-path encoder (conv_numeric_text) so a saved
   value always matches its own literal — including the rounding of
   excess fractional digits. Invalid or out-of-range literals evaluate
   to 0, the pre-Phase-1 junk behavior; criterion literals becoming hard
   query errors is the Phase 5 planner overhaul. */
static int64_t parse_numeric_i64(const TypedField *tf, const char *s) {
    int64_t v = 0;
    if (!tf || !s) return 0;
    if (conv_numeric_text(tf, s, strlen(s), &v, NULL, 0) != 0) return 0;
    return v;
}
```

Update the three call sites, all of which have `cc->tf` in scope:

- query_plan.c:356-357 (anchor `cc->i1 = parse_numeric_i64(c->value, cc->tf->numeric_scale_mult);` and the `i2` line):

```c
        cc->i1 = parse_numeric_i64(cc->tf, c->value);
        cc->i2 = parse_numeric_i64(cc->tf, c->value2);
```

- query_plan.c:507 (anchor `cc->in_i64[i] = parse_numeric_i64(c->in_values[i], cc->tf->numeric_scale_mult);`):

```c
                cc->in_i64[i] = parse_numeric_i64(cc->tf, c->in_values[i]);
```

Integer criterion compiles (352-353, 371, 501) stay as-is: every writable
integer value round-trips exactly through `strtoll`, so no saved value can
fail its lookup; strict rejection arrives with Phase 5. FT_DOUBLE criteria
(`atof`) stay as-is: writable float/double values compare identically.
`conv_numeric_text` strips outer whitespace itself, so criterion text like
`" 0.145"` matches a value written as `"0.145"` on every route.

### 3b. Textual index-key encoders (config.c `encode_field_for_index`)

Indexed lookups build keys from criterion text through
`encode_field_for_index` (query.c:379, 8099; query_join.c:298;
index.c:1255, 1290, 1638, 1682 — the rebuild-from-text builders render
stored values exactly and re-parse here, so exact render → exact parse
round-trips). Every scalar case converts through a small truncating
stack window — `cbuf[32]`/`cbuf[16]`/`cbuf[8]` for the integer family,
`cbuf[48]` for float/double — so a literal the write path accepts can
produce a different lookup key after truncation (review probes: a
20-char leading-zero int stored 1, lookup encoded 0; a 50-char
float/double literal stored 1e10, lookup dropped the exponent and
encoded ~1e-36). Rewrite all six scalar cases to parse the complete
span with `span_to_cstr` (Task 1) and the same library call the
scan-side criterion compiler uses, so the three routes agree on every
literal and on junk:

**Disambiguation for every hunk below:** the write encoder
`encode_field_len` contains near-identical `cbuf[N]` conversion pairs;
the anchors here therefore include the flip/emit line that only exists
in `encode_field_for_index`. Replace within `encode_field_for_index`
only.

FT_LONG / FT_TIMESTAMP — replace
(`char cbuf[32]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);` +
`int64_t v = strtoll(cbuf, NULL, 10);` +
`uint64_t u = (uint64_t)v ^ (1ULL << 63);`) with:

```c
        char *s = span_to_cstr(val, vlen);
        int64_t v = s ? strtoll(s, NULL, 10) : 0;
        free(s);
        uint64_t u = (uint64_t)v ^ (1ULL << 63);
```

FT_INT — replace (`char cbuf[16]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);` +
`int32_t v = (int32_t)atoi(cbuf);` +
`uint32_t u = (uint32_t)v ^ 0x80000000u;`) with:

```c
        char *s = span_to_cstr(val, vlen);
        int32_t v = (int32_t)(s ? strtoll(s, NULL, 10) : 0);
        free(s);
        uint32_t u = (uint32_t)v ^ 0x80000000u;
```

FT_SHORT — replace (`char cbuf[16]; ...` + `int16_t v = (int16_t)atoi(cbuf);` +
`uint16_t u = (uint16_t)v ^ 0x8000u;`) with:

```c
        char *s = span_to_cstr(val, vlen);
        int16_t v = (int16_t)(s ? strtoll(s, NULL, 10) : 0);
        free(s);
        uint16_t u = (uint16_t)v ^ 0x8000u;
```

FT_BYTE — replace (`char cbuf[8]; ...` + `out[0] = (uint8_t)atoi(cbuf);` +
`*out_len = 1;`) with:

```c
        char *s = span_to_cstr(val, vlen);
        out[0] = (uint8_t)(s ? strtoll(s, NULL, 10) : 0);
        free(s);
        *out_len = 1;
```

FT_DOUBLE — replace (`char cbuf[48]; ...` + `double v = atof(cbuf);` +
`uint64_t bits;` + `memcpy(&bits, &v, 8);`) with:

```c
        char *s = span_to_cstr(val, vlen);
        double v = s ? strtod(s, NULL) : 0.0;
        free(s);
        uint64_t bits;
        memcpy(&bits, &v, 8);
```

FT_FLOAT — replace (`char cbuf[48]; ...` + `float v = (float)atof(cbuf);` +
`uint32_t bits;` + `memcpy(&bits, &v, 4);`) with:

```c
        char *s = span_to_cstr(val, vlen);
        float v = s ? strtof(s, NULL) : 0.0f;
        free(s);
        uint32_t bits;
        memcpy(&bits, &v, 4);
```

All six keep their remaining flip/emit tails byte-identical. Junk and
range semantics are deliberately identical to the scan-side compiler
(`strtoll`/`strtod`/`strtof` over the full text, including the historic
junk→0 and saturate behaviors) — that consistency is the point; the
*write* side stays strict, and an invalid literal cannot be stored, so
scan and index still agree for every storable value. FT_NUMERIC keeps
the exact `conv_numeric_text` conversion below (it already handles the
complete span, whitespace and leading zeros):

```c
    case FT_NUMERIC: {
        /* Exact: the same conversion as the write path, so a saved
           value is always found by its own literal. Invalid or
           out-of-range literals evaluate to 0 — identical to the
           scan-side criterion compiler, so indexed and scan routes
           agree for junk input too (the historic atof behavior). */
        int64_t v;
        if (conv_numeric_text(f, val, vlen, &v, NULL, 0) != 0) v = 0;
        uint64_t u = (uint64_t)v ^ (1ULL << 63);
```

(keep the existing eight emit lines and `*out_len = 8; break;`).

### 3c. Float criterion compiles (query_plan.c)

The matcher casts the criterion to float (query_plan.c:997
`float qf1 = (float)cc->d1;`), so the criterion must be parsed exactly
or a double-rounding-sensitive literal stores as one binary32 and
compares as another. Anchor `cc->d1 = atof(c->value);` in the FT_FLOAT
case (the second atof pair, after `case FT_FLOAT:` — the FT_DOUBLE pair
above it stays) and replace:

```c
    case FT_FLOAT:
        cc->d1 = (double)strtof(c->value, NULL);
        cc->d2 = (double)strtof(c->value2, NULL);
        break;
```

Anchor `cc->in_f64[i] = atof(c->in_values[i]);` in the FT_FLOAT in-list
case and replace the whole case:

```c
        case FT_FLOAT:
            cc->in_f64 = malloc(sizeof(double) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_f64[i] = (double)strtof(c->in_values[i], NULL);
            break;
```

## Task 4 — Numeric scale validation and the mult fallback (T06)

In query_schema.c `validate_field_type_clean`, replace the anchor
`if (strncmp(type, "numeric:", 8) == 0) return 8;` with:

```c
    if (strncmp(type, "numeric:", 8) == 0) {
        /* Full-input P[,S] validation. P stays informational until the
           packed exact format lands; S must fit the int64 scaled storage:
           0..18 (10^18 is the largest power of ten in int64; S = 0 is
           valid). Legacy "numeric:N" without a comma keeps its
           historical scale-2 meaning. Junk suffixes are rejected, not
           atoi-prefixed. */
        const char *ps = type + 8;
        if (!(*ps >= '0' && *ps <= '9')) return 0;
        while (*ps >= '0' && *ps <= '9') ps++;
        if (*ps == ',') {
            ps++;
            if (!(*ps >= '0' && *ps <= '9')) return 0;
            while (*ps >= '0' && *ps <= '9') ps++;
        }
        if (*ps != '\0') return 0;
        const char *comma = strchr(type + 8, ',');
        long s = comma ? strtol(comma + 1, NULL, 10) : 2;
        if (s < 0 || s > 18) return 0;
        return 8;
    }
```

In config.c `parse_field_type`, make the multiplier computation safe for
already-existing roots declared with an out-of-range scale (anchor
`if (f->type == FT_NUMERIC) {` at the function tail, through the closing
`} else { f->numeric_scale_mult = 0; }`):

```c
    if (f->type == FT_NUMERIC) {
        if (f->numeric_scale >= 0 && f->numeric_scale <= 18) {
            int64_t mult = 1;
            for (int i = 0; i < f->numeric_scale; i++) mult *= 10;
            f->numeric_scale_mult = mult;
        } else {
            /* Pre-Phase-1 roots may declare S outside 0..18 (S was never
               validated). Those objects rendered wrapped garbage before;
               keep them loadable, but every consumer guards mult <= 0
               (raw-integer fallback) and conv_numeric_text rejects
               writes — never compute a wrapped multiplier again. */
            f->numeric_scale_mult = 0;
            LOG_WARN(LOG_SUB_CONFIG,
                     "numeric scale %d outside representable 0..18; "
                     "writes rejected, decoded as raw integer",
                     f->numeric_scale);
        }
    } else {
        f->numeric_scale_mult = 0;
    }
```

Guard **every** remaining integer-division consumer — an unguarded
`v / 0` is SIGFPE. Three sites get complete case-body replacements (the
replacements also fix the scale-zero `INT64_MIN` double-minus render —
see the header, issue 2); two double-division sites get small guards.
The 1110 replacement does **not** re-declare `scale` beyond what the
body below contains — the first revision's hunk duplicated the existing
declaration.

config.c `decode_field_to_buf` FT_NUMERIC — replace the whole case body
(from `case FT_NUMERIC: {` through its closing `}`; current body is the
unsigned-accumulate load, the `v == 0` short-circuit, and the signed
whole/frac split):

```c
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
```

query_aggregate.c:1110 `typed_field_to_buf_raw` FT_NUMERIC — replace the
whole case body (current body: `int64_t v = ld_be_i64(p);`, the
`v == 0` short-circuit, then the signed split):

```c
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        if (v == 0) { buf[0] = '0'; buf[1] = '\0'; return 1; }
        if (f->numeric_scale_mult <= 0)   /* unsupported legacy scale */
            return snprintf(buf, bufsz, "%lld", (long long)v);
        int64_t scale = f->numeric_scale_mult;
        uint64_t m = (uint64_t)v;
        int neg = v < 0;
        if (neg) m = 0u - m;   /* magnitude without negating INT64_MIN */
        uint64_t whole = m / (uint64_t)scale;
        uint64_t frac = m % (uint64_t)scale;
        if (frac == 0)
            return snprintf(buf, bufsz, "%s%llu", neg ? "-" : "",
                            (unsigned long long)whole);
        return snprintf(buf, bufsz, "%s%llu.%0*llu", neg ? "-" : "",
                        (unsigned long long)whole, f->numeric_scale,
                        (unsigned long long)frac);
    }
```

query_aggregate.c:1519 `decode_idx_to_buf` FT_NUMERIC — replace the whole
case body (current body: the `plen < 8` check, the explicit
byte-assembly load, `u ^ (1ULL << 63)`, the `v == 0` short-circuit, then
the signed split):

```c
    case FT_NUMERIC: {
        if (plen < 8) return 0;
        uint64_t u = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                     ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                     ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                     ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
        int64_t v = (int64_t)(u ^ (1ULL << 63));
        if (v == 0) { buf[0] = '0'; buf[1] = '\0'; return 1; }
        int64_t scale = f->numeric_scale_mult;
        if (scale <= 0)   /* unsupported legacy scale: raw integer fallback */
            return snprintf(buf, bufsz, "%lld", (long long)v);
        uint64_t m = (uint64_t)v;
        int neg = v < 0;
        if (neg) m = 0u - m;
        uint64_t whole = m / (uint64_t)scale;
        uint64_t frac = m % (uint64_t)scale;
        if (frac == 0)
            return snprintf(buf, bufsz, "%s%llu", neg ? "-" : "",
                            (unsigned long long)whole);
        return snprintf(buf, bufsz, "%s%llu.%0*llu", neg ? "-" : "",
                        (unsigned long long)whole, f->numeric_scale,
                        (unsigned long long)frac);
    }
```

query_aggregate.c:1311 `decode_index_key_to_double` — the division pair
is inside the function that starts
`static AggValueStatus decode_index_key_to_double(const TypedField *f,`
(query_aggregate.c:1211). Its numeric case reads anchor
`*out = (double)v / (double)f->numeric_scale_mult;` followed by
`return AGG_VALUE_PRESENT;` — replace with:

```c
        *out = (f->numeric_scale_mult > 0)
               ? (double)v / (double)f->numeric_scale_mult
               : (double)v;   /* unsupported legacy scale: raw integer */
        return AGG_VALUE_PRESENT;
```

query_aggregate.c:2060 `typed_field_to_double` — inside the function
that starts `static AggValueStatus typed_field_to_double(const TypedField *f, const uint8_t *p,`
(query_aggregate.c:2034). Its exact numeric-case anchor is the single line
`*out = (double)v / (double)f->numeric_scale_mult; return AGG_VALUE_PRESENT;`
(the index helper has two lines). Replace only that line within
`typed_field_to_double`:

```c
        *out = (f->numeric_scale_mult > 0)
               ? (double)v / (double)f->numeric_scale_mult
               : (double)v;   /* unsupported legacy scale: raw integer */
        return AGG_VALUE_PRESENT;
```

(The two double-division sites cannot trap, but get the same fallback so
every consumer renders identically. query_plan.c becomes mult-free via
Task 3; query_schema.c:769 merely carries the parsed value into the
edited schema, whose consumers are the guarded sites.)

## Task 5 — Float→double widening byte-order fix (query_find.c)

Replace the FT_FLOAT widen case. Anchor (inside
`transform_field_value`, from `case FT_FLOAT: {` through the case's
closing `}` — current body: a size-4 branch that reads the four source
bytes big-endian into a float, widens, emits big-endian double bytes,
then a memcpy fallback and `return;`):

```c
    case FT_FLOAT: {
        /* float → double widen. Other float edits not supported. */
        if (new_f->type == FT_DOUBLE && new_f->size == 8) {
            /* Stored float bytes are native-order (written by memcpy in
               encode_field); widen with memcpy in both directions so the
               exact stored binary32 becomes its exact binary64. The old
               big-endian reinterpretation destroyed every widened value
               (Phase 0: 3.25 → 5.34788e-318 on the edit-field route). */
            float fv;
            memcpy(&fv, src, 4);
            double dv = (double)fv;
            memcpy(dst, &dv, 8);
            return;
        }
        memcpy(dst, src, old_f->size < new_f->size ? old_f->size : new_f->size);
        return;
    }
```

(Read the exact current case body at execution time and replace from
`case FT_FLOAT: {` to its closing `}`; keep the surrounding switch
byte-identical otherwise. If the case body differs from this shape,
PLAN_NOTES.md and halt.)

## Task 6 — Round-trip float output

config.c `decode_field_to_buf`: replace `%g` in the FT_DOUBLE case (anchor
`return snprintf(buf, buflen, "%g", v);`):

```c
        /* 17 significant digits: every double round-trips exactly. */
        return snprintf(buf, buflen, "%.17g", v);
```

and FT_FLOAT (anchor `return snprintf(buf, buflen, "%g", (double)v);`):

```c
        /* 9 significant digits: every float round-trips exactly, parsed
           directly as binary32. */
        return snprintf(buf, buflen, "%.9g", (double)v);
```

Both aggregate text renderers must follow the same contract. In
`query_aggregate.c`, apply the following replacements separately within
`typed_field_to_buf_raw` and `decode_idx_to_buf` (two occurrences per
anchor across the file; constrain each replacement to its named function).
The FT_DOUBLE anchor `return snprintf(buf, bufsz, "%g", v);` becomes:

```c
        return snprintf(buf, bufsz, "%.17g", v);
```

The FT_FLOAT anchor `return snprintf(buf, bufsz, "%g", (double)v);` becomes:

```c
        return snprintf(buf, bufsz, "%.9g", (double)v);
```

Test first: the Task 7 aggregate group-label assertions fail on the
unchanged renderers through both scan and index paths. Run the direct
helper regression as well: it checks both numeric text helpers at
INT64_MIN/scale 0 and all four aggregate legacy-scale fallback consumers.

`typed_get_field_str` delegates to `decode_field_to_buf` for both types
(config.c:3801) and needs no change. Output-text compat: values like
`3.14` now render `3.1400000000000001` (the exact binary value); the
fixed 9/17-digit contract is the parent's frozen choice. Phase 0 grep
found no test pinning rendered float text.

## Task 7 — Regression test case

New file `src/test/cases/test_scalar_conversion_safety.c` (complete; was
extracted and passed `cc -fsyntax-only -I. -Isrc/db -Isrc/test` against
the real headers in this revision):

```c
/* Phase 1 regression: checked scalar conversion boundary on public
   routes, criterion/write numeric parity, and widening. Red on the base
   branch (wraps/saturation/clipping/mismatch observed), green after the
   fix. Complements (does not modify) the Phase 7 calendar/bool/uuid
   cases and the unit-level test-config-encode. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *req(TestClient *tc, const char *json) {
    char *resp = NULL;
    if (tc_request(tc, json, &resp) != 0) return NULL;
    return resp;
}

static int test_scalar_conversion_safety_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *r = req(tc, "{\"mode\":\"add-dir\",\"dir\":\"conv\"}"); free(r);
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"t\",\"splits\":8,\"max_key\":64,\"fields\":["
        "\"s:short\",\"i:int\",\"l:long\",\"b:byte\",\"n:numeric:26,10\","
        "\"r:numeric:12,2\",\"z:numeric:10,0\",\"f:double\",\"g:float\","
        "\"v:varchar:32\"]}");
    ASSERT_NOT_NULL(r, "create responds");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "create ok");
    free(r);

    /* Rejections: each is refused with a specific message and creates
       no record. */
    struct { const char *key, *field, *val, *err_frag; } bad[] = {
        {"k1","s","32768","out of short range"},
        {"k2","i","2147483648","out of int range"},
        {"k3","i","123abc","expected a whole decimal number"},
        {"k4","l","9223372036854775808","out of signed 64-bit range"},
        {"k5","b","256","out of byte range"},
        {"k6","b","-1","out of byte range"},
        {"k7","n","1234567890123456.1234567890",
         "out of range for numeric scale 10"},
        {"k8","f","1e999","only finite values are accepted"},
        {"k9","f","1e-999","underflows to zero"},
        {"k10","g","nan","only finite values are accepted"},
        {"k11","i","1.5","expected a whole decimal number"},
    };
    for (size_t k = 0; k < sizeof(bad)/sizeof(bad[0]); k++) {
        char jq[512];
        snprintf(jq, sizeof(jq),
                 "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
                 "\"key\":\"%s\",\"value\":{\"%s\":\"%s\"}}",
                 bad[k].key, bad[k].field, bad[k].val);
        char *resp = req(tc, jq);
        ASSERT_NOT_NULL(resp, "reject insert responds");
        ASSERT_TRUE(strstr(resp, "\"error\"") != NULL, "insert rejected");
        ASSERT_TRUE(strstr(resp, bad[k].err_frag) != NULL, bad[k].err_frag);
        free(resp);
        snprintf(jq, sizeof(jq),
                 "{\"mode\":\"exists\",\"dir\":\"conv\",\"object\":\"t\","
                 "\"key\":\"%s\"}", bad[k].key);
        resp = req(tc, jq);
        ASSERT_NOT_NULL(resp, "exists responds");
        ASSERT_TRUE(strncmp(resp, "false", 5) == 0, "rejected record absent");
        free(resp);
    }

    /* Rejection preserves the old record. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"keep\",\"value\":{\"i\":\"7\",\"s\":\"1\",\"l\":\"1\","
        "\"b\":\"1\",\"n\":\"1.50\",\"f\":\"1.5\",\"g\":\"1.5\",\"v\":\"abc\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "seed insert");
    free(r);
    r = req(tc, "{\"mode\":\"update\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"keep\",\"value\":{\"i\":\"2147483648\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") != NULL, "update rejected");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"keep\"}");
    ASSERT_TRUE(r && strstr(r, "\"i\":7") != NULL, "old value intact");
    ASSERT_TRUE(r && strstr(r, "\"v\":\"abc\"") != NULL, "varchar intact");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\"}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "count is 1");
    free(r);

    /* Exact boundaries: INT64_MIN long, SHORT_MIN, scale-0 numeric
       INT64_MIN, scalar outer-whitespace trimming, varchar
       byte-exactness. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"ok\",\"value\":{\"l\":\"-9223372036854775808\","
        "\"s\":\"-32768\",\"i\":\" 42 \",\"z\":\"-9223372036854775808\","
        "\"v\":\"  padded  \"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "boundary insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"ok\"}");
    ASSERT_TRUE(r && strstr(r, "\"l\":-9223372036854775808") != NULL,
                "INT64_MIN long round trips");
    ASSERT_TRUE(r && strstr(r, "\"s\":-32768") != NULL, "SHORT_MIN round trips");
    ASSERT_TRUE(r && strstr(r, "\"i\":42") != NULL, "scalar span trimmed");
    ASSERT_TRUE(r && strstr(r, "\"z\":-9223372036854775808") != NULL,
                "scale-0 numeric INT64_MIN round trips");
    ASSERT_TRUE(r && strstr(r, "\"v\":\"  padded  \"") != NULL,
                "varchar bytes exact (spaces kept)");
    free(r);
    /* Scale-0 and near-zero numerics are findable by their exact value. */
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"z\",\"op\":\"eq\","
        "\"value\":\"-9223372036854775808\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "scale-0 INT64_MIN eq finds");
    free(r);

    /* Numeric rounding + write/query parity: 0.145 at scale 2 stores
       0.15 (ties away from zero — the old double path stored 14); both
       the rounded and the original literal must find the row. 6e-100
       rounds to 0.00, not 0.01. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"rnd\",\"value\":{\"r\":\"0.145\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "rounding insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"rnd\"}");
    ASSERT_TRUE(r && strstr(r, "\"r\":0.15") != NULL,
                "0.145 stored as 0.15 (ties away)");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"r\",\"op\":\"eq\",\"value\":\"0.145\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "eq 0.145 finds (parity)");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"r\",\"op\":\"eq\",\"value\":\"0.15\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "eq 0.15 finds (parity)");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"tiny\",\"value\":{\"r\":\"6e-100\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "tiny insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"tiny\"}");
    ASSERT_TRUE(r && strstr(r, "\"r\":0") != NULL &&
                strstr(r, "\"r\":0.01") == NULL,
                "6e-100 rounds to 0.00, not 0.01");
    free(r);
    /* Zero-with-exponent and leading-zero forms are valid numbers. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"ze\",\"value\":{\"r\":\"0e19\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "0e19 insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"ze\"}");
    ASSERT_TRUE(r && strstr(r, "\"r\":0") != NULL &&
                strstr(r, "\"r\":0.01") == NULL, "0e19 stored as 0");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"leadz\",\"value\":{\"r\":\"001.50\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "leading-zero insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"r\",\"op\":\"eq\",\"value\":\"1.5\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "eq 1.5 finds 001.50 (parity)");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"r\",\"op\":\"eq\",\"value\":\"0\"}]}");
    ASSERT_TRUE(r && strncmp(r, "2", 1) == 0, "eq 0 finds tiny + 0e19");
    free(r);

    /* Indexed/unindexed parity for a validated numeric write. */
    r = req(tc, "{\"mode\":\"add-index\",\"dir\":\"conv\",\"object\":\"t\","
        "\"field\":\"n\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "add-index");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"idx\",\"value\":{\"n\":\"1.23456789012345678901\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "indexed insert");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"idx\"}");
    ASSERT_TRUE(r && strstr(r, "\"n\":1.2345678901") != NULL,
                "numeric rounded to declared scale");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"n\",\"op\":\"eq\","
        "\"value\":\"1.2345678901\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "indexed eq finds");
    free(r);

    /* Indexed/scan literal parity (review R3-01): every accepted
       spelling must find its own row by scan (before add-index) and by
       index (after). */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"lz\",\"value\":{\"i\":\"00000000000000000001\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "leading-zero int insert");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"lzl\",\"value\":{\"l\":\"0000000000000000000001\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "leading-zero long insert");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"bigf\",\"value\":{\"f\":"
        "\"1.000000000000000000000000000000000000000000000000e10\",\"g\":"
        "\"1.000000000000000000000000000000000000000000000000e10\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "long float literal insert");
    free(r);
    /* scan side — i, l, f, g have no indexes yet */
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"i\",\"op\":\"eq\","
        "\"value\":\"00000000000000000001\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "scan finds leading-zero int");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"l\",\"op\":\"eq\","
        "\"value\":\"0000000000000000000001\"}]}");
    /* the seed row keep also holds l=1, so the count is 2 */
    ASSERT_TRUE(r && strncmp(r, "2", 1) == 0, "scan finds leading-zero long");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"f\",\"op\":\"eq\",\"value\":"
        "\"1.000000000000000000000000000000000000000000000000e10\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "scan finds long double literal");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"g\",\"op\":\"eq\",\"value\":"
        "\"1.000000000000000000000000000000000000000000000000e10\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "scan finds long float literal");
    free(r);
    /* index side — the add-index build itself must encode these rows
       findably, and subsequent writes must maintain them */
    r = req(tc, "{\"mode\":\"add-index\",\"dir\":\"conv\",\"object\":\"t\","
        "\"field\":\"i\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "add-index i"); free(r);
    r = req(tc, "{\"mode\":\"add-index\",\"dir\":\"conv\",\"object\":\"t\","
        "\"field\":\"l\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "add-index l"); free(r);
    r = req(tc, "{\"mode\":\"add-index\",\"dir\":\"conv\",\"object\":\"t\","
        "\"field\":\"f\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "add-index f"); free(r);
    r = req(tc, "{\"mode\":\"add-index\",\"dir\":\"conv\",\"object\":\"t\","
        "\"field\":\"g\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "add-index g"); free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"i\",\"op\":\"eq\","
        "\"value\":\"00000000000000000001\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "index finds leading-zero int");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"l\",\"op\":\"eq\","
        "\"value\":\"0000000000000000000001\"}]}");
    ASSERT_TRUE(r && strncmp(r, "2", 1) == 0, "index finds leading-zero long");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"f\",\"op\":\"eq\",\"value\":"
        "\"1.000000000000000000000000000000000000000000000000e10\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "index finds long double literal");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"g\",\"op\":\"eq\",\"value\":"
        "\"1.000000000000000000000000000000000000000000000000e10\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "index finds long float literal");
    free(r);
    /* binary32 double-rounding literal: stores and looks up identically
       only when every route parses with strtof. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"dr\",\"value\":{\"g\":\"1.0000000596046448\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "rounding-literal insert");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"g\",\"op\":\"eq\","
        "\"value\":\"1.0000000596046448\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0,
                "index finds double-rounding literal");
    free(r);

    /* Bulk insert: mixed valid/invalid rows keep the per-row contract
       (wire field `records`, response carries `"errors":N`). */
    r = req(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"records\":[{\"key\":\"bok\",\"value\":{\"i\":\"5\"}},"
        "{\"key\":\"bbad\",\"value\":{\"i\":\"2147483648\"}}]}");
    ASSERT_NOT_NULL(r, "bulk insert responds");
    ASSERT_TRUE(r && strstr(r, "\"errors\":1") != NULL,
                "bulk error tallied");
    free(r);
    r = req(tc, "{\"mode\":\"exists\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"bok\"}");
    ASSERT_TRUE(r && strncmp(r, "true", 4) == 0, "valid bulk row stored");
    free(r);
    r = req(tc, "{\"mode\":\"exists\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"bbad\"}");
    ASSERT_TRUE(r && strncmp(r, "false", 5) == 0, "bad bulk row skipped");
    free(r);

    /* Overflowing literal default now fails the insert (was silent
       wrap). */
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"dflt\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"x:int:default=99999999999\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "dflt object created "
                "(default validation is Phase 4)");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"dflt\","
        "\"key\":\"a\",\"value\":{}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") != NULL &&
                strstr(r, "x") != NULL,
                "overflowing literal default refuses the insert");
    free(r);

    /* Float→double widening preserves the exact stored value. */
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"w\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"x:float\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "widen create"); free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"w\","
        "\"key\":\"a\",\"value\":{\"x\":\"3.25\"}}"); free(r);
    r = req(tc, "{\"mode\":\"edit-field\",\"dir\":\"conv\",\"object\":\"w\","
        "\"fields\":[\"x:double\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "widen edit"); free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"w\","
        "\"key\":\"a\"}");
    ASSERT_TRUE(r && strstr(r, "\"x\":3.25") != NULL,
                "widened float reads back exactly");
    free(r);

    /* Round-trip float output: 9/17 significant digits. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"flt\",\"value\":{\"f\":\"1.2345678901234567\","
        "\"g\":\"1.2345678\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "float insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"flt\"}");
    ASSERT_TRUE(r && strstr(r, "\"f\":1.2345678901234567") != NULL,
                "double output round-trips");
    ASSERT_TRUE(r && strstr(r, "\"g\":1.23456776") != NULL,
                "float output round-trips (9 digits; Phase 0 types-audit "
                "shows stored=1.23456776)");
    free(r);

    /* Aggregate text must agree with ordinary reads, before and after
       index creation. A dedicated object keeps each group unambiguous. */
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"ag\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"n:numeric:19,0\",\"d:double\",\"f:float\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "aggregate object created");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"ag\","
        "\"key\":\"one\",\"value\":{\"n\":\"-9223372036854775808\","
        "\"d\":\"1.2345678901234567\",\"f\":\"1.2345678\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "aggregate seed stored");
    free(r);
    const char *ag_fields[] = {"n", "d", "f"};
    const char *ag_values[] = {"-9223372036854775808",
                              "1.2345678901234567", "1.23456776"};
    for (int indexed = 0; indexed < 2; indexed++) {
        for (int j = 0; j < 3; j++) {
            char jq[512], expected[128];
            if (indexed) {
                snprintf(jq, sizeof(jq),
                    "{\"mode\":\"add-index\",\"dir\":\"conv\","
                    "\"object\":\"ag\",\"field\":\"%s\"}", ag_fields[j]);
                r = req(tc, jq);
                ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL,
                            "aggregate index created");
                free(r);
            }
            snprintf(jq, sizeof(jq),
                "{\"mode\":\"aggregate\",\"dir\":\"conv\",\"object\":\"ag\","
                "\"group_by\":\"%s\",\"aggregates\":[{\"fn\":\"count\","
                "\"alias\":\"c\"}]}", ag_fields[j]);
            r = req(tc, jq);
            snprintf(expected, sizeof(expected), "\"%s\":\"%s\"",
                     ag_fields[j], ag_values[j]);
            ASSERT_TRUE(r && strstr(r, expected) != NULL,
                        indexed ? "indexed aggregate label exact" :
                                  "scan aggregate label exact");
            ASSERT_TRUE(r && strstr(r, "\"c\":1") != NULL,
                        "aggregate group contains the seed");
            free(r);
        }
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-scalar-conversion-safety",
              test_scalar_conversion_safety_run)
```

Route shapes verified against the source during this revision: the bulk
insert request field is `records` (server.c:1938) holding an array of
`{key,value}` objects, and the response tallies `{"inserted":N,…}`,
gaining `"errors":M` plus an `"error":"some_records_dropped"` marker
when rows drop (docs/query-protocol/bulk.md:56-71).

Build-list hunk — build.sh line 345 is the exact anchor
`    src/test/cases/test_config_encode.c \` (with continuation backslash);
insert after it:

```make
    src/test/cases/test_scalar_conversion_safety.c \
```

(matching the list's existing indentation).

### Existing test updates (deliberate contract changes)

`src/test/cases/test_computed_update.c` section 13 seeded a NaN via a
plain insert and an Infinity via a plain update to exercise the
computed-update non-finite source guard. Phase 1 rejects both at the
write boundary, so the seed fails and the guard becomes unreachable
through supported routes (it remains as defense in depth). Replace the
whole section — from the anchor
`/* 13. A non-finite $copy source rejects — NaN/Inf bytes must not`
through the `free(resp); resp = NULL;` that follows
`"Infinity copy rejected");` — with:

```c
    /* 13. Non-finite literals are rejected at the write boundary
           (Phase 1 scalar policy): NaN/Infinity bytes never enter
           records through supported routes. The computed-update
           engine's non-finite source guard stays as defense in depth
           but is no longer reachable through supported writes; $copy
           itself is re-checked here with a finite source. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cx\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"s:double\",\"t:double\"]}",
        &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cx\","
        "\"key\":\"n1\",\"value\":{\"s\":NaN,\"t\":1}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "NaN seed rejected");
    ASSERT_CONTAINS(resp, "only finite", "NaN rejection names the policy");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cx\","
        "\"key\":\"n1\",\"value\":{\"s\":1.5,\"t\":1}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "finite seed stored");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cx\","
        "\"key\":\"n1\",\"value\":{\"s\":Infinity}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "Infinity update rejected");
    ASSERT_CONTAINS(resp, "only finite", "Infinity rejection names the policy");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cx\","
                   "\"key\":\"n1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"t\":1", "target unchanged after NaN reject");
    ASSERT_CONTAINS(resp, "\"s\":1.5", "s unchanged after rejects");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cx\","
        "\"key\":\"n1\",\"value\":{\"t\":{\"$copy\":\"s\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\"", "finite $copy applied");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cx\","
                   "\"key\":\"n1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"t\":1.5", "copy mirrored the finite source");
    free(resp); resp = NULL;
```

(The integrated scratch run hit exactly these four failures —
`NaN seed stored`, `non-finite source named`, `target unchanged after
NaN reject`, `Infinity copy rejected` — and this hunk resolves them by
asserting the new boundary behavior rather than weakening anything.)

Expected red on the base branch: the 11 rejection assertions fail
(inserts succeed silently with wrapped values), INT64_MIN/SHORT_MIN fail
(saturation), the rounding and parity assertions fail (double rounding
stores/compares 14 for 0.145; 6e-100 renders 0.01), scale-0 and
trimmed-scalar and varchar-exactness assertions fail, bulk/defaults
assertions fail (no rejection exists), the widening assertion fails
(`5.34788e-318`), and the round-trip-output assertions fail (`%g` text).
Capture both runs per CORE-PROCESS.

### 7b. Aggregate direct-helper regression and integration manifest

The reproducible support files live in [2026-10-22-phase1-scratch](2026-10-22-phase1-scratch/).
`aggregate-probe.c` is a standalone test, not linked into the daemon or
normal test binary: it includes the scratch aggregate translation unit
so both static double helpers can be tested without exposing new APIs.
Run each case in a separate process because the baseline legacy-scale
text cases trap. The complete test source is:

```c
/* Compile with P1_AGG_SOURCE pointing to the scratch query_aggregate.c.
   Separate processes isolate the baseline SIGFPE cases. */
#include P1_AGG_SOURCE
__thread ShardDb *g_db = NULL;

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    int which = atoi(argv[1]);
    TypedField f = {0};
    uint8_t raw[8] = {0}, key[9] = {0};
    char buf[128] = {0};
    const char *expected = NULL;
    f.type = FT_NUMERIC; f.size = 8;
    f.numeric_scale = 0; f.numeric_scale_mult = 1;
    if (which < 2) {
        raw[0] = 0x80; /* INT64_MIN, scaled by 1 */
        key[1] = 0;   /* biased INT64_MIN is zero */
        expected = "-9223372036854775808";
    } else {
        raw[7] = 1; key[1] = 0x80; key[8] = 1;
        f.numeric_scale = 19; f.numeric_scale_mult = 0;
        expected = "1";
    }
    if (which == 0 || which == 2)
        typed_field_to_buf_raw(&f, raw, buf, sizeof(buf), 1);
    else if (which == 1 || which == 3)
        decode_idx_to_buf(&f, key + 1, 8, buf, sizeof(buf), 0);
    else if (which == 4 || which == 5) {
        double out = 0;
        AggValueStatus status = which == 4
            ? typed_field_to_double(&f, raw, 8, &out)
            : decode_index_key_to_double(&f, key, 9, &out);
        printf("case=%d status=%d value=%.17g expected=1\n", which, status, out);
        return status == AGG_VALUE_PRESENT && out == 1.0 ? 0 : 1;
    } else if (which >= 6 && which <= 9) {
        uint64_t bits = 0;
        if (which < 8) {
            double d = 1.2345678901234567;
            f.type = FT_DOUBLE; f.size = 8;
            memcpy(raw, &d, 8); memcpy(&bits, &d, 8);
            bits ^= 1ULL << 63;
            for (int i = 0; i < 8; i++) key[1+i] = bits >> (56 - 8*i);
            expected = "1.2345678901234567";
        } else {
            float v = 1.2345678f; uint32_t b;
            f.type = FT_FLOAT; f.size = 4;
            memcpy(raw, &v, 4); memcpy(&b, &v, 4); b ^= 1U << 31;
            for (int i = 0; i < 4; i++) key[1+i] = b >> (24 - 8*i);
            expected = "1.23456776";
        }
        if (which % 2 == 0)
            typed_field_to_buf_raw(&f, raw, buf, sizeof(buf), 1);
        else
            decode_idx_to_buf(&f, key + 1, f.size, buf, sizeof(buf), 0);
    } else return 2;
    printf("case=%d text=%s expected=%s\n", which, buf, expected);
    return strcmp(buf, expected) == 0 ? 0 : 1;
}
```

Before applying the aggregate hunks, run (from the repository root):

```sh
rtk proxy python3 docs/plans/2026-10-22-phase1-scratch/run-probe.py /tmp/p1-int /tmp/p1-r5-evidence/red
```

After capturing red, apply Task 4's four aggregate numeric changes and
Task 6's four aggregate formatting changes with:

```sh
rtk proxy python3 docs/plans/2026-10-22-phase1-scratch/apply-aggregate.py /tmp/p1-int docs/plans/2026-10-20-phase1-safe-scalar-conversions-plan.md
rtk proxy python3 docs/plans/2026-10-22-phase1-scratch/run-probe.py /tmp/p1-int /tmp/p1-r5-evidence/green
rtk proxy python3 docs/plans/2026-10-22-phase1-scratch/manifest.py . /tmp/p1-int /tmp/p1-r5-evidence/manifest.json
```

The manifest asserts all 14 expected compiled-code files differ from the
baseline and verifies all eight aggregate hunks. It records SHA256 hashes
for the reviewed snapshot. Apply Task 8 documentation in the implementation
branch separately; this code manifest does not claim documentation coverage.
Rebuild and rerun the public regression and the normal full suite after
all eight hunks. These commands repair an existing partial integration;
a fresh execution still stages tests first against baseline production.

## Task 8 — Documentation (same branch)

docs/concepts/typed-records.md:

1. The numeric table row (anchor
   `| \`numeric\` | \`price:numeric:P,S\` | 8 | Scaled int64 BE: stored value = value × 10^S. P is total digits (informational), S is scale. |`)
   becomes:

```markdown
| `numeric` | `price:numeric:P,S` | 8 | Scaled int64 BE: stored value = value × 10^S, parsed exactly (no float), rounded half-away-from-zero to the declared scale — identically on writes and in query literals. P is informational until the packed exact format; S is validated to 0..18. Values whose scaled magnitude exceeds int64 are rejected with an explicit error. |
```

2. New subsection after the `### numeric vs double` block (anchor
   `- **\`currency\`** — shortcut for \`numeric:19,4\` (enough for any real-world currency value).`):

```markdown
### Scalar conversion contract (2026.10 Phase 1)

Writes, updates, defaults, bulk rows and numeric query literals validate
the entire value through one shared exact converter: whole decimal tokens
for `short`/`int`/`long`/`timestamp`/`byte` (optional sign, no fraction
or exponent, full range enforced — no wrapping or saturation, `INT64_MIN`
included), finite `float`/`double` text via `strtof`/`strtod` (non-finite
spellings, overflow, and nonzero underflow-to-zero rejected; subnormals
accepted), and exact scaled-decimal `numeric`/`currency` as above. Outer
ASCII whitespace is stripped once — for the scalar numeric types only;
`varchar` and `enum` values are byte-exact. Embedded whitespace, trailing
junk and hex floats are errors. A numeric literal in a query is converted
by the same rule as the write, so `0.145` at scale 2 finds the row stored
from `0.145` (both mean 0.15). Invalid values fail the write with a
field-specific error — the historic wrap/saturation/zero behaviors are
gone. `float` output uses 9 and `double` 17 significant digits (exact
binary round trip), so `3.14` renders as `3.1400000000000001`. Malformed
calendar/boolean/UUID/IP input keeps its pre-2026.10 behavior until the
Phase 7 validation layer.
```

3. docs/reference/limits.md: no limit changes. The error is the standard
   `{"error"}` shape; no error-code additions.

## Execution order, red/green, gates

1. Branch off default (git writes need the human's authorization per
   CORE-PROCESS; leave work uncommitted per AGENTS.md).
2. **RED FIRST, against unchanged production code:** add
   `src/test/cases/test_scalar_conversion_safety.c` and its build-list
   hunk (Task 7) and touch nothing else. Build
   `rtk proxy env SKIP_TESTS=1 ./build.sh`, run
   `rtk proxy ./build/bin/shard-db-test run test-scalar-conversion-safety`,
   and paste the real failures: rejections that silently wrap, the
   saturating long, `0.145` storing and comparing 14, the leading-zero
   int/long and 50-char float literals whose indexed lookups miss,
   the scale-zero `INT64_MIN` double-minus render, the corrupted
   widening, and the clipped `%g` output. Run Task 7b's separate direct-helper red
   capture before applying its aggregate hunks. No production hunk may land
   before this red capture exists.
3. Apply Tasks 1-6 plus Task 7's documented existing-test contract update
   on top, rebuild, rerun the case, and paste the green
   output directly below the red one (CORE-PROCESS rule). No temporary
   rollback procedure exists in this plan.
4. `rtk proxy ./build/bin/shard-db-test run-all` — full suite after every
   production change round. Phase 0 grep found no test pinning the old
   `%g` float text; if one surfaces, treat it as a documented contract
   change and update it deliberately (never weaken an assertion).
5. Docs hunk with the code. No benches. Sanitizer gates (ASan+UBSan and
   TSan, three fresh runs each) run once after ALL fixes of the completed
   branch series, immediately before push/PR — not between rounds
   (AGENTS.md).
6. Embedded rules from the parent plan apply verbatim: quoted-anchor
   PLAN_NOTES.md halt, stop on uncovered decisions, no assertion
   weakening, work left uncommitted.

## Compatibility notes (release-note material)

- Previously silently-wrapped/saturated/zeroed out-of-range writes now
  fail with `{"error":"value for field '…' …"}`; overflowing
  `default=` literals fail the insert/rebuild instead of storing wrapped
  bytes.
- Numeric writes and numeric query literals round identically (exact,
  ties away from zero); a literal and the value it stored always match.
  Literals that fail to parse evaluate to 0 in criteria — the historic
  junk behavior, unchanged — and become hard errors in Phase 5.
- `numeric` S is validated to 0..18 at declaration; roots with S outside
  that range (already rendering garbage) load with a raw-integer
  fallback everywhere, reject writes, and log a warning.
- Outer whitespace on scalar numeric text is trimmed; varchar/enum
  content is byte-exact as before.
- Float/double response text gains digits (round-trip exact).
- Numeric criterion literals tolerate outer whitespace and treat
  invalid/overflowing literals as 0 on BOTH the indexed and scan routes
  (historic junk behavior, now consistent); float criterion literals and
  float index keys parse with `strtof`, so lookups match stored values
  bit-for-bit. Numeric scale-zero rows containing `INT64_MIN` now render
  correctly (`-9223372036854775808`, single minus, valid JSON).
