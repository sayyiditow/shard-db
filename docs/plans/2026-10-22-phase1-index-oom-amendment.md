# Phase 1 I-01 amendment — allocation-free typed text index conversion

Authorized by the user's request to fix the implementation-review findings.
No commit, push, sanitizer gate or certification work is included.

## Root cause and chosen interface

The new span_to_cstr allocation could fail and each scalar case then emitted
zero with a normal key length. The checked boundary's allocation status was
therefore lost; query/mutation callers could not distinguish a failed parse
from a legitimate zero. The previously captured fault-injection red uses
input42: normal indexed42, failed allocation indexed0, both out_len4.

Avoid that allocation entirely. Rename this INTERNAL interface to
`encode_field_text_for_index` to make the audited text ownership contract
explicit. This symbol is not exported by the public shard_db.h interface.
For FT_LONG/TIMESTAMP/INT/SHORT/BYTE/DOUBLE/FLOAT callers must provide a
complete NUL-terminated scalar spelling, including a readable zero byte at
val[vlen]. The six scalar switch bodies assert that invariant and call the
same libc converter as criteria directly. No copies, truncated prefixes,
heap allocations, OOM fallback, or criterion-policy change remain. Numeric
retains its length-aware exact converter. Varchar retains its explicit
binary span and may contain NUL; other pre-existing type logic is unchanged.

This replaces the earlier plan instruction to allocate and substitute zero
on failure. It is not a silent weakening of the old span contract: the API
is deliberately renamed, every consumer is audited, and the header/wrapper
document the stricter scalar precondition. A future raw mmap scalar span
consumer must use a separate checked span adapter, not this text interface.

## Complete consumer inventory and proof of ownership

- config.c definition and internal stored-key inverse documentation.
- types.h declaration and index API comments: internal only, not shard_db.h.
- index.c index_parallel: extracted heap C strings; four direct call sites
  total across this function and build_index_key_from_json. The quoted
  extraction is coordinated with I-04, decoded once, with NUL rejection for
  scalar strings and preserved failure cleanup.
- query.c encode_criterion_value: SearchCriterion owns terminated strings;
  every numeric bound/range/in-list call supplies its full spelling. LIKE
  partial-prefix spans are raw varchar/composite only. Its tf==NULL raw span
  branch remains unchanged (including Coverity overflow regression).
- query.c cursor encoding: parse_cursor_object checks length and copies
  vlen+1 into each FindCursor part; termination is guaranteed.
- query_plan.c build_exact_composite_key: full SearchCriterion strings.
- query_join.c lookup_remote: non-composite local values originate in
  extract_local_key -> typed_field_to_buf_raw, which terminates all scalar
  spellings; composite bytes bypass the converter. Varchar keeps its length.
- query_aggregate.c only indirect encode_criterion_value calls and comments.
- No direct callers found in bench, fuzz, tests, public header, or CLI.
  test_coverity_encode_criterion_overflow calls wrapper with tf==NULL, whose
  arbitrary span contract is preserved.

## Anchored edits

1. In config.c, delete the complete span_to_cstr function and its preceding
   comment beginning `/* NUL-terminated copy of the complete span` and ending
   in that function's `return s; }`. Add `<assert.h>` beside `<math.h>` and
   `<stdarg.h>`.
2. Replace the entire existing `void encode_field_for_index(` definition
   through its closing brace with the complete body below.
3. Rename every `encode_field_for_index` reference to
   `encode_field_text_for_index` in config.c, types.h, index.c, query.c,
   query_plan.c, query_join.c and query_aggregate.c (comments included).
   No criterion-call signatures change; no index format changes.
4. Before the types.h declaration add:

```c
/* Internal typed-text lookup encoder. Scalar libc-parsed kinds require
   a complete C string: val[vlen] must be a readable NUL terminator.
   Varchar remains an explicit binary span; embedded NUL bytes are valid.
   Scalar conversion is allocation-free; callers never receive a zero key
   merely because an allocation failed. Raw/mmap scalar spans need a
   separate checked adapter, not this text-only interface. */
```

5. Extend encode_criterion_value's comment to require full C strings when
   tf identifies a libc-parsed scalar; its raw/varchar span cases are unchanged.

### Complete replacement encoder

```c
void encode_field_text_for_index(const TypedField *f, const char *val, size_t vlen,
                            uint8_t *out, size_t *out_len) {
    if (!val || vlen == 0) {
        /* Empty / null input. For fixed-width fields we zero f->size bytes
           and the caller sees a sort-minimum key. For varchar we emit zero
           bytes (length=0 sorts before any non-empty). */
        if (f->type == FT_VARCHAR) { *out_len = 0; return; }
        memset(out, 0, f->size);
        *out_len = f->size;
        return;
    }
    switch (f->type) {
    case FT_VARCHAR: {
        /* Raw content, up to vlen bytes. No length prefix — val_cmp does the
           length tiebreak. Cap at f->size - 2 to match on-disk content cap. */
        size_t cap = (size_t)(f->size - 2);
        if (vlen > cap) vlen = cap;
        memcpy(out, val, vlen);
        *out_len = vlen;
        break;
    }
    case FT_LONG:
    case FT_TIMESTAMP: {
        /* Same index-key shape — top-bit-flipped BE int64 gives a memcmp
           total order over the signed range. */
        assert(val[vlen] == '\0');
        int64_t v = strtoll(val, NULL, 10);
        uint64_t u = (uint64_t)v ^ (1ULL << 63);
        out[0] = (u >> 56) & 0xFF; out[1] = (u >> 48) & 0xFF;
        out[2] = (u >> 40) & 0xFF; out[3] = (u >> 32) & 0xFF;
        out[4] = (u >> 24) & 0xFF; out[5] = (u >> 16) & 0xFF;
        out[6] = (u >> 8) & 0xFF;  out[7] = u & 0xFF;
        *out_len = 8;
        break;
    }
    case FT_INT: {
        assert(val[vlen] == '\0');
        int32_t v = (int32_t)strtoll(val, NULL, 10);
        uint32_t u = (uint32_t)v ^ 0x80000000u;
        out[0] = (u >> 24) & 0xFF; out[1] = (u >> 16) & 0xFF;
        out[2] = (u >> 8) & 0xFF;  out[3] = u & 0xFF;
        *out_len = 4;
        break;
    }
    case FT_SHORT: {
        assert(val[vlen] == '\0');
        int16_t v = (int16_t)strtoll(val, NULL, 10);
        uint16_t u = (uint16_t)v ^ 0x8000u;
        out[0] = (u >> 8) & 0xFF; out[1] = u & 0xFF;
        *out_len = 2;
        break;
    }
    case FT_DOUBLE: {
        assert(val[vlen] == '\0');
        double v = strtod(val, NULL);
        uint64_t bits;
        memcpy(&bits, &v, 8);
        /* IEEE-754 total order: if sign bit set (negative), flip all bits;
           else flip just the sign bit. Maps all doubles to a monotonic
           unsigned key under memcmp. */
        if (bits & (1ULL << 63)) bits = ~bits;
        else bits ^= (1ULL << 63);
        out[0] = (bits >> 56) & 0xFF; out[1] = (bits >> 48) & 0xFF;
        out[2] = (bits >> 40) & 0xFF; out[3] = (bits >> 32) & 0xFF;
        out[4] = (bits >> 24) & 0xFF; out[5] = (bits >> 16) & 0xFF;
        out[6] = (bits >> 8) & 0xFF;  out[7] = bits & 0xFF;
        *out_len = 8;
        break;
    }
    case FT_FLOAT: {
        assert(val[vlen] == '\0');
        float v = strtof(val, NULL);
        uint32_t bits;
        memcpy(&bits, &v, 4);
        /* IEEE-754 total order: if sign bit set (negative), flip all bits;
           else flip just the sign bit. Maps all floats to a monotonic
           unsigned key under memcmp. */
        if (bits & 0x80000000u) bits = ~bits;
        else bits ^= 0x80000000u;
        out[0] = (bits >> 24) & 0xFF; out[1] = (bits >> 16) & 0xFF;
        out[2] = (bits >> 8) & 0xFF;  out[3] = bits & 0xFF;
        *out_len = 4;
        break;
    }
    case FT_BOOL: {
        out[0] = (vlen > 0 && (val[0] == 't' || val[0] == 'T' || val[0] == '1')) ? 1 : 0;
        *out_len = 1;
        break;
    }
    case FT_BYTE: {
        assert(val[vlen] == '\0');
        out[0] = (uint8_t)strtoll(val, NULL, 10);
        *out_len = 1;
        break;
    }
    case FT_DATE: {
        char clean[16]; int ci = 0;
        for (size_t i = 0; i < vlen && ci < 8; i++)
            if (val[i] >= '0' && val[i] <= '9') clean[ci++] = val[i];
        clean[ci] = '\0';
        int32_t v = (int32_t)atoi(clean);
        uint32_t u = (uint32_t)v ^ 0x80000000u;
        out[0] = (u >> 24) & 0xFF; out[1] = (u >> 16) & 0xFF;
        out[2] = (u >> 8) & 0xFF;  out[3] = u & 0xFF;
        *out_len = 4;
        break;
    }
    case FT_DATETIME: {
        char clean[16]; int ci = 0;
        for (size_t i = 0; i < vlen && ci < 14; i++)
            if (val[i] >= '0' && val[i] <= '9') clean[ci++] = val[i];
        while (ci < 14) clean[ci++] = '0';
        clean[14] = '\0';
        char datebuf[9]; memcpy(datebuf, clean, 8); datebuf[8] = '\0';
        int32_t d = (int32_t)atoi(datebuf);
        uint32_t du = (uint32_t)d ^ 0x80000000u;
        out[0] = (du >> 24) & 0xFF; out[1] = (du >> 16) & 0xFF;
        out[2] = (du >> 8) & 0xFF;  out[3] = du & 0xFF;
        int hh = (clean[8]-'0')*10 + (clean[9]-'0');
        int mm = (clean[10]-'0')*10 + (clean[11]-'0');
        int ss = (clean[12]-'0')*10 + (clean[13]-'0');
        uint32_t t = (uint32_t)hh * 3600u + (uint32_t)mm * 60u + (uint32_t)ss;
        out[4] = (uint8_t)(t >> 16);
        out[5] = (uint8_t)(t >> 8);
        out[6] = (uint8_t)t;
        *out_len = 7;
        break;
    }
    case FT_DATETIMEMS: {
        char clean[24]; int ci = 0;
        for (size_t i = 0; i < vlen && ci < 17; i++)
            if (val[i] >= '0' && val[i] <= '9') clean[ci++] = val[i];
        while (ci < 17) clean[ci++] = '0';
        clean[17] = '\0';
        char datebuf[9]; memcpy(datebuf, clean, 8); datebuf[8] = '\0';
        int32_t d = (int32_t)atoi(datebuf);
        uint32_t du = (uint32_t)d ^ 0x80000000u;
        out[0] = (du >> 24) & 0xFF; out[1] = (du >> 16) & 0xFF;
        out[2] = (du >> 8) & 0xFF;  out[3] = du & 0xFF;
        int hh = (clean[8]-'0')*10 + (clean[9]-'0');
        int mm = (clean[10]-'0')*10 + (clean[11]-'0');
        int ss = (clean[12]-'0')*10 + (clean[13]-'0');
        int fff = (clean[14]-'0')*100 + (clean[15]-'0')*10 + (clean[16]-'0');
        uint32_t ms = (uint32_t)((hh * 3600 + mm * 60 + ss) * 1000 + fff);
        out[4] = (ms >> 24) & 0xFF; out[5] = (ms >> 16) & 0xFF;
        out[6] = (ms >> 8) & 0xFF;  out[7] = ms & 0xFF;
        *out_len = 8;
        break;
    }
    case FT_TIME: {
        /* Parse "HH:MM:SS" into 3 bytes, top-bit flip for sorted index */
        int hh = 0, mm = 0, ss = 0;
        if (vlen >= 8) {
            hh = (val[0]-'0')*10 + (val[1]-'0');
            mm = (val[3]-'0')*10 + (val[4]-'0');
            ss = (val[6]-'0')*10 + (val[7]-'0');
        }
        uint32_t secs = hh * 3600 + mm * 60 + ss;
        uint32_t u = secs ^ 0x800000u;  /* top-bit flip to sort correctly */
        out[0] = (u >> 16) & 0xFF;
        out[1] = (u >> 8) & 0xFF;
        out[2] = u & 0xFF;
        *out_len = 3;
        break;
    }
    case FT_UUID: {
        /* Same as encode_field_len - parse 32 hex digits to 16 bytes BE */
        uint8_t buf[16] = {0};
        int pos = 0;
        for (size_t i = 0; i < vlen && pos < 32; i++) {
            char c = val[i];
            if (c == '-') continue;
            int nibble = -1;
            if (c >= '0' && c <= '9') nibble = c - '0';
            else if (c >= 'a' && c <= 'f') nibble = c - 'a' + 10;
            else if (c >= 'A' && c <= 'F') nibble = c - 'A' + 10;
            if (nibble < 0) { pos = 0; break; }
            if (pos % 2 == 0) buf[pos / 2] = nibble << 4;
            else buf[pos / 2] |= nibble;
            pos++;
        }
        if (pos != 32) memset(buf, 0, 16);
        memcpy(out, buf, 16);
        *out_len = 16;
        break;
    }
    case FT_IPV4: {
        char ipbuf[16];
        size_t n = vlen < sizeof(ipbuf) - 1 ? vlen : sizeof(ipbuf) - 1;
        memcpy(ipbuf, val, n);
        ipbuf[n] = '\0';
        if (n == 0 || inet_pton(AF_INET, ipbuf, out) != 1)
            memset(out, 0, 4);
        *out_len = 4;
        break;
    }
    case FT_IPV6: {
        char ipbuf[46];
        size_t n = vlen < sizeof(ipbuf) - 1 ? vlen : sizeof(ipbuf) - 1;
        memcpy(ipbuf, val, n);
        ipbuf[n] = '\0';
        if (n == 0 || inet_pton(AF_INET6, ipbuf, out) != 1)
            memset(out, 0, 16);
        *out_len = 16;
        break;
    }
    case FT_NUMERIC: {
        /* Exact: the same conversion as the write path, so a saved
           value is always found by its own literal. Invalid or
           out-of-range literals evaluate to 0 — identical to the
           scan-side criterion compiler, so indexed and scan routes
           agree for junk input too (the historic atof behavior). */
        int64_t v;
        if (conv_numeric_text(f, val, vlen, &v, NULL, 0) != 0) v = 0;
        uint64_t u = (uint64_t)v ^ (1ULL << 63);
        out[0] = (u >> 56) & 0xFF; out[1] = (u >> 48) & 0xFF;
        out[2] = (u >> 40) & 0xFF; out[3] = (u >> 32) & 0xFF;
        out[4] = (u >> 24) & 0xFF; out[5] = (u >> 16) & 0xFF;
        out[6] = (u >> 8) & 0xFF;  out[7] = u & 0xFF;
        *out_len = 8;
        break;
    }
    case FT_ENUM: {
        /* Index-key for enum = byte-index encoding identical to the
           stored record bytes. On miss (criterion uses a value not in
           the list), emit a sentinel that doesn't match any valid
           record: out_len = 0 ensures the bitmap dict lookup misses
           cleanly, and any btree range/eq search using this key
           finds nothing — which is the right answer for a query
           filtering on a value that doesn't exist. */
        int idx = enum_value_index(f, (const char *)val, vlen);
        if (idx < 0) {
            *out_len = 0;
            break;
        }
        if (f->enum_width == 2) {
            out[0] = (uint8_t)((idx >> 8) & 0xFF);
            out[1] = (uint8_t)(idx & 0xFF);
            *out_len = 2;
        } else {
            out[0] = (uint8_t)(idx & 0xFF);
            *out_len = 1;
        }
        break;
    }
    default:
        memset(out, 0, f->size);
        *out_len = f->size;
        break;
    }
}
```

## I-04 coordination in index.c

The JSON agent adds typed_json_string_type and quote-aware
json_get_fields_unescaped. In index_parallel replace its old raw extraction
plus varchar-only decoding block (anchor `char *extracted[MAX_FIELDS * 4];`
through before `IndexThreadArg args[MAX_FIELDS];`) with the following. It
reuses the existing metadata cleanup policy and propagates extraction OOM:

```c
    char *extracted[MAX_FIELDS * 4] = {0};
    enum FieldType extracted_types[MAX_FIELDS * 4];
    for (int j = 0; j < unique_count; j++) {
        int fidx = ts ? typed_field_index(ts, unique_keys[j]) : -1;
        extracted_types[j] = fidx >= 0 ? ts->fields[fidx].type : FT_COUNT;
    }
    errno = 0;
    if (json_get_fields_unescaped(value, unique_keys, unique_count,
                                  extracted_types, extracted) != 0) {
        int decode_errno = errno == ENOMEM ? ENOMEM : EINVAL;
        for (int k = 0; k < unique_count; k++) free(extracted[k]);
        for (int k = 0; k < unique_count; k++) {
            int is_field = 0;
            for (int m = 0; m < nfields; m++)
                if (unique_keys[k] == fields[m]) { is_field = 1; break; }
            if (!is_field) free((char *)unique_keys[k]);
        }
        errno = decode_errno;
        return -1;
    }
```

In build_index_key_from_json's single-field branch replace the extraction
block from `JsonObj jo;` through before `if (!txt || !txt[0])` with:

```c
    int fi = ts ? typed_field_index(ts, spec) : -1;
    enum FieldType text_type = fi >= 0 ? ts->fields[fi].type : FT_COUNT;
    const char *text_key = spec;
    char *txt = NULL;
    errno = 0;
    if (json_get_fields_unescaped(json, &text_key, 1, &text_type, &txt) != 0) {
        free(txt);
        if (errno != ENOMEM) errno = EINVAL;
        return -1;
    }
```

## Verification

The actual pre-fix fault injection already captured input42 -> zero on
malloc3. Preserve it as red evidence. The standalone source-inclusion
probe is `docs/plans/2026-10-22-phase1-index-text-oom-probe.c`. It checks the renamed encoder: arm a malloc substitution which fails every
allocation, exercise all six scalar switch cases (including unbounded
leading-zero integer and 53-character floating spellings), compare each key
against its normal encoding and require zero allocation attempts. Also check
varchar binary `a<NUL>b` retains length3. This proves the removed failure
point cannot corrupt lookup or mutation keys; production query/mutation
consumers now use that same nonallocating converter. Existing public lookup
parity and the new JSON routes tests remain mandatory. Coordinate one build
and focused/full suites with the root agent; no concurrent builds.

### Exact probe commands

Run only after the root agent freezes source and completes the normal build:

```sh
rtk proxy cc -O0 -ffunction-sections -fdata-sections -Isrc/db '-DP1_CONFIG_SOURCE="/home/hashim/junk-system/git-workspace/sayyiditow/shard-db/src/db/config.c"' docs/plans/2026-10-22-phase1-index-text-oom-probe.c build/obj/util.o -Wl,--gc-sections -lpthread -lm -o /tmp/phase1-index-text-oom-probe
rtk proxy /tmp/phase1-index-text-oom-probe
```

Expected: seven scalar kinds plus binary varchar and explicit fault-armed42
check, 9passed/0failed and zero source allocation attempts. The C include
uses the actual current config.c; diagnostic type-name/global stubs only
allow isolated linking. The substitution fails every malloc reachable in
that translation unit while armed. No production failure seam was added.

All nine direct source consumers were renamed. The public embedded header
does not declare this internal converter; CLI, bench and fuzz have no
direct consumers. Existing tf==NULL overflow-wrapper unit tests retain
their arbitrary nonterminated-span behavior unchanged.

### Complete standalone regression source

```c
/* Standalone I-01 regression. Compile with P1_CONFIG_SOURCE naming the
   current config.c; link util.o with function/data sections and gc-sections.
   Deliberately does not add an allocation-failure seam to production. */
#include "types.h"
static int oom_armed;
static size_t allocation_attempts;
static void *index_probe_malloc(size_t n) {
    if (oom_armed) { allocation_attempts++; return NULL; }
    return malloc(n);
}
#define malloc index_probe_malloc
#include P1_CONFIG_SOURCE
#undef malloc
__thread ShardDb *g_db = NULL;
const char *typed_type_name(enum FieldType t) { (void)t; return "scalar"; }

static int one(enum FieldType type, const char *text, size_t width) {
    TypedField f = {.type = type, .size = width};
    uint8_t normal[16] = {0}, failing[16] = {0};
    size_t nl = 0, fl = 0;
    oom_armed = 0;
    encode_field_text_for_index(&f, text, strlen(text), normal, &nl);
    allocation_attempts = 0;
    oom_armed = 1;
    encode_field_text_for_index(&f, text, strlen(text), failing, &fl);
    oom_armed = 0;
    int ok = nl == width && fl == nl &&
             memcmp(normal, failing, nl) == 0 && allocation_attempts == 0;
    printf("type=%d malloc_attempts=%zu key_stable=%d\n", type,
           allocation_attempts, ok);
    return ok ? 0 : 1;
}

int main(void) {
    int failed = 0;
    const char *long_int = "00000000000000000000000000000000000000000042";
    const char *long_float = "1.000000000000000000000000000000000000000000000000e10";
    failed += one(FT_LONG, long_int, 8);
    failed += one(FT_TIMESTAMP, long_int, 8);
    failed += one(FT_INT, long_int, 4);
    failed += one(FT_SHORT, long_int, 2);
    failed += one(FT_BYTE, long_int, 1);
    failed += one(FT_DOUBLE, long_float, 8);
    failed += one(FT_FLOAT, long_float, 4);

    /* An explicit binary varchar span need not have any terminator at
       val[vlen]. The scalar precondition must never be read for varchar. */
    TypedField vf = {.type = FT_VARCHAR, .size = 5};
    const char bytes[3] = {'a', 0, 'b'};
    uint8_t out[3] = {0}; size_t len = 0;
    oom_armed = 1; allocation_attempts = 0;
    encode_field_text_for_index(&vf, bytes, sizeof(bytes), out, &len);
    oom_armed = 0;
    int ok = len == 3 && memcmp(bytes, out, 3) == 0 && allocation_attempts == 0;
    printf("varchar_binary key_stable=%d malloc_attempts=%zu\n", ok,
           allocation_attempts);
    failed += !ok;

    TypedField integer = {.type = FT_INT, .size = 4};
    uint8_t key[4]; size_t keylen = 0;
    oom_armed = 1; allocation_attempts = 0;
    encode_field_text_for_index(&integer, "42", 2, key, &keylen);
    oom_armed = 0;
    uint32_t bits = ((uint32_t)key[0] << 24) | ((uint32_t)key[1] << 16) |
                    ((uint32_t)key[2] << 8) | key[3];
    int value = (int32_t)(bits ^ 0x80000000u);
    ok = keylen == 4 && value == 42 && allocation_attempts == 0;
    printf("fault_armed=1 indexed_value=%d out_len=%zu malloc_attempts=%zu\n",
           value, keylen, allocation_attempts);
    failed += !ok;
    printf("%d passed, %d failed\n", 9 - failed, failed);
    return failed ? 1 : 0;
}
```

### Wrapper precondition declaration

At query_internal.h's `void encode_criterion_value(const TypedField *tf,`
add this comment (signature and raw-span behavior remain unchanged):

```c
/* Raw/varchar criteria are explicit spans. Libc-parsed scalar criteria
   must be full C strings with a readable terminator at val[vlen]. */
```

The only wrapper call arguments not spelled strlen in source are the two
LIKE exact-match calls with pat/pl in btree_dispatch and
chunked_stream_dispatch. Both define pl=strlen(pat) and additionally require
raw_prefix=(!tf || tf->type==FT_VARCHAR). STARTS_WITH on a scalar passes the
complete pc->value/strlen pair. No partial nonterminated scalar consumer
was found. The stricter assertion is absent from varchar/numeric/calendar/
UUID/IP/enum branches.
