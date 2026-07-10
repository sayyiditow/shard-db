#include "types.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include <math.h>
#include <dirent.h>

/* ========== Compiled criteria ========== */
/* ========== Typed-binary compiled criteria (fast path) ==========
 *
 * Replaces per-record malloc+snprintf+strdup+atof+free in scan callbacks
 * by pre-resolving each SearchCriterion against the TypedSchema and
 * pre-parsing rvalues into target binary form. Runtime match compares
 * raw bytes directly against known offsets.
 *
 * Composite fields (contain '+') and unknown fields fall back to the
 * legacy string path via decode_field + match_criterion. */

/* ld_be_i64/i32/i16/u16/u32 and varchar_eff_len are now in query_internal.h */

/* Case-insensitive substring search moved to src/db/simd.c (simd_memcasemem)
   so the AVX2 path and the scalar fallback share one implementation. */

/* Parse decimal date string "yyyyMMdd" (tolerant of separators) → int32. */
static int32_t parse_date_i32(const char *s) {
    char clean[16]; int ci = 0;
    for (const char *c = s; *c && ci < 8; c++)
        if (*c >= '0' && *c <= '9') clean[ci++] = *c;
    clean[ci] = '\0';
    return (int32_t)atoi(clean);
}

/* Parse "yyyyMMddHHmmss" (tolerant) → date (int32) + time (uint16 seconds). */
static void parse_datetime(const char *s, int32_t *out_date, uint16_t *out_time) {
    char clean[16]; int ci = 0;
    for (const char *c = s; *c && ci < 14; c++)
        if (*c >= '0' && *c <= '9') clean[ci++] = *c;
    while (ci < 14) clean[ci++] = '0';
    clean[14] = '\0';
    char dbuf[9]; memcpy(dbuf, clean, 8); dbuf[8] = '\0';
    *out_date = (int32_t)atoi(dbuf);
    int hh = (clean[8]-'0')*10 + (clean[9]-'0');
    int mm = (clean[10]-'0')*10 + (clean[11]-'0');
    int ss = (clean[12]-'0')*10 + (clean[13]-'0');
    *out_time = (uint16_t)(hh * 3600 + mm * 60 + ss);
}

/* Parse "yyyyMMddHHmmssfff" (digit-only, right-zero-padded if short) into
   a calendar date (int32, yyyyMMdd) and ms-of-day (uint32, 0..86399999). */
static void parse_datetimems(const char *s, int32_t *out_date, uint32_t *out_ms) {
    char clean[24]; int ci = 0;
    for (const char *c = s; *c && ci < 17; c++)
        if (*c >= '0' && *c <= '9') clean[ci++] = *c;
    while (ci < 17) clean[ci++] = '0';
    clean[17] = '\0';
    char dbuf[9]; memcpy(dbuf, clean, 8); dbuf[8] = '\0';
    *out_date = (int32_t)atoi(dbuf);
    int hh = (clean[8]-'0')*10 + (clean[9]-'0');
    int mm = (clean[10]-'0')*10 + (clean[11]-'0');
    int ss = (clean[12]-'0')*10 + (clean[13]-'0');
    int fff = (clean[14]-'0')*100 + (clean[15]-'0')*10 + (clean[16]-'0');
    *out_ms = (uint32_t)((hh * 3600 + mm * 60 + ss) * 1000 + fff);
}

/* Parse decimal string → int64 scaled by 10^scale (matches encode_field FT_NUMERIC). */
static int64_t parse_numeric_i64(const char *s, int64_t mul) {
    double dv = atof(s);
    return (int64_t)(dv * mul + (dv >= 0 ? 0.5 : -0.5));
}

/* Parse UUID string (32 hex digits, with or without hyphens) → 16 bytes.
   On malformed input, output is all zeros. */
static void parse_uuid(const char *s, uint8_t out[16]) {
    memset(out, 0, 16);
    if (!s) return;
    int pos = 0;
    for (const char *c = s; *c && pos < 32; c++) {
        if (*c == '-') continue;
        int nibble = -1;
        if (*c >= '0' && *c <= '9') nibble = *c - '0';
        else if (*c >= 'a' && *c <= 'f') nibble = *c - 'a' + 10;
        else if (*c >= 'A' && *c <= 'F') nibble = *c - 'A' + 10;
        if (nibble < 0) { memset(out, 0, 16); return; }
        if (pos % 2 == 0) out[pos / 2] = nibble << 4;
        else out[pos / 2] |= nibble;
        pos++;
    }
    if (pos != 32) { memset(out, 0, 16); }
}

/* Parse a dotted-quad IPv4 string into 4 raw network-byte-order bytes.
   Malformed/empty input zero-fills (mirrors parse_uuid's fallback). */
static void parse_ipv4(const char *s, uint8_t out[4]) {
    if (!s || !s[0] || inet_pton(AF_INET, s, out) != 1)
        memset(out, 0, 4);
}

/* Parse a canonical IPv6 string into 16 raw network-byte-order bytes.
   Malformed/empty input zero-fills (mirrors parse_ipv4/parse_uuid). */
static void parse_ipv6(const char *s, uint8_t out[16]) {
    if (!s || !s[0] || inet_pton(AF_INET6, s, out) != 1)
        memset(out, 0, 16);
}

/* Parse "HH:MM:SS" into 3 bytes (seconds since midnight, big-endian) */
static void parse_time(const char *s, uint8_t out[3]) {
    /* Parse "HH:MM:SS" query operand into 3 bytes BE. Strict validation
       (digits + ':' separators + range) matches the FT_TIME encode path in
       config.c so an out-of-range or malformed criterion can never silently
       compare against an unrelated wraparound value. Malformed → 0. */
    memset(out, 0, 3);
    if (!s || strlen(s) < 8) return;
    if (s[2] != ':' || s[5] != ':') return;
    for (int i = 0; i < 8; i++) {
        if (i == 2 || i == 5) continue;
        if (s[i] < '0' || s[i] > '9') return;
    }
    int hh = (s[0]-'0')*10 + (s[1]-'0');
    int mm = (s[3]-'0')*10 + (s[4]-'0');
    int ss = (s[6]-'0')*10 + (s[7]-'0');
    if (hh > 23 || mm > 59 || ss > 59) return;
    uint32_t secs = (uint32_t)hh * 3600u + (uint32_t)mm * 60u + (uint32_t)ss;
    out[0] = (secs >> 16) & 0xFF;
    out[1] = (secs >> 8) & 0xFF;
    out[2] = secs & 0xFF;
}

/* Lowercase duplicate (ASCII). Caller frees. */
static char *strdup_lower(const char *s, size_t len) {
    char *r = malloc(len + 1);
    for (size_t i = 0; i < len; i++) {
        char c = s[i];
        if (c >= 'A' && c <= 'Z') c = (char)(c + 32);
        r[i] = c;
    }
    r[len] = '\0';
    return r;
}

/* Best-effort literal-anchor extraction from a POSIX ERE pattern. Sets
   *out_anchors / *out_lens (heap-allocated arrays) and *out_count.
   Sets *out_count = 0 (and leaves the arrays NULL) when the pattern
   can't be reduced to a set of literal substrings — caller falls back
   to plain regexec on every record.

   Soundness contract: we only emit anchors when EVERY top-level
   alternative is a pure literal. Stripping outer `(...)` is allowed
   only when no other parens or alternation operators appear inside.
   `^` / `$` anchors are stripped (they impose position constraints
   that the substring search ignores; that's fine because it can only
   produce false positives, never false negatives). Branches with
   `[`, `\`, `.`, `*`, `+`, `?`, `{`, `}`, `(`, `)` make us bail.

   Examples:
     "(engineer|developer)"   →  ["engineer", "developer"]
     "@(gmail|yahoo)"         →  ["@gmail", "@yahoo"]   *(see note)
     "^z"                     →  ["z"]
     "alice|bob"              →  ["alice", "bob"]
     "[abc]xy"                →  no anchors (bracket class)
     ".+@.+"                  →  no anchors

   *Note: `@(gmail|yahoo)` doesn't reduce trivially since the `(` is
    not at position 0; we'd need a richer grammar. Falls through to
    no-anchors. The wins still hit the simple cases above. */
#define MAX_REGEX_ANCHORS 16
#define MAX_REGEX_ANCHOR_BYTES 64

static int regex_char_is_meta(char c) {
    return c == '[' || c == ']' || c == '.' || c == '*' || c == '+' ||
           c == '?' || c == '{' || c == '}' || c == '(' || c == ')' ||
           c == '\\';
}

static void regex_extract_anchors(const char *pat,
                                  char ***out_anchors,
                                  size_t **out_lens,
                                  int *out_count) {
    *out_anchors = NULL;
    *out_lens = NULL;
    *out_count = 0;
    if (!pat) return;

    const char *p = pat;
    size_t plen = strlen(p);

    /* Strip a single layer of outer (...) only when no inner parens
       or alternation operators appear (i.e. the parens just group a
       flat literal alternation). */
    if (plen >= 2 && p[0] == '(' && p[plen - 1] == ')') {
        int depth_safe = 1;
        for (size_t i = 1; i + 1 < plen; i++) {
            if (p[i] == '(' || p[i] == ')') { depth_safe = 0; break; }
        }
        if (depth_safe) { p++; plen -= 2; }
    }
    /* Strip ^ at start, $ at end (position anchors don't constrain
       substring search soundness). */
    if (plen > 0 && p[0] == '^')           { p++;    plen--; }
    if (plen > 0 && p[plen - 1] == '$')    { plen--; }
    if (plen == 0) return;

    /* Split on top-level '|'. Each segment must be pure-literal — no
       metachars, no positional anchors, no nesting. */
    char  tmp_anchors[MAX_REGEX_ANCHORS][MAX_REGEX_ANCHOR_BYTES];
    size_t tmp_lens[MAX_REGEX_ANCHORS];
    int n = 0;

    const char *seg = p;
    const char *end = p + plen;
    while (seg <= end) {
        const char *cur = seg;
        int seg_has_meta = 0;
        while (cur < end && *cur != '|') {
            if (regex_char_is_meta(*cur)) { seg_has_meta = 1; break; }
            cur++;
        }
        if (seg_has_meta) return;
        size_t seg_len = (size_t)(cur - seg);
        if (seg_len == 0 || seg_len >= MAX_REGEX_ANCHOR_BYTES) return;
        if (n >= MAX_REGEX_ANCHORS) return;
        memcpy(tmp_anchors[n], seg, seg_len);
        tmp_anchors[n][seg_len] = '\0';
        tmp_lens[n] = seg_len;
        n++;
        if (cur >= end) break;
        seg = cur + 1;  /* skip the '|' */
    }
    if (n == 0) return;

    /* Promote to heap. */
    char **anchors = malloc((size_t)n * sizeof(char *));
    size_t *lens = malloc((size_t)n * sizeof(size_t));
    if (!anchors || !lens) { free(anchors); free(lens); return; }
    for (int i = 0; i < n; i++) {
        anchors[i] = strdup(tmp_anchors[i]);
        lens[i] = tmp_lens[i];
        if (!anchors[i]) {
            for (int k = 0; k < i; k++) free(anchors[k]);
            free(anchors); free(lens);
            return;
        }
    }
    *out_anchors = anchors;
    *out_lens = lens;
    *out_count = n;
}

/* Returns 1 iff at least one of the criterion's literal anchors appears
   anywhere in the (hay, hay_len) byte range. Caller must ensure
   cc->re_anchor_count > 0 before calling.
   Uses simd_memmem (AVX2 first-byte+last-byte filter on x86_64) which is
   3-5× faster than glibc memmem for ASCII haystacks and 3-32B needles. */
static inline int regex_anchors_match(const CompiledCriterion *cc,
                                      const char *hay, size_t hay_len) {
    for (int i = 0; i < cc->re_anchor_count; i++) {
        if (simd_memmem(hay, hay_len, cc->re_anchors[i], cc->re_anchor_lens[i])) {
            return 1;
        }
    }
    return 0;
}

void compile_one(CompiledCriterion *cc, const SearchCriterion *c,
                        const TypedSchema *ts) {
    memset(cc, 0, sizeof(*cc));
    cc->op = c->op;
    cc->raw = c;

    if (strchr(c->field, '+')) { cc->composite = 1; return; }
    int idx = ts ? typed_field_index(ts, c->field) : -1;
    if (idx < 0) { cc->tf = NULL; return; }
    cc->tf = &ts->fields[idx];
    cc->ftype = cc->tf->type;

    /* Copy string forms (needed for varchar ops and fallback BETWEEN strings) */
    cc->s1_len = strlen(c->value);
    cc->s1 = malloc(cc->s1_len + 1);
    memcpy(cc->s1, c->value, cc->s1_len + 1);
    cc->s2_len = strlen(c->value2);
    cc->s2 = malloc(cc->s2_len + 1);
    memcpy(cc->s2, c->value2, cc->s2_len + 1);

    /* Length ops parse value/value2 as integers regardless of the field's
       native type. Compile time, so the hot path skips strtoll per record. */
    if (cc->op == OP_LEN_EQ || cc->op == OP_LEN_NEQ ||
        cc->op == OP_LEN_LESS || cc->op == OP_LEN_GREATER ||
        cc->op == OP_LEN_LESS_EQ || cc->op == OP_LEN_GREATER_EQ ||
        cc->op == OP_LEN_BETWEEN) {
        cc->i1 = (int64_t)strtoll(c->value, NULL, 10);
        cc->i2 = (int64_t)strtoll(c->value2, NULL, 10);
        return;
    }

    /* Field-vs-field ops: c->value names the RHS sibling field. Resolve to
       a TypedField pointer and require type match — mismatched types yield
       cc->rhs_tf=NULL, which match_typed treats as "no match" for every
       record (graceful degradation; planner sees the leaf as non-indexable). */
    if (cc->op == OP_EQ_FIELD || cc->op == OP_NEQ_FIELD ||
        cc->op == OP_LT_FIELD || cc->op == OP_GT_FIELD ||
        cc->op == OP_LTE_FIELD || cc->op == OP_GTE_FIELD) {
        /* ts is non-NULL here — the early-return at the top of compile_one
           (cc->tf = NULL on idx < 0) only proceeds when ts && idx >= 0. */
        int rhs_idx = typed_field_index(ts, c->value);
        if (rhs_idx >= 0) {
            const TypedField *rhs = &ts->fields[rhs_idx];
            if (rhs->type == cc->ftype) cc->rhs_tf = rhs;
            /* else: leave rhs_tf NULL; match returns 0 every time. */
        }
        return;
    }

    /* Regex ops: compile pattern once with REG_EXTENDED. REG_NOSUB is
       deliberately omitted — REG_STARTEND in match_typed_varchar relies
       on pmatch[0] being honored, which REG_NOSUB suppresses. The
       per-call subgroup-tracking cost is small for the patterns users
       actually write. Failed regcomp leaves re_compiled=0 → no record
       matches OP_REGEX, every record matches OP_NOT_REGEX. */
    if (cc->op == OP_REGEX || cc->op == OP_NOT_REGEX) {
        cc->re = malloc(sizeof(regex_t));
        if (cc->re && regcomp(cc->re, c->value, REG_EXTENDED) == 0) {
            cc->re_compiled = 1;
        } else if (cc->re) {
            free(cc->re); cc->re = NULL;
        }
        /* Extract literal anchors for the substring pre-filter. */
        regex_extract_anchors(c->value, &cc->re_anchors, &cc->re_anchor_lens,
                              &cc->re_anchor_count);
        return;
    }

    /* Type-specific parsing of scalar rvalue */
    switch (cc->ftype) {
    case FT_LONG:
    case FT_TIMESTAMP:   /* Unix epoch ms — same int64 BE encoding as FT_LONG. */
    case FT_INT:
    case FT_SHORT:
        cc->i1 = (int64_t)strtoll(c->value, NULL, 10);
        cc->i2 = (int64_t)strtoll(c->value2, NULL, 10);
        break;
    case FT_NUMERIC:
        cc->i1 = parse_numeric_i64(c->value, cc->tf->numeric_scale_mult);
        cc->i2 = parse_numeric_i64(c->value2, cc->tf->numeric_scale_mult);
        break;
    case FT_DOUBLE:
        cc->d1 = atof(c->value);
        cc->d2 = atof(c->value2);
        break;
    case FT_FLOAT:
        cc->d1 = atof(c->value);
        cc->d2 = atof(c->value2);
        break;
    case FT_BOOL:
        cc->b1 = (c->value[0] == 't' || c->value[0] == 'T' || c->value[0] == '1') ? 1 : 0;
        break;
    case FT_BYTE:
        cc->b1 = (uint8_t)atoi(c->value);
        break;
    case FT_DATE:
        cc->i1 = parse_date_i32(c->value);
        cc->i2 = parse_date_i32(c->value2);
        break;
    case FT_DATETIME: {
        int32_t d; uint16_t t;
        parse_datetime(c->value, &d, &t); cc->i1 = d; cc->t1 = t;
        parse_datetime(c->value2, &d, &t); cc->i2 = d; cc->t2 = t;
        break;
    }
    case FT_DATETIMEMS: {
        int32_t d; uint32_t ms;
        parse_datetimems(c->value, &d, &ms); cc->i1 = d; cc->dm1 = (int32_t)ms;
        parse_datetimems(c->value2, &d, &ms); cc->i2 = d; cc->dm2 = (int32_t)ms;
        break;
    }
    case FT_TIME: {
        if (c->value[0]) {
            parse_time(c->value, cc->time_val);
        }
        if (c->value2[0]) {
            parse_time(c->value2, cc->time_val2);
        }
        break;
    }
    case FT_UUID: {
        /* Parse canonical UUID string to 16 bytes */
        if (c->value[0]) {
            parse_uuid(c->value, cc->uuid_bytes);
        }
        if (c->value2[0]) {
            parse_uuid(c->value2, cc->uuid_bytes2);
        }
        break;
    }
    case FT_IPV4: {
        if (c->value[0]) { parse_ipv4(c->value, cc->ipv4_val); }
        if (c->value2[0]) { parse_ipv4(c->value2, cc->ipv4_val2); }
        break;
    }
    case FT_IPV6: {
        if (c->value[0]) { parse_ipv6(c->value, cc->ipv6_val); }
        if (c->value2[0]) { parse_ipv6(c->value2, cc->ipv6_val2); }
        break;
    }
    case FT_ENUM:
        /* Resolve the criterion's display-string value to its byte index
           in the enum's value list. -1 means "no such value" — match_typed
           interprets that as "no record can match this criterion under
           positive ops" (EQ/IN/LT/GT/etc.) and "every record matches under
           the negation" (NEQ/NOT_IN). The c->value lookup uses
           enum_value_index (linear scan; cold path, n ≤ 65535). */
        if (cc->tf && c->value[0]) {
            cc->i1 = (int64_t)enum_value_index(cc->tf, c->value, strlen(c->value));
        } else {
            cc->i1 = -1;
        }
        if (cc->tf && c->value2[0]) {
            cc->i2 = (int64_t)enum_value_index(cc->tf, c->value2, strlen(c->value2));
        } else {
            cc->i2 = -1;
        }
        break;
    case FT_VARCHAR:
    default:
        break;
    }

    /* LIKE/CONTAINS/STARTS/ENDS needle prep for varchar ops.
       Stores the needle pre-stripped (LIKE strips `%`) in cc->needle_lc.
       CS family (LIKE/NOT_LIKE/CONTAINS/NOT_CONTAINS/STARTS_WITH/ENDS_WITH):
         needle stored raw — matchers use memcmp/memmem.
       CI family (ILIKE/INOT_LIKE/ICONTAINS/INOT_CONTAINS/ISTARTS_WITH/IENDS_WITH):
         needle stored ASCII-lowered — matchers tolower the haystack per char.
       The field name `needle_lc` is now misleading; kept for diff continuity. */
    int is_like_op = (cc->op == OP_LIKE || cc->op == OP_NOT_LIKE ||
                      cc->op == OP_ILIKE || cc->op == OP_INOT_LIKE);
    int is_substr_op = (cc->op == OP_CONTAINS || cc->op == OP_NOT_CONTAINS ||
                        cc->op == OP_ICONTAINS || cc->op == OP_INOT_CONTAINS ||
                        cc->op == OP_STARTS_WITH || cc->op == OP_ENDS_WITH ||
                        cc->op == OP_ISTARTS_WITH || cc->op == OP_IENDS_WITH);
    int is_ci_op = (cc->op == OP_ILIKE || cc->op == OP_INOT_LIKE ||
                    cc->op == OP_ICONTAINS || cc->op == OP_INOT_CONTAINS ||
                    cc->op == OP_ISTARTS_WITH || cc->op == OP_IENDS_WITH);

    if (cc->ftype == FT_VARCHAR && (is_like_op || is_substr_op)) {
        const char *pat = c->value;
        size_t pl = cc->s1_len;
        const char *needle_start = pat;
        size_t needle_len = pl;
        cc->like_kind = LK_EXACT;

        if (is_like_op) {
            if (pl >= 2 && pat[0] == '%' && pat[pl-1] == '%') {
                cc->like_kind = LK_CONTAINS;
                needle_start = pat + 1;
                needle_len = pl - 2;
            } else if (pl >= 1 && pat[0] == '%') {
                cc->like_kind = LK_CONTAINS;
                needle_start = pat + 1;
                needle_len = pl - 1;
            } else if (pl >= 1 && pat[pl-1] == '%') {
                cc->like_kind = LK_PREFIX;
                needle_len = pl - 1;
            }
        }

        cc->needle_len = needle_len;
        if (is_ci_op) {
            cc->needle_lc = strdup_lower(needle_start, needle_len);
        } else {
            cc->needle_lc = malloc(needle_len > 0 ? needle_len : 1);
            if (needle_len > 0) memcpy(cc->needle_lc, needle_start, needle_len);
        }
    }

    /* IN/NOT_IN list pre-parsing (numerics only — varchar uses raw strings) */
    if ((cc->op == OP_IN || cc->op == OP_NOT_IN) && c->in_count > 0) {
        cc->in_count = c->in_count;
        switch (cc->ftype) {
        case FT_LONG: case FT_TIMESTAMP: case FT_INT: case FT_SHORT:
        case FT_BOOL: case FT_BYTE:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++) {
                if (cc->ftype == FT_BOOL && (c->in_values[i][0] == 't' ||
                    c->in_values[i][0] == 'T' || c->in_values[i][0] == '1'))
                    cc->in_i64[i] = 1;
                else
                    cc->in_i64[i] = (int64_t)strtoll(c->in_values[i], NULL, 10);
            }
            break;
        case FT_NUMERIC:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_i64[i] = parse_numeric_i64(c->in_values[i], cc->tf->numeric_scale_mult);
            break;
        case FT_DOUBLE:
            cc->in_f64 = malloc(sizeof(double) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_f64[i] = atof(c->in_values[i]);
            break;
        case FT_FLOAT:
            cc->in_f64 = malloc(sizeof(double) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_f64[i] = atof(c->in_values[i]);
            break;
        case FT_DATE:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_i64[i] = (int64_t)parse_date_i32(c->in_values[i]);
            break;
        case FT_TIME:
            cc->in_time = malloc(sizeof(uint8_t[3]) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                parse_time(c->in_values[i], cc->in_time[i]);
            break;
        case FT_UUID:
            cc->in_uuid = malloc(sizeof(uint8_t[16]) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                parse_uuid(c->in_values[i], cc->in_uuid[i]);
            break;
        case FT_IPV4:
            cc->in_ipv4 = malloc(sizeof(uint8_t[4]) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                parse_ipv4(c->in_values[i], cc->in_ipv4[i]);
            break;
        case FT_IPV6:
            cc->in_ipv6 = malloc(sizeof(uint8_t[16]) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                parse_ipv6(c->in_values[i], cc->in_ipv6[i]);
            break;
        case FT_ENUM:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_i64[i] = (int64_t)enum_value_index(cc->tf,
                                                          c->in_values[i],
                                                          strlen(c->in_values[i]));
            break;
        default:
            /* VARCHAR, DATETIME: values stay raw strings via c->in_values,
               but lengths are fixed for the life of this compiled criterion —
               precompute once here instead of strlen() per record in the
               match loop (match_typed_varchar's OP_IN/OP_NOT_IN cases). */
            cc->in_lens = malloc(sizeof(size_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_lens[i] = strlen(c->in_values[i]);
            break;
        }
    }
}

CompiledCriterion *compile_criteria(const SearchCriterion *in, int n,
                                    const TypedSchema *ts) {
    if (n <= 0) return NULL;
    CompiledCriterion *arr = calloc(n, sizeof(CompiledCriterion));
    for (int i = 0; i < n; i++) compile_one(&arr[i], &in[i], ts);
    return arr;
}

void free_compiled_criteria(CompiledCriterion *arr, int n) {
    if (!arr) return;
    for (int i = 0; i < n; i++) {
        free(arr[i].s1);
        free(arr[i].s2);
        free(arr[i].needle_lc);
        free(arr[i].in_i64);
        free(arr[i].in_f64);
        free(arr[i].in_uuid);
        free(arr[i].in_time);
        free(arr[i].in_ipv4);
        free(arr[i].in_ipv6);
        free(arr[i].in_lens);
        if (arr[i].re) {
            if (arr[i].re_compiled) regfree(arr[i].re);
            free(arr[i].re);
        }
        if (arr[i].re_anchors) {
            for (int k = 0; k < arr[i].re_anchor_count; k++) {
                free(arr[i].re_anchors[k]);
            }
            free(arr[i].re_anchors);
            free(arr[i].re_anchor_lens);
        }
    }
    free(arr);
}

/* Compare varchar field at p (fixed size) against compiled criterion. */
static int match_typed_varchar(const uint8_t *p, int size,
                               const CompiledCriterion *cc) {
    int elen = varchar_eff_len(p, size);
    const char *hay = (const char *)(p + 2);  /* content starts after uint16 prefix */
    const SearchCriterion *c = cc->raw;

    switch (cc->op) {
    case OP_EXISTS: return elen > 0;
    case OP_NOT_EXISTS: return elen == 0;
    case OP_EQUAL:
        return elen == (int)cc->s1_len && memcmp(hay, cc->s1, elen) == 0;
    case OP_NOT_EQUAL:
        return !(elen == (int)cc->s1_len && memcmp(hay, cc->s1, elen) == 0);
    case OP_LESS: case OP_GREATER: case OP_LESS_EQ: case OP_GREATER_EQ: {
        size_t n = elen < (int)cc->s1_len ? (size_t)elen : cc->s1_len;
        int r = memcmp(hay, cc->s1, n);
        if (r == 0) r = (elen < (int)cc->s1_len) ? -1 : (elen > (int)cc->s1_len ? 1 : 0);
        switch (cc->op) {
            case OP_LESS: return r < 0;
            case OP_GREATER: return r > 0;
            case OP_LESS_EQ: return r <= 0;
            case OP_GREATER_EQ: return r >= 0;
            default: return 0;
        }
    }
    case OP_BETWEEN: {
        size_t n1 = elen < (int)cc->s1_len ? (size_t)elen : cc->s1_len;
        int r1 = memcmp(hay, cc->s1, n1);
        if (r1 == 0) r1 = (elen < (int)cc->s1_len) ? -1 : (elen > (int)cc->s1_len ? 1 : 0);
        if (c->min_exclusive ? (r1 <= 0) : (r1 < 0)) return 0;
        size_t n2 = elen < (int)cc->s2_len ? (size_t)elen : cc->s2_len;
        int r2 = memcmp(hay, cc->s2, n2);
        if (r2 == 0) r2 = (elen < (int)cc->s2_len) ? -1 : (elen > (int)cc->s2_len ? 1 : 0);
        return c->max_exclusive ? (r2 < 0) : (r2 <= 0);
    }
    /* CS string ops — needle stored raw in cc->needle_lc (despite the
       legacy field name; compile_one only lowers for CI variants below). */
    case OP_LIKE:
        switch (cc->like_kind) {
        case LK_EXACT:
            return elen == (int)cc->needle_len &&
                   memcmp(hay, cc->needle_lc, cc->needle_len) == 0;
        case LK_PREFIX:
            return elen >= (int)cc->needle_len &&
                   memcmp(hay, cc->needle_lc, cc->needle_len) == 0;
        case LK_CONTAINS:
            return simd_memmem(hay, elen, cc->needle_lc, cc->needle_len) != NULL;
        }
        return 0;
    case OP_NOT_LIKE: {
        CompiledCriterion tmp = *cc; tmp.op = OP_LIKE;
        return !match_typed_varchar(p, size, &tmp);
    }
    case OP_CONTAINS:
        return simd_memmem(hay, elen, cc->needle_lc, cc->needle_len) != NULL;
    case OP_NOT_CONTAINS:
        return simd_memmem(hay, elen, cc->needle_lc, cc->needle_len) == NULL;
    case OP_STARTS_WITH:
        return elen >= (int)cc->needle_len &&
               memcmp(hay, cc->needle_lc, cc->needle_len) == 0;
    case OP_ENDS_WITH:
        return elen >= (int)cc->needle_len &&
               memcmp(hay + elen - cc->needle_len, cc->needle_lc, cc->needle_len) == 0;
    /* CI variants — needle is pre-lowered in compile_one; haystack is
       lowered per char on the hot path. memcasemem fuses both. */
    case OP_ILIKE:
        switch (cc->like_kind) {
        case LK_EXACT: {
            if (elen != (int)cc->needle_len) return 0;
            for (int i = 0; i < elen; i++) {
                char a = hay[i], b = cc->needle_lc[i];
                if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
                if (a != b) return 0;
            }
            return 1;
        }
        case LK_PREFIX:
            if (elen < (int)cc->needle_len) return 0;
            for (size_t i = 0; i < cc->needle_len; i++) {
                char a = hay[i], b = cc->needle_lc[i];
                if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
                if (a != b) return 0;
            }
            return 1;
        case LK_CONTAINS:
            return simd_memcasemem(hay, elen, cc->needle_lc, cc->needle_len) != NULL;
        }
        return 0;
    case OP_INOT_LIKE: {
        CompiledCriterion tmp = *cc; tmp.op = OP_ILIKE;
        return !match_typed_varchar(p, size, &tmp);
    }
    case OP_ICONTAINS:
        return simd_memcasemem(hay, elen, cc->needle_lc, cc->needle_len) != NULL;
    case OP_INOT_CONTAINS:
        return simd_memcasemem(hay, elen, cc->needle_lc, cc->needle_len) == NULL;
    case OP_ISTARTS_WITH: {
        if (elen < (int)cc->needle_len) return 0;
        for (size_t i = 0; i < cc->needle_len; i++) {
            char a = hay[i], b = cc->needle_lc[i];
            if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
            if (a != b) return 0;
        }
        return 1;
    }
    case OP_IENDS_WITH: {
        if (elen < (int)cc->needle_len) return 0;
        const char *tail = hay + elen - cc->needle_len;
        for (size_t i = 0; i < cc->needle_len; i++) {
            char a = tail[i], b = cc->needle_lc[i];
            if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
            if (a != b) return 0;
        }
        return 1;
    }
    case OP_IN:
        for (int i = 0; i < c->in_count; i++) {
            size_t vl = cc->in_lens[i];
            if (elen == (int)vl && memcmp(hay, c->in_values[i], vl) == 0) return 1;
        }
        return 0;
    case OP_NOT_IN:
        for (int i = 0; i < c->in_count; i++) {
            size_t vl = cc->in_lens[i];
            if (elen == (int)vl && memcmp(hay, c->in_values[i], vl) == 0) return 0;
        }
        return 1;
    case OP_LEN_EQ:         return elen == (int)cc->i1;
    case OP_LEN_NEQ:        return elen != (int)cc->i1;
    case OP_LEN_LESS:       return elen <  (int)cc->i1;
    case OP_LEN_GREATER:    return elen >  (int)cc->i1;
    case OP_LEN_LESS_EQ:    return elen <= (int)cc->i1;
    case OP_LEN_GREATER_EQ: return elen >= (int)cc->i1;
    case OP_LEN_BETWEEN:    return elen >= (int)cc->i1 && elen <= (int)cc->i2;
    case OP_REGEX:
    case OP_NOT_REGEX: {
        if (!cc->re_compiled) return cc->op == OP_REGEX ? 0 : 1;
        /* Substring pre-filter: if the pattern reduces to a flat set
           of literal alternatives, regex CAN'T match unless at least
           one of those literals appears in `hay`. memmem at ~30ns per
           anchor beats regexec at ~100ns-5µs. False positives (anchor
           present but regex doesn't match) fall through to regexec.
           Skipped when re_anchor_count == 0 (pattern wasn't reducible). */
        if (cc->re_anchor_count > 0 &&
            !regex_anchors_match(cc, hay, (size_t)elen)) {
            return cc->op == OP_REGEX ? 0 : 1;
        }
        /* REG_STARTEND lets regexec consume (ptr, len) directly so we
           don't need to copy + NUL-terminate every record's value. */
        regmatch_t pm[1];
        pm[0].rm_so = 0;
        pm[0].rm_eo = elen;
        int rc = regexec(cc->re, hay, 0, pm, REG_STARTEND);
        return cc->op == OP_REGEX ? (rc == 0) : (rc != 0);
    }
    /* Field-vs-field is intercepted in match_typed before per-type dispatch;
       these labels exist only to keep the switch exhaustive (silences -Wswitch). */
    case OP_EQ_FIELD: case OP_NEQ_FIELD:
    case OP_LT_FIELD: case OP_GT_FIELD:
    case OP_LTE_FIELD: case OP_GTE_FIELD:
    case OP_UNKNOWN:
        return 0;
    }
    return 0;
}

/* Numeric comparison helpers — integer and double flavors, sharing op dispatch. */
static inline int cmp_op_i64(int64_t v, int64_t q1, int64_t q2,
                             enum SearchOp op, const int64_t *in_list,
                             int in_count, const CompiledCriterion *cc) {
    switch (op) {
    case OP_EXISTS: return 1; /* numeric fields always "exist" — zero is valid */
    case OP_NOT_EXISTS: return 0;
    case OP_EQUAL: return v == q1;
    case OP_NOT_EQUAL: return v != q1;
    case OP_LESS: return v < q1;
    case OP_GREATER: return v > q1;
    case OP_LESS_EQ: return v <= q1;
    case OP_GREATER_EQ: return v >= q1;
    case OP_BETWEEN: {
        int lo = (cc && cc->raw && cc->raw->min_exclusive) ? (v > q1) : (v >= q1);
        int hi = (cc && cc->raw && cc->raw->max_exclusive) ? (v < q2) : (v <= q2);
        return lo && hi;
    }
    case OP_IN:
        for (int i = 0; i < in_count; i++) if (v == in_list[i]) return 1;
        return 0;
    case OP_NOT_IN:
        for (int i = 0; i < in_count; i++) if (v == in_list[i]) return 0;
        return 1;
    default: return 0;
    }
}

static inline int cmp_op_f64(double v, double q1, double q2,
                             enum SearchOp op, const double *in_list, int in_count,
                             const CompiledCriterion *cc) {
    switch (op) {
    case OP_EXISTS: return 1;
    case OP_NOT_EXISTS: return 0;
    case OP_EQUAL: return v == q1;
    case OP_NOT_EQUAL: return v != q1;
    case OP_LESS: return v < q1;
    case OP_GREATER: return v > q1;
    case OP_LESS_EQ: return v <= q1;
    case OP_GREATER_EQ: return v >= q1;
    case OP_BETWEEN: {
        int lo = (cc && cc->raw && cc->raw->min_exclusive) ? (v > q1) : (v >= q1);
        int hi = (cc && cc->raw && cc->raw->max_exclusive) ? (v < q2) : (v <= q2);
        return lo && hi;
    }
    case OP_IN:
        for (int i = 0; i < in_count; i++) if (v == in_list[i]) return 1;
        return 0;
    case OP_NOT_IN:
        for (int i = 0; i < in_count; i++) if (v == in_list[i]) return 0;
        return 1;
    default: return 0;
    }
}

/* Generic 3-way comparator for a typed field given two pointers into the
   same record. Returns negative / 0 / positive. Both pointers must reference
   the same TypedField type — compile_one enforces this for field-vs-field
   ops by setting rhs_tf only when types match. */
static int cmp_typed_field_pair(const uint8_t *a, const uint8_t *b,
                                const TypedField *f) {
    switch (f->type) {
    case FT_VARCHAR: {
        int la = varchar_eff_len(a, f->size);
        int lb = varchar_eff_len(b, f->size);
        size_t n = la < lb ? (size_t)la : (size_t)lb;
        int r = memcmp(a + 2, b + 2, n);
        if (r != 0) return r;
        return la - lb;
    }
    case FT_LONG:    { int64_t va = ld_be_i64(a), vb = ld_be_i64(b);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_INT:     { int32_t va = ld_be_i32(a), vb = ld_be_i32(b);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_SHORT:   { int16_t va = ld_be_i16(a), vb = ld_be_i16(b);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_NUMERIC: { int64_t va = ld_be_i64(a), vb = ld_be_i64(b);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_DOUBLE:  { double va, vb; memcpy(&va, a, 8); memcpy(&vb, b, 8);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_FLOAT:   { float va, vb; memcpy(&va, a, 4); memcpy(&vb, b, 4);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_BOOL:
    case FT_BYTE:    return (int)a[0] - (int)b[0];
    case FT_DATE:    { int32_t va = ld_be_i32(a), vb = ld_be_i32(b);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_DATETIME: {
        int64_t va = (int64_t)ld_be_i32(a) * 100000LL + ld_be_u16(a + 4);
        int64_t vb = (int64_t)ld_be_i32(b) * 100000LL + ld_be_u16(b + 4);
        return va < vb ? -1 : (va > vb ? 1 : 0);
    }
    case FT_DATETIMEMS: {
        int64_t va = (int64_t)ld_be_i32(a) * 100000000LL + (int64_t)ld_be_u32(a + 4);
        int64_t vb = (int64_t)ld_be_i32(b) * 100000000LL + (int64_t)ld_be_u32(b + 4);
        return va < vb ? -1 : (va > vb ? 1 : 0);
    }
    case FT_TIME: {
        uint32_t va = ((uint32_t)a[0] << 16) | ((uint32_t)a[1] << 8) | a[2];
        uint32_t vb = ((uint32_t)b[0] << 16) | ((uint32_t)b[1] << 8) | b[2];
        return va < vb ? -1 : (va > vb ? 1 : 0);
    }
    case FT_UUID:
        return memcmp(a, b, 16);
    case FT_IPV4:
        return memcmp(a, b, 4);
    case FT_IPV6:
        return memcmp(a, b, 16);
    default: return 0;
    }
}

static int field_vs_field_match(int cmp, enum SearchOp op) {
    switch (op) {
    case OP_EQ_FIELD:  return cmp == 0;
    case OP_NEQ_FIELD: return cmp != 0;
    case OP_LT_FIELD:  return cmp <  0;
    case OP_GT_FIELD:  return cmp >  0;
    case OP_LTE_FIELD: return cmp <= 0;
    case OP_GTE_FIELD: return cmp >= 0;
    default: return 0;
    }
}

/* Hot-path match: typed binary compare against pre-compiled criterion.
   `rec` points at the raw value region of the record (hdr->key_len offset
   into block). Returns 1 on match, 0 otherwise.
   For composite/unknown fields, falls back to decode_field + match_criterion. */
int match_typed(const uint8_t *rec, const CompiledCriterion *cc, FieldSchema *fs) {
    if (cc->composite || !cc->tf) {
        char *attr = decode_field((const char *)rec, 0, cc->raw->field, fs);
        int r = match_criterion(attr, cc->raw);
        free(attr);
        return r;
    }

    /* Field-vs-field: compare LHS bytes against the sibling RHS field on
       the same record. cc->rhs_tf is NULL when types didn't match at
       compile time — every record fails (caller's fault). */
    if (cc->op == OP_EQ_FIELD || cc->op == OP_NEQ_FIELD ||
        cc->op == OP_LT_FIELD || cc->op == OP_GT_FIELD ||
        cc->op == OP_LTE_FIELD || cc->op == OP_GTE_FIELD) {
        if (!cc->rhs_tf) return 0;
        int r = cmp_typed_field_pair(rec + cc->tf->offset,
                                     rec + cc->rhs_tf->offset, cc->tf);
        return field_vs_field_match(r, cc->op);
    }

    const TypedField *f = cc->tf;
    const uint8_t *p = rec + f->offset;

    switch (f->type) {
    case FT_NONE:
    case FT_COUNT:      /* sentinel — never a real field type. Listed (rather
                           than a default:) so -Wswitch keeps flagging this
                           switch when a new FieldType is added. */
        return 0;
    case FT_VARCHAR:
        return match_typed_varchar(p, f->size, cc);
    case FT_LONG:
    case FT_TIMESTAMP: {
        int64_t v = ld_be_i64(p);
        return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
    }
    case FT_INT: {
        int64_t v = (int64_t)ld_be_i32(p);
        return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
    }
    case FT_SHORT: {
        int64_t v = (int64_t)ld_be_i16(p);
        return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
    }
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
    }
    case FT_DOUBLE: {
        double v; memcpy(&v, p, 8);
        return cmp_op_f64(v, cc->d1, cc->d2, cc->op, cc->in_f64, cc->in_count, cc);
    }
    case FT_FLOAT: {
        float v; memcpy(&v, p, 4);
        /* Comparisons use float32 precision throughout: cast query literals to
         * float before comparing so that a stored value of 3.14f (encoded as
         * 3.140000104... in double) is not incorrectly excluded by `lte 3.14`
         * (where the double literal 3.14 < 3.140000104...).  The btree
         * encoding path stores the literal as float32, so record and query
         * round identically there; the scan path must do the same. */
        float qf1 = (float)cc->d1;
        float qf2 = (float)cc->d2;
        switch (cc->op) {
        case OP_EQUAL:      return v == qf1;
        case OP_NOT_EQUAL:  return v != qf1;
        case OP_LESS:       return v <  qf1;
        case OP_GREATER:    return v >  qf1;
        case OP_LESS_EQ:    return v <= qf1;
        case OP_GREATER_EQ: return v >= qf1;
        case OP_BETWEEN: {
            int lo = (cc->raw && cc->raw->min_exclusive) ? (v > qf1) : (v >= qf1);
            int hi = (cc->raw && cc->raw->max_exclusive) ? (v < qf2) : (v <= qf2);
            return lo && hi;
        }
        case OP_IN: {
            for (int i = 0; i < cc->in_count; i++)
                if (v == (float)cc->in_f64[i]) return 1;
            return 0;
        }
        case OP_NOT_IN: {
            for (int i = 0; i < cc->in_count; i++)
                if (v == (float)cc->in_f64[i]) return 0;
            return 1;
        }
        default: return 0;
        }
    }
    case FT_BOOL: case FT_BYTE: {
        int64_t v = (int64_t)p[0];
        int64_t q = (int64_t)cc->b1;
        switch (cc->op) {
        case OP_EXISTS: return 1;
        case OP_NOT_EXISTS: return 0;
        case OP_EQUAL: return v == q;
        case OP_NOT_EQUAL: return v != q;
        case OP_LESS: return v < q;
        case OP_GREATER: return v > q;
        case OP_LESS_EQ: return v <= q;
        case OP_GREATER_EQ: return v >= q;
        case OP_IN: {
            for (int i = 0; i < cc->in_count; i++)
                if (v == cc->in_i64[i]) return 1;
            return 0;
        }
        case OP_NOT_IN: {
            for (int i = 0; i < cc->in_count; i++)
                if (v == cc->in_i64[i]) return 0;
            return 1;
        }
        /* OP_BETWEEN on bool/byte not compiled — CompiledCriterion only
           carries one byte bound; in practice bool BETWEEN is degenerate
           (just IN [true,false]) and byte BETWEEN can be expressed as
           a pair of GTE+LTE via the existing range coalescer. Falls
           through to default → 0. */
        default: return 0;
        }
    }
    case FT_DATE: {
        int64_t v = (int64_t)ld_be_i32(p);
        switch (cc->op) {
        case OP_EXISTS: return v != 0;
        case OP_NOT_EXISTS: return v == 0;
        default:
            if (v == 0) return 0; /* empty date matches nothing except NOT_EXISTS */
            if (cc->op == OP_IN || cc->op == OP_NOT_IN)
                return cmp_op_i64(v, 0, 0, cc->op, cc->in_i64, cc->in_count, cc);
            return cmp_op_i64(v, cc->i1, cc->i2, cc->op, NULL, 0, cc);
        }
    }
    case FT_DATETIME: {
        int64_t d = (int64_t)ld_be_i32(p);
        uint16_t t = ld_be_u16(p + 4);
        /* Compose into single int64 for ordered compare: date*100000 + seconds */
        int64_t v = d * 100000LL + (int64_t)t;
        int64_t q1 = cc->i1 * 100000LL + (int64_t)cc->t1;
        int64_t q2 = cc->i2 * 100000LL + (int64_t)cc->t2;
        switch (cc->op) {
        case OP_EXISTS: return !(d == 0 && t == 0);
        case OP_NOT_EXISTS: return d == 0 && t == 0;
        default:
            if (d == 0 && t == 0) return 0;
            return cmp_op_i64(v, q1, q2, cc->op, NULL, 0, cc);
        }
    }
    case FT_DATETIMEMS: {
        int64_t d = (int64_t)ld_be_i32(p);
        uint32_t ms = ld_be_u32(p + 4);
        int64_t v = d * 100000000LL + (int64_t)ms;
        int64_t q1 = cc->i1 * 100000000LL + (int64_t)(uint32_t)cc->dm1;
        int64_t q2 = cc->i2 * 100000000LL + (int64_t)(uint32_t)cc->dm2;
        switch (cc->op) {
        case OP_EXISTS: return !(d == 0 && ms == 0);
        case OP_NOT_EXISTS: return d == 0 && ms == 0;
        default:
            if (d == 0 && ms == 0) return 0;
            return cmp_op_i64(v, q1, q2, cc->op, NULL, 0, cc);
        }
    }
    case FT_TIME: {
        uint32_t secs = ((uint32_t)p[0] << 16) | ((uint32_t)p[1] << 8) | p[2];
        uint32_t q1 = ((uint32_t)cc->time_val[0] << 16) | ((uint32_t)cc->time_val[1] << 8) | cc->time_val[2];
        uint32_t q2 = ((uint32_t)cc->time_val2[0] << 16) | ((uint32_t)cc->time_val2[1] << 8) | cc->time_val2[2];
        int exists = !(secs == 0 && p[0]==0 && p[1]==0 && p[2]==0);
        switch (cc->op) {
        case OP_EXISTS: return exists;
        case OP_NOT_EXISTS: return !exists;
        case OP_EQUAL: return exists && secs == q1;
        case OP_NOT_EQUAL: return !exists || secs != q1;
        case OP_LESS: return exists && secs < q1;
        case OP_GREATER: return exists && secs > q1;
        case OP_LESS_EQ: return !exists || secs <= q1;
        case OP_GREATER_EQ: return exists && secs >= q1;
        case OP_BETWEEN: {
            if (!exists) return 0;
            int lo = cc->i1;
            int hi = cc->i2;
            if (lo && secs < q1) return 0;
            if (hi && secs > q2) return 0;
            return 1;
        }
        case OP_IN: case OP_NOT_IN: {
            if (!exists) return cc->op == OP_NOT_IN;
            int found = 0;
            for (int i = 0; i < cc->in_count; i++) {
                uint32_t in_val = ((uint32_t)cc->in_time[i][0] << 16) | ((uint32_t)cc->in_time[i][1] << 8) | cc->in_time[i][2];
                if (secs == in_val) { found = 1; break; }
            }
            return cc->op == OP_IN ? found : !found;
        }
        default: return 0;
        }
    }
    case FT_UUID: {
        int exists = !(p[0]==0 && p[1]==0 && p[2]==0 && p[3]==0 &&
                       p[4]==0 && p[5]==0 && p[6]==0 && p[7]==0 &&
                       p[8]==0 && p[9]==0 && p[10]==0 && p[11]==0 &&
                       p[12]==0 && p[13]==0 && p[14]==0 && p[15]==0);
        switch (cc->op) {
        case OP_EXISTS: return exists;
        case OP_NOT_EXISTS: return !exists;
        case OP_EQUAL: return exists && memcmp(p, cc->uuid_bytes, 16) == 0;
        case OP_NOT_EQUAL: return !exists || memcmp(p, cc->uuid_bytes, 16) != 0;
        case OP_LESS: return exists && memcmp(p, cc->uuid_bytes, 16) < 0;
        case OP_GREATER: return exists && memcmp(p, cc->uuid_bytes, 16) > 0;
        case OP_LESS_EQ: return !exists || memcmp(p, cc->uuid_bytes, 16) <= 0;
        case OP_GREATER_EQ: return exists && memcmp(p, cc->uuid_bytes, 16) >= 0;
        case OP_BETWEEN: {
            if (!exists) return 0;
            int lo = cc->i1;  /* using i1 as between flag */
            int hi = cc->i2;  /* using i2 as between flag */
            if (lo && memcmp(p, cc->uuid_bytes, 16) < 0) return 0;
            if (hi && memcmp(p, cc->uuid_bytes2, 16) > 0) return 0;
            return 1;
        }
        case OP_IN: case OP_NOT_IN: {
            if (!exists) return cc->op == OP_NOT_IN;
            int found = 0;
            for (int i = 0; i < cc->in_count; i++) {
                if (memcmp(p, cc->in_uuid[i], 16) == 0) { found = 1; break; }
            }
            return cc->op == OP_IN ? found : !found;
        }
        default: return 0;
        }
    }
    case FT_IPV4: {
        int exists = !(p[0]==0 && p[1]==0 && p[2]==0 && p[3]==0);
        switch (cc->op) {
        case OP_EXISTS: return exists;
        case OP_NOT_EXISTS: return !exists;
        case OP_EQUAL: return exists && memcmp(p, cc->ipv4_val, 4) == 0;
        case OP_NOT_EQUAL: return !exists || memcmp(p, cc->ipv4_val, 4) != 0;
        case OP_LESS: return exists && memcmp(p, cc->ipv4_val, 4) < 0;
        case OP_GREATER: return exists && memcmp(p, cc->ipv4_val, 4) > 0;
        case OP_LESS_EQ: return !exists || memcmp(p, cc->ipv4_val, 4) <= 0;
        case OP_GREATER_EQ: return exists && memcmp(p, cc->ipv4_val, 4) >= 0;
        case OP_BETWEEN: {
            if (!exists) return 0;
            int lo = cc->i1;
            int hi = cc->i2;
            if (lo && memcmp(p, cc->ipv4_val, 4) < 0) return 0;
            if (hi && memcmp(p, cc->ipv4_val2, 4) > 0) return 0;
            return 1;
        }
        case OP_IN: case OP_NOT_IN: {
            if (!exists) return cc->op == OP_NOT_IN;
            int found = 0;
            for (int i = 0; i < cc->in_count; i++) {
                if (memcmp(p, cc->in_ipv4[i], 4) == 0) { found = 1; break; }
            }
            return cc->op == OP_IN ? found : !found;
        }
        default: return 0;
        }
    }
    case FT_IPV6: {
        int exists = 0;
        for (int bi = 0; bi < 16; bi++) if (p[bi] != 0) { exists = 1; break; }
        switch (cc->op) {
        case OP_EXISTS: return exists;
        case OP_NOT_EXISTS: return !exists;
        case OP_EQUAL: return exists && memcmp(p, cc->ipv6_val, 16) == 0;
        case OP_NOT_EQUAL: return !exists || memcmp(p, cc->ipv6_val, 16) != 0;
        case OP_LESS: return exists && memcmp(p, cc->ipv6_val, 16) < 0;
        case OP_GREATER: return exists && memcmp(p, cc->ipv6_val, 16) > 0;
        case OP_LESS_EQ: return !exists || memcmp(p, cc->ipv6_val, 16) <= 0;
        case OP_GREATER_EQ: return exists && memcmp(p, cc->ipv6_val, 16) >= 0;
        case OP_BETWEEN: {
            if (!exists) return 0;
            int lo = cc->i1;
            int hi = cc->i2;
            if (lo && memcmp(p, cc->ipv6_val, 16) < 0) return 0;
            if (hi && memcmp(p, cc->ipv6_val2, 16) > 0) return 0;
            return 1;
        }
        case OP_IN: case OP_NOT_IN: {
            if (!exists) return cc->op == OP_NOT_IN;
            int found = 0;
            for (int i = 0; i < cc->in_count; i++) {
                if (memcmp(p, cc->in_ipv6[i], 16) == 0) { found = 1; break; }
            }
            return cc->op == OP_IN ? found : !found;
        }
        default: return 0;
        }
    }
    case FT_ENUM: {
        /* Encoded byte index → enum_values lookup → criterion match.
           compile_one (FT_ENUM case in phase 3) pre-resolves c->value
           to its byte index in cc->i1, and (for IN/NOT_IN) c->in_values
           to cc->in_i64. Here we just decode the record bytes and
           compare integer indices. -1 means "no such value in dict"
           — matches OP_NOT_EQUAL, OP_NOT_IN, OP_NOT_EXISTS only. */
        if (!f->enum_values || f->n_enum_values <= 0) return 0;
        int64_t v;
        if (f->enum_width == 2) {
            v = (int64_t)(((uint16_t)p[0] << 8) | (uint16_t)p[1]);
        } else {
            v = (int64_t)p[0];
        }
        /* The literal byte 0x00 is a legit enum index. EXISTS is
           always 1 because every record gets a valid index on insert. */
        switch (cc->op) {
        case OP_EXISTS:     return 1;
        case OP_NOT_EXISTS: return 0;
        case OP_EQUAL:      return cc->i1 >= 0 && v == cc->i1;
        case OP_NOT_EQUAL:  return cc->i1 < 0 || v != cc->i1;
        case OP_LESS:       return cc->i1 >= 0 && v <  cc->i1;
        case OP_GREATER:    return cc->i1 >= 0 && v >  cc->i1;
        case OP_LESS_EQ:    return cc->i1 >= 0 && v <= cc->i1;
        case OP_GREATER_EQ: return cc->i1 >= 0 && v >= cc->i1;
        case OP_IN: {
            for (int i = 0; i < cc->in_count; i++)
                if (cc->in_i64[i] >= 0 && v == cc->in_i64[i]) return 1;
            return 0;
        }
        case OP_NOT_IN: {
            for (int i = 0; i < cc->in_count; i++)
                if (cc->in_i64[i] >= 0 && v == cc->in_i64[i]) return 0;
            return 1;
        }
        default: return 0;
        }
    }
    }
    return 0;
}

/* ========== Criteria parsers + tree planner ========== */
/* ========== Reusable criteria parser ========== */

/* Parse a single criterion object {"field":"x","op":"eq","value":"y"} into *c.
   Helper for parse_criteria_json. Called in a loop over every element of
   the criteria array; parsing the sub-object once and indexing into the
   JsonObj saves 5 walks per criterion over the previous per-field parse. */
/* Returns 1 if the op requires a value operand, 0 for existence-only ops. */
static int op_requires_value(enum SearchOp op) {
    return op != OP_EXISTS && op != OP_NOT_EXISTS;
}

/* Parse one criterion leaf from a JSON object buffer.
   Returns 0 on success, -1 on error (missing field, missing/unknown op,
   missing value for ops that require one, or missing value2 for
   between/len_between).
   On error, c is zeroed. */
static int parse_one_criterion(const char *obj_buf, SearchCriterion *c) {
    memset(c, 0, sizeof(*c));

    JsonObj cobj;
    json_parse_object(obj_buf, strlen(obj_buf), &cobj);

    char *f     = json_obj_strdup(&cobj, "field");
    char *o     = json_obj_strdup(&cobj, "op");
    if (!o) o = json_obj_strdup(&cobj, "operator");   /* alias */
    char *v     = json_obj_strdup(&cobj, "value");
    char *v_raw = json_obj_strdup_raw(&cobj, "value");
    char *v2    = json_obj_strdup(&cobj, "value2");

    /* Validate field — every criterion must name a field */
    if (!f || f[0] == '\0') { free(f); free(o); free(v); free(v_raw); free(v2); return -1; }
    strncpy(c->field, f, 255); free(f);

    /* Validate op — must be present and recognised */
    if (o) {
        c->op = parse_op(o);
        free(o);
        if (c->op == OP_UNKNOWN) { free(v); free(v_raw); free(v2); return -1; }
    } else {
        free(v); free(v_raw); free(v2);
        return -1;   /* neither "op" nor "operator" present */
    }

    /* between/len_between accept the two bounds either as separate
       "value"/"value2" keys, or as a single two-element array in "value"
       (legacy array-form input, e.g. "value":["25","30"]). Split the
       array form into value/value2 before the required-value checks
       below, so both input shapes are validated and matched identically. */
    if ((c->op == OP_BETWEEN || c->op == OP_LEN_BETWEEN) && !v2 && v && v[0] == '[') {
        const char *ap = v + 1;
        char parts[2][1024] = {{0}};
        int pi = 0;
        while (*ap && pi < 2) {
            while (*ap == ' ' || *ap == ',') ap++;
            if (*ap == ']' || !*ap) break;
            const char *start; size_t plen;
            if (*ap == '"') {
                ap++;
                start = ap;
                while (*ap && *ap != '"') ap++;
                plen = (size_t)(ap - start);
                if (*ap == '"') ap++;
            } else {
                start = ap;
                while (*ap && *ap != ',' && *ap != ']') ap++;
                plen = (size_t)(ap - start);
            }
            if (plen >= sizeof(parts[0])) plen = sizeof(parts[0]) - 1;
            memcpy(parts[pi], start, plen);
            parts[pi][plen] = '\0';
            pi++;
        }
        if (pi == 2 && parts[0][0] && parts[1][0]) {
            free(v);
            v = strdup(parts[0]);
            v2 = strdup(parts[1]);
        }
    }

    /* Validate value — required for all ops except exists/not_exists */
    if (op_requires_value(c->op) && (!v || v[0] == '\0')) {
        free(v); free(v_raw); free(v2);
        return -1;
    }

    /* Validate value2 — between/len_between need both bounds; a missing
       value2 must not silently fall back to an empty-string bound. */
    if ((c->op == OP_BETWEEN || c->op == OP_LEN_BETWEEN) && (!v2 || v2[0] == '\0')) {
        free(v); free(v_raw); free(v2);
        return -1;
    }
    if (v) {
        strncpy(c->value, v, sizeof(c->value) - 1);
        /* LIKE/NOT_LIKE accept '*' as an alias for '%'. Normalize once
           so both the typed and legacy match paths see a single form. */
        if (c->op == OP_LIKE || c->op == OP_NOT_LIKE) {
            for (char *q = c->value; *q; q++) if (*q == '*') *q = '%';
        }
        if (c->op == OP_IN || c->op == OP_NOT_IN) {
            c->in_cap = 64;
            c->in_values = malloc(c->in_cap * sizeof(char *));
            const char *ap = v_raw ? v_raw : v;
            if (*ap == '"') ap++;
            if (*ap == '[') {
                ap++;
                while (*ap) {
                    while (*ap == ' ' || *ap == ',') ap++;
                    /* The skip-ws/comma loop can advance ap to the NUL
                       terminator if the input ends with a trailing comma
                       and no closing ']' (the upstream json_skip_value
                       can be tricked into truncating the span at an
                       embedded NUL — see fuzzer-found bug). Without this
                       guard, the else `ap++` below walks past NUL → OOB
                       read on next iteration. */
                    if (!*ap) break;
                    if (*ap == ']') break;
                    if (*ap == '"') {
                        ap++;
                        const char *start = ap;
                        while (*ap && *ap != '"') ap++;
                        size_t len = ap - start;
                        if (c->in_count >= c->in_cap) {
                            int new_cap = c->in_cap * 2;
                            char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                            if (!t) { c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                            c->in_values = t;
                            c->in_cap = new_cap;
                        }
                        char *val = malloc(len + 1);
                        memcpy(val, start, len); val[len] = '\0';
                        c->in_values[c->in_count++] = val;
                        if (*ap == '"') ap++;
                    } else {
                        const char *start = ap;
                        while (*ap && *ap != ',' && *ap != ']' && *ap != ' ') ap++;
                        size_t len = ap - start;
                        if (len > 0) {
                            if (c->in_count >= c->in_cap) {
                                int new_cap = c->in_cap * 2;
                                char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                                if (!t) { c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                                c->in_values = t;
                                c->in_cap = new_cap;
                            }
                            char *val = malloc(len + 1);
                            memcpy(val, start, len); val[len] = '\0';
                            c->in_values[c->in_count++] = val;
                        }
                    }
                }
            } else {
                char *iv = strdup(v);
                char *_tok_save = NULL; char *tok = strtok_r(iv, ",", &_tok_save);
                while (tok) {
                    if (c->in_count >= c->in_cap) {
                        int new_cap = c->in_cap * 2;
                        char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                        if (!t) { c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                        c->in_values = t;
                        c->in_cap = new_cap;
                    }
                    c->in_values[c->in_count++] = strdup(tok);
                    tok = strtok_r(NULL, ",", &_tok_save);
                }
                free(iv);
            }
        }
        free(v);
    }
    free(v_raw);
    if (v2) { strncpy(c->value2, v2, sizeof(c->value2) - 1); free(v2); }
    if (op_is_length(c->op)) {
        c->len_target = strtoll(c->value, NULL, 10);
        if (c->op == OP_LEN_BETWEEN)
            c->len_target2 = strtoll(c->value2, NULL, 10);
    }
    return 0;
}

/* Parse criteria from JSON — supports two forms:
   Array form:  [{"field":"x","op":"eq","value":"y"}, ...]
   Simple form: {"status":"pending","city":"London"}  (all equality)
   Returns heap-allocated array in *out, count in *count. Caller must free_criteria(). */
int parse_criteria_json(const char *json, SearchCriterion **out, int *count) {
    const char *p = json_skip(json);

    if (*p == '[') {
        /* Array form — same as old cmd_find inline parser */
        SearchCriterion *criteria = calloc(64, sizeof(SearchCriterion));
        int n = 0;
        p++;
        while (*p && n < 64) {
            p = json_skip(p);
            if (*p == ']') break;
            if (*p == ',') { p++; continue; }
            if (*p != '{') { p++; continue; }

            const char *obj_start = p;
            const char *obj_end = json_skip_value(p);
            size_t obj_len = obj_end - obj_start;
            char obj_buf[MAX_LINE];
            if (obj_len >= sizeof(obj_buf)) { p = obj_end; continue; }
            memcpy(obj_buf, obj_start, obj_len);
            obj_buf[obj_len] = '\0';

            if (parse_one_criterion(obj_buf, &criteria[n]) != 0) {
                free_criteria(criteria, n);
                *out = NULL;
                *count = 0;
                return -1;
            }
            n++;
            p = obj_end;
        }
        *out = criteria;
        *count = n;
        return 0;
    } else if (*p == '{') {
        /* Simple equality form: {"field1":"val1","field2":"val2"} */
        SearchCriterion *criteria = calloc(64, sizeof(SearchCriterion));
        int n = 0;
        p++;
        while (*p && n < 64) {
            p = json_skip(p);
            if (*p == '}') break;
            if (*p == ',') { p++; continue; }
            if (*p != '"') { p++; continue; }

            /* Parse key */
            p++;
            const char *fname = p;
            while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
            size_t flen = p - fname;
            if (*p == '"') p++;

            p = json_skip(p);
            if (*p == ':') p++;
            p = json_skip(p);

            /* Parse value */
            const char *vstart = p;
            const char *vend = json_skip_value(p);
            size_t vlen = vend - vstart;

            SearchCriterion *c = &criteria[n];
            memset(c, 0, sizeof(*c));
            if (flen > 255) flen = 255;
            memcpy(c->field, fname, flen);
            c->field[flen] = '\0';
            c->op = OP_EQUAL;

            /* Strip quotes from value */
            if (vlen >= 2 && *vstart == '"' && *(vend - 1) == '"') {
                vlen -= 2; vstart++;
            }
            if (vlen > sizeof(c->value) - 1) vlen = sizeof(c->value) - 1;
            memcpy(c->value, vstart, vlen);
            c->value[vlen] = '\0';

            n++;
            p = vend;
        }
        *out = criteria;
        *count = n;
        return 0;
    }

    *out = NULL;
    *count = 0;
    return -1;
}

void free_criteria(SearchCriterion *c, int count) {
    if (!c) return;
    for (int i = 0; i < count; i++) {
        if (c[i].in_values) {
            for (int j = 0; j < c[i].in_count; j++) free(c[i].in_values[j]);
            free(c[i].in_values);
        }
    }
    free(c);
}

/* ========== CriteriaNode tree parser (AND/OR composition) ========== */

static CriteriaNode *cnode_new(CriteriaNodeKind kind) {
    CriteriaNode *n = calloc(1, sizeof(CriteriaNode));
    if (n) n->kind = kind;
    return n;
}

static int cnode_append(CriteriaNode *parent, CriteriaNode *child) {
    CriteriaNode **nc = realloc(parent->children,
                                (parent->n_children + 1) * sizeof(CriteriaNode *));
    if (!nc) return -1;
    parent->children = nc;
    parent->children[parent->n_children++] = child;
    return 0;
}

void free_criteria_tree(CriteriaNode *n) {
    if (!n) return;
    if (n->kind == CNODE_LEAF) {
        if (n->leaf.in_values) {
            for (int i = 0; i < n->leaf.in_count; i++) free(n->leaf.in_values[i]);
            free(n->leaf.in_values);
        }
        if (n->compiled) {
            free_compiled_criteria(n->compiled, 1);
        }
    } else {
        for (int i = 0; i < n->n_children; i++) free_criteria_tree(n->children[i]);
        free(n->children);
    }
    free(n);
}

void compile_criteria_tree(CriteriaNode *n, const TypedSchema *ts) {
    if (!n) return;
    if (n->kind == CNODE_LEAF) {
        if (!n->compiled) {
            n->compiled = calloc(1, sizeof(CompiledCriterion));
            if (n->compiled) compile_one(n->compiled, &n->leaf, ts);
        }
        return;
    }
    for (int i = 0; i < n->n_children; i++) compile_criteria_tree(n->children[i], ts);
}

/* Drop cached CompiledCriterion state recursively so the next
   compile_criteria_tree() call rebuilds from the (possibly mutated)
   SearchCriterion in each leaf. Used by the NEQ aggregate shortcut when
   it temporarily flips the leaf's op to OP_EQUAL — without this the
   compiled cache keeps the original op and match_typed misclassifies
   every record. */
void recompile_criteria_tree(CriteriaNode *n, const TypedSchema *ts) {
    if (!n) return;
    if (n->kind == CNODE_LEAF) {
        if (n->compiled) {
            /* free_compiled_criteria frees inner buffers AND the array
               allocation, so don't free(n->compiled) again. */
            free_compiled_criteria(n->compiled, 1);
            n->compiled = NULL;
        }
        n->compiled = calloc(1, sizeof(CompiledCriterion));
        if (n->compiled) compile_one(n->compiled, &n->leaf, ts);
        return;
    }
    for (int i = 0; i < n->n_children; i++) recompile_criteria_tree(n->children[i], ts);
}

int criteria_match_tree(const uint8_t *rec, const CriteriaNode *n, FieldSchema *fs) {
    if (!n) return 1;
    switch (n->kind) {
    case CNODE_LEAF:
        if (!n->compiled) return 0;
        return match_typed(rec, n->compiled, fs);
    case CNODE_AND:
        for (int i = 0; i < n->n_children; i++)
            if (!criteria_match_tree(rec, n->children[i], fs)) return 0;
        return 1;
    case CNODE_OR:
        for (int i = 0; i < n->n_children; i++)
            if (criteria_match_tree(rec, n->children[i], fs)) return 1;
        return 0;
    }
    return 0;
}

/* Forward decl — parse_tree_element and parse_tree_array recurse through each other. */
static CriteriaNode *parse_tree_element(const char *obj_buf, int depth, const char **err);

/* Parse the children of an array `[elem, elem, ...]` into parent. `arr_p` must
   point at the opening '['. Each element is a leaf or a branch object. */
static int parse_tree_array(const char *arr_p, CriteriaNode *parent,
                            int depth, const char **err) {
    if (depth > MAX_CRITERIA_DEPTH) {
        if (err) *err = "nesting too deep (max 16)";
        return -1;
    }
    const char *p = json_skip(arr_p);
    if (*p != '[') { if (err) *err = "expected array"; return -1; }
    p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p != '{') { if (err) *err = "expected object in criteria array"; return -1; }

        const char *obj_start = p;
        const char *obj_end = json_skip_value(p);
        size_t obj_len = obj_end - obj_start;
        char *obj_buf = malloc(obj_len + 1);
        if (!obj_buf) { if (err) *err = "out of memory"; return -1; }
        memcpy(obj_buf, obj_start, obj_len);
        obj_buf[obj_len] = '\0';

        CriteriaNode *child = parse_tree_element(obj_buf, depth + 1, err);
        free(obj_buf);
        if (!child) return -1;

        if (cnode_append(parent, child) != 0) {
            free_criteria_tree(child);
            if (err) *err = "out of memory";
            return -1;
        }
        p = obj_end;
    }
    return 0;
}

/* Parse a single element: either a branch `{or:[...]}` / `{and:[...]}` or a leaf
   `{field,op,value,...}`. Returns newly-allocated node or NULL on error. */
static CriteriaNode *parse_tree_element(const char *obj_buf, int depth,
                                        const char **err) {
    if (depth > MAX_CRITERIA_DEPTH) {
        if (err) *err = "nesting too deep (max 16)";
        return NULL;
    }

    JsonObj tobj;
    json_parse_object(obj_buf, strlen(obj_buf), &tobj);
    char *or_arr  = json_obj_strdup_raw(&tobj, "or");
    char *and_arr = or_arr ? NULL : json_obj_strdup_raw(&tobj, "and");

    if (or_arr || and_arr) {
        CriteriaNode *n = cnode_new(or_arr ? CNODE_OR : CNODE_AND);
        if (!n) { free(or_arr); free(and_arr); if (err) *err = "out of memory"; return NULL; }
        const char *arr = or_arr ? or_arr : and_arr;
        if (parse_tree_array(arr, n, depth, err) != 0) {
            free_criteria_tree(n); free(or_arr); free(and_arr);
            return NULL;
        }
        if (n->n_children == 0) {
            free_criteria_tree(n); free(or_arr); free(and_arr);
            if (err) *err = "empty or/and";
            return NULL;
        }
        free(or_arr); free(and_arr);
        return n;
    }

    CriteriaNode *n = cnode_new(CNODE_LEAF);
    if (!n) { if (err) *err = "out of memory"; return NULL; }
    if (parse_one_criterion(obj_buf, &n->leaf) != 0) {
        free_criteria_tree(n);
        if (err) *err = "invalid criterion: missing field, op or value";
        return NULL;
    }
    if (n->leaf.field[0] == '\0') {
        free_criteria_tree(n);
        if (err) *err = "leaf missing 'field'";
        return NULL;
    }
    return n;
}

/* Tree-rewrite pre-pass: collapse paired same-field range bounds under an
 * AND into a single OP_BETWEEN leaf with explicit min/max exclusivity flags
 * so the planner can drive a tightly-bounded btree_range_ex walk instead of
 * intersecting two unbounded KeySets.
 *
 * Why this matters: choose_primary_source prefers PRIMARY_INTERSECT whenever
 * 2+ AND'd indexed range leaves exist. Without this rewrite, criteria like
 *   [{"field":"d","op":"gte","value":"X"}, {"field":"d","op":"lte","value":"Y"}]
 * become two leaves on the SAME field, and the intersect path builds two
 * separate KeySets — leaf 1 walks d >= X (potentially the whole btree),
 * leaf 2 walks d <= Y (also potentially the whole btree), then intersects.
 * Two full btree walks for what should be a single bounded range.
 *
 * The rewrite handles all four lower×upper pairings on the same field:
 *   gte + lte → BETWEEN(min, max), both inclusive
 *   gt  + lte → BETWEEN(min, max), min_exclusive=1
 *   gte + lt  → BETWEEN(min, max), max_exclusive=1
 *   gt  + lt  → BETWEEN(min, max), both exclusive
 *
 * Recursive into children so nested AND blocks under an OR get the same
 * treatment. */
static void coalesce_same_field_ranges(CriteriaNode *node) {
    if (!node) return;
    for (int i = 0; i < node->n_children; i++)
        coalesce_same_field_ranges(node->children[i]);
    if (node->kind != CNODE_AND) return;

    for (int i = 0; i < node->n_children; i++) {
        CriteriaNode *a = node->children[i];
        if (!a || a->kind != CNODE_LEAF) continue;
        int a_lower = (a->leaf.op == OP_GREATER_EQ || a->leaf.op == OP_GREATER);
        int a_upper = (a->leaf.op == OP_LESS_EQ    || a->leaf.op == OP_LESS);
        if (!a_lower && !a_upper) continue;

        for (int j = i + 1; j < node->n_children; j++) {
            CriteriaNode *b = node->children[j];
            if (!b || b->kind != CNODE_LEAF) continue;
            if (strcmp(a->leaf.field, b->leaf.field) != 0) continue;
            int b_lower = (b->leaf.op == OP_GREATER_EQ || b->leaf.op == OP_GREATER);
            int b_upper = (b->leaf.op == OP_LESS_EQ    || b->leaf.op == OP_LESS);
            /* Need exactly one lower and one upper to combine into BETWEEN. */
            if (!((a_lower && b_upper) || (a_upper && b_lower))) continue;

            CriteriaNode *low = a_lower ? a : b;
            CriteriaNode *high = a_upper ? a : b;
            int min_excl = (low->leaf.op  == OP_GREATER); /* GT exclusive, GTE inclusive */
            int max_excl = (high->leaf.op == OP_LESS);    /* LT exclusive, LTE inclusive */

            /* Snapshot bounds before mutating either leaf. */
            char gv[sizeof(a->leaf.value)];
            char hv[sizeof(a->leaf.value)];
            strncpy(gv, low->leaf.value,  sizeof(gv) - 1); gv[sizeof(gv) - 1] = '\0';
            strncpy(hv, high->leaf.value, sizeof(hv) - 1); hv[sizeof(hv) - 1] = '\0';

            /* Rewrite a as BETWEEN(low, high) with the right exclusivity. */
            a->leaf.op = OP_BETWEEN;
            strncpy(a->leaf.value,  gv, sizeof(a->leaf.value)  - 1);
            a->leaf.value[sizeof(a->leaf.value)   - 1] = '\0';
            strncpy(a->leaf.value2, hv, sizeof(a->leaf.value2) - 1);
            a->leaf.value2[sizeof(a->leaf.value2) - 1] = '\0';
            a->leaf.min_exclusive = min_excl;
            a->leaf.max_exclusive = max_excl;
            /* a's compiled state is stale; null it so planner recompiles on
               first use (compile_criteria_tree is idempotent). */
            a->compiled = NULL;

            free_criteria_tree(b);
            for (int k = j; k < node->n_children - 1; k++)
                node->children[k] = node->children[k + 1];
            node->n_children--;
            /* a is now BETWEEN, no longer a "lower" or "upper" by itself.
               Leaving the loop — further bounds on the same field are rare
               and would require BETWEEN+gt-style narrowing (handled below
               by re-running on the new tree). */
            a_lower = a_upper = 0;
            break;
        }
    }
}

CriteriaNode *parse_criteria_tree(const char *json, const char **err) {
    if (err) *err = NULL;
    if (!json || !json[0]) return NULL;

    const char *p = json_skip(json);
    if (!*p) return NULL;

    if (*p == '[') {
        CriteriaNode *root = cnode_new(CNODE_AND);
        if (!root) { if (err) *err = "out of memory"; return NULL; }
        if (parse_tree_array(p, root, 0, err) != 0) {
            free_criteria_tree(root);
            return NULL;
        }
        if (root->n_children == 0) {
            free_criteria_tree(root);
            return NULL;
        }
        coalesce_same_field_ranges(root);
        return root;
    }

    if (*p == '{') {
        JsonObj pobj;
        json_parse_object(p, strlen(p), &pobj);
        char *or_arr  = json_obj_strdup_raw(&pobj, "or");
        char *and_arr = or_arr ? NULL : json_obj_strdup_raw(&pobj, "and");
        if (or_arr || and_arr) {
            CriteriaNode *n = cnode_new(or_arr ? CNODE_OR : CNODE_AND);
            if (!n) { free(or_arr); free(and_arr); if (err) *err = "out of memory"; return NULL; }
            if (parse_tree_array(or_arr ? or_arr : and_arr, n, 0, err) != 0) {
                free_criteria_tree(n); free(or_arr); free(and_arr);
                return NULL;
            }
            if (n->n_children == 0) {
                free_criteria_tree(n); free(or_arr); free(and_arr);
                if (err) *err = "empty or/and";
                return NULL;
            }
            free(or_arr); free(and_arr);
            coalesce_same_field_ranges(n);
            return n;
        }

        const char *field_v; size_t field_vl;
        if (json_obj_get(&pobj, "field", &field_v, &field_vl)) {
            CriteriaNode *n = cnode_new(CNODE_LEAF);
            if (!n) { if (err) *err = "out of memory"; return NULL; }
            if (parse_one_criterion(p, &n->leaf) != 0) {
                free_criteria_tree(n);
                if (err) *err = "invalid criterion: missing field, op or value";
                return NULL;
            }
            return n;
        }

        /* Simple-equality form `{"k1":"v1","k2":"v2"}` — backward compat.
           Parse k:v pairs as EQ leaves under an implicit AND root. */
        CriteriaNode *root = cnode_new(CNODE_AND);
        if (!root) { if (err) *err = "out of memory"; return NULL; }
        p++;
        while (*p) {
            p = json_skip(p);
            /* Guard: json_skip may have walked p to the NUL terminator
               on a buffer that has trailing whitespace (or never had a
               closing `}`). Without this break, the unrecognised-char
               fall-through (`if (*p != '"') { p++; continue; }`) below
               advances past the NUL → heap-OOB read on the next loop
               iteration. Found by libFuzzer; same fix pattern as the
               json_skip_value and parse_one_criterion bugs. */
            if (!*p) break;
            if (*p == '}') break;
            if (*p == ',') { p++; continue; }
            if (*p != '"') { p++; continue; }
            p++;
            const char *fname = p;
            while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
            size_t flen = p - fname;
            if (*p == '"') p++;
            p = json_skip(p);
            if (*p == ':') p++;
            p = json_skip(p);
            const char *vstart = p;
            const char *vend = json_skip_value(p);
            size_t vlen = vend - vstart;

            CriteriaNode *leaf = cnode_new(CNODE_LEAF);
            if (!leaf) { free_criteria_tree(root); if (err) *err = "out of memory"; return NULL; }
            if (flen > 255) flen = 255;
            memcpy(leaf->leaf.field, fname, flen);
            leaf->leaf.field[flen] = '\0';
            leaf->leaf.op = OP_EQUAL;
            if (vlen >= 2 && *vstart == '"' && *(vend - 1) == '"') { vlen -= 2; vstart++; }
            if (vlen > sizeof(leaf->leaf.value) - 1) vlen = sizeof(leaf->leaf.value) - 1;
            memcpy(leaf->leaf.value, vstart, vlen);
            leaf->leaf.value[vlen] = '\0';

            if (cnode_append(root, leaf) != 0) {
                free_criteria_tree(leaf); free_criteria_tree(root);
                if (err) *err = "out of memory";
                return NULL;
            }
            p = vend;
        }
        if (root->n_children == 0) { free_criteria_tree(root); return NULL; }
        coalesce_same_field_ranges(root);
        return root;
    }

    if (err) *err = "criteria must be array or object";
    return NULL;
}

/* ========== Unknown-field validation ==========
 *
 * Single source of truth for "does this field name resolve in the typed
 * schema?". Composite fields (`field1+field2`) bypass schema lookup
 * because decode_field handles them per-record; everything else must
 * exist as a declared typed field or we error out loudly. Pre-2026-05-25
 * an unknown field name silently dispatched to a no-op match path,
 * producing empty results in O(scan-all) time — see the bench-DB
 * `category` query that ran for 38 s and returned `[]` against a
 * persistent DB that didn't have that field. Validating up front means
 * the user gets `{"error":"unknown field 'category'"}` in microseconds
 * instead.
 *
 * `validate_*_fields` helpers all return 0 on success / -1 on first
 * failure with a human-readable message written into `err`. Callers
 * emit the message and abort. */

/* For composite-field references (`a+b+c`) every subfield must resolve.
 * Composite names only exist in the context of composite indexes; there is
 * no per-record dynamic-field semantics in shard-db, so an unknown subfield
 * is just as broken as an unknown plain field. Returns the offending sub-
 * name in `bad` (caller-owned buffer ≥256 B) when validation fails, so the
 * error message can pinpoint it. */
int composite_subfields_known(const TypedSchema *ts, const char *name,
                                     char *bad, size_t bad_sz) {
    char buf[512];
    snprintf(buf, sizeof(buf), "%s", name);
    char *save = NULL;
    for (char *tok = strtok_r(buf, "+", &save); tok;
         tok = strtok_r(NULL, "+", &save)) {
        if (!tok[0]) continue;     /* empty token from leading/trailing '+' */
        if (typed_field_index(ts, tok) < 0) {
            snprintf(bad, bad_sz, "%s", tok);
            return 0;
        }
    }
    return 1;
}

int field_known(const TypedSchema *ts, const char *name) {
    if (!ts || !name || !name[0]) return 1;          /* nothing to check */
    if (strchr(name, '+')) {
        char dummy[256];
        return composite_subfields_known(ts, name, dummy, sizeof(dummy));
    }
    return typed_field_index(ts, name) >= 0;
}

int validate_field(const TypedSchema *ts, const char *name,
                          const char *label, char *err, size_t err_sz) {
    if (!ts || !name || !name[0]) return 0;
    if (strchr(name, '+')) {
        char bad[256];
        if (composite_subfields_known(ts, name, bad, sizeof(bad))) return 0;
        snprintf(err, err_sz,
                 "unknown sub-field '%s' in composite '%s' (%s)",
                 bad, name, label);
        return -1;
    }
    if (typed_field_index(ts, name) >= 0) return 0;
    snprintf(err, err_sz, "unknown field '%s' in %s", name, label);
    return -1;
}

int validate_criteria_tree_fields(const CriteriaNode *n,
                                         const TypedSchema *ts,
                                         char *err, size_t err_sz) {
    if (!n) return 0;
    if (n->kind == CNODE_LEAF) {
        const SearchCriterion *c = &n->leaf;
        if (validate_field(ts, c->field, "criteria", err, err_sz) < 0) return -1;
        switch (c->op) {
            case OP_EQ_FIELD:  case OP_NEQ_FIELD:
            case OP_LT_FIELD:  case OP_GT_FIELD:
            case OP_LTE_FIELD: case OP_GTE_FIELD:
                /* RHS of field-vs-field ops names a second field. */
                if (validate_field(ts, c->value, "criteria (rhs)",
                                   err, err_sz) < 0) return -1;
                break;
            default:
                break;
        }
        return 0;
    }
    for (int i = 0; i < n->n_children; i++) {
        if (validate_criteria_tree_fields(n->children[i], ts, err, err_sz) < 0)
            return -1;
    }
    return 0;
}

int validate_field_list(const char *const *names, int n,
                               const TypedSchema *ts, const char *label,
                               char *err, size_t err_sz) {
    for (int i = 0; i < n; i++) {
        if (validate_field(ts, names[i], label, err, err_sz) < 0) return -1;
    }
    return 0;
}

/* ========== Criteria tree planner ========== */

/* Is the leaf's field indexable AND does the operator make a useful btree range?
   Returns 1 and fills out_idx_path on success, 0 otherwise. */
int leaf_is_indexed(const SearchCriterion *c, const char *db_root,
                           const char *object, char *out_idx_path, size_t out_sz) {
    /* All "is this leaf indexable?" logic lives in pick_index_for_leaf
       (the same picker the executor dispatches off, so planner and
       builder stay in sync). out_idx_path is an opaque tag for callers
       that want a non-empty string to mean "indexed"; per-shard wrappers
       rebuild the real per-shard paths internally from (field, splits). */
    if (pick_index_for_leaf(db_root, object, c) < 0) return 0;
    if (out_idx_path) {
        snprintf(out_idx_path, out_sz, "%s/%s/indexes/%s",
                 db_root, object, c->field);
    }
    return 1;
}

/* True if every child of `or_node` is a LEAF with an index. Nested AND/OR inside
   OR disqualifies (keeps the planner simple — those fall to full scan). */
static int or_all_children_indexed(const CriteriaNode *or_node,
                                   const char *db_root, const char *object) {
    if (!or_node || or_node->kind != CNODE_OR) return 0;
    for (int i = 0; i < or_node->n_children; i++) {
        CriteriaNode *c = or_node->children[i];
        if (c->kind != CNODE_LEAF) return 0;
        if (!leaf_is_indexed(&c->leaf, db_root, object, NULL, 0)) return 0;
    }
    return or_node->n_children > 0;
}

/* Look for an OR child of the root AND (or root itself if root is OR) whose
   children are all indexed. Used when no AND leaf is indexable — we can still
   narrow candidates via an OR-union fast path. Returns the OR node, or NULL. */
static CriteriaNode *find_fully_indexed_or(CriteriaNode *root,
                                           const char *db_root, const char *object) {
    if (!root) return NULL;
    if (root->kind == CNODE_OR) {
        return or_all_children_indexed(root, db_root, object) ? root : NULL;
    }
    if (root->kind == CNODE_AND) {
        for (int i = 0; i < root->n_children; i++) {
            CriteriaNode *c = root->children[i];
            if (c->kind == CNODE_OR && or_all_children_indexed(c, db_root, object))
                return c;
        }
    }
    return NULL;
}

/* Single source of truth for which operators can drive which plan capability.
 * Replaces the per-site op whitelists (intersect / composite-seed / composite-
 * exact / order-bound / trigram). Site-specific logic (trigram min length,
 * card-est estimation, btree_dispatch bounds) stays at the site; this table
 * only answers "is op X eligible for capability Y". Add an op or flip a flag
 * here and every path sees it — no more one-gate-at-a-time drift. */
/* OpCaps is defined in query_internal.h */

OpCaps op_caps(enum SearchOp op) {
    OpCaps c = {0};
    c.rank = 9;  /* default: not applicable */
    switch (op) {
        case OP_EQUAL:        c.intersect=1; c.composite_seed=1; c.composite_exact=1; c.order_bound=1; c.rank=0; break;
        case OP_STARTS_WITH:  c.intersect=1; c.composite_seed=1; c.trigram_starts=1; c.rank=1; break;
        case OP_LESS: case OP_GREATER: case OP_LESS_EQ: case OP_GREATER_EQ: case OP_BETWEEN:
                              c.intersect=1; c.order_bound=1; c.rank = (op==OP_BETWEEN) ? 2 : 4; break;
        case OP_IN:           c.intersect=1; c.composite_seed=1; c.rank=3; break;
        case OP_CONTAINS: case OP_ICONTAINS: c.trigram_prefers=1; break;
        default: break;
    }
    return c;
}

/* Thin wrappers that keep call sites untouched */
static int op_eligible_for_intersect(enum SearchOp op) { return op_caps(op).intersect; }
static int op_selectivity_rank(enum SearchOp op)      { return op_caps(op).rank; }

/* Collect indexable AND-children whose ops are intersection-eligible, sorted
   by selectivity rank. Returns the count (≥2 → intersection plan viable).

   Stage 1 restriction: ALL AND children must be eligible+indexed. Mixed
   trees (e.g., one LIKE leaf alongside indexed eq/range leaves) stay on
   PRIMARY_LEAF where the existing path post-filters via criteria_match_tree.
   Stage 2+ will lift this to feed intersection survivors into the post-filter
   pipeline for mixed trees. */
int find_intersect_leaves(CriteriaNode *root,
                                 const char *db_root, const char *object,
                                 SearchCriterion *out_leaves[MAX_INTERSECT_LEAVES],
                                 char out_paths[MAX_INTERSECT_LEAVES][PATH_MAX],
                                 int *out_partial) {
    if (out_partial) *out_partial = 0;
    if (!root || root->kind != CNODE_AND) return 0;
    if (root->n_children < 2) return 0;

    /* Two-pass collection. We need to know whether any non-bitmap
     * eligible leaf exists before deciding whether to keep bitmaps in
     * the intersect or push them into the post-filter:
     *
     * - n_nonbitmap >= 2: intersect non-bitmap leaves only. Bitmaps
     *   re-evaluate per-record via criteria_match_tree (single byte
     *   read on the decoded record — cheaper than walking a 5M-entry
     *   bool/enum bitmap into a KeySet just to intersect against the
     *   small set the non-bitmap leaves already produced).
     *
     * - n_nonbitmap == 1: only one non-bitmap leaf — too few to
     *   intersect. Return 0 so the caller falls back to PRIMARY_LEAF
     *   on that non-bitmap leaf (find_primary_leaf's selectivity
     *   scoring promotes it over any bitmap sibling).
     *
     * - n_nonbitmap == 0, n_bitmap >= 2: pure-bitmap AND. Keep all
     *   bitmaps in the intersect so PRIMARY_INTERSECT's popcount /
     *   bitmap-keyset fast path runs (PRIMARY_LEAF on one bitmap +
     *   per-record post-filter on the others collects every matching
     *   hash for the chosen value — 5.6M for type=story — then runs
     *   per-record verification, which is dramatically worse). This is
     *   the path the landing page hits (type IN […] AND dead=false
     *   AND deleted=false) and was the regression that motivated this
     *   second-pass rework. */
    SearchCriterion *cand_leaves[MAX_INTERSECT_LEAVES * 2];
    char             cand_paths[MAX_INTERSECT_LEAVES * 2][PATH_MAX];
    int              cand_is_bitmap[MAX_INTERSECT_LEAVES * 2];
    int n_cand = 0, n_nonbitmap = 0, n_bitmap = 0;
    int dropped = 0;
    const int CAND_CAP = (int)(sizeof(cand_leaves) / sizeof(cand_leaves[0]));

    for (int i = 0; i < root->n_children; i++) {
        CriteriaNode *c = root->children[i];
        if (c->kind != CNODE_LEAF) { dropped = 1; continue; }
        if (!op_eligible_for_intersect(c->leaf.op)) { dropped = 1; continue; }
        char path[PATH_MAX];
        if (!leaf_is_indexed(&c->leaf, db_root, object, path, sizeof(path))) { dropped = 1; continue; }
        if (n_cand >= CAND_CAP) { dropped = 1; continue; }
        int picked = pick_index_for_leaf(db_root, object, &c->leaf);
        cand_leaves[n_cand] = &c->leaf;
        memcpy(cand_paths[n_cand], path, PATH_MAX);
        cand_is_bitmap[n_cand] = (picked == IT_BITMAP) ? 1 : 0;
        if (picked == IT_BITMAP) n_bitmap++; else n_nonbitmap++;
        n_cand++;
    }

    int n = 0;
    int bitmaps_skipped = 0;
    if (n_nonbitmap >= 2) {
        for (int i = 0; i < n_cand && n < MAX_INTERSECT_LEAVES; i++) {
            if (cand_is_bitmap[i]) { bitmaps_skipped = 1; continue; }
            out_leaves[n] = cand_leaves[i];
            memcpy(out_paths[n], cand_paths[i], PATH_MAX);
            n++;
        }
        if (n < n_nonbitmap) dropped = 1;  /* spilled past MAX_INTERSECT_LEAVES */
    } else if (n_nonbitmap == 0 && n_bitmap >= 2) {
        for (int i = 0; i < n_cand && n < MAX_INTERSECT_LEAVES; i++) {
            out_leaves[n] = cand_leaves[i];
            memcpy(out_paths[n], cand_paths[i], PATH_MAX);
            n++;
        }
        if (n < n_bitmap) dropped = 1;
    } else {
        return 0;
    }
    if (n < 2) return 0;
    if (out_partial) *out_partial = dropped || bitmaps_skipped;
    /* Insertion sort by selectivity rank — n is tiny (≤ MAX_INTERSECT_LEAVES). */
    for (int i = 1; i < n; i++) {
        for (int j = i; j > 0; j--) {
            int a = op_selectivity_rank(out_leaves[j]->op);
            int b = op_selectivity_rank(out_leaves[j-1]->op);
            if (a >= b) break;
            SearchCriterion *tl = out_leaves[j];
            out_leaves[j] = out_leaves[j-1];
            out_leaves[j-1] = tl;
            char tp[PATH_MAX];
            memcpy(tp, out_paths[j], PATH_MAX);
            memcpy(out_paths[j], out_paths[j-1], PATH_MAX);
            memcpy(out_paths[j-1], tp, PATH_MAX);
        }
    }
    return n;
}


/* ========== cmd_explain ========== */
/* cmd_explain -- emit query plan (FilterPlan + hints) without executing.
   Hints are always emitted — EXPLAIN is a diagnostic tool and the user
   explicitly asked for suggestions. The caller decides whether to act
   on them based on their table size and workload.
   Called from server dispatch and CLI with explain=true on find/count/aggregate. */
void cmd_explain(const char *db_root, const char *object, const char *criteria_json,
                 const char *order_by, int fetching) {
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json ? criteria_json : "[]", &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        return;
    }

    Schema sch = load_schema(db_root, object);
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);

    {
        char verr[256];
        if (validate_criteria_tree_fields(tree, fs.ts, verr, sizeof(verr)) < 0) {
            OUT("{\"error\":\"%s\"}\n", verr);
            free_criteria_tree(tree);
            return;
        }
    }
    if (tree) compile_criteria_tree(tree, fs.ts);

    /* Get table row count (O(1) metadata lookup) */
    int table_rows = get_live_count(db_root, object);

    /* Compute the FilterPlan; limit=0 means unbounded (we're not executing).
       fetching=1 for find, 0 for count/aggregate. */
    FilterPlan fp = plan_filter(tree, db_root, object, &fs, sch.splits,
                                 table_rows, order_by, fetching, 0);

    /* Map FilterPlan kind to string */
    const char *plan_str = "unknown";
    switch (fp.kind) {
        case FP_FULL_SCAN:       plan_str = "scan"; break;
        case FP_PRIMARY_LEAF:    plan_str = "leaf"; break;
        case FP_BITMAP_SMALLER:  plan_str = "bitmap"; break;
        case FP_INTERSECT:       plan_str = "intersect"; break;
        case FP_UNION:           plan_str = "union"; break;
    }

    /* Map FilterOrderKind to string */
    const char *order_str = "none";
    switch (fp.order) {
        case FP_ORDER_NONE:              order_str = "none"; break;
        case FP_ORDER_COMPOSITE:         order_str = "composite"; break;
        case FP_ORDER_COMPOSITE_EXACT:   order_str = "composite_exact"; break;
        case FP_ORDER_SORT:              order_str = "sort"; break;
        case FP_ORDER_INDEX_WALK:        order_str = "index_walk"; break;
    }

    /* Emit plan header */
    OUT("{\"plan\":\"%s\",\"order\":\"%s\",\"total_cheap\":%s,\"table_rows\":%d,"
        "\"source\":[", plan_str, order_str, fp.total_cheap ? "true" : "false", table_rows);

    /* Emit source leaves (indexed seed criteria) */
    for (int i = 0; i < fp.n_source; i++) {
        SearchCriterion *leaf = fp.source_leaves[i];
        if (!leaf) continue;

        int it = pick_index_for_leaf(db_root, object, leaf);
        const char *it_str = (it == IT_BTREE)    ? "btree"   :
                             (it == IT_BITMAP)   ? "bitmap"  :
                             (it == IT_TRIGRAM)  ? "trigram" : "none";

        /* Estimate rows for this leaf via card_est_leaf.
           card_est_leaf takes a single TypedField (not TypedSchema), and
           needs a non-zero cap -- use selectivity_budget(table_rows). */
        const TypedField *leaf_tf = resolve_idx_field(fs.ts, leaf->field);
        CardEst est = card_est_leaf(db_root, object, sch.splits, leaf,
                                    leaf_tf, selectivity_budget((size_t)table_rows));
        size_t est_rows = est.k;  /* CardEst.k is the estimated match count */

        if (i > 0) OUT(",");
        OUT("{\"field\":\"%s\",\"op\":\"%s\",\"index\":\"%s\",\"role\":\"seed\",\"estimated_rows\":%zu}",
            leaf->field, 
            (leaf->op == OP_EQUAL) ? "eq" :
            (leaf->op == OP_LESS) ? "lt" :
            (leaf->op == OP_GREATER) ? "gt" :
            (leaf->op == OP_LESS_EQ) ? "lte" :
            (leaf->op == OP_GREATER_EQ) ? "gte" :
            (leaf->op == OP_LIKE) ? "like" :
            (leaf->op == OP_CONTAINS) ? "contains" :
            (leaf->op == OP_STARTS_WITH) ? "starts" :
            (leaf->op == OP_BETWEEN) ? "between" :
            (leaf->op == OP_IN) ? "in" :
            (leaf->op == OP_EXISTS) ? "exists" : "other",
            it_str, est_rows);
    }
    OUT("],\"postfilter\":[");

    /* Emit postfilter leaves. Report the actual index type on the field
       (the index exists but the planner chose not to use it as the primary
       driver), or null when the field truly has no index. */
    for (int i = 0; i < fp.n_postfilter; i++) {
        SearchCriterion *leaf = fp.postfilter_leaves[i];
        if (!leaf) continue;

        int it = pick_index_for_leaf(db_root, object, leaf);
        /* it < 0 means no usable index for this op/field combination */
        const char *pf_it_str = (it == IT_BTREE) ? "\"btree\"" :
                                (it == IT_BITMAP) ? "\"bitmap\"" :
                                (it == IT_TRIGRAM) ? "\"trigram\"" : "null";

        if (i > 0) OUT(",");
        OUT("{\"field\":\"%s\",\"op\":\"%s\",\"index\":%s,\"role\":\"postfilter\",\"estimated_rows\":null}",
            leaf->field,
            (leaf->op == OP_EQUAL) ? "eq" :
            (leaf->op == OP_LESS) ? "lt" :
            (leaf->op == OP_GREATER) ? "gt" :
            (leaf->op == OP_LESS_EQ) ? "lte" :
            (leaf->op == OP_GREATER_EQ) ? "gte" :
            (leaf->op == OP_LIKE) ? "like" :
            (leaf->op == OP_CONTAINS) ? "contains" :
            (leaf->op == OP_STARTS_WITH) ? "starts" :
            (leaf->op == OP_BETWEEN) ? "between" :
            (leaf->op == OP_IN) ? "in" :
            (leaf->op == OP_EXISTS) ? "exists" : "other",
            pf_it_str);
    }
    OUT("],\"hints\":[");

    /* Generate hints */
    int hint_count = 0;

    /* Hint: add_index for unindexed postfilter leaves */
    for (int i = 0; i < fp.n_postfilter; i++) {
        SearchCriterion *leaf = fp.postfilter_leaves[i];
        if (!leaf) continue;

        int it = pick_index_for_leaf(db_root, object, leaf);
        if (it < 0) {  /* unindexed */
            /* Suggest btree index for range/eq ops; trigram for text ops */
            const TypedField *tf = resolve_idx_field(fs.ts, leaf->field);
            if (tf && tf->type == FT_VARCHAR &&
                (leaf->op == OP_LIKE || leaf->op == OP_CONTAINS ||
                 leaf->op == OP_ILIKE || leaf->op == OP_ICONTAINS ||
                 leaf->op == OP_STARTS_WITH || leaf->op == OP_ISTARTS_WITH)) {
                /* Will emit trigram hint below */
            } else {
                if (hint_count > 0) OUT(",");
                OUT("{\"type\":\"add_index\",\"field\":\"%s\",\"reason\":\"unindexed field in postfilter; index avoids full record scan\"}", 
                    leaf->field);
                hint_count++;
            }
        }
    }

    /* Hint: add_trigram_index for varchar text-search ops (always emitted when applicable) */
    /* First, walk source+postfilter leaves for non-full-scan plans. */
    int trigram_checked = 0;
    for (int j = 0; j < 2; j++) {  /* loop 0: source, 1: postfilter */
        int n = (j == 0) ? fp.n_source : fp.n_postfilter;
        SearchCriterion **leaves = (j == 0) ? fp.source_leaves : fp.postfilter_leaves;

        for (int i = 0; i < n; i++) {
            SearchCriterion *leaf = leaves[i];
            if (!leaf) continue;
            trigram_checked = 1;

            const TypedField *tf = resolve_idx_field(fs.ts, leaf->field);
            if (tf && tf->type == FT_VARCHAR &&
                (leaf->op == OP_LIKE || leaf->op == OP_CONTAINS ||
                 leaf->op == OP_ILIKE || leaf->op == OP_ICONTAINS ||
                 leaf->op == OP_STARTS_WITH || leaf->op == OP_ISTARTS_WITH ||
                 leaf->op == OP_NOT_LIKE || leaf->op == OP_NOT_CONTAINS ||
                 leaf->op == OP_INOT_LIKE || leaf->op == OP_INOT_CONTAINS ||
                 leaf->op == OP_ENDS_WITH || leaf->op == OP_IENDS_WITH)) {

                int it = pick_index_for_leaf(db_root, object, leaf);
                if (it != IT_TRIGRAM) {  /* not already a trigram index */
                    if (hint_count > 0) OUT(",");
                    OUT("{\"type\":\"add_trigram_index\",\"field\":\"%s\",\"reason\":\"varchar text-search op on %s field; trigram index enables substring matching without full record scan\"}",
                        leaf->field,
                        (it < 0) ? "unindexed" : "non-trigram-indexed");
                    hint_count++;
                }
            }
        }
    }

    /* Fallback for full-scan plans: walk the raw criteria tree directly. */
    if (!trigram_checked && tree) {
        SearchCriterion *raw_ptrs[64];
        int n_raw = collect_and_leaves(tree, raw_ptrs, 64);
        for (int i = 0; i < n_raw; i++) {
            SearchCriterion *leaf = raw_ptrs[i];
            if (!leaf) continue;

            const TypedField *tf = resolve_idx_field(fs.ts, leaf->field);
            if (tf && tf->type == FT_VARCHAR &&
                (leaf->op == OP_LIKE || leaf->op == OP_CONTAINS ||
                 leaf->op == OP_ILIKE || leaf->op == OP_ICONTAINS ||
                 leaf->op == OP_STARTS_WITH || leaf->op == OP_ISTARTS_WITH ||
                 leaf->op == OP_NOT_LIKE || leaf->op == OP_NOT_CONTAINS ||
                 leaf->op == OP_INOT_LIKE || leaf->op == OP_INOT_CONTAINS ||
                 leaf->op == OP_ENDS_WITH || leaf->op == OP_IENDS_WITH)) {

                int it = pick_index_for_leaf(db_root, object, leaf);
                if (it != IT_TRIGRAM) {
                    if (hint_count > 0) OUT(",");
                    OUT("{\"type\":\"add_trigram_index\",\"field\":\"%s\",\"reason\":\"varchar text-search op on %s field; trigram index enables substring matching without full record scan\"}",
                        leaf->field,
                        (it < 0) ? "unindexed" : "non-trigram-indexed");
                    hint_count++;
                }
            }
        }
    }

    /* Hint: composite_index -- only when the planner chose FP_ORDER_SORT,
       meaning it has an indexed filter but no composite covering the order_by.
       Do not emit when fp.order is already COMPOSITE/COMPOSITE_EXACT/INDEX_WALK
       since the planner already found an index path for ordering. */
    if (fp.order == FP_ORDER_SORT && order_by && fp.n_source > 0) {
        SearchCriterion *seed = fp.source_leaves[0];
        if (seed && strcmp(seed->field, order_by) != 0 &&
            pick_index_for_leaf(db_root, object, seed) >= 0) {
            if (hint_count > 0) OUT(",");
            OUT("{\"type\":\"composite_index\",\"field\":\"%s+%s\","
                "\"reason\":\"filter on %s + order_by %s; composite index avoids in-memory sort\"}",
                seed->field, order_by, seed->field, order_by);
            hint_count++;
        }
    }

    OUT("]}\n");

    free_criteria_tree(tree);
}

/* Tree-based variant: tree already parsed by caller (NQL path). Does NOT free
   tree — ownership stays with the caller. */

/* ========== Cardinality estimator + filter planner ========== */
/* ========== Cardinality estimator (planner primitive) ==========
 *
 * card_est_leaf: cheap estimate of how many records match `leaf` on a
 * single indexed field.
 *   IT_BITMAP  -> exact, free (bm_count, ~1 ms/128-shard even cold)
 *   IT_BTREE   -> capped walk (exact when <= cap, saturated when broad)
 *   IT_TRIGRAM -> rarest gram's capped posting (upper-bounds candidates)
 * Ops needing per-record verification (contains-on-btree, like, ends,
 * regex, len_*, exists, etc.) return estimable=0 — the cost model treats
 * those leaves as unestimable.
 *
 * PRODUCTION code — the cost model wires it in at runtime in Phase 1b.
 * __attribute__((unused)) keeps -Wunused-function quiet until then (same
 * idiom as idx_build_worker above (both are __attribute__((unused))).
 *
 * cap is carried in the signature for those future callers; the bitmap
 * path ignores it (bitmap counts are exact, not bounded). */

/* Counting callback for btree cardinality estimation.  Returns -1 once n
   exceeds cap so btree_idx_walk_ordered stops early — O(min(K, cap)). */
typedef struct { size_t n; size_t cap; } CardCountCtx;
static int card_count_cb(const char *v, size_t vl, const uint8_t h[16], void *ctx) {
    (void)v; (void)vl; (void)h;
    CardCountCtx *c = (CardCountCtx *)ctx;
    c->n++;
    return (c->n > c->cap) ? -1 : 0;
}

CardEst card_est_leaf(const char *db_root, const char *object,
                             int splits, const SearchCriterion *leaf,
                             const TypedField *tf, size_t cap) {
    CardEst e = { 0, 0, 0 };
    int it = pick_index_for_leaf(db_root, object, leaf);
    if (it < 0) return e;              /* not indexed -> not estimable */
    e.estimable = 1;
    if (it == IT_BITMAP && (leaf->op == OP_EQUAL || leaf->op == OP_IN)) {
        uint8_t val[1024]; size_t vlen = 0;
        encode_criterion_value(tf, leaf->value, strlen(leaf->value), val, &vlen);
        if (vlen == 0) { e.estimable = 0; return e; }
        for (int s = 0; s < splits; s++) {
            char bp[1024];
            bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
            BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
            if (!bm) continue;
            e.k += bm_count(bm, val, vlen);
            bm_close(bm);
        }
        e.saturated = 0;               /* exact count — never saturated */
        return e;
    }
    if (it == IT_BTREE) {
        /* Capped count via btree_idx_walk_ordered (single-threaded k-way merge).
           Returning -1 from the callback stops the walk, giving O(min(K,cap))
           cost.  Bounds mirror btree_dispatch exactly for each rangeable op.
           Ops that require per-record check_primary (contains, like-non-prefix,
           ends, regex, etc.) cannot be bounded cheaply — leave estimable=0. */
        uint8_t buf1[1032], buf2[1032];
        size_t  len1 = 0,   len2 = 0;

        CardCountCtx cctx = { 0, cap };

        switch (leaf->op) {
            case OP_EQUAL:
                encode_criterion_value(tf, leaf->value, strlen(leaf->value), buf1, &len1);
                if (len1 == 0) { e.estimable = 0; return e; }
                btree_idx_walk_ordered(db_root, object, leaf->field, splits,
                                       (const char *)buf1, len1, 0,
                                       (const char *)buf1, len1, 0,
                                       0, card_count_cb, &cctx);
                break;

            case OP_GREATER_EQ:
                encode_criterion_value(tf, leaf->value, strlen(leaf->value), buf1, &len1);
                if (len1 == 0) { e.estimable = 0; return e; }
                btree_idx_walk_ordered(db_root, object, leaf->field, splits,
                                       (const char *)buf1, len1, 0,
                                       "\xff\xff\xff\xff", 4, 0,
                                       0, card_count_cb, &cctx);
                break;

            case OP_GREATER:
                encode_criterion_value(tf, leaf->value, strlen(leaf->value), buf1, &len1);
                if (len1 == 0) { e.estimable = 0; return e; }
                btree_idx_walk_ordered(db_root, object, leaf->field, splits,
                                       (const char *)buf1, len1, 1,
                                       "\xff\xff\xff\xff", 4, 0,
                                       0, card_count_cb, &cctx);
                break;

            case OP_LESS_EQ:
                encode_criterion_value(tf, leaf->value, strlen(leaf->value), buf1, &len1);
                if (len1 == 0) { e.estimable = 0; return e; }
                btree_idx_walk_ordered(db_root, object, leaf->field, splits,
                                       "", 0, 0,
                                       (const char *)buf1, len1, 0,
                                       0, card_count_cb, &cctx);
                break;

            case OP_LESS:
                encode_criterion_value(tf, leaf->value, strlen(leaf->value), buf1, &len1);
                if (len1 == 0) { e.estimable = 0; return e; }
                btree_idx_walk_ordered(db_root, object, leaf->field, splits,
                                       "", 0, 0,
                                       (const char *)buf1, len1, 1,
                                       0, card_count_cb, &cctx);
                break;

            case OP_BETWEEN:
                encode_criterion_value(tf, leaf->value,  strlen(leaf->value),  buf1, &len1);
                encode_criterion_value(tf, leaf->value2, strlen(leaf->value2), buf2, &len2);
                if (len1 == 0 || len2 == 0) { e.estimable = 0; return e; }
                btree_idx_walk_ordered(db_root, object, leaf->field, splits,
                                       (const char *)buf1, len1, leaf->min_exclusive,
                                       (const char *)buf2, len2, leaf->max_exclusive,
                                       0, card_count_cb, &cctx);
                break;

            case OP_IN: {
                /* Sum across all IN values; stop the whole estimate once cap
                   is exceeded (cctx.n is shared across all per-value walks). */
                for (int iv = 0; iv < leaf->in_count; iv++) {
                    if (cctx.n > cap) break;
                    encode_criterion_value(tf, leaf->in_values[iv],
                                           strlen(leaf->in_values[iv]), buf1, &len1);
                    if (len1 == 0) continue;
                    btree_idx_walk_ordered(db_root, object, leaf->field, splits,
                                           (const char *)buf1, len1, 0,
                                           (const char *)buf1, len1, 0,
                                           0, card_count_cb, &cctx);
                }
                break;
            }

            case OP_STARTS_WITH: {
                int raw_prefix = (!tf || tf->type == FT_VARCHAR);
                size_t plen;
                if (raw_prefix) {
                    plen = strlen(leaf->value);
                    memcpy(buf1, leaf->value, plen);
                } else {
                    encode_criterion_value(tf, leaf->value, strlen(leaf->value), buf1, &plen);
                }
                memcpy(buf2, buf1, plen);
                memset(buf2 + plen, 0xff, 4);
                btree_idx_walk_ordered(db_root, object, leaf->field, splits,
                                       (const char *)buf1, plen, 0,
                                       (const char *)buf2, plen + 4, 0,
                                       0, card_count_cb, &cctx);
                break;
            }

            default:
                /* contains, ends, like (non-prefix), i-variants, regex, exists,
                   len_* — require per-record check_primary; not cheaply bounded.
                   Leave estimable=0 so the cost model treats them as unknown. */
                e.estimable = 0;
                return e;
        }

        e.k         = cctx.n;
        e.saturated = (cctx.n > cap) ? 1 : 0;
        return e;
    }
    if (it == IT_TRIGRAM &&
        (leaf->op == OP_CONTAINS || leaf->op == OP_ICONTAINS ||
         leaf->op == OP_STARTS_WITH)) {
        /* Estimate = rarest gram's posting size, capped at `cap`.
           For contains/icontains: intersection of all grams in the pattern,
           bounded above by the rarest gram's posting count.
           For starts_with: same primitive — extract the leading 3-gram(s)
           from the prefix; the rarest 3-gram gives a tight upper bound on
           the candidate set (records whose field starts with the prefix are
           a subset of records containing each of those grams).
           We lowercase the pattern/prefix to match the index (trigrams are
           stored lowercase — see tg_build_grams / build_keyset_from_trigram). */
        size_t plen = strlen(leaf->value);
        if (plen < 3) { e.estimable = 0; return e; }
        if (plen > 1023) plen = 1023;

        uint8_t pattern_lc[1024];
        for (size_t i = 0; i < plen; i++) {
            uint8_t c = (uint8_t)leaf->value[i];
            pattern_lc[i] = (c >= 'A' && c <= 'Z') ? (uint8_t)(c + 32) : c;
        }

        uint8_t trigrams[TG_MAX_DISTINCT][3];
        size_t ng = tg_extract_distinct(pattern_lc, plen, trigrams, TG_MAX_DISTINCT);
        if (ng == 0) { e.estimable = 0; return e; }

        int idx_n = index_splits_for(splits);
        size_t min_count = (size_t)-1;   /* will be clamped below */

        for (size_t gi = 0; gi < ng; gi++) {
            /* Count this gram's posting across all .tg shards, stopping
               once we exceed cap (no need to count further — the gram
               already saturates, so it cannot be the rarest useful one
               unless all grams saturate). */
            CardCountCtx cctx = { 0, cap };
            for (int s = 0; s < idx_n; s++) {
                char tp[PATH_MAX];
                tg_build_path(tp, sizeof(tp), db_root, object, leaf->field, s);
                btree_range(tp,
                            (const char *)trigrams[gi], 3,
                            (const char *)trigrams[gi], 3,
                            card_count_cb, &cctx);
                if (cctx.n > cap) break;   /* already saturated */
            }
            size_t gram_count = cctx.n;
            if (gram_count < min_count) min_count = gram_count;
            /* If rarest so far is 0, result must be empty. */
            if (min_count == 0) break;
        }

        if (min_count == (size_t)-1) min_count = 0;
        e.k         = min_count;
        e.saturated = (min_count > cap) ? 1 : 0;
        return e;
    }
    /* non-eq bitmap ops / trigram non-contains: not estimated */
    e.estimable = 0;
    return e;
}

#ifdef TEST_BUILD

/* Test-only hook for the cardinality estimator.  Accepts a combined
 * "dir/object" string so tests can pass (env.db_root, "default/ce")
 * without needing to pre-build the effective root themselves.
 * Sets g_db_root (same pattern as planner_primary_kind_for_test's callers)
 * so that load_schema can locate schema.conf in the test process. */
CardEst card_est_by_field(const char *db_root, const char *object,
                          const char *field, const char *value, size_t cap) {
    /* Point g_db_root at the raw root so load_schema finds schema.conf. */
    snprintf(g_db_root, PATH_MAX, "%s", db_root);

    /* Split "dir/obj" into eff_root = db_root + "/" + dir, bare = obj. */
    char eff_root[PATH_MAX];
    char bare[256];
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dirlen = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s", db_root, (int)dirlen, object);
        snprintf(bare, sizeof(bare), "%s", slash + 1);
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        snprintf(bare, sizeof(bare), "%s", object);
    }
    Schema sc = load_schema(eff_root, bare);
    FieldSchema fs; init_field_schema(&fs, eff_root, bare);
    SearchCriterion leaf; memset(&leaf, 0, sizeof(leaf));
    snprintf(leaf.field, sizeof(leaf.field), "%s", field);
    leaf.op = OP_EQUAL;
    snprintf(leaf.value, sizeof(leaf.value), "%s", value);
    const TypedField *tf = resolve_idx_field(fs.ts, field);
    return card_est_leaf(eff_root, bare, sc.splits, &leaf, tf, cap);
}

/* Sibling hook for trigram/contains estimation.  Same path setup as
 * card_est_by_field but sets leaf.op = OP_CONTAINS so the IT_TRIGRAM
 * branch in card_est_leaf is exercised. */
CardEst card_est_by_field_contains(const char *db_root, const char *object,
                                   const char *field, const char *value, size_t cap) {
    snprintf(g_db_root, PATH_MAX, "%s", db_root);
    char eff_root[PATH_MAX];
    char bare[256];
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dirlen = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s", db_root, (int)dirlen, object);
        snprintf(bare, sizeof(bare), "%s", slash + 1);
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        snprintf(bare, sizeof(bare), "%s", object);
    }
    Schema sc = load_schema(eff_root, bare);
    FieldSchema fs; init_field_schema(&fs, eff_root, bare);
    SearchCriterion leaf; memset(&leaf, 0, sizeof(leaf));
    snprintf(leaf.field, sizeof(leaf.field), "%s", field);
    leaf.op = OP_CONTAINS;
    snprintf(leaf.value, sizeof(leaf.value), "%s", value);
    const TypedField *tf = resolve_idx_field(fs.ts, field);
    return card_est_leaf(eff_root, bare, sc.splits, &leaf, tf, cap);
}
#endif

/* ========== Phase 1b: cost model + unified filter planner ==========
 *
 * plan_filter() is the single decision function the rewrite (Phase 1c) wires
 * count/find/aggregate onto. It consumes card_est_leaf (1a) and the cost knob
 * below to pick a plan per the decision table
 * (docs/superpowers/specs/2026-05-28-planner-filter-decision-table.md).
 *
 * PURE ADDITION in 1b: no runtime caller yet (1c migrates executors), so
 * __attribute__((unused)) keeps -Wunused-function quiet — same idiom as
 * card_est_leaf. Decisions are validated via the TEST_BUILD hook + the
 * test_planner_cost_model.c matrix BEFORE any executor changes. */

/* Cost model: fetch-and-check costs K random reads; full scan costs N
 * sequential reads. random_read ≈ ratio × seq_read (page-fault/seek vs
 * prefetch; PG's random_page_cost/seq_page_cost). Fetch beats scan when
 * K × ratio < N, i.e. K < N/ratio. g_random_seq_ratio is the tunable
 * RANDOM_SEQ_COST_RATIO knob (default 8). */
size_t selectivity_budget(size_t N) {
    int r = g_random_seq_ratio;
    if (r < 1) r = 1;
    return N / (size_t)r;            /* rows below which fetch-and-check wins */
}

/* A leaf is "selective" when its estimated match count clears the cost bar:
 * estimable, not saturated (the capped walk didn't bail), and K ≤ budget.
 * Bitmaps are exact (never saturated) so a rare bitmap value CAN be selective
 * via the K ≤ budget test; a broad one (type=story) cannot. Call card_est_leaf
 * with cap = budget so a btree/trigram walk stops at budget+1 → saturated ⟺
 * K > budget, unifying the test across index types. */
int leaf_is_selective(CardEst e, size_t N) {
    if (!e.estimable || e.saturated) return 0;
    return e.k <= selectivity_budget(N);
}

/* Pick the most-selective indexed leaf among `leaves` to seed the plan, and
 * fill est[i] for each. Returns the chosen index, or -1 if none is indexed.
 * Selection: smallest estimated K wins; bitmaps are pushed last (a broad
 * bitmap must never seed). Unestimable-but-indexed leaves (e.g. like/regex)
 * rank after every estimable one. N = live rows (for the budget/cap). */
static int most_selective_indexed(const char *db_root, const char *object,
                                  int splits, SearchCriterion **leaves, int n,
                                  const FieldSchema *fs, size_t N,
                                  CardEst *est /* [n] */) {
    int best = -1;
    size_t budget = selectivity_budget(N);
    for (int i = 0; i < n; i++) {
        int it = pick_index_for_leaf(db_root, object, leaves[i]);
        if (it < 0) { est[i] = (CardEst){0,0,0}; continue; }     /* not indexed */
        const TypedField *tf = resolve_idx_field(fs->ts, leaves[i]->field);
        est[i] = card_est_leaf(db_root, object, splits, leaves[i], tf, budget);
        if (best < 0) { best = i; continue; }
        /* Compare i vs best. Bitmaps deprioritized when the non-bitmap has a
         * smaller K (btree/trigram yield records in a useful order for direct
         * leaf walks).  When the bitmap has the smaller K and both estimates
         * are reliable, cardinality wins — the primary feeds a prefilter set
         * whose order is irrelevant (always a hash table for D2/D3/count). */
        int it_best = pick_index_for_leaf(db_root, object, leaves[best]);
        int i_bm = (it == IT_BITMAP), b_bm = (it_best == IT_BITMAP);
        if (i_bm != b_bm) {
            int bm_i  = i_bm ? i : best;
            int oth_i = i_bm ? best : i;
            /* Bitmap K is always exact (saturated==0 by construction in
             * card_est_leaf's bitmap branch).  When the other leaf is
             * saturated, its true K is at least cap+1 = budget+1; if the
             * bitmap's exact K is at or below budget, it's provably the
             * smaller candidate. */
            if (est[bm_i].estimable && !est[bm_i].saturated &&
                est[oth_i].saturated && est[bm_i].k <= budget) {
                best = bm_i;
                continue;
            }
            /* Both reliable — smaller K wins regardless of index type.
               Old code did `if (b_bm) best = i` which ignores cardinality.
               For prefilter/KeySet builds (D2, D3, count, aggregate) only
               cardinality matters — index type doesn't affect prefilter
               quality and D1 composite detection is independent. */
            if (est[bm_i].estimable && !est[bm_i].saturated &&
                est[oth_i].estimable && !est[oth_i].saturated) {
                if (est[bm_i].k < est[oth_i].k) best = bm_i;
                else if (est[oth_i].k < est[bm_i].k) best = oth_i;
                continue;
            }
            if (b_bm) best = i;       /* fallback: one unreliable estimate */
            continue;
        }
        int i_e = est[i].estimable && !est[i].saturated;
        int b_e = est[best].estimable && !est[best].saturated;
        if (i_e != b_e) { if (i_e) best = i; continue; }          /* prefer estimable */
        if (i_e && b_e && est[i].k < est[best].k) best = i;       /* smaller K wins */
    }
    return best;
}

/* True if a composite index named "<a>+<b>" exists on the object. Used for
 * D1: a (filter_field + order_field) composite gives a sorted prefix scan.
 * Composite indexes are always btree (only btree supports multi-field prefix);
 * the on-disk dir name is the literal "a+b" (see CLAUDE.md "Composite indexes"). */
static int composite_index_exists(const char *db_root, const char *object,
                                  const char *a, const char *b) {
    char name[256];
    snprintf(name, sizeof(name), "%s+%s", a, b);
    return field_has_index_type(db_root, object, name, IT_BTREE);
}

/* Scan all AND-leaves for an EQ/STARTS_WITH leaf whose "<field>+<order_by>"
 * composite btree exists.  Returns that leaf's index, or -1 if none.
 *
 * Why this exists: most_selective_indexed() prefers a non-bitmap leaf as the
 * seed, so for `dead=… AND deleted=… AND type=X AND time>=T ORDER BY time`
 * the seed is always `time` (the lone btree), never the bitmap `type` — even
 * though `type+time` is exactly the composite that turns the ordered walk into
 * a bounded prefix scan.  The D1 overlay must look past the single seed and
 * find the leaf the composite actually covers.  EQ/STARTS_WITH only: those are
 * the ops find_via_composite_prefix bounds correctly (see the D1 gate). */
static int find_covering_composite(const char *db_root, const char *object,
                                   SearchCriterion **leaves, int nL,
                                   const char *order_by) {
    if (!order_by || !order_by[0]) return -1;
    for (int i = 0; i < nL; i++) {
        if (!op_caps(leaves[i]->op).composite_seed) continue;
        if (composite_index_exists(db_root, object, leaves[i]->field, order_by))
            return i;
    }
    return -1;
}

/* Build the exact composite key for `composite_field` (e.g. "by+time") from
 * eq leaves, in the composite's field order. encode_field_for_index is the
 * same encoder typed_field_to_index_key uses at build time (config.c:1948),
 * so the key is byte-identical to the stored composite key. Returns 1 with
 * *out_len set iff EVERY sub-field is matched by an OP_EQUAL leaf. */
int build_exact_composite_key(const FieldSchema *fs, const char *composite_field,
                                     SearchCriterion **leaves, int nL,
                                     uint8_t *out, size_t *out_len) {
    char buf[256]; strncpy(buf, composite_field, sizeof(buf)-1); buf[sizeof(buf)-1]='\0';
    size_t total = 0; char *save = NULL;
    for (char *sub = strtok_r(buf, "+", &save); sub; sub = strtok_r(NULL, "+", &save)) {
        SearchCriterion *m = NULL;
        for (int i = 0; i < nL; i++)
            if (op_caps(leaves[i]->op).composite_exact && strcmp(leaves[i]->field, sub) == 0) { m = leaves[i]; break; }
        if (!m) return 0;  /* sub-field not pinned by an eq leaf */
        const TypedField *tf = resolve_idx_field(fs ? fs->ts : NULL, sub);
        size_t l = 0;
        encode_field_for_index(tf, m->value, strlen(m->value), out + total, &l);
        total += l;
    }
    *out_len = total;
    return 1;
}

/* Find a composite index fully covered by eq leaves. Returns its name in
 * out_name (size>=256) and 1 if found, else 0. Iterates the object's index
 * names (cache-backed; composites contain '+'). */
static int find_exact_covering_composite(const char *db_root, const char *object,
                                         const FieldSchema *fs,
                                         SearchCriterion **leaves, int nL,
                                         char *out_name) {
    char names[MAX_FIELDS][256];
    int n = load_index_fields(db_root, object, names, MAX_FIELDS);
    for (int i = 0; i < n; i++) {
        if (!strchr(names[i], '+')) continue;            /* composites only */
        uint8_t key[1024]; size_t klen = 0;
        if (build_exact_composite_key(fs, names[i], leaves, nL, key, &klen) && klen > 0) {
            snprintf(out_name, 256, "%s", names[i]);
            return 1;
        }
    }
    return 0;
}

/* True if `order_by` has a btree index that can drive an ORDER_INDEX_WALK
 * (D3 from the decision table).  When this is the case the B5 demotion
 * (broad single leaf → FULL_SCAN) must be suppressed so the seed stays
 * PRIMARY_LEAF and the order overlay below can set FP_ORDER_INDEX_WALK.
 * Composite indexes are handled separately by composite_index_exists. */
static int order_field_drivable(const char *db_root, const char *object,
                                const char *order_by) {
    if (!order_by || !order_by[0]) return 0;
    return field_has_index_type(db_root, object, order_by, IT_BTREE);
}

/* D2 (fetch+sort) vs D3 (walk order index, post-filter to limit).
 *
 * D2 only pays off when the candidate set is genuinely small: it materializes
 * and sorts every match before applying limit. A *bitmap* seed yields an EXACT
 * count (estimable && !saturated) that can still be enormous (k ≈ N) — sorting
 * that many rows times out (observed: `type in (...) ORDER BY score` over 4.3M
 * → 30s). So gate on magnitude, not just estimability: when the most-selective
 * seed is still broad (fails the N/g_random_seq_ratio budget) and order_by has
 * a btree to walk, walk it (D3); the broad filter's high post-filter pass-rate
 * fills `limit` in ~limit fetches. Only fall back to sort when order_by isn't
 * drivable (no btree → nothing to walk; D2 is best-effort, bounded by
 * QUERY_BUFFER_MB). Mirrors the cursor path's g_ordered_find_keyset_max guard. */
/* Crossover for fetch+sort (D2) vs order-index walk (D3). D2 cost ≈ K (fetch+
 * sort all candidates). D3 cost ≈ (offset+limit)/pass_rate ≈ (offset+limit)*N/K
 * (walk the order index until `limit` matches fill). They cross at
 * K ≈ sqrt((offset+limit)*N): below it sort wins, above it walk wins. This
 * replaces the limit-agnostic N/g_random_seq_ratio cutoff and the fixed
 * SMALL_PREFILTER_THRESHOLD with one decision. */
int prefer_fetch_sort(size_t candidates, size_t N, int offset, int limit,
                             int is_bitmap_seed) {
    if (candidates == 0) return 1;
    size_t want = (size_t)((offset > 0 ? offset : 0) + (limit > 0 ? limit : 1));

    /* For bitmap seeds, the uniform distribution assumption often fails
       (sparse bitmaps are clustered, not spread). The walk cost scales as
       want × N² / K² instead of want × N / K. Crossover: K³ < want × N².
       Use __int128 to avoid overflow for large N (up to 4 billion). */
    if (is_bitmap_seed) {
        unsigned __int128 k3 = (unsigned __int128)candidates * candidates * candidates;
        unsigned __int128 want_n2 = (unsigned __int128)want * N * N;
        if (k3 < want_n2) return 1;
    }

    /* Cost model: K² < want × N (crossover where fetch+sort beats walk).
       Assumes uniform distribution — valid for non-bitmap seeds and
       large bitmap seeds (> 5% of N). */
    return candidates * candidates < want * N;
}

static FilterOrderKind pick_sort_or_walk(const char *db_root, const char *object,
                                         const char *order_by, CardEst se, size_t N,
                                         int offset, int limit, int is_bitmap_seed) {
    int driv = order_field_drivable(db_root, object, order_by);
    /* Unestimable/saturated → can't size the set; only walk if drivable. */
    if (!se.estimable || se.saturated)
        return driv ? FP_ORDER_INDEX_WALK : FP_ORDER_SORT;
    if (prefer_fetch_sort(se.k, N, offset, limit, is_bitmap_seed) || !driv)
        return FP_ORDER_SORT;
    return FP_ORDER_INDEX_WALK;
}

/* Flatten a tree into its AND-leaves (the implicit-AND children, or a lone
 * leaf). OR sub-trees and nested AND are handled by the caller; here we only
 * collect direct LEAF children for the per-leaf cost pass. Returns count. */
int collect_and_leaves(CriteriaNode *tree, SearchCriterion **out, int max) {
    int n = 0;
    if (!tree) return 0;
    if (tree->kind == CNODE_LEAF) { if (max>0){ out[0]=&tree->leaf; return 1;} return 0; }
    if (tree->kind == CNODE_AND) {
        for (int i=0;i<tree->n_children && n<max;i++)
            if (tree->children[i]->kind == CNODE_LEAF) out[n++]=&tree->children[i]->leaf;
    }
    return n;
}

/* plan_filter — central query planner.  Takes a criteria tree plus
 * caller intent (fetching, order_by, limit) and returns a FilterPlan
 * describing which executor + which source leaf(es) to use.
 *
 * `limit > 0` signals a limit-bounded find/aggregate — the executors
 * stop at `limit` rows, so seed-cardinality decisions that would
 * otherwise drive plan choice become moot (the streaming path walks
 * O(limit / match_rate) records regardless of total K).  Used by the
 * skip-card_est fast paths below to avoid the expensive `card_est_leaf`
 * btree walk when the saturated flag wouldn't change the outcome.
 * Pass 0 for unbounded queries (count, aggregate-without-limit, find
 * without limit). */
FilterPlan plan_filter(CriteriaNode *tree, const char *db_root,
                              const char *object, const FieldSchema *fs,
                              int splits, size_t N, const char *order_by,
                              int fetching, int limit) {
    FilterPlan fp; memset(&fp, 0, sizeof(fp));
    fp.fetching = fetching;
    fp.order = FP_ORDER_NONE;
    if (!tree) { fp.kind = FP_FULL_SCAN; return fp; }

    /* Declared before the OR/UNION `goto order_overlay` jumps below so those
     * paths can't skip its initialization (the overlay reads est[prim] only
     * under an fp.n_source>0 guard the UNION paths don't satisfy, but leaving
     * prim uninitialized on a jumped-over path is UB regardless). */
    int prim    = -1;
    int prim_it = IT_BTREE;   /* safe default for goto-overlay paths; set below */
    int prim_sel = 0;         /* safe default for goto-overlay paths; set below */
    SearchCriterion *leaves[MAX_INTERSECT_LEAVES];
    int nL = 0;               /* safe default for goto-overlay paths; set below */

    /* (1b.4) OR / hybrid handling.
     *
     * C1/C2: Pure OR root (CNODE_OR) or AND whose only children are OR nodes
     * (collect_and_leaves returns nL=0). Use find_fully_indexed_or which handles
     * both CNODE_OR root and CNODE_AND-with-OR-child.
     *   C1: every OR child indexed → UNION; 1b.5: goto order_overlay.
     *   C2: any non-indexed child → FULL_SCAN.
     *
     * C3: AND of indexed leaf + OR sub-tree: collect_and_leaves returns the
     * LEAF children only (skips the OR child). The OR sub-tree is automatically
     * excluded from `source`; has_subtree marks its presence so 1c's executor
     * rechecks the WHOLE tree via criteria_match_tree (completeness guarantee). */
    if (tree->kind == CNODE_OR) {
        CriteriaNode *orn = find_fully_indexed_or(tree, db_root, object);
        if (orn) {
            fp.kind = FP_UNION; fp.or_node = orn;
            goto order_overlay;
        }
        fp.kind = FP_FULL_SCAN; return fp;
    }

    nL = collect_and_leaves(tree, leaves, MAX_INTERSECT_LEAVES);
    if (nL == 0) {
        /* No LEAF children — could be CNODE_AND whose only children are OR
         * nodes (the array form [{"or":[...]}] produces this). Try the OR
         * fast path; if not fully indexed, fall to scan.
         *
         * KNOWN LIMITATION (matches the legacy choose_primary_source — not a
         * regression): find_fully_indexed_or returns only the FIRST fully-
         * indexed OR child. For an AND of multiple ORs (e.g.
         * [{"or":[a,b]},{"or":[c,d]}]) the union seeds on that first OR; the
         * remaining OR(s) are NOT index-narrowed here. Results stay CORRECT —
         * 1c's executor rechecks the WHOLE tree via criteria_match_tree per
         * fetched record — only the plan is suboptimal. AND-of-multiple-ORs is
         * outside the current decision table (rows C1-C3); revisit as its own
         * row + an OR-intersect seed if a real query needs it. */
        CriteriaNode *orn = find_fully_indexed_or(tree, db_root, object);
        if (orn) {
            fp.kind = FP_UNION; fp.or_node = orn;
            goto order_overlay;
        }
        fp.kind = FP_FULL_SCAN; return fp;
    }

    /* C3: an AND that also contains OR/nested-AND sub-trees alongside LEAF
     * children. collect_and_leaves already skipped those non-LEAF children, so
     * they are NOT in `source`. Mark has_subtree so 1c's executor knows to
     * recheck the full tree — never SKIP the per-record post-filter when an OR
     * sub-tree is present (the seed leaf still uses its index; the OR is
     * verified on each fetched record). */
    int has_subtree = 0;
    if (tree->kind == CNODE_AND)
        for (int i = 0; i < tree->n_children; i++)
            if (tree->children[i]->kind != CNODE_LEAF) has_subtree = 1;
    /* has_subtree forces full-tree recheck in 1c; add a FilterPlan field then.
     * The single-seed block below already post-filters all non-source leaves,
     * and 1c will recheck the whole tree on each fetched record. */
    (void)has_subtree;

    CardEst est[MAX_INTERSECT_LEAVES];

    /* Single-leaf fast path: skip the card_est_leaf walk entirely when
     * the saturated flag has no decision-changing effect downstream.
     * That's true for:
     *   - cmd_count / cmd_aggregate (fetching=0) — B5 is gated on
     *     `fetching` so it can't fire here.
     *   - cmd_find with NO order_by (fetching=1 && !order_by) — B5
     *     would fire for broad seeds and demote to FULL_SCAN, but
     *     that's the wrong call for find: the streaming path
     *     (idx_find_streaming) walks the seed and stops at limit, so
     *     it always beats a full-record scan regardless of K.  The
     *     order overlay (D2 vs D3) only runs when order_by is set;
     *     without it, the saturated flag is dead information.
     *
     * Without this fast path, find queries like `find age>50 limit=10`
     * at 25M scale pay a 3.1M-entry capped btree walk (~135ms warm)
     * in card_est_leaf BEFORE the streaming executor even starts —
     * pure planning overhead, no perf decision actually made.
     *
     * Find WITH order_by still calls card_est for the D2/D3 fork —
     * that decision IS saturated-flag-driven (bounded fetch+sort vs
     * order-index walk).  Multi-leaf cases (nL>=2) also call
     * card_est because most_selective_indexed picks the seed by K. */
    /* Skip-card_est fast path.  The 3M-entry capped btree walk inside
     * card_est_leaf is wasted overhead when the saturated flag can't
     * change the planner's decision.  Three concrete cases:
     *
     *   (a) Single-leaf count / aggregate (nL=1, fetching=0).
     *       B5 demotion is gated on `fetching`; saturated is dead
     *       information here. (PR #100)
     *
     *   (b) Single-leaf find without order_by (nL=1, fetching=1,
     *       !order_by).  B5 would demote broad seeds to FULL_SCAN,
     *       but the streaming executor walks the seed and stops at
     *       `limit`, which always beats a full-record scan when
     *       limit > 0.  Order overlay (D2 vs D3) is the only other
     *       consumer of saturated and runs only when order_by is set.
     *
     *   (c) Multi-leaf find without order_by AND with limit > 0
     *       (nL>=2, fetching=1, !order_by, limit>0).  Seed choice
     *       is essentially irrelevant for limit-bounded streaming —
     *       the executor walks O(limit / match_rate) records of any
     *       indexed seed.  Pretend all non-bitmap leaves are
     *       selective; pick the first indexed leaf as seed; the
     *       all-bitmap pure-intersect path stays correct because
     *       bitmaps still pay their (cheap, exact) popcount.
     *
     * Bitmaps in ALL cases pay their exact popcount card_est — it's a
     * cheap lookup, NOT a walk, and the result drives FP_PRIMARY_LEAF
     * vs FP_BITMAP_SMALLER classification (test_planA2_broad_bitmap).
     *
     * Multi-leaf count / aggregate (nL>=2, fetching=0) and any path
     * with order_by ALWAYS calls most_selective_indexed: count needs
     * real K to drive the intersect decisions (1 vs 2 selective
     * leaves changes the plan); order_by needs saturated for D2/D3.
     *
     * Measured wins at 25M scale, warm:
     *   find age>50  limit=10  : 134ms → 6ms   (22×)  case (b)
     *   find user_id>500K l=10 : 240ms → 7ms   (34×)  case (b)
     *   find {active=false AND age>50} limit=10 (c): see bench. */
    int skip_est_single = (nL == 1) &&
                          (!fetching || !(order_by && order_by[0]));
    int skip_est_multi  = (nL >= 2) && fetching && limit > 0 &&
                          !(order_by && order_by[0]);
    int skip_est = skip_est_single || skip_est_multi;
    prim = -1;
    if (skip_est) {
        for (int i = 0; i < nL; i++) {
            int it_i = pick_index_for_leaf(db_root, object, leaves[i]);
            if (it_i < 0) { est[i] = (CardEst){0,0,0}; continue; }
            if (it_i == IT_BITMAP) {
                const TypedField *tf_i = resolve_idx_field(fs->ts, leaves[i]->field);
                est[i] = card_est_leaf(db_root, object, splits, leaves[i],
                                       tf_i, selectivity_budget(N));
            } else {
                est[i] = (CardEst){ .k = 0, .saturated = 0, .estimable = 1 };
            }
            /* First indexed leaf becomes the seed.  For limit-bounded
             * find this choice is essentially irrelevant; for the
             * single-leaf cases (a, b) it's the only leaf. */
            if (prim < 0) prim = i;
        }
    } else {
        prim = most_selective_indexed(db_root, object, splits, leaves, nL, fs, N, est);
    }
    if (prim < 0) { fp.kind = FP_FULL_SCAN; return fp; }   /* nothing indexed → A5/B7 */

    /* Phase B: a composite fully pinned by eq leaves → exact key lookup,
     * skipping the two-index intersect. Only without order_by (Phase A owns
     * ordered prefix scans). */
    if (!(order_by && order_by[0]) && nL >= 2) {
        char cname[256];
        if (find_exact_covering_composite(db_root, object, fs, leaves, nL, cname)) {
            fp.kind = FP_PRIMARY_LEAF;
            fp.order = FP_ORDER_COMPOSITE_EXACT;
            snprintf(fp.composite_field, sizeof(fp.composite_field), "%s", cname);
            /* All covered leaves go in source_leaves (composite order) so the
             * executor can rebuild the key; siblings stay in the tree for
             * post-filter. n_source>0 satisfies downstream guards. */
            fp.n_source = 0;
            char tmp[256]; strncpy(tmp, cname, sizeof(tmp)-1); tmp[sizeof(tmp)-1]='\0';
            char *sv=NULL;
            for (char *sub=strtok_r(tmp,"+",&sv); sub && fp.n_source<MAX_INTERSECT_LEAVES;
                 sub=strtok_r(NULL,"+",&sv))
                for (int i=0;i<nL;i++)
                    if (leaves[i]->op==OP_EQUAL && strcmp(leaves[i]->field,sub)==0)
                        { fp.source_leaves[fp.n_source++]=leaves[i]; break; }
            return fp;
        }
    }

    /* prim is always >= 0 when we reach here (normal path); on goto-overlay
       paths prim stays -1 and prim_it/prim_sel keep their safe defaults. */
    if (prim >= 0) {
        prim_it  = pick_index_for_leaf(db_root, object, leaves[prim]);
        prim_sel = leaf_is_selective(est[prim], N);
    }

    /* Multi-leaf AND (Task 1b.3): count indexed + selective leaves among all
     * AND-leaves, then decide: pure-bitmap → intersect; count + 2 selective →
     * intersect; find + selective primary → fall to single-seed; all broad →
     * intersect-to-narrow. */
    {
        int n_indexed = 0, n_selective = 0, all_bitmap = 1;
        for (int i = 0; i < nL; i++) {
            int it = pick_index_for_leaf(db_root, object, leaves[i]);
            if (it < 0) continue;
            n_indexed++;
            if (it != IT_BITMAP) all_bitmap = 0;
            if (leaf_is_selective(est[i], N)) n_selective++;
        }
        if (n_indexed >= 2) {
            /* B3: pure-bitmap AND → popcount intersect (mirror choose_primary_source).
             * Fires regardless of fetching: bitmap intersect is always cheapest. */
            if (all_bitmap) {
                fp.kind = FP_INTERSECT; fp.source_is_bitmap = 1;
                for (int i = 0; i < nL; i++) {
                    int it = pick_index_for_leaf(db_root, object, leaves[i]);
                    if (it == IT_BITMAP && fp.n_source < MAX_INTERSECT_LEAVES)
                        fp.source_leaves[fp.n_source++] = leaves[i];
                    else if (fp.n_postfilter < MAX_INTERSECT_LEAVES)
                        fp.postfilter_leaves[fp.n_postfilter++] = leaves[i];
                }
                if (!fp.n_source) fp.kind = FP_FULL_SCAN; /* defensive */
                goto order_overlay;
            }
            /* COUNT (fetching==0) with ≥2 selective indexed leaves:
             * intersect index-only — no record reads needed for count. */
            if (!fetching && n_selective >= 2) {
                fp.kind = FP_INTERSECT;
                for (int i = 0; i < nL; i++) {
                    int it = pick_index_for_leaf(db_root, object, leaves[i]);
                    if (it >= 0 && leaf_is_selective(est[i], N)
                            && fp.n_source < MAX_INTERSECT_LEAVES)
                        fp.source_leaves[fp.n_source++] = leaves[i];
                    else if (fp.n_postfilter < MAX_INTERSECT_LEAVES)
                        fp.postfilter_leaves[fp.n_postfilter++] = leaves[i];
                }
                goto order_overlay;
            }
            /* FIND (fetching==1) with a selective primary seed: fall through to
             * the single-seed PRIMARY_LEAF block below — fetch the few, check
             * the rest on the record for free (B4's "fetch the 100, check rest").
             * Also the path for B2: selective btree + broad bitmap → leaf-on-btree. */
            if (fetching && prim_sel) { /* fall through to single-seed */ }
            else if (n_selective == 0) {
                /* All indexed leaves are broad. For find: intersect-to-narrow
                 * before fetch (build KeySets, intersect, then fetch+verify).
                 * For count: fall through to single-seed — walk only the
                 * least-broad index and post-filter the rest via the full
                 * criteria tree. This avoids building KeySets for each leaf
                 * (which can silently return 0 when memory exceeds budget). */
                if (fetching) {
                    fp.kind = FP_INTERSECT;
                    for (int i = 0; i < nL; i++) {
                        int it = pick_index_for_leaf(db_root, object, leaves[i]);
                        if (it >= 0 && fp.n_source < MAX_INTERSECT_LEAVES)
                            fp.source_leaves[fp.n_source++] = leaves[i];
                        else if (fp.n_postfilter < MAX_INTERSECT_LEAVES)
                            fp.postfilter_leaves[fp.n_postfilter++] = leaves[i];
                    }
                    goto order_overlay;
                }
                /* count: fall through to single-seed block below */
            }
            /* else: one selective leaf present, fetching=1 OR count with only
             * one selective leaf → fall through to single-seed PRIMARY_LEAF. */
        }
    }

    /* --- single effective seed --- */
    if (prim_it == IT_BITMAP && !prim_sel) {
        fp.kind = FP_BITMAP_SMALLER; fp.source_is_bitmap = 1;
    } else if (prim_sel || prim_it != IT_BITMAP) {
        /* selective btree/trigram/rare-bitmap → seed it; OR a broad non-bitmap
         * indexed leaf whose fetch still beats scan stays a leaf, else scan. */
        if (fetching && !prim_sel && prim_it != IT_BITMAP && prim_it != IT_TRIGRAM
                && est[prim].estimable && est[prim].saturated
                && op_eligible_for_intersect(leaves[prim]->op)
                && !order_field_drivable(db_root, object, order_by)) {
            /* broad non-bitmap non-trigram PRECISE-lookup leaf: K > budget →
             * fetch loses to a data scan (B5). Leaf-scan ops (contains/like/
             * ends/regex/len_* and their i-variants) are NOT demoted here —
             * their index leaf-scan always beats a data-file scan (A4).
             * Trigram-backed starts_with is similarly never demoted: it narrows
             * candidates via the leading 3-gram posting list, which is always
             * faster than a full seg-file scan regardless of candidate count.
             * Also NOT demoted when an indexed order_by is available (D3:
             * ORDER_INDEX_WALK is cheaper than scan-and-sort — the order
             * overlay below sets it for the saturated seed).
             *
             * Gated on `fetching` (set only by cmd_find).  For COUNT and
             * AGGREGATE the demotion is wrong: counting via an index walk
             * is always cheaper than a full-segment scan since the index
             * leaves are smaller and sorted (no per-record fetch, no
             * data-file I/O).  The pre-1c cmd_count went directly to
             * btree_idx_range with no plan-filter detour; restoring that
             * shape recovers the 2026.05.8-era count perf — at 25M scale,
             * `count gt age>50` (~11M matches) returns in ~1s via the
             * btree walk vs ~13s when this demotion sent it to scan
             * all 25M records. */
            fp.kind = FP_FULL_SCAN; return fp;
        }
        fp.kind = FP_PRIMARY_LEAF; fp.source_is_bitmap = (prim_it == IT_BITMAP);
    } else {
        fp.kind = FP_FULL_SCAN; return fp;
    }
    fp.source_leaves[0] = leaves[prim];
    fp.n_source = 1;
    /* every other leaf is a post-filter */
    for (int i=0;i<nL;i++) if (i!=prim && fp.n_postfilter<MAX_INTERSECT_LEAVES)
        fp.postfilter_leaves[fp.n_postfilter++] = leaves[i];

order_overlay:
    /* total is free whenever the plan materializes a candidate KeySet — the
     * count of candidates is known before any record fetch (1d). */
    fp.total_cheap = (fp.kind == FP_PRIMARY_LEAF || fp.kind == FP_INTERSECT ||
                      fp.kind == FP_UNION        || fp.kind == FP_BITMAP_SMALLER);

    /* order_by overlay (D1–D3): only when we have a source leaf to composite-
     * check against, and the plan is not a full scan (scans handle order in 1c
     * via a separate streaming merge — no overlay needed here). */
    if (order_by && order_by[0] && fp.kind != FP_FULL_SCAN && fp.n_source > 0) {
        /* D1: a (seed_field + order_by) composite index exists → the index
         * already delivers rows in (filter_field, order_by) order → sorted
         * prefix scan; no in-memory sort and no extra index walk needed.
         *
         * Gated on the seed op being EQ or STARTS_WITH: find_via_composite_prefix
         * bounds the walk as [encoded(value), encoded(value)+0xff*4] — that's
         * the prefix-range pattern. For GTE/LT/BETWEEN/IN seeds, those bounds
         * are wrong (cuts off the actual range we need to scan) and the
         * executor returns ~0 rows. Those ops fall through to ORDER_SORT /
         * ORDER_INDEX_WALK below, which correctly post-filter the criterion
         * per fetched record. (Symptom that surfaced: showcase trending
         * `time>=since order_by score` returned [] because the composite
         * prefix range didn't actually cover the seek range.) */
        /* D1 (preferred): any indexed eq/starts leaf whose <field>+<order_by>
         * composite exists drives a sorted prefix scan, independent of which
         * leaf most_selective_indexed chose. Fixes the bitmap-eq + ordered-range
         * shape (e.g. type=job ... ORDER BY time) that previously fell to D3. */
        int cc = find_covering_composite(db_root, object, leaves, nL, order_by);
        if (cc >= 0) {
            /* Find an order_by range/eq leaf (used both for the range-fold and
             * the guard below). EQ-seed only — STARTS seeds don't fold. */
            SearchCriterion *obr = NULL;
            if (leaves[cc]->op == OP_EQUAL) {
                for (int i = 0; i < nL; i++) {
                    if (strcmp(leaves[i]->field, order_by) != 0) continue;
                    enum SearchOp o = leaves[i]->op;
                    if (o == OP_GREATER || o == OP_GREATER_EQ || o == OP_LESS ||
                        o == OP_LESS_EQ || o == OP_BETWEEN || o == OP_EQUAL) {
                        obr = leaves[i];
                        break;
                    }
                }
            }

            /* Selectivity guard. A composite ordered by order_by walks the whole
             * seed prefix and post-filters siblings, so it's only cheap when:
             *   - the seed prefix is itself selective (small partition), OR
             *   - order_by carries the range (walk seeks / fills fast), OR
             *   - there's no more-selective sibling to drive a better plan.
             * When the seed is provably broad AND a selective sibling exists on
             * a non-order_by field AND order_by has no range, the composite walk
             * would scan the whole broad partition (e.g. type=story ≈ all rows,
             * ORDER BY score, time>=T post-filtered) — far worse than letting the
             * selective sibling (time>=T) drive a fetch+sort. Skip it. */
            int seed_broad = est[cc].estimable && !leaf_is_selective(est[cc], N);
            int has_sel_other = 0;
            for (int i = 0; i < nL; i++) {
                if (i == cc) continue;
                if (strcmp(leaves[i]->field, order_by) == 0) continue; /* order_by sibling → range-fold */
                if (leaf_is_selective(est[i], N)) { has_sel_other = 1; break; }
            }
            int skip_composite = seed_broad && !obr && has_sel_other;
            /* Also skip when a non-order_by sibling is more selective (smaller K).
               The composite prefix scan walks the entire seed partition — if another
               leaf can produce fewer candidates, it's cheaper to filter via that leaf
               and then sort or walk the order_by index (D2/D3). Range-fold (obr)
               already limits the walk, so skip when obr is set. */
            if (!skip_composite && !obr && est[cc].estimable) {
                for (int i = 0; i < nL && !skip_composite; i++) {
                    if (i == cc || strcmp(leaves[i]->field, order_by) == 0) continue;
                    if (est[i].estimable && leaf_is_selective(est[i], N) &&
                        est[i].k < est[cc].k)
                        skip_composite = 1;
                }
            }

            if (!skip_composite) {
                fp.kind             = FP_PRIMARY_LEAF;  /* composite executor requires this */
                fp.source_is_bitmap = (pick_index_for_leaf(db_root, object, leaves[cc]) == IT_BITMAP);
                fp.source_leaves[0] = leaves[cc];
                fp.n_source         = 1;
                fp.order            = FP_ORDER_COMPOSITE;
                fp.order_range      = obr;
                /* Populate postfilter_leaves with all non-composite-seed leaves */
                fp.n_postfilter = 0;
                for (int i = 0; i < nL && fp.n_postfilter < MAX_INTERSECT_LEAVES; i++) {
                    if (i != cc) {
                        fp.postfilter_leaves[fp.n_postfilter++] = leaves[i];
                    }
                }
                /* prefilter_card: smallest estimable K among non-cc leaves.
                   In the cursor path this overrides the composite seed's KeySet size
                   for the prefer_fetch_sort decision — without it, a broad composite
                   seed (e.g. type=job ~17k) masks a narrow real match set
                   (time>=T ~6) and the cursor wrongly chooses walk-over-limit. */
                fp.prefilter_card = 0;
                fp.prefilter_source_leaf = NULL;
                size_t best_k = SIZE_MAX;
                int best_i = -1;
                for (int i_ = 0; i_ < nL; i_++) {
                    if (i_ == cc) continue;
                    if (est[i_].estimable && !est[i_].saturated && est[i_].k < best_k) {
                        best_k = est[i_].k;
                        best_i = i_;
                    }
                }
                if (best_k < SIZE_MAX) {
                    fp.prefilter_card = best_k;
                    fp.prefilter_source_leaf = leaves[best_i];
                }
            } else {
                /* Drive on the most-selective seed instead (pre-overlay fp.kind /
                 * source_leaves already point at prim). D2 if bounded, else D3. */
                CardEst se = est[prim];
                fp.order = pick_sort_or_walk(db_root, object, order_by, se, N, 0, limit,
                                            (prim >= 0) ? (prim_it == IT_BITMAP) : 0);
            }
        } else if (composite_index_exists(db_root, object,
                                    fp.source_leaves[0]->field, order_by)
            && (fp.source_leaves[0]->op == OP_EQUAL ||
                fp.source_leaves[0]->op == OP_STARTS_WITH)) {
            fp.order = FP_ORDER_COMPOSITE;
            fp.order_range = NULL;
        } else {
            /* D2 vs D3: if the seed leaf's candidate set is bounded (estimable
             * and not saturated), fetch + sort in memory (D2).  When it's broad
             * (saturated / unestimable), walk the order_by index directly (D3). */
            /* Reuse est[prim] from most_selective_indexed. When order_by
             * is set, skip_est is always false, so est[] is fully populated.
             * On goto paths (B3 all-bitmap, all-broad intersect),
             * fp.source_leaves[0] may differ from leaves[prim], but the
             * D2/D3 fork depends only on saturated/estimable which are
             * uniform across same-type leaves. Do not relax the skip_est
             * condition without re-evaluating this reuse. */
            /* prim is always ≥0 here (guarded by `if (prim < 0) return` above);
             * the explicit check keeps -Wmaybe-uninitialized quiet across the
             * added composite branches without changing behaviour. */
            CardEst se = (prim >= 0) ? est[prim] : (CardEst){0, 0, 0};
            fp.order = pick_sort_or_walk(db_root, object, order_by, se, N, 0, limit,
                                        (prim >= 0) ? (prim_it == IT_BITMAP) : 0);
        }
    }
    return fp;
}

/* Does the planner's prefilter for `tree` represent the ENTIRE criteria tree?
 *
 * The aggregate group_by fast-paths (IGB walk, streaming top-N, no-group
 * keyset) filter their index walk by ONE prefilter KeySet built from the
 * planner's PRIMARY and have no per-record recheck. That's only correct when
 * the prefilter == the whole filter. A partial intersect (find_intersect_leaves
 * dropped a bitmap / non-rangeable / non-indexed leaf) or a PRIMARY_LEAF chosen
 * out of a multi-leaf AND leaves siblings unapplied → those fast-paths would
 * silently drop them (wrong counts). When this returns 0 the caller must fall
 * through to agg_run_plan, which fetches the selective candidates and rechecks
 * the full tree via agg_scan_cb (the "scan the small set, verify the rest"
 * path). Returns 1 for no-criteria (nothing to drop). */
int agg_criteria_fully_covered(const char *db_root, const char *object,
                                      CriteriaNode *tree) {
    if (!tree) return 1;
    /* Phase 1c.5: equivalent to (PRIMARY_INTERSECT && !partial) ||
     * PRIMARY_KEYSET || (PRIMARY_LEAF && single_leaf).
     * n_postfilter == 0 means every leaf is in the source set; nothing
     * is left to verify per-record, so the index walk alone covers the
     * full criteria tree. */
    FieldSchema fs; init_field_schema(&fs, db_root, object);
    Schema sc = load_schema(db_root, object);
    size_t N = (size_t)get_live_count(db_root, object);
    FilterPlan fp = plan_filter(tree, db_root, object, &fs, sc.splits, N,
                                NULL /*order_by*/, 0 /*fetching*/, 0 /*limit*/);
    return (fp.kind != FP_FULL_SCAN && fp.n_postfilter == 0);
}

#ifdef TEST_BUILD
/* Per-capability test hooks — let test_planner_op_capability.c verify the
 * OpCaps table directly without caring about the static linkage. */
int op_intersect_eligible_for_test(enum SearchOp op) { return op_caps(op).intersect; }
int op_composite_seed_eligible_for_test(enum SearchOp op) { return op_caps(op).composite_seed; }
int op_composite_exact_eligible_for_test(enum SearchOp op) { return op_caps(op).composite_exact; }
int op_order_bound_eligible_for_test(enum SearchOp op) { return op_caps(op).order_bound; }
int op_trigram_prefers_for_test(enum SearchOp op) { return op_caps(op).trigram_prefers; }
int op_trigram_starts_for_test(enum SearchOp op) { return op_caps(op).trigram_starts; }
int op_selectivity_rank_for_test(enum SearchOp op) { return op_caps(op).rank; }

 static const char *fp_kind_str(FilterPlanKind k){
     switch(k){case FP_FULL_SCAN:return "scan";case FP_PRIMARY_LEAF:return "leaf";
     case FP_BITMAP_SMALLER:return "bitmap";case FP_INTERSECT:return "intersect";
     case FP_UNION:return "union";} return "?"; }
 static const char *fp_order_str(FilterOrderKind o){
     switch(o){case FP_ORDER_NONE:return "none";case FP_ORDER_COMPOSITE:return "composite";
     case FP_ORDER_COMPOSITE_EXACT:return "composite_exact";
     case FP_ORDER_SORT:return "sort";case FP_ORDER_INDEX_WALK:return "walk";} return "?"; }

const char *plan_filter_kind_for_test(const char *db_root, const char *object,
        const char *criteria_json, const char *order_by, int fetching,
        char *out_field, size_t fsz, char *out_order, size_t osz,
        int *out_total_cheap) {
    if (out_field && fsz) out_field[0]='\0';
    if (out_order && osz) out_order[0]='\0';
    if (out_total_cheap) *out_total_cheap = -1;
    snprintf(g_db_root, PATH_MAX, "%s", db_root);
    char eff_root[PATH_MAX], bare[256];
    const char *slash = strchr(object,'/');
    if (slash){size_t d=(size_t)(slash-object);
        snprintf(eff_root,sizeof(eff_root),"%s/%.*s",db_root,(int)d,object);
        snprintf(bare,sizeof(bare),"%s",slash+1);
    } else { snprintf(eff_root,sizeof(eff_root),"%s",db_root);
        snprintf(bare,sizeof(bare),"%s",object); }
    const char *err=NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &err);
    if (!tree) return "parse_error";
    Schema sc = load_schema(eff_root, bare);
    FieldSchema fs; init_field_schema(&fs, eff_root, bare);
    size_t N = (size_t)get_live_count(eff_root, bare);
    FilterPlan fp = plan_filter(tree, eff_root, bare, &fs, sc.splits, N, order_by, fetching, 0);
    if (out_field && fsz && fp.n_source>0 && fp.source_leaves[0])
        snprintf(out_field, fsz, "%s", fp.source_leaves[0]->field);
    if (out_order && osz) snprintf(out_order, osz, "%s", fp_order_str(fp.order));
    if (out_total_cheap) *out_total_cheap = fp.total_cheap;
    free_criteria_tree(tree);
    return fp_kind_str(fp.kind);
}
#endif

#ifdef TEST_BUILD
/* Expose leaf_is_selective for a single eq leaf. Returns 1/0; writes K to
 * *out_k. Mirrors card_est_by_field's dir/obj split + g_db_root setup. */
int leaf_selective_for_test(const char *db_root, const char *object,
                            const char *field, const char *value, size_t *out_k) {
    snprintf(g_db_root, PATH_MAX, "%s", db_root);
    char eff_root[PATH_MAX], bare[256];
    const char *slash = strchr(object, '/');
    if (slash) { size_t d=(size_t)(slash-object);
        snprintf(eff_root,sizeof(eff_root),"%s/%.*s",db_root,(int)d,object);
        snprintf(bare,sizeof(bare),"%s",slash+1);
    } else { snprintf(eff_root,sizeof(eff_root),"%s",db_root);
        snprintf(bare,sizeof(bare),"%s",object); }
    Schema sc = load_schema(eff_root, bare);
    FieldSchema fs; init_field_schema(&fs, eff_root, bare);
    SearchCriterion leaf; memset(&leaf,0,sizeof(leaf));
    snprintf(leaf.field,sizeof(leaf.field),"%s",field);
    leaf.op = OP_EQUAL;
    snprintf(leaf.value,sizeof(leaf.value),"%s",value);
    const TypedField *tf = resolve_idx_field(fs.ts, field);
    size_t N = (size_t)get_live_count(eff_root, bare);
    CardEst e = card_est_leaf(eff_root, bare, sc.splits, &leaf, tf, selectivity_budget(N));
    if (out_k) *out_k = e.k;
    return leaf_is_selective(e, N);
}
#endif

#ifdef TEST_BUILD
/* Test hook: which sides of the order-by walk got bounded by a range/eq leaf
 * on order_by. Mirrors plan_filter_kind_for_test's dir/object split. */
int order_walk_bounds_for_test(const char *db_root, const char *object,
                               const char *criteria_json, const char *order_by,
                               int *out_has_lo, int *out_has_hi) {
    snprintf(g_db_root, PATH_MAX, "%s", db_root);
    char eff_root[PATH_MAX], bare[256];
    const char *slash = strchr(object, '/');
    if (slash) { size_t d=(size_t)(slash-object);
        snprintf(eff_root,sizeof(eff_root),"%s/%.*s",db_root,(int)d,object);
        snprintf(bare,sizeof(bare),"%s",slash+1);
    } else { snprintf(eff_root,sizeof(eff_root),"%s",db_root);
        snprintf(bare,sizeof(bare),"%s",object); }
    const char *err = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &err);
    if (!tree) return -1;
    FieldSchema fs; init_field_schema(&fs, eff_root, bare);
    OrderWalkBounds b;
    order_walk_bounds(tree, &fs, order_by, &b);
    if (out_has_lo) *out_has_lo = b.has_lo;
    if (out_has_hi) *out_has_hi = b.has_hi;
    free_criteria_tree(tree);
    return 0;
}
#endif

#ifdef TEST_BUILD
extern size_t g_ordered_find_keyset_max;
void set_ordered_find_keyset_max_for_test(size_t v) { g_ordered_find_keyset_max = v; }
size_t get_ordered_find_keyset_max_for_test(void)   { return g_ordered_find_keyset_max; }
#endif

