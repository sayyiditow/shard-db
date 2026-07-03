# FT_DATETIMEMS field type (closes GitHub issue #8)

## Execution rules (read before starting)

- Branch off `main`: `git checkout -b feat/datetimems-field-type`.
- Do the tasks in the order listed. Each task is independently buildable/testable.
- Build with `SKIP_TESTS=1 ./build.sh` after each task; run the full type's test only at
  the end with `./build/bin/shard-db-test run test-datetimems`, then the full suite with
  `./build/bin/shard-db-test run-all`.
- Never claim a step passed without pasting the real command output.
- Every insertion point below is located by **quoted anchor text**, not line numbers —
  line numbers drift because another branch may be landing changes concurrently (e.g.
  the ipv4 field type plan touches many of the same functions). If a quoted anchor is
  not found verbatim in the file, **stop** and write `PLAN_NOTES.md` at the repo root
  describing exactly what you searched for and what the surrounding code looks like
  instead. Do not guess or improvise a fix.
- Leave all work **uncommitted** when done. Do not `git commit`, `git push`, or open a
  PR — that happens outside this session.

## Design summary

New field type `datetimems`: a calendar datetime with millisecond precision.

- **Wire format** (JSON in/out): 17-digit string `"yyyyMMddHHmmssfff"` (14-digit
  calendar + 3-digit milliseconds), e.g. `"20260703161530123"`. Digit-only input is
  tolerated with right-zero-padding if short (mirrors `FT_DATETIME`'s existing
  convention — see `parse_datetime` in query.c).
- **On-disk storage**: 8 bytes = 4-byte BE `int32` calendar date (`yyyyMMdd`, same
  encoding as `FT_DATE`) followed by 4-byte BE `uint32` milliseconds-of-day
  (`0..86399999`, i.e. `hh*3600000 + mm*60000 + ss*1000 + fff`).
- **Index-key ordering**: top bit of byte 0 (the date's leading byte) is flipped for
  signed-to-unsigned total order, exactly like `FT_DATE`/`FT_DATETIME`. The ms-of-day
  component is `uint32` and never has its top bit set in the valid range, so it is
  **not** flipped — direct memcmp on the 4 ms bytes already gives correct unsigned
  order.
- **Zero/unset convention**: `date == 0 && ms == 0` is treated as "empty" — mirrors
  `FT_DATETIME`'s `d == 0 && t == 0` check throughout query.c.
- **Comparisons**: composed as `int64_t v = (int64_t)date * 100000000LL + (int64_t)ms`.
  The multiplier is `1e8` (not `FT_DATETIME`'s `1e5`) because `ms` ranges up to
  `86,399,999` (8 digits), vs. `FT_DATETIME`'s packed `HHmmss` field which only needs 6
  digits (`1e5` multiplier). `date` magnitudes (~8 digits) times `1e8` stay well inside
  `int64_t` range.
- **No IN-list support**: mirrors `FT_DATETIME`, which also has no `in_datetime`/compile
  case — pre-existing limitation, not new scope.
- **`auto_create`/`auto_update` defaults**: supported, generating "now" at ms
  precision via `clock_gettime(CLOCK_REALTIME)` + `localtime_r`.
- **Bug fix bundled per user decision**: the 3 bulk-update worker `*_value_compute`
  functions in query.c stamp `auto_update` fields using second-precision
  `time(NULL)`/`localtime_r` for **every** type including `FT_TIMESTAMP` — which is
  wrong, since `FT_TIMESTAMP` is epoch-milliseconds and `storage.c`'s `cmd_update_v2`
  already correctly special-cases it via `clock_gettime(CLOCK_REALTIME)`. This plan
  fixes all 3 call sites to match `cmd_update_v2`'s correct `FT_TIMESTAMP` handling,
  and adds the new `FT_DATETIMEMS` branch alongside it in all 4 places (the 3 bulk
  sites + `cmd_update_v2` itself).

---

## Task 1 — `src/db/types.h`: enum + struct

### 1a. Insert `FT_DATETIMEMS` into `enum FieldType`

Anchor (exact, in the enum body):

```c
    FT_DATE,        /* date — 4 bytes int32 yyyyMMdd big-endian */
    FT_DATETIME,    /* datetime — 6 bytes packed yyyyMMddHHmmss big-endian */
    FT_TIME,        /* time — 3 bytes uint24 big-endian (seconds since midnight) */
```

Replace with:

```c
    FT_DATE,        /* date — 4 bytes int32 yyyyMMdd big-endian */
    FT_DATETIME,    /* datetime — 6 bytes packed yyyyMMddHHmmss big-endian */
    FT_DATETIMEMS,  /* datetimems — 8 bytes: int32 BE yyyyMMdd date + uint32 BE
                       ms-of-day (0..86399999). Wire format is the 17-digit
                       string "yyyyMMddHHmmssfff". */
    FT_TIME,        /* time — 3 bytes uint24 big-endian (seconds since midnight) */
```

**Invariant**: enum ordinal values are never disk-persisted (schemas are re-derived
from `fields.conf` text on every load), so this insertion position is safe and does not
require a migration.

### 1b. Add `dm1, dm2` to `CompiledCriterion`

Anchor (exact):

```c
    uint16_t t1, t2;
    uint8_t  b1;
```

Replace with:

```c
    uint16_t t1, t2;
    int32_t  dm1, dm2;  /* FT_DATETIMEMS ms-of-day bounds (0..86399999) */
    uint8_t  b1;
```

Build check: `SKIP_TESTS=1 ./build.sh` should still succeed (no functional change yet,
just new dead enum value + unused struct fields).

---

## Task 2 — `src/db/config.c`: parse, encode, decode, defaults

### 2a. `parse_field_type` — recognize `"datetimems"`

Anchor (exact):

```c
    } else if (strcmp(spec, "datetime") == 0) {
        f->type = FT_DATETIME;
        f->size = 6;
    } else if (strcmp(spec, "time") == 0) {
        f->type = FT_TIME;
        f->size = 3;
```

Replace with:

```c
    } else if (strcmp(spec, "datetime") == 0) {
        f->type = FT_DATETIME;
        f->size = 6;
    } else if (strcmp(spec, "datetimems") == 0) {
        f->type = FT_DATETIMEMS;
        f->size = 8;
    } else if (strcmp(spec, "time") == 0) {
        f->type = FT_TIME;
        f->size = 3;
```

### 2b. `encode_field_len` — new `FT_DATETIMEMS` case

Locate the `FT_DATETIME` case inside `encode_field_len`'s switch (it precedes the
`FT_TIME` case). Anchor on the case label plus the following case's label to place the
new block precisely between them:

```c
    case FT_TIME: {
```

Insert immediately **before** that line (i.e. right after the closing `break;` and `}`
of the preceding `FT_DATETIME` case) the following new case:

```c
    case FT_DATETIMEMS: {
        /* Parse up to 17 digits: yyyyMMddHHmmssfff. Short input zero-pads
           on the right (mirrors FT_DATETIME's convention). */
        char clean[24]; int ci = 0;
        for (size_t i = 0; i < vlen && ci < 17; i++)
            if (val[i] >= '0' && val[i] <= '9') clean[ci++] = val[i];
        while (ci < 17) clean[ci++] = '0';
        clean[17] = '\0';
        char datebuf[9]; memcpy(datebuf, clean, 8); datebuf[8] = '\0';
        int32_t d = (int32_t)atoi(datebuf);
        out[0] = (d >> 24) & 0xFF; out[1] = (d >> 16) & 0xFF;
        out[2] = (d >> 8) & 0xFF;  out[3] = d & 0xFF;
        int hh = (clean[8]-'0')*10 + (clean[9]-'0');
        int mm = (clean[10]-'0')*10 + (clean[11]-'0');
        int ss = (clean[12]-'0')*10 + (clean[13]-'0');
        int fff = (clean[14]-'0')*100 + (clean[15]-'0')*10 + (clean[16]-'0');
        uint32_t ms = (uint32_t)((hh * 3600 + mm * 60 + ss) * 1000 + fff);
        out[4] = (ms >> 24) & 0xFF; out[5] = (ms >> 16) & 0xFF;
        out[6] = (ms >> 8) & 0xFF;  out[7] = ms & 0xFF;
        break;
    }
```

**Edge case**: empty/absent value (`vlen == 0`) → `clean` fills entirely with `'0'`
padding → `d = 0`, `ms = 0` → the all-zero "unset" sentinel, consistent with
`FT_DATETIME`'s empty-input behavior.

### 2c. `encode_field_for_index` — new `FT_DATETIMEMS` case

Same anchor strategy: this case must be inserted between the existing `FT_DATETIME`
case and the `FT_TIME` case in `encode_field_for_index`'s switch. The `FT_TIME` case in
this function begins:

```c
    case FT_TIME: {
```

Insert immediately before it:

```c
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
```

Note this case flips the date's top bit (`^ 0x80000000u`) for total signed order,
matching `FT_DATE`/`FT_DATETIME` in the same function; the ms bytes are unflipped.

### 2d. `typed_field_to_index_key` — new `FT_DATETIMEMS` case

This function operates on an already-encoded on-disk field (`src` = the 8 stored
bytes), producing an index key. Anchor on the existing block:

```c
    case FT_DATETIME: {
        memcpy(out, src, 6);
        out[0] ^= 0x80;
        *out_len = 6;
        break;
    }
    case FT_TIME: {
```

Replace with:

```c
    case FT_DATETIME: {
        memcpy(out, src, 6);
        out[0] ^= 0x80;
        *out_len = 6;
        break;
    }
    case FT_DATETIMEMS: {
        /* int32 BE date (flip) + uint32 BE ms-of-day (already unsigned-sortable). */
        memcpy(out, src, 8);
        out[0] ^= 0x80;
        *out_len = 8;
        break;
    }
    case FT_TIME: {
```

### 2e. `decode_field_to_buf` — new `FT_DATETIMEMS` case

Anchor on the `FT_DATETIME` case's end and `FT_TIME` case's start in this function
(the JSON-emission path — output must be double-quoted since it's textual):

```c
    case FT_TIME: {
```

Insert immediately before it:

```c
    case FT_DATETIMEMS: {
        int32_t d = ((int32_t)data[0] << 24) | ((int32_t)data[1] << 16) |
                    ((int32_t)data[2] << 8) | data[3];
        uint32_t ms = ((uint32_t)data[4] << 24) | ((uint32_t)data[5] << 16) |
                      ((uint32_t)data[6] << 8) | data[7];
        if (d == 0 && ms == 0) return 0;
        int hh = ms / 3600000;
        int mm = (ms % 3600000) / 60000;
        int ss = (ms % 60000) / 1000;
        int fff = ms % 1000;
        return snprintf(buf, buflen, "\"%08d%02d%02d%02d%03d\"", d, hh, mm, ss, fff);
    }
```

Check the exact local variable name used for the raw field pointer in this function
(the surrounding `FT_DATE`/`FT_DATETIME` cases use `data` as the pointer name per the
existing code — confirm this matches; if the function instead uses a differently-named
pointer parameter, use that name instead of `data` throughout this new case).

### 2f. `typed_get_field_str` — new `FT_DATETIMEMS` case

This function returns a malloc'd, **unquoted** string (used outside JSON emission).
Anchor on the `FT_TIME` case's start:

```c
    case FT_TIME: {
```

Insert immediately before it:

```c
    case FT_DATETIMEMS: {
        const uint8_t *d = src + f->offset;
        int32_t dv = ((int32_t)d[0] << 24) | ((int32_t)d[1] << 16) |
                     ((int32_t)d[2] << 8) | d[3];
        uint32_t ms = ((uint32_t)d[4] << 24) | ((uint32_t)d[5] << 16) |
                      ((uint32_t)d[6] << 8) | d[7];
        if (dv == 0 && ms == 0) return NULL;
        int hh = ms / 3600000, mm = (ms % 3600000) / 60000,
            ss = (ms % 60000) / 1000, fff = ms % 1000;
        char *out = malloc(18);
        if (!out) return NULL;
        snprintf(out, 18, "%08d%02d%02d%02d%03d", dv, hh, mm, ss, fff);
        return out;
    }
```

Confirm the exact names of `src`/`f`/`data`-pointer parameters used by the surrounding
`FT_DATE`/`FT_DATETIME`/`FT_TIME` cases in this function and match them (the names above
are best-effort based on prior verification — if `typed_get_field_str`'s parameter
names differ, use the function's actual parameter/variable names instead).

### 2g. New helper `gen_datetimems_now`

Anchor on the existing `gen_date_now` function — insert the new helper immediately
after its closing brace:

```c
static void gen_date_now(char *buf, size_t bufsz) {
```

(Locate the full body of `gen_date_now` and insert the new function directly after its
closing `}`.) New function:

```c
/* Current date+time-of-day with millisecond precision as
   "yyyyMMddHHmmssfff" into buf (>= 18 bytes). Used by FT_DATETIMEMS's
   auto_create / auto_update generators. */
static void gen_datetimems_now(char *buf, size_t bufsz) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    time_t now = ts.tv_sec;
    struct tm tm;
    localtime_r(&now, &tm);
    int msec = (int)(ts.tv_nsec / 1000000L);
    snprintf(buf, bufsz, "%04d%02d%02d%02d%02d%02d%03d",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
}
```

### 2h. `generate_default` — dispatch to the new generator

Anchor (exact):

```c
        case DK_AUTO_CREATE:
        case DK_AUTO_UPDATE:
            if (tf->type == FT_TIMESTAMP)
                gen_timestamp_now(gen_buf, bufsz);
            else if (tf->type == FT_DATETIME)
                gen_datetime_now(gen_buf, bufsz);
            else if (tf->type == FT_DATE)
                gen_date_now(gen_buf, bufsz);
            else
                gen_datetime_now(gen_buf, bufsz); /* fallback for varchar etc. */
            return gen_buf;
```

Replace with:

```c
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
```

Build check: `SKIP_TESTS=1 ./build.sh` should succeed.

---

## Task 3 — `src/db/index.c`: `typed_field_str_avg`

Anchor (exact):

```c
    case FT_DATETIME: return 6;
```

Replace with:

```c
    case FT_DATETIME: return 6;
    case FT_DATETIMEMS: return 8;
```

---

## Task 4 — `src/db/query.c`: helpers, compile, compare, match

### 4a. New `ld_be_u32` helper

Anchor — the existing helper block ends with `ld_be_u16`. Find:

```c
static inline uint16_t ld_be_u16(const uint8_t *p) {
    return ((uint16_t)p[0] << 8) | (uint16_t)p[1];
}
```

Insert immediately after it:

```c
static inline uint32_t ld_be_u32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}
```

(If `ld_be_u16`'s body differs in whitespace from the quoted text above, match on the
function signature line `static inline uint16_t ld_be_u16(const uint8_t *p) {` and
insert the new helper after that function's closing `}`.)

### 4b. New `parse_datetimems` helper

Anchor — insert immediately after the existing `parse_datetime` function's closing
brace (locate `static void parse_datetime(...)` and its body, insert directly after):

```c
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
```

### 4c. `compile_one` — new scalar case

Anchor on the existing `FT_DATETIME` case in `compile_one`'s scalar switch:

```c
    case FT_DATETIME: {
        int32_t d; uint16_t t;
        parse_datetime(c->value, &d, &t); cc->i1 = d; cc->t1 = t;
        parse_datetime(c->value2, &d, &t); cc->i2 = d; cc->t2 = t;
        break;
    }
```

Replace with (inserting the new case immediately after, before whatever case follows —
`FT_UUID` per prior verification):

```c
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
```

**No IN-list case is added** for `FT_DATETIMEMS` — this mirrors `FT_DATETIME`, which
also lacks an IN-list compile case in the same function. Confirmed pre-existing,
accepted limitation (not new scope for this plan).

### 4d. `cmp_typed_field_pair` — new case

Anchor on the existing `FT_DATETIME` case:

```c
    case FT_DATETIME: {
```

(Locate this case's full body — composes `d*100000+t` from the two 6-byte buffers and
returns the signed comparison — and insert the new case immediately after its closing
`}`, before the following case, which is `FT_TIME` per prior verification):

```c
    case FT_DATETIMEMS: {
        int64_t va = (int64_t)ld_be_i32(a) * 100000000LL + (int64_t)ld_be_u32(a + 4);
        int64_t vb = (int64_t)ld_be_i32(b) * 100000000LL + (int64_t)ld_be_u32(b + 4);
        return va < vb ? -1 : (va > vb ? 1 : 0);
    }
```

### 4e. `match_typed` — new case (the main filter path)

Anchor on the full existing `FT_DATETIME` case body:

```c
    case FT_DATETIME: {
        int64_t d = (int64_t)ld_be_i32(p);
        uint16_t t = ld_be_u16(p + 4);
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
```

Insert immediately after this case's closing `}` (before the following `case FT_TIME:`):

```c
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
```

### 4f. `buf_field_value` — add to the quoted-string case list (JOIN row emission)

Anchor (exact):

```c
        case FT_DATE:
        case FT_DATETIME:
        case FT_ENUM:
            n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
            if (n <= 0) return snprintf(buf, bufsz, "null");
            return snprintf(buf, bufsz, "\"%s\"", tmp);
```

Replace with:

```c
        case FT_DATE:
        case FT_DATETIME:
        case FT_DATETIMEMS:
        case FT_ENUM:
            n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
            if (n <= 0) return snprintf(buf, bufsz, "null");
            return snprintf(buf, bufsz, "\"%s\"", tmp);
```

*Aside (out of scope, not to be fixed here)*: this same function's `default:` branch
currently emits `FT_UUID` **unquoted**, which is invalid JSON for UUID fields projected
through a JOIN (the primary find/get path via `decode_field_to_buf` in config.c quotes
UUID correctly — this bug is specific to the JOIN row-emission path only). This is a
pre-existing, unrelated bug; it is called out here for visibility but is **not** part
of this plan's scope.

### 4g. `typed_field_is_numeric` — include `FT_DATETIMEMS`

Anchor (exact):

```c
static int typed_field_is_numeric(uint8_t ft) {
    return ft == FT_INT || ft == FT_LONG || ft == FT_SHORT || ft == FT_DOUBLE ||
           ft == FT_NUMERIC || ft == FT_DATE || ft == FT_DATETIME ||
           ft == FT_BOOL || ft == FT_BYTE;
}
```

Replace with:

```c
static int typed_field_is_numeric(uint8_t ft) {
    return ft == FT_INT || ft == FT_LONG || ft == FT_SHORT || ft == FT_DOUBLE ||
           ft == FT_NUMERIC || ft == FT_DATE || ft == FT_DATETIME ||
           ft == FT_DATETIMEMS || ft == FT_BOOL || ft == FT_BYTE;
}
```

**Invariant**: this makes `FT_DATETIMEMS` eligible for aggregate/order-by numeric
treatment, consistent with `FT_DATETIME`. `FT_UUID`/`FT_TIME` are deliberately absent
from this list (byte-lex sort instead) — do not add `FT_DATETIMEMS` there; it belongs
here because it mirrors `FT_DATETIME`'s existing inclusion.

### 4h. `field_type_str` — new case

Anchor on the existing `FT_DATETIME` case:

```c
    case FT_DATETIME: return "datetime";
```

Insert immediately after:

```c
    case FT_DATETIMEMS: return "datetimems";
```

(If the actual case body differs slightly in format, e.g. no `return` on the same
line, apply the same case-label + string pairing using the function's existing style.)

### 4i. `typed_field_to_buf_raw` — new case (unquoted raw string, used by `buf_field_value`)

Anchor on the existing `FT_DATETIME` case (14-digit unquoted output) — insert the new
case immediately after its closing `}`, before `case FT_TIME:`:

```c
    case FT_DATETIMEMS: {
        int32_t d = ld_be_i32(p);
        uint32_t ms = ld_be_u32(p + 4);
        if (d == 0 && ms == 0) return 0;
        int hh = ms / 3600000, mm = (ms % 3600000) / 60000,
            ss = (ms % 60000) / 1000, fff = ms % 1000;
        return snprintf(buf, bufsz, "%08d%02d%02d%02d%03d", d, hh, mm, ss, fff);
    }
```

Confirm the pointer variable name (`p`) matches what the surrounding `FT_DATETIME` case
in this specific function actually uses; adjust if different.

### 4j. `decode_index_key_to_double` — new case ("summable" for aggregates over indexes)

Anchor on the existing `FT_DATETIME` case:

```c
    case FT_DATETIME: {
```

(Full body composes `*out = (double)d * 1000000.0 + (double)t; return 1;` from the
6-byte flipped index key.) Insert immediately after its closing `}`, before
`case FT_TIME:`:

```c
    case FT_DATETIMEMS: {
        if (plen < 8) return 0;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        uint32_t ms = ((uint32_t)p[4] << 24) | ((uint32_t)p[5] << 16) |
                      ((uint32_t)p[6] << 8)  |  (uint32_t)p[7];
        if (d == 0 && ms == 0) return 0;
        *out = (double)d * 100000000.0 + (double)ms;
        return 1;
    }
```

Confirm `plen`/`p`/`out` variable names against the function's actual signature and
the neighboring `FT_DATETIME` case; adjust names if they differ.

### 4k. `decode_idx_to_buf` — new case (cursor/range-scan key-to-string decode)

Anchor on the existing `FT_DATETIME` case:

```c
    case FT_DATETIME: {
```

(Full body: undoes the top-bit flip, decomposes `t` into `hh/mm/ss`, and returns
`snprintf(buf, bufsz, "%08d%02d%02d%02d", d, hh, mm, ss)`.) Insert immediately after its
closing `}`, before `case FT_TIME:`:

```c
    case FT_DATETIMEMS: {
        if (plen < 8) return 0;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        uint32_t ms = ((uint32_t)p[4] << 24) | ((uint32_t)p[5] << 16) |
                      ((uint32_t)p[6] << 8)  |  (uint32_t)p[7];
        if (skip_zero && d == 0 && ms == 0) return 0;
        int hh = ms / 3600000, mm = (ms % 3600000) / 60000,
            ss = (ms % 60000) / 1000, fff = ms % 1000;
        return snprintf(buf, bufsz, "%08d%02d%02d%02d%03d", d, hh, mm, ss, fff);
    }
```

**Confirmed**: `decode_idx_to_buf` takes a `skip_zero` parameter (its signature is
`decode_idx_to_buf(const TypedField *f, const uint8_t *p, size_t plen, char *buf,
size_t bufsz, int skip_zero)`), and the neighboring `FT_DATETIME` case gates its
empty-check on it: `if (skip_zero && d == 0 && t == 0) return 0;`. The new
`FT_DATETIMEMS` case above already uses the matching `skip_zero &&` guard — do not
drop it.

### 4l. `typed_field_to_double` — new case ("not summable" for UUID-like; here it IS summable)

Anchor on the existing `FT_DATETIME` case:

```c
    case FT_DATETIME: {
```

(Full body: `*out = (double)d * 1000000.0 + (double)t; return 1;`, reading raw stored
bytes via `ld_be_i32`/`ld_be_u16`.) Insert immediately after its closing `}`, before
`case FT_TIME:`:

```c
    case FT_DATETIMEMS: {
        int32_t d = ld_be_i32(p);
        uint32_t ms = ld_be_u32(p + 4);
        if (d == 0 && ms == 0) return 0;
        *out = (double)d * 100000000.0 + (double)ms;
        return 1;
    }
```

### 4m. `validate_field_type` — recognize `"datetimems"`

Anchor on the existing line for `"datetime"` (returns byte size 6):

```c
    if (strcmp(type, "datetime") == 0) return 6;
```

Insert immediately after:

```c
    if (strcmp(type, "datetimems") == 0) return 8;
```

### 4n. Error message — append `datetimems` to the valid-types list

Anchor (exact, in the invalid-field-type error string):

```
invalid field type: \"%s\" — valid types: varchar:N, int, long, short, double, float, bool, byte, date, datetime, time, timestamp, uuid, currency, numeric:P,S, enum(v1,v2,...)
```

Replace `date, datetime, time, timestamp, uuid,` with
`date, datetime, datetimems, time, timestamp, uuid,` inside that string (keep the rest
of the string byte-identical, just splice in `datetimems, ` after `datetime, `).

### 4o. Fix bulk-update `auto_update` stamping (3 sites) — bug fix + new type support

There are three near-identical blocks in query.c that stamp `DK_AUTO_UPDATE` fields
during bulk-update, in functions `v2_bulk_upd_value_compute`,
`v2_bulk_upd_delim_value_compute`, and `v2_bulk_upd_json_value_compute`. Each currently
looks like this (confirm the exact block in each function before editing — all three
should match this pattern verbatim modulo the outer loop variable names, which use
`w->ts->fields[fi]` consistently across all three per prior verification):

```c
            char tbuf[20];
            time_t now = time(NULL);
            struct tm tm; localtime_r(&now, &tm);
            if (w->ts->fields[fi].type == FT_DATE)
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                          tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
            else
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                          tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                          tm.tm_hour, tm.tm_min, tm.tm_sec);
            encode_field(&w->ts->fields[fi], tbuf,
                          new_buf + w->ts->fields[fi].offset);
```

In **all three** locations, replace with:

```c
            char tbuf[24];
            if (w->ts->fields[fi].type == FT_TIMESTAMP) {
                struct timespec tsn;
                clock_gettime(CLOCK_REALTIME, &tsn);
                long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
                snprintf(tbuf, sizeof(tbuf), "%lld", ms);
            } else if (w->ts->fields[fi].type == FT_DATETIMEMS) {
                struct timespec tsn;
                clock_gettime(CLOCK_REALTIME, &tsn);
                time_t nowsec = tsn.tv_sec;
                struct tm tm; localtime_r(&nowsec, &tm);
                int msec = (int)(tsn.tv_nsec / 1000000L);
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d%03d",
                          tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                          tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
            } else {
                time_t now = time(NULL);
                struct tm tm; localtime_r(&now, &tm);
                if (w->ts->fields[fi].type == FT_DATE)
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                              tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
                else
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                              tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                              tm.tm_hour, tm.tm_min, tm.tm_sec);
            }
            encode_field(&w->ts->fields[fi], tbuf,
                          new_buf + w->ts->fields[fi].offset);
```

**Important**: there are 3 separate call sites for this exact block (one per
`v2_bulk_upd_*_compute` function). Apply the same replacement at each of the 3
locations — do not use a global replace-all, since the surrounding function bodies
differ; verify each site individually by reading the enclosing function name before
editing.

**Why this is a bug fix, not scope creep**: `storage.c`'s `cmd_update_v2` (the
non-bulk single-record update path) already special-cases `FT_TIMESTAMP` correctly via
`clock_gettime`. Prior to this fix, a bulk-update on a `timestamp` field would stamp
second-precision epoch-seconds-as-a-decimal-string (wrong unit and wrong precision)
via the generic calendar-string branch, silently producing corrupt data for any
`auto_update timestamp` field updated through `bulk-update`. This plan closes that gap
consistently with the new `FT_DATETIMEMS` type's stamping logic.

Build check: `SKIP_TESTS=1 ./build.sh` should succeed with zero warnings introduced.

---

## Task 5 — `src/db/storage.c`: `cmd_update_v2`'s auto_update stamper

Anchor (exact):

```c
        char tbuf[24];
        if (ts->fields[i].type == FT_TIMESTAMP) {
            struct timespec tsn;
            clock_gettime(CLOCK_REALTIME, &tsn);
            long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
            snprintf(tbuf, sizeof(tbuf), "%lld", ms);
        } else {
            time_t now = time(NULL);
            struct tm tmv;
            localtime_r(&now, &tmv);
            if (ts->fields[i].type == FT_DATE)
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                         tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
            else
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                         tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                         tmv.tm_hour, tmv.tm_min, tmv.tm_sec);
        }
```

Replace with:

```c
        char tbuf[24];
        if (ts->fields[i].type == FT_TIMESTAMP) {
            struct timespec tsn;
            clock_gettime(CLOCK_REALTIME, &tsn);
            long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
            snprintf(tbuf, sizeof(tbuf), "%lld", ms);
        } else if (ts->fields[i].type == FT_DATETIMEMS) {
            struct timespec tsn;
            clock_gettime(CLOCK_REALTIME, &tsn);
            time_t nowsec = tsn.tv_sec;
            struct tm tmv;
            localtime_r(&nowsec, &tmv);
            int msec = (int)(tsn.tv_nsec / 1000000L);
            snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d%03d",
                     tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                     tmv.tm_hour, tmv.tm_min, tmv.tm_sec, msec);
        } else {
            time_t now = time(NULL);
            struct tm tmv;
            localtime_r(&now, &tmv);
            if (ts->fields[i].type == FT_DATE)
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                         tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
            else
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                         tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                         tmv.tm_hour, tmv.tm_min, tmv.tm_sec);
        }
```

Build check: `SKIP_TESTS=1 ./build.sh` should succeed.

---

## Task 6 — register the new test file in `build.sh`

Anchor (exact line in the manually-curated test source list):

```
    src/test/cases/test_timestamp.c \
```

Insert immediately after it:

```
    src/test/cases/test_datetimems.c \
```

(This list is order-insensitive for build correctness, but keep it adjacent to
`test_timestamp.c` for readability — both are single-scalar-type dedicated test
files.)

---

## Task 7 — new test file `src/test/cases/test_datetimems.c`

Create this file with the following content (mirrors `test_timestamp.c`'s structure —
same `TestEnv`/`TestClient` harness, same `TEST_REGISTER` pattern):

```c
/* test-datetimems — exercises the FT_DATETIMEMS field type. Storage is
 * 8 bytes: BE int32 yyyyMMdd date + BE uint32 ms-of-day (0..86399999).
 * Wire format is the 17-digit string "yyyyMMddHHmmssfff". Distinct from
 * FT_DATETIME (second precision, 6 bytes) and FT_TIMESTAMP (epoch ms,
 * no calendar semantics).
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

static void dtms_now_parts(struct tm *out_tm, int *out_msec) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    time_t now = ts.tv_sec;
    localtime_r(&now, out_tm);
    *out_msec = (int)(ts.tv_nsec / 1000000L);
}

static long long dtms_to_ordinal(int y, int mo, int d, int hh, int mm, int ss, int fff) {
    /* Simple monotonic-enough ordinal for range comparisons within a test run
       (not calendar-correct across month/year boundaries, which the test
       does not cross). */
    return ((long long)y * 10000LL + mo * 100LL + d) * 100000000LL
         + ((long long)hh * 3600 + mm * 60 + ss) * 1000LL + fff;
}

static int test_datetimems_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"dtms\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"created_at:datetimems:auto_create\","
                    "\"updated_at:datetimems:auto_update\","
                    "\"event_time:datetimems\"],"
        "\"indexes\":[\"event_time\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "create-object with datetimems fields succeeded");
    free(resp); resp = NULL;

    /* Insert with explicit event_time only; created_at + updated_at should
       be auto-populated to "now" at ms precision. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"key\":\"e1\",\"value\":{\"event_time\":\"20260703161530123\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "insert with auto_create datetimems");
    free(resp); resp = NULL;

    /* Read back — event_time should round-trip exactly as a 17-digit string;
       created_at + updated_at should be present. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"dtms\",\"object\":\"events\",\"key\":\"e1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"event_time\":\"20260703161530123\"", "event_time round-tripped");
    ASSERT_CONTAINS(resp, "\"created_at\":\"", "auto_create populated created_at");
    ASSERT_CONTAINS(resp, "\"updated_at\":\"", "auto_update populated updated_at");
    free(resp); resp = NULL;

    /* Update — updated_at should advance to a new value (best-effort ordinal
       check; a 5ms sleep guarantees strictly-greater timestamps). */
    {
        struct tm tm_before; int msec_before;
        dtms_now_parts(&tm_before, &msec_before);
        long long before = dtms_to_ordinal(tm_before.tm_year + 1900, tm_before.tm_mon + 1,
            tm_before.tm_mday, tm_before.tm_hour, tm_before.tm_min, tm_before.tm_sec, msec_before);

        struct timespec sl = { 0, 10 * 1000000L }; nanosleep(&sl, NULL);

        tc_request(tc,
            "{\"mode\":\"update\",\"dir\":\"dtms\",\"object\":\"events\","
            "\"key\":\"e1\",\"value\":{\"event_time\":\"20260703170000456\"}}", &resp);
        free(resp); resp = NULL;

        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"dtms\",\"object\":\"events\",\"key\":\"e1\"}", &resp);
        ASSERT_CONTAINS(resp, "\"event_time\":\"20260703170000456\"", "event_time updated");

        const char *uap = strstr(resp, "\"updated_at\":\"");
        ASSERT_NOT_NULL(uap, "updated_at present after update");
        if (uap) {
            const char *digits = uap + strlen("\"updated_at\":\"");
            int y, mo, d, hh, mm, ss, fff;
            sscanf(digits, "%4d%2d%2d%2d%2d%2d%3d", &y, &mo, &d, &hh, &mm, &ss, &fff);
            long long after = dtms_to_ordinal(y, mo, d, hh, mm, ss, fff);
            ASSERT_TRUE(after > before, "updated_at advanced past the pre-update ordinal");
        }
        free(resp); resp = NULL;
    }

    /* Indexed range query on event_time — should hit the btree path. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"event_time\",\"op\":\"gte\",\"value\":\"20260101000000000\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "indexed gte query returned 1");
    free(resp); resp = NULL;

    /* Range that excludes our record. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"event_time\",\"op\":\"lt\",\"value\":\"20200101000000000\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "0", "indexed lt query excludes out-of-range datetimems");
    free(resp); resp = NULL;

    /* Millisecond precision must be preserved through ordering: insert two
       records that differ only in their ms component and confirm gte/lt
       distinguish them correctly. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"key\":\"e2\",\"value\":{\"event_time\":\"20260703170000455\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"event_time\",\"op\":\"gt\",\"value\":\"20260703170000455\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "ms-precision gt distinguishes 456 from 455");
    free(resp); resp = NULL;

    /* describe-object should report the field type as "datetimems". */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"dtms\",\"object\":\"events\"}", &resp);
    ASSERT_CONTAINS(resp, "\"datetimems\"", "describe-object reports datetimems type");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-datetimems", test_datetimems_run)
```

---

## Task 8 — docs: `docs/concepts/typed-records.md`

Find the existing table row for `datetime` (format:
`| \`datetime\` | 6 bytes ... |`). Insert a new row immediately after it:

```
| `datetimems` | 8 bytes (BE int32 yyyyMMdd date + BE uint32 ms-of-day, 0..86399999) |
```

Match whatever column structure the existing table actually uses (the table has a
`Type` / `Encoding` column pair per the `CLAUDE.md` reference table — read the file
first to confirm exact column headers and formatting style before inserting, and match
the existing rows' phrasing convention, e.g. how `datetime`'s row is phrased).

Also update `CLAUDE.md`'s own "Typed binary record format" table (the row list
currently shows `date`, `datetime`, `time`, `timestamp`, `numeric:P,S`) — add a
`datetimems` row there too, following the same table format:

```
| `datetimems` | 8 bytes (BE int32 yyyyMMdd date + BE uint32 ms-of-day) |
```

---

## Final verification

1. `SKIP_TESTS=1 ./build.sh` — must complete with no errors, no new warnings.
2. `./build/bin/shard-db-test run test-datetimems` — must show all assertions passing.
3. `./build/bin/shard-db-test run-all` — must show `# total: N passed, 0 failed` (N =
   previous total + the new test's assertion count). Paste the real output.
4. If any step fails, fix the root cause — do not weaken assertions or skip the test.

Leave the branch **uncommitted** on completion; report back for review.
