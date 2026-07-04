# FT_IPV4 field type (closes GitHub issue #10)

## Execution rules (read before starting)

- Branch off `main`: `git checkout -b feat/ipv4-field-type`.
- Do the tasks in the order listed. Each task is independently buildable/testable.
- Build with `SKIP_TESTS=1 ./build.sh` after each task; run the full type's test only
  at the end with `./build/bin/shard-db-test run test-ipv4`, then the full suite with
  `./build/bin/shard-db-test run-all`.
- Never claim a step passed without pasting the real command output.
- Every insertion point below is located by **quoted anchor text**, not line numbers —
  line numbers drift because another branch may be landing changes concurrently (in
  particular, the datetimems field-type plan touches many of the same functions in
  query.c/config.c). If a quoted anchor is not found verbatim in the file, **stop** and
  write `PLAN_NOTES.md` at the repo root describing exactly what you searched for and
  what the surrounding code looks like instead. Do not guess or improvise a fix.
- Leave all work **uncommitted** when done. Do not `git commit`, `git push`, or open a
  PR — that happens outside this session.
- **No new `#include` directives are needed anywhere.** `<netinet/in.h>` and
  `<arpa/inet.h>` are already included in `src/db/types.h` (near the top of the file),
  which both `config.c` and `query.c` include first — `inet_pton`/`inet_ntop` are
  already transitively available.
- **No `src/cli/` changes are required.** shard-cli only round-trips typed fields as
  opaque JSON strings; it does not special-case field types.

## Design summary

New field type `ipv4`: an IPv4 address stored and indexed as raw network-byte-order
bytes, mirroring `FT_UUID`'s treatment almost exactly (fixed-width binary, no sign
flip needed since the bytes are already in the correct memcmp order, no
auto_create/auto_update semantics).

- **Wire format** (JSON in/out): dotted-quad string, e.g. `"192.168.1.1"`.
- **On-disk storage**: 4 raw bytes, network byte order (i.e. exactly what
  `inet_pton(AF_INET, ...)` produces — already the correct big-endian representation
  for byte-lexicographic sort order matching numeric IP order).
- **Index-key ordering**: **no bit flip** — network-byte-order IPv4 bytes are already
  unsigned and already memcmp-sortable in the correct numeric order. This exactly
  mirrors `FT_UUID`, which also does not flip.
- **Zero/unset convention**: all 4 bytes zero (`0.0.0.0`) is treated as "empty" —
  mirrors `FT_UUID`'s `uuid_is_zero` convention. Note `0.0.0.0` is a valid (if unusual)
  IPv4 address that will therefore be indistinguishable from "unset"; this is an
  accepted limitation matching the analogous all-zero-UUID convention already in the
  codebase.
- **Comparisons**: direct `memcmp(a, b, 4)` — no numeric composition needed (unlike the
  calendar types), same as `FT_UUID`'s `memcmp(a, b, 16)`.
- **IN-list support**: yes — `cc->in_ipv4` is added to `CompiledCriterion`, mirroring
  `cc->in_uuid` exactly (4-byte elements instead of 16-byte).
- **No `auto_create`/`auto_update` generator**: IPv4 addresses have no "current value"
  concept, so `generate_default`/`config.c`'s default-generation dispatch is untouched.

---

## Task 1 — `src/db/types.h`: enum + struct

### 1a. Append `FT_IPV4` to `enum FieldType`

Anchor (exact — the last two entries of the enum):

```c
    FT_UUID,        /* uuid — 16 bytes binary */
    FT_ENUM         /* enum(v1,v2,...) — declared value list, encoded as
```

The `FT_ENUM` entry is the final enum member (its line ends the enum body with no
trailing comma, followed by a multi-line comment and `};`). Locate the `FT_ENUM` line
and change it to add a trailing comma, then insert `FT_IPV4` as the new final member.
Concretely, find:

```c
    FT_UUID,        /* uuid — 16 bytes binary */
    FT_ENUM         /* enum(v1,v2,...) — declared value list, encoded as
                       0-based index into the value list (uint8_t if
                       ≤256 values, else uint16_t). Ordering follows
                       declaration order, not lexical order. Maps directly
                       to a bitmap index (like FT_BOOL). */
```

Replace with:

```c
    FT_UUID,        /* uuid — 16 bytes binary */
    FT_ENUM,        /* enum(v1,v2,...) — declared value list, encoded as
                       0-based index into the value list (uint8_t if
                       ≤256 values, else uint16_t). Ordering follows
                       declaration order, not lexical order. Maps directly
                       to a bitmap index (like FT_BOOL). */
    FT_IPV4         /* ipv4 — 4 bytes binary, network byte order (same
                       ordering as inet_pton output). No sign-bit flip
                       needed for index-key ordering — mirrors FT_UUID. */
```

**Invariant**: enum ordinal values are never disk-persisted (schemas are re-derived
from `fields.conf` text on every load), so appending here is safe.

**Note for the ipv6 plan (informational only, no action needed here)**: a future
`FT_IPV6` type is planned to be appended immediately after `FT_IPV4` once this plan has
merged to `main`, to avoid a merge conflict on this same anchor. Do not add `FT_IPV6`
as part of this plan.

### 1b. Add ipv4 fields to `CompiledCriterion`

Anchor (exact):

```c
    uint8_t  uuid_bytes[16];
    uint8_t  uuid_bytes2[16];
    uint8_t  time_val[3];
    uint8_t  time_val2[3];
```

Replace with:

```c
    uint8_t  uuid_bytes[16];
    uint8_t  uuid_bytes2[16];
    uint8_t  ipv4_val[4];
    uint8_t  ipv4_val2[4];
    uint8_t  time_val[3];
    uint8_t  time_val2[3];
```

Anchor for the IN-list arrays (exact):

```c
    int64_t  *in_i64;
    double   *in_f64;
    uint8_t (*in_uuid)[16];
    uint8_t (*in_time)[3];
    int       in_count;
```

Replace with:

```c
    int64_t  *in_i64;
    double   *in_f64;
    uint8_t (*in_uuid)[16];
    uint8_t (*in_time)[3];
    uint8_t (*in_ipv4)[4];
    int       in_count;
```

Build check: `SKIP_TESTS=1 ./build.sh` should still succeed (no functional change yet).

---

## Task 2 — `src/db/config.c`: parse, encode, decode

### 2a. `parse_field_type` — recognize `"ipv4"`

Anchor (exact):

```c
    } else if (strcmp(spec, "uuid") == 0) {
        f->type = FT_UUID;
        f->size = 16;
    }
```

Replace with:

```c
    } else if (strcmp(spec, "uuid") == 0) {
        f->type = FT_UUID;
        f->size = 16;
    } else if (strcmp(spec, "ipv4") == 0) {
        f->type = FT_IPV4;
        f->size = 4;
    }
```

(If this `else if` chain continues further after the `uuid` branch before its final
closing — e.g. a `currency` or `numeric` branch follows — insert the new `ipv4` branch
immediately after the `uuid` branch specifically, preserving whatever comes after it
unchanged.)

### 2b. `encode_field_len` — new `FT_IPV4` case

Anchor on the existing `FT_UUID` case in this function's switch (locate its full body —
it parses a canonical UUID string into 16 bytes) and insert immediately after its
closing `}`:

```c
    case FT_IPV4: {
        char ipbuf[16]; /* max "255.255.255.255" + NUL = 16 */
        size_t n = vlen < sizeof(ipbuf) - 1 ? vlen : sizeof(ipbuf) - 1;
        memcpy(ipbuf, val, n);
        ipbuf[n] = '\0';
        if (n == 0 || inet_pton(AF_INET, ipbuf, out) != 1)
            memset(out, 0, 4);
        break;
    }
```

**Edge case**: empty value or malformed dotted-quad → all-zero bytes (the "unset"
sentinel), matching `FT_UUID`'s fallback-to-zero convention on parse failure.

### 2c. `encode_field_for_index` — new `FT_IPV4` case

Anchor on the existing `FT_UUID` case in this function and insert immediately after its
closing `}`:

```c
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
```

Note: **no top-bit flip** — this mirrors the `FT_UUID` case in this same function,
which also does not flip.

### 2d. `typed_field_to_index_key` — new `FT_IPV4` case

This function converts an already-encoded on-disk field (`src`) into an index key.
Anchor on the existing `FT_UUID` case:

```c
    case FT_UUID: {
        memcpy(out, src, 16);
        *out_len = 16;
        break;
    }
```

Insert immediately after it (before whatever case follows — `FT_DOUBLE` per prior
verification):

```c
    case FT_IPV4: {
        memcpy(out, src, 4);
        *out_len = 4;
        break;
    }
```

### 2e. `decode_field_to_buf` — new `FT_IPV4` case (JSON emission, quoted)

Anchor on the existing `FT_UUID` case:

```c
    case FT_UUID: {
```

(Full body checks `buflen < 39`, calls `uuid_is_zero`, formats via
`uuid_format_canonical` wrapped in quotes.) Insert immediately after its closing `}`:

```c
    case FT_IPV4: {
        if (data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 0)
            return 0;
        char ipstr[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, data, ipstr, sizeof(ipstr)))
            return 0;
        return snprintf(buf, buflen, "\"%s\"", ipstr);
    }
```

Confirm the raw-bytes pointer name used by the surrounding `FT_UUID` case in this
function (quoted above as `data`, per `decode_field_to_buf`'s established convention
for `FT_DATE`/`FT_DATETIME`/`FT_UUID`) and match it exactly; adjust if the actual
parameter name differs.

### 2f. `typed_get_field_str` — new `FT_IPV4` case (unquoted string)

Anchor on the existing `FT_UUID` case:

```c
    case FT_UUID: {
```

(Full body mallocs 37 bytes and calls `uuid_format_canonical`, unquoted.) Insert
immediately after its closing `}`:

```c
    case FT_IPV4: {
        const uint8_t *ip = src + f->offset;
        if (ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0)
            return NULL;
        char *out = malloc(INET_ADDRSTRLEN);
        if (!out) return NULL;
        if (!inet_ntop(AF_INET, ip, out, INET_ADDRSTRLEN)) {
            free(out);
            return NULL;
        }
        return out;
    }
```

Confirm `src`/`f`-parameter names against the function's actual signature and the
neighboring `FT_UUID` case; adjust if they differ.

**No `generate_default`/`gen_*_now` changes are needed** — `ipv4` has no
`auto_create`/`auto_update` semantics (there is no meaningful "current IP address" to
generate).

Build check: `SKIP_TESTS=1 ./build.sh` should succeed.

---

## Task 3 — `src/db/query.c`: helper, compile, compare, match, cleanup

### 3a. New `parse_ipv4` helper

Anchor — insert immediately after the existing `parse_uuid` function's closing brace
(locate `static void parse_uuid(...)` or equivalent signature and its full body,
insert directly after):

```c
/* Parse a dotted-quad IPv4 string into 4 raw network-byte-order bytes.
   Malformed/empty input zero-fills (mirrors parse_uuid's fallback). */
static void parse_ipv4(const char *s, uint8_t out[4]) {
    if (!s || !s[0] || inet_pton(AF_INET, s, out) != 1)
        memset(out, 0, 4);
}
```

### 3b. `compile_one` — new scalar case

Anchor on the existing `FT_UUID` scalar case:

```c
    case FT_UUID: {
        if (c->value[0]) { parse_uuid(c->value, cc->uuid_bytes); }
        if (c->value2[0]) { parse_uuid(c->value2, cc->uuid_bytes2); }
        break;
    }
```

Insert immediately after (before whatever case follows):

```c
    case FT_IPV4: {
        if (c->value[0]) { parse_ipv4(c->value, cc->ipv4_val); }
        if (c->value2[0]) { parse_ipv4(c->value2, cc->ipv4_val2); }
        break;
    }
```

### 3c. `compile_one` — new IN-list case

Anchor on the existing `FT_UUID` IN-list case:

```c
    case FT_UUID:
        cc->in_uuid = malloc(sizeof(uint8_t[16]) * c->in_count);
        for (int i = 0; i < c->in_count; i++)
            parse_uuid(c->in_values[i], cc->in_uuid[i]);
        break;
```

Insert immediately after:

```c
    case FT_IPV4:
        cc->in_ipv4 = malloc(sizeof(uint8_t[4]) * c->in_count);
        for (int i = 0; i < c->in_count; i++)
            parse_ipv4(c->in_values[i], cc->in_ipv4[i]);
        break;
```

**Invariant**: `malloc` here is not NULL-checked, matching the existing `in_uuid`/
`in_i64`/`in_f64` cases in this same switch (pre-existing convention, not introduced by
this plan — `MAX_CRITERIA_DEPTH`/query size limits bound `c->in_count` in practice).

### 3d. `free_compiled_criteria` — free the new `in_ipv4` array

Anchor (exact):

```c
        free(arr[i].in_uuid);
        free(arr[i].in_time);
```

Replace with:

```c
        free(arr[i].in_uuid);
        free(arr[i].in_time);
        free(arr[i].in_ipv4);
```

`free(NULL)` is a no-op, so this is safe to add unconditionally even when `in_ipv4` was
never allocated for a given criterion (the default-zeroed `CompiledCriterion` struct
leaves unused pointers at `NULL`).

### 3e. `cmp_typed_field_pair` — new case

Anchor on the existing `FT_UUID` case:

```c
    case FT_UUID:
        return memcmp(a, b, 16);
```

Insert immediately after:

```c
    case FT_IPV4:
        return memcmp(a, b, 4);
```

(Match the exact existing style — if `FT_UUID`'s case is a one-line `case: return`
without braces as shown above, mirror the same brace-less style for `FT_IPV4`.)

### 3f. `match_typed` — new case (mechanical copy of the `FT_UUID` block, 4 bytes)

Anchor on the full existing `FT_UUID` case body:

```c
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
            int lo = cc->i1;
            int hi = cc->i2;
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
```

Insert immediately after this case's closing `}` (before whatever case follows —
`FT_ENUM` per prior verification):

```c
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
```

**Note**: the `OP_BETWEEN` branch's `lo`/`hi` gating via `cc->i1`/`cc->i2` is copied
verbatim from the `FT_UUID` case — those two ints are reused as generic "bound present"
flags across multiple field types in this switch (not ipv4/uuid-specific fields), so
this is intentional and correct, not a copy-paste bug.

### 3g. `buf_field_value` — new textual quoted case (JOIN row emission)

Anchor (exact, after the datetimems plan's edit is applied — if this plan is executed
independently and the `FT_DATETIMEMS` line from the datetimems plan is not present yet,
anchor on the `FT_ENUM` line as shown in the second alternative below):

Primary anchor (if datetimems plan already merged):

```c
        case FT_DATE:
        case FT_DATETIME:
        case FT_DATETIMEMS:
        case FT_ENUM:
```

Fallback anchor (if datetimems plan not yet merged):

```c
        case FT_DATE:
        case FT_DATETIME:
        case FT_ENUM:
```

Whichever form is found, add `case FT_IPV4:` as an additional case label in the same
group (so the block becomes one of):

```c
        case FT_DATE:
        case FT_DATETIME:
        case FT_DATETIMEMS:
        case FT_IPV4:
        case FT_ENUM:
            n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
            if (n <= 0) return snprintf(buf, bufsz, "null");
            return snprintf(buf, bufsz, "\"%s\"", tmp);
```

or, if the datetimems case label is not yet present:

```c
        case FT_DATE:
        case FT_DATETIME:
        case FT_IPV4:
        case FT_ENUM:
            n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
            if (n <= 0) return snprintf(buf, bufsz, "null");
            return snprintf(buf, bufsz, "\"%s\"", tmp);
```

*Aside (out of scope, not to be fixed here)*: this same function's `default:` branch
currently emits `FT_UUID` **unquoted**, which is invalid JSON for UUID fields projected
through a JOIN (the primary find/get path via `decode_field_to_buf` in config.c quotes
UUID correctly — the bug is specific to this JOIN row-emission path only). This is a
pre-existing, unrelated bug; called out here for visibility but **not** part of this
plan's scope — do not fix it as part of this task.

### 3h. `field_type_str` — new case

Anchor on the existing `FT_ENUM` or `FT_UUID` case (use whichever is the last case
before this function's `default:`/closing brace — per prior verification, `FT_UUID`'s
case exists as `case FT_UUID: return "uuid";`):

```c
    case FT_UUID: return "uuid";
```

Insert immediately after:

```c
    case FT_IPV4: return "ipv4";
```

### 3i. `typed_field_to_buf_raw` — new case (unquoted raw string)

Anchor on the existing `FT_UUID` case:

```c
    case FT_UUID: {
```

(Full body: `if (uuid_is_zero(b)) return 0; return uuid_format_canonical(buf, bufsz, b);`)
Insert immediately after its closing `}`:

```c
    case FT_IPV4: {
        if (p[0] == 0 && p[1] == 0 && p[2] == 0 && p[3] == 0) return 0;
        char ipstr[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, p, ipstr, sizeof(ipstr))) return 0;
        return snprintf(buf, bufsz, "%s", ipstr);
    }
```

Confirm the pointer variable name (`p` vs. `b`, matching this function's own
convention rather than `decode_field_to_buf`'s) against the neighboring `FT_UUID` case
in this specific function; adjust if different.

### 3j. `decode_index_key_to_double` — new "not summable" case

Anchor on the existing `FT_UUID` case:

```c
    case FT_UUID:
```

(Body returns `0` — "UUIDs aren't summable".) Insert immediately after:

```c
    case FT_IPV4:
        /* IPv4 addresses aren't summable. */
        return 0;
```

### 3k. `decode_idx_to_buf` — new case (cursor/range-scan key-to-string decode)

Anchor on the existing `FT_UUID` case:

```c
    case FT_UUID: {
```

(Full body: `if (uuid_is_zero(b)) return 0; return uuid_format_canonical(buf, bufsz, b);`)
Insert immediately after its closing `}`:

```c
    case FT_IPV4: {
        if (p[0] == 0 && p[1] == 0 && p[2] == 0 && p[3] == 0) return 0;
        char ipstr[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, p, ipstr, sizeof(ipstr))) return 0;
        return snprintf(buf, bufsz, "%s", ipstr);
    }
```

Confirm the pointer variable name against this function's actual `FT_UUID` case
convention (may be `p` or a differently-named parameter) and match it.

### 3l. `typed_field_to_double` — new "not summable" case

Anchor on the existing `FT_UUID` case:

```c
    case FT_UUID:
```

(Body returns `0` — "not summable".) Insert immediately after:

```c
    case FT_IPV4:
        /* IPv4 addresses aren't summable. */
        return 0;
```

### 3m. `validate_field_type` — recognize `"ipv4"`

Anchor on the existing line for `"uuid"`:

```c
    if (strcmp(type, "uuid") == 0) return 16;
```

Insert immediately after:

```c
    if (strcmp(type, "ipv4") == 0) return 4;
```

### 3n. Error message — append `ipv4` to the valid-types list

Anchor (exact — apply against whatever the string currently looks like; if the
datetimems plan's task 4n has already merged, `datetimems` will already be present
in this string, in which case anchor on that variant instead):

```
invalid field type: \"%s\" — valid types: varchar:N, int, long, short, double, float, bool, byte, date, datetime, time, timestamp, uuid, currency, numeric:P,S, enum(v1,v2,...)
```

Splice in `ipv4, ` immediately after `uuid, ` so the result reads
`..., timestamp, uuid, ipv4, currency, numeric:P,S, ...` (keep the rest of the string
byte-identical). If the datetimems row already landed, the pre-edit string will instead
read `..., datetimems, time, timestamp, uuid, currency, ...` — in that case apply the
same `uuid, ` → `uuid, ipv4, ` splice against that variant.

Build check: `SKIP_TESTS=1 ./build.sh` should succeed with zero warnings introduced.

---

## Task 4 — register the new test file in `build.sh`

Anchor (exact line in the manually-curated test source list):

```
    src/test/cases/test_enum.c \
```

Insert immediately after it:

```
    src/test/cases/test_ipv4.c \
```

(If the datetimems plan's `test_datetimems.c` line has already been added between
`test_timestamp.c` and this point, that is fine — just ensure `test_ipv4.c` is added
somewhere in the list; exact ordering doesn't affect build correctness.)

---

## Task 5 — new test file `src/test/cases/test_ipv4.c`

Create this file with the following content (mirrors `test_timestamp.c`'s harness
structure, adapted for a non-generated, indexable, comparable, IN-list-capable type):

```c
/* test-ipv4 — exercises the FT_IPV4 field type. Storage is 4 raw bytes,
 * network byte order (inet_pton output), no sign-bit flip for index-key
 * ordering (mirrors FT_UUID). Wire format is a dotted-quad string.
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

static int test_ipv4_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"ip4\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"ip4\",\"object\":\"hosts\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"addr:ipv4\",\"label:varchar:32\"],"
        "\"indexes\":[\"addr\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "create-object with ipv4 field succeeded");
    free(resp); resp = NULL;

    /* Insert several hosts with distinct addresses. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip4\",\"object\":\"hosts\","
        "\"key\":\"h1\",\"value\":{\"addr\":\"10.0.0.1\",\"label\":\"one\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "insert h1");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip4\",\"object\":\"hosts\","
        "\"key\":\"h2\",\"value\":{\"addr\":\"10.0.0.2\",\"label\":\"two\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip4\",\"object\":\"hosts\","
        "\"key\":\"h3\",\"value\":{\"addr\":\"192.168.1.1\",\"label\":\"three\"}}", &resp);
    free(resp); resp = NULL;

    /* Round-trip: get should return the dotted-quad string unchanged. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ip4\",\"object\":\"hosts\",\"key\":\"h1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"addr\":\"10.0.0.1\"", "addr round-tripped");
    free(resp); resp = NULL;

    /* eq lookup via index. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip4\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"eq\",\"value\":\"10.0.0.2\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "indexed eq query returned 1");
    free(resp); resp = NULL;

    /* Numeric-order range: 10.0.0.1 and 10.0.0.2 should both be < 192.168.1.1
       under byte-lexicographic (== numeric IPv4) ordering. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip4\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"lt\",\"value\":\"11.0.0.0\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "range query respects numeric ipv4 ordering");
    free(resp); resp = NULL;

    /* IN-list. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip4\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"in\",\"value\":[\"10.0.0.1\",\"192.168.1.1\"]}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "IN-list query matches 2 of 3 hosts");
    free(resp); resp = NULL;

    /* NOT_IN. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip4\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"not_in\",\"value\":[\"10.0.0.1\",\"192.168.1.1\"]}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "NOT_IN query matches the remaining host");
    free(resp); resp = NULL;

    /* between. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip4\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"between\","
        "\"value\":\"10.0.0.0\",\"value2\":\"10.255.255.255\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "between query matches the 10.x hosts");
    free(resp); resp = NULL;

    /* describe-object should report the field type as "ipv4". */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"ip4\",\"object\":\"hosts\"}", &resp);
    ASSERT_CONTAINS(resp, "\"ipv4\"", "describe-object reports ipv4 type");
    free(resp); resp = NULL;

    /* Malformed address should not crash; should encode to the zero/unset
       sentinel and round-trip as absent (mirrors FT_UUID's parse-failure
       fallback convention). */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip4\",\"object\":\"hosts\","
        "\"key\":\"h4\",\"value\":{\"addr\":\"not-an-ip\",\"label\":\"bad\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "insert with malformed addr does not error");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ip4\",\"object\":\"hosts\",\"key\":\"h4\"}", &resp);
    ASSERT_TRUE(strstr(resp, "\"addr\":\"") == NULL,
        "malformed addr encodes to the unset sentinel (field omitted or null)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-ipv4", test_ipv4_run)
```

**Note on the last assertion**: confirm against `decode_field_to_buf`'s actual
zero-field convention (established by `FT_UUID`/`FT_DATE`/etc. in this codebase) —
fields that decode to "empty" are omitted entirely from the JSON object (the `return 0`
path signals "skip this field" to the caller), not emitted as `null`. If investigation
during execution shows the actual behavior differs (e.g. the field IS present as an
empty string), adjust this assertion to match reality and note the discrepancy in
`PLAN_NOTES.md`.

---

## Task 6 — docs: `docs/concepts/typed-records.md`

Find the existing table row for `uuid`. Insert a new row immediately after it:

```
| `ipv4` | 4 bytes, network byte order (no sign-bit flip; byte-lexicographic order matches numeric IPv4 order) |
```

Match whatever column structure the existing table actually uses — read the file first
to confirm exact column headers and formatting style before inserting, and match the
existing rows' phrasing convention (e.g. how the `uuid` row is phrased).

Also update `CLAUDE.md`'s own "Typed binary record format" table (the row list
currently shows `date`, `datetime`, `time`, `timestamp`, `numeric:P,S` — and, if the
datetimems plan has already merged, `datetimems` too) — add an `ipv4` row there too,
following the same table format:

```
| `ipv4` | 4 bytes, network byte order |
```

---

## Final verification

1. `SKIP_TESTS=1 ./build.sh` — must complete with no errors, no new warnings.
2. `./build/bin/shard-db-test run test-ipv4` — must show all assertions passing.
3. `./build/bin/shard-db-test run-all` — must show `# total: N passed, 0 failed` (N =
   previous total + the new test's assertion count). Paste the real output.
4. If any step fails, fix the root cause — do not weaken assertions or skip the test.

Leave the branch **uncommitted** on completion; report back for review.
