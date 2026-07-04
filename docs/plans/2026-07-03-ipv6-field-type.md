# FT_IPV6 field type (re-closes GitHub issue #11)

## IMPORTANT — sequencing vs. the ipv4 plan

Issue #11 was previously closed **without any actual implementation** — no
`FT_IPV6`/`inet6` code exists anywhere in the codebase or git history. It has been
reopened and is being re-planned here.

**Execute this plan only after `docs/plans/2026-07-03-ipv4-field-type.md` has been
executed and merged to `main`.** This plan's `types.h` anchor (Task 1a) specifically
targets the state of the enum *after* `FT_IPV4` has been appended, to avoid a merge
conflict between the two branches on the same enum-tail region. If `FT_IPV4` is not
yet present in `enum FieldType` when you start this task, **stop** and write
`PLAN_NOTES.md` noting that the ipv4 plan must land first; do not improvise an
alternate insertion point.

Everything else in this plan (config.c/query.c/index.c touch points) is structurally
independent of the ipv4 changes — it's a parallel, mechanical 16-byte analogue of the
same pattern, keyed off `FT_UUID`'s (16-byte) code rather than `FT_IPV4`'s (4-byte)
code as the closest existing precedent (both are fixed 16-byte binary types with no
sign flip). The *only* hard sequencing dependency is the shared enum-tail anchor.

## Execution rules (read before starting)

- Branch off `main` **after** the ipv4 branch has merged: `git checkout -b
  feat/ipv6-field-type`.
- Do the tasks in the order listed. Each task is independently buildable/testable.
- Build with `SKIP_TESTS=1 ./build.sh` after each task; run the full type's test only
  at the end with `./build/bin/shard-db-test run test-ipv6`, then the full suite with
  `./build/bin/shard-db-test run-all`.
- Never claim a step passed without pasting the real command output.
- Every insertion point below is located by **quoted anchor text**, not line numbers.
  If a quoted anchor is not found verbatim in the file, **stop** and write
  `PLAN_NOTES.md` at the repo root describing exactly what you searched for and what
  the surrounding code looks like instead. Do not guess or improvise a fix.
- Leave all work **uncommitted** when done. Do not `git commit`, `git push`, or open a
  PR — that happens outside this session.
- **No new `#include` directives are needed anywhere.** `<netinet/in.h>` and
  `<arpa/inet.h>` are already included in `src/db/types.h`, which both `config.c` and
  `query.c` include first — `inet_pton`/`inet_ntop` with `AF_INET6` are already
  transitively available.
- **No `src/cli/` changes are required.**

## Design summary

New field type `ipv6`: a 16-byte IPv6 address, structurally a close analogue of
`FT_IPV4` (this plan's sibling) and of `FT_UUID` (same 16-byte fixed-width, no sign
flip, memcmp-ordered, IN-list-capable, no auto_create/auto_update semantics).

- **Wire format** (JSON in/out): canonical IPv6 string via `inet_ntop(AF_INET6, ...)`,
  e.g. `"2001:db8::1"`.
- **On-disk storage**: 16 raw bytes, network byte order (`inet_pton(AF_INET6, ...)`
  output — already correct big-endian representation for byte-lexicographic sort order
  matching numeric IPv6 order).
- **Index-key ordering**: **no bit flip** — mirrors `FT_UUID`/`FT_IPV4` exactly.
- **Zero/unset convention**: all 16 bytes zero (`::`) treated as "empty" — mirrors
  `FT_UUID`'s `uuid_is_zero`/`FT_IPV4`'s all-zero convention. `::` (the unspecified
  address) is therefore indistinguishable from "unset" — same accepted limitation as
  `0.0.0.0` for ipv4 and the all-zero UUID.
- **Comparisons**: direct `memcmp(a, b, 16)` — identical in shape to `FT_UUID`.
- **IN-list support**: yes — `cc->in_ipv6` mirrors `cc->in_uuid` exactly (16-byte
  elements).
- **No `auto_create`/`auto_update` generator** — same rationale as ipv4.
- **No leak-fix bundling needed here** — the `free_compiled_criteria` leak fix (for
  `in_uuid`/`in_time`) was already bundled into the ipv4 plan; this plan just adds one
  more `free(arr[i].in_ipv6);` line alongside it.

---

## Task 1 — `src/db/types.h`: enum + struct

### 1a. Append `FT_IPV6` after `FT_IPV4`

Anchor (exact — **only valid after the ipv4 plan has merged**; per that plan's Task 1a,
`FT_IPV4` became the new final enum member; note `FT_ENUM`'s comment already reads
"1-byte index (≤256 values) or 2-byte BE index (257-65535 values)..." in the merged
code, not the older "0-based index... uint8_t if ≤256 values, else uint16_t" text):

```c
    FT_UUID,        /* uuid — 16 bytes binary */
    FT_ENUM,        /* enum(v1,v2,...) — declared value list, encoded as
                       1-byte index (≤256 values) or 2-byte BE index
                       (257-65535 values). The byte width is fixed at
                       declaration time; auto-widens 1→2 via edit-field
                       when an append pushes count past 256. Auto-defaults
                       to a bitmap index (like FT_BOOL). */
    FT_IPV4         /* ipv4 — 4 bytes binary, network byte order (same
                       ordering as inet_pton output). No sign-bit flip
                       needed for index-key ordering — mirrors FT_UUID. */
};
```

Replace with:

```c
    FT_UUID,        /* uuid — 16 bytes binary */
    FT_ENUM,        /* enum(v1,v2,...) — declared value list, encoded as
                       1-byte index (≤256 values) or 2-byte BE index
                       (257-65535 values). The byte width is fixed at
                       declaration time; auto-widens 1→2 via edit-field
                       when an append pushes count past 256. Auto-defaults
                       to a bitmap index (like FT_BOOL). */
    FT_IPV4,        /* ipv4 — 4 bytes binary, network byte order (same
                       ordering as inet_pton output). No sign-bit flip
                       needed for index-key ordering — mirrors FT_UUID. */
    FT_IPV6         /* ipv6 — 16 bytes binary, network byte order (same
                       ordering as inet_pton(AF_INET6, ...) output). No
                       sign-bit flip needed — mirrors FT_UUID/FT_IPV4. */
};
```

**Invariant**: enum ordinal values are never disk-persisted, so appending here is
safe.

### 1b. Add ipv6 fields to `CompiledCriterion`

Anchor (exact — assumes the ipv4 plan's Task 1b has already landed `ipv4_val`/
`ipv4_val2`; if not present, stop and write `PLAN_NOTES.md`):

```c
    uint8_t  uuid_bytes[16];
    uint8_t  uuid_bytes2[16];
    uint8_t  ipv4_val[4];
    uint8_t  ipv4_val2[4];
    uint8_t  time_val[3];
    uint8_t  time_val2[3];
```

Replace with:

```c
    uint8_t  uuid_bytes[16];
    uint8_t  uuid_bytes2[16];
    uint8_t  ipv4_val[4];
    uint8_t  ipv4_val2[4];
    uint8_t  ipv6_val[16];
    uint8_t  ipv6_val2[16];
    uint8_t  time_val[3];
    uint8_t  time_val2[3];
```

Anchor for the IN-list arrays (exact — assumes ipv4's `in_ipv4` has already landed;
note `in_lens` sits between `in_ipv4` and `in_count` in the merged code):

```c
    int64_t  *in_i64;
    double   *in_f64;
    uint8_t (*in_uuid)[16];
    uint8_t (*in_time)[3];
    uint8_t (*in_ipv4)[4];
    size_t   *in_lens;   /* varchar only: strlen(in_values[i]), precomputed once
                             at compile time instead of per-record in the match
                             loop (in_values[] themselves stay raw strings) */
    int       in_count;
```

Replace with:

```c
    int64_t  *in_i64;
    double   *in_f64;
    uint8_t (*in_uuid)[16];
    uint8_t (*in_time)[3];
    uint8_t (*in_ipv4)[4];
    uint8_t (*in_ipv6)[16];
    size_t   *in_lens;   /* varchar only: strlen(in_values[i]), precomputed once
                             at compile time instead of per-record in the match
                             loop (in_values[] themselves stay raw strings) */
    int       in_count;
```

Build check: `SKIP_TESTS=1 ./build.sh` should still succeed.

---

## Task 2 — `src/db/config.c`: parse, encode, decode

### 2a. `parse_field_type` — recognize `"ipv6"`

Anchor (exact — assumes ipv4's branch from its own Task 2a has landed; if not found,
stop and write `PLAN_NOTES.md`):

```c
    } else if (strcmp(spec, "ipv4") == 0) {
        f->type = FT_IPV4;
        f->size = 4;
    }
```

Replace with:

```c
    } else if (strcmp(spec, "ipv4") == 0) {
        f->type = FT_IPV4;
        f->size = 4;
    } else if (strcmp(spec, "ipv6") == 0) {
        f->type = FT_IPV6;
        f->size = 16;
    }
```

### 2b. `encode_field_len` — new `FT_IPV6` case

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 2b of that plan):

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

Insert immediately after its closing `}`:

```c
    case FT_IPV6: {
        char ipbuf[46]; /* INET6_ADDRSTRLEN */
        size_t n = vlen < sizeof(ipbuf) - 1 ? vlen : sizeof(ipbuf) - 1;
        memcpy(ipbuf, val, n);
        ipbuf[n] = '\0';
        if (n == 0 || inet_pton(AF_INET6, ipbuf, out) != 1)
            memset(out, 0, 16);
        break;
    }
```

### 2c. `encode_field_for_index` — new `FT_IPV6` case

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 2c of that plan):

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

Insert immediately after its closing `}`:

```c
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
```

No top-bit flip — mirrors `FT_IPV4`/`FT_UUID`.

### 2d. `typed_field_to_index_key` — new `FT_IPV6` case

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 2d):

```c
    case FT_IPV4: {
        memcpy(out, src, 4);
        *out_len = 4;
        break;
    }
```

Insert immediately after:

```c
    case FT_IPV6: {
        memcpy(out, src, 16);
        *out_len = 16;
        break;
    }
```

### 2e. `decode_field_to_buf` — new `FT_IPV6` case (JSON emission, quoted)

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 2e):

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

Insert immediately after:

```c
    case FT_IPV6: {
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (data[bi] != 0) { allzero = 0; break; }
        if (allzero) return 0;
        char ipstr[INET6_ADDRSTRLEN];
        if (!inet_ntop(AF_INET6, data, ipstr, sizeof(ipstr)))
            return 0;
        return snprintf(buf, buflen, "\"%s\"", ipstr);
    }
```

Confirm the raw-bytes pointer name (`data`) against the function's actual convention,
matching whatever name the neighboring `FT_IPV4`/`FT_UUID` cases use.

### 2f. `typed_get_field_str` — new `FT_IPV6` case (unquoted string)

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 2f):

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

Insert immediately after:

```c
    case FT_IPV6: {
        const uint8_t *ip = src + f->offset;
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (ip[bi] != 0) { allzero = 0; break; }
        if (allzero) return NULL;
        char *out = malloc(INET6_ADDRSTRLEN);
        if (!out) return NULL;
        if (!inet_ntop(AF_INET6, ip, out, INET6_ADDRSTRLEN)) {
            free(out);
            return NULL;
        }
        return out;
    }
```

**No `generate_default`/`gen_*_now` changes needed** — same rationale as ipv4.

Build check: `SKIP_TESTS=1 ./build.sh` should succeed.

---

## Task 3 — `src/db/query.c`: helper, compile, compare, match, cleanup

### 3a. New `parse_ipv6` helper

Anchor — insert immediately after the `parse_ipv4` function added by the ipv4 plan
(Task 4a of that plan):

```c
static void parse_ipv4(const char *s, uint8_t out[4]) {
    if (!s || !s[0] || inet_pton(AF_INET, s, out) != 1)
        memset(out, 0, 4);
}
```

Insert immediately after its closing `}`:

```c
/* Parse a canonical IPv6 string into 16 raw network-byte-order bytes.
   Malformed/empty input zero-fills (mirrors parse_ipv4/parse_uuid). */
static void parse_ipv6(const char *s, uint8_t out[16]) {
    if (!s || !s[0] || inet_pton(AF_INET6, s, out) != 1)
        memset(out, 0, 16);
}
```

### 3b. `compile_one` — new scalar case

Anchor on the `FT_IPV4` scalar case added by the ipv4 plan (Task 4b):

```c
    case FT_IPV4: {
        if (c->value[0]) { parse_ipv4(c->value, cc->ipv4_val); }
        if (c->value2[0]) { parse_ipv4(c->value2, cc->ipv4_val2); }
        break;
    }
```

Insert immediately after:

```c
    case FT_IPV6: {
        if (c->value[0]) { parse_ipv6(c->value, cc->ipv6_val); }
        if (c->value2[0]) { parse_ipv6(c->value2, cc->ipv6_val2); }
        break;
    }
```

### 3c. `compile_one` — new IN-list case

Anchor on the `FT_IPV4` IN-list case added by the ipv4 plan (Task 4c):

```c
        case FT_IPV4:
            cc->in_ipv4 = malloc(sizeof(uint8_t[4]) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                parse_ipv4(c->in_values[i], cc->in_ipv4[i]);
            break;
```

Insert immediately after:

```c
        case FT_IPV6:
            cc->in_ipv6 = malloc(sizeof(uint8_t[16]) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                parse_ipv6(c->in_values[i], cc->in_ipv6[i]);
            break;
```

### 3d. `free_compiled_criteria` — add ipv6 cleanup

Anchor (exact — assumes the ipv4 plan's Task 4d has already landed
`free(arr[i].in_ipv4);`):

```c
        free(arr[i].in_uuid);
        free(arr[i].in_time);
        free(arr[i].in_ipv4);
```

Replace with:

```c
        free(arr[i].in_uuid);
        free(arr[i].in_time);
        free(arr[i].in_ipv4);
        free(arr[i].in_ipv6);
```

### 3e. `cmp_typed_field_pair` — new case

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 4e):

```c
    case FT_IPV4:
        return memcmp(a, b, 4);
```

Insert immediately after:

```c
    case FT_IPV6:
        return memcmp(a, b, 16);
```

### 3f. `match_typed` — new case (mechanical copy of the `FT_UUID`/`FT_IPV4` block, 16 bytes)

Anchor on the full `FT_IPV4` case added by the ipv4 plan (Task 4f):

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

Insert immediately after this case's closing `}`:

```c
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
```

**Note**: the `exists` check for ipv4 tested 4 individual bytes inline; for ipv6's 16
bytes this plan uses a small loop instead (`p[0]==0 && p[1]==0 && ...` would be
unreadable at 16 terms) — functionally equivalent, matches this codebase's existing
loop-based zero-check style already used by `uuid_is_zero` in util.c.

### 3g. `buf_field_value` — new textual quoted case (JOIN row emission)

Anchor on the case-label group as left by the ipv4 plan (Task 4g) — one of these two
forms, depending on whether the datetimems plan also landed:

```c
        case FT_DATE:
        case FT_DATETIME:
        case FT_DATETIMEMS:
        case FT_IPV4:
        case FT_ENUM:
```

or:

```c
        case FT_DATE:
        case FT_DATETIME:
        case FT_IPV4:
        case FT_ENUM:
```

Whichever form is found, add `case FT_IPV6:` immediately after `case FT_IPV4:`:

```c
        case FT_IPV4:
        case FT_IPV6:
        case FT_ENUM:
```

(splice into whichever of the two variants above is actually present, preserving the
rest of the case list unchanged).

### 3h. `field_type_str` — new case

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 4h):

```c
        case FT_IPV4:     return "ipv4";
```

Insert immediately after:

```c
        case FT_IPV6:     return "ipv6";
```

### 3i. `typed_field_to_buf_raw` — new case (unquoted raw string)

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 3i):

```c
    case FT_IPV4: {
        if (p[0] == 0 && p[1] == 0 && p[2] == 0 && p[3] == 0) return 0;
        char ipstr[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, p, ipstr, sizeof(ipstr))) return 0;
        return snprintf(buf, bufsz, "%s", ipstr);
    }
```

Insert immediately after:

```c
    case FT_IPV6: {
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (p[bi] != 0) { allzero = 0; break; }
        if (allzero) return 0;
        char ipstr[INET6_ADDRSTRLEN];
        if (!inet_ntop(AF_INET6, p, ipstr, sizeof(ipstr))) return 0;
        return snprintf(buf, bufsz, "%s", ipstr);
    }
```

### 3j. `decode_index_key_to_double` — new "not summable" case

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 4j):

```c
    case FT_IPV4:
        /* IPv4 addresses aren't summable. */
        return 0;
```

Insert immediately after:

```c
    case FT_IPV6:
        /* IPv6 addresses aren't summable. */
        return 0;
```

### 3k. `decode_idx_to_buf` — new case

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 4k):

```c
    case FT_IPV4: {
        if (p[0] == 0 && p[1] == 0 && p[2] == 0 && p[3] == 0) return 0;
        char ipstr[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, p, ipstr, sizeof(ipstr))) return 0;
        return snprintf(buf, bufsz, "%s", ipstr);
    }
```

Insert immediately after:

```c
    case FT_IPV6: {
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (p[bi] != 0) { allzero = 0; break; }
        if (allzero) return 0;
        char ipstr[INET6_ADDRSTRLEN];
        if (!inet_ntop(AF_INET6, p, ipstr, sizeof(ipstr))) return 0;
        return snprintf(buf, bufsz, "%s", ipstr);
    }
```

### 3l. `typed_field_to_double` — new "not summable" case

Anchor on the `FT_IPV4` case added by the ipv4 plan (Task 3l — merged code uses the
single-line form, not a multi-line comment+return):

```c
    case FT_IPV4: return 0;  /* not summable */
```

Insert immediately after:

```c
    case FT_IPV6: return 0;  /* not summable */
```

### 3m. `validate_field_type` — recognize `"ipv6"`

Anchor on the `"ipv4"` line added by the ipv4 plan (Task 4m):

```c
    if (strcmp(type, "ipv4") == 0)    return 4;
```

Insert immediately after:

```c
    if (strcmp(type, "ipv6") == 0)    return 16;
```

### 3n. Error message — append `ipv6` to the valid-types list

Anchor on whatever the string looks like after the ipv4 plan's Task 4n has landed
(`..., uuid, ipv4, currency, ...`). Splice in `ipv6, ` immediately after `ipv4, ` so the
result reads `..., uuid, ipv4, ipv6, currency, numeric:P,S, ...`.

Build check: `SKIP_TESTS=1 ./build.sh` should succeed with zero warnings introduced.

---

## Task 4 — register the new test file in `build.sh`

Anchor on the `test_ipv4.c` line added by the ipv4 plan:

```
    src/test/cases/test_ipv4.c \
```

Insert immediately after it:

```
    src/test/cases/test_ipv6.c \
```

---

## Task 5 — new test file `src/test/cases/test_ipv6.c`

Create this file with the following content (mirrors `test_ipv4.c`'s structure, 16-byte
analogue):

```c
/* test-ipv6 — exercises the FT_IPV6 field type. Storage is 16 raw bytes,
 * network byte order (inet_pton(AF_INET6, ...) output), no sign-bit flip
 * for index-key ordering (mirrors FT_UUID/FT_IPV4). Wire format is the
 * canonical IPv6 string via inet_ntop.
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

static int test_ipv6_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"ip6\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"addr:ipv6\",\"label:varchar:32\"],"
        "\"indexes\":[\"addr\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "create-object with ipv6 field succeeded");
    free(resp); resp = NULL;

    /* Insert several hosts with distinct addresses. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"key\":\"h1\",\"value\":{\"addr\":\"2001:db8::1\",\"label\":\"one\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "insert h1");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"key\":\"h2\",\"value\":{\"addr\":\"2001:db8::2\",\"label\":\"two\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"key\":\"h3\",\"value\":{\"addr\":\"fe80::1\",\"label\":\"three\"}}", &resp);
    free(resp); resp = NULL;

    /* Round-trip: get should return the canonical IPv6 string unchanged. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ip6\",\"object\":\"hosts\",\"key\":\"h1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"addr\":\"2001:db8::1\"", "addr round-tripped");
    free(resp); resp = NULL;

    /* eq lookup via index. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"eq\",\"value\":\"2001:db8::2\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "indexed eq query returned 1");
    free(resp); resp = NULL;

    /* Numeric-order range: both 2001:db8::1 and 2001:db8::2 should be
       < fe80::1 under byte-lexicographic (== numeric IPv6) ordering, since
       0x20 < 0xfe as the first address byte. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"lt\",\"value\":\"3000::\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "range query respects numeric ipv6 ordering");
    free(resp); resp = NULL;

    /* IN-list. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"in\",\"value\":[\"2001:db8::1\",\"fe80::1\"]}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "IN-list query matches 2 of 3 hosts");
    free(resp); resp = NULL;

    /* NOT_IN. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"not_in\",\"value\":[\"2001:db8::1\",\"fe80::1\"]}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "NOT_IN query matches the remaining host");
    free(resp); resp = NULL;

    /* between. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"between\","
        "\"value\":\"2001:db8::\",\"value2\":\"2001:db8:ffff:ffff:ffff:ffff:ffff:ffff\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "between query matches the 2001:db8:: hosts");
    free(resp); resp = NULL;

    /* describe-object should report the field type as "ipv6". */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"ip6\",\"object\":\"hosts\"}", &resp);
    ASSERT_CONTAINS(resp, "\"ipv6\"", "describe-object reports ipv6 type");
    free(resp); resp = NULL;

    /* Malformed address should not crash; should encode to the zero/unset
       sentinel and round-trip as absent (mirrors FT_UUID/FT_IPV4's
       parse-failure fallback convention). */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"key\":\"h4\",\"value\":{\"addr\":\"not-an-ip\",\"label\":\"bad\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "insert with malformed addr does not error");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ip6\",\"object\":\"hosts\",\"key\":\"h4\"}", &resp);
    ASSERT_TRUE(strstr(resp, "\"addr\":\"") == NULL,
        "malformed addr encodes to the unset sentinel (field omitted or null)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-ipv6", test_ipv6_run)
```

**Note on the last assertion**: same caveat as the ipv4 plan's Task 5 — confirm against
the actual zero-field omission convention during execution and adjust if reality
differs, noting the discrepancy in `PLAN_NOTES.md`.

---

## Task 6 — docs: `docs/concepts/typed-records.md`

Find the table row for `ipv4` (added by the ipv4 plan). Insert a new row immediately
after it:

```
| `ipv6` | 16 bytes, network byte order (no sign-bit flip; byte-lexicographic order matches numeric IPv6 order) |
```

Also update `CLAUDE.md`'s "Typed binary record format" table — add an `ipv6` row
immediately after the `ipv4` row added by that plan:

```
| `ipv6` | 16 bytes, network byte order |
```

---

## Final verification

1. `SKIP_TESTS=1 ./build.sh` — must complete with no errors, no new warnings.
2. `./build/bin/shard-db-test run test-ipv6` — must show all assertions passing.
3. `./build/bin/shard-db-test run-all` — must show `# total: N passed, 0 failed` (N =
   previous total + the new test's assertion count). Paste the real output.
4. If any step fails, fix the root cause — do not weaken assertions or skip the test.

Leave the branch **uncommitted** on completion; report back for review.
