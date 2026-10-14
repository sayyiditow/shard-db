# Plan: real NULL for typed fields (per-record null bitmap + explicit-null fixes)

**Date:** 2026-10-10
**Status:** awaiting human approval (CORE-PROCESS step 1 → 2).
Revised 2026-10-10 per plan review. Blocking findings addressed:
(1) encoders now write field bytes at `prefix + offset` — the original plan
left encoder destinations overlapping the bitmap; (2) the explicit-null
token check now runs on the raw span **before** quote stripping, so JSON
`"null"` (string) stays a varchar value; (3) NULLs are **indexed** under a
tier-prefix key byte so `exists`/`not_exists`/ordered walks/cursors are all
index-enumerable — the original "index miss ⇒ not_exists" was unsound; (4)
the legacy→nullmap record conversion on rewrite is specified completely,
with a rendering-preserving rule; all record consumers are mapped to their
generation-flag source; task code blocks completed.

Revised again 2026-10-10 per second review. (1) Task 5's NULL truth table
corrected — `OP_EXISTS` ⇒ false, `OP_NOT_EXISTS` ⇒ true for NULL (it
previously contradicted §2.4) — with both directions tested and legacy
generation behavior pinned; (2) the BTRI key format is now a complete
serialization spec (§2.4.1: component widths, NULL padding per type, hash
tie-break placement unchanged, key budget) with seek/range rules including
mandatory **tier clamping of open-ended upper bounds** (§2.4.2); (3) the
upgrade path is now a **2026.10.2 startup migration** (§2.4.3) — reindex
before serving, fail startup on failure, ordering reindex → KFM2 replay,
completion stamped in root evidence, 2026.09.2 minimum source version
unchanged — replacing the earlier per-request refusal design, which could
not work: the KFM2 marker is durable *before* index apply, so a write that
met a `'BTRH'` index at apply time could not be an ordinary rejected
request; (4) the rendering-preservation invariant is restated against
legacy client-visible semantics (omitted keys materialize as `null`), with
the Task 3 conversion test asserting the exact expected response; (5) new
§2.4.4: nullness for **non-indexed** fields comes from the per-record
bitmap at match time on the existing full scan.

Revised a third time 2026-10-10 per third review. (1) **Blocking —
startup ordering:** the BTRI migration now runs **after** rebuild
transaction/artifact recovery (`rebuild_recovery()`, which resolves
`indexes.new`/`indexes.old` staging) and **before** KFM2 marker replay,
as steps 2-4 of the explicit five-step sequence in §2.4.3 — reindexing
before artifact recovery could rebuild from a partially published object
layout after a crash; Task 6 separately covers pre-existing object-rebuild
staging and interrupted migration's per-shard publication (corrected in
the latest revision below); (2)
**consistency** — locked decision 6 restated as "no record/data
migration": the one-time index rebuild is intentional.

Revised a fourth time 2026-10-10 per fourth review (8×P1, 2×P2):
(1) varchar index keys corrected to **raw content** (BTRH stores no
length prefix; config.c:2247) — BTRI varchar = `[tier][content]`, no
stored length (it would break alphabetical order), `len_*` consumers use
`vlen − 1` and reject tier-1; (2) §2.7 checks `out_null[i]` **before**
`field_vals[i]` — the old order made explicit-NULL updates unreachable
(old value carried forward); single+bulk value→NULL update tests added;
(3) §2.7 conversion is bounds-safe: OLD buffers are exactly `vlen`
(slotcask.c:5448) and may be trim-compacted or predate add-field —
bounded copies, zero-padded tail, predicate runs on the materialized
destination; (4) `typed_encode_trim_len` is prefix-aware
(§2.9): prefix-inclusive returns, minimum retained length = the whole
bitmap, nullmap records never trim to 0; (5) skip-on-zero decode and
matcher absence gates (FT_TIME `exists &&` gating, FT_DATE
zero-matches-nothing) are **legacy-only** — nullmap records render and
match zero values with no zero-existence gate (§2.9, Tasks 4-5); (6)
indexed `exists` = tier-0 walk **plus per-candidate record verify**
(legacy zero-values are simultaneously value and absent under legacy
scan semantics — no tier placement reproduces both), `not_exists` =
full scan (§2.4.2 revised), mixed-generation indexed-vs-unindexed
equivalence test added (Task 6); (7) explicit `""` now sets `seen[i]`
(a value with a clear bit — never NULL, never defaulted); (8)
auto_create/auto_update stamp paths specified generation-aware with
prefixed destinations and bit clears (§2.9, Task 3); (9) the Task 4 NULL
emission hunk fixes its missing comma prepend and the tests parse
complete JSON responses; (10) `BT_MAX_VAL_LEN` expands 512 → 544 so
every previously indexable value (incl. the old 512-byte boundary and
composites) stays indexable — no silent index gaps (Task 6).

Revised a fifth time 2026-10-10 per fifth review (9×P1, 1×P2):
(1) **schema rebuilds** specified — `v2_rebuild_walk_cb`
(query_find.c:831) preserves the generation flag and remaps NULL bits
through `new_to_old` (removal shifts bit positions; §2.10), manual
`TypedSchema` construction sites in query_find.c/query_schema.c get
prefix-inclusive sizes, tests across a bitmap-width boundary (Task 7);
(2) **`cmd_count` shortcuts removed** (query.c:5521: non-varchar exists
= live count, not_exists = 0, bool-in = all) — they bypass matching
entirely; count/find equivalence tests added (§2.5, Task 5); (3) the
indexed-exists verify predicate corrected to **`legacy_field_query_exists`**
(the legacy matcher's actual OP_EXISTS semantics) — `legacy_field_absent`
models rendering omission and stays conversion-only (§2.4.2); (4) BTRI
**leaf-value consumers** enumerated (`decode_idx_to_buf` query_aggregate.c:1335,
`decode_index_key_to_double/_text`) — new `decode_btri_idx_*` wrappers
strip the tier at btree sites only, bitmap callers unchanged (§2.4.5,
Task 6); (5) trim spec replaced by a **complete single-convention
function** (§2.9) asserting `prefix ≤ result ≤ full_len`; (6)
**`typed_record_view`** validated record view added (§2.1) — bitmap
lengths 0/33, zero-length flagged values, and prefix > vlen fail closed
before any pointer math; §2.7/§2.10 use it (Task 2 tests); (7) **CAS
described truthfully** — `cas_check` (storage.c:455) flows through
`typed_get_field_str` + `match_criterion`, generation plumbing via hdr2,
JSON-null operands rejected in the shared raw-token parser (§2.6), conditional-write tests
added (Task 5); (8) **auto_create preserve transfers complete field
state** — old NULL preserved as NULL, old value clears a supplied NULL's
bit (Task 3 item 7, both directions tested); (9) §2.4.1's three obsolete
rules (u16 varchar prefix, 512 budget, len +1) deleted — one
authoritative serialization spec; (10) **canonical NULL bytes on
conversion** — legacy absent-marked fields are zeroed (float/double
-0.0 sign bit must not survive under a set bit), negative-zero fixtures
added (§2.7, Task 3).

Revised 2026-10-11 after the sixth review: BTRI output capacities are
checked before writes; index diffs use encoded OLD/NEW records; decoder
entry points use the validated record view; aggregate leaves distinguish
NULL, empty values, and errors; complement optimizations are removed;
auto_create NULL preservation zeros the destination; field comparisons
check both operands; and CAS raw-operand validation and all callers are
specified below. These are plan changes; storage implementation has not
started.

Revised 2026-10-11 after the seventh review: range ceilings end at the
NULL tier byte without padding, including varchar and fixed-prefix
composite ranges; startup migration requires durably published shards
through a separate strict checked entry point before recording completion;
and migration crash-retry tests exercise the actual per-shard publication
and temporary/spill cleanup paths. Recovery of pre-existing object-rebuild
directory staging has its own ordering test. The 2026.10.2 automatic
reindex and 2026.09.2 minimum source release remain unchanged.

**Scope.** Fix the current null-semantics bugs and represent NULL properly
for every typed field, moving forward. Explicitly **out of scope**:
`:not_null` (deliberate follow-up plan), new `IS NULL` operators
(`exists`/`not_exists` become the null tests), changes to the `schema.conf`
engine-version slot, and any data rewrite of existing objects.

**Human decisions locked in (2026-10-10 discussion):**

1. Explicit JSON `null` must never store the *string* `"null"` (varchar bug).
2. NULL is stored for **every** type — fixed-width primitives included — via
   a bitmap, Postgres-style. No sentinels.
3. Explicit `null` **wins over `:default=`** (SQL rule).
4. Omitted field + no default on insert → **NULL** (not zeros) for new records.
5. Bulk errors stay per-record (existing `errors++`/`continue` semantics).
6. No **record/data** migration: legacy records read exactly as today; new
   records carry the bitmap; stored record bytes never move. The one-time
   **index format rebuild** (`'BTRH'` → `'BTRI'`) is intentional and is
   handled by the 2026.10.2 startup migration (§2.4.3).

---

## 1. Background: today's behavior and root causes

Per-type inventory established 2026-10-10 against source:

| Type | Omitted on insert (no default) | Client sends `"field": null` | `find`/`get` renders zeroed bytes as | `exists` op |
|---|---|---|---|---|
| `varchar:N` | len=0 | **stores 4-char string `"null"`** | key omitted | len > 0 |
| `long` | zeros | zeros | **key omitted** | always true |
| `timestamp` | zeros | zeros | key omitted | always true |
| `int` | zeros | zeros | `"field":0` | always true |
| `short` | zeros | zeros | `"field":0` | always true |
| `double` | 0.0 | 0.0 | key omitted | always true |
| `float` | 0.0 | 0.0 | key omitted | always true |
| `numeric:P,S` / `currency` | zeros | zeros | `"field":0` | always true |
| `bool` | 0 → false | false | `"field":false` | always true |
| `byte` | 0 | 0 | `"field":0` | always true |
| `date` | 0 | 0 | key omitted | nonzero |
| `datetime` | d=0,t=0 | d=0,t=0 | key omitted | nonzero |
| `datetimems` | zeros | zeros | key omitted | nonzero |
| `time` | 0 | 0 | key omitted | nonzero |
| `uuid` | all-zero | all-zero | key omitted | nonzero |
| `enum` | byte idx 0 | **rejected as unknown enum value** | **first declared value** | always true |
| `ipv4` | 0.0.0.0 | 0.0.0.0 | key omitted | nonzero |
| `ipv6` | `::` | `::` | key omitted | nonzero |

### Bug B1 — explicit null on varchar stores the string `"null"`

**Root cause:** the insert encode walk (`typed_encode_defaults`,
`src/db/config.c`) strips quotes, then treats any non-empty span as a value:

```c
const char *ev = vstart; size_t el = vlen;
if (el >= 2 && ev[0] == '"' && ev[el - 1] == '"') { ev++; el -= 2; }
if (el > 0) {
```

The unquoted JSON `null` is a 4-byte span; `el > 0` is true. For
`FT_VARCHAR` the span goes through `json_unescape_cstring`, whose engine
`json_unescape_string` (`src/db/util.c`) copies bytes verbatim (it only
interprets `\` escapes; it does not require quotes), so the 4 chars `null`
are stored as varchar content. The numeric family parses `"null"` to 0 via
`atoi`/`atof`/`strtoll`. `FT_ENUM` misses the value list and the wire layer
rejects "unknown enum value" — a third, inconsistent behavior.
`typed_encode` (`config.c:2413-2414`) has the identical pattern.

### Bug B2 — omitted enum reads back as the first declared value

**Root cause:** omitted fields are zero-filled (`memset(out, 0,
ts->total_size)` at the top of `typed_encode_defaults`). FT_ENUM stores a
byte index, and `decode_field_to_buf` treats index 0 as a *legitimate*
value ("byte 0 is a legit enum index, not 'unset'"). A record that never
chose a value reports `enum_values[0]`.

### Bug B3 — zero/absent conflation (12 of 19 types)

**Root cause:** there is no null representation, so decoders adopted
skip-on-zero as an absence heuristic (`decode_field_to_buf` returns 0 for
zeroed varchar/long/timestamp/double/float/date/datetime/datetimems/time/
uuid/ipv4/ipv6; `int`/`short`/`byte`/`numeric`/`bool`/`enum` always
render). Consequences: a real `long 0` or `double 0.0` is invisible in find
output; `timestamp` epoch-0 is conflated (acknowledged in its decoder
comment); `uuid` nil, `ipv4` 0.0.0.0, `ipv6` :: are conflated; **`time`
"00:00:00" (midnight) encodes to zero, so `eq "00:00:00"` never matches its
own value** (`match_typed` FT_TIME gates equality on `exists`). The
criteria layer disagrees with the decoders (`cmp_op_i64`/`cmp_op_f64`:
`OP_EXISTS` is always 1), and joins already render literal `null` for
skipped fields while `find` omits the key — three different answers to
"what does null look like."

---

## 2. Design

### 2.1 Record layout — field-region-relative offsets (review fix #1)

v2 segment records are **varlen**: `[0..15] hash | [16..17] klen (LE u16) |
[18] flag (1B, 0=pad/1=live/2=tombstone) | [19] reserved (1B) | [20..23]
vlen (LE u32) | key bytes | value bytes | 8-aligned padding`
(`seg_scan_varlen.h:23-57` validates flag/klen/vlen and ignores byte 19;
each record's exact segment offset is in its kf entry `slotcask.h:95-101`).
slotcask.c writes the header explicitly: staging stores `dst[18]=0;
dst[19]=0` (slotcask.c:3966-3967), activation stores `dst[18]=1`
(slotcask.c:3979) — byte 19 is written before activation and never after,
so a nullmap flag written at staging is crash-safe (pre-activation records
are ignored by every reader).

**Value region, nullmap generation:**

```
[ u8 bm_nbytes ][ bm_nbytes bitmap bytes ][ field bytes ]
  \------------ value-prefix ----------------/
  field bytes begin at value + 1 + bm_nbytes   ("field-region base")
```

- Bit *i* of the bitmap = field schema-index *i* is NULL. NULL fields also
  have zeroed bytes (canonical; the bit is the truth).
- Stored `bm_nbytes = ceil(nfields/8)` at write time (1..32 at
  `MAX_FIELDS=256`); the prefix is therefore `1 + bm_nbytes`. The length
  byte makes each record self-describing across later `add-field`s.
- Header byte 19 **bit 0 (0x01) = `SREC_FLAG_NULLMAP`**. Bits 1–7 must be
  0; unknown bits ⇒ record invalid (fail closed).

**`TypedField.offset` stays field-region-relative — it is NOT shifted at
schema load.** Both generations measure field offsets from the field-region
base:

| Role | How the prefix is handled |
|---|---|
| `load_typed_schema` (config.c:1625 area) | sets `ts->nullmap_prefix = 1 + typed_nullmap_bytes(ts->nfields)` and `ts->total_size += ts->nullmap_prefix`. Offsets untouched. `total_size` includes the prefix, so every `out_size >= ts->total_size` buffer check, the derived `max_value`, and `slot_size = 24 + max_key + max_value` grow automatically. |
| **Encoders** (write) | write the head (`out[0] = bm_nbytes`, bitmap at `out[1..]`), then field bytes at `out + ts->nullmap_prefix + f->offset` (the schema's *current* prefix — encoders create the record). |
| **Decoders / criteria / all record readers** | compute `fbase = typed_field_base(value, hdr2)` once at entry (below), then use `fbase + f->offset`. The *record's own* stored length byte is used, not the schema's current one — add-field-safe. |

```c
/* Segment record header byte 19 (reserved): bit 0 = value region carries
   a field-null bitmap ([u8 bm_nbytes][bits...]) ahead of the field bytes.
   Bits 1-7 must be zero on disk; unknown bits fail closed. */
#define SREC_FLAG_NULLMAP 0x01

static inline int typed_nullmap_bytes(int nfields) {
    return (nfields + 7) / 8;
}

/* Field-region base for a typed record value. Nullmap records carry
   [u8 bm_nbytes][bm bytes] ahead of the field bytes; legacy records start
   directly at the field region. TypedField.offset is field-region-
   relative for both generations. */
static inline const uint8_t *typed_field_base(const uint8_t *value,
                                              uint8_t hdr2) {
    if (!(hdr2 & SREC_FLAG_NULLMAP)) return value;
    return value + 1 + value[0];
}

static inline const uint8_t *typed_bitmap_of(const uint8_t *value,
                                             uint8_t hdr2) {
    return (hdr2 & SREC_FLAG_NULLMAP) ? value + 1 : NULL;
}

static inline int typed_field_null(const uint8_t *bm, int bm_bytes, int i) {
    if (!bm || i < 0 || (i >> 3) >= bm_bytes) return 0;
    return (bm[i >> 3] >> (i & 7)) & 1;
}

static inline void typed_set_null(uint8_t *bm, int bm_bytes, int i) {
    if (i < 0 || (i >> 3) >= bm_bytes) return;
    bm[i >> 3] |= (uint8_t)(1u << (i & 7));
}

static inline void typed_clear_null(uint8_t *bm, int bm_bytes, int i) {
    if (i < 0 || (i >> 3) >= bm_bytes) return;
    bm[i >> 3] &= (uint8_t)~(1u << (i & 7));
}

/* Validated view of a typed record's value region. EVERY consumer uses
   this before any bitmap access or field-region pointer arithmetic —
   raw `value[0]` reads on flagged records are not permitted (a corrupt
   bm_nbytes — 0, >32, or 1+bm_nbytes > vlen — must never reach pointer
   math; the §2.7 old_region subtraction underflows otherwise).
   Returns 0 and fills the outs on success; -1 on a structurally
   invalid record (unknown hdr2 bits, bad bm_nbytes, short vlen).
   Legacy records (hdr2 without SREC_FLAG_NULLMAP) always succeed:
   base = value, bm = NULL, region = vlen. */
int typed_record_view(const uint8_t *value, size_t vlen, uint8_t hdr2,
                      const uint8_t **out_base, const uint8_t **out_bm,
                      int *out_bm_bytes, size_t *out_region);

```

Complete implementation in config.c:

```c
int typed_record_view(const uint8_t *value, size_t vlen, uint8_t hdr2,
                      const uint8_t **out_base, const uint8_t **out_bm,
                      int *out_bm_bytes, size_t *out_region) {
    if (!value || !out_base || !out_bm || !out_bm_bytes || !out_region)
        return -1;
    *out_bm = NULL; *out_bm_bytes = 0;
    if (hdr2 & ~SREC_FLAG_NULLMAP) return -1;
    if (!(hdr2 & SREC_FLAG_NULLMAP)) {
        *out_base = value; *out_region = vlen; return 0;
    }
    if (vlen < 1) return -1;
    int bm_bytes = value[0];
    if (bm_bytes < 1 || bm_bytes > 32) return -1; /* §4.2 bounds */
    if ((size_t)bm_bytes + 1 > vlen) return -1; /* no underflow */
    *out_base  = value + 1 + bm_bytes;
    *out_bm    = value + 1;
    *out_bm_bytes = bm_bytes;
    *out_region = vlen - 1 - (size_t)bm_bytes;
    return 0;
}
```

**Why no migration (verified):** records are individually sized and
8-aligned (`seg_scan_varlen_size`); kf entries store exact `file_id` +
`offset`; `slot_size`/`max_value` are derived in memory from fields.conf at
every load (`storage.c:22-23`), not persisted in schema.conf or the kf
header. The single explicit write-path gate is `if ((int)vlen >
sc->max_value)` (`storage.c:979`) — it uses the derived max and grows with
it. Free-pool buckets (`slotcask_bucket_for(capacity, max_slot_size)`,
0-3 buckets) are capacity-keyed with the schema max as bound — Task 2
verifies a grown max neither strands existing free slots nor admits a
record into a too-small slot.

### 2.2 Write semantics

| Case | Behavior |
|---|---|
| insert, field provided | encode at `prefix + offset`; bit clear |
| insert, field = JSON `null` (unquoted) | **NULL** — bit set, bytes zeroed. Wins over `:default=`. Skips the enum membership check. |
| insert, field = string `"null"` (quoted) | the 4-char string — unchanged (review fix #2: quotedness is captured **before** stripping) |
| insert, field omitted, default configured | default, bit clear (unchanged) |
| insert, field omitted, no default | **NULL** (bit set) |
| update, field omitted | value/NULL preserved from OLD (generation-aware — §2.7) |
| update, field = `null` | set NULL |
| update of a **legacy** record | NEW record written in nullmap layout via the §2.7 conversion (rendering-preserving) |
| bulk insert/update | same encoders; failures per-record (`errors++`, `continue`) — existing semantics |
| KFM2 marker replay | replays already-encoded record bytes verbatim (bitmap included) — byte-identical, no re-encode |

**Every write through the new encoders emits a nullmap record** (the
encoder always writes the prefix). A record's generation therefore flips
legacy→nullmap on first rewrite. The conversion preserves rendering
exactly (§2.7), so a rewrite never changes what clients see.

### 2.3 Read semantics

- Nullmap record + bit set → field renders as JSON **`null`** (key
  present) in `find`/`get`/`fetch`/`dict` — unifying with the join emitter,
  which already renders `null`.
- CSV: null → empty cell.
- Legacy records (byte 19 = 0): **byte-identical rendering to today** —
  no slide, no bitmap consult, skip-on-zero, enum-first-value, all of it.
- Nullmap record + bit clear → normal per-type decode, including values
  that legacy rendering hid: a real `long 0`, `double 0.0`, `time`
  midnight, `timestamp` epoch-0, `uuid` nil, `ipv4` 0.0.0.0, `ipv6` :: now
  render. This kills B3 for everything written from now on.

### 2.4 Criteria and indexes — NULLs are indexed (review fix #3)

#### 2.4.1 BTRI key layout (complete serialization spec)

Today `btree_idx_insert(path, value, vlen, hash)` passes the **sortable
value bytes** (opaque to the btree) plus the 16-byte record hash
separately; the btree sorts by (value, hash), stores the hash outside the
compared value in each leaf entry (`[u16 data_len][u8 prefix_len][suffix][hash:16B]`,
btree.h:90-95), and prefixes internal-page separators with the hash
(btree.h:26-28). The sortable value bytes per type are the existing BTRH
encodings (`typed_field_to_index_key`, config.c:2237): fixed-width types
big-endian with the numeric sign-flip applied (header `key_signed`),
**varchar = raw content, no length prefix**, enum its 1-2 byte index,
composite fields concatenated (index.c:1020-1224).

**BTRI inserts exactly one tier byte at the head of each component's
sortable bytes. Nothing else about the value string changes:**

```
single field:   key_value = [t][sortable bytes]                  (+ BT_MAX_VAL_LEN budget)
composite:      key_value = [t1][s1][t2][s2]...                  (one tier byte per component)
                t  = 0x00 (value)  → sortable bytes exactly as BTRH produced them
                t  = 0x01 (NULL)   → canonical zeroed sortable bytes:
                                     fixed-width: w zero bytes (numeric: raw zeros,
                                       sign-flip NOT applied — NULL padding is not
                                       a value; it is unreachable by 0x00 seeks)
                                     varchar: [0x01] alone (zero content bytes)
                                     enum: enum_width zero bytes
hash: unchanged — still passed separately to btree_insert, still appended
      outside the compared value, still the (value,hash) tie-break and
      separator prefix. All NULL rows of a field share key_value
      [0x01][zeros…] and tie-break by hash16, so the tier range enumerates
      every NULL row exactly once.
```

**varchar sortable bytes are RAW CONTENT — no length prefix** (BTRH
already stores raw content: `typed_field_to_index_key`,
config.c:2247 — "Index key = raw content" — and length-aware consumers
use the callback's key length, `bt_result_cb(value, vlen, …)`). BTRI
keeps that: `[tier][content]`. A stored length prefix must NOT be
introduced — it would sort keys by their first length byte before
content, breaking alphabetical ordering. Length consumers (`len_*`
callbacks) subtract the tier byte: content length = `vlen − 1`, and
tier-1 (NULL) entries are rejected — `len_*` walks are value-tier only.

- **Bytewise ordering is unchanged**: `0x00 < 0x01`, so every value key
  sorts before every NULL key, and within the value tier the BTRH byte
  order (including numeric sign-flip) is preserved verbatim. Every
  existing comparator — leaf, internal separator, k-way merge — works
  without modification. ASC walks deliver values then NULLs (Postgres
  `NULLS LAST` for ASC); DESC delivers NULLs first. Prefix compression is
  unaffected (NULL keys share a common prefix and anchor every K=16 as
  before).
- **Key budget**: `BT_MAX_VAL_LEN` expands **512 → 544** so every
  previously indexable value stays indexable (old 512-byte payload + one
  tier byte per component at the composite cap, index.c `subs[16]`);
  values already beyond the old limit keep today's never-stored
  behavior. Oversize checks compare the tier-inclusive length against
  the new constant (`btree_idx_delete`'s guard, index.c:83, and its
  insert-side counterpart).
- **`len_*` varchar ops** (btree-leaf-only): the leaf value is
  `[t][content]` — there is no stored length prefix (raw content, per
  the paragraph above); length consumers use the callback key length
  minus the tier (`vlen − 1`) and reject tier-1 entries. `len_*` walks
  are value-tier only.

#### 2.4.2 Seek and range rules (tier clamping)

- Value-op point seeks (`eq`, `in`): `[0x00] + BTRH seek bytes`
  (composite: `[0x00][v1][0x00][v2]`).
- Value-op ranges (`lt/lte/gt/gte/between`): bounds are
  `[0x00] + BTRH bound bytes`. Lower bounds cannot reach the NULL tier
  (all `0x00`-prefixed keys precede all `0x01`-prefixed keys at byte 0).
  **Upper bounds must be tier-clamped**: an open-ended upper bound
  (`gt`/`gte`, or `between` with open high side) must stop **before the
  first `0x01`-prefixed key** (`upper = [0x01]`, length **1**, exclusive),
  because a walk-to-end would sweep in NULL rows. Do not pad this bound:
  NULL varchar is exactly `[0x01]`, and the btree's length tie-break sorts
  it below `[0x01, 0x00]`. The one-byte bound excludes both that key and
  every longer fixed-width NULL key. For a range on a later composite
  component with equality-fixed preceding components, the ceiling is
  `[encoded fixed prefix][0x01]`, ending at that component's tier byte;
  it must not include payload padding or later components. Every site
  that today expresses
  "unbounded above" as walk-to-end or a max-byte sentinel must become
  tier-aware — enumerated in Task 6.
- **`exists`-driven leaves: tier-0 walk + per-candidate record verify.**
  Legacy scan semantics make a zero value simultaneously "a value" (eq 0
  matches a legacy zero long) and "absent" (scan `exists` fails an empty
  legacy varchar, midnight, zero date). No single tier placement can
  reproduce both, so BTRI builders place **all** legacy field bytes in
  tier 0 (preserving value ops exactly), and the exists leaf delivers
  tier-0 candidates that are then verified per record with the
  generation-correct **query-existence** predicate (nullmap: bitmap bit;
  legacy: `legacy_field_query_exists(f, bytes)` — a NEW helper mirroring
  the legacy matcher's actual `OP_EXISTS` semantics, query_plan.c:784
  ff.: varchar len > 0; int/long/short/double/float/numeric/bool/byte/
  enum always true; date/datetime/datetimems/time/uuid/ipv4/ipv6
  nonzero). It is deliberately NOT `legacy_field_absent` — that helper
  models *rendering omission* (a legacy `long 0` renders absent but
  query-exists true) and stays confined to the §2.7 conversion, as its
  comment requires. Indexed and unindexed `exists` therefore agree on
  mixed-generation data. (A tier-0-only walk without verification would
  disagree with legacy scan semantics — rejected.)
- **`not_exists`-driven leaves: full scan** with the same per-record
  predicate, in the same bucket as `neq`/`not_in` (which already
  full-scan). Pure tier-1 enumeration is unsound for legacy generations
  (their absent-valued fields sit in tier 0) — the second-review design
  that made not_exists a tier walk is revised here. Bitmap-indexed
  fields: unchanged (full scan, §2.4.2 last bullet).
- Task 6 adds the equivalence test: identical mixed-generation fixtures
  queried with and without the index must return identical key sets for
  `exists`, `not_exists`, `eq 0`, and midnight/empty-varchar cases.
- `neq`/`not_in` stay full-scan (unchanged), with per-record bitmap
  filtering so NULLs are excluded.
- **Bitmap-indexed fields** (enum/bool `:bitmap`): a NULL sets no bit in
  any value bitmap; `exists`/`not_exists` on bitmap-indexed fields fall
  back to full scan with bitmap filtering (a bitmap index cannot enumerate
  "no bit set"). Documented, rare.
- **Trigram `.tg` files** share the btree magic ("both extensions share
  the BTRH btree format", index.c:90-94). They bump to `'BTRI'` with
  **unchanged key shape** — NULL fields contribute no trigrams, so there
  is no NULL tier in `.tg`; the bump exists purely because the shared
  format version advances. Rebuilt by reindex like `.idx`.

#### 2.4.3 Format bump and the 2026.10.2 startup migration

Leaf/interior key strings change shape ⇒ btree magic `'BTRH'` → `'BTRI'`
(`BT_MAGIC` 0x42545248 → 0x42545249; precedent: `'BTRG'` → `'BTRH'` in
2026.05.5, btree.h:26-36).

**Why the old index must be unreachable by any write (the KFM2 constraint).**
The indexed-write protocol persists the KFM2 commit-intent marker **before**
applying the secondary-index window — that is what makes forward-replay
crash-safe. A write that discovers a `'BTRH'` index at index-apply time has
therefore already committed its marker; the mutation cannot be surfaced as
an ordinary rejected request, and per-request refusal would strand durable
markers whose replay cannot succeed against the old format. The hazard is
eliminated by making the format upgrade a **startup migration that
reindexes before the database serves any request**:

- **2026.10.2 startup migration.** On first start of 2026.10.2 against an
  eligible DB_ROOT (2026.09.2 clean-open evidence, unchanged gate), the
  daemon reindexes every indexed object (the all-tenants `reindex` path:
  O_DIRECT segment scan → fresh temporary shards → per-shard in-place
  publication via `bt_publish_replace` / `bm_publish_replace`). This
  path has no upfront `btree_idx_unlink_all` and no directory swap through
  `indexes.new`/`indexes.old` (index.c's quoted anchor:
  `/* Post-build cleanup for reindex: with the upfront wipe removed, indexes`).
  It never reads old index content, so `'BTRH'` files need not open.
  Completion is recorded by a persisted stamp alongside the
  existing root evidence files (exact mechanism: a marker file written
  atomically only after every requested shard is **durably published**
  and KFM2 replay succeeds; fsync the stamp file before rename and its
  containing directory after rename, and fail startup on either error).
  Subsequent starts skip the migration. **Any reindex failure, including
  unconfirmed publication durability, fails startup.** Existing reindex
  wrappers return success for `INDEX_BUILD_DURABILITY_UNCONFIRMED`; their
  return code alone cannot authorize the stamp. The migration uses the
  strict checked entry point specified below, requiring `INDEX_BUILD_OK`
  and `all_requested_shards_published`. No completion stamp is attempted
  on an unconfirmed build; partially published shards are left for a
  complete retry at the next start. The minimum source gate stays
  2026.09.2.
- **Startup ordering (required, five steps):**
  1. Compatibility gate (existing root gating / 2026.09.2 evidence check).
  2. **Rebuild transaction and artifact recovery** — already exists as
     `rebuild_recovery()` (objlock.c:757, via
     `recover_one_object`: `recover_transaction_layout`,
     `recover_unambiguous_legacy_layout`, and resolution of `data.new/
     data.old/indexes.new/indexes.old/fields.conf.new/fields.conf.old/
     schema.conf.new/schema.conf.old`), called first inside
     `shard_db_recover_before_stamp()` (embedded.c:646).
  3. **BTRI reindex migration** — new; inserted inside
     `shard_db_recover_before_stamp()` immediately after the
     `rebuild_recovery(db_root) != 0` check and **before the `was_clean`
     early-return** (embedded.c:652-654): a cleanly-stopped pre-2026.10.2
     root has no artifacts and no markers to replay but still carries
     `'BTRH'` indexes, so the migration must not be skipped by the clean
     path. Runs before the per-object marker sweeps.
  4. KFM2 marker replay — existing `marker_recovery_sweep_object` loop in
     the same function.
  5. Stamp migration completion and serve.

  The ordering is load-bearing in both directions. Reindex must precede
  replay: replay applies index windows, which require `'BTRI'` — and
  replay-after-reindex is safe (replayed record bytes are byte-identical;
  replayed index inserts are duplicate-tolerant via `BT_INSERT_DUPLICATE`,
  btree.h:106-109). Artifact recovery must precede the migration reindex:
  the migration *is* a rebuild, and rebuilding while an earlier crash's
  `indexes.new`/`indexes.old` staging is unresolved risks reading a
  partially published object layout. Convergence: a crash at any point
  converges on the next start — `rebuild_recovery()` resolves pre-existing
  object-rebuild transactions; the existing `index_rebuild_temp_sweep()`
  cleans abandoned reindex temporary/spill files. An interrupted migration
  can leave mixed BTRH/BTRI live shards because publication is per shard.
  With the completion stamp absent, migration rebuilds every requested
  shard from segment records again, without opening either old or already
  replaced index contents, then replay completes. Do not infer completion
  from a subset of shards having BTRI magic.
- **Minimum required source version unchanged.** 2026.10.2 keeps the
  **2026.09.2** minimum-supported-source gate exactly as 2026.10.1 has it.
  The upgrade contract is: same eligibility (2026.09.2 clean-open
  evidence), one extra automatic step at first start (the reindex
  migration), no operator action, no intermediate-version hop.
- **Per-request `'BTRH'` refusal is defense-in-depth only.** With the
  migration, a serving database always has `'BTRI'` indexes; the existing
  non-current-magic refusal (btree.h:31-32) covers out-of-band situations
  only (e.g., an operator restoring old index files into a running root).
  It must never be the primary migration mechanism — the marker hazard
  above is why.
- The operator-facing story: upgrade the binary, start once (migration
  runs, may take reindex-scale time proportional to data), server begins
  serving. Documented in changelog + upgrade docs (Task 9).

#### 2.4.4 Non-indexed fields — where nullness comes from

Non-indexed fields have no index to consult, so their nullness is known
**only per-record, from the record's own bitmap, at match time**: every
predicate on a non-indexed field already evaluates during a full parallel
shard scan (`scan_shards` / `match_typed` per record), and the matcher
gains the bitmap check there — decode hdr2 from the record's header byte
19, slide to the field region, test bit *i*. `exists`/`not_exists` on a
non-indexed field are the same full scan with the bitmap test; there is no
cross-record enumeration because nothing enumerates non-indexed fields
today either. The BTRI tier work exists purely to give **indexed** fields
index-driven NULL enumeration (`not_exists`, ordered placement, cursors);
for non-indexed fields, NULL filtering costs one branch per record on a
scan the query was already paying for, with identical semantics to the
indexed path.

#### 2.4.5 BTRI leaf-value consumers beyond `len_*` (finding: aggregate
decoders read the sortable payload directly)

`decode_idx_to_buf` (query_aggregate.c:1335), `decode_index_key_to_double`,
and `decode_index_key_to_text` decode btree leaf bytes for the indexed
`group_by` fast path and aggregate value readers; their output feeds
bucket keys that must stay byte-identical to the per-record
`agg_scan_cb` path. Under BTRI their first input byte is the tier, so an
unmodified reader misdecodes every value (the sign-flip undo would eat
the tier byte) and could treat NULL padding as a number. Rules:

- **Strip the tier at btree-leaf call sites only.** Bitmap and trigram
  payloads have no tier. Use `decode_btri_idx_*` adapters at B-tree sites;
  leave raw bitmap/trigram adapters separate. The adapters return an
  explicit status, independent of decoded length:

```c
typedef enum {
    BTRI_DECODE_ERROR = -1,
    BTRI_DECODE_NULL = 0,
    BTRI_DECODE_PRESENT = 1
} BtriDecodeStatus;

/* Every adapter sets its length out-param to 0 before decoding. An
   empty varchar is PRESENT with length 0; NULL is a different status. */
static BtriDecodeStatus btri_payload_view(const uint8_t *key, size_t klen,
                                          const uint8_t **payload,
                                          size_t *payload_len) {
    if (!payload || !payload_len) return BTRI_DECODE_ERROR;
    *payload = NULL;
    *payload_len = 0;
    if (!key || klen == 0 || key[0] > 1) return BTRI_DECODE_ERROR;
    *payload = key + 1;
    *payload_len = klen - 1;
    return key[0] ? BTRI_DECODE_NULL : BTRI_DECODE_PRESENT;
}
```

- Tier-1 is not passed to a value decoder. Numeric accumulators skip it;
  group-by **creates/looks up a NULL bucket** and associates the record
  hash with that bucket. `sec_map_build_worker` must store explicit NULL
  state for secondary group fields; a missing hash-map entry is an error
  or absent candidate, never a representation of NULL. Replace each
  `if (n <= 0) continue` at migrated sites with status dispatch. PRESENT
  with zero length must reach matching, grouping, and text min/max.
- Tier-0 empty varchar is decoded as the empty string without calling
  originals that return missing for `plen == 0`. Bit-clear zero calendar
  values also need a value decoder with zero rendering enabled. A tier-0
  key alone cannot distinguish a legacy absent marker from a nullmap real
  zero: for payloads where the existing per-record aggregate/group reader
  has generation-dependent absence semantics, fetch the record through
  the existing lock-safe record-read path and use that same reader with
  hdr2. Do not infer generation from the index tier or use the rendering
  conversion predicate as an aggregate predicate. Non-ambiguous values
  retain the index-only decode path.
- Group buckets and secondary maps carry nullness separately from text:
  `NULL`, `""`, and the literal `"null"` are three distinct keys. Encode
  every group component as `[u8 is_null][u32 BE text_length][text]`, with
  NULL length zero. Indexed, scan, clone/merge, and raw-integer group
  paths must use the same representation (or route to the common group
  builder); raw index bytes with a tier must not be mixed with untagged
  scan integer keys. Emit JSON null using the tag, not a magic string.
- Migrate the B-tree callers of `decode_idx_to_buf`,
  `decode_index_key_to_double`, and `decode_index_key_to_text`, including
  `sec_map_build_worker`, `igb_pass1_worker`, indexed min/max/sum readers,
  and query filtering callbacks. Preserve the `AggValueStatus` contract
  of numeric/text originals: a wrapper must return the appropriate
  missing enum for NULL, not numeric literal 0 unless that enum is 0.
  Unknown tiers, malformed fixed widths, or corrupt records abort the
  request rather than skipping a row. Bitmap callers retain raw decoders.

### 2.5 Aggregates / order_by / cursor

- **`count` shortcuts are removed (finding: they bypass NULL matching
  entirely).** `cmd_count` (query.c:5521) today answers non-varchar
  `exists` with the live-record count, `not_exists` with 0, treats bool
  `in [true,false]` as every record, and derives varchar `not_exists`
  from an index count complement — all false under NULLs, and
  unreachable by any `match_typed` fix because they never call it. All
  of these shortcut branches are deleted: exists/not_exists/bool-in
  counts route through the normal planner path (index drive or scan
  with the §2.4.2 predicates). The documented O(1) behavior for *empty*
  criteria (`count` with no predicate) is untouched. Tests: count
  must equal find-length on mixed-generation indexed and unindexed
  fixtures for exists/not_exists/bool-in (Task 5).
- **Remove the remaining complement optimizations too.** At the quoted
  anchor `if (is_single_leaf && op_is_negatable(op))` in `cmd_count`,
  delete the live-count-minus-positive branch and its now-unused
  `op_is_negatable`/`op_invert` helpers. The normal branch must execute
  the original negative predicate, including NULL exclusion, rather
  than count its positive inverse. In `cmd_aggregate_do`, remove the
  NEQ algebraic split selected by `if (neq_eligible)` (including the
  count-only subtraction and full-side sum/avg subtraction/inverted
  prefilter variants); route through the normal `agg_run_plan` with
  the original criteria tree. Indexes may still source candidates, but
  all matches are verified with generation-correct semantics. A NULL
  criterion field belongs to neither a positive value predicate nor
  its negative value predicate. Empty-criteria O(1) row count remains.
  Test all negative families: neq/not_in/not_like/not_contains, count
  and aggregate count/sum/avg, with indexed and unindexed twins.

- `sum`/`avg`/`min`/`max` skip NULLs (SQL); `avg` of all-NULL → NULL.
  `count` is a row count — unchanged. `group_by` NULL forms its own group
  (key renders `null`).
- Ordered finds: the k-way merge and buffered sort need **no comparator
  changes** (tier byte orders NULLs); the merge simply delivers NULL-tier
  rows at the ASC tail (review fix #4).
- Keyset cursor tokens: the per-field resume value for ordered fields
  stores the **raw last key including its tier byte** (and the multi-field
  cursor format from 2026-09-18 gains a per-field tier byte; parser
  fail-closed on unknown token versions). Resume `> last_key` crosses the
  NULL boundary with no special case.

### 2.6 Explicitly unchanged

Wire protocol, framing, CLI surface, `fields.conf` syntax (no new
modifiers), auth, files, tokens, record-level `get`/`exists`/`delete`/
`keys`.

**CAS is not "unchanged" but its mechanism is** (corrected — an earlier
revision wrongly claimed CAS encodes both sides through the encoder).
Actual CAS flow: `cas_check` (storage.c:455) extracts each criterion's
field via `typed_get_field_str` and calls `match_criterion` on the
string. NULL support therefore rides the §2.9 plumbing:
`typed_get_field_str` becomes generation-aware (nullmap record: bit set
⇒ returns NULL, same "absent string" convention trim-compacted fields
already produce; bit clear ⇒ the value string, including `"0"` and
`""`). Consequences, defined: a NULL field never matches a value
criterion (`match_criterion`'s existing NULL-string behavior — the same
behavior a trimmed field has today). Nullmap exists/not_exists are
bitmap-authoritative; legacy CAS retains its current extraction/matching
semantics (these differ from legacy typed query existence for some zeros).
Do not silently change legacy CAS to the query-existence predicate.

**Raw operand rejection belongs in the shared parser, not compilation.**
CAS calls `parse_criteria_json` without `compile_criteria_tree`. In
`parse_one_criterion`, before quote stripping/unescaping destroys token
kind, at the quoted anchor
`/* Validate value — required for all ops except exists/not_exists */`,
insert this rejection; the existing validation follows it:

```c
const char *operand_keys[] = { "value", "value2" };
for (size_t oi = 0; oi < sizeof(operand_keys) / sizeof(operand_keys[0]); oi++) {
    const char *raw_operand = NULL;
    size_t raw_operand_len = 0;
    if (op_requires_value(c->op) &&
        json_obj_get(&cobj, operand_keys[oi], &raw_operand, &raw_operand_len) &&
        json_span_is_null(raw_operand, raw_operand_len)) {
        free(v); free(v_raw); free(v2);
        return -1;
    }
}
```

For array operands, apply the same raw-span test to each IN/NOT_IN and
array-form BETWEEN element before stripping quotes; reject unquoted NULL
members rather than treating them as strings or numeric zero. Quoted
`"null"` remains a valid varchar/enum value. Existence operands are unused;
NULL checks use exists/not_exists. The parser rejection is shared by query
and CAS, including bulk per-record conditions.

`cas_check` gains a per-record `uint8_t hdr2` argument. **All callers:**

| Quoted function anchor | Flag source |
|---|---|
| `static int v2_insert_check_fn(` | `old->hdr2` |
| `static int v2_update_check_fn(` | `old->hdr2` |
| `static int v2_delete_check_fn(` | `old->hdr2` |
| `if (dry_run) {` in `cmd_update_v2` and `cmd_delete_v2` | GET result's hdr2, read together with value/vlen |
| `static int v2_bulk_upd_value_compute(` | `old->hdr2` |
| `static int v2_bulk_upd_json_value_compute(` | `old->hdr2` |
| `static int v2_bulk_del_crit_pre_commit_bulk(` | `old->hdr2` |
| `static int v2_bulk_del_crit_prepare_window(` | `r->old_hdr2`, paired with `r->old_value` |

Carry flags in the existing OLD/result structs, not a shared mutable
schema/request context. At completion, search every `cas_check(` caller
in storage.c/query_bulk.c and match it to this table. Single/bulk writes,
deletes, and dry-run must agree on nullmap conditions; legacy CAS golden
responses remain unchanged. Composite CAS existence is based on component
presence, not concatenated length: an all-empty but non-NULL composite is
present; any NULL component makes it absent.

### 2.7 Legacy→nullmap record conversion (review fix #4) — update path

`v2_update_new_from_old` (storage.c) currently does
`memcpy(out_value, old->value, old->vlen)` + tail zero-fill, then encodes
patched fields at `out + f->offset`. With the prefix, that copy would
smear old field bytes over the new head. The complete OLD→NEW
transformation:

```c
/* Rendering predicate for legacy records: 1 if the legacy decoder would
   SKIP this field's bytes (the §1 "renders" column, negated). Used ONLY
   to port a legacy record into nullmap form without changing what
   clients see. */
static int legacy_field_absent(const TypedField *f, const uint8_t *p) {
    switch (f->type) {
    case FT_VARCHAR: {
        int len = ((int)p[0] << 8) | p[1];
        return len == 0;
    }
    case FT_LONG: case FT_TIMESTAMP: {
        int64_t v = (int64_t)(((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                              ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                              ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                              ((uint64_t)p[6] << 8) | p[7]);
        return v == 0;
    }
    case FT_DOUBLE: { double v; memcpy(&v, p, 8); return v == 0.0; }
    case FT_FLOAT:  { float v;  memcpy(&v, p, 4); return v == 0.0f; }
    case FT_DATE: {
        int32_t v = (int32_t)(((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                              ((uint32_t)p[2] << 8) | p[3]);
        return v == 0;
    }
    case FT_DATETIME: {
        int32_t d = (int32_t)(((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                              ((uint32_t)p[2] << 8) | p[3]);
        uint32_t t = ((uint32_t)p[4] << 16) | ((uint32_t)p[5] << 8) | p[6];
        return d == 0 && t == 0;
    }
    case FT_DATETIMEMS: {
        int32_t d = (int32_t)(((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                              ((uint32_t)p[2] << 8) | p[3]);
        uint32_t ms = ((uint32_t)p[4] << 24) | ((uint32_t)p[5] << 16) |
                      ((uint32_t)p[6] << 8) | p[7];
        return d == 0 && ms == 0;
    }
    case FT_TIME:
        return p[0] == 0 && p[1] == 0 && p[2] == 0;
    case FT_UUID: {
        int allzero = 1;
        for (int i = 0; i < 16; i++) if (p[i]) { allzero = 0; break; }
        return allzero;
    }
    case FT_IPV4:
        return p[0] == 0 && p[1] == 0 && p[2] == 0 && p[3] == 0;
    case FT_IPV6: {
        int allzero = 1;
        for (int i = 0; i < 16; i++) if (p[i]) { allzero = 0; break; }
        return allzero;
    }
    default:  /* int/short/byte/numeric/currency/bool/enum always rendered */
        return 0;
    }
}
```

Transformation in `v2_update_new_from_old` (replacing the memcpy + tail
zero-fill):

```c
/* NEW is always written in the current nullmap layout. OLD may be legacy
   (oh2 == 0) or nullmap; field offsets are field-region-relative for both
   (§2.1). Client-provided fields override; out_null[i] (from
   json_get_fields_null_aware) marks explicit JSON null — note out[i] is
   NULL for those, so out_null must be consulted FIRST. A removed schema
   field keeps its bytes untouched (existing rule). */
const uint8_t *old_base; const uint8_t *old_bm; int old_bm_bytes;
size_t old_region;
if (typed_record_view(old->value, (size_t)old->vlen, c->old_hdr2,
                      &old_base, &old_bm, &old_bm_bytes,
                      &old_region) != 0) {
    /* Malformed flagged OLD (§2.1 view validation) — fail the update,
       never copy. */
    snprintf(c->err_buf, sizeof(c->err_buf),
             "corrupt record header on update target");
    return -1;
}

memset(out_value, 0, (size_t)c->idx_ts->total_size);
out_value[0] = (uint8_t)typed_nullmap_bytes(c->idx_ts->nfields);
uint8_t *nbm = out_value + 1;
int nbm_bytes = typed_nullmap_bytes(c->idx_ts->nfields);

for (int i = 0; i < c->idx_ts->nfields; i++) {
    TypedField *f = &c->idx_ts->fields[i];
    if (f->removed) continue;
    uint8_t *dst = out_value + c->idx_ts->nullmap_prefix + f->offset;

    if (out_null[i]) {                     /* explicit JSON null */
        typed_set_null(nbm, nbm_bytes, i); /* dst stays zero — canonical */
        continue;
    }
    if (field_vals[i]) {
        /* ... existing varchar-overflow check on field_vals[i] ... */
        encode_field(f, field_vals[i], dst);   /* bit already clear */
        continue;
    }
    /* Omitted on update → carry OLD forward, preserving rendering. */
    if ((c->old_hdr2 & SREC_FLAG_NULLMAP) &&
        typed_field_null(old_bm, old_bm_bytes, i)) {
        typed_set_null(nbm, nbm_bytes, i); /* dst stays zero — canonical */
        continue;
    }
    /* Bounded copy: out-of-bounds tail stays zero (NEW was memset);
       src is formed only when in-bounds. OLD may be trim-compacted or
       predate add-field (slotcask allocates OLD at exactly vlen,
       slotcask.c:5448). */
    size_t avail = (old_region > (size_t)f->offset)
        ? old_region - (size_t)f->offset : 0;
    size_t copyn = avail < (size_t)f->size ? avail : (size_t)f->size;
    if (copyn) memcpy(dst, old_base + f->offset, copyn);
    if (!(c->old_hdr2 & SREC_FLAG_NULLMAP) &&
        legacy_field_absent(f, dst)) {
        /* legacy skip-on-zero ⇒ NULL. Zero dst even when OLD bytes were
           nonzero-but-omitted (float/double -0.0 has its sign bit set
           yet compares == 0.0 and the legacy decoder omits it) — NULL
           bytes are canonically all-zero (§4.3). dst is fully
           materialized, so the predicate is safe for trimmed/short OLD
           records too. */
        memset(dst, 0, (size_t)f->size);
        typed_set_null(nbm, nbm_bytes, i);
    }
}
```

(`c->old_hdr2` is a new `V2UpdateCtx`/`SlotcaskOldRecord` field filled by
slotcask when it materializes OLD — the same header byte it already parses
for flag/klen/vlen.) Note the trailing-zero trim behavior is preserved:
NULL and zero fields trim identically; a nulled trailing field survives via
its head bit.

### 2.8 Generation-flag plumbing — every record consumer mapped (review item)

The flag source is always the segment record header byte 19 at its
existing parse points. Distribution:

| Consumer | How it obtains hdr2 |
|---|---|
| slotcask record reads (single GET / two-phase bulk fetch) | the header is already parsed at `rec[18]` check sites (slotcask.c:2094, 2981, 5444, 8753, 9458 area); each adds `hdr2 = rec[19]` and threads it via the existing out-structs (`slotcask.h:423-427` value-out struct gains `uint8_t hdr2`; the resolved-location struct at `slotcask.h:550-579` likewise) |
| O_DIRECT seg scan (io_direct.c) | `seg_scan_varlen_struct_ok` (seg_scan_varlen.h:35) gains `out_hdr2` (with the unknown-bit fail-closed check) — the scan callback struct carries it |
| mmap maintenance walks (slotcask.c `seg_stat_one_varlen`, compaction) | same helper, same threading |
| v1-compat callbacks (synthesized `SlotHeader`) | synthesis sites (`query_find.c:40`, `query_find.c:1712`, `query_aggregate.c:37,76,2781,2848`) set the synthesized u16 flag to `1 \| (hdr2 << 8)`; every consumer normalizes `hdr->flag & 0xFF` before comparing and extracts `hdr2 = hdr->flag >> 8` once at entry |
| `match_typed` + per-type dispatch (query_plan.c) | receives hdr2 via the scan-callback chain; computes `fbase`/bitmap once per record |
| `typed_decode` / `typed_decode_stream` / `typed_get_field_str` | take `uint8_t hdr2` param; callers from §3 pass it through |
| `v2_update_new_from_old` | `V2UpdateCtx`/`SlotcaskOldRecord` gain `uint8_t old_hdr2` (§2.7) |
| index apply (write-path diff, bulk window, rebuild scan) | record readers above supply hdr2; index-key builders take the tier decision from the bitmap |

Standing rule for the sweep: `grep -rn "rec\[18\]\|record\[18\]\|->flag == 1\|flag != 1" src/db/*.c`
must return only normalized (`& 0xFF`) comparisons plus the flagged parse
points when the tasks complete.

### 2.9 Trim, auto-stamp, and zero-rendering under the prefix

**Trailing-zero trimming (`typed_encode_trim_len`, config.c:3305).** The
function scans backward reading `buf + f->offset` and returns field-end
offsets (0 when every field is zero); slotcask consumes the returned
length directly as the stored vlen. Under the prefix the original shape
would inspect bitmap bytes as field bytes, or double-add the prefix.
Complete replacement — **one coordinate convention: scan in field-region
coordinates, add the prefix exactly once at the return**:

```c
/* Return the minimum byte length needed to represent the record.
   Legacy schemas (nullmap_prefix == 0): unchanged behavior — 0 when
   every field is zero. Nullmap schemas: work in field-region
   coordinates (buf + prefix ..), add the prefix once at the return;
   result is always in [prefix, full_len] — a nullmap record is never
   trimmed below its bitmap (a NULL trailing field's zero bytes trim
   away; its bit must survive) and never reports more than it was
   given. */
size_t typed_encode_trim_len(const TypedSchema *ts, const uint8_t *buf,
                              size_t full_len) {
    if (!ts || !ts->typed || !buf || full_len == 0) return 0;
    size_t prefix = (size_t)ts->nullmap_prefix;   /* 0 for legacy */
    if (full_len < prefix) return full_len;       /* defensive */
    size_t min_ret = prefix;
    const uint8_t *fields = buf + prefix;
    size_t region = full_len - prefix;            /* single subtraction */
    for (int i = ts->nfields - 1; i >= 0; i--) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;
        size_t end = (size_t)f->offset + (size_t)f->size;
        if (end > region) continue;   /* field outside stored region — skip */
        const uint8_t *fp = fields + f->offset;
        for (size_t b = 0; b < (size_t)f->size; b++) {
            if (fp[b]) return min_ret + end;  /* prefix added once, here */
        }
    }
    return min_ret;   /* legacy: 0 (unchanged); nullmap: whole bitmap */
}
```

Invariant (asserted in tests): for nullmap records
`ts->nullmap_prefix ≤ result ≤ full_len`, including a nonzero final
field (`prefix + end`) and an all-NULL/all-zero record (`prefix`).
`schema_trim_fn` (config.c:3325) needs no signature change; the ctx
TypedSchema carries the prefix.

**Auto-stamp paths write at raw offsets — all become generation-aware.**

- `auto_create` preserve/stamp (storage.c:1012 area): the preserve branch
  copies from OLD (`ac_old`, exactly `ac_old_vlen` bytes) — source reads
  use the OLD generation base from `typed_record_view`, bounded by
  the OLD field-region length, zero_field fallback
  like `typed_get_field_str`; the stamp branch writes at
  `typed_buf + ts->nullmap_prefix + f->offset` **and clears the field's
  NULL bit** (a stamp materializes a value). **Preserve transfers
  complete field state — bytes AND nullness**: OLD nullmap bit set ⇒
  zero the destination, then set the new bit; OLD legacy and
  `legacy_field_absent` ⇒ zero the destination, then set the new bit;
  otherwise clear the bit, zero the destination, and bounded-copy —
  copying bytes over a client-supplied NULL without clearing its bit
  would produce NULL-with-nonzero-bytes, and preserving an old NULL
  without setting the bit would render zeros (Task 3 tests both
  directions).
- `auto_update` stamping loop (`v2_update_new_from_old`, after the §2.7
  transformation): same prefixed destination + `typed_clear_null` — an
  auto_update stamp over a carried-forward NULL replaces it with now(),
  which is the existing semantic (auto_update always stamps).
- Bulk equivalents mirror via the shared helpers.

**Zero values render on nullmap records — skip-on-zero becomes
legacy-only (generation-aware decode/match).** The decoders'
skip-on-zero cases (`decode_field_to_buf`: long/timestamp/double/float/
date/datetime/datetimems/time/uuid/ipv4/ipv6 return 0 for zeroed bytes;
varchar returns 0 for len 0) and the matchers' absence gates
(`match_typed` FT_TIME gates every comparison on `exists` — so `eq
"00:00:00"` never matches; FT_DATE: "empty date matches nothing except
NOT_EXISTS") apply **only to legacy records**. Nullmap records carry the
authoritative bit, so:

- decode: bit clear ⇒ render the value unconditionally — a real
  `long 0`, `""`, midnight, zero date, nil uuid, 0.0.0.0 all render;
  bit set ⇒ `null` (§2.3). `decode_field_to_buf` gains a mode/flag
  (`render_zeros`, set for nullmap records) threaded from
  `typed_decode`/`typed_decode_stream`/`typed_get_field_str`.
- match: the NULL check replaces the absence heuristics — bit clear ⇒
  all value ops evaluate on the raw bytes with no zero-existence gate
  (midnight `eq` matches; zero date matches `eq 0`), bit set ⇒ §2.4.2
  truth table. Legacy records keep every gate unchanged.

### 2.10 Schema rebuilds — explicit bitmap transformation (finding:
`v2_rebuild_walk_cb` reconstructs records)

Vacuum, field edits, and field additions rebuild records through
`v2_rebuild_walk_cb` (query_find.c:831), not through the §2.7 update
transformation. Rules:

- **Verbatim path** (`slot_changed == 0`, e.g. splits-only resplit):
  record bytes are re-inserted as-is — the generation flag travels with
  the bytes; nothing to do (assert via test).
- **Recompose path** (add-field / edit-field / compact): the walk maps
  `new_to_old[k]`; it must also map **NULL bits**: nullmap OLD with bit
  set at schema index `oi` ⇒ new bit set at index `k` (new bytes zero);
  legacy OLD ⇒ per-field `legacy_field_absent` on the (bounded) old
  bytes, same as §2.7 — **removal shifts bit positions**: removing
  field 0 moves field 1's NULL bit to position 0, because bit indices
  are schema-index based and the new schema re-indexes fields. New
  fields (`new_to_old == -1`) are never NULL (zeros/backfill — existing
  behavior). Edited (transformed) fields: transform runs on non-NULL
  bytes only; a NULL stays NULL through a size/scale change.
- **OLD validation**: the walk reads OLD via `typed_record_view`
  (§2.1) — malformed flagged records fail the rebuild (fail closed),
  bounded copies as in §2.7.
- **Manually constructed schemas**: `query_find.c` and `query_schema.c`
  build `TypedSchema` instances by hand (rebuild staging) — each must
  set `nullmap_prefix` and the prefix-inclusive `total_size`
  (`load_typed_schema` alone does not cover them). Sweep item in Task 7
  with a grep for `TypedSchema` construction sites.
- Tests (Task 7): field removal across a bitmap-width boundary (9
  fields → 8: bitmap shrinks 2→1 bytes; NULL on old field 8 must land
  on new field 7's bit), resizing (edit-field) a NULL field (stays
  NULL), and a layout-preserving rebuild (verbatim copy preserves
  hdr2).

---

### 2.11 BTRI capacity and binary-record key derivation

The raw `typed_field_to_index_key` remains a sortable-payload primitive:
it has no tier and reads field-region-relative bytes. Bitmap/trigram
payloads use a validated record base and their own NULL exclusion. BTRI
and typed buffered-sort callers use the checked adapter below. No caller
prepends a tier by writing beyond an old allocation or uses a JSON
round-trip to reconstruct a stored key.

```c
/* 1 = key written, -1 = invalid input/record or insufficient capacity.
   Every valid BTRI field has a key, including NULL and empty varchar.
   Capacity is checked BEFORE any output write. The raw payload encoder
   remains unchanged and is called only with validated field coordinates. */
int typed_btri_field_key(const TypedSchema *ts, const uint8_t *value,
                         size_t vlen, uint8_t hdr2, int field_idx,
                         uint8_t *out, size_t out_cap, size_t *out_len) {
    if (!out_len) { errno = EINVAL; return -1; }
    *out_len = 0;
    if (!ts || !out || field_idx < 0 || field_idx >= ts->nfields) {
        errno = EINVAL; return -1;
    }
    const uint8_t *base = NULL, *bm = NULL;
    int bm_bytes = 0;
    size_t region = 0;
    if (typed_record_view(value, vlen, hdr2, &base, &bm,
                          &bm_bytes, &region) != 0) {
        errno = EILSEQ; return -1;
    }
    const TypedField *f = &ts->fields[field_idx];
    if (f->removed || f->offset < 0 || f->size <= 0) {
        errno = EINVAL; return -1;
    }
    int is_null = (hdr2 & SREC_FLAG_NULLMAP) &&
                   typed_field_null(bm, bm_bytes, field_idx);
    size_t payload_len = (size_t)f->size;
    if (f->type == FT_VARCHAR) {
        if (f->size < 2) { errno = EINVAL; return -1; }
        payload_len = 0;
        size_t off = (size_t)f->offset;
        if (!is_null && off <= region && (size_t)f->size <= region - off) {
            const uint8_t *src = base + off;
            payload_len = ((size_t)src[0] << 8) | src[1];
            size_t max_content = (size_t)f->size - 2;
            if (payload_len > max_content) payload_len = max_content;
        }
    }
    if (out_cap < 1 || payload_len > out_cap - 1) {
        errno = ENOBUFS; return -1;
    }
    out[0] = is_null ? 1 : 0;
    if (is_null) {
        memset(out + 1, 0, payload_len);
    } else {
        size_t raw_len = 0;
        typed_field_to_index_key(ts, base, region, field_idx, out + 1, &raw_len);
        if (raw_len != payload_len) { errno = EILSEQ; return -1; }
    }
    *out_len = 1 + payload_len;
    return 1;
}
```

Expose this signature in types.h beside the raw encoder declaration.
Callers must pass actual vlen and the record's hdr2; schema size is not a
substitute for record length. A fixed-width field's maximum output is
`1 + f->size`; varchar's is `1 + (f->size - 2)`. For allocation a checked
`1 + f->size` bound is sufficient for all supported types. Component
bounds accumulate with checked size_t addition, up to the existing
16-component cap. For composite output, call the adapter with
`out_cap - used` and advance `used` only after success. An ENOBUFS error
must not be turned into an absent field or silent index omission.

| Quoted anchor / consumer | Required change |
|---|---|
| `int build_index_key_from_record_into(` | add hdr2 and index-kind arguments; BTRI uses checked adapter with remaining capacity; check before writing, never afterwards |
| `int build_index_key_from_record(` | add hdr2/index kind; allocate checked tier-inclusive bound before writing, including UUID/IPv6 and composite keys |
| `static int v2_bulk_ins_prepare_window(` and its single-field/composite key collection | replace raw writes with checked BTRI adapter; pass NEW hdr2; NULL bitmap/trigram fields produce no value entry |
| `static void mf_append_field(` | actual record view and tier-inclusive scratch bounds before each component; preserve distinct bitmap/trigram payload formats |
| `static int ordered_collect_cb(` | allocate each typed sort payload at tier-inclusive capacity; call checked BTRI adapter with hdr2 |
| `static int small_prefilter_collect_cb(` and surrounding sort/fetch callbacks | all later sort-key builders use checked BTRI adapter; fixed array capacities passed explicitly |
| `static int cursor_fetch_cb(` and heap/top-N/driver-prefilter sort-key sites | same tier-inclusive representation and explicit capacities; propagate failure to request |
| `static int v2_insert_pre_commit(` | OLD key from OLD bytes/hdr2, NEW key from final encoded new_value/new_vlen/NEW hdr2; remove old_json and request-JSON key sourcing |
| insert prepare/apply, update/delete diffs, bulk window, replay, add-index/reindex builders | reuse the same binary key builder and preserve existing pre-marker preparation/post-marker apply ordering |

Search all `typed_field_to_index_key`, `build_index_key_from_record`, and
`build_index_key_from_json` consumers. Remaining raw calls are only inside
the checked adapter or documented bitmap/trigram payload extraction using
validated bases. JSON key builders may remain for explicit JSON utility
uses, but not mutation index diffs or durable replay. Query seek encoding
is a separate value-operand path and must not accidentally gain two tiers.

Keep admission consistent with the stated 512-byte old payload budget:
sum raw payload lengths (tier bytes excluded) before admitting a key;
raw payload >512 retains the old not-indexable policy. Tier-inclusive
output must also fit BT_MAX_VAL_LEN=544. The encoder's capacity error is
distinct from this documented oversized-value admission policy. Apply
the same admission decision to insert, delete, rebuild, and seeks.

Regression fixtures: buffer canaries around 16/17-byte UUID and IPv6
outputs, composite exact-fit and one-byte-short output, buffered ordering
of NULL/value fields without an index, legacy indexed long zero updated
to 5 (no stale zero hit), defaults/sequences/UUID generators and automatic
timestamps (no regeneration during key derivation), and equality of
incremental index contents with reindex/replay contents.

## 3. Call-site inventory (consumers of typed record bytes)

Searched 2026-10-10 (`grep -rn "\.offset + \|f->offset\|fields\[i\].offset"
src/db/*.c` — 47 sites in 11 files). Grouped by the entry function that
must adopt `typed_field_base` (internal sites switch `value +` → `fbase +`
mechanically):

**Encode (write) — prefix + offset (§2.1):** `typed_encode_defaults`
(config.c — client-value branch ~2951/2955, defaults pass ~2984, head
emission), `typed_encode` (config.c:2372 — client-value branch
2423-2428), `v2_update_new_from_old` (storage.c:1241/1289 — replaced by
§2.7), bulk update merge (`json_get_fields*` caller, query_bulk.c:3763),
add-index rebuild field encode (index.c:1405 — applies only if it writes a
record buffer; index *keys* change via §2.4 instead), **trim**
(`typed_encode_trim_len` config.c:3305 / `schema_trim_fn` config.c:3325 —
§2.9), **auto-stamp** (`auto_create` preserve/stamp storage.c:1012 area,
`auto_update` loop in `v2_update_new_from_old` — §2.9).

**Decode (read) — fbase slide:** `typed_decode` (config.c:3198),
`typed_decode_stream` (config.c:3265), `typed_get_field_str` (config.c),
`print_record_json/dict/row` (query_find.c:1470/1498/1527 — wrap the
above), CSV emit (query_find.c), join emit (query_join.c:325-414 —
already renders `null`).

**Criteria:** `match_typed` (query_plan.c:910) + per-type dispatch and
`cmp_op_i64`/`cmp_op_f64`; index-leaf len-op parse sites (tier +1);
`criteria_match_tree`/plan leaves are op-agnostic (no change).

**Index:** index-key builders gain the tier byte — write-path pre-commit
diff (`storage.c`), bulk apply-window collection (`sw->idx_pairs`,
query_bulk.c), `btree_idx_insert/delete` key composition, seek-key
construction in `btree_idx_search/range/range_ex` (prepend `0x00`),
`btree_idx_walk_ordered` (no comparator change — tier orders), add-index
rebuild scan (index.c:1405 area), KFM2 replay re-derivation (Task 7).

**Aggregates:** `agg_scan_cb` + accumulator/group-by value readers
(query_aggregate.c).

**Storage/flag:** header staging/activation (slotcask.c:3966-3979,
3996-3997), header parse points (§2.8 list), `seg_scan_varlen_struct_ok` /
`_resync` (seg_scan_varlen.h), read-API out-structs (slotcask.h:423-427,
550-579), synthesized-`SlotHeader` sites (§2.8).

---

## 4. Invariants and edge cases

1. Legacy record + any read op ⇒ byte-identical client-visible behavior to
   the pre-plan binary. First rewrite flips the record to nullmap form via
   the §2.7 conversion. "Rendering-preserving" is defined against **legacy
   client-visible semantics**, not raw JSON shape: every field the legacy
   binary rendered keeps rendering with the same value, and every field it
   omitted (skip-on-zero set, mirrored exactly by `legacy_field_absent`)
   renders as `null` under the new decoder. The JSON shape therefore
   changes on rewrite exactly in that omitted keys materialize as `null` —
   the Task 3 conversion test pins this explicitly.
2. Unknown bits in header byte 19 ⇒ record invalid, everywhere (scan,
   resync, GET). `bm_nbytes` must be 1..32 and `1 + bm_nbytes ≤ vlen`,
   else invalid.
3. NULL field bytes are always zero on disk; a bit set with nonzero bytes
   is invalid (defensive decode: LOG_ERROR + render NULL — never crash,
   never render garbage).
4. Explicit JSON `null` detection runs on the **raw span before quote
   stripping**, in every encoder (`typed_encode`, `typed_encode_defaults`)
   and every `json_get_fields*` consumer — quoted `"null"` is always a
   string value.
5. `:default=` never fires for a provided null **or a provided empty
   string** (`seen[i]` is set for any provided span — `""` is a value
   with a clear bit, never NULL, never defaulted).
6. `auto_update` still stamps on update even when other fields go NULL;
   a stamp clears the field's NULL bit and lands at the prefixed offset
   (§2.9).
7. Trim (§2.9): nullmap records are never trimmed below their bitmap
   prefix; reads are field-region-relative and prefix-inclusive; the
   all-zero nullmap record retains `nullmap_prefix` bytes (legacy
   records keep the vlen-0 convention). A nulled trailing field's bytes
   trim away; its bit survives.
8. Every failure mode fails the request (or the per-record bulk slot) —
   never silently stores a non-null zero.
9. Old `'BTRH'` index files refuse to open with a reindex hint (§2.4);
   no silent wrong results from key-shape mixing.
10. Bitmap-indexed `exists`/`not_exists` full-scan fallback is documented,
    not silent (doc task covers it).

---

## 5. Tasks (TDD; build `SKIP_TESTS=1 ./build.sh`, test
`./build/bin/shard-db-test run[-all]`; branch `feat/null-bitmap` off
`main`; work left **uncommitted** per AGENTS.md standing exception)

### Task 1 — failing regression tests for B1 and B2 (base-branch proof)

New `src/test/cases/test_null_bugs.c` (register in `build.sh`'s explicit C
test list next to the enum test). Daemon-isolated case: create object with
`label:varchar:32` + `status:enum(new,used)` + `n:long`.

- **B1:** insert `{"label": null, "n": 5}` → `get` → assert the response
  does **not** contain `"label":"null"`.
- **B2:** insert `{"n": 6}` → `get` → assert `status` is not `"new"`.

Steps: write test → `./build/bin/shard-db-test run test-null-bugs` on the
**base branch** → confirm both fail for the expected reasons (B1: stored
string comes back; B2: first enum value leaks) → paste both outputs into
the task log → keep the tests failing; fixes land in Tasks 3–4. After the
full plan executes, revert src/ (keep tests), rerun, and paste the
failure output again as the revert-proof.

### Task 2 — nullmap plumbing: helpers, header flag, derived sizes

1. `src/db/types.h` (near `TypedSchema`): the §2.1 helper block verbatim
   (`SREC_FLAG_NULLMAP`, `typed_nullmap_bytes`, `typed_field_base`,
   `typed_bitmap_of`, `typed_field_null`, `typed_set_null`,
   `typed_clear_null`), plus `int nullmap_prefix;` on `TypedSchema`.
   Add the `typed_record_view` declaration and implement its complete
   §2.1 reference body in config.c; it is a real implementation task,
   not only a helper test. Every untrusted value reader uses it.
2. `load_typed_schema` (config.c — the layout loop at `int offset = 0;`,
   config.c:1629): after the loop completes and before the schema is
   cached/returned, add:

```c
ts.nullmap_prefix = 1 + typed_nullmap_bytes(ts.nfields);
ts.total_size += ts.nullmap_prefix;
```

   (Locate the single completion point of the accumulation at execution;
   if the anchor text differs from expectation, write PLAN_NOTES and halt
   per CORE-PROCESS.)
3. Header write: at the staging/activation sites (slotcask.c:3966-3967
   `__atomic_store_n(&dst[18], 0, __ATOMIC_RELEASE); dst[19] = 0;` and
   3979 activation), thread the flag: the put path takes `uint8_t hdr2`
   (0 today) and writes `dst[19] = hdr2 & SREC_FLAG_NULLMAP;` at staging —
   i.e. replace `dst[19] = 0;` with `dst[19] = (uint8_t)(hdr2 &
   SREC_FLAG_NULLMAP);`. Encoders pass the flag from Task 3 onward.
4. Header validation: `seg_scan_varlen_struct_ok` gains:

```c
uint8_t hdr2 = rec[19];
if (hdr2 & ~SREC_FLAG_NULLMAP) return 0;
*out_hdr2 = hdr2;
```

   (new out-param; `_resync` threads it). Mirrors at the slotcask
   `rec[18]` parse points listed in §2.8.
5. Read APIs: `uint8_t hdr2` added to the out-structs (slotcask.h:423-427
   value-out; 550-579 resolved-location) and filled at the parse points;
   `SlotcaskOldRecord` gains `uint8_t hdr2`.
6. **Verification sub-step:** roundtrip test via the slotcask harness —
   `xxd` the segment bytes: byte 19 carries 0x01 only for nullmap records,
   0 for plain ones; `seg_scan_varlen_resync` finds both generations
   across a zero-filled reuse gap; unknown-bit record rejected.
7. Tests: `test_null_storage.c` — legacy + nullmap records coexisting in
   one stream; scan finds both; resync; GET returns hdr2; grown
   `max_value`/`slot_size` accepted by scan (`rec_size ≤ max_slot_size`
   still holds for legacy records since the cap only grew);
   **`typed_record_view` validation**: zero-length flagged value,
   `bm_nbytes` 0, `bm_nbytes` 33, prefix exceeding `vlen` — all rejected
   (finding: unchecked `value[0]` reads would underflow the §2.7
   region subtraction); legacy records always view-valid.

### Task 3 — encode: layout, explicit null, omitted→null (fixes B1)

1. **Quotedness-first null detection** (review fix #2). Shared helper in
   `src/db/util.c`/util.h:

```c
/* 1 iff the raw JSON value span is the unquoted literal null.
   Must be called BEFORE quote stripping — quoted "null" is a string. */
int json_span_is_null(const char *v, size_t len) {
    return len == 4 && v[0] == 'n' && v[1] == 'u' && v[2] == 'l' && v[3] == 'l';
}
```

2. `typed_encode_defaults` (config.c). After `memset(out, 0,
   ts->total_size);` add the head emission:

```c
out[0] = (uint8_t)typed_nullmap_bytes(ts->nfields);
```

   In the value-walk branch, replace the strip+encode block (anchor: the
   `const char *ev = vstart; size_t el = vlen;` at ~2914) with:

```c
const char *ev = vstart; size_t el = vlen;
int was_quoted = (el >= 2 && ev[0] == '"' && ev[el - 1] == '"');
if (was_quoted) { ev++; el -= 2; }
seen[i] = 1;   /* provided — including explicit "" — suppresses defaults */
if (!was_quoted && json_span_is_null(vstart, vlen)) {
    /* JSON null — store NULL: bit set, field bytes stay zero (the
       memset above already zeroed them). Wins over :default=.
       Skips enum membership validation (null is not a value miss). */
    typed_set_null(out + 1, ts->nullmap_prefix - 1, i);
    break;
}
if (el > 0) {
```

   The `seen[i] = 1` moved **above** the null/encode branches fixes
   explicit empty strings: `"label":""` strips to `el == 0`, which
   previously left the field unseen — the defaults pass would have
   stored NULL (no default) or applied a default. Now: bytes stay zero
   from the memset, the bit stays clear (an empty-string value, not
   NULL), and defaults are suppressed. The same placement applies in
   `typed_encode` (config.c:2413 area).

   and every encode destination in this function gains the prefix:
   `out + ts->nullmap_prefix + ts->fields[i].offset` (two client-value
   sites ~2951/2955, and the defaults pass site ~2984
   `encode_field(&ts->fields[i], dv, out + ...)`).
   Defaults pass (anchor: `if (ts->fields[i].default_kind == DK_NONE)
   continue;`):

```c
if (ts->fields[i].default_kind == DK_NONE) {
    /* Omitted, no default → NULL for nullmap records. */
    typed_set_null(out + 1, ts->nullmap_prefix - 1, i);
    continue;
}
```

3. `typed_encode` (config.c:2372): identical quotedness-first check at
   2413-2414, head emission after its memset, prefix on both
   `encode_field_len` destinations (2423, 2427).
4. `json_get_fields_null_aware` (util.c) replacing the three
   `json_get_fields_unescaped` call sites (storage.c:1260,
   query_bulk.c:3763, index.c:1405):

```c
/* json_get_fields + per-field null detection + varchar unescape.
   out_null[i] = 1 ⇔ field i's raw span was unquoted null (out[i] is
   then NULL and no unescape is attempted for it). Same -1 contract as
   json_get_fields_unescaped for malformed escapes. */
int json_get_fields_null_aware(const char *json, const char **keys,
                               int nkeys, const enum FieldType *field_types,
                               char **out, uint8_t *out_null);
```

5. `v2_update_new_from_old`: replace the `memcpy(out_value, old->value,
   old->vlen)` + tail-zero block with the §2.7 transformation verbatim
   (including `legacy_field_absent`), then keep the existing
   varchar-overflow rejection inside the `field_vals[i]` branch as
   marked.
6. Indexed-write paths that build the record before markers (bulk insert
   payload encode, query_bulk.c ~1541): no encode changes needed — they
   call `typed_encode_defaults` (updated above); verify by test.
7. **Auto-stamp paths (§2.9):** the `auto_create` preserve/stamp block
   (storage.c:1012 area — `memcpy(typed_buf + off, (uint8_t *)ac_old +
   off, w)` and `encode_field(&ts->fields[i], tbuf, typed_buf + off)`)
   becomes generation-aware: preserve-source reads use
   `typed_record_view(ac_old, ac_old_vlen, old_hdr2, ...)`, with offsets
   bounded by the returned field-region length (zero_field fallback); stamp destinations use
   `typed_buf + ts->nullmap_prefix + f->offset` plus
   `typed_clear_null(nbm, nbm_bytes, i)`. **Preserve transfers complete
   field state, not just bytes (finding: copying OLD bytes over a
   client-supplied NULL leaves the new bit set with nonzero bytes, and
   preserving an old NULL without setting the bit renders zeros):** if
   OLD (nullmap) has the bit set ⇒ `memset(dst, 0, f->size)` then set
   the new bit; if OLD is legacy and `legacy_field_absent` ⇒ the same
   zero-and-set operation. Otherwise clear the bit, zero the whole
   destination, then bounded-copy OLD bytes. NEW has already been encoded
   (including defaults or supplied values), so copying nothing is not
   sufficient to make it canonical NULL. Use §2.1's checked view, never
   unchecked `typed_field_base`, for the OLD source. The
   `auto_update` loop in `v2_update_new_from_old` gets the same
   prefixed destination + bit-clear. Bulk equivalents share the
   helpers. In the existing preserve branch, after the checked OLD view
   has filled old_base/old_region/old_bm/old_bm_bytes, use this complete
   replacement for its raw memcpy (off/w/i are the existing loop values):

```c
uint8_t *dst = typed_buf + ts->nullmap_prefix + off;
memset(dst, 0, w); /* NEW defaults/client values must be erased first */
int preserve_null = (old_hdr2 & SREC_FLAG_NULLMAP) &&
    typed_field_null(old_bm, old_bm_bytes, i);
if (!preserve_null) {
    size_t avail = old_region > off ? old_region - off : 0;
    size_t copyn = avail < w ? avail : w;
    if (copyn) memcpy(dst, old_base + off, copyn);
    if (!(old_hdr2 & SREC_FLAG_NULLMAP) &&
        legacy_field_absent(&ts->fields[i], dst)) preserve_null = 1;
}
if (preserve_null) {
    memset(dst, 0, w);
    typed_set_null(typed_buf + 1, ts->nullmap_prefix - 1, i);
} else {
    typed_clear_null(typed_buf + 1, ts->nullmap_prefix - 1, i);
}
```

   Branch on record existence, not complete OLD field width: trimmed or
   pre-add-field records use bounded zero fallback rather than leaving a
   newly generated timestamp in place. Tests: re-insert (upsert) of a record whose auto_create
   field is NULL stays NULL with all-zero stored field bytes, both when
   the client omits the field (default stamps NEW before preservation)
   and when it supplies a nonzero timestamp; re-insert over a real timestamp stays the
   timestamp even when the request supplies explicit null for it
   (preserve wins, bit cleared).
8. Tests (`test_null_encode.c`): the §1 table per type — explicit null →
   bit set + zero bytes; omitted-no-default → NULL; default + omitted →
   default bit clear; default + explicit null → NULL (wins); **explicit
   `""` on a varchar → bit clear, len 0, no default applied even when
   `:default=` is configured** (finding: seen[] must fire for empty
   spans); quoted
   `"null"` varchar → 4-char string (regression for review fix #2 —
   assert stored bytes, not just rendering); enum explicit null → OK;
   enum omitted → NULL (Task 1's B2 assertion goes green here); **single
   update from a non-NULL value to explicit null → field reads back
   null; same via bulk update** (the §2.7 out_null ordering regression);
   auto_create/auto_update objects: stamp after a null-carrying update
   lands at the prefixed offset and clears the bit (assert stored bytes
   via re-read); legacy
   conversion shape (per §4.1 semantics-level preservation): a legacy
   record rendering `{"n":5}` — with a `long` field `0` that the legacy
   binary omitted — re-updated on an unrelated field must, under the new
   decoder, render exactly `{"n":5,"l":null}`: the omitted key
   materializes as `null`. Assert the full expected response; do not
   interpret "rendering-preserving" as raw-JSON-shape equality;
   **negative-zero conversion fixtures**: legacy `double -0.0` /
   `float -0.0` records converted by an unrelated update must render
   `null` with all-zero stored bytes (canonical-NULL invariant §4.3 —
   the sign bit must not survive under a set bit); **trim invariant
   (§2.9)**: for nullmap records `nullmap_prefix ≤ typed_encode_trim_len
   ≤ full_len` — nonzero final field trims to `prefix + end`, all-NULL
   record trims to exactly `prefix` (bit survives), legacy all-zero
   record still trims to 0.

### Task 4 — decode: explicit null rendering, legacy passthrough

`typed_decode`, `typed_decode_stream`, `typed_get_field_str` gain
`uint8_t hdr2`; pointer-returning entry points use:

```c
errno = 0;
const uint8_t *fbase = NULL;
const uint8_t *bm = NULL;
int bm_bytes = 0;
size_t field_region_len = 0;
if (data_len < 0 ||
    typed_record_view(data, (size_t)data_len, hdr2, &fbase, &bm,
                      &bm_bytes, &field_region_len) != 0) {
    errno = EILSEQ;
    return NULL; /* typed_decode / typed_get_field_str error, not JSON null */
}
```

`typed_decode_stream` gains a status return (0 success, -1 error) and
validates before emitting anything:

```c
const uint8_t *fbase = NULL;
const uint8_t *bm = NULL;
int bm_bytes = 0;
size_t field_region_len = 0;
if (data_len < 0 ||
    typed_record_view(data, (size_t)data_len, hdr2, &fbase, &bm,
                      &bm_bytes, &field_region_len) != 0) {
    errno = EILSEQ;
    return -1;
}
```

Use the actual parameter name/length type at each entry (negative-length
check is only for signed lengths); return 0 after successful stream
emission. Update every stream caller to propagate -1. Pointer-returning
APIs set `errno = EILSEQ` on corruption; callers distinguish that error
from a legitimate NULL field and fail the request. Initialize/reset errno
at these API boundaries so stale errno cannot turn NULL into an error.
GET, scan, CSV, join, CAS, index apply, and replay must propagate corruption;
no caller substitutes `{}`, a skipped row, or a successful CAS miss.

`field_src` is formed only when the complete field fits in
`field_region_len`; otherwise use the existing zero-field fallback. In the field loop, before the per-type
decode — with the **same comma handling as normal emission** (the `if
(!first …)` prepend the regular branch performs; the snippet below shows
the shared shape):

```c
if (typed_field_null(bm, bm_bytes, i)) {
    /* defensive (§4.3): nonzero bytes under a NULL bit are encoder bugs */
    if (!first && pos + 1 < est) buf[pos++] = ',';
    SB_APPEND(buf, pos, est, "\"%s\":null", f->name);
    first = 0;
    continue;
}
```

(An earlier draft of this hunk omitted the comma prepend, producing
malformed JSON like `{"n":5"label":null}` after a value field or a
preceding NULL — the tests below parse complete responses, which catches
that class.)

**Generation-aware zero rendering (§2.9):** `decode_field_to_buf` gains a
`render_zeros` mode set for nullmap records — its skip-on-zero cases
(long/timestamp/double/float/date/datetime/datetimems/time/uuid/ipv4/
ipv6 zeroed bytes, varchar len 0) apply **only to legacy records**; on
nullmap records a bit-clear zero renders its value (`0`, `""`,
`"00:00:00"`, …). `typed_decode_stream` mirrors everything. Callers (§3
list: `print_record_*`, CSV, joins, scan wrappers) thread hdr2 from Task
2's plumbing; synthesized `SlotHeader` sites set `flag = 1 | (hdr2 << 8)`
and consumers normalize `& 0xFF` (standing §2.8 grep rule).

Tests (`test_null_decode.c`): nullmap record renders `"n":null`,
`"label":null` — **and every emitted response is parsed as complete JSON
(records with value→NULL→value field orders)**; real `long 0` / `time
"00:00:00"` / nil `uuid` on nullmap records render their values; a
nullmap record with an explicit empty varchar renders `""`, distinct
from its NULL sibling field; legacy fixture renders byte-identically to
a golden string; CSV empty cell for null; `find` and `join` now agree;
Task 1's B1 assertion goes green here. Malformed bitmap lengths and
unknown header bits are tested through GET, find/scan, CSV, and CAS;
requests fail as errors without row omission or synthesized empty JSON.

### Task 5 — criteria: null-aware matching

`match_typed` (query_plan.c:910) gains hdr2 through the scan-callback
chain; per record it computes fbase/bitmap once. Truth table for a
**NULL** field on a nullmap record: every value op ⇒ false, **`OP_EXISTS`
⇒ false, `OP_NOT_EXISTS` ⇒ true** (matches §2.4: `exists` = non-null).
For a non-NULL field: value ops evaluate normally, `OP_EXISTS` ⇒ true,
`OP_NOT_EXISTS` ⇒ false. **Non-nullmap (legacy) records keep today's
per-type semantics unchanged** — including `cmp_op_i64`/`cmp_op_f64`
returning `OP_EXISTS` = 1 for numerics — so the two generations are
deliberately different until rewritten. The helpers gain an early
"is-null ⇒ value-ops false / NOT_EXISTS true / EXISTS false" check
supplied by the caller rather than changing their signatures. The
absence gates inside the per-type matchers also become generation-aware
(§2.9): FT_TIME's `exists &&` comparison gates (query_plan.c:1074 area)
and FT_DATE's "empty date matches nothing except NOT_EXISTS" apply only
to legacy records — on nullmap records the bitmap decides existence and
value ops evaluate on raw bytes with no zero-existence gate (midnight
`eq` matches; zero date matches `eq 0`).

**Field-to-field operators:** at the quoted anchor
`/* Field-vs-field: compare LHS bytes against the sibling RHS field on`,
validate the record view and test BOTH schema indices before comparing
payload bytes. Insert in that branch after its `if (!cc->rhs_tf) return 0;`:

```c
int lhs_idx = (int)(cc->tf - fs->ts->fields);
int rhs_idx = (int)(cc->rhs_tf - fs->ts->fields);
if ((hdr2 & SREC_FLAG_NULLMAP) &&
    (typed_field_null(bm, bm_bytes, lhs_idx) ||
     typed_field_null(bm, bm_bytes, rhs_idx))) return 0;
```

`cc->tf` and `cc->rhs_tf` are resolved pointers into this same schema;
retain compile-time type validation. Both byte pointers use the validated
field-region base and bounded zero-field fallback. Apply to eq_field,
neq_field, lt_field, gt_field, lte_field, and gte_field; neither-NULL
comparisons retain their normal behavior, legacy records retain theirs.

Tests (`test_null_criteria.c`): on a nullmap record — `exists` returns
only the non-NULL rows and excludes the NULL row; `not_exists` returns
exactly the NULL row (both directions asserted, both ops); `eq 0` matches
a real 0, never NULL; `eq "00:00:00"` matches midnight (fails on base per
§1 — paste base output); `neq` excludes NULL; on a legacy record —
`exists` on a zeroed numeric still matches (today's semantics, pinned so
a future rewrite can't silently change legacy behavior); mixed
legacy+nullmap records behave per their own generation in one query;
**count/find equivalence** (§2.5): `count` with exists / not_exists /
bool-in-[true,false] criteria equals the corresponding find length on
mixed-generation indexed and unindexed fixtures (the removed
`cmd_count` shortcuts' regression); also `5, 7, NULL` with `neq 5`
returns count 1 and aggregate row-count 1, and negative text predicates
agree across count/find/aggregate and indexes. Every field-to-field op
is false for NULL/value, value/NULL, and NULL/NULL (including value zero);
non-NULL comparison controls pass. **CAS** (§2.6): conditional update
with `if exists` against a NULL field fails; `if not_exists` succeeds;
`if eq 0` against NULL fails and against real zero succeeds; same
against legacy records per existing CAS semantics; single, bulk, delete,
and dry-run outcomes agree. Raw null operands (scalar, bound, IN member)
are rejected before mutation; quoted "null" remains a value. Empty but
non-NULL varchar/composite existence succeeds.

### Task 6 — index: tier byte, NULL entries, format bump

1. Magic `'BTRH'` → `'BTRI'` (`BT_MAGIC` 0x42545248 → 0x42545249,
   btree.h:36; same site pattern as the 2026.05.5 `'BTRG'` → `'BTRH'`
   bump). The existing non-current-magic refusal (btree.h:31-32) stays as
   defense-in-depth (§2.4.3) — no new refusal code, only the new constant.
2. **2026.10.2 startup migration (§2.4.3).** Implement the migration step
   inside `shard_db_recover_before_stamp()` (embedded.c:646): immediately
   after the `rebuild_recovery(db_root) != 0` failure check and **before
   the `was_clean = clean_flag_exists(db_root)` early-return** — the clean
   path must still migrate (a cleanly-stopped root has `'BTRH'` indexes).
   Iterate all indexed objects using the existing enumeration and locking
   machinery, calling `reindex_object_durable_checked_ex` below for each
   object while holding its object write lock. Do not use `cmd_reindex`'s
   success code or either permissive checked wrapper as migration proof.
   Write the durable completion stamp only after all strict rebuilds and
   KFM2 replay succeed (§2.4.3); return non-zero on any failure (the
   function's existing `-1` contract fails startup). If the function's
   shape differs from this anchor, write
   PLAN_NOTES and halt. Tests (`test_null_migration.c`):
   (a) `'BTRH'` fixture object (fresh index file with magic bytes
   hand-flipped back) in a DB_ROOT → start daemon → migration reindexes
   before serving (first query on the field succeeds; rebuilt file is
   `'BTRI'`); (b) second start skips the migration (completion stamp
   present — assert via the stamp or a migration-invocation counter
   seam); (c) migration failure fails startup — simulate via the existing
   test-control seam (`test_control.h`) forcing the reindex hook to fail,
   assert the daemon exits non-zero and never serves; (d) an unindexed
   sibling object is unaffected throughout; (e) migration ordering — with
   a durable marker present (create one via an injected crash in an
   indexed bulk write, the existing crash-test seam), the next start
   reindexes first and the replay then completes against `'BTRI'` (final
   record set and index contents correct); (f) **per-shard crash-retry** —
   interrupt migration between shard publications, assert a mixed
   BTRH/BTRI live object and an absent completion stamp, restart, and
   assert every requested shard is rebuilt before serving with correct
   final index contents. Exercise abandoned temporary/spill cleanup via
   the actual `index_rebuild_temp_sweep()` path, not fictional migration
   directory swaps; (g) separately create **pre-existing object-rebuild
   staging** (`indexes.new`/`indexes.old`, using the object-rebuild crash
   seam), assert `rebuild_recovery()` resolves it before migration scans
   the object; (h) inject a post-rename publication-directory fsync failure
   in the btree builder (and bitmap builder when present), assert the
   strict entry point fails, no completion stamp is written, and startup
   never serves. Clear the injection and restart: rebuild all shards,
   replay, durably stamp, then serve. Also test stamp-file fsync and
   stamp-directory fsync failures: neither allows startup to serve;
   after a directory-fsync failure a renamed stamp may be visible, but
   all indexes were already durable before that rename. These are
   test-first cases: demonstrate that the permissive reindex wrapper
   returns success on unconfirmed publication while the new strict
   wrapper rejects it, and repeat with the strict gate reverted.

   **Strict durability entry point — complete changed hunks.** Root cause:
   `reindex_object_checked_impl` rejects only `INDEX_BUILD_FAILED`, so
   `INDEX_BUILD_DURABILITY_UNCONFIRMED` reaches `return 0`. Preserve the
   existing maintenance API contract and add a strict migration caller.
   The private implementation has exactly two current callers, the
   adjacent `reindex_object_checked` and `reindex_object_checked_ex`
   wrappers; both are updated below. The new strict wrapper has one new
   caller, the startup migration object loop. Add its declaration beside
   the existing checked declarations in `types.h`.

   Replace the private signature anchored by
   `static int reindex_object_checked_impl(const char *eff_root, const char *object,`:

   ```c
   static int reindex_object_checked_impl(const char *eff_root, const char *object,
                                          int composites_only, int *out_count,
                                          int *out_errno, int require_durable) {
   ```

   Replace its complete failure branch anchored by
   `if (build_result.status == INDEX_BUILD_FAILED) {` (within this function):

   ```c
   if (build_result.status == INDEX_BUILD_FAILED ||
       (require_durable &&
        (build_result.status != INDEX_BUILD_OK ||
         !build_result.all_requested_shards_published))) {
       int failure_errno = build_result.error_errno ? build_result.error_errno : EIO;
       free(descs);
       free(field_specs);
       LOG_ERROR(LOG_SUB_REINDEX,
                 "REINDEX %s/%s: index build failed or durability unconfirmed (errno=%d %s)",
                 eff_root, object, failure_errno, strerror(failure_errno));
       if (out_errno) *out_errno = failure_errno;
       return -1;
   }
   ```

   Replace the two complete adjacent wrappers anchored by
   `int reindex_object_checked(const char *eff_root, const char *object,`
   and append the strict wrapper:

   ```c
   int reindex_object_checked(const char *eff_root, const char *object,
                              int composites_only, int *out_count) {
       return reindex_object_checked_impl(eff_root, object, composites_only,
                                          out_count, NULL, 0);
   }

   int reindex_object_checked_ex(const char *eff_root, const char *object,
                                 int composites_only, int *out_count,
                                 int *out_errno) {
       return reindex_object_checked_impl(eff_root, object, composites_only,
                                          out_count, out_errno, 0);
   }

   int reindex_object_durable_checked_ex(const char *eff_root, const char *object,
                                         int composites_only, int *out_count,
                                         int *out_errno) {
       return reindex_object_checked_impl(eff_root, object, composites_only,
                                          out_count, out_errno, 1);
   }
   ```

   Insert this declaration beside the quoted anchor
   `int reindex_object_checked_ex(` in `types.h`:

   ```c
   int reindex_object_durable_checked_ex(const char *eff_root, const char *object,
                                         int composites_only, int *out_count,
                                         int *out_errno);
   ```
3. Index-key builders (write-path pre-commit diff, bulk `sw->idx_pairs`
   collection, rebuild scan): implement §2.11's checked BTRI encoder and
   migrate the capacity/caller inventory there. Diff keys must come from
   encoded OLD and NEW (with their own hdr2), never request or rendered
   JSON. In particular, replace `char *old_json = typed_decode(` and
   `build_index_key_from_json` inside `v2_insert_pre_commit`; fresh-insert
   prepare/apply also use the final encoded bytes after defaults/stamps.
   Delete, bulk, replay, and rebuild share this binary key derivation.
   Keep pre-marker preparation and post-marker index application ordering.
   Encoding/capacity/corruption failures are errors, never missing keys.
   Representation: per §2.4.1 — single field
   `[0x00][sortable bytes]` or `[0x01][canonical zeros]`; composite
   per-component tier bytes; NULL padding per type (fixed-width zeros,
   varchar `[0x01]` alone — raw content, no length prefix; enum width
   zeros; numeric sign-flip NOT applied to NULL padding). Delete paths
   compose the identical key. `len_*` consumers use `vlen − 1` and
   reject tier-1 entries. **Key budget (`BT_MAX_VAL_LEN`) is expanded,
   not shrunk (finding: a previously valid 512-byte key would become
   513 bytes and be silently unindexable): 512 → 544, covering the old
   payload plus one tier byte per component at the composite cap
   (index.c `subs[16]`), so every previously indexable value remains
   indexable; values already beyond the old limit keep today's
   never-stored behavior.** Oversize checks compare the tier-inclusive
   length against the new constant (index.c:83 guard and the
   insert-side counterpart).
4. Seek construction in `btree_idx_search/range/range_ex`: prepend `0x00`
   for value seeks (composite: interleave). **Tier-clamp every upper
   bound** (§2.4.2): enumerate the sites that today express "unbounded
   above" as walk-to-end or a max-byte sentinel — `gt`/`gte` upper
   bounds, open `between` high sides, and any range helper's max-sentinel
   composition — and clamp at `[0x01]`, **length 1**, exclusive (or the
   equality-fixed composite prefix followed by just the ranged component's
   tier byte). No NULL payload padding belongs in a range ceiling. Add the
   enumeration to the task log.
5. **`exists` leaf = tier-0 walk + per-candidate record verify** (the
   generation-correct query-existence predicate: nullmap bit /
   `legacy_field_query_exists` — NOT `legacy_field_absent`, which models
   rendering omission, §2.4.2); **`not_exists` = full scan** with the
   same predicate, in the `neq`/`not_in` bucket (§2.4.2 — pure tier-1
   enumeration is unsound for legacy generations). Bitmap-indexed
   fields: full scan (unchanged).
5b. **BTRI leaf-value consumers (§2.4.5):** add the
   status-bearing `decode_btri_idx_*` adapters (§2.4.5: explicit
   ERROR/NULL/PRESENT, PRESENT empty values have length 0) and migrate
   every btree-leaf caller — indexed group_by
   fast path, aggregate min/max readers, `decode_idx_to_buf`,
   `decode_index_key_to_double`, `decode_index_key_to_text` call sites
   — while bitmap-index callers keep the originals. Sweep:
   `grep -n "decode_idx_to_buf\|decode_index_key_to_"` must show no
   direct btree-leaf callers left. Test: indexed group_by and aggregate
   on fields containing NULL, empty varchar, literal "null", and real
   zeros — bucket keys identical to the per-record path, separate NULL
   buckets even for secondary group fields, zeros not misdecoded. Text
   min over "" and "z" is "", not "z"/null. Corrupt keys fail the request.
6. `len_*` leaf parses: skip the tier byte (`vlen − 1`), reject tier-1 —
   enumerate the leaf value-parse sites in btree.c used by len ops.
7. `btree_idx_walk_ordered`: no comparator change; ordered leaves now
   include NULL-tier entries (delivered last ASC).
8. Tests (`test_null_index.c`): indexed long — insert null → `eq 0`
   finds nothing, `not_exists` finds it (scan path); update null→5 →
   `eq 5` hits; 5→null → `eq 5` misses, `not_exists` hits;
   **`gt 0` / `gte` (open-ended upper bound) excludes the NULL row** —
   the numeric tier-clamp regression; additionally varchar fixtures with
   `""`, `"a"`, `"b"`, and NULL must give identical indexed/unindexed key
   sets and counts for `gt "a"` and `gte "a"`. Assert the raw candidate
   walk excludes the one-byte `[0x01]` key so a record post-filter cannot
   hide a broken clamp. Cover a later composite ranged component where
   that plan is supported, with its preceding component fixed by equality;
   add-index over records containing NULLs →
   tier-1 entries exist; ordered walk returns values-then-NULL;
   multi-field-order-by cursor paginates across the NULL boundary with
   the tier byte in the resume token (resume neither repeats nor skips
   the NULL row); trigram `.tg` on a field with NULLs → rebuilt cleanly,
   substring queries still serve; **mixed-generation equivalence** — a
   fixture with legacy records (empty varchar, midnight `time`, zero
   `long`/`date`) beside nullmap records (NULLs and real zeros) queried
   with the index and with the index removed/unindexed twin field must
   return identical key sets for `exists`, `not_exists`, `eq 0`,
   `eq "00:00:00"` (finding: legacy zero-values are simultaneously value
   and absent under legacy scan semantics — the verify step is what
   keeps indexed and unindexed results identical); **512-byte boundary**
   — a 512-byte varchar value indexed pre-upgrade survives the `'BTRI'`
   migration and remains findable by `eq`, and a composite key at the
   old boundary stays indexable under the expanded budget. Capacity
   regressions: UUID/IPv6 16-byte payloads need 17 output bytes; a
   too-small buffer returns error before modifying its canaries; composite
   final-component capacity is checked before its write. Legacy indexed
   long zero → update to 5 removes the old zero entry; generated defaults,
   auto_create preservation, and auto_update keys equal a fresh reindex
   and marker replay of the same encoded bytes.

### Task 7 — bulk + replay + maintenance + schema rebuilds

Bulk insert: no encode changes (Task 3); add a mixed null/non-null bulk
case asserting per-record error isolation is untouched. Bulk update merge
(query_bulk.c:3763): migrate to `json_get_fields_null_aware` + the §2.7
transformation (shared helper so single and bulk update cannot drift).
KFM2 replay: extend an existing marker-replay case — replay a window
containing nullmap records; assert records re-apply byte-identically and
index re-derivation emits tier-1 entries for NULL fields. Maintenance
(vacuum/reindex/recount walks): golden-output assertions plus the §2.10
rebuild work — `v2_rebuild_walk_cb` (query_find.c:831) NULL-bit remap
through `new_to_old`, `typed_record_view` on OLD, and the
manually-constructed-schema sweep (grep `TypedSchema` construction in
query_find.c/query_schema.c; every site sets `nullmap_prefix` and
prefix-inclusive `total_size`). Tests: field removal across a
bitmap-width boundary (9 fields → 8; NULL on old field 8 lands on new
field 7's bit), edit-field resize of a NULL field (stays NULL),
layout-preserving rebuild preserves hdr2 verbatim.

### Task 8 — aggregates, group_by, cursor tokens

`agg_scan_cb` + accumulators (query_aggregate.c): NULL skips
sum/avg/min/max; all-NULL avg → NULL (renders `null`); group_by NULL =
own group with `null` key. Multi-field cursor (2026-09-18 format): tier
byte per ordered field, parser fail-closed on unknown versions (§2.5).

Tests (`test_null_agg_order.c`): aggregates with NULLs (SQL semantics),
group-by with NULL, "", and "null" as separate buckets (also in a
secondary grouping component), indexed-versus-scan parity, min("", "z")
= "", negative-predicate count/sum/avg parity, order_by NULL placement
ASC + DESC, cursor pagination across the NULL boundary.

### Task 9 — docs

`docs/concepts/typed-records.md` (NULL section: representation, §2.2-2.5
semantics, legacy rule, conversion rule), `AGENTS.md` (storage-model
bullet + typed-record note; update the compatibility paragraph: 2026.10.2
keeps the **2026.09.2 minimum supported source release** and adds the
startup reindex migration), `docs/query-protocol/find.md` (exists/
not_exists = null tests; NULL ordering), `aggregate.md` (NULL-skipping),
`docs/reference/changelog.md` (2026.10.2 entry: BTRI bump, automatic
startup migration with fail-closed behavior, minimum version unchanged),
`docs/reference/limits.md` (per-record overhead `1 + ceil(nfields/8)`).

### Task 10 — gates

- `SKIP_TESTS=1 ./build.sh` clean; full `run-all` green.
- **ASan+UBSan** (`BUILD_MODE=asan`): 3 consecutive fresh full-suite runs,
  bare `run-all`, no env options, no `halt_on_error=0`.
- **TSan** (`BUILD_MODE=tsan`): 3 consecutive fresh runs,
  `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" run-all
  --jobs 2`, no suppressions.
- Mandatory: the diff touches record encoders/decoders, kfcache-adjacent
  record paths, scan callbacks, and index apply.

---

## 6. Risks / open verification items

1. `slotcask_bucket_for` under a grown max — Task 2 verification sub-step.
2. Grown `max_slot_size` in resync accepts legacy records (cap is `≤`) —
   Task 2 test.
3. Synthesized-`SlotHeader` flag normalization — standing §2.8 grep rule;
   Task 4 includes the zero-remaining-sites check.
4. Replay determinism with tier-1 keys — Task 7 test.
5. Legacy-zero under new fields after add-field reads as zero-value, not
   NULL (documents today's add-field behavior; `:not_null`/backfill plan
   may tighten).
6. `shard-cli` views.c JSON mini-parser must tolerate `"field":null` —
   tolerant-parse test added in Task 4 if its parser skips unknown value
   kinds.
7. Performance: prefix bytes per record + one branch per field + one tier
   byte per index key — noise against per-commit-window fsyncs; benches
   remain human-run per AGENTS.md.
8. Operational: the `'BTRI'` bump is handled by the **2026.10.2 startup
   migration** (§2.4.3) — reindex-before-serving, fail startup on
   reindex failure or unconfirmed publication durability, completion
   durably stamped only after every requested shard and replay succeed, minimum
   source version stays 2026.09.2. The per-request magic refusal remains
   only as defense-in-depth; the KFM2 marker hazard (durable intent
   precedes index apply) is why it must never be the primary path.
   Called out in changelog/upgrade docs (Task 9).
9. Tier-clamp completeness (§2.4.2): the task-log enumeration of
   open-ended-upper-bound sites must be reviewed in the review pass — a
   missed site returns NULL rows for value ranges (silent wrong
   results). Task 6 covers numeric `gt 0`/`gte`, varchar `gt`/`gte`
   (including the one-byte NULL key at the exclusive ceiling), and
   supported later-component composite ranges; the enumeration covers
   the rest. Candidate-walk assertions must expose a broken ceiling even
   when per-record verification would mask it.
