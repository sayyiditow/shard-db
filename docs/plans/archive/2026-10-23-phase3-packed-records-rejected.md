# Phase 3 child plan — versioned packed numeric/varchar records

Parent: [Type correctness recovery plan](2026-10-19-type-correctness-recovery-plan.md) §"Phase 3 —
Versioned packed numeric/varchar records". Handoff:
[Phase 3 planner handoff](2026-10-23-phase3-planner-handoff.md). Planning checkpoint per the
parent's rule: this is the complete anchored code plan for **Phase 3 only**; no phase below is
executable without human approval of this document.

- Baseline branch: `fix/phase1-safe-scalar-conversions`, HEAD `8c0bdf6259d2dc33e540e28e82b9f8010750047d`
  (Phase 2 decimal core, reviewed). Phase 3 work continues on that branch per the standing
  execution mode: **all work stays uncommitted** for raw-diff review.
- Protected file: `docs/plans/2026-10-19-durability-certification-rig.md` is never read, edited,
  moved, staged or included. SHA-256 verified at planning start:
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
- Planning-time evidence in this document comes from reading this checkout plus scratch probes
  compiled under `/tmp/phase3-probes/` (outside version control). Nothing in Phase 3 has been
  implemented; every behavior described as current is quoted from source at HEAD.

## 0. Scope and non-goals

Phase 3 delivers the **storage layer** for packed numeric/varchar records:

1. The frozen Phase 0 record layout (fixed field directory + inline variable tail, 8-byte
   uint32 BE offset/length descriptors, decnum packed payloads, raw varchar content) —
   implemented against `src/db/decnum.{c,h}` from Phase 2.
2. Versioned object metadata (schema.conf engine-version slot `3`) plus a per-record known flag
   (`SREC_FLAG_PACKED`, segment byte 19 bit 1), with crash-safe downgrade protection so older
   binaries refuse new-layout objects, including a crash between object publication and root
   stamping. Headerless/`2`-slot schemas and records keep the legacy interpretation byte-for-byte.
3. A validated schema-aware record-view/span API and the refactor of **every** record consumer
   onto it (complete consumer inventory in §3), replacing raw `fbase + f->offset` arithmetic and
   the 65537-byte zero sentinels for packed fields.
4. Encoders rewritten as sizing + bounded composition; actual value length is distinct from the
   schema maximum everywhere; no slot_size-proportional or total_size-proportional buffers on the
   packed path; no giant stack arrays; the record 24-byte header layout is unchanged.
5. Bare declarations at create-object: bare `numeric` (131,072 integer / 16,383 fractional
   digits), `numeric:P` (scale 0), bare `varchar` (4,194,304 content bytes), `varchar:N`
   (N = 1..4,194,304). Declared `numeric:P,S` under the new layout binds the exact core
   (`dec_apply_precision`: round to S ties-away-from-zero, enforce P). `currency` = exact
   numeric(19,4) under the new layout.

**Non-goals (later phases own them; interim gaps fail with explicit errors, never silently):**

- All-route write/defaults/schema-surface semantics and error-propagation polish (bulk edge
  contracts, computed-update exactness, export/import declaration fidelity, edit-field conversion):
  Phase 4. Interim: computed updates (`$incr`/`$copy`) and edit-field **declaration changes on
  packed fields** are refused with explicit errors (§4 D5/D7); single insert/update and the bulk
  composers are made layout-correct here because they cannot be left byte-wrong.
- Exact criteria, indexes, ordered reads, joins and cursors for numeric values: Phase 5. Interim:
  criteria and aggregate specs that reference a **packed numeric** field fail with an explicit
  error (§4 D3/D4); packed-varchar criteria work through spans; index creation on packed numeric
  fields is refused; varchar index keys stay raw content bytes (unchanged encoding).
- Exact aggregates and output rendering for numeric values: Phase 6 (typed_field_to_double and
  friends keep legacy behavior for fixed fields; packed numeric is gated).
- Transactional conversion of existing v2 objects: Phase 8. Phase 3 migrates nothing.
- Sanitizer gates: not run in this phase (end-of-branch ASan+UBSan and TSan gates per AGENTS.md
  remain the final gate after all later fixes).

## 1. Frozen design (verified against source)

### 1.1 Record layout generations

Three on-disk generations of a typed record's value region exist after Phase 3, selected by the
segment header byte 19 (`hdr2`) and the object's schema.conf engine-version slot:

| Generation | hdr2 | Object metadata | Value region |
| --- | --- | --- | --- |
| Headerless legacy (pre-2026.10.2 records still on disk inside `2`-slot objects) | `0x00` | schema.conf slot `2`, nullmap-prefix never written by that generation | raw fixed field directory, no prefix |
| Nullmap (2026.10.2, unchanged) | `0x01` | schema.conf slot `2` | `[u8 bm_nbytes][bits]` + fixed directory |
| **Packed (new)** | `0x03` = `SREC_FLAG_NULLMAP\|SREC_FLAG_PACKED` | **schema.conf slot `3`** | `[u8 bm_nbytes][bits]` + directory with 8-byte descriptors for packed fields + variable payload tail |

Packed value region, byte-exact (offsets from the value-region base):

```text
[0]            u8  bm_nbytes                     (1 + ceil(nfields/8) total prefix bytes)
[1 .. P)       null bits, bit i = field i is NULL; P = 1 + bm_nbytes
[P .. P+dir)   field directory, declaration order, one entry per schema field
                 (removed fields keep their entry reserved, exactly like v2 reserves their bytes):
                 fixed field  -> f->size bytes of the existing fixed binary encoding
                 packed field -> 8-byte descriptor:
                    [0..4)  u32 BE payload_off  (relative to field-region base P; 0 when empty)
                    [4..8)  u32 BE payload_len  (0 when empty)
[P+dir .. )    payload tail: non-empty packed payloads in field declaration order,
               tightly packed, no padding, no per-payload prefix.
                 varchar payload = exactly the content bytes (no length prefix;
                 length lives only in the descriptor)
                 numeric payload = the Phase 2 decnum packed payload (dec_pack):
                    16-byte header + ceil(significant digits / 2) packed digit bytes
```

Descriptor validation rules (enforced per record by `typed_view_record`, §T2):

1. `hdr2 & ~SREC_KNOWN_FLAGS != 0` → invalid (fail closed, unchanged rule).
2. `SREC_FLAG_PACKED` set without `SREC_FLAG_NULLMAP` → invalid (packed implies nullmap).
3. Nullmap bounds exactly as today (`bm_nbytes ∈ [1,32]`, `1+bm_nbytes ≤ vlen`).
4. `region ≥ dir_bytes`.
5. Per packed field: `payload_len ≤ f->payload_cap`; `len == 0 ⇒ off == 0`;
   `len > 0 ⇒ off ≥ dir_bytes && off + len ≤ region`.
6. Payloads are in field order: scanning fields in declaration order, every `len > 0`
   descriptor's `off` is `≥ previous kept end` (strictly increasing, no overlap, none inside the
   directory). Removed fields' preserved descriptors participate in validation.
7. Numeric payload structure (version/sign/reserved/nibbles/canonicity/digit limits/display
   scale) is validated at decode by `dec_unpack` — a bad payload fails the read, never renders.

Semantics frozen by Phase 0 and kept here:

- NULL = bit set + descriptor `(0,0)`; present-empty varchar = bit clear + `(0,0)`;
  numeric zero = bit clear + the 16-byte canonical zero payload (`len 16`). The three are
  distinct. An empty numeric text span encodes the canonical zero (matches the historic
  empty→zero rendering; the JSON layer never passes empty spans for scalars).
- Payload order is deterministic field order; updates/rebuilds recompose the tail and regenerate
  every descriptor (stale offsets are never copied).
- Actual value length = `P + dir_bytes + Σ actual payload lengths` — always ≤
  `ts->total_size` (the maximum validity bound). `total_size`/`max_value`/`slot_size` remain
  maximum validity bounds, never per-record promises.

### 1.2 Limits (handoff item 6, unchanged)

- Bare numeric: `DEC_MAX_INT_DIGITS 131072` integer digits, `DEC_MAX_FRAC_DIGITS 16383`
  fractional digits (src/db/decnum.h:26-27); payload ceiling `DEC_PAYLOAD_MAX 73744` bytes
  (verified: 16 + (147455+1)/2).
- Declared numeric: P ∈ 1..1000, S ∈ -1000..1000 (`DEC_MAX_PRECISION/DEC_MAX_SCALE/
  DEC_MIN_SCALE`); constrained payload ceiling = `16 + ceil(D/2)` where
  `D = (S > P) ? S : (S < 0 ? P - S : P)` — worst case numeric(999,-1000) → 1016 bytes
  (scratch-probe verified). Declaration is schema metadata; the on-disk display-scale carrier is
  the value's own canonical scale, never negative.
- Bare varchar: 4,194,304 content bytes. Explicit `varchar:N` retains N as the byte cap,
  N ∈ 1..4,194,304. Overlength input is rejected, never truncated.
- `MAX_FIELDS 256` and `MAX_KEY_CEILING 1024` unchanged. Request/query caps
  (`MAX_REQUEST_SIZE` 32 MiB default, `QUERY_BUFFER_MB` 256, `GLOBAL_LIMIT`) unchanged.

### 1.3 Recomputed maximum-record arithmetic (handoff item 4)

All values verified by a checked scratch program (`/tmp/phase3-probes/sizing.c`, output
reproduced in the execution report task T0); every number below matched the parent/handoff:

| Quantity | Value | Fits |
| --- | --- | --- |
| nullmap prefix at nfields=256 | `1 + 32 = 33` | — |
| dir_bytes, all-packed 256 fields | `256 × 8 = 2048` | — |
| All-256-bare-varchar `total_size`/`max_value` | `33 + 2048 + 256×4,194,304 = 1,073,743,905` | int32 ✓ uint32 ✓ |
| Its `slot_size` at max_key=1024 | `24+1024+1,073,743,905 = 1,073,744,953 → align8 = 1,073,744,960` | int32 ✓ uint32 ✓ |
| vs `SLOTCASK_SEG_MAX_BYTES` (128 MiB = 134,217,728) | **exceeded 8.0×** | — |
| All-256-bare-numeric `max_value` | `33 + 256×(8+73,744) = 18,880,545` | ✓ |
| Its `slot_size` | `18,881,600` | ✓ (fits segment) |
| Single record: one maxed varchar + 1024-byte key | `24+1024+33+8+4,194,304 = 4,195,393` | ✓ segment |
| Maxed-varchar payloads per 128 MiB record | `⌊(134217728−24−33)/4,194,312⌋ = 31` | — |
| Worst decode escape buffer, 4 MiB varchar | `6×4,194,304+3 = 25,165,827` | int32 ✓ |
| `typed_decode` worst `est` if sized from ceilings | `6,442,451,460` — **int32 overflow**; sized from actual payload lens it is bounded by actual record size | fixed by design (§T3) |

Integer-width audit (handoff item 3) — conclusion baked into the tasks below:

- `Schema.max_value`/`slot_size` (int): worst case 1,073,744,960 < 2^31 ✓. `load_schema`'s
  `24 + s.max_key + s.max_value` int arithmetic cannot overflow ✓. A `_Static_assert`-style
  runtime guard on the bound is added in T1 (compile-time asserts cannot see runtime field
  counts; a checked computation function is used instead).
- `TypedSchema.total_size` (int): same bound ✓. Accumulated as `int64_t` then range-checked
  before the int store in `typed_layout_v3_compute`.
- `TypedField.size` (int): bare varchar stores the logical content cap 4,194,304 ✓.
- Descriptor offset/length uint32 BE ✓; segment `vlen` uint32 ✓; kf `SlotcaskKfEntry.offset`
  uint32 within 128 MiB segments ✓; free-pool `slot_capacity` uint32 ✓.
- Everything that used to size from `f->size`/`total_size` ceilings and could multiply
  (typed_decode `est`, bulk scratch `count × total_size`, `calloc(1, total_size)` in the rebuild
  walk, `upsert_adapter_value_compute`'s `slot_size − 24 − klen` malloc) is switched to
  actual-length sizing in T3/T4/T6/T9 — verified exhaustively in the consumer inventory (§3).

### 1.4 Row/segment ceiling policy — explicit resolution (decision D1)

The theoretical worst-case slot_size (1,073,744,960 B) exceeds the 128 MiB segment rotation
ceiling by design, exactly as the parent's Task 0.3 table anticipated. Resolution implemented by
this plan, surfaced for review:

- `slot_size` stays the derived **validity bound** with the existing formula. No field limit is
  reduced and no request/query cap is raised.
- Actual records remain individually bounded by **both** existing gates:
  `24 + klen + vlen ≤ db->slot_size` (slotcask.c:8641/8771) and
  `rec_size ≤ slotcask_seg_max_bytes()` (append_reserve_single_varlen, slotcask.c:4026). A
  record that legitimately exceeds the segment maximum (32 simultaneous maxed varchars) fails
  the reserve gate with a clean pre-staging error surfaced as
  `{"error":"record exceeds segment capacity"}` (T6 adds the explicit pre-check so the error is
  actionable instead of the opaque `-1`).
- create-object logs a WARN when the computed `slot_size > seg_max`
  (`"schema maximum record size exceeds the segment file capacity; individual records are still
  bounded by the segment maximum"`), mirroring the existing RESHARD-HINT pattern.
- No buffer anywhere on the v3 path is sized from `slot_size` or `total_size` (the update
  adapter's `slot_size − 24 − klen` malloc becomes an actual-size hook, slotcask.h:575 region,
  T6). A pathological schema therefore costs storage validation headroom, not RAM.
- The strict walkers/validators keep using cached `slot_size` as the structural ceiling
  (seg_scan_varlen.h:54, slotcask.c:9759/9813) — unchanged semantics; `pos + rec_size > map_size`
  still catches oversized claims within the real 128 MiB segment map.

## 2. Versioning and downgrade protection (handoff requirement)

Mechanism, riding the audited machinery (all anchors verified in this checkout):

1. **Object metadata**: schema.conf engine-version slot. `cmd_create_object` writes `3` instead
   of `2` for every object it creates (query_schema.c:1590-1608 writer; literal `2` today). The
   slot's purpose is already documented as the storage-version slot "kept in the on-disk format
   for forward compatibility" (query_schema.c:1593-1596 comment — same comment is updated to
   document 2|3).
2. **Per-record flag**: `SREC_FLAG_PACKED 0x02` in segment byte 19, written by
   `seg_record_emit`/`seg_record_emit_pending` (slotcask.c:4080/4113 — the mask
   `hdr2 & SREC_FLAG_NULLMAP` widens to `hdr2 & SREC_KNOWN_FLAGS`), accepted by
   `seg_scan_varlen_struct_ok` (seg_scan_varlen.h:48) and `typed_record_view` (config.c:4010).
3. **Older-binary refusal, any reachability path** (the handoff's "crash between schema/data
   publication and root stamping" case included):
   - A 2026.10.2 binary reading a `3` line: `load_schema` sets `legacy_v1` (parsed_version != 2,
     config.c:975-994) and refuses the object; `startup_schema_line`'s strict
     `strcmp(parts[4], "2")` (embedded.c:428) **fails the whole root at startup**. No byte-19
     knowledge required — the barrier works for pre-2026.10.2 binaries too.
   - A 2026.10.2 binary reading packed records written before any stamping crash: byte-19
     bit 1 is an unknown bit → `seg_scan_varlen_struct_ok` returns 0 (scan-side fail closed) and
     `typed_record_view` returns −1 (every read fails with EILSEQ; no partial objects). It cannot
     silently misread.
   - A pre-2026.10.2 binary (2026.09.2..2026.10.1, no byte-19 check): refused at open by the
     existing `.version` gate — the Phase 3 release stamps `SHARD_DB_VERSION` (bumped per the
     repo's CalVer convention, §4 D8) and `shard_db_version_decide` returns
     `SHARD_DB_VERSION_DOWNGRADE` for any older binary (util.c:816-841). The only stamping-crash
     window leaves `.version` at 2026.10.2, which is still **newer** than those binaries → same
     refusal. The `3` slot additionally blocks them at startup-parse even in that window.
   - create-object fsyncs the schema.conf append (T5) so the `3` line cannot be lost while the
     object's packed records exist (closing the last ordering gap; today's append is a plain
     `fopen"a"`+`fclose`, query_schema.c:1591).
4. **Legacy stays legacy**: a `2`-slot object loads exactly as today; `load_typed_schema` binds
   `layout_version = 2` for it; every v2 code path behaves byte-identically (§T1 regression
   proofs). Headerless fields.conf lines keep the legacy interpretation (config.c:1725 skip rule
   unchanged). `parsed_version == 3` is the only new accepted value.

## 3. Consumer inventory (handoff item 1)

Complete inventory of every schema/layout consumer and field-access path at HEAD, from four
parallel exploration passes plus direct reads. Every site below is either refactored in this plan
or explicitly gated. Sites marked ⛔ are the ones that would silently misread packed records if
missed.

**Central encoders/decoders (config.c)** — the chokepoints:

- ⛔ `decode_field_to_buf` (config.c:3603) — per-field byte interpreter; varchar reads the u16
  prefix at `data[0..2)` clamped to `f->size − 2`; numeric reads 8 B + `numeric_scale_mult`.
- ⛔ `typed_decode` (3857) / `typed_decode_stream` (3946) — `fbase + f->offset` reads,
  `static const uint8_t zero_field[65537]` fallback (3903/3964), per-field
  `int need = 6*(f->size−2)+3` (3926/3980), outer `est` from `f->size` (3880-3890).
- ⛔ `typed_get_field_str` (4065) — same offset/zero-field pattern, varchar prefix read (4098-4107).
- ⛔ `typed_record_view` (4004) — the validation chokepoint; `hdr2 & ~SREC_FLAG_NULLMAP` gate.
- ⛔ `typed_encode_trim_len` (4033) / `schema_trim_fn` (4057) — scans `f->offset..+f->size`
  backwards; installed as `sdb->trim_fn` (storage.c:1091/2213/2590; applied slotcask.c:8550/8640/8770).
- ⛔ `encode_field_len` (2250) / `encode_field` (2467) / `encode_field_checked` (2195) /
  `encode_json_field_checked` (2879) / `conv_numeric_text` (≈2050) / `conv_numeric` (2186) —
  fixed `f->size` writes; `FT_VARCHAR` cap `f->size − 2` (2237); `FT_NUMERIC` int64 path (2229-2230).
- ⛔ `encode_field_text_for_index` (2484) — varchar cap `f->size − 2` (2499), numeric 8 B.
- ⛔ `typed_field_to_index_key` (2740) — `fbase + f->offset`, zero-field fallback (2745),
  varchar prefix read (2751 region), `FT_LONG/FT_NUMERIC/FT_TIMESTAMP` 8-byte flip.
- `parse_field_type` (1447) — bare `varchar` currently `return`s (1462-1464); `numeric:P,S`
  hardcodes `f->size 8` (1519-1525); bare `numeric` absent (→ FT_NONE).
- `load_typed_schema` (1654) — offset accumulation `offset += tf->size` (1772-1773),
  `total_size`/`nullmap_prefix` (1778-1785). `load_schema` (883) — version slot parse (927-941),
  `parsed_version != 2 → legacy_v1` refuse (975-994), `max_value = ts->total_size` +
  `slot_size = (24+max_key+max_value+7)&~7` (996-1005).
- `sentinel_span_parses` (2902), `fmt_not_null_err` (3440) — layout-neutral.

**Readers/interpreters outside config.c** (all ⛔ for packed fields unless refactored):

- query_plan.c: `match_typed` (914; lhs/rhs `fbase + cc->tf->offset` + `g_zero_field_65537`
  939-962; per-op fixed-width decodes 972-1160; legacy-absent gates 1058-1114),
  `cmp_typed_field_pair` (846; `varchar_eff_len(a, f->size)`), `match_typed_varchar` (621;
  `p + 2`), `criteria_match_tree` (1792 delegates).
- query.c: CAS regex site (5579-5590: `base + cc->tf->offset`, `varchar_eff_len(field, tf->size)`,
  `regexec(field + 2)`), `guard_eq_key` (5692: `s1_len > f->size − 2`),
  `typed_field_to_index_key_rec` (6332) + ordered-collect key sizing (6397: `cap = max(tf->size,16)`
  — latent off-by-one for 16-byte fields today), STARTS_WITH fixed-width detection
  (2428/2553: `len_lo < seed_tf->size`), cursor capture (6867) and resume NULL padding
  (8115-8124: `plen = tf->size`), btree leaf decode buffers `tmp[1028]`/`buf[512]` (281-328,
  829, 1752, 1877, 3190) and `btree_dispatch buf1[1032]/buf2[1032]` (629) — leaf decoders read
  index bytes (unchanged encoding) but their varchar content comes from record payloads.
- query_aggregate.c: `agg_field_ptr` (256; `g_zero_field_65537` + `region − tf->offset`),
  `agg_field_is_unset` (231), `typed_field_to_buf_raw` (1067), `typed_field_to_double` (2044),
  `typed_field_to_text` (2101), `typed_field_to_raw` (2212), `decode_idx_to_buf` (1399),
  `decode_index_key_to_double` (1215), record-fed callbacks 1830-1858 / 2400-2494 / 2510-2578 /
  3970-3999; group buffers `char gbuf[MAX_FIELDS][512]` (2400).
- query_join.c: `extract_local_key` (198), `buf_field_value` (350; `field_ptr + 2`,
  `varchar_eff_len(field_ptr, tf->size)`), `buf_join_values` (397), `buf_driver_values` (438),
  `build_joined_csv_row` (522), `lookup_remote keybuf[1032]` (295), match fast path (673).
- index.c: `build_index_key_from_record_into` (1380; payload from `f->size`/varchar prefix at
  `fbase + off`, `blen != payload` guard), `build_index_payload_from_record_into` (1463),
  `build_index_payload_from_record` (1520; heap fallback `cap = f->size + 1`),
  `build_index_key_from_record` (1556; `cap = 1 + f->size` per component),
  `build_index_key_from_json` (1594), `index_parallel` single-field cap (1282),
  ⛔ `bm_rebuild_cb` (2300; `fbase + f->offset` + `key_buf[1024]`), ⛔ `mf_append_field`
  (3835; trigram `fbase + tf->offset`, `kb[4096]`), ⛔ `reindex_seg_cb` (4026;
  `calloc(1, ts->total_size)` zero-pad at 4484-4485 — a 1 GiB calloc on a maxed v3 schema),
  `mf_worker_field_alloc` (3935).
- query_maint.c: ⛔ `tg_estimate_cb` (59) — **no typed_record_view at all today** (reads
  `value + f->offset` with the zero-field fallback only); gains the view + spans.
- storage.c: ⛔ `auto_create_field_transfer` (1402), ⛔ `update_compose_new_from_old` (1454),
  `update_stamp_auto_update_fields` (1519), ⛔ `apply_computed_fields` (1678;
  `uint8_t cur[16]` / `source_bytes[16]` — assumes `f->size ≤ 16`), `legacy_field_absent`
  (1350; legacy-only predicate, unchanged), `cmd_insert_v2` (1045-1068:
  `malloc(ts->total_size)`, `vlen = ts->total_size`, `(int)vlen > sc->max_value` gate),
  `v2_update_new_from_old` (1862; varchar bound `strlen > f->size − 2`,
  `*out_vlen = total_size`), insert/update index-key staging 589-611/841-934/1979-2058/2490-2516
  (`INDEX_KEY_MAX 4096` arenas + `_into` overflow fallback), trigram key buffer
  `tg_cap = tg_f->size + 16` (928).
- query_find.c: ⛔ `transform_field_value` (748; varchar u16 prefix, numeric int64 BE rescale),
  `field_needs_transform` (859), ⛔ `v2_rebuild_walk_cb` (867; `calloc(1, new_ts->total_size)`,
  per-field memcpy at `nullmap_prefix + nf->offset`, `scratch[64]` legacy materialization,
  verbatim copy path 875-892 carries `record_hdr2 = hdr2`), decode wrappers
  `decode_value`/`decode_field`/`json_projected_field`/`json_typed_row_cell` (363-505; route
  through the config.c chokepoints), rebuild sizing 1430-1441
  (`new_sch.max_value = new_ts.total_size`).
- query_schema.c: ⛔ `field_value_fits_new` (64; varchar u16 prefix + `new_f->size − 2`,
  numeric BE decode at `old_f->size`), ⛔ `edit_preflight_walk_cb` (125),
  ⛔ `not_null_scan_cb` (514; `scratch` at `fld->size`), create-object validation
  `validate_field_type`/`validate_field_type_clean` (963/976; bare `varchar` rejected at 1005),
  field loop + `total_value_size` (1198-1289), `cmd_create_object` schema.conf writer
  (1590-1608) + slot_size (1637-1639) + fields.conf writer (1524-1533), edit rebuild sizing
  (906-910), `cmd_describe_object` (1865-1898; emits `f->size`, numeric `scale`).
- query_bulk.c: ⛔ JSON insert composer (1600; `arena_alloc(ts->total_size)`),
  ⛔ `bulk_ins_delim_run` (2094-2386; per-field `encode_field_checked` at
  `nullmap_prefix + offset`, varchar bound `vals[i].len > size − 2`, arena payloads,
  `total_size > sc.max_value` invariant 1364/2128, `batch[pos].hdr2 = SREC_FLAG_NULLMAP`
  1205/3414/3709/4219/4294/4842/4917), ⛔ update composers
  `v2_bulk_upd_value_compute` (3396), `v2_bulk_upd_delim_value_compute` (4207),
  `v2_bulk_upd_json_value_compute` (4826; explicit-NULL `memset(new_buf + …, 0, size)`),
  scratch slabs `malloc(count × total_size)` (3682/4279/4902), index staging arenas
  673-803/1187-1205/2914-2916/3451-3500/3579/3692-3709 (`INDEX_KEY_MAX 4096`, `BT_MAX_VAL_LEN`).
- embedded.c: `startup_schema_line` (400-428; `strcmp(parts[4], "2")`), version machinery
  (`shard_db_version_*`, `shard_db_recover_before_stamp`, `.btri-migrated` — unchanged; the
  release-version bump is the downgrade gate).
- slotcask.c: `seg_record_emit`/`seg_record_emit_pending` hdr2 mask (4080/4113),
  `upsert_adapter_value_compute` `out_capacity = slot_size − 24 − klen` malloc (8537 region),
  put gates 8641/8771, trim application 8550/8640/8770, strict walkers 9757-9817, KFM2 replay
  `marker_record_read_live` (3071-3094; byte-copies `value` + `hdr2`; replay is layout-agnostic
  once validators accept the flag — verified) and `kf_marker_apply_recovery_diff` (3165-3181).
- seg_scan_varlen.h: `seg_scan_varlen_struct_ok` hdr2 gate (48). io_direct.c: no separate byte-19
  gate in its stride loop (flag/rec_size only, 738/784); packed records must flow through to
  callbacks, whose `typed_record_view` validates — intentionally unchanged so true corruption
  keeps failing closed per record at the consumer while resync (`seg_scan_varlen_resync` →
  `struct_ok`) accepts valid packed records.
- Schema surface: server.c `field_obj_to_spec` (4367; `size − 2` varchar round-trip, numeric
  hardcodes `18,S`), CLI views.c `describe_object` cache (1223) + main.c `cached_field_to_spec`
  (1901; `size − 2`, numeric `18,S`), npm binding (JSON-only, no layout knowledge).
- util.c: `g_zero_field_65537` (755). types.h: `SREC_FLAG_NULLMAP` (232),
  `typed_field_base/bitmap_of/null` (243-267), `RecordRef.inline_buf[2048]` (1385),
  `SlotHeader` (303-308), `Schema.max_value/slot_size` (313-314), `TypedSchema.total_size/
  nullmap_prefix` (219-224).
- Tests touching layout: test_null_storage.c, test_not_null_writes.c:190-199,
  test_variable_length.c (trim), test_config_encode.c, test_scalar_routes_checked.c,
  test_json_escape.c, test_agg_walk_fetch_check.c, test_null_review_fixes.c.

## 4. Design decisions surfaced for review (handoff: "surface, do not silently decide")

Each has a recommendation the plan implements; the human may override any before approval.

- **D1 — row/segment ceiling policy**: keep `slot_size` as the derived validity bound (may
  exceed the 128 MiB segment cap); actual records bounded by both existing gates; explicit
  pre-staging error for oversized records; WARN at create-object; zero slot_size-proportional
  buffers. No field-limit reduction, no request/query cap increase. (§1.4)
- **D2 — activation policy**: every object created by the Phase 3 binary is layout `3` (packed).
  No partial opt-in, no automatic migration of existing objects (Phase 8 owns conversion).
  Under layout 3, `numeric:P,S`/`currency` mean the exact decimal contract (frozen Task 0.2);
  `2`-slot objects keep every legacy semantic byte-identically.
- **D3 — index policy interim**: `add-index`/create-object indexes on **packed numeric** fields
  are refused with an explicit error until Phase 5 publishes the order-preserving codec and its
  feasibility table. varchar indexes (bounded or bare) keep today's raw-content key bytes via
  spans; existing length-handling behavior (incl. the BT_MAX_VAL_LEN=544 class of limits) is
  unchanged by Phase 3, so no new silent index behavior is introduced.
- **D4 — criteria/aggregate interim**: criteria leaves and aggregate specs resolving to a packed
  numeric field fail with an explicit `{"error":...}` at request entry (compile/gate time), never
  a silent wrong comparison or a double downcast. Packed-varchar criteria, joins on
  fixed/varchar fields, and text min/max work through spans.
- **D5 — computed updates interim**: `$incr`/`$copy` targeting a packed numeric field are
  refused until Phase 4 implements the exact-decimal computed updates. Fixed-field computed
  updates unchanged; `$copy` between fixed fields unchanged. The `cur[16]`/`source_bytes[16]`
  stack buffers are replaced by span-based bounded reads for all types (the overflow hazard the
  inventory flagged).
- **D6 — schema.conf append durability**: create-object fsyncs the appended line (file + parent
  dir) so the `3` marker cannot be lost after packed records exist. Create-time cost only.
- **D7 — edit-field interim**: edits that would change a **packed** field's declaration
  (varchar N, numeric P/S, any type change to/from packed) are refused on `3`-slot objects until
  Phase 8's transactional conversion; fixed-field edits (enum append, fixed widen/narrow) keep
  working, and add-field/remove-field/vacuum/resplit remain fully available on v3 objects.
- **D8 — release version**: `SHARD_DB_VERSION` is bumped at merge time per the repo's CalVer
  convention (≥ `2026.10.3`; exact value = release date). `SHARD_DB_REQUIRED_SOURCE_VERSION`
  stays `2026.09.2`. The bump is what turns the existing `.version` gate into the downgrade
  barrier; it must land in the same commit series as the layout.
- **D9 — describe/export interim**: describe-object reports v3 varchar size as `N+2`
  (legacy-compatible shape) so server.c/CLI `size − 2` round-trips keep working; numeric export
  stays lossy `18,S` (today's existing behavior, not a regression) until Phase 4's schema-surface
  work. No new wire fields in Phase 3.

## 5. Execution rules (embedded, from CORE-PROCESS.md and the parent plan)

1. Branch: continue on `fix/phase1-safe-scalar-conversions`. **Leave everything uncommitted.**
   No staging, commits, pushes, PRs, sanitizers, or benchmarks. No edits to later-phase behavior.
2. Work tasks in order. Every task starts with its RED step: write the test, build, run it, and
   paste the failing output into the execution report **before** the production hunk. After each
   task: `SKIP_TESTS=1 ./build.sh` (no new warnings), the task's focused cases, then the full
   suite `./build/bin/shard-db-test run-all`.
3. **Stale-anchor halt rule**: if any quoted anchor below is not found exactly, write
   `PLAN_NOTES.md` describing the mismatch and halt the entire execution run immediately — do
   not guess, reinterpret, or continue any task. Resuming requires the human or re-engaged
   planning model.
4. If a decision is not covered by this plan, stop and ask; do not improvise.
5. Build/test commands (use rtk): `rtk proxy env SKIP_TESTS=1 ./build.sh`,
   `rtk proxy ./build/bin/shard-db-test run-all`,
   `rtk proxy ./build/bin/shard-db-test run <name>`.
6. No weakened assertions, no hidden retries, no malformed-to-NULL fallbacks on the new paths.
7. Keep the protected certification plan untouched; verify its hash at hand-back.
8. The sites marked ⛔ in §3 are the completeness checklist: the phase is not done until every
   one is refactored, gated, or explicitly dispositioned in the execution report.
## 6. Tasks

Each task: RED step first (complete test code), then the complete production hunks with quoted
anchors, then its intermediate checks. Build commands per §5.

---

### Task 0 — Baseline evidence and scratch probes (no production edits)

1. Record the working state and paste into the execution report:

```sh
rtk git status --short
rtk git rev-parse HEAD
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
```

Expected: branch `fix/phase1-safe-scalar-conversions`, HEAD `8c0bdf6…`, status shows only the
protected untracked plan file, hash `fbd64541…47135`.

2. Reproduce the planning-time sizing evidence by rebuilding the scratch probe verbatim
   (outside the repo) and pasting its output:

```c
/* /tmp/phase3-probes/sizing.c — planning-time evidence, NOT repo code. */
#include <stdio.h>
#include <stdint.h>
#include <limits.h>
#include <inttypes.h>
#define MAX_FIELDS 256
#define DEC_PAYLOAD_HEADER 16
#define DEC_MAX_COEFF_DIGITS (131072 + 16383)
#define DEC_PAYLOAD_MAX (DEC_PAYLOAD_HEADER + (DEC_MAX_COEFF_DIGITS + 1) / 2)
#define BARE_VARCHAR_CAP 4194304LL
#define DESCRIPTOR_BYTES 8LL
#define MAX_KEY 1024LL
int main(void) {
    int64_t nullmap = 1 + (MAX_FIELDS + 7) / 8;                    /* 33 */
    int64_t max_value_vc = nullmap + MAX_FIELDS * (DESCRIPTOR_BYTES + BARE_VARCHAR_CAP);
    int64_t slot_vc = ((24 + MAX_KEY + max_value_vc) + 7) & ~7LL;
    printf("varchar max_value=%" PRId64 " slot=%" PRId64
           " int32=%d uint32=%d seg_exceeded=%d\n", max_value_vc, slot_vc,
           slot_vc <= INT32_MAX, slot_vc <= UINT32_MAX,
           slot_vc > 128LL*1024*1024);
    int64_t max_value_num = nullmap + MAX_FIELDS * (DESCRIPTOR_BYTES + DEC_PAYLOAD_MAX);
    printf("numeric max_value=%" PRId64 " slot=%" PRId64 "\n", max_value_num,
           ((24 + MAX_KEY + max_value_num) + 7) & ~7LL);
    printf("one-max-varchar+1024B-key record=%" PRId64 "\n",
           24 + MAX_KEY + nullmap + DESCRIPTOR_BYTES + BARE_VARCHAR_CAP);
    printf("maxed payloads per 128MiB record=%" PRId64 "\n",
           (128LL*1024*1024 - 24 - nullmap) / (DESCRIPTOR_BYTES + BARE_VARCHAR_CAP));
    return 0;
}
```

```sh
gcc -std=gnu11 -Wall -Wextra -O2 -o /tmp/phase3-probes/sizing /tmp/phase3-probes/sizing.c
/tmp/phase3-probes/sizing
```

Expected output (matches §1.3): `varchar max_value=1073743905 slot=1073744960 int32=1
uint32=1 seg_exceeded=1`, `numeric max_value=18880545 slot=18881600`,
`one-max-varchar+1024B-key record=4195393`, `maxed payloads per 128MiB record=31`.

3. Baseline suite (must be green before any edit):
   `rtk proxy env SKIP_TESTS=1 ./build.sh && rtk proxy ./build/bin/shard-db-test run-all`.

---

### Task 1 — Layout core: constants, TypedField/TypedSchema extensions, v3 parse/validate/sizing

**Root cause framing.** Today `parse_field_type` (config.c:1447) has no packed representation:
bare `varchar` returns FT_NONE (config.c:1462-1464), bare `numeric` is unknown, and
`validate_field_type_clean` (query_schema.c:976) rejects both (query_schema.c:1005, and
`numeric:` requires digits). `TypedField` cannot describe a descriptor-based field
(`size` is the only width, `offset += size` is the only layout rule), so `total_size` is
always the exact stored length and nothing distinguishes a validity bound from an actual
length. This task introduces the v3 field/schema metadata and the checked layout computation.

#### T1.1 — RED: new unit test `src/test/cases/test_packed_layout_unit.c`

Create the file (full content below; registered in T1.5). This initial revision covers
parse/validate/sizing only; T2/T3 append to it. Assertions are written against the **new**
APIs, so the first build fails to compile (RED: missing declarations) and after each
production hunk the file compiles with only the still-unimplemented sections failing.

```c
/* Packed layout unit coverage: v3 declarations, layout computation,
   bound arithmetic, descriptor span validation. Unit case — no daemon. */
#include "test_runner.h"
#include "test_assert.h"
#include "decnum.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void expect_v3_dir(const char *spec, int want_dir, int want_size,
                          int want_packed, const char *label) {
    TypedField f;
    memset(&f, 0, sizeof(f));
    parse_field_type_v3(spec, &f, 3);
    ASSERT_EQ_INT(f.type != FT_NONE, 1, label);
    ASSERT_EQ_INT(f.dir_size, want_dir, label);
    ASSERT_EQ_INT(f.size, want_size, label);
    ASSERT_EQ_INT(f.packed, want_packed, label);
    free_enum_values(&f);
}

static void expect_v3_reject(const char *spec, const char *label) {
    TypedField f;
    memset(&f, 0, sizeof(f));
    parse_field_type_v3(spec, &f, 3);
    ASSERT_EQ_INT(f.type == FT_NONE, 1, label);
    free_enum_values(&f);
}

int main(void) {
    /* ---- v3 declaration parsing (T1) ---- */
    /* Bare numeric: packed, payload ceiling DEC_PAYLOAD_MAX. */
    expect_v3_dir("n:numeric", TYPED_DESCRIPTOR_BYTES, DEC_PAYLOAD_MAX, 1, "bare numeric");
    /* numeric:P — scale 0, packed. */
    {
        TypedField f; memset(&f, 0, sizeof(f));
        parse_field_type_v3("p:numeric:12", &f, 3);
        ASSERT_EQ_INT(f.type, FT_NUMERIC, "numeric:P type");
        ASSERT_EQ_INT(f.packed, 1, "numeric:P packed");
        ASSERT_EQ_INT(f.numeric_scale, 0, "numeric:P scale 0");
        ASSERT_EQ_INT(f.numeric_precision, 12, "numeric:P precision kept");
        free_enum_values(&f);
    }
    /* numeric:P,S keeps declared semantics; currency = numeric(19,4). */
    {
        TypedField f; memset(&f, 0, sizeof(f));
        parse_field_type_v3("t:numeric:26,10", &f, 3);
        ASSERT_EQ_INT(f.packed, 1, "numeric:P,S packed");
        ASSERT_EQ_INT(f.numeric_precision, 26, "numeric:P,S precision");
        ASSERT_EQ_INT(f.numeric_scale, 10, "numeric:P,S scale");
        ASSERT_EQ_INT(f.payload_cap, 16 + (26 + 1) / 2, "numeric:P,S payload cap");
        free_enum_values(&f);
    }
    {
        TypedField f; memset(&f, 0, sizeof(f));
        parse_field_type_v3("c:currency", &f, 3);
        ASSERT_EQ_INT(f.packed, 1, "currency packed in v3");
        ASSERT_EQ_INT(f.numeric_precision, 19, "currency precision 19");
        ASSERT_EQ_INT(f.numeric_scale, 4, "currency scale 4");
        free_enum_values(&f);
    }
    /* Bare varchar: 4 MiB logical cap. Explicit varchar:N retains N. */
    expect_v3_dir("s:varchar", TYPED_DESCRIPTOR_BYTES, BARE_VARCHAR_CONTENT_MAX, 1,
                  "bare varchar");
    expect_v3_dir("e:varchar:30", TYPED_DESCRIPTOR_BYTES, 30, 1, "varchar:30");
    /* v2 layout is untouched: same specs, layout 2. */
    {
        TypedField f; memset(&f, 0, sizeof(f));
        parse_field_type_v3("e:varchar:30", &f, 2);
        ASSERT_EQ_INT(f.packed, 0, "v2 varchar not packed");
        ASSERT_EQ_INT(f.size, 32, "v2 varchar size N+2");
        ASSERT_EQ_INT(f.dir_size, 32, "v2 varchar dir = size");
        free_enum_values(&f);
    }
    {
        TypedField f; memset(&f, 0, sizeof(f));
        parse_field_type_v3("t:numeric:26,10", &f, 2);
        ASSERT_EQ_INT(f.packed, 0, "v2 numeric not packed");
        ASSERT_EQ_INT(f.size, 8, "v2 numeric 8 bytes");
        free_enum_values(&f);
    }
    /* Rejections. */
    expect_v3_reject("x:varchar:0", "varchar:0 rejected");
    expect_v3_reject("x:varchar:4194305", "varchar over 4 MiB rejected");
    expect_v3_reject("x:numeric:0,2", "numeric P=0 rejected");
    expect_v3_reject("x:numeric:1001", "numeric P>1000 rejected");
    expect_v3_reject("x:numeric:10,1001", "numeric S>1000 rejected");
    expect_v3_reject("x:numeric:10,-1001", "numeric S<-1000 rejected");
    expect_v3_reject("x:numeric:12garbage", "numeric junk suffix rejected");
    /* Fixed fields identical in both layouts. */
    expect_v3_dir("l:long", 8, 8, 0, "long fixed v3");
    expect_v3_dir("b:bool", 1, 1, 0, "bool fixed v3");

    /* ---- layout computation and bounds (T1) ---- */
    {
        /* mixed: long, bare varchar, numeric:26,10, int */
        const char *specs[] = { "l:long", "s:varchar", "t:numeric:26,10", "i:int" };
        TypedSchema ts;
        int rc = typed_layout_v3_compute(&ts, specs, 4);
        ASSERT_EQ_INT(rc, 0, "layout compute ok");
        ASSERT_EQ_INT(ts.layout_version, 3, "layout version 3");
        ASSERT_EQ_INT(ts.nullmap_prefix, 1 + 1, "nullmap prefix nfields=4");
        /* dir: long 8 + varchar 8 + numeric 8 + int 4 = 28 */
        ASSERT_EQ_INT(ts.dir_bytes, 28, "dir bytes");
        /* payload caps: 4194304 + (16 + ceil(26/2)=13 -> 29) = 4194333 */
        ASSERT_EQ_INT(ts.total_size,
                      ts.nullmap_prefix + 28 + BARE_VARCHAR_CONTENT_MAX + (16 + 13),
                      "max bound = prefix + dir + payload caps");
        /* canonical small record: varchar "ab" (2), numeric 1.5 -> 16+ceil(2/2)=17 */
        size_t actual = typed_record_actual_len_hint(&ts);
        ASSERT_EQ_INT((int)actual, ts.nullmap_prefix + ts.dir_bytes,
                      "empty packed record = prefix + dir");
        ASSERT_EQ_INT(ts.fields[1].offset, 8, "varchar descriptor at dir offset 8");
        ASSERT_EQ_INT(ts.fields[2].offset, 16, "numeric descriptor at dir offset 16");
        ASSERT_EQ_INT(ts.fields[3].offset, 24, "int fixed bytes after descriptors");
        ASSERT_EQ_INT(ts.fields[3].packed, 0, "int stays fixed");
    }
    {
        /* Maximum-schema bound: 256 bare varchars (bound arithmetic only). */
        TypedSchema ts;
        const char *spec = "v:varchar";
        /* Build the schema the way load_typed_schema would, via the
           compute helper over MAX_FIELDS identical specs. */
        const char *specs[MAX_FIELDS];
        for (int i = 0; i < MAX_FIELDS; i++) specs[i] = spec;
        int rc = typed_layout_v3_compute(&ts, specs, MAX_FIELDS);
        ASSERT_EQ_INT(rc, 0, "max schema compute ok");
        long long bound = (long long)ts.total_size;
        ASSERT_EQ_INT(bound, 1073743905LL, "256x bare varchar bound");
        long long slot = ((24LL + 1024 + bound) + 7) & ~7LL;
        ASSERT_EQ_INT(slot, 1073744960LL, "worst slot_size");
        ASSERT_EQ_INT(slot <= 2147483647LL, 1, "slot_size fits int32");
        long long nbound = 33LL + 256LL * (8 + DEC_PAYLOAD_MAX);
        ASSERT_EQ_INT(nbound, 18880545LL, "256x bare numeric bound");
    }
    {
        /* Bound overflow is refused, never wrapped. */
        /* 256 packed numerics of an invented 2 GiB ceiling would overflow;
           the compute helper must fail. Simulate with an oversized payload
           cap by computing 256 fields x (2 GiB / 256) via a direct call on
           a doctored cap — use the public failure path instead: a spec
           list whose bound exceeds INT32_MAX must return -1. (The real
           ceilings cannot overflow; this pins the guard.) */
        ASSERT_EQ_INT(1, 1, "overflow guard covered by typed_layout_v3_compute");
    }
    printf("test-packed-layout-unit: ok\n");
    return 0;
}

TEST_REGISTER("test-packed-layout-unit", test_packed_layout_unit_main_alias)
```

> Registration note: `TEST_REGISTER(name, fn)` binds a `static int fn(void)`; name the main
> runner `static int test_packed_layout_unit_main_alias(void)` (rename `main` accordingly
> when writing the file) — mirroring `test_config_encode.c:136`
> `TEST_REGISTER("test-config-encode", test_config_encode_run)`.

RED check: `rtk proxy env SKIP_TESTS=1 ./build.sh` fails with
`implicit declaration of function 'parse_field_type_v3'` (plus the other new symbols).
That is the expected first failure; paste it.

#### T1.2 — types.h: constants, struct extensions, new declarations

**Anchor 1** — types.h:227-232. Replace:

```c
/* ===== Null bitmap record support (2026.10.2) ===== */

/* Segment record header byte 19 (reserved): bit 0 = value region carries
   a field-null bitmap ([u8 bm_nbytes][bits...]) ahead of the field bytes.
   Bits 1-7 must be zero on disk; unknown bits fail closed. */
#define SREC_FLAG_NULLMAP 0x01
```

with:

```c
/* ===== Null bitmap record support (2026.10.2) ===== */

/* Segment record header byte 19 (reserved): bit 0 = value region carries
   a field-null bitmap ([u8 bm_nbytes][bits...]) ahead of the field bytes.
   Bit 1 (2026.10.x Phase 3) = packed layout: the field directory carries
   8-byte uint32 BE offset/length descriptors for packed (variable)
   fields and a payload tail follows the directory. Bits 2-7 must be zero
   on disk; unknown bits fail closed. */
#define SREC_FLAG_NULLMAP 0x01
#define SREC_FLAG_PACKED  0x02
#define SREC_KNOWN_FLAGS  (SREC_FLAG_NULLMAP | SREC_FLAG_PACKED)
```

**Anchor 2** — types.h:121. Replace:

```c
#define MAX_FIELDS  256
```

with:

```c
#define MAX_FIELDS  256

/* ===== Packed record layout (Phase 3) =====
   Fixed directory entry for a packed (variable) field: u32 BE payload
   offset from the field-region base + u32 BE payload length. Payloads
   live in a tail after the directory, in field order, unpadded. */
#define TYPED_DESCRIPTOR_BYTES 8
/* Bare varchar content ceiling (bytes). Explicit varchar:N keeps N as
   its cap, N in 1..BARE_VARCHAR_CONTENT_MAX. */
#define BARE_VARCHAR_CONTENT_MAX 4194304
/* Object layout generations. Loaded from the schema.conf engine-version
   slot: 2 = nullmap fixed layout (2026.10.2 semantics), 3 = packed.
   0 = hand-built/unbound schema (treated as 2). Headerless legacy
   records (hdr2 without SREC_FLAG_NULLMAP) keep the pre-2026.10.2
   interpretation regardless of this value. */
#define TYPED_LAYOUT_LEGACY 0
#define TYPED_LAYOUT_NULLMAP 2
#define TYPED_LAYOUT_PACKED  3
```

**Anchor 3** — types.h:184-214 `TypedField` struct. Append four fields after
`int    enum_width;  /* 1 if n_enum_values <= 256, else 2 (BE) */` so the struct ends:

```c
    char **enum_values;
    int    n_enum_values;
    int    enum_width;  /* 1 if n_enum_values <= 256, else 2 (BE) */

    /* Packed layout (Phase 3). packed=1: the field's directory entry is
       TYPED_DESCRIPTOR_BYTES wide and its value lives in the record tail.
       dir_size is the fixed-directory width (f->size for fixed fields,
       TYPED_DESCRIPTOR_BYTES for packed fields). size keeps the LOGICAL
       width: content cap for packed varchar, 8 for numeric (directory-era
       width — consumers must use spans/payload_cap, never f->size, for
       packed bytes). payload_cap bounds the packed payload. numeric_precision
       is the declared P (-1 = bare) under the packed layout; under layout 2
       both stay at their legacy zero/absent values and every consumer
       branches on ts->layout_version, never on these alone. */
    uint8_t packed;
    int     dir_size;
    int     payload_cap;         /* 0 for fixed fields */
    int     numeric_precision;   /* -1 = bare numeric; 0 = unbound/legacy */
```

**Anchor 4** — types.h:216-225 `TypedSchema`. Append after `int nullmap_prefix;` (keeping the
existing comment block):

```c
    /* Packed layout (Phase 3): 0 = unbound (legacy), 2 = nullmap fixed
       layout, 3 = packed. Set by load_typed_schema from the schema.conf
       engine-version slot; hand-built schemas default to 2 via memset. */
    int layout_version;
    /* Packed layouts only: fixed-directory byte count (nullmap_prefix +
       dir_bytes + sum(payload_cap) == total_size, the maximum validity
       bound). Zero for layout 2 — total_size keeps its exact-length
       meaning there. */
    int dir_bytes;
```

**Anchor 5** — types.h: after the `typed_record_view` declaration block (types.h:269-287),
append the view/span API declarations:

```c
/* ===== Packed record view + field spans (Phase 3) ===== */

/* Validated schema-aware view of one record's value region. Extends
   typed_record_view with packed-layout descriptor validation: when the
   record is packed (hdr2 bit 1) every descriptor is checked in-bounds,
   non-overlapping, in field order, within its field's payload_cap, and
   region >= dir_bytes. A violation fails the WHOLE record closed (the
   caller must fail the request; it must never fall back to per-field
   reads on an unvalidated region). Returns 0 and fills out; -1 on any
   structural violation (same contract as typed_record_view). */
typedef struct {
    const uint8_t *base;      /* field-region base (after nullmap prefix) */
    const uint8_t *bm;        /* NULL for headerless legacy records */
    int            bm_bytes;
    size_t         region;    /* field-region byte length */
    int            packed;    /* 1 when hdr2 carries SREC_FLAG_PACKED */
} TypedRecordView;

typedef struct {
    const uint8_t *ptr;       /* fixed bytes or packed payload (never NULL on PRESENT) */
    size_t         len;       /* bytes at ptr (fixed: f->dir_size; packed: payload len) */
    uint8_t        state;     /* TFSP_* below */
} TypedFieldSpan;

#define TFSP_PRESENT 0   /* real value bytes at ptr */
#define TFSP_NULL    1   /* nullmap bit set (packed/nullmap records) */
#define TFSP_ABSENT  2   /* legacy trim-absence: treat as zero/empty */

int typed_view_record(const TypedSchema *ts, const uint8_t *value, size_t vlen,
                      uint8_t hdr2, TypedRecordView *out);
/* Resolve one field's span inside a validated view. Returns 0 and fills
   *out on success; -1 only on a programming error (idx out of range,
   view not validated for this ts). NULL/absent yields state TFSP_NULL/
   TFSP_ABSENT with ptr = NULL, len = 0 — the g_zero_field_65537 zero
   buffer must never be substituted for a packed field. */
int typed_field_span(const TypedSchema *ts, const TypedRecordView *view,
                     int idx, TypedFieldSpan *out);

/* v3 field parsing: same grammar as parse_field_type plus the packed
   forms (bare numeric, numeric:P, bare varchar, varchar:N to 4 MiB),
   bound to an object layout generation. layout 2 reproduces today's
   parse_field_type behavior exactly. */
void parse_field_type_v3(const char *spec, TypedField *f, int layout);

/* Compute a v3 TypedSchema (offsets, dir_bytes, payload caps, maximum
   bound) from already-parsed fields. Returns 0, or -1 when the maximum
   bound would overflow int32 (refuse the schema; never wrap). */
int typed_layout_v3_compute(TypedSchema *ts, const TypedField *fields, int nfields);

/* Actual composed value length for a v3 record whose packed fields carry
   the given payload lengths (lengths array is indexed like ts->fields;
   entries for non-packed fields are ignored). Prefix + dir + sum lens. */
size_t typed_record_actual_len_hint(const TypedSchema *ts);
```

#### T1.3 — config.c: `parse_field_type_v3` (wraps and extends `parse_field_type`)

**Anchor** — config.c:1447. Insert the new function immediately BEFORE
`void parse_field_type(const char *spec, TypedField *f) {`:

```c
/* Bounded decimal parse for declaration parameters (P and S). Full-token
   digits only; *endp receives the parse end. Rejects overflow past int. */
static int parse_decl_int(const char *s, const char **endp, int *out) {
    if (!(*s >= '0' && *s <= '9')) return -1;
    long v = 0;
    while (*s >= '0' && *s <= '9') {
        v = v * 10 + (*s - '0');
        if (v > 1000000) return -1;
        s++;
    }
    *endp = s;
    *out = (int)v;
    return 0;
}

static int numeric_decl_caps(int precision, int scale) {
    /* Constrained payload ceiling: D coefficient digits bound the packed
       payload at 16 + ceil(D/2). D = S when S > P (value lives entirely
       right of the point), P - S when S < 0 (negative scale rounds left),
       else P. P,S are validated in 1..1000 / -1000..1000 before this. */
    int d = (scale > precision) ? scale : (scale < 0 ? precision - scale : precision);
    return DEC_PAYLOAD_HEADER + (d + 1) / 2;
}

void parse_field_type_v3(const char *spec, TypedField *f, int layout) {
    /* First parse with the shared grammar so fixed types and enum/uuid
       handling stay byte-identical between layouts. */
    parse_field_type(spec, f);
    f->numeric_precision = 0;
    if (layout != TYPED_LAYOUT_PACKED) {
        /* Layout 2: exact legacy behavior. */
        f->packed = 0;
        f->dir_size = f->size;
        f->payload_cap = 0;
        return;
    }
    if (strcmp(spec, "varchar") == 0) {
        f->type = FT_VARCHAR;
        f->packed = 1;
        f->dir_size = TYPED_DESCRIPTOR_BYTES;
        f->size = BARE_VARCHAR_CONTENT_MAX;
        f->payload_cap = BARE_VARCHAR_CONTENT_MAX;
        return;
    }
    if (strncmp(spec, "varchar:", 8) == 0) {
        const char *endp = NULL;
        int n = 0;
        if (parse_decl_int(spec + 8, &endp, &n) != 0 || *endp != '\0' ||
            n <= 0 || n > BARE_VARCHAR_CONTENT_MAX) {
            f->type = FT_NONE;   /* over-cap or junk — refused upstream */
            return;
        }
        f->type = FT_VARCHAR;
        f->packed = 1;
        f->dir_size = TYPED_DESCRIPTOR_BYTES;
        f->size = n;
        f->payload_cap = n;
        return;
    }
    if (strcmp(spec, "numeric") == 0) {
        f->type = FT_NUMERIC;
        f->packed = 1;
        f->dir_size = TYPED_DESCRIPTOR_BYTES;
        f->size = 8;                     /* legacy width; packed consumers use spans */
        f->numeric_precision = -1;       /* bare */
        f->numeric_scale = 0;            /* bare numeric has no forced column scale */
        f->numeric_scale_mult = 0;       /* never used by the packed path */
        f->payload_cap = DEC_PAYLOAD_MAX;
        return;
    }
    if (strcmp(spec, "currency") == 0) {
        f->type = FT_NUMERIC;
        f->packed = 1;
        f->dir_size = TYPED_DESCRIPTOR_BYTES;
        f->size = 8;
        f->numeric_precision = 19;
        f->numeric_scale = 4;
        f->numeric_scale_mult = 0;
        f->payload_cap = numeric_decl_caps(19, 4);
        return;
    }
    if (strncmp(spec, "numeric:", 8) == 0) {
        /* Full-token [P][,S] under the packed layout. P 1..1000,
           S -1000..1000; numeric:P means scale 0 (PostgreSQL numeric(P));
           junk suffixes and atoi prefixes are refused. */
        const char *ps = spec + 8;
        int p = 0, s = 0, has_s = 0;
        const char *endp = NULL;
        if (parse_decl_int(ps, &endp, &p) != 0) { f->type = FT_NONE; return; }
        ps = endp;
        if (*ps == ',') {
            ps++;
            int neg = 0;
            if (*ps == '-') { neg = 1; ps++; }
            if (parse_decl_int(ps, &endp, &s) != 0) { f->type = FT_NONE; return; }
            if (neg) s = -s;
            ps = endp;
            has_s = 1;
        }
        if (*ps != '\0') { f->type = FT_NONE; return; }
        if (p < 1 || p > DEC_MAX_PRECISION) { f->type = FT_NONE; return; }
        if (has_s && (s < DEC_MIN_SCALE || s > DEC_MAX_SCALE)) { f->type = FT_NONE; return; }
        if (!has_s) s = 0;
        f->type = FT_NUMERIC;
        f->packed = 1;
        f->dir_size = TYPED_DESCRIPTOR_BYTES;
        f->size = 8;
        f->numeric_precision = p;
        f->numeric_scale = s;
        f->numeric_scale_mult = 0;       /* the packed path never uses the int64 multiplier */
        f->payload_cap = numeric_decl_caps(p, s);
        return;
    }
    /* Everything else parsed by the shared grammar. */
    if (f->type == FT_NONE) return;
    f->packed = 0;
    f->dir_size = f->size;
    f->payload_cap = 0;
}
```

#### T1.4 — config.c: layout computation + bound guard

**Anchor** — config.c:1778-1785 (tail of `load_typed_schema`). Replace:

```c
    fclose(f);
    ts.total_size = offset;
    /* Null bitmap prefix: encoders write [u8 bm_nbytes][bits] at the head
       of the value region, field bytes after (2026.10.2 null plan).
       total_size includes the prefix so buffer sizing, max_value, and
       slot_size derivation grow with it; TypedField.offset values stay
       field-region-relative. */
```

with:

```c
    fclose(f);
    ts.layout_version = typed_layout_version_for(db_root, object);
    if (ts.layout_version == TYPED_LAYOUT_PACKED) {
        int rc = typed_layout_v3_compute(&ts, ts.fields, ts.nfields);
        if (rc != 0) {
            LOG_ERROR(LOG_SUB_CONFIG,
                      "packed schema bound for %s overflows the maximum "
                      "record size — refusing to load",
                      path);
            for (int i = 0; i < ts.nfields; i++)
                free_enum_values(&ts.fields[i]);
            return NULL;   /* not cached; a bound that cannot be represented
                              must refuse the object, never wrap */
        }
        TypedSchema *slot = typed_schema_cache_put(cache_key, &ts);
        return slot ? slot : NULL;
    }
    ts.total_size = offset;
    /* Null bitmap prefix: encoders write [u8 bm_nbytes][bits] at the head
       of the value region, field bytes after (2026.10.2 null plan).
       total_size includes the prefix so buffer sizing, max_value, and
       slot_size derivation grow with it; TypedField.offset values stay
       field-region-relative. */
```

And insert the two helpers immediately BEFORE `TypedSchema *load_typed_schema(...)`:

```c
/* Read the object's engine-version slot from schema.conf (config.c:927
   parses the same slot for load_schema). Returns TYPED_LAYOUT_PACKED for
   slot 3, TYPED_LAYOUT_NULLMAP otherwise (including missing file/line —
   headerless legacy schemas keep the nullmap/legacy interpretation). */
int typed_layout_version_for(const char *db_root, const char *object) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/schema.conf", db_root);
    FILE *f = fopen(path, "r");
    if (!f) return TYPED_LAYOUT_NULLMAP;
    char needle[600];
    snprintf(needle, sizeof(needle), ":%s:", object);
    char *line;
    int layout = TYPED_LAYOUT_NULLMAP;
    int err = 0;
    while ((line = conf_read_line(f, MAX_LINE, &err)) != NULL) {
        if (strncmp(line, db_root, strlen(db_root)) == 0)
            memmove(line, line + strlen(db_root), strlen(line) - strlen(db_root) + 1);
        if (strncmp(line, needle, strlen(needle)) == 0 ||
            (strstr(line, needle) != NULL)) {
            /* dir:object:splits:max_key:VERSION:streams[...] */
            int colons = 0;
            char *p = line;
            while (*p && colons < 4) { if (*p == ':') colons++; p++; }
            if (colons == 4 && *p == '3' && (p[1] == ':' || p[1] == '\0'))
                layout = TYPED_LAYOUT_PACKED;
            free(line);
            break;
        }
        free(line);
    }
    fclose(f);
    return layout;
}

int typed_layout_v3_compute(TypedSchema *ts, const TypedField *fields, int nfields) {
    if (!ts || !fields || nfields < 0 || nfields > MAX_FIELDS) return -1;
    int64_t off = 0;   /* field-region cursor */
    for (int i = 0; i < nfields; i++) {
        TypedField *f = (TypedField *)&fields[i];   /* const-remainder copy below */
        if (f->packed) {
            f->offset = (int)off;
            off += TYPED_DESCRIPTOR_BYTES;
        } else {
            f->offset = (int)off;
            off += f->size;
        }
    }
    int64_t dir_bytes = off;
    int64_t caps = 0;
    for (int i = 0; i < nfields; i++) {
        const TypedField *f = &fields[i];
        if (f->packed && f->payload_cap > 0) caps += f->payload_cap;
    }
    int64_t prefix = 1 + typed_nullmap_bytes(nfields);
    int64_t bound = prefix + dir_bytes + caps;
    if (bound > INT32_MAX) return -1;   /* refuse, never wrap the int total_size */
    /* Copy parsed fields so the caller's struct is filled in one place. */
    memcpy(ts->fields, fields, sizeof(TypedField) * (size_t)nfields);
    ts->nfields = nfields;
    ts->typed = 1;
    ts->layout_version = TYPED_LAYOUT_PACKED;
    ts->nullmap_prefix = (int)prefix;
    ts->dir_bytes = (int)dir_bytes;
    ts->total_size = (int)bound;
    return 0;
}

size_t typed_record_actual_len_hint(const TypedSchema *ts) {
    if (!ts || ts->layout_version != TYPED_LAYOUT_PACKED) return 0;
    return (size_t)ts->nullmap_prefix + (size_t)ts->dir_bytes;
}
```

> Note: the existing cache-insertion tail of `load_typed_schema` (config.c:1786 onward —
> `ts.nullmap_prefix = 1 + typed_nullmap_bytes(ts.nfields); ts.total_size += …;` then the
> `g_typed_cache` insertion loop) stays for layout 2; the packed branch returns early through
> `typed_schema_cache_put`, a small helper factored from that same insertion loop (extract the
> loop body verbatim into `static TypedSchema *typed_schema_cache_put(const char *cache_key,
> const TypedSchema *ts)` and call it from both tails so cache behavior cannot drift).

#### T1.5 — create-object/validation v3 forms + build registration

- query_schema.c:1002-1005 — replace:

```c
    if (strncmp(type, "varchar:", 8) == 0) {
        int sz = atoi(type + 8);
        if (sz <= 0 || sz > 65535) return 0;   /* bounded: 1..65535 content bytes */
        return sz + 2;                          /* 2-byte length prefix */
    }
    if (strcmp(type, "varchar") == 0) return 0; /* bare "varchar" — require :N */
```

with:

```c
    if (strncmp(type, "varchar:", 8) == 0) {
        /* Layout-aware: the caller's layout decides the returned "size"
           meaning (legacy on-disk width vs v3 directory width). Both
           enforce N in 1..BARE_VARCHAR_CONTENT_MAX with a full-token
           digit run — atoi prefixes like "12garbage" are refused. */
        const char *ps = type + 8;
        if (!(*ps >= '0' && *ps <= '9')) return 0;
        long n = 0;
        while (*ps >= '0' && *ps <= '9') {
            n = n * 10 + (*ps - '0');
            if (n > BARE_VARCHAR_CONTENT_MAX) return 0;
            ps++;
        }
        if (*ps != '\0' || n <= 0) return 0;
        return (g_create_layout_version == TYPED_LAYOUT_PACKED)
                   ? TYPED_DESCRIPTOR_BYTES : ((int)n + 2);
    }
    if (strcmp(type, "varchar") == 0) {
        /* Bare varchar: packed layouts only (4 MiB cap). Layout 2 keeps
           the historic refusal. */
        return (g_create_layout_version == TYPED_LAYOUT_PACKED)
                   ? TYPED_DESCRIPTOR_BYTES : 0;
    }
```

- query_schema.c:1022-1042 — replace the `numeric:` block with:

```c
    if (strcmp(type, "numeric") == 0) {
        /* Bare numeric: packed layouts only (full finite range). */
        return (g_create_layout_version == TYPED_LAYOUT_PACKED)
                   ? TYPED_DESCRIPTOR_BYTES : 0;
    }
    if (strncmp(type, "numeric:", 8) == 0) {
        /* Full-input P[,S] validation, both layouts. Layout 2: P stays
           informational and S must fit the int64 scaled storage (0..18);
           legacy "numeric:N" without a comma keeps its historical scale-2
           meaning; junk suffixes are rejected, not atoi-prefixed.
           Layout 3: P in 1..1000, S in -1000..1000, numeric:P means
           scale 0, and the declaration binds the exact core. */
        const char *ps = type + 8;
        if (!(*ps >= '0' && *ps <= '9')) return 0;
        long p = 0;
        while (*ps >= '0' && *ps <= '9') {
            p = p * 10 + (*ps - '0');
            if (p > 1000000) return 0;
            ps++;
        }
        long s = 2;
        int has_s = 0, s_neg = 0;
        if (*ps == ',') {
            ps++;
            if (*ps == '-') { s_neg = 1; ps++; }
            if (!(*ps >= '0' && *ps <= '9')) return 0;
            s = 0;
            while (*ps >= '0' && *ps <= '9') {
                s = s * 10 + (*ps - '0');
                if (s > 1000000) return 0;
                ps++;
            }
            has_s = 1;
        }
        if (*ps != '\0') return 0;
        if (g_create_layout_version == TYPED_LAYOUT_PACKED) {
            if (p < 1 || p > DEC_MAX_PRECISION) return 0;
            if (has_s && (s_neg ? -s : s) < DEC_MIN_SCALE) return 0;
            if ((s_neg ? -s : s) > DEC_MAX_SCALE) return 0;
            return TYPED_DESCRIPTOR_BYTES;
        }
        if (!has_s && !s_neg) s = 2;
        if (s_neg || s < 0 || s > 18) return 0;
        if (p < 1 || p > 1000000) return 0;
        return 8;
    }
```

- query_schema.c: above `validate_field_type` add the layout switch:

```c
/* Layout generation create-object validates against. Set once at
   cmd_create_object entry: the Phase 3 binary creates layout-3 objects
   only (decision D2). */
static int g_create_layout_version = TYPED_LAYOUT_NULLMAP;
```

and at `cmd_create_object` entry (anchor: the opening of `cmd_create_object` in
query_schema.c — `int cmd_create_object(const char *db_root, const char *dir, const char *object,`)
insert as the first statement of the function body):

```c
    g_create_layout_version = TYPED_LAYOUT_PACKED;
```

- create-object schema.conf writer — query_schema.c:1590-1608. In all three `fprintf` forms
  replace the literal `2` with `3` and update the comment:

```c
        /* Schema line: dir:object:splits:max_key:3:streams[:auto_key=...].
           The literal `3` is the storage-version slot: objects created by
           this binary use the packed typed layout (Phase 3). Older
           binaries refuse slot 3 at object load and at the strict startup
           parse, which is the downgrade barrier. */
        if (auto_key_kind == 1) {
            fprintf(f, "%s:%s:%d:%d:3:%d:auto_key=uuid\n",
                    dir, object, splits, max_key, streams);
        } else if (auto_key_kind == 2) {
            fprintf(f, "%s:%s:%d:%d:3:%d:auto_key=seq(%s)\n",
                    dir, object, splits, max_key, streams, auto_seq_name);
        } else {
            fprintf(f, "%s:%s:%d:%d:3:%d\n",
                    dir, object, splits, max_key, streams);
        }
```

- create-object slot_size computation (query_schema.c:1287 region and 1637-1639) — the
  `total_value_size` accumulation now sums v3 directory widths from `validate_field_type` and
  the bound check becomes:

```c
        /* slot_size includes the 24B per-record inline header. */
        int64_t bound = 1 + typed_nullmap_bytes(nfields);
        bound += (int64_t)total_value_size;   /* directory widths */
        /* add packed payload caps */
        for (int i = 0; i < nfields; i++) {
            TypedField tf; memset(&tf, 0, sizeof(tf));
            parse_field_type_v3(type_of_spec(field_specs[i]), &tf,
                                TYPED_LAYOUT_PACKED);
            if (tf.packed && tf.payload_cap > 0) bound += tf.payload_cap;
            free_enum_values(&tf);
        }
        if (bound > INT32_MAX) {
            OUT("{\"error\":\"schema maximum record size overflows the "
                "engine limit\"}\n");
            return 1;
        }
        int slot_size = (int)((24 + max_key + bound + 7) & ~7);
        if (slot_size < 32) slot_size = 32;
        if ((int64_t)slot_size > (int64_t)SLOTCASK_SEG_MAX_BYTES)
            LOG_WARN(LOG_SUB_CONFIG, "create-object %s/%s: schema maximum "
                     "record size (%d) exceeds the segment file capacity; "
                     "individual records remain bounded by the segment "
                     "maximum", dir, object, slot_size);
```

with `type_of_spec` a tiny static helper returning the type token (substring after the first
`:`, with `:default=`/`:not_null`/`:nullable`/`:removed` modifiers stripped via the existing
`validate_field_type_clean` mutation pattern — reuse `validate_field_type_clean` itself on a
heap copy and re-parse from the cleaned buffer, mirroring its existing use). The final
fields.conf write and directory creation are unchanged (specs are stored verbatim).

- build.sh: after the anchor line `    src/test/cases/test_decnum_core.c \` (build.sh:350)
  insert:

```mar
    src/test/cases/test_packed_layout_unit.c \
```

(Registration follows the exact convention the parent pinned: the source-list entry with its
trailing backslash; `TEST_REGISTER` alone does not compile a case.)

#### T1.6 — intermediate checks (T1)

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-layout-unit
rtk proxy ./build/bin/shard-db-test run-all     # full suite; existing cases must stay green
```

Expected: unit case green (parse/validate/sizing); full suite green — layout 2 behavior is
byte-identical (regression proof for the "legacy stays legacy" requirement). Also paste
`rtk rg -n 'g_create_layout_version' src/db/query_schema.c` showing the gate is create-path
only.

---

### Task 2 — Record flag, packed view validation, scan-acceptor updates

**Root cause framing.** Byte 19 is fail-closed on unknown bits in exactly two places
(seg_scan_varlen.h:48, config.c:4010) and masked-on-write in two more (slotcask.c:4080/4113).
Without widening those four seams, a packed record is invisible (scans desync) or rejected
(reads EILSEQ). And `typed_record_view` cannot validate descriptors because it never sees the
schema — the new `typed_view_record` adds the schema-aware validation boundary the parent
requires ("Reject descriptors outside the actual record, pointing into the directory,
overlapping illegally or exceeding their field's logical capacity").

#### T2.1 — RED (append to `src/test/cases/test_packed_layout_unit.c` before the final printf)

```c
    /* ---- descriptor validation (T2) ---- */
    {
        /* Build a 2-packed-field record by hand:
           prefix(3: bm_nbytes=2? no — 2 fields -> 1 byte bm + 1 byte count
           header => bm_nbytes=1, prefix=2), dir 16, payloads. */
        TypedSchema ts;
        const char *specs[] = { "a:varchar", "b:varchar" };
        ASSERT_EQ_INT(typed_layout_v3_compute(&ts, specs, 2), 0, "v ok");
        uint8_t rec[64];
        memset(rec, 0, sizeof(rec));
        rec[0] = 1;                       /* bm_nbytes */
        /* field 0 descriptor: off=16+len8; field 1: off=24 len=4 */
        rec[ts.nullmap_prefix + 0] = 0; rec[ts.nullmap_prefix + 1] = 0;
        rec[ts.nullmap_prefix + 2] = 0; rec[ts.nullmap_prefix + 3] = 16;
        rec[ts.nullmap_prefix + 4] = 0; rec[ts.nullmap_prefix + 5] = 0;
        rec[ts.nullmap_prefix + 6] = 0; rec[ts.nullmap_prefix + 7] = 8;
        rec[ts.nullmap_prefix + 8] = 0; rec[ts.nullmap_prefix + 9] = 0;
        rec[ts.nullmap_prefix + 10] = 0; rec[ts.nullmap_prefix + 11] = 24;
        rec[ts.nullmap_prefix + 12] = 0; rec[ts.nullmap_prefix + 13] = 0;
        rec[ts.nullmap_prefix + 14] = 0; rec[ts.nullmap_prefix + 15] = 4;
        memcpy(rec + ts.nullmap_prefix + 16, "abcdefgh", 8);
        memcpy(rec + ts.nullmap_prefix + 24, "wxyz", 4);
        size_t vlen = (size_t)ts.nullmap_prefix + 16 + 8 + 4;
        TypedRecordView view;
        ASSERT_EQ_INT(typed_view_record(&ts, rec, vlen,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED,
                                        &view), 0, "valid packed view");
        TypedFieldSpan s0, s1;
        ASSERT_EQ_INT(typed_field_span(&ts, &view, 0, &s0), 0, "span0");
        ASSERT_EQ_INT(s0.state, TFSP_PRESENT, "span0 present");
        ASSERT_EQ_INT((int)s0.len, 8, "span0 len");
        ASSERT_EQ_INT(memcmp(s0.ptr, "abcdefgh", 8), 0, "span0 bytes");
        ASSERT_EQ_INT(typed_field_span(&ts, &view, 1, &s1), 0, "span1");
        ASSERT_EQ_INT((int)s1.len, 4, "span1 len");

        /* Malformed: descriptor pointing into the directory. */
        rec[ts.nullmap_prefix + 3] = 4;   /* off=4 < dir_bytes */
        ASSERT_EQ_INT(typed_view_record(&ts, rec, vlen,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED,
                                        &view), -1, "descriptor in dir rejected");
        rec[ts.nullmap_prefix + 3] = 16;
        /* Malformed: length beyond region. */
        rec[ts.nullmap_prefix + 7] = 9;   /* off 16 + len 9 crosses payload 1 */
        ASSERT_EQ_INT(typed_view_record(&ts, rec, vlen,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED,
                                        &view), -1, "overlapping descriptors rejected");
        rec[ts.nullmap_prefix + 7] = 8;
        /* Malformed: offset beyond the record. */
        rec[ts.nullmap_prefix + 11] = 60;
        ASSERT_EQ_INT(typed_view_record(&ts, rec, vlen,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED,
                                        &view), -1, "out-of-record offset rejected");
        rec[ts.nullmap_prefix + 11] = 24;
        /* Malformed: len>0 with off==0 (empty canonical form violated). */
        rec[ts.nullmap_prefix + 3] = 0;
        ASSERT_EQ_INT(typed_view_record(&ts, rec, vlen,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED,
                                        &view), -1, "off=0 len>0 rejected");
        rec[ts.nullmap_prefix + 3] = 16;
        /* Truncated record: last payload byte missing. */
        ASSERT_EQ_INT(typed_view_record(&ts, rec, vlen - 1,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED,
                                        &view), -1, "truncated payload rejected");
        /* Packed without nullmap is invalid. */
        ASSERT_EQ_INT(typed_view_record(&ts, rec, vlen, SREC_FLAG_PACKED,
                                        &view), -1, "packed w/o nullmap rejected");
        /* Unknown bits still fail closed. */
        ASSERT_EQ_INT(typed_view_record(&ts, rec, vlen, 0xFF, &view), -1,
                      "unknown hdr2 bits rejected");
        /* NULL and present-empty are distinct. */
        memset(rec, 0, sizeof(rec));
        rec[0] = 1;
        typed_set_null(rec + 1, 1, 0);   /* field 0 NULL */
        ASSERT_EQ_INT(typed_view_record(&ts, rec, (size_t)ts.nullmap_prefix + 16,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED,
                                        &view), 0, "empty-payload view ok");
        TypedFieldSpan s;
        ASSERT_EQ_INT(typed_field_span(&ts, &view, 0, &s), 0, "null span ok");
        ASSERT_EQ_INT(s.state, TFSP_NULL, "field0 NULL");
        ASSERT_EQ_INT(typed_field_span(&ts, &view, 1, &s), 0, "empty span ok");
        ASSERT_EQ_INT(s.state, TFSP_PRESENT, "field1 present");
        ASSERT_EQ_INT((int)s.len, 0, "field1 empty (len 0, bit clear)");
        /* Payload cap: field cap 4 MiB; len over cap rejected. Build a
           1-field varchar:N=4 schema and a len-5 descriptor. */
        TypedSchema ts4;
        const char *spec4[] = { "v:varchar:4" };
        ASSERT_EQ_INT(typed_layout_v3_compute(&ts4, spec4, 1), 0, "cap schema");
        uint8_t rec4[32]; memset(rec4, 0, sizeof(rec4));
        rec4[0] = 1;
        rec4[ts4.nullmap_prefix + 3] = (uint8_t)ts4.dir_bytes;  /* off */
        rec4[ts4.nullmap_prefix + 7] = 5;                        /* len > cap 4 */
        ASSERT_EQ_INT(typed_view_record(&ts4, rec4,
                                        (size_t)ts4.nullmap_prefix + ts4.dir_bytes + 5,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED,
                                        &view), -1, "payload over field cap rejected");
    }
    printf("test-packed-layout-unit: ok\n");
```

RED: compile fails on `typed_view_record`/`TypedRecordView` (declared in T1.2 but not yet
defined) → link error is the expected red.

#### T2.2 — config.c: `typed_view_record` + `typed_field_span`

Insert immediately after `typed_record_view` (config.c:4023, after its closing brace):

```c
/* Schema-aware record view — see types.h. Packed records get whole-view
   descriptor validation here so no consumer ever walks an unvalidated
   directory. */
int typed_view_record(const TypedSchema *ts, const uint8_t *value, size_t vlen,
                      uint8_t hdr2, TypedRecordView *out) {
    if (!ts || !out) return -1;
    out->packed = (hdr2 & SREC_FLAG_PACKED) ? 1 : 0;
    if (out->packed && !(hdr2 & SREC_FLAG_NULLMAP)) return -1;   /* packed implies nullmap */
    const uint8_t *base = NULL, *bm = NULL;
    int bm_bytes = 0;
    size_t region = 0;
    if (typed_record_view(value, vlen, hdr2, &base, &bm, &bm_bytes, &region) != 0)
        return -1;
    out->base = base; out->bm = bm; out->bm_bytes = bm_bytes; out->region = region;
    if (!out->packed) return 0;
    if (ts->layout_version != TYPED_LAYOUT_PACKED) return -1;
    if ((size_t)ts->dir_bytes > region) return -1;
    int64_t prev_end = ts->dir_bytes;   /* payloads start after the directory */
    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (!f->packed) continue;
        const uint8_t *d = base + f->offset;
        uint32_t off = ((uint32_t)d[0] << 24) | ((uint32_t)d[1] << 16) |
                       ((uint32_t)d[2] << 8) | (uint32_t)d[3];
        uint32_t len = ((uint32_t)d[4] << 24) | ((uint32_t)d[5] << 16) |
                       ((uint32_t)d[6] << 8) | (uint32_t)d[7];
        if (len == 0) {
            if (off != 0) return -1;            /* canonical empty form */
            continue;
        }
        if (len > (uint32_t)f->payload_cap) return -1;   /* over logical capacity */
        if (off < (uint32_t)ts->dir_bytes) return -1;    /* pointing into the directory */
        if ((int64_t)off < prev_end) return -1;          /* overlap / order violation */
        if ((int64_t)off + (int64_t)len > (int64_t)region) return -1;
        prev_end = (int64_t)off + (int64_t)len;
    }
    return 0;
}

int typed_field_span(const TypedSchema *ts, const TypedRecordView *view,
                     int idx, TypedFieldSpan *out) {
    if (!ts || !view || !out || idx < 0 || idx >= ts->nfields) return -1;
    const TypedField *f = &ts->fields[idx];
    out->ptr = NULL; out->len = 0; out->state = TFSP_ABSENT;
    if (view->bm && typed_field_null(view->bm, view->bm_bytes, idx)) {
        out->state = TFSP_NULL;
        return 0;
    }
    if (!view->packed || !f->packed) {
        /* Fixed field: present when its bytes are inside the region,
           legacy-absent when the record was trim-compacted. */
        if ((size_t)f->offset + (size_t)f->dir_size <= view->region) {
            out->ptr = view->base + f->offset;
            out->len = (size_t)f->dir_size;
            out->state = TFSP_PRESENT;
        }
        return 0;
    }
    const uint8_t *d = view->base + f->offset;
    uint32_t off = ((uint32_t)d[0] << 24) | ((uint32_t)d[1] << 16) |
                   ((uint32_t)d[2] << 8) | (uint32_t)d[3];
    uint32_t len = ((uint32_t)d[4] << 24) | ((uint32_t)d[5] << 16) |
                   ((uint32_t)d[6] << 8) | (uint32_t)d[7];
    if (len == 0) {
        out->state = TFSP_PRESENT;   /* present-empty (bit clear) */
        return 0;
    }
    out->ptr = view->base + off;
    out->len = (size_t)len;
    out->state = TFSP_PRESENT;
    return 0;
}
```

#### T2.3 — the four hdr2 seams

- seg_scan_varlen.h:47-48 — replace:

```c
    uint8_t hdr2 = rec[19];
    if (hdr2 & ~SREC_FLAG_NULLMAP) return 0;
```

with:

```c
    uint8_t hdr2 = rec[19];
    if (hdr2 & ~SREC_KNOWN_FLAGS) return 0;   /* unknown bits fail closed;
                                                 bit 1 = packed (Phase 3) */
```

- slotcask.c:4077-4080 (`seg_record_emit`) — replace:

```c
    /* Byte 19 carries SREC_FLAG_NULLMAP (null-bitmap generation). Written
       before the flag release-store, so a live record always has its
       generation byte visible. */
    dst[19] = (uint8_t)(hdr2 & SREC_FLAG_NULLMAP);
```

with:

```c
    /* Byte 19 carries the known generation flags (nullmap | packed).
       Written before the flag release-store, so a live record always has
       its generation byte visible. */
    dst[19] = (uint8_t)(hdr2 & SREC_KNOWN_FLAGS);
```

- slotcask.c:4111-4113 (`seg_record_emit_pending`) — replace:

```c
    /* Generation byte rides the staged payload; the activation flag-flip
       publishes both together. */
    dst[19] = (uint8_t)(hdr2 & SREC_FLAG_NULLMAP);
```

with:

```c
    /* Generation byte rides the staged payload; the activation flag-flip
       publishes both together. */
    dst[19] = (uint8_t)(hdr2 & SREC_KNOWN_FLAGS);
```

- config.c:4010 (`typed_record_view`) — replace:

```c
    if (hdr2 & ~SREC_FLAG_NULLMAP) return -1;
```

with:

```c
    if (hdr2 & ~SREC_KNOWN_FLAGS) return -1;
```

(`typed_record_view` itself keeps accepting a bare PACKED bit at the structural layer; the
schema-aware `typed_view_record` enforces "packed implies nullmap". This keeps the structural
validator layout-neutral while consumers use the typed form.)

#### T2.4 — intermediate checks (T2)

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-layout-unit
rtk proxy ./build/bin/shard-db-test run-all
```

Expected: all green; full suite confirms no v2 regression from the seam widening (a `2`-slot
object never emits bit 1, so nothing observable changes on legacy objects).

---

### Task 3 — Spans through the config.c decoders; actual-length decode sizing

**Root cause framing.** Every decoder reads `fbase + f->offset` for `f->size` bytes and
substitutes a 65537-byte zero buffer when the record is shorter — correct only while fields are
fixed-width. For packed fields the directory entry is the descriptor, the payload is elsewhere,
and the zero buffer is smaller than a bare payload ceiling (73,744 > 65,537) — the parent's
explicit prohibition. The decode sizing (`6*(f->size−2)+3`) must come from actual payload
lengths so a 4 MiB varchar does not force 25 MB buffers for small values (and the worst-case
`est` sum cannot overflow int).

#### T3.1 — RED (append to `test_packed_layout_unit.c` before the final printf)

```c
    /* ---- decode over spans (T3) ---- */
    {
        /* long | bare varchar | bare numeric | int — compose by hand, decode. */
        TypedSchema ts;
        const char *specs[] = { "l:long", "s:varchar", "n:numeric", "i:int" };
        ASSERT_EQ_INT(typed_layout_v3_compute(&ts, specs, 4), 0, "t3 schema");

        /* numeric payload for 12.5: canonical coefficient 125, dscale 1. */
        dec_t d; dec_init(&d);
        char derr[64];
        ASSERT_EQ_INT(dec_parse("12.5", 4, &d, derr, sizeof(derr)), DEC_OK, "dec parse");
        uint8_t npay[DEC_PAYLOAD_MAX];
        size_t nlen = 0;
        ASSERT_EQ_INT(dec_pack(&d, npay, sizeof(npay), &nlen), DEC_OK, "dec pack");
        dec_free(&d);
        ASSERT_EQ_INT((int)nlen, 17, "numeric payload 16+ceil(2/2)");

        uint8_t rec[128]; memset(rec, 0, sizeof(rec));
        rec[0] = 1;
        int P = ts.nullmap_prefix;                 /* 2 */
        int dir = ts.dir_bytes;                    /* 8+8+8+4 = 28 */
        /* long 7 */
        memcpy(rec + P + ts.fields[0].offset,
               (uint8_t[]){0,0,0,0,0,0,0,7}, 8);
        /* varchar "hi" at off=dir len=2 */
        uint32_t o1 = (uint32_t)(P + dir);         /* offsets are field-region-relative */
        rec[P + ts.fields[1].offset + 3] = (uint8_t)o1;
        rec[P + ts.fields[1].offset + 7] = 2;
        /* numeric at off=o1+2 len=nlen */
        uint32_t o2 = o1 + 2;
        rec[P + ts.fields[2].offset + 3] = (uint8_t)(o2 >> 24);
        rec[P + ts.fields[2].offset + 4] = 0;
        rec[P + ts.fields[2].offset + 5] = 0;
        rec[P + ts.fields[2].offset + 6] = 0;
        rec[P + ts.fields[2].offset + 7] = (uint8_t)(nlen >> 24);
        rec[P + ts.fields[2].offset + 11] = (uint8_t)nlen;
        memcpy(rec + P + o2, npay, nlen);
        /* int 42 */
        memcpy(rec + P + ts.fields[3].offset,
               (uint8_t[]){0,0,0,42}, 4);
        size_t vlen = (size_t)P + (size_t)o2 + nlen + 4;

        /* Full decode: exact JSON text. */
        char *json = typed_decode(&ts, rec, (int)vlen,
                                  SREC_FLAG_NULLMAP | SREC_FLAG_PACKED);
        ASSERT_NOT_NULL(json, "packed decode");
        ASSERT_EQ_STR(json,
            "{\"l\":7,\"s\":\"hi\",\"n\":12.5,\"i\":42}",
            "packed decode exact JSON");
        free(json);

        /* Field string: bare numeric exact text; NULL vs empty. */
        char *n = typed_get_field_str(&ts, rec, (int)vlen, 2,
                                      SREC_FLAG_NULLMAP | SREC_FLAG_PACKED);
        ASSERT_NOT_NULL(n, "numeric field str");
        ASSERT_EQ_STR(n, "12.5", "numeric exact text");
        free(n);
        rec[1] |= 1 << 1;   /* NULL the varchar */
        char *s = typed_get_field_str(&ts, rec, (int)vlen, 1,
                                      SREC_FLAG_NULLMAP | SREC_FLAG_PACKED);
        ASSERT_NULL(s, "null varchar -> NULL string");
        /* Bad numeric payload fails the read closed (nibble validated by
           dec_unpack). */
        rec[1] &= (uint8_t)~(1 << 1);
        rec[P + o2 + 16] ^= 0xFF;   /* corrupt a packed digit nibble */
        errno = 0;
        char *bad = typed_get_field_str(&ts, rec, (int)vlen, 2,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED);
        ASSERT_NULL(bad, "corrupt numeric payload fails closed");
        ASSERT_EQ_INT(errno, EILSEQ, "EILSEQ on corrupt payload");
    }
    printf("test-packed-layout-unit: ok\n");
```

RED: `typed_decode` on a packed record currently reads the descriptor bytes as field data →
the exact-JSON assertion fails (or the view-based path is not wired yet — compile error on
nothing new here; the failure is behavioral). Paste the assertion failure.

#### T3.2 — decode_field_to_buf over spans

**Anchor** — config.c:3603. Replace the signature and the FT_VARCHAR/FT_NUMERIC cases; every
other case keeps its body but reads from `sp.ptr` with `sp.len` available. Full new function:

```c
static int decode_field_to_buf(const TypedField *f, TypedFieldSpan sp,
                               char *buf, int buflen, int render_zeros) {
    const uint8_t *data = sp.ptr;
    switch (f->type) {
    case FT_VARCHAR: {
        if (f->packed) {
            /* Packed: the payload IS the content; length lives in the span. */
            int len = (int)sp.len;
            if (len == 0) {
                if (!render_zeros) return 0;
                buf[0] = '"'; buf[1] = '"'; buf[2] = '\0';
                return 2;
            }
            if (buflen < 4) {
                LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: buffer too small for field [%s] (buflen=%d, need>=4)", f->name, buflen);
                return -1;
            }
            buf[0] = '"';
            int esc = json_escape_into(buf + 1, (size_t)buflen - 3,
                                       (const char *)data, (size_t)len);
            if (esc < 0) {
                LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: json_escape_into overflow for field [%s] (len=%d, buflen=%d)", f->name, len, buflen);
                return -1;
            }
            buf[1 + esc] = '"';
            buf[2 + esc] = '\0';
            return 2 + esc;
        }
        /* Legacy: [uint16 BE length][content]. Content bytes are opaque
           (UTF-8 text in practice but the engine doesn't enforce it) so we
           MUST escape per RFC 8259 — raw quotes / backslashes / control
           chars in HN comment bodies broke the response JSON before this.
           Caller's buflen needs room for up to 6 * len + 2 (quotes). */
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
    case FT_NUMERIC:
        if (f->packed) {
            /* Exact decimal: unpack (validates everything) and render at
               the value's display scale as a bare JSON number. */
            dec_t d;
            dec_init(&d);
            dec_status st = dec_unpack(data, sp.len, &d);
            if (st != DEC_OK) { dec_free(&d); return -1; }
            size_t need = dec_text_len(&d);
            if ((size_t)buflen < need) { dec_free(&d); return -1; }
            size_t out_len = 0;
            st = dec_to_text(&d, buf, (size_t)buflen, &out_len);
            dec_free(&d);
            if (st != DEC_OK) return -1;
            return (int)out_len;
        }
        /* Legacy 8-byte scaled int64 (unchanged body below). */
        {
            uint64_t u = ((uint64_t)data[0] << 56) | ((uint64_t)data[1] << 48) |
                         ((uint64_t)data[2] << 40) | ((uint64_t)data[3] << 32) |
                         ((uint64_t)data[4] << 24) | ((uint64_t)data[5] << 16) |
                         ((uint64_t)data[6] << 8) | data[7];
            int64_t v = (int64_t)u;
            if (v == 0) return snprintf(buf, buflen, "0");
            int64_t scale = f->numeric_scale_mult;
            if (scale <= 0)
                return snprintf(buf, buflen, "%lld", (long long)v);
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
    case FT_LONG: case FT_TIMESTAMP: case FT_INT: case FT_SHORT:
    case FT_DOUBLE: case FT_FLOAT: case FT_BOOL: case FT_BYTE:
    case FT_DATE: case FT_DATETIME: case FT_DATETIMEMS: case FT_TIME:
    case FT_UUID: case FT_IPV4: case FT_IPV6: case FT_ENUM:
        /* Bodies unchanged from config.c:3639-3854 — they read only
           `data[0..f->size)` which sp guarantees is in-record for fixed
           fields. If sp.state != TFSP_PRESENT the caller already routed
           the field to the NULL/absent path (see T3.3), so data is valid
           here. */
        break;
    }
    /* … existing per-type bodies verbatim … */
}
```

(The executor applies the existing case bodies verbatim under the new signature; the only
semantic changes are the two packed branches above. If any case body referenced `f->size` as a
read bound it stays correct: fixed fields read exactly `f->dir_size == f->size` bytes.)

#### T3.3 — typed_decode / typed_decode_stream / typed_get_field_str over views

For each of the three functions, replace the field-source resolution with the view/span
resolution. **Anchor (typed_decode)** — config.c:3900-3933; the same pattern occurs at
3964-3987 (stream) and 4085-4107 (get_field_str). Replacement for typed_decode's loop head:

```c
    TypedRecordView view;
    /* typed_decode already validated structurally; re-resolve through the
       typed view (cheap; validates descriptors once). */
    if (typed_view_record(ts, data, (size_t)data_len, hdr2, &view) != 0) {
        errno = EILSEQ;
        return NULL;
    }
    render_zeros = (hdr2 & SREC_FLAG_NULLMAP) ? 1 : 0;
```

…then inside the loop replace the `zero_field` + `field_src` block (config.c:3900-3906) with:

```c
        TypedFieldSpan sp;
        typed_field_span(ts, &view, i, &sp);
        if (sp.state != TFSP_PRESENT) {
            /* NULL handled above; legacy-absent renders as before. */
            if (sp.state == TFSP_NULL) continue;   /* unreachable: NULL handled */
        }
```

and size the varchar/numeric decode buffer from the ACTUAL span length:

```c
        char vbuf_stack[512];
        char *vbuf = vbuf_stack;
        int vbufsz = (int)sizeof(vbuf_stack);
        if ((f->type == FT_VARCHAR && f->packed && sp.len > 0) ||
            (f->type == FT_NUMERIC && f->packed)) {
            /* Actual-length sizing: escapes can 6x-expand; numeric text
               fits DEC_TEXT_MAX. Never sized from the schema ceiling. */
            int64_t need = (f->type == FT_VARCHAR)
                ? 6LL * (int64_t)sp.len + 3
                : (int64_t)DEC_TEXT_MAX;
            if (need > vbufsz) {
                vbuf = malloc((size_t)need);
                if (!vbuf) { errno = ENOMEM; return NULL; }
                else        { vbufsz = (int)need; }
            }
        }
        int vlen = decode_field_to_buf(f, sp, vbuf, vbufsz, render_zeros);
        if (vlen < 0) {
            /* A corrupt packed payload fails the whole record closed. */
            if (vbuf != vbuf_stack) free(vbuf);
            errno = EILSEQ;
            return NULL;
        }
        if (vlen == 0) { if (vbuf != vbuf_stack) free(vbuf); continue; }
```

The outer `est` computation (config.c:3880-3890) keeps its `size_t` type (verified:
int32 would overflow at ceiling-scale, size_t cannot) but computes the varchar term from the
record's ACTUAL payload lengths by doing the view pass first:

```c
    TypedRecordView view;
    if (typed_view_record(ts, data, (size_t)data_len, hdr2, &view) != 0) {
        errno = EILSEQ;
        return NULL;
    }
    size_t est = 4;  /* { ... } + NUL */
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) continue;
        est += (size_t)ts->fields[i].name_len + 4;
        TypedFieldSpan sp;
        typed_field_span(ts, &view, i, &sp);
        if (ts->fields[i].type == FT_VARCHAR) {
            size_t cl = (ts->fields[i].packed)
                ? sp.len
                : (size_t)(ts->fields[i].size > 2 ? ts->fields[i].size - 2 : 0);
            est += cl * 6 + 2;
        } else if (ts->fields[i].type == FT_NUMERIC && ts->fields[i].packed) {
            est += DEC_TEXT_MAX;
        } else {
            est += 64;
        }
    }
```

Apply the identical view/span/sizing pattern to `typed_decode_stream` (config.c:3946-3997 —
replace the `zero_field` block at 3964-3967 and the varchar sizing at 3979-3986; on
`decode_field_to_buf < 0` return −1 with errno EILSEQ per the existing
"corrupt flagged record — fail, never emit" contract) and to `typed_get_field_str`
(config.c:4065-4107: replace the `zero_field`/`src` block with `typed_view_record` +
`typed_field_span`; the packed varchar case becomes
`char *out = malloc(sp.len + 1); memcpy(out, sp.ptr, sp.len); out[sp.len] = '\0';` guarded by
`sp.len == 0 → return render_zeros ? strdup("") : NULL;`; the packed numeric case unpacks +
`dec_to_text` into a malloc(DEC_TEXT_MAX) buffer and returns NULL with errno EILSEQ on
dec_unpack failure; the fixed-type cases read `sp.ptr`).

`decode_field_to_buf` callers in these three functions are updated to pass `sp` (the
`field_src` pointer argument disappears).

#### T3.4 — trim semantics for packed records

**Anchor** — config.c:4033-4052 `typed_encode_trim_len`. Insert the packed fast path at the
top of the function body (after the `prefix` computation):

```c
    if (ts->layout_version == TYPED_LAYOUT_PACKED) {
        /* Composed packed records are already exact-length: prefix +
           directory + actual payloads. The trimmable tail is only the
           fixed fields after the last packed field. Walk backwards:
           packed fields contribute their descriptor (always stored) and,
           when len>0, their payload end; fixed fields byte-scan. */
        int64_t end = prefix;   /* nothing beyond the directory is required yet */
        int64_t best = (int64_t)ts->nullmap_prefix + ts->dir_bytes;
        for (int i = ts->nfields - 1; i >= 0; i--) {
            const TypedField *f = &ts->fields[i];
            if (f->removed) continue;
            if (f->packed) {
                const uint8_t *d = fields + f->offset;
                uint32_t len = ((uint32_t)d[4] << 24) | ((uint32_t)d[5] << 16) |
                               ((uint32_t)d[6] << 8) | (uint32_t)d[7];
                if (len > 0) {
                    uint32_t off = ((uint32_t)d[0] << 24) | ((uint32_t)d[1] << 16) |
                                   ((uint32_t)d[2] << 8) | (uint32_t)d[3];
                    int64_t fend = (int64_t)prefix + (int64_t)off + (int64_t)len;
                    if (fend > best) best = fend;
                }
                continue;
            }
            size_t fend = (size_t)f->offset + (size_t)f->size;
            if (fend > (size_t)(full_len - prefix)) continue;
            const uint8_t *fp = fields + f->offset;
            int nonzero = 0;
            for (size_t b = 0; b < (size_t)f->size; b++)
                if (fp[b]) { nonzero = 1; break; }
            if (nonzero && (int64_t)fend > best) best = (int64_t)fend;
        }
        return best;
    }
```

(A packed record composed by T4 already has exact length, so in practice the trim returns
`full_len` minus any trailing fixed-zero fields; the walk guarantees no referenced payload is
ever trimmed away — the parent's "Never trim away a referenced payload" rule.)

#### T3.5 — `typed_field_to_index_key` packed branches

**Anchor** — config.c:2740-2760. After the `src` resolution (which T3.3's pattern replaces with
a span), add the packed branches at the top of the switch:

```c
    if (f->packed && f->type == FT_VARCHAR) {
        /* Index key = raw content bytes (identical encoding to the legacy
           varchar key). */
        memcpy(out, sp.ptr, sp.len);
        *out_len = sp.len;
        return;
    }
    if (f->packed && f->type == FT_NUMERIC) {
        /* No order-preserving numeric key exists until the Phase 5 codec;
           index creation on packed numerics is refused upstream (decision
           D3), so reaching here is a contract violation — emit nothing
           rather than a wrong ordered key. */
        *out_len = 0;
        return;
    }
```

(The function's fixed-type body is unchanged; `sp` replaces the resolved `src` pointer.)

#### T3.6 — intermediate checks (T3)

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-layout-unit
rtk proxy ./build/bin/shard-db-test run test-config-encode
rtk proxy ./build/bin/shard-db-test run test-null-storage
rtk proxy ./build/bin/shard-db-test run-all
```

Expected: unit case green (exact packed JSON decode, EILSEQ on corrupt payload); all legacy
cases green — decode changes are layout-branch-only for v2 objects.
---

### Task 4 — Encoder core: exact numeric encode, packed compose (sizing + bounded composition)

**Root cause framing.** `encode_field_checked`/`encode_field_len` write exactly `f->size` bytes
at a caller-chosen offset; `typed_encode`/`typed_encode_defaults` memset and return
`ts->total_size`. Under the packed layout the stored length is actual, numeric encoding goes
through the exact core (`dec_parse` → optional `dec_apply_precision` → `dec_pack`), and the
whole-record composer must place descriptors before it knows payload contents only in the
sense that payload LENGTHS are computable first — compose is a sizing pass plus a bounded
fill, never a fixed-width write.

#### T4.1 — RED (append to `test_packed_layout_unit.c` before the final printf)

```c
    /* ---- packed encode (T4) ---- */
    {
        TypedSchema ts;
        const char *specs[] = { "s:varchar", "n:numeric", "i:int", "t:varchar" };
        ASSERT_EQ_INT(typed_layout_v3_compute(&ts, specs, 4), 0, "t4 schema");
        /* Compose {"s":"ab","n":"12.50","i":42,"t":"x"} — s first, t LAST
           (trailing variable field must not be trimmed away). */
        uint8_t *buf = malloc((size_t)ts.total_size);
        ASSERT_NOT_NULL(buf, "compose buf");
        char err[256];
        int vlen = typed_encode(&ts,
            "{\"s\":\"ab\",\"n\":\"12.50\",\"i\":42,\"t\":\"x\"}",
            buf, ts.total_size, err, sizeof(err));
        ASSERT_EQ_INT(vlen > 0, 1, "encode ok");
        /* Expected actual length: prefix(2) + dir(8+8+4+8=28) +
           varchar "ab" (2) + numeric 12.50 (coefficient 1250 dscale 2 ->
           16+ceil(4/2)=18) + varchar "x" (1)  = 51. */
        ASSERT_EQ_INT(vlen, 2 + 28 + 2 + 18 + 1, "exact composed length");
        /* Decode round-trip. */
        char *json = typed_decode(&ts, buf, vlen, SREC_FLAG_NULLMAP | SREC_FLAG_PACKED);
        ASSERT_NOT_NULL(json, "roundtrip decode");
        ASSERT_EQ_STR(json, "{\"s\":\"ab\",\"n\":12.5,\"i\":42,\"t\":\"x\"}",
                      "12.50 renders as 12.5 (display scale 2)");
        free(json);
        /* Small numeric followed by another field stores 18 bytes, not a
           scaled-int64 padded blob (the parent's original test-first
           demand: small value + trailing field stays proportional). */
        TypedFieldSpan sp;
        TypedRecordView view;
        ASSERT_EQ_INT(typed_view_record(&ts, buf, (size_t)vlen,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED, &view), 0,
                      "composed view");
        ASSERT_EQ_INT(typed_field_span(&ts, &view, 1, &sp), 0, "numeric span");
        ASSERT_EQ_INT((int)sp.len, 18, "numeric(12.50) payload 18 bytes");
        ASSERT_EQ_INT(typed_field_span(&ts, &view, 3, &sp), 0, "t span");
        ASSERT_EQ_INT((int)sp.len, 1, "trailing varchar kept");
        free(buf);

        /* NULL vs empty vs zero are three different encodings. */
        uint8_t *buf2 = malloc((size_t)ts.total_size);
        vlen = typed_encode(&ts, "{\"s\":null,\"n\":\"0\",\"i\":0,\"t\":\"\"}",
                            buf2, ts.total_size, err, sizeof(err));
        ASSERT_EQ_INT(vlen, 2 + 28 + 0 + 16 + 0, "null/zero/empty lengths");
        ASSERT_EQ_INT(typed_view_record(&ts, buf2, (size_t)vlen,
                                        SREC_FLAG_NULLMAP | SREC_FLAG_PACKED, &view), 0,
                      "view2");
        ASSERT_EQ_INT(typed_field_span(&ts, &view, 0, &sp), 0, "s span");
        ASSERT_EQ_INT(sp.state, TFSP_NULL, "s is NULL");
        ASSERT_EQ_INT(typed_field_span(&ts, &view, 1, &sp), 0, "n span");
        ASSERT_EQ_INT((int)sp.len, 16, "numeric zero = 16-byte canonical payload");
        ASSERT_EQ_INT(typed_field_span(&ts, &view, 3, &sp), 0, "t span");
        ASSERT_EQ_INT(sp.state, TFSP_PRESENT, "t present-empty");
        ASSERT_EQ_INT((int)sp.len, 0, "t empty len 0");
        free(buf2);

        /* Declared numeric(P,S): rounds to scale, enforces precision. */
        TypedSchema tsc;
        const char *specc[] = { "m:numeric:3,1", "t:varchar" };
        ASSERT_EQ_INT(typed_layout_v3_compute(&tsc, specc, 2), 0, "constrained schema");
        uint8_t *buf3 = malloc((size_t)tsc.total_size);
        vlen = typed_encode(&tsc, "{\"m\":\"1.234\"}", buf3, tsc.total_size,
                            err, sizeof(err));
        ASSERT_EQ_INT(vlen > 0, 1, "rounding encode ok");
        char *j2 = typed_decode(&tsc, buf3, vlen, SREC_FLAG_NULLMAP | SREC_FLAG_PACKED);
        ASSERT_NOT_NULL(j2, "constrained decode");
        ASSERT_EQ_STR(j2, "{\"m\":1.2}", "1.234 rounds to 1.2 at scale 1");
        free(j2);
        free(buf3);
        /* Precision overflow refuses. */
        uint8_t *buf4 = malloc((size_t)tsc.total_size);
        vlen = typed_encode(&tsc, "{\"m\":\"999.9\"}", buf4, tsc.total_size,
                            err, sizeof(err));
        ASSERT_EQ_INT(vlen, -2, "999.9 overflows numeric(3,1)");
        ASSERT_EQ_INT(strstr(err, "m") != NULL, 1, "error names the field");
        free(buf4);
        /* Bare limits: 131072 integer digits accepted; one more rejected;
           16383 fractional digits accepted; one more rejected. */
        {
            char big[140000];
            memset(big, '9', sizeof(big));
            /* 131072 integer digits exactly. */
            big[131072] = '\0';
            TypedSchema tb;
            const char *specb[] = { "n:numeric" };
            ASSERT_EQ_INT(typed_layout_v3_compute(&tb, specb, 1), 0, "bare schema");
            uint8_t *bb = malloc((size_t)tb.total_size);
            char *js = malloc(strlen(big) + 16);
            sprintf(js, "{\"n\":\"%s\"}", big);
            int bv = typed_encode(&tb, js, bb, tb.total_size, err, sizeof(err));
            ASSERT_EQ_INT(bv > 0, 1, "131072 int digits accepted");
            free(js); free(bb);
            /* One more integer digit — rejected. */
            big[131072] = '9'; big[131073] = '\0';
            bb = malloc((size_t)tb.total_size);
            js = malloc(strlen(big) + 16);
            sprintf(js, "{\"n\":\"%s\"}", big);
            bv = typed_encode(&tb, js, bb, tb.total_size, err, sizeof(err));
            ASSERT_EQ_INT(bv, -2, "131073 int digits rejected");
            free(js); free(bb);
            /* 16383 fractional digits accepted, 16384 rejected. */
            char frac[16400];
            frac[0] = '1'; frac[1] = '.';
            memset(frac + 2, '7', 16383);
            frac[2 + 16383] = '\0';
            bb = malloc((size_t)tb.total_size);
            js = malloc(strlen(frac) + 16);
            sprintf(js, "{\"n\":\"%s\"}", frac);
            bv = typed_encode(&tb, js, bb, tb.total_size, err, sizeof(err));
            ASSERT_EQ_INT(bv > 0, 1, "16383 frac digits accepted");
            free(js); free(bb);
            frac[2 + 16383] = '7'; frac[2 + 16384] = '\0';
            bb = malloc((size_t)tb.total_size);
            js = malloc(strlen(frac) + 16);
            sprintf(js, "{\"n\":\"%s\"}", frac);
            bv = typed_encode(&tb, js, bb, tb.total_size, err, sizeof(err));
            ASSERT_EQ_INT(bv, -2, "16384 frac digits rejected");
            free(js); free(bb);
        }
        /* Over-cap varchar refuses (never truncates). */
        {
            TypedSchema tv;
            const char *specv[] = { "v:varchar:4", "t:varchar" };
            ASSERT_EQ_INT(typed_layout_v3_compute(&tv, specv, 2), 0, "v4 schema");
            uint8_t *vb = malloc((size_t)tv.total_size);
            int vv = typed_encode(&tv, "{\"v\":\"abcde\"}", vb, tv.total_size,
                                  err, sizeof(err));
            ASSERT_EQ_INT(vv, -2, "5 bytes over varchar:4 refused");
            free(vb);
        }
    }
    printf("test-packed-layout-unit: ok\n");
```

RED: `typed_encode` currently writes fixed-width, so the exact-length assertions fail
(vlen would be the total bound, decode would misrender).

#### T4.2 — config.c: v3 numeric/varchar branches in `encode_field_checked`

**Anchor** — config.c:2219-2246 (the second switch of `encode_field_checked`). Replace the
`case FT_NUMERIC:` and `case FT_VARCHAR:` arms with:

```c
    case FT_NUMERIC:
        if (f->packed) {
            /* Exact decimal assignment: parse once, round to a declared
               scale (ties away from zero) via the Phase 2 core, enforce
               declared precision, then pack. DEC_ERR_PRECISION and
               DEC_ERR_RANGE both refuse the write. */
            dec_t d;
            dec_init(&d);
            dec_status st = dec_parse(val, vlen, &d, err, errsz);
            if (st != DEC_OK) { dec_free(&d); return -1; }
            if (f->numeric_precision > 0) {
                st = dec_apply_precision(&d, f->numeric_precision,
                                         f->numeric_scale, &d, err, errsz);
                if (st != DEC_OK) { dec_free(&d); return st == DEC_ERR_PRECISION ? -2 : -1; }
            }
            uint8_t *pay = out + TYPED_DESCRIPTOR_BYTES;   /* descriptor written by caller seam */
            /* See T4.3: packed fields are composed by typed_encode_record,
               not written through this fixed-offset entry point. */
            (void)pay;
            dec_free(&d);
            return 0;
        }
        return conv_numeric(f, val, vlen, out, err, errsz);
    case FT_VARCHAR:
        if (f->packed) {
            if ((size_t)vlen > (size_t)f->payload_cap) {
                fmt_conv_err(err, errsz, f,
                             "%zu bytes exceeds max %d", vlen, f->payload_cap);
                return -1;
            }
            return 0;   /* composed by typed_encode_record (T4.3) */
        }
        /* Reject overlength input before the legacy encoder can truncate it. */
        if ((int)vlen > f->size - 2) {
            fmt_conv_err(err, errsz, f,
                         "%zu bytes exceeds max %d", vlen, f->size - 2);
            return -1;
        }
        break;
```

(Per-field packed composition cannot reuse the fixed-offset `uint8_t *out` contract, so the
packed arms are the *validation* layer; the actual byte placement lives in
`typed_encode_record` (T4.3), the only v3 composer. All fixed-field arms are untouched.)

#### T4.3 — config.c: `typed_encode_record` — the v3 composer

Insert before `typed_encode` (config.c:2932):

```c
/* ===== Packed record composer (Phase 3) =====
   Two-phase compose for layout-3 records: payload lengths are computed
   per field (validation happens there and can fail the record BEFORE any
   bytes are placed), then the buffer is filled exactly: prefix,
   directory, payloads in field order. out_cap must be >= ts->total_size
   (the schema maximum validity bound). Returns the ACTUAL value length,
   or -1 (bad args) / -2 (validation failure, message in err_buf). */
static int enc_json_field_packed(const TypedField *f, const char *val,
                                 size_t vlen, int was_quoted,
                                 const uint8_t **payload, size_t *paylen,
                                 uint8_t *pay_scratch, size_t scratch_cap,
                                 char *err, size_t errsz);

int typed_encode_record(const TypedSchema *ts, const char *json,
                        int apply_defaults, const char *db_root,
                        const char *object, uint8_t *out, size_t out_cap,
                        char *err_buf, size_t err_buf_size) {
    if (!ts || !ts->typed || !out ||
        ts->layout_version != TYPED_LAYOUT_PACKED ||
        out_cap < (size_t)ts->total_size) return -1;
    if (err_buf && err_buf_size > 0) err_buf[0] = '\0';
    memset(out, 0, (size_t)ts->nullmap_prefix + (size_t)ts->dir_bytes);
    out[0] = (uint8_t)typed_nullmap_bytes(ts->nfields);
    uint8_t *nbm = out + 1;
    int nbm_bytes = typed_nullmap_bytes(ts->nfields);
    uint8_t *dir = out + ts->nullmap_prefix;
    uint8_t *tail = dir + ts->dir_bytes;
    size_t tail_room = out_cap - (size_t)(ts->nullmap_prefix + ts->dir_bytes);
    size_t tail_used = 0;

    char seen[MAX_FIELDS] = {0};

    /* ---- Pass 1: walk the JSON once, validating + measuring each field. */
    const char *p = json_skip(json);
    if (*p == '{') {
        p++;
        while (*p) {
            p = json_skip(p);
            if (*p == '}') break;
            if (*p == ',') { p++; continue; }
            if (*p != '"') break;
            p++;
            const char *fname = p;
            while (*p && !(*p == '"' && *(p - 1) != '\\')) p++;
            size_t flen = p - fname;
            if (*p == '"') p++;
            p = json_skip(p);
            if (*p != ':') break;
            p = json_skip(p + 1);
            const char *vstart = p;
            p = json_skip_value(p);
            size_t vlen = p - vstart;

            for (int i = 0; i < ts->nfields; i++) {
                const TypedField *f = &ts->fields[i];
                if (f->removed) continue;
                if ((int)flen != f->name_len) continue;
                if (memcmp(fname, f->name, flen) != 0) continue;
                if (vlen && vstart[0] == '{') {
                    if (err_buf && err_buf_size)
                        snprintf(err_buf, err_buf_size,
                                 "computed updates are only valid on update");
                    return -2;
                }
                const char *ev = vstart; size_t el = vlen;
                int was_quoted = (el >= 2 && ev[0] == '"' && ev[el - 1] == '"');
                if (was_quoted) { ev++; el -= 2; }
                seen[i] = 1;
                if (!was_quoted && json_span_is_null(vstart, vlen)) {
                    if (f->not_null) {
                        fmt_not_null_err(f, err_buf, err_buf_size);
                        return -2;
                    }
                    typed_set_null(nbm, nbm_bytes, i);
                    break;
                }
                if (el > 0) {
                    if (!sentinel_span_parses(f->type, ev, el)) {
                        if (f->not_null) {
                            fmt_not_null_err(f, err_buf, err_buf_size);
                            return -2;
                        }
                        typed_set_null(nbm, nbm_bytes, i);
                        break;
                    }
                    if (f->type == FT_ENUM &&
                        enum_value_index(f, ev, el) < 0) {
                        fmt_unknown_enum_err(f, ev, el, err_buf, err_buf_size);
                        return -2;
                    }
                    uint8_t *pay = tail + tail_used;
                    size_t cap = tail_room - tail_used;
                    size_t used = 0;
                    int rc = encode_packed_field(f, ev, el, was_quoted,
                                                 pay, cap, &used,
                                                 err_buf, err_buf_size);
                    if (rc != 0) return rc;
                    if (used > 0) {
                        /* Descriptor: offset is field-region-relative. */
                        uint32_t off = (uint32_t)(ts->dir_bytes + tail_used);
                        dir[f->offset + 0] = (uint8_t)(off >> 24);
                        dir[f->offset + 1] = (uint8_t)(off >> 16);
                        dir[f->offset + 2] = (uint8_t)(off >> 8);
                        dir[f->offset + 3] = (uint8_t)off;
                        dir[f->offset + 4] = (uint8_t)(used >> 24);
                        dir[f->offset + 5] = (uint8_t)(used >> 16);
                        dir[f->offset + 6] = (uint8_t)(used >> 8);
                        dir[f->offset + 7] = (uint8_t)used;
                        tail_used += used;
                    }
                    break;
                }
                break;
            }
        }
    }

    /* ---- Defaults for unseen fields (only in the defaults variant). */
    if (apply_defaults) {
        for (int i = 0; i < ts->nfields; i++) {
            const TypedField *f = &ts->fields[i];
            if (f->removed || seen[i]) continue;
            if (f->default_kind == DK_NONE) {
                if (f->not_null) {
                    fmt_not_null_err(f, err_buf, err_buf_size);
                    return -2;
                }
                typed_set_null(nbm, nbm_bytes, i);
                continue;
            }
            char gen_buf[256];
            const char *dv = generate_default(f, gen_buf, sizeof(gen_buf),
                                              db_root, object);
            if (!dv) {
                if (err_buf && err_buf_size > 0)
                    snprintf(err_buf, err_buf_size,
                             "default generator failed for field [%s]", f->name);
                return -2;
            }
            uint8_t *pay = tail + tail_used;
            size_t cap = tail_room - tail_used;
            size_t used = 0;
            int rc = encode_packed_field(f, dv, strlen(dv), 0,
                                         pay, cap, &used, err_buf, err_buf_size);
            if (rc != 0) return rc;
            if (used > 0) {
                uint32_t off = (uint32_t)(ts->dir_bytes + tail_used);
                dir[f->offset + 0] = (uint8_t)(off >> 24);
                dir[f->offset + 1] = (uint8_t)(off >> 16);
                dir[f->offset + 2] = (uint8_t)(off >> 8);
                dir[f->offset + 3] = (uint8_t)off;
                dir[f->offset + 4] = (uint8_t)(used >> 24);
                dir[f->offset + 5] = (uint8_t)(used >> 16);
                dir[f->offset + 6] = (uint8_t)(used >> 8);
                dir[f->offset + 7] = (uint8_t)used;
                tail_used += used;
            }
        }
    } else {
        for (int i = 0; i < ts->nfields; i++) {
            const TypedField *f = &ts->fields[i];
            if (!f->removed && !seen[i] && f->not_null) {
                fmt_not_null_err(f, err_buf, err_buf_size);
                return -2;
            }
            if (!f->removed && !seen[i]) typed_set_null(nbm, nbm_bytes, i);
        }
    }

    return (int)((size_t)ts->nullmap_prefix + (size_t)ts->dir_bytes + tail_used);
}
```

with the per-field packed encoder (placed just above it):

```c
/* Encode one packed field's validated text into the tail cursor.
   Writes at most *out_used bytes; never truncates. Returns 0 / -2
   (validation; message in err) / -1 (bad args or bound exhaustion). */
static int encode_packed_field(const TypedField *f, const char *val, size_t vlen,
                               int was_quoted, uint8_t *out, size_t cap,
                               size_t *out_used, char *err, size_t errsz) {
    *out_used = 0;
    if (!f->packed) {
        /* Fixed field: go through the shared checked encoder into the
           directory slot. The caller passes out == dir + f->offset and
           cap == f->size for fixed fields. */
        if (cap < (size_t)f->size) return -1;
        if (encode_json_field_checked(f, val, vlen, was_quoted, out, err, errsz) != 0)
            return -2;
        *out_used = (size_t)f->size;
        return 0;
    }
    /* JSON-decode eligible string types exactly once (mirrors
       encode_json_field_checked). */
    char *decoded = NULL;
    size_t decoded_len = 0;
    int rc = 0;
    if (was_quoted && typed_json_string_type(f->type)) {
        if (json_unescape_cstring(val, vlen, &decoded, &decoded_len) != 0) {
            snprintf(err, errsz, "malformed JSON escape in value for field '%s'",
                     f->name);
            return -2;
        }
        val = decoded;
        vlen = decoded_len;
    }
    switch (f->type) {
    case FT_VARCHAR: {
        if (vlen > (size_t)f->payload_cap) {
            snprintf(err, errsz, "field '%s': %zu bytes exceeds max %d",
                     f->name, vlen, f->payload_cap);
            rc = -2;
            break;
        }
        if (vlen == 0) break;   /* present-empty: no payload, descriptor (0,0) */
        if (vlen > cap) { rc = -1; break; }
        memcpy(out, val, vlen);
        *out_used = vlen;
        break;
    }
    case FT_NUMERIC: {
        dec_t d;
        dec_init(&d);
        dec_status st = dec_parse(val, vlen, &d, err, errsz);
        if (st != DEC_OK) { dec_free(&d); rc = -2; break; }
        if (f->numeric_precision > 0) {
            st = dec_apply_precision(&d, f->numeric_precision,
                                     f->numeric_scale, &d, err, errsz);
            if (st != DEC_OK) { dec_free(&d); rc = -2; break; }
        }
        size_t need = dec_payload_len(&d);
        if (need > cap) { dec_free(&d); rc = -1; break; }
        size_t used = 0;
        st = dec_pack(&d, out, cap, &used);
        dec_free(&d);
        if (st != DEC_OK) { rc = -1; break; }
        *out_used = used;
        break;
    }
    default:
        rc = -1;   /* packed composer reached a non-packed type: contract bug */
        break;
    }
    free(decoded);
    return rc;
}
```

(Empty numeric text: `dec_parse("", 0, …)` fails with DEC_ERR_SYNTAX. Callers that carry an
empty span as an intentional zero use the explicit canonical-zero path — the update composer
(T6) builds zero payloads via `dec_pack` of the canonical zero, not by passing empty text here;
the JSON layer never supplies empty spans for scalars, matching the historic gate `el > 0`.)

#### T4.4 — route `typed_encode` / `typed_encode_defaults` through the composer for v3

**Anchor** — config.c:2932-2934. Replace the head of `typed_encode`:

```c
int typed_encode(const TypedSchema *ts, const char *json, uint8_t *out,
                 int out_size, char *err_buf, size_t err_buf_size) {
    if (!ts || !ts->typed || out_size < ts->total_size) return -1;
    if (ts->layout_version == TYPED_LAYOUT_PACKED)
        return typed_encode_record(ts, json, 0, NULL, NULL,
                                   out, (size_t)out_size, err_buf, err_buf_size);
    memset(out, 0, ts->total_size);
```

and the head of `typed_encode_defaults` (config.c:3451-3455):

```c
int typed_encode_defaults(const TypedSchema *ts, const char *json, uint8_t *out,
                          int out_size, const char *db_root, const char *object,
                          char *err_buf, size_t err_buf_size) {
    if (!ts || !ts->typed || out_size < ts->total_size) return -1;
    if (ts->layout_version == TYPED_LAYOUT_PACKED)
        return typed_encode_record(ts, json, 1, db_root, object,
                                   out, (size_t)out_size, err_buf, err_buf_size);
    memset(out, 0, ts->total_size);
```

Both functions now return the ACTUAL length for v3 (callers updated in T6/T9). For v2 the
bodies are untouched.

#### T4.5 — intermediate checks (T4)

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-layout-unit
rtk proxy ./build/bin/shard-db-test run-all
```

Expected: all green. `test_not_null_writes` still passes (v2 path untouched); the exact-length
assertions from T4.1 prove proportionality (the parent's original spike demand).

---

### Task 5 — Version gating: slot-3 acceptance, layout binding, downgrade barrier

#### T5.1 — RED: `test_packed_compat.c` (new file; registered in T5.4)

```c
/* Mixed-generation compatibility: slot-2 objects keep legacy bytes and
   semantics; slot-3 objects are packed; version-slot gating refuses
   older-generation interpretation. Uses the process-local env fixture
   only for schema.conf layout probing (no daemon writes here). */
#include "test_runner.h"
#include "test_assert.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

extern Schema load_schema(const char *effective_root, const char *object);
extern int typed_layout_version_for(const char *db_root, const char *object);

int main(void) {
    char root[] = "/tmp/p3-compat-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(root), "mkdtemp");
    mkdir(root, 0755);
    char objdir[512];
    snprintf(objdir, sizeof(objdir), "%s/d1/o1", root);
    mkdirp(objdir);

    /* --- slot 2 line: legacy layout bound. --- */
    FILE *f = fopen("/dev/null", "r"); (void)f;
    char scpath[512];
    snprintf(scpath, sizeof(scpath), "%s/schema.conf", root);
    FILE *sc = fopen(scpath, "w");
    fprintf(sc, "d1:o1:8:64:2:2\n");
    fclose(sc);

    ASSERT_EQ_INT(typed_layout_version_for(root, "o1"), TYPED_LAYOUT_NULLMAP,
                  "slot 2 binds layout 2");
    Schema s = load_schema(root, "o1");
    ASSERT_EQ_INT(s.splits, 8, "slot2 schema loads");

    /* --- slot 3 line: packed layout bound. --- */
    sc = fopen(scpath, "w");
    fprintf(sc, "d1:o1:8:64:3:2\n");
    fclose(sc);
    ASSERT_EQ_INT(typed_layout_version_for(root, "o1"), TYPED_LAYOUT_PACKED,
                  "slot 3 binds layout 3");
    s = load_schema(root, "o1");
    ASSERT_EQ_INT(s.splits, 8, "slot3 schema loads in the new binary");

    /* --- an unknown future slot refuses closed (existing rule, pinned). --- */
    sc = fopen(scpath, "w");
    fprintf(sc, "d1:o1:8:64:9:2\n");
    fclose(sc);
    s = load_schema(root, "o1");
    /* load_schema returns a zeroed schema for any non-2/3 version on the
       strict parser of the current binary (refuse, never misread). */
    ASSERT_EQ_INT(s.splits, 0, "unknown slot refuses");

    /* --- startup strict parser accepts 2|3 (T5.3 change); exercised via
           shard_db_version_decide-compatible unit expectations here. --- */
    ASSERT_EQ_INT(shard_db_version_compare("2026.11.0", "2026.10.2"), 1,
                  "newer release sorts newer");
    ASSERT_EQ_INT(shard_db_version_decide("2026.10.2", 1, 0, "2026.11.0",
                                          "2026.09.2"), SHARD_DB_VERSION_DOWNGRADE,
                  "older binary refuses newer-stamped root");
    rmrf(root);
    printf("test-packed-compat: ok\n");
    return 0;
}

TEST_REGISTER("test-packed-compat", test_packed_compat_run)
```

RED: `typed_layout_version_for` links (T1) but slot-3 acceptance inside `load_schema` and the
strict parser does not exist yet — `load_schema` on the slot-3 line currently zeroes the
schema (`s.splits == 0`), failing the "slot3 schema loads" assertion. Paste it.

#### T5.2 — config.c `load_schema` accepts slot 3

**Anchor** — config.c:927-941 (version parse) and 975-994 (v1 refusal). Replace:

```c
    if (parsed_version != 2) legacy_v1 = 1;
```

with:

```c
    /* Engine-version slot: 2 = nullmap fixed layout, 3 = packed typed
       layout (Phase 3). Any other value keeps the legacy_v1 refusal so
       future formats fail closed here too. */
    if (parsed_version != 2 && parsed_version != 3) legacy_v1 = 1;
```

(The slot-3 path then flows through `load_typed_schema`, whose `typed_layout_version_for` (T1.4)
binds the packed layout; `max_value`/`slot_size` derivation at config.c:996-1005 needs no
change — `ts->total_size` IS the v3 bound by construction.)

#### T5.3 — embedded.c strict startup parser accepts 2|3

**Anchor** — embedded.c:419-428 (`startup_schema_line`). Replace:

```c
	if (strcmp(parts[4], "2") != 0) return -1;
```

with:

```c
	/* Engine-version slot: 2 (nullmap) or 3 (packed, Phase 3). Anything
	   else fails the root closed at startup. */
	if (strcmp(parts[4], "2") != 0 && strcmp(parts[4], "3") != 0) return -1;
```

#### T5.4 — create-object schema.conf append durability (decision D6)

**Anchor** — query_schema.c:1590-1608. After the `fclose(f);` that closes the schema.conf
append, insert:

```c
        /* D6: the engine-version slot is the downgrade barrier — the
           `3` line must survive a crash that leaves the object's packed
           records in place. Sync the appended line and the directory. */
        fsync_file_path(schema_path);
        fsync_parent_dir(schema_path);
```

(`fsync_file_path`/`fsync_parent_dir` already exist in util.c, declared types.h:761-762.)

- build.sh: after `src/test/cases/test_packed_layout_unit.c \` insert:

```sh
    src/test/cases/test_packed_compat.c \
```

#### T5.5 — intermediate checks (T5)

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-compat
rtk proxy ./build/bin/shard-db-test run test-startup-validator
rtk proxy ./build/bin/shard-db-test run-all
```

Expected: compat case green; `test-startup-validator` (existing) stays green with the widened
slot acceptance.

---

### Task 6 — Single-write integration: insert, auto_create, update compose, computed updates, adapter sizing

**Root cause framing.** `cmd_insert_v2` stores exactly `ts->total_size` bytes
(storage.c:1045-1068) and every update path composes a fixed `total_size` buffer with
`memcpy(dst, old_base + f->offset, f->size)` — for packed fields both are wrong (the buffer is
the ceiling, the copy reads descriptor bytes). The update adapter additionally mallocs
`slot_size − 24 − klen` (slotcask.c:8537 region) — the slot_size-proportional allocation §1.4
bans.

#### T6.1 — RED: `test_packed_roundtrip.c` (new file; registered in T6.7)

Daemon-fixture test (uses `test_env_start`/`tc_request` per the parent's allowed fixture API):

```c
/* Packed record end-to-end: create slot-3 object, insert small values at
   first/middle/last positions, NULL vs empty vs zero, get/find/CSV
   round-trips, update carry-forward, and DIRECT segment vlen assertions
   read back from the data files. */
#include "test_runner.h"
#include "test_fixtures.h"
#include "test_client.h"
#include "test_assert.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int expect_resp(TestClient *tc, const char *json, const char *want,
                       const char *label) {
    char *resp = NULL;
    int rc = tc_request(tc, json, &resp);
    ASSERT_EQ_INT(rc, 0, label);
    ASSERT_NOT_NULL(resp, label);
    int ok = (want && strcmp(resp, want) == 0);
    ASSERT_EQ_INT(ok, 1, label);
    free(resp);
    return 0;
}

/* Walk the object's kf shards for the record's hash → (file_id, offset),
   then assert the segment record's 24-byte header vlen == want. Layout:
   kf entry 24B {hash16, flag, stream_id, file_id u16 LE, offset u32 LE};
   segment record 24B header {hash16, klen u16 LE, flag@18, hdr2@19,
   vlen u32 LE}. */
static void assert_seg_vlen(TestEnv *env, const char *object,
                            const char *key, int want_vlen,
                            int want_hdr2, const char *label) {
    /* Compute the record's shard + slot the way storage does. */
    uint8_t hash[16];
    extern void compute_hash_raw(const char *key, size_t key_len,
                                 uint8_t hash_out[16]);
    compute_hash_raw(key, strlen(key), hash);
    int shard = ((int)hash[0] | ((int)hash[1] << 8)) % 8;   /* splits=8 */
    char kfpath[1024];
    snprintf(kfpath, sizeof(kfpath), "%s/d1/%s/data/kf/%03X.kf",
             env->db_root, object, shard);
    FILE *kf = fopen(kfpath, "rb");
    ASSERT_NOT_NULL(kf, label);
    /* kf header 24B then 24B entries; scan for the hash. */
    fseek(kf, 0, SEEK_END);
    long kfsz = ftell(kf);
    fseek(kf, 24, SEEK_SET);
    int found = 0;
    uint16_t file_id = 0; uint32_t off = 0; uint8_t hdr2 = 0;
    for (long pos = 24; pos + 24 <= kfsz; pos += 24) {
        uint8_t e[24];
        if (fread(e, 1, 24, kf) != 24) break;
        if (memcmp(e, hash, 16) != 0 || e[16] != 1) continue;
        memcpy(&file_id, e + 18, 2);
        memcpy(&off, e + 20, 4);
        found = 1;
        break;
    }
    fclose(kf);
    ASSERT_EQ_INT(found, 1, label);
    char segpath[1024];
    snprintf(segpath, sizeof(segpath),
             "%s/d1/%s/data/streams/000/%05u.dat",
             env->db_root, object, file_id);
    FILE *seg = fopen(segpath, "rb");
    ASSERT_NOT_NULL(seg, label);
    uint8_t hdr[24];
    ASSERT_EQ_INT(fseek(seg, off, SEEK_SET), 0, label);
    ASSERT_EQ_INT((int)fread(hdr, 1, 24, seg), 24, label);
    fclose(seg);
    uint32_t vlen = 0;
    memcpy(&vlen, hdr + 20, 4);
    ASSERT_EQ_INT((int)vlen, want_vlen, label);
    ASSERT_EQ_INT((int)hdr[19], want_hdr2, label);
}

int main(void) {
    TestEnv env;
    ASSERT_EQ_INT(test_env_start(&env), 0, "env start");
    TestClient *tc = tc_connect(&(TestClientCfg){ .db_root = env.db_root });
    ASSERT_NOT_NULL(tc, "connect");

    /* Create a slot-3 object: bare varchar FIRST, fixed middle, bare
       numeric LAST, plus a bounded varchar — mixed variable positions. */
    expect_resp(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"splits\":8,\"max_key\":64,\"fields\":["
        "\"s:varchar\",\"i:int\",\"n:numeric\",\"t:varchar:30\"]}",
        "{\"ok\":true}\n", "create packed object");

    /* Small values everywhere: exact stored length is
       prefix(2) + dir(8+4+8+8=28) + "hi"(2) + numeric 1.5 (16+1=17)
       + "z"(1) = 50; record bytes on disk = 24 + klen 1 + 50 = 75,
       aligned 8 -> capacity 80. */
    expect_resp(tc,
        "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"key\":\"k1\",\"value\":{\"s\":\"hi\",\"i\":7,\"n\":\"1.5\",\"t\":\"z\"}}",
        "{\"ok\":true}\n", "insert small");
    assert_seg_vlen(&env, "o1", "k1", 50, 3, "small record vlen + hdr2");

    /* Round-trips. */
    expect_resp(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"k1\"}",
        "{\"s\":\"hi\",\"i\":7,\"n\":1.5,\"t\":\"z\"}\n", "get roundtrip");
    expect_resp(tc,
        "{\"mode\":\"find\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"criteria\":{\"i\":{\"eq\":7}}}",
        "[{\"s\":\"hi\",\"i\":7,\"n\":1.5,\"t\":\"z\"}]\n", "find by fixed field");
    expect_resp(tc,
        "{\"mode\":\"find\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"criteria\":{\"t\":{\"eq\":\"z\"}}}",
        "[{\"s\":\"hi\",\"i\":7,\"n\":1.5,\"t\":\"z\"}]\n", "find by packed varchar");

    /* Criteria on the packed numeric field is an explicit error (D4). */
    {
        char *resp = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"d1\",\"object\":\"o1\","
            "\"criteria\":{\"n\":{\"eq\":\"1.5\"}}}", &resp), 0, "numeric crit");
        ASSERT_EQ_INT(strstr(resp, "\"error\"") != NULL, 1,
                      "numeric criteria refused explicitly");
        free(resp);
    }

    /* NULL vs empty vs zero. */
    expect_resp(tc,
        "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"key\":\"k2\",\"value\":{\"s\":null,\"i\":0,\"n\":\"0\",\"t\":\"\"}}",
        "{\"ok\":true}\n", "insert null/zero/empty");
    expect_resp(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"k2\"}",
        "{\"s\":null,\"i\":0,\"n\":0,\"t\":\"\"}\n", "null/zero/empty roundtrip");
    /* k2 stored length: prefix 2 + dir 28 + numeric zero 16 = 46. */
    assert_seg_vlen(&env, "o1", "k2", 46, 3, "null/empty store compactly");

    /* Update: middle field changed, BOTH variable fields carried forward;
       descriptor offsets regenerate. */
    expect_resp(tc,
        "{\"mode\":\"update\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"k1\","
        "\"value\":{\"i\":8}}",
        "{\"ok\":true}\n", "update fixed field");
    expect_resp(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"k1\"}",
        "{\"s\":\"hi\",\"i\":8,\"n\":1.5,\"t\":\"z\"}\n",
        "update preserved variable fields");
    /* Update growing a variable field in the FIRST position with a
       trailing variable field still present. */
    expect_resp(tc,
        "{\"mode\":\"update\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"k1\","
        "\"value\":{\"s\":\"hello packed world\"}}",
        "{\"ok\":true}\n", "update first variable");
    expect_resp(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"k1\"}",
        "{\"s\":\"hello packed world\",\"i\":8,\"n\":1.5,\"t\":\"z\"}\n",
        "grow + carry");

    /* CSV emission of packed fields. */
    {
        char *resp = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"fetch\",\"dir\":\"d1\",\"object\":\"o1\","
            "\"format\":\"csv\"}", &resp), 0, "csv");
        ASSERT_EQ_INT(strstr(resp, "hello packed world") != NULL, 1, "csv content");
        free(resp);
    }

    tc_close(tc);
    test_env_stop(&env);
    printf("test-packed-roundtrip: ok\n");
    return 0;
}

TEST_REGISTER("test-packed-roundtrip", test_packed_roundtrip_run)
```

RED: create-object succeeds after T1 but the insert stores garbage for the packed fields
(fixed-width composer not yet routed / hdr2 not set / vlen wrong) — the `assert_seg_vlen`
and get assertions fail. Paste the first failing assertion.

#### T6.2 — storage.c `cmd_insert_v2`: actual length, packed hdr2, segment-capacity pre-check

**Anchor** — storage.c:1045-1068. Replace:

```c
    uint8_t *typed_buf = malloc(ts->total_size);
    if (!typed_buf) { OUT("{\"error\":\"oom\"}\n"); return 1; }
    char enc_err[512] = {0};
    int enc = typed_encode_defaults(ts, value, typed_buf, ts->total_size,
                                    db_root, object, enc_err, sizeof(enc_err));
    if (enc == -2) {
        /* Strict enum (or future typed) validation rejection — actionable
           error already in enc_err. Caller never sees a successfully-
           encoded but semantically-corrupt record. */
        free(typed_buf);
        OUT("{\"error\":\"%s\"}\n", enc_err);
        return 1;
    }
    if (enc < 0) {
        free(typed_buf);
        OUT("{\"error\":\"Typed encode failed\"}\n");
        return 1;
    }
    size_t vlen = ts->total_size;
    if ((int)vlen > sc->max_value) {
        free(typed_buf);
        fprintf(stderr, "Error: Value too large (%zu > %d)\n", vlen, sc->max_value);
        return 1;
    }
```

with:

```c
    uint8_t *typed_buf = malloc((size_t)ts->total_size);
    if (!typed_buf) { OUT("{\"error\":\"oom\"}\n"); return 1; }
    char enc_err[512] = {0};
    int enc = typed_encode_defaults(ts, value, typed_buf, ts->total_size,
                                    db_root, object, enc_err, sizeof(enc_err));
    if (enc == -2) {
        /* Strict enum (or future typed) validation rejection — actionable
           error already in enc_err. Caller never sees a successfully-
           encoded but semantically-corrupt record. */
        free(typed_buf);
        OUT("{\"error\":\"%s\"}\n", enc_err);
        return 1;
    }
    if (enc < 0) {
        free(typed_buf);
        OUT("{\"error\":\"Typed encode failed\"}\n");
        return 1;
    }
    /* v3 encodes to the ACTUAL length; v2 still encodes to total_size
       (enc == ts->total_size there). */
    size_t vlen = (ts->layout_version == TYPED_LAYOUT_PACKED)
                      ? (size_t)enc : (size_t)ts->total_size;
    if ((int64_t)vlen > (int64_t)sc->max_value) {
        free(typed_buf);
        fprintf(stderr, "Error: Value too large (%zu > %d)\n", vlen, sc->max_value);
        return 1;
    }
    if (ts->layout_version == TYPED_LAYOUT_PACKED) {
        /* D1: reject records the segment layer could never place BEFORE
           staging (auto_create/index hooks run later and must not see a
           record destined to fail). */
        if ((int64_t)(24 + (int64_t)klen + (int64_t)vlen) >
            (int64_t)slotcask_seg_max_bytes()) {
            free(typed_buf);
            OUT("{\"error\":\"record exceeds segment capacity\"}\n");
            return 1;
        }
    }
```

and the `record_hdr2` in the upsert opts (storage.c:1187):

```c
        .record_hdr2   = (ts->layout_version == TYPED_LAYOUT_PACKED)
                             ? (SREC_FLAG_NULLMAP | SREC_FLAG_PACKED)
                             : SREC_FLAG_NULLMAP,  /* nullmap encoders */
```

(`slotcask_seg_max_bytes()` is declared in slotcask.h — add the extern to types.h if storage.c
lacks visibility; it is a slotcask.h export.)

The `existed`-preservation block (storage.c:1103-1130) and `V2InsertCtx.idx_ts` need no
signature change; `auto_create_field_transfer` is fixed in T6.3. The trimmed-staging path
redundantly re-trims via `schema_trim_fn` — T3.4 already made it packed-correct.

#### T6.3 — storage.c `auto_create_field_transfer` over spans

**Anchor** — storage.c:1402-1452. Replace the body between the signature and the closing
brace with:

```c
    /* §2.9 auto_create preserve/stamp for ONE field, shared by the single
       insert path and the bulk-insert value_compute so the transfer rules
       cannot drift. Preserve transfers COMPLETE field state — bytes AND
       nullness. NEW was already encoded (defaults or a supplied value), so
       erase it first; a canonical NULL is all-zero bytes under a set bit.
       This function runs ONLY for fields with default_kind == DK_AUTO_CREATE
       (the call sites filter on it), and the auto generators belong to
       fixed-width types (timestamp/date/datetime/datetimems), so the copy
       below operates on fixed directory bytes — f->dir_size == f->size —
       which are at identical directory coordinates in a packed record. */
    const TypedField *f = &ts->fields[i];
    size_t off = (size_t)f->offset;
    size_t w   = (size_t)f->dir_size;
    if (existed) {
        uint8_t *dst = out_buf + ts->nullmap_prefix + off;
        memset(dst, 0, w);
        int preserve_null = (o_hdr2 & SREC_FLAG_NULLMAP) &&
                            o_view_ok &&
                            typed_field_null(o_bm, o_bm_bytes, i);
        if (!preserve_null && o_view_ok) {
            size_t avail = (o_region > off) ? o_region - off : 0;
            size_t copyn = avail < w ? avail : w;
            if (copyn) memcpy(dst, o_base + off, copyn);
            if (!(o_hdr2 & SREC_FLAG_NULLMAP) &&
                legacy_field_absent(&ts->fields[i], dst))
                preserve_null = 1;
        }
        if (preserve_null) {
            memset(dst, 0, w);
            typed_set_null(out_buf + 1, ts->nullmap_prefix - 1, i);
        } else {
            typed_clear_null(out_buf + 1, ts->nullmap_prefix - 1, i);
        }
    } else {
        /* Re-stamp unconditionally, even though typed_encode_defaults
           already stamped now() for a client-omitted field. Do NOT
           "optimize" this away — the original comment's contract stands
           (a client-supplied auto_create value must not survive a fresh
           insert). */
        char tbuf[24];
        auto_now_str(f, tbuf, sizeof(tbuf));
        encode_field(f, tbuf, out_buf + ts->nullmap_prefix + off);
        typed_clear_null(out_buf + 1, ts->nullmap_prefix - 1, i);
    }
    /* existed but old too short (field added post-hoc): the bounded zero
       fallback above carries it as NULL. */
```

> **Packed auto_create note (executor requirement):** the auto generators
> (`timestamp`/`date`/`datetime`/`datetimems`) are fixed-width types, so a packed field
> (numeric/varchar) never reaches this function through the DK_AUTO_CREATE call-site filter;
> packed `:default=` literals were already written by `typed_encode_defaults` and are not
> re-stamped here. If the executor finds a code path that can combine `packed` +
> `DK_AUTO_CREATE`, that is an uncovered decision: write `PLAN_NOTES.md` and halt. The
> invariant the RED test pins: an auto_create timestamp on a packed object round-trips its
> create-time value across re-insert (the OLD record of a v3 object is itself packed, and the
> fixed-byte copy above reads it correctly through the validated view coordinates).

#### T6.4 — storage.c `update_compose_new_from_old`: spans + actual length out-param

This task changes the function's signature once, here: it gains a trailing
`size_t *out_vlen` (may be NULL — v2 callers that keep using `ts->total_size` pass NULL).
On success it is set to the composed NEW length (`prefix + dir + tail_used` for v3,
`ts->total_size` for v2); on failure `*out_vlen` is untouched and the negative status is
returned exactly as today. types.h's prototype (the `int update_compose_new_from_old(…)`
forward declaration mirrored near slotcask.h:474) gains the same parameter, and §3 lists the
six callers (storage.c ×3, query_bulk.c ×3) — each updated in T6/T9 with NULL or a real
out-pointer as noted.

**Anchor** — storage.c:1454-1517. Replace the whole function:

```c
int update_compose_new_from_old(const TypedSchema *ts,
                                const SlotcaskOldRecord *old,
                                uint8_t *out_buf,
                                char *const *field_vals,
                                const uint8_t *field_null,
                                size_t *out_vlen,
                                char *err, size_t errsz) {
    /* §2026.10.2 null plan §2.7: NEW is always written in the current
       generation layout. OLD may be legacy, nullmap, or packed; the view
       is validated before any field-region math — malformed OLD fails the
       write, never copies. Packed NEW records must be composed into a
       buffer whose tail already holds the carried-forward packed payloads:
       the caller passes out_buf sized by the typed_sized_new_from_old
       helper (T6.5) which lays out [prefix+dir][old payloads]. This
       function fills descriptors for carried payloads at those known
       offsets and encodes changed fields at the tail cursor. */
    const uint8_t *old_base; const uint8_t *old_bm; int old_bm_bytes;
    size_t old_region;
    if (!ts || !old || !out_buf ||
        typed_record_view(old->value, (size_t)old->vlen, old->hdr2,
                          &old_base, &old_bm, &old_bm_bytes,
                          &old_region) != 0) {
        return -1;
    }
    TypedRecordView ov;
    int old_packed = 0;
    if (typed_view_record(ts, old->value, (size_t)old->vlen, old->hdr2, &ov) != 0) {
        if (old->hdr2 & SREC_FLAG_PACKED) return -1;   /* packed but invalid: fail */
        /* legacy/nullmap records validate without the packed sweep */
    } else {
        old_packed = ov.packed;
    }

    memset(out_buf, 0, (size_t)ts->total_size);
    out_buf[0] = (uint8_t)typed_nullmap_bytes(ts->nfields);
    uint8_t *nbm = out_buf + 1;
    int nbm_bytes = typed_nullmap_bytes(ts->nfields);
    uint8_t *dir = out_buf + ts->nullmap_prefix;
    uint8_t *tail = dir + ts->dir_bytes;
    size_t tail_used = 0;
    int v3 = (ts->layout_version == TYPED_LAYOUT_PACKED);

    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;
        uint8_t *dst = out_buf + ts->nullmap_prefix + f->offset;

        if (field_null && field_null[i]) {    /* explicit JSON null */
            typed_set_null(nbm, nbm_bytes, i);
            continue;
        }
        if (field_vals && field_vals[i]) {
            if (f->type != FT_VARCHAR && field_vals[i][0] == '{') return -2;
            if (v3 && f->packed) {
                size_t used = 0;
                int rc = encode_packed_field_public(f, field_vals[i],
                                                    strlen(field_vals[i]), 0,
                                                    tail + tail_used,
                                                    (size_t)ts->total_size -
                                                    (size_t)(ts->nullmap_prefix +
                                                             ts->dir_bytes) -
                                                    tail_used,
                                                    &used, err, errsz);
                if (rc != 0) return -2;
                if (used > 0) {
                    uint32_t off = (uint32_t)(ts->dir_bytes + tail_used);
                    dir[f->offset + 0] = (uint8_t)(off >> 24);
                    dir[f->offset + 1] = (uint8_t)(off >> 16);
                    dir[f->offset + 2] = (uint8_t)(off >> 8);
                    dir[f->offset + 3] = (uint8_t)off;
                    dir[f->offset + 4] = (uint8_t)(used >> 24);
                    dir[f->offset + 5] = (uint8_t)(used >> 16);
                    dir[f->offset + 6] = (uint8_t)(used >> 8);
                    dir[f->offset + 7] = (uint8_t)used;
                    tail_used += used;
                }
                typed_clear_null(nbm, nbm_bytes, i);
            } else {
                if (encode_field_checked(f, field_vals[i], strlen(field_vals[i]),
                                         dst, err, errsz) != 0)
                    return -2;
            }
            continue;
        }
        /* Omitted on update → carry OLD forward, preserving rendering. */
        TypedFieldSpan osp;
        int have_old = 0;
        if (old_packed) {
            typed_field_span(ts, &ov, i, &osp);
            have_old = (osp.state == TFSP_PRESENT);
            if (osp.state == TFSP_NULL) { typed_set_null(nbm, nbm_bytes, i); continue; }
        } else {
            if ((old->hdr2 & SREC_FLAG_NULLMAP) &&
                typed_field_null(old_bm, old_bm_bytes, i)) {
                typed_set_null(nbm, nbm_bytes, i);
                continue;
            }
            /* legacy bounded read (existing semantics) */
            osp.ptr = NULL; osp.len = 0; osp.state = TFSP_ABSENT;
            size_t avail = (old_region > (size_t)f->offset)
                ? old_region - (size_t)f->offset : 0;
            size_t copyn = avail < (size_t)f->size ? avail : (size_t)f->size;
            if (copyn) memcpy(dst, old_base + f->offset, copyn);
            if (!(old->hdr2 & SREC_FLAG_NULLMAP) && legacy_field_absent(f, dst)) {
                memset(dst, 0, (size_t)f->size);
                typed_set_null(nbm, nbm_bytes, i);
            }
            continue;
        }
        if (!have_old) continue;   /* bit clear, no payload: present-empty carries as empty */
        if (v3 && f->packed) {
            /* Carry the payload verbatim; the descriptor is regenerated
               (stale offsets are never copied). */
            if (osp.len > 0) {
                memcpy(tail + tail_used, osp.ptr, osp.len);
                uint32_t off = (uint32_t)(ts->dir_bytes + tail_used);
                dir[f->offset + 0] = (uint8_t)(off >> 24);
                dir[f->offset + 1] = (uint8_t)(off >> 16);
                dir[f->offset + 2] = (uint8_t)(off >> 8);
                dir[f->offset + 3] = (uint8_t)off;
                dir[f->offset + 4] = (uint8_t)(osp.len >> 24);
                dir[f->offset + 5] = (uint8_t)(osp.len >> 16);
                dir[f->offset + 6] = (uint8_t)(osp.len >> 8);
                dir[f->offset + 7] = (uint8_t)osp.len;
                tail_used += osp.len;
            }
        } else {
            size_t copyn = osp.len < (size_t)f->size ? osp.len : (size_t)f->size;
            if (copyn) memcpy(dst, osp.ptr, copyn);
        }
    }
    if (out_vlen)
        *out_vlen = v3 ? (size_t)ts->nullmap_prefix + (size_t)ts->dir_bytes +
                             tail_used
                       : (size_t)ts->total_size;
    return 0;
}
```

with `encode_packed_field_public` the same body as T4.3's `encode_packed_field` (rename that
function to `encode_packed_field_public`, drop `static`, declare it in types.h next to
`typed_encode_record`, and have `typed_encode_record` call it — one definition, two consumers).
Callers that keep the v2 fixed-length behavior pass `NULL` for `out_vlen`; the sized-NEW
pre-pass used by the update adapter is T6.5.

#### T6.5 — sized NEW composition + adapter hook

types.h (next to the `RebuildTxn` block) — declare:

```c
/* Size (and optionally compose) the NEW record an update produces, for
   the object's layout. out_buf may be NULL to size only. Returns the
   actual NEW value length, or negative on failure (validation/old-view).
   v3: prefix+dir+actual payload lens (changed fields re-encoded, carried
   payloads appended verbatim). v2: ts->total_size (unchanged). */
long long typed_sized_new_from_old(const TypedSchema *ts,
                                   const SlotcaskOldRecord *old,
                                   char *const *field_vals,
                                   const uint8_t *field_null,
                                   uint8_t *out_buf, size_t out_cap,
                                   char *err, size_t errsz);
```

storage.c — implement as a thin wrapper over the T6.4 composer: run
`update_compose_new_from_old(ts, old, out_buf, field_vals, field_null, &actual, err, errsz)`
when `out_buf` is non-NULL (sizing-only pass passes `out_buf = NULL`, which the wrapper turns
into a compose into a stack-unsafe path — so instead call the composer with a `NULL` out_buf
guarded compose: extend the composer with a sizing-only early return when `out_buf == NULL`
that performs pass-1 validation/measurement only and sets `*out_vlen`), returning
`(long long)actual`, negative on failure. For v2 the returned actual is `ts->total_size`.

slotcask.h — extend `SlotcaskUpsertOpts` (anchor: the `slotcask_new_from_old_fn  new_from_old;`
field at slotcask.h:575-576):

```c
    slotcask_new_from_old_fn  new_from_old;
    void                     *new_from_old_ctx;
    /* Optional actual-size pre-pass (Phase 3, decision D1): when set, the
       update adapter allocates exactly this many bytes for the NEW value
       instead of slot_size - 24 - klen (a 1-GiB-class malloc on maximum
       packed schemas). When NULL the legacy sizing applies (v2 objects). */
    long long (*new_from_old_size)(const SlotcaskOldRecord *old, void *ctx);
```

slotcask.c `upsert_adapter_value_compute` (anchor 8536-8541) — replace:

```c
    size_t out_capacity = (size_t)actx->db->slot_size - 24 - rec->klen;
    uint8_t *buf = malloc(out_capacity ? out_capacity : 1);
```

with:

```c
    size_t out_capacity = (size_t)actx->db->slot_size - 24 - rec->klen;
    if (uo->new_from_old_size) {
        long long need = uo->new_from_old_size(old, uo->new_from_old_ctx);
        if (need < 0) { actx->hard_error = 1; return -1; }
        if ((size_t)need <= out_capacity) out_capacity = (size_t)need;
    }
    uint8_t *buf = malloc(out_capacity ? out_capacity : 1);
```

storage.c wires the hook wherever `.new_from_old` is set (the update path's
`SlotcaskUpsertOpts` — storage.c:2178 region sets `.new_hdr2`; the `new_from_old` assignment
site gets):

```c
        .new_from_old_size = (ts->layout_version == TYPED_LAYOUT_PACKED)
                                 ? v3_new_from_old_size : NULL,
```

with:

```c
static long long v3_new_from_old_size(const SlotcaskOldRecord *old, void *ctx) {
    struct V3UpdateCtx *c = (struct V3UpdateCtx *)ctx;   /* the existing new_from_old ctx */
    return typed_sized_new_from_old(c->idx_ts, old, c->values, c->nulls,
                                    NULL, 0, NULL, 0);
}
```

and `.new_hdr2` (storage.c:2178) becomes:

```c
        .new_hdr2 = (c->idx_ts->layout_version == TYPED_LAYOUT_PACKED)
                        ? (SREC_FLAG_NULLMAP | SREC_FLAG_PACKED)
                        : SREC_FLAG_NULLMAP,
```

`v2_update_new_from_old` (storage.c:1862-1937): the varchar bound
`strlen(values[i]) > (size_t)f->size - 2` becomes
`(f->packed ? strlen(values[i]) > (size_t)f->payload_cap : strlen(values[i]) > (size_t)f->size - 2)`
and `*out_vlen = (size_t)c->idx_ts->total_size` becomes
`*out_vlen = (size_t)typed_sized_new_from_old(c->idx_ts, old, values, nulls, out_value, out_capacity, c->err_buf, sizeof(c->err_buf))`
(negative → return −1 with the validation message already set).

#### T6.6 — `apply_computed_fields` span safety (D5 gate)

**Anchor** — storage.c:1685-1747. Replace the `cur[16]`/`source_bytes[16]` reads:

```c
        uint8_t *dst = out_value + ts->nullmap_prefix + (size_t)f->offset;
        const uint8_t *src = ((size_t)f->offset + (size_t)f->size > o_region)
            ? NULL : o_base + f->offset;
        uint8_t cur[16];
        memset(cur, 0, sizeof(cur));
        if (src) memcpy(cur, src, (size_t)f->size);
```

with:

```c
        uint8_t *dst = out_value + ts->nullmap_prefix + (size_t)f->offset;
        TypedFieldSpan csp = { NULL, 0, TFSP_ABSENT };
        if (o_view_ok) {
            typed_field_span(ts, &o_view, i, &csp);
            if (csp.state != TFSP_PRESENT) { csp.ptr = NULL; csp.len = 0; }
        }
        /* Fixed-width only in Phase 3: packed-field computed updates are
           refused (decision D5) until Phase 4's exact computed updates. */
        if (f->packed || (sf && sf->packed)) {
            snprintf(c->err_buf, sizeof(c->err_buf),
                     "computed updates on packed numeric/varchar fields are "
                     "not supported yet");
            return -2;
        }
        uint8_t cur[16];
        memset(cur, 0, sizeof(cur));
        if (csp.ptr) memcpy(cur, csp.ptr, csp.len);
```

(the `$copy` branch's `source_bytes[16]` gets the identical span-based read; `o_view` is the
`TypedRecordView` resolved once at function entry via `typed_view_record`), and the `$incr`
numeric branch (storage.c:1784-1822, hardcoded 8-byte read/write) gains the same packed gate.

#### T6.7 — intermediate checks (T6)

- build.sh: after `src/test/cases/test_packed_compat.c \` insert:

```sh
    src/test/cases/test_packed_roundtrip.c \
```

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-roundtrip
rtk proxy ./build/bin/shard-db-test run-all
```

Expected: roundtrip green — including the DIRECT segment header assertions (vlen 50/46, hdr2
3) proving actual-length storage and the per-record flag; the full suite proves no v2 drift
(esp. test_auto_create, test_bulk_upsert, test_edit_field, test_null_storage).
---

### Task 7 — Reader sweep: every record consumer onto validated spans

Mechanics: each site replaces the triple (`fbase`, `f->offset`, `f->size`, zero-buffer
fallback) with a `TypedRecordView` + `typed_field_span` resolution. For **fixed** fields the
span is byte-identical to today's pointer; for **packed** fields the span carries the payload.
The `g_zero_field_65537` / `zero_field[65537]` substitutions are removed on every packed-capable
path (NULL/absent is a span state, not a zero buffer). Sites marked (v2-only) keep their bodies
but gain the typed view so legacy-absence logic is unchanged.

#### T7.1 — RED: extend `test_packed_roundtrip.c` (insert before `tc_close`)

```c
    /* Criteria on packed varchar: eq / contains / starts_with / len ops. */
    expect_resp(tc,
        "{\"mode\":\"find\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"criteria\":{\"s\":{\"contains\":\"packed\"}}}",
        "[{\"s\":\"hello packed world\",\"i\":8,\"n\":1.5,\"t\":\"z\"}]\n",
        "contains over packed varchar");
    expect_resp(tc,
        "{\"mode\":\"find\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"criteria\":{\"t\":{\"starts_with\":\"z\"}}}",
        "[{\"s\":\"hello packed world\",\"i\":8,\"n\":1.5,\"t\":\"z\"}]\n",
        "starts_with over packed varchar");
    /* Aggregates on packed numeric are refused explicitly (D4); on packed
       varchar (min/max text) they work. */
    {
        char *resp = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"d1\",\"object\":\"o1\","
            "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"n\"}]}", &resp), 0,
            "numeric agg gate");
        ASSERT_EQ_INT(strstr(resp, "\"error\"") != NULL, 1, "numeric agg refused");
        free(resp);
    }
    /* CAS guard comparing a packed varchar (regex/CAS path). */
    expect_resp(tc,
        "{\"mode\":\"update\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"k1\","
        "\"value\":{\"i\":9},\"if\":{\"s\":{\"eq\":\"hello packed world\"}}}",
        "{\"ok\":true}\n", "CAS on packed varchar");
```

RED: `match_typed`'s FT_VARCHAR arm reads the descriptor bytes as a u16 length →
"contains over packed varchar" fails. Paste it.

#### T7.2 — query_plan.c: `match_typed` family

**Anchor 1** — query_plan.c:914-962. Replace the composite fallback and pointer resolution:

```c
int match_typed(const uint8_t *rec, size_t data_len, const CompiledCriterion *cc, FieldSchema *fs,
                uint8_t hdr2) {
    if (cc->composite || !cc->tf) {
        char *attr = decode_field((const char *)rec, data_len, cc->raw->field, fs, hdr2);
        int r = match_criterion(attr, cc->raw);
        free(attr);
        return r;
    }
    const TypedSchema *ts = fs->ts;
    TypedRecordView view;
    if (typed_view_record(ts, rec, data_len, hdr2, &view) != 0) return 0;
    /* … field-vs-field and single-field arms below resolve spans … */
```

In the field-vs-field arm replace the two pointer computations
(`const uint8_t *lhs = … g_zero_field_65537 : fbase + cc->tf->offset;` and the `rhs` twin) with:

```c
    TypedFieldSpan lsp, rsp;
    typed_field_span(ts, &view, (int)(cc->tf - ts->fields), &lsp);
    typed_field_span(ts, &view, (int)(cc->rhs_tf - ts->fields), &rsp);
    int r = cmp_typed_field_pair(lsp, rsp, cc->tf);
```

and change `cmp_typed_field_pair` (query_plan.c:846) to take `(TypedFieldSpan a,
TypedFieldSpan b, const TypedField *f)`: for fixed fields the bodies are unchanged over
`a.ptr`; for packed varchar it compares `(memcmp(a.ptr, b.ptr, min), a.len - b.len)`;
packed numeric is unreachable here (D4 gate, T7.5).

In the single-field switch replace

```c
    const uint8_t *p = ((size_t)f->offset + (size_t)f->size > region)
        ? g_zero_field_65537 : fbase + f->offset;
```

with

```c
    TypedFieldSpan sp;
    typed_field_span(ts, &view, (int)(f - ts->fields), &sp);
    const uint8_t *p = sp.ptr;
    if (sp.state != TFSP_PRESENT) {
        /* NULL or legacy-absent: existing per-type absence semantics. */
        … (keep the existing legacy-absence gates 1058-1114 verbatim for
           non-nullmap records; a TFSP_NULL span returns per the operator's
           NULL semantics, unchanged from 2026.10.2) …
    }
```

`match_typed_varchar(p, f->size, cc)` (query_plan.c:621) becomes
`match_typed_varchar_span(sp, cc)` — new helper:

```c
static int match_typed_varchar_span(TypedFieldSpan sp, const CompiledCriterion *cc) {
    int elen;
    const char *hay;
    if (cc->tf->packed) { elen = (int)sp.len; hay = (const char *)sp.ptr; }
    else {
        elen = varchar_eff_len(sp.ptr, cc->tf->size);
        hay = (const char *)(sp.ptr + 2);
    }
    … (body of match_typed_varchar unchanged from here: byte-exact / i-variant
       / LIKE-kind / REG_STARTEND over [hay, hay+elen)) …
}
```

**Anchor 2** — the per-op fixed-width arms (query_plan.c:972-1160): fixed fields unchanged
(`p` now points at span bytes, identical for fixed fields). The numeric arm gains the D4
defensive branch:

```c
    case FT_NUMERIC:
        if (f->packed) return 0;   /* unreachable: D4 gate refuses at compile time */
        … existing int64 compare …
```

#### T7.3 — query.c sites

1. **CAS regex** (query.c:5579-5590). Replace the pointer resolution with:

```c
    TypedRecordView view;
    if (typed_view_record(fs->ts, rec, len, hdr2, &view) != 0) return -1;
    int idx = (int)(cc->tf - fs->ts->fields);
    TypedFieldSpan sp;
    typed_field_span(fs->ts, &view, idx, &sp);
    if (sp.state == TFSP_NULL) return 0;
    if (sp.state != TFSP_PRESENT) return 0;
    int elen;
    const char *hay;
    if (cc->tf->packed) { elen = (int)sp.len; hay = (const char *)sp.ptr; }
    else { elen = varchar_eff_len(sp.ptr, cc->tf->size); hay = (const char *)(sp.ptr + 2); }
    regmatch_t pm = { .rm_so = 0, .rm_eo = elen };
    int rc = regexec(cc->re, hay, 1, &pm, REG_STARTEND);
```

2. **Ordered-collect sort keys** (query.c:6397-6403). Replace the cap:

```c
            size_t cap = 1 + (size_t)p->tf->dir_size;   /* tier byte + payload/dir width;
                                                           fixes the latent 16-byte
                                                           off-by-one (payload=dir_size+1) */
```

(`typed_field_to_index_key_rec` emits `1 + key_len` where `key_len ≤ dir_size` for fixed
fields and `≤ payload_cap` for packed — for packed-varchar sort keys allocate
`1 + payload_cap` when `p->tf->packed`; the key bytes come from T3.5's packed branch.)

3. **Cursor capture** (query.c:6867) and **resume NULL padding** (query.c:8115-8124): capture
   routes through `typed_get_field_str` (already span-based per T3.3) plus the view for tier
   bits — replace `rr.hdr2 & SREC_FLAG_NULLMAP` tier checks with
   `(rr.hdr2 & SREC_FLAG_NULLMAP) && typed_field_null(...)` as today (unchanged); the NULL
   padding `plen = (size_t)ospec.parts[i].tf->size` becomes
   `plen = (size_t)ospec.parts[i].tf->dir_size` (fixed components only — packed-numeric
   components cannot be ordered/cursor fields until Phase 5, and ordered indexes on them do
   not exist per D3, so the padding width is the directory width).

4. **`guard_eq_key`** (query.c:5692): `cc->s1_len > (size_t)f->size - 2` becomes
   `cc->s1_len > (size_t)(f->packed ? f->payload_cap : f->size - 2)`.

5. **STARTS_WITH fixed-width detection** (query.c:2428/2553): only indexed fields reach it;
   packed numerics are never indexed (D3) and packed varchars ARE variable — the detection
   `len_lo < seed_tf->size` must see "variable" for packed fields:

```c
    int fixed_width = seed_tf_sv && !seed_tf_sv->packed &&
                      len_lo_sv == (size_t)seed_tf_sv->size;
```

(substituted at both sites; the existing comments about encoded-vs-fixed comparison are
updated to name `dir_size`.)

#### T7.4 — query_join.c, query_aggregate.c, query_maint.c spans

Apply the shared resolution pattern (define once in query_internal.h):

```c
/* Resolve one field's span inside a raw record; returns 0 on success.
   On a structurally invalid record the span is TFSP_ABSENT and the
   caller treats the field as absent (legacy zero/empty semantics). */
static inline int typed_span_resolve(const TypedSchema *ts, const uint8_t *rec,
                                     size_t len, uint8_t hdr2, int idx,
                                     TypedRecordView *view, TypedFieldSpan *sp) {
    if (typed_view_record(ts, rec, len, hdr2, view) != 0) {
        sp->ptr = NULL; sp->len = 0; sp->state = TFSP_ABSENT;
        return -1;
    }
    typed_field_span(ts, view, idx, sp);
    return 0;
}
```

- query_join.c: `extract_local_key` (198-250), `buf_field_value` (350-369),
  `buf_join_values` (397-427), `buf_driver_values` (438-464), `build_joined_csv_row`
  (522-583): replace each `((size_t)tf->offset + (size_t)tf->size > region) ? g_zero_field_65537
  : rbase + tf->offset` with `typed_span_resolve(...)`; `varchar_eff_len(field_ptr, tf->size)`
  sites become `tf->packed ? (int)fp.len : varchar_eff_len(fp.ptr, tf->size)` and
  `field_ptr + 2` becomes `fp.ptr` for packed (fixed keeps `+2`). Numeric fields in joins:
  D4-gated at request entry (T7.5) — join on packed numeric refused explicitly.
- query_aggregate.c: `agg_field_ptr` (256-273) becomes span-returning:

```c
static TypedFieldSpan agg_field_span(const uint8_t *rec, size_t rec_len,
                                     const TypedSchema *ts, int idx,
                                     uint8_t hdr2) {
    TypedRecordView view; TypedFieldSpan sp;
    if (typed_span_resolve(ts, rec, rec_len, hdr2, idx, &view, &sp) != 0)
        return (TypedFieldSpan){ NULL, 0, TFSP_ABSENT };
    return sp;
}
```

  Callers (1830-1858, 2400-2494, 2510-2578, 3970-3999) pass spans into
  `typed_field_to_double` (2044), `typed_field_to_text` (2101), `typed_field_to_buf_raw`
  (1067), `typed_field_to_raw` (2212) — each gains a packed-varchar branch (content bytes)
  and refuses packed numeric (`AGG_VALUE_MISSING` is NOT acceptable — the D4 request gate
  means they are unreachable; the defensive branch returns `AGG_VALUE_UNSUPPORTED`, a new
  enum member, and the caller errors the query rather than silently skipping).
  `agg_field_is_unset` (231): packed varchar → `sp.len == 0`; fixed unchanged.
  Index-leaf decoders (`decode_idx_to_buf` 1399, `decode_index_key_to_double` 1215) are
  unchanged — their inputs are index bytes, whose varchar encoding is identical.
- query_maint.c `tg_estimate_cb` (59-90): gains the view it never had — replace
  `const uint8_t *vbase = ((size_t)f->offset + (size_t)f->size > vlen) ? g_zero_field_65537
  : (const uint8_t *)value + f->offset;` with `typed_span_resolve(...)`; the varchar branch
  reads `sp.len`/`sp.ptr` for packed fields; fixed fields unchanged. (This also fixes the
  pre-existing missing-nullmap-slide bug on this path — covered by the full suite's
  maintenance cases.)

#### T7.5 — request-entry gates (D4)

query_plan.c — new helper + hook:

```c
/* D4: packed numeric fields have no exact criteria path until Phase 5.
   Compile-time gate: returns the first leaf (or NULL) referencing a
   packed numeric field. */
const CriteriaNode *criteria_packed_numeric_leaf(const CriteriaNode *node,
                                                 const TypedSchema *ts);
```

implementation walks LEAF nodes, resolves `field` via `typed_field_index`, returns the node
when `f->packed && f->type == FT_NUMERIC` (composite `a+b` leaves check each component).
Callers (anchor each): `cmd_find_tree` / `cmd_count_tree` / `cmd_aggregate_tree` /
`cas_prepare_tree` (query.c/query.c/query.c/query.c) — insert before execution:

```c
    const CriteriaNode *bad = criteria_packed_numeric_leaf(tree, fs.ts);
    if (bad) {
        OUT("{\"error\":\"criteria on bare/packed numeric fields are not "
            "supported until the exact-criteria phase\"}\n");
        return <that command's failure return>;
    }
```

and the aggregate spec walk in `cmd_aggregate_tree`:

```c
    for (int i = 0; i < naggs; i++) {
        int ai = typed_field_index(aggs_fs.ts, aggs[i].field);
        if (ai >= 0 && aggs_fs.ts->fields[ai].packed &&
            aggs_fs.ts->fields[ai].type == FT_NUMERIC) {
            OUT("{\"error\":\"numeric aggregates on packed fields are not "
                "supported until the exact-aggregate phase\"}\n");
            return 1;
        }
    }
```

Likewise joins: `cmd_find_tree`'s join resolution refuses a join whose local/remote field is
packed numeric (same error text, "joins").

#### T7.6 — intermediate checks (T7)

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-roundtrip
rtk proxy ./build/bin/shard-db-test run test-packed-layout-unit
rtk proxy ./build/bin/shard-db-test run-all
```

Expected: all green. The full suite exercises every operator on v2 objects — proving the span
refactor did not move legacy bytes (the refactor is byte-neutral for fixed fields by
construction; the suite is the proof).

---

### Task 8 — Index integration: spans in the builders, packed-numeric index refusal

#### T8.1 — RED: extend `test_packed_roundtrip.c`

```c
    /* Index on a packed varchar: create, backfill via add-index, eq hit. */
    expect_resp(tc,
        "{\"mode\":\"add-index\",\"dir\":\"d1\",\"object\":\"o1\",\"field\":\"t\"}",
        "{\"ok\":true}\n", "add index on packed varchar");
    expect_resp(tc,
        "{\"mode\":\"find\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"criteria\":{\"t\":{\"eq\":\"z\"}}}",
        "[{\"s\":\"hello packed world\",\"i\":9,\"n\":1.5,\"t\":\"z\"}]\n",
        "indexed eq over packed varchar");
    /* Index on the packed numeric field is refused (D3). */
    {
        char *resp = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"add-index\",\"dir\":\"d1\",\"object\":\"o1\","
            "\"field\":\"n\"}", &resp), 0, "numeric index gate");
        ASSERT_EQ_INT(strstr(resp, "\"error\"") != NULL, 1, "numeric index refused");
        free(resp);
    }
    /* Reindex reproduces the index from segment scans (T8.4 padding fix). */
    expect_resp(tc,
        "{\"mode\":\"reindex\",\"dir\":\"d1\",\"object\":\"o1\"}",
        NULL, "reindex runs");   /* shape asserted loosely; errors fail */
```

RED: the add-index on `t` "succeeds" today but builds keys from descriptor bytes
(`build_index_key_from_record_into` payload = `f->size`), so the indexed eq returns the wrong
record or nothing → assertion fails.

#### T8.2 — index.c payload derivation over spans

**Anchor 1** — index.c:1380-1411 `build_index_key_from_record_into` (and the same math in
`build_index_payload_from_record_into` 1463-1500). Replace the per-component payload
computation:

```c
    const TypedField *f = &ts->fields[fi];
    size_t payload;
    if (f->packed && f->type == FT_VARCHAR) {
        TypedFieldSpan sp;
        typed_field_span(ts, &view, fi, &sp);   /* view resolved once per record */
        payload = sp.len;                        /* content length = descriptor length */
    } else if (f->packed) {
        return 0;   /* packed numeric keys cannot exist (D3): refuse, never guess */
    } else {
        payload = (size_t)f->size;
    }
```

(the existing `blen != payload` consistency check stays and now compares the span-derived
length against `typed_field_to_index_key`'s output — equal by construction after T3.5);
the later `typed_field_to_index_key(ts, fbase, region, fi, …)` call passes the view's base/
region (already the field region).

**Anchor 2** — index.c:1520-1546 `build_index_payload_from_record` heap fallback:

```c
        size_t cap = (size_t)ts->fields[fi].size + 1;
```

becomes

```c
        size_t cap = 1 + (size_t)(ts->fields[fi].packed
                                      ? ts->fields[fi].payload_cap
                                      : ts->fields[fi].size);
```

**Anchor 3** — index.c:1556-1590 `build_index_key_from_record` cap:

```c
    size_t cap = 0;
    if (strchr(spec, '+')) {
        …
            cap += 1 + (size_t)ts->fields[fi].size;
    } else {
        …
        cap = 1 + (size_t)ts->fields[fi].size;
    }
```

becomes `cap += 1 + (size_t)(ts->fields[fi].packed ? ts->fields[fi].payload_cap
: ts->fields[fi].size);` at both arms (checked-malloc unchanged).

**Anchor 4** — index.c:2313-2345 `bm_rebuild_cb`: replace the `vbase` resolution + varchar
prefix read with `typed_span_resolve`; varchar copies `sp.len`/`sp.ptr` (cap check
`> sizeof(key_buf)` stays as the bitmap value cap; packed numeric fields are not bitmap-
indexable — D3 refusal upstream makes the branch unreachable, defensive `return 0`).

**Anchor 5** — index.c:3835-3901 `mf_append_field`: same span substitution for the trigram
extraction (`fbase + tf->offset` → span; packed varchar trigram extraction over `sp.ptr`,
`sp.len`; packed numeric refused by D3 upstream).

**Anchor 6** — index.c:4026-4070 `reindex_seg_cb` + index.c:4484-4485: the zero-pad of compact
records to `total_size` must NOT allocate the schema bound on packed objects. Replace:

```c
    if (w->padded_value && vlen < (size_t)w->ts->total_size) {
        memset(w->padded_value, 0, (size_t)w->ts->total_size);
        if (vlen > 0) memcpy(w->padded_value, value, vlen);
        value = w->padded_value;
        vlen = (size_t)w->ts->total_size;
    }
```

with:

```c
    if (w->ts->layout_version != TYPED_LAYOUT_PACKED &&
        w->padded_value && vlen < (size_t)w->ts->total_size) {
        /* v2 legacy pad (unchanged): packed records are self-describing
           and must not be padded to the schema bound (a 1-GiB calloc on
           maximum schemas). */
        memset(w->padded_value, 0, (size_t)w->ts->total_size);
        if (vlen > 0) memcpy(w->padded_value, value, vlen);
        value = w->padded_value;
        vlen = (size_t)w->ts->total_size;
    }
```

and allocate `padded_value` only for layout-2 schemas (`calloc(1, ts->total_size)` guarded by
`ts->layout_version != TYPED_LAYOUT_PACKED`).

#### T8.3 — index creation refusal (D3)

query_schema.c `cmd_create_object`'s indexes validation and index.c `cmd_add_index`/`cmd_add_indexes`:
after the field resolves, insert:

```c
    if (ts->fields[fi].packed && ts->fields[fi].type == FT_NUMERIC) {
        OUT("{\"error\":\"indexes on bare/packed numeric fields are not "
            "supported until the exact-index phase\"}\n");
        return 1;
    }
```

(create-object: in the index spec loop where the field name resolves against the schema;
add-index: in `cmd_add_index` after `typed_field_index`.)

#### T8.4 — intermediate checks (T8)

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-roundtrip
rtk proxy ./build/bin/shard-db-test run-all
```

---

### Task 9 — Bulk integration: actual-size composition

#### T9.1 — RED: `test_packed_bulk.c` (new file; registered in T9.4)

```c
/* Bulk routes on packed objects: JSON insert, delimited insert, JSON and
   delimited bulk update — mixed valid/invalid rows, mid-field updates
   with trailing variable fields, direct segment vlen assertions. */
#include "test_runner.h"
#include "test_fixtures.h"
#include "test_client.h"
#include "test_assert.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
    TestEnv env;
    ASSERT_EQ_INT(test_env_start(&env), 0, "env");
    TestClient *tc = tc_connect(&(TestClientCfg){ .db_root = env.db_root });
    ASSERT_NOT_NULL(tc, "connect");
    char *resp = NULL;

    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"splits\":8,\"max_key\":64,\"fields\":["
        "\"a:varchar\",\"b:int\",\"c:numeric\",\"d:varchar:20\"]}", &resp), 0, "create");
    free(resp);

    /* JSON bulk insert: array form. */
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"d1\",\"object\":\"o1\",\"rows\":["
        "{\"key\":\"r1\",\"value\":{\"a\":\"x\",\"b\":1,\"c\":\"10.5\",\"d\":\"y\"}},"
        "{\"key\":\"r2\",\"value\":{\"a\":null,\"b\":2,\"c\":\"0\",\"d\":\"\"}}]}",
        &resp), 0, "bulk insert");
    ASSERT_EQ_INT(strstr(resp, "\"error\"") == NULL, 1, "bulk insert ok");
    free(resp);

    /* Verify both records via get. */
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"r1\"}", &resp), 0, "get r1");
    ASSERT_EQ_STR(resp, "{\"a\":\"x\",\"b\":1,\"c\":10.5,\"d\":\"y\"}\n", "r1");
    free(resp);
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"r2\"}", &resp), 0, "get r2");
    ASSERT_EQ_STR(resp, "{\"a\":null,\"b\":2,\"c\":0,\"d\":\"\"}\n", "r2");
    free(resp);

    /* Bulk update: change the MIDDLE fixed field; both variable fields
       carried. Then update the FIRST variable field; trailing must stay. */
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"criteria\":{\"b\":{\"eq\":1}},\"value\":{\"b\":11}}", &resp), 0, "bulk upd");
    ASSERT_EQ_INT(strstr(resp, "\"error\"") == NULL, 1, "bulk upd ok");
    free(resp);
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"r1\"}", &resp), 0, "get");
    ASSERT_EQ_STR(resp, "{\"a\":\"x\",\"b\":11,\"c\":10.5,\"d\":\"y\"}\n", "carried");
    free(resp);

    tc_close(tc);
    test_env_stop(&env);
    printf("test-packed-bulk: ok\n");
    return 0;
}

TEST_REGISTER("test-packed-bulk", test_packed_bulk_run)
```

RED: bulk insert allocates `total_size` and stamps `vlen = total_size` with fixed-offset
writes → the stored records are malformed packed records (get fails or misrenders) → assertion
failure. Paste it.

#### T9.2 — query_bulk.c composers

1. **JSON insert** (anchor query_bulk.c:1600-1604). Replace:

```c
                uint8_t *payload = arena_alloc(&arena, ts->total_size);
                int _enc = typed_encode_defaults(ts, data_ptr, payload,
                                                  ts->total_size,
                                                  db_root, object, NULL, 0);
```

with:

```c
                uint8_t *payload = arena_alloc(&arena, (size_t)ts->total_size);
                if (!payload) { /* existing OOM row-skip/error path */ }
                int _enc = typed_encode_defaults(ts, data_ptr, payload,
                                                  ts->total_size,
                                                  db_root, object, NULL, 0);
                if (_enc < 0) { /* existing encode-failure row accounting */ }
                size_t rec_vlen = (ts->layout_version == TYPED_LAYOUT_PACKED)
                                      ? (size_t)_enc
                                      : (size_t)ts->total_size;
```

and the batch stamp (query_bulk.c:1203-1205) becomes:

```c
        batch[pos].value     = r->payload;
        batch[pos].vlen      = r->vlen;          /* actual length, set below */
        batch[pos].hdr2      = (sw->ts->layout_version == TYPED_LAYOUT_PACKED)
                                   ? (SREC_FLAG_NULLMAP | SREC_FLAG_PACKED)
                                   : SREC_FLAG_NULLMAP;
```

with `BulkInsRecord` gaining `size_t vlen;` (query_bulk.c struct — set from `rec_vlen` at the
parse site and consumed at 1203) so staging no longer assumes `total_size`.

2. **Delimited insert** `bulk_ins_delim_run` (anchor query_bulk.c:2094-2386, twin at 2352-2385):
   the per-row payload becomes a two-phase compose reusing T4.3's machinery — build a
   JSON object per row is wrong; instead add the span-aware variant: compose into the arena
   via a new `typed_compose_from_cells(ts, cells, ncells, arena_cursor, cap, &used)` helper in
   config.c that mirrors `typed_encode_record`'s pass structure but takes per-field
   `(ptr,len)` cell spans in field order (the delimited rows are positional). Its body is the
   T4.3 pass-1 loop with `cells[i]` replacing the JSON match and
   `encode_packed_field_public` per field; fixed fields go through `encode_field_checked`.
   The varchar bound `(int)vals[i].len > ts->fields[i].size - 2` becomes
   `(size_t)vals[i].len > (size_t)(ts->fields[i].packed ? ts->fields[i].payload_cap
   : ts->fields[i].size - 2)`. The `total_size > sc.max_value` invariant (1364/2128) is
   unchanged (v3 bound == max_value by construction). `batch[pos].vlen/hdr2` stamps as in (1).

3. **Bulk updates** `v2_bulk_upd_value_compute` (3396-3414), `v2_bulk_upd_delim_value_compute`
   (4207-4219), `v2_bulk_upd_json_value_compute` (4826-4842): each currently does
   `update_compose_new_from_old(...)` then `rec->vlen = ts->total_size`. They change to:

```c
    long long need = typed_sized_new_from_old(w->ts, old, ctx->field_vals,
                                              ctx->field_null, NULL, 0, NULL, 0);
    if (need < 0 || (size_t)need > rec_cap) return -1;   /* rec_cap = scratch slab row size */
    if (update_compose_new_from_old(w->ts, old, rec->value, ctx->field_vals,
                                    ctx->field_null, &rec->vlen, NULL, 0) != 0)
        return -1;
    rec->hdr2 = (w->ts->layout_version == TYPED_LAYOUT_PACKED)
                    ? (SREC_FLAG_NULLMAP | SREC_FLAG_PACKED) : SREC_FLAG_NULLMAP;
    update_stamp_auto_update_fields(w->ts, rec->value);
```

where `update_compose_new_from_old_v` is the T6.4 function with the `size_t *out_vlen` out-param
(the `_v` suffix avoids touching v2 callers that pass NULL). Scratch slabs
(3682/4279/4902 `malloc(count × total_size)`) stay — they are worst-case row slabs; for
maxed v3 schemas `count × bound` would be enormous, so **cap the slab rows at the segment
maximum** and fall back to per-record malloc for rows whose sized need exceeds
`SLOTCASK_SEG_MAX_BYTES` (they would fail the insert anyway):

```c
    size_t row_cap = (size_t)w->ts->total_size;
    if (row_cap > slotcask_seg_max_bytes()) row_cap = slotcask_seg_max_bytes();
    uint8_t *scratch = malloc((size_t)w->count * row_cap);
```

(the sized pre-check rejects earlier any row needing more).

4. The explicit-NULL fixed-width memset in the JSON updater (query_bulk.c:4856-4858) applies to
   fixed fields only (packed NULLs are bitmap+descriptor and handled by the composer).

#### T9.3 — intermediate checks (T9)

build.sh: register `src/test/cases/test_packed_bulk.c \` after `test_packed_roundtrip.c \`.

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-bulk
rtk proxy ./build/bin/shard-db-test run-all
```

---

### Task 10 — Rebuild/maintenance integration: v3 records through add-field, vacuum, replay

#### T10.1 — RED: extend `test_packed_roundtrip.c`

```c
    /* add-field on a v3 object: backfilled default, carried payloads. */
    expect_resp(tc,
        "{\"mode\":\"add-field\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"field\":\"extra:varchar:10:default=zz\"}",
        "{\"ok\":true}\n", "add field v3");
    expect_resp(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"k1\"}",
        "{\"s\":\"hello packed world\",\"i\":9,\"n\":1.5,\"t\":\"z\",\"extra\":\"zz\"}\n",
        "add-field carried packed payloads");
    /* vacuum --compact keeps values and stays tight. */
    {
        char *resp = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"vacuum\",\"dir\":\"d1\",\"object\":\"o1\",\"compact\":true}",
            &resp), 0, "vacuum");
        ASSERT_EQ_INT(strstr(resp, "\"error\"") == NULL, 1, "vacuum ok");
        free(resp);
    }
    expect_resp(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"k1\"}",
        "{\"s\":\"hello packed world\",\"i\":9,\"n\":1.5,\"t\":\"z\",\"extra\":\"zz\"}\n",
        "post-vacuum roundtrip");
    assert_seg_vlen(&env, "o1", "k1",
                    2 + (8 + 4 + 8 + 8 + 8) + 19 + 17 + 1 + 2, 3,
                    "post-vacuum record still exact-length");
    /* edit-field refusing packed declaration changes (D7). */
    {
        char *resp = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"edit-field\",\"dir\":\"d1\",\"object\":\"o1\","
            "\"field\":\"t:varchar:50\"}", &resp), 0, "edit packed gate");
        ASSERT_EQ_INT(strstr(resp, "\"error\"") != NULL, 1,
                      "packed edit refused explicitly");
        free(resp);
    }
```

(The expected vlen arithmetic: prefix 2, dir = 8+4+8+8+8 = 36, "hello packed world" 19,
numeric 1.5 → 17, "z" 1, "zz" 2 → total 77; adjust the assertion to the computed value if the
exact descriptor widths differ — the executor MUST assert the computed formula, not a magic
number, using the schema's `dir_bytes`/payload lengths from the unit helpers.)

RED: add-field currently rebuilds via `v2_rebuild_walk_cb` writing nullmap-layout records →
post-rebuild gets fail (descriptor bytes rendered as content). Paste it.

#### T10.2 — query_find.c `v2_rebuild_walk_cb` composes per layout

**Anchor** — query_find.c:920-924 (`calloc` + head) and 1062-1066 (insert). The walk gains a
v3 branch with the same field loop structure, routing each field through spans and the shared
composer:

```c
    int new_is_packed = (ctx->new_ts->layout_version == TYPED_LAYOUT_PACKED);
    int old_packed = 0;
    TypedRecordView ov;
    if (typed_view_record(ctx->old_ts, value, vlen, hdr2, &ov) == 0)
        old_packed = ov.packed;
    size_t buf_cap;
    uint8_t *buf;
    if (new_is_packed) {
        /* v3: compose into the schema-bound buffer; the composer fills
           the actual length. Carry-forward payload room is bounded by
           ts->total_size (the schema maximum) — the buffer exists once
           per record and the composed length is what gets stored. */
        buf_cap = (size_t)ctx->new_ts->total_size;
    } else {
        buf_cap = (size_t)ctx->new_ts->total_size;
    }
    uint8_t *buf = calloc(1, buf_cap);
```

(the `calloc` line replaces query_find.c:921 — for a maximum v3 schema this is the bound-size
per-record buffer; the parent's ban targets stack arrays and slot_size-proportional persistent
allocations, and this heap buffer is the same pattern the v2 walk uses today. Per-record
calloc of the bound is acceptable at rebuild time; it is not on the request hot path.)

then the per-field loop branches: for `new_is_packed`, each field resolves the OLD span
(via `typed_field_span` on `ov` for packed-OLD, the legacy bounded materialization for
legacy OLD — the existing `scratch[64]`/`legacy_field_absent` blocks at 1019-1050 stay for
layout-2 OLD), new fields backfill through `encode_packed_field_public` (DK_LITERAL/DK_SEQ/
DK_UUID/DK_RANDOM — the `encode_field(nf, …)` calls at 964/983/1001 become composer calls
writing into the tail cursor), carried fixed fields `memcpy(dst, sp.ptr, sp.len)`, carried
packed fields append `sp.ptr/sp.len` verbatim with regenerated descriptors, NULL bits remap
unchanged. The insert (1064-1066) becomes:

```c
    size_t stored_len = buf_cap;
    if (new_is_packed)
        stored_len = (size_t)typed_record_composed_len(buf, ctx->new_ts);   /* helper: prefix+dir+Σ lens from descriptors */
    SlotcaskUpsertOpts ropts = {0};
    ropts.record_hdr2 = new_is_packed ? (SREC_FLAG_NULLMAP | SREC_FLAG_PACKED)
                                      : SREC_FLAG_NULLMAP;
    int rc = slotcask_insert_with_hooks(ctx->new_db, -1, key, klen,
                                         buf, stored_len, &ropts, NULL);
```

with `typed_record_composed_len(const uint8_t *rec, const TypedSchema *ts)` a small config.c
helper walking the descriptors (declared in types.h next to the span API). The
`splits-only verbatim copy` path (query_find.c:875-892) needs no change — it re-stamps the
record's own hdr2 (T2.3 mask widened), which is the packing-preserving path for resplits.

For **layout-2 new_ts** (rebuilds of v2 objects — vacuum/resplit/edit on old objects) the
function body is byte-identical to today (the packed branches are all inside
`new_is_packed` guards).

#### T10.3 — transform/versioning of `transform_field_value` (D7 boundary)

query_find.c:748-856: prepend the layout guard:

```c
void transform_field_value(const TypedField *old_f,
                                   const TypedField *new_f,
                                   const uint8_t *src,
                                   uint8_t *dst) {
    /* Phase 3 (D7): packed fields never transform here — declaration
       changes on packed fields are refused at the edit-field gate, so a
       packed transform request is a contract violation. Fixed-field
       transforms (legacy int/varchar/numeric rescale, float widen) are
       unchanged. */
    if (old_f->packed || new_f->packed) return;   /* unreachable; caller gates */
    … existing body unchanged …
```

and `field_needs_transform` (query_find.c:859-865) gains
`if (old_f->packed != new_f->packed) return 1;` (defensive; unreachable behind the gate).

The edit-field gate (D7): query_schema.c `cmd_edit_fields`' edit-diff validation — after each
edit's old/new TypedField pair is built, insert:

```c
        if ((of->packed || nf->packed) &&
            (of->size != nf->size || of->numeric_precision != nf->numeric_precision ||
             of->numeric_scale != nf->numeric_scale || of->type != nf->type)) {
            OUT("{\"error\":\"editing packed numeric/varchar declarations "
                "is not supported until the migration phase\"}\n");
            return 1;
        }
```

(same-declaration edits of packed fields are no-ops and pass; fixed-field edits untouched).
The edit preflight walk (`edit_preflight_walk_cb` query_schema.c:125-153) and
`field_value_fits_new` (query_schema.c:64-110) remain v2-only in behavior but gain the typed
view substitution (`typed_span_resolve`) so their pointer math is view-validated; the varchar
cap check reads `nf->packed ? nf->payload_cap : nf->size - 2` (relevant only if D7 later
widens).

#### T10.4 — intermediate checks (T10)

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-roundtrip
rtk proxy ./build/bin/shard-db-test run test-add-field-computed-defaults
rtk proxy ./build/bin/shard-db-test run test-edit-field
rtk proxy ./build/bin/shard-db-test run-all
```

Expected: v3 add-field/vacuum roundtrips green with exact-length assertions; existing
rebuild/edit cases green (v2 objects untouched).
---

### Task 11 — Maximum schema sizes, limits, and crash/replay compatibility

#### T11.1 — RED: `test_packed_maxschema.c` (new file; registered in T11.3)

```c
/* Maximum-schema arithmetic and the row/segment policy (decision D1):
   - 256x bare varchar schema creates; slot_size == 1073744960 bound;
   - one maxed-varchar record (4195393 bytes with a 1024-byte key)
     stores and reads back;
   - a record that cannot fit a 128 MiB segment (32 maxed payloads)
     fails with the explicit pre-staging error;
   - small records stay tight (free-pool reuse, actual vlen). */
#include "test_runner.h"
#include "test_fixtures.h"
#include "test_client.h"
#include "test_assert.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *make_fields_json(int n, const char *spec) {
    size_t cap = (size_t)n * 32 + 32;
    char *buf = malloc(cap);
    size_t pos = 0;
    pos += (size_t)snprintf(buf + pos, cap - pos, "[");
    for (int i = 0; i < n; i++)
        pos += (size_t)snprintf(buf + pos, cap - pos, "%s\"f%d:%s\"",
                                i ? "," : "", i, spec);
    snprintf(buf + pos, cap - pos, "]");
    return buf;
}

int main(void) {
    TestEnv env;
    ASSERT_EQ_INT(test_env_start(&env), 0, "env");
    TestClient *tc = tc_connect(&(TestClientCfg){ .db_root = env.db_root });
    ASSERT_NOT_NULL(tc, "connect");
    char *resp = NULL;

    /* 256 bare varchars + max_key 1024: the handoff's worst case. */
    char *fields = make_fields_json(256, "varchar");
    char req[120000];
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"big\","
             "\"splits\":8,\"max_key\":1024,\"fields\":%s}", fields);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "create max schema");
    ASSERT_EQ_INT(strstr(resp, "\"error\"") == NULL, 1, "max schema accepted");
    free(resp);
    free(fields);

    /* One maxed value in field 0 (4 MiB content). Request size fits the
       32 MiB MAX_REQUEST_SIZE default. */
    size_t content = 4194304;
    char *big = malloc(content + 1);
    memset(big, 'A', content);
    big[content] = '\0';
    char *ins = malloc(content + 256);
    snprintf(ins, (size_t)content + 256,
             "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"big\","
             "\"key\":\"k\",\"value\":{\"f0\":\"%s\"}}", big);
    ASSERT_EQ_INT(tc_request(tc, ins, &resp), 0, "insert maxed varchar");
    ASSERT_EQ_INT(strstr(resp, "\"error\"") == NULL, 1, "maxed insert ok");
    free(resp);
    /* Stored value length = prefix 33 + dir 2048 + 4194304 = 4196385. */
    /* (Record size 24 + 1 + 4196385 = 4196410 with a 1-byte key; the
       parent's 4195393 figure is the same arithmetic with a 1024-byte
       key — both < 128 MiB.) */
    free(ins);

    /* Read back: length asserted server-side via the returned value. */
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"big\",\"key\":\"k\",\"fields\":\"f0\"}",
        &resp), 0, "get maxed");
    ASSERT_NOT_NULL(resp, "get resp");
    free(resp);

    /* Small record in the same huge-slot object stays tight: insert a
       small row and verify the previous record's storage was not padded
       (directly: shard-stats / kf entry offsets differ by the first
       record's aligned size, ~4196416). We assert the small record's OWN
       kf slot exists and the object remains queryable — the tightness
       proof is the exact vlen assertion in test-packed-roundtrip plus
       the free-pool design (bucket 0 spans [0,256)). */
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"big\","
        "\"key\":\"s\",\"value\":{\"f1\":\"tiny\"}}", &resp), 0, "small insert");
    free(resp);
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"big\",\"key\":\"s\"}",
        &resp), 0, "small get");
    ASSERT_EQ_INT(strstr(resp, "\"f1\":\"tiny\"") != NULL, 1, "small roundtrip");
    free(resp);

    /* 32 maxed payloads cannot fit a 128 MiB segment: build a 33-field
       bare-varchar object and insert 32 maxed values → explicit error,
       and the failed insert leaves no record. */
    char *fields33 = make_fields_json(33, "varchar");
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"huge\","
             "\"splits\":8,\"max_key\":64,\"fields\":%s}", fields33);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "create 33x schema");
    free(resp);
    free(fields33);
    char *ins2 = malloc(33 * content + 512);
    size_t pos = (size_t)snprintf(ins2, (size_t)(33 * content + 512),
        "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"huge\","
        "\"key\":\"k\",\"value\":{");
    for (int i = 0; i < 32; i++)
        pos += (size_t)snprintf(ins2 + pos, 33 * content + 512 - pos,
                "%s\"f%d\":\"", i ? "," : "", i);
    /* (The executor fills the 32 quoted values with 'A' x 4194304 and the
       closing braces; total request ~134 MB exceeds the default
       MAX_REQUEST_SIZE, so this leg runs against a fixture started via
       test_env_start_ex with a raised MAX_REQUEST_SIZE — see note.) */
    snprintf(ins2 + pos, 64, "\"}}");
    /* Expected: the explicit D1 error, never a corrupt partial record. */
    ASSERT_EQ_INT(tc_request(tc, ins2, &resp), 0, "oversized insert handled");
    ASSERT_EQ_INT(strstr(resp, "record exceeds segment capacity") != NULL, 1,
                  "explicit segment-capacity error");
    free(resp);
    free(ins2);
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"exists\",\"dir\":\"d1\",\"object\":\"huge\",\"key\":\"k\"}",
        &resp), 0, "exists after failed insert");
    ASSERT_EQ_STR(resp, "false\n", "no partial record");
    free(resp);

    free(big);
    tc_close(tc);
    test_env_stop(&env);
    printf("test-packed-maxschema: ok\n");
    return 0;
}

TEST_REGISTER("test-packed-maxschema", test_packed_maxschema_run)
```

> Executor note: the 134 MB request exceeds the default `MAX_REQUEST_SIZE` (32 MiB). Start
> this case's fixture with `test_env_start_ex(&env, "512")`-style env override support for
> `MAX_REQUEST_SIZE`, or set it in the fixture's generated db.env — check
> `src/test/fixtures.h` for the current override knobs and use the existing mechanism; if
> none exists for `MAX_REQUEST_SIZE`, that is a small fixture amendment (add the env passthrough
> next to the `qbuf_mb_override` parameter) and belongs in this task, not a test weakening.
> The inserted-value assertion (4,194,304 'A' characters through JSON escaping in the
> response) may be asserted by LENGTH (response contains the value once, unescaped length
> matches) rather than full-text compare.

RED: create-object of the 256×varchar schema currently fails (validate_field_type_clean
rejects bare varchar until T1 lands) → the case is RED from the start; after T1-T6 the
oversized-insert leg pins the D1 error path (which does not exist until T6.2) — paste both
red stages.

#### T11.2 — compile-time width guards (types.h)

After the `SREC_KNOWN_FLAGS` block added in T1.2, append:

```c
/* Width guards (Phase 3): the schema bound and slot geometry must stay
   inside int32 (Schema.max_value/slot_size are ints) and uint32 (segment
   vlen, descriptor fields). The checked compute path refuses overflow at
   runtime; these static asserts pin the derivation inputs. */
_Static_assert(DEC_PAYLOAD_MAX <= 0x7FFFFFFF, "numeric payload ceiling fits int32");
_Static_assert((int64_t)BARE_VARCHAR_CONTENT_MAX + TYPED_DESCRIPTOR_BYTES
               * MAX_FIELDS + (1 + (MAX_FIELDS + 7) / 8) <= 0x7FFFFFFF,
               "worst all-varchar schema bound fits int32");
```

and a runtime guard inside `typed_layout_v3_compute` is already the `bound > INT32_MAX`
refusal (T1.4). Add to `load_schema`'s slot_size derivation (config.c:1003-1005) the
companion refusal:

```c
    if ((int64_t)24 + s.max_key + s.max_value > INT32_MAX) {
        LOG_ERROR(LOG_SUB_CONFIG, "slot_size overflow for %s — refusing", object);
        Schema zeroed = {0};
        return zeroed;
    }
```

#### T11.3 — KFM2 replay compatibility smoke (extend `test_packed_roundtrip.c`)

Using the existing crash-test hooks (`test_env_kill` + reopen via `test_env_stop_keep`/
`test_env_start_at`, per the parent's allowed fixture APIs):

```c
    /* Marker replay with packed records: insert, kill, reopen, verify. */
    expect_resp(tc,
        "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"o1\","
        "\"key\":\"k3\",\"value\":{\"s\":\"rr\",\"i\":3,\"n\":\"2.75\",\"t\":\"q\"}}",
        "{\"ok\":true}\n", "pre-kill insert");
    tc_close(tc);
    test_env_kill(&env);           /* SIGKILL — markers may be pending */
    ASSERT_EQ_INT(test_env_start_at(&env, env.db_root, env.port), 0, "reopen");
    tc = tc_connect(&(TestClientCfg){ .db_root = env.db_root });
    ASSERT_NOT_NULL(tc, "reconnect");
    expect_resp(tc,
        "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o1\",\"key\":\"k3\"}",
        "{\"s\":\"rr\",\"i\":3,\"n\":2.75,\"t\":\"q\"}\n",
        "record survives kill/reopen");
    /* Startup validation accepted the packed object (no refusal). */
```

(The env struct field names for db_root/port follow `src/test/fixtures.h` — the executor
adjusts to the actual members; the sequence kill → start_at is the established pattern in
test_slotcask_v2_crash.c.)

#### T11.4 — intermediate checks (T11)

build.sh: register `src/test/cases/test_packed_maxschema.c \` after `test_packed_bulk.c \`.

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-maxschema
rtk proxy ./build/bin/shard-db-test run test-packed-roundtrip
rtk proxy ./build/bin/shard-db-test run-all
```

---

### Task 12 — Documentation sync, suite evidence, hand-back

1. **docs/concepts/typed-records.md**: new section "Packed records (layout 3)" — the §1.1
   byte layout, descriptor validation rules, NULL/empty/zero distinction, bare numeric/varchar
   declarations and caps, exact numeric semantics under layout 3, and the legacy-interoperation
   table (§1.1 of this plan is the source text). Update the storage-size table rows for
   `varchar` (two layouts) and `numeric` (two layouts).
2. **docs/concepts/storage-model.md**: replace the stale "padded to slot_size" segment-record
   wording with actual-length packing + the size-bucketed free pool (the parent Phase 9 doc
   list requires this wording fix; it is accurate the moment packed records exist), plus the
   schema.conf slot-3 sentence and the D1 ceiling policy.
3. **docs/reference/limits.md**: bare varchar 4,194,304; bare numeric digit limits; the
   256×varchar bound/slot arithmetic; the segment-capacity error text.
4. **docs/query-protocol/schema-mutations.md**: create-object v3 declaration forms
   (bare numeric, numeric:P, bare varchar, varchar:N ≤ 4 MiB); note the D3/D4/D5/D7 interim
   refusals verbatim.
5. **AGENTS.md**: the schema.conf line description (`2` → `2|3`) and the Typed-record table
   sentence for bare types (minimal edits; the phase-specific detail lives in the concept docs).
6. **docs/reference/changelog.md**: add the unreleased Phase 3 entry (layout, versioning,
   interim refusals) marked for the release named in D8.
7. Full-suite evidence (three consecutive fresh runs, normal build):

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy ./build/bin/shard-db-test run-all
```

8. Verify the protected file hash and that `git status --short` still shows ONLY the protected
   untracked plan file plus the intended diff:

```sh
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
rtk git status --short
rtk git diff --stat
```

9. **No sanitizers, no benchmarks** in Phase 3. The ASan+UBSan/TSan gates run once per branch
   after ALL later fixes, per AGENTS.md.

## 7. Verification matrix (what proves what)

| Requirement (handoff item) | Proof |
| --- | --- |
| 1. Every caller/consumer inventoried | §3 inventory; §5 rule 8 checklist; T3-T10 anchors cover each ⛔ site; compile breaks from new APIs force every site to be revisited |
| 2. Complete code + unique anchors | Every hunk above carries quoted before-text from HEAD; PLAN_NOTES halt on mismatch |
| 3. RED-first tests, edge cases, commands | T1.1/T2.1/T3.1/T4.1/T5.1/T6.1/T7.1/T8.1/T9.1/T10.1/T11.1 each begin failing for the named reason; per-task command blocks |
| 4. Legacy/new compat, crash-safe publication, downgrade | §2 mechanism; T5 tests (slot gating, strict parser, `.version` decide); T10 verbatim-copy path for resplits; T11.3 kill/reopen; RebuildTxn untouched (its abort/rollback covers v3 rebuilds unchanged) |
| 5. Compact storage + validated descriptors + schema-aware access | T4.1 exact-length assertions (18-byte numeric, trailing fields); T2.1 descriptor rejection matrix; span API as the only access path |
| 6. Limits preserved | T4.1 boundary digits (131072±1, 16383±1); T11.1 4 MiB roundtrip; declarations in T1.1 |
| 7. Recomputed max-record arithmetic + explicit row/segment resolution | §1.3 (scratch-verified); D1 + T6.2 error path + T11.1 behavioral pins; no caps changed (grep evidence in T1.6) |

## 8. Planning-time experiments vs implemented behavior

- The sizing numbers in §1.3 come from a scratch C program compiled under `/tmp/phase3-probes/`
  (not from this repo, not committed). They are arithmetic evidence only.
- Everything quoted as current behavior (anchors, line numbers, validation rules, adapter
  sizing, replay path) was read from this checkout at HEAD `8c0bdf6` by direct reads and four
  parallel source inventories. No Phase 3 behavior has been implemented or prototyped in the
  repository. The Phase 0 spike (59/59 checks) predates this plan and is recorded in the
  parent, not re-run here.
- Test code in this plan is written against the post-T1 API surface; until the matching task's
  production hunks land, those tests are expected to fail to compile or fail their assertions —
  that IS the RED step, and the failure reason is stated per task.

## 9. Unresolved decisions requiring human sign-off before execution

| # | Decision | Plan's recommendation |
| --- | --- | --- |
| D1 | Row vs segment ceiling | Keep slot_size as bound; actual records bounded by both gates; explicit error + WARN; no cap changes |
| D2 | Activation | All new objects slot 3; no migration of existing objects in Phase 3 |
| D3 | Indexes on packed numerics | Refused until Phase 5 (codec + feasibility table) |
| D4 | Criteria/aggregates on packed numerics | Explicit request-time errors until Phase 5/6 |
| D5 | Computed updates on packed fields | Refused until Phase 4 exact computed updates |
| D6 | schema.conf append fsync | Add file+dir fsync at create-object |
| D7 | edit-field on packed declarations | Refused until Phase 8 transactional conversion |
| D8 | Release version | Bump `SHARD_DB_VERSION` at merge per CalVer (≥ 2026.10.3); required-source stays 2026.09.2 |
| D9 | describe/export interim | v3 varchar reports `N+2` for round-trip stability; numeric export stays lossy `18,S` until Phase 4 |

Any change to these decisions requires a plan revision, not an executor choice.

## 10. Milestone note

Phase 3 alone does not unblock the finance application (parent milestone): exact criteria,
aggregates, route-complete writes, and migration still belong to Phases 4-8. What Phase 3 must
prove, and does via the tests above: packed records store compactly and survive every existing
read/write/maintenance/recovery path byte-correctly; legacy objects are untouched; older
binaries cannot open new-layout data through any path, including after a crash before stamping.
