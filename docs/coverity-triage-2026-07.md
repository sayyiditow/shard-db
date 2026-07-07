# Coverity triage — 2026-07-06 scan (82 findings)

Source export: Coverity run dated 07/06/26, 82 findings, all New/Unassigned/Unclassified.
Triage method: 5 parallel source-code investigations (one per module cluster), consolidated
here. Each row is this triage's verdict, not Coverity's own classification.

Verdict legend: **TP** = true positive (real bug, fix warranted) · **FP** = false positive
(safe as-is, usually an already-established idiom elsewhere in the same file) · **AMB** =
ambiguous, could not fully confirm either way from source alone.

Fix plans for every TP below live in `docs/plans/2026-07-07-coverity-*.md`.

## server.c / embedded.c / config.c (startup, dispatch, schema)

| CID | Function | Verdict | Note |
|---|---|---|---|
| 1696481 | warmup_thread | **TP** | `goto done_collect` on realloc failure skips `closedir(dd)`. → resource-leaks plan |
| 1696456 | dispatch_nql_query | **TP** | parse-failure return path never calls `nql_free_command(&cmd)`. → resource-leaks plan |
| 1696441 | dispatch_nql_query | **TP** | same root cause as 1696456, different leaked field reached via a different parse-failure branch; one fix covers both. → resource-leaks plan |
| 1696483 | shard_db_open | AMB (leans FP) | `slots_inited` guard on `sem_destroy` — Coverity may be modeling `sem_init` as infallible; not acted on. |
| 1696479 | dispatch_json_query | **TP** | "compact" mode reads `sdb->format` before the wrlock is taken; "migrate" mutates it under wrlock. → data-races plan |
| 1696474 | cmd_server | **TP** (minor) | accept-loop error branch: both arms of an `if` do the same `continue`, so unexpected errno values are silently retried forever instead of logged. → misc-hardening plan |
| 1696469 | decode_field_to_buf | **TP** (edge case) | `content_max = f->size - 2` can go negative on a corrupt schema; clamped `len` then wraps huge when cast to `size_t`. → misc-hardening plan |
| 1696459 | cmd_server | FP | fields read are `_Atomic int`; no lock needed. |
| 1696447 | shard_db_open_internal | FP | ignored return's value is never used either — dead either way. |
| 1696445 | dispatch_json_query | **TP** | list-tokens iterates `g_token_*` arrays with no `g_token_lock`, while add/remove-token mutate them under that lock. → data-races plan |
| 1696439 | dispatch_json_query | **TP** | *(missed by initial batch, found on follow-up)* — identical bug, different mode: list-ips iterates `g_ip_set`/`g_ip_set_used` (server.c ~904-912) with no `g_ip_lock`, while add-ip/remove-ip (~862-868) mutate them under that lock. → data-races plan |
| 1696438 | cmd_server | FP | same atomics reasoning as 1696459. |
| 1696432 | load_typed_schema | FP | loop is hard-capped by `MAX_FIELDS`, a trusted constant, regardless of file content. |
| 1696422 | seq_next_val_batch | AMB | persisted `val` near `LLONG_MAX` could signed-overflow `val += n`; requires an extreme pre-condition. |
| 1696420 | warmup_touch_file | FP | discarded read() is a deliberate best-effort prefetch hint. |
| 1696482 | run_startup_migration | **TP** (benign) | real TOCTOU (`access` then later `fopen`) but not exploitable — `cmd_server`'s flock guarantees single-instance. |

## storage.c / slotcask.c / parallel.c (caching, recovery, I/O pools)

| CID | Function | Verdict | Note |
|---|---|---|---|
| 1696478 | cmd_get_multi | **TP** | OOM realloc-failure branch falls through instead of returning — leaks parsed `key`/`wire_key` strings and risks use of a NULL'd `entries`. → resource-leaks plan |
| 1696460 | cmd_not_exists | **TP** (minor, already ack'd) | same OOM path leaks per-key strings; comment already documents accepted-on-OOM. |
| 1696409 | cmd_exists_multi | **TP** (minor, already ack'd) | identical pattern to 1696460. |
| 1696419 | slotcask_migrate_to_varlen | **TP** | `goto fail` from Phase-0 calloc failure can fire before `dest[]` is memset; `fail:` handler then reads uninitialized stack. → disk-corruption-hardening plan |
| 1696414 | parallel_io_pool_shutdown | FP | lock-free fast-skip on `_Atomic int g_io_running`; same idiom as the already-annotated CPU-pool twin, just missing the suppression comment. |
| 1696408 | slotcask_bulk_resolve_and_fetch | FP | every exit path frees `resolved`/`sorted`/`args`; no leak found. |
| 1696475 | segcache_acquire_direct | FP | warm-hit path intentionally returns holding the rwlock, released later by the caller — same as the annotated `segcache_acquire`. |
| 1696470 | compact_stream_worker | **TP** | `ftruncate` return ignored when rotating a destination segment. → misc-hardening plan |
| 1696449 | slotcask_migrate_to_varlen | **TP** | same unchecked `ftruncate` pattern during dest-segment rotation. → misc-hardening plan |
| 1696433 | parallel_for_io | FP | same accepted TOCTOU as the CPU-pool `parallel_for`, just missing the comment. |
| 1696429 | kfcache_acquire_direct | FP | same intentional rwlock-handoff pattern as `kfcache_acquire`. |
| 1696416 | recover_one_stream | **TP** | writes `active_file_id`/`reserve_off` without `rotation_lock`, unlike the equivalent update in `slotcask_migrate_to_varlen`. → data-races plan |
| 1696410 | recover_one_stream | **TP** | same finding, second field in the same unlocked write. |
| 1696471 | recover_scan_tombstones_od | AMB (leans TP) | varlen carry-buffer reassembly reads payload from `buf` offset 0 instead of `buf+pos`, re-reading header bytes as payload — real correctness bug, may not be strict OOB. → disk-corruption-hardening plan |
| 1696480 | recover_scan_tombstones_od | **TP** (cosmetic) | dead store, `carry_len` assigned then immediately overwritten. → dead-code-cleanup plan |

## index.c / query_bulk.c / query_aggregate.c / query_find.c / query_maint.c / query_join.c

| CID | Function | Verdict | Note |
|---|---|---|---|
| 1696472 | bulk_ins_run | **TP** | timeout cleanup branch frees `records`/`arena`/`idx_pairs*` but not `wire_keys`, unlike the two sibling cleanup blocks. → resource-leaks plan |
| 1696462 | reindex_object | FP | every `strncpy` is followed by an explicit NUL-terminate; interprocedural false alarm. |
| 1696446 | cmd_aggregate_tree | **TP** | `group_by_buf[4096]` quote-writes are unconditional (only the comma-write is bounds-checked) — real stack buffer overflow on long `group_by_csv`. → stack-overflow plan |
| 1696426 | cmd_aggregate_do | FP | `fi>=0` is provably guaranteed by the eligibility loop just above, just not locally. |
| 1696421 | cmd_add_indexes | FP | same NUL-terminate-follows-strncpy pattern as 1696462. |
| 1696418 | cmd_add_indexes | **TP** | `memcpy` copies the full `MAX_FIELDS`-row array when only `btree_count` rows are initialized. → misc-hardening plan |
| 1696484 | index_parallel | FP | redundant but harmless defensive condition. |
| 1696476 | rebuild_object | **TP** | unreachable legacy v1 body after `return rebuild_object_v2(...)`. → dead-code-cleanup plan |
| 1696464 | cmd_recount | **TP** | same pattern, unreachable v1 recount code after `return 0`. → dead-code-cleanup plan |
| 1696463 | buf_join_values | **TP** | returns `snprintf`'s "would-have-written" length unclamped; caller's `row+pos` becomes OOB and `sizeof(row)-pos` underflows. → stack-overflow plan |
| 1696458 | buf_driver_values | **TP** | identical mechanism to 1696463, same caller. → stack-overflow plan |
| 1696451 | reindex_seg_cb | **TP** | `klen` read straight from an on-disk segment record, unvalidated before computing `value = rec+24+klen`. → disk-corruption-hardening plan |
| 1696450 | cmd_vacuum | **TP** | unreachable legacy v1 vacuum-shard code after `return 0`. → dead-code-cleanup plan |
| 1696448 | iter_init_desc_leaves | **TP** | descent loop follows on-disk child/leaf pointers with no depth or page-id bound. → disk-corruption-hardening plan |
| 1696431 | btree_leaf_scan_o_direct | **TP** | `cnt` read from on-disk page header with no cap before indexing `bts_slot_off`. → disk-corruption-hardening plan |
| 1696428 | mf_append_field | **TP** | `al` (varchar length) read from raw record bytes, used unchecked as a read bound. → disk-corruption-hardening plan |
| 1696427 | tg_estimate_cb | **TP** | identical pattern to 1696428, different call site. → disk-corruption-hardening plan |
| 1696424 | resolve_bitmaps | FP | `vl` is bounds-checked before use. |
| 1696415 | cmd_bulk_delete_criteria | **TP** | unreachable legacy per-key delete loop after `return 0`. → dead-code-cleanup plan |
| 1696405 | cmd_fetch | **TP** | unreachable legacy v1 fetch/pagination code after `return cmd_fetch_v2(...)`. → dead-code-cleanup plan |

*Systemic note: the five "structurally dead code" findings above (1696476, 1696464, 1696450,
1696415, 1696405) share one root cause — the v1→v2 migration left old per-command bodies
physically in the file after an early `return`. One cleanup pass fixes all five.*

## query.c / query_plan.c (planner, criteria, bulk-criteria, keyset)

| CID | Function | Verdict | Note |
|---|---|---|---|
| 1696444 | selective_reindex_dirty | FP | `strncpy` immediately followed by explicit NUL-terminate; caller always NUL-terminates too. |
| 1696413 | find_via_composite_prefix | **TP** | `encode_criterion_value`'s NULL-`tf` fallback does an unbounded `memcpy` of a user-supplied criterion value into a fixed 1032-byte stack buffer whenever the seed/order field fails to resolve (e.g. a typo'd field name). → stack-overflow plan |
| 1696407 | bulk_criteria_indexed_cb | FP | lock-free fast-skip on `_Atomic int` fields, authoritative recheck under the lock — same idiom as the annotated sibling `bulk_criteria_scan_cb`. |
| 1696473 | bulk_criteria_indexed_cb | FP | same fields as 1696407, reported from a second angle. |
| 1696468 | batch_buf_flush_copy | FP | read-then-reset of `pending_n` happens entirely within one locked critical section. |
| 1696467 | bt_alloc_page | **TP** | deliberate `bt->map = NULL` on mremap failure is then dereferenced unconditionally a few lines later instead of propagating an error. → disk-corruption-hardening plan |
| 1696455 | keyset_count_from_or | **TP** | `slotcask_registry_get()`'s NULL return is used unchecked, unlike the sibling `bulk_delete_phase1_indexed` which does check. → null-deref-registry plan |
| 1696453 | bulk_delete_phase1_indexed | FP | lock-free polling of atomic fields to decide early-exit, same idiom as 1696407. |
| 1696443 | keyset_emit_find | **TP** | same unchecked-NULL bug as 1696455, different call site. → null-deref-registry plan |
| 1696442 | keyset_emit_find | **TP** | Coverity's second report of the same unchecked-`sdb` use in 1696443 — one fix resolves both. → null-deref-registry plan |
| 1696434 | compile_one | FP | `ts` non-NULL is provably guaranteed by the branch that reaches this line; already commented in source. |
| 1696417 | bulk_criteria_indexed_cb | FP | third angle on the same atomic-field idiom as 1696407/1696473. |
| 1696412 | cursor_fetch_cb | **TP** | duplicated block after an unconditional `return 0`, operating on an already-unlocked/freed `row`. → dead-code-cleanup plan |
| 1696411 | cmd_count_with_tree | FP | `tree` is guaranteed non-NULL by the only caller, `cmd_count`. |
| 1696410 | *(covered above, slotcask.c)* | | |
| 1696406 | stream_find_record_cb | AMB | no bug found at the flagged short-circuit guard; can't rule out an interprocedural path without Coverity's own trace. |
| 1696477 | bulk_delete_phase1_indexed | FP | same atomic-polling idiom as 1696453. |

## btree.c / io_direct.c / bitmap.c (low-level storage structures)

| CID | Function | Verdict | Note |
|---|---|---|---|
| 1696467 | *(covered above, btree.c)* | | |
| 1696466 | seg_scan_o_direct_varlen | **TP** | `vlen` read straight from an on-disk record header with no bound check; can wrap `int need` negative and pass an oversized length to the callback. → disk-corruption-hardening plan |
| 1696465 | btree_walk_all_values | **TP** | walks `next_leaf` from on-disk page headers with only a `== 0` exit check — no bound against `page_count`, no cycle check. → disk-corruption-hardening plan |
| 1696461 | seg_scan_o_direct | FP | narrowing only ever fires inside a `< 0` (errno) guard; never a real byte count. |
| 1696454 | od_open | FP | `fcntl(F_NOCACHE)` is an explicitly-commented best-effort hint. |
| 1696452 | btree_leaf_scan_o_direct | FP | same guarded-negative pattern as 1696461. |
| 1696440 | bm_open | FP | `bm_mkdir_p`'s discarded return is still safely caught by the very next `stat`/`open` calls. |
| 1696437 | seg_scan_o_direct_match | FP | same guarded-negative pattern as 1696461/1696452. |
| 1696436 | bm_mkdir_p | FP | standard idiomatic "mkdir -p", intermediate EEXIST is expected. |
| 1696435 | bm_open | FP | rwlock intentionally handed off to the caller, released later by `bm_close` — same design as `bt_acquire`. |
| 1696431 | *(covered above, index/io_direct)* | | |
| 1696430 | bm_dict_add | **TP** | truncates a `size_t` value length to 16 bits for the on-disk prefix while still writing the full length via `pwrite` — desyncs the dict for any value ≥ 64KB. → disk-corruption-hardening plan |
| 1696425 | btree_walk_all_values | FP | `bt.map` is guaranteed non-NULL by `bt_acquire`'s own contract; the `&&` guard is just paranoia. |
| 1696423 | bm_open | FP | `g_bm_cache_count` is protected by a mutex, not atomics — correct, just a different mechanism than its neighbors. |
| 1696404 | bt_stream_build_finish | AMB | every value-length argument found is already clamped before use; couldn't pin the specific tainted argument Coverity means. |
| 1696403 | bm_dict_used_bytes | **TP** | loop reads `p[off]`/`p[off+1]` for `n_values` iterations with no bound against `mmap_size`, unlike the sibling `bm_dict_lookup` which does check. → disk-corruption-hardening plan |
| 1696457 | seg_scan_o_direct_varlen | **TP** (cosmetic) | dead store, `carry_len` assigned then immediately overwritten. → dead-code-cleanup plan |

## Summary

- **82 findings total.** ~37 TP (real bugs worth fixing), ~40 FP (already-safe idioms, mostly
  missing an inline `coverity[...]` suppression comment matching an already-annotated sibling),
  ~5 AMB (inconclusive from source alone).
- Two real, user/attacker-reachable memory-corruption bugs stand out as highest priority:
  1696413 (unbounded memcpy via a typo'd criteria field name) and 1696446 (unconditional
  stack-buffer writes via a long `group_by` list) — both in the stack-overflow-fixes plan.
- A cluster of 12 findings share one theme: trusting raw on-disk bytes (segment/btree/bitmap
  file records) as lengths, counts, or pointers without validating them against the file's own
  bounds — grouped into the disk-corruption-hardening plan.
- The FP list is dominated by two repeating idioms already present and annotated elsewhere in
  the codebase: (a) lock-free fast-skip reads of `_Atomic` fields before an authoritative locked
  recheck, and (b) intentional rwlock hand-off from an `_acquire` function to its `_release`
  counterpart. Each FP notes the sibling that already carries the suppression comment; adding
  the same comment to these call sites is optional cosmetic follow-up, not a fix.
