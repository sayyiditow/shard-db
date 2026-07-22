# typed-schema cache: enum_values leak at process exit (LeakSanitizer)

**STATUS: root-caused and fixed.** See "Confirmed root cause" and "Fix
shipped" below.

Found during ASan re-verification of the durability-sync branch
(`feat/durability-sync-embedded-bg-threads`). Reproduced twice, in two
independent sequential (non-contaminated) full-suite ASan runs. Not a
regression from this branch's diff — `src/db/config.c` is untouched by
this branch — pre-existing.

Not fixed in the first pass: root cause (why the allocation becomes
unreachable, specifically) was not nailed down after a real attempt, and
the fix surface (schema-cache lifecycle) is unrelated to the
durability-sync/warmup work this branch is actually about. Blind-fixing
without confirming the mechanism risks masking the real bug or a
double-free against `invalidate_schema_caches`'s own (correct) cleanup.
A follow-up pass empirically confirmed the mechanism (below) and shipped
the fix predicted in "Suggested next step" #1.

## Signature

```
==<pid>==ERROR: LeakSanitizer: detected memory leaks

Direct leak of 56 byte(s) in 2 object(s) allocated from:
    #0 calloc
    #1 parse_field_type src/db/config.c:1364      (the `char **vals` array)
    #2 load_typed_schema src/db/config.c:1486
    #3 load_schema src/db/config.c:867
    #4 warmup_object_open src/db/server.c:2608
    #5 warmup_thread src/db/server.c:2878

Indirect leak of 41 byte(s) in 7 object(s) allocated from:
    #0 malloc
    #1 parse_field_type src/db/config.c:1373      (individual vals[i] strings)
    ... same chain ...

SUMMARY: AddressSanitizer: 97 byte(s) leaked in 9 allocation(s).
```

Seen in `test-enum` (full-suite run) and reproducible via any object
with an `enum(...)` field whose typed schema gets cached via
`load_typed_schema`. Reported at the owning daemon's own process exit
(right after its "shard-db stopped" log line) — i.e. this is a
leak-at-shutdown finding, not a growing-during-normal-operation leak:
each unique `(db_root, object)` key is parsed and cached at most once
per process lifetime (`load_typed_schema`'s cache-hit path returns
early), so this does not accumulate under load.

## What's ruled out

`TypedField.enum_values` (the `char **` from `parse_field_type`'s
`enum(...)` branch, config.c:1339-1390) is heap-owned and freed via
`free_enum_values()` (config.c:1183). Two call sites explicitly free it
before clearing a cache slot:

- `invalidate_schema_caches()` (config.c:3175-3214): loops
  `g_typed_cache`, calls `free_enum_values(&ts->fields[fi])` for every
  field before `used = 0`. Correct.
- The `add-fields`/`edit-fields` cache-invalidation block further down
  config.c (~3781-3815): same pattern, also correct.
- `parse_field_type()` itself defensively calls `free_enum_values(f)` on
  entry (config.c:1263), for the in-place-reparse case. Also correct —
  and not relevant here since `load_typed_schema` always parses into a
  freshly-`memset`'d local `TypedSchema ts` (config.c:1443-1446), so
  `enum_values` starts NULL for every field on a first parse.

So this isn't "some cache-clear path forgot to free before overwrite" —
both real invalidation sites already do it right.

## What's suspected but not confirmed

`g_typed_cache` is `#define`d as `(g_db->typed_cache)`
(`shard_db_internal.h:355`), and `g_db` itself is a `__thread ShardDb
*g_db` (`embedded.c:8`) — thread-local, not a plain global pointer.
Every other subsystem-level cache (bitmap, btree, kfcache, segcache,
slotcask registry, tls, parallel pools) has an explicit `*_shutdown()`
that runs at daemon exit and is presumably where owned heap state gets
freed before LeakSanitizer's exit-time reachability scan runs — grep
confirms **no such function exists for the typed/schema caches**
(no `typed_cache_shutdown`, no `config_shutdown`, nothing analogous).

Working theory, not verified: by the time the daemon's LSan exit check
runs, whatever thread(s) held `g_db` (TLS) pointing at the live
`ShardDb` have already exited/were never the one the check's root-scan
sees as still holding it live, and nothing else keeps
`typed_cache[*].schema.fields[*].enum_values` reachable at that specific
point — while the surrounding `ShardDb` struct's *other* fields don't
trip LSan because they're either embedded/inline (not separately
heap-allocated) or are freed by one of the `*_shutdown()` calls that DO
exist. This theory is plausible (it would explain why only the
enum-values sub-allocations show up, and why every other cache with a
`_shutdown()` doesn't) but is not confirmed — I did not instrument or
single-step an actual exit to prove the TLS/root-scan mechanism.

## Confirmed root cause

The TLS/reachability theory above was **wrong** — LSan's exit-time scan
walks the whole process image (globals, TLS blocks, stacks), and
`g_db`/`typed_cache` stays reachable through exit; that was never the
issue. Confirmed by instrumenting `load_typed_schema` (store + cache-hit)
and `invalidate_schema_caches` (match count) with `fprintf(stderr, ...)`
+ `fflush`, then racing `test_env_start`'s `daemon.log` redirect (see
`src/test/fixtures.c:267-282`) to capture the *first* daemon's own trace
before `test_env_stop`'s `rm -rf` deleted it (`SHARD_TEST_TMPDIR` pinned
to a fixed path, polling `cp` in a loop until the test process exited).

The captured trace for object `e:a` (one enum field, widened 3→4 values
across the test) showed exactly:

```
STORE   e:a  (create, 3 values: red,green,blue)
...20 HITs...
INVALIDATE e:a matches=1     <- append-yellow (edit-field, cmd_edit_fields)
STORE   e:a  (reload, 4 values: red,green,blue,yellow)
...6 HITs...
INVALIDATE e:a matches=1     <- rename (edit-field, cmd_edit_fields)
STORE   e:a  (reload, 4 values: crimson,green,blue,yellow)
...1 HIT...
shard-db stopped (SIGTERM)
```

`invalidate_schema_caches` is correct and does free the *previous*
store's `enum_values` before each subsequent store (2 stores freed by 2
matching invalidates, exactly as designed). But the **third, final**
store is never followed by another invalidate — the process just gets
SIGTERM'd and exits. The leak byte-counts confirm this precisely: 32
bytes/1 object direct (one `calloc` for a 4-pointer `enum_values` array)
+ 26 bytes/4 objects indirect (four `malloc`'d value strings:
"crimson\0", "green\0", "blue\0", "yellow\0") — exactly the *last*
loaded generation of `e:a`'s schema, not the first or second.

So the real bug is exactly what "What's ruled out" already established
plus one missing piece: every mutation-triggered reload is correctly
paired with a free of the *previous* generation, but **nothing ever
frees the *last* generation still resident when the process exits** —
because no shutdown path ever calls anything equivalent to
`invalidate_schema_caches` for the schema/fields/typed/index caches.
`bt_cache_shutdown()`/`bm_cache_shutdown()`/`slotcask_shutdown()` all
exist and are called from `server.c`'s and `embedded.c`'s shutdown
sequences; the schema-cache family had no equivalent.

## Fix shipped

- `src/db/config.c`: extracted the existing `TEST_BUILD`-only
  `test_reset_caches()` body (which already correctly freed all four
  caches, including `enum_values`, for the test-runner's
  between-test-case reset) into a new non-gated `schema_caches_shutdown(void)`.
  `test_reset_caches()` is now a thin `TEST_BUILD` wrapper calling it —
  no logic duplicated.
- `src/db/types.h`: declared `schema_caches_shutdown(void)` next to
  `invalidate_schema_caches`.
- `src/db/server.c`: call `schema_caches_shutdown()` in both shutdown
  sequences (the early background-thread-start-failure path and the
  main graceful-shutdown path), alongside `bt_cache_shutdown()` /
  `bm_cache_shutdown()` / `slotcask_shutdown()`, before
  `shard_db_destroy_after_storage()` frees the `ShardDb` struct itself.
- `src/db/embedded.c`: same, at both `db_cleanup_before_pools()` (early
  `shard_db_open()` failure paths) and `shard_db_close()`.

All temporary `DEBUG-TYPEDCACHE-*` instrumentation removed after the
root cause was confirmed.

## Verification

- `./build/bin/shard-db-test run test-enum` in isolation under ASan:
  51/51 passed, **no LeakSanitizer report** (previously 97 bytes / 9
  allocations every run).
- Full ASan suite re-run pending as part of this branch's standard
  pre-merge sweep (see branch-level verification, not tracked in this
  doc).

## Suggested next step (superseded — done, see "Fix shipped" above)

The three steps originally listed here (add a shutdown function, confirm
the mechanism before committing to the fix, verify under ASan
single-test-then-full-suite) were all completed. Kept below for the
historical record of what was planned before root-causing:

1. ~~Add a `typed_schema_cache_shutdown(void)`...~~ → shipped as
   `schema_caches_shutdown()`, reusing the existing (already-correct)
   `test_reset_caches()` body instead of writing new free logic.
2. ~~Confirm the TLS/reachability theory before committing...~~ → theory
   was wrong; real mechanism confirmed empirically (see above) and
   turned out simpler: a missing shutdown call, no lock-discipline
   surprises, no race with `invalidate_schema_caches` (same mutexes,
   same free pattern, just invoked at a time no concurrent request can
   be in flight — daemon shutdown, after `bg_threads_stop`).
3. ~~Verify fix under ASan, single-test then full suite~~ → isolated
   `test-enum` confirmed clean; full-suite ASan re-run pending as part
   of the branch's standard pre-merge sweep.

## Severity assessment

Bounded (97 bytes, fixed per unique enum-bearing object key, does not
grow under sustained load), leak-only (no UAF, no data corruption, no
crash), confined to process exit. Did not block this branch's merge on
its own — unrelated file, pre-existing, no data-loss/data-corruption
implication — but was fixed on this branch anyway per the "never paper
over, fix it when we see it" standard once root-caused.
