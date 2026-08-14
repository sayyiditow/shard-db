# Test harness: port-picker TOCTOU causes rare random test failures

**STATUS: root-caused, not fixed. Confirmed pre-existing, non-blocking, test-only.**

## Signature

A daemon-backed test case fails its first connect-dependent assertion (e.g.
"create-object: X", "create ambiguous-layout fixture", "vacuum/rebuild
succeeds...") with no daemon crash, no sanitizer report, and no consistent
victim test across runs. Seen in `test-rebuild-txn-edit-field-after-metadata`
(this run) and, in four prior full-suite ASan runs from earlier this session,
in `test-rebuild-recovery`, `test-rebuild-legacy-ambiguous-refuses-start`, and
`test-coverity-estimate-index-overflow` — a different random victim each
time, 0-2 failures per ~10600 assertions per run.

## Root cause

`test_pick_port()` (`src/test/fixtures.c:88`) allocates a port by binding an
ephemeral socket (`sin_port=0`), reading back the assigned number, then
immediately closing it — the real daemon binds that same port number
moments later via a separate fork+exec. This bind-close-then-rebind pattern
has an inherent gap: between the `close()` and the daemon's own `bind()`,
that exact port number is free at the OS level and can be handed to *any*
other ephemeral-port consumer on the machine (another test worker's
`test_pick_port()` call, a client-side `tc_connect()`, or any unrelated
process), especially under the parallel run-all mode's "up to nproc test
daemons... concurrently" load, which churns the ephemeral range fast. The
code's own comment (fixtures.c:26) already calls this "unavoidable."

The existing mitigation — an advisory `flock` on
`/tmp/shard-db-test-port-<N>.lock`, held for a 5000ms TTL
(`PORT_RESERVE_TTL_MS`) — only protects the initial pick-to-first-bind
window, and only against other callers that go through `test_pick_port()`
in the first place. It does not, and cannot, prevent an external ephemeral
socket (anything not going through this codepath) from transiently
occupying the number during that same window. It also does not get renewed
across `test_env_start_at()` restarts (fixtures.c:317): tests like
`test_edit_crash_after_metadata` (test_rebuild_txn_recovery.c) stop and
restart a daemon on the *same* port two more times after the original
`test_pick_port()` call, well past the 5s TTL, with no live socket bound
during the restart gap and no active reservation protecting it either.

## Why this isn't fixed now

A real fix means avoiding the bind-close-rebind gap entirely — e.g. keep the
picker's listening socket open and hand its fd to the daemon child directly
(socket-activation style: pass the fd number via env var, `execl` with it
inherited, daemon `dup2`s onto its accept socket instead of calling its own
`bind()`), or make `test_env_start_at()` re-reserve for the actual lifetime
of a restart-heavy test. Either is a real change to daemon startup and/or
the shared test fixture surface used by ~300 test cases — worth doing
carefully, not as a drive-by edit while verifying an unrelated durability
fix. This is test-infrastructure-only: it does not affect the production
`shard-db` binary, does not touch data-durability or data-loss paths, and
every occurrence so far has been a clean connect/setup failure with no
daemon crash and no sanitizer finding.

## Evidence this is pre-existing and unrelated to schema_caches_shutdown()

Full ASan suite pass/fail history across this session (before and after the
typed-schema-cache leak fix landed):

| run | total | failed | victim test | failure kind |
|---|---|---|---|---|
| asan_build.log | 10590/305 | 0 | — | — |
| asan_build2.log | 10589/305 | 1 | test-rebuild-recovery | setup/connect |
| asan_build3.log | 10547/305 | 2 | test-rebuild-legacy-ambiguous-refuses-start | setup/connect |
| asan_build4.log | 10589/305 | 1 | test-coverity-estimate-index-overflow | setup/connect |
| full_asan_after_fix.log | 10602/306 | 3 | test-rebuild-txn-edit-field-after-metadata | setup/connect |

Same shape, different random victim every time, zero sanitizer findings in
any of the five runs. Confirms this run's failure is the same pre-existing
class, not a regression introduced by `schema_caches_shutdown()`.

## Suggested next step (not started)

1. Give the daemon an inherited-fd startup mode (e.g. `LISTEN_FD=<n>` env
   var, `server.c`'s listen-socket setup skips its own `bind()`/`listen()`
   when set and `dup2`s the inherited fd instead) so the test harness can
   keep the picker socket alive and hand it straight to the child —
   eliminates the gap structurally instead of narrowing it.
2. Until then, mitigate: have `test_env_start_at()` call `try_reserve_port()`
   again on entry to renew the TTL for restart-heavy tests, closing the
   specific gap this run's failure most plausibly hit. Cheaper than #1 but
   only narrows the window, doesn't remove it.

## Severity assessment

Non-blocking. Test-harness-only race, already documented as accepted before
this session, reproducible at a low rate (0-2 per full run) independent of
this branch's changes. No data-loss, no data-corruption, no crash, no
sanitizer finding. Does not gate this branch's merge.
