# Plan J — `backup` takes the object's exclusive lock (fenced copy)

## Brief

**Problem and root cause:** `backup` is a manual, per-object logical copy meant to run when no writes are happening (docs: "pause writes first"). The code does not enforce it: `backup` is not in `mode_is_schema` (`src/db/server.c`), so both dispatchers give it only the *shared* `objlock_rdlock`, which every insert, bulk write, resplit and `sequence` call also holds. `cmd_backup` ([src/db/query_maint.c](../../src/db/query_maint.c)) is a plain recursive `cprf` of `data/`, `indexes/`, `metadata/`, so a backup of a live object can capture a half-finished commit window, a mid-resplit keyfile, a `.bmfix-NNN` bitmap-repair marker, or a half-written `metadata/sequences/<name>` file (explicit `sequence` writes in place with `fopen(..., "w")`, `cmd_sequence`; a restored empty sequence reads as 0 and can re-issue keys). `restore` copies all of that back without any startup repair.

**Fix:** make `backup` take `objlock_wrlock` for the whole copy, exactly like `vacuum`/`truncate`. That excludes every other operation on the object, so nothing can be in flight.

**Why not a writes-only fence (considered and rejected, review rounds 1'–2'):** the all-shard writer reservation (`slotcask_write_reservation_begin`) would let reads continue, but bulk requests dispatch one IO-pool task per shard and each task blocks on its gate in a pool worker; while a backup holds every gate, a burst of bulk writes parks IO-pool workers, and reads and other objects' writes (which also use `parallel_for_io`) queue behind them for the whole copy. Avoiding that needs new dispatcher-level locking. It would also not fix the torn `sequence` file. Backup is rare and run when the database is quiet, so pausing reads on that one object is acceptable.

**Verified facts (code-read 2026-11-12):**
- Normal data operations (all CRUD, queries, bulk, `sequence`, `put-file`) hold the **shared** objlock; only `mode_is_schema` modes take the wrlock (`took_wrlock = mode_is_schema(mode)` in `dispatch_json_query`; `fast_wr = mode_is_schema(cmd)` in the text path). A wrlock therefore waits for every one of them and blocks new ones.
- Bulk requests are synchronous: `slotcask_bulk_request_execute` joins its per-shard pipeline tasks (`parallel_for_io`) before returning, and the dispatcher holds its objlock until the command returns; single writes take one gate inside the dispatch. So no pipeline outlives its request's objlock.
- Auto-vacuum and auto-reshard take `objlock_wrlock` before touching an object (server.c); `reindex`, `restore`, `vacuum`, `truncate`, schema modes and `drop-object` already take it. Warmup tasks only read, under the rdlock.
- `objlock` entries are plain `pthread_rwlock_t` with default attributes (`objlock.c`): a waiting wrlock waits for in-flight holders exactly as `vacuum` does today.
- `cmd_backup` takes no objlock and calls no pool code (so no self-lock when the dispatcher holds the wrlock; `cmd_restore` is the opposite case and stays special-cased). `load_schema` takes no objlock (Phase 0 re-checks).
- The existing test seam `SCHEMA_WRLOCK_TEST_DELAY_MS` fires for **any** `took_wrlock` mode and writes a marker file `<db_root>/<dir>/.schema-wrlock-test-delay-<object>.active` whose first line is `mode=<mode> object=<object>`.

**Logical flow:**
1. Add `backup` to `mode_is_schema` → both dispatchers take `objlock_wrlock` for it; `in_flight_writes` also counts it, so `stop` waits (up to 30 s) for a running backup.
2. Exempt `backup` from the bitmap-escape refusal in both dispatchers (the Unreleased changelog already promises `backup` stays allowed on a bitmap-disabled object; adding it to `mode_is_schema` alone would silently break that).
3. Tests: new `test_backup_objlock.c` (backup blocks a concurrent insert and get); one pin case in `test_bitmap_resplit.c` (backup works on a flagged object, JSON and text).
4. Docs.

**Decisions made:**

| # | Decision | Alternatives | Why | Tag |
|---|---|---|---|---|
| 1 | Whole-object wrlock for the whole copy (reads and writes on that object wait). | Writers-only all-shard reservation. | Rejected above: IO-pool starvation under bulk writers, new machinery needed, doesn't fix the torn sequence file. You chose this. | NEEDS YOU (answered: wrlock) |
| 2 | `backup` counted in `in_flight_writes` (falls out of `mode_is_schema`): `stop` waits for a running backup. | Special-case it out. | A half-copied backup is worse than a delayed stop; same as vacuum. | DEFAULT |
| 3 | `backup` exempt from the bitmap-escape refusal. | Let it be refused on a damaged object. | You want a backup *before* `reindex`; the changelog already lists it as allowed. | DEFAULT |
| 4 | No restore-side marker check. | `cmd_restore` scans for `.bmfix-NNN`. | A fenced backup cannot capture an in-flight resplit or repair. `cprf` copies `indexes/` verbatim, so a backup of an already-flagged object carries its persistent `.bmfix-disabled`/`.bmfix-NNN`, and `cmd_restore` already re-arms the escape for the flag. Older unfenced backups: documented, not coded. | DEFAULT |
| 5 | Text-path *lock* behaviour is not separately lock-tested. | Add the delay seam to the text path. | Both dispatchers read the single `mode_is_schema` predicate, which the JSON test pins; the text-path *gate exemption* is tested directly. A test seam in production code for this is out of proportion. | DEFAULT |

**Blast radius:** `mode_is_schema` has four functional uses, all in `server.c`: JSON wrlock selection (`took_wrlock`), JSON bitmap gate, `in_flight_writes` accounting (`is_write`), and text `fast_wr` (the text bitmap gate reuses `fast_wr`); other hits (`embedded.c`, comments) are not functional. `cmd_backup` has two direct callers (JSON `mode:"backup"`, text `backup`, both `server.c`); `./shard-db backup` and shard-cli Maintenance → backup reach it over the wire, unchanged. Observable change: every request on that object (reads included) waits for the copy; `stop` waits for a running backup. No wire format, on-disk format, config key or CLI flag changes. Requests parked behind a backup occupy their dispatcher workers (same as behind `vacuum`), so on a very busy object clients should be quiesced first. Behaviour change for operators, flagged in the changelog.

**Test strategy:**
- `test-backup-fences-writers`: with the existing seam widening the wrlock window, a concurrent JSON insert and JSON get block until the backup finishes. Red on base (backup takes no wrlock → no marker ever appears).
- `test-bmfix-backup-allowed-when-disabled`: pin, green before and after; proven load-bearing by mutation (lock edit without the gate exemption → red).

**Out of scope:** restore-side marker handling; older unfenced backups; a writers-only variant; crash/kill mid-copy (a partial `backup/<ts>` directory remains possible, as before); copying `files/` (never done); a durable/fsync'd copy; the text-path delay seam; changing `cmd_sequence` to write atomically (fenced by the wrlock for backup purposes; hardening the command itself is separate); Plans G/H/I files.

**Review log:**
- Round 1 (earlier draft of this same wrlock design): 0 blockers, 2 majors, 9 minors. Majors: stale claims left in `schema-mutations.md` (and a grep that couldn't catch them); "a fenced backup cannot contain a live marker" false for an already-flagged object. All fixed; the minors are folded into the plan below.
- Rounds 1'–2' (writers-only reservation design, abandoned): 0 blockers, 2 majors — IO-pool starvation under bulk writers; torn `sequence` file. These are the reasons for rejecting that design; the wrlock addresses both. Round 2' also flagged items now folded in: remove timing leftovers, name the pin test function, explicit docs correction for `files/`/`metadata/`, caller count, N0 as before/after delta, changelog is a moving target while Plan G edits it, `stop`/in-flight accounting note.
- Round 3 (fresh reviewer on this final design): 0 blockers, 1 major, 5 minors. Major: Phase 4 status/"unmodified" checks were written for a dirty tree (Plan G uncommitted) — resolved by the execution rule that J runs on a committed Plan G with a clean tracked tree (and halts otherwise). Minors fixed: 1A wording/header recipe, deployment.md `files/` claim, embedded.c two-line phrase, queued-request client timeouts note, stale `path` example in schema-mutations.md, line-485 comment note.
- Round 4 (fresh reviewer; it read the plan around the time the round-3 fixes landed): **0 blockers, 0 majors → converged.** Minors folded in: `<scratch>` defined; Phase 0 config.c wording ("comments only"); Phase 4 status list is not exclusive of untracked plan/evidence files; worker-parking-behind-backup note in the Brief and `backup.md`; timestamp format (`YYYYMMDD_HHMMSS`, underscore) and `path` shape fixed in `backup.md`/`schema-mutations.md`; `stats` `in_flight_writes` doc note. Still open: none.

---

## Plan (executor-facing)

### Execution rules
- Branch `fix/type-correctness-recovery`. **Run only after Plan G has been reviewed and committed locally by the human** (J is based on that commit, so the tracked tree is clean when J starts and J's diff is the whole `git diff`). Record `rtk proxy git rev-parse HEAD` and `rtk proxy git status --short` in the evidence file first; the only expected entries are untracked plan/evidence files. If the tracked tree is not clean → halt and ask.
- No git writes, no benchmarks, no sanitizers (the branch gate runs after Plan I). Every shell command starts with `rtk`.
- Build `rtk proxy env SKIP_TESTS=1 ./build.sh`; single case `rtk ./build/bin/shard-db-test run <name>`; full suite `rtk ./build/bin/shard-db-test run-all --jobs 4`. Capture real exit statuses; `rm -rf /tmp/shard-db-*` after every test invocation once processes stop.
- `<scratch>` below = `/tmp/claude-1000/-home-hashim-junk-system-git-workspace-sayyiditow-shard-db/2be29ed5-8e91-4388-bb20-eb3df94ea65e/scratchpad` (create it if missing).
- Evidence: `docs/plans/2026-11-12-plan-j-execution-evidence.md` (untracked). Paste real outputs. Never weaken/delete an assertion; this plan modifies **no existing assertion**.
- Anchor mismatch → write `PLAN_NOTES-planj.md` and halt. Moved symbol with unambiguous site → proceed, record it. Uncovered decision → stop and ask.

### Phase 0 — audits (no edits; record results)
1. Exact-once anchor greps in `src/db/server.c`: `rtk grep -c 'strcasecmp(m, "remove-index") == 0' src/db/server.c` → 1; `rtk grep -c 'strcasecmp(mode, "sequence") != 0' src/db/server.c` → 1; `rtk grep -c 'if (!fast_restore &&' src/db/server.c` → 1. `rtk grep -c 'test_read_objlock_contention' build.sh` → 1. `git status --short` must show no modified tracked file (Plan G committed); otherwise halt.
2. `rtk grep -n 'mode_is_schema' src/db/*.c`: four functional uses besides the definition (`took_wrlock = mode_is_schema(mode)`, the JSON gate `(mode_is_schema(mode) || mode_is_write(mode))`, `is_write = ... mode_is_schema(mode_for_write)`, `fast_wr = mode_is_schema(cmd)`); the text bitmap gate reuses `fast_wr`; the rest are comments. Any other functional use → halt.
3. No self-lock: `rtk grep -n 'objlock_' src/db/query_maint.c` shows no call-form hit outside `cmd_restore` and none between the `cmd_backup` definition and `parse_object_meta` (the comment at about line 485, just before `cmd_restore`, is expected — do not halt on it); `rtk grep -n 'objlock_' src/db/config.c` shows comments only ("Caller must hold objlock_wrlock"), no lock call. A real lock call → halt.
4. The seam fires for any `took_wrlock` mode: read the block `if (took_wrlock && g_db && g_schema_wrlock_test_delay_ms > 0)` in `dispatch_json_query` (marker text `mode=%s object=%s\n`, path `<db_root>/.schema-wrlock-test-delay-<object>.active` where `db_root` is the effective tenant root — the template test uses `<db_root>/default/...`).
5. Read `test_read_objlock_contention.c` end to end (template) and `br_escape_start`, `br_q`, `br_flag_exists`, `br_close`, `ASSERT_CONTAINS`, `br_expect_refusal`, `br_text_insert_expect_refusal` in `test_bitmap_resplit.c`.
6. Baseline: build; `rtk ./build/bin/shard-db-test list | wc -l` → `N0` (record the number; the final check is the delta, not an absolute).

### Phase 1 — tests first (both written before the production edit)

**1A. New file `src/test/cases/test_backup_objlock.c`**, registered in `build.sh` as a new line `    src/test/cases/test_backup_objlock.c \` immediately after the `test_read_objlock_contention.c \` line. Copy `test_read_objlock_contention.c` and apply exactly:
- Rewrite the header comment (the template's is NQL-specific): fenced backup, same seam. Rename every remaining `vacuum`/NQL wording in the copied file (comments, `connect for vacuum`, `vacuum request sent`, the `shard-db-nql-lock-race` path); assertion messages other than the one marker message pinned below are free-form and do not affect any failing set.
- `test_read_objlock_contention_run` → `test_backup_fences_writers`; `TEST_REGISTER("test-backup-fences-writers", test_backup_fences_writers)`; tmp base `/tmp/shard-db-backup-fence-XXXXXX`; object `lockrace` → `bkplock` everywhere (create-object, insert, requests, marker path, marker match string).
- `wait_for_wrlock_delay_start` match strings: `mode=backup` (was `mode=vacuum`) and `object=bkplock`. Marker-observed assertion message becomes exactly `"synchronous backup wrlock delay marker observed"`.
- The round-2 held request is `{"mode":"backup","dir":"default","object":"bkplock"}` (was vacuum), still sent with `tc_send` on `vc`; when drained at the end, assert its response contains `"status":"backed_up"` (message "backup completes").
- Replace the NQL find client (`nc`, `nql_t0`, `nql_sent`, `nql_rc`, `nresp`) with a JSON **insert** client using the new names `ic`, `ins_t0`, `ins_sent`, `ins_rc`, `iresp`: request `{"mode":"insert","dir":"default","object":"bkplock","key":"k2","value":{"name":"bob","age":41}}`, sent with `tc_send` back to back with the existing JSON `get` of `k1` (`gc`) before either response is awaited. Insert assertions (mirror the get block's four): round-trip rc 0 ("JSON insert round-trip succeeds"); response non-NULL without `"error"` ("JSON insert succeeds after the backup's wrlock releases"); marker gone at response ("JSON insert response arrives only after the held-wrlock delay ends"); `elapsed >= DELAY_MS / 2` ("JSON insert blocks behind the backup's exclusive lock"). The existing get block stays as is (messages may say backup instead of schema wrlock). Keep `DELAY_MS 2000`, `now_ms()` and the elapsed diagnostics (they are still used; delete nothing that becomes unused — if the build warns about an unused symbol, remove exactly that symbol and record it).
- After the get and insert blocks and after `tc_close(vc)`: open a fresh `TestClient *rc = tc_connect(&cfg)`, `tc_request` a JSON `get` of `k2`, assert the response contains `"bob"` (message "insert committed after the lock released"), then `tc_close(rc)`.

**Base-red proof (before any production edit):** build; `rtk ./build/bin/shard-db-test run test-backup-fences-writers`; `rm -rf /tmp/shard-db-*`. On base `backup` takes no wrlock, so no marker appears, the 10 s wait fails and the test returns early. Expected failing-assertion set **exactly** `{"synchronous backup wrlock delay marker observed"}`. Paste the full TAP output. Anything else → halt.

**1B. Pin case in `src/test/cases/test_bitmap_resplit.c`**: new function `static int test_bmfix_backup_allowed_when_disabled(void)` placed after `test_bmfix_escape_disabled`; register `TEST_REGISTER("test-bmfix-backup-allowed-when-disabled", test_bmfix_backup_allowed_when_disabled)` next to `test-bmfix-escape-disabled`. Body:
```
Br *b = calloc(1, sizeof(*b)); ASSERT_NOT_NULL(b,"alloc"); if(!b) return 1;
char root[256]; int port = 0;
int st = br_escape_start(b, root, sizeof(root), &port);
if (st != 0) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
ASSERT_TRUE(br_flag_exists(b), "object is flagged disabled");            /* non-vacuity */
br_expect_refusal(b, "{\"mode\":\"vacuum\",\"dir\":\"br\",\"object\":\"t\"}", "vacuum still refused while flagged");
char *r = br_q(b, "{\"mode\":\"backup\",\"dir\":\"br\",\"object\":\"t\"}");
ASSERT_CONTAINS(r, "\"status\":\"backed_up\"", "JSON backup allowed on a flagged object"); free(r);
r = br_q(b, "%s", "backup" "\x1F" "br" "\x1F" "t");   /* text path; concatenation after each \x1F */
ASSERT_CONTAINS(r, "\"status\":\"backed_up\"", "text backup allowed on a flagged object"); free(r);
ASSERT_TRUE(br_flag_exists(b), "flag untouched by backup");
br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0;
```
Match the file's actual signatures (Phase 0 item 5). Run on base: all assertions pass (green by design — it guards the changelog promise; it has no red state on base). Paste the output.

### Phase 2 — production edits (`src/db/server.c`)
**2a. Lock + accounting.** In `mode_is_schema`, add `strcasecmp(m, "backup") == 0 ||` on a new line after the `strcasecmp(m, "add-index") == 0 || strcasecmp(m, "remove-index") == 0 ||` line, keeping the trailing `0;`. Append one sentence to the multi-line comment immediately above `mode_is_schema` (before its closing `*/`): "backup takes the exclusive object lock so the copy cannot observe a write, resplit, repair marker or sequence update in flight."
- **Mutation proof m1 (gate exemption is load-bearing):** build with only 2a; run `test-bmfix-backup-allowed-when-disabled`. Expected failing-assertion set **exactly** `{"JSON backup allowed on a flagged object","text backup allowed on a flagged object"}` (the dispatcher refuses backup with the escape error; vacuum is still refused and the flag untouched). Paste. Continue to 2b.

**2b. Gate exemptions.**
- JSON gate: add `strcasecmp(mode, "backup") != 0 &&` on a new line after `strcasecmp(mode, "sequence") != 0 &&`. In the comment above that `if`, replace the single-line fragment `and restore (the repair's own escape hatches)` with `restore and backup (backup is a read-only copy an operator wants before reindex)`.
- Text gate: change `if (!fast_restore &&` to `if (!fast_restore && strcasecmp(cmd, "backup") != 0 &&`. In the comment above it, that sentence spans two source lines, so replace only the single-line fragment `put-file, get-file-path and restore stay allowed` with `put-file, get-file-path, restore and backup stay allowed`.
- Build: no new warnings.

**Green proof:** both cases all-pass; record the printed `elapsed` diagnostics of `test-backup-fences-writers`.

**Revert proofs (hand-revert, no git writes).** First `rtk proxy git diff > <scratch>/plan-j-full.patch`.
(i) Remove only the `strcasecmp(m, "backup") == 0 ||` line; build; run `test-backup-fences-writers` → failing set **exactly** `{"synchronous backup wrlock delay marker observed"}` (same as the base-red proof).
(ii) Restore; remove only the JSON-gate line `strcasecmp(mode, "backup") != 0 &&`; build; run `test-bmfix-backup-allowed-when-disabled` → failing set **exactly** `{"JSON backup allowed on a flagged object"}`.
(iii) Restore; remove only the text-gate clause `&& strcasecmp(cmd, "backup") != 0`; build; same case → failing set **exactly** `{"text backup allowed on a flagged object"}`.
(iv) Restore all: `rtk proxy git diff | diff - <scratch>/plan-j-full.patch` prints nothing; rebuild; both cases green. Paste every output; `rm -rf /tmp/shard-db-*` after each run.

### Phase 3 — docs (same plan)
1. `docs/operations/backup.md`, section "### Consistency": replace the two sentences with: backup holds the object's **exclusive lock for the whole copy** — every request on that object, reads included, waits until it finishes, so the copy cannot observe a write, resplit, bitmap-repair marker or `sequence` update in flight; backup waits for in-flight operations to drain exactly as `vacuum` does and, because the object lock is a default-attribute (reader-preferring on glibc) rwlock, may wait a long time under continuous overlapping traffic — quiesce clients on busy objects (requests parked behind the backup occupy their dispatcher workers, as behind `vacuum`); `./shard-db stop` waits for a running backup like it waits for writes; an object already flagged bitmap-disabled is copied as-is (flag and markers included) and `restore` re-arms the escape until `reindex`; backups taken by releases before this change were not fenced (if one was taken on a live object, run `reindex` after restoring it); filesystem snapshots remain the recommendation for very large or always-busy objects.
   Also state: requests waiting behind the backup are not subject to the per-request `timeout_ms` while queued (same as behind `vacuum`), so clients with short I/O timeouts (shard-cli included) may time out and should retry.
   Also correct the timestamp format in backup.md's examples to what the code emits (`%Y%m%d_%H%M%S`, e.g. `20260418_153012`, underscore).
   **Also correct, explicitly (pre-existing inaccuracies):** `cmd_backup` copies `data/`, `indexes/`, `metadata/` plus `fields.conf` and an `object.json` manifest — **not** `files/`. In backup.md fix the "Everything for that object" sentence and the directory tree (remove `files/`, add `metadata/`).
2. `docs/query-protocol/schema-mutations.md`, section `## backup`: replace "Snapshot is a point-in-time copy — in-flight writes after the copy starts may or may not be included. For consistent production backups, pause writes first (or accept best-effort consistency)." with a statement that backup holds the object's exclusive lock for the copy (reads and writes wait). Fix the same `files/` claim in that section's first paragraph, and the example `path` value (`<dir>/<obj>.backup-...`) to the real `<obj>/backup/<timestamp>` form (check `cmd_backup`'s output `path` and match it). In the "Lock model summary" table split the row `` | `backup`, `recount` | `objlock_rdlock` | Only schema mutations. | `` into `recount` (unchanged, rdlock) and a new `backup` row `objlock_wrlock` / "All other ops on this object (reads + writes)." — match the table's column format.
3. `docs/concepts/concurrency.md`, "Per-object rwlock" bullet list: in the `objlock_wrlock()` bullet add `backup` to the mode list and the clause "backup holds it for the whole copy."
3b. `docs/query-protocol/diagnostics.md` (the `in_flight_writes` stats description, about line 45): add "a running `backup` counts".
4. `docs/operations/deployment.md` line `2. **Per-object \`backup\` command**`: change "data/indexes/files" to "data/indexes/metadata" (it never copied `files/`) and append "It holds the object exclusively for the copy (reads and writes wait)."
5. `docs/reference/changelog.md`: **re-read the `## Unreleased` section first** (Plan G edits it too); append a new paragraph at the end of that section, without touching any existing paragraph: "**`backup` takes the object's exclusive lock.** ..." — the behaviour change (every request on the object waits during the copy; `stop` waits for a running backup), the reason (no live write, resplit, repair marker or sequence update can be captured), that backup stays allowed on bitmap-disabled objects, and "No wire, CLI, config or on-disk change."
6. `AGENTS.md` (repo root), the bitmap-escape bullet in "Storage model": where it says every `mode_is_schema` mutation is refused on a flagged object, add "(except `backup`)". Also the identical phrase in the `embedded.c` comment (it spans two source lines, about lines 959–960, `every record mutation and every mode_is_schema mutation is refused up front`): add "(except backup)" in **that comment only**; optional — if skipped, record why.
7. Stale-claim checks (each must print nothing): `rtk grep -n -i 'may or may not be included\|best-effort consistency' docs/operations/backup.md docs/query-protocol/schema-mutations.md`; `rtk grep -n -i 'files/' docs/operations/backup.md docs/query-protocol/schema-mutations.md` (any remaining hit must be about put-file/`files/` for the file store, not about backup copying it — judge each and record); `rtk grep -n 'backup.*objlock_rdlock' docs/query-protocol/schema-mutations.md docs/concepts/concurrency.md docs/operations/*.md`.

### Phase 4 — full suite and hand-back
1. `rtk ./build/bin/shard-db-test run-all --jobs 4`; real exit status; `rm -rf /tmp/shard-db-*`. Expected: total cases = `N0 + 2`, **0 failed**. A pass-on-rerun is a bug to root-cause, never a rerun-until-green.
2. Definition of done: no new warnings; no TODO/FIXME/debug output; no mutation left (the `diff` against `plan-j-full.patch` is empty); `rtk proxy git status --short` shows only the following tracked/new files (plus the untracked plan and evidence files of earlier plans, which are not exclusive of this list): `build.sh`, `src/db/server.c`, `src/db/embedded.c` (one comment, optional), `AGENTS.md`, `src/test/cases/test_bitmap_resplit.c`, new `src/test/cases/test_backup_objlock.c`, the docs (`backup.md`, `schema-mutations.md`, `concurrency.md`, `deployment.md`, changelog), this plan and the evidence file. `query_maint.c`, `slotcask.c` and `config.c` are **unmodified**. Nothing committed.
3. Hand back: diff stat of Plan J's delta, base-red / m1 / revert outputs, full-suite totals, any deviation.
