# Plan K — `backup` / `restore` also cover the object's `files/` directory and `tokens.conf`

## Brief

**Goal:** a `backup` of an object must be enough to restore the same object — records, indexes, sequences, stored files **and access tokens** — into another database later (the object directory must exist there first — `restore` answers "object not found" otherwise — and the backup folder goes inside it, as `restore` already requires).

**Problem and root cause:** `backup` copies only `data/`, `indexes/` and `metadata/` (plus `fields.conf` and a generated `object.json`); `restore` restores only those three. Two more members of the object directory are silently left out:
- `files/` — files stored with `put-file`. A `backup`/`restore` cycle loses them, and the docs (until Plan J corrected them) even claimed they were copied.
- `tokens.conf` — the object's own access tokens (`<db_root>/<dir>/<object>/tokens.conf`).
Cause: the `subs[]` lists in `cmd_backup`/`cmd_restore` ([src/db/query_maint.c](../../src/db/query_maint.c)) never included them, and tokens additionally live in an in-memory token store (`server.c`) that is loaded from the files only at startup.

**Logical flow:**
1. `cmd_backup` (query_maint.c): after the three-directory copy, also `cprf` `files/` → `<backup>/files/`, copy `<obj>/tokens.conf` → `<backup>/tokens.conf` (mode forced to `0600`), and add `"files":1` to the `object.json` manifest.
2. `cmd_restore` (query_maint.c): read the `"files":1` marker from the backup's manifest. Marker present → refuse without `force` when live `files/` is non-empty, then replace live `files/`. Marker absent (backup from before this change) → leave live `files/` untouched. `cmd_restore` does **not** touch `tokens.conf`.
3. The JSON dispatcher (server.c), after `cmd_restore` **succeeds**, calls a new small function `restore_merge_object_tokens` that, if the backup directory holds a `tokens.conf`, adds the tokens that are **not already present** in the running token store (scoped to the restored `(dir, object)`; an existing token is never changed) and rewrites the object's live `tokens.conf` from the store — the `add-token` locking and save sequence. Restored tokens work immediately, no restart.
4. `mode_admin_level` (server.c): `restore` joins the `ADMIN_OBJECT` modes (see Verified facts — today any token that covers the object, even read-only, can run it).
5. Tests (`test_backup_files.c`, `test_restore_tokens.c`), docs.

**Verified facts (code-read 2026-11-12):**
- `cprf` copies regular files and directories recursively with `O_NOFOLLOW`, ignores symlinks/specials, returns `void` (copy errors unreported — pre-existing, unchanged), and silently returns when the source is missing.
- Since Plan J, `backup` runs under `objlock_wrlock`; `put-file`/`delete-file` take the shared objlock. `restore` self-locks with `objlock_wrlock` and releases it before returning.
- `put-file` writes `files/<name>` via a unique temp sibling + rename; crash leftovers, if any, are copied verbatim.
- `parse_object_meta` reads `object.json` via `strstr` into a 256-byte buffer; an extra key is ignored by older binaries; the manifest is about 95 bytes.
- Tokens: loaded at startup by `load_tokens_conf` into an in-memory store (`token_set_add_full`, `g_token_set*`); every request is authorized against the store, not the file. `add-token` takes `g_token_lock`, calls `token_set_add_full(token, dir, obj, perm)` then `save_tokens_conf_full(raw_db_root, dir, obj)` (which rewrites the scope's file from the store). `token_set_add_full` **overwrites** the scope and perm of a token string that already exists (its comment says so), and `load_one_tokens_file` calls it unconditionally — so the restore merge must not use `load_one_tokens_file`; it parses the backup file itself with the same `parse_token_line` and adds a token only when `token_find_slot` does not already find it. Nothing in this plan removes a token (the `token_set_remove` probe-chain defect noticed during design is not touched; see Out of scope). `token_set_add_full` silently does nothing when the table is full (`TOKEN_CAP`); the merge checks capacity and logs.
- **Authorization gap (verified):** `mode_admin_level` lists `restore` nowhere (the `ADMIN_OBJECT` list is truncate, vacuum, backup, recount, add-field, edit-field, remove-field, rename-field, add-index, remove-index, drop-object), so `restore` is `ADMIN_NONE` and `is_authorized` lets any token whose scope covers the object run it as a non-write mode (`perm >= PERM_R`), including a read-only one, with `force:true`. `docs/concepts/multi-tenancy.md` line 41 already documents `restore` as object-admin (`rwx`). Because this plan makes `restore` mutate the live auth store, it also fixes the classification (decision 10). No existing test covers `restore` with a non-admin token. The text-wire path's token handling is audited in Phase 0 item 7.
- Restored tokens are always scoped to the restored `(dir, object)` — the file format carries no scope — so a restored backup cannot grant access beyond that object.
- `restore` dispatch in `dispatch_json_query` (`db_root_eff`, `fromv`, `force`) runs before `is_valid_dir`, so the new function must validate `dir` and `object` itself.

**Decisions made:**

| # | Decision | Alternatives | Why | Tag |
|---|---|---|---|---|
| 1 | Back up `files/` **and** `tokens.conf`. | `files/` only. | You need a backup to be portable to another database with the same access. | DEFAULT (you decided) |
| 2 | The backup's `tokens.conf` copy is created with mode `0600`. | Inherit the source mode (0644 by default). | It holds token secrets in plaintext; do not widen exposure. The backup directory's own permissions are still the operator's responsibility (documented). | DEFAULT |
| 3 | Restore **adds only tokens that are not already present** (by token string, any scope) to the running store and rewrites the object's file; it never removes tokens, never changes an existing token's scope or perm, and needs no `force` for tokens. | Use `load_one_tokens_file` (overwrites scope/perm of existing tokens); replace the live file; reload by removing scope entries. | Avoids both the removal path and silently re-scoping a global/tenant token or upgrading an existing `r` token to `rwx` because a backup line has the same string. Consequences, documented: a token revoked after the backup comes back if it is in the backup (and the live file is rewritten from the in-memory set, so hand-edits made after startup are lost — same as `add-token`); live tokens absent from the backup are kept. | NEEDS YOU |
| 4 | New backups carry `"files":1`; restore replaces `files/` only when the marker is present. | Always replace `files/`. | An old backup has no `files/`; "always replace" would delete every live file on restore. | DEFAULT |
| 5 | Restore without `force` refuses when live `files/` is non-empty and the backup has the marker; with `force` it wipes live `files/` and copies the backup's back (an empty or missing backup `files/` leaves it empty). | Merge instead of replace. | Mirrors `data/`/`indexes/`/`metadata/`; a merge would keep files added after the backup. | DEFAULT |
| 6 | Copy errors stay unreported (`cprf` is `void`). | Make `cprf` propagate errors. | Pre-existing, affects all backup content; own change. | DEFAULT |
| 7 | The marker is a new `"files":1` key read by a small separate helper. | Extend `parse_object_meta`'s signature. | Leaves `parse_object_meta` and its call untouched. | DEFAULT |
| 8 | Backup time and disk use under the exclusive object lock now scale with the stored-file volume. | Exclude `files/` from `backup`. | Needed for a complete object backup; you described backup as rare and run quiet. | DEFAULT (raised in review; not objected to) |
| 9 | The token merge runs on the **JSON** `restore` path only. The legacy text-wire path (`server_process_fast`) has **no authentication at all** (verified: no `is_ip_trusted`/`is_authorized` call), so a text `restore` stays callable by any network client exactly as today, and it does **not** touch the token store (restoring from the text path therefore needs a daemon restart for restored tokens to take effect). | Run the merge on both paths. | An unauthenticated caller must not be able to resurrect revoked tokens by restoring a backup. The `ADMIN_OBJECT` reclassification (decision 10) is likewise JSON-only; the text path's missing auth is pre-existing and out of scope. | NEEDS YOU |
| 10 | `restore` becomes an `ADMIN_OBJECT` mode (one word in `mode_admin_level`'s `obj[]` list): it requires an `rwx` token whose scope covers the object, as `multi-tenancy.md` already documents. | Leave the classification; document the gap. | Without it a read-only object token could wipe the object **and**, with this plan, inject tokens (possibly `rwx`) into the auth store — a privilege escalation. Behaviour change: `r`/`rw` tokens that could (wrongly) restore now get `{"error":"auth failed"}`. JSON path only (the text path has no auth, decision 9). | NEEDS YOU |

**Blast radius:** `cmd_backup`, `cmd_restore` (query_maint.c); `server.c` gets one new static function and one call site (the JSON `restore` branch; the text branch is deliberately left alone, decision 9) and touches the token store only through existing `token_set_add_full` / `save_tokens_conf_full` under `g_token_lock`. Callers of `backup`/`restore`: JSON/text modes, `./shard-db`, shard-cli (over the wire, unchanged). Authentication caveat (pre-existing): the text-wire path has no authentication, so `restore` there is open to any network client and is not covered by decision 10. Observable changes: backups contain `files/` and `tokens.conf` and are larger (copy time under the object lock grows with stored-file volume); restore from a new backup replaces `files/` and adds the backup's tokens. `object.json` gains one key; older binaries ignore it and ignore `files/` and `tokens.conf` in a backup. No wire, CLI or config change. Behaviour change flagged in the changelog. Security-relevant changes: `restore` now mutates auth state, so it is reclassified `ADMIN_OBJECT` (decision 10 — a behaviour change for non-`rwx` tokens, which could run `restore` before); restored tokens are scoped to the restored object only and never change existing tokens. Capacity: a full token table (`TOKEN_CAP`) makes the merge skip the remaining tokens and log it.

**Security consequence to approve (decision 3):** `restore` re-adds any token that is in the backup even if it was revoked after the backup was taken, and it can be run by any `rwx` token covering the object (after decision 10). That is the behaviour you need for "restore the same object elsewhere with the same access", but on the **same** database it can bring back a token you revoked. The docs say so; no tombstone mechanism is added.

**Known windows (same as `data/` today, extended to `files/`):** `cprf` is `void`, so a backup whose `files/` copy partly failed (ENOSPC/EIO) still carries the marker and a later `restore --force` wipes live `files/` and copies back the partial set; a crash after the wipe leaves live `files/` empty until `restore --force` is re-run (the backup is intact). Likewise, if an operator pruned `<backup>/files/` but kept the manifest, `restore --force` wipes live `files/` and copies nothing. Documented in `backup.md`, not fixed here.

**Test strategy:**
- `test-backup-restore-files`: put-file ×2, backup, delete one + add one, restore with force → originals back with their bytes, late file gone; backup holds `files/` and the manifest marker. Red on base.
- `test-restore-old-backup-keeps-files`: strip the marker from the backup's `object.json`; restore with force leaves live `files/` as it was. Green on base by design; proven by mutation.
- `test-restore-merges-object-tokens` (daemon with `DISABLE_LOCALHOST_TRUST=1`, pre-seeded global token): an `rw` object token cannot `restore`; add an object-scoped token, backup, remove the token, restore → the restored token authenticates immediately and the object's live `tokens.conf` contains it; a token that already existed with different perms is not changed by the restore. Red on base.
- The no-`force` files refusal is tested only if the Phase 1C probe finds a reachable state.

**Out of scope:** removing or revoking tokens on restore; the `token_set_remove` open-addressing defect (it clears a slot without a tombstone while `token_find_slot` stops at the first unused slot, so removing one token can make a later colliding token unfindable until restart — noticed during design, read-only analysis, not reproduced; recommend its own investigation); `cprf` error reporting; same-second backup directory collisions; making backup non-blocking; global/tenant-scope tokens; the text-path token test; a second effect of the removal defect — after a removal leaves a hole in a probe chain, `add-token` of a token further down the chain can insert a duplicate, which `save_tokens_conf_full` then writes twice, and the `token_find_slot` skip used by the merge shares that blind spot (pre-existing); Plans H/I.

**Review log:**
- Round 1 (files/ only design): 0 blockers, 2 majors, 5 minors — fixed the m2 simulation (old-backup test now keeps `files/` and strips only the marker), non-existent `## restore` doc section (restore wording lives in `backup.md`), missed `docs/cli/index.md`, recipe/guess points, known windows.
- Round 2 (files/ only design): 0 blockers, 0 majors, converged; minors fixed.
- Round 3 (this revision adds `tokens.conf` with the live merge): 1 blocker, 1 major, 10 minors. **Blocker:** the Brief claimed `restore` already requires an object-admin token; it does not (`mode_admin_level` omits it; any covering token, even read-only, can run it) — so the new token merge would sit behind an under-authorised mode. Fixed by decision 10 (reclassify `restore` as `ADMIN_OBJECT`, step 2e, test, changelog note) and the corrected claim. **Major:** `load_one_tokens_file` overwrites existing tokens' scope/perm, contradicting "add-only" — the merge now parses the file itself and skips tokens already present (decision 3, step 2d, test assertion, mutation m6). Minors fixed: helper placement above the RESTORE comment block, `strstr` NULL guard in the old-backup test, `O_NOFOLLOW` in the merge, token-table-full handling, the removal-defect side effect noted, hand-edit loss documented, Goal wording, pruned-backup window, harness note for test 1D, multi-tenancy table.
- Round 4: 0 blockers, 3 majors, 9 minors. Majors: (1) Phase 0 item 3's `grep object.json` matched `json_parse_object(json` lines and would cause a false halt — now a fixed-string grep with the real expected hits; (2) the legacy text-wire path has no authentication, so the security fix and the token merge only apply to JSON — the merge is removed from the text path (decision 9, NEEDS YOU), the claim is stated as JSON-only everywhere; (3) decision 3 (resurrecting revoked tokens) is a real security trade-off — retagged NEEDS YOU and added to the Brief. Minors fixed: FIFO safety (`O_NONBLOCK` + `S_ISREG`), no rewrite when nothing was added, m2/m4/1C wording, 1D helpers and request shapes, `main.c` help text and `error-codes.md` judgements, files refusal in backup.md's error list.
- Round 5 (fresh reviewer): **0 blockers, 0 majors → converged.** Verified anchors, compile context, base-red sets for both new tests, mutations m1–m7, existing-test impact, compatibility. Minors fixed: stale "two call sites"/Phase 0 item references, decision-table order, vacuous `not copied` grep (asterisk-aware now), fixed `GTOK` string with a recorded hash slot, m4 and 1C instructions spelled out, `npm/index.d.ts` and `backup.md` error-list judgements.

---

## Plan (executor-facing)

### Execution rules
- Branch `fix/type-correctness-recovery`. Run **after Plan J is committed** (HEAD is J's commit; tracked tree clean — if any tracked file is modified, halt and ask). Record `rtk proxy git rev-parse HEAD` and `rtk proxy git status --short` first in the evidence file.
- No git writes, no benchmarks, no sanitizers (the branch gate runs after Plan I). Every shell command starts with `rtk`.
- Build `rtk proxy env SKIP_TESTS=1 ./build.sh`; single case `rtk ./build/bin/shard-db-test run <name>`; full suite `rtk ./build/bin/shard-db-test run-all --jobs 4`. Capture real exit statuses; `rm -rf /tmp/shard-db-*` after every test invocation once processes stop.
- `<scratch>` = `/tmp/claude-1000/-home-hashim-junk-system-git-workspace-sayyiditow-shard-db/2be29ed5-8e91-4388-bb20-eb3df94ea65e/scratchpad` (create if missing).
- Evidence: `docs/plans/2026-11-12-plan-k-execution-evidence.md` (untracked). Paste real outputs. Never weaken/delete an assertion; this plan modifies **no existing assertion**.
- Anchor mismatch → write `PLAN_NOTES-plank.md` and halt. Moved symbol with an unambiguous site → proceed and record it. Uncovered decision → stop and ask.

### Phase 0 — audits (no edits)
1. Exact-once anchors in `src/db/query_maint.c`: `rtk grep -c 'int cmd_backup(const char \*db_root, const char \*object) {' src/db/query_maint.c` → 1; `rtk grep -c 'Schema capture — make the backup directory self-contained' src/db/query_maint.c` → 1; `rtk grep -n 'storage_version\\":2' src/db/query_maint.c` → exactly one `fprintf(mf, ...)` whose format ends `\"layout\":\"%s\"}\n"`; `rtk grep -c 'static int parse_object_meta' src/db/query_maint.c` → 1; `rtk grep -c 'int cmd_restore(' src/db/query_maint.c` → 1; `rtk grep -n 'const char \*subs\[\]' src/db/query_maint.c` → exactly 2 hits (backup and restore) — **do not edit either array**.
2. In `cmd_restore`, read and record the order of: the "Refuse if any live subdir is non-empty" block (inside `if (!force)`), the `parse_object_meta(...)` call, the wipe-and-copy `for` loop over `subs[]`, and the `slotcask_bmfix_disabled_exists` re-arm block. `cmd_restore` returns 0 on success and 1 on every refusal (confirm by reading every `return`).
3. `rtk grep -rn -F 'object.json' src/ --include='*.c'` (fixed-string match, the `.` is literal) → expected hits: `query_maint.c` (writer + parser + comments), `server.c` (one comment), tests `test_restore.c`, `test_layout_manifest.c`, `test_backup_objlock.c`. Any other hit that reads or parses a manifest → halt.
4. `server.c` token code: read `token_set_add_full`, `load_one_tokens_file`, `save_tokens_conf_full`, `parse_token_line`, and the `add-token` handler (the validation order and the `pthread_mutex_lock(&g_token_lock); token_set_add_full(...); save_tokens_conf_full(raw_db_root, ds, os); pthread_mutex_unlock(&g_token_lock);` sequence). Confirm: `load_one_tokens_file` is `static` and defined **before** the `restore` branch of `dispatch_json_query` and before `server_process_fast` (the new function must be placed after `save_tokens_conf_full`'s definition); `g_token_lock` is initialised before any request can run. Record the exact variable that holds the **raw** db root in `dispatch_json_query` (`raw_db_root`) and the existing `is_valid_dir`/`is_valid_object` signatures.
5. Exact-once anchors in `server.c`: `rtk grep -c 'cmd_restore(db_root_eff, object, fromv, force);' src/db/server.c` → 1; `rtk grep -c 'cmd_restore(eff_root, object, arg1\[0\] ? arg1 : NULL, force);' src/db/server.c` → 1; `rtk grep -c '"truncate", "vacuum", "backup", "recount",' src/db/server.c` → 1.
6. Read `src/test/cases/test_restore.c` (harness template for 1A) and `src/test/cases/test_token_perms.c` (daemon spawn with `DISABLE_LOCALHOST_TRUST=1` and a pre-seeded global token; how `auth` is attached to requests; what a rejected token returns) — template for 1D. Read `docs/query-protocol/files.md` for `put-file`/`delete-file`/`list-files` request shapes.
7. Audit the **text-wire** path (`server_process_fast`): find where, if anywhere, a token or trusted-IP check happens before the `restore`/`backup` call, and record it. If the text path accepts `restore` without any token check, record that as an existing property; do not add a text-path auth gate in this plan, and surface the finding in the hand-back (the text path stays unauthenticated).
8. Baseline: build; record `rtk ./build/bin/shard-db-test list | wc -l` as `N0`.

### Phase 1 — tests first

**1A. New file `src/test/cases/test_backup_files.c`**, registered in `build.sh` as a new line `    src/test/cases/test_backup_files.c \` immediately after the `test_backup_objlock.c \` line. Model its includes (add `<unistd.h>` for `access`/`unlink`), copy `extract_str` and `path_tail` verbatim, and the daemon start/stop on `test_restore.c`. Per case: `add-dir` `default`, then create the object with exactly `{"mode":"create-object","dir":"default","object":"bkf","splits":8,"max_key":16,"fields":["name:varchar:16"]}` (`bkf2`, `bkf3` likewise). `<root>` = `env.db_root`; `ts` = `path_tail` of the backup response `path`. Helper `put_file(tc, name, b64)` sends `{"mode":"put-file","dir":"default","object":"<obj>","filename":"<name>","data":"<b64>"}` and asserts `"status":"stored"`; helper `read_live(root, obj, name, out, sz)` reads `<root>/default/<obj>/files/<name>` with `fopen`.

Case **`test-backup-restore-files`** (`test_backup_restore_files`, object `bkf`):
1. `put_file a.txt "YWxwaGE="` (alpha), `put_file b.bin "YmV0YQ=="` (beta).
2. `backup` → `ts`.
3. Assert (messages exact): `<root>/default/bkf/backup/<ts>/files/a.txt` exists and contains `alpha` — "backup copied a.txt"; same for `b.bin`/`beta` — "backup copied b.bin"; `<root>/default/bkf/backup/<ts>/object.json` contains `"files":1` — "manifest marks files as governed".
4. `delete-file` `a.txt` (`{"mode":"delete-file","dir":"default","object":"bkf","filename":"a.txt"}`); `put_file c.txt "Z2FtbWE="` (gamma).
5. `restore` with `force:true`; assert `"status":"restored"`.
6. Assert: live `a.txt` reads `alpha` — "a.txt restored with original bytes"; live `b.bin` reads `beta` — "b.bin intact after restore"; live `c.txt` absent (`access(..., F_OK) != 0`) — "c.txt added after the backup is gone after restore".

**Base-red proof (before any production edit):** build; run `test-backup-restore-files`; `rm -rf /tmp/shard-db-*`. Expected failing-assertion set **exactly** `{"backup copied a.txt","backup copied b.bin","manifest marks files as governed","a.txt restored with original bytes","c.txt added after the backup is gone after restore"}` ("b.bin intact after restore" passes on base). Any other set → halt. Paste the TAP output.

Case **`test-restore-old-backup-keeps-files`** (`test_restore_old_backup_keeps_files`, object `bkf2`):
1. `put_file a.txt "YWxwaGE="`; `backup` → `ts`.
2. Simulate a backup taken before this change: rewrite `<root>/default/bkf2/backup/<ts>/object.json` — `fopen` "r", `fread` into `char buf[512]` (NUL-terminate), find the exact substring `,"files":1` with `strstr` — **if it returns NULL (base: the marker was never written) skip the `memmove` and write the buffer back unchanged** — otherwise `memmove` the tail over it; `fopen` "w", `fwrite` the result. Leave the backup's `files/` directory exactly as `backup` wrote it (holds `a.txt` on the new code; absent on base): the test pins that the **marker alone** decides. Setup assertion (passes on base too): manifest no longer contains `"files":1` — "pre-change manifest simulated".
3. `delete-file` `a.txt`; `put_file x.txt "eHh4"` (xxx).
4. `restore` with `force:true`; assert `"status":"restored"`.
5. Assert: live `x.txt` still reads `xxx` — "restore from a marker-less backup leaves live files/ untouched (x.txt kept)"; live `a.txt` still absent — "restore from a marker-less backup does not resurrect a.txt".
Run on base: all pass (green by design; a pin with no red state on base). Paste output.

**1C. Probe (evidence only):** determine whether restore **without** `force` can reach the new files refusal. Recipe (object `bkf3`, no indexes — `truncate` keeps per-field index directories, so an indexed object stays non-empty): (1) `put_file a.txt "YWxwaGE="`; (2) `backup` → `ts`; (3) `{"mode":"truncate","dir":"default","object":"bkf3"}`; (4) `restore` **without** `force` from `ts`. If the answer contains `files/ is not empty (use force=true to overwrite)`, keep it as case **`test-restore-files-refusal`** with assertions "restore without force refuses when only files/ is non-empty" and "refused restore left live files/ intact" (live `a.txt` still present); its mutation proof: delete the new refusal block, rebuild, the case must fail on exactly the first assertion (restore answers `restored`) — record. If the answer is the `data/`/`indexes/`/`metadata/ is not empty` refusal instead, the files refusal is unreachable by test: record the response, add no case, and state "covered by code-read parity with the existing refusal" in the evidence. How to run the probe: after Phase 2, write the case `test-restore-files-refusal` with the two assertions above in `test_backup_files.c`, run it, and record the response; if the answer is not the files refusal, delete the case again (and its `TEST_REGISTER` line) and record that. (On base the marker is absent, so it cannot refuse.)

**1D. New file `src/test/cases/test_restore_tokens.c`**, registered in `build.sh` after the `test_backup_files.c \` line. The stock fixture cannot set `DISABLE_LOCALHOST_TRUST`, so copy `test_token_perms.c`'s manual daemon spawn/stop (about 100 lines: its own `db.env` with `DISABLE_LOCALHOST_TRUST=1`, a pre-seeded global `rwx` token `GTOK`, `test_env_stop_keep`-style cleanup) and its request-auth convention (`"auth":"<token>"`); do **not** call the stock `test_env_start`. Also copy `extract_str`/`path_tail` from `test_restore.c`, and write a small `file_contains(path, needle)` helper (fopen, read up to 4 KiB, `strstr`). Build the pre-seeded global token like `test_token_perms.c` does but with the **fixed** string `GTOK = "sdb_rt_admin_fixed"` (`test_token_perms.c` uses a time-based string, whose table slot varies per second); its `dirs.conf` is pre-seeded, so `add-dir default` answers `dir_exists` — do **not** assert on it. A `get` needs a key: use `{"mode":"get","dir":"default","object":"tk","key":"k1","auth":"<token>"}` (a not-found answer is fine; only `auth failed` means rejected) and `insert` `{"mode":"insert","dir":"default","object":"tk","key":"k9","value":{"name":"x"},"auth":"<token>"}`. The three `objtok-*` strings occupy slots 573, 864 and 36 of the 1024-slot token table; **compute `str_hash("sdb_rt_admin_fixed") % 1024` (djb2-style `h = h*33 + c`, start 5381, as in `str_hash` in `server.c`) and record it in the evidence — it must differ from those three so the removal defect cannot affect the test; if it collides, pick another fixed string and record it.** A rejected token answers `{"error":"auth failed"}` on the generic gate (`{"error":"unauthorized"}` on admin-management modes) — treat both as "rejected". Case **`test-restore-merges-object-tokens`** (`test_restore_merges_object_tokens`), object `tk` under dir `default`; every request below is authenticated with `GTOK` unless another token is named:
1. `add-dir` `default`; create `tk` (same create-object shape as 1A).
2. `add-token` object-scoped, one request each (`{"mode":"add-token","dir":"default","object":"tk","token":"<t>","perm":"<p>","auth":"GTOK"}`), asserting `"status":"token_added"`: `objtok-restore-1` perm `rw`; `objtok-keep` perm `rw`.
3. Non-vacuity before the backup: an authenticated `get` on `tk` with `objtok-restore-1` does not answer `auth failed` — "object token works before backup".
4. `backup` → `ts`. Assert `<root>/default/tk/backup/<ts>/tokens.conf` exists and contains `objtok-restore-1` — "backup copied tokens.conf" — and its mode is `0600` (`stat`, `st_mode & 0777`) — "backup tokens.conf is 0600".
5. After the backup: `remove-token` `objtok-restore-1` (`{"mode":"remove-token","token":"objtok-restore-1","auth":"GTOK"}`) and assert it is rejected (a `get` with it answers `auth failed`) — "revoked token is rejected"; re-`add-token` `objtok-keep` with perm `r` (overwrites its perm; the backup still lists it as `rw`); add a third token `objtok-rw` perm `rw`.
6. `restore` from `ts` **without** `force`, authenticated with `objtok-rw`: assert the answer contains `auth failed` — "rw object token cannot restore". (On base the request is allowed and answers the non-empty refusal instead, so this assertion fails there; it also changes no state.)
7. `restore` from `ts` with `force:true` authenticated with `GTOK`: assert `"status":"restored"`.
8. Assert: `objtok-restore-1` authenticates again **without a restart** (a `get` with it does not answer `auth failed`) — "restored object token authenticates immediately"; `<root>/default/tk/tokens.conf` contains `objtok-restore-1` — "live tokens.conf rewritten with the restored token"; `objtok-keep` is **still read-only** — an authenticated `insert` with it answers `auth failed` while a `get` with it succeeds — "restore does not change an existing token's perm" (passes on base too — it guards the merge; mutation m6 proves it).

**Base-red proof:** run before any production edit. On base the backup has no `tokens.conf`, so "backup copied tokens.conf", "backup tokens.conf is 0600" (the `stat` fails), "rw object token cannot restore" (restore is allowed for any covering token), "restored object token authenticates immediately" and "live tokens.conf rewritten with the restored token" fail; the non-vacuity assertions and "restore does not change an existing token's perm" pass. Expected failing set **exactly** those five; record the actual set, any other → halt.

### Phase 2 — production edits
**2a. `cmd_backup` (query_maint.c).** Immediately before the comment `/* Schema capture — make the backup directory self-contained.`, insert:
```c
    /* files/ (put-file store) is part of the object; copy it as-is. cprf
       returns silently when the source does not exist. */
    snprintf(src_sub, sizeof(src_sub), "%s/files", src_dir);
    snprintf(dst_sub, sizeof(dst_sub), "%s/files", bak_dir);
    cprf(src_sub, dst_sub);

    /* The object's access tokens travel with it. They are secrets: the copy
       is 0600 regardless of the source mode. A missing file is fine. */
    snprintf(src_sub, sizeof(src_sub), "%s/tokens.conf", src_dir);
    snprintf(dst_sub, sizeof(dst_sub), "%s/tokens.conf", bak_dir);
    if (access(src_sub, F_OK) == 0) copy_file(src_sub, dst_sub, 0600);
```
(`src_sub`/`dst_sub` are declared earlier in `cmd_backup`; if the compiler complains, read the declarations and adapt.) Change the manifest `fprintf` format string from `...\"layout\":\"%s\"}\n` to `...\"layout\":\"%s\",\"files\":1}\n` (no new argument). `copy_file` opens the destination with `O_CREAT|O_TRUNC` and mode `mode & 0777`; with `0600` and a normal umask the result is `0600` (the test asserts it).

**2b. `cmd_restore` (query_maint.c).** Add the helper **above the `/* ========== RESTORE ==========` comment block that precedes `cmd_restore`** (so that block's doc comment stays attached to `cmd_restore`):
```c
/* 1 when the backup's object.json carries "files":1 (written by backups
   taken with files/ support); 0 for older backups or unreadable manifests. */
static int backup_manifest_has_files(const char *meta_path) {
    FILE *f = fopen(meta_path, "r");
    if (!f) return 0;
    char buf[256] = {0};
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    int err = ferror(f);
    fclose(f);
    if (err) return 0;
    buf[n] = '\0';
    return strstr(buf, "\"files\":1") != NULL;
}
```
In `cmd_restore`:
- Insert, after the `objlock_wrlock` call succeeds and before the "Refuse if any live subdir is non-empty" block (`meta_src` is declared later, so use a new variable and duplicate the path), compute `char fmeta[PATH_MAX]; snprintf(fmeta, sizeof(fmeta), "%s/object.json", src_dir); int bak_files = backup_manifest_has_files(fmeta);`.
- Immediately **after** the existing `if (!force) { ... }` refusal block (the three-directory refusal keeps its precedence), add — this check therefore runs before the later layout/schema/fields.conf refusals; in the rare state where only `files/` is non-empty it answers the files error first, and nothing is mutated before it — if `bak_files && !force`: `opendir` `<obj_dir>/files`; if it opens and has any entry other than `.`/`..`, `closedir`, `objlock_wrunlock(db_root, object)`, `OUT("{\"error\":\"files/ is not empty (use force=true to overwrite)\"}\n")`, `return 1`.
- After the existing wipe-and-copy `for` loop over `subs[]` and **before** the `/* The restored indexes/ replaces this object's flag/marker state` comment, add: `if (bak_files) { snprintf(dst_sub, ...,"%s/files", obj_dir); rmrf(dst_sub); snprintf(src_sub, ...,"%s/files", src_dir); if (stat(src_sub, &st) == 0) cprf(src_sub, dst_sub); }` (reuse the function's `src_sub`/`dst_sub`/`st`, matching how the loop uses them).

**2c. Stale comments (comment-only).** In `query_maint.c`: the `cmd_backup` comment starting "Recursively copy data/, indexes/, and metadata/" and the `cmd_restore` header comment ("copies data/ + indexes/ + metadata/ + fields.conf") — add `files/` and `tokens.conf` (restore: files when the backup carries the marker; tokens are merged by the dispatcher). In `src/test/cases/test_backup_objlock.c`: the header comment describing backup as a copy of data/indexes/metadata — add files/ and tokens.conf.

**2d. `server.c`: `restore_merge_object_tokens`.** Add a static function **after** the definition of `save_tokens_conf_full` (and therefore after `load_one_tokens_file`/`token_set_add_full`) and before its first use:
```c
/* After a successful restore: add the backup's object tokens (if the backup
   carries a tokens.conf) to the running token store and rewrite the object's
   live tokens.conf from the store, like add-token. A token string that is
   already present (in any scope) is left exactly as it is, nothing is ever
   removed, and the scope is always (dir, object), so a restored file can
   never grant access beyond the restored object or change an existing token. */
static void restore_merge_object_tokens(const char *raw_db_root, const char *dir,
                                        const char *object, const char *from) {
    if (!dir || !object || !from || !from[0]) return;
    if (!is_valid_dir(dir) || !is_valid_object(object)) return;
    if (strchr(from, '/') || strstr(from, "..")) return;
    char path[PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/%s/%s/backup/%s/tokens.conf",
                     raw_db_root, dir, object, from);
    if (n < 0 || (size_t)n >= sizeof(path)) return;
    int fd = open(path, O_RDONLY | O_NOFOLLOW | O_NONBLOCK);  /* no symlinks, never block on a FIFO */
    if (fd < 0) return;
    struct stat tst;
    if (fstat(fd, &tst) != 0 || !S_ISREG(tst.st_mode)) { close(fd); return; }
    FILE *f = fdopen(fd, "r");
    if (!f) { close(fd); return; }
    token_store_init();
    int added = 0, skipped_present = 0, skipped_full = 0;
    pthread_mutex_lock(&g_token_lock);
    char line[512];
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n\r")] = '\0';
        char *p = line; while (*p == ' ' || *p == '\t') p++;
        if (!*p || *p == '#') continue;
        char tokbuf[256]; uint8_t perm;
        if (!parse_token_line(p, tokbuf, sizeof(tokbuf), &perm)) continue;
        if (token_find_slot(tokbuf) >= 0) { skipped_present++; continue; }
        if (g_token_count >= g_token_cap) { skipped_full++; continue; }
        token_set_add_full(tokbuf, dir, object, perm);
        added++;
    }
    fclose(f);
    if (added > 0) save_tokens_conf_full(raw_db_root, dir, object);   /* no rewrite when nothing was added */
    pthread_mutex_unlock(&g_token_lock);
    LOG_AUDIT(LOG_SUB_AUTH, "AUTH restore-tokens scope=%s/%s from=%s added=%d present=%d full=%d",
              dir, object, from, added, skipped_present, skipped_full);
}
```
Adapt names exactly to Phase 0 item 4 findings (if `token_store_init` is not visible there, `g_token_count`/`g_token_cap` are named differently, `<fcntl.h>`/`<stdio.h>` are not already included in server.c, or `LOG_AUDIT`'s arguments differ, follow the `add-token` handler's usage and record it). Note `token_find_slot` ends its probe at the first unused slot, so after a `remove-token` hole an already-present token further down the chain may be missed and re-added as a duplicate line (the pre-existing removal defect, Out of scope). Call site:
- JSON branch: replace `cmd_restore(db_root_eff, object, fromv, force);` by `if (cmd_restore(db_root_eff, object, fromv, force) == 0) restore_merge_object_tokens(raw_db_root, dir, object, fromv);` (use the Phase 0-recorded name of the raw db root variable; keep `free(fromv)` after it).
- Text branch: **no code change** (decision 9); the only edit is one comment line, added above the existing `cmd_restore(eff_root, ...)` call there: `/* The text path has no authentication and deliberately does not merge restored tokens (JSON restore does). */`.
Build: no new warnings (no unused-function warning: the call site exists).

**2e. `server.c`: `restore` becomes an object-admin mode.** In `mode_admin_level`, in the `obj[]` array, change the line `"truncate", "vacuum", "backup", "recount",` to `"truncate", "vacuum", "backup", "restore", "recount",` (exact-once anchor: `rtk grep -c '"truncate", "vacuum", "backup", "recount",' src/db/server.c` → 1; check it in Phase 0). Do not change any other classification. `restore` still bypasses the `fields.conf` pre-check as before; only its auth level changes.

**Green proof:** `test-backup-restore-files` all pass; `test-restore-old-backup-keeps-files` all pass; `test-restore-merges-object-tokens` all pass; run also the existing `test-restore` (it runs against a localhost-trusted daemon, so it is unaffected by the auth change — confirm), `test-backup-fences-writers`, `test-bmfix-backup-allowed-when-disabled`, `test-token-perms`, `test-per-tenant-auth`, `test-tenant-mgmt` → green. Then run the Phase 1C probe and act per its recipe.

**Mutation proofs (hand-revert, no git writes).** First `rtk proxy git diff > <scratch>/plan-k-full.patch`.
- m1: delete the `cprf` insert for `files/` in 2a only; build; run `test-backup-restore-files` → failing set **exactly** `{"backup copied a.txt","backup copied b.bin","a.txt restored with original bytes","b.bin intact after restore"}` (marker still written, so restore wipes live files and finds none to copy). Record actual; halt on mismatch.
- m2: restore; replace the standalone `if (bak_files) {` (the files-restore block after the `subs[]` loop — **not** the `if (bak_files && !force)` refusal) by `if (1) {`; build; run `test-restore-old-backup-keeps-files` → failing set **exactly** `{"restore from a marker-less backup leaves live files/ untouched (x.txt kept)","restore from a marker-less backup does not resurrect a.txt"}`. Record actual; halt on mismatch.
- m3: restore; remove `,\"files\":1` from the backup `fprintf`; build; run `test-backup-restore-files` → failing set **exactly** `{"manifest marks files as governed","a.txt restored with original bytes","c.txt added after the backup is gone after restore"}`. Record actual; halt on mismatch.
- m4: restore; replace the line `if (cmd_restore(db_root_eff, object, fromv, force) == 0) restore_merge_object_tokens(raw_db_root, dir, object, fromv);` by `cmd_restore(db_root_eff, object, fromv, force);` (the merge call removed, the restore call kept); build (an `unused function` warning for `restore_merge_object_tokens` is expected here and only here); run `test-restore-merges-object-tokens` → failing set **exactly** `{"restored object token authenticates immediately","live tokens.conf rewritten with the restored token"}`. Record actual; halt on mismatch.
- m5: restore; in 2a change `copy_file(src_sub, dst_sub, 0600)` to `copy_file(src_sub, dst_sub, 0644)`; build; run `test-restore-merges-object-tokens` → failing set **exactly** `{"backup tokens.conf is 0600"}`. Record actual; halt on mismatch.
- m6: restore; in `restore_merge_object_tokens` delete the `if (token_find_slot(tokbuf) >= 0) { skipped_present++; continue; }` line (so an existing token is overwritten by `token_set_add_full`); build; run `test-restore-merges-object-tokens` → failing set **exactly** `{"restore does not change an existing token's perm"}`. Record actual; halt on mismatch.
- m7: restore; remove `"restore", ` from the `obj[]` list; build; run `test-restore-merges-object-tokens` → failing set **exactly** `{"rw object token cannot restore"}`. Record actual; halt on mismatch.
- Restore all: `rtk proxy git diff | diff - <scratch>/plan-k-full.patch` prints nothing; rebuild; all new cases green. Paste every output; `rm -rf /tmp/shard-db-*` after each run.

### Phase 3 — docs (same plan)
Plan J changed several statements to say "`files/` is not copied"; flip them and add tokens:
1. `docs/operations/backup.md`: "What it copies" sentence and the directory tree now include `files/` and `tokens.conf` (and `metadata/`). Add: `tokens.conf` holds token secrets in plaintext — the backup copy is created `0600`, but protect the backup directory; backup time under the exclusive lock grows with stored-file volume; in the Restore section: a backup carrying files replaces live `files/` (refusing without `force` when it is non-empty) and a backup taken before this change leaves live `files/` untouched; restore **adds** the backup's object tokens to the running daemon immediately (no restart) and rewrites the object's `tokens.conf`; it adds only tokens that are not already present (an existing token is never changed) and never removes any, so a token revoked after the backup returns if the backup contains it, and live tokens absent from the backup are kept; the object's `tokens.conf` is rewritten from the in-memory token set (hand edits made after startup are lost, as with `add-token`); a full token table (`TOKEN_CAP`) skips the remaining tokens and logs it; `restore` requires an object-admin (`rwx`) token on the JSON protocol — the legacy text-wire protocol has no authentication at all (pre-existing), does not merge tokens, and is not covered by that requirement. Add the new `files/ is not empty (use force=true to overwrite)` refusal to the "Errors you might see" list in this file. Add a short note for the two known windows (partial `files/` copy after a silent copy error; a crash after the restore wipe → re-run `restore --force`). Put all restore wording in this file only.
2. `docs/query-protocol/schema-mutations.md` has a `## backup` section but **no** `## restore` section: update the `## backup` intro ("Copies the object's `data/`, `indexes/`, and `metadata/` directories") to include `files/` and `tokens.conf`, and add one sentence pointing to `docs/operations/backup.md` for the restore rules.
2b. `docs/cli/index.md` line about `backup` ("Copy the object's data + metadata + indexes to a timestamped backup directory.") → include files and tokens. `src/cli/main.c:1651` ("snapshot copy of the object's data") and `src/db/main.c:52` ("Backup data") stay; record them as judged non-stale hits. `docs/reference/error-codes.md` has no restore entries today: add none, record the judgement. `npm/index.d.ts` documents `restore` ("Required if the live object isn't empty"): add one line saying the JSON `restore` needs an `rwx` token covering the object if that comment block describes permissions, otherwise record it as judged non-stale. `backup.md`'s existing error list carries a message ("refusing to overwrite non-empty live object") that the code never emits; leave it and record the judgement.
3. `docs/operations/deployment.md` line `2. **Per-object \`backup\` command**`: "data/indexes/metadata" → "data/indexes/metadata/files and the object's tokens".
4. `docs/concepts/multi-tenancy.md`: where object-scoped `tokens.conf` is described (line ~94), add one sentence: `backup` copies it (0600) and `restore` adds its tokens to the running token set (existing tokens are never changed), and `restore` itself requires an `rwx` token covering the object (the table above is now accurate).
5. `docs/query-protocol/files.md`: add one sentence: stored files are included in `backup`/`restore`.
6. `docs/reference/changelog.md`, `## Unreleased`: re-read it first; append a new paragraph at the end without touching existing ones: "**`backup`/`restore` now cover `files/` and the object's `tokens.conf`.** ..." — behaviour changes (larger backups and a longer copy under the object lock; restore replaces `files/` for new backups only; restore adds tokens live, never removes or changes existing ones), the 0600 copy mode, **and a security fix:** on the JSON protocol `restore` is now an object-admin mode (`rwx` token covering the object) as `multi-tenancy.md` always documented — previously any token covering the object, even read-only, could run it; clients using such tokens to restore must switch to an `rwx` token (the legacy text-wire protocol remains unauthenticated, as before), and "No wire, CLI or config change; `object.json` gains `"files":1`."
7. Stale-claim checks (each must print nothing, or only hits you judge to be about something else — record each judgement): `rtk grep -n -i 'does not copy.*files\|not .*files/' docs/operations/backup.md docs/query-protocol/schema-mutations.md docs/operations/deployment.md`; `rtk grep -n -i -F '**not** copied' docs/operations/backup.md docs/query-protocol/schema-mutations.md docs/operations/deployment.md` (must print nothing after the edit); `rtk grep -n -i 'never copied\|not copied' docs/operations/backup.md docs/query-protocol/schema-mutations.md docs/reference/changelog.md`; `rtk grep -rn 'data + metadata + indexes' docs` → nothing; `rtk grep -rn -i 'copies the object' docs/query-protocol/schema-mutations.md` → judge each hit.

### Phase 4 — full suite and hand-back
1. `rtk ./build/bin/shard-db-test run-all --jobs 4`; real exit status; `rm -rf /tmp/shard-db-*`. Expected: total cases = `N0 + 3` (`+4` if the 1C probe adds the refusal case), **0 failed**. A pass-on-rerun is a bug to root-cause, never a rerun-until-green.
2. Definition of done: no new warnings; no TODO/FIXME/debug output; no mutation left (the diff check above is empty); `rtk proxy git status --short` shows only: `build.sh`, `src/db/query_maint.c`, `src/db/server.c` (token merge function, one JSON call site, one comment line on the text path, one `mode_admin_level` word), `src/test/cases/test_backup_objlock.c` (header comment only), the docs listed in Phase 3 (including `docs/cli/index.md`, `docs/concepts/multi-tenancy.md`) and any other doc you had to edit (list it in the evidence), new `src/test/cases/test_backup_files.c` and `src/test/cases/test_restore_tokens.c`, plus the untracked plan and evidence files of earlier plans. `slotcask.c` and `config.c` are **unmodified**. Nothing committed.
3. Hand back: diff stat, base-red / green / mutation outputs, the 1C probe result, full-suite totals, any deviation.
