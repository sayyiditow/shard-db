# R5 — Authoritative layout metadata, version gating, durable publication, conversion and recovery

> Executed; R5b and R6 landed on top of it. Superseded hunks were not applied.

Status: **REV 1 — PART A (Tasks 0–4) and PART B (Tasks 5–12) written; executable once approved. D9 and D10 CONFIRMED by the human.** All of D1–D8 are decided. Part A hunks below are complete and anchored on text read at HEAD `b6911a1`. The human must approve §2 (D1–D8). Release/min-version and live-vs-gated are decided (see D2, D4); executor is GLM 5.3 Flash (rev 1 hunks must therefore be complete, literal and anchor-quoted — no judgment left to the executor). Exact quoted-anchor code hunks (CORE-PROCESS "a plan is not done until…") are written in rev 1, *after* the decisions, because D1/D2/D4 change every hunk's site. No code is written under this revision.

Parent: `docs/plans/2026-10-19-type-correctness-recovery-plan.md` row R5 = Phase 3C + Phase 8. R1–R4 are committed (R4 = `b6911a1`). R6 (public activation, application acceptance, sanitizer gate, push/PR) and R7 (varchar NUL audit) are out of scope. **No sanitizers and no benchmarks in this plan** (human runs sanitizers once, in R6). Concurrency constraint of the parent plan applies: no new locks, no new lock order, no stronger reader semantics; conversion uses the existing exclusive per-object lock and `RebuildTxn`.

## 1. Findings in the current checkout (verified by reading source at HEAD `b6911a1`)

| # | Finding | Where |
|---|---|---|
| F1 | **The loader never binds a packed layout.** `TypedSchema.packed` is documented "Existing loaders never bind packed layouts"; `ts->packed.nfields == 0` ⇒ legacy everywhere. Packed objects exist only in tests that hand-build a `TypedSchema`. | `types.h` (`PkrSchema packed;` comment), `config.c` `load_typed_schema` |
| F2 | **The `schema.conf` parser silently ignores unknown tokens** ("Liberal … unknown tokens are silently ignored so the format stays forward-compatible") and reads only `splits`, `max_key`, the literal `2`, `streams`, `auto_key=`. A `layout=packed` token written by a new binary would be read as *legacy* by every current binary. A token is therefore **not** a downgrade barrier. | `config.c` schema-line parser (`parsed_version`, tail scan) |
| F3 | The only real downgrade barrier is the root version file: `shard_db_version_check` → `DOWNGRADE` refuses a binary older than the on-disk stamp; `SHARD_DB_VERSION` is `2026.10.2`, `SHARD_DB_REQUIRED_SOURCE_VERSION` `2026.09.2`. The stamp is written at startup *after* `shard_db_recover_before_stamp` + `shard_db_validate_before_stamp`. | `version.h`, `server.c` `cmd_server`, `embedded.c` |
| F4 | Startup validation (`shard_db_validate_before_stamp`) checks only: object dir exists, `fields.conf` readable, `data/kf` + `data/streams` dirs, segment-file names vs `streams`. It does **not** parse `fields.conf` or the layout. | `embedded.c` |
| F5 | `create-object` import already carries `declaration_layout` (`"legacy"`/`"packed"`) in describe output, and the create path **rejects anything but `"legacy"`** ("packed or unknown schema layout import is inactive until migration integration"). `describe-object` derives it from `ts->packed.nfields` (so always `"legacy"` today). | `server.c` create-object, `query_schema.c` describe |
| F6 | `load_typed_schema` cache key is `"%s:%s"` of `(db_root, object)` where the caller passes the *effective root*; negative results (missing `fields.conf`) are cached as non-typed. Identity/duplicate-name-across-tenants behaviour must be re-proved once layout is bound. | `config.c` |
| F7 | `RebuildTxn` already stages `preparing/ → active/ → done/` directories, backs up `data/` to `legacy_data`, snapshots `fields.conf` to `fields_rollback`, and `rebuild_txn_abort` restores fields, `schema.conf` splits/streams (`update_schema_conf_splits_streams`), caches and (optionally) indexes. It restores **splits/streams only** — it has no notion of a layout field in the manifest. `rebuild_txn_commit` is a single `rename(active, done)`. | `objlock.c` |
| F8 | KFM2 V2 markers re-derive record bytes from the *named segment records*, so a pending marker is layout-independent in content but is replayed through the *current loaded schema* for index apply. A marker pending across a layout change would be replayed against the wrong schema. | `docs/concepts/concurrency.md`, `embedded.c` recovery |
| F9 | Segment-record flags already distinguish layouts: `SREC_FLAG_NULLMAP 0x01`, `SREC_FLAG_PACKED 0x02`, `srec_flags_valid` accepts only 0, 1, 3. Record-level identification therefore exists; object-level identification does not. | `types.h` |
| F10 | `schema_wire.c` export already cross-checks `fields.conf` declaration text against `ts->packed` bindings (kind, payload_max, bare, currency, precision, scale) and fails `EILSEQ` on mismatch — this is the model for load-time agreement checks. | `schema_wire.c` |

## 2. Decisions needed (recommendation first)

| ID | Decision | Options | Recommendation |
|---|---|---|---|
| D1 | **Where the authoritative layout lives** — **APPROVED; written by the startup migration (D3) for existing objects and by `create-object` for new ones.** | `layout=packed` token on the object's `schema.conf` line | Strict parser: unknown/duplicate/malformed `layout=` ⇒ fail closed, never legacy fallback. `fields.conf` declarations must agree with the layout (F10 model). The token is *not* the downgrade barrier (F2) — the version stamp is (D2). |
| D2 | **Downgrade barrier and release number** — **DECIDED 2026-11-02** | `SHARD_DB_VERSION` = `2026.11.1`; `SHARD_DB_REQUIRED_SOURCE_VERSION` = `2026.10.2` (the previous release) | Roots must carry 2026.10.2 clean-open evidence to open under 2026.11.1; older roots get the existing TOO_OLD upgrade hint (install 2026.10.2, start once, then upgrade). Rule: **no packed layout may be published unless the running process has already stamped the new version** (assert at publication; embedded open path verified in Task 0). Because F2 means a token cannot protect an un-stamped root, the stamp *is* the barrier. |
| D3 | **Conversion mechanism** — **REVISED 2026-11-02: automatic startup migration of every object; no operator-invoked mode.** | Same pattern as the 2026.10.2 BTRI migration (`embedded.c`): in `shard_db_recover_before_stamp`, after artifact recovery, per object under `objlock_wrlock`, convert via `RebuildTxn`, then write a durable completion stamp (`.layout-migrated`, temp-write → fsync → rename → dir fsync; any failure refuses startup). Restartable: already-converted objects (layout token present) are skipped, so a crash mid-run is safe. | New objects are created packed (all objects are packed after R5). No `convert-object-layout` mode. |
| D4 | **Live vs gated** — **DECIDED 2026-11-02: live in R5, no test seam.** | R5 ships real production code: loader binding, packed `create-object`, the automatic startup migration, recovery. Nothing is released from this branch before R6 (acceptance, docs/clients, sanitizers, push/PR), so a throwaway gate would only add code to write and later remove. | The `create-object` import rejection (F5) is lifted in Task 5. R6 is acceptance + the pre-push gate, not a switch. (Only fixture seam: D10, TEST_BUILD-only legacy create.) |
| D5 | **Legacy `numeric` conversion** | per Phase 8: headerless legacy numeric is 8-byte scaled int64; legacy `numeric:2` means scale 2 (not packed scale 0); legacy P is informational | Conversion emits an explicit full declaration; audits stored values against the target precision and **refuses with field/key diagnostics** (bounded list) if any value would violate it; never rounds/widens silently; a human widens P or chooses bare in the request. |
| D6 | **Packed objects and layout-changing ops in R5** | support all (add/remove/edit field, vacuum compact, restore/rebuild, reshard) vs refuse some | Supported ops are all of them. **Guard-first ordering:** because packed objects become creatable in Task 5/6, land a **fail-closed guard first** (every layout-changing op refuses a packed object with a clear error until its recompose task lands), then enable each op in its own task. Splits-only copy is legal only when encoding/layout are unchanged (Phase 8). |
| D7 | **Preconditions for migration** — **REVISED 2026-11-02: the previous version must have closed cleanly; no marker draining.** | Before migrating, require (a) the previous binary's clean-shutdown evidence (the clean flag `shard_db_close`/server shutdown already records) and (b) zero pending KFM2 markers. | If either fails, **refuse to start** with an instruction: start the previous version (2026.10.2), let it stop cleanly, then upgrade. The migration never replays markers and never touches an unclean root. Removes the marker-vs-schema problem (F8) entirely. |
| D8 | **Backup/restore** | restore verifies layout + version | Backup captures the schema line incl. layout token and `fields.conf`; restore goes through the same strict loader and refuses a layout newer than the stamped version. Restore of a packed backup on a pre-D2 stamp is refused. |

## 3. Invariants (executor may not relax)

1. Layout is bound **before** any field is parsed; the binding is a single source of truth read once per load.
2. Any failure to establish the layout (unreadable manifest, bad token, declaration/layout disagreement, unreadable `fields.conf`) ⇒ load error, **never** legacy fallback and never a negative-cached "non-typed".
3. Publication order for any layout change: all new object files durable (checked `write`/`fsync`/`close`) → directory sync → manifest line replaced atomically (`write tmp` → `fsync` → `rename` → dir `fsync`) → caches invalidated **after** success. EIO/ENOSPC/EACCES never returns success.
4. After a crash at any point the object is wholly old or wholly new (schema + data + index); startup either completes forward or rolls back via the `RebuildTxn` path — no mixed interpretation.
5. Old record bytes are never reinterpreted by new-layout code, and vice-versa (record flags checked before field access — F9).
6. No new lock, no changed lock order, no new reader blocking (parent constraint).
7. Request/record bounds: 4 MiB bare varchar and numeric digit limits unchanged; `MAX_REQUEST_SIZE`, `QUERY_BUFFER_MB`, 128 MiB segment cap unchanged and not silently raised.

## 4. Consumer inventory (to be completed mechanically in Task 0, re-run at HEAD)

Changing the loader/manifest touches every reader of: `load_typed_schema` callers; `read_schema`/schema-line parsing in `config.c`; `startup_read_schema` + `shard_db_validate_before_stamp` (`embedded.c`); `update_schema_conf_splits_streams` + `rebuild_txn_*` (`objlock.c`); `create-object`/`describe-object`/export/import (`server.c`, `query_schema.c`, `schema_wire.c`); backup/restore, vacuum, reshard, add/remove/edit-field, reindex (`query_maint.c`, `query_schema.c`); version functions (`version.h`, `types.h`, `server.c`, `embedded.c`). Task 0 records the exact `grep` outputs and the count of `typed_record_view` call sites in the plan evidence file; any consumer found that is not listed here halts execution (stop-and-ask).

## 5. Tasks — SUPERSEDED OUTLINE

The task list that was here described rev 0 (a `convert-object-layout` mode, Tasks 0–11). **The authoritative tasks are Part A (Tasks 0–4) and Part B (Tasks 5–12) below.** Mapping: 0 characterization · 1 strict token parser · 2 loader binds packed · 3 version bump/startup agreement · 4 checked `schema.conf` rewrite · 5 packed `create-object` · 6 fail-closed guard (D9) · 7 metadata helpers + `RebuildTxn` layout restore · 8 conversion core + startup migration · 9 refusal/crash-point proofs · 10 backup/restore · 11 default flip + triage · 12 bounds + docs.

Definition of done (this plan): fresh full normal suite, zero new warnings, all revert-proofs pasted, docs in the same branch, raw-diff review by a reviewer blind to this plan (on-disk format, version gating, crash recovery). Sanitizers are the R6 gate. Left **uncommitted** for review per repo standing exception.

## 6. Edge cases (all pinned by tests in Part A/B)

- Layout token present on a line whose object has no `fields.conf`; `fields.conf` present but manifest line missing.
- Crash between manifest rename and cache invalidation; between `fields.conf` and manifest publication (which order, per invariant 3 — rev 1 pins it with a diagram).
- Object with zero records; object with only tombstones; maximum `streams`/`splits`; removed fields (`:removed`) in descriptor order.
- Legacy numeric with P larger than stored values need (informational P); noncanonical P; scale edits in the same request.
- Concurrent readers during conversion see the old object until commit (existing exclusive object lock semantics, no new blocking).
- Two daemons on one root: single-instance flock already prevents; embedded + daemon not supported.

## 7. Resolved / remaining

Approved: D1, D5, D6, D8; D3 and D7 revised per the human (automatic startup migration; clean prior shutdown and zero markers required, no draining); D2, D4 decided; executor GLM 5.3 Flash.

**D5 under automatic migration — DECIDED 2026-11-02 (a):** if a legacy `numeric:P,S` object holds a stored value that does not fit P, startup **refuses** with bounded field/key diagnostics and touches nothing; the operator fixes it with the previous version (e.g. widens P), stops it cleanly, then upgrades. Never auto-widen, round or drop a declared constraint.

All decisions D1–D8 are closed. Rev 1 (Parts A and B below) holds the executor hunks. Part B adds D9–D11 (see B.1); D9/D10 need the human's confirmation.

---

# REV 1 — PART A: executable hunks for Tasks 0–4

**Execution rules (embedded, per CORE-PROCESS).** Branch off the default branch is already satisfied (work continues on `fix/type-correctness-recovery`, human's choice); do tasks in order; build `SKIP_TESTS=1 ./build.sh`; test `./build/bin/shard-db-test run <case>` per task, then `./build/bin/shard-db-test run-all` fresh at the end of each task; leave everything **uncommitted**. If a quoted anchor is not found **exactly once**, write `PLAN_NOTES.md` describing the mismatch and **halt the whole run** — do not guess, do not continue to another task. If you hit a decision this plan does not cover, stop and ask. Never weaken/skip/delete a test to get green; a test that fails then passes on rerun is a bug to root-cause, not to rerun. Paste real command output for every RED and GREEN step.

## A.0 Additional findings (verified in source at HEAD)

| # | Finding | Consequence |
|---|---|---|
| F11 | `startup_schema_line` (`embedded.c`) tokenizes with `strtok_r` into `parts[7]` and checks `n > 7`. The fill loop stops at 7, so the check never fires: a line with a **8th token** (e.g. `auto_key=uuid` + `layout=packed`) is accepted and the 8th token is **silently dropped** — the daemon does not refuse, but it also never sees the layout token (and a malformed 8th token would be ignored). | Part A hunk 1f raises the cap to 8 and validates the `layout` token strictly. `test-layout-token-with-auto-key-parses` is a regression pin (GREEN on base); the true RED is `test-layout-token-with-auto-key-malformed-refuses`. |
| F12 | `update_schema_conf_splits_streams` (`query_find.c`) re-emits every trailing token verbatim, so a `layout=packed` token survives splits/streams rewrites. Its tail is **unchecked**: `fclose(fout)` result ignored, no `fsync`, no parent-dir sync, `rename` result only. `create-object` appends the line with `fopen("a")`/`fclose` unchecked, no fsync. | Task 4 hardens the rewrite; create-append hardening is Part B (Task 5 owns create). |
| F13 | An exact binder already exists: `typed_schema_bind_packed(TypedSchema*, const char *const *types, size_t n, char *err, size_t errsz)` (`typed_row.c`). It needs every `ts->fields[i].type` pre-set (not `FT_NONE`) and one type token per field (`varchar[:N]`, `numeric[:P[,S]]`, `currency`, or a fixed-type token such as `int`, `enum(a,b)`). It overwrites `offset`, `size`, `numeric_scale`, `nullmap_prefix`, `total_size`. | Task 2 reuses it; no new layout arithmetic. |
| F14 | In the legacy loader, bare `varchar` / unknown types parse to `FT_NONE` and the line is **silently dropped** (`continue`). Under a packed layout that would shift every later field. | Task 2: in packed mode an unparseable field line **fails the load**. |
| F15 | `durability_publish_replace(target, tmp, after_rename_cb, ctx)` already exists (`durability.c`): fsync(tmp) → rename → fsync(parent). Returns `0` ok, `-1` failed before the rename (tmp left in place), `1` renamed but parent-dir sync failed. `durability_test_fsync_fail_on_call(n, errno)` (TEST_BUILD) injects an fsync failure on the n-th `durability_fsync` call. A db.env TEST knob pattern exists in `config.c` (`BTRI_STAMP_TEST_FAIL=`). | Task 4 uses it; no new primitive. |
| F16 | **Stamp ordering hazard.** `shard_db_recover_before_stamp` (where the startup migration will run) executes **before** `shard_db_version_stamp`. A crash after the first object is converted but before the stamp leaves a packed object under an old stamp; a 2026.10.2 binary would then open it as legacy. | **Rule for Task 6 (Part B):** write the 2026.11.1 stamp *before* publishing any packed layout. D7's "previous version closed cleanly + zero markers" is checked only on the **first** start (disk version < 2026.11.1); a restart that finds the 2026.11.1 stamp and no `.layout-migrated` **resumes** the migration after ordinary recovery. Recorded now so Task 3 does not contradict it. |
| F17 | `load_schema` returns a zeroed `Schema` as its failure signal (already used for the legacy-v1 refusal) and does not cache it. `load_typed_schema` returns `NULL` uncached on read/parse failure. | Layout errors use the same two signals: zeroed `Schema` / `NULL`, never cached, never legacy fallback. |

## Task 0 — Characterization (no production change)

Create `src/test/cases/test_layout_manifest.c` with **only** the characterization case below (it is extended in Tasks 1–2) and register it in `build.sh`. Add the case name to the `gcc … -o shard-db-test` list **immediately after** the line `    src/test/cases/test_scalar_grammar_core.c \`.

Also produce `docs/plans/2026-11-02-r5-evidence.md` containing the raw output of these commands (paste, don't summarize):

```bash
grep -rn "load_typed_schema(" src/db/*.c | wc -l
grep -rn "load_schema(" src/db/*.c | wc -l
grep -rn "typed_record_view(" src/db/*.c | wc -l
grep -rn "2026\.10\.2\|2026\.09\.2" src --include='*.c' --include='*.h'
grep -rn "schema.conf" src/db/*.c | grep -n "fopen\|rename"
grep -n "shard_db_version_stamp\|shard_db_recover_before_stamp\|shard_db_validate_before_stamp" src/db/embedded.c src/db/server.c
```

and the answer (with the quoted source lines) to these two verification questions — if either answer differs from §1/A.0, **halt per the stop rule**:
1. Does the embedded open path (`embedded.c`, the block calling `shard_db_recover_before_stamp` / `shard_db_validate_before_stamp` near the `shard_db_open` code) stamp the version before serving, in the same order as `cmd_server`?
2. Which of `durability_test_fsync_fail_on_call`, `btri_stamp_test_inject`, `index_test_fail_next_build` are reachable from a db.env TEST knob today?

Characterization test (complete file; it must PASS on the unmodified tree, proving the *current* behaviour that Tasks 1–2 change):

```c
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "test_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {
    char base[256];
    char db_root[300];
    int  port;
} LmRoot;

static void lm_init(LmRoot *m) {
    memset(m, 0, sizeof(*m));
    char tmpl[] = "/tmp/shard-db-lm-XXXXXX";
    char *d = mkdtemp(tmpl);
    ASSERT_NOT_NULL(d, "mkdtemp layout root");
    if (!d) return;
    snprintf(m->base, sizeof(m->base), "%s", d);
    snprintf(m->db_root, sizeof(m->db_root), "%s/data", d);
    ASSERT_EQ_INT(mkdir(m->db_root, 0755), 0, "mkdir db_root");
    m->port = test_pick_port();
    ASSERT_TRUE(m->port > 0, "pick port");
}

static void lm_env(const LmRoot *m, const char *extra) {
    char path[300];
    snprintf(path, sizeof(path), "%s/db.env", m->base);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f, "write db.env");
    if (!f) return;
    fprintf(f,
        "export DB_ROOT=\"%s\"\nexport PORT=%d\nexport TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n", m->db_root, m->port, m->base);
    if (extra && extra[0]) fprintf(f, "%s", extra);
    fclose(f);
}

static void lm_destroy(LmRoot *m) { tu_run_cmd("rm -rf %s", m->base); }

static TestClient *lm_connect(int port) {
    TestClientCfg cfg = { .host = "127.0.0.1", .port = port };
    return tc_connect(&cfg);
}

/* Rewrites the single schema.conf line for lay:o by appending `tail`
   (e.g. ":layout=packed"). Returns 0 on success. */
static int lm_schema_append(const LmRoot *m, const char *tail) {
    char path[400], tmp[420];
    snprintf(path, sizeof(path), "%s/schema.conf", m->db_root);
    snprintf(tmp, sizeof(tmp), "%s.lm", path);
    FILE *in = fopen(path, "r");
    FILE *out = fopen(tmp, "w");
    if (!in || !out) { if (in) fclose(in); if (out) fclose(out); return -1; }
    char line[1024];
    while (fgets(line, sizeof(line), in)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (strncmp(line, "lay:o:", 6) == 0) fprintf(out, "%s%s\n", line, tail);
        else fprintf(out, "%s\n", line);
    }
    fclose(in);
    if (fclose(out) != 0) return -1;
    return rename(tmp, path);
}

static int lm_build(const LmRoot *m, TestEnv *env) {
    if (test_env_start_at(env, m->db_root, m->port) != 0) return -1;
    TestClient *tc = lm_connect(m->port);
    if (!tc) return -1;
    char *r = NULL;
    int ok = tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"lay\"}", &r) == 0;
    free(r); r = NULL;
    ok = ok && tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"o\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:20\",\"qty:int\"]}",
        &r) == 0 && r && !strstr(r, "\"error\"");
    free(r);
    tc_close(tc);
    test_env_stop_keep(env);
    return ok ? 0 : -1;
}

/* Today: an unknown/valid-looking layout token is silently ignored and the
   object loads as legacy (finding F2). Task 1 changes this deliberately. */
static int test_layout_manifest_characterization(void) {
    LmRoot m;
    lm_init(&m);
    if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build(&m, &env), 0, "build legacy object");
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "append token");
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start with token");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"o\"}", &r),
            0, "describe");
        ASSERT_NOT_NULL(r, "describe resp");
        ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"legacy\""),
                    "token ignored: still legacy today (F2)");
        free(r);
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-layout-manifest-characterization", test_layout_manifest_characterization)
```

RED/GREEN: this case is GREEN on the base (it documents F2). Paste the output. It is **replaced** by the real cases in Tasks 1–2 (its assertion flips on purpose; that edit is pre-authorised).

## Task 1 — Strict `layout` token parser and `Schema.packed_layout`

**Root cause being fixed (F2/F11):** both `schema.conf` readers ignore or mishandle layout tokens: `load_schema`'s tail scan stops at `auto_key=` (`break`) and ignores everything else; `startup_schema_line` silently drops any 8th token. Without a strict parser a layout cannot be both recorded and trusted.

**1a. Test first.** Replace the Task-0 case body's final assertions and add cases (same file, same helpers). Add these cases and register each:

```c
/* packed token accepted by the strict reader; object stays servable */
static int test_layout_token_packed_loads(void) {
    LmRoot m;
    lm_init(&m);
    if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build(&m, &env), 0, "build legacy object");
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "append token");
    /* Task 1 alone only requires the token to PARSE and the process to start;
       binding is Task 2, so fields.conf is still legacy here: expect the
       load to be REFUSED by the loader (describe errors), not served as legacy. */
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start with token");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"o\"}", &r),
            0, "describe");
        ASSERT_NOT_NULL(r, "resp");
        ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"legacy\"") == NULL,
                    "packed token is never silently served as legacy");
        free(r);
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* malformed / duplicate / unknown layout tokens refuse startup */
static int test_layout_token_malformed_refuses(void) {
    static const char *const bad[] = {
        ":layout=legacy", ":layout=", ":layout", ":layout=packed:layout=packed",
        ":layout=Packed", ":layout=packed2"
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        LmRoot m;
        lm_init(&m);
        if (t_ctx->failed) return 1;
        lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build(&m, &env), 0, "build legacy object");
        ASSERT_EQ_INT(lm_schema_append(&m, bad[i]), 0, "append bad token");
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0,
                    "malformed layout token refuses startup");
        lm_destroy(&m);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* layout token together with auto_key must be accepted by the 8-part cap (F11).
   Regression pin: GREEN on base too (the 8th token is silently dropped there). */
static int test_layout_token_with_auto_key_parses(void) {
    LmRoot m;
    lm_init(&m);
    if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"lay\"}", &r); free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"o\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"qty:int\"],"
            "\"auto_key\":\"uuid\"}", &r), 0, "create auto_key object");
        free(r);
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "append token after auto_key");
    /* Must pass the startup parser (8 parts); the loader may still refuse the
       object (Task 2), but the DAEMON must start. */
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0,
                  "8-part line passes the startup parser");
    test_env_stop_keep(&env);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* a malformed 8th token after auto_key must REFUSE startup. On base the
   startup parser drops the 8th token and the daemon starts: this is the
   true RED for hunk 1f. */
static int test_layout_token_with_auto_key_malformed_refuses(void) {
    LmRoot m;
    lm_init(&m);
    if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"lay\"}", &r); free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"o\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"qty:int\"],"
            "\"auto_key\":\"uuid\"}", &r), 0, "create auto_key object");
        free(r);
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=legacy"), 0, "append malformed 8th token");
    ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0,
                "malformed 8th token refuses startup (not silently truncated)");
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-layout-token-packed-loads", test_layout_token_packed_loads)
TEST_REGISTER("test-layout-token-malformed-refuses", test_layout_token_malformed_refuses)
TEST_REGISTER("test-layout-token-with-auto-key-parses", test_layout_token_with_auto_key_parses)
TEST_REGISTER("test-layout-token-with-auto-key-malformed-refuses", test_layout_token_with_auto_key_malformed_refuses)
```

Delete the Task-0 characterization case and its `TEST_REGISTER` line (its premise, F2, is what Task 1 removes). Run `./build/bin/shard-db-test run test-layout-token-packed-loads` etc. **RED expected on base:** packed-loads fails (describe says `legacy`), malformed-refuses fails (daemon starts), with-auto-key-malformed-refuses fails (the daemon starts: the 8th token is silently dropped). `with-auto-key-parses` is GREEN on base — it is a regression pin, say so. Paste the RED output.

**1b. `types.h` — add the field.** Anchor (exactly once):

```c
    char auto_key_seq_name[128];   /* meaningful only when auto_key == AK_SEQ */
} Schema;
```
Replace with:

```c
    char auto_key_seq_name[128];   /* meaningful only when auto_key == AK_SEQ */
    /* 0 = legacy layout, 1 = packed layout. Set from the strict
       `layout=packed` schema.conf token (schema_tail_layout); a malformed
       token makes load_schema refuse the object instead of defaulting. */
    int packed_layout;
} Schema;
```

**1c. `types.h` — declarations.** Anchor (exactly once): `Schema load_schema(const char *effective_root, const char *object);` Replace with:

```c
Schema load_schema(const char *effective_root, const char *object);
/* Strict layout-token reader over the text after "dir:object:" of a
   schema.conf line. *out = 1 for exactly one `layout=packed` token, else 0.
   Returns 0 ok, -2 for any malformed/unknown/duplicate `layout` token. */
int schema_tail_layout(const char *tail, int *out);
/* Layout of an object from the global schema.conf. Returns 0 (*out set;
   0 when the object has no line or the root is outside DB_ROOT), -1 when
   schema.conf is unreadable, -2 for a malformed token. *out is 0 on error. */
int schema_conf_layout(const char *effective_root, const char *object, int *out);
```

**1d. `config.c` — the two helpers.** Anchor (exactly once, it is the comment immediately above `int load_splits`):

```c
/* Backward compat wrapper */
int load_splits(const char *db_root, const char *object) {
```
Insert **before** it:

```c
int schema_tail_layout(const char *tail, int *out) {
    int seen = 0;
    *out = 0;
    const char *p = tail;
    while (p && *p) {
        const char *next = strchr(p, ':');
        size_t tlen = next ? (size_t)(next - p) : strlen(p);
        if (tlen >= 6 && strncmp(p, "layout", 6) == 0 &&
            (tlen == 6 || p[6] == '=')) {
            if (seen) { *out = 0; return -2; }
            if (tlen != 13 || strncmp(p, "layout=packed", 13) != 0) {
                *out = 0;
                return -2;
            }
            seen = 1;
            *out = 1;
        }
        p = next ? next + 1 : NULL;
    }
    return 0;
}

int schema_conf_layout(const char *effective_root, const char *object, int *out) {
    *out = 0;
    size_t rootlen = strlen(g_db_root);
    if (strncmp(effective_root, g_db_root, rootlen) != 0) return 0;
    const char *dir = effective_root + rootlen;
    if (*dir == '/') dir++;
    char path[PATH_MAX], prefix[512];
    snprintf(path, sizeof(path), "%s/schema.conf", g_db_root);
    int pfxlen = snprintf(prefix, sizeof(prefix), "%s:%s:", dir, object);
    if (pfxlen < 0 || (size_t)pfxlen >= sizeof(prefix)) return -1;
    FILE *f = fopen(path, "r");
    if (!f) return errno == ENOENT ? 0 : -1;
    char line[4096];
    int rc = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, prefix, (size_t)pfxlen) != 0) continue;
        line[strcspn(line, "\r\n")] = '\0';
        rc = schema_tail_layout(line + pfxlen, out);
        break;
    }
    if (ferror(f)) rc = -1;
    if (fclose(f) != 0) rc = -1;
    if (rc != 0) *out = 0;
    return rc;
}

```

**1e. `config.c` — `load_schema` parses and refuses.** Three edits, each anchor exactly once.

(i) Anchor: `    int legacy_v1 = 0;` Replace with:
```c
    int legacy_v1 = 0;
    int layout_bad = 0;
```
(ii) Anchor: `            if (parsed_version != 2) legacy_v1 = 1;` Replace with:
```c
            if (schema_tail_layout(p, &s.packed_layout) != 0) layout_bad = 1;
            if (parsed_version != 2) legacy_v1 = 1;
```
(iii) Anchor: `    /* Derive max_value from typed fields.conf */` Replace with:
```c
    if (found && layout_bad) {
        /* Never fall back to legacy on a malformed layout token: an
           unreadable layout means the bytes cannot be interpreted. */
        fprintf(stderr, "Error: Object [%s] has a malformed schema layout token\n",
                object);
        Schema zeroed = {0};
        return zeroed;
    }

    /* Derive max_value from typed fields.conf */
```

**1f. `embedded.c` — startup parser (H6).** Edits in `startup_schema_line`, each anchor exactly once.

(i) `    char *parts[7] = {0};` → `    char *parts[8] = {0};`
(ii) `         part && n < 7; part = strtok_r(NULL, ":", &save))` → `         part && n < 8; part = strtok_r(NULL, ":", &save))`
(iii) `    if (n < 6 || n > 7 || !dir_name_ok(parts[0]) ||` → `    if (n < 6 || n > 8 || !dir_name_ok(parts[0]) ||`
(iv) Anchor: `    if (strcmp(parts[4], "2") != 0) return -1;` Replace with:
```c
    if (strcmp(parts[4], "2") != 0) return -1;
    {
        int layouts = 0;
        for (int i = 6; i < n; i++) {
            if (strncmp(parts[i], "layout", 6) != 0 ||
                (parts[i][6] != '\0' && parts[i][6] != '='))
                continue;
            if (layouts++ || strcmp(parts[i], "layout=packed") != 0) return -1;
        }
    }
```

**1g. GREEN.** `SKIP_TESTS=1 ./build.sh` (zero warnings), then the three cases pass **except** `test-layout-token-packed-loads`: at this point the loader still ignores the token inside `load_typed_schema`, so that case stays RED until Task 2 — expected; record it. malformed-refuses, with-auto-key-parses and with-auto-key-malformed-refuses must be GREEN. Revert-proof (`git stash` the three production files `types.h config.c embedded.c`): malformed-refuses and with-auto-key-malformed-refuses go RED for the stated reasons; restore ⇒ GREEN. Paste both outputs.

## Task 2 — Loader binds the packed layout (invariants 1, 2; F13, F14, F17)

**Root cause (F1):** `load_typed_schema` never reads the layout and never calls `typed_schema_bind_packed`; bare `varchar`/`numeric` declarations would additionally be dropped as `FT_NONE` (F14).

**2a. Test first** — add to `test_layout_manifest.c` (keep helpers). `lm_packed_fields` rewrites `fields.conf` for `lay/o`:

```c
static int lm_write_fields(const LmRoot *m, const char *body) {
    char path[400];
    snprintf(path, sizeof(path), "%s/lay/o/fields.conf", m->db_root);
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fputs(body, f);
    return fclose(f);
}

static char *lm_describe(int port) {
    TestClient *tc = lm_connect(port);
    if (!tc) return NULL;
    char *r = NULL;
    if (tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"o\"}", &r) != 0) r = NULL;
    tc_close(tc);
    return r;
}

/* matching manifest + declarations => bound packed layout */
static int test_loader_binds_packed(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1; lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build(&m, &env), 0, "build");
    ASSERT_EQ_INT(lm_write_fields(&m, "name:varchar\nqty:int\namt:numeric:12,2\n"), 0, "fields");
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "token");
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
    char *r = lm_describe(m.port);
    ASSERT_NOT_NULL(r, "describe");
    ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"packed\""), "packed bound");
    ASSERT_TRUE(r && strstr(r, "\"bare\":true"), "bare varchar preserved");
    free(r);
    test_env_stop_keep(&env); lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* packed manifest + legacy declarations => load refused, never legacy (F14) */
static int test_loader_packed_with_legacy_fields_refused(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1; lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build(&m, &env), 0, "build");
    ASSERT_EQ_INT(lm_write_fields(&m, "name:varchar:20\nqty:int\nbroken:nosuchtype\n"), 0, "fields");
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "token");
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "daemon starts");
    char *r = lm_describe(m.port);
    ASSERT_TRUE(r == NULL || strstr(r, "\"error\"") != NULL, "object refused, not served");
    ASSERT_TRUE(r == NULL || strstr(r, "\"declaration_layout\":\"legacy\"") == NULL,
                "never legacy fallback");
    free(r);
    test_env_stop_keep(&env); lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* the same object name in two tenants stays independent */
static int test_loader_same_object_two_tenants(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1; lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"ta\"}", &r); free(r); r = NULL;
        tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"tb\"}", &r); free(r); r = NULL;
        const char *mk =
            "{\"mode\":\"create-object\",\"dir\":\"%s\",\"object\":\"o\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"qty:int\"]}";
        char req[300];
        snprintf(req, sizeof(req), mk, "ta"); tc_request(tc, req, &r); free(r); r = NULL;
        snprintf(req, sizeof(req), mk, "tb"); tc_request(tc, req, &r); free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    /* mark only ta:o packed */
    char path[400], tmp[420], line[1024];
    snprintf(path, sizeof(path), "%s/schema.conf", m.db_root);
    snprintf(tmp, sizeof(tmp), "%s.lm", path);
    FILE *in = fopen(path, "r"), *out = fopen(tmp, "w");
    ASSERT_TRUE(in && out, "open schema.conf");
    if (in && out) {
        while (fgets(line, sizeof(line), in)) {
            line[strcspn(line, "\r\n")] = '\0';
            fprintf(out, "%s%s\n", line, strncmp(line, "ta:o:", 5) == 0 ? ":layout=packed" : "");
        }
    }
    if (in) fclose(in);
    if (out) fclose(out);
    ASSERT_EQ_INT(rename(tmp, path), 0, "publish schema.conf");
    snprintf(path, sizeof(path), "%s/ta/o/fields.conf", m.db_root);
    FILE *f = fopen(path, "w"); ASSERT_NOT_NULL(f, "ta fields");
    if (f) { fputs("qty:int\n", f); fclose(f); }
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "restart");
    tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 2");
    if (tc) {
        char *ra = NULL, *rb = NULL;
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"ta\",\"object\":\"o\"}", &ra);
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"tb\",\"object\":\"o\"}", &rb);
        ASSERT_TRUE(ra && strstr(ra, "\"declaration_layout\":\"packed\""), "ta packed");
        ASSERT_TRUE(rb && strstr(rb, "\"declaration_layout\":\"legacy\""), "tb legacy");
        free(ra); free(rb);
        tc_close(tc);
    }
    test_env_stop_keep(&env); lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-loader-binds-packed", test_loader_binds_packed)
TEST_REGISTER("test-loader-packed-with-legacy-fields-refused", test_loader_packed_with_legacy_fields_refused)
TEST_REGISTER("test-loader-same-object-two-tenants", test_loader_same_object_two_tenants)
```
RED on base: binds-packed (legacy / bare varchar dropped), packed-with-legacy-fields (served as legacy), two-tenants (ta shows legacy). Paste RED.

**2b. `config.c` — helper + loader edits.**

(i) Immediately above `TypedSchema *load_typed_schema(const char *db_root, const char *object) {` (anchor exactly once) insert:

```c
/* Packed layouts declare varchar/numeric/currency with the bare grammar that
   the legacy parse_field_type rejects (it would return FT_NONE and the field
   would be silently dropped). Pre-set the logical type so the line survives
   until typed_schema_bind_packed assigns the real width. */
static void packed_declared_type(const char *spec, TypedField *tf) {
    if (!strncmp(spec, "varchar", 7) || !strncmp(spec, "numeric", 7) ||
        !strcmp(spec, "currency")) {
        tf->type = spec[0] == 'v' ? FT_VARCHAR : FT_NUMERIC;
        tf->size = 1;   /* placeholder; overwritten by typed_schema_bind_packed */
        return;
    }
    parse_field_type(spec, tf);
}

static void ptypes_free(char **ptypes, int n) {
    for (int i = 0; i < n; i++) free(ptypes[i]);
}

```

(ii) Anchor (exactly once, inside `load_typed_schema`):
```c
    /* Cache miss — read from file */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/fields.conf", db_root, object);
    FILE *f = fopen(path, "r");
    if (!f) {
        /* No fields.conf — cache as non-typed */
```
Replace with:
```c
    /* Cache miss — establish the authoritative layout BEFORE parsing any
       field. A layout that cannot be established is a load failure: never
       cached, never a legacy fallback. */
    int layout_packed = 0;
    if (schema_conf_layout(db_root, object, &layout_packed) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG,
                  "%s/%s: schema layout unreadable or malformed — load refused",
                  db_root, object);
        return NULL;
    }
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/fields.conf", db_root, object);
    FILE *f = fopen(path, "r");
    if (!f) {
        if (layout_packed) return NULL;   /* packed object without fields.conf: refuse, do not negative-cache */
        /* No fields.conf — cache as non-typed */
```

(iii) Anchor (exactly once): 
```c
    ts.nfields = 0;
    int offset = 0;

    for (;;) {
        int rderr = 0;
```
Replace with:
```c
    ts.nfields = 0;
    int offset = 0;
    char *ptypes[MAX_FIELDS] = {0};   /* packed: one type token per accepted field */

    for (;;) {
        int rderr = 0;
```

(iv) Three early-return cleanups. Anchor A (exactly once):
```c
                for (int i = 0; i < ts.nfields; i++)
                    free_enum_values(&ts.fields[i]);
                fclose(f);
                return NULL;   /* not cached; next load retries the file */
```
Replace with:
```c
                for (int i = 0; i < ts.nfields; i++)
                    free_enum_values(&ts.fields[i]);
                ptypes_free(ptypes, ts.nfields);
                fclose(f);
                return NULL;   /* not cached; next load retries the file */
```
Anchor B (exactly once):
```c
        if(!default_check){free(line);fclose(f);for(int i=0;i<ts.nfields;i++)free_enum_values(&ts.fields[i]);return NULL;}
```
Replace with:
```c
        if(!default_check){free(line);fclose(f);for(int i=0;i<ts.nfields;i++)free_enum_values(&ts.fields[i]);ptypes_free(ptypes,ts.nfields);return NULL;}
```
Anchor C (exactly once):
```c
            for (int i = 0; i < ts.nfields; i++)
                free_enum_values(&ts.fields[i]);
            free_enum_values(tf);
            free(line);
            fclose(f);
            return NULL;
```
Replace with:
```c
            for (int i = 0; i < ts.nfields; i++)
                free_enum_values(&ts.fields[i]);
            free_enum_values(tf);
            ptypes_free(ptypes, ts.nfields);
            free(line);
            fclose(f);
            return NULL;
```

(v) Anchor (exactly once):
```c
        parse_field_type(type_spec, tf);
        free(line);
        if (tf->type == FT_NONE || tf->size <= 0) continue;

        tf->offset = offset;
        offset += tf->size;
        ts.nfields++;
        ts.typed = 1;
```
Replace with:
```c
        if (layout_packed) packed_declared_type(type_spec, tf);
        else parse_field_type(type_spec, tf);
        char *ptype_copy = layout_packed ? strdup(type_spec) : NULL;
        free(line);
        if (layout_packed && (!ptype_copy || tf->type == FT_NONE || tf->size <= 0)) {
            /* Packed: a field line we cannot parse is a load failure, never a
               silently dropped field (it would shift every later position). */
            free(ptype_copy);
            free_enum_values(tf);
            for (int i = 0; i < ts.nfields; i++)
                free_enum_values(&ts.fields[i]);
            ptypes_free(ptypes, ts.nfields);
            fclose(f);
            return NULL;
        }
        if (tf->type == FT_NONE || tf->size <= 0) continue;

        tf->offset = offset;
        offset += tf->size;
        ptypes[ts.nfields] = ptype_copy;
        ts.nfields++;
        ts.typed = 1;
```

(vi) Anchor (exactly once):
```c
    ts.nullmap_prefix = 1 + typed_nullmap_bytes(ts.nfields);
    ts.total_size += ts.nullmap_prefix;

    /* Store in cache, capture the slot we wrote to so we can return the
```
Replace with:
```c
    ts.nullmap_prefix = 1 + typed_nullmap_bytes(ts.nfields);
    ts.total_size += ts.nullmap_prefix;

    if (layout_packed) {
        char berr[160] = {0};
        if (typed_schema_bind_packed(&ts, (const char *const *)ptypes,
                                     (size_t)ts.nfields, berr, sizeof(berr)) != 0) {
            LOG_ERROR(LOG_SUB_CONFIG,
                      "%s/%s: packed layout does not match fields.conf (%s) — load refused",
                      db_root, object, berr[0] ? berr : "bind failed");
            for (int i = 0; i < ts.nfields; i++)
                free_enum_values(&ts.fields[i]);
            ptypes_free(ptypes, ts.nfields);
            return NULL;   /* not cached */
        }
    }
    ptypes_free(ptypes, ts.nfields);

    /* Store in cache, capture the slot we wrote to so we can return the
```

**2c.** `load_schema` already calls `load_typed_schema` for `max_value`; packed `ts->total_size` is `pkr.maximum` (set by the binder) so `max_value`/`slot_size` automatically become the packed *maximum validity bound* (see §Task 10 for the bounds test).

**2d. GREEN.** Build (zero warnings); run the three Task-2 cases and the three Task-1 cases (`test-layout-token-packed-loads` is now GREEN too); then `./build/bin/shard-db-test run-all` fresh. Revert-proof: stash `config.c`, expect RED for the three Task-2 cases, restore, GREEN. Paste outputs. **Edge cases the executor must test (add as assertions):** packed object with zero `fields.conf` lines ⇒ refused; field named with `:removed` suffix keeps its slot (bind receives the cleaned type token); `enum(a,b)` fixed field with `numeric` neighbour binds; `fields.conf` longer than `MAX_FIELDS` is cut by the existing `ts.nfields >= MAX_FIELDS` break (unchanged).

## Task 3 — Version bump, strict startup agreement, no stamp contradiction (D2, F16)

**3a. Test first.** Add `test-layout-startup-agreement` to `test_layout_manifest.c`: build the legacy object; write `fields.conf` with the packed text (as in `test_loader_binds_packed`) **but without** the manifest token; start; describe must report `legacy` **and** the object must be readable as a `legacy` object *or* refused — assert exactly the current behaviour of a bare `varchar` line in a legacy object (record it in the evidence file; this pins that "declarations without a token never silently become packed"). Then add `src/test/cases/test_version_gate_2026_11.c`:

```c
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "version.h"

static int test_version_gate_2026_11_run(void) {
    ASSERT_TRUE(strcmp(SHARD_DB_VERSION, "2026.11.1") == 0, "release is 2026.11.1");
    ASSERT_TRUE(strcmp(SHARD_DB_REQUIRED_SOURCE_VERSION, "2026.10.2") == 0,
                "minimum source is the previous release");
    ASSERT_EQ_INT(shard_db_version_decide("2026.10.2", 1, 0,
        SHARD_DB_VERSION, SHARD_DB_REQUIRED_SOURCE_VERSION), SHARD_DB_VERSION_STAMP,
        "previous release opens and re-stamps");
    ASSERT_EQ_INT(shard_db_version_decide("2026.09.2", 1, 0,
        SHARD_DB_VERSION, SHARD_DB_REQUIRED_SOURCE_VERSION), SHARD_DB_VERSION_TOO_OLD,
        "pre-previous release refuses with the upgrade hint");
    ASSERT_EQ_INT(shard_db_version_decide("2026.11.1", 1, 0,
        SHARD_DB_VERSION, SHARD_DB_REQUIRED_SOURCE_VERSION), SHARD_DB_VERSION_NOOP,
        "current release is noop");
    ASSERT_EQ_INT(shard_db_version_decide("2026.11.2", 1, 0,
        SHARD_DB_VERSION, SHARD_DB_REQUIRED_SOURCE_VERSION), SHARD_DB_VERSION_DOWNGRADE,
        "newer on-disk version refuses (downgrade barrier)");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-version-gate-2026-11", test_version_gate_2026_11_run)
```
Add `#include <string.h>` if the build warns. Register in `build.sh` after `src/test/cases/test_version_gate.c \` (find the exact line first; if absent halt per the stop rule). RED on base (version strings). 

**3b. `version.h`.** Anchor (exactly once): 
```c
#define SHARD_DB_VERSION "2026.10.2"
#define SHARD_DB_REQUIRED_SOURCE_VERSION "2026.09.2"
```
Replace with:
```c
#define SHARD_DB_VERSION "2026.11.1"
#define SHARD_DB_REQUIRED_SOURCE_VERSION "2026.10.2"
```

**3c. Fix tests that encode the old constants.** Run `grep -rn "2026\.10\.2\|2026\.09\.2" src/test/cases` (files known from audit: `test_null_migration.c`, `test_auto_key.c`, `test_trim_compact_oob_fields.c`, `test_multi_order_cursor.c`, `test_not_null_compat.c`, `test_null_storage.c`, `test_version_gate.c`). For each hit, the executor decides **only** between: (a) the literal is a *fixture value* meaning "an older on-disk version" ⇒ leave it; (b) it asserts the *current* binary's version/minimum ⇒ change to `SHARD_DB_VERSION` / `SHARD_DB_REQUIRED_SOURCE_VERSION` or the new literal. List every hit and its (a)/(b) classification in the evidence file. Any hit that fits neither ⇒ stop and ask. `test_version_gate.c` passes its own version strings and is not changed.

**3d.** Do **not** change `shard_db_not_null_upgrade_audit` (its `"2026.10.2"` comparison correctly means "root already at or past the NULL-bitmap release").

**3e. Docs for this task:** create `docs/release-notes/2026.11.1.md` (one paragraph: strict scalar grammars R4, packed layout metadata R5, upgrade requires a cleanly stopped 2026.10.2 root — final text in Part B), and add the 2026.11.1 header to `docs/reference/changelog.md` above the R4 entry.

**3f. GREEN.** Build; `test-version-gate-2026-11`, `test-layout-startup-agreement`, then fresh `run-all`. Revert-proof for `version.h`. Paste outputs.

## Task 4 — Checked `schema.conf` rewrite (invariant 3; F12, F15)

**Root cause:** `update_schema_conf_splits_streams` ignores the `fclose(fout)` result and never syncs the temp file or its directory; ENOSPC/EIO during the write returns success (`rename` runs on a truncated file). This function is what `RebuildTxn` abort/commit and reshard use to republish a layout-bearing line.

**4a. Test first.** Add a db.env TEST knob (hunk 4b) and the case in `test_layout_manifest.c`:

```c
/* a failed fsync during the schema.conf rewrite must surface as failure and
   leave the original schema.conf intact (old-or-new, never torn) */
static int test_schema_conf_rewrite_fsync_failure(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build(&m, &env), 0, "build");
    char path[400];
    snprintf(path, sizeof(path), "%s/schema.conf", m.db_root);
    char *before = tu_read_file(path);
    ASSERT_NOT_NULL(before, "read schema.conf before");
    /* fail the 1st durability_fsync call made inside the schema.conf rewrite */
    lm_env(&m, "export SCHEMA_CONF_TEST_FSYNC_FAIL_CALL=1\n");
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start with knob");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        /* documented reshard request (schema-mutations.md: vacuum + splits=N);
           splits 8 -> 16 reaches update_schema_conf_splits_streams */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"vacuum\",\"dir\":\"lay\",\"object\":\"o\",\"splits\":16}", &r), 0, "vacuum");
        ASSERT_TRUE(r && strstr(r, "\"error\""), "failed fsync surfaces as an error");
        free(r);
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    char *after = tu_read_file(path);
    ASSERT_NOT_NULL(after, "read schema.conf after");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0,
                "schema.conf unchanged after a failed rewrite");
    free(before); free(after);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-schema-conf-rewrite-fsync-failure", test_schema_conf_rewrite_fsync_failure)
```
RED on base: the knob is unrecognised, the request succeeds and `schema.conf` changes (the assertions "failed fsync surfaces as an error" and "unchanged" fail). Splits `8 → 16` is a real change, so `update_schema_conf_splits_streams` is called (`splits_changed` in `query_find.c`).

**4b. TEST-only injection (three edits).**

(i) `shard_db_internal.h`, anchor (exactly once):
```c
void btri_stamp_test_inject(int mode);
#endif
```
Replace with:
```c
void btri_stamp_test_inject(int mode);
/* Test-only (db.env SCHEMA_CONF_TEST_FSYNC_FAIL_CALL): arm the n-th
   durability_fsync call made inside the NEXT schema.conf rewrite to fail
   with EIO; no-op at 0. */
void schema_conf_test_fsync_fail_call(int n);
#endif
```
(ii) `config.c`, anchor (exactly once):
```c
            btri_stamp_test_inject(atoi(p + 21));
#endif
```
Replace with:
```c
            btri_stamp_test_inject(atoi(p + 21));
        } else if (strncmp(p, "SCHEMA_CONF_TEST_FSYNC_FAIL_CALL=", 33) == 0) {
            /* Test-only knob; not a documented production setting. */
            schema_conf_test_fsync_fail_call(atoi(p + 33));
#endif
```
Verify the prefix length first: `printf %s 'SCHEMA_CONF_TEST_FSYNC_FAIL_CALL=' | wc -c` must print `33` (halt otherwise).

(iii) `query_find.c`, anchor (exactly once): `int update_schema_conf_splits_streams(const char *db_root, const char *object,\n                                             int new_splits, int new_streams) {` — insert **before** it:
```c
#ifdef TEST_BUILD
static _Atomic int g_schema_conf_fsync_fail_call;
void schema_conf_test_fsync_fail_call(int n) {
    atomic_store_explicit(&g_schema_conf_fsync_fail_call, n > 0 ? n : 0,
                          memory_order_release);
}
#endif

```
(`<stdatomic.h>` is already pulled in through `types.h`.) The arming is consumed inside hunk 4c.

**4c. `query_find.c` — checked rewrite.** Anchor (exactly once):
```c
    fclose(fout);
    int ok = (rename(tmp, conf) == 0);
    flock(lockfd, LOCK_UN);
    fclose(fin);
    return ok ? 0 : -1;
}
```
Replace with:
```c
    /* Checked publication: flush + close must succeed, then
       fsync(tmp) → rename → fsync(parent) via the existing publisher. Any
       failure leaves the original schema.conf in place (old-or-new). */
    int ok = fflush(fout) == 0 && ferror(fout) == 0;
    if (fclose(fout) != 0) ok = 0;
#ifdef TEST_BUILD
    {
        int fail_call = atomic_exchange_explicit(&g_schema_conf_fsync_fail_call, 0,
                                                 memory_order_acq_rel);
        if (fail_call > 0) {
            durability_test_fsync_reset();
            durability_test_fsync_fail_on_call(fail_call, EIO);
        }
    }
#endif
    if (ok) {
        int prc = durability_publish_replace(conf, tmp, NULL, NULL);
        if (prc < 0) { unlink(tmp); ok = 0; }
        else if (prc > 0) {
            LOG_ERROR(LOG_SUB_CONFIG,
                      "update_schema_conf_splits_streams: schema.conf renamed but parent-dir sync failed: %s",
                      strerror(errno));
            ok = 0;   /* new content is visible but not confirmed durable: report failure */
        }
    } else {
        unlink(tmp);
    }
    flock(lockfd, LOCK_UN);
    fclose(fin);
    return ok ? 0 : -1;
}
```
(`unlink` needs `<unistd.h>`, already used in this file via other calls; if the compiler warns about an implicit declaration add the include.)

**4d. GREEN.** Build zero warnings; `test-schema-conf-rewrite-fsync-failure` GREEN; fresh `run-all`. Revert-proof: temporarily restore **only** the original tail of the function (hunk 4c's replaced block: `fclose(fout); int ok = (rename(tmp, conf) == 0);`) while leaving 4b in place, expect RED (request succeeds, file torn or changed), then re-apply 4c, GREEN. Paste both.

**Known limitation recorded, not fixed here:** `update_schema_conf_splits_streams` still returns success when the object's line is not found (`replaced == 0`); characterize this in the evidence file (a test that calls the function path for an absent object is out of Part A). Part B decides whether to make absence an error once all its callers are inventoried.

## Part A — definition of done

- [ ] Fresh `./build/bin/shard-db-test run-all` passes (paste the tail); zero build warnings.
- [ ] Every RED and revert-proof output pasted in `docs/plans/2026-11-02-r5-evidence.md`.
- [ ] `git diff --check` clean; no leftover `TODO`/debug prints; only the files named in Tasks 0–4 changed (plus the new test files, `build.sh`, `docs/release-notes/2026.11.1.md`, `docs/reference/changelog.md`, the evidence file).
- [ ] Docs: changelog + release notes (full R5 text lands with Part B).
- [ ] Left **uncommitted** for raw-diff review (blind reviewer: diff touches version gating, an on-disk format reader and publication ordering).

# REV 1 — PART B: executable hunks for Tasks 5–12

**Same execution rules as Part A** (stop-and-ask, `PLAN_NOTES.md` halt on any anchor that is not found exactly once, uncommitted, paste every RED/GREEN, no sanitizers/benches). **Part B depends on Part A being applied and green.** Anchors that quote text *created by Part A* are marked "(Part A text)"; if the executor's tree differs from Part A's hunks, halt per the stop rule.

Task order (changed from §5 — the default flip moves last so every earlier task can be tested while legacy fixtures still exist): **5** packed `create-object` (explicit), **6** fail-closed guard, **7** metadata helpers + `RebuildTxn` layout restore, **8** conversion core + startup migration, **9** crash/precondition/refusal tests, **10** backup/restore, **11** default flip (all new objects packed) + full-suite triage, **12** bounds + docs.

## B.0 Additional findings (verified in source at HEAD `b6911a1`)

| # | Finding | Consequence |
|---|---|---|
| F18 | `rebuild_object_v2` (`query_find.c`) is the single choke point for `add-field` (`cmd_add_fields` → `rebuild_object`), `vacuum` compact/splits/streams (`cmd_vacuum` → `rebuild_object`), `edit-field` (`cmd_edit_fields` → `rebuild_object_v2` directly). Its `!slot_changed` branch re-inserts value bytes **verbatim with the record's own `hdr2`** (layout-agnostic: splits/streams-only rebuilds already work for packed objects). Its `slot_changed` branch is **legacy-only** (`typed_record_view`, `transform_field_value`, hard-coded `SREC_FLAG_NULLMAP`). | Task 6 guards the `slot_changed` branch for packed objects. |
| F19 | `RebuildTxn` meta holds only `old_splits/old_streams/indexes_may_change`; `rebuild_txn_abort` (also what startup `rebuild_recovery` runs for an `.rebuild_txn.active` directory) restores `fields.conf` + splits/streams, **not the layout token**. A crash after the token is published but before commit would roll data back to legacy and leave `layout=packed` ⇒ packed loader over legacy bytes. | Task 7 adds `old_layout` to the meta and restores it in abort. |
| F20 | Legacy `numeric:N` (no comma) means precision N, **scale 2**; packed `numeric:N` means scale 0 (`pkr_field_parse`). Legacy `numeric:P,S` P is informational (never enforced). | Conversion rewrites `numeric:N` → `numeric:N,2`; every `numeric:P,S` is kept and now **enforced** — values that do not fit P refuse startup (D5). |
| F21 | `cmd_create_object` writes `fields.conf` and appends the `schema.conf` line with unchecked `fopen/fprintf/fclose` and no fsync; only `server.c` calls it. | Task 5 checks both writes. |
| F22 | `cmd_backup` records no layout (`object.json` = splits, max_key, storage_version, streams); `cmd_restore` blindly copies `data/`, `indexes/`, `metadata/`, `fields.conf` over the live object and appends a schema line only when absent. A legacy backup restored over a packed object (or vice-versa) would mix layouts. | Task 10. |
| F23 | The startup order in both `cmd_server` and `shard_db_open` is: version check → `shard_db_recover_before_stamp` → `shard_db_validate_before_stamp` → stamp. `shard_db_version_check` returns `SHARD_DB_VERSION_STAMP` when the on-disk version is `[2026.10.2, 2026.11.1)` ("first start of this binary") and `SHARD_DB_VERSION_NOOP` when equal. The clean flag is `<db_root>/.shard-db.clean` (`clean_flag_exists/remove`), pending markers are detected by `object_has_pending_markers(data_dir)` (returns 1 pending, 0 none, -1 scan error). | Task 8 uses exactly these. |
| F24 | `decode_field_to_buf` renders JSON (quoted/escaped varchar), not raw text; it is unsuitable for conversion. Legacy numeric bytes are an int64 BE scaled by `10^numeric_scale` with `numeric_scale` in the `TypedField`. | Conversion code reads the int64 directly. |
| F25 | The test daemon is a `TEST_BUILD` binary (`btri_stamp_test_inject`, `schema_conf_test_fsync_fail_call` exist there); `test_env_start_at` returns `-1` when the daemon dies before accepting connections. | Crash-point tests use a TEST-only `raise(SIGKILL)` phase knob; refusal tests assert `start_at == -1`. |

## B.1 Decisions made while writing Part B (flag to the human)

| ID | Decision | Rationale |
|---|---|---|
| D9 — **CONFIRMED by the human (sequencing only: nothing ships before R5b/R6/R7, so no functionality is lost)** | For **packed objects**, `add-field`, `edit-field` and `vacuum --compact` (the three operations that *recompose record bytes* with a changed schema) are **fail-closed refused** in R5 with the error `"<op> is not supported on packed-layout objects yet"`. Splits/streams changes (`vacuum` with `splits`, auto-reshard), `remove-field`, `rename-field`, `add-index`/`remove-index`, `reindex`, `backup`/`restore` are supported and tested. The recompose of those three is a follow-up plan **R5b, which must land before R6**. | Each needs a semantic decision the human has not made (numeric rescale on edit, computed-default backfill — seq/uuid()/random — on packed, bare-vs-bounded edits, re-pointing the legacy-grammar validators `validate_field_type`/`typed_legacy_decl_size` at the packed grammar). Writing them blind would be guessing. Because every migrated object is packed, **until R5b the only way to add/edit a field is to recreate the object**. If you want them in R5 say so and I will write Part C (tasks numbered 13+) before execution. |
| D10 | `create-object` `declaration_layout` states the **grammar** of the request's `fields` strings. `"packed"` (and, after Task 11, absent) ⇒ packed grammar (`numeric:12` means scale 0; `varchar` bare allowed). `"legacy"` is accepted **only in `TEST_BUILD`** (fixtures for the migration tests need legacy objects); a production build rejects it with `"legacy layout objects can no longer be created"`. Anything else is rejected. | There is no other way to create the legacy fixtures the migration tests need; the seam is compiled out of production. |
| D11 | Migration diagnostics: on an audit refusal the daemon writes a bounded report `<db_root>/.layout-migration-report` (bounded to a 2048-byte buffer — at most 8 diagnostic lines per object; atomic tmp+rename) and prints the same lines to stderr; the file is removed when a later start succeeds. | Gives the operator the field/key list and gives the test something to assert on (`log_init` has not run yet at that point in `cmd_server`). |

## Task 5 — Packed `create-object` (explicit `declaration_layout:"packed"`), checked creation writes (D10; F21)

**Root cause being fixed:** `server.c` rejects every `declaration_layout` other than `"legacy"` (F5), `cmd_create_object` validates only the legacy grammar (`typed_legacy_decl_size` rejects bare `varchar`, mis-sizes packed `numeric`), has no way to write the `layout=packed` token, and its `fields.conf`/`schema.conf` writes are unchecked (F21).

**5a. Tests first** (append to `test_layout_manifest.c`, register each):

```c
static TestClient *lm_start_client(LmRoot *m, TestEnv *env) {
    if (test_env_start_at(env, m->db_root, m->port) != 0) return NULL;
    return lm_connect(m->port);
}

static void lm_add_dir(TestClient *tc) {
    char *r = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"lay\"}", &r);
    free(r);
}

/* explicit packed create: bare varchar + exact numeric + int; round trip */
static int test_create_object_packed_explicit(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start");
    if (!tc) { lm_destroy(&m); return 1; }
    lm_add_dir(tc);
    char *r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"p\","
        "\"splits\":8,\"max_key\":16,\"declaration_layout\":\"packed\","
        "\"fields\":[\"name:varchar\",\"amt:numeric:12,3\",\"qty:int\"]}", &r),
        0, "create");
    ASSERT_TRUE(r && strstr(r, "\"status\":\"created\""), "created packed");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\","
        "\"value\":{\"name\":\"alice\",\"amt\":\"12.345\",\"qty\":7}}", &r),
        0, "insert");
    ASSERT_TRUE(r && strstr(r, "\"status\":\"inserted\""), "inserted");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r),
        0, "get");
    ASSERT_TRUE(r && strstr(r, "\"name\":\"alice\"") && strstr(r, "12.345") &&
                strstr(r, "\"qty\":7"), "round trip");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"p\"}", &r),
        0, "describe");
    ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"packed\""), "describe packed");
    free(r);
    tc_close(tc);
    test_env_stop_keep(&env);
    char path[400];
    snprintf(path, sizeof(path), "%s/schema.conf", m.db_root);
    char *conf = tu_read_file(path);
    ASSERT_TRUE(conf && strstr(conf, "lay:p:8:16:2:") && strstr(conf, ":layout=packed\n"),
                "schema.conf carries the layout token");
    free(conf);
    /* restart: the token is authoritative, object still packed */
    tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "restart");
    if (tc) {
        r = NULL;
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r);
        ASSERT_TRUE(r && strstr(r, "12.345"), "persisted after restart");
        free(r);
        tc_close(tc);
        test_env_stop_keep(&env);
    }
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* unknown layout strings and invalid packed declarations are refused with no residue */
static int test_create_object_packed_refusals(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start");
    if (!tc) { lm_destroy(&m); return 1; }
    lm_add_dir(tc);
    static const char *bad[] = {
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"b1\",\"declaration_layout\":\"wide\",\"fields\":[\"a:int\"]}",
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"b2\",\"declaration_layout\":\"packed\",\"fields\":[\"a:numeric:0\"]}",
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"b3\",\"declaration_layout\":\"packed\",\"fields\":[\"a:varchar:0\"]}",
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"b4\",\"declaration_layout\":\"packed\",\"fields\":[\"a:varchar:4194305\"]}",
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"b5\",\"declaration_layout\":\"packed\",\"fields\":[\"a:numeric:5,9\"]}",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc, bad[i], &r), 0, "request");
        ASSERT_TRUE(r && strstr(r, "\"error\""), "refused");
        free(r);
        char probe[400];
        snprintf(probe, sizeof(probe), "%s/lay/b%zu/fields.conf", m.db_root, i + 1);
        ASSERT_TRUE(!tu_file_exists(probe), "no fields.conf residue");
    }
    tc_close(tc);
    test_env_stop_keep(&env);
    char path[400];
    snprintf(path, sizeof(path), "%s/schema.conf", m.db_root);
    char *conf = tu_read_file(path);
    ASSERT_TRUE(!conf || !strstr(conf, "lay:b"), "no schema.conf residue");
    free(conf);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-create-object-packed-explicit", test_create_object_packed_explicit)
TEST_REGISTER("test-create-object-packed-refusals", test_create_object_packed_refusals)
```
RED on base: `test-create-object-packed-explicit` fails at "created packed" (server rejects `packed`); `test-create-object-packed-refusals` fails (`wide`/`packed` produce the legacy-import message for b1 only; b2–b5 are never reached/validated, and the first request already errors, but `numeric:5,9` etc. are not distinguishable — the case must fail on base at the final `ASSERT_TRUE(!conf …)` only if residue exists; if it unexpectedly passes on base, that is acceptable because refusal is already today's behaviour — record this in the evidence file, do **not** weaken it). Paste RED.

**5b. `server.c` — accept the layout, pass it down.** Anchor (exactly once):

```c
        const char *layout_value=NULL;size_t layout_len=0;
        if(json_obj_get(&req,"declaration_layout",&layout_value,&layout_len)&&
           (layout_len!=8||memcmp(layout_value,"\"legacy\"",8))){
            OUT("{\"error\":\"packed or unknown schema layout import is inactive until migration integration\"}\n");
            free(mode);free(dir);free(object);return;
        }
```
Replace with:

```c
        /* declaration_layout names the GRAMMAR of the request's fields strings
           (D10). "packed" (and, once the default flips, absent) creates a
           packed object. "legacy" exists only in TEST_BUILD so the migration
           tests can build legacy fixtures. */
        const char *layout_value=NULL;size_t layout_len=0;
        int create_packed=0;   /* Task 11 flips this initial value to 1 */
        if(json_obj_get(&req,"declaration_layout",&layout_value,&layout_len)){
            if(layout_len==8&&!memcmp(layout_value,"\"packed\"",8)){
                create_packed=1;
            }else if(layout_len==8&&!memcmp(layout_value,"\"legacy\"",8)){
#ifdef TEST_BUILD
                create_packed=0;
#else
                OUT("{\"error\":\"legacy layout objects can no longer be created\"}\n");
                free(mode);free(dir);free(object);return;
#endif
            }else{
                OUT("{\"error\":\"unknown declaration_layout; expected \\\"packed\\\"\"}\n");
                free(mode);free(dir);free(object);return;
            }
        }
```
(The initial value is `0` in this hunk on purpose: until Task 11 an absent `declaration_layout` keeps creating legacy objects so every existing test stays valid.)

Then the call: anchor (exactly once):

```c
        cmd_create_object(g_db_root, dir, object,
                          fields_j, indexes_j,
                          splits_s ? atoi(splits_s) : 0,
                          max_key_s ? atoi(max_key_s) : 0,
                          if_not_exists,
                          auto_key_s);
```
Replace with:

```c
        cmd_create_object(g_db_root, dir, object,
                          fields_j, indexes_j,
                          splits_s ? atoi(splits_s) : 0,
                          max_key_s ? atoi(max_key_s) : 0,
                          if_not_exists,
                          auto_key_s, create_packed);
```

**5c. `types.h` — signature.** Anchor (exactly once):
```c
int cmd_create_object(const char *db_root, const char *dir, const char *object,
                      const char *fields_json, const char *indexes_json,
                      int splits, int max_key, int if_not_exists,
                      const char *auto_key_spec);
```
Replace with:
```c
int cmd_create_object(const char *db_root, const char *dir, const char *object,
                      const char *fields_json, const char *indexes_json,
                      int splits, int max_key, int if_not_exists,
                      const char *auto_key_spec, int packed);
```
`grep -rn "cmd_create_object(" src --include='*.c' --include='*.h'` must show exactly: `types.h`, `server.c`, `query_schema.c`. If anything else appears, halt.

**5d. `query_schema.c` — packed validator.** Insert **before** the comment `/* Validate + parse the auto_key spec. Returns 0 on AK_NONE, 1 on` (anchor exactly once):

```c
/* Packed-grammar counterpart of validate_field_type: returns > 0 on a valid
   declaration, 0 otherwise. Bare `varchar` / `numeric` are legal; numeric
   precision/scale and varchar capacity are validated by the packed parser;
   every other type keeps the existing fixed-width grammar. */
static int validate_packed_field_type(const char *field_spec) {
    const char *colon = strchr(field_spec, ':');
    if (!colon || colon == field_spec) return 0;
    char *buf = strdup(colon + 1);
    if (!buf) return 0;
    TypedField modifiers = {0};
    PkrField pf;
    int is_pkr = 0, rc = 0;
    if (parse_default_modifier_checked(buf, &modifiers) >= 0) {
        /* strip :removed / :not_null / :nullable exactly as the legacy
           validator does, then look at the bare type token */
        size_t blen = strlen(buf);
        if (blen >= 8 && strcmp(buf + blen - 8, ":removed") == 0) buf[blen - 8] = '\0';
        blen = strlen(buf);
        if (blen >= 9 && (strcmp(buf + blen - 9, ":not_null") == 0 ||
                          strcmp(buf + blen - 9, ":nullable") == 0)) buf[blen - 9] = '\0';
        if (!strstr(buf, ":not_null") && !strstr(buf, ":nullable")) {
            if (!strncmp(buf, "varchar", 7) || !strncmp(buf, "numeric", 7) ||
                !strcmp(buf, "currency")) {
                is_pkr = 1;
                rc = pkr_field_parse(buf, &pf) == DEC_OK ? 1 : 0;
            } else {
                rc = typed_legacy_decl_size(buf);   /* fixed-width types: identical grammar */
            }
        }
    }
    if (rc > 0 && is_pkr) {
        /* :default= literal must fit the declaration (legacy validated it via
           typed_default_validate; that cannot parse bare/packed types). */
        if (modifiers.default_kind == DK_LITERAL) {
            size_t dl = strlen(modifiers.default_val);
            if (pf.kind == PKR_VARCHAR) {
                if (dl > pf.payload_max) rc = 0;
            } else {
                uint8_t payload[DEC_PAYLOAD_MAX];
                size_t used = 0;
                char perr[64];
                if (pkr_numeric_encode(&pf, modifiers.default_val, dl, payload,
                                       sizeof(payload), &used, perr, sizeof(perr)) != DEC_OK)
                    rc = 0;
            }
        }
    } else if (rc > 0) {
        char *again = strdup(colon + 1);
        TypedField dm = {0};
        if (again && parse_default_modifier_checked(again, &dm) >= 0) {
            parse_field_type(again, &dm);
            if (dm.type != FT_ENUM && typed_default_validate(&dm)) rc = 0;
            free_enum_values(&dm);
        }
        free(again);
    }
    free_enum_values(&modifiers);
    free(buf);
    return rc;
}

```
(`pkr_field_parse` needs `#include "packed_record.h"` in `query_schema.c`; add it next to the other includes if absent — check with `grep -n 'packed_record.h' src/db/query_schema.c`.)

**5e. `query_schema.c` — signature + validation branch + slot size + checked writes.** Five edits, each anchor exactly once.

(i) Anchor: `                      int splits, int max_key, int if_not_exists,\n                      const char *auto_key_spec) {` → replace with `                      int splits, int max_key, int if_not_exists,\n                      const char *auto_key_spec, int packed) {`.

(ii) Anchor: `        int field_size = validate_field_type(field_specs[nfields]);` → replace with `        int field_size = packed ? validate_packed_field_type(field_specs[nfields])\n                                : validate_field_type(field_specs[nfields]);`.

(iii) Anchor: `        if(field_size>INT_MAX-total_value_size){OUT("{\"error\":\"schema size overflow\"}\n");return 1;}\n        total_value_size+=field_size;` → replace with:
```c
        if (packed) continue;   /* packed sizes come from the bound codec below */
        if(field_size>INT_MAX-total_value_size){OUT("{\"error\":\"schema size overflow\"}\n");return 1;}
        total_value_size+=field_size;
```
(the `continue` is the last statement of the loop body, so it only skips the two legacy lines).

(iv) Anchor: `    if (nfields == 0) {\n        OUT("{\"error\":\"fields array is empty — at least one typed field required\"}\n");\n        return 1;\n    }` → append after it:
```c
    /* Packed: bind the codec once from the declared field lines so slot_size
       and value_size come from pkr (single source of truth, same builder the
       loader and the migration use). The cleanup attribute releases the enum
       value lists on every return path (no-op on a zeroed schema). */
    TypedSchema packed_ts __attribute__((cleanup(typed_schema_release_p)));
    memset(&packed_ts, 0, sizeof(packed_ts));
    size_t packed_slot = 0;
    if (packed) {
        char perr[160] = {0};
        if (typed_schema_build_packed((const char *const *)field_specs,
                                      (size_t)nfields, &packed_ts,
                                      perr, sizeof(perr)) != 0) {
            OUT("{\"error\":\"invalid packed field declarations: %s\"}\n", perr);
            return 1;
        }
        total_value_size = packed_ts.total_size;
    }
```
and add, just before `int cmd_create_object(` (anchor: the line `int cmd_create_object(const char *db_root, const char *dir, const char *object,` — once), the helper:
```c
static void typed_schema_release_p(TypedSchema *ts) { typed_schema_release(ts); }

```
Then after the `max_key` validation block (anchor exactly once: `    if (max_key > MAX_KEY_CEILING) {` … its closing `}`), insert:
```c
    if (packed) {
        if (pkr_slot_bound(&packed_ts.packed, (size_t)max_key, &packed_slot) != DEC_OK ||
            packed_slot > INT_MAX) {
            OUT("{\"error\":\"packed schema exceeds the maximum slot size\"}\n");
            return 1;
        }
    }
```

(v) **Checked fields.conf write.** Anchor (exactly once):
```c
    FILE *f = fopen(path, "w");
    if (!f) {
        OUT("{\"error\":\"cannot write fields.conf\"}\n");
        return 1;
    }
    for (int i = 0; i < nfields; i++)
        fprintf(f, "%s\n", field_specs[i]);
    fclose(f);
```
Replace with:
```c
    FILE *f = fopen(path, "w");
    if (!f) {
        OUT("{\"error\":\"cannot write fields.conf\"}\n");
        return 1;
    }
    int fields_ok = 1;
    for (int i = 0; i < nfields; i++)
        if (fprintf(f, "%s\n", field_specs[i]) < 0) fields_ok = 0;
    if (fflush(f) != 0 || fsync(fileno(f)) != 0) fields_ok = 0;
    if (fclose(f) != 0) fields_ok = 0;
    if (!fields_ok) {
        unlink(path);
        OUT("{\"error\":\"cannot write fields.conf\"}\n");
        return 1;
    }
```

(vi) **Checked schema.conf append with token.** Anchor (exactly once; this is the complete existing block — it must be matched byte for byte and replaced whole):
```c
    if (!exists) {
        f = fopen(schema_path, "a");
        if (f) {
            /* Schema line: dir:object:splits:max_key:2:streams[:auto_key=...].
               The literal `2` is the storage-version slot — kept in the on-disk
               format for forward compatibility with future engine versions,
               even though every object the daemon writes is v2. */
            if (auto_key_kind == 1) {
                fprintf(f, "%s:%s:%d:%d:2:%d:auto_key=uuid\n",
                        dir, object, splits, max_key, streams);
            } else if (auto_key_kind == 2) {
                fprintf(f, "%s:%s:%d:%d:2:%d:auto_key=seq(%s)\n",
                        dir, object, splits, max_key, streams, auto_seq_name);
            } else {
                fprintf(f, "%s:%s:%d:%d:2:%d\n",
                        dir, object, splits, max_key, streams);
            }
            fclose(f);
        }
    }
```
Replace the **entire block** with:

```c
    if (!exists) {
        /* Schema line: dir:object:splits:max_key:2:streams[:auto_key=...][:layout=packed].
           The literal `2` is the storage-version slot (kept for forward
           compatibility). The write is checked and synced: a failure here
           removes the fields.conf written above so a retry starts clean. */
        char extra[192];
        extra[0] = '\0';
        if (auto_key_kind == 1)
            snprintf(extra, sizeof(extra), ":auto_key=uuid");
        else if (auto_key_kind == 2)
            snprintf(extra, sizeof(extra), ":auto_key=seq(%s)", auto_seq_name);
        if (packed) {
            size_t el = strlen(extra);
            snprintf(extra + el, sizeof(extra) - el, ":layout=packed");
        }
        f = fopen(schema_path, "a");
        int line_ok = f != NULL;
        if (f) {
            if (fprintf(f, "%s:%s:%d:%d:2:%d%s\n",
                        dir, object, splits, max_key, streams, extra) < 0) line_ok = 0;
            if (fflush(f) != 0 || fsync(fileno(f)) != 0) line_ok = 0;
            if (fclose(f) != 0) line_ok = 0;
        }
        if (!line_ok) {
            snprintf(path, sizeof(path), "%s/%s/fields.conf", eff_root, object);
            unlink(path);
            OUT("{\"error\":\"cannot write schema.conf\"}\n");
            return 1;
        }
    }
```
(`f` is reused later at `f = fopen(dirs_path, "r")`; keep that.)

(vii) **Slot size.** Anchor (exactly once): `        int slot_size = (24 + max_key + total_value_size + 7) & ~7;\n        if (slot_size < 32) slot_size = 32;` → replace with:
```c
        int slot_size = (24 + max_key + total_value_size + 7) & ~7;
        if (slot_size < 32) slot_size = 32;
        if (packed) slot_size = (int)packed_slot;
```

(viii) Response `value_size` already prints `total_value_size` (set from `packed_ts.total_size` for packed).

**5f. `config.c` — the three helpers** `typed_schema_build_packed`, `typed_schema_release` and `layout_packed_decl_line`: **defined in Task 7** (they are needed by Task 5's code, so Task 5 will not compile until Task 7a/7b land). **Therefore execute Task 7a and 7b (config.c + types.h only) immediately after Task 5a–5b and before building.** The executor builds only after 7a/7b are in; the RED for Task 5 is recorded from a build that has 7a/7b but not 5b–5e (i.e. apply 5a, 7a, 7b, build, run RED; then 5b–5e, GREEN).

**5g. GREEN.** Both cases pass; `grep` for `cmd_create_object(` as in 5c; fresh `run-all` (expect all green — the default is still legacy until Task 11). Revert-proof: temporarily restore the old `if(json_obj_get(&req,"declaration_layout"…` rejection block and expect `test-create-object-packed-explicit` RED at `created packed`; re-apply; GREEN.

## Task 6 — Fail-closed guard for recomposing operations on packed objects (D6, D9; F18)

**Root cause:** the `slot_changed` branch of `v2_rebuild_walk_cb` (and `cmd_add_fields`/`cmd_edit_fields` validation) interprets record bytes with the legacy layout. On a packed object the rebuild would read `hdr2 = 3` records through `typed_record_view` (which rejects flag `0x02`) and fail with "malformed record view" **after** staging data and emptying `data/` — recoverable (rollback) but wasteful and, for `add-field`, it validates the new line with the legacy grammar (`numeric:12` would be accepted as scale 2 and then loaded as scale 0). The guard refuses up front.

**6a. Tests first** (append, register):

```c
static int lm_expect_error(TestClient *tc, const char *req, const char *needle) {
    char *r = NULL;
    int ok = tc_request(tc, req, &r) == 0 && r && strstr(r, "\"error\"") &&
             (!needle || strstr(r, needle));
    free(r);
    return ok;
}

static int test_packed_recompose_ops_refused(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start");
    if (!tc) { lm_destroy(&m); return 1; }
    lm_add_dir(tc);
    char *r = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"p\",\"splits\":8,"
        "\"max_key\":16,\"declaration_layout\":\"packed\","
        "\"fields\":[\"name:varchar:20\",\"qty:int\"]}", &r);
    free(r);
    ASSERT_TRUE(lm_expect_error(tc,
        "{\"mode\":\"add-field\",\"dir\":\"lay\",\"object\":\"p\",\"fields\":[\"extra:int\"]}",
        "not supported on packed"), "add-field refused");
    ASSERT_TRUE(lm_expect_error(tc,
        "{\"mode\":\"edit-field\",\"dir\":\"lay\",\"object\":\"p\",\"fields\":[\"name:varchar:30\"]}",
        "not supported on packed"), "edit-field refused");
    ASSERT_TRUE(lm_expect_error(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"lay\",\"object\":\"p\",\"compact\":true}",
        "not supported on packed"), "compact refused");
    /* supported: splits change on a packed object copies verbatim */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\","
        "\"value\":{\"name\":\"a\",\"qty\":1}}", &r);
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"lay\",\"object\":\"p\",\"splits\":16}", &r),
        0, "vacuum splits");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "splits change allowed");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r), 0, "get");
    ASSERT_TRUE(r && strstr(r, "\"name\":\"a\"") && strstr(r, "\"qty\":1"),
                "record intact after verbatim reshard");
    free(r);
    tc_close(tc);
    test_env_stop_keep(&env);
    char path[400];
    snprintf(path, sizeof(path), "%s/schema.conf", m.db_root);
    char *conf = tu_read_file(path);
    ASSERT_TRUE(conf && strstr(conf, "lay:p:16:") && strstr(conf, ":layout=packed"),
                "reshard preserved the layout token");
    free(conf);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-packed-recompose-ops-refused", test_packed_recompose_ops_refused)
```
RED on base+5: add-field/edit-field/compact are **not** refused (they error later with other text, e.g. "Failed to begin…/malformed record view…", so the `needle` check fails) — paste.

**6b. `query_find.c` — guard in `rebuild_object_v2`.** Anchor (exactly once):
```c
    RebuildTxn *txn = rebuild_txn_begin(
        db_root, object, old_sch->splits, old_sch->streams,
        splits_changed || (finalize && finalize->indexes_may_change));
```
Insert **before** it:
```c
    /* D9: record-recomposing operations are not implemented for the packed
       layout in R5. Verbatim copies (splits/streams only) are layout-
       agnostic and stay allowed. Refuse before touching anything. */
    if (old_ts->packed.nfields && (slot_changed || drop_tombstoned || n_added > 0)) {
        OUT("{\"error\":\"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet\"}\n");
        return 1;
    }
```
**6b-2. `query_find.c` — `rebuild_object` must not rebuild a packed schema with legacy arithmetic.** Root cause: `rebuild_object` builds `new_ts` by copying `old_ts->fields[i]` and re-summing `->size` (for a packed field `size` is the 8-byte descriptor width, not the legacy size) and then derives `new_sch.slot_size` from that sum; for a packed object the slot size therefore changes, `slot_changed` becomes 1 and a harmless reshard would be refused (or, without the 6b guard, mis-rebuilt). Anchor (exactly once):
```c
    /* Build new TypedSchema:
         1. Copy existing fields (skip tombstoned if drop_tombstoned).
         2. Append any added fields at the end. */
```
Insert **before** it:
```c
    /* D9: a packed object supports only verbatim splits/streams rebuilds.
       Reuse the bound schema as-is; the legacy-shaped construction below
       would re-sum descriptor widths and mis-size the slot. */
    if (old_ts->packed.nfields) {
        if (drop_tombstoned || n_added > 0) {
            OUT("{\"error\":\"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet\"}\n");
            return 1;
        }
        if (!splits_changed && !streams_changed) {
            OUT("{\"status\":\"noop\",\"reason\":\"no change requested\"}\n");
            return 0;
        }
        TypedSchema same = *old_ts;
        int identity[MAX_FIELDS];
        for (int i = 0; i < old_ts->nfields; i++) identity[i] = i;
        Schema same_sch = old_sch;
        same_sch.splits = new_splits;
        same_sch.streams = new_streams;
        return rebuild_object_v2(db_root, object, &old_sch, old_ts,
                                 &same_sch, &same, identity,
                                 0, splits_changed, 0, added_lines, 0, NULL);
    }

```
(`splits_changed`, `streams_changed`, `new_splits`, `new_streams` are all defined above this anchor in `rebuild_object`.)

**6c. `config.c` — `cmd_add_fields`.** Anchor (exactly once):
```c
        OUT("{\"error\":\"fields.conf missing for [%s]\"}\n", object);
        return 1;
    }

    for (int a = 0; a < nlines; a++) {
        char *colon = strchr(lines[a], ':');
```
Replace with the same text plus the guard between the `}` and the `for`:
```c
        OUT("{\"error\":\"fields.conf missing for [%s]\"}\n", object);
        return 1;
    }
    if (ts->packed.nfields) {
        OUT("{\"error\":\"add-field is not supported on packed-layout objects yet\"}\n");
        return 1;
    }

    for (int a = 0; a < nlines; a++) {
        char *colon = strchr(lines[a], ':');
```
**6d. `query_schema.c` — `cmd_edit_fields`.** Anchor (exactly once):
```c
        OUT("{\"error\":\"fields.conf missing for [%s]\"}\n", object);
        return 1;
    }

    /* Parse each edit line, validate name/type, look up target field. */
```
Replace with the same text plus, after the `}` and before the comment:
```c
    if (old_ts->packed.nfields) {
        OUT("{\"error\":\"edit-field is not supported on packed-layout objects yet\"}\n");
        return 1;
    }

```
Error texts: test needle is `not supported on packed` — all three messages contain it (`…are not supported on packed-layout…`, `add-field is not supported on packed-layout…`, `edit-field is not supported on packed-layout…`) ✓.

**6e. GREEN.** `test-packed-recompose-ops-refused` GREEN; fresh `run-all`. Revert-proofs (paste both): (1) remove hunk 6b-2 only → expect RED at `splits change allowed` (the legacy arithmetic re-sums descriptor widths, `slot_changed` becomes 1 and 6b refuses a harmless reshard); restore. (2) remove hunk 6c only → expect RED at `add-field refused`; restore. Hunk 6b (the `rebuild_object_v2` guard) is defense-in-depth for the direct `cmd_edit_fields → rebuild_object_v2` call, which 6d already refuses earlier, so no test fails without it — say so in the evidence file instead of inventing a test.

**Edge cases pinned by 6a:** reshard keeps the `layout=packed` token (F12: trailing tokens re-emitted); `remove-field`/`rename-field` are *not* guarded because they only edit `fields.conf` text and the object reloads through the strict packed loader (covered by Task 12 test `test-packed-metadata-ops`).

## Task 7 — Metadata helpers: packed schema builder, declaration rewrite, layout-token rewriter, `RebuildTxn` layout restore (invariants 3, 4; F19, F20)

**Root cause (F19):** `rebuild_txn_abort` — and therefore startup `rebuild_recovery`, which calls it for any `.rebuild_txn.active` directory — restores `fields.conf` and `splits`/`streams` but not the `layout=packed` token. If the migration publishes the token and the process dies before commit, recovery restores the legacy `data/` and legacy `fields.conf` while the token still says packed: the strict packed loader would then read legacy bytes. The meta file must carry the original layout and abort must restore it. **Tasks 7a–7b must be applied together with Task 5 (see 5f); 7c–7d are tested through Task 9's crash-point cases.**

**7a. `config.c` — three functions.** Anchor (exactly once, unchanged by Part A; it is the line directly below `load_typed_schema`'s closing brace):

```c
/* Find field index by name. Tombstoned fields are invisible to lookups. */
int typed_field_index(const TypedSchema *ts, const char *name) {
```
Insert **before** it (this uses Part A's `packed_declared_type` and `ptypes_free`, which are `static` in this same file above `load_typed_schema`):

```c
void typed_schema_release(TypedSchema *ts) {
    if (!ts) return;
    for (int i = 0; i < ts->nfields; i++) free_enum_values(&ts->fields[i]);
    memset(ts, 0, sizeof(*ts));
}

/* Rewrites one fields.conf line from the legacy grammar to the packed
   grammar. The ONLY change: a legacy `numeric:N` (no comma — historical
   scale 2, F20) becomes `numeric:N,2`; every other line is copied verbatim.
   0 ok, -1 if the result does not fit in cap. */
int layout_packed_decl_line(const char *line, char *out, size_t cap) {
    size_t n = strlen(line);
    const char *colon = strchr(line, ':');
    if (colon && strncmp(colon + 1, "numeric:", 8) == 0) {
        const char *digits = colon + 9;
        const char *p = digits;
        while (*p >= '0' && *p <= '9') p++;
        if (p > digits && (*p == '\0' || *p == ':')) {
            size_t head = (size_t)(p - line), tail = strlen(p);
            if (head + 2 + tail >= cap) return -1;
            memcpy(out, line, head);
            memcpy(out + head, ",2", 2);
            memcpy(out + head + 2, p, tail + 1);
            return 0;
        }
    }
    if (n >= cap) return -1;
    memcpy(out, line, n + 1);
    return 0;
}

/* Builds an UNCACHED packed TypedSchema from fields.conf lines (declaration
   order; blank and '#' lines are skipped). This mirrors load_typed_schema's
   packed parse exactly — same modifiers, same `:removed` handling, same
   typed_schema_bind_packed call — so the migration converts to precisely
   what the loader will later read (the migration re-verifies this after
   publication). 0 ok; -1 with err filled. On success the caller owns the
   enum value lists: release with typed_schema_release(). */
int typed_schema_build_packed(const char *const *lines, size_t nlines,
                              TypedSchema *out, char *err, size_t errsz) {
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    TypedSchema ts;
    memset(&ts, 0, sizeof(ts));
    char *ptypes[MAX_FIELDS] = {0};
    int offset = 0;
#define BP_FAIL(...) do { if (err && errsz) snprintf(err, errsz, __VA_ARGS__); goto fail; } while (0)
    for (size_t li = 0; li < nlines; li++) {
        const char *src = lines[li];
        if (!src || !src[0] || src[0] == '#') continue;
        if (ts.nfields >= MAX_FIELDS) BP_FAIL("too many fields (max %d)", MAX_FIELDS);
        char *line = strdup(src);
        if (!line) BP_FAIL("out of memory");
        TypedField *tf = &ts.fields[ts.nfields];
        char *colon = strchr(line, ':');
        if (!colon || colon == line || (size_t)(colon - line) >= sizeof(tf->name)) {
            free(line);
            BP_FAIL("invalid field line");
        }
        size_t name_len = (size_t)(colon - line);
        memcpy(tf->name, line, name_len);
        tf->name[name_len] = '\0';
        tf->name_len = (int)name_len;
        tf->removed = 0;
        tf->default_kind = DK_NONE;
        tf->default_val[0] = '\0';
        char *type_spec = colon + 1;
        size_t ts_len = strlen(type_spec);
        if (ts_len >= 8 && strcmp(type_spec + ts_len - 8, ":removed") == 0) {
            type_spec[ts_len - 8] = '\0';
            tf->removed = 1;
        }
        char *default_check = strdup(type_spec);
        if (!default_check) { free(line); BP_FAIL("out of memory"); }
        TypedField checked_default = {0};
        tf->default_invalid = parse_default_modifier_checked(default_check, &checked_default) < 0;
        free(default_check);
        if (parse_default_modifier(type_spec, tf) < 0) {
            free_enum_values(tf);
            free(line);
            BP_FAIL("invalid nullability structure in field '%s'", tf->name);
        }
        packed_declared_type(type_spec, tf);
        if (tf->type == FT_NONE || tf->size <= 0) {
            free_enum_values(tf);
            free(line);
            BP_FAIL("unparseable type for field '%s'", tf->name);
        }
        ptypes[ts.nfields] = strdup(type_spec);
        free(line);
        if (!ptypes[ts.nfields]) { free_enum_values(tf); BP_FAIL("out of memory"); }
        tf->offset = offset;
        offset += tf->size;
        ts.nfields++;
        ts.typed = 1;
    }
    if (ts.nfields < 1) BP_FAIL("no fields");
    ts.total_size = offset;
    ts.nullmap_prefix = 1 + typed_nullmap_bytes(ts.nfields);
    ts.total_size += ts.nullmap_prefix;
    if (typed_schema_bind_packed(&ts, (const char *const *)ptypes,
                                 (size_t)ts.nfields, err, errsz) != 0)
        goto fail;
    ptypes_free(ptypes, ts.nfields);
    *out = ts;
    return 0;
fail:
    for (int i = 0; i < ts.nfields; i++) free_enum_values(&ts.fields[i]);
    ptypes_free(ptypes, ts.nfields);
    return -1;
#undef BP_FAIL
}

```
(`typed_schema_bind_packed` is declared in `typed_row.h`; `config.c` must already include it after Part A Task 2 — `grep -n 'typed_row.h' src/db/config.c`; add `#include "typed_row.h"` next to the other local includes if absent. `parse_default_modifier_checked` and `parse_default_modifier` are defined earlier in this file. If `packed_declared_type`/`ptypes_free` are not found, Part A Task 2 was not applied: halt.)

**7b. `types.h` — declarations.** Anchor (exactly once, **Part A text**): `int schema_conf_layout(const char *effective_root, const char *object, int *out);` Replace with:

```c
int schema_conf_layout(const char *effective_root, const char *object, int *out);
/* Sets (packed=1) or clears (packed=0) the `layout=packed` token on the
   object's schema.conf line, preserving every other token; checked
   publication; -1 when the line is absent or anything fails. */
int update_schema_conf_layout(const char *effective_root, const char *object, int packed);
/* fields.conf legacy→packed declaration rewrite and the uncached packed
   builder used by create-object and the startup migration. */
int layout_packed_decl_line(const char *line, char *out, size_t cap);
int typed_schema_build_packed(const char *const *lines, size_t nlines,
                              TypedSchema *out, char *err, size_t errsz);
void typed_schema_release(TypedSchema *ts);
/* Startup migration of ONE legacy object to the packed layout. dry_run=1:
   read-only audit (nothing is written). Returns 0 ok, 1 when records do not
   fit their packed declaration (diag lists up to 8, one per line), -1 on
   any other failure (diag has the reason). Caller holds objlock_wrlock. */
int layout_migrate_object(const char *effective_root, const char *object,
                          int dry_run, char *diag, size_t diagsz);
/* No-op outside TEST_BUILD; in TEST_BUILD raises SIGKILL when the db.env
   knob LAYOUT_MIGRATE_TEST_CRASH names this phase. */
void layout_test_crash_point(const char *phase);
```

**7c. `query_find.c` — `update_schema_conf_layout`.** Anchor (exactly once):

```c
/* Defined in config.c — see header doc-comment there. Declared locally
   because config.c's helpers live across the layering boundary and the
```
Insert **before** it:

```c
int update_schema_conf_layout(const char *effective_root, const char *object, int packed) {
    size_t rootlen = strlen(g_db_root);
    if (strncmp(effective_root, g_db_root, rootlen) != 0) return -1;   /* same guard as schema_conf_layout */
    const char *dir = effective_root + rootlen;
    if (*dir == '/') dir++;
    char conf[PATH_MAX], tmp[PATH_MAX], prefix[512];
    snprintf(conf, sizeof(conf), "%s/schema.conf", g_db_root);
    snprintf(tmp, sizeof(tmp), "%s/schema.conf.layout.tmp.%d", g_db_root, (int)getpid());
    int pfxlen = snprintf(prefix, sizeof(prefix), "%s:%s:", dir, object);
    if (pfxlen < 0 || (size_t)pfxlen >= sizeof(prefix)) return -1;

    FILE *fin = fopen(conf, "r");
    if (!fin) return -1;
    int lockfd = fileno(fin);
    flock(lockfd, LOCK_EX);
    FILE *fout = fopen(tmp, "w");
    if (!fout) { flock(lockfd, LOCK_UN); fclose(fin); return -1; }

    char line[4096];
    int replaced = 0, ok = 1;
    while (ok && fgets(line, sizeof(line), fin)) {
        size_t ll = strlen(line);
        if (ll == sizeof(line) - 1 && line[ll - 1] != '\n') { ok = 0; break; } /* never split a line */
        if (!replaced && strncmp(line, prefix, (size_t)pfxlen) == 0) {
            line[strcspn(line, "\r\n")] = '\0';
            char rebuilt[4096];
            size_t o = 0;
            int first = 1;
            char *p = line;
            while (p) {
                char *next = strchr(p, ':');
                size_t tl = next ? (size_t)(next - p) : strlen(p);
                int is_layout = tl >= 6 && strncmp(p, "layout", 6) == 0 &&
                                (tl == 6 || p[6] == '=');
                if (!is_layout) {
                    if (o + tl + 2 >= sizeof(rebuilt)) { ok = 0; break; }
                    if (!first) rebuilt[o++] = ':';
                    memcpy(rebuilt + o, p, tl);
                    o += tl;
                    first = 0;
                }
                p = next ? next + 1 : NULL;
            }
            if (ok && packed) {
                if (o + 16 >= sizeof(rebuilt)) ok = 0;
                else { memcpy(rebuilt + o, ":layout=packed", 14); o += 14; }
            }
            if (ok) {
                rebuilt[o] = '\0';
                if (fprintf(fout, "%s\n", rebuilt) < 0) ok = 0;
                replaced = 1;
            }
        } else if (fputs(line, fout) < 0) {
            ok = 0;
        }
    }
    if (ferror(fin) || !replaced) ok = 0;
    if (fflush(fout) != 0 || ferror(fout)) ok = 0;
    if (fclose(fout) != 0) ok = 0;
    if (ok) {
        int prc = durability_publish_replace(conf, tmp, NULL, NULL);
        if (prc < 0) { unlink(tmp); ok = 0; }
        else if (prc > 0) {
            LOG_ERROR(LOG_SUB_CONFIG,
                      "update_schema_conf_layout: schema.conf renamed but parent-dir sync failed: %s",
                      strerror(errno));
            ok = 0;
        }
    } else {
        unlink(tmp);
    }
    flock(lockfd, LOCK_UN);
    fclose(fin);
    return ok ? 0 : -1;
}

```

**7d. `objlock.c` — `RebuildTxn` carries and restores the layout.** Five edits, each anchor exactly once.

(i) struct. Anchor: `    int old_streams;\n    int indexes_may_change;\n};` → replace with `    int old_streams;\n    int indexes_may_change;\n    int old_layout_packed;   /* layout token at begin; restored by abort (F19) */\n};`

(ii) `write_meta`. Anchor:
```c
    int rc = fprintf(f,
                     "version=1\nold_splits=%d\nold_streams=%d\n"
                     "indexes_may_change=%d\n",
                     txn->old_splits, txn->old_streams,
                     txn->indexes_may_change ? 1 : 0) < 0 ? -1 : 0;
```
Replace with:
```c
    int rc = fprintf(f,
                     "version=1\nold_splits=%d\nold_streams=%d\n"
                     "indexes_may_change=%d\nold_layout=%d\n",
                     txn->old_splits, txn->old_streams,
                     txn->indexes_may_change ? 1 : 0,
                     txn->old_layout_packed ? 1 : 0) < 0 ? -1 : 0;
```
(iii) `parse_meta` — three sub-edits. Anchor A: `    int seen_version = 0, seen_splits = 0, seen_streams = 0, seen_indexes = 0;\n    int version = 0, splits = 0, streams = 0, indexes = -1;` → replace with:
```c
    int seen_version = 0, seen_splits = 0, seen_streams = 0, seen_indexes = 0;
    int seen_layout = 0, layout = 0;   /* old_layout is optional: absent = legacy (pre-2026.11.1 meta) */
    int version = 0, splits = 0, streams = 0, indexes = -1;
```
Anchor B: 
```c
        } else if (sscanf(line, "indexes_may_change=%d%c", &indexes,
                          &extra) == 1) {
            if (seen_indexes++) bad = 1;
        } else {
```
→ replace with the same plus a new branch before the final `else`:
```c
        } else if (sscanf(line, "indexes_may_change=%d%c", &indexes,
                          &extra) == 1) {
            if (seen_indexes++) bad = 1;
        } else if (sscanf(line, "old_layout=%d%c", &layout, &extra) == 1) {
            if (seen_layout++) bad = 1;
        } else {
```
Anchor C: 
```c
        streams <= 0 || (indexes != 0 && indexes != 1)) return -1;
    txn->old_splits = splits;
    txn->old_streams = streams;
    txn->indexes_may_change = indexes;
    return 0;
```
→ replace with:
```c
        streams <= 0 || (indexes != 0 && indexes != 1) ||
        (layout != 0 && layout != 1)) return -1;
    txn->old_splits = splits;
    txn->old_streams = streams;
    txn->indexes_may_change = indexes;
    txn->old_layout_packed = layout;
    return 0;
```
(iv) `rebuild_txn_begin`. Anchor: `    txn->indexes_may_change = indexes_may_change ? 1 : 0;\n\n    struct stat st;` → replace with:
```c
    txn->indexes_may_change = indexes_may_change ? 1 : 0;
    {
        int lp = 0;
        if (schema_conf_layout(db_root, object, &lp) != 0) {
            rebuild_txn_free(txn);
            return NULL;   /* layout unreadable: do not stage anything */
        }
        txn->old_layout_packed = lp;
    }

    struct stat st;
```
(v) `rebuild_txn_abort`. Anchor:
```c
    if (update_schema_conf_splits_streams(txn->db_root, txn->object,
                                          txn->old_splits,
                                          txn->old_streams) != 0) return -1;
    invalidate_schema_caches(txn->db_root, txn->object);
```
→ replace with:
```c
    if (update_schema_conf_splits_streams(txn->db_root, txn->object,
                                          txn->old_splits,
                                          txn->old_streams) != 0) return -1;
    {
        int cur = 0;
        if (schema_conf_layout(txn->db_root, txn->object, &cur) != 0) return -1;
        if (cur != txn->old_layout_packed &&
            update_schema_conf_layout(txn->db_root, txn->object,
                                      txn->old_layout_packed) != 0) return -1;
    }
    invalidate_schema_caches(txn->db_root, txn->object);
```
**7e. Test (new meta + old meta).** `test-rebuild-txn-recovery-idempotent` and the `test-rebuild-txn-crash-*` / `test-rebuild-txn-edit-field-after-metadata` / `test-rebuild-txn-done-cleanup-never-restores` family (all in `test_rebuild_txn_recovery.c`) must stay green (it writes/reads meta through the real code). Add nothing else here: the restore behaviour is proven by Task 9's `phase` cases (`after-stage` … `after-reindex` inspect the rolled-back `schema.conf`).

## Task 8 — Conversion core and automatic startup migration (D3, D5, D7; F16, F19–F24)

**Root cause/purpose:** every legacy object must become packed before R6; records must be converted by one exact function (no second grammar), refusals (values that do not fit their declared precision, unclean prior shutdown, pending markers) must happen **before anything is stamped or touched**, and a crash anywhere must leave each object wholly old or wholly new (invariants 3, 4).

**Design (all of it is implemented by the hunks below — nothing is left to judgment):**

1. `layout_migration_run` (new, `embedded.c`) runs inside `shard_db_recover_before_stamp` after the BTRI block and before the clean-flag early return.
2. If `<db_root>/.layout-migrated` exists → return. Otherwise: list objects; read each layout token strictly; if **no** legacy object → write the stamp and return.
3. Preconditions (D7): zero pending markers on every object (always); and on the **first start** of this binary (`shard_db_version_check` == `STAMP`) the clean-shutdown flag must exist. Otherwise refuse with the instruction *start 2026.10.2, stop it cleanly, then upgrade*. A restart that finds the 2026.11.1 stamp (`NOOP`) is a **resume** after a crashed migration and does not re-require the flag (F16).
4. Audit pass (D5): `layout_migrate_object(dry_run=1)` for every legacy object; any value that does not fit ⇒ write `.layout-migration-report` + stderr, refuse, **nothing stamped, nothing touched**.
5. **Stamp first** (F16): on the first start `shard_db_version_stamp` must succeed before any packed layout is published.
6. Convert each legacy object under `objlock_wrlock` via `RebuildTxn` (stage → walk-convert → publish `fields.conf` → publish token → verify the loader agrees → strict reindex → commit). Any failure aborts the transaction (data, `fields.conf`, splits/streams **and the layout token** restored) and refuses startup.
7. Write `.layout-migrated` (checked publication) only after every object succeeded.

Note: on the first start the version is stamped twice — once by the migration (step 5, F16) and again by `cmd_server`/`shard_db_open` after validation. The second write is idempotent (same value); this is intended, do not remove either.

**8a. Tests first** (append to `test_layout_manifest.c`; register). Helpers:

```c
static int lm_write_file(const char *path, const char *text) {
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    int rc = fputs(text, f) < 0 ? -1 : 0;
    if (fclose(f) != 0) rc = -1;
    return rc;
}

/* Make the root look like a clean 2026.10.2 root the new binary has not
   migrated yet: previous version stamp, no layout stamp. */
static void lm_make_pre_migration(const LmRoot *m) {
    char p[420];
    snprintf(p, sizeof(p), "%s/.version", m->db_root);
    ASSERT_EQ_INT(lm_write_file(p, "2026.10.2\n"), 0, "rewrite .version");
    snprintf(p, sizeof(p), "%s/.layout-migrated", m->db_root);
    unlink(p);
}

static char *lm_read(const LmRoot *m, const char *rel) {
    char p[420];
    snprintf(p, sizeof(p), "%s/%s", m->db_root, rel);
    return tu_read_file(p);
}

/* Builds lay/o as a LEGACY object (TEST_BUILD seam, D10) with 3 records and
   an index on qty. numeric fields: legacy2 uses the legacy `numeric:N`
   (scale 2) grammar, amt has an explicit P,S. `bad_amt` stores a value that
   cannot fit amt:numeric:4,2 (D5 audit refusal fixture). Stops the daemon
   gracefully (clean-shutdown evidence) unless `crash` is set. */
static int lm_build_legacy(LmRoot *m, TestEnv *env, int bad_amt, int crash) {
    TestClient *tc = lm_start_client(m, env);
    if (!tc) return -1;
    lm_add_dir(tc);
    char *r = NULL;
    int ok = tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"o\",\"splits\":8,"
        "\"max_key\":16,\"declaration_layout\":\"legacy\","
        "\"fields\":[\"name:varchar:20\",\"qty:int\",\"legacy2:numeric:10\","
        "\"amt:numeric:4,2\",\"note:varchar:30\"],\"indexes\":[\"qty\"]}", &r) == 0 &&
        r && strstr(r, "\"status\":\"created\"");
    free(r);
    static const char *rows[] = {
        "{\"name\":\"alice\",\"qty\":7,\"legacy2\":\"12.34\",\"amt\":\"1.25\",\"note\":\"n1\"}",
        "{\"name\":\"bob\",\"qty\":8,\"legacy2\":\"-0.05\",\"amt\":\"99.99\",\"note\":\"n2\"}",
        "{\"name\":\"carol\",\"qty\":7,\"legacy2\":\"0\",\"amt\":\"0\",\"note\":\"n3\"}",
    };
    for (int i = 0; ok && i < 3; i++) {
        char req[512];
        const char *row = rows[i];
        char bad_row[256];
        if (bad_amt && i == 1) {
            snprintf(bad_row, sizeof(bad_row),
                "{\"name\":\"bob\",\"qty\":8,\"legacy2\":\"-0.05\",\"amt\":\"12345.67\",\"note\":\"n2\"}");
            row = bad_row;
        }
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"o\",\"key\":\"k%d\",\"value\":%s}",
            i + 1, row);
        r = NULL;
        ok = tc_request(tc, req, &r) == 0 && r && strstr(r, "\"status\":\"inserted\"");
        free(r);
    }
    tc_close(tc);
    if (crash) test_env_kill(env); else test_env_stop_keep(env);
    return ok ? 0 : -1;
}

/* Everything the migration must preserve, asserted on a packed object. */
static void lm_assert_migrated(LmRoot *m, TestClient *tc) {
    char *r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"o\"}", &r), 0, "describe");
    ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"packed\""), "object is packed");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"o\",\"key\":\"k1\"}", &r), 0, "get k1");
    ASSERT_TRUE(r && strstr(r, "\"name\":\"alice\"") && strstr(r, "\"qty\":7") &&
                strstr(r, "12.34") && strstr(r, "1.25") && strstr(r, "n1"), "k1 values exact");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"o\",\"key\":\"k2\"}", &r), 0, "get k2");
    ASSERT_TRUE(r && strstr(r, "-0.05") && strstr(r, "99.99"), "k2 negative/boundary numeric exact");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc, "{\"mode\":\"count\",\"dir\":\"lay\",\"object\":\"o\"}", &r), 0, "count");
    ASSERT_EQ_INT(tu_parse_count(r), 3, "count preserved");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"lay\",\"object\":\"o\","
        "\"criteria\":[{\"field\":\"qty\",\"op\":\"eq\",\"value\":\"7\"}]}", &r), 0, "find via rebuilt index");
    ASSERT_TRUE(r && strstr(r, "k1") && strstr(r, "k3") && !strstr(r, "k2"), "index rebuilt over packed records");
    free(r);
    char *stamp = lm_read(m, ".layout-migrated");
    ASSERT_NOT_NULL(stamp, "layout stamp written");
    free(stamp);
    char *conf = lm_read(m, "schema.conf");
    ASSERT_TRUE(conf && strstr(conf, "lay:o:8:16:2:") && strstr(conf, ":layout=packed"), "token published");
    free(conf);
    char *fields = lm_read(m, "lay/o/fields.conf");
    ASSERT_TRUE(fields && strstr(fields, "legacy2:numeric:10,2\n") && strstr(fields, "amt:numeric:4,2\n"),
                "legacy numeric:N rewritten to scale 2, explicit P,S kept");
    free(fields);
    char *ver = lm_read(m, ".version");
    ASSERT_TRUE(ver && strncmp(ver, "2026.11.1", 9) == 0, "root stamped with the new version");
    free(ver);
}

static int test_layout_migration_converts_legacy(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build_legacy(&m, &env, 0, 0), 0, "build legacy fixture");
    char *clean = lm_read(&m, ".shard-db.clean");
    ASSERT_NOT_NULL(clean, "graceful stop left clean-shutdown evidence");
    free(clean);
    lm_make_pre_migration(&m);
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start migrates");
    if (tc) {
        lm_assert_migrated(&m, tc);
        tc_close(tc);
        test_env_stop_keep(&env);
        /* second start skips the migration and still serves */
        tc = lm_start_client(&m, &env);
        ASSERT_NOT_NULL(tc, "second start");
        if (tc) { lm_assert_migrated(&m, tc); tc_close(tc); test_env_stop_keep(&env); }
    }
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-layout-migration-converts-legacy", test_layout_migration_converts_legacy)
```
RED on base+Task 5–7: the daemon starts, nothing migrates, `describe-object` says `legacy` (assertion `object is packed` fails) — paste.

**8b. `query_find.c` — conversion core and `layout_migrate_object`.** Anchor (exactly once):

```c
int rebuild_object_v2(const char *db_root, const char *object,
                      const Schema *old_sch, const TypedSchema *old_ts,
```
Insert **before** it (add `#include <signal.h>` to `query_find.c` if `raise`/`SIGKILL` are undeclared):

```c
/* ===== Legacy → packed conversion (R5 Task 8) ===== */

#ifdef TEST_BUILD
static char g_layout_crash_phase[32];
void layout_migrate_test_crash_phase(const char *phase) {
    snprintf(g_layout_crash_phase, sizeof(g_layout_crash_phase), "%s", phase ? phase : "");
}
#endif

void layout_test_crash_point(const char *phase) {
#ifdef TEST_BUILD
    if (g_layout_crash_phase[0] && strcmp(g_layout_crash_phase, phase) == 0)
        raise(SIGKILL);
#else
    (void)phase;
#endif
}

typedef struct {
    SlotcaskDb        *new_db;       /* NULL in dry-run (audit) mode */
    const TypedSchema *old_ts;       /* legacy snapshot */
    const TypedSchema *new_ts;       /* packed */
    uint64_t           live_count;   /* records converted (or convertible) */
    uint64_t           bad;          /* records whose value does not fit */
    int                error;        /* hard failure */
    int                ndiag;
    char               diag[8][192];
    char               hard[192];
} LayoutWalkCtx;

static void layout_diag_key(char *out, size_t cap, const void *key, size_t klen) {
    const uint8_t *k = (const uint8_t *)key;
    size_t n = klen < 32 ? klen : 32;
    int printable = 1;
    for (size_t i = 0; i < n; i++) if (k[i] < 0x20 || k[i] > 0x7e) printable = 0;
    size_t o = 0;
    for (size_t i = 0; i < n && o + 3 < cap; i++)
        o += (size_t)snprintf(out + o, cap - o, printable ? "%c" : "%02x", k[i]);
    if (klen > n && o + 4 < cap) snprintf(out + o, cap - o, "...");
    else if (o < cap) out[o] = '\0';
}

/* 0 ok (*out/*outlen owned), 1 value does not fit the packed declaration
   (*bad_field, why), -1 hard error (why). */
static int layout_convert_record(const LayoutWalkCtx *ctx, size_t klen,
                                 const void *value, size_t vlen, uint8_t hdr2,
                                 uint8_t **out, size_t *outlen,
                                 int *bad_field, char *why, size_t whysz) {
    const TypedSchema *ots = ctx->old_ts, *nts = ctx->new_ts;
    *out = NULL; *outlen = 0; *bad_field = -1;
    TypedRowView view;
    if (typed_row_open(ots, (const uint8_t *)value, vlen, hdr2, &view) != 0) {
        snprintf(why, whysz, "malformed source record (hdr2=%u vlen=%zu)", hdr2, vlen);
        return -1;
    }
    int nullmap = (hdr2 & SREC_FLAG_NULLMAP) != 0;
    PkrCell cells[MAX_FIELDS];
    uint8_t *owned[MAX_FIELDS] = {0};
    uint8_t *leg = malloc(65537);
    uint8_t *composed = NULL;
    int rc = -1;
    if (!leg) { snprintf(why, whysz, "out of memory"); return -1; }
    for (int k = 0; k < nts->nfields; k++) {
        const TypedField *of = &ots->fields[k];
        const PkrField *pf = &nts->packed.fields[k];
        memset(&cells[k], 0, sizeof(cells[k]));
        if (of->removed) { cells[k].is_null = 1; continue; }   /* tombstoned: slot retained as NULL */
        TypedRowSpan span;
        if (typed_row_span(&view, (size_t)k, &span) != 0 ||
            typed_span_copy_legacy(ots, (size_t)k, &span, leg, 65537) != 0) {
            snprintf(why, whysz, "malformed source field '%s'", of->name);
            goto done;
        }
        int is_null = nullmap ? span.is_null : legacy_field_absent(of, leg);
        if (is_null) { cells[k].is_null = 1; continue; }
        if (pf->kind == PKR_FIXED) {
            if (pf->width != (uint32_t)of->size) {
                snprintf(why, whysz, "field '%s': width mismatch %u vs %d", of->name, pf->width, of->size);
                goto done;
            }
            owned[k] = malloc(pf->width);
            if (!owned[k]) { snprintf(why, whysz, "out of memory"); goto done; }
            memcpy(owned[k], leg, pf->width);
            cells[k] = (PkrCell){owned[k], pf->width, 0};
        } else if (pf->kind == PKR_VARCHAR) {
            size_t len = ((size_t)leg[0] << 8) | leg[1];
            size_t cap = of->size >= 2 ? (size_t)of->size - 2 : 0;
            if (len > cap) len = cap;
            if (len > pf->payload_max) {
                *bad_field = k;
                snprintf(why, whysz, "varchar length %zu exceeds packed capacity %u", len, pf->payload_max);
                rc = 1;
                goto done;
            }
            owned[k] = malloc(len ? len : 1);
            if (!owned[k]) { snprintf(why, whysz, "out of memory"); goto done; }
            if (len) memcpy(owned[k], leg + 2, len);
            cells[k] = (PkrCell){owned[k], len, 0};
        } else { /* PKR_NUMERIC: legacy int64 BE scaled by 10^scale → exact decimal text */
            int scale = of->numeric_scale;
            if (scale < 0 || scale > 18 || pf->scale != scale) {
                *bad_field = k;
                snprintf(why, whysz, "legacy numeric scale %d is not representable (packed scale %d)", scale, pf->scale);
                rc = 1;
                goto done;
            }
            uint64_t u = 0;
            for (int b = 0; b < 8; b++) u = (u << 8) | leg[b];
            int neg = (int64_t)u < 0;
            uint64_t mag = neg ? (0u - u) : u;
            char ds[32], padded[48], text[80];
            int nd = snprintf(ds, sizeof(ds), "%llu", (unsigned long long)mag);
            int pn = 0;
            for (int i = nd; i <= scale; i++) padded[pn++] = '0';
            memcpy(padded + pn, ds, (size_t)nd);
            pn += nd;
            int ip = pn - scale;
            size_t o = 0;
            if (neg) text[o++] = '-';
            memcpy(text + o, padded, (size_t)ip);
            o += (size_t)ip;
            if (scale > 0) {
                text[o++] = '.';
                memcpy(text + o, padded + ip, (size_t)scale);
                o += (size_t)scale;
            }
            text[o] = '\0';
            uint8_t payload[DEC_PAYLOAD_MAX];
            size_t used = 0;
            char perr[96] = {0};
            if (pkr_numeric_encode(pf, text, o, payload, sizeof(payload), &used,
                                   perr, sizeof(perr)) != DEC_OK) {
                *bad_field = k;
                snprintf(why, whysz, "value %s does not fit numeric(%d,%d)%s%s",
                         text, pf->precision, pf->scale, perr[0] ? ": " : "", perr);
                rc = 1;
                goto done;
            }
            owned[k] = malloc(used ? used : 1);
            if (!owned[k]) { snprintf(why, whysz, "out of memory"); goto done; }
            memcpy(owned[k], payload, used);
            cells[k] = (PkrCell){owned[k], used, 0};
        }
    }
    size_t need = 0;
    if (pkr_measure(&nts->packed, cells, (size_t)nts->nfields, &need) != DEC_OK) {
        snprintf(why, whysz, "packed measure failed");
        goto done;
    }
    size_t record_bytes = 0;
    if (pkr_record_fits(&nts->packed, need, klen, SLOTCASK_SEG_MAX_BYTES, &record_bytes) != DEC_OK) {
        snprintf(why, whysz, "packed record exceeds the segment limit");
        goto done;
    }
    composed = malloc(need ? need : 1);
    size_t used = 0;
    if (!composed ||
        pkr_compose(&nts->packed, cells, (size_t)nts->nfields, composed, need, &used) != DEC_OK) {
        snprintf(why, whysz, "packed compose failed");
        goto done;
    }
    *out = composed; *outlen = used; composed = NULL;
    rc = 0;
done:
    for (int k = 0; k < nts->nfields; k++) free(owned[k]);
    free(composed);
    free(leg);
    return rc;
}

static int layout_walk_cb(const uint8_t hash16[16], const void *key, size_t klen,
                          const void *value, size_t vlen, uint8_t hdr2, void *ctxp) {
    (void)hash16;
    LayoutWalkCtx *ctx = (LayoutWalkCtx *)ctxp;
    if (ctx->error) return 1;
    uint8_t *out = NULL;
    size_t outlen = 0;
    int bad_field = -1;
    char why[160] = {0};
    int rc = layout_convert_record(ctx, klen, value, vlen, hdr2, &out, &outlen,
                                   &bad_field, why, sizeof(why));
    if (rc < 0) {
        snprintf(ctx->hard, sizeof(ctx->hard), "%s", why);
        ctx->error = 1;
        return 1;
    }
    if (rc > 0) {
        ctx->bad++;
        if (ctx->ndiag < 8) {
            char k[128];
            layout_diag_key(k, sizeof(k), key, klen);
            snprintf(ctx->diag[ctx->ndiag++], sizeof(ctx->diag[0]),
                     "field '%s' key '%s': %s",
                     ctx->old_ts->fields[bad_field].name, k, why);
        }
        return 0;
    }
    if (ctx->new_db) {
        SlotcaskUpsertOpts o = {0};
        o.record_hdr2 = typed_schema_hdr2(ctx->new_ts);
        if (slotcask_insert_with_hooks(ctx->new_db, -1, key, klen, out, outlen, &o, NULL) != 0) {
            snprintf(ctx->hard, sizeof(ctx->hard), "insert into the packed copy failed");
            ctx->error = 1;
            free(out);
            return 1;
        }
    }
    free(out);
    ctx->live_count++;
    return 0;
}

static int packed_schema_same(const PkrSchema *a, const PkrSchema *b) {
    if (a->nfields != b->nfields || a->prefix != b->prefix ||
        a->directory != b->directory || a->maximum != b->maximum) return 0;
    for (uint32_t i = 0; i < a->nfields; i++) {
        const PkrField *x = &a->fields[i], *y = &b->fields[i];
        if (x->kind != y->kind || x->width != y->width || x->payload_max != y->payload_max ||
            x->offset != y->offset || x->precision != y->precision || x->scale != y->scale ||
            x->bare != y->bare || x->currency != y->currency) return 0;
    }
    return 1;
}

static int layout_write_fields_checked(const char *fpath, char *const *all, size_t nall) {
    char tmp[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s.layout.tmp", fpath) >= (int)sizeof(tmp)) return -1;
    FILE *f = fopen(tmp, "w");
    if (!f) return -1;
    int ok = 1;
    for (size_t i = 0; i < nall; i++) if (fprintf(f, "%s\n", all[i]) < 0) ok = 0;
    if (fflush(f) != 0 || ferror(f)) ok = 0;
    if (fclose(f) != 0) ok = 0;
    if (!ok) { unlink(tmp); return -1; }
    int prc = durability_publish_replace(fpath, tmp, NULL, NULL);
    if (prc < 0) { unlink(tmp); return -1; }
    return prc == 0 ? 0 : -1;
}

int layout_migrate_object(const char *eff_root, const char *object, int dry_run,
                          char *diag, size_t diagsz) {
    if (diag && diagsz) diag[0] = '\0';
#define LM_FAIL(...) do { if (diag && diagsz) snprintf(diag, diagsz, __VA_ARGS__); rc = -1; goto out; } while (0)
    int rc = 0;
    char **all = NULL;
    char **decl = NULL;
    size_t nall = 0, ndecl = 0;
    TypedSchema *old_ts = NULL, *new_ts = NULL;
    RebuildTxn *txn = NULL;
    SlotcaskDb legacy_db, new_db;
    int legacy_open = 0, new_open = 0;
    LayoutWalkCtx wc;
    memset(&wc, 0, sizeof(wc));

    Schema old_sch = load_schema(eff_root, object);
    if (old_sch.splits <= 0) LM_FAIL("schema.conf entry for %s is unreadable", object);
    if (old_sch.packed_layout) goto out;   /* already converted: idempotent */
    TypedSchema *cached = load_typed_schema(eff_root, object);
    if (!cached) LM_FAIL("fields.conf for %s is unreadable", object);
    if (cached->packed.nfields) goto out;
    old_ts = malloc(sizeof(*old_ts));
    new_ts = malloc(sizeof(*new_ts));
    if (!old_ts || !new_ts) LM_FAIL("out of memory");
    memset(new_ts, 0, sizeof(*new_ts));
    *old_ts = *cached;   /* snapshot: the cache slot may be reused after invalidation; enum lists stay cache-owned and are never touched here */

    /* Rewritten fields.conf (comments/blank lines kept verbatim). */
    char fpath[PATH_MAX], obj_dir[PATH_MAX];
    snprintf(fpath, sizeof(fpath), "%s/%s/fields.conf", eff_root, object);
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", eff_root, object);
    FILE *fin = fopen(fpath, "r");
    if (!fin) LM_FAIL("cannot read fields.conf");
    all = calloc(4096, sizeof(char *));
    decl = calloc(MAX_FIELDS, sizeof(char *));
    if (!all || !decl) { fclose(fin); LM_FAIL("out of memory"); }
    for (;;) {
        int rderr = 0;
        char *line = conf_read_line(fin, MAX_FIELD_DEF, &rderr);
        if (!line) {
            if (rderr) { fclose(fin); LM_FAIL("fields.conf line exceeds the limit"); }
            break;
        }
        char *use = line;
        if (line[0] != '\0' && line[0] != '#') {
            char buf[MAX_FIELD_DEF + 8];
            if (layout_packed_decl_line(line, buf, sizeof(buf)) != 0) {
                free(line); fclose(fin); LM_FAIL("field declaration too long");
            }
            use = strdup(buf);
            free(line);
            if (!use) { fclose(fin); LM_FAIL("out of memory"); }
        }
        if (nall >= 4096) { free(use); fclose(fin); LM_FAIL("fields.conf has too many lines"); }
        all[nall++] = use;
        if (use[0] != '\0' && use[0] != '#') {
            if (ndecl >= MAX_FIELDS) { fclose(fin); LM_FAIL("too many fields"); }
            decl[ndecl++] = use;
        }
    }
    fclose(fin);

    char berr[160] = {0};
    if (typed_schema_build_packed((const char *const *)decl, ndecl, new_ts, berr, sizeof(berr)) != 0)
        LM_FAIL("cannot express %s in the packed layout: %s", object, berr);
    if (new_ts->nfields != old_ts->nfields) LM_FAIL("field count changed (%d → %d)", old_ts->nfields, new_ts->nfields);
    for (int i = 0; i < old_ts->nfields; i++)
        if (strcmp(old_ts->fields[i].name, new_ts->fields[i].name) != 0 ||
            old_ts->fields[i].removed != new_ts->fields[i].removed)
            LM_FAIL("field %d changed identity (%s → %s)", i, old_ts->fields[i].name, new_ts->fields[i].name);

    size_t bound = 0;
    if (pkr_slot_bound(&new_ts->packed, (size_t)old_sch.max_key, &bound) != DEC_OK || bound > INT_MAX)
        LM_FAIL("packed slot size exceeds the supported maximum");
    Schema new_sch = old_sch;
    new_sch.max_value = new_ts->total_size;
    new_sch.slot_size = (int)bound;
    new_sch.packed_layout = 1;

    wc.old_ts = old_ts;
    wc.new_ts = new_ts;

    if (dry_run) {
        legacy_open = slotcask_open(&legacy_db, obj_dir, old_sch.splits, old_sch.streams, old_sch.slot_size) == 0;
        if (!legacy_open) LM_FAIL("cannot open %s for audit", object);
        uint64_t invalid_refs = 0;
        if (slotcask_validate_live_refs(&legacy_db, &invalid_refs) != 0) LM_FAIL("invalid live kf references in %s", object);
        slotcask_walk_live(&legacy_db, layout_walk_cb, &wc);
        slotcask_close(&legacy_db);
        legacy_open = 0;
        if (wc.error) LM_FAIL("%s", wc.hard);
        if (wc.bad) {
            size_t o = 0;
            for (int i = 0; i < wc.ndiag && diag && o + 1 < diagsz; i++)
                o += (size_t)snprintf(diag + o, diagsz - o, "  %s\n", wc.diag[i]);
            if (wc.bad > (uint64_t)wc.ndiag && diag && o + 1 < diagsz)
                snprintf(diag + o, diagsz - o, "  ... and %llu more\n",
                         (unsigned long long)(wc.bad - (uint64_t)wc.ndiag));
            rc = 1;
        }
        goto out;
    }

    /* ---- real conversion ---- */
    txn = rebuild_txn_begin(eff_root, object, old_sch.splits, old_sch.streams, 1);
    if (!txn) LM_FAIL("cannot begin the conversion transaction for %s", object);
    layout_test_crash_point("after-stage");
    legacy_open = slotcask_open(&legacy_db, rebuild_txn_legacy_root(txn), old_sch.splits, old_sch.streams, old_sch.slot_size) == 0;
    new_open = slotcask_open(&new_db, obj_dir, new_sch.splits, new_sch.streams, new_sch.slot_size) == 0;
    if (!legacy_open || !new_open) { snprintf(diag, diagsz, "cannot open slotcask handles for %s", object); rc = -1; goto abort_txn; }
    uint64_t invalid_refs = 0;
    if (slotcask_validate_live_refs(&legacy_db, &invalid_refs) != 0) { snprintf(diag, diagsz, "invalid live kf references in %s", object); rc = -1; goto abort_txn; }
    uint64_t src_total = 0, src_deleted = 0;
    if (slotcask_sum_kf_totals(&legacy_db, &src_total, &src_deleted) != 0 || src_deleted > src_total) { snprintf(diag, diagsz, "cannot read the source record count of %s", object); rc = -1; goto abort_txn; }
    uint64_t expected = src_total - src_deleted;
    wc.new_db = &new_db;
    slotcask_walk_live(&legacy_db, layout_walk_cb, &wc);
    slotcask_close(&legacy_db); legacy_open = 0;
    slotcask_close(&new_db);    new_open = 0;
    layout_test_crash_point("after-walk");
    if (wc.error || wc.bad || wc.live_count != expected) {
        snprintf(diag, diagsz, "conversion of %s failed: %s (converted %llu of %llu, %llu unfit)", object,
                 wc.hard[0] ? wc.hard : "count mismatch", (unsigned long long)wc.live_count,
                 (unsigned long long)expected, (unsigned long long)wc.bad);
        rc = -1; goto abort_txn;
    }
    if (layout_write_fields_checked(fpath, all, nall) != 0) { snprintf(diag, diagsz, "cannot publish fields.conf for %s", object); rc = -1; goto abort_txn; }
    if (update_schema_conf_layout(eff_root, object, 1) != 0) { snprintf(diag, diagsz, "cannot publish the layout token for %s", object); rc = -1; goto abort_txn; }
    layout_test_crash_point("after-publish");
    invalidate_schema_caches(eff_root, object);
    invalidate_idx_cache(eff_root, object);
    slotcask_registry_invalidate(eff_root, object);
    {
        Schema chk_sch = load_schema(eff_root, object);
        TypedSchema *chk = load_typed_schema(eff_root, object);
        if (!chk_sch.packed_layout || !chk || chk_sch.slot_size != new_sch.slot_size ||
            !packed_schema_same(&chk->packed, &new_ts->packed)) {
            snprintf(diag, diagsz, "the loader disagrees with the converted layout of %s", object);
            rc = -1; goto abort_txn;
        }
    }
    {
        int nidx = 0, eno = 0;
        if (reindex_object_durable_checked_ex(eff_root, object, 0, &nidx, &eno) != 0) {
            snprintf(diag, diagsz, "strict reindex of %s failed (errno=%d)", object, eno);
            rc = -1; goto abort_txn;
        }
    }
    layout_test_crash_point("after-reindex");
    invalidate_schema_caches(eff_root, object);
    invalidate_idx_cache(eff_root, object);
    slotcask_registry_invalidate(eff_root, object);
    if (rebuild_txn_commit(txn) != 0) { snprintf(diag, diagsz, "cannot commit the conversion of %s", object); rc = -1; goto abort_txn; }
    layout_test_crash_point("after-commit");
    rebuild_txn_cleanup_committed(txn);
    rebuild_txn_free(txn);
    txn = NULL;
    reset_deleted_count(eff_root, object);
    set_count(eff_root, object, wc.live_count > INT_MAX ? INT_MAX : (int)wc.live_count);
    LOG_AUDIT(LOG_SUB_CONFIG, "LAYOUT-MIGRATE %s/%s: %llu records → packed, slot_size=%d→%d",
              eff_root, object, (unsigned long long)wc.live_count, old_sch.slot_size, new_sch.slot_size);
    goto out;

abort_txn:
    if (legacy_open) slotcask_close(&legacy_db);
    if (new_open) slotcask_close(&new_db);
    slotcask_registry_invalidate(eff_root, object);
    if (rebuild_txn_abort(txn) != 0) {
        if (diag && diagsz) {
            size_t l = strlen(diag);
            snprintf(diag + l, diagsz - l, "; rollback incomplete — startup recovery required");
        }
    }
    rebuild_txn_free(txn);
    txn = NULL;
out:
    if (legacy_open) slotcask_close(&legacy_db);
    if (new_open) slotcask_close(&new_db);
    if (all) { for (size_t i = 0; i < nall; i++) free(all[i]); free(all); }
    free(decl);
    if (new_ts) { typed_schema_release(new_ts); free(new_ts); }
    free(old_ts);
    return rc;
#undef LM_FAIL
}

```
Notes the executor must honor: (a) `conf_read_line`, `reindex_object_durable_checked_ex`, `reset_deleted_count`, `set_count`, `invalidate_*`, `durability_publish_replace` are already declared via the headers `query_find.c` includes (the same names are used by `rebuild_object_v2` and `embedded.c`'s BTRI block) — if the compiler reports an implicit declaration, add the include of the header that declares it and note it in the evidence file, do not redefine; (b) the `abort_txn` label is reached after `txn` may be non-NULL only — `rebuild_txn_abort(NULL)` returns -1 harmlessly if `rebuild_txn_begin` failed (that path uses `LM_FAIL`, which goes to `out`, not `abort_txn`); (c) the stack buffer `diag` is never NULL in the real path (callers pass a buffer) — `layout_migration_run` below always does.

**8c. `shard_db_internal.h` — TEST setter declaration.** Anchor (exactly once, **Part A text**): `void schema_conf_test_fsync_fail_call(int n);` Replace with:

```c
void schema_conf_test_fsync_fail_call(int n);
/* Test-only (db.env LAYOUT_MIGRATE_TEST_CRASH=<phase>): the startup layout
   migration raises SIGKILL at the named phase — after-recovery, after-audit,
   after-stage, after-walk, after-publish, after-reindex, after-commit. */
void layout_migrate_test_crash_phase(const char *phase);
```
**8d. `config.c` — db.env knob.** First `printf %s 'LAYOUT_MIGRATE_TEST_CRASH=' | wc -c` must print `26` (halt otherwise). Anchor (exactly once, **Part A text**):
```c
            schema_conf_test_fsync_fail_call(atoi(p + 33));
#endif
```
Replace with:
```c
            schema_conf_test_fsync_fail_call(atoi(p + 33));
        } else if (strncmp(p, "LAYOUT_MIGRATE_TEST_CRASH=", 26) == 0) {
            /* Test-only knob; not a documented production setting. */
            char *v = p + 26;
            v[strcspn(v, "\r\n")] = '\0';
            layout_migrate_test_crash_phase(v);
#endif
```
**8e. `embedded.c` — the orchestrator and its call.** Insert **before** `int shard_db_recover_before_stamp(const char *db_root,` (anchor exactly once):

```c
/* ── Layout migration (R5 Task 8) ──
   Converts every legacy object to the packed layout. See the plan for the
   ordering rationale: audit and preconditions first, version stamp second,
   per-object RebuildTxn conversion third, completion stamp last. */
#define LAYOUT_MIGRATION_STAMP  ".layout-migrated"
#define LAYOUT_MIGRATION_REPORT ".layout-migration-report"

static int layout_stamp_exists(const char *db_root) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", db_root, LAYOUT_MIGRATION_STAMP);
    return access(path, F_OK) == 0;
}

static int layout_stamp_write(const char *db_root) {
    char tmp[PATH_MAX], final[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s/%s.tmp", db_root, LAYOUT_MIGRATION_STAMP);
    snprintf(final, sizeof(final), "%s/%s", db_root, LAYOUT_MIGRATION_STAMP);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    int rc = dprintf(fd, "layout-migration complete (binary %s)\n", SHARD_DB_VERSION) > 0 ? 0 : -1;
    if (close(fd) != 0) rc = -1;
    if (rc != 0) { (void)unlink(tmp); return -1; }
    int prc = durability_publish_replace(final, tmp, NULL, NULL);   /* fsync(tmp) → rename → fsync(dir) */
    if (prc < 0) { (void)unlink(tmp); return -1; }
    return prc == 0 ? 0 : -1;   /* renamed but unconfirmed durable: refuse; the next start re-checks */
}

static void layout_report_write(const char *db_root, const char *text) {
    char tmp[PATH_MAX], final[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s/%s.tmp", db_root, LAYOUT_MIGRATION_REPORT);
    snprintf(final, sizeof(final), "%s/%s", db_root, LAYOUT_MIGRATION_REPORT);
    FILE *f = fopen(tmp, "w");
    if (!f) return;
    int ok = fputs(text, f) >= 0;
    if (fclose(f) != 0) ok = 0;
    if (!ok || rename(tmp, final) != 0) (void)unlink(tmp);
}

static void layout_report_remove(const char *db_root) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", db_root, LAYOUT_MIGRATION_REPORT);
    (void)unlink(path);
}

static int layout_migration_run(const char *db_root) {
    if (layout_stamp_exists(db_root)) return 0;
    layout_test_crash_point("after-recovery");

    StartupSchemaEntry *ents = NULL;
    size_t n = 0;
    if (startup_read_schema(db_root, &ents, &n) != 0) return -1;

    size_t nlegacy = 0;
    for (size_t i = 0; i < n; i++) {
        char eff[PATH_MAX];
        snprintf(eff, sizeof(eff), "%s/%s", db_root, ents[i].dir);
        int packed = 0;
        if (schema_conf_layout(eff, ents[i].object, &packed) != 0) {
            fprintf(stderr, "shard-db: refusing to start: unreadable layout token for %s/%s\n",
                    ents[i].dir, ents[i].object);
            free(ents);
            return -1;
        }
        if (!packed) nlegacy++;
    }
    if (nlegacy == 0) {
        free(ents);
        if (layout_stamp_write(db_root) != 0) return -1;
        layout_report_remove(db_root);
        return 0;
    }

    char disk[64] = {0};
    int decision = shard_db_version_check(db_root, disk, sizeof(disk));
    if (decision != SHARD_DB_VERSION_STAMP && decision != SHARD_DB_VERSION_NOOP) {
        free(ents);
        return -1;
    }
    int first_start = decision == SHARD_DB_VERSION_STAMP;

    /* D7: the previous version must have closed cleanly with no pending
       markers. Nothing has been touched yet. */
    if (first_start && !clean_flag_exists(db_root)) {
        fprintf(stderr,
                "shard-db: refusing to start: database was not closed cleanly by shard-db %s. "
                "Start shard-db %s against this DB_ROOT, stop it cleanly, then upgrade to %s.\n",
                disk, SHARD_DB_REQUIRED_SOURCE_VERSION, SHARD_DB_VERSION);
        free(ents);
        return -1;
    }
    for (size_t i = 0; i < n; i++) {
        char data_dir[PATH_MAX];
        snprintf(data_dir, sizeof(data_dir), "%s/%s/%s", db_root, ents[i].dir, ents[i].object);
        if (object_has_pending_markers(data_dir) != 0) {
            fprintf(stderr,
                    "shard-db: refusing to start: %s/%s has pending commit markers. "
                    "Start shard-db %s against this DB_ROOT, stop it cleanly, then upgrade to %s.\n",
                    ents[i].dir, ents[i].object, SHARD_DB_REQUIRED_SOURCE_VERSION, SHARD_DB_VERSION);
            free(ents);
            return -1;
        }
    }

    /* D5 audit: read-only, whole root, before the stamp. */
    char report[2048];
    size_t rl = 0;
    int refused = 0;
    report[0] = '\0';
    for (size_t i = 0; i < n; i++) {
        char eff[PATH_MAX], diag[1024];
        int packed = 0;
        snprintf(eff, sizeof(eff), "%s/%s", db_root, ents[i].dir);
        (void)schema_conf_layout(eff, ents[i].object, &packed);
        if (packed) continue;
        if (objlock_wrlock(eff, ents[i].object) != 0) { free(ents); return -1; }
        int rc = layout_migrate_object(eff, ents[i].object, 1, diag, sizeof(diag));
        objlock_wrunlock(eff, ents[i].object);
        if (rc != 0) {
            refused = 1;
            if (rl + 1 < sizeof(report))
                rl += (size_t)snprintf(report + rl, sizeof(report) - rl,
                                       "%s/%s: layout migration refused:\n%s%s",
                                       ents[i].dir, ents[i].object, diag,
                                       diag[0] && diag[strlen(diag) - 1] == '\n' ? "" : "\n");
        }
    }
    if (refused) {
        fputs(report, stderr);
        layout_report_write(db_root, report);
        free(ents);
        return -1;
    }
    layout_test_crash_point("after-audit");

    /* F16: the new version must be stamped BEFORE any packed layout is
       published, otherwise a crash leaves a packed object under an old
       stamp that 2026.10.2 would open as legacy. */
    if (first_start && shard_db_version_stamp(db_root) != SHARD_DB_VERSION_STAMP_OK) {
        free(ents);
        return -1;
    }

    for (size_t i = 0; i < n; i++) {
        char eff[PATH_MAX], diag[1024];
        int packed = 0;
        snprintf(eff, sizeof(eff), "%s/%s", db_root, ents[i].dir);
        (void)schema_conf_layout(eff, ents[i].object, &packed);
        if (packed) continue;
        if (objlock_wrlock(eff, ents[i].object) != 0) { free(ents); return -1; }
        int rc = layout_migrate_object(eff, ents[i].object, 0, diag, sizeof(diag));
        objlock_wrunlock(eff, ents[i].object);
        if (rc != 0) {
            fprintf(stderr, "shard-db: refusing to start: layout migration failed for %s/%s: %s\n",
                    ents[i].dir, ents[i].object, diag);
            free(ents);
            return -1;
        }
    }
    free(ents);
    if (layout_stamp_write(db_root) != 0) return -1;
    layout_report_remove(db_root);
    return 0;
}

```
and the call. Anchor (exactly once):
```c
    int was_clean = clean_flag_exists(db_root);
    if (clean_flag_remove(db_root) != 0) return -1;
```
Replace with:
```c
    /* Layout migration: after artifact recovery and the BTRI migration,
       before the clean flag is consumed (its presence is a D7 precondition). */
    if (layout_migration_run(db_root) != 0) return -1;

    int was_clean = clean_flag_exists(db_root);
    if (clean_flag_remove(db_root) != 0) return -1;
```
Required includes in `embedded.c`: `grep -n 'durability.h\|durability_publish_replace' src/db/embedded.c` — if not already reachable add the header that declares `durability_publish_replace` (the same one `query_find.c` gets via `types.h`); `fcntl.h` is already used (`open`).

**8f. Verify and GREEN.** Build zero warnings. `test-layout-migration-converts-legacy` GREEN. Fresh `run-all`: **every existing test must still be green** — roots created by the existing tests are fresh (no legacy objects ⇒ `layout_migration_run` writes the stamp and returns), and the default create is still legacy until Task 11, so existing legacy-object tests run unchanged (their roots are never "pre-migration": the stamp exists after the first start). Paste the tail. **Revert-proof (paste both):** (1) in `layout_walk_cb` temporarily make the numeric branch of `layout_convert_record` skip the `pf->scale != scale` agreement check **and** write `numeric:N` unchanged (revert hunk 7a's `,2` insertion by temporarily returning the line verbatim from `layout_packed_decl_line`) → expect RED at `legacy numeric:N rewritten to scale 2` / `k1 values exact` (the 12.34 renders as 1234); restore. (2) temporarily delete the `if (layout_migration_run(db_root) != 0) return -1;` call → expect RED at `object is packed`; restore.

## Task 9 — Refusal, precondition and crash-point proofs (D5, D7, invariant 4; F16)

No production code unless a case fails for a real reason — in that case **halt and write `PLAN_NOTES.md`** (do not patch around a failing proof). Append to `test_layout_manifest.c` and register each case.

```c
/* The root must be exactly as the previous version left it. */
static void lm_assert_untouched(LmRoot *m) {
    char *conf = lm_read(m, "schema.conf");
    ASSERT_TRUE(conf && !strstr(conf, "layout=packed"), "schema.conf untouched");
    free(conf);
    char *ver = lm_read(m, ".version");
    ASSERT_TRUE(ver && strcmp(ver, "2026.10.2\n") == 0, "version NOT advanced (refusal precedes the stamp)");
    free(ver);
    char *stamp = lm_read(m, ".layout-migrated");
    ASSERT_TRUE(stamp == NULL, "no completion stamp");
    free(stamp);
    char *fields = lm_read(m, "lay/o/fields.conf");
    ASSERT_TRUE(fields && strstr(fields, "legacy2:numeric:10\n"), "fields.conf untouched");
    free(fields);
}

static int test_layout_migration_refusals(void) {
    /* (a) previous run was killed: no clean-shutdown evidence (D7) */
    {
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
        lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build_legacy(&m, &env, 0, 1), 0, "build (killed)");
        lm_make_pre_migration(&m);
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0, "unclean root refused");
        lm_assert_untouched(&m);
        lm_destroy(&m);
    }
    /* (b) clean stop but a pending commit marker is present (D7) */
    {
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
        lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build_legacy(&m, &env, 0, 0), 0, "build");
        char mk[420];
        snprintf(mk, sizeof(mk), "%s/lay/o/data/kf/000_batch_1_marker.dat", m.db_root);
        /* name verified against marker_tmp_name_valid(): final markers are
           "%x_batch_%u[_%16llx]_marker.dat"; an unrecognised file in the marker
           namespace makes the scan fail closed (-1), which also refuses. */
        ASSERT_EQ_INT(lm_write_file(mk, "x"), 0, "plant marker");
        lm_make_pre_migration(&m);
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0, "pending marker refused");
        ASSERT_TRUE(tu_file_exists(mk), "marker preserved, never replayed or deleted");
        lm_assert_untouched(&m);
        lm_destroy(&m);
    }
    /* (c) a legacy numeric value does not fit its declared precision (D5) */
    {
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
        lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build_legacy(&m, &env, 1, 0), 0, "build (unfit value)");
        lm_make_pre_migration(&m);
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0, "unfit value refused");
        lm_assert_untouched(&m);
        char *rep = lm_read(&m, ".layout-migration-report");
        ASSERT_TRUE(rep && strstr(rep, "amt") && strstr(rep, "k2") && strstr(rep, "12345.67"),
                    "report names the field, the key and the value");
        free(rep);
        lm_destroy(&m);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

static void lm_env_crash(const LmRoot *m, const char *phase) {
    char extra[96];
    snprintf(extra, sizeof(extra), "export LAYOUT_MIGRATE_TEST_CRASH=%s\n", phase);
    lm_env(m, extra);
}

/* kill the daemon at each migration phase; the next start must find every
   object wholly old (before commit) or wholly new (after commit), and a
   clean start must finish the job */
static int test_layout_migration_crash_points(void) {
    static const char *phases[] = {
        "after-recovery", "after-audit", "after-stage", "after-walk",
        "after-publish", "after-reindex", "after-commit",
    };
    for (size_t pi = 0; pi < sizeof(phases) / sizeof(phases[0]); pi++) {
        const char *phase = phases[pi];
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
        lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build_legacy(&m, &env, 0, 0), 0, "build legacy fixture");
        lm_make_pre_migration(&m);

        lm_env_crash(&m, phase);                                     /* run 1: die at <phase> */
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0, "run 1 killed");

        lm_env_crash(&m, "after-recovery");                          /* run 2: recover, then die before converting */
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0, "run 2 killed after recovery");
        int committed = strcmp(phase, "after-commit") == 0;
        char *conf = lm_read(&m, "schema.conf");
        char *fields = lm_read(&m, "lay/o/fields.conf");
        if (committed) {
            ASSERT_TRUE(conf && strstr(conf, ":layout=packed"), "committed: token stays");
            ASSERT_TRUE(fields && strstr(fields, "legacy2:numeric:10,2\n"), "committed: packed fields.conf");
        } else {
            ASSERT_TRUE(conf && !strstr(conf, "layout=packed"), "rolled back: token restored (F19)");
            ASSERT_TRUE(fields && strstr(fields, "legacy2:numeric:10\n"), "rolled back: legacy fields.conf");
        }
        free(conf); free(fields);

        lm_env(&m, NULL);                                            /* run 3: no knob — finish */
        TestClient *tc = lm_start_client(&m, &env);
        ASSERT_NOT_NULL(tc, "run 3 starts");
        if (tc) {
            lm_assert_migrated(&m, tc);
            tc_close(tc);
            test_env_stop_keep(&env);
        }
        lm_destroy(&m);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-layout-migration-refusals", test_layout_migration_refusals)
TEST_REGISTER("test-layout-migration-crash-points", test_layout_migration_crash_points)
```

**9a. Expected state per case** — if reality differs, that is a defect in Tasks 7–8: halt.
- *refusals*: daemon exits non-zero before listening; `.version` is still `2026.10.2` (audit and preconditions precede the stamp), no token, no stamp, `fields.conf` byte-identical, the planted marker file still present.
- *crash points*: `after-recovery`/`after-audit` ⇒ nothing converted; `after-stage`…`after-reindex` ⇒ run 2's recovery (`rebuild_recovery` → `rebuild_txn_abort`) restored data, `fields.conf` **and the layout token** (the RED for F19 is: remove hunk 7d(v) and the `rolled back: token restored` assertion fails because the token survives); `after-commit` ⇒ object already packed and `layout_migration_run` just writes the stamp.

**9b. Revert-proofs (paste):** (1) remove hunk 7d(v) only → `test-layout-migration-crash-points` RED at `rolled back: token restored (F19)`; restore. (2) in `layout_migration_run` move the `shard_db_version_stamp` block after the conversion loop → `test-layout-migration-crash-points` RED is **not** expected (the test cannot observe the stamp order directly) — instead add this assertion to the `after-publish` iteration just after run 1 (`lm_assert_stamp_before_publish`):
```c
        if (strcmp(phase, "after-publish") == 0) {
            char *ver = lm_read(&m, ".version");
            ASSERT_TRUE(ver && strncmp(ver, "2026.11.1", 9) == 0,
                        "F16: the new version is stamped before any packed layout is published");
            free(ver);
        }
```
(insert right after the `run 1 killed` assertion). With the stamp moved after the loop this assertion is RED; restore → GREEN. Paste both.

**9c. GREEN.** All three new cases pass; fresh `run-all` green.

## Task 10 — Backup / restore carry and enforce the layout (D8; F22)

**Root cause:** `cmd_backup` writes no layout; `cmd_restore` swaps `data/`, `indexes/`, `metadata/` and `fields.conf` blindly. After migration every live object is packed, so restoring an old (legacy-layout) backup would put legacy bytes under a packed manifest.

**10a. Test first** (append, register):

```c
static int test_backup_restore_layout(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start");
    if (!tc) { lm_destroy(&m); return 1; }
    lm_add_dir(tc);
    char *r = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"p\",\"splits\":8,\"max_key\":16,"
        "\"declaration_layout\":\"packed\",\"fields\":[\"name:varchar:20\",\"amt:numeric:12,3\"]}", &r);
    free(r); r = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\","
        "\"value\":{\"name\":\"a\",\"amt\":\"1.500\"}}", &r);
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"lay\",\"object\":\"p\"}", &r), 0, "backup");
    ASSERT_TRUE(r && strstr(r, "\"status\":\"backed_up\""), "backed up");
    char stamp[64] = {0};
    const char *bp = r ? strstr(r, "/backup/") : NULL;
    if (bp) sscanf(bp + 8, "%63[^\"]", stamp);
    ASSERT_TRUE(stamp[0] != '\0', "backup id parsed");
    free(r); r = NULL;
    char meta[500];
    snprintf(meta, sizeof(meta), "%s/lay/p/backup/%s/object.json", m.db_root, stamp);
    char *mj = tu_read_file(meta);
    ASSERT_TRUE(mj && strstr(mj, "\"layout\":\"packed\""), "backup records the layout");
    free(mj);

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k2\","
        "\"value\":{\"name\":\"b\",\"amt\":\"2.000\"}}", &r);
    free(r); r = NULL;
    char req[300];
    snprintf(req, sizeof(req),
        "{\"mode\":\"restore\",\"dir\":\"lay\",\"object\":\"p\",\"from\":\"%s\",\"force\":true}", stamp);
    ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "restore");
    ASSERT_TRUE(r && strstr(r, "\"status\":\"restored\""), "same-layout restore works");
    free(r); r = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r);
    ASSERT_TRUE(r && strstr(r, "1.500"), "k1 restored exactly");
    free(r); r = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"lay\",\"object\":\"p\"}", &r);
    ASSERT_EQ_INT(tu_parse_count(r), 1, "k2 gone after restore");
    free(r); r = NULL;

    /* a backup that does not say "packed" (taken before this release) is refused even with force */
    ASSERT_EQ_INT(lm_write_file(meta, "{\"splits\":8,\"max_key\":16,\"storage_version\":2,\"streams\":1}\n"), 0, "forge legacy meta");
    ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "restore request");
    ASSERT_TRUE(r && strstr(r, "\"error\"") && strstr(r, "layout"), "legacy-layout backup refused");
    free(r); r = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r);
    ASSERT_TRUE(r && strstr(r, "1.500"), "live object untouched by the refused restore");
    free(r);
    tc_close(tc);
    test_env_stop_keep(&env);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-backup-restore-layout", test_backup_restore_layout)
```
RED on base+8: `backup records the layout` fails (no `layout` key) and the forged-legacy restore succeeds (`legacy-layout backup refused` fails). Paste.

**10b. `query_maint.c` — `cmd_backup` records the layout.** Anchor (exactly once):
```c
        fprintf(mf, "{\"splits\":%d,\"max_key\":%d,\"storage_version\":2,\"streams\":%d}\n",
                sch.splits, sch.max_key, sch.streams);
```
Replace with:
```c
        fprintf(mf, "{\"splits\":%d,\"max_key\":%d,\"storage_version\":2,\"streams\":%d,\"layout\":\"%s\"}\n",
                sch.splits, sch.max_key, sch.streams,
                sch.packed_layout ? "packed" : "legacy");
```
**10c. `query_maint.c` — `parse_object_meta` reads it.** Three edits, each anchor exactly once.
(i) signature: `static int parse_object_meta(const char *path, int *out_splits, int *out_max_key,\n                              int *out_storage_version, int *out_streams) {` → add a trailing parameter: `... int *out_storage_version, int *out_streams, int *out_packed) {`.
(ii) anchor: `    p = strstr(buf, "\"streams\":");\n    if (p) streams = atoi(p + strlen("\"streams\":"));` → replace with the same plus:
```c
    int packed = strstr(buf, "\"layout\":\"packed\"") != NULL;
```
(iii) anchor: `    if (out_streams)         *out_streams = streams;\n    return 1;` → replace with `    if (out_streams)         *out_streams = streams;\n    if (out_packed)          *out_packed = packed;\n    return 1;`.

**10d. `query_maint.c` — `ensure_schema_conf_line` writes the token for orphan restores.** Anchor (exactly once): `static int ensure_schema_conf_line(const char *db_root, const char *object,\n                                   int splits, int max_key,\n                                   int storage_version, int streams) {` → add `, int packed`. And replace the two `fprintf` in its `if (!found)` branch:
```c
        if (storage_version >= 2)
            fprintf(f, "%s%d:%d:%d:%d\n", prefix, splits, max_key,
                    storage_version, streams);
```
with
```c
        if (storage_version >= 2)
            fprintf(f, "%s%d:%d:%d:%d%s\n", prefix, splits, max_key,
                    storage_version, streams, packed ? ":layout=packed" : "");
```
**10e. `query_maint.c` — `cmd_restore` enforces it.** Anchor (exactly once):
```c
    int bak_splits = 0, bak_max_key = 0, bak_sv = 1, bak_streams = 0;
    int have_meta = parse_object_meta(meta_src, &bak_splits, &bak_max_key,
                                       &bak_sv, &bak_streams);
    if (have_meta) {
        Schema live = load_schema(db_root, object);
```
Replace with:
```c
    int bak_splits = 0, bak_max_key = 0, bak_sv = 1, bak_streams = 0, bak_packed = 0;
    int have_meta = parse_object_meta(meta_src, &bak_splits, &bak_max_key,
                                       &bak_sv, &bak_streams, &bak_packed);
    if (!have_meta) {
        /* No layout evidence: after the layout migration every live object
           is packed, so unknown bytes must never be copied over it. */
        objlock_wrunlock(db_root, object);
        OUT("{\"error\":\"backup has no layout metadata (object.json); restore refused\"}\n");
        return 1;
    }
    if (have_meta) {
        Schema live = load_schema(db_root, object);
        if ((live.splits > 0 && (live.packed_layout ? 1 : 0) != bak_packed) ||
            (live.splits <= 0 && !bak_packed)) {
            objlock_wrunlock(db_root, object);
            OUT("{\"error\":\"backup layout (%s) does not match the live object's layout (%s); restore refused — a backup taken before the layout migration cannot be restored into a migrated object\"}\n",
                bak_packed ? "packed" : "legacy",
                live.splits > 0 ? (live.packed_layout ? "packed" : "legacy") : "absent");
            return 1;
        }
```
and in the same function change the call `ensure_schema_conf_line(db_root, object, bak_splits, bak_max_key,\n                                     bak_sv, bak_streams)` to pass `, bak_packed` as the last argument (anchor that exact 2-line call text; exactly once).
**10f. GREEN** `test-backup-restore-layout`; fresh `run-all`. Revert-proof: remove 10e's refusal → RED at `legacy-layout backup refused`; restore. Existing backup/restore tests (`grep -ln '"mode":"restore"' src/test/cases/*.c`): they create legacy objects (default still legacy until Task 11) and restore **their own** backups, which now carry `"layout":"legacy"` and match the live legacy object — they stay green. After Task 11 they run on packed objects and also stay green; a case that restores a hand-written `object.json` without `layout` is expected to RED in Task 11 triage (classification (ii)).

## Task 11 — Default flip: every new object is created packed (D3, D4, D10) and full-suite triage

One-line production change; the work is the triage.

**11a. `server.c` hunk.** Anchor (exactly once, from Task 5b): `        int create_packed=0;` → replace with `        int create_packed=1;`.

**11b. Run the full suite, fresh:** `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`. **Expect failures** — every existing test now creates packed objects, whose grammar differs deliberately (R1–R4: `numeric:N` is scale 0, bare `varchar`, exact numeric rendering, slot sizes derived from the packed bound, `describe-object` says `packed`).

**11c. Triage, do not fix.** Write `docs/plans/2026-11-02-r5-flip-triage.md` with one row per failing case: *case name · first failing assertion (verbatim) · classification*:
1. **fixture** — the case targets legacy behaviour on purpose (legacy storage/migration/`numeric:N` scale-2 semantics): the *only* edit pre-authorised is adding `"declaration_layout":"legacy"` to that case's `create-object` request(s). Do not make this edit yet — list it.
2. **difference** — packed and legacy deliberately differ (rendering, limits, error text): needs the human's decision on whether the test or the product changes.
3. **defect** — a packed path is wrong (crash, wrong value, hang, leak).

Then **stop**: write the triage file, paste the pass/fail totals, and hand back. **No test edit, no code fix, no assertion change until the human has read the triage.** (A defect is fixed in its own follow-up task in the same plan after review; a fixture edit is applied only after approval.)

**11d. Revert-proof:** n/a (the flip is exercised by the whole suite). Evidence: the pass/fail totals before (Task 10 GREEN) and after.

## Task 12 — Bounds and documentation (invariant 7)

**12a. Test first** (append, register). Limits are unchanged and enforced on a packed object:

```c
_Static_assert(PKR_VARCHAR_MAX == 4194304u, "bare varchar cap must stay 4 MiB");
_Static_assert(SLOTCASK_SEG_MAX_BYTES == 128ull * 1024 * 1024, "segment cap must stay 128 MiB");
_Static_assert(MAX_FIELDS == 256, "field cap must stay 256");

static int test_packed_limits_unchanged(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start");
    if (!tc) { lm_destroy(&m); return 1; }
    lm_add_dir(tc);
    char *r = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"p\",\"splits\":8,\"max_key\":16,"
        "\"declaration_layout\":\"packed\",\"fields\":[\"body:varchar\"]}", &r);
    ASSERT_TRUE(r && strstr(r, "\"status\":\"created\""), "bare varchar object");
    free(r); r = NULL;
    size_t big = 1024 * 1024;                       /* 1 MiB: accepted */
    char *req = malloc(big + 256);
    ASSERT_NOT_NULL(req, "alloc");
    if (req) {
        int n = snprintf(req, 128, "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"big\",\"value\":{\"body\":\"");
        memset(req + n, 'a', big);
        strcpy(req + n + big, "\"}}");
        ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "insert 1 MiB");
        ASSERT_TRUE(r && strstr(r, "\"status\":\"inserted\""), "1 MiB bare varchar accepted");
        free(r); r = NULL;
        free(req);
    }
    size_t over = 4u * 1024 * 1024 + 1;             /* 4 MiB + 1: refused */
    req = malloc(over + 256);
    ASSERT_NOT_NULL(req, "alloc over");
    if (req) {
        int n = snprintf(req, 128, "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"over\",\"value\":{\"body\":\"");
        memset(req + n, 'a', over);
        strcpy(req + n + over, "\"}}");
        ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "insert over cap");
        ASSERT_TRUE(r && strstr(r, "\"error\""), "4 MiB + 1 refused");
        free(r); r = NULL;
        free(req);
    }
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"lay\",\"object\":\"p\"}", &r);
    ASSERT_EQ_INT(tu_parse_count(r), 1, "refused record not stored");
    free(r);
    tc_close(tc);
    test_env_stop_keep(&env);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* remove-field / rename-field / indexes keep working on a packed object
   (they edit fields.conf text; the strict loader re-reads it) */
static int test_packed_metadata_ops(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start");
    if (!tc) { lm_destroy(&m); return 1; }
    lm_add_dir(tc);
    char *r = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"p\",\"splits\":8,\"max_key\":16,"
        "\"declaration_layout\":\"packed\",\"fields\":[\"name:varchar:20\",\"qty:int\",\"gone:int\"],"
        "\"indexes\":[\"qty\"]}", &r);
    free(r); r = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\","
        "\"value\":{\"name\":\"a\",\"qty\":5,\"gone\":9}}", &r);
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"rename-field\",\"dir\":\"lay\",\"object\":\"p\",\"old\":\"name\",\"new\":\"title\"}", &r), 0, "rename");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "rename-field works on packed");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"remove-field\",\"dir\":\"lay\",\"object\":\"p\",\"fields\":[\"gone\"]}", &r), 0, "remove");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "remove-field works on packed");
    free(r); r = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r);
    ASSERT_TRUE(r && strstr(r, "\"title\":\"a\"") && strstr(r, "\"qty\":5") && !strstr(r, "\"gone\""),
                "record intact through metadata ops");
    free(r); r = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"lay\",\"object\":\"p\","
        "\"criteria\":[{\"field\":\"qty\",\"op\":\"eq\",\"value\":\"5\"}]}", &r);
    ASSERT_TRUE(r && strstr(r, "k1"), "index still serves after metadata ops");
    free(r);
    tc_close(tc);
    test_env_stop_keep(&env);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-packed-limits-unchanged", test_packed_limits_unchanged)
TEST_REGISTER("test-packed-metadata-ops", test_packed_metadata_ops)
```
(The test file needs `#include "packed_record.h"` and `#include "slotcask.h"` for the `_Static_assert`s — add them next to the existing includes; if the test build does not expose those headers to `src/test/cases`, drop the three `_Static_assert` lines and add this note to the evidence file instead of weakening anything else.) If `remove-field`'s request key is not `fields` (check `docs/query-protocol/schema-mutations.md` → `## remove-field` and use the documented shape), correct the test, not the product. Both cases are expected to be GREEN immediately (they pin existing behaviour); paste the output and say so — they are regression pins, not bug fixes.

**12b. Docs (same branch).** Exact edits:
1. `docs/release-notes/2026.11.1.md` (created in Part A Task 3): **append**
```markdown

## Upgrading from 2026.10.2 — automatic layout migration

On first start 2026.11.1 converts **every object** to the packed layout, then stamps `.layout-migrated` at the DB_ROOT and serves. Requirements and behaviour:

- The previous version must have **closed cleanly with no pending commit markers**. Otherwise 2026.11.1 refuses to start and tells you to start 2026.10.2, stop it cleanly, then upgrade again. The migration never replays markers and never touches an unclean root.
- Before anything is stamped or changed, every value is checked against its declared type. A legacy `numeric:P,S` value that does not fit `P` **refuses startup** (nothing is modified) and writes `.layout-migration-report` (field, key, value; at most eight per object) at the DB_ROOT. Fix it with 2026.10.2 (e.g. widen `P`), stop cleanly, upgrade again.
- Legacy `numeric:N` (no comma) meant scale 2 and is rewritten to `numeric:N,2`; `numeric:P,S` is kept and `P` is now enforced.
- The version stamp is written **before** the first object is converted, so 2026.10.2 can no longer open a partially converted root. A crash mid-migration is safe: each object is wholly old or wholly new, and the next start resumes.
- Backups taken before this release cannot be restored into a migrated object (the restore is refused with a layout error).
- `add-field`, `edit-field` and `vacuum compact` on a migrated object are not yet supported (they return a clear error); `vacuum` with `splits`, `remove-field`, `rename-field`, indexes, backup and restore are supported.
```
2. `docs/reference/changelog.md` — anchor (exactly once): `**BREAKING (R4 — strict scalar grammars).**` → insert **before** it (keep the blank line after):
```markdown
**2026.11.1 — packed layout, automatic migration (R5).** The `layout=packed`
token in `schema.conf` is the authoritative object layout (a malformed token
fails the load; there is no legacy fallback). First start migrates **every
object** to the packed layout: the previous version must have closed cleanly
with no pending markers, legacy values that do not fit their declared numeric
precision refuse startup (`.layout-migration-report`), and the new version is
stamped before the first object is converted. New objects are created packed:
`numeric:N` now means scale 0 for new objects (migration rewrites legacy
`numeric:N` to `numeric:N,2`); bare `varchar`/`numeric` are accepted;
`create-object` takes `declaration_layout:"packed"`. Backup `object.json` gains
`"layout"`; backups without it cannot be restored into a migrated object.
`add-field`, `edit-field` and `vacuum compact` are not yet supported on packed
objects (they return an error; follow-up R5b).

```
3. `docs/reference/error-codes.md` — anchor (exactly once, a table row):
```markdown
| `invalid field name (no :, +, /, spaces)` | Reserved character in field name. | Use `[a-zA-Z0-9_-]`. |
```
→ replace with that row followed by these rows:
```markdown
| `invalid field name (no :, +, /, spaces)` | Reserved character in field name. | Use `[a-zA-Z0-9_-]`. |
| `legacy layout objects can no longer be created` | `create-object` with `declaration_layout:"legacy"` on a production build. | Omit it; objects are packed. |
| `unknown declaration_layout; expected "packed"` | `declaration_layout` is neither `packed` nor (test builds) `legacy`. | Use `"packed"`. |
| `invalid packed field declarations: <reason>` | A packed field declaration failed the packed parser (e.g. `numeric:0`, `varchar:0`, `varchar:4194305`, scale > precision). | Fix the declaration. |
| `vacuum compact / add-field / edit-field are not supported on packed-layout objects yet` | Recomposing operation on a packed object. | Recreate the object, or wait for R5b. |
| `add-field is not supported on packed-layout objects yet` | Same, `add-field`. | Same. |
| `edit-field is not supported on packed-layout objects yet` | Same, `edit-field`. | Same. |
| `backup has no layout metadata (object.json); restore refused` | Restore of a backup without `object.json`. | Restore with the version that made it, then migrate. |
| `backup layout (<x>) does not match the live object's layout (<y>); restore refused …` | Backup layout differs from the live object (e.g. pre-2026.11.1 backup into a migrated object). | Not restorable across layouts. |
```
4. `docs/concepts/typed-records.md` — two edits, each anchor exactly once.
(i) the line
```markdown
The fixed-field description below is the currently public legacy format.
```
→ replace its first sentence so the line begins `The fixed-field description below is the legacy format (objects not yet migrated by 2026.11.1).` followed by the unchanged remainder of that line.
(ii) anchor `## On-disk encoding` → insert **before** it:
```markdown
## Layouts (2026.11.1)

Every object has one layout, recorded as the `layout=packed` token on its `schema.conf` line. **Packed** records are variable length (segment flag `SREC_FLAG_PACKED`): a descriptor directory followed by exact payloads; `varchar` stores only its content, `numeric` is an exact decimal bounded by its declared precision/scale. **Legacy** records (fixed slots) exist only in a root that 2026.11.1 has not yet migrated; the first start converts every object automatically (see the [2026.11.1 release notes](../release-notes/2026.11.1.md)). A malformed layout token fails the object's load; it never falls back to legacy.

```
5. `docs/query-protocol/schema-mutations.md` — four edits, each anchor exactly once.
(i) anchor `## add-field\n\nAppend new fields to an existing object.` → replace with `## add-field\n\n> **Not supported on packed-layout objects in this release** (every migrated or newly created object): the call returns an error. Follow-up R5b.\n\nAppend new fields to an existing object.`
(ii) anchor `## edit-field\n\nEdit one or more existing fields in place` → replace with `## edit-field\n\n> **Not supported on packed-layout objects in this release**: the call returns an error. Follow-up R5b.\n\nEdit one or more existing fields in place`
(iii) anchor (a table row, exactly once)
```markdown
| `{"mode":"vacuum","compact":true}` | Full rebuild. Drops tombstoned fields, shrinks `slot_size`. Indexes preserved. |
```
→ replace with
```markdown
| `{"mode":"vacuum","compact":true}` | Full rebuild. Drops tombstoned fields, shrinks `slot_size`. Indexes preserved. **Not supported on packed-layout objects in this release** (error; also with `splits`). |
```
(iv) anchor `## create-object\n\nCreate a new typed object. See [Quick start](../getting-started/quickstart.md) for a full example.` → append after that sentence the paragraph: `\n\n\`declaration_layout\` (optional) names the grammar of the \`fields\` strings; the only accepted value is \`"packed"\` (the default). Objects are always created in the packed layout: \`numeric:N\` means scale 0, bare \`varchar\` / \`numeric\` are accepted, \`varchar:N\` is limited to 4 194 304.`
6. `AGENTS.md`: in "Storage model", Segment-files bullet — anchor the exact phrase `+ key bytes + value bytes, padded to \`slot_size\`.` (once) → replace with `+ key bytes + value bytes (packed layout: the record's actual length; legacy objects pad to \`slot_size\`).`
7. Re-run the Part A grep for stale version strings; none may remain outside fixtures.

## Part B — definition of done

- [ ] Fresh `./build/bin/shard-db-test run-all` passes **before Task 11** (paste the tail after each of Tasks 5–10), and after Task 11 the failures (if any) are listed in `docs/plans/2026-11-02-r5-flip-triage.md` and **handed back unresolved** — Task 11/12 are not "done" until the human has triaged.
- [ ] Zero build warnings; every RED and revert-proof pasted into `docs/plans/2026-11-02-r5-evidence.md`.
- [ ] No leftover `TODO`/`FIXME`/debug prints; `git diff --check` clean; only the files named in Tasks 5–12 changed (plus the test additions in `test_layout_manifest.c`, the evidence/triage files, and the docs listed).
- [ ] Uncommitted; blind-reviewer pass required (diff touches an on-disk format, version gating, crash recovery and startup ordering).
- [ ] No sanitizers, no benches (R6 gate). Note for R6: `layout_migrate_object` and the new startup path run with all locks single-threaded at startup, but the sanitizer gate must still cover the migration cases.

## Open items handed to the human

**D9 and D10 are CONFIRMED** (D9: staging only — R5b restores the three operations before anything is released; D10: legacy create is `TEST_BUILD`-only). Remaining notes:

3. **Release behaviour change to document in R6:** for *new* objects `numeric:N` means scale 0 (it meant scale 2); migration preserves existing meaning by rewriting to `numeric:N,2`.
4. **Task 11 will likely produce a large triage list** (every existing test now runs on packed objects). That list, not this plan, determines how much R5 work remains.
