# Finding 8 — `vacuum` and `recount` must reject missing, unopenable, and unreadable objects

Source: Finding 8 in `docs/plans/2026-07-16-storage-durability-and-recovery-findings.md`.
Third in the agreed order (7 → 4 → **8** → rest).

## Corrected root cause

There are three related failure modes, and they must not be conflated:

1. `load_schema(db_root, object)` returns a zeroed `Schema` (`splits == 0`)
   when the object is absent and when it is a legacy-v1 object this binary
   refuses to load.
2. A valid schema can load while `slotcask_registry_get(...)` still returns
   `NULL` because the object cannot be opened (bad permissions, missing or
   corrupt storage files, allocation failure, or a full registry).
3. `cmd_recount` can obtain a valid `SlotcaskDb` and then fail to read one of
   its kf headers. `slotcask_sum_kf_totals()` currently skips those failures
   and returns success with a partial total, despite its public declaration
   promising a nonzero return when any shard fails.

The original Finding 8 analysis is correct for `cmd_recount` on a missing
schema: it passes the zeroed schema to `slotcask_registry_get`, gets `NULL`,
leaves both totals at zero, and prints `{"count":0}`.

The original analysis is **not** correct for `cmd_vacuum` on a missing
schema. A zeroed schema has `streams == 0`, while
`slotcask_streams_for_nproc()` always returns a positive value (fallback 4,
otherwise 1–16). Therefore `cmd_vacuum` always detects a streams mismatch,
calls `rebuild_object`, and that function already returns
`{"error":"Object [<name>] not found"}`. The new schema guard is still
worth adding because it:

- validates at the command boundary instead of entering a rebuild with
  invalid input;
- makes `vacuum`, `recount`, and `rebuild-kf` use the same stable
  `{"error":"object not found"}` response;
- avoids a redundant second schema load in `rebuild_object`.

The real fake-success bug in `cmd_vacuum` occurs when the schema is valid but
`slotcask_registry_get` fails on the light path. The current `if (sdb)` guard
only skips the work; it does not handle the failure, and the function still
prints `{"status":"vacuumed","cleaned":0}`. `cmd_recount` has the same
problem and prints `{"count":0}`.

`cmd_rebuild_kf` already demonstrates both required command-boundary checks:

```c
Schema sch = load_schema(db_root, object);
if (!sch.splits) { OUT("{\"error\":\"object not found\"}\n"); return 1; }
...
SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
if (!sdb) { OUT("{\"error\":\"object not open\"}\n"); return 1; }
```

## Fix

### 1. `src/db/query_maint.c` — validate schema and registry handles

#### `cmd_vacuum`

Immediately after `load_schema`, add:

```c
if (!sch.splits) {
    OUT("{\"error\":\"object not found\"}\n");
    return 1;
}
```

After constructing `SlotcaskSchemaInfo` and calling
`slotcask_registry_get`, replace the success-only wrapper with an explicit
failure guard:

```c
SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
if (!sdb) {
    OUT("{\"error\":\"object not open\"}\n");
    return 1;
}

int dropped = 0;
(void)slotcask_compact_segs(sdb, &dropped);
(void)slotcask_compact_kf(sdb);
```

Keep `reset_deleted_count`, the success response, and the heavy
`rebuild_object` path otherwise unchanged. The new registry guard applies to
the light path; heavy-path open/rebuild failures remain the responsibility of
`rebuild_object` and `rebuild_object_v2`.

#### `cmd_recount`

Immediately after `load_schema`, add the same missing-schema guard:

```c
if (!sch.splits) {
    OUT("{\"error\":\"object not found\"}\n");
    return 1;
}
```

After `slotcask_registry_get`, add:

```c
if (!sdb) {
    OUT("{\"error\":\"object not open\"}\n");
    return 1;
}
```

Then require the kf-header sum to succeed before emitting a count:

```c
uint64_t total_hdr = 0, deleted_hdr = 0;
if (slotcask_sum_kf_totals(sdb, &total_hdr, &deleted_hdr) != 0) {
    OUT("{\"error\":\"recount failed\"}\n");
    return 1;
}
```

Only compute and print `live` after all headers were read successfully.
Never expose the partial totals produced before an I/O failure.

### 2. `src/db/slotcask.c` — honor `slotcask_sum_kf_totals`'s contract

The declaration in `src/db/slotcask.h` already says the function returns
nonzero if any shard cannot be read. Make the implementation match it.

Keep the initial output-zeroing and argument validation. Within the shard
loop, treat each of these as an immediate `-1` return:

- `open(kf_path, O_RDONLY)` fails;
- `pread` returns anything other than a complete `SlotcaskKfHeader`;
- the header magic is not `SLOTCASK_KF_MAGIC`.

For example:

```c
int fd = open(kf_path, O_RDONLY);
if (fd < 0) return -1;

SlotcaskKfHeader hdr;
ssize_t n = pread(fd, &hdr, sizeof(hdr), 0);
close(fd);
if (n != (ssize_t)sizeof(hdr)) return -1;
if (hdr.magic != SLOTCASK_KF_MAGIC) return -1;
```

Assign `out_total` and `out_deleted` only after every shard succeeds. This
makes the operation all-or-nothing and prevents callers from accidentally
using partial values on error.

Update the implementation comment if needed so it no longer claims only a
successful return path while silently skipping failures. No public signature
change is required.

### 3. `src/db/storage.c` — propagate the corrected sum failure

Audit every production caller of `slotcask_sum_kf_totals` as part of this
change:

- `cmd_recount` is fixed in Fix step 1 above;
- `rebuild_object_v2` in `query_find.c` already checks for return value `0`
  before using the totals and needs no change;
- `resolve_counts_with_schema` in `storage.c` currently ignores the return
  value and must be corrected.

Replace the unchecked call in `resolve_counts_with_schema` with:

```c
uint64_t total = 0, deleted = 0;
if (slotcask_sum_kf_totals(sdb, &total, &deleted) != 0) {
    LOG_ERROR(LOG_SUB_SLOTCASK,
        "resolve_counts %s/%s: failed to read kf totals",
        eff_root, bare_obj);
    *out_live = 0;
    *out_deleted = 0;
    return -1;
}
```

Then publish the totals and return `0` as before. The scalar convenience
wrappers (`get_live_count`, `get_deleted_count`, and their full-width
siblings) have no error channel and currently ignore `resolve_counts`'s
return value; leave their API unchanged in this plan. They will receive the
explicit zeroed outputs and log the underlying failure, which is fail-closed
for sizing and background-maintenance decisions. `cmd_recount` does not use
those wrappers and therefore returns its explicit wire error instead of a
zero.

The remaining references in `test_slotcask_api.c` are success-path tests,
not production callers, and require no behavior change.

## Task 1 — regression tests (test-first)

Add `src/test/cases/test_vacuum_recount_validation.c`. Use the existing
single-daemon `TestEnv`/`TestClient` pattern and the proven permission-failure
setup in `test_count_object_not_open_run` from
`src/test/cases/test_get_fields.c`.

Register these four cases from the new file:

### 1a. Missing schema at the command boundary and real-empty TCP control

The JSON and NQL dispatchers already reject a missing `fields.conf` before
calling either maintenance command, using their generic
`Object [ghost] not found. Use create-object first.` response. Cover both
layers rather than changing that established dispatcher behavior.

Register `test-vacuum-recount-command-boundary-missing-object` as an
in-process test. Capture `g_out` with `open_memstream`, call the exported
`cmd_vacuum` and `cmd_recount` functions directly against a nonexistent
`ghost`, and assert both return `1` with the exact
`"error":"object not found"` maintenance-command response. Also assert
recount publishes no `"count"` key.

Before the fix, command-boundary vacuum errors through `rebuild_object` as
`{"error":"Object [ghost] not found"}`, while command-boundary recount
returns `{"count":0}`. This gives the required red proof for both guards.

Register the TCP control as `test-vacuum-recount-missing-object`:

1. Start a daemon and add the `default` directory.
2. Do not create `ghost`.
3. Request `vacuum` for `ghost` and assert an error response and no
   `"status":"vacuumed"`.
4. Request `recount` for `ghost` and assert an error response and no
   `"count"` key.
5. Create `real_empty` with a valid v2 schema and zero records.
6. Assert vacuum succeeds with `"status":"vacuumed"` and recount succeeds
   with `"count":0`.

The TCP missing-object assertions pass before and after the fix because they
verify the dispatcher's existing behavior; the real-empty controls prove the
new command guards do not over-trigger. The in-process case above is the red
and green proof for the newly-added command-boundary guards.

### 1b. Valid schema but registry open failure

Register as `test-vacuum-recount-object-not-open`.

This is a permission-injection test. If `geteuid() == 0`, emit a TAP skip
diagnostic and return success, matching the established test in
`test_get_fields.c`; root bypasses the permission failure being tested.

For a non-root run:

1. Start a daemon, add `default`, create object `maint_not_open`, and insert
   one record.
2. Save `env.db_root` and `env.port` into independent local variables.
3. Close the client and call `test_env_stop_keep(&env)` so the data remains
   but the daemon's process-local registry is discarded.
4. `chmod(.../default/maint_not_open/data/kf/000.kf, 0)` and assert it
   succeeds.
5. Restart a fresh daemon with `test_env_start_at` using the saved root and
   port, then reconnect.
6. Request light-path `vacuum` with no compact/splits flags. Assert the exact
   `"error":"object not open"` response and absence of
   `"status":"vacuumed"`.
7. Request `recount`. Assert the same exact open error and absence of a
   `"count"` key.
8. Restore the kf mode before stopping/cleaning the environment, including
   every early-failure cleanup branch after the chmod.

Stopping and restarting is required: create/insert already opens and caches
the registry handle, so chmod alone would not make
`slotcask_registry_get` reopen the object. The fresh daemon makes the first
maintenance request exercise the actual open failure.

Before the fix, with schema streams matching the same host after restart,
vacuum takes the light path and reports fake success; recount reports a fake
zero. After the fix, both report `object not open`.

### 1c. Recount kf-header read failure after a handle is cached

Register as `test-recount-kf-header-read-failure`.

This is also skipped with a TAP diagnostic when running as root.

For a non-root run:

1. Start a daemon, add `default`, create `recount_read_fail`, and insert one
   record. The insert ensures the daemon has a live cached `SlotcaskDb`.
2. Run a control recount and assert it returns `"count":1`.
3. Without stopping the daemon, chmod shard `data/kf/000.kf` to mode `0`.
4. Run recount again. Assert `"error":"recount failed"` and assert no
   `"count"` key.
5. Restore the mode before all cleanup paths.

The daemon must remain running for this case: the goal is to prove that a
valid cached registry handle does not make a later per-shard `open`/`pread`
failure look like a successful partial count. The record does not need to
hash to shard 0; before the fix, any unreadable shard is silently skipped and
the command emits some count instead of an error.

### Test implementation safety requirements

- Include `<limits.h>`, `<sys/stat.h>`, and `<unistd.h>` for `PATH_MAX`,
  `chmod`, and `geteuid`.
- Assert every `tc_request` used to establish test state succeeds before
  relying on its response.
- Use `SAFE_STRSTR` for negative response assertions.
- Do not use `ASSERT_TRUE(!resp || ...)` for success: a `NULL` response is a
  failure, not success. Assert `resp != NULL` and then inspect it.
- Preserve the kf file's original mode from `stat` and restore that mode,
  rather than assuming `0644`.
- After chmod succeeds, structure cleanup so permissions are restored even
  if reconnect or daemon restart fails.

### Build registration

In `build.sh`, add:

```text
    src/test/cases/test_vacuum_recount_validation.c \
```

immediately after `src/test/cases/test_get_fields.c \` when Finding 7 is
already present. If that line is absent, place it after
`src/test/cases/test_count_varchar_field.c \`. List order has no semantic
effect.

### Prove the tests fail first

Before changing production code:

1. Add and register all four tests.
2. Build with `SKIP_TESTS=1 ./build.sh`.
3. Run each new case separately and paste its actual output:

```text
./build/bin/shard-db-test run test-vacuum-recount-command-boundary-missing-object
./build/bin/shard-db-test run test-vacuum-recount-missing-object
./build/bin/shard-db-test run test-vacuum-recount-object-not-open
./build/bin/shard-db-test run test-recount-kf-header-read-failure
```

Expected pre-fix failures:

- command-boundary vacuum fails the exact normalized-message assertion;
- command-boundary recount fails because it returns `{"count":0}`;
- TCP missing-object and real-empty controls already pass;
- registry-open vacuum/recount fail because they return fake success/zero;
- cached-handle recount fails because it emits a partial count rather than
  `recount failed`.

On root, the two permission-injection tests legitimately skip; the
command-boundary missing-object test must still demonstrate red before the
fix.

Then apply all production-code tasks, rebuild, rerun all four cases, and
paste the passing output.

## Task 2 — build and full verification

1. `SKIP_TESTS=1 ./build.sh` — zero new warnings.
2. Run all three targeted tests individually — green or documented root-only
   permission skips as specified above.
3. `./build/bin/shard-db-test run-all` — full suite green.

Pay particular attention to:

- `test_auto_reshard.c` and `test_warmup_vacuum_race.c`, which exercise the
  heavy vacuum/rebuild path;
- existing vacuum tests on valid objects, which exercise the light path;
- `test_rebuild_kf.c`, which establishes the chosen maintenance-command
  error conventions;
- count/recount and crash-safety tests, because
  `slotcask_sum_kf_totals` now reports previously-suppressed shard failures;
- auto-vacuum and auto-reshard tests, because their scalar count helpers now
  receive logged, fail-closed zero totals if a kf-header read fails.

## Edge cases and invariants

- A real empty object remains valid and returns genuine vacuum/recount zeros.
- Missing and refused legacy-v1 schemas both return
  `{"error":"object not found"}`, matching `cmd_rebuild_kf`.
- A schema-valid object whose registry handle cannot open returns
  `{"error":"object not open"}`; an `if (sdb)` wrapper is not error
  handling.
- A cached object with any unreadable, short, or corrupt kf header returns
  `{"error":"recount failed"}` and never exposes a partial count.
- Vacuum's compact/splits/streams heavy path remains unchanged after the
  early schema guard.
- Both commands return `1` on every newly-handled error and `0` only after
  emitting success.
- No on-disk format or lock ordering changes. The kf-sum change is read-only
  and fails closed before publishing totals.

## Execution rules (embedded, per CORE-PROCESS)

- Branch off `main`. Work stays uncommitted per this repo's standing
  execution-mode exception.
- Order: tests and red proof → production fixes → targeted green proof → full
  suite.
- Build with `SKIP_TESTS=1 ./build.sh`; test with
  `./build/bin/shard-db-test run[-all]`.
- If a quoted production-code anchor is absent, write `PLAN_NOTES.md`
  describing the mismatch and halt. The anticipated `build.sh` insertion
  variance described above is not a halt condition.
- If a decision is not covered here, stop and ask; do not improvise.
