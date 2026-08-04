# Plan: make single partial updates atomic

## Root cause

'cmd_update_v2' reads OLD with 'slotcask_get', builds NEW outside the
kf-shard write lock, then calls 'slotcask_upsert_with_hooks'. A concurrent
partial update can publish a replacement based on an older snapshot and erase
fields changed by the intervening update. The existing 'check' callback makes
the CAS criteria atomic, but it does not rebuild the replacement from the OLD
record that the upsert path reads while holding the lock.

## Required invariants

- The ordinary full-value upsert path is byte-for-byte behaviorally unchanged.
- The new callback is opt-in. Existing callers that leave it NULL continue to
  pass their supplied 'value' and 'vlen' through the existing paths.
- A partial update's callback runs only after 'if_not_exists',
  'require_existing', and 'check' have accepted the current OLD record, while
  the kf-shard write lock is still held.
- The callback runs before segment reservation, segment writing, and
  'pre_commit'. If allocation, parsing, validation, or output sizing fails,
  no segment slot is reserved, no index hook runs, and the old kf entry remains
  live.
- The callback's output length is non-zero only when it sets
  '*out_vlen'; '*out_vlen <= out_capacity' is mandatory. The upsert path checks
  the final record-header/key/value size after the callback and before
  reservation.
- The callback receives the current OLD bytes, not the caller's earlier
  snapshot. It copies every untouched field, applies only fields present in the
  request, preserves removed fields, and applies existing 'auto_update'
  behavior exactly once per successful update.
- A missing key still returns 'Not found'; a failed 'if' still returns
  'condition_not_met' with the current record; malformed field input and
  varchar overflow still reject the complete update.
- 'pre_commit' continues to receive the same OLD and the callback-produced NEW
  bytes, so secondary-index diffs and bitmap index updates remain unchanged.
- The deterministic test pause is compiled only for TEST_BUILD, is disabled
  when no hook is installed, is one-shot per test phase, and is never read from
  production configuration.
- The cross-process test control channel is compiled and started only by the
  TEST_BUILD daemon. The production `shard-db` binary receives no control FD,
  starts no control thread, and performs no test-channel I/O.

## Call-site and consumer inventory

Before changing the interface, search and account for every consumer of
'SlotcaskUpsertOpts' and 'slotcask_upsert_with_hooks':

- 'src/db/slotcask.h': the callback typedef insertion, the complete
  'SlotcaskUpsertOpts' definition at the anchor 'typedef struct {', and the
  declarations at 'int slotcask_upsert_with_hooks(' and
  'int slotcask_insert_with_hooks('.
- 'src/db/slotcask.c': the definition at
  'int slotcask_upsert_with_hooks(', the forward declaration at
  'static int upsert_slow_path(', the implementation at the exact comment
  '/* Original lookup-first path, kept for require_existing / check_needs_old',
  and the insert-only implementation at
  'int slotcask_insert_with_hooks('. The new fields must not alter the
  existing insert-only behavior.
- 'src/db/storage.c': the insert caller at
  'rc = slotcask_upsert_with_hooks(sdb, -1, key, klen,' near the v2 insert
  path, and the single-update caller at the same exact anchor in
  'cmd_update_v2'.
- 'src/db/query_bulk.c': the bulk-update implementations do not call the
  single-record function directly; their comments and separate bulk primitive
  must remain unchanged. Confirm this with a code search rather than changing
  them as part of this plan.
- Tests using the options or function directly:
  'src/test/cases/test_durability_sync_failures.c',
  'src/test/cases/test_durability_ordering.c', and
  'src/test/cases/test_slotcask_cas.c'. Their existing designated initializers
  and NULL arguments must compile and retain their current assertions.
- Test-daemon harness consumers: the daemon source list and test-runner link
  list in 'build.sh', plus both binary-selection statements in
  'src/test/fixtures.c': the one in 'test_env_start_ex' following the exact
  comment '/* Find shard-db: prefer ./build/bin/, fall back to ./shard-db.'
  and the one in 'test_env_start_at' at its exact 'char logs_dir[400];'
  statement. The TEST_BUILD daemon must be a test artifact
  only; the normal 'shard-db' production binary and its build flags remain
  unchanged.
- No external installed-library consumer is known: the repository builds a
  single static binary and the search above covers the in-tree public header
  consumers. If the executor finds another consumer, stop and update this
  plan before changing it.
- Cross-process test-control consumers: `src/test/fixtures.h`'s complete
  `TestEnv` definition and the complete start/stop implementations in
  `src/test/fixtures.c`; `src/db/main.c`'s lifecycle dispatch for the exact
  `if (strcmp(cmd, "start") == 0 || strcmp(cmd, "server") == 0 ||` anchor;
  and the new TEST_BUILD-only `src/db/test_control.h` /
  `src/db/test_control.c` module. The test case must use only the fixture
  helpers below; it must not duplicate the control protocol or call a
  daemon-process function directly.

## Tasks

### 1. Add the deterministic TCP regression test first

Create 'src/test/cases/test_update_partial_concurrent.c' and register it as
'test-update-partial-concurrent'. Use the existing 'TestEnv', 'TestClient',
'tc_request', and pthread/condition-variable patterns from
'src/test/cases/test_btree_bulk_merge_delete_race.c'.

Add a TEST_BUILD daemon target before the exact build.sh comment
'# shard-db-test — TAP-style C test runner'. Compile the same daemon source
list used by the production 'shard-db' target with '-DTEST_BUILD', producing
'build/bin/shard-db-test-server'. Do not replace or relabel the production
binary. At both binary-selection sites—'test_env_start_ex' immediately after
the comment '/* Find shard-db: prefer ./build/bin/, fall back to
./shard-db.' and, within 'test_env_start_at', at its exact 'char
logs_dir[400];' statement—make the fixture prefer
'./build/bin/shard-db-test-server' and retain
the production path as a fallback for builds that do not include the test
target. This ensures the TCP case reaches the TEST_BUILD hook while all normal
builds remain unchanged. The side effect is that every TCP test using either
shared fixture runs against the TEST_BUILD daemon; the review must confirm
that this changes no non-test behavior and that direct-call TEST_BUILD
coverage remains valid.

The daemon/test-runner transport is an inherited anonymous Unix socketpair,
not TCP, a marker file, a fixed path, an environment variable, or a sleep.
Extend `TestEnv` at the complete existing `typedef struct {` block in
`src/test/fixtures.h` with this field:

~~~c
    int test_control_fd;    /* parent side; -1 when the selected daemon is production */
~~~

At both fixture spawn implementations—`test_env_start_ex` immediately after
the exact binary-resolution block ending at `if (!realpath(binary_rel,
binary_abs)) {`, and `test_env_start_at` immediately after its exact
`if (!realpath(binary_rel, binary_abs)) return -1;`—create an
`AF_UNIX/SOCK_STREAM` `socketpair` only when the selected binary is
`shard-db-test-server`. Keep the parent endpoint in `env->test_control_fd`,
close it in every start-failure path and in `test_env_stop`,
`test_env_stop_keep`, and `test_env_kill`, and close the child endpoint in the
parent after `fork`. The child must retain its endpoint across `exec` and
invoke the test server as `server --test-control-fd <fd>`. Do not put the FD
number or any control state in `db.env`; the FD is per-`TestEnv` and each
fixture has its own channel. The production-binary fallback must pass no
test-control argument and leave `test_control_fd == -1`.
Set `env->test_control_fd = -1` as the first state initialization in both
start functions so existing `TestEnv env = {0}` callers never cause cleanup
to close descriptor 0 on an early return.

Add these complete fixture-interface declarations immediately after the
existing `test_env_start_at` declaration in `src/test/fixtures.h`:

~~~c
/* Deterministic TEST_BUILD daemon seam. Each operation returns 0 on a
   complete protocol exchange and -1 on EOF, malformed framing, or I/O error.
   `wait` blocks until the daemon-side hook reports the requested phase. */
int test_env_test_hook_install(TestEnv *env);
int test_env_test_hook_wait(TestEnv *env, int *out_under_kf_wrlock);
int test_env_test_hook_release(TestEnv *env);
int test_env_test_hook_clear(TestEnv *env);
~~~

Implement these four helpers in `src/test/fixtures.c` as the only runner-side
transport adapter. Use one fixed-size message for every write/read, handle
short I/O and `EINTR`, and reject an unexpected message kind or phase. The
messages are private to the TEST_BUILD fixture and have this complete layout:

~~~c
typedef struct {
    uint32_t kind;   /* INSTALL=1, RELEASE=2, CLEAR=3, ACK=4, REACHED=5 */
    int32_t  phase;  /* REACHED: 0=stale snapshot, 1=under kf wrlock; else 0 */
} TestHookMessage;
~~~

`test_env_test_hook_install` sends `INSTALL` and requires `ACK`; `wait`
requires `REACHED` and returns its phase; `release` sends `RELEASE` and
requires `ACK`; `clear` sends `CLEAR` and requires `ACK`. The helpers must
not retry, poll, sleep, use a fixed pathname, or mutate process-wide
environment state. A failed exchange is a test failure and the caller still
performs release/clear/close cleanup before joining request threads.

Add `src/db/test_control.h` and `src/db/test_control.c` to the TEST_BUILD
daemon target only. `test_control.h` must expose exactly these functions:

~~~c
#ifndef SHARD_DB_TEST_CONTROL_H
#define SHARD_DB_TEST_CONTROL_H

#ifdef TEST_BUILD
int shard_db_test_control_start(int fd);
void shard_db_test_control_stop(void);
#endif

#endif
~~~

`test_control.c` owns one daemon-local `TestControl` state containing the
control FD, a mutex, a condition variable, `running`, `waiting_for_release`,
and `release` flags. Its control thread reads the same `TestHookMessage`
layout (the protocol constants may be private duplicated constants, not a
shared production header). On `INSTALL` it calls
`slotcask_test_set_after_old_hook(test_control_after_old, &state)` and sends
`ACK`. On `RELEASE` it sets `release=1`, broadcasts the condition variable,
and sends `ACK`. On `CLEAR` it calls
`slotcask_test_set_after_old_hook(NULL, NULL)`, sets `release=1`, broadcasts,
and sends `ACK`. EOF or a malformed message stops the control thread. The
complete callback must be equivalent to:

~~~c
typedef struct {
    int              fd;
    pthread_t        thread;
    pthread_mutex_t  lock;
    pthread_cond_t   cond;
    int              running;
    int              waiting_for_release;
    int              release;
} TestControl;

static void test_control_after_old(int under_kf_wrlock, void *ctx_ptr) {
    TestControl *c = ctx_ptr;
    TestHookMessage reached = { .kind = TEST_HOOK_REACHED,
                                .phase = under_kf_wrlock };
    pthread_mutex_lock(&c->lock);
    c->waiting_for_release = 1;
    c->release = 0;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->lock);

    if (write_full(c->fd, &reached, sizeof(reached)) != 0) {
        pthread_mutex_lock(&c->lock);
        c->release = 1;
        pthread_cond_broadcast(&c->cond);
        c->waiting_for_release = 0;
        pthread_mutex_unlock(&c->lock);
        return;
    }

    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}
~~~

The implementation must clear the hook, set `running=0`, set `release=1`,
broadcast, call `shutdown(fd, SHUT_RDWR)` to wake a control thread blocked in
`read`, join that thread, then close the FD and destroy the mutex/condition
variable. It must never wait for a runner message while holding the kf-shard
lock except for the intentional callback pause itself; the control thread
only installs/clears the hook and signals the callback. `start` and `stop`
must be idempotent for `fd < 0` and must not exist in the production build.
The callback's one fixed-size `write` is IPC on the inherited Unix socketpair,
not TCP/network I/O; the production partial-update callback remains free of
I/O and database re-entry because this adapter is compiled only into the
TEST_BUILD daemon.

At the exact lifecycle dispatch anchor in `src/db/main.c`, parse only the
TEST_BUILD-only argument pair `server --test-control-fd <fd>`. Start the
control module immediately before `cmd_server(db_root, 0)`, stop it on every
return path after `cmd_server` completes, and reject malformed/extra control
arguments. The normal `shard-db server` invocation and every non-TEST_BUILD
build must retain their current argument and startup behavior. The target
recipe must compile `test_control.c` only into `shard-db-test-server`; it must
not add it to the production `shard-db`, embedded library, CLI, benchmark, or
test-runner link lists.

Add this TEST_BUILD-only hook interface immediately before the exact existing
anchor '#ifdef TEST_BUILD\nvoid segcache_test_force_identity_mismatches(int count);'
in 'src/db/slotcask.h', with the complete declaration:

~~~c
#ifdef TEST_BUILD
typedef void (*slotcask_test_after_old_fn)(int under_kf_wrlock, void *ctx);
void slotcask_test_set_after_old_hook(slotcask_test_after_old_fn fn, void *ctx);
void slotcask_test_after_old(int under_kf_wrlock);
#endif
~~~

Implement the matching setter and one-shot invocation in 'src/db/slotcask.c'
using a mutex-protected function/context pair, following the existing
'btree_test_set_after_extract_hook' pattern. The hook must be called once per
installed hook: invocation must atomically take and clear the stored function
and context before calling it, and a NULL function must be a no-op. The test
also calls the setter with NULL during cleanup. The runner never calls this
setter directly: `test_control.c` calls it inside the daemon process after an
`INSTALL` message. The hook remains TEST_BUILD-only and has no production
transport or callback path.
For the pre-fix seam, invoke the hook with 'under_kf_wrlock == 0' immediately
after the successful read at the exact storage.c anchor
'if (slotcask_get(sdb, key, klen, &old_val, &old_vlen) != 0) {'. This is the
stale snapshot pause, before the old code builds NEW. In the fixed path, invoke
the same hook with 'under_kf_wrlock == 1' at the start of the complete
'v2_update_new_from_old' callback, after it has received OLD. That callback is
called by 'upsert_slow_path' while the kf write lock is held. The hook argument
must distinguish those two locations so the test never uses a timing guess.

The test fixture must:

1. Start an isolated daemon, create one object with two independent mutable
   fields, and seed one record with both fields set.
2. Start update A through the TCP JSON seam, pause after its OLD snapshot is
   available, then start update B for the other field.
3. In the pre-fix phase ('under_kf_wrlock == 0'), wait for B to complete before
   releasing A. Assert the base implementation loses B's field. This is the
   required failing proof; do not accept a pass caused by scheduling.
4. Re-run the same scenario after the atomic implementation. When the hook
   reports 'under_kf_wrlock == 1', release A without waiting for B, join both
   request threads, and assert that both field changes survive in the final
   'get' response.
5. Assert both update responses are successful, clear the hook on every exit
   path, join every thread, and stop the isolated environment.

The test must install and coordinate the seam through the fixture adapter:
`test_env_test_hook_install(&env)`, `test_env_test_hook_wait(&env, &phase)`,
`test_env_test_hook_release(&env)`, and
`test_env_test_hook_clear(&env)`. It must never assume that the runner's
`slotcask_test_set_after_old_hook` symbol controls the child daemon; those are
different processes. In the pre-fix phase, wait for phase `0`, run B to
completion, then send `RELEASE`. In the fixed phase, wait for phase `1`, send
`RELEASE` immediately, then join both requests. Every error path must release
if a `REACHED` message was observed, clear the hook, close the fixture's
control FD through the fixture lifecycle, join both request threads, and stop
the daemon.

The executor must first add only the test, TEST_BUILD test-server target,
fixture selection, inherited control transport, and hook seam; build and run
the case against the unchanged production algorithm. Record the expected
failure showing the stale-snapshot field loss. Then implement the remaining
tasks, rerun the same case, and record the passing output. Do not replace the
deterministic synchronization with sleeps, retries, fixed ports, fixed paths,
or process-global environment state.

### 2. Add the opt-in NEW-from-OLD upsert interface

At the exact header anchor 'typedef struct {' immediately following
'typedef int (*slotcask_check_fn)(const SlotcaskOldRecord *old, void *ctx);',
insert this complete callback type:

~~~c
typedef int (*slotcask_new_from_old_fn)(const SlotcaskOldRecord *old,
                                         uint8_t *out_value,
                                         size_t out_capacity,
                                         size_t *out_vlen,
                                         void *ctx);
~~~

Replace the existing 'SlotcaskUpsertOpts' block at the exact anchor
'typedef struct {' / '} SlotcaskUpsertOpts;' with the same complete block plus
the two new fields shown here; preserve every existing field and comment:

~~~c
typedef struct {
    int                       if_not_exists;
    int                       require_existing;
    int                       check_needs_old;
    slotcask_check_fn         check;
    void                     *check_ctx;
    slotcask_new_from_old_fn  new_from_old;
    void                     *new_from_old_ctx;
    slotcask_pre_commit_fn    pre_commit;
    void                     *pre_commit_ctx;
    slotcask_prepare_commit_fn prepare_commit;
    slotcask_apply_commit_fn   apply_commit;
    slotcask_abort_commit_fn   abort_commit;
    int                      *out_kf_shard;
    uint32_t                 *out_kf_slot;
    int                       has_indexed_fields;
    int                      *out_durability_degraded;
} SlotcaskUpsertOpts;
~~~

The executor must retain the existing explanatory comments for each field;
the compact block above is the complete field order and type contract, not a
request to delete documentation.

Update the header declaration and the source forward declaration only as
needed for the new option; 'slotcask_upsert_with_hooks' keeps its existing
function signature. No caller passes the callback through the public function
arguments.

### 3. Make the slow upsert path build NEW under the lock

At the exact source anchor
'/* Original lookup-first path, kept for require_existing / check_needs_old',
modify the complete 'upsert_slow_path' implementation as follows:

~~~c
/* After the existing if_not_exists/require_existing/check gates have accepted
   old_ptr, and before any pool_try_pop_* or append_reserve_* call: */
size_t write_vlen = vlen;
const uint8_t *write_value = value;
uint8_t *callback_value = NULL;

if (opts->new_from_old) {
    if (!found || !old_ptr) goto new_from_old_failed;
    if ((size_t)db->slot_size < (size_t)24 + klen)
        goto new_from_old_failed;

    size_t out_capacity = (size_t)db->slot_size - 24 - klen;
    callback_value = malloc(out_capacity ? out_capacity : 1);
    if (!callback_value) goto new_from_old_failed;

    write_vlen = 0;
    if (opts->new_from_old(old_ptr, callback_value, out_capacity,
                           &write_vlen, opts->new_from_old_ctx) != 0 ||
        write_vlen > out_capacity) {
        goto new_from_old_failed;
    }
    if (db->format == SLOTCASK_FORMAT_VARIABLE && db->trim_fn)
        write_vlen = db->trim_fn(callback_value, write_vlen, db->trim_ctx);
    if ((size_t)24 + klen + write_vlen > (size_t)db->slot_size)
        goto new_from_old_failed;
    write_value = callback_value;
}

/* Use write_value/write_vlen for every reservation, segment write, and
   pre_commit argument below. On every failure after allocation, free
   callback_value before returning; on success, free it after publication. */
~~~

The executor must implement the 'new_from_old_failed' cleanup as a complete
local cleanup path that releases 'kh', frees 'old_buf' and 'callback_value',
and returns '-1' without reserving or tombstoning a segment. Existing later
failure paths must also free 'callback_value'; they must not free the caller's
'value'. For NULL 'new_from_old', preserve the current value/vlen checks and
all existing full-value behavior.

The callback must be invoked after the built-in CAS gates and before segment
reservation, while 'kh' is held, and before 'pre_commit'. Preserve the current
ordering of segment write, index marker/hooks, kf publication, sync, and
cleanup after the callback succeeds.

At the exact dispatch anchor 'if (opts->require_existing ||
opts->check_needs_old || opts->has_indexed_fields)', include
'opts->new_from_old' in the slow-path condition so every callback caller is
forced through the lock-protected implementation. Immediately before the
exact comment '/* ===== FAST PATH =====', add this defensive guard so a future
dispatch edit cannot silently write the callback caller's NULL value:

~~~c
if (opts->new_from_old) {
    errno = EINVAL;
    return -1;
}
~~~

The guard is unreachable with the corrected dispatch condition; it is
intentional fail-closed API behavior for any future fast-path regression.

### 4. Move single-update patch construction into the callback

At the exact source anchor 'typedef struct {' immediately after the comment
'/* ========== PARTIAL UPDATE — v2 (slotcask) helper ==========' in
'src/db/storage.c', extend 'V2UpdateCtx' with only the new callback input
below. Reuse the existing 'TypedSchema *idx_ts' member at storage.c:1105; do
not add a second 'idx_ts' member:

~~~c
    const char       *partial_json;
~~~

Add a complete 'v2_update_new_from_old' function immediately before the exact
anchor 'static int v2_update_check_fn('. Its body is the existing code block
that starts at '/* Build new typed buffer = copy of old, with partial fields
applied. */' and ends after the 'auto_update' loop, moved without semantic
changes, with these callback boundaries:

~~~c
static int v2_update_new_from_old(const SlotcaskOldRecord *old,
                                  uint8_t *out_value,
                                  size_t out_capacity,
                                  size_t *out_vlen,
                                  void *ctx_ptr) {
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
    if (!old || !out_value || !out_vlen || old->vlen > out_capacity) return -1;
#ifdef TEST_BUILD
    slotcask_test_after_old(1);
#endif
    memcpy(out_value, old->value, old->vlen);
    *out_vlen = old->vlen;

    /* Move the complete existing field parsing, malformed-escape rejection,
       varchar-bound validation, encode_field loop, and auto_update loop here.
       Replace old_val/new_buf/old_vlen/partial_json/ts references with
       old->value/out_value/*out_vlen/c->partial_json/c->idx_ts. */
    return 0;
}
~~~

The executor must not leave a prose placeholder in the implementation: the
complete moved code must be present in the resulting function. Every error
path must free all parsed field values before returning non-zero. The callback
must not parse or apply any field outside 'partial_json', and must preserve the
existing response text for malformed escapes and varchar overflow by copying
the existing error into 'V2UpdateCtx.err_buf' before returning non-zero. Since
the callback runs under the kf lock, it must not perform network I/O or call
back into the database.

At the exact anchor 'void *old_val = NULL; size_t old_vlen = 0;', retain the
pre-read only for 'dry_run'; for a normal update remove the stale snapshot,
the old outside-lock allocation, and its associated free. Keep dry-run's
current validation and response behavior unchanged. Before removing that
pre-read, add this TEST_BUILD-only call immediately after its successful
completion, so the first test run can prove the base lost-update ordering:

~~~c
#ifdef TEST_BUILD
slotcask_test_after_old(0);
#endif
~~~

Document and test the only intentional precedence shift: when both the
partial field JSON and the 'if' criteria are invalid, parsing 'if' before the
lock-protected callback means the response may report 'invalid if condition'
first. The existing response text for either error when it occurs alone must
remain unchanged; do not broaden this change to any other validation order.

Within 'cmd_update_v2', at the exact anchor 'V2UpdateCtx ctx = {', initialize
'.partial_json = partial_json'. Within that same update-path function, at its
'SlotcaskUpsertOpts opts = {' initializer (not the insert initializer near
storage.c:1002), add the complete designated fields:

~~~c
        .new_from_old     = v2_update_new_from_old,
        .new_from_old_ctx = &ctx,
~~~

Pass 'NULL, 0' as the 'value, vlen' arguments at the update-path call inside
'cmd_update_v2' at the exact anchor
'rc = slotcask_upsert_with_hooks(sdb, -1, key, klen,' (not the insert call
near storage.c:1035); the upsert callback now
constructs the replacement from the lock-protected OLD record. Keep
'require_existing', 'check', 'pre_commit', index outputs, result handling,
and 'free_criteria' exactly as before.

### 5. Documentation corrections

At the exact anchor
"On success the server bumps `version` via the sequence, and `updated` via `auto_update`."
in 'docs/query-protocol/cas.md', state that 'default=seq(...)' is evaluated
on insert only. Explain that update CAS revisions must be explicitly supplied
or incremented by the caller, or by a separately implemented update policy; the
server does not implicitly run the insert default during an update.

At the exact anchor
"Combined with `{"mode":"update", "if":{"version":42}}` gives you optimistic concurrency control without app-side locking."
in 'docs/concepts/typed-records.md', make the same insert-only/default-versus-
update-CAS distinction without changing the wire protocol example.

### 6. Verification and handoff

Execution must happen on a fresh feature branch based on the default branch,
in task order, and must stop immediately with 'PLAN_NOTES.md' if any quoted
anchor is not found exactly. No commits or pushes are part of this plan.

After the test-first failure proof and implementation:

~~~bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-update-partial-concurrent
./build/bin/shard-db-test run test-slotcask-cas
./build/bin/shard-db-test run test-bulk-update-json
./build/bin/shard-db-test run test-bulk-update-delimited
./build/bin/shard-db-test run-all
~~~

Because this changes a lock-protected shared-state write path, run the local
dynamic-safety gate as well:

~~~bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all --jobs 1
~~~

The handoff must include the base-implementation failing output, the fixed
regression output, targeted test output, full-suite output, sanitizer output,
and the raw uncommitted 'git diff'. Do not run benchmarks. Reviewers must
inspect the raw diff for lock ordering, callback ownership, cleanup on every
failure path, index consistency, and unintended changes to full upsert or
bulk-update semantics.

## Scope

Only single-record partial-update construction, its opt-in slotcask callback,
the deterministic TEST_BUILD regression seam, the regression test, and the two
specified documentation corrections are in scope. Do not change full-record
upsert semantics, bulk-update semantics, on-disk formats, wire response
shapes, commit history, or remote branches.
