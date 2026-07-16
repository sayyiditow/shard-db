# TCP-to-direct-call conversion for planner/topn setup-only test cases

## Goal

Reduce daemon-subprocess-spawn + TCP-connection overhead during parallel
test runs (`./build/bin/shard-db-test run-all --jobs N`) by converting the
test cases whose **setup** currently goes over TCP but whose **assertions**
never touch the wire — they only inspect direct in-process function return
values — to issue that same setup via the existing in-process embedded API
(`src/db/shard_db.h`) instead of spawning a daemon and connecting over TCP.

This is a **speed-motivated conversion**, distinct from and downstream of
the already-completed, correctness-only audit at
`docs/plans/2026-07-15-tcp-vs-direct-call-test-audit.md` (see its findings
at `docs/plans/2026-07-15-tcp-vs-direct-call-test-audit-findings.md`).
That audit found zero correctness gaps and was explicitly scoped as "not a
speed conversion." This plan targets the subset of cases that audit
flagged `TCP NOT REQUIRED BY STATED ASSERTIONS`.

## Corrected candidate list (audit finding was wrong on one row)

The audit findings doc lists `test_a3_trigram_starts_with_executor` (in
`src/test/cases/test_planner_cost_model.c`) as `TCP NOT REQUIRED BY STATED
ASSERTIONS`. This is **incorrect** — re-reading its full body shows it
makes three real `tc_request(..., "mode":"find", ...)` calls and asserts
directly on the wire response content (row count via `strstr` counting,
exact `"[]"` / `"[]\n"` empty-array checks, negative substring checks for
`"Ask HN"` / `"How to"`). It must remain TCP-based and is **excluded**
from this plan's scope. (Likely cause: the audit's per-file boilerplate
"what it asserts" text suggests the executing model templated by
fixture-setup-pattern rather than reading every case's full assertion body
— worth flagging back to the audit's own quality record, but out of scope
to fix retroactively here.)

The corrected candidate list is **21 cases**:

- `src/test/cases/test_planner_cost_model.c` — 20 cases (every
  `TEST_REGISTER` in that file **except**
  `test_a3_trigram_starts_with_executor`).
- `src/test/cases/test_agg_topn_stream.c` — 1 case:
  `test_topn_eligible_truth_table`.

## Background — why this is safe, and the exact mechanism

### The existing embedded API

`src/db/shard_db.h` (already built into `shard-db-test` — `embedded.c` is
directly compiled into the test binary per `build.sh`'s `shard-db-test`
link line) exposes:

```c
typedef struct ShardDb ShardDb;
ShardDb *shard_db_open(const char *db_root);
int shard_db_query(ShardDb *db, const char *json, char **out, size_t *out_len);
void shard_db_free_result(char *out);
void shard_db_close(ShardDb *db);
```

`shard_db_query()` calls the **exact same** `dispatch_json_query()` that
the TCP daemon path calls (`src/db/server.c:2223`), just with `g_out`
redirected to an in-memory `open_memstream` buffer instead of a
socket-backed `FILE*`. Same dispatch code, same correctness guarantees,
zero daemon fork/exec, zero TCP listen/accept/connect, zero wire I/O.

### Why we must NOT call `shard_db_open()` again in a test case

`src/test/test_runner.c`'s `run_case_result()` (called for every
registered case, both in the sequential path and inside the forked child
per case in the parallel `--jobs N` path — see `start_case()` at
`test_runner.c:178-233`, which `fork()`s at line 196 and the child calls
`run_case_result()` directly at line 216) does this **unconditionally
before every test case function runs**:

```c
#ifdef TEST_BUILD
    test_init_process_db();
#endif
```

`test_init_process_db()` (`src/db/embedded.c:136-151`) is:

```c
void test_init_process_db(void) {
    if (g_db) return;
    char tmpdir[] = "/tmp/shard-db-unit-XXXXXX";
    if (!mkdtemp(tmpdir)) return;
    shard_db_open_internal(tmpdir);  /* sets g_db as a side effect */
    static pthread_mutex_t instance_lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&instance_lock);
    if (!g_shard_db_instance) g_shard_db_instance = g_db;
    pthread_mutex_unlock(&instance_lock);
}
```

So by the time **any** test case function body starts running, `g_db` is
already non-NULL, pointing at a minimal `ShardDb` rooted at a fresh, empty
`mkdtemp`'d tmpdir, with caches/mutexes already initialized. Calling the
public `shard_db_open()` a second time in the same process would allocate
and leak an entire second `ShardDb` (open fds, mmaps, caches) while
silently swapping `g_db` to the new, disconnected instance — confirmed by
reading `shard_db_open_internal()`: it unconditionally `calloc`s a new
struct and never frees or reuses whatever `g_db` previously pointed to.

**This is per-process reuse, not per-case reuse — the two execution modes
differ:**

- **Parallel (`--jobs N>1`, the default):** `start_case()`
  (`test_runner.c:178-233`) `fork()`s a fresh child per case (line 196); the
  child calls `run_case_result()` directly (line 216). Each case gets its
  own process, its own `mkdtemp`'d tmpdir, its own single `g_db` — full
  per-case isolation, same as today's TCP path getting a fresh daemon per
  case.
- **Sequential (`--jobs 1`, `run_all_sequential()` at `test_runner.c:96-113`):**
  every registered case in the entire binary runs in **one process**, calling
  `run_case_result()` directly in a loop. `test_init_process_db()` is a
  no-op after the first case (`if (g_db) return;`), so **all cases in this
  mode share one `g_db` and one on-disk tmpdir for the whole suite run** —
  not just the 21 being converted here, every case in the binary.

  On the (unconverted) TCP path, this sharing is invisible because
  `test_env_start()` spawns a brand-new daemon at a fresh `mkdtemp` port
  per case regardless of `--jobs`, and `test_env_stop()` (see its doc
  comment at `src/test/fixtures.h:27-29`) `rm -rf`s that daemon's db_root
  on the way out — TCP-path cases already get full per-case on-disk
  isolation today, in both execution modes.

  Converted cases lose that per-case isolation under `--jobs 1`, since they
  now all write into the one shared process-wide tmpdir. Each of the 21
  cases uses a distinct object name today (confirmed per-case in Tasks
  3-6), so two converted cases never collide on the same object — but a
  case function that returns early without cleanup, or a future case reusing
  an existing object name, would now leak state into later cases in the same
  sequential run in a way the TCP path never allowed. Task 2 adds a
  `tu_pdb_drop_object()` teardown helper (`{"mode":"drop-object",...,
  "if_exists":true}`) and every converted case in Tasks 4-6 calls it,
  per object it created, immediately before `return 0;` — restoring
  per-case on-disk isolation under `--jobs 1` without needing a fresh
  process or tmpdir. Task 7 adds an explicit `--jobs 1` run to verify this.

### Cache-key safety confirmed

Investigated `src/db/config.c`, `storage.c`, `btree.c`, `bitmap.c`,
`slotcask.c`: `load_dirs()` fully resets (`memset` + zero count) before
repopulating — no stale-merge risk. The schema cache, `fcache`, `bt_cache`,
`bm_cache`, and `kfcache` all key on the **full path string** derived from
`db->db_root` (e.g. `"%s:%s", effective_root, object` for the schema
cache), never on a bare object/dir name — so reusing the existing
`g_db`/db_root for a fresh, never-before-used object name (as every one of
these 21 cases already does — each uses a distinct object name) carries no
collision risk. `fcache_init`/`bt_cache_init`/`bm_cache_init`/`slotcask_init`
are all idempotent (`if (already-initialized) return;` guards) — moot here
since we're not calling them again anyway.

### The conversion mechanism

Two new test-only accessor functions (Task 1, in `embedded.c`, following
the file's existing "Test-only helper" convention next to
`test_init_process_db`) expose the already-live instance:

```c
ShardDb *test_get_process_db(void);
const char *test_get_process_db_root(void);
```

A shared test utility (Task 2, in `fixtures.c`/`fixtures.h`, following the
file's existing `tu_` prefix convention for helpers "formerly duplicated
as `static` in 20+ case files") wraps `shard_db_query()` with the same
3-argument call shape as `tc_request()`:

```c
int tu_pdb_request(ShardDb *db, const char *json, char **out_response);
```

This makes every existing `tc_request(tc, json, &resp)` call site
convertible by a **pure rename** (`tc_request` → `tu_pdb_request`, zero
argument changes).

`cm_setup()` itself is **not** rewritten in place — it stays exactly as it
is today, because `test_a3_trigram_starts_with_executor` (the one case
excluded from this plan; see "Corrected candidate list") still calls it
and still needs a `TestClient *` for its `tc_request`/`tc_close` TCP
round-trip. Instead, Task 3 adds a new sibling function, `cm_pdb_setup()`,
with the embedded-equivalent body (`ShardDb *` return type, using
`test_get_process_db()` / `tu_pdb_request()`), and the 19 converted case
functions in Task 4 call `cm_pdb_setup()` instead of `cm_setup()`. The two
functions coexist permanently after this plan lands; there is no follow-up
step that deletes `cm_setup()`.

Every `tc_close(tc); test_env_stop(&env);` teardown line is replaced —
**not deleted outright** — with one or more calls to a new
`tu_pdb_drop_object(ShardDb *db, const char *dir, const char *object)`
helper (Task 2; wraps `{"mode":"drop-object","dir":...,"object":...,
"if_exists":true}`), one call per object the case created. This is
required, not optional cleanup: see "Process isolation: sequential vs
parallel" above — under `--jobs 1` every converted case shares one
process-wide `g_db` and tmpdir for the whole suite run, so leaving a
case's object behind would leak into every later case in the same
sequential run. `drop-object` bypasses the `fields.conf` existence
pre-check and is idempotent with `if_exists:true`, so it is safe to call
even if an earlier assertion in the same case already failed and returned
early — though the plan does not currently add teardown to early-return
paths (`if (!tc) return 1;`), since those exit before any object is
created.

`free(resp)` calls on a buffer that came from `tu_pdb_request` become
`shard_db_free_result(resp)` — the two are equivalent today only because
`shard_db_query`'s buffer happens to come from `open_memstream` (glibc
malloc-backed), but `shard_db_free_result()` is the *documented* contract
in `shard_db.h` and must be used, not relied-upon `free()`.

### Invariant: config knobs

`shard_db_open_internal()` applies `db_defaults_set()` defaults (e.g.
`FCACHE_MAX`, `INDEX_BUILD_BUDGET_MB`, `THREADS`, and
`random_seq_ratio = 8`) and **does** then `fopen("db.env", "r")` relative
to the test binary's current working directory (`embedded.c:78` onward),
overriding those defaults with whatever the CWD's `db.env` sets — it is
not defaults-only. This repo's root `db.env` (the file that's on disk when
`./build/bin/shard-db-test` is invoked from the repo root, as the
Definition of Done and Task 7 require) explicitly sets
`RANDOM_SEQ_COST_RATIO=8`, which matches the compiled default, so the
embedded path's effective ratio is 8 either way. This differs from how the
**TCP** path arrives at the same number: `test_env_start_at()`
(`fixtures.c:195-249`) writes a fresh, isolated `db.env` for the spawned
daemon that does **not** set `RANDOM_SEQ_COST_RATIO` at all, so that
daemon falls back to the compiled default (also 8). Both paths converge on
`random_seq_ratio = 8` today, but via two different mechanisms — the
embedded path inherits it from the repo-root `db.env`, the TCP path
inherits it from the compiled default via an knob-silent fixture-authored
`db.env`. This is a coincidence of current values, not a structural
guarantee: if the repo-root `db.env` is ever edited to a different
`RANDOM_SEQ_COST_RATIO`, the 21 converted cases (all of which assert
directly on `selectivity_budget(N) = N / random_seq_ratio` thresholds)
would silently start computing budgets against the *new* value while the
rest of the suite's TCP-path cases kept using 8 — a divergence this plan's
tests would not by themselves catch. Task 7 adds an explicit check
confirming the repo-root `db.env`'s `RANDOM_SEQ_COST_RATIO` is still 8 at
verification time, precisely because this invariant is a live fact about
today's config files, not a property of the code.

All 21 target cases operate on tiny fixtures (≤255 records) and assert
purely on planner/eligibility decisions (index selection, plan kind,
selectivity thresholds) or fixed heap-size behavior — none depend on any
other non-default tuning knob. This was verified by re-reading every
target case's body (Tasks 3-6 below quote them in full).

## Execution rules

- Branch off `main`: `fix/tcp-to-direct-call-test-conversion` (or
  `feat/...` — this is a test-infra speed improvement, not a bug fix,
  `feat` fits per this repo's branch-naming rule). Task 0 captures the
  pre-edit baseline before this branch is created — do not branch first
  and capture the baseline after, since the branch itself makes no edits
  but any later task's edits would invalidate a baseline captured too late.
- Do tasks in order, starting with Task 0.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test
  run-all` (full suite — every case, not just the converted ones, must
  still pass, since these two files' untouched cases and every other test
  file remain on the TCP path).
- **If a quoted anchor is not found exactly as given, write
  `PLAN_NOTES.md` describing the mismatch and halt the entire execution
  run immediately** — do not guess, reinterpret, or continue to any
  further task, even an unrelated one.
- **If you hit a decision this plan doesn't cover, stop and ask** — do
  not improvise.
- Leave work uncommitted when done (per this repo's standing execution
  exception) — a Sonnet reviewer inspects the raw `git diff` before
  anything is committed.

## Task 0 — capture the pre-edit baseline

**Do this before any other task, on a clean checkout, before making any
edit in this plan.** Task 7's build/test comparison is only meaningful
against numbers captured before Tasks 1-6 touch the working tree; capturing
them after the fact (e.g. by re-checking-out `main` mid-execution) risks
comparing against a stashed or dirty state instead of the actual baseline.

1. Confirm a clean working tree on `main` (or the tip this branch forks
   from) before branching: `git status` must show no uncommitted changes.
2. Build: `SKIP_TESTS=1 ./build.sh`. Save the full compiler output —
   Task 7 step 1 diffs against this for new warnings.
3. Full suite: `./build/bin/shard-db-test run-all`. Record the total pass
   count and total assertion count reported at the end of the run.
4. Full suite, sequential: `./build/bin/shard-db-test run-all --jobs 1`.
   Record its pass count and assertion count separately — this is the
   number Task 7 step 7 compares against for the `--jobs 1` path
   specifically (parallel and sequential runs are not guaranteed to report
   identical counts if any pre-existing case is order- or
   concurrency-sensitive, so keep the two baselines distinct rather than
   assuming they match).
5. Record the repo-root `db.env`'s current `RANDOM_SEQ_COST_RATIO` value
   (`grep RANDOM_SEQ_COST_RATIO db.env`) — Task 7 step 8 checks this hasn't
   drifted by the time verification runs.
6. Branch off `main`: `fix/tcp-to-direct-call-test-conversion` (or
   `feat/...` — this is a test-infra speed improvement, not a bug fix,
   `feat` fits per this repo's branch-naming rule). Only after this baseline
   is captured does Task 1 begin.

## Task 1 — expose the process-local `ShardDb` instance

**File:** `src/db/embedded.c`

Test-first: there is no separate failing test for this task — it is pure
infrastructure consumed by Tasks 3-6, whose regression tests (the existing
21 case functions themselves, already passing on `main` today via the TCP
path) are the proof this task must satisfy. No behavior changes for any
currently-passing test until Tasks 3-6 rewire their setup to use it.

Locate this exact anchor (the end of `test_init_process_db()` and the
start of the next declaration):

```c
    static pthread_mutex_t instance_lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&instance_lock);
    if (!g_shard_db_instance) g_shard_db_instance = g_db;
    pthread_mutex_unlock(&instance_lock);
}

/* Forward decl — defined below in the impl section. */
static void db_mutexes_destroy(void);
```

Replace with:

```c
    static pthread_mutex_t instance_lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&instance_lock);
    if (!g_shard_db_instance) g_shard_db_instance = g_db;
    pthread_mutex_unlock(&instance_lock);
}

/* Test-only accessors exposing the process-local ShardDb instance that
   test_init_process_db() already opened before this test case started
   running. Lets a test case issue setup queries (add-dir, create-object,
   bulk-insert) in-process via shard_db_query() instead of spawning a
   daemon over TCP, without calling shard_db_open() again — a second
   shard_db_open_internal() call in the same process would allocate and
   leak a whole second ShardDb (open fds, mmaps, caches) while silently
   swapping g_db to the new, disconnected instance. */
ShardDb *test_get_process_db(void) {
    return g_db;
}

const char *test_get_process_db_root(void) {
    return g_db ? g_db->db_root : NULL;
}

/* Forward decl — defined below in the impl section. */
static void db_mutexes_destroy(void);
```

Edge case: both functions return `NULL`/the appropriate falsy value if
`g_db` is somehow unset — cannot happen in practice since
`test_init_process_db()` runs unconditionally before every case, but the
null-check on `test_get_process_db_root()` costs nothing and avoids a
NULL-deref if that invariant is ever violated.

## Task 2 — shared `tu_pdb_request()` / `tu_pdb_drop_object()` test utilities

**Files:** `src/test/fixtures.h`, `src/test/fixtures.c`

Test-first: same as Task 1 — proven by Tasks 3-6's case functions, which
call these directly.

### `src/test/fixtures.h`

Locate the existing declaration of `tu_parse_count` (or whichever `tu_`
helper appears last in the "Shared test utilities" block) and add
immediately after it:

```c
int tu_pdb_request(ShardDb *db, const char *json, char **out_response);
int tu_pdb_drop_object(ShardDb *db, const char *dir, const char *object);
```

This requires the `ShardDb` opaque type to be visible in this header. Add
near the top of `fixtures.h`, alongside its existing includes:

```c
#include "../db/shard_db.h"
```

### `src/test/fixtures.c`

Add near the top, alongside the existing `#include "test_client.h"`:

```c
#include "../db/shard_db.h"

extern ShardDb *test_get_process_db(void);
extern const char *test_get_process_db_root(void);
```

Add the implementations (placement: anywhere in the file at file scope,
e.g. adjacent to the other `tu_` helper implementations):

```c
int tu_pdb_request(ShardDb *db, const char *json, char **out_response) {
    size_t out_len = 0;
    return shard_db_query(db, json, out_response, &out_len);
}

/* Per-case teardown for the embedded (in-process) test transport. Restores
   object-level isolation the TCP path gets for free from a fresh daemon +
   tmpdir per case — NOT full fresh-root isolation: this only removes the
   named object's on-disk tree (via cmd_drop_object -> rmrf(obj_dir) plus
   its schema.conf line); it does not touch the "default" dirs.conf entry
   or the cache/mutex state cmd_create_object's auto-registration set up
   (see "Invariant: pre-existing add-dir key mismatch" in Task 3), which
   stay registered for the remainder of the sequential run. That's
   sufficient here because every one of the 21 cases uses the same "default"
   dir and a distinct object name (Tasks 3-6), so the surviving dir
   registration is inert, not a collision risk.

   Under `--jobs 1` (test_runner.c's run_all_sequential()) every case in
   the binary shares one process-wide ShardDb and tmpdir, so a converted
   case must drop what it created before returning. Idempotent
   (if_exists:true) so it's safe even if an earlier assertion in the same
   case already failed.

   shard_db_query()'s int return is NOT a reliable success/failure signal:
   dispatch_json_query() writes an "{"error":...}" JSON body on failure but
   shard_db_query() (embedded.c:310-348) still returns 0 in that case — it
   only returns -1 for a NULL argument, never for a dispatch-level error.
   The only way to detect a failed drop-object is to inspect the response
   text, matching the "\"error\"" substring convention already used
   elsewhere in this suite (e.g. src/test/cases/test_error_paths.c:39). */
int tu_pdb_drop_object(ShardDb *db, const char *dir, const char *object) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"drop-object\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"if_exists\":true}", dir, object);
    char *resp = NULL;
    size_t out_len = 0;
    int rc = shard_db_query(db, req, &resp, &out_len);
    int failed = (rc != 0) || !resp || strstr(resp, "\"error\"") != NULL;
    shard_db_free_result(resp);
    return failed ? 1 : 0;
}
```

Edge case: `shard_db_query`'s `out_len` output parameter is discarded in
both helpers because every existing call site treats the response as a
NUL-terminated C string (via `strstr`/`snprintf`/etc., matching how
`tc_request` responses are already consumed) — `shard_db_query`
NUL-terminates the buffer before returning (confirmed in `embedded.c`, it
strips the wire `\0\n` framing terminator and the underlying
`open_memstream` buffer is NUL-terminated on flush). If a future caller
needs raw length (e.g. binary-safe payloads), add a `_len` variant then —
not needed by any of the 21 target cases.

`tu_pdb_drop_object()` deliberately does **not** call any `ASSERT_*` macro
on the success path — only `strstr`, a plain C library call that records
nothing in the TAP counters. This keeps every converted case's assertion
count identical to its TCP-based baseline (see Finding 1's fix above):
adding a passing assertion here on top of the 21 unchanged assertions each
case already makes would itself create a new baseline mismatch. Callers
(Tasks 4-6) check the return value directly (`if (tu_pdb_drop_object(...) 
!= 0) return 1;`) and propagate failure as a plain non-zero test-function
return — which the runner reports as a failed case without needing a new
assertion to explain why.

## Task 3 — convert shared helpers in `test_planner_cost_model.c`

**File:** `src/test/cases/test_planner_cost_model.c`

Test-first: proven by every case in Task 4 (all 19 call `cm_pdb_setup`/
`cm_insert_tags`) — this task alone changes no test behavior. `cm_setup()`
itself is **not modified** by this task (see below) — only a new sibling
function, `cm_pdb_setup()`, is added, plus an in-place conversion of
`cm_insert_tags()`. The file compiles after Task 3 alone (`cm_pdb_setup`
is simply unused until Task 4 wires it in — expect a harmless
`-Wunused-function` warning between Task 3 and Task 4 if built standalone;
Do Task 3 and Task 4 together before building/testing, as originally
planned, so this warning window never actually surfaces in a real build).

Add near the top of the file, next to the existing `extern` declaration
(quote whatever that line currently is, e.g.):

```c
extern int leaf_selective_for_test(...);
```

add immediately after it:

```c
extern ShardDb *test_get_process_db(void);
extern const char *test_get_process_db_root(void);
```

(If the file does not already `#include "../../db/shard_db.h"` transitively
via `fixtures.h`, confirm `fixtures.h` is included — it already is, since
every case in this file uses `TestEnv`/`tu_` helpers from it.)

Locate this exact anchor:

```c
static TestClient *cm_setup(TestEnv *env, const char *obj, const char *fields,
                            const char *indexes) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }
    char *resp=NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp=NULL;
    char co[1024];
    snprintf(co,sizeof(co),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":8,\"max_key\":12,\"fields\":[%s],\"indexes\":[%s]}",
        obj, fields, indexes);
    tc_request(tc, co, &resp); free(resp); resp=NULL;
    return tc;
}
static void cm_insert_tags(TestClient *tc, const char *obj) {
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    for (int i=0;i<5;i++){SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"tag\":\"rare\"}",k==0?"":",",k);k++;}
    for (int i=0;i<200;i++){SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"tag\":\"common\"}",k);k++;}
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":%s}",obj,body);
    tc_request(tc, req, &resp); free(resp);
}
```

Replace with (note: `cm_setup()` itself is reproduced here **byte-for-byte
unchanged** — it is not being edited, only quoted so the new function has
an unambiguous insertion point right after it; `cm_insert_tags()` is the
one function in this block that actually changes, converted in place to
take `ShardDb *`):

```c
static TestClient *cm_setup(TestEnv *env, const char *obj, const char *fields,
                            const char *indexes) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }
    char *resp=NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp=NULL;
    char co[1024];
    snprintf(co,sizeof(co),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":8,\"max_key\":12,\"fields\":[%s],\"indexes\":[%s]}",
        obj, fields, indexes);
    tc_request(tc, co, &resp); free(resp); resp=NULL;
    return tc;
}

/* Embedded-transport twin of cm_setup(), for the 19 converted case
   functions in Task 4. cm_setup() above is left untouched because
   test_a3_trigram_starts_with_executor (excluded from this plan; see
   "Corrected candidate list") still calls it and still needs a
   TestClient * for its TCP round-trip via tc_request/tc_close. The
   add-dir request below intentionally keeps the pre-existing "name"
   key (not "dir", which is what the add-dir dispatcher in server.c:933
   actually reads) to match cm_setup()'s byte-for-byte behavior above —
   see "Invariant: pre-existing add-dir key mismatch" for why this is a
   deliberate no-op left alone rather than fixed as a drive-by change. */
static ShardDb *cm_pdb_setup(TestEnv *env, const char *obj, const char *fields,
                              const char *indexes) {
    ShardDb *db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return NULL;
    snprintf(env->db_root, sizeof(env->db_root), "%s", test_get_process_db_root());
    char *resp=NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); shard_db_free_result(resp); resp=NULL;
    char co[1024];
    snprintf(co,sizeof(co),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":8,\"max_key\":12,\"fields\":[%s],\"indexes\":[%s]}",
        obj, fields, indexes);
    tu_pdb_request(db, co, &resp); shard_db_free_result(resp); resp=NULL;
    return db;
}

static void cm_insert_tags(ShardDb *tc, const char *obj) {
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    for (int i=0;i<5;i++){SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"tag\":\"rare\"}",k==0?"":",",k);k++;}
    for (int i=0;i<200;i++){SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"tag\":\"common\"}",k);k++;}
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":%s}",obj,body);
    tu_pdb_request(tc, req, &resp); shard_db_free_result(resp);
}
```

Note: `env->db_root` is still populated (from `test_get_process_db_root()`)
because several downstream case bodies pass `env.db_root` to direct-call
planner/introspection functions (e.g. `leaf_selective_for_test(env.db_root,
...)`) — those calls are untouched by this plan and must keep receiving a
valid, live db_root. `env->port`/`env->daemon_pid` are simply never
populated/used by the converted cases from this point on (no case reads
them after this change — verified per-case in Task 4/5's full bodies).

### Invariant: pre-existing add-dir key mismatch

`cm_pdb_setup()` (and Task 5/6's bespoke setup code) sends
`{"mode":"add-dir","name":"default"}`. The `add-dir` dispatcher
(`server.c:933-948`) reads the JSON key `"dir"`, not `"name"` —
`json_obj_strdup()` does no key aliasing — so this call always returns
`{"error":"Missing dir"}` and is a no-op, both before and after this plan.
It is not a bug this plan introduces: `cm_setup()` above sends the exact
same `"name":"default"` payload today, over TCP, and has done so
throughout this file's history (confirmed: all `add-dir` calls in both
target files use `"name"`, never `"dir"`).

It is harmless because `cmd_create_object()` (`query_schema.c:1266-1284`)
unconditionally registers its own `dir` argument into `dirs.conf` (and
reloads `load_dirs()`'s in-memory set) as a side effect of every
successful `create-object` call — independent of whether the preceding
`add-dir` call succeeded. Verified empirically: a live daemon probe
confirmed `add-dir` with `"name"` returns `Missing dir` while the very
next `create-object` + `bulk-insert` for the same dir still succeeds.

This plan intentionally leaves the wrong key as-is rather than changing it
to `"dir"` — fixing a pre-existing, already-shipped, functionally-inert
mismatch is out of scope for a TCP-to-direct-call transport conversion,
and would be an unrelated drive-by change per this repo's review checklist
("no unrelated changes, no drive-by refactors that weren't asked for").
Task 7's build/test comparison against the Task 0 baseline is what
confirms this conversion changes no test outcomes, including this one.

## Task 4 — convert the 19 `cm_setup`-based case functions

**File:** `src/test/cases/test_planner_cost_model.c`

Test-first / regression proof: each function below is an existing,
currently-passing (TCP-based) test. After conversion, run
`./build/bin/shard-db-test run <name>` for every one of the 19 (names
given in each `TEST_REGISTER` line) and confirm identical PASS output and
**the same total assertion count** — not identical assertion *messages*.
One message intentionally changes: `cm_setup()`'s unconditional
`ASSERT_NOT_NULL(tc, "connect")` becomes `cm_pdb_setup()`'s unconditional
`ASSERT_NOT_NULL(db, "process db")` (Task 3) — same TAP slot (one passing
assertion on the success path in both), different, transport-accurate
wording, since there is no TCP connect step to name in the embedded path.
Every other assertion in these 19 functions is untouched text, so this is
the only message diff expected per case.

Apply these four mechanical rules uniformly (already threaded through
Task 3's `cm_pdb_setup`/`cm_insert_tags`, so each case function below only
needs its own local call sites updated):
- R1: `TestClient *tc = cm_setup(` → `ShardDb *tc = cm_pdb_setup(` (note:
  this calls the new `cm_pdb_setup()` from Task 3, not `cm_setup()` —
  `cm_setup()` itself is never called by any of these 19 functions after
  conversion)
- R2: `tc_request(tc, ...)` → `tu_pdb_request(tc, ...)` (rename only, same
  arguments)
- R3: `free(resp)` (and `resp2`/`resp3` where present) →
  `shard_db_free_result(resp)` / `shard_db_free_result(resp2)` /
  `shard_db_free_result(resp3)`
- R4: replace the `tc_close(tc); test_env_stop(&env);` line with one
  `if (tu_pdb_drop_object(tc, "default", "<object-name>") != 0) return 1;`
  call per object the function created (one call for every function except
  4.17, which creates two objects — `d3` and `d3b` — and needs two calls),
  placed immediately before `return 0;`. This is not optional — see
  "Process isolation: sequential vs parallel" in the Background section
  for why converted cases must clean up their own object under `--jobs 1`.
  The `if (...) return 1;` wrapper (rather than a bare call) surfaces a
  broken teardown as a failed case instead of silently ignoring it — see
  `tu_pdb_drop_object()`'s doc comment in Task 2 for why its own int return
  can't be trusted without inspecting the response body, and note this
  wrapper adds **no** new assertion, so it does not affect the
  same-assertion-count check above.

### 4.1 — `test_cost_selectivity_primitive`

Anchor:

```c
static int test_cost_selectivity_primitive(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "cm", "\"tag:varchar:8\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "cm");
    size_t kr=0, kc=0;
    int sr = leaf_selective_for_test(env.db_root, "default/cm", "tag", "rare", &kr);
    int sc = leaf_selective_for_test(env.db_root, "default/cm", "tag", "common", &kc);
    ASSERT_EQ_INT((int)kr, 5, "rare K=5");
    ASSERT_EQ_INT(sr, 1, "rare selective (5 <= budget 25)");
    ASSERT_EQ_INT(sc, 0, "common not selective (200 > budget 25)");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-cost-selectivity-primitive", test_cost_selectivity_primitive)
```

Replace with:

```c
static int test_cost_selectivity_primitive(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "cm", "\"tag:varchar:8\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "cm");
    size_t kr=0, kc=0;
    int sr = leaf_selective_for_test(env.db_root, "default/cm", "tag", "rare", &kr);
    int sc = leaf_selective_for_test(env.db_root, "default/cm", "tag", "common", &kc);
    ASSERT_EQ_INT((int)kr, 5, "rare K=5");
    ASSERT_EQ_INT(sr, 1, "rare selective (5 <= budget 25)");
    ASSERT_EQ_INT(sc, 0, "common not selective (200 > budget 25)");
    if (tu_pdb_drop_object(tc, "default", "cm") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-cost-selectivity-primitive", test_cost_selectivity_primitive)
```

### 4.2 — `test_planA1_selective_leaf`

Anchor:

```c
static int test_planA1_selective_leaf(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "a1", "\"tag:varchar:8\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "a1");
    char f[64]={0}, o[16]={0};
    const char *k = plan_filter_kind_for_test(env.db_root, "default/a1",
        "{\"tag\":\"rare\"}", NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k, "leaf", "A1 selective eq → PRIMARY_LEAF");
    ASSERT_EQ_STR(f, "tag", "A1 seeds on tag");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-a1-selective-leaf", test_planA1_selective_leaf)
```

Replace with:

```c
static int test_planA1_selective_leaf(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "a1", "\"tag:varchar:8\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "a1");
    char f[64]={0}, o[16]={0};
    const char *k = plan_filter_kind_for_test(env.db_root, "default/a1",
        "{\"tag\":\"rare\"}", NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k, "leaf", "A1 selective eq → PRIMARY_LEAF");
    ASSERT_EQ_STR(f, "tag", "A1 seeds on tag");
    if (tu_pdb_drop_object(tc, "default", "a1") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-a1-selective-leaf", test_planA1_selective_leaf)
```

### 4.3 — `test_planA2_broad_bitmap`

Anchor:

```c
static int test_planA2_broad_bitmap(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "a2", "\"active:bool\"", "\"active:bitmap\"");
    if (!tc) return 1;
    char body[16384]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    for(int i=0;i<70;i++){SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"active\":true}",k==0?"":",",k);k++;}
    for(int i=0;i<30;i++){SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"active\":false}",k);k++;}
    SB_APPEND(body, p, sizeof(body),"}");
    char req[17408];
    snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"a2\",\"records\":%s}",body);
    tc_request(tc, req, &resp); free(resp);
    char f[64]={0}, o[16]={0};
    const char *kind = plan_filter_kind_for_test(env.db_root, "default/a2",
        "{\"active\":true}", NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind, "bitmap", "A2 broad bitmap → BITMAP_SMALLER (not leaf)");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-a2-broad-bitmap", test_planA2_broad_bitmap)
```

Replace with:

```c
static int test_planA2_broad_bitmap(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "a2", "\"active:bool\"", "\"active:bitmap\"");
    if (!tc) return 1;
    char body[16384]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    for(int i=0;i<70;i++){SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"active\":true}",k==0?"":",",k);k++;}
    for(int i=0;i<30;i++){SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"active\":false}",k);k++;}
    SB_APPEND(body, p, sizeof(body),"}");
    char req[17408];
    snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"a2\",\"records\":%s}",body);
    tu_pdb_request(tc, req, &resp); shard_db_free_result(resp);
    char f[64]={0}, o[16]={0};
    const char *kind = plan_filter_kind_for_test(env.db_root, "default/a2",
        "{\"active\":true}", NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind, "bitmap", "A2 broad bitmap → BITMAP_SMALLER (not leaf)");
    if (tu_pdb_drop_object(tc, "default", "a2") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-a2-broad-bitmap", test_planA2_broad_bitmap)
```

### 4.4 — `test_planA5_nonindexed_scan`

Anchor:

```c
static int test_planA5_nonindexed_scan(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "a5", "\"tag:varchar:8\",\"note:varchar:16\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "a5"); /* note absent → empty; only tag indexed */
    char f[64]={0}, o[16]={0};
    /* Use array-form criteria so parse_criteria_tree gets a proper contains leaf */
    const char *k = plan_filter_kind_for_test(env.db_root, "default/a5",
        "[{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k, "scan", "A5 non-indexed → FULL_SCAN");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-a5-nonindexed-scan", test_planA5_nonindexed_scan)
```

Replace with:

```c
static int test_planA5_nonindexed_scan(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "a5", "\"tag:varchar:8\",\"note:varchar:16\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "a5"); /* note absent → empty; only tag indexed */
    char f[64]={0}, o[16]={0};
    /* Use array-form criteria so parse_criteria_tree gets a proper contains leaf */
    const char *k = plan_filter_kind_for_test(env.db_root, "default/a5",
        "[{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k, "scan", "A5 non-indexed → FULL_SCAN");
    if (tu_pdb_drop_object(tc, "default", "a5") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-a5-nonindexed-scan", test_planA5_nonindexed_scan)
```

### 4.5 — `test_planB1_two_selective_btree`

Anchor:

```c
static int test_planB1_two_selective_btree(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "b1",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;
    /* Insert 5 rows matching both, 200 matching only tag=common/tag2=common */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    for(int i=0;i<5;i++){
        SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"rare\"}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"tag\":\"common\",\"tag2\":\"common\"}",k); k++;
    }
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b1\",\"records\":%s}",body);
    tc_request(tc,req,&resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* count path (fetching=0): two selective indexed leaves → intersect */
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b1",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "intersect", "B1 count: two selective → intersect");

    /* find path (fetching=1): most-selective seeds PRIMARY_LEAF, other post-filters */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b1",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "leaf", "B1 find: two selective → leaf (fetch+check)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-b1-two-selective-btree", test_planB1_two_selective_btree)
```

Replace with:

```c
static int test_planB1_two_selective_btree(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "b1",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;
    /* Insert 5 rows matching both, 200 matching only tag=common/tag2=common */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    for(int i=0;i<5;i++){
        SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"rare\"}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"tag\":\"common\",\"tag2\":\"common\"}",k); k++;
    }
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b1\",\"records\":%s}",body);
    tu_pdb_request(tc,req,&resp); shard_db_free_result(resp);

    char f[64]={0}, o[16]={0};
    /* count path (fetching=0): two selective indexed leaves → intersect */
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b1",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "intersect", "B1 count: two selective → intersect");

    /* find path (fetching=1): most-selective seeds PRIMARY_LEAF, other post-filters */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b1",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "leaf", "B1 find: two selective → leaf (fetch+check)");

    if (tu_pdb_drop_object(tc, "default", "b1") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-b1-two-selective-btree", test_planB1_two_selective_btree)
```

### 4.6 — `test_planB2_selective_btree_broad_bitmap`

Anchor:

```c
static int test_planB2_selective_btree_broad_bitmap(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "b2",
        "\"tag:varchar:8\",\"active:bool\"",
        "\"tag\",\"active:bitmap\"");
    if (!tc) return 1;
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    /* 5 rare+active=true, 200 common+active=true (broad bitmap: 205/205 ≫ budget) */
    for(int i=0;i<5;i++){
        SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"tag\":\"rare\",\"active\":true}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"tag\":\"common\",\"active\":true}",k); k++;
    }
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b2\",\"records\":%s}",body);
    tc_request(tc,req,&resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* fetching=1: selective btree seeds, bitmap post-filters */
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b2",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "leaf", "B2 find: selective btree seeds, bitmap post-filters");
    ASSERT_EQ_STR(f, "tag", "B2 find: seed is tag (btree), not active (bitmap)");

    /* fetching=0: same — selective btree wins over broad bitmap */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b2",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "leaf", "B2 count: selective btree still seeds (not intersect: bitmap broad)");
    ASSERT_EQ_STR(f, "tag", "B2 count: seed is tag");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-b2-selective-btree-broad-bitmap", test_planB2_selective_btree_broad_bitmap)
```

Replace with:

```c
static int test_planB2_selective_btree_broad_bitmap(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "b2",
        "\"tag:varchar:8\",\"active:bool\"",
        "\"tag\",\"active:bitmap\"");
    if (!tc) return 1;
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    /* 5 rare+active=true, 200 common+active=true (broad bitmap: 205/205 ≫ budget) */
    for(int i=0;i<5;i++){
        SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"tag\":\"rare\",\"active\":true}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"tag\":\"common\",\"active\":true}",k); k++;
    }
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b2\",\"records\":%s}",body);
    tu_pdb_request(tc,req,&resp); shard_db_free_result(resp);

    char f[64]={0}, o[16]={0};
    /* fetching=1: selective btree seeds, bitmap post-filters */
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b2",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "leaf", "B2 find: selective btree seeds, bitmap post-filters");
    ASSERT_EQ_STR(f, "tag", "B2 find: seed is tag (btree), not active (bitmap)");

    /* fetching=0: same — selective btree wins over broad bitmap */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b2",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "leaf", "B2 count: selective btree still seeds (not intersect: bitmap broad)");
    ASSERT_EQ_STR(f, "tag", "B2 count: seed is tag");

    if (tu_pdb_drop_object(tc, "default", "b2") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-b2-selective-btree-broad-bitmap", test_planB2_selective_btree_broad_bitmap)
```

### 4.7 — `test_planB3_two_broad_bitmaps`

Anchor:

```c
static int test_planB3_two_broad_bitmaps(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "b3",
        "\"active:bool\",\"flagged:bool\"",
        "\"active:bitmap\",\"flagged:bitmap\"");
    if (!tc) return 1;
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    /* 150 rows: active=true (150/205 broad), flagged=true (150/205 broad) */
    for(int i=0;i<150;i++){
        SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"active\":true,\"flagged\":true}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<55;i++){
        SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"active\":false,\"flagged\":false}",k); k++;
    }
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b3\",\"records\":%s}",body);
    tc_request(tc,req,&resp); free(resp);

    char f[64]={0}, o[16]={0};
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b3",
        "[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"},"
         "{\"field\":\"flagged\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "intersect", "B3 find: pure-bitmap AND → intersect");

    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b3",
        "[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"},"
         "{\"field\":\"flagged\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "intersect", "B3 count: pure-bitmap AND → intersect");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-b3-two-broad-bitmaps", test_planB3_two_broad_bitmaps)
```

Replace with:

```c
static int test_planB3_two_broad_bitmaps(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "b3",
        "\"active:bool\",\"flagged:bool\"",
        "\"active:bitmap\",\"flagged:bitmap\"");
    if (!tc) return 1;
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    /* 150 rows: active=true (150/205 broad), flagged=true (150/205 broad) */
    for(int i=0;i<150;i++){
        SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"active\":true,\"flagged\":true}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<55;i++){
        SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"active\":false,\"flagged\":false}",k); k++;
    }
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b3\",\"records\":%s}",body);
    tu_pdb_request(tc,req,&resp); shard_db_free_result(resp);

    char f[64]={0}, o[16]={0};
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b3",
        "[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"},"
         "{\"field\":\"flagged\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "intersect", "B3 find: pure-bitmap AND → intersect");

    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b3",
        "[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"},"
         "{\"field\":\"flagged\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "intersect", "B3 count: pure-bitmap AND → intersect");

    if (tu_pdb_drop_object(tc, "default", "b3") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-b3-two-broad-bitmaps", test_planB3_two_broad_bitmaps)
```

### 4.8 — `test_planB4_selective_btree_nonindexed`

Anchor:

```c
static int test_planB4_selective_btree_nonindexed(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "b4",
        "\"tag:varchar:8\",\"note:varchar:16\"",
        "\"tag\"");   /* note is NOT indexed */
    if (!tc) return 1;
    cm_insert_tags(tc, "b4");   /* 5 rare + 200 common; note field empty */

    char f[64]={0}, o[16]={0};
    /* fetching=1: selective btree seeds; non-indexed note post-filters on record */
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b4",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "leaf", "B4 find: selective btree seeds, non-indexed post-filters");
    ASSERT_EQ_STR(f, "tag", "B4 find: seed is tag");

    /* fetching=0: same — one selective + one non-indexed → leaf (can't intersect non-indexed) */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b4",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "leaf", "B4 count: selective btree + non-indexed → leaf");
    ASSERT_EQ_STR(f, "tag", "B4 count: seed is tag");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-b4-selective-btree-nonindexed", test_planB4_selective_btree_nonindexed)
```

Replace with:

```c
static int test_planB4_selective_btree_nonindexed(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "b4",
        "\"tag:varchar:8\",\"note:varchar:16\"",
        "\"tag\"");   /* note is NOT indexed */
    if (!tc) return 1;
    cm_insert_tags(tc, "b4");   /* 5 rare + 200 common; note field empty */

    char f[64]={0}, o[16]={0};
    /* fetching=1: selective btree seeds; non-indexed note post-filters on record */
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b4",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "leaf", "B4 find: selective btree seeds, non-indexed post-filters");
    ASSERT_EQ_STR(f, "tag", "B4 find: seed is tag");

    /* fetching=0: same — one selective + one non-indexed → leaf (can't intersect non-indexed) */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b4",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "leaf", "B4 count: selective btree + non-indexed → leaf");
    ASSERT_EQ_STR(f, "tag", "B4 count: seed is tag");

    if (tu_pdb_drop_object(tc, "default", "b4") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-b4-selective-btree-nonindexed", test_planB4_selective_btree_nonindexed)
```

### 4.9 — `test_planB7_all_nonindexed`

Anchor:

```c
static int test_planB7_all_nonindexed(void) {
    TestEnv env={0};
    /* bio and about: NO indexes at all (empty index list) */
    TestClient *tc = cm_setup(&env, "b7",
        "\"bio:varchar:16\",\"about:varchar:16\"",
        "");   /* intentionally no indexes */
    if (!tc) return 1;
    /* Insert a few rows so N>0 */
    char *resp=NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b7\","
        "\"records\":{\"k0\":{\"bio\":\"hello\",\"about\":\"world\"},"
                      "\"k1\":{\"bio\":\"foo\",\"about\":\"bar\"}}}",
        &resp); free(resp);

    char f[64]={0}, o[16]={0};
    const char *k = plan_filter_kind_for_test(env.db_root,"default/b7",
        "[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"x\"},"
         "{\"field\":\"about\",\"op\":\"contains\",\"value\":\"y\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k, "scan", "B7: both non-indexed → full scan");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-b7-all-nonindexed", test_planB7_all_nonindexed)
```

Replace with:

```c
static int test_planB7_all_nonindexed(void) {
    TestEnv env={0};
    /* bio and about: NO indexes at all (empty index list) */
    ShardDb *tc = cm_pdb_setup(&env, "b7",
        "\"bio:varchar:16\",\"about:varchar:16\"",
        "");   /* intentionally no indexes */
    if (!tc) return 1;
    /* Insert a few rows so N>0 */
    char *resp=NULL;
    tu_pdb_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b7\","
        "\"records\":{\"k0\":{\"bio\":\"hello\",\"about\":\"world\"},"
                      "\"k1\":{\"bio\":\"foo\",\"about\":\"bar\"}}}",
        &resp); shard_db_free_result(resp);

    char f[64]={0}, o[16]={0};
    const char *k = plan_filter_kind_for_test(env.db_root,"default/b7",
        "[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"x\"},"
         "{\"field\":\"about\",\"op\":\"contains\",\"value\":\"y\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k, "scan", "B7: both non-indexed → full scan");

    if (tu_pdb_drop_object(tc, "default", "b7") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-b7-all-nonindexed", test_planB7_all_nonindexed)
```

### 4.10 — `test_planA4_saturated_trigram_stays_leaf`

Anchor:

```c
static int test_planA4_saturated_trigram_stays_leaf(void) {
    TestEnv env={0};
    /* trigram index on title field */
    TestClient *tc = cm_setup(&env, "a4tg",
        "\"title:varchar:32\"",
        "\"title:trigram\"");
    if (!tc) return 1;

    /* Build bulk-insert: 50 rows with "abcdef" (contains trigram abc,bcd,cde,def)
     * and 155 rows with "xyZpqr" (no overlap). N=205, budget=205/8=25.
     * Rarest gram among abc/bcd/cde/def = 50 > 25 → saturated. */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i=0; i<50; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"title\":\"abcdef\"}", k==0?"":",", k); k++;
    }
    for (int i=0; i<155; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"title\":\"xyZpqr\"}", k); k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"a4tg\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* Pattern "abc" → IT_TRIGRAM, saturated. Must be "leaf", never "scan". */
    const char *k_str = plan_filter_kind_for_test(env.db_root, "default/a4tg",
        "[{\"field\":\"title\",\"op\":\"contains\",\"value\":\"abc\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "leaf",
        "A4: saturated trigram-contains stays PRIMARY_LEAF (never FULL_SCAN)");

    /* Also check fetching=0 (count path) — same result. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root, "default/a4tg",
        "[{\"field\":\"title\",\"op\":\"contains\",\"value\":\"abc\"}]",
        NULL, 0, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "leaf",
        "A4 count: saturated trigram-contains stays leaf");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-a4-saturated-trigram-stays-leaf",
              test_planA4_saturated_trigram_stays_leaf)
```

Replace with:

```c
static int test_planA4_saturated_trigram_stays_leaf(void) {
    TestEnv env={0};
    /* trigram index on title field */
    ShardDb *tc = cm_pdb_setup(&env, "a4tg",
        "\"title:varchar:32\"",
        "\"title:trigram\"");
    if (!tc) return 1;

    /* Build bulk-insert: 50 rows with "abcdef" (contains trigram abc,bcd,cde,def)
     * and 155 rows with "xyZpqr" (no overlap). N=205, budget=205/8=25.
     * Rarest gram among abc/bcd/cde/def = 50 > 25 → saturated. */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i=0; i<50; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"title\":\"abcdef\"}", k==0?"":",", k); k++;
    }
    for (int i=0; i<155; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"title\":\"xyZpqr\"}", k); k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"a4tg\","
        "\"records\":%s}", body);
    tu_pdb_request(tc, req, &resp); shard_db_free_result(resp);

    char f[64]={0}, o[16]={0};
    /* Pattern "abc" → IT_TRIGRAM, saturated. Must be "leaf", never "scan". */
    const char *k_str = plan_filter_kind_for_test(env.db_root, "default/a4tg",
        "[{\"field\":\"title\",\"op\":\"contains\",\"value\":\"abc\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "leaf",
        "A4: saturated trigram-contains stays PRIMARY_LEAF (never FULL_SCAN)");

    /* Also check fetching=0 (count path) — same result. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root, "default/a4tg",
        "[{\"field\":\"title\",\"op\":\"contains\",\"value\":\"abc\"}]",
        NULL, 0, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "leaf",
        "A4 count: saturated trigram-contains stays leaf");

    if (tu_pdb_drop_object(tc, "default", "a4tg") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-a4-saturated-trigram-stays-leaf",
              test_planA4_saturated_trigram_stays_leaf)
```

### 4.11 — `test_planBCS_count_one_selective_leaf`

Anchor:

```c
static int test_planBCS_count_one_selective_leaf(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "bcs",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;

    /* 5 rows tag=rare + tag2=common, 200 rows tag=common + tag2=common.
     * tag=rare: 5 ≤ 25 → selective. tag2=common: 205 > 25 → broad.
     * n_selective=1 (only tag=rare clears the bar). */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i=0; i<5; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"common\"}",
            k==0?"":",", k); k++;
    }
    for (int i=0; i<200; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"tag\":\"common\",\"tag2\":\"common\"}", k); k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bcs\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* fetching=0, n_indexed=2, n_selective=1 → must fall through to leaf. */
    const char *kc = plan_filter_kind_for_test(env.db_root, "default/bcs",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"common\"}]",
        NULL, 0, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "leaf",
        "BCS count: n_indexed=2, exactly 1 selective → PRIMARY_LEAF (not intersect)");
    ASSERT_EQ_STR(f, "tag",
        "BCS count: seed is the selective field `tag`");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-bcs-count-one-selective-leaf",
              test_planBCS_count_one_selective_leaf)
```

Replace with:

```c
static int test_planBCS_count_one_selective_leaf(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "bcs",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;

    /* 5 rows tag=rare + tag2=common, 200 rows tag=common + tag2=common.
     * tag=rare: 5 ≤ 25 → selective. tag2=common: 205 > 25 → broad.
     * n_selective=1 (only tag=rare clears the bar). */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i=0; i<5; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"common\"}",
            k==0?"":",", k); k++;
    }
    for (int i=0; i<200; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"tag\":\"common\",\"tag2\":\"common\"}", k); k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bcs\","
        "\"records\":%s}", body);
    tu_pdb_request(tc, req, &resp); shard_db_free_result(resp);

    char f[64]={0}, o[16]={0};
    /* fetching=0, n_indexed=2, n_selective=1 → must fall through to leaf. */
    const char *kc = plan_filter_kind_for_test(env.db_root, "default/bcs",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"common\"}]",
        NULL, 0, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "leaf",
        "BCS count: n_indexed=2, exactly 1 selective → PRIMARY_LEAF (not intersect)");
    ASSERT_EQ_STR(f, "tag",
        "BCS count: seed is the selective field `tag`");

    if (tu_pdb_drop_object(tc, "default", "bcs") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-bcs-count-one-selective-leaf",
              test_planBCS_count_one_selective_leaf)
```

### 4.12 — `test_planC1_pure_or_all_indexed`

Anchor:

```c
static int test_planC1_pure_or_all_indexed(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "c1",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;
    /* 5 rows tag=rare+tag2=rare, 200 tag=common+tag2=common.
     * Both selective fields indexed → pure OR with all indexed children. */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    for(int i=0;i<5;i++){
        SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"rare\"}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"tag\":\"common\",\"tag2\":\"common\"}",k); k++;
    }
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"c1\",\"records\":%s}",body);
    tc_request(tc,req,&resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* Pure OR array form: [{"or":[{tag=rare},{tag2=rare}]}] */
    const char *k_str = plan_filter_kind_for_test(env.db_root,"default/c1",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "union", "C1: pure OR all-indexed → FP_UNION");

    /* Also test fetching=0 (count) */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root,"default/c1",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "union", "C1 count: pure OR all-indexed → FP_UNION");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-c1-pure-or-all-indexed", test_planC1_pure_or_all_indexed)
```

Replace with:

```c
static int test_planC1_pure_or_all_indexed(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "c1",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;
    /* 5 rows tag=rare+tag2=rare, 200 tag=common+tag2=common.
     * Both selective fields indexed → pure OR with all indexed children. */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    for(int i=0;i<5;i++){
        SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"rare\"}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"tag\":\"common\",\"tag2\":\"common\"}",k); k++;
    }
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"c1\",\"records\":%s}",body);
    tu_pdb_request(tc,req,&resp); shard_db_free_result(resp);

    char f[64]={0}, o[16]={0};
    /* Pure OR array form: [{"or":[{tag=rare},{tag2=rare}]}] */
    const char *k_str = plan_filter_kind_for_test(env.db_root,"default/c1",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "union", "C1: pure OR all-indexed → FP_UNION");

    /* Also test fetching=0 (count) */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root,"default/c1",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "union", "C1 count: pure OR all-indexed → FP_UNION");

    if (tu_pdb_drop_object(tc, "default", "c1") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-c1-pure-or-all-indexed", test_planC1_pure_or_all_indexed)
```

### 4.13 — `test_planC2_or_with_nonindexed_child`

Anchor:

```c
static int test_planC2_or_with_nonindexed_child(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "c2",
        "\"tag:varchar:8\",\"note:varchar:16\"",
        "\"tag\"");   /* note is NOT indexed */
    if (!tc) return 1;
    cm_insert_tags(tc, "c2");   /* 5 rare + 200 common; note field absent */

    char f[64]={0}, o[16]={0};
    /* OR where one child (note contains "x") has no index */
    const char *k_str = plan_filter_kind_for_test(env.db_root,"default/c2",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "scan", "C2: OR with non-indexed child → FULL_SCAN");

    /* Also count path */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root,"default/c2",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "scan", "C2 count: OR with non-indexed child → FULL_SCAN");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-c2-or-with-nonindexed-child", test_planC2_or_with_nonindexed_child)
```

Replace with:

```c
static int test_planC2_or_with_nonindexed_child(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "c2",
        "\"tag:varchar:8\",\"note:varchar:16\"",
        "\"tag\"");   /* note is NOT indexed */
    if (!tc) return 1;
    cm_insert_tags(tc, "c2");   /* 5 rare + 200 common; note field absent */

    char f[64]={0}, o[16]={0};
    /* OR where one child (note contains "x") has no index */
    const char *k_str = plan_filter_kind_for_test(env.db_root,"default/c2",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "scan", "C2: OR with non-indexed child → FULL_SCAN");

    /* Also count path */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root,"default/c2",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "scan", "C2 count: OR with non-indexed child → FULL_SCAN");

    if (tu_pdb_drop_object(tc, "default", "c2") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-c2-or-with-nonindexed-child", test_planC2_or_with_nonindexed_child)
```

### 4.14 — `test_planC3_and_leaf_plus_or_subtree`

Anchor:

```c
static int test_planC3_and_leaf_plus_or_subtree(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "c3",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;
    /* 5 rows tag=rare+tag2=a, 200 rows tag=common+tag2=b */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    for(int i=0;i<5;i++){
        SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"a\"}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"tag\":\"common\",\"tag2\":\"b\"}",k); k++;
    }
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"c3\",\"records\":%s}",body);
    tc_request(tc,req,&resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* AND + OR sub-tree: [{tag=rare}, {"or":[{tag2=a},{tag2=b}]}]
     * collect_and_leaves sees 1 LEAF (tag=rare), skips the OR child.
     * Seeds on tag → FP_PRIMARY_LEAF. */
    const char *k_str = plan_filter_kind_for_test(env.db_root,"default/c3",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"or\":[{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"a\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"b\"}]}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "leaf", "C3: AND+OR sub-tree → PRIMARY_LEAF");
    ASSERT_EQ_STR(f, "tag", "C3: seed is the AND leaf `tag`, not the OR child");

    /* fetching=0 (count): same — one selective indexed AND-leaf seeds */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root,"default/c3",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"or\":[{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"a\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"b\"}]}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "leaf", "C3 count: AND+OR sub-tree → PRIMARY_LEAF");
    ASSERT_EQ_STR(f, "tag", "C3 count: seed is tag");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-c3-and-leaf-plus-or-subtree", test_planC3_and_leaf_plus_or_subtree)
```

Replace with:

```c
static int test_planC3_and_leaf_plus_or_subtree(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "c3",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;
    /* 5 rows tag=rare+tag2=a, 200 rows tag=common+tag2=b */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body),"{");
    for(int i=0;i<5;i++){
        SB_APPEND(body, p, sizeof(body),"%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"a\"}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        SB_APPEND(body, p, sizeof(body),",\"k%d\":{\"tag\":\"common\",\"tag2\":\"b\"}",k); k++;
    }
    SB_APPEND(body, p, sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"c3\",\"records\":%s}",body);
    tu_pdb_request(tc,req,&resp); shard_db_free_result(resp);

    char f[64]={0}, o[16]={0};
    /* AND + OR sub-tree: [{tag=rare}, {"or":[{tag2=a},{tag2=b}]}]
     * collect_and_leaves sees 1 LEAF (tag=rare), skips the OR child.
     * Seeds on tag → FP_PRIMARY_LEAF. */
    const char *k_str = plan_filter_kind_for_test(env.db_root,"default/c3",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"or\":[{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"a\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"b\"}]}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "leaf", "C3: AND+OR sub-tree → PRIMARY_LEAF");
    ASSERT_EQ_STR(f, "tag", "C3: seed is the AND leaf `tag`, not the OR child");

    /* fetching=0 (count): same — one selective indexed AND-leaf seeds */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root,"default/c3",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"or\":[{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"a\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"b\"}]}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "leaf", "C3 count: AND+OR sub-tree → PRIMARY_LEAF");
    ASSERT_EQ_STR(f, "tag", "C3 count: seed is tag");

    if (tu_pdb_drop_object(tc, "default", "c3") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-c3-and-leaf-plus-or-subtree", test_planC3_and_leaf_plus_or_subtree)
```

### 4.15 — `test_planD1_composite_order`

Anchor:

```c
static int test_planD1_composite_order(void) {
    TestEnv env={0};
    /* Two fields; composite index "by+time" stays btree. */
    TestClient *tc = cm_setup(&env, "d1",
        "\"by:varchar:16\",\"time:long\"",
        "\"by\",\"by+time\"");
    if (!tc) return 1;

    /* Insert 5 rows by="alice", 200 rows by="bob" (N=205, budget=25).
     * by="alice" is selective (5 ≤ 25). */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i=0; i<5; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"by\":\"alice\",\"time\":%d}",
            k==0?"":",", k, k*100); k++;
    }
    for (int i=0; i<200; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"by\":\"bob\",\"time\":%d}", k, k*100); k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d1\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp);

    char f[64]={0}, o[32]={0};
    int tc_val = -1;
    const char *k_str = plan_filter_kind_for_test(env.db_root, "default/d1",
        "{\"by\":\"alice\"}", "time", 1,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_str, "leaf", "D1: selective by= → PRIMARY_LEAF");
    ASSERT_EQ_STR(o, "composite", "D1: by+time composite → FP_ORDER_COMPOSITE");
    ASSERT_EQ_INT(tc_val, 1, "D1: total_cheap=1 (KeySet materialized)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-d1-composite-order", test_planD1_composite_order)
```

Replace with:

```c
static int test_planD1_composite_order(void) {
    TestEnv env={0};
    /* Two fields; composite index "by+time" stays btree. */
    ShardDb *tc = cm_pdb_setup(&env, "d1",
        "\"by:varchar:16\",\"time:long\"",
        "\"by\",\"by+time\"");
    if (!tc) return 1;

    /* Insert 5 rows by="alice", 200 rows by="bob" (N=205, budget=25).
     * by="alice" is selective (5 ≤ 25). */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i=0; i<5; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"by\":\"alice\",\"time\":%d}",
            k==0?"":",", k, k*100); k++;
    }
    for (int i=0; i<200; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"by\":\"bob\",\"time\":%d}", k, k*100); k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d1\","
        "\"records\":%s}", body);
    tu_pdb_request(tc, req, &resp); shard_db_free_result(resp);

    char f[64]={0}, o[32]={0};
    int tc_val = -1;
    const char *k_str = plan_filter_kind_for_test(env.db_root, "default/d1",
        "{\"by\":\"alice\"}", "time", 1,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_str, "leaf", "D1: selective by= → PRIMARY_LEAF");
    ASSERT_EQ_STR(o, "composite", "D1: by+time composite → FP_ORDER_COMPOSITE");
    ASSERT_EQ_INT(tc_val, 1, "D1: total_cheap=1 (KeySet materialized)");

    if (tu_pdb_drop_object(tc, "default", "d1") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-d1-composite-order", test_planD1_composite_order)
```

### 4.16 — `test_planD2_sort_order`

Anchor:

```c
static int test_planD2_sort_order(void) {
    TestEnv env={0};
    /* Only "by" indexed; no composite; time is not indexed. */
    TestClient *tc = cm_setup(&env, "d2",
        "\"by:varchar:16\",\"time:long\"",
        "\"by\"");
    if (!tc) return 1;

    /* 5 rows by="alice" (selective), 200 rows by="bob". */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i=0; i<5; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"by\":\"alice\",\"time\":%d}",
            k==0?"":",", k, k*100); k++;
    }
    for (int i=0; i<200; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"by\":\"bob\",\"time\":%d}", k, k*100); k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d2\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp);

    char f[64]={0}, o[32]={0};
    int tc_val = -1;
    const char *k_str = plan_filter_kind_for_test(env.db_root, "default/d2",
        "{\"by\":\"alice\"}", "time", 1,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_str, "leaf", "D2: selective by= → PRIMARY_LEAF");
    ASSERT_EQ_STR(o, "sort", "D2: bounded K=5, no composite → FP_ORDER_SORT");
    ASSERT_EQ_INT(tc_val, 1, "D2: total_cheap=1 (KeySet materialized)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-d2-sort-order", test_planD2_sort_order)
```

Replace with:

```c
static int test_planD2_sort_order(void) {
    TestEnv env={0};
    /* Only "by" indexed; no composite; time is not indexed. */
    ShardDb *tc = cm_pdb_setup(&env, "d2",
        "\"by:varchar:16\",\"time:long\"",
        "\"by\"");
    if (!tc) return 1;

    /* 5 rows by="alice" (selective), 200 rows by="bob". */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i=0; i<5; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"by\":\"alice\",\"time\":%d}",
            k==0?"":",", k, k*100); k++;
    }
    for (int i=0; i<200; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"by\":\"bob\",\"time\":%d}", k, k*100); k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d2\","
        "\"records\":%s}", body);
    tu_pdb_request(tc, req, &resp); shard_db_free_result(resp);

    char f[64]={0}, o[32]={0};
    int tc_val = -1;
    const char *k_str = plan_filter_kind_for_test(env.db_root, "default/d2",
        "{\"by\":\"alice\"}", "time", 1,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_str, "leaf", "D2: selective by= → PRIMARY_LEAF");
    ASSERT_EQ_STR(o, "sort", "D2: bounded K=5, no composite → FP_ORDER_SORT");
    ASSERT_EQ_INT(tc_val, 1, "D2: total_cheap=1 (KeySet materialized)");

    if (tu_pdb_drop_object(tc, "default", "d2") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-d2-sort-order", test_planD2_sort_order)
```

### 4.17 — `test_planD3_walk_order`

This one has three separate `tc_request`/`free` call sites (initial bulk-insert
via `resp`, a mid-function `create-object` for a second object `d3b` via
`resp2`, and its bulk-insert via `resp3`) — all three convert by the same
rules.

Anchor:

```c
static int test_planD3_walk_order(void) {
    TestEnv env={0};
    /* Only "by" indexed; no composite. */
    TestClient *tc = cm_setup(&env, "d3",
        "\"by:varchar:16\",\"time:long\"",
        "\"by\"");
    if (!tc) return 1;

    /* 200 rows by="bob" (broad: 200 > budget 25), 5 rows by="alice".
     * Query on by="bob" → broad → seed saturated → FP_ORDER_INDEX_WALK. */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i=0; i<200; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"by\":\"bob\",\"time\":%d}",
            k==0?"":",", k, k*100); k++;
    }
    for (int i=0; i<5; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"by\":\"alice\",\"time\":%d}", k, k*100); k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d3\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp);

    char f[64]={0}, o[32]={0};
    int tc_val = -1;
    /* by="bob" is broad: N=205, budget=25, K=200 > 25 → saturated.
     * pick_index_for_leaf=IT_BTREE (not bitmap). The demotion gate
     * (op_eligible_for_intersect) fires for eq, demoting to FULL_SCAN unless
     * the seed is a PRIMARY_LEAF at all. Wait — the single-seed block checks
     * !prim_sel && prim_it!=IT_BITMAP && est.saturated → FULL_SCAN.
     * But "bob" IS broad (saturated) and the single-seed block would scan.
     * For order overlay D3 we need the path where the plan IS a leaf but
     * the seed is broad. Use the multi-leaf path to force PRIMARY_LEAF:
     * criteria AND of [by="bob", time >= 0] where time is not indexed —
     * only by is indexed → n_indexed=1, prim=by, prim_sel=0, prim_it=IT_BTREE.
     * BUT the demotion guard fires → FULL_SCAN. No order overlay for FULL_SCAN.
     *
     * The spec says D3 fires "when candidates > budget" and the plan is
     * non-scan. To reach D3 cleanly: use a BROAD bitmap (never demoted to
     * FULL_SCAN) with order_by. A broad bitmap → FP_BITMAP_SMALLER. Its seed
     * K is the smaller-side complement count (estimable, may be saturated when
     * both sides are broad). For a pure D3-like "walk" path via btree, we need
     * n_selective=0 and all_bitmap=0, falling to intersect (which can then
     * have a broad seed for the overlay). OR: use the multi-leaf broad intersect
     * path (n_selective=0, n_indexed≥2) → FP_INTERSECT → order overlay runs
     * on source_leaves[0] which is broad → saturated → WALK.
     *
     * Build: two non-selective btree leaves, n_selective=0 → INTERSECT,
     * then overlay on source_leaves[0] (broad) → WALK. */

    /* Re-insert into a fresh object d3b with two indexed fields, both broad. */
    char *resp2=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"d3b\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"by:varchar:16\",\"cat:varchar:8\"],"
        "\"indexes\":[\"by\",\"cat\"]}",
        &resp2); free(resp2); resp2=NULL;

    /* 200 rows by="bob"/cat="x" → both broad (200 > budget 25). N=200. */
    char body2[65536]; size_t p2=0; int k2=0;
    SB_APPEND(body2, p2, sizeof(body2), "{");
    for (int i=0; i<200; i++) {
        SB_APPEND(body2, p2, sizeof(body2),
            "%s\"r%d\":{\"by\":\"bob\",\"cat\":\"x\"}",
            k2==0?"":",", k2); k2++;
    }
    SB_APPEND(body2, p2, sizeof(body2), "}");
    char req2[66560];
    snprintf(req2, sizeof(req2),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d3b\","
        "\"records\":%s}", body2);
    char *resp3=NULL;
    tc_request(tc, req2, &resp3); free(resp3);

    /* fetching=1 (find), both broad indexed → FP_INTERSECT (n_selective=0 path).
     * source_leaves[0] = "by"="bob" → broad → saturated → FP_ORDER_INDEX_WALK.
     * After Fix 2, count with all-broad falls through to single-seed
     * PRIMARY_LEAF; only the find path still hits FP_INTERSECT here. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); tc_val=-1;
    const char *k_d3b = plan_filter_kind_for_test(env.db_root, "default/d3b",
        "[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"bob\"},"
         "{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"x\"}]",
        "by", 1,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_d3b, "intersect", "D3: all-broad indexed find → INTERSECT");
    ASSERT_EQ_STR(o, "walk", "D3: broad seed + order_by → FP_ORDER_INDEX_WALK");
    ASSERT_EQ_INT(tc_val, 1, "D3: total_cheap=1 (INTERSECT materializes KeySet)");

    /* After Fix 2: count with all-broad falls through to single-seed
     * PRIMARY_LEAF instead of FP_INTERSECT. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); tc_val=-1;
    const char *k_d3b_cnt = plan_filter_kind_for_test(env.db_root, "default/d3b",
        "[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"bob\"},"
         "{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"x\"}]",
        "by", 0,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_d3b_cnt, "leaf",
        "D3 count: all-broad indexed count → PRIMARY_LEAF (not INTERSECT)");
    ASSERT_EQ_STR(f, "by",
        "D3 count: seed is most-selective indexed leaf");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-d3-walk-order", test_planD3_walk_order)
```

Replace with:

```c
static int test_planD3_walk_order(void) {
    TestEnv env={0};
    /* Only "by" indexed; no composite. */
    ShardDb *tc = cm_pdb_setup(&env, "d3",
        "\"by:varchar:16\",\"time:long\"",
        "\"by\"");
    if (!tc) return 1;

    /* 200 rows by="bob" (broad: 200 > budget 25), 5 rows by="alice".
     * Query on by="bob" → broad → seed saturated → FP_ORDER_INDEX_WALK. */
    char body[65536]; size_t p=0; int k=0; char *resp=NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i=0; i<200; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"by\":\"bob\",\"time\":%d}",
            k==0?"":",", k, k*100); k++;
    }
    for (int i=0; i<5; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"by\":\"alice\",\"time\":%d}", k, k*100); k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d3\","
        "\"records\":%s}", body);
    tu_pdb_request(tc, req, &resp); shard_db_free_result(resp);

    char f[64]={0}, o[32]={0};
    int tc_val = -1;
    /* by="bob" is broad: N=205, budget=25, K=200 > 25 → saturated.
     * pick_index_for_leaf=IT_BTREE (not bitmap). The demotion gate
     * (op_eligible_for_intersect) fires for eq, demoting to FULL_SCAN unless
     * the seed is a PRIMARY_LEAF at all. Wait — the single-seed block checks
     * !prim_sel && prim_it!=IT_BITMAP && est.saturated → FULL_SCAN.
     * But "bob" IS broad (saturated) and the single-seed block would scan.
     * For order overlay D3 we need the path where the plan IS a leaf but
     * the seed is broad. Use the multi-leaf path to force PRIMARY_LEAF:
     * criteria AND of [by="bob", time >= 0] where time is not indexed —
     * only by is indexed → n_indexed=1, prim=by, prim_sel=0, prim_it=IT_BTREE.
     * BUT the demotion guard fires → FULL_SCAN. No order overlay for FULL_SCAN.
     *
     * The spec says D3 fires "when candidates > budget" and the plan is
     * non-scan. To reach D3 cleanly: use a BROAD bitmap (never demoted to
     * FULL_SCAN) with order_by. A broad bitmap → FP_BITMAP_SMALLER. Its seed
     * K is the smaller-side complement count (estimable, may be saturated when
     * both sides are broad). For a pure D3-like "walk" path via btree, we need
     * n_selective=0 and all_bitmap=0, falling to intersect (which can then
     * have a broad seed for the overlay). OR: use the multi-leaf broad intersect
     * path (n_selective=0, n_indexed≥2) → FP_INTERSECT → order overlay runs
     * on source_leaves[0] which is broad → saturated → WALK.
     *
     * Build: two non-selective btree leaves, n_selective=0 → INTERSECT,
     * then overlay on source_leaves[0] (broad) → WALK. */

    /* Re-insert into a fresh object d3b with two indexed fields, both broad. */
    char *resp2=NULL;
    tu_pdb_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"d3b\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"by:varchar:16\",\"cat:varchar:8\"],"
        "\"indexes\":[\"by\",\"cat\"]}",
        &resp2); shard_db_free_result(resp2); resp2=NULL;

    /* 200 rows by="bob"/cat="x" → both broad (200 > budget 25). N=200. */
    char body2[65536]; size_t p2=0; int k2=0;
    SB_APPEND(body2, p2, sizeof(body2), "{");
    for (int i=0; i<200; i++) {
        SB_APPEND(body2, p2, sizeof(body2),
            "%s\"r%d\":{\"by\":\"bob\",\"cat\":\"x\"}",
            k2==0?"":",", k2); k2++;
    }
    SB_APPEND(body2, p2, sizeof(body2), "}");
    char req2[66560];
    snprintf(req2, sizeof(req2),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d3b\","
        "\"records\":%s}", body2);
    char *resp3=NULL;
    tu_pdb_request(tc, req2, &resp3); shard_db_free_result(resp3);

    /* fetching=1 (find), both broad indexed → FP_INTERSECT (n_selective=0 path).
     * source_leaves[0] = "by"="bob" → broad → saturated → FP_ORDER_INDEX_WALK.
     * After Fix 2, count with all-broad falls through to single-seed
     * PRIMARY_LEAF; only the find path still hits FP_INTERSECT here. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); tc_val=-1;
    const char *k_d3b = plan_filter_kind_for_test(env.db_root, "default/d3b",
        "[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"bob\"},"
         "{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"x\"}]",
        "by", 1,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_d3b, "intersect", "D3: all-broad indexed find → INTERSECT");
    ASSERT_EQ_STR(o, "walk", "D3: broad seed + order_by → FP_ORDER_INDEX_WALK");
    ASSERT_EQ_INT(tc_val, 1, "D3: total_cheap=1 (INTERSECT materializes KeySet)");

    /* After Fix 2: count with all-broad falls through to single-seed
     * PRIMARY_LEAF instead of FP_INTERSECT. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); tc_val=-1;
    const char *k_d3b_cnt = plan_filter_kind_for_test(env.db_root, "default/d3b",
        "[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"bob\"},"
         "{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"x\"}]",
        "by", 0,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_d3b_cnt, "leaf",
        "D3 count: all-broad indexed count → PRIMARY_LEAF (not INTERSECT)");
    ASSERT_EQ_STR(f, "by",
        "D3 count: seed is most-selective indexed leaf");

    if (tu_pdb_drop_object(tc, "default", "d3") != 0) return 1;
    if (tu_pdb_drop_object(tc, "default", "d3b") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-d3-walk-order", test_planD3_walk_order)
```

### 4.18 — `test_plan_total_cheap`

No direct `tc_request`/`free` calls in this function — only R1 and R4 apply.

Anchor:

```c
static int test_plan_total_cheap(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "tc_obj",
        "\"tag:varchar:8\",\"note:varchar:16\"",
        "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "tc_obj");

    char f[64]={0}, o[32]={0};
    int cheap = -1;

    /* A1: selective btree leaf, no order_by → PRIMARY_LEAF → total_cheap=1 */
    const char *k1 = plan_filter_kind_for_test(env.db_root, "default/tc_obj",
        "{\"tag\":\"rare\"}", NULL, 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k1, "leaf", "total_cheap A1: kind=leaf");
    ASSERT_EQ_INT(cheap, 1, "total_cheap A1: total_cheap=1 (KeySet materialized)");

    /* A5: non-indexed → FULL_SCAN → total_cheap=0 */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); cheap=-1;
    const char *k5 = plan_filter_kind_for_test(env.db_root, "default/tc_obj",
        "[{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k5, "scan", "total_cheap A5: kind=scan");
    ASSERT_EQ_INT(cheap, 0, "total_cheap A5: total_cheap=0 (no KeySet)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-total-cheap", test_plan_total_cheap)
```

Replace with:

```c
static int test_plan_total_cheap(void) {
    TestEnv env={0};
    ShardDb *tc = cm_pdb_setup(&env, "tc_obj",
        "\"tag:varchar:8\",\"note:varchar:16\"",
        "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "tc_obj");

    char f[64]={0}, o[32]={0};
    int cheap = -1;

    /* A1: selective btree leaf, no order_by → PRIMARY_LEAF → total_cheap=1 */
    const char *k1 = plan_filter_kind_for_test(env.db_root, "default/tc_obj",
        "{\"tag\":\"rare\"}", NULL, 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k1, "leaf", "total_cheap A1: kind=leaf");
    ASSERT_EQ_INT(cheap, 1, "total_cheap A1: total_cheap=1 (KeySet materialized)");

    /* A5: non-indexed → FULL_SCAN → total_cheap=0 */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); cheap=-1;
    const char *k5 = plan_filter_kind_for_test(env.db_root, "default/tc_obj",
        "[{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k5, "scan", "total_cheap A5: kind=scan");
    ASSERT_EQ_INT(cheap, 0, "total_cheap A5: total_cheap=0 (no KeySet)");

    if (tu_pdb_drop_object(tc, "default", "tc_obj") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-total-cheap", test_plan_total_cheap)
```

### 4.19 — `test_planA3_trigram_starts_with`

Not to be confused with `test_a3_trigram_starts_with_executor` (excluded — see
"Corrected candidate list" above). This function has one direct
`tc_request`/`free` call site beyond `cm_setup`.

Anchor:

```c
static int test_planA3_trigram_starts_with(void) {
    TestEnv env = {0};
    /* trigram-only index on title: no btree */
    TestClient *tc = cm_setup(&env, "cm_a3",
        "\"title:varchar:64\"",
        "\"title:trigram\"");
    if (!tc) return 1;

    /* Insert 20 rows title="Show HN: <i>" and 80 unrelated rows. */
    char body[65536]; size_t p = 0; int k = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 20; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"title\":\"Show HN: item %d\"}",
            k == 0 ? "" : ",", k, i);
        k++;
    }
    for (int i = 0; i < 80; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"title\":\"Ask HN: something %d\"}", k, i);
        k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"cm_a3\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    char f[64] = {0}, o[16] = {0};
    /* Prefix "Show HN" is 7 chars >= 3 → trigram eligible.
     * Planner must return "leaf" (not "scan") and seed on "title". */
    const char *kind = plan_filter_kind_for_test(env.db_root, "default/cm_a3",
        "[{\"field\":\"title\",\"op\":\"starts_with\",\"value\":\"Show HN\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind, "leaf",
        "A3: starts_with on trigram-only field (prefix>=3) → PRIMARY_LEAF, not FULL_SCAN");
    ASSERT_EQ_STR(f, "title",
        "A3: seed field is 'title'");

    /* Also verify fetching=0 (count path) — same result. */
    memset(f, 0, sizeof(f)); memset(o, 0, sizeof(o));
    const char *kind_count = plan_filter_kind_for_test(env.db_root, "default/cm_a3",
        "[{\"field\":\"title\",\"op\":\"starts_with\",\"value\":\"Show HN\"}]",
        NULL, 0, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind_count, "leaf",
        "A3 count: starts_with on trigram-only field stays leaf on count path too");

    /* Short prefix (<3 chars) must still fall to scan — no trigram extractable. */
    memset(f, 0, sizeof(f)); memset(o, 0, sizeof(o));
    const char *kind_short = plan_filter_kind_for_test(env.db_root, "default/cm_a3",
        "[{\"field\":\"title\",\"op\":\"starts_with\",\"value\":\"Sh\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind_short, "scan",
        "A3 short: prefix <3 chars → no usable trigram → FULL_SCAN");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-a3-trigram-starts-with", test_planA3_trigram_starts_with)
```

Replace with:

```c
static int test_planA3_trigram_starts_with(void) {
    TestEnv env = {0};
    /* trigram-only index on title: no btree */
    ShardDb *tc = cm_pdb_setup(&env, "cm_a3",
        "\"title:varchar:64\"",
        "\"title:trigram\"");
    if (!tc) return 1;

    /* Insert 20 rows title="Show HN: <i>" and 80 unrelated rows. */
    char body[65536]; size_t p = 0; int k = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 20; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"title\":\"Show HN: item %d\"}",
            k == 0 ? "" : ",", k, i);
        k++;
    }
    for (int i = 0; i < 80; i++) {
        SB_APPEND(body, p, sizeof(body),
            ",\"k%d\":{\"title\":\"Ask HN: something %d\"}", k, i);
        k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"cm_a3\","
        "\"records\":%s}", body);
    tu_pdb_request(tc, req, &resp); shard_db_free_result(resp); resp = NULL;

    char f[64] = {0}, o[16] = {0};
    /* Prefix "Show HN" is 7 chars >= 3 → trigram eligible.
     * Planner must return "leaf" (not "scan") and seed on "title". */
    const char *kind = plan_filter_kind_for_test(env.db_root, "default/cm_a3",
        "[{\"field\":\"title\",\"op\":\"starts_with\",\"value\":\"Show HN\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind, "leaf",
        "A3: starts_with on trigram-only field (prefix>=3) → PRIMARY_LEAF, not FULL_SCAN");
    ASSERT_EQ_STR(f, "title",
        "A3: seed field is 'title'");

    /* Also verify fetching=0 (count path) — same result. */
    memset(f, 0, sizeof(f)); memset(o, 0, sizeof(o));
    const char *kind_count = plan_filter_kind_for_test(env.db_root, "default/cm_a3",
        "[{\"field\":\"title\",\"op\":\"starts_with\",\"value\":\"Show HN\"}]",
        NULL, 0, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind_count, "leaf",
        "A3 count: starts_with on trigram-only field stays leaf on count path too");

    /* Short prefix (<3 chars) must still fall to scan — no trigram extractable. */
    memset(f, 0, sizeof(f)); memset(o, 0, sizeof(o));
    const char *kind_short = plan_filter_kind_for_test(env.db_root, "default/cm_a3",
        "[{\"field\":\"title\",\"op\":\"starts_with\",\"value\":\"Sh\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind_short, "scan",
        "A3 short: prefix <3 chars → no usable trigram → FULL_SCAN");

    if (tu_pdb_drop_object(tc, "default", "cm_a3") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-a3-trigram-starts-with", test_planA3_trigram_starts_with)
```

## Task 5 — bespoke conversion of `test_planD3_single_leaf_indexed_order`

This case does not use `cm_setup` — it does its own manual
`test_env_start`/`tc_connect` setup, structurally identical to what
`cm_setup` looked like *before* Task 3's rewrite. The conversion mirrors
Task 3's `cm_setup` pattern exactly: replace the daemon-spawn/connect block
with `test_get_process_db()`, populate `env.db_root` from
`test_get_process_db_root()` (the original never populates it itself — the
daemon fills it in via `test_env_start`, and `plan_filter_kind_for_test`
depends on it downstream), rename the three `tc_request` calls to
`tu_pdb_request`, rename the three `free(resp)` calls to
`shard_db_free_result(resp)`, and delete the trailing
`tc_close(tc); test_env_stop(&env);` line.

Anchor:

```c
static int test_planD3_single_leaf_indexed_order(void) {
    TestEnv env={0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    /* add-dir */
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* create object: score + time indexed, note NOT indexed */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"d3sl\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"score:int\",\"time:long\",\"note:varchar:32\"],"
        "\"indexes\":[\"score\",\"time\"]}",
        &resp); free(resp); resp = NULL;

    /* insert 250 rows: score=100, time=i, note="x" */
    char body[65536]; size_t bp = 0; int ki = 0;
    SB_APPEND(body, bp, sizeof(body), "{");
    for (int i = 0; i < 250; i++) {
        SB_APPEND(body, bp, sizeof(body),
            "%s\"r%d\":{\"score\":\"100\",\"time\":\"%d\",\"note\":\"x\"}",
            ki == 0 ? "" : ",", ki, i);
        ki++;
    }
    SB_APPEND(body, bp, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d3sl\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    char f[64]={0}, o[32]={0};
    int cheap = -1;

    /* (a) order_by="time" (indexed btree) — demotion must be suppressed.
     *   Expected: kind="leaf" (PRIMARY_LEAF, NOT demoted to scan),
     *             out_order="walk" (saturated seed → ORDER_INDEX_WALK). */
    const char *k_a = plan_filter_kind_for_test(env.db_root, "default/d3sl",
        "[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}]",
        "time", 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k_a, "leaf",
        "D3 single-leaf: indexed order_by suppresses B5 demotion → PRIMARY_LEAF");
    ASSERT_EQ_STR(o, "walk",
        "D3 single-leaf: saturated seed + indexed order_by → ORDER_INDEX_WALK");

    /* (b) order_by="note" (NOT indexed) — demotion must still fire → FULL_SCAN.
     *   Regression lock: no indexed order_by means B5 should demote as before. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); cheap = -1;
    const char *k_b = plan_filter_kind_for_test(env.db_root, "default/d3sl",
        "[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}]",
        "note", 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k_b, "scan",
        "D3 single-leaf dual: unindexed order_by → B5 demotion still fires → FULL_SCAN");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-d3-single-leaf-indexed-order", test_planD3_single_leaf_indexed_order)
```

Replace with:

```c
static int test_planD3_single_leaf_indexed_order(void) {
    TestEnv env={0};
    ShardDb *tc = test_get_process_db();
    if (!tc) { ASSERT_TRUE(0, "process db"); return 1; }
    snprintf(env.db_root, sizeof(env.db_root), "%s", test_get_process_db_root());

    char *resp = NULL;
    /* add-dir */
    tu_pdb_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    shard_db_free_result(resp); resp = NULL;

    /* create object: score + time indexed, note NOT indexed */
    tu_pdb_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"d3sl\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"score:int\",\"time:long\",\"note:varchar:32\"],"
        "\"indexes\":[\"score\",\"time\"]}",
        &resp); shard_db_free_result(resp); resp = NULL;

    /* insert 250 rows: score=100, time=i, note="x" */
    char body[65536]; size_t bp = 0; int ki = 0;
    SB_APPEND(body, bp, sizeof(body), "{");
    for (int i = 0; i < 250; i++) {
        SB_APPEND(body, bp, sizeof(body),
            "%s\"r%d\":{\"score\":\"100\",\"time\":\"%d\",\"note\":\"x\"}",
            ki == 0 ? "" : ",", ki, i);
        ki++;
    }
    SB_APPEND(body, bp, sizeof(body), "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d3sl\","
        "\"records\":%s}", body);
    tu_pdb_request(tc, req, &resp); shard_db_free_result(resp); resp = NULL;

    char f[64]={0}, o[32]={0};
    int cheap = -1;

    /* (a) order_by="time" (indexed btree) — demotion must be suppressed.
     *   Expected: kind="leaf" (PRIMARY_LEAF, NOT demoted to scan),
     *             out_order="walk" (saturated seed → ORDER_INDEX_WALK). */
    const char *k_a = plan_filter_kind_for_test(env.db_root, "default/d3sl",
        "[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}]",
        "time", 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k_a, "leaf",
        "D3 single-leaf: indexed order_by suppresses B5 demotion → PRIMARY_LEAF");
    ASSERT_EQ_STR(o, "walk",
        "D3 single-leaf: saturated seed + indexed order_by → ORDER_INDEX_WALK");

    /* (b) order_by="note" (NOT indexed) — demotion must still fire → FULL_SCAN.
     *   Regression lock: no indexed order_by means B5 should demote as before. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); cheap = -1;
    const char *k_b = plan_filter_kind_for_test(env.db_root, "default/d3sl",
        "[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}]",
        "note", 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k_b, "scan",
        "D3 single-leaf dual: unindexed order_by → B5 demotion still fires → FULL_SCAN");

    if (tu_pdb_drop_object(tc, "default", "d3sl") != 0) return 1;
    return 0;
}
TEST_REGISTER("test-plan-d3-single-leaf-indexed-order", test_planD3_single_leaf_indexed_order)
```

Note: `env.db_root` is a fixed-size buffer (`TestEnv.db_root`, see
`src/test/fixtures.h`); `test_get_process_db_root()` returns the process-local
tmpdir path created once by `test_init_process_db()`, which is well within
that buffer's size in every test-runner invocation.

## Task 6 — bespoke conversion of `test_topn_eligible_truth_table`

`src/test/cases/test_agg_topn_stream.c` contains several test cases; only
`test_topn_eligible_truth_table` (the sole setup-only case flagged by the
audit — it calls `eligible_for_topn_stream()` directly against `env.db_root`
after a one-record insert, with no further `tc_request` calls that assert on
wire-response content) is in scope. The other registered cases in this file
(`test_topn_stream_count_no_criteria` and its neighbors) make real
`tc_request(..., "mode":"aggregate", ...)` calls and assert on the JSON
response body — they stay on TCP and are untouched by this plan.

Like Task 5's target, this case builds its own daemon connection manually
rather than via `cm_setup`. Convert with the same mechanical substitution:
`test_get_process_db()` replaces the spawn/connect block,
`test_get_process_db_root()` populates `env.db_root`, `tu_pdb_request`
replaces `tc_request`, `shard_db_free_result` replaces `free`, and the
trailing `tc_close(tc); test_env_stop(&env);` is deleted.

Anchor:

```c
static int test_topn_eligible_truth_table(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        ASSERT_TRUE(0, "daemon spawn");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    /* Register the "default" tenant dir at runtime. */
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp);
    resp = NULL;

    /* Create object with fields + immediately-declared indexes. This creates
       index.conf with the index entries at object creation time, before any
       inserts trigger btree file allocation. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"topn_elig\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"name:varchar:32\",\"score:int\",\"score_unindexed:int\"],"
        "\"indexes\":[\"name\",\"score\"]}", &resp);
    free(resp);
    resp = NULL;

    /* Insert one record to trigger btree files. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"topn_elig\",\"key\":\"k1\",\"value\":{\"name\":\"a\",\"score\":1,\"score_unindexed\":0}}", &resp);
    free(resp); resp = NULL;

    /* AGG_COUNT enum value — from query.c enum AggFn { AGG_COUNT=0, ... } */
    const int AGG_COUNT_VAL = 0;
    const int AGG_SUM_VAL = 1;

    /* Verify db_root + object exist */
    ASSERT_NOT_NULL(env.db_root, "env.db_root is set");

    /* The shape checks: eligibility gating on input shape alone */

    /* The object path includes the dir: "default/topn_elig" */
    const char *obj_path = "default/topn_elig";

    /* Case A: minimal eligible shape - count() + single-field group_by + limit */
    TestAggSpec spec_a;
    spec_a.fn = AGG_COUNT_VAL;
    spec_a.field[0] = '\0';
    strcpy(spec_a.alias, "n");
    int r = eligible_for_topn_stream(env.db_root, obj_path,
                                      &spec_a, 1, "name", "n", 20, NULL);
    ASSERT_EQ_INT(r, 1, "Case A: count + indexed group_by + order_by agg alias + limit → ELIGIBLE");

    /* Case B: no limit → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 0, NULL);
    ASSERT_EQ_INT(r, 0, "Case B: no limit (0) → NOT ELIGIBLE");

    /* Case C: order_by on group_by field instead of agg alias → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "name", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case C: order_by on group_by field → NOT ELIGIBLE");

    /* Case D: group_by on un-indexed field → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "score_unindexed", "n", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case D: un-indexed group_by → NOT ELIGIBLE");

    /* Case E: multi-field group_by → NOT ELIGIBLE in Phase 1 */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name,score", "n", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case E: multi-field group_by (Phase 1 limit) → NOT ELIGIBLE");

    /* Case F: sum on group_by field itself + order_by on sum alias → ELIGIBLE */
    TestAggSpec spec_f;
    spec_f.fn = AGG_SUM_VAL;
    strcpy(spec_f.field, "score");  /* sum on the group_by field itself */
    strcpy(spec_f.alias, "total");
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_f, 1, "score", "total", 10000, NULL);
    ASSERT_EQ_INT(r, 1, "Case F: sum on group_by field + limit=max → ELIGIBLE");

    /* Case G: limit > 10000 → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 10001, NULL);
    ASSERT_EQ_INT(r, 0, "Case G: limit > 10000 → NOT ELIGIBLE");

    /* Case H: sum on different field → NOT ELIGIBLE in Phase 1 */
    TestAggSpec spec_h;
    spec_h.fn = AGG_SUM_VAL;
    strcpy(spec_h.field, "score_unindexed");  /* NOT the group_by field */
    strcpy(spec_h.alias, "total");
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_h, 1, "name", "total", 10000, NULL);
    ASSERT_EQ_INT(r, 0, "Case H: agg on non-group_by field (Phase 1 limit) → NOT ELIGIBLE");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-topn-eligible-truth-table", test_topn_eligible_truth_table)
```

Replace with:

```c
static int test_topn_eligible_truth_table(void) {
    TestEnv env = {0};
    ShardDb *tc = test_get_process_db();
    ASSERT_NOT_NULL(tc, "process db");
    if (!tc) return 1;
    snprintf(env.db_root, sizeof(env.db_root), "%s", test_get_process_db_root());

    /* Register the "default" tenant dir at runtime. */
    char *resp = NULL;
    tu_pdb_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    shard_db_free_result(resp);
    resp = NULL;

    /* Create object with fields + immediately-declared indexes. This creates
       index.conf with the index entries at object creation time, before any
       inserts trigger btree file allocation. */
    tu_pdb_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"topn_elig\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"name:varchar:32\",\"score:int\",\"score_unindexed:int\"],"
        "\"indexes\":[\"name\",\"score\"]}", &resp);
    shard_db_free_result(resp);
    resp = NULL;

    /* Insert one record to trigger btree files. */
    tu_pdb_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"topn_elig\",\"key\":\"k1\",\"value\":{\"name\":\"a\",\"score\":1,\"score_unindexed\":0}}", &resp);
    shard_db_free_result(resp); resp = NULL;

    /* AGG_COUNT enum value — from query.c enum AggFn { AGG_COUNT=0, ... } */
    const int AGG_COUNT_VAL = 0;
    const int AGG_SUM_VAL = 1;

    /* Verify db_root + object exist */
    ASSERT_NOT_NULL(env.db_root, "env.db_root is set");

    /* The shape checks: eligibility gating on input shape alone */

    /* The object path includes the dir: "default/topn_elig" */
    const char *obj_path = "default/topn_elig";

    /* Case A: minimal eligible shape - count() + single-field group_by + limit */
    TestAggSpec spec_a;
    spec_a.fn = AGG_COUNT_VAL;
    spec_a.field[0] = '\0';
    strcpy(spec_a.alias, "n");
    int r = eligible_for_topn_stream(env.db_root, obj_path,
                                      &spec_a, 1, "name", "n", 20, NULL);
    ASSERT_EQ_INT(r, 1, "Case A: count + indexed group_by + order_by agg alias + limit → ELIGIBLE");

    /* Case B: no limit → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 0, NULL);
    ASSERT_EQ_INT(r, 0, "Case B: no limit (0) → NOT ELIGIBLE");

    /* Case C: order_by on group_by field instead of agg alias → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "name", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case C: order_by on group_by field → NOT ELIGIBLE");

    /* Case D: group_by on un-indexed field → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "score_unindexed", "n", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case D: un-indexed group_by → NOT ELIGIBLE");

    /* Case E: multi-field group_by → NOT ELIGIBLE in Phase 1 */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name,score", "n", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case E: multi-field group_by (Phase 1 limit) → NOT ELIGIBLE");

    /* Case F: sum on group_by field itself + order_by on sum alias → ELIGIBLE */
    TestAggSpec spec_f;
    spec_f.fn = AGG_SUM_VAL;
    strcpy(spec_f.field, "score");  /* sum on the group_by field itself */
    strcpy(spec_f.alias, "total");
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_f, 1, "score", "total", 10000, NULL);
    ASSERT_EQ_INT(r, 1, "Case F: sum on group_by field + limit=max → ELIGIBLE");

    /* Case G: limit > 10000 → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 10001, NULL);
    ASSERT_EQ_INT(r, 0, "Case G: limit > 10000 → NOT ELIGIBLE");

    /* Case H: sum on different field → NOT ELIGIBLE in Phase 1 */
    TestAggSpec spec_h;
    spec_h.fn = AGG_SUM_VAL;
    strcpy(spec_h.field, "score_unindexed");  /* NOT the group_by field */
    strcpy(spec_h.alias, "total");
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_h, 1, "name", "total", 10000, NULL);
    ASSERT_EQ_INT(r, 0, "Case H: agg on non-group_by field (Phase 1 limit) → NOT ELIGIBLE");

    if (tu_pdb_drop_object(tc, "default", "topn_elig") != 0) return 1;
    return 0;
}

TEST_REGISTER("test-topn-eligible-truth-table", test_topn_eligible_truth_table)
```

This file already `#include "../fixtures.h"` (line 6), and Task 2 adds
`#include "../db/shard_db.h"` to `fixtures.h` — so `ShardDb` is already
visible here with no new include needed. It only needs the two new externs.

Anchor (`src/test/cases/test_agg_topn_stream.c`, immediately after the
existing `topn_heap_*` externs, before the `TestAggSpec` forward-decl
comment):

```c
extern int   topn_heap_drain(void *h, double *metrics_out,
                              char **gkeys_out, size_t *gklens_out,
                              int64_t *counts_out, double *sums_out,
                              double *mins_out, double *maxs_out);

/* Forward decl of AggSpec — test's version must be compatible on
```

Replace with:

```c
extern int   topn_heap_drain(void *h, double *metrics_out,
                              char **gkeys_out, size_t *gklens_out,
                              int64_t *counts_out, double *sums_out,
                              double *mins_out, double *maxs_out);

extern ShardDb *test_get_process_db(void);
extern const char *test_get_process_db_root(void);

/* Forward decl of AggSpec — test's version must be compatible on
```

## Task 7 — build and verify

1. Build: `SKIP_TESTS=1 ./build.sh`. Must complete with **no new compiler
   warnings** relative to a build of `main` at the same commit (diff the
   warning output if any appear).
2. Full suite: `./build/bin/shard-db-test run-all`. Must report the same
   total pass count and same total assertion count as the baseline captured
   in **Task 0** before any edits in this plan were made (do not re-capture
   the baseline here — by this point the working tree already has Tasks
   1-6's changes, so a fresh `main` checkout is no longer available without
   stashing; Task 0's numbers are the only valid comparison point).
3. Targeted re-run of every converted case by name, to isolate failures to
   this change rather than unrelated suite flakiness:
   ```bash
   for t in test-cost-selectivity-primitive test-plan-a1-selective-leaf \
            test-plan-a2-broad-bitmap test-plan-a5-nonindexed-scan \
            test-plan-b1-two-selective-btree \
            test-plan-b2-selective-btree-broad-bitmap \
            test-plan-b3-two-broad-bitmaps \
            test-plan-b4-selective-btree-nonindexed \
            test-plan-b7-all-nonindexed \
            test-plan-a4-saturated-trigram-stays-leaf \
            test-plan-bcs-count-one-selective-leaf \
            test-plan-c1-pure-or-all-indexed \
            test-plan-c2-or-with-nonindexed-child \
            test-plan-c3-and-leaf-plus-or-subtree \
            test-plan-d1-composite-order \
            test-plan-d2-sort-order \
            test-plan-d3-walk-order \
            test-plan-total-cheap \
            test-plan-a3-trigram-starts-with \
            test-plan-d3-single-leaf-indexed-order \
            test-topn-eligible-truth-table; do
     if ! ./build/bin/shard-db-test run "$t"; then
       echo "FAILED: $t"
       exit 1
     fi
   done
   ```
   `|| echo "FAILED: $t"` would only print and continue to the next case —
   the `exit 1` form above is required so a genuine failure actually stops
   the loop immediately, matching the plan's halt rule below rather than
   silently running the remaining cases and losing which one failed first.
   Any failure here halts execution per the plan's halt rule (write
   `PLAN_NOTES.md`, stop, do not improvise a fix).
4. Confirm the 19 converted case functions in `test_planner_cost_model.c`
   (plus `test_planD3_single_leaf_indexed_order` in the same file, converted
   by Task 5) no longer reference TCP-daemon machinery. This check must be
   scoped to those 20 functions specifically — it can never be "zero matches
   in the whole file", because `cm_setup()` (kept, TCP-only, Task 3) and
   `test_a3_trigram_starts_with_executor` (deliberately excluded from this
   plan) both legitimately still use `TestClient`/`tc_connect`/`tc_close`/
   `test_env_start`/`test_env_stop`/`TestClientCfg`:
   ```bash
   awk '/^static (int|TestClient) test_a3_trigram_starts_with_executor|^static TestClient \*cm_setup\(/{skip=1}
        /^}/{if(skip){skip=0; next}}
        !skip' src/test/cases/test_planner_cost_model.c \
     | grep -n 'test_env_start\|tc_connect\|tc_close\|test_env_stop\|TestClientCfg\|TestClient '
   ```
   must show **zero** matches (this filters out `cm_setup()`'s body and
   `test_a3_trigram_starts_with_executor`'s body, then greps everything
   else — the 19 Task-4 functions, `cm_pdb_setup`, `cm_insert_tags`, and
   Task 5's function). If the `awk` skip-region approach doesn't cleanly
   bracket both excluded functions (e.g. due to reformatting), fall back to
   manually reading each of the 20 converted functions and confirming by
   eye that none reference TCP machinery — do not weaken this to a
   whole-file grep that would silently pass with `cm_setup()`'s legitimate
   references included.

   For `test_agg_topn_stream.c`, the same grep (no `awk` filtering needed,
   since this file has no shared TCP-only helper to exclude) must show
   matches **only** inside `test_topn_stream_count_no_criteria` and the
   other untouched TCP-based cases — never inside
   `test_topn_eligible_truth_table`.
5. Confirm `test-a3-trigram-starts-with-executor` (the excluded, correctly-
   TCP-based case) is unaffected: run it by name and confirm it still passes
   unchanged.
6. Run the parallel path specifically, since `test_init_process_db()` is
   invoked once per forked child: `./build/bin/shard-db-test run-all --jobs 4`
   (or whatever `--jobs` value the harness defaults to) to confirm no
   cross-case interference from process-local `ShardDb` reuse under forking.
7. Run the **sequential** path explicitly:
   `./build/bin/shard-db-test run-all --jobs 1`. This is the path where
   every case in the entire binary — not just the 21 converted here —
   shares one process-wide `ShardDb` and tmpdir (`run_all_sequential()`,
   `test_runner.c:96-113`; `test_init_process_db()` no-ops after the first
   case). This is the scenario Task 2's `tu_pdb_drop_object()` teardown
   exists for — confirm this run reports the same pass/assertion counts as
   the Task 0 baseline's `--jobs 1` run, not just the (already-covered)
   parallel run in step 6.
8. Confirm the config invariant from the Background section still holds at
   verification time: `grep RANDOM_SEQ_COST_RATIO db.env` from the repo
   root must show `RANDOM_SEQ_COST_RATIO=8`. If it shows a different value
   (or is absent), stop — the "Invariant: config knobs" section's
   both-paths-converge-on-8 argument no longer holds, and the 21 converted
   cases' cost-model assertions must be re-verified against the new value
   before this plan can be considered complete (write `PLAN_NOTES.md`
   describing the mismatch per the halt rule, do not silently proceed).

## Definition of done

- [ ] `SKIP_TESTS=1 ./build.sh` succeeds with no new compiler warnings.
- [ ] `./build/bin/shard-db-test run-all` reports identical pass/assertion
      counts before and after this change.
- [ ] All 21 targeted cases (listed in Task 7 step 3) pass individually.
- [ ] `test-a3-trigram-starts-with-executor` still passes unchanged and is
      confirmed to remain TCP-based (not touched by this diff).
- [ ] Parallel (`--jobs`) run passes with no new failures.
- [ ] Sequential (`--jobs 1`) run passes with pass/assertion counts matching
      the Task 0 `--jobs 1` baseline — this is the path where converted
      cases share one process-wide `ShardDb`/tmpdir for the whole suite,
      and is what `tu_pdb_drop_object()` teardown exists to protect.
- [ ] Repo-root `db.env`'s `RANDOM_SEQ_COST_RATIO` still matches the value
      recorded in Task 0 (still `8` as of this writing).
- [ ] `git diff` touches only: `src/db/embedded.c`, `src/test/fixtures.h`,
      `src/test/fixtures.c`, `src/test/cases/test_planner_cost_model.c`,
      `src/test/cases/test_agg_topn_stream.c`. No unrelated files changed.
- [ ] No leftover `TestClient`/`TestClientCfg`/`tc_connect`/`tc_close`/
      `test_env_start`/`test_env_stop` references remain in any of the 21
      converted functions.
- [ ] Work is left **uncommitted** on the fresh branch, per this repo's
      standing execution-mode exception, for the raw-diff review pass.

## Post-implementation review evidence — 2026-07-16

This evidence was reconstructed during review because the executor did not
capture Task 0 before editing. It proves behavioral parity against the exact
base commit, but it does not claim that the original execution order complied
with Task 0.

Base under review: `17f1786129c5e3c4350bdbcac0111c6ebc5708fb`
(`origin/main`). The base was exported with `git archive` into an isolated
directory under `/tmp`; the feature worktree was not stashed or modified.

| Check | Exact base commit | Final feature worktree |
|---|---:|---:|
| `SKIP_TESTS=1 ./build.sh` | passed, no compiler warnings | passed, no compiler warnings |
| `./build/bin/shard-db-test run-all` | 10,230 passed, 0 failed, 282 cases | four captured runs at 10,230 passed, 0 failed, 282 cases; see intermittent-run note below |
| `./build/bin/shard-db-test run-all --jobs 1` | 10,230 passed, 0 failed, 282 cases | 10,230 passed, 0 failed, 282 cases |

Additional final-worktree checks:

- All 21 converted cases passed individually with the plan's fail-fast loop.
- `test-a3-trigram-starts-with-executor` remained TCP-backed and passed all
  13 assertions.
- `run-all --jobs 4` passed 10,230 assertions across 282 cases.
- `grep RANDOM_SEQ_COST_RATIO db.env` reported
  `export RANDOM_SEQ_COST_RATIO=8`.
- `git diff --check` reported no whitespace errors.

One additional final-worktree default run reported 10,229 passed and one
failed assertion. Its streamed output was truncated before the failing case
name was retained. A subsequent captured run and three time-boxed captured
attempts all passed 10,230 assertions; the failure did not recur, so there was
no case-level feedback loop to diagnose. The red run remains recorded here as
an intermittent suite caveat rather than being treated as resolved by reruns.

The initial edits were made on `main` before the required feature branch was
created. Review moved the uncommitted worktree to
`feat/tcp-to-direct-call-test-conversion`, whose base is the exact commit above.
That historical ordering lapse cannot be retroactively changed; it is retained
here explicitly rather than represented as a successful pre-edit Task 0 run.
