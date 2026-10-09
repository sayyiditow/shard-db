# Plan: lift the fields.conf field-definition length cap (long enum lists)

**Date:** 2026-10-09
**Status:** awaiting human approval (CORE-PROCESS step 1 → 2).
Revised 2026-10-09 per plan review: (1) `conf_read_line` checks the
accumulated length in the EOF branch too, so an unterminated over-cap final
line fails EOVERFLOW instead of returning; (2) `load_typed_schema` frees
earlier lines' enum value lists before its fail-closed return; (3) the
regression test returns early on failed reconnects instead of dereferencing
a null client in later blocks; (4) object A declares `status:bitmap`, making
the "indexed find" regression genuinely indexed.

Executed 2026-10-09 (branch `fix/long-field-defs`, uncommitted). Execution-time
corrections, all covered by the plan's strategy but missing from / wrong in
its task blocks:

1. **build.sh registration** — the C test list in `build.sh` is explicit, not
   globbed; the new case is registered next to `test_enum.c`.
2. **`parse_field_line` (query_find.c) heap copy** — listed in §1 G5 / §2 but
   missing from Task 5's hunks. Its 256-byte `clean_spec` truncated
   add-field specs at rebuild time ("Invalid field line"); now strdups the
   type portion. Found by execution, root-caused via a scratch daemon.
3. **GCC `cleanup` passes the ADDRESS of the variable** — proven by a
   standalone ASan experiment (bare `cleanup(free)` on a heap `char *`
   frees the stack slot). `free_field_specs` takes `char *(*specs)[MAX_FIELDS]`
   and the row-array sites use per-file `free_spec_lines(char (**lines)[MAX_FIELD_DEF])`
   which frees `*lines`. The task blocks' bare `cleanup(free)` was a real
   bad-free bug; the `-Wfree-nonheap-object` warnings were correct.
4. **Test-body corrections** — block E (over-cap probe) runs before restart #1
   (the plan placed it after the last daemon was stopped); describe-object
   renders type tokens, not value lists, so the "LAST enum value" assertion
   is now insert+get of value `0124` through the live enum; `enum_spec`
   asserts `nvalues ≤ 10^vlen` (`%0*d` is a *minimum* width — 5-digit values
   past 9999 overflowed the sizing budget; found by ASan) and the over-cap
   probe uses `vlen=5`.

Task 8 outputs (reviewer artifacts): base failure `/tmp/base-failure.txt`
(15 failed / 26 passed — block A rejected at the create gate, block B created
then silently dropped on restart, `{"n":"y"}` decode proving the corruption);
revert-proof `/tmp/revert-fail.txt` (16 failed / 26 passed with src stashed);
green `/tmp/fix-pass2.txt` (42/42); full suite 16,051 assertions across 462
cases, 0 failed.

Task 10 outputs: ASan+UBSan (strict, `-fno-sanitize-recover=undefined`) —
3 × full suite, 16,051/462, 0 findings. TSan — the first run under the bare
AGENTS.md command was watchdog-aborted at 180 s on `test-auto-reshard-throttle`
(0 ThreadSanitizer reports up to the abort); the gate was then run with the
tsan.yml workflow's documented configuration (`SHARD_TEST_WATCHDOG_SEC=1200`,
`--exclude` of its 11 fsync/timing-heavy cases): 3 × full suite, 15,914/451,
0 findings, exit 0.

Round-2 review (2026-10-09, same day) — three findings, two accepted:

1. **Accepted — unchecked `calloc` in the two server.c wire handlers.**
   Both add-field and edit-field now return `{"error":"out of memory"}`
   before parsing when the 16 MiB row allocation fails.
2. **Accepted — `rebuild_object_v2`'s fields.conf staging rewrite still
   split long lines** (`fgets(line[512])` at query_find.c, missed by the
   §1 sweep because it was filed under the rebuild engine, not the config
   readers). Now uses `conf_read_line` + fail-closed (unlinks the staged
   `.new`, aborts the rebuild via the existing txn_fail path). The
   regression test's block C now adds a field to object A — whose existing
   status line is 637 bytes — so the staging rewrite is exercised; proven
   load-bearing by temporarily capping that reader at 512 (test fails
   2/43: add-field refuses, added field absent post-restart), then
   restoring (43/43). Final sweep after the fix: no remaining
   `fields.conf` reader uses fixed-size `fgets` (`query_find.c:558` and
   `query_maint.c:506` are schema.conf readers — short lines, out of
   scope; `load_fields_conf` remains dead code).
3. **Rejected — "over-cap probe exceeds the daemon's request limit."**
   The premise breaks at `db_defaults_set()` (embedded.c:47), which sets
   `max_request_size = 33554432` before db.env parsing; a db.env without
   the knob keeps the 32 MB default, and config.c rejects out-of-range
   values (keeping the default), so server.c's `: MAX_LINE` fallback is
   unreachable via configuration. Empirically consistent: the ~84 KB
   block-E request reached create-object's own cap error on every build
   (normal, ASan, TSan). A test comment now cites the dependency.

Gates were rerun on the updated diff: regression 43/43, full suite
16,052/462, 0 failed; ASan+UBSan and TSan runs listed in the execution log.

Round-3 review (2026-10-09) — one finding, accepted (Medium):
`:default=` literals beyond `TypedField.default_val`'s 255-byte capacity
were accepted and silently truncated at parse/insert time, and an
edit-field rewrite that couldn't carry such an on-disk modifier dropped
it silently. (The truncation predates this branch; lifting the spec cap
made it reachable on longer specs, and the plan's edge-case note wrongly
claimed the cap was already documented.) Fix — reject at the wire rather
than heap-ify `default_val` (TypedField is value-copied across the
rebuild engine; a heap member would be a lifetime audit):

- `field_default_too_long()` helper (config.c, decl in types.h); gates in
  create-object, `cmd_add_fields`, and `cmd_edit_fields` reject with
  `"default= literal too long (max 255 bytes)"`.
- `rewrite_fields_conf_for_edit` refuses the whole rewrite (unlinks the
  staged `.new`, error response) when a matched line carries a modifier
  the carry buffer can't hold — detected by the new
  `line_has_default_modifier()` mirroring the carrier's grammar. Unmatched
  lines pass through verbatim and were never at risk.
- `parse_default_modifier` LOG_WARNs on any truncation (reachable only
  via older-binary/hand-edited lines now).
- limits.md row (`Default-literal length | 255 bytes`), typed-records.md
  defaults note, changelog sentence.

Regression coverage: test blocks F (three wire-gate rejections) and G
(hand-edited 300-byte on-disk default + edit of its own field must
refuse and leave the file intact). Pre-fix run: 5 failed / 48 passed
(`/tmp/r3-prefix-fail.txt` — all three gates accepted the long default;
the G edit dropped it from disk). Post-fix: 53/53; full suite 16,062/462;
sanitizer gates rerun on this diff.
**Branch:** `fix/long-field-defs` (created at execution start, off `main`)
**Execution mode:** per AGENTS.md standing exception — leave work **uncommitted**; reviewing agent + human review the raw `git diff`.

---

## 1. Bug and root cause

Users cannot create objects with large `enum(...)` value lists: 125 four-char
values (a 632-byte field definition) is rejected at create-object, even though
the enum type documents and implements up to 65,535 values.

**Root cause — one cap enforced at different, inconsistent points in the
fields.conf line pipeline.** A field definition (e.g.
`status:enum(v1,v2,...)`) travels: wire request → create/add/edit validation →
`fields.conf` on disk → re-parse at every schema load. Each hop has its own
independent fixed buffer, and they disagree:

| # | Site | Buffer | Effect |
|---|------|--------|--------|
| G1 | `server.c` add-field/edit-field handlers (anchor: `char lines[MAX_FIELDS][256];`) | `[MAX_FIELDS][256]`, `if (l > 0 && l < 256)` | specs ≥256 bytes **silently dropped** from the request |
| G2 | `query_schema.c` create-object (anchor: `char field_specs[MAX_FIELDS][512];`, gate `flen <= 0 || flen >= 511`) | 512 | explicit rejection ≥511 bytes — the reported repro point |
| G3 | `query_schema.c` `validate_field_type` (anchor: `char buf[512];`) | 512 | type portion truncated before validation — misleading errors, inconsistent with loader |
| G4 | `config.c` `load_typed_schema` (anchor: `char line[512];`) **and inner** `char type_spec[256];` | 512 / 256 | **the corruption point**: on every schema load, a line >511 bytes is split by `fgets` (tail becomes a phantom line, silently skipped) and the type portion is truncated to 255 bytes (truncated `enum(` loses its `)` → `parse_field_type` → `FT_NONE` → **field silently dropped**, every later field's offset shifts → all records decode wrong) |
| G5 | `config.c` `cmd_add_fields` (anchor: `char clean_spec[256];`) + `query_find.c` `parse_field_line` (anchor: `char clean_spec[256];`) | 256 | type portion truncated before parse on add/edit paths |
| G6 | `config.c` `cmd_remove_fields` / `cmd_rename_field` (anchor: `char lines[MAX_FIELDS][512];`) | 512 | read-whole-file-and-rewrite paths **truncate and split long lines while rewriting** — remove/rename/edit-field on an object that merely *contains* a long field def corrupts the schema |

Net effect: even below the 510-byte create gate (G2), a ~300–510-byte enum
field **creates fine and then corrupts on restart** (G4) — the reported
"schema corruption on restart" risk, reached at ~255 bytes of type portion,
not 510. The writer side (`fprintf(f, "%s\n", field_specs[i])`) is unbounded,
so wire gates and file readers disagree — that disagreement *is* the bug.

**Adjacent, deliberately out of scope** (noted for the reviewer, no changes):
`load_fields_conf` (`config.c`, `char fields[][256]`) has no callers in the
tree — dead legacy code, left untouched. `index.conf` readers
(`reindex_object_checked_impl` `char fline[512]`, `rename_indexes_for_field`
`char line[512], newline[512]`) hold index *names* (≤255) and sit exactly at
the composite-name edge (511 bytes) — separate concern, separate plan.
`embedded.c` `datetime_migration_has_marker` reads `fields.conf` with
`line[512]` but only does full-line equality against the short literal
`#datetime_7byte` — a split line can never false-match, benign.

## 2. Fix strategy

One shared cap and one shared reader, used by every hop:

- `#define MAX_FIELD_DEF 65536` in `types.h` — max bytes of one field
  definition (max spec length 65,535). 64 KiB fits ≈13k four-char enum values;
  worst-case per-request schema scratch is `MAX_FIELDS × MAX_FIELD_DEF` =
  16 MiB transient heap (calloc'd → lazily mapped; these are rare admin ops).
  `MAX_REQUEST_SIZE` default 32 MiB leaves ample headroom on the wire.
- `conf_read_line(FILE *, size_t max_len, int *err)` in `config.c` — portable
  growing-buffer line reader (fgets + doubling realloc; **no `getline`**, so no
  feature-macro regime questions on macOS — see the three macOS feature-macro
  fix commits currently at HEAD). Returns NULL with `*err = EOVERFLOW` when a
  line's content reaches `max_len` before its newline; **readers fail closed**
  on that (refuse to load/rewrite the schema) instead of silently misparsing —
  same philosophy as `default_modifiers_for_line`'s existing
  "refusing to truncate is intentional" behavior.
- All fixed-size spec arrays (`[256]`, `[512]`) become heap rows
  `char (*lines)[MAX_FIELD_DEF]` (calloc'd, `__attribute__((cleanup))`-freed
  where functions have many exits). `load_typed_schema` parses the type
  portion **in place** (nothing reuses the line afterwards); the
  add/edit-path parsers (`parse_field_line`, `cmd_add_fields`) keep parsing a
  **heap copy** because `parse_default_modifier` mutates the buffer and their
  callers re-use the original line for the fields.conf write.
- Over-cap specs on the wire get an explicit error naming the cap (replaces
  both the vague G2 message and the silent G1 drop — the silent drop becomes a
  hard error; that is an intentional, documented behavior change).

## 3. Consumers of every changed signature (complete call-site list)

- `rebuild_object` (`types.h:~1273`, def `query_find.c:1217`): called from
  `cmd_add_fields` (non-NULL lines) and `query_maint.c` vacuum heavy path
  (anchor: `return rebuild_object(db_root, object, new_splits, compact,` — passes `NULL, 0`). Tail-dispatches to `rebuild_object_v2` passing
  `added_lines` through.
- `rebuild_object_v2` (`query_internal.h:~128`): called from `query_schema.c`
  cmd_edit_fields finalize (anchor: `NULL /* added_lines */, 0 /* n_added */,`)
  and `embedded.c` datetime migration (anchor: `new_to_old, 1, 0, 0, NULL, 0,`).
  Both pass NULL — pointer-type change is source-compatible for them.
- `cmd_add_fields` (`types.h:~1353`, def `config.c:3577`): only caller
  `server.c` (anchor: `else cmd_add_fields(db_root, object, lines, nlines);`).
- `cmd_edit_fields` (`types.h:~1368`, def `query_schema.c:380`): only caller
  `server.c` (anchor: `else cmd_edit_fields(db_root, object, lines, nlines, allow_rename, dry);`).
- `rewrite_fields_conf_for_edit` (`query_schema.c:195`) + its ctx struct
  (locate via anchor `rewrite_fields_conf_for_edit(ctx->obj_dir, ctx->edit_lines,`): internal to query_schema.c.
- `validate_field_type`: static, single caller `query_schema.c` (anchor:
  `int field_size = validate_field_type(field_specs[nfields]);`).
- `conf_read_line`: new; callers are the fields.conf readers changed below.

## 4. Embedded execution rules

- Branch off `main`: `git checkout -b fix/long-field-defs`. Do tasks in order.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run-all`;
  single case: `./build/bin/shard-db-test run <name>`.
- If a quoted anchor isn't found exactly, write `PLAN_NOTES.md` describing the
  mismatch and halt the entire execution run immediately — do not guess,
  reinterpret, or continue to any further task, even an unrelated one.
- If you hit a decision the plan doesn't cover, stop and ask — do not improvise.
- Never weaken a test to make a failure disappear.
- No new compiler warnings (`-Wall -Wextra` is already in WARN_CFLAGS).

---

## Task 1 — Regression test (test-first; must fail on base)

Create `src/test/cases/test_long_field_def.c`. Naming mirrors the case file;
register at the file tail exactly like `test_enum.c` does
(anchor: `TEST_REGISTER("test-enum", test_enum_run)`).

```c
/* src/test/cases/test_long_field_def.c
 *
 * Field definitions larger than the legacy 510-byte create gate / 255-byte
 * loader type buffer: enum value lists that previously failed at create or,
 * worse, created fine and corrupted the schema on reload.
 *
 * Base-branch failure points (documented, per CORE-PROCESS):
 *   - 637-byte enum create → rejected ("invalid field definition").
 *   - 337-byte enum create succeeds on base, but after daemon restart the
 *     field is silently dropped by load_typed_schema's 255-byte type_spec
 *     truncation → get/describe assertions fail.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* "name:enum(0000,0001,...)" — nvalues values of vlen digits, ','-joined.
   125×4 → 637 bytes total; 65×4 → 337 bytes total (>255 type portion,
   <511 — passes the legacy create gate, corrupts the legacy loader). */
static char *enum_spec(const char *name, int nvalues, int vlen) {
    size_t cap = strlen(name) + (size_t)nvalues * ((size_t)vlen + 1) + 16;
    char *s = malloc(cap);
    ASSERT_NOT_NULL(s, "malloc enum spec");
    int off = snprintf(s, cap, "%s:enum(", name);
    for (int i = 0; i < nvalues && off >= 0; i++)
        off += snprintf(s + off, cap - (size_t)off, "%s%0*d",
                        i ? "," : "", vlen, i);
    if (off >= 0) snprintf(s + off, cap - (size_t)off, ")");
    return s;
}

static int test_long_field_def_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    char req[131072];

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"lfd\"}", &resp);
    free(resp); resp = NULL;

    /* === A: 637-byte enum (125 four-char values) — rejected on base at G2 === */
    char *spec_a = enum_spec("status", 125, 4);
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"lfd\",\"object\":\"a\","
             "\"splits\":8,\"max_key\":16,"
             "\"fields\":[\"%s\",\"name:varchar:32\"],"
             "\"indexes\":[\"status:bitmap\"]}", spec_a);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "A: long enum created");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"lfd\",\"object\":\"a\","
                   "\"key\":\"r1\",\"value\":{\"status\":\"0007\",\"name\":\"x\"}}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "A: insert r1");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"a\",\"key\":\"r1\"}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"0007\"", "A: get r1 pre-restart");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"lfd\",\"object\":\"a\","
                   "\"criteria\":{\"status\":\"0007\"}}", &resp);
    ASSERT_CONTAINS(resp, "0007", "A: indexed find pre-restart");
    free(resp); resp = NULL;
    free(spec_a);

    /* === B: 337-byte enum — created on base, dropped on base reload (G4) === */
    char *spec_b = enum_spec("state", 65, 4);
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"lfd\",\"object\":\"b\","
             "\"splits\":8,\"max_key\":16,"
             "\"fields\":[\"%s\",\"n:varchar:8\"]}", spec_b);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "B: mid-size enum created");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"lfd\",\"object\":\"b\","
                   "\"key\":\"q1\",\"value\":{\"state\":\"0042\",\"n\":\"y\"}}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "B: insert q1");
    free(resp); resp = NULL;
    free(spec_b);

    /* === restart #1: the loader must re-read what the writer wrote === */
    char saved_root[256]; int saved_port = env.port;
    snprintf(saved_root, sizeof(saved_root), "%s", env.db_root);
    tc_close(tc); tc = NULL;
    test_env_stop_keep(&env);

    TestEnv env2 = {0};
    ASSERT_EQ_INT(test_env_start_at(&env2, saved_root, saved_port), 0,
                  "daemon restart 1");
    TestClient *tc2 = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc2, "reconnect 1");
    if (!tc2) { test_env_stop(&env2); return 1; }
    {
        tc_request(tc2, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"a\",\"key\":\"r1\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"0007\"", "A: get r1 post-restart");
        free(resp); resp = NULL;

        tc_request(tc2, "{\"mode\":\"find\",\"dir\":\"lfd\",\"object\":\"a\","
                        "\"criteria\":{\"status\":\"0007\"}}", &resp);
        ASSERT_CONTAINS(resp, "0007", "A: indexed find post-restart");
        free(resp); resp = NULL;

        tc_request(tc2, "{\"mode\":\"describe-object\",\"dir\":\"lfd\",\"object\":\"a\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"type\":\"enum\"", "A: describe type=enum post-restart");
        ASSERT_CONTAINS(resp, "\"status:bitmap\"", "A: describe shows the declared index");
        ASSERT_CONTAINS(resp, "0124", "A: describe shows the LAST enum value (full list)");
        free(resp); resp = NULL;

        tc_request(tc2, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"b\",\"key\":\"q1\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"state\":\"0042\"", "B: get q1 post-restart (not dropped)");
        free(resp); resp = NULL;
    }

    /* === C: add-field with a >255-byte type portion (G1/G5) === */
    char *spec_tag = enum_spec("tag", 60, 4);   /* 313 bytes total */
    snprintf(req, sizeof(req),
             "{\"mode\":\"add-field\",\"dir\":\"lfd\",\"object\":\"b\","
             "\"fields\":[\"%s\"]}", spec_tag);
    tc_request(tc2, req, &resp);
    ASSERT_NOT_NULL(resp, "C: add-field responded");
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "C: add-field long enum succeeds");
    free(resp); resp = NULL;
    free(spec_tag);

    tc_request(tc2, "{\"mode\":\"insert\",\"dir\":\"lfd\",\"object\":\"b\","
                    "\"key\":\"q2\",\"value\":{\"state\":\"0043\",\"n\":\"z\","
                    "\"tag\":\"0005\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "C: insert uses the added field");
    free(resp); resp = NULL;

    /* === D: edit-field rewrite must pass a long sibling line through (G6) === */
    tc_request(tc2, "{\"mode\":\"edit-field\",\"dir\":\"lfd\",\"object\":\"a\","
                    "\"fields\":[\"name:varchar:64\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "D: edit-field sibling rewrite ok");
    free(resp); resp = NULL;

    tc_close(tc2); tc2 = NULL;
    test_env_stop_keep(&env2);

    /* === restart #2: add-field + edit-field rewrites must have been faithful === */
    TestEnv env3 = {0};
    ASSERT_EQ_INT(test_env_start_at(&env3, saved_root, saved_port), 0,
                  "daemon restart 2");
    TestClient *tc3 = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc3, "reconnect 2");
    if (!tc3) { test_env_stop(&env3); return 1; }
    {
        tc_request(tc3, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"b\",\"key\":\"q2\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"tag\":\"0005\"", "C: added enum survived restart");
        free(resp); resp = NULL;

        tc_request(tc3, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"a\",\"key\":\"r1\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"0007\"", "D: A's long enum survived the edit rewrite");
        free(resp); resp = NULL;

        tc_close(tc3);
    }
    test_env_stop(&env3);

    /* === E: over-cap spec → explicit error (not silently dropped) === */
    TestClient *tc4 = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc4, "reconnect 3");
    if (tc4) {
        char *spec_big = enum_spec("huge", 13999, 4);   /* ≈70 KB > 65,535 */
        snprintf(req, sizeof(req),
                 "{\"mode\":\"create-object\",\"dir\":\"lfd\",\"object\":\"c\","
                 "\"splits\":8,\"max_key\":16,\"fields\":[\"%s\"]}", spec_big);
        tc_request(tc4, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\"", "E: over-cap spec rejected");
        ASSERT_CONTAINS(resp, "longer than", "E: error names the cap");
        free(resp); resp = NULL;
        free(spec_big);
        tc_close(tc4);
    }

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-long-field-def", test_long_field_def_run)
```

> Executor note: the negative assertion in block C uses `ASSERT_TRUE(strstr(...) == NULL)`
> because `test_assert.h` has no negative-contains macro (verified: only
> `ASSERT_TRUE / ASSERT_EQ_INT / ASSERT_EQ_STR / ASSERT_CONTAINS / ASSERT_NOT_NULL`).
> Do not weaken assertions to pass.

**Prove it fails on base:** build `SKIP_TESTS=1 ./build.sh`, run
`./build/bin/shard-db-test run test-long-field-def`. Expected on base: A's
create assertion fails first ("invalid field definition (empty or too long)").
Paste the failure output into the execution log. Re-apply nothing yet —
continue to Task 2; the final green run comes in Task 8.

## Task 2 — Shared cap + shared reader

**`src/db/types.h`** — directly below the anchor line `#define MAX_LINE    65536`, insert:

```c
/* Max bytes of one field-definition spec (fields.conf line grammar:
   "name:type[:param][:default=...][:removed]"). This single cap must be
   enforced identically by the wire gates (create-object / add-field /
   edit-field) and by every fields.conf reader and rewriter: a spec
   accepted on the wire is written verbatim to fields.conf and re-read
   verbatim on every schema load, so a reader with a smaller line buffer
   than the wire gate corrupts the schema on reload. 64 KiB fits ~13k
   four-char enum values (the 65,535-value enum ceiling is storage-side;
   the line cap binds first for longer values). Worst-case per-request
   schema scratch is MAX_FIELDS * MAX_FIELD_DEF = 16 MiB transient heap. */
#define MAX_FIELD_DEF 65536
```

**`src/db/types.h`** — directly below the anchor line
`void free_enum_values(TypedField *f);` insert (add `#include <stdio.h>` at
the top of types.h if not already present):

```c
/* Read one '\n'-terminated line from fp into a malloc'd buffer.
   Returns the line without its trailing '\n' (caller frees), or NULL:
   clean EOF sets *err = 0, a read error sets *err = errno, and a line
   whose content reaches max_len before its newline sets *err = EOVERFLOW
   (the rest of the offending line is consumed). Never returns a line of
   max_len bytes or longer. Deliberately not getline(): no feature-macro
   regime questions on macOS. */
char *conf_read_line(FILE *fp, size_t max_len, int *err);
```

**`src/db/config.c`** — directly above the anchor line
`TypedSchema *load_typed_schema(const char *db_root, const char *object) {`, insert:

```c
/* Read one '\n'-terminated line into a malloc'd buffer (see types.h).
   Growing buffer, doubling from 512 B. A line whose content reaches
   max_len before its newline fails with EOVERFLOW and consumes the rest
   of the line, so fields.conf readers can fail closed instead of
   mis-parsing a truncated line (a truncated enum( loses its ')' and the
   field silently vanishes from the loaded schema). */
char *conf_read_line(FILE *fp, size_t max_len, int *err) {
    if (err) *err = 0;
    size_t cap = 512, len = 0;
    char *buf = malloc(cap);
    if (!buf) { if (err) *err = ENOMEM; return NULL; }
    for (;;) {
        if (cap - len < 2 || len >= max_len) {
            if (len >= max_len) {
                int c;
                while ((c = fgetc(fp)) != EOF && c != '\n') {}
                free(buf);
                if (err) *err = EOVERFLOW;
                return NULL;
            }
            char *nb = realloc(buf, cap * 2);
            if (!nb) { free(buf); if (err) *err = ENOMEM; return NULL; }
            buf = nb;
            cap *= 2;
        }
        if (!fgets(buf + len, (int)(cap - len), fp)) {
            if (ferror(fp)) { int e = errno; free(buf); if (err) *err = e; return NULL; }
            if (len == 0) { free(buf); return NULL; }   /* clean EOF */
            if (len >= max_len) {                       /* unterminated over-cap final line */
                free(buf);
                if (err) *err = EOVERFLOW;
                return NULL;
            }
            return buf;                                 /* final line, no '\n' */
        }
        size_t chunk = strlen(buf + len);
        char *nl = memchr(buf + len, '\n', chunk);
        if (nl) {
            if ((size_t)(nl - buf) >= max_len) {
                free(buf);
                if (err) *err = EOVERFLOW;
                return NULL;
            }
            *nl = '\0';
            return buf;
        }
        len += chunk;
    }
}
```

Semantics note: max accepted content = `max_len - 1` = 65,535 bytes — exactly
the wire gate's max (`flen >= MAX_FIELD_DEF` rejected), so a spec accepted on
the wire always re-reads.

## Task 3 — create-object path (`src/db/query_schema.c`)

**3a.** Directly above the anchor comment
`/* Validate fields array — must be non-empty, every field must have a valid type */`
(it sits inside the create-object command; the helper must be in scope above
its use), insert at file scope near `validate_field_type`:

```c
/* __attribute__((cleanup)) helper for cmd_create_object's field_specs. */
static void free_field_specs(char **specs) {
    for (int i = 0; i < MAX_FIELDS; i++) free(specs[i]);
}
```

**3b.** Replace:

```c
    /* First pass: validate all fields before creating anything */
    char field_specs[MAX_FIELDS][512];
    int nfields = 0;
    int total_value_size = 0;
```

with:

```c
    /* First pass: validate all fields before creating anything. Rows are
       heap strings (a MAX_FIELD_DEF-sized spec must fit); the cleanup
       attribute frees every row on any of this function's early exits. */
    char *field_specs[MAX_FIELDS] __attribute__((cleanup(free_field_specs))) = {0};
    int nfields = 0;
    int total_value_size = 0;
```

**3c.** Replace:

```c
        if (flen <= 0 || flen >= 511) {
            OUT("{\"error\":\"invalid field definition (empty or too long)\"}\n");
            return 1;
        }
        memcpy(field_specs[nfields], start, flen);
        field_specs[nfields][flen] = '\0';
```

with:

```c
        if (flen <= 0 || flen >= MAX_FIELD_DEF) {
            OUT("{\"error\":\"invalid field definition (empty or longer than %d bytes)\"}\n",
                MAX_FIELD_DEF - 1);
            return 1;
        }
        field_specs[nfields] = malloc((size_t)flen + 1);
        if (!field_specs[nfields]) {
            OUT("{\"error\":\"out of memory\"}\n");
            return 1;
        }
        memcpy(field_specs[nfields], start, flen);
        field_specs[nfields][flen] = '\0';
```

All later uses of `field_specs[i]` (enum-default check, `fprintf(f, "%s\n", ...)`
writer, index-name cross-checks) work unchanged on `char *` rows.

**3d. `validate_field_type`** — replace:

```c
/* Validate a field type spec like "name:varchar:30" or "age:int".
   Returns the storage size (>0) on success, 0 on invalid. */
static int validate_field_type(const char *field_spec) {
    const char *colon = strchr(field_spec, ':');
    if (!colon || colon == field_spec) return 0; /* no type separator or empty name */

    /* Work on a copy so we can strip modifiers */
    char buf[512];
    strncpy(buf, colon + 1, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';
```

with:

```c
static int validate_field_type_clean(char *buf);

/* Validate a field type spec like "name:varchar:30" or "age:int".
   Returns the storage size (>0) on success, 0 on invalid. */
static int validate_field_type(const char *field_spec) {
    const char *colon = strchr(field_spec, ':');
    if (!colon || colon == field_spec) return 0; /* no type separator or empty name */

    /* Heap copy so the type portion can be MAX_FIELD_DEF-sized (enum value
       lists). _clean mutates buf in place (strips modifiers). */
    char *buf = strdup(colon + 1);
    if (!buf) return 0;
    int rc = validate_field_type_clean(buf);
    free(buf);
    return rc;
}

static int validate_field_type_clean(char *buf) {
```

The remainder of the original body (from `/* Strip :removed */` through the
final `return 0;`) moves into `validate_field_type_clean` unchanged — delete
nothing else. The enum branch inside it already manages its own heap value
list.

## Task 4 — load_typed_schema (`src/db/config.c`)

Replace the block from the anchor

```c
    char line[512];
    while (fgets(line, sizeof(line), f) && ts.nfields < MAX_FIELDS) {
```

through the anchor

```c
    fclose(f);
    ts.total_size = offset;
```

with:

```c
    for (;;) {
        int rderr = 0;
        char *line = conf_read_line(f, MAX_FIELD_DEF, &rderr);
        if (!line) {
            if (rderr) {
                /* Fail closed: a line we cannot read whole would parse as a
                   truncated spec and silently drop/shift fields. Free the
                   enum value lists parsed from earlier lines first — `ts`
                   is stack-local and about to be abandoned. */
                LOG_ERROR(LOG_SUB_CONFIG,
                          "fields.conf line exceeds %d bytes — refusing to "
                          "load a schema we cannot parse faithfully",
                          MAX_FIELD_DEF - 1);
                for (int i = 0; i < ts.nfields; i++)
                    free_enum_values(&ts.fields[i]);
                fclose(f);
                return NULL;   /* not cached; next load retries the file */
            }
            break;   /* EOF */
        }
        if (ts.nfields >= MAX_FIELDS) { free(line); break; }
        if (line[0] == '\0' || line[0] == '#') { free(line); continue; }

        TypedField *tf = &ts.fields[ts.nfields];
        /* Parse "name:type[:param][:removed]" */
        char *colon = strchr(line, ':');
        if (!colon) { free(line); continue; } /* no type = skip (legacy format) */

        size_t name_len = (size_t)(colon - line);
        if (name_len >= 256) name_len = 255;
        memcpy(tf->name, line, name_len);
        tf->name[name_len] = '\0';
        tf->name_len = (int)name_len;

        /* Tombstone marker: line ends with ":removed". Strip it before
           type parsing so the type spec parses cleanly. The field's
           offset/size are still reserved to preserve on-disk layout. */
        tf->removed = 0;
        tf->default_kind = DK_NONE;
        tf->default_val[0] = '\0';
        /* Type portion — parsed in place. parse_default_modifier /
           parse_field_type only mutate the buffer to strip modifiers,
           and nothing reuses `line` after this block. */
        char *type_spec = colon + 1;
        size_t ts_len = strlen(type_spec);
        if (ts_len >= 8 && strcmp(type_spec + ts_len - 8, ":removed") == 0) {
            type_spec[ts_len - 8] = '\0';
            tf->removed = 1;
        }

        /* Default modifiers — strip from type_spec before parse_field_type.
           Format: :auto_create, :auto_update, :default=<val>
           These are always the last colon-segment (before :removed). */
        parse_default_modifier(type_spec, tf);

        parse_field_type(type_spec, tf);
        free(line);
        if (tf->type == FT_NONE || tf->size <= 0) continue;

        tf->offset = offset;
        offset += tf->size;
        ts.nfields++;
        ts.typed = 1;
    }
    fclose(f);
    ts.total_size = offset;
```

This removes both the 512-byte line split and the 255-byte `type_spec`
truncation (the hidden corruption threshold).

## Task 5 — signatures, add-field, wire handlers

**5a. Signature flips** — in `src/db/types.h` (declarations of
`rebuild_object`, `cmd_add_fields`, `cmd_edit_fields`) and
`src/db/query_internal.h` (`rebuild_object_v2`), replace every parameter of
the form `char ...[][256]` with the heap-row form:

```c
char (*added_lines)[MAX_FIELD_DEF]
```

(used where the current text is `char added_lines[][256]`) and:

```c
char (*lines)[MAX_FIELD_DEF]
```

(used where the current text is `char lines[][256]`). The definitions in
`query_find.c` (`rebuild_object`), `config.c` (`cmd_add_fields`), and
`query_schema.c` (`cmd_edit_fields`) must be flipped identically. NULL-passing
callers (`query_maint.c` vacuum, `embedded.c` migration, `query_schema.c`
edit finalize) need no change.

**5b. `cmd_add_fields` (`src/db/config.c`)** — replace:

```c
        TypedField tf;
        memset(&tf, 0, sizeof(tf));
        char clean_spec[256];
        strncpy(clean_spec, colon + 1, sizeof(clean_spec) - 1);
        clean_spec[sizeof(clean_spec) - 1] = '\0';
        parse_default_modifier(clean_spec, &tf);
        parse_field_type(clean_spec, &tf);
```

with (heap copy — `parse_default_modifier` mutates, and `lines[a]` is written
verbatim to fields.conf later, so the original must stay intact):

```c
        TypedField tf;
        memset(&tf, 0, sizeof(tf));
        char *clean_spec = strdup(colon + 1);
        if (!clean_spec) {
            OUT("{\"error\":\"out of memory\"}\n");
            return 1;
        }
        parse_default_modifier(clean_spec, &tf);
        parse_field_type(clean_spec, &tf);
        free(clean_spec);
```

**5c. Pre-existing enum leak in the same loop (required: Task 1 exercises
add-field with an enum and the ASan gate fails otherwise).**
`parse_field_type` heap-allocates `tf.enum_values`; `tf` is a per-iteration
throwaway that is memset on the next pass — the values leak. Replace the
random(N) preflight block (anchor: `/* Pre-flight: refuse random(N) with hex
output that exceeds the` through its closing `        }` before the loop's
final brace) so every exit frees the parsed field, and free at loop end:

```c
        /* Pre-flight: refuse random(N) with hex output that exceeds the
           field's storage. random(N) generates 2*N hex chars; for varchar
           the on-disk content cap is f->size - 2. For other types the
           encoded representation is rendered through encode_field which
           takes a string — best-effort is to refuse if the raw hex won't
           fit a varchar. Non-varchar destinations are caller error. */
        if (tf.default_kind == DK_RANDOM) {
            int n_bytes = atoi(tf.default_val);
            int hex_chars = n_bytes * 2;
            if (tf.type == FT_VARCHAR) {
                int cap = tf.size - 2;  /* uint16 length prefix */
                if (n_bytes <= 0 || hex_chars > cap) {
                    free_enum_values(&tf);
                    OUT("{\"error\":\"random(%d) produces %d hex chars but field [%s] holds only %d\"}\n",
                        n_bytes, hex_chars, name, cap);
                    return 1;
                }
            } else if (n_bytes <= 0) {
                free_enum_values(&tf);
                OUT("{\"error\":\"random(N) requires N > 0 in: %s\"}\n", lines[a]);
                return 1;
            }
        }
        free_enum_values(&tf);
```

**5d. server.c add-field handler** — replace the block from the anchor

```c
    } else if (strcmp(mode, "add-field") == 0) {
```

through the anchor `            free(fields_arr);\n        }\n    } else if (strcmp(mode, "edit-field") == 0) {`
(i.e. up to but not including the edit-field branch) with:

```c
    } else if (strcmp(mode, "add-field") == 0) {
        /* fields is a JSON array of spec lines, e.g. ["phone:varchar:20","dob:date"] */
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        if (!fields_arr) { OUT("{\"error\":\"Missing 'fields' array\"}\n"); }
        else {
            char (*lines)[MAX_FIELD_DEF] __attribute__((cleanup(free))) =
                calloc(MAX_FIELDS, MAX_FIELD_DEF);
            int nlines = 0;
            int too_long = 0;
            const char *p = fields_arr;
            while (*p && nlines < MAX_FIELDS) {
                while (*p == '[' || *p == ',' || *p == ' ' || *p == '\t') p++;
                if (*p == ']' || *p == '\0') break;
                if (*p == '"') {
                    p++;
                    const char *start = p;
                    while (*p && *p != '"') p++;
                    size_t l = p - start;
                    if (l > 0 && l < MAX_FIELD_DEF) {
                        memcpy(lines[nlines], start, l);
                        lines[nlines][l] = '\0';
                        nlines++;
                    } else if (l >= MAX_FIELD_DEF) {
                        too_long = 1;
                    }
                    if (*p == '"') p++;
                } else p++;
            }
            if (too_long)
                OUT("{\"error\":\"field definition longer than %d bytes\"}\n",
                    MAX_FIELD_DEF - 1);
            else if (nlines == 0) OUT("{\"error\":\"No fields in 'fields' array\"}\n");
            else cmd_add_fields(db_root, object, lines, nlines);
            free(fields_arr);
        }
    } else if (strcmp(mode, "edit-field") == 0) {
```

(Behavior change, documented in the changelog task: an over-cap spec now
fails the whole request with an explicit error instead of being silently
dropped while the remaining specs are applied.)

**5e. server.c edit-field handler** — same treatment. Replace, inside the
`"edit-field"` branch, the declaration + parse loop from the anchor
`            char lines[MAX_FIELDS][256];` through the anchor `            }` that closes its `while (*p && nlines < MAX_FIELDS)` loop
(the loop body is identical in shape to add-field's), with the identical
heap-row block shown in 5d (declare `too_long`, same `if (l > 0 && l < MAX_FIELD_DEF) ... else if (l >= MAX_FIELD_DEF) too_long = 1;` body).
Then replace:

```c
            if (nlines == 0) OUT("{\"error\":\"No fields in 'fields' array\"}\n");
            else cmd_edit_fields(db_root, object, lines, nlines, allow_rename, dry);
```

with:

```c
            if (too_long)
                OUT("{\"error\":\"field definition longer than %d bytes\"}\n",
                    MAX_FIELD_DEF - 1);
            else if (nlines == 0) OUT("{\"error\":\"No fields in 'fields' array\"}\n");
            else cmd_edit_fields(db_root, object, lines, nlines, allow_rename, dry);
```

Note: `allow_rename` / `dry_run` parsing stays where it is (it does not
depend on the buffer type).

## Task 6 — edit-field rewrite (`src/db/query_schema.c`)

**6a.** Flip `cmd_edit_fields`'s definition (anchor:
`int cmd_edit_fields(const char *db_root, const char *object,` followed by
`                    char lines[][256], int n_edits,`) to
`char (*lines)[MAX_FIELD_DEF], int n_edits,`. Flip the ctx struct's member
feeding `rewrite_fields_conf_for_edit(ctx->obj_dir, ctx->edit_lines, ...)` to
`char (*edit_lines)[MAX_FIELD_DEF];` (locate the struct via that call
anchor), and flip `rewrite_fields_conf_for_edit`'s own signature (anchor:
`static int rewrite_fields_conf_for_edit(const char *obj_dir,` /
`                                         char edit_lines[][256], int n_edits) {`)
to `char (*edit_lines)[MAX_FIELD_DEF], int n_edits) {`.

**6b.** In `rewrite_fields_conf_for_edit`, replace the block from the anchor

```c
    char line[512];
    while (fgets(line, sizeof(line), fin)) {
        char stripped[512];
        strncpy(stripped, line, sizeof(stripped) - 1);
        stripped[sizeof(stripped) - 1] = '\0';
        stripped[strcspn(stripped, "\n")] = '\0';
        if (stripped[0] == '\0' || stripped[0] == '#') {
            fputs(line, fout);
            continue;
        }
        const char *colon = strchr(stripped, ':');
        size_t nlen = colon ? (size_t)(colon - stripped) : strlen(stripped);
        int matched = -1;
        for (int e = 0; e < n_edits; e++) {
            size_t elen = strlen(edit_names[e]);
            if (nlen == elen && memcmp(stripped, edit_names[e], elen) == 0) {
                matched = e;
                break;
            }
        }
        if (matched < 0) {
            fputs(line, fout);
            continue;
        }
```

with:

```c
    for (;;) {
        int rderr = 0;
        char *line = conf_read_line(fin, MAX_FIELD_DEF, &rderr);
        if (!line) {
            if (rderr) {
                LOG_ERROR(LOG_SUB_CONFIG,
                          "rewrite_fields_conf_for_edit: fields.conf line exceeds "
                          "%d bytes — refusing to rewrite a schema we cannot "
                          "parse faithfully", MAX_FIELD_DEF - 1);
                fclose(fin);
                fclose(fout);
                unlink(fpath_new);
                return -1;
            }
            break;   /* EOF */
        }
        if (line[0] == '\0' || line[0] == '#') {
            fputs(line, fout);
            fputc('\n', fout);   /* conf_read_line strips the newline */
            free(line);
            continue;
        }
        const char *colon = strchr(line, ':');
        size_t nlen = colon ? (size_t)(colon - line) : strlen(line);
        int matched = -1;
        for (int e = 0; e < n_edits; e++) {
            size_t elen = strlen(edit_names[e]);
            if (nlen == elen && memcmp(line, edit_names[e], elen) == 0) {
                matched = e;
                break;
            }
        }
        if (matched < 0) {
            fputs(line, fout);
            fputc('\n', fout);   /* conf_read_line strips the newline */
            free(line);
            continue;
        }
```

Then in the matched-line tail of the loop, the two
`default_modifiers_for_line` calls change their first argument from
`stripped` to `line`:

```c
        char old_mods[256] = "";
        default_modifiers_for_line(line, old_mods, sizeof(old_mods));
        char new_mods[256] = "";
        int new_has = default_modifiers_for_line(edit_lines[matched], new_mods, sizeof(new_mods));
        if (!new_has && old_mods[0])
            fprintf(fout, "%s%s\n", edit_lines[matched], old_mods);
        else
            fprintf(fout, "%s\n", edit_lines[matched]);
        free(line);
    }
```

(`old_mods`/`new_mods` stay 256 bytes — they hold only the `:default=...` /
`:auto_*` modifier tail, and `default_modifiers_for_line` already refuses to
truncate. Names ≤128 bytes via `edit_names`, unchanged.)

## Task 7 — remove-field / rename-field rewrites (`src/db/config.c`)

**7a. `cmd_remove_fields`** — replace:

```c
    char lines[MAX_FIELDS][512];
    int nlines = 0;
    while (nlines < MAX_FIELDS && fgets(lines[nlines], sizeof(lines[0]), f)) {
        lines[nlines][strcspn(lines[nlines], "\n")] = '\0';
        nlines++;
    }
    fclose(f);
```

with:

```c
    char (*lines)[MAX_FIELD_DEF] __attribute__((cleanup(free))) =
        calloc(MAX_FIELDS, MAX_FIELD_DEF);
    if (!lines) {
        fclose(f);
        OUT("{\"error\":\"out of memory\"}\n");
        return 1;
    }
    int nlines = 0;
    int rderr = 0;
    while (nlines < MAX_FIELDS) {
        int e = 0;
        char *rd_line = conf_read_line(f, MAX_FIELD_DEF, &e);
        if (!rd_line) { rderr = e; break; }
        memcpy(lines[nlines], rd_line, strlen(rd_line) + 1);
        free(rd_line);
        nlines++;
    }
    fclose(f);
    if (rderr) {
        OUT("{\"error\":\"fields.conf line exceeds %d bytes — refusing to rewrite\"}\n",
            MAX_FIELD_DEF - 1);
        return 1;
    }
```

The cleanup attribute covers the function's later error returns and success
return; remove any now-redundant explicit `free(lines)` (there is none today —
today's buffer is stack). Downstream `lines[i]` uses are unchanged.

**7b. `cmd_rename_field`** — replace the declaration + loop header + strip
line:

```c
    char lines[MAX_FIELDS][512];
    int nlines = 0;
    int found_old = 0;
    int new_conflict = 0;
    while (nlines < MAX_FIELDS && fgets(lines[nlines], sizeof(lines[0]), f)) {
        lines[nlines][strcspn(lines[nlines], "\n")] = '\0';
```

with:

```c
    char (*lines)[MAX_FIELD_DEF] __attribute__((cleanup(free))) =
        calloc(MAX_FIELDS, MAX_FIELD_DEF);
    if (!lines) {
        fclose(f);
        OUT("{\"error\":\"out of memory\"}\n");
        return 1;
    }
    int nlines = 0;
    int found_old = 0;
    int new_conflict = 0;
    int rderr = 0;
    while (nlines < MAX_FIELDS) {
        int e = 0;
        char *rd_line = conf_read_line(f, MAX_FIELD_DEF, &e);
        if (!rd_line) { rderr = e; break; }
        memcpy(lines[nlines], rd_line, strlen(rd_line) + 1);
        free(rd_line);
```

The rest of the loop body (`const char *ln = lines[nlines]; ... nlines++;`)
is unchanged. Directly after the loop's `fclose(f);` insert:

```c
    if (rderr) {
        OUT("{\"error\":\"fields.conf line exceeds %d bytes — refusing to rewrite\"}\n",
            MAX_FIELD_DEF - 1);
        return 1;
    }
```

Downstream rewrite loop (`fprintf(nf, "%s\n", ln);` etc.) is unchanged.

## Task 8 — green run

1. `SKIP_TESTS=1 ./build.sh` — no new warnings.
2. `./build/bin/shard-db-test run test-long-field-def` — paste the pass.
3. Prove the fix is load-bearing (CORE-PROCESS regression proof, second
   half): `git stash` the source changes **excluding the new test file**
   (`git stash push src/ include/` — or equivalent; keep the test), rebuild,
   re-run the test, paste the failure, `git stash pop`, rebuild. If stashing
   is awkward, instead `git diff > /tmp/fix.patch && git checkout -- src/ &&
   ... build/run/restore` — either way, both outputs get pasted.
4. `./build/bin/shard-db-test run-all` — full suite green, fresh.

## Task 9 — docs (same branch, not deferred)

**`docs/reference/limits.md`** — directly below the anchor row
`| Fields per object | 256 | `MAX_FIELDS`. Includes tombstoned fields until compact. |` insert:

```markdown
| Field-definition length | 65 535 bytes | One `fields.conf` line — the same string in `create-object` / `add-field` / `edit-field` `fields[]`. Bounds the enum value list (≈13k four-char values; the 65,535-value enum ceiling is storage-side — the line cap binds first for longer values). Enforced identically on the wire and on reload, so an accepted schema re-reads faithfully. **Downgrade hazard:** binaries older than 2026.10 truncate `fields.conf` lines at 511 bytes at load — objects with field definitions above the old limit must not be opened by older binaries. |
```

**`docs/concepts/typed-records.md`** — in the enum row of the type table
(anchor: `| `enum` | `color:enum(red,green,blue)` | 1 or 2 |`), append one
sentence to the row: `Value lists share the 65,535-byte field-definition cap
(see [limits](../reference/limits.md)) — ≈13k four-char values.`

**`docs/query-protocol/schema-mutations.md`** — in the add-field and
edit-field sections, add one sentence each: `Each spec line is limited to
65,535 bytes (see [limits](../reference/limits.md)); requests containing a
longer spec fail with an explicit error.`

**`docs/reference/changelog.md`** — insert directly below the `## Unreleased`
heading:

```markdown
Field definitions (fields.conf lines and the `fields[]` strings of
create-object / add-field / edit-field) are now accepted up to 65,535
bytes, up from an undocumented 510-byte create gate whose companion
loader buffers (512-byte line reader, 255-byte type-portion buffer)
silently dropped or split long fields on every schema reload — long
`enum(...)` value lists were the practical casualty (125 four-char
values = 632 bytes was rejected at create; even ~300-byte lists
corrupted the schema on restart). The cap is enforced identically by
the wire validators and every fields.conf reader/rewriter, so an
accepted schema re-reads faithfully; readers refuse to load or rewrite
a fields.conf containing an over-long line instead of misparsing it.
add-field / edit-field requests containing an over-long spec now fail
with an explicit error instead of silently dropping it. Objects whose
field definitions exceed the old 510-byte limit must not be opened by
older binaries — those truncate the line at load.
```

## Task 10 — dynamic-safety gate (AGENTS.md definition of done)

The diff touches the shared typed-schema cache's feed path and request-scope
heap lifetimes, so both suites run locally, three consecutive fresh runs each:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all          # ×3, all green
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2   # ×3, all green
```

No `halt_on_error=0`, no suppressions. Any finding: root-cause and fix, then
rerun the full suite three times. The add-field enum-leak fix in Task 5c
exists precisely because this gate would flag it.

## Task 11 — hand off for review

Leave everything uncommitted. Produce for the reviewing agent + human:
`git status --short`, `git diff --stat`, the Task 1 base-failure output, the
Task 8 green outputs (including the revert-proof pair), and the Task 10
6× green run confirmations.

---

## Edge cases & invariants (explicit)

- A spec accepted on the wire (≤65,535 bytes) must re-read identically from
  `fields.conf`. The reader's max (`max_len - 1`) matches the wire gate's max
  exactly.
- Over-long lines in an on-disk `fields.conf` (only possible via hand-editing
  or downgrade) → fail closed: `load_typed_schema` returns NULL with a
  LOG_ERROR (object refuses typed-schema-dependent paths rather than decoding
  records against a shifted layout); `remove-field` / `rename-field` /
  edit-field rewrite refuse to rewrite the file.
- `conf_read_line` strips only `'\n'` (today's readers strip only `'\n'` — a
  literal `\r` in a value behaves exactly as before).
- `:default=` / `:auto_*` modifier tails stay capped at 255 bytes via
  `default_val[256]` + `default_modifiers_for_line`'s refuse-to-truncate.
  Consequence (pre-existing, now documented in limits.md context): enum values
  used as `:default=` literals must fit 255 bytes — over-cap defaults
  truncate. Not changed in this plan.
- The old 255-byte `type_spec` truncation is the *hidden* threshold this fix
  removes; the regression test's object B (337-byte spec) exists to prove
  that path, not just the loud 511-byte gate.
- `field_specs` / `lines` heap rows are per-request scratch; nothing shared
  outlives the request. Typed-schema cache contents are unchanged in shape —
  `TypedField` already stores enum value lists on the heap.
- Worst-case transient heap: `MAX_FIELDS × MAX_FIELD_DEF` = 16 MiB per
  create/add/edit/remove/rename request (calloc → lazily mapped); below
  `QUERY_BUFFER_MB` and bounded by `MAX_CONCURRENT_QUERIES` as usual.
- Scope guards: no changes to `schema.conf` / `index.conf` / `tokens.conf`
  readers (their lines are short by construction — see §1 out-of-scope notes);
  no wire-protocol or CLI changes; no new compile flags or feature macros.
