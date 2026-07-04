# Fix: object names are not validated for path traversal (cross-tenant escape)

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/object-name-traversal`.
- Do tasks in order. Task 1 (failing test) is built and run **before** Task 2+,
  and its failure output pasted verbatim, to prove the escape reproduces.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run test-object-name-validation`, then `run-all`.
- Every edit is anchored on quoted exact text. If an anchor is not found
  exactly, STOP and write `PLAN_NOTES.md`. Do not guess.
- Never claim a step passed without pasting real output. Leave work uncommitted.

## Background

The `object` field from a request is interpolated straight into filesystem
paths — e.g. `snprintf(obj_check, "%s/%s/fields.conf", db_root, object)` in
`server.c` — with no check for `/` or `..`. Only `dir` is validated
(`is_valid_dir`), and `build_effective_root` does no `realpath` canonicalization
(`config.c`: it is a bare `snprintf(out, outlen, "%s/%s", g_db_root, dir)`).

`is_authorized(token, req_dir, req_obj, mode)` scope-checks only `req_dir`
against a tenant token; the object name is never checked. So a tenant-scoped
token for dir `A` can send `object:"../B/secret"`: auth passes (dir matches),
the path resolves to `$DB_ROOT/B/secret/fields.conf` (which exists), and the
operation runs against tenant B's data.

Confirmed by memory notes #6957 and #6958. This is the release's one
ship-blocker.

## Fix strategy

Add a dedicated `is_valid_object()` validator (mirroring `valid_filename()` in
`util.c`) and call it in all three request entry points **before** any path is
built from the object name: the JSON dispatch, the legacy `\x1F` fast path, and
the NQL path. Also enforce it in `cmd_create_object` so an object dir can never
be created outside its tenant.

A valid object name: non-empty, ≤255 bytes, no `/`, no `\`, no control chars
(`< 0x20` or `0x7F`), not `.` or `..`, and not beginning with `.`.

---

## Task 1 — Failing test (prove the escape)

Create `src/test/cases/test_object_name_validation.c`:

```c
/* src/test/cases/test_object_name_validation.c
 * Object names must never contain path-traversal characters. A tenant must
 * not be able to reach a sibling tenant's object by passing "../other/obj".
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

static int test_object_name_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* Set up two tenants; victim tenant B holds a real object with a secret. */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"tenant_a\"}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"tenant_b\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"tenant_b\",\"object\":\"secrets\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"v:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create victim object"); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"tenant_b\",\"object\":\"secrets\","
        "\"key\":\"k1\",\"value\":{\"v\":\"topsecret\"}}", &resp); free(resp); resp = NULL;

    /* Attack 1: create-object with a traversal object name must be rejected. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"tenant_a\",\"object\":\"../tenant_b/evil\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"v:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "invalid object name", "create-object rejects traversal"); free(resp); resp = NULL;

    /* Attack 2: read tenant_b's secret from a tenant_a request via traversal. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"../tenant_b/secrets\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "invalid object name", "get rejects traversal object");
    ASSERT_TRUE(strstr(resp, "topsecret") == NULL, "secret must not leak"); free(resp); resp = NULL;

    /* Sanity: a normal object name still works. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"tenant_a\",\"object\":\"orders\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"v:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "normal object name still accepted"); free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-object-name-validation", test_object_name_validation_run)
```

Register it in `build.sh`. Anchor on this exact line:

```
    src/test/cases/test_auto_key.c \
```

Insert immediately **after** it:

```
    src/test/cases/test_object_name_validation.c \
```

Build and run — it MUST fail (the traversal currently succeeds / leaks):

```
SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-object-name-validation
```

Paste the real failing output before proceeding.

---

## Task 2 — Add `is_valid_object()` to util.c

In `src/db/util.c`, anchor on the end of `valid_filename` — this exact block:

```c
    /* No component may be "..". Since we disallow '/', the whole name is one component;
       we already rejected "..". Done. */
    return 1;
}
```

Insert **immediately after** it:

```c

/* Validate an object name arriving from a request. Object names are a single
   path component under $DB_ROOT/<dir>/; they are interpolated directly into
   filesystem paths, so they must never contain a separator or traversal.
   Rejects: empty, > 255 bytes, "/" or "\", control chars, leading '.',
   and the literal "." / "..". Mirrors valid_filename's contract but is named
   for the call sites so the intent is unambiguous. */
int is_valid_object(const char *name) {
    if (!name || !name[0]) return 0;
    size_t n = strlen(name);
    if (n > 255) return 0;
    if (name[0] == '.') return 0;  /* rejects ".", "..", and dotfiles */
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)name[i];
        if (c == '/' || c == '\\' || c < 0x20 || c == 0x7F) return 0;
    }
    return 1;
}
```

Declare it in `src/db/types.h`. Anchor on this exact line:

```c
int valid_filename(const char *name);
```

Insert **immediately after** it:

```c
int is_valid_object(const char *name);
```

---

## Task 3 — Enforce in the JSON dispatch

In `src/db/server.c`, anchor on this exact block (the required-fields check):

```c
    if (!mode || !dir || !object) {
        OUT("{\"error\":\"Missing mode, dir, or object\"}\n");
        free(mode); free(dir); free(object);
        return;
    }
```

Insert **immediately after** it:

```c

    /* Object names are interpolated into filesystem paths below; reject any
       traversal ("/", "..") before create-object/drop-object/restore and every
       data path build their $DB_ROOT/<dir>/<object> paths. Without this a
       tenant token could pass object:"../other/obj" and escape its tenant. */
    if (!is_valid_object(object)) {
        OUT("{\"error\":\"invalid object name (no /,\\\\, leading dot, control chars; max 255 bytes)\"}\n");
        free(mode); free(dir); free(object);
        return;
    }
```

> Note: this sits **after** the `shard-stats`/`list-objects` early-returns
> (which legitimately run with `object == NULL`) and **before** the
> `create-object` / `drop-object` / `restore` branches, so all of them are
> covered.

---

## Task 4 — Enforce in the legacy `\x1F` fast path

In `src/db/server.c`, `server_process_fast`, anchor on this exact block:

```c
    if (!object || !object[0]) {
        OUT("{\"error\":\"object is required\"}\n");
        goto timing;
    }
```

Insert **immediately after** it:

```c
    if (!is_valid_object(object)) {
        OUT("{\"error\":\"invalid object name (no /,\\\\, leading dot, control chars; max 255 bytes)\"}\n");
        goto timing;
    }
```

---

## Task 5 — Enforce in the NQL path

In `src/db/server.c`, `dispatch_nql_query`, anchor on this exact block (the
auth gate that runs just before the db_root/object paths are built):

```c
    /* Build db_root = g_db->db_root / dir */
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof db_root, "%s/%s", raw_db_root, cmd.dir);
```

Insert **immediately before** it:

```c
    if (!is_valid_object(cmd.obj)) {
        OUT("{\"error\":\"invalid object name\"}\n");
        nql_free_command(&cmd);
        return;
    }

```

---

## Task 6 — Defense in depth in cmd_create_object

In `src/db/query.c`, `cmd_create_object`, anchor on this exact block:

```c
    if (!object || !object[0]) {
        OUT("{\"error\":\"object is required\"}\n");
        return 1;
    }
```

Insert **immediately after** it:

```c
    if (!is_valid_object(object)) {
        OUT("{\"error\":\"invalid object name (no /,\\\\, leading dot, control chars; max 255 bytes)\"}\n");
        return 1;
    }
```

---

## Task 7 — Verify

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-object-name-validation   # must now pass
./build/bin/shard-db-test run-all
```

Paste both outputs. The suite must end `# total: N passed, 0 failed`. Leave
uncommitted.

## Notes / invariants

- Internal callers that legitimately pass a composite `"dir/obj"` to helpers
  (e.g. `card_est_by_field` in `query.c` splits on `strchr(object, '/')`) are
  **not** reached from the wire — they receive already-validated names from
  server-side code, never the raw request field. Do not add `is_valid_object`
  inside those internal helpers; it would break the composite convention.
- The `dir` remains validated separately by `is_valid_dir`; this plan adds the
  missing *object* half only.
