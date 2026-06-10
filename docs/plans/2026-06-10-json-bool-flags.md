# Plan: accept JSON boolean `true` for all boolean query flags

**Date:** 2026-06-10
**Branch:** `feat/json-bool-flags`
**Files:** `src/db/util.c`, `src/db/types.h`, `src/db/server.c`

## Problem

`server.c` reads boolean query parameters (`total`, `force`, `dry`, `if_not_exists`, etc.) with
`json_obj_strdup` + `strcmp(..., "true")`. `json_obj_strdup` calls `json_obj_unquoted`, which
strips surrounding quotes from string values but does not correctly return the raw bytes for JSON
boolean literals. Result: a client that sends `"total":true` (JSON boolean) gets `want_total=0`;
only `"total":"true"` (JSON string) works.

There are **19** such comparisons across `server.c`. All have the same bug.

**Fix:** add a zero-allocation `json_obj_is_true(o, key)` helper in `util.c` that uses
`json_obj_get` directly and accepts both forms:

| JSON on the wire | Raw bytes from `json_obj_get` | Should return |
|---|---|---|
| `"total":true` | `true` (4 B) | 1 |
| `"total":"true"` | `"true"` (6 B, with quotes) | 1 |
| `"total":false` | `false` (5 B) | 0 |
| `"total":"false"` | `"false"` (7 B) | 0 |
| key absent | — | 0 |

## Execution rules

- Branch off `main`.
- Tasks in order; build after every task with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- Never claim a step passed without the real build/test output.
- Locate every edit by the **quoted anchor text** below; if an anchor is not found exactly, stop
  and write `PLAN_NOTES.md` — do not guess.

---

## Task 1 — Add `json_obj_is_true` to `util.c`

**File:** `src/db/util.c`

**Anchor** (insert immediately after this closing brace + blank line):
```
    if (val)  *val  = v;
    if (vlen) *vlen = vl;
    return 1;
}

char *json_obj_strdup(const JsonObj *o, const char *key) {
```

Insert the following block **between** those two functions (i.e. after the closing `}` of
`json_obj_unquoted` and before `char *json_obj_strdup`):

```c
/* Returns 1 if the named field is the JSON boolean true OR the string "true";
   0 for false, "false", absent, or any other value.  Uses json_obj_get (raw
   span) so it works for both quoted string values and unquoted JSON booleans.
   No allocation — safe to call from any hot path. */
int json_obj_is_true(const JsonObj *o, const char *key)
{
    const char *v; size_t vl;
    if (!json_obj_get(o, key, &v, &vl)) return 0;
    /* JSON boolean: true (4 bytes, no quotes) */
    if (vl == 4 && memcmp(v, "true", 4) == 0) return 1;
    /* JSON string: "true" (6 bytes including surrounding quotes) */
    if (vl == 6 && memcmp(v, "\"true\"", 6) == 0) return 1;
    return 0;
}

```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 2 — Declare `json_obj_is_true` in `types.h`

**File:** `src/db/types.h`

**Anchor** (exact text):
```
char *json_obj_strdup(const JsonObj *o, const char *key);
```

Replace with:

```c
char *json_obj_strdup(const JsonObj *o, const char *key);
int   json_obj_is_true(const JsonObj *o, const char *key);
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 3 — Replace all 19 boolean-flag patterns in `server.c`

Each occurrence follows one of two shapes. Replace **every** instance.

### Shape A — strdup + strcmp on a dedicated variable

```c
char *<var> = json_obj_strdup(&req, "<key>");
...
int <flag> = (<var> && strcmp(<var>, "true") == 0);
...
free(...); ... free(<var>);
```

Replace the `strdup` + `strcmp` with `json_obj_is_true` and remove the `free` for that variable.
The full replacement for each occurrence:

**Before:**
```c
char *<var> = json_obj_strdup(&req, "<key>");
```
and the corresponding:
```c
int <flag> = (<var> && strcmp(<var>, "true") == 0);
```
and the corresponding `free(<var>);` in the cleanup block.

**After:**
```c
int <flag> = json_obj_is_true(&req, "<key>");
```
(Remove the `char *<var>` declaration, the `strcmp` line, and the `free(<var>)` call entirely.)

### Shape B — inline strdup + strcmp in one expression

```c
int <flag> = (<var> && strcmp(<var>, "true") == 0);
```
where `<var>` was declared just above. Same treatment as Shape A.

### Complete list of occurrences to replace

All 19 are listed below by their anchor text (the `strcmp` line). Replace each one using the
shapes above.

1. **`composites_only`** — key `"composites_only"`
   Anchor: `int composites_only = (co_s && strcmp(co_s, "true") == 0);`

2. **`if_not_exists`** (insert mode) — key `"if_not_exists"`
   Anchor: `int if_not_exists = ine_s && (strcmp(ine_s, "true") == 0 || strcmp(ine_s, "1") == 0);`
   Note: this one also accepts `"1"` — preserve that: after replacement use:
   ```c
   int if_not_exists = json_obj_is_true(&req, "if_not_exists") ||
                       (ine_s && strcmp(ine_s, "1") == 0);
   ```
   where `ine_s` is still fetched for the `"1"` check. Keep the `free(ine_s)`.

3. **`if_exists`** — key `"if_exists"`
   Anchor: `int if_exists = ie_s && (strcmp(ie_s, "true") == 0 || strcmp(ie_s, "1") == 0);`
   Same note as above — preserve `"1"` acceptance:
   ```c
   int if_exists = json_obj_is_true(&req, "if_exists") ||
                   (ie_s && strcmp(ie_s, "1") == 0);
   ```

4. **`force`** (add-index) — key `"force"`
   Anchor: `int force = fstr && strcmp(fstr, "true") == 0;`

5. **`if_not_exists`** (update mode) — key `"if_not_exists"`
   Anchor: `int if_not_exists = (ine_raw && strcmp(ine_raw, "true") == 0);`

6. **`dry`** (bulk-delete) — key `"dry_run"`
   Anchor: `int dry = (dry_s && strcmp(dry_s, "true") == 0);` ← first occurrence

7. **`dry`** (bulk-update) — key `"dry_run"`
   Anchor: `int dry = (dry_s && strcmp(dry_s, "true") == 0);` ← second occurrence

8. **`want_total`** (find) — key `"total"`
   Anchor: `int want_total = (tot_s && strcmp(tot_s, "true") == 0) ? 1 : 0;` ← first occurrence

9. **`want_total`** (fetch) — key `"total"`
   Anchor: `int want_total = (tot_s && strcmp(tot_s, "true") == 0) ? 1 : 0;` ← second occurrence

10. **`f`** (force, vacuum) — key `"force"`
    Anchor: `int f = force && strcmp(force, "true") == 0;`

11. **`ifne`** (put-file first) — key `"if_not_exists"`
    Anchor: `int ifne = (ifne_s && strcmp(ifne_s, "true") == 0);` ← first occurrence

12. **`ifne`** (put-file second) — key `"if_not_exists"`
    Anchor: `int ifne = (ifne_s && strcmp(ifne_s, "true") == 0);` ← second occurrence

13. **`dry`** (edit-field bulk-delete) — key `"dry_run"`
    Anchor: `int dry = (dry_s && strcmp(dry_s, "true") == 0);` ← third occurrence

14. **`dry`** (edit-field bulk-update) — key `"dry_run"`
    Anchor: `int dry = (dry_s && strcmp(dry_s, "true") == 0);` ← fourth occurrence

15. **`compact`** — key `"compact"`
    Anchor: `int compact = compact_s && strcmp(compact_s, "true") == 0;`

16. **`allow_rename`** — key `"allow_rename"`
    Anchor: `strcmp(allow_rename_str, "true") == 0 ||`
    This one is part of a compound expression; replace only the `strcmp(allow_rename_str, "true") == 0`
    sub-expression with `json_obj_is_true(&req, "allow_rename")` and remove the `char *allow_rename_str`
    declaration and its `free`.

17. **`dry`** (rename-field) — key `"dry_run"`
    Anchor: `int dry = (dry_s && (strcmp(dry_s, "true") == 0 || strcmp(dry_s, "1") == 0));`
    Preserve `"1"`:
    ```c
    int dry = json_obj_is_true(&req, "dry_run") ||
              (dry_s && strcmp(dry_s, "1") == 0);
    ```

18. **`if_not_exists`** (create-object) — key `"if_not_exists"`
    Anchor: `int if_not_exists = ine && strcmp(ine, "true") == 0;`

19. **`want_total`** (aggregate) — key `"total"`
    Anchor: `int want_total = (tot_s && strcmp(tot_s, "true") == 0) ? 1 : 0;` ← third occurrence

For each occurrence that fully replaces a `char *var = json_obj_strdup(...)` + `strcmp` +
`free(var)` triple, remove all three lines and replace with the single `json_obj_is_true` line.
For occurrences that share a variable also used for something else (items 2, 3, 17 above), keep
the variable for the `"1"` check as noted.

Build after all replacements: `SKIP_TESTS=1 ./build.sh` — must succeed with zero errors/warnings.

---

## Task 4 — Build and test

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed`.

Paste the actual output. Do not claim pass without real output.

---

## Invariants and edge cases

| Case | Expected behaviour |
|---|---|
| `"total":true` (boolean) | `want_total = 1` ✓ |
| `"total":"true"` (string) | `want_total = 1` ✓ (backward compatible) |
| `"total":false` (boolean) | `want_total = 0` |
| `"total":"false"` (string) | `want_total = 0` |
| `"total":1` (number) | `want_total = 0` — not accepted; use `true` or `"true"` |
| key absent | `want_total = 0` |
| All other 18 flags | Same two-form acceptance: boolean `true` and string `"true"` |
| `"1"` shortcuts (if_not_exists, if_exists, dry_run/rename) | Still accepted — existing behaviour preserved |
| No allocation | `json_obj_is_true` calls only `json_obj_get` (no malloc) — safe in hot paths |
