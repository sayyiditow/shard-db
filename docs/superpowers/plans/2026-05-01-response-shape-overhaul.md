# 2026.05.1 Response-Shape Overhaul + ./migrate Binary

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land all breaking-change response-shape work in a single release so callers update their parsers exactly once. Add a separate `./migrate` binary that owns one-shot per-release upgrade steps. Reissue the `2026.05.1` tag with everything bundled.

**Architecture:**
- Read responses unwrap to bare values where possible (`get` → value dict, `exists` → bool, `count`/`size`/`orphaned` → number) and `get-multi` shifts from array-of-pairs to a key-keyed dict for O(1) lookup. `find`/`fetch` gain a third format option `dict` next to default/`csv`/`rows`. `aggregate`, writes, admin, files, and stats are untouched. Cursor envelope on `find`/`fetch` is preserved (only the `results` payload changes when `format=dict`).
- A new `./migrate` binary lives next to `shard-db` and `shard-cli` in `build/bin/`. It owns one-shot per-release migrations. For 2026.05.1 it runs (a) `migrate-files` (lift pre-2026.05 `XX/XX` hash buckets into flat layout) using FS-direct logic linked into the migrate binary itself, and (b) `reindex` by spawning the daemon and sending `{"mode":"reindex"}`. The `migrate-files` mode and `./shard-db migrate-files` subcommand are removed from the daemon entirely so the dead code doesn't ship forever.
- Tag `2026.05.1` is deleted on origin and rebuilt at the new HEAD with release notes documenting the breaking changes and the upgrade procedure.

**Tech Stack:** C (gcc -O2 -flto), pthread, OpenSSL 1.1+, ncurses, bash for tests/build.

---

## File Structure

**Modify:**
- `src/db/storage.c` — `cmd_get` (line 786), `cmd_get_multi` (line 1605); `print_record_json` helper used by find/fetch.
- `src/db/query.c` — `cmd_exists` (line 4035), `cmd_count` (line 8041), `cmd_size` (line 126), `cmd_find` (line 8531), `cmd_fetch` (around line 4310, find via grep), `print_record_json` (line 4125).
- `src/db/server.c` — JSON dispatch table (line 274 mode list), strip `migrate-files` mode (line 977-978), strip `migrate-files` from CLI dispatch.
- `src/db/main.c` — strip `migrate-files` CLI subcommand (lines 55, 131-134), help text update.
- `src/db/types.h` — add `cmd_orphaned` declaration; remove `cmd_migrate_files` declaration (line 857) once moved.
- `src/cli/views.c` — TUI parser updates: `flatten value` shape change (lines 354-379, 567-582), array-of-pairs → dict for get-multi (rendering path).
- `build.sh` — add migrate binary build step.
- `CLAUDE.md` — protocol docs section.
- `README.md` — examples and curl/CLI snippets.
- `tests/*.sh` — every test that asserts on read response shapes (32 files; ~19 reference `"value":`, ~16 reference `"count":`, etc.).

**Create:**
- `src/migrate/main.c` — orchestrator entry point.
- `src/migrate/migrate_files.c` — extracted from `cmd_migrate_files` in `src/db/query.c` lines 9439-9700ish.
- `src/migrate/migrate.h` — declarations for the migrate binary's internal functions.
- `tests/test-migrate-binary.sh` — end-to-end test of `./migrate` against a seeded XX/XX layout.
- `docs/release-notes/2026.05.1.md` — release notes.

**Delete:**
- `cmd_migrate_files` impl in `src/db/query.c` (lines ~9439-9700) — moved.
- `migrate-files` mode dispatch in `src/db/server.c` (lines 977-978).
- `migrate-files` CLI dispatch in `src/db/main.c` (lines 131-134).

---

## Conventions

- **TDD-style for every shape change**: update the test to assert the NEW shape, run it (fails on current impl), change the impl, run it (passes), commit. Most tests already exist; we're updating expectations.
- **Tests are mine**: run `tests/test-*.sh` after each change. User runs benches separately.
- **Commit cadence**: one commit per phase boundary at minimum; per-task is fine when changes are independent.
- **No emojis in code or docs.**
- **Don't add comments** explaining what we're doing for the task — only why-non-obvious comments allowed.

---

## Task 1: get → bare value dict

**Files:**
- Modify: `src/db/storage.c:786-827` (`cmd_get`)
- Test: `tests/test-objlock.sh` (and any other tests that grep `"value":` in get output — sweep with `grep -l '"key":' tests/*.sh` and update each)

- [ ] **Step 1.1: Update tests to expect bare value**

Find every test that asserts on `cmd_get` output. The current shape is `{"key":"k","value":{...}}`; new shape is just `{...}` (the value dict).

Run: `grep -l '"key":' tests/*.sh`

For each test in that list, look at the assertion against `./shard-db get ...` output. Replace expected `'{"key":"k1","value":{"name":"alice"}}'` with `'{"name":"alice"}'`. Replace `jq '.value.name'` with `jq '.name'`. Replace `jq -r '.value | ...'` with `jq -r '. | ...'`.

If a test parses both `.key` and `.value`, the `.key` is gone — replace with the literal key the caller already knows.

- [ ] **Step 1.2: Run tests to confirm they fail on current impl**

Run: `./build.sh && ./tests/test-objlock.sh`
Expected: assertions about response shape FAIL.

- [ ] **Step 1.3: Change `cmd_get` to emit bare value**

Edit `src/db/storage.c:823`:

```c
// OLD:
OUT("{\"key\":\"%s\",\"value\":%s}\n", key, json);
// NEW:
OUT("%s\n", json);
```

The `key` parameter becomes unused for output; leave the function signature as-is (the key is still needed for the lookup itself).

- [ ] **Step 1.4: Run all tests touched in 1.1**

Run: `./build.sh && for t in $(grep -l '"key":' tests/*.sh); do echo "==> $t"; bash $t || break; done`
Expected: all PASS.

- [ ] **Step 1.5: Update TUI parser**

Edit `src/cli/views.c:354-379` and `:567-582`. The single-record render path that destructures `{"key","value"}` into a flat row no longer applies. Now the response IS the flat record. Two callsites:

- Line 354 ("flatten value" comment block): the parser currently looks for `"value"` key inside the JSON. After this change, there is no `"value"` wrapper — the JSON itself is the value. Update by removing the wrapper-strip step; render the top-level object directly.
- Line 567 (Shape 4 single-record render): currently extracts `key` and `value` from the response; change to render the whole top-level object as a key-value table where each top-level field is a row, and prepend a synthetic `key` row using the key the caller searched with (the TUI knows the lookup key — pass it through).

Read `cli.h` and `cli/views.c` thoroughly before editing — there is more parser logic than the line numbers above suggest.

- [ ] **Step 1.6: Build, smoke-test TUI manually**

Run: `./build.sh`
Then: launch `./shard-cli`, navigate to a get request on a known object, confirm the record renders as a flat table with the key shown as the first row.

- [ ] **Step 1.7: Commit**

```bash
git add src/db/storage.c src/cli/views.c tests/
git commit -m "$(cat <<'EOF'
break: get returns bare value dict, no {key,value} wrapper

Callers should treat the response body AS the value. Saves bytes on
the wire and aligns with the get-multi dict shape (next commit).

Breaking change. Reissued 2026.05.1.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: get-multi → dict shape

**Files:**
- Modify: `src/db/storage.c:1605` (`cmd_get_multi`) and the JSON output loop (around line 1687-1750ish — read the full function)
- Test: any test that asserts on `cmd_get` with multi-key form (probably in `test-objlock.sh`, `test-cli-shortcuts.sh`)

- [ ] **Step 2.1: Read the full cmd_get_multi function**

Run: `sed -n '1605,1810p' src/db/storage.c`
Identify exactly where the JSON output loop emits `[{"key":...,"value":...},...]`. Note all the `OUT("[")`, `OUT(",")`, `OUT("]")` calls and the per-entry `OUT("{\"key\":...,\"value\":...}")` call.

- [ ] **Step 2.2: Update tests**

Find every test asserting on multi-key get response. Old shape: `[{"key":"k1","value":{"a":1}},{"key":"k2","value":{"a":2}}]`. New shape: `{"k1":{"a":1},"k2":{"a":2}}`.

Update jq paths: `jq '.[0].value.a'` → `jq '.k1.a'`. Update grep assertions to match the new shape.

- [ ] **Step 2.3: Run tests, confirm they fail**

Run: `./build.sh && ./tests/test-cli-shortcuts.sh && ./tests/test-objlock.sh`
Expected: shape assertions FAIL.

- [ ] **Step 2.4: Rewrite the JSON output loop**

Replace the array-emitting loop in `cmd_get_multi` with a dict-emitting loop. Skeleton:

```c
// OLD (sketch):
OUT("[");
for (int i = 0; i < key_count; i++) {
    if (i) OUT(",");
    if (entries[i].result_json)
        OUT("{\"key\":\"%s\",\"value\":%s}", entries[i].key, entries[i].result_json);
    else
        OUT("{\"key\":\"%s\",\"error\":\"Not found\"}", entries[i].key);
}
OUT("]\n");

// NEW (sketch):
OUT("{");
int first = 1;
for (int i = 0; i < key_count; i++) {
    if (!first) OUT(",");
    first = 0;
    if (entries[i].result_json) {
        OUT("\"%s\":%s", entries[i].key, entries[i].result_json);
    } else {
        OUT("\"%s\":null", entries[i].key);
    }
}
OUT("}\n");
```

Decisions:
- Missing key → emit `"k":null`. (Was `{"key":"k","error":"Not found"}` before.) Document this in CLAUDE.md.
- Empty input array `[]` → emit `{}` (not `[]`). Update line 1638 (`if (key_count == 0) { free(entries); OUT("[]\n"); ... }`) → `OUT("{}\n")`.

- [ ] **Step 2.5: Update CSV path consistency check**

Look at the `csv_delim` branch (line 1688). CSV output is unchanged (it's already row-oriented, key as first column). Confirm by reading. No change needed in CSV branch unless something has snuck in.

- [ ] **Step 2.6: Run tests, confirm pass**

Run: `./build.sh && ./tests/test-cli-shortcuts.sh && ./tests/test-objlock.sh`
Expected: PASS.

- [ ] **Step 2.7: Sweep all tests for multi-get assertions**

Run: `grep -l 'get-multi\|"keys":\[' tests/*.sh`
Run each in the list and fix any remaining shape mismatches.

- [ ] **Step 2.8: Update TUI rendering**

Edit `src/cli/views.c` array-of-pairs render path. Now multi-get returns a dict; the TUI should render it as a table with `key` column + value field columns, iterating dict entries (not array entries). Look for the existing "shape 4" or "shape 5" code paths in views.c around lines 526-672 — this is where the auto-detect runs. Add a new shape that detects "dict whose values are dicts" and treats each top-level key as a row, columns = union of inner keys.

- [ ] **Step 2.9: Smoke-test TUI**

`./shard-cli` → multi-key get → confirm tabular render.

- [ ] **Step 2.10: Commit**

```bash
git add src/db/storage.c src/cli/views.c tests/
git commit -m "$(cat <<'EOF'
break: get-multi returns {key:value,...} dict, not array

Missing keys emit "k":null instead of an error object per-key. Empty
input array emits {} instead of []. CSV format unchanged.

Breaking change. Reissued 2026.05.1.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: exists single → bare bool

**Files:**
- Modify: `src/db/query.c:4035-4065` (`cmd_exists`)
- Test: any test that asserts on `cmd_exists` single-key output

- [ ] **Step 3.1: Update tests**

`grep -l '"exists":' tests/*.sh` — there should be matches. For SINGLE-key exists assertions, replace `'{"exists":true}'` with `'true'`. For MULTI-key exists, no change (multi shape `{"k1":true,...}` stays as decided).

- [ ] **Step 3.2: Run failing tests**

Run: `./build.sh && for t in $(grep -l '"exists":' tests/*.sh); do echo "==> $t"; bash $t || break; done`
Expected: single-key exists assertions FAIL.

- [ ] **Step 3.3: Change cmd_exists**

Edit `src/db/query.c:4045, 4058, 4063`:

```c
// 4045: not-found-shard branch
OUT("false\n");                    // was: OUT("{\"exists\":false}\n");
// 4058: found
OUT("true\n");                     // was: OUT("{\"exists\":true}\n");
// 4063: scanned through, not found
OUT("false\n");                    // was: OUT("{\"exists\":false}\n");
```

The function returns `0` for found / `1` for not-found — preserve return codes (the CLI uses them for shell exit status).

- [ ] **Step 3.4: Run tests, confirm pass**

Run: `for t in $(grep -l '"exists":' tests/*.sh); do bash $t || exit 1; done; echo "all pass"`

- [ ] **Step 3.5: Commit**

```bash
git add src/db/query.c tests/
git commit -m "$(cat <<'EOF'
break: exists (single key) returns bare true/false

Multi-key exists keeps its {key:bool} dict shape — already useful for
key lookup. Single-key just returns the boolean.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: count → bare number

**Files:**
- Modify: `src/db/query.c:8041` (`cmd_count`) — multiple emit sites; sweep for every `OUT("{\"count\":` in the function.
- Test: tests touching `count` (`test-or-logic.sh`, `test-and-intersection.sh`, `test-objlock.sh`, others).

- [ ] **Step 4.1: Find every count emit site**

Run: `grep -n '"count":' src/db/query.c`
Should hit `cmd_count` (multiple branches: no-criteria, single-leaf inline, intersect, full-scan) and possibly `cmd_size`. **DO NOT touch `cmd_size` here** (that's task 5).

- [ ] **Step 4.2: Update tests**

`grep -l '"count":' tests/*.sh` — for each, replace assertions on `cmd_count` output. `'{"count":42}'` → `'42'`. `jq '.count'` → `jq '.'`.

CAREFUL: `cmd_size` also emits `"count"` — those tests stay on the old shape until task 5. Disambiguate by reading what command the test invokes.

- [ ] **Step 4.3: Run failing tests**

Run: `./build.sh && ./tests/test-or-logic.sh`
Expected: count assertions FAIL.

- [ ] **Step 4.4: Change every count emit in cmd_count**

For every emit pattern in `cmd_count` (line 8041 onward), replace:

```c
OUT("{\"count\":%d}\n", n);          // → OUT("%d\n", n);
OUT("{\"count\":%zu}\n", n);         // → OUT("%zu\n", n);
```

Leave error emits unchanged (`{"error":"..."}` is the established error envelope).

- [ ] **Step 4.5: Run tests, confirm pass**

Run touched tests; expected PASS.

- [ ] **Step 4.6: Commit**

```bash
git add src/db/query.c tests/
git commit -m "$(cat <<'EOF'
break: count returns bare integer, no JSON wrapper

Errors still come as {"error":"..."} for shape-distinguishability.
size and orphaned reshape in the next commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: size → bare number; new `orphaned` mode

**Files:**
- Modify: `src/db/query.c:126` (`cmd_size`)
- Create: `cmd_orphaned` in `src/db/query.c` immediately after `cmd_size`
- Modify: `src/db/types.h` — add `int cmd_orphaned(const char *db_root, const char *object);` declaration
- Modify: `src/db/server.c` — add `orphaned` to the JSON mode dispatch and to the JSON CLI mode list (line 274)
- Modify: `src/db/main.c` — add `orphaned` CLI subcommand
- Test: `test-objlock.sh` (size assertions) + new test for orphaned

- [ ] **Step 5.1: Update size tests**

`grep -l '"orphaned":\|"count":' tests/*.sh | xargs grep -l 'size\b'` — any test that runs `./shard-db size`. Replace assertions:
- `'{"count":N}'` → `'N'`
- `'{"count":N,"orphaned":M}'` → `'N'` (orphan count goes elsewhere; size is now ONLY live count)
- `jq '.count'` → `jq '.'`

- [ ] **Step 5.2: Add new test for orphaned**

Create `tests/test-orphaned.sh` modeled on `tests/test-objlock.sh` (start server, create-object, insert N records, delete M records, assert `./shard-db orphaned dir obj` returns `M`, `./shard-db size dir obj` returns `N-M`).

Skeleton:
```bash
#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."
source ./bench/_helpers.sh 2>/dev/null || true   # check if a helper exists; otherwise inline

# Start server, etc — copy boilerplate from test-objlock.sh

DIR=t1
OBJ=orph
./shard-db query "{\"mode\":\"create-object\",\"dir\":\"$DIR\",\"object\":\"$OBJ\",\"fields\":[\"name:varchar:32\"]}" >/dev/null

for i in $(seq 1 10); do ./shard-db insert $DIR $OBJ k$i "{\"name\":\"n$i\"}" >/dev/null; done
for i in $(seq 1 4); do ./shard-db delete $DIR $OBJ k$i >/dev/null; done

got_size=$(./shard-db size $DIR $OBJ)
got_orph=$(./shard-db orphaned $DIR $OBJ)

[ "$got_size" = "6" ] || { echo "size expected 6, got $got_size"; exit 1; }
[ "$got_orph" = "4" ] || { echo "orphaned expected 4, got $got_orph"; exit 1; }
echo "ok"
```

Use the actual boilerplate from a working test as a template — the snippet above is illustrative.

- [ ] **Step 5.3: Run tests, confirm size FAILS, orphaned FAILS (mode doesn't exist)**

Run: `./build.sh && ./tests/test-objlock.sh && ./tests/test-orphaned.sh`
Expected: both FAIL — size on shape, orphaned on missing mode.

- [ ] **Step 5.4: Reshape `cmd_size`**

Edit `src/db/query.c:126-134`:

```c
int cmd_size(const char *db_root, const char *object) {
    int count = get_live_count(db_root, object);
    OUT("%d\n", count);
    return 0;
}
```

(Drop the `deleted` branch entirely — that's the new `cmd_orphaned`'s job.)

- [ ] **Step 5.5: Add `cmd_orphaned`**

Add immediately after `cmd_size`:

```c
int cmd_orphaned(const char *db_root, const char *object) {
    int deleted = get_deleted_count(db_root, object);
    OUT("%d\n", deleted);
    return 0;
}
```

- [ ] **Step 5.6: Add declaration to types.h**

Find the `cmd_size` declaration in `src/db/types.h` and add `cmd_orphaned` adjacent:

```c
int cmd_size(const char *db_root, const char *object);
int cmd_orphaned(const char *db_root, const char *object);
```

- [ ] **Step 5.7: Add `orphaned` JSON dispatch in server.c**

Find `cmd_size` dispatch in `src/db/server.c:1222`:

```c
} else if (strcmp(mode, "size") == 0) {
    cmd_size(db_root, object);
} else if (strcmp(mode, "orphaned") == 0) {
    cmd_orphaned(db_root, object);
}
```

Also add `"orphaned"` to the mode-list array near line 274 (the array used for help/discovery).

- [ ] **Step 5.8: Add `orphaned` CLI subcommand in main.c**

Find `cmd_size` CLI dispatch in `src/db/server.c:1761` (it's actually in server.c not main.c — confirm via grep). Mirror the pattern:

```c
} else if (strcasecmp(cmd, "orphaned") == 0) {
    cmd_orphaned(eff_root, object);
}
```

Update help text in `src/db/main.c` to mention `orphaned`.

- [ ] **Step 5.9: Run tests, confirm pass**

Run: `./build.sh && ./tests/test-objlock.sh && ./tests/test-orphaned.sh`

- [ ] **Step 5.10: Commit**

```bash
git add src/db/query.c src/db/server.c src/db/main.c src/db/types.h tests/test-orphaned.sh tests/
git commit -m "$(cat <<'EOF'
break: size returns bare live count; new orphaned mode

size emits the integer live record count, no JSON wrapper. orphaned is
a new mode that returns the deleted-but-not-vacuumed (tombstoned)
slot count. Both are O(1) metadata reads.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: find/fetch format:"dict"

**Files:**
- Modify: `src/db/query.c:4125` (`print_record_json`) and `cmd_fetch` (around line 4310) and `cmd_find` (line 8531).
- Test: new `tests/test-format-dict.sh`.

- [ ] **Step 6.1: Read each function fully before editing**

Run: `sed -n '4122,4220p' src/db/query.c`  (`print_record_json` + `print_record_row`)
Run: `sed -n '4310,4430p' src/db/query.c`  (`cmd_fetch`)
Run: `sed -n '8531,8770p' src/db/query.c`  (`cmd_find`)

Internalize the format dispatch pattern. `rows_fmt` and `csv_delim` are local ints set near the top of each function; the dict mode adds a third local `dict_fmt`.

- [ ] **Step 6.2: Add a `print_record_dict` helper**

Add immediately after `print_record_json` (around line 4153 in current numbering):

```c
/* Emit a record as a dict entry: "key":<value-json> (with leading comma when needed) */
void print_record_dict(const SlotHeader *hdr, const uint8_t *block,
                       const char **proj_fields, int proj_count,
                       int *printed, FieldSchema *fs) {
    char *key = malloc(hdr->key_len + 1);
    memcpy(key, block, hdr->key_len);
    key[hdr->key_len] = '\0';

    const char *raw = (const char *)block + hdr->key_len;

    OUT("%s\"%s\":", *printed ? "," : "", key);
    if (proj_count > 0) {
        OUT("{");
        int first = 1;
        for (int i = 0; i < proj_count; i++) {
            char *pv = decode_field(raw, hdr->value_len, proj_fields[i], fs);
            if (!pv) continue;
            OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[i], pv);
            first = 0;
            free(pv);
        }
        OUT("}");
    } else {
        char *val = decode_value(raw, hdr->value_len, fs);
        OUT("%s", val);
        free(val);
    }
    free(key);
    (*printed)++;
}
```

- [ ] **Step 6.3: Add `dict_fmt` parsing + dispatch in cmd_fetch**

Edit `cmd_fetch` near line 4310. Add:

```c
int rows_fmt = (format && strcmp(format, "rows") == 0);
int dict_fmt = (format && strcmp(format, "dict") == 0);
char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
```

Where the function currently emits `OUT("[")` to open the JSON array, branch on `dict_fmt`:

```c
if (dict_fmt) OUT("{");
else if (csv_delim) { /* csv header */ }
else if (rows_fmt) { /* rows envelope */ }
else OUT("[");
```

Where it currently calls `print_record_json`, branch:

```c
if (dict_fmt) print_record_dict(...);
else if (rows_fmt) print_record_row(...);
else if (csv_delim) print_record_csv(...);
else print_record_json(...);
```

Closing: `dict_fmt` → `OUT("}\n")`, default → `OUT("]\n")`.

**Cursor envelope handling** (when fetch supports cursor — confirm by reading): if cursor is active, emit `{"results":` then the dict/array, then `,"cursor":...,"...}\n"`. When `dict_fmt` is on with cursor, it's `{"results":{...},"cursor":...}`.

- [ ] **Step 6.4: Same dispatch in cmd_find**

Edit `cmd_find` (line 8531) the same way. Pay attention to the cursor branch (line 8599+) — emit `{"results":{...}}` instead of `{"results":[...]}` when `dict_fmt`.

Reject combos:
```c
if (dict_fmt && has_joins) {
    OUT("{\"error\":\"format=dict is not supported with join\"}\n");
    /* cleanup */
    return -1;
}
```

(Joins force tabular output; dict can't represent the joined columns cleanly.)

- [ ] **Step 6.5: Add test**

Create `tests/test-format-dict.sh`:

```bash
#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Boilerplate: start server, create object, seed records.
# (Copy the start/stop/create-object boilerplate from test-or-logic.sh.)

DIR=t1
OBJ=fmt
./shard-db query "{\"mode\":\"create-object\",\"dir\":\"$DIR\",\"object\":\"$OBJ\",\"fields\":[\"name:varchar:32\",\"age:int\"],\"indexes\":[\"age\"]}" >/dev/null
./shard-db insert $DIR $OBJ k1 '{"name":"alice","age":30}' >/dev/null
./shard-db insert $DIR $OBJ k2 '{"name":"bob","age":25}'   >/dev/null
./shard-db insert $DIR $OBJ k3 '{"name":"carol","age":40}' >/dev/null

# Fetch dict
got=$(./shard-db query "{\"mode\":\"fetch\",\"dir\":\"$DIR\",\"object\":\"$OBJ\",\"format\":\"dict\",\"limit\":10}")
echo "$got" | jq -e '.k1.name == "alice" and .k2.age == 25 and .k3.name == "carol"' >/dev/null

# Find dict (no cursor)
got=$(./shard-db query "{\"mode\":\"find\",\"dir\":\"$DIR\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"30\"}],\"format\":\"dict\"}")
echo "$got" | jq -e '.k1.age == 30 and .k3.age == 40 and (.k2 | not)' >/dev/null

# Find dict + cursor
got=$(./shard-db query "{\"mode\":\"find\",\"dir\":\"$DIR\",\"object\":\"$OBJ\",\"criteria\":[],\"format\":\"dict\",\"order_by\":\"age\",\"limit\":2,\"cursor\":null}")
echo "$got" | jq -e '.results | length == 2' >/dev/null
echo "$got" | jq -e '.cursor.age != null' >/dev/null

# Find dict + join → must error
got=$(./shard-db query "{\"mode\":\"find\",\"dir\":\"$DIR\",\"object\":\"$OBJ\",\"format\":\"dict\",\"join\":[{\"object\":\"unused\",\"local\":\"name\",\"remote\":\"key\",\"as\":\"x\"}]}")
echo "$got" | jq -e '.error | contains("dict")' >/dev/null

echo "ok"
```

- [ ] **Step 6.6: Build, run test**

Run: `./build.sh && ./tests/test-format-dict.sh`
Expected: PASS.

- [ ] **Step 6.7: Run the full suite to make sure no regression**

Run: `for t in tests/test-*.sh; do echo "==> $t"; bash $t || { echo "FAILED: $t"; exit 1; }; done`

- [ ] **Step 6.8: Commit**

```bash
git add src/db/query.c tests/test-format-dict.sh
git commit -m "$(cat <<'EOF'
feat: format=dict on find/fetch returns {key:value} shape

Sits alongside default array, format=rows, format=csv. Cursor envelope
preserved (results becomes a dict). Joins reject format=dict (forced
tabular). order_by allowed; document that JSON-spec dict order is
parser-dependent — use default array if strict ordering matters.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: ./migrate binary — extract migrate-files logic

**Files:**
- Create: `src/migrate/migrate.h`
- Create: `src/migrate/migrate_files.c`

- [ ] **Step 7.1: Read the existing cmd_migrate_files**

Run: `sed -n '9420,9700p' src/db/query.c`
Identify everything `cmd_migrate_files` calls into: dirs.conf parsing, schema.conf walk, file rename, etc. List every external symbol it depends on.

- [ ] **Step 7.2: Create src/migrate/migrate.h**

```c
#ifndef MIGRATE_H
#define MIGRATE_H

/* Walk every (dir, object) in $DB_ROOT/dirs.conf + schema.conf,
   lift pre-2026.05.2 XX/XX hash-bucketed files to flat layout.
   Idempotent: skips files already at the flat target. Returns 0 on
   success, nonzero on hard error. Prints progress to stdout. */
int migrate_files(const char *db_root);

#endif
```

- [ ] **Step 7.3: Create src/migrate/migrate_files.c**

Move the body of `cmd_migrate_files` (and its file-walking helpers) from `src/db/query.c` into `src/migrate/migrate_files.c`. Inline any small helpers it depends on (don't pull in the whole config.c). Include the minimum: `<stdio.h>`, `<stdlib.h>`, `<string.h>`, `<dirent.h>`, `<sys/stat.h>`, `<unistd.h>`, `<errno.h>`.

The function is filesystem-only — it reads `dirs.conf` (one line per allowed dir) and `schema.conf` (one line per object: `dir:object:splits:max_key:max_value:prealloc_mb`). Inline a tiny line-by-line parser; don't pull in the full schema cache machinery.

Output (stdout): `{"status":"migrated","objects_seen":N,"objects_migrated":M,"files_moved":F,"conflicts":C,"duration_ms":T}` (same shape as before, callers may scrape it).

- [ ] **Step 7.4: Delete cmd_migrate_files from query.c**

Remove lines 9439-9700ish in `src/db/query.c` (the full function and any helpers used ONLY by it).

Run: `grep -n 'migrate_files_to_flat\|cmd_migrate_files\|migrate-files' src/db/`
Confirm the only remaining hits are in:
- `src/db/server.c` (mode dispatch — to be removed in next task)
- `src/db/main.c` (CLI — to be removed)
- `src/db/types.h` (decl — to be removed)

If any helper function (e.g. `migrate_files_to_flat`) is now only used by the deleted `cmd_migrate_files`, delete it too.

- [ ] **Step 7.5: Build query.c standalone to confirm no broken refs**

Run: `gcc -c -O2 -Isrc/db src/db/query.c -o /tmp/query.o`
Expected: succeeds. If any `migrate_files_*` symbol is referenced elsewhere, fix.

- [ ] **Step 7.6: Commit**

```bash
git add src/migrate/migrate.h src/migrate/migrate_files.c src/db/query.c
git commit -m "$(cat <<'EOF'
refactor: move migrate-files logic out of daemon to src/migrate/

The migration is a one-shot upgrade step from pre-2026.05.2 XX/XX
hash-bucketed file layout to flat. It belonged in a separate binary
all along — it's filesystem-only, doesn't need the daemon's mmap or
threading model, and the dead code shouldn't ship with shard-db
forever.

Daemon dispatch (server.c) and CLI subcommand (main.c) wired off in
the next commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Strip migrate-files from shard-db

**Files:**
- Modify: `src/db/server.c:274` (mode list array), `:977-978` (dispatch), `:1761`-area (CLI dispatch — search for `migrate-files`).
- Modify: `src/db/main.c:55` (help text), `:131-134` (CLI dispatch).
- Modify: `src/db/types.h:857` — remove `cmd_migrate_files` declaration.

- [ ] **Step 8.1: Remove server.c JSON dispatch**

Edit `src/db/server.c:977-978`:

```c
// Remove:
if (mode && strcmp(mode, "migrate-files") == 0) {
    cmd_migrate_files(g_db_root);
    /* ... */
}
```

Remove the entry from the mode-list array (line 274).

- [ ] **Step 8.2: Remove CLI subcommand**

Edit `src/db/main.c:131-134`:

```c
// Remove:
if (strcmp(cmd, "migrate-files") == 0) {
    return cmd_query_json(port, "{\"mode\":\"migrate-files\"}");
}
```

And the help line at `:55`.

- [ ] **Step 8.3: Remove decl from types.h**

Edit `src/db/types.h:857`:

```c
// Remove:
int cmd_migrate_files(const char *db_root);
```

- [ ] **Step 8.4: Build shard-db, confirm clean**

Run: `./build.sh`
Expected: clean compile of `shard-db` (and `shard-cli`, untouched).

- [ ] **Step 8.5: Confirm CLI no longer accepts `migrate-files`**

Run: `./shard-db migrate-files 2>&1 | head -3`
Expected: error / unknown command (depending on existing dispatcher fallback).

- [ ] **Step 8.6: Run all tests except migrate-related**

Run: `for t in tests/test-*.sh; do [ "$t" = "tests/test-migrate-files.sh" ] && continue; bash $t || { echo "FAIL $t"; exit 1; }; done`

- [ ] **Step 8.7: Disable the old migrate-files test**

Rename `tests/test-migrate-files.sh` → `tests/test-migrate-files.sh.bak` (we'll replace it with `tests/test-migrate-binary.sh` in the next task). Or delete outright if it's already stale.

- [ ] **Step 8.8: Commit**

```bash
git add src/db/server.c src/db/main.c src/db/types.h tests/
git commit -m "$(cat <<'EOF'
break: remove migrate-files mode + CLI from shard-db daemon

Migration owned by ./migrate binary going forward. See
src/migrate/main.c (next commit). Existing test renamed pending
replacement.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: ./migrate binary main + build.sh

**Files:**
- Create: `src/migrate/main.c`
- Modify: `build.sh` — add migrate compile step.

- [ ] **Step 9.1: Write src/migrate/main.c**

Skeleton (read all comments; this is the orchestrator):

```c
/* migrate — one-shot per-release upgrade runner.
 *
 * For 2026.05.1:
 *   1. Acquire flock on $DB_ROOT/.shard-db.lock so no daemon runs.
 *   2. Run migrate_files() (FS-direct, no daemon needed).
 *   3. Release flock.
 *   4. Spawn `./shard-db start` (waits for daemon-ready).
 *   5. Send {"mode":"reindex"} over the configured PORT (TLS if enabled).
 *   6. Send shutdown signal — wait for daemon to exit cleanly.
 *   7. Exit 0 on success, nonzero on any step failure.
 *
 * Reads db.env from CWD. Prints progress + final summary to stdout.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>     /* flock */
#include <sys/wait.h>     /* waitpid */
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include "migrate.h"

/* TODO: read db.env and extract DB_ROOT, PORT, TLS_ENABLE, TLS_CA, TLS_SKIP_VERIFY, TOKEN.
   Reuse the parsing pattern from src/db/config.c — copy a minimal
   loader inline; don't link the full config.c. */

static int load_db_env(const char *path, char **db_root, int *port, int *tls);
static int acquire_lock(const char *db_root, int *out_fd);
static int spawn_daemon(void);                     /* fork+exec ./shard-db start */
static int wait_daemon_ready(int timeout_sec);     /* poll until status=running */
static int send_reindex(int port, int tls);        /* TCP write {"mode":"reindex"} */
static int stop_daemon(void);                      /* exec ./shard-db stop */

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    char *db_root = NULL; int port = 0, tls = 0;
    if (load_db_env("db.env", &db_root, &port, &tls) < 0) {
        fprintf(stderr, "migrate: failed to read db.env from CWD\n");
        return 1;
    }

    int lock_fd = -1;
    if (acquire_lock(db_root, &lock_fd) < 0) {
        fprintf(stderr, "migrate: cannot acquire %s/.shard-db.lock — is shard-db running?\n", db_root);
        return 1;
    }

    fprintf(stdout, "migrate: phase 1/2 — migrate-files (flatten XX/XX file layout)\n");
    if (migrate_files(db_root) != 0) {
        fprintf(stderr, "migrate: migrate_files failed\n");
        flock(lock_fd, LOCK_UN); close(lock_fd);
        return 1;
    }
    flock(lock_fd, LOCK_UN); close(lock_fd);  /* release before daemon takes it */

    fprintf(stdout, "migrate: phase 2/2 — reindex (rebuild B+ trees per shard)\n");
    if (spawn_daemon() < 0) { fprintf(stderr, "migrate: spawn_daemon failed\n"); return 1; }
    if (wait_daemon_ready(30) < 0) { fprintf(stderr, "migrate: daemon never came up\n"); return 1; }
    if (send_reindex(port, tls) < 0) {
        fprintf(stderr, "migrate: reindex failed\n");
        stop_daemon();
        return 1;
    }
    if (stop_daemon() < 0) {
        fprintf(stderr, "migrate: warning — daemon stop failed; check status manually\n");
    }

    fprintf(stdout, "migrate: complete\n");
    return 0;
}

/* ---- TODO: Implementations below.
       For TCP send_reindex, copy the connect/write/read pattern from
       src/cli/conn.c; that file is self-contained and supports TLS. ---- */
```

The TODOs in the skeleton above are real work — fill them in:
- `load_db_env`: open `db.env`, line-by-line shell-export parser. Extract `DB_ROOT`, `PORT`, `TLS_ENABLE`. (Existing `config.c:env_load` does this — copy that function inline; don't link config.c.)
- `acquire_lock`: `open($DB_ROOT/.shard-db.lock, O_CREAT|O_WRONLY, 0644)` then `flock(LOCK_EX|LOCK_NB)`. Same convention `cmd_server` uses.
- `spawn_daemon`: `fork()` → `execlp("./shard-db", "shard-db", "start", NULL)`. Or `system("./shard-db start")` — simpler.
- `wait_daemon_ready`: poll `./shard-db status` every 200ms up to N seconds.
- `send_reindex`: open TCP/TLS connection per `src/cli/conn.c`, write `{"mode":"reindex","timeout_ms":0}\n`, read response, parse `{"status":...}` or `{"error":...}`.
- `stop_daemon`: `system("./shard-db stop")`.

The migrate binary may call `system("./shard-db ...")` for start/stop — simpler than reimplementing — but it MUST do `send_reindex` over the wire to get a meaningful response (the existing CLI's `reindex` already wraps this; you can also `system("./shard-db reindex")` but you lose the structured response).

**Recommended**: use `system()` for spawn/stop, custom TCP for reindex (so we get a clean error on failure).

- [ ] **Step 9.2: Modify build.sh — add migrate target**

Edit `build.sh`. After the `gcc ... shard-cli ...` line, add:

```bash
# migrate — one-shot per-release upgrade runner. No daemon code; no
# ncurses; just FS ops + a TCP client for reindex.
gcc $MODE_CFLAGS -o migrate src/migrate/main.c src/migrate/migrate_files.c $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip migrate
```

Update the `cp` line to also copy `migrate`:

```bash
cp shard-db shard-cli migrate build/bin/
```

If `migrate`'s TCP/TLS path needs `pthread`, add `-lpthread`.

- [ ] **Step 9.3: Build, confirm artifacts**

Run: `./build.sh`
Expected: `build/bin/{shard-db,shard-cli,migrate,db.env.example}` present.

- [ ] **Step 9.4: Write end-to-end test**

Create `tests/test-migrate-binary.sh`:

```bash
#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# 1. Set up a fresh DB with the OLD XX/XX file layout (manually plant
#    files in $DB_ROOT/$DIR/$OBJ/files/{XX}/{XX}/<filename>).
# 2. Make sure the daemon is NOT running.
# 3. Run ./migrate.
# 4. Assert: files lifted to flat layout, indexes rebuilt, no daemon
#    leftover.

TMP=/tmp/test-migrate-$$
mkdir -p $TMP/db
export DB_ROOT=$TMP/db PORT=19999 TLS_ENABLE=0
cat > db.env <<EOF
export DB_ROOT="$TMP/db"
export PORT=$PORT
export TLS_ENABLE=0
EOF

# Boot, create object, insert test data, put files (using the daemon),
# stop daemon.
./shard-db start
./shard-db query "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"o\",\"fields\":[\"name:varchar:32\"],\"indexes\":[\"name\"]}"
./shard-db insert t o k1 '{"name":"alice"}'
./shard-db put-file t o foo.txt /etc/hostname
./shard-db stop

# Manually move the file back into XX/XX bucket form to simulate pre-2026.05.2.
# (Inspect actual layout to compute the bucket.)
flat="$TMP/db/t/o/files/foo.txt"
[ -f "$flat" ] || { echo "expected $flat to exist"; exit 1; }
mkdir -p "$TMP/db/t/o/files/aa/bb"
mv "$flat" "$TMP/db/t/o/files/aa/bb/foo.txt"

# Run migrate.
./migrate

# Assert: file lifted back to flat.
[ -f "$flat" ] || { echo "migrate did not lift foo.txt to flat"; exit 1; }
[ ! -d "$TMP/db/t/o/files/aa" ] || { echo "leftover aa/ bucket dir"; exit 1; }

# Assert: indexes rebuilt — query the index field.
./shard-db start
got=$(./shard-db query "{\"mode\":\"find\",\"dir\":\"t\",\"object\":\"o\",\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"alice\"}]}")
echo "$got" | jq -e 'length == 1' >/dev/null
./shard-db stop

rm -rf $TMP
echo "ok"
```

Adapt to actual XX/XX bucket scheme (read `migrate_files.c` to confirm the bucket path format).

- [ ] **Step 9.5: Run the test**

Run: `./tests/test-migrate-binary.sh`
Expected: PASS.

- [ ] **Step 9.6: Run full suite**

Run: `for t in tests/test-*.sh; do echo "==> $t"; bash $t || { echo "FAIL $t"; exit 1; }; done`

- [ ] **Step 9.7: Commit**

```bash
git add src/migrate/main.c build.sh tests/test-migrate-binary.sh
git commit -m "$(cat <<'EOF'
feat: ./migrate binary for per-release one-shot upgrade steps

For 2026.05.1: flattens XX/XX file layout, then spawns daemon and
sends reindex. Future releases extend src/migrate/main.c with the
relevant migrations and ship a new build.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Update CLAUDE.md + README

**Files:**
- Modify: `CLAUDE.md` (multiple sections)
- Modify: `README.md` (top-level usage, examples)

- [ ] **Step 10.1: CLAUDE.md — Commands section**

Find the `## Commands` block. Make these updates:
- Drop `./shard-db migrate-files` from the maintenance subsection.
- Add a new subsection "Migrations" right after Lifecycle:

```
# Migrations
./migrate                                         # One-shot upgrade from previous release
                                                  #   - lifts XX/XX file buckets to flat layout
                                                  #   - rebuilds all B+ tree indexes
                                                  # Run BEFORE starting the new shard-db. Daemon must be stopped.
```

- Add `./shard-db orphaned <dir> <obj>` next to `size`.

- [ ] **Step 10.2: CLAUDE.md — JSON query protocol section**

Update every example response shape:

For `get`:
```
Response: {...}   // bare value dict
Not found: {"error":"Not found"}
```

For `get-multi` (find the section, may be under Bulk or in JSON protocol):
```
Response: {"k1":{...},"k2":{...},"k3":null}   // missing keys map to null
```

For `exists` (single):
```
Response: true | false
```

For `count`:
```
Response: <integer>   // bare number
```

For `size`:
```
Response: <integer>   // live record count, O(1) metadata read
```

Add a new entry for `orphaned`:
```
### Orphaned (deleted-not-vacuumed slot count)

{"mode":"orphaned","dir":"...","object":"..."}
Response: <integer>  // tombstoned slots; vacuum reclaims these.
```

For `find`/`fetch`:
```
"format":"dict"  // {"k1":{...},"k2":{...}}; with cursor: {"results":{...},"cursor":...}
                 // Note: JSON dicts are unordered per spec; if you need strict order_by
                 // iteration across all clients, use default array or "rows".
                 // Rejected with join (joins force tabular).
```

For `aggregate`:
```
// no change
```

- [ ] **Step 10.3: CLAUDE.md — Daemon source files section**

Update the line for `query.c` to drop the migrate-files reference. Add a paragraph under `### shard-cli (src/cli/)` (or a new sibling section) for the migrate binary:

```
### migrate (src/migrate/)

One-shot binary for per-release upgrade steps. Filesystem-direct file-layout
migrations link in here; daemon-required migrations (reindex) are issued via
TCP after spawning shard-db. Builds to ./migrate at the repo root, copied to
build/bin/. Invoke before starting the new shard-db on an upgraded host.
```

- [ ] **Step 10.4: README.md — usage examples**

`grep -n '"key":\|"value":\|"count":\|"exists":\|migrate-files' README.md` — every hit gets fixed:
- Get response examples: drop wrapper.
- Multi-get response examples: dict shape.
- Exists single: bare bool.
- Count examples: bare number.
- Replace `./shard-db migrate-files` with `./migrate` in any upgrade snippet.

Add an "Upgrading" section near the top showing:
```
# Upgrade from prior release (2026.04.x → 2026.05.1)
./shard-db stop
# install new build/bin/ over your existing install
./migrate          # runs every required migration for this release
./shard-db start
```

- [ ] **Step 10.5: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "$(cat <<'EOF'
docs: update CLAUDE.md + README for 2026.05.1 response shapes

get → bare value dict, get-multi → key-keyed dict, exists single → bool,
count/size/orphaned → bare integer, find/fetch new format=dict.
Upgrade procedure documents the new ./migrate binary.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Release notes

**Files:**
- Create: `docs/release-notes/2026.05.1.md`

- [ ] **Step 11.1: Write release notes**

```markdown
# 2026.05.1 — reissued 2026-05-DD

This is a **breaking** release; the 2026.05.1 tag was reissued on this
date. If you pulled the prior 2026.05.1 build, replace it.

## Upgrade procedure

1. Stop the running daemon: `./shard-db stop`
2. Replace `build/bin/` with the new release artifacts.
3. Run `./migrate` (one-shot; idempotent if you re-run it).
4. Start the new daemon: `./shard-db start`

`./migrate` runs two phases:
- **migrate-files** — lifts pre-2026.05.2 `XX/XX/` file-storage hash buckets
  to flat layout. (Required only if you used `put-file` on prior versions.)
- **reindex** — rebuilds every B+ tree index under the new
  per-shard-btree layout shipped in 2026.05.1.

Total time: ~1 second per million records for reindex (parallelized per
shard); migrate-files is constant time per file.

## Breaking changes — response shapes

Read responses are now bare values where possible. Update your client.

### `get` (single)
**Before:** `{"key":"k1","value":{"name":"alice","age":30}}`
**After:**  `{"name":"alice","age":30}`

### `get` (multi-key)
**Before:** `[{"key":"k1","value":{...}},{"key":"k2","value":{...}}]`
**After:**  `{"k1":{...},"k2":{...}}`
Missing keys map to `null` (was `{"key":"k","error":"Not found"}` per entry).
Empty input → `{}` (was `[]`).

### `exists` (single)
**Before:** `{"exists":true}`
**After:**  `true`
(Multi-key `exists` keeps `{"k1":true,"k2":false}` shape.)

### `count`
**Before:** `{"count":42}`
**After:**  `42`

### `size`
**Before:** `{"count":42}` or `{"count":42,"orphaned":3}`
**After:**  `42`   (live records only)
Use the new `orphaned` mode for the deleted/tombstoned count.

### New: `orphaned`
`{"mode":"orphaned","dir":"...","object":"..."}` → `3` (bare integer).
O(1) metadata read.

### `find`, `fetch` — new `format:"dict"`
Default array shape, `format:"rows"`, and `format:"csv"` are unchanged.
`format:"dict"` returns `{"k1":{...},"k2":{...}}`. With cursor active:
`{"results":{...},"cursor":...}`. Rejected with `join` (joins force tabular).

JSON dict iteration order is parser-dependent. If you need strict
`order_by` iteration across all clients, use the default array or
`format:"rows"`.

### Errors
Errors continue to use `{"error":"..."}`. Distinguishability vs. bare
values: any client should branch on JSON type — object with `error`
key vs. bare scalar/array/dict.

## Removed

- `./shard-db migrate-files` — moved to `./migrate`.
- JSON `{"mode":"migrate-files"}` — removed from daemon dispatch.

## Other changes

(Carry forward from 2026.05.1's original notes if any: per-shard btree,
native TLS 1.3, per-tenant + per-object auth, OR + AND-intersection,
find-cursor, TUI client, 38 search operators, Coverity sweep.)

## Skipped / future

`aggregate` does not gain `format:"dict"` in this release; the no-group
case is already dict-shaped and the multi-column-group case has no
clean dict mapping. Revisit if there is concrete demand.
```

- [ ] **Step 11.2: Commit**

```bash
git add docs/release-notes/2026.05.1.md
git commit -m "$(cat <<'EOF'
docs: add 2026.05.1 reissue release notes

Documents the breaking response-shape changes and the ./migrate
upgrade procedure for users coming from 2026.04.x.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: Final regression sweep

- [ ] **Step 12.1: Run every test**

```bash
./build.sh
for t in tests/test-*.sh; do
  echo "==> $t"
  bash "$t" || { echo "FAILED: $t"; exit 1; }
done
echo "all 32 tests pass"
```

- [ ] **Step 12.2: Smoke-test TUI**

`./shard-cli` — exercise:
- Server status
- Browse → describe-object
- Query → get a single key (confirm flat render)
- Query → find with criteria (confirm table render)
- Stats → live refresh

- [ ] **Step 12.3: Smoke-test migrate end-to-end on a fresh DB**

```bash
mkdir -p /tmp/sd-up && cd /tmp/sd-up
cp -r /path/to/shard-db/build/bin/* .
cp db.env.example db.env
./shard-db start
./shard-db query '{"mode":"create-object","dir":"t","object":"o","fields":["a:int"],"indexes":["a"]}'
for i in $(seq 1 100); do ./shard-db insert t o k$i "{\"a\":$i}" >/dev/null; done
./shard-db stop

# Pretend we're upgrading: run migrate.
./migrate
./shard-db start
./shard-db query '{"mode":"find","dir":"t","object":"o","criteria":[{"field":"a","op":"eq","value":"42"}]}'
# Expect a single record back.
./shard-db stop
```

---

## Task 13: Reissue tag — STOP AND ASK USER FIRST

- [ ] **Step 13.1: Check user explicitly approved before deleting tag**

This is destructive: deleting the `2026.05.1` tag on origin discards the prior signed release. Even though the user said "no one is using it", confirm in this session right before running the commands.

- [ ] **Step 13.2: Delete tag locally and on origin**

```bash
git tag -d 2026.05.1
git push origin :refs/tags/2026.05.1
```

- [ ] **Step 13.3: Tag the merge commit at HEAD**

```bash
git tag -s 2026.05.1 -m "2026.05.1 — reissued (response-shape overhaul + ./migrate)"
git push origin 2026.05.1
```

(`-s` if signing keys are configured; otherwise `git tag -a`.)

- [ ] **Step 13.4: Trigger release workflow**

If the `release.yml` GitHub Actions workflow is keyed on tag push, this step is automatic. Otherwise, run `gh workflow run release.yml --ref 2026.05.1`. Confirm artifacts (cosign signing) match expectation.

---

## Self-review checklist

After finishing the plan above, before claiming done:

1. **Spec coverage** — every read mode the user signed off on (get, get-multi, exists single, count, size, new orphaned, find/fetch dict) has a task. Aggregate explicitly NOT touched. Writes/admin explicitly NOT touched.
2. **No placeholders** — every step has actual code or actual commands. The migrate binary skeleton has `TODO`s; those are flagged in step 9.1's notes for the executor.
3. **Type consistency** — `cmd_orphaned`, `print_record_dict`, declarations match where they're used.
4. **Tests for every shape change** — yes (all touch existing tests; new tests added for orphaned, format=dict, migrate binary).
5. **Docs covered** — CLAUDE.md, README, release notes.
6. **Tag reissue gated on user approval** — yes (Task 13 step 1).
