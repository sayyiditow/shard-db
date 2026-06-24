# size: Return Disk Bytes Instead of Record Count

> **For agentic workers:** implement this plan task-by-task in order. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change `{"mode":"size"}` (and `./shard-db size`) to return actual bytes used on disk rather than the live record count, making it distinct from `count` and useful for confirming varlen migration savings.

**Architecture:** Replace `cmd_size`'s body in `query.c` with a recursive directory walk (`opendir`/`readdir`/`lstat`) over the object directory, summing `st_blocks * 512` (same accounting as `du`). Thread-safe via a stack-allocated accumulator passed by pointer — no global state, no `nftw`.

**Branch:** branch off `main` — do NOT use `feat/variable-length-records`.

## Global Constraints

- Build: `SKIP_TESTS=1 ./build.sh` — zero warnings, zero errors.
- Test: `./build/bin/shard-db-test run-all` — `# total: N passed, 0 failed`.
- Never claim a step passed without pasting real terminal output.
- If a quoted anchor is not found exactly in the file, stop and write `PLAN_NOTES.md` — do not guess.
- No commits until user approves.

---

## Task 1 — Replace `cmd_size` in `src/db/query.c`

**Files:**
- Modify: `src/db/query.c`

### Background

`cmd_size` currently calls `get_live_count` — identical to `count` with no criteria. The new implementation uses a recursive `dir_du` helper that sums `st_blocks * 512` for every regular file under the object directory (data/kf/, data/streams/, indexes/, etc.). `st_blocks` is in 512-byte units on all POSIX platforms, matching `du -sb` behaviour. Sparse files (the old fixed 128 MB segment files) report only their written blocks.

Headers already present in `query.c`: `<sys/stat.h>`, `<dirent.h>`, `<string.h>`. If any are missing, add them — no new dependencies are required.

- [ ] **Step 1: Add `dir_du` helper immediately before `cmd_size`**

  Locate the anchor:
  ```c
  int cmd_size(const char *db_root, const char *object) {
      OUT("%d\n", get_live_count(db_root, object));
      return 0;
  }
  ```

  Replace with:
  ```c
  static void dir_du(const char *path, int64_t *acc) {
      DIR *d = opendir(path);
      if (!d) return;
      struct dirent *e;
      while ((e = readdir(d))) {
          if (e->d_name[0] == '.' &&
              (e->d_name[1] == '\0' ||
               (e->d_name[1] == '.' && e->d_name[2] == '\0'))) continue;
          char sub[PATH_MAX];
          snprintf(sub, sizeof(sub), "%s/%s", path, e->d_name);
          struct stat st;
          if (lstat(sub, &st) != 0) continue;
          if (S_ISREG(st.st_mode))
              *acc += (int64_t)st.st_blocks * 512;
          else if (S_ISDIR(st.st_mode))
              dir_du(sub, acc);
      }
      closedir(d);
  }

  int cmd_size(const char *db_root, const char *object) {
      char obj_dir[PATH_MAX];
      snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);
      int64_t bytes = 0;
      dir_du(obj_dir, &bytes);
      OUT("%lld\n", (long long)bytes);
      return 0;
  }
  ```

- [ ] **Step 2: Check that `<dirent.h>` is included in `query.c`**

  Run:
  ```bash
  grep -n '#include.*dirent' src/db/query.c
  ```

  If the output is empty, add `#include <dirent.h>` alongside the existing `<sys/stat.h>` include in that file.

- [ ] **Step 3: Build and verify**

  ```bash
  SKIP_TESTS=1 ./build.sh
  ```

  Expected: `BUILD OK` with zero warnings. Paste output.

- [ ] **Step 4: Smoke-test**

  ```bash
  ./shard-db start
  ./shard-db size hn stories
  ./shard-db stop
  ```

  Expected: a positive integer (bytes, not a small record-count). Cross-check:
  ```bash
  du -sb db/hn/stories/
  ```
  Both numbers should be within a few KB of each other (minor difference from in-progress segment files whose trailing sparse pages aren't yet written). Paste both.

---

## Task 2 — Update tests

**Files:**
- Modify: `src/test/cases/test_bare_shapes.c`
- Modify: `src/test/cases/test_parallel_index_integrity.c`
- Modify: `src/test/cases/test_auto_vacuum.c`
- Modify: `src/test/cases/test_bulk_cas.c`
- Modify: `src/test/cases/test_bulk_update_delimited.c`
- Modify: `src/test/cases/test_schema_export.c`

### Background

The old `cmd_size` returned live record count. Seven test files assert against `size` in ways that will now fail or be fragile. The fix strategy depends on intent:

- Tests that use `size` to validate a **record count** → switch `"mode":"size"` to `"mode":"count"` (the assertion itself is unchanged and stays correct).
- `test_bare_shapes.c` tests the **response shape** of `size` specifically → keep `size` mode but change the assertion to `atoll(resp) > 0`.
- `test_bulk_cas.c` dry-run comparison → keep `size` mode (validates "bytes unchanged after dry run") but switch `atoi` to `atoll` since disk bytes can exceed INT_MAX on real databases.
- `test_schema_export.c:179` asserts an empty object has 0 records → switch to `count` (an empty object still has kf shard files on disk so bytes > 0; `ASSERT_CONTAINS(resp, "0")` on e.g. "4096" is luck).

- [ ] **Step 1: Fix `src/test/cases/test_bare_shapes.c`**

  Locate:
  ```c
      /* size → bare live count */
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"shape_t\"}", &resp);
      ASSERT_TRUE(eq_str(resp, "3"), "size = 3 live");
      free(resp); resp = NULL;
  ```

  Replace with:
  ```c
      /* size → bare disk bytes (positive integer) */
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"shape_t\"}", &resp);
      ASSERT_TRUE(atoll(resp) > 0, "size > 0 (disk bytes)");
      free(resp); resp = NULL;
  ```

  Then locate:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"shape_t\"}", &resp);
      ASSERT_TRUE(eq_str(resp, "2"), "size = 2 (live drops on delete)");
      free(resp); resp = NULL;
  ```

  Replace with:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"shape_t\"}", &resp);
      ASSERT_TRUE(atoll(resp) > 0, "size > 0 after delete");
      free(resp); resp = NULL;
  ```

- [ ] **Step 2: Fix `src/test/cases/test_parallel_index_integrity.c`**

  Locate:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"idxtest\"}", &resp);
      ASSERT_EQ_INT(tu_parse_count(resp), TOTAL, "100000 records present");
  ```

  Replace with:
  ```c
      tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"idxtest\"}", &resp);
      ASSERT_EQ_INT(tu_parse_count(resp), TOTAL, "100000 records present");
  ```

- [ ] **Step 3: Fix `src/test/cases/test_auto_vacuum.c`**

  Locate:
  ```c
          /* Live count for big unchanged (vacuum reclaims tombstones, not live). */
          tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"big\"}", &resp);
          ASSERT_EQ_INT(tu_parse_count(resp), 20, "big size=20 (50 - 30 deleted)");
  ```

  Replace with:
  ```c
          /* Live count for big unchanged (vacuum reclaims tombstones, not live). */
          tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"big\"}", &resp);
          ASSERT_EQ_INT(tu_parse_count(resp), 20, "big count=20 (50 - 30 deleted)");
  ```

- [ ] **Step 4: Fix `src/test/cases/test_bulk_cas.c` — record count assertions**

  Locate:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
      ASSERT_CONTAINS(resp, "5", "after JSON CAS: 5 records");
      free(resp); resp = NULL;
  ```

  Replace with:
  ```c
      tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
      ASSERT_CONTAINS(resp, "5", "after JSON CAS: 5 records");
      free(resp); resp = NULL;
  ```

  Then locate:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
      ASSERT_CONTAINS(resp, "9", "after CSV CAS: 9 records total");
      free(resp); resp = NULL;
  ```

  Replace with:
  ```c
      tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
      ASSERT_CONTAINS(resp, "9", "after CSV CAS: 9 records total");
      free(resp); resp = NULL;
  ```

- [ ] **Step 5: Fix `src/test/cases/test_bulk_cas.c` — dry-run comparison**

  Locate:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
      char size_before[64] = {0};
      if (resp) {
          const char *p = resp; while (*p == ' ' || *p == '\n') p++;
          snprintf(size_before, sizeof(size_before), "%d", atoi(p));
      }
      free(resp); resp = NULL;
  ```

  Replace with:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
      char size_before[64] = {0};
      if (resp) {
          const char *p = resp; while (*p == ' ' || *p == '\n') p++;
          snprintf(size_before, sizeof(size_before), "%lld", (long long)atoll(p));
      }
      free(resp); resp = NULL;
  ```

  Then locate:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
      {
          const char *p = resp; while (p && (*p == ' ' || *p == '\n')) p++;
          char after[64]; snprintf(after, sizeof(after), "%d", atoi(p));
          ASSERT_TRUE(strcmp(size_before, after) == 0, "size unchanged after dry runs");
      }
  ```

  Replace with:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"castest\"}", &resp);
      {
          const char *p = resp; while (p && (*p == ' ' || *p == '\n')) p++;
          char after[64]; snprintf(after, sizeof(after), "%lld", (long long)atoll(p));
          ASSERT_TRUE(strcmp(size_before, after) == 0, "size unchanged after dry runs");
      }
  ```

- [ ] **Step 6: Fix `src/test/cases/test_bulk_update_delimited.c`**

  Locate:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"udtest\"}", &resp);
      ASSERT_CONTAINS(resp, "5", "seeded 5 records"); free(resp); resp = NULL;
  ```

  Replace with:
  ```c
      tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"udtest\"}", &resp);
      ASSERT_CONTAINS(resp, "5", "seeded 5 records"); free(resp); resp = NULL;
  ```

  Then locate:
  ```c
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"udtest\"}", &resp);
      ASSERT_CONTAINS(resp, "5", "record count still 5");
      free(resp); resp = NULL;
  ```

  Replace with:
  ```c
      tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"udtest\"}", &resp);
      ASSERT_CONTAINS(resp, "5", "record count still 5");
      free(resp); resp = NULL;
  ```

- [ ] **Step 7: Fix `src/test/cases/test_schema_export.c`**

  Locate:
  ```c
      /* Imported objects start empty. */
      tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"mig_users\"}", &resp);
      ASSERT_CONTAINS(resp, "0", "mig_users empty after import");
      free(resp); resp = NULL;
  ```

  Replace with:
  ```c
      /* Imported objects start empty. */
      tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"mig_users\"}", &resp);
      ASSERT_CONTAINS(resp, "0", "mig_users empty after import");
      free(resp); resp = NULL;
  ```

- [ ] **Step 8: Build and run the full test suite**

  ```bash
  SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all
  ```

  Expected: zero warnings + `# total: N passed, 0 failed`. Paste the summary line.

---

## Task 3 — Update help text and docs

**Files:**
- Modify: `src/db/main.c`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update `main.c` help text**

  Locate in `src/db/main.c`:
  ```c
          fprintf(stderr, "  size <object>                        Live record count (O(1) metadata)\n");
  ```

  Replace with:
  ```c
          fprintf(stderr, "  size <object>                        Disk bytes used by object files (same as du -sb)\n");
  ```

- [ ] **Step 2: Update the CLI commands table in `CLAUDE.md`**

  Locate:
  ```
  ./shard-db size | orphaned <dir> <obj>                          # bare integers (O(1) metadata)
  ```

  Replace with:
  ```
  ./shard-db size <dir> <obj>                                     # disk bytes used by the object (all data + index files); same as du -sb
  ./shard-db orphaned <dir> <obj>                                 # deleted record count (O(1) metadata)
  ```

- [ ] **Step 3: Update the response shapes table**

  Locate:
  ```
  | `count`, `size`, `orphaned` | bare integer |
  ```

  Replace with:
  ```
  | `count`, `orphaned` | bare integer (record count) |
  | `size` | bare integer (disk bytes, same accounting as `du -sb`) |
  ```

- [ ] **Step 4: Build once more to confirm no regressions crept in**

  ```bash
  SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all
  ```

  Expected: zero warnings + `# total: N passed, 0 failed`. Paste the summary line.
