# Embedded Migration Plan

> **For agentic workers:** implement this plan task-by-task in order. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose varlen migration to embedded (C) and npm consumers — auto-runs at startup and is callable as `{"mode":"migrate"}` query.

**Branch:** `feat/variable-length-records` — do NOT create a new branch.

**Architecture:**
- Embedded `shard_db_open` auto-migrates all FIXED objects before thread pools start (offline, no registry entries open yet, idempotent).
- `{"mode":"migrate","dir":"...","object":"..."}` query handler added to `dispatch_json_query` in `server.c` — used by npm callers who want per-object explicit control or by TCP clients. Takes exclusive schema wrlock (via `mode_is_schema`), operates on the registry's live `SlotcaskDb *`.
- npm gains a `migrate(dir, object)` convenience wrapper and updated TypeScript types.

## Global Constraints

- Build: `SKIP_TESTS=1 ./build.sh` — must produce zero warnings and zero errors.
- Test: `./build/bin/shard-db-test run-all` — must show `# total: N passed, 0 failed`.
- Never claim a step passed without pasting the real terminal output.
- If a quoted anchor is not found exactly in the file, **stop and write `PLAN_NOTES.md`** — do not guess or reinterpret.
- All edits on `feat/variable-length-records`. No commits unless user says to.

---

## Task 1 — `{"mode":"migrate"}` handler in `src/db/server.c`

**Files:**
- Modify: `src/db/server.c`

### Background

`mode_is_schema()` (near the top of `server.c`) gates exclusive write-lock acquisition before dispatch. Any mode listed there gets serialised against concurrent queries on the same object. `dispatch_json_query` extracts `db_root` (effective root = `$DB_ROOT/<dir>`) and `object` early; all maintenance handlers receive these values.

`slotcask_registry_get(eff_root, object, &info)` returns a borrowed `SlotcaskDb *` owned by the registry — **never call `slotcask_close` on it**. The `info` struct carries the schema dimensions. `slotcask_migrate_to_varlen(sdb)` modifies the instance in-place (updates `sdb->format`, repoints KF entries, deletes old segment files). Subsequent registry use automatically operates in VARIABLE mode.

- [ ] **Step 1: Add `"migrate"` to `mode_is_schema`**

  Locate the line:
  ```c
           strcasecmp(m, "migrate-storage-version") == 0;
  ```
  Replace with:
  ```c
           strcasecmp(m, "migrate-storage-version") == 0 ||
           strcasecmp(m, "migrate") == 0;
  ```

- [ ] **Step 2: Add the dispatch handler**

  Locate the block:
  ```c
    } else if (strcmp(mode, "vacuum") == 0) {
        /* Optional flags: "compact":true and "splits":N route to rebuild_object;
           no flags means fast in-place tombstone reclaim. */
        char *splits_s  = json_obj_strdup(&req, "splits");
        int compact = json_obj_is_true(&req, "compact");
        int new_splits = splits_s ? atoi(splits_s) : 0;
        cmd_vacuum(db_root, object, compact, new_splits);
        free(splits_s);
    } else if (strcmp(mode, "rename-field") == 0) {
  ```

  Insert a new branch between `vacuum` and `rename-field`:
  ```c
    } else if (strcmp(mode, "migrate") == 0) {
        /* Migrate one object from FIXED to VARIABLE segment format.
           Idempotent — returns migrated:false if already VARIABLE.
           Exclusive schema wrlock (mode_is_schema) serialises against
           concurrent queries; the registry instance is updated in-place
           so no registry invalidation is needed. */
        Schema sch = load_schema(db_root, object);
        if (sch.splits <= 0) {
            OUT("{\"error\":\"object not found in schema\"}\n");
        } else {
            SlotcaskSchemaInfo info = {
                .splits    = sch.splits,
                .slot_size = sch.slot_size,
                .streams   = sch.streams,
            };
            SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
            if (!sdb) {
                OUT("{\"error\":\"failed to open object\"}\n");
            } else if (sdb->format == SLOTCASK_FORMAT_VARIABLE) {
                OUT("{\"status\":\"ok\",\"migrated\":false}\n");
            } else {
                int mrc = slotcask_migrate_to_varlen(sdb);
                if (mrc != 0)
                    OUT("{\"error\":\"migration failed\"}\n");
                else
                    OUT("{\"status\":\"ok\",\"migrated\":true}\n");
            }
        }
  ```

  The final `} else if (strcmp(mode, "rename-field") == 0) {` line closes this branch — no extra brace needed.

- [ ] **Step 3: Build and verify**
  ```
  SKIP_TESTS=1 ./build.sh
  ```
  Expected: `BUILD OK` with zero warnings. Paste output.

- [ ] **Step 4: Smoke-test via CLI**

  Start the server, then:
  ```bash
  ./shard-db query '{"mode":"migrate","dir":"default","object":"users"}'
  ```
  Expected: `{"status":"ok","migrated":false}` (already VARIABLE).
  Stop the server. Paste output.

---

## Task 2 — Auto-migration in `src/db/embedded.c`

**Files:**
- Modify: `src/db/embedded.c`

### Background

`shard_db_open` calls `shard_db_open_internal` (which sets `g_db`, initialises all caches including `slotcask_init`) and then starts thread pools. Migration must happen after `shard_db_open_internal` (caches ready) and before `parallel_pool_init` (no concurrent registry access yet). At this point no registry entries exist, so we open a fresh `SlotcaskDb` per object, migrate, then close it — the registry will open the already-VARIABLE object on first query.

Schema parsing mirrors `src/migrate/main.c` exactly. `load_schema(eff_root, obj)` where `eff_root = db_root/dir`. `slotcask_open(&sdb, obj_data, sch.splits, sch.streams, sch.slot_size)` — parameter order confirmed from `main.c:165`.

- [ ] **Step 1: Add `run_startup_migration` before `shard_db_open`**

  Locate the line:
  ```c
  /* ── Public API ── */
  ```

  Insert the following function immediately before it:
  ```c
  /* Migrate every registered object still in FIXED segment format to
     VARIABLE format.  Called from shard_db_open before thread pools
     start, so no registry entries are open and slotcask_close is safe.
     Logs progress to stderr.  Returns 0 on success, -1 if any object
     fails (shard_db_open will refuse to proceed). */
  static int run_startup_migration(const char *db_root) {
      char schema_path[PATH_MAX];
      snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
      FILE *f = fopen(schema_path, "r");
      if (!f) return 0; /* no schema.conf — nothing to migrate */

      char line[4096];
      int failed = 0;
      while (!failed && fgets(line, sizeof(line), f)) {
          line[strcspn(line, "\n")] = '\0';
          char *p = line;
          while (*p == ' ' || *p == '\t') p++;
          if (*p == '#' || !*p) continue;

          /* Format: dir:object:splits:max_key:2:streams[...] */
          char *c1 = strchr(p, ':');
          if (!c1) continue;
          *c1 = '\0';
          char *c2 = strchr(c1 + 1, ':');
          if (!c2) continue;
          *c2 = '\0';

          const char *dir = p;
          const char *obj = c1 + 1;

          char obj_data[PATH_MAX];
          snprintf(obj_data, sizeof(obj_data), "%s/%s/%s", db_root, dir, obj);

          /* Skip objects with no materialised data directory. */
          char kf_probe[PATH_MAX];
          snprintf(kf_probe, sizeof(kf_probe), "%s/data/kf", obj_data);
          struct stat kf_st;
          if (stat(kf_probe, &kf_st) != 0) continue;

          char eff_root[PATH_MAX];
          snprintf(eff_root, sizeof(eff_root), "%s/%s", db_root, dir);
          Schema sch = load_schema(eff_root, obj);
          if (sch.splits <= 0) continue;

          SlotcaskDb sdb;
          if (slotcask_open(&sdb, obj_data, sch.splits, sch.streams, sch.slot_size) != 0)
              continue;

          if (sdb.format == SLOTCASK_FORMAT_VARIABLE) {
              slotcask_close(&sdb);
              continue;
          }

          fprintf(stderr, "[shard-db] migrating %s/%s...\n", dir, obj);
          int mrc = slotcask_migrate_to_varlen(&sdb);
          slotcask_close(&sdb);
          if (mrc != 0) {
              fprintf(stderr, "[shard-db] migration failed for %s/%s\n", dir, obj);
              failed = 1;
          } else {
              fprintf(stderr, "[shard-db] migrated %s/%s\n", dir, obj);
          }
      }
      fclose(f);
      return failed ? -1 : 0;
  }
  ```

- [ ] **Step 2: Call `run_startup_migration` in `shard_db_open`**

  Locate the comment:
  ```c
      /* Expose instance before starting pools so pool_worker / io_pool_worker
         can bind their thread-local g_db on entry. */
      g_shard_db_instance = db;
  ```

  Insert immediately after the `g_shard_db_instance = db;` assignment:
  ```c
      /* Auto-migrate any FIXED-format objects before thread pools start.
         Migration is offline at this point — no registry entries open. */
      if (run_startup_migration(db_root) != 0) {
          fprintf(stderr, "shard_db_open: startup migration failed\n");
          g_shard_db_instance = NULL;
          g_db = NULL;
          /* Thread pools not yet started — call shutdown helpers that
             are safe on uninitialised state, skip parallel_pool_shutdown. */
          bt_cache_shutdown();
          bm_cache_shutdown();
          slotcask_shutdown();
          ucache_shutdown();
          free(db->token_set);
          free(db->token_scope);
          free(db->token_scope_obj);
          free(db->token_perm);
          free(db->token_set_used);
          db_mutexes_destroy();
          if (db->slots_inited) sem_destroy(&db->query_slots);
          free(db);
          atomic_store(&g_instance_open, 0);
          return NULL;
      }
  ```

  **Verify** that the `free(db->token_*)` fields listed above match exactly the ones in `shard_db_close` below the `bt_cache_shutdown()` block. If the list differs (fields added/removed), match `shard_db_close` exactly.

- [ ] **Step 3: Build and verify**
  ```
  SKIP_TESTS=1 ./build.sh
  ```
  Expected: zero warnings, zero errors. Paste output.

- [ ] **Step 4: Run test suite**
  ```
  ./build/bin/shard-db-test run-all
  ```
  Expected: `# total: N passed, 0 failed`. Paste the summary line.

---

## Task 3 — npm: TypeScript types + `migrate()` convenience method

**Files:**
- Modify: `npm/index.js`
- Modify: `npm/index.d.ts`

No changes to `npm/src/binding.c` — migration goes through the existing `napi_query` path.

- [ ] **Step 1: Add `migrate()` to `index.js`**

  Locate:
  ```js
    close() {
      binding.close(this._handle)
    }
  ```

  Insert before `close()`:
  ```js
    migrate(dir, object) {
      return this.query({ mode: 'migrate', dir, object })
    }

  ```

- [ ] **Step 2: Add `migrate` to `QueryBody` union in `index.d.ts`**

  Locate:
  ```ts
    // ── Maintenance ───────────────────────────────────────────────────────────
    | { mode: 'truncate'
        dir: string; object: string }

    | { mode: 'vacuum'
        dir: string; object: string }
  ```

  Replace with:
  ```ts
    // ── Maintenance ───────────────────────────────────────────────────────────
    | { mode: 'truncate'
        dir: string; object: string }

    | { mode: 'vacuum'
        dir: string; object: string }

    | { mode: 'migrate'
        dir: string; object: string }
  ```

- [ ] **Step 3: Add `migrate()` method declaration to the class in `index.d.ts`**

  Locate:
  ```ts
    /** Close the database and release all resources. */
    close(): void
  ```

  Insert before `close()`:
  ```ts
    /**
     * Migrate one object from fixed-slot to variable-length segment format.
     * Idempotent — safe to call on already-migrated objects (returns immediately).
     * Resolves to `{"status":"ok","migrated":true}` on success,
     * `{"status":"ok","migrated":false}` if already variable-length,
     * or `{"error":"..."}` on failure.
     * Called automatically during construction; use this for explicit per-object control.
     */
    migrate(dir: string, object: string): Promise<string>

  ```

- [ ] **Step 4: Build npm addon and verify types compile**
  ```bash
  cd npm && npm run build && cd ..
  ```
  Expected: native addon rebuilt without errors. Paste output.

- [ ] **Step 5: Smoke-test from JS**
  ```bash
  cd npm && node -e "
  const ShardDb = require('.')
  const db = new ShardDb('../db')
  db.migrate('default', 'users').then(r => { console.log(r); db.close() })
  " && cd ..
  ```
  Expected: `{"status":"ok","migrated":false}` (already VARIABLE). Paste output.
