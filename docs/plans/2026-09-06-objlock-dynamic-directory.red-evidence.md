# Red evidence — objlock table exhaustion (fail-open path)

Captured 2026-09-15, Task 1 Step 1 of
[2026-09-06-objlock-dynamic-directory.md](2026-09-06-objlock-dynamic-directory.md).

## Procedure

1. `git worktree add /tmp/shard-db-red-base 676d7a5` — pristine base
   (main @ `676d7a5`, pre-fix objlock).
2. In the worktree only: `OBJLOCK_BUCKETS` 256 → **4** in
   `src/db/shard_db_internal.h` (one line, reverted with the worktree).
3. Temporary red variant `src/test/cases/test_objlock_dynamic_growth.c`
   (statement-form lock calls only — the base API is `void`-returning and
   has no `objlock_test_set_fail_alloc`), registered in build.sh, case
   name `test-objlock-red-base`. The variant creates 2000 distinct
   `growth-root:obj-N` keys, then parks a reader in `objlock_rdlock` on
   `obj-1500` (past the old 256 ceiling) while main invokes
   `objlock_wrlock`/`objlock_wrunlock` on the same key.
4. `SKIP_TESTS=1 ./build.sh`, then
   `./build/bin/shard-db-test run test-objlock-red-base`.
5. Worktree removed afterwards; nothing above exists in the fix branch.

## Result

**3996 occurrences of the fail-open log line** in one run:

```
ERROR [server] objlock get_lock: table full (4 buckets), object 'growth-root:obj-4' will run WITHOUT rwlock protection
```

3996 = 3992 (keys obj-4..obj-1999, each lock+unlock resolving to NULL on
the full table) + 4 (the mutual-exclusion probe: the parked reader's
rdlock/rdunlock and main's wrlock/wrunlock on `obj-1500`). All four
probe calls on the probe key itself ran **without any rwlock
protection**:

```
ERROR [server] objlock get_lock: table full (4 buckets), object 'growth-root:obj-1500' will run WITHOUT rwlock protection
ERROR [server] objlock get_lock: table full (4 buckets), object 'growth-root:obj-1500' will run WITHOUT rwlock protection
ERROR [server] objlock get_lock: table full (4 buckets), object 'growth-root:obj-1500' will run WITHOUT rwlock protection
ERROR [server] objlock get_lock: table full (4 buckets), object 'growth-root:obj-1500' will run WITHOUT rwlock protection
```

That is the fail-open path firing deterministically: past the fixed
table's capacity, `get_lock()` returned NULL and every caller proceeded
unprotected — the exact use-after-free window the objlock exists to
prevent. The post-fix `test-objlock-dynamic-growth` case proves the same
scenario succeeds with real mutual exclusion at the default capacity.
