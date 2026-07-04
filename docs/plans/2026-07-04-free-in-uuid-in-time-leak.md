# Fix: free in_uuid / in_time arrays leaked in free_compiled_criteria

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/free-in-uuid-in-time-leak`.
- Build with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- **Never claim the task passed without pasting the real command output.** "# total: N passed, 0 failed" from the actual test binary is the only acceptable evidence it's done.
- The edit below is located by **quoted anchor text**, not line numbers. If the anchor is not found character-for-character, **stop immediately** and write `docs/plans/PLAN_NOTES.md` describing what you searched for and what you found instead — do not guess.
- Leave all changes **uncommitted** on the branch when done.

## Background

`compile_one` (`src/db/query.c`) precomputes typed `IN`/`NOT_IN` list arrays once at query-compile time. For `FT_TIME` it mallocs `cc->in_time`; for `FT_UUID` it mallocs `cc->in_uuid`:

```c
        case FT_TIME:
            cc->in_time = malloc(sizeof(uint8_t[3]) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                parse_time(c->in_values[i], cc->in_time[i]);
            break;
        case FT_UUID:
            cc->in_uuid = malloc(sizeof(uint8_t[16]) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                parse_uuid(c->in_values[i], cc->in_uuid[i]);
            break;
```

`free_compiled_criteria` frees `in_i64` and `in_f64` (the other two IN-list arrays) but never frees `in_uuid` or `in_time` — every compiled `IN`/`NOT_IN` criterion on a `uuid` or `time` field leaks one malloc per compile. This was noticed as a side observation while reviewing a separate perf plan (precomputing varchar IN-list lengths) and is being fixed here as its own tiny, unrelated change.

## Task 1 — Free in_uuid and in_time

### File: `src/db/query.c`

Find this exact block:

```c
void free_compiled_criteria(CompiledCriterion *arr, int n) {
    if (!arr) return;
    for (int i = 0; i < n; i++) {
        free(arr[i].s1);
        free(arr[i].s2);
        free(arr[i].needle_lc);
        free(arr[i].in_i64);
        free(arr[i].in_f64);
```

Replace it with:

```c
void free_compiled_criteria(CompiledCriterion *arr, int n) {
    if (!arr) return;
    for (int i = 0; i < n; i++) {
        free(arr[i].s1);
        free(arr[i].s2);
        free(arr[i].needle_lc);
        free(arr[i].in_i64);
        free(arr[i].in_f64);
        free(arr[i].in_uuid);
        free(arr[i].in_time);
```

### Invariant this preserves

`in_uuid` is only ever allocated in the `FT_UUID` case and `in_time` only in the `FT_TIME` case of `compile_one`'s switch; every other criterion leaves both `NULL` because `compile_criteria`'s `CompiledCriterion *arr = calloc(n, sizeof(CompiledCriterion))` zero-initializes the whole array before any field gets set (same guarantee already relied on for `in_i64`/`in_f64`). `free(NULL)` is a no-op in C, so the unconditional `free(arr[i].in_uuid)` / `free(arr[i].in_time)` added here is safe for every criterion, not just `uuid`/`time` ones.

**Note:** if this branch is created after the separate `perf/in-list-precomputed-lengths` plan has already landed on `main`, the anchor block above will additionally contain a `free(arr[i].in_lens);` line between `free(arr[i].in_f64);` and the closing of this snippet. If so, insert the two new `free()` lines immediately after whatever the last `free(arr[i].in_*)` line is at that point, preserving order; if the block doesn't match either form exactly, stop and write `PLAN_NOTES.md` rather than guess.

## Verification

1. `SKIP_TESTS=1 ./build.sh` — must complete with no compile errors.
2. `./build/bin/shard-db-test run-all` — paste the real output; must show `# total: N passed, 0 failed` with N equal to the pre-change total.

Do not report this plan as complete without pasting the actual output of step 2.
