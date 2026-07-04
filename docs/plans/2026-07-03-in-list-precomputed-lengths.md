# Perf: precompute IN-list varchar lengths at compile time instead of per-record strlen

## Execution rules (read first)

- Branch off `main`: `git checkout -b perf/in-list-precomputed-lengths`.
- Build with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- **Never claim the task passed without pasting the real command output.** "# total: N passed, 0 failed" from the actual test binary is the only acceptable evidence it's done.
- Every edit below is located by **quoted anchor text**, not line numbers. If an anchor is not found character-for-character, **stop immediately** and write `docs/plans/PLAN_NOTES.md` describing what you searched for and what you found instead — do not guess.
- Do all four tasks in order — task 1 adds the struct field the others depend on; task 4 (free) must land or every compiled IN/NOT_IN criterion on a varchar field leaks the new array.
- Leave all changes **uncommitted** on the branch when done.

## Background

`CompiledCriterion` (`src/db/types.h`) already precomputes typed representations for `OP_IN`/`OP_NOT_IN` lists exactly once, at query-compile time (`compile_one`, `src/db/query.c`) — for every field type *except* varchar. The switch in `compile_one` has a dedicated case for `FT_LONG`/`FT_INT`/`FT_BOOL` (parses to `in_i64[]` once), `FT_DOUBLE`/`FT_FLOAT` (`in_f64[]`), `FT_DATE`, `FT_TIME`, `FT_UUID`, `FT_ENUM` — all precomputed once. The `default:` case (which VARCHAR and DATETIME fall into) explicitly does nothing: `/* VARCHAR, DATETIME: use raw strings via c->in_values */`.

Because of that, the varchar match path (`OP_IN`/`OP_NOT_IN` inside `match_typed_varchar`) calls `strlen(c->in_values[i])` on every IN-list value for *every record* it evaluates, even though every value in the list is fixed for the lifetime of the compiled criterion — the length never changes between records. This is the same "finish applying a pattern the codebase already established for every other type" shape as the two already-completed fixes (arena reuse, `SlotRef` fast path) — not new machinery, just extending an existing precompute-once convention to the one type that was left out.

## Task 1 — Add the in_lens field

### File: `src/db/types.h`

Find this exact block:

```c
    /* IN / NOT_IN lists */
    int64_t  *in_i64;
    double   *in_f64;
    uint8_t (*in_uuid)[16];
    uint8_t (*in_time)[3];
    int       in_count;
```

Replace it with:

```c
    /* IN / NOT_IN lists */
    int64_t  *in_i64;
    double   *in_f64;
    uint8_t (*in_uuid)[16];
    uint8_t (*in_time)[3];
    size_t   *in_lens;   /* varchar only: strlen(in_values[i]), precomputed once
                             at compile time instead of per-record in the match
                             loop (in_values[] themselves stay raw strings) */
    int       in_count;
```

## Task 2 — Precompute lengths for the varchar/datetime case

### File: `src/db/query.c`

Find this exact block:

```c
        case FT_ENUM:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_i64[i] = (int64_t)enum_value_index(cc->tf,
                                                          c->in_values[i],
                                                          strlen(c->in_values[i]));
            break;
        default:
            /* VARCHAR, DATETIME: use raw strings via c->in_values */
            break;
        }
    }
}
```

Replace it with:

```c
        case FT_ENUM:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_i64[i] = (int64_t)enum_value_index(cc->tf,
                                                          c->in_values[i],
                                                          strlen(c->in_values[i]));
            break;
        default:
            /* VARCHAR, DATETIME: values stay raw strings via c->in_values,
               but lengths are fixed for the life of this compiled criterion —
               precompute once here instead of strlen() per record in the
               match loop (match_typed_varchar's OP_IN/OP_NOT_IN cases). */
            cc->in_lens = malloc(sizeof(size_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_lens[i] = strlen(c->in_values[i]);
            break;
        }
    }
}
```

## Task 3 — Use the precomputed lengths in the match loop

### File: `src/db/query.c`

Find this exact block:

```c
    case OP_IN:
        for (int i = 0; i < c->in_count; i++) {
            size_t vl = strlen(c->in_values[i]);
            if (elen == (int)vl && memcmp(hay, c->in_values[i], vl) == 0) return 1;
        }
        return 0;
    case OP_NOT_IN:
        for (int i = 0; i < c->in_count; i++) {
            size_t vl = strlen(c->in_values[i]);
            if (elen == (int)vl && memcmp(hay, c->in_values[i], vl) == 0) return 0;
        }
        return 1;
```

Replace it with:

```c
    case OP_IN:
        for (int i = 0; i < c->in_count; i++) {
            size_t vl = cc->in_lens[i];
            if (elen == (int)vl && memcmp(hay, c->in_values[i], vl) == 0) return 1;
        }
        return 0;
    case OP_NOT_IN:
        for (int i = 0; i < c->in_count; i++) {
            size_t vl = cc->in_lens[i];
            if (elen == (int)vl && memcmp(hay, c->in_values[i], vl) == 0) return 0;
        }
        return 1;
```

**Note for the executing model:** confirm `cc` (the `CompiledCriterion *`) is in scope at this exact point before making this edit — this switch is inside `match_typed_varchar`, whose signature is `static int match_typed_varchar(const uint8_t *p, int size, const CompiledCriterion *cc)`, and `c` (`const SearchCriterion *c = cc->raw;`) is a local alias already declared earlier in that same function, so both `c` and `cc` are already in scope side by side — this is the same scoping every other case in this switch already relies on (e.g. `cc->needle_lc` a few lines above). If `cc` is not in scope for any reason, stop and write `PLAN_NOTES.md` rather than guess at how to obtain it.

## Task 4 — Free the new array

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
        free(arr[i].in_lens);
```

### Invariant this preserves

`in_lens` is only ever allocated when `(cc->op == OP_IN || cc->op == OP_NOT_IN) && c->in_count > 0` and the field's type falls into the `default:` case (i.e. varchar or datetime) — every other criterion leaves it `NULL` (guaranteed by `compile_one`'s `CompiledCriterion *arr = calloc(n, sizeof(CompiledCriterion))` zero-initializing the whole array before any field gets set). `free(NULL)` is a no-op in C, so the unconditional `free(arr[i].in_lens)` added in Task 4 is safe for every criterion, not just IN/NOT_IN ones.

## Verification

1. `SKIP_TESTS=1 ./build.sh` — must complete with no compile errors.
2. `./build/bin/shard-db-test run-all` — paste the real output; must show `# total: N passed, 0 failed` with N equal to the pre-change total (4891).

Do not report this plan as complete without pasting the actual output of step 2.
