# Finding: bitmap indexes under-count after a keyfile shard grows during inserts (2026-11-04)

**Status:** fixed by `docs/plans/2026-11-05-bitmap-stale-after-resplit-fix.md` (option 1: remap in the resplit + durable marker).
work: the identical result occurs on a `declaration_layout:"legacy"` object. Needs the human's decision (R6 D-R6-5).

## Symptom

Bitmap-indexed fields (`flag:bitmap`, `kind:bitmap`) return too few rows after the object's keyfile shards have
auto-grown while records were being inserted. Btree-indexed fields on the same object are correct. `reindex` repairs it.

Measured (4 000 rows, 8 splits, kf tier shrunk to 64 slots/shard so shards grow ~6 times):

| criteria | expected | before `reindex` | after `reindex` |
|---|---|---|---|
| `flag eq true` | 1714 | 762 | 1714 |
| `kind eq b` | 1334 | 567 | 1334 |
| `amt between 100..250` (btree) | 758 | 758 | 758 |
| `qty lt 5` (btree) | 1819 | 1819 | 1819 |

`count` and `find` agree with each other (both read the stale bitmap), so the error is silent.

## Root cause (mechanism)

A bitmap bit `i` of shard `s` means "kf slot `i` of shard `s` holds this value". The keyfile resplit
(`src/db/slotcask.c`, Phase B "probe-rebuild loop") re-inserts every live entry with
`kf_slot_for(hash, new_cap)`, so **entries move to different slot numbers** when the shard doubles. The live-write path
only calls `bm_grow()` when a *new* record's slot exceeds the bitmap (`src/db/index.c`, "Auto-grow on slot overflow" and
`bitmap_apply_grow_for_slot`); nothing renumbers bits that were already set for pre-existing records. Rebuild paths
(`reindex`, recompose, streams rebuild) recompute bits from the current slots, which is why they repair it.

## Why it is probably invisible in normal use, and why it is still a bug

With the default tier (1 Mi slots/shard at `splits <= 16`) the first in-place growth happens at ~750 K records in a shard,
so small and mid-size objects never hit it; large bulk loads do. The nightly auto-reshard/`reindex` also repairs it
eventually. Until then bitmap predicates are silently wrong.

## Reproduction (deterministic, daemon-backed)

Take `src/test/cases/test_predicate_equivalence.c` from the R6 plan and replace the single line
`    /* NOTE: no kf-tier seam here -- see docs/plans/2026-11-04-bitmap-stale-after-resplit.md */`
(it sits between `test_env_start` and the `TestClientCfg` line) with:

```c
    ASSERT_EQ_INT(test_env_ctl_set_kf_seams(&env, 64, 0), 0, "shrink the kf tier to force growth");
```

Expected today: 16 of 32 assertions fail with the counts above. The same failure occurs with
`"declaration_layout":"legacy"` added to `create-object`.

## Options (human decision)

1. **Fix at the source:** make the resplit path renumber/rebuild bitmaps for the shard it resplits (hook in the resplit
   commit, under the shard writer gate), or mark the object `bitmaps_need_rebuild` and rebuild lazily/at startup.
2. **Detect and refuse:** queries on a bitmap field whose kf capacity differs from the capacity recorded in the bitmap
   header fall back to a scan (correct, slower) until reindexed.
3. **Defer:** document as a known issue, rely on `reindex`; add the repro as a skipped/xfail-free separate plan.

Any fix touches locks and shared cache state, so it needs the full ASan+UBSan and TSan gate and its own plan under
CORE-PROCESS (root cause is stated above; the regression test is the repro).
