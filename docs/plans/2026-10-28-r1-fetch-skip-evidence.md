# R1 fetch-skip amendment evidence — 2026-10-28

The user explicitly requested restoring the previous behavior after reviewing
the offset-pagination cost. This implements the
[authorized amendment](2026-10-28-r1-fetch-skip-amendment.md) on the existing
uncommitted `fix/phase1-safe-scalar-conversions` checkout at HEAD
`0ef1a0ba3b33ec94c947e85091f866b39a1ee583`.

## Change and scope

Root cause: R1 consumed offsets inside callbacks reached only after a segment
had been acquired and its record header checked. The new checked skip walker
consumes live entries in the keyfile before accessing segments, preserving
tombstone counting, order, limits and cursor arithmetic. Ordinary fetch/keys
and private packed CSV fetch use it. The existing no-skip checked walker remains
as a compatibility wrapper.

Returned records retain checked reads. Existing cache reader lifetimes and
locks are unchanged; no writer reservation, writer gate or new lock was added.
Inserts, bulk writes and multi-key scheduling were not changed. The original
475,195-byte slotcask source prefix retains SHA256
`8fef2c466c99dc26e5ca7eb11655a7c4990a71e5fbaebc3e1cc228a9816d71c5`.

The regression deliberately injects an unavailable offset into a temporary
fixture, restores it afterward and removes the fixture. This is proof that
skipped locations are not accessed, not an observed corruption incident.

## Regression evidence

Both builds used `rtk proxy env SKIP_TESTS=1 ./build.sh`, exited zero and emitted
zero compiler warnings. Raw build logs are `/tmp/r1-fetch-skip-red-build.log`
and `/tmp/r1-fetch-skip-green-build.log`. The exact same registered test runs
against both implementations:

```text
rtk proxy ./build/bin/shard-db-test run test-r1-fetch-skip
RED:   # test-r1-fetch-skip: 116 passed, 48 failed
       exit 48
GREEN: # test-r1-fetch-skip: 164 passed, 0 failed
       exit 0
```

Logs: `/tmp/r1-fetch-skip-red.log` and `/tmp/r1-fetch-skip-green.log`. All 48 RED
failures are skipped-offset/cursor cases; returned-record error controls pass.
Coverage includes legacy and packed layouts, JSON/default/rows/dict/CSV output,
cursor skipping, offset skipping, combined skip beyond the end, correct next
key, failure before any success prefix for a returned unreadable record, and
absence of writer reservation. No timing benchmark or sleep-based assertion.

Pre-correction source and the actual RED binary remain under
`/tmp/r1-fetch-skip-before/`. No production source was temporarily reverted.

```text
rtk proxy ./build/bin/shard-db-test run test-r1-routes
# test-r1-routes: 109 passed, 0 failed
exit 0
```

Log: `/tmp/r1-fetch-skip-routes.log`.

## Compatibility probes

The revision-3 checks pass against amended source via the derived script
`/tmp/r1-fetch-skip-checks.py`, SHA256
`805d7cbf8708c7f35f75af046a40cb672e1d86eddde67e4e2bbf2150d5808c12`.
Only the scratch pre-correction slotcask source construction changes: it appends
the complete final skip function to resolve the new internal symbol when linked
with the current library. The old bulk-reader defect, original probe code and
all expected RED/GREEN assertions remain intact. The frozen plan/tools retain
their historical bytes and hashes.

```text
rtk proxy python3 /tmp/r1-fetch-skip-checks.py --plan docs/plans/2026-10-27-r1-write-read-integration-plan.md --output /tmp/r1-fetch-skip-compatibility
allocation failure checks=126, live=0
checked bulk corrupt flags: baseline RED reproduced; final GREEN and tombstone controls passed
export regression: six baseline RED scenarios reproduced; all 18 final GREEN scenarios passed
R1 checks complete
exit 0
```

Full probe log: `/tmp/r1-fetch-skip-checks.log`. Quota, scheduling, Unicode,
sequence-key and JavaScript byte-round-trip checks also pass.

## Independent amendment review

A fresh reviewer inspected the amendment-only source diff without the plan or
rationale. Pagination arithmetic, callback stop conditions, cache releases and
test mechanics passed review. A proposed deadline concern was withdrawn after
comparison with the requested pre-R1 skip helper, which likewise consumed skips
before callbacks; emitted-row and post-walk checks remain. No actionable
findings remain.

## Final full suite

```text
rtk proxy ./build/bin/shard-db-test run-all
1..530
# total: 21095 passed, 0 failed across 530 cases
exit 0
```

Raw log: `/tmp/r1-fetch-skip-full-suite.log`. No build replaced binaries while
the suite ran. `rtk git diff --check` also passes. The final full suite was run
after all source fixes and includes the new 164-check pagination case.

No benchmark or sanitizer runs; branch-final sanitizer timing remains as
specified by AGENTS.md. Everything remains unstaged/uncommitted. No R2 work or
packed activation. The original frozen revision-3 plan retains SHA256
`7ad0e3958f9f22f5a293d2e99964bde0a15ea17b7b8a1a2c0f6a595db50936d2`.
