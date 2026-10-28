# R1 independent blind review — revision 3

Reviewed base: `0ef1a0ba3b33ec94c947e85091f866b39a1ee583` on
`fix/phase1-safe-scalar-conversions`. The proposed source was materialized from
the R1 frozen blocks into an archived `/tmp` tree. Two fresh reviewers saw source,
the base and repository requirements, without the plan, rationale or earlier
review conclusions. Production source was not edited.

## Findings and corrections

1. **P2 — incomplete schema export published as success.** The argv export
   callback accepted a `list-objects` error envelope or a response missing
   `objects`. It could replace an existing destination with a partial manifest.
   Revision 3 rejects any error member, including mixed error/objects responses,
   and rejects a missing objects member. A transport regression covers both named
   and stdout export in 18 scenarios, including valid empty-tenant controls.
   Before correction, six scenarios failed: export returned zero and published
   incomplete content. After correction, all 18 pass, including preservation of
   the prior destination and no stdout manifest on failure.
2. **P2 — checked bulk reads hid corrupt keyfile flags.** Bulk lookup reused an
   unchecked probe: flag 3 produced exists=true; resolve/revalidation could
   silently retire the corrupt reference, producing multi-get=null. Revision 3
   adds a checked probe in the appended reader code and propagates EUCLEAN from
   lookup, resolution and revalidation. No existing prefix, lock, read-handle
   lifetime or scheduling changes. The frozen corruption probe reports three
   bulk failures against the pre-correction suffix and zero against final code;
   checked single-read and legitimate tombstone controls pass in both states.
3. **P3 — tool checksum comparison lacked expected values.** Task 0 required a
   comparison but Appendix A only printed hashes. Appendix A now publishes the
   expected driver/checks SHA256 values; the checks hash includes both new
   regressions. The executor can perform the required comparison literally.

The corrections were cross-reviewed against the source. No further actionable
findings were identified in those corrections. This does not replace review of
the executor's eventual raw diff.

## Verification

Artifacts/logs remain under `/tmp/r1-blind-r7twgtc9`; fresh frozen replay is under
`/tmp/r1-review-replay-mb0nkcmp`. They are disposable evidence, not executor inputs;
the revised plan contains all required code and probes.

```text
root driver --stage 1 --check-only:
stage 1 preflight: 10 files, no writes

fresh archived replay:
stage 1 applied: 10 files; exact postimages verified
stage 2 applied: 24 files; exact postimages verified
stage 3 applied: 8 files; exact postimages verified
checkpoint 3 verified
```

Every final owned postimage in that fresh replay matched the corrected measured
scratch tree. Driver/checks hashes match their published values. The protected
certification artifact was accessed only through the driver's SHA256 check.

The ordinary build ran with `rtk proxy env SKIP_TESTS=1 ./build.sh`, exited zero
and emitted no compiler warnings, both before and after these corrections.

```text
rtk proxy ./build/bin/shard-db-test run test-r1-routes
# test-r1-routes: 109 passed, 0 failed
exit 0

rtk proxy python3 <extracted checks> --plan <R1 plan> --output <fresh /tmp path>
allocation failure checks=126, live=0
checked bulk corrupt flags: baseline RED reproduced; final GREEN and tombstone controls passed
18 export scenarios: PASS
R1 checks complete
exit 0
```

The checks also passed quota rejection, immutable OLD/computed error handling,
shard scheduling, sequence auto-key order, CLI Unicode/JSON framing, and 2,000
seeded JavaScript byte round trips. Expected scratch-baseline failures are
asserted by the script and are distinct from final-source failures.

The initial and final corrected full suites both passed:

```text
rtk proxy ./build/bin/shard-db-test run-all
1..529
# total: 20931 passed, 0 failed across 529 cases
exit 0
```

Final corrected output is in `final-full-suite.log`. The focused test output is
in `final-focused.log`; the extended checks, including isolated baseline export
reproduction, are in `isolated-checks.log`. No build replaced suite binaries while
either full-suite invocation was running.

No sanitizer or benchmark runs were performed: this is plan review. The plan
retains the repository's branch-final sanitizer timing. Nothing was staged,
committed, pushed or activated. Existing parent-plan modifications and the
protected untracked artifact remain untouched.
