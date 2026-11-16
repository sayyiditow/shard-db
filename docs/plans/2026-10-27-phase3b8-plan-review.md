# Phase 3B.8 — blind plan review and repairs

Reviewed the proposed child against committed base
`cfc4970c2e0fcfa4e4a649f5cb7e347a147a04f7` on
`fix/phase1-safe-scalar-conversions`. Root production implementation is absent.
Planner/reviewer: GPT 6.1 Sol; intended executor: GLM 5.3 per user assignment.
Fresh reviewers did not receive the authoring discussion and did not rely on
claimed green evidence to establish correctness. Their repair passes were
independent static checks; runtime verification below is the planner's scratch work.

## Standards

**STD-01 (P1), resolved — Appendix A targeted the prior child plan.**
Every driver/checks invocation passed the Phase3B.7 plan path despite extracting
Phase3B.8 tooling. Literal execution would consume the wrong manifest and halt.
Revision2 targets Phase3B.8 consistently in all eight --plan arguments. The
bootstrap path, script folder references and driver/checks arguments now agree.
A fresh verification parses every published --plan argument and checks the
current plan filename, rather than relying only on manually invoked preflight.

The standards reviewer independently replayed all31 frozen block hashes,
unique anchors and checkpoints0–3, and confirmed the repaired published command
sequence. No additional actionable standards/executability issue was found.

## Spec

**SPEC-01 (P2), resolved — escaped composite output could exceed int counts.**
The existing json_escape_into accumulates size_t but returns int. The new grammar
could select one4MiB all-NUL varchar86 times, forming360710144 raw bytes and
2164260864 escaped payload bytes, beyond INT_MAX on the target without size_t
overflow. Original quote_into would call that backend once and reject its
narrowed count after a large allocation/write. Scalar rendering had kept backend
results small, so this becomes newly relevant with repeated composites.

Revision2 quotes in input chunks<=4096. Each backend result is at most24576,
which fits even the minimum C INT_MAX; offsets, capacity arithmetic and combined
counts remain size_t. No response limit, public signature or escaping-byte
change was introduced. The spec reviewer independently checked empty input,
remaining-capacity subtraction, chunk boundaries and ownership and found the
repair complete, with no further actionable issue.

The new permanent fixture compares all24579 raw NUL bytes and the complete
147476-byte escaped JSON, including exact/short budgets. The scratch p8_escape
wrapper delegates unchanged to the real backend while observing its input span.
The original writer produces correct medium output but violates the safe span
bound (largest24579); the repaired writer's largest input is4096. This tests
the actual count-safety invariant without artificial backend failures. The
multi-gigabyte reproducer is established by arithmetic and was not allocated
or executed. No sanitizer/benchmark run is claimed.

## Runtime and checkpoint verification

- Guard-only original quote writer:257 passing/1 failing probe gate, largest
  backend input24579; /tmp/p3b8-r2-quote-red.log.
- Amended fresh checkpoint1: new case38/158 exit158, existing projection216/2
  exit2; RED probe21/17, expected exit1.
- Repaired focused cases:196/0 and218/0, zero-warning normal build.
- Repaired green probe:258/0, all77 allocation injections; max schema
  actual2085/largest selected allocation36/live0; backend span4096.
- Frozen scratch reversal/reapply: checkpoint1/unchanged-storage hashes and
 21/17 defect gates, then checkpoint2/unchanged-storage hashes and258/0.
- Both independent repair passes confirm all31 blocks/unique anchors/checkpoints.
  Root driver stage1 --check-only verifies five files without mutations.
- Fresh full normal suite:20459 passed,0 failed across526 cases, exit0, after
  final docs. Raw logs/probes/binaries remain outside version control under /tmp.

No root production edits, staging, commits, pushes, PRs, metadata/public binding,
migration or later-phase work occurred. Protected certification document access
is hash-only. Branch-final dynamic-safety gates remain before an authorized push.

## Final result

Standards: one P1 resolved. Spec: one P2 resolved. Zero open findings after
independent repair checks and normal runtime verification. No production
implementation has been started in the root checkout.

Final Revision2 plan SHA256:
6085fe6c14b1e58905b812f5db96aab22fff088fb2f84512064dfbaad1bd0551.
Protected hash verified again at finish:
fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.
The next action is executor Tasks0–3 using the revised Appendix A, followed by
plan-aware and independent blind executed raw-diff reviews, still uncommitted.
