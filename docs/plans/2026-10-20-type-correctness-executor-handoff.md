# Executing-agent handoff: type correctness Phase 0

Copy the instruction block below into the executing agent's chat. This handoff
starts the investigation and next code-plan authoring; it does not authorize
implementing all phases from the parent design.

---

Work in `/home/hashim/junk-system/git-workspace/sayyiditow/shard-db`.
Execute **Phase 0 only** of the type-correctness recovery work, then deliver
the complete executable Phase 1 child plan for human review. Do not start
Phase 1 production edits until that child plan has explicit human approval.

Read, in order:

1. `AGENTS.md` and its imported `CORE-PROCESS.md`.
2. `docs/plans/2026-10-19-type-correctness-recovery-plan.md` (authoritative
   parent design, including the revised phase-by-phase execution checkpoint).
3. `docs/plans/2026-10-19-type-correctness-baseline-plan.md`.
4. `docs/plans/2026-10-19-type-correctness-acceptance-plan.md`.

The older roadmap is historical context; the parent recovery plan supersedes
it. Do not treat prose or a syntax-checked acceptance fixture as a completed
production patch or a passing feature test.

## Boundaries

- Do not change production source, register lasting test cases, or alter the
  storage format during this assignment. Isolated prototypes, temporary
  fixture data, evidence documents and the Phase 1 code-plan document are
  within scope. Compile scratch sources into a unique `/tmp` folder.
- Preserve all existing uncommitted files. Do not switch/reset the checkout,
  stage, commit, push, open a PR or merge. Phase 0 does not require a feature
  branch because it makes no production changes; obtain the required git
  authorization before branch setup for a later approved implementation.
- Prefix every shell command with `rtk`; use `rtk proxy` when appropriate.
  Do not run benchmarks or mutate shared process-wide test configuration.
- The user reserved `docs/plans/2026-10-19-durability-certification-rig.md`
  for **after this work lands**. Do not edit, move, remove, execute or absorb
  its work. Verify its checksum at start and finish:

  ```text
  fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135
  ```

- If an exact quoted source anchor is missing, write `PLAN_NOTES.md` with
  the mismatch and halt the entire run. Do not reinterpret it or continue
  unrelated tasks. If an uncovered design decision appears, stop for a
  planning-model/human resolution rather than inventing an implementation.

## Execute this sequence

1. Record the source revision, working-tree status and protected checksum.
   Run a fresh normal build and full normal suite once to establish the
   current baseline; record unexpected failures without fixing them under
   this assignment. Use `rtk proxy env SKIP_TESTS=1 ./build.sh` and
   `rtk proxy ./build/bin/shard-db-test run-all`. Reuse previous diagnostic
   observations only when revision and relevant source/diff identity match;
   otherwise rerun the small probes. Clearly label expected diagnostic
   nonzero exits versus unexpected build/test failures.
2. Verify the consumer inventory in Task 0.1, including independent criterion
   and index encoders, direct bulk/default paths, schema conversion and schema
   export/import. Include each affected declaration and caller in the evidence.
3. Use isolated daemon/client fixtures to establish public-route failures:
   the original `1234567890123456.1234567890` numeric(26,10) round trip,
   unsafe integer conversions, clipped floating output, float-to-double
   schema widening, and bare numeric/varchar schema rejection. Preserve old
   records/counts/indexes when testing rejection expectations. Do not claim
   all routes have been tested based on a shared-helper probe.
4. Run the Task 0.3 scratch layout/sizing spike. Prove compact first/middle/
   last variable fields, two variable fields, NULL/empty/zero distinctions,
   full-range decimal capacity, descriptor bounds, update/rebuild
   recomposition, small allocation sizes and size-bucket/free-pool behavior.
   No sidecar storage or unconditional maximum-width padding is authorized.
   Verify maximum record sizes against the actual segment limit and all
   integer/callback capacity types; do not silently weaken numeric limits or
   raise request/query memory caps to make a case pass.
5. Freeze the cross-cutting contract with evidence: scalar grammar, finite
   numeric limits, precision/scale/display-scale and average semantics,
   serialized layout/version/downgrade protection, sizing, key feasibility,
   migration/replay/publication ordering. Read PostgreSQL 18 primary sources
   specified by the parent, and independently validate arithmetic using
   Python decimal with explicit context. A live PostgreSQL server is optional;
   do not make lack of one a gate or use a real finance database as a fixture.
   Distinguish stored fractional scale (up to 16383) from PostgreSQL 18's
   division/average calculation-scale clamp (0..1000); derive its exact
   base-10000 selection algorithm and boundary cases from the pinned source.
6. Author **only the next child code plan, Phase 1**, against the current
   source. Supply exact quoted anchors, every complete changed/new code block,
   every caller/consumer update, complete regression cases/build-list hunks,
   documentation changes, and red/green/full-suite commands. Keep Phase 1
   scoped to its parent's scalar-safety and floating-output work. If it would
   require a format change, stop and revise the phase boundary through review.
   Future child plans are written just before their respective phases; do
   not write all eight upfront or implement unapproved later phases.

## Required arithmetic and capacity assertions

- A new-format decimal field contributes
  `8 + 16 + ceil(canonical_coefficient_digits/2)` bytes, excluding shared
  headers/alignment. For 25 or 26 coefficient digits this is **37**, not 33.
- A new varchar field contributes `8 + actual_content_bytes`. Its maximum
  raw payload is **65535**, not the old prefix-inclusive 65537.
- With 256 variable fields the nullmap prefix is 33 bytes. Maximum values:
  all bare numeric `18880545` bytes; all bare varchar `16779041` bytes.
  At max_key=1024, plus the 24-byte record header and alignment, maximum
  slots are `18881600` and `16780096` bytes respectively. Recompute using
  checked arithmetic and verify mixed layouts and actual short-key records.
- Bare numeric must handle 131072 integer digits and 16383 fractional digits
  in the same finite value. A 1000-digit or int128-only bare implementation
  is insufficient. Small values use actual payload bytes, not that ceiling.
- Establish index feasibility before freezing the format: actual-key budget,
  tier/NULL/composite accounting and failure preservation. Phase 5 later
  publishes the final selected codec's exact formula and full P=1..1000
  feasibility table. Never truncate keys or reject an entire bare column
  solely because its theoretical maximum exceeds B+ tree key capacity.

## Deliver and stop

Create an evidence/decision ledger under `docs/plans/` identifying source and
uncommitted-diff identity, commands, actual outputs, failures, proved contracts,
unresolved decisions and protected checksum. The next agent must be able to
distinguish planning evidence from implemented/passing behavior.

Deliver the complete Phase 1 executable child plan and a concise report of
Phase 0 findings. Stop for human review/approval; do not implement Phase 1 or
claim the finance application is unblocked. Later phases each require their
own complete child plan, approval, test-first execution, full normal suite and
raw-diff review. Sanitizer gates remain after all production fixes, as stated
in AGENTS.md; the separate certification rig remains later work.
