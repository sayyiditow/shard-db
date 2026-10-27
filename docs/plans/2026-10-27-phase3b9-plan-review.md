# Phase 3B.9 plan review — 2026-10-27

Base: `994e2eb43497721e34cd4aecf0159a00e42c226a` on
`fix/phase1-safe-scalar-conversions`. This review covers the proposed frozen
edits; no production implementation is applied in the root checkout.

## Standards / independent code review

**STD-01 [P2] — default field identity: resolved.** Default all-fields selection
stored names and then interpreted them through the named composite resolver.
The packed binder accepts an internal schema whose actual field name contains
'+', even though public field-name validation forbids it. A field named
`f3+f4` could therefore render the values of f3/f4 instead of its own stored
value. Revision 2 keeps indices for default selection and uses indexed raw
display rendering. Explicit selectors retain named/composite semantics.
Regressions cover NULL and empty default selectors, header labels, and an
explicit composite under the same accepted bound schema.

The independent reviewer saw complete proposed source/header/test/concept
snapshots, without plans, prior reviews or execution evidence. Re-review of
the repaired snapshot found no remaining actionable code finding.

## Spec / independent execution review

**SPEC-01 [P2] — incomplete header schema validation: resolved.** The adapter's
packed_schema_ok checks correspondence, but not all codec structural rules.
Headers never opened a record view and could accept an unknown packed kind,
bad directory/prefix/offset or invalid declaration/payload bound. Revision 2
adds the allocation-free pkr_schema_validate wrapper around the existing
authoritative codec schema_ok and calls it before header construction. It
does not duplicate or loosen the codec's rules. Seven malformed-metadata
cases plus direct valid/NULL validator contracts precede implementation.

Independent execution review verified all frozen blocks, base checkpoints,
quoted anchors, all stage postimages, bootstrap and actual-root check-only
preflight. The historical CSV_REVIEW_BASELINE exactly matches the retained
pre-review renderer. No driver stage references it; scratch regression-proof
uses it solely to reproduce the actual review defects. Re-review found no
additional execution blocker.

## Verification and boundaries

- Literal scratch stage 1 full build: exit 0, zero warnings. Focused RED:
  32 passed / 112 failed, exit 112, captured before stage 2.
- Literal scratch stage 2 full build: exit 0, zero warnings. Focused GREEN:
  144 passed / 0 failed, exit 0.
- Published probes: semantic checks pass; repair checks pass; all 15 selected
  allocation failures return NULL/ENOMEM/length zero with zero live buffers;
  maximum-schema actual 2085 / largest selected allocation 9 / live 0;
  independent Python CSV/byte oracle 3035/0, seed 20261027.
- Published reversal proof: exact renderer and validator stage-1 hashes,
  three runtime semantic failures; functioning pre-review renderer reproduces
  all three repair failures; restored final renderer passes those same gates
  and all green probes/oracle. This is stronger than an absence-only stub test.
- Fresh full normal suite after final build/docs: 20603 passed / 0 failed
  across 527 cases, exit 0.

Raw current verification lives under `/tmp/p3b9-r2/`; the scratch source tree
is `/tmp/p3b9-r2-tree-9j6h54m3`. Revision 1's historical planning results are
retained in the plan, but its temporary logs were cleaned externally and were
not represented as freshly inspected during this review.

No sanitizers, benchmarks, root production edits, staging, commits, pushes,
PRs or public packed activation occurred. The protected certification plan
was accessed only by SHA256, matching
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
It was never read, copied, edited, moved or staged.

The format uses visible \u0000, doubles literal backslashes, and escapes C0/DEL
before ordinary CSV quoting. UTF-8 bytes remain readable; CSV parsing must
precede byte unescaping. Empty and NULL cells remain indistinguishable by the
documented CSV convention. Private CR/LF and delimiter-policy differences are
explicit; legacy CSV output is unchanged. No public-command, FILE-atomicity,
charset-validity, request-memory-cap or performance claim is made.

Following emitter/error integration, remaining 3B consumers, 3C metadata and
later roadmap gates remain required. Nine final files belong to this child;
the approved plan and review record accompany execution as planning artifacts.
No remaining code/spec finding; ready for the executor handoff.

Reviewed Revision 2 plan SHA256:
`a71db6d07f2e4de71263c1a5f57b53c8e75c53ea5bc18b1c174822fcf09cfa89`.
All 27 manifest block hashes (28 extracted blocks including MANIFEST) and all
nine final postimages were verified after the last documentation update. The
actual-root stage-1 check-only preflight validates six files without writes.
