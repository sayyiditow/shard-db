<!--
  GENERATED FILE — do not hand-edit.
  Source: https://github.com/sayyiditow/dev-standards/blob/5a779c1/CORE-PROCESS.md
  Synced: 2026-11-08
  To update: edit CORE-PROCESS.md in dev-standards, then re-run sync.sh against this repo.
-->

# Core Development Process

Reusable process rules for all projects. This file is synced verbatim into
each project as `CORE-PROCESS.md` at the project root by `sync.sh` in this
repo — do not hand-edit the copy in a project repo; edit this file and
re-run the sync.

Project-specific content (build commands, architecture, domain docs, and
this repo's chosen execution mode) lives in that project's own `AGENTS.md`,
below the `@CORE-PROCESS.md` import line.

## Git safety

- NEVER commit, push, or merge to `main`/`master` directly. EVER.
- Always work on a feature branch: `<type>/<short-name>` where `<type>` ∈
  `feat|fix|chore|docs|refactor|perf|test`.
- Only push feature branches. Never push the default branch.
- All git write operations — `add`, `commit`, `push`, PR create, PR merge —
  are run by the human, or by the agent only when the human explicitly
  requests that exact operation in the moment. That's a one-time
  exception, not a standing permission — it does not carry over to the
  next git operation, even a routine one (e.g. a follow-up Dependabot PR).
- CI green is necessary but not sufficient to merge. Merging still
  requires the human's explicit go-ahead on top of a passing review pass.
  Never merge on "CI is green" alone.
- These rules override convenience or speed. If tempted to push directly:
  stop, open a PR instead.

## Review lenses

One checklist, used both to review a plan (against what the plan proposes)
and to review a diff (against what the code does):

- **Correctness**: against the stated invariants and edge cases.
- **Security**: injection, path traversal, unchecked bounds, secrets.
- **Resources**: leaks, unclosed handles, unbounded allocation.
- **Concurrency**: races, lock ordering, TOCTOU.
- **Interrupt/crash safety**: if the operation is killed or fails partway
  through, is the resulting state safe (consistent, recoverable, or at
  worst inert) rather than corrupted or stuck? If a new multi-step
  mutation is introduced, does an existing recovery/cleanup path cover it,
  or does one need to be added?
- **Breaking changes**: does this change any externally-observable
  contract (wire protocol, on-disk/serialized format, public API
  signature, CLI flags, config keys/semantics) in a way an existing caller
  could break? If yes, is there an explicit migration path, or is it
  flagged to the human as breaking (version bump / migration note)?
- **Test quality**: does each new test actually fail without the change?
  Is coverage of the stated edge cases real, or just line coverage?
- **Scope**: no unrelated changes, no drive-by refactors.

Findings are ranked **blocker** (wrong or unsafe; must fix), **major**
(likely to cause rework or a defect; should fix), **minor** (polish).

## Working model: plan → review → approve → execute → review → merge

The human owns all feature, design, and architecture decisions. The agent
investigates, proposes, and implements — it does not decide what gets
built. No execution starts without explicit human approval of a written
plan.

### 1. Plan

Diagnose the problem or feature, then write one file,
`docs/plans/YYYY-MM-DD-<feature>.md`, with two parts: a **Brief** on top
(for the human) and the **Plan** below it (for the executor and reviewers).
The Brief is the approval surface; the Plan is its appendix. Both live in
one file so they cannot drift apart.

#### 1a. The Brief (human-facing, one page at most)

The human must be able to approve or reject from the Brief alone, without
reading the Plan.

- **Problem and root cause**: 2–3 sentences. For a bug, the specific
  mechanism producing the wrong behavior, not just where it shows up. If
  the root cause isn't known with confidence, the plan's first task is a
  diagnostic task to establish it, not a speculative fix.
- **Logical flow**: a numbered sequence or small diagram of what changes
  and in what order, at the level of "request hits X, X now calls Y, Z is
  removed". No code.
- **Decisions made**: a table — decision, alternatives considered, why,
  and a tag:
  - `NEEDS YOU`: a real tradeoff only the human should settle.
  - `DEFAULT`: the agent's choice; the human overrides if they disagree.
  Never pick a real design tradeoff silently.
- **Blast radius**: every call site / consumer of anything whose
  signature, format, or protocol changes (via codebase search plus any
  known external consumers), and whether the change is breaking.
- **Test strategy**: one line per behavior being verified.
- **Out of scope**: what this deliberately does not touch.
- **Review log**: rounds of blind review run, notable findings and how
  each was resolved, and anything still open.

#### 1b. The Plan (executor-facing; length is fine)

Specify behavior, not keystrokes. A plan is complete when:

- Every change names its **file and symbol** (function, struct, section).
  Line numbers are never used — they drift. Quoted anchor text is only
  needed where a symbol name is ambiguous.
- **Interfaces, invariants, and edge cases** are spelled out explicitly,
  not left to the executor's judgment.
- Full **code blocks** appear only where the logic is subtle (concurrency,
  parsing, format/protocol changes, tricky algorithms). Everything else is
  described by behavior plus the tests that pin it down.
- Every task states its **test-first** step: a failing test (or a
  reproduction script for a bug) before the fix/feature code, with test
  names and what they assert.
- Bug-fix tasks include a **regression test** that fails on the base
  branch and passes after. The executor proves this directly: temporarily
  revert the fix, run the test and confirm it fails for the expected
  reason, re-apply the fix, confirm it passes. Both outputs are pasted.
- New tests don't depend on process-wide mutable state (env vars, global
  singletons, fixed ports/paths) unless already guarded for concurrent
  access, and don't assume they run alone or in a specific order — unless
  the project's harness only ever runs tests strictly sequentially.
- Embedded execution rules: branch off the default branch; do tasks in
  order; the exact build/test commands for this repo; "if the plan's
  assumption about the code is wrong (the thing it describes doesn't exist
  or doesn't behave as stated), write `PLAN_NOTES.md` describing the
  mismatch and halt the run — do not guess or continue to any further
  task. If a symbol has merely moved and the intended site is unambiguous,
  proceed and record it in `PLAN_NOTES.md`. Resuming after a halt requires
  the human (or the planning model) to read `PLAN_NOTES.md` and hand back
  a patched or fresh plan"; "if you hit a decision the plan doesn't cover,
  stop and ask — do not improvise."

#### 1c. Blind review loop (before the human sees it)

Review the plan until it converges. Each round:

1. Spawn a **fresh** reviewer that sees only the plan file and the repo —
   not the planner's reasoning, not earlier rounds' findings. Fresh each
   round, so it cannot anchor.
2. Run two lenses: (a) the **Review lenses** above applied to the plan,
   plus missed call sites and wrong assumptions about the code; (b) an
   **executor dry-run**: "could you implement this without asking a
   question or improvising?" Every point where it would have to guess is a
   finding.
3. The planner fixes blockers and majors (minors at discretion), updates
   the Brief's decision table and review log, and starts the next round.

Exit when a round returns **zero blockers and zero majors**. Cap at 4
rounds; if it hasn't converged, stop and escalate to the human with the
unresolved disagreement rather than looping.

#### 1d. Ready for approval

Present the plan only when all of these hold:

- The review loop has converged (or been escalated, with that stated).
- Every decision in the Brief is tagged `NEEDS YOU` or `DEFAULT`.
- Open questions are empty or explicitly assigned to the human.

The human may reject or send back the plan on the Brief alone.

### 2. Approve

The human approves, requests changes, or rejects, based on the Brief. No
execution starts without an explicit go-ahead. Changes requested here
re-enter the review loop (1c) if they alter the Plan materially.

### 3. Execute

Planner and executor are whichever model the human designates for that
task, per task — not fixed to a particular model or family, and not
inherited from precedent set by an earlier task in the same session. If
the human asks the current agent to execute a plan directly, it executes
it; if the human instead hands the plan to a separately-run model, the
current agent does not execute it. Don't assume a default — if it's
unclear who is executing a given plan, ask.

The plan is carried out literally, task by task, on a fresh branch, per
this repo's execution mode (declared in the project's own `AGENTS.md` —
see "Standing exceptions" below: either left uncommitted for review, or
committed locally per task). Never claim a step passed without pasting the
real command output. Never weaken a test — loosen an assertion, delete a
case, mark it skip/xfail — to make a failure disappear. If a test can't be
made to pass honestly, stop and report why instead of hiding it. A test
that fails and then passes on rerun is not resolved — treat it as a
confirmed bug until root-caused, or explicitly time-boxed and escalated to
the human with what was checked. Never quietly rerun-until-green.

### 4. Review

Inspect the actual diff, not the executor's summary. For any diff touching
concurrency, security boundaries, or an on-disk/wire format, include a
pass from a reviewer that hasn't seen the plan — reviewing the raw diff
and repo on its own terms, blind to the plan's stated intent, catches both
confirmation bias ("did the executor follow the plan") and flaws baked
into the plan itself that a plan-aware reviewer would wave through.

Apply the **Review lenses** above to the diff, and additionally check
correctness against the plan's stated invariants and edge cases. Report
every finding, ranked by severity. Fix and re-review rather than merge
with known issues outstanding.

### 5. Merge

Human runs, or explicitly directs the agent to run, commit → push → PR →
merge, per the Git safety rules above.

## Reference: git command sequence

```bash
# 1. Commit (on the feature branch, work already staged or unstaged)
git add <files>
git commit -m "$(cat <<'EOF'
type: short description

Longer explanation if needed.

Co-Authored-By: <planning/review model name> <noreply@anthropic.com>
Co-Authored-By: <executing model name> <executing model's noreply address>
EOF
)"

# 2. Push and open PR
git push -u origin <branch>
gh pr create --title "type: short description" --body "$(cat <<'EOF'
## Summary
- bullet points

## Test plan
- [ ] relevant test cases
EOF
)"

# 3. Admin merge (once CI is green AND the human has given an explicit go)
gh pr merge <number> --merge --admin

# 4. Clean up (optional — many hosts auto-delete the branch on merge)
git branch -d <branch>
git push origin --delete <branch>
```

**Co-author lines:** one `Co-Authored-By:` line per model that materially
contributed — the planning/review model (`<noreply@anthropic.com>` for a
Claude model) and, if different, the executing model with its own noreply
address (e.g. `Gemini 2.5 Pro <noreply@google.com>`). One line only when
the same model planned and executed. The human says who filled each role
for *this* task; never guess the executing model's identity or version, and
never carry over a default from an earlier task.

## Definition of done (before handing back for review)

- [ ] Full test suite passes locally, fresh — not relying on a stale
      cache or a subset run.
- [ ] Dynamic-safety tooling gate: if the project has dynamic-analysis
      tooling for concurrency- or resource-safety (sanitizers in
      C/C++/Rust, `-race` in Go, strict concurrency/leak linting in
      TS/Python/Java, valgrind, etc.), and the diff touches
      shared/concurrent state or resource lifetimes (locks, connections,
      file handles, goroutines/threads/async tasks), that tooling has been
      run locally against the affected tests — not deferred to a
      separately-scheduled CI job. Conditional on what the project already
      has; not a mandate to add new tooling.
- [ ] No new compiler/linter warnings.
- [ ] No leftover `TODO`/`FIXME`/debug prints/commented-out code from the
      work itself.
- [ ] Diff contains only the changes the task called for.
- [ ] Dependency additions, if any, are called out explicitly to the
      human — a new dependency is a supply-chain and maintenance cost,
      never add one silently.
- [ ] Documentation sync: if this diff changes a documented command, flag,
      config option, API surface, or invariant, the corresponding docs are
      updated in the same branch/PR — not deferred as a follow-up.
- [ ] Commit messages are atomic (one logical change each) and explain
      *why*, not *what*.

## Standing exceptions

None by default beyond declaring this repo's execution mode (see step 3
above). Project-specific `AGENTS.md` content may add further exceptions
(e.g. "deploys via artifact copy, never `git pull` on the server") below
the import — those are additive, not overrides of the git-safety rules
above.
