<!--
  GENERATED FILE — do not hand-edit.
  Source: https://github.com/sayyiditow/dev-standards/blob/b34d2c1/CORE-PROCESS.md
  Synced: 2026-07-09
  To update: edit CORE-PROCESS.md in dev-standards, then re-run sync.sh against this repo.
-->

# Core Development Process

Reusable process rules for all projects. This file is synced verbatim into
each project as `docs/CORE-PROCESS.md` by `sync.sh` in this repo — do not
hand-edit the copy in a project repo; edit this file and re-run the sync.

Project-specific content (build commands, architecture, domain docs, and
this repo's chosen execution mode) lives in that project's own `CLAUDE.md`,
below the `@docs/CORE-PROCESS.md` import line.

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

## Working model: plan → approve → execute → review → merge

The human owns all feature, design, and architecture decisions. The agent
investigates, proposes, and implements — it does not decide what gets
built. No execution starts without explicit human approval of a written
plan.

1. **Plan.** Diagnose the problem or feature, then write a self-contained,
   TDD, task-by-task implementation plan to `docs/plans/YYYY-MM-DD-<feature>.md`.
   Present it; if there's a real design tradeoff, surface it and ask —
   don't pick silently.

   A plan is not done until:
   - Every insertion/edit locates its site by **quoted anchor text**, never
     line numbers (they drift, especially under concurrent work on other
     branches).
   - Every new/changed function, struct, or hunk is a **complete code
     block** — no prose descriptions of what to write.
   - Every task states its **test-first** step: a failing test (or a
     reproduction script for a bug) before the fix/feature code.
   - Bug-fix tasks include a **regression test** — one that fails on the
     base branch before the change and passes after.
   - Edge cases and invariants are spelled out explicitly, not left to the
     executor's judgment.
   - Embedded execution rules: branch off the default branch; do tasks in
     order; the exact build/test commands for this repo; "if a quoted
     anchor isn't found exactly, stop and write `PLAN_NOTES.md` — do not
     guess or reinterpret"; "if you hit a decision the plan doesn't cover,
     stop and ask — do not improvise."

2. **Approve.** The human approves, requests changes, or rejects. No
   execution starts without an explicit go-ahead.

3. **Execute.** The plan is carried out literally, task by task, on a
   fresh branch, per this repo's execution mode (declared in the
   project's own `CLAUDE.md` — see "Standing exceptions" below: either
   left uncommitted for review, or committed locally per task). Never
   claim a step passed without pasting the real command output. Never
   weaken a test — loosen an assertion, delete a case, mark it
   skip/xfail — to make a failure disappear. If a test can't be made to
   pass honestly, stop and report why instead of hiding it.

4. **Review.** Inspect the actual diff, not the executor's summary. Check,
   at minimum:
   - Correctness against the plan's stated invariants and edge cases.
   - Security: injection, path traversal, unchecked bounds, secrets in the
     diff.
   - Resource handling: leaks, unclosed handles, unbounded allocation.
   - Concurrency: races, lock ordering, TOCTOU.
   - Test quality: does the new test actually fail without the fix? Is
     coverage of the stated edge cases real, or just line coverage?
   - Scope: no unrelated changes, no drive-by refactors that weren't asked
     for.
   Report every finding, ranked by severity. Fix and re-review rather than
   merge with known issues outstanding.

5. **Merge.** Human runs, or explicitly directs the agent to run, commit →
   push → PR → merge, per the Git safety rules above.

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

**Co-author line(s):** planning and execution are frequently different
models in this workflow (a Claude model plans/reviews, a non-Claude model
executes on the branch). Credit every model that materially contributed,
one `Co-Authored-By:` line each:

- `Co-Authored-By: <planning/review model> <noreply@anthropic.com>` — the
  model that wrote or reviewed the plan (e.g. `Claude Sonnet 5`).
- `Co-Authored-By: <executing model> <its own noreply address>` — the
  model that actually ran the tasks and wrote the diff, if different from
  the planning model (e.g. `Gemini 2.5 Pro <noreply@google.com>`,
  `GPT-5 <noreply@openai.com>`, `DeepSeek V3 <noreply@deepseek.com>`).

Only one line when the same model both planned and executed. Never guess
the executing model's identity or version — use whatever the human
confirms was actually run.

## Definition of done (before handing back for review)

- [ ] Full test suite passes locally, fresh — not relying on a stale
      cache or a subset run.
- [ ] No new compiler/linter warnings.
- [ ] No leftover `TODO`/`FIXME`/debug prints/commented-out code from the
      work itself.
- [ ] Diff contains only the changes the task called for.
- [ ] Dependency additions, if any, are called out explicitly to the
      human — a new dependency is a supply-chain and maintenance cost,
      never add one silently.
- [ ] Commit messages are atomic (one logical change each) and explain
      *why*, not *what*.

## Standing exceptions

None by default beyond declaring this repo's execution mode (see step 3
above). Project-specific `CLAUDE.md` content may add further exceptions
(e.g. "deploys via artifact copy, never `git pull` on the server") below
the import — those are additive, not overrides of the git-safety rules
above.
