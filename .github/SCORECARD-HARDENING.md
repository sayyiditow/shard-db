# Scorecard hardening — maintainer notes

Repo-management steps to lift the OpenSSF Scorecard score for checks
that aren't fixable by code changes alone. Bookmark this for the day
you decide to enforce a stricter workflow.

## Status (as of 2026-04-29)

| Check | Status | Notes |
|---|---|---|
| Pinned-Dependencies | ✓ done | All workflow `uses:` SHA-pinned; `requirements.txt` exact-pinned |
| Token-Permissions | ✓ done | Default `read-all`, per-job grants |
| Dependency-Update-Tool | ✓ done | `.github/dependabot.yml` for github-actions + pip |
| Branch-Protection | needs UI | See below |
| Code-Review | needs workflow | Depends on PR-based development |
| Maintained | time-based | Cut tagged releases; close issues monthly |

## Branch-Protection (the big one)

`Settings → Branches → Add classic branch protection rule` (or `Branches → Branch rulesets → Add ruleset` in the newer UI):

Branch name pattern: `main`

| Setting | Value | Why |
|---|---|---|
| Require a pull request before merging | ✓ | Forces PR flow |
| Required approvals | `1` (or `0` if solo) | Scorecard wants ≥1 for full marks; `0` still scores partial |
| Dismiss stale reviews on push | ✓ | Stops "approved before the bad commit" bypass |
| Require status checks to pass | ✓, add `Build & test (Linux x86_64)` and `Build & test (Linux arm64)` | Ensures CI gates merges |
| Require branches to be up to date before merging | ✓ | Catches semantic conflicts |
| Require linear history | ✓ | No merge commits — keeps `git log` readable |
| Require signed commits | ✓ (optional) | Small score lift |
| **Do not allow bypassing the above settings** | ✓ | **THE KEY ONE.** Without this, admins (you) can push directly and Scorecard treats the rule as unenforceable. |

The "Bypassed rule violations" warnings you see on every `git push` today are the symptom of this last toggle being off.

## Code-Review (auto-fixes once Branch-Protection is on)

Once Branch-Protection forces PR flow, every change goes:

```bash
git checkout -b fix-foo
# ... edit ...
git push -u origin fix-foo
gh pr create --fill           # opens PR
# wait for CI green
gh pr merge --squash --delete-branch
```

Scorecard reads the **last 30 merged commits** and counts how many came in via approved PR. After 30 PR-merged commits, score is 10/10.

For a solo maintainer who doesn't want self-approval theater, leave **Required approvals = 0**. Scorecard still sees "merged via PR" and scores ~7/10 (vs. 0/10 for direct push to main).

## Maintained

Easiest lift: cut tagged releases.

`Releases → Draft a new release`:
- Tag: `2026.05.1` (matches `yyyy.MM.N` scheme)
- Target: `main`
- Title: `2026.05.1 — per-shard btree layout`
- Body: cribbed from `docs/reference/changelog.md`
- Attach: `shard-db-x86_64.tar.gz` and `shard-db-arm64.tar.gz` from a recent CI run

The check looks at:
- Commits in last 90 days (we have lots)
- Issues closed in last 90 days (close some occasionally)
- **Releases in last 90 days** ← this is the easy lift

## Working with Dependabot

Once `.github/dependabot.yml` is in place (it is), Dependabot opens PRs every Monday for any out-of-date GitHub Action SHA or pip package.

PR review checklist:
1. Open the upstream release notes (Dependabot links them).
2. Skim for breaking changes.
3. If CI passes (it usually does for SHA bumps), squash-merge.
4. If CI fails, that's signal — investigate before bumping.

This rotation is what keeps Pinned-Dependencies sustainable: we don't have to manually chase upstream commits.

## When does each check re-score?

- **Push to main** → Scorecard re-runs immediately (≈1 min) and uploads SARIF (≈3 min total).
- **Weekly cron Friday 18:45 UTC** → catches anything we didn't push.
- **Manual trigger** → `Actions → "Scorecard supply-chain security" → "Run workflow" → main`.

Watch the run in the Actions tab; any check failure logs to the run's "Run analysis" step.

## What we're deliberately not doing

- **Mandatory two-reviewer rule** — overkill for a single-maintainer project; we'd never merge anything.
- **Required signed commits across all forks** — friction for drive-by contributors; revisit if there's ever a real fork ecosystem.
- **OpenSSF Best Practices Badge (Gold tier)** — process-checklist mostly redundant with what's already in place; passing tier is a fine target.

## Related

- The Scorecard workflow: [`.github/workflows/scorecard.yml`](workflows/scorecard.yml)
- Dependabot config: [`.github/dependabot.yml`](dependabot.yml)
- Live results: <https://scorecard.dev/viewer/?uri=github.com/sayyiditow/shard-db>
