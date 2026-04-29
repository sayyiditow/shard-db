# External scanner / badge signups

Three external services need a one-time signup before their CI workflows produce results. Workflows are already in place but gated on the relevant secret existing — they no-op until you complete the signup.

Estimated total time: **~30 minutes**.

---

## 1. Coverity Scan (~10 min)

Free static analyzer for OSS. Catches things CodeQL and cppcheck miss.

### Steps

1. Sign in at <https://scan.coverity.com/users/sign_up> (GitHub OAuth works).
2. Click **"Add Project"** → enter the GitHub URL `https://github.com/sayyiditow/shard-db`.
3. Confirm; you'll be taken to the project's Coverity dashboard.
4. Click **"Project Settings"** (top right gear) → copy the **Project Token**.
5. In GitHub: **Settings → Secrets and variables → Actions → New repository secret**:
   - `COVERITY_SCAN_TOKEN` = the project token.
   - `COVERITY_SCAN_EMAIL` = the email you signed up with.
6. Trigger the workflow once: **Actions → "Coverity Scan" → Run workflow → main**.
7. Wait ~10 minutes for Coverity to process the upload, then refresh the dashboard. Findings appear under the project.

### Limits

Coverity Scan rate-limits public repos to **1 build/day, 3 builds/week**, so the workflow only runs:
- Weekly Mondays 06:00 UTC (cron)
- On-demand via `workflow_dispatch`

### Badge

After the first successful scan, copy the project's badge markdown from the Coverity dashboard ("Embed badge"). Drop it next to the existing badges in `README.md`:

```markdown
[![Coverity](https://scan.coverity.com/projects/<id>/badge.svg)](https://scan.coverity.com/projects/sayyiditow-shard-db)
```

---

## 2. Codecov (~5 min)

Line + branch coverage. Surfaces test-suite blind spots.

### Steps

1. Sign in at <https://app.codecov.io/login/gh> via GitHub OAuth.
2. From the dashboard, pick `sayyiditow/shard-db` from your repo list. Codecov walks you through "Coverage Analytics" setup; pick **Using GitHub Actions** as the setup option.
3. **Step 2 of Codecov's UI** — pick **Repository token** as the upload-token type.
4. **Step 3 of Codecov's UI** shows a UUID-shaped repository token. **Important:** if that token has been visible in chat / logs / a screenshot, click the **Regenerate token** button first and use the new one. Tokens cannot be revoked retroactively, only rotated.
5. Copy the token. In GitHub: **Settings → Secrets and variables → Actions → New repository secret**:
   - Name: `CODECOV_TOKEN`
   - Value: the token from Codecov
6. Steps 4-5 of Codecov's UI walk through editing the workflow YAML. **Skip this** — the workflow is already in place at `.github/workflows/codecov.yml` and references `secrets.CODECOV_TOKEN`.
7. Push to main (or trigger manually via Actions → "Coverage (Codecov)" → Run workflow). After the first successful upload, your repo page on codecov.io shows the coverage % and per-file heatmap.

### Badge

Codecov shows the badge URL on the repo's settings page on codecov.io. Add to README:

```markdown
[![Coverage](https://codecov.io/gh/sayyiditow/shard-db/branch/main/graph/badge.svg)](https://codecov.io/gh/sayyiditow/shard-db)
```

---

## 3. OpenSSF Best Practices Badge (~15 min)

Process-checklist badge. Passing tier is a real bar — fewer than half of OSS projects clear it.

### Steps

1. Sign in at <https://www.bestpractices.dev/en/signup> via GitHub OAuth.
2. Click **"Get Your Badge Now"** → enter `https://github.com/sayyiditow/shard-db`.
3. Fill the self-assessment. Most criteria are **already met** by what we've built — see the cheat-sheet below for which checkboxes apply directly. The few that need a sentence of justification take ~10 minutes total.
4. Submit. The badge URL is generated immediately; copy it to README.

### Cheat-sheet — what we already meet

Use this when filling the self-assessment form. Every claim points at something already in the repo so reviewers can verify.

| Question category | Status | Evidence |
|---|---|---|
| Project website | ✓ | https://sayyiditow.github.io/shard-db/ (GitHub Pages docs) |
| OSS license, distributed | ✓ | LICENSE file in repo root |
| FLOSS license OSI-approved | ✓ | (whichever license you picked) |
| Documented project | ✓ | docs/ tree |
| Bug reporting process | ✓ | GitHub Issues, link from README |
| Public version-control system | ✓ | this very repo |
| Unique version numbers | ✓ | `yyyy.mm.N` scheme, see `docs/reference/changelog.md` |
| Release notes | ✓ | `docs/reference/changelog.md` per release |
| Reproducible builds | ✓ | `./build.sh` produces deterministic output |
| Automated test suite | ✓ | 884 tests in tests/, run on every CI |
| Public test suite | ✓ | tests/ committed |
| New functionality has tests | ✓ | every 2026.05 feature shipped with a test |
| Static-analysis tool used | ✓ | CodeQL, cppcheck, scan-build all in CI |
| Dynamic-analysis tool used | ✓ | ASan + UBSan + TSan + libFuzzer all in CI |
| HTTPS for project URLs | ✓ | github.io is HTTPS |
| Cryptographic mechanisms documented | ✓ | TLS 1.3 doc in docs/operations/ |
| Vulnerability report process | ⚠️ | **Add a SECURITY.md if missing**: "report security issues to <email>" |
| Vulnerabilities fixed within 60 days | ✓ | recent CodeQL alert sweep landed in days |
| Code review of contributions | ⚠️ | only meets if Branch-Protection rule "Require PR review" is enabled — see `.github/SCORECARD-HARDENING.md` |

### Items that need work first

If any of the ⚠️ items above isn't addressed, you'll score below "passing":

1. **SECURITY.md** — if missing, create one with: a) where to report security bugs (email), b) supported versions, c) disclosure timeline. GitHub auto-detects and links it from the Security tab.
2. **Branch-Protection** — passing tier requires PR-based code review. See the steps in `.github/SCORECARD-HARDENING.md` to enable it.

### Badge

After submission:

```markdown
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/<id>/badge)](https://www.bestpractices.dev/projects/<id>)
```

The `<id>` is in the URL after submitting the form.

---

## After all three signups

Add the new badges to the top of `README.md` next to the existing CI / Docs / Scorecard badges. The README header should end up looking like:

```markdown
[![CI](...)](...)
[![Docs](...)](...)
[![OpenSSF Scorecard](...)](...)
[![Coverity](...)](...)
[![Coverage](...)](...)
[![OpenSSF Best Practices](...)](...)
```

Once they're in place we're at 6 badges = real signal that the project takes safety seriously, not just badge collecting. Each maps to a workflow that actually runs.

---

## What I (Claude) can do without the signups

The workflows in `.github/workflows/` for **coverity.yml** and **codecov.yml** are committed and ready. They auto-trigger on the right events but skip cleanly until the secrets exist (or, for Codecov, until the repo is enabled on codecov.io).

`SECURITY.md` and the README badge edits are blocked on you completing the signups (need the URLs). Ping me with the badge URLs once you've submitted and I'll wire them in.
