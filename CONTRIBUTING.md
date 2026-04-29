# Contributing to shard-db

Thanks for considering a contribution. This file covers the practical bits — how to build, test, and submit changes — plus what kinds of contributions are most useful right now and what's deliberately out of scope.

For security-sensitive issues, **don't open a public issue** — see [`SECURITY.md`](SECURITY.md) for the disclosure flow.

## Build & test

Linux x86_64 or arm64. macOS support is on the near-term backlog (2026.05.2); meantime use a Linux container.

```bash
git clone https://github.com/sayyiditow/shard-db.git
cd shard-db
./build.sh                              # release build → build/bin/shard-db + shard-cli
cp build/bin/shard-db ./shard-db        # tests expect ./shard-db at the repo root
./tests/test-objlock.sh                 # single test (each test starts/stops its own daemon)
for t in tests/test-*.sh; do "$t"; done # full suite, sequential (~5 min)
```

The build script accepts `BUILD_MODE`:

```bash
BUILD_MODE=asan ./build.sh      # AddressSanitizer + UBSan (~5× slower; runs the suite under it)
BUILD_MODE=tsan ./build.sh      # ThreadSanitizer (concurrency tests only — see CI)
BUILD_MODE=coverage ./build.sh  # gcov coverage (used by Codecov upload)
BUILD_MODE=debug ./build.sh     # -O0 -g, no sanitizers, for gdb
```

## Code conventions

The codebase is C11 with these conventions:

- **Indentation**: 4 spaces, never tabs.
- **Identifiers**: `snake_case` for functions, variables, types. `MACRO_CASE` for `#define`s.
- **Braces**: K&R (opening brace on same line; `else` on the closing-brace line).
- **Comments**:
  - Default to writing none. Well-named identifiers describe **what**.
  - Add a comment only when the **why** is non-obvious — a hidden constraint, a subtle invariant, a workaround for a specific bug, or behaviour that would surprise a reader.
  - No comments referencing the task / fix / caller (e.g. "added for the X flow", "used by Y") — those rot as the codebase evolves.
- **Headers**: `types.h` is the daemon-side single point of truth. `cli.h` is the TUI's. Both are intentionally decoupled; don't make `cli/` files include `types.h`.
- **No new dependencies** without discussion. Current external deps: libc, pthreads, OpenSSL (TLS only), ncurses (CLI only).

The `clang-format` config and `.editorconfig` are deliberately not committed — the rules above are short enough to follow by reading existing files.

## Submitting a change

```bash
git checkout -b fix-foo
# ... make changes, build, test ...
./build.sh && cp build/bin/shard-db ./shard-db
./tests/test-objlock.sh   # whichever test covers your change

git push -u origin fix-foo
gh pr create --fill        # or via the GitHub UI
```

### Pre-submit checklist

Before opening a PR:

- [ ] `./build.sh` passes with no new compiler warnings.
- [ ] All tests in `tests/test-*.sh` that touch your change still pass. For non-trivial changes, run the full suite.
- [ ] If you added a feature, **add a test** under `tests/test-<feature>.sh` and wire it into `.github/workflows/ci.yml`. See [Adding a test](#adding-a-test) below.
- [ ] If you touched parser / wire-format code, run the fuzzers locally for at least 60 seconds each:
  ```bash
  ./fuzz/build.sh
  ./fuzz/build/fuzz_json     -max_total_time=60 fuzz/corpora/json/
  ./fuzz/build/fuzz_b64      -max_total_time=60 fuzz/corpora/b64/
  ./fuzz/build/fuzz_criteria -max_total_time=60 fuzz/corpora/criteria/
  ```
- [ ] If you changed concurrency-relevant code (per-shard locks, btree cache, parallel-for), run `BUILD_MODE=asan ./build.sh` and re-run the affected test under it.

CI runs everything above on every PR. A green CI is the bar — don't merge until it's green.

### What CI does

| Workflow | What it runs |
|---|---|
| **CI** | Builds + full test suite on Linux x86_64 + arm64 |
| **Sanitizers** | ASan + UBSan build, runs 7 tests including the stress test |
| **TSan** | ThreadSanitizer build, runs 4 concurrency tests |
| **libFuzzer** | Builds the fuzz harnesses, runs 60s per parser |
| **cppcheck** | Static analyzer with warning/performance/portability checks |
| **scan-build** | Clang Static Analyzer (path-sensitive) |
| **CodeQL** | GitHub's static analysis (default setup) |
| **Coverage (Codecov)** | gcov line+branch coverage upload |
| **Coverity Scan** | Weekly upload to scan.coverity.com |
| **Scorecard** | OpenSSF supply-chain checks |
| **Docs** | mkdocs build + GitHub Pages deploy |

### Commit messages

- One short imperative sentence on the first line. Under 72 chars.
- A blank line, then a paragraph (or two) explaining **why**. The diff already shows what.
- For bug fixes, mention how the bug was found (CodeQL, fuzzer, customer report, etc.) and what surface it was reachable from.
- We don't squash-merge by default; commits are kept individually so `git log` reads as a story.

Examples:

```
btree: use memcpy for unaligned uint16 access

UBSan flagged six call sites where *(uint16_t*)entry on packed page
storage was undefined behaviour. x86_64 silently handled it; ARM/RISC-V
would SIGBUS. Replaced with bt_load_u16/bt_store_u16 helpers.
```

## Adding a test

Tests live in `tests/test-<feature>.sh` — one bash script per feature, each starts and stops its own daemon. Pattern:

```bash
#!/bin/bash
cd "$(dirname "$0")/.."
# test-<feature>.sh — short description.
#
# Covers:
# - Bullet point per behaviour exercised

set -e
BIN="./shard-db"
PASS=0; FAIL=0; TOTAL=0
pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo "  FAIL: $1"; }

# ... start daemon, exercise the feature, assert with pass/fail ...

echo "================================"
echo "  $PASS passed, $FAIL failed ($TOTAL total)"
echo "================================"
[ "$FAIL" -eq 0 ]
```

Then add to `.github/workflows/ci.yml`:

```yaml
      - name: <Feature> (test-<feature>)
        run: ./tests/test-<feature>.sh
```

Run it locally first to confirm it passes consistently — flaky tests are worse than no test.

## What we're keen for

- **Bug reports** with a reproducer (a 3-line `./shard-db query` sequence is ideal).
- **Performance regressions** in the bench scripts (`bench/bench-*.sh`) with before/after numbers.
- **Test coverage** on under-tested code paths — `Coverage (Codecov)` shows hot spots.
- **Documentation fixes** when something in `docs/` doesn't match the code.

## What's intentionally out of scope

These won't be accepted as PRs (we've thought about them and they're not on the path):

- **Windows native build.** The `epoll` + `mmap` model is deliberate. Containers cover the cross-platform case.
- **SQL parser / surface.** The JSON wire protocol is the language. SQL bolted on top would double the surface area without adding capability.
- **Multi-master / strongly-consistent replication.** Beyond scope. Use a database that targets that workload if you need it.
- **In-process / embedded mode.** Always a server, always TCP. Simpler mental model, simpler auth story.
- **New `MAX_*` ceilings.** `MAX_SPLITS=4096`, `MAX_FIELDS=256`, `MAX_KEY_CEILING=1024` are deliberate. If you have a workload that needs more, open an issue describing it before sending a PR.

## Security issues

Don't open a public GitHub Issue for a vulnerability report. The disclosure flow is in [`SECURITY.md`](SECURITY.md) — TL;DR: GitHub Security Advisories or email.

## Code of conduct

Be kind. We don't have a separate file for this; the standard summary applies: assume good faith, focus on the change not the contributor, no harassment.

## License

By contributing, you agree that your changes are licensed under the same MIT license as the rest of the project (see [`LICENSE`](LICENSE)). The repo doesn't require a CLA — the standard "PR = license grant" model applies.
