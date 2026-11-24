#!/bin/bash
# audit/scripts/run-gate-163.sh
#
# Known-bug gate for the crash/restart harness (plan Phase 1 step 4): the
# kf grave-frontier clobber must be reproduced on 163b36a (parent of fix
# f3c6ee5) and absent on the current build.
#
# The gate is the deterministic, craft-seeded case `audit-craft-grave-frontier`
# (the incident shape byte-exact): it FAILS on 163b36a and PASSES on the fixed
# build. The random biased crash/restart fuzzer is NOT used as the gate — it
# did not rediscover this bug within the plan's seed budget; it is run here
# only as an informational smoke on the fixed build.
#
# Usage: run-gate-163.sh <repo-with-harness> <gate-worktree> [seeds]
# The worktree must already exist and be checked out at 163b36a, and
# overlay-gate-163.sh must have run against it.
set -e
SRC=${1:?usage: run-gate-163.sh <repo> <gate-worktree> [seeds]}
DST=${2:?usage: run-gate-163.sh <repo> <gate-worktree> [seeds]}
SEEDS=${3:-20}

echo "=== building gate daemon at $DST (163b36a) ==="
BUILD_LOG=$(mktemp /tmp/gate163-build.XXXXXX)
( cd "$DST" && SKIP_TESTS=1 ./build.sh > "$BUILD_LOG" 2>&1 ) \
    || { tail -20 "$BUILD_LOG"; exit 1; }
rm -f "$BUILD_LOG"

echo "=== GATE A: 163b36a must FAIL the craft-seeded case (bug rediscovered) ==="
GATE_A_LOG=$(mktemp /tmp/gate163-A.XXXXXX)
if ( cd "$DST" && ./build/bin/shard-db-test run-all --include-audit \
        --filter audit-craft-grave-frontier ) > "$GATE_A_LOG" 2>&1; then
    echo "GATE A UNEXPECTED: craft case PASSED on 163b36a — bug NOT rediscovered"
    exit 2
fi
# A rediscovery is a failed ASSERTION of the case (a "not ok" TAP line); a
# build break, crash or missing binary prints none and is an environment
# failure.
if grep -q "^not ok" "$GATE_A_LOG"; then
    echo "GATE A OK: craft case failed on 163b36a (bug rediscovered):"
    grep "^not ok" "$GATE_A_LOG" | head -3
else
    echo "GATE A ENVIRONMENT FAILURE: no failed assertion (log: $GATE_A_LOG)"
    tail -20 "$GATE_A_LOG"
    exit 4
fi
rm -f "$GATE_A_LOG"

echo "=== GATE B: current build must PASS the craft-seeded case ==="
( cd "$SRC" && ./build/bin/shard-db-test run-all --include-audit \
    --filter audit-craft-grave-frontier ) \
    && echo "GATE B OK: craft case passed on the fixed build" \
    || { echo "GATE B FAILURE: craft case failed on the FIXED build"; exit 3; }

echo "=== informational: random crash/restart smoke on the fixed build ($SEEDS seeds) ==="
( cd "$SRC" && SHARD_AUDIT_SEEDS="$SEEDS" SHARD_AUDIT_OPS=30 \
    SHARD_AUDIT_BIASED_CYCLES=2 SHARD_AUDIT_FIRST_SEED=1001 \
    ./build/bin/shard-db-test run audit-crash-restart ) \
    && echo "smoke OK" || echo "smoke FAILED on the fixed build (investigate)"

echo "gate complete"
