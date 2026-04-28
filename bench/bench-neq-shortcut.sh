#!/bin/bash
# Run from project root regardless of CWD.
cd "$(dirname "$0")/.."
# bench-neq-shortcut.sh — A/B latency for the aggregate(NEQ X) algebraic
# shortcut on an indexed field. Compares two pre-built binaries:
#   ./shard-db.with     — shortcut enabled (HEAD)
#   ./shard-db.without  — same source, eligibility forced to 0
# Identical refactor in both, so any delta is the shortcut path itself.

set -e

[ -x ./shard-db.with ] || { echo "missing ./shard-db.with — build & save first"; exit 1; }
[ -x ./shard-db.without ] || { echo "missing ./shard-db.without — build & save first"; exit 1; }

ITERS=5
# Size check: start daemon briefly, query, stop. Avoids ambiguity if a stale
# daemon is around from a prior run.
./shard-db stop 2>/dev/null || true
sleep 0.3
./shard-db start > /dev/null
sleep 0.4
SIZE=$(./shard-db query '{"mode":"size","dir":"default","object":"users"}' 2>/dev/null \
       | grep -oE '[0-9]+' | head -1)
./shard-db stop > /dev/null
sleep 0.3
[ -z "$SIZE" ] && { echo "users object not found — run bench/create-user-object.sh + insert-users.sh first"; exit 1; }
echo "users: $SIZE rows; $ITERS iters per query, min latency reported"
echo

# Each query is run $ITERS times, min latency reported (warm-cache best-case).
# `agg neq age=30`, `agg neq age=30 + sum/avg`, `agg neq active=false` cover
# the high-selectivity and low-selectivity ends. Controls verify identical
# paths produce identical timings (no regression on non-shortcut paths).
QUERIES=(
  "agg-count-neq-age|{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],\"criteria\":[{\"field\":\"age\",\"op\":\"neq\",\"value\":\"30\"}]}"
  "agg-sum-neq-age|{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"balance\",\"alias\":\"s\"}],\"criteria\":[{\"field\":\"age\",\"op\":\"neq\",\"value\":\"30\"}]}"
  "agg-avg-neq-age|{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"balance\",\"alias\":\"a\"}],\"criteria\":[{\"field\":\"age\",\"op\":\"neq\",\"value\":\"30\"}]}"
  "agg-mixed-neq-age|{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"balance\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"balance\",\"alias\":\"a\"}],\"criteria\":[{\"field\":\"age\",\"op\":\"neq\",\"value\":\"30\"}]}"
  "agg-count-neq-active|{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],\"criteria\":[{\"field\":\"active\",\"op\":\"neq\",\"value\":\"false\"}]}"
  "CONTROL-agg-count-eq-age|{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"}]}"
  "CONTROL-agg-count-nocriteria|{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}"
)

bench_one() {
    local label="$1"
    local req="$2"
    local min_ms=999999
    local result=""
    for _ in $(seq 1 $ITERS); do
        local t0=$(date +%s%N)
        result=$(./shard-db query "$req" 2>/dev/null)
        local t1=$(date +%s%N)
        local ms=$(( (t1 - t0) / 1000000 ))
        [ "$ms" -lt "$min_ms" ] && min_ms=$ms
    done
    printf "  %-32s %5d ms   %s\n" "$label" "$min_ms" "$(echo "$result" | head -c 60)"
}

run_against() {
    local binary="$1"
    local label="$2"
    ./shard-db stop 2>/dev/null || true
    sleep 0.3
    cp "$binary" ./shard-db
    cp "$binary" ./build/bin/shard-db
    ./shard-db start > /dev/null
    sleep 0.5
    echo "=== $label ($binary) ==="
    # Warmup: run each query once before measuring.
    for entry in "${QUERIES[@]}"; do
        IFS='|' read -r _ req <<<"$entry"
        ./shard-db query "$req" > /dev/null 2>&1
    done
    for entry in "${QUERIES[@]}"; do
        IFS='|' read -r label2 req <<<"$entry"
        bench_one "$label2" "$req"
    done
    ./shard-db stop > /dev/null
    sleep 0.3
    echo
}

run_against ./shard-db.without "WITHOUT shortcut (force-disabled)"
run_against ./shard-db.with    "WITH    shortcut (HEAD)"

# Restore the canonical .with binary as the live ./shard-db.
cp ./shard-db.with ./shard-db
cp ./shard-db.with ./build/bin/shard-db
echo "(restored ./shard-db = with-shortcut binary)"
