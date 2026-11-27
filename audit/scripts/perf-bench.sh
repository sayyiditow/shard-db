#!/bin/bash
# audit/scripts/perf-bench.sh <bench-name> [bench args...]
#
# Profiling prep (campaign Phase 1 step 7): wraps one registered bench
# with perf record + a flat profile report. The HUMAN runs this — the
# campaign only builds targets and scripts.
#
# Output: perf.data + perf-report.txt in the CWD (move aside between
# runs; perf refuses to overwrite a live perf.data).
#
# Example:
#   ./audit/scripts/perf-bench.sh bench-queries
#   ./audit/scripts/perf-bench.sh bench-kv
#
# Requires: linux perf ("perf --version"); the bench binary at
# ./build/bin/shard-db-bench.
set -e
BENCH=${1:?usage: perf-bench.sh <bench-name> [bench args...]}
shift || true

command -v perf >/dev/null || { echo "perf not installed" >&2; exit 1; }
[ -x ./build/bin/shard-db-bench ] || { echo "build/bin/shard-db-bench missing (SKIP_TESTS=1 ./build.sh)" >&2; exit 1; }

OUT="perf-report-$(date +%Y%m%d-%H%M%S).txt"

perf record -F 999 -g -o perf.data \
    ./build/bin/shard-db-bench run "$BENCH" "$@"
perf report --stdio --no-children -i perf.data | tee "$OUT"

echo
echo "wrote $OUT (perf.data kept for interactive 'perf report -i perf.data')"
