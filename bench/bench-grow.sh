#!/bin/bash
# Run from project root regardless of CWD.
cd "$(dirname "$0")/.."
# bench-grow.sh — measure shard-grow cost during bulk-insert.
#
# Two scenarios exercise different grow-pressure regimes:
#   1. heavy:  8 splits × 5M rows  — many grows/shard pre-fix
#   2. light: 16 splits × 1M rows  — typical user shape (~1 grow/shard)
#
# The daemon already logs `grows=N grow_total=Tms` per bulk-insert via
# the BULK-INSERT log line at LOG_LEVEL >= 3.  db.env has LOG_LEVEL=3 by
# default, so no extra config needed.
#
# Usage:
#   ./bench/bench-grow.sh
#
# Compare two builds by running this once on each commit; the script just
# reports timings — the operator does the diff.

set -euo pipefail

BIN="./shard-db"
DB_ROOT=$(grep ^export\ DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
PORT=$(grep ^export\ PORT db.env | sed "s/.*=\([0-9]*\).*/\1/")
PORT=${PORT:-9199}
LOG_DIR=$(grep ^export\ LOG_DIR db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
LOG_FILE="$LOG_DIR/$(date +%Y-%m-%d)-info.log"

echo "======================================"
echo "  shard-db grow bench"
echo "  DB_ROOT=$DB_ROOT  PORT=$PORT  LOG=$LOG_FILE"
echo "======================================"

grep -q "^bench$" "$DB_ROOT/dirs.conf" 2>/dev/null || \
    echo "bench" >> "$DB_ROOT/dirs.conf"

$BIN stop 2>/dev/null || true
sleep 0.3

# Clean prior bench objects
rm -rf "$DB_ROOT/bench/heavy" "$DB_ROOT/bench/light"
sed -i "/^bench:heavy:/d;/^bench:light:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true

$BIN start
sleep 0.5

q_cli() { $BIN query "$1"; }

run_scenario() {
    local label="$1" splits="$2" rows="$3" obj="$4"
    local input="/tmp/bench-grow-$obj.csv"

    echo ""
    echo "--- scenario: $label  (splits=$splits, rows=$rows, obj=$obj) ---"

    q_cli "{\"mode\":\"create-object\",\"dir\":\"bench\",\"object\":\"$obj\",\"splits\":$splits,\"max_key\":24,\"fields\":[\"v:varchar:128\"]}" > /dev/null

    # Generate CSV: keyN,valueN
    python3 -c "
n = $rows
with open('$input','w') as f:
    for i in range(n):
        f.write('k%010d,v%010d\n' % (i, i))
" > /dev/null

    # Mark log offset before the run so we only grep this scenario's line.
    local before_lines
    before_lines=$(wc -l < "$LOG_FILE" 2>/dev/null || echo 0)

    local t0 t1
    t0=$(date +%s%3N)
    q_cli "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"bench\",\"object\":\"$obj\",\"file\":\"$input\",\"delimiter\":\",\"}" > /dev/null
    t1=$(date +%s%3N)

    sleep 0.2  # let the log line flush

    # Pull the BULK-INSERT log line that landed after our offset
    local line
    line=$(tail -n +$((before_lines + 1)) "$LOG_FILE" 2>/dev/null | grep "BULK-INSERT $obj:" | tail -1 || true)

    local grows="-" grow_ms="-" phase2="-" total="-"
    if [[ -n "$line" ]]; then
        grows=$(echo   "$line" | grep -oP 'grows=\K[0-9]+'         || echo "-")
        grow_ms=$(echo "$line" | grep -oP 'grow_total=\K[0-9]+'    || echo "-")
        phase2=$(echo  "$line" | grep -oP 'phase2_write=\K[0-9]+'  || echo "-")
        total=$(echo   "$line" | grep -oP 'total=\K[0-9]+'         || echo "-")
    fi

    printf "  rows=%-8s wall=%dms  daemon: grows=%-3s grow_ms=%-5s phase2_write=%-5s total=%-5s\n" \
        "$rows" "$((t1 - t0))" "$grows" "$grow_ms" "$phase2" "$total"

    rm -f "$input"
}

run_scenario "heavy-grow"  8  5000000 heavy
run_scenario "light-grow"  16 1000000 light

echo ""
echo "--- final shard-stats (heavy) ---"
q_cli "{\"mode\":\"shard-stats\",\"dir\":\"bench\",\"object\":\"heavy\"}" | head -c 800
echo ""
echo "--- final shard-stats (light) ---"
q_cli "{\"mode\":\"shard-stats\",\"dir\":\"bench\",\"object\":\"light\"}" | head -c 800
echo ""

$BIN stop 2>/dev/null || true

echo ""
echo "======================================"
echo "  done"
echo "======================================"
