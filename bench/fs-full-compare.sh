#!/bin/bash
# Run the 4 main README benchmarks across 3 filesystems and aggregate results.
# Expects the loop mounts /mnt/ext4-tuned and /mnt/xfs-test to exist.
set -u
cd "$(dirname "$0")/.."

BIN="./shard-db"
OUT=/tmp/fs-compare-results
mkdir -p "$OUT"
rm -f "$OUT"/*.log

run_suite() {
    local label="$1"
    local root="$2"
    echo ""
    echo "###############################################"
    echo "  [$label]  DB_ROOT=$root"
    echo "###############################################"

    # Prep root
    mkdir -p "$root"
    rm -rf "$root"/*
    echo "default" > "$root/dirs.conf"
    touch "$root/schema.conf"

    # Swap db.env DB_ROOT
    sed -i "s|^export DB_ROOT=.*|export DB_ROOT=\"$root\"|" db.env

    local log="$OUT/${label}.log"
    : > "$log"

    # --- KV single-threaded 10M ---
    echo "--- KV single 10M ---" | tee -a "$log"
    $BIN stop 2>/dev/null || true; sleep 0.3
    SPLITS=512 timeout 300 bash bench/bench-kv.sh 10000000 2>&1 | grep -E "real|insert|throughput|count" | tee -a "$log" || true
    $BIN stop 2>/dev/null; sleep 0.3

    # --- KV parallel 10M × 10 ---
    echo "" | tee -a "$log"
    echo "--- KV parallel 10M × 10 ---" | tee -a "$log"
    $BIN stop 2>/dev/null || true; sleep 0.3
    SPLITS=1024 timeout 300 bash bench/bench-kv-parallel.sh 10000000 1000000 10 2>&1 | grep -E "TEST|real|count" | tee -a "$log" || true
    $BIN stop 2>/dev/null; sleep 0.3

    # --- Invoice single 1M ---
    echo "" | tee -a "$log"
    echo "--- Invoice single 1M ---" | tee -a "$log"
    $BIN stop 2>/dev/null || true; sleep 0.3
    timeout 300 bash bench/bench-invoice.sh 1000000 persistent 2>&1 | grep -E "real|Bulk insert|count" | tee -a "$log" || true
    $BIN stop 2>/dev/null; sleep 0.3

    # --- Invoice parallel 1M × 10 ---
    echo "" | tee -a "$log"
    echo "--- Invoice parallel 1M × 10 ---" | tee -a "$log"
    $BIN stop 2>/dev/null || true; sleep 0.3
    timeout 300 bash bench/bench-parallel.sh 1000000 100000 10 2>&1 | grep -E "TEST|real|count" | tee -a "$log" || true
    $BIN stop 2>/dev/null; sleep 0.3
}

# Save initial db.env
cp db.env .db.env.original

run_suite "default-ext4" "./db"
run_suite "tuned-ext4"   "/mnt/ext4-tuned/shards"
run_suite "tuned-xfs"    "/mnt/xfs-test/shards"

# Restore db.env
mv .db.env.original db.env

echo ""
echo "==============================="
echo "  All logs in $OUT"
echo "==============================="
ls -l "$OUT"
