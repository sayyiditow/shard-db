#!/bin/bash
# Run only the 2 KV benches (single + parallel) across 3 filesystems.
# Adds a cooldown between suites to reduce thermal contamination.
set -u
cd "$(dirname "$0")/.."

BIN="./shard-db"
OUT=/tmp/fs-kv-results
mkdir -p "$OUT"
rm -f "$OUT"/*.log

run_suite() {
    local label="$1"
    local root="$2"
    local cooldown="${3:-20}"
    echo ""
    echo "###############################################"
    echo "  [$label]  DB_ROOT=$root  (cooldown ${cooldown}s first)"
    echo "###############################################"

    sleep "$cooldown"

    mkdir -p "$root"
    rm -rf "$root"/*
    echo "default" > "$root/dirs.conf"
    touch "$root/schema.conf"

    sed -i "s|^export DB_ROOT=.*|export DB_ROOT=\"$root\"|" db.env

    local log="$OUT/${label}.log"
    : > "$log"

    # KV single 10M (bench-kv.sh) — captures the bulk-insert numbers
    echo "--- KV single 10M ---" | tee -a "$log"
    $BIN stop 2>/dev/null || true; sleep 0.3
    SPLITS=512 timeout 300 bash bench/bench-kv.sh 10000000 2>&1 \
        | grep -E "^real|count|insert|throughput|Bulk|single-thread" | tee -a "$log" || true
    $BIN stop 2>/dev/null; sleep 0.3

    sleep 10

    # KV parallel 10M × 10
    echo "" | tee -a "$log"
    echo "--- KV parallel 10M × 10 ---" | tee -a "$log"
    $BIN stop 2>/dev/null || true; sleep 0.3
    SPLITS=1024 timeout 300 bash bench/bench-kv-parallel.sh 10000000 1000000 10 2>&1 \
        | grep -E "TEST|real|count" | tee -a "$log" || true
    $BIN stop 2>/dev/null; sleep 0.3
}

cp db.env .db.env.orig

run_suite "default-ext4" "./db"        0
run_suite "tuned-ext4"   "/mnt/ext4-tuned/shards" 20
run_suite "tuned-xfs"    "/mnt/xfs-test/shards"   20

mv .db.env.orig db.env
echo ""
echo "logs in $OUT"
