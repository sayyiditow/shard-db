#!/bin/bash
# Sweep pool size × submit-chunk for parallel KV CSV bench.
# Usage: ./sweep-pool.sh
cd "$(dirname "$0")/.."

BIN="./shard-db"
TOTAL=10000000
CHUNK=1000000
CONNS=10
SPLITS=1024
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
NCHUNKS=$(( (TOTAL + CHUNK - 1) / CHUNK ))

ensure_data() {
    if [ ! -f /tmp/shard-db_kv_par_0.csv ] || [ "$(wc -l < /tmp/shard-db_kv_par_0.csv 2>/dev/null)" != "$CHUNK" ]; then
        echo "Generating $NCHUNKS × $CHUNK records..."
        TOTAL=$TOTAL CHUNK=$CHUNK python3 -c '
import os, random, hashlib
total = int(os.environ["TOTAL"])
chunk_size = int(os.environ["CHUNK"])
for c in range(0, total, chunk_size):
    end = min(c + chunk_size, total)
    idx = c // chunk_size
    with open(f"/tmp/shard-db_kv_par_{idx}.csv", "w") as f:
        for i in range(c, end):
            key = hashlib.sha256(str(i).encode()).hexdigest()[:32]
            val = f"payload-{i}-{random.randint(1,1000000):08d}"
            f.write(f"{key}|{val}\n")
'
    fi
}

run_once() {
    local T=$1
    local C=$2
    $BIN stop 2>/dev/null || true; sleep 0.3
    sed -i "s/^export THREADS=.*/export THREADS=$T/" db.env
    POOL_CHUNK=$C $BIN start > /dev/null 2>&1
    sleep 0.5

    $BIN query '{"mode":"truncate","dir":"default","object":"kvbench"}' > /dev/null 2>&1
    rm -rf "$DB_ROOT/default/kvbench"
    sed -i '/^default:kvbench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
    $BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"kvbench\",\"splits\":$SPLITS,\"max_key\":64,\"fields\":[\"v:varchar:100\"],\"indexes\":[]}" > /dev/null

    local START=$(date +%s.%N)
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.02; done
        $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"kvbench\",\"file\":\"/tmp/shard-db_kv_par_$i.csv\",\"delimiter\":\"|\"}" > /dev/null &
    done
    wait
    local END=$(date +%s.%N)
    awk "BEGIN{printf \"%.3f\", $END - $START}"
    $BIN stop > /dev/null 2>&1
}

ensure_data

echo "Parallel CSV KV 10M records, 10 conns × 1M, SPLITS=$SPLITS"
echo "Columns: POOL_CHUNK. Rows: THREADS (pool size)."
echo ""
printf "         "
for C in 1 4 8 16 32 64; do printf "  chunk=%-4d" $C; done
echo ""

for T in 16 32 64 96 128; do
    printf "THREADS=%-4d" $T
    for C in 1 4 8 16 32 64; do
        t=$(run_once $T $C)
        printf "  %7ss  " $t
    done
    echo ""
done

# Restore
sed -i 's/^export THREADS=.*/export THREADS=0/' db.env
$BIN query '{"mode":"truncate","dir":"default","object":"kvbench"}' > /dev/null 2>&1
rm -rf "$DB_ROOT/default/kvbench"
sed -i '/^default:kvbench:/d' "$DB_ROOT/schema.conf" 2>/dev/null

echo ""
echo "Done. THREADS in db.env restored to 0 (auto = 4× cores)."
