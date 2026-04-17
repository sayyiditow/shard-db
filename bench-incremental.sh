#!/bin/bash
# bench-incremental.sh — find rebuild-vs-point crossover empirically.
# Since SHARDKV_BULK_RATIO must be set when the SERVER starts (not the client),
# we stop/start the server around each strategy change.

BIN="./shard-db"
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
BASELINE=900000
BATCHES="1000 10000 50000 100000 200000"

start_server() {
    local ratio="$1"
    $BIN stop 2>/dev/null || true
    sleep 0.3
    SHARDKV_BULK_RATIO="$ratio" $BIN start > /dev/null
    sleep 0.5
}

gen_records() {
    local start=$1 end=$2 out=$3
    python3 -c "
import json
recs = []
for i in range($start, $end):
    recs.append({'id': f'INV-{i:08d}', 'data': {
        'status': ['DRAFT','PENDING','APPROVED','REJECTED'][i%4],
        'region': ['EU','US','APAC','LATAM','ME'][i%5],
        'amount': i * 1.5,
    }})
with open('$out','w') as f:
    json.dump(recs, f)
"
}

time_cmd() {
    local t0=$(date +%s.%N)
    "$@" > /dev/null 2>&1
    local t1=$(date +%s.%N)
    awk "BEGIN{printf \"%.3f\", $t1 - $t0}"
}

setup_baseline() {
    echo ">>> Setup: 900K baseline with 2 indexes"
    $BIN query '{"mode":"truncate","dir":"default","object":"inc"}' 2>/dev/null || true
    rm -rf "$DB_ROOT/default/inc"
    sed -i '/^default:inc:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
    OUT=$($BIN query '{"mode":"create-object","dir":"default","object":"inc","splits":64,"max_key":32,"fields":["status:varchar:16","region:varchar:16","amount:double"],"indexes":["status","region"]}')
    echo "    create: $OUT"

    if [ ! -f /tmp/inc_baseline.json ]; then
        echo "    generating $BASELINE records..."
        gen_records 0 $BASELINE /tmp/inc_baseline.json
    fi

    echo -n "    inserting baseline: "
    T=$(time_cmd $BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"inc\",\"file\":\"/tmp/inc_baseline.json\"}")
    echo "${T}s"
    SIZE=$($BIN query '{"mode":"size","dir":"default","object":"inc"}')
    echo "    size: $SIZE"
    if ! echo "$SIZE" | grep -q "\"count\":$BASELINE"; then
        echo "    ERROR: baseline did not fully insert — aborting" >&2
        exit 1
    fi
}

measure_batch() {
    local N=$1
    local label="$2"

    # Generate the batch once
    if [ ! -f /tmp/inc_batch_$N.json ]; then
        gen_records $BASELINE $((BASELINE + N)) /tmp/inc_batch_$N.json
    fi

    # Measure 3× take best
    local best=999999
    for _ in 1 2 3; do
        T=$(time_cmd $BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"inc\",\"file\":\"/tmp/inc_batch_$N.json\"}")
        if awk "BEGIN{exit !($T<$best)}"; then best=$T; fi

        # Remove added records so next iteration starts from the 900K baseline
        python3 -c "
import json
keys = [f'INV-{i:08d}' for i in range($BASELINE, $BASELINE + $N)]
with open('/tmp/inc_del.json','w') as f: json.dump(keys, f)
"
        $BIN query '{"mode":"bulk-delete","dir":"default","object":"inc","file":"/tmp/inc_del.json"}' > /dev/null 2>&1
        rm -f /tmp/inc_del.json
    done
    echo "$best"
}

run_with_strategy() {
    local ratio="$1"
    local label="$2"
    echo ""
    echo "=== STRATEGY: $label (SHARDKV_BULK_RATIO=$ratio) ==="
    start_server "$ratio"
    setup_baseline

    for N in $BATCHES; do
        T=$(measure_batch $N)
        printf "    batch=%-8s  %s: %ss\n" "$N" "$label" "$T"
        eval "${label}_${N}=$T"
    done
}

echo "======================================"
echo "  Incremental bulk-insert crossover"
echo "  baseline=$BASELINE  batches=$BATCHES"
echo "======================================"

run_with_strategy 0 "REBUILD"
run_with_strategy 1 "POINT"

echo ""
echo "======================================"
echo "  SUMMARY (best of 3 per batch size)"
echo "======================================"
printf "%-12s %-14s %-14s %-10s\n" "batch" "REBUILD(s)" "POINT(s)" "winner"
for N in $BATCHES; do
    R=$(eval echo \$REBUILD_${N})
    P=$(eval echo \$POINT_${N})
    W="REBUILD"
    if awk "BEGIN{exit !($P<$R)}"; then W="POINT"; fi
    printf "%-12s %-14s %-14s %-10s\n" "$N" "$R" "$P" "$W"
done

$BIN stop 2>/dev/null || true
rm -f /tmp/inc_del.json
echo ""
echo "Done. (/tmp/inc_baseline.json and /tmp/inc_batch_*.json kept for reruns)"
