#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# Parallel K/V insert benchmark — mirrors bench-parallel.sh but for the
# single-field varchar(100) schema. Use to compare parallel insert
# throughput against the invoice parallel bench.
# Usage: ./bench-kv-parallel.sh [total_records] [chunk_size] [connections]

TOTAL=${1:-1000000}
CHUNK=${2:-200000}
CONNS=${3:-5}
BIN="./shard-db"
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
PORT=$(grep PORT db.env | sed "s/.*[\"']\{0,1\}\([0-9]*\).*/\1/")
PORT=${PORT:-9199}
OBJ="kvbench"
NCHUNKS=$(( (TOTAL + CHUNK - 1) / CHUNK ))

echo "======================================"
echo "  shard-db K/V parallel benchmark"
echo "  $TOTAL records, ${CHUNK}/chunk, $CONNS connections"
echo "  $NCHUNKS chunks"
echo "  key=32B hex, value=varchar(100)"
echo "======================================"

grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

$BIN stop 2>/dev/null || true
sleep 0.3
rm -rf "$DB_ROOT/default/$OBJ"
sed -i "/^default:$OBJ:/d" "$DB_ROOT/schema.conf" 2>/dev/null
$BIN start
sleep 0.5

q_cli() { $BIN query "$1"; }

# Generate chunks
echo ""
echo "Generating $NCHUNKS chunks ($CHUNK records each)..."
TOTAL=$TOTAL CHUNK=$CHUNK python3 << 'PYEOF'
import json, hashlib, os
total = int(os.environ['TOTAL'])
chunk_size = int(os.environ['CHUNK'])
for c in range(0, total, chunk_size):
    end = min(c + chunk_size, total)
    idx = c // chunk_size
    recs = []
    for i in range(c, end):
        k = hashlib.sha256(str(i).encode()).hexdigest()[:32]
        v = ('val_' + str(i)).ljust(100, 'x')[:100]
        recs.append({'id': k, 'data': {'v': v}})
    with open(f'/tmp/shard-db_kv_par_{idx}.json', 'w') as f:
        json.dump(recs, f, separators=(',', ':'))
    with open(f'/tmp/shard-db_kv_par_{idx}.csv', 'w') as f:
        for r in recs:
            f.write(r['id'] + ',' + r['data']['v'] + '\n')

# Combined single-file baseline
with open('/tmp/shard-db_kv_par_single.json', 'w') as out:
    out.write('[')
    for c in range(0, total, chunk_size):
        idx = c // chunk_size
        with open(f'/tmp/shard-db_kv_par_{idx}.json') as f:
            d = f.read().strip()
            if d.startswith('['): d = d[1:]
            if d.endswith(']'): d = d[:-1]
            if idx > 0: out.write(',')
            out.write(d)
    out.write(']')
PYEOF

ls -lh /tmp/shard-db_kv_par_0.json /tmp/shard-db_kv_par_0.csv /tmp/shard-db_kv_par_single.json

create_fresh() {
    $BIN query "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"$OBJ\"}" > /dev/null 2>&1
    rm -rf "$DB_ROOT/default/$OBJ"
    sed -i "/^default:$OBJ:/d" "$DB_ROOT/schema.conf" 2>/dev/null
    $BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"$OBJ\",\"splits\":256,\"max_key\":32,\"fields\":[\"v:varchar:100\"]}" > /dev/null
}

# ==================== TEST 1a: Single JSON file (baseline) ====================
echo ""
echo "--- TEST 1a: Single JSON file, $TOTAL records ---"
create_fresh
time $BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_kv_par_single.json\"}" > /dev/null
$BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}"

# ==================== TEST 1b: Single CSV file (baseline) ====================
echo ""
echo "--- TEST 1b: Single CSV file, $TOTAL records ---"
create_fresh
cat /tmp/shard-db_kv_par_*.csv > /tmp/shard-db_kv_par_single.csv
time $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_kv_par_single.csv\",\"delimiter\":\",\"}" > /dev/null
$BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}"

# ==================== TEST 2: Parallel JSON chunks ====================
echo ""
echo "--- TEST 2: Parallel JSON $CONNS connections × ${CHUNK} ---"
create_fresh
time (
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.05; done
        $BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_kv_par_$i.json\"}" > /dev/null &
    done
    wait
)
$BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}"

# ==================== TEST 3: Parallel CSV chunks ====================
echo ""
echo "--- TEST 3: Parallel CSV $CONNS connections × ${CHUNK} ---"
create_fresh
time (
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.05; done
        $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_kv_par_$i.csv\",\"delimiter\":\",\"}" > /dev/null &
    done
    wait
)
$BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}"

# ==================== DISK + SHARD STATS ====================
echo ""
du -sh "$DB_ROOT/default/$OBJ"
echo ""
echo "--- SHARD STATS ---"
$BIN query "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"$OBJ\"}" | python3 -c "
import json, sys
d = json.loads(sys.stdin.read())
print(f\"splits={d['splits']} shards={d['shards']} total_records={d['total_records']} total_bytes={d['total_bytes']:,} max_grows={d['max_grows']}\")
loads = [s['load'] for s in d['shard_list']]
recs = [s['records'] for s in d['shard_list']]
if loads:
    print(f\"load: min={min(loads):.3f} max={max(loads):.3f} avg={sum(loads)/len(loads):.3f}\")
    print(f\"recs: min={min(recs)} max={max(recs)} stddev_pct={(max(recs)-min(recs))/max(max(recs),1)*100:.1f}%\")
if 'hint' in d: print('hint:', d['hint'])
"

# ==================== RECOUNT (diagnostic — is metadata counter off, or actual data loss?) ====================
echo ""
echo "--- RECOUNT vs SIZE ---"
echo "size:    $($BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}")"
echo "recount: $($BIN query "{\"mode\":\"recount\",\"dir\":\"default\",\"object\":\"$OBJ\"}")"

# ==================== CLEANUP ====================
$BIN query "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"$OBJ\"}" > /dev/null 2>&1
rm -rf "$DB_ROOT/default/$OBJ"
sed -i "/^default:$OBJ:/d" "$DB_ROOT/schema.conf" 2>/dev/null
find /tmp -maxdepth 1 -name 'shard-db_kv_par_*' -delete 2>/dev/null
$BIN stop

echo ""
echo "======================================"
echo "  K/V Parallel benchmark complete"
echo "======================================"
