#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# Pure key/value throughput — what shard-db looks like as a plain Map.
# Schema: 1 field varchar(100), keys = 16-byte hex.
# Matches the industry-standard 16B key / 100B value record shape used
# by LMDB / LevelDB / RocksDB db_bench so numbers compare directly.
# Usage: ./bench-kv.sh [record_count]

COUNT=${1:-1000000}
SPLITS=${SPLITS:-128}
BIN="./shard-db"
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
PORT=$(grep PORT db.env | sed "s/.*[\"']\{0,1\}\([0-9]*\).*/\1/")
PORT=${PORT:-9199}
OBJ="kvbench"

echo "======================================"
echo "  shard-db K/V benchmark ($COUNT records)"
echo "  key=16B hex, value=varchar(100) — matches db_bench / LMDB shape"
echo "======================================"

grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

$BIN stop 2>/dev/null || true
sleep 0.3

# Clean prior bench
rm -rf "$DB_ROOT/default/$OBJ"
sed -i "/^default:$OBJ:/d" "$DB_ROOT/schema.conf" 2>/dev/null

$BIN start
sleep 0.5

q() { echo "$1" | socat - TCP:localhost:$PORT 2>/dev/null | tr -d '\0'; }
q_cli() { $BIN query "$1"; }

# 256 splits keeps shard load manageable; max_key=16 max_value=100
q_cli "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"$OBJ\",\"splits\":$SPLITS,\"max_key\":16,\"fields\":[\"v:varchar:100\"]}" > /dev/null

echo ""
echo "Generating $COUNT records (16B hex key, ~100B value)..."
python3 -c "
import json, hashlib
recs = []
for i in range($COUNT):
    k = hashlib.sha256(str(i).encode()).hexdigest()[:16]  # exactly 16 hex chars
    v = ('val_' + str(i)).ljust(100, 'x')[:100]
    recs.append({'id': k, 'data': {'v': v}})
with open('/tmp/shard-db_kv.json','w') as f:
    json.dump(recs, f)
with open('/tmp/shard-db_kv.csv','w') as f:
    for r in recs:
        f.write(r['id'] + ',' + r['data']['v'] + '\n')
with open('/tmp/shard-db_kv_keys.txt','w') as f:
    for r in recs:
        f.write(r['id'] + '\n')
print('done')
"

# ==================== BULK INSERT (single connection, JSON file) ====================
echo ""
echo "--- BULK INSERT JSON ($COUNT records, 1 file) ---"
time q_cli "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_kv.json\"}"
q_cli "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}"

# ==================== BULK INSERT CSV ====================
echo ""
echo "--- BULK INSERT CSV ($COUNT records, 1 file) ---"
q_cli "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"$OBJ\"}" > /dev/null
time q_cli "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_kv.csv\",\"delimiter\":\",\"}"
q_cli "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}"

# ==================== GET single random ====================
echo ""
echo "--- GET single (warm) ---"
SAMPLE=$(shuf -n 1 /tmp/shard-db_kv_keys.txt)
time q "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"$OBJ\",\"key\":\"$SAMPLE\"}" > /dev/null

# ==================== GET x10000 pipelined ====================
echo ""
echo "--- GET x10000 (pipelined, 1 connection) ---"
time (
    shuf -n 10000 /tmp/shard-db_kv_keys.txt | while read k; do
        echo "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"$OBJ\",\"key\":\"$k\"}"
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== EXISTS x10000 ====================
echo ""
echo "--- EXISTS x10000 (pipelined) ---"
time (
    shuf -n 10000 /tmp/shard-db_kv_keys.txt | while read k; do
        echo "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"$OBJ\",\"key\":\"$k\"}"
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== EXISTS-MISS x10000 (cold-miss path) ====================
echo ""
echo "--- EXISTS x10000 (all misses, cold probe path) ---"
time (
    for i in $(seq 1 10000); do
        echo "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"$OBJ\",\"key\":\"nope_$i\"}"
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== UPDATE x10000 ====================
echo ""
echo "--- UPDATE x10000 (pipelined, value field) ---"
time (
    shuf -n 10000 /tmp/shard-db_kv_keys.txt | while read k; do
        echo "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"$OBJ\",\"key\":\"$k\",\"value\":{\"v\":\"updated_$k\"}}"
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== DELETE x10000 ====================
echo ""
echo "--- DELETE x10000 (pipelined) ---"
time (
    shuf -n 10000 /tmp/shard-db_kv_keys.txt | while read k; do
        echo "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"$OBJ\",\"key\":\"$k\"}"
    done | socat - TCP:localhost:$PORT > /dev/null
)
q_cli "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}"

# ==================== Re-populate for parallel test ====================
echo ""
echo "--- Re-populating for parallel test ---"
q_cli "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"$OBJ\"}" > /dev/null
q_cli "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_kv.json\"}" > /dev/null

# ==================== PARALLEL GET (5 connections × 10000) ====================
echo ""
echo "--- PARALLEL GET 5 connections × 10000 each (50000 total) ---"
time (
    for c in 0 1 2 3 4; do
        (shuf -n 10000 /tmp/shard-db_kv_keys.txt | while read k; do
            echo "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"$OBJ\",\"key\":\"$k\"}"
        done | socat - TCP:localhost:$PORT > /dev/null) &
    done
    wait
)

# ==================== PARALLEL UPDATE (5 connections × 10000) ====================
echo ""
echo "--- PARALLEL UPDATE 5 connections × 10000 each ---"
time (
    for c in 0 1 2 3 4; do
        (shuf -n 10000 /tmp/shard-db_kv_keys.txt | while read k; do
            echo "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"$OBJ\",\"key\":\"$k\",\"value\":{\"v\":\"par_$k\"}}"
        done | socat - TCP:localhost:$PORT > /dev/null) &
    done
    wait
)

# ==================== Disk usage ====================
echo ""
echo "--- DISK USAGE ---"
du -sh "$DB_ROOT/default/$OBJ"

$BIN stop 2>/dev/null
rm -f /tmp/shard-db_kv.json /tmp/shard-db_kv.csv /tmp/shard-db_kv_keys.txt

echo ""
echo "======================================"
echo "  K/V Benchmark complete"
echo "======================================"
