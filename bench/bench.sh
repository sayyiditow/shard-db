#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# Usage: ./bench.sh [record_count] [cli|persistent]

COUNT=${1:-100000}
MODE=${2:-persistent}
BIN="./shard-db"
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
PORT=$(grep PORT db.env | sed "s/.*[\"']\{0,1\}\([0-9]*\).*/\1/")
PORT=${PORT:-9199}

echo "======================================"
echo "  shard-db benchmark ($COUNT records)"
echo "  mode: $MODE"
echo "======================================"

# Query helpers
q_cli() { $BIN query "$1"; }

# Setup
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

$BIN stop 2>/dev/null || true
sleep 0.3
$BIN start
sleep 0.5

# Create object with fields.conf (columnar storage)
q_cli '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
rm -rf "$DB_ROOT/default/bench"
sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
q_cli '{"mode":"create-object","dir":"default","object":"bench","splits":256,"max_key":64,"fields":["fullName:varchar:20","email:varchar:30","location:varchar:10","score:int"],"indexes":[]}' > /dev/null

# q uses persistent socat for individual queries, q_cli for long ops
# Pipelined throughput tests use their own socat connections
if [ "$MODE" = "persistent" ]; then
    q() {
        echo "$1" | socat - TCP:localhost:$PORT 2>/dev/null | tr -d '\0'
    }
else
    q() { $BIN query "$1"; }
fi
cleanup_conn() { :; }

q '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1

# Generate test data
echo "Generating $COUNT records..."
python3 -c "
import json
records = [{'id': f'key_{i}', 'data': {
    'fullName': f'User_{i}',
    'email': f'user{i}@test.com',
    'location': ['London','Paris','Berlin','Tokyo','NYC'][i%5],
    'score': str(i * 100)
}} for i in range($COUNT)]
with open('/tmp/shard-db_bench.json', 'w') as f:
    json.dump(records, f)
"

# ==================== BULK INSERT ====================
echo ""
echo "--- BULK INSERT ($COUNT records, no index) ---"
time q_cli "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_bench.json\"}" > /dev/null
q_cli '{"mode":"size","dir":"default","object":"bench"}'

# ==================== GET x1000 (pipelined via socat) ====================
echo ""
echo "--- GET x1000 (pipelined, single connection) ---"
time (
    for i in $(shuf -i 0-$((COUNT-1)) -n 1000); do
        echo "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"bench\",\"key\":\"key_$i\"}"
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== EXISTS x1000 ====================
echo ""
echo "--- EXISTS x1000 (pipelined) ---"
time (
    for i in $(shuf -i 0-$((COUNT-1)) -n 1000); do
        echo "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"bench\",\"key\":\"key_$i\"}"
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== ADD INDEX ====================
echo ""
echo "--- ADD INDEX (fullName) ---"
time q_cli '{"mode":"add-index","dir":"default","object":"bench","field":"fullName"}' > /dev/null

echo ""
echo "--- ADD INDEX (location) ---"
time q_cli '{"mode":"add-index","dir":"default","object":"bench","field":"location"}' > /dev/null

# ==================== INDEXED SEARCH x100 (pipelined) ====================
echo ""
echo "--- INDEXED SEARCH x100 (pipelined) ---"
time (
    for i in $(shuf -i 0-$((COUNT-1)) -n 100); do
        echo "{\"mode\":\"search\",\"dir\":\"default\",\"object\":\"bench\",\"field\":\"fullName\",\"value\":\"User_$i\"}"
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== BULK INSERT WITH INDEXES ====================
echo ""
echo "--- BULK INSERT ($COUNT records, 2 indexes) ---"
q_cli '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
printf "fullName\nlocation\n" > "$DB_ROOT/default/bench/indexes/index.conf"
time q_cli "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_bench.json\"}" > /dev/null
q_cli '{"mode":"size","dir":"default","object":"bench"}'

# ==================== FIND ====================
echo ""
echo "--- FIND: eq (indexed) ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"fullName","op":"eq","value":"User_50000"}],"limit":1}' > /dev/null

echo ""
echo "--- FIND: contains (indexed leaf scan) ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"fullName","op":"contains","value":"User_999"}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: IN indexed (5 values) ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"fullName","op":"in","value":["User_100","User_200","User_300","User_400","User_500"]}],"limit":5}' > /dev/null

echo ""
echo "--- FIND: indexed eq + non-indexed filter ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"fullName","op":"eq","value":"User_50000"},{"field":"location","op":"eq","value":"London"}],"limit":1}' > /dev/null

echo ""
echo "--- FIND: indexed starts (prefix) ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"fullName","op":"starts","value":"User_5000"}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: indexed location=London (200K), limit=10 ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"location","op":"eq","value":"London"}],"limit":10}' > /dev/null

# ==================== COMPOSITE INDEX ====================
echo ""
echo "--- ADD COMPOSITE INDEX (location+score) ---"
time q_cli '{"mode":"add-index","dir":"default","object":"bench","field":"location+score"}' > /dev/null

echo ""
echo "--- FIND: composite starts (London) ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"location+score","op":"starts","value":"London"}],"limit":10}' > /dev/null

# ==================== RANGE ====================
echo ""
echo "--- RANGE: indexed fullName ---"
time q '{"mode":"range","dir":"default","object":"bench","field":"fullName","min":"User_10000","max":"User_10010"}' > /dev/null

# ==================== FETCH ====================
echo ""
echo "--- FETCH: page of 100, offset 5000 ---"
time q '{"mode":"fetch","dir":"default","object":"bench","offset":"5000","limit":"100"}' > /dev/null

echo ""
echo "--- FETCH: with projection ---"
time q '{"mode":"fetch","dir":"default","object":"bench","offset":"0","limit":"100","fields":["fullName","email"]}' > /dev/null

# ==================== KEYS ====================
echo ""
echo "--- KEYS: first 100 ---"
time q '{"mode":"keys","dir":"default","object":"bench","offset":"0","limit":"100"}' > /dev/null

# ==================== SINGLE DELETE x1000 (pipelined) ====================
echo ""
echo "--- SINGLE DELETE x1000 (pipelined, 3 indexes) ---"
time (
    for i in $(shuf -i 0-$((COUNT-1)) -n 1000); do
        echo "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"bench\",\"key\":\"key_$i\"}"
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== BULK DELETE x1000 ====================
echo ""
echo "--- BULK DELETE x1000 (inline JSON, 3 indexes) ---"
BULK_DEL_KEYS=""
for i in $(shuf -i 0-$((COUNT-1)) -n 1000); do
    BULK_DEL_KEYS="${BULK_DEL_KEYS}\"key_$i\","
done
BULK_DEL_KEYS="[${BULK_DEL_KEYS%,}]"
time q_cli "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"bench\",\"keys\":$BULK_DEL_KEYS}" > /dev/null

# ==================== VACUUM ====================
echo ""
echo "--- VACUUM ---"
time q_cli '{"mode":"vacuum","dir":"default","object":"bench"}' > /dev/null

# ==================== RECOUNT ====================
echo ""
echo "--- RECOUNT ---"
time q_cli '{"mode":"recount","dir":"default","object":"bench"}' > /dev/null

# ==================== DISK USAGE ====================
echo ""
echo "--- DISK USAGE ---"
du -sh "$DB_ROOT/default/bench/"

# ==================== CLEANUP ====================
q '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
cleanup_conn
$BIN stop
rm -f /tmp/shard-db_bench.json

echo ""
echo "======================================"
echo "  Benchmark complete"
echo "======================================"
