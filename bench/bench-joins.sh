#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# Join benchmark — requires users (1M records) already inserted.
# Creates orders object referencing users by key, then measures join throughput.
# Usage: ./bench-joins.sh [orders_count]    (default 500000)

BIN="./shard-db"
COUNT=${1:-500000}

echo "======================================"
echo "  shard-db JOIN benchmark"
USERS_CNT=$($BIN query '{"mode":"size","dir":"default","object":"users"}' 2>/dev/null)
echo "  orders: $COUNT, users: $USERS_CNT"
echo "======================================"
echo ""

run() {
    local label="$1"
    shift
    local start=$(date +%s%N)
    local result=$($BIN query "$@" 2>/dev/null)
    local end=$(date +%s%N)
    local ms=$(( (end - start) / 1000000 ))
    local short=$(echo "$result" | head -c 140)
    if [ ${#result} -gt 140 ]; then short="$short..."; fi
    printf "  %-58s %6dms  %s\n" "$label" "$ms" "$short"
}

# ==================== SETUP ====================

EXIST=$($BIN query '{"mode":"size","dir":"default","object":"orders"}' 2>/dev/null)
if [[ "$EXIST" != *"\"count\":$COUNT"* ]]; then
    echo "--- setup: create+populate orders ($COUNT records) ---"
    # Drop existing orders if size mismatch
    $BIN query '{"mode":"truncate","dir":"default","object":"orders"}' >/dev/null 2>&1

    $BIN query '{"mode":"create-object","dir":"default","object":"orders","splits":32,"max_key":32,"fields":["order_num:long","amount:numeric:12,2","status:varchar:16","user_id:varchar:32","product_sku:varchar:12","created_at:datetime"],"indexes":["status","user_id","product_sku"]}' >/dev/null 2>&1

    echo "  generating orders JSON..."
    python3 -c "
import json, hashlib, random
random.seed(42)
count = $COUNT
statuses = ['paid','pending','shipped','cancelled']
skus = [f'SKU-{i:04d}' for i in range(100)]
records = []
for i in range(count):
    if i % 20 == 0:
        user_id = 'MISSING_USER_ID_X'   # ~5% will left-join to null
    else:
        user_i = i % 1000000
        user_id = hashlib.sha256(f'user-{user_i}'.encode()).hexdigest()[:32]
    order_key = hashlib.sha256(f'order-{i}'.encode()).hexdigest()[:32]
    amount = round(random.uniform(5, 5000), 2)
    status = statuses[i % 4]
    sku = skus[i % len(skus)]
    year = 2023 + (i % 3)
    month = (i % 12) + 1
    day = (i % 28) + 1
    hour = i % 24
    minute = i % 60
    sec = (i * 13) % 60
    created_at = f'{year}{month:02d}{day:02d}{hour:02d}{minute:02d}{sec:02d}'
    records.append({'id': order_key, 'data': {
        'order_num': i + 1,
        'amount': str(amount),
        'status': status,
        'user_id': user_id,
        'product_sku': sku,
        'created_at': created_at,
    }})
with open('/tmp/shard-db_orders.json','w') as f:
    json.dump(records, f)
"
    echo "  inserting..."
    time $BIN query '{"mode":"bulk-insert","dir":"default","object":"orders","file":"/tmp/shard-db_orders.json"}' > /dev/null
    echo ""
fi

$BIN query '{"mode":"size","dir":"default","object":"orders"}'
echo ""

# ==================== BASELINE (no join) ====================

echo "--- BASELINE (no join) ---"
run "count orders (total)"                  '{"mode":"count","dir":"default","object":"orders"}'
run "count status=paid (indexed)"           '{"mode":"count","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}]}'
run "find paid limit 10 (indexed)"          '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"limit":10}'
run "find paid limit 100"                   '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"limit":100}'
run "find paid limit 1000"                  '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"limit":1000}'
run "find amount>4000 FULL SCAN"            '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"amount","op":"gt","value":"4000"}],"limit":10}'
echo ""

# ==================== INNER JOIN by KEY ====================

echo "--- INNER join orders → users by KEY (hash lookup) ---"
run "inner limit=10 (paid)"                 '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","fields":["username","email"]}],"limit":10}'
run "inner limit=100"                       '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","fields":["username","email"]}],"limit":100}'
run "inner limit=1000"                      '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","fields":["username","email"]}],"limit":1000}'
run "inner limit=10000"                     '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","fields":["username","email"]}],"limit":10000}'
run "inner FULL (no limit)"                 '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","fields":["username","email"]}]}'
echo ""

# ==================== LEFT JOIN by KEY ====================

echo "--- LEFT join orders → users by KEY ---"
run "left limit=10"                         '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","type":"left","fields":["username","email"]}],"limit":10}'
run "left limit=1000"                       '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","type":"left","fields":["username","email"]}],"limit":1000}'
run "left limit=10000"                      '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","type":"left","fields":["username","email"]}],"limit":10000}'
echo ""

# ==================== INDEXED-FIELD JOIN ====================

echo "--- INNER join on INDEXED field (users.username) ---"
run "idx-join limit=10"                     '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"username","as":"u","fields":["email"]}],"limit":10}'
run "idx-join limit=100"                    '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"username","as":"u","fields":["email"]}],"limit":100}'
echo ""

# ==================== MULTI-JOIN ====================

echo "--- MULTI-JOIN (two keyed joins) ---"
run "double-inner limit=100"                '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","fields":["username"]},{"object":"users","local":"user_id","remote":"key","as":"u2","fields":["email"]}],"limit":100}'
run "double-inner limit=1000"               '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","fields":["username"]},{"object":"users","local":"user_id","remote":"key","as":"u2","fields":["email"]}],"limit":1000}'
echo ""

# ==================== JOIN + PROJECTION ====================

echo "--- JOIN + driver projection (fewer columns) ---"
run "inner w/projection limit=1000"         '{"mode":"find","dir":"default","object":"orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"fields":["order_num","amount"],"join":[{"object":"users","local":"user_id","remote":"key","as":"u","fields":["username"]}],"limit":1000}'
echo ""

echo "======================================"
echo "  Benchmark complete"
echo "======================================"
