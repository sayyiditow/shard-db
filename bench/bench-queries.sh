#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# Query benchmark — tests find, count, aggregate across all ops and indexed/unindexed paths.
# Requires: users object with data (./create-user-object.sh + ./insert-users.sh N)
# Usage: ./bench-queries.sh

BIN="./shard-db"
SIZE=$($BIN query '{"mode":"size","dir":"default","object":"users"}' 2>/dev/null)

echo "======================================"
echo "  shard-db QUERY benchmark"
echo "  users: $SIZE"
echo "======================================"
echo ""

run() {
    local label="$1"
    shift
    local start=$(date +%s%N)
    local result=$($BIN query "$@" 2>/dev/null)
    local end=$(date +%s%N)
    local ms=$(( (end - start) / 1000000 ))
    local short=$(echo "$result" | head -c 100)
    if [ ${#result} -gt 100 ]; then short="$short..."; fi
    printf "  %-58s %6dms  %s\n" "$label" "$ms" "$short"
}

# ==================== COUNT ====================
echo "--- COUNT ---"
echo "  [no criteria]"
run "count all (metadata)" \
    '{"mode":"count","dir":"default","object":"users"}'

echo "  [indexed field: age]"
run "count eq (age=30)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"age","op":"eq","value":"30"}]}'
run "count neq (age!=30)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"age","op":"neq","value":"30"}]}'
run "count gt (age>50)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"age","op":"gt","value":"50"}]}'
run "count lt (age<25)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"age","op":"lt","value":"25"}]}'
run "count gte (age>=60)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"age","op":"gte","value":"60"}]}'
run "count lte (age<=20)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"age","op":"lte","value":"20"}]}'
run "count between (age 30-40)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"age","op":"between","value":"30","value2":"40"}]}'
run "count in (age in 20,30,40,50)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"age","op":"in","value":"20,30,40,50"}]}'

echo "  [indexed field: active]"
run "count eq (active=false)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"active","op":"eq","value":"false"}]}'
run "count neq (active!=false)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"active","op":"neq","value":"false"}]}'

echo "  [indexed field: username]"
run "count starts (username starts alice)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"username","op":"starts","value":"alice"}]}'
run "count contains (username contains baker)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"username","op":"contains","value":"baker"}]}'
run "count ends (username ends 99)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"username","op":"ends","value":"99"}]}'
run "count ncontains (username !contain baker)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"username","op":"ncontains","value":"baker"}]}'
run "count exists (username exists)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"username","op":"exists"}]}'

echo "  [non-indexed field: balance]"
run "count lt (balance<0) FULL SCAN" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"balance","op":"lt","value":"0"}]}'
run "count gt (score>90) FULL SCAN" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"score","op":"gt","value":"90"}]}'

echo "  [indexed + secondary]"
run "count (active=false AND balance<0)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"active","op":"eq","value":"false"},{"field":"balance","op":"lt","value":"0"}]}'
run "count (age>50 AND score>80)" \
    '{"mode":"count","dir":"default","object":"users","criteria":[{"field":"age","op":"gt","value":"50"},{"field":"score","op":"gt","value":"80"}]}'

echo ""

# ==================== FIND ====================
echo "--- FIND (limit 10) ---"

echo "  [indexed ops]"
run "find eq (active=false)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"active","op":"eq","value":"false"}],"limit":10,"fields":["username","age"]}'
run "find neq (active!=false)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"active","op":"neq","value":"false"}],"limit":10,"fields":["username","active"]}'
run "find gt (age>70)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"age","op":"gt","value":"70"}],"limit":10,"fields":["username","age"]}'
run "find lt (age<20)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"age","op":"lt","value":"20"}],"limit":10,"fields":["username","age"]}'
run "find between (age 25-35)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"age","op":"between","value":"25","value2":"35"}],"limit":10,"fields":["username","age"]}'
run "find in (age in 18,25,50,77)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"age","op":"in","value":"18,25,50,77"}],"limit":10,"fields":["username","age"]}'
run "find starts (username starts alice)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"username","op":"starts","value":"alice"}],"limit":10,"fields":["username"]}'
run "find contains (email contains yahoo)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"email","op":"contains","value":"yahoo"}],"limit":10,"fields":["username","email"]}'
run "find ends (email ends dev.io)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"email","op":"ends","value":"dev.io"}],"limit":10,"fields":["username","email"]}'
run "find exists (birthday exists)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"birthday","op":"exists"}],"limit":10,"fields":["username","birthday"]}'

echo "  [non-indexed]"
run "find gt (balance>40000) FULL SCAN" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"balance","op":"gt","value":"40000"}],"limit":10,"fields":["username","balance"]}'
run "find contains (bio contains DevOps) FULL SCAN" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"bio","op":"contains","value":"DevOps"}],"limit":10,"fields":["username","bio"]}'

echo "  [indexed + secondary]"
run "find (age>60 AND balance>30000)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"age","op":"gt","value":"60"},{"field":"balance","op":"gt","value":"30000"}],"limit":10,"fields":["username","age","balance"]}'
run "find (active=false AND score>95)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"active","op":"eq","value":"false"},{"field":"score","op":"gt","value":"95"}],"limit":10,"fields":["username","score"]}'

echo ""

# ==================== AGGREGATE ====================
echo "--- AGGREGATE ---"

echo "  [no criteria — full scan]"
run "agg count all" \
    '{"mode":"aggregate","dir":"default","object":"users","aggregates":[{"fn":"count","alias":"n"}]}'
run "agg sum/avg/min/max balance" \
    '{"mode":"aggregate","dir":"default","object":"users","aggregates":[{"fn":"count"},{"fn":"sum","field":"balance"},{"fn":"avg","field":"balance"},{"fn":"min","field":"balance"},{"fn":"max","field":"balance"}]}'
run "agg group by active" \
    '{"mode":"aggregate","dir":"default","object":"users","group_by":["active"],"aggregates":[{"fn":"count","alias":"n"},{"fn":"avg","field":"balance"}]}'
run "agg group by age top 5" \
    '{"mode":"aggregate","dir":"default","object":"users","group_by":["age"],"aggregates":[{"fn":"count","alias":"n"}],"order_by":"n","order":"desc","limit":5}'

echo "  [indexed criteria]"
run "agg where active=false" \
    '{"mode":"aggregate","dir":"default","object":"users","criteria":[{"field":"active","op":"eq","value":"false"}],"aggregates":[{"fn":"count","alias":"n"},{"fn":"avg","field":"age"},{"fn":"avg","field":"balance"}]}'
run "agg where age>50" \
    '{"mode":"aggregate","dir":"default","object":"users","criteria":[{"field":"age","op":"gt","value":"50"}],"aggregates":[{"fn":"count","alias":"n"},{"fn":"avg","field":"balance"}]}'
run "agg where age neq 30" \
    '{"mode":"aggregate","dir":"default","object":"users","criteria":[{"field":"age","op":"neq","value":"30"}],"aggregates":[{"fn":"count","alias":"n"}]}'
run "agg where username starts alice" \
    '{"mode":"aggregate","dir":"default","object":"users","criteria":[{"field":"username","op":"starts","value":"alice"}],"aggregates":[{"fn":"count","alias":"n"},{"fn":"avg","field":"age"}]}'
run "agg group by active where age>50" \
    '{"mode":"aggregate","dir":"default","object":"users","criteria":[{"field":"age","op":"gt","value":"50"}],"group_by":["active"],"aggregates":[{"fn":"count","alias":"n"},{"fn":"avg","field":"balance"}]}'

echo "  [non-indexed criteria]"
run "agg where balance<0 FULL SCAN" \
    '{"mode":"aggregate","dir":"default","object":"users","criteria":[{"field":"balance","op":"lt","value":"0"}],"aggregates":[{"fn":"count","alias":"n"},{"fn":"sum","field":"balance"}]}'

echo "  [indexed + secondary]"
run "agg where active=false AND balance<0" \
    '{"mode":"aggregate","dir":"default","object":"users","criteria":[{"field":"active","op":"eq","value":"false"},{"field":"balance","op":"lt","value":"0"}],"aggregates":[{"fn":"count","alias":"n"},{"fn":"avg","field":"balance"}]}'

echo "  [having]"
run "agg group by age having n>16000" \
    '{"mode":"aggregate","dir":"default","object":"users","group_by":["age"],"aggregates":[{"fn":"count","alias":"n"},{"fn":"avg","field":"balance","alias":"avg_bal"}],"having":[{"field":"n","op":"gt","value":"16000"}],"order_by":"avg_bal","order":"desc"}'

echo ""

# ==================== CURSOR (keyset pagination on find) ====================
echo "--- CURSOR (keyset pagination) ---"
echo "  [Page 1 — signal cursor mode with cursor:null]"
run "cursor ASC page 1 (age, limit 100)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[],"order_by":"age","order":"asc","limit":100,"cursor":null,"fields":["username","age"]}'
run "cursor DESC page 1 (age, limit 100)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[],"order_by":"age","order":"desc","limit":100,"cursor":null,"fields":["username","age"]}'
run "cursor ASC page 1 + criteria (active=false)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"active","op":"eq","value":"false"}],"order_by":"age","order":"asc","limit":100,"cursor":null,"fields":["username","age"]}'

echo "  [Continuation — hand back a cursor to a mid-range position]"
run "cursor ASC page N (age=50, continuation)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[],"order_by":"age","order":"asc","limit":100,"cursor":{"age":"50","key":"00000000000000000000000000000000"},"fields":["username","age"]}'
run "cursor DESC page N (age=30, continuation)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[],"order_by":"age","order":"desc","limit":100,"cursor":{"age":"30","key":"ffffffffffffffffffffffffffffffff"},"fields":["username","age"]}'

echo "  [Offset-based deep page for contrast — buffer-sort path]"
run "offset 50000 limit 100 order_by age (no cursor, full buffer-sort)" \
    '{"mode":"find","dir":"default","object":"users","criteria":[],"order_by":"age","order":"asc","offset":50000,"limit":100,"fields":["username","age"]}'

echo ""
echo "======================================"
echo "  Benchmark complete"
echo "======================================"
