#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-describe.sh — list-objects + describe-object (used by shard-cli to
# populate menus and criteria builders without reading on-disk config files).

set -e

BIN="./shard-db"
PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo "  FAIL: $1"; }

assert_contains() {
    local desc="$1" needle="$2" hay="$3"
    if [[ "$hay" == *"$needle"* ]]; then pass "$desc"
    else fail "$desc: expected '$needle' in: $hay"; fi
}
assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then pass "$desc"
    else fail "$desc: expected='$expected' actual='$actual'"; fi
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3

DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

# Wipe any prior fixture objects with deterministic names.
for o in dsc_users dsc_orders dsc_empty; do
    rm -rf "$DB_ROOT/default/$o"
    sed -i "/^default:$o:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
done

$BIN start > /dev/null
sleep 0.5

# Create three objects with varying shapes.
$BIN query '{"mode":"create-object","dir":"default","object":"dsc_users","splits":8,"max_key":40,"fields":["name:varchar:64","age:int","email:varchar:80","active:bool"],"indexes":["age","email"]}' > /dev/null
$BIN query '{"mode":"create-object","dir":"default","object":"dsc_orders","splits":4,"max_key":32,"fields":["amount:numeric:10,2","total:long","placed:datetime","sku:varchar:24"],"indexes":["sku","amount","sku+placed"]}' > /dev/null
$BIN query '{"mode":"create-object","dir":"default","object":"dsc_empty","splits":2,"max_key":16,"fields":["k:varchar:8"]}' > /dev/null

# Populate dsc_users with 3 records to verify record_count.
$BIN insert default dsc_users k1 '{"name":"alice","age":30,"email":"a@x","active":true}' > /dev/null
$BIN insert default dsc_users k2 '{"name":"bob","age":40,"email":"b@x","active":false}' > /dev/null
$BIN insert default dsc_users k3 '{"name":"carol","age":50,"email":"c@x","active":true}' > /dev/null

echo "=== list-objects ==="
out=$($BIN query '{"mode":"list-objects","dir":"default"}')
assert_contains "list-objects includes dsc_users" '"dsc_users"' "$out"
assert_contains "list-objects includes dsc_orders" '"dsc_orders"' "$out"
assert_contains "list-objects includes dsc_empty" '"dsc_empty"' "$out"
assert_contains "list-objects has dir field" '"dir":"default"' "$out"
assert_contains "list-objects wraps in objects array" '"objects":' "$out"

echo "=== list-objects negative ==="
out=$($BIN query '{"mode":"list-objects"}' 2>&1)
assert_contains "list-objects without dir → error" 'dir is required' "$out"

out=$($BIN query '{"mode":"list-objects","dir":"unknown_tenant"}' 2>&1)
assert_contains "list-objects unknown tenant → error" 'unknown dir' "$out"

echo "=== describe-object: basic shape ==="
out=$($BIN query '{"mode":"describe-object","dir":"default","object":"dsc_users"}')
assert_contains "describe has dir" '"dir":"default"' "$out"
assert_contains "describe has object name" '"object":"dsc_users"' "$out"
assert_contains "describe has splits=8" '"splits":8' "$out"
assert_contains "describe has max_key=40" '"max_key":40' "$out"
assert_contains "describe has slot_size" '"slot_size":' "$out"
assert_contains "describe has record_count=3" '"record_count":3' "$out"

echo "=== describe-object: fields ==="
assert_contains "field name (varchar)" '"name":"name","type":"varchar"' "$out"
assert_contains "field age (int)" '"name":"age","type":"int"' "$out"
assert_contains "field email (varchar)" '"name":"email","type":"varchar"' "$out"
assert_contains "field active (bool)" '"name":"active","type":"bool"' "$out"

echo "=== describe-object: indexes ==="
assert_contains "indexes contain age" '"age"' "$out"
assert_contains "indexes contain email" '"email"' "$out"

echo "=== describe-object: numeric scale + composite index ==="
out=$($BIN query '{"mode":"describe-object","dir":"default","object":"dsc_orders"}')
assert_contains "numeric type emitted" '"type":"numeric"' "$out"
assert_contains "numeric scale emitted" '"scale":2' "$out"
assert_contains "composite index emitted" '"sku+placed"' "$out"
assert_contains "datetime type emitted" '"type":"datetime"' "$out"

echo "=== describe-object: empty object ==="
out=$($BIN query '{"mode":"describe-object","dir":"default","object":"dsc_empty"}')
assert_contains "empty object record_count=0" '"record_count":0' "$out"
assert_contains "empty object indexes=[]" '"indexes":[]' "$out"

echo "=== describe-object: not found ==="
out=$($BIN query '{"mode":"describe-object","dir":"default","object":"does_not_exist"}' 2>&1)
assert_contains "describe missing → error" 'not found' "$out"

echo
echo "=== TEARDOWN ==="
for o in dsc_users dsc_orders dsc_empty; do
    $BIN query "{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"$o\"}" > /dev/null 2>&1 || true
done
$BIN stop > /dev/null
sleep 0.3

if [ "$FAIL" -eq 0 ]; then
    echo "================================"
    echo "  $PASS passed, 0 failed ($TOTAL total)"
    echo "================================"
    exit 0
else
    echo "================================"
    echo "  $PASS passed, $FAIL failed ($TOTAL total)"
    echo "================================"
    exit 1
fi
