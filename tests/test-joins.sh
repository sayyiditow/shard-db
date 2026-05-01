#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# Tests for join support on find mode.
set -e
BIN="./shard-db"
PASS=0
FAIL=0

pass() { PASS=$((PASS+1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL+1)); echo "  FAIL: $1"; }
assert_contains() { if [[ "$3" == *"$2"* ]]; then pass "$1"; else fail "$1: '$2' not in: $3"; fi }
assert_not_contains() { if [[ "$3" != *"$2"* ]]; then pass "$1"; else fail "$1: '$2' unexpectedly in: $3"; fi }

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.5
$BIN start >/dev/null
sleep 1

# Clean + create
$BIN query '{"mode":"create-object","dir":"default","object":"j_cust","splits":16,"max_key":16,"fields":["name:varchar:32","city:varchar:32"]}' >/dev/null
$BIN query '{"mode":"create-object","dir":"default","object":"j_orders","splits":16,"max_key":16,"fields":["amount:numeric:10,2","status:varchar:20","cust_id:varchar:16","ref_code:varchar:32"],"indexes":["status","cust_id","ref_code"]}' >/dev/null

# Add customers (primary keys = their IDs)
$BIN query '{"mode":"insert","dir":"default","object":"j_cust","key":"c1","value":{"name":"Alice","city":"NYC"}}' >/dev/null
$BIN query '{"mode":"insert","dir":"default","object":"j_cust","key":"c2","value":{"name":"Bob","city":"LA"}}' >/dev/null

# Add orders
$BIN query '{"mode":"insert","dir":"default","object":"j_orders","key":"o1","value":{"amount":100,"status":"paid","cust_id":"c1","ref_code":"ABC"}}' >/dev/null
$BIN query '{"mode":"insert","dir":"default","object":"j_orders","key":"o2","value":{"amount":250,"status":"paid","cust_id":"c1","ref_code":"DEF"}}' >/dev/null
$BIN query '{"mode":"insert","dir":"default","object":"j_orders","key":"o3","value":{"amount":75,"status":"paid","cust_id":"c2","ref_code":"ABC"}}' >/dev/null
$BIN query '{"mode":"insert","dir":"default","object":"j_orders","key":"o4","value":{"amount":40,"status":"pending","cust_id":"MISSING","ref_code":"XYZ"}}' >/dev/null

echo ""
echo "=== INNER JOIN on primary key ==="
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"j_cust","local":"cust_id","remote":"key","as":"cust","fields":["name","city"]}]}')
assert_contains "inner: columns include j_orders.key"    '"j_orders.key"' "$GOT"
assert_contains "inner: columns include cust.name"       '"cust.name"'   "$GOT"
assert_contains "inner: o1 present (has match)"          '"o1"'          "$GOT"
assert_contains "inner: Alice joined"                    '"Alice"'       "$GOT"
assert_contains "inner: o3 present"                      '"o3"'          "$GOT"
assert_contains "inner: Bob joined"                      '"Bob"'         "$GOT"
assert_not_contains "inner: o4 not in output (pending filtered out)" '"o4"' "$GOT"

echo ""
echo "=== LEFT JOIN: MISSING cust_id emits nulls, o4 survives ==="
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","join":[{"object":"j_cust","local":"cust_id","remote":"key","as":"cust","type":"left","fields":["name"]}]}')
assert_contains "left: o4 still appears"                 '"o4"'          "$GOT"
assert_contains "left: null for MISSING match"           'null'          "$GOT"
assert_contains "left: Alice still joined"               '"Alice"'       "$GOT"

echo ""
echo "=== JOIN on INDEXED field (cust_id -> j_orders.ref_code) ==="
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","criteria":[{"field":"ref_code","op":"eq","value":"ABC"}],"join":[{"object":"j_orders","local":"cust_id","remote":"cust_id","as":"sibling","fields":["ref_code"]}]}')
# self-join by cust_id
assert_contains "indexed self-join: executes"            '"sibling.ref_code"' "$GOT"

echo ""
echo "=== ERROR: remote field not indexed ==="
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"j_cust","local":"cust_id","remote":"name","as":"c","fields":["city"]}]}')
assert_contains "remote field not indexed error"         "must be 'key' or indexed" "$GOT"

echo ""
echo "=== ERROR: as collides with driver ==="
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"j_cust","local":"cust_id","remote":"key","as":"j_orders","fields":["name"]}]}')
assert_contains "as collision error"                     'collides with driver' "$GOT"

echo ""
echo "=== ERROR: duplicate as ==="
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"j_cust","local":"cust_id","remote":"key","as":"c","fields":["name"]},{"object":"j_cust","local":"cust_id","remote":"key","as":"c","fields":["city"]}]}')
assert_contains "duplicate as error"                     'duplicate join' "$GOT"

echo ""
echo "=== MULTIPLE joins (inner + left) ==="
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"j_cust","local":"cust_id","remote":"key","as":"cust","fields":["name"]},{"object":"j_orders","local":"cust_id","remote":"cust_id","as":"related","type":"left","fields":["ref_code"]}]}')
assert_contains "multi-join: cust.name column"          '"cust.name"'       "$GOT"
assert_contains "multi-join: related.ref_code column"   '"related.ref_code"' "$GOT"

echo ""
echo "=== format:csv on joins (new in 2026.05.1) ==="
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"j_cust","local":"cust_id","remote":"key","as":"cust","fields":["name","city"]}],"format":"csv"}')
# Header line
HEADER=$(echo "$GOT" | head -1)
assert_contains "csv header has driver key column"   'j_orders.key'  "$HEADER"
assert_contains "csv header has cust.name column"    'cust.name'     "$HEADER"
assert_contains "csv header has cust.city column"    'cust.city'     "$HEADER"
# Data rows
ROWS=$(echo "$GOT" | tail -n +2 | wc -l)
[[ "$ROWS" -ge 2 ]] && pass "csv: at least 2 data rows" || fail "csv: expected >=2 rows, got $ROWS"
echo "$GOT" | tail -n +2 | grep -q 'Alice' && pass "csv: Alice cell present" || fail "csv: missing Alice"
echo "$GOT" | tail -n +2 | grep -q 'Bob'   && pass "csv: Bob cell present"   || fail "csv: missing Bob"

# Custom delimiter
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"j_cust","local":"cust_id","remote":"key","as":"cust","fields":["name"]}],"format":"csv","delimiter":"|"}')
HEADER=$(echo "$GOT" | head -1)
assert_contains "pipe delimiter respected"           'j_orders.key|'  "$HEADER"

# Left join CSV — missing match → empty cell, not "null"
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","join":[{"object":"j_cust","local":"cust_id","remote":"key","as":"cust","type":"left","fields":["name"]}],"format":"csv"}')
echo "$GOT" | grep -q '^o4,' && pass "csv left-join: o4 row present" || fail "csv left-join: o4 missing in $GOT"
# o4's cust_id has no match — its row should end with `,` (empty cust.name)
ROW_O4=$(echo "$GOT" | grep '^o4,' | head -1)
[[ "$ROW_O4" == *',' ]] && pass "csv left-join: empty trailing cell for missing match" || fail "csv left-join: expected trailing comma in $ROW_O4"

echo ""
echo "=== LIMIT with inner join drops applied ==="
GOT=$($BIN query '{"mode":"find","dir":"default","object":"j_orders","criteria":[{"field":"status","op":"eq","value":"paid"}],"join":[{"object":"j_cust","local":"cust_id","remote":"key","as":"cust","fields":["name"]}],"limit":2}')
# Count rows in the output — should be at most 2
ROWS=$(echo "$GOT" | grep -o 'paid' | wc -l)
if [[ $ROWS -le 2 ]]; then pass "limit=2 respected (got $ROWS rows)"; else fail "limit=2 violated (got $ROWS rows)"; fi

echo ""
echo "=== CLEANUP ==="
$BIN stop >/dev/null
sleep 0.3
rm -rf build/db/default/j_cust build/db/default/j_orders
grep -v "default:j_cust\|default:j_orders" build/db/schema.conf > /tmp/sc.new 2>/dev/null && mv /tmp/sc.new build/db/schema.conf 2>/dev/null

echo ""
echo "=== RESULT: $PASS passed, $FAIL failed ==="
if [[ $FAIL -gt 0 ]]; then exit 1; fi
