#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-find-cursor.sh — keyset pagination (transparent JSON cursor)
#
# Covers:
# - Page 1 shapes: cursor:null, cursor:{} — both wrap the response and emit initial cursor
# - Page N with cursor from previous page — continues from correct position
# - Last page emits cursor:null
# - ASC and DESC both work
# - Tie-breaking on equal order_by values (multiple records share same value)
# - Cursor pointing to deleted key still seeks correctly (standard keyset semantics)
# - Indexed-only requirement: reject cursor on non-indexed field
# - Malformed cursor rejection (missing key, wrong order_by field)
# - No cursor in request → unwrapped array (backward compat)
# - Cursor with criteria filter — works through secondary filters

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
assert_not_contains() {
    local desc="$1" needle="$2" hay="$3"
    if [[ "$hay" != *"$needle"* ]]; then pass "$desc"
    else fail "$desc: '$needle' unexpectedly in: $hay"; fi
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3

DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

for obj in curs_int curs_tie curs_crit; do
    rm -rf "$DB_ROOT/default/$obj"
    sed -i "/^default:$obj:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
done

$BIN start > /dev/null
sleep 0.5

# --- 1. Basic ASC/DESC pagination ---

$BIN query '{"mode":"create-object","dir":"default","object":"curs_int","splits":2,"max_key":16,"fields":["n:int","tag:varchar:16"],"indexes":["n"]}' > /dev/null
for i in 1 2 3 4 5 6 7 8 9 10; do
    $BIN insert default curs_int "k$i" "{\"n\":$((i*10)),\"tag\":\"paid\"}" > /dev/null
done

echo "=== PAGE 1 SHAPES ==="
R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"n","order":"asc","limit":3,"cursor":null}')
assert_contains "cursor:null → wrapped response"           '"rows":'           "$R"
assert_contains "cursor:null → emits k1"                   '"key":"k1"'        "$R"
assert_contains "cursor:null → emits initial cursor"       '"cursor":{'        "$R"
assert_contains "cursor:null → cursor value at n=30"       '"n":"30"'          "$R"
assert_contains "cursor:null → cursor.key is k3"           '"key":"k3"'        "$R"

R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"n","order":"asc","limit":3,"cursor":{}}')
assert_contains "cursor:{} also works as page 1"           '"rows":'           "$R"
assert_contains "cursor:{} emits k1 at top"                '"key":"k1"'        "$R"

echo "=== PAGE 2 CONTINUES FROM CURSOR ==="
R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"n","order":"asc","limit":3,"cursor":{"n":"30","key":"k3"}}')
assert_contains "page 2 starts at k4"                      '"key":"k4"'        "$R"
assert_contains "page 2 includes k5"                       '"key":"k5"'        "$R"
assert_contains "page 2 includes k6"                       '"key":"k6"'        "$R"
assert_not_contains "page 2 does NOT include k3 (excl)"    '"key":"k3"'        "$R"
assert_contains "page 2 emits next cursor at k6"           '"key":"k6"'        "$R"

echo "=== LAST PAGE EMITS cursor:null ==="
R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"n","order":"asc","limit":5,"cursor":{"n":"60","key":"k6"}}')
assert_contains "last page includes k7..k10"               '"key":"k10"'       "$R"
assert_contains "last page has cursor:null"                '"cursor":null'     "$R"

echo "=== DESC PAGINATION ==="
R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"n","order":"desc","limit":3,"cursor":null}')
assert_contains "desc page 1 starts at k10"                '"key":"k10"'       "$R"
assert_contains "desc page 1 ends at k8 (cursor.key)"      '"key":"k8"'        "$R"
assert_contains "desc page 1 cursor value n=80"            '"n":"80"'          "$R"

R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"n","order":"desc","limit":3,"cursor":{"n":"80","key":"k8"}}')
assert_contains "desc page 2 starts at k7"                 '"key":"k7"'        "$R"
assert_not_contains "desc page 2 does NOT include k8"      '"key":"k8"'        "$R"
assert_contains "desc page 2 includes k6"                  '"key":"k6"'        "$R"

echo "=== NO CURSOR → UNWRAPPED ARRAY (backward compat) ==="
R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"n","order":"asc","limit":3}')
assert_not_contains "no cursor → no rows wrapper"          '"rows":'           "$R"
assert_not_contains "no cursor → no cursor field"          '"cursor":'         "$R"
assert_contains "no cursor → still returns array"          '"key":"k1"'        "$R"

echo "=== ERROR CASES ==="
R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"tag","limit":3,"cursor":{"tag":"paid","key":"k1"}}')
assert_contains "reject non-indexed order_by"              'field to be indexed' "$R"

R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"n","limit":3,"cursor":{"n":"30"}}')
assert_contains "reject cursor missing key"                "missing 'key'"     "$R"

R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"limit":3,"cursor":{"n":"30","key":"k3"}}')
assert_contains "reject cursor without order_by"           "cursor requires order_by" "$R"

R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"n","limit":3,"cursor":{"key":"k3"}}')
assert_contains "reject cursor missing value for order_by" "missing order_by field value" "$R"

# --- 2. Tie-breaking on equal order_by values ---

$BIN query '{"mode":"create-object","dir":"default","object":"curs_tie","splits":2,"max_key":16,"fields":["grp:int"],"indexes":["grp"]}' > /dev/null
# Insert 10 records all with grp=5 (should still paginate deterministically)
for i in 1 2 3 4 5 6 7 8 9 10; do
    $BIN insert default curs_tie "t$i" "{\"grp\":5}" > /dev/null
done

echo "=== TIE-BREAK ON EQUAL ORDER_BY ==="
R=$($BIN query '{"mode":"find","dir":"default","object":"curs_tie","criteria":[],"order_by":"grp","limit":3,"cursor":null}')
# Should return 3 records from the 10, all with grp=5. Cursor must let us continue.
COUNT_P1=$(echo "$R" | grep -oE '"key":"t[0-9]+","value"' | wc -l)
if [ "$COUNT_P1" -eq 3 ]; then pass "tie-break page 1 returns 3 rows"
else fail "tie-break page 1 row count: got $COUNT_P1"; fi
assert_contains "tie-break page 1 has cursor"              '"cursor":{'        "$R"

# Extract cursor's key from the response to paginate.
CUR_KEY=$(echo "$R" | grep -oE '"cursor":\{[^}]*"key":"t[0-9]+"' | grep -oE 't[0-9]+' | tail -1)
if [ -n "$CUR_KEY" ]; then
    R=$($BIN query "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_tie\",\"criteria\":[],\"order_by\":\"grp\",\"limit\":3,\"cursor\":{\"grp\":\"5\",\"key\":\"$CUR_KEY\"}}")
    COUNT_P2=$(echo "$R" | grep -oE '"key":"t[0-9]+","value"' | wc -l)
    if [ "$COUNT_P2" -eq 3 ]; then pass "tie-break page 2 returns 3 rows (different from p1)"
    else fail "tie-break page 2 row count: got $COUNT_P2"; fi
    # Ensure no overlap with page 1 (simple check: the cursor key from p1 should NOT appear on p2)
    if ! echo "$R" | grep -q "\"key\":\"$CUR_KEY\""; then
        pass "tie-break page 2 does not include p1's last key ($CUR_KEY)"
    else
        fail "tie-break page 2 wrongly includes $CUR_KEY"
    fi
fi

# --- 3. Cursor with criteria filter ---

$BIN query '{"mode":"create-object","dir":"default","object":"curs_crit","splits":2,"max_key":16,"fields":["n:int","flag:bool"],"indexes":["n"]}' > /dev/null
for i in 1 2 3 4 5 6 7 8 9 10; do
    FLAG=$(( i % 2 == 0 ? 1 : 0 ))
    FLAG_S=$([ "$FLAG" = "1" ] && echo "true" || echo "false")
    $BIN insert default curs_crit "c$i" "{\"n\":$((i*10)),\"flag\":$FLAG_S}" > /dev/null
done

echo "=== CURSOR + CRITERIA FILTER ==="
# flag=true records are c2, c4, c6, c8, c10. Cursor-paginate them.
R=$($BIN query '{"mode":"find","dir":"default","object":"curs_crit","criteria":[{"field":"flag","op":"eq","value":"true"}],"order_by":"n","order":"asc","limit":2,"cursor":null}')
assert_contains "cursor+filter page 1 has c2"              '"key":"c2"'        "$R"
assert_contains "cursor+filter page 1 has c4"              '"key":"c4"'        "$R"
assert_not_contains "cursor+filter page 1 does NOT have c1" '"key":"c1"'       "$R"
assert_not_contains "cursor+filter page 1 does NOT have c3" '"key":"c3"'       "$R"
assert_contains "cursor+filter page 1 emits cursor"        '"cursor":{'        "$R"

# Page 2 continues from cursor (n=40, key=c4)
R=$($BIN query '{"mode":"find","dir":"default","object":"curs_crit","criteria":[{"field":"flag","op":"eq","value":"true"}],"order_by":"n","order":"asc","limit":2,"cursor":{"n":"40","key":"c4"}}')
assert_contains "cursor+filter page 2 has c6"              '"key":"c6"'        "$R"
assert_contains "cursor+filter page 2 has c8"              '"key":"c8"'        "$R"
assert_not_contains "cursor+filter page 2 does NOT have c4" '"key":"c4"'       "$R"

# --- 4. Cursor through deletes ---

echo "=== CURSOR AFTER DELETE OF CURSOR-REFERENCED KEY ==="
# Delete k5 (at n=50). A cursor pointing to (n=50, key=k5) should still seek
# correctly to the byte position and emit records after it.
$BIN delete default curs_int k5 > /dev/null
R=$($BIN query '{"mode":"find","dir":"default","object":"curs_int","criteria":[],"order_by":"n","order":"asc","limit":5,"cursor":{"n":"50","key":"k5"}}')
assert_contains "cursor past deleted k5 yields k6"         '"key":"k6"'        "$R"
assert_not_contains "cursor past deleted k5 does NOT emit k5" '"key":"k5"'    "$R"

echo ""
echo "=== CLEANUP ==="
$BIN stop > /dev/null 2>&1 || true
for obj in curs_int curs_tie curs_crit; do
    rm -rf "$DB_ROOT/default/$obj"
    sed -i "/^default:$obj:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
done

echo
echo "================================"
echo "  $PASS passed, $FAIL failed ($TOTAL total)"
echo "================================"
exit $FAIL
