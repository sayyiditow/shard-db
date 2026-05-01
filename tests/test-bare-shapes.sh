#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-bare-shapes.sh — verifies the 2026.05.1 read-mode response shape changes:
#   - get (single)        → bare value dict
#   - get-multi           → {key:value,...} dict with null for missing keys
#   - exists (single)     → bare true/false
#   - count               → bare number
#   - size                → bare number (live count, O(1) metadata)
#   - orphaned            → bare number (deleted/tombstoned, O(1) metadata)

set -e

BIN="./shard-db"
PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1)); echo "  FAIL: $1"; }

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

rm -rf "$DB_ROOT/default/shape_t"
sed -i '/^default:shape_t:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true

$BIN start > /dev/null
sleep 0.5

$BIN query '{"mode":"create-object","dir":"default","object":"shape_t","fields":["name:varchar:32","age:int"]}' > /dev/null

# ===== Seed =====
$BIN insert default shape_t k1 '{"name":"alice","age":30}' > /dev/null
$BIN insert default shape_t k2 '{"name":"bob","age":25}' > /dev/null
$BIN insert default shape_t k3 '{"name":"carol","age":40}' > /dev/null

echo ""
echo "=== get (single) → bare value dict ==="
got=$($BIN get default shape_t k1)
assert_eq "get k1 bare value" '{"name":"alice","age":30}' "$got"

echo ""
echo "=== get-multi → dict shape ==="
got=$($BIN query '{"mode":"get","dir":"default","object":"shape_t","keys":["k1","k2"]}')
# Both present
assert_eq "two-key dict shape" '{"k1":{"name":"alice","age":30},"k2":{"name":"bob","age":25}}' "$got"

# Missing key → null
got=$($BIN query '{"mode":"get","dir":"default","object":"shape_t","keys":["k1","missing"]}')
assert_eq "missing key emits null" '{"k1":{"name":"alice","age":30},"missing":null}' "$got"

# Empty input → {}
got=$($BIN query '{"mode":"get","dir":"default","object":"shape_t","keys":[]}')
assert_eq "empty keys array → {}" '{}' "$got"

echo ""
echo "=== exists (single) → bare bool ==="
got=$($BIN exists default shape_t k1)
assert_eq "exists present → true" 'true' "$got"

got=$($BIN exists default shape_t nothere)
assert_eq "exists absent → false" 'false' "$got" || true  # exit code is 1 on absent

echo ""
echo "=== count → bare number ==="
got=$($BIN count default shape_t)
assert_eq "count no-criteria" '3' "$got"

got=$($BIN query '{"mode":"count","dir":"default","object":"shape_t","criteria":[{"field":"age","op":"gte","value":"30"}]}')
assert_eq "count with criteria" '2' "$got"

echo ""
echo "=== size → bare live count ==="
got=$($BIN size default shape_t)
assert_eq "size = 3 live" '3' "$got"

echo ""
echo "=== orphaned → bare deleted count ==="
$BIN delete default shape_t k2 > /dev/null

got=$($BIN orphaned default shape_t)
assert_eq "orphaned = 1 after delete" '1' "$got"

got=$($BIN size default shape_t)
assert_eq "size = 2 (live drops on delete)" '2' "$got"

# Re-run via JSON dispatch to confirm both paths.
got=$($BIN query '{"mode":"orphaned","dir":"default","object":"shape_t"}')
assert_eq "orphaned via JSON mode" '1' "$got"

echo ""
echo "=== fetch format:dict ==="
got=$($BIN query '{"mode":"fetch","dir":"default","object":"shape_t","format":"dict","limit":10}')
# fetch always wraps with {"results":...,"cursor":...} envelope
echo "$got" | grep -q '"results":{' && pass "fetch dict: results is dict" || fail "fetch dict envelope: $got"
echo "$got" | grep -q '"cursor":' && pass "fetch dict: cursor present" || fail "fetch dict cursor: $got"

echo ""
echo "=== find format:dict (full-scan, no criteria) ==="
got=$($BIN query '{"mode":"find","dir":"default","object":"shape_t","criteria":[],"format":"dict","limit":10}')
# find returns bare {} dict when no cursor active
[ "${got:0:1}" = "{" ] || fail "find dict header: $got"
[ "${got: -1}" = "$(printf '}')" ] && pass "find dict close" || fail "find dict close: $got"
# k1 was deleted earlier; k3 should be present
echo "$got" | grep -q '"k3":' && pass "find dict contains k3" || fail "find dict k3 missing: $got"

echo ""
echo "=== find format:dict + order_by allowed ==="
got=$($BIN query '{"mode":"find","dir":"default","object":"shape_t","criteria":[],"format":"dict","order_by":"age","limit":10}')
[ "${got:0:1}" = "{" ] && pass "ordered dict opens with {" || fail "ordered dict: $got"

echo ""
echo "=== find format:dict + indexed criteria → REJECTED ==="
# shape_t doesn't have indexes; use an indexed object to trigger PRIMARY_LEAF
$BIN query '{"mode":"create-object","dir":"default","object":"shape_idx","fields":["status:varchar:16","amount:int"],"indexes":["status"]}' > /dev/null
$BIN insert default shape_idx ki1 '{"status":"paid","amount":100}' > /dev/null
got=$($BIN query '{"mode":"find","dir":"default","object":"shape_idx","criteria":[{"field":"status","op":"eq","value":"paid"}],"format":"dict"}')
echo "$got" | grep -q 'not yet supported' && pass "dict + indexed → clear error" || fail "dict + indexed should error: $got"
$BIN query '{"mode":"drop-object","dir":"default","object":"shape_idx"}' > /dev/null

echo ""
echo "=== find format:dict + join → REJECTED ==="
got=$($BIN query '{"mode":"find","dir":"default","object":"shape_t","criteria":[],"format":"dict","join":[{"object":"x","local":"name","remote":"key","as":"y"}]}' 2>&1)
echo "$got" | grep -q 'dict.*not supported with join' && pass "dict + join → error" || fail "dict + join: $got"

echo ""
echo "=== TEARDOWN ==="
$BIN stop > /dev/null
rm -rf "$DB_ROOT/default/shape_t"
sed -i '/^default:shape_t:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true

echo ""
echo "================================"
echo "  $PASS passed, $FAIL failed ($TOTAL total)"
echo "================================"
[ $FAIL -eq 0 ]
