#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-case-sensitivity.sh — case-sensitive defaults for like/contains/
# starts/ends, plus the new case-insensitive variants ilike/icontains/
# istarts/iends. Verifies both the typed-binary fast path (indexed and
# non-indexed varchar) and that not_* variants invert correctly.

set -e

BIN="./shard-db"
PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo "  FAIL: $1"; }

assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then pass "$desc"
    else fail "$desc: expected='$expected' actual='$actual'"; fi
}
count() { echo "$1" | tr -dc "0-9"; }

run_count() {
    # $1 = field, $2 = op, $3 = value
    local q="{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"cit\",\"criteria\":[{\"field\":\"$1\",\"op\":\"$2\",\"value\":\"$3\"}]}"
    count "$($BIN query "$q")"
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
rm -rf "$DB_ROOT/default/cit"
sed -i "/^default:cit:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN start > /dev/null
sleep 0.5

# `name` is indexed (drives btree-walk path). `tag` is non-indexed
# (drives full-scan typed match). Both should agree on results.
$BIN query '{"mode":"create-object","dir":"default","object":"cit","fields":["name:varchar:32","tag:varchar:32"],"indexes":["name"],"splits":16}' > /dev/null

echo "=== seed (mixed case) ==="
$BIN query '{"mode":"bulk-insert","dir":"default","object":"cit","records":[{"id":"k1","data":{"name":"Alice","tag":"VIP"}},{"id":"k2","data":{"name":"alice","tag":"vip"}},{"id":"k3","data":{"name":"BOB","tag":"vip"}},{"id":"k4","data":{"name":"bob","tag":"VIP"}},{"id":"k5","data":{"name":"Carol","tag":"std"}}]}' > /dev/null

echo "=== CS contains: byte-exact (was CI before) ==="
assert_eq "contains 'bob' (CS) → 1 (only bob)" "1" "$(run_count name contains bob)"
assert_eq "contains 'BOB' (CS) → 1 (only BOB)" "1" "$(run_count name contains BOB)"
assert_eq "contains 'Bob' (CS) → 0 (no exact match)" "0" "$(run_count name contains Bob)"

echo "=== CI icontains: case-folded ==="
assert_eq "icontains 'bob' (CI) → 2 (BOB + bob)" "2" "$(run_count name icontains bob)"
assert_eq "icontains 'BOB' (CI) → 2" "2" "$(run_count name icontains BOB)"
assert_eq "icontains 'Bob' (CI) → 2" "2" "$(run_count name icontains Bob)"

echo "=== CS starts_with: byte-exact (was CI for matcher, CS for index — now both CS) ==="
assert_eq "starts 'ALI' (CS) → 0" "0" "$(run_count name starts ALI)"
assert_eq "starts 'Ali' (CS) → 1 (Alice)" "1" "$(run_count name starts Ali)"
assert_eq "starts 'ali' (CS) → 1 (alice)" "1" "$(run_count name starts ali)"

echo "=== CI istarts_with ==="
assert_eq "istarts 'ALI' (CI) → 2 (Alice + alice)" "2" "$(run_count name istarts ALI)"
assert_eq "istarts 'b' (CI) → 2 (BOB + bob)" "2" "$(run_count name istarts b)"

echo "=== CS ends_with ==="
assert_eq "ends 'OB' (CS) → 1 (BOB only)" "1" "$(run_count name ends OB)"
assert_eq "ends 'ob' (CS) → 1 (bob only)" "1" "$(run_count name ends ob)"
assert_eq "ends 'Ob' (CS) → 0" "0" "$(run_count name ends Ob)"

echo "=== CI iends_with ==="
assert_eq "iends 'OB' (CI) → 2 (BOB + bob)" "2" "$(run_count name iends OB)"
assert_eq "iends 'ce' (CI) → 2 (Alice + alice)" "2" "$(run_count name iends ce)"

echo "=== CS like ==="
assert_eq "like 'B%' (CS) → 1 (BOB)" "1" "$(run_count name like 'B%')"
assert_eq "like 'A%' (CS) → 1 (Alice; alice starts lowercase)" "1" "$(run_count name like 'A%')"
assert_eq "like 'a%' (CS) → 1 (alice)" "1" "$(run_count name like 'a%')"
assert_eq "like '%E' (CS) → 0 (Alice/alice both end lowercase 'e')" "0" "$(run_count name like '%E')"
assert_eq "like '%e' (CS) → 2 (Alice + alice)" "2" "$(run_count name like '%e')"
# Edge: like with no % → exact byte match
assert_eq "like 'BOB' no-% → 1 (BOB exact)" "1" "$(run_count name like BOB)"
assert_eq "like 'bob' no-% → 1 (bob exact)" "1" "$(run_count name like bob)"
assert_eq "like 'Bob' no-% → 0" "0" "$(run_count name like Bob)"

echo "=== CI ilike ==="
assert_eq "ilike 'B%' (CI) → 2 (BOB + bob)" "2" "$(run_count name ilike 'B%')"
assert_eq "ilike '%LICE' (CI) → 2 (Alice + alice)" "2" "$(run_count name ilike '%LICE')"
assert_eq "ilike 'BoB' no-% (CI) → 2" "2" "$(run_count name ilike BoB)"
assert_eq "ilike '%c%' (CI) → 3 (Alice, alice, Carol)" "3" "$(run_count name ilike '%c%')"

echo "=== negative variants ==="
assert_eq "not_contains 'bob' (CS) → 4 (everything except bob)" "4" "$(run_count name not_contains bob)"
assert_eq "not_icontains 'bob' (CI) → 3 (Alice, alice, Carol)" "3" "$(run_count name not_icontains bob)"
assert_eq "not_like 'B%' (CS) → 4 (everything except BOB)" "4" "$(run_count name not_like 'B%')"
assert_eq "not_ilike 'B%' (CI) → 3" "3" "$(run_count name not_ilike 'B%')"

echo "=== non-indexed field (tag) — same answers ==="
assert_eq "tag contains 'vip' (CS) → 2 (k2,k3)" "2" "$(run_count tag contains vip)"
assert_eq "tag icontains 'vip' (CI) → 4 (all VIP/vip)" "4" "$(run_count tag icontains vip)"
assert_eq "tag starts 'V' (CS) → 2 (VIP rows)" "2" "$(run_count tag starts V)"
assert_eq "tag istarts 'v' (CI) → 4" "4" "$(run_count tag istarts v)"

echo "=== find returns the right rows (indexed CS path) ==="
out=$($BIN query '{"mode":"find","dir":"default","object":"cit","criteria":[{"field":"name","op":"contains","value":"bob"}],"fields":["name"]}')
[[ "$out" == *'"name":"bob"'* ]] && pass "find contains 'bob' returns bob" || fail "expected bob: $out"
[[ "$out" == *'"name":"BOB"'* ]] && fail "find contains 'bob' must NOT return BOB: $out" || pass "find contains 'bob' rejects BOB"

echo "=== find with ilike returns both (indexed CI path) ==="
out=$($BIN query '{"mode":"find","dir":"default","object":"cit","criteria":[{"field":"name","op":"ilike","value":"%ob%"}],"fields":["name"]}')
[[ "$out" == *'"name":"bob"'* ]] && pass "find ilike returns bob" || fail "expected bob: $out"
[[ "$out" == *'"name":"BOB"'* ]] && pass "find ilike returns BOB" || fail "expected BOB: $out"

echo "=== aggregate with new ops ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"cit","aggregates":[{"fn":"count","alias":"n"}],"criteria":[{"field":"name","op":"icontains","value":"a"}]}')
# Alice, alice, Carol all contain 'a' or 'A' → 3
[[ "$out" == *'"n":3'* ]] && pass "agg count icontains 'a' = 3" || fail "agg count: $out"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"cit"}' > /dev/null 2>&1 || true
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
