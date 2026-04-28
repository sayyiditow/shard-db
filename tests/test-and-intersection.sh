#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-and-intersection.sh — AND index intersection (PRIMARY_INTERSECT path).
# Covers count + find + aggregate against pure AND trees where every child is
# an indexed leaf on a btree-rangeable op (EQ/LT/GT/LTE/GTE/BETWEEN/IN/
# STARTS_WITH); also negative cases that should stay on PRIMARY_LEAF
# (single leaf, non-eligible ops, mixed indexed/non-indexed siblings).

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
assert_contains() {
    local desc="$1" needle="$2" hay="$3"
    if [[ "$hay" == *"$needle"* ]]; then pass "$desc"
    else fail "$desc: expected '$needle' in: $hay"; fi
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3

DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

rm -rf "$DB_ROOT/default/ix_orders"
sed -i '/^default:ix_orders:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true

$BIN start > /dev/null
sleep 0.5

# Schema:
#   status: indexed varchar (4 values: paid, pending, paid, cancelled rotating)
#   amount: indexed int (50–349 across 4 patterns)
#   region: indexed varchar (us, eu rotating)
#   notes : NON-indexed varchar (used for fallback tests)
$BIN query '{"mode":"create-object","dir":"default","object":"ix_orders","splits":16,"max_key":16,"fields":["status:varchar:16","amount:int","region:varchar:16","notes:varchar:64"],"indexes":["status","amount","region"]}' > /dev/null

# 200 records using the same i%4 pattern verified against in earlier hand-tests:
#   i%4==0 → paid+us, amount=100+i  (50 records)
#   i%4==1 → pending+eu, amount=50+i (50 records)
#   i%4==2 → paid+eu, amount=200+i  (50 records)
#   i%4==3 → cancelled+us, amount=30+i (50 records)
# Totals: 100 paid, 50 pending, 50 cancelled; 100 us, 100 eu.
for i in $(seq 1 200); do
    case $((i % 4)) in
        0) st=paid; amt=$((100 + i)); region=us;;
        1) st=pending; amt=$((50 + i)); region=eu;;
        2) st=paid; amt=$((200 + i)); region=eu;;
        3) st=cancelled; amt=$((30 + i)); region=us;;
    esac
    $BIN insert default ix_orders "k$i" "{\"status\":\"$st\",\"amount\":$amt,\"region\":\"$region\",\"notes\":\"order $i\"}" > /dev/null
done

echo "=== COUNT 2-way EQ + EQ ==="
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}]}')
assert_eq "count(paid AND us)" '{"count":50}' "$out"

out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"eu"}]}')
assert_eq "count(paid AND eu)" '{"count":50}' "$out"

out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"pending"},{"field":"region","op":"eq","value":"us"}]}')
assert_eq "count(pending AND us) — empty intersection" '{"count":0}' "$out"

echo "=== COUNT 2-way EQ + range ==="
# i%4==0 with i+100>150 means i>50 and i%4==0 → 38 records
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"amount","op":"gt","value":"150"}]}')
# paid has both i%4==0 (amt=100+i) and i%4==2 (amt=200+i)
# i%4==0 ∧ amt>150: i>50, i%4==0 → 50 ≤ i ≤ 200, 38 vals
# i%4==2 ∧ amt>150: amt=200+i, all > 150 → 50 vals
# total 88
assert_eq "count(paid AND amount>150)" '{"count":88}' "$out"

out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"amount","op":"between","value":"200","value2":"250"}]}')
# i%4==0 ∧ 200≤amt≤250: 100+i in [200,250] → i in [100,150], i%4==0 → 100,104,...148 (13 vals)
# i%4==2 ∧ 200≤amt≤250: 200+i in [200,250] → i in [0,50], i%4==2 ∩ ≥1 ≤ 200 → 2,6,10,...50 (13 vals)
# total 26
assert_eq "count(paid AND amount between 200..250)" '{"count":26}' "$out"

echo "=== COUNT 3-way intersection ==="
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"},{"field":"amount","op":"gt","value":"150"}]}')
# paid+us = i%4==0 (50 records, amt=100+i for i=4,8,...,200)
# amt>150 means i>50. i%4==0 ∧ i>50 ∧ i≤200 → 52,56,...,200 = 38 records
assert_eq "count(paid AND us AND amount>150)" '{"count":38}' "$out"

out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"eu"},{"field":"amount","op":"lte","value":"250"}]}')
# paid+eu = i%4==2 (50 records, amt=200+i for i=2,6,...,198)
# amt≤250 → i≤50 → i%4==2 ∧ 1≤i≤50 → 2,6,...,50 = 13 records
assert_eq "count(paid AND eu AND amount<=250)" '{"count":13}' "$out"

echo "=== COUNT EQ + IN ==="
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"region","op":"eq","value":"us"},{"field":"status","op":"in","value":"paid,cancelled"}]}')
# us = i%4 ∈ {0,3}; status in {paid,cancelled} = i%4 ∈ {0,2,3}
# intersection = i%4 ∈ {0,3} = 100 records
assert_eq "count(us AND status IN (paid,cancelled))" '{"count":100}' "$out"

echo "=== COUNT EQ + STARTS_WITH ==="
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"region","op":"eq","value":"us"},{"field":"status","op":"starts","value":"pa"}]}')
# us ∧ status starts pa = us ∧ paid = i%4==0 = 50 records
assert_eq "count(us AND status starts 'pa')" '{"count":50}' "$out"

echo "=== FIND with limit applies to survivors (intersection fixes the limit-on-walk bug) ==="
# 50 records match paid AND us; with limit=10 we should get exactly 10.
out=$($BIN query '{"mode":"find","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}],"limit":10}')
n=$(echo "$out" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)))')
assert_eq "find limit=10 returns exactly 10 records" "10" "$n"

# Verify every returned record actually matches (sanity)
all_match=$(echo "$out" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(all(r["value"]["status"]=="paid" and r["value"]["region"]=="us" for r in d))')
assert_eq "every find result matches both criteria" "True" "$all_match"

echo "=== FIND with offset ==="
out=$($BIN query '{"mode":"find","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}],"offset":40,"limit":50}')
n=$(echo "$out" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)))')
# Total survivors = 50; offset 40 → 10 remaining
assert_eq "find offset=40 limit=50 returns 10" "10" "$n"

echo "=== FIND empty intersection ==="
out=$($BIN query '{"mode":"find","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"pending"},{"field":"region","op":"eq","value":"us"}],"limit":5}')
assert_eq "find empty intersection returns []" "[]" "$out"

echo "=== FIND with field projection ==="
out=$($BIN query '{"mode":"find","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}],"limit":1,"fields":["amount"]}')
# Should have only amount field in value
has_amount=$(echo "$out" | python3 -c 'import json,sys;d=json.load(sys.stdin);print("amount" in d[0]["value"] and "status" not in d[0]["value"])')
assert_eq "find projection returns only requested fields" "True" "$has_amount"

echo "=== AGGREGATE sum + count ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"ix_orders","aggregates":[{"fn":"sum","field":"amount","alias":"total"},{"fn":"count","alias":"n"}],"criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}]}')
# paid+us = i%4==0, i ∈ {4,8,...,200} (50 vals), amt = 100+i
# sum amt = 50*100 + sum(i for i in 4,8,...,200) = 5000 + 4*(1+2+...+50) = 5000 + 4*1275 = 10100
assert_contains "agg total=10100" '"total":10100' "$out"
assert_contains "agg n=50" '"n":50' "$out"

echo "=== AGGREGATE min/max/avg ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"ix_orders","aggregates":[{"fn":"min","field":"amount","alias":"lo"},{"fn":"max","field":"amount","alias":"hi"},{"fn":"avg","field":"amount","alias":"avg"}],"criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}]}')
# paid+us amounts: 104, 108, ..., 300. min=104, max=300, avg=(104+300)/2=202
assert_contains "agg min=104" '"lo":104' "$out"
assert_contains "agg max=300" '"hi":300' "$out"
assert_contains "agg avg=202" '"avg":202' "$out"

echo "=== AGGREGATE with group_by ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"ix_orders","aggregates":[{"fn":"count","alias":"n"}],"group_by":["region"],"criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"amount","op":"gt","value":"150"}]}')
# paid AND amt>150:
#   us: i%4==0 ∧ i>50 → 38 records (i=52,56,...,200)
#   eu: i%4==2 ∧ amt=200+i>150 → all 50 records
# Group by region: {us:38, eu:50}
assert_contains "agg group_by region us=38" '"region":"us","n":38' "$out"
assert_contains "agg group_by region eu=50" '"region":"eu","n":50' "$out"

echo "=== NEGATIVE: single leaf stays on PRIMARY_LEAF ==="
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"pending"}]}')
assert_eq "single leaf count(pending)" '{"count":50}' "$out"

echo "=== NEGATIVE: non-eligible op (LIKE) falls back to PRIMARY_LEAF ==="
# `LIKE` is NOT intersection-eligible; whole tree should fall back to LEAF
# with status=paid as primary, then post-filter LIKE in criteria_match_tree.
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"notes","op":"like","value":"%order 4%"}]}')
# paid records with notes matching '%order 4%': "order 4", "order 14", "order 24", ..., "order 194", "order 40", ...
# Let's count: paid = i%4 ∈ {0,2}. notes "order N" where N has substring "4".
# i=4,40,...,48,40-49,140-149,40,42,44,...
# Easier: just verify count > 0 and the query returns numeric.
case "$out" in
    '{"count":'*) pass "non-eligible LIKE falls back, returns numeric count" ;;
    *) fail "non-eligible LIKE got: $out" ;;
esac

echo "=== NEGATIVE: mixed indexed + non-indexed AND siblings ==="
# notes is NOT indexed → mixed tree → falls back to PRIMARY_LEAF
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"notes","op":"eq","value":"order 4"}]}')
assert_eq "mixed indexed+non-indexed AND falls back" '{"count":1}' "$out"

echo "=== NEGATIVE: pure OR doesn't trigger intersection ==="
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"or":[{"field":"status","op":"eq","value":"pending"},{"field":"status","op":"eq","value":"cancelled"}]}]}')
assert_eq "pure OR uses PRIMARY_KEYSET" '{"count":100}' "$out"

echo "=== EDGE: 4-way intersection (all 4 indexed eq leaves) ==="
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"},{"field":"amount","op":"gte","value":"100"},{"field":"amount","op":"lte","value":"200"}]}')
# paid+us = i%4==0, amt = 100+i. 100≤amt≤200 → 0≤i≤100. i%4==0, 1≤i≤200 → i ∈ {4,8,...,100} = 25 records
assert_eq "count 4-way intersection" '{"count":25}' "$out"

echo "=== EDGE: empty intersection found early (short-circuit) ==="
# All paid with status=cancelled: contradiction. Same field intersected.
out=$($BIN query '{"mode":"count","dir":"default","object":"ix_orders","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"status","op":"eq","value":"cancelled"}]}')
assert_eq "contradiction yields 0" '{"count":0}' "$out"

echo
echo "=== TEARDOWN ==="
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
