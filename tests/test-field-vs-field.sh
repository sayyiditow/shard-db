#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-field-vs-field.sh — eq_field/neq_field/lt_field/gt_field/lte_field/
# gte_field. The criterion's value names a sibling field on the same
# record. Both sides must be the same typed type — mismatched types
# silently match no record (graceful, not an error).

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
count() { echo "$1" | tr -dc "0-9"; }

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
rm -rf "$DB_ROOT/default/fft"
sed -i "/^default:fft:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN start > /dev/null
sleep 0.5

# Mix of types so we can verify per-type comparison: int x int, varchar x
# varchar (CS), numeric x numeric. `name` is indexed but field-vs-field
# always full-scans (planner returns leaf_is_indexed=0 for these ops).
$BIN query '{"mode":"create-object","dir":"default","object":"fft","fields":["received_at:int","scheduled_at:int","name:varchar:32","alias:varchar:32","amount:numeric:10,2","budget:numeric:10,2","day_a:date","day_b:date"],"indexes":["name"]}' > /dev/null

echo "=== seed ==="
$BIN query '{"mode":"bulk-insert","dir":"default","object":"fft","records":[{"id":"k1","data":{"received_at":100,"scheduled_at":200,"name":"alice","alias":"alice","amount":"50.00","budget":"100.00","day_a":"20260101","day_b":"20260201"}},{"id":"k2","data":{"received_at":300,"scheduled_at":200,"name":"bob","alias":"BOB","amount":"100.00","budget":"100.00","day_a":"20260301","day_b":"20260301"}},{"id":"k3","data":{"received_at":150,"scheduled_at":150,"name":"carol","alias":"carol","amount":"75.00","budget":"50.00","day_a":"20260601","day_b":"20260101"}}]}' > /dev/null

echo "=== int eq_field / neq_field / lt_field / gt_field ==="
assert_eq "received_at eq_field scheduled_at → 1 (k3 150=150)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"received_at","op":"eq_field","value":"scheduled_at"}]}')")"
assert_eq "received_at neq_field scheduled_at → 2 (k1, k2)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"received_at","op":"neq_field","value":"scheduled_at"}]}')")"
assert_eq "received_at lt_field scheduled_at → 1 (k1 100<200)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"received_at","op":"lt_field","value":"scheduled_at"}]}')")"
assert_eq "received_at gt_field scheduled_at → 1 (k2 300>200)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"received_at","op":"gt_field","value":"scheduled_at"}]}')")"
assert_eq "received_at lte_field scheduled_at → 2 (k1, k3)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"received_at","op":"lte_field","value":"scheduled_at"}]}')")"
assert_eq "received_at gte_field scheduled_at → 2 (k2, k3)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"received_at","op":"gte_field","value":"scheduled_at"}]}')")"

echo "=== varchar eq_field is byte-exact (CS) ==="
# k1: alice/alice → match
# k2: bob/BOB → mismatch (CS)
# k3: carol/carol → match
assert_eq "name eq_field alias (CS) → 2 (k1, k3)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"name","op":"eq_field","value":"alias"}]}')")"
assert_eq "name neq_field alias → 1 (k2)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"name","op":"neq_field","value":"alias"}]}')")"
out=$($BIN query '{"mode":"find","dir":"default","object":"fft","criteria":[{"field":"name","op":"neq_field","value":"alias"}],"fields":["name","alias"]}')
assert_contains "find name neq_field alias returns k2" '"key":"k2"' "$out"

echo "=== varchar lex compare via lt_field/gt_field ==="
# k1: alice vs alice → eq → not lt
# k2: bob vs BOB  → 'b'(0x62) > 'B'(0x42), so name > alias; lt_field=false, gt_field=true
# k3: carol vs carol → eq
assert_eq "name lt_field alias → 0 (no name lex < alias)" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"name","op":"lt_field","value":"alias"}]}')")"
assert_eq "name gt_field alias → 1 (k2 bob > BOB lex)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"name","op":"gt_field","value":"alias"}]}')")"

echo "=== numeric (decimal fixed-point) field-vs-field ==="
# amount vs budget per row:
# k1: 50  vs 100 → lt
# k2: 100 vs 100 → eq
# k3: 75  vs 50  → gt
assert_eq "amount eq_field budget → 1 (k2)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"amount","op":"eq_field","value":"budget"}]}')")"
assert_eq "amount lt_field budget → 1 (k1)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"amount","op":"lt_field","value":"budget"}]}')")"
assert_eq "amount gt_field budget → 1 (k3)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"amount","op":"gt_field","value":"budget"}]}')")"
assert_eq "amount gte_field budget → 2 (k2, k3)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"amount","op":"gte_field","value":"budget"}]}')")"

echo "=== date field-vs-field ==="
# day_a vs day_b:
# k1: 20260101 vs 20260201 → lt
# k2: 20260301 vs 20260301 → eq
# k3: 20260601 vs 20260101 → gt
assert_eq "day_a eq_field day_b → 1 (k2)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"day_a","op":"eq_field","value":"day_b"}]}')")"
assert_eq "day_a lt_field day_b → 1 (k1)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"day_a","op":"lt_field","value":"day_b"}]}')")"

echo "=== type mismatch silently matches nothing ==="
# varchar vs int — different types, no match.
assert_eq "name eq_field received_at → 0 (varchar vs int)" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"name","op":"eq_field","value":"received_at"}]}')")"

echo "=== unknown RHS field ==="
assert_eq "name eq_field totally_missing → 0" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"name","op":"eq_field","value":"missing_field"}]}')")"

echo "=== combined with regular ops via AND ==="
# received_at lt_field scheduled_at AND received_at gt 50 → k1 only (100<200, 100>50)
assert_eq "lt_field AND gt 50 → 1 (k1)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"fft","criteria":[{"field":"received_at","op":"lt_field","value":"scheduled_at"},{"field":"received_at","op":"gt","value":"50"}]}')")"

echo "=== aggregate respects field-vs-field ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"fft","aggregates":[{"fn":"count","alias":"n"},{"fn":"sum","field":"received_at","alias":"s"}],"criteria":[{"field":"received_at","op":"lte_field","value":"scheduled_at"}]}')
assert_contains "agg count where lte_field" '"n":2' "$out"
# k1=100, k3=150 → sum=250
assert_contains "agg sum where lte_field = 250" '"s":250' "$out"

echo "=== bulk-update with field-vs-field criteria ==="
$BIN query '{"mode":"bulk-update","dir":"default","object":"fft","criteria":[{"field":"amount","op":"gt_field","value":"budget"}],"value":{"name":"OVERSPEND"}}' > /dev/null
out=$($BIN query '{"mode":"get","dir":"default","object":"fft","key":"k3"}')
assert_contains "k3 name updated to OVERSPEND (amount>budget)" '"name":"OVERSPEND"' "$out"
out=$($BIN query '{"mode":"get","dir":"default","object":"fft","key":"k1"}')
assert_contains "k1 name unchanged" '"name":"alice"' "$out"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"fft"}' > /dev/null 2>&1 || true
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
