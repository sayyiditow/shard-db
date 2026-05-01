#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-bulk-upsert.sh — bulk-insert acts as a true upsert: overwriting an
# existing key updates the slot data AND drops the stale index entry for
# the old field value. Without the drop, idx_count_cb over-counts and
# index files grow per overwrite.

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
count() {
    echo "$1" | tr -dc "0-9"
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
rm -rf "$DB_ROOT/default/upsert_t"
sed -i "/^default:upsert_t:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN start > /dev/null
sleep 0.5

$BIN query '{"mode":"create-object","dir":"default","object":"upsert_t","fields":["status:varchar:16","amount:int","note:varchar:32"],"indexes":["status","amount"],"splits":16}' > /dev/null

echo "=== seed: bulk-insert 3 records ==="
$BIN query '{"mode":"bulk-insert","dir":"default","object":"upsert_t","records":[{"id":"k1","data":{"status":"paid","amount":100,"note":"v"}},{"id":"k2","data":{"status":"paid","amount":200,"note":"v"}},{"id":"k3","data":{"status":"pending","amount":50,"note":"v"}}]}' > /dev/null
assert_eq "count(status=paid)=2 after seed" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"status","op":"eq","value":"paid"}]}')")"
assert_eq "count(status=pending)=1 after seed" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"status","op":"eq","value":"pending"}]}')")"

echo "=== bulk-insert overwrite changes indexed field — old idx entry must be dropped ==="
$BIN query '{"mode":"bulk-insert","dir":"default","object":"upsert_t","records":[{"id":"k1","data":{"status":"refunded","amount":100,"note":"v"}}]}' > /dev/null
assert_eq "count(status=paid) drops 2->1 after k1 paid->refunded" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"status","op":"eq","value":"paid"}]}')")"
assert_eq "count(status=refunded) is 1" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"status","op":"eq","value":"refunded"}]}')")"

echo "=== indexed find returns new value, not stale ==="
out=$($BIN query '{"mode":"find","dir":"default","object":"upsert_t","criteria":[{"field":"status","op":"eq","value":"paid"}],"fields":["status"]}')
assert_contains "find(status=paid) returns k2" '"key":"k2"' "$out"
[[ "$out" == *'"key":"k1"'* ]] && fail "find(status=paid) MUST NOT return k1 (stale)" || pass "find(status=paid) does not return stale k1"

echo "=== bulk-insert overwrite to SAME indexed value is a no-op (must not duplicate) ==="
# k2 stays paid+200. Re-insert with same payload. Counts must be unchanged.
$BIN query '{"mode":"bulk-insert","dir":"default","object":"upsert_t","records":[{"id":"k2","data":{"status":"paid","amount":200,"note":"v"}}]}' > /dev/null
assert_eq "count(status=paid)=1 after no-op rewrite" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"status","op":"eq","value":"paid"}]}')")"
assert_eq "count(amount=200)=1 after no-op rewrite" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"amount","op":"eq","value":"200"}]}')")"

echo "=== bulk-insert overwrite of multi-indexed record — both indexes drift correctly ==="
# Move k2 from (paid, 200) to (cancelled, 999). Both indexed fields change.
$BIN query '{"mode":"bulk-insert","dir":"default","object":"upsert_t","records":[{"id":"k2","data":{"status":"cancelled","amount":999,"note":"v"}}]}' > /dev/null
assert_eq "count(status=paid)=0 after k2 moves" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"status","op":"eq","value":"paid"}]}')")"
assert_eq "count(amount=200)=0 after k2 moves" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"amount","op":"eq","value":"200"}]}')")"
assert_eq "count(status=cancelled)=1 after k2 moves" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"status","op":"eq","value":"cancelled"}]}')")"
assert_eq "count(amount=999)=1 after k2 moves" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"amount","op":"eq","value":"999"}]}')")"

echo "=== bulk-insert + if_not_exists still skips the existing record ==="
out=$($BIN query '{"mode":"bulk-insert","dir":"default","object":"upsert_t","if_not_exists":true,"records":[{"id":"k1","data":{"status":"paid","amount":1,"note":"v"}}]}')
assert_contains "if_not_exists returns skipped:1" '"skipped":1' "$out"
assert_eq "k1 still has status=refunded after CAS skip" "refunded" \
    "$($BIN query '{"mode":"get","dir":"default","object":"upsert_t","key":"k1"}' | grep -oE '"status":"[^"]*"' | head -1 | sed 's/.*":"\(.*\)"/\1/')"

echo "=== AND-intersection (paid AND amount<200) sees no stale rows ==="
# Bulk-insert had over-count bug ⇒ AND intersection over PRIMARY_INTERSECT path
# would return ghost candidates whose record is post-filter rejected, but the
# count-shortcut for pure intersect skips the post-filter. Verify count is right.
$BIN query '{"mode":"bulk-insert","dir":"default","object":"upsert_t","records":[{"id":"k4","data":{"status":"paid","amount":75,"note":"v"}}]}' > /dev/null
$BIN query '{"mode":"bulk-insert","dir":"default","object":"upsert_t","records":[{"id":"k4","data":{"status":"refunded","amount":75,"note":"v"}}]}' > /dev/null
# k4 is now refunded+75. count(paid AND amount<100) should not include k4.
assert_eq "AND-intersect count(paid AND amount<100)=0 (no stale k4)" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"upsert_t","criteria":[{"field":"status","op":"eq","value":"paid"},{"field":"amount","op":"lt","value":"100"}]}')")"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"upsert_t"}' > /dev/null 2>&1 || true
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
