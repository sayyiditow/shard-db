#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-or-logic.sh — coverage for OR criteria across find/count/aggregate/bulk
# Exercises Shapes A (AND only), B (AND+OR w/ indexed AND sibling), C (pure OR
# all-indexed → KeySet path), D (OR with non-indexed child → full scan), and
# the hybrid case (non-indexed AND + fully-indexed OR).

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

rm -rf "$DB_ROOT/default/or_orders"
sed -i '/^default:or_orders:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true

$BIN start > /dev/null
sleep 0.5

# status + region are indexed; amount + notes are NOT indexed.
$BIN query '{"mode":"create-object","dir":"default","object":"or_orders","splits":16,"max_key":16,"fields":["status:varchar:16","amount:int","region:varchar:16","notes:varchar:64"],"indexes":["status","region"]}' > /dev/null

$BIN insert default or_orders o1 '{"status":"paid","amount":100,"region":"us","notes":"vip"}'          > /dev/null
$BIN insert default or_orders o2 '{"status":"paid","amount":50,"region":"eu","notes":"standard"}'      > /dev/null
$BIN insert default or_orders o3 '{"status":"pending","amount":200,"region":"us","notes":"vip"}'       > /dev/null
$BIN insert default or_orders o4 '{"status":"cancelled","amount":0,"region":"asia","notes":"refunded"}' > /dev/null
$BIN insert default or_orders o5 '{"status":"refunded","amount":75,"region":"eu","notes":"duplicate"}' > /dev/null

echo ""
echo "=== PARSER: error cases ==="
GOT=$($BIN count default or_orders '[{"or":[]}]' 2>&1)
assert_contains "empty or rejected"            'empty or/and'      "$GOT"

GOT=$($BIN count default or_orders '[{"and":[]}]' 2>&1)
assert_contains "empty and rejected"           'empty or/and'      "$GOT"

GOT=$($BIN count default or_orders '[{"field":""}]' 2>&1)
assert_contains "missing field rejected"       "leaf missing 'field'" "$GOT"

echo ""
echo "=== SHAPE A (AND only) — regression baseline ==="
GOT=$($BIN count default or_orders '[{"field":"status","op":"eq","value":"paid"}]')
assert_contains "count status=paid"            '"count":2'          "$GOT"

GOT=$($BIN count default or_orders '[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}]')
assert_contains "count status=paid AND region=us" '"count":1'       "$GOT"

GOT=$($BIN find default or_orders '[{"field":"status","op":"eq","value":"paid"},{"field":"amount","op":"gte","value":"100"}]')
assert_contains "find paid+amount>=100 includes o1" '"key":"o1"'    "$GOT"
assert_not_contains "find paid+amount>=100 excludes o2" '"key":"o2"' "$GOT"

echo ""
echo "=== SHAPE B (AND + OR, indexed AND sibling drives primary) ==="
GOT=$($BIN count default or_orders '[{"field":"status","op":"eq","value":"paid"},{"or":[{"field":"region","op":"eq","value":"us"},{"field":"amount","op":"lt","value":"100"}]}]')
assert_contains "Shape B: paid AND (region=us OR amount<100) = 2" '"count":2' "$GOT"

GOT=$($BIN find default or_orders '[{"field":"status","op":"eq","value":"paid"},{"or":[{"field":"region","op":"eq","value":"us"},{"field":"amount","op":"lt","value":"100"}]}]')
assert_contains "Shape B: includes o1 (paid+us)"   '"key":"o1"' "$GOT"
assert_contains "Shape B: includes o2 (paid+amount=50)" '"key":"o2"' "$GOT"
assert_not_contains "Shape B: excludes o3 (pending)" '"key":"o3"' "$GOT"

echo ""
echo "=== SHAPE C (pure OR, all children indexed → KeySet path) ==="
GOT=$($BIN count default or_orders '[{"or":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}]}]')
# o1 (paid,us), o2 (paid,eu), o3 (pending,us)
assert_contains "Shape C: paid OR us = 3"         '"count":3' "$GOT"

GOT=$($BIN find default or_orders '[{"or":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}]}]')
assert_contains "Shape C find: o1 present"         '"key":"o1"' "$GOT"
assert_contains "Shape C find: o2 present"         '"key":"o2"' "$GOT"
assert_contains "Shape C find: o3 present"         '"key":"o3"' "$GOT"
assert_not_contains "Shape C find: o4 absent"      '"key":"o4"' "$GOT"
assert_not_contains "Shape C find: o5 absent"      '"key":"o5"' "$GOT"

# Three indexed OR children (status=paid OR status=pending OR region=asia)
GOT=$($BIN count default or_orders '[{"or":[{"field":"status","op":"eq","value":"paid"},{"field":"status","op":"eq","value":"pending"},{"field":"region","op":"eq","value":"asia"}]}]')
# o1, o2 (paid), o3 (pending), o4 (asia)
assert_contains "Shape C: 3-child OR = 4"         '"count":4' "$GOT"

echo ""
echo "=== SHAPE D (OR with non-indexed child → full scan) ==="
GOT=$($BIN count default or_orders '[{"or":[{"field":"status","op":"eq","value":"paid"},{"field":"amount","op":"gte","value":"200"}]}]')
# o1, o2 (paid), o3 (amount=200)
assert_contains "Shape D: paid OR amount>=200 = 3" '"count":3' "$GOT"

GOT=$($BIN find default or_orders '[{"or":[{"field":"notes","op":"eq","value":"vip"},{"field":"status","op":"eq","value":"cancelled"}]}]')
# notes is non-indexed; o1 (vip), o3 (vip), o4 (cancelled)
assert_contains "Shape D find: o1"                 '"key":"o1"' "$GOT"
assert_contains "Shape D find: o3"                 '"key":"o3"' "$GOT"
assert_contains "Shape D find: o4"                 '"key":"o4"' "$GOT"

echo ""
echo "=== HYBRID (non-indexed AND + fully-indexed OR → KeySet primary) ==="
# amount is non-indexed (AND sibling); OR children both indexed
GOT=$($BIN count default or_orders '[{"field":"amount","op":"gt","value":"60"},{"or":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}]}]')
# OR-union = {o1,o2,o3}. Of those amount>60: o1(100), o3(200). Not o2(50).
assert_contains "Hybrid: amount>60 AND (paid OR us) = 2" '"count":2' "$GOT"

echo ""
echo "=== limit / order_by across OR ==="
GOT=$($BIN find default or_orders '[{"or":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}]}]' 0 2)
# Shape C with limit=2 should emit at most 2 rows
N=$(echo "$GOT" | grep -o '"key":"o' | wc -l)
assert_eq "Shape C find: limit=2 respected" "2" "$N"

# Ordered find with OR (goes through ordered scan path — full-scan based)
GOT=$($BIN query '{"mode":"find","dir":"default","object":"or_orders","criteria":[{"or":[{"field":"status","op":"eq","value":"paid"},{"field":"status","op":"eq","value":"refunded"}]}],"order_by":"amount"}')
# paid: o1(100), o2(50); refunded: o5(75). Sort by amount asc: o2(50), o5(75), o1(100).
assert_contains "ordered find with OR: first is o2"  '"key":"o2"'   "$GOT"

echo ""
echo "=== aggregate with OR ==="
GOT=$($BIN aggregate default or_orders '[{"fn":"count","alias":"n"},{"fn":"sum","field":"amount","alias":"total"}]' '' '[{"or":[{"field":"status","op":"eq","value":"paid"},{"field":"region","op":"eq","value":"us"}]}]')
# o1,o2,o3 matches — n=3, sum=100+50+200=350
assert_contains "agg Shape C count"                  '"n":3'         "$GOT"
assert_contains "agg Shape C sum"                    '"total":350'   "$GOT"

GOT=$($BIN aggregate default or_orders '[{"fn":"count","alias":"n"}]' 'region' '[{"or":[{"field":"status","op":"eq","value":"paid"},{"field":"status","op":"eq","value":"refunded"}]}]')
# paid: o1(us),o2(eu); refunded: o5(eu). us=1, eu=2.
assert_contains "agg group_by region us=1"           '"region":"us","n":1' "$GOT"
assert_contains "agg group_by region eu=2"           '"region":"eu","n":2' "$GOT"

echo ""
echo "=== bulk-delete with OR (dry_run + actual) ==="
GOT=$($BIN query '{"mode":"bulk-delete","dir":"default","object":"or_orders","criteria":[{"or":[{"field":"status","op":"eq","value":"cancelled"},{"field":"status","op":"eq","value":"refunded"}]}],"dry_run":true}')
assert_contains "bulk-delete dry_run matched=2"      '"matched":2'   "$GOT"
assert_contains "bulk-delete dry_run deleted=0"      '"deleted":0'   "$GOT"
assert_contains "bulk-delete dry_run flag"           '"dry_run":true' "$GOT"

GOT=$($BIN count default or_orders)
assert_contains "count before actual delete = 5"      '"count":5'    "$GOT"

GOT=$($BIN query '{"mode":"bulk-delete","dir":"default","object":"or_orders","criteria":[{"or":[{"field":"status","op":"eq","value":"cancelled"},{"field":"status","op":"eq","value":"refunded"}]}]}')
assert_contains "bulk-delete actual deleted=2"       '"deleted":2'   "$GOT"

GOT=$($BIN count default or_orders)
assert_contains "count after delete = 3"             '"count":3'     "$GOT"

echo ""
echo "=== bulk-update with OR condition ==="
GOT=$($BIN query '{"mode":"bulk-update","dir":"default","object":"or_orders","criteria":[{"or":[{"field":"amount","op":"lt","value":"100"},{"field":"region","op":"eq","value":"us"}]}],"value":{"notes":"touched"}}')
# amount<100: o2(50); region=us: o1, o3
assert_contains "bulk-update matched=3"              '"matched":3'   "$GOT"
assert_contains "bulk-update updated=3"              '"updated":3'   "$GOT"

GOT=$($BIN find default or_orders '[{"field":"notes","op":"eq","value":"touched"}]')
# All three should have notes=touched
assert_contains "o1 updated"                          '"key":"o1"'    "$GOT"
assert_contains "o2 updated"                          '"key":"o2"'    "$GOT"
assert_contains "o3 updated"                          '"key":"o3"'    "$GOT"

echo ""
echo "=== Backward compatibility: flat array still means AND ==="
GOT=$($BIN count default or_orders '[{"field":"status","op":"eq","value":"paid"}]')
assert_contains "flat-array single leaf"              '"count":2' "$GOT"

GOT=$($BIN count default or_orders '{"status":"paid","region":"us"}')
# Simple-equality form → AND
assert_contains "simple-equality form still AND"      '"count":1' "$GOT"

echo ""
echo "=== nested OR-in-OR and AND-in-OR ==="
# OR of OR: ((paid OR pending) OR refunded). But we only have paid left after prior bulk-delete.
GOT=$($BIN count default or_orders '[{"or":[{"or":[{"field":"status","op":"eq","value":"paid"}]}]}]' 2>&1)
# nested OR is now single leaf 'paid' inside two OR wrappers — non-indexable because
# nested OR children aren't LEAF directly → full scan. Still correct count.
# After bulk-delete + bulk-update, remaining records: o1 (paid), o2 (paid), o3 (pending).
# Only paid = 2 (o1, o2). o3 is still pending (not touched by the prior bulk ops).
assert_contains "nested-OR single leaf count=2"        '"count":2'   "$GOT"

echo ""
echo "=== CLEANUP ==="
$BIN stop > /dev/null
rm -rf "$DB_ROOT/default/or_orders"
sed -i '/^default:or_orders:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true

echo ""
echo "================================"
echo "  $PASS passed, $FAIL failed ($TOTAL total)"
echo "================================"
[ "$FAIL" -eq 0 ]
