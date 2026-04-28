#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-bulk-update-json.sh — JSON per-key partial update form of bulk-update.
# Mirrors bulk-update-delimited semantics: update-only (key must exist),
# only fields present in `data` overwrite, fields absent are left alone.

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
field() {
    # extract first occurrence of "<field>":"<value>" or "<field>":<value>
    echo "$1" | grep -oE "\"$2\":\"?[^,}\"]*\"?" | head -1 | sed -E "s/^\"$2\":\"?//; s/\"$//"
}
count() {
    echo "$1" | grep -oE '"count":[0-9]+' | head -1 | sed 's/.*://'
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
rm -rf "$DB_ROOT/default/budj_t"
sed -i "/^default:budj_t:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN start > /dev/null
sleep 0.5

$BIN query '{"mode":"create-object","dir":"default","object":"budj_t","fields":["status:varchar:16","amount:int","note:varchar:32"],"indexes":["status","amount"],"splits":16}' > /dev/null

echo "=== seed ==="
$BIN query '{"mode":"bulk-insert","dir":"default","object":"budj_t","records":[{"id":"k1","data":{"status":"paid","amount":100,"note":"vip"}},{"id":"k2","data":{"status":"paid","amount":200,"note":"vip"}},{"id":"k3","data":{"status":"pending","amount":50,"note":""}}]}' > /dev/null

echo "=== inline records: matched/updated/skipped accounting ==="
out=$($BIN query '{"mode":"bulk-update","dir":"default","object":"budj_t","records":[{"id":"k1","data":{"status":"refunded"}},{"id":"k2","data":{"status":"refunded","amount":201}},{"id":"missing","data":{"status":"x"}}]}')
assert_contains "matched=3" '"matched":3' "$out"
assert_contains "updated=2" '"updated":2' "$out"
assert_contains "skipped=1 (missing key)" '"skipped":1' "$out"

echo "=== absent fields are left alone (note untouched) ==="
g=$($BIN query '{"mode":"get","dir":"default","object":"budj_t","key":"k1"}')
assert_contains "k1 status=refunded" '"status":"refunded"' "$g"
assert_contains "k1 amount=100 (untouched)" '"amount":100' "$g"
assert_contains "k1 note=vip (untouched)" '"note":"vip"' "$g"

g=$($BIN query '{"mode":"get","dir":"default","object":"budj_t","key":"k2"}')
assert_contains "k2 status=refunded" '"status":"refunded"' "$g"
assert_contains "k2 amount=201 (changed)" '"amount":201' "$g"
assert_contains "k2 note=vip (untouched)" '"note":"vip"' "$g"

echo "=== indexes track changes (drop-old/insert-new) ==="
assert_eq "count(status=paid)=0" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"budj_t","criteria":[{"field":"status","op":"eq","value":"paid"}]}')")"
assert_eq "count(status=refunded)=2" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"budj_t","criteria":[{"field":"status","op":"eq","value":"refunded"}]}')")"
assert_eq "count(amount=200)=0 (k2 moved)" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"budj_t","criteria":[{"field":"amount","op":"eq","value":"200"}]}')")"
assert_eq "count(amount=201)=1" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"budj_t","criteria":[{"field":"amount","op":"eq","value":"201"}]}')")"

echo "=== file form: same shape, from a JSON file ==="
TF=$(mktemp /tmp/budj_XXXX.json)
cat > "$TF" <<'EOF'
[
  {"id":"k1","data":{"amount":111}},
  {"id":"k3","data":{"status":"paid"}}
]
EOF
out=$($BIN query "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\",\"file\":\"$TF\"}")
assert_contains "file form matched=2" '"matched":2' "$out"
assert_contains "file form updated=2" '"updated":2' "$out"
rm -f "$TF"

g=$($BIN query '{"mode":"get","dir":"default","object":"budj_t","key":"k1"}')
assert_contains "k1 amount=111 (file update)" '"amount":111' "$g"
assert_contains "k1 status=refunded (untouched)" '"status":"refunded"' "$g"
g=$($BIN query '{"mode":"get","dir":"default","object":"budj_t","key":"k3"}')
assert_contains "k3 status=paid (file update)" '"status":"paid"' "$g"

echo "=== empty records list ==="
out=$($BIN query '{"mode":"bulk-update","dir":"default","object":"budj_t","records":[]}')
assert_contains "empty records → matched=0" '"matched":0' "$out"
assert_contains "empty records → updated=0" '"updated":0' "$out"

echo "=== negative: missing records and criteria ==="
out=$($BIN query '{"mode":"bulk-update","dir":"default","object":"budj_t"}' 2>&1)
assert_contains "no input → error" 'requires criteria' "$out"

echo "=== negative: malformed (object instead of array) ==="
out=$($BIN query '{"mode":"bulk-update","dir":"default","object":"budj_t","records":{"id":"k1"}}' 2>&1)
assert_contains "non-array records → error" 'top-level array' "$out"

echo "=== existing criteria-form bulk-update still works (no regression) ==="
out=$($BIN query '{"mode":"bulk-update","dir":"default","object":"budj_t","criteria":[{"field":"status","op":"eq","value":"refunded"}],"value":{"note":"audited"}}')
assert_contains "criteria form returns matched" '"matched":' "$out"
g=$($BIN query '{"mode":"get","dir":"default","object":"budj_t","key":"k2"}')
assert_contains "criteria form patched k2 note" '"note":"audited"' "$g"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"budj_t"}' > /dev/null 2>&1 || true
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
