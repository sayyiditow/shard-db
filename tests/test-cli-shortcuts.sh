#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-cli-shortcuts.sh — CLI shortcuts for count/aggregate + delete-file mode (2026.05)

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
assert_present() {
    local desc="$1" path="$2"
    if [ -e "$path" ]; then pass "$desc"; else fail "$desc (missing: $path)"; fi
}
assert_absent() {
    local desc="$1" path="$2"
    if [ ! -e "$path" ]; then pass "$desc"; else fail "$desc (still exists: $path)"; fi
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3

DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

rm -rf "$DB_ROOT/default/cli_orders" "$DB_ROOT/default/cli_files"
sed -i '/^default:cli_orders:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
sed -i '/^default:cli_files:/d'  "$DB_ROOT/schema.conf" 2>/dev/null || true

$BIN start > /dev/null
sleep 0.5

$BIN query '{"mode":"create-object","dir":"default","object":"cli_orders","splits":16,"max_key":16,"fields":["status:varchar:16","amount:int","region:varchar:16"]}' > /dev/null
$BIN query '{"mode":"create-object","dir":"default","object":"cli_files","splits":16,"max_key":32,"fields":["name:varchar:32"]}' > /dev/null

$BIN insert default cli_orders o1 '{"status":"paid","amount":100,"region":"us"}'    > /dev/null
$BIN insert default cli_orders o2 '{"status":"paid","amount":250,"region":"us"}'    > /dev/null
$BIN insert default cli_orders o3 '{"status":"paid","amount":75,"region":"eu"}'     > /dev/null
$BIN insert default cli_orders o4 '{"status":"pending","amount":40,"region":"eu"}'  > /dev/null
$BIN insert default cli_orders o5 '{"status":"cancelled","amount":0,"region":"us"}' > /dev/null

echo ""
echo "=== count CLI ==="
GOT=$($BIN count default cli_orders)
assert_contains "count no-criteria returns total"         '5'    "$GOT"

GOT=$($BIN count default cli_orders '[{"field":"status","op":"eq","value":"paid"}]')
assert_contains "count with eq criteria"                  '3'    "$GOT"

GOT=$($BIN count default cli_orders '[{"field":"amount","op":"gte","value":"100"}]')
assert_contains "count with gte criteria"                 '2'    "$GOT"

GOT=$($BIN count default cli_orders '[{"field":"status","op":"eq","value":"nope"}]')
assert_contains "count no-match returns 0"                '0'    "$GOT"

GOT=$($BIN count default 2>&1 || true)
assert_contains "count usage when too few args"           'Usage: shard-db count' "$GOT"

echo ""
echo "=== aggregate CLI ==="
# Bare aggregates, no group_by
GOT=$($BIN aggregate default cli_orders '[{"fn":"count","alias":"n"},{"fn":"sum","field":"amount","alias":"total"}]')
assert_contains "aggregate bare: count"                   '"n":5'       "$GOT"
assert_contains "aggregate bare: sum"                     '"total":465' "$GOT"

# With group_by CSV
GOT=$($BIN aggregate default cli_orders '[{"fn":"count","alias":"n"}]' 'status')
assert_contains "aggregate group_by status: paid=3"       '"status":"paid","n":3'       "$GOT"
assert_contains "aggregate group_by status: pending=1"    '"status":"pending","n":1'    "$GOT"
assert_contains "aggregate group_by status: cancelled=1"  '"status":"cancelled","n":1'  "$GOT"

# Multi-field group_by CSV with whitespace
GOT=$($BIN aggregate default cli_orders '[{"fn":"count","alias":"n"}]' 'status, region')
assert_contains "aggregate multi-group: paid+us"          '"status":"paid","region":"us","n":2' "$GOT"
assert_contains "aggregate multi-group: paid+eu"          '"status":"paid","region":"eu","n":1' "$GOT"

# With criteria (4th positional)
GOT=$($BIN aggregate default cli_orders '[{"fn":"count","alias":"n"}]' 'status' '[{"field":"region","op":"eq","value":"us"}]')
assert_contains "aggregate with criteria filters region"  '"status":"paid","n":2'       "$GOT"
assert_not_contains "aggregate criteria excludes eu"      '"status":"pending"'          "$GOT"

# With having (5th positional, empty group_by skipped — but we already have group_by)
GOT=$($BIN aggregate default cli_orders '[{"fn":"count","alias":"n"}]' 'status' '' '[{"field":"n","op":"gte","value":"2"}]')
assert_contains "aggregate having filters groups"         '"status":"paid","n":3'       "$GOT"
assert_not_contains "aggregate having drops pending"      '"status":"pending"'          "$GOT"

GOT=$($BIN aggregate default cli_orders 2>&1 || true)
assert_contains "aggregate usage when too few args"       'Usage: shard-db aggregate'   "$GOT"

echo ""
echo "=== delete-file mode + CLI ==="
TMP=$(mktemp)
printf "shard-db test payload" > "$TMP"
BASENAME="$(basename "$TMP")"

$BIN put-file default cli_files "$TMP" > /dev/null

# Compute on-disk path via xxh128 of basename-without-ext (server convention)
STORE_PATH=$($BIN query "{\"mode\":\"get-file-path\",\"dir\":\"default\",\"object\":\"cli_files\",\"filename\":\"$BASENAME\"}" | sed 's/.*"path":"\([^"]*\)".*/\1/')
assert_present "put-file stored file exists on disk"  "$STORE_PATH"

# Delete via CLI
GOT=$($BIN delete-file default cli_files "$BASENAME")
assert_contains "delete-file success status"          '"status":"deleted"'    "$GOT"
assert_contains "delete-file echoes filename"         "\"filename\":\"$BASENAME\"" "$GOT"
assert_absent   "delete-file removed file from disk"  "$STORE_PATH"

# Second delete → not found
GOT=$($BIN delete-file default cli_files "$BASENAME")
assert_contains "delete-file on missing returns error" '"error":"file not found"' "$GOT"

# Filename traversal rejected
GOT=$($BIN delete-file default cli_files '../evil.txt')
assert_contains "delete-file rejects traversal"       '"error":"invalid filename"' "$GOT"

# JSON mode without filename
GOT=$($BIN query '{"mode":"delete-file","dir":"default","object":"cli_files"}')
assert_contains "delete-file JSON missing filename"   '"error":"filename is required"' "$GOT"

# JSON mode happy path (put again, delete via JSON)
$BIN put-file default cli_files "$TMP" > /dev/null
assert_present "re-upload present before JSON delete" "$STORE_PATH"
GOT=$($BIN query "{\"mode\":\"delete-file\",\"dir\":\"default\",\"object\":\"cli_files\",\"filename\":\"$BASENAME\"}")
assert_contains "delete-file JSON succeeds"           '"status":"deleted"'     "$GOT"
assert_absent   "JSON delete removed file"            "$STORE_PATH"

# CLI usage message
GOT=$($BIN delete-file default cli_files 2>&1 || true)
assert_contains "delete-file usage when missing args" 'Usage: shard-db delete-file' "$GOT"

rm -f "$TMP"

echo ""
echo "=== CLEANUP ==="
$BIN stop > /dev/null
rm -rf "$DB_ROOT/default/cli_orders" "$DB_ROOT/default/cli_files"
sed -i '/^default:cli_orders:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
sed -i '/^default:cli_files:/d'  "$DB_ROOT/schema.conf" 2>/dev/null || true

echo ""
echo "================================"
echo "  $PASS passed, $FAIL failed ($TOTAL total)"
echo "================================"
[ "$FAIL" -eq 0 ]
