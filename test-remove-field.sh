#!/bin/bash
# test-remove-field.sh — tests for remove-field (task #3)

set -e

BIN="./shard-db"
PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1)); echo "  FAIL: $1"; }

assert_contains() {
    local desc="$1" needle="$2" hay="$3"
    if echo "$hay" | grep -q -- "$needle"; then pass "$desc"; else fail "$desc: expected '$needle', got: $hay"; fi
}
assert_not_contains() {
    local desc="$1" needle="$2" hay="$3"
    if echo "$hay" | grep -q -- "$needle"; then fail "$desc: should not contain '$needle', got: $hay"; else pass "$desc"; fi
}
assert_present() { if [ -e "$2" ]; then pass "$1"; else fail "$1 (missing: $2)"; fi }
assert_absent()  { if [ ! -e "$2" ]; then pass "$1"; else fail "$1 (present: $2)"; fi }

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
$BIN start
sleep 0.5

# Fresh leads object with 4 fields + 3 indexes (including composite)
$BIN query '{"mode":"truncate","dir":"default","object":"leads"}' 2>/dev/null || true
rm -rf "$DB_ROOT/default/leads"
sed -i '/^default:leads:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN query '{"mode":"create-object","dir":"default","object":"leads","splits":8,"max_key":32,"fields":["fullName:varchar:32","email:varchar:40","age:int","score:int"],"indexes":["email","age","fullName+age"]}' > /dev/null

$BIN insert default leads k1 '{"fullName":"Alice","email":"a@x.com","age":30,"score":100}' > /dev/null
$BIN insert default leads k2 '{"fullName":"Bob","email":"b@x.com","age":25,"score":90}' > /dev/null

OBJ="$DB_ROOT/default/leads"
assert_present "email.idx exists"      "$OBJ/indexes/email.idx"
assert_present "age.idx exists"        "$OBJ/indexes/age.idx"
assert_present "fullName+age.idx exists" "$OBJ/indexes/fullName+age.idx"

echo ""
echo "=== REMOVE single field: 'email' ==="
GOT=$($BIN query '{"mode":"remove-field","dir":"default","object":"leads","fields":["email"]}')
assert_contains "remove status"        '"status":"removed"' "$GOT"
assert_contains "remove fields count"  '"fields":1'          "$GOT"
assert_contains "remove indexes count" '"indexes_dropped":1' "$GOT"

assert_contains "fields.conf has email:removed" "email:varchar:40:removed" "$(cat $OBJ/fields.conf)"
assert_absent   "email.idx dropped"    "$OBJ/indexes/email.idx"
assert_present  "age.idx still exists" "$OBJ/indexes/age.idx"
assert_present  "fullName+age.idx still exists" "$OBJ/indexes/fullName+age.idx"
assert_not_contains "index.conf no email" "^email$" "$(cat $OBJ/indexes/index.conf)"

echo ""
echo "=== READS exclude tombstoned field ==="
GOT=$($BIN get default leads k1)
assert_not_contains "GET no 'email'"           '"email"'       "$GOT"
assert_contains     "GET still has 'fullName'" '"fullName":"Alice"' "$GOT"
assert_contains     "GET still has 'age'"      '"age":30'           "$GOT"
assert_contains     "GET still has 'score'"    '"score":100'        "$GOT"

echo ""
echo "=== INSERT ignores tombstoned field in JSON ==="
$BIN insert default leads k3 '{"fullName":"Carol","email":"c@x.com","age":40,"score":80}' > /dev/null
GOT=$($BIN get default leads k3)
assert_not_contains "insert: 'email' ignored" '"email"' "$GOT"
assert_contains     "insert: fullName saved"  '"fullName":"Carol"' "$GOT"

echo ""
echo "=== UPDATE ignores tombstoned field ==="
$BIN query '{"mode":"update","dir":"default","object":"leads","key":"k1","value":{"email":"ignored@x.com","score":200}}' > /dev/null
GOT=$($BIN get default leads k1)
assert_not_contains "update: 'email' still absent" '"email"' "$GOT"
assert_contains     "update: score updated"        '"score":200' "$GOT"

echo ""
echo "=== SEARCH on tombstoned field fails gracefully ==="
GOT=$($BIN query '{"mode":"find","dir":"default","object":"leads","criteria":[{"field":"email","op":"eq","value":"a@x.com"}]}' 2>&1 || true)
assert_not_contains "search tombstoned: no results" '"key":"k1"' "$GOT"

echo ""
echo "=== REMOVE multiple fields at once ==="
# Remove both 'age' and 'score' in one call — should drop all age-based indexes
GOT=$($BIN query '{"mode":"remove-field","dir":"default","object":"leads","fields":["age","score"]}')
assert_contains "multi-remove status"  '"status":"removed"' "$GOT"
assert_contains "multi-remove count 2" '"fields":2'         "$GOT"
# age.idx + fullName+age.idx should both be dropped
assert_absent   "age.idx dropped"      "$OBJ/indexes/age.idx"
assert_absent   "fullName+age.idx dropped (composite with removed)" "$OBJ/indexes/fullName+age.idx"

GOT=$($BIN get default leads k1)
assert_not_contains "GET no 'age'"    '"age"'   "$GOT"
assert_not_contains "GET no 'score'"  '"score"' "$GOT"
assert_contains     "GET still has 'fullName'" '"fullName"' "$GOT"

echo ""
echo "=== ERROR: remove nonexistent field ==="
GOT=$($BIN query '{"mode":"remove-field","dir":"default","object":"leads","fields":["nope"]}')
assert_contains "missing field error" '"error"' "$GOT"

echo ""
echo "=== ERROR: remove already-tombstoned field ==="
GOT=$($BIN query '{"mode":"remove-field","dir":"default","object":"leads","fields":["age"]}')
assert_contains "already-removed error" "already removed" "$GOT"

echo ""
echo "=== ERROR: empty fields array ==="
GOT=$($BIN query '{"mode":"remove-field","dir":"default","object":"leads","fields":[]}')
assert_contains "empty array error" '"error"' "$GOT"

echo ""
echo "=== CSV bulk insert uses ACTIVE field count ==="
# Remaining active field after remove-field calls: fullName only
# Create fresh object where we can test CSV behavior cleanly
$BIN query '{"mode":"truncate","dir":"default","object":"csvtest"}' 2>/dev/null || true
rm -rf "$DB_ROOT/default/csvtest"
sed -i '/^default:csvtest:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN query '{"mode":"create-object","dir":"default","object":"csvtest","splits":4,"max_key":16,"fields":["a:varchar:8","b:varchar:8","c:varchar:8"],"indexes":[]}' > /dev/null

# Remove middle field 'b' — CSV should now be key|a|c (3 columns)
$BIN query '{"mode":"remove-field","dir":"default","object":"csvtest","fields":["b"]}' > /dev/null

# Write a CSV with 3 columns (key|a|c) and bulk import
CSV=/tmp/shard-db_test_csv.txt
cat > $CSV <<EOF
row1|aaa|ccc
row2|xxx|zzz
EOF
$BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"csvtest\",\"file\":\"$CSV\",\"delimiter\":\"|\"}" > /dev/null
rm -f $CSV

GOT=$($BIN get default csvtest row1)
assert_contains "csv: 'a' = aaa"  '"a":"aaa"' "$GOT"
assert_contains "csv: 'c' = ccc"  '"c":"ccc"' "$GOT"
assert_not_contains "csv: no 'b'" '"b"'       "$GOT"

GOT=$($BIN get default csvtest row2)
assert_contains "csv row2: 'a' = xxx" '"a":"xxx"' "$GOT"
assert_contains "csv row2: 'c' = zzz" '"c":"zzz"' "$GOT"

echo ""
$BIN stop 2>/dev/null || true

echo ""
echo "=== RESULT: $PASS passed, $FAIL failed, $TOTAL total ==="
[ $FAIL -eq 0 ]
