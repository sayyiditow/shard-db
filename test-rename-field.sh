#!/bin/bash
# test-rename-field.sh — tests for rename-field (task #2)

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

# Fresh leads object
$BIN query '{"mode":"truncate","dir":"default","object":"leads"}' 2>/dev/null || true
rm -rf "$DB_ROOT/default/leads"
sed -i '/^default:leads:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN query '{"mode":"create-object","dir":"default","object":"leads","splits":8,"max_key":32,"fields":["fullName:varchar:32","email:varchar:40","age:int"],"indexes":["email","age","fullName+age"]}' > /dev/null

# Seed data so indexes exist with entries
$BIN insert default leads k1 '{"fullName":"Alice","email":"a@x.com","age":30}' > /dev/null
$BIN insert default leads k2 '{"fullName":"Bob","email":"b@x.com","age":25}' > /dev/null

OBJ="$DB_ROOT/default/leads"
assert_present "email index exists"          "$OBJ/indexes/email.idx"
assert_present "age index exists"            "$OBJ/indexes/age.idx"
assert_present "composite index exists"      "$OBJ/indexes/fullName+age.idx"

echo ""
echo "=== HAPPY PATH: rename 'email' -> 'contact' ==="
GOT=$($BIN query '{"mode":"rename-field","dir":"default","object":"leads","old":"email","new":"contact"}')
assert_contains "rename-field status" '"status":"renamed"' "$GOT"
assert_present  "new index file"            "$OBJ/indexes/contact.idx"
assert_absent   "old index file gone"       "$OBJ/indexes/email.idx"
assert_contains "fields.conf has new name"  "contact:varchar:40" "$(cat $OBJ/fields.conf)"
assert_not_contains "fields.conf old name gone" "^email:"       "$(cat $OBJ/fields.conf)"
assert_contains "index.conf has new name"   "contact"           "$(cat $OBJ/indexes/index.conf)"

# Verify GET returns field under new name
GOT=$($BIN get default leads k1)
assert_contains "GET k1 has 'contact'"  '"contact":"a@x.com"' "$GOT"
assert_not_contains "GET k1 no 'email'" '"email"' "$GOT"

# Verify INSERT with new name works
$BIN insert default leads k3 '{"fullName":"Carol","contact":"c@x.com","age":40}' > /dev/null
GOT=$($BIN get default leads k3)
assert_contains "insert+get with new name" '"contact":"c@x.com"' "$GOT"

# Verify search by renamed indexed field works
GOT=$($BIN query '{"mode":"find","dir":"default","object":"leads","criteria":[{"field":"contact","op":"eq","value":"a@x.com"}]}')
assert_contains "search by renamed field"  '"key":"k1"' "$GOT"

echo ""
echo "=== COMPOSITE INDEX: rename 'fullName' -> 'fn' ==="
GOT=$($BIN query '{"mode":"rename-field","dir":"default","object":"leads","old":"fullName","new":"fn"}')
assert_contains "composite rename status" '"status":"renamed"' "$GOT"
assert_present  "composite index renamed"  "$OBJ/indexes/fn+age.idx"
assert_absent   "old composite index gone" "$OBJ/indexes/fullName+age.idx"
assert_contains "fields.conf has 'fn'"     "fn:varchar:32"     "$(cat $OBJ/fields.conf)"

GOT=$($BIN get default leads k1)
assert_contains "GET k1 has 'fn'" '"fn":"Alice"' "$GOT"

echo ""
echo "=== ERROR: rename missing field ==="
GOT=$($BIN query '{"mode":"rename-field","dir":"default","object":"leads","old":"nope","new":"x"}')
assert_contains "missing field error" '"error"' "$GOT"
assert_contains "missing field msg" "not found" "$GOT"

echo ""
echo "=== ERROR: rename to existing name ==="
GOT=$($BIN query '{"mode":"rename-field","dir":"default","object":"leads","old":"age","new":"contact"}')
assert_contains "conflict error" '"error"' "$GOT"
assert_contains "conflict msg" "already exists" "$GOT"

echo ""
echo "=== ERROR: invalid new name (contains +) ==="
GOT=$($BIN query '{"mode":"rename-field","dir":"default","object":"leads","old":"age","new":"bad+name"}')
assert_contains "invalid name error" '"error"' "$GOT"

echo ""
echo "=== ERROR: rename to identical name ==="
GOT=$($BIN query '{"mode":"rename-field","dir":"default","object":"leads","old":"age","new":"age"}')
assert_contains "identical name error" '"error"' "$GOT"

echo ""
$BIN stop 2>/dev/null || true

echo ""
echo "=== RESULT: $PASS passed, $FAIL failed, $TOTAL total ==="
[ $FAIL -eq 0 ]
