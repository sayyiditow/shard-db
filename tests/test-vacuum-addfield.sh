#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-vacuum-addfield.sh — tests for vacuum --compact / --splits (task #5)
# and add-field (task #6).

set -e

BIN="./shard-db"
PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1)); echo "  FAIL: $1"; }
assert_contains() {
    local desc="$1" needle="$2" hay="$3"
    if echo "$hay" | grep -q -- "$needle"; then pass "$desc"; else fail "$desc: '$needle' not in: $hay"; fi
}
assert_not_contains() {
    local desc="$1" needle="$2" hay="$3"
    if echo "$hay" | grep -q -- "$needle"; then fail "$desc: '$needle' in: $hay"; else pass "$desc"; fi
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

make_fresh() {
    local obj=$1; shift
    $BIN query "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"$obj\"}" 2>/dev/null || true
    rm -rf "$DB_ROOT/default/$obj"
    sed -i "/^default:$obj:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
}

#####################################################################
echo ""
echo "=== TASK #5: vacuum without flags (fast in-place, unchanged) ==="
make_fresh vac1
$BIN query '{"mode":"create-object","dir":"default","object":"vac1","splits":8,"max_key":32,"fields":["name:varchar:32","age:int"],"indexes":[]}' > /dev/null

for i in 1 2 3 4 5; do $BIN insert default vac1 k$i "{\"name\":\"n$i\",\"age\":$i}" > /dev/null; done
$BIN delete default vac1 k1 > /dev/null
$BIN delete default vac1 k3 > /dev/null

GOT=$($BIN query '{"mode":"vacuum","dir":"default","object":"vac1"}')
assert_contains "plain vacuum status"    '"status":"vacuumed"' "$GOT"
assert_contains "plain vacuum cleaned 2" '"cleaned":2'         "$GOT"

# Remaining records still retrievable
GOT=$($BIN get default vac1 k2); assert_contains "vac1: k2 still there" '"name":"n2"' "$GOT"
GOT=$($BIN get default vac1 k4); assert_contains "vac1: k4 still there" '"name":"n4"' "$GOT"

#####################################################################
echo ""
echo "=== TASK #5: vacuum --compact drops tombstoned fields + shrinks layout ==="
make_fresh vac2
$BIN query '{"mode":"create-object","dir":"default","object":"vac2","splits":8,"max_key":32,"fields":["name:varchar:32","email:varchar:40","age:int","score:int"],"indexes":[]}' > /dev/null

for i in 1 2 3; do
    $BIN insert default vac2 k$i "{\"name\":\"n$i\",\"email\":\"e$i@x.com\",\"age\":$((20+i)),\"score\":$((100-i))}" > /dev/null
done

# Tombstone 'email' and 'score'
$BIN query '{"mode":"remove-field","dir":"default","object":"vac2","fields":["email","score"]}' > /dev/null
BEFORE_FIELDS=$(cat "$DB_ROOT/default/vac2/fields.conf")
assert_contains "vac2: email tombstoned" "email:varchar:40:removed" "$BEFORE_FIELDS"
assert_contains "vac2: score tombstoned" "score:int:removed" "$BEFORE_FIELDS"

# vacuum --compact should strip tombstoned entries and shrink slot_size
GOT=$($BIN query '{"mode":"vacuum","dir":"default","object":"vac2","compact":true}')
assert_contains "compact vacuum status"   '"status":"rebuilt"' "$GOT"
assert_contains "compact flag in output"  '"compact":true'     "$GOT"

AFTER_FIELDS=$(cat "$DB_ROOT/default/vac2/fields.conf")
assert_not_contains "fields.conf: no email"  "email" "$AFTER_FIELDS"
assert_not_contains "fields.conf: no score"  "score" "$AFTER_FIELDS"
assert_contains     "fields.conf: name kept" "name:varchar:32" "$AFTER_FIELDS"
assert_contains     "fields.conf: age kept"  "age:int"         "$AFTER_FIELDS"

# Records retrievable with kept fields
GOT=$($BIN get default vac2 k1); assert_contains "vac2: k1 name" '"name":"n1"' "$GOT"
GOT=$($BIN get default vac2 k1); assert_contains "vac2: k1 age"  '"age":21'    "$GOT"
GOT=$($BIN get default vac2 k2); assert_contains "vac2: k2 name" '"name":"n2"' "$GOT"
GOT=$($BIN get default vac2 k3); assert_contains "vac2: k3 age"  '"age":23'    "$GOT"

# Partial artifacts should be cleaned
assert_absent "data.new cleaned"   "$DB_ROOT/default/vac2/data.new"
assert_absent "data.old cleaned"   "$DB_ROOT/default/vac2/data.old"
assert_absent "fields.conf.new gone" "$DB_ROOT/default/vac2/fields.conf.new"
assert_absent "fields.conf.old gone" "$DB_ROOT/default/vac2/fields.conf.old"

#####################################################################
echo ""
echo "=== TASK #5: vacuum --splits N reshards ==="
make_fresh vac3
$BIN query '{"mode":"create-object","dir":"default","object":"vac3","splits":4,"max_key":32,"fields":["name:varchar:16"],"indexes":["name"]}' > /dev/null

for i in $(seq 1 50); do $BIN insert default vac3 k$i "{\"name\":\"n$i\"}" > /dev/null; done

# Before: splits=4
SPLITS_BEFORE=$(grep "^default:vac3:" "$DB_ROOT/schema.conf" | head -1)
assert_contains "vac3 schema: splits=4" "default:vac3:4:" "$SPLITS_BEFORE"

# Reshard to 16
GOT=$($BIN query '{"mode":"vacuum","dir":"default","object":"vac3","splits":16}')
assert_contains "reshard status"  '"status":"rebuilt"' "$GOT"
assert_contains "reshard splits"  '"splits":16'        "$GOT"
assert_contains "reshard live=50" '"live":50'          "$GOT"

SPLITS_AFTER=$(grep "^default:vac3:" "$DB_ROOT/schema.conf" | head -1)
assert_contains "vac3 schema: splits=16" "default:vac3:16:" "$SPLITS_AFTER"

# All records still retrievable via GET (uses hash → new shard routing)
FOUND=0
for i in 1 10 25 50; do
    GOT=$($BIN get default vac3 k$i)
    if echo "$GOT" | grep -q "\"name\":\"n$i\""; then FOUND=$((FOUND+1)); fi
done
if [ $FOUND -eq 4 ]; then pass "all sampled records survive reshard"; else fail "only $FOUND/4 survived"; fi

# Indexed search still works (hashes unchanged, so index entries still valid)
GOT=$($BIN query '{"mode":"find","dir":"default","object":"vac3","criteria":[{"field":"name","op":"eq","value":"n25"}]}')
assert_contains "indexed search post-reshard" '"key":"k25"' "$GOT"

#####################################################################
echo ""
echo "=== TASK #6: add-field appends new fields ==="
make_fresh add1
$BIN query '{"mode":"create-object","dir":"default","object":"add1","splits":8,"max_key":32,"fields":["name:varchar:16"],"indexes":[]}' > /dev/null
$BIN insert default add1 k1 '{"name":"alice"}' > /dev/null
$BIN insert default add1 k2 '{"name":"bob"}'   > /dev/null

OLD_SLOT=$(stat -c%s "$DB_ROOT/default/add1/data/00/00.bin" 2>/dev/null || echo 0)

GOT=$($BIN query '{"mode":"add-field","dir":"default","object":"add1","fields":["age:int","email:varchar:40"]}')
assert_contains "add-field status" '"status":"rebuilt"' "$GOT"

FIELDS=$(cat "$DB_ROOT/default/add1/fields.conf")
assert_contains "fields.conf has age"   "age:int"         "$FIELDS"
assert_contains "fields.conf has email" "email:varchar:40" "$FIELDS"
assert_contains "fields.conf keeps name" "name:varchar:16" "$FIELDS"

# Existing records: name preserved; new int field zero-valued, new varchar absent
GOT=$($BIN get default add1 k1)
assert_contains      "k1 name preserved"   '"name":"alice"' "$GOT"
assert_contains      "k1 age defaults to 0" '"age":0'       "$GOT"
assert_not_contains  "k1 no email (empty varchar)" '"email"' "$GOT"

# New insert can set the new fields
$BIN insert default add1 k3 '{"name":"carol","age":40,"email":"c@x.com"}' > /dev/null
GOT=$($BIN get default add1 k3)
assert_contains "k3 all fields present" '"age":40'           "$GOT"
assert_contains "k3 email"              '"email":"c@x.com"'  "$GOT"

# Update can set new field on existing record
$BIN query '{"mode":"update","dir":"default","object":"add1","key":"k1","value":{"age":30}}' > /dev/null
GOT=$($BIN get default add1 k1)
assert_contains "k1 age set via update" '"age":30' "$GOT"

#####################################################################
echo ""
echo "=== TASK #6: add-field error cases ==="
GOT=$($BIN query '{"mode":"add-field","dir":"default","object":"add1","fields":["name:varchar:32"]}')
assert_contains "duplicate name rejected" '"error"' "$GOT"

GOT=$($BIN query '{"mode":"add-field","dir":"default","object":"add1","fields":["bad:unknowntype"]}')
assert_contains "invalid type rejected" '"error"' "$GOT"

GOT=$($BIN query '{"mode":"add-field","dir":"default","object":"add1","fields":[]}')
assert_contains "empty array rejected" '"error"' "$GOT"

GOT=$($BIN query '{"mode":"add-field","dir":"default","object":"add1","fields":["dup:int","dup:varchar:10"]}')
assert_contains "duplicate-in-request rejected" '"error"' "$GOT"

#####################################################################
echo ""
echo "=== INTEGRATION: remove-field + vacuum --compact + add-field ==="
make_fresh integ
$BIN query '{"mode":"create-object","dir":"default","object":"integ","splits":4,"max_key":32,"fields":["a:varchar:16","b:varchar:16","c:int"],"indexes":[]}' > /dev/null
$BIN insert default integ k1 '{"a":"aa","b":"bb","c":42}' > /dev/null
$BIN insert default integ k2 '{"a":"aaa","b":"bbb","c":43}' > /dev/null

# Remove 'b', vacuum --compact to drop it, add 'd'
$BIN query '{"mode":"remove-field","dir":"default","object":"integ","fields":["b"]}' > /dev/null
$BIN query '{"mode":"vacuum","dir":"default","object":"integ","compact":true}' > /dev/null
$BIN query '{"mode":"add-field","dir":"default","object":"integ","fields":["d:varchar:8"]}' > /dev/null

FIELDS=$(cat "$DB_ROOT/default/integ/fields.conf")
assert_contains     "integ: a kept"   "a:varchar:16" "$FIELDS"
assert_not_contains "integ: b gone"   "b:"           "$FIELDS"
assert_contains     "integ: c kept"   "c:int"        "$FIELDS"
assert_contains     "integ: d added"  "d:varchar:8"  "$FIELDS"

GOT=$($BIN get default integ k1)
assert_contains     "k1 a preserved"  '"a":"aa"' "$GOT"
assert_not_contains "k1 b gone"       '"b"'      "$GOT"
assert_contains     "k1 c preserved"  '"c":42'   "$GOT"
assert_not_contains "k1 d empty"      '"d"'      "$GOT"

# Can set new 'd' on existing record
$BIN query '{"mode":"update","dir":"default","object":"integ","key":"k1","value":{"d":"new"}}' > /dev/null
GOT=$($BIN get default integ k1)
assert_contains "k1 d updated" '"d":"new"' "$GOT"

echo ""
$BIN stop 2>/dev/null || true

echo ""
echo "=== RESULT: $PASS passed, $FAIL failed, $TOTAL total ==="
[ $FAIL -eq 0 ]
