#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-bulk-update-delimited.sh — CSV per-key partial update (2026.05 item 9a)
# Semantics: update-only, key must exist, blank cell = leave alone,
# non-blank cell overwrites. Indexes only touched when their value changed.

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

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3

DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

OBJ="udtest"
rm -rf "$DB_ROOT/default/$OBJ"
sed -i "/^default:$OBJ:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true

$BIN start
sleep 0.5

$BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"$OBJ\",\"splits\":16,\"max_key\":16,\"fields\":[\"name:varchar:40\",\"age:int\",\"status:varchar:12\",\"city:varchar:40\"],\"indexes\":[\"name\",\"status\",\"city\"]}" > /dev/null
pass "object created with 3 indexes (name, status, city)"

# Seed 5 records via a JSON file
cat > /tmp/shard-db_seed.json <<'EOF'
[
  {"id":"k1","data":{"name":"alice","age":"30","status":"active","city":"London"}},
  {"id":"k2","data":{"name":"bob","age":"25","status":"active","city":"Paris"}},
  {"id":"k3","data":{"name":"carol","age":"40","status":"inactive","city":"Berlin"}},
  {"id":"k4","data":{"name":"dave","age":"35","status":"active","city":"Tokyo"}},
  {"id":"k5","data":{"name":"eve","age":"28","status":"inactive","city":"Madrid"}}
]
EOF
$BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_seed.json\"}" > /dev/null
COUNT=$($BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}")
assert_contains "seeded 5 records" '"count":5' "$COUNT"

echo ""
echo "=== BASIC CSV UPDATE ==="

# Comma-delimited, row shape: key,name,age,status,city (fields.conf order)
# Non-blank cells should overwrite; blank cells leave alone.
cat > /tmp/shard-db_upd.csv <<'EOF'
k1,,31,,
k2,robert,,,Lyon
EOF

RESP=$($BIN query "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_upd.csv\",\"delimiter\":\",\"}")
assert_contains "matched=2 updated=2 skipped=0" '"matched":2,"updated":2,"skipped":0' "$RESP"

# k1 should now have age=31 but name still alice, status still active, city still London
K1=$($BIN get default $OBJ k1)
assert_contains "k1 age updated to 31"      '"age":31'       "$K1"
assert_contains "k1 name unchanged (alice)" '"name":"alice"'    "$K1"
assert_contains "k1 status unchanged (active)" '"status":"active"' "$K1"
assert_contains "k1 city unchanged (London)" '"city":"London"'  "$K1"

# k2 should have name=robert, city=Lyon, age unchanged (25), status unchanged (active)
K2=$($BIN get default $OBJ k2)
assert_contains "k2 name updated to robert"  '"name":"robert"'  "$K2"
assert_contains "k2 city updated to Lyon"    '"city":"Lyon"'    "$K2"
assert_contains "k2 age unchanged (25)"      '"age":25'       "$K2"
assert_contains "k2 status unchanged (active)" '"status":"active"' "$K2"

echo ""
echo "=== MISSING KEYS ARE SKIPPED, NOT INSERTED ==="

cat > /tmp/shard-db_upd.csv <<'EOF'
k3,,,suspended,
k_missing_1,zzz,99,foo,bar
k_missing_2,yyy,88,baz,qux
EOF

RESP=$($BIN query "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_upd.csv\",\"delimiter\":\",\"}")
assert_contains "matched=3 updated=1 skipped=2" '"matched":3,"updated":1,"skipped":2' "$RESP"

# k3 status changed to suspended
K3=$($BIN get default $OBJ k3)
assert_contains "k3 status=suspended" '"status":"suspended"' "$K3"
assert_contains "k3 age unchanged (40)" '"age":40' "$K3"

# Missing keys must NOT have been inserted
MISS1=$($BIN get default $OBJ k_missing_1 2>&1 || true)
assert_not_contains "k_missing_1 was NOT inserted" '"name":"zzz"' "$MISS1"

COUNT=$($BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}")
assert_contains "record count still 5 (no upserts)" '"count":5' "$COUNT"

echo ""
echo "=== INDEX UPDATES TRACK CHANGES ==="

# Before: 3 active (k1,k2,k4), 2 inactive (k3,k5).
# After csv update only flipped k3 (inactive→suspended):
#   3 active, 1 inactive, 1 suspended.
N_ACT=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}]}")
N_INA=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"inactive\"}]}")
N_SUS=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"suspended\"}]}")
assert_contains "index: active=3 (unchanged)"            '"count":3' "$N_ACT"
assert_contains "index: inactive=1 (k3 moved out)"       '"count":1' "$N_INA"
assert_contains "index: suspended=1 (k3 arrived)"        '"count":1' "$N_SUS"

# city index: k2 moved Paris → Lyon
N_PAR=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"Paris\"}]}")
N_LYON=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"Lyon\"}]}")
assert_contains "city: Paris=0 (k2 moved out)" '"count":0' "$N_PAR"
assert_contains "city: Lyon=1 (k2 landed)"      '"count":1' "$N_LYON"

# name index: k2 moved bob → robert
N_BOB=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"bob\"}]}")
N_ROB=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"robert\"}]}")
assert_contains "name: bob=0"    '"count":0' "$N_BOB"
assert_contains "name: robert=1" '"count":1' "$N_ROB"

echo ""
echo "=== BLANK-CELL INDEXES ARE UNTOUCHED ==="

# Update k4 age only — name/status/city blank. Index entries for those fields
# must not be rewritten (the idx_changed_bitmap optimisation). Observable
# result: find by name=dave should still resolve, status=active should still
# include k4, city=Tokyo should still include k4.
cat > /tmp/shard-db_upd.csv <<'EOF'
k4,,36,,
EOF
RESP=$($BIN query "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_upd.csv\",\"delimiter\":\",\"}")
assert_contains "k4 age-only update matched=1 updated=1" '"matched":1,"updated":1' "$RESP"

K4=$($BIN get default $OBJ k4)
assert_contains "k4 age=36"          '"age":36'   "$K4"
assert_contains "k4 name=dave still" '"name":"dave"' "$K4"

NAME_DAVE=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"dave\"}]}")
assert_contains "name=dave index still resolves to 1" '"count":1' "$NAME_DAVE"
CITY_TOKYO=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"Tokyo\"}]}")
assert_contains "city=Tokyo index still resolves to 1" '"count":1' "$CITY_TOKYO"

echo ""
echo "=== PIPE DELIMITER ==="

printf 'k5|frank|||\n' > /tmp/shard-db_upd.tsv
RESP=$($BIN query "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_upd.tsv\",\"delimiter\":\"|\"}")
assert_contains "pipe delimited update succeeded" '"updated":1' "$RESP"
K5=$($BIN get default $OBJ k5)
assert_contains "k5 name=frank"     '"name":"frank"' "$K5"
assert_contains "k5 age unchanged"  '"age":28'       "$K5"

echo ""
echo "=== ERROR HANDLING ==="

# Empty file
echo -n "" > /tmp/shard-db_upd.csv
RESP=$($BIN query "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_upd.csv\",\"delimiter\":\",\"}")
assert_contains "empty file returns error" '"error"' "$RESP"

# Missing file path
RESP=$($BIN query "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\"}")
assert_contains "missing file arg returns error" '"error"' "$RESP"

# Nonexistent file
RESP=$($BIN query "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_upd_does_not_exist.csv\",\"delimiter\":\",\"}")
assert_contains "nonexistent file returns error" '"error"' "$RESP"

echo ""
echo "=== CLEANUP ==="
$BIN query "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"$OBJ\"}" > /dev/null
rm -rf "$DB_ROOT/default/$OBJ"
sed -i "/^default:$OBJ:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
rm -f /tmp/shard-db_upd.csv /tmp/shard-db_upd.tsv /tmp/shard-db_seed.json
$BIN stop

echo ""
echo "=== RESULT: $PASS passed, $FAIL failed, $TOTAL total ==="
exit $FAIL
