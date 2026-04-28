#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-objlock.sh — tests for per-object rwlock + rebuild crash recovery (task #1)

set -e

BIN="./shard-db"
PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1)); echo "  FAIL: $1"; }

assert_absent() {
    local desc="$1" path="$2"
    if [ ! -e "$path" ]; then pass "$desc"; else fail "$desc (still exists: $path)"; fi
}

assert_present() {
    local desc="$1" path="$2"
    if [ -e "$path" ]; then pass "$desc"; else fail "$desc (missing: $path)"; fi
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3

DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

# Clean leads object, start server, recreate
$BIN start
sleep 0.5
$BIN query '{"mode":"truncate","dir":"default","object":"leads"}' 2>/dev/null || true
rm -rf "$DB_ROOT/default/leads"
sed -i '/^default:leads:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN query '{"mode":"create-object","dir":"default","object":"leads","splits":16,"max_key":32,"fields":["name:varchar:32","age:int"],"indexes":[]}' > /dev/null

$BIN insert default leads a '{"name":"alice","age":30}' > /dev/null
$BIN insert default leads b '{"name":"bob","age":25}'   > /dev/null
assert_present "leads/data exists after insert" "$DB_ROOT/default/leads/data"

echo ""
echo "=== CRASH RECOVERY: stale .new artifacts are cleaned on startup ==="

$BIN stop
sleep 0.3

# Inject fake partial-rebuild artifacts
OBJ="$DB_ROOT/default/leads"
mkdir -p "$OBJ/data.new/00"
echo "stale shard" > "$OBJ/data.new/00/00.bin"
mkdir -p "$OBJ/indexes.new"
echo "stale idx" > "$OBJ/indexes.new/stale.idx"
echo "stale fields" > "$OBJ/fields.conf.new"
echo "stale schema" > "$OBJ/schema.conf.new"
mkdir -p "$OBJ/data.old"
echo "stale old" > "$OBJ/data.old/leftover"

assert_present "injected data.new before recovery"      "$OBJ/data.new"
assert_present "injected indexes.new before recovery"   "$OBJ/indexes.new"
assert_present "injected fields.conf.new before recovery" "$OBJ/fields.conf.new"
assert_present "injected schema.conf.new before recovery" "$OBJ/schema.conf.new"
assert_present "injected data.old before recovery"      "$OBJ/data.old"

# Restart — recovery should sweep
$BIN start
sleep 0.5

assert_absent "data.new removed after startup"        "$OBJ/data.new"
assert_absent "indexes.new removed after startup"     "$OBJ/indexes.new"
assert_absent "fields.conf.new removed after startup" "$OBJ/fields.conf.new"
assert_absent "schema.conf.new removed after startup" "$OBJ/schema.conf.new"
assert_absent "data.old removed after startup"        "$OBJ/data.old"

# Live data is untouched
assert_present "data/ still present"        "$OBJ/data"

echo ""
echo "=== SERVER STILL FUNCTIONAL AFTER RECOVERY ==="
GOT=$($BIN get default leads a)
if echo "$GOT" | grep -q '"name":"alice"'; then pass "GET a returns alice"; else fail "GET a: $GOT"; fi

$BIN insert default leads c '{"name":"carol","age":40}' > /dev/null
GOT=$($BIN get default leads c)
if echo "$GOT" | grep -q '"name":"carol"'; then pass "INSERT+GET c"; else fail "GET c: $GOT"; fi

echo ""
echo "=== NO REGRESSION: normal write throughput sanity ==="
START=$(date +%s%N)
for i in $(seq 1 200); do
    $BIN insert default leads "k$i" '{"name":"x","age":1}' > /dev/null
done
END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
pass "200 sequential inserts in ${ELAPSED_MS}ms"

echo ""
echo "=== CREATE-OBJECT LIMITS: max_key ceiling (MAX_KEY_CEILING=1024) ==="

# Ensure clean slate for this object
$BIN query '{"mode":"truncate","dir":"default","object":"keylim"}' 2>/dev/null || true
rm -rf "$DB_ROOT/default/keylim"
sed -i '/^default:keylim:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true

# max_key above ceiling must be rejected
OUT=$($BIN query '{"mode":"create-object","dir":"default","object":"keylim","splits":16,"max_key":2000,"fields":["v:int"],"indexes":[]}')
if echo "$OUT" | grep -q '"error"' && echo "$OUT" | grep -q "exceeds ceiling"; then
    pass "create-object rejects max_key=2000"
else
    fail "create-object should reject max_key=2000 (got: $OUT)"
fi
assert_absent "rejected object not created on disk" "$DB_ROOT/default/keylim"

# max_key at the ceiling must be accepted
OUT=$($BIN query '{"mode":"create-object","dir":"default","object":"keylim","splits":16,"max_key":1024,"fields":["v:int"],"indexes":[]}')
if echo "$OUT" | grep -q '"error"'; then
    fail "create-object should accept max_key=1024 (got: $OUT)"
else
    pass "create-object accepts max_key=1024"
fi
$BIN query '{"mode":"truncate","dir":"default","object":"keylim"}' > /dev/null 2>&1 || true

echo ""
$BIN stop 2>/dev/null || true

echo ""
echo "=== RESULT: $PASS passed, $FAIL failed, $TOTAL total ==="
[ $FAIL -eq 0 ]
