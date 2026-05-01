#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-schema-export.sh — argv-form schema export / import (CLI parity with
# the TUI Migrate menu). Round-trip: build a few tenants/objects → export →
# wipe → import → verify all schemas reconstructed.

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

# Two tenants for this test (both isolated objects with the `mig_` prefix).
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
grep -q "^migtest$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "migtest" >> "$DB_ROOT/dirs.conf"
mkdir -p "$DB_ROOT/migtest"

# Wipe any leftover mig_ schemas / data from previous runs.
sed -i "/^default:mig_/d"  "$DB_ROOT/schema.conf" 2>/dev/null || true
sed -i "/^migtest:mig_/d"  "$DB_ROOT/schema.conf" 2>/dev/null || true
rm -rf "$DB_ROOT/default/mig_users"  "$DB_ROOT/default/mig_orders"  "$DB_ROOT/migtest/mig_events"

$BIN start
sleep 0.5

# Build three test objects across two tenants. Mix typed shapes so the
# round-trip exercises varchar (length translation), int, numeric (scale
# preserved), bool, datetime, and composite indexes.
$BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"mig_users\",\"splits\":16,\"max_key\":32,\"fields\":[\"name:varchar:40\",\"age:int\",\"active:bool\",\"created_at:datetime\"],\"indexes\":[\"age\",\"name\"]}" > /dev/null
$BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"mig_orders\",\"splits\":16,\"max_key\":16,\"fields\":[\"status:varchar:16\",\"amount:numeric:18,2\",\"region:varchar:16\"],\"indexes\":[\"status\",\"status+region\"]}" > /dev/null
$BIN query "{\"mode\":\"create-object\",\"dir\":\"migtest\",\"object\":\"mig_events\",\"splits\":16,\"max_key\":24,\"fields\":[\"kind:varchar:24\",\"ts:datetime\"],\"indexes\":[]}" > /dev/null
pass "3 objects created across 2 tenants"

# Insert one record into each, just to confirm import is data-free.
$BIN insert default mig_users     u1 '{"name":"alice","age":"30","active":"true","created_at":"20260427120000"}' > /dev/null
$BIN insert default mig_orders    o1 '{"status":"paid","amount":"123.45","region":"us"}' > /dev/null
$BIN insert migtest mig_events    e1 '{"kind":"login","ts":"20260427121500"}' > /dev/null

# ----------------------------------------------------------------------
echo ""
echo "=== EXPORT ==="

OUT="/tmp/shard-db_migtest_export.json"
rm -f "$OUT"
$BIN export-schema "$OUT" 2>&1 | grep -q "exported" && pass "export-schema reported success" || fail "export-schema did not announce success"
[ -s "$OUT" ] && pass "manifest file is non-empty" || fail "manifest file empty or missing"

MANIFEST=$(cat "$OUT")
assert_contains "manifest has version field"   '"version"'              "$MANIFEST"
assert_contains "manifest has dirs[]"           '"dirs"'                 "$MANIFEST"
assert_contains "manifest has objects[]"        '"objects"'              "$MANIFEST"
assert_contains "default tenant present"        '"default"'              "$MANIFEST"
assert_contains "migtest tenant present"        '"migtest"'              "$MANIFEST"
assert_contains "mig_users entry"               '"object":"mig_users"'   "$MANIFEST"
assert_contains "mig_orders entry"              '"object":"mig_orders"'  "$MANIFEST"
assert_contains "mig_events entry"              '"object":"mig_events"'  "$MANIFEST"

# Field-spec round-trip — content-size for varchar (40, not 42), scale for numeric.
assert_contains "varchar:40 (content size, not on-disk)" '"name:varchar:40"' "$MANIFEST"
assert_contains "numeric scale preserved"        '"amount:numeric:18,2"' "$MANIFEST"
assert_contains "int simple"                     '"age:int"'             "$MANIFEST"
assert_contains "bool simple"                    '"active:bool"'         "$MANIFEST"
assert_contains "datetime simple"                '"created_at:datetime"' "$MANIFEST"

# Composite index round-trip
assert_contains "composite index exported"       '"status+region"'       "$MANIFEST"

# splits / max_key preserved
assert_contains "mig_users splits=16"            '"splits":16'  "$MANIFEST"
assert_contains "mig_users max_key=32"          '"max_key":32' "$MANIFEST"

# Manifest must NOT contain data, tokens, or record_count
assert_not_contains "no record bodies"           '"alice"'      "$MANIFEST"
assert_not_contains "no record_count field"      'record_count' "$MANIFEST"

# ----------------------------------------------------------------------
echo ""
echo "=== STDOUT FORM ==="

STDOUT_MANIFEST=$($BIN export-schema 2>/dev/null)
assert_contains "stdout export contains version" '"version"' "$STDOUT_MANIFEST"
assert_contains "stdout export contains mig_users" '"mig_users"' "$STDOUT_MANIFEST"

# ----------------------------------------------------------------------
echo ""
echo "=== WIPE + IMPORT ROUND-TRIP ==="

# Drop the three objects we just exported; data is not part of the manifest
# so we expect import to recreate the schema only.
$BIN query "{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"mig_users\"}" > /dev/null
$BIN query "{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"mig_orders\"}" > /dev/null
$BIN query "{\"mode\":\"drop-object\",\"dir\":\"migtest\",\"object\":\"mig_events\"}" > /dev/null

# Confirm the wipe actually happened
LIST_DEF=$($BIN query "{\"mode\":\"list-objects\",\"dir\":\"default\"}")
assert_not_contains "wipe: mig_users gone from default" '"mig_users"' "$LIST_DEF"
LIST_MIG=$($BIN query "{\"mode\":\"list-objects\",\"dir\":\"migtest\"}")
assert_not_contains "wipe: mig_events gone from migtest" '"mig_events"' "$LIST_MIG"

# Replay the manifest. Use --if-not-exists so any other objects that
# already exist in the live DB (the test isn't the only thing in `default`)
# get skipped cleanly rather than counted as failures.
IMPORT_OUT=$($BIN import-schema "$OUT" --if-not-exists 2>&1)
assert_contains "import announced created=3 (the 3 we dropped)" 'created=3' "$IMPORT_OUT"
assert_contains "import announced failed=0"                      'failed=0'  "$IMPORT_OUT"

# Verify schemas reconstructed via describe-object
DESC1=$($BIN query "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"mig_users\"}")
assert_contains "mig_users splits restored"     '"splits":16'           "$DESC1"
assert_contains "mig_users name field restored" '"name":"name"'        "$DESC1"
assert_contains "mig_users age index restored"  '"age"'                "$DESC1"
# describe-object reports varchar on-disk size (= content + 2)
assert_contains "mig_users name varchar size=42" '"size":42'           "$DESC1"

DESC2=$($BIN query "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"mig_orders\"}")
assert_contains "mig_orders numeric scale=2"     '"scale":2'           "$DESC2"
assert_contains "mig_orders composite idx"       '"status+region"'     "$DESC2"

DESC3=$($BIN query "{\"mode\":\"describe-object\",\"dir\":\"migtest\",\"object\":\"mig_events\"}")
assert_contains "mig_events kind field restored" '"kind"'              "$DESC3"

# Imported objects start empty (no data carried)
COUNT1=$($BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"mig_users\"}")
assert_contains "mig_users empty after import" '0' "$COUNT1"

# ----------------------------------------------------------------------
echo ""
echo "=== --if-not-exists IS IDEMPOTENT ==="

# Re-import same manifest with --if-not-exists → everything already exists,
# nothing created, nothing failed.
IMPORT_OUT=$($BIN import-schema "$OUT" --if-not-exists 2>&1)
assert_contains "rerun: created=0"   'created=0' "$IMPORT_OUT"
assert_contains "rerun: failed=0"    'failed=0'  "$IMPORT_OUT"

# Without --if-not-exists, re-import collides on every object — at least
# the 3 from this test must show up as failures.
IMPORT_OUT=$($BIN import-schema "$OUT" 2>&1 || true)
assert_contains "default rerun: created=0" 'created=0' "$IMPORT_OUT"
# Don't pin failed=N exactly — depends on how many other live objects exist.

# ----------------------------------------------------------------------
echo ""
echo "=== ERROR HANDLING ==="

OUT2=$($BIN import-schema /tmp/shard-db_does_not_exist.json 2>&1 || true)
assert_contains "missing manifest reports error" 'cannot open' "$OUT2"

# Manifest without an objects[] array
echo '{"version":"x"}' > /tmp/shard-db_bad_manifest.json
OUT2=$($BIN import-schema /tmp/shard-db_bad_manifest.json 2>&1 || true)
assert_contains "no-objects manifest reports error" 'objects' "$OUT2"

# ----------------------------------------------------------------------
echo ""
echo "=== CLEANUP ==="
$BIN query "{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"mig_users\"}" > /dev/null 2>&1 || true
$BIN query "{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"mig_orders\"}" > /dev/null 2>&1 || true
$BIN query "{\"mode\":\"drop-object\",\"dir\":\"migtest\",\"object\":\"mig_events\"}" > /dev/null 2>&1 || true
rm -rf "$DB_ROOT/migtest"
sed -i "/^migtest$/d" "$DB_ROOT/dirs.conf" 2>/dev/null || true
rm -f "$OUT" /tmp/shard-db_bad_manifest.json
$BIN stop

echo ""
echo "=== RESULT: $PASS passed, $FAIL failed, $TOTAL total ==="
exit $FAIL
