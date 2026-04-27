#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-tenant-mgmt.sh — add-dir / remove-dir + remove-token via fingerprint.

set -u

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

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3

DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
# Wipe any test tenants from prior runs
sed -i '/^tm_/d' "$DB_ROOT/dirs.conf" 2>/dev/null || true
rm -rf "$DB_ROOT/tm_alpha" "$DB_ROOT/tm_beta" "$DB_ROOT/tm_with_obj"

$BIN start > /dev/null
sleep 0.5

echo "=== add-dir ==="
out=$($BIN query '{"mode":"add-dir","dir":"tm_alpha"}')
assert_contains "add tm_alpha" '"status":"dir_added"' "$out"

out=$($BIN query '{"mode":"db-dirs"}')
assert_contains "tm_alpha appears in db-dirs" '"tm_alpha"' "$out"

# Re-add: idempotent → dir_exists
out=$($BIN query '{"mode":"add-dir","dir":"tm_alpha"}')
assert_contains "re-add returns dir_exists" '"dir_exists"' "$out"

# Now able to create-object under tm_alpha (was rejected before)
out=$($BIN query '{"mode":"create-object","dir":"tm_alpha","object":"obj1","fields":["k:varchar:8"]}')
assert_contains "create-object under fresh tenant" '"status":"created"' "$out"

echo "=== add-dir validation ==="
for bad in "bad/name" "../escape" ".dotleader"; do
    out=$($BIN query "{\"mode\":\"add-dir\",\"dir\":\"$bad\"}" 2>&1)
    assert_contains "reject '$bad'" 'invalid dir name' "$out"
done
out=$($BIN query '{"mode":"add-dir","dir":""}' 2>&1)
assert_contains "reject empty dir" 'Missing dir' "$out"

echo "=== remove-dir empty-check ==="
# tm_alpha now has obj1 → remove with default check_empty should fail
out=$($BIN query '{"mode":"remove-dir","dir":"tm_alpha"}' 2>&1)
assert_contains "non-empty refused by default" 'not empty' "$out"

# Drop the object then remove the tenant
$BIN query '{"mode":"drop-object","dir":"tm_alpha","object":"obj1"}' > /dev/null
out=$($BIN query '{"mode":"remove-dir","dir":"tm_alpha"}')
assert_contains "remove now empty tenant" '"dir_removed"' "$out"

# Forced removal regardless of contents
out=$($BIN query '{"mode":"add-dir","dir":"tm_with_obj"}')
$BIN query '{"mode":"create-object","dir":"tm_with_obj","object":"o","fields":["k:varchar:8"]}' >/dev/null
out=$($BIN query '{"mode":"remove-dir","dir":"tm_with_obj","check_empty":"false"}')
assert_contains "force-remove non-empty tenant" '"dir_removed"' "$out"

echo "=== remove-dir not-found ==="
out=$($BIN query '{"mode":"remove-dir","dir":"never_existed"}')
assert_contains "remove unknown tenant" '"dir_not_found"' "$out"

echo "=== add-dir survives across is_valid_dir reload ==="
$BIN query '{"mode":"add-dir","dir":"tm_beta"}' > /dev/null
out=$($BIN query '{"mode":"create-object","dir":"tm_beta","object":"o","fields":["k:varchar:8"]}')
assert_contains "is_valid_dir picks up the new tenant" '"created"' "$out"
$BIN query '{"mode":"drop-object","dir":"tm_beta","object":"o"}' > /dev/null
$BIN query '{"mode":"remove-dir","dir":"tm_beta"}' > /dev/null

echo "=== remove-token via fingerprint ==="
$BIN query '{"mode":"add-token","token":"fp-test-aaaa00001111bbbb","perm":"r"}' > /dev/null
list_out=$($BIN query '{"mode":"list-tokens"}')
fp=$(echo "$list_out" | python3 -c 'import json,sys;d=json.load(sys.stdin);print([t["token"] for t in d if t["scope"]=="global"][-1])')
out=$($BIN query "{\"mode\":\"remove-token\",\"fingerprint\":\"$fp\"}")
assert_contains "remove-token by fingerprint" '"token_removed"' "$out"

# Confirm gone
list_out=$($BIN query '{"mode":"list-tokens"}')
if echo "$list_out" | grep -q "$fp"; then
    fail "fingerprint still present after remove"
else
    pass "fingerprint absent after remove"
fi

# Backward compat: remove by full token still works
$BIN query '{"mode":"add-token","token":"fp-test2-cccc22223333dddd","perm":"r"}' > /dev/null
out=$($BIN query '{"mode":"remove-token","token":"fp-test2-cccc22223333dddd"}')
assert_contains "remove-token by full token" '"token_removed"' "$out"

# Both missing → error
out=$($BIN query '{"mode":"remove-token"}' 2>&1)
assert_contains "neither token nor fingerprint → error" 'Missing token or fingerprint' "$out"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"remove-dir","dir":"tm_with_obj","check_empty":"false"}' > /dev/null 2>&1
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
