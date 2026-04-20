#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-per-tenant-auth.sh — per-tenant token scoping.
#
# Covers: global (admin) token works anywhere, tenant token works only in its
# dir, tenant token rejected on admin commands, bootstrap from empty state,
# revocation, list-tokens shape, persistence across restarts.

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
grep -q "^tenant_a$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "tenant_a" >> "$DB_ROOT/dirs.conf"
grep -q "^tenant_b$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "tenant_b" >> "$DB_ROOT/dirs.conf"

# Clean any existing auth files / test objects
rm -f "$DB_ROOT/tokens.conf" "$DB_ROOT/tenant_a/tokens.conf" "$DB_ROOT/tenant_b/tokens.conf"
rm -rf "$DB_ROOT/tenant_a" "$DB_ROOT/tenant_b"
sed -i '/^tenant_a:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
sed -i '/^tenant_b:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
mkdir -p "$DB_ROOT/tenant_a" "$DB_ROOT/tenant_b"

# Temporarily clear allowed_ips.conf AND enable DISABLE_LOCALHOST_TRUST —
# the server otherwise short-circuits auth for 127.0.0.1/::1, which would
# hide every token test. Back up everything, restore at cleanup.
if [ -e "$DB_ROOT/allowed_ips.conf" ]; then
    mv "$DB_ROOT/allowed_ips.conf" "$DB_ROOT/allowed_ips.conf.testbak"
fi
: > "$DB_ROOT/allowed_ips.conf"
# Append the strict-auth flag to db.env for the duration of this test.
cp db.env db.env.testbak
echo "export DISABLE_LOCALHOST_TRUST=1" >> db.env

# Seed global admin token BEFORE starting (so bootstrap path is covered
# elsewhere; here we want the server to enforce auth from the first request).
GLOBAL_TOKEN="sdb_global_admin_$(date +%s)"
echo "$GLOBAL_TOKEN" > "$DB_ROOT/tokens.conf"

$BIN start > /dev/null
sleep 0.5

$BIN query "{\"mode\":\"create-object\",\"dir\":\"tenant_a\",\"object\":\"users\",\"auth\":\"$GLOBAL_TOKEN\",\"splits\":2,\"max_key\":16,\"fields\":[\"name:varchar:32\"]}" > /dev/null
$BIN query "{\"mode\":\"create-object\",\"dir\":\"tenant_b\",\"object\":\"users\",\"auth\":\"$GLOBAL_TOKEN\",\"splits\":2,\"max_key\":16,\"fields\":[\"name:varchar:32\"]}" > /dev/null
$BIN query "{\"mode\":\"insert\",\"dir\":\"tenant_a\",\"object\":\"users\",\"key\":\"a1\",\"value\":{\"name\":\"Alice\"},\"auth\":\"$GLOBAL_TOKEN\"}" > /dev/null
$BIN query "{\"mode\":\"insert\",\"dir\":\"tenant_b\",\"object\":\"users\",\"key\":\"b1\",\"value\":{\"name\":\"Bob\"},\"auth\":\"$GLOBAL_TOKEN\"}" > /dev/null

echo ""
echo "=== Global token: works in any dir + admin commands ==="
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\",\"key\":\"a1\",\"auth\":\"$GLOBAL_TOKEN\"}")
assert_contains "global reads tenant_a"          '"name":"Alice"'        "$GOT"
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tenant_b\",\"object\":\"users\",\"key\":\"b1\",\"auth\":\"$GLOBAL_TOKEN\"}")
assert_contains "global reads tenant_b"          '"name":"Bob"'          "$GOT"
GOT=$($BIN query "{\"mode\":\"stats\",\"auth\":\"$GLOBAL_TOKEN\"}")
assert_contains "global runs stats (admin)"       '"uptime_ms"'           "$GOT"
GOT=$($BIN query "{\"mode\":\"db-dirs\",\"auth\":\"$GLOBAL_TOKEN\"}")
assert_contains "global runs db-dirs (admin)"    'tenant_a'              "$GOT"

echo ""
echo "=== Missing / wrong token → auth failed (short, uninformative) ==="
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\",\"key\":\"a1\"}")
assert_contains "no token rejected"              '"error":"auth failed"' "$GOT"
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\",\"key\":\"a1\",\"auth\":\"wrong\"}")
assert_contains "wrong token rejected"           '"error":"auth failed"' "$GOT"
assert_not_contains "error does not leak token"  'wrong'                 "$GOT"

echo ""
echo "=== add-token with scope persists to tenant-local file ==="
TENANT_A_TOKEN="sdb_tenant_a_$(date +%s)_v1"
GOT=$($BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"$TENANT_A_TOKEN\",\"dir\":\"tenant_a\"}")
assert_contains "add-token (tenant_a) status"    '"status":"token_added"' "$GOT"
assert_contains "add-token echoes scope"         '"scope":"tenant_a"'    "$GOT"

# File side-effect: should be in $DB_ROOT/tenant_a/tokens.conf, NOT the global file.
if grep -q "$TENANT_A_TOKEN" "$DB_ROOT/tenant_a/tokens.conf" 2>/dev/null; then
    pass "tenant token saved in tenant_a/tokens.conf"
else
    fail "tenant token not saved in tenant_a/tokens.conf"
fi
if grep -q "$TENANT_A_TOKEN" "$DB_ROOT/tokens.conf" 2>/dev/null; then
    fail "tenant token leaked into global tokens.conf"
else
    pass "tenant token NOT in global tokens.conf"
fi

# Add a tenant_b token for cross-tenant tests.
TENANT_B_TOKEN="sdb_tenant_b_$(date +%s)_v1"
$BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"$TENANT_B_TOKEN\",\"dir\":\"tenant_b\"}" > /dev/null

echo ""
echo "=== Tenant token: works in its own dir ==="
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\",\"key\":\"a1\",\"auth\":\"$TENANT_A_TOKEN\"}")
assert_contains "tenant_a token reads tenant_a"  '"name":"Alice"'        "$GOT"

echo ""
echo "=== Tenant token: rejected on a different dir ==="
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tenant_b\",\"object\":\"users\",\"key\":\"b1\",\"auth\":\"$TENANT_A_TOKEN\"}")
assert_contains "tenant_a token rejected on tenant_b" '"error":"auth failed"' "$GOT"
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\",\"key\":\"a1\",\"auth\":\"$TENANT_B_TOKEN\"}")
assert_contains "tenant_b token rejected on tenant_a" '"error":"auth failed"' "$GOT"

echo ""
echo "=== Tenant token: rejected on admin commands ==="
GOT=$($BIN query "{\"mode\":\"stats\",\"auth\":\"$TENANT_A_TOKEN\"}")
assert_contains "tenant token rejected on stats"        '"error":"auth failed"' "$GOT"
GOT=$($BIN query "{\"mode\":\"db-dirs\",\"auth\":\"$TENANT_A_TOKEN\"}")
assert_contains "tenant token rejected on db-dirs"      '"error":"auth failed"' "$GOT"
GOT=$($BIN query "{\"mode\":\"create-object\",\"dir\":\"tenant_a\",\"object\":\"x\",\"auth\":\"$TENANT_A_TOKEN\",\"splits\":2,\"max_key\":16,\"fields\":[\"n:int\"]}")
assert_contains "tenant token rejected on create-object" '"error":"auth failed"' "$GOT"
GOT=$($BIN query "{\"mode\":\"add-token\",\"auth\":\"$TENANT_A_TOKEN\",\"token\":\"new\"}")
# Note: add-token has its own early auth gate that treats it as admin-only;
# it returns "unauthorized" from that gate (stricter than the main dispatcher).
assert_contains "tenant token rejected on add-token"    '"error"'               "$GOT"

echo ""
echo "=== list-tokens: shows fingerprint + scope ==="
GOT=$($BIN query "{\"mode\":\"list-tokens\",\"auth\":\"$GLOBAL_TOKEN\"}")
assert_contains "list-tokens shows global scope"        '"scope":"global"'      "$GOT"
assert_contains "list-tokens shows tenant_a scope"      '"scope":"tenant_a"'    "$GOT"
assert_contains "list-tokens shows tenant_b scope"      '"scope":"tenant_b"'    "$GOT"
assert_not_contains "full token never printed"          "$TENANT_A_TOKEN"       "$GOT"

echo ""
echo "=== remove-token rewrites correct file ==="
# Remove the tenant_a token; tenant_a's tokens.conf should become empty, global unchanged.
$BIN query "{\"mode\":\"remove-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"$TENANT_A_TOKEN\"}" > /dev/null
if [ ! -s "$DB_ROOT/tenant_a/tokens.conf" ]; then
    pass "tenant_a tokens.conf empty after removal"
else
    fail "tenant_a tokens.conf should be empty (was: $(cat "$DB_ROOT/tenant_a/tokens.conf"))"
fi
if grep -q "$GLOBAL_TOKEN" "$DB_ROOT/tokens.conf"; then
    pass "global tokens.conf still has admin token"
else
    fail "global tokens.conf lost admin token"
fi

# After removal, the former tenant_a token fails auth.
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\",\"key\":\"a1\",\"auth\":\"$TENANT_A_TOKEN\"}")
assert_contains "revoked tenant token rejected"         '"error":"auth failed"' "$GOT"

echo ""
echo "=== Persistence: restart picks up per-tenant tokens from disk ==="
$BIN stop > /dev/null
sleep 0.3
# Manually write a tenant_b token directly to the file, restart, verify it works.
RESTART_TOKEN="sdb_restart_test_$(date +%s)"
echo "$RESTART_TOKEN" > "$DB_ROOT/tenant_b/tokens.conf"
$BIN start > /dev/null
sleep 0.5
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tenant_b\",\"object\":\"users\",\"key\":\"b1\",\"auth\":\"$RESTART_TOKEN\"}")
assert_contains "disk-written tenant token loaded on start" '"name":"Bob"' "$GOT"
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\",\"key\":\"a1\",\"auth\":\"$RESTART_TOKEN\"}")
assert_contains "disk-written tenant token scoped to its dir" '"error":"auth failed"' "$GOT"

echo ""
echo "=== CLEANUP ==="
$BIN stop > /dev/null
rm -f "$DB_ROOT/tokens.conf" "$DB_ROOT/tenant_a/tokens.conf" "$DB_ROOT/tenant_b/tokens.conf"
rm -rf "$DB_ROOT/tenant_a" "$DB_ROOT/tenant_b"
sed -i '/^tenant_a:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
sed -i '/^tenant_b:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
sed -i '/^tenant_a$/d' "$DB_ROOT/dirs.conf" 2>/dev/null || true
sed -i '/^tenant_b$/d' "$DB_ROOT/dirs.conf" 2>/dev/null || true
# Restore allowed_ips.conf so the main test suite's localhost bypass works again.
rm -f "$DB_ROOT/allowed_ips.conf"
if [ -e "$DB_ROOT/allowed_ips.conf.testbak" ]; then
    mv "$DB_ROOT/allowed_ips.conf.testbak" "$DB_ROOT/allowed_ips.conf"
fi
# Restore db.env.
if [ -e db.env.testbak ]; then
    mv db.env.testbak db.env
fi

echo ""
echo "================================"
echo "  $PASS passed, $FAIL failed ($TOTAL total)"
echo "================================"
[ "$FAIL" -eq 0 ]
