#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-token-perms.sh — per-object tokens + read/write/admin permissions.
#
# Covers:
# - add-token with dir+object (object scope) writes to $DB_ROOT/<dir>/<obj>/tokens.conf
# - perm=r / rw / rwx enforced per operation class
# - object token rejected on any other (dir, obj) combo
# - tenant-rwx can create-object + schema mutations + index mgmt on its dir
# - object-rwx can do object-scope admin on its own object only
# - tenant/object rwx CANNOT run server-admin (stats, add-token)
# - empty / missing perm suffix in tokens.conf = admin (backward compat)
# - invalid perm suffix rejected at add-token and at file-load
# - TOKEN_CAP configurable (smoke test; full scaling test is manual)

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
grep -q "^tp_acme$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "tp_acme" >> "$DB_ROOT/dirs.conf"
grep -q "^tp_beta$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "tp_beta" >> "$DB_ROOT/dirs.conf"

rm -rf "$DB_ROOT/tp_acme" "$DB_ROOT/tp_beta"
sed -i '/^tp_acme:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
sed -i '/^tp_beta:/d'  "$DB_ROOT/schema.conf" 2>/dev/null || true
mkdir -p "$DB_ROOT/tp_acme" "$DB_ROOT/tp_beta"

# Strict auth for tests (bypass loopback trust).
if [ -e "$DB_ROOT/allowed_ips.conf" ]; then
    mv "$DB_ROOT/allowed_ips.conf" "$DB_ROOT/allowed_ips.conf.tpbak"
fi
: > "$DB_ROOT/allowed_ips.conf"
cp db.env db.env.tpbak
echo "export DISABLE_LOCALHOST_TRUST=1" >> db.env

# Pre-seed a global admin token so the server enforces from first request.
GLOBAL_TOKEN="sdb_tp_admin_$(date +%s)"
echo "$GLOBAL_TOKEN" > "$DB_ROOT/tokens.conf"

$BIN start > /dev/null
sleep 0.5

$BIN query "{\"mode\":\"create-object\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"auth\":\"$GLOBAL_TOKEN\",\"splits\":2,\"max_key\":16,\"fields\":[\"status:varchar:16\",\"amount:int\"]}" > /dev/null
$BIN query "{\"mode\":\"create-object\",\"dir\":\"tp_acme\",\"object\":\"users\",\"auth\":\"$GLOBAL_TOKEN\",\"splits\":2,\"max_key\":16,\"fields\":[\"name:varchar:32\"]}" > /dev/null
$BIN query "{\"mode\":\"create-object\",\"dir\":\"tp_beta\",\"object\":\"orders\",\"auth\":\"$GLOBAL_TOKEN\",\"splits\":2,\"max_key\":16,\"fields\":[\"status:varchar:16\"]}" > /dev/null
$BIN query "{\"mode\":\"insert\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"key\":\"o1\",\"value\":{\"status\":\"paid\",\"amount\":100},\"auth\":\"$GLOBAL_TOKEN\"}" > /dev/null
$BIN query "{\"mode\":\"insert\",\"dir\":\"tp_acme\",\"object\":\"users\",\"key\":\"u1\",\"value\":{\"name\":\"Alice\"},\"auth\":\"$GLOBAL_TOKEN\"}" > /dev/null
$BIN query "{\"mode\":\"insert\",\"dir\":\"tp_beta\",\"object\":\"orders\",\"key\":\"b1\",\"value\":{\"status\":\"paid\"},\"auth\":\"$GLOBAL_TOKEN\"}" > /dev/null

echo ""
echo "=== perm=r : read ops work, writes rejected ==="
READ_TOKEN="sdb_tp_read_$(date +%s)"
$BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"$READ_TOKEN\",\"dir\":\"tp_acme\",\"perm\":\"r\"}" > /dev/null
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"key\":\"o1\",\"auth\":\"$READ_TOKEN\"}")
assert_contains "perm=r reads ok"                          '"status":"paid"'       "$GOT"
GOT=$($BIN query "{\"mode\":\"insert\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"key\":\"new\",\"value\":{\"status\":\"x\",\"amount\":1},\"auth\":\"$READ_TOKEN\"}")
assert_contains "perm=r insert rejected"                    '"error":"auth failed"' "$GOT"
GOT=$($BIN query "{\"mode\":\"delete\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"key\":\"o1\",\"auth\":\"$READ_TOKEN\"}")
assert_contains "perm=r delete rejected"                    '"error":"auth failed"' "$GOT"
GOT=$($BIN query "{\"mode\":\"find\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"criteria\":[],\"auth\":\"$READ_TOKEN\"}")
assert_contains "perm=r find ok"                            '"o1"'                  "$GOT"

echo ""
echo "=== perm=rw : reads+writes work, admin rejected ==="
RW_TOKEN="sdb_tp_rw_$(date +%s)"
$BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"$RW_TOKEN\",\"dir\":\"tp_acme\",\"perm\":\"rw\"}" > /dev/null
GOT=$($BIN query "{\"mode\":\"insert\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"key\":\"o2\",\"value\":{\"status\":\"pending\",\"amount\":50},\"auth\":\"$RW_TOKEN\"}")
assert_contains "perm=rw insert ok"                         '"status":"inserted"'   "$GOT"
GOT=$($BIN query "{\"mode\":\"delete\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"key\":\"o2\",\"auth\":\"$RW_TOKEN\"}")
assert_contains "perm=rw delete ok"                         '"status":"deleted"'    "$GOT"
# Admin ops rejected on rw token
GOT=$($BIN query "{\"mode\":\"create-object\",\"dir\":\"tp_acme\",\"object\":\"newthing\",\"auth\":\"$RW_TOKEN\",\"splits\":2,\"max_key\":16,\"fields\":[\"n:int\"]}")
assert_contains "perm=rw create-object rejected"            '"error":"auth failed"' "$GOT"
GOT=$($BIN query "{\"mode\":\"add-field\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"fields\":[\"note:varchar:32\"],\"auth\":\"$RW_TOKEN\"}")
assert_contains "perm=rw add-field rejected"                '"error":"auth failed"' "$GOT"

echo ""
echo "=== perm=rwx at tenant scope : data + dir/object admin, not server admin ==="
TADMIN_TOKEN="sdb_tp_tadmin_$(date +%s)"
$BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"$TADMIN_TOKEN\",\"dir\":\"tp_acme\",\"perm\":\"rwx\"}" > /dev/null
# tenant-scope admin: can create-object within its dir
GOT=$($BIN query "{\"mode\":\"create-object\",\"dir\":\"tp_acme\",\"object\":\"widgets\",\"auth\":\"$TADMIN_TOKEN\",\"splits\":2,\"max_key\":16,\"fields\":[\"n:int\"]}")
assert_contains "tenant-rwx create-object on own dir"       '"status":"created"'    "$GOT"
# object-scope admin: can add-field on own dir
GOT=$($BIN query "{\"mode\":\"add-field\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"fields\":[\"note:varchar:32\"],\"auth\":\"$TADMIN_TOKEN\"}")
assert_contains "tenant-rwx add-field on own dir"           '"status":'             "$GOT"
# Server admin: rejected
GOT=$($BIN query "{\"mode\":\"stats\",\"auth\":\"$TADMIN_TOKEN\"}")
assert_contains "tenant-rwx stats rejected"                 '"error":"auth failed"' "$GOT"
GOT=$($BIN query "{\"mode\":\"db-dirs\",\"auth\":\"$TADMIN_TOKEN\"}")
assert_contains "tenant-rwx db-dirs rejected"               '"error":"auth failed"' "$GOT"
# Token management: ALWAYS admin-only regardless of target
GOT=$($BIN query "{\"mode\":\"add-token\",\"auth\":\"$TADMIN_TOKEN\",\"token\":\"foo\",\"dir\":\"tp_acme\"}")
assert_contains "tenant-rwx cannot add-token (ever)"        '"error":"unauthorized"' "$GOT"
# Cross-dir rejected
GOT=$($BIN query "{\"mode\":\"create-object\",\"dir\":\"tp_beta\",\"object\":\"x\",\"auth\":\"$TADMIN_TOKEN\",\"splits\":2,\"max_key\":16,\"fields\":[\"n:int\"]}")
assert_contains "tenant-rwx rejected on other dir"          '"error":"auth failed"' "$GOT"

echo ""
echo "=== add-token with object scope persists to <dir>/<obj>/tokens.conf ==="
OBJ_TOKEN="sdb_tp_obj_$(date +%s)"
GOT=$($BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"$OBJ_TOKEN\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"perm\":\"rw\"}")
assert_contains "object-scoped add-token success"           '"status":"token_added"' "$GOT"
assert_contains "scope shows dir/obj"                       '"scope":"tp_acme/orders"' "$GOT"
if [ -f "$DB_ROOT/tp_acme/orders/tokens.conf" ] && grep -q "$OBJ_TOKEN" "$DB_ROOT/tp_acme/orders/tokens.conf"; then
    pass "object-scoped token saved in <dir>/<obj>/tokens.conf"
else
    fail "object-scoped token not saved (expected in $DB_ROOT/tp_acme/orders/tokens.conf)"
fi
# Has the :rw suffix in the file
if grep -q "$OBJ_TOKEN:rw" "$DB_ROOT/tp_acme/orders/tokens.conf" 2>/dev/null; then
    pass "file line has :rw suffix"
else
    fail "file line missing :rw suffix (actual: $(cat "$DB_ROOT/tp_acme/orders/tokens.conf"))"
fi

echo ""
echo "=== object-rw token: only works on exact (dir, object) ==="
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"key\":\"o1\",\"auth\":\"$OBJ_TOKEN\"}")
assert_contains "object token reads target"                 '"status":"paid"'       "$GOT"
GOT=$($BIN query "{\"mode\":\"insert\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"key\":\"o3\",\"value\":{\"status\":\"x\",\"amount\":1},\"auth\":\"$OBJ_TOKEN\"}")
assert_contains "object token writes target (rw)"           '"status":"inserted"'   "$GOT"
# Same tenant, different object: rejected
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tp_acme\",\"object\":\"users\",\"key\":\"u1\",\"auth\":\"$OBJ_TOKEN\"}")
assert_contains "object token rejected on sibling object"   '"error":"auth failed"' "$GOT"
# Different tenant entirely: rejected
GOT=$($BIN query "{\"mode\":\"get\",\"dir\":\"tp_beta\",\"object\":\"orders\",\"key\":\"b1\",\"auth\":\"$OBJ_TOKEN\"}")
assert_contains "object token rejected on other tenant"     '"error":"auth failed"' "$GOT"

echo ""
echo "=== object-rwx can do object-scope admin on target; not server admin ==="
OBJX_TOKEN="sdb_tp_objx_$(date +%s)"
$BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"$OBJX_TOKEN\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"perm\":\"rwx\"}" > /dev/null
# Add-index on target object: allowed
GOT=$($BIN query "{\"mode\":\"add-index\",\"dir\":\"tp_acme\",\"object\":\"orders\",\"field\":\"status\",\"auth\":\"$OBJX_TOKEN\"}")
assert_contains "object-rwx add-index on target"            '"status":'             "$GOT"
# But NOT create-object (tenant-scope admin, object-rwx is too narrow)
GOT=$($BIN query "{\"mode\":\"create-object\",\"dir\":\"tp_acme\",\"object\":\"something_new\",\"auth\":\"$OBJX_TOKEN\",\"splits\":2,\"max_key\":16,\"fields\":[\"n:int\"]}")
assert_contains "object-rwx cannot create-object (tenant-scope)" '"error":"auth failed"' "$GOT"

echo ""
echo "=== default perm on add-token is 'rw' (not 'rwx') ==="
DEFAULT_TOKEN="sdb_tp_default_$(date +%s)"
$BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"$DEFAULT_TOKEN\",\"dir\":\"tp_acme\"}" > /dev/null
# Verify: file line has :rw suffix (not bare, which would mean rwx)
if grep -q "$DEFAULT_TOKEN:rw$" "$DB_ROOT/tp_acme/tokens.conf" 2>/dev/null; then
    pass "default perm written as :rw"
else
    fail "default perm not :rw (actual: $(grep "$DEFAULT_TOKEN" "$DB_ROOT/tp_acme/tokens.conf"))"
fi

echo ""
echo "=== invalid perm rejected at add-token ==="
GOT=$($BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"bad1\",\"dir\":\"tp_acme\",\"perm\":\"x\"}")
assert_contains "perm=x rejected"                           'invalid perm'          "$GOT"
GOT=$($BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"bad2\",\"dir\":\"tp_acme\",\"perm\":\"rx\"}")
assert_contains "perm=rx rejected"                          'invalid perm'          "$GOT"

echo ""
echo "=== object scope requires dir ==="
GOT=$($BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"bad3\",\"object\":\"orders\"}")
assert_contains "object-without-dir rejected"               'object scope requires dir' "$GOT"

echo ""
echo "=== object must exist ==="
GOT=$($BIN query "{\"mode\":\"add-token\",\"auth\":\"$GLOBAL_TOKEN\",\"token\":\"bad4\",\"dir\":\"tp_acme\",\"object\":\"nonexistent\"}")
assert_contains "nonexistent object rejected"               'object not found'      "$GOT"

echo ""
echo "=== list-tokens shows all three scope forms + perm ==="
GOT=$($BIN query "{\"mode\":\"list-tokens\",\"auth\":\"$GLOBAL_TOKEN\"}")
assert_contains "list shows global"                         '"scope":"global"'      "$GOT"
assert_contains "list shows tenant"                         '"scope":"tp_acme"'     "$GOT"
assert_contains "list shows object"                         '"scope":"tp_acme/orders"' "$GOT"
assert_contains "list shows perm r"                         '"perm":"r"'            "$GOT"
assert_contains "list shows perm rw"                        '"perm":"rw"'           "$GOT"
assert_contains "list shows perm rwx"                       '"perm":"rwx"'          "$GOT"

echo ""
echo "=== backward compat: plain token line (no suffix) = rwx ==="
$BIN stop > /dev/null
sleep 0.3
# Direct disk write with NO suffix — should become rwx on load
LEGACY_TOKEN="sdb_tp_legacy_$(date +%s)"
echo "$LEGACY_TOKEN" > "$DB_ROOT/tokens.conf"   # bare line, no :perm
# Need to also preserve the admin token so we can manage auth
echo "$GLOBAL_TOKEN" >> "$DB_ROOT/tokens.conf"
$BIN start > /dev/null
sleep 0.5
GOT=$($BIN query "{\"mode\":\"stats\",\"auth\":\"$LEGACY_TOKEN\"}")
assert_contains "bare-line token = admin (runs stats)"      '"uptime_ms"'           "$GOT"

echo ""
echo "=== TOKEN_CAP configurable (smoke) ==="
$BIN stop > /dev/null; sleep 0.3
echo "export TOKEN_CAP=4096" >> db.env
$BIN start > /dev/null; sleep 0.3
GOT=$($BIN query "{\"mode\":\"stats\",\"auth\":\"$GLOBAL_TOKEN\"}")
assert_contains "server runs with TOKEN_CAP=4096"           '"uptime_ms"'           "$GOT"

echo ""
echo "=== CLEANUP ==="
$BIN stop > /dev/null
rm -rf "$DB_ROOT/tp_acme" "$DB_ROOT/tp_beta" "$DB_ROOT/tokens.conf"
sed -i '/^tp_acme:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
sed -i '/^tp_beta:/d'  "$DB_ROOT/schema.conf" 2>/dev/null || true
sed -i '/^tp_acme$/d' "$DB_ROOT/dirs.conf" 2>/dev/null || true
sed -i '/^tp_beta$/d'  "$DB_ROOT/dirs.conf" 2>/dev/null || true
rm -f "$DB_ROOT/allowed_ips.conf"
if [ -e "$DB_ROOT/allowed_ips.conf.tpbak" ]; then
    mv "$DB_ROOT/allowed_ips.conf.tpbak" "$DB_ROOT/allowed_ips.conf"
fi
if [ -e db.env.tpbak ]; then
    mv db.env.tpbak db.env
fi

echo ""
echo "================================"
echo "  $PASS passed, $FAIL failed ($TOTAL total)"
echo "================================"
[ "$FAIL" -eq 0 ]
