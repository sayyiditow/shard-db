#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-bulk-cas.sh — CAS on bulk operations (2026.05 item 7a)
#
# Covers:
#   * bulk-insert (JSON + CSV) with if_not_exists=true:
#       - existing keys counted as `skipped`, payload NOT overwritten
#       - response shape {"inserted":N,"skipped":M}
#       - all-skipped run still reports skipped count, no errors
#   * back-compat: bulk-insert without if_not_exists still emits {"count":N}
#     for the all-new path (existing semantics: count = new keys inserted)
#   * bulk-update with `if:[...]` (array) and `if:{...}` (object):
#       - per-record CAS, mismatches counted as skipped not errors
#       - matched-but-if-failed records keep their old payload + indexes
#   * bulk-delete (criteria) with `if`:
#       - same per-record gate, skipped vs deleted accounting
#   * dry_run + if combined: no writes
#   * back-compat: update/delete without `if` keep their old shape
#
# To stay focused on CAS, all object state is built via plain bulk-insert
# of fresh keys + single-op updates — never via bulk-insert overwrite, which
# has its own pre-existing index semantics outside this feature's scope.

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

OBJ="castest"
rm -rf "$DB_ROOT/default/$OBJ"
sed -i "/^default:$OBJ:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true

$BIN start
sleep 0.5

$BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"$OBJ\",\"splits\":16,\"max_key\":16,\"fields\":[\"name:varchar:40\",\"status:varchar:16\",\"attempt:int\"],\"indexes\":[\"name\",\"status\"]}" > /dev/null
pass "object created (name, status indexed; attempt scalar)"

# ----------------------------------------------------------------------
echo ""
echo "=== bulk-insert (JSON) — back-compat shape on fresh keys ==="

# Plain bulk-insert of 3 brand-new keys → existing semantics: {"count":3}
cat > /tmp/shard-db_seed.json <<'EOF'
[
  {"key":"k1","value":{"name":"alice","status":"pending","attempt":"0"}},
  {"key":"k2","value":{"name":"bob","status":"pending","attempt":"1"}},
  {"key":"k3","value":{"name":"carol","status":"pending","attempt":"0"}}
]
EOF
RESP=$($BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_seed.json\"}")
assert_contains "fresh insert returns {inserted:3}" '"inserted":3' "$RESP"
assert_not_contains "no skipped field on plain run" '"skipped"' "$RESP"

# ----------------------------------------------------------------------
echo ""
echo "=== bulk-insert (JSON) — if_not_exists ==="

# Re-run WITH if_not_exists, but try to overwrite k1/k2/k3. All collide → skipped.
cat > /tmp/shard-db_collide.json <<'EOF'
[
  {"key":"k1","value":{"name":"OVERWRITTEN","status":"x","attempt":"99"}},
  {"key":"k2","value":{"name":"OVERWRITTEN","status":"x","attempt":"99"}},
  {"key":"k3","value":{"name":"OVERWRITTEN","status":"x","attempt":"99"}}
]
EOF
RESP=$($BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_collide.json\",\"if_not_exists\":true}")
assert_contains "all-collision: inserted=0" '"inserted":0' "$RESP"
assert_contains "all-collision: skipped=3" '"skipped":3' "$RESP"

K1=$($BIN get default $OBJ k1)
assert_contains "k1 untouched (still alice)" '"name":"alice"' "$K1"
assert_contains "k1 attempt still 0" '"attempt":0' "$K1"

# Mixed run: 2 collide (k1, k3), 2 are new (k4, k5)
cat > /tmp/shard-db_mixed.json <<'EOF'
[
  {"key":"k1","value":{"name":"OVERWRITTEN","status":"x","attempt":"99"}},
  {"key":"k4","value":{"name":"dave","status":"pending","attempt":"0"}},
  {"key":"k5","value":{"name":"eve","status":"pending","attempt":"2"}},
  {"key":"k3","value":{"name":"OVERWRITTEN","status":"x","attempt":"99"}}
]
EOF
RESP=$($BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_mixed.json\",\"if_not_exists\":true}")
assert_contains "mixed: inserted=2 (k4, k5)" '"inserted":2' "$RESP"
assert_contains "mixed: skipped=2 (k1, k3)" '"skipped":2' "$RESP"

K3=$($BIN get default $OBJ k3)
assert_contains "k3 still carol (CAS held)" '"name":"carol"' "$K3"
K4=$($BIN get default $OBJ k4)
assert_contains "k4 dave inserted" '"name":"dave"' "$K4"
K5=$($BIN get default $OBJ k5)
assert_contains "k5 eve inserted" '"name":"eve"' "$K5"

COUNT=$($BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}")
assert_contains "after JSON CAS: 5 records" '5' "$COUNT"

# ----------------------------------------------------------------------
echo ""
echo "=== bulk-insert-delimited (CSV) — if_not_exists ==="

# All-new CSV — no CAS, expect back-compat shape
cat > /tmp/shard-db_new.csv <<'EOF'
k6|frank|pending|0
k7|grace|pending|1
k8|henry|pending|0
EOF
RESP=$($BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_new.csv\",\"delimiter\":\"|\"}")
assert_contains "CSV plain insert: {count:3}" '3' "$RESP"

# Re-run same CSV with if_not_exists → all skipped
RESP=$($BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_new.csv\",\"delimiter\":\"|\",\"if_not_exists\":true}")
assert_contains "CSV CAS rerun: inserted=0" '"inserted":0' "$RESP"
assert_contains "CSV CAS rerun: skipped=3" '"skipped":3' "$RESP"

K7=$($BIN get default $OBJ k7)
assert_contains "k7 still grace (CSV CAS held)" '"name":"grace"' "$K7"

# Mixed CSV: 1 collide (k7), 1 new (k9)
cat > /tmp/shard-db_csvmix.csv <<'EOF'
k7|OVERWRITTEN|x|99
k9|iris|pending|0
EOF
RESP=$($BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"$OBJ\",\"file\":\"/tmp/shard-db_csvmix.csv\",\"delimiter\":\"|\",\"if_not_exists\":true}")
assert_contains "CSV mixed: inserted=1 (k9)" '"inserted":1' "$RESP"
assert_contains "CSV mixed: skipped=1 (k7)" '"skipped":1' "$RESP"

K7=$($BIN get default $OBJ k7)
assert_contains "k7 still grace post-mix" '"name":"grace"' "$K7"
K9=$($BIN get default $OBJ k9)
assert_contains "k9 iris inserted" '"name":"iris"' "$K9"

COUNT=$($BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}")
assert_contains "after CSV CAS: 9 records total" '9' "$COUNT"

# ----------------------------------------------------------------------
echo ""
echo "=== bulk-update with if (CAS guard, array form) ==="

# State: 9 records, all status=pending.
# attempt=0: k1, k3, k4, k6, k8, k9 (6 records)
# attempt=1: k2, k7                 (2 records)
# attempt=2: k5                     (1 record)

# Promote pending → processing IF attempt=0. Array form.
RESP=$($BIN query "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}],\"value\":{\"status\":\"processing\"},\"if\":[{\"field\":\"attempt\",\"op\":\"eq\",\"value\":\"0\"}]}")
assert_contains "matched=9 (all pending)"        '"matched":9' "$RESP"
assert_contains "updated=6 (attempt=0 winners)"  '"updated":6' "$RESP"
assert_contains "skipped=3 (CAS losers)"         '"skipped":3' "$RESP"

# Index integrity: status=processing must be 6, status=pending must be 3
N_PROC=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}]}")
assert_contains "index: 6 processing" '6' "$N_PROC"
N_PEND=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}")
assert_contains "index: 3 still pending" '3' "$N_PEND"

# Spot-check payloads
K1=$($BIN get default $OBJ k1)
assert_contains "k1 processing (CAS won)" '"status":"processing"' "$K1"
K2=$($BIN get default $OBJ k2)
assert_contains "k2 still pending (attempt=1 lost CAS)" '"status":"pending"' "$K2"
K5=$($BIN get default $OBJ k5)
assert_contains "k5 still pending (attempt=2 lost CAS)" '"status":"pending"' "$K5"

# ----------------------------------------------------------------------
echo ""
echo "=== bulk-update with if (CAS guard, object form) ==="

# 3 still pending: k2, k5, k7 (attempts 1, 2, 1).
# Move pending → archived IF attempt=1 (object form). k2+k7 should match,
# k5 should be skipped.
RESP=$($BIN query "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}],\"value\":{\"status\":\"archived\"},\"if\":{\"attempt\":\"1\"}}")
assert_contains "object-form if: matched=3" '"matched":3' "$RESP"
assert_contains "object-form if: updated=2" '"updated":2' "$RESP"
assert_contains "object-form if: skipped=1" '"skipped":1' "$RESP"

K2=$($BIN get default $OBJ k2)
assert_contains "k2 archived (attempt=1)" '"status":"archived"' "$K2"
K5=$($BIN get default $OBJ k5)
assert_contains "k5 still pending (attempt=2 lost CAS)" '"status":"pending"' "$K5"

N_PEND=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}")
assert_contains "1 pending left (k5)" '1' "$N_PEND"
N_ARCH=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"archived\"}]}")
assert_contains "2 archived" '2' "$N_ARCH"

# ----------------------------------------------------------------------
echo ""
echo "=== bulk-update back-compat (no if) ==="

# Force-promote k5 pending → done with no CAS guard
RESP=$($BIN query "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}],\"value\":{\"status\":\"done\"}}")
assert_contains "back-compat update: matched=1" '"matched":1' "$RESP"
assert_contains "back-compat update: updated=1" '"updated":1' "$RESP"
assert_contains "back-compat update: skipped=0" '"skipped":0' "$RESP"

K5=$($BIN get default $OBJ k5)
assert_contains "k5 now done (force-promoted)" '"status":"done"' "$K5"

# ----------------------------------------------------------------------
echo ""
echo "=== bulk-delete with if (CAS guard) ==="

# Reset attempt so we can drive a deterministic delete:
# k1, k3 → attempt=5 ; rest stay as-is.
$BIN query "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"$OBJ\",\"key\":\"k1\",\"value\":{\"attempt\":\"5\"}}" > /dev/null
$BIN query "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"$OBJ\",\"key\":\"k3\",\"value\":{\"attempt\":\"5\"}}" > /dev/null

# Currently processing: k1, k3, k4, k6, k8, k9 (6 records).
# Delete WHERE status=processing IF attempt=5 → only k1+k3 should drop.
RESP=$($BIN query "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}],\"if\":[{\"field\":\"attempt\",\"op\":\"eq\",\"value\":\"5\"}]}")
assert_contains "delete CAS: matched=6"   '"matched":6' "$RESP"
assert_contains "delete CAS: deleted=2"   '"deleted":2' "$RESP"
assert_contains "delete CAS: skipped=4"   '"skipped":4' "$RESP"

GET_K1=$($BIN get default $OBJ k1 2>&1 || true)
assert_contains "k1 gone" "Not found" "$GET_K1"
GET_K3=$($BIN get default $OBJ k3 2>&1 || true)
assert_contains "k3 gone" "Not found" "$GET_K3"

K4=$($BIN get default $OBJ k4)
assert_contains "k4 survived (attempt=0 lost CAS)" '"name":"dave"' "$K4"

N_PROC=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}]}")
assert_contains "4 processing left after CAS-delete" '4' "$N_PROC"

# ----------------------------------------------------------------------
echo ""
echo "=== bulk-delete back-compat (no if) ==="

RESP=$($BIN query "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"archived\"}]}")
assert_contains "back-compat delete: matched=2" '"matched":2' "$RESP"
assert_contains "back-compat delete: deleted=2" '"deleted":2' "$RESP"
assert_contains "back-compat delete: skipped=0" '"skipped":0' "$RESP"

# ----------------------------------------------------------------------
echo ""
echo "=== dry_run + if combined ==="

SIZE_BEFORE=$($BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}")

RESP=$($BIN query "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}],\"value\":{\"status\":\"WOULD-NOT-WRITE\"},\"if\":[{\"field\":\"attempt\",\"op\":\"eq\",\"value\":\"0\"}],\"dry_run\":true}")
assert_contains "dry_run shape" '"dry_run":true' "$RESP"
assert_contains "dry_run: updated=0" '"updated":0' "$RESP"

RESP=$($BIN query "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"processing\"}],\"if\":[{\"field\":\"attempt\",\"op\":\"eq\",\"value\":\"0\"}],\"dry_run\":true}")
assert_contains "dry_run delete shape" '"dry_run":true' "$RESP"
assert_contains "dry_run delete: deleted=0" '"deleted":0' "$RESP"

SIZE_AFTER=$($BIN query "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"$OBJ\"}")
assert_contains "size unchanged after dry runs" "$SIZE_BEFORE" "$SIZE_AFTER"

NONE=$($BIN query "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"$OBJ\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"WOULD-NOT-WRITE\"}]}")
assert_contains "sentinel not written" '0' "$NONE"

# ----------------------------------------------------------------------
echo ""
echo "=== CLEANUP ==="
$BIN query "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"$OBJ\"}" > /dev/null
rm -rf "$DB_ROOT/default/$OBJ"
sed -i "/^default:$OBJ:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
rm -f /tmp/shard-db_seed.json /tmp/shard-db_collide.json /tmp/shard-db_mixed.json /tmp/shard-db_new.csv /tmp/shard-db_csvmix.csv
$BIN stop

echo ""
echo "=== RESULT: $PASS passed, $FAIL failed, $TOTAL total ==="
exit $FAIL
