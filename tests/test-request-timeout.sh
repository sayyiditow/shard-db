#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-request-timeout.sh — per-request "timeout_ms" override.
#
# Covers:
# - timeout_ms on a long scan triggers the deadline and returns query_timeout
# - timeout_ms: 0 / absent falls back to global TIMEOUT (unchanged behaviour)
# - per-request timeout does not leak across threads: a slow query with tight
#   timeout_ms followed by a normal query without it does not inherit the
#   tight value

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

rm -rf "$DB_ROOT/default/rt_big"
sed -i '/^default:rt_big:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true

$BIN start > /dev/null
sleep 0.5

# Seed a large enough object so a full scan takes clearly measurable time.
# Clock resolution is CLOCK_MONOTONIC_COARSE (~4 ms on Linux), so we aim for
# scans in the 50-100 ms range and set timeout_ms=10 to get a reliable trip
# without needing millisecond precision. Sized for ~16-way parallel scan —
# smaller counts complete in <10 ms on multicore and mask the deadline.
$BIN query '{"mode":"create-object","dir":"default","object":"rt_big","splits":16,"max_key":16,"fields":["status:varchar:16","amount:int","note:varchar:32"]}' > /dev/null

# Bulk-insert 1.5M records — scan of this on a full filter reliably takes
# ~100 ms under full shard-scan parallelism.
SEED=$(mktemp)
{
  printf '['
  for i in $(seq 1 1500000); do
    if [ $i -gt 1 ]; then printf ','; fi
    printf '{"key":"k%d","value":{"status":"paid","amount":%d,"note":"note_for_record_%d_padding_x"}}' "$i" $((i % 1000)) "$i"
  done
  printf ']'
} > "$SEED"
$BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_big\",\"file\":\"$SEED\"}" > /dev/null
rm -f "$SEED"

COUNT=$($BIN count default rt_big | sed 's/.*"count":\([0-9]*\).*/\1/')
echo "  seeded: $COUNT records"

echo ""
echo "=== timeout_ms trips on count / aggregate / bulk-delete / bulk-update ==="
# These paths all emit {"error":"query_timeout"} cleanly when the deadline fires.
# count: full-scan with criteria that matches ~everything → 100-200ms of work.
GOT=$($BIN query '{"mode":"count","dir":"default","object":"rt_big","criteria":[{"field":"note","op":"contains","value":"record"}],"timeout_ms":10}' 2>&1)
assert_contains "count tight timeout trips"                '"query_timeout"' "$GOT"

# aggregate: group by a high-cardinality field → every match creates a bucket.
GOT=$($BIN query '{"mode":"aggregate","dir":"default","object":"rt_big","aggregates":[{"fn":"count","alias":"n"}],"group_by":["note"],"timeout_ms":10}' 2>&1)
assert_contains "aggregate tight timeout trips"            '"query_timeout"' "$GOT"

# bulk-update (dry_run): scans + would-update every match, stops on deadline.
GOT=$($BIN query '{"mode":"bulk-update","dir":"default","object":"rt_big","criteria":[{"field":"note","op":"contains","value":"record"}],"value":{"status":"updated"},"dry_run":true,"timeout_ms":10}' 2>&1)
assert_contains "bulk-update tight timeout trips"          '"query_timeout"' "$GOT"

# bulk-delete (dry_run): same pattern.
GOT=$($BIN query '{"mode":"bulk-delete","dir":"default","object":"rt_big","criteria":[{"field":"note","op":"contains","value":"record"}],"dry_run":true,"timeout_ms":10}' 2>&1)
assert_contains "bulk-delete tight timeout trips"          '"query_timeout"' "$GOT"

echo ""
echo "=== timeout_ms: 0 → falls back to global (completes normally) ==="
GOT=$($BIN query '{"mode":"count","dir":"default","object":"rt_big","criteria":[{"field":"amount","op":"eq","value":"42"}],"timeout_ms":0}')
assert_not_contains "timeout_ms:0 does not trip"           '"query_timeout"' "$GOT"
assert_contains "timeout_ms:0 returns result"              '1500'            "$GOT"

echo ""
echo "=== timeout_ms absent → same as 0 (global fallback) ==="
GOT=$($BIN query '{"mode":"count","dir":"default","object":"rt_big","criteria":[{"field":"amount","op":"eq","value":"42"}]}')
assert_not_contains "absent timeout_ms does not trip"      '"query_timeout"' "$GOT"
assert_contains "absent timeout_ms returns result"         '1500'            "$GOT"

echo ""
echo "=== thread-local does not leak across requests on same thread ==="
# Send a tight-timeout trip, then a normal query. The worker thread's
# thread-local must have been reset, so the second query should complete.
$BIN query '{"mode":"count","dir":"default","object":"rt_big","criteria":[{"field":"note","op":"contains","value":"record"}],"timeout_ms":10}' > /dev/null 2>&1
GOT=$($BIN query '{"mode":"count","dir":"default","object":"rt_big","criteria":[{"field":"amount","op":"eq","value":"1"}]}')
assert_not_contains "follow-up count does not inherit timeout" '"query_timeout"' "$GOT"
assert_contains "follow-up returns result"                 '1500'            "$GOT"

echo ""
echo "=== CLEANUP ==="
$BIN stop > /dev/null
rm -rf "$DB_ROOT/default/rt_big"
sed -i '/^default:rt_big:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true

echo ""
echo "================================"
echo "  $PASS passed, $FAIL failed ($TOTAL total)"
echo "================================"
[ "$FAIL" -eq 0 ]
