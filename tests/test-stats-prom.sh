#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-stats-prom.sh — Prometheus text-format exposition. Same atomics as
# `stats`, different formatter; this test pins the output shape and verifies
# counters move under traffic.

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
assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then pass "$desc"
    else fail "$desc: expected='$expected' actual='$actual'"; fi
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3

DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

rm -rf "$DB_ROOT/default/prom_test"
sed -i "/^default:prom_test:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true

$BIN start > /dev/null
sleep 0.5

echo "=== shape: HELP/TYPE comments + every metric present ==="
out=$($BIN stats-prom)

# Every metric must have HELP + TYPE lines AND a sample line.
for m in \
    shard_db_uptime_seconds \
    shard_db_active_threads \
    shard_db_in_flight_writes \
    shard_db_ucache_used \
    shard_db_ucache_capacity \
    shard_db_ucache_bytes \
    shard_db_ucache_hits_total \
    shard_db_ucache_misses_total \
    shard_db_bt_cache_used \
    shard_db_bt_cache_capacity \
    shard_db_bt_cache_bytes \
    shard_db_bt_cache_hits_total \
    shard_db_bt_cache_misses_total \
    shard_db_slow_query_threshold_milliseconds \
    shard_db_slow_query_total
do
    assert_contains "HELP for $m"   "# HELP $m"  "$out"
    assert_contains "TYPE for $m"   "# TYPE $m"  "$out"
    # Sample line: metric name at start of a line, followed by space.
    if grep -qE "^${m} " <<<"$out"; then pass "sample line for $m"
    else fail "sample line for $m: missing"; fi
done

echo "=== type discipline: counters end in _total, are typed counter ==="
assert_contains "ucache_hits_total typed counter"   "# TYPE shard_db_ucache_hits_total counter"   "$out"
assert_contains "ucache_misses_total typed counter" "# TYPE shard_db_ucache_misses_total counter" "$out"
assert_contains "bt_cache_hits_total typed counter" "# TYPE shard_db_bt_cache_hits_total counter" "$out"
assert_contains "slow_query_total typed counter"    "# TYPE shard_db_slow_query_total counter"    "$out"
assert_contains "uptime_seconds typed gauge"        "# TYPE shard_db_uptime_seconds gauge"        "$out"
assert_contains "ucache_capacity typed gauge"       "# TYPE shard_db_ucache_capacity gauge"       "$out"

echo "=== content-type sanity: not JSON ==="
# stats-prom returns plain text — must not be a JSON object.
if [[ "$out" != "{"* ]]; then pass "output does not start with '{'"
else fail "output looks like JSON: ${out:0:80}"; fi

echo "=== counters move under traffic ==="
hits_before=$(grep '^shard_db_ucache_hits_total ' <<<"$out" | awk '{print $2}')
miss_before=$(grep '^shard_db_ucache_misses_total ' <<<"$out" | awk '{print $2}')

$BIN query '{"mode":"create-object","dir":"default","object":"prom_test","fields":["name:varchar:32"],"splits":16}' > /dev/null
$BIN insert default prom_test k1 '{"name":"alice"}' > /dev/null
for i in 1 2 3 4 5; do $BIN get default prom_test k1 > /dev/null; done

out2=$($BIN stats-prom)
hits_after=$(grep '^shard_db_ucache_hits_total ' <<<"$out2" | awk '{print $2}')
miss_after=$(grep '^shard_db_ucache_misses_total ' <<<"$out2" | awk '{print $2}')

if [ "$hits_after" -gt "$hits_before" ]; then pass "ucache_hits_total increased ($hits_before → $hits_after)"
else fail "ucache_hits_total did not increase ($hits_before → $hits_after)"; fi

# At least one miss should have happened on the first read after the insert.
if [ "$miss_after" -ge "$miss_before" ]; then pass "ucache_misses_total non-decreasing ($miss_before → $miss_after)"
else fail "ucache_misses_total decreased ($miss_before → $miss_after)"; fi

echo "=== uptime advances ==="
up_before=$(grep '^shard_db_uptime_seconds ' <<<"$out2"  | awk '{print $2}')
sleep 1
out3=$($BIN stats-prom)
up_after=$(grep '^shard_db_uptime_seconds ' <<<"$out3"  | awk '{print $2}')
# Compare via awk so we don't fight bash's lack of float math.
if awk -v a="$up_before" -v b="$up_after" 'BEGIN{exit !(b > a)}'; then
    pass "uptime_seconds advances ($up_before → $up_after)"
else
    fail "uptime_seconds did not advance ($up_before → $up_after)"
fi

echo "=== counters are integer-formatted (no trailing .0) ==="
# Counter samples should be plain integers — no decimal point.
if grep -qE '^shard_db_ucache_hits_total [0-9]+$' <<<"$out3"; then pass "hits_total formatted as integer"
else fail "hits_total not integer-formatted: $(grep '^shard_db_ucache_hits_total ' <<<"$out3")"; fi

echo "=== auth: stats-prom is server-admin (rejects unknown token) ==="
# Pull a known-bad token through the JSON path.
out=$($BIN query '{"mode":"stats-prom","auth":"definitely-not-a-real-token"}' 2>&1 || true)
# Localhost is trusted by default in the dev db.env, so the call still
# succeeds — but the request must at least parse and return prom output
# rather than 500-style JSON garbage. Pin the *shape* check, not the
# auth-failure path (covered by test-token-perms.sh).
assert_contains "stats-prom over JSON returns prom output" "shard_db_uptime_seconds " "$out"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"prom_test"}' > /dev/null 2>&1 || true
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
