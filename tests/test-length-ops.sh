#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-length-ops.sh — len_eq / len_neq / len_lt / len_gt / len_lte /
# len_gte / len_between filters on varchar fields. Verifies correctness
# on both indexed (btree-walk-without-fetch) and non-indexed (full scan)
# paths, plus boundary cases (empty string, exact bounds).

set -e

BIN="./shard-db"
PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo "  FAIL: $1"; }

assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then pass "$desc"
    else fail "$desc: expected='$expected' actual='$actual'"; fi
}
assert_contains() {
    local desc="$1" needle="$2" hay="$3"
    if [[ "$hay" == *"$needle"* ]]; then pass "$desc"
    else fail "$desc: expected '$needle' in: $hay"; fi
}
count() { echo "$1" | grep -oE '"count":[0-9]+' | head -1 | sed 's/.*://'; }

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
rm -rf "$DB_ROOT/default/lent"
sed -i "/^default:lent:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN start > /dev/null
sleep 0.5

# `name` is indexed (drives btree-walk path), `bio` is unindexed (drives
# full-scan path). Both paths must agree on the answer.
$BIN query '{"mode":"create-object","dir":"default","object":"lent","fields":["name:varchar:64","bio:varchar:200"],"indexes":["name"],"splits":16}' > /dev/null

echo "=== seed ==="
# names: a(1), bob(3), alice(5), carol(5), elizabeth(9), longusername123(15)
# bios:  "" (0), "x" (1), "vip" (3), "vip" (3), "lengthier note" (14), "..." (3)
$BIN query '{"mode":"bulk-insert","dir":"default","object":"lent","records":[{"id":"k1","data":{"name":"a","bio":""}},{"id":"k2","data":{"name":"bob","bio":"x"}},{"id":"k3","data":{"name":"alice","bio":"vip"}},{"id":"k4","data":{"name":"carol","bio":"vip"}},{"id":"k5","data":{"name":"elizabeth","bio":"lengthier note"}},{"id":"k6","data":{"name":"longusername123","bio":"..."}}]}' > /dev/null

echo "=== indexed: name len_eq ==="
assert_eq "len_eq 5 (alice, carol)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_eq","value":"5"}]}')")"
assert_eq "len_eq 1 (a)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_eq","value":"1"}]}')")"
assert_eq "len_eq 7 (none)" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_eq","value":"7"}]}')")"

echo "=== indexed: name len_neq ==="
assert_eq "len_neq 5 → 4 (everyone except alice, carol)" "4" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_neq","value":"5"}]}')")"

echo "=== indexed: name len_lt / lte ==="
assert_eq "len_lt 5 → 2 (a, bob)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_lt","value":"5"}]}')")"
assert_eq "len_lte 5 → 4 (a, bob, alice, carol)" "4" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_lte","value":"5"}]}')")"

echo "=== indexed: name len_gt / gte ==="
assert_eq "len_gt 5 → 2 (elizabeth, longusername123)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_gt","value":"5"}]}')")"
assert_eq "len_gte 9 → 2 (elizabeth, longusername123)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_gte","value":"9"}]}')")"

echo "=== indexed: name len_between ==="
assert_eq "len_between 3,5 → 4 (bob, alice, carol)" "3" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_between","value":"3","value2":"5"}]}')")"
assert_eq "len_between 1,1 → 1 (a)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_between","value":"1","value2":"1"}]}')")"
assert_eq "len_between 100,200 → 0" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_between","value":"100","value2":"200"}]}')")"

echo "=== indexed: find returns the right rows ==="
out=$($BIN query '{"mode":"find","dir":"default","object":"lent","criteria":[{"field":"name","op":"len_eq","value":"5"}],"fields":["name"],"order_by":"name","order":"asc"}')
assert_contains "find len_eq 5 has alice" '"name":"alice"' "$out"
assert_contains "find len_eq 5 has carol" '"name":"carol"' "$out"
[[ "$out" == *'"name":"a"'* ]] && fail "find len_eq 5 leaked 'a'" || pass "find len_eq 5 didn't leak 'a'"
[[ "$out" == *'"name":"bob"'* ]] && fail "find len_eq 5 leaked 'bob'" || pass "find len_eq 5 didn't leak 'bob'"

echo "=== non-indexed: bio len_eq 0 (empty bio) ==="
# k1's bio is "" → len 0
assert_eq "bio len_eq 0 → 1 (k1)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"bio","op":"len_eq","value":"0"}]}')")"

echo "=== non-indexed: bio len ranges ==="
# bios: "" (0), "x" (1), "vip" (3), "vip" (3), "lengthier note" (14), "..." (3)
assert_eq "bio len_lt 3 → 2 (k1, k2)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"bio","op":"len_lt","value":"3"}]}')")"
assert_eq "bio len_eq 3 → 3 (k3,k4,k6)" "3" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"bio","op":"len_eq","value":"3"}]}')")"
assert_eq "bio len_gt 5 → 1 (k5)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"bio","op":"len_gt","value":"5"}]}')")"

echo "=== aggregate respects len_* criteria ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"lent","aggregates":[{"fn":"count","alias":"n"}],"criteria":[{"field":"name","op":"len_gte","value":"5"}]}')
assert_contains "agg(count) name len_gte 5 → 4 (alice, carol, elizabeth, longusername123)" '"n":4' "$out"

echo "=== combined: len + non-len criteria via AND ==="
# name starts_with "a" AND name len_lt 5  → just nothing? alice is 5; "a" alone is 1.
# name = "a" matches starts_with "a" (length 1) and len_lt 5 → yes
out=$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"name","op":"starts","value":"a"},{"field":"name","op":"len_lt","value":"5"}]}')
assert_eq "starts a AND len_lt 5 → 1 (just 'a')" "1" "$(count "$out")"

echo "=== non-varchar field with len_* must return 0 (graceful) ==="
# Make sure len_eq on a non-existent / non-varchar field doesn't crash.
out=$($BIN query '{"mode":"count","dir":"default","object":"lent","criteria":[{"field":"missing_field","op":"len_eq","value":"5"}]}')
assert_eq "len_eq on unknown field → 0" "0" "$(count "$out")"

echo "=== bulk-update where len_* criteria ==="
# Set bio="VIP" for any record where bio length is 3.
$BIN query '{"mode":"bulk-update","dir":"default","object":"lent","criteria":[{"field":"bio","op":"len_eq","value":"3"}],"value":{"bio":"VIP"}}' > /dev/null
out=$($BIN query '{"mode":"get","dir":"default","object":"lent","key":"k3"}')
assert_contains "k3 bio after bulk-update with len criteria" '"bio":"VIP"' "$out"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"lent"}' > /dev/null 2>&1 || true
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
