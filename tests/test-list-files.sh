#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-list-files.sh — list-files mode walks <dir>/<obj>/files/XX/XX/,
# applies optional prefix filter, and paginates alphabetically. Default
# limit when absent is GLOBAL_LIMIT.

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
total_of() { echo "$1" | grep -oE '"total":[0-9]+' | head -1 | sed 's/.*://'; }

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
rm -rf "$DB_ROOT/default/lft"
sed -i "/^default:lft:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN start > /dev/null
sleep 0.5

$BIN query '{"mode":"create-object","dir":"default","object":"lft","fields":["k:varchar:32"]}' > /dev/null

echo "=== empty object → 0 files ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft"}')
assert_contains "files=[]" '"files":[]' "$out"
assert_eq "total=0" "0" "$(total_of "$out")"
assert_contains "default limit echoes global limit" '"limit":100000' "$out"

echo "=== upload mixed-name files ==="
TMP=/tmp/lft_test_$$
mkdir -p "$TMP"
for f in alpha.pdf alpha2.pdf beta.pdf delta.pdf gamma.txt zeta.png; do
    echo "x" > "$TMP/$f"
    $BIN query "{\"mode\":\"put-file\",\"dir\":\"default\",\"object\":\"lft\",\"path\":\"$TMP/$f\"}" > /dev/null
done
rm -rf "$TMP"

echo "=== full listing alphabetical ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft"}')
assert_eq "total=6" "6" "$(total_of "$out")"
assert_contains "alphabetical: alpha.pdf first" '"files":["alpha.pdf","alpha2.pdf"' "$out"
assert_contains "alphabetical: zeta.png last" 'zeta.png"]' "$out"

echo "=== prefix filter ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","prefix":"alpha"}')
assert_eq "prefix alpha → total=2" "2" "$(total_of "$out")"
assert_contains "prefix alpha → alpha.pdf" 'alpha.pdf' "$out"
assert_contains "prefix alpha → alpha2.pdf" 'alpha2.pdf' "$out"
[[ "$out" == *'beta.pdf'* ]] && fail "prefix alpha leaked beta.pdf" || pass "prefix alpha rejects beta.pdf"

out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","prefix":"z"}')
assert_eq "prefix z → total=1" "1" "$(total_of "$out")"
assert_contains "prefix z → zeta.png" 'zeta.png' "$out"

echo "=== pagination: limit ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","limit":2}')
assert_contains "limit=2 first page = alpha,alpha2" '"files":["alpha.pdf","alpha2.pdf"]' "$out"
assert_eq "limit=2 echoes total=6" "6" "$(total_of "$out")"
assert_contains "limit echoed in response" '"limit":2' "$out"

echo "=== pagination: offset+limit ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","offset":2,"limit":2}')
assert_contains "offset=2 limit=2 → beta,delta" '"files":["beta.pdf","delta.pdf"]' "$out"
assert_contains "offset echoed" '"offset":2' "$out"

out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","offset":4,"limit":10}')
assert_contains "offset=4 → tail starts gamma" 'gamma.txt' "$out"
assert_contains "offset=4 → ends with zeta" 'zeta.png' "$out"

echo "=== prefix + pagination combined ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","prefix":"alpha","offset":1,"limit":1}')
assert_contains "prefix alpha + offset=1 → just alpha2" '"files":["alpha2.pdf"]' "$out"
assert_eq "prefix-filtered total=2" "2" "$(total_of "$out")"

echo "=== prefix no-match ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","prefix":"xxx"}')
assert_contains "no match → []" '"files":[]' "$out"
assert_eq "no match → total=0" "0" "$(total_of "$out")"

echo "=== offset past end ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","offset":100,"limit":10}')
assert_contains "offset>total → empty page" '"files":[]' "$out"
assert_eq "but total still reports the full set" "6" "$(total_of "$out")"

echo "=== match=suffix ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","pattern":".pdf","match":"suffix"}')
assert_eq "suffix .pdf → total=4" "4" "$(total_of "$out")"
assert_contains "suffix .pdf → alpha.pdf"  'alpha.pdf'  "$out"
assert_contains "suffix .pdf → alpha2.pdf" 'alpha2.pdf' "$out"
assert_contains "suffix .pdf → beta.pdf"   'beta.pdf'   "$out"
assert_contains "suffix .pdf → delta.pdf"  'delta.pdf'  "$out"
[[ "$out" == *'gamma.txt'* ]] && fail "suffix .pdf leaked gamma.txt" || pass "suffix .pdf rejects gamma.txt"
[[ "$out" == *'zeta.png'*  ]] && fail "suffix .pdf leaked zeta.png"  || pass "suffix .pdf rejects zeta.png"

echo "=== match=contains ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","pattern":"lpha","match":"contains"}')
assert_eq "contains lpha → total=2" "2" "$(total_of "$out")"
assert_contains "contains lpha → alpha.pdf"  'alpha.pdf'  "$out"
assert_contains "contains lpha → alpha2.pdf" 'alpha2.pdf' "$out"

out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","pattern":"eta","match":"contains"}')
assert_eq "contains eta → total=2" "2" "$(total_of "$out")"
assert_contains "contains eta → beta.pdf"  'beta.pdf'  "$out"
assert_contains "contains eta → zeta.png"  'zeta.png'  "$out"
[[ "$out" == *'delta.pdf'* ]] && fail "contains eta wrongly matched delta.pdf" || pass "contains eta rejects delta.pdf"

echo "=== match=glob ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","pattern":"*.pdf","match":"glob"}')
assert_eq "glob *.pdf → total=4" "4" "$(total_of "$out")"

out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","pattern":"alpha?.pdf","match":"glob"}')
assert_eq "glob alpha?.pdf → total=1 (alpha2.pdf)" "1" "$(total_of "$out")"
assert_contains "glob alpha?.pdf → alpha2.pdf" 'alpha2.pdf' "$out"
[[ "$out" == *'alpha.pdf"'* ]] && fail "glob alpha?.pdf wrongly matched alpha.pdf" || pass "glob alpha?.pdf rejects alpha.pdf"

out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","pattern":"[ab]*","match":"glob"}')
assert_eq "glob [ab]* → total=3" "3" "$(total_of "$out")"

echo "=== invalid match mode ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","pattern":"foo","match":"regex"}')
assert_contains "invalid match mode → error" 'invalid match mode' "$out"

echo "=== back-compat: legacy prefix field still works ==="
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft","prefix":"alpha"}')
assert_eq "legacy prefix → total=2" "2" "$(total_of "$out")"
assert_contains "legacy prefix → alpha.pdf"  'alpha.pdf'  "$out"

echo "=== files survive delete-file ==="
$BIN query '{"mode":"delete-file","dir":"default","object":"lft","filename":"beta.pdf"}' > /dev/null
out=$($BIN query '{"mode":"list-files","dir":"default","object":"lft"}')
assert_eq "after delete → total=5" "5" "$(total_of "$out")"
[[ "$out" == *'beta.pdf'* ]] && fail "deleted beta.pdf still listed" || pass "deleted beta.pdf gone from list"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"lft"}' > /dev/null 2>&1 || true
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
