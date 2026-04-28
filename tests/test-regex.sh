#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-regex.sh — POSIX extended regex via the regex / not_regex ops.
# Always full-scan (no btree shortcut). Patterns compile once at criteria
# compile-time; matching uses REG_STARTEND so the daemon can match against
# (ptr, len) pairs without copying.
#
# Note on JSON escaping: shard-db's minimal JSON parser doesn't decode \\
# back to a single backslash, so regex meta-chars that need escaping
# (`\.`, `\d`, etc.) are written with a single literal backslash in the
# JSON value. This is non-standard JSON but works against this parser.

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
count() { echo "$1" | grep -oE '"count":[0-9]+' | head -1 | sed 's/.*://'; }

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
rm -rf "$DB_ROOT/default/rxt"
sed -i "/^default:rxt:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN start > /dev/null
sleep 0.5

$BIN query '{"mode":"create-object","dir":"default","object":"rxt","fields":["email:varchar:64","phone:varchar:32"]}' > /dev/null

echo "=== seed ==="
$BIN query '{"mode":"bulk-insert","dir":"default","object":"rxt","records":[{"id":"k1","data":{"email":"alice@example.com","phone":"+15551234567"}},{"id":"k2","data":{"email":"bob@TEST.org","phone":"5555555"}},{"id":"k3","data":{"email":"not-an-email","phone":"+44 20 7946 0958"}},{"id":"k4","data":{"email":"carol@x.io","phone":"abc"}}]}' > /dev/null

echo "=== substring (no anchors) ==="
assert_eq "regex 'org' → 1 (k2 bob@TEST.org)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"org"}]}')")"
assert_eq "regex '@example' → 1" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"@example"}]}')")"

echo "=== anchors ^ and \$ ==="
assert_eq "regex '^carol' → 1 (k4)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"^carol"}]}')")"
assert_eq "regex 'org\$' → 1 (k2)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"org$"}]}')")"
assert_eq "regex '^.*\\.com\$' → 1 (alice@example.com only)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"^.*\.com$"}]}')")"

echo "=== character classes ==="
assert_eq "regex '[A-Z]' (any uppercase) → 1 (k2 TEST)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"[A-Z]"}]}')")"
assert_eq "regex '^[a-z]+@[a-z]+\\.[a-z]+\$' (lowercase email) → 2 (k1, k4)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"^[a-z]+@[a-z]+\.[a-z]+$"}]}')")"

echo "=== quantifiers ==="
assert_eq "regex 'al+' (one or more l) → 1 (alice)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"al+"}]}')")"
assert_eq "regex 'al{2,}' (l at least twice) → 0" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"al{2,}"}]}')")"

echo "=== alternation ==="
assert_eq "regex '(alice|bob)' → 2" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"(alice|bob)"}]}')")"

echo "=== phone patterns ==="
assert_eq "regex '^\\+' (international starting with +) → 2 (k1, k3)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"phone","op":"regex","value":"^\+"}]}')")"
assert_eq "regex '[0-9]{7,}' (7+ consecutive digits) → 2 (k1, k2)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"phone","op":"regex","value":"[0-9]{7,}"}]}')")"

echo "=== not_regex inverts ==="
assert_eq "not_regex '^\\+' → 2 (k2, k4 — no leading +)" "2" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"phone","op":"not_regex","value":"^\+"}]}')")"
assert_eq "not_regex 'org' → 3 (everything except k2)" "3" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"not_regex","value":"org"}]}')")"

echo "=== invalid regex degrades gracefully ==="
# Unbalanced paren should fail regcomp; OP_REGEX returns no records.
assert_eq "bad regex '^(.*' → 0 records (no match)" "0" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"^(.*"}]}')")"
# not_regex on a bad regex: re_compiled=0 → all 4 records "match" the negation.
assert_eq "bad not_regex '^(.*' → 4 (negation of compile-fail = match all)" "4" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"not_regex","value":"^(.*"}]}')")"

echo "=== combined with other ops ==="
# Regex matches lowercase emails AND phone starts with + → only k1
assert_eq "lowercase email AND +phone → 1 (k1)" "1" \
    "$(count "$($BIN query '{"mode":"count","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"^[a-z]+@[a-z]+\.[a-z]+$"},{"field":"phone","op":"regex","value":"^\+"}]}')")"

echo "=== find returns the right rows ==="
out=$($BIN query '{"mode":"find","dir":"default","object":"rxt","criteria":[{"field":"phone","op":"regex","value":"^\+"}],"fields":["email","phone"]}')
[[ "$out" == *'"key":"k1"'* ]] && pass "find regex includes k1" || fail "find regex missing k1: $out"
[[ "$out" == *'"key":"k3"'* ]] && pass "find regex includes k3" || fail "find regex missing k3: $out"
[[ "$out" == *'"key":"k2"'* ]] && fail "find regex must NOT include k2" || pass "find regex excludes k2"

echo "=== aggregate respects regex ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"rxt","aggregates":[{"fn":"count","alias":"n"}],"criteria":[{"field":"email","op":"regex","value":"@"}]}')
[[ "$out" == *'"n":3'* ]] && pass "agg count regex '@' = 3 (k1,k2,k4)" || fail "agg count: $out"

echo "=== bulk-update with regex criteria ==="
$BIN query '{"mode":"bulk-update","dir":"default","object":"rxt","criteria":[{"field":"email","op":"regex","value":"^not-"}],"value":{"phone":"BLOCKED"}}' > /dev/null
out=$($BIN query '{"mode":"get","dir":"default","object":"rxt","key":"k3"}')
[[ "$out" == *'"phone":"BLOCKED"'* ]] && pass "bulk-update via regex hit k3" || fail "expected BLOCKED phone: $out"
out=$($BIN query '{"mode":"get","dir":"default","object":"rxt","key":"k1"}')
[[ "$out" == *'"phone":"+15551234567"'* ]] && pass "bulk-update did not touch k1" || fail "k1 changed: $out"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"rxt"}' > /dev/null 2>&1 || true
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
