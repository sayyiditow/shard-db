#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-binary-index.sh — correctness of binary-native B+ tree keys (Path B).
#
# Covers:
# - Signed-int range queries across zero (pre-Path-B would mis-sort under ASCII strcmp)
# - Numeric (fixed-point) range across zero with fractional scale
# - Date range (int32 yyyyMMdd, non-negative but encoded via sign-flip)
# - Varchar equality and prefix (starts-with)
# - Composite index regression: ASCII-concat path still returns expected rows
# - reindex all/tenant/object forms rebuild indexes idempotently

set -e

BIN="./shard-db"
PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo "  FAIL: $1"; }

assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [[ "$actual" == *"$expected"* ]]; then pass "$desc"
    else fail "$desc: expected '$expected' in: $actual"; fi
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3

DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

for obj in bi_int bi_int_bulk bi_num bi_date bi_varchar bi_comp; do
    rm -rf "$DB_ROOT/default/$obj"
    sed -i "/^default:$obj:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
done

$BIN start > /dev/null
sleep 0.5

echo "=== SIGNED INT RANGE ==="
$BIN query '{"mode":"create-object","dir":"default","object":"bi_int","splits":16,"max_key":16,"fields":["n:int"],"indexes":["n"]}' > /dev/null
for n in -2147483647 -1000000 -1 0 1 1000000 2147483647; do
    $BIN insert default bi_int "k_$n" "{\"n\":$n}" > /dev/null
done

# Same fixture via bulk-insert to guard against write-side drift between
# the single-record path (storage.c) and the bulk path (query.c).
rm -rf "$DB_ROOT/default/bi_int_bulk"
sed -i "/^default:bi_int_bulk:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN query '{"mode":"create-object","dir":"default","object":"bi_int_bulk","splits":16,"max_key":16,"fields":["n:int"],"indexes":["n"]}' > /dev/null
BULK=$(mktemp)
cat > "$BULK" <<'EOF'
[{"key":"b_min","value":{"n":-2147483647}},{"key":"b_-1m","value":{"n":-1000000}},{"key":"b_-1","value":{"n":-1}},{"key":"b_0","value":{"n":0}},{"key":"b_1","value":{"n":1}},{"key":"b_1m","value":{"n":1000000}},{"key":"b_max","value":{"n":2147483647}}]
EOF
$BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bi_int_bulk\",\"file\":\"$BULK\"}" > /dev/null
rm -f "$BULK"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_int_bulk","criteria":[{"field":"n","op":"eq","value":"-1"}]}')
assert_eq "bulk-insert: eq -1 finds 1 (write path binary-correct)" '1' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_int_bulk","criteria":[{"field":"n","op":"lt","value":"0"}]}')
assert_eq "bulk-insert: lt 0 finds 3 negatives" '3' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_int_bulk","criteria":[{"field":"n","op":"gte","value":"0"}]}')
assert_eq "bulk-insert: gte 0 finds 4 non-negatives" '4' "$R"

R=$($BIN query '{"mode":"count","dir":"default","object":"bi_int","criteria":[{"field":"n","op":"eq","value":"-2147483647"}]}')
assert_eq "eq MIN_INT" '1' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_int","criteria":[{"field":"n","op":"eq","value":"2147483647"}]}')
assert_eq "eq MAX_INT" '1' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_int","criteria":[{"field":"n","op":"lt","value":"0"}]}')
assert_eq "lt 0 returns 3 negatives" '3' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_int","criteria":[{"field":"n","op":"gte","value":"0"}]}')
assert_eq "gte 0 returns 4 non-negatives" '4' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_int","criteria":[{"field":"n","op":"between","value":"-1000","value2":"1000"}]}')
assert_eq "between -1000 and 1000 = 3" '3' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_int","criteria":[{"field":"n","op":"gt","value":"-2"}]}')
assert_eq "gt -2 = 4 (-1, 0, 1, 1M, MAX)" '5' "$R"

echo "=== NUMERIC (fixed-point) RANGE ==="
$BIN query '{"mode":"create-object","dir":"default","object":"bi_num","splits":16,"max_key":16,"fields":["amt:numeric:10,2"],"indexes":["amt"]}' > /dev/null
for v in -999.99 -0.01 0 0.01 999.99; do
    $BIN insert default bi_num "n_$v" "{\"amt\":$v}" > /dev/null
done
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_num","criteria":[{"field":"amt","op":"lt","value":"0"}]}')
assert_eq "numeric lt 0 = 2 negatives" '2' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_num","criteria":[{"field":"amt","op":"between","value":"-1","value2":"1"}]}')
assert_eq "numeric between -1 and 1 = 3" '3' "$R"

echo "=== DATE RANGE ==="
$BIN query '{"mode":"create-object","dir":"default","object":"bi_date","splits":16,"max_key":16,"fields":["d:date"],"indexes":["d"]}' > /dev/null
for d in 20200101 20250601 20300101; do
    $BIN insert default bi_date "d_$d" "{\"d\":\"$d\"}" > /dev/null
done
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_date","criteria":[{"field":"d","op":"between","value":"20220101","value2":"20270101"}]}')
assert_eq "date between 2022 and 2027 = 1" '1' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_date","criteria":[{"field":"d","op":"gte","value":"20260101"}]}')
assert_eq "date gte 2026 = 1" '1' "$R"

echo "=== VARCHAR EQ / PREFIX ==="
$BIN query '{"mode":"create-object","dir":"default","object":"bi_varchar","splits":16,"max_key":16,"fields":["s:varchar:32"],"indexes":["s"]}' > /dev/null
for v in alpha alpine beta gamma; do
    $BIN insert default bi_varchar "v_$v" "{\"s\":\"$v\"}" > /dev/null
done
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_varchar","criteria":[{"field":"s","op":"eq","value":"alpha"}]}')
assert_eq "varchar eq alpha = 1" '1' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_varchar","criteria":[{"field":"s","op":"starts","value":"alp"}]}')
assert_eq "varchar starts alp = 2" '2' "$R"

echo "=== COMPOSITE INDEX (ASCII path regression) ==="
$BIN query '{"mode":"create-object","dir":"default","object":"bi_comp","splits":16,"max_key":16,"fields":["status:varchar:16","region:varchar:16"],"indexes":["status+region"]}' > /dev/null
$BIN insert default bi_comp "c1" '{"status":"paid","region":"us"}' > /dev/null
$BIN insert default bi_comp "c2" '{"status":"paid","region":"eu"}' > /dev/null
$BIN insert default bi_comp "c3" '{"status":"pending","region":"us"}' > /dev/null
# Composite indexes serve concatenated-value equality — verify the insertions
# landed and indexed queries on a component still work (falls to full scan
# since status alone isn't a separate index).
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_comp","criteria":[{"field":"status","op":"eq","value":"paid"}]}')
assert_eq "composite: status=paid scans + matches 2" '2' "$R"

echo "=== REINDEX: three forms ==="
R=$($BIN reindex default bi_int)
assert_eq "reindex single object" '"indexes":1' "$R"
R=$($BIN reindex default)
assert_eq "reindex tenant reports success" '"status":"reindexed"' "$R"
R=$($BIN reindex)
assert_eq "reindex all reports success" '"status":"reindexed"' "$R"

# Post-reindex correctness spot check — same assertions as pre.
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_int","criteria":[{"field":"n","op":"lt","value":"0"}]}')
assert_eq "post-reindex: lt 0 still 3" '3' "$R"
R=$($BIN query '{"mode":"count","dir":"default","object":"bi_varchar","criteria":[{"field":"s","op":"starts","value":"alp"}]}')
assert_eq "post-reindex: varchar prefix still 2" '2' "$R"

echo "=== CLEANUP ==="
$BIN stop > /dev/null 2>&1 || true
for obj in bi_int bi_int_bulk bi_num bi_date bi_varchar bi_comp; do
    rm -rf "$DB_ROOT/default/$obj"
    sed -i "/^default:$obj:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
done

echo
echo "================================"
echo "  $PASS passed, $FAIL failed ($TOTAL total)"
echo "================================"
exit $FAIL
