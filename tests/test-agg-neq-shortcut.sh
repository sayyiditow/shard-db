#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-agg-neq-shortcut.sh — algebraic shortcut for aggregate(NEQ X) on an
# indexed field with COUNT/SUM/AVG specs. Identity:
#     agg(neq X) = agg(*) - agg(eq X)
# Verifies correctness against the existing path (criteria=eq + arithmetic),
# and verifies the shortcut bails out for MIN/MAX, group_by, having, and
# non-indexed fields, where the existing path still produces the right answer.

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
field() {
    # extract first occurrence of "<alias>":<number> (handles ints + floats)
    echo "$1" | grep -oE "\"$2\":-?[0-9]+(\.[0-9]+)?" | head -1 | sed -E "s/^\"$2\"://"
}

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
rm -rf "$DB_ROOT/default/neq_t"
sed -i "/^default:neq_t:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN start > /dev/null
sleep 0.5

# Object with `status` indexed (drives the shortcut), `region` NOT indexed
# (fall-through), `amount` numeric (for sum/avg).
$BIN query '{"mode":"create-object","dir":"default","object":"neq_t","fields":["status:varchar:16","region:varchar:8","amount:int"],"indexes":["status"],"splits":16}' > /dev/null

echo "=== seed: 100 rows, status: 1/5 paid, 4/5 pending ==="
# Build a JSON array of 100 records: i=1..100, status=paid iff i%5==0,
# region=us iff i odd else eu, amount=i.
recs="["
for i in $(seq 1 100); do
    if [ $((i % 5)) -eq 0 ]; then s="paid"; else s="pending"; fi
    if [ $((i % 2)) -eq 1 ]; then r="us"; else r="eu"; fi
    [ "$recs" != "[" ] && recs="$recs,"
    recs="$recs{\"id\":\"k$i\",\"data\":{\"status\":\"$s\",\"region\":\"$r\",\"amount\":$i}}"
done
recs="$recs]"
$BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"neq_t\",\"records\":$recs}" > /dev/null

# Expected: total=100, paid=20, pending=80
# sum(1..100)=5050; sum of multiples of 5 in 1..100 = 1050; sum(neq paid) = 4000.
echo "=== count(neq paid) — shortcut path ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"}],"criteria":[{"field":"status","op":"neq","value":"paid"}]}')
assert_eq "count(neq paid)=80" "80" "$(field "$out" n)"

echo "=== sum(amount) where neq paid — shortcut path ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"sum","field":"amount","alias":"s"}],"criteria":[{"field":"status","op":"neq","value":"paid"}]}')
assert_eq "sum(amount neq paid)=4000" "4000" "$(field "$out" s)"

echo "=== avg(amount) where neq paid — shortcut path ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"avg","field":"amount","alias":"a"}],"criteria":[{"field":"status","op":"neq","value":"paid"}]}')
# 4000/80 = 50; fmt_double trims trailing zeros, so we accept "50" or "50.0"
av=$(field "$out" a)
if [ "$av" = "50" ] || [ "$av" = "50.0" ] || [ "$av" = "50.000000" ]; then pass "avg=50"
else fail "avg=50: got '$av'"; fi

echo "=== combined COUNT+SUM+AVG — shortcut path ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"},{"fn":"sum","field":"amount","alias":"s"},{"fn":"avg","field":"amount","alias":"a"}],"criteria":[{"field":"status","op":"neq","value":"paid"}]}')
assert_eq "combined count=80" "80" "$(field "$out" n)"
assert_eq "combined sum=4000" "4000" "$(field "$out" s)"

echo "=== correctness vs the existing eq-path (subtraction by hand) ==="
total=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"}]}')
eq_paid=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"}],"criteria":[{"field":"status","op":"eq","value":"paid"}]}')
neq_paid=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"}],"criteria":[{"field":"status","op":"neq","value":"paid"}]}')
t=$(field "$total" n); e=$(field "$eq_paid" n); n=$(field "$neq_paid" n)
assert_eq "100 - eq_paid == neq_paid" "$((t - e))" "$n"

echo "=== shortcut bails for MIN — must fall through ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"min","field":"amount","alias":"mn"}],"criteria":[{"field":"status","op":"neq","value":"paid"}]}')
# Smallest non-paid (i=1): 1
assert_eq "min(amount neq paid)=1" "1" "$(field "$out" mn)"

echo "=== shortcut bails for MAX — must fall through ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"max","field":"amount","alias":"mx"}],"criteria":[{"field":"status","op":"neq","value":"paid"}]}')
# Largest non-paid: 99 (multiples of 5 are paid, so 100,95,90... are paid; 99 is pending)
assert_eq "max(amount neq paid)=99" "99" "$(field "$out" mx)"

echo "=== mixed COUNT + MIN — bails for MIN, must fall through ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"},{"fn":"min","field":"amount","alias":"mn"}],"criteria":[{"field":"status","op":"neq","value":"paid"}]}')
assert_eq "mixed count=80" "80" "$(field "$out" n)"
assert_eq "mixed min=1" "1" "$(field "$out" mn)"

echo "=== shortcut bails for non-indexed field — must fall through ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"}],"criteria":[{"field":"region","op":"neq","value":"us"}]}')
# 50 records have region=eu (even i)
assert_eq "count(region neq us)=50" "50" "$(field "$out" n)"

echo "=== shortcut bails for group_by — must fall through ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"}],"group_by":["region"],"criteria":[{"field":"status","op":"neq","value":"paid"}]}')
# pending: 80 records → 40 in us (i odd, not %5), 40 in eu (i even, not %5).
# Wait actually let's recompute: pending = i%5!=0. Among 1..100, 80 of them.
# us = i odd = 50 records total; us & pending = i odd & i%5!=0 = 50 - 10 = 40.
# eu = i even = 50 total; eu & pending = 50 - 10 = 40.
[[ "$out" == *'"region":"us"'* ]] && pass "group_by has us bucket" || fail "group_by missing us bucket: $out"
[[ "$out" == *'"region":"eu"'* ]] && pass "group_by has eu bucket" || fail "group_by missing eu bucket: $out"
[[ "$out" == *'"n":40'* ]] && pass "group_by yields 40 per bucket" || fail "group_by counts wrong: $out"

echo "=== shortcut bails for having — must fall through (group_by case) ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"}],"group_by":["status"],"having":[{"field":"n","op":"gt","value":"50"}],"criteria":[]}')
[[ "$out" == *'"status":"pending"'* ]] && pass "having keeps pending (n=80>50)" || fail "having missing pending: $out"
[[ "$out" == *'"status":"paid"'* ]] && fail "having should drop paid (n=20): $out" || pass "having drops paid"

echo "=== empty criteria — shortcut not engaged, returns whole-set ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"}]}')
assert_eq "no-criteria count=100" "100" "$(field "$out" n)"

echo "=== CSV format on shortcut path ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"},{"fn":"sum","field":"amount","alias":"s"}],"criteria":[{"field":"status","op":"neq","value":"paid"}],"format":"csv"}')
# Expected: header line "n,s\n80,4000\n"
header=$(echo "$out" | head -1)
data=$(echo "$out" | head -2 | tail -1)
assert_eq "CSV header" "n,s" "$header"
assert_eq "CSV data" "80,4000" "$data"

echo "=== NEQ value that matches everything (count=0) ==="
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"}],"criteria":[{"field":"status","op":"neq","value":"this_value_doesnt_exist"}]}')
assert_eq "count(neq nonexistent)=100" "100" "$(field "$out" n)"

echo "=== NEQ on indexed field with single-value table — count=0 ==="
# After overwriting all rows to status=paid, count(neq paid) should be 0.
$BIN query '{"mode":"bulk-insert","dir":"default","object":"neq_t","records":[{"id":"k1","data":{"status":"paid","region":"us","amount":1}},{"id":"k2","data":{"status":"paid","region":"us","amount":2}}]}' > /dev/null
# Just k1 and k2 changed; k3..k100 still mixed. Test the original distribution still works.
out=$($BIN query '{"mode":"aggregate","dir":"default","object":"neq_t","aggregates":[{"fn":"count","alias":"n"}],"criteria":[{"field":"status","op":"neq","value":"paid"}]}')
# Originally 80 pending; we just overwrote k1 (was pending) and k2 (was pending) to paid.
assert_eq "count(neq paid) after overwrite = 78" "78" "$(field "$out" n)"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"neq_t"}' > /dev/null 2>&1 || true
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
