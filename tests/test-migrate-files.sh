#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-migrate-files.sh — one-shot upgrade step that lifts pre-2026.05.2
# hash-bucketed XX/XX/<filename> file storage into the flat
# <obj>/files/<filename> layout. Idempotent. Skips conflicts.

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
field() { echo "$1" | grep -oE "\"$2\":[0-9]+" | head -1 | sed 's/.*://'; }

echo "=== SETUP ==="
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
rm -rf "$DB_ROOT/default/mft"
sed -i "/^default:mft:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN start > /dev/null
sleep 0.5

$BIN query '{"mode":"create-object","dir":"default","object":"mft","fields":["k:varchar:32"]}' > /dev/null

echo "=== seed XX/XX layout (simulates pre-2026.05.2 install) ==="
mkdir -p "$DB_ROOT/default/mft/files/ab/cd"
mkdir -p "$DB_ROOT/default/mft/files/12/34"
mkdir -p "$DB_ROOT/default/mft/files/ef/00"
mkdir -p "$DB_ROOT/default/mft/files/aa/bb"
echo alpha   > "$DB_ROOT/default/mft/files/ab/cd/alpha.pdf"
echo bravo   > "$DB_ROOT/default/mft/files/12/34/bravo.txt"
echo charlie > "$DB_ROOT/default/mft/files/ef/00/charlie.png"
echo delta   > "$DB_ROOT/default/mft/files/aa/bb/delta.csv"

assert_eq "before: 4 leaf XX/XX dirs"   "4" "$(find "$DB_ROOT/default/mft/files" -mindepth 2 -maxdepth 2 -type d | wc -l)"
assert_eq "before: 0 flat files"        "0" "$(find "$DB_ROOT/default/mft/files" -maxdepth 1 -mindepth 1 -type f | wc -l)"

echo "=== run migrate-files ==="
out=$($BIN migrate-files)
assert_contains "status=migrated"        '"status":"migrated"'      "$out"
assert_eq "files_moved=4"   "4" "$(field "$out" files_moved)"
assert_eq "objects_migrated=1 (only mft has bucket files)" "1" "$(field "$out" objects_migrated)"
assert_eq "conflicts=0"     "0" "$(field "$out" conflicts)"

echo "=== layout flattened ==="
assert_eq "after: 0 leaf XX/XX dirs"   "0" "$(find "$DB_ROOT/default/mft/files" -mindepth 2 -maxdepth 2 -type d 2>/dev/null | wc -l)"
assert_eq "after: 0 top-level XX dirs" "0" "$(find "$DB_ROOT/default/mft/files" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)"
assert_eq "after: 4 flat files"        "4" "$(find "$DB_ROOT/default/mft/files" -maxdepth 1 -mindepth 1 -type f | wc -l)"

echo "=== list-files sees migrated files alphabetically ==="
out=$($BIN list-files default mft)
assert_contains "alpha.pdf listed"   'alpha.pdf'   "$out"
assert_contains "bravo.txt listed"   'bravo.txt'   "$out"
assert_contains "charlie.png listed" 'charlie.png' "$out"
assert_contains "delta.csv listed"   'delta.csv'   "$out"
assert_contains "total=4" '"total":4' "$out"

echo "=== get-file works after migration ==="
$BIN get-file default mft alpha.pdf /tmp/mft_alpha.$$ > /dev/null
got=$(cat /tmp/mft_alpha.$$)
rm -f /tmp/mft_alpha.$$
assert_eq "alpha.pdf content" "alpha" "$got"

echo "=== second run is idempotent (no-op) ==="
out=$($BIN migrate-files)
assert_eq "second run files_moved=0"       "0" "$(field "$out" files_moved)"
assert_eq "second run objects_migrated=0"  "0" "$(field "$out" objects_migrated)"

echo "=== conflict handling: flat target already exists ==="
mkdir -p "$DB_ROOT/default/mft/files/ab/cd"
echo new-content > "$DB_ROOT/default/mft/files/ab/cd/alpha.pdf"  # collides w/ existing flat alpha.pdf
out=$($BIN migrate-files 2>&1)
assert_eq "conflict reported" "1" "$(field "$out" conflicts)"
assert_eq "no overwrite: alpha.pdf flat content unchanged" "alpha" "$(cat "$DB_ROOT/default/mft/files/alpha.pdf")"
[[ -f "$DB_ROOT/default/mft/files/ab/cd/alpha.pdf" ]] && pass "conflicting bucket leaf preserved" || fail "conflicting bucket leaf vanished"
# clean the conflict so the test object can be dropped cleanly
rm -rf "$DB_ROOT/default/mft/files/ab"

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"mft"}' > /dev/null 2>&1 || true
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
