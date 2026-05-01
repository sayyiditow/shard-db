#!/bin/bash
# Run from project root regardless of CWD so binaries and db.env resolve.
cd "$(dirname "$0")/.."
# test-migrate-binary.sh — exercises ./migrate, the per-release one-shot
# upgrade binary. For 2026.05.1 it does two phases:
#   1. migrate-files (lift pre-2026.05.2 XX/XX hash buckets to flat layout)
#   2. reindex (rebuild B+ trees under the per-shard btree layout)
#
# This test seeds the XX/XX layout directly in the FS, runs ./migrate, and
# verifies the flatten phase emitted the expected JSON summary and the
# files were moved correctly. Phase 2 (reindex) runs on a no-index object
# so it's a no-op — but the daemon spawn/stop cycle is exercised end-to-end.

set -e

BIN="./shard-db"
MIG="./migrate"
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
$BIN stop > /dev/null
sleep 0.3

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

echo "=== run ./migrate (phase 1 = files, phase 2 = reindex) ==="
out=$($MIG 2>&1)
echo "$out" | grep -q '"status":"migrated"' && pass "phase 1 emitted status=migrated" || fail "no status=migrated in: $out"

# Pull the migrate-files summary line out of the multi-line output.
files_line=$(echo "$out" | grep '"status":"migrated"' | head -1)
assert_eq "files_moved=4"   "4" "$(field "$files_line" files_moved)"
assert_eq "objects_migrated>=1" "1" "$([ "$(field "$files_line" objects_migrated)" -ge 1 ] && echo 1 || echo 0)"
assert_eq "conflicts=0"     "0" "$(field "$files_line" conflicts)"

assert_contains "phase 1 banner" 'phase 1/2' "$out"
assert_contains "phase 2 banner" 'phase 2/2' "$out"
assert_contains "complete banner" 'migrate: complete' "$out"

echo "=== layout flattened ==="
assert_eq "after: 0 leaf XX/XX dirs"   "0" "$(find "$DB_ROOT/default/mft/files" -mindepth 2 -maxdepth 2 -type d 2>/dev/null | wc -l)"
assert_eq "after: 0 top-level XX dirs" "0" "$(find "$DB_ROOT/default/mft/files" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)"
assert_eq "after: 4 flat files"        "4" "$(find "$DB_ROOT/default/mft/files" -maxdepth 1 -mindepth 1 -type f | wc -l)"

echo "=== list-files / get-file work after migration ==="
$BIN start > /dev/null
sleep 0.5
out=$($BIN list-files default mft)
assert_contains "alpha.pdf listed"   'alpha.pdf'   "$out"
assert_contains "bravo.txt listed"   'bravo.txt'   "$out"
assert_contains "charlie.png listed" 'charlie.png' "$out"
assert_contains "delta.csv listed"   'delta.csv'   "$out"
assert_contains "total=4" '"total":4' "$out"

$BIN get-file default mft alpha.pdf /tmp/mft_alpha.$$ > /dev/null
got=$(cat /tmp/mft_alpha.$$)
rm -f /tmp/mft_alpha.$$
assert_eq "alpha.pdf content" "alpha" "$got"
$BIN stop > /dev/null
sleep 0.3

echo "=== second ./migrate run is idempotent on the files phase ==="
out=$($MIG 2>&1)
files_line=$(echo "$out" | grep '"status":"migrated"' | head -1)
assert_eq "second run files_moved=0"       "0" "$(field "$files_line" files_moved)"
assert_eq "second run objects_migrated=0"  "0" "$(field "$files_line" objects_migrated)"

echo "=== conflict handling: flat target already exists ==="
mkdir -p "$DB_ROOT/default/mft/files/ab/cd"
echo new-content > "$DB_ROOT/default/mft/files/ab/cd/alpha.pdf"  # collides w/ existing flat alpha.pdf
out=$($MIG 2>&1)
files_line=$(echo "$out" | grep '"status":"migrated"' | head -1)
assert_eq "conflict reported" "1" "$(field "$files_line" conflicts)"
assert_eq "no overwrite: alpha.pdf flat content unchanged" "alpha" "$(cat "$DB_ROOT/default/mft/files/alpha.pdf")"
[[ -f "$DB_ROOT/default/mft/files/ab/cd/alpha.pdf" ]] && pass "conflicting bucket leaf preserved" || fail "conflicting bucket leaf vanished"
rm -rf "$DB_ROOT/default/mft/files/ab"

echo
echo "=== TEARDOWN ==="
$BIN start > /dev/null
sleep 0.3
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
