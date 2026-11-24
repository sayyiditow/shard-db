#!/bin/bash
# audit/scripts/overlay-gate-163.sh
#
# Overlay the audit oracle harness onto a scratch git worktree at 163b36a
# (parent of the kf grave-frontier fix f3c6ee5) for the crash/restart
# fuzzer's known-bug gate (plan Phase 1 step 4). Test-infra files are
# byte-identical between 163b36a and main (verified 2026-11-19 with
# `git diff --stat 163b36a f19b17d -- <files>`), so they copy cleanly;
# build.sh edits are applied with sed against the WORKTREE's own build.sh
# (main's carries the grave-run test-case line that 163 predates).
#
# Usage: overlay-gate-163.sh <src-repo-with-harness> <gate-worktree-root>
set -e
SRC=${1:?usage: overlay-gate-163.sh <src-repo-with-harness> <gate-worktree>}
DST=${2:?usage: overlay-gate-163.sh <src-repo-with-harness> <gate-worktree>}

cd "$SRC"
mkdir -p "$DST/audit" "$DST/src/test/audit"
for f in src/test/test_runner.c src/test/test_runner.h \
         src/test/shard-db-test.c src/test/fixtures.c src/test/fixtures.h \
         src/db/test_control.c audit/faultlib.c \
         src/bench/shard-db-bench.c; do
    cp "$f" "$DST/$f"
done
cp src/test/audit/*.h src/test/audit/*.c "$DST/src/test/audit/"

# The current test_control.c also wires seams that exist only on newer
# daemons (btree range/walk open-failure injection, the Pass-1 route
# counter). The gate commit predates them, so neutralize those calls in the
# WORKTREE copy only: the crash/restart gate uses the kill/pause seams, which
# 163b36a already has. GET_GLOBALS stays answerable (unknown selectors reply).
python3 - "$DST/src/db/test_control.c" <<'PYEOF'
import re, sys
p = sys.argv[1]
s = open(p).read()
s = re.sub(r'btree_test_fail_nth_range_open_shard\([^;]*\);', '(void)msg;', s)
s = re.sub(r'btree_test_fail_walk_open_shard\([^;]*\);', '(void)msg;', s)
s = re.sub(r'igb_pass1_parallel_reset_for_test\(\);', '(void)msg;', s)
s = s.replace('(uint64_t)igb_pass1_parallel_taken_for_test()', '0')
s = re.sub(r'build_keyset_leaf_test_set_fail_alloc\(\(int\)[^;]*\);', '(void)msg;', s)
open(p, 'w').write(s)
PYEOF

# Wire the audit cases into the worktree's own shard-db-test link line.
sed -i '/test_bitmap_stream_find_flush_gate.c \\/a\    src\/test\/audit\/audit_crash_restart.c \\' "$DST/build.sh"
sed -i '/audit_crash_restart.c \\/a\    src\/test\/audit\/audit_planner_diff.c \\' "$DST/build.sh"
sed -i '/audit_planner_diff.c \\/a\    src\/test\/audit\/audit_fault_inject.c \\' "$DST/build.sh"
sed -i '/audit_fault_inject.c \\/a\    src\/test\/audit\/audit_craft_grave_frontier.c \\' "$DST/build.sh"

# Fault interposer tool target, inserted before the bench section.
sed -i '/^# shard-db-bench — bench runner/i\# audit fault-injection interposer (audit campaign tooling)\ngcc -shared -fPIC -O1 -g -Wall -o build/bin/audit-faultlib.so audit/faultlib.c -ldl\n' "$DST/build.sh"

# Verify the overlay landed.
grep -q 'audit/audit_crash_restart.c' "$DST/build.sh"
grep -q 'audit-faultlib.so' "$DST/build.sh"
grep -q 'TEST_REGISTER_AUDIT' "$DST/src/test/test_runner.h"
grep -q 'test_env_ctl_set_pause_phase' "$DST/src/test/fixtures.c"
echo "overlay complete: $DST"
