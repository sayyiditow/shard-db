#!/bin/bash
# audit/scripts/coverage-baseline-finish.sh
#
# Campaign Phase 1 step 5 — FINAL step after the serial full-suite
# coverage run completes (run-all --jobs 1 under BUILD_MODE=coverage).
#
# Collection model (root-caused 2026-11-19, supersedes the "parallel
# clobber" hypothesis): test processes chdir into the tmpfs scratch root
# (src/test/fixtures.c:548/766) and every --coverage object path baked at
# compile time is RELATIVE (build.sh compiles -o build/obj/x.o), so each
# dumping process writes its .gcda against ITS OWN cwd — inside
# /tmp/shard-db-* — and the mandatory `rm -rf /tmp/shard-db-*` deletes
# them. The suite itself was green the whole time; only the counters were
# thrown away.
#
# Fix: run every coverage-consuming process with GCOV_PREFIX pointing at
# build/cov-gcda/ so ALL dumps (runner, case children, daemons under
# TEST_BUILD or the plain daemon binary) land in one tree regardless of
# cwd. The same env MUST wrap the serial suite run itself:
#   GCOV_PREFIX="$PWD/build/cov-gcda" ./build/bin/shard-db-test run-all --jobs 1
# (fresh/empty the tree first; merges accumulate inside it).
#
# gcno live in TWO places on this repo's build (matching CI):
#   build/obj/<tu>.gcno                     — per-TU objects (test/loop build)
#   ./<binary>-<tu>.gcno                    — single-invocation binaries
#                                             (shard-db, shard-db-test,
#                                             shard-db-test-server, bench)
# gcda basenames match their gcno, so each dump is paired with its gcno
# by basename (with a -suffix fallback for temp-object naming), and gcov
# runs per group; .gcov text files are collected per group before the
# next group overwrites them.
#
# Output: docs/plans/audit-artifacts/coverage-baseline.txt
#   (per-TU line/branch table, worst-first, labeled by object variant,
#    + never-executed functions).
#
# Run from the repo root after the serial suite:
#   ./audit/scripts/coverage-baseline-finish.sh
set -e
command -v python3 >/dev/null 2>&1 || { echo 'coverage-baseline-finish.sh: python3 is required (scratch ownership check and report merge)' >&2; exit 1; }

# Per-run work dir for gcov text/json and staged lists: fixed /tmp names
# collide between concurrent runs and let a rerun pick up a previous run's
# stale lists. Not named shard-db-* so no scratch cleanup can touch it.
COV_TMP=$(mktemp -d /tmp/cov-work-XXXXXX)
export COV_TMP
trap 'rm -rf "$COV_TMP" "${COV_START_MARK:-}"' EXIT
[ -f build/obj/btree.gcno ] || { echo "no gcno in build/obj — coverage build?" >&2; exit 1; }

GCOV_DIR="$(pwd)/build/cov-gcda"

# Scratch cleanup is scoped to the harness roots THIS run created, and
# only when their owner is gone: the audit harnesses name their roots
# with the creating case-child pid (/tmp/shard-db-audit{cr,fault,diff}-<pid>-*),
# and by the time the case returns, this run's children have exited, so
# a dead embedded pid means "our leak" while a live pid means a
# CONCURRENT run (or pid reuse — skipping is the safe direction).
# The shared /tmp/shard-db-audit-logs dir and the fixture mkdtemp roots
# never match these prefixes, so they are never touched; leftovers that
# fail the ownership check are listed, not deleted.
COV_START_MARK="$(mktemp)"
cov_clean_scratch() {
    # only roots created after this script started (never older triage roots)
    find /tmp -maxdepth 1 -newer "$COV_START_MARK" \( -name 'shard-db-auditcr-*' \
        -o -name 'shard-db-auditfault-*' \
        -o -name 'shard-db-auditdiff-*' \) 2>/dev/null |
    while IFS= read -r d; do
        pid=$(printf '%s' "$d" | sed -E 's#.*/shard-db-audit(cr|fault|diff)-([0-9]+).*#\2#')
        [ -n "$pid" ] || continue
        # os.kill distinguishes ESRCH (dead owner -> our leak, delete)
        # from EPERM/0 (live owner -> a concurrent run, leave alone);
        # a bare kill -0 cannot (EPERM reads as "dead").
        if python3 -c '
import os, sys
try:
    os.kill(int(sys.argv[1]), 0)
    print("live")
except ProcessLookupError:
    print("dead")
except PermissionError:
    print("live")  # exists, but not ours
' "$pid" | grep -qx live; then
            echo "NOTE: scratch $d has a live owner (pid $pid) — not ours, left alone"
        else
            rm -rf "$d"
        fi
    done
}

run_oracle() {  # run one oracle case under the SAME prefix env as the suite
    GCOV_PREFIX="$GCOV_DIR" GCOV_PREFIX_STRIP=0 \
        ./build/bin/shard-db-test run "$1" --include-audit \
        > "$COV_TMP/cov-oracle-$1.log" 2>&1 || \
        echo "NOTE: $1 exited nonzero (expected for audit-planner-diff on audit-base: AUD-QRY-1/2 divergences)"
    cov_clean_scratch
}

# Only needed when this script re-runs the oracle cases on a tree that
# has NO prior gcda (first finish); if the suite already ran under the
# prefix, SKIP_ORACLE=1 keeps their data and this script only regenerates
# the report.
if [ "${SKIP_ORACLE:-0}" != "1" ]; then
    for c in audit-crash-restart audit-planner-diff audit-fault-inject \
             audit-craft-grave-frontier audit-gate-resplit-bitmap; do
        echo "=== oracle under coverage: $c"
        run_oracle "$c"
    done
fi
cov_clean_scratch

n_gcda=$(find "$GCOV_DIR" -name '*.gcda' | wc -l)
echo "gcda files collected under $GCOV_DIR: $n_gcda"
[ "$n_gcda" -gt 0 ] || { echo "no gcda collected — did the suite run under GCOV_PREFIX?" >&2; exit 1; }

pair_gcno() {  # gcda path -> gcno dir (build/obj or repo root), empty if none
    local b gcda
    gcda="$1"; b=$(basename "$gcda" .gcda)
    if [ -f "build/obj/$b.gcno" ]; then echo "build/obj"; return; fi
    if [ -f "$b.gcno" ]; then echo "."; return; fi
    # temp-object fallback: <dumpbase>-<tu>.gcda already covered above;
    # last resort: suffix match on the tu name
    local tu=${b##*-}
    if [ -f "build/obj/$tu.gcno" ]; then echo "build/obj"; return; fi
    if [ -f "$tu.gcno" ]; then echo "."; return; fi
    echo ""
}

collect_group() {  # label, gcov -o dir, list of gcda files
    local label="$1" odir="$2"; shift 2
    [ -n "$label" ] || return 0
    rm -f ./*.gcov
    gcov -b -c -o "$odir" "$@" > "$COV_TMP/gcov-$label.txt" 2>&1 || true
    python3 - "$label" <<'PYEOF'
import os, re, sys, glob
label = sys.argv[1]
txt = open(os.environ['COV_TMP'] + f'/gcov-{label}.txt', errors='replace').read()
entries = []
for b in re.split(r"(?=File ')", txt):
    fm = re.search(r"File '([^']+)'", b)
    lm = re.search(r"Lines executed:([\d.]+)% of (\d+)", b)
    bm = re.search(r"Branches executed:([\d.]+)% of (\d+)", b)
    if fm and lm:
        entries.append({'file': fm.group(1),
                        'line_pct': float(lm.group(1)), 'line_n': int(lm.group(2)),
                        'br_pct': float(bm.group(1)) if bm else -1,
                        'br_n': int(bm.group(2)) if bm else 0})
entries = [e for e in entries if e['line_n'] > 0]
# .gcov files carry the never-exec function list for THIS group only
never = {}
for f in sorted(glob.glob('*.gcov')):
    fns = [m.group(1) for line in open(f, errors='replace')
           if line.startswith('function') and ' called 0 ' in line
           for m in [re.match(r"function (\S+) called 0", line)] if m]
    if fns:
        never[f] = fns
import json
json.dump({'label': label, 'entries': entries, 'never': never},
          open(os.environ['COV_TMP'] + f'/gcov-{label}.json', 'w'))
print(f"{label}: {len(entries)} blocks, {sum(len(v) for v in never.values())} never-exec")
PYEOF
}

# GCOV PAIRING MODEL (root-caused 2026-11-20): gcov does NOT read the
# explicitly-passed .gcda path — it re-derives the data file from the
# gcno's recorded object name INSIDE the -o directory ("cannot open
# data file, assuming not executed" when absent). The prefix tree's
# dumps are therefore invisible to a plain -o run, and stale .gcda
# left in the -o dir from an earlier build produce "stamp mismatch".
# Fix: STAGE every prefix-tree gcda next to its matching gcno (repo
# root or build/obj) before gcov, run the groups on the staged files,
# and clean them up afterwards. Within one binary the object names are
# unique, so staging is collision-free across the four binaries.
#
# Variant grouping (before staging): per-binary prefixes keep a
# source's several compiled variants from colliding on the same
# <source>.gcov output file.
GROUP_PREFIXES="shard-db shard-db-test-server shard-db-test shard-db-bench"
declare -A STAGED_OBJ=() STAGED_ROOT=()
n_staged=0
for pfx in $GROUP_PREFIXES; do
    var="GB_${pfx//-/_}"   # bash identifiers cannot carry dashes
    while read -r g; do
        b=$(basename "$g" .gcda)
        case "$b" in
            "$pfx"-*) ;;
            *) continue ;;
        esac
        # longest-prefix wins: skip only when a LONGER group prefix
        # matches too (shard-db-test-X matches "shard-db-" as well —
        # that shorter match must NOT steal the file)
        local_skip=0
        for other in $GROUP_PREFIXES; do
            [ "$other" = "$pfx" ] && continue
            case "$b" in
                "${other}-"*)
                    if [ "${#other}" -gt "${#pfx}" ]; then
                        local_skip=1
                        break
                    fi ;;
            esac
        done
        [ "$local_skip" = 1 ] && continue
        if [ -f "build/obj/$b.gcno" ]; then
            cp "$g" "build/obj/$b.gcda"
            STAGED_OBJ["build/obj/$b.gcda"]=1
        elif [ -f "$b.gcno" ]; then
            cp "$g" "./$b.gcda"
            STAGED_ROOT["./$b.gcda"]=1
            echo "./$b.gcda" >> "$COV_TMP/staged-$pfx.list"
        else
            echo "NOTE: no gcno for $b — dump skipped"
            continue
        fi
        n_staged=$((n_staged + 1))
    done < <(find "$GCOV_DIR" -name "${pfx}-*.gcda")
done
echo "staged $n_staged gcda next to their gcno"

# Per-group gcda lists (the staged paths), for gcov + .gcov isolation.
# Membership was decided at staging time (longest prefix wins) — do NOT
# re-derive it here by glob, or shard-db-* re-matches every binary.
mapfile -t GA < <(printf '%s\n' "${!STAGED_OBJ[@]}" | sort)
collect_group obj build/obj "${GA[@]:-__none__}"
for pfx in $GROUP_PREFIXES; do
    var="GB_${pfx//-/_}"
    if [ -f "$COV_TMP/staged-$pfx.list" ]; then
        mapfile -t "$var" < <(sort "$COV_TMP/staged-$pfx.list")
    else
        mapfile -t "$var" < /dev/null
    fi
    eval "collect_group $pfx . \"\${$var[@]:-__none__}\""
done

# cleanup: remove the staged gcda copies (the prefix tree keeps the
# originals for re-runs)
rm -f ./*.gcda 2>/dev/null || true
find build/obj -maxdepth 1 -name '*.gcda' -delete 2>/dev/null || true

python3 - <<'PYEOF'
import json, os
groups = []
for lbl in ('obj', 'shard-db', 'shard-db-test-server', 'shard-db-test',
            'shard-db-bench'):
    try:
        groups.append(json.load(open(os.environ['COV_TMP'] + f'/gcov-{lbl}.json')))
    except FileNotFoundError:
        pass
out = ["# Coverage baseline — campaign Phase 1 step 5 (2026-11-19; serial merge)",
       "# Runs: full suite run-all --jobs 1 (BUILD_MODE=coverage, GCOV_PREFIX",
       "# collection — see audit/scripts/coverage-baseline-finish.sh header)",
       "# + Phase-1 oracle cases. gcov -b -c per object group; rows labeled",
       "# by the object variant that produced them (a source compiles into",
       "# several binaries with differing bodies — e.g. bitmap.c TEST_BUILD).",
       "", "== Per-TU coverage, worst-first ==",
       f"{'lines%':>7} {'of':>7} {'branch%':>8} {'of':>7}  file [variant]"]
rows = []
for g in groups:
    for e in g['entries']:
        rows.append((e['line_pct'], -e['line_n'], e, g['label']))
rows.sort(key=lambda r: (r[0], r[1]))
for pct, _, e, lbl in rows:
    out.append(f"{e['line_pct']:7.2f} {e['line_n']:7d} {e['br_pct']:8.2f} {e['br_n']:7d}  {e['file']} [{lbl}]")
out.append("")
out.append("== Never-executed functions (per TU, per variant) ==")
total = 0
for g in groups:
    for f, fns in sorted(g['never'].items()):
        total += len(fns)
        out.append(f"--- [{g['label']}] {f}: {len(fns)} never-executed ---")
        out += [f"    {n}" for n in fns]
out.append("")
out.append(f"Total never-executed functions: {total}")
open('docs/plans/audit-artifacts/coverage-baseline.txt', 'w').write('\n'.join(out) + '\n')
print("coverage-baseline.txt written:", sum(len(g['entries']) for g in groups),
      "blocks,", total, "never-exec functions")
PYEOF

rm -f ./*.gcov
echo "next: SKIP_TESTS=1 ./build.sh (rebuild normal), then update the index"
