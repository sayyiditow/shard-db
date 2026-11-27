#!/bin/bash
# audit/scripts/overlay-gate-e69170b.sh
#
# Overlay the audit oracle harness onto a scratch git worktree at
# e69170b^ (parent of "fix: remap bitmap slots across keyfile resplit")
# for the planner-gate pair (plan Phase 1 step 4, human decision 2).
# UNLIKE the 163b36a overlay (where the infra files were byte-identical
# to main and copied wholesale), this era drifts ~448 lines across the
# infra files, so every edit is ADDITIVE against the worktree's own
# file content (verified against faa8340 on 2026-11-19):
#   - test_runner.h/.c + shard-db-test.c + bench: TEST_REGISTER_AUDIT
#     registry field, --include-audit flag, 4-arg test_run_all
#   - build.sh: audit TUs on the shard-db-test link line
# Copied verbatim: src/test/audit/audit_common.h (wire-only),
# audit_gate_resplit_bitmap.c (the gate case; uses only era-present
# fixtures: test_env_start_at/stop_keep, test_env_ctl_set_kf_seams,
# test_pick_port), audit_planner_diff.c (era sanity run).
# NOT copied: audit_crash_restart.c / audit_fault_inject.c — they need
# the pause-phase selector 8 / bmfix fault seam, which this era's
# test_control.c and fixtures predate.
#
# Usage: overlay-gate-e69170b.sh <src-repo-with-harness> <era-worktree>
set -e
SRC=${1:?usage: overlay-gate-e69170b.sh <src-repo-with-harness> <era-worktree>}
DST=${2:?usage: overlay-gate-e69170b.sh <src-repo-with-harness> <era-worktree>}

cd "$SRC"
mkdir -p "$DST/src/test/audit"
cp src/test/audit/audit_common.h "$DST/src/test/audit/"
cp src/test/audit/audit_gate_resplit_bitmap.c "$DST/src/test/audit/"
cp src/test/audit/audit_planner_diff.c "$DST/src/test/audit/"

python3 - "$DST" <<'PYEOF'
import sys
dst = sys.argv[1]

def patch(path, subs):
    with open(f"{dst}/{path}") as f:
        src = f.read()
    for old, new in subs:
        assert old in src, f"{path}: pattern not found: {old[:60]!r}"
        src = src.replace(old, new, 1)
    with open(f"{dst}/{path}", "w") as f:
        f.write(src)

# ---- test_runner.h: audit registry field + macros ----
patch("src/test/test_runner.h", [
    ("""typedef struct TestCaseEntry {
    const char *name;
    TestFn fn;
    /* An exclusive case runs alone in the process-pool scheduler. */
    int exclusive;
    struct TestCaseEntry *next;
} TestCaseEntry;""",
     """typedef struct TestCaseEntry {
    const char *name;
    TestFn fn;
    /* An exclusive case runs alone in the process-pool scheduler. */
    int exclusive;
    /* Audit-campaign case: excluded from the default run-all, selected
       with --include-audit (2026-11-19 audit campaign tooling). */
    int audit;
    struct TestCaseEntry *next;
} TestCaseEntry;"""),
    ("""#define TEST_REGISTER_IMPL(name_, fn_, exclusive_)                   \\
    static int fn_(void);                                            \\
    static TestCaseEntry _tce_##fn_ = { name_, fn_, exclusive_, NULL }; \\
    __attribute__((constructor)) static void _tcr_##fn_(void) {      \\
        test_register(&_tce_##fn_);                                  \\
    }

#define TEST_REGISTER(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 0)""",
     """#define TEST_REGISTER_IMPL(name_, fn_, exclusive_, audit_)           \\
    static int fn_(void);                                            \\
    static TestCaseEntry _tce_##fn_ =                                \\
        { name_, fn_, exclusive_, audit_, NULL };                    \\
    __attribute__((constructor)) static void _tcr_##fn_(void) {      \\
        test_register(&_tce_##fn_);                                  \\
    }

#define TEST_REGISTER(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 0, 0)"""),
    ("""#define TEST_REGISTER_EXCLUSIVE(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 1)""",
     """#define TEST_REGISTER_EXCLUSIVE(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 1, 0)

/* Audit-campaign cases: excluded from the default run-all; selected
   with --include-audit. */
#define TEST_REGISTER_AUDIT(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 0, 1)"""),
])

# ---- test_runner.h: test_run_all prototype gains include_audit ----
patch("src/test/test_runner.h", [
    ("""int test_run_all(const char *filter, const char *exclude, int jobs);""",
     """int test_run_all(const char *filter, const char *exclude, int jobs,
                 int include_audit);"""),
])

# ---- test_runner.c: include_audit plumbed through ----
patch("src/test/test_runner.c", [
    ("""static int collect_matching(const char *filter, const char *exclude,
                            TestCaseEntry ***out) {""",
     """static int collect_matching(const char *filter, const char *exclude,
                            int include_audit, TestCaseEntry ***out) {"""),
    ("""        if (filter && !strstr(p->name, filter)) continue;
        if (is_excluded(p->name, exclude)) continue;""",
     """        if (filter && !strstr(p->name, filter)) continue;
        if (is_excluded(p->name, exclude)) continue;
        if (p->audit && !include_audit) continue;"""),
    ("""int test_run_all(const char *filter, const char *exclude, int jobs) {
    test_fixture_set_jobs(jobs);
    TestCaseEntry **cases = NULL;
    int n = collect_matching(filter, exclude, &cases);""",
     """int test_run_all(const char *filter, const char *exclude, int jobs,
                 int include_audit) {
    test_fixture_set_jobs(jobs);
    TestCaseEntry **cases = NULL;
    int n = collect_matching(filter, exclude, include_audit, &cases);"""),
])

# ---- shard-db-test.c: --include-audit flag ----
patch("src/test/shard-db-test.c", [
    ("""        const char *filter = NULL;
        const char *exclude = NULL;
        int jobs = 0;
        for (int i = 2; i + 1 < argc; i++) {
            if (strcmp(argv[i], "--filter") == 0) filter = argv[i + 1];
            else if (strcmp(argv[i], "--exclude") == 0) exclude = argv[i + 1];
            else if (strcmp(argv[i], "--jobs") == 0) jobs = atoi(argv[i + 1]);
        }""",
     """        const char *filter = NULL;
        const char *exclude = NULL;
        int jobs = 0;
        int include_audit = 0;
        for (int i = 2; i + 1 < argc; i++) {
            if (strcmp(argv[i], "--filter") == 0) filter = argv[i + 1];
            else if (strcmp(argv[i], "--exclude") == 0) exclude = argv[i + 1];
            else if (strcmp(argv[i], "--jobs") == 0) jobs = atoi(argv[i + 1]);
            else if (strcmp(argv[i], "--include-audit") == 0)
                include_audit = 1;
        }"""),
    ("""        return test_run_all(filter, exclude, jobs) == 0 ? 0 : 1;""",
     """        return test_run_all(filter, exclude, jobs, include_audit) == 0
            ? 0 : 1;"""),
])

# ---- bench caller: mechanical signature update ----
patch("src/bench/shard-db-bench.c", [
    ("test_run_all(NULL, NULL, 1)", "test_run_all(NULL, NULL, 1, 0)"),
])

# ---- build.sh: audit TUs on the shard-db-test link line ----
patch("build.sh", [
    ("""    src/test/cases/test_bitmap_stream_find_flush_gate.c \\""",
     """    src/test/cases/test_bitmap_stream_find_flush_gate.c \\
    src/test/audit/audit_gate_resplit_bitmap.c \\
    src/test/audit/audit_planner_diff.c \\"""),
])
print("era overlay edits applied")
PYEOF

# Verify the overlay landed.
grep -q 'TEST_REGISTER_AUDIT' "$DST/src/test/test_runner.h"
grep -q 'include_audit' "$DST/src/test/test_runner.c"
grep -q 'audit_gate_resplit_bitmap.c' "$DST/build.sh"
grep -q 'TEST_REGISTER_AUDIT' "$DST/src/test/audit/audit_gate_resplit_bitmap.c"
echo "overlay complete: $DST"
