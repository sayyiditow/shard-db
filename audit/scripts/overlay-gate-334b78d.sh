#!/bin/bash
# audit/scripts/overlay-gate-334b78d.sh
#
# Overlay the audit fault-injection harness onto a scratch git worktree
# at 334b78d^ (parent of the durability-windows/bt-kf-inversion commit
# 334b78d, 2026-08-28) for the fault-gate pair (plan Phase 1 step 4,
# human decisions 3 + follow-up): the INTERPOSER-ONLY variant — this
# era predates the control channel, so the engine-seam scenarios (8/9)
# are compiled OUT via -DSHARD_AUDIT_NO_SEAMS on the shard-db-test link
# line; the seam scenarios stay on current code (default there).
#
# Additive edits against the worktree's own files (verified against
# 334b78d^ on 2026-11-19; the registry shapes are byte-identical to
# e69170b^'s, so the same patches apply):
#   - test_runner.h/.c + shard-db-test.c + bench: audit registry field,
#     --include-audit, 4-arg test_run_all
#   - build.sh: audit TU on the shard-db-test link line (+ the
#     NO_SEAMS define) and the faultlib .so tool target
#
# Usage: overlay-gate-334b78d.sh <src-repo-with-harness> <era-worktree>
set -e
SRC=${1:?usage: overlay-gate-334b78d.sh <src-repo-with-harness> <era-worktree>}
DST=${2:?usage: overlay-gate-334b78d.sh <src-repo-with-harness> <era-worktree>}

cd "$SRC"
mkdir -p "$DST/src/test/audit" "$DST/audit"
cp src/test/audit/audit_common.h "$DST/src/test/audit/"
cp src/test/audit/audit_fault_inject.c "$DST/src/test/audit/"
cp audit/faultlib.c "$DST/audit/"

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
    ("""int test_run_all(const char *filter, const char *exclude, int jobs);""",
     """int test_run_all(const char *filter, const char *exclude, int jobs,
                 int include_audit);"""),
    ("""#define TEST_REGISTER_EXCLUSIVE(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 1)""",
     """#define TEST_REGISTER_EXCLUSIVE(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 1, 0)

/* Audit-campaign cases: excluded from the default run-all; selected
   with --include-audit. */
#define TEST_REGISTER_AUDIT(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 0, 1)"""),
])

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

patch("src/bench/shard-db-bench.c", [
    ("test_run_all(NULL, NULL, 1)", "test_run_all(NULL, NULL, 1, 0)"),
])

# shard-db-test link line: audit TU + the NO_SEAMS define (inert for
# every other TU in this build).
patch("build.sh", [
    ("""gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test \\
    src/test/shard-db-test.c \\
    src/test/test_client.c \\
    src/test/test_runner.c \\
    src/test/fixtures.c \\""",
     """gcc $MODE_CFLAGS -DTEST_BUILD -DSHARD_AUDIT_NO_SEAMS -o shard-db-test \\
    src/test/shard-db-test.c \\
    src/test/test_client.c \\
    src/test/test_runner.c \\
    src/test/fixtures.c \\
    src/test/audit/audit_fault_inject.c \\"""),
    # Fault interposer tool target, before the bench section.
    ("""# shard-db-bench — bench runner. Same TCP+TLS client as shard-db-test,""",
     """# audit fault-injection interposer (audit campaign tooling)
gcc -shared -fPIC -O1 -g -Wall -o build/bin/audit-faultlib.so audit/faultlib.c -ldl

# shard-db-bench — bench runner. Same TCP+TLS client as shard-db-test,"""),
])
print("era overlay edits applied")
PYEOF

# Verify the overlay landed.
grep -q 'TEST_REGISTER_AUDIT' "$DST/src/test/test_runner.h"
grep -q 'include_audit' "$DST/src/test/test_runner.c"
grep -q 'SHARD_AUDIT_NO_SEAMS' "$DST/build.sh"
grep -q 'audit-faultlib.so' "$DST/build.sh"
grep -q 'AUD_SEAMS_COMPILED' "$DST/src/test/audit/audit_fault_inject.c"
echo "overlay complete: $DST"
