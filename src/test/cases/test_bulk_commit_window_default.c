/* The compiled BULK_COMMIT_WINDOW default is 16384 — the KFM2 marker
   entry ceiling (embedded.c db_defaults_set): biggest legal window =
   fewest per-window marker-publish fsyncs. Raised 1024 → 4096
   (2026-09-04 bulk-commit plan Task 7) → 16384 when KFM2 raised the
   marker entry ceiling. Asserted on a calloc'd scratch instance
   (test_kf_auto_defaults pattern) so neither db.env parsing nor case
   order can influence the result — the committed db.env legitimately
   pins its own operator value, and load_db_root leaves absent knobs
   untouched. The second half pins that load_db_root semantics: a
   db.env without the knob keeps the current value. Same fixture
   pattern as test-bulk-commit-window-config. */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "shard_db_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static int write_plain_env(const char *dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/db.env", dir);
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "DB_ROOT=/tmp/ignored\n");   /* no BULK_COMMIT_WINDOW */
    return fclose(f);
}

static int test_bulk_commit_window_default_run(void) {
    /* Compiled default: scratch instance, no env in the way. */
    ShardDb *scratch = calloc(1, sizeof(ShardDb));
    ASSERT_NOT_NULL(scratch, "scratch ShardDb alloc");
    if (!scratch) return 1;
    db_defaults_set(scratch);
    ASSERT_EQ_INT(scratch->bulk_commit_window, 16384,
                  "default commit window is 16384 (KFM2 marker entry ceiling)");
    free(scratch);

    /* Absent knob in db.env: load_db_root overrides only present knobs. */
    char fixture[] = "/tmp/shard-db-win-default-XXXXXX";
    char cwd[PATH_MAX], parsed_root[PATH_MAX];
    char *cwd_ok = getcwd(cwd, sizeof(cwd));
    ASSERT_NOT_NULL(cwd_ok, "capture cwd");
    if (!cwd_ok) return 1;
    char *fixture_ok = mkdtemp(fixture);
    ASSERT_NOT_NULL(fixture_ok, "create fixture");
    if (!fixture_ok) return 1;
    int saved = g_db->bulk_commit_window;
    chdir(fixture);
    ASSERT_EQ_INT(write_plain_env(fixture), 0, "write env without the knob");
    ASSERT_EQ_INT(load_db_root(parsed_root, sizeof(parsed_root)), 0,
                  "env parses");
    ASSERT_EQ_INT(g_db->bulk_commit_window, saved,
                  "absent knob keeps the current value");
    g_db->bulk_commit_window = saved;
    chdir(cwd);
    rmrf(fixture);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-bulk-commit-window-default", test_bulk_commit_window_default_run)
