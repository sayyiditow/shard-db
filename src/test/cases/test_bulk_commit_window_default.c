/* Red on base: the default BULK_COMMIT_WINDOW is 1024. Task 7 of
   docs/plans/2026-09-04-bulk-commit-throughput-and-durability.md raises it
   to 4096. Fixture db.env deliberately omits the knob so load_db_root
   resolves the compiled default. Same fixture pattern as
   test-bulk-commit-window-config. */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"

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
    ASSERT_EQ_INT(g_db->bulk_commit_window, 4096,
                  "default commit window is 4096 (RED on base: 1024)");
    g_db->bulk_commit_window = saved;
    chdir(cwd);
    rmrf(fixture);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-bulk-commit-window-default", test_bulk_commit_window_default_run)
