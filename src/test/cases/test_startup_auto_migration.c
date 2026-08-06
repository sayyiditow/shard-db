#include "test_assert.h"
#include "test_runner.h"
#include "fixtures.h"
#include "test_client.h"
#include "types.h"
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
#include <dirent.h>

/* Fresh db_root: first start writes .version with the compiled-in
   SHARD_DB_VERSION; second start (versions now match) is a fast no-op —
   .version content is unchanged. */
static int test_startup_migration_bootstraps_version_file(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;
    test_env_stop_keep(&env);

    char vpath[PATH_MAX];
    snprintf(vpath, sizeof(vpath), "%s/.version", saved_db_root);
    char buf[64];
    FILE *f = fopen(vpath, "r");
    ASSERT_TRUE(f != NULL, ".version file exists after first start");
    if (f) { ASSERT_TRUE(fgets(buf, sizeof(buf), f) != NULL, "read .version"); fclose(f); }
    buf[strcspn(buf, "\n")] = '\0';
    ASSERT_EQ_STR(buf, SHARD_DB_VERSION, ".version matches the compiled-in version");

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "second start with matching .version succeeds");
    test_env_stop(&env);
    return 0;
}

/* A .version file claiming a newer release than this binary must refuse
   to start with a clear message, and must not touch any data. */
static int test_startup_refuses_downgrade(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;
    /* Stop before any direct filesystem mutation — same ordering as
       test_durability_corrupt_marker_policy, so nothing races the live
       daemon's own view of schema.conf. */
    test_env_stop_keep(&env);

    char tenant[PATH_MAX];
    snprintf(tenant, sizeof(tenant), "%s/tenant", saved_db_root);
    ASSERT_EQ_INT(mkdir(tenant, 0755), 0, "created tenant metadata");
    char schema[PATH_MAX];
    snprintf(schema, sizeof(schema), "%s/schema.conf", saved_db_root);
    FILE *sf = fopen(schema, "w");
    ASSERT_TRUE(sf != NULL, "created nonempty schema metadata");
    if (sf) { fputs("tenant:obj:8:16:2:1\n", sf); fclose(sf); }

    char vpath[PATH_MAX];
    snprintf(vpath, sizeof(vpath), "%s/.version", saved_db_root);
    FILE *f = fopen(vpath, "w");
    ASSERT_TRUE(f != NULL, "opened .version for overwrite");
    if (f) { fprintf(f, "9999.12.1\n"); fclose(f); }

    int rc = test_env_start_at(&env, saved_db_root, saved_port);
    ASSERT_TRUE(rc != 0, "daemon refuses to start against a newer .version");
    if (rc == 0) test_env_stop(&env);
    return 0;
}

/* Reindex is folded into this release's version-triggered migration batch.
   Simulate "wrong/stale on-disk index data"
   directly by deleting the indexed field's shard files while the daemon
   is stopped — index.conf (the field-list descriptor) is untouched, only
   the .idx shard content is gone — then confirm a version-triggered
   restart rebuilds them. A plain matching-version restart (see
   test_startup_migration_bootstraps_version_file) must not touch
   indexes at all; reindex only runs inside the version-gated batch. */
static int test_startup_migration_reindexes_stale_indexes(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_TRUE(tc != NULL, "connected to daemon");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"ridx\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:64\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: ridx");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"t\",\"object\":\"ridx\","
        "\"fields\":[\"name\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"ridx\","
        "\"key\":\"k1\",\"value\":{\"name\":\"alice\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);

    test_env_stop_keep(&env);

    char idx_dir[PATH_MAX];
    snprintf(idx_dir, sizeof(idx_dir), "%s/t/ridx/indexes/name", saved_db_root);
    DIR *d = opendir(idx_dir);
    ASSERT_TRUE(d != NULL, "index shard directory exists before corruption");
    int deleted = 0;
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            char fpath[PATH_MAX];
            snprintf(fpath, sizeof(fpath), "%s/%s", idx_dir, e->d_name);
            if (unlink(fpath) == 0) deleted++;
        }
        closedir(d);
    }
    ASSERT_TRUE(deleted > 0,
                "deleted existing index shard file(s) to simulate stale data");

    char vpath[PATH_MAX];
    snprintf(vpath, sizeof(vpath), "%s/.version", saved_db_root);
    FILE *f = fopen(vpath, "w");
    ASSERT_TRUE(f != NULL, "opened .version for downgrade-stamp");
    if (f) { fprintf(f, "2026.07.3\n"); fclose(f); }

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "version-triggered restart succeeds");

    d = opendir(idx_dir);
    int has_file = 0;
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            has_file = 1;
            break;
        }
        closedir(d);
    }
    ASSERT_TRUE(has_file, "index shard rebuilt by version-triggered reindex");

    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("startup-migration-bootstrap", test_startup_migration_bootstraps_version_file)
TEST_REGISTER("startup-migration-refuses-downgrade", test_startup_refuses_downgrade)
TEST_REGISTER("startup-migration-reindexes-stale-indexes",
              test_startup_migration_reindexes_stale_indexes)

static int test_startup_refuses_trailing_version_content(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;
    test_env_stop_keep(&env);

    char schema[PATH_MAX];
    snprintf(schema, sizeof(schema), "%s/schema.conf", saved_db_root);
    FILE *sf = fopen(schema, "w");
    ASSERT_TRUE(sf != NULL, "created nonempty schema metadata");
    if (sf) { fputs("tenant:obj:8:16:2:1\n", sf); fclose(sf); }

    char vpath[PATH_MAX];
    snprintf(vpath, sizeof(vpath), "%s/.version", saved_db_root);
    FILE *vf = fopen(vpath, "w");
    ASSERT_TRUE(vf != NULL, "opened .version for malformed-content test");
    if (vf) { fputs("2026.08.1\njunk\n", vf); fclose(vf); }

    int rc = test_env_start_at(&env, saved_db_root, saved_port);
    ASSERT_TRUE(rc != 0, "daemon refuses malformed version evidence");
    if (rc == 0) test_env_stop(&env);
    return 0;
}

TEST_REGISTER("startup-migration-refuses-malformed-version",
              test_startup_refuses_trailing_version_content)

static int test_empty_root_bypasses_minimum_and_downgrade(void) {
    ASSERT_EQ_INT(shard_db_version_decide("9999.12.1", 1, 1,
                                          "2026.08.1", "2026.08.1", 1),
                  SHARD_DB_VERSION_STAMP_ONLY,
                  "empty root bootstraps even with newer evidence");
    return 0;
}

static int test_missing_nonempty_version_runs_legacy_migration(void) {
    ASSERT_EQ_INT(shard_db_version_decide(NULL, 0, 0,
                                          "2026.08.1", "2026.08.1", 1),
                  SHARD_DB_VERSION_RUN_MIGRATION,
                  "unversioned data runs migration because old releases lacked .version");
    ASSERT_EQ_INT(shard_db_version_decide("2026.07.2", 1, 0,
                                          "2026.08.1", "2026.07.3", 1),
                  SHARD_DB_VERSION_RUN_MIGRATION,
                  "minimum source version is informational in this release");
    return 0;
}

static int test_older_version_without_migration_only_stamps(void) {
    ASSERT_EQ_INT(shard_db_version_decide("2026.07.1", 1, 0,
                                          "2026.08.1", "", 0),
                  SHARD_DB_VERSION_STAMP_ONLY,
                  "older supported version without migration stamps only");
    return 0;
}

TEST_REGISTER("startup-migration-empty-version-exception",
              test_empty_root_bypasses_minimum_and_downgrade)
TEST_REGISTER("startup-migration-minimum-version",
              test_missing_nonempty_version_runs_legacy_migration)
TEST_REGISTER("startup-migration-no-step",
              test_older_version_without_migration_only_stamps)
