/* src/test/cases/test_startup_validator.c
 * The startup metadata validator should refuse to bring the server up
 * when on-disk objects don't match dirs.conf / schema.conf / fields.conf.
 * Three failure modes verified, plus the clean-startup happy path.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>



/* Try to spawn a daemon that's expected to FAIL. Polls for up to 3s
   waiting for the child to exit non-zero. Returns:
     1 = child exited (validator did its job, refused to start)
     0 = child still running after 3s (validator did NOT block startup) */
static int spawn_expecting_failure(const char *base, const char *shard_db_abs,
                                   int port, TestEnv *env) {
    pid_t pid = fork();
    if (pid < 0) return 0;
    if (pid == 0) {
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    env->port = port; env->daemon_pid = pid;

    for (int i = 0; i < 30; i++) {
        int status;
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            env->daemon_pid = -1;
            /* Refused to start — exit code != 0 expected */
            return WIFEXITED(status) && WEXITSTATUS(status) != 0;
        }
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    /* Still running after 3s = validator didn't refuse */
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
    env->daemon_pid = -1;
    return 0;
}

static int test_startup_validator_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-val-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755); mkdir(db_root, 0755);
    char logs_dir[300]; snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    char env_path[300]; snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "w");
    if (!f) { ASSERT_TRUE(0, "open db.env"); tu_run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=2\n"
        "export TLS_ENABLE=0\n",
        db_root, port, base);
    fclose(f);

    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    /* === Phase 1: clean startup. Empty DB_ROOT, no objects, no problems. */
    TestEnv env = {0};
    snprintf(env.db_root, sizeof(env.db_root), "%s", db_root);
    pid_t pid = fork();
    if (pid < 0) { ASSERT_TRUE(0, "fork"); tu_run_cmd("rm -rf %s", base); return 1; }
    if (pid == 0) {
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    env.port = port; env.daemon_pid = pid;

    int ready = 0;
    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            if (tc_request(probe, "{\"mode\":\"db-dirs\"}", &r) == 0 && r) {
                ready = 1; free(r); tc_close(probe); break;
            }
            free(r); tc_close(probe);
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    ASSERT_TRUE(ready, "clean DB starts (validator passes empty-DB)");
    if (!ready) { kill(pid, SIGKILL); waitpid(pid, NULL, 0);
                  tu_run_cmd("rm -rf %s", base); return 1; }

    /* Create one object to give the validator something to validate. */
    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { kill(pid, SIGKILL); waitpid(pid, NULL, 0);
               tu_run_cmd("rm -rf %s", base); return 1; }
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"v\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"n:int\"]}", &resp); free(resp); resp = NULL;
    /* Insert a record so data/ has actual files. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"v\","
        "\"key\":\"k1\",\"value\":{\"n\":42}}", &resp); free(resp); resp = NULL;
    tc_close(tc);
    test_env_stop_keep(&env);

    /* === Phase 2: restart cleanly. Same metadata, validator passes. */
    pid = fork();
    if (pid == 0) { chdir(base); execl(shard_db_abs, shard_db_abs, "server", (char *)NULL); _exit(127); }
    env.daemon_pid = pid;
    ready = 0;
    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            if (tc_request(probe, "{\"mode\":\"db-dirs\"}", &r) == 0 && r) {
                ready = 1; free(r); tc_close(probe); break;
            }
            free(r); tc_close(probe);
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    ASSERT_TRUE(ready, "restart with intact metadata: validator passes");
    test_env_stop_keep(&env);

    /* === Phase 3: delete the schema.conf line, restart should refuse. */
    {
        char schema_path[400], tmp[400];
        snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
        snprintf(tmp, sizeof(tmp), "%s.t", schema_path);
        FILE *fin = fopen(schema_path, "r");
        FILE *fout = fopen(tmp, "w");
        if (fin && fout) {
            char line[512];
            while (fgets(line, sizeof(line), fin))
                if (strncmp(line, "default:v:", 10) != 0) fputs(line, fout);
        }
        if (fin) fclose(fin);
        if (fout) fclose(fout);
        rename(tmp, schema_path);
    }
    int refused = spawn_expecting_failure(base, shard_db_abs, port, &env);
    ASSERT_TRUE(refused, "validator refuses start when schema.conf line is missing");

    /* Restore the line so phase 4 can isolate the fields.conf-deletion case. */
    {
        char schema_path[400];
        snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
        FILE *sf = fopen(schema_path, "a");
        if (sf) { fputs("default:v:16:16\n", sf); fclose(sf); }
    }

    /* === Phase 4: delete fields.conf, restart should refuse. */
    {
        char fields_path[400];
        snprintf(fields_path, sizeof(fields_path), "%s/default/v/fields.conf", db_root);
        unlink(fields_path);
    }
    refused = spawn_expecting_failure(base, shard_db_abs, port, &env);
    ASSERT_TRUE(refused, "validator refuses start when fields.conf is missing");

    /* === Phase 5: schema.conf references a dir not in dirs.conf — soft
       warning, NOT fatal. The auth/route layer rejects unknown dirs
       before any read is dispatched, so a stale schema entry can't cause
       silent mis-routing. Earlier behavior (refused start) blocked
       operators on any DB that had outlived a removed test tenant. */
    {
        char fields_path[400];
        snprintf(fields_path, sizeof(fields_path), "%s/default/v/fields.conf", db_root);
        FILE *ff = fopen(fields_path, "w");
        if (ff) { fputs("n:int\n", ff); fclose(ff); }
    }
    {
        char schema_path[400];
        snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
        FILE *sf = fopen(schema_path, "a");
        if (sf) { fputs("ghost_tenant:obj:16:16\n", sf); fclose(sf); }
    }
    refused = spawn_expecting_failure(base, shard_db_abs, port, &env);
    ASSERT_TRUE(!refused, "stale schema.conf dir is warned, not refused");

    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-startup-validator", test_startup_validator_run)
