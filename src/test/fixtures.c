/* src/test/fixtures.c */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "fixtures.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "test_client.h"

int test_pick_port(void) {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) return -1;
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = 0,
                                .sin_addr.s_addr = htonl(INADDR_LOOPBACK) };
    if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) { close(s); return -1; }
    socklen_t len = sizeof(addr);
    if (getsockname(s, (struct sockaddr *)&addr, &len) < 0) { close(s); return -1; }
    int port = ntohs(addr.sin_port);
    close(s);
    return port;
}

static int sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    return nanosleep(&ts, NULL);
}

static int wait_daemon_ready(int port, int timeout_ms) {
    int waited = 0;
    while (waited < timeout_ms) {
        TestClientCfg cfg = { .port = port, .connect_timeout_ms = 200 };
        TestClient *tc = tc_connect(&cfg);
        if (tc) {
            char *resp = NULL;
            if (tc_request(tc, "{\"mode\":\"db-dirs\"}", &resp) == 0) {
                free(resp); tc_close(tc); return 0;
            }
            tc_close(tc);
        }
        sleep_ms(50);
        waited += 50;
    }
    return -1;
}

static int run_cmd(const char *fmt, ...) {
    char cmd[2048];
    va_list ap; va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    return system(cmd);
}

int test_env_start(TestEnv *env) {
    if (!env) return -1;

    /* Unique DB_ROOT under <dir>/shard-db-test-<pid>-<idx>/db. Default
       <dir> is /tmp (fast, RAM-backed on Linux), but tmpfs is typically
       sized at ~50% of RAM — not enough for high-scale benches (25M+
       records). Override via SHARD_TEST_TMPDIR to point at a disk-backed
       path (e.g. /var/tmp, $HOME) when running large benches. */
    static int counter = 0;
    int idx = __atomic_fetch_add(&counter, 1, __ATOMIC_RELAXED);

    const char *tmpdir = getenv("SHARD_TEST_TMPDIR");
    if (!tmpdir || !tmpdir[0]) tmpdir = "/tmp";

    char base[512];
    snprintf(base, sizeof(base), "%s/shard-db-test-%d-%d",
             tmpdir, (int)getpid(), idx);
    snprintf(env->db_root, sizeof(env->db_root), "%s/db", base);

    /* Wipe + recreate the directory tree. */
    run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(env->db_root, 0755);

    /* db.env tells the daemon which port + paths to use. */
    char env_path[300];
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "w");
    if (!f) return -1;
    env->port = test_pick_port();
    if (env->port < 0) { fclose(f); return -1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=2\n"
        "export THREADS=0\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n",
        env->db_root, env->port, base);
    fclose(f);
    char logs_dir[400];
    snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    /* Find shard-db: prefer ./build/bin/, fall back to ./shard-db. The path
       is recorded with realpath() so child can chdir(base) safely. */
    const char *binary_rel = "./build/bin/shard-db";
    if (access(binary_rel, X_OK) != 0) binary_rel = "./shard-db";
    char binary_abs[PATH_MAX];
    if (!realpath(binary_rel, binary_abs)) {
        return -1;
    }

    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        /* Child: cd to base/ so daemon picks up the db.env we wrote there,
           then exec daemon in foreground mode. */
        chdir(base);
        /* Test-only env propagation: lets the migrate tests stage v1
           objects via create-object. Production daemons don't have
           this set and refuse storage_version=1. */
        setenv("SHARD_ALLOW_V1_CREATE", "1", 1);
        execl(binary_abs, binary_abs, "server", (char *)NULL);
        _exit(127);
    }
    env->daemon_pid = pid;

    if (wait_daemon_ready(env->port, 5000) != 0) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        return -1;
    }
    return 0;
}

int test_env_start_at(TestEnv *env, const char *db_root, int port) {
    if (!env || !db_root || port <= 0) return -1;

    /* Derive base = parent(db_root). Caller must have created it + db_root. */
    const char *slash = strrchr(db_root, '/');
    if (!slash || slash == db_root) return -1;
    size_t base_len = (size_t)(slash - db_root);
    char base[256];
    if (base_len >= sizeof(base)) return -1;
    memcpy(base, db_root, base_len);
    base[base_len] = '\0';

    snprintf(env->db_root, sizeof(env->db_root), "%s", db_root);
    env->port = port;

    /* (Re)write db.env. Idempotent — overwrites any existing file from a
       previous run with the same base. */
    char env_path[300];
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "w");
    if (!f) return -1;
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=2\n"
        "export THREADS=0\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n",
        env->db_root, env->port, base);
    fclose(f);
    char logs_dir[400];
    snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    const char *binary_rel = "./build/bin/shard-db";
    if (access(binary_rel, X_OK) != 0) binary_rel = "./shard-db";
    char binary_abs[PATH_MAX];
    if (!realpath(binary_rel, binary_abs)) return -1;

    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        chdir(base);
        setenv("SHARD_ALLOW_V1_CREATE", "1", 1);  /* test-only opt-in */
        execl(binary_abs, binary_abs, "server", (char *)NULL);
        _exit(127);
    }
    env->daemon_pid = pid;

    if (wait_daemon_ready(env->port, 5000) != 0) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        return -1;
    }
    return 0;
}

void test_env_kill(TestEnv *env) {
    if (!env || env->daemon_pid <= 0) return;
    kill(env->daemon_pid, SIGKILL);
    waitpid(env->daemon_pid, NULL, 0);
    env->daemon_pid = -1;
    /* No db_root cleanup — caller controls persistent state. */
}

/* Graceful SIGTERM stop, NO cleanup of db_root. Used by bench when
   persistent mode wants the daemon to flush counts/logs/etc to disk
   before exit, but the data tree must survive the bench process. */
void test_env_stop_keep(TestEnv *env) {
    if (!env || env->daemon_pid <= 0) return;
    kill(env->daemon_pid, SIGTERM);
    int reaped = 0;
    for (int i = 0; i < 50; i++) {
        if (waitpid(env->daemon_pid, NULL, WNOHANG) == env->daemon_pid) {
            reaped = 1;
            break;
        }
        sleep_ms(100);
    }
    if (!reaped) {
        kill(env->daemon_pid, SIGKILL);
        waitpid(env->daemon_pid, NULL, 0);
    }
    env->daemon_pid = -1;
    /* No db_root cleanup. */
}

void test_env_stop(TestEnv *env) {
    if (!env || env->daemon_pid <= 0) return;
    kill(env->daemon_pid, SIGTERM);
    /* Give it 5s to drain. */
    int reaped = 0;
    for (int i = 0; i < 50; i++) {
        if (waitpid(env->daemon_pid, NULL, WNOHANG) == env->daemon_pid) {
            reaped = 1;
            break;
        }
        sleep_ms(100);
    }
    if (!reaped) {
        kill(env->daemon_pid, SIGKILL);
        waitpid(env->daemon_pid, NULL, 0);
    }
    env->daemon_pid = -1;

    /* Best-effort cleanup of the tmp tree. db_root is "<base>/db";
       strrchr finds the last '/' so we can rm -rf the parent. */
    char base[256];
    const char *slash = strrchr(env->db_root, '/');
    if (slash && slash != env->db_root) {
        size_t base_len = (size_t)(slash - env->db_root);
        if (base_len < sizeof(base)) {
            memcpy(base, env->db_root, base_len);
            base[base_len] = '\0';
            run_cmd("rm -rf %s", base);
        }
    }
}
