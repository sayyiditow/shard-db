/* src/test/fixtures.c */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "fixtures.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "test_client.h"

/* test_pick_port() below has an unavoidable close-to-daemon-bind window.
   Parallel tests now run in separate processes, so reservations must be
   visible across workers. Keep an advisory per-port lock through the same
   five-second bind window used by the old reservation table. Locks are
   purged on subsequent allocations rather than held to process exit, which
   keeps the sequential coverage run from accumulating one file descriptor
   for every daemon-backed case. */
#define PORT_RESERVE_TTL_MS 5000
#define PORT_RESERVE_MAX    64

typedef struct { int fd; struct timespec claimed; } PortReservation;
static PortReservation g_port_reservations[PORT_RESERVE_MAX];
static int g_port_reservation_count = 0;
static pthread_mutex_t g_port_reservation_lock = PTHREAD_MUTEX_INITIALIZER;

static long port_reservation_age_ms(const struct timespec *claimed,
                                    const struct timespec *now) {
    return (now->tv_sec - claimed->tv_sec) * 1000L +
           (now->tv_nsec - claimed->tv_nsec) / 1000000L;
}

static int try_reserve_port(int port) {
    pthread_mutex_lock(&g_port_reservation_lock);
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    int i = 0;
    while (i < g_port_reservation_count) {
        if (port_reservation_age_ms(&g_port_reservations[i].claimed, &now) >
            PORT_RESERVE_TTL_MS) {
            close(g_port_reservations[i].fd);
            g_port_reservations[i] =
                g_port_reservations[--g_port_reservation_count];
            continue;
        }
        i++;
    }
    if (g_port_reservation_count == PORT_RESERVE_MAX) {
        pthread_mutex_unlock(&g_port_reservation_lock);
        return 0;
    }

    char path[128];
    snprintf(path, sizeof(path), "/tmp/shard-db-test-port-%d.lock", port);
    int fd = open(path, O_RDWR | O_CREAT, 0600);
    if (fd < 0) {
        pthread_mutex_unlock(&g_port_reservation_lock);
        return 0;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        close(fd);
        pthread_mutex_unlock(&g_port_reservation_lock);
        return 0;
    }
    /* Exec'd daemons must not hold a stale reservation past their parent
       test's short bind window. */
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    g_port_reservations[g_port_reservation_count++] =
        (PortReservation){ .fd = fd, .claimed = now };
    pthread_mutex_unlock(&g_port_reservation_lock);
    return 1;
}

int test_pick_port(void) {
    for (int attempt = 0; attempt < 20; attempt++) {
        int s = socket(AF_INET, SOCK_STREAM, 0);
        if (s < 0) return -1;
        struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = 0,
                                    .sin_addr.s_addr = htonl(INADDR_LOOPBACK) };
        if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) { close(s); return -1; }
        socklen_t len = sizeof(addr);
        if (getsockname(s, (struct sockaddr *)&addr, &len) < 0) { close(s); return -1; }
        int port = ntohs(addr.sin_port);
        close(s);
        if (try_reserve_port(port)) return port;
    }
    return -1;
}

static int sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    return nanosleep(&ts, NULL);
}

/* Diagnostic state: when wait_daemon_ready times out, the test fixture
   prints why. Filled by the last failed step. */
static __thread int g_wait_last_step = 0;   /* 1=connect 2=request */
static __thread int g_wait_last_errno = 0;
static int g_test_jobs = 0;

int test_fixture_set_jobs(int jobs) {
    int previous = g_test_jobs;
    g_test_jobs = jobs;
    return previous;
}

static int fixture_pool_size(void) {
    if (g_test_jobs <= 0) return 0;
    return g_test_jobs <= 4 ? 4 : 2;
}

static int wait_daemon_ready(int port, int timeout_ms) {
    int waited = 0;
    int last_step = 0, last_errno = 0;
    while (waited < timeout_ms) {
        TestClientCfg cfg = { .port = port, .connect_timeout_ms = 200 };
        TestClient *tc = tc_connect(&cfg);
        if (!tc) {
            last_step = 1; last_errno = errno;
        } else {
            char *resp = NULL;
            if (tc_request(tc, "{\"mode\":\"db-dirs\"}", &resp) == 0) {
                free(resp); tc_close(tc); return 0;
            }
            last_step = 2; last_errno = errno;
            tc_close(tc);
        }
        sleep_ms(50);
        waited += 50;
    }
    g_wait_last_step = last_step;
    g_wait_last_errno = last_errno;
    return -1;
}

/* Dump the captured daemon log file to stderr, then unlink. Called on
   daemon spawn failure so CI logs show the actual error. */
static void dump_daemon_log(const char *log_path) {
    FILE *f = fopen(log_path, "r");
    if (!f) return;
    fprintf(stderr, "\n--- daemon log (%s) ---\n", log_path);
    char buf[4096];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
        fwrite(buf, 1, n, stderr);
    fprintf(stderr, "--- end daemon log ---\n");
    fclose(f);
}

static int run_cmd(const char *fmt, ...) {
    char cmd[2048];
    va_list ap; va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    return system(cmd);
}

int test_env_start_ex(TestEnv *env, const char *qbuf_mb_override) {
    if (!env) return -1;
    /* First state init so `TestEnv env = {0}` callers never cause cleanup
       to close descriptor 0 on an early return. */
    env->test_control_fd = -1;

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

    /* Wipe + recreate the directory tree. */
    run_cmd("rm -rf %s", base);
    mkdir(base, 0755);

    /* Resolve to absolute. The fixture chdirs the daemon into `base`
       before exec, so a relative DB_ROOT in db.env would resolve to
       `base/<DB_ROOT>` from the daemon's POV — but the bench / test
       still references `env.db_root` from its own CWD (the caller's
       cwd, not `base`), producing a `base/db/.../base/db/...` double
       nesting visible in `du` (cosmetic) and breaking any post-bench
       path checks. realpath() collapses the path under the caller's
       cwd; everything written into db.env + env.db_root is then
       absolute and consistent across both processes. */
    char base_abs[PATH_MAX];
    if (realpath(base, base_abs)) {
        snprintf(base, sizeof(base), "%s", base_abs);
    }
    snprintf(env->db_root, sizeof(env->db_root), "%s/db", base);
    mkdir(env->db_root, 0755);

    /* db.env tells the daemon which port + paths to use. */
    char env_path[300];
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "w");
    if (!f) return -1;
    env->port = test_pick_port();
    if (env->port < 0) { fclose(f); return -1; }
    int pool_size = fixture_pool_size();
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=2\n"
        "export THREADS=2\n"
        /* MAX_CONCURRENT_QUERIES auto-derives from THREADS (slot_init()
           in config.c floors it at max(4, min(parallel_threads(),32))),
           so shrinking THREADS to 2 above also silently floors the
           query-admission semaphore at 4 — well below the concurrency
           several tests exercise (e.g. test-registry-single-flight's 16
           concurrent connections, test-parallel-index-integrity's 5).
           Pin it explicitly so the CPU-pool shrink doesn't reject
           legitimate concurrent test traffic with "server at capacity". */
        "export MAX_CONCURRENT_QUERIES=32\n"
        /* A standalone test keeps both pools in auto mode (0). During
           run-all, give lightly parallel runs four threads per daemon and
           reduce wider runs to two so their total thread count stays bounded. */
        "export WORKERS=%d\n"
        "export IO_THREADS=%d\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n",
        env->db_root, env->port, base, pool_size, pool_size);
    /* Optional per-test override: a test that needs to exercise the
       per-query memory cap (e.g. forcing a bitmap KeySet past budget)
       passes qbuf_mb_override via test_env_start_ex. NULL → daemon
       default. Kept out of TestEnv so the many uninitialised
       `TestEnv env;` callers are unaffected. */
    if (qbuf_mb_override && *qbuf_mb_override)
        fprintf(f, "export QUERY_BUFFER_MB=%s\n", qbuf_mb_override);
    fclose(f);
    char logs_dir[400];
    snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    /* Find shard-db: prefer ./build/bin/, fall back to ./shard-db. The path
       is recorded with realpath() so child can chdir(base) safely. The
       TEST_BUILD daemon (shard-db-test-server) is preferred when present so
       TCP cases reach the deterministic test-control seam; the production
       path stays as a fallback for builds that lack the test target. */
    const char *binary_rel = "./build/bin/shard-db";
    if (access(binary_rel, X_OK) != 0) binary_rel = "./shard-db";
    if (access("./build/bin/shard-db-test-server", X_OK) == 0)
        binary_rel = "./build/bin/shard-db-test-server";
    char binary_abs[PATH_MAX];
    if (!realpath(binary_rel, binary_abs)) {
        return -1;
    }

    /* Inherited anonymous Unix socketpair control channel for the TEST_BUILD
       daemon: the child keeps its endpoint across exec and invokes the test
       server as `server --test-control-fd <fd>`; the parent side lives in
       env->test_control_fd and is closed by test_env_stop / stop_keep /
       kill and on every start-failure path. The production-binary fallback
       passes no test-control argument and leaves test_control_fd == -1. */
    int is_test_server = (strstr(binary_abs, "/shard-db-test-server") != NULL);
    int child_ctl_fd = -1;
    if (is_test_server) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) return -1;
        env->test_control_fd = sv[0];
        child_ctl_fd = sv[1];
    }

    /* Capture daemon stdout+stderr to a log file so spawn failures can be
       diagnosed in CI (where each test's "daemon spawn" fails as a bare
       assertion otherwise). Path lives under base/, so test_env_stop's
       rm -rf cleans it up. */
    char dlog[400];
    snprintf(dlog, sizeof(dlog), "%s/daemon.log", base);

    pid_t pid = fork();
    if (pid < 0) {
        if (env->test_control_fd > 0) {
            close(env->test_control_fd);
            env->test_control_fd = -1;
        }
        if (child_ctl_fd >= 0) close(child_ctl_fd);
        return -1;
    }
    if (pid == 0) {
        /* Child: cd to base/ so daemon picks up the db.env we wrote there,
           then exec daemon in foreground mode. Redirect stdout+stderr to
           daemon.log so wait_daemon_ready failures can dump the actual
           error reason. */
        chdir(base);
        int lfd = open(dlog, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (lfd >= 0) {
            dup2(lfd, 1);
            dup2(lfd, 2);
            close(lfd);
        }
        if (env->test_control_fd > 0) close(env->test_control_fd);
        if (child_ctl_fd >= 0) {
            char fdarg[16];
            snprintf(fdarg, sizeof(fdarg), "%d", child_ctl_fd);
            execl(binary_abs, binary_abs, "server", "--test-control-fd",
                  fdarg, (char *)NULL);
        } else {
            execl(binary_abs, binary_abs, "server", (char *)NULL);
        }
        _exit(127);
    }
    if (child_ctl_fd >= 0) close(child_ctl_fd);
    env->daemon_pid = pid;

    /* Sanitizer builds make daemon startup much slower (TSan especially,
       ~5-15x); a full-parallel run-all burst of concurrent daemon spawns
       widens this further. Match test_env_start_at's restart-path wait,
       which already accounts for this. */
    if (wait_daemon_ready(env->port, 30000) != 0) {
        fprintf(stderr,
            "wait_daemon_ready: timeout on port %d (last_step=%d errno=%d %s)\n",
            env->port, g_wait_last_step, g_wait_last_errno,
            strerror(g_wait_last_errno));
        /* Check whether daemon process is still alive — if it crashed,
           waitpid(WNOHANG) returns >0; if alive, returns 0. */
        int wstatus = 0;
        pid_t r = waitpid(pid, &wstatus, WNOHANG);
        if (r == pid) {
            fprintf(stderr, "daemon pid=%d exited before ready: status=0x%x\n",
                    (int)pid, wstatus);
        } else {
            fprintf(stderr, "daemon pid=%d still running but not responsive\n",
                    (int)pid);
        }
        dump_daemon_log(dlog);
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        if (env->test_control_fd > 0) {
            close(env->test_control_fd);
            env->test_control_fd = -1;
        }
        return -1;
    }
    return 0;
}

int test_env_start(TestEnv *env) {
    return test_env_start_ex(env, getenv("SHARD_TEST_QUERY_BUFFER_MB"));
}

int test_env_start_at(TestEnv *env, const char *db_root, int port) {
    if (!env || !db_root || port <= 0) return -1;
    /* First state init — see test_env_start_ex. */
    env->test_control_fd = -1;

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

    /* Write db.env at <base>/ ONLY if no existing one is there. Persistent
       bench mode is often pointed at the user's working repo (base=".",
       env_path="./db.env"), where a pre-existing operator-managed db.env
       must be preserved — otherwise the bench silently nukes its full
       contents on every run. If db.env exists, honour its PORT so the
       bench client connects where the daemon will actually listen.

       Atomic via open(O_CREAT|O_EXCL) — no TOCTOU window between the
       check and the create. CodeQL flagged the earlier access()-then-
       fopen() pattern (CWE-367); this is the prescribed fix. */
    char env_path[300];
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    int wfd = open(env_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (wfd >= 0) {
        FILE *f = fdopen(wfd, "w");
        if (!f) { close(wfd); return -1; }
        fprintf(f,
            "export DB_ROOT=\"%s\"\n"
            "export PORT=%d\n"
            "export TIMEOUT=0\n"
            "export LOG_DIR=\"%s/logs\"\n"
            "export LOG_LEVEL=2\n"
            "export THREADS=2\n"
            /* See test_env_start_ex's comment: MAX_CONCURRENT_QUERIES
               auto-derives from THREADS, so pin it explicitly here too. */
            "export MAX_CONCURRENT_QUERIES=32\n"
            "export FCACHE_MAX=4096\n"
            "export TLS_ENABLE=0\n",
            env->db_root, env->port, base);
        fclose(f);
    } else if (errno == EEXIST) {
        /* Existing file — parse PORT= so env->port matches what the
           spawned daemon will bind to. Anything else we'd want to
           override (DB_ROOT, LOG_DIR) is already user-owned in this
           path. open(O_RDONLY) directly so there's no second stat. */
        int rfd = open(env_path, O_RDONLY);
        if (rfd >= 0) {
            FILE *fr = fdopen(rfd, "r");
            if (fr) {
                char line[512];
                while (fgets(line, sizeof(line), fr)) {
                    const char *p = line;
                    while (*p == ' ' || *p == '\t') p++;
                    if (strncmp(p, "export ", 7) == 0) p += 7;
                    if (strncmp(p, "PORT=", 5) == 0) {
                        int parsed = atoi(p + 5);
                        if (parsed > 0) env->port = parsed;
                        break;
                    }
                }
                fclose(fr);
            } else {
                close(rfd);
            }
        }
    } else {
        /* open() failed for some other reason (EACCES, ENOSPC, etc.). */
        return -1;
    }
    char logs_dir[400];
    snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    const char *binary_rel = "./build/bin/shard-db";
    if (access(binary_rel, X_OK) != 0) binary_rel = "./shard-db";
    if (access("./build/bin/shard-db-test-server", X_OK) == 0)
        binary_rel = "./build/bin/shard-db-test-server";
    char binary_abs[PATH_MAX];
    if (!realpath(binary_rel, binary_abs)) return -1;

    /* Inherited anonymous socketpair control channel — same rules as
       test_env_start_ex. */
    int is_test_server = (strstr(binary_abs, "/shard-db-test-server") != NULL);
    int child_ctl_fd = -1;
    if (is_test_server) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) return -1;
        env->test_control_fd = sv[0];
        child_ctl_fd = sv[1];
    }

    pid_t pid = fork();
    if (pid < 0) {
        if (env->test_control_fd > 0) {
            close(env->test_control_fd);
            env->test_control_fd = -1;
        }
        if (child_ctl_fd >= 0) close(child_ctl_fd);
        return -1;
    }
    if (pid == 0) {
        chdir(base);
        if (env->test_control_fd > 0) close(env->test_control_fd);
        if (child_ctl_fd >= 0) {
            char fdarg[16];
            snprintf(fdarg, sizeof(fdarg), "%d", child_ctl_fd);
            execl(binary_abs, binary_abs, "server", "--test-control-fd",
                  fdarg, (char *)NULL);
        } else {
            execl(binary_abs, binary_abs, "server", (char *)NULL);
        }
        _exit(127);
    }
    if (child_ctl_fd >= 0) close(child_ctl_fd);
    env->daemon_pid = pid;

    /* Restarting an existing database runs shared crash-recovery evidence
       checks (marker/sidecar replay, clean-lifecycle verification) before
       serving.  Sanitizer builds make that recovery path much slower;
       allow the restart helper to wait for the daemon to become ready
       without weakening the production startup path. */
    if (wait_daemon_ready(env->port, 30000) != 0) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        if (env->test_control_fd > 0) {
            close(env->test_control_fd);
            env->test_control_fd = -1;
        }
        return -1;
    }
    return 0;
}

void test_env_kill(TestEnv *env) {
    if (!env || env->daemon_pid <= 0) return;
    kill(env->daemon_pid, SIGKILL);
    waitpid(env->daemon_pid, NULL, 0);
    env->daemon_pid = -1;
    if (env->test_control_fd > 0) {
        close(env->test_control_fd);
        env->test_control_fd = -1;
    }
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
    if (env->test_control_fd > 0) {
        close(env->test_control_fd);
        env->test_control_fd = -1;
    }
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
    if (env->test_control_fd > 0) {
        close(env->test_control_fd);
        env->test_control_fd = -1;
    }

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


/* ---- Shared test utilities (formerly duplicated across case files) ---- */

int tu_run_cmd(const char *fmt, ...) {
    char cmd[2048];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    return system(cmd);
}

char *tu_capture_cmd(const char *fmt, ...) {
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    FILE *fp = popen(cmd, "r");
    if (!fp) return NULL;
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) { pclose(fp); return NULL; }
    int c;
    while ((c = fgetc(fp)) != EOF) {
        if (len + 1 >= cap) {
            cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); pclose(fp); return NULL; }
            buf = nb;
        }
        buf[len++] = (char)c;
    }
    buf[len] = '\0';
    pclose(fp);
    return buf;
}

char *tu_read_file(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;
    if (fseek(fp, 0, SEEK_END) != 0) { fclose(fp); return NULL; }
    long sz = ftell(fp);
    if (sz < 0) { fclose(fp); return NULL; }
    if (fseek(fp, 0, SEEK_SET) != 0) { fclose(fp); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(fp); return NULL; }
    size_t n = fread(buf, 1, (size_t)sz, fp);
    fclose(fp);
    buf[n] = '\0';
    return buf;
}

int tu_file_exists(const char *path) {
    /* Existence-only — matches the legacy `stat() == 0` semantics so
       callers passing directory paths (`indexes/<field>/`) work. */
    struct stat st;
    return (stat(path, &st) == 0) ? 1 : 0;
}

int tu_parse_count(const char *resp) {
    if (!resp) return -1;
    while (*resp == ' ' || *resp == '\n' || *resp == '\t') resp++;
    if (*resp == '{') {
        const char *p = strstr(resp, "\"count\":");
        return p ? atoi(p + 8) : -1;
    }
    return atoi(resp);
}

int tu_pdb_request(ShardDb *db, const char *json, char **out_response) {
    size_t out_len = 0;
    return shard_db_query(db, json, out_response, &out_len);
}

/* A process-local database outlives individual sequential runner invocations,
   so object state needs explicit teardown. Dispatch failures are reported in
   the JSON body even when shard_db_query() itself returns success. */
int tu_pdb_drop_object(ShardDb *db, const char *dir, const char *object) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"drop-object\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"if_exists\":true}", dir, object);
    char *resp = NULL;
    size_t out_len = 0;
    int rc = shard_db_query(db, req, &resp, &out_len);
    int failed = (rc != 0) || !resp || strstr(resp, "\"error\"") != NULL;
    shard_db_free_result(resp);
    return failed ? 1 : 0;
}

/* ---- Deterministic TEST_BUILD daemon seam ---- */

/* Private runner-side transport adapter for the TEST_BUILD daemon's
   test-control channel. Messages are fixed-size and private to this
   fixture + src/db/test_control.c (duplicated constants, no shared header).
   Helpers never retry, poll, sleep, use a fixed pathname, or mutate
   process-wide environment state. */
typedef struct {
    uint32_t kind;   /* INSTALL=1, RELEASE=2, CLEAR=3, ACK=4, REACHED=5 */
    int32_t  phase;  /* REACHED: 0=stale snapshot, 1=under kf wrlock; else 0 */
} TestHookMessage;

enum {
    TEST_HOOK_INSTALL = 1,
    TEST_HOOK_RELEASE = 2,
    TEST_HOOK_CLEAR   = 3,
    TEST_HOOK_ACK     = 4,
    TEST_HOOK_REACHED = 5,
};

static int test_hook_write_full(int fd, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    while (n > 0) {
        ssize_t w = write(fd, p, n);
        if (w > 0) {
            p += w;
            n -= (size_t)w;
            continue;
        }
        if (w < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

static int test_hook_read_full(int fd, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    while (n > 0) {
        ssize_t r = read(fd, p, n);
        if (r > 0) {
            p += r;
            n -= (size_t)r;
            continue;
        }
        if (r < 0 && errno == EINTR) continue;
        return -1; /* EOF or I/O error */
    }
    return 0;
}

int test_env_test_hook_install(TestEnv *env) {
    if (!env || env->test_control_fd < 0) return -1;
    TestHookMessage msg = { .kind = TEST_HOOK_INSTALL, .phase = 0 };
    if (test_hook_write_full(env->test_control_fd, &msg, sizeof(msg)) != 0)
        return -1;
    TestHookMessage rep = {0};
    if (test_hook_read_full(env->test_control_fd, &rep, sizeof(rep)) != 0)
        return -1;
    if (rep.kind != TEST_HOOK_ACK) return -1;
    return 0;
}

int test_env_test_hook_wait(TestEnv *env, int *out_under_kf_wrlock) {
    if (!env || env->test_control_fd < 0 || !out_under_kf_wrlock) return -1;
    TestHookMessage rep = {0};
    if (test_hook_read_full(env->test_control_fd, &rep, sizeof(rep)) != 0)
        return -1;
    if (rep.kind != TEST_HOOK_REACHED) return -1;
    if (rep.phase != 0 && rep.phase != 1) return -1;
    *out_under_kf_wrlock = rep.phase;
    return 0;
}

int test_env_test_hook_release(TestEnv *env) {
    if (!env || env->test_control_fd < 0) return -1;
    TestHookMessage msg = { .kind = TEST_HOOK_RELEASE, .phase = 0 };
    if (test_hook_write_full(env->test_control_fd, &msg, sizeof(msg)) != 0)
        return -1;
    TestHookMessage rep = {0};
    if (test_hook_read_full(env->test_control_fd, &rep, sizeof(rep)) != 0)
        return -1;
    if (rep.kind != TEST_HOOK_ACK) return -1;
    return 0;
}

int test_env_test_hook_clear(TestEnv *env) {
    if (!env || env->test_control_fd < 0) return -1;
    TestHookMessage msg = { .kind = TEST_HOOK_CLEAR, .phase = 0 };
    if (test_hook_write_full(env->test_control_fd, &msg, sizeof(msg)) != 0)
        return -1;
    TestHookMessage rep = {0};
    if (test_hook_read_full(env->test_control_fd, &rep, sizeof(rep)) != 0)
        return -1;
    if (rep.kind != TEST_HOOK_ACK) return -1;
    return 0;
}

void tu_join_signal_init(TuJoinSignal *js) {
    pthread_mutex_init(&js->lock, NULL);
    pthread_cond_init(&js->cond, NULL);
    js->done = 0;
}

void tu_join_signal_destroy(TuJoinSignal *js) {
    pthread_mutex_destroy(&js->lock);
    pthread_cond_destroy(&js->cond);
}

void tu_join_signal_mark_done(TuJoinSignal *js) {
    pthread_mutex_lock(&js->lock);
    js->done = 1;
    pthread_cond_broadcast(&js->cond);
    pthread_mutex_unlock(&js->lock);
}

int tu_timed_join(pthread_t tid, TuJoinSignal *js, int seconds) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += seconds;

    pthread_mutex_lock(&js->lock);
    int rc = 0;
    while (!js->done) {
        rc = pthread_cond_timedwait(&js->cond, &js->lock, &deadline);
        if (rc != 0) break;
    }
    pthread_mutex_unlock(&js->lock);
    if (rc != 0) return rc;
    return pthread_join(tid, NULL);
}
