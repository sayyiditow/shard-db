/* src/test/fixtures.h
 *
 * Shared lifecycle and query helpers for daemon-backed and process-local
 * test cases.
 */
#ifndef TEST_FIXTURES_H
#define TEST_FIXTURES_H
#include <pthread.h>
#include <sys/types.h>
#include "../db/shard_db.h"

typedef struct {
    char db_root[256];      /* /tmp/shard-db-test-<pid>-<n>/db */
    int  port;              /* picked from the OS-allocated range */
    pid_t daemon_pid;
    int  test_control_fd;   /* parent side; -1 when the selected daemon is production */
} TestEnv;

/* Allocate db_root + free port, write a minimal db.env, fork the daemon,
   wait until it accepts connections. Returns 0 on success. */
int test_env_start(TestEnv *env);

/* Same as test_env_start, but takes the QUERY_BUFFER_MB override
   explicitly instead of reading SHARD_TEST_QUERY_BUFFER_MB from the
   process environment — avoids a setenv/unsetenv race when tests run
   concurrently under parallel run-all. Pass NULL for "no override"
   (daemon default). */
int test_env_start_ex(TestEnv *env, const char *qbuf_mb_override);

/* Tell daemon-backed fixtures how many cases run concurrently. Standalone
   `run` leaves this unset, so daemon worker pools retain their auto defaults.
   Returns the previous value so a specialized case can restore it. */
int test_fixture_set_jobs(int jobs);

/* Stop the daemon (SIGTERM, then SIGKILL if it doesn't exit in 5s) and
   rm -rf the db_root. */
void test_env_stop(TestEnv *env);

/* Pick an ephemeral port by binding 127.0.0.1:0, getsockname()ing, and
   closing. Returns -1 on failure. */
int test_pick_port(void);

/* Spawn a daemon at a caller-supplied db_root + port, no automatic
   tmpdir / cleanup. Used by crash tests that need to spawn → kill →
   respawn against the same on-disk state. Caller is responsible for
   wiping db_root afterwards. Populates env->db_root / env->port /
   env->daemon_pid. Returns 0 on success. */
int test_env_start_at(TestEnv *env, const char *db_root, int port);

/* Deterministic TEST_BUILD daemon seam. Each operation returns 0 on a
   complete protocol exchange and -1 on EOF, malformed framing, or I/O error.
   `wait` blocks until the daemon-side hook reports the requested phase. */
int test_env_test_hook_install(TestEnv *env);
int test_env_test_hook_wait(TestEnv *env, int *out_under_kf_wrlock);
int test_env_test_hook_release(TestEnv *env);
int test_env_test_hook_clear(TestEnv *env);

/* SIGKILL the daemon immediately and reap. Does NOT clean up db_root.
   Mirrors a crash — no graceful drain, no in-flight write completion.
   For regular teardown use test_env_stop instead. */
void test_env_kill(TestEnv *env);

/* SIGTERM (graceful) the daemon, wait up to 5s, then SIGKILL if it
   didn't exit. Does NOT clean up db_root. Use this for persistent-data
   teardown where you need the daemon to flush state (counts, logs,
   sequences) but want to keep the on-disk tree. */
void test_env_stop_keep(TestEnv *env);


/* Shared test utilities — formerly duplicated as `static` in 20+ case
   files. Prefix `tu_` is "test utility". */
int   tu_run_cmd(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
char *tu_capture_cmd(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
char *tu_read_file(const char *path);
int   tu_file_exists(const char *path);
int   tu_parse_count(const char *resp);
ShardDb *test_get_process_db(void);
const char *test_get_process_db_root(void);
int   tu_pdb_request(ShardDb *db, const char *json, char **out_response);
int   tu_pdb_drop_object(ShardDb *db, const char *dir, const char *object);

/* Portable stand-in for pthread_timedjoin_np (glibc-only, unavailable on
   macOS). The target thread must call tu_join_signal_mark_done(js)
   immediately before it returns; tu_timed_join then waits on that signal
   with a deadline and performs the real (by then near-instant) join. */
typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int done;
} TuJoinSignal;

void tu_join_signal_init(TuJoinSignal *js);
void tu_join_signal_destroy(TuJoinSignal *js);
void tu_join_signal_mark_done(TuJoinSignal *js);

/* Returns 0 if the thread signaled completion within `seconds` (the
   thread is then joined for real). Returns ETIMEDOUT if it didn't — the
   thread is presumed still running/deadlocked and is NOT joined; `js`
   must not be destroyed in that case since the thread may still touch it. */
int tu_timed_join(pthread_t tid, TuJoinSignal *js, int seconds);

#endif
