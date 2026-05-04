/* src/test/fixtures.h
 *
 * Per-test daemon lifecycle. Each test gets its own DB_ROOT and PORT,
 * forks the daemon as a child process, and reaps it on cleanup.
 */
#ifndef TEST_FIXTURES_H
#define TEST_FIXTURES_H
#include <sys/types.h>

typedef struct {
    char db_root[256];      /* /tmp/shard-db-test-<pid>-<n>/db */
    int  port;              /* picked from the OS-allocated range */
    pid_t daemon_pid;
} TestEnv;

/* Allocate db_root + free port, write a minimal db.env, fork the daemon,
   wait until it accepts connections. Returns 0 on success. */
int test_env_start(TestEnv *env);

/* Stop the daemon (SIGTERM, then SIGKILL if it doesn't exit in 5s) and
   rm -rf the db_root. */
void test_env_stop(TestEnv *env);

/* Pick an ephemeral port by binding 127.0.0.1:0, getsockname()ing, and
   closing. Returns -1 on failure. */
int test_pick_port(void);

#endif
