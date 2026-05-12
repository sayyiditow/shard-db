#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/wait.h>

static int test_signal_handling_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "daemon spawn"); return 1; }

    int ret = kill(env.daemon_pid, SIGTERM);
    ASSERT_EQ_INT(ret, 0, "SIGTERM sent");

    int wstatus;
    pid_t wpid = waitpid(env.daemon_pid, &wstatus, 0);
    ASSERT_EQ_INT((int)wpid, (int)env.daemon_pid, "daemon reaped");
    ASSERT_TRUE(WIFEXITED(wstatus), "daemon exited normally after SIGTERM");

    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    if (tc) {
        ASSERT_TRUE(0, "connection should fail after SIGTERM");
        tc_close(tc);
    }

    test_env_stop_keep(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-signal-handling", test_signal_handling_run)
