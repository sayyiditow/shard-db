#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int reservation_child(const char *mode) {
    char path[PATH_MAX];
    if (!realpath("./build/bin/reservation_harness", path)) return -1;
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (!pid) { execl(path, path, mode, (char *)NULL); _exit(127); }
    for (int i = 0; i < 600; i++) {
        int status; pid_t got = waitpid(pid, &status, WNOHANG);
        if (got == pid) return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        if (got < 0 && errno != EINTR) break;
        struct timespec tick = {0, 50000000}; nanosleep(&tick, NULL);
    }
    kill(pid, SIGKILL);
    while (waitpid(pid, NULL, 0) < 0 && errno == EINTR) {}
    fprintf(stderr, "reservation harness timed out: %s\n", mode);
    return -1;
}
static int test_reservation_run(void) {
    const char *modes[] = {"lifecycle", "partial", "saturation", "recovery", "corrupt", "schema"};
    for (size_t i = 0; i < sizeof(modes) / sizeof(modes[0]); i++) {
        int rc = reservation_child(modes[i]);
        ASSERT_EQ_INT(rc, 0, modes[i]);
    }
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-write-reservation", test_reservation_run)
