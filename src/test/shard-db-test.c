/* src/test/shard-db-test.c */
#include "test_runner.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char **argv) {
    /* Case children fork without exec, so these inherit down to every
       daemon-backed test and the daemons' grandchildren:
       - SIGPIPE ignored: a write to a daemon connection the peer closed
         must fail with EPIPE (a clean, visible tc_request failure), not
         kill the child with signal 13. Signal deaths are counted by the
         runner as phantom failures with no TAP line — the reported
         "crashed with signal 13" empty-output cases in parallel runs.
       - Unbuffered stdout: case stdout is a dup2'd file, which stdio
         fully buffers; a child that dies mid-case loses every buffered
         "ok" line. Flush-per-line so crash post-mortems see real output. */
    signal(SIGPIPE, SIG_IGN);
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc < 2) {
        fprintf(stderr,
            "usage: shard-db-test list | run <name> | run-all [--filter <substr>] [--exclude <name,...>] [--jobs N]\n");
        return 1;
    }
    const char *cmd = argv[1];
    if (strcmp(cmd, "list") == 0) {
        for (const TestCaseEntry *p = test_first(); p; p = p->next)
            printf("%s\n", p->name);
        return 0;
    }
    if (strcmp(cmd, "run") == 0 && argc >= 3) {
        return test_run_one(argv[2]);
    }
    if (strcmp(cmd, "run-all") == 0) {
        const char *filter = NULL;
        const char *exclude = NULL;
        int jobs = 0;
        for (int i = 2; i + 1 < argc; i++) {
            if (strcmp(argv[i], "--filter") == 0) filter = argv[i + 1];
            else if (strcmp(argv[i], "--exclude") == 0) exclude = argv[i + 1];
            else if (strcmp(argv[i], "--jobs") == 0) jobs = atoi(argv[i + 1]);
        }
        if (jobs <= 0) {
            long nproc = sysconf(_SC_NPROCESSORS_ONLN);
            jobs = (nproc > 0) ? (int)nproc : 4;
        }
        return test_run_all(filter, exclude, jobs) == 0 ? 0 : 1;
    }
    fprintf(stderr, "unknown subcommand: %s\n", cmd);
    return 1;
}
