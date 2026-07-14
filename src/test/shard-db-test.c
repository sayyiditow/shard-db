/* src/test/shard-db-test.c */
#include "test_runner.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr,
            "usage: shard-db-test list | run <name> | run-all [--filter <substr>] [--jobs N]\n");
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
        int jobs = 0;
        for (int i = 2; i + 1 < argc; i++) {
            if (strcmp(argv[i], "--filter") == 0) filter = argv[i + 1];
            else if (strcmp(argv[i], "--jobs") == 0) jobs = atoi(argv[i + 1]);
        }
        if (jobs <= 0) {
            long nproc = sysconf(_SC_NPROCESSORS_ONLN);
            jobs = (nproc > 0) ? (int)nproc : 4;
        }
        return test_run_all(filter, jobs) == 0 ? 0 : 1;
    }
    fprintf(stderr, "unknown subcommand: %s\n", cmd);
    return 1;
}
