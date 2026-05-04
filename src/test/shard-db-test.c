/* src/test/shard-db-test.c */
#include "test_runner.h"
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: shard-db-test list | run <name> | run-all [--filter <substr>]\n");
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
        for (int i = 2; i + 1 < argc; i++)
            if (strcmp(argv[i], "--filter") == 0) { filter = argv[i + 1]; break; }
        return test_run_all(filter) == 0 ? 0 : 1;
    }
    fprintf(stderr, "unknown subcommand: %s\n", cmd);
    return 1;
}
