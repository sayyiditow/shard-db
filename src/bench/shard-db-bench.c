/* src/bench/shard-db-bench.c
 *
 * Bench runner. Reuses the test_runner registry/dispatch machinery
 * because each binary has its own g_head — a bench is just a TEST_REGISTER'd
 * function that prints latency stats instead of TAP assertions.
 */
#include "test_runner.h"
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    /* Line-buffer stdout so each printf flushes on '\n'. Without this,
       stdout pipes to a file get block-buffered (8 KB) and progress
       output appears all-at-once at process exit. Benches print
       progress while running — we want it visible immediately. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    if (argc < 2) {
        fprintf(stderr, "usage: shard-db-bench list | run <name> | run-all\n");
        return 1;
    }
    if (strcmp(argv[1], "list") == 0) {
        for (const TestCaseEntry *p = test_first(); p; p = p->next)
            printf("%s\n", p->name);
        return 0;
    }
    if (strcmp(argv[1], "run") == 0 && argc >= 3) return test_run_one(argv[2]);
    if (strcmp(argv[1], "run-all") == 0) return test_run_all(NULL);
    fprintf(stderr, "unknown subcommand: %s\n", argv[1]);
    return 1;
}
