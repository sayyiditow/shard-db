/* src/test/shard-db-test.c */
#include <stdio.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: shard-db-test list | run <name> | run-all [--filter <glob>]\n");
        return 1;
    }
    if (argv[1][0] == 'l') { printf("# (no tests registered yet)\n"); return 0; }
    fprintf(stderr, "not implemented\n");
    return 1;
}
