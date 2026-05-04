/* src/bench/shard-db-bench.c */
#include <stdio.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: shard-db-bench list | run <name> | run-all\n");
        return 1;
    }
    if (argv[1][0] == 'l') { printf("# (no benches registered yet)\n"); return 0; }
    fprintf(stderr, "not implemented\n");
    return 1;
}
