/* migrate — one-shot per-release upgrade runner.
 *
 * For 2026.05.5:
 *   1. Spawn `./shard-db start` and poll until ready.
 *   2. Run `./shard-db reindex` to rebuild every B+ tree under the new
 *      BTRH (value, hash)-sorted layout that 2026.05.5 expects.
 *   3. Stop the daemon.
 *
 * reindex is idempotent — running migrate on a fresh install or a
 * database already on BTRH format simply rewrites btrees in their
 * current format. No magic-number sniffing here.
 *
 * Reads DB_ROOT from db.env in the current working directory.
 */

#define _POSIX_C_SOURCE 200809L
#ifdef __APPLE__
#define _DARWIN_C_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <limits.h>
#include <sys/param.h>

static int load_db_root(const char *path, char *out, size_t out_sz) {
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "migrate: cannot open %s: %s\n", path, strerror(errno));
        return -1;
    }
    char line[4096];
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n")] = '\0';
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || !*p) continue;
        if (strncmp(p, "export ", 7) == 0) p += 7;
        if (strncmp(p, "DB_ROOT", 7) != 0) continue;
        char *eq = strchr(p, '=');
        if (!eq) continue;
        char *v = eq + 1;
        while (*v == ' ' || *v == '\t') v++;
        size_t vl = strlen(v);
        if (vl >= 2 && (v[0] == '"' || v[0] == '\'') && v[vl-1] == v[0]) {
            v[vl-1] = '\0'; v++;
        }
        snprintf(out, out_sz, "%s", v);
        fclose(f);
        return 0;
    }
    fclose(f);
    fprintf(stderr, "migrate: DB_ROOT not found in %s\n", path);
    return -1;
}

static int wait_daemon_ready(int timeout_sec) {
    for (int i = 0; i < timeout_sec * 5; i++) {
        if (system("./shard-db status > /dev/null 2>&1") == 0) return 0;
        struct timespec ts = { 0, 200 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return -1;
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    char db_root[PATH_MAX];
    if (load_db_root("db.env", db_root, sizeof(db_root)) < 0) return 1;
    fprintf(stdout, "migrate: DB_ROOT=%s\n", db_root);

    fprintf(stdout, "migrate: phase 1/1 — reindex (rebuild B+ trees under (value, hash) sort)\n");
    if (system("./shard-db start") != 0) {
        fprintf(stderr, "migrate: ./shard-db start failed\n");
        return 1;
    }
    if (wait_daemon_ready(30) < 0) {
        fprintf(stderr, "migrate: daemon never came up within 30s\n");
        system("./shard-db stop > /dev/null 2>&1");
        return 1;
    }

    int reindex_rc = system("./shard-db reindex");
    int stop_rc = system("./shard-db stop");

    if (reindex_rc != 0) {
        fprintf(stderr, "migrate: reindex failed (rc=%d)\n", reindex_rc);
        return 1;
    }
    if (stop_rc != 0) {
        fprintf(stderr, "migrate: warning — daemon stop returned %d; check status manually\n",
                stop_rc);
    }

    fprintf(stdout, "migrate: complete\n");
    return 0;
}
