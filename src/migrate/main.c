/* migrate — one-shot per-release upgrade runner.
 *
 * For 2026.05.1:
 *   1. Acquire flock on $DB_ROOT/.shard-db.lock (refuse if daemon is running).
 *   2. Run migrate_files() — lifts pre-2026.05.2 XX/XX file buckets into flat
 *      <obj>/files/ layout. Filesystem-only.
 *   3. Release flock so the daemon can re-take it.
 *   4. Spawn `./shard-db start` and poll until ready.
 *   5. Run `./shard-db reindex` to rebuild every B+ tree under the new
 *      per-shard layout shipped in 2026.05.1.
 *   6. Stop the daemon.
 *
 * Reads DB_ROOT from db.env in the current working directory. Each phase
 * prints progress; final exit is 0 on success, nonzero on any step failure.
 *
 * Future releases append migrations here as the schema evolves.
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <linux/limits.h>

#include "migrate.h"

/* Tiny db.env parser — line-oriented, picks up `export KEY="value"` and
   `KEY=value`. Quotes (single or double) trimmed. Unknown keys ignored. */
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

/* Acquire exclusive non-blocking flock so we can detect a running daemon. */
static int lock_db_root(const char *db_root, int *out_fd) {
    char lockpath[PATH_MAX];
    snprintf(lockpath, sizeof(lockpath), "%s/.shard-db.lock", db_root);
    int fd = open(lockpath, O_CREAT | O_WRONLY, 0644);
    if (fd < 0) {
        fprintf(stderr, "migrate: cannot open %s: %s\n", lockpath, strerror(errno));
        return -1;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        fprintf(stderr,
            "migrate: cannot acquire %s — is shard-db running? Stop it first.\n",
            lockpath);
        close(fd);
        return -1;
    }
    *out_fd = fd;
    return 0;
}

static void unlock_db_root(int fd) {
    if (fd >= 0) {
        flock(fd, LOCK_UN);
        close(fd);
    }
}

/* `./shard-db status` returns 0 when running. Poll for up to N seconds. */
static int wait_daemon_ready(int timeout_sec) {
    for (int i = 0; i < timeout_sec * 5; i++) {
        if (system("./shard-db status > /dev/null 2>&1") == 0) return 0;
        struct timespec ts = { 0, 200 * 1000000L }; /* 200 ms */
        nanosleep(&ts, NULL);
    }
    return -1;
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    char db_root[PATH_MAX];
    if (load_db_root("db.env", db_root, sizeof(db_root)) < 0) return 1;
    fprintf(stdout, "migrate: DB_ROOT=%s\n", db_root);

    int lock_fd = -1;

    /* Phase 1 — migrate-files (FS-direct, no daemon needed). */
    fprintf(stdout, "migrate: phase 1/2 — migrate-files (lift XX/XX hash buckets to flat)\n");
    if (lock_db_root(db_root, &lock_fd) < 0) return 1;
    int rc = migrate_files(db_root);
    unlock_db_root(lock_fd);
    if (rc != 0) {
        fprintf(stderr, "migrate: phase 1 failed (rc=%d)\n", rc);
        return 1;
    }

    /* Phase 2 — reindex. Spawn the daemon, run reindex, stop. */
    fprintf(stdout, "migrate: phase 2/2 — reindex (rebuild B+ trees under per-shard layout)\n");
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
