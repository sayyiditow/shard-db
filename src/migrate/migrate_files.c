/* migrate-files — extracted from src/db/query.c (pre-2026.05.2 layout fix).
   Filesystem-only, no daemon required, no shared state with the daemon.
   Linked into the ./migrate binary; not part of shard-db. */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <linux/limits.h>

#include "migrate.h"

static uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static int hex2_name(const char *s) {
    if (strlen(s) != 2) return 0;
    return ((s[0] >= '0' && s[0] <= '9') || (s[0] >= 'a' && s[0] <= 'f')) &&
           ((s[1] >= '0' && s[1] <= '9') || (s[1] >= 'a' && s[1] <= 'f'));
}

/* Migrate one object's files dir. Returns count of files moved (>=0).
   Conflicts (flat target already exists) are logged to stderr and skipped —
   the bucket leaf is left in place for manual review. */
static int migrate_object_files(const char *db_root, const char *dir,
                                const char *obj, int *conflicts_out) {
    char files_dir[PATH_MAX];
    snprintf(files_dir, sizeof(files_dir), "%s/%s/%s/files", db_root, dir, obj);
    DIR *d1 = opendir(files_dir);
    if (!d1) return 0;

    int moved = 0;
    int conflicts = 0;
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (!hex2_name(e1->d_name)) continue;
        char sub1[PATH_MAX];
        snprintf(sub1, sizeof(sub1), "%s/%s", files_dir, e1->d_name);
        DIR *d2 = opendir(sub1);
        if (!d2) continue;
        struct dirent *e2;
        while ((e2 = readdir(d2))) {
            if (!hex2_name(e2->d_name)) continue;
            char sub2[PATH_MAX];
            snprintf(sub2, sizeof(sub2), "%s/%s", sub1, e2->d_name);
            DIR *d3 = opendir(sub2);
            if (!d3) continue;
            struct dirent *e3;
            while ((e3 = readdir(d3))) {
                if (e3->d_name[0] == '.') continue;
                char src[PATH_MAX], dst[PATH_MAX];
                snprintf(src, sizeof(src), "%s/%s", sub2, e3->d_name);
                snprintf(dst, sizeof(dst), "%s/%s", files_dir, e3->d_name);
                struct stat st;
                if (stat(dst, &st) == 0) {
                    fprintf(stderr,
                        "migrate-files: skip %s/%s/%s (flat target exists at %s)\n",
                        dir, obj, e3->d_name, dst);
                    conflicts++;
                    continue;
                }
                if (rename(src, dst) != 0) {
                    fprintf(stderr,
                        "migrate-files: rename failed for %s -> %s: %s\n",
                        src, dst, strerror(errno));
                    continue;
                }
                moved++;
            }
            closedir(d3);
            rmdir(sub2);
        }
        closedir(d2);
        rmdir(sub1);
    }
    closedir(d1);
    if (conflicts_out) *conflicts_out += conflicts;
    return moved;
}

int migrate_files(const char *db_root) {
    char scpath[PATH_MAX];
    snprintf(scpath, sizeof(scpath), "%s/schema.conf", db_root);
    FILE *sf = fopen(scpath, "r");
    if (!sf) {
        fprintf(stderr, "migrate-files: cannot open %s: %s\n", scpath, strerror(errno));
        return 1;
    }

    uint64_t t0 = now_ms();
    int objects_seen = 0, objects_migrated = 0, files_moved = 0, conflicts = 0;

    char line[1024];
    while (fgets(line, sizeof(line), sf)) {
        line[strcspn(line, "\n")] = '\0';
        if (!line[0] || line[0] == '#') continue;

        char *c1 = strchr(line, ':');
        if (!c1) continue;
        *c1 = '\0';
        const char *dir = line;
        char *rest = c1 + 1;
        char *c2 = strchr(rest, ':');
        if (!c2) continue;
        *c2 = '\0';
        const char *obj = rest;

        objects_seen++;
        int n = migrate_object_files(db_root, dir, obj, &conflicts);
        if (n > 0) {
            objects_migrated++;
            files_moved += n;
        }
    }
    fclose(sf);

    uint64_t t1 = now_ms();
    printf("{\"status\":\"migrated\",\"objects_seen\":%d,\"objects_migrated\":%d,"
           "\"files_moved\":%d,\"conflicts\":%d,\"duration_ms\":%llu}\n",
           objects_seen, objects_migrated, files_moved, conflicts,
           (unsigned long long)(t1 - t0));
    return 0;
}
