/* migrate — one-shot per-release upgrade runner.
 *
 * Offline (daemon must NOT be running):
 *   Reads schema.conf, converts every registered object from fixed-slot to
 *   variable-length segment format via ./shard-db migrate-varlen.
 *   Idempotent: objects already in variable-length format are skipped.
 *
 * Prerequisite: must be on 2026.05.5+ (BTRH format). If upgrading from an
 * earlier version, run 2026.05.4's ./migrate first, then 2026.05.5's
 * ./migrate (full reindex), then this one.
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

typedef struct { char dir[256]; char obj[256]; } SchemaEntry;

/* Parses <db_root>/schema.conf and returns all dir:obj pairs.
   Caller frees *out.  Returns 0 on success (including empty schema). */
static int parse_schema(const char *db_root, SchemaEntry **out, int *n_out) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/schema.conf", db_root);
    FILE *f = fopen(path, "r");
    if (!f) {
        /* No schema.conf → nothing to migrate. */
        *out = NULL;
        *n_out = 0;
        return 0;
    }

    SchemaEntry *list = NULL;
    int n = 0, cap = 0;
    char line[4096];

    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n")] = '\0';
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || !*p) continue;

        /* Format: dir:object:splits:max_key:2:streams[...] */
        char *c1 = strchr(p, ':');
        if (!c1) continue;
        *c1 = '\0';
        char *c2 = strchr(c1 + 1, ':');
        if (!c2) continue;
        *c2 = '\0';

        if (n == cap) {
            int nc = cap ? cap * 2 : 16;
            SchemaEntry *t = realloc(list, (size_t)nc * sizeof(SchemaEntry));
            if (!t) { free(list); fclose(f); return -1; }
            list = t;
            cap = nc;
        }
        snprintf(list[n].dir, sizeof(list[n].dir), "%s", p);
        snprintf(list[n].obj, sizeof(list[n].obj), "%s", c1 + 1);
        n++;
    }
    fclose(f);
    *out = list;
    *n_out = n;
    return 0;
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    char db_root[PATH_MAX];
    if (load_db_root("db.env", db_root, sizeof(db_root)) < 0) return 1;
    fprintf(stdout, "migrate: DB_ROOT=%s\n", db_root);

    /* Phase 1/2: varlen segment migration (offline). */
    fprintf(stdout, "migrate: phase 1/2 — varlen segment migration (offline)\n");

    SchemaEntry *objects = NULL;
    int n_objects = 0;
    if (parse_schema(db_root, &objects, &n_objects) != 0) {
        fprintf(stderr, "migrate: failed to parse schema.conf\n");
        return 1;
    }

    for (int i = 0; i < n_objects; i++) {
        char cmd[PATH_MAX + 512];
        snprintf(cmd, sizeof(cmd), "./shard-db migrate-varlen %s %s",
                 objects[i].dir, objects[i].obj);
        fprintf(stdout, "migrate:   varlen %s/%s\n", objects[i].dir, objects[i].obj);
        fflush(stdout);
        int rc = system(cmd);
        if (rc != 0) {
            fprintf(stderr, "migrate: varlen migration failed for %s/%s (rc=%d)\n",
                    objects[i].dir, objects[i].obj, rc);
            free(objects);
            return 1;
        }
    }
    free(objects);

    if (n_objects == 0)
        fprintf(stdout, "migrate:   no objects in schema.conf, nothing to migrate\n");

    fprintf(stdout, "migrate: complete\n");
    return 0;
}
