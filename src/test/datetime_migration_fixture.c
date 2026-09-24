#include "slotcask.h"
#include "types.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

extern ShardDb *shard_db_open_internal(const char *db_root);

static int mkdir_one(const char *path) {
    return mkdir(path, 0755) == 0 || errno == EEXIST ? 0 : -1;
}

static int make_dirs(const char *root) {
    char path[PATH_MAX];
    if (mkdir_one(root) != 0) return -1;
    snprintf(path, sizeof(path), "%s/default", root);
    if (mkdir_one(path) != 0) return -1;
    const char *objects[] = {"migrate_dt", "control"};
    for (int i = 0; i < 2; i++) {
        snprintf(path, sizeof(path), "%s/default/%s", root, objects[i]);
        if (mkdir_one(path) != 0) return -1;
        snprintf(path, sizeof(path), "%s/default/%s/data", root, objects[i]);
        if (mkdir_one(path) != 0) return -1;
        snprintf(path, sizeof(path), "%s/default/%s/data/kf", root, objects[i]);
        if (mkdir_one(path) != 0) return -1;
        snprintf(path, sizeof(path), "%s/default/%s/data/streams", root, objects[i]);
        if (mkdir_one(path) != 0) return -1;
    }
    return 0;
}

static int write_text(const char *path, const char *text, const char *mode) {
    FILE *f = fopen(path, mode);
    if (!f) return -1;
    int ok = fputs(text, f) >= 0;
    if (ok) ok = fclose(f) == 0;
    else fclose(f);
    return ok ? 0 : -1;
}

static int insert_record(SlotcaskDb *db, const char *key,
                         const uint8_t *value, size_t value_len) {
    SlotcaskUpsertResult result;
    return slotcask_insert_with_hooks(db, -1, key, strlen(key), value,
                                      value_len, NULL, &result);
}

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    const char *root = argv[1];
    char path[PATH_MAX];
    if (make_dirs(root) != 0) return 1;

    snprintf(path, sizeof(path), "%s/schema.conf", root);
    if (write_text(path, "default:migrate_dt:8:16:2:1\n", "w") != 0 ||
        write_text(path, "default:control:8:16:2:1\n", "a") != 0)
        return 1;
    snprintf(path, sizeof(path), "%s/dirs.conf", root);
    if (write_text(path, "default\n", "w") != 0) return 1;
    snprintf(path, sizeof(path), "%s/.version", root);
    if (write_text(path, "2026.09.1\n", "w") != 0) return 1;
    snprintf(path, sizeof(path), "%s/default/migrate_dt/fields.conf", root);
    if (write_text(path, "v:datetime\n", "w") != 0) return 1;
    snprintf(path, sizeof(path), "%s/default/control/fields.conf", root);
    if (write_text(path, "v:int\n", "w") != 0) return 1;

    if (!shard_db_open_internal(root)) return 1;

    snprintf(path, sizeof(path), "%s/default/migrate_dt", root);
    SlotcaskDb db;
    if (slotcask_open(&db, path, 8, 1, 48) != 0) return 1;
    const uint8_t noon[] = {0x01, 0x34, 0xD6, 0xE6, 0xA8, 0xC0};
    const uint8_t last_ok[] = {0x01, 0x34, 0xD6, 0xE6, 0xFF, 0xFF};
    const uint8_t first_wrap[] = {0x01, 0x34, 0xD6, 0xE6, 0x00, 0x00};
    const uint8_t eod[] = {0x01, 0x34, 0xD6, 0xE6, 0x51, 0x7F};
    if (insert_record(&db, "noon", noon, sizeof(noon)) != 0 ||
        insert_record(&db, "last_ok", last_ok, sizeof(last_ok)) != 0 ||
        insert_record(&db, "first_wrap", first_wrap, sizeof(first_wrap)) != 0 ||
        insert_record(&db, "eod", eod, sizeof(eod)) != 0) {
        slotcask_close(&db);
        return 1;
    }
    slotcask_close(&db);

    snprintf(path, sizeof(path), "%s/default/control", root);
    if (slotcask_open(&db, path, 8, 1, 48) != 0) return 1;
    const uint8_t control[] = {0, 0, 0, 1};
    int rc = insert_record(&db, "one", control, sizeof(control));
    slotcask_close(&db);
    return rc == 0 ? 0 : 1;
}
