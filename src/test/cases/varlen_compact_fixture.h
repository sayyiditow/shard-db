#ifndef SHARD_DB_TEST_VARLEN_COMPACT_FIXTURE_H
#define SHARD_DB_TEST_VARLEN_COMPACT_FIXTURE_H

#include "slotcask.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define VARLEN_FIXTURE_VALUE_LEN 8000u
#define VARLEN_FIXTURE_TEST_SEG_BYTES (512u * 1024u)

/* Build three files on stream 0:
 *   file 0: A and C live, all filler tombstoned (donor with a reuse gap)
 *   file 1: four tombstoned filler records and the rest live (recipient)
 *   file 2: active and empty
 * The two-pointer compactor therefore has an eligible donor/recipient pair
 * and must execute seg_stat_one_varlen, the recipient walk, and the donor
 * scan. Returns 0 on success, -1 on any setup failure. */
static int varlen_compact_fixture_build(SlotcaskDb *db) {
    char xval[5000];
    char fill[VARLEN_FIXTURE_VALUE_LEN];
    memset(xval, 'x', sizeof(xval));
    memset(fill, 'z', sizeof(fill));

    if (slotcask_insert_with_hooks(db, 0, "keyX", 4, xval, sizeof(xval),
                                    NULL, NULL) != 0) return -1;
    if (slotcask_delete_with_hooks(db, "keyX", 4, NULL, NULL) != 0) return -1;
    if (slotcask_insert_with_hooks(db, 0, "kA", 2, "v", 1, NULL, NULL) != 0) return -1;
    if (slotcask_insert_with_hooks(db, 0, "kkeyC", 5, "cvalue", 6,
                                    NULL, NULL) != 0) return -1;

    /* Fixed -> varlen migration starts the new generation at a private
       file-ID base (currently 48000), so rotations must be relative to the
       post-migration active file rather than assuming IDs 0, 1, and 2. */
    uint32_t base_file_id = db->streams[0].active_file_id;

    size_t rec_size = (24u + 10u + VARLEN_FIXTURE_VALUE_LEN + 7u) & ~7u;
    size_t key_cap = (size_t)(slotcask_seg_max_bytes() / rec_size) + 4u;
    char (*file0_keys)[32] = calloc(key_cap, sizeof(*file0_keys));
    if (!file0_keys) return -1;

    size_t file0_count = 0;
    while (db->streams[0].active_file_id == base_file_id) {
        if (file0_count >= key_cap) { free(file0_keys); return -1; }
        snprintf(file0_keys[file0_count], 32, "f0-%06zu", file0_count);
        if (slotcask_insert_with_hooks(db, 0, file0_keys[file0_count],
                            strlen(file0_keys[file0_count]),
                            fill, sizeof(fill), NULL, NULL) != 0) {
            free(file0_keys);
            return -1;
        }
        file0_count++;
    }

    char file1_keys[4][32];
    size_t file1_count = 0;
    while (db->streams[0].active_file_id == base_file_id + 1) {
        char key[32];
        snprintf(key, sizeof(key), "f1-%06zu", file1_count);
        if (file1_count < 4) snprintf(file1_keys[file1_count], 32, "%s", key);
        if (slotcask_insert_with_hooks(db, 0, key, strlen(key), fill,
                                        sizeof(fill), NULL, NULL) != 0) {
            free(file0_keys);
            return -1;
        }
        file1_count++;
    }
    if (db->streams[0].active_file_id != base_file_id + 2 || file1_count < 4) {
        free(file0_keys);
        return -1;
    }

    /* Create recipient capacity only after rotation, so these tombstones
       cannot be consumed by later inserts and move the fixture backward. */
    for (size_t i = 0; i < file0_count; i++) {
        if (slotcask_delete_with_hooks(db, file0_keys[i], strlen(file0_keys[i]),
                                        NULL, NULL) != 0) {
            free(file0_keys);
            return -1;
        }
    }
    free(file0_keys);
    for (size_t i = 0; i < 4; i++) {
        if (slotcask_delete_with_hooks(db, file1_keys[i], strlen(file1_keys[i]),
                                        NULL, NULL) != 0)
            return -1;
    }
    return 0;
}

#endif
