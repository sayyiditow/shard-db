#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "io_direct.h"
#include "seg_scan_varlen.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int noop_cb(const uint8_t *rec, size_t vlen,
                    const uint8_t hash16[16], void *ctx) {
    (void)rec; (void)vlen; (void)hash16; (void)ctx;
    return 0;
}

static int test_varlen_compact_donor_preserved_on_desync_run(void) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/shard-db-varlen-donor-desync-%d.dat", (int)getpid());

    FILE *f = fopen(path, "wb");
    ASSERT_NOT_NULL(f, "open crafted file for writing");
    if (!f) return 1;

    uint8_t hash[16];
    compute_hash_raw("kd", 2, hash);
    uint8_t hdr[24];
    memcpy(hdr, hash, 16);
    uint16_t klen = 2;
    uint32_t vlen = 1;
    memcpy(hdr + 16, &klen, 2);
    hdr[18] = 1;
    hdr[19] = 0;
    memcpy(hdr + 20, &vlen, 4);
    fwrite(hdr, 1, 24, f);
    fwrite("kd", 1, 2, f);
    fwrite("v", 1, 1, f);
    uint8_t z[5] = {0};
    fwrite(z, 1, 5, f);

    const size_t max_slot_size = 64;
    uint8_t *junk = malloc(max_slot_size);
    ASSERT_NOT_NULL(junk, "alloc junk region");
    if (junk) {
        memset(junk, 0xFF, max_slot_size);
        fwrite(junk, 1, max_slot_size, f);
        free(junk);
    }
    fclose(f);

    int rc = seg_scan_o_direct(path, max_slot_size, noop_cb, NULL);
    ASSERT_TRUE(rc < 0, "scan reports failure on an unrecoverable desync, "
                         "instead of silently truncating (the exact "
                         "condition compact_migrate_records_varlen relies "
                         "on to avoid deleting a donor with unaccounted "
                         "live records)");

    unlink(path);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-donor-preserved-on-desync",
              test_varlen_compact_donor_preserved_on_desync_run)
