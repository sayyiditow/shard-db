/* src/test/cases/test_coverity_seg_scan_varlen_overflow.c
 *
 * CID 1696466: seg_scan_o_direct_varlen narrowed a size_t rec_size
 * (derived from an on-disk, unvalidated vlen field) into an int before
 * computing `need`. A corrupted vlen near the top of the uint32_t range
 * makes the truncated int small or negative, skipping the "need more
 * data" carry-buffer growth entirely and passing the huge vlen straight
 * to the scan callback.
 *
 * This is exercised through a directly-crafted on-disk segment file (not
 * a live daemon + corruption, since this scan path is only reachable via
 * slotcask's internal VARLEN recovery/migration machinery, not a public
 * JSON mode). od_varlen_rec_size(klen=0, vlen=0xFFFFFFF0) = 24 (header)
 * + 0 (klen) + 0xFFFFFFF0 = 4294967304, which truncates as an int to 8
 * (4294967304 mod 2^32 = 8) — reproducing the exact wrap described in
 * the finding.
 *
 * seg_scan_o_direct_varlen reads in odirect_buf_size-sized chunks
 * (default 32 MiB = 33554432 bytes, set once lazily and shared across
 * the whole test-runner process — see io_direct.c). To make this
 * deterministic regardless of what earlier-run tests may have done to
 * that global, the crafted file's split point is computed relative to
 * the well-known 32 MiB default: 1,398,101 padding records of 24 bytes
 * each (tombstones: klen=0, vlen=0, flag=2) = 33,554,424 bytes, leaving
 * exactly 8 bytes before the 33,554,432-byte chunk boundary. The
 * malicious record's 24-byte header (klen=0, vlen=0xFFFFFFF0, flag=1) is
 * placed there, split 8 bytes into chunk 1 / 16 bytes into chunk 2 — so
 * Stage 1 (header completion) and Stage 2 (rec_size computation) both
 * run while processing chunk 2, deterministically hitting the narrowing
 * bug. Total file size: 33,554,448 bytes.
 *
 * Records here are 24-byte headers with klen=0 and no value bytes
 * (matches od_varlen_rec_size's minimum shape); vlen=0 padding records
 * are valid empty-value VARLEN tombstone-shaped records for this raw
 * scan (the callback is never invoked for flag=2 padding — see
 * seg_scan_o_direct_varlen's flag handling — so their content doesn't
 * matter, only their header bytes).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "io_direct.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

static int g_cb_calls = 0;

static int capture_cb(const uint8_t *rec, size_t vlen,
                       const uint8_t hash16[16], void *ctx) {
    (void)rec; (void)vlen; (void)hash16; (void)ctx;
    g_cb_calls++;
    return 0;
}

/* Write a 24-byte VARLEN record header: 16B hash + 2B klen + 1B flag +
   1B reserved + 4B vlen. No key/value bytes follow (klen=0, and for the
   malicious record vlen is a lie the header makes but no bytes back it). */
static void write_header(FILE *f, uint16_t klen, uint8_t flag, uint32_t vlen) {
    uint8_t hdr[24];
    memset(hdr, 0, sizeof(hdr));
    memcpy(hdr + 16, &klen, 2);
    hdr[18] = flag;
    hdr[19] = 0;
    memcpy(hdr + 20, &vlen, 4);
    fwrite(hdr, 1, sizeof(hdr), f);
}

static int test_coverity_seg_scan_varlen_overflow_run(void) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/shard-db-cov-varlen-%d.dat", (int)getpid());

    FILE *f = fopen(path, "wb");
    ASSERT_NOT_NULL(f, "open crafted file for writing");
    if (!f) return 1;

    const size_t CHUNK = 33554432; /* 32 MiB default odirect_buf_size */
    const size_t PAD_RECORD = 24;
    size_t n_pad = (CHUNK - 8) / PAD_RECORD; /* 1,398,101 */

    for (size_t i = 0; i < n_pad; i++) {
        write_header(f, 0, 2 /* tombstone */, 0);
    }
    long pos_before_bad = ftell(f);
    ASSERT_EQ_INT((int)((size_t)pos_before_bad % CHUNK), (int)(CHUNK - 8),
                  "padding lands exactly 8 bytes before the chunk boundary");

    /* Malicious record: klen=0, flag=1 (live), vlen=0xFFFFFFF0. */
    write_header(f, 0, 1, 0xFFFFFFF0u);

    fclose(f);

    struct stat st;
    ASSERT_EQ_INT(stat(path, &st), 0, "crafted file exists");
    ASSERT_EQ_INT((int)st.st_size, (int)(n_pad * PAD_RECORD + 24),
                  "crafted file is the expected size");

    g_cb_calls = 0;
    int rc = seg_scan_o_direct_varlen(path, capture_cb, NULL);

    /* Pre-fix: rec_size (4294967304) truncates to int(8); with carry_len
       already at 24 by the time Stage 2 runs, need = 8 - 24 = -16 <= 0,
       so the "need more data" branch is skipped and capture_cb is called
       with vlen=0xFFFFFFF0 — g_cb_calls would go to 1 and rc would be 0.
       Post-fix: rec_size > SLOTCASK_SEG_MAX_BYTES is caught before the
       narrowing cast, the scan aborts with -EIO, and capture_cb is never
       reached for the malicious record. */
    ASSERT_TRUE(rc != 0, "scan reports an error for the corrupted record");
    ASSERT_EQ_INT(g_cb_calls, 0, "callback never invoked with the lying vlen");

    unlink(path);
    return 0;
}

TEST_REGISTER("test-coverity-seg-scan-varlen-overflow", test_coverity_seg_scan_varlen_overflow_run);
