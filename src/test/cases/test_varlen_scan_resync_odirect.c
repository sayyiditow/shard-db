#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "io_direct.h"
#include "seg_scan_varlen.h"
#include "varlen_scan_fixture.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

typedef struct {
    int count;
    char keys[4][32];
} CaptureCtx;

static int capture_cb(const uint8_t *rec, size_t vlen,
                       const uint8_t hash16[16], void *raw) {
    CaptureCtx *ctx = (CaptureCtx *)raw;
    uint16_t klen;
    memcpy(&klen, rec + 16, 2);
    if (ctx->count < 4) {
        memcpy(ctx->keys[ctx->count], rec + 24, klen);
        ctx->keys[ctx->count][klen] = '\0';
    }
    ctx->count++;
    (void)vlen; (void)hash16;
    return 0;
}

static int test_varlen_scan_resync_odirect_run(void) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/shard-db-varlen-resync-%d.dat", (int)getpid());

    FILE *f = fopen(path, "wb");
    ASSERT_NOT_NULL(f, "open crafted file for writing");
    if (!f) return 1;

    /* Record A: natural 32 bytes, but simulate it reusing a 64-byte
       freed slot by padding an extra 32 zero bytes after it — matching
       seg_record_emit()'s real zero-pad-to-slot_size behavior on a
       pool-reuse write. Record C starts at the 64-byte boundary. */
    size_t a_natural = varlen_fixture_write_file(f, 1, "ka", 2, "v", 1);
    ASSERT_EQ_INT((int)a_natural, 32, "record A padded size");
    uint8_t extra_pad[32] = {0};
    fwrite(extra_pad, 1, sizeof(extra_pad), f); /* total gap after A: 64 bytes */

    long c_off = ftell(f);
    ASSERT_EQ_INT((int)c_off, 64, "record C starts at the old slot's capacity boundary");
    varlen_fixture_write_file(f, 1, "kc", 2, "cc", 2);
    uint8_t sparse_tail[128] = {0};
    fwrite(sparse_tail, 1, sizeof(sparse_tail), f);

    fclose(f);

    CaptureCtx ctx = {0};
    int rc = seg_scan_o_direct(path, 64, capture_cb, &ctx);

    ASSERT_EQ_INT(rc, 0, "scan completes without error (resync recovers and sparse flag-0 tail is clean)");
    ASSERT_EQ_INT(ctx.count, 2, "both A and C are delivered to the callback");
    if (ctx.count == 2) {
        ASSERT_TRUE(strcmp(ctx.keys[0], "ka") == 0, "first record is A");
        ASSERT_TRUE(strcmp(ctx.keys[1], "kc") == 0, "second record is C, not padding");
    }

    unlink(path);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-scan-resync-odirect", test_varlen_scan_resync_odirect_run)
