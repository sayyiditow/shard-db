/* src/test/cases/test_coverity_join_buf_overflow.c
 * CID 1696463 / CID 1696458: buf_join_values / buf_driver_values (and the
 * buf_field_value they call) accumulated snprintf's unclamped "would have
 * written" return value into a running buffer offset, so once one field's
 * rendered value didn't fit, `pos` desynced from the buffer and later
 * calls computed an out-of-bounds `buf + pos` with an underflowed,
 * huge `bufsz - pos`. This test calls buf_driver_values directly with a
 * deliberately tiny destination buffer (immediately followed by a canary
 * region) and several typed fields whose rendered values don't fit, to
 * force exactly the truncation chain that used to desync pos. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "query_internal.h"
#include <stdlib.h>
#include <string.h>

static int test_coverity_join_buf_overflow_run(void) {
    /* Build a typed schema with several long-ish varchar fields so their
       rendered JSON values are each bigger than the tiny buffer below. */
    TypedSchema ts;
    memset(&ts, 0, sizeof(ts));
    ts.nfields = 4;
    int off = 0;
    for (int i = 0; i < ts.nfields; i++) {
        TypedField *f = &ts.fields[i];
        memset(f, 0, sizeof(*f));
        snprintf(f->name, sizeof(f->name), "f%d", i);
        f->type = FT_VARCHAR;
        f->size = 64; /* on-disk: 2-byte length prefix + 62 content bytes */
        f->offset = off;
        off += f->size;
    }
    ts.total_size = off;

    FieldSchema fs;
    memset(&fs, 0, sizeof(fs));
    fs.ts = &ts;

    /* Build a raw record buffer with each field filled to near its cap
       with distinct content, so JSON-escaping renders a value clearly
       longer than the destination buffer used below. */
    uint8_t *raw = calloc(1, (size_t)ts.total_size);
    ASSERT_NOT_NULL(raw, "alloc raw record");
    if (!raw) return 1;
    for (int i = 0; i < ts.nfields; i++) {
        TypedField *f = &ts.fields[i];
        int content_len = f->size - 2;
        uint8_t *p = raw + f->offset;
        p[0] = (uint8_t)((content_len >> 8) & 0xff);
        p[1] = (uint8_t)(content_len & 0xff);
        memset(p + 2, 'A' + i, (size_t)content_len);
    }

    /* Destination buffer tiny enough that not all 4 fields fit, followed
       by a canary region — any OOB write past bufsz shows up here. */
    const size_t BUF_SZ = 40;
    const size_t CANARY_SZ = 256;
    char *region = malloc(BUF_SZ + CANARY_SZ);
    ASSERT_NOT_NULL(region, "alloc region");
    if (!region) { free(raw); return 1; }
    memset(region + BUF_SZ, 0xAB, CANARY_SZ);

    int pos = buf_driver_values(raw, (size_t)ts.total_size, &fs, NULL, 0, region, BUF_SZ);

    ASSERT_TRUE(pos >= 0 && (size_t)pos < BUF_SZ, "returned pos stays within bufsz");

    int canary_intact = 1;
    for (size_t i = 0; i < CANARY_SZ; i++) {
        if ((unsigned char)region[BUF_SZ + i] != 0xAB) { canary_intact = 0; break; }
    }
    ASSERT_TRUE(canary_intact, "no write past the declared buffer size");

    free(raw);
    free(region);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-coverity-join-buf-overflow", test_coverity_join_buf_overflow_run)
