/* src/test/cases/test_null_storage.c
 *
 * Null-bitmap plumbing (2026.10.2 null plan, Task 2): typed_record_view
 * validation, segment header byte-19 generation flag validation, and
 * legacy/nullmap coexistence at the varlen-scan level.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "seg_scan_varlen.h"
#include <stdlib.h>
#include <string.h>

/* ==== typed_record_view validation ==== */

static void test_view(void) {
    const uint8_t *base = NULL; const uint8_t *bm = NULL;
    int bmb = 0; size_t region = 0;

    /* Legacy records: always valid, no bitmap, full region. */
    uint8_t legacy[8] = {1,2,3,4,5,6,7,8};
    ASSERT_EQ_INT(typed_record_view(legacy, sizeof(legacy), 0,
                                    &base, &bm, &bmb, &region), 0,
                  "view: legacy valid");
    ASSERT_TRUE(base == legacy, "view: legacy base = value");
    ASSERT_TRUE(bm == NULL && bmb == 0, "view: legacy has no bitmap");
    ASSERT_EQ_INT((int)region, 8, "view: legacy region = vlen");

    /* Well-formed nullmap record: bm_nbytes=1, one bitmap byte, payload. */
    uint8_t nm[8] = {1, 0x05, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF};
    ASSERT_EQ_INT(typed_record_view(nm, sizeof(nm), SREC_FLAG_NULLMAP,
                                    &base, &bm, &bmb, &region), 0,
                  "view: nullmap valid");
    ASSERT_TRUE(base == nm + 2, "view: nullmap base after prefix");
    ASSERT_TRUE(bm == nm + 1 && bmb == 1, "view: nullmap bitmap");
    ASSERT_EQ_INT((int)region, 6, "view: nullmap region");
    ASSERT_EQ_INT(typed_field_null(bm, bmb, 0), 1, "view: bit0 set");
    ASSERT_EQ_INT(typed_field_null(bm, bmb, 1), 0, "view: bit1 clear");
    ASSERT_EQ_INT(typed_field_null(bm, bmb, 2), 1, "view: bit2 set");

    /* Malformed: unknown header bits, zero-length flagged value,
       bm_nbytes 0, bm_nbytes 33, prefix exceeding vlen. */
    ASSERT_EQ_INT(typed_record_view(nm, sizeof(nm), 0x02,
                                    &base, &bm, &bmb, &region), -1,
                  "view: unknown hdr2 bit rejected");
    ASSERT_EQ_INT(typed_record_view(nm, sizeof(nm), 0x81,
                                    &base, &bm, &bmb, &region), -1,
                  "view: NULLMAP+unknown bit rejected");
    uint8_t zerolen[1] = {1};
    ASSERT_EQ_INT(typed_record_view(zerolen, 0, SREC_FLAG_NULLMAP,
                                    &base, &bm, &bmb, &region), -1,
                  "view: zero-length flagged rejected");
    uint8_t bm0[4] = {0, 0xAA, 0xBB, 0xCC};
    ASSERT_EQ_INT(typed_record_view(bm0, sizeof(bm0), SREC_FLAG_NULLMAP,
                                    &base, &bm, &bmb, &region), -1,
                  "view: bm_nbytes 0 rejected");
    uint8_t bm33[35]; memset(bm33, 0, sizeof(bm33)); bm33[0] = 33;
    ASSERT_EQ_INT(typed_record_view(bm33, sizeof(bm33), SREC_FLAG_NULLMAP,
                                    &base, &bm, &bmb, &region), -1,
                  "view: bm_nbytes 33 rejected");
    uint8_t shortv[2] = {4, 0xFF};   /* claims 4 bitmap bytes, vlen=2 */
    ASSERT_EQ_INT(typed_record_view(shortv, sizeof(shortv), SREC_FLAG_NULLMAP,
                                    &base, &bm, &bmb, &region), -1,
                  "view: prefix exceeding vlen rejected");
    /* NULL args rejected. */
    ASSERT_EQ_INT(typed_record_view(NULL, 8, 0, &base, &bm, &bmb, &region), -1,
                  "view: NULL value rejected");
}

/* ==== header byte-19 validation + coexistence at the scan level ==== */

static void seg_emit_rec(uint8_t *dst, const char *key, const char *val,
                         uint8_t flag, uint8_t hdr2) {
    memset(dst, 0, 64);
    uint16_t klen = (uint16_t)strlen(key);
    uint32_t vlen = (uint32_t)strlen(val);
    compute_hash_raw(key, klen, dst);   /* resync verifies the hash */
    memcpy(dst + 16, &klen, 2);
    dst[18] = flag;
    dst[19] = hdr2;
    memcpy(dst + 20, &vlen, 4);
    memcpy(dst + 24, key, klen);
    memcpy(dst + 24 + klen, val, vlen);
}

static void test_scan_coexistence(void) {
    /* Layout: legacy record at 0 (64B slot), zero gap at 64,
       nullmap record at 128, unknown-bit record at 192. */
    static uint8_t buf[256];
    memset(buf, 0, sizeof(buf));
    seg_emit_rec(buf + 0,   "k1", "v1", 1, 0);    /* legacy live */
    seg_emit_rec(buf + 128, "k2", "v2", 1, SREC_FLAG_NULLMAP); /* nullmap live */
    seg_emit_rec(buf + 192, "kX", "vX", 1, 0x02); /* unknown generation bit */

    size_t rec_size; uint8_t flag, hdr2 = 0xEE; uint16_t klen; uint32_t vlen;

    ASSERT_EQ_INT(seg_scan_varlen_struct_ok(buf, sizeof(buf), 0, 512,
                                            &rec_size, &flag, &klen, &vlen,
                                            &hdr2), 1,
                  "scan: legacy record structurally valid");
    ASSERT_EQ_INT((int)hdr2, 0, "scan: legacy hdr2 = 0");
    ASSERT_EQ_INT(seg_scan_varlen_struct_ok(buf, sizeof(buf), 128, 512,
                                            &rec_size, &flag, &klen, &vlen,
                                            &hdr2), 1,
                  "scan: nullmap record structurally valid");
    ASSERT_EQ_INT((int)hdr2, SREC_FLAG_NULLMAP, "scan: nullmap hdr2 flag");
    ASSERT_EQ_INT(seg_scan_varlen_struct_ok(buf, sizeof(buf), 192, 512,
                                            &rec_size, &flag, &klen, &vlen,
                                            &hdr2), 0,
                  "scan: unknown generation bit rejected");

    /* Resync across the zero gap finds the nullmap record. */
    size_t off = 0;
    ASSERT_EQ_INT(seg_scan_varlen_resync(buf, sizeof(buf), 64, 512, 256,
                                         &off), 1,
                  "scan: resync crosses gap to nullmap record");
    ASSERT_EQ_INT((int)off, 128, "scan: resync landed on the nullmap record");
    ASSERT_EQ_INT(seg_scan_varlen_resync(buf, sizeof(buf), 192, 512, 64,
                                         &off), 0,
                  "scan: resync never lands on unknown-bit record");
}

static int test_null_storage_run(void) {
    test_view();
    test_scan_coexistence();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-null-storage", test_null_storage_run)
