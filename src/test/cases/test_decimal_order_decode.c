/* src/test/cases/test_decimal_order_decode.c
 *
 * decimal_order_decode is the exact inverse of decimal_order_into over the
 * canonical value domain: sign, weight and limbs round-trip, the display
 * scale becomes the minimal lossless one, and every malformed payload is
 * rejected without a partial result. Expectations are literal strings; the
 * oracle for ordering equivalence is the byte order of the encoder itself
 * (verified against dec_cmp on 40x40 pairs). */
#include "test_runner.h"
#include "test_assert.h"
#include "../db/decimal_order.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

static int dod_enc(const char *text, uint8_t *buf, size_t cap, size_t *len) {
    dec_t d; dec_init(&d);
    if (dec_parse(text, strlen(text), &d, NULL, 0) != DEC_OK) { dec_free(&d); return -1; }
    int rc = decimal_order_into(&d, buf, cap, len);
    dec_free(&d);
    return rc;
}

static void dod_roundtrip(const char *in, const char *expect_minimal) {
    uint8_t buf[600]; size_t n = 0;
    ASSERT_EQ_INT(dod_enc(in, buf, sizeof buf, &n), 0, "encode");
    dec_t out; dec_init(&out);
    ASSERT_EQ_INT(decimal_order_decode(buf, n, &out), 0, "decode");
    char text[300]; size_t tl = 0;
    ASSERT_EQ_INT(dec_to_text(&out, text, sizeof text, &tl), DEC_OK, "render");
    ASSERT_EQ_STR(text, expect_minimal, in);
    dec_t again; dec_init(&again);
    ASSERT_TRUE(dec_parse(in, strlen(in), &again, NULL, 0) == DEC_OK &&
                dec_cmp(&again, &out) == 0, "mathematical value preserved");
    ASSERT_EQ_INT(decimal_min_dscale(&out), out.dscale, "dscale is minimal");
    dec_free(&out); dec_free(&again);
}

static int dod_run(void) {
    static const struct { const char *in, *minimal; } v[] = {
        {"0", "0"}, {"0.000", "0"}, {"-0.0", "0"}, {"1", "1"}, {"-1", "-1"},
        {"1.2300", "1.23"}, {"100", "100"}, {"1e2", "100"}, {"0.0001", "0.0001"},
        {"-0.00010", "-0.0001"}, {"9999", "9999"}, {"10000", "10000"},
        {"12345678901234567890.0000000001", "12345678901234567890.0000000001"},
        {"-9007199254740993", "-9007199254740993"},
        {"0.000000000000000000000000000001", "0.000000000000000000000000000001"},
    };
    for (size_t i = 0; i < sizeof v / sizeof *v; i++)
        dod_roundtrip(v[i].in, v[i].minimal);

    /* order equivalence: byte order of keys == dec_cmp order */
    static const char *pool[] = {"-100","-10.5","-1","-0.5","-0.0001","0","0.0001",
        "0.5","1","1.00001","9.9999","10","10.0001","99999999","100000000"};
    for (size_t a = 0; a < sizeof pool / sizeof *pool; a++)
      for (size_t b = 0; b < sizeof pool / sizeof *pool; b++) {
        uint8_t x[600], y[600]; size_t xn, yn;
        ASSERT_EQ_INT(dod_enc(pool[a], x, sizeof x, &xn), 0, "enc a");
        ASSERT_EQ_INT(dod_enc(pool[b], y, sizeof y, &yn), 0, "enc b");
        dec_t da, db; dec_init(&da); dec_init(&db);
        dec_parse(pool[a], strlen(pool[a]), &da, NULL, 0);
        dec_parse(pool[b], strlen(pool[b]), &db, NULL, 0);
        int want = dec_cmp(&da, &db);
        size_t m = xn < yn ? xn : yn;
        int got = memcmp(x, y, m); if (!got) got = xn < yn ? -1 : xn > yn;
        ASSERT_TRUE((want < 0) == (got < 0) && (want > 0) == (got > 0),
                    "key byte order equals numeric order");
        dec_free(&da); dec_free(&db);
      }

    /* malformed payloads are rejected, output stays canonical zero */
    uint8_t good[600]; size_t gn = 0;
    ASSERT_EQ_INT(dod_enc("12.5", good, sizeof good, &gn), 0, "base");
    struct { const char *why; size_t len; int byte; uint8_t val; } bad[] = {
        {"empty", 0, -1, 0}, {"short", 3, -1, 0}, {"odd length", gn - 1, -1, 0},
        {"bad tag", gn, 0, 7}, {"zero tag with body", gn, 0, 1},
        {"nonzero terminator", gn, (int)gn - 1, 9}, {"limb 0 sentinel", gn, 5, 0},
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        uint8_t t[600]; memcpy(t, good, gn);
        if (bad[i].byte >= 0) t[bad[i].byte] = bad[i].val;
        if (!strcmp(bad[i].why, "limb 0 sentinel")) { t[5] = 0; t[6] = 0; }
        dec_t out; dec_init(&out); errno = 0;
        ASSERT_EQ_INT(decimal_order_decode(t, bad[i].len, &out), -1, bad[i].why);
        ASSERT_TRUE(errno == EILSEQ && out.sign == DEC_SIGN_ZERO && !out.ndigits,
                    "rejected with EILSEQ and zero result");
        dec_free(&out);
    }
    ASSERT_EQ_INT(decimal_order_decode(good, gn, NULL), -1, "null out rejected");

    /* 7-byte payloads: nd == 0 must be rejected (zero is the single byte
       01), never dereferenced through a zero-limb dec_t. */
    static const uint8_t nd0_pos[] = {0x02,0x80,0,0,0,0,0};
    static const uint8_t nd0_neg[] = {0x00,0x7f,0xff,0xff,0xff,0xff,0xff};
    dec_t nd0; dec_init(&nd0); errno = 0;
    ASSERT_EQ_INT(decimal_order_decode(nd0_pos, sizeof nd0_pos, &nd0), -1,
                  "7-byte positive payload rejected");
    ASSERT_TRUE(errno == EILSEQ && nd0.sign == DEC_SIGN_ZERO && !nd0.ndigits,
                "7-byte positive payload is EILSEQ zero");
    errno = 0;
    ASSERT_EQ_INT(decimal_order_decode(nd0_neg, sizeof nd0_neg, &nd0), -1,
                  "7-byte negative payload rejected");
    ASSERT_TRUE(errno == EILSEQ && nd0.sign == DEC_SIGN_ZERO && !nd0.ndigits,
                "7-byte negative payload is EILSEQ zero");
    dec_free(&nd0);
    return 0;
}
TEST_REGISTER("test-decimal-order-decode", dod_run)
