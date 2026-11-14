/* Exact decimal core unit coverage: parsing, normalization, comparison,
   hashing, precision/rounding, addition, average division, text and
   packed serialization. Unit case — no daemon, no fixtures. */
#include "test_runner.h"
#include "test_assert.h"
#include "decnum.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Parse text, compare status, render at display scale, compare text
   and display scale. */
static void expect_parse(const char *text, dec_status want_status,
                         const char *want_text, int32_t want_dscale,
                         const char *label) {
    dec_t d;
    dec_init(&d);
    char err[192] = {0};
    dec_status st = dec_parse(text, strlen(text), &d, err, sizeof(err));
    ASSERT_EQ_INT(st, want_status, label);
    if (st == DEC_OK) {
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, label);
        if (buf) {
            size_t n = 0;
            dec_status rt = dec_to_text(&d, buf, DEC_TEXT_MAX, &n);
            ASSERT_EQ_INT(rt, DEC_OK, label);
            if (want_text) ASSERT_EQ_STR(buf, want_text, label);
            ASSERT_EQ_INT((long long)d.dscale, (long long)want_dscale, label);
            ASSERT_EQ_INT((long long)n + 1, (long long)dec_text_len(&d), label);
            free(buf);
        }
    } else if (want_text) {
        ASSERT_EQ_STR(err, want_text, label);
    }
    dec_free(&d);
}

static void expect_cmp_text(const char *a_text, const char *b_text,
                            int want, const char *label) {
    dec_t a, b;
    dec_init(&a); dec_init(&b);
    char err[64];
    ASSERT_EQ_INT(dec_parse(a_text, strlen(a_text), &a, err, sizeof(err)),
                  DEC_OK, label);
    ASSERT_EQ_INT(dec_parse(b_text, strlen(b_text), &b, err, sizeof(err)),
                  DEC_OK, label);
    int c = dec_cmp(&a, &b);
    int norm = c < 0 ? -1 : (c > 0 ? 1 : 0);
    ASSERT_EQ_INT(norm, want, label);
    dec_free(&a); dec_free(&b);
}

static void expect_hash_equal(const char *const *texts, size_t n,
                              const char *label) {
    dec_t d;
    dec_init(&d);
    uint64_t h0 = 0;
    char err[64];
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ_INT(dec_parse(texts[i], strlen(texts[i]), &d,
                                err, sizeof(err)), DEC_OK, label);
        uint64_t h = dec_hash(&d);
        if (i == 0) h0 = h;
        ASSERT_TRUE(h == h0, label);
    }
    dec_free(&d);
}

static void expect_add_text(const char *a_text, const char *b_text,
                            dec_status want_status, const char *want_text,
                            const char *label) {
    dec_t a, b, r;
    dec_init(&a); dec_init(&b); dec_init(&r);
    char err[64];
    ASSERT_EQ_INT(dec_parse(a_text, strlen(a_text), &a, err, sizeof(err)),
                  DEC_OK, label);
    ASSERT_EQ_INT(dec_parse(b_text, strlen(b_text), &b, err, sizeof(err)),
                  DEC_OK, label);
    dec_status st = dec_add(&a, &b, &r);
    ASSERT_EQ_INT(st, want_status, label);
    if (st == DEC_OK && want_text) {
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, label);
        if (buf) {
            size_t n = 0;
            dec_status rt = dec_to_text(&r, buf, DEC_TEXT_MAX, &n);
            ASSERT_EQ_INT(rt, DEC_OK, label);
            if (rt == DEC_OK) ASSERT_EQ_STR(buf, want_text, label);
            free(buf);
        }
    }
    dec_free(&a); dec_free(&b); dec_free(&r);
}

static void expect_sub_text(const char *a_text, const char *b_text,
                            const char *want_text, const char *label) {
    dec_t a, b, r;
    dec_init(&a); dec_init(&b); dec_init(&r);
    char err[64];
    ASSERT_EQ_INT(dec_parse(a_text, strlen(a_text), &a, err, sizeof(err)),
                  DEC_OK, label);
    ASSERT_EQ_INT(dec_parse(b_text, strlen(b_text), &b, err, sizeof(err)),
                  DEC_OK, label);
    dec_status st = dec_sub(&a, &b, &r);
    ASSERT_EQ_INT(st, DEC_OK, label);
    if (st == DEC_OK && want_text) {
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, label);
        if (buf) {
            size_t n = 0;
            dec_status rt = dec_to_text(&r, buf, DEC_TEXT_MAX, &n);
            ASSERT_EQ_INT(rt, DEC_OK, label);
            if (rt == DEC_OK) ASSERT_EQ_STR(buf, want_text, label);
            free(buf);
        }
    }
    dec_free(&a); dec_free(&b); dec_free(&r);
}

static void expect_prec_text(const char *text, int32_t precision, int32_t scale,
                             dec_status want_status, const char *want_text,
                             const char *label) {
    dec_t in, out;
    dec_init(&in); dec_init(&out);
    char err[192] = {0};
    ASSERT_EQ_INT(dec_parse(text, strlen(text), &in, err, sizeof(err)),
                  DEC_OK, label);
    dec_status st = dec_apply_precision(&in, precision, scale, &out,
                                        err, sizeof(err));
    ASSERT_EQ_INT(st, want_status, label);
    if (st == DEC_OK && want_text) {
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, label);
        if (buf) {
            size_t n = 0;
            dec_status rt = dec_to_text(&out, buf, DEC_TEXT_MAX, &n);
            ASSERT_EQ_INT(rt, DEC_OK, label);
            if (rt == DEC_OK) ASSERT_EQ_STR(buf, want_text, label);
            free(buf);
        }
    }
    dec_free(&in); dec_free(&out);
}

static void expect_avg_text(const char *sum_text, uint64_t count,
                            dec_status want_status, const char *want_text,
                            const char *label) {
    dec_t sum, out;
    dec_init(&sum); dec_init(&out);
    char err[64];
    if (sum_text) {
        ASSERT_EQ_INT(dec_parse(sum_text, strlen(sum_text), &sum,
                                err, sizeof(err)), DEC_OK, label);
    }
    dec_status st = dec_div_avg(&sum, count, &out);
    ASSERT_EQ_INT(st, want_status, label);
    if (st == DEC_OK && want_text) {
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, label);
        if (buf) {
            size_t n = 0;
            dec_status rt = dec_to_text(&out, buf, DEC_TEXT_MAX, &n);
            ASSERT_EQ_INT(rt, DEC_OK, label);
            if (rt == DEC_OK) ASSERT_EQ_STR(buf, want_text, label);
            free(buf);
        }
    }
    dec_free(&sum); dec_free(&out);
}

/* Parse, pack, unpack; compare mathematical value, display scale and
   rendered text against the original. */
static void expect_pack_roundtrip(const char *text, const char *label) {
    dec_t d, u;
    dec_init(&d); dec_init(&u);
    char err[64];
    ASSERT_EQ_INT(dec_parse(text, strlen(text), &d, err, sizeof(err)),
                  DEC_OK, label);
    size_t plen = dec_payload_len(&d);
    ASSERT_TRUE(plen >= DEC_PAYLOAD_HEADER && plen <= DEC_PAYLOAD_MAX, label);
    uint8_t *payload = malloc(plen);
    ASSERT_NOT_NULL(payload, label);
    if (payload) {
        size_t out_len = 0;
        ASSERT_EQ_INT(dec_pack(&d, payload, plen, &out_len), DEC_OK, label);
        ASSERT_EQ_INT((long long)out_len, (long long)plen, label);
        ASSERT_EQ_INT(dec_unpack(payload, out_len, &u), DEC_OK, label);
        ASSERT_EQ_INT(dec_cmp(&d, &u), 0, label);
        ASSERT_EQ_INT((long long)d.dscale, (long long)u.dscale, label);
        char *t1 = malloc(DEC_TEXT_MAX), *t2 = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(t1, label); ASSERT_NOT_NULL(t2, label);
        if (t1 && t2) {
            size_t n1 = 0, n2 = 0;
            dec_status r1 = dec_to_text(&d, t1, DEC_TEXT_MAX, &n1);
            dec_status r2 = dec_to_text(&u, t2, DEC_TEXT_MAX, &n2);
            ASSERT_EQ_INT(r1, DEC_OK, label);
            ASSERT_EQ_INT(r2, DEC_OK, label);
            /* never print unrendered buffers: RED-phase stub output
               must stay valid TAP text */
            if (r1 == DEC_OK && r2 == DEC_OK) ASSERT_EQ_STR(t2, t1, label);
        }
        free(t1); free(t2);
        free(payload);
    }
    dec_free(&d); dec_free(&u);
}

/* Flip one byte of a valid payload and require DEC_ERR_PAYLOAD (or
   DEC_ERR_RANGE for fields whose corruption can only exceed limits). */
static void expect_pack_corrupt(const char *text, size_t off, uint8_t xor_mask,
                                dec_status want_status, const char *label) {
    dec_t d, u;
    dec_init(&d); dec_init(&u);
    char err[64];
    ASSERT_EQ_INT(dec_parse(text, strlen(text), &d, err, sizeof(err)),
                  DEC_OK, label);
    size_t plen = dec_payload_len(&d);
    uint8_t *payload = malloc(plen + 1);
    ASSERT_NOT_NULL(payload, label);
    if (payload) {
        size_t out_len = 0;
        ASSERT_EQ_INT(dec_pack(&d, payload, plen, &out_len), DEC_OK, label);
        payload[off] = (uint8_t)(payload[off] ^ xor_mask);
        dec_status st = dec_unpack(payload, out_len, &u);
        ASSERT_EQ_INT(st, want_status, label);
        free(payload);
    }
    dec_free(&d); dec_free(&u);
}

static char *build_max_value(size_t *len_out) {
    size_t len = (size_t)DEC_MAX_INT_DIGITS + 1 + (size_t)DEC_MAX_FRAC_DIGITS;
    char *s = malloc(len + 1);
    if (!s) return NULL;
    memset(s, '9', (size_t)DEC_MAX_INT_DIGITS);
    s[DEC_MAX_INT_DIGITS] = '.';
    memset(s + DEC_MAX_INT_DIGITS + 1, '1', (size_t)DEC_MAX_FRAC_DIGITS);
    s[len] = '\0';
    *len_out = len;
    return s;
}

static int test_decnum_core_run(void) {
    /* ---- group 1: parse + display-scale rendering ---- */
    expect_parse("1234567890123456.1234567890", DEC_OK,
                 "1234567890123456.1234567890", 10,
                 "original numeric(26,10) value parses exactly");
    expect_parse("-1234567890123456.1234567890", DEC_OK,
                 "-1234567890123456.1234567890", 10,
                 "original negative value parses exactly");
    expect_parse("1234567890123456.1234567891", DEC_OK,
                 "1234567890123456.1234567891", 10,
                 "adjacent last-place value +1");
    expect_parse("1234567890123456.1234567889", DEC_OK,
                 "1234567890123456.1234567889", 10,
                 "adjacent last-place value -1");
    expect_parse("0", DEC_OK, "0", 0, "bare zero");
    expect_parse("-0", DEC_OK, "0", 0, "signed zero renders unsigned");
    expect_parse("-0.00", DEC_OK, "0.00", 2, "signed zero keeps display scale");
    expect_parse("0.0e-3", DEC_OK, "0.0000", 4, "zero exponent negative scale");
    expect_parse("1.2300", DEC_OK, "1.2300", 4, "trailing zeros preserved");
    expect_parse("0.00100", DEC_OK, "0.00100", 5, "leading+trailing frac zeros");
    expect_parse("1.0e2", DEC_OK, "100", 0, "exponent normalizes display scale");
    expect_parse("100e-5", DEC_OK, "0.00100", 5, "negative exponent scale");
    expect_parse("0e5", DEC_OK, "0", 0, "zero with positive exponent");
    expect_parse("0.00e5", DEC_OK, "0", 0, "zero exponent swallows frac zeros");
    expect_parse("9007199254740993", DEC_OK, "9007199254740993", 0,
                 "2^53+1 exact (loses low bit in double)");
    expect_parse("-9007199254740993", DEC_OK, "-9007199254740993", 0,
                 "negative 2^53+1 exact");
    expect_parse("0.30000000000000004", DEC_OK, "0.30000000000000004", 17,
                 "classic double-error value exact");
    expect_parse("9223372036854775808", DEC_OK, "9223372036854775808", 0,
                 "2^63 exact (one past INT64_MAX)");
    expect_parse("18446744073709551615", DEC_OK, "18446744073709551615", 0,
                 "2^64-1 exact");
    expect_parse("  12.50  ", DEC_OK, "12.50", 2, "outer whitespace stripped");
    expect_parse("+7.25", DEC_OK, "7.25", 2, "explicit plus sign");
    expect_parse(".5", DEC_OK, "0.5", 1, "leading-dot fraction");
    expect_parse("5.", DEC_OK, "5", 0, "trailing dot");
    expect_parse("1e00005", DEC_OK, "100000", 0, "leading-zero exponent digits");
    expect_parse("1e131071", DEC_OK, NULL, 0,
                 "10^131071 inside the integer limit");
    expect_parse("1e-16383", DEC_OK, NULL, 16383,
                 "10^-16383 inside the fractional limit");
    expect_parse("99999e131067", DEC_OK, NULL, 0,
                 "131072 integer digits via exponent");

    /* ---- group 2: grammar rejection ---- */
    expect_parse("", DEC_ERR_SYNTAX, NULL, 0, "empty input rejected");
    expect_parse("   ", DEC_ERR_SYNTAX, NULL, 0, "whitespace-only rejected");
    expect_parse("+", DEC_ERR_SYNTAX, NULL, 0, "bare sign rejected");
    expect_parse("-", DEC_ERR_SYNTAX, NULL, 0, "bare minus rejected");
    expect_parse(".", DEC_ERR_SYNTAX, NULL, 0, "bare dot rejected");
    expect_parse("+.e5", DEC_ERR_SYNTAX, NULL, 0, "sign dot exponent rejected");
    expect_parse("e5", DEC_ERR_SYNTAX, NULL, 0, "bare exponent rejected");
    expect_parse("1e", DEC_ERR_SYNTAX, NULL, 0, "dangling e rejected");
    expect_parse("1e+", DEC_ERR_SYNTAX, NULL, 0, "dangling e+ rejected");
    expect_parse("1e-", DEC_ERR_SYNTAX, NULL, 0, "dangling e- rejected");
    expect_parse("1e5.5", DEC_ERR_SYNTAX, NULL, 0, "fractional exponent rejected");
    expect_parse("1e5e5", DEC_ERR_SYNTAX, NULL, 0, "double exponent rejected");
    expect_parse("1.2.3", DEC_ERR_SYNTAX, NULL, 0, "second dot rejected");
    expect_parse("abc", DEC_ERR_SYNTAX, NULL, 0, "letters rejected");
    expect_parse("--1", DEC_ERR_SYNTAX, NULL, 0, "double sign rejected");
    expect_parse("1 2", DEC_ERR_SYNTAX, NULL, 0, "embedded space rejected");
    expect_parse("1,2", DEC_ERR_SYNTAX, NULL, 0, "comma rejected");
    expect_parse("1\t2", DEC_ERR_SYNTAX, NULL, 0, "embedded tab rejected");
    expect_parse("12x", DEC_ERR_SYNTAX, NULL, 0, "trailing junk rejected");

    /* ---- group 3: finite digit limits, one past each ---- */
    expect_parse("1.5e-16383", DEC_ERR_RANGE, NULL, 0,
                 "16384 fractional digits via exponent rejected");
    {
        size_t len = 0;
        char *mx = build_max_value(&len);
        ASSERT_NOT_NULL(mx, "max value builder");
        if (mx) {
            dec_t d;
            dec_init(&d);
            char err[64];
            ASSERT_EQ_INT(dec_parse(mx, len, &d, err, sizeof(err)), DEC_OK,
                          "full 147455-digit value parses");
            ASSERT_EQ_INT((long long)d.dscale, DEC_MAX_FRAC_DIGITS,
                          "max value display scale");
            ASSERT_EQ_INT((long long)dec_int_digits(&d), DEC_MAX_INT_DIGITS,
                          "max value integer digits");
            char *buf = malloc(DEC_TEXT_MAX);
            ASSERT_NOT_NULL(buf, "max value render buffer");
            if (buf) {
                size_t n = 0;
                ASSERT_EQ_INT(dec_to_text(&d, buf, DEC_TEXT_MAX, &n), DEC_OK,
                              "max value renders");
                ASSERT_TRUE(n == len && memcmp(buf, mx, len) == 0,
                            "max value renders byte-identical");
                free(buf);
            }
            dec_free(&d);
            free(mx);
        }
    }
    {
        /* one more integer digit than the limit, direct and via exponent */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS + 1;
        char *s = malloc(n_int + 2);
        ASSERT_NOT_NULL(s, "over-limit builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '\0';
            expect_parse(s, DEC_ERR_RANGE, NULL, 0,
                         "131073 integer digits rejected");
            /* 131072 nines followed by e1 = 131073 digits */
            s[n_int - 1] = 'e';
            s[n_int] = '1';
            s[n_int + 1] = '\0';
            expect_parse(s, DEC_ERR_RANGE, NULL, 0,
                         "131073 digits via exponent rejected");
            free(s);
        }
    }
    {
        size_t n = (size_t)DEC_MAX_FRAC_DIGITS + 1;
        char *s = malloc(n + 3);
        ASSERT_NOT_NULL(s, "frac over-limit builder");
        if (s) {
            s[0] = '0'; s[1] = '.';
            memset(s + 2, '1', n);
            s[n + 2] = '\0';
            expect_parse(s, DEC_ERR_RANGE, NULL, 0,
                         "16384 fractional digits rejected");
            free(s);
        }
    }

    /* ---- group 4: exponent abuse ---- */
    {
        size_t big = 1000000;
        char *s = malloc(big + 4);
        ASSERT_NOT_NULL(s, "exponent abuse builder");
        if (s) {
            s[0] = '1'; s[1] = 'e';
            memset(s + 2, '9', big);
            s[big + 2] = '\0';
            expect_parse(s, DEC_ERR_EXPONENT, NULL, 0,
                         "million-digit exponent rejected");
            free(s);
        }
        char *z = malloc(64);
        ASSERT_NOT_NULL(z, "zero exponent builder");
        if (z) {
            snprintf(z, 64, "1e0000000000000000000005");
            expect_parse(z, DEC_OK, "100000", 0,
                         "leading-zero exponent run accepted");
            free(z);
        }
    }

    /* ---- group 5: comparison order ---- */
    expect_cmp_text("1", "2", -1, "1 < 2");
    expect_cmp_text("2", "10", -1, "2 < 10 (numeric, not lexicographic)");
    expect_cmp_text("-2", "-10", 1, "-2 > -10");
    expect_cmp_text("1", "1.0000000000000000000000001", -1,
                    "deeper fraction is larger");
    expect_cmp_text("-1", "-1.0000000000000000000000001", 1,
                    "negative deeper fraction is smaller");
    expect_cmp_text("1.0", "1.00", 0, "display scale ignored");
    expect_cmp_text("0", "-0.000", 0, "zero equals signed zero");
    expect_cmp_text("0", "0.00001", -1, "zero < tiny");
    expect_cmp_text("-0.00001", "0.00001", -1, "tiny negative < tiny positive");
    expect_cmp_text("1e3", "999.9999999999", 1, "exponent form compares");
    expect_cmp_text("1234567890123456.1234567890",
                    "1234567890123456.1234567891", -1,
                    "original value adjacent ordering");
    expect_cmp_text("-1234567890123456.1234567890",
                    "-1234567890123456.1234567891", 1,
                    "original negative adjacent ordering");

    /* ---- group 6: scale-independent hashing ---- */
    {
        static const char *const eq1[] = {
            "1", "1.0", "1.00", "1e0", "10e-1", "01", "0001.000", "+1"
        };
        expect_hash_equal(eq1, sizeof(eq1) / sizeof(eq1[0]),
                          "equal ones hash equally");
        static const char *const eqz[] = {
            "0", "-0", "0.0", "0.000", "0e5", "-0.00e2"
        };
        expect_hash_equal(eqz, sizeof(eqz) / sizeof(eqz[0]),
                          "zeros hash equally across spellings");
        static const char *const eqo[] = {
            "1234567890123456.1234567890",
            "12345678901234561234567890e-10",
            "0.12345678901234561234567890e16"
        };
        expect_hash_equal(eqo, sizeof(eqo) / sizeof(eqo[0]),
                          "original value hashes equal across spellings");
    }

    /* ---- group 7: addition / subtraction ---- */
    expect_add_text("9223372036854775807", "1", DEC_OK,
                    "9223372036854775808", "INT64_MAX + 1 exact");
    expect_sub_text("-9223372036854775808", "1", "-9223372036854775809",
                    "INT64_MIN - 1 exact (no signed negation)");
    expect_add_text("9007199254740992", "1", DEC_OK,
                    "9007199254740993", "2^53 + 1 exact");
    expect_add_text("1.0000000000000001", "-1.0000000000000000", DEC_OK,
                    "0.0000000000000001", "near-cancellation keeps low digits");
    expect_add_text("1.5", "-1.5", DEC_OK, "0.0",
                    "exact cancellation keeps display scale");
    expect_add_text("0.9999", "0.0001", DEC_OK, "1.0000",
                    "carry keeps display scale");
    expect_add_text("1.2300", "1.2", DEC_OK, "2.4300",
                    "sum display scale is the larger operand's");
    expect_add_text("-1.25", "1.25", DEC_OK, "0.00",
                    "sign-symmetric cancellation keeps scale");
    expect_add_text("1e131071", "1e131071", DEC_OK, NULL,
                    "sum inside the integer limit");
    expect_add_text("999999999999999999999999999999", "1", DEC_OK,
                    "1000000000000000000000000000000", "carry across 30 digits");
    {
        /* 131072 nines plus one overflows the bare integer limit */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS;
        char *s = malloc(n_int + 1);
        dec_t a, b, r;
        dec_init(&a); dec_init(&b); dec_init(&r);
        ASSERT_NOT_NULL(s, "overflow sum builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '\0';
            char err[64];
            ASSERT_EQ_INT(dec_parse(s, n_int, &a, err, sizeof(err)), DEC_OK,
                          "max integer parses");
            ASSERT_EQ_INT(dec_parse("1", 1, &b, err, sizeof(err)), DEC_OK,
                          "one parses");
            dec_t z;
            dec_init(&z);
            ASSERT_EQ_INT(dec_add(&a, &b, &r), DEC_ERR_RANGE,
                          "sum past 131072 integer digits is a range error");
            ASSERT_EQ_INT((int)r.sign, (int)DEC_SIGN_ZERO,
                          "range failure leaves the canonical zero sign");
            ASSERT_EQ_INT((long long)r.weight, 0,
                          "range failure leaves weight 0");
            ASSERT_EQ_INT((long long)r.ndigits, 0,
                          "range failure leaves no limbs");
            ASSERT_EQ_INT(dec_cmp(&r, &z), 0,
                          "range failure output compares as zero");
            ASSERT_TRUE(dec_hash(&r) == dec_hash(&z),
                          "range failure output hashes as zero");
            dec_free(&z);
        }
        free(s);
        dec_free(&a); dec_free(&b); dec_free(&r);
    }
    expect_add_text("0", "0.5", DEC_OK, "0.5", "zero + fraction");
    expect_add_text("0", "-0.5", DEC_OK, "-0.5", "zero + negative fraction");
    expect_sub_text("0", "0.5", "-0.5", "zero - fraction");
    expect_sub_text("0", "-0.5", "0.5", "zero - negative fraction");
    expect_add_text("0.5", "0", DEC_OK, "0.5", "fraction + zero");
    expect_sub_text("0.5", "0", "0.5", "fraction - zero");
    expect_add_text("-0.5", "0", DEC_OK, "-0.5", "negative fraction + zero");
    expect_sub_text("-0.5", "0", "-0.5", "negative fraction - zero");
    expect_add_text("0", "0", DEC_OK, "0", "zero + zero");
    expect_sub_text("0", "0", "0", "zero - zero");
    expect_add_text("0.00", "0.5", DEC_OK, "0.50",
                    "scaled zero keeps the larger display scale");
    expect_add_text("0.5", "0.00", DEC_OK, "0.50",
                    "scaled zero second operand keeps display scale");
    {
        /* repeated accumulation starting at zero stays exact */
        dec_t acc, step, r;
        dec_init(&acc); dec_init(&step); dec_init(&r);
        char err[64];
        ASSERT_EQ_INT(dec_parse("0", 1, &acc, err, sizeof(err)), DEC_OK,
                      "accumulator starts at zero");
        for (int i = 0; i < 7; i++) {
            ASSERT_EQ_INT(dec_parse("0.0000000000000001",
                                    strlen("0.0000000000000001"), &step,
                                    err, sizeof(err)), DEC_OK, "step parses");
            ASSERT_EQ_INT(dec_add(&acc, &step, &r), DEC_OK, "accumulate");
            dec_free(&acc);
            acc = r;
            dec_init(&r);
        }
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, "accumulator render");
        if (buf) {
            size_t n = 0;
            ASSERT_EQ_INT(dec_to_text(&acc, buf, DEC_TEXT_MAX, &n), DEC_OK,
                          "accumulator renders");
            ASSERT_EQ_STR(buf, "0.0000000000000007",
                          "seven steps sum exactly");
            free(buf);
        }
        dec_free(&acc); dec_free(&step); dec_free(&r);
    }

    {
        /* associativity holds exactly across scales */
        dec_t a, b, c, ab, abc, bc, abc2;
        dec_init(&a); dec_init(&b); dec_init(&c);
        dec_init(&ab); dec_init(&abc); dec_init(&bc); dec_init(&abc2);
        char err[64];
        ASSERT_EQ_INT(dec_parse("1.000000000000000001", 19, &a, err, sizeof(err)), DEC_OK, "assoc a");
        ASSERT_EQ_INT(dec_parse("-2345678901234.5678", 19, &b, err, sizeof(err)), DEC_OK, "assoc b");
        ASSERT_EQ_INT(dec_parse("777.123456789000000000", 21, &c, err, sizeof(err)), DEC_OK, "assoc c");
        ASSERT_EQ_INT(dec_add(&a, &b, &ab), DEC_OK, "assoc ab");
        ASSERT_EQ_INT(dec_add(&ab, &c, &abc), DEC_OK, "assoc (a+b)+c");
        ASSERT_EQ_INT(dec_add(&b, &c, &bc), DEC_OK, "assoc bc");
        ASSERT_EQ_INT(dec_add(&a, &bc, &abc2), DEC_OK, "assoc a+(b+c)");
        ASSERT_EQ_INT(dec_cmp(&abc, &abc2), 0, "exact associativity");
        dec_free(&a); dec_free(&b); dec_free(&c);
        dec_free(&ab); dec_free(&abc); dec_free(&bc); dec_free(&abc2);
    }

    /* ---- group 7b: direct rounding meets canonical invariants ---- */
    {
        struct { const char *in; int32_t scale; const char *out; } rt[] = {
            { "0.1", 0, "0" }, { "0.49", 0, "0" },
            { "0.5", 0, "1" }, { "0.6", 0, "1" },
            { "-0.1", 0, "0" }, { "-0.5", 0, "-1" },
            { "9999.9999", 2, "10000.00" }, { "10000.4", 0, "10000" },
            { "2.5", 0, "3" },
        };
        dec_t v, want;
        for (size_t k = 0; k < sizeof(rt) / sizeof(rt[0]); k++) {
            dec_init(&v); dec_init(&want);
            char err[64];
            ASSERT_EQ_INT(dec_parse(rt[k].in, strlen(rt[k].in), &v,
                                    err, sizeof(err)), DEC_OK, "round seed");
            ASSERT_EQ_INT(dec_round_scale(&v, rt[k].scale), DEC_OK,
                          "direct round succeeds");
            char *buf = malloc(DEC_TEXT_MAX);
            ASSERT_NOT_NULL(buf, "round render");
            if (buf) {
                size_t n = 0;
                dec_status rr = dec_to_text(&v, buf, DEC_TEXT_MAX, &n);
                ASSERT_EQ_INT(rr, DEC_OK, "round result renders");
                if (rr == DEC_OK)
                    ASSERT_EQ_STR(buf, rt[k].out, "direct round text");
                free(buf);
            }
            ASSERT_EQ_INT(dec_parse(rt[k].out, strlen(rt[k].out), &want,
                                    err, sizeof(err)), DEC_OK, "round expect");
            ASSERT_EQ_INT(dec_cmp(&v, &want), 0, "round result compares");
            ASSERT_TRUE(dec_hash(&v) == dec_hash(&want),
                        "round result hashes like its value");
            ASSERT_TRUE(v.ndigits != 0 || v.sign == DEC_SIGN_ZERO,
                        "rounded zero is the canonical zero");
            dec_free(&v); dec_free(&want);
        }
    }
    {
        /* fix round 2: rounding at scale 0 can carry into a 131073rd
           integer digit; the public boundary must fail with the
           canonical zero. */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS;
        char *s = malloc(n_int + 3);
        dec_t v, z;
        dec_init(&v); dec_init(&z);
        ASSERT_NOT_NULL(s, "round overflow builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '.';
            s[n_int + 1] = '9';
            s[n_int + 2] = '\0';
            char err[64];
            ASSERT_EQ_INT(dec_parse(s, n_int + 2, &v, err, sizeof(err)),
                          DEC_OK, "round overflow seed parses");
            ASSERT_EQ_INT((long long)dec_int_digits(&v), DEC_MAX_INT_DIGITS,
                          "round overflow seed at the integer limit");
            ASSERT_EQ_INT(dec_round_scale(&v, 0), DEC_ERR_RANGE,
                          "rounding into a 131073rd integer digit is a "
                          "range error");
            ASSERT_EQ_INT((int)v.sign, (int)DEC_SIGN_ZERO,
                          "round range failure leaves the canonical zero "
                          "sign");
            ASSERT_EQ_INT((long long)v.ndigits, 0,
                          "round range failure leaves no limbs");
            ASSERT_EQ_INT(dec_cmp(&v, &z), 0,
                          "round range failure output compares as zero");
            free(s);
        }
        dec_free(&v); dec_free(&z);
    }
    {
        /* display scale is bounded by the frozen 16383 fractional-digit
           domain on both sides of zero */
        dec_t v;
        dec_init(&v);
        char err[64];
        ASSERT_EQ_INT(dec_parse("1", 1, &v, err, sizeof(err)), DEC_OK,
                      "dscale domain seed");
        ASSERT_EQ_INT(dec_round_scale(&v, DEC_MAX_DSCALE + 1), DEC_ERR_RANGE,
                      "scale 16384 rejected");
        ASSERT_EQ_INT((int)v.sign, (int)DEC_SIGN_ZERO,
                      "scale domain failure leaves the canonical zero sign");
        ASSERT_EQ_INT(dec_parse("1", 1, &v, err, sizeof(err)), DEC_OK,
                      "positive boundary seed");
        ASSERT_EQ_INT(dec_round_scale(&v, DEC_MAX_DSCALE), DEC_OK,
                      "scale 16383 accepted");
        ASSERT_EQ_INT((long long)v.dscale, DEC_MAX_DSCALE,
                      "scale 16383 kept as display scale");
        ASSERT_EQ_INT(dec_parse("1", 1, &v, err, sizeof(err)), DEC_OK,
                      "negative scale seed");
        ASSERT_EQ_INT(dec_round_scale(&v, -DEC_MAX_DSCALE), DEC_OK,
                      "scale -16383 accepted");
        ASSERT_EQ_INT(dec_parse("1", 1, &v, err, sizeof(err)), DEC_OK,
                      "negative domain seed");
        ASSERT_EQ_INT(dec_round_scale(&v, -DEC_MAX_DSCALE - 1), DEC_ERR_RANGE,
                      "scale -16384 rejected");
        dec_free(&v);
    }
    {
        /* constrained assignment keeps reporting precision overflow
           even when rounding carries past the bare integer limit */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS;
        char *s = malloc(n_int + 3);
        dec_t in, out;
        dec_init(&in); dec_init(&out);
        ASSERT_NOT_NULL(s, "prec boundary builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '.';
            s[n_int + 1] = '9';
            s[n_int + 2] = '\0';
            char err[192] = {0};
            ASSERT_EQ_INT(dec_parse(s, n_int + 2, &in, err, sizeof(err)),
                          DEC_OK, "prec boundary seed parses");
            ASSERT_EQ_INT(dec_apply_precision(&in, 1000, 0, &out, err,
                                              sizeof(err)),
                          DEC_ERR_PRECISION,
                          "constrained overflow stays a precision error");
            free(s);
        }
        dec_free(&in); dec_free(&out);
    }

    /* ---- group 7c: output lifecycle (reuse and failure) ---- */
    {
        dec_t out, zero;
        dec_init(&out); dec_init(&zero);
        char err[64];
        /* reused output through success and cancellation */
        ASSERT_EQ_INT(dec_parse("1.5", 3, &out, err, sizeof(err)), DEC_OK,
                      "lifecycle seed");
        dec_t a, b;
        dec_init(&a); dec_init(&b);
        ASSERT_EQ_INT(dec_parse("2.25", 4, &a, err, sizeof(err)), DEC_OK, "a");
        ASSERT_EQ_INT(dec_parse("0.75", 4, &b, err, sizeof(err)), DEC_OK, "b");
        ASSERT_EQ_INT(dec_add(&a, &b, &out), DEC_OK, "reuse out on success");
        ASSERT_EQ_INT(dec_cmp(&out, &zero) != 0 ? 1 : 0, 1,
                      "reused out holds the sum");
        ASSERT_EQ_INT(dec_parse("-3", 2, &a, err, sizeof(err)), DEC_OK, "-3");
        ASSERT_EQ_INT(dec_add(&a, &b, &out), DEC_OK, "reuse out on success 2");
        ASSERT_EQ_INT(dec_parse("-0.75", 5, &a, err, sizeof(err)), DEC_OK,
                      "-0.75");
        ASSERT_EQ_INT(dec_add(&a, &b, &out), DEC_OK, "reuse out on cancel");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0, "cancellation is canonical zero");
        ASSERT_TRUE(out.sign == DEC_SIGN_ZERO, "cancellation sign is ZERO");
        /* precision overflow leaves the canonical zero in out */
        ASSERT_EQ_INT(dec_parse("99.95", 5, &a, err, sizeof(err)), DEC_OK,
                      "overflow operand");
        ASSERT_EQ_INT(dec_apply_precision(&a, 3, 1, &out, err, sizeof(err)),
                      DEC_ERR_PRECISION, "precision overflow rejects");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "precision failure leaves canonical zero");
        /* validation failure leaves canonical zero */
        ASSERT_EQ_INT(dec_apply_precision(&a, 0, 0, &out, NULL, 0),
                      DEC_ERR_INVALID, "bad precision rejects");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "validation failure leaves canonical zero");
        /* average zero-count failure leaves canonical zero */
        ASSERT_EQ_INT(dec_parse("42.5", 4, &a, err, sizeof(err)), DEC_OK, "42.5");
        ASSERT_EQ_INT(dec_div_avg(&a, 7, &out), DEC_OK, "avg success reuses out");
        ASSERT_EQ_INT(dec_div_avg(&a, 0, &out), DEC_ERR_DIV_ZERO,
                      "zero count rejects");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "average failure leaves canonical zero");
        /* unpack failure after success leaves canonical zero */
        uint8_t payload[DEC_PAYLOAD_HEADER + 2] = {0};
        size_t plen = 0;
        ASSERT_EQ_INT(dec_parse("1.23", 4, &a, err, sizeof(err)), DEC_OK, "1.23");
        ASSERT_EQ_INT(dec_pack(&a, payload, sizeof(payload), &plen), DEC_OK,
                      "pack for lifecycle");
        ASSERT_EQ_INT(dec_unpack(payload, plen, &out), DEC_OK,
                      "unpack success reuses out");
        payload[16] ^= 0xA0;
        ASSERT_EQ_INT(dec_unpack(payload, plen, &out), DEC_ERR_PAYLOAD,
                      "unpack failure rejects");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "unpack failure leaves canonical zero");

        /* RAW-02: NULL text into a reused output must release the prior
           buffer and leave the canonical zero, not retain the value.
           The seed carries nonzero weight and display scale so every
           contract assertion below genuinely fails on an unfixed
           module (a "42" seed would make the weight and scale checks
           vacuously pass against the stale value). */
        ASSERT_EQ_INT(dec_parse("42000.50", 8, &out, err, sizeof(err)), DEC_OK,
                      "reused output seed for NULL text");
        ASSERT_EQ_INT(dec_parse(NULL, 0, &out, NULL, 0), DEC_ERR_INVALID,
                      "NULL text rejected");
        ASSERT_EQ_INT((int)out.sign, (int)DEC_SIGN_ZERO,
                      "NULL text failure leaves the canonical zero sign");
        ASSERT_EQ_INT((long long)out.weight, 0,
                      "NULL text failure leaves weight 0");
        ASSERT_EQ_INT((long long)out.ndigits, 0,
                      "NULL text failure leaves no limbs");
        ASSERT_TRUE(out.digits == NULL,
                    "NULL text failure releases the prior buffer");
        ASSERT_EQ_INT((long long)out.dscale, 0,
                      "NULL text failure clears display scale");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "NULL text failure output compares as zero");
        /* NULL output pointer stays rejected without dereference */
        ASSERT_EQ_INT(dec_parse("42", 2, NULL, NULL, 0), DEC_ERR_INVALID,
                      "NULL output pointer rejected");

        dec_free(&out); dec_free(&zero); dec_free(&a); dec_free(&b);
    }

    /* ---- group 7d: FNV-1a 64 known vector ---- */
    {
        dec_t z;
        dec_init(&z);
        /* FNV-1a 64 offset 0xcbf29ce484222325, seven zero bytes
           (sign, weight, ndigits of the canonical zero) */
        ASSERT_TRUE(dec_hash(&z) == UINT64_C(8614007388540201639),
                    "canonical zero hash matches the FNV-1a known vector");
        dec_free(&z);
    }

    /* ---- group 8: precision / scale enforcement, ties away ---- */
    expect_prec_text("9.94", 3, 1, DEC_OK, "9.9", "numeric(3,1) rounds down");
    expect_prec_text("9.96", 3, 1, DEC_OK, "10.0",
                     "numeric(3,1) carries into a new integer digit");
    expect_prec_text("99.95", 3, 1, DEC_ERR_PRECISION, NULL,
                     "numeric(3,1) overflows only after rounding");
    expect_prec_text("0.00999", 3, 5, DEC_OK, "0.00999",
                     "numeric(3,5) maximum value fits");
    expect_prec_text("-0.00999", 3, 5, DEC_OK, "-0.00999",
                     "numeric(3,5) negative maximum fits");
    expect_prec_text("0.01", 3, 5, DEC_ERR_PRECISION, NULL,
                     "numeric(3,5) rejects one unit above maximum");
    expect_prec_text("0.009995", 3, 5, DEC_ERR_PRECISION, NULL,
                     "numeric(3,5) overflows after rounding");
    expect_prec_text("999949", 4, -2, DEC_OK, "999900",
                     "negative scale rounds left of the point");
    expect_prec_text("999994", 4, -2, DEC_ERR_PRECISION, NULL,
                     "negative scale overflow after rounding");
    expect_prec_text("123456", 4, -2, DEC_OK, "123500",
                     "negative scale rounds to hundreds");
    expect_prec_text("2.5", 2, 0, DEC_OK, "3", "ties away: 2.5 -> 3");
    expect_prec_text("-2.5", 2, 0, DEC_OK, "-3", "ties away: -2.5 -> -3");
    expect_prec_text("1.005", 3, 2, DEC_OK, "1.01", "ties away at scale 2");
    expect_prec_text("0.5", 1, 0, DEC_OK, "1", "ties away: 0.5 -> 1");
    expect_prec_text("12345678901234.5678", 19, 4, DEC_OK,
                     "12345678901234.5678", "currency-shape value fits");
    expect_prec_text("999999999999999.9999", 19, 4, DEC_OK,
                     "999999999999999.9999", "numeric(19,4) maximum fits");
    expect_prec_text("9999999999999999.9999", 19, 4, DEC_ERR_PRECISION, NULL,
                     "numeric(19,4) one integer digit too many");
    expect_prec_text("0", 1, 0, DEC_OK, "0", "zero fits any precision");
    expect_prec_text("0.000", 3, 5, DEC_OK, "0.00000",
                     "zero renders at declared scale");
    expect_prec_text("1234567890123456.1234567890", 26, 10, DEC_OK,
                     "1234567890123456.1234567890",
                     "original value fits numeric(26,10)");
    {
        /* P/S domain validation */
        dec_t in, out;
        dec_init(&in); dec_init(&out);
        char err[64];
        ASSERT_EQ_INT(dec_parse("1", 1, &in, err, sizeof(err)), DEC_OK,
                      "precision domain seed");
        ASSERT_EQ_INT(dec_apply_precision(&in, 0, 0, &out, NULL, 0),
                      DEC_ERR_INVALID, "P=0 rejected");
        ASSERT_EQ_INT(dec_apply_precision(&in, 1001, 0, &out, NULL, 0),
                      DEC_ERR_INVALID, "P=1001 rejected");
        ASSERT_EQ_INT(dec_apply_precision(&in, 10, -1001, &out, NULL, 0),
                      DEC_ERR_INVALID, "S=-1001 rejected");
        ASSERT_EQ_INT(dec_apply_precision(&in, 10, 1001, &out, NULL, 0),
                      DEC_ERR_INVALID, "S=1001 rejected");
        dec_free(&in); dec_free(&out);
    }

    /* ---- group 9: average division (PostgreSQL 18 policy) ---- */
    expect_avg_text("3", 2, DEC_OK, "1.5000000000000000",
                    "avg(1,2): sum 3 count 2, 16 places");
    expect_avg_text("1", 2, DEC_OK, "0.50000000000000000000",
                    "1/2: firstdigit decrement gives 20 places");
    expect_avg_text("10", 3, DEC_OK, "3.3333333333333333",
                    "10/3: 16 places");
    expect_avg_text("2", 3, DEC_OK, "0.66666666666666666667",
                    "2/3: repeating, firstdigit decrement gives 20 places");
    expect_avg_text("1", 3, DEC_OK, "0.33333333333333333333",
                    "1/3: repeating, 20 places");
    expect_avg_text("5", 5, DEC_OK, "1.00000000000000000000",
                    "5/5: equal firstdigits decrement qweight (20 places)");
    expect_avg_text("0", 2, DEC_OK, "0.00000000000000000000",
                    "zero dividend renders zeros at selected scale");
    expect_avg_text("13", 3, DEC_OK, "4.3333333333333333",
                    "partitioned sum 13 over 3");
    expect_avg_text("1", 10000000, DEC_OK, "0.000000100000000000000000",
                    "tiny average: qweight from 10^7 divisor (24 places)");
    expect_avg_text("1", 100000000, DEC_OK, "0.0000000100000000000000000000",
                    "tiny average below 0.000001 (28 places)");
    expect_avg_text("10000", 9999, DEC_OK, "1.0001000100010001",
                    "quotient-weight boundary: firstdigit 1 vs 9999 gives 16");
    expect_avg_text("99999", 10000, DEC_OK, "9.9999000000000000",
                    "quotient-weight boundary: firstdigit 9 vs 1");
    expect_avg_text("1234567890123456.1234567890", 1, DEC_OK,
                    "1234567890123456.1234567890",
                    "count 1 returns the sum exactly");
    expect_avg_text("0", 1, DEC_OK, "0.00000000000000000000",
                    "zero over one row at 20 places (firstdigit decrement)");
    {
        dec_t sum, out;
        dec_init(&sum); dec_init(&out);
        ASSERT_EQ_INT(dec_div_avg(&sum, 0, &out), DEC_ERR_DIV_ZERO,
                      "count 0 rejected");
        dec_free(&sum); dec_free(&out);
    }

    {
        /* RAW-01: rounding the quotient can carry into a 131073rd
           integer digit; the final average must respect the bare
           integer limit and leave the canonical zero on failure. */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS;
        size_t n_frac = 1001;
        char *s = malloc(n_int + 1 + n_frac + 1);
        dec_t sum, out, z;
        dec_init(&sum); dec_init(&out); dec_init(&z);
        ASSERT_NOT_NULL(s, "avg overflow builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '.';
            memset(s + n_int + 1, '9', n_frac);
            s[n_int + 1 + n_frac] = '\0';
            char err[64];
            ASSERT_EQ_INT(dec_parse(s, n_int + 1 + n_frac, &sum, err,
                                    sizeof(err)), DEC_OK,
                          "avg overflow sum parses");
            ASSERT_EQ_INT(dec_div_avg(&sum, 1, &out), DEC_ERR_RANGE,
                          "rounded average past 131072 integer digits is a "
                          "range error");
            ASSERT_EQ_INT((int)out.sign, (int)DEC_SIGN_ZERO,
                          "average overflow leaves the canonical zero sign");
            ASSERT_EQ_INT((long long)out.weight, 0,
                          "average overflow leaves weight 0");
            ASSERT_EQ_INT((long long)out.ndigits, 0,
                          "average overflow leaves no limbs");
            ASSERT_EQ_INT(dec_cmp(&out, &z), 0,
                          "average overflow output compares as zero");
            ASSERT_TRUE(dec_hash(&out) == dec_hash(&z),
                        "average overflow output hashes as zero");
            free(s);
        }
        dec_free(&sum); dec_free(&out); dec_free(&z);
    }
    {
        /* neighboring valid boundary: 1000 fractional nines need no
           round-up and stay exactly at the integer limit */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS;
        size_t n_frac = 1000;
        char *s = malloc(n_int + 1 + n_frac + 1);
        dec_t sum, out;
        dec_init(&sum); dec_init(&out);
        ASSERT_NOT_NULL(s, "avg boundary builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '.';
            memset(s + n_int + 1, '9', n_frac);
            s[n_int + 1 + n_frac] = '\0';
            char err[64];
            ASSERT_EQ_INT(dec_parse(s, n_int + 1 + n_frac, &sum, err,
                                    sizeof(err)), DEC_OK,
                          "avg boundary sum parses");
            ASSERT_EQ_INT(dec_div_avg(&sum, 1, &out), DEC_OK,
                          "rounded average at the valid boundary succeeds");
            ASSERT_EQ_INT((long long)dec_int_digits(&out),
                          DEC_MAX_INT_DIGITS,
                          "boundary average keeps exactly 131072 integer "
                          "digits");
            free(s);
        }
        dec_free(&sum); dec_free(&out);
    }
    {
        /* operand display scale > 1000: the calculation clamp wins */
        size_t frac = 1500;
        char *s = malloc(frac + 4);
        dec_t sum, out;
        dec_init(&sum); dec_init(&out);
        ASSERT_NOT_NULL(s, "big-dscale sum builder");
        if (s) {
            s[0] = '1'; s[1] = '.';
            memset(s + 2, '0', frac);
            s[frac + 2] = '\0';
            char err[64];
            ASSERT_EQ_INT(dec_parse(s, frac + 2, &sum, err, sizeof(err)),
                          DEC_OK, "display scale 1500 parses");
            ASSERT_EQ_INT(dec_div_avg(&sum, 1, &out), DEC_OK,
                          "average over big-dscale sum succeeds");
            ASSERT_EQ_INT((long long)out.dscale, DEC_AVG_MAX_RSCALE,
                          "average result clamped to 1000 fractional digits");
        }
        free(s);
        dec_free(&sum); dec_free(&out);
    }

    /* ---- group 10: packed serialization ---- */
    expect_pack_roundtrip("1234567890123456.1234567890",
                          "original value packs/unpacks");
    expect_pack_roundtrip("-1234567890123456.1234567890",
                          "original negative packs/unpacks");
    expect_pack_roundtrip("0", "zero packs/unpacks");
    expect_pack_roundtrip("-0.00", "signed zero keeps display scale in payload");
    expect_pack_roundtrip("1.2300", "trailing-zero display scale survives");
    expect_pack_roundtrip("1.0e2", "exponent-normalized value survives");
    expect_pack_roundtrip("0.00100", "small display-scale value survives");
    {
        size_t len = 0;
        char *mx = build_max_value(&len);
        dec_t d;
        dec_init(&d);
        ASSERT_NOT_NULL(mx, "pack max builder");
        if (mx) {
            char err[64];
            ASSERT_EQ_INT(dec_parse(mx, len, &d, err, sizeof(err)), DEC_OK,
                          "max value parse for pack");
            size_t plen = dec_payload_len(&d);
            ASSERT_EQ_INT((long long)plen,
                          (long long)(DEC_PAYLOAD_HEADER +
                                      (DEC_MAX_COEFF_DIGITS + 1) / 2),
                          "max payload is exactly 73744 bytes");
            expect_pack_roundtrip(mx, "147455-digit value packs/unpacks");
            free(mx);
        }
        dec_free(&d);
    }
    {
        /* payload byte layout spot check: 1.23 -> 123 x 10^-2, dscale 2 */
        dec_t d;
        dec_init(&d);
        char err[64];
        ASSERT_EQ_INT(dec_parse("1.23", 4, &d, err, sizeof(err)), DEC_OK,
                      "payload layout seed");
        size_t plen = dec_payload_len(&d);
        ASSERT_EQ_INT((long long)plen, 16 + 2, "1.23 payload length");
        uint8_t p[32] = {0};
        size_t out_len = 0;
        ASSERT_EQ_INT(dec_pack(&d, p, sizeof(p), &out_len), DEC_OK,
                      "1.23 packs");
        ASSERT_EQ_INT(p[0], DEC_PAYLOAD_VERSION, "payload version byte");
        ASSERT_EQ_INT(p[1], DEC_SIGN_POS, "payload sign byte");
        ASSERT_EQ_INT(p[2], 0, "reserved high byte");
        ASSERT_EQ_INT(p[3], 0, "reserved low byte");
        ASSERT_EQ_INT(p[4], 0, "digit count byte 0");
        ASSERT_EQ_INT(p[5], 0, "digit count byte 1");
        ASSERT_EQ_INT(p[6], 0, "digit count byte 2");
        ASSERT_EQ_INT(p[7], 3, "digit count byte 3 (123 digits)");
        ASSERT_EQ_INT(p[8], 0xFF, "exponent -2 high byte");
        ASSERT_EQ_INT(p[11], 0xFE, "exponent -2 low byte (123 x 10^-2)");
        ASSERT_EQ_INT(p[15], 2, "display scale byte");
        ASSERT_EQ_INT(p[16], 0x12, "digits high nibble 1");
        ASSERT_EQ_INT(p[17], 0x30, "digits low nibble pair 2,3");
        dec_free(&d);
    }
    expect_pack_corrupt("1.23", 1, 0x01, DEC_ERR_PAYLOAD,
                        "corrupt sign byte rejected");
    expect_pack_corrupt("1.23", 3, 0x01, DEC_ERR_PAYLOAD,
                        "corrupt reserved byte rejected");
    expect_pack_corrupt("1.23", 16, 0xA0, DEC_ERR_PAYLOAD,
                        "corrupt digit nibble rejected");
    expect_pack_corrupt("1.23", 0, 0x01, DEC_ERR_PAYLOAD,
                        "corrupt version rejected");
    {
        /* SPEC-02 regressions: limits for every exponent sign, odd
           pad nibble, canonical zero header. */
        dec_t d;
        dec_init(&d);
        /* hand-built payload: nd digits of 9 with exponent m */
        struct hand_payload {
            uint8_t *bytes;
            size_t len;
        };
        size_t len = 0;
        char *mx = build_max_value(&len);
        ASSERT_NOT_NULL(mx, "unpack limit builder");
        if (mx) {
            char err2[64];
            ASSERT_EQ_INT(dec_parse(mx, len, &d, err2, sizeof(err2)), DEC_OK,
                          "max value for unpack limits");
            size_t plen = dec_payload_len(&d);
            uint8_t *pl = malloc(plen + 2);
            ASSERT_NOT_NULL(pl, "unpack limit payload");
            if (pl) {
                size_t ol = 0;
                ASSERT_EQ_INT(dec_pack(&d, pl, plen, &ol), DEC_OK, "pack max");
                /* valid boundary: m = -16383 decodes inside the limits */
                dec_t u;
                dec_init(&u);
                ASSERT_EQ_INT(dec_unpack(pl, ol, &u), DEC_OK,
                              "147455 digits at m=-16383 decode");
                dec_free(&u);
                free(pl);
            }
            free(mx);
        }
        /* 131073 integer digits at m = 0 exceed the integer limit */
        {
            long long nd = 131073;
            size_t plen = DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
            uint8_t *pl = malloc(plen);
            ASSERT_NOT_NULL(pl, "m0 payload");
            if (pl) {
                memset(pl, 0, plen);
                pl[0] = DEC_PAYLOAD_VERSION;
                pl[1] = DEC_SIGN_POS;
                pl[7] = (uint8_t)(nd & 0xFF);
                pl[6] = (uint8_t)((nd >> 8) & 0xFF);
                pl[5] = (uint8_t)((nd >> 16) & 0xFF);
                for (size_t k = 0; k < (size_t)((nd + 1) / 2) - 1; k++)
                    pl[DEC_PAYLOAD_HEADER + k] = 0x99;
                pl[plen - 1] = 0x90;   /* odd count: zero pad nibble */
                ASSERT_EQ_INT(pl[5], 0x02, "m0 fixture count byte 1");
                ASSERT_EQ_INT(pl[7], 0x01, "m0 fixture count byte 3");
                ASSERT_EQ_INT(pl[11], 0x00, "m0 fixture exponent byte 3");
                dec_t u;
                dec_init(&u);
                ASSERT_EQ_INT(dec_unpack(pl, plen, &u), DEC_ERR_RANGE,
                              "131073 integer digits at m=0 rejected");
                ASSERT_EQ_INT((int)u.sign, (int)DEC_SIGN_ZERO,
                              "m=0 rejection leaves the canonical zero");
                dec_free(&u);
                free(pl);
            }
        }
        /* 131074 coefficient digits at m = -1 still exceed the limit
           (131074 - 1 = 131073 integer digits). The fixture asserts the
           exact encoded header before decoding so a mislabeled case
           cannot silently exercise another branch. */
        {
            long long nd = 131074;
            size_t plen = DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
            uint8_t *pl = malloc(plen);
            ASSERT_NOT_NULL(pl, "mneg payload");
            if (pl) {
                memset(pl, 0, plen);
                pl[0] = DEC_PAYLOAD_VERSION;
                pl[1] = DEC_SIGN_POS;
                pl[7] = (uint8_t)(nd & 0xFF);
                pl[6] = (uint8_t)((nd >> 8) & 0xFF);
                pl[5] = (uint8_t)((nd >> 16) & 0xFF);
                pl[8] = 0xFF; pl[9] = 0xFF; pl[10] = 0xFF; pl[11] = 0xFF;
                pl[15] = 0x01;   /* dscale 1 satisfies the lossless rule */
                for (size_t k = 0; k < (size_t)((nd + 1) / 2); k++)
                    pl[DEC_PAYLOAD_HEADER + k] = 0x99;
                /* fixture setup: assert the encoded header fields */
                ASSERT_EQ_INT(pl[4], 0, "m-1 fixture count byte 0");
                ASSERT_EQ_INT(pl[5], 0x02, "m-1 fixture count byte 1");
                ASSERT_EQ_INT(pl[6], 0x00, "m-1 fixture count byte 2");
                ASSERT_EQ_INT(pl[7], 0x02, "m-1 fixture count byte 3");
                ASSERT_EQ_INT(pl[8], 0xFF, "m-1 fixture exponent byte 0");
                ASSERT_EQ_INT(pl[9], 0xFF, "m-1 fixture exponent byte 1");
                ASSERT_EQ_INT(pl[10], 0xFF, "m-1 fixture exponent byte 2");
                ASSERT_EQ_INT(pl[11], 0xFF, "m-1 fixture exponent byte 3");
                ASSERT_EQ_INT(pl[15], 0x01, "m-1 fixture display scale");
                dec_t u;
                dec_init(&u);
                ASSERT_EQ_INT(dec_unpack(pl, plen, &u), DEC_ERR_RANGE,
                              "131073 integer digits at m=-1 rejected");
                ASSERT_EQ_INT((int)u.sign, (int)DEC_SIGN_ZERO,
                              "m=-1 rejection leaves the canonical zero");
                dec_free(&u);
                free(pl);
            }
        }
        /* valid boundary: 131073 digits at m = -1 is exactly 131072
           integer digits and decodes */
        {
            long long nd = 131073;
            size_t plen = DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
            uint8_t *pl = malloc(plen);
            ASSERT_NOT_NULL(pl, "mneg boundary payload");
            if (pl) {
                memset(pl, 0, plen);
                pl[0] = DEC_PAYLOAD_VERSION;
                pl[1] = DEC_SIGN_POS;
                pl[7] = (uint8_t)(nd & 0xFF);
                pl[6] = (uint8_t)((nd >> 8) & 0xFF);
                pl[5] = (uint8_t)((nd >> 16) & 0xFF);
                pl[8] = 0xFF; pl[9] = 0xFF; pl[10] = 0xFF; pl[11] = 0xFF;
                pl[15] = 0x01;   /* dscale 1 >= -m */
                for (size_t k = 0; k < (size_t)((nd + 1) / 2) - 1; k++)
                    pl[DEC_PAYLOAD_HEADER + k] = 0x99;
                pl[plen - 1] = 0x90;   /* odd count: zero pad nibble */
                ASSERT_EQ_INT(pl[5], 0x02, "boundary fixture count byte 1");
                ASSERT_EQ_INT(pl[7], 0x01, "boundary fixture count byte 3");
                ASSERT_EQ_INT(pl[11], 0xFF, "boundary fixture exponent byte 3");
                dec_t u;
                dec_init(&u);
                ASSERT_EQ_INT(dec_unpack(pl, plen, &u), DEC_OK,
                              "131072 integer digits at m=-1 decode");
                ASSERT_EQ_INT((long long)dec_int_digits(&u),
                              DEC_MAX_INT_DIGITS,
                              "boundary value has exactly 131072 integer digits");
                dec_free(&u);
                free(pl);
            }
        }
        /* exact boundary: 131070 digits at m = +2 fit */
        {
            long long nd = 131070;
            size_t plen = DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
            uint8_t *pl = malloc(plen);
            ASSERT_NOT_NULL(pl, "boundary payload");
            if (pl) {
                memset(pl, 0, plen);
                pl[0] = DEC_PAYLOAD_VERSION;
                pl[1] = DEC_SIGN_POS;
                pl[7] = (uint8_t)(nd & 0xFF);
                pl[6] = (uint8_t)((nd >> 8) & 0xFF);
                pl[5] = (uint8_t)((nd >> 16) & 0xFF);
                pl[11] = 0x02;   /* m = +2 */
                pl[15] = 0x02;   /* dscale 0 would fail the lossless rule */
                for (size_t k = 0; k < (size_t)((nd + 1) / 2); k++)
                    pl[DEC_PAYLOAD_HEADER + k] = 0x99;
                dec_t u;
                dec_init(&u);
                ASSERT_EQ_INT(dec_unpack(pl, plen, &u), DEC_OK,
                              "131072 integer digits at m=2 decode");
                ASSERT_EQ_INT((long long)dec_int_digits(&u), DEC_MAX_INT_DIGITS,
                              "boundary value has exactly 131072 integer digits");
                dec_free(&u);
                free(pl);
            }
        }
        /* odd count pad nibble must be zero */
        {
            char err2[64];
            ASSERT_EQ_INT(dec_parse("1", 1, &d, err2, sizeof(err2)), DEC_OK,
                          "one-digit seed");
            uint8_t one[DEC_PAYLOAD_HEADER + 1];
            size_t ol = 0;
            ASSERT_EQ_INT(dec_pack(&d, one, sizeof(one), &ol), DEC_OK,
                          "one-digit packs");
            ASSERT_EQ_INT((int)one[16], 0x10, "pad nibble is zero on pack");
            one[16] = 0x1F;   /* same digit, nonzero pad */
            dec_t u;
            dec_init(&u);
            ASSERT_EQ_INT(dec_unpack(one, ol, &u), DEC_ERR_PAYLOAD,
                          "nonzero pad nibble rejected");
            dec_free(&u);
        }
        /* canonical zero header: nonzero exponent rejected */
        {
            uint8_t zero_pl[DEC_PAYLOAD_HEADER];
            memset(zero_pl, 0, sizeof(zero_pl));
            zero_pl[0] = DEC_PAYLOAD_VERSION;
            zero_pl[1] = DEC_SIGN_ZERO;
            zero_pl[15] = 2;   /* dscale 2 */
            dec_t u;
            dec_init(&u);
            ASSERT_EQ_INT(dec_unpack(zero_pl, sizeof(zero_pl), &u), DEC_OK,
                          "canonical zero payload decodes");
            ASSERT_EQ_INT((long long)u.dscale, 2, "zero payload keeps dscale");
            zero_pl[11] = 0xFF;   /* m = -1 */
            ASSERT_EQ_INT(dec_unpack(zero_pl, sizeof(zero_pl), &u),
                          DEC_ERR_PAYLOAD,
                          "zero payload with nonzero exponent rejected");
            dec_free(&u);
        }
        dec_free(&d);
    }
    {
        dec_t d, u;
        dec_init(&d); dec_init(&u);
        char err[64];
        ASSERT_EQ_INT(dec_parse("1.23", 4, &d, err, sizeof(err)), DEC_OK,
                      "truncation seed");
        uint8_t p[32];
        size_t out_len = 0;
        ASSERT_EQ_INT(dec_pack(&d, p, sizeof(p), &out_len), DEC_OK,
                      "truncation pack");
        ASSERT_EQ_INT(dec_unpack(p, out_len - 1, &u), DEC_ERR_PAYLOAD,
                      "short payload rejected");
        ASSERT_EQ_INT(dec_unpack(p, out_len + 1, &u), DEC_ERR_PAYLOAD,
                      "long payload rejected");
        dec_free(&d); dec_free(&u);
    }

    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-decnum-core", test_decnum_core_run)
