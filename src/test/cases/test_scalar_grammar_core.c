/* Strict scalar grammar core unit coverage (R4 Task 1): bool spellings,
 * calendar dates, datetimes, datetimems, time, UUID, IPv4/IPv6 — each
 * valid form asserted with its exact output and each invalid form asserted
 * rejected. Unit case — no daemon, links src/db/scalar_grammar.c only. */
#include "test_runner.h"
#include "test_assert.h"
#include "../db/scalar_grammar.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void expect_bool(const char *s, int want_ok, int want_val,
                        const char *label) {
    uint8_t b = 99;
    int rc = sg_bool(s, strlen(s), &b);
    ASSERT_EQ_INT(rc, want_ok ? 0 : -1, label);
    if (want_ok) ASSERT_EQ_INT(b, want_val, label);
    else ASSERT_EQ_INT(b, 99, "outputs untouched on failure");
}
static void expect_date(const char *s, int want_ok, int32_t want,
                        const char *label) {
    int32_t d = -1;
    int rc = sg_date(s, strlen(s), &d);
    ASSERT_EQ_INT(rc, want_ok ? 0 : -1, label);
    if (want_ok) ASSERT_EQ_INT(d, want, label);
    else ASSERT_EQ_INT(d, -1, "outputs untouched on failure");
}
static void expect_datetime(const char *s, int want_ok, int32_t want_d,
                            uint32_t want_t, const char *label) {
    int32_t d = -1; uint32_t t = 999999;
    int rc = sg_datetime(s, strlen(s), &d, &t);
    ASSERT_EQ_INT(rc, want_ok ? 0 : -1, label);
    if (want_ok) {
        ASSERT_EQ_INT(d, want_d, label);
        ASSERT_EQ_INT((long long)t, (long long)want_t, label);
    }
}
static void expect_datetimems(const char *s, int want_ok, int32_t want_d,
                              uint32_t want_ms, const char *label) {
    int32_t d = -1; uint32_t ms = 99999999;
    int rc = sg_datetimems(s, strlen(s), &d, &ms);
    ASSERT_EQ_INT(rc, want_ok ? 0 : -1, label);
    if (want_ok) {
        ASSERT_EQ_INT(d, want_d, label);
        ASSERT_EQ_INT((long long)ms, (long long)want_ms, label);
    }
}
static void expect_time(const char *s, int want_ok, uint32_t want_secs,
                        const char *label) {
    uint32_t secs = 999999;
    int rc = sg_time(s, strlen(s), &secs);
    ASSERT_EQ_INT(rc, want_ok ? 0 : -1, label);
    if (want_ok) ASSERT_EQ_INT((long long)secs, (long long)want_secs, label);
}
static void expect_uuid(const char *s, int want_ok, const char *hexle,
                        const char *label) {
    uint8_t out[16], want[16], untouched[16];
    memset(out, 0xAA, sizeof out);
    memset(untouched, 0xAA, sizeof untouched);
    memset(want, 0xBB, sizeof want);
    if (hexle) for (int i = 0; i < 16; i++) {
        unsigned hi, lo;
        sscanf(hexle + i * 2, "%2x%2x", &hi, &lo);
        (void)lo; want[i] = (uint8_t)hi;
    }
    int rc = sg_uuid(s, strlen(s), out);
    ASSERT_EQ_INT(rc, want_ok ? 0 : -1, label);
    if (want_ok) ASSERT_TRUE(memcmp(out, want, 16) == 0, label);
    else ASSERT_TRUE(memcmp(out, untouched, 16) == 0, "outputs untouched on failure");
}
static void expect_ip4(const char *s, int want_ok, const uint8_t *want,
                       const char *label) {
    uint8_t out[4];
    memset(out, 0xAA, sizeof out);
    int rc = sg_ipv4(s, strlen(s), out);
    ASSERT_EQ_INT(rc, want_ok ? 0 : -1, label);
    if (want_ok) ASSERT_TRUE(memcmp(out, want, 4) == 0, label);
}
static void expect_ip6(const char *s, int want_ok, const uint8_t *want,
                       const char *label) {
    uint8_t out[16];
    memset(out, 0xAA, sizeof out);
    int rc = sg_ipv6(s, strlen(s), out);
    ASSERT_EQ_INT(rc, want_ok ? 0 : -1, label);
    if (want_ok) ASSERT_TRUE(memcmp(out, want, 16) == 0, label);
}

static int test_scalar_grammar_core_run(void) {
    /* ---- bool: the 19 contract spellings in all three cases ---- */
    static const char *const T[] = {"1", "t", "tr", "tru", "true",
                                    "y", "ye", "yes", "on"};
    static const char *const F[] = {"0", "f", "fa", "fal", "fals", "false",
                                    "n", "no", "of", "off"};
    char buf[16];
    for (size_t i = 0; i < sizeof T / sizeof *T; i++) {
        expect_bool(T[i], 1, 1, "bool true spelling (lower)");
        snprintf(buf, sizeof buf, "%s", T[i]);
        for (char *c = buf; *c; c++)
            if (*c >= 'a' && *c <= 'z') *c = (char)(*c - 32);
        expect_bool(buf, 1, 1, "bool true spelling (UPPER)");
        snprintf(buf, sizeof buf, "%s", T[i]);
        if (buf[0] >= 'a' && buf[0] <= 'z') buf[0] = (char)(buf[0] - 32);
        expect_bool(buf, 1, 1, "bool true spelling (Mixed)");
    }
    for (size_t i = 0; i < sizeof F / sizeof *F; i++) {
        expect_bool(F[i], 1, 0, "bool false spelling (lower)");
        snprintf(buf, sizeof buf, "%s", F[i]);
        for (char *c = buf; *c; c++)
            if (*c >= 'a' && *c <= 'z') *c = (char)(*c - 32);
        expect_bool(buf, 1, 0, "bool false spelling (UPPER)");
        snprintf(buf, sizeof buf, "%s", F[i]);
        if (buf[0] >= 'a' && buf[0] <= 'z') buf[0] = (char)(buf[0] - 32);
        expect_bool(buf, 1, 0, "bool false spelling (Mixed)");
    }
    /* bool: invalid forms. 'o' is ambiguous → error. */
    expect_bool("o", 0, 0, "bool o ambiguous");
    expect_bool("O", 0, 0, "bool O ambiguous");
    expect_bool("10", 0, 0, "bool 10 invalid");
    expect_bool("tru e", 0, 0, "bool interior space invalid");
    expect_bool("", 0, 0, "bool empty invalid");
    expect_bool("  ", 0, 0, "bool whitespace-only invalid");
    expect_bool("\vtrue", 0, 0, "bool vertical tab is not whitespace");
    expect_bool("true\f", 0, 0, "bool form feed is not whitespace");
    expect_bool(" true ", 1, 1, "bool outer space stripped");
    expect_bool("\tno\r", 1, 0, "bool tab/newline stripped");
    {
        /* explicit length variant: "yes" with trailing NUL inside the span */
        uint8_t b = 99;
        char span[5] = {'y', 'e', 's', '\0', 'x'};
        ASSERT_EQ_INT(sg_bool(span, 4, &b), -1, "bool span with interior NUL");
        ASSERT_EQ_INT(sg_bool(span + 0, 3, &b), 0, "bool clean prefix span");
        ASSERT_EQ_INT(b, 1, "bool clean prefix span value");
    }

    /* ---- date ---- */
    expect_date("20240229", 1, 20240229, "date leap compact");
    expect_date("2000-02-29", 1, 20000229, "date 2000 leap dashed");
    expect_date("0001-01-01", 1, 10101, "date year 1");
    expect_date("9999-12-31", 1, 99991231, "date max");
    expect_date("19000229", 0, 0, "date 1900 not leap");
    expect_date("2023-02-29", 0, 0, "date 2023 not leap");
    expect_date("20241301", 0, 0, "date month 13");
    expect_date("20240100", 0, 0, "date day 0");
    expect_date("2024-1-5", 0, 0, "date short fields rejected");
    expect_date("20240105x", 0, 0, "date trailing junk rejected");
    expect_date("0000-01-01", 0, 0, "date year 0 rejected");
    expect_date("2024/01/05", 0, 0, "date slashes rejected");
    expect_date("20240230", 0, 0, "date feb 30 rejected");
    expect_date("", 0, 0, "date empty rejected");
    expect_date(" 20240105 ", 1, 20240105, "date outer space stripped");

    /* ---- datetime ---- */
    expect_datetime("20240105010203", 1, 20240105, 3723, "datetime compact");
    expect_datetime("2024-01-05 01:02:03", 1, 20240105, 3723, "datetime space");
    expect_datetime("2024-01-05T01:02:03", 1, 20240105, 3723, "datetime T");
    expect_datetime("20240105235959", 1, 20240105, 86399, "datetime max secs");
    expect_datetime("20240105", 0, 0, 0, "datetime date-only rejected (D2)");
    expect_datetime("2024-01-05T24:00:00", 0, 0, 0, "datetime hour 24 rejected");
    expect_datetime("2024-01-05T12:60:00", 0, 0, 0, "datetime minute 60 rejected");
    expect_datetime("2024-01-05T12:00:60", 0, 0, 0, "datetime second 60 rejected");
    expect_datetime("2024010501020", 0, 0, 0, "datetime 13 digits rejected");
    expect_datetime("202401050102033", 0, 0, 0, "datetime 15 digits rejected");
    expect_datetime("", 0, 0, 0, "datetime empty rejected");
    expect_datetime("2024-01-05 01:02:03 ", 1, 20240105, 3723,
                    "datetime trailing space stripped");

    /* ---- datetimems ---- */
    expect_datetimems("20240105010203004", 1, 20240105, 3723004,
                      "datetimems compact");
    expect_datetimems("2024-01-05T01:02:03.004", 1, 20240105, 3723004,
                      "datetimems T.f");
    expect_datetimems("2024-01-05 01:02:03.999", 1, 20240105, 3723999,
                      "datetimems .999 max fraction");
    expect_datetimems("20240105", 0, 0, 0, "datetimems date-only rejected");
    expect_datetimems("2024-01-05T01:02:03.12", 0, 0, 0,
                      "datetimems 2 fraction digits rejected");
    expect_datetimems("2024-01-05T01:02:03.1234", 0, 0, 0,
                      "datetimems 4 fraction digits rejected");
    expect_datetimems("20240105010203.12", 0, 0, 0,
                      "datetimems compact-with-dot rejected");
    expect_datetimems("2024-01-05T24:00:00.000", 0, 0, 0,
                      "datetimems hour 24 rejected");

    /* ---- time ---- */
    expect_time("00:00:00", 1, 0, "time midnight");
    expect_time("23:59:59", 1, 86399, "time max");
    expect_time(" 01:02:03\t", 1, 3723, "time outer whitespace stripped");
    expect_time("24:00:00", 0, 0, "time hour 24 rejected");
    expect_time("12:60:00", 0, 0, "time minute 60 rejected");
    expect_time("12:00:60", 0, 0, "time second 60 rejected");
    expect_time("1:02:03", 0, 0, "time short rejected");
    expect_time("120000", 0, 0, "time compact rejected");
    expect_time("", 0, 0, "time empty rejected");

    /* ---- uuid ---- */
    expect_uuid("550e8400e29b41d4a716446655440000", 1,
                "550e8400e29b41d4a716446655440000", "uuid 32 hex");
    expect_uuid("550e8400-e29b-41d4-a716-446655440000", 1,
                "550e8400e29b41d4a716446655440000", "uuid dashed");
    expect_uuid("{550e8400-e29b-41d4-a716-446655440000}", 1,
                "550e8400e29b41d4a716446655440000", "uuid braced");
    expect_uuid("{550E8400E29B41D4A716446655440000}", 1,
                "550e8400e29b41d4a716446655440000", "uuid braced upper");
    expect_uuid("00000000-0000-0000-0000-000000000000", 1,
                "00000000000000000000000000000000", "uuid nil valid");
    expect_uuid("550e8400-e29b-41d4-a716-44665544000", 0, NULL,
                "uuid 31 digits rejected");
    expect_uuid("550e8400-e29b-41d4-a716-4466554400000", 0, NULL,
                "uuid 33 digits rejected");
    expect_uuid("550e8400-e29b41d4-a716-446655440000", 0, NULL,
                "uuid wrong dash positions rejected");
    expect_uuid("550e8400e29b41d4a716446655440000x", 0, NULL,
                "uuid trailing text rejected");
    expect_uuid("{550e8400-e29b-41d4-a716-446655440000", 0, NULL,
                "uuid mismatched braces rejected");
    expect_uuid("550e8400-e29b-41d4-a716-446655440000}", 0, NULL,
                "uuid stray close brace rejected");
    expect_uuid("550E8400-E29B-41D4-A716-446655440000", 1,
                "550e8400e29b41d4a716446655440000", "uuid dashed upper");
    expect_uuid("", 0, NULL, "uuid empty rejected");
    expect_uuid(" 550e8400-e29b-41d4-a716-446655440000 ", 1,
                "550e8400e29b41d4a716446655440000", "uuid outer space stripped");

    /* ---- ipv4 ---- */
    {
        const uint8_t zero[4] = {0, 0, 0, 0}, max[4] = {255, 255, 255, 255};
        const uint8_t ten[4] = {10, 0, 0, 1};
        expect_ip4("0.0.0.0", 1, zero, "ipv4 zero valid");
        expect_ip4("255.255.255.255", 1, max, "ipv4 max valid");
        expect_ip4(" 10.0.0.1\r", 1, ten, "ipv4 whitespace stripped");
        expect_ip4("256.1.1.1", 0, NULL, "ipv4 256 rejected");
        expect_ip4("1.2.3", 0, NULL, "ipv4 3 octets rejected");
        expect_ip4("1.2.3.4.5", 0, NULL, "ipv4 5 octets rejected");
        expect_ip4("1234567890123456", 0, NULL, "ipv4 16 chars rejected");
        expect_ip4("999.1.1.1", 0, NULL, "ipv4 999 rejected");
        expect_ip4("", 0, NULL, "ipv4 empty rejected");
        {
            uint8_t out[4];
            char span[8] = {'1','.','2','.','3','\0','4'};
            memset(out, 0xAA, 4);
            ASSERT_EQ_INT(sg_ipv4(span, 6, out), -1, "ipv4 span interior NUL");
        }
    }

    /* ---- ipv6 ---- */
    {
        uint8_t loopback[16] = {0};
        loopback[15] = 1;
        uint8_t db8[16] = {0};
        db8[0] = 0x20; db8[1] = 0x01; db8[2] = 0x0d; db8[3] = 0xb8; db8[15] = 1;
        expect_ip6("::", 1, (const uint8_t *)"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
                   "ipv6 :: valid");
        expect_ip6("::1", 1, loopback, "ipv6 ::1 valid");
        expect_ip6("2001:db8::1", 1, db8, "ipv6 2001:db8::1 valid");
        expect_ip6("123456789012345678901234567890123456789012345678901", 0, NULL,
                   "ipv6 46+ chars rejected");
        expect_ip6(":::", 0, NULL, "ipv6 ::: rejected");
        expect_ip6("::g", 0, NULL, "ipv6 ::g rejected");
        expect_ip6("", 0, NULL, "ipv6 empty rejected");
        {
            uint8_t out[16];
            char span[5] = {':', ':', '\0', '1'};
            memset(out, 0xAA, 16);
            ASSERT_EQ_INT(sg_ipv6(span, 3, out), -1, "ipv6 span interior NUL");
        }
    }
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-scalar-grammar-core", test_scalar_grammar_core_run)
