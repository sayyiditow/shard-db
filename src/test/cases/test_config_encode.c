#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <string.h>
#include <stdlib.h>

static TypedField make_f(enum FieldType type, int size, int scale) {
    TypedField f; memset(&f, 0, sizeof(f));
    f.type = type; f.size = size; f.numeric_scale = scale;
    if (type == FT_NUMERIC) {
        int64_t mult = 1;
        for (int i = 0; i < scale; i++) mult *= 10;
        f.numeric_scale_mult = mult;
    }
    return f;
}

static int test_config_encode_run(void) {
    uint8_t out[64]; memset(out, 0, 64);

    TypedField f_vc = make_f(FT_VARCHAR, 12, 0);
    encode_field_len(&f_vc, "hello", 5, out);
    ASSERT_EQ_INT(out[0], 0, "vc len hi"); ASSERT_EQ_INT(out[1], 5, "vc len lo");
    ASSERT_TRUE(memcmp(out + 2, "hello", 5) == 0, "vc content");
    memset(out, 0, 64);
    encode_field_len(&f_vc, "", 0, out);
    ASSERT_EQ_INT(out[1], 0, "vc empty");
    memset(out, 0, 64);
    encode_field_len(&f_vc, NULL, 0, out);
    ASSERT_EQ_INT(out[1], 0, "vc null");
    memset(out, 0, 64);
    encode_field_len(&f_vc, "aaaaaaaaaaaaaaaaaaaa", 20, out);
    ASSERT_EQ_INT(out[1], 10, "vc truncated");

    TypedField f_long = make_f(FT_LONG, 8, 0);
    memset(out, 0, 64); encode_field_len(&f_long, "42", 2, out);
    ASSERT_EQ_INT(out[7], 42, "long 42");
    ASSERT_EQ_INT(out[0], 0, "long 42 MSB");
    memset(out, 0, 64); encode_field_len(&f_long, "-1", 2, out);
    ASSERT_EQ_INT(out[7], 0xFF, "long -1");

    TypedField f_int = make_f(FT_INT, 4, 0);
    memset(out, 0, 64); encode_field_len(&f_int, "100", 3, out);
    ASSERT_EQ_INT(out[3], 100, "int 100");
    memset(out, 0, 64); encode_field_len(&f_int, "-128", 4, out);
    ASSERT_EQ_INT(out[3], 0x80, "int -128");

    TypedField f_short = make_f(FT_SHORT, 2, 0);
    memset(out, 0, 64); encode_field_len(&f_short, "32767", 5, out);
    ASSERT_EQ_INT(out[0], 0x7F, "short 32767"); ASSERT_EQ_INT(out[1], 0xFF, "short 32767 lo");

    TypedField f_dbl = make_f(FT_DOUBLE, 8, 0);
    memset(out, 0, 64); encode_field_len(&f_dbl, "3.14", 4, out);
    double dv; memcpy(&dv, out, 8);
    ASSERT_TRUE(dv > 3.13 && dv < 3.15, "double 3.14");

    TypedField f_flt = make_f(FT_FLOAT, 4, 0);
    memset(out, 0, 64); encode_field_len(&f_flt, "2.5", 3, out);
    float fv; memcpy(&fv, out, 4);
    ASSERT_TRUE(fv > 2.49f && fv < 2.51f, "float 2.5");

    TypedField f_bool = make_f(FT_BOOL, 1, 0);
    memset(out, 0, 64); encode_field_len(&f_bool, "true", 4, out);
    ASSERT_EQ_INT(out[0], 1, "bool true");
    memset(out, 0, 64); encode_field_len(&f_bool, "false", 5, out);
    ASSERT_EQ_INT(out[0], 0, "bool false");
    memset(out, 0, 64); encode_field_len(&f_bool, "1", 1, out);
    ASSERT_EQ_INT(out[0], 1, "bool 1");

    TypedField f_byte = make_f(FT_BYTE, 1, 0);
    memset(out, 0, 64); encode_field_len(&f_byte, "200", 3, out);
    ASSERT_EQ_INT(out[0], 200, "byte 200");

    TypedField f_date = make_f(FT_DATE, 4, 0);
    memset(out, 0, 64); encode_field_len(&f_date, "20260513", 8, out);
    ASSERT_EQ_INT(out[3], 0xA1, "date lo");
    memset(out, 0, 64); encode_field_len(&f_date, "2026-05-13", 10, out);
    ASSERT_EQ_INT(out[3], 0xA1, "date dashes");

    TypedField f_dt = make_f(FT_DATETIME, 6, 0);
    memset(out, 0, 64); encode_field_len(&f_dt, "20260513123000", 14, out);
    ASSERT_TRUE(out[0] != 0 || out[4] != 0, "datetime packed");

    TypedField f_time = make_f(FT_TIME, 3, 0);
    memset(out, 0, 64); encode_field_len(&f_time, "12:30:00", 8, out);
    ASSERT_EQ_INT(out[0], 0, "time hi");
    ASSERT_EQ_INT(out[1], 0xAF, "time mid");

    TypedField f_num = make_f(FT_NUMERIC, 8, 2);
    memset(out, 0, 64); encode_field_len(&f_num, "123.45", 6, out);
    ASSERT_EQ_INT(out[7], 57, "numeric 12345 lo");

    TypedField f_uuid = make_f(FT_UUID, 16, 0);
    memset(out, 0, 64); encode_field_len(&f_uuid, "550e8400-e29b-41d4-a716-446655440000", 36, out);
    ASSERT_TRUE(out[0] != 0, "uuid packed");
    memset(out, 0, 64); encode_field_len(&f_uuid, "bad-input", 9, out);
    ASSERT_EQ_INT(out[0], 0, "uuid bad");

    TypedField f_none = make_f(FT_NONE, 4, 0);
    memset(out, 0xAA, 64); encode_field_len(&f_none, "test", 4, out);
    ASSERT_EQ_INT(out[0], 0, "FT_NONE zeros");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-config-encode", test_config_encode_run)
