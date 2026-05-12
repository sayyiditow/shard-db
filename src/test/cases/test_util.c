#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <string.h>
#include <stdlib.h>

static int test_util_run(void) {
    uint8_t buf[64];
    size_t olen;

    const uint8_t raw1[] = "hello";
    size_t elen1 = b64_encoded_size(5);
    ASSERT_TRUE(elen1 > 0, "encoded size >0");
    char *b64 = malloc(elen1 + 1);
    b64_encode(raw1, 5, b64);
    ASSERT_EQ_STR(b64, "aGVsbG8=", "b64 hello");
    ASSERT_EQ_INT(b64_decode(b64, strlen(b64), buf, &olen), 0, "decode hello");
    ASSERT_EQ_INT((int)olen, 5, "decoded len");
    ASSERT_TRUE(memcmp(buf, raw1, olen) == 0, "roundtrip hello");
    free(b64);

    uint8_t raw2[] = {0, 0, 0};
    b64 = malloc(b64_encoded_size(3) + 1);
    b64_encode(raw2, 3, b64);
    ASSERT_EQ_STR(b64, "AAAA", "b64 three nulls");
    free(b64);

    b64 = malloc(b64_encoded_size(0) + 1);
    b64_encode(raw1, 0, b64);
    ASSERT_EQ_STR(b64, "", "b64 empty");
    free(b64);

    uint8_t raw3[] = "f";
    b64 = malloc(b64_encoded_size(1) + 1);
    b64_encode(raw3, 1, b64);
    ASSERT_EQ_STR(b64, "Zg==", "b64 single byte");
    free(b64);

    uint8_t raw4[] = "fo";
    b64 = malloc(b64_encoded_size(2) + 1);
    b64_encode(raw4, 2, b64);
    ASSERT_EQ_STR(b64, "Zm8=", "b64 two bytes");
    free(b64);

    memset(buf, 0, 64);
    ASSERT_EQ_INT(b64_decode("AAAA", 4, buf, &olen), 0, "decode AAAA");
    ASSERT_EQ_INT((int)olen, 3, "decode AAAA len");

    ASSERT_EQ_INT(b64_decode("!!!invalid", 10, buf, &olen), -1, "decode invalid");
    ASSERT_EQ_INT(b64_decode("A", 1, buf, &olen), -1, "decode trunc");

    ASSERT_TRUE(valid_filename("hello.txt"), "fn basic");
    ASSERT_TRUE(valid_filename("a"), "fn single");
    ASSERT_TRUE(!valid_filename(""), "fn empty");
    ASSERT_TRUE(!valid_filename(NULL), "fn null");
    ASSERT_TRUE(!valid_filename("."), "fn dot");
    ASSERT_TRUE(!valid_filename(".."), "fn dotdot");
    ASSERT_TRUE(!valid_filename("a/b"), "fn slash");
    ASSERT_TRUE(!valid_filename("a\\b"), "fn backslash");

    char longname[300];
    memset(longname, 'x', 256); longname[256] = '\0';
    ASSERT_TRUE(!valid_filename(longname), "fn too long");
    memset(longname, 'x', 255); longname[255] = '\0';
    ASSERT_TRUE(valid_filename(longname), "fn 255 chars");

    JsonObj obj;
    ASSERT_EQ_INT(json_parse_object("", 0, &obj), -1, "parse empty string");
    ASSERT_EQ_INT(json_parse_object("notjson", 7, &obj), -1, "parse not json");
    ASSERT_EQ_INT(json_parse_object("{}", 2, &obj), 0, "parse empty obj");
    ASSERT_EQ_INT(obj.n, 0, "empty obj n");

    ASSERT_TRUE(json_parse_object("{\"a\":1}", 7, &obj) >= 0, "parse single");
    ASSERT_EQ_INT(obj.n, 1, "single n");
    const char *v = NULL; size_t vl = 0;
    ASSERT_TRUE(json_obj_get(&obj, "a", &v, &vl), "get a");
    ASSERT_EQ_INT((int)vl, 1, "val len");
    ASSERT_EQ_INT(v[0], '1', "val is 1");

    ASSERT_TRUE(json_parse_object("{\"a\":\"hello\",\"b\":42}", 21, &obj) >= 0, "parse two");
    ASSERT_EQ_INT(obj.n, 2, "two n");
    ASSERT_TRUE(json_obj_get(&obj, "a", &v, &vl), "get a");
    ASSERT_TRUE(memcmp(v, "\"hello\"", 7) == 0, "val a raw");
    ASSERT_TRUE(json_obj_unquoted(&obj, "a", &v, &vl), "unquoted a");
    ASSERT_EQ_INT((int)vl, 5, "unquoted a len");
    ASSERT_TRUE(memcmp(v, "hello", 5) == 0, "unquoted a val");
    ASSERT_EQ_INT(json_obj_int(&obj, "b", -1), 42, "int b");
    ASSERT_EQ_INT(json_obj_int(&obj, "nonexistent", -1), -1, "int missing fallback");

    char cbuf[32];
    ASSERT_EQ_INT(json_obj_copy(&obj, "a", cbuf, 6), 5, "copy a");
    ASSERT_EQ_STR(cbuf, "hello", "copy a value");
    ASSERT_EQ_INT(json_obj_copy(&obj, "nonexistent", cbuf, 6), 0, "copy missing");
    ASSERT_EQ_STR(cbuf, "", "copy missing empty");
    char *s = json_obj_strdup(&obj, "a");
    ASSERT_NOT_NULL(s, "strdup a");
    ASSERT_EQ_STR(s, "hello", "strdup a val");
    free(s);
    ASSERT_TRUE(json_obj_strdup(&obj, "nonexistent") == NULL, "strdup missing");
    s = json_obj_strdup_raw(&obj, "a");
    ASSERT_NOT_NULL(s, "strdup_raw a");
    ASSERT_EQ_STR(s, "\"hello\"", "strdup_raw a val");
    free(s);
    char *arr = json_obj_string_or_array(&obj, "b");
    ASSERT_NOT_NULL(arr, "string_or_array b");
    ASSERT_EQ_STR(arr, "42", "string_or_array b val");
    free(arr);

    ASSERT_TRUE(json_obj_string_or_array(&obj, "nonexistent") == NULL, "string_or_array missing");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-util", test_util_run)
