/* test-ipv6 — exercises the FT_IPV6 field type. Storage is 16 raw bytes,
 * network byte order (inet_pton(AF_INET6, ...) output), no sign-bit flip
 * for index-key ordering (mirrors FT_UUID/FT_IPV4). Wire format is the
 * canonical IPv6 string via inet_ntop.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int test_ipv6_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"ip6\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"addr:ipv6\",\"label:varchar:32\"],"
        "\"indexes\":[\"addr\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "create-object with ipv6 field succeeded");
    free(resp); resp = NULL;

    /* Insert several hosts with distinct addresses. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"key\":\"h1\",\"value\":{\"addr\":\"2001:db8::1\",\"label\":\"one\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "insert h1");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"key\":\"h2\",\"value\":{\"addr\":\"2001:db8::2\",\"label\":\"two\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"key\":\"h3\",\"value\":{\"addr\":\"fe80::1\",\"label\":\"three\"}}", &resp);
    free(resp); resp = NULL;

    /* Round-trip: get should return the canonical IPv6 string unchanged. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ip6\",\"object\":\"hosts\",\"key\":\"h1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"addr\":\"2001:db8::1\"", "addr round-tripped");
    free(resp); resp = NULL;

    /* eq lookup via index. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"eq\",\"value\":\"2001:db8::2\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "indexed eq query returned 1");
    free(resp); resp = NULL;

    /* Numeric-order range: both 2001:db8::1 and 2001:db8::2 should be
       < fe80::1 under byte-lexicographic (== numeric IPv6) ordering, since
       0x20 < 0xfe as the first address byte. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"lt\",\"value\":\"3000::\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "range query respects numeric ipv6 ordering");
    free(resp); resp = NULL;

    /* IN-list. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"in\",\"value\":[\"2001:db8::1\",\"fe80::1\"]}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "IN-list query matches 2 of 3 hosts");
    free(resp); resp = NULL;

    /* NOT_IN. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"not_in\",\"value\":[\"2001:db8::1\",\"fe80::1\"]}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "NOT_IN query matches the remaining host");
    free(resp); resp = NULL;

    /* between. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"criteria\":[{\"field\":\"addr\",\"op\":\"between\","
        "\"value\":\"2001:db8::\",\"value2\":\"2001:db8:ffff:ffff:ffff:ffff:ffff:ffff\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "2", "between query matches the 2001:db8:: hosts");
    free(resp); resp = NULL;

    /* describe-object should report the field type as "ipv6". */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"ip6\",\"object\":\"hosts\"}", &resp);
    ASSERT_CONTAINS(resp, "\"ipv6\"", "describe-object reports ipv6 type");
    free(resp); resp = NULL;

    /* Malformed address should not crash; should encode to the zero/unset
       sentinel and round-trip as absent (mirrors FT_UUID/FT_IPV4's
       parse-failure fallback convention). */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ip6\",\"object\":\"hosts\","
        "\"key\":\"h4\",\"value\":{\"addr\":\"not-an-ip\",\"label\":\"bad\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "insert with malformed addr does not error");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ip6\",\"object\":\"hosts\",\"key\":\"h4\"}", &resp);
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"addr\":\"") == NULL,
        "malformed addr encodes to the unset sentinel (field omitted or null)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-ipv6", test_ipv6_run)
