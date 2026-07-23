/* src/test/cases/test_secure_random_keys.c
 *
 * Validates UUID key format and uniqueness after the fill_random hardening.
 * Covers:
 *   - single-insert auto_key=uuid: returned key is a valid v4 UUID
 *   - bulk-insert auto_key=uuid (100 records): every key is a valid v4 UUID
 *     and all keys are pairwise unique
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

static char *extract_key_field(const char *resp) {
    const char *p = SAFE_STRSTR(resp, "\"key\":\"");
    if (!p) return NULL;
    p += 7;
    const char *end = strchr(p, '"');
    if (!end) return NULL;
    size_t n = (size_t)(end - p);
    char *out = malloc(n + 1);
    if (!out) return NULL;
    memcpy(out, p, n);
    out[n] = '\0';
    return out;
}

static int is_valid_uuid_v4(const char *s) {
    if (!s) return 0;
    if (strlen(s) != 36) return 0;
    if (s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-') return 0;
    if (s[14] != '4') return 0;
    if (s[19] != '8' && s[19] != '9' && s[19] != 'a' && s[19] != 'b') return 0;
    for (int i = 0; i < 36; i++) {
        if (i == 8 || i == 13 || i == 18 || i == 23) continue;
        if (!isxdigit((unsigned char)s[i])) return 0;
    }
    return 1;
}

static int test_secure_random_keys_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Create an auto_key=uuid object. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"uuidtest\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"],\"auto_key\":\"uuid\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "uuid create OK");
    free(resp); resp = NULL;

    /* === 1. single-insert: returned key is a valid v4 UUID === */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"uuidtest\","
        "\"value\":{\"name\":\"single\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "single insert OK");
    char *single_key = extract_key_field(resp);
    ASSERT_NOT_NULL(single_key, "single: extracted key");
    if (single_key) {
        ASSERT_TRUE(is_valid_uuid_v4(single_key),
                    "single: key is valid UUID v4");
    }
    free(single_key);
    free(resp); resp = NULL;

    /* === 2. bulk-insert 100 records: all keys valid + unique === */
    /* Build JSON payload with 100 omit-key records. */
    size_t cap = 1024 * 64;
    char *bulk = malloc(cap);
    ASSERT_NOT_NULL(bulk, "bulk: malloc payload");
    if (!bulk) { tc_close(tc); test_env_stop(&env); return 1; }

    int off = snprintf(bulk, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"uuidtest\","
        "\"records\":[");
    for (int i = 0; i < 100 && off < (int)cap - 128; i++) {
        off += snprintf(bulk + off, cap - (size_t)off,
            "%s{\"value\":{\"name\":\"b%d\"}}", i > 0 ? "," : "", i);
    }
    snprintf(bulk + off, cap - (size_t)off, "]}");

    tc_request(tc, bulk, &resp);
    free(bulk);
    ASSERT_CONTAINS(resp, "\"status\":\"bulk-inserted\"", "bulk: status OK");
    ASSERT_CONTAINS(resp, "\"count\":100", "bulk: count=100");

    /* Extract the "keys":[...] array from the response. */
    const char *kp = SAFE_STRSTR(resp, "\"keys\":[");
    ASSERT_NOT_NULL(kp, "bulk: keys array present");
    if (kp) {
        kp += 8; /* skip "keys":['\0' */
        /* Parse out up to 100 key strings. */
        char *keys[100];
        int nkeys = 0;
        const char *p = kp;
        while (nkeys < 100 && *p) {
            if (*p == '"') {
                p++;
                const char *start = p;
                while (*p && *p != '"') p++;
                size_t len = (size_t)(p - start);
                keys[nkeys] = malloc(len + 1);
                if (keys[nkeys]) {
                    memcpy(keys[nkeys], start, len);
                    keys[nkeys][len] = '\0';
                }
                nkeys++;
                if (*p == '"') p++;  /* skip closing quote */
            } else if (*p == ']') {
                break;
            } else {
                p++;
            }
        }
        ASSERT_EQ_INT(nkeys, 100, "bulk: parsed 100 keys");

        /* Validate every key is a valid UUID v4 and check uniqueness. */
        int all_valid = 1;
        for (int i = 0; i < nkeys; i++) {
            if (!is_valid_uuid_v4(keys[i])) {
                all_valid = 0;
                break;
            }
        }
        ASSERT_TRUE(all_valid, "bulk: all 100 keys are valid UUID v4");

        /* Pairwise uniqueness check. */
        int all_unique = 1;
        for (int i = 0; i < nkeys && all_unique; i++) {
            for (int j = i + 1; j < nkeys; j++) {
                if (keys[i] && keys[j] && strcmp(keys[i], keys[j]) == 0) {
                    all_unique = 0;
                    break;
                }
            }
        }
        ASSERT_TRUE(all_unique, "bulk: all 100 keys are pairwise unique");

        for (int i = 0; i < nkeys; i++) free(keys[i]);
    }
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-secure-random-keys", test_secure_random_keys_run)
