/* src/test/cases/test_computed_update.c
 *
 * Conditional-writes F1 — atomic computed updates ($incr / $copy),
 * plus the D8 boundary: computed spans outside single `update` reject.
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
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <dirent.h>

static int test_computed_update_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cp\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"balance:long\",\"shares:int\","
        "\"price:numeric:12,2\",\"ratio:double\",\"note:varchar:20\","
        "\"mirror:long\",\"tiny:short\"],"
        "\"indexes\":[\"balance\",\"shares\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create cp");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"cp\",\"records\":["
        "{\"key\":\"c1\",\"value\":{\"balance\":100,\"shares\":4,"
        "\"price\":\"10.50\",\"ratio\":1.5,\"note\":\"n\",\"mirror\":100}}]}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "seed c1");
    free(resp); resp = NULL;

    /* 1. $incr long, positive and negative; the index reflects the
          result. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":50}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "incr +50");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":-25}}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"cp\","
        "\"criteria\":[{\"field\":\"balance\",\"op\":\"eq\",\"value\":125}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "find/count sees indexed new value");
    free(resp); resp = NULL;

    /* 2. $incr int / numeric / double — numeric scaling is exact. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"shares\":{\"$incr\":-1}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "incr int");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"price\":{\"$incr\":\"125.50\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "incr numeric");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"ratio\":{\"$incr\":0.25}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "incr double");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"shares\":3", "int applied");
    /* Engine numeric rendering prints whole values bare (136.00 at scale
       2 → "136"); the exactness being tested is the stored scaled
       integer, not display padding (see PLAN_NOTES.md item 2). */
    ASSERT_CONTAINS(resp, "\"price\":136,", "numeric applied exactly");
    ASSERT_CONTAINS(resp, "\"ratio\":1.75", "double applied");
    free(resp); resp = NULL;

    /* 3. $copy same type. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"mirror\":{\"$copy\":\"balance\"}}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "copy long");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"mirror\":125", "copied value");
    free(resp); resp = NULL;

    /* 4. Overflow rejects the whole request and leaves the record. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":"
        "9223372036854775807}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "overflow rejected");
    ASSERT_CONTAINS(resp, "overflow", "named overflow");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"balance\":125", "value unchanged after reject");
    free(resp); resp = NULL;

    /* 5. NULL target / NULL copy source reject. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"note\":null}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"mirror\":{\"$copy\":\"note\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "copy from NULL rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"cp\",\"records\":[{\"key\":\"c2\",\"value\":{}}]}",
        &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c2\",\"value\":{\"balance\":{\"$incr\":1}}}", &resp);
    ASSERT_CONTAINS(resp, "is NULL", "incr on NULL rejected");
    free(resp); resp = NULL;

    /* 6. Type and grammar rejections — no write in any case. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"note\":{\"$incr\":1}}}", &resp);
    ASSERT_CONTAINS(resp, "unsupported type", "varchar incr rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$mul\":2}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown op rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":"
        "{\"balance\":{\"$incr\":1,\"$copy\":\"mirror\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "two ops rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":1,\"junk\":9}}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown member rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"price\":{\"$incr\":\"0.005\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "exact increment rounded once on assignment");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$copy\":\"price\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "cross-type copy rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$copy\":\"nope\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown copy source rejected");
    free(resp); resp = NULL;

    /* 7. Condition + computed: if fails → nothing; if passes → applied. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":-200}},"
        "\"if\":[{\"field\":\"balance\",\"op\":\"gte\",\"value\":500}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"",
                    "condition gates the compute");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"balance\":125", "no compute on failed if");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":-25}},"
        "\"if\":[{\"field\":\"balance\",\"op\":\"gte\",\"value\":100}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "condition passes");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"balance\":100", "computed under condition");
    free(resp); resp = NULL;

    /* 8. Computed + return_record agree. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":1}},"
        "\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"record\":", "record returned");
    ASSERT_CONTAINS(resp, "\"balance\":101", "record shows computed value");
    free(resp); resp = NULL;

    /* 9. D8 boundary — computed spans outside single update reject. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c3\",\"value\":{\"balance\":{\"$incr\":1}}}", &resp);
    ASSERT_CONTAINS(resp, "only valid on update", "insert rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "c3 was never written");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"cp\",\"records\":[{\"key\":\"c4\","
        "\"value\":{\"balance\":{\"$incr\":1}}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"errors\":1", "bulk-insert record rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
        "\"object\":\"cp\",\"criteria\":[{\"field\":\"balance\",\"op\":\"eq\","
        "\"value\":100}],\"value\":{\"balance\":{\"$incr\":1}}}", &resp);
    ASSERT_CONTAINS(resp, "only valid on update", "criteria bulk rejected");
    free(resp); resp = NULL;

    /* 10. Negative int/short decode and decimal padding regressions. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"shares\":-1,\"tiny\":-1,\"price\":\"0.00\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"shares\":{\"$incr\":1},\"tiny\":{\"$incr\":1},"
        "\"price\":{\"$incr\":\"1.5\"},\"note\":\"{hello}\"},\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"shares\":0", "negative int increment");
    ASSERT_CONTAINS(resp, "\"tiny\":0", "negative short increment");
    ASSERT_CONTAINS(resp, "1.50", "fraction padded to scale");
    ASSERT_CONTAINS(resp, "{hello}", "varchar brace stays literal");
    free(resp); resp = NULL;

    /* 11. $copy source names are escaped — the error response stays
           valid JSON (a raw quote in the name must not break the wire). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"mirror\":{\"$copy\":\"bad\\\"name\"}}}",
        &resp);
    ASSERT_CONTAINS(resp, "bad\\\"name", "source name escaped in error");
    ASSERT_TRUE(SAFE_STRSTR(resp, "bad\"name") == NULL,
                "no raw quote in error response");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"note\":\"after-esc\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "writer progress after escape error");
    free(resp); resp = NULL;

    /* 12. $copy onto a NULL destination rejects (D4: current NULL is an
           error for every computed op — copying would silently clear
           NULL). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"mirror\":null}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"mirror\":{\"$copy\":\"balance\"}}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "NULL destination rejected");
    ASSERT_CONTAINS(resp, "is NULL", "NULL destination named");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"mirror\":null", "mirror still NULL after reject");
    free(resp); resp = NULL;

    /* 13. New non-finite literals reject at the write boundary. Existing
           roots may still carry old NaN/Infinity bytes; the separate
           legacy fixture below preserves coverage of that $copy guard.
           Keep a finite-source $copy check here as well. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cx\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"s:double\",\"t:double\"]}",
        &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cx\","
        "\"key\":\"n1\",\"value\":{\"s\":NaN,\"t\":1}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "NaN seed rejected");
    ASSERT_CONTAINS(resp, "only finite", "NaN rejection names the policy");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cx\","
        "\"key\":\"n1\",\"value\":{\"s\":1.5,\"t\":1}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "finite seed stored");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cx\","
        "\"key\":\"n1\",\"value\":{\"s\":Infinity}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "Infinity update rejected");
    ASSERT_CONTAINS(resp, "only finite", "Infinity rejection names the policy");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cx\","
                   "\"key\":\"n1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"t\":1", "target unchanged after NaN reject");
    ASSERT_CONTAINS(resp, "\"s\":1.5", "s unchanged after rejects");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cx\","
        "\"key\":\"n1\",\"value\":{\"t\":{\"$copy\":\"s\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\"", "finite $copy applied");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cx\","
                   "\"key\":\"n1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"t\":1.5", "copy mirrored the finite source");
    free(resp); resp = NULL;

    /* 14. D8 boundary in per-record bulk-update: a RAW object span on a
           varchar field is a computed op — the record skips, the stored
           text is untouched; a QUOTED brace string stays literal. */
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
        "\"object\":\"cp\",\"records\":[{\"key\":\"c1\","
        "\"value\":{\"note\":{\"$incr\":1}}}]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "varchar computed bulk ok");
    ASSERT_CONTAINS(resp, "\"updated\":0", "varchar computed record skipped");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "skip counted");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"note\":\"after-esc\"", "note untouched by skip");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
        "\"object\":\"cp\",\"records\":[{\"key\":\"c1\","
        "\"value\":{\"note\":\"{quoted brace}\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"updated\":1", "quoted brace bulk update applies");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"note\":\"{quoted brace}\"",
                    "quoted brace stays literal in bulk");
    free(resp); resp = NULL;

    /* 15. Long $copy source names cannot break the wire: the raw message
           is formatted (and, if needed, truncated) first, and JSON
           escaping happens exactly once at emission. Pre-escaping the
           name and then truncating at err_buf's boundary used to cut an
           escape sequence in half, swallowing the response's closing
           quote and emitting invalid JSON. */
    {
        char req[1024]; size_t off = 0;
        int req_ok = tu_appendf(req, sizeof(req), &off,
            "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
            "\"key\":\"c1\",\"value\":{\"mirror\":{\"$copy\":\"") == 0;
        for (int i = 0; i < 200 && req_ok; i++)
            req_ok = tu_appendf(req, sizeof(req), &off, "\\\"") == 0;
        if (req_ok) req_ok = tu_appendf(req, sizeof(req), &off,
            "tail\"}}}") == 0;
        ASSERT_TRUE(req_ok, "build long-name $copy request");
        if (!req_ok) { tc_close(tc); test_env_stop(&env); return 1; }
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "is not a field",
                        "long-name message survives truncation");
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\\\"}") == NULL,
                    "no dangling escape before the closing quote");
        free(resp); resp = NULL;
        tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
            "\"key\":\"c1\",\"value\":{\"note\":\"post-15\"}}", &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "writer healthy after long-name error");
        free(resp); resp = NULL;
    }

    /* 16. The D8 probe fails CLOSED: a data span the probe cannot parse
           (here: more members than JSON_OBJ_MAX_FIELDS) must skip the
           record — it used to fall open and store the computed object
           as varchar text. */
    {
        char req[4096]; size_t off = 0;
        int req_ok = tu_appendf(req, sizeof(req), &off,
            "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"cp\","
            "\"records\":[{\"key\":\"c1\",\"value\":{") == 0;
        for (int i = 0; i < 256 && req_ok; i++)
            req_ok = tu_appendf(req, sizeof(req), &off, "\"u%d\":0,", i) == 0;
        if (req_ok) req_ok = tu_appendf(req, sizeof(req), &off,
            "\"note\":{\"$incr\":1}}}]}") == 0;
        ASSERT_TRUE(req_ok, "build over-limit-fields bulk request");
        if (!req_ok) { tc_close(tc); test_env_stop(&env); return 1; }
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"updated\":0", "unparseable-probe record skipped");
        ASSERT_CONTAINS(resp, "\"skipped\":1", "skip counted for unparseable probe");
        free(resp); resp = NULL;
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                       "\"key\":\"c1\"}", &resp);
        ASSERT_CONTAINS(resp, "\"note\":\"post-15\"",
                        "note untouched by probe-failure skip");
        free(resp); resp = NULL;
    }

    /* 17. Escaping failure must fall back to a static error: raw
           client-controlled text never rides the wire, even when the
           escape itself fails (fault-injected over the test-control
           channel; the fallback replaces the whole message). */
    ASSERT_TRUE(test_env_ctl_set_json_escape_fail(&env, 1) == 0,
                "arm escape fault");
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"mirror\":{\"$copy\":\"bad\\\"name\"}}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"update failed\"",
                    "static fallback on escape failure");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "bad\"name") == NULL,
                "raw client text absent from fallback");
    free(resp); resp = NULL;
    ASSERT_TRUE(test_env_ctl_set_json_escape_fail(&env, 0) == 0,
                "clear escape fault");
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"note\":\"post-17\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "writer healthy after fault");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"mirror\":{\"$copy\":\"bad\\\"name\"}}}",
        &resp);
    ASSERT_CONTAINS(resp, "is not a field", "escape restored after fault cleared");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "update failed") == NULL,
                "fallback no longer used after fault cleared");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Recreate bytes accepted by earlier releases, while the fixture daemon
   is stopped. Segment layout follows slotcask.c:seg_record_emit and the
   stopped-daemon patch pattern in test_null_legacy_rebuild.c. The schema
   has two double fields and a two-byte nullmap prefix; only source bytes
   change, preserving header, key, nullmap, destination and keyfile refs. */
static int legacy_copy_patch_source(const char *root, const char *object,
                                    uint64_t source_bits) {
    char streams[512];
    int n = snprintf(streams, sizeof(streams),
                     "%s/default/%s/data/streams", root, object);
    if (n < 0 || (size_t)n >= sizeof(streams)) return -1;
    DIR *dir = opendir(streams);
    if (!dir) return -1;
    int patched = 0;
    struct dirent *entry;
    while (!patched && (entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        char path[1024];
        n = snprintf(path, sizeof(path), "%s/%s/000000.dat",
                     streams, entry->d_name);
        if (n < 0 || (size_t)n >= sizeof(path)) continue;
        int fd = open(path, O_RDWR);
        if (fd < 0) continue;
        uint8_t record[43] = {0};  /* 24-byte header + key x + 18-byte value */
        int ok = pread(fd, record, sizeof(record), 0) == (ssize_t)sizeof(record);
        uint16_t klen = 0;
        uint32_t vlen = 0;
        memcpy(&klen, record + 16, sizeof(klen));
        memcpy(&vlen, record + 20, sizeof(vlen));
        double source = 1.5, target = 7.0;
        ok = ok && klen == 1 && vlen == 18 && record[18] == 1 &&
             (record[19] & 1) != 0 && record[24] == 'x' &&
             record[25] == 1 && record[26] == 0 &&
             memcmp(record + 27, &source, sizeof(source)) == 0 &&
             memcmp(record + 35, &target, sizeof(target)) == 0;
        if (ok)
            ok = pwrite(fd, &source_bits, sizeof(source_bits), 27) ==
                 (ssize_t)sizeof(source_bits) && fsync(fd) == 0;
        uint64_t actual = 0;
        if (ok)
            ok = pread(fd, &actual, sizeof(actual), 27) == (ssize_t)sizeof(actual) &&
                 actual == source_bits;
        close(fd);
        patched = ok;
    }
    closedir(dir);
    return patched ? 0 : -1;
}

static int test_computed_update_legacy_nonfinite_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "legacy fixture connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    const char *objects[] = {"legacy_nan", "legacy_inf"};
    const uint64_t source_bits[] = {
        UINT64_C(0x7ff8000000000000), UINT64_C(0x7ff0000000000000)
    };
    for (int i = 0; i < 2; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"s:double\",\"t:double\"]}", objects[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"created\"", "legacy object created");
        free(resp); resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"x\",\"value\":{\"s\":1.5,\"t\":7}}", objects[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "finite fixture seed stored");
        free(resp); resp = NULL;
    }
    tc_close(tc); tc = NULL;
    char saved_root[256];
    snprintf(saved_root, sizeof(saved_root), "%s", env.db_root);
    int saved_port = env.port;
    test_env_stop_keep(&env);
    int fixture_ready = 1;
    for (int i = 0; i < 2; i++) {
        int patch_rc = legacy_copy_patch_source(saved_root, objects[i], source_bits[i]);
        ASSERT_EQ_INT(patch_rc, 0, "stored non-finite legacy source fixture verified");
        if (patch_rc != 0) fixture_ready = 0;
    }
    int rc = test_env_start_at(&env, saved_root, saved_port);
    ASSERT_EQ_INT(rc, 0, "existing root with non-finite source opens");
    if (rc != 0) {
        char base[256];
        snprintf(base, sizeof(base), "%s", saved_root);
        char *slash = strrchr(base, '/');
        if (slash) { *slash = '\0'; tu_run_cmd("rm -rf %s", base); }
        return 1;
    }
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "legacy fixture reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    if (!fixture_ready) { tc_close(tc); test_env_stop(&env); return 1; }
    for (int i = 0; i < 2; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"x\"}", objects[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, i == 0 ? "\"s\":nan" : "\"s\":inf",
                        "reopened record exposes the non-finite legacy source");
        ASSERT_CONTAINS(resp, "\"t\":7", "legacy destination starts unchanged");
        free(resp); resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"x\",\"value\":{\"t\":{\"$copy\":\"s\"}}}", objects[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\"", "legacy non-finite copy rejected");
        ASSERT_CONTAINS(resp, "not finite", "legacy source guard names non-finite input");
        free(resp); resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"x\"}", objects[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"t\":7", "legacy copy rejection preserves destination");
        free(resp); resp = NULL;
    }
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-computed-update-legacy-nonfinite",
              test_computed_update_legacy_nonfinite_run)

TEST_REGISTER("test-computed-update", test_computed_update_run)
