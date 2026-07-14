/* src/test/cases/test_vacuum_streams_mismatch.c
 *
 * Default v2 vacuum auto-rebuilds when schema.conf's stream count diverges
 * from slotcask_streams_for_nproc() — simulates a CPU upgrade or a
 * hand-edited schema.conf.
 *
 * Flow:
 *   1. Start daemon, create v2 object → schema.streams = nproc-derived (X).
 *   2. Insert N records, snapshot schema.conf.
 *   3. SIGKILL daemon (preserves on-disk state).
 *   4. Hand-edit schema.conf: streams = X+1 (deliberate mismatch).
 *   5. Restart daemon at same db_root, same port.
 *   6. Run vacuum (no flags).
 *   7. Re-read schema.conf: streams should be back to X (rebuilt).
 *   8. Every original record still readable.
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
#include <sys/stat.h>
#include <limits.h>

/* Read whole file → malloc'd nul-terminated string. NULL on error. */
static char *read_text_file(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    fread(buf, 1, (size_t)sz, f);
    buf[sz] = '\0';
    fclose(f);
    return buf;
}

/* Schema.conf line format: dir:object:splits:max_key:storage_version:streams.
   Parse the streams field for our (default, vmstreams) row. -1 on miss. */
static int parse_streams_field(const char *schema_text) {
    if (!schema_text) return -1;
    const char *line = strstr(schema_text, "default:vmstreams:");
    if (!line) return -1;
    int splits, max_key, sv, streams;
    if (sscanf(line, "default:vmstreams:%d:%d:%d:%d",
               &splits, &max_key, &sv, &streams) >= 4) {
        return streams;
    }
    return -1;
}

/* Rewrite the schema.conf line for default:vmstreams with new_streams. */
static int set_streams_in_schema_conf(const char *path, int new_streams) {
    char *txt = read_text_file(path);
    if (!txt) return -1;

    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    FILE *out = fopen(tmp, "w");
    if (!out) { free(txt); return -1; }

    char *p = txt;
    while (*p) {
        char *eol = strchr(p, '\n');
        size_t len = eol ? (size_t)(eol - p) : strlen(p);
        char line[1024];
        if (len >= sizeof(line)) len = sizeof(line) - 1;
        memcpy(line, p, len);
        line[len] = '\0';

        if (strncmp(line, "default:vmstreams:", 18) == 0) {
            int splits, max_key, sv, old_streams;
            if (sscanf(line, "default:vmstreams:%d:%d:%d:%d",
                       &splits, &max_key, &sv, &old_streams) >= 3) {
                fprintf(out, "default:vmstreams:%d:%d:%d:%d\n",
                        splits, max_key, sv, new_streams);
            } else {
                fputs(line, out);
                fputc('\n', out);
            }
        } else {
            fputs(line, out);
            if (eol) fputc('\n', out);
        }
        if (!eol) break;
        p = eol + 1;
    }
    fclose(out);
    free(txt);
    return rename(tmp, path);
}

static int test_vacuum_streams_mismatch_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Create v2 object — streams gets nproc-derived value. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"vmstreams\","
        "\"splits\":16,\"max_key\":32,"
        "\"fields\":[\"name:varchar:32\",\"age:int\"],\"indexes\":[]}",
        &resp);
    free(resp); resp = NULL;

    /* Insert 50 records. */
    for (int i = 0; i < 50; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"vmstreams\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"name%d\",\"age\":%d}}",
            i, i, i % 100);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }

    /* Snapshot pre-mismatch streams count from schema.conf. */
    char schema_path[PATH_MAX];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", env.db_root);
    char *schema_pre = read_text_file(schema_path);
    int streams_pre = parse_streams_field(schema_pre);
    free(schema_pre);
    ASSERT_TRUE(streams_pre >= 1 && streams_pre <= 16,
                "pre-mismatch streams in [1,16]");
    if (streams_pre < 1) {
        tc_close(tc);
        test_env_stop(&env);
        return 1;
    }

    tc_close(tc);

    /* Kill the daemon (preserve on-disk state) so we can hand-edit
       schema.conf without racing against any in-flight reload. */
    test_env_kill(&env);

    /* Hand-edit schema.conf: streams = streams_pre + 1 (deliberate mismatch
       against nproc-derived value). On the next slotcask_open, the engine
       will route by hash[15] % (X+1) and create stream X dir on demand,
       leaving on-disk state internally consistent with the lie. */
    int wrong_streams = streams_pre + 1;
    if (wrong_streams > 16) wrong_streams = streams_pre - 1;  /* cap */
    if (wrong_streams < 1) wrong_streams = 1;
    if (wrong_streams == streams_pre) {
        /* Edge case: if nproc is exactly 16 and streams_pre=16, we can
           only diverge downward; but +1 over 16 is also a valid mismatch
           since slotcask_streams_for_nproc still returns 16. */
        wrong_streams = streams_pre - 1;
        if (wrong_streams < 1) wrong_streams = 2;
    }
    ASSERT_EQ_INT(set_streams_in_schema_conf(schema_path, wrong_streams), 0,
                  "rewrite schema.conf with mismatched streams");

    /* Restart daemon at the same db_root + port. */
    if (test_env_start_at(&env, saved_db_root, saved_port) != 0) {
        return 1;
    }
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect");
    if (!tc) { test_env_stop(&env); return 1; }

    /* Sanity: every record still readable under the lying schema. */
    int readable_pre = 0;
    for (int i = 0; i < 50; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"vmstreams\","
            "\"key\":\"k%d\"}", i);
        tc_request(tc, req, &resp);
        if (resp && SAFE_STRSTR(resp, "\"name\"") != NULL) readable_pre++;
        free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(readable_pre, 50, "all 50 records readable under mismatched schema");

    /* Confirm schema.conf is currently the lie. */
    char *schema_mid = read_text_file(schema_path);
    int streams_mid = parse_streams_field(schema_mid);
    free(schema_mid);
    ASSERT_EQ_INT(streams_mid, wrong_streams, "schema.conf reflects the lie pre-vacuum");

    /* Vacuum (no flags) should detect the mismatch and rebuild. */
    tc_request(tc, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"vmstreams\"}",
                &resp);
    /* The heavy path returns "rebuilt"; the light path returns "vacuumed".
       We expect rebuilt because streams diverged. */
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "vacuum dispatches to rebuild");
    free(resp); resp = NULL;

    /* schema.conf should be back to streams_pre (nproc-derived). */
    char *schema_post = read_text_file(schema_path);
    int streams_post = parse_streams_field(schema_post);
    free(schema_post);
    ASSERT_EQ_INT(streams_post, streams_pre,
                  "schema.conf streams reverted to nproc-derived");

    /* Every record still readable after rebuild. */
    int readable_post = 0;
    for (int i = 0; i < 50; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"vmstreams\","
            "\"key\":\"k%d\"}", i);
        tc_request(tc, req, &resp);
        if (resp && SAFE_STRSTR(resp, "\"name\"") != NULL) readable_post++;
        free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(readable_post, 50, "all 50 records still readable after rebuild");

    /* No-op idempotence: a second vacuum should be a fast cleaned=0 path,
       not another rebuild — schema and nproc match now. */
    tc_request(tc, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"vmstreams\"}",
                &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"vacuumed\"", "second vacuum stays in light path");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-vacuum-streams-mismatch", test_vacuum_streams_mismatch_run)
