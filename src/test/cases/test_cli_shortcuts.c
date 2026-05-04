/* src/test/cases/test_cli_shortcuts.c
 * Port of tests/test-cli-shortcuts.sh — CLI shortcut commands (count,
 * aggregate, delete-file). Most logic is positional-arg parsing in the
 * CLI binary; we exercise it via system() with CWD=<base> so the CLI
 * picks up the fixture's db.env (and PORT).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static char *capture_cmd(const char *fmt, ...) {
    char cmd[4096];
    va_list ap; va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    FILE *fp = popen(cmd, "r");
    if (!fp) return NULL;
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) { pclose(fp); return NULL; }
    int c;
    while ((c = fgetc(fp)) != EOF) {
        if (len + 1 >= cap) {
            cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); pclose(fp); return NULL; }
            buf = nb;
        }
        buf[len++] = (char)c;
    }
    buf[len] = '\0';
    pclose(fp);
    return buf;
}

static int file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

/* base = parent(db_root). The fixture wrote db.env there. */
static void base_of(const char *db_root, char *out, size_t out_sz) {
    const char *slash = strrchr(db_root, '/');
    if (!slash || slash == db_root) { out[0] = '\0'; return; }
    size_t n = (size_t)(slash - db_root);
    if (n + 1 > out_sz) { out[0] = '\0'; return; }
    memcpy(out, db_root, n);
    out[n] = '\0';
}

static int test_cli_shortcuts_run(void) {
    /* Resolve absolute path to the just-built shard-db CLI. */
    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db binary not found"); return 1;
    }

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char base[256]; base_of(env.db_root, base, sizeof(base));

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cli_orders\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\",\"amount:int\",\"region:varchar:16\"]}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cli_files\","
        "\"splits\":16,\"max_key\":32,\"fields\":[\"name:varchar:32\"]}",
        &resp); free(resp); resp = NULL;

    const char *seed[] = {
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cli_orders\",\"key\":\"o1\",\"value\":{\"status\":\"paid\",\"amount\":100,\"region\":\"us\"}}",
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cli_orders\",\"key\":\"o2\",\"value\":{\"status\":\"paid\",\"amount\":250,\"region\":\"us\"}}",
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cli_orders\",\"key\":\"o3\",\"value\":{\"status\":\"paid\",\"amount\":75,\"region\":\"eu\"}}",
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cli_orders\",\"key\":\"o4\",\"value\":{\"status\":\"pending\",\"amount\":40,\"region\":\"eu\"}}",
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cli_orders\",\"key\":\"o5\",\"value\":{\"status\":\"cancelled\",\"amount\":0,\"region\":\"us\"}}",
    };
    for (size_t i = 0; i < sizeof(seed)/sizeof(seed[0]); i++) {
        tc_request(tc, seed[i], &resp); free(resp); resp = NULL;
    }

    /* count CLI. */
    char *out = capture_cmd("cd %s && %s count default cli_orders 2>&1", base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "5") != NULL, "count no-criteria returns total");
    free(out);

    out = capture_cmd("cd %s && %s count default cli_orders '[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]' 2>&1",
                      base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "3") != NULL, "count with eq criteria returns 3");
    free(out);

    out = capture_cmd("cd %s && %s count default cli_orders '[{\"field\":\"amount\",\"op\":\"gte\",\"value\":\"100\"}]' 2>&1",
                      base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "2") != NULL, "count with gte criteria returns 2");
    free(out);

    out = capture_cmd("cd %s && %s count default cli_orders '[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"nope\"}]' 2>&1",
                      base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "0") != NULL, "count no-match returns 0");
    free(out);

    out = capture_cmd("cd %s && %s count default 2>&1", base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "Usage: shard-db count") != NULL,
                "count usage when too few args");
    free(out);

    /* aggregate CLI. */
    out = capture_cmd("cd %s && %s aggregate default cli_orders "
                      "'[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"total\"}]' 2>&1",
                      base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "\"n\":5") != NULL, "aggregate bare: count=5");
    ASSERT_TRUE(out && strstr(out, "\"total\":465") != NULL, "aggregate bare: sum=465");
    free(out);

    out = capture_cmd("cd %s && %s aggregate default cli_orders "
                      "'[{\"fn\":\"count\",\"alias\":\"n\"}]' 'status' 2>&1",
                      base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "\"status\":\"paid\",\"n\":3") != NULL, "group_by status: paid=3");
    ASSERT_TRUE(out && strstr(out, "\"status\":\"pending\",\"n\":1") != NULL, "group_by status: pending=1");
    ASSERT_TRUE(out && strstr(out, "\"status\":\"cancelled\",\"n\":1") != NULL, "group_by status: cancelled=1");
    free(out);

    out = capture_cmd("cd %s && %s aggregate default cli_orders "
                      "'[{\"fn\":\"count\",\"alias\":\"n\"}]' 'status, region' 2>&1",
                      base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "\"status\":\"paid\",\"region\":\"us\",\"n\":2") != NULL,
                "multi-group: paid+us");
    ASSERT_TRUE(out && strstr(out, "\"status\":\"paid\",\"region\":\"eu\",\"n\":1") != NULL,
                "multi-group: paid+eu");
    free(out);

    out = capture_cmd("cd %s && %s aggregate default cli_orders "
                      "'[{\"fn\":\"count\",\"alias\":\"n\"}]' 'status' "
                      "'[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]' 2>&1",
                      base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "\"status\":\"paid\",\"n\":2") != NULL,
                "aggregate criteria: paid filtered to us=2");
    ASSERT_TRUE(out && strstr(out, "\"status\":\"pending\"") == NULL,
                "aggregate criteria: pending excluded");
    free(out);

    out = capture_cmd("cd %s && %s aggregate default cli_orders "
                      "'[{\"fn\":\"count\",\"alias\":\"n\"}]' 'status' '' "
                      "'[{\"field\":\"n\",\"op\":\"gte\",\"value\":\"2\"}]' 2>&1",
                      base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "\"status\":\"paid\",\"n\":3") != NULL, "having: paid kept");
    ASSERT_TRUE(out && strstr(out, "\"status\":\"pending\"") == NULL, "having: pending dropped");
    free(out);

    out = capture_cmd("cd %s && %s aggregate default cli_orders 2>&1", base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "Usage: shard-db aggregate") != NULL, "aggregate usage when too few args");
    free(out);

    /* delete-file mode + CLI. Put a temp file via put-file CLI, then delete. */
    char tmpfile[256];
    snprintf(tmpfile, sizeof(tmpfile), "/tmp/shard_cli_pf_%d", (int)getpid());
    FILE *f = fopen(tmpfile, "w");
    if (f) { fputs("shard-db test payload", f); fclose(f); }

    /* basename — we just use the chosen filename. */
    const char *basename = strrchr(tmpfile, '/'); basename = basename ? basename + 1 : tmpfile;

    out = capture_cmd("cd %s && %s put-file default cli_files %s 2>&1", base, shard_db_abs, tmpfile);
    free(out);

    /* Resolve on-disk store path via get-file-path JSON. */
    char req[512]; char store_path[1024] = {0};
    snprintf(req, sizeof(req),
        "{\"mode\":\"get-file-path\",\"dir\":\"default\",\"object\":\"cli_files\","
        "\"filename\":\"%s\"}", basename);
    tc_request(tc, req, &resp);
    if (resp) {
        const char *p = strstr(resp, "\"path\":\"");
        if (p) {
            p += strlen("\"path\":\"");
            const char *q = strchr(p, '"');
            if (q && (size_t)(q - p) < sizeof(store_path)) {
                memcpy(store_path, p, q - p); store_path[q - p] = '\0';
            }
        }
        free(resp); resp = NULL;
    }
    ASSERT_TRUE(store_path[0] != '\0' && file_exists(store_path),
                "put-file stored file exists on disk");

    out = capture_cmd("cd %s && %s delete-file default cli_files '%s' 2>&1", base, shard_db_abs, basename);
    ASSERT_TRUE(out && strstr(out, "\"status\":\"deleted\"") != NULL, "delete-file success status");
    {
        char want[128]; snprintf(want, sizeof(want), "\"filename\":\"%s\"", basename);
        ASSERT_TRUE(out && strstr(out, want) != NULL, "delete-file echoes filename");
    }
    free(out);
    ASSERT_TRUE(!file_exists(store_path), "delete-file removed file from disk");

    /* Second delete → not found. */
    out = capture_cmd("cd %s && %s delete-file default cli_files '%s' 2>&1", base, shard_db_abs, basename);
    ASSERT_TRUE(out && strstr(out, "\"error\":\"file not found\"") != NULL,
                "delete-file on missing returns error");
    free(out);

    /* Filename traversal rejected. */
    out = capture_cmd("cd %s && %s delete-file default cli_files '../evil.txt' 2>&1", base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "\"error\":\"invalid filename\"") != NULL,
                "delete-file rejects traversal");
    free(out);

    /* JSON mode without filename. */
    tc_request(tc, "{\"mode\":\"delete-file\",\"dir\":\"default\",\"object\":\"cli_files\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"filename is required\"",
                    "delete-file JSON missing filename");
    free(resp); resp = NULL;

    /* JSON mode happy path: re-upload, delete via JSON. */
    out = capture_cmd("cd %s && %s put-file default cli_files %s 2>&1", base, shard_db_abs, tmpfile);
    free(out);
    ASSERT_TRUE(file_exists(store_path), "re-upload present before JSON delete");

    snprintf(req, sizeof(req),
        "{\"mode\":\"delete-file\",\"dir\":\"default\",\"object\":\"cli_files\","
        "\"filename\":\"%s\"}", basename);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "delete-file JSON succeeds");
    free(resp); resp = NULL;
    ASSERT_TRUE(!file_exists(store_path), "JSON delete removed file");

    /* CLI usage. */
    out = capture_cmd("cd %s && %s delete-file default cli_files 2>&1", base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "Usage: shard-db delete-file") != NULL,
                "delete-file usage when missing args");
    free(out);

    unlink(tmpfile);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-cli-shortcuts", test_cli_shortcuts_run)
