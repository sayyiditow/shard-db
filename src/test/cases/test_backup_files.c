/* src/test/cases/test_backup_files.c
 * A backup covers the object's files/ directory (put-file store)
 * and restore replaces live files/ for backups carrying the "files":1
 * manifest marker. A marker-less (pre-change) backup leaves live files/
 * untouched.
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
#include <sys/types.h>
#include <unistd.h>

/* Pull "<key>":"<value>" out of a JSON-ish response. Stops on first '"'. */
static int extract_str(const char *resp, const char *key, char *out, size_t out_sz) {
    if (!resp) return 0;
    char needle[64]; snprintf(needle, sizeof(needle), "\"%s\":\"", key);
    const char *p = SAFE_STRSTR(resp, needle);
    if (!p) return 0;
    p += strlen(needle);
    const char *q = strchr(p, '"');
    if (!q) return 0;
    size_t n = (size_t)(q - p);
    if (n + 1 > out_sz) n = out_sz - 1;
    memcpy(out, p, n); out[n] = '\0';
    return 1;
}

/* Trailing path component (basename) of a slash-separated path. */
static const char *path_tail(const char *p) {
    const char *t = strrchr(p, '/');
    return t ? t + 1 : p;
}

/* Whole-file substring check (files can be binary; no line semantics). */
static int file_contains(const char *path, const char *needle) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return strstr(buf, needle) != NULL;
}

/* put-file helper: base64 payload stored under <obj>'s files/ directory. */
static void put_file(TestClient *tc, const char *obj, const char *name, const char *b64) {
    char *resp = NULL;
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"put-file\",\"dir\":\"default\",\"object\":\"%s\","
        "\"filename\":\"%s\",\"data\":\"%s\"}", obj, name, b64);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"stored\"", "put-file stored");
    free(resp);
}

/* Read a live stored file (<root>/default/<obj>/files/<name>) into out. */
static int read_live(const char *root, const char *obj, const char *name,
                     char *out, size_t sz) {
    char path[512];
    snprintf(path, sizeof(path), "%s/default/%s/files/%s", root, obj, name);
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    size_t n = fread(out, 1, sz - 1, f);
    fclose(f);
    out[n] = '\0';
    return 1;
}

/* Case 1: backup captures files/, restore (marker present) replaces them. */
static int test_backup_restore_files(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bkf\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}",
        &resp); free(resp); resp = NULL;

    /* 1. Seed stored files. */
    put_file(tc, "bkf", "a.txt", "YWxwaGE=");   /* alpha */
    put_file(tc, "bkf", "b.bin", "YmV0YQ==");   /* beta  */

    /* 2. Backup. */
    tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"bkf\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"backed_up\"", "backup succeeded");
    char path[256];
    if (!extract_str(resp, "path", path, sizeof(path))) {
        ASSERT_TRUE(0, "backup path extracted");
        free(resp); tc_close(tc); test_env_stop(&env); return 1;
    }
    free(resp); resp = NULL;
    char ts[64];
    snprintf(ts, sizeof(ts), "%s", path_tail(path));

    /* 3. Backup holds both files and the manifest marker. */
    {
        char bpath[512];
        snprintf(bpath, sizeof(bpath), "%s/default/bkf/backup/%s/files/a.txt",
                 env.db_root, ts);
        ASSERT_TRUE(file_contains(bpath, "alpha"), "backup copied a.txt");
        snprintf(bpath, sizeof(bpath), "%s/default/bkf/backup/%s/files/b.bin",
                 env.db_root, ts);
        ASSERT_TRUE(file_contains(bpath, "beta"), "backup copied b.bin");
        char mpath[512];
        snprintf(mpath, sizeof(mpath), "%s/default/bkf/backup/%s/object.json",
                 env.db_root, ts);
        ASSERT_TRUE(file_contains(mpath, "\"files\":1"),
                    "manifest marks files as governed");
    }

    /* 4. Mutate live files: drop a.txt, add c.txt. */
    tc_request(tc,
        "{\"mode\":\"delete-file\",\"dir\":\"default\",\"object\":\"bkf\","
        "\"filename\":\"a.txt\"}", &resp); free(resp); resp = NULL;
    put_file(tc, "bkf", "c.txt", "Z2FtbWE=");   /* gamma */

    /* 5. Restore with force=true. */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"bkf\","
            "\"from\":\"%s\",\"force\":true}", ts);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "restore status");
        free(resp); resp = NULL;
    }

    /* 6. Live files/ mirrors the backup snapshot. */
    {
        char out[64];
        ASSERT_TRUE(read_live(env.db_root, "bkf", "a.txt", out, sizeof(out))
                    && strstr(out, "alpha") != NULL,
                    "a.txt restored with original bytes");
        ASSERT_TRUE(read_live(env.db_root, "bkf", "b.bin", out, sizeof(out))
                    && strstr(out, "beta") != NULL,
                    "b.bin intact after restore");
        char cpath[512];
        snprintf(cpath, sizeof(cpath), "%s/default/bkf/files/c.txt", env.db_root);
        ASSERT_TRUE(access(cpath, F_OK) != 0,
                    "c.txt added after the backup is gone after restore");
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Case 2: a backup taken before files/ support (no "files":1 marker)
   leaves live files/ untouched on restore — the marker alone decides. */
static int test_restore_old_backup_keeps_files(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bkf2\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}",
        &resp); free(resp); resp = NULL;

    /* 1. Seed + backup. */
    put_file(tc, "bkf2", "a.txt", "YWxwaGE=");   /* alpha */
    tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"bkf2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"backed_up\"", "backup succeeded");
    char path[256];
    if (!extract_str(resp, "path", path, sizeof(path))) {
        ASSERT_TRUE(0, "backup path extracted");
        free(resp); tc_close(tc); test_env_stop(&env); return 1;
    }
    free(resp); resp = NULL;
    char ts[64];
    snprintf(ts, sizeof(ts), "%s", path_tail(path));

    /* 2. Simulate a backup taken before this change: strip the exact
       substring ,"files":1 from the backup's manifest. The backup's
       files/ directory stays exactly as backup wrote it — the test pins
       that the marker alone decides. */
    char mpath[512];
    snprintf(mpath, sizeof(mpath), "%s/default/bkf2/backup/%s/object.json",
             env.db_root, ts);
    {
        FILE *f = fopen(mpath, "r");
        ASSERT_NOT_NULL(f, "backup manifest opened");
        if (f) {
            char buf[512] = {0};
            size_t n = fread(buf, 1, sizeof(buf) - 1, f);
            fclose(f);
            buf[n] = '\0';
            char *hit = strstr(buf, ",\"files\":1");
            if (hit) {
                memmove(hit, hit + strlen(",\"files\":1"),
                        strlen(hit + strlen(",\"files\":1")) + 1);
            }
            f = fopen(mpath, "w");
            if (f) {
                fwrite(buf, 1, strlen(buf), f);
                fclose(f);
            }
        }
    }
    ASSERT_TRUE(!file_contains(mpath, "\"files\":1"),
                "pre-change manifest simulated");

    /* 3. Mutate live files: drop a.txt, add x.txt. */
    tc_request(tc,
        "{\"mode\":\"delete-file\",\"dir\":\"default\",\"object\":\"bkf2\","
        "\"filename\":\"a.txt\"}", &resp); free(resp); resp = NULL;
    put_file(tc, "bkf2", "x.txt", "eHh4");   /* xxx */

    /* 4. Restore with force=true. */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"bkf2\","
            "\"from\":\"%s\",\"force\":true}", ts);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "restore status");
        free(resp); resp = NULL;
    }

    /* 5. Marker-less backup: live files/ untouched. */
    {
        char out[64];
        ASSERT_TRUE(read_live(env.db_root, "bkf2", "x.txt", out, sizeof(out))
                    && strstr(out, "xxx") != NULL,
                    "restore from a marker-less backup leaves live files/ untouched (x.txt kept)");
        char apath[512];
        snprintf(apath, sizeof(apath), "%s/default/bkf2/files/a.txt", env.db_root);
        ASSERT_TRUE(access(apath, F_OK) != 0,
                    "restore from a marker-less backup does not resurrect a.txt");
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Case 3: restore without force must refuse when only
   files/ is non-empty (backup governs files/, live data/indexes/metadata
   are empty after a truncate), and the refusal must leave live files/
   intact. */
static int test_restore_files_refusal(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bkf3\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}",
        &resp); free(resp); resp = NULL;

    put_file(tc, "bkf3", "a.txt", "YWxwaGE=");   /* alpha */
    tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"bkf3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"backed_up\"", "backup succeeded");
    char path[256];
    if (!extract_str(resp, "path", path, sizeof(path))) {
        ASSERT_TRUE(0, "backup path extracted");
        free(resp); tc_close(tc); test_env_stop(&env); return 1;
    }
    free(resp); resp = NULL;
    char ts[64];
    snprintf(ts, sizeof(ts), "%s", path_tail(path));

    /* Empty data/ so only files/ stands between restore and a clean sweep. */
    tc_request(tc,
        "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"bkf3\"}",
        &resp); free(resp); resp = NULL;

    /* Restore without force. */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"bkf3\","
            "\"from\":\"%s\"}", ts);
        tc_request(tc, req, &resp);
        TAP_DIAG("# restore-no-force response: %s\n", resp ? resp : "(null)");
        ASSERT_TRUE(resp && strstr(resp,
            "files/ is not empty (use force=true to overwrite)") != NULL,
            "restore without force refuses when only files/ is non-empty");
        free(resp); resp = NULL;
    }

    /* The refusal mutates nothing: live a.txt is still present. */
    {
        char apath[512];
        snprintf(apath, sizeof(apath), "%s/default/bkf3/files/a.txt", env.db_root);
        ASSERT_TRUE(access(apath, F_OK) == 0,
                    "refused restore left live files/ intact");
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-backup-restore-files", test_backup_restore_files)
TEST_REGISTER("test-restore-old-backup-keeps-files", test_restore_old_backup_keeps_files)
TEST_REGISTER("test-restore-files-refusal", test_restore_files_refusal)
