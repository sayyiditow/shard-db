/* src/test/cases/test_null_legacy_rebuild.c
 *
 * Regression test for the F6 review finding on the null-bitmap branch:
 * the schema-rebuild walk (v2_rebuild_walk_cb) blanket-NULLed legacy
 * fields that extended past the stored (trimmed) region, instead of
 * materializing the zero-padded bytes and running the §2.7 rendering
 * predicate like the update-path composer does. A trimmed trailing
 * always-render zero (int 0) converted to NULL — changing what clients
 * see — while a trimmed skip-on-zero field (date 0) correctly converts
 * to NULL.
 *
 * The legacy record is produced by byte surgery on a real record: the
 * daemon writes a nullmap record, the test stops the daemon, rewrites
 * the value region in place to legacy layout (hdr2 byte 19 = 0, value =
 * [00 02 'h' 'i' 00 00 00 00] — s="hi" at full field width, n and d
 * trimmed away by the legacy trim rule), and restarts. Adding a field
 * then forces the recompose rebuild through v2_rebuild_walk_cb.
 *
 * Post-rebuild the record must render exactly what the legacy record
 * rendered: "s":"hi", "n":0 (always-render zero stays a value),
 * "d":null (skip-on-zero converts to NULL), plus the new field.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Scan every segment under <obj_dir>/data/streams for the live record
   with key `key` and rewrite it in place as a legacy (hdr2 = 0) record
   carrying new_vlen bytes from new_value. Slots are fixed-size, so the
   shrink stays inside the record's own slot. Returns 1 on success. */
static int lr_make_legacy(const char *obj_dir, const char *key,
                          const uint8_t *new_value, size_t new_vlen) {
    char streams[768];
    snprintf(streams, sizeof(streams), "%s/data/streams", obj_dir);
    DIR *sd = opendir(streams);
    if (!sd) return 0;
    size_t klen = strlen(key);
    int done = 0;
    struct dirent *de;
    while (!done && (de = readdir(sd)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char sub[832];
        snprintf(sub, sizeof(sub), "%s/%s", streams, de->d_name);
        DIR *sdir = opendir(sub);
        if (!sdir) continue;
        struct dirent *fe;
        while (!done && (fe = readdir(sdir)) != NULL) {
            if (!strstr(fe->d_name, ".dat")) continue;
            char fp[1088];
            snprintf(fp, sizeof(fp), "%s/%s", sub, fe->d_name);
            FILE *f = fopen(fp, "r+b");
            if (!f) continue;
            fseek(f, 0, SEEK_END);
            long sz = ftell(f);
            for (long off = 0; off + 24 <= sz && !done; off += 8) {
                uint8_t hdr[24];
                if (fseek(f, off, SEEK_SET) != 0) break;
                if (fread(hdr, 1, 24, f) != 24) break;
                if (hdr[18] != 1) continue;
                uint16_t kl = (uint16_t)(hdr[16] | (hdr[17] << 8));
                if (kl != klen) continue;
                char kb[64];
                if (fread(kb, 1, (size_t)kl, f) != (size_t)kl) break;
                if (memcmp(kb, key, (size_t)kl) != 0) continue;
                uint32_t old_vlen = (uint32_t)hdr[20] |
                                    ((uint32_t)hdr[21] << 8) |
                                    ((uint32_t)hdr[22] << 16) |
                                    ((uint32_t)hdr[23] << 24);
                /* hdr2 = 0 (legacy); shrink vlen to the legacy bytes.
                   The value region starts AFTER the key bytes. */
                if (fseek(f, off + 19, SEEK_SET) != 0) break;
                fputc(0, f);
                fputc((int)(new_vlen & 0xFF), f);
                fputc((int)((new_vlen >> 8) & 0xFF), f);
                fputc((int)((new_vlen >> 16) & 0xFF), f);
                fputc((int)((new_vlen >> 24) & 0xFF), f);
                if (fseek(f, off + 24 + (long)kl, SEEK_SET) != 0) break;
                fwrite(new_value, 1, new_vlen, f);
                /* Zero the remainder of the old value region (same slot). */
                for (uint32_t z = new_vlen; z < old_vlen; z++) fputc(0, f);
                done = (fflush(f) == 0);
            }
            fclose(f);
        }
        closedir(sdir);
    }
    closedir(sd);
    return done;
}

static int test_null_legacy_rebuild_trims(void) {
    char base[] = "/tmp/lr_rebuild_XXXXXX";
    if (!mkdtemp(base)) return 1;
    TestEnv env;
    int port = test_pick_port();
    if (port <= 0) { rmdir(base); return 1; }

    char envpath[512];
    snprintf(envpath, sizeof(envpath), "%s/db.env", base);
    FILE *ef = fopen(envpath, "w");
    if (!ef) { rmdir(base); return 1; }
    fprintf(ef, "export DB_ROOT=\"%s/root\"\nexport PORT=%d\n"
                "export LOG_DIR=\"%s/logs\"\nexport LOG_LEVEL=3\n"
                "export TIMEOUT=0\n", base, port, base);
    fclose(ef);

    char db_root[512];
    snprintf(db_root, sizeof(db_root), "%s/root", base);

    /* Start 1: create the object and one nullmap record. */
    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0, "start 1");
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect 1");
    if (!tc) { test_env_stop_keep(&env); return 1; }
    char *r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"add-dir\",\"dir\":\"lr\"}", &r), 0, "add-dir");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lr\",\"object\":\"t\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"s:varchar:8\",\"n:int\",\"d:date\"]}", &r), 0,
        "create lr object");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lr\",\"object\":\"t\",\"key\":\"k1\","
        "\"value\":{\"s\":\"hi\"}}", &r), 0, "insert k1");
    free(r); r = NULL;
    tc_close(tc);
    test_env_stop_keep(&env);

    /* Byte surgery: legacy layout. s:varchar:8 stores 8 CONTENT bytes,
       10 on disk ([00 02 'h' 'i' + 6 slack zeros]); n and d are the zero
       bytes the legacy trim rule cut — vlen 10 ends at s's field
       boundary. */
    static const uint8_t legacy_val[10] =
        { 0x00, 0x02, 'h', 'i', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    char obj_dir[768];
    snprintf(obj_dir, sizeof(obj_dir), "%s/lr/t", db_root);
    ASSERT_EQ_INT(lr_make_legacy(obj_dir, "k1",
                                 legacy_val, sizeof(legacy_val)), 1,
                  "rewrote k1 as a trimmed legacy record");

    /* Start 2: the legacy record decodes exactly as it did historically —
       s renders, the trimmed int zero still renders 0, the trimmed date
       zero is absent (skip-on-zero, key omitted). */
    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0, "start 2");
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect 2");
    if (!tc) { test_env_stop_keep(&env); return 1; }
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"lr\",\"object\":\"t\",\"key\":\"k1\"}",
        &r), 0, "get legacy k1");
    ASSERT_TRUE(r && strstr(r, "\"s\":\"hi\"") != NULL,
                "legacy s renders");
    ASSERT_TRUE(r && strstr(r, "\"n\":0") != NULL,
                "legacy trimmed int zero renders 0 (pre-rebuild baseline)");
    ASSERT_TRUE(r && strstr(r, "\"d\"") == NULL,
                "legacy trimmed date is absent pre-rebuild");
    free(r); r = NULL;

    /* add-field forces the recompose rebuild through v2_rebuild_walk_cb. */
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"lr\",\"object\":\"t\","
        "\"fields\":[\"x:varchar:4\"]}", &r), 0, "add-field request");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL,
                "add-field accepted (triggers the recompose rebuild)");
    free(r); r = NULL;

    /* Post-rebuild rendering must be IDENTICAL for the converted field
       set: the trimmed int zero stays a zero VALUE (the §2.7 composer's
       materialize-then-predicate rule), the trimmed date converts to
       NULL (skip-on-zero), and the new field joins as zeros. */
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"lr\",\"object\":\"t\",\"key\":\"k1\"}",
        &r), 0, "get rebuilt k1");
    ASSERT_TRUE(r && strstr(r, "\"s\":\"hi\"") != NULL,
                "rebuild keeps s");
    ASSERT_TRUE(r && strstr(r, "\"n\":0") != NULL,
                "trimmed always-render zero stays a VALUE after rebuild "
                "(was blanket-NULLed by the old code)");
    ASSERT_TRUE(r && strstr(r, "\"d\":null") != NULL,
                "trimmed skip-on-zero date converts to NULL after rebuild");
    free(r); r = NULL;
    tc_close(tc);
    test_env_stop_keep(&env);

    char cmd[600];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    (void)system(cmd);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-legacy-rebuild-trims", test_null_legacy_rebuild_trims)
