/* src/test/cases/test_not_null_edit_gate.c
 *
 * :not_null modifier — edit-field add-gate
 * clean add, dry_run prediction, NULL refusal, legacy-generation
 * refusal, invalid-hdr2/open refusal, invalid-bitmap/callback refusal,
 * missing-segment (invalid live ref) refusal, composite-index control.
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

/* Rewrite the live record `key` in place with hdr2_byte and new_vlen
   value bytes. Fixed-size slots keep the shrink in-slot. Returns 1 on
   success. Run with the daemon STOPPED. */
static int nn_surgery(const char *obj_dir, const char *key,
                      uint8_t hdr2_byte,
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
                if (fseek(f, off + 19, SEEK_SET) != 0) break;
                fputc((int)hdr2_byte, f);
                fputc((int)(new_vlen & 0xFF), f);
                fputc((int)((new_vlen >> 8) & 0xFF), f);
                fputc((int)((new_vlen >> 16) & 0xFF), f);
                fputc((int)((new_vlen >> 24) & 0xFF), f);
                if (fseek(f, off + 24 + (long)kl, SEEK_SET) != 0) break;
                fwrite(new_value, 1, new_vlen, f);
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

/* Truncate every segment .dat under obj_dir to 0 bytes — live kf
   references become unreadable (the strict gate must flag
   them). Returns 1 if anything was truncated. Daemon STOPPED. */
static int nn_truncate_all_segments(const char *obj_dir) {
    char streams[768];
    snprintf(streams, sizeof(streams), "%s/data/streams", obj_dir);
    DIR *sd = opendir(streams);
    if (!sd) return 0;
    int any = 0;
    struct dirent *de;
    while ((de = readdir(sd)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char sub[832];
        snprintf(sub, sizeof(sub), "%s/%s", streams, de->d_name);
        DIR *sdir = opendir(sub);
        if (!sdir) continue;
        struct dirent *fe;
        while ((fe = readdir(sdir)) != NULL) {
            if (!strstr(fe->d_name, ".dat")) continue;
            char fp[1088];
            snprintf(fp, sizeof(fp), "%s/%s", sub, fe->d_name);
            FILE *f = fopen(fp, "r+b");
            if (!f) continue;
            if (ftruncate(fileno(f), 0) == 0) any = 1;
            fclose(f);
        }
        closedir(sdir);
    }
    closedir(sd);
    return any;
}

static int test_not_null_edit_gate_run(void) {
    char base[] = "/tmp/nn_gate_XXXXXX";
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
    char objdir[600];

    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0, "start");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop_keep(&env); return 1; }
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* === g1: clean add on an all-values field. ==================== */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"g1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"a:int\",\"n:int\"]}",
        &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"g1\","
                   "\"key\":\"k1\",\"value\":{\"a\":1,\"n\":10}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"g1\","
                   "\"key\":\"k2\",\"value\":{\"a\":2,\"n\":20}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"g1\",\"fields\":[\"n:int:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "clean add succeeds");
    free(resp); resp = NULL;
    snprintf(objdir, sizeof(objdir), "%s/default/g1/fields.conf", db_root);
    char *fc = tu_read_file(objdir);
    ASSERT_TRUE(fc && strstr(fc, "n:int:not_null"), "g1 line carries token");
    free(fc);
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"g1\","
                   "\"key\":\"k3\",\"value\":{\"a\":3}}", &resp);
    ASSERT_CONTAINS(resp, "field 'n' is not nullable",
                    "enforcement live after gate");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"g1\","
                   "\"criteria\":[{\"field\":\"n\",\"op\":\"not_exists\"}]}",
               &resp);
    ASSERT_EQ_INT(atoi(resp), 0, "not_exists finds 0 rows (no shortcut, D6)");
    free(resp); resp = NULL;

    /* === g2: NULL present → refuse (real + dry_run); data untouched. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"g2\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"m:int\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"g2\","
                   "\"key\":\"g2a\",\"value\":{\"m\":1}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"g2\","
                   "\"key\":\"g2b\",\"value\":{}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"g2\",\"fields\":[\"m:int:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "cannot add :not_null to field 'm'",
                    "refusal names the field");
    ASSERT_CONTAINS(resp, "1 record(s) have NULL", "refusal counts NULLs");
    free(resp); resp = NULL;
    snprintf(objdir, sizeof(objdir), "%s/default/g2/fields.conf", db_root);
    fc = tu_read_file(objdir);
    ASSERT_TRUE(fc && !strstr(fc, "not_null"), "fields.conf unchanged");
    free(fc);
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"g2\",\"fields\":[\"m:int:not_null\"],"
                   "\"dry_run\":true}", &resp);
    ASSERT_CONTAINS(resp, "cannot add :not_null to field 'm'",
                    "dry_run predicts the refusal");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"g2\","
                   "\"key\":\"g2b\"}", &resp);
    ASSERT_CONTAINS(resp, "\"m\":null", "data untouched");
    free(resp); resp = NULL;

    /* === gs: unreadable live reference → refuse. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gs\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"c:int\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gs\","
                   "\"key\":\"gs1\",\"value\":{\"c\":5}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);
    test_env_stop_keep(&env);
    snprintf(objdir, sizeof(objdir), "%s/default/gs", db_root);
    ASSERT_EQ_INT(nn_truncate_all_segments(objdir), 1, "segments truncated");
    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0, "restart gs");
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect gs");
    if (!tc) { test_env_stop_keep(&env); return 1; }
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"gs\",\"fields\":[\"c:int:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "cannot add :not_null to field 'c'",
                    "invalid live ref refuses the edit");
    free(resp); resp = NULL;
    snprintf(objdir, sizeof(objdir), "%s/default/gs/fields.conf", db_root);
    fc = tu_read_file(objdir);
    ASSERT_TRUE(fc && !strstr(fc, "not_null"), "invalid-reference refusal preserves schema");
    free(fc);

    /* === gc: invalid hdr2 prevents registry open; edit refuses. ==== */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gc\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"c:int\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gc\","
                   "\"key\":\"gc1\",\"value\":{\"c\":5}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);
    test_env_stop_keep(&env);
    static const uint8_t junk[2] = {0xAA, 0xBB};
    snprintf(objdir, sizeof(objdir), "%s/default/gc", db_root);
    ASSERT_EQ_INT(nn_surgery(objdir, "gc1", 0x02, junk, sizeof(junk)), 1,
                  "corrupt hdr2 written");
    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0, "restart gc");
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect gc");
    if (!tc) { test_env_stop_keep(&env); return 1; }
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"gc\",\"fields\":[\"c:int:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "record scan failed",
                    "invalid hdr2/open failure refuses the edit");
    free(resp); resp = NULL;

    /* === gb: legal hdr2, invalid bitmap prefix. Stream recovery can
       open this record; typed_record_view in the gate callback refuses it.
       Keep the original 6-byte value length and 40-byte padded record. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gb\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"c:int\"]}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "create bitmap fixture");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gb\","
        "\"key\":\"gb1\",\"value\":{\"c\":5}}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "insert bitmap fixture");
    free(resp); resp = NULL;
    tc_close(tc);
    test_env_stop_keep(&env);
    static const uint8_t bad_bitmap[6] = {0, 0, 0, 0, 0, 5};
    snprintf(objdir, sizeof(objdir), "%s/default/gb", db_root);
    ASSERT_EQ_INT(nn_surgery(objdir, "gb1", 0x01, bad_bitmap, sizeof(bad_bitmap)), 1,
                  "invalid bitmap prefix with legal hdr2");
    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0, "restart bitmap fixture");
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect bitmap fixture");
    if (!tc) { test_env_stop_keep(&env); return 1; }
    /* exists opens the registry and reads the valid key/reference without
       typed decoding. Success distinguishes callback refusal from an
       unopenable object. */
    tc_request(tc,
        "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"gb\","
        "\"key\":\"gb1\"}", &resp);
    ASSERT_CONTAINS(resp, "true", "registry opens malformed-bitmap object");
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "registry open succeeded");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"gb\","
        "\"fields\":[\"c:int:not_null\"]}", &resp);
    ASSERT_CONTAINS(resp, "record scan failed", "callback rejects malformed bitmap");
    free(resp); resp = NULL;
    snprintf(objdir, sizeof(objdir), "%s/default/gb/fields.conf", db_root);
    fc = tu_read_file(objdir);
    ASSERT_TRUE(fc && !strstr(fc, "not_null"), "callback refusal preserves schema");
    free(fc);

    /* === gl: legacy-generation record (hdr2 = 0, long 0 → absent →
           NULL) → refuse; present legacy value → succeed. ========== */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gl\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"t:long\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gl\","
                   "\"key\":\"gl1\",\"value\":{\"t\":5}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);
    test_env_stop_keep(&env);
    static const uint8_t legacy_zero[8] = {0};
    snprintf(objdir, sizeof(objdir), "%s/default/gl", db_root);
    ASSERT_EQ_INT(nn_surgery(objdir, "gl1", 0x00, legacy_zero, 8), 1,
                  "legacy record written");
    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0, "restart gl");
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect gl");
    if (!tc) { test_env_stop_keep(&env); return 1; }
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gl\","
                   "\"key\":\"gl1\"}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"t\":"),
                "legacy long-0 is omitted before nullmap conversion");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"gl\",\"fields\":[\"t:long:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "1 record(s) have NULL",
                    "legacy absence counts as NULL");
    free(resp); resp = NULL;
    tc_close(tc);
    test_env_stop_keep(&env);
    static const uint8_t legacy_nine[8] = {0, 0, 0, 0, 0, 0, 0, 9};
    ASSERT_EQ_INT(nn_surgery(objdir, "gl1", 0x00, legacy_nine, 8), 1,
                  "legacy present value written");
    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0, "restart gl 2");
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect gl 2");
    if (!tc) { test_env_stop_keep(&env); return 1; }
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"gl\",\"fields\":[\"t:long:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"",
                    "clean legacy field admits the constraint");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gl\","
                   "\"key\":\"gl2\",\"value\":{}}", &resp);
    ASSERT_CONTAINS(resp, "field 't' is not nullable",
                    "omission now refused (no default)");
    free(resp); resp = NULL;

    /* === gx: composite index control — the constraint is per-field. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gx\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"a:int\",\"n:int\"],"
        "\"indexes\":[\"a+n\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gx\","
                   "\"key\":\"x1\",\"value\":{\"a\":1,\"n\":20}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gx\","
                   "\"key\":\"x2\",\"value\":{\"a\":2,\"n\":30}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"gx\",\"fields\":[\"a:int:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"",
                    "per-field constraint with composite index");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gx\","
                   "\"key\":\"x3\",\"value\":{\"n\":40}}", &resp);
    ASSERT_CONTAINS(resp, "field 'a' is not nullable",
                    "component omission refused");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"gx\","
                   "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}]}",
               &resp);
    ASSERT_EQ_INT(atoi(resp), 1,
                  "composite index still serves its leading field");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop_keep(&env);
    /* The manual-env base holds hundreds of MB of segment files;
       leaving it behind fills /tmp across suite runs (a watchdog-killed
       worker cannot clean up its own). */
    char cleanup_cmd[600];
    snprintf(cleanup_cmd, sizeof(cleanup_cmd), "rm -rf %s", base);
    (void)system(cleanup_cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-not-null-edit-gate", test_not_null_edit_gate_run)
