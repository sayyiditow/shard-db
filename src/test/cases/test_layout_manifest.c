#include "test_runner.h"
#include "bappend.h"
#include "test_assert.h"
#include "fixtures.h"
#include "test_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {
    char base[256];
    char db_root[300];
    int  port;
} LmRoot;

static void lm_init(LmRoot *m) {
    memset(m, 0, sizeof(*m));
    /* Same convention as the harness: SHARD_TEST_TMPDIR overrides /tmp so a
       full run can be pointed at a disk-backed directory. */
    const char *tmpdir = getenv("SHARD_TEST_TMPDIR");
    if (!tmpdir || !tmpdir[0]) tmpdir = "/tmp";
    char tmpl[512];
    snprintf(tmpl, sizeof(tmpl), "%s/shard-db-lm-XXXXXX", tmpdir);
    char *d = mkdtemp(tmpl);
    ASSERT_NOT_NULL(d, "mkdtemp layout root");
    if (!d) return;
    snprintf(m->base, sizeof(m->base), "%s", d);
    snprintf(m->db_root, sizeof(m->db_root), "%s/data", d);
    ASSERT_EQ_INT(mkdir(m->db_root, 0755), 0, "mkdir db_root");
    m->port = test_pick_port();
    ASSERT_TRUE(m->port > 0, "pick port");
}

static void lm_env(const LmRoot *m, const char *extra) {
    char path[300];
    snprintf(path, sizeof(path), "%s/db.env", m->base);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f, "write db.env");
    if (!f) return;
    fprintf(f,
        "export DB_ROOT=\"%s\"\nexport PORT=%d\nexport TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n", m->db_root, m->port, m->base);
    if (extra && extra[0]) fprintf(f, "%s", extra);
    fclose(f);
}

static void lm_destroy(LmRoot *m) { tu_run_cmd("rm -rf %s", m->base); }

static TestClient *lm_connect(int port) {
    /* Backup/restore fsync-copy whole objects; the default 5 s I/O timeout
       is too tight when the full suite saturates the disk. */
    TestClientCfg cfg = { .host = "127.0.0.1", .port = port, .io_timeout_ms = 60000 };
    return tc_connect(&cfg);
}

/* Rewrites the single schema.conf line for lay:o by appending `tail`
   (e.g. ":layout=packed"). Returns 0 on success. */
static int lm_schema_append(const LmRoot *m, const char *tail) {
    char path[400], tmp[420];
    snprintf(path, sizeof(path), "%s/schema.conf", m->db_root);
    snprintf(tmp, sizeof(tmp), "%s.lm", path);
    FILE *in = fopen(path, "r");
    FILE *out = fopen(tmp, "w");
    if (!in || !out) { if (in) fclose(in); if (out) fclose(out); return -1; }
    char line[1024];
    while (fgets(line, sizeof(line), in)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (strncmp(line, "lay:o:", 6) == 0) fprintf(out, "%s%s\n", line, tail);
        else fprintf(out, "%s\n", line);
    }
    fclose(in);
    if (fclose(out) != 0) return -1;
    return rename(tmp, path);
}

static int lm_build(const LmRoot *m, TestEnv *env) {
    if (test_env_start_at(env, m->db_root, m->port) != 0) return -1;
    TestClient *tc = lm_connect(m->port);
    if (!tc) return -1;
    char *r = NULL;
    int ok = tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"lay\"}", &r) == 0;
    free(r); r = NULL;
    ok = ok && tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"o\","
        "\"declaration_layout\":\"legacy\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:20\",\"qty:int\"]}",
        &r) == 0 && r && !strstr(r, "\"error\"");
    free(r);
    tc_close(tc);
    test_env_stop_keep(env);
    return ok ? 0 : -1;
}

/* packed token accepted by the strict reader; object stays servable */
static int test_layout_token_packed_loads(void) {
    LmRoot m;
    lm_init(&m);
    if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build(&m, &env), 0, "build legacy object");
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "append token");
    /* Task 1 alone only requires the token to PARSE and the process to start;
       binding is Task 2, so fields.conf is still legacy here: expect the
       load to be REFUSED by the loader (describe errors), not served as legacy. */
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start with token");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"o\"}", &r),
            0, "describe");
        ASSERT_NOT_NULL(r, "resp");
        ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"legacy\"") == NULL,
                    "packed token is never silently served as legacy");
        free(r);
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* malformed / duplicate / unknown layout tokens refuse startup */
static int test_layout_token_malformed_refuses(void) {
    static const char *const bad[] = {
        ":layout=legacy", ":layout=", ":layout", ":layout=packed:layout=packed",
        ":layout=Packed", ":layout=packed2"
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        LmRoot m;
        lm_init(&m);
        if (t_ctx->failed) return 1;
        lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build(&m, &env), 0, "build legacy object");
        ASSERT_EQ_INT(lm_schema_append(&m, bad[i]), 0, "append bad token");
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0,
                    "malformed layout token refuses startup");
        lm_destroy(&m);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* layout token together with auto_key must be accepted by the 8-part cap (F11).
   Regression pin: GREEN on base too (the 8th token is silently dropped there). */
static int test_layout_token_with_auto_key_parses(void) {
    LmRoot m;
    lm_init(&m);
    if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"lay\"}", &r); free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"o\",\"declaration_layout\":\"legacy\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"qty:int\"],"
            "\"auto_key\":\"uuid\"}", &r), 0, "create auto_key object");
        free(r);
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "append token after auto_key");
    /* Must pass the startup parser (8 parts); the loader may still refuse the
       object (Task 2), but the DAEMON must start. */
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0,
                  "8-part line passes the startup parser");
    test_env_stop_keep(&env);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* a malformed 8th token after auto_key must REFUSE startup. On base the
   startup parser drops the 8th token and the daemon starts: this is the
   true RED for hunk 1f. */
static int test_layout_token_with_auto_key_malformed_refuses(void) {
    LmRoot m;
    lm_init(&m);
    if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"lay\"}", &r); free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"o\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"qty:int\"],"
            "\"auto_key\":\"uuid\"}", &r), 0, "create auto_key object");
        free(r);
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=legacy"), 0, "append malformed 8th token");
    ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0,
                "malformed 8th token refuses startup (not silently truncated)");
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int lm_write_fields(const LmRoot *m, const char *body) {
    char path[400];
    snprintf(path, sizeof(path), "%s/lay/o/fields.conf", m->db_root);
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fputs(body, f);
    return fclose(f);
}

static char *lm_describe(int port) {
    TestClient *tc = lm_connect(port);
    if (!tc) return NULL;
    char *r = NULL;
    if (tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"o\"}", &r) != 0) r = NULL;
    tc_close(tc);
    return r;
}

/* matching manifest + declarations => bound packed layout */
static int test_loader_binds_packed(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1; lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build(&m, &env), 0, "build");
    ASSERT_EQ_INT(lm_write_fields(&m, "name:varchar\nqty:int\namt:numeric:12,2\n"), 0, "fields");
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "token");
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
    char *r = lm_describe(m.port);
    ASSERT_NOT_NULL(r, "describe");
    ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"packed\""), "packed bound");
    ASSERT_TRUE(r && strstr(r, "\"bare\":true"), "bare varchar preserved");
    free(r);
    test_env_stop_keep(&env); lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* packed manifest + legacy declarations => load refused, never legacy (F14) */
static int test_loader_packed_with_legacy_fields_refused(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1; lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build(&m, &env), 0, "build");
    ASSERT_EQ_INT(lm_write_fields(&m, "name:varchar:20\nqty:int\nbroken:nosuchtype\n"), 0, "fields");
    ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "token");
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "daemon starts");
    char *r = lm_describe(m.port);
    ASSERT_TRUE(r == NULL || strstr(r, "\"error\"") != NULL, "object refused, not served");
    ASSERT_TRUE(r == NULL || strstr(r, "\"declaration_layout\":\"legacy\"") == NULL,
                "never legacy fallback");
    free(r);
    test_env_stop_keep(&env); lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* the same object name in two tenants stays independent */
static int test_loader_same_object_two_tenants(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1; lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"ta\"}", &r); free(r); r = NULL;
        tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"tb\"}", &r); free(r); r = NULL;
        const char *mk =
            "{\"mode\":\"create-object\",\"dir\":\"%s\",\"object\":\"o\",\"declaration_layout\":\"legacy\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"qty:int\"]}";
        char req[340];
        snprintf(req, sizeof(req), mk, "ta"); tc_request(tc, req, &r); free(r); r = NULL;
        snprintf(req, sizeof(req), mk, "tb"); tc_request(tc, req, &r); free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    /* mark only ta:o packed */
    char path[400], tmp[420], line[1024];
    snprintf(path, sizeof(path), "%s/schema.conf", m.db_root);
    snprintf(tmp, sizeof(tmp), "%s.lm", path);
    FILE *in = fopen(path, "r"), *out = fopen(tmp, "w");
    ASSERT_TRUE(in && out, "open schema.conf");
    if (in && out) {
        while (fgets(line, sizeof(line), in)) {
            line[strcspn(line, "\r\n")] = '\0';
            fprintf(out, "%s%s\n", line, strncmp(line, "ta:o:", 5) == 0 ? ":layout=packed" : "");
        }
    }
    if (in) fclose(in);
    if (out) fclose(out);
    ASSERT_EQ_INT(rename(tmp, path), 0, "publish schema.conf");
    snprintf(path, sizeof(path), "%s/ta/o/fields.conf", m.db_root);
    FILE *f = fopen(path, "w"); ASSERT_NOT_NULL(f, "ta fields");
    if (f) { fputs("qty:int\n", f); fclose(f); }
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "restart");
    tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 2");
    if (tc) {
        char *ra = NULL, *rb = NULL;
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"ta\",\"object\":\"o\"}", &ra);
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"tb\",\"object\":\"o\"}", &rb);
        ASSERT_TRUE(ra && strstr(ra, "\"declaration_layout\":\"packed\""), "ta packed");
        ASSERT_TRUE(rb && strstr(rb, "\"declaration_layout\":\"legacy\""), "tb legacy");
        free(ra); free(rb);
        tc_close(tc);
    }
    test_env_stop_keep(&env); lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Task 2d edge cases: empty fields.conf under packed refuses; :removed keeps
   its slot; enum neighbours numeric; >MAX_FIELDS lines cut (unchanged). */
static int test_loader_packed_edge_cases(void) {
    /* (1) packed object with zero fields.conf lines => loader refuses the
       object: no fields are served and writes are refused (object inert). */
    {
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1; lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build(&m, &env), 0, "build");
        ASSERT_EQ_INT(lm_write_fields(&m, ""), 0, "empty fields");
        ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "token");
        ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
        char *r = lm_describe(m.port);
        ASSERT_TRUE(r == NULL || strstr(r, "\"fields\":[]") != NULL,
                    "empty packed object serves no fields");
        free(r);
        TestClient *tc = lm_connect(m.port);
        ASSERT_NOT_NULL(tc, "connect");
        if (tc) {
            char *ir = NULL;
            ASSERT_EQ_INT(tc_request(tc,
                "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"o\","
                "\"key\":\"k1\",\"value\":\"x\"}", &ir), 0, "insert probe");
            ASSERT_TRUE(ir && strstr(ir, "\"error\""),
                        "empty packed object refuses writes");
            free(ir);
            tc_close(tc);
        }
        test_env_stop_keep(&env); lm_destroy(&m);
    }
    /* (2) :removed field keeps its slot; enum neighbours numeric */
    {
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1; lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build(&m, &env), 0, "build");
        ASSERT_EQ_INT(lm_write_fields(&m,
            "a:int\ngone:varchar:5:removed\ne:enum(red,green)\nn:numeric:10,2\n"),
            0, "fields");
        ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "token");
        ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
        char *r = lm_describe(m.port);
        ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"packed\""),
                    "removed + enum + numeric binds packed");
        ASSERT_TRUE(r && strstr(r, "\"type\":\"enum\"") &&
                    strstr(r, "\"spec\":\"e:enum(red,green)\""),
                    "enum field retained; removed slot keeps the bind whole");
        free(r);
        test_env_stop_keep(&env); lm_destroy(&m);
    }
    /* (3) fields.conf longer than MAX_FIELDS is cut by the existing break
       (unchanged): the packed loader accepts the 256-field cut without
       refusing, and describe's pre-existing line cross-check (layout-
       independent) refuses to render the extra lines, exactly as legacy. */
    {
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1; lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build(&m, &env), 0, "build");
        char body[4096];
        size_t o = 0;
        for (int i = 0; i < 300 && o + 16 < sizeof(body); i++)
            o = tappendf(body, sizeof(body), o, "f%d:int\n", i);
        ASSERT_EQ_INT(lm_write_fields(&m, body), 0, "300 fields");
        ASSERT_EQ_INT(lm_schema_append(&m, ":layout=packed"), 0, "token");
        ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
        char *r = lm_describe(m.port);
        /* Pre-existing schema_wire cross-check refuses >MAX_FIELDS lines
           (EILSEQ at index >= ts->nfields); same on the legacy path. */
        ASSERT_TRUE(r && strstr(r, "\"error\":\"invalid or unreadable schema declaration\""),
                    "over-MAX_FIELDS cut unchanged (describe cross-check refuses)");
        free(r);
        test_env_stop_keep(&env); lm_destroy(&m);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* declarations WITHOUT the manifest token never silently become packed:
   a bare `varchar` line in a token-less legacy object keeps exactly the
   current legacy behaviour (pinned; see evidence file). */
static int test_layout_startup_agreement(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1; lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build(&m, &env), 0, "build");
    ASSERT_EQ_INT(lm_write_fields(&m, "name:varchar\nqty:int\namt:numeric:12,2\n"), 0, "fields");
    /* no schema token appended */
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start");
    char *r = lm_describe(m.port);
    /* Current behaviour of a bare `varchar` line in a token-less legacy
       object (characterized on base, see evidence file): the legacy loader
       silently drops the field, and describe's schema_wire line cross-check
       refuses the declaration mismatch. It is never served as packed. */
    ASSERT_TRUE(r && strstr(r, "\"error\":\"invalid or unreadable schema declaration\""),
                "bare varchar + no token: cross-check refuses (current behaviour)");
    ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"packed\"") == NULL,
                "never packed without the token");
    free(r);
    test_env_stop_keep(&env); lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* a failed fsync during the schema.conf rewrite must surface as failure and
   leave the original schema.conf intact (old-or-new, never torn) */
static int test_schema_conf_rewrite_fsync_failure(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build(&m, &env), 0, "build");
    char path[400];
    snprintf(path, sizeof(path), "%s/schema.conf", m.db_root);
    char *before = tu_read_file(path);
    ASSERT_NOT_NULL(before, "read schema.conf before");
    /* fail the 1st durability_fsync call made inside the schema.conf rewrite */
    lm_env(&m, "export SCHEMA_CONF_TEST_FSYNC_FAIL_CALL=1\n");
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start with knob");
    TestClient *tc = lm_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        /* documented reshard request (schema-mutations.md: vacuum + splits=N);
           splits 8 -> 16 reaches update_schema_conf_splits_streams */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"vacuum\",\"dir\":\"lay\",\"object\":\"o\",\"splits\":16}", &r), 0, "vacuum");
        ASSERT_TRUE(r && strstr(r, "\"error\""), "failed fsync surfaces as an error");
        free(r);
        tc_close(tc);
    }
    test_env_stop_keep(&env);
    char *after = tu_read_file(path);
    ASSERT_NOT_NULL(after, "read schema.conf after");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0,
                "schema.conf unchanged after a failed rewrite");
    free(before); free(after);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

static TestClient *lm_start_client(LmRoot *m, TestEnv *env) {
    if (test_env_start_at(env, m->db_root, m->port) != 0) return NULL;
    return lm_connect(m->port);
}

static void lm_add_dir(TestClient *tc) {
    char *r = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"lay\"}", &r);
    free(r);
}

/* explicit packed create: bare varchar + exact numeric + int; round trip */
static int test_create_object_packed_explicit(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start");
    if (!tc) { lm_destroy(&m); return 1; }
    lm_add_dir(tc);
    char *r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"p\","
        "\"splits\":8,\"max_key\":16,\"declaration_layout\":\"packed\","
        "\"fields\":[\"name:varchar\",\"amt:numeric:12,3\",\"qty:int\"]}", &r),
        0, "create");
    ASSERT_TRUE(r && strstr(r, "\"status\":\"created\""), "created packed");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\","
        "\"value\":{\"name\":\"alice\",\"amt\":\"12.345\",\"qty\":7}}", &r),
        0, "insert");
    ASSERT_TRUE(r && strstr(r, "\"status\":\"inserted\""), "inserted");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r),
        0, "get");
    ASSERT_TRUE(r && strstr(r, "\"name\":\"alice\"") && strstr(r, "12.345") &&
                strstr(r, "\"qty\":7"), "round trip");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"p\"}", &r),
        0, "describe");
    ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"packed\""), "describe packed");
    free(r);
    tc_close(tc);
    test_env_stop_keep(&env);
    char path[400];
    snprintf(path, sizeof(path), "%s/schema.conf", m.db_root);
    char *conf = tu_read_file(path);
    ASSERT_TRUE(conf && strstr(conf, "lay:p:8:16:2:") && strstr(conf, ":layout=packed\n"),
                "schema.conf carries the layout token");
    free(conf);
    /* restart: the token is authoritative, object still packed */
    tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "restart");
    if (tc) {
        r = NULL;
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r);
        ASSERT_TRUE(r && strstr(r, "12.345"), "persisted after restart");
        free(r);
        tc_close(tc);
        test_env_stop_keep(&env);
    }
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* unknown layout strings and invalid packed declarations are refused with no residue */
static int test_create_object_packed_refusals(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start");
    if (!tc) { lm_destroy(&m); return 1; }
    lm_add_dir(tc);
    static const char *bad[] = {
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"b1\",\"declaration_layout\":\"wide\",\"fields\":[\"a:int\"]}",
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"b2\",\"declaration_layout\":\"packed\",\"fields\":[\"a:numeric:0\"]}",
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"b3\",\"declaration_layout\":\"packed\",\"fields\":[\"a:varchar:0\"]}",
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"b4\",\"declaration_layout\":\"packed\",\"fields\":[\"a:varchar:4194305\"]}",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc, bad[i], &r), 0, "request");
        ASSERT_TRUE(r && strstr(r, "\"error\""), "refused");
        free(r);
        char probe[400];
        snprintf(probe, sizeof(probe), "%s/lay/b%zu/fields.conf", m.db_root, i + 1);
        ASSERT_TRUE(!tu_file_exists(probe), "no fields.conf residue");
    }
    tc_close(tc);
    test_env_stop_keep(&env);
    char path[400];
    snprintf(path, sizeof(path), "%s/schema.conf", m.db_root);
    char *conf = tu_read_file(path);
    ASSERT_TRUE(!conf || !strstr(conf, "lay:b"), "no schema.conf residue");
    free(conf);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_recompose_ops_supported(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start");
    if (!tc) { lm_destroy(&m); return 1; }
    lm_add_dir(tc);
    char *r = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"p\",\"splits\":8,"
        "\"max_key\":16,\"declaration_layout\":\"packed\","
        "\"fields\":[\"name:varchar:20\",\"qty:int\"]}", &r);
    free(r);
    {
        char *a = NULL, *e = NULL, *c = NULL;
        tc_request(tc, "{\"mode\":\"add-field\",\"dir\":\"lay\",\"object\":\"p\",\"fields\":[\"extra:int\"]}", &a);
        ASSERT_TRUE(a && strstr(a, "\"status\":\"rebuilt\""), "add-field succeeds on a packed object");
        tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"lay\",\"object\":\"p\",\"fields\":[\"name:varchar:30\"]}", &e);
        ASSERT_TRUE(e && strstr(e, "\"status\":\"edited\""), "edit-field succeeds on a packed object");
        tc_request(tc, "{\"mode\":\"vacuum\",\"dir\":\"lay\",\"object\":\"p\",\"compact\":true}", &c);
        ASSERT_TRUE(c && strstr(c, "\"status\":\"rebuilt\""), "compact succeeds on a packed object");
        free(a); free(e); free(c);
    }
    /* supported: splits change on a packed object copies verbatim */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\","
        "\"value\":{\"name\":\"a\",\"qty\":1}}", &r);
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"lay\",\"object\":\"p\",\"splits\":16}", &r),
        0, "vacuum splits");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "splits change allowed");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r), 0, "get");
    ASSERT_TRUE(r && strstr(r, "\"name\":\"a\"") && strstr(r, "\"qty\":1"),
                "record intact after verbatim reshard");
    free(r);
    tc_close(tc);
    test_env_stop_keep(&env);
    char path[400];
    snprintf(path, sizeof(path), "%s/schema.conf", m.db_root);
    char *conf = tu_read_file(path);
    ASSERT_TRUE(conf && strstr(conf, "lay:p:16:") && strstr(conf, ":layout=packed"),
                "reshard preserved the layout token");
    free(conf);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-packed-recompose-ops-supported", test_packed_recompose_ops_supported)
static int lm_write_file(const char *path, const char *text) {
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    int rc = fputs(text, f) < 0 ? -1 : 0;
    if (fclose(f) != 0) rc = -1;
    return rc;
}

/* Make the root look like a clean 2026.10.2 root the new binary has not
   migrated yet: previous version stamp, no layout stamp. */
static void lm_make_pre_migration(const LmRoot *m) {
    char p[420];
    snprintf(p, sizeof(p), "%s/.version", m->db_root);
    ASSERT_EQ_INT(lm_write_file(p, "2026.10.2\n"), 0, "rewrite .version");
    snprintf(p, sizeof(p), "%s/.layout-migrated", m->db_root);
    unlink(p);
}

static char *lm_read(const LmRoot *m, const char *rel) {
    char p[420];
    snprintf(p, sizeof(p), "%s/%s", m->db_root, rel);
    return tu_read_file(p);
}

/* Builds lay/o as a LEGACY object (TEST_BUILD seam, D10) with 3 records and
   an index on qty. numeric fields: legacy2 uses the legacy `numeric:N`
   (scale 2) grammar, amt has an explicit P,S. `bad_amt` stores a value that
   cannot fit amt:numeric:4,2 (D5 audit refusal fixture). Stops the daemon
   gracefully (clean-shutdown evidence) unless `crash` is set. */
static int lm_build_legacy(LmRoot *m, TestEnv *env, int bad_amt, int crash) {
    TestClient *tc = lm_start_client(m, env);
    if (!tc) return -1;
    lm_add_dir(tc);
    char *r = NULL;
    int ok = tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"o\",\"splits\":8,"
        "\"max_key\":16,\"declaration_layout\":\"legacy\","
        "\"fields\":[\"name:varchar:20\",\"qty:int\",\"legacy2:numeric:10\","
        "\"amt:numeric:4,2\",\"note:varchar:30\"],\"indexes\":[\"qty\"]}", &r) == 0 &&
        r && strstr(r, "\"status\":\"created\"");
    free(r);
    static const char *rows[] = {
        "{\"name\":\"alice\",\"qty\":7,\"legacy2\":\"12.34\",\"amt\":\"1.25\",\"note\":\"n1\"}",
        "{\"name\":\"bob\",\"qty\":8,\"legacy2\":\"-0.05\",\"amt\":\"99.99\",\"note\":\"n2\"}",
        "{\"name\":\"carol\",\"qty\":7,\"legacy2\":\"0\",\"amt\":\"0\",\"note\":\"n3\"}",
    };
    for (int i = 0; ok && i < 3; i++) {
        char req[512];
        const char *row = rows[i];
        char bad_row[256];
        if (bad_amt && i == 1) {
            snprintf(bad_row, sizeof(bad_row),
                "{\"name\":\"bob\",\"qty\":8,\"legacy2\":\"-0.05\",\"amt\":\"12345.67\",\"note\":\"n2\"}");
            row = bad_row;
        }
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"o\",\"key\":\"k%d\",\"value\":%s}",
            i + 1, row);
        r = NULL;
        ok = tc_request(tc, req, &r) == 0 && r && strstr(r, "\"status\":\"inserted\"");
        free(r);
    }
    tc_close(tc);
    if (crash) test_env_kill(env); else test_env_stop_keep(env);
    return ok ? 0 : -1;
}

/* Everything the migration must preserve, asserted on a packed object. */
static void lm_assert_migrated(LmRoot *m, TestClient *tc) {
    char *r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"o\"}", &r), 0, "describe");
    ASSERT_TRUE(r && strstr(r, "\"declaration_layout\":\"packed\""), "object is packed");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"o\",\"key\":\"k1\"}", &r), 0, "get k1");
    ASSERT_TRUE(r && strstr(r, "\"name\":\"alice\"") && strstr(r, "\"qty\":7") &&
                strstr(r, "12.34") && strstr(r, "1.25") && strstr(r, "n1"), "k1 values exact");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"o\",\"key\":\"k2\"}", &r), 0, "get k2");
    ASSERT_TRUE(r && strstr(r, "-0.05") && strstr(r, "99.99"), "k2 negative/boundary numeric exact");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc, "{\"mode\":\"count\",\"dir\":\"lay\",\"object\":\"o\"}", &r), 0, "count");
    ASSERT_EQ_INT(tu_parse_count(r), 3, "count preserved");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"lay\",\"object\":\"o\","
        "\"criteria\":[{\"field\":\"qty\",\"op\":\"eq\",\"value\":\"7\"}]}", &r), 0, "find via rebuilt index");
    ASSERT_TRUE(r && strstr(r, "k1") && strstr(r, "k3") && !strstr(r, "k2"), "index rebuilt over packed records");
    free(r);
    char *stamp = lm_read(m, ".layout-migrated");
    ASSERT_NOT_NULL(stamp, "layout stamp written");
    free(stamp);
    char *conf = lm_read(m, "schema.conf");
    ASSERT_TRUE(conf && strstr(conf, "lay:o:8:16:2:") && strstr(conf, ":layout=packed"), "token published");
    free(conf);
    char *fields = lm_read(m, "lay/o/fields.conf");
    ASSERT_TRUE(fields && strstr(fields, "legacy2:numeric:10,2\n") && strstr(fields, "amt:numeric:4,2\n"),
                "legacy numeric:N rewritten to scale 2, explicit P,S kept");
    free(fields);
    char *ver = lm_read(m, ".version");
    ASSERT_TRUE(ver && strncmp(ver, "2026.11.1", 9) == 0, "root stamped with the new version");
    free(ver);
}

static int test_layout_migration_converts_legacy(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build_legacy(&m, &env, 0, 0), 0, "build legacy fixture");
    char *clean = lm_read(&m, ".shard-db.clean");
    ASSERT_NOT_NULL(clean, "graceful stop left clean-shutdown evidence");
    free(clean);
    lm_make_pre_migration(&m);
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start migrates");
    if (tc) {
        lm_assert_migrated(&m, tc);
        tc_close(tc);
        test_env_stop_keep(&env);
        /* second start skips the migration and still serves */
        tc = lm_start_client(&m, &env);
        ASSERT_NOT_NULL(tc, "second start");
        if (tc) { lm_assert_migrated(&m, tc); tc_close(tc); test_env_stop_keep(&env); }
    }
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-layout-migration-converts-legacy", test_layout_migration_converts_legacy)
/* The root must be exactly as the previous version left it. */
static void lm_assert_untouched(LmRoot *m) {
    char *conf = lm_read(m, "schema.conf");
    ASSERT_TRUE(conf && !strstr(conf, "layout=packed"), "schema.conf untouched");
    free(conf);
    char *ver = lm_read(m, ".version");
    ASSERT_TRUE(ver && strcmp(ver, "2026.10.2\n") == 0, "version NOT advanced (refusal precedes the stamp)");
    free(ver);
    char *stamp = lm_read(m, ".layout-migrated");
    ASSERT_TRUE(stamp == NULL, "no completion stamp");
    free(stamp);
    char *fields = lm_read(m, "lay/o/fields.conf");
    ASSERT_TRUE(fields && strstr(fields, "legacy2:numeric:10\n"), "fields.conf untouched");
    free(fields);
}

static int test_layout_migration_refusals(void) {
    /* (a) previous run was killed: no clean-shutdown evidence (D7) */
    {
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
        lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build_legacy(&m, &env, 0, 1), 0, "build (killed)");
        lm_make_pre_migration(&m);
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0, "unclean root refused");
        lm_assert_untouched(&m);
        lm_destroy(&m);
    }
    /* (b) clean stop but a pending commit marker is present (D7) */
    {
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
        lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build_legacy(&m, &env, 0, 0), 0, "build");
        char mk[420];
        snprintf(mk, sizeof(mk), "%s/lay/o/data/kf/000_batch_1_marker.dat", m.db_root);
        /* name verified against marker_tmp_name_valid(): final markers are
           "%x_batch_%u[_%16llx]_marker.dat"; an unrecognised file in the marker
           namespace makes the scan fail closed (-1), which also refuses. */
        ASSERT_EQ_INT(lm_write_file(mk, "x"), 0, "plant marker");
        lm_make_pre_migration(&m);
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0, "pending marker refused");
        ASSERT_TRUE(tu_file_exists(mk), "marker preserved, never replayed or deleted");
        lm_assert_untouched(&m);
        lm_destroy(&m);
    }
    /* (c) a legacy numeric value does not fit its declared precision (D5) */
    {
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
        lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build_legacy(&m, &env, 1, 0), 0, "build (unfit value)");
        lm_make_pre_migration(&m);
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0, "unfit value refused");
        lm_assert_untouched(&m);
        char *rep = lm_read(&m, ".layout-migration-report");
        ASSERT_TRUE(rep && strstr(rep, "amt") && strstr(rep, "k2") && strstr(rep, "12345.67"),
                    "report names the field, the key and the value");
        free(rep);
        lm_destroy(&m);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

static void lm_env_crash(const LmRoot *m, const char *phase) {
    char extra[96];
    snprintf(extra, sizeof(extra), "export LAYOUT_MIGRATE_TEST_CRASH=%s\n", phase);
    lm_env(m, extra);
}

/* kill the daemon at each migration phase; the next start must find every
   object wholly old (before commit) or wholly new (after commit), and a
   clean start must finish the job */
static int test_layout_migration_crash_points(void) {
    static const char *phases[] = {
        "after-recovery", "after-audit", "after-stage", "after-walk",
        "after-publish", "after-reindex", "after-commit",
    };
    for (size_t pi = 0; pi < sizeof(phases) / sizeof(phases[0]); pi++) {
        const char *phase = phases[pi];
        LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
        lm_env(&m, NULL);
        TestEnv env;
        ASSERT_EQ_INT(lm_build_legacy(&m, &env, 0, 0), 0, "build legacy fixture");
        lm_make_pre_migration(&m);

        lm_env_crash(&m, phase);                                     /* run 1: die at <phase> */
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0, "run 1 killed");
        if (strcmp(phase, "after-publish") == 0) {
            char *ver = lm_read(&m, ".version");
            ASSERT_TRUE(ver && strncmp(ver, "2026.11.1", 9) == 0,
                        "F16: the new version is stamped before any packed layout is published");
            free(ver);
        }

        lm_env_crash(&m, "after-recovery");                          /* run 2: recover, then die before converting */
        ASSERT_TRUE(test_env_start_at(&env, m.db_root, m.port) != 0, "run 2 killed after recovery");
        int committed = strcmp(phase, "after-commit") == 0;
        char *conf = lm_read(&m, "schema.conf");
        char *fields = lm_read(&m, "lay/o/fields.conf");
        if (committed) {
            ASSERT_TRUE(conf && strstr(conf, ":layout=packed"), "committed: token stays");
            ASSERT_TRUE(fields && strstr(fields, "legacy2:numeric:10,2\n"), "committed: packed fields.conf");
        } else {
            ASSERT_TRUE(conf && !strstr(conf, "layout=packed"), "rolled back: token restored (F19)");
            ASSERT_TRUE(fields && strstr(fields, "legacy2:numeric:10\n"), "rolled back: legacy fields.conf");
        }
        free(conf); free(fields);

        lm_env(&m, NULL);                                            /* run 3: no knob — finish */
        TestClient *tc = lm_start_client(&m, &env);
        ASSERT_NOT_NULL(tc, "run 3 starts");
        if (tc) {
            lm_assert_migrated(&m, tc);
            tc_close(tc);
            test_env_stop_keep(&env);
        }
        lm_destroy(&m);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-layout-migration-refusals", test_layout_migration_refusals)
TEST_REGISTER("test-layout-migration-crash-points", test_layout_migration_crash_points)
static int test_backup_restore_layout(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "start");
    if (!tc) { lm_destroy(&m); return 1; }
    lm_add_dir(tc);
    char *r = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"p\",\"splits\":8,\"max_key\":16,"
        "\"declaration_layout\":\"packed\",\"fields\":[\"name:varchar:20\",\"amt:numeric:12,3\"]}", &r);
    free(r); r = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\","
        "\"value\":{\"name\":\"a\",\"amt\":\"1.500\"}}", &r);
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"lay\",\"object\":\"p\"}", &r), 0, "backup");
    ASSERT_TRUE(r && strstr(r, "\"status\":\"backed_up\""), "backed up");
    char stamp[64] = {0};
    const char *bp = r ? strstr(r, "/backup/") : NULL;
    if (bp) sscanf(bp + 8, "%63[^\"]", stamp);
    ASSERT_TRUE(stamp[0] != '\0', "backup id parsed");
    free(r); r = NULL;
    char meta[500];
    snprintf(meta, sizeof(meta), "%s/lay/p/backup/%s/object.json", m.db_root, stamp);
    char *mj = tu_read_file(meta);
    ASSERT_TRUE(mj && strstr(mj, "\"layout\":\"packed\""), "backup records the layout");
    free(mj);

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k2\","
        "\"value\":{\"name\":\"b\",\"amt\":\"2.000\"}}", &r);
    free(r); r = NULL;
    char req[340];
    snprintf(req, sizeof(req),
        "{\"mode\":\"restore\",\"dir\":\"lay\",\"object\":\"p\",\"from\":\"%s\",\"force\":true}", stamp);
    ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "restore");
    ASSERT_TRUE(r && strstr(r, "\"status\":\"restored\""), "same-layout restore works");
    free(r); r = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r);
    ASSERT_TRUE(r && strstr(r, "1.500"), "k1 restored exactly");
    free(r); r = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"lay\",\"object\":\"p\"}", &r);
    ASSERT_EQ_INT(tu_parse_count(r), 1, "k2 gone after restore");
    free(r); r = NULL;

    /* a backup that does not say "packed" (taken before this release) is refused even with force */
    ASSERT_EQ_INT(lm_write_file(meta, "{\"splits\":8,\"max_key\":16,\"storage_version\":2,\"streams\":1}\n"), 0, "forge legacy meta");
    ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "restore request");
    ASSERT_TRUE(r && strstr(r, "\"error\"") && strstr(r, "layout"), "legacy-layout backup refused");
    free(r); r = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"p\",\"key\":\"k1\"}", &r);
    ASSERT_TRUE(r && strstr(r, "1.500"), "live object untouched by the refused restore");
    free(r);
    tc_close(tc);
    test_env_stop_keep(&env);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-backup-restore-layout", test_backup_restore_layout)
TEST_REGISTER("test-schema-conf-rewrite-fsync-failure", test_schema_conf_rewrite_fsync_failure)
TEST_REGISTER("test-create-object-packed-explicit", test_create_object_packed_explicit)
TEST_REGISTER("test-create-object-packed-refusals", test_create_object_packed_refusals)
TEST_REGISTER("test-layout-token-packed-loads", test_layout_token_packed_loads)
TEST_REGISTER("test-layout-token-malformed-refuses", test_layout_token_malformed_refuses)
TEST_REGISTER("test-layout-token-with-auto-key-parses", test_layout_token_with_auto_key_parses)
TEST_REGISTER("test-layout-token-with-auto-key-malformed-refuses", test_layout_token_with_auto_key_malformed_refuses)
TEST_REGISTER("test-loader-binds-packed", test_loader_binds_packed)
TEST_REGISTER("test-loader-packed-with-legacy-fields-refused", test_loader_packed_with_legacy_fields_refused)
TEST_REGISTER("test-loader-same-object-two-tenants", test_loader_same_object_two_tenants)
TEST_REGISTER("test-loader-packed-edge-cases", test_loader_packed_edge_cases)
TEST_REGISTER("test-layout-startup-agreement", test_layout_startup_agreement)

/* ---- R6: finance-shaped migration acceptance ---------------------------
   A populated legacy root (decimal, currency, scale-2 legacy numeric, date,
   bool, varchar, three btree indexes + a bitmap) is stopped cleanly, aged to
   2026.10.2, and migrated by the first start of this binary. Every value is
   compared as text against a pre-migration snapshot and every index route is
   compared against its pre-migration answer. */
#define LM_FIN_ROWS 3000
#define LM_FIN_CHUNK 1000

static int lm_cmp_lines(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/* Splits `text` in place into lines, sorts them and rejoins into a new string. */
static char *lm_sorted_lines(const char *text) {
    if (!text) return NULL;
    char *copy = strdup(text);
    size_t cap = 1024, n = 0;
    char **lines = malloc(cap * sizeof(*lines));
    if (!copy || !lines) { free(copy); free(lines); return NULL; }
    for (char *s = strtok(copy, "\n"); s; s = strtok(NULL, "\n")) {
        if (n == cap) {
            cap *= 2;
            char **grown = realloc(lines, cap * sizeof(*lines));
            if (!grown) { free(copy); free(lines); return NULL; }
            lines = grown;
        }
        lines[n++] = s;
    }
    qsort(lines, n, sizeof(*lines), lm_cmp_lines);
    char *out = malloc(strlen(text) + n + 2);
    if (out) {
        out[0] = '\0';
        for (size_t i = 0; i < n; i++) { strcat(out, lines[i]); strcat(out, "\n"); }
    }
    free(copy); free(lines);
    return out;
}

static char *lm_fin_csv(TestClient *tc) {
    char *r = NULL;
    if (tc_request(tc,
        "{\"mode\":\"fetch\",\"dir\":\"lay\",\"object\":\"fin\",\"offset\":0,"
        "\"limit\":100000,\"format\":\"csv\"}", &r) != 0) return NULL;
    char *sorted = lm_sorted_lines(r);
    free(r);
    return sorted;
}

typedef struct { int qty7, amt_range, bool_true, date_range; char sum[64]; } LmFinProbe;

static int lm_fin_count(TestClient *tc, const char *criteria) {
    char req[400];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"lay\",\"object\":\"fin\",\"criteria\":%s}", criteria);
    char *r = NULL;
    int rc = tc_request(tc, req, &r);
    int n = rc == 0 ? tu_parse_count(r) : -1;
    free(r);
    return n;
}

static void lm_fin_probe(TestClient *tc, LmFinProbe *p) {
    p->qty7 = lm_fin_count(tc, "[{\"field\":\"qty\",\"op\":\"eq\",\"value\":\"7\"}]");
    p->amt_range = lm_fin_count(tc,
        "[{\"field\":\"amt\",\"op\":\"between\",\"value\":[\"100.0000\",\"250.5000\"]}]");
    p->bool_true = lm_fin_count(tc, "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]");
    p->date_range = lm_fin_count(tc,
        "[{\"field\":\"d\",\"op\":\"between\",\"value\":[\"20260101\",\"20260630\"]}]");
    p->sum[0] = '\0';
    char *r = NULL;
    if (tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"lay\",\"object\":\"fin\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"amt\",\"alias\":\"s\"}]}", &r) == 0 && r) {
        const char *s = strstr(r, "\"s\":");
        if (s) { s += 4; size_t l = strcspn(s, ",}] \r\n"); if (l < sizeof(p->sum)) { memcpy(p->sum, s, l); p->sum[l] = '\0'; } }
    }
    free(r);
}

static int lm_build_finance(LmRoot *m, TestEnv *env) {
    TestClient *tc = lm_start_client(m, env);
    if (!tc) return -1;
    lm_add_dir(tc);
    char *r = NULL;
    int ok = tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"fin\",\"splits\":8,"
        "\"max_key\":16,\"declaration_layout\":\"legacy\","
        "\"fields\":[\"amt:numeric:18,4\",\"cur:currency\",\"scaled2:numeric:12\","
        "\"note:varchar:40\",\"qty:int\",\"d:date\",\"flag:bool\"],"
        "\"indexes\":[\"qty\",\"amt\",\"d\",\"flag:bitmap\"]}", &r) == 0 &&
        r && strstr(r, "\"status\":\"created\"");
    free(r);
    r = NULL;
    ok = ok && tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"zs\",\"splits\":8,"
        "\"max_key\":16,\"declaration_layout\":\"legacy\",\"fields\":[\"v:numeric:12\"]}", &r) == 0 &&
        r && strstr(r, "\"status\":\"created\"");
    free(r);
    r = NULL;
    ok = ok && tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"lay\",\"object\":\"zs\",\"key\":\"w\","
        "\"value\":{\"v\":\"100\"}}", &r) == 0 && r && strstr(r, "\"status\":\"inserted\"");
    free(r);
    for (int chunk = 0; ok && chunk < LM_FIN_ROWS / LM_FIN_CHUNK; chunk++) {
        size_t cap = (size_t)LM_FIN_CHUNK * 220 + 256, len;
        char *bulk = malloc(cap);
        if (!bulk) { ok = 0; break; }
        len = (size_t)snprintf(bulk, cap,
            "{\"mode\":\"bulk-insert\",\"dir\":\"lay\",\"object\":\"fin\",\"records\":[");
        for (int j = 0; j < LM_FIN_CHUNK; j++) {
            int i = chunk * LM_FIN_CHUNK + j + 1;
            long long whole = (i % 11 == 0) ? -(long long)i : i;
            len = tappendf(bulk, cap, len,
                "%s{\"key\":\"f%05d\",\"value\":{\"amt\":\"%s%lld.%04d\",\"cur\":\"%d.%04d\","
                "\"scaled2\":\"%d.%02d\",\"note\":\"memo %d\",\"qty\":%d,"
                "\"d\":\"2026%02d%02d\",\"flag\":%s}}",
                j ? "," : "", i, whole < 0 ? "-" : "", whole < 0 ? -whole : whole,
                (i * 37) % 10000, i, (i * 91) % 10000, i, 1 + (i * 13) % 99, i, i % 13,
                1 + i % 12, 1 + i % 28, i % 3 == 0 ? "true" : "false");
        }
        snprintf(bulk + len, cap - len, "]}");
        r = NULL;
        ok = tc_request(tc, bulk, &r) == 0 && r && !strstr(r, "\"error\"");
        free(r);
        free(bulk);
    }
    tc_close(tc);
    test_env_stop_keep(env);
    return ok ? 0 : -1;
}

static int test_layout_migration_finance_acceptance(void) {
    LmRoot m; lm_init(&m); if (t_ctx->failed) return 1;
    lm_env(&m, NULL);
    TestEnv env;
    ASSERT_EQ_INT(lm_build_finance(&m, &env), 0, "build legacy finance root");

    /* snapshot while still legacy: restart on the old layout, read, stop cleanly */
    TestClient *tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "legacy restart for the snapshot");
    char *before = NULL; LmFinProbe pre = {0};
    if (tc) {
        char *r = NULL;
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"fin\"}", &r);
        ASSERT_CONTAINS(r, "\"declaration_layout\":\"legacy\"", "fixture is legacy before migration");
        free(r);
        before = lm_fin_csv(tc);
        lm_fin_probe(tc, &pre);
        r = NULL;
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"zs\",\"key\":\"w\"}", &r);
        ASSERT_CONTAINS(r, "\"v\":100}", "legacy renders a whole scale-2 value without decimals");
        free(r);
        tc_close(tc);
        test_env_stop_keep(&env);
    }
    ASSERT_TRUE(before && strlen(before) > (size_t)LM_FIN_ROWS * 20, "snapshot captured");
    ASSERT_TRUE(pre.qty7 > 0 && pre.amt_range > 0 && pre.bool_true == LM_FIN_ROWS / 3 &&
                pre.date_range > 0 && pre.sum[0], "pre-migration probes are non-trivial");

    lm_make_pre_migration(&m);
    tc = lm_start_client(&m, &env);
    ASSERT_NOT_NULL(tc, "first start migrates");
    if (tc) {
        char *r = NULL;
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"lay\",\"object\":\"fin\"}", &r);
        ASSERT_CONTAINS(r, "\"declaration_layout\":\"packed\"", "object is packed after migration");
        free(r);
        r = NULL;
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lay\",\"object\":\"zs\",\"key\":\"w\"}", &r);
        ASSERT_CONTAINS(r, "\"v\":100.00}",
                        "documented display change: migrated numeric:N renders its declared scale (100 -> 100.00)");
        free(r);
        char *after = lm_fin_csv(tc);
        ASSERT_TRUE(before && after && strcmp(before, after) == 0,
                    "every row is textually identical to the pre-migration snapshot");
        free(after);
        LmFinProbe post = {0};
        lm_fin_probe(tc, &post);
        ASSERT_EQ_INT(post.qty7, pre.qty7, "btree eq answer unchanged");
        ASSERT_EQ_INT(post.amt_range, pre.amt_range, "decimal range answer unchanged");
        ASSERT_EQ_INT(post.bool_true, pre.bool_true, "bitmap answer unchanged");
        ASSERT_EQ_INT(post.date_range, pre.date_range, "date range answer unchanged");
        ASSERT_EQ_STR(post.sum, pre.sum, "exact decimal sum unchanged");
        r = NULL;
        tc_request(tc, "{\"mode\":\"count\",\"dir\":\"lay\",\"object\":\"fin\"}", &r);
        ASSERT_EQ_INT(tu_parse_count(r), LM_FIN_ROWS, "record count unchanged");
        free(r);
        tc_close(tc);
        test_env_stop_keep(&env);
    }
    free(before);
    lm_destroy(&m);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-layout-migration-finance-acceptance", test_layout_migration_finance_acceptance)
