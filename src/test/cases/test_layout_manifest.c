#include "test_runner.h"
#include "version.h"
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

/* Rewrites the single schema.conf line for lay:o, replacing its trailing
   `:layout=packed` token with `tail` (e.g. a malformed token). lm_build now
   creates the object packed, so the token is already present. */
static int lm_schema_replace_layout(const LmRoot *m, const char *tail) {
    char path[400], tmp[420];
    snprintf(path, sizeof(path), "%s/schema.conf", m->db_root);
    snprintf(tmp, sizeof(tmp), "%s.lm", path);
    FILE *in = fopen(path, "r");
    FILE *out = fopen(tmp, "w");
    if (!in || !out) { if (in) fclose(in); if (out) fclose(out); return -1; }
    char line[1024];
    int seen = 0;
    while (fgets(line, sizeof(line), in)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (strncmp(line, "lay:o:", 6) == 0) {
            const char *tok = ":layout=packed";
            size_t n = strlen(line), t = strlen(tok);
            if (n >= t && strcmp(line + n - t, tok) == 0) line[n - t] = '\0';
            fprintf(out, "%s%s\n", line, tail);
            seen = 1;
        } else {
            fprintf(out, "%s\n", line);
        }
    }
    fclose(in);
    if (fclose(out) != 0) return -1;
    if (!seen) return -1;
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
        "\"declaration_layout\":\"packed\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:20\",\"qty:int\"]}",
        &r) == 0 && r && !strstr(r, "\"error\"");
    free(r);
    tc_close(tc);
    test_env_stop_keep(env);
    return ok ? 0 : -1;
}

/* packed token accepted by the strict reader; object stays servable */
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
        ASSERT_EQ_INT(lm_schema_replace_layout(&m, bad[i]), 0, "replace with bad token");
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
            "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"o\",\"declaration_layout\":\"packed\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"qty:int\"],"
            "\"auto_key\":\"uuid\"}", &r), 0, "create auto_key object");
        free(r);
        tc_close(tc);
    }
    test_env_stop_keep(&env);
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
    ASSERT_EQ_INT(lm_schema_replace_layout(&m, ":layout=legacy"), 0, "replace with malformed 8th token");
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
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    if (test_env_start_at(env, m->db_root, m->port) != 0) {
        /* Intermittent sanitizer-leg failure (2026-11-15, "first start
           migrates"): record how long the start waited and what the daemon
           logged, to tell a readiness timeout from a startup refusal. */
        clock_gettime(CLOCK_MONOTONIC, &t1);
        TAP_DIAG("# lm_start_client: start failed after %ld ms\n",
                 (long)((t1.tv_sec - t0.tv_sec) * 1000L +
                        (t1.tv_nsec - t0.tv_nsec) / 1000000L));
        tu_run_cmd("tail -n 30 \"$(dirname '%s')\"/daemon*.log 2>&1; "
                   "tail -q -n 30 \"$(dirname '%s')\"/logs/*.log 2>&1",
                   m->db_root, m->db_root);
        return NULL;
    }
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
        /* T3: the legacy layout no longer exists, so "legacy" is refused in
           TEST_BUILD too (it used to build the migration fixtures). */
        "{\"mode\":\"create-object\",\"dir\":\"lay\",\"object\":\"b5\",\"declaration_layout\":\"legacy\",\"fields\":[\"a:int\"]}",
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
    ASSERT_TRUE(r && strstr(r, "\"error\"") && strstr(r, "restore with shard-db 2026.11.2 first"),
                "legacy-layout backup refused with the upgrade hint");
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
TEST_REGISTER("test-layout-token-malformed-refuses", test_layout_token_malformed_refuses)
TEST_REGISTER("test-layout-token-with-auto-key-parses", test_layout_token_with_auto_key_parses)
TEST_REGISTER("test-layout-token-with-auto-key-malformed-refuses", test_layout_token_with_auto_key_malformed_refuses)
TEST_REGISTER("test-loader-binds-packed", test_loader_binds_packed)
TEST_REGISTER("test-loader-packed-with-legacy-fields-refused", test_loader_packed_with_legacy_fields_refused)
TEST_REGISTER("test-loader-packed-edge-cases", test_loader_packed_edge_cases)

