#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/bitmap.h"
#include "../db/shard_db.h"
#include "../db/slotcask.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>
#include <sys/stat.h>

static const char *rsc_titles[] = {
    "Honda announces new EV platform",
    "Honda wins Le Mans prototype class",
    "Toyota beats Honda in sales",
    "The best motorcycles of 2026",
    "Honda recall affects 200k vehicles",
    "Apple releases new MacBook",
    "OpenAI launches GPT-5",
    "PostgreSQL 17 performance review",
    "Rust 2.0 stabilized",
    "Linux kernel 7.0 released",
    "Microsoft acquires startup",
    "Google search algorithm update",
    "Amazon AWS outage post-mortem",
    "Meta VR headset review",
    "SpaceX starship test flight",
};

static int test_reindex_spill_collision_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"articles\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"title:varchar:200\"],"
        "\"indexes\":[\"title\",\"title:trigram\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object with both indexes");
    free(resp); resp = NULL;

    for (int i = 0; i < 15; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"articles\","
            "\"key\":\"k%02d\",\"value\":{\"title\":\"%s\"}}",
            i, rsc_titles[i]);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"honda\"}]}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 4, "pre-reindex trigram icontains honda");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"starts\",\"value\":\"Honda\"}]}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 3, "pre-reindex btree starts Honda");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"reindex\",\"dir\":\"d\",\"object\":\"articles\"}",
        &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"honda\"}]}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 4, "post-reindex trigram icontains honda");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"starts\",\"value\":\"Honda\"}]}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 3, "post-reindex btree starts Honda");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"honda\"}],"
        "\"limit\":10}",
        &resp);
    ASSERT_NOT_NULL(resp, "post-reindex find response");
    ASSERT_EQ_INT(resp[0] == '[', 1, "post-reindex find returns array");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

/* ==================== fail-closed regression (Task 2) ====================
   These run against the process-local embedded ShardDb so the TEST_BUILD
   index-spill errno injection seam is live — the daemon
   binary spawned above is built without TEST_BUILD. Each scenario seeds
   the same object the wire test uses, snapshots every file under
   indexes/ (path|size|hex-content, sorted — deterministic, no external
   checksum tool), triggers the failure, and requires the snapshot to be
   byte-identical afterwards. */

static int rsc_pdb_setup(ShardDb *db) {
    char *resp = NULL;
    tu_pdb_drop_object(db, "d", "articles");
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"articles\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"title:varchar:200\"],"
        "\"indexes\":[\"title\",\"title:trigram\"]}",
        &resp);
    free(resp); resp = NULL;
    for (int i = 0; i < 15; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"articles\","
            "\"key\":\"k%02d\",\"value\":{\"title\":\"%s\"}}",
            i, rsc_titles[i]);
        tu_pdb_request(db, req, &resp);
        free(resp); resp = NULL;
    }
    return 0;
}

static const char *RSC_ADD_IDX_FORCE =
    "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"articles\","
    "\"fields\":[\"title\",\"title:trigram\"],\"force\":true}";

typedef struct { char *line; } RscLine;

static int rsc_line_cmp(const void *a, const void *b) {
    return strcmp(((const RscLine *)a)->line, ((const RscLine *)b)->line);
}

/* Descriptor-first recursive walk of <base>/<rel>, collecting
   "path|size|hexcontent" lines for every regular file. Opening before
   inspecting the type ensures the bytes come from the same inode that was
   classified, even if a test fixture is being changed concurrently. Returns
   0 on success (including empty). */
static int rsc_snapshot_walk(const char *base, const char *rel,
                             RscLine **lines, int *n, int *cap) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s%s", base, rel);
    int fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return -1;
    }

    if (S_ISDIR(st.st_mode)) {
        DIR *d = fdopendir(fd);
        if (!d) {
            close(fd);
            return -1;
        }
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            char sub[PATH_MAX];
            snprintf(sub, sizeof(sub), "%s/%s", rel, e->d_name);
            (void)rsc_snapshot_walk(base, sub, lines, n, cap);
        }
        closedir(d);
        return 0;
    }

    if (!S_ISREG(st.st_mode)) {
        close(fd);
        return 0;
    }

    FILE *f = fdopen(fd, "rb");
    if (!f) {
        close(fd);
        return -1;
    }
    long fsz = 0;
    if (fseek(f, 0, SEEK_END) == 0 && (fsz = ftell(f)) >= 0 &&
        fseek(f, 0, SEEK_SET) == 0) {
        size_t sz = (size_t)fsz;
        char *hex = malloc(sz * 2 + 1);
        char *content = malloc(sz ? sz : 1);
        if (!hex || !content) {
            free(hex);
            free(content);
            fclose(f);
            return -1;
        }
        size_t got = fread(content, 1, sz, f);
        for (size_t i = 0; i < got; i++)
            snprintf(hex + i * 2, 3, "%02x", (unsigned char)content[i]);
        hex[got * 2] = '\0';
        char *line = malloc(strlen(rel) + got * 2 + 40);
        if (line) {
            sprintf(line, "%s|%zu|%s", rel, got, hex);
            if (*n >= *cap) {
                *cap = *cap ? *cap * 2 : 64;
                RscLine *t = realloc(*lines,
                                     (size_t)*cap * sizeof(RscLine));
                if (t) *lines = t;
            }
            if (*n < *cap)
                (*lines)[(*n)++].line = line;
            else
                free(line);
        }
        free(hex);
        free(content);
    }
    fclose(f);
    return 0;
}

/* Deterministic content oracle for the object's indexes/ tree. */
static char *rsc_indexes_snapshot_for_object(const char *db_root, const char *object) {
    char base[PATH_MAX];
    snprintf(base, sizeof(base), "%s/d/%s", db_root, object);
    RscLine *lines = NULL;
    int n = 0, cap = 0;
    if (rsc_snapshot_walk(base, "/indexes", &lines, &n, &cap) != 0 && n == 0) {
        free(lines);
        return strdup("");
    }
    qsort(lines, (size_t)n, sizeof(RscLine), rsc_line_cmp);
    size_t total = 1;
    for (int i = 0; i < n; i++) total += strlen(lines[i].line) + 1;
    char *out = malloc(total);
    out[0] = '\0';
    for (int i = 0; i < n; i++) {
        strcat(out, lines[i].line);
        strcat(out, "\n");
        free(lines[i].line);
    }
    free(lines);
    return out;
}

static char *rsc_indexes_snapshot(const char *db_root) {
    return rsc_indexes_snapshot_for_object(db_root, "articles");
}

static int rsc_spill_dir_count(const char *db_root) {
    char base[PATH_MAX];
    snprintf(base, sizeof(base), "%s/d/articles/indexes", db_root);
    DIR *d = opendir(base);
    if (!d) return -1;
    struct dirent *e;
    int count = 0;
    while ((e = readdir(d))) {
        if (strncmp(e->d_name, ".spill_", 7) == 0) count++;
    }
    closedir(d);
    return count;
}

/* 1. Non-ENOENT spill-open failure. With the seam armed at EACCES every
   merge open fails, so every shard merge aborts before publishing — the
   request must fail and every pre-existing target file must remain
   byte-identical. (The seam stays armed until reset rather than firing
   once: a one-shot failure would let sibling shards publish first and
   make this assertion racy.) */
static int run_spill_open_failure(ShardDb *db, const char *db_root) {
    rsc_pdb_setup(db);
    char *before = rsc_indexes_snapshot(db_root);
    ASSERT_TRUE(before && before[0],
        "pre-existing index files present before injected failure");

    index_test_spill_open_fail_errno(EACCES);
    char *resp = NULL;
    tu_pdb_request(db, RSC_ADD_IDX_FORCE, &resp);
    index_test_spill_open_fail_errno(0);

    ASSERT_NOT_NULL(resp, "add-indexes response received");
    ASSERT_TRUE(resp && strstr(resp, "\"error\"") != NULL,
        "non-ENOENT spill-open failure rejects the request");
    if (resp) TAP_DIAG("# response: %s\n", resp);
    free(resp);

    char *after = rsc_indexes_snapshot(db_root);
    ASSERT_TRUE(after && before && strcmp(after, before) == 0,
        "all pre-existing target shards remain byte-identical");
    free(after);
    free(before);
    tu_pdb_drop_object(db, "d", "articles");
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_reindex_spill_open_failure_run(void) {
    ShardDb *db = test_get_process_db();
    const char *db_root = test_get_process_db_root();
    if (!db || !db_root) {
        ASSERT_TRUE(0, "process-local db available");
        return 1;
    }
    int rc = run_spill_open_failure(db, db_root);
    index_test_spill_open_fail_errno(0);  /* belt-and-braces disarm */
    return rc;
}

TEST_REGISTER("test-reindex-spill-open-fail", test_reindex_spill_open_failure_run)

/* 2. Missing stream-directory enumeration failure. enumerate_segments
   must fail closed before worker allocation or any publication; the
   pre-existing target files must stay byte-identical and no .spill_*
   directory may be left behind. */
static int run_enumeration_failure(ShardDb *db, const char *db_root) {
    rsc_pdb_setup(db);
    char *before = rsc_indexes_snapshot(db_root);
    ASSERT_TRUE(before && before[0],
        "pre-existing index files present before enumeration failure");

    char stream_dir[PATH_MAX];
    snprintf(stream_dir, sizeof(stream_dir),
             "%s/d/articles/data/streams/000", db_root);
    int nuke_ok = tu_run_cmd("rm -rf %s", stream_dir) == 0;
    ASSERT_TRUE(nuke_ok, "first stream directory removed");

    char *resp = NULL;
    tu_pdb_request(db, RSC_ADD_IDX_FORCE, &resp);
    ASSERT_NOT_NULL(resp, "add-indexes response received");
    ASSERT_TRUE(resp && strstr(resp, "\"error\"") != NULL,
        "inaccessible stream enumeration rejects the request");
    free(resp);

    char *after = rsc_indexes_snapshot(db_root);
    ASSERT_TRUE(after && before && strcmp(after, before) == 0,
        "targets byte-identical after enumeration failure");
    free(after);
    free(before);
    ASSERT_EQ_INT(rsc_spill_dir_count(db_root), 0,
        "no spill directories left behind");
    tu_pdb_drop_object(db, "d", "articles");
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_reindex_enum_fail_run(void) {
    ShardDb *db = test_get_process_db();
    const char *db_root = test_get_process_db_root();
    if (!db || !db_root) {
        ASSERT_TRUE(0, "process-local db available");
        return 1;
    }
    return run_enumeration_failure(db, db_root);
}

TEST_REGISTER("test-reindex-enum-fail", test_reindex_enum_fail_run)

/* 3. Schema/setup failure through both public add-index command shapes.
   An unreadable fields.conf must be rejected before any build or
   metadata write: index.conf and every target file stay byte-identical.
   The typed-schema cache is invalidated first so the unreadable file is
   actually re-read (and fails) instead of returning the cached copy. */
static int run_add_indexes_setup_failure(ShardDb *db, const char *db_root) {
    rsc_pdb_setup(db);
    char *before = rsc_indexes_snapshot(db_root);
    ASSERT_TRUE(before && before[0],
        "pre-existing index files present before setup failure");

    char fields_conf[PATH_MAX];
    snprintf(fields_conf, sizeof(fields_conf),
             "%s/d/articles/fields.conf", db_root);
    char eff_root[PATH_MAX];
    snprintf(eff_root, sizeof(eff_root), "%s/d", db_root);
    const char *commands[] = {
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"articles\","
        "\"field\":\"title\",\"force\":true}",
        RSC_ADD_IDX_FORCE,
    };
    const char *names[] = { "singular", "plural" };
    for (int command = 0; command < 2; command++) {
        /* The cache is keyed on the effective root. Flush it each time so
           fields.conf is actually reopened while unreadable. */
        invalidate_schema_caches(eff_root, "articles");
        ASSERT_EQ_INT(chmod(fields_conf, 0000), 0,
                      "fields.conf made unreadable");
        char *resp = NULL;
        tu_pdb_request(db, commands[command], &resp);
        ASSERT_EQ_INT(chmod(fields_conf, 0644), 0, "fields.conf restored");
        char assertion[160];
        snprintf(assertion, sizeof(assertion), "%s add-index response received",
                 names[command]);
        ASSERT_NOT_NULL(resp, assertion);
        snprintf(assertion, sizeof(assertion),
                 "%s add-index rejects unreadable schema", names[command]);
        ASSERT_TRUE(resp && strstr(resp, "\"error\"") != NULL, assertion);
        snprintf(assertion, sizeof(assertion),
                 "%s error identifies schema-load failure", names[command]);
        ASSERT_TRUE(resp && strstr(resp, "cannot load object schema") != NULL,
                    assertion);
        free(resp);

        char *after = rsc_indexes_snapshot(db_root);
        snprintf(assertion, sizeof(assertion),
                 "%s setup failure preserves metadata and shards", names[command]);
        ASSERT_TRUE(after && before && strcmp(after, before) == 0, assertion);
        free(after);
    }
    free(before);
    tu_pdb_drop_object(db, "d", "articles");
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_reindex_add_indexes_setup_fail_run(void) {
    ShardDb *db = test_get_process_db();
    const char *db_root = test_get_process_db_root();
    if (!db || !db_root) {
        ASSERT_TRUE(0, "process-local db available");
        return 1;
    }
    return run_add_indexes_setup_failure(db, db_root);
}

TEST_REGISTER("test-reindex-add-indexes-setup-fail",
              test_reindex_add_indexes_setup_fail_run)

/* Task 5: index metadata is a second atomic publication. Exercise both
   command entry points in the process-local database so the one-shot seam is
   visible, and prove pre-rename failures retain the complete old file. */
static int test_index_conf_atomic_publish_run(void) {
    ShardDb *db = test_get_process_db();
    const char *db_root = test_get_process_db_root();
    if (!db || !db_root) { ASSERT_TRUE(0, "process-local db available"); return 1; }
    rsc_pdb_setup(db);
    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/d/articles/indexes/index.conf", db_root);
    char *before = tu_read_file(conf_path);
    ASSERT_NOT_NULL(before, "index.conf exists before metadata injections");

    const char *plural = RSC_ADD_IDX_FORCE;
    const char *singular =
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"articles\","
        "\"field\":\"title\",\"force\":true}";
    const char *commands[] = { singular, plural };
    const char *command_names[] = { "singular", "plural" };
    for (int command = 0; command < 2; command++) {
        for (int stage = 1; stage <= 5; stage++) {
            index_test_conf_publish_fail_stage(stage);
            char *stage_resp = NULL;
            tu_pdb_request(db, commands[command], &stage_resp);
            char assertion[128];
            snprintf(assertion, sizeof(assertion),
                     "%s metadata stage %d fails before rename",
                     command_names[command], stage);
            ASSERT_TRUE(stage_resp && strstr(stage_resp,
                        "index shards published but index metadata update failed") != NULL,
                        assertion);
            free(stage_resp);
            char *stage_after = tu_read_file(conf_path);
            snprintf(assertion, sizeof(assertion),
                     "%s metadata stage %d preserves index.conf",
                     command_names[command], stage);
            ASSERT_TRUE(before && stage_after && strcmp(before, stage_after) == 0,
                        assertion);
            free(stage_after);

            /* Shards publish before metadata. Even though activation fails,
               every replacement shard must remain structurally queryable. */
            char *query_resp = NULL;
            tu_pdb_request(db,
                "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"articles\","
                "\"criteria\":[{\"field\":\"title\",\"op\":\"starts\","
                "\"value\":\"Honda\"}]}", &query_resp);
            snprintf(assertion, sizeof(assertion),
                     "%s metadata stage %d leaves valid btree shards",
                     command_names[command], stage);
            ASSERT_EQ_INT(tu_parse_count(query_resp), 3, assertion);
            free(query_resp);
            if (command == 1) {
                query_resp = NULL;
                tu_pdb_request(db,
                    "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"articles\","
                    "\"criteria\":[{\"field\":\"title\",\"op\":\"icontains\","
                    "\"value\":\"honda\"}]}", &query_resp);
                snprintf(assertion, sizeof(assertion),
                         "plural metadata stage %d leaves valid trigram shards",
                         stage);
                ASSERT_EQ_INT(tu_parse_count(query_resp), 4, assertion);
                free(query_resp);
            }
        }
        index_test_conf_publish_fail_stage(6);
        char *stage_resp = NULL;
        tu_pdb_request(db, commands[command], &stage_resp);
        char assertion[128];
        snprintf(assertion, sizeof(assertion),
                 "%s metadata post-rename durability warning is explicit",
                 command_names[command]);
        ASSERT_TRUE(stage_resp && strstr(stage_resp,
                    "index and metadata published but directory durability is unconfirmed") != NULL,
                    assertion);
        free(stage_resp);
    }
    char *resp = NULL;
    char *after = tu_read_file(conf_path);
    ASSERT_TRUE(after && strstr(after, "title\n") != NULL &&
                strstr(after, "title:trigram\n") != NULL,
        "metadata remains canonical and readable after post-rename warning");
    free(after);

    /* A pre-auto-promotion bare bool entry must be replaced, not retained
       beside the canonical bitmap spelling on a force/retry. */
    tu_pdb_drop_object(db, "d", "legacy_bits");
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"legacy_bits\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"flag:bool\"]}", &resp);
    free(resp); resp = NULL;
    char legacy_conf[PATH_MAX];
    snprintf(legacy_conf, sizeof(legacy_conf),
             "%s/d/legacy_bits/indexes/index.conf", db_root);
    char legacy_dir[PATH_MAX];
    snprintf(legacy_dir, sizeof(legacy_dir), "%s/d/legacy_bits/indexes", db_root);
    mkdirp(legacy_dir);
    FILE *legacy = fopen(legacy_conf, "w");
    ASSERT_NOT_NULL(legacy, "create legacy bare index.conf");
    if (legacy) { fputs("flag\n", legacy); fclose(legacy); }
    tu_pdb_request(db,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"legacy_bits\","
        "\"field\":\"flag\",\"force\":true}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""),
        "legacy bool index rebuild succeeds");
    free(resp); resp = NULL;
    after = tu_read_file(legacy_conf);
    ASSERT_TRUE(after && strcmp(after, "flag:bitmap\n") == 0,
        "legacy bare bool metadata is atomically canonicalized without duplicates");
    free(after);
    tu_pdb_drop_object(db, "d", "legacy_bits");

    /* A retry after shard publication but before metadata publication must
       rebuild/validate the complete index and repair index.conf. Physical
       shard zero alone is not proof that the requested index is complete. */
    tu_pdb_drop_object(db, "d", "retry_bits");
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"retry_bits\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"kind:varchar:16\"]}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"retry_bits\","
        "\"key\":\"b1\",\"value\":{\"kind\":\"alpha\"}}", &resp);
    free(resp); resp = NULL;
    index_test_conf_publish_fail_stage(5);
    tu_pdb_request(db,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"retry_bits\","
        "\"field\":\"kind:bitmap\",\"force\":true}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "index shards published but index metadata update failed"),
        "singular metadata failure occurs after bitmap shard publication");
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"retry_bits\","
        "\"field\":\"kind:bitmap\"}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""),
        "singular bitmap retry succeeds");
    free(resp); resp = NULL;
    char retry_conf[PATH_MAX];
    snprintf(retry_conf, sizeof(retry_conf),
             "%s/d/retry_bits/indexes/index.conf", db_root);
    after = tu_read_file(retry_conf);
    ASSERT_TRUE(after && strcmp(after, "kind:bitmap\n") == 0,
        "singular bitmap retry repairs missing metadata");
    free(after);
    tu_pdb_drop_object(db, "d", "retry_bits");

    /* Simulate a crash after shard zero publication for plural btree and
       trigram indexes: metadata is absent and shard one is missing. The
       public retry must rebuild both complete shard sets before activation. */
    tu_pdb_drop_object(db, "d", "retry_text");
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"retry_text\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"title:varchar:64\"],"
        "\"indexes\":[\"title\",\"title:trigram\"]}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"retry_text\","
        "\"fields\":[\"title\",\"title:trigram\"],\"force\":true}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""),
        "force-build complete plural shard sets before interruption");
    free(resp); resp = NULL;
    char retry_text_conf[PATH_MAX];
    char retry_idx_1[PATH_MAX];
    char retry_tg_1[PATH_MAX];
    snprintf(retry_text_conf, sizeof(retry_text_conf),
             "%s/d/retry_text/indexes/index.conf", db_root);
    snprintf(retry_idx_1, sizeof(retry_idx_1),
             "%s/d/retry_text/indexes/title/001.idx", db_root);
    snprintf(retry_tg_1, sizeof(retry_tg_1),
             "%s/d/retry_text/indexes/title/001.tg", db_root);
    ASSERT_EQ_INT(unlink(retry_text_conf), 0,
        "remove metadata to model interrupted first publication");
    ASSERT_EQ_INT(unlink(retry_idx_1), 0,
        "remove btree sibling to model partial shard publication");
    ASSERT_EQ_INT(unlink(retry_tg_1), 0,
        "remove trigram sibling to model partial shard publication");
    tu_pdb_request(db,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"retry_text\","
        "\"fields\":[\"title\",\"title:trigram\"]}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""),
        "plural retry succeeds after partial shard publication");
    free(resp); resp = NULL;
    ASSERT_EQ_INT(access(retry_idx_1, F_OK), 0,
        "plural retry restores every btree shard");
    ASSERT_EQ_INT(access(retry_tg_1, F_OK), 0,
        "plural retry restores every trigram shard");
    after = tu_read_file(retry_text_conf);
    ASSERT_TRUE(after && strcmp(after, "title\ntitle:trigram\n") == 0,
        "plural retry activates complete shard sets with no duplicate metadata lines");
    free(after);
    tu_pdb_drop_object(db, "d", "retry_text");

    free(before);
    index_test_conf_publish_fail_stage(0);
    tu_pdb_drop_object(db, "d", "articles");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-index-conf-atomic-publish", test_index_conf_atomic_publish_run)

/* Task 4: a temporary bitmap is never publishable until sync, checked close,
   and checked cache discard all succeed. The TEST_BUILD failures happen after
   real teardown, avoiding fake leaked handles while still making the error
   path deterministic. */
static int test_bitmap_temp_teardown_failures_run(void) {
    ShardDb *db = test_get_process_db();
    const char *db_root = test_get_process_db_root();
    if (!db || !db_root) { ASSERT_TRUE(0, "process-local db available"); return 1; }
    tu_pdb_drop_object(db, "d", "bits");
    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"bits\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"flag:bool\"],"
        "\"indexes\":[\"flag:bitmap\"]}", &resp);
    free(resp); resp = NULL;
    for (int i = 0; i < 4; i++) {
        char req[256];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"bits\","
                 "\"key\":\"b%d\",\"value\":{\"flag\":%s}}",
                 i, i & 1 ? "false" : "true");
        tu_pdb_request(db, req, &resp); free(resp); resp = NULL;
    }
    char *before = rsc_indexes_snapshot_for_object(db_root, "bits");
    ASSERT_TRUE(before && before[0], "bitmap targets exist before teardown failure");
    const char *commands[] = {
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"bits\","
        "\"field\":\"flag:bitmap\",\"force\":true}",
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"bits\","
        "\"fields\":[\"flag:bitmap\"],\"force\":true}",
    };
    const char *shapes[] = { "singular", "plural" };
    for (int shape = 0; shape < 2; shape++) {
        char assertion[160];
        bm_test_fail_close_next(1);
        tu_pdb_request(db, commands[shape], &resp);
        snprintf(assertion, sizeof(assertion),
                 "%s temporary bitmap close failure rejects rebuild",
                 shapes[shape]);
        ASSERT_TRUE(resp && strstr(resp, "index build failed") != NULL,
                    assertion);
        free(resp); resp = NULL;
        char *after = rsc_indexes_snapshot_for_object(db_root, "bits");
        snprintf(assertion, sizeof(assertion),
                 "%s close failure leaves targets byte-identical",
                 shapes[shape]);
        ASSERT_TRUE(after && before && strcmp(after, before) == 0, assertion);
        free(after);

        /* Persistent failure survives compatibility-wrapper invalidations
           until the checked singular/resolver discard observes it. */
        bm_test_fail_invalidate_next(-1);
        tu_pdb_request(db, commands[shape], &resp);
        snprintf(assertion, sizeof(assertion),
                 "%s temporary bitmap discard failure rejects rebuild",
                 shapes[shape]);
        ASSERT_TRUE(resp && strstr(resp, "index build failed") != NULL,
                    assertion);
        free(resp); resp = NULL;
        after = rsc_indexes_snapshot_for_object(db_root, "bits");
        snprintf(assertion, sizeof(assertion),
                 "%s discard failure leaves targets byte-identical",
                 shapes[shape]);
        ASSERT_TRUE(after && before && strcmp(after, before) == 0, assertion);
        free(after);
        bm_test_fail_reset();
    }
    free(before);
    bm_test_fail_reset();
    tu_pdb_drop_object(db, "d", "bits");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bitmap-temp-teardown-failures",
              test_bitmap_temp_teardown_failures_run)

/* Task 7: exercise the resolver's bitmap-spill decoder through the public
   reindex command.  The pause is before temporary bitmap writers open, so a
   malformed spill must reject the whole field before any target rename. */
typedef struct {
    ShardDb *db;
    const char *object;
    char *response;
} RscQueryThread;

static void *rsc_reindex_thread(void *arg) {
    RscQueryThread *q = arg;
    char request[256];
    snprintf(request, sizeof(request),
             "{\"mode\":\"reindex\",\"dir\":\"d\",\"object\":\"%s\","
             "\"timeout_ms\":30000}",
             q->object);
    tu_pdb_request(q->db, request, &q->response);
    return NULL;
}

static int test_reindex_malformed_bitmap_spill_run(void) {
    ShardDb *db = test_get_process_db();
    const char *root = test_get_process_db_root();
    if (!db || !root) { ASSERT_TRUE(0, "process-local db available"); return 1; }
    tu_pdb_drop_object(db, "d", "spill_bits");
    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp); free(resp);
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"spill_bits\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"flag:bool\"],"
        "\"indexes\":[\"flag:bitmap\"]}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"spill_bits\","
        "\"key\":\"k\",\"value\":{\"flag\":true}}", &resp);
    free(resp);
    char *before = rsc_indexes_snapshot_for_object(root, "spill_bits");
    ASSERT_TRUE(before && before[0], "bitmap targets exist before malformed spill");

    snprintf(db->durability_test_pause_phase,
             sizeof(db->durability_test_pause_phase), "bm-resolve-before-open");
    db->durability_test_pause_ms = 2000;
    RscQueryThread q = { .db = db, .object = "spill_bits", .response = NULL };
    pthread_t tid;
    ASSERT_EQ_INT(db_thread_create(&tid, rsc_reindex_thread, &q), 0,
                  "start paused bitmap reindex");
    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/d/spill_bits/indexes/flag/.spill_0/.durability-test-bm-resolve-before-open.active",
             root);
    int seen = 0;
    for (int i = 0; i < 500; i++) {
        if (access(marker, F_OK) == 0) { seen = 1; break; }
        usleep(20 * 1000);
    }
    ASSERT_TRUE(seen, "bitmap resolver pause reached");
    if (seen) {
        char spill[PATH_MAX];
        snprintf(spill, sizeof(spill), "%s/d/spill_bits/indexes/flag/.spill_0/bmw0.bin", root);
        int fd = open(spill, O_WRONLY | O_TRUNC);
        ASSERT_TRUE(fd >= 0, "open generated bitmap spill for truncation");
        if (fd >= 0) { (void)write(fd, "x", 1); close(fd); }
    }
    pthread_join(tid, NULL);
    db->durability_test_pause_ms = 0;
    db->durability_test_pause_phase[0] = '\0';
    ASSERT_TRUE(q.response && strstr(q.response, "\"error\"") != NULL,
        "malformed bitmap spill rejects reindex");
    free(q.response);
    char *after = rsc_indexes_snapshot_for_object(root, "spill_bits");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0,
        "malformed bitmap spill leaves every target byte-identical");
    free(after); free(before);
    tu_pdb_drop_object(db, "d", "spill_bits");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-reindex-malformed-bitmap-spill",
              test_reindex_malformed_bitmap_spill_run)

static int test_reindex_malformed_btree_spill_run(void) {
    ShardDb *db = test_get_process_db();
    const char *root = test_get_process_db_root();
    if (!db || !root) { ASSERT_TRUE(0, "process-local db available"); return 1; }
    tu_pdb_drop_object(db, "d", "spill_tree");
    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"spill_tree\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"title:varchar:64\"],"
        "\"indexes\":[\"title\"]}", &resp);
    free(resp); resp = NULL;
    for (int i = 0; i < 64; i++) {
        char request[256];
        snprintf(request, sizeof(request),
                 "{\"mode\":\"insert\",\"dir\":\"d\","
                 "\"object\":\"spill_tree\",\"key\":\"k%02d\","
                 "\"value\":{\"title\":\"atomic tree %02d\"}}", i, i);
        tu_pdb_request(db, request, &resp);
        free(resp); resp = NULL;
    }
    char *before = rsc_indexes_snapshot_for_object(root, "spill_tree");
    ASSERT_TRUE(before && before[0], "btree targets exist before malformed spill");

    snprintf(db->durability_test_pause_phase,
             sizeof(db->durability_test_pause_phase), "idx-spills-before-merge");
    db->durability_test_pause_ms = 2000;
    RscQueryThread q = { .db = db, .object = "spill_tree", .response = NULL };
    pthread_t tid;
    ASSERT_EQ_INT(db_thread_create(&tid, rsc_reindex_thread, &q), 0,
                  "start paused btree reindex");
    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/d/spill_tree/indexes/title/.spill_0/"
             ".durability-test-idx-spills-before-merge.active", root);
    int seen = 0;
    for (int i = 0; i < 500; i++) {
        if (access(marker, F_OK) == 0) { seen = 1; break; }
        usleep(20 * 1000);
    }
    ASSERT_TRUE(seen, "btree spill merge pause reached");
    int reordered = 0;
    if (seen) {
        for (int w = 0; w < 8 && !reordered; w++) {
            for (int s = 0; s < 2 && !reordered; s++) {
                char spill[PATH_MAX];
                snprintf(spill, sizeof(spill),
                         "%s/d/spill_tree/indexes/title/.spill_0/w%d_s%d.bin",
                         root, w, s);
                int fd = open(spill, O_RDWR);
                if (fd < 0) continue;
                uint32_t count = 0, body_len = 0;
                if (pread(fd, &count, 4, 0) == 4 &&
                    pread(fd, &body_len, 4, 4) == 4 && count >= 2) {
                    uint8_t *body = malloc(body_len);
                    if (body && pread(fd, body, body_len, 8) == (ssize_t)body_len) {
                        uint16_t first_len = 0, second_len = 0;
                        memcpy(&first_len, body, 2);
                        size_t first_size = 2 + first_len + BT_HASH_SIZE;
                        if (first_size + 2 <= body_len) {
                            memcpy(&second_len, body + first_size, 2);
                            size_t second_size = 2 + second_len + BT_HASH_SIZE;
                            if (first_size + second_size <= body_len) {
                                uint8_t *swapped = malloc(first_size + second_size);
                                if (swapped) {
                                    memcpy(swapped, body + first_size, second_size);
                                    memcpy(swapped + second_size, body, first_size);
                                    if (pwrite(fd, swapped,
                                               first_size + second_size, 8) ==
                                        (ssize_t)(first_size + second_size))
                                        reordered = 1;
                                    free(swapped);
                                }
                            }
                        }
                    }
                    free(body);
                }
                close(fd);
            }
        }
    }
    ASSERT_TRUE(reordered, "reorder two entries in a generated btree run");
    pthread_join(tid, NULL);
    db->durability_test_pause_ms = 0;
    db->durability_test_pause_phase[0] = '\0';
    ASSERT_TRUE(q.response && strstr(q.response, "\"error\"") != NULL,
                "unsorted btree spill rejects reindex");
    free(q.response);
    char *after = rsc_indexes_snapshot_for_object(root, "spill_tree");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0,
                "unsorted btree spill leaves every target byte-identical");
    free(after); free(before);
    tu_pdb_drop_object(db, "d", "spill_tree");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-reindex-malformed-btree-spill",
              test_reindex_malformed_btree_spill_run)

typedef struct {
    ShardDb *db;
    atomic_int *stop;
    atomic_int *completed;
    atomic_int *failed;
} RscReaderThread;

static void *rsc_continuous_reader_thread(void *arg) {
    RscReaderThread *r = arg;
    while (!atomic_load(r->stop)) {
        char *resp = NULL;
        tu_pdb_request(r->db,
            "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"concurrent_idx\","
            "\"criteria\":[{\"field\":\"title\",\"op\":\"starts\","
            "\"value\":\"row\"}],\"timeout_ms\":30000}", &resp);
        if (!resp || strstr(resp, "\"error\"")) atomic_store(r->failed, 1);
        free(resp);
        atomic_fetch_add(r->completed, 1);
        /* objlock's per-object rwlock is deliberately reader-preferring (see
           docs/plans/2026-07-29-cache-rwlock-writer-preference.md), so a
           zero-gap back-to-back reader loop can starve reindex's write-lock
           request indefinitely (confirmed: >12 min with no progress). No
           real client issues requests with zero gap across 4 threads in
           lockstep, so this small pause keeps the concurrency coverage
           (readers genuinely overlap reindex + the online bulk insert)
           while giving the writer a real chance to be granted. */
        usleep(1 * 1000);
    }
    return NULL;
}

static void *rsc_bulk_insert_thread(void *arg) {
    RscQueryThread *q = arg;
    tu_pdb_request(q->db,
        "{\"mode\":\"bulk-insert\",\"dir\":\"d\",\"object\":\"concurrent_idx\","
        "\"records\":{\"late\":{\"title\":\"row late\"}},"
        "\"timeout_ms\":30000}",
        &q->response);
    return NULL;
}

static int test_online_bulk_reindex_readers_run(void) {
    ShardDb *db = test_get_process_db();
    const char *root = test_get_process_db_root();
    if (!db || !root) { ASSERT_TRUE(0, "process-local db available"); return 1; }
    tu_pdb_drop_object(db, "d", "concurrent_idx");
    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"concurrent_idx\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"title:varchar:64\"],"
        "\"indexes\":[\"title\"]}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"bulk-insert\",\"dir\":\"d\",\"object\":\"concurrent_idx\","
        "\"records\":{\"a\":{\"title\":\"row a\"},"
        "\"b\":{\"title\":\"row b\"}}}", &resp);
    free(resp); resp = NULL;

    atomic_int stop = 0, completed = 0, reader_failed = 0;
    RscReaderThread reader_args = {
        .db = db, .stop = &stop, .completed = &completed,
        .failed = &reader_failed,
    };
    pthread_t readers[4];
    for (int i = 0; i < 4; i++)
        ASSERT_EQ_INT(db_thread_create(&readers[i],
                                    rsc_continuous_reader_thread,
                                    &reader_args), 0,
                      "start continuous public index reader");
    for (int i = 0; i < 100 && atomic_load(&completed) < 4; i++)
        usleep(10 * 1000);
    ASSERT_TRUE(atomic_load(&completed) >= 4,
                "continuous readers make progress before publication");

    snprintf(db->durability_test_pause_phase,
             sizeof(db->durability_test_pause_phase), "idx-spills-before-merge");
    db->durability_test_pause_ms = 2000;
    RscQueryThread reindex = {
        .db = db, .object = "concurrent_idx", .response = NULL,
    };
    pthread_t reindex_tid;
    ASSERT_EQ_INT(db_thread_create(&reindex_tid, rsc_reindex_thread,
                                 &reindex), 0,
                  "start public reindex with continuous readers");
    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/d/concurrent_idx/indexes/title/.spill_0/"
             ".durability-test-idx-spills-before-merge.active", root);
    int seen = 0;
    for (int i = 0; i < 3000; i++) {
        if (access(marker, F_OK) == 0) { seen = 1; break; }
        usleep(10 * 1000);
    }
    ASSERT_TRUE(seen, "public reindex reaches pre-publication pause");

    RscQueryThread bulk = { .db = db, .object = NULL, .response = NULL };
    pthread_t bulk_tid;
    ASSERT_EQ_INT(db_thread_create(&bulk_tid, rsc_bulk_insert_thread,
                                 &bulk), 0,
                  "start online bulk insert while reindex is publishing");
    pthread_join(reindex_tid, NULL);
    pthread_join(bulk_tid, NULL);
    db->durability_test_pause_ms = 0;
    db->durability_test_pause_phase[0] = '\0';
    ASSERT_TRUE(reindex.response && !strstr(reindex.response, "\"error\""),
                "public reindex completes under reader and bulk pressure");
    ASSERT_TRUE(bulk.response && !strstr(bulk.response, "\"error\""),
                "online bulk insert completes after publication");
    free(reindex.response);
    free(bulk.response);

    int before_release = atomic_load(&completed);
    for (int i = 0; i < 100 && atomic_load(&completed) == before_release; i++)
        usleep(10 * 1000);
    ASSERT_TRUE(atomic_load(&completed) > before_release,
                "continuous readers resume after publication");
    atomic_store(&stop, 1);
    for (int i = 0; i < 4; i++) pthread_join(readers[i], NULL);
    ASSERT_EQ_INT(atomic_load(&reader_failed), 0,
                  "continuous public readers observe no index errors");
    tu_pdb_request(db,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"concurrent_idx\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"starts\","
        "\"value\":\"row\"}]}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 3,
                  "post-publication index contains the concurrent bulk row");
    free(resp);
    tu_pdb_drop_object(db, "d", "concurrent_idx");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-online-bulk-reindex-readers",
              test_online_bulk_reindex_readers_run)

typedef struct {
    pthread_mutex_t lock;
    char text[16384];
    size_t used;
} RscCapturedLog;

static void rsc_capture_log(int type, const char *message, void *userdata) {
    (void)type;
    RscCapturedLog *captured = userdata;
    pthread_mutex_lock(&captured->lock);
    size_t len = strlen(message);
    if (len < sizeof(captured->text) - captured->used) {
        memcpy(captured->text + captured->used, message, len + 1);
        captured->used += len;
    }
    pthread_mutex_unlock(&captured->lock);
}

static int rsc_log_line_has(const char *line, const char *needle) {
    if (!line) return 0;
    const char *end = strchr(line, '\n');
    if (!end) end = line + strlen(line);
    const char *found = strstr(line, needle);
    return found && found < end;
}

/* Online bulk must surface the exact B-tree publication state in its error
   log so operators can distinguish an untouched target from a renamed target
   whose parent-directory durability is unconfirmed. */
static int test_online_bulk_publication_diagnostics_run(void) {
    ShardDb *db = test_get_process_db();
    const char *root = test_get_process_db_root();
    if (!db || !root) { ASSERT_TRUE(0, "process-local db available"); return 1; }
    char *resp = NULL;
    RscCapturedLog captured = { .lock = PTHREAD_MUTEX_INITIALIZER };
    shard_db_set_log_handler(db, rsc_capture_log, &captured);
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;

    const char *objects[] = { "bulk_diag_pre", "bulk_diag_post" };
    const char *fields[] = { "diag_pre", "diag_post" };
    const char *states[] = {
        "state=pre-rename-failed",
        "state=post-rename-durability-unconfirmed",
    };
    for (int scenario = 0; scenario < 2; scenario++) {
        tu_pdb_drop_object(db, "d", objects[scenario]);
        char request[512];
        snprintf(request, sizeof(request),
                 "{\"mode\":\"create-object\",\"dir\":\"d\","
                 "\"object\":\"%s\",\"splits\":8,\"max_key\":16,"
                 "\"fields\":[\"%s:varchar:32\"],"
                 "\"indexes\":[\"%s\"]}",
                 objects[scenario], fields[scenario], fields[scenario]);
        tu_pdb_request(db, request, &resp);
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""),
                    "create online-bulk diagnostic fixture");
        free(resp); resp = NULL;

        btree_test_publish_fail_stage(scenario + 1);
        snprintf(request, sizeof(request),
                 "{\"mode\":\"bulk-insert\",\"dir\":\"d\","
                 "\"object\":\"%s\",\"records\":{\"k\":{\"%s\":\"v\"}}}",
                 objects[scenario], fields[scenario]);
        tu_pdb_request(db, request, &resp);
        ASSERT_NOT_NULL(resp, "online bulk returns after logged publication failure");
        free(resp); resp = NULL;

        pthread_mutex_lock(&captured.lock);
        char *log = strdup(captured.text);
        pthread_mutex_unlock(&captured.lock);
        ASSERT_TRUE(log && strstr(log, fields[scenario]),
                    "online-bulk publication failure reaches log handler");
        char field_needle[96], target_needle[PATH_MAX];
        snprintf(field_needle, sizeof(field_needle), "field=%s", fields[scenario]);
        snprintf(target_needle, sizeof(target_needle),
                 "target=%s/d/%s/indexes/%s/", root,
                 objects[scenario], fields[scenario]);
        const char *line = log ? strstr(log, field_needle) : NULL;
        ASSERT_TRUE(rsc_log_line_has(line, field_needle),
                    "diagnostic line names field");
        ASSERT_TRUE(rsc_log_line_has(line, "shard="),
                    "diagnostic line names shard");
        ASSERT_TRUE(rsc_log_line_has(line, target_needle),
                    "diagnostic line names target path");
        ASSERT_TRUE(rsc_log_line_has(line, states[scenario]),
                    "diagnostic line has exact publication state");
        ASSERT_TRUE(rsc_log_line_has(line, "errno=5"),
                    "diagnostic line preserves injected errno");
        free(log);
        btree_test_publish_fail_stage(0);
        tu_pdb_drop_object(db, "d", objects[scenario]);
    }
    shard_db_set_log_handler(db, NULL, NULL);
    pthread_mutex_destroy(&captured.lock);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-online-bulk-publication-diagnostics",
              test_online_bulk_publication_diagnostics_run)

/* Task 7: a partial kf header is a materialisation failure, not an empty
   shard.  The public force rebuild must leave its complete prior bitmap set
   untouched. */
static int test_bitmap_truncated_kf_fails_closed_run(void) {
    ShardDb *db = test_get_process_db();
    const char *root = test_get_process_db_root();
    if (!db || !root) { ASSERT_TRUE(0, "process-local db available"); return 1; }
    tu_pdb_drop_object(db, "d", "truncated_kf");
    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp); free(resp);
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"truncated_kf\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"flag:bool\"],"
        "\"indexes\":[\"flag:bitmap\"]}", &resp); free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"truncated_kf\","
        "\"key\":\"k\",\"value\":{\"flag\":true}}", &resp); free(resp);
    char *before = rsc_indexes_snapshot_for_object(root, "truncated_kf");
    char kf[PATH_MAX];
    snprintf(kf, sizeof(kf), "%s/d/truncated_kf/data/kf/000.kf", root);
    /* Inserts leave kf shards cached and mmap'd. Detach that mapping before
       truncation so the test exercises the rebuild's checked open/format
       path rather than faulting on the test process's stale mapping. */
    kfcache_shutdown();
    kfcache_init(16);
    int fd = open(kf, O_WRONLY);
    ASSERT_TRUE(fd >= 0, "open kf shard for deterministic truncation");
    if (fd >= 0) { ASSERT_EQ_INT(ftruncate(fd, 1), 0, "truncate kf to partial header"); close(fd); }
    tu_pdb_request(db,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"truncated_kf\","
        "\"field\":\"flag:bitmap\",\"force\":true}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"error\"") != NULL,
        "truncated kf rejects bitmap rebuild");
    free(resp);
    char *after = rsc_indexes_snapshot_for_object(root, "truncated_kf");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0,
        "truncated kf leaves all bitmap targets byte-identical");
    free(after); free(before);
    tu_pdb_drop_object(db, "d", "truncated_kf");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bitmap-truncated-kf-fails-closed",
              test_bitmap_truncated_kf_fails_closed_run)

/* Valid empty data is distinct from malformed input: after public deletes, a
   force rebuild publishes valid, empty bitmap shards with no stale bits. */
static int test_bitmap_empty_rebuild_run(void) {
    ShardDb *db = test_get_process_db();
    const char *root = test_get_process_db_root();
    if (!db || !root) { ASSERT_TRUE(0, "process-local db available"); return 1; }
    tu_pdb_drop_object(db, "d", "empty_bits");
    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp); free(resp);
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"empty_bits\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"flag:bool\"],"
        "\"indexes\":[\"flag:bitmap\"]}", &resp); free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"empty_bits\","
        "\"key\":\"k\",\"value\":{\"flag\":true}}", &resp); free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"delete\",\"dir\":\"d\",\"object\":\"empty_bits\",\"key\":\"k\"}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"empty_bits\","
        "\"field\":\"flag:bitmap\",\"force\":true}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "empty bitmap rebuild succeeds");
    free(resp);
    char eff_root[PATH_MAX];
    snprintf(eff_root, sizeof(eff_root), "%s/d", root);
    for (int s = 0; s < 8; s++) {
        char path[PATH_MAX];
        bm_build_path(path, sizeof(path), eff_root, "empty_bits", "flag", s);
        BitmapShard *bm = bm_open(path, 0, 0, 0, 0, 0);
        ASSERT_NOT_NULL(bm, "empty replacement bitmap opens");
        if (!bm) continue;
        uint8_t true_value = 1, false_value = 0;
        /* Bool bitmaps retain their fixed true/false dictionary even when
           empty; emptiness is the absence of candidate bits, not n_values. */
        ASSERT_EQ_INT((int)bm_count(bm, &true_value, 1), 0, "empty replacement has no stale true bits");
        ASSERT_EQ_INT((int)bm_count(bm, &false_value, 1), 0, "empty replacement has no stale false bits");
        bm_close(bm);
    }
    tu_pdb_drop_object(db, "d", "empty_bits");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bitmap-empty-rebuild", test_bitmap_empty_rebuild_run)

/* Task 3: bitmap follows data-shard fanout while B-tree/trigram follow the
   capped index fanout. Cleanup must therefore inspect an exact extension,
   never applying the B-tree cutoff to .bm siblings. */
static int test_reindex_mixed_extension_cleanup_run(void) {
    ShardDb *db = test_get_process_db();
    const char *root = test_get_process_db_root();
    if (!db || !root) { ASSERT_TRUE(0, "process-local db available"); return 1; }
    tu_pdb_drop_object(db, "d", "mixed");
    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp); free(resp);
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"mixed\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"title:varchar:32\",\"flag:bool\"],"
        "\"indexes\":[\"title\",\"title:trigram\",\"flag:bitmap\"]}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"mixed\","
        "\"key\":\"k\",\"value\":{\"title\":\"cleanable title\",\"flag\":true}}", &resp);
    free(resp); resp = NULL;
    char path[PATH_MAX];
    for (int s = 4; s < 16; s++) {
        snprintf(path, sizeof(path), "%s/d/mixed/indexes/title/%03x.idx", root, s);
        int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) { (void)write(fd, "old", 3); close(fd); }
        snprintf(path, sizeof(path), "%s/d/mixed/indexes/title/%03x.tg", root, s);
        fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) { (void)write(fd, "old", 3); close(fd); }
    }
    snprintf(path, sizeof(path), "%s/d/mixed/indexes/title/004.bmx", root);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) { (void)write(fd, "keep", 4); close(fd); }
    snprintf(path, sizeof(path), "%s/d/mixed/indexes/title/zz.idx", root);
    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) { (void)write(fd, "keep", 4); close(fd); }
    tu_pdb_request(db, "{\"mode\":\"reindex\",\"dir\":\"d\",\"object\":\"mixed\"}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "mixed index reindex succeeds");
    free(resp);
    for (int s = 0; s < 16; s++) {
        snprintf(path, sizeof(path), "%s/d/mixed/indexes/flag/%03x.bm", root, s);
        ASSERT_TRUE(access(path, F_OK) == 0, "all bitmap data-shard files survive cleanup");
    }
    for (int s = 4; s < 16; s++) {
        snprintf(path, sizeof(path), "%s/d/mixed/indexes/title/%03x.idx", root, s);
        ASSERT_TRUE(access(path, F_OK) != 0, "obsolete btree shard removed by exact extension");
        snprintf(path, sizeof(path), "%s/d/mixed/indexes/title/%03x.tg", root, s);
        ASSERT_TRUE(access(path, F_OK) != 0, "obsolete trigram shard removed by exact extension");
    }
    snprintf(path, sizeof(path), "%s/d/mixed/indexes/title/004.bmx", root);
    ASSERT_TRUE(access(path, F_OK) == 0, "unrelated extension remains");
    snprintf(path, sizeof(path), "%s/d/mixed/indexes/title/zz.idx", root);
    ASSERT_TRUE(access(path, F_OK) == 0, "noncanonical filename remains");
    tu_pdb_drop_object(db, "d", "mixed");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-reindex-mixed-extension-cleanup",
              test_reindex_mixed_extension_cleanup_run)

static int rsc_touch(const char *path) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    return close(fd);
}

static void rsc_plant_startup_siblings(const char *root) {
    const char *fields[] = { "tree", "grams", "flag" };
    char path[PATH_MAX];
    for (int i = 0; i < 3; i++) {
        snprintf(path, sizeof(path), "%s/d/sweep/indexes/%s", root, fields[i]);
        mkdirp(path);
        snprintf(path, sizeof(path), "%s/d/sweep/indexes/%s/.rebuild-abandoned",
                 root, fields[i]);
        (void)rsc_touch(path);
    }
    snprintf(path, sizeof(path), "%s/d/sweep/indexes/.index-conf-abandoned", root);
    (void)rsc_touch(path);
    snprintf(path, sizeof(path), "%s/d/sweep/indexes/tree/.operator-note", root);
    (void)rsc_touch(path);
    char live[PATH_MAX], link[PATH_MAX];
    snprintf(live, sizeof(live), "%s/d/sweep/indexes/tree/000.idx", root);
    (void)rsc_touch(live);
    snprintf(link, sizeof(link), "%s/d/sweep/indexes/tree/.rebuild-link", root);
    if (access(link, F_OK) != 0) (void)symlink(live, link);
}

static void rsc_assert_startup_siblings_swept(const char *root,
                                               const char *startup) {
    const char *fields[] = { "tree", "grams", "flag" };
    char path[PATH_MAX], assertion[160];
    for (int i = 0; i < 3; i++) {
        snprintf(path, sizeof(path), "%s/d/sweep/indexes/%s/.rebuild-abandoned",
                 root, fields[i]);
        snprintf(assertion, sizeof(assertion),
                 "%s startup removes %s generated sibling", startup, fields[i]);
        ASSERT_TRUE(access(path, F_OK) != 0, assertion);
    }
    snprintf(path, sizeof(path), "%s/d/sweep/indexes/.index-conf-abandoned", root);
    snprintf(assertion, sizeof(assertion),
             "%s startup removes generated metadata sibling", startup);
    ASSERT_TRUE(access(path, F_OK) != 0, assertion);
    snprintf(path, sizeof(path), "%s/d/sweep/indexes/tree/.operator-note", root);
    snprintf(assertion, sizeof(assertion),
             "%s startup retains unrelated dotfile", startup);
    ASSERT_TRUE(access(path, F_OK) == 0, assertion);
    snprintf(path, sizeof(path), "%s/d/sweep/indexes/tree/.rebuild-link", root);
    struct stat lst;
    snprintf(assertion, sizeof(assertion),
             "%s startup retains generated-looking symlink", startup);
    ASSERT_TRUE(lstat(path, &lst) == 0 && S_ISLNK(lst.st_mode), assertion);
}

/* Task 6: exercise the shared fd-relative cleanup through both real startup
   paths. Generated regular siblings are removed for btree, trigram, bitmap,
   and metadata while symlinks and unrelated dotfiles remain inert. */
static int test_index_rebuild_temp_sweep_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        ASSERT_TRUE(0, "start daemon sweep fixture");
        return 1;
    }
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect daemon sweep fixture");
    char *resp = NULL;
    if (tc) {
        tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
        free(resp); resp = NULL;
        tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"sweep\","
            "\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"tree:varchar:32\",\"grams:varchar:32\",\"flag:bool\"],"
            "\"indexes\":[\"tree\",\"grams:trigram\",\"flag:bitmap\"]}",
            &resp);
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""),
                    "create valid startup sweep object");
        free(resp); resp = NULL;
        tc_close(tc);
    }
    int port = env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", env.db_root);
    test_env_stop_keep(&env);

    /* Cleanup runs before the normal schema validator. A hostile component
       in schema.conf must not let its fd-relative walk escape DB_ROOT. */
    char base[256];
    const char *slash = strrchr(root, '/');
    size_t base_len = slash ? (size_t)(slash - root) : 0;
    memcpy(base, root, base_len);
    base[base_len] = '\0';
    char schema_path[PATH_MAX], outside_dir[PATH_MAX], outside_temp[PATH_MAX];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", root);
    snprintf(outside_dir, sizeof(outside_dir),
             "%s/escape/sweep/indexes/tree", base);
    mkdirp(outside_dir);
    snprintf(outside_temp, sizeof(outside_temp),
             "%s/.rebuild-must-survive", outside_dir);
    ASSERT_EQ_INT(rsc_touch(outside_temp), 0, "plant generated-looking file outside DB_ROOT");
    char *valid_schema = tu_read_file(schema_path);
    ASSERT_NOT_NULL(valid_schema, "read valid schema before containment probe");
    FILE *schema = fopen(schema_path, "a");
    ASSERT_NOT_NULL(schema, "append hostile schema component");
    if (schema) {
        fputs("../escape:sweep:8:16:2:1\n", schema);
        fclose(schema);
    }
    ASSERT_EQ_INT(index_rebuild_temp_sweep(root), 0,
                  "startup sweep ignores invalid schema path components");
    ASSERT_TRUE(access(outside_temp, F_OK) == 0,
                "startup sweep cannot unlink outside DB_ROOT");
    schema = fopen(schema_path, "w");
    ASSERT_NOT_NULL(schema, "restore valid schema after containment probe");
    if (schema) {
        if (valid_schema) fputs(valid_schema, schema);
        fclose(schema);
    }
    free(valid_schema);

    rsc_plant_startup_siblings(root);
    TestEnv restarted = {0};
    ASSERT_EQ_INT(test_env_start_at(&restarted, root, port), 0,
                  "daemon restart runs index sibling sweep");
    if (restarted.daemon_pid > 0) test_env_stop_keep(&restarted);
    rsc_assert_startup_siblings_swept(root, "daemon");

    rsc_plant_startup_siblings(root);
    char callback_log[PATH_MAX];
    snprintf(callback_log, sizeof(callback_log), "%s/embedded-sweep.log", base);
    ASSERT_EQ_INT(tu_run_cmd("./build/bin/embedded_bg_harness %s %s 0 1 %s",
                             base, root, callback_log), 0,
                  "embedded open runs index sibling sweep");
    rsc_assert_startup_siblings_swept(root, "embedded");
    (void)tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-index-rebuild-temp-sweep", test_index_rebuild_temp_sweep_run)

TEST_REGISTER("test-reindex-spill-collision", test_reindex_spill_collision_run);
