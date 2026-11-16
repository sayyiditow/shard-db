/* src/test/cases/test_indexed_zero_exists.c
 *
 * P1 regression (2026-10-29 blind-review finding): the indexed candidate
 * pre-filters dropped every leaf entry the legacy index decoder could not
 * render, and the decoder reports "missing" for values that are live
 * non-NULL rows — the packed numeric zero marker (1-byte decimal_order
 * payload vs the 8-byte legacy width), the empty varchar (0-byte payload),
 * and the all-zero uuid/ipv4 values. exists/find/count discarded those
 * candidates BEFORE record verification, so an indexed `exists` omitted
 * rows a full scan returns.
 *
 * Part 1 builds a real packed object (numeric + varchar + uuid + ipv4,
 * all indexed), writes rows 0/""/zero-uuid/0.0.0.0, a non-zero sibling
 * and an all-NULL row, then requires: the planner routes `exists` to the
 * indexed primary leaf; the count, the collect_hash_cb candidate set and
 * the O_DIRECT record scan all agree on 2 rows per field.
 *
 * Part 2 drives the same zero/empty values through the embedded request
 * layer on ordinary nullmap-generation objects and requires the indexed
 * object to match the identical unindexed (scan-path) object.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/typed_row.h"
#include "../db/typed_index.h"
#include "../db/packed_record.h"
#include "../db/query_internal.h"
#include "../db/shard_db.h"
#include <errno.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int zex_remove(const char *p, const struct stat *s, int k,
                      struct FTW *f) {
    (void)s; (void)k; (void)f;
    return remove(p);
}

#define ZEX_KEYS 3
static const char *zex_rows[ZEX_KEYS] = {
    "{\"f0\":0,\"f1\":\"\",\"f2\":\"00000000-0000-0000-0000-000000000000\","
    "\"f3\":\"0.0.0.0\",\"f4\":7}",
    "{\"f0\":1,\"f1\":\"x\",\"f2\":\"123e4567-e89b-12d3-a456-426614174000\","
    "\"f3\":\"1.2.3.4\",\"f4\":8}",
    "{\"f0\":null,\"f1\":null,\"f2\":null,\"f3\":null,\"f4\":9}",
};
static const char *zex_fields[4] = { "f0", "f1", "f2", "f3" };

static void zex_mkdir(const char *root, const char *sub) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", root, sub);
    ASSERT_EQ_INT(mkdir(path, 0700), 0, "fixture directory created");
}

extern int cmd_find_with_schema_for_test(const char *, const char *,
    const Schema *, const FieldSchema *, CriteriaNode *, int, int,
    const char *, const char *, const char *, const char *);

static void zex_restore_root(char (*saved)[PATH_MAX]) {
    memcpy(g_db_root, *saved, PATH_MAX);
}

static void zex_assert_page(const char *root, const Schema *schema,
                            const FieldSchema *fields, const char *criteria,
                            int offset, int limit, int expected) {
    const char *error = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria, &error);
    ASSERT_NOT_NULL(tree, "page criteria parse");
    if (!tree) return;
    ASSERT_EQ_INT(compile_criteria_tree_checked(tree, fields->ts, NULL, 0),
                  0, "page criteria compile");
    FilterPlan plan = plan_filter(tree, root, "obj", fields, schema->splits,
                                  5, NULL, 0, 0);
    ASSERT_EQ_INT(plan.kind, FP_PRIMARY_LEAF, "page uses primary index");
    ASSERT_TRUE(plan.n_source == 1 &&
                !strcmp(plan.source_leaves[0]->field, "f0") &&
                plan.n_postfilter == 1,
                "numeric index candidates retain a record postfilter");
    char *text = NULL;
    size_t len = 0;
    FILE *mem = open_memstream(&text, &len);
    ASSERT_NOT_NULL(mem, "page response stream");
    if (!mem) { free_criteria_tree(tree); return; }
    FILE *saved = g_out;
    g_out = mem;
    int rc = cmd_find_with_schema_for_test(root, "obj", schema, fields,
                                           tree, offset, limit, "rows",
                                           NULL, NULL, NULL);
    g_out = saved;
    ASSERT_EQ_INT(fclose(mem), 0, "page response closes");
    ASSERT_EQ_INT(rc, 0, "paginated find succeeds");
    JsonObj result;
    const char *rows = NULL;
    size_t rows_len = 0;
    int parsed = text && json_parse_object(text, len, &result) >= 0;
    ASSERT_TRUE(parsed, "page response is JSON");
    int has_rows = parsed && json_obj_get(&result, "rows", &rows, &rows_len);
    ASSERT_TRUE(has_rows, "page has rows array");
    int count = 0;
    if (has_rows) {
        /* Fixture values contain no arrays: every nested '[' is one row. */
        for (size_t i = 1; i < rows_len; i++) count += rows[i] == '[';
    }
    ASSERT_EQ_INT(count, expected, "pagination counts matches, not candidates");
    free(text);
    free_criteria_tree(tree);
}

static int zex_packed_run(void) {
    char saved_root[PATH_MAX] __attribute__((cleanup(zex_restore_root)));
    memcpy(saved_root, g_db_root, PATH_MAX);
    /* Keep both numeric seed rows in one index shard: zero is visited
       before one regardless of inter-shard worker scheduling. */
    char matching_key[32];
    uint8_t zero_hash[16], hash[16];
    compute_hash_raw("z0", 2, zero_hash);
    int key_i = 0;
    do {
        snprintf(matching_key, sizeof(matching_key), "zmatch-%d", key_i++);
        compute_hash_raw(matching_key, strlen(matching_key), hash);
    } while (idx_shard_for_hash(hash, 8) != idx_shard_for_hash(zero_hash, 8));
    const char *zex_key_names[ZEX_KEYS] = { "z0", matching_key, "zn" };
    TypedSchema *ts = calloc(1, sizeof(*ts));
    ASSERT_NOT_NULL(ts, "private schema");
    if (!ts) return 1;
    ts->typed = 1;
    ts->nfields = 5;
    const char *types[5] = { "numeric", "varchar:4096", "uuid", "ipv4", "int" };
    for (int i = 0; i < 5; i++) {
        TypedField *f = &ts->fields[i];
        if (i == 0) f->type = FT_NUMERIC;
        else if (i == 1) f->type = FT_VARCHAR;
        else parse_field_type(types[i], f);
        snprintf(f->name, sizeof(f->name), "f%d", i);
        f->name_len = 2;
    }
    int rc = typed_schema_bind_packed(ts, types, 5, NULL, 0);
    ASSERT_EQ_INT(rc, 0, "packed schema binds");
    if (rc) { free(ts); return 1; }

    char root[] = "/tmp/shard-zero-exists-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(root), "fixture root");
    char path[512];
    snprintf(g_db_root, PATH_MAX, "%s", root);
    {
        char cmd[600];
        snprintf(cmd, sizeof(cmd), "%s/obj", root);
        ASSERT_EQ_INT(mkdir(cmd, 0700), 0, "obj dir");
    }
    snprintf(path, sizeof(path), "%s/schema.conf", root);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f, "schema.conf");
    if (f) { fputs(":obj:8:32:2:1\n", f); fclose(f); }
    snprintf(path, sizeof(path), "%s/obj/fields.conf", root);
    f = fopen(path, "w");
    ASSERT_NOT_NULL(f, "fields.conf");
    if (f) {
        fputs("f0:numeric\nf1:varchar:8192\nf2:uuid\nf3:ipv4\nf4:int\n", f);
        fclose(f);
    }
    Schema schema = load_schema(root, "obj");
    ASSERT_EQ_INT(schema.splits, 8, "fixture schema loads");
    FieldSchema fields = { .ts = ts };
    SlotcaskSchemaInfo info = { .splits = schema.splits,
                                .slot_size = schema.slot_size,
                                .streams = schema.streams };
    SlotcaskDb *db = slotcask_registry_get(root, "obj", &info);
    ASSERT_NOT_NULL(db, "slotcask open");
    if (!db || schema.splits != 8) { free(ts); return 1; }

    for (int i = 0; i < ZEX_KEYS; i++) {
        uint8_t *value = NULL;
        size_t n = 0;
        ASSERT_EQ_INT(typed_encode_alloc(ts, zex_rows[i], 0, NULL, NULL,
                                         &value, &n, NULL, 0), 0,
                      "packed record encodes");
        SlotcaskUpsertOpts opts = { .record_hdr2 = PKR_FLAGS };
        SlotcaskUpsertResult result;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(db, 0, zex_key_names[i],
                                                 strlen(zex_key_names[i]),
                                                 value, n, &opts, &result), 0,
                      "packed record stored");
        free(value);
    }

    zex_mkdir(root, "obj/indexes");
    for (int i = 0; i < 4; i++) {
        snprintf(path, sizeof(path), "%s/obj/indexes/%s", root, zex_fields[i]);
        ASSERT_EQ_INT(mkdir(path, 0700), 0, "index dir");
    }
    snprintf(path, sizeof(path), "%s/obj/indexes/index.conf", root);
    f = fopen(path, "w");
    ASSERT_NOT_NULL(f, "index.conf");
    if (f) { fputs("f0\nf1\nf2\nf3\n", f); fclose(f); }
    invalidate_idx_cache(root, "obj");

    for (size_t i = 0; i < ZEX_KEYS; i++) {
        void *value = NULL;
        size_t n = 0;
        uint8_t flags = 0;
        ASSERT_EQ_INT(slotcask_get_ex(db, zex_key_names[i],
                                      strlen(zex_key_names[i]), &value, &n,
                                      &flags), 0, "record readable");
        ASSERT_EQ_INT(flags, PKR_FLAGS, "persisted packed generation");
        for (int fi = 0; fi < 4; fi++) {
            uint8_t encoded[544];
            size_t len = 0;
            int have = typed_index_record_key_into(ts, value, n, flags,
                                                   zex_fields[fi], encoded,
                                                   sizeof(encoded), &len);
            ASSERT_TRUE(have == 1, "index key encodes");
            if (have != 1) continue;
            uint8_t hash[16];
            compute_hash_raw(zex_key_names[i], strlen(zex_key_names[i]), hash);
            ASSERT_EQ_INT(btree_idx_insert(root, "obj", zex_fields[fi],
                                           schema.splits, (const char *)encoded,
                                           len, hash), 0, "index entry inserted");
        }
        free(value);
    }

    for (int fi = 0; fi < 4; fi++) {
        char json[128];
        snprintf(json, sizeof(json), "{\"field\":\"%s\",\"op\":\"exists\"}",
                 zex_fields[fi]);
        const char *err = NULL;
        CriteriaNode *tree = parse_criteria_tree(json, &err);
        ASSERT_NOT_NULL(tree, "exists tree parses");
        if (!tree) continue;
        ASSERT_EQ_INT(compile_criteria_tree_checked(tree, ts, NULL, 0), 0,
                      "exists tree compiles");
        FilterPlan plan = plan_filter(tree, root, "obj", &fields,
                                      schema.splits, ZEX_KEYS, NULL, 0, 0);
        ASSERT_EQ_INT(plan.kind, FP_PRIMARY_LEAF, "exists drives the index");
        ASSERT_EQ_INT(plan.n_source, 1, "exists has one seed leaf");

        QueryDeadline dl = { now_ms_coarse(), 0, 0 };
        size_t count = idx_count_for_leaf(root, "obj", &schema, &fields,
                                          &tree->leaf, &dl);
        ASSERT_TRUE(count == 2, "indexed exists counts the zero value row");

        int64_t scanned = -1;
        ASSERT_EQ_INT(scan_shards_v2_o_direct_match_checked(db, &fields, NULL,
                                                            tree, &dl,
                                                            &scanned), 0,
                      "record scan runs");
        ASSERT_TRUE(scanned == 2, "record scan counts 2 non-NULL rows");
        ASSERT_TRUE((int64_t)count == scanned,
                    "index path matches full scan");

        const TypedField *tf = resolve_idx_field(ts, zex_fields[fi]);
        CollectCtx cc;
        collect_ctx_init(&cc);
        cc.splits = schema.splits;
        cc.primary_crit = &tree->leaf;
        cc.check_primary = op_needs_check_primary(tree->leaf.op);
        cc.deadline = &dl;
        cc.tf = tf;
        ASSERT_EQ_INT(btree_dispatch_schema_checked(ts, root, "obj",
                                                    zex_fields[fi],
                                                    schema.splits, &tree->leaf,
                                                    tf, collect_hash_cb,
                                                    &cc), 0,
                      "candidate walk runs");
        ASSERT_TRUE(cc.count == 2, "candidate collector keeps the zero value");
        ASSERT_TRUE(!cc.budget_exceeded, "collector within budget");
        collect_ctx_destroy(&cc);
        free_criteria_tree(tree);
    }

    /* Sanity: exact equality on the same fields is unaffected (the eq seek
       has no per-entry decode), so the fix must not over-admit either. */
    const char *eq_json[2] = { "{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"}",
                               "{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}" };
    for (int i = 0; i < 2; i++) {
        const char *err = NULL;
        CriteriaNode *tree = parse_criteria_tree(eq_json[i], &err);
        ASSERT_NOT_NULL(tree, "eq tree parses");
        if (!tree) continue;
        ASSERT_EQ_INT(compile_criteria_tree_checked(tree, ts, NULL, 0), 0,
                      "eq tree compiles");
        QueryDeadline dl = { now_ms_coarse(), 0, 0 };
        size_t count = idx_count_for_leaf(root, "obj", &schema, &fields,
                                          &tree->leaf, &dl);
        ASSERT_TRUE(count == 1, "eq count exact on packed numeric");
        free_criteria_tree(tree);
    }

    /* Add two later matching entries. All f0 entries share an index
       shard, so the first candidate is the nonmatching zero row. */
    for (int i = 0; i < 2; i++) {
        char key[32], json[64];
        do {
            snprintf(key, sizeof(key), "zpage-%d", key_i++);
            compute_hash_raw(key, strlen(key), hash);
        } while (idx_shard_for_hash(hash, 8) != idx_shard_for_hash(zero_hash, 8));
        snprintf(json, sizeof(json), "{\"f0\":%d,\"f4\":8}", 10 + i);
        uint8_t *value = NULL;
        size_t n = 0, encoded_len = 0;
        ASSERT_EQ_INT(typed_encode_alloc(ts, json, 0, NULL, NULL, &value,
                                         &n, NULL, 0), 0,
                      "later matching packed row encodes");
        SlotcaskUpsertOpts opts = { .record_hdr2 = PKR_FLAGS };
        SlotcaskUpsertResult result;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(db, 0, key, strlen(key),
                                                 value, n, &opts, &result),
                      0, "later matching packed row stores");
        uint8_t encoded[544];
        ASSERT_EQ_INT(typed_index_record_key_into(ts, value, n, PKR_FLAGS,
                                                  "f0", encoded,
                                                  sizeof(encoded), &encoded_len),
                      1, "later matching index key encodes");
        ASSERT_EQ_INT(btree_idx_insert(root, "obj", "f0", schema.splits,
                                       (const char *)encoded, encoded_len, hash),
                      0, "later matching index key inserts");
        free(value);
    }
    const char *page_criteria =
        "{\"and\":[{\"field\":\"f0\",\"op\":\"exists\"},"
        "{\"field\":\"f4\",\"op\":\"eq\",\"value\":8}]}";
    zex_assert_page(root, &schema, &fields, page_criteria, 0, 1, 1);
    zex_assert_page(root, &schema, &fields, page_criteria, 1, 2, 2);
    zex_assert_page(root, &schema, &fields, page_criteria, 0, 0, 3);

    slotcask_registry_put(db);
    slotcask_registry_invalidate(root, "obj");
    invalidate_idx_cache(root, "obj");
    for (int i = 0; i < ts->nfields; i++) free_enum_values(&ts->fields[i]);
    free(ts);
    memcpy(g_db_root, saved_root, PATH_MAX);
    ASSERT_EQ_INT(nftw(root, zex_remove, 16, FTW_DEPTH | FTW_PHYS), 0,
                  "fixture removed");
    return 0;
}
TEST_REGISTER("test-indexed-zero-exists-packed", zex_packed_run)

/* ---- Part 2: ordinary nullmap-generation objects over the request layer.
   The identical rows live in an indexed object and an unindexed one; the
   indexed `exists` counts must match the scan-path object exactly. ---- */

static void zex_req(ShardDb *db, const char *req, char **resp) {
    tu_pdb_request(db, req, resp);
    ASSERT_NOT_NULL(*resp, "response");
}

static long zex_count(ShardDb *db, const char *obj, const char *field) {
    char req[256], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
             "\"criteria\":{\"field\":\"%s\",\"op\":\"exists\"}}", obj, field);
    zex_req(db, req, &resp);
    long v = strtol(resp, NULL, 10);
    shard_db_free_result(resp);
    return v;
}

static int zex_request_run(void) {
    ShardDb *db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;
    char *resp = NULL;
    zex_req(db, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    shard_db_free_result(resp); resp = NULL;

    const char *fields = "\"v:varchar:64\",\"u:uuid\",\"i:ipv4\",\"n:long\"";
    const char *indexes = "\"v\",\"u\",\"i\",\"n\"";
    char req[1024];
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":"
             "\"zex_i\",\"splits\":8,\"max_key\":16,\"fields\":[%s],"
             "\"indexes\":[%s]}", fields, indexes);
    zex_req(db, req, &resp);
    ASSERT_TRUE(!strstr(resp, "\"error\""), "indexed object created");
    shard_db_free_result(resp); resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":"
             "\"zex_s\",\"splits\":8,\"max_key\":16,\"fields\":[%s]}",
             fields);
    zex_req(db, req, &resp);
    ASSERT_TRUE(!strstr(resp, "\"error\""), "scan object created");
    shard_db_free_result(resp); resp = NULL;

    const char *rows[3] = {
        "{\"key\":\"k1\",\"value\":{\"v\":\"\",\"u\":\"00000000-0000-0000-"
        "0000-000000000000\",\"i\":\"0.0.0.0\",\"n\":0}}",
        "{\"key\":\"k2\",\"value\":{\"v\":\"x\",\"u\":\"123e4567-e89b-12d3-"
        "a456-426614174000\",\"i\":\"1.2.3.4\",\"n\":1}}",
        "{\"key\":\"k3\",\"value\":{\"v\":null,\"u\":null,\"i\":null,"
        "\"n\":null}}",
    };
    for (int i = 0; i < 3; i++) {
        char rec[512];
        snprintf(rec, sizeof(rec), "%s", rows[i]);
        for (int pass = 0; pass < 2; pass++) {
            snprintf(req, sizeof(req),
                     "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                     "\"object\":\"%s\",\"records\":[%s]}",
                     pass ? "zex_s" : "zex_i", rec);
            zex_req(db, req, &resp);
            ASSERT_TRUE(!strstr(resp, "\"error\""), "row inserted");
            shard_db_free_result(resp); resp = NULL;
        }
    }

    const char *names[4] = { "v", "u", "i", "n" };
    for (int i = 0; i < 4; i++) {
        long indexed = zex_count(db, "zex_i", names[i]);
        long scan = zex_count(db, "zex_s", names[i]);
        ASSERT_TRUE(indexed == 2, "indexed exists counts the zero/empty row");
        ASSERT_TRUE(scan == 2, "scan exists counts 2 non-NULL rows");
        ASSERT_TRUE(indexed == scan, "index path matches full scan");
    }

    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"zex_i\","
             "\"criteria\":{\"field\":\"u\",\"op\":\"exists\"}}");
    zex_req(db, req, &resp);
    ASSERT_CONTAINS(resp, "00000000-0000-0000-0000-000000000000",
                    "find emits the zero uuid row");
    ASSERT_CONTAINS(resp, "123e4567-e89b-12d3-a456-426614174000",
                    "find emits the non-zero uuid row");
    shard_db_free_result(resp); resp = NULL;

    tu_pdb_drop_object(db, "default", "zex_i");
    tu_pdb_drop_object(db, "default", "zex_s");
    return 0;
}
TEST_REGISTER("test-indexed-zero-exists-request", zex_request_run)

/* UUID display-pattern operators currently match no typed records.
   The index prefilter still admits undecodable zero UUIDs. A passing bitmap
   must not turn an admitted candidate into a match; require scan parity. */
static int zex_candidate_count_run(void) {
    ShardDb *db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;
    char *resp = NULL;
    zex_req(db, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    shard_db_free_result(resp);
    resp = NULL;
    zex_req(db, "{\"mode\":\"create-object\",\"dir\":\"default\","
                "\"object\":\"zex_count\",\"splits\":8,\"max_key\":16,"
                "\"fields\":[\"u:uuid\",\"b:bool\"],"
                "\"indexes\":[\"u\",\"b:bitmap\"]}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "mixed-index object creates");
    shard_db_free_result(resp);
    resp = NULL;
    zex_req(db, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                "\"object\":\"zex_count\",\"records\":["
                "{\"key\":\"zero\",\"value\":{\"u\":"
                "\"00000000-0000-0000-0000-000000000000\",\"b\":true}},"
                "{\"key\":\"match\",\"value\":{\"u\":"
                "\"123e4567-e89b-12d3-a456-426614174000\",\"b\":true}},"
                "{\"key\":\"bitmap-reject\",\"value\":{\"u\":"
                "\"123e4567-e89b-12d3-a456-426614174001\",\"b\":false}}]}",
            &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "mixed-index rows insert");
    shard_db_free_result(resp);
    resp = NULL;
    const char *criteria =
        "{\"and\":[{\"field\":\"u\",\"op\":\"contains\",\"value\":\"123e\"},"
        "{\"field\":\"b\",\"op\":\"eq\",\"value\":true}]}";
    const char *error = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria, &error);
    ASSERT_NOT_NULL(tree, "mixed-index criteria parse");
    char root[PATH_MAX];
    ASSERT_TRUE(snprintf(root, sizeof(root), "%s/default", g_db_root) <
                (int)sizeof(root), "tenant root fits");
    Schema schema = load_schema(root, "zex_count");
    FieldSchema fields;
    init_field_schema(&fields, root, "zex_count");
    if (tree) {
        ASSERT_EQ_INT(compile_criteria_tree_checked(tree, fields.ts, NULL, 0),
                      0, "mixed-index criteria compile");
        FilterPlan plan = plan_filter(tree, root, "zex_count", &fields,
                                      schema.splits, 3, NULL, 0, 0);
        ASSERT_EQ_INT(plan.kind, FP_PRIMARY_LEAF, "mixed count uses primary leaf");
        ASSERT_TRUE(plan.n_source == 1 &&
                    !strcmp(plan.source_leaves[0]->field, "u") &&
                    plan.n_postfilter == 1 &&
                    !strcmp(plan.postfilter_leaves[0]->field, "b"),
                    "pattern primary retains bitmap postfilter");
        free_criteria_tree(tree);
    }
    char req[1024];
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\","
             "\"object\":\"zex_count\",\"criteria\":%s}", criteria);
    zex_req(db, req, &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "indexed count succeeds");
    long indexed = resp ? strtol(resp, NULL, 10) : -1;
    ASSERT_EQ_INT(indexed, 0, "bitmap pass still verifies the primary predicate");
    shard_db_free_result(resp);
    resp = NULL;

    zex_req(db, "{\"mode\":\"count\",\"dir\":\"default\","
                "\"object\":\"zex_count\",\"criteria\":"
                "{\"field\":\"b\",\"op\":\"eq\",\"value\":true}}", &resp);
    ASSERT_EQ_INT(resp ? strtol(resp, NULL, 10) : -1, 2,
                  "positive bitmap control sees both live candidates");
    shard_db_free_result(resp);
    resp = NULL;

    const char *index_fields[2] = { "u", "b" };
    for (int i = 0; i < 2; i++) {
        snprintf(req, sizeof(req),
                 "{\"mode\":\"remove-index\",\"dir\":\"default\","
                 "\"object\":\"zex_count\",\"field\":\"%s\"}", index_fields[i]);
        zex_req(db, req, &resp);
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "control index removes");
        shard_db_free_result(resp);
        resp = NULL;
    }
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\","
             "\"object\":\"zex_count\",\"criteria\":%s}", criteria);
    zex_req(db, req, &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "full-scan count succeeds");
    long scanned = resp ? strtol(resp, NULL, 10) : -1;
    ASSERT_EQ_INT(scanned, 0, "full scan rejects UUID display patterns");
    ASSERT_EQ_INT(indexed, scanned, "mixed-index count agrees with full scan");
    shard_db_free_result(resp);
    tu_pdb_drop_object(db, "default", "zex_count");
    return 0;
}
TEST_REGISTER("test-indexed-zero-exists-count-verification", zex_candidate_count_run)
