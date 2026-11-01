/* src/test/cases/test_agg_packed_routes.c
 *
 * R3 private physical packed fixture (public packed binding stays inactive).
 * Hand-built TypedSchema bound with typed_schema_bind_packed; records written
 * through typed_encode_alloc + slotcask_upsert_with_hooks(PKR_FLAGS); index
 * keys inserted with typed_index_record_key_into + btree_idx_insert;
 * aggregates executed through the TEST_BUILD schema seam.
 *
 * Registrations: test-agg-packed-routes-bounded, test-agg-packed-routes-bare,
 * test-agg-packed-routes-extended (bare tie scale-equivalence, large bounded
 * decimals + cancellation, scaled-unit overflow past 2^63, allocation faults,
 * the >=100k-row partition merge, and the bounded-memory rejection).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/typed_row.h"
#include "../db/typed_index.h"
#include "../db/packed_record.h"
#include "../db/query_internal.h"
#include "../db/agg_exact.h"
#include <errno.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

extern int cmd_aggregate_with_schema_for_test(const char *, const char *,
    const Schema *, const FieldSchema *, const char *, const char *,
    const char *, const char *, const char *, int, int, const char *,
    const char *, int);

static int apr_rm(const char *p, const struct stat *s, int k, struct FTW *f) {
    (void)s; (void)k; (void)f; return remove(p);
}
static void apr_restore_root(char (*saved)[PATH_MAX]) { memcpy(g_db_root, *saved, PATH_MAX); }

static char *apr_run(const char *root, const char *obj, const Schema *sch,
                     const FieldSchema *fs, const char *group, const char *aggs,
                     const char *criteria) {
    char *out = NULL; size_t len = 0;
    FILE *m = open_memstream(&out, &len);
    FILE *saved = g_out; g_out = m;
    cmd_aggregate_with_schema_for_test(root, obj, sch, fs, criteria, group, aggs,
                                       NULL, NULL, 0, 0, NULL, NULL, 0);
    g_out = saved; fclose(m);
    if (out) { size_t n = strlen(out); while (n && out[n - 1] == '\n') out[--n] = 0; }
    return out;
}

typedef struct { const char *key, *f0, *f1; } AprRow;

static void apr_object(const char *root, const char *obj, int splits, int indexed,
                       TypedSchema *ts, const char *f0_decl, const AprRow *rows,
                       int nrows, Schema *sch_out, SlotcaskDb **db_out) {
    char path[512];
    snprintf(path, sizeof path, "%s/%s", root, obj);
    ASSERT_EQ_INT(mkdir(path, 0700), 0, "obj dir");
    snprintf(path, sizeof path, "%s/schema.conf", root);
    FILE *f = fopen(path, "a");                      /* one line per object */
    fprintf(f, ":%s:%d:32:2:1\n", obj, splits); fclose(f);
    snprintf(path, sizeof path, "%s/%s/fields.conf", root, obj);
    f = fopen(path, "w"); fprintf(f, "f0:%s\nf1:int\n", f0_decl); fclose(f);
    *sch_out = load_schema(root, obj);
    ASSERT_EQ_INT(sch_out->splits, splits, "schema loads");
    SlotcaskSchemaInfo info = { .splits = sch_out->splits, .slot_size = sch_out->slot_size,
                                .streams = sch_out->streams };
    *db_out = slotcask_registry_get(root, obj, &info);
    ASSERT_NOT_NULL(*db_out, "slotcask open");
    if (indexed) {
        snprintf(path, sizeof path, "%s/%s/indexes", root, obj); mkdir(path, 0700);
        snprintf(path, sizeof path, "%s/%s/indexes/f0", root, obj); mkdir(path, 0700);
        snprintf(path, sizeof path, "%s/%s/indexes/f1", root, obj); mkdir(path, 0700);
        snprintf(path, sizeof path, "%s/%s/indexes/index.conf", root, obj);
        f = fopen(path, "w"); fputs("f0\nf1\n", f); fclose(f);
        invalidate_idx_cache(root, obj);
    }
    for (int i = 0; i < nrows; i++) {
        char json[4096];
        snprintf(json, sizeof json, "{\"f0\":%s,\"f1\":%s}", rows[i].f0, rows[i].f1);
        uint8_t *value = NULL; size_t n = 0;
        ASSERT_EQ_INT(typed_encode_alloc(ts, json, 0, NULL, NULL, &value, &n, NULL, 0), 0, "encode");
        SlotcaskUpsertOpts opts = { .record_hdr2 = PKR_FLAGS };
        SlotcaskUpsertResult res;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(*db_out, 0, rows[i].key, strlen(rows[i].key),
                                                 value, n, &opts, &res), 0, "store");
        if (indexed) {
            uint8_t hash[16];
            compute_hash_raw(rows[i].key, strlen(rows[i].key), hash);
            for (int fi = 0; fi < 2; fi++) {
                uint8_t enc[544]; size_t el = 0;
                int have = typed_index_record_key_into(ts, value, n, PKR_FLAGS,
                                                       fi ? "f1" : "f0", enc, sizeof enc, &el);
                ASSERT_TRUE(have == 1, "key encodes");
                ASSERT_EQ_INT(btree_idx_insert(root, obj, fi ? "f1" : "f0", sch_out->splits,
                                               (const char *)enc, el, hash), 0, "index insert");
            }
        }
        free(value);
    }
}

static TypedSchema *apr_schema(const char *f0_decl) {
    const char *types[2] = { f0_decl, "int" };
    TypedSchema *ts = calloc(1, sizeof *ts);
    ASSERT_NOT_NULL(ts, "private schema");
    if (!ts) return NULL;
    ts->typed = 1; ts->nfields = 2;
    if (!strncmp(f0_decl, "varchar:", 8)) parse_field_type(f0_decl, &ts->fields[0]);
    else ts->fields[0].type = FT_NUMERIC;
    parse_field_type("int", &ts->fields[1]);
    snprintf(ts->fields[0].name, 256, "f0"); ts->fields[0].name_len = 2;
    snprintf(ts->fields[1].name, 256, "f1"); ts->fields[1].name_len = 2;
    ASSERT_EQ_INT(typed_schema_bind_packed(ts, types, 2, NULL, 0), 0, "bind");
    return ts;
}

static void apr_teardown_schema(TypedSchema *ts) {
    for (int i = 0; i < ts->nfields; i++) free_enum_values(&ts->fields[i]);
    free(ts);
}

/* ---- core matrix: 3 layouts x (scalars, packed-int scalars, group labels,
   criteria sum). Oracle-checked literals: 0,1,2,NULL dataset. ---- */
static const AprRow APR_ROWS[] = {
    { "zero", "0", "3" }, { "one", "1", "2" }, { "two", "2", "1" },
    { "null", "null", "0" },
};

static int apr_case(int bare) {
    char saved_root[PATH_MAX] __attribute__((cleanup(apr_restore_root)));
    memcpy(saved_root, g_db_root, PATH_MAX);
    const char *f0_type = bare ? "numeric" : "numeric:19,4";
    TypedSchema *ts = apr_schema(f0_type);
    if (!ts) return 1;

    char root[] = "/tmp/shard-agg-packed-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(root), "root");
    snprintf(g_db_root, PATH_MAX, "%s", root);
    struct { const char *name; int splits, indexed; } layouts[] = {
        { "pi", 8, 1 }, { "ps", 8, 0 }, { "p16", 16, 1 },
    };
    for (int li = 0; li < 3; li++) {
        Schema sch; SlotcaskDb *db = NULL;
        apr_object(root, layouts[li].name, layouts[li].splits, layouts[li].indexed, ts,
                   f0_type, APR_ROWS, 4, &sch, &db);
        FieldSchema fs = { .ts = ts };
        const char *obj = layouts[li].name;
        char *o;
        /* A1: min/max/sum/avg of the packed numeric (index shortcut when
           bounded, record-fed route when bare — D1 gate). */
        o = apr_run(root, obj, &sch, &fs, NULL,
            "[{\"fn\":\"min\",\"field\":\"f0\",\"alias\":\"mn\"},{\"fn\":\"max\",\"field\":\"f0\",\"alias\":\"mx\"},"
            "{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"},"
            "{\"fn\":\"count\",\"field\":\"f0\",\"alias\":\"c\"}]", NULL);
        ASSERT_EQ_STR(o, bare
            ? "{\"mn\":0,\"mx\":2,\"s\":3,\"a\":1.00000000000000000000,\"c\":3}"
            : "{\"mn\":0.0000,\"mx\":2.0000,\"s\":3.0000,\"a\":1.00000000000000000000,\"c\":3}",
            "packed numeric scalars");
        free(o);
        /* A9: packed fixed int through the record-fed accumulator */
        o = apr_run(root, obj, &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f1\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f1\",\"alias\":\"a\"}]", NULL);
        ASSERT_EQ_STR(o, "{\"s\":6,\"a\":1.5000000000000000}", "packed int scalars");
        free(o);
        /* A2: group labels (value and NULL groups), indexed and scan parity */
        o = apr_run(root, obj, &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"}]", NULL);
        const char *l0 = bare ? "{\"f0\":\"0\",\"c\":1}" : "{\"f0\":\"0.0000\",\"c\":1}";
        const char *l1 = bare ? "{\"f0\":\"1\",\"c\":1}" : "{\"f0\":\"1.0000\",\"c\":1}";
        const char *l2 = bare ? "{\"f0\":\"2\",\"c\":1}" : "{\"f0\":\"2.0000\",\"c\":1}";
        ASSERT_TRUE(o && strstr(o, l0) && strstr(o, l1) && strstr(o, l2) &&
                    strstr(o, "{\"f0\":null,\"c\":1}") && !strstr(o, "\"f0\":\"\""),
                    "four labelled groups (value + NULL), none unlabeled");
        free(o);
        /* criteria-bearing aggregate over the packed rows (scan or candidate) */
        o = apr_run(root, obj, &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]",
            "{\"field\":\"f1\",\"op\":\"gte\",\"value\":2}");
        ASSERT_EQ_STR(o, bare ? "{\"s\":1}" : "{\"s\":1.0000}", "criteria sum");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, obj);
        invalidate_idx_cache(root, obj);
    }
    apr_teardown_schema(ts);
    memcpy(g_db_root, saved_root, PATH_MAX);
    ASSERT_EQ_INT(nftw(root, apr_rm, 16, FTW_DEPTH | FTW_PHYS), 0, "fixture removed");
    return 0;
}
static int apr_bounded(void) { return apr_case(0); }
static int apr_bare(void) { return apr_case(1); }
TEST_REGISTER("test-agg-packed-routes-bounded", apr_bounded)
TEST_REGISTER("test-agg-packed-routes-bare", apr_bare)

/* ---- extended: (a) bare tie scale-equivalence; (b) large bounded decimals
   + cancellation; (c) scaled-unit overflow past 2^63; (d) allocation faults;
   (e) >=100k-row partition merge; (f) bounded-memory rejection. ---- */

typedef struct { ShardDb *db; size_t cap; } AprBudgetSave;
static void apr_budget_restore(AprBudgetSave *saved) {
    saved->db->query_buffer_max_bytes = saved->cap;
}

static void apr_fault_restore(int *unused) {
    (void)unused; agg_exact_test_set_fail_alloc(0);
}

static int apr_extended(void) {
    int fault_guard __attribute__((cleanup(apr_fault_restore))) = 0;
    char saved_root[PATH_MAX] __attribute__((cleanup(apr_restore_root)));
    memcpy(saved_root, g_db_root, PATH_MAX);
    char root[] = "/tmp/shard-agg-packed-ext-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(root), "root");
    snprintf(g_db_root, PATH_MAX, "%s", root);
    Schema sch; SlotcaskDb *db = NULL; FieldSchema fs; char *o;

    /* (a) bare tie: rows 1, 1.00, 1.0 — one canonical group, larger-scale
       min/max ties, sum at the largest operand scale (oracle goldens). */
    {
        static const AprRow rows[] = {
            { "t1", "1", "1" }, { "t2", "1.00", "1" }, { "t3", "1.0", "1" },
        };
        TypedSchema *ts = apr_schema("numeric");
        ASSERT_NOT_NULL(ts, "tie schema"); if (!ts) return 1;
        apr_object(root, "tie", 8, 1, ts, "numeric", rows, 3, &sch, &db);
        fs.ts = ts;
        o = apr_run(root, "tie", &sch, &fs, NULL,
            "[{\"fn\":\"min\",\"field\":\"f0\",\"alias\":\"mn\"},{\"fn\":\"max\",\"field\":\"f0\",\"alias\":\"mx\"},"
            "{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"},"
            "{\"fn\":\"count\",\"field\":\"f0\",\"alias\":\"c\"}]", NULL);
        ASSERT_EQ_STR(o, "{\"mn\":1.00,\"mx\":1.00,\"s\":3.00,"
                         "\"a\":1.00000000000000000000,\"c\":3}", "tie scalars");
        free(o);
        o = apr_run(root, "tie", &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"}]", NULL);
        ASSERT_EQ_STR(o, "[{\"f0\":\"1\",\"c\":3}]", "one canonical bare group");
        free(o);
        /* scale-equivalent keys on the index route form one group too */
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "tie");
        invalidate_idx_cache(root, "tie");
        apr_teardown_schema(ts);
    }

    /* D8: full packed varchar spans, differing after byte 1023. */
    {
        char a[1101], b[1101], ja[1103], jb[1103], expected[2300];
        memset(a, 'a', 1100); a[1100] = 0;
        memcpy(b, a, sizeof b); b[1050] = 'b';
        snprintf(ja, sizeof ja, "\"%s\"", a);
        snprintf(jb, sizeof jb, "\"%s\"", b);
        AprRow rows[] = { {"a", ja, "1"}, {"b", jb, "1"} };
        TypedSchema *ts = apr_schema("varchar:2000");
        ASSERT_NOT_NULL(ts, "packed text schema"); if (!ts) return 1;
        apr_object(root, "text", 8, 0, ts, "varchar:2000", rows, 2, &sch, &db);
        fs.ts = ts;
        snprintf(expected, sizeof expected, "{\"mn\":\"%s\",\"mx\":\"%s\"}", a, b);
        o = apr_run(root, "text", &sch, &fs, NULL,
            "[{\"fn\":\"min\",\"field\":\"f0\",\"alias\":\"mn\"},"
            "{\"fn\":\"max\",\"field\":\"f0\",\"alias\":\"mx\"}]", NULL);
        ASSERT_EQ_STR(o, expected, "full packed text extrema"); free(o);
        slotcask_registry_put(db); slotcask_registry_invalidate(root, "text");
        invalidate_idx_cache(root, "text"); apr_teardown_schema(ts);
    }

    /* (b)+(c) bounded numeric:38,4 — large values, cancellation tail, and a
       sum crossing 2^63 scaled units (checked __int128, no double). */
    {
        static const AprRow rows[] = {
            { "g1", "12345678901234567890.1234", "1" },
            { "g2", "0.0001", "1" }, { "g3", "-0.0001", "1" },
            { "g4", "999999999999999.9999", "1" },
            { "g5", "-999999999999999.9999", "1" },
            { "h1", "500000000000000.0000", "2" },
            { "h2", "500000000000000.0000", "2" },
        };
        TypedSchema *ts = apr_schema("numeric:38,4");
        ASSERT_NOT_NULL(ts, "large schema"); if (!ts) return 1;
        apr_object(root, "large", 8, 1, ts, "numeric:38,4", rows, 7, &sch, &db);
        fs.ts = ts;
        o = apr_run(root, "large", &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"},"
            "{\"fn\":\"min\",\"field\":\"f0\",\"alias\":\"mn\"},{\"fn\":\"max\",\"field\":\"f0\",\"alias\":\"mx\"}]",
            "{\"field\":\"f1\",\"op\":\"eq\",\"value\":1}");
        ASSERT_EQ_STR(o, "{\"s\":12345678901234567890.1234,"
                         "\"a\":2469135780246913578.0247,"
                         "\"mn\":-999999999999999.9999,"
                         "\"mx\":12345678901234567890.1234}", "large + cancellation");
        free(o);
        o = apr_run(root, "large", &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"}]",
            "{\"field\":\"f1\",\"op\":\"eq\",\"value\":2}");
        ASSERT_EQ_STR(o, "{\"s\":1000000000000000.0000,\"a\":500000000000000.0000}",
                      "scaled-unit sum past 2^63");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "large");
        invalidate_idx_cache(root, "large");
        apr_teardown_schema(ts);
    }

    /* (d) allocation faults on the packed render path: error or exact. */
    {
        TypedSchema *ts = apr_schema("numeric:19,4");
        ASSERT_NOT_NULL(ts, "fault schema"); if (!ts) return 1;
        apr_object(root, "flt", 8, 0, ts, "numeric:19,4", APR_ROWS, 4, &sch, &db);
        fs.ts = ts;
        int saw_error = 0, saw_ok = 0;
        for (int n = 1; n <= 32; n++) {
            agg_exact_test_set_fail_alloc(n);
            o = apr_run(root, "flt", &sch, &fs, NULL,
                "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},"
                "{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"}]", NULL);
            agg_exact_test_set_fail_alloc(0);
            ASSERT_NOT_NULL(o, "fault response");
            if (!o) break;
            if (strstr(o, "\"error\"")) {
                saw_error = 1;
                ASSERT_TRUE(!strstr(o, "\"s\""), "error response carries no partial value");
            } else {
                saw_ok = 1;
                ASSERT_EQ_STR(o, "{\"s\":3.0000,\"a\":1.00000000000000000000}", "exact after faults");
            }
            free(o);
        }
        /* Parallel scan/merge allocations vary with worker layout. The
           sweep proves errors are explicit; a disabled-fault call proves
           exact success without guessing how many allocations it takes. */
        agg_exact_test_set_fail_alloc(0);
        o = apr_run(root, "flt", &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},"
            "{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"}]", NULL);
        ASSERT_EQ_STR(o, "{\"s\":3.0000,\"a\":1.00000000000000000000}", "unfaulted exact success");
        if (o && !strstr(o, "\"error\"")) saw_ok = 1;
        free(o);
        ASSERT_TRUE(saw_error && saw_ok, "fault injection errors and unfaulted exact success");
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "flt");
        invalidate_idx_cache(root, "flt");
        apr_teardown_schema(ts);
    }

    /* (f) bounded memory: tiny cap rejects the bucket allocation explicitly. */
    {
        TypedSchema *ts = apr_schema("numeric:19,4");
        ASSERT_NOT_NULL(ts, "budget schema"); if (!ts) return 1;
        apr_object(root, "bud", 8, 1, ts, "numeric:19,4", APR_ROWS, 4, &sch, &db);
        fs.ts = ts;
        ShardDb *pdb = test_get_process_db();
        AprBudgetSave saved_budget __attribute__((cleanup(apr_budget_restore))) = {
            pdb, pdb->query_buffer_max_bytes
        };
        pdb->query_buffer_max_bytes = 1024;
        o = apr_run(root, "bud", &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"},{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]", NULL);
        pdb->query_buffer_max_bytes = saved_budget.cap;
        ASSERT_NOT_NULL(o, "budget response");
        ASSERT_TRUE(o && strstr(o, "query memory buffer exceeded") && !strstr(o, "\"c\""),
                    "budget rejection is an error, not a partial success");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "bud");
        invalidate_idx_cache(root, "bud");
        apr_teardown_schema(ts);
    }

    /* (e) partition merge: >=100k rows at splits 64 (8 idx shards) forces the
       parallel IGB scatter + per-partition merge. Groups must be identical to
       the serial layouts and sum exactly. */
    {
        static const AprRow few[] = { { "seed", "null", "null" } };
        TypedSchema *ts = apr_schema("numeric");
        ASSERT_NOT_NULL(ts, "partition schema"); if (!ts) return 1;
        apr_object(root, "part", 64, 1, ts, "numeric", few, 1, &sch, &db);
        fs.ts = ts;
        for (int i = 0; i < 120000; i++) {
            char json[64], key[16];
            snprintf(json, sizeof json, "{\"f0\":%d,\"f1\":1}", i % 3);
            snprintf(key, sizeof key, "p%d", i);
            uint8_t *value = NULL; size_t n = 0;
            ASSERT_EQ_INT(typed_encode_alloc(ts, json, 0, NULL, NULL, &value, &n, NULL, 0), 0, "bulk encode");
            SlotcaskUpsertOpts opts = { .record_hdr2 = PKR_FLAGS };
            SlotcaskUpsertResult res;
            ASSERT_EQ_INT(slotcask_upsert_with_hooks(db, 0, key, strlen(key),
                                                     value, n, &opts, &res), 0, "bulk store");
            uint8_t hash[16];
            compute_hash_raw(key, strlen(key), hash);
            for (int fi = 0; fi < 2; fi++) {
                uint8_t enc[544]; size_t el = 0;
                ASSERT_EQ_INT(typed_index_record_key_into(ts, value, n, PKR_FLAGS,
                                                          fi ? "f1" : "f0", enc,
                                                          sizeof enc, &el), 1, "bulk key");
                ASSERT_EQ_INT(btree_idx_insert(root, "part", fi ? "f1" : "f0",
                                               sch.splits, (const char *)enc, el,
                                               hash), 0, "bulk index");
            }
            free(value);
        }
        o = apr_run(root, "part", &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"},{\"fn\":\"sum\",\"field\":\"f1\",\"alias\":\"s\"}]",
            NULL);
        ASSERT_NOT_NULL(o, "partition response");
        ASSERT_TRUE(o && strstr(o, "{\"f0\":\"0\",\"c\":40000,\"s\":40000}") &&
                    strstr(o, "{\"f0\":\"1\",\"c\":40000,\"s\":40000}") &&
                    strstr(o, "{\"f0\":\"2\",\"c\":40000,\"s\":40000}") &&
                    strstr(o, "{\"f0\":null,\"c\":1,\"s\":0}"),
                    "partition merge folds every worker exactly including NULL seed");
        free(o);
        o = apr_run(root, "part", &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f1\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"}]",
            NULL);
        ASSERT_EQ_STR(o, "{\"s\":120000,\"a\":1.00000000000000000000}", "partition scalar totals");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "part");
        invalidate_idx_cache(root, "part");
        apr_teardown_schema(ts);
    }

    /* (g) partition merge with DECIMAL sums (2026-10-31, R3 review): the
       (e) case sums an int field, so the per-partition fold never merged
       exact dec_t states.

       (g1) mirrors (e)'s shape with bare-decimal values: 150k rows over 100
       distinct groups (1500 rows each, spread across partitions) so the
       fold genuinely merges dec_t sums. Expected lexemes are the
       Python-decimal oracle's: group t sums
       V(t) 1500 times, V(t) = (t).01, grand total over all rows. */
    {
        TypedSchema *ts = apr_schema("numeric");
        ASSERT_NOT_NULL(ts, "dec-partition schema"); if (!ts) return 1;
        apr_object(root, "decpart", 64, 1, ts, "numeric", NULL, 0, &sch, &db);
        fs.ts = ts;
        for (int i = 0; i < 150000; i++) {
            int t = i % 100;
            char json[32], key[16];
            snprintf(json, sizeof json, "{\"f0\":\"%d.%02d\"}", t, 1);
            static const char b36[] = "0123456789abcdefghijklmnopqrstuvwxyz";
            key[0] = 'd';
            key[1] = b36[(i / 36 / 36 / 36 / 36) % 36];
            key[2] = b36[(i / 36 / 36 / 36) % 36];
            key[3] = b36[(i / 36 / 36) % 36];
            key[4] = b36[(i / 36) % 36];
            key[5] = b36[i % 36];
            key[6] = 0;
            uint8_t *value = NULL; size_t n = 0;
            ASSERT_EQ_INT(typed_encode_alloc(ts, json, 0, NULL, NULL, &value, &n, NULL, 0), 0, "dec bulk encode");
            SlotcaskUpsertOpts opts = { .record_hdr2 = PKR_FLAGS };
            SlotcaskUpsertResult res;
            ASSERT_EQ_INT(slotcask_upsert_with_hooks(db, 0, key, strlen(key),
                                                     value, n, &opts, &res), 0, "dec bulk store");
            uint8_t hash[16];
            compute_hash_raw(key, strlen(key), hash);
            uint8_t enc[544]; size_t el = 0;
            ASSERT_EQ_INT(typed_index_record_key_into(ts, value, n, PKR_FLAGS,
                                                      "f0", enc, sizeof enc, &el), 1, "dec bulk key");
            ASSERT_EQ_INT(btree_idx_insert(root, "decpart", "f0",
                                           sch.splits, (const char *)enc, el,
                                           hash), 0, "dec bulk index");
            free(value);
        }
        o = apr_run(root, "decpart", &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"},{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]",
            NULL);
        ASSERT_NOT_NULL(o, "dec-partition response");
        int ngroups = 0;
        const char *p = o;
        while ((p = strstr(p, "{\"f0\":")) != NULL) { ngroups++; p++; }
        ASSERT_EQ_INT(ngroups, 100, "100 distinct decimal groups across partitions");
        ASSERT_TRUE(strstr(o, "{\"f0\":\"0.01\",\"c\":1500,\"s\":15.00}") &&
                    strstr(o, "{\"f0\":\"1.01\",\"c\":1500,\"s\":1515.00}") &&
                    strstr(o, "{\"f0\":\"42.01\",\"c\":1500,\"s\":63015.00}"),
                    "decimal sums fold exactly across partitions (oracle)");
        free(o);
        o = apr_run(root, "decpart", &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},"
            "{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"}]",
            NULL);
        ASSERT_EQ_STR(o, "{\"s\":7426500.00,\"a\":49.5100000000000000}",
                      "decimal grand totals across partitions (oracle)");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "decpart");
        invalidate_idx_cache(root, "decpart");
        apr_teardown_schema(ts);
    }

    /* (g2) 100k distinct decimal groups, one row each, at splits 64 (8 idx
       shards): the parallel IGB partition path must bucket every distinct
       group exactly. Expected lexemes are the Python-decimal oracle's.
       The duplicate-group variant is (g3) below. */
    {
        TypedSchema *ts = apr_schema("numeric");
        ASSERT_NOT_NULL(ts, "dec-cardinality schema"); if (!ts) return 1;
        apr_object(root, "deccard", 64, 1, ts, "numeric", NULL, 0, &sch, &db);
        fs.ts = ts;
        for (int i = 0; i < 100000; i++) {
            char json[32], key[16];
            snprintf(json, sizeof json, "{\"f0\":\"%d.%02d\"}",
                     i % 10000, i / 10000 + 1);
            snprintf(key, sizeof key, "d%d", i);
            uint8_t *value = NULL; size_t n = 0;
            ASSERT_EQ_INT(typed_encode_alloc(ts, json, 0, NULL, NULL, &value, &n, NULL, 0), 0, "card bulk encode");
            SlotcaskUpsertOpts opts = { .record_hdr2 = PKR_FLAGS };
            SlotcaskUpsertResult res;
            ASSERT_EQ_INT(slotcask_upsert_with_hooks(db, 0, key, strlen(key),
                                                     value, n, &opts, &res), 0, "card bulk store");
            uint8_t hash[16];
            compute_hash_raw(key, strlen(key), hash);
            uint8_t enc[544]; size_t el = 0;
            ASSERT_EQ_INT(typed_index_record_key_into(ts, value, n, PKR_FLAGS,
                                                      "f0", enc, sizeof enc, &el), 1, "card bulk key");
            ASSERT_EQ_INT(btree_idx_insert(root, "deccard", "f0",
                                           sch.splits, (const char *)enc, el,
                                           hash), 0, "card bulk index");
            free(value);
        }
        o = apr_run(root, "deccard", &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"},{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]",
            NULL);
        ASSERT_NOT_NULL(o, "cardinality response");
        int ngroups = 0;
        const char *p = o;
        while ((p = strstr(p, "{\"f0\":")) != NULL) { ngroups++; p++; }
        ASSERT_EQ_INT(ngroups, 100000, "100k distinct decimal groups across partitions");
        ASSERT_TRUE(strstr(o, "{\"f0\":\"0.01\",\"c\":1,\"s\":0.01}") &&
                    strstr(o, "{\"f0\":\"1.01\",\"c\":1,\"s\":1.01}") &&
                    strstr(o, "{\"f0\":\"9999.05\",\"c\":1,\"s\":9999.05}") &&
                    strstr(o, "{\"f0\":\"0.06\",\"c\":1,\"s\":0.06}") &&
                    strstr(o, "{\"f0\":\"9999.1\",\"c\":1,\"s\":9999.10}"),
                    "100k single-entry decimal groups count and sum exactly");
        free(o);
        o = apr_run(root, "deccard", &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]",
            NULL);
        ASSERT_EQ_STR(o, "{\"s\":499955500.00}",
                      "cardinality grand sum across partitions (oracle)");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "deccard");
        invalidate_idx_cache(root, "deccard");
        apr_teardown_schema(ts);
    }

    /* (g3) 150k rows over 100k decimal groups, 50k of them two-row groups
       whose entries hash to different idx shards: the parallel IGB
       partition fold must account for every row. */
    {
        TypedSchema *ts = apr_schema("numeric");
        ASSERT_NOT_NULL(ts, "dec-dup schema"); if (!ts) return 1;
        apr_object(root, "decdup", 64, 1, ts, "numeric", NULL, 0, &sch, &db);
        fs.ts = ts;
        for (int i = 0; i < 150000; i++) {
            int gi = i < 100000 ? i / 2 : i - 50000;
            char json[32], key[16];
            snprintf(json, sizeof json, "{\"f0\":\"%d.%02d\"}",
                     gi % 10000, gi / 10000 + 1);
            snprintf(key, sizeof key, "d%d", i);
            uint8_t *value = NULL; size_t n = 0;
            ASSERT_EQ_INT(typed_encode_alloc(ts, json, 0, NULL, NULL, &value, &n, NULL, 0), 0, "dup bulk encode");
            SlotcaskUpsertOpts opts = { .record_hdr2 = PKR_FLAGS };
            SlotcaskUpsertResult res;
            ASSERT_EQ_INT(slotcask_upsert_with_hooks(db, 0, key, strlen(key),
                                                     value, n, &opts, &res), 0, "dup bulk store");
            uint8_t hash[16];
            compute_hash_raw(key, strlen(key), hash);
            uint8_t enc[544]; size_t el = 0;
            ASSERT_EQ_INT(typed_index_record_key_into(ts, value, n, PKR_FLAGS,
                                                      "f0", enc, sizeof enc, &el), 1, "dup bulk key");
            ASSERT_EQ_INT(btree_idx_insert(root, "decdup", "f0",
                                           sch.splits, (const char *)enc, el,
                                           hash), 0, "dup bulk index");
            free(value);
        }
        o = apr_run(root, "decdup", &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"}]", NULL);
        ASSERT_NOT_NULL(o, "dup response");
        int ngroups = 0, n1 = 0, n2 = 0;
        for (const char *p = o; (p = strstr(p, "{\"f0\":")) != NULL; p++) ngroups++;
        for (const char *p = o; (p = strstr(p, "\"c\":1}")) != NULL; p++) n1++;
        for (const char *p = o; (p = strstr(p, "\"c\":2}")) != NULL; p++) n2++;
        ASSERT_EQ_INT(ngroups, 100000, "dup: 100k distinct groups");
        ASSERT_EQ_INT(n1, 50000, "dup: 50k singleton groups");
        ASSERT_EQ_INT(n2, 50000, "dup: 50k two-row groups count 2 (no vanished rows)");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "decdup");
        invalidate_idx_cache(root, "decdup");
        apr_teardown_schema(ts);
    }

    memcpy(g_db_root, saved_root, PATH_MAX);
    ASSERT_EQ_INT(nftw(root, apr_rm, 16, FTW_DEPTH | FTW_PHYS), 0, "extended fixture removed");
    return 0;
}
TEST_REGISTER("test-agg-packed-routes-extended", apr_extended)

static char *apr_ordered(const char *root, const char *obj, const Schema *sch,
                         const FieldSchema *fs, const char *group) {
    char *out = NULL; size_t len = 0;
    FILE *m = open_memstream(&out, &len), *saved = g_out; g_out = m;
    cmd_aggregate_with_schema_for_test(root,obj,sch,fs,NULL,group,
        "[{\"fn\":\"count\",\"alias\":\"c\"}]",NULL,"c",1,10,NULL,NULL,0);
    g_out = saved; fclose(m); return out;
}
static int apr_output_safety(void) {
    char saved_root[PATH_MAX] __attribute__((cleanup(apr_restore_root)));
    memcpy(saved_root,g_db_root,PATH_MAX);
    char root[]="/tmp/shard-agg-output-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(root),"root"); snprintf(g_db_root,PATH_MAX,"%s",root);
    static const AprRow rows[] = { {"a","0","3"},{"b","1","2"},{"c","2","1"},{"n","null","null"} };
    for (int bare = 0; bare < 2; bare++) {
        const char *decl = bare ? "numeric" : "numeric:19,4", *obj = bare ? "bare" : "bounded";
        TypedSchema *ts=apr_schema(decl); Schema sch; SlotcaskDb *db=NULL;
        apr_object(root,obj,8,1,ts,decl,rows,4,&sch,&db); FieldSchema fs={.ts=ts};
        char *out=apr_ordered(root,obj,&sch,&fs,"[\"f0\"]");
        ASSERT_TRUE(out && strstr(out,bare ? "\"f0\":\"0\"" : "\"f0\":\"0.0000\"") &&
            strstr(out,bare ? "\"f0\":\"1\"" : "\"f0\":\"1.0000\"") &&
            strstr(out,bare ? "\"f0\":\"2\"" : "\"f0\":\"2.0000\"") &&
            strstr(out,"\"f0\":null") && !strstr(out,"\"f0\":\"\""),"packed count(*) topN labels exact"); free(out);
        out=apr_ordered(root,obj,&sch,&fs,"[\"f1\"]");
        ASSERT_TRUE(out && strstr(out,"{\"f1\":null,\"c\":1}"),"fixed-width NULL tier is a NULL group"); free(out);
        slotcask_registry_put(db);slotcask_registry_invalidate(root,obj);invalidate_idx_cache(root,obj);apr_teardown_schema(ts);
    }
    TypedSchema *ts=apr_schema("numeric"); Schema sch; SlotcaskDb *db=NULL;
    apr_object(root,"budget",8,0,ts,"numeric",NULL,0,&sch,&db); FieldSchema fs={.ts=ts};
    char *json=malloc(17000), *literal=malloc(16400);
    ASSERT_NOT_NULL(json,"json buffer"); ASSERT_NOT_NULL(literal,"literal buffer");
    if (!json || !literal) { free(json);free(literal);return 1; }
    literal[0]='1';literal[1]='.';memset(literal+2,'0',16383);literal[16385]=0;
    for (int i=0;i<200;i++) {
        char key[32];snprintf(key,sizeof key,"k%d",i);
        snprintf(json,17000,"{\"f0\":%s,\"f1\":%d}",literal,i);
        uint8_t *value=NULL;size_t len=0;
        ASSERT_EQ_INT(typed_encode_alloc(ts,json,0,NULL,NULL,&value,&len,NULL,0),0,"large-scale encode");
        SlotcaskUpsertOpts opts={.record_hdr2=PKR_FLAGS};SlotcaskUpsertResult res;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(db,0,key,strlen(key),value,len,&opts,&res),0,"large-scale row");free(value);
    }
    free(json);
    ShardDb *pdb=test_get_process_db();
    AprBudgetSave saved __attribute__((cleanup(apr_budget_restore)))={pdb,pdb->query_buffer_max_bytes};
    pdb->query_buffer_max_bytes=1024*1024;
    char *out=apr_run(root,"budget",&sch,&fs,"[\"f1\"]",
        "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]",NULL);
    ASSERT_EQ_STR(out,"{\"error\":\"query memory buffer exceeded; narrow criteria, add limit/offset, or stream via fetch+cursor\"}",
        "3MB retained lexemes reject a 1MiB budget before output");free(out);
    pdb->query_buffer_max_bytes=8*1024*1024;
    out=apr_run(root,"budget",&sch,&fs,"[\"f1\"]",
        "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]",NULL);
    ASSERT_NOT_NULL(out,"large rendered success");
    int seen=0;const char *pos=out;
    while (pos && (pos=strstr(pos,"\"s\":"))) {
        pos+=4;
        ASSERT_TRUE(!strncmp(pos,literal,strlen(literal)) && pos[strlen(literal)]=='}',"every retained decimal keeps full display scale");
        seen++;pos+=strlen(literal);
    }
    ASSERT_EQ_INT(seen,200,"all 200 exact groups survive with sufficient budget");
    free(out);free(literal);pdb->query_buffer_max_bytes=saved.cap;
    slotcask_registry_put(db);slotcask_registry_invalidate(root,"budget");invalidate_idx_cache(root,"budget");apr_teardown_schema(ts);
    memcpy(g_db_root,saved_root,PATH_MAX);
    ASSERT_EQ_INT(nftw(root,apr_rm,16,FTW_DEPTH|FTW_PHYS),0,"output fixtures removed");
    return 0;
}
TEST_REGISTER("test-agg-packed-output-safety",apr_output_safety)

