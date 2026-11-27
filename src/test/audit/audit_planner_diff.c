/* src/test/audit/audit_planner_diff.c
 *
 * Audit-campaign planner differential (plan Phase 1 step 4, second
 * harness; docs/plans/2026-11-19-audit-campaign.md). No engine changes:
 * identical generated data is loaded into twin objects differing only in
 * index shape, and every generated query must return IDENTICAL results
 * across shapes. `explain` records which plan path each run took; a path
 * never reached by data shape is tallied and reported (the plan's
 * force-hook decision needs that list).
 *
 * Twin shapes (create-object "indexes") — every generated criterion is
 * sent to all four twins, so criteria must be shape-agnostic:
 *   pd_plain  — no indexes (full-scan oracle)
 *   pd_btree  — btrees on v, t, w
 *   pd_bitmap — bitmaps on v, w + btree on t
 *   pd_comp   — composite u+v
 * Fields: v varchar:48 ("cat_0".."cat_4", 5 cats ≈ rows/5 — safely over
 * the N/8 selectivity budget so single v-eq leaves drive FP_BITMAP_SMALLER
 * on the bitmap shape and the B5 broad-btree demotion on the btree shape),
 * w varchar:16 ("w_0".."w_2", ≈ rows/3 — very broad bitmap), u int
 * (i%13 — composite-prefix driver), t long (=i, unique, 26-row ranges —
 * the selective leaf and the order_by total order).
 *
 * Structural path drivers (coverage decision 2026-11-19, human decision 4):
 *   scan      — u/w/t/v leaves on shapes lacking that index; broad single
 *               v-eq on the btree shape (B5 fetch-loses-to-scan demotion)
 *   leaf      — selective t-ranges; AND[u-eq,v-eq] composite-exact on
 *               pd_comp; count AND[v-eq,w-eq] on pd_btree
 *   bitmap    — broad single bitmap leaf (v-eq / w-eq) on pd_bitmap
 *   intersect — AND[t-range,t-range] count (2 selective → B1) on
 *               pd_btree/pd_bitmap; AND[v-eq,w-eq]: all-bitmap B3 on
 *               pd_bitmap, all-broad find B6 on pd_btree
 *   union     — OR[t-range,t-range] / OR[v-eq,v-eq] on pd_btree/pd_bitmap;
 *               OR[u-eq,u-eq] on pd_comp
 * explain runs per shape every 2nd criterion, alternating find mode and
 * count mode so fetching=0 plans (count-intersect, count leaf) are
 * tallied too; the plan string lands in the coverage table printed at
 * the end ("# PLANPATH" lines).
 *
 * Budgets: SHARD_AUDIT_DIFF_ROWS (default 5000), SHARD_AUDIT_DIFF_CRITERIA
 * (default 30; plan budget 500 × shapes via audit/scripts),
 * SHARD_AUDIT_DIFF_SEED (default 1). Divergences are written to a
 * PER-RUN directory under /tmp/shard-db-audit-logs/ (parallel runs never
 * append to each other's logs).
 *
 * Known-bug gate: deferred — the executor listed candidate fix commits
 * and the human picks the pair (plan Phase 1 step 4, NEEDS-YOU).
 * Harness v1 notes: cursor and join variants are not yet exercised here
 * (recorded as a harness gap in the campaign index; order_by covers the
 * ordered-walk path in this version).
 *
 * KNOWN divergence classes on audit-base (found by this harness on its
 * first tuned smoke, 2026-11-19; full repros + proof traces in
 * docs/plans/audit-artifacts/AUD-QRY-{1,2}-repro.*, findings in the
 * campaign index — Phase-4 fix branches, NOT fixed here):
 *   AUD-QRY-1 — group_by over a bitmap-ONLY-indexed group field with
 *     criteria whose plan is not all-bitmap-eq: the IGB bitmap branch
 *     falls through to a btree walk of a field that has no btree →
 *     silently empty groups (query_aggregate.c:5943/6086/6495).
 *     Signature: group_by section, pd_plain vs pd_bitmap.
 *   AUD-QRY-2 — cmd_aggregate KeySet fast path drops OR sub-trees: the
 *     aw_eligible branch counts |leaf KeySet| without rechecking
 *     has_subtree (query_aggregate.c:5443-5530; the planner discards
 *     the flag at query_plan.c:3638). Signature: aggregate section
 *     (and btree-shape group_by), criteria containing "or".
 * Any divergence in the full-budget run outside these two signatures is
 * NEW signal and must be triaged before the run counts as clean.
 *
 * Audit case: excluded from default run-all (TEST_REGISTER_AUDIT).
 */
#include "test_runner.h"
#include "test_assert.h"
#include "audit_common.h"
#include "fixtures.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define AUD_SHAPES 4
static const char *AUD_SHAPE_OBJ[AUD_SHAPES] = {
    "pd_plain", "pd_btree", "pd_bitmap", "pd_comp"
};
/* bitmap indexes require bool/varchar/enum fields — the bitmap shape
   indexes v and w (varchar) plus a t btree, so every two-leaf AND pair
   that should drive the intersect paths has an index on both leaves.
   The btree shape mirrors that with plain btrees (v, t, w). u stays
   unindexed outside the composite shape so single u-leaves keep driving
   FP_FULL_SCAN there and the composite prefix on pd_comp. */
static const char *AUD_SHAPE_IDX[AUD_SHAPES] = {
    "", "\"v\",\"t\",\"w\"", "\"v:bitmap\",\"w:bitmap\",\"t\"", "\"u+v\""
};

#define AUD_DIFF_MAX_KEYS 8192

typedef struct {
    int n;
    char keys[AUD_DIFF_MAX_KEYS][16];
} AudKeySet;

static int aud_keycmp(const void *a, const void *b) {
    return strcmp((const char *)a, (const char *)b);
}

/* Extract the sorted key set from a find/keys array response. */
static int aud_collect_keys(const char *resp, AudKeySet *ks) {
    ks->n = 0;
    const char *p = resp;
    while (p && (p = strstr(p, "\"key\":\"")) != NULL) {
        p += 7;
        if (ks->n >= AUD_DIFF_MAX_KEYS) return -1;
        size_t n = 0;
        while (*p && *p != '"' && n + 1 < sizeof(ks->keys[0]))
            ks->keys[ks->n][n++] = *p++;
        ks->keys[ks->n][n] = '\0';
        ks->n++;
    }
    qsort(ks->keys, (size_t)ks->n, sizeof(ks->keys[0]), aud_keycmp);
    return 0;
}

static int aud_keyset_diff(const AudKeySet *a, const AudKeySet *b,
                           char *out, size_t cap) {
    if (a->n != b->n) {
        snprintf(out, cap, "size %d vs %d", a->n, b->n);
        return 1;
    }
    for (int i = 0; i < a->n; i++) {
        if (strcmp(a->keys[i], b->keys[i]) != 0) {
            snprintf(out, cap, "first diff at %d: '%s' vs '%s'",
                     i, a->keys[i], b->keys[i]);
            return 1;
        }
    }
    return 0;
}

/* ---- generated data + criteria ---- */

typedef struct {
    int rows;
    uint64_t rng;
} AudDiffCtx;

static void aud_gen_value(char *v, size_t cap, int i) {
    snprintf(v, cap, "cat_%d", i % 5);
}

/* w: 3 categories ≈ rows/3 — far above the N/8 budget, so a lone w-eq
   bitmap leaf is always classified broad (FP_BITMAP_SMALLER driver). */
static void aud_gen_wvalue(char *w, size_t cap, int i) {
    snprintf(w, cap, "w_%d", i % 3);
}

/* Generate one criteria JSON string. Criteria shapes (each a structural
   driver for one or more plan paths — see the file header table):
   single leaf, AND[t-range,t-range], AND[v-eq,w-eq], AND[u-eq,v-eq],
   OR[v-eq,v-eq], OR[u-eq,u-eq], AND+OR hybrid. All leaves are eq /
   between / in — the rangeable ops the intersect planner requires. */
static void aud_gen_criteria(AudDiffCtx *c, char *buf, size_t cap) {
    const char *vvals[5] = { "cat_0", "cat_1", "cat_2", "cat_3", "cat_4" };
    const char *wvals[3] = { "w_0", "w_1", "w_2" };
    char l1[192], l2[192];
    switch (aud_rng_below(&c->rng, 4)) {
    case 0: /* v-eq: broad on btree + bitmap (rows/5 > N/8 budget) */
        snprintf(l1, sizeof(l1),
                 "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"%s\"}",
                 vvals[aud_rng_below(&c->rng, 5)]);
        break;
    case 1: /* w-eq: very broad bitmap leaf (rows/3) */
        snprintf(l1, sizeof(l1),
                 "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"%s\"}",
                 wvals[aud_rng_below(&c->rng, 3)]);
        break;
    case 2: { /* u leaf: composite-prefix driver on pd_comp, scan elsewhere */
        int u = (int)aud_rng_below(&c->rng, 13);
        switch (aud_rng_below(&c->rng, 3)) {
        case 0:
            snprintf(l1, sizeof(l1),
                     "{\"field\":\"u\",\"op\":\"eq\",\"value\":%d}", u);
            break;
        case 1:
            snprintf(l1, sizeof(l1),
                     "{\"field\":\"u\",\"op\":\"between\",\"value\":%d,"
                     "\"value2\":%d}", u, u + 3);
            break;
        default:
            snprintf(l1, sizeof(l1),
                     "{\"field\":\"u\",\"op\":\"in\",\"value\":[%d,%d,%d]}",
                     u, (u + 4) % 13, (u + 8) % 13);
            break;
        }
        break;
    }
    default: { /* t range: selective (26 of rows) */
        int lo = (int)aud_rng_below(&c->rng, (uint32_t)(c->rows - 30));
        snprintf(l1, sizeof(l1),
                 "{\"field\":\"t\",\"op\":\"between\",\"value\":%d,"
                 "\"value2\":%d}", lo, lo + 25);
        break;
    }
    }
    switch (aud_rng_below(&c->rng, 7)) {
    case 0: /* single leaf */
        snprintf(buf, cap, "[%s]", l1);
        break;
    case 1: { /* two selective t-ranges as separate implicit-AND elements
                 → B1 count-intersect. Ranges are nested so the
                 intersection is non-empty (empty results would agree
                 across shapes and hide plan divergences). NOTE: explicit
                 {\"and\":[...]} as the sole list element does NOT
                 flatten — the planner then sees no AND-leaves and scans
                 (correct results, suboptimal plan; see campaign index
                 AUD-PLN-2 note). */
        int lo1 = (int)aud_rng_below(&c->rng, (uint32_t)(c->rows - 30));
        snprintf(buf, cap,
                 "[{\"field\":\"t\",\"op\":\"between\",\"value\":%d,"
                 "\"value2\":%d},{\"field\":\"t\",\"op\":\"between\","
                 "\"value\":%d,\"value2\":%d}]",
                 lo1, lo1 + 25, lo1 + 5, lo1 + 20);
        break;
    }
    case 2: /* [v-eq, w-eq] as two implicit-AND elements: B3 all-bitmap
               intersect on pd_bitmap, all-broad find-intersect B6 on
               pd_btree */
        snprintf(buf, cap,
                 "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"%s\"},"
                 "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"%s\"}]",
                 vvals[aud_rng_below(&c->rng, 5)],
                 wvals[aud_rng_below(&c->rng, 3)]);
        break;
    case 3: /* [u-eq, v-eq] as two implicit-AND elements: composite-exact
               leaf on pd_comp */
        snprintf(buf, cap,
                 "[{\"field\":\"u\",\"op\":\"eq\",\"value\":%d},"
                 "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"%s\"}]",
                 (int)aud_rng_below(&c->rng, 13),
                 vvals[aud_rng_below(&c->rng, 5)]);
        break;
    case 4: /* OR[v-eq,v-eq]: union on pd_btree/pd_bitmap */
        snprintf(buf, cap,
                 "[{\"or\":[{\"field\":\"v\",\"op\":\"eq\","
                 "\"value\":\"%s\"},{\"field\":\"v\",\"op\":\"eq\","
                 "\"value\":\"%s\"}]}]",
                 vvals[aud_rng_below(&c->rng, 5)],
                 vvals[aud_rng_below(&c->rng, 5)]);
        break;
    case 5: /* OR[u-eq,u-eq]: union on pd_comp */
        snprintf(buf, cap,
                 "[{\"or\":[{\"field\":\"u\",\"op\":\"eq\",\"value\":%d},"
                 "{\"field\":\"u\",\"op\":\"eq\",\"value\":%d}]}]",
                 (int)aud_rng_below(&c->rng, 13),
                 (int)aud_rng_below(&c->rng, 13));
        break;
    default: /* hybrid: [leaf, OR[v-eq, u-between]] — implicit AND with a
               leaf + OR sub-tree (C3: indexed seed + full-tree recheck) */
        snprintf(l2, sizeof(l2),
                 "{\"field\":\"u\",\"op\":\"between\",\"value\":%d,"
                 "\"value2\":%d}", (int)aud_rng_below(&c->rng, 4),
                 8 + (int)aud_rng_below(&c->rng, 5));
        snprintf(buf, cap,
                 "[%s,{\"or\":[{\"field\":\"v\",\"op\":\"eq\","
                 "\"value\":\"cat_2\"},%s]}]", l1, l2);
        break;
    }
}

static int audit_planner_diff_run(void) {
    /* 5000 rows: small enough for a fast smoke, big enough that
       selective indexed leaves (t eq = 1 row, u eq ≈ rows/13) beat the
       scan cost model (matches < live_rows / RANDOM_SEQ_COST_RATIO). */
    int rows = aud_env_int("SHARD_AUDIT_DIFF_ROWS", 5000);
    int n_crit = aud_env_int("SHARD_AUDIT_DIFF_CRITERIA", 30);
    int seed = aud_env_int("SHARD_AUDIT_DIFF_SEED", 1);
    if (rows > AUD_DIFF_MAX_KEYS) rows = AUD_DIFF_MAX_KEYS;

    mkdir("/tmp/shard-db-audit-logs", 0755);
    /* Per-run log directory: parallel or back-to-back runs never append
       into each other's divergence logs. */
    char logdir[160];
    snprintf(logdir, sizeof(logdir), "/tmp/shard-db-audit-logs/plandiff-%ld-%d",
             (long)time(NULL), (int)getpid());
    mkdir(logdir, 0755);
    char logpath[256];
    snprintf(logpath, sizeof(logpath), "%s/diff.log", logdir);
    FILE *logf = fopen(logpath, "w");
    if (!logf) logf = stderr;

    char base[256], db_root[512];
    snprintf(base, sizeof(base), "/tmp/shard-db-auditdiff-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    char cmd[600];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", base);
    if (system(cmd) != 0) { /* best effort */ }
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) {
        ASSERT_TRUE(0, "audit-planner-diff: pick port");
        if (logf != stderr) fclose(logf);
        return 1;
    }
    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "audit-planner-diff: daemon spawn");
        snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", base);
        (void)system(cmd);
        if (logf != stderr) fclose(logf);
        return 1;
    }
    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        ASSERT_TRUE(0, "audit-planner-diff: connect");
        goto fail_env;
    }

    /* Setup: dir + twins with per-shape indexes. */
    char req[1024], *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    for (int s = 0; s < AUD_SHAPES; s++) {
        if (AUD_SHAPE_IDX[s][0] == '\0')
            snprintf(req, sizeof(req),
                     "{\"mode\":\"create-object\",\"dir\":\"default\","
                     "\"object\":\"%s\",\"splits\":8,\"max_key\":16,"
                     "\"fields\":[\"v:varchar:48\",\"u:int\",\"t:long\","
                     "\"w:varchar:16\"]}",
                     AUD_SHAPE_OBJ[s]);
        else
            snprintf(req, sizeof(req),
                     "{\"mode\":\"create-object\",\"dir\":\"default\","
                     "\"object\":\"%s\",\"splits\":8,\"max_key\":16,"
                     "\"fields\":[\"v:varchar:48\",\"u:int\",\"t:long\","
                     "\"w:varchar:16\"],"
                     "\"indexes\":[%s]}",
                     AUD_SHAPE_OBJ[s], AUD_SHAPE_IDX[s]);
        int rc = tc_request(tc, req, &resp);
        if (rc != 0 || !resp || strstr(resp, "\"error\"")) {
            fprintf(logf, "FATAL: create-object %s failed: %.200s\n",
                    AUD_SHAPE_OBJ[s], resp ? resp : "(no response)");
            goto fail_env;
        }
        free(resp); resp = NULL;
    }

    /* Load identical rows into every twin (bulk chunks: the request is
       ~80 bytes/record, so the buffer must hold 50 comfortably). */
    {
        char bulkreq[16384];
        for (int s = 0; s < AUD_SHAPES; s++) {
            for (int base_i = 0; base_i < rows; base_i += 50) {
                int chunk = rows - base_i < 50 ? rows - base_i : 50;
                int off = snprintf(bulkreq, sizeof(bulkreq),
                                   "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                                   "\"object\":\"%s\",\"records\":[",
                                   AUD_SHAPE_OBJ[s]);
                for (int j = 0; j < chunk; j++) {
                    int i = base_i + j;
                    char v[32], w[16];
                    aud_gen_value(v, sizeof(v), i);
                    aud_gen_wvalue(w, sizeof(w), i);
                    off += snprintf(bulkreq + off, sizeof(bulkreq) - (size_t)off,
                                    "%s{\"key\":\"k%06d\",\"value\":{\"v\":\"%s\","
                                    "\"u\":%d,\"t\":%d,\"w\":\"%s\"}}",
                                    j ? "," : "", i, v, i % 13, i, w);
                }
                off += snprintf(bulkreq + off, sizeof(bulkreq) - (size_t)off, "]}");
                int rc = tc_request(tc, bulkreq, &resp);
                if (rc != 0 || !resp || strstr(resp, "\"error\"")) {
                    fprintf(logf, "FATAL: bulk-insert %s chunk %d failed: %.200s\n",
                            AUD_SHAPE_OBJ[s], base_i, resp ? resp : "(none)");
                    goto fail_env;
                }
                free(resp); resp = NULL;
            }
        }
    }

    /* Differential loop. */
    AudDiffCtx genc = { .rows = rows,
                        .rng = (uint64_t)seed * 88172645463325252ULL + 7ULL };
    int failed = 0;
    int div_sec[5]; /* divergences per section: count/find/agg/group/order */
    memset(div_sec, 0, sizeof(div_sec));
    int planpath[5][AUD_SHAPES]; /* scan/leaf/bitmap/intersect/union × shape */
    memset(planpath, 0, sizeof(planpath));
    static const char *plan_names[5] = {
        "scan", "leaf", "bitmap", "intersect", "union"
    };
    char crit[1024];
    AudKeySet ks[AUD_SHAPES];
    for (int ci = 0; ci < n_crit; ci++) {
        aud_gen_criteria(&genc, crit, sizeof(crit));

        /* count equality across shapes */
        int counts[AUD_SHAPES];
        for (int s = 0; s < AUD_SHAPES; s++) {
            snprintf(req, sizeof(req),
                     "{\"mode\":\"count\",\"dir\":\"default\","
                     "\"object\":\"%s\",\"criteria\":%s}",
                     AUD_SHAPE_OBJ[s], crit);
            int rc = tc_request(tc, req, &resp);
            counts[s] = (rc == 0 && resp) ? atoi(resp) : -1;
            free(resp); resp = NULL;
            if (counts[s] < 0) {
                fprintf(logf, "criterion %d: count request failed on %s\n",
                        ci, AUD_SHAPE_OBJ[s]);
                failed++;
                goto next_crit;
            }
        }
        for (int s = 1; s < AUD_SHAPES; s++) {
            if (counts[s] != counts[0]) {
                fprintf(logf,
                        "DIVERGENCE criterion %d count: %s=%d %s=%d %s=%d "
                        "%s=%d criteria=%s\n",
                        ci, AUD_SHAPE_OBJ[0], counts[0], AUD_SHAPE_OBJ[1],
                        counts[1], AUD_SHAPE_OBJ[2], counts[2],
                        AUD_SHAPE_OBJ[3], counts[3], crit);
                failed++;
                div_sec[0]++;
                break;
            }
        }

        /* find key-set equality across shapes (key is present in every
           element; no value projection needed) */
        for (int s = 0; s < AUD_SHAPES; s++) {
            snprintf(req, sizeof(req),
                     "{\"mode\":\"find\",\"dir\":\"default\","
                     "\"object\":\"%s\",\"criteria\":%s,"
                     "\"offset\":0,\"limit\":%d}",
                     AUD_SHAPE_OBJ[s], crit, rows + 16);
            int rc = tc_request(tc, req, &resp);
            if (rc != 0 || !resp || aud_collect_keys(resp, &ks[s]) != 0) {
                fprintf(logf,
                        "criterion %d: find request failed on %s (%.120s)\n",
                        ci, AUD_SHAPE_OBJ[s], resp ? resp : "(none)");
                failed++;
                free(resp); resp = NULL;
                goto next_crit;
            }
            free(resp); resp = NULL;
        }
        for (int s = 1; s < AUD_SHAPES; s++) {
            char why[128];
            if (aud_keyset_diff(&ks[0], &ks[s], why, sizeof(why))) {
                fprintf(logf,
                        "DIVERGENCE criterion %d find: %s vs %s (%s) "
                        "criteria=%s\n",
                        ci, AUD_SHAPE_OBJ[0], AUD_SHAPE_OBJ[s], why, crit);
                failed++;
                div_sec[1]++;
            }
        }

        /* aggregate scalar equality (count + sum(t) + max(t)) */
        {
            char agg_out[AUD_SHAPES][256];
            for (int s = 0; s < AUD_SHAPES; s++) {
                snprintf(req, sizeof(req),
                         "{\"mode\":\"aggregate\",\"dir\":\"default\","
                         "\"object\":\"%s\",\"criteria\":%s,"
                         "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"},"
                         "{\"fn\":\"sum\",\"field\":\"t\",\"alias\":\"s\"},"
                         "{\"fn\":\"max\",\"field\":\"t\",\"alias\":\"m\"}]}",
                         AUD_SHAPE_OBJ[s], crit);
                int rc = tc_request(tc, req, &resp);
                snprintf(agg_out[s], sizeof(agg_out[s]), "%s",
                         (rc == 0 && resp) ? resp : "(fail)");
                free(resp); resp = NULL;
            }
            for (int s = 1; s < AUD_SHAPES; s++) {
                if (strcmp(agg_out[0], agg_out[s]) != 0) {
                    fprintf(logf,
                            "DIVERGENCE criterion %d aggregate: %s gave "
                            "%.100s but %s gave %.100s criteria=%s\n",
                            ci, AUD_SHAPE_OBJ[0], agg_out[0],
                            AUD_SHAPE_OBJ[s], agg_out[s], crit);
                    failed++;
                    div_sec[2]++;
                }
            }
        }

        /* aggregate group_by=v: sorted group-set equality via count-vec */
        {
            /* v has 7 labels; extract the 7 counts per shape from the
               group array and compare shape 0 vs others positionally. The
               response order is unspecified, so collect counts by label. */
            int gcount[AUD_SHAPES][7];
            memset(gcount, 0xff, sizeof(gcount)); /* -1 fill */
            for (int s = 0; s < AUD_SHAPES; s++) {
                snprintf(req, sizeof(req),
                         "{\"mode\":\"aggregate\",\"dir\":\"default\","
                         "\"object\":\"%s\",\"criteria\":%s,"
                         "\"group_by\":\"v\","
                         "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}",
                         AUD_SHAPE_OBJ[s], crit);
                int rc = tc_request(tc, req, &resp);
                if (rc == 0 && resp) {
                    const char *p = resp;
                    while ((p = strstr(p, "\"v\":\"cat_")) != NULL) {
                        int label = atoi(p + 9);
                        const char *np = strstr(p, "\"n\":");
                        if (label >= 0 && label < 7 && np)
                            gcount[s][label] = atoi(np + 4);
                        p += 9;
                    }
                }
                free(resp); resp = NULL;
            }
            for (int s = 1; s < AUD_SHAPES; s++) {
                if (memcmp(gcount[0], gcount[s], sizeof(gcount[0])) != 0) {
                    fprintf(logf,
                            "DIVERGENCE criterion %d group_by: %s vs %s "
                            "group counts differ criteria=%s\n",
                            ci, AUD_SHAPE_OBJ[0], AUD_SHAPE_OBJ[s], crit);
                    failed++;
                    div_sec[3]++;
                }
            }
        }

        /* order_by=t sequence equality (every 5th criterion) */
        if (ci % 5 == 0) {
            for (int s = 0; s < AUD_SHAPES; s++) {
                snprintf(req, sizeof(req),
                         "{\"mode\":\"find\",\"dir\":\"default\","
                         "\"object\":\"%s\",\"criteria\":%s,"
                         "\"order_by\":\"t\",\"offset\":0,\"limit\":%d}",
                         AUD_SHAPE_OBJ[s], crit, rows + 16);
                int rc = tc_request(tc, req, &resp);
                if (rc != 0 || !resp || aud_collect_keys(resp, &ks[s]) != 0) {
                    fprintf(logf,
                            "criterion %d: order_by find failed on %s\n",
                            ci, AUD_SHAPE_OBJ[s]);
                    failed++;
                    free(resp); resp = NULL;
                    goto next_crit;
                }
                free(resp); resp = NULL;
            }
            for (int s = 1; s < AUD_SHAPES; s++) {
                char why[128];
                if (aud_keyset_diff(&ks[0], &ks[s], why, sizeof(why))) {
                    fprintf(logf,
                            "DIVERGENCE criterion %d order_by=t: %s vs %s "
                            "(%s) criteria=%s\n",
                            ci, AUD_SHAPE_OBJ[0], AUD_SHAPE_OBJ[s], why, crit);
                    failed++;
                    div_sec[4]++;
                }
            }
        }

        /* explain per shape: tally plan paths. Every 2nd criterion;
           find mode and count mode alternate so both fetching states
           are tallied (B1 count-intersect and the count-side broad-leaf
           decisions only exist when fetching=0). */
        if (ci % 2 == 0) {
            for (int s = 0; s < AUD_SHAPES; s++) {
                snprintf(req, sizeof(req),
                         "{\"mode\":\"%s\",\"dir\":\"default\","
                         "\"object\":\"%s\",\"criteria\":%s,"
                         "\"explain\":true}",
                         (ci & 2) ? "find" : "count",
                         AUD_SHAPE_OBJ[s], crit);
                int rc = tc_request(tc, req, &resp);
                if (rc == 0 && resp) {
                    for (int k = 0; k < 5; k++) {
                        char pat[32];
                        snprintf(pat, sizeof(pat), "\"plan\":\"%s\"",
                                 plan_names[k]);
                        if (strstr(resp, pat)) { planpath[k][s]++; break; }
                    }
                }
                free(resp); resp = NULL;
            }
        }

next_crit:;
    }

    /* Coverage table — which plan paths the data shapes reached. */
    for (int k = 0; k < 5; k++) {
        TAP_DIAG("# PLANPATH %-10s plain=%d btree=%d bitmap=%d composite=%d\n",
                 plan_names[k], planpath[k][0], planpath[k][1],
                 planpath[k][2], planpath[k][3]);
        fprintf(logf, "PLANPATH %s plain=%d btree=%d bitmap=%d composite=%d\n",
                plan_names[k], planpath[k][0], planpath[k][1],
                planpath[k][2], planpath[k][3]);
    }
    TAP_DIAG("# DIVSEC count=%d find=%d agg=%d group_by=%d order_by=%d\n",
             div_sec[0], div_sec[1], div_sec[2], div_sec[3], div_sec[4]);
    fprintf(logf, "DIVSEC count=%d find=%d agg=%d group_by=%d order_by=%d\n",
            div_sec[0], div_sec[1], div_sec[2], div_sec[3], div_sec[4]);
    fprintf(logf, "audit-planner-diff seed %d: %d divergences over %d "
            "criteria, %d rows\n", seed, failed, n_crit, rows);

    tc_close(tc);
    test_env_stop(&env);
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", base);
    (void)system(cmd);
    if (logf != stderr) fclose(logf);

    {
        char desc[160];
        snprintf(desc, sizeof(desc),
                 "audit-planner-diff: %d divergences over %d criteria × %d "
                 "shapes (log: %s)",
                 failed, n_crit, AUD_SHAPES, logdir);
        ASSERT_EQ_INT(failed, 0, desc);
    }
    return t_ctx->failed > 0 ? 1 : 0;

fail_env:
    if (tc) tc_close(tc);
    test_env_kill(&env);
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", base);
    (void)system(cmd);
    if (logf != stderr) fclose(logf);
    ASSERT_TRUE(0, "audit-planner-diff: environment failure (see log)");
    return 1;
}

TEST_REGISTER_AUDIT("audit-planner-diff", audit_planner_diff_run)
