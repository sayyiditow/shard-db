/* src/test/cases/test_kef_batches.c
 *
 * Unordered indexed find fetches only the record batches its page needs. Drives keyset_emit_find directly on test-built
 * KeySets (deterministic bucket order) and end-to-end through find:
 *
 *   1. early stop after the page fills          (batches + counters)
 *   2. stale candidates with an offset          (missing/deleted candidates)
 *   3. exclusions and joins rejecting a batch   (batch-boundary placement)
 *   4. byte-budget halving and oversized record (QUERY_BUFFER_MB site)
 *   5. fault injection                          (-1/errno, never short success)
 *   6. differential vs the retained reference   (byte-identical pages)
 *   7. wide splits with a running IO pool       (concurrent callbacks)
 *   8. real find routes                         (rows + left join)
 *   9. A3 trigram starts_with route             (unterminated page on failure)
 *
 * Counters/seams are process-wide (kef_test_reset before each measured call
 * and on every exit path, so nothing survives into later cases under
 * run-all --jobs 1). Cases 6 and 7 start the IO pool so fetch callbacks run
 * concurrently; cases 1-5 assert the pool is not running, so their callbacks
 * run inline (deterministic seam hit order and exact copy counts).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "bappend.h"
#include "test_assert.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/query_internal.h"
#include "../db/shard_db.h"
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char CRIT_N1[] = "[{\"field\":\"n\",\"op\":\"eq\",\"value\":\"1\"}]";

/* ---- fixture ---- */

typedef struct { uint8_t h[16]; int i; } KHash;
typedef int (*KefKindFn)(int ord);           /* 0 live, 1 never inserted, 2 inserted then deleted */
typedef int (*KefNFn)(int ord);
typedef const char *(*KefRefFn)(int ord);    /* NULL -> JSON null */
typedef struct {
    ShardDb *db; char eff[600]; char obj[24]; KeySet *ks;
    int ncand; int *idx_of_ord; int remote;
} KefFix;

static void kef_idx_key(int i, char buf[8]) { snprintf(buf, 8, "k%04d", i); }
static void kef_ord_key(const KefFix *f, int o, char buf[8]) { kef_idx_key(f->idx_of_ord[o], buf); }
static int kef_has(const char *text, const KefFix *f, int o) {
    char buf[8], pat[16];
    kef_ord_key(f, o, buf);
    snprintf(pat, sizeof(pat), "\"%s\"", buf);
    return text && strstr(text, pat) != NULL;
}

static int kef_khash_cmp(const void *a, const void *b) {
    return memcmp(((const KHash *)a)->h, ((const KHash *)b)->h, 16);
}
static const KHash *kef_khash_find(const KHash *arr, int n, const uint8_t hash[16]) {
    int lo = 0, hi = n;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        int c = memcmp(hash, arr[mid].h, 16);
        if (c == 0) return &arr[mid];
        if (c < 0) hi = mid; else lo = mid + 1;
    }
    return NULL;
}

/* Build the object, the remote object (optional), a KeySet holding every
 * candidate hash, the ordinal map (bucket order) and the record bodies.
 * Returns non-zero on failed setup (the assertion already fired). */
static int kef_fix_build(KefFix *f, const char *obj, int splits, int ncand,
                         KefKindFn kind, KefNFn nval, KefRefFn refv, int remote) {
    memset(f, 0, sizeof(*f));
    f->ncand = ncand;
    f->remote = remote;
    snprintf(f->obj, sizeof(f->obj), "%s", obj);
    f->db = test_get_process_db();
    ASSERT_NOT_NULL(f->db, "setup: process db");
    if (!f->db) return 1;
    snprintf(f->eff, sizeof(f->eff), "%s/default", test_get_process_db_root());

    char msg[160], co[512], needle[64];
    char *resp = NULL;
    tu_pdb_request(f->db, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    shard_db_free_result(resp); resp = NULL;

    snprintf(co, sizeof(co),
             "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
             "\"splits\":%d,\"max_key\":16,"
             "\"fields\":[\"n:int\",\"tag:varchar:8\",\"ref:varchar:8\"],"
             "\"indexes\":[\"tag\"]}", obj, splits);
    tu_pdb_request(f->db, co, &resp);
    snprintf(msg, sizeof(msg), "setup: create %s", obj);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), msg);
    if (!resp || strstr(resp, "\"error\"")) { shard_db_free_result(resp); return 1; }
    shard_db_free_result(resp); resp = NULL;

    if (remote) {
        char robj[32], req[512];
        snprintf(robj, sizeof(robj), "%s_r", obj);
        snprintf(req, sizeof(req),
                 "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
                 "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:8\"]}", robj);
        tu_pdb_request(f->db, req, &resp);
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "setup: remote");
        if (!resp || strstr(resp, "\"error\"")) { shard_db_free_result(resp); return 1; }
        shard_db_free_result(resp); resp = NULL;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
                 "\"key\":\"r1\",\"value\":{\"name\":\"x\"}}", robj);
        tu_pdb_request(f->db, req, &resp);
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "setup: remote");
        if (!resp || strstr(resp, "\"error\"")) { shard_db_free_result(resp); return 1; }
        shard_db_free_result(resp); resp = NULL;
    }

    f->ks = keyset_new((size_t)ncand);
    ASSERT_NOT_NULL(f->ks, "setup: keyset");
    if (!f->ks) return 1;
    f->idx_of_ord = malloc((size_t)ncand * sizeof(int));
    ASSERT_NOT_NULL(f->idx_of_ord, "setup: ordinal map");
    if (!f->idx_of_ord) return 1;

    KHash *arr = malloc((size_t)ncand * sizeof(KHash));
    ASSERT_NOT_NULL(arr, "setup: hash table");
    if (!arr) return 1;
    char kb[8];
    for (int i = 0; i < ncand; i++) {
        kef_idx_key(i, kb);
        compute_hash_raw(kb, 5, arr[i].h);
        arr[i].i = i;
        keyset_insert(f->ks, arr[i].h);
    }
    qsort(arr, (size_t)ncand, sizeof(arr[0]), kef_khash_cmp);

    int o = 0;
    for (size_t b = 0; b < f->ks->cap; b++) {
        if (f->ks->state[b] != 2) continue;
        const KHash *ent = kef_khash_find(arr, ncand, f->ks->keys[b]);
        if (!ent) {
            snprintf(msg, sizeof(msg), "setup: %s keyset hash maps to a candidate", obj);
            ASSERT_TRUE(0, msg);
            free(arr);
            return 1;
        }
        f->idx_of_ord[o++] = ent->i;
    }
    snprintf(msg, sizeof(msg), "setup: %s keyset holds every candidate", obj);
    ASSERT_EQ_INT(o, ncand, msg);
    free(arr);
    if (o != ncand) return 1;

    /* Bulk-insert every non-missing ordinal in one request. */
    size_t cap = (size_t)ncand * 96 + 256;
    char *body = malloc(cap);
    ASSERT_NOT_NULL(body, "setup: bulk buffer");
    if (!body) return 1;
    size_t p = (size_t)snprintf(body, cap, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                                "\"object\":\"%s\",\"records\":[", obj);
    int first = 1, nins = 0;
    char refjson[16];
    for (int ord = 0; ord < ncand; ord++) {
        if (kind && kind(ord) == 1) continue;
        kef_ord_key(f, ord, kb);
        const char *rv = refv ? refv(ord) : "zz";
        snprintf(refjson, sizeof(refjson), rv ? "\"%s\"" : "null", rv ? rv : "");
        p = tappendf(body, cap, p,
                              "%s{\"key\":\"%s\",\"value\":{\"n\":%d,\"tag\":\"hot\",\"ref\":%s}}",
                              first ? "" : ",", kb, nval ? nval(ord) : 1, refjson);
        first = 0;
        nins++;
    }
    p = tappendf(body, cap, p, "]}");
    tu_pdb_request(f->db, body, &resp);
    snprintf(msg, sizeof(msg), "setup: %s bulk insert", obj);
    snprintf(needle, sizeof(needle), "\"inserted\":%d", nins);
    ASSERT_TRUE(resp && strstr(resp, needle) != NULL, msg);
    int bad = !resp || strstr(resp, needle) == NULL;
    shard_db_free_result(resp); resp = NULL;
    free(body);
    if (bad) return 1;

    /* Delete the kind==2 ordinals in one request. */
    int ndel = 0;
    for (int ord = 0; ord < ncand; ord++) if (kind && kind(ord) == 2) ndel++;
    if (ndel > 0) {
        size_t dcap = (size_t)ncand * 12 + 128;
        char *del = malloc(dcap);
        ASSERT_NOT_NULL(del, "setup: delete buffer");
        if (!del) return 1;
        size_t dp = (size_t)snprintf(del, dcap, "{\"mode\":\"bulk-delete\",\"dir\":\"default\","
                                     "\"object\":\"%s\",\"keys\":[", obj);
        int dfirst = 1;
        for (int ord = 0; ord < ncand; ord++) {
            if (!kind || kind(ord) != 2) continue;
            kef_ord_key(f, ord, kb);
            dp = tappendf(del, dcap, dp, "%s\"%s\"", dfirst ? "" : ",", kb);
            dfirst = 0;
        }
        dp = tappendf(del, dcap, dp, "]}");
        tu_pdb_request(f->db, del, &resp);
        snprintf(msg, sizeof(msg), "setup: %s bulk delete", obj);
        snprintf(needle, sizeof(needle), "\"deleted\":%d", ndel);
        ASSERT_TRUE(resp && strstr(resp, needle) != NULL, msg);
        bad = !resp || strstr(resp, needle) == NULL;
        shard_db_free_result(resp); resp = NULL;
        free(del);
        if (bad) return 1;
    }
    return 0;
}

static void kef_fix_free(KefFix *f) {
    if (f->db && f->obj[0]) {
        char robj[32];
        tu_pdb_drop_object(f->db, "default", f->obj);
        if (f->remote) {
            snprintf(robj, sizeof(robj), "%s_r", f->obj);
            tu_pdb_drop_object(f->db, "default", robj);
        }
    }
    keyset_free(f->ks);
    free(f->idx_of_ord);
    memset(f, 0, sizeof(*f));
}

/* Run keyset_emit_find (use_ref) or the reference on `ks`, capturing the
 * emitted rows. Returns an owned text buffer ("" when nothing was emitted). */
static char *kef_run(KefFix *f, KeySet *ks, int use_ref, const char *crit,
                     const char *excl, int off, int lim, const char *fmt,
                     const char *join, uint32_t tmo, int *rc_out, int *err_out) {
    char *text = NULL;
    size_t tlen = 0;
    FILE *ms = open_memstream(&text, &tlen);
    int rc = keyset_emit_find_for_test(f->eff, f->obj, ks, crit, excl, off, lim,
                                       fmt, join, tmo, use_ref, ms);
    int e = errno;
    fclose(ms);
    if (!text) text = strdup("");
    *rc_out = rc;
    *err_out = e;
    return text;
}

static void kef_join_json(const KefFix *f, int left, char *buf, size_t bufsz) {
    snprintf(buf, bufsz,
             "[{\"object\":\"%s_r\",\"local\":\"ref\",\"remote\":\"key\",\"as\":\"r\","
             "\"type\":\"%s\",\"fields\":[\"name\"]}]",
             f->obj, left ? "left" : "inner");
}

/* Same-as-reference condition: equal rc; on failure the same errno; identical bytes. */
static int kef_same_ref(int rc_n, int err_n, const char *out_n,
                        int rc_r, int err_r, const char *out_r) {
    return rc_n == rc_r && (rc_n >= 0 || err_n == err_r) && strcmp(out_n, out_r) == 0;
}

/* ---- per-case record shapes ---- */

static int kfe_kind(int o) { (void)o; return 0; }
static int kfe_n(int o) { return (o >= 1100 && o <= 1199) ? 1 : 0; }
static const char *kfe_ref(int o) { (void)o; return "zz"; }

static int kfs_kind(int o) { return o % 16 == 5 ? 1 : o % 16 == 9 ? 2 : 0; }
static int kfs_n(int o) { (void)o; return 1; }
static const char *kfs_ref(int o) { (void)o; return "zz"; }

static int kfj_kind(int o) { (void)o; return 0; }
static int kfj_n(int o) { (void)o; return 1; }
static const char *kfj_ref(int o) { return o < 1024 ? "zz" : "r1"; }

static int kfb_kind(int o) { (void)o; return 0; }
static int kfb_n(int o) { (void)o; return 1; }
static const char *kfb_ref(int o) { (void)o; return "zz"; }

static int kff_kind(int o) { (void)o; return 0; }
static int kff_n(int o) { (void)o; return 1; }
static const char *kff_ref(int o) { (void)o; return "r1"; }

static int kfd_n(int o) { return o % 3 == 0 ? 1 : 0; }
static const char *kfd_ref(int o) {
    if (o % 5 == 4) return NULL;
    return o % 2 == 0 ? "r1" : "zz";
}

static int kfw_kind(int o) { (void)o; return 0; }
static const char *kfw_ref(int o) { (void)o; return "zz"; }

static int kfr_kind(int o) { (void)o; return 0; }
static int kfr_n(int o) { (void)o; return 1; }
static const char *kfr_ref(int o) { (void)o; return "r1"; }

/* ---- Case 1: early stop after the first batch / exhaustion ---- */

static int test_kef_early_stop_run(void) {
    KefFix f;
    memset(&f, 0, sizeof(f));
    char *out = NULL, *ref = NULL;
    int rc1 = 0, rc2 = 0, e1 = 0, e2 = 0;
    KefTestStats st;
    long B = 0;

    ASSERT_TRUE(parallel_io_pool_size() == 0, "setup: fetch callbacks run inline");
    if (parallel_io_pool_size() != 0) goto cleanup;
    if (kef_fix_build(&f, "kfe", 8, 3000, kfe_kind, kfe_n, kfe_ref, 0)) goto cleanup;

    kef_test_reset();
    out = kef_run(&f, f.ks, 0, CRIT_N1, NULL, 0, 10, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    B = st.copies > 0 ? st.copy_bytes / st.copies : 0;
    ASSERT_EQ_INT(rc1, 10, "early stop: rc is the page size");
    ref = kef_run(&f, f.ks, 1, CRIT_N1, NULL, 0, 10, NULL, NULL, 0, &rc2, &e2);
    ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref),
                "early stop: page equals the reference page");
    ASSERT_TRUE(kef_has(out, &f, 1100), "early stop: page starts at the first survivor");
    ASSERT_TRUE(kef_has(out, &f, 1109) && !kef_has(out, &f, 1110),
                "early stop: page ends at the tenth survivor");
    ASSERT_EQ_INT(st.fetch_calls, 2, "early stop: two fetch calls");
    ASSERT_EQ_INT(st.fetch_hashes, 2048, "early stop: 2048 hashes requested");
    ASSERT_EQ_INT(st.copies, 2048, "early stop: 2048 records copied");
    ASSERT_TRUE(st.copies > 0 && st.copy_bytes % st.copies == 0,
                "setup: uniform record copies");
    ASSERT_EQ_INT(st.peak_bytes, 1024 * B, "early stop: retained copies peak at one batch");
    ASSERT_TRUE(st.live_copies == 0 && st.live_bytes == 0 && st.copies > 0,
                "early stop: no copy outlives the call");
    free(out); out = NULL;
    free(ref); ref = NULL;

    kef_test_reset();
    out = kef_run(&f, f.ks, 0, CRIT_N1, NULL, 0, 101, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_EQ_INT(rc1, 100, "exhaustion: every survivor returned");
    ref = kef_run(&f, f.ks, 1, CRIT_N1, NULL, 0, 101, NULL, NULL, 0, &rc2, &e2);
    ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref),
                "exhaustion: page equals the reference page");
    ASSERT_EQ_INT(st.fetch_calls, 3, "exhaustion: all three batches fetched");

cleanup:
    kef_test_reset();
    kef_fix_free(&f);
    free(out);
    free(ref);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- Case 2: stale (missing/deleted) candidates with an offset ---- */

static int test_kef_stale_candidates_run(void) {
    KefFix f;
    memset(&f, 0, sizeof(f));
    char *out = NULL, *ref = NULL;
    int rc1 = 0, rc2 = 0, e1 = 0, e2 = 0;
    KefTestStats st;

    ASSERT_TRUE(parallel_io_pool_size() == 0, "setup: fetch callbacks run inline");
    if (parallel_io_pool_size() != 0) goto cleanup;
    if (kef_fix_build(&f, "kfs", 8, 3200, kfs_kind, kfs_n, kfs_ref, 0)) goto cleanup;

    kef_test_reset();
    out = kef_run(&f, f.ks, 0, NULL, NULL, 1000, 100, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_EQ_INT(rc1, 100, "stale: rc is the page size");
    ref = kef_run(&f, f.ks, 1, NULL, NULL, 1000, 100, NULL, NULL, 0, &rc2, &e2);
    ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref),
                "stale: page equals the reference page");
    ASSERT_TRUE(kef_has(out, &f, 1143) && !kef_has(out, &f, 1142),
                "stale: page starts at survivor 1001");
    ASSERT_EQ_INT(st.fetch_calls, 2, "stale: page filled in the second batch");
    ASSERT_EQ_INT(st.fetch_hashes, 2048, "stale: 2048 hashes requested");
    ASSERT_EQ_INT(st.copies, 1792, "stale: only live candidates of two batches copied");
    ASSERT_TRUE(st.live_copies == 0 && st.live_bytes == 0 && st.copies > 0,
                "stale: no copy outlives the call");

cleanup:
    kef_test_reset();
    kef_fix_free(&f);
    free(out);
    free(ref);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- Case 3: exclusions and joins rejecting a whole batch ---- */

static int test_kef_exclusions_joins_run(void) {
    KefFix f;
    memset(&f, 0, sizeof(f));
    char *out = NULL, *ref = NULL;
    char *excl = NULL, JI[256], JL[256];
    int rc1 = 0, rc2 = 0, e1 = 0, e2 = 0;
    KefTestStats st;

    ASSERT_TRUE(parallel_io_pool_size() == 0, "setup: fetch callbacks run inline");
    if (parallel_io_pool_size() != 0) goto cleanup;
    if (kef_fix_build(&f, "kfj", 8, 3000, kfj_kind, kfj_n, kfj_ref, 1)) goto cleanup;
    kef_join_json(&f, 0, JI, sizeof(JI));
    kef_join_json(&f, 1, JL, sizeof(JL));

    excl = malloc((size_t)1024 * 8 + 16);
    ASSERT_NOT_NULL(excl, "setup: exclusion csv");
    if (!excl) goto cleanup;
    {
        size_t p = 0, cap = (size_t)1024 * 8 + 16;
        char kb[8];
        excl[0] = '\0';
        for (int o = 0; o <= 1023; o++) {
            kef_ord_key(&f, o, kb);
            p = tappendf(excl, cap, p, "%s%s", o ? "," : "", kb);
        }
    }

    kef_test_reset();
    out = kef_run(&f, f.ks, 0, NULL, excl, 0, 5, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_EQ_INT(rc1, 5, "exclusions: rc is the page size");
    ref = kef_run(&f, f.ks, 1, NULL, excl, 0, 5, NULL, NULL, 0, &rc2, &e2);
    ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref),
                "exclusions: page equals the reference page");
    ASSERT_TRUE(kef_has(out, &f, 1024), "exclusions: page starts after the excluded batch");
    ASSERT_EQ_INT(st.fetch_calls, 2, "exclusions: two fetch calls");
    free(out); out = NULL;
    free(ref); ref = NULL;

    kef_test_reset();
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 5, NULL, JI, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_EQ_INT(rc1, 5, "inner join: rc is the page size");
    ref = kef_run(&f, f.ks, 1, NULL, NULL, 0, 5, NULL, JI, 0, &rc2, &e2);
    ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref),
                "inner join: page equals the reference page");
    ASSERT_TRUE(kef_has(out, &f, 1024), "inner join: page starts in the second batch");
    ASSERT_EQ_INT(st.fetch_calls, 2, "inner join: two fetch calls");
    free(out); out = NULL;
    free(ref); ref = NULL;

    kef_test_reset();
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 5, NULL, JL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_EQ_INT(rc1, 5, "left join: rc is the page size");
    ref = kef_run(&f, f.ks, 1, NULL, NULL, 0, 5, NULL, JL, 0, &rc2, &e2);
    ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref),
                "left join: page equals the reference page");
    ASSERT_TRUE(kef_has(out, &f, 0), "left join: misses stay eligible");
    ASSERT_EQ_INT(st.fetch_calls, 1, "left join: one fetch call");

cleanup:
    kef_test_reset();
    kef_fix_free(&f);
    free(out);
    free(ref);
    free(excl);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- Case 4: byte-budget halving and the oversized record ---- */

static int test_kef_byte_budget_run(void) {
    KefFix f;
    memset(&f, 0, sizeof(f));
    char *out = NULL, *ref = NULL;
    int rc1 = 0, rc2 = 0, e1 = 0, e2 = 0;
    KefTestStats st;
    long B = 0;

    ASSERT_TRUE(parallel_io_pool_size() == 0, "setup: fetch callbacks run inline");
    if (parallel_io_pool_size() != 0) goto cleanup;
    if (kef_fix_build(&f, "kfb", 8, 3000, kfb_kind, kfb_n, kfb_ref, 0)) goto cleanup;

    /* Calibrate the per-record copy size. */
    kef_test_reset();
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 1, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_TRUE(st.copies > 0 && st.copy_bytes % st.copies == 0,
                "setup: uniform record copies");
    if (st.copies <= 0 || st.copy_bytes % st.copies != 0) goto cleanup;
    B = st.copy_bytes / st.copies;
    free(out); out = NULL;

    /* Split: 1024 and 512 exceed 300*B; 256 fits; 600 rows need three
     * 256-candidate batches: attempts 1024, 512, 256, 256, 256. */
    kef_test_reset();
    kef_test_arm(KEF_SEAM_BUDGET, 300 * B);
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 600, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    kef_test_reset();
    ref = kef_run(&f, f.ks, 1, NULL, NULL, 0, 600, NULL, NULL, 0, &rc2, &e2);
    ASSERT_EQ_INT(rc1, 600, "split: rc is the page size");
    ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref),
                "split: page equals the reference page");
    ASSERT_EQ_INT(st.fetch_calls, 5, "split: five fetch attempts");
    ASSERT_EQ_INT(st.fetch_hashes, 2304, "split: 2304 hashes requested");
    ASSERT_TRUE(st.peak_bytes <= 300 * B, "split: retained copies stay within the budget");
    ASSERT_TRUE(st.live_copies == 0 && st.live_bytes == 0 && st.copies > 0,
                "split: no copy outlives the call");
    free(out); out = NULL;
    free(ref); ref = NULL;

    /* Oversized: budget B-1; halving 1024..1 is 11 attempts, 2047 hashes,
     * then ENOBUFS; every reservation fails before a copy. */
    kef_test_reset();
    kef_test_arm(KEF_SEAM_BUDGET, B - 1);
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 10, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_TRUE(rc1 == -1, "oversized record: rc is -1");
    ASSERT_TRUE(rc1 == -1 && e1 == ENOBUFS, "oversized record: errno is ENOBUFS");
    ASSERT_EQ_INT(st.fetch_calls, 11, "oversized record: eleven fetch attempts");
    ASSERT_EQ_INT(st.fetch_hashes, 2047, "oversized record: 2047 hashes requested");
    ASSERT_TRUE(out && out[0] == '\0', "oversized record: no rows emitted");
    ASSERT_TRUE(st.copies == 0 && st.live_copies == 0 && st.live_bytes == 0,
                "oversized record: nothing was ever copied");

cleanup:
    kef_test_reset();
    kef_fix_free(&f);
    free(out);
    free(ref);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- Case 5: fault injection ---- */

static int test_kef_failure_injection_run(void) {
    KefFix f;
    memset(&f, 0, sizeof(f));
    char *out = NULL, JL[256];
    int rc1 = 0, e1 = 0;
    KefTestStats st;

    ASSERT_TRUE(parallel_io_pool_size() == 0, "setup: fetch callbacks run inline");
    if (parallel_io_pool_size() != 0) goto cleanup;
    if (kef_fix_build(&f, "kff", 8, 3000, kff_kind, kff_n, kff_ref, 1)) goto cleanup;
    kef_join_json(&f, 1, JL, sizeof(JL));

    kef_test_reset();
    kef_test_arm(KEF_SEAM_SETUP_ALLOC, 1);
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 10, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_TRUE(rc1 == -1, "setup alloc: rc is -1");
    ASSERT_TRUE(rc1 == -1 && e1 == ENOMEM, "setup alloc: errno is ENOMEM");
    ASSERT_TRUE(st.fetch_calls == 0 && out && out[0] == '\0',
                "setup alloc: nothing fetched or emitted");
    free(out); out = NULL;

    kef_test_reset();
    kef_test_arm(KEF_SEAM_FETCH, 2);
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 1500, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_TRUE(rc1 == -1, "fetch failure: rc is -1");
    ASSERT_TRUE(rc1 == -1 && e1 == EIO, "fetch failure: errno is EIO");
    ASSERT_TRUE(kef_has(out, &f, 1023), "fetch failure: rows of the first batch stay emitted");
    ASSERT_TRUE(!kef_has(out, &f, 2048), "fetch failure: no row after the failed batch");
    ASSERT_EQ_INT(st.fetch_calls, 2, "fetch failure: two fetch attempts");
    ASSERT_EQ_INT(st.copies, 1024, "fetch failure: only the first batch copied");
    ASSERT_TRUE(st.live_copies == 0 && st.live_bytes == 0 && st.copies > 0,
                "fetch failure: no copy outlives the call");
    free(out); out = NULL;

    kef_test_reset();
    kef_test_arm(KEF_SEAM_JOIN, 3);
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 10, NULL, JL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_TRUE(rc1 == -1, "join failure: rc is -1");
    ASSERT_TRUE(rc1 == -1 && e1 == EIO, "join failure: errno is EIO");
    ASSERT_TRUE(kef_has(out, &f, 1) && !kef_has(out, &f, 2),
                "join failure: rows before the failing join emitted");
    ASSERT_EQ_INT(st.fetch_calls, 1, "join failure: one fetch call");
    ASSERT_EQ_INT(st.copies, 1024, "join failure: only the first batch copied");
    ASSERT_TRUE(st.live_copies == 0 && st.live_bytes == 0 && st.copies > 0,
                "join failure: no copy outlives the call");
    free(out); out = NULL;

    kef_test_reset();
    kef_test_arm(KEF_SEAM_REGISTRY, 1);
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 10, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_TRUE(rc1 == -1, "registry failure: rc is -1");
    ASSERT_TRUE(rc1 == -1 && e1 == EIO, "registry failure: errno is EIO");
    ASSERT_TRUE(st.fetch_calls == 0 && out && out[0] == '\0',
                "registry failure: nothing fetched or emitted");
    free(out); out = NULL;

    kef_test_reset();
    kef_test_arm(KEF_SEAM_TIMEOUT, 1);
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 10, NULL, NULL, 600000, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_EQ_INT(rc1, 0, "timeout: rc is 0");
    ASSERT_TRUE(out && out[0] == '\0', "timeout: nothing emitted");
    ASSERT_EQ_INT(st.fetch_calls, 1, "timeout: one fetch call");
    ASSERT_EQ_INT(st.copies, 1024, "timeout: only the first batch copied");
    ASSERT_TRUE(st.live_copies == 0 && st.live_bytes == 0 && st.copies > 0,
                "timeout: no copy outlives the call");
    free(out); out = NULL;

    kef_test_reset();
    kef_test_arm(KEF_SEAM_COPY_ALLOC, 5);
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 10, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_TRUE(rc1 == -1, "copy alloc: rc is -1");
    ASSERT_TRUE(rc1 == -1 && e1 == ENOMEM, "copy alloc: errno is ENOMEM");
    ASSERT_TRUE(out && out[0] == '\0', "copy alloc: no rows emitted");
    ASSERT_EQ_INT(st.fetch_calls, 1, "copy alloc: one fetch call");
    ASSERT_EQ_INT(st.copies, 4, "copy alloc: copying stopped after the failure");
    ASSERT_TRUE(st.live_copies == 0 && st.live_bytes == 0,
                "copy alloc: no copy outlives the call");

cleanup:
    kef_test_reset();
    kef_fix_free(&f);
    free(out);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- Case 6: differential against the retained reference ---- */

typedef struct {
    const char *label;
    const char *crit;
    int excl_head;          /* 1 = keys of ordinals 0..99 */
    int off, lim;
    const char *fmt;
    int join;               /* 0 none, 1 inner, 2 left */
} DiffRow;

static const DiffRow diff_rows[] = {
    {"D1 all 0+10",       NULL,    0, 0,          10,      NULL,   0},
    {"D2 all 1020+10",    NULL,    0, 1020,       10,      NULL,   0},
    {"D3 all unlimited",  NULL,    0, 0,          0,       NULL,   0},
    {"D4 all tail",       NULL,    0, 2790,       50,      NULL,   0},
    {"D5 offset past end",NULL,    0, 5000,       10,      NULL,   0},
    {"D6 int max",        NULL,    0, INT_MAX-5,  INT_MAX, NULL,   0},
    {"D7 n1 0+10",        CRIT_N1, 0, 0,          10,      NULL,   0},
    {"D8 n1 tail",        CRIT_N1, 0, 900,        40,      NULL,   0},
    {"D9 rows",           NULL,    0, 0,          5,       "rows", 0},
    {"D10 dict",          NULL,    0, 0,          5,       "dict", 0},
    {"D11 csv",           NULL,    0, 0,          5,       "csv",  0},
    {"D12 excluded head", NULL,    1, 0,          5,       NULL,   0},
    {"D13 inner",         NULL,    0, 0,          10,      NULL,   1},
    {"D14 left",          NULL,    0, 0,          10,      NULL,   2},
};

static int test_kef_differential_run(void) {
    KefFix f;
    memset(&f, 0, sizeof(f));
    char *out = NULL, *ref = NULL;
    char *excl_head = NULL, JI[256], JL[256];
    int rc1 = 0, rc2 = 0, e1 = 0, e2 = 0;
    KefTestStats st;
    KeySet *ks2 = NULL;
    int own_pool = parallel_io_pool_size() == 0;
    if (own_pool) parallel_io_pool_init(4);
    ASSERT_TRUE(parallel_io_pool_size() > 0, "setup: IO pool running");
    if (parallel_io_pool_size() <= 0) goto cleanup;

    if (kef_fix_build(&f, "kfd", 8, 3200, kfs_kind, kfd_n, kfd_ref, 1)) goto cleanup;
    kef_join_json(&f, 0, JI, sizeof(JI));
    kef_join_json(&f, 1, JL, sizeof(JL));

    excl_head = malloc((size_t)100 * 8 + 16);
    ASSERT_NOT_NULL(excl_head, "setup: exclusion csv");
    if (!excl_head) goto cleanup;
    {
        size_t p = 0, cap = (size_t)100 * 8 + 16;
        char kb[8];
        excl_head[0] = '\0';
        for (int o = 0; o <= 99; o++) {
            kef_ord_key(&f, o, kb);
            p = tappendf(excl_head, cap, p, "%s%s", o ? "," : "", kb);
        }
    }

    char msg[96];
    for (size_t r = 0; r < sizeof(diff_rows) / sizeof(diff_rows[0]); r++) {
        const DiffRow *row = &diff_rows[r];
        const char *excl = row->excl_head ? excl_head : NULL;
        const char *join_json = row->join == 0 ? NULL : (row->join == 1 ? JI : JL);
        snprintf(msg, sizeof(msg), "differential: %s", row->label);
        kef_test_reset();
        out = kef_run(&f, f.ks, 0, row->crit, excl, row->off, row->lim, row->fmt,
                      join_json, 0, &rc1, &e1);
        kef_test_reset();
        ref = kef_run(&f, f.ks, 1, row->crit, excl, row->off, row->lim, row->fmt,
                      join_json, 0, &rc2, &e2);
        ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref), msg);
        free(out); out = NULL;
        free(ref); ref = NULL;
    }

    ks2 = keyset_new(16);
    ASSERT_NOT_NULL(ks2, "setup: empty keyset");
    if (ks2) {
        kef_test_reset();
        out = kef_run(&f, ks2, 0, NULL, NULL, 0, 10, NULL, NULL, 0, &rc1, &e1);
        kef_test_stats(&st);
        ASSERT_EQ_INT(st.fetch_calls, 0, "empty keyset: nothing fetched");
        kef_test_reset();
        ref = kef_run(&f, ks2, 1, NULL, NULL, 0, 10, NULL, NULL, 0, &rc2, &e2);
        ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref),
                    "differential: D15 empty keyset");
    }

cleanup:
    kef_test_reset();
    if (own_pool) parallel_io_pool_shutdown();
    keyset_free(ks2);
    kef_fix_free(&f);
    free(out);
    free(ref);
    free(excl_head);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- Case 7: wide splits with a running IO pool ---- */

static int test_kef_wide_splits_run(void) {
    KefFix f;
    memset(&f, 0, sizeof(f));
    char *out = NULL, *ref = NULL;
    int rc1 = 0, rc2 = 0, e1 = 0, e2 = 0;
    KefTestStats st;
    long B = 0;
    int own_pool = parallel_io_pool_size() == 0;
    if (own_pool) parallel_io_pool_init(4);
    ASSERT_TRUE(parallel_io_pool_size() > 0, "setup: IO pool running");
    if (parallel_io_pool_size() <= 0) goto cleanup;

    if (kef_fix_build(&f, "kfw", 512, 3000, kfw_kind, kfe_n, kfw_ref, 0)) goto cleanup;

    {
        int in_high = 0;
        for (int o = 0; o < 2048 && o < f.ncand; o++) {
            uint8_t h[16];
            char kb[8];
            kef_ord_key(&f, o, kb);
            compute_hash_raw(kb, 5, h);
            if (compute_record_shard(h, 512) >= 256) in_high++;
        }
        ASSERT_TRUE(in_high > 0, "setup: candidates in kf shards >= 256");
        if (in_high <= 0) goto cleanup;
    }

    kef_test_reset();
    out = kef_run(&f, f.ks, 0, CRIT_N1, NULL, 0, 10, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    ASSERT_EQ_INT(rc1, 10, "wide splits: rc is the page size");
    ref = kef_run(&f, f.ks, 1, CRIT_N1, NULL, 0, 10, NULL, NULL, 0, &rc2, &e2);
    ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref),
                "wide splits: page equals the reference page");
    ASSERT_EQ_INT(st.fetch_calls, 2, "wide splits: two fetch calls");
    ASSERT_TRUE(st.live_copies == 0 && st.live_bytes == 0 && st.copies > 0,
                "wide splits: no copy outlives the call");
    ASSERT_TRUE(st.copies > 0 && st.copy_bytes % st.copies == 0,
                "setup: uniform record copies");
    if (st.copies <= 0 || st.copy_bytes % st.copies != 0) goto cleanup;
    B = st.copy_bytes / st.copies;
    free(out); out = NULL;
    free(ref); ref = NULL;

    /* Concurrent split: budget overflow and halving on the IO pool. */
    kef_test_reset();
    kef_test_arm(KEF_SEAM_BUDGET, 300 * B);
    out = kef_run(&f, f.ks, 0, NULL, NULL, 0, 600, NULL, NULL, 0, &rc1, &e1);
    kef_test_stats(&st);
    kef_test_reset();
    ref = kef_run(&f, f.ks, 1, NULL, NULL, 0, 600, NULL, NULL, 0, &rc2, &e2);
    ASSERT_TRUE(kef_same_ref(rc1, e1, out, rc2, e2, ref),
                "wide split: page equals the reference page");
    ASSERT_TRUE(st.peak_bytes <= 300 * B, "wide split: retained copies stay within the budget");
    ASSERT_TRUE(st.live_copies == 0 && st.live_bytes == 0 && st.copies > 0,
                "wide split: no copy outlives the call");

cleanup:
    kef_test_reset();
    if (own_pool) parallel_io_pool_shutdown();
    kef_fix_free(&f);
    free(out);
    free(ref);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- Case 8: real find routes (rows + left join) ---- */

static int test_kef_find_route_run(void) {
    KefFix f;
    memset(&f, 0, sizeof(f));
    char req[1024];
    char *resp = NULL;
    KefTestStats st;

    if (kef_fix_build(&f, "kfr", 8, 3000, kfr_kind, kfr_n, kfr_ref, 1)) goto cleanup;

    kef_test_reset();
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"kfr\","
             "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"hot\"}],"
             "\"format\":\"rows\",\"limit\":10}");
    tu_pdb_request(f.db, req, &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\"") && strstr(resp, "\"rows\":["),
                "route: rows find succeeded");
    if (!resp || strstr(resp, "\"error\"")) goto cleanup;
    shard_db_free_result(resp); resp = NULL;
    kef_test_stats(&st);
    ASSERT_EQ_INT(st.calls, 1, "route: find entered keyset_emit_find once");
    ASSERT_EQ_INT(st.fetch_calls, 1, "route: ten-row page fetched one batch");
    ASSERT_EQ_INT(st.copies, 1024, "route: one batch copied");

    kef_test_reset();
    {
        char JL[256];
        kef_join_json(&f, 1, JL, sizeof(JL));
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"kfr\","
                 "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"hot\"}],"
                 "\"join\":%s,\"limit\":10}", JL);
    }
    tu_pdb_request(f.db, req, &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\"") && strstr(resp, "\"rows\":["),
                "join route: join find succeeded");
    if (!resp || strstr(resp, "\"error\"")) goto cleanup;
    shard_db_free_result(resp); resp = NULL;
    kef_test_stats(&st);
    ASSERT_EQ_INT(st.calls, 1, "join route: find entered keyset_emit_find once");
    ASSERT_EQ_INT(st.fetch_calls, 1, "join route: one fetch call");

cleanup:
    kef_test_reset();
    shard_db_free_result(resp);
    kef_fix_free(&f);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- Case 9: A3 trigram starts_with route leaves the page unterminated ---- */

static int test_kef_trigram_starts_error_run(void) {
    KefFix f;
    memset(&f, 0, sizeof(f));
    char *resp = NULL;
    KefTestStats st;

    f.db = test_get_process_db();
    ASSERT_NOT_NULL(f.db, "setup: process db");
    if (!f.db) goto cleanup;
    snprintf(f.obj, sizeof(f.obj), "kft");
    snprintf(f.eff, sizeof(f.eff), "%s/default", test_get_process_db_root());

    char req[1024];
    char body[50 * 64 + 256];
    tu_pdb_request(f.db, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    shard_db_free_result(resp); resp = NULL;

    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"kft\","
             "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"],"
             "\"indexes\":[\"name:trigram\"]}");
    tu_pdb_request(f.db, req, &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "setup: kft create");
    if (!resp || strstr(resp, "\"error\"")) { shard_db_free_result(resp); resp = NULL; goto cleanup; }
    shard_db_free_result(resp); resp = NULL;

    {
        size_t p = (size_t)snprintf(body, sizeof(body),
                                    "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                                    "\"object\":\"kft\",\"records\":[");
        for (int i = 0; i < 50; i++)
            p = tappendf(body, sizeof(body), p, "%s{\"key\":\"t%02d\","
                                  "\"value\":{\"name\":\"abcdef%02d\"}}", i ? "," : "", i, i);
        p = tappendf(body, sizeof(body), p, "]}");
        tu_pdb_request(f.db, body, &resp);
        ASSERT_TRUE(resp && strstr(resp, "\"inserted\":50") != NULL, "setup: kft bulk insert");
        if (!resp || strstr(resp, "\"inserted\":50") == NULL) { shard_db_free_result(resp); resp = NULL; goto cleanup; }
        shard_db_free_result(resp); resp = NULL;
    }

    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"kft\","
             "\"criteria\":[{\"field\":\"name\",\"op\":\"starts_with\",\"value\":\"abc\"}],"
             "\"limit\":10}");

    kef_test_reset();
    tu_pdb_request(f.db, req, &resp);
    ASSERT_TRUE(resp && strstr(resp, "abcdef") != NULL, "trigram route: control find returns rows");
    if (!resp) goto cleanup;
    shard_db_free_result(resp); resp = NULL;

    kef_test_reset();
    kef_test_arm(KEF_SEAM_FETCH, 1);
    tu_pdb_request(f.db, req, &resp);
    kef_test_stats(&st);
    ASSERT_EQ_INT(st.calls, 1, "trigram route: find entered keyset_emit_find once");
    ASSERT_EQ_INT(st.fetch_calls, 1, "trigram route: the fetch seam fired");
    ASSERT_TRUE(resp && strchr(resp, ']') == NULL,
                "trigram route: fetch failure leaves the page unterminated");

cleanup:
    kef_test_reset();
    shard_db_free_result(resp);
    kef_fix_free(&f);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-kef-early-stop-after-first-batch", test_kef_early_stop_run)
TEST_REGISTER("test-kef-stale-candidates-offset", test_kef_stale_candidates_run)
TEST_REGISTER("test-kef-exclusions-and-joins", test_kef_exclusions_joins_run)
TEST_REGISTER("test-kef-byte-budget-split", test_kef_byte_budget_run)
TEST_REGISTER("test-kef-failure-injection", test_kef_failure_injection_run)
TEST_REGISTER("test-kef-differential", test_kef_differential_run)
TEST_REGISTER("test-kef-wide-splits", test_kef_wide_splits_run)
TEST_REGISTER("test-kef-find-route-early-stop", test_kef_find_route_run)
TEST_REGISTER("test-kef-trigram-starts-error", test_kef_trigram_starts_error_run)
