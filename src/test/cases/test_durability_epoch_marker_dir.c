/* src/test/cases/test_durability_epoch_marker_dir.c
 *
 * B3a D4: marker-directory fsyncs route through the process-wide
 * durability epoch. Two concurrent deferred bulk upsert requests on
 * one object (one shared data/kf dir) both succeed, and at least one
 * raw dir fsync runs through the epoch.
 *
 * Red before D4: the B2 per-request coalescer never calls the epoch,
 * so the dir count is 0 while the requests still succeed.
 *
 * Workers bind g_db themselves: it is __thread and spawned threads
 * start with NULL (same precedent as test-bt-cache-writer-starvation).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

#define MD_SPLITS 8
#define MD_NRECS  4

typedef struct {
    SlotcaskDb db;
    char base[PATH_MAX];
} MdDb;

static int md_db_open(MdDb *w) {
    slotcask_init(64, 64);
    char b[] = "/tmp/shard-db-durepo-mkdir-XXXXXX";
    if (!mkdtemp(b)) return -1;
    snprintf(w->base, sizeof(w->base), "%s", b);
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", w->base);
    snprintf(k, sizeof(k), "%s/data/kf", w->base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return -1;
    memset(&w->db, 0, sizeof(w->db));
    if (slotcask_open(&w->db, w->base, MD_SPLITS, 1, 64) != 0) return -1;
    w->db.bulk_commit_window = 16;
    return 0;
}

static void md_db_close(MdDb *w) {
    slotcask_close(&w->db);
    rmrf(w->base);
    slotcask_shutdown();
}

typedef struct {
    char keys[MD_NRECS][24];
    char vals[MD_NRECS][24];
    SlotcaskBulkRec recs[MD_NRECS];
    SlotcaskBulkRec perm[MD_NRECS];  /* bucketed copy the inputs point
                                        into; lives for the request   */
} MdBatch;

static int md_shard_of(const char *key) {
    uint8_t h[16];
    compute_hash_raw(key, strlen(key), h);
    return compute_record_shard(h, MD_SPLITS);
}

static void md_batch_fill(MdBatch *b, int prefix) {
    for (int i = 0; i < MD_NRECS; i++) {
        snprintf(b->keys[i], sizeof(b->keys[i]), "md-%d-%02d", prefix, i);
        snprintf(b->vals[i], sizeof(b->vals[i]), "v-%d-%02d", prefix, i);
        memset(&b->recs[i], 0, sizeof(b->recs[i]));
        b->recs[i].key = b->keys[i];
        b->recs[i].klen = strlen(b->keys[i]);
        b->recs[i].value = b->vals[i];
        b->recs[i].vlen = strlen(b->vals[i]);
    }
}

static int md_noop_prepare(SlotcaskBulkRec *recs, const size_t *active,
                           size_t nactive, void *ctx,
                           void **out_window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    *out_window_state = NULL;
    return 0;
}
static int md_noop_apply(SlotcaskBulkRec *recs, const size_t *active,
                         size_t nactive, void *ctx, void *window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    (void)window_state;
    return 0;
}
static void md_noop_terminal(void *ctx, void *window_state) {
    (void)ctx; (void)window_state;
}

static int md_run_request(MdDb *w, MdBatch *b) {
    SlotcaskBulkShardInput inputs[MD_SPLITS];
    size_t ninputs = 0;
    size_t bucket_n[MD_SPLITS] = {0};
    for (int i = 0; i < MD_NRECS; i++)
        bucket_n[md_shard_of(b->keys[i])]++;
    size_t run = 0;
    int bucket_start[MD_SPLITS];
    for (int s = 0; s < MD_SPLITS; s++) {
        bucket_start[s] = (int)run;
        run += bucket_n[s];
    }
    if (run != MD_NRECS) return -1;
    size_t cursors[MD_SPLITS];
    for (int s = 0; s < MD_SPLITS; s++) cursors[s] = bucket_start[s];
    for (int i = 0; i < MD_NRECS; i++) {
        int s = md_shard_of(b->keys[i]);
        b->perm[cursors[s]++] = b->recs[i];
    }
    SlotcaskBulkOpts opts;
    memset(&opts, 0, sizeof(opts));
    opts.has_indexed_fields = 1;   /* markers published → M/C barriers */
    opts.prepare_window = md_noop_prepare;
    opts.apply_window = md_noop_apply;
    opts.commit_done = md_noop_terminal;
    opts.release_window = md_noop_terminal;
    opts.abort_window = md_noop_terminal;
    for (int s = 0; s < MD_SPLITS; s++) {
        if (bucket_n[s] == 0) continue;
        inputs[ninputs].kf_shard_id = s;
        inputs[ninputs].recs = b->perm + bucket_start[s];
        inputs[ninputs].nrecs = bucket_n[s];
        inputs[ninputs].kind = SLOTCASK_BULK_INPUT_UPSERT;
        inputs[ninputs].opts.upsert = opts;
        inputs[ninputs].rc = 0;
        ninputs++;
    }
    return slotcask_bulk_request_execute(&w->db, inputs, ninputs);
}

typedef struct { MdDb *w; MdBatch *b; ShardDb *tdb; int rc; } MdReqThr;

static void *md_req_thread(void *raw) {
    MdReqThr *a = raw;
    g_db = a->tdb;   /* __thread g_db: spawned threads start NULL */
    a->rc = md_run_request(a->w, a->b);
    return NULL;
}

static int test_durability_epoch_marker_dir_run(void) {
    MdDb w;
    memset(&w, 0, sizeof(w));
    ASSERT_EQ_INT(md_db_open(&w), 0, "open marker-dir epoch fixture");
    if (w.base[0] != '/') { slotcask_shutdown(); return 1; }
    ShardDb *proc_db = g_db;

    durability_test_epoch_reset();

    MdBatch ba, bb;
    md_batch_fill(&ba, 1);
    md_batch_fill(&bb, 2);
    MdReqThr ta = { &w, &ba, proc_db, 0 }, tb = { &w, &bb, proc_db, 0 };
    pthread_t tha, thb;
    ASSERT_EQ_INT(pthread_create(&tha, NULL, md_req_thread, &ta), 0,
                  "spawn request A");
    ASSERT_EQ_INT(pthread_create(&thb, NULL, md_req_thread, &tb), 0,
                  "spawn request B");
    pthread_join(tha, NULL);
    pthread_join(thb, NULL);

    ASSERT_EQ_INT(ta.rc, 0, "request A succeeds");
    ASSERT_EQ_INT(tb.rc, 0, "request B succeeds");
    /* RED pre-D4: the B2 per-request coalescer never calls the epoch,
       so this count is 0 while both requests still succeed. GREEN
       post-D4: at least one marker-dir fsync ran through it. The
       coalescing DEGREE is deliberately not asserted here — that is
       the bench's job (Task 5). */
    ASSERT_TRUE(durability_test_epoch_dir_sync_count() >= 1,
                "marker-dir fsyncs route through the process-wide epoch");

    durability_test_epoch_reset();
    md_db_close(&w);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-durability-epoch-marker-dir",
              test_durability_epoch_marker_dir_run)
