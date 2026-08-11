/* Regression for the per-shard resume-floor bug in btree_idx_walk_ordered's
 * release/reopen protocol (docs/plans/2026-08-10-kfcache-btree-lock-inversion.md,
 * addendum). A single global resume floor, shared across all index shards,
 * silently strands any shard's un-pulled entries that fall between its own
 * last-delivered value and the round's globally-highest delivered value once
 * every shard reopens from that single floor. This test constructs exactly
 * that shape: two records on the SAME index shard (F=1, F=2) and one record
 * on the OTHER index shard (F=3) that sorts higher and gets delivered in the
 * same release-triggered drain, becoming the (broken) global floor. F=2 is
 * then permanently unreachable on reopen. The criteria is a single,
 * unbounded `F >= 1` leaf (not `F >= 1 AND F < 1000`) — deliberately, so
 * its own capped cardinality estimate saturates against the object's 1050
 * filler records and the planner is forced into the order-index-walk path
 * unconditionally; a bounded/selective leaf lets the planner choose
 * fetch+sort instead, which never reaches this walk at all. */
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "query_internal.h"
#include "shard_db_internal.h"
#include "types.h"
#include "btree.h"
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define SKIP_SPLITS 8               /* index_splits_for(8) == 2 shards */
#define BULK_FILLER_COUNT 1050
#define KEY_SEARCH_LIMIT 100000
#define JOIN_TIMEOUT_SEC 5

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int reached;
    int release;
} RaceSync;

static void race_sync_init(RaceSync *s) {
    pthread_mutex_init(&s->lock, NULL);
    pthread_cond_init(&s->cond, NULL);
    s->reached = 0;
    s->release = 0;
}

static void race_sync_destroy(RaceSync *s) {
    pthread_mutex_destroy(&s->lock);
    pthread_cond_destroy(&s->cond);
}

static void skip_pause(void *ctx) {
    RaceSync *s = ctx;
    pthread_mutex_lock(&s->lock);
    s->reached = 1;
    pthread_cond_broadcast(&s->cond);
    while (!s->release) pthread_cond_wait(&s->cond, &s->lock);
    pthread_mutex_unlock(&s->lock);
}

typedef struct {
    ShardDb *db;
    const char *dir;
    const char *object;
    const char *writer_key;
    int role; /* 1 = ordered find, 2 = indexed write */
    char *response;
    int rc;
    TuJoinSignal js;
} QueryArgs;

static void *query_thread_main(void *arg) {
    QueryArgs *a = arg;
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof(db_root), "%s/%s", a->db->db_root, a->dir);
    g_db = a->db;
    size_t out_len = 0;
    FILE *out = open_memstream(&a->response, &out_len);
    if (!out) { a->rc = -1; tu_join_signal_mark_done(&a->js); return NULL; }
    g_out = out;
    if (a->role == 1) {
        a->rc = cmd_find(db_root, a->object,
            "[{\"field\":\"F\",\"op\":\"gte\",\"value\":\"1\"}]",
            0, 10, NULL, NULL, NULL, NULL, NULL, "F", "asc", NULL, 0);
    } else {
        char bulk[256];
        snprintf(bulk, sizeof(bulk),
            "[{\"key\":\"%s\",\"value\":{\"F\":500}}]",
            a->writer_key);
        a->rc = cmd_bulk_insert_string(db_root, a->object, bulk, 0);
    }
    fflush(out);
    fclose(out);
    g_out = NULL;
    tu_join_signal_mark_done(&a->js);
    return NULL;
}

static int request_ok(ShardDb *db, const char *request, char **response) {
    size_t out_len = 0;
    *response = NULL;
    int rc = shard_db_query(db, request, response, &out_len);
    return rc == 0 && *response && strstr(*response, "\"error\"") == NULL;
}

static int route_same(const uint8_t a[16], const uint8_t b[16]) {
    int kf_a = compute_record_shard(a, SKIP_SPLITS);
    int kf_b = compute_record_shard(b, SKIP_SPLITS);
    return kf_a == kf_b && idx_shard_for_hash(a, SKIP_SPLITS) ==
        idx_shard_for_hash(b, SKIP_SPLITS);
}

static int find_seed_and_writer(char *seed_key, size_t seed_cap,
                                char *writer_key, size_t writer_cap,
                                int *target_idx) {
    uint8_t seed_hash[16];
    for (int i = 0; i < KEY_SEARCH_LIMIT; i++) {
        char candidate[64];
        snprintf(candidate, sizeof(candidate), "seed_%d", i);
        compute_hash_raw(candidate, strlen(candidate), seed_hash);
        for (int j = i + 1; j < KEY_SEARCH_LIMIT; j++) {
            char other[64];
            snprintf(other, sizeof(other), "writer_%d", j);
            uint8_t other_hash[16];
            compute_hash_raw(other, strlen(other), other_hash);
            if (!route_same(seed_hash, other_hash)) continue;
            snprintf(seed_key, seed_cap, "%s", candidate);
            snprintf(writer_key, writer_cap, "%s", other);
            *target_idx = idx_shard_for_hash(seed_hash, SKIP_SPLITS);
            return 1;
        }
    }
    return 0;
}

static int find_key_on_idx_shard(char *out_key, size_t cap, int wanted_idx,
                                 const char *prefix) {
    for (int i = 0; i < KEY_SEARCH_LIMIT; i++) {
        char candidate[64];
        snprintf(candidate, sizeof(candidate), "%s_%d", prefix, i);
        uint8_t hash[16];
        compute_hash_raw(candidate, strlen(candidate), hash);
        if (idx_shard_for_hash(hash, SKIP_SPLITS) == wanted_idx) {
            snprintf(out_key, cap, "%s", candidate);
            return 1;
        }
    }
    return 0;
}

static int find_key_on_routes(char *out_key, size_t cap,
                              int wanted_kf, int wanted_idx,
                              const char *prefix) {
    for (int i = 0; i < KEY_SEARCH_LIMIT; i++) {
        char candidate[64];
        snprintf(candidate, sizeof(candidate), "%s_%d", prefix, i);
        uint8_t hash[16];
        compute_hash_raw(candidate, strlen(candidate), hash);
        if (compute_record_shard(hash, SKIP_SPLITS) == wanted_kf &&
            idx_shard_for_hash(hash, SKIP_SPLITS) == wanted_idx) {
            snprintf(out_key, cap, "%s", candidate);
            return 1;
        }
    }
    return 0;
}

static int append_bulk_record(char *buf, size_t cap, size_t *used,
                              const char *key, int field, int first) {
    int n = snprintf(buf + *used, cap - *used,
        "%s{\"key\":\"%s\",\"value\":{\"F\":%d}}",
        first ? "" : ",", key, field);
    if (n < 0 || (size_t)n >= cap - *used) return -1;
    *used += (size_t)n;
    return 0;
}

static int count_occurrences(const char *haystack, const char *needle) {
    int n = 0;
    const char *p = haystack;
    size_t nl = strlen(needle);
    while (haystack && (p = strstr(p, needle)) != NULL) { n++; p += nl; }
    return n;
}

typedef struct {
    char seen[8];
    int count;
    int fail_shard;
} ReopenRetryCtx;

static int reopen_retry_cb(const char *value, size_t vlen,
                           const uint8_t hash[BT_HASH_SIZE],
                           BtOrderedWalkHandle *wh, void *ctx_v) {
    (void)hash;
    ReopenRetryCtx *ctx = ctx_v;
    if (vlen > 0 && ctx->count < (int)sizeof(ctx->seen) - 1)
        ctx->seen[ctx->count++] = value[0];
    if (ctx->count == 1) {
        btree_test_fail_next_range_open_shard(ctx->fail_shard);
        btree_ordered_walk_release_for_blocking(wh);
    } else if (ctx->count == 2) {
        btree_ordered_walk_release_for_blocking(wh);
    }
    return 0;
}

static int find_hash_on_idx_shard(uint8_t out[16], int wanted_idx,
                                  const char *prefix) {
    for (int i = 0; i < KEY_SEARCH_LIMIT; i++) {
        char candidate[64];
        snprintf(candidate, sizeof(candidate), "%s_%d", prefix, i);
        compute_hash_raw(candidate, strlen(candidate), out);
        if (idx_shard_for_hash(out, SKIP_SPLITS) == wanted_idx) return 1;
    }
    return 0;
}

static int test_ordered_walk_failed_reopen_stays_retired_run(void) {
    char root[] = "/tmp/shard-db-ordered-reopen-XXXXXX";
    char *made = mkdtemp(root);
    ASSERT_NOT_NULL(made, "create ordered-reopen fixture");
    if (!made) return 1;

    const char *object = "rows";
    const char *field = "F";
    char index_dir[PATH_MAX];
    snprintf(index_dir, sizeof(index_dir), "%s/%s/indexes/%s",
             root, object, field);
    mkdirp(index_dir);

    uint8_t shard0_hash[16], shard1_hash[16];
    ASSERT_TRUE(find_hash_on_idx_shard(shard0_hash, 0, "retry_s0"),
                "find hash for index shard 0");
    ASSERT_TRUE(find_hash_on_idx_shard(shard1_hash, 1, "retry_s1"),
                "find hash for index shard 1");
    ASSERT_EQ_INT(btree_idx_insert(root, object, field, SKIP_SPLITS,
                                   "1", 1, shard0_hash),
                  0, "insert F=1 on surviving shard");
    ASSERT_EQ_INT(btree_idx_insert(root, object, field, SKIP_SPLITS,
                                   "3", 1, shard0_hash),
                  0, "insert F=3 on surviving shard");
    ASSERT_EQ_INT(btree_idx_insert(root, object, field, SKIP_SPLITS,
                                   "2", 1, shard1_hash),
                  0, "insert F=2 on shard whose reopen will fail");

    ReopenRetryCtx ctx = { .fail_shard = 1 };
    btree_idx_walk_ordered(root, object, field, SKIP_SPLITS,
                           "0", 1, 0, "9", 1, 0, 0,
                           reopen_retry_cb, &ctx);
    ctx.seen[ctx.count] = '\0';

    /* First release: shard 1's reopen fails. Second release: the failed
       shard must stay retired; otherwise it reopens and emits F=2 after F=3. */
    ASSERT_EQ_STR(ctx.seen, "13",
                  "failed reopen stays retired for the rest of the walk");

    btree_test_fail_next_range_open_shard(-1);
    btree_idx_unlink_all(root, object, field, SKIP_SPLITS);
    tu_run_cmd("rm -rf %s", root);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_ordered_walk_multishard_skip_run(void) {
    ShardDb *db = test_get_process_db();
    const char *dir = "ordered_skip";
    const char *object = "rows";
    char request[512];
    char *response = NULL;
    char seed_key[64], writer_key[64], k2_key[64], k3_key[64];
    int target_idx = -1, other_idx = -1;

    ASSERT_TRUE(db != NULL, "process-local database available");
    if (!db) return 1;
    snprintf(request, sizeof(request),
        "{\"mode\":\"add-dir\",\"dir\":\"%s\"}", dir);
    request_ok(db, request, &response);
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"create-object\",\"dir\":\"%s\","
        "\"object\":\"%s\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"F:int\"],\"indexes\":[\"F\"]}", dir, object);
    ASSERT_TRUE(request_ok(db, request, &response), "create indexed object");
    free(response); response = NULL;

    ASSERT_TRUE(find_seed_and_writer(seed_key, sizeof(seed_key),
                                     writer_key, sizeof(writer_key),
                                     &target_idx),
                "find same kf/index shard seed/writer pair");
    if (target_idx < 0) goto cleanup;
    other_idx = 1 - target_idx; /* index_splits_for(8) == 2 */

    ASSERT_TRUE(find_key_on_idx_shard(k2_key, sizeof(k2_key), target_idx, "k2cand"),
                "find second key on the same index shard as seed");
    uint8_t writer_hash[16];
    compute_hash_raw(writer_key, strlen(writer_key), writer_hash);
    int writer_kf = compute_record_shard(writer_hash, SKIP_SPLITS);
    ASSERT_TRUE(find_key_on_routes(k3_key, sizeof(k3_key), writer_kf, other_idx, "k3cand"),
                "find key on writer's kf shard but other index shard");

    size_t cap = 130000;
    char *bulk = malloc(cap);
    ASSERT_NOT_NULL(bulk, "allocate bulk filler request");
    if (!bulk) goto cleanup;
    size_t used = (size_t)snprintf(bulk, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"%s\","
        "\"object\":\"%s\",\"records\":[", dir, object);
    int count = 0;
    for (int i = 0; i < KEY_SEARCH_LIMIT && count < BULK_FILLER_COUNT; i++) {
        char key[64];
        snprintf(key, sizeof(key), "bulk_%d", i);
        uint8_t hash[16];
        compute_hash_raw(key, strlen(key), hash);
        if (idx_shard_for_hash(hash, SKIP_SPLITS) != target_idx) continue;
        if (append_bulk_record(bulk, cap, &used, key, 1000 + i, count == 0) != 0) {
            ASSERT_TRUE(0, "bulk filler request fits in buffer");
            free(bulk);
            goto cleanup;
        }
        count++;
    }
    ASSERT_EQ_INT(count, BULK_FILLER_COUNT, "collect 1050 same-shard filler records");
    if (count != BULK_FILLER_COUNT) { free(bulk); goto cleanup; }
    if (used + 3 >= cap) { free(bulk); goto cleanup; }
    memcpy(bulk + used, "]}", 3);
    used += 2;
    bulk[used] = '\0';
    ASSERT_TRUE(request_ok(db, bulk, &response), "prepopulate splice-path index shard");
    free(response); response = NULL;
    free(bulk);

    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"F\":1}}", dir, object, seed_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert seed record F=1");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"F\":2}}", dir, object, k2_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert same-shard record F=2");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"F\":3}}", dir, object, k3_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert other-shard record F=3");
    free(response); response = NULL;

    RaceSync sync;
    race_sync_init(&sync);
    order_walk_test_set_pause_hook_after(skip_pause, &sync, 2);

    QueryArgs reader = { .db = db, .dir = dir, .object = object, .role = 1 };
    tu_join_signal_init(&reader.js);
    pthread_t reader_tid;
    ASSERT_EQ_INT(pthread_create(&reader_tid, NULL, query_thread_main, &reader),
                  0, "start ordered reader");

    pthread_mutex_lock(&sync.lock);
    struct timespec hook_deadline;
    clock_gettime(CLOCK_REALTIME, &hook_deadline);
    hook_deadline.tv_sec += JOIN_TIMEOUT_SEC;
    int hook_wait_rc = 0;
    while (!sync.reached && hook_wait_rc == 0)
        hook_wait_rc = pthread_cond_timedwait(&sync.cond, &sync.lock, &hook_deadline);
    int hook_reached = sync.reached;
    pthread_mutex_unlock(&sync.lock);
    if (!hook_reached) {
        TAP_DIAG("# reader never reached the parked order-index fetch: %s\n",
                 hook_wait_rc == ETIMEDOUT ? "ETIMEDOUT" : strerror(hook_wait_rc));
        ASSERT_TRUE(0, "ordered reader reaches pause hook before timeout");
        order_walk_test_set_pause_hook(NULL, NULL);
        btree_test_fail_next_range_open_shard(-1);
        _exit(1);
    }

    QueryArgs writer = {
        .db = db, .dir = dir, .object = object, .writer_key = writer_key, .role = 2
    };
    tu_join_signal_init(&writer.js);
    pthread_t writer_tid;
    ASSERT_EQ_INT(pthread_create(&writer_tid, NULL, query_thread_main, &writer),
                  0, "start indexed writer");

    int writer_pending = 0;
    for (int waited = 0; waited < JOIN_TIMEOUT_SEC * 10; waited++) {
        if (btree_test_writer_pending_count() > 0) { writer_pending = 1; break; }
        struct timespec poll = { 0, 100 * 1000000L };
        nanosleep(&poll, NULL);
    }
    ASSERT_TRUE(writer_pending, "writer reaches bt_cache writer acquisition");
    pthread_mutex_lock(&sync.lock);
    sync.release = 1;
    pthread_cond_broadcast(&sync.cond);
    pthread_mutex_unlock(&sync.lock);

    int reader_join = tu_timed_join(reader_tid, &reader.js, JOIN_TIMEOUT_SEC);
    int writer_join = tu_timed_join(writer_tid, &writer.js, JOIN_TIMEOUT_SEC);
    if (reader_join != 0 || writer_join != 0) {
        ASSERT_TRUE(0, "ordered walk and indexed writer finish before timeout");
        fflush(NULL);
        _exit(1);
    }

    ASSERT_EQ_INT(reader.rc, 0, "ordered reader request succeeds");
    ASSERT_EQ_INT(writer.rc, 0, "indexed writer request succeeds");
    ASSERT_CONTAINS(reader.response, seed_key, "reader returns F=1 seed record");

    /* The regression: F=2's key must survive the release/reopen. Pre-fix
       (single global resume floor), it is permanently skipped once F=3
       (the other shard) advances the shared floor past it. */
    ASSERT_CONTAINS(reader.response, k2_key, "reader returns F=2 record (regression)");
    ASSERT_CONTAINS(reader.response, k3_key, "reader returns F=3 record");
    ASSERT_EQ_INT(count_occurrences(reader.response, seed_key), 1,
                  "F=1 record appears exactly once");
    ASSERT_EQ_INT(count_occurrences(reader.response, k2_key), 1,
                  "F=2 record appears exactly once");
    ASSERT_EQ_INT(count_occurrences(reader.response, k3_key), 1,
                  "F=3 record appears exactly once");

    const char *p1 = reader.response ? strstr(reader.response, seed_key) : NULL;
    const char *p2 = reader.response ? strstr(reader.response, k2_key) : NULL;
    const char *p3 = reader.response ? strstr(reader.response, k3_key) : NULL;
    ASSERT_TRUE(p1 && p2 && p3 && p1 < p2 && p2 < p3,
                "records returned in F order (1, 2, 3)");

    free(reader.response);
    free(writer.response);
    order_walk_test_set_pause_hook(NULL, NULL);
    btree_test_fail_next_range_open_shard(-1);
    race_sync_destroy(&sync);
    tu_join_signal_destroy(&reader.js);
    tu_join_signal_destroy(&writer.js);

cleanup:
    free(response);
    tu_pdb_drop_object(db, dir, object);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_ordered_walk_multishard_skip_failed_reopen_run(void) {
    ShardDb *db = test_get_process_db();
    const char *dir = "ordered_skip_reopen";
    const char *object = "rows";
    char request[512];
    char *response = NULL;
    char seed_key[64], writer_key[64], k2_key[64], k3_key[64];
    int target_idx = -1, other_idx = -1;
    char first_filler_key[64] = {0};

    ASSERT_TRUE(db != NULL, "process-local database available");
    if (!db) return 1;
    snprintf(request, sizeof(request),
        "{\"mode\":\"add-dir\",\"dir\":\"%s\"}", dir);
    request_ok(db, request, &response);
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"create-object\",\"dir\":\"%s\","
        "\"object\":\"%s\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"F:int\"],\"indexes\":[\"F\"]}", dir, object);
    ASSERT_TRUE(request_ok(db, request, &response), "create indexed object");
    free(response); response = NULL;

    ASSERT_TRUE(find_seed_and_writer(seed_key, sizeof(seed_key),
                                     writer_key, sizeof(writer_key),
                                     &target_idx),
                "find same kf/index shard seed/writer pair");
    if (target_idx < 0) goto cleanup;
    other_idx = 1 - target_idx;

    ASSERT_TRUE(find_key_on_idx_shard(k2_key, sizeof(k2_key), target_idx, "k2cand"),
                "find second key on the same index shard as seed");
    uint8_t writer_hash[16];
    compute_hash_raw(writer_key, strlen(writer_key), writer_hash);
    int writer_kf = compute_record_shard(writer_hash, SKIP_SPLITS);
    ASSERT_TRUE(find_key_on_routes(k3_key, sizeof(k3_key), writer_kf, other_idx, "k3cand"),
                "find key on writer's kf shard but other index shard");

    size_t cap = 130000;
    char *bulk = malloc(cap);
    ASSERT_NOT_NULL(bulk, "allocate bulk filler request");
    if (!bulk) goto cleanup;
    size_t used = (size_t)snprintf(bulk, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"%s\","
        "\"object\":\"%s\",\"records\":[", dir, object);
    int count = 0;
    for (int i = 0; i < KEY_SEARCH_LIMIT && count < BULK_FILLER_COUNT; i++) {
        char key[64];
        snprintf(key, sizeof(key), "bulk_%d", i);
        uint8_t hash[16];
        compute_hash_raw(key, strlen(key), hash);
        if (idx_shard_for_hash(hash, SKIP_SPLITS) != target_idx) continue;
        if (count == 0) snprintf(first_filler_key, sizeof(first_filler_key), "%s", key);
        if (append_bulk_record(bulk, cap, &used, key, 1000 + i, count == 0) != 0) {
            ASSERT_TRUE(0, "bulk filler request fits in buffer");
            free(bulk);
            goto cleanup;
        }
        count++;
    }
    ASSERT_EQ_INT(count, BULK_FILLER_COUNT, "collect 1050 same-shard filler records");
    if (count != BULK_FILLER_COUNT) { free(bulk); goto cleanup; }
    if (used + 3 >= cap) { free(bulk); goto cleanup; }
    memcpy(bulk + used, "]}", 3);
    used += 2;
    bulk[used] = '\0';
    ASSERT_TRUE(request_ok(db, bulk, &response), "prepopulate splice-path index shard");
    free(response); response = NULL;
    free(bulk);

    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"F\":1}}", dir, object, seed_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert seed record F=1");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"F\":2}}", dir, object, k2_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert same-shard record F=2");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"F\":3}}", dir, object, k3_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert other-shard record F=3");
    free(response); response = NULL;

    RaceSync sync;
    race_sync_init(&sync);
    order_walk_test_set_pause_hook_after(skip_pause, &sync, 2);

    QueryArgs reader = { .db = db, .dir = dir, .object = object, .role = 1 };
    tu_join_signal_init(&reader.js);
    pthread_t reader_tid;
    ASSERT_EQ_INT(pthread_create(&reader_tid, NULL, query_thread_main, &reader),
                  0, "start ordered reader");

    pthread_mutex_lock(&sync.lock);
    struct timespec hook_deadline;
    clock_gettime(CLOCK_REALTIME, &hook_deadline);
    hook_deadline.tv_sec += JOIN_TIMEOUT_SEC;
    int hook_wait_rc = 0;
    while (!sync.reached && hook_wait_rc == 0)
        hook_wait_rc = pthread_cond_timedwait(&sync.cond, &sync.lock, &hook_deadline);
    int hook_reached = sync.reached;
    pthread_mutex_unlock(&sync.lock);
    if (!hook_reached) {
        TAP_DIAG("# reader never reached the parked order-index fetch: %s\n",
                 hook_wait_rc == ETIMEDOUT ? "ETIMEDOUT" : strerror(hook_wait_rc));
        ASSERT_TRUE(0, "ordered reader reaches pause hook before timeout");
        order_walk_test_set_pause_hook(NULL, NULL);
        btree_test_fail_next_range_open_shard(-1);
        _exit(1);
    }

    btree_test_fail_next_range_open_shard(target_idx);

    QueryArgs writer = {
        .db = db, .dir = dir, .object = object, .writer_key = writer_key, .role = 2
    };
    tu_join_signal_init(&writer.js);
    pthread_t writer_tid;
    ASSERT_EQ_INT(pthread_create(&writer_tid, NULL, query_thread_main, &writer),
                  0, "start indexed writer");

    int writer_pending = 0;
    for (int waited = 0; waited < JOIN_TIMEOUT_SEC * 10; waited++) {
        if (btree_test_writer_pending_count() > 0) { writer_pending = 1; break; }
        struct timespec poll = { 0, 100 * 1000000L };
        nanosleep(&poll, NULL);
    }
    ASSERT_TRUE(writer_pending, "writer reaches bt_cache writer acquisition");
    pthread_mutex_lock(&sync.lock);
    sync.release = 1;
    pthread_cond_broadcast(&sync.cond);
    pthread_mutex_unlock(&sync.lock);

    int reader_join = tu_timed_join(reader_tid, &reader.js, JOIN_TIMEOUT_SEC);
    int writer_join = tu_timed_join(writer_tid, &writer.js, JOIN_TIMEOUT_SEC);
    if (reader_join != 0 || writer_join != 0) {
        ASSERT_TRUE(0, "ordered walk and indexed writer finish before timeout");
        fflush(NULL);
        _exit(1);
    }

    ASSERT_EQ_INT(reader.rc, 0, "ordered reader request succeeds");
    ASSERT_EQ_INT(writer.rc, 0, "indexed writer request succeeds");

    ASSERT_CONTAINS(reader.response, seed_key, "reader returns F=1 seed record");
    ASSERT_CONTAINS(reader.response, k2_key, "reader returns F=2 record");
    ASSERT_CONTAINS(reader.response, k3_key, "reader returns F=3 record");
    ASSERT_EQ_INT(count_occurrences(reader.response, seed_key), 1,
                  "F=1 record appears exactly once");
    ASSERT_EQ_INT(count_occurrences(reader.response, k2_key), 1,
                  "F=2 record appears exactly once");
    ASSERT_EQ_INT(count_occurrences(reader.response, k3_key), 1,
                  "F=3 record appears exactly once");

    /* The failed-reopen regression: the first filler key's shard failed to
       reopen, so its stale buffered head must not appear in the response. */
    if (first_filler_key[0]) {
        ASSERT_EQ_INT(count_occurrences(reader.response, first_filler_key), 0,
                      "stale filler not emitted after failed reopen");
    }

    const char *p1 = reader.response ? strstr(reader.response, seed_key) : NULL;
    const char *p2 = reader.response ? strstr(reader.response, k2_key) : NULL;
    const char *p3 = reader.response ? strstr(reader.response, k3_key) : NULL;
    ASSERT_TRUE(p1 && p2 && p3 && p1 < p2 && p2 < p3,
                "records returned in F order (1, 2, 3)");

    free(reader.response);
    free(writer.response);
    order_walk_test_set_pause_hook(NULL, NULL);
    btree_test_fail_next_range_open_shard(-1);
    race_sync_destroy(&sync);
    tu_join_signal_destroy(&reader.js);
    tu_join_signal_destroy(&writer.js);

cleanup:
    free(response);
    tu_pdb_drop_object(db, dir, object);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-ordered-walk-multishard-skip",
              test_ordered_walk_multishard_skip_run)
TEST_REGISTER("test-ordered-walk-multishard-skip-failed-reopen",
              test_ordered_walk_multishard_skip_failed_reopen_run)
TEST_REGISTER("test-ordered-walk-failed-reopen-stays-retired",
              test_ordered_walk_failed_reopen_stays_retired_run)
