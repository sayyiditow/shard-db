/* Regression for the missing per-cursor bound narrowing in
 * find_via_composite_prefix's OP_IN k-way merge (docs/plans/
 * 2026-08-10-kfcache-btree-lock-inversion.md, addendum). Every cursor
 * reopens from its pristine original sub-range on every round — nothing
 * narrows lo/hi on release — so entries already delivered before the
 * round's release point get redelivered in full on reopen, protected only
 * by an exact-(value,hash) tie-break that doesn't cover them. This test
 * puts two records (F=1, F=2) on the same composite-index shard as the
 * IN-seed match that triggers contention, and a third (F=3) on the other
 * shard that becomes the resume floor; F=1 and F=2 are expected to be
 * delivered twice. */
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

#define DUP_SPLITS 8               /* index_splits_for(8) == 2 shards */
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

static void dup_pause(void *ctx) {
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
    int role; /* 1 = OP_IN ordered find, 2 = indexed write */
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
            "[{\"field\":\"CAT\",\"op\":\"in\",\"value\":[\"cat1\"]}]",
            0, 10, NULL, NULL, NULL, NULL, NULL, "F", "asc", NULL, 0);
    } else {
        char bulk[256];
        snprintf(bulk, sizeof(bulk),
            "[{\"key\":\"%s\",\"value\":{\"CAT\":\"filler\",\"F\":500}}]",
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
    int kf_a = compute_record_shard(a, DUP_SPLITS);
    int kf_b = compute_record_shard(b, DUP_SPLITS);
    return kf_a == kf_b && idx_shard_for_hash(a, DUP_SPLITS) ==
        idx_shard_for_hash(b, DUP_SPLITS);
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
            *target_idx = idx_shard_for_hash(seed_hash, DUP_SPLITS);
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
        if (idx_shard_for_hash(hash, DUP_SPLITS) == wanted_idx) {
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
        if (compute_record_shard(hash, DUP_SPLITS) == wanted_kf &&
            idx_shard_for_hash(hash, DUP_SPLITS) == wanted_idx) {
            snprintf(out_key, cap, "%s", candidate);
            return 1;
        }
    }
    return 0;
}

static int append_bulk_record(char *buf, size_t cap, size_t *used,
                              const char *key, int field, int first) {
    int n = snprintf(buf + *used, cap - *used,
        "%s{\"key\":\"%s\",\"value\":{\"CAT\":\"filler\",\"F\":%d}}",
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

static int test_composite_in_multishard_duplicate_run(void) {
    ShardDb *db = test_get_process_db();
    const char *dir = "composite_in_dup";
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
        "\"fields\":[\"CAT:varchar:20\",\"F:int\"],"
        "\"indexes\":[\"CAT\",\"CAT+F\"]}", dir, object);
    ASSERT_TRUE(request_ok(db, request, &response), "create composite-indexed object");
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
    int writer_kf = compute_record_shard(writer_hash, DUP_SPLITS);
    ASSERT_TRUE(find_key_on_routes(k3_key, sizeof(k3_key), writer_kf, other_idx, "k3cand"),
                "find key on writer's kf shard but other index shard");

    size_t cap = 150000;
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
        if (idx_shard_for_hash(hash, DUP_SPLITS) != target_idx) continue;
        if (append_bulk_record(bulk, cap, &used, key, 9000 + i, count == 0) != 0) {
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
    /* Fillers use CAT="filler" — outside the "cat1" IN-value's composite
       prefix range entirely, but they still grow this composite index
       shard's on-disk entry count past the splice-path crossover so the
       writer below genuinely blocks on bt_cache. */
    ASSERT_TRUE(request_ok(db, bulk, &response), "prepopulate splice-path index shard");
    free(response); response = NULL;
    free(bulk);

    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"CAT\":\"cat1\",\"F\":1}}", dir, object, seed_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert seed record F=1");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"CAT\":\"cat1\",\"F\":2}}", dir, object, k2_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert same-shard record F=2");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"CAT\":\"cat1\",\"F\":3}}", dir, object, k3_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert other-shard record F=3");
    free(response); response = NULL;

    RaceSync sync;
    race_sync_init(&sync);
    order_walk_test_set_pause_hook_after(dup_pause, &sync, 2);

    QueryArgs reader = { .db = db, .dir = dir, .object = object, .role = 1 };
    tu_join_signal_init(&reader.js);
    pthread_t reader_tid;
    ASSERT_EQ_INT(pthread_create(&reader_tid, NULL, query_thread_main, &reader),
                  0, "start OP_IN ordered reader");

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
        TAP_DIAG("# reader never reached the parked composite-prefix fetch: %s\n",
                 hook_wait_rc == ETIMEDOUT ? "ETIMEDOUT" : strerror(hook_wait_rc));
        ASSERT_TRUE(0, "OP_IN reader reaches pause hook before timeout");
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
        ASSERT_TRUE(0, "OP_IN walk and indexed writer finish before timeout");
        fflush(NULL);
        _exit(1);
    }

    ASSERT_EQ_INT(reader.rc, 0, "OP_IN reader request succeeds");
    ASSERT_EQ_INT(writer.rc, 0, "indexed writer request succeeds");

    /* The regression: F=1 and F=2's keys must appear exactly once each.
       Pre-fix (no per-cursor bound narrowing on reopen), the reopened
       round restarts this shard's cursor from its pristine start and
       redelivers both — protected only by an exact-(value,hash) tie-break
       against F=3, which doesn't match either. */
    ASSERT_EQ_INT(count_occurrences(reader.response, seed_key), 1,
                  "F=1 record appears exactly once (regression)");
    ASSERT_EQ_INT(count_occurrences(reader.response, k2_key), 1,
                  "F=2 record appears exactly once (regression)");
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

static int test_composite_in_multishard_duplicate_failed_reopen_run(void) {
    ShardDb *db = test_get_process_db();
    const char *dir = "composite_in_dup_reopen";
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
        "\"fields\":[\"CAT:varchar:20\",\"F:int\"],"
        "\"indexes\":[\"CAT\",\"CAT+F\"]}", dir, object);
    ASSERT_TRUE(request_ok(db, request, &response), "create composite-indexed object");
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
    int writer_kf = compute_record_shard(writer_hash, DUP_SPLITS);
    ASSERT_TRUE(find_key_on_routes(k3_key, sizeof(k3_key), writer_kf, other_idx, "k3cand"),
                "find key on writer's kf shard but other index shard");

    size_t cap = 150000;
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
        if (idx_shard_for_hash(hash, DUP_SPLITS) != target_idx) continue;
        if (count == 0) snprintf(first_filler_key, sizeof(first_filler_key), "%s", key);
        if (append_bulk_record(bulk, cap, &used, key, 9000 + i, count == 0) != 0) {
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
        "\"key\":\"%s\",\"value\":{\"CAT\":\"cat1\",\"F\":1}}", dir, object, seed_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert seed record F=1");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"CAT\":\"cat1\",\"F\":2}}", dir, object, k2_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert same-shard record F=2");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"CAT\":\"cat1\",\"F\":3}}", dir, object, k3_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert other-shard record F=3");
    free(response); response = NULL;

    RaceSync sync;
    race_sync_init(&sync);
    order_walk_test_set_pause_hook_after(dup_pause, &sync, 2);

    QueryArgs reader = { .db = db, .dir = dir, .object = object, .role = 1 };
    tu_join_signal_init(&reader.js);
    pthread_t reader_tid;
    ASSERT_EQ_INT(pthread_create(&reader_tid, NULL, query_thread_main, &reader),
                  0, "start OP_IN ordered reader");

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
        TAP_DIAG("# reader never reached the parked composite-prefix fetch: %s\n",
                 hook_wait_rc == ETIMEDOUT ? "ETIMEDOUT" : strerror(hook_wait_rc));
        ASSERT_TRUE(0, "OP_IN reader reaches pause hook before timeout");
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
        ASSERT_TRUE(0, "OP_IN walk and indexed writer finish before timeout");
        fflush(NULL);
        _exit(1);
    }

    ASSERT_EQ_INT(reader.rc, 0, "OP_IN reader request succeeds");
    ASSERT_EQ_INT(writer.rc, 0, "indexed writer request succeeds");

    ASSERT_EQ_INT(count_occurrences(reader.response, seed_key), 1,
                  "F=1 record appears exactly once");
    ASSERT_EQ_INT(count_occurrences(reader.response, k2_key), 1,
                  "F=2 record appears exactly once");
    ASSERT_EQ_INT(count_occurrences(reader.response, k3_key), 1,
                  "F=3 record appears exactly once");

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

TEST_REGISTER("test-composite-in-multishard-duplicate",
              test_composite_in_multishard_duplicate_run)
TEST_REGISTER("test-composite-in-multishard-dup-failed-reopen",
              test_composite_in_multishard_duplicate_failed_reopen_run)
