/* Deterministic regression for the kfcache -> bt_cache lock-order inversion.
 * The reader parks after opening every order-by index shard and before its
 * first record fetch. The writer then acquires the matching kf-shard write
 * lock and blocks on the matching index-shard bt_cache write lock. On the
 * unfixed path, releasing the reader makes it block on the kf-shard read
 * lock, completing the AB-BA cycle. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
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

#define DEADLOCK_SPLITS 8
#define BULK_SEED_COUNT 1050
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

static void order_walk_pause(void *ctx) {
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
    int single_write;
    char *response;
    int rc;
} QueryArgs;

static void *query_thread_main(void *arg) {
    QueryArgs *a = arg;
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof(db_root), "%s/%s", a->db->db_root, a->dir);
    g_db = a->db;
    size_t out_len = 0;
    FILE *out = open_memstream(&a->response, &out_len);
    if (!out) { a->rc = -1; return NULL; }
    g_out = out;
    if (a->role == 1) {
        a->rc = cmd_find(db_root, a->object,
            "[{\"field\":\"F\",\"op\":\"gte\",\"value\":\"1\"}]",
            0, 2, NULL, NULL, NULL, NULL, NULL, "F", "asc", NULL, 0);
    } else if (a->single_write) {
        char value[] = "{\"F\":2}";
        a->rc = cmd_insert(db_root, a->object, a->writer_key,
                           strlen(a->writer_key), value, NULL, 0);
    } else {
        char bulk[256];
        snprintf(bulk, sizeof(bulk),
            "[{\"key\":\"%s\",\"value\":{\"F\":2}}]",
            a->writer_key);
        a->rc = cmd_bulk_insert_string(db_root, a->object, bulk, 0);
    }
    fflush(out);
    fclose(out);
    g_out = NULL;
    return NULL;
}

static int timed_join(pthread_t tid, int seconds) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += seconds;
    return pthread_timedjoin_np(tid, NULL, &deadline);
}

static int request_ok(ShardDb *db, const char *request, char **response) {
    size_t out_len = 0;
    *response = NULL;
    int rc = shard_db_query(db, request, response, &out_len);
    return rc == 0 && *response && strstr(*response, "\"error\"") == NULL;
}

static int route_same(const uint8_t a[16], const uint8_t b[16]) {
    int kf_a = compute_record_shard(a, DEADLOCK_SPLITS);
    int kf_b = compute_record_shard(b, DEADLOCK_SPLITS);
    return kf_a == kf_b && idx_shard_for_hash(a, DEADLOCK_SPLITS) ==
        idx_shard_for_hash(b, DEADLOCK_SPLITS);
}

static int find_seed_and_writer(char *seed_key, size_t seed_cap,
                                char *writer_key, size_t writer_cap,
                                int *target_idx) {
    uint8_t seed_hash[16];
    int found = 0;
    for (int i = 0; i < KEY_SEARCH_LIMIT && !found; i++) {
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
            *target_idx = idx_shard_for_hash(seed_hash, DEADLOCK_SPLITS);
            found = 1;
            break;
        }
    }
    return found;
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

static int run_ordered_walk_deadlock(int single_write) {
    ShardDb *db = test_get_process_db();
    const char *dir = single_write ? "ordered_single" : "ordered_bulk";
    const char *object = "rows";
    char request[512];
    char *response = NULL;
    char seed_key[64], writer_key[64];
    int target_idx = -1;

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
                "find same kf/index shard key pair");
    if (target_idx < 0) goto cleanup;

    if (!single_write) {
        size_t cap = 120000;
        char *bulk = malloc(cap);
        ASSERT_NOT_NULL(bulk, "allocate bulk seed request");
        if (!bulk) goto cleanup;
        size_t used = (size_t)snprintf(bulk, cap,
            "{\"mode\":\"bulk-insert\",\"dir\":\"%s\","
            "\"object\":\"%s\",\"records\":[", dir, object);
        int count = 0;
        for (int i = 0; i < KEY_SEARCH_LIMIT && count < BULK_SEED_COUNT; i++) {
            char key[64];
            snprintf(key, sizeof(key), "bulk_%d", i);
            uint8_t hash[16];
            compute_hash_raw(key, strlen(key), hash);
            if (idx_shard_for_hash(hash, DEADLOCK_SPLITS) != target_idx) continue;
            if (append_bulk_record(bulk, cap, &used, key, 1000 + i, count == 0) != 0) {
                ASSERT_TRUE(0, "bulk seed request fits in buffer");
                free(bulk);
                goto cleanup;
            }
            count++;
        }
        ASSERT_EQ_INT(count, BULK_SEED_COUNT, "collect 1050 same index-shard records");
        if (count != BULK_SEED_COUNT) { free(bulk); goto cleanup; }
        if (used + 3 >= cap) { free(bulk); goto cleanup; }
        memcpy(bulk + used, "]}", 3);
        used += 2;
        bulk[used] = '\0';
        ASSERT_TRUE(request_ok(db, bulk, &response), "prepopulate splice-path index");
        free(response); response = NULL;
        free(bulk);
    }

    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"F\":1}}", dir, object, seed_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert seed record");
    free(response); response = NULL;

    RaceSync sync;
    race_sync_init(&sync);
    order_walk_test_set_pause_hook(order_walk_pause, &sync);

    QueryArgs reader = {
        .db = db, .dir = dir, .object = object, .role = 1
    };
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
        _exit(1);
    }

    QueryArgs writer = {
        .db = db, .dir = dir, .object = object, .writer_key = writer_key,
        .role = 2, .single_write = single_write
    };
    pthread_t writer_tid;
    ASSERT_EQ_INT(pthread_create(&writer_tid, NULL, query_thread_main, &writer),
                  0, "start indexed writer");

    /* Confirm the writer reached a real bt_cache writer acquisition before
       releasing the parked reader. This is the same TEST_BUILD-only pending
       counter used by the existing bt-cache lock tests. */
    int writer_pending = 0;
    for (int waited = 0; waited < JOIN_TIMEOUT_SEC * 10; waited++) {
        if (btree_test_writer_pending_count() > 0) {
            writer_pending = 1;
            break;
        }
        struct timespec poll = { 0, 100 * 1000000L };
        nanosleep(&poll, NULL);
    }
    ASSERT_TRUE(writer_pending, "writer reaches bt_cache writer acquisition");
    pthread_mutex_lock(&sync.lock);
    sync.release = 1;
    pthread_cond_broadcast(&sync.cond);
    pthread_mutex_unlock(&sync.lock);

    int reader_join = timed_join(reader_tid, JOIN_TIMEOUT_SEC);
    int writer_join = timed_join(writer_tid, JOIN_TIMEOUT_SEC);
    if (reader_join != 0 || writer_join != 0) {
        TAP_DIAG("# reader still blocked in read_record_ref: %s\n",
                 reader_join == 0 ? "no" : strerror(reader_join));
        TAP_DIAG("# writer still blocked in %s: %s\n",
                 single_write ? "btree_insert_locked" : "btree_bulk_merge",
                 writer_join == 0 ? "no" : strerror(writer_join));
        ASSERT_TRUE(0, "ordered walk and indexed writer finish before timeout");
        fflush(NULL);
        _exit(1);
    }

    ASSERT_EQ_INT(reader.rc, 0, "ordered reader request succeeds");
    ASSERT_EQ_INT(writer.rc, 0, "indexed writer request succeeds");
    ASSERT_CONTAINS(writer.response, "inserted", "writer response is populated");
    ASSERT_CONTAINS(reader.response, seed_key, "ordered reader returns seed record");
    ASSERT_CONTAINS(reader.response, writer_key,
                    "in-flight ordered reader returns writer record");
    const char *reader_seed_pos = reader.response
                                    ? strstr(reader.response, seed_key) : NULL;
    const char *reader_writer_pos = reader.response
                                      ? strstr(reader.response, writer_key) : NULL;
    ASSERT_TRUE(reader_seed_pos && reader_writer_pos &&
                reader_seed_pos < reader_writer_pos,
                "in-flight ordered reader returns both records in F order");
    char final_request[512];
    snprintf(final_request, sizeof(final_request),
        "{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"criteria\":[{\"field\":\"F\",\"op\":\"gte\",\"value\":\"1\"},"
        "{\"field\":\"F\",\"op\":\"lte\",\"value\":\"2\"}],"
        "\"order_by\":\"F\",\"order\":\"asc\"}", dir, object);
    char *final_response = NULL;
    ASSERT_TRUE(request_ok(db, final_request, &final_response),
                "post-race ordered find succeeds");
    ASSERT_CONTAINS(final_response, seed_key, "post-race find returns seed record");
    ASSERT_CONTAINS(final_response, writer_key, "post-race find returns writer record");
    const char *seed_pos = final_response ? strstr(final_response, seed_key) : NULL;
    const char *writer_pos = final_response ? strstr(final_response, writer_key) : NULL;
    ASSERT_TRUE(seed_pos && writer_pos && seed_pos < writer_pos,
                "post-race ordered find returns records in F order");
    free(final_response);
    free(reader.response);
    free(writer.response);
    order_walk_test_set_pause_hook(NULL, NULL);
    race_sync_destroy(&sync);

cleanup:
    free(response);
    tu_pdb_drop_object(db, dir, object);
    return t_ctx->failed > 0 ? 1 : 0;
}

int ordered_walk_kfcache_deadlock_run_single(void) {
    return run_ordered_walk_deadlock(1);
}

static int test_ordered_walk_kfcache_deadlock_run(void) {
    return run_ordered_walk_deadlock(0);
}

TEST_REGISTER("test-ordered-walk-kfcache-deadlock", test_ordered_walk_kfcache_deadlock_run)
