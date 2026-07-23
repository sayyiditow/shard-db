/* Public-protocol TSan regressions for the kfcache -> bitmap-cache order.
 * Every test performs ordinary inserts first, establishing the write-side
 * order, then exercises exactly one read/rebuild route. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LOCK_ORDER_RACE_OPS 32

typedef struct {
    TestEnv env;
    TestClient *tc;
} LockOrderFixture;

static void lock_order_fixture_stop(LockOrderFixture *f) {
    if (f->tc) tc_close(f->tc);
    f->tc = NULL;
    test_env_stop(&f->env);
}

static int request_contains(TestClient *tc, const char *request,
                            const char *needle, const char *description) {
    char *resp = NULL;
    int rc = tc_request(tc, request, &resp);
    int ok = rc == 0 && resp && strstr(resp, needle) != NULL;
    ASSERT_TRUE(ok, description);
    free(resp);
    return ok ? 0 : -1;
}

static int lock_order_fixture_start(LockOrderFixture *f) {
    memset(f, 0, sizeof(*f));
    if (test_env_start(&f->env) != 0) {
        ASSERT_TRUE(0, "start isolated daemon");
        return -1;
    }

    TestClientCfg cfg = {
        .port = f->env.port,
        .io_timeout_ms = 30000,
    };
    f->tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(f->tc, "connect to isolated daemon");
    if (!f->tc) {
        lock_order_fixture_stop(f);
        return -1;
    }

    if (request_contains(
            f->tc,
            "{\"mode\":\"add-dir\",\"dir\":\"lock_order\"}",
            "\"dir\":\"lock_order\"",
            "create lock-order test tenant") != 0 ||
        request_contains(
            f->tc,
            "{\"mode\":\"create-object\",\"dir\":\"lock_order\","
            "\"object\":\"rows\",\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"bucket:varchar:8\",\"kind:varchar:8\","
            "\"score:int\"],"
            "\"indexes\":[\"bucket\",\"kind:bitmap\",\"score\"]}",
            "\"status\":\"created\"",
            "create object with bitmap and btree indexes") != 0) {
        lock_order_fixture_stop(f);
        return -1;
    }

    static const char *keys[] = {
        "k1", "k2", "k3", "k4", "k5", "k6"
    };
    static const char *buckets[] = {
        "g1", "g1", "g2", "g2", "g2", "g2"
    };
    static const char *kinds[] = {
        "alpha", "alpha", "alpha", "alpha", "alpha", "beta"
    };
    static const int scores[] = { 10, 20, 30, 40, 50, 60 };

    for (int i = 0; i < 6; i++) {
        char request[512];
        snprintf(
            request, sizeof(request),
            "{\"mode\":\"insert\",\"dir\":\"lock_order\","
            "\"object\":\"rows\",\"key\":\"%s\","
            "\"value\":{\"bucket\":\"%s\",\"kind\":\"%s\","
            "\"score\":%d}}",
            keys[i], buckets[i], kinds[i], scores[i]);
        if (request_contains(f->tc, request, "\"status\":\"inserted\"",
                             "insert bitmap-indexed row") != 0) {
            lock_order_fixture_stop(f);
            return -1;
        }
    }
    return 0;
}

typedef struct {
    int port;
    const char *read_request;
    _Atomic int failures;
} LockOrderRace;

static void *lock_order_race_writer(void *arg) {
    LockOrderRace *race = (LockOrderRace *)arg;
    TestClientCfg cfg = { .port = race->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        __atomic_fetch_add(&race->failures, 1, __ATOMIC_RELAXED);
        return NULL;
    }

    for (int i = 0; i < LOCK_ORDER_RACE_OPS; i++) {
        char *resp = NULL;
        char request[512];
        const char *kind = (i & 1) ? "alpha" : "beta";
        snprintf(
            request, sizeof(request),
            "{\"mode\":\"update\",\"dir\":\"lock_order\","
            "\"object\":\"rows\",\"key\":\"k1\","
            "\"value\":{\"bucket\":\"g1\",\"kind\":\"%s\","
            "\"score\":10}}",
            kind);
        if (tc_request(tc, request, &resp) != 0 ||
            !resp || SAFE_STRSTR(resp, "\"error\"") != NULL) {
            __atomic_fetch_add(&race->failures, 1, __ATOMIC_RELAXED);
        }
        free(resp);
    }
    tc_close(tc);
    return NULL;
}

static void *lock_order_race_reader(void *arg) {
    LockOrderRace *race = (LockOrderRace *)arg;
    TestClientCfg cfg = { .port = race->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        __atomic_fetch_add(&race->failures, 1, __ATOMIC_RELAXED);
        return NULL;
    }

    for (int i = 0; i < LOCK_ORDER_RACE_OPS; i++) {
        char *resp = NULL;
        int rc = tc_request(tc, race->read_request, &resp);
        if (rc != 0 || !resp || SAFE_STRSTR(resp, "\"error\"") != NULL) {
            __atomic_fetch_add(&race->failures, 1, __ATOMIC_RELAXED);
        }
        free(resp);
    }
    tc_close(tc);
    return NULL;
}

static int lock_order_fixture_race(LockOrderFixture *f,
                                   const char *read_request) {
    LockOrderRace race = {
        .port = f->env.port,
        .read_request = read_request,
        .failures = 0,
    };
    pthread_t writer;
    pthread_t reader;
    int writer_rc = pthread_create(&writer, NULL, lock_order_race_writer, &race);
    int reader_rc = writer_rc == 0
                  ? pthread_create(&reader, NULL, lock_order_race_reader, &race)
                  : -1;
    if (writer_rc != 0 || reader_rc != 0) {
        ASSERT_TRUE(0, "start concurrent lock-order clients");
        if (writer_rc == 0) pthread_join(writer, NULL);
        if (reader_rc == 0) pthread_join(reader, NULL);
        return -1;
    }
    pthread_join(writer, NULL);
    pthread_join(reader, NULL);
    ASSERT_EQ_INT(__atomic_load_n(&race.failures, __ATOMIC_RELAXED), 0,
                  "concurrent bitmap writer and reader requests succeed");
    return __atomic_load_n(&race.failures, __ATOMIC_RELAXED) == 0 ? 0 : -1;
}

static int test_bitmap_kfcache_lock_order_eq(void) {
    LockOrderFixture f;
    if (lock_order_fixture_start(&f) != 0) return 1;

    if (lock_order_fixture_race(
            &f,
            "{\"mode\":\"find\",\"dir\":\"lock_order\","
            "\"object\":\"rows\",\"criteria\":[{\"field\":\"kind\","
            "\"op\":\"eq\",\"value\":\"alpha\"}],\"limit\":10}") != 0) {
        lock_order_fixture_stop(&f);
        return 1;
    }

    char *resp = NULL;
    int rc = tc_request(
        f.tc,
        "{\"mode\":\"find\",\"dir\":\"lock_order\",\"object\":\"rows\","
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\","
        "\"value\":\"alpha\"}],\"limit\":10}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp != NULL, "bitmap equality find returns");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k1\"") != NULL,
                "bitmap equality includes first alpha row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k5\"") != NULL,
                "bitmap equality includes last alpha row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k6\"") == NULL,
                "bitmap equality excludes beta row");
    free(resp);

    lock_order_fixture_stop(&f);
    return 0;
}

TEST_REGISTER("test-bitmap-kfcache-lock-order-eq",
              test_bitmap_kfcache_lock_order_eq)

static int test_bitmap_kfcache_lock_order_generic(void) {
    LockOrderFixture f;
    if (lock_order_fixture_start(&f) != 0) return 1;

    if (lock_order_fixture_race(
            &f,
            "{\"mode\":\"find\",\"dir\":\"lock_order\","
            "\"object\":\"rows\",\"criteria\":[{\"field\":\"kind\","
            "\"op\":\"lt\",\"value\":\"beta\"}],\"limit\":10}") != 0) {
        lock_order_fixture_stop(&f);
        return 1;
    }

    char *resp = NULL;
    int rc = tc_request(
        f.tc,
        "{\"mode\":\"find\",\"dir\":\"lock_order\",\"object\":\"rows\","
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"lt\","
        "\"value\":\"beta\"}],\"limit\":10}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp != NULL, "generic bitmap find returns");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k1\"") != NULL,
                "generic bitmap find includes first alpha row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k2\"") != NULL,
                "generic bitmap find includes second alpha row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k6\"") == NULL,
                "generic bitmap find excludes beta row");
    free(resp);

    lock_order_fixture_stop(&f);
    return 0;
}

TEST_REGISTER("test-bitmap-kfcache-lock-order-generic",
              test_bitmap_kfcache_lock_order_generic)

static int test_bitmap_kfcache_lock_order_keyset(void) {
    LockOrderFixture f;
    if (lock_order_fixture_start(&f) != 0) return 1;

    if (lock_order_fixture_race(
            &f,
            "{\"mode\":\"find\",\"dir\":\"lock_order\","
            "\"object\":\"rows\",\"criteria\":[{\"or\":["
            "{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"beta\"},"
            "{\"field\":\"score\",\"op\":\"eq\",\"value\":\"10\"}]}],"
            "\"limit\":10}") != 0) {
        lock_order_fixture_stop(&f);
        return 1;
    }

    char *resp = NULL;
    int rc = tc_request(
        f.tc,
        "{\"mode\":\"find\",\"dir\":\"lock_order\",\"object\":\"rows\","
        "\"criteria\":[{\"or\":["
        "{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"beta\"},"
        "{\"field\":\"score\",\"op\":\"eq\",\"value\":\"10\"}]}],"
        "\"limit\":10}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp != NULL, "bitmap KeySet find returns");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k6\"") != NULL,
                "bitmap OR KeySet includes beta row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k1\"") != NULL,
                "bitmap OR KeySet includes score=10 row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k2\"") == NULL,
                "bitmap OR KeySet excludes non-matching row");
    free(resp);

    lock_order_fixture_stop(&f);
    return 0;
}

TEST_REGISTER("test-bitmap-kfcache-lock-order-keyset",
              test_bitmap_kfcache_lock_order_keyset)

static int test_bitmap_kfcache_lock_order_complement(void) {
    LockOrderFixture f;
    if (lock_order_fixture_start(&f) != 0) return 1;

    if (lock_order_fixture_race(
            &f,
            "{\"mode\":\"aggregate\",\"dir\":\"lock_order\","
            "\"object\":\"rows\",\"group_by\":[\"bucket\"],"
            "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
            "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\","
            "\"value\":\"alpha\"}],\"order_by\":\"n\","
            "\"order\":\"desc\",\"limit\":2}") != 0) {
        lock_order_fixture_stop(&f);
        return 1;
    }

    if (request_contains(
            f.tc,
            "{\"mode\":\"update\",\"dir\":\"lock_order\","
            "\"object\":\"rows\",\"key\":\"k1\","
            "\"value\":{\"bucket\":\"g1\",\"kind\":\"alpha\","
            "\"score\":10}}",
            "\"status\":\"updated\"",
            "restore alpha row after concurrent complement reads") != 0) {
        lock_order_fixture_stop(&f);
        return 1;
    }

    char *resp = NULL;
    int rc = tc_request(
        f.tc,
        "{\"mode\":\"aggregate\",\"dir\":\"lock_order\","
        "\"object\":\"rows\",\"group_by\":[\"bucket\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\","
        "\"value\":\"alpha\"}],\"order_by\":\"n\","
        "\"order\":\"desc\",\"limit\":2}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp != NULL,
                "majority-bitmap top-N aggregate returns");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"bucket\":\"g2\",\"n\":3") != NULL,
                "majority-bitmap complement keeps g2 count");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"bucket\":\"g1\",\"n\":2") != NULL,
                "majority-bitmap complement keeps g1 count");
    free(resp);

    lock_order_fixture_stop(&f);
    return 0;
}

TEST_REGISTER("test-bitmap-kfcache-lock-order-complement",
              test_bitmap_kfcache_lock_order_complement)

static int test_bitmap_kfcache_lock_order_rebuild(void) {
    LockOrderFixture f;
    if (lock_order_fixture_start(&f) != 0) return 1;

    char *resp = NULL;
    int rc = tc_request(
        f.tc,
        "{\"mode\":\"add-index\",\"dir\":\"lock_order\","
        "\"object\":\"rows\",\"field\":\"kind:bitmap\","
        "\"force\":true}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "forced bitmap rebuild succeeds");
    free(resp);
    resp = NULL;

    rc = tc_request(
        f.tc,
        "{\"mode\":\"count\",\"dir\":\"lock_order\",\"object\":\"rows\","
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\","
        "\"value\":\"alpha\"}]}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp != NULL,
                "post-rebuild bitmap count returns");
    ASSERT_EQ_INT(tu_parse_count(resp), 5,
                  "post-rebuild bitmap count preserves five alpha rows");
    free(resp);

    lock_order_fixture_stop(&f);
    return 0;
}

TEST_REGISTER("test-bitmap-kfcache-lock-order-rebuild",
              test_bitmap_kfcache_lock_order_rebuild)
