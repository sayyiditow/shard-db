/* src/test/cases/test_update_partial_concurrent.c
 *
 * Deterministic proof that single partial updates are atomic. The base
 * implementation reads OLD via slotcask_get and builds NEW outside the
 * kf-shard write lock, so a concurrent partial update can be erased when
 * the first update publishes a replacement built from its stale snapshot.
 *
 * The TEST_BUILD daemon seam pauses update A at a deterministic point and
 * reports which call site it hit:
 *
 *   phase 0 (base): A parks after its stale-snapshot read. B runs to
 *   completion first; when A resumes it publishes f1 from the stale
 *   snapshot and erases B's f2 change. The survival assertion FAILS — this
 *   is the required failing proof of the base implementation.
 *
 *   phase 1 (fixed): A parks inside v2_update_new_from_old while holding
 *   the kf wrlock. Releasing A publishes f1 first; B's update then reads
 *   the fresh OLD under the lock and both field changes survive.
 *
 * All coordination is the socketpair protocol via test_env_test_hook_*;
 * there are no sleeps, retries, or timing guesses.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    int         port;
    const char *json;
    int         rc;
    char       *resp;
} UpdateReq;

static void *update_thread_main(void *arg) {
    UpdateReq *r = arg;
    TestClientCfg cfg = { .port = r->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        r->rc = -1;
        r->resp = NULL;
        return NULL;
    }
    r->rc = tc_request(tc, r->json, &r->resp);
    tc_close(tc);
    return NULL;
}

static int test_update_partial_concurrent_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        TAP_DIAG("# test-update-partial-concurrent: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        ASSERT_NOT_NULL(tc, "connect");
        test_env_stop(&env);
        return 1;
    }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d1\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"o\",\"splits\":8,"
        "\"max_key\":64,\"fields\":[\"f1:varchar:64\",\"f2:varchar:64\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create obj");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\","
        "\"value\":{\"f1\":\"a\",\"f2\":\"b\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed insert");
    free(resp); resp = NULL;

    UpdateReq a = {
        .port = env.port,
        .json = "{\"mode\":\"update\",\"dir\":\"d1\",\"object\":\"o\","
                "\"key\":\"k1\",\"value\":{\"f1\":\"A1\"}}",
    };
    UpdateReq b = {
        .port = env.port,
        .json = "{\"mode\":\"update\",\"dir\":\"d1\",\"object\":\"o\","
                "\"key\":\"k1\",\"value\":{\"f2\":\"B1\"}}",
    };
    pthread_t a_tid, b_tid;
    int a_started = 0;
    int b_started = 0;
    int b_completed = 0;
    int hook_installed = 0;
    int hook_reached = 0;
    int hook_released = 0;
    int control_broken = 0;

    if (test_env_test_hook_install(&env) != 0) {
        ASSERT_TRUE(0, "hook install");
        control_broken = 1;
        goto cleanup;
    }
    hook_installed = 1;

    int create_rc = pthread_create(&a_tid, NULL, update_thread_main, &a);
    ASSERT_EQ_INT(create_rc, 0, "start A update thread");
    if (create_rc != 0)
        goto cleanup;
    a_started = 1;

    int phase = -1;
    if (test_env_test_hook_wait(&env, &phase) != 0) {
        ASSERT_TRUE(0, "hook reached");
        control_broken = 1;
        goto cleanup;
    }
    hook_reached = 1;
    ASSERT_TRUE(phase == 0 || phase == 1, "reported phase is 0 or 1");

    if (phase == 0) {
        /* Base code: A holds a stale snapshot. B must complete before A is
           released so the stale replacement deterministically erases B. */
        create_rc = pthread_create(&b_tid, NULL, update_thread_main, &b);
        ASSERT_EQ_INT(create_rc, 0, "start B update thread");
        if (create_rc != 0)
            goto cleanup;
        b_started = 1;
        pthread_join(b_tid, NULL);
        b_started = 0;
        b_completed = 1;
        ASSERT_EQ_INT(b.rc, 0, "B update request");
        ASSERT_CONTAINS(b.resp, "\"status\":\"updated\"", "B update ok");
    }

    int release_rc = test_env_test_hook_release(&env);
    ASSERT_EQ_INT(release_rc, 0, "hook release");
    if (release_rc != 0) {
        control_broken = 1;
        goto cleanup;
    }
    hook_released = 1;
    pthread_join(a_tid, NULL);
    a_started = 0;
    ASSERT_EQ_INT(a.rc, 0, "A update request");
    ASSERT_CONTAINS(a.resp, "\"status\":\"updated\"", "A update ok");

    if (!b_completed) {
        /* Fixed code: A published under the lock; B now reads the fresh
           OLD and must preserve A's f1 change. */
        create_rc = pthread_create(&b_tid, NULL, update_thread_main, &b);
        ASSERT_EQ_INT(create_rc, 0, "start B update thread");
        if (create_rc != 0)
            goto cleanup;
        b_started = 1;
        pthread_join(b_tid, NULL);
        b_started = 0;
        b_completed = 1;
        ASSERT_EQ_INT(b.rc, 0, "B update request");
        ASSERT_CONTAINS(b.resp, "\"status\":\"updated\"", "B update ok");
    }

    int clear_rc = test_env_test_hook_clear(&env);
    ASSERT_EQ_INT(clear_rc, 0, "hook clear");
    if (clear_rc != 0) {
        control_broken = 1;
        goto cleanup;
    }
    hook_installed = 0;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"f1\":\"A1\"", "A's f1 change survives");
    /* On the base implementation this fails with f2 still "b" — the
       recorded stale-snapshot field loss. */
    ASSERT_CONTAINS(resp, "\"f2\":\"B1\"", "B's f2 change survives (atomicity)");
    free(resp); resp = NULL;

cleanup:
    /* A broken control channel can leave the daemon-side callback parked;
       kill first so joining a request thread cannot hang the test runner. */
    if (control_broken && env.daemon_pid > 0)
        test_env_kill(&env);

    if (!control_broken && hook_reached && !hook_released) {
        if (test_env_test_hook_release(&env) == 0) {
            hook_released = 1;
        } else {
            control_broken = 1;
            if (env.daemon_pid > 0)
                test_env_kill(&env);
        }
    }

    if (a_started)
        pthread_join(a_tid, NULL);
    if (b_started)
        pthread_join(b_tid, NULL);

    if (!control_broken && hook_installed) {
        if (test_env_test_hook_clear(&env) != 0) {
            control_broken = 1;
            if (env.daemon_pid > 0)
                test_env_kill(&env);
        }
    }

    free(a.resp);
    free(b.resp);
    free(resp);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-update-partial-concurrent", test_update_partial_concurrent_run)
