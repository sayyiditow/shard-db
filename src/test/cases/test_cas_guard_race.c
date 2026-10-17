/* src/test/cases/test_cas_guard_race.c
 *
 * Conditional-writes F2 — two concurrent guarded inserts: exactly one
 * may win, deterministically, via the guard-check park hook (TEST_BUILD
 * control channel, kind 3 / REACHED phase 4).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    TestEnv          *env;
    TestClient       *tc;      /* owned by the worker thread */
    const char       *req;
    char             *resp;    /* out */
    atomic_int        done;
} GuardRaceArg;

static void *guard_race_worker(void *p) {
    GuardRaceArg *a = p;
    TestClientCfg cfg = { .port = a->env->port, .io_timeout_ms = 30000 };
    a->tc = tc_connect(&cfg);
    if (!a->tc) return NULL;
    tc_request(a->tc, a->req, &a->resp);
    atomic_store(&a->done, 1);
    return NULL;
}

static int test_cas_guard_race_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *main_tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(main_tc, "connect");
    if (!main_tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(main_tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(main_tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rc\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"domain:varchar:40\"]}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create rc");
    free(resp); resp = NULL;

    tc_request(main_tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rc\","
        "\"key\":\"seed\",\"value\":{\"domain\":\"seed.io\"}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "seed rc");
    free(resp); resp = NULL;

    /* Arm the guard park: a guarded request parks AFTER its guard check
       (holding shared objlock plus every writer gate) until released. */
    ASSERT_EQ_INT(test_env_test_hook_install_kind(&env, 3), 0,
                  "arm guard hook (needs TEST_BUILD daemon)");

    const char *req_a =
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rc\","
        "\"key\":\"w1\",\"value\":{\"domain\":\"x.io\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"x.io\"}]}";
    const char *req_b =
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rc\","
        "\"key\":\"w2\",\"value\":{\"domain\":\"x.io\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"x.io\"}]}";

    GuardRaceArg a = { .env = &env, .tc = NULL, .req = req_a,
                       .resp = NULL, .done = 0 };
    pthread_t th1;
    ASSERT_EQ_INT(pthread_create(&th1, NULL, guard_race_worker, &a), 0,
                  "spawn first writer");

    /* First writer parks after its (passing) guard check. */
    int phase = -1;
    ASSERT_EQ_INT(test_env_test_hook_wait(&env, &phase), 0, "park reached");
    ASSERT_EQ_INT(phase, 4, "phase 4 = parked after guard check");

    /* Second writer on its own connection: must BLOCK on writer gates
       until the first commits — it cannot finish while we wait. */
    GuardRaceArg b = { .env = &env, .tc = NULL, .req = req_b,
                       .resp = NULL, .done = 0 };
    pthread_t th2;
    ASSERT_EQ_INT(pthread_create(&th2, NULL, guard_race_worker, &b), 0,
                  "spawn second writer");
    struct timespec pause_ts = { 0, 200 * 1000 * 1000 };
    nanosleep(&pause_ts, NULL);
    ASSERT_TRUE(atomic_load(&b.done) == 0,
                "second writer blocked on reserved writer gates");

    /* No kf write lock is held at this pause. Same-object GET must
       finish while writers remain reserved; a short socket deadline
       makes exclusive-object-lock regressions fail without hanging. */
    TestClientCfg read_cfg = { .port = env.port, .io_timeout_ms = 1000 };
    TestClient *read_tc = tc_connect(&read_cfg);
    ASSERT_NOT_NULL(read_tc, "reader connects during guard");
    if (read_tc) {
        tc_request(read_tc,
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rc\","
            "\"key\":\"seed\"}", &resp);
        ASSERT_CONTAINS(resp, "seed.io", "reader progresses during guard");
        free(resp); resp = NULL;
        tc_close(read_tc);
    }
    GuardRaceArg ordinary = { .env = &env,
        .req = "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"rc\","
               "\"key\":\"seed\",\"value\":{\"domain\":\"changed.io\"}}",
        .done = 0 };
    pthread_t th3;
    ASSERT_EQ_INT(pthread_create(&th3, NULL, guard_race_worker, &ordinary), 0,
                  "spawn ordinary writer");
    nanosleep(&pause_ts, NULL);
    ASSERT_TRUE(atomic_load(&ordinary.done) == 0,
                "unguarded writer also waits for reservation");

    /* Release the first writer: it commits, then the second's guard
       check runs and must fail. */
    ASSERT_EQ_INT(test_env_test_hook_release(&env), 0, "release");
    pthread_join(th1, NULL);
    pthread_join(th2, NULL);
    pthread_join(th3, NULL);
    ASSERT_TRUE(ordinary.resp && !strstr(ordinary.resp, "\"error\""),
                "ordinary writer completes after reservation");

    ASSERT_TRUE(a.resp && strstr(a.resp, "\"status\":\"inserted\"") != NULL,
                "first guarded insert commits");
    ASSERT_TRUE(b.resp &&
                strstr(b.resp, "\"error\":\"condition_not_met\"") != NULL,
                "second guarded insert blocked then rejected");
    ASSERT_TRUE(b.resp && strstr(b.resp, "\"matched\":1") != NULL,
                "second sees the first");

    tc_request(main_tc, "{\"mode\":\"count\",\"dir\":\"default\","
                       "\"object\":\"rc\"}", &resp);
    ASSERT_CONTAINS(resp, "2", "seed plus exactly one guarded insert");
    free(resp); resp = NULL;

    test_env_test_hook_clear(&env);
    tc_close(main_tc);
    if (a.tc) tc_close(a.tc);
    if (b.tc) tc_close(b.tc);
    free(a.resp);
    free(b.resp);
    if (ordinary.tc) tc_close(ordinary.tc);
    free(ordinary.resp);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-cas-guard-race", test_cas_guard_race_run)
