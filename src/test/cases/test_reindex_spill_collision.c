#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_reindex_spill_collision_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"articles\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"title:varchar:200\"],"
        "\"indexes\":[\"title\",\"title:trigram\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object with both indexes");
    free(resp); resp = NULL;

    const char *titles[] = {
        "Honda announces new EV platform",
        "Honda wins Le Mans prototype class",
        "Toyota beats Honda in sales",
        "The best motorcycles of 2026",
        "Honda recall affects 200k vehicles",
        "Apple releases new MacBook",
        "OpenAI launches GPT-5",
        "PostgreSQL 17 performance review",
        "Rust 2.0 stabilized",
        "Linux kernel 7.0 released",
        "Microsoft acquires startup",
        "Google search algorithm update",
        "Amazon AWS outage post-mortem",
        "Meta VR headset review",
        "SpaceX starship test flight",
    };
    for (int i = 0; i < 15; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"articles\","
            "\"key\":\"k%02d\",\"value\":{\"title\":\"%s\"}}",
            i, titles[i]);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"honda\"}]}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 4, "pre-reindex trigram icontains honda");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"starts\",\"value\":\"Honda\"}]}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 3, "pre-reindex btree starts Honda");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"reindex\",\"dir\":\"d\",\"object\":\"articles\"}",
        &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"honda\"}]}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 4, "post-reindex trigram icontains honda");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"starts\",\"value\":\"Honda\"}]}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 3, "post-reindex btree starts Honda");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d\",\"object\":\"articles\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"honda\"}],"
        "\"limit\":10}",
        &resp);
    ASSERT_NOT_NULL(resp, "post-reindex find response");
    ASSERT_EQ_INT(resp[0] == '[', 1, "post-reindex find returns array");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-reindex-spill-collision", test_reindex_spill_collision_run);
