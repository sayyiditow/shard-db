/* src/test/cases/test_guard_corrupt_flag.c
 *
 * Conditional-writes F2 hardening — a corrupt keyfile flag byte (neither
 * 0/empty, 1/live, nor 2/tombstone) must FAIL the guard search closed,
 * never read as absence. The fallback strict walk is the path under
 * test: the guarded field carries no index, so guard_has_other uses
 * slotcask_walk_live_strict (its indexed sibling slotcask_guard_lookup
 * already rejected unknown flags).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include "slotcask.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static int corrupt_live_flag(const char *db_root, const char *dir,
                             const char *object, const char *key,
                             int splits) {
    uint8_t hash[16];
    compute_hash_raw(key, strlen(key), hash);
    int shard = compute_record_shard(hash, splits);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/%s/data/kf/%03d.kf",
             db_root, dir, object, shard);
    int fd = open(path, O_RDWR);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) < 0) { close(fd); return -1; }
    size_t nentries = (size_t)st.st_size / sizeof(SlotcaskKfEntry);
    void *map = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) { close(fd); return -1; }
    const SlotcaskKfEntry *entries = map;
    ssize_t hit = -1;
    for (size_t i = 0; i < nentries; i++) {
        if (entries[i].flag == 1 && memcmp(entries[i].hash, hash, 16) == 0) {
            hit = (ssize_t)i;
            break;
        }
    }
    munmap((void *)map, (size_t)st.st_size);
    if (hit < 0) { close(fd); return -1; }
    off_t off = (off_t)hit * (off_t)sizeof(SlotcaskKfEntry) +
                offsetof(SlotcaskKfEntry, flag);
    uint8_t bad = 3;
    if (pwrite(fd, &bad, 1, off) != 1) { close(fd); return -1; }
    fsync(fd);
    close(fd);
    return 0;
}

static int test_guard_corrupt_flag_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    /* No index on domain or owner — the guard must take the strict-walk
       fallback, whose flag handling is under test. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cf\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"domain:varchar:40\",\"owner:varchar:20\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create cf");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cf\","
        "\"key\":\"m1\",\"value\":{\"domain\":\"d.io\",\"owner\":\"a\"}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed m1");
    free(resp); resp = NULL;

    /* Sanity: before corruption the guard sees the row (matched:1). */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cf\","
        "\"key\":\"sanity\",\"value\":{\"domain\":\"z.io\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "guard sees row");
    ASSERT_CONTAINS(resp, "\"matched\":1", "matched before corruption");
    free(resp); resp = NULL;

    /* Corrupt m1's kf flag byte to an invalid value (3). */
    ASSERT_EQ_INT(corrupt_live_flag(env.db_root, "default", "cf", "m1", 8), 0,
                  "corrupt m1 kf flag to 3");

    /* A corrupt matching row must fail the absence proof — the guard
       rejects with an error, it never authorizes the write. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cf\","
        "\"key\":\"denied\",\"value\":{\"domain\":\"d.io\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "corrupt flag rejects absence proof");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"status\":\"inserted\"") == NULL,
                "corrupt flag never authorizes");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cf\","
                   "\"key\":\"denied\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "denied was never written");
    free(resp); resp = NULL;

    /* ...and the existence direction fails closed too (never matched:0). */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cf\","
        "\"key\":\"sanity\",\"value\":{\"owner\":\"b\"},"
        "\"if_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "corrupt flag rejects existence proof");
    free(resp); resp = NULL;

    /* Unguarded writes still progress — the rejection released the
       reservation cleanly. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cf\","
        "\"key\":\"plain\",\"value\":{\"domain\":\"p.io\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "unguarded write works");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-guard-corrupt-flag", test_guard_corrupt_flag_run)
