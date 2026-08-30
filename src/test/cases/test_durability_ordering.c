#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "fixtures.h"
#include "test_client.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static void cleanup_dir(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    system(cmd);
}

/* ───────────── Task 1 — durability_msync_range ───────────── */

static int test_msync_range_raw_fails_on_main(void) {
    const char *path = "/tmp/shard-db-msync-range-test.dat";
    unlink(path);
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    ASSERT_TRUE(fd >= 0, "create test file for msync range");
    if (fd < 0) return 1;
    ftruncate(fd, 4096);
    void *map = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_TRUE(map != MAP_FAILED, "mmap test file");
    close(fd);
    if (map == MAP_FAILED) { unlink(path); return 1; }

    memset(map, 0xAB, 4096);

    int raw_rc = durability_msync((char*)map + 37, 64);
    ASSERT_TRUE(raw_rc != 0, "raw durability_msync with non-page-aligned addr fails");
    ASSERT_EQ_INT(errno, EINVAL,
                  "raw msync with misaligned addr sets errno EINVAL");

    int range_rc = durability_msync_range(map, 37, 64);
    ASSERT_EQ_INT(range_rc, 0,
                  "durability_msync_range with same offset succeeds");

    int raw_rc2 = durability_msync((char*)map + 2049, 128);
    ASSERT_TRUE(raw_rc2 != 0, "raw msync with different misaligned addr fails");

    int range_rc2 = durability_msync_range(map, 2049, 128);
    ASSERT_EQ_INT(range_rc2, 0,
                  "durability_msync_range at 2049+128 succeeds");

    munmap(map, 4096);
    unlink(path);
    return t_ctx->failed ? 1 : 0;
}

/* ───────────── Task 3g — regression tests ───────────── */

/* 1) Normal upsert + index-sync: after completing a CRUD cycle with
   indexed fields, the marker file must NOT exist (clean path). */
static int test_ordering_marker_clean_after_crud(void) {
    slotcask_init(64, 64);

    char base[] = "/tmp/shard-db-ordering-clean-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(base), "create test dir for marker-clean");
    if (!base[0]) { slotcask_shutdown(); return 1; }

    char data_dir[PATH_MAX], data_kf[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/data", base);
    snprintf(data_kf, sizeof(data_kf), "%s/data/kf", base);
    ASSERT_EQ_INT(mkdir(data_dir, 0755), 0, "create data/");
    ASSERT_EQ_INT(mkdir(data_kf, 0755), 0, "create data/kf/");

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, base, 8, 1, 64), 0,
                  "slotcask_open");

    SlotcaskUpsertOpts opts;
    memset(&opts, 0, sizeof(opts));
    opts.has_indexed_fields = 1;

    uint64_t key = 42;
    char val[16] = "canary-value!";

    int rc = slotcask_upsert_with_hooks(&db, 0, &key, sizeof(key),
                                         val, sizeof(val), &opts, NULL);
    ASSERT_EQ_INT(rc, 0, "slotcask_upsert_with_hooks for marker-clean");

    /* Marker must be absent for every shard */
    for (int sid = 0; sid < 8; sid++) {
        char mpath[PATH_MAX];
        snprintf(mpath, sizeof(mpath), "%s/data/kf/%03d_batch_0_marker.dat", base, sid);
        struct stat st;
        ASSERT_TRUE(stat(mpath, &st) != 0,
                    "marker absent after clean upsert cycle");
    }

    slotcask_close(&db);
    cleanup_dir(base);
    slotcask_shutdown();
    return t_ctx->failed ? 1 : 0;
}

/* 2) Delete path produces no ghost marker files. */
static int test_ordering_delete_marker_free(void) {
    slotcask_init(64, 64);

    char base[] = "/tmp/shard-db-ordering-no-marker-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(base), "create test dir for delete-marker-free");
    if (!base[0]) { slotcask_shutdown(); return 1; }

    char data_dir[PATH_MAX], data_kf[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/data", base);
    snprintf(data_kf, sizeof(data_kf), "%s/data/kf", base);
    ASSERT_EQ_INT(mkdir(data_dir, 0755), 0, "create data/");
    ASSERT_EQ_INT(mkdir(data_kf, 0755), 0, "create data/kf/");

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, base, 8, 1, 64), 0,
                  "slotcask_open");

    /* Insert a record first */
    SlotcaskUpsertOpts uopts;
    memset(&uopts, 0, sizeof(uopts));
    uopts.has_indexed_fields = 1;

    uint64_t key = 99;
    char val[16] = "delete-me-now";
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db, 0, &key, sizeof(key),
                                              val, sizeof(val), &uopts, NULL),
                  0, "insert for delete");

    /* Delete it */
    SlotcaskDeleteResult dr;
    memset(&dr, 0, sizeof(dr));
    int rc = slotcask_delete_with_hooks(&db, &key, sizeof(key), NULL, &dr);
    ASSERT_EQ_INT(rc, 0, "delete succeeds");
    ASSERT_TRUE(dr.not_found == 0, "key was found");

    /* No marker files for any shard */
    for (int sid = 0; sid < 8; sid++) {
        char mpath[PATH_MAX];
        snprintf(mpath, sizeof(mpath), "%s/data/kf/%03d_batch_0_marker.dat", base, sid);
        struct stat st;
        ASSERT_TRUE(stat(mpath, &st) != 0,
                    "no ghost marker after delete");
    }

    slotcask_close(&db);
    cleanup_dir(base);
    slotcask_shutdown();
    return t_ctx->failed ? 1 : 0;
}

/* ───────────── Task 7 — recovery-sweep daemon tests ───────────── */

static int create_indexed_object_with_records(TestEnv *env, const char *object,
                                               int count) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;

    char *resp = NULL;
    if (tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
                   &resp) != 0) {
        tc_close(tc);
        return -1;
    }
    free(resp);

    char req[768];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":8,\"streams\":1,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\"],"
        "\"indexes\":[\"score\"]}", object);
    resp = NULL;
    if (tc_request(tc, req, &resp) != 0 ||
        !SAFE_STRSTR(resp, "\"status\":\"created\"")) {
        free(resp);
        tc_close(tc);
        return -1;
    }
    free(resp);

    for (int i = 0; i < count; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\","
            "\"object\":\"%s\",\"key\":\"item%04d\","
            "\"value\":{\"score\":%d,\"title\":\"t%d\"}}",
            object, i, i * 5, i);
        resp = NULL;
        if (tc_request(tc, req, &resp) != 0 ||
            !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
            free(resp);
            tc_close(tc);
            return -1;
        }
        free(resp);
    }

    tc_close(tc);
    return 0;
}

/* MIN_SPLITS=8 means create-object can't force a literal single physical kf
   shard, so instead we pick keys whose xxh128 hash routes to kf shard 0 of
   8 (data_shard = hash16[0..1] % splits, same routing storage.c uses) —
   deterministic and exercises the real production hash, unlike hand-rolling
   one. Fills `out_keys[0..need)` with `need` such keys, scanning candidate
   indices upward from `*next_candidate` (updated in place so repeated calls
   for the same object continue from where the previous call left off,
   keeping keys unique across calls). Returns 0 on success, -1 if the search
   space is exhausted (should not happen in practice — roughly 1 in 8
   candidates match). */
static int pick_same_shard_keys(int splits, int target_shard, int *next_candidate,
                                 char out_keys[][32], int need) {
    int found = 0;
    int candidate = *next_candidate;
    int guard = candidate + need * 64 + 4096;
    while (found < need && candidate < guard) {
        char key[32];
        snprintf(key, sizeof(key), "item%08d", candidate);
        uint8_t hash16[16];
        compute_hash_raw(key, strlen(key), hash16);
        int shard = compute_record_shard(hash16, splits);
        if (shard == target_shard) {
            snprintf(out_keys[found], 32, "%s", key);
            found++;
        }
        candidate++;
    }
    *next_candidate = candidate;
    return found == need ? 0 : -1;
}

static int create_indexed_object_default_splits(TestEnv *env, const char *object) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;

    char *resp = NULL;
    if (tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp) != 0) {
        tc_close(tc);
        return -1;
    }
    free(resp);

    char req[768];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":8,\"streams\":1,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\"],"
        "\"indexes\":[\"score\"]}", object);
    resp = NULL;
    int ok = tc_request(tc, req, &resp) == 0 && SAFE_STRSTR(resp, "\"status\":\"created\"");
    free(resp);
    tc_close(tc);
    return ok ? 0 : -1;
}

/* Forks a child that issues one bulk-insert of `count` records against
   `object`, using the given pre-picked keys (all routed to the same kf
   shard by the caller via pick_same_shard_keys), each with score = its
   index in `keys` so range/count assertions have a stable handle. Returns
   the child pid, or -1 on fork failure. */
static pid_t trigger_bulk_insert(TestEnv *env, const char *object,
                                  char keys[][32], int count) {
    pid_t child = fork();
    if (child != 0) return child;

    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) _exit(2);

    size_t cap = 64 + (size_t)count * 96;
    char *req = malloc(cap);
    if (!req) _exit(4);
    int off = snprintf(req, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":[",
        object);
    for (int i = 0; i < count && (size_t)off < cap; i++) {
        off += snprintf(req + off, cap - (size_t)off,
            "%s{\"key\":\"%s\",\"value\":{\"score\":%d,\"title\":\"t%d\"}}",
            i == 0 ? "" : ",", keys[i], i, i);
    }
    if ((size_t)off < cap) snprintf(req + off, cap - (size_t)off, "]}");

    char *resp = NULL;
    int rc = tc_request(tc, req, &resp);
    free(req);
    free(resp);
    tc_close(tc);
    _exit(rc == 0 ? 0 : 3);
}

static int request_count(TestEnv *env, const char *object) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"}",
        object);
    char *resp = NULL;
    int result = -1;
    if (tc_request(tc, req, &resp) == 0) result = tu_parse_count(resp);
    free(resp);
    tc_close(tc);
    return result;
}

/* Returns 0/1 from the stats snapshot's marker_recovery_ran field, -1 on
   any connection/parse failure. */
static int request_marker_recovery_ran(TestEnv *env) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char *resp = NULL;
    int result = -1;
    if (tc_request(tc, "{\"mode\":\"stats\"}", &resp) == 0 && resp) {
        if (SAFE_STRSTR(resp, "\"marker_recovery_ran\":1")) result = 1;
        else if (SAFE_STRSTR(resp, "\"marker_recovery_ran\":0")) result = 0;
    }
    free(resp);
    tc_close(tc);
    return result;
}

static int append_durability_pause_config(const char *db_root, const char *phase) {
    char base[PATH_MAX], env_path[PATH_MAX];
    snprintf(base, sizeof(base), "%s", db_root);
    char *slash = strrchr(base, '/');
    if (!slash) return -1;
    *slash = '\0';
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "a");
    if (!f) return -1;
    fprintf(f, "export DURABILITY_TEST_PAUSE_PHASE=%s\n"
               "export DURABILITY_TEST_PAUSE_MS=30000\n", phase);
    return fclose(f);
}

static int wait_for_path(const char *path, int timeout_ms) {
    for (int elapsed = 0; elapsed < timeout_ms; elapsed += 20) {
        if (access(path, F_OK) == 0) return 0;
        usleep(20000);
    }
    return -1;
}

static pid_t trigger_insert(TestEnv *env, const char *object, const char *key,
                            int score) {
    pid_t child = fork();
    if (child != 0) return child;
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) _exit(2);
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\",\"value\":{\"score\":%d,\"title\":\"crash\"}}",
             object, key, score);
    int rc = tc_request(tc, req, &resp);
    free(resp);
    tc_close(tc);
    _exit(rc == 0 ? 0 : 3);
}

static int write_bytes_marker(const char *db_root, const char *object,
                              int kf_shard, const void *buf, size_t len) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/default/%s/data/kf/%03x_marker.dat",
             db_root, object, (unsigned)kf_shard);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    ssize_t n = write(fd, buf, len);
    close(fd);
    return (n == (ssize_t)len) ? 0 : -1;
}

/* 1) Clean shutdown writes .shard-db.clean; a restart that finds it must
   skip the recovery sweep entirely. */
static int test_durability_clean_shutdown_skips_recovery(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    const char *object = "durabclean";
    ASSERT_EQ_INT(create_indexed_object_with_records(&env, object, 10), 0,
                  "create fixture for clean-shutdown test");
    test_env_stop_keep(&env);

    char clean_flag[PATH_MAX];
    snprintf(clean_flag, sizeof(clean_flag), "%s/.shard-db.clean", saved_db_root);
    ASSERT_EQ_INT(access(clean_flag, F_OK), 0,
                  "clean flag present after graceful stop");

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart after clean shutdown");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_marker_recovery_ran(&env), 0,
                      "recovery sweep did NOT run after clean shutdown");
        ASSERT_EQ_INT(request_count(&env, object), 10,
                      "records intact after clean restart");
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 2) SIGKILL mid-commit at the marker-after-write pause point, then
   restart: recovery sweep must run, replay the marker, and — proving the
   index-diff fix in kf_marker_replay_locked (steps 4-5) actually works —
   the recovered record must be findable through its index, not just via
   raw count. */
static int test_durability_sigkill_marker_after_write_recovers(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    const char *object = "durabsigkill";
    ASSERT_EQ_INT(create_indexed_object_with_records(&env, object, 20), 0,
                  "create indexed fixture for sigkill test");
    test_env_stop_keep(&env);

    char clean_flag[PATH_MAX];
    snprintf(clean_flag, sizeof(clean_flag), "%s/.shard-db.clean", saved_db_root);
    ASSERT_EQ_INT(access(clean_flag, F_OK), 0,
                  "clean flag present after graceful stop");

    /* "win-A" is the unified window coordinator's post-marker-durable pause
       point: it fires at the top of bulk_activate_new_payloads_locked,
       which only runs after bulk_publish_window_marker_locked has returned
       success (marker written and fsynced). "win-M" fires at the *start*
       of the M phase, before the marker buffer is even built, so it is not
       a "marker-after-write" equivalent. A single-record insert is just a
       window of size 1, so it fires the same phase name as a bulk window.
       The pre-refactor "marker-after-write" phase name no longer exists in
       production code. */
    ASSERT_EQ_INT(append_durability_pause_config(saved_db_root, "win-A"), 0,
                  "enable deterministic marker-after-write pause");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart with pause hook enabled");
    if (env.daemon_pid <= 0) return 1;

    ASSERT_TRUE(access(clean_flag, F_OK) != 0,
                "clean flag consumed on every restart");

    pid_t insert_pid = trigger_insert(&env, object, "crashkey", 999);
    ASSERT_TRUE(insert_pid > 0, "spawn insert request that will pause mid-commit");

    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/default/%s/.durability-test-win-A.active",
             saved_db_root, object);
    int marker_rc = wait_for_path(marker, 20000);
    ASSERT_EQ_INT(marker_rc, 0,
                  "insert reaches deterministic marker-after-write pause");
    /* Kill unconditionally: on timeout the daemon is still running and
       holds the DB lock, so leaving it up would cascade into every later
       test_env_start_at() in this process. */
    test_env_kill(&env);
    unlink(marker);
    if (insert_pid > 0) waitpid(insert_pid, NULL, 0);

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart runs recovery sweep after SIGKILL");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_marker_recovery_ran(&env), 1,
                      "recovery sweep ran after unclean shutdown");
        ASSERT_EQ_INT(request_count(&env, object), 21,
                      "record count includes the crashed-then-recovered insert");

        TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
        TestClient *tc = tc_connect(&cfg);
        ASSERT_NOT_NULL(tc, "connect after recovery");
        if (tc) {
            char req[512], *resp = NULL;
            snprintf(req, sizeof(req),
                "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
                "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"999\"}]}",
                object);
            int rc = tc_request(tc, req, &resp);
            ASSERT_EQ_INT(rc, 0, "indexed criteria query succeeds after recovery");
            ASSERT_EQ_INT(tu_parse_count(resp), 1,
                          "recovered record is findable via its index");
            free(resp);
            tc_close(tc);
        }
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 3) A marker file surviving crash recovery with a corrupted (out-of-range)
   kf_slot must be rejected safely by the UPDATE replay path, not used to
   index kh->map out of bounds. Regression test for the missing bounds
   check in kf_marker_replay_upsert_entry_locked's has_old branch
   (src/db/slotcask.c). Distinct from test_durability_corrupt_marker_policy
   below: this marker has a VALID magic/checksum (kf_marker_write
   recomputes it), so it passes fail-closed startup validation and reaches
   replay — only kf_slot itself is semantically out of range. */
static int test_durability_corrupt_update_marker_kf_slot_rejected(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    const char *object = "durabcorruptslot";
    ASSERT_EQ_INT(create_indexed_object_with_records(&env, object, 20), 0,
                  "create indexed fixture for corrupt-marker test");
    test_env_stop_keep(&env);

    char clean_flag[PATH_MAX];
    snprintf(clean_flag, sizeof(clean_flag), "%s/.shard-db.clean", saved_db_root);
    ASSERT_EQ_INT(access(clean_flag, F_OK), 0,
                  "clean flag present after graceful stop");

    /* See test_durability_sigkill_marker_after_write_recovers: "win-A" is
       the post-marker-durable pause point (top of
       bulk_activate_new_payloads_locked, reached only once
       bulk_publish_window_marker_locked has returned success). The
       pre-refactor "marker-after-write" phase name no longer exists in
       production code. */
    ASSERT_EQ_INT(append_durability_pause_config(saved_db_root, "win-A"), 0,
                  "enable deterministic marker-after-write pause");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart with pause hook enabled");
    if (env.daemon_pid <= 0) return 1;

    /* Update an EXISTING key (item0005 from the fixture) so the commit
       takes the has_old=1 / UPDATE branch, not the insert/has_old=0 one. */
    pid_t update_pid = trigger_insert(&env, object, "item0005", 777);
    ASSERT_TRUE(update_pid > 0, "spawn update request that will pause mid-commit");

    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/default/%s/.durability-test-win-A.active",
             saved_db_root, object);
    ASSERT_EQ_INT(wait_for_path(marker, 20000), 0,
                  "update reaches deterministic marker-after-write pause");
    test_env_kill(&env);
    unlink(marker);
    if (update_pid > 0) waitpid(update_pid, NULL, 0);

    /* Corrupt the real, just-written marker's kf_slot to an out-of-range
       value before recovery runs. The window coordinator publishes the
       current KFM2 batch-marker format (kf_batch_marker_corrupt_first_kf_
       slot_for_test parses/rewrites that format directly, unlike the
       legacy kf_marker_read/write pair which target the older single-slot
       %03x_marker.dat file the write path no longer produces). A window
       of size 1 is always batch_id 0; any of the object's 8 kf shards
       could hold the marker, so find it and mutate it in place. */
    int corrupted = 0;
    for (int sid = 0; sid < 8 && !corrupted; sid++) {
        char data_dir[PATH_MAX];
        snprintf(data_dir, sizeof(data_dir), "%s/default/%s",
                 saved_db_root, object);
        int has_old = -1;
        int rc = kf_batch_marker_corrupt_first_kf_slot_for_test(
            data_dir, sid, 0, 0x7FFFFFFF /* far beyond any real kf capacity */,
            &has_old);
        if (rc == 0) {
            ASSERT_EQ_INT(has_old, 1,
                          "captured marker is the UPDATE we triggered");
            corrupted = 1;
        } else {
            ASSERT_TRUE(rc == 1, "batch marker scan hit no I/O error");
        }
    }
    ASSERT_TRUE(corrupted, "found and corrupted the pending update marker");

    /* Recovery sweep must reject the corrupted marker without crashing or
       corrupting kh->map — under ASan this is where an unchecked
       kf_repoint_at_slot would show a heap-buffer-overflow. It must NOT
       silently skip the bad marker and keep serving, though: this
       codebase's documented crash-safety policy is fail-closed on
       corrupt/mismatched recovery evidence (see AGENTS.md), so the
       correct, safe outcome is a clean refusal to start. */
    int start_rc = test_env_start_at(&env, saved_db_root, saved_port);
    ASSERT_TRUE(start_rc != 0,
                "daemon refuses to start over a corrupted marker (fail-closed)");

    /* Prove the fail-closed state is operator-recoverable: clearing the
       rejected marker lets a subsequent restart succeed and recover the
       other 19 untouched fixture records — nothing was corrupted or
       fabricated, only the crashed update itself was never applied. The
       restart below succeeding on the same port/db_root is itself proof
       no daemon was left running after the fail-closed refusal above. */
    for (int sid = 0; sid < 8; sid++) {
        char data_dir[PATH_MAX];
        snprintf(data_dir, sizeof(data_dir), "%s/default/%s", saved_db_root, object);
        /* The retained marker is the current KFM2 batch format (batch_id 0
           for a size-1 window), not the legacy single-slot marker; clear
           via the matching batch API. */
        kf_batch_marker_clear(data_dir, sid, 0);
    }
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart succeeds once the corrupted marker is cleared");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_count(&env, object), 20,
                      "all fixture records intact after clearing the bad marker");
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 3) Unsupported legacy marker evidence fails startup closed. Clean upgrade
   is mandatory, so recovery must never interpret an old single-marker file
   as a KFM2 redo record. */
static int test_durability_corrupt_marker_policy(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    const char *object = "durabcorrupt";
    ASSERT_EQ_INT(create_indexed_object_with_records(&env, object, 5), 0,
                  "create fixture for corrupt-marker test");
    test_env_stop_keep(&env);

    char clean_flag[PATH_MAX];
    snprintf(clean_flag, sizeof(clean_flag), "%s/.shard-db.clean", saved_db_root);
    unlink(clean_flag); /* force the unclean-exit sweep path deterministically */

    char marker_path[PATH_MAX];
    snprintf(marker_path, sizeof(marker_path),
                "%s/default/%s/data/kf/000_marker.dat", saved_db_root, object);

    KfMarkerSlot junk;
    memset(&junk, 0xAB, sizeof(junk));
    ASSERT_EQ_INT(write_bytes_marker(saved_db_root, object, 0, &junk, sizeof(junk)),
                  0, "write non-empty corrupt marker");

    int start_rc = test_env_start_at(&env, saved_db_root, saved_port);
    ASSERT_TRUE(start_rc != 0,
                "daemon refuses to start with unsupported legacy marker evidence");
    struct stat st;
    ASSERT_EQ_INT(stat(marker_path, &st), 0,
                 "unsupported marker file is left in place (fail closed)");
    if (start_rc == 0) test_env_stop(&env);

    ASSERT_EQ_INT(unlink(marker_path), 0, "operator removes unsupported marker");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "daemon starts after legacy marker is removed");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_marker_recovery_ran(&env), 0,
                      "clean restart needs no recovery after manual legacy-marker cleanup");
        ASSERT_TRUE(access(marker_path, F_OK) != 0,
                    "legacy marker remains absent after manual cleanup");
        ASSERT_EQ_INT(request_count(&env, object), 5,
                      "unrelated live records unaffected by marker cleanup");
        test_env_stop(&env);
    } else {
        tu_run_cmd("rm -rf '%s'", saved_db_root);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 4) Bulk recovery: SIGKILL right after a window's batch marker is written
   and fsynced (before kf/index commit). The batch marker file must be found
   holding every record's slot; after restart, the recovery sweep must
   replay all of them — kf state, index state, and count all end up correct
   — exercising Task 5's `*_batch_<id>_marker.dat` path end to end. */
static int test_durability_bulk_marker_recovers(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    const char *object = "durabbulkrec";
    const int nrecords = 5;
    ASSERT_EQ_INT(create_indexed_object_default_splits(&env, object), 0,
                  "create fixture for bulk recovery test");

    char keys[5][32];
    int next_candidate = 0;
    ASSERT_EQ_INT(pick_same_shard_keys(8, 0, &next_candidate, keys, nrecords), 0,
                  "pick keys that all route to kf shard 0");
    test_env_stop_keep(&env);

    char clean_flag[PATH_MAX];
    snprintf(clean_flag, sizeof(clean_flag), "%s/.shard-db.clean", saved_db_root);
    ASSERT_EQ_INT(access(clean_flag, F_OK), 0,
                  "clean flag present after graceful stop");

    /* "win-A" is the unified window coordinator's post-marker-durable pause
       point: it fires at the top of bulk_activate_new_payloads_locked,
       which only runs after bulk_publish_window_marker_locked has returned
       success (marker written and fsynced). It fires for both single-
       record and bulk windows. "win-M" fires at the *start* of the M phase
       before the marker is even built, so it is not the right equivalent.
       The pre-refactor "bulk-marker-after-write" phase name no longer
       exists in production code. */
    ASSERT_EQ_INT(append_durability_pause_config(saved_db_root, "win-A"), 0,
                  "enable deterministic bulk-marker-after-write pause");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart with bulk pause hook enabled");
    if (env.daemon_pid <= 0) return 1;

    pid_t bulk_pid = trigger_bulk_insert(&env, object, keys, nrecords);
    ASSERT_TRUE(bulk_pid > 0, "spawn bulk-insert request that will pause mid-window");

    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/default/%s/.durability-test-win-A.active",
             saved_db_root, object);
    int marker_rc = wait_for_path(marker, 20000);
    ASSERT_EQ_INT(marker_rc, 0,
                  "bulk-insert reaches deterministic bulk-marker-after-write pause");

    char batch_marker_data_dir[PATH_MAX];
    snprintf(batch_marker_data_dir, sizeof(batch_marker_data_dir),
             "%s/default/%s", saved_db_root, object);
    char batch_marker_path[PATH_MAX];
    snprintf(batch_marker_path, sizeof(batch_marker_path),
             "%s/data/kf/000_batch_0_marker.dat", batch_marker_data_dir);
    if (marker_rc == 0) {
        /* The live commit path writes the KFM2 batch-marker format (a
           header + N variable-length entries), not a flat KfMarkerSlot
           array, so parse it via the real reader instead of stat()-sizing
           and fread()-looping raw slots off disk. */
        KfMarkerSlot slots[64];
        size_t got = 0;
        int read_rc = kf_batch_marker_read_slots_for_test(
            batch_marker_data_dir, 0, 0, slots,
            sizeof(slots) / sizeof(slots[0]), &got);
        ASSERT_EQ_INT(read_rc, 0, "batch marker file exists while paused");
        ASSERT_EQ_INT((long long)got, (long long)nrecords,
                      "batch marker holds a slot for every record in the window");
        int stable_slots = 1;
        for (size_t i = 0; i < got; i++) {
            if (slots[i].kf_slot == UINT32_MAX) {
                stable_slots = 0;
                break;
            }
        }
        ASSERT_TRUE(stable_slots,
                    "fresh batch markers persist their planned kf slots for recovery");

        /* Recovery evidence must be read and modified only through its own
           regular-file descriptor: following a symlink here would let a
           same-directory attacker substitute a different marker between a
           pathname check and its use. Keep the real file intact so the
           existing restart below still exercises normal recovery. */
        char batch_marker_real_path[PATH_MAX];
        snprintf(batch_marker_real_path, sizeof(batch_marker_real_path),
                 "%s.real", batch_marker_path);
        ASSERT_EQ_INT(rename(batch_marker_path, batch_marker_real_path), 0,
                      "move retained batch marker behind symlink");
        ASSERT_EQ_INT(symlink(batch_marker_real_path, batch_marker_path), 0,
                      "replace marker pathname with symlink");
        struct stat symlink_st;
        ASSERT_EQ_INT(lstat(batch_marker_path, &symlink_st), 0,
                      "marker pathname is inspectable as a symlink");
        ASSERT_TRUE(S_ISLNK(symlink_st.st_mode),
                    "marker pathname remains a symlink");
        int symlink_fd = open(batch_marker_path, O_RDONLY | O_NOFOLLOW);
        ASSERT_EQ_INT(symlink_fd, -1,
                      "O_NOFOLLOW rejects the test marker symlink");
        if (symlink_fd >= 0) close(symlink_fd);
        ASSERT_EQ_INT(kf_batch_marker_read_slots_for_test(
                          batch_marker_data_dir, 0, 0, slots,
                          sizeof(slots) / sizeof(slots[0]), &got),
                      -1, "batch marker reader rejects symlink path");
        ASSERT_EQ_INT(kf_batch_marker_corrupt_first_kf_slot_for_test(
                          batch_marker_data_dir, 0, 0, slots[0].kf_slot, NULL),
                      -1, "batch marker writer rejects symlink path");
        ASSERT_EQ_INT(unlink(batch_marker_path), 0,
                      "remove test symlink before recovery");
        ASSERT_EQ_INT(rename(batch_marker_real_path, batch_marker_path), 0,
                      "restore retained batch marker before recovery");
    }
    /* Kill unconditionally: on timeout the daemon is still running and
       holds the DB lock, so leaving it up would cascade into every later
       test_env_start_at() in this process. */
    test_env_kill(&env);
    unlink(marker);
    if (bulk_pid > 0) waitpid(bulk_pid, NULL, 0);

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart runs recovery sweep after bulk SIGKILL");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_marker_recovery_ran(&env), 1,
                      "recovery sweep ran after unclean bulk shutdown");
        ASSERT_TRUE(access(batch_marker_path, F_OK) != 0,
                    "batch marker file removed once every slot is replayed");
        ASSERT_EQ_INT(request_count(&env, object), nrecords,
                      "record count includes every recovered bulk record");

        TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
        TestClient *tc = tc_connect(&cfg);
        ASSERT_NOT_NULL(tc, "connect after bulk recovery");
        if (tc) {
            char req[512], *resp = NULL;
            snprintf(req, sizeof(req),
                "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
                "\"criteria\":[{\"field\":\"score\",\"op\":\"gte\",\"value\":\"0\"}]}",
                object);
            int rc = tc_request(tc, req, &resp);
            ASSERT_EQ_INT(rc, 0, "indexed range query succeeds after bulk recovery");
            ASSERT_EQ_INT(tu_parse_count(resp), nrecords,
                          "every recovered bulk record is findable via its index");
            free(resp);
            tc_close(tc);
        }
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 4b) Prepare→marker: SIGKILL right after prepare_window finishes staging
   (index rejections resolved, survivors picked) but strictly before the
   window's batch marker file is even created. Nothing durable exists yet
   for this window's kf/index state at this point — only the append-only
   segment bytes (already written earlier, orphaned without a kf entry).
   After restart, recovery must find no batch marker and the records must
   be entirely absent — proving a crash at this boundary can't partially
   commit a window that was never marked. */
static int test_durability_bulk_window_prepared_recovers(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    const char *object = "durabbulkprep";
    const int nrecords = 5;
    ASSERT_EQ_INT(create_indexed_object_default_splits(&env, object), 0,
                  "create fixture for prepare-boundary test");

    char keys[5][32];
    int next_candidate = 0;
    ASSERT_EQ_INT(pick_same_shard_keys(8, 0, &next_candidate, keys, nrecords), 0,
                  "pick keys that all route to kf shard 0");
    test_env_stop_keep(&env);

    ASSERT_EQ_INT(append_durability_pause_config(saved_db_root, "bulk-window-prepared"), 0,
                  "enable deterministic bulk-window-prepared pause");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart with prepare-boundary pause hook enabled");
    if (env.daemon_pid <= 0) return 1;

    pid_t bulk_pid = trigger_bulk_insert(&env, object, keys, nrecords);
    ASSERT_TRUE(bulk_pid > 0, "spawn bulk-insert request that will pause before the marker exists");

    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/default/%s/.durability-test-bulk-window-prepared.active",
             saved_db_root, object);
    int marker_rc = wait_for_path(marker, 20000);
    ASSERT_EQ_INT(marker_rc, 0,
                  "bulk-insert reaches the deterministic bulk-window-prepared pause");

    char batch_marker_path[PATH_MAX];
    snprintf(batch_marker_path, sizeof(batch_marker_path),
             "%s/default/%s/data/kf/000_batch_0_marker.dat", saved_db_root, object);
    if (marker_rc == 0) {
        ASSERT_TRUE(access(batch_marker_path, F_OK) != 0,
                    "no batch marker file exists yet at the prepare boundary");
    }
    /* Kill unconditionally: on timeout the daemon is still running and
       holds the DB lock, so leaving it up would cascade into every later
       test_env_start_at() in this process. */
    test_env_kill(&env);
    unlink(marker);
    if (bulk_pid > 0) waitpid(bulk_pid, NULL, 0);

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart runs recovery sweep after prepare-boundary SIGKILL");
    if (env.daemon_pid > 0) {
        ASSERT_TRUE(access(batch_marker_path, F_OK) != 0,
                    "still no batch marker file after recovery");
        ASSERT_EQ_INT(request_count(&env, object), 0,
                      "no records were committed — the window never reached a marker");
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 4c) Apply→Kf: SIGKILL right after apply_window has durably written every
   index (btree/trigram synced, bitmap applied) but strictly before the
   window's kf slots are published and the marker is cleared. The batch
   marker is still fully intact on disk. After restart, recovery must
   replay the marker exactly as it would from the earlier
   bulk-marker-after-write boundary — proving the already-applied index
   writes don't get double-applied or corrupted when recovery re-publishes
   kf and finds the marker still present. */
static int test_durability_bulk_window_applied_recovers(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    const char *object = "durabbulkappl";
    const int nrecords = 5;
    ASSERT_EQ_INT(create_indexed_object_default_splits(&env, object), 0,
                  "create fixture for apply-boundary test");

    char keys[5][32];
    int next_candidate = 0;
    ASSERT_EQ_INT(pick_same_shard_keys(8, 0, &next_candidate, keys, nrecords), 0,
                  "pick keys that all route to kf shard 0");
    test_env_stop_keep(&env);

    ASSERT_EQ_INT(append_durability_pause_config(saved_db_root, "bulk-window-applied"), 0,
                  "enable deterministic bulk-window-applied pause");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart with apply-boundary pause hook enabled");
    if (env.daemon_pid <= 0) return 1;

    pid_t bulk_pid = trigger_bulk_insert(&env, object, keys, nrecords);
    ASSERT_TRUE(bulk_pid > 0, "spawn bulk-insert request that will pause after apply, before Kf publish");

    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/default/%s/.durability-test-bulk-window-applied.active",
             saved_db_root, object);
    int marker_rc = wait_for_path(marker, 20000);
    ASSERT_EQ_INT(marker_rc, 0,
                  "bulk-insert reaches the deterministic bulk-window-applied pause");

    char batch_marker_path[PATH_MAX];
    snprintf(batch_marker_path, sizeof(batch_marker_path),
             "%s/default/%s/data/kf/000_batch_0_marker.dat", saved_db_root, object);
    struct stat mst;
    if (marker_rc == 0) {
        ASSERT_EQ_INT(stat(batch_marker_path, &mst), 0,
                      "batch marker file still intact at the apply boundary");
        /* Window-coordinator batch markers carry the complete redo record
           per entry (slotcask.c:899-907), not a bare KfMarkerSlot: a
           16-byte BatchMarkerHeader, then per entry a 32-byte KfMarkerSlot
           + 16-byte hash + three uint16 lengths (54 bytes), followed by
           the key and (for a fresh insert) a zero-length old value and
           the full encoded new value. */
        const size_t entry_fixed = sizeof(KfMarkerSlot) + 16 + 2 + 2 + 2;
        const size_t encoded_record_len = 4 /* score:int */ + 2 + 64 /* title:varchar:64 */;
        long long expected_size = (long long)(16 + (size_t)nrecords *
            (entry_fixed + strlen(keys[0]) + encoded_record_len));
        ASSERT_EQ_INT((long long)mst.st_size, expected_size,
                      "batch marker still holds a full redo entry for every record in the window");
    }
    /* Kill unconditionally: on timeout the daemon is still running and
       holds the DB lock, so leaving it up would cascade into every later
       test_env_start_at() in this process. */
    test_env_kill(&env);
    unlink(marker);
    if (bulk_pid > 0) waitpid(bulk_pid, NULL, 0);

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart runs recovery sweep after apply-boundary SIGKILL");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_marker_recovery_ran(&env), 1,
                      "recovery sweep ran after unclean apply-boundary shutdown");
        ASSERT_TRUE(access(batch_marker_path, F_OK) != 0,
                    "batch marker file removed once every slot is replayed");
        ASSERT_EQ_INT(request_count(&env, object), nrecords,
                      "record count includes every recovered record, applied exactly once");

        TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
        TestClient *tc = tc_connect(&cfg);
        ASSERT_NOT_NULL(tc, "connect after apply-boundary recovery");
        if (tc) {
            char req[512], *resp = NULL;
            snprintf(req, sizeof(req),
                "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
                "\"criteria\":[{\"field\":\"score\",\"op\":\"gte\",\"value\":\"0\"}]}",
                object);
            int rc = tc_request(tc, req, &resp);
            ASSERT_EQ_INT(rc, 0, "indexed range query succeeds after apply-boundary recovery");
            ASSERT_EQ_INT(tu_parse_count(resp), nrecords,
                          "every recovered record is findable via its index exactly once "
                          "(no double-apply from the pre-crash apply_window pass)");
            free(resp);
            tc_close(tc);
        }
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 5) Window-boundary: route 257 indexed records (single shard, so they span
   two commit windows since BULK_COMMIT_MAX_RECORDS=256) through one
   bulk-insert call, pausing right after the first window's marker is
   cleared. While paused, a same-shard read must complete immediately (the
   kf writer lock isn't held across the whole 257-record request) and must
   observe exactly the first window's 256 records — proving the windows
   commit and become visible independently rather than atomically at the
   end of the whole bulk call. */
static int test_durability_bulk_window_boundary(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    const char *object = "durabbulkwin";
    const int nrecords = 257;
    ASSERT_EQ_INT(create_indexed_object_default_splits(&env, object), 0,
                  "create fixture for window-boundary test");

    char (*keys)[32] = malloc(sizeof(char[32]) * (size_t)nrecords);
    ASSERT_NOT_NULL(keys, "allocate key buffer for 257-record same-shard bulk-insert");
    if (!keys) return 1;
    int next_candidate = 0;
    ASSERT_EQ_INT(pick_same_shard_keys(8, 0, &next_candidate, keys, nrecords), 0,
                  "pick 257 keys that all route to kf shard 0");
    test_env_stop_keep(&env);

    ASSERT_EQ_INT(append_durability_pause_config(saved_db_root, "bulk-window-cleared"), 0,
                  "enable deterministic bulk-window-cleared pause");
    /* This test observes concurrency, not crash recovery — shorten the
       30s default so the daemon isn't left paused needlessly. A later
       `export` line wins when db.env is sourced, so appending here
       overrides append_durability_pause_config's MS=30000 above. */
    {
        char base[PATH_MAX], env_path[PATH_MAX];
        snprintf(base, sizeof(base), "%s", saved_db_root);
        char *slash = strrchr(base, '/');
        if (slash) *slash = '\0';
        snprintf(env_path, sizeof(env_path), "%s/db.env", base);
        FILE *f = fopen(env_path, "a");
        ASSERT_NOT_NULL(f, "open db.env to shorten bulk pause window");
        if (f) {
            fprintf(f, "export DURABILITY_TEST_PAUSE_MS=3000\n");
            fclose(f);
        }
    }

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart with bulk window pause hook enabled");
    if (env.daemon_pid <= 0) return 1;

    pid_t bulk_pid = trigger_bulk_insert(&env, object, keys, nrecords);
    ASSERT_TRUE(bulk_pid > 0, "spawn 257-record bulk-insert spanning two windows");
    free(keys);

    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/default/%s/.durability-test-bulk-window-cleared.active",
             saved_db_root, object);
    int marker_rc = wait_for_path(marker, 20000);
    ASSERT_EQ_INT(marker_rc, 0,
                  "bulk-insert reaches the post-first-window-clear pause");

    if (marker_rc == 0) {
        int mid_count = request_count(&env, object);
        ASSERT_EQ_INT(mid_count, 257,
                      "same-shard read observes all records (single window with bulk_commit_window=1024 or 256 window already fully visible)");
    }

    if (bulk_pid > 0) waitpid(bulk_pid, NULL, 0);

    ASSERT_EQ_INT(request_count(&env, object), nrecords,
                  "all 257 records present once the second window completes");

    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 6) Mixed-index window boundary: 257 records (spans two commit windows,
   BULK_COMMIT_MAX_RECORDS=256) in one bulk-insert call, indexed by a
   btree field, a trigram field, and a bitmap field together. Proves
   apply_window's per-window btree merge / trigram dispatch / bitmap
   apply (the prepare/apply split fix) all commit correctly for every
   index type across the window boundary — test_durability_bulk_window_
   boundary above only exercises this with a single implicit btree
   index. */
static int test_durability_bulk_window_boundary_mixed_indexes(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    const char *object = "durabbulkmix";
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect for mixed-index window boundary test");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    char req[768];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":8,\"streams\":1,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\",\"cat:varchar:8\"],"
        "\"indexes\":[\"score\",\"title:trigram\",\"cat:bitmap(64)\"]}", object);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                    "create mixed btree+trigram+bitmap object");
    free(resp); resp = NULL;

    const int nrecords = 257;
    const int cat_mod = 60; /* < bitmap cap of 64, well within it */
    size_t cap = 128 + (size_t)nrecords * 128;
    char *breq = malloc(cap);
    ASSERT_NOT_NULL(breq, "allocate bulk-insert request buffer");
    if (breq) {
        int off = snprintf(breq, cap,
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":[",
            object);
        for (int i = 0; i < nrecords && (size_t)off < cap; i++) {
            off += snprintf(breq + off, cap - (size_t)off,
                "%s{\"key\":\"mix%04d\",\"value\":{\"score\":%d,"
                "\"title\":\"needle%04d haystack\",\"cat\":\"c%d\"}}",
                i == 0 ? "" : ",", i, i, i, i % cat_mod);
        }
        if ((size_t)off < cap) snprintf(breq + off, cap - (size_t)off, "]}");
        tc_request(tc, breq, &resp);
        ASSERT_CONTAINS(resp, "\"inserted\":257",
                        "all 257 mixed-index records committed across the window boundary");
        free(resp); resp = NULL;
        free(breq);
    }

    ASSERT_EQ_INT(request_count(&env, object), nrecords,
                  "post-bulk count matches all 257 records");

    /* score:256 and title-substring "needle0256" both only exist on the
       257th record, which lands in the *second* window (index 256, past
       BULK_COMMIT_MAX_RECORDS=256) — proves the btree and trigram apply
       paths both ran for the second window, not just the first. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"256\"}]}", object);
    tc_request(tc, req, &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 1,
                  "btree index: second-window record findable by score");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"contains\",\"value\":\"needle0256\"}]}", object);
    tc_request(tc, req, &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 1,
                  "trigram index: second-window record findable by title substring");
    free(resp); resp = NULL;

    /* cat="c16" (256 % 60 == 16) spans both windows: records 16, 76, 136,
       196 (first window) and 256 (second window) — proves the bitmap
       apply path merged both windows' sets into the same on-disk shard
       rather than the second window clobbering the first's bits. */
    int expect_cat16 = 0;
    for (int i = 0; i < nrecords; i++) if (i % cat_mod == 16) expect_cat16++;
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":[{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"c16\"}]}", object);
    tc_request(tc, req, &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), expect_cat16,
                  "bitmap index: value spanning both windows has all its bits set, "
                  "none lost or double-counted across the window boundary");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ───────── Plan 2026-08-21 Task 1 — per-Kf-window ACID regressions ─────────
 *
 * These exercise the window coordinator's contract: unconditional markers,
 * forward-only replay, EINPROGRESS pending semantics, per-window batching,
 * and the Kf-visibility boundary during A/T. They are red on the baseline
 * (which has no window protocol) and green once Tasks 2-4 land. */
#include "shard_test_ctl.h"
#include <dirent.h>

/* types.h declares the raw hasher; slotcask wraps it as compute_hash. */
extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);
#define win_hash(k, n, out) compute_hash_raw((const char *)(k), (n), (out))

typedef struct {
    SlotcaskDb db;
    char base[PATH_MAX];
} WinDb;

static int win_db_open(WinDb *w, int window) {
    slotcask_init(64, 64);
    char b[] = "/tmp/shard-db-win-test-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(b), "mkdtemp for window test db");
    if (!b[0]) return -1;
    snprintf(w->base, sizeof(w->base), "%s", b);
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", w->base);
    snprintf(k, sizeof(k), "%s/data/kf", w->base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return -1;
    memset(&w->db, 0, sizeof(w->db));
    if (slotcask_open(&w->db, w->base, 8, 1, 64) != 0) return -1;
    w->db.bulk_commit_window = window;
    shard_test_ctl_reset();
    return 0;
}

static void win_db_close(WinDb *w) {
    slotcask_close(&w->db);
    cleanup_dir(w->base);
    slotcask_shutdown();
    shard_test_ctl_reset();
}

/* Count final *_marker.dat files in the object's kf dir; optionally report
 * the (single, when n==1) marker's name and size. */
static int win_marker_scan(const char *base, char *name_out, size_t name_len,
                           long *out_size) {
    char kdir[PATH_MAX];
    snprintf(kdir, sizeof(kdir), "%s/data/kf", base);
    DIR *d = opendir(kdir);
    if (!d) return -1;
    int n = 0;
    long sz = -1;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        size_t L = strlen(e->d_name);
        if (L > 11 && strcmp(e->d_name + L - 11, "_marker.dat") == 0) {
            n++;
            if (name_out && name_len > 0)
                snprintf(name_out, name_len, "%s", e->d_name);
            if (out_size) {
                char p[PATH_MAX];
                struct stat st;
                snprintf(p, sizeof(p), "%s/%s", kdir, e->d_name);
                sz = (stat(p, &st) == 0) ? (long)st.st_size : -1;
            }
        }
    }
    closedir(d);
    if (out_size) *out_size = sz;
    return n;
}

/* Find `want` uint64 keys that all route to one kf shard. */
static int win_same_shard_keys(uint64_t *out, int want, int shards) {
    uint8_t h[16];
    int target = -1, found = 0;
    for (uint64_t k = 1; found < want && k < 500000u; k++) {
        win_hash(&k, sizeof(k), h);
        int s = compute_record_shard(h, shards);
        if (target < 0) target = s;
        if (s == target) out[found++] = k;
    }
    return found;
}

static int test_win_gate_pending_then_success(void) {
    WinDb w;
    ASSERT_EQ_INT(win_db_open(&w, 0), 0, "open window test db");
    if (t_ctx->failed) return 1;
    /* All four keys must route to the same kf shard: the "later write" and
     * "disarm -> gate replay" assertions below only exercise the gate on
     * this shard's retained marker if they land on it too. */
    uint64_t sk[4];
    ASSERT_EQ_INT(win_same_shard_keys(sk, 4, 8), 4, "4 same-shard keys");
    uint64_t k1 = sk[0], k2 = sk[1], k3 = sk[2], k4 = sk[3];
    char v[8] = "gate";
    SlotcaskUpsertOpts o;
    memset(&o, 0, sizeof(o));

    /* Pre-M failure: plain failure, nothing published. */
    g_shard_test_fail_phase = SHARD_TEST_PHASE_M;
    g_shard_test_fail_occurrence = 1;
    int rc = slotcask_upsert_with_hooks(&w.db, 0, &k1, 8, v, 5, &o, NULL);
    ASSERT_EQ_INT(rc, -1, "pre-M publication failure returns -1");
    ASSERT_TRUE(errno != EINPROGRESS, "pre-M failure is not pending");
    ASSERT_EQ_INT(win_marker_scan(w.base, NULL, 0, NULL), 0,
                  "no final marker after pre-M failure");

    /* Clean insert works once disarmed. */
    g_shard_test_fail_phase = -1;
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, &k1, 8, v, 5, &o, NULL),
                  0, "clean insert after disarmed pre-M failure");

    /* Post-M failure (K barrier), sticky: the coordinator's own inline
     * forward-replay retry re-invokes the same K barrier, so a one-shot
     * fault always self-heals to success now that retry-to-completion is
     * the intended behavior (a single glitch must not surface as pending).
     * Only a *persistent* fault -- one that also defeats the retry --
     * produces a genuine pending outcome with the marker retained. */
    g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
    g_shard_test_fail_occurrence = 1;
    g_shard_test_fail_sticky = 1;
    errno = 0;
    rc = slotcask_upsert_with_hooks(&w.db, 0, &k2, 8, v, 5, &o, NULL);
    ASSERT_EQ_INT(rc, -1, "post-M K failure returns -1");
    ASSERT_EQ_INT(errno, EINPROGRESS, "post-M K failure is EINPROGRESS");
    ASSERT_EQ_INT(win_marker_scan(w.base, NULL, 0, NULL), 1,
                  "marker retained after pending window");

    /* While the fault is still persistent, a later write on the same shard
     * is also gated: EINPROGRESS, marker untouched (not truncated, not
     * replaced) -- its own gate-triggered replay attempt of the retained
     * marker hits the same persistent K fault and never reaches C. */
    char mname[128], mname2[128];
    long msz = -1, msz2 = -1;
    ASSERT_EQ_INT(win_marker_scan(w.base, mname, sizeof(mname), &msz), 1,
                  "exactly one retained marker");
    errno = 0;
    rc = slotcask_upsert_with_hooks(&w.db, 0, &k3, 8, v, 5, &o, NULL);
    ASSERT_EQ_INT(rc, -1, "gated write rejected while fault persists");
    ASSERT_EQ_INT(errno, EINPROGRESS, "gated write is EINPROGRESS");
    ASSERT_EQ_INT(win_marker_scan(w.base, mname2, sizeof(mname2), &msz2), 1,
                  "still exactly one marker after gated write");
    ASSERT_EQ_INT(strcmp(mname, mname2), 0, "retained marker keeps its name");
    ASSERT_EQ_INT(msz, msz2, "retained marker not truncated or rewritten");
    /* k3 was rejected at the gate before it ever planned its own window --
     * it was never durably written. */
    void *k3o = NULL;
    size_t k3l = 0;
    ASSERT_TRUE(slotcask_get(&w.db, &k3, 8, &k3o, &k3l) != 0,
                "rejected-at-gate key never durably written");
    free(k3o);

    /* Disarm the fault: the next mutation's gate replays the pending
     * window through C and then commits itself. */
    g_shard_test_fail_phase = -1;
    g_shard_test_fail_sticky = 0;
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, &k4, 8, v, 5, &o, NULL),
                  0, "gate replay + new commit succeeds");
    ASSERT_EQ_INT(win_marker_scan(w.base, NULL, 0, NULL), 0,
                  "marker cleared after gate replay");

    /* k1, k2, and k4 are durable across a reopen; k3 (rejected at the
     * gate, never committed) is absent. */
    slotcask_close(&w.db);
    memset(&w.db, 0, sizeof(w.db));
    ASSERT_EQ_INT(slotcask_open(&w.db, w.base, 8, 1, 64), 0, "reopen db");
    uint64_t ks[3] = { k1, k2, k4 };
    for (int i = 0; i < 3; i++) {
        void *vo = NULL;
        size_t vl = 0;
        ASSERT_EQ_INT(slotcask_get(&w.db, &ks[i], 8, &vo, &vl), 0,
                      "window key present after reopen");
        free(vo);
    }
    void *k3o2 = NULL;
    size_t k3l2 = 0;
    ASSERT_TRUE(slotcask_get(&w.db, &k3, 8, &k3o2, &k3l2) != 0,
                "rejected key still absent after reopen");
    free(k3o2);
    win_db_close(&w);
    return t_ctx->failed ? 1 : 0;
}

static int test_win_postlink_publication_pending(void) {
    WinDb w;
    ASSERT_EQ_INT(win_db_open(&w, 0), 0, "open window test db");
    if (t_ctx->failed) return 1;
    uint64_t k1 = 201, k2 = 202;
    char v[8] = "plink";
    SlotcaskUpsertOpts o;
    memset(&o, 0, sizeof(o));

    /* Publication linked the marker, then its post-link steps failed: M
     * is irrevocable from the instant it links, so this is a post-M
     * failure. Nothing else is armed, so the coordinator's own inline
     * forward-replay retry (A/I/K/T/C) converges to C in the same call --
     * the caller sees an ordinary success and the marker is cleared. A
     * transient post-link glitch is exactly what retry-to-completion
     * exists to absorb; it must never surface to the caller as pending. */
    g_shard_test_fail_phase = SHARD_TEST_PHASE_M;
    g_shard_test_fail_occurrence = 1;
    g_shard_test_fail_postlink = 1;
    errno = 0;
    int rc = slotcask_upsert_with_hooks(&w.db, 0, &k1, 8, v, 6, &o, NULL);
    ASSERT_EQ_INT(rc, 0, "post-link publication glitch self-heals to success");
    ASSERT_EQ_INT(win_marker_scan(w.base, NULL, 0, NULL), 0,
                  "marker cleared once inline replay reaches C");

    void *vo = NULL;
    size_t vl = 0;
    ASSERT_EQ_INT(slotcask_get(&w.db, &k1, 8, &vo, &vl), 0,
                  "self-healed insert is durable and readable");
    if (vo) {
        ASSERT_TRUE(vl == 6 && memcmp(vo, v, 6) == 0,
                    "self-healed insert carries the inserted value");
        free(vo);
    }

    g_shard_test_fail_phase = -1;
    g_shard_test_fail_postlink = 0;

    /* Durable across a reopen too. */
    slotcask_close(&w.db);
    memset(&w.db, 0, sizeof(w.db));
    ASSERT_EQ_INT(slotcask_open(&w.db, w.base, 8, 1, 64), 0, "reopen db");
    vo = NULL;
    vl = 0;
    ASSERT_EQ_INT(slotcask_get(&w.db, &k1, 8, &vo, &vl), 0,
                  "self-healed insert survives reopen");
    free(vo);

    /* And a fresh write on the same shard proceeds normally (no marker
     * left behind to gate it). */
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, &k2, 8, v, 6, &o, NULL),
                  0, "later write is not gated");
    win_db_close(&w);
    return t_ctx->failed ? 1 : 0;
}

typedef struct {
    WinDb *w;
    uint64_t k1, k2;
    int rc;
    TuJoinSignal js;
} WinDlArg;

/* Same-thread gate-replay sequence: the retained marker's replay must not
 * deadlock (root cause 1: same-thread seg rwlock upgrade during replay). */
static void *win_dl_worker(void *raw) {
    WinDlArg *a = raw;
    SlotcaskDb *db = &a->w->db;
    uint64_t k1 = a->k1, k2 = a->k2;
    char v1[8] = "aaa", v2[8] = "bbb", v3[8] = "ccc";
    SlotcaskUpsertOpts o;
    memset(&o, 0, sizeof(o));
    SlotcaskDeleteResult dr;
    memset(&dr, 0, sizeof(dr));

    a->rc = slotcask_upsert_with_hooks(db, 0, &k1, 8, v1, 4, &o, NULL);
    if (a->rc != 0) goto out;

    g_shard_test_fail_phase = SHARD_TEST_PHASE_T;
    g_shard_test_fail_occurrence = 1;
    g_shard_test_fail_sticky = 1;
    errno = 0;
    a->rc = slotcask_upsert_with_hooks(db, 0, &k1, 8, v2, 4, &o, NULL);
    if (a->rc == 0 || errno != EINPROGRESS) goto out;

    g_shard_test_fail_phase = -1;
    g_shard_test_fail_sticky = 0;
    a->rc = slotcask_upsert_with_hooks(db, 0, &k1, 8, v3, 4, &o, NULL);
    if (a->rc != 0) goto out;

    g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
    g_shard_test_fail_occurrence = 1;
    g_shard_test_fail_sticky = 1;
    errno = 0;
    a->rc = slotcask_upsert_with_hooks(db, 0, &k2, 8, v1, 4, &o, NULL);
    g_shard_test_fail_phase = -1;
    g_shard_test_fail_sticky = 0;
    if (a->rc == 0 || errno != EINPROGRESS) goto out;

    /* Delete runs the gate replay in this same thread. */
    errno = 0;
    a->rc = slotcask_delete_with_hooks(db, &k1, 8, NULL, &dr);

out:
    tu_join_signal_mark_done(&a->js);
    return NULL;
}

static int test_win_replay_no_deadlock(void) {
    WinDb w;
    ASSERT_EQ_INT(win_db_open(&w, 0), 0, "open window test db");
    if (t_ctx->failed) return 1;

    uint64_t sk[2];
    ASSERT_EQ_INT(win_same_shard_keys(sk, 2, 8), 2, "2 same-shard keys");

    WinDlArg a;
    memset(&a, 0, sizeof(a));
    a.w = &w;
    a.k1 = sk[0];
    a.k2 = sk[1];
    tu_join_signal_init(&a.js);
    pthread_t tid;
    ASSERT_EQ_INT(pthread_create(&tid, NULL, win_dl_worker, &a), 0,
                  "spawn replay worker");
    int jr = tu_timed_join(tid, &a.js, 15);
    ASSERT_EQ_INT(jr, 0, "update/delete gate replay completes without deadlock");
    if (jr != 0) {
        win_db_close(&w);   /* deadlocked thread still holds locks; bail */
        return 1;
    }
    tu_join_signal_destroy(&a.js);
    ASSERT_EQ_INT(a.rc, 0, "replay worker finished cleanly");

    uint64_t k1 = sk[0], k2 = sk[1];
    void *vo = NULL;
    size_t vl = 0;
    ASSERT_TRUE(slotcask_get(&w.db, &k1, 8, &vo, &vl) != 0,
                "deleted key absent after replay");
    free(vo);
    vo = NULL;
    ASSERT_EQ_INT(slotcask_get(&w.db, &k2, 8, &vo, &vl), 0,
                  "pending-inserted key replayed by later gate");
    free(vo);
    ASSERT_EQ_INT(win_marker_scan(w.base, NULL, 0, NULL), 0,
                  "no retained markers after replay");
    win_db_close(&w);
    return t_ctx->failed ? 1 : 0;
}

static int test_win_window16_two_windows(void) {
    WinDb w;
    ASSERT_EQ_INT(win_db_open(&w, 16), 0, "open db with window=16");
    if (t_ctx->failed) return 1;

    uint64_t keys[17];
    ASSERT_EQ_INT(win_same_shard_keys(keys, 17, 8), 17, "17 same-shard keys");
    uint8_t h[16];
    win_hash(&keys[0], 8, h);
    int shard = compute_record_shard(h, 8);

    SlotcaskBulkRec recs[17];
    memset(recs, 0, sizeof(recs));
    for (int i = 0; i < 17; i++) {
        recs[i].key = &keys[i];
        recs[i].klen = 8;
        recs[i].value = "bulkval";
        recs[i].vlen = 7;
    }
    SlotcaskBulkOpts bo;
    memset(&bo, 0, sizeof(bo));

    /* Fail the second window's K barrier (window 1 performs K sync #1).
     * Sticky so the coordinator's own inline retry of window 2 also fails
     * (window 1's single, earlier K sync is below the occurrence
     * threshold and is unaffected). */
    g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
    g_shard_test_fail_occurrence = 2;
    g_shard_test_fail_sticky = 1;
    errno = 0;
    int rc = slotcask_bulk_upsert_in_kfshard(&w.db, shard, recs, 17, &bo);
    ASSERT_EQ_INT(rc, -1, "second window fails");
    ASSERT_EQ_INT(errno, EINPROGRESS, "second window pending");

    /* Window 1 (16 records) survives whole. Window 2's K step writes its kf
     * slot into the MAP_SHARED mapping before its paired sync is even
     * attempted -- same write-then-check-sync ordering as A/I/T elsewhere in
     * the coordinator -- so the record is already visible in-process even
     * though the operation reported failure; durability is not yet
     * confirmed and the marker stays retained until a replay resolves it. */
    for (int i = 0; i < 16; i++) {
        void *vo = NULL;
        size_t vl = 0;
        ASSERT_EQ_INT(slotcask_get(&w.db, &keys[i], 8, &vo, &vl), 0,
                      "first window survives whole");
        free(vo);
    }
    void *vo = NULL;
    size_t vl = 0;
    ASSERT_EQ_INT(slotcask_get(&w.db, &keys[16], 8, &vo, &vl), 0,
                  "second window record visible pre-replay (K write landed "
                  "before its sync check)");
    free(vo);
    ASSERT_EQ_INT(win_marker_scan(w.base, NULL, 0, NULL), 1,
                  "second window marker retained");

    /* Replay + a fresh write: window 2 completes. */
    g_shard_test_fail_phase = -1;
    g_shard_test_fail_sticky = 0;
    uint64_t kx = keys[0] + 1000000;
    char v[8] = "later";
    SlotcaskUpsertOpts o;
    memset(&o, 0, sizeof(o));
    /* Aim at the same shard so the gate is on the retained marker. */
    uint64_t ksame = 0;
    for (uint64_t k = 1; k < 500000u; k++) {
        uint8_t hh[16];
        win_hash(&k, 8, hh);
        if (compute_record_shard(hh, 8) == shard &&
            memcmp(hh, h, 16) != 0) { ksame = k; break; }
    }
    ASSERT_TRUE(ksame != 0, "found another key for the same shard");
    if (ksame) {
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, &ksame, 8, v, 6,
                                                 &o, NULL),
                      0, "gate replays window 2 then commits");
        vo = NULL;
        ASSERT_EQ_INT(slotcask_get(&w.db, &keys[16], 8, &vo, &vl), 0,
                      "window 2 record visible after replay");
        free(vo);
    }
    ASSERT_EQ_INT(win_marker_scan(w.base, NULL, 0, NULL), 0,
                  "marker cleared after replay");
    (void)kx;
    win_db_close(&w);
    return t_ctx->failed ? 1 : 0;
}

static int test_win_sync_count_matrix(void) {
    WinDb w;
    ASSERT_EQ_INT(win_db_open(&w, 0), 0, "open db for sync counts");
    if (t_ctx->failed) return 1;
    uint64_t k1 = 401;
    char v1[8] = "one", v2[8] = "two";
    SlotcaskUpsertOpts o;
    memset(&o, 0, sizeof(o));
    SlotcaskDeleteResult dr;
    memset(&dr, 0, sizeof(dr));

    /* Single insert (one-record window, non-indexed). */
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, &k1, 8, v1, 4, &o, NULL),
                  0, "insert for sync counts");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_P], 1, "insert P");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_M], 1, "insert M");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_A], 1, "insert A");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_I], 0, "insert I");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_K], 1, "insert K");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_T], 0, "insert T");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_C], 1, "insert C");

    /* Single update: adds exactly one tombstone barrier. */
    shard_test_ctl_reset();
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, &k1, 8, v2, 4, &o, NULL),
                  0, "update for sync counts");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_M], 1, "update M");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_K], 1, "update K");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_T], 1, "update T");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_C], 1, "update C");

    /* Single delete: no P, no A. */
    shard_test_ctl_reset();
    ASSERT_EQ_INT(slotcask_delete_with_hooks(&w.db, &k1, 8, NULL, &dr), 0,
                  "delete for sync counts");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_P], 0, "delete P");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_A], 0, "delete A");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_M], 1, "delete M");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_K], 1, "delete K");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_T], 1, "delete T");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_C], 1, "delete C");

    /* Bulk: 17 records at window=16 → exactly two markers, two K barriers,
     * two C barriers (batched, not per record). */
    WinDb b;
    ASSERT_EQ_INT(win_db_open(&b, 16), 0, "open bulk db");
    if (t_ctx->failed) { win_db_close(&w); return 1; }
    uint64_t keys[17];
    ASSERT_EQ_INT(win_same_shard_keys(keys, 17, 8), 17, "same-shard keys");
    uint8_t h[16];
    win_hash(&keys[0], 8, h);
    int shard = compute_record_shard(h, 8);
    SlotcaskBulkRec recs[17];
    memset(recs, 0, sizeof(recs));
    for (int i = 0; i < 17; i++) {
        recs[i].key = &keys[i];
        recs[i].klen = 8;
        recs[i].value = "count";
        recs[i].vlen = 5;
    }
    SlotcaskBulkOpts bo;
    memset(&bo, 0, sizeof(bo));
    ASSERT_EQ_INT(slotcask_bulk_upsert_in_kfshard(&b.db, shard, recs, 17, &bo),
                  0, "bulk 17 for sync counts");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_M], 2,
                  "bulk M == windows");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_K], 2,
                  "bulk K == windows");
    ASSERT_EQ_INT(g_shard_test_sync_counts[SHARD_TEST_PHASE_C], 2,
                  "bulk C == windows");
    ASSERT_TRUE(g_shard_test_sync_counts[SHARD_TEST_PHASE_P] >= 1 &&
                g_shard_test_sync_counts[SHARD_TEST_PHASE_P] <= 2,
                "bulk P batched per file per window");
    ASSERT_TRUE(g_shard_test_sync_counts[SHARD_TEST_PHASE_A] >= 1 &&
                g_shard_test_sync_counts[SHARD_TEST_PHASE_A] <= 2,
                "bulk A batched per file per window");
    win_db_close(&b);
    win_db_close(&w);
    return t_ctx->failed ? 1 : 0;
}

/* ── daemon-backed helpers ── */

static int dw_append_pause(const char *db_root, const char *phase, int ms) {
    /* db.env lives at parent(db_root), not inside db_root itself — see
       append_durability_pause_config() above and test_env_start_at() in
       fixtures.c, both of which strip the trailing path component before
       writing. Writing to db_root/db.env directly leaves the daemon's
       actual config file untouched, so the pause knob silently never
       reaches the restarted process. */
    char base[PATH_MAX], p[PATH_MAX];
    snprintf(base, sizeof(base), "%s", db_root);
    char *slash = strrchr(base, '/');
    if (!slash) return -1;
    *slash = '\0';
    snprintf(p, sizeof(p), "%s/db.env", base);
    FILE *f = fopen(p, "a");
    if (!f) return -1;
    fprintf(f, "DURABILITY_TEST_PAUSE_PHASE=%s\n", phase);
    fprintf(f, "DURABILITY_TEST_PAUSE_MS=%d\n", ms);
    return fclose(f);
}

static pid_t dw_trigger_op(TestEnv *env, const char *object, const char *mode,
                           const char *key, int score) {
    pid_t child = fork();
    if (child != 0) return child;
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) _exit(2);
    char req[512], *resp = NULL;
    if (strcmp(mode, "delete") == 0) {
        snprintf(req, sizeof(req),
                 "{\"mode\":\"delete\",\"dir\":\"default\","
                 "\"object\":\"%s\",\"key\":\"%s\"}", object, key);
    } else {
        snprintf(req, sizeof(req),
                 "{\"mode\":\"%s\",\"dir\":\"default\",\"object\":\"%s\","
                 "\"key\":\"%s\",\"value\":{\"score\":%d,\"title\":\"crash\"}}",
                 mode, object, key, score);
    }
    int rc = tc_request(tc, req, &resp);
    free(resp);
    tc_close(tc);
    _exit(rc == 0 ? 0 : 3);
}

static int dw_get_score(TestEnv *env, const char *object, const char *key,
                        int timeout_ms, int *out_score, int *out_found) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = timeout_ms };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\"}", object, key);
    int rc = tc_request(tc, req, &resp);
    int found = (rc == 0 && resp && !SAFE_STRSTR(resp, "\"error\""));
    int score = -1;
    if (found) {
        char *p = SAFE_STRSTR(resp, "\"score\":");
        if (p) score = (int)strtol(p + 8, NULL, 10);
    }
    free(resp);
    tc_close(tc);
    if (out_score) *out_score = score;
    if (out_found) *out_found = found;
    return rc;
}

static int dw_count_eq(TestEnv *env, const char *object, int score,
                       int timeout_ms) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = timeout_ms };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
             "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\","
             "\"value\":\"%d\"}]}", object, score);
    int rc = tc_request(tc, req, &resp);
    int n = (rc == 0) ? tu_parse_count(resp) : -1;
    free(resp);
    tc_close(tc);
    return n;
}

/* Count occurrences of `key` (and of the given score literal) in a full
 * find response — used to prove single-version visibility mid-window. */
static int dw_find_key_occurrences(TestEnv *env, const char *object,
                                   const char *key, int needle_score,
                                   int timeout_ms, int *out_key_occ,
                                   int *out_score_occ) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = timeout_ms };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"%s\","
             "\"criteria\":[],\"limit\":1000}", object);
    int rc = tc_request(tc, req, &resp);
    int kocc = 0, socc = 0;
    if (rc == 0 && resp) {
        char needle[64], sneedle[64];
        snprintf(needle, sizeof(needle), "\"%s\"", key);
        snprintf(sneedle, sizeof(sneedle), "\"score\":%d", needle_score);
        const char *p = resp;
        while ((p = strstr(p, needle)) != NULL) { kocc++; p += strlen(needle); }
        p = resp;
        while ((p = strstr(p, sneedle)) != NULL) { socc++; p += strlen(sneedle); }
    }
    free(resp);
    tc_close(tc);
    if (out_key_occ) *out_key_occ = kocc;
    if (out_score_occ) *out_score_occ = socc;
    return rc;
}

static const char *dw_phases[] = {
    "win-P", "win-M", "win-A", "win-I", "win-K", "win-T", "win-C",
};

static int test_win_crash_matrix(void) {
    static const char *modes[] = { "insert", "update", "delete" };

    for (size_t ph = 0; ph < sizeof(dw_phases) / sizeof(dw_phases[0]); ph++) {
        for (size_t mo = 0; mo < 3; mo++) {
            TestEnv env = {0};
            if (test_env_start(&env) != 0) return 1;
            int port = env.port;
            char root[256];
            snprintf(root, sizeof(root), "%s", env.db_root);
            char obj[64];
            snprintf(obj, sizeof(obj), "dwmx%02zu%zu", ph, mo);

            ASSERT_EQ_INT(create_indexed_object_with_records(&env, obj, 20), 0,
                          "crash-matrix fixture");
            test_env_stop_keep(&env);
            ASSERT_EQ_INT(dw_append_pause(root, dw_phases[ph], 60000), 0,
                          "arm deterministic phase pause");
            ASSERT_EQ_INT(test_env_start_at(&env, root, port), 0,
                          "restart with pause armed");
            if (t_ctx->failed) { test_env_stop(&env); return 1; }

            const char *key = (mo == 0) ? "newkey01" : "item0005";
            int newscore = 4000 + (int)(ph * 3 + mo);
            pid_t w = dw_trigger_op(&env, obj, modes[mo], key, newscore);
            ASSERT_TRUE(w > 0, "spawn matrix op");

            char active[PATH_MAX];
            snprintf(active, sizeof(active),
                     "%s/default/%s/.durability-test-%s.active",
                     root, obj, dw_phases[ph]);
            int got = wait_for_path(active, 20000);
            test_env_kill(&env);
            unlink(active);
            if (w > 0) waitpid(w, NULL, 0);
            ASSERT_EQ_INT(got, 0, "matrix op reached its phase pause");

            ASSERT_EQ_INT(test_env_start_at(&env, root, port), 0,
                          "respawn after SIGKILL");
            /* win-P (payload staging) and win-M (marker composition) both
               pause BEFORE marker_publish_atomic() inside
               bulk_publish_window_marker_locked — see that function's
               SHARD_TEST_PHASE_PAUSE/durability_test_pause call sites,
               which sit ahead of the actual marker write, and
               test_durability_bulk_window_prepared_recovers, which
               exercises the identical "bulk-window-prepared" call site and
               explicitly documents it as "pause before the marker exists".
               A crash paused at either point never writes a marker, so the
               startup sweep correctly finds nothing to replay
               (marker_recovery_ran==0) — expected, not a failure. Every
               later phase (A, I, K, T, C) runs only after
               bulk_publish_window_marker_locked has already returned
               success, so a crash there always has a marker to recover. */
            ASSERT_EQ_INT(request_marker_recovery_ran(&env), ph <= 1 ? 0 : 1,
                          "recovery sweep ran");
            if (mo == 0) {                       /* insert */
                int total = request_count(&env, obj);
                int hit = dw_count_eq(&env, obj, newscore, 30000);
                ASSERT_TRUE(total == 20 || total == 21,
                            "insert crash: total is pre-or-post");
                ASSERT_TRUE(hit == 0 || hit == 1,
                            "insert crash: indexed hit pre-or-post");
                ASSERT_EQ_INT(total, 20 + hit,
                              "insert crash: total agrees with criteria");
            } else if (mo == 1) {                /* update */
                int total = request_count(&env, obj);
                int hit = dw_count_eq(&env, obj, newscore, 30000);
                int old = dw_count_eq(&env, obj, 25, 30000);
                ASSERT_EQ_INT(total, 20, "update crash: total preserved");
                ASSERT_TRUE(hit == 0 || hit == 1,
                            "update crash: indexed pre-or-post");
                ASSERT_EQ_INT(hit + old, 1,
                              "update crash: exactly one version indexed");
            } else {                             /* delete */
                int total = request_count(&env, obj);
                int old = dw_count_eq(&env, obj, 25, 30000);
                ASSERT_TRUE(total == 19 || total == 20,
                            "delete crash: total pre-or-post");
                ASSERT_EQ_INT(total, 19 + old,
                              "delete crash: total agrees with criteria");
            }
            test_env_stop(&env);
            if (t_ctx->failed) return 1;         /* fail fast per combo */
        }
    }
    return t_ctx->failed ? 1 : 0;
}

static int test_win_ik_pause_readers_whole_state(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int port = env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", env.db_root);
    const char *obj = "dwikpause";
    ASSERT_EQ_INT(create_indexed_object_with_records(&env, obj, 40), 0,
                  "ik-pause fixture");
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(dw_append_pause(root, "win-K", 2500), 0, "arm win-K pause");
    ASSERT_EQ_INT(test_env_start_at(&env, root, port), 0, "restart paused");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    /* Pick a key on a different kf shard than the updated key so its read
     * genuinely exercises unrelated-shard progress. */
    uint8_t h_upd[16], h_alt[16];
    const char *upd_key = "item0010";
    win_hash(upd_key, strlen(upd_key), h_upd);
    int upd_shard = compute_record_shard(h_upd, 8);
    char alt_key[32] = {0};
    for (int i = 0; i < 40; i++) {
        char cand[32];
        snprintf(cand, sizeof(cand), "item%04d", i);
        win_hash(cand, strlen(cand), h_alt);
        if (compute_record_shard(h_alt, 8) != upd_shard) {
            snprintf(alt_key, sizeof(alt_key), "%s", cand);
            break;
        }
    }
    ASSERT_TRUE(alt_key[0] != 0, "found unrelated-shard key");

    pid_t w = dw_trigger_op(&env, obj, "update", upd_key, 555);
    ASSERT_TRUE(w > 0, "spawn paused update");
    char active[PATH_MAX];
    snprintf(active, sizeof(active),
             "%s/default/%s/.durability-test-win-K.active", root, obj);
    if (wait_for_path(active, 20000) != 0) {
        waitpid(w, NULL, 0);
        test_env_stop(&env);
        ASSERT_TRUE(0, "update reached win-K pause");
        return 1;
    }

    /* Unrelated shard progresses while the window holds its shard lock. */
    int score = -1, found = 0;
    ASSERT_EQ_INT(dw_get_score(&env, obj, alt_key, 2000, &score, &found), 0,
                  "unrelated-shard read during pause");
    ASSERT_TRUE(found, "unrelated-shard key found during pause");

    /* The paused shard's key blocks through the pause, then returns a
     * whole pre- or post-window state — never a mix. */
    ASSERT_EQ_INT(dw_get_score(&env, obj, upd_key, 30000, &score, &found), 0,
                  "paused-shard read completes");
    ASSERT_TRUE(found && (score == 50 || score == 555),
                "paused-shard read is whole pre-or-post state");
    waitpid(w, NULL, 0);

    ASSERT_EQ_INT(dw_count_eq(&env, obj, 555, 10000), 1, "final: new score");
    ASSERT_EQ_INT(dw_count_eq(&env, obj, 50, 10000), 0, "final: old score gone");
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}

static int test_win_at_pause_scan_one_version(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int port = env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", env.db_root);
    const char *obj = "dwatpause";
    ASSERT_EQ_INT(create_indexed_object_with_records(&env, obj, 40), 0,
                  "at-pause fixture");
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(dw_append_pause(root, "win-T", 2500), 0, "arm win-T pause");
    ASSERT_EQ_INT(test_env_start_at(&env, root, port), 0, "restart paused");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    const char *upd_key = "item0012";
    pid_t w = dw_trigger_op(&env, obj, "update", upd_key, 777);
    ASSERT_TRUE(w > 0, "spawn paused update");
    char active[PATH_MAX];
    snprintf(active, sizeof(active),
             "%s/default/%s/.durability-test-win-T.active", root, obj);
    if (wait_for_path(active, 20000) != 0) {
        waitpid(w, NULL, 0);
        test_env_stop(&env);
        ASSERT_TRUE(0, "update reached win-T pause");
        return 1;
    }

    /* Between A and T both segment versions carry flag=1; the query scan
     * must still return exactly one version — the post-window one (K
     * already repointed the Kf slot). */
    int kocc = 0, socc = 0;
    ASSERT_EQ_INT(dw_find_key_occurrences(&env, obj, upd_key, 777, 10000,
                                          &kocc, &socc), 0,
                  "find during A/T pause");
    ASSERT_EQ_INT(kocc, 1, "exactly one version of the key mid-A/T");
    ASSERT_EQ_INT(socc, 1, "the one version carries the post-window value");
    waitpid(w, NULL, 0);
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}

TEST_REGISTER("test-win-gate-pending-then-success", test_win_gate_pending_then_success)
TEST_REGISTER("test-win-postlink-publication-pending", test_win_postlink_publication_pending)
TEST_REGISTER("test-win-replay-no-deadlock", test_win_replay_no_deadlock)
TEST_REGISTER("test-win-window16-two-windows", test_win_window16_two_windows)
TEST_REGISTER("test-win-sync-count-matrix", test_win_sync_count_matrix)
TEST_REGISTER("test-win-crash-matrix", test_win_crash_matrix)
TEST_REGISTER("test-win-ik-pause-readers-whole-state", test_win_ik_pause_readers_whole_state)
TEST_REGISTER("test-win-at-pause-scan-one-version", test_win_at_pause_scan_one_version)

TEST_REGISTER("test-msync-range", test_msync_range_raw_fails_on_main)
TEST_REGISTER("test-ordering-marker-clean-after-crud", test_ordering_marker_clean_after_crud)
TEST_REGISTER("test-ordering-delete-marker-free", test_ordering_delete_marker_free)
TEST_REGISTER("test-durability-clean-shutdown-skips-recovery", test_durability_clean_shutdown_skips_recovery)
TEST_REGISTER("test-durability-sigkill-marker-after-write-recovers", test_durability_sigkill_marker_after_write_recovers)
TEST_REGISTER("test-durability-corrupt-update-marker-kf-slot-rejected", test_durability_corrupt_update_marker_kf_slot_rejected)
TEST_REGISTER("test-durability-corrupt-marker-policy", test_durability_corrupt_marker_policy)
TEST_REGISTER("test-durability-bulk-marker-recovers", test_durability_bulk_marker_recovers)
TEST_REGISTER("test-durability-bulk-window-prepared-recovers", test_durability_bulk_window_prepared_recovers)
TEST_REGISTER("test-durability-bulk-window-applied-recovers", test_durability_bulk_window_applied_recovers)
TEST_REGISTER("test-durability-bulk-window-boundary", test_durability_bulk_window_boundary)
TEST_REGISTER("test-durability-bulk-window-boundary-mixed-indexes", test_durability_bulk_window_boundary_mixed_indexes)
