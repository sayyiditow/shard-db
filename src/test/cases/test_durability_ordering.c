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

/* ───────────── Task 2 — marker write/clear/read ───────────── */

static int test_marker_write_roundtrip(void) {
    char base[] = "/tmp/shard-db-marker-test-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(base), "create marker test dir");
    if (!base[0]) return 1;

    char data_dir[PATH_MAX], data_kf[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/data", base);
    snprintf(data_kf, sizeof(data_kf), "%s/data/kf", base);
    ASSERT_EQ_INT(mkdir(data_dir, 0755), 0, "create data/");
    ASSERT_EQ_INT(mkdir(data_kf, 0755), 0, "create data/kf/");

    KfMarkerSlot slot;
    memset(&slot, 0, sizeof(slot));
    slot.magic = KF_MARKER_MAGIC;
    slot.kf_slot = 42;
    slot.has_old = 1;
    slot.old_file_id = 3;
    slot.new_file_id = 7;
    slot.old_offset = 4096;
    slot.new_offset = 8192;
    slot.old_stream_id = 1;
    slot.new_stream_id = 2;

    int rc = kf_marker_write(base, 0, &slot);
    ASSERT_EQ_INT(rc, 0, "kf_marker_write returns 0");

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/data/kf/000_marker.dat", base);
    struct stat st;
    ASSERT_EQ_INT(stat(path, &st), 0, "marker file exists after write");
    ASSERT_TRUE(st.st_size > 0, "marker file is non-empty");

    KfMarkerSlot readback;
    memset(&readback, 0xAA, sizeof(readback));
    int rrc = kf_marker_read(base, 0, &readback);
    ASSERT_EQ_INT(rrc, 0, "kf_marker_read returns 0 (valid)");
    ASSERT_EQ_INT((int)readback.magic, (int)KF_MARKER_MAGIC, "magic round-trips");
    ASSERT_EQ_INT((int)readback.kf_slot, 42, "kf_slot round-trips");
    ASSERT_EQ_INT((int)readback.has_old, 1, "has_old round-trips");
    ASSERT_EQ_INT((int)readback.old_file_id, 3, "old_file_id round-trips");
    ASSERT_EQ_INT((int)readback.new_file_id, 7, "new_file_id round-trips");
    ASSERT_EQ_INT((int)readback.old_offset, 4096, "old_offset round-trips");
    ASSERT_EQ_INT((int)readback.new_offset, 8192, "new_offset round-trips");
    ASSERT_EQ_INT((int)readback.old_stream_id, 1, "old_stream_id round-trips");
    ASSERT_EQ_INT((int)readback.new_stream_id, 2, "new_stream_id round-trips");

    cleanup_dir(base);
    return t_ctx->failed ? 1 : 0;
}

static int test_marker_clear_removes_file(void) {
    char base[] = "/tmp/shard-db-marker-clear-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(base), "create marker clear test dir");
    if (!base[0]) return 1;

    char data_dir[PATH_MAX], data_kf[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/data", base);
    snprintf(data_kf, sizeof(data_kf), "%s/data/kf", base);
    ASSERT_EQ_INT(mkdir(data_dir, 0755), 0, "create data/");
    ASSERT_EQ_INT(mkdir(data_kf, 0755), 0, "create data/kf/");

    KfMarkerSlot slot;
    memset(&slot, 0, sizeof(slot));
    slot.magic = KF_MARKER_MAGIC;
    slot.kf_slot = 7;
    slot.has_old = 0;
    slot.new_file_id = 5;
    slot.new_offset = 2048;
    slot.new_stream_id = 0;

    ASSERT_EQ_INT(kf_marker_write(base, 0, &slot), 0, "write marker before clear");

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/data/kf/000_marker.dat", base);
    struct stat st;
    ASSERT_EQ_INT(stat(path, &st), 0, "marker file exists before clear");

    ASSERT_EQ_INT(kf_marker_clear(base, 0), 0, "kf_marker_clear returns 0");

    ASSERT_TRUE(stat(path, &st) != 0, "marker file gone after clear");

    cleanup_dir(base);
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
        snprintf(mpath, sizeof(mpath), "%s/data/kf/%03d_marker.dat", base, sid);
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
        snprintf(mpath, sizeof(mpath), "%s/data/kf/%03d_marker.dat", base, sid);
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

static int append_index_abort_config(const char *db_root, int fail_after,
                                     const char *pause_phase) {
    char base[PATH_MAX], env_path[PATH_MAX];
    snprintf(base, sizeof(base), "%s", db_root);
    char *slash = strrchr(base, '/');
    if (!slash) return -1;
    *slash = '\0';
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "a");
    if (!f) return -1;
    fprintf(f, "export INDEXED_ABORT_FAIL_AFTER=%d\n"
               "export DURABILITY_TEST_PAUSE_PHASE=%s\n"
               "export DURABILITY_TEST_PAUSE_MS=30000\n",
            fail_after, pause_phase ? pause_phase : "disabled");
    return fclose(f);
}

static int create_abort_matrix_object(TestEnv *env, const char *object,
                                      const char *index_spec,
                                      const char *initial_value) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char *resp = NULL;
    if (tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
                   &resp) != 0) {
        free(resp); tc_close(tc); return -1;
    }
    free(resp); resp = NULL;

    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":8,\"streams\":1,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\",\"cat:varchar:8\"],"
        "\"indexes\":[\"%s\"]}", object, index_spec);
    if (tc_request(tc, req, &resp) != 0 ||
        !SAFE_STRSTR(resp, "\"status\":\"created\"")) {
        free(resp); tc_close(tc); return -1;
    }
    free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
        "\"key\":\"failure-key\",\"value\":{\"score\":1,"
        "\"title\":\"%s\",\"cat\":\"%s\"}}",
        object, initial_value, initial_value);
    int rc = tc_request(tc, req, &resp) == 0 &&
             SAFE_STRSTR(resp, "\"status\":\"inserted\"") ? 0 : -1;
    free(resp); tc_close(tc);
    return rc;
}

static pid_t trigger_indexed_update(TestEnv *env, const char *object,
                                    const char *field, const char *value) {
    pid_t child = fork();
    if (child != 0) return child;
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) _exit(2);
    char req[768], *resp = NULL;
    if (strcmp(field, "score") == 0) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"update\",\"dir\":\"default\","
            "\"object\":\"%s\",\"key\":\"failure-key\","
            "\"value\":{\"score\":%s}}", object, value);
    } else {
        snprintf(req, sizeof(req),
            "{\"mode\":\"update\",\"dir\":\"default\","
            "\"object\":\"%s\",\"key\":\"failure-key\","
            "\"value\":{\"%s\":\"%s\"}}", object, field, value);
    }
    int rc = tc_request(tc, req, &resp);
    free(resp); tc_close(tc);
    _exit(rc == 0 ? 0 : 3);
}

static pid_t trigger_indexed_delete(TestEnv *env, const char *object) {
    pid_t child = fork();
    if (child != 0) return child;
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) _exit(2);
    char req[768], *resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"delete\",\"dir\":\"default\","
        "\"object\":\"%s\",\"key\":\"failure-key\"}", object);
    int rc = tc_request(tc, req, &resp);
    free(resp); tc_close(tc);
    _exit(rc == 0 ? 0 : 3);
}

static int request_indexed_count(TestEnv *env, const char *object,
                                 const char *field, const char *op,
                                 const char *value) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[768], *resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":[{\"field\":\"%s\",\"op\":\"%s\","
        "\"value\":\"%s\"}]}", object, field, op, value);
    int result = -1;
    if (tc_request(tc, req, &resp) == 0) result = tu_parse_count(resp);
    free(resp); tc_close(tc);
    return result;
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

    ASSERT_EQ_INT(append_durability_pause_config(saved_db_root, "marker-after-write"), 0,
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
             "%s/default/%s/.durability-test-marker-after-write.active",
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

/* 3) Corrupt-marker policy: a non-empty marker with an invalid
   magic/checksum must fail startup closed (marker retained, listener
   never binds); a zero-byte marker (torn create before fsync) is benign
   and must be cleared, letting startup proceed normally. */
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
                "daemon refuses to start with an unreplayable corrupt marker");
    struct stat st;
    ASSERT_EQ_INT(stat(marker_path, &st), 0,
                 "corrupt marker file is left in place (fail closed)");
    if (start_rc == 0) test_env_stop(&env);

    ASSERT_EQ_INT(truncate(marker_path, 0), 0,
                  "truncate marker to zero bytes to simulate a torn create");

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "daemon starts and clears a zero-byte marker");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_marker_recovery_ran(&env), 1,
                      "recovery sweep ran for the zero-byte marker cleanup");
        ASSERT_TRUE(access(marker_path, F_OK) != 0,
                    "zero-byte marker removed by recovery sweep");
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

    ASSERT_EQ_INT(append_durability_pause_config(saved_db_root, "bulk-marker-after-write"), 0,
                  "enable deterministic bulk-marker-after-write pause");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart with bulk pause hook enabled");
    if (env.daemon_pid <= 0) return 1;

    pid_t bulk_pid = trigger_bulk_insert(&env, object, keys, nrecords);
    ASSERT_TRUE(bulk_pid > 0, "spawn bulk-insert request that will pause mid-window");

    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/default/%s/.durability-test-bulk-marker-after-write.active",
             saved_db_root, object);
    int marker_rc = wait_for_path(marker, 20000);
    ASSERT_EQ_INT(marker_rc, 0,
                  "bulk-insert reaches deterministic bulk-marker-after-write pause");

    char batch_marker_path[PATH_MAX];
    snprintf(batch_marker_path, sizeof(batch_marker_path),
             "%s/default/%s/data/kf/000_batch_0_marker.dat", saved_db_root, object);
    struct stat mst;
    if (marker_rc == 0) {
        ASSERT_EQ_INT(stat(batch_marker_path, &mst), 0,
                      "batch marker file exists while paused");
        ASSERT_EQ_INT((long long)mst.st_size, (long long)(sizeof(KfMarkerSlot) * nrecords),
                      "batch marker holds a slot for every record in the window");
        FILE *mf = fopen(batch_marker_path, "rb");
        ASSERT_NOT_NULL(mf, "open batch marker to inspect planned slots");
        if (mf) {
            KfMarkerSlot ms;
            int stable_slots = 1;
            for (int i = 0; i < nrecords; i++) {
                if (fread(&ms, sizeof(ms), 1, mf) != 1 || ms.kf_slot == UINT32_MAX) {
                    stable_slots = 0;
                    break;
                }
            }
            fclose(mf);
            ASSERT_TRUE(stable_slots,
                        "fresh batch markers persist their planned kf slots for recovery");
        }
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
        ASSERT_EQ_INT((long long)mst.st_size, (long long)(sizeof(KfMarkerSlot) * nrecords),
                      "batch marker still holds a slot for every record in the window");
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

/* Apply failure after one durable index mutation must take the abort path,
   not the old forward-replay path. Keep the three index implementations as
   separate rows: a shared fixture can otherwise hide a missing dispatch or
   an incomplete inverse in one of them. */
static int test_durability_index_apply_abort_matrix(void) {
    struct {
        const char *object;
        const char *index_spec;
        const char *field;
        const char *old_value;
        const char *new_value;
        const char *op;
    } rows[] = {
        { "abortbtree", "score", "score", "1", "2", "eq" },
        { "aborttrigram", "title:trigram", "title", "old", "newneedle", "contains" },
        { "abortbitmap", "cat:bitmap(64)", "cat", "old", "newcat", "eq" },
    };

    for (size_t ri = 0; ri < sizeof(rows) / sizeof(rows[0]); ri++) {
        TestEnv env = {0};
        ASSERT_EQ_INT(test_env_start(&env), 0, "start abort-matrix daemon");
        if (env.daemon_pid <= 0) continue;

        ASSERT_EQ_INT(create_abort_matrix_object(&env, rows[ri].object,
                                                 rows[ri].index_spec, "old"), 0,
                      "create single-index abort fixture");
        int saved_port = env.port;
        char saved_db_root[PATH_MAX];
        snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
        test_env_stop_keep(&env);

        ASSERT_EQ_INT(append_index_abort_config(saved_db_root, 1,
                                                "abort-sidecar-after-fsync"), 0,
                      "enable deterministic post-index failure");
        ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                      "restart with deterministic index failure enabled");
        if (env.daemon_pid <= 0) continue;

        uint8_t hash[16];
        compute_hash_raw("failure-key", strlen("failure-key"), hash);
        int kf_shard = compute_record_shard(hash, 8);
        char pause_path[PATH_MAX], marker_path[PATH_MAX], sidecar_path[PATH_MAX];
        snprintf(pause_path, sizeof(pause_path),
                 "%s/default/%s/.durability-test-abort-sidecar-after-fsync.active",
                 saved_db_root, rows[ri].object);
        snprintf(marker_path, sizeof(marker_path),
                 "%s/default/%s/data/kf/%03x_marker.dat", saved_db_root,
                 rows[ri].object, (unsigned)kf_shard);
        snprintf(sidecar_path, sizeof(sidecar_path),
                 "%s/default/%s/data/kf/%03x_marker_abort.dat", saved_db_root,
                 rows[ri].object, (unsigned)kf_shard);

        pid_t update_pid = trigger_indexed_update(&env, rows[ri].object,
                                                   rows[ri].field,
                                                   rows[ri].new_value);
        ASSERT_TRUE(update_pid > 0, "spawn update that will fail after index apply");
        int pause_rc = wait_for_path(pause_path, 20000);
        ASSERT_EQ_INT(pause_rc, 0,
                      "update reaches durable abort-sidecar pause");
        if (pause_rc == 0) {
            ASSERT_EQ_INT(access(marker_path, F_OK), 0,
                           "forward marker remains paired with abort sidecar");
            ASSERT_EQ_INT(access(sidecar_path, F_OK), 0,
                           "abort sidecar is durable before the error returns");
        }
        /* Kill unconditionally: on timeout the daemon is still running and
           holds the DB lock, cascading into every later test_env_start_at()
           call, including the rest of this loop. */
        test_env_kill(&env);
        unlink(pause_path);
        if (update_pid > 0) waitpid(update_pid, NULL, 0);

        ASSERT_EQ_INT(append_index_abort_config(saved_db_root, 0, "disabled"), 0,
                      "disable failure injection before recovery");
        ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                      "restart and recover the pinned abort");
        if (env.daemon_pid <= 0) continue;

        ASSERT_EQ_INT(request_indexed_count(&env, rows[ri].object,
                                            rows[ri].field, rows[ri].op,
                                            rows[ri].old_value), 1,
                      "old index entry remains visible after abort recovery");
        ASSERT_EQ_INT(request_indexed_count(&env, rows[ri].object,
                                            rows[ri].field, rows[ri].op,
                                            rows[ri].new_value), 0,
                      "new index entry is absent after abort recovery");

        TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
        TestClient *tc = tc_connect(&cfg);
        ASSERT_NOT_NULL(tc, "connect after abort recovery");
        if (tc) {
            char req[768], *resp = NULL;
            snprintf(req, sizeof(req),
                "{\"mode\":\"get\",\"dir\":\"default\","
                "\"object\":\"%s\",\"key\":\"failure-key\"}",
                rows[ri].object);
            ASSERT_EQ_INT(tc_request(tc, req, &resp), 0,
                          "direct get succeeds after abort recovery");
            if (strcmp(rows[ri].field, "score") == 0)
                ASSERT_CONTAINS(resp, "\"score\":1", "direct get retained old score");
            else if (strcmp(rows[ri].field, "title") == 0)
                ASSERT_CONTAINS(resp, "\"title\":\"old\"", "direct get retained old title");
            else
                ASSERT_CONTAINS(resp, "\"cat\":\"old\"", "direct get retained old category");
            free(resp);
            tc_close(tc);
        }

        pid_t retry_pid = trigger_indexed_update(&env, rows[ri].object,
                                                  rows[ri].field,
                                                  rows[ri].new_value);
        ASSERT_TRUE(retry_pid > 0, "retry after orphan-sidecar recovery starts");
        if (retry_pid > 0) {
            int retry_status = 0;
            waitpid(retry_pid, &retry_status, 0);
            ASSERT_TRUE(WIFEXITED(retry_status) && WEXITSTATUS(retry_status) == 0,
                        "retry after recovery commits normally");
        }
        ASSERT_EQ_INT(request_indexed_count(&env, rows[ri].object,
                                            rows[ri].field, rows[ri].op,
                                            rows[ri].new_value), 1,
                      "retry publishes the new index entry exactly once");
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A single indexed delete uses the same durable abort decision as an
   upsert, but its inverse must re-insert the OLD index entry and leave the
   OLD segment live. Exercise that path across a crash while the sidecar is
   paired with the forward marker. */
static int test_durability_index_delete_abort(void) {
    TestEnv env = {0};
    ASSERT_EQ_INT(test_env_start(&env), 0, "start indexed-delete abort daemon");
    if (env.daemon_pid <= 0) return 1;

    ASSERT_EQ_INT(create_abort_matrix_object(&env, "abortdelete", "score", "old"), 0,
                  "create indexed-delete abort fixture");
    int saved_port = env.port;
    char saved_db_root[PATH_MAX];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    test_env_stop_keep(&env);

    ASSERT_EQ_INT(append_index_abort_config(saved_db_root, 1,
                                            "abort-sidecar-after-fsync"), 0,
                  "enable deterministic delete index failure");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart indexed-delete fixture with failure enabled");
    if (env.daemon_pid <= 0) return 1;

    uint8_t hash[16];
    compute_hash_raw("failure-key", strlen("failure-key"), hash);
    int kf_shard = compute_record_shard(hash, 8);
    char pause_path[PATH_MAX], marker_path[PATH_MAX], sidecar_path[PATH_MAX];
    snprintf(pause_path, sizeof(pause_path),
             "%s/default/abortdelete/.durability-test-abort-sidecar-after-fsync.active",
             saved_db_root);
    snprintf(marker_path, sizeof(marker_path),
             "%s/default/abortdelete/data/kf/%03x_marker.dat", saved_db_root,
             (unsigned)kf_shard);
    snprintf(sidecar_path, sizeof(sidecar_path),
             "%s/default/abortdelete/data/kf/%03x_marker_abort.dat", saved_db_root,
             (unsigned)kf_shard);

    pid_t delete_pid = trigger_indexed_delete(&env, "abortdelete");
    ASSERT_TRUE(delete_pid > 0, "spawn delete that will fail after index apply");
    int pause_rc = wait_for_path(pause_path, 20000);
    ASSERT_EQ_INT(pause_rc, 0, "delete reaches durable abort-sidecar pause");
    if (pause_rc == 0) {
        ASSERT_EQ_INT(access(marker_path, F_OK), 0,
                       "delete marker remains paired with abort sidecar");
        ASSERT_EQ_INT(access(sidecar_path, F_OK), 0,
                       "delete abort sidecar is durable before the error returns");
    }
    /* Kill unconditionally: on timeout the daemon is still running and
       holds the DB lock, so leaving it up would cascade into every later
       test_env_start_at() in this process. */
    test_env_kill(&env);
    unlink(pause_path);
    if (delete_pid > 0) waitpid(delete_pid, NULL, 0);

    ASSERT_EQ_INT(append_index_abort_config(saved_db_root, 0, "disabled"), 0,
                  "disable delete failure injection before recovery");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart and recover indexed delete abort");
    if (env.daemon_pid <= 0) return 1;

    ASSERT_EQ_INT(request_indexed_count(&env, "abortdelete", "score", "eq", "1"), 1,
                  "delete abort recovery restores the OLD index entry");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect after indexed-delete abort recovery");
    if (tc) {
        char *resp = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"get\",\"dir\":\"default\","
            "\"object\":\"abortdelete\",\"key\":\"failure-key\"}",
            &resp), 0, "direct get succeeds after indexed-delete abort recovery");
        ASSERT_CONTAINS(resp, "\"score\":1", "delete abort leaves OLD record live");
        free(resp);
        tc_close(tc);
    }

    pid_t retry_pid = trigger_indexed_delete(&env, "abortdelete");
    ASSERT_TRUE(retry_pid > 0, "retry indexed delete after recovery starts");
    if (retry_pid > 0) {
        int retry_status = 0;
        waitpid(retry_pid, &retry_status, 0);
        ASSERT_TRUE(WIFEXITED(retry_status) && WEXITSTATUS(retry_status) == 0,
                    "retry indexed delete commits normally");
    }
    ASSERT_EQ_INT(request_indexed_count(&env, "abortdelete", "score", "eq", "1"), 0,
                  "retry removes the OLD index entry exactly once");
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* An abort sidecar can outlive its marker after the binding cleanup step 2.
   The next indexed bulk operation must discover and validate that orphan
   before reusing batch id 0, rather than truncating it as a new marker. */
static int test_durability_orphan_batch_sidecar_gate(void) {
    TestEnv env = {0};
    ASSERT_EQ_INT(test_env_start(&env), 0, "start orphan-sidecar gate daemon");
    if (env.daemon_pid <= 0) return 1;
    ASSERT_EQ_INT(create_abort_matrix_object(&env, "orphanbatch", "score", "old"), 0,
                  "create orphan-sidecar gate fixture");

    char keys[1][32];
    int next_candidate = 0;
    ASSERT_EQ_INT(pick_same_shard_keys(8, 0, &next_candidate, keys, 1), 0,
                  "pick key for batch shard zero");
    char object_root[PATH_MAX], sidecar_path[PATH_MAX];
    snprintf(object_root, sizeof(object_root), "%s/default/orphanbatch", env.db_root);
    snprintf(sidecar_path, sizeof(sidecar_path),
             "%s/data/kf/000_batch_0_abort.dat", object_root);
    ASSERT_EQ_INT(kf_abort_write_sidecar(object_root, KF_ABORT_BATCH, 0, 0, 1), 0,
                  "create valid orphan batch sidecar");
    ASSERT_EQ_INT(access(sidecar_path, F_OK), 0,
                  "orphan sidecar exists before batch-id reuse");

    pid_t bulk_pid = trigger_bulk_insert(&env, "orphanbatch", keys, 1);
    ASSERT_TRUE(bulk_pid > 0, "spawn indexed bulk write that reuses batch id zero");
    if (bulk_pid > 0) {
        int status = 0;
        waitpid(bulk_pid, &status, 0);
        ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0,
                    "bulk write succeeds after clearing validated orphan sidecar");
    }
    ASSERT_TRUE(access(sidecar_path, F_OK) != 0,
                "validated orphan sidecar is cleared before batch-id reuse");
    ASSERT_EQ_INT(request_count(&env, "orphanbatch"), 2,
                  "bulk write remains visible after orphan-sidecar gate");
    test_env_stop(&env);
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
        ASSERT_EQ_INT(mid_count, 256,
                      "same-shard read observes exactly the first window's "
                      "256 records while the second window is paused");
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

TEST_REGISTER("test-msync-range", test_msync_range_raw_fails_on_main)
TEST_REGISTER("test-marker-write-roundtrip", test_marker_write_roundtrip)
TEST_REGISTER("test-marker-clear-removes-file", test_marker_clear_removes_file)
TEST_REGISTER("test-ordering-marker-clean-after-crud", test_ordering_marker_clean_after_crud)
TEST_REGISTER("test-ordering-delete-marker-free", test_ordering_delete_marker_free)
TEST_REGISTER("test-durability-clean-shutdown-skips-recovery", test_durability_clean_shutdown_skips_recovery)
TEST_REGISTER("test-durability-sigkill-marker-after-write-recovers", test_durability_sigkill_marker_after_write_recovers)
TEST_REGISTER("test-durability-corrupt-marker-policy", test_durability_corrupt_marker_policy)
TEST_REGISTER("test-durability-bulk-marker-recovers", test_durability_bulk_marker_recovers)
TEST_REGISTER("test-durability-bulk-window-prepared-recovers", test_durability_bulk_window_prepared_recovers)
TEST_REGISTER("test-durability-bulk-window-applied-recovers", test_durability_bulk_window_applied_recovers)
TEST_REGISTER("test-durability-index-apply-abort-matrix", test_durability_index_apply_abort_matrix)
TEST_REGISTER("test-durability-index-delete-abort", test_durability_index_delete_abort)
TEST_REGISTER("test-durability-orphan-batch-sidecar-gate", test_durability_orphan_batch_sidecar_gate)
TEST_REGISTER("test-durability-bulk-window-boundary", test_durability_bulk_window_boundary)
TEST_REGISTER("test-durability-bulk-window-boundary-mixed-indexes", test_durability_bulk_window_boundary_mixed_indexes)
