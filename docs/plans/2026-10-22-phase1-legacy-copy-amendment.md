# Phase 1 amendment — legacy non-finite computed-update coverage

Authorized scope: I-03 of the 2026-10-22 implementation review. Test-only;
no daemon bypass, storage-format change, or new public API. The earlier
write-boundary NaN/Infinity rejection assertions and finite-copy assertions
remain. Existing roots may retain bytes accepted by older releases, so
new-write rejection does not remove the stored-source guard requirement.

## Root cause and regression

The section-13 rewrite removed the only checks that an already-stored
NaN/Infinity source cannot be copied into its destination. Keep those
boundary checks and add a stopped-daemon raw fixture using the existing
storage layout. This follows the pattern in
`test_null_legacy_rebuild.c:lr_make_legacy` and
`test_coverity_disk_corruption_segments.c:test_coverity_seg_klen_corruption_run`.
Only source payload bytes change; the fixture opens a supported current
root, exactly modeling pre-Phase-1 data with no startup migration.

Test first: register the new case before any production fixes and run
`rtk proxy ./build/bin/shard-db-test run test-computed-update-legacy-nonfinite`.
The existing guard should already pass it. In an isolated scratch mutant,
changing the `apply_computed_fields` anchor `if (!isfinite(sv)) {` to
`if (0) {` must make the named `not finite` assertions fail. Never mutate
the active production checkout for that proof while other fixes are running.
Root coordinates one build/test sequence for all review corrections.

## Exact edits

In `src/test/cases/test_computed_update.c`, insert after the existing
anchor `#include <unistd.h>`:

```c
#include <fcntl.h>
#include <stdint.h>
#include <dirent.h>
```

Replace the section-13 comment only, beginning
`/* 13. Non-finite literals are rejected at the write boundary` and ending
`itself is re-checked here with a finite source. */`, with:

```c
    /* 13. New non-finite literals reject at the write boundary. Existing
           roots may still carry old NaN/Infinity bytes; the separate
           legacy fixture below preserves coverage of that $copy guard.
           Keep a finite-source $copy check here as well. */
```

Insert this complete block immediately after the definition of
`test_computed_update_run`, before the existing
`TEST_REGISTER("test-computed-update", test_computed_update_run)`:

```c
/* Recreate bytes accepted by earlier releases, while the fixture daemon
   is stopped. Segment layout follows slotcask.c:seg_record_emit and the
   stopped-daemon patch pattern in test_null_legacy_rebuild.c. The schema
   has two double fields and a two-byte nullmap prefix; only source bytes
   change, preserving header, key, nullmap, destination and keyfile refs. */
static int legacy_copy_patch_source(const char *root, const char *object,
                                    uint64_t source_bits) {
    char streams[512];
    int n = snprintf(streams, sizeof(streams),
                     "%s/default/%s/data/streams", root, object);
    if (n < 0 || (size_t)n >= sizeof(streams)) return -1;
    DIR *dir = opendir(streams);
    if (!dir) return -1;
    int patched = 0;
    struct dirent *entry;
    while (!patched && (entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        char path[1024];
        n = snprintf(path, sizeof(path), "%s/%s/000000.dat",
                     streams, entry->d_name);
        if (n < 0 || (size_t)n >= sizeof(path)) continue;
        int fd = open(path, O_RDWR);
        if (fd < 0) continue;
        uint8_t record[43] = {0};  /* 24-byte header + key x + 18-byte value */
        int ok = pread(fd, record, sizeof(record), 0) == (ssize_t)sizeof(record);
        uint16_t klen = 0;
        uint32_t vlen = 0;
        memcpy(&klen, record + 16, sizeof(klen));
        memcpy(&vlen, record + 20, sizeof(vlen));
        double source = 1.5, target = 7.0;
        ok = ok && klen == 1 && vlen == 18 && record[18] == 1 &&
             (record[19] & 1) != 0 && record[24] == 'x' &&
             record[25] == 1 && record[26] == 0 &&
             memcmp(record + 27, &source, sizeof(source)) == 0 &&
             memcmp(record + 35, &target, sizeof(target)) == 0;
        if (ok)
            ok = pwrite(fd, &source_bits, sizeof(source_bits), 27) ==
                 (ssize_t)sizeof(source_bits) && fsync(fd) == 0;
        uint64_t actual = 0;
        if (ok)
            ok = pread(fd, &actual, sizeof(actual), 27) == (ssize_t)sizeof(actual) &&
                 actual == source_bits;
        close(fd);
        patched = ok;
    }
    closedir(dir);
    return patched ? 0 : -1;
}

static int test_computed_update_legacy_nonfinite_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "legacy fixture connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    const char *objects[] = {"legacy_nan", "legacy_inf"};
    const uint64_t source_bits[] = {
        UINT64_C(0x7ff8000000000000), UINT64_C(0x7ff0000000000000)
    };
    for (int i = 0; i < 2; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"s:double\",\"t:double\"]}", objects[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"created\"", "legacy object created");
        free(resp); resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"x\",\"value\":{\"s\":1.5,\"t\":7}}", objects[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "finite fixture seed stored");
        free(resp); resp = NULL;
    }
    tc_close(tc); tc = NULL;
    char saved_root[256];
    snprintf(saved_root, sizeof(saved_root), "%s", env.db_root);
    int saved_port = env.port;
    test_env_stop_keep(&env);
    int fixture_ready = 1;
    for (int i = 0; i < 2; i++) {
        int patch_rc = legacy_copy_patch_source(saved_root, objects[i], source_bits[i]);
        ASSERT_EQ_INT(patch_rc, 0, "stored non-finite legacy source fixture verified");
        if (patch_rc != 0) fixture_ready = 0;
    }
    int rc = test_env_start_at(&env, saved_root, saved_port);
    ASSERT_EQ_INT(rc, 0, "existing root with non-finite source opens");
    if (rc != 0) {
        char base[256];
        snprintf(base, sizeof(base), "%s", saved_root);
        char *slash = strrchr(base, '/');
        if (slash) { *slash = '\0'; tu_run_cmd("rm -rf %s", base); }
        return 1;
    }
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "legacy fixture reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    if (!fixture_ready) { tc_close(tc); test_env_stop(&env); return 1; }
    for (int i = 0; i < 2; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"x\"}", objects[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, i == 0 ? "\"s\":nan" : "\"s\":inf",
                        "reopened record exposes the non-finite legacy source");
        ASSERT_CONTAINS(resp, "\"t\":7", "legacy destination starts unchanged");
        free(resp); resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"x\",\"value\":{\"t\":{\"$copy\":\"s\"}}}", objects[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\"", "legacy non-finite copy rejected");
        ASSERT_CONTAINS(resp, "not finite", "legacy source guard names non-finite input");
        free(resp); resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"x\"}", objects[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"t\":7", "legacy copy rejection preserves destination");
        free(resp); resp = NULL;
    }
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-computed-update-legacy-nonfinite",
              test_computed_update_legacy_nonfinite_run)
```

The new case reuses the existing compiled translation unit, so no build.sh
entry changes. It owns isolated temporary roots and ports via TestEnv.
Stop the daemon before touching bytes, fsync the patched bytes, verify the
read-back pattern, reopen the same root, then exercise the public update
route and prove destination preservation for both sources.

No commits, pushes, benchmarks, sanitizer loops or certification work.
Leave changes uncommitted for raw-diff review. This amendment does not
advance the parent roadmap beyond Phase 1.

## Fixture correction from coordinated first run

The initial runtime attempt reported 7 passed / 8 failed. Its two raw-fixture
setup assertions failed, so the later successful copies exercised finite
seeds and were **not** evidence of a broken source guard. Root cause:
`create-object` ignores a client `streams` field and derives its count in
`query_schema.c` with `slotcask_streams_for_nproc()`. Hard-coding stream 000
therefore addressed the wrong path. The corrected helper discovers the
actual stream directory and only patches a verified first record; the
unsupported `streams:1` request field is removed. The test now stops before
copy assertions if fixture injection fails, and explicitly verifies GET
exposes the non-finite source plus destination 7 immediately after reopening.
Root will run this corrected case in the next coordinated build; no guard
failure or green runtime result is claimed from the invalid first fixture.
