/* Sequence durability (Task 5 of docs/plans/2026-09-04-bulk-commit-
   throughput-and-durability.md): allocations persist via temp+fdatasync+
   rename+dir-fsync and FAIL CLOSED (-1) when the state write fails — on
   base the fopen("w") failure was swallowed and the value returned anyway
   (behavioral red: "state write failure returns -1"). seq_state_reset does
   not exist on base (link-red) and gives reset the same flock the
   allocation paths hold. */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static void expect(int cond, const char *what) {
    ASSERT_TRUE(cond, what);
}

static int test_sequence_durability_run(void) {
    char base[] = "/tmp/shard-db-seq-dur-XXXXXX";
    if (!mkdtemp(base)) return 1;

    /* 1. Normal allocation persists and increments. */
    long long v1 = seq_next_val(base, "obj", "s1");
    expect(v1 == 1, "first allocation returns 1");
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/obj/metadata/sequences/s1", base);
    FILE *f = fopen(path, "r");
    expect(f != NULL, "state file exists");
    if (f) {
        long long stored = -1;
        expect(fscanf(f, "%lld", &stored) == 1 && stored == 1,
               "state file holds 1");
        fclose(f);
    }

    /* 2. Write failure must fail closed (RED on base: returns the value). */
    long long v2 = seq_next_val(base, "obj", "s2dir");
    expect(v2 == 1, "fresh sequence starts at 1");
    char dpath[PATH_MAX];
    snprintf(dpath, sizeof(dpath), "%s/obj/metadata/sequences/s2dir", base);
    remove(dpath);                       /* drop the state file */
    if (mkdir(dpath, 0755) != 0) return 1;   /* state path is now a dir */
    errno = 0;
    long long v3 = seq_next_val(base, "obj", "s2dir");
    expect(v3 == -1, "state write failure returns -1 (RED on base)");

    /* 3. Durable reset helper (link-red on base: symbol does not exist). */
    expect(seq_state_reset(base, "obj", "s1") == 0, "reset succeeds");
    f = fopen(path, "r");
    expect(f != NULL, "state file exists after reset");
    if (f) {
        long long stored = -1;
        expect(fscanf(f, "%lld", &stored) == 1 && stored == 0,
               "reset wrote 0 durably");
        fclose(f);
    }
    expect(seq_next_val(base, "obj", "s1") == 1, "post-reset allocation is 1");
    /* Reset against a wedged state path must error, not silently succeed. */
    expect(seq_state_reset(base, "obj", "s2dir") == -1,
           "reset on unwritable state fails (RED via seq_state_reset)");

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-sequence-durability", test_sequence_durability_run)
