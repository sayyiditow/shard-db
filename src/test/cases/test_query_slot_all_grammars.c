/* src/test/cases/test_query_slot_all_grammars.c
 * AUD-CAP-1 (docs/plans/2026-11-25-fix-query-slot-all-grammars.md): the
 * MAX_CONCURRENT_QUERIES slot cap must cover the NQL and legacy text
 * grammars, not only JSON. With every slot held, each grammar answers
 * "server at capacity"; once released, the same lines are processed (they
 * fail later for other reasons: unknown dir). In-process: no daemon.
 */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "shard_db_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *run_line(const char *line) {
    char *buf = NULL; size_t n = 0;
    FILE *f = open_memstream(&buf, &n);
    FILE *old = g_out;
    g_out = f;
    server_process_fast("/tmp/shard-db-slot-none", line, "127.0.0.1");
    fflush(f);
    g_out = old;
    fclose(f);
    return buf;
}

static int test_query_slot_all_grammars(void) {
    slot_init();
    int held = 0;
    while (slot_try_acquire() == 0 && held < 4096) held++;
    ASSERT_TRUE(held > 0, "the cap is active (slots can be taken)");
    if (held == 0) return 1;

    const char *lines[] = {
        "get\x1f" "nodir\x1f" "obj\x1f" "k",              /* legacy text */
        "find nodir obj",                                  /* NQL find */
        "count nodir obj",                                 /* NQL count */
        "{\"mode\":\"count\",\"dir\":\"nodir\",\"object\":\"obj\"}", /* JSON */
    };
    for (size_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++) {
        char *r = run_line(lines[i]);
        ASSERT_TRUE(r && strstr(r, "server at capacity"), "grammar answers 'server at capacity' while all slots are held");
        free(r);
    }
    for (int i = 0; i < held; i++) slot_release();
    for (size_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++) {
        char *r = run_line(lines[i]);
        ASSERT_TRUE(r && !strstr(r, "server at capacity"), "grammar is processed again once slots are released");
        free(r);
    }
    /* No slot leak: repeated rejected requests leave every slot free. */
    for (int i = 0; i < 2000; i++) free(run_line("get\x1f" "nodir\x1f" "obj\x1f" "k"));
    int again = 0;
    while (slot_try_acquire() == 0 && again < 4096) again++;
    ASSERT_EQ_INT(again, held, "no slot leaked by processed requests");
    for (int i = 0; i < again; i++) slot_release();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-query-slot-all-grammars", test_query_slot_all_grammars)
