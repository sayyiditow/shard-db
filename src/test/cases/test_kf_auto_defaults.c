/* src/test/cases/test_kf_auto_defaults.c
 *
 * W1 D3: the nightly reshard sweep defaults ON (pre-W1 it defaulted
 * to 0). Asserts on a calloc'd scratch instance rather than the
 * runner's process-local ShardDb so neither db.env parsing nor case
 * order can influence the result.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "shard_db_internal.h"

#include <stdlib.h>
#include <string.h>

static int test_kf_auto_defaults_run(void) {
    ShardDb *db = calloc(1, sizeof(ShardDb));
    ASSERT_NOT_NULL(db, "scratch ShardDb alloc");
    if (!db) return 1;

    db_defaults_set(db);
    ASSERT_EQ_INT(db->auto_reshard_enable, 1, "AUTO_RESHARD_ENABLE default 1");

    free(db);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-kf-auto-defaults", test_kf_auto_defaults_run)
