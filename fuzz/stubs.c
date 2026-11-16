/* fuzz/stubs.c — symbol stubs for fuzzer builds.
 *
 * query.c, objlock.c, and parallel.c reference g_db (the thread-local
 * ShardDb pointer) and g_shard_db_instance, which are normally defined
 * in embedded.c and server.c.
 *
 * g_db must point at a real struct, not NULL: parse_criteria_tree seeds
 * its CriterionBudget from g_query_buffer_max_bytes (i.e.
 * g_db->query_buffer_max_bytes) before it looks at a single input byte,
 * so a NULL g_db crashes the fuzz_criteria harness on its very first
 * execution. 256 MiB mirrors the daemon's QUERY_BUFFER_MB default so the
 * parser sees the same budget semantics it does in production. Nothing
 * else in the struct is read by the parser-only harness.
 */

#include "types.h"

static struct ShardDb fuzz_db;

__thread struct ShardDb *g_db                = &fuzz_db;
struct ShardDb          *g_shard_db_instance = (struct ShardDb *)0;

__attribute__((constructor))
static void fuzz_stubs_init(void) {
    fuzz_db.query_buffer_max_bytes = (size_t)256 * 1024 * 1024;
}

/* durability.c's sync thread checks server_running; set to 0 so the
   thread function exits immediately if the linker ever pulls it in. */
_Atomic int server_running = 0;
