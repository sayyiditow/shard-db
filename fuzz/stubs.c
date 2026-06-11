/* fuzz/stubs.c — symbol stubs for fuzzer builds.
 *
 * query.c, objlock.c, and parallel.c reference g_db (the thread-local
 * ShardDb pointer) and g_shard_db_instance, which are normally defined
 * in embedded.c and server.c. The fuzzer never enters any code path that
 * dereferences these; they only need to exist for the linker.
 */

struct ShardDb; /* opaque forward declaration — no struct body needed */

__thread struct ShardDb *g_db             = (struct ShardDb *)0;
struct ShardDb          *g_shard_db_instance = (struct ShardDb *)0;
