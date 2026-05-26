/* src/test/cases/test_slow_query_log.c
 *
 * log_slow_query must capture the FULL request JSON (truncated) so slow
 * queries are identifiable from the log / the /stats recent ring without
 * having to reproduce them. Added 2026-05-26 after a live count regression
 * (mode/dir/object alone wasn't enough to tell WHICH count was slow).
 *
 * Unit test: log_slow_query populates the in-memory ring (g_slow_queries)
 * before the g_log_running file gate, so we can assert capture directly
 * without a running daemon or log-drain thread. The test is single-threaded
 * and reads the ring immediately after the call, so no lock is needed.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <string.h>

static int test_slow_query_captures_full_query(void) {
    const char *q =
        "{\"mode\":\"count\",\"dir\":\"hn\",\"object\":\"stories\",\"criteria\":"
        "[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"stephen hawking\"}]}";
    log_slow_query("count", "hn", "stories", q, 20457);

    int idx = (g_slow_query_head - 1 + SLOW_QUERY_RING) % SLOW_QUERY_RING;
    SlowQueryEntry *e = &g_slow_queries[idx];
    ASSERT_EQ_STR(e->mode, "count", "ring captured mode");
    ASSERT_EQ_STR(e->object, "stories", "ring captured object");
    ASSERT_EQ_STR(e->query, q, "ring captured the FULL query JSON");
    ASSERT_EQ_INT((int)e->duration_ms, 20457, "ring captured duration");
    return 0;
}

static int test_slow_query_truncates_long(void) {
    /* A bulk-insert payload can be megabytes — the ring field is fixed, so
     * an oversized query must be truncated (NUL-terminated), never overflow. */
    char big[8192];
    memset(big, 'x', sizeof(big) - 1);
    big[sizeof(big) - 1] = '\0';
    log_slow_query("bulk-insert", "hn", "stories", big, 5000);

    int idx = (g_slow_query_head - 1 + SLOW_QUERY_RING) % SLOW_QUERY_RING;
    SlowQueryEntry *e = &g_slow_queries[idx];
    size_t qlen = strlen(e->query);
    ASSERT_TRUE(qlen > 0 && qlen < sizeof(e->query),
                "oversized query truncated to fit the ring field, NUL-terminated");
    return 0;
}

static int test_slow_query_null_query_safe(void) {
    /* A NULL query (e.g. a non-JSON legacy request) must not crash. */
    log_slow_query("legacy", "", "", NULL, 100);
    int idx = (g_slow_query_head - 1 + SLOW_QUERY_RING) % SLOW_QUERY_RING;
    ASSERT_EQ_STR(g_slow_queries[idx].query, "", "NULL query stored as empty string");
    return 0;
}

TEST_REGISTER("test-slow-query-captures-full-query", test_slow_query_captures_full_query)
TEST_REGISTER("test-slow-query-truncates-long", test_slow_query_truncates_long)
TEST_REGISTER("test-slow-query-null-query-safe", test_slow_query_null_query_safe)
