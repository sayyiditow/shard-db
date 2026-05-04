/* src/bench/bench_table.h
 *
 * Section-oriented bench output formatter. Each section buffers a list of
 * (label, ms, response_snippet) tuples, then on close emits an aligned
 * table with a relative bar chart and min/p50/max/total summary.
 *
 * Why: a single bench run can fire 100+ queries. One-line-per-query output
 * makes outliers and patterns invisible. Sections + bar chart let the eye
 * spot the slow query at a glance.
 *
 * Usage:
 *   bench_table_section_begin("COUNT — indexed int (age)");
 *   bench_table_run(tc, "eq age=30",   "{...}");
 *   bench_table_run(tc, "neq age!=30", "{...}");
 *   bench_table_section_end();
 *
 * The helper depends on test_client.h for tc_request and bench_stats.h for
 * bench_now_ns.
 */
#ifndef BENCH_TABLE_H
#define BENCH_TABLE_H

#include "test_client.h"

/* Begin a new section. Closes the previous section if one is open
   (forgiving: callers can omit explicit _end before the next _begin). */
void bench_table_section_begin(const char *name);

/* Run one timed query and buffer the result for end-of-section flush. */
void bench_table_run(TestClient *tc, const char *label, const char *json);

/* Flush the current section: emits header, all buffered rows aligned
   with a relative bar chart, and a footer with min/p50/max/total ms. */
void bench_table_section_end(void);

#endif /* BENCH_TABLE_H */
