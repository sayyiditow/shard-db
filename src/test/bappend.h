/* src/test/bappend.h — bounded string-append helper for test fixtures.
 *
 * CodeQL (2026-11 PR #385) flags the fixture idiom
 *     n += (size_t)snprintf(buf + n, cap - n, ...);
 * because n derives from snprintf's return value (the would-be length) and
 * `cap - n` underflows once n exceeds cap — every later append then writes
 * past the buffer. Fixtures never hit that in practice, but the pattern is
 * flagged on sight, so the accumulating sites use this clamped helper
 * instead: pos can never exceed cap, so the internal `cap - pos` size is
 * always in range and truncation behaves like repeated snprintf (content
 * cut, NUL kept, position pinned at cap).
 */
#ifndef SHARD_TEST_BAPPEND_H
#define SHARD_TEST_BAPPEND_H

#include <stdarg.h>
#include <stdio.h>
#include <stddef.h>

static inline size_t tappendf(char *buf, size_t cap, size_t pos,
                              const char *fmt, ...)
        __attribute__((format(printf, 4, 5), nonnull(1, 4)));
static inline size_t tappendf(char *buf, size_t cap, size_t pos,
                              const char *fmt, ...) {
    if (pos >= cap) return cap;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf + pos, cap - pos, fmt, ap);
    va_end(ap);
    if (n < 0) return pos;
    return (size_t)n >= cap - pos ? cap : pos + (size_t)n;
}

#endif /* SHARD_TEST_BAPPEND_H */
