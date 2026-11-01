#ifndef SCALAR_GRAMMAR_H
#define SCALAR_GRAMMAR_H
#include <stddef.h>
#include <stdint.h>
/* Strict, length-aware scalar grammars (R4). Each returns 0 and fills the
   output, or -1 leaving outputs untouched. Outer ASCII whitespace is
   stripped first; embedded NUL is always invalid. */
int sg_bool(const char *s, size_t n, uint8_t *out);
int sg_date(const char *s, size_t n, int32_t *yyyymmdd);
int sg_datetime(const char *s, size_t n, int32_t *yyyymmdd, uint32_t *secs);
int sg_datetimems(const char *s, size_t n, int32_t *yyyymmdd, uint32_t *ms);
int sg_time(const char *s, size_t n, uint32_t *secs);
int sg_uuid(const char *s, size_t n, uint8_t out[16]);
int sg_ipv4(const char *s, size_t n, uint8_t out[4]);
int sg_ipv6(const char *s, size_t n, uint8_t out[16]);
#endif
