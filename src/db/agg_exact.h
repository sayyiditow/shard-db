/* src/db/agg_exact.h
 *
 * Exact aggregate states. Integer-class fields and legacy numeric accumulate
 * in checked 128-bit integers, packed numeric in the decnum core, float/double
 * stay binary floating. Nothing here touches I/O, locks or schema loading;
 * every function is thread-confined to the AggEnv/AggNum it is handed. */
#ifndef AGG_EXACT_H
#define AGG_EXACT_H
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include "decnum.h"
#include "types.h"
#include "typed_row.h"

typedef __int128 agg_i128;
enum AggFn { AGG_COUNT, AGG_SUM, AGG_AVG, AGG_MIN, AGG_MAX };
typedef enum { AGC_NONE = 0, AGC_INT, AGC_DEC, AGC_FLT } AggClass;

typedef struct {
    AggClass cls;
    int      scale;    /* AGC_INT: decimal places of the stored integer (legacy numeric S, else 0) */
    int      dscale;   /* AGC_DEC: constant display scale of a bounded column (max(S,0)); bare: 0 */
    int      bare;     /* AGC_DEC: bare numeric - display scale is per value */
    size_t   index;    /* schema field index */
    enum FieldType type;
} AggNumSpec;

typedef struct {
    _Atomic size_t *bytes;   /* shared QUERY_BUFFER_MB counter; NULL = unmetered */
    size_t          limit;
    dec_t           val;     /* decoded-input scratch (one per ctx/worker) */
} AggEnv;

typedef struct {
    uint8_t has;             /* min/max hold a value */
    uint8_t float_sum_overflow; /* sticky; only SUM/AVG consume this error */
    union {
        struct { agg_i128 sum, min, max; } i;
        struct { dec_t    sum, min, max; } d;
        struct { double   sum, min, max; } f;
    } u;
} AggNum;                    /* all-zero is the valid empty state */

typedef struct AggValue {
    AggClass cls;
    int      present;        /* 0: no contributing value */
    agg_i128 i;  int scale;  /* AGC_INT */
    const dec_t *dv;         /* AGC_DEC: borrowed from the AggNum or &own */
    dec_t    own;            /* storage for computed decimals (avg) */
    double   f;              /* AGC_FLT */
} AggValue;                  /* never copy by value once dv may be &own */

/* 1 when the field can be sum/avg/min/max-ed (fills *out), else 0. */
int  agg_num_spec_for(const TypedSchema *ts, size_t idx, AggNumSpec *out);
/* 0 for bare packed numeric: its ordered index key lacks the display scale, so
   index-only VALUE shortcuts must not be used (decision D1). */
int  agg_index_exact(const AggNumSpec *sp);

void agg_env_init(AggEnv *e, _Atomic size_t *bytes, size_t limit);
void agg_env_free(AggEnv *e);
void agg_num_release(AggNum *n, AggClass cls, AggEnv *e);

/* Return 1 value accumulated, 0 absent (NULL / incomplete / foreign tier),
   -1 error (errno: ENOMEM, ENOBUFS = budget, ERANGE = overflow/non-finite,
   EILSEQ = malformed). The caller owns the per-spec count. */
int  agg_num_offer_row(const AggNumSpec *sp, const TypedRowView *row,
                       AggNum *n, AggEnv *e);
int  agg_num_offer_key(const AggNumSpec *sp, const uint8_t *key, size_t klen,
                       AggNum *n, AggEnv *e);
/* Direct offers (also the unit-test surface): same contract, 0 ok / -1 errno.
   The caller owns the per-spec contributing-value count. A finite floating
   sum overflow is sticky state, checked by agg_num_value for SUM/AVG only;
   MIN/MAX and presence counts remain usable. Non-finite input is -1. */
int  agg_num_offer_int(AggNum *n, agg_i128 v);
int  agg_num_offer_double(AggNum *n, double v);
int  agg_num_offer_decimal(AggNum *n, AggEnv *e, const dec_t *v);
/* Decode a floating-class ordered key (top-N streaming only). */
int  agg_key_double(const AggNumSpec *sp, const uint8_t *key, size_t klen,
                    double *out);
int  agg_num_merge(AggNum *dst, const AggNum *src, AggClass cls, AggEnv *e);

/* n holds exactly one offered value v (has = 1, sum = v). Makes sum equal to
   `times` offers of v (times >= 1); min/max/has unchanged. INT: checked
   multiply; DEC: double-and-add (every partial sum is at most |v * times|, so
   it overflows exactly when the offers would). FLT: EINVAL (floating sums
   depend on addition order). 0 ok / -1 errno. */
int  agg_num_repeat(AggNum *n, AggClass cls, int64_t times, AggEnv *e);

void agg_value_init(AggValue *v);
void agg_value_release(AggValue *v);
void agg_value_from_count(AggValue *v, int64_t count);
/* Result of one spec over `count` contributing values. 0 ok / -1 errno. */
int  agg_num_value(const AggNum *n, const AggNumSpec *sp, enum AggFn fn,
                   int64_t count, AggValue *out);
/* Parse a canonical group label (INT/DEC via dec_parse, FLT via strtod). */
int  agg_value_from_label(const AggNumSpec *sp, const char *label, AggValue *out);
/* malloc'd number lexeme for a PRESENT value; NULL with errno on failure. */
char *agg_value_render(const AggValue *v);
char *agg_render_double(double v);
/* Same-spec ordering, allocation-free (sort comparator). */
int  agg_value_cmp(const AggValue *a, const AggValue *b);
/* Compare to a numeric literal. 0 ok (*cmp = -1/0/1) / -1 errno (EINVAL when
   the literal is not a finite number). */
int  agg_value_cmp_text(const AggValue *a, const char *lit, int *cmp);
/* Label text of a decimal: packed cell or ordered key payload. D2 scale rule.
   Returns length (>=0) or -1 (errno; ENAMETOOLONG when buf is too small). */
int  agg_dec_label(const AggNumSpec *sp, const uint8_t *p, size_t plen,
                   int ordered, char *buf, size_t cap, AggEnv *e);

#ifdef TEST_BUILD
void agg_exact_test_set_fail_alloc(int fail_n);  /* Nth guarded allocation fails; 0 = never */
#endif
#endif
