# Type correctness: executable baseline investigation

## Primary goal

Establish repeatable evidence for the reported data loss and the public-path
regressions before changing production code. This is Phase 0 of
[the recovery plan](2026-10-19-type-correctness-recovery-plan.md).

## Phase 0 — Capture the existing failure mechanisms

**How this serves the primary goal:** prevents planning a fix around a guessed
representation, and provides source-driven red evidence before implementation.

This task performs read-only source compilation into a unique temporary folder.
It does not build the separate certification rig, change the repository, create
a branch, commit, or run benchmarks. It is Linux-workstation diagnostic code,
not a portable production implementation.

The code below includes current production sources, so its exact anchors must
be verified first. If an anchor differs, write PLAN_NOTES.md and halt the entire
execution; a planning-model/human revision is required before resuming.

Anchors:

- config.c: `void parse_field_type(const char *spec, TypedField *f)`
- config.c: `void encode_field_len(` and `static int sentinel_span_parses(`
- query_find.c: `void transform_field_value(`
- query_schema.c: `static int validate_field_type(const char *field_spec)`

All four probes were compiled and run during planning on 2026-10-19. The
reproduction command for the original numeric/widening bug exits 1, because it
asserts the desired sign/value. The broad audit and schema validator print
observations and exit 0; they are diagnostics, not green correctness tests.

Copy each complete source block to the indicated temporary filename (not a
production source file). From the repository root run the commands below using
a unique /tmp directory; substitute its absolute path for TASK_TMP.

### numeric-repro.c

```c
#include "src/db/config.c"
int main(void) {
 TypedField f = {0}; uint8_t out[8];
 parse_field_type("numeric:26,10", &f);
 const char *s="1234567890123456.1234567890";
 encode_field_len(&f,s,strlen(s),out);
 uint64_t u=0; for(int i=0;i<8;i++) u=(u<<8)|out[i];
 int64_t v=(int64_t)u;
 printf("size=%d scale=%d stored=%lld\n", f.size,f.numeric_scale,(long long)v);
 printf("read=%lld.%010lld\n", (long long)(v/f.numeric_scale_mult),(long long)llabs(v%f.numeric_scale_mult));
 TypedField bare={0}; parse_field_type("numeric",&bare);
 printf("bare numeric type=%d (FT_NONE=%d)\n",bare.type,FT_NONE);
 return v<0 ? 1:0;
}
```

### types-audit.c

```c
#include "src/db/config.c"
static void probe(const char *spec,const char *val) {
 TypedField f={0}; uint8_t b[128]={0}; parse_field_type(spec,&f);
 encode_field_len(&f,val,strlen(val),b);
 uint64_t u=0; for(int i=0;i<f.size && i<8;i++) u=(u<<8)|b[i];
 printf("%s input=%s => ",spec,val);
 if(f.type==FT_SHORT) printf("%d",(int)(int16_t)u);
 else if(f.type==FT_INT || f.type==FT_DATE) printf("%d",(int32_t)u);
 else if(f.type==FT_LONG || f.type==FT_TIMESTAMP) printf("%lld",(long long)(int64_t)u);
 else if(f.type==FT_DOUBLE) { double d; memcpy(&d,b,8); printf("stored=%.17g rendered=%%g: %g",d,d); }
 else if(f.type==FT_FLOAT) { float d; memcpy(&d,b,4); printf("stored=%.9g rendered=%%g: %g",d,d); }
 else if(f.type==FT_TIME) printf("seconds=%llu",(unsigned long long)u);
 else if(f.type==FT_BOOL || f.type==FT_BYTE) printf("%u",b[0]);
 printf("\n");
}
int main(void) {
 probe("short","32768"); probe("int","2147483648");
 probe("long","9223372036854775808"); probe("int","123abc"); probe("int","oops");
 probe("byte","256"); probe("byte","-1");
 probe("double","1.2345678901234567"); probe("float","1.2345678"); probe("double","1e999");
 probe("bool","yes"); probe("bool","trash"); probe("bool","10");
 probe("date","2026-02-30"); probe("time","25:00:00");
 const char *bad="01234567-89ab-cdef-0123-456789abcdefTRAILING";
 printf("UUID trailing junk accepted=%d\n",sentinel_span_parses(FT_UUID,bad,strlen(bad)));
 return 0;
}
```

### widen-repro.c

```c
#include "src/db/config.c"
extern void transform_field_value(const TypedField*, const TypedField*, const uint8_t*, uint8_t*);
int main(void) {
 TypedField a={0},b={0}; uint8_t src[8]={0},dst[8]={0};
 parse_field_type("float",&a); parse_field_type("double",&b);
 encode_field_len(&a,"3.25",4,src); transform_field_value(&a,&b,src,dst);
 double d; memcpy(&d,dst,8); printf("float 3.25 -> double %.17g\n",d);
 return d==3.25?0:1;
}
```

### varchar-validator.c

```c
#include "src/db/query_schema.c"
int main(void) {
 const char *specs[]={"v:varchar","v:varchar:65535","v:varchar:65536"};
 for(unsigned i=0;i<3;i++) printf("%s => validated byte width %d\n",specs[i],validate_field_type(specs[i]));
 return 0;
}
```

### Exact diagnostic commands

```sh
rtk proxy cc -O0 -ffunction-sections -fdata-sections -I. TASK_TMP/numeric-repro.c -Wl,--gc-sections -lpthread -lm -o TASK_TMP/numeric-repro
rtk proxy TASK_TMP/numeric-repro
rtk proxy cc -O0 -ffunction-sections -fdata-sections -I. TASK_TMP/types-audit.c -Wl,--gc-sections -lpthread -lm -o TASK_TMP/types-audit
rtk proxy TASK_TMP/types-audit
rtk proxy cc -O0 -ffunction-sections -fdata-sections -Isrc/db -c src/db/query_find.c -o TASK_TMP/query-find.o
rtk proxy cc -O0 -ffunction-sections -fdata-sections -I. TASK_TMP/widen-repro.c TASK_TMP/query-find.o -Wl,--gc-sections -lpthread -lm -o TASK_TMP/widen-repro
rtk proxy TASK_TMP/widen-repro
rtk proxy cc -O0 -ffunction-sections -fdata-sections -I. TASK_TMP/varchar-validator.c -Wl,--gc-sections -lpthread -lm -o TASK_TMP/varchar-validator
rtk proxy TASK_TMP/varchar-validator
```

### Captured baseline output

```text
size=8 scale=10 stored=-9223372036854775808
read=-922337203.6854775808
bare numeric type=0 (FT_NONE=0)

short input=32768 => -32768
int input=2147483648 => -2147483648
long input=9223372036854775808 => 9223372036854775807
int input=123abc => 123
int input=oops => 0
byte input=256 => 0
byte input=-1 => 255
double input=1.2345678901234567 => stored=1.2345678901234567 rendered=%g: 1.23457
float input=1.2345678 => stored=1.23456776 rendered=%g: 1.23457
double input=1e999 => stored=inf rendered=%g: inf
bool input=yes => 0
bool input=trash => 1
bool input=10 => 1
date input=2026-02-30 => 20260230
time input=25:00:00 => seconds=0
UUID trailing junk accepted=1

float 3.25 -> double 5.3478801856841961e-318

v:varchar => validated byte width 0
v:varchar:65535 => validated byte width 65537
v:varchar:65536 => validated byte width 0
```

The output format calculation in types-audit reproduces the actual decoder's
%g choice; it does not call the static decoder. Follow it with the TCP regression
case in the acceptance document to prove the user-facing output route. Do not
claim every route shares a failure merely because a shared helper does.

### Deliverables and stop boundary

1. Save observed results, source revision and protected-plan checksum.
2. Inventory all callers using the commands in the parent plan.
3. Reproduce each public route with isolated TestEnv/TestClient cases, distinguishing
   expected unsupported-feature errors from silent changed-value bugs.
4. Validate packed-record sizing/lookup/recomposition in scratch prototypes and
   freeze contracts, migration and index-key handling from the parent plan.
5. Have the planning model author the complete anchored production/test patches
   for the next slice; no production implementation starts from prose alone.
6. Leave source/test changes uncommitted and preserve the protected certification
   document byte-for-byte. Remove disposable diagnostic binaries after outputs
   are captured; do not remove the protected plan or pre-existing user files.

Actual baseline experiments above are completed planning evidence. Public-path
acceptance, full suites, format spike, sanitizer gates and production fixes are
not claimed completed.
