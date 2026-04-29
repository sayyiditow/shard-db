/* libFuzzer harness for json_parse_object().
 *
 * Every inbound shard-db request lands in json_parse_object() before any
 * routing happens. That makes this the highest-leverage parser to fuzz —
 * memory corruption here is reachable from any unauthenticated TCP byte
 * (modulo the IP allowlist + token check that comes later).
 *
 * Build:
 *   clang -O1 -g -fsanitize=fuzzer,address,undefined \
 *         -Isrc/db -o fuzz_json fuzz/fuzz_json.c src/db/util.c
 *
 * Run for 60 seconds:
 *   ./fuzz_json -max_total_time=60 fuzz/corpora/json/
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include "types.h"

/* json_parse_object expects a NUL-terminated buffer (its skip helpers use
   the trailing NUL as a secondary bound). libFuzzer hands us a bare span
   so we copy into a NUL-terminated heap buffer of size+1. */
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    /* Cap at a generous upper bound to avoid OOM on degenerate input.
       Real requests are bounded by MAX_REQUEST_SIZE (32 MB default). */
    if (size > (1u << 16)) return 0;

    char *buf = (char *)malloc(size + 1);
    if (!buf) return 0;
    memcpy(buf, data, size);
    buf[size] = '\0';

    JsonObj obj;
    json_parse_object(buf, size, &obj);

    /* Exercise every getter so the fuzzer drives the full surface — not
       just the parser, but every read path that walks the parsed spans. */
    const char *v;
    size_t vl;
    json_obj_get(&obj, "mode", &v, &vl);
    json_obj_get(&obj, "dir",  &v, &vl);
    json_obj_get(&obj, "object", &v, &vl);
    json_obj_get(&obj, "criteria", &v, &vl);
    json_obj_unquoted(&obj, "key", &v, &vl);
    (void)json_obj_int(&obj, "limit", 0);

    char tmp[256];
    json_obj_copy(&obj, "value", tmp, sizeof(tmp));

    char *s;
    s = json_obj_strdup(&obj, "auth");        free(s);
    s = json_obj_strdup_raw(&obj, "fields");  free(s);
    s = json_obj_string_or_array(&obj, "fields"); free(s);

    free(buf);
    return 0;
}
