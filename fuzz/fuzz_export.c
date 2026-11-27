/* libFuzzer harness for the export / get-file response re-parse sites.
 *
 * The schema export (describe_one_object / describe_one_tenant) and the
 * get-file client re-parse the daemon's OWN JSON responses: list/array
 * walkers, per-field spec composition, manifest quoting, and the base64
 * "data" extraction. Those run only when the daemon acts as its own
 * client, so they are not reachable through fuzz_frame. This harness
 * feeds arbitrary bytes straight to the pure parsing steps (the socket
 * round-trip in query_collect is not the interesting part):
 *   mode 0: walk_string_array  -> emit_one_index   (manifest "indexes")
 *   mode 1: walk_object_array  -> emit_one_field   (field_obj_to_spec)
 *   mode 2: json_parse_object + json_obj_strdup("data") + b64_decode
 *           (get-file payload path)
 * server.c enters via #include (statics visible); nothing in it is
 * executed except what is called here.
 */
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/db/server.c"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 2 || size > 65536) return 0;
    uint8_t mode = data[0] % 3;
    char *buf = malloc(size);
    if (!buf) return 0;
    memcpy(buf, data + 1, size - 1);
    buf[size - 1] = '\0';

    if (mode == 0 || mode == 1) {
        FILE *out = fopen("/dev/null", "w");
        if (out) {
            EmitFieldsCtx ec = { out, 1, 0 };
            if (mode == 0) walk_string_array(buf, emit_one_index, &ec);
            else           walk_object_array(buf, emit_one_field, &ec);
            fclose(out);
        }
    } else {
        JsonObj o;
        if (json_parse_object(buf, size - 1, &o) > 0) {
            char *b64 = json_obj_strdup(&o, "data");
            if (b64) {
                size_t n = strlen(b64);
                size_t cap = b64_decoded_maxsize(n);
                uint8_t *raw = malloc(cap ? cap : 1);
                size_t rl = 0;
                if (raw) (void)b64_decode(b64, n, raw, &rl);
                free(raw);
                free(b64);
            }
        }
    }
    free(buf);
    return 0;
}
