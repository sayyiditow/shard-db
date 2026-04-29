/* libFuzzer harness for b64_decode().
 *
 * The base64 decoder is the gateway for every put-file / get-file body
 * over the wire. A bad decoder = remote crash trigger; bad output sizing
 * = OOB writes into the destination buffer.
 *
 * Build:
 *   clang -O1 -g -fsanitize=fuzzer,address,undefined \
 *         -Isrc/db -o fuzz_b64 fuzz/fuzz_b64.c src/db/util.c
 *
 * Run:
 *   ./fuzz_b64 -max_total_time=60 fuzz/corpora/b64/
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include "types.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > (1u << 20)) return 0;  /* 1 MiB cap */

    /* b64_decode max output size is 3*ceil(N/4) ≈ 3N/4 + 2.
       Allocate generously; real call sites use b64_decoded_max(). */
    size_t out_cap = (size * 3) / 4 + 16;
    uint8_t *out = (uint8_t *)malloc(out_cap);
    if (!out) return 0;

    size_t out_len = 0;
    /* b64_decode reads (b64, b64_len) directly — no NUL-termination needed */
    b64_decode((const char *)data, size, out, &out_len);

    free(out);
    return 0;
}
