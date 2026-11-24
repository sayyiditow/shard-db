/* libFuzzer harness for the filesystem-name validators.
 *
 * dir/object/filename names are interpolated directly into filesystem
 * paths by every file mode and by the storage layer itself (kf shards,
 * streams, indexes, tokens.conf). The three validators below are the
 * ONLY thing between a request-supplied name and sprintf("%s/%s/..."),
 * so traversal, separator, control-char and length edge cases here are
 * reachable from any tenant on the network. See AUD slices 9/13 (files,
 * remaining surfaces) — added 2026-11-19, campaign Phase 1 step 3.
 *
 * Fuzzed functions (src/db/util.c, src/db/config.c):
 *   valid_filename()  — file-name component rule (files modes)
 *   is_valid_object() — object-name component rule (every object ref)
 *   dir_name_ok()     — tenant-dir component rule (add-dir / db-dirs)
 *
 * Invariant asserted: a name accepted by any validator contains no
 * '/', no '\\', no control byte, and is not "." or ".." — i.e. joining
 * it into a path cannot traverse out of its directory.
 *
 * Build: see fuzz/build.sh (links util.c + config.c + stubs.c).
 *
 * Run:
 *   ./fuzz_filename -max_total_time=60 fuzz/corpora/filename/
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include "types.h"

extern int valid_filename(const char *name);
extern int is_valid_object(const char *name);
extern int dir_name_ok(const char *dir);

/* The shared invariant: an accepted name can never traverse. Returns 0
   if the validator accepted a name that violates the invariant. */
static int invariant_ok(const char *name) {
    size_t n = strlen(name);
    if (n == 0) return 1;                       /* rejected anyway */
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return 0;
    if (memchr(name, '/', n)) return 0;
    if (memchr(name, '\\', n)) return 0;
    for (size_t i = 0; i < n; i++)
        if ((unsigned char)name[i] < 0x20) return 0;
    return 1;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    /* Validators bound names at 255/64 bytes; cap the input so the
       fuzzer spends its budget at the interesting lengths. */
    if (size == 0 || size > 320) return 0;

    char *buf = (char *)malloc(size + 1);
    if (!buf) return 0;
    memcpy(buf, data, size);

    /* The validators are str/NUL-terminated interfaces: keep buf a
       string (strnlen-style reads stop at the first NUL exactly like the
       callers' string views do). */
    buf[size] = '\0';

    if (valid_filename(buf) && !invariant_ok(buf)) {
        free(buf);
        abort();  /* valid_filename accepted a traversal-capable name */
    }
    if (is_valid_object(buf) && (!invariant_ok(buf) || strchr(buf, ':'))) {
        free(buf);
        abort();  /* is_valid_object accepted a traversal-capable name */
    }
    if (dir_name_ok(buf) && !invariant_ok(buf)) {
        free(buf);
        abort();  /* dir_name_ok accepted a traversal-capable name */
    }

    free(buf);
    return 0;
}
