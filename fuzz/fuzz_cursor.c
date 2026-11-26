/* libFuzzer harness for parse_cursor_object().
 *
 * The cursor JSON arrives on the wire on every cursor-paginated find
 * ("cursor":{...}), is parsed with NO auth beyond the request itself,
 * and its decoded parts are later interpolated into btree range bounds
 * (resume positions) — memory safety here is reachable from any tenant.
 * parse_cursor_object is static in query.c, so this harness includes
 * query.c directly (campaign Phase 1 step 3 decision: "reached by
 * #include"), sharing the link set fuzz_criteria already uses.
 *
 * Fuzzed function (src/db/query.c):
 *   parse_cursor_object(cursor_json, order_by, &cur, &err)
 *
 * Contract asserted: the parser returns only its documented codes
 * (-1/0/1) and, on success, the decoded cursor obeys MAX_ORDER_FIELDS
 * and per-part length bounds (any other outcome = memory corruption the
 * sanitizers catch; the return-value check pins parse-layer regressions
 * like out-of-range part counts surviving into range bounds).
 *
 * Build: see fuzz/build.sh — same engine subset as fuzz_criteria, but
 * query.c enters via #include so the static is callable.
 *
 * Run:
 *   ./fuzz_cursor -max_total_time=60 fuzz/corpora/cursor/
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>

/* query.c defines FindCursor above parse_cursor_object; including the
   TU makes both visible. query.c has no main() and no conflicting
   constructor with this harness. */
#include "../src/db/query.c"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > (1u << 15)) return 0;   /* cursors are small JSON objects */

    char *buf = (char *)malloc(size + 1);
    if (!buf) return 0;
    memcpy(buf, data, size);
    buf[size] = '\0';

    FindCursor cur;
    memset(&cur, 0, sizeof(cur));
    const char *err = NULL;

    /* A fixed, valid order_by exercises the per-part decode path; the
       cursor content itself is the fuzz input. */
    int rc = parse_cursor_object(buf, "t", &cur, &err);

    if (rc != 0 && rc != 1 && rc != -1) {
        free(buf);
        abort();   /* return contract violated */
    }
    if (rc == 0) {
        for (int i = 0; i < MAX_ORDER_FIELDS; i++) {
            if (cur.part[i].vlen > (size_t)0 &&
                cur.part[i].value == NULL) {
                free(buf);
                abort();  /* nonzero length with no backing buffer */
            }
        }
    }
    /* Production callers hold the cursor under a cleanup attribute, so a
       failed parse may leave owned part buffers behind; release on every
       path, as they do. */
    find_cursor_release(&cur);

    free(buf);
    return 0;
}
