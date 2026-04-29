/* libFuzzer harness for parse_criteria_tree().
 *
 * The criteria tree is the most complex parser in the wire protocol —
 * AND/OR nesting up to MAX_CRITERIA_DEPTH=16, every operator has its own
 * value-shape, plus special-case `or:[...]`/`and:[...]` branches. Any
 * bug here is reachable from any find/count/aggregate/bulk-update/
 * bulk-delete request.
 *
 * Build: see fuzz/build.sh — this one needs a chunk of the daemon
 * source for the criteria-node helpers.
 *
 * Run:
 *   ./fuzz_criteria -max_total_time=60 fuzz/corpora/criteria/
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include "types.h"

extern CriteriaNode *parse_criteria_tree(const char *json, const char **err);
extern void free_criteria_tree(CriteriaNode *root);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > (1u << 16)) return 0;

    char *buf = (char *)malloc(size + 1);
    if (!buf) return 0;
    memcpy(buf, data, size);
    buf[size] = '\0';

    const char *err = NULL;
    CriteriaNode *root = parse_criteria_tree(buf, &err);
    if (root) free_criteria_tree(root);

    free(buf);
    return 0;
}
