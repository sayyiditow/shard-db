/* src/test/cases/test_not_null_compat.c
 *
 * :not_null modifier — shared startup upgrade audit: read-only
 * ambiguity check over raw pre-2026.10.2 fields.conf files, stale-entry
 * exception, byte-identical refusal.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int nn_write_text(const char *path, const char *text) {
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    int failed = fputs(text, f) == EOF;
    if (fclose(f) != 0) failed = 1;
    return failed ? -1 : 0;
}

static int test_not_null_compat_run(void) {
    char root[] = "/tmp/nn_compat_XXXXXX";
    if (!mkdtemp(root)) return 1;
    char dir[512], obj[512], fields[600], schema[512], version[512];
    snprintf(dir, sizeof(dir), "%s/default", root);
    snprintf(obj, sizeof(obj), "%s/default/obj", root);
    snprintf(fields, sizeof(fields), "%s/fields.conf", obj);
    snprintf(schema, sizeof(schema), "%s/schema.conf", root);
    snprintf(version, sizeof(version), "%s/.version", root);
    ASSERT_EQ_INT(mkdir(dir, 0700), 0, "tenant directory");
    ASSERT_EQ_INT(mkdir(obj, 0700), 0, "object directory");
    ASSERT_EQ_INT(nn_write_text(schema, "default:obj:8:16:2:1\n"), 0,
                  "raw schema manifest");
    ASSERT_EQ_INT(nn_write_text(version, "2026.09.2\n"), 0, "old evidence");
    ASSERT_EQ_INT(nn_write_text(fields, "tag:varchar:40:default=plain\n"), 0,
                  "unambiguous old default");
    ASSERT_EQ_INT(shard_db_not_null_upgrade_audit(root), 0,
                  "ordinary old root accepted");
    ASSERT_EQ_INT(nn_write_text(schema,
                  "default:obj:8:16:2:1\nghost_tenant:gone:8:16:2:1\n"), 0,
                  "older root with inert stale schema entry");
    ASSERT_EQ_INT(shard_db_not_null_upgrade_audit(root), 0,
                  "absent stale object does not block upgrade");
    ASSERT_EQ_INT(unlink(fields), 0, "remove materialized object's metadata");
    ASSERT_EQ_INT(shard_db_not_null_upgrade_audit(root), -1,
                  "missing fields on materialized object still refuses upgrade");
    ASSERT_EQ_INT(nn_write_text(fields, "tag:varchar:40:default=plain\n"), 0,
                  "restore materialized object's metadata");
    const char *ambiguous[] = {
        "tag:varchar:40:default=prefix:not_null:suffix\n",
        "tag:varchar:40:default=prefix:not_null\n",
        "tag:varchar:40:default=prefix:nullable\n",
        "tag:enum(a,b:not_null)\n"
    };
    for (size_t i = 0; i < sizeof(ambiguous) / sizeof(ambiguous[0]); i++) {
        ASSERT_EQ_INT(nn_write_text(fields, ambiguous[i]), 0, "old ambiguous field");
        ASSERT_EQ_INT(shard_db_not_null_upgrade_audit(root), -1,
                      "ambiguous old root refused before reinterpretation");
        char *before = tu_read_file(fields);
        ASSERT_EQ_STR(before, ambiguous[i], "audit leaves schema byte-identical");
        free(before);
        char *stamp = tu_read_file(version);
        ASSERT_EQ_STR(stamp, "2026.09.2\n", "audit does not stamp upgrade");
        free(stamp);
    }
    ASSERT_EQ_INT(nn_write_text(fields, "tag:varchar:40:not_null\n"), 0,
                  "new-generation schema");
    ASSERT_EQ_INT(nn_write_text(version, "2026.10.2\n"), 0, "new evidence");
    ASSERT_EQ_INT(shard_db_not_null_upgrade_audit(root), 0,
                  "new root can carry actual constraints");

    /* Malformed new grammar must refuse the whole schema, not drop the
       first column and reinterpret the next column at offset zero. */
    ASSERT_EQ_INT(nn_write_text(fields,
                  "tag:varchar:40:not_null:default=x\ntail:int\n"), 0,
                  "malformed new schema");
    ASSERT_TRUE(load_typed_schema(dir, "obj") == NULL,
                "malformed nullability refuses entire schema");
    invalidate_schema_caches(dir, "obj");
    unlink(fields); unlink(schema); unlink(version);
    rmdir(obj); rmdir(dir); rmdir(root);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-not-null-compat", test_not_null_compat_run)
