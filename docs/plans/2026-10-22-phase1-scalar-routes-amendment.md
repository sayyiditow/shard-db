# Phase 1 scalar JSON routes amendment — 2026-10-22

User authorized correction of implementation-review I-04 (JSON escape
handling) together with I-01 through I-03. This document covers I-04;
OOM status propagation has a separate amendment and consumer inventory.
No storage/index format change. Keep this branch uncommitted.

## Root cause and contract

The inline typed encoders strip JSON quotes, but decode escapes only for
varchar. Typed update/index extraction repeats that varchar-only policy.
Thus a JSON scalar string whose decoded content is `1` can be rejected or
indexed inconsistently. Decoding after strlen can hide escaped NUL suffixes.

Decode quoted JSON string values exactly once for varchar and the eight
Phase 1 scalar numeric types. Pass the decoded byte length into the checked
encoder; the cstring decoder rejects decoded NUL rather than truncating it.
Leave unquoted JSON objects/literals, non-JSON defaults/CSV and Phase 7
calendar/bool/UUID/enum syntax on their existing literal rules.

## Consumer inventory

- config.c typed_encode and typed_encode_defaults: one shared JSON-aware
  checked-encoding adapter, only after current null/sentinel/enum checks.
- util.c json_get_fields_unescaped and json_get_fields_null_aware: share
  quoted-token metadata from the existing JSON field walk; decode eligible
  quoted fields only. Preserve true JSON null versus quoted string `null`.
- index.c index_parallel and build_index_key_from_json: use the typed
  extraction policy; no second unescape pass over extracted values.
- storage.c updates already use json_get_fields_null_aware; no extra decode.
- query_bulk.c JSON updates already use json_get_fields_null_aware; no extra
  decode in value_compute. Bulk JSON insert uses typed_encode_defaults.
- query_plan.c scalar and IN/BETWEEN criteria already unescape once; retain.
- types.h: shared eligibility helper below. Only its JSON-prototype region
  changes here; the OOM agent owns the index-encoder declaration region.

## Test first

`test-scalar-routes-checked` in the new test_scalar_routes_checked.c covers
escaped digits, sign, fraction and exponent through insert/update/bulk
insert/bulk update and indexed criteria, escaped NUL rejection with old
record preservation, and literal backslash-u decode-once rejection. Both
this case and test-scalar-csv-update are registered once after the existing
`src/test/cases/test_scalar_conversion_safety.c \` build-list anchor.
Root coordinated the red build/run before production edits. Actual red:
31 passed / 14 failed, exit 14 (`/tmp/p1-test-scalar-routes-checked-red.log`).
Three count checks also had test-only format/expectation defects: tc_recv
preserves trailing newline, and b.f=100 differs from a.f=120. The final
checks parse the complete integer allowing trailing JSON whitespace and
require exactly one matching row. Failed escaped-write assertions, rather
than those formatting defects, are the load-bearing red evidence. The
earlier independent public escaped-insert reproduction also remains valid.

After the red capture, low-level typed_encode coverage was added for all
eight scalar types (escaped one matches canonical bytes; escaped NUL
rejects). This checks the no-defaults encoder in addition to public routes.

## Exact edits

Insert before the types.h anchor `/* JSON helpers */`:

```c
static inline int typed_json_string_type(enum FieldType t) {
    return t == FT_VARCHAR || t == FT_SHORT || t == FT_INT ||
           t == FT_LONG || t == FT_TIMESTAMP || t == FT_BYTE ||
           t == FT_NUMERIC || t == FT_DOUBLE || t == FT_FLOAT;
}
```

Insert in config.c immediately before
`static int sentinel_span_parses(enum FieldType t, const char *val, size_t vlen)`:

```c
static int encode_json_field_checked(const TypedField *f, const char *val,
                                      size_t vlen, int was_quoted,
                                      uint8_t *out, char *err, size_t errsz) {
    if (!was_quoted || !typed_json_string_type(f->type))
        return encode_field_checked(f, val, vlen, out, err, errsz);
    char *decoded = NULL;
    size_t decoded_len = 0;
    errno = 0;
    if (json_unescape_cstring(val, vlen, &decoded, &decoded_len) != 0) {
        int decode_errno = errno;
        if (err && errsz)
            snprintf(err, errsz, decode_errno == ENOMEM
                     ? "out of memory decoding JSON value for field '%s'"
                     : "malformed JSON escape in value for field '%s'",
                     f->name);
        errno = decode_errno;
        return -1;
    }
    int rc = encode_field_checked(f, decoded, decoded_len, out, err, errsz);
    free(decoded);
    return rc;
}
```

In each inline encoder, replace the complete varchar/unescape versus
non-varchar encode branch inside `if (el > 0)` (after the sentinel and,
where present, enum checks), with this call, preserving indentation:

```c
if (encode_json_field_checked(&ts->fields[i], ev, el, was_quoted,
                              out + ts->nullmap_prefix + ts->fields[i].offset,
                              err_buf, err_buf_size) != 0)
    return -2;
```

For util.c, rename the existing json_get_fields implementation to the
internal signature below, recording the original quote flag before stripping surrounding quotes.
Allocation failure frees partial outputs and returns -1, which the typed
helper propagates instead of treating it as a missing field. Add the original
public API wrapper immediately afterward:

```c
static int json_get_fields_impl(const char *json, const char **keys, int nkeys,
                                char **out_values, uint8_t *quoted)
```

At anchor `if (out[0] == '\"' && vlen > 1 && out[vlen-1] == '\"') {` insert:

```c
if (quoted) quoted[i] = 1;
```

```c
int json_get_fields(const char *json, const char **keys, int nkeys,
                    char **out_values) {
    return json_get_fields_impl(json, keys, nkeys, out_values, NULL);
}
```

Before json_get_fields_unescaped, insert:

```c
static int json_get_typed_fields(const char *json, const char **keys, int nkeys,
                                 const enum FieldType *field_types, char **out,
                                 uint8_t *out_null) {
    uint8_t quoted[MAX_FIELDS * 4] = {0};
    if (nkeys < 0 || nkeys > MAX_FIELDS * 4) { errno = EINVAL; return -1; }
    if (out_null) memset(out_null, 0, (size_t)nkeys);
    if (json_get_fields_impl(json, keys, nkeys, out, quoted) < 0) return -1;
    int rc = 0;
    for (int i = 0; i < nkeys; i++) {
        if (out_null) out_null[i] = 0;
        if (!out[i]) continue;
        if (out_null && !quoted[i] &&
            json_span_is_null(out[i], strlen(out[i]))) {
            free(out[i]); out[i] = NULL; out_null[i] = 1;
            continue;
        }
        if (!quoted[i] || !typed_json_string_type(field_types[i])) continue;
        char *unesc = NULL;
        size_t ulen = 0;
        if (json_unescape_cstring(out[i], strlen(out[i]), &unesc, &ulen) != 0) {
            free(out[i]); out[i] = NULL; rc = -1;
            continue;
        }
        free(out[i]); out[i] = unesc;
    }
    return rc;
}
```

Replace the two public typed-extraction function bodies with:

```c
int json_get_fields_unescaped(const char *json, const char **keys, int nkeys,
                              const enum FieldType *field_types, char **out) {
    return json_get_typed_fields(json, keys, nkeys, field_types, out, NULL);
}

int json_get_fields_null_aware(const char *json, const char **keys, int nkeys,
                               const enum FieldType *field_types, char **out,
                               uint8_t *out_null) {
    return json_get_typed_fields(json, keys, nkeys, field_types, out, out_null);
}
```

Index extraction hunks are coordinated with the OOM amendment, which owns
index.c and contains the complete current changes. index_parallel uses
json_get_fields_unescaped with its typed array; single-field
build_index_key_from_json uses the same helper for one field. Neither
performs a second unescape pass over the decoded output.

## Verification

After root's red capture, apply edits, build once with
`rtk proxy env SKIP_TESTS=1 ./build.sh`, then run the new focused case,
existing test-scalar-conversion-safety, affected varchar/null extraction
cases and the normal full suite. No benches, sanitizer gate, git write or
certification change. Record actual results in the parent evidence ledger.

## Complete internal JSON walk replacement

This replaces the original `json_get_fields` definition, before the
public wrapper shown above. Only quote metadata and checked allocation
cleanup differ from the old walk.

```c
static int json_get_fields_impl(const char *json, const char **keys, int nkeys,
                                char **out_values, uint8_t *quoted) {
    for (int i = 0; i < nkeys; i++) out_values[i] = NULL;

    const char *p = json_skip(json);
    if (*p != '{') return 0;
    p++;
    int found = 0;

    while (*p && found < nkeys) {
        p = json_skip(p);
        if (*p == '}') break;
        if (*p == ',') { p++; continue; }

        if (*p != '"') break;
        p++;
        const char *fname = p;
        while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
        size_t flen = p - fname;
        if (*p == '"') p++;

        p = json_skip(p);
        if (*p != ':') break;
        p = json_skip(p + 1);

        /* Check against all requested keys */
        for (int i = 0; i < nkeys; i++) {
            if (out_values[i]) continue; /* already found */
            size_t klen = strlen(keys[i]);
            if (flen == klen && memcmp(fname, keys[i], klen) == 0) {
                const char *vstart = p;
                const char *vend = json_skip_value(p);
                size_t vlen = vend - vstart;
                char *out = malloc(vlen + 1);
                if (!out) {
                    for (int j = 0; j < nkeys; j++) {
                        free(out_values[j]); out_values[j] = NULL;
                    }
                    return -1;
                }
                memcpy(out, vstart, vlen);
                out[vlen] = '\0';
                if (out[0] == '"' && vlen > 1 && out[vlen-1] == '"') {
                    if (quoted) quoted[i] = 1;
                    memmove(out, out + 1, vlen - 2);
                    out[vlen - 2] = '\0';
                }
                out_values[i] = out;
                found++;
                break;
            }
        }

        p = json_skip_value(p);
    }
    return found;
}
```

## Final regression source

```c
/* JSON scalar strings decode exactly once before checked conversion. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../../db/types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void scalar_route_expect(TestClient *tc, const char *request,
                                const char *fragment, const char *label) {
    char *response = NULL;
    int rc = tc_request(tc, request, &response);
    ASSERT_TRUE(rc == 0 && response != NULL, label);
    ASSERT_TRUE(response && strstr(response, fragment) != NULL, label);
    free(response);
}

static void scalar_routes_lowlevel(void) {
    const char *specs[] = {"short", "int", "long", "timestamp", "byte",
                          "numeric:12,2", "float", "double"};
    TypedSchema *ts = calloc(1, sizeof(*ts));
    ASSERT_NOT_NULL(ts, "low-level scalar schema");
    if (!ts) return;
    ts->typed = 1; ts->nfields = 1; ts->nullmap_prefix = 2;
    for (size_t i = 0; i < sizeof(specs) / sizeof(specs[0]); i++) {
        parse_field_type(specs[i], &ts->fields[0]);
        strcpy(ts->fields[0].name, "x"); ts->fields[0].name_len = 1;
        ts->fields[0].offset = 0; ts->total_size = 2 + ts->fields[0].size;
        uint8_t expected[16] = {0}, actual[32] = {0};
        char err[256] = {0};
        int rc = encode_field_checked(&ts->fields[0], "1", 1, expected,
                                      err, sizeof(err));
        ASSERT_EQ_INT(rc, 0, "canonical scalar one encodes");
        rc = typed_encode(ts, "{\"x\":\"\\u0031\"}", actual, sizeof(actual),
                          err, sizeof(err));
        ASSERT_EQ_INT(rc, ts->total_size, "low-level encoder decodes escaped scalar");
        ASSERT_TRUE(rc == ts->total_size &&
                    memcmp(actual + ts->nullmap_prefix, expected,
                           (size_t)ts->fields[0].size) == 0,
                    "low-level decoded value equals canonical scalar");
        rc = typed_encode(ts, "{\"x\":\"1\\u0000junk\"}", actual, sizeof(actual),
                          err, sizeof(err));
        ASSERT_EQ_INT(rc, -2, "low-level scalar escaped NUL rejects");
    }
    free(ts);
}

static int test_scalar_routes_checked_run(void) {
    scalar_routes_lowlevel();
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = {.port = env.port, .io_timeout_ms = 60000};
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    scalar_route_expect(tc,
        "{\"mode\":\"add-dir\",\"dir\":\"scalar_routes\"}",
        "\"status\"", "add-dir");
    scalar_route_expect(tc,
        "{\"mode\":\"create-object\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"splits\":8,\"max_key\":64,\"fields\":[\"i:int\",\"n:numeric:12,2\",\"f:float\",\"d:double\"]}",
        "\"status\":\"created\"", "create");
    scalar_route_expect(tc,
        "{\"mode\":\"insert\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\",\"value\":{\"i\":\"\\u0031\",\"n\":\"\\u002d\\u0031.\\u0032\\u0035\",\"f\":\"\\u0031.2\\u0065\\u0032\",\"d\":\"\\u0031.25\"}}",
        "\"status\":\"inserted\"", "escaped insert");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\"}",
        "\"i\":1", "insert i decoded");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\"}",
        "\"n\":-1.25", "insert sign decoded");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\"}",
        "\"f\":120", "insert exponent decoded");
    scalar_route_expect(tc,
        "{\"mode\":\"add-index\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"field\":\"i\"}",
        "\"status\"", "i index");
    scalar_route_expect(tc,
        "{\"mode\":\"add-index\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"field\":\"n\"}",
        "\"status\"", "n index");
    scalar_route_expect(tc,
        "{\"mode\":\"add-index\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"field\":\"f\"}",
        "\"status\"", "f index");
    scalar_route_expect(tc,
        "{\"mode\":\"update\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\",\"value\":{\"i\":\"\\u0032\",\"n\":\"\\u0032.\\u0035\"}}",
        "\"status\"", "escaped update");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\"}",
        "\"i\":2", "updated scalar");
    scalar_route_expect(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"records\":[{\"key\":\"b\",\"value\":{\"i\":\"\\u0033\",\"n\":\"\\u0033.5\",\"f\":\"\\u0031e\\u0032\"}}]}",
        "\"inserted\":1", "escaped bulk insert");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"b\"}",
        "\"i\":3", "bulk insert decoded");
    scalar_route_expect(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"records\":[{\"key\":\"b\",\"value\":{\"i\":\"\\u0034\"}}]}",
        "\"updated\":1", "escaped bulk update");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"b\"}",
        "\"i\":4", "bulk update decoded");
    scalar_route_expect(tc,
        "{\"mode\":\"insert\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"nul\",\"value\":{\"i\":\"12\\u0000junk\"}}",
        "\"error\"", "escaped nul insert rejected");
    scalar_route_expect(tc,
        "{\"mode\":\"update\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\",\"value\":{\"i\":\"12\\u0000junk\"}}",
        "\"error\"", "escaped nul update rejected");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\"}",
        "\"i\":2", "nul rejection preserves old");
    scalar_route_expect(tc,
        "{\"mode\":\"insert\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"once\",\"value\":{\"i\":\"\\\\u0031\"}}",
        "\"error\"", "decode exactly once");
    struct { const char *field, *value; } checks[] = {
        {"i", "\\u0032"}, {"n", "\\u0032.5"}, {"f", "\\u0031.2e2"}
    };
    for (int j = 0; j < 3; j++) {
        char request[512], *response = NULL;
        snprintf(request, sizeof(request),
            "{\"mode\":\"count\",\"dir\":\"scalar_routes\",\"object\":\"t\","
            "\"criteria\":[{\"field\":\"%s\",\"op\":\"eq\",\"value\":\"%s\"}]}",
            checks[j].field, checks[j].value);
        int rc = tc_request(tc, request, &response);
        ASSERT_TRUE(rc == 0 && response != NULL, "escaped indexed criterion responds");
        /* Every criterion matches a only: b has i=4/n=3.5/f=100.
           The wire allows trailing JSON whitespace before its frame. */
        char *end = NULL;
        long count = response ? strtol(response, &end, 10) : -1;
        if (end) while (*end == ' ' || *end == '\t' || *end == '\r' || *end == '\n') end++;
        ASSERT_TRUE(response && end != response && count == 1 && *end == '\0',
                    "escaped indexed criterion finds exact rows");
        free(response);
    }
    tc_close(tc); test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-scalar-routes-checked", test_scalar_routes_checked_run)

```
