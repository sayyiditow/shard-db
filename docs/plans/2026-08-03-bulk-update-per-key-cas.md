# Plan: Per-key CAS for JSON partial bulk updates

## Goal

Add an optional CAS condition to each per-key JSON partial-update record, so a
caller can say “patch this key only if its current revision/status still has
this value.” Keep the existing criteria bulk-update `if` behavior unchanged,
and do not introduce `if_no_match`/`if_match` or field uniqueness in this
plan.

## Proposed protocol

Existing array records remain valid:

```json
{
  "mode": "bulk-update",
  "dir": "acme",
  "object": "companies",
  "records": [
    {"key": "company-1", "value": {"name": "New name"}}
  ]
}
```

Each array record may additionally contain `if`, using the existing criteria
grammar:

```json
{
  "mode": "bulk-update",
  "dir": "acme",
  "object": "companies",
  "records": [
    {
      "key": "company-1",
      "value": {"name": "New name", "revision": "8"},
      "if": [{"field": "revision", "op": "eq", "value": "7"}]
    }
  ]
}
```

The condition is evaluated against that key’s current record while the kf
shard write lock is held. If it fails, that record is skipped and remains
unchanged; other records in the request continue. The response keeps the
existing `matched`/`updated`/`skipped` shape, with CAS failures included in
`skipped`. A syntactically invalid per-record `if` also rejects only that
record: it is included in `matched` and `skipped`, and is reported in an
additional `errors` array so the caller can distinguish malformed input from
a valid CAS miss. For example:

```json
{
  "matched": 2,
  "updated": 1,
  "skipped": 1,
  "errors": [{"key": "company-2", "error": "invalid if condition"}]
}
```

There is no all-or-nothing transaction across the batch.

Duplicate keys in the array form are rejected as a request-level input error
before any write starts, with `{"error":"duplicate key in records: ..."}`.
This is a caller error, but rejecting it is necessary for deterministic CAS
semantics: the bulk primitive reads all OLD values before applying the batch,
so two records for the same key would otherwise both test the same revision
and the later record could overwrite the earlier one. The dictionary form
already has one JSON member per key and is unchanged.

The object/dictionary form remains backward-compatible and has no per-record
CAS syntax. Callers that need a condition should use the array form; this
avoids ambiguity with possible schema field names such as `if` or `value`.
Duplicate JSON member behavior in the dictionary form is unchanged.

The `if` member must contain at least one valid criterion. A present-but-empty
array/object, malformed criterion, unknown operator, or invalid JSON value is
an invalid per-record condition and follows the record-level `skipped` plus
`errors` behavior above. The `errors` member is omitted when there are no
invalid conditions; error keys are JSON-escaped with the repository’s existing
`json_escape_const()` helper.

This same array shape should work for inline JSON and JSON files because both
already pass through `bulk_upd_json_run`. The delimited per-key format is
intentionally not changed here: it has no natural per-row criteria object.
Adding CAS to that format should be a follow-up with an explicit wire format,
rather than silently inventing a sidecar convention.

## Why this is safe

The current JSON partial bulk path already uses the correct atomic seam:

1. `slotcask_bulk_upsert_in_kfshard` acquires the kf-shard write lock.
2. It reads the current old record under that lock.
3. `value_compute` copies that current record and applies the partial patch.
4. The new record and index changes commit before releasing the lock.

The new per-record `if` check belongs inside `value_compute`, before copying
and patching the old value. It must use the same typed criteria/CAS matcher as
single-record update and criteria bulk-update. A failed check returns the
existing non-zero “skip this bulk record” result from the bulk primitive.

This preserves the intended semantics:

- Disjoint patches from separate requests on the same key can both succeed and
  merge because each request starts from the current value under the lock;
  duplicate keys within one array request are rejected before this point.
- Same-field races remain last-writer-wins when no CAS is supplied.
- A revision/status CAS lets the caller reject a stale writer. The caller
  must include the desired revision change in its patch if it wants a version
  bump; sequence defaults remain insert-only.
- Each key update is atomic, but the request is not a transaction over all
  keys.

## Implementation steps

### 1. Add regression tests first

Extend `src/test/cases/test_bulk_update_json.c` after the exact anchor
`/* inline records */` with tests that initially fail because the parser does
not yet accept per-record `if`:

```c
/* matching per-record CAS commits and preserves an unmodified field */
tc_request(tc,
    "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
    "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"approved\"},"
    "\"if\":[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"100\"}]}]}",
    &resp);
ASSERT_CONTAINS(resp, "\"updated\":1", "matching per-key CAS updates");
free(resp); resp = NULL;
ASSERT_EQ_INT(do_count(tc,
                       "[{\"field\":\"status\",\"op\":\"eq\","
                       "\"value\":\"approved\"}]"),
              1, "matching CAS updates the index");

/* non-matching CAS skips and leaves the record unchanged */
tc_request(tc,
    "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
    "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"stale\"},"
    "\"if\":[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"999\"}]}]}",
    &resp);
ASSERT_CONTAINS(resp, "\"updated\":0", "non-matching CAS does not update");
ASSERT_CONTAINS(resp, "\"skipped\":1", "non-matching CAS skips");
free(resp); resp = NULL;

/* malformed per-record if rejects only that record and reports its key */
tc_request(tc,
    "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
    "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"bad\"},"
    "\"if\":[{\"field\":\"amount\",\"op\":\"not-an-operator\",\"value\":\"100\"}]}]}",
    &resp);
ASSERT_CONTAINS(resp, "\"skipped\":1", "invalid if skips");
ASSERT_CONTAINS(resp, "invalid if condition", "invalid if is reported");
free(resp); resp = NULL;
```

Also add tests for the same CAS record shape through the JSON file path, an
unconditional legacy array record, and dictionary-form compatibility. Use the
existing `amount` field as the revision in the concurrency test so the fixture
schema does not change. Add `k4` with `amount:7`, then issue two requests from
two separate `TestClient` connections, both patching `amount:8` with
`if amount == 7`; assert exactly one response has `updated:1`, the other has
`skipped:1`, and the final record has `amount:8` and one writer’s patch. Add a
duplicate-key request and assert it returns a request-level error without
changing the record.

At the exact include anchor `#include <unistd.h>`, add
`#include <pthread.h>`. At the exact function anchor
`static int test_bulk_update_json_run(void) {`, insert this complete helper
immediately before the test function:

```c
typedef struct {
    int port;
    const char *status;
    char *response;
} BulkCasWriter;

static void *bulk_cas_writer_run(void *arg) {
    BulkCasWriter *writer = (BulkCasWriter *)arg;
    TestClientCfg cfg = { .port = writer->port, .io_timeout_ms = 30000 };
    TestClient *client = tc_connect(&cfg);
    if (!client) return NULL;
    char request[512];
    snprintf(request, sizeof(request),
        "{\"mode\":\"bulk-update\",\"dir\":\"default\","
        "\"object\":\"budj_t\",\"records\":[{\"key\":\"k4\","
        "\"value\":{\"status\":\"%s\",\"amount\":8},"
        "\"if\":[{\"field\":\"amount\",\"op\":\"eq\","
        "\"value\":\"7\"}]}]}", writer->status);
    tc_request(client, request, &writer->response);
    tc_close(client);
    return NULL;
}
```

Then add this test block after the exact anchor `/* inline records */`:

```c

tc_request(tc,
    "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
    "\"object\":\"budj_t\",\"records\":[{\"key\":\"k4\","
    "\"value\":{\"status\":\"pending\",\"amount\":7,"
    "\"note\":\"cas\"}}]}", &resp);
free(resp); resp = NULL;

BulkCasWriter writer_a = { env.port, "writer-a", NULL };
BulkCasWriter writer_b = { env.port, "writer-b", NULL };
pthread_t thread_a, thread_b;
int create_a = pthread_create(&thread_a, NULL, bulk_cas_writer_run, &writer_a);
int create_b = pthread_create(&thread_b, NULL, bulk_cas_writer_run, &writer_b);
ASSERT_EQ_INT(create_a, 0, "start CAS writer A");
ASSERT_EQ_INT(create_b, 0, "start CAS writer B");
if (create_a == 0) ASSERT_EQ_INT(pthread_join(thread_a, NULL), 0,
                                  "join CAS writer A");
if (create_b == 0) ASSERT_EQ_INT(pthread_join(thread_b, NULL), 0,
                                  "join CAS writer B");
if (create_a == 0 && create_b == 0) {
    ASSERT_TRUE((SAFE_STRSTR(writer_a.response, "\"updated\":1") != NULL) !=
                (SAFE_STRSTR(writer_b.response, "\"updated\":1") != NULL),
                "exactly one CAS writer updates");
    ASSERT_TRUE((SAFE_STRSTR(writer_a.response, "\"skipped\":1") != NULL) !=
                (SAFE_STRSTR(writer_b.response, "\"skipped\":1") != NULL),
                "exactly one CAS writer skips");
}
tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\","
                "\"object\":\"budj_t\",\"key\":\"k4\"}", &resp);
ASSERT_CONTAINS(resp, "\"amount\":8", "one CAS writer changed revision");
ASSERT_TRUE(SAFE_STRSTR(resp, "\"status\":\"writer-a\"") != NULL ||
            SAFE_STRSTR(resp, "\"status\":\"writer-b\"") != NULL,
            "one CAS writer's patch is visible");
free(resp); resp = NULL;
free(writer_a.response);
free(writer_b.response);

tc_request(tc,
    "{\"mode\":\"bulk-update\",\"dir\":\"default\","
    "\"object\":\"budj_t\",\"records\":["
    "{\"key\":\"k4\",\"value\":{\"note\":\"first\"}},"
    "{\"key\":\"k4\",\"value\":{\"note\":\"second\"}}]}", &resp);
ASSERT_CONTAINS(resp, "duplicate key in records", "duplicate keys reject request");
free(resp); resp = NULL;
tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\","
                "\"object\":\"budj_t\",\"key\":\"k4\"}", &resp);
ASSERT_CONTAINS(resp, "\"amount\":8", "duplicate request did not alter record");
free(resp); resp = NULL;
```

Run the affected test with the current implementation and capture the expected
failure before changing production code. Restore the test and continue only
after the failure demonstrates the missing per-record-CAS behavior.

### 2. Add condition ownership to parsed per-key records

At the exact anchor `} BulkUpdJsonRec;` in `src/db/query_bulk.c`, replace the
complete struct with:

```c
typedef struct {
    char        *key;            /* heap-owned, null-terminated */
    size_t       klen;
    uint8_t      hash[16];
    int          start_slot;
    int          shard_id;
    /* Field deltas: aligned arrays of (typed-field index, owned-string value). */
    int         n_fields;
    int         *field_indices;
    char       **field_values;
    /* Optional per-record CAS, parsed before any worker is dispatched. */
    int          if_present;
    SearchCriterion *if_crit;
    int          if_ncrit;
} BulkUpdJsonRec;
```

The parser owns each record’s parsed criteria and frees it with
`free_criteria()` on every normal, OOM, malformed-record, duplicate-key, and
worker-cleanup path. An omitted `if` has `if_present == 0`; it is
unconditional and does not allocate criteria.

Add this complete helper immediately after that struct:

```c
static int bulk_upd_json_parse_if(const JsonObj *obj,
                                  SearchCriterion **out, int *out_count) {
    const char *raw = NULL;
    size_t raw_len = 0;
    *out = NULL;
    *out_count = 0;
    if (!json_obj_get(obj, "if", &raw, &raw_len)) return 0;
    if (raw_len == 0) return -1;

    char *buf = malloc(raw_len + 1);
    if (!buf) return -1;
    memcpy(buf, raw, raw_len);
    buf[raw_len] = '\0';

    int rc = parse_criteria_json(buf, out, out_count);
    free(buf);
    if (rc != 0 || *out_count <= 0) {
        if (*out) free_criteria(*out, *out_count);
        *out = NULL;
        *out_count = 0;
        return -1;
    }
    return 1;
}
```

At the exact multi-line parser anchor
`JsonObj rec;\n            json_parse_object(obj_str, obj_len, &rec);\n\n            const char *iv; size_t ivl;`
in the array branch of `bulk_upd_json_run`—not the earlier bulk-insert parser—
insert this complete key-ref block immediately after extracting `key`:

```c
if (key && bulk_upd_json_key_ref_add(&key_refs, &key_ref_count,
                                     &key_ref_capacity, key, klen) != 0) {
    free(key);
    if (obj_heap) free(obj_str);
    parse_oom = 1;
    break;
}
```

Then call `bulk_upd_json_parse_if()` after extracting `key` and `value`, storing its
return value and criteria in locals until the record is allocated. Insert this
complete decision block:

```c
SearchCriterion *if_crit = NULL;
int if_ncrit = 0;
int if_rc = bulk_upd_json_parse_if(&rec, &if_crit, &if_ncrit);
if (if_rc < 0) {
    matched++;
    skipped++;
    if (bulk_upd_json_error_add(&errors, &error_count, &error_capacity,
                                key, "invalid if condition") != 0) {
        free(key);
        if (obj_heap) free(obj_str);
        free(if_crit);
        /* Exit through the new parse-phase OOM cleanup label. */
        parse_oom = 1;
        break;
    }
    free(key);
    if (obj_heap) free(obj_str);
    p = obj_end;
    continue;
}
```

For `if_rc == 0`, keep `if_crit == NULL` and `if_ncrit == 0`; for
`if_rc == 1`, assign both locals to the newly allocated `BulkUpdJsonRec` at
the exact record-initialization anchor. If later field parsing rejects the
record, free these locals before continuing. Never treat invalid `if` as
omitted.

The key-ref block is intentionally guarded by `if (key)`. When key extraction
fails, the existing `if (!key || !data_str)` branch runs before this block and
continues without recording a ref; retain that ordering explicitly.

Add this complete owned error type and helper immediately before
`bulk_upd_json_run`:

```c
typedef struct {
    char *key;
    char *message;
} BulkUpdJsonError;

static void bulk_upd_json_errors_free(BulkUpdJsonError *errors, size_t count) {
    for (size_t i = 0; i < count; i++) {
        free(errors[i].key);
        free(errors[i].message);
    }
    free(errors);
}

static int bulk_upd_json_error_add(BulkUpdJsonError **errors, size_t *count,
                                   size_t *capacity, const char *key,
                                   const char *message) {
    if (*count == *capacity) {
        size_t next = *capacity ? *capacity * 2 : 8;
        BulkUpdJsonError *grown = realloc(*errors,
                                          next * sizeof(BulkUpdJsonError));
        if (!grown) return -1;
        *errors = grown;
        *capacity = next;
    }
    (*errors)[*count].key = strdup(key ? key : "");
    (*errors)[*count].message = strdup(message);
    if (!(*errors)[*count].key || !(*errors)[*count].message) {
        free((*errors)[*count].key);
        free((*errors)[*count].message);
        (*errors)[*count].key = NULL;
        (*errors)[*count].message = NULL;
        return -1;
    }
    (*count)++;
    return 0;
}
```

An error-list allocation failure aborts parsing with an OOM response before
any write is dispatched. The final response emits `errors` only when its count
is non-zero, and emits each key/message as JSON strings using
`json_escape_const()`; all error-list allocations are freed on every return.

Add this complete response helper immediately after the error helper:

```c
static void bulk_upd_json_emit_response(int matched, int updated, int skipped,
                                        const BulkUpdJsonError *errors,
                                        size_t error_count) {
    OUT("{\"matched\":%d,\"updated\":%d,\"skipped\":%d",
        matched, updated, skipped);
    if (error_count > 0) {
        OUT(",\"errors\":[");
        for (size_t i = 0; i < error_count; i++) {
            char *key = json_escape_const(errors[i].key);
            char *message = json_escape_const(errors[i].message);
            if (i > 0) OUT(",");
            OUT("{\"key\":\"%s\",\"error\":\"%s\"}",
                key ? key : "", message ? message : "");
            free(key);
            free(message);
        }
        OUT("]");
    }
    OUT("}\n");
}
```

Add these complete helpers immediately after `bulk_upd_json_error_add()`:

```c
typedef struct {
    char *key;                 /* owned by the temporary ref array */
    size_t klen;
} BulkUpdJsonKeyRef;

static void bulk_upd_json_key_refs_free(BulkUpdJsonKeyRef *refs, size_t count) {
    for (size_t i = 0; i < count; i++) free(refs[i].key);
    free(refs);
}

static int bulk_upd_json_key_ref_add(BulkUpdJsonKeyRef **refs, size_t *count,
                                     size_t *capacity, const char *key,
                                     size_t klen) {
    if (*count == *capacity) {
        size_t next = *capacity ? *capacity * 2 : 8;
        BulkUpdJsonKeyRef *grown = realloc(*refs,
                                           next * sizeof(BulkUpdJsonKeyRef));
        if (!grown) return -1;
        *refs = grown;
        *capacity = next;
    }
    (*refs)[*count].key = strndup(key, klen);
    if (!(*refs)[*count].key) return -1;
    (*refs)[*count].klen = klen;
    (*count)++;
    return 0;
}

static int bulk_upd_json_key_ref_cmp(const void *lhs, const void *rhs) {
    const BulkUpdJsonKeyRef *a = (const BulkUpdJsonKeyRef *)lhs;
    const BulkUpdJsonKeyRef *b = (const BulkUpdJsonKeyRef *)rhs;
    if (a->klen < b->klen) return -1;
    if (a->klen > b->klen) return 1;
    return memcmp(a->key, b->key, a->klen);
}

static int bulk_upd_json_find_duplicate(BulkUpdJsonKeyRef *refs, size_t count,
                                         char **duplicate_key) {
    *duplicate_key = NULL;
    if (count < 2) return 0;
    qsort(refs, count, sizeof(*refs), bulk_upd_json_key_ref_cmp);
    for (size_t i = 1; i < count; i++) {
        if (refs[i - 1].klen == refs[i].klen &&
            memcmp(refs[i - 1].key, refs[i].key, refs[i].klen) == 0) {
            *duplicate_key = strndup(refs[i].key, refs[i].klen);
            return *duplicate_key ? 1 : -1;
        }
    }
    return 0;
}
```

Populate one owned ref with `bulk_upd_json_key_ref_add()` immediately after
every array-record key is extracted, including records later rejected for an
invalid `if`. A ref-allocation failure aborts parsing with an OOM response
before any write. If any key appears more than once, free the temporary refs
with `bulk_upd_json_key_refs_free()`, all parsed criteria/errors/records, and
return the escaped request-level duplicate-key error before dispatching workers
or writing anything. Do not use `KeySet`, because its hash-only identity is
insufficient for this input-validation decision. The duplicate check is O(n
log n) and does not change the wire format.

Carry the owned condition through shard bucketing and free it on every normal,
OOM, malformed-record, and worker-cleanup path.

At the exact record-initialization anchor `r = &records[rec_count++];`,
initialize `if_present`, `if_crit`, and `if_ncrit` to zero/NULL before any
record-specific parser branch. This is required because `records` grows with
`realloc()` and the new fields are not otherwise initialized.

At the existing parse-loop `realloc(records, ...)` failure cleanup, extend the
complete per-record cleanup loop with:

```c
if (records[k].if_crit)
    free_criteria(records[k].if_crit, records[k].if_ncrit);
```

The same `if_crit` cleanup applies to every later record-discard path after
criteria ownership has transferred into a `BulkUpdJsonRec`.

### 3. Evaluate CAS inside the locked bulk compute callback

`V2BulkUpdJsonCtx` does not need a layout change; its existing `rec` pointer
reaches the new criteria fields. At the exact anchor
`static int v2_bulk_upd_json_value_compute(`, replace the complete function
with:

```c
/* Compute NEW from OLD: copy old to scratch, patch JSON-named fields,
   apply auto_update. Same logic the per-record path used to inline. */
static int v2_bulk_upd_json_value_compute(const SlotcaskOldRecord *old,
                                           SlotcaskBulkRec *rec) {
    V2BulkUpdJsonCtx *ctx = (V2BulkUpdJsonCtx *)rec->user_ctx;
    BulkUpdJsonShardWork *w = ctx->w;
    BulkUpdJsonRec       *json_rec = ctx->rec;
    if (!old) return -1;
    if (json_rec->if_present &&
        !cas_check(w->ts, old->value, (int)old->vlen,
                   json_rec->if_crit, json_rec->if_ncrit))
        return -1;

    uint8_t *new_buf = (uint8_t *)rec->value;
    if (old->vlen >= (size_t)w->ts->total_size) {
        memcpy(new_buf, old->value, w->ts->total_size);
    } else {
        memcpy(new_buf, old->value, old->vlen);
        memset(new_buf + old->vlen, 0,
               (size_t)w->ts->total_size - old->vlen);
    }
    rec->vlen = (size_t)w->ts->total_size;

    for (int i = 0; i < json_rec->n_fields; i++) {
        int tidx = json_rec->field_indices[i];
        if (tidx < 0 || tidx >= w->ts->nfields) continue;
        if (w->ts->fields[tidx].removed) continue;
        encode_field(&w->ts->fields[tidx], json_rec->field_values[i],
                     new_buf + w->ts->fields[tidx].offset);
    }
    for (int fi = 0; fi < w->ts->nfields; fi++) {
        if (w->ts->fields[fi].removed) continue;
        if (w->ts->fields[fi].default_kind == DK_AUTO_UPDATE) {
            char tbuf[24];
            if (w->ts->fields[fi].type == FT_TIMESTAMP) {
                struct timespec tsn;
                clock_gettime(CLOCK_REALTIME, &tsn);
                long long ms = (long long)tsn.tv_sec * 1000LL +
                               tsn.tv_nsec / 1000000LL;
                snprintf(tbuf, sizeof(tbuf), "%lld", ms);
            } else if (w->ts->fields[fi].type == FT_DATETIMEMS) {
                struct timespec tsn;
                clock_gettime(CLOCK_REALTIME, &tsn);
                time_t nowsec = tsn.tv_sec;
                struct tm tm;
                localtime_r(&nowsec, &tm);
                int msec = (int)(tsn.tv_nsec / 1000000L);
                snprintf(tbuf, sizeof(tbuf),
                         "%04d%02d%02d%02d%02d%02d%03d",
                         tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                         tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
            } else {
                time_t now = time(NULL);
                struct tm tm;
                localtime_r(&now, &tm);
                if (w->ts->fields[fi].type == FT_DATE)
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
                else
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                             tm.tm_hour, tm.tm_min, tm.tm_sec);
            }
            encode_field(&w->ts->fields[fi], tbuf,
                         new_buf + w->ts->fields[fi].offset);
        }
    }
    return 0;
}
```

Do not call `slotcask_get` before entering the bulk primitive and do not run a
separate `find` as the CAS check. Either would recreate the race this feature
is intended to avoid.

Preserve the existing indexed-field pre-commit hook so index comparison uses
the same old value and newly computed value under the same lock.

At the exact multi-line response anchor in `bulk_upd_json_run`
`LOG_INFO(LOG_SUB_QUERY, "BULK-UPDATE-JSON %s matched=%d updated=%d skipped=%d",\n            object, matched, updated, skipped);\n    OUT("{\"matched\":%d,\"updated\":%d,\"skipped\":%d}\n", matched, updated, skipped);`,
replace the complete emission statement with:

```c
bulk_upd_json_emit_response(matched, updated, skipped, errors, error_count);
bulk_upd_json_errors_free(errors, error_count);
```

At the exact multi-line local anchor
`/* Phase 1: parse the array, extract per-record (key, touched fields, hash, shard). */\n    BulkUpdJsonRec *records = NULL;\n    size_t rec_cap = 1024, rec_count = 0;\n    records = malloc(rec_cap * sizeof(BulkUpdJsonRec));\n\n    int matched = 0, skipped = 0;`
in `bulk_upd_json_run`, initialize `errors`, `error_count`, `error_capacity`,
and `parse_oom`. Free them on every return path, including the zero-record,
OOM, duplicate-key, timeout, and malformed-input paths.
Initialize the duplicate-key state at the same anchor:

```c
BulkUpdJsonError *errors = NULL;
size_t error_count = 0;
size_t error_capacity = 0;
int parse_oom = 0;
BulkUpdJsonKeyRef *key_refs = NULL;
size_t key_ref_count = 0;
size_t key_ref_capacity = 0;
```

After the parse loop, call `bulk_upd_json_find_duplicate(key_refs,
key_ref_count, &duplicate_key)`. On return `1`, emit the complete request-level
error with `json_escape_const(duplicate_key)`, free the escaped and duplicate
strings, then free key refs, errors, and all records before returning. On `-1`,
take the new parse-phase OOM path. On `0`, free key refs before worker
dispatch and retain only the parsed records/errors.

The complete request-level duplicate branch is:

```c
char *duplicate_key = NULL;
int duplicate_rc = bulk_upd_json_find_duplicate(key_refs, key_ref_count,
                                                &duplicate_key);
if (duplicate_rc == 1) {
    char *escaped = json_escape_const(duplicate_key);
    OUT("{\"error\":\"duplicate key in records: %s\"}\n",
        escaped ? escaped : "");
    free(escaped);
    free(duplicate_key);
    bulk_upd_json_key_refs_free(key_refs, key_ref_count);
    bulk_upd_json_errors_free(errors, error_count);
    /* Free all parsed records and input storage through the parse cleanup. */
    goto bulk_upd_json_parse_cleanup;
}
if (duplicate_rc < 0) {
    parse_oom = 1;
    goto bulk_upd_json_parse_cleanup;
}
bulk_upd_json_key_refs_free(key_refs, key_ref_count);
key_refs = NULL;
```

The `bulk_upd_json_parse_cleanup` label must distinguish the already-emitted
duplicate response from OOM before emitting its own response, then release all
record-owned allocations and input storage exactly once.

Update the existing `rec_count == 0` branch so it does not emit the old
hardcoded response. Its complete response/cleanup body must be:

```c
bulk_upd_json_emit_response(matched, 0, skipped, errors, error_count);
bulk_upd_json_errors_free(errors, error_count);
bulk_upd_json_key_refs_free(key_refs, key_ref_count);
if (json_mmaped) munmap((void *)json, len);
else if (input_is_file) free(json);
free(records);
return 0;
```

This preserves `matched` and exposes `errors` when every parsed record was
rejected before worker bucketing.

At the existing post-parse `shard_counts` allocation-failure branch and the
existing workers allocation-failure branch, add complete cleanup for
`errors`, `key_refs`, and every record-owned `if_crit` before emitting their
existing hard-error response and returning. These are pre-dispatch paths, so
they must not call the worker cleanup loop later.

The new parse-phase OOM path must free every `records[i].key`, every field
value/index array, every `records[i].if_crit`, the records array, key refs,
errors, and the input mapping/buffer before emitting the OOM response and
returning. It must be reached for `parse_oom`, key-ref allocation failure,
duplicate-key allocation failure, and error-list allocation failure; it must
not dispatch any worker.

### 4. Correct the stale concurrency documentation/comment

Replace the complete comment beginning at the exact anchor
`/* Concurrency caveat (bulk-update-json + delim, partial-field):` immediately
above `bulk_upd_json_shard_worker_v2`. State that JSON partial updates derive
NEW from OLD inside `slotcask_bulk_upsert_in_kfshard` while the kf-shard write
lock is held; delimited updates retain their existing semantics unless they
are separately changed. Explain that different requests on the same key merge
only through this lock, while duplicate keys in one array request are rejected.

At the exact anchor `### Per-key partial update — inline records` in
`docs/query-protocol/bulk.md`, document the array-record `if`, valid CAS misses
as `skipped`, invalid per-record conditions as `skipped` plus `errors`,
request-level duplicate-key rejection, the lack of batch-wide transactionality,
and the fact that dictionary and delimited forms do not provide this
per-record CAS. Note that `errors` is a new optional response member; existing
clients that only read `matched`/`updated`/`skipped` remain compatible.

### 5. Build and test

After approval and implementation:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-bulk-update-json
```

Then run the complete suite with `./build/bin/shard-db-test run-all`. Because
this changes a shared-state/lock-sensitive path, run the repository-required
ASan+UBSan and TSan build/test gates before calling it complete:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all --jobs 1
```

## Scope boundaries

- No unique-field constraints or new index requirements.
- No change to ordinary upsert/full-record replacement semantics.
- No change to criteria-driven bulk-update top-level `if` semantics.
- No `if_no_match` object-wide insert condition.
- No delimited per-key CAS until its request format is designed explicitly.
- No commit or push; leave the implementation uncommitted for review.
