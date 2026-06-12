/* npm/src/binding.c — N-API wrapper around shard_db_open/query/close */
#include <node_api.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include "../../src/db/shard_db.h"

#define NAPI_CALL(env, call)                                        \
    do {                                                            \
        napi_status _s = (call);                                    \
        if (_s != napi_ok) {                                        \
            const napi_extended_error_info *ei = NULL;              \
            napi_get_last_error_info((env), &ei);                   \
            napi_throw_error((env), NULL,                           \
                ei && ei->error_message ? ei->error_message         \
                                        : "N-API call failed");     \
            return NULL;                                            \
        }                                                           \
    } while (0)

#define LOG_BUF_CAP 128

typedef struct {
    int  type;
    char msg[1024];
} LogBufEntry;

typedef struct {
    ShardDb        *db;
    int             closed;
    napi_ref        log_fn_ref;     /* NULL when no handler registered */
    LogBufEntry     log_buf[LOG_BUF_CAP];
    int             log_buf_n;
    pthread_mutex_t log_buf_lock;
} DbHandle;

typedef struct {
    DbHandle       *h;
    char           *json;       /* heap copy of the input JSON string */
    char           *out;        /* result buffer — written by execute */
    size_t          out_len;
    int             rc;         /* return value of shard_db_query */
    napi_deferred   deferred;   /* resolves / rejects the returned Promise */
    napi_async_work work;
} QueryWork;

static void execute_query(napi_env env, void *data) {
    (void)env;
    QueryWork *w = (QueryWork *)data;
    w->rc = shard_db_query(w->h->db, w->json, &w->out, &w->out_len);
}

static void complete_query(napi_env env, napi_status status, void *data) {
    QueryWork *w = (QueryWork *)data;

    if (status == napi_cancelled || w->h->closed) {
        napi_value msg;
        napi_create_string_utf8(env, "Query cancelled", NAPI_AUTO_LENGTH, &msg);
        napi_value err;
        napi_create_error(env, NULL, msg, &err);
        napi_reject_deferred(env, w->deferred, err);
        goto cleanup;
    }

    if (w->rc != 0) {
        napi_value msg;
        napi_create_string_utf8(env, "shard_db_query allocation failure",
                                NAPI_AUTO_LENGTH, &msg);
        napi_value err;
        napi_create_error(env, NULL, msg, &err);
        napi_reject_deferred(env, w->deferred, err);
        goto cleanup;
    }

    /* Drain log buffer on the JS thread (same logic as the old sync path). */
    if (w->h->log_fn_ref && w->h->log_buf_n > 0) {
        pthread_mutex_lock(&w->h->log_buf_lock);
        int n = w->h->log_buf_n; w->h->log_buf_n = 0;
        LogBufEntry msgs[LOG_BUF_CAP];
        memcpy(msgs, w->h->log_buf, (size_t)n * sizeof(LogBufEntry));
        pthread_mutex_unlock(&w->h->log_buf_lock);

        napi_value log_fn, global;
        napi_get_reference_value(env, w->h->log_fn_ref, &log_fn);
        napi_get_global(env, &global);
        for (int i = 0; i < n; i++) {
            napi_value argv[2];
            napi_create_int32(env, msgs[i].type, &argv[0]);
            napi_create_string_utf8(env, msgs[i].msg, NAPI_AUTO_LENGTH, &argv[1]);
            napi_call_function(env, global, log_fn, 2, argv, NULL);
        }
    }

    {
        napi_value result;
        napi_create_string_utf8(env, w->out ? w->out : "", w->out_len, &result);
        shard_db_free_result(w->out);
        w->out = NULL;
        napi_resolve_deferred(env, w->deferred, result);
    }

cleanup:
    napi_delete_async_work(env, w->work);
    free(w->json);
    free(w);
}

static void c_log_handler(int type, const char *msg, void *ud) {
    DbHandle *h = (DbHandle *)ud;
    pthread_mutex_lock(&h->log_buf_lock);
    if (h->log_buf_n < LOG_BUF_CAP) {
        h->log_buf[h->log_buf_n].type = type;
        snprintf(h->log_buf[h->log_buf_n].msg,
                 sizeof(h->log_buf[0].msg), "%s", msg ? msg : "");
        h->log_buf_n++;
    }
    pthread_mutex_unlock(&h->log_buf_lock);
}

static void db_finalizer(napi_env env, void *data, void *hint) {
    (void)hint;
    DbHandle *h = (DbHandle *)data;
    if (h && !h->closed && h->db) shard_db_close(h->db);
    if (h && h->log_fn_ref) napi_delete_reference(env, h->log_fn_ref);
    if (h) pthread_mutex_destroy(&h->log_buf_lock);
    free(h);
}

/* open(db_root: string) → external handle */
static napi_value napi_open(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, args, NULL, NULL));

    if (argc < 1) {
        napi_throw_type_error(env, NULL, "open() requires db_root argument");
        return NULL;
    }

    size_t len = 0;
    NAPI_CALL(env, napi_get_value_string_utf8(env, args[0], NULL, 0, &len));
    char *db_root = malloc(len + 1);
    if (!db_root) { napi_throw_error(env, NULL, "OOM"); return NULL; }
    NAPI_CALL(env, napi_get_value_string_utf8(env, args[0], db_root, len + 1, &len));

    ShardDb *db = shard_db_open(db_root);
    free(db_root);

    if (!db) {
        napi_throw_error(env, NULL, "shard_db_open failed \u2014 check db_root path");
        return NULL;
    }

    DbHandle *h = calloc(1, sizeof(DbHandle));
    if (!h) { shard_db_close(db); napi_throw_error(env, NULL, "OOM"); return NULL; }
    h->db = db; h->closed = 0;
    pthread_mutex_init(&h->log_buf_lock, NULL);

    napi_value handle;
    NAPI_CALL(env, napi_create_external(env, h, db_finalizer, NULL, &handle));
    return handle;
}

/* query(handle, json: string) → Promise<string> */
static napi_value napi_query(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value args[2];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, args, NULL, NULL));

    if (argc < 2) {
        napi_throw_type_error(env, NULL, "query() requires (handle, json)");
        return NULL;
    }

    void *data = NULL;
    NAPI_CALL(env, napi_get_value_external(env, args[0], &data));
    DbHandle *h = (DbHandle *)data;

    if (h->closed) {
        napi_throw_error(env, NULL, "Database is closed");
        return NULL;
    }

    size_t len = 0;
    NAPI_CALL(env, napi_get_value_string_utf8(env, args[1], NULL, 0, &len));
    char *json = malloc(len + 1);
    if (!json) { napi_throw_error(env, NULL, "OOM"); return NULL; }
    NAPI_CALL(env, napi_get_value_string_utf8(env, args[1], json, len + 1, &len));

    QueryWork *w = calloc(1, sizeof(QueryWork));
    if (!w) { free(json); napi_throw_error(env, NULL, "OOM"); return NULL; }
    w->h    = h;
    w->json = json;

    napi_value promise;
    NAPI_CALL(env, napi_create_promise(env, &w->deferred, &promise));

    napi_value resource_name;
    napi_create_string_utf8(env, "shard_db_query", NAPI_AUTO_LENGTH, &resource_name);
    napi_create_async_work(env, NULL, resource_name,
                           execute_query, complete_query, w, &w->work);
    napi_queue_async_work(env, w->work);

    return promise;
}

/* setLogHandler(handle, fn | null) → undefined */
static napi_value napi_set_log_handler(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value args[2];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, args, NULL, NULL));

    if (argc < 2) {
        napi_throw_type_error(env, NULL, "setLogHandler() requires (handle, fn|null)");
        return NULL;
    }

    void *data = NULL;
    NAPI_CALL(env, napi_get_value_external(env, args[0], &data));
    DbHandle *h = (DbHandle *)data;

    if (h->log_fn_ref) {
        napi_delete_reference(env, h->log_fn_ref);
        h->log_fn_ref = NULL;
    }

    napi_valuetype vt;
    napi_typeof(env, args[1], &vt);
    if (vt == napi_function) {
        napi_create_reference(env, args[1], 1, &h->log_fn_ref);
        shard_db_set_log_handler(h->db, c_log_handler, h);
    } else {
        shard_db_set_log_handler(h->db, NULL, NULL);
    }

    napi_value undef;
    napi_get_undefined(env, &undef);
    return undef;
}

/* close(handle) → undefined */
static napi_value napi_close(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, args, NULL, NULL));

    if (argc < 1) {
        napi_throw_type_error(env, NULL, "close() requires handle argument");
        return NULL;
    }

    void *data = NULL;
    NAPI_CALL(env, napi_get_value_external(env, args[0], &data));
    DbHandle *h = (DbHandle *)data;

    if (!h->closed) {
        h->closed = 1;
        shard_db_close(h->db);
        h->db = NULL;
    }

    napi_value undef;
    napi_get_undefined(env, &undef);
    return undef;
}

static napi_value Init(napi_env env, napi_value exports) {
    napi_value fn_open, fn_query, fn_close, fn_set_log;
    napi_create_function(env, "open",          NAPI_AUTO_LENGTH, napi_open,            NULL, &fn_open);
    napi_create_function(env, "query",         NAPI_AUTO_LENGTH, napi_query,           NULL, &fn_query);
    napi_create_function(env, "close",         NAPI_AUTO_LENGTH, napi_close,           NULL, &fn_close);
    napi_create_function(env, "setLogHandler", NAPI_AUTO_LENGTH, napi_set_log_handler, NULL, &fn_set_log);
    napi_set_named_property(env, exports, "open",          fn_open);
    napi_set_named_property(env, exports, "query",         fn_query);
    napi_set_named_property(env, exports, "close",         fn_close);
    napi_set_named_property(env, exports, "setLogHandler", fn_set_log);
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
