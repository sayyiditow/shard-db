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

/* query(handle, json: string) → string */
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

    char  *out     = NULL;
    size_t out_len = 0;
    int rc = shard_db_query(h->db, json, &out, &out_len);
    free(json);

    if (rc != 0) {
        napi_throw_error(env, NULL, "shard_db_query allocation failure");
        return NULL;
    }

    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, out ? out : "", out_len, &result));
    shard_db_free_result(out);

    /* Drain log buffer on the JS thread (safe for napi_call_function). */
    if (h->log_fn_ref && h->log_buf_n > 0) {
        pthread_mutex_lock(&h->log_buf_lock);
        int n = h->log_buf_n; h->log_buf_n = 0;
        LogBufEntry msgs[LOG_BUF_CAP];
        memcpy(msgs, h->log_buf, (size_t)n * sizeof(LogBufEntry));
        pthread_mutex_unlock(&h->log_buf_lock);

        napi_value log_fn, global;
        napi_get_reference_value(env, h->log_fn_ref, &log_fn);
        napi_get_global(env, &global);
        for (int i = 0; i < n; i++) {
            napi_value argv[2];
            napi_create_int32(env, msgs[i].type, &argv[0]);
            napi_create_string_utf8(env, msgs[i].msg, NAPI_AUTO_LENGTH, &argv[1]);
            napi_call_function(env, global, log_fn, 2, argv, NULL);
        }
    }

    return result;
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
