/* npm/src/binding.c — N-API wrapper around shard_db_open/query/close */
#include <node_api.h>
#include <string.h>
#include <stdlib.h>
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

typedef struct { ShardDb *db; int closed; } DbHandle;

static void db_finalizer(napi_env env, void *data, void *hint) {
    (void)env; (void)hint;
    DbHandle *h = (DbHandle *)data;
    if (h && !h->closed && h->db) shard_db_close(h->db);
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

    DbHandle *h = malloc(sizeof(DbHandle));
    if (!h) { shard_db_close(db); napi_throw_error(env, NULL, "OOM"); return NULL; }
    h->db = db; h->closed = 0;

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
    return result;
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
    napi_value fn_open, fn_query, fn_close;
    napi_create_function(env, "open",  NAPI_AUTO_LENGTH, napi_open,  NULL, &fn_open);
    napi_create_function(env, "query", NAPI_AUTO_LENGTH, napi_query, NULL, &fn_query);
    napi_create_function(env, "close", NAPI_AUTO_LENGTH, napi_close, NULL, &fn_close);
    napi_set_named_property(env, exports, "open",  fn_open);
    napi_set_named_property(env, exports, "query", fn_query);
    napi_set_named_property(env, exports, "close", fn_close);
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
