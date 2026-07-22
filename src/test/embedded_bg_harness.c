#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "shard_db.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    FILE *file;
    pthread_mutex_t lock;
} CallbackLog;

static void callback_log(int type, const char *msg, void *userdata) {
    (void)type;
    CallbackLog *log = userdata;
    pthread_mutex_lock(&log->lock);
    fputs(msg, log->file);
    fflush(log->file);
    pthread_mutex_unlock(&log->lock);
}

static void marker(CallbackLog *log, int cycle, const char *phase) {
    pthread_mutex_lock(&log->lock);
    fprintf(log->file, "=== cycle %d %s ===\n", cycle, phase);
    fflush(log->file);
    pthread_mutex_unlock(&log->lock);
}

static int query_has(ShardDb *db, const char *query, const char *needle) {
    char *out = NULL;
    size_t out_len = 0;
    int rc = shard_db_query(db, query, &out, &out_len);
    int ok = rc == 0 && out && (!needle || strstr(out, needle));
    if (!ok)
        fprintf(stderr, "embedded-bg-harness query failed: %s\nresponse: %s\n",
                query, out ? out : "(null)");
    shard_db_free_result(out);
    return ok ? 0 : -1;
}

static int query_contains(ShardDb *db, const char *query, const char *needle) {
    char *out = NULL;
    size_t out_len = 0;
    int rc = shard_db_query(db, query, &out, &out_len);
    int found = rc == 0 && out && strstr(out, needle);
    shard_db_free_result(out);
    return found;
}

static void sleep_ms(int ms) {
    struct timespec delay = {
        .tv_sec = ms / 1000,
        .tv_nsec = (long)(ms % 1000) * 1000000L
    };
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
    }
}

int main(int argc, char **argv) {
    if (argc != 6) return 2;
    const char *env_dir = argv[1];
    const char *db_root = argv[2];
    int hold_ms = atoi(argv[3]);
    int cycles = atoi(argv[4]);
    if (hold_ms < 0 || cycles < 1 || chdir(env_dir) != 0) return 2;

    CallbackLog log = { .file = fopen(argv[5], "w") };
    if (!log.file || pthread_mutex_init(&log.lock, NULL) != 0) return 2;

    int rc = 0;
    for (int cycle = 1; cycle <= cycles; cycle++) {
        marker(&log, cycle, "start");
        ShardDb *db = shard_db_open(db_root);
        if (!db) {
            fprintf(stderr, "embedded-bg-harness: shard_db_open cycle %d failed\n",
                    cycle);
            rc = 3;
            break;
        }
        shard_db_set_log_handler(db, callback_log, &log);

        if (cycle == 1) {
            if (query_has(db, "{\"mode\":\"add-dir\",\"name\":\"default\"}",
                          NULL) != 0 ||
                query_has(db,
                    "{\"mode\":\"create-object\",\"dir\":\"default\","
                    "\"object\":\"durable\",\"splits\":8,\"max_key\":32,"
                    "\"fields\":[\"v:int\",\"flag:bool\"],"
                    "\"indexes\":[\"v\",\"flag:bitmap\"]}",
                    NULL) != 0) {
                shard_db_close(db);
                rc = 4;
                break;
            }
            int already_present = query_contains(db,
                "{\"mode\":\"count\",\"dir\":\"default\","
                "\"object\":\"durable\"}", "1");
            if (!already_present && query_has(db,
                    "{\"mode\":\"insert\",\"dir\":\"default\","
                    "\"object\":\"durable\",\"key\":\"k1\","
                    "\"value\":{\"v\":7,\"flag\":true}}",
                    "\"status\"") != 0) {
                shard_db_close(db);
                rc = 4;
                break;
            }
        }

        sleep_ms(hold_ms);
        if (query_has(db,
                "{\"mode\":\"count\",\"dir\":\"default\","
                "\"object\":\"durable\"}", "1") != 0 ||
            query_has(db,
                "{\"mode\":\"find\",\"dir\":\"default\","
                "\"object\":\"durable\","
                "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\","
                "\"value\":\"true\"}]}", "\"k1\"") != 0) {
            shard_db_close(db);
            rc = 5;
            break;
        }

        shard_db_close(db);
        marker(&log, cycle, "end");
    }

    fclose(log.file);
    pthread_mutex_destroy(&log.lock);
    return rc;
}
