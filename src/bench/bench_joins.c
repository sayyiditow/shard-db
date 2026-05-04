/* src/bench/bench_joins.c — port of bench/bench-joins.sh
 *
 * Join performance: 1M users + 500K orders (defaults; override with
 * SHARD_BENCH_USERS / SHARD_BENCH_ORDERS env vars). Measures inner /
 * left / indexed-field / multi joins at multiple result limits, with
 * a baseline (no-join) section for comparison.
 *
 * orders.user_id is the SHA prefix of "user-<i>"; ~5% of orders use a
 * non-existent placeholder so left-join → null is exercised.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"
#include "bench_stats.h"
#include "bench_table.h"
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

/* sha256("<prefix>-<i>")[:32] hex. */
static void make_sha32(const char *prefix, int i, char out[33]) {
    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%s-%d", prefix, i);
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char *)buf, (size_t)n, digest);
    static const char hex[] = "0123456789abcdef";
    for (int j = 0; j < 16; j++) {
        out[2 * j]     = hex[digest[j] >> 4];
        out[2 * j + 1] = hex[digest[j] & 0xF];
    }
    out[32] = '\0';
}

static int make_memfd(const char *name, const char *data, size_t size) {
#if defined(__x86_64__)
    long sysno = 319;
#elif defined(__aarch64__)
    long sysno = 279;
#else
# error "memfd_create syscall number unknown for this arch"
#endif
    int fd = (int)syscall(sysno, name, 0u);
    if (fd < 0) return -1;
    size_t written = 0;
    while (written < size) {
        ssize_t n = write(fd, data + written, size - written);
        if (n <= 0) { close(fd); return -1; }
        written += (size_t)n;
    }
    return fd;
}

/* Wrapper preserved as a thin alias so call sites stay readable. */
static void run_one(TestClient *tc, const char *label, const char *json) {
    bench_table_run(tc, label, json);
}

/* ----- Minimal users dataset: just enough to make the join projection
   produce non-empty username/email values. The full bench-queries
   bench exercises richer query coverage; this bench focuses on join
   throughput, not user-record diversity. */
static int build_users_json(int count, char **out_buf, size_t *out_size) {
    /* ~250 bytes/record at this stripped-down shape. */
    size_t cap = (size_t)count * 256 + 16;
    char *buf = malloc(cap);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = '[';
    for (int i = 0; i < count; i++) {
        if (pos + 256 > cap) {
            cap = pos + (size_t)(count - i) * 256 + 16;
            char *t = realloc(buf, cap);
            if (!t) { free(buf); return -1; }
            buf = t;
        }
        char key[33];
        make_sha32("user", i, key);
        int n = snprintf(buf + pos, cap - pos,
            "%s{\"key\":\"%s\",\"value\":{"
              "\"username\":\"u%d\","
              "\"email\":\"u%d@example.com\","
              "\"age\":%d,"
              "\"active\":%s,"
              "\"birthday\":\"19900101\","
              "\"created_at\":\"20240101000000\""
            "}}",
            i > 0 ? "," : "", key, i, i, 18 + (i % 60),
            (i % 7 != 0) ? "true" : "false");
        if (n < 0 || (size_t)n >= cap - pos) { free(buf); return -1; }
        pos += (size_t)n;
    }
    buf[pos++] = ']';
    *out_buf = buf; *out_size = pos;
    return 0;
}

/* ----- Orders dataset: matches bench-joins.sh layout. ~5% of records
   use MISSING_USER_ID_X so left-join can produce nulls. */
static int build_orders_json(int count, int users_count, char **out_buf, size_t *out_size) {
    static const char *const STATUSES[] = {"paid","pending","shipped","cancelled"};
    size_t cap = (size_t)count * 320 + 16;
    char *buf = malloc(cap);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = '[';
    for (int i = 0; i < count; i++) {
        if (pos + 320 > cap) {
            cap = pos + (size_t)(count - i) * 320 + 16;
            char *t = realloc(buf, cap);
            if (!t) { free(buf); return -1; }
            buf = t;
        }
        char order_key[33];
        make_sha32("order", i, order_key);

        char user_id[64];
        if (i % 20 == 0) {
            strcpy(user_id, "MISSING_USER_ID_X");
        } else {
            int u = users_count > 0 ? (i % users_count) : 0;
            make_sha32("user", u, user_id);
        }

        unsigned mix = (unsigned)i * 2654435761u;
        double amount = 5.0 + (double)(mix % 499500) / 100.0;  /* 5..5000 */
        const char *status = STATUSES[i % 4];
        int sku = i % 100;
        int year  = 2023 + (i % 3);
        int mo    = (i % 12) + 1;
        int da    = (i % 28) + 1;
        int hh    = i % 24;
        int mm    = i % 60;
        int ss    = (i * 13) % 60;

        int n = snprintf(buf + pos, cap - pos,
            "%s{\"key\":\"%s\",\"value\":{"
              "\"order_num\":%d,"
              "\"amount\":\"%.2f\","
              "\"status\":\"%s\","
              "\"user_id\":\"%s\","
              "\"product_sku\":\"SKU-%04d\","
              "\"created_at\":\"%04d%02d%02d%02d%02d%02d\""
            "}}",
            i > 0 ? "," : "", order_key,
            i + 1, amount, status, user_id, sku,
            year, mo, da, hh, mm, ss);
        if (n < 0 || (size_t)n >= cap - pos) { free(buf); return -1; }
        pos += (size_t)n;
    }
    buf[pos++] = ']';
    *out_buf = buf; *out_size = pos;
    return 0;
}

/* ---------------------------------------------------------------- main bench */

#define DEFAULT_USERS  1000000
#define DEFAULT_ORDERS  500000

static int bench_joins_run(void) {
    const char *users_env  = getenv("SHARD_BENCH_USERS");
    const char *orders_env = getenv("SHARD_BENCH_ORDERS");
    int USERS_CNT  = users_env  ? atoi(users_env)  : DEFAULT_USERS;
    int ORDERS_CNT = orders_env ? atoi(orders_env) : DEFAULT_ORDERS;
    if (USERS_CNT  <= 0) USERS_CNT  = DEFAULT_USERS;
    if (ORDERS_CNT <= 0) ORDERS_CNT = DEFAULT_ORDERS;

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 600000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    /* users object — same schema as bench-queries / bench/create-user-object.sh */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"users\","
        "\"splits\":64,\"max_key\":32,"
        "\"fields\":[\"username:varchar:50\",\"email:varchar:100\","
                    "\"bio:varchar:500\",\"age:int\",\"user_id:long\","
                    "\"rank:short\",\"score:double\",\"active:bool\","
                    "\"level:byte\",\"birthday:date\",\"created_at:datetime\","
                    "\"balance:numeric:12,2\",\"hourly_rate:currency\"],"
        "\"indexes\":[\"username\",\"email\",\"age\",\"active\",\"birthday\"]}",
        &resp);
    free(resp); resp = NULL;

    /* orders object — same as bench-joins.sh */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"orders\","
        "\"splits\":32,\"max_key\":32,"
        "\"fields\":[\"order_num:long\",\"amount:numeric:12,2\","
                    "\"status:varchar:16\",\"user_id:varchar:32\","
                    "\"product_sku:varchar:12\",\"created_at:datetime\"],"
        "\"indexes\":[\"status\",\"user_id\",\"product_sku\"]}",
        &resp);
    free(resp); resp = NULL;

    printf("======================================\n");
    printf("  shard-db JOIN benchmark — orders=%d users=%d\n",
           ORDERS_CNT, USERS_CNT);
    printf("======================================\n\n");

    /* Populate users. */
    {
        printf("Generating %d users...\n", USERS_CNT);
        fflush(stdout);
        char *buf = NULL; size_t sz = 0;
        if (build_users_json(USERS_CNT, &buf, &sz) != 0) {
            fprintf(stderr, "bench-joins: OOM users dataset\n");
            tc_close(tc); test_env_stop(&env); return 1;
        }
        printf("  users dataset: %.1f MB\n", (double)sz / (1024.0 * 1024.0));
        int fd = make_memfd("joins-users", buf, sz);
        free(buf);
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"users\","
            "\"file\":\"/proc/%d/fd/%d\"}", (int)getpid(), fd);
        uint64_t t0 = bench_now_ns();
        tc_request(tc, req, &resp);
        uint64_t t1 = bench_now_ns();
        printf("  users insert: wall=%.2fs throughput=%.2f k/sec\n",
               (double)(t1 - t0) / 1e9,
               (double)USERS_CNT / 1000.0 / ((double)(t1 - t0) / 1e9));
        free(resp); resp = NULL;
        close(fd);
    }

    /* Populate orders. */
    {
        printf("\nGenerating %d orders...\n", ORDERS_CNT);
        fflush(stdout);
        char *buf = NULL; size_t sz = 0;
        if (build_orders_json(ORDERS_CNT, USERS_CNT, &buf, &sz) != 0) {
            fprintf(stderr, "bench-joins: OOM orders dataset\n");
            tc_close(tc); test_env_stop(&env); return 1;
        }
        printf("  orders dataset: %.1f MB\n", (double)sz / (1024.0 * 1024.0));
        int fd = make_memfd("joins-orders", buf, sz);
        free(buf);
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"orders\","
            "\"file\":\"/proc/%d/fd/%d\"}", (int)getpid(), fd);
        uint64_t t0 = bench_now_ns();
        tc_request(tc, req, &resp);
        uint64_t t1 = bench_now_ns();
        printf("  orders insert: wall=%.2fs throughput=%.2f k/sec\n\n",
               (double)(t1 - t0) / 1e9,
               (double)ORDERS_CNT / 1000.0 / ((double)(t1 - t0) / 1e9));
        free(resp); resp = NULL;
        close(fd);
    }

    /* ==================== BASELINE ==================== */
    bench_table_section_begin("BASELINE (no join)");
    run_one(tc, "count orders (total)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"orders\"}");
    run_one(tc, "count status=paid (indexed)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}");
    run_one(tc, "find paid limit 10 (indexed)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"limit\":10}");
    run_one(tc, "find paid limit 100",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"limit\":100}");
    run_one(tc, "find paid limit 1000",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"limit\":1000}");
    run_one(tc, "find amount>4000 FULL SCAN",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"amount\",\"op\":\"gt\",\"value\":\"4000\"}],\"limit\":10}");

    /* ==================== INNER JOIN by KEY ==================== */
    bench_table_section_begin("INNER join orders → users by KEY (hash lookup)");
    run_one(tc, "inner limit=10 (paid)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"fields\":[\"username\",\"email\"]}],\"limit\":10}");
    run_one(tc, "inner limit=100",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"fields\":[\"username\",\"email\"]}],\"limit\":100}");
    run_one(tc, "inner limit=1000",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"fields\":[\"username\",\"email\"]}],\"limit\":1000}");
    run_one(tc, "inner limit=10000",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"fields\":[\"username\",\"email\"]}],\"limit\":10000}");
    run_one(tc, "inner FULL (no limit)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"fields\":[\"username\",\"email\"]}]}");

    /* ==================== LEFT JOIN by KEY ==================== */
    bench_table_section_begin("LEFT join orders → users by KEY");
    run_one(tc, "left limit=10",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"type\":\"left\",\"fields\":[\"username\",\"email\"]}],\"limit\":10}");
    run_one(tc, "left limit=1000",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"type\":\"left\",\"fields\":[\"username\",\"email\"]}],\"limit\":1000}");
    run_one(tc, "left limit=10000",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"type\":\"left\",\"fields\":[\"username\",\"email\"]}],\"limit\":10000}");

    /* ==================== INDEXED-FIELD JOIN ==================== */
    bench_table_section_begin("INNER join on INDEXED field (users.username)");
    run_one(tc, "idx-join limit=10",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"username\",\"as\":\"u\",\"fields\":[\"email\"]}],\"limit\":10}");
    run_one(tc, "idx-join limit=100",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"username\",\"as\":\"u\",\"fields\":[\"email\"]}],\"limit\":100}");

    /* ==================== MULTI-JOIN ==================== */
    bench_table_section_begin("MULTI-JOIN (two keyed joins)");
    run_one(tc, "double-inner limit=100",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"fields\":[\"username\"]},{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u2\",\"fields\":[\"email\"]}],\"limit\":100}");
    run_one(tc, "double-inner limit=1000",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"fields\":[\"username\"]},{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u2\",\"fields\":[\"email\"]}],\"limit\":1000}");

    /* ==================== JOIN + PROJECTION ==================== */
    bench_table_section_begin("JOIN + driver projection (fewer columns)");
    run_one(tc, "inner w/projection limit=1000",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],\"fields\":[\"order_num\",\"amount\"],\"join\":[{\"object\":\"users\",\"local\":\"user_id\",\"remote\":\"key\",\"as\":\"u\",\"fields\":[\"username\"]}],\"limit\":1000}");
    bench_table_section_end();

    printf("\n======================================\n");
    printf("  Join bench complete\n");
    printf("======================================\n");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("bench-joins", bench_joins_run)
