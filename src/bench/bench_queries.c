/* src/bench/bench_queries.c — port of bench/bench-queries.sh
 *
 * Latency battery on a 1M-user object: 17 query operator classes across
 * count/find/aggregate, plus cursor pagination. Default COUNT=1000000;
 * override via SHARD_BENCH_COUNT env var (matches bench-kv).
 *
 * Schema (from bench/create-user-object.sh):
 *   splits=64, max_key=32
 *   indexed:    username, email, age, active, birthday
 *   non-indexed: bio, balance, score, level, etc.
 *
 * Data generator mirrors bench/insert-users.sh's python loader so query
 * results land in the same distributional bands (e.g. "starts alice"
 * matches every 30th record).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"
#include "bench_stats.h"
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#define SPLITS 64

/* Mirrors the python lists in insert-users.sh — 30 first names, 30
   last names, 7 domains, 15 bios — so that username/email/bio
   distributions hit every query band the bash bench expects. */
static const char *const FIRST_NAMES[] = {
    "Alice","Bob","Carol","Dave","Eve","Frank","Grace","Hank","Iris","Jack",
    "Kate","Leo","Mia","Nick","Olga","Paul","Quinn","Rose","Sam","Tina",
    "Uma","Vic","Wendy","Xena","Yuri","Zara","Omar","Layla","Hassan","Fatima"
};
#define N_FIRST 30
static const char *const LAST_NAMES[] = {
    "Smith","Jones","Brown","Wilson","Taylor","Clark","Hall","Lewis","Young","King",
    "Wright","Adams","Baker","Green","Hill","Moore","White","Allen","Scott","Davis",
    "Evans","Thomas","Roberts","Walker","Lee","Khan","Ali","Ahmed","Chen","Park"
};
#define N_LAST 30
static const char *const DOMAINS[] = {
    "gmail.com","yahoo.com","outlook.com","company.org","dev.io","mail.co","example.com"
};
#define N_DOMAINS 7
static const char *const BIOS[] = {
    "Software engineer with a passion for open source",
    "Data scientist exploring ML frontiers",
    "Full-stack developer and coffee enthusiast",
    "DevOps engineer automating everything",
    "Product manager building user-centric tools",
    "Designer who codes on the side",
    "Student learning distributed systems",
    "Freelancer specializing in backend APIs",
    "Tech lead at a growing startup",
    "Retired professor turned hobbyist coder",
    "Cloud architect with AWS and GCP experience",
    "Mobile developer building cross-platform apps",
    "Security researcher and CTF player",
    "Database engineer optimizing queries",
    "Frontend developer obsessed with performance",
};
#define N_BIOS 15

/* Lowercase a name into out (in-place safe with the static table since
   we're not mutating the source). out must be sized for input length+1. */
static void lower_str(const char *in, char *out) {
    while (*in) { *out++ = (*in >= 'A' && *in <= 'Z') ? (char)(*in + 32) : *in; in++; }
    *out = '\0';
}

/* sha256("user-<i>")[:32] hex — matches python's
     hashlib.sha256(f'user-{i}'.encode()).hexdigest()[:32]. */
static void make_user_key(int i, char out[33]) {
    char buf[32];
    int n = snprintf(buf, sizeof(buf), "user-%d", i);
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char *)buf, (size_t)n, digest);
    static const char hex[] = "0123456789abcdef";
    for (int j = 0; j < 16; j++) {
        out[2 * j]     = hex[digest[j] >> 4];
        out[2 * j + 1] = hex[digest[j] & 0xF];
    }
    out[32] = '\0';
}

/* memfd_create wrapper — matches the syscall pattern used in bench_kv.c
   and friends. Returns fd; caller closes. */
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

/* -------- run() helper: time one query, print "label  Xms  resp[..100c]" */

static void run_one(TestClient *tc, const char *label, const char *json) {
    uint64_t t0 = bench_now_ns();
    char *resp = NULL;
    int rc = tc_request(tc, json, &resp);
    uint64_t t1 = bench_now_ns();
    long ms = (long)((t1 - t0) / 1000000ULL);
    char short_buf[128];
    if (rc == 0 && resp) {
        size_t len = strlen(resp);
        size_t copy = len > 100 ? 100 : len;
        memcpy(short_buf, resp, copy);
        short_buf[copy] = '\0';
        if (len > 100) {
            short_buf[100] = '.';  short_buf[101] = '.';
            short_buf[102] = '.';  short_buf[103] = '\0';
        }
    } else {
        strcpy(short_buf, "(no response)");
    }
    printf("  %-58s %6ldms  %s\n", label, ms, short_buf);
    free(resp);
}

#define RUN(tc, label, ...) run_one((tc), (label), __VA_ARGS__)

/* -------- dataset generation (~700 bytes / record at the 1M-record scale) */

static int build_users_json(int count, char **out_buf, size_t *out_size) {
    /* Cap per-record size at ~720 bytes (long bio + email + everything else). */
    size_t cap = (size_t)count * 720 + 16;
    char *buf = malloc(cap);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = '[';
    for (int i = 0; i < count; i++) {
        if (pos + 720 > cap) {
            cap = pos + (size_t)(count - i) * 720 + 16;
            char *t = realloc(buf, cap);
            if (!t) { free(buf); return -1; }
            buf = t;
        }
        const char *fn = FIRST_NAMES[i % N_FIRST];
        const char *ln = LAST_NAMES[i % N_LAST];
        char fn_lc[32], ln_lc[32];
        lower_str(fn, fn_lc); lower_str(ln, ln_lc);
        const char *dom = DOMAINS[i % N_DOMAINS];
        const char *bio = BIOS[i % N_BIOS];

        int    age = 18 + (i % 60);
        long   uid = 100000 + i;
        int    rnk = (i % 100) + 1;
        /* Pseudo-random but deterministic score/balance/hourly_rate via
           the LCG `i * 2654435761u` so test bands are predictable. */
        unsigned mix = (unsigned)i * 2654435761u;
        double score = (double)(mix % 1000000) / 10000.0;          /* 0..100 */
        int    active_flag = (i % 7 != 0);
        int    level = i % 256;
        int    year  = 1960 + (i % 46);
        int    mo    = (i % 12) + 1;
        int    da    = (i % 28) + 1;
        int    c_h   = i % 24;
        int    c_m   = i % 60;
        int    c_s   = (i * 7) % 60;
        long   bal_cents = (long)(((long)((mix >> 8) % 5050000) - 50000)); /* -500..50000 dollars in cents-ish */
        double balance = (double)bal_cents / 100.0;
        double hourly  = 10.0 + (double)((mix >> 16) % 49000) / 100.0;     /* 10..500 */

        char key[33];
        make_user_key(i, key);

        int n = snprintf(buf + pos, cap - pos,
            "%s{\"key\":\"%s\",\"value\":{"
              "\"username\":\"%s.%s%d\","
              "\"email\":\"%s%d@%s\","
              "\"bio\":\"%s\","
              "\"age\":%d,"
              "\"user_id\":%ld,"
              "\"rank\":%d,"
              "\"score\":%.4f,"
              "\"active\":%s,"
              "\"level\":%d,"
              "\"birthday\":\"%04d%02d%02d\","
              "\"created_at\":\"2024%02d%02d%02d%02d%02d\","
              "\"balance\":\"%.2f\","
              "\"hourly_rate\":\"%.4f\""
            "}}",
            i > 0 ? "," : "",
            key,
            fn_lc, ln_lc, i,
            fn_lc, i, dom,
            bio,
            age, uid, rnk, score,
            active_flag ? "true" : "false",
            level,
            year, mo, da,
            mo, da, c_h, c_m, c_s,
            balance, hourly);
        if (n < 0 || (size_t)n >= cap - pos) { free(buf); return -1; }
        pos += (size_t)n;
    }
    buf[pos++] = ']';
    *out_buf = buf;
    *out_size = pos;
    return 0;
}

/* ---------------------------------------------------------------- main bench */

static int bench_queries_run(void) {
    const char *count_env = getenv("SHARD_BENCH_COUNT");
    int COUNT = count_env ? atoi(count_env) : 1000000;
    if (COUNT <= 0) COUNT = 1000000;

    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        fprintf(stderr, "bench-queries: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 600000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    /* create users object — schema mirrors bench/create-user-object.sh exactly. */
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

    printf("======================================\n");
    printf("  shard-db QUERY benchmark — %d users\n", COUNT);
    printf("======================================\n\n");

    /* Build dataset + bulk-insert. */
    printf("Generating %d user records...\n", COUNT);
    fflush(stdout);
    char *buf = NULL; size_t sz = 0;
    if (build_users_json(COUNT, &buf, &sz) != 0) {
        fprintf(stderr, "bench-queries: OOM building dataset\n");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    printf("  dataset: %.1f MB\n", (double)sz / (1024.0 * 1024.0));
    int data_fd = make_memfd("queries-users", buf, sz);
    free(buf);
    if (data_fd < 0) { tc_close(tc); test_env_stop(&env); return 1; }

    char ins_req[256];
    snprintf(ins_req, sizeof(ins_req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"users\","
        "\"file\":\"/proc/%d/fd/%d\"}", (int)getpid(), data_fd);
    uint64_t t0 = bench_now_ns();
    tc_request(tc, ins_req, &resp);
    uint64_t t1 = bench_now_ns();
    printf("  insert: wall=%.2fs throughput=%.2f k/sec resp=%s\n\n",
           (double)(t1 - t0) / 1e9,
           (double)COUNT / 1000.0 / ((double)(t1 - t0) / 1e9),
           resp ? resp : "(null)");
    free(resp); resp = NULL;
    close(data_fd);

    /* ==================== COUNT ==================== */
    printf("--- COUNT ---\n");
    printf("  [no criteria]\n");
    RUN(tc, "count all (metadata)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\"}");

    printf("  [indexed field: age]\n");
    RUN(tc, "count eq (age=30)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"}]}");
    RUN(tc, "count neq (age!=30)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"neq\",\"value\":\"30\"}]}");
    RUN(tc, "count gt (age>50)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"50\"}]}");
    RUN(tc, "count lt (age<25)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"lt\",\"value\":\"25\"}]}");
    RUN(tc, "count gte (age>=60)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"60\"}]}");
    RUN(tc, "count lte (age<=20)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"lte\",\"value\":\"20\"}]}");
    RUN(tc, "count between (age 30-40)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"between\",\"value\":\"30\",\"value2\":\"40\"}]}");
    RUN(tc, "count in (age in 20,30,40,50)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"in\",\"value\":\"20,30,40,50\"}]}");

    printf("  [indexed field: active]\n");
    RUN(tc, "count eq (active=false)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"}]}");
    RUN(tc, "count neq (active!=false)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"neq\",\"value\":\"false\"}]}");

    printf("  [indexed field: username]\n");
    RUN(tc, "count starts (username starts alice)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"starts\",\"value\":\"alice\"}]}");
    RUN(tc, "count contains (username contains baker)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"contains\",\"value\":\"baker\"}]}");
    RUN(tc, "count ends (username ends 99)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"ends\",\"value\":\"99\"}]}");
    RUN(tc, "count ncontains (username !contain baker)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"ncontains\",\"value\":\"baker\"}]}");
    RUN(tc, "count exists (username exists)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"exists\"}]}");

    printf("  [non-indexed field: balance]\n");
    RUN(tc, "count lt (balance<0) FULL SCAN",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"lt\",\"value\":\"0\"}]}");
    RUN(tc, "count gt (score>90) FULL SCAN",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"90\"}]}");

    printf("  [indexed + secondary]\n");
    RUN(tc, "count (active=false AND balance<0)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"},{\"field\":\"balance\",\"op\":\"lt\",\"value\":\"0\"}]}");
    RUN(tc, "count (age>50 AND score>80)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"50\"},{\"field\":\"score\",\"op\":\"gt\",\"value\":\"80\"}]}");

    /* ==================== FIND ==================== */
    printf("\n--- FIND (limit 10) ---\n");
    printf("  [indexed ops]\n");
    RUN(tc, "find eq (active=false)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    RUN(tc, "find neq (active!=false)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"neq\",\"value\":\"false\"}],\"limit\":10,\"fields\":[\"username\",\"active\"]}");
    RUN(tc, "find gt (age>70)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"70\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    RUN(tc, "find lt (age<20)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"lt\",\"value\":\"20\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    RUN(tc, "find between (age 25-35)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"between\",\"value\":\"25\",\"value2\":\"35\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    RUN(tc, "find in (age in 18,25,50,77)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"in\",\"value\":\"18,25,50,77\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    RUN(tc, "find starts (username starts alice)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"starts\",\"value\":\"alice\"}],\"limit\":10,\"fields\":[\"username\"]}");
    RUN(tc, "find contains (email contains yahoo)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"contains\",\"value\":\"yahoo\"}],\"limit\":10,\"fields\":[\"username\",\"email\"]}");
    RUN(tc, "find ends (email ends dev.io)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"ends\",\"value\":\"dev.io\"}],\"limit\":10,\"fields\":[\"username\",\"email\"]}");
    RUN(tc, "find exists (birthday exists)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"exists\"}],\"limit\":10,\"fields\":[\"username\",\"birthday\"]}");

    printf("  [non-indexed]\n");
    RUN(tc, "find gt (balance>40000) FULL SCAN",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"gt\",\"value\":\"40000\"}],\"limit\":10,\"fields\":[\"username\",\"balance\"]}");
    RUN(tc, "find contains (bio contains DevOps) FULL SCAN",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"DevOps\"}],\"limit\":10,\"fields\":[\"username\",\"bio\"]}");

    printf("  [indexed + secondary]\n");
    RUN(tc, "find (age>60 AND balance>30000)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"60\"},{\"field\":\"balance\",\"op\":\"gt\",\"value\":\"30000\"}],\"limit\":10,\"fields\":[\"username\",\"age\",\"balance\"]}");
    RUN(tc, "find (active=false AND score>95)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"},{\"field\":\"score\",\"op\":\"gt\",\"value\":\"95\"}],\"limit\":10,\"fields\":[\"username\",\"score\"]}");

    /* ==================== AGGREGATE ==================== */
    printf("\n--- AGGREGATE ---\n");
    printf("  [no criteria — full scan]\n");
    RUN(tc, "agg count all",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}");
    RUN(tc, "agg sum/avg/min/max balance",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"count\"},{\"fn\":\"sum\",\"field\":\"balance\"},{\"fn\":\"avg\",\"field\":\"balance\"},{\"fn\":\"min\",\"field\":\"balance\"},{\"fn\":\"max\",\"field\":\"balance\"}]}");
    RUN(tc, "agg group by active",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"group_by\":[\"active\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"balance\"}]}");
    RUN(tc, "agg group by age top 5",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"group_by\":[\"age\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],\"order_by\":\"n\",\"order\":\"desc\",\"limit\":5}");

    printf("  [indexed criteria]\n");
    RUN(tc, "agg where active=false",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"}],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"age\"},{\"fn\":\"avg\",\"field\":\"balance\"}]}");
    RUN(tc, "agg where age>50",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"50\"}],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"balance\"}]}");
    RUN(tc, "agg where age neq 30",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"neq\",\"value\":\"30\"}],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}");
    RUN(tc, "agg where username starts alice",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"starts\",\"value\":\"alice\"}],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"age\"}]}");
    RUN(tc, "agg group by active where age>50",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"50\"}],\"group_by\":[\"active\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"balance\"}]}");

    printf("  [non-indexed criteria]\n");
    RUN(tc, "agg where balance<0 FULL SCAN",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"lt\",\"value\":\"0\"}],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"balance\"}]}");

    printf("  [indexed + secondary]\n");
    RUN(tc, "agg where active=false AND balance<0",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"},{\"field\":\"balance\",\"op\":\"lt\",\"value\":\"0\"}],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"balance\"}]}");

    printf("  [having]\n");
    RUN(tc, "agg group by age having n>16000",
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"group_by\":[\"age\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"balance\",\"alias\":\"avg_bal\"}],\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"16000\"}],\"order_by\":\"avg_bal\",\"order\":\"desc\"}");

    /* ==================== CURSOR (keyset pagination) ==================== */
    printf("\n--- CURSOR (keyset pagination) ---\n");
    printf("  [Page 1 — signal cursor mode with cursor:null]\n");
    RUN(tc, "cursor ASC page 1 (age, limit 100)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"age\",\"order\":\"asc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"age\"]}");
    RUN(tc, "cursor DESC page 1 (age, limit 100)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"age\",\"order\":\"desc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"age\"]}");
    RUN(tc, "cursor ASC page 1 + criteria (active=false)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"}],\"order_by\":\"age\",\"order\":\"asc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"age\"]}");

    printf("  [Continuation — hand back a cursor to a mid-range position]\n");
    RUN(tc, "cursor ASC page N (age=50, continuation)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"age\",\"order\":\"asc\",\"limit\":100,\"cursor\":{\"age\":\"50\",\"key\":\"00000000000000000000000000000000\"},\"fields\":[\"username\",\"age\"]}");
    RUN(tc, "cursor DESC page N (age=30, continuation)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"age\",\"order\":\"desc\",\"limit\":100,\"cursor\":{\"age\":\"30\",\"key\":\"ffffffffffffffffffffffffffffffff\"},\"fields\":[\"username\",\"age\"]}");

    printf("  [Offset-based deep page for contrast — buffer-sort path]\n");
    RUN(tc, "offset 50000 limit 100 order_by age (no cursor, full buffer-sort)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"age\",\"order\":\"asc\",\"offset\":50000,\"limit\":100,\"fields\":[\"username\",\"age\"]}");

    printf("\n======================================\n");
    printf("  Query bench complete\n");
    printf("======================================\n");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("bench-queries", bench_queries_run)
