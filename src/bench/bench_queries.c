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
#include "bench_table.h"
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

/* SPLITS is now derived per-run from SHARD_BENCH_COUNT (or overridden via
   SHARD_BENCH_SPLITS); see bench_queries_run() for the tier table. */

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

/* -------- dataset generation (~700 bytes / record at the 1M-record scale) */

/* Build one chunk of records [start_i, start_i+count) as a JSON array.
 * Used by the chunked-insert main loop so the in-memory blob stays
 * bounded regardless of total record count: at 10M per chunk × ~720B
 * per record, peak buffer is ~7 GB, fits comfortably in modern RAM
 * even at SHARD_BENCH_COUNT=100M. */
static int build_users_chunk_json(int start_i, int count, char **out_buf, size_t *out_size) {
    size_t cap = (size_t)count * 720 + 16;
    char *buf = malloc(cap);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = '[';
    for (int k = 0; k < count; k++) {
        int i = start_i + k;
        if (pos + 720 > cap) {
            cap = pos + (size_t)(count - k) * 720 + 16;
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
        unsigned mix = (unsigned)i * 2654435761u;
        double score = (double)(mix % 1000000) / 10000.0;
        int    active_flag = (i % 7 != 0);
        int    level = i % 256;
        int    year  = 1960 + (i % 46);
        int    mo    = (i % 12) + 1;
        int    da    = (i % 28) + 1;
        int    c_h   = i % 24;
        int    c_m   = i % 60;
        int    c_s   = (i * 7) % 60;
        long   bal_cents = (long)(((long)((mix >> 8) % 5050000) - 50000));
        double balance = (double)bal_cents / 100.0;
        double hourly  = 10.0 + (double)((mix >> 16) % 49000) / 100.0;

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
            k > 0 ? "," : "",
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

#define BR(label, json) bench_table_run(tc, (label), (json))

static int bench_queries_run(void) {
    const char *count_env = getenv("SHARD_BENCH_COUNT");
    long COUNT = count_env ? atol(count_env) : 1000000L;
    if (COUNT <= 0) COUNT = 1000000;

    /* SHARD_BENCH_CHUNK controls records-per-bulk-insert call. Default
       1M — small enough for prompt progress on disk-backed runs, big
       enough that per-chunk overhead stays sub-1% of insert wall time.
       (10M chunks were silently slow on /tmp tmpfs; on real disk they
       hide progress for ~1+ min per chunk.) */
    const char *chunk_env = getenv("SHARD_BENCH_CHUNK");
    long CHUNK_SIZE = chunk_env ? atol(chunk_env) : 1000000L;
    if (CHUNK_SIZE <= 0) CHUNK_SIZE = 1000000;
    if (CHUNK_SIZE > COUNT) CHUNK_SIZE = COUNT;

    /* SHARD_BENCH_SPLITS overrides the create-object splits value. Default
       picks the per-tier recommendation from docs/operations/tuning.md so
       higher SHARD_BENCH_COUNT runs land in the 78K–200K rec/shard sweet
       spot without manual tuning. Power-of-2 in [8, 4096]. */
    const char *splits_env = getenv("SHARD_BENCH_SPLITS");
    long SPLITS;
    if (splits_env) {
        SPLITS = atol(splits_env);
    } else {
        if      (COUNT <=    1000000L) SPLITS = 8;
        else if (COUNT <=    4000000L) SPLITS = 16;
        else if (COUNT <=   10000000L) SPLITS = 32;
        else if (COUNT <=   25000000L) SPLITS = 64;
        else if (COUNT <=   60000000L) SPLITS = 128;
        else if (COUNT <=  125000000L) SPLITS = 256;
        else if (COUNT <=  250000000L) SPLITS = 512;
        else if (COUNT <=  500000000L) SPLITS = 1024;
        else if (COUNT <= 1000000000L) SPLITS = 2048;
        else                           SPLITS = 4096;
    }
    if (SPLITS < 8) SPLITS = 8;
    if (SPLITS > 4096) SPLITS = 4096;
    /* Round to power of two if needed. */
    {
        long p = 8;
        while (p < SPLITS) p <<= 1;
        SPLITS = p;
    }

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

    /* Schema: 13 fields, `bio` left non-indexed so we exercise full-scan
       paths. ALL 12 indexes created upfront — production workloads
       always have their indexes from the start, so the bench mirrors
       that even at high N. (If chunked-bulk-insert into a populated
       indexed table hits performance issues, the engine needs to fix
       it — not the bench's job to paper over it.) */
    char create_obj[2048];
    snprintf(create_obj, sizeof(create_obj),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"users\","
        "\"splits\":%ld,\"max_key\":32,\"storage_version\":%d,"
        "\"fields\":[\"username:varchar:50\",\"email:varchar:100\","
                    "\"bio:varchar:500\",\"age:int\",\"user_id:long\","
                    "\"rank:short\",\"score:double\",\"active:bool\","
                    "\"level:byte\",\"birthday:date\",\"created_at:datetime\","
                    "\"balance:numeric:12,2\",\"hourly_rate:currency\"],"
        "\"indexes\":[\"username\",\"email\",\"age\",\"user_id\",\"rank\","
                     "\"score\",\"active\",\"level\",\"birthday\","
                     "\"created_at\",\"balance\",\"hourly_rate\"]}",
        SPLITS, bench_storage_version());
    tc_request(tc, create_obj, &resp);
    free(resp); resp = NULL;

    printf("======================================================================\n");
    printf("  shard-db QUERY benchmark — %ld users  (splits=%ld, chunk = %ld)\n",
           COUNT, SPLITS, CHUNK_SIZE);
    printf("======================================================================\n\n");

    /* Chunked insert. Build CHUNK_SIZE records at a time, push via memfd,
       free the buffer. Bounds peak memory regardless of total COUNT. */
    long inserted = 0;
    uint64_t insert_t0 = bench_now_ns();
    while (inserted < COUNT) {
        long remaining = COUNT - inserted;
        int chunk = (remaining < CHUNK_SIZE) ? (int)remaining : CHUNK_SIZE;

        printf("  chunk: rows %ld..%ld (%d records)…", inserted, inserted + chunk, chunk);
        fflush(stdout);

        char *buf = NULL; size_t sz = 0;
        if (build_users_chunk_json((int)inserted, chunk, &buf, &sz) != 0) {
            fprintf(stderr, "\nbench-queries: OOM building chunk at i=%ld\n", inserted);
            tc_close(tc); test_env_stop(&env); return 1;
        }

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
        close(data_fd);
        free(resp); resp = NULL;
        printf(" %.2fs (%.2f M/sec)\n",
               (double)(t1 - t0) / 1e9,
               (double)chunk / 1e6 / ((double)(t1 - t0) / 1e9));
        inserted += chunk;
    }
    uint64_t insert_t1 = bench_now_ns();
    printf("  total insert: %.2fs  throughput: %.2f M/sec\n\n",
           (double)(insert_t1 - insert_t0) / 1e9,
           (double)COUNT / 1e6 / ((double)(insert_t1 - insert_t0) / 1e9));

    /* ==================================================================
       Sections — every operator class on every applicable field type.
       Every section uses bench_table_run so output is bar-charted with
       per-section min/p50/max footers.
       ================================================================== */

    /* ---------- COUNT — eq on every indexed type ---------- */
    bench_table_section_begin("COUNT — eq by field type");
    BR("eq active=false       (bool)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"}]}");
    BR("eq age=30             (int)",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"}]}");
    BR("eq user_id=500000     (long)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"user_id\",\"op\":\"eq\",\"value\":\"500000\"}]}");
    BR("eq rank=42            (short)",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"rank\",\"op\":\"eq\",\"value\":\"42\"}]}");
    BR("eq score=50.0         (double)",    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"50\"}]}");
    BR("eq level=128          (byte)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"level\",\"op\":\"eq\",\"value\":\"128\"}]}");
    BR("eq birthday=19850515  (date)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"eq\",\"value\":\"19850515\"}]}");
    BR("eq created_at exact   (datetime)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"created_at\",\"op\":\"eq\",\"value\":\"20240101000000\"}]}");
    BR("eq balance=1000.00    (numeric)",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"eq\",\"value\":\"1000.00\"}]}");
    BR("eq hourly=100.00      (currency)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"hourly_rate\",\"op\":\"eq\",\"value\":\"100.0000\"}]}");
    BR("eq username           (varchar idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"eq\",\"value\":\"alice.smith0\"}]}");
    BR("eq email              (varchar idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"eq\",\"value\":\"alice0@gmail.com\"}]}");
    BR("eq bio                (varchar non-idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"eq\",\"value\":\"DevOps engineer automating everything\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — neq on every indexed type ---------- */
    bench_table_section_begin("COUNT — neq by field type");
    BR("neq active!=false     (bool)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"neq\",\"value\":\"false\"}]}");
    BR("neq age!=30           (int)",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"neq\",\"value\":\"30\"}]}");
    BR("neq user_id!=500000   (long)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"user_id\",\"op\":\"neq\",\"value\":\"500000\"}]}");
    BR("neq rank!=42          (short)",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"rank\",\"op\":\"neq\",\"value\":\"42\"}]}");
    BR("neq score!=50.0       (double)",    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"score\",\"op\":\"neq\",\"value\":\"50\"}]}");
    BR("neq level!=128        (byte)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"level\",\"op\":\"neq\",\"value\":\"128\"}]}");
    BR("neq birthday!=…       (date)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"neq\",\"value\":\"19850515\"}]}");
    BR("neq created_at!=…     (datetime)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"created_at\",\"op\":\"neq\",\"value\":\"20240101000000\"}]}");
    BR("neq balance!=1000.00  (numeric)",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"neq\",\"value\":\"1000.00\"}]}");
    BR("neq hourly!=100       (currency)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"hourly_rate\",\"op\":\"neq\",\"value\":\"100.0000\"}]}");
    BR("neq username          (varchar idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"neq\",\"value\":\"alice.smith0\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — range (gt/lt/gte/lte/between) by type ---------- */
    bench_table_section_begin("COUNT — range bounds by field type (single-bound)");
    BR("gt age>50             (int)",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"50\"}]}");
    BR("lt age<25             (int)",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"lt\",\"value\":\"25\"}]}");
    BR("gte age>=60           (int)",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"60\"}]}");
    BR("lte age<=20           (int)",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"lte\",\"value\":\"20\"}]}");
    BR("between age 30..40    (int)",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"between\",\"value\":\"30\",\"value2\":\"40\"}]}");
    BR("gt user_id>500000     (long)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"user_id\",\"op\":\"gt\",\"value\":\"500000\"}]}");
    BR("lt user_id<200000     (long)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"user_id\",\"op\":\"lt\",\"value\":\"200000\"}]}");
    BR("gt rank>50            (short)",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"rank\",\"op\":\"gt\",\"value\":\"50\"}]}");
    BR("between rank 25..75   (short)",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"rank\",\"op\":\"between\",\"value\":\"25\",\"value2\":\"75\"}]}");
    BR("gt score>50           (double)",    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}]}");
    BR("lt score<25           (double)",    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"score\",\"op\":\"lt\",\"value\":\"25\"}]}");
    BR("gt level>200          (byte)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"level\",\"op\":\"gt\",\"value\":\"200\"}]}");
    BR("lt level<32           (byte)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"level\",\"op\":\"lt\",\"value\":\"32\"}]}");
    BR("gte birthday>=2000    (date)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"gte\",\"value\":\"20000101\"}]}");
    BR("lte birthday<=1980    (date)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"lte\",\"value\":\"19800101\"}]}");
    BR("gt created_at>jun     (datetime)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"created_at\",\"op\":\"gt\",\"value\":\"20240601000000\"}]}");
    BR("lt created_at<feb     (datetime)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"created_at\",\"op\":\"lt\",\"value\":\"20240201000000\"}]}");
    BR("gt balance>0          (numeric)",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"gt\",\"value\":\"0\"}]}");
    BR("lt balance<-100       (numeric)",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"lt\",\"value\":\"-100\"}]}");
    BR("gt hourly>100         (currency)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"hourly_rate\",\"op\":\"gt\",\"value\":\"100\"}]}");
    BR("between hourly 50..200(currency)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"hourly_rate\",\"op\":\"between\",\"value\":\"50\",\"value2\":\"200\"}]}");
    BR("gt username>'m'       (varchar idx)","{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"gt\",\"value\":\"m\"}]}");
    BR("lt username<'d'       (varchar idx)","{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"lt\",\"value\":\"d\"}]}");
    BR("between email a..m    (varchar idx)","{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"between\",\"value\":\"a\",\"value2\":\"m\"}]}");
    bench_table_section_end();

    /* ---------- PAIRED RANGE BOUNDS — the bug class we just fixed ---------- */
    bench_table_section_begin("COUNT — paired same-field bounds (range coalesce)");
    BR("age gte 30 + lte 40              (gte+lte)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"30\"},{\"field\":\"age\",\"op\":\"lte\",\"value\":\"40\"}]}");
    BR("age gt 30 + lt 40                (gt+lt)",    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"30\"},{\"field\":\"age\",\"op\":\"lt\",\"value\":\"40\"}]}");
    BR("age gt 30 + lte 40               (gt+lte)",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"30\"},{\"field\":\"age\",\"op\":\"lte\",\"value\":\"40\"}]}");
    BR("age gte 30 + lt 40               (gte+lt)",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"30\"},{\"field\":\"age\",\"op\":\"lt\",\"value\":\"40\"}]}");
    BR("user_id gte..lte                 (long range)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"user_id\",\"op\":\"gte\",\"value\":\"100000\"},{\"field\":\"user_id\",\"op\":\"lte\",\"value\":\"200000\"}]}");
    BR("score gt..lt                     (double range)","{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"40\"},{\"field\":\"score\",\"op\":\"lt\",\"value\":\"60\"}]}");
    BR("birthday gte..lte                (date range)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"gte\",\"value\":\"19900101\"},{\"field\":\"birthday\",\"op\":\"lte\",\"value\":\"20000101\"}]}");
    BR("created_at gte..lte              (datetime range)","{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"created_at\",\"op\":\"gte\",\"value\":\"20240101000000\"},{\"field\":\"created_at\",\"op\":\"lte\",\"value\":\"20240301000000\"}]}");
    BR("balance gte..lte                 (numeric range)","{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"gte\",\"value\":\"0\"},{\"field\":\"balance\",\"op\":\"lte\",\"value\":\"1000\"}]}");
    BR("hourly gt..lt                    (currency range)","{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"hourly_rate\",\"op\":\"gt\",\"value\":\"50\"},{\"field\":\"hourly_rate\",\"op\":\"lt\",\"value\":\"200\"}]}");
    BR("rank gte..lte                    (short range)",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"rank\",\"op\":\"gte\",\"value\":\"25\"},{\"field\":\"rank\",\"op\":\"lte\",\"value\":\"75\"}]}");
    BR("level gte..lte                   (byte range)",    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"level\",\"op\":\"gte\",\"value\":\"32\"},{\"field\":\"level\",\"op\":\"lte\",\"value\":\"200\"}]}");
    BR("username gte..lte                (varchar idx range)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"gte\",\"value\":\"a\"},{\"field\":\"username\",\"op\":\"lte\",\"value\":\"m\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — set membership (in / not_in) — all indexed types ---------- */
    bench_table_section_begin("COUNT — set membership (in, not_in)");
    BR("active in {true,false} (bool)",         "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"in\",\"value\":\"true,false\"}]}");
    BR("age in {20,30,40,50}    (int)",         "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"in\",\"value\":\"20,30,40,50\"}]}");
    BR("age not_in {20,30,40,50}(int)",         "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"not_in\",\"value\":\"20,30,40,50\"}]}");
    BR("user_id in 4-set        (long)",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"user_id\",\"op\":\"in\",\"value\":\"100000,250000,500000,750000\"}]}");
    BR("user_id not_in 4-set    (long)",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"user_id\",\"op\":\"not_in\",\"value\":\"100000,250000,500000,750000\"}]}");
    BR("rank in {1,50,100}      (short)",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"rank\",\"op\":\"in\",\"value\":\"1,50,100\"}]}");
    BR("rank not_in {1,50,100}  (short)",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"rank\",\"op\":\"not_in\",\"value\":\"1,50,100\"}]}");
    BR("score in 3-set          (double)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"score\",\"op\":\"in\",\"value\":\"10,50,90\"}]}");
    BR("level in {0,128,255}    (byte)",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"level\",\"op\":\"in\",\"value\":\"0,128,255\"}]}");
    BR("birthday in 3-day set   (date)",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"in\",\"value\":\"19900101,19950101,20000101\"}]}");
    BR("created_at in 3-set     (datetime)",    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"created_at\",\"op\":\"in\",\"value\":\"20240101000000,20240601000000,20241201000000\"}]}");
    BR("balance in 3-set        (numeric)",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"in\",\"value\":\"100.00,500.00,1000.00\"}]}");
    BR("hourly in 3-set         (currency)",    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"hourly_rate\",\"op\":\"in\",\"value\":\"50,100,200\"}]}");
    BR("username in 2-set       (varchar idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"in\",\"value\":\"alice.smith0,bob.jones1\"}]}");
    BR("email in 2-set          (varchar idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"in\",\"value\":\"alice0@gmail.com,bob1@yahoo.com\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — string operators (CS) on indexed varchar ---------- */
    bench_table_section_begin("COUNT — string ops CS (indexed varchar: username, email)");
    BR("starts username 'alice'",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"starts\",\"value\":\"alice\"}]}");
    BR("ends username '99'",            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"ends\",\"value\":\"99\"}]}");
    BR("contains username 'baker'",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"contains\",\"value\":\"baker\"}]}");
    BR("not_contains username 'baker'", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"not_contains\",\"value\":\"baker\"}]}");
    BR("like username 'a%'",            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"like\",\"value\":\"a%\"}]}");
    BR("not_like username 'a%'",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"not_like\",\"value\":\"a%\"}]}");
    BR("starts email 'alice'",          "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"starts\",\"value\":\"alice\"}]}");
    BR("ends email '@gmail.com'",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"ends\",\"value\":\"@gmail.com\"}]}");
    BR("contains email '@gmail.com'",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"contains\",\"value\":\"@gmail.com\"}]}");
    BR("like email 'al%'",              "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"like\",\"value\":\"al%\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — string operators on non-indexed bio (full scan) ---------- */
    bench_table_section_begin("COUNT — string ops on non-indexed bio (full scan)");
    BR("starts bio 'Software'",         "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"starts\",\"value\":\"Software\"}]}");
    BR("ends bio 'startup'",            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"ends\",\"value\":\"startup\"}]}");
    BR("contains bio 'DevOps'",         "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"DevOps\"}]}");
    BR("not_contains bio 'DevOps'",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"not_contains\",\"value\":\"DevOps\"}]}");
    BR("like bio '%engineer%'",         "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"like\",\"value\":\"%engineer%\"}]}");
    BR("icontains bio 'devops'",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"icontains\",\"value\":\"devops\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — case-insensitive ---------- */
    bench_table_section_begin("COUNT — string ops CI (i*) on indexed varchar");
    BR("istarts username 'ALICE'",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"istarts\",\"value\":\"ALICE\"}]}");
    BR("iends username '99'",            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"iends\",\"value\":\"99\"}]}");
    BR("icontains username 'BAKER'",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"icontains\",\"value\":\"BAKER\"}]}");
    BR("not_icontains username 'BAKER'", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"not_icontains\",\"value\":\"BAKER\"}]}");
    BR("ilike username 'A%'",            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"ilike\",\"value\":\"A%\"}]}");
    BR("not_ilike username 'A%'",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"not_ilike\",\"value\":\"A%\"}]}");
    BR("istarts email 'ALICE'",          "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"istarts\",\"value\":\"ALICE\"}]}");
    BR("icontains email 'GMAIL'",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"icontains\",\"value\":\"GMAIL\"}]}");
    BR("ilike email 'AL%'",              "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"ilike\",\"value\":\"AL%\"}]}");
    BR("icontains bio 'DEVOPS' (full scan)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"icontains\",\"value\":\"DEVOPS\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — length operators on varchar ---------- */
    bench_table_section_begin("COUNT — length operators (varchar len_*)");
    BR("len_eq username == 12",                  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"len_eq\",\"value\":\"12\"}]}");
    BR("len_neq username != 12",                 "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"len_neq\",\"value\":\"12\"}]}");
    BR("len_lt username < 10",                   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"len_lt\",\"value\":\"10\"}]}");
    BR("len_gt username > 15",                   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"len_gt\",\"value\":\"15\"}]}");
    BR("len_lte username <= 12",                 "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"len_lte\",\"value\":\"12\"}]}");
    BR("len_gte username >= 14",                 "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"len_gte\",\"value\":\"14\"}]}");
    BR("len_between username 10..15",            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"len_between\",\"value\":\"10\",\"value2\":\"15\"}]}");
    BR("len_eq email == 16   (idx varchar)",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"len_eq\",\"value\":\"16\"}]}");
    BR("len_gt email > 20    (idx varchar)",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"len_gt\",\"value\":\"20\"}]}");
    BR("len_between email 12..18",               "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"len_between\",\"value\":\"12\",\"value2\":\"18\"}]}");
    BR("len_gt bio > 40     (full scan)",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"len_gt\",\"value\":\"40\"}]}");
    BR("len_between bio 30..50 (full scan)",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"len_between\",\"value\":\"30\",\"value2\":\"50\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — existence (per type) ---------- */
    bench_table_section_begin("COUNT — existence by field type");
    BR("exists username        (varchar idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"exists\"}]}");
    BR("not_exists username    (varchar idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"not_exists\"}]}");
    BR("exists email           (varchar idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"exists\"}]}");
    BR("exists bio             (varchar non-idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"exists\"}]}");
    BR("exists age             (int)",       "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"exists\"}]}");
    BR("exists user_id         (long)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"user_id\",\"op\":\"exists\"}]}");
    BR("exists rank            (short)",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"rank\",\"op\":\"exists\"}]}");
    BR("exists score           (double)",    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"score\",\"op\":\"exists\"}]}");
    BR("exists active          (bool)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"exists\"}]}");
    BR("exists level           (byte)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"level\",\"op\":\"exists\"}]}");
    BR("exists birthday        (date)",      "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"exists\"}]}");
    BR("exists created_at      (datetime)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"created_at\",\"op\":\"exists\"}]}");
    BR("exists balance         (numeric)",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"exists\"}]}");
    BR("exists hourly_rate     (currency)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"hourly_rate\",\"op\":\"exists\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — regex (indexed btree on varchar; bio = full scan) ---------- */
    bench_table_section_begin("COUNT — regex (varchar)");
    BR("regex username '^(alice|bob)\\.'  (idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"regex\",\"value\":\"^(alice|bob)\\\\.\"}]}");
    BR("not_regex username '^z'           (idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"not_regex\",\"value\":\"^z\"}]}");
    BR("regex email '@(gmail|yahoo)'      (idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"@(gmail|yahoo)\"}]}");
    BR("not_regex email '\\.com$'          (idx)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"not_regex\",\"value\":\"\\\\.com$\"}]}");
    BR("regex bio '(engineer|developer)'  (full)","{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"regex\",\"value\":\"(engineer|developer)\"}]}");
    BR("not_regex bio '^z'                (full)","{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"not_regex\",\"value\":\"^z\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — field-vs-field (always full scan; same-type only) ---------- */
    bench_table_section_begin("COUNT — field-vs-field (always full scan)");
    BR("eq_field age == rank        (int↔short coerced)",  "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"eq_field\",\"value\":\"rank\"}]}");
    BR("neq_field age != rank",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"neq_field\",\"value\":\"rank\"}]}");
    BR("lt_field age < rank",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"lt_field\",\"value\":\"rank\"}]}");
    BR("gt_field age > rank",     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt_field\",\"value\":\"rank\"}]}");
    BR("lte_field age <= rank",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"lte_field\",\"value\":\"rank\"}]}");
    BR("gte_field age >= rank",   "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gte_field\",\"value\":\"rank\"}]}");
    BR("eq_field username == email (varchar↔varchar)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"eq_field\",\"value\":\"email\"}]}");
    BR("lt_field score < hourly_rate (double↔currency)","{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"score\",\"op\":\"lt_field\",\"value\":\"hourly_rate\"}]}");
    bench_table_section_end();

    /* ---------- COUNT — OR widths (tests OR limit pushdown sensitivity) ---------- */
    bench_table_section_begin("COUNT — OR with various leaf counts");
    BR("OR 2 leaves (statuses)",    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"or\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"20\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"}]}]}");
    BR("OR 3 leaves (ages)",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"or\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"20\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"40\"}]}]}");
    BR("OR 5 leaves (ages)",        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"or\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"20\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"40\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"50\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"60\"}]}]}");
    BR("OR cross-field (age=30 OR active=false)", "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"or\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"},{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"}]}]}");
    bench_table_section_end();

    /* ---------- FIND — paired ranges (limit 10) — confirms range coalesce wins on find too ---------- */
    bench_table_section_begin("FIND — paired ranges with limit=10");
    BR("find age 30..40 (gte+lte)",    "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"30\"},{\"field\":\"age\",\"op\":\"lte\",\"value\":\"40\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    BR("find age 30..40 (gt+lt)",      "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"30\"},{\"field\":\"age\",\"op\":\"lt\",\"value\":\"40\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    BR("find user_id 100k..200k",      "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"user_id\",\"op\":\"gte\",\"value\":\"100000\"},{\"field\":\"user_id\",\"op\":\"lte\",\"value\":\"200000\"}],\"limit\":10,\"fields\":[\"username\",\"user_id\"]}");
    BR("find score 40..60",            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"40\"},{\"field\":\"score\",\"op\":\"lt\",\"value\":\"60\"}],\"limit\":10,\"fields\":[\"username\",\"score\"]}");
    BR("find birthday 1990..2000",     "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"gte\",\"value\":\"19900101\"},{\"field\":\"birthday\",\"op\":\"lte\",\"value\":\"20000101\"}],\"limit\":10,\"fields\":[\"username\",\"birthday\"]}");
    BR("find created_at jan-mar",      "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"created_at\",\"op\":\"gte\",\"value\":\"20240101000000\"},{\"field\":\"created_at\",\"op\":\"lte\",\"value\":\"20240301000000\"}],\"limit\":10,\"fields\":[\"username\",\"created_at\"]}");
    BR("find balance 0..1000",         "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"gte\",\"value\":\"0\"},{\"field\":\"balance\",\"op\":\"lte\",\"value\":\"1000\"}],\"limit\":10,\"fields\":[\"username\",\"balance\"]}");
    bench_table_section_end();

    /* ---------- FIND — AND across different fields (PRIMARY_INTERSECT) ---------- */
    bench_table_section_begin("FIND — AND across different indexed fields");
    BR("active=false AND age>50",       "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"},{\"field\":\"age\",\"op\":\"gt\",\"value\":\"50\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    BR("age=30 AND score>50",           "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"},{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}],\"limit\":10,\"fields\":[\"username\",\"score\"]}");
    BR("3-way: active+age+score",       "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"},{\"field\":\"age\",\"op\":\"gte\",\"value\":\"30\"},{\"field\":\"score\",\"op\":\"gte\",\"value\":\"50\"}],\"limit\":10,\"fields\":[\"username\"]}");
    BR("indexed + non-indexed (bio)",   "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"},{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"DevOps\"}],\"limit\":10,\"fields\":[\"username\"]}");
    bench_table_section_end();

    /* ---------- FIND — basic operators on indexed fields (limit=10) ---------- */
    bench_table_section_begin("FIND — basic ops on indexed fields (limit=10)");
    BR("eq age=30",                      "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    BR("neq active!=true",               "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"neq\",\"value\":\"true\"}],\"limit\":10,\"fields\":[\"username\",\"active\"]}");
    BR("in age{20,30,40}",               "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"in\",\"value\":\"20,30,40\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    BR("not_in age{20,30,40}",           "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"not_in\",\"value\":\"20,30,40\"}],\"limit\":10,\"fields\":[\"username\",\"age\"]}");
    BR("starts username 'alice'",        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"starts\",\"value\":\"alice\"}],\"limit\":10,\"fields\":[\"username\"]}");
    BR("contains username 'baker'",      "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"contains\",\"value\":\"baker\"}],\"limit\":10,\"fields\":[\"username\"]}");
    BR("istarts username 'ALICE'",       "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"istarts\",\"value\":\"ALICE\"}],\"limit\":10,\"fields\":[\"username\"]}");
    BR("len_eq username==12",            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"len_eq\",\"value\":\"12\"}],\"limit\":10,\"fields\":[\"username\"]}");
    BR("regex username 'alice|bob'",     "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"regex\",\"value\":\"^(alice|bob)\\\\.\"}],\"limit\":10,\"fields\":[\"username\"]}");
    BR("not_regex username '^z'",        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"username\",\"op\":\"not_regex\",\"value\":\"^z\"}],\"limit\":10,\"fields\":[\"username\"]}");
    BR("exists email",                   "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"email\",\"op\":\"exists\"}],\"limit\":10,\"fields\":[\"username\"]}");
    bench_table_section_end();

    /* ---------- FIND — full-scan fallback on non-indexed field ---------- */
    bench_table_section_begin("FIND — full scan (non-indexed bio)");
    BR("bio contains 'DevOps'",        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"DevOps\"}],\"limit\":10,\"fields\":[\"username\",\"bio\"]}");
    BR("bio starts 'Software'",        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"starts\",\"value\":\"Software\"}],\"limit\":10,\"fields\":[\"username\",\"bio\"]}");
    BR("bio ends 'startup'",           "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"bio\",\"op\":\"ends\",\"value\":\"startup\"}],\"limit\":10,\"fields\":[\"username\"]}");
    bench_table_section_end();

    /* ---------- AGGREGATE — single-fn standalone (per numeric type) ---------- */
    bench_table_section_begin("AGGREGATE — single-fn standalone");
    BR("count all",                "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}");
    BR("sum age              (int)",     "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"age\"}]}");
    BR("avg age              (int)",     "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"age\"}]}");
    BR("sum user_id          (long)",    "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"user_id\"}]}");
    BR("sum rank             (short)",   "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"rank\"}]}");
    BR("sum score            (double)",  "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"score\"}]}");
    BR("avg score            (double)",  "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"score\"}]}");
    BR("sum level            (byte)",    "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"level\"}]}");
    BR("sum balance          (numeric)", "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"balance\"}]}");
    BR("avg balance          (numeric)", "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"balance\"}]}");
    BR("sum hourly_rate      (currency)","{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"hourly_rate\"}]}");
    BR("avg hourly_rate      (currency)","{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"hourly_rate\"}]}");
    BR("min age            (int)",       "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"min\",\"field\":\"age\"}]}");
    BR("max age            (int)",       "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"max\",\"field\":\"age\"}]}");
    BR("min user_id        (long)",      "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"min\",\"field\":\"user_id\"}]}");
    BR("max user_id        (long)",      "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"max\",\"field\":\"user_id\"}]}");
    BR("min rank           (short)",     "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"min\",\"field\":\"rank\"}]}");
    BR("max rank           (short)",     "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"max\",\"field\":\"rank\"}]}");
    BR("min score          (double)",    "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"min\",\"field\":\"score\"}]}");
    BR("max score          (double)",    "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"max\",\"field\":\"score\"}]}");
    BR("min level          (byte)",      "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"min\",\"field\":\"level\"}]}");
    BR("max level          (byte)",      "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"max\",\"field\":\"level\"}]}");
    BR("min birthday       (date)",      "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"min\",\"field\":\"birthday\"}]}");
    BR("max birthday       (date)",      "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"max\",\"field\":\"birthday\"}]}");
    BR("min created_at     (datetime)",  "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"min\",\"field\":\"created_at\"}]}");
    BR("max created_at     (datetime)",  "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"max\",\"field\":\"created_at\"}]}");
    BR("min balance        (numeric)",   "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"min\",\"field\":\"balance\"}]}");
    BR("max balance        (numeric)",   "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"max\",\"field\":\"balance\"}]}");
    BR("min hourly_rate    (currency)",  "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"min\",\"field\":\"hourly_rate\"}]}");
    BR("max hourly_rate    (currency)",  "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"max\",\"field\":\"hourly_rate\"}]}");
    bench_table_section_end();

    /* ---------- AGGREGATE — with criteria (post-filter cost) ---------- */
    bench_table_section_begin("AGGREGATE — with criteria");
    BR("min age where active=true",     "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}],\"aggregates\":[{\"fn\":\"min\",\"field\":\"age\"}]}");
    BR("max balance where age>40",      "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"40\"}],\"aggregates\":[{\"fn\":\"max\",\"field\":\"balance\"}]}");
    BR("avg score where active=false",  "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"}],\"aggregates\":[{\"fn\":\"avg\",\"field\":\"score\"}]}");
    BR("min age birthday 1990..2000",   "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"gte\",\"value\":\"19900101\"},{\"field\":\"birthday\",\"op\":\"lte\",\"value\":\"20000101\"}],\"aggregates\":[{\"fn\":\"min\",\"field\":\"age\"}]}");
    BR("agg where active=false (count+avg)", "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"}],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"age\"}]}");
    BR("agg age neq 30 (NEQ short)",    "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"neq\",\"value\":\"30\"}],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}");
    /* Same-field min/max — criterion field == agg field. Should hit the
       btree-only shortcut: walk the agg btree within criterion bounds, take
       first leaf entry per shard. No KeySet, no record fetch. */
    BR("min age where age>30 (same)",   "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gt\",\"value\":\"30\"}],\"aggregates\":[{\"fn\":\"min\",\"field\":\"age\"}]}");
    BR("max age where age<60 (same)",   "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"lt\",\"value\":\"60\"}],\"aggregates\":[{\"fn\":\"max\",\"field\":\"age\"}]}");
    BR("min birthday in 1990..2000 (same)", "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"birthday\",\"op\":\"gte\",\"value\":\"19900101\"},{\"field\":\"birthday\",\"op\":\"lte\",\"value\":\"20000101\"}],\"aggregates\":[{\"fn\":\"min\",\"field\":\"birthday\"}]}");
    BR("max balance where balance>0 (same)", "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"balance\",\"op\":\"gt\",\"value\":\"0\"}],\"aggregates\":[{\"fn\":\"max\",\"field\":\"balance\"}]}");
    /* Multi-leaf AND min/max — 2+ indexed leaves, intersect-eligible ops.
       Should hit the new PRIMARY_INTERSECT-aware shortcut: build candidate
       KeySet via intersect_indexed_leaves, walk agg btree, first in-set
       hash per shard wins. No record fetches. */
    BR("min age where active=true AND score>50",  "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"},{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}],\"aggregates\":[{\"fn\":\"min\",\"field\":\"age\"}]}");
    BR("max balance where active=true AND age>30","{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"},{\"field\":\"age\",\"op\":\"gt\",\"value\":\"30\"}],\"aggregates\":[{\"fn\":\"max\",\"field\":\"balance\"}]}");
    BR("min score where age 25..50 AND active=true","{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"25\"},{\"field\":\"age\",\"op\":\"lte\",\"value\":\"50\"},{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}],\"aggregates\":[{\"fn\":\"min\",\"field\":\"score\"}]}");
    bench_table_section_end();

    /* ---------- AGGREGATE — bundled + group_by + having ---------- */
    bench_table_section_begin("AGGREGATE — bundled & grouped");
    BR("sum/avg/min/max balance",      "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"aggregates\":[{\"fn\":\"count\"},{\"fn\":\"sum\",\"field\":\"balance\"},{\"fn\":\"avg\",\"field\":\"balance\"},{\"fn\":\"min\",\"field\":\"balance\"},{\"fn\":\"max\",\"field\":\"balance\"}]}");
    BR("group by active",              "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"group_by\":[\"active\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"balance\"}]}");
    BR("group by age top 5",           "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"group_by\":[\"age\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],\"order_by\":\"n\",\"order\":\"desc\",\"limit\":5}");
    BR("group by age having n>16k",    "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"group_by\":[\"age\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"16000\"}]}");
    /* Indexed group_by fast path — group_by field has btree, every spec is
       count(*) or sum/avg/min/max on an indexed non-varchar field. Walks
       group_by btree once + each agg field's btree once via a hash16 →
       bucket map; skips per-record decode. Should drop bundled+grouped
       cases from ~250-400ms to tens of ms. */
    BR("group by active, sum(balance)",       "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"group_by\":[\"active\"],\"aggregates\":[{\"fn\":\"sum\",\"field\":\"balance\"}]}");
    BR("group by active, min/max balance",    "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"group_by\":[\"active\"],\"aggregates\":[{\"fn\":\"min\",\"field\":\"balance\"},{\"fn\":\"max\",\"field\":\"balance\"}]}");
    BR("group by age, avg(score)",            "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"group_by\":[\"age\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"score\"}]}");
    BR("group by birthday, count",            "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"users\",\"group_by\":[\"birthday\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}");
    bench_table_section_end();

    /* ---------- CURSOR — keyset pagination by various indexed types ---------- */
    bench_table_section_begin("CURSOR (keyset pagination, limit 100)");
    BR("ASC by age            (int)",        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"age\",\"order\":\"asc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"age\"]}");
    BR("DESC by age           (int)",        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"age\",\"order\":\"desc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"age\"]}");
    BR("ASC by user_id        (long)",       "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"user_id\",\"order\":\"asc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"user_id\"]}");
    BR("ASC by score          (double)",     "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"score\",\"order\":\"asc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"score\"]}");
    BR("ASC by birthday       (date)",       "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"birthday\",\"order\":\"asc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"birthday\"]}");
    BR("ASC by created_at     (datetime)",   "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"created_at\",\"order\":\"asc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"created_at\"]}");
    BR("ASC by balance        (numeric)",    "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"balance\",\"order\":\"asc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"balance\"]}");
    BR("ASC by username       (varchar)",    "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"username\",\"order\":\"asc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\"]}");
    BR("ASC + criteria active=false (age)",  "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"}],\"order_by\":\"age\",\"order\":\"asc\",\"limit\":100,\"cursor\":null,\"fields\":[\"username\",\"age\"]}");
    BR("ASC continuation @age=50",           "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"age\",\"order\":\"asc\",\"limit\":100,\"cursor\":{\"age\":\"50\",\"key\":\"00000000000000000000000000000000\"},\"fields\":[\"username\",\"age\"]}");
    BR("offset 50000 limit 100",             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[],\"order_by\":\"age\",\"order\":\"asc\",\"offset\":50000,\"limit\":100,\"fields\":[\"username\",\"age\"]}");
    bench_table_section_end();

    printf("\n======================================================================\n");
    printf("  Query bench complete\n");
    printf("======================================================================\n");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("bench-queries", bench_queries_run)
