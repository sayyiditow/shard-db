/* Isolated parse benchmark for yyjson on an invoice JSON file.
 * Compile: gcc -O2 -o bench-yyjson bench/bench-yyjson.c src/yyjson.c
 * Usage:   ./bench-yyjson [path]
 * Default path: /tmp/shard-db_par_single.json
 * Reports parse-only GB/s + parse-plus-iterate (touching every field) GB/s.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include "../src/yyjson.h"

static double elapsed_s(struct timespec *a, struct timespec *b) {
    return (b->tv_sec - a->tv_sec) + (b->tv_nsec - a->tv_nsec) / 1e9;
}

int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1] : "/tmp/shard-db_par_single.json";
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror("open"); return 1; }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); close(fd); return 1; }
    size_t sz = (size_t)st.st_size;
    void *data = mmap(NULL, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (data == MAP_FAILED) { perror("mmap"); return 1; }
    madvise(data, sz, MADV_SEQUENTIAL);

    /* Warm page cache with a touch pass */
    volatile unsigned long sum = 0;
    for (size_t i = 0; i < sz; i += 4096) sum += ((unsigned char *)data)[i];

    printf("File: %s  size: %.2f MB\n", path, sz / (1024.0 * 1024.0));

    /* ---- Parse-only ---- */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    yyjson_doc *doc = yyjson_read(data, sz, 0);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    if (!doc) { fprintf(stderr, "parse failed\n"); return 1; }
    double parse_s = elapsed_s(&t0, &t1);
    double parse_gbs = (sz / 1e9) / parse_s;
    size_t count = yyjson_arr_size(yyjson_doc_get_root(doc));
    printf("parse-only:            %6.3f s   %5.2f GB/s   (%zu records)\n",
           parse_s, parse_gbs, count);

    /* ---- Parse + iterate every field of every record ---- */
    yyjson_doc_free(doc);
    clock_gettime(CLOCK_MONOTONIC, &t0);
    doc = yyjson_read(data, sz, 0);
    if (!doc) { fprintf(stderr, "parse failed\n"); return 1; }
    yyjson_val *root = yyjson_doc_get_root(doc);
    size_t total_fields = 0;
    volatile size_t checksum = 0;
    yyjson_arr_iter it = yyjson_arr_iter_with(root);
    yyjson_val *rec;
    while ((rec = yyjson_arr_iter_next(&it))) {
        yyjson_val *id = yyjson_obj_get(rec, "id");
        if (id) { size_t l; const char *s = yyjson_get_str(id); if (s) { l = strlen(s); checksum += l; } }
        yyjson_val *data_obj = yyjson_obj_get(rec, "data");
        if (data_obj) {
            yyjson_obj_iter fit = yyjson_obj_iter_with(data_obj);
            yyjson_val *k;
            while ((k = yyjson_obj_iter_next(&fit))) {
                yyjson_val *v = yyjson_obj_iter_get_val(k);
                if (yyjson_is_str(v)) {
                    const char *s = yyjson_get_str(v);
                    if (s) checksum += strlen(s);
                } else if (yyjson_is_num(v)) {
                    checksum += (size_t)yyjson_get_real(v);
                }
                total_fields++;
            }
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double full_s = elapsed_s(&t0, &t1);
    double full_gbs = (sz / 1e9) / full_s;
    printf("parse + walk all:      %6.3f s   %5.2f GB/s   (%zu records, %zu field touches)\n",
           full_s, full_gbs, count, total_fields);
    printf("                       records/sec: %.0f\n", count / full_s);

    yyjson_doc_free(doc);
    munmap(data, sz);
    (void)sum; (void)checksum;
    return 0;
}
