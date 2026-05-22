/* src/bench/bench_common.c */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "bench_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/stat.h>
#include <dirent.h>

int bench_make_memfd(const char *name, const char *data, size_t size) {
    /* memfd_create syscall number on x86_64 = 319, aarch64 = 279 */
#if defined(__x86_64__)
    int fd = (int)syscall(319, name, 0);
#elif defined(__aarch64__)
    int fd = (int)syscall(279, name, 0);
#else
    /* Fallback: anonymous tmpfs via mkstemp */
    (void)name;
    char path[] = "/tmp/bench-memfd-XXXXXX";
    int fd = mkstemp(path);
    if (fd >= 0) unlink(path);
#endif
    if (fd < 0) { perror("memfd_create"); return -1; }

    size_t written = 0;
    while (written < size) {
        ssize_t r = write(fd, data + written, size - written);
        if (r <= 0) { perror("write memfd"); close(fd); return -1; }
        written += (size_t)r;
    }
    if (lseek(fd, 0, SEEK_SET) != 0) { perror("lseek"); close(fd); return -1; }
    return fd;
}

long long bench_du_bytes(const char *path) {
    DIR *d = opendir(path);
    if (!d) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISREG(st.st_mode)) return (long long)st.st_size;
        return 0;
    }
    long long total = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        char sub[1024];
        snprintf(sub, sizeof(sub), "%s/%s", path, e->d_name);
        struct stat st;
        if (stat(sub, &st) != 0) continue;
        if (S_ISDIR(st.st_mode)) total += bench_du_bytes(sub);
        else if (S_ISREG(st.st_mode)) total += (long long)st.st_size;
    }
    closedir(d);
    return total;
}

void bench_fmt_bytes(long long b, char *out, size_t outlen) {
    if (b < 1024)             snprintf(out, outlen, "%lld B",  b);
    else if (b < 1024 * 1024) snprintf(out, outlen, "%.1f KB", b / 1024.0);
    else if (b < 1LL << 30)   snprintf(out, outlen, "%.1f MB", b / (1024.0 * 1024));
    else                      snprintf(out, outlen, "%.2f GB", b / (1024.0 * 1024 * 1024));
}
