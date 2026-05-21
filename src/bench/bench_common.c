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
