/* src/bench/bench_common.h
 *
 * Shared bench helpers — utilities used across multiple bench files.
 * Keeps individual bench files focused on schema/workload rather than
 * boilerplate.
 */
#ifndef BENCH_COMMON_H
#define BENCH_COMMON_H

#include <stddef.h>

/* Create an anonymous in-memory file containing `data` (`size` bytes),
   ready for read-from-start. On Linux uses memfd_create via syscall (319
   on x86_64, 279 on aarch64); on other platforms falls back to mkstemp
   + immediate unlink. Returns the fd on success, -1 on failure (with
   perror'd context). Used by bulk-insert benches that prefer to skip
   the userspace ringbuf copy and let the kernel pipe data into the
   server via splice/sendfile. */
int bench_make_memfd(const char *name, const char *data, size_t size);

#endif
