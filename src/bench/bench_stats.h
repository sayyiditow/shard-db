/* src/bench/bench_stats.h
 *
 * Allocation-free latency histogram. Captures every sample into a fixed
 * array (caller pre-sizes); on summarize() sorts in place to extract
 * percentiles. Designed for the "do N requests, report mean/p50/p99 +
 * throughput" pattern.
 */
#ifndef BENCH_STATS_H
#define BENCH_STATS_H
#include <stddef.h>
#include <ctype.h>
#include <stdio.h>

/* Safe StringBuilder-style append. Mirrors the daemon-side SB_APPEND in
   src/db/types.h — we don't include types.h from bench files to keep
   the bench TU dep tree tiny. Replaces the unsafe
   `off += (size_t)snprintf(buf + off, cap - off, ...)` idiom that
   CodeQL flags as "potentially overflowing snprintf": snprintf returns
   the would-have-written count, so on truncation `off` advances past
   `cap` and subsequent writes hit OOB memory. */
#define BENCH_SB_APPEND(buf, off, cap, ...) do {                          \
    size_t _rem = (cap) > (off) ? (cap) - (off) : 0;                      \
    if (_rem == 0) break;                                                 \
    int _n = snprintf((buf) + (off), _rem, __VA_ARGS__);                  \
    if (_n < 0) { (off) = (cap) - 1; break; }                             \
    (off) += ((size_t)_n < _rem) ? (size_t)_n : (_rem - 1);               \
} while (0)

/* True iff `s` contains only characters safe to embed in a shell command
   without quoting (alnum, '/', '.', '_', '-'). NULL or empty returns 0.
   Kept for legacy paths that still want a sanity gate even though
   bench_safe_exec below is the preferred route for shell-free spawn. */
static inline int bench_path_safe(const char *s) {
    if (!s || !*s) return 0;
    for (const char *p = s; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (!(isalnum(c) || c == '/' || c == '.' || c == '_' || c == '-'))
            return 0;
    }
    return 1;
}

/* fork+execvp wrapper: runs `argv` directly via exec, no shell — so
   the per-arg bytes can never be interpreted as shell metacharacters.
   This is what CodeQL recognises as the safe replacement for
   system("cmd " + user_input) — the validator-then-system pattern
   keeps the dataflow from input → system() that CodeQL flags as
   "uncontrolled data in OS command".
   argv must be NULL-terminated. Returns 0 on exit-zero, the child's
   non-zero exit status, or -1 if fork/exec failed. */
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>
static inline int bench_safe_exec(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        execvp(argv[0], argv);
        _exit(127);
    }
    int status;
    pid_t r;
    do { r = waitpid(pid, &status, 0); } while (r < 0 && errno == EINTR);
    if (r < 0) return -1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}
#include <stdint.h>
#include <stdlib.h>

typedef struct {
    uint64_t *samples_ns;   /* caller-owned buffer of length cap */
    size_t    cap;
    size_t    count;
} BenchHist;

uint64_t bench_now_ns(void);  /* CLOCK_MONOTONIC */

static inline void bench_hist_init(BenchHist *h, uint64_t *buf, size_t cap) {
    h->samples_ns = buf; h->cap = cap; h->count = 0;
}

static inline void bench_hist_add(BenchHist *h, uint64_t ns) {
    if (h->count < h->cap) h->samples_ns[h->count++] = ns;
}

/* Sort in place, then print:
   "<label>: N=10000 mean=Xµs p50=Yµs p99=Zµs throughput=Q.QQ k ops/sec"
   If total_wall_ns is 0, throughput is computed from sum-of-samples.
*/
void bench_hist_report(BenchHist *h, const char *label, uint64_t total_wall_ns);

/* Sort samples and return the median latency in nanoseconds. Use when you
   want the p50 separately (e.g., to feed into a bench_table_record extra
   column) without printing a histogram line. */
uint64_t bench_hist_p50_ns(BenchHist *h);

#endif
