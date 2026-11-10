/* Bitmap handle-safety and complete-read-result regression cases.
 *
 * Thirteen independent cases:
 *   test-bitmap-open-saturation        — a saturated cap16 cache must never
 *                                        hand back an uncached handle: the
 *                                        17th reader open fails NULL/EBUSY,
 *                                        release-then-retry succeeds cached,
 *                                        and the no-cache fixtures keep
 *                                        their reader fallback / writer
 *                                        ENODEV contracts.
 *   test-bitmap-open-stale-publication — a held old-inode reader plus a
 *                                        published replacement: reacquisition
 *                                        must not return an uncached new
 *                                        inode; after the hold drops, a
 *                                        cached new-inode handle is served;
 *                                        a DROP_TRY fault injected into the
 *                                        stale retirement surfaces its exact
 *                                        errno and is consumed once.
 *   test-bitmap-damaged-shard-reads    — daemon fixture with enum bitmap
 *                                        fields: seed, delete, stop, truncate
 *                                        one target bitmap, restart without a
 *                                        bmfix marker. Vacuum skips exactly
 *                                        that shard and keeps its marker;
 *                                        damaged-field reads are errors or
 *                                        exact whole-oracle results, never
 *                                        silent partial sets; healthy-field
 *                                        and metadata reads stay exact; a
 *                                        restart with the marker repairs and
 *                                        removes it.
 *   test-bitmap-corrupt-cache-retirement — in-process: a warm cached mapping
 *                                        whose header is damaged in place is
 *                                        skipped by a keyfile rebuild only
 *                                        together with retirement of its
 *                                        cache entry; retirement failure
 *                                        refuses the vacuum with the keyfile
 *                                        state unchanged, and a stale cached
 *                                        success never survives the skip.
 *   test-bitmap-read-error-propagation — table-driven consumer matrix (eq,
 *                                        IN, generic, AND, OR, complement
 *                                        prefilter, ordered prefilter,
 *                                        group-by popcount, slot intersect,
 *                                        HBM emit) over a cold-damaged
 *                                        bitmap shard plus ENOENT, absent
 *                                        value and empty-bitmap controls.
 *   test-bitmap-find-error-framing     — a damaged later bitmap shard with
 *                                        earlier matching candidates: every
 *                                        find shape returns either one
 *                                        complete error object before any
 *                                        envelope byte or exact full-oracle
 *                                        results, and the connection stays
 *                                        usable for a healthy follow-up.
 *   test-bitmap-prepared-small-limit   — small-limit finds over many large
 *                                        matching candidates return exact
 *                                        results with a small query buffer,
 *                                        including rejected candidates
 *                                        before the survivors.
 *   test-bitmap-keyset-capacity-race   — a synchronized barrier between the
 *                                        bitmap KeySet sizing and enumeration
 *                                        passes lets a concurrent writer
 *                                        overflow the allocated capacity:
 *                                        eq, generic and complement
 *                                        collectors must surface the
 *                                        insertion failure instead of
 *                                        silently truncating.
 *   test-bitmap-candidate-budget       — a match count whose approximate
 *                                        hint fits the query buffer but
 *                                        whose actual rounded KeySet arrays
 *                                        exceed it must be rejected with the
 *                                        existing buffer error instead of
 *                                        silently admitted; a nearby truly
 *                                        fitting case still succeeds.
 *   test-bitmap-candidate-floor        — bitmap candidate collections admit
 *                                        the 8,192-slot preferred floor
 *                                        (sizing boundary table, floor
 *                                        admission under a barrier burst,
 *                                        frozen-write oracles) and overflow
 *                                        past the observed capacity still
 *                                        recovers exactly once through the
 *                                        full-record criteria; budget-refusal,
 *                                        allocation-failure, hard-error and
 *                                        expired-deadline outcomes stay
 *                                        separate from that recovery.
 *   test-bitmap-capacity-fallback-shapes — forced insertion overflow across
 *                                        seven find/aggregate shapes
 *                                        (page/exclusions/total, join,
 *                                        cursor/order, sorted dict,
 *                                        aggregate, OR, CSV) recovers once
 *                                        to the frozen original-criteria
 *                                        oracle with strict framing.
 *   test-bitmap-or-hard-precedence     — in one OR child worker context a
 *                                        real or_collect_cb insertion
 *                                        overflow and a damaged bitmap's
 *                                        EUCLEAN cold-open both occur; the
 *                                        hard outcome must win with its
 *                                        exact errno, all reservations
 *                                        released, and no capacity recovery.
 *   test-bitmap-agg-topn-deadline      — a streaming top-N aggregate whose
 *                                        prefilter genuinely overflows hands
 *                                        off at the capacity recovery site,
 *                                        where the request's original
 *                                        deadline expires: the caller must
 *                                        answer query_timeout and never
 *                                        start the recovery scan.
 *
 * Every potentially-blocking child runs in its own process group under a
 * monotonic watchdog with a lifeline guard, exact-PID kill/reap and progress
 * stages; daemon fixtures run under a 150 s work budget + 30 s cleanup
 * reserve with the fixtures' opt-in startup deadline. */
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "test_client.h"
#include "types.h"
#include "bitmap.h"
#include "query_internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* ───────────────────────── child runner + stages ───────────────────────── */

#define BMF_UNIT_CHILD_TIMEOUT_MS 30000
#define BMF_POLL_SLICE_MS         50
#define BMF_WORK_BUDGET_MS        150000
#define BMF_CLEANUP_RESERVE_MS    30000

static __thread int bmf_stage_fd = -1;

static void bmf_stage(const char *msg) {
    if (bmf_stage_fd >= 0) dprintf(bmf_stage_fd, "STAGE %s\n", msg);
}

static ShardDb *bmf_child_db;

typedef int (*BmfChildFn)(void);

typedef struct {
    int  ran;
    int  exit_code;
    int  timed_out;
    int  termsig;
    char stages[4096];
} BmfChildResult;

static int64_t bmf_monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int64_t bmf_budget_anchor;
static int64_t bmf_budget_total_ms = BMF_WORK_BUDGET_MS;

static int64_t bmf_work_remaining_ms(void) {
    if (bmf_budget_anchor == 0) return BMF_UNIT_CHILD_TIMEOUT_MS;
    int64_t left = bmf_budget_total_ms - (bmf_monotonic_ms() - bmf_budget_anchor);
    return left > 0 ? left : 0;
}

static int bmf_budget_expired(void) {
    return bmf_budget_anchor != 0 && bmf_work_remaining_ms() == 0;
}

static int bmf_child_bound_ms(void) {
    int64_t left = bmf_work_remaining_ms();
    if (left > BMF_UNIT_CHILD_TIMEOUT_MS) return BMF_UNIT_CHILD_TIMEOUT_MS;
    return (int)left;
}

static pid_t bmf_child_group;

static int bmf_run_child_bounded(const char *label, BmfChildFn fn,
                                 BmfChildResult *out, int timeout_ms) {
    memset(out, 0, sizeof(*out));
    int pfd[2];
    if (pipe(pfd) != 0) return -1;
    if (fcntl(pfd[0], F_SETFL, O_NONBLOCK) != 0) {
        close(pfd[0]); close(pfd[1]); return -1;
    }
    int owns_group = bmf_child_group == 0;
    int life[2] = {-1, -1};
    if (owns_group && pipe(life) != 0) { close(pfd[0]); close(pfd[1]); return -1; }
    pid_t pid = fork();
    if (pid < 0) {
        close(pfd[0]); close(pfd[1]);
        if (owns_group) { close(life[0]); close(life[1]); }
        return -1;
    }
    if (pid == 0) {
        close(pfd[0]);
        if (owns_group) {
            close(life[1]);
            if (setpgid(0, 0) != 0) _exit(125);
            bmf_child_group = getpid();
            pid_t guard = fork();
            if (guard < 0) _exit(125);
            if (guard == 0) {
                close(pfd[1]);
                char byte;
                ssize_t n;
                do { n = read(life[0], &byte, 1); } while (n < 0 && errno == EINTR);
                kill(-getpgrp(), SIGKILL);
                _exit(125);
            }
            close(life[0]);
        }
        bmf_stage_fd = pfd[1];
        bmf_child_db = g_db;
        int rc = fn();
        if (rc != 0) bmf_stage("CHILD-FAILED");
        close(bmf_stage_fd);
        _exit(rc);
    }
    close(pfd[1]);
    if (owns_group) close(life[0]);
    int monitor_error = 0;
    if (owns_group && setpgid(pid, pid) != 0 && getpgid(pid) != pid)
        monitor_error = 1;
    int64_t deadline = bmf_monotonic_ms() + timeout_ms;
    char buf[sizeof(out->stages)];
    size_t got = 0;
    int status = 0, done = 0;
    while (!monitor_error) {
        int64_t left = deadline - bmf_monotonic_ms();
        if (left <= 0) break;
        int wait_ms = left < BMF_POLL_SLICE_MS ? (int)left : BMF_POLL_SLICE_MS;
        struct pollfd p = { .fd = pfd[0], .events = POLLIN };
        if (poll(&p, 1, wait_ms) < 0 && errno != EINTR) {
            monitor_error = 1; break;
        }
        while (got < sizeof(buf) - 1) {
            ssize_t n = read(pfd[0], buf + got, sizeof(buf) - 1 - got);
            if (n > 0) { got += (size_t)n; continue; }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
                monitor_error = 1;
            break;
        }
        siginfo_t si;
        memset(&si, 0, sizeof(si));
        if (waitid(P_PID, (id_t)pid, &si, WEXITED | WNOHANG | WNOWAIT) == 0) {
            if (si.si_pid == pid) { done = 1; break; }
        } else if (errno != EINTR) { monitor_error = 1; break; }
    }
    if (!done && !monitor_error) out->timed_out = 1;
    if (owns_group) {
        if (kill(-pid, SIGKILL) != 0 && errno != ESRCH) monitor_error = 1;
        kill(pid, SIGKILL);
    } else if (!done) {
        kill(pid, SIGKILL);
    }
    if (owns_group) close(life[1]);
    pid_t reaped;
    do { reaped = waitpid(pid, &status, 0); } while (reaped < 0 && errno == EINTR);
    if (reaped != pid) monitor_error = 1;
    while (got < sizeof(buf) - 1) {
        ssize_t n = read(pfd[0], buf + got, sizeof(buf) - 1 - got);
        if (n > 0) { got += (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        break;
    }
    close(pfd[0]);
    buf[got] = '\0';
    snprintf(out->stages, sizeof(out->stages), "%s", buf);
    if (monitor_error) {
        TAP_DIAG("# %s: child monitor failed\n", label);
        return -1;
    }
    out->ran = 1;
    out->exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    out->termsig = WIFSIGNALED(status) ? WTERMSIG(status) : 0;
    if (out->timed_out || out->exit_code != 0 || out->termsig)
        TAP_DIAG("# %s: timed_out=%d exit=%d termsig=%d stages:\n# %s\n",
                 label, out->timed_out, out->exit_code, out->termsig, out->stages);
    return 1;
}

static int bmf_run_child(const char *label, BmfChildFn fn, BmfChildResult *out) {
    return bmf_run_child_bounded(label, fn, out, bmf_child_bound_ms());
}

#define BMF_CHECK(cond, code, msg) \
    do { if (!(cond)) { bmf_stage("FAIL " msg); return (code); } } while (0)

static void bmf_sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

static void bmf_child_cache_teardown(void) {
    bm_cache_shutdown();
    bm_test_set_cache_cap(0);
    bm_test_reset_lock_failure();
}

static int bmf_child_cache16(void) {
    bm_cache_shutdown();
    if (bm_test_set_cache_cap(16) != 0) return 90;
    bm_cache_init(4096);
    if (g_bm_cache_slots != 32) return 91;
    bm_test_reset_lock_failure();
    return 0;
}

static void bmf_rmtree_flat(const char *base) {
    DIR *dh = opendir(base);
    if (dh) {
        struct dirent *de;
        while ((de = readdir(dh))) {
            if (de->d_name[0] == '.') continue;
            char fp[PATH_MAX];
            snprintf(fp, sizeof(fp), "%s/%s", base, de->d_name);
            unlink(fp);
        }
        closedir(dh);
    }
    rmdir(base);
}

/* ───────────────────────── strict wire validation ───────────────────────── */

static const char *bmf_strict_json_ws(const char *p) {
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    return p;
}

/* Parse a JSON string at `p` (pointing at the opening quote). Returns the
 * position after the closing quote and a heap-decoded copy, or NULL on
 * malformed/truncated input or allocation failure. \u escapes are rejected
 * (the keys and values this suite reads are plain). */
static const char *bmf_strict_json_string(const char *p, char **out) {
    *out = NULL;
    if (*p != '"') return NULL;
    p++;
    size_t cap = 32, len = 0;
    char *buf = malloc(cap);
    if (!buf) return NULL;
    while (*p != '"') {
        unsigned char c = (unsigned char)*p++;
        if (c < 0x20) { free(buf); return NULL; }
        if (c == '\\') {
            unsigned char e = (unsigned char)*p++;
            switch (e) {
            case '"': case '\\': case '/': c = e; break;
            case 'n': c = '\n'; break;
            case 't': c = '\t'; break;
            case 'r': c = '\r'; break;
            case 'b': c = '\b'; break;
            case 'f': c = '\f'; break;
            case 'u': {
                unsigned int code = 0;
                for (int i = 0; i < 4; i++) {
                    unsigned char h = (unsigned char)*p;
                    unsigned int digit;
                    if (h >= '0' && h <= '9') digit = h - '0';
                    else if (h >= 'a' && h <= 'f') digit = h - 'a' + 10;
                    else if (h >= 'A' && h <= 'F') digit = h - 'A' + 10;
                    else { free(buf); return NULL; }
                    code = code * 16 + digit;
                    p++;
                }
                /* These fixture identities are ASCII; reject unsupported
                   Unicode rather than confuse escaped member names. */
                if (code == 0 || code > 0x7f) { free(buf); return NULL; }
                c = (unsigned char)code;
                break;
            }
            default: free(buf); return NULL;
            }
        }
        if (len + 2 > cap) {
            if (cap > SIZE_MAX / 2) { free(buf); return NULL; }
            size_t next = cap * 2;
            char *nb = realloc(buf, next);
            if (!nb) { free(buf); return NULL; }
            buf = nb; cap = next;
        }
        buf[len++] = (char)c;
    }
    buf[len] = '\0';
    *out = buf;
    return p + 1;
}

/* Skip one well-formed JSON value (object/array/string/number/literal).
 * Returns the position after it, or NULL. Bounded recursion depth. */
typedef struct {
    char *name;
    const char *value;
    const char *end;
} BmfStrictJsonMember;

typedef struct {
    BmfStrictJsonMember *members;
    size_t count;
} BmfStrictJsonObject;

static const char *bmf_strict_json_object(const char *p, int depth, BmfStrictJsonObject *out);

static void bmf_strict_json_object_free(BmfStrictJsonObject *obj) {
    for (size_t i = 0; i < obj->count; i++) free(obj->members[i].name);
    free(obj->members);
    memset(obj, 0, sizeof(*obj));
}

static const char *bmf_strict_json_skip_value(const char *p, int depth) {
    if (depth > 32) return NULL;
    p = bmf_strict_json_ws(p);
    if (*p == '"') {
        char *tmp = NULL;
        const char *r = bmf_strict_json_string(p, &tmp);
        free(tmp);
        return r;
    }
    if (*p == '{') return bmf_strict_json_object(p, depth, NULL);
    if (*p == '[') {
        p = bmf_strict_json_ws(p + 1);
        if (*p == ']') return p + 1;
        for (;;) {
            p = bmf_strict_json_skip_value(p, depth + 1);
            if (!p) return NULL;
            p = bmf_strict_json_ws(p);
            if (*p == ',') { p = bmf_strict_json_ws(p + 1); continue; }
            if (*p == ']') return p + 1;
            return NULL;
        }
    }
    if (strncmp(p, "true", 4) == 0) return p + 4;
    if (strncmp(p, "false", 5) == 0) return p + 5;
    if (strncmp(p, "null", 4) == 0) return p + 4;
    if (*p == '-') p++;
    if (*p == '0') {
        p++;
        if (*p >= '0' && *p <= '9') return NULL;
    } else if (*p >= '1' && *p <= '9') {
        do { p++; } while (*p >= '0' && *p <= '9');
    } else return NULL;
    if (*p == '.') {
        p++;
        if (*p < '0' || *p > '9') return NULL;
        do { p++; } while (*p >= '0' && *p <= '9');
    }
    if (*p == 'e' || *p == 'E') {
        p++;
        if (*p == '+' || *p == '-') p++;
        if (*p < '0' || *p > '9') return NULL;
        do { p++; } while (*p >= '0' && *p <= '9');
    }
    return p;
}

/* Bare nonnegative JSON integer: whitespace, 1+ digits, whitespace, end.
 * Rejects empty/error shapes, signs, overflow and trailing junk. */
static const char *bmf_strict_json_object(const char *p, int depth, BmfStrictJsonObject *out) {
    BmfStrictJsonObject obj = {0};
    if (depth > 32 || *p != '{') return NULL;
    p = bmf_strict_json_ws(p + 1);
    if (*p != '}') for (;;) {
        char *name = NULL;
        const char *q = bmf_strict_json_string(p, &name);
        if (!q) goto fail;
        for (size_t i = 0; i < obj.count; i++)
            if (strcmp(obj.members[i].name, name) == 0) { free(name); goto fail; }
        q = bmf_strict_json_ws(q);
        if (*q != ':') { free(name); goto fail; }
        q = bmf_strict_json_ws(q + 1);
        const char *end = bmf_strict_json_skip_value(q, depth + 1);
        if (!end || obj.count >= SIZE_MAX / sizeof(*obj.members) - 1) {
            free(name); goto fail;
        }
        BmfStrictJsonMember *next = realloc(obj.members, (obj.count + 1) * sizeof(*next));
        if (!next) { free(name); goto fail; }
        obj.members = next;
        obj.members[obj.count++] = (BmfStrictJsonMember){name, q, end};
        p = bmf_strict_json_ws(end);
        if (*p == ',') { p = bmf_strict_json_ws(p + 1); continue; }
        if (*p == '}') break;
        goto fail;
    }
    if (out) *out = obj;
    else bmf_strict_json_object_free(&obj);
    return p + 1;
fail:
    bmf_strict_json_object_free(&obj);
    return NULL;
}

static int bmf_json_ws(const char **p) {
    while (**p == ' ' || **p == '\t' || **p == '\r' || **p == '\n') (*p)++;
    return 0;
}

static int bmf_json_string(const char **p, const char **out, size_t *out_len) {
    if (**p != '"') return -1;
    (*p)++;
    const char *start = *p;
    while (**p && **p != '"') {
        if (**p == '\\' && (*p)[1]) (*p)++;
        (*p)++;
    }
    if (**p != '"') return -1;
    *out = start;
    *out_len = (size_t)(*p - start);
    (*p)++;
    return 0;
}

static int bmf_json_skip_value(const char **p, int depth) {
    const char *end = bmf_strict_json_skip_value(*p, depth);
    if (!end) return -1;
    *p = end;
    return 0;
}

static int bmf_validate_json_complete(const char *s) {
    if (!s) return -1;
    const char *end = bmf_strict_json_skip_value(s, 0);
    return end && *bmf_strict_json_ws(end) == '\0' ? 0 : -1;
}

/* Complete top-level object; duplicate members reject. If the object carries
 * an "error" string member, report it. */
static int bmf_json_object_walk(const char *s, int *is_error,
                                const char **err_val, size_t *err_len) {
    if (!s) return -1;
    BmfStrictJsonObject obj = {0};
    const char *end = bmf_strict_json_object(bmf_strict_json_ws(s), 0, &obj);
    if (!end || *bmf_strict_json_ws(end)) {
        bmf_strict_json_object_free(&obj);
        return -1;
    }
    int rc = 0;
    if (is_error) *is_error = 0;
    for (size_t i = 0; i < obj.count; i++) {
        if (strcmp(obj.members[i].name, "error") != 0) continue;
        const char *p = obj.members[i].value, *value = NULL;
        size_t len = 0;
        if (obj.count != 1 || bmf_json_string(&p, &value, &len) != 0) {
            rc = -1;
            break;
        }
        if (is_error) *is_error = 1;
        if (err_val) *err_val = value;
        if (err_len) *err_len = len;
    }
    bmf_strict_json_object_free(&obj);
    return rc;
}

static int bmf_is_error_object(const char *resp,
                               const char **ev, size_t *elen) {
    int is_err = 0;
    if (!resp) return 0;
    if (bmf_json_object_walk(resp, &is_err, ev, elen) != 0) return -1;
    return is_err ? 1 : 0;
}

static int bmf_validate_error_exact(const char *resp, const char *text) {
    const char *ev = NULL; size_t elen = 0;
    int r = bmf_is_error_object(resp, &ev, &elen);
    if (r != 1 || !ev) return -1;
    size_t tlen = strlen(text);
    if (elen != tlen || strncmp(ev, text, tlen) != 0) return -1;
    return 0;
}

static int bmf_validate_error_prefix(const char *resp, const char *prefix) {
    const char *ev = NULL; size_t elen = 0;
    int r = bmf_is_error_object(resp, &ev, &elen);
    if (r != 1 || !ev) return -1;
    size_t plen = strlen(prefix);
    if (elen < plen) return -1;
    return strncmp(ev, prefix, plen) == 0 ? 0 : -1;
}

static int bmf_validate_no_error(const char *resp) {
    if (!resp) return -1;
    const char *p = resp;
    bmf_json_ws(&p);
    if (*p == '{') {
        int is_err = 0;
        if (bmf_json_object_walk(resp, &is_err, NULL, NULL) != 0) return -1;
        return is_err ? -1 : 0;
    }
    if (bmf_json_skip_value(&p, 0) != 0) return -1;
    bmf_json_ws(&p);
    return *p == '\0' ? 0 : -1;
}

static int bmf_parse_bare_int(const char *s, long long *out) {
    if (!s || !*s) return -1;
    const char *p = s;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    if (*p == '\0') return -1;
    if (*p == '0' && p[1] >= '0' && p[1] <= '9') return -1;
    long long v = 0;
    int digits = 0;
    while (*p >= '0' && *p <= '9') {
        if (v > (LLONG_MAX - (*p - '0')) / 10) return -1;
        v = v * 10 + (*p - '0');
        digits++;
        p++;
    }
    if (digits == 0) return -1;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    if (*p != '\0') return -1;
    *out = v;
    return 0;
}

static int bmf_parse_find_keys(const char *s, char ***out_keys, int *out_n) {
    *out_keys = NULL; *out_n = 0;
    if (bmf_validate_json_complete(s) != 0) return -1;
    const char *p = s;
    bmf_json_ws(&p);
    if (*p != '[') return -1;
    p++;
    char **keys = NULL;
    int n = 0, cap = 0;
    for (;;) {
        bmf_json_ws(&p);
        if (*p == ']') { p++; break; }
        if (*p == ',') { p++; continue; }
        if (*p != '{') goto fail;
        p++;
        const char *key = NULL; size_t klen = 0;
        int nmembers = 0;
        for (;;) {
            bmf_json_ws(&p);
            if (*p == '}') { p++; break; }
            if (*p == ',') { p++; continue; }
            const char *mk; size_t mlen;
            if (bmf_json_string(&p, &mk, &mlen) != 0) goto fail;
            nmembers++;
            bmf_json_ws(&p);
            if (*p != ':') goto fail;
            p++;
            if (mlen == 3 && strncmp(mk, "key", 3) == 0) {
                if (bmf_json_string(&p, &key, &klen) != 0) goto fail;
            } else {
                if (bmf_json_skip_value(&p, 1) != 0) goto fail;
            }
        }
        if (nmembers < 1 || !key) goto fail;
        for (int i = 0; i < n; i++)
            if (strlen(keys[i]) == klen && strncmp(keys[i], key, klen) == 0)
                goto fail;
        if (n == cap) {
            int ncap = cap ? cap * 2 : 64;
            char **nk = realloc(keys, (size_t)ncap * sizeof(*nk));
            if (!nk) goto fail;
            keys = nk;
            cap = ncap;
        }
        keys[n] = malloc(klen + 1);
        if (!keys[n]) goto fail;
        memcpy(keys[n], key, klen);
        keys[n][klen] = '\0';
        n++;
    }
    bmf_json_ws(&p);
    if (*p != '\0') goto fail;
    *out_keys = keys;
    *out_n = n;
    return 0;
fail:
    for (int i = 0; i < n; i++) free(keys[i]);
    free(keys);
    return -1;
}

static int bmf_key_cmp(const void *a, const void *b) {
    return strcmp(*(char *const *)a, *(char *const *)b);
}

static void bmf_free_keys(char **keys, int n) {
    for (int i = 0; i < n; i++) free(keys[i]);
    free(keys);
}

/* Strict aggregate bucket parse: complete array of objects, each carrying a
 * string member named `group_member` plus integer count members. Duplicate
 * group labels reject. */
static int bmf_parse_group_counts(const char *s, const char *group_member,
                                  char labels[][8], long counts[], int max,
                                  int *out_n) {
    *out_n = 0;
    if (bmf_validate_json_complete(s) != 0) return -1;
    const char *p = s;
    bmf_json_ws(&p);
    if (*p != '[') return -1;
    p++;
    int n = 0;
    size_t gmlen = strlen(group_member);
    for (;;) {
        bmf_json_ws(&p);
        if (*p == ']') { p++; break; }
        if (*p == ',') { p++; continue; }
        if (*p != '{' || n >= max) goto fail;
        p++;
        const char *lab = NULL; size_t lab_len = 0;
        int seen_count = 0;
        for (;;) {
            bmf_json_ws(&p);
            if (*p == '}') { p++; break; }
            if (*p == ',') { p++; continue; }
            const char *mk; size_t mlen;
            if (bmf_json_string(&p, &mk, &mlen) != 0) goto fail;
            bmf_json_ws(&p);
            if (*p != ':') goto fail;
            p++;
            if (mlen == gmlen && strncmp(mk, group_member, gmlen) == 0) {
                if (bmf_json_string(&p, &lab, &lab_len) != 0 ||
                    lab_len == 0 || lab_len >= 8)
                    goto fail;
            } else if (mlen == 5 && strncmp(mk, "count", 5) == 0) {
                bmf_json_ws(&p);
                const char *vs = p;
                if (bmf_json_skip_value(&p, 1) != 0) goto fail;
                long long v;
                char num[32];
                size_t vl = (size_t)(p - vs);
                if (vl == 0 || vl >= sizeof(num)) goto fail;
                memcpy(num, vs, vl);
                num[vl] = '\0';
                if (bmf_parse_bare_int(num, &v) != 0) goto fail;
                counts[n] = (long)v;
                seen_count = 1;
            } else {
                if (bmf_json_skip_value(&p, 1) != 0) goto fail;
            }
        }
        if (!lab || !seen_count) goto fail;
        for (int i = 0; i < n; i++)
            if (strlen(labels[i]) == lab_len &&
                memcmp(labels[i], lab, lab_len) == 0) goto fail;
        memcpy(labels[n], lab, lab_len < 8 ? lab_len : 7);
        labels[n][lab_len < 8 ? lab_len : 7] = '\0';
        n++;
    }
    bmf_json_ws(&p);
    if (*p != '\0') goto fail;
    *out_n = n;
    return 0;
fail:
    *out_n = 0;
    return -1;
}

/* ───────────────────────── seeding oracle ───────────────────────── */

#define BMF_KEY_MAX 32
#define BMF_ERR_QUERY_BUFFER "query memory buffer exceeded"

typedef struct {
    int  seeded;
    int  deleted_n;
    char obj_dir[128];    /* "<dir>/<object>" */
    char root[256];
} BmfOracle;

static void bmf_key_for(char *out, size_t cap, long i) {
    snprintf(out, cap, "k%06ld", i);
}

static int bmf_f0_for(long i) {
    return (i % 5 == 0) ? (int)(1 + (i / 5) % 3) : 0;
}

static int bmf_f1_for(long i) {
    return (int)((i / 3) % 4);
}

static int bmf_is_deleted(long i) {
    return (i % 10 == 0) ? 1 : 0;
}

static long bmf_oracle_count_field(const BmfOracle *o, int field, int value) {
    long n = 0;
    for (long i = 1; i <= o->seeded; i++) {
        if (bmf_is_deleted(i)) continue;
        int v = field == 0 ? bmf_f0_for(i) : bmf_f1_for(i);
        if (v == value) n++;
    }
    return n;
}

static long bmf_oracle_count(const BmfOracle *o) {
    long n = 0;
    for (long i = 1; i <= o->seeded; i++)
        if (!bmf_is_deleted(i)) n++;
    return n;
}

static void bmf_oracle_init(BmfOracle *o, const char *root,
                            const char *dir, const char *object,
                            int seeded) {
    memset(o, 0, sizeof(*o));
    snprintf(o->root, sizeof(o->root), "%s", root);
    snprintf(o->obj_dir, sizeof(o->obj_dir), "%s/%s", dir, object);
    o->seeded = seeded;
}

/* Split "<dir>/<object>" into the request's dir/object parts. */
static void bmf_split_obj(const char *obj_dir, char *dir_part, size_t dcap,
                          char *obj_part, size_t ocap) {
    const char *slash = strchr(obj_dir, '/');
    size_t dlen = slash ? (size_t)(slash - obj_dir) : strlen(obj_dir);
    if (dlen >= dcap) dlen = dcap - 1;
    memcpy(dir_part, obj_dir, dlen);
    dir_part[dlen] = '\0';
    snprintf(obj_part, ocap, "%s", slash ? slash + 1 : obj_dir);
}

/* Build a bulk-insert request for records [from..to]. chunk records per
 * request; each record's pay is padded with pay_pad filler bytes. Returns 0
 * when done, 1 when a request was filled (call again), -1 on overflow. */
static int bmf_build_bulk_insert(const BmfOracle *o, long *cursor, long to,
                                 int chunk, int pay_pad,
                                 char *req, size_t cap) {
    if (*cursor > to) return 0;
    char dir_part[64], obj_part[64];
    bmf_split_obj(o->obj_dir, dir_part, sizeof(dir_part),
                  obj_part, sizeof(obj_part));
    size_t off = (size_t)snprintf(req, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"records\":{", dir_part, obj_part);
    long end = *cursor + chunk - 1 > to ? to : *cursor + chunk - 1;
    for (long j = *cursor; j <= end; j++) {
        char key[BMF_KEY_MAX];
        bmf_key_for(key, sizeof(key), j);
        char pay[128];
        int pn = snprintf(pay, sizeof(pay), "p%ld", j);
        int n = snprintf(req + off, cap - off,
            "%s\"%s\":{\"id\":\"%ld\",\"f0\":\"%d\",\"f1\":\"%d\",\"pay\":\"",
            j > *cursor ? "," : "", key, j, bmf_f0_for(j), bmf_f1_for(j));
        if (n < 0 || (size_t)n >= cap - off) return -1;
        off += (size_t)n;
        size_t tail_len = (size_t)pn + (size_t)(pay_pad > 0 ? pay_pad : 0) + 4;
        if (tail_len >= cap - off) return -1;
        memcpy(req + off, pay, (size_t)pn);
        off += (size_t)pn;
        if (pay_pad > 0) {
            memset(req + off, 'x', (size_t)pay_pad);
            off += (size_t)pay_pad;
        }
        memcpy(req + off, "\"}", 2);
        off += 2;
    }
    int n = snprintf(req + off, cap - off, "}}");
    if (n < 0 || (size_t)n >= cap - off) return -1;
    *cursor = end + 1;
    return 1;
}

static int bmf_build_bulk_delete_tenth(const BmfOracle *o, long *cursor,
                                       long to, char *req, size_t cap) {
    if (*cursor > to) return 0;
    char dir_part[64], obj_part[64];
    bmf_split_obj(o->obj_dir, dir_part, sizeof(dir_part),
                  obj_part, sizeof(obj_part));
    size_t off = (size_t)snprintf(req, cap,
        "{\"mode\":\"bulk-delete\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"keys\":[", dir_part, obj_part);
    int n_keys = 0;
    long j = *cursor;
    for (; j <= to && n_keys < 200; j++) {
        if (!bmf_is_deleted(j)) continue;
        char key[BMF_KEY_MAX];
        bmf_key_for(key, sizeof(key), j);
        int n = snprintf(req + off, cap - off, "%s\"%s\"",
                         n_keys ? "," : "", key);
        if (n < 0 || (size_t)n >= cap - off) return -1;
        off += (size_t)n;
        n_keys++;
    }
    int n = snprintf(req + off, cap - off, "]}");
    if (n < 0 || (size_t)n >= cap - off) return -1;
    *cursor = j;
    return n_keys > 0 ? 1 : 2;
}

/* ───────────────────────── daemon fixture ───────────────────────── */

typedef struct {
    TestEnv    env;
    TestClient *tc;
    BmfOracle  o;
    int64_t    deadline_ms;
} BmfDaemon;

static int bmf_daemon_start(BmfDaemon *d, const char *qbuf_mb) {
    memset(d, 0, sizeof(*d));
    bmf_budget_anchor = bmf_monotonic_ms();
    bmf_budget_total_ms = BMF_WORK_BUDGET_MS + BMF_CLEANUP_RESERVE_MS;
    int64_t prev = test_fixture_set_start_deadline_ms(
        bmf_budget_anchor + BMF_WORK_BUDGET_MS);
    int rc = qbuf_mb ? test_env_start_ex(&d->env, qbuf_mb) : test_env_start(&d->env);
    test_fixture_set_start_deadline_ms(prev);
    if (rc != 0) return 1;
    TestClientCfg cfg = { .port = d->env.port };
    d->tc = tc_connect(&cfg);
    if (!d->tc) return 2;
    d->deadline_ms = bmf_monotonic_ms() + bmf_work_remaining_ms()
                     - BMF_CLEANUP_RESERVE_MS;
    return 0;
}

static void bmf_daemon_stop(BmfDaemon *d) {
    if (d->tc) tc_close(d->tc);
    d->tc = NULL;
    test_env_stop(&d->env);
    bmf_budget_anchor = 0;
    bmf_budget_total_ms = BMF_WORK_BUDGET_MS;
}

static int bmf_req(BmfDaemon *d, const char *json, char **out_resp) {
    *out_resp = NULL;
    if (!d->tc) return -1;
    if (bmf_monotonic_ms() > d->deadline_ms) return -2;
    return tc_request(d->tc, json, out_resp);
}

/* Strict create-object with the standard D fixture schema. */
static int bmf_create_object(BmfDaemon *d, const char *dir, const char *object,
                             int splits, int pay_size) {
    char req[1024];
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"%s\",\"object\":\"%s\","
             "\"splits\":%d,\"max_key\":64,"
             "\"fields\":[\"id:int\",\"f0:enum(0,1,2,3)\","
             "\"f1:enum(0,1,2,3)\",\"f2:enum(0,1,2,3)\","
             "\"pay:varchar:%d\"],"
             "\"indexes\":[\"id\",\"f0:bitmap\",\"f1:bitmap\",\"f2:bitmap\"]}",
             dir, object, splits, pay_size);
    char *resp = NULL;
    int rc = bmf_req(d, req, &resp);
    if (rc == 0) rc = bmf_validate_no_error(resp);
    free(resp);
    return rc;
}

/* Seed [from..to]; strict complete responses per chunk. pay_pad pads each
 * record's pay to force large payloads (0 = compact). */
static int bmf_seed_range(BmfDaemon *d, const BmfOracle *o, long from, long to,
                          int chunk, int pay_pad) {
    size_t req_cap = (size_t)chunk * (size_t)(220 + (pay_pad > 0 ? pay_pad : 0))
                     + 8192;
    char *req = malloc(req_cap);
    if (!req) return -1;
    long cursor = from;
    int rc = 0;
    while (cursor <= to) {
        if (bmf_budget_expired()) { rc = -3; break; }
        int built = bmf_build_bulk_insert(o, &cursor, to, chunk, pay_pad,
                                          req, req_cap);
        if (built <= 0) {
            TAP_DIAG("SEED-BUILD-FAIL built=%d chunk_at=%ld pad=%d\n",
                     built, cursor, pay_pad);
            rc = built < 0 ? -1 : 0;
            break;
        }
        char *resp = NULL;
        int rrc = bmf_req(d, req, &resp);
        if (rrc != 0) {
            TAP_DIAG("SEED-REQ-FAIL rc=%d chunk_at=%ld reqlen=%zu\n",
                     rrc, cursor, strlen(req));
            rc = -1;
            free(resp);
            break;
        }
        rrc = bmf_validate_no_error(resp);
        if (rrc != 0)
            TAP_DIAG("SEED-REJECT chunk at %ld: %.200s\n", cursor, resp);
        free(resp);
        if (rrc != 0) { rc = -1; break; }
    }
    free(req);
    return rc;
}

static int bmf_delete_tenth(BmfDaemon *d, const BmfOracle *o, long from,
                            long to) {
    char req[1 << 16];
    long cursor = from;
    while (cursor <= to) {
        if (bmf_budget_expired()) return -3;
        int built = bmf_build_bulk_delete_tenth(o, &cursor, to, req, sizeof(req));
        if (built <= 0) return built < 0 ? -1 : 0;
        char *resp = NULL;
        int rc = bmf_req(d, req, &resp);
        if (rc != 0) { free(resp); return -1; }
        rc = bmf_validate_no_error(resp);
        free(resp);
        if (rc != 0) return -1;
    }
    return 0;
}

static int bmf_daemon_setup(BmfDaemon *d, const char *qbuf_mb,
                            const char *dir, const char *object,
                            int seeded, int pay_size, int chunk) {
    int rc = bmf_daemon_start(d, qbuf_mb);
    if (rc != 0) return rc;
    bmf_oracle_init(&d->o, d->env.db_root, dir, object, seeded);
    if (bmf_create_object(d, dir, object, 8, pay_size) != 0) {
        char req2[1024];
        snprintf(req2, sizeof(req2),
                 "{\"mode\":\"create-object\",\"dir\":\"%s\",\"object\":\"%s\"}",
                 dir, object);
        char *resp2 = NULL;
        int rrc2 = bmf_req(d, req2, &resp2);
        TAP_DIAG("CREATE-FAILED probe rc=%d resp=%.120s\n", rrc2,
                 resp2 ? resp2 : "(null)");
        free(resp2);
        return 3;
    }
    {
        int pad = pay_size > 96 ? pay_size - 96 : 0;
        if (bmf_seed_range(d, &d->o, 1, seeded, chunk, pad) != 0) return 4;
    }
    if (bmf_delete_tenth(d, &d->o, 1, seeded) != 0) return 5;
    return 0;
}

/* Per-shard keyfile header oracle. */
typedef struct {
    int      shard;
    uint64_t total;
    uint64_t deleted;
    int      valid;
} BmfKfShard;

static int bmf_kf_shards(const BmfOracle *o, BmfKfShard *out, int max) {
    char kf_dir[512];
    snprintf(kf_dir, sizeof(kf_dir), "%s/%s/data/kf", o->root, o->obj_dir);
    DIR *d = opendir(kf_dir);
    if (!d) return -1;
    struct dirent *de;
    int n = 0;
    while ((de = readdir(d)) && n < max) {
        size_t nl = strlen(de->d_name);
        if (nl < 4 || strcmp(de->d_name + nl - 3, ".kf") != 0) continue;
        char path[600];
        snprintf(path, sizeof(path), "%s/%s", kf_dir, de->d_name);
        int fd = open(path, O_RDONLY);
        if (fd < 0) continue;
        uint32_t hdr[6];
        if (pread(fd, hdr, sizeof(hdr), 0) == (ssize_t)sizeof(hdr) &&
            hdr[0] == 0x31464B53u) {
            out[n].shard = (int)strtol(de->d_name, NULL, 10);
            memcpy(&out[n].total, &hdr[2], sizeof(uint64_t));
            memcpy(&out[n].deleted, &hdr[4], sizeof(uint64_t));
            out[n].valid = 1;
            n++;
        }
        close(fd);
    }
    closedir(d);
    return n;
}

static int bmf_shard_by_live(const BmfOracle *o, int nth, int *out_shard) {
    BmfKfShard shards[MAX_SPLITS];
    int n = bmf_kf_shards(o, shards, MAX_SPLITS);
    if (n <= nth) return -1;
    for (int i = 1; i < n; i++) {
        BmfKfShard k = shards[i];
        int j = i - 1;
        while (j >= 0 && (shards[j].total - shards[j].deleted) <
                         (k.total - k.deleted)) {
            shards[j + 1] = shards[j];
            j--;
        }
        shards[j + 1] = k;
    }
    if ((shards[nth].total - shards[nth].deleted) == 0) return -1;
    *out_shard = shards[nth].shard;
    return 0;
}

static int bmf_shard_highest_index(const BmfOracle *o, int *out_shard) {
    BmfKfShard shards[MAX_SPLITS];
    int n = bmf_kf_shards(o, shards, MAX_SPLITS);
    int best = -1;
    for (int i = 0; i < n; i++) {
        if (!shards[i].valid) continue;
        if ((shards[i].total - shards[i].deleted) == 0) continue;
        if (shards[i].shard > best) best = shards[i].shard;
    }
    if (best < 0) return -1;
    *out_shard = best;
    return 0;
}

/* Diagnostic: dump the tail of a fixture daemon's log after a failed start. */
static void bmf_dump_daemon_log(const TestEnv *env) {
    char base[300];
    snprintf(base, sizeof(base), "%s", env->db_root);
    char *slash = strrchr(base, '/');
    if (slash) *slash = '\0';
    const char *names[2] = { "daemon.log", "logs" };
    (void)names;
    /* Scan every log file under <base>/logs (plus a bare daemon.log) for
     * BMKS needles; dump the last 24 matching lines. */
    char dirs[2][300];
    snprintf(dirs[0], sizeof(dirs[0]), "%s/logs", base);
    snprintf(dirs[1], sizeof(dirs[1]), "%s", base);
    char ring[24][512];
    int n = 0;
    for (int di = 0; di < 2; di++) {
        DIR *ld = opendir(dirs[di]);
        if (!ld) continue;
        struct dirent *de;
        while ((de = readdir(ld))) {
            if (de->d_name[0] == '.') continue;
            if (!strstr(de->d_name, ".log")) continue;
            char fp[500];
            snprintf(fp, sizeof(fp), "%s/%s", dirs[di], de->d_name);
            FILE *f = fopen(fp, "r");
            if (!f) continue;
            char line[512];
            while (fgets(line, sizeof(line), f))
                if (strstr(line, "BMKS") || strstr(line, "candidate capacity"))
                    snprintf(ring[n++ % 24], sizeof(ring[0]), "%s", line);
            fclose(f);
        }
        closedir(ld);
    }
    if (n == 0) { TAP_DIAG("# (no BMKS log lines under %s)\n", base); return; }
    int start = n > 24 ? n - 24 : 0;
    for (int i = start; i < n; i++) TAP_DIAG("# log: %s", ring[i % 24]);
}

static int bmf_file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static void bmf_bm_path(char *out, size_t cap, const BmfOracle *o,
                        const char *field, int shard) {
    snprintf(out, cap, "%s/%s/indexes/%s/%03x.bm",
             o->root, o->obj_dir, field, shard);
}

static void bmf_marker_path(char *out, size_t cap, const BmfOracle *o,
                            int shard) {
    snprintf(out, cap, "%s/%s/indexes/.bmfix-%03x",
             o->root, o->obj_dir, shard);
}

static void bmf_vacuum_req(const BmfOracle *o, char *req, size_t cap) {
    char dir_part[64], obj_part[64];
    bmf_split_obj(o->obj_dir, dir_part, sizeof(dir_part),
                  obj_part, sizeof(obj_part));
    snprintf(req, cap, "{\"mode\":\"vacuum\",\"dir\":\"%s\",\"object\":\"%s\"}",
             dir_part, obj_part);
}

/* ───────────────── response-shape policy checks ───────────────── */

#define BMF_WANT_EXACT          0
#define BMF_WANT_ERROR          1
#define BMF_WANT_EXACT_OR_ERROR 2

/* COUNT: response must be a complete exact-error object or the exact bare
 * oracle count, per policy. */
static int bmf_check_count(BmfDaemon *d, const BmfOracle *o,
                           const char *criteria, long oracle, int policy,
                           const char *label) {
    char req[1024];
    char dir_part[64], obj_part[64];
    bmf_split_obj(o->obj_dir, dir_part, sizeof(dir_part),
                  obj_part, sizeof(obj_part));
    if (criteria && criteria[0])
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\","
                 "\"criteria\":%s}", dir_part, obj_part, criteria);
    else
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\"}",
                 dir_part, obj_part);
    char *resp = NULL;
    int rc = bmf_req(d, req, &resp);
    if (rc != 0) {
        TAP_DIAG("# %s: count request failed rc=%d\n", label, rc);
        free(resp);
        return 1;
    }
    long long got = -1;
    int is_err = bmf_is_error_object(resp, NULL, NULL);
    if (is_err != 1 && bmf_parse_bare_int(resp, &got) != 0) {
        TAP_DIAG("# %s: malformed count response (%.80s)\n", label, resp);
        free(resp);
        return 1;
    }
    if (is_err == 1) {
        free(resp);
        if (policy == BMF_WANT_EXACT) {
            TAP_DIAG("# %s: expected exact count, got an error\n", label);
            return 1;
        }
        return 0;
    }
    free(resp);
    if (policy == BMF_WANT_ERROR) {
        TAP_DIAG("# %s: expected an error, got count %lld\n", label, got);
        return 1;
    }
    if ((long long)oracle != got) {
        TAP_DIAG("# %s: count %lld != oracle %ld\n", label, got, oracle);
        return 1;
    }
    return 0;
}

/* FIND membership: every live matching key, in seeding order, exactly. */
static long bmf_next_matching(const BmfOracle *o, long i, int field, int value) {
    while (i <= o->seeded) {
        if (!bmf_is_deleted(i) &&
            (field == 0 ? bmf_f0_for(i) : bmf_f1_for(i)) == value)
            return i;
        i++;
    }
    return 0;
}

static int bmf_check_find(BmfDaemon *d, const BmfOracle *o,
                          const char *criteria, int field, int value,
                          int limit, int policy, const char *label) {
    char req[1024];
    char dir_part[64], obj_part[64];
    bmf_split_obj(o->obj_dir, dir_part, sizeof(dir_part),
                  obj_part, sizeof(obj_part));
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\","
             "\"criteria\":%s,\"limit\":%d}",
             dir_part, obj_part, criteria, limit);
    char *resp = NULL;
    int rc = bmf_req(d, req, &resp);
    if (rc != 0) {
        TAP_DIAG("# %s: find request failed rc=%d\n", label, rc);
        free(resp);
        return 1;
    }
    int is_err = bmf_is_error_object(resp, NULL, NULL);
    char **keys = NULL; int n = 0;
    if (is_err != 1 && bmf_parse_find_keys(resp, &keys, &n) != 0) {
        TAP_DIAG("# %s: malformed find response (%.80s)\n", label, resp);
        free(resp);
        return 1;
    }
    if (is_err == 1) {
        free(resp);
        if (policy == BMF_WANT_EXACT) {
            TAP_DIAG("# %s: expected exact rows, got an error\n", label);
            return 1;
        }
        return 0;
    }
    free(resp);
    long expect = bmf_oracle_count_field(o, field, value);
    int mismatch = (n != expect);
    /* Unordered find: compare as a set (zero-padded keys sort numerically). */
    if (!mismatch && n > 1) qsort(keys, (size_t)n, sizeof(*keys), bmf_key_cmp);
    if (!mismatch) {
        long i = 1;
        for (int j = 0; j < n; j++) {
            i = bmf_next_matching(o, i, field, value);
            if (i == 0) { mismatch = 1; break; }
            char key[BMF_KEY_MAX];
            bmf_key_for(key, sizeof(key), i);
            if (strcmp(keys[j], key) != 0) { mismatch = 1; break; }
            i++;
        }
    }
    bmf_free_keys(keys, n);
    if (mismatch) {
        TAP_DIAG("# %s: find membership is not the exact oracle set\n", label);
        return 1;
    }
    return 0;
}

/* Aggregate group counts: complete bucket array matching the oracle exactly,
 * or a complete error object per policy. */
static int bmf_check_group_counts(BmfDaemon *d, const BmfOracle *o,
                                  const char *req_tail, const char *group_field,
                                  long counts[4], int policy,
                                  const char *label) {
    char req[1280];
    char dir_part[64], obj_part[64];
    bmf_split_obj(o->obj_dir, dir_part, sizeof(dir_part),
                  obj_part, sizeof(obj_part));
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"%s\",\"object\":\"%s\",%s}",
             dir_part, obj_part, req_tail);
    char *resp = NULL;
    int rc = bmf_req(d, req, &resp);
    if (rc != 0) {
        TAP_DIAG("# %s: aggregate request failed rc=%d\n", label, rc);
        free(resp);
        return 1;
    }
    int is_err = bmf_is_error_object(resp, NULL, NULL);
    char labels[8][8];
    long got[8];
    int n = 0;
    if (is_err != 1 &&
        bmf_parse_group_counts(resp, group_field, labels, got, 8, &n) != 0) {
        TAP_DIAG("# %s: malformed aggregate response (%.80s)\n", label, resp);
        free(resp);
        return 1;
    }
    if (is_err == 1) {
        free(resp);
        if (policy == BMF_WANT_EXACT) {
            TAP_DIAG("# %s: expected exact buckets, got an error\n", label);
            return 1;
        }
        return 0;
    }
    free(resp);
    int mismatch = (n != 4);
    for (int v = 0; v < 4 && !mismatch; v++) {
        char want[8];
        snprintf(want, sizeof(want), "%d", v);
        int found = 0;
        for (int b = 0; b < n; b++) {
            if (strcmp(labels[b], want) == 0) {
                if (got[b] != counts[v]) mismatch = 1;
                found = 1;
                break;
            }
        }
        if (!found) mismatch = 1;
    }
    if (mismatch) {
        TAP_DIAG("# %s: group buckets are not the exact oracle\n", label);
        return 1;
    }
    return 0;
}

static void bmf_oracle_group_counts(const BmfOracle *o, int field,
                                    long counts[4]) {
    for (int v = 0; v < 4; v++) counts[v] = bmf_oracle_count_field(o, field, v);
}

/* ═══════════════════ Case 1: open saturation ═══════════════════ */

#define BMF_SAT_FILES 17

static int bmf_sat_prepare_files(const char *base) {
    char path[PATH_MAX];
    for (int i = 0; i < BMF_SAT_FILES; i++) {
        snprintf(path, sizeof(path), "%s/sat%02d.bm", base, i);
        BitmapShard *bm = bm_open(path, 128, 1, 0, 0, 0);
        if (!bm) return 10;
        if (bm_set(bm, (const uint8_t *)"v", 1, 3) != 0) { bm_close(bm); return 11; }
        bm_close(bm);
    }
    return 0;
}

static int bmf_saturation_child(void) {
    char tmpl[128];
    snprintf(tmpl, sizeof(tmpl), "/tmp/shard-db-bmf-sat-%d-XXXXXX", getpid());
    char *base = mkdtemp(tmpl);
    BMF_CHECK(base != NULL, 1, "mkdtemp");
    int rc = bmf_child_cache16();
    BMF_CHECK(rc == 0, 2, "cap16 cache init");
    BMF_CHECK(bmf_sat_prepare_files(base) == 0, 3, "file creation");

    BitmapShard *held[16];
    for (int i = 0; i < 16; i++) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/sat%02d.bm", base, i);
        held[i] = bm_open(path, 0, 0, 0, 0, 0);
        BMF_CHECK(held[i] != NULL, 4, "16 reader opens must succeed");
        BMF_CHECK(bm_handle_cached(held[i]) == 1, 5, "held handles must be cached");
    }
    bmf_stage("seeded16-held");

    char path17[PATH_MAX];
    snprintf(path17, sizeof(path17), "%s/sat16.bm", base);
    errno = 0;
    BitmapShard *bm17 = bm_open(path17, 0, 0, 0, 0, 0);
    if (bm17 != NULL) {
        int cached = bm_handle_cached(bm17);
        bm_close(bm17);
        BMF_CHECK(cached == 1, 6, "17th open must not be uncached");
        BMF_CHECK(0, 7, "17th open must fail under a saturated cache");
    }
    BMF_CHECK(errno == EBUSY, 8, "saturation failure must report EBUSY");
    bmf_stage("17th-rejected");

    bm_close(held[0]);
    held[0] = NULL;
    bm17 = bm_open(path17, 0, 0, 0, 0, 0);
    BMF_CHECK(bm17 != NULL, 9, "retry after release must succeed");
    BMF_CHECK(bm_handle_cached(bm17) == 1, 10, "post-release retry must be cached");
    BMF_CHECK(bm_test(bm17, (const uint8_t *)"v", 1, 3) == 1, 11,
              "retry handle must read the file's data");
    bm_close(bm17);
    bmf_stage("retry-cached-ok");

    for (int i = 1; i < 16; i++)
        if (held[i]) bm_close(held[i]);
    bmf_child_cache_teardown();
    bmf_rmtree_flat(base);
    return 0;
}

static int bmf_nocache_reader_child(void) {
    char tmpl[128];
    snprintf(tmpl, sizeof(tmpl), "/tmp/shard-db-bmf-nocr-%d-XXXXXX", getpid());
    char *base = mkdtemp(tmpl);
    BMF_CHECK(base != NULL, 1, "mkdtemp");
    bm_cache_shutdown();
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/f.bm", base);
    BitmapShard *wm = bm_open(path, 128, 1, 0, 0, 0);
    BMF_CHECK(wm != NULL, 2, "uncached create must work");
    BMF_CHECK(bm_set(wm, (const uint8_t *)"v", 1, 5) == 0, 3, "uncached set");
    bm_close(wm);
    BMF_CHECK(1, 4, "uncached close");
    BitmapShard *rm = bm_open(path, 0, 0, 0, 0, 0);
    BMF_CHECK(rm != NULL, 5, "no-cache reader fallback must succeed");
    BMF_CHECK(bm_handle_cached(rm) == 0, 6, "fallback handle is uncached");
    BMF_CHECK(bm_test(rm, (const uint8_t *)"v", 1, 5) == 1, 7, "fallback reads data");
    bm_close(rm);
    bmf_child_cache_teardown();
    bmf_rmtree_flat(base);
    return 0;
}

static int bmf_nocache_writer_child(void) {
    char tmpl[128];
    snprintf(tmpl, sizeof(tmpl), "/tmp/shard-db-bmf-nocw-%d-XXXXXX", getpid());
    char *base = mkdtemp(tmpl);
    BMF_CHECK(base != NULL, 1, "mkdtemp");
    bm_cache_shutdown();
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/w.bm", base);
    errno = 0;
    BitmapShard *wm = bm_open(path, 128, 1, 0, 0, 1);
    BMF_CHECK(wm == NULL, 2, "no-cache writer open must fail");
    BMF_CHECK(errno == ENODEV, 3, "no-cache writer open must be ENODEV");
    bmf_child_cache_teardown();
    bmf_rmtree_flat(base);
    return 0;
}

static int bmf_write_raw(const char *path, const void *buf, size_t len) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    ssize_t w = write(fd, buf, len);
    close(fd);
    return w == (ssize_t)len ? 0 : -1;
}

/* Each corrupt-file rejection branch must report deterministic EUCLEAN, and
 * a syscall failure its real errno — never a stale or zero errno. */
static int bmf_open_errno_child(void) {
    char tmpl[128];
    snprintf(tmpl, sizeof(tmpl), "/tmp/shard-db-bmf-errno-%d-XXXXXX", getpid());
    char *base = mkdtemp(tmpl);
    BMF_CHECK(base != NULL, 1, "mkdtemp");
    BMF_CHECK(bmf_child_cache16() == 0, 2, "cap16 cache init");
    char path[PATH_MAX];

    snprintf(path, sizeof(path), "%s/short.bm", base);
    BMF_CHECK(bmf_write_raw(path, "BM01xxxx", 8) == 0, 3, "short write");
    errno = ENOENT;
    BMF_CHECK(bm_open(path, 0, 0, 0, 0, 0) == NULL, 4, "short header must fail");
    BMF_CHECK(errno == EUCLEAN, 5, "short header must report EUCLEAN");

    uint8_t hdr[64];
    memset(hdr, 0, sizeof(hdr));
    memcpy(hdr, "XXXX", 4);
    snprintf(path, sizeof(path), "%s/magic.bm", base);
    BMF_CHECK(bmf_write_raw(path, hdr, sizeof(hdr)) == 0, 6, "magic write");
    errno = 0;
    BMF_CHECK(bm_open(path, 0, 0, 0, 0, 0) == NULL, 7, "bad magic must fail");
    BMF_CHECK(errno == EUCLEAN, 8, "bad magic must report EUCLEAN");

    memset(hdr, 0, sizeof(hdr));
    uint32_t magic = 0x31304D42u, slots = 128, n_values = 1,
             dict_off = 32, bitmaps_off = 32, stride = 999, maxv = 256;
    uint16_t version = 1, flags = 0;
    memcpy(hdr + 0, &magic, 4);
    memcpy(hdr + 4, &version, 2);
    memcpy(hdr + 6, &flags, 2);
    memcpy(hdr + 8, &slots, 4);
    memcpy(hdr + 12, &n_values, 4);
    memcpy(hdr + 16, &dict_off, 4);
    memcpy(hdr + 20, &bitmaps_off, 4);
    memcpy(hdr + 24, &stride, 4);
    memcpy(hdr + 28, &maxv, 4);
    snprintf(path, sizeof(path), "%s/layout.bm", base);
    BMF_CHECK(bmf_write_raw(path, hdr, sizeof(hdr)) == 0, 9, "layout write");
    errno = 0;
    BMF_CHECK(bm_open(path, 0, 0, 0, 0, 0) == NULL, 10, "bad layout must fail");
    BMF_CHECK(errno == EUCLEAN, 11, "bad layout must report EUCLEAN");

    snprintf(path, sizeof(path), "%s/missing-dir/x.bm", base);
    errno = 0;
    BMF_CHECK(bm_open(path, 0, 0, 0, 0, 0) == NULL, 12, "missing file must fail");
    BMF_CHECK(errno == ENOENT, 13, "missing file must report ENOENT");

    bmf_child_cache_teardown();
    bmf_rmtree_flat(base);
    return 0;
}

static int test_bitmap_open_saturation_run(void) {
    BmfChildResult r;
    if (bmf_run_child("saturation", bmf_saturation_child, &r) != 1 ||
        r.ran != 1 || r.exit_code != 0 || r.timed_out || r.termsig) {
        TAP_DIAG("# saturation child: ran=%d exit=%d timed_out=%d termsig=%d\n%s",
                 r.ran, r.exit_code, r.timed_out, r.termsig, r.stages);
        ASSERT_TRUE(0, "0");
        return 1;
    }
    ASSERT_EQ_INT(0, r.exit_code, "child exits 0");
    ASSERT_TRUE(strstr(r.stages, "seeded16-held") != NULL, "strstr(r.stages, 'seeded16-held') != NULL");
    ASSERT_TRUE(strstr(r.stages, "17th-rejected") != NULL, "strstr(r.stages, '17th-rejected') != NULL");
    ASSERT_TRUE(strstr(r.stages, "retry-cached-ok") != NULL, "strstr(r.stages, 'retry-cached-ok') != NULL");

    if (bmf_run_child("nocache-reader", bmf_nocache_reader_child, &r) != 1 ||
        r.exit_code != 0 || r.timed_out) {
        TAP_DIAG("# nocache-reader child: exit=%d timed_out=%d\n%s",
                 r.exit_code, r.timed_out, r.stages);
        ASSERT_TRUE(0, "0");
        return 1;
    }
    ASSERT_EQ_INT(0, r.exit_code, "child exits 0");

    if (bmf_run_child("nocache-writer", bmf_nocache_writer_child, &r) != 1 ||
        r.exit_code != 0 || r.timed_out) {
        TAP_DIAG("# nocache-writer child: exit=%d timed_out=%d\n%s",
                 r.exit_code, r.timed_out, r.stages);
        ASSERT_TRUE(0, "0");
        return 1;
    }
    ASSERT_EQ_INT(0, r.exit_code, "child exits 0");

    if (bmf_run_child("open-errno", bmf_open_errno_child, &r) != 1 ||
        r.exit_code != 0 || r.timed_out) {
        TAP_DIAG("# open-errno child: exit=%d timed_out=%d\n%s",
                 r.exit_code, r.timed_out, r.stages);
        ASSERT_TRUE(0, "open-errno branches report exact errno");
        return 1;
    }
    ASSERT_EQ_INT(0, r.exit_code, "open-errno child exits 0");
    return 0;
}

/* ═══════════════════ Case 2: stale publication ═══════════════════ */

typedef struct {
    const char   *path;
    atomic_int    release;
    atomic_int    holding;
    pthread_t     tid;
} BmfHolder;

static void *bmf_holder_main(void *arg) {
    BmfHolder *h = (BmfHolder *)arg;
    /* g_db is thread-local: a thread created inside the forked child starts
     * with NULL and must adopt the child's db before touching cache globals. */
    g_db = bmf_child_db;
    BitmapShard *bm = bm_open(h->path, 0, 0, 0, 0, 0);
    if (!bm || !bm_handle_cached(bm)) {
        if (bm) bm_close(bm);
        atomic_store(&h->holding, -1);
        return NULL;
    }
    atomic_store(&h->holding, 1);
    while (!atomic_load(&h->release))
        bmf_sleep_ms(2);
    bm_close(bm);
    atomic_store(&h->holding, 0);
    return NULL;
}

static int bmf_holder_start(BmfHolder *h, const char *path) {
    memset(h, 0, sizeof(*h));
    h->path = path;
    atomic_init(&h->release, 0);
    atomic_init(&h->holding, 0);
    if (pthread_create(&h->tid, NULL, bmf_holder_main, h) != 0) return -1;
    for (int i = 0; i < 5000 && atomic_load(&h->holding) == 0; i++)
        bmf_sleep_ms(2);
    return atomic_load(&h->holding) == 1 ? 0 : -1;
}

static void bmf_holder_release(BmfHolder *h) {
    atomic_store(&h->release, 1);
    pthread_join(h->tid, NULL);
}

static int bmf_make_old(const char *target) {
    BitmapShard *wm = bm_open(target, 128, 1, 0, 0, 0);
    if (!wm) return 1;
    if (bm_set(wm, (const uint8_t *)"old", 3, 1) != 0) { bm_close(wm); return 2; }
    bm_close(wm);
    return 0;
}

static int bmf_publish_replacement(const char *base, const char *target,
                                   const char *content) {
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s/stale-tmp.bm", base);
    BitmapShard *wm = bm_open(tmp, 128, 1, 0, 0, 0);
    if (!wm) return 1;
    for (uint32_t s = 0; s < 8; s++)
        if (bm_set(wm, (const uint8_t *)content, strlen(content), s) != 0) {
            bm_close(wm); return 2;
        }
    bm_close(wm);
    if (bm_publish_replace(target, tmp) != BM_PUBLISH_OK) return 4;
    return 0;
}

static int bmf_stale_reader_child(void) {
    char tmpl[128];
    snprintf(tmpl, sizeof(tmpl), "/tmp/shard-db-bmf-stale-%d-XXXXXX", getpid());
    char *base = mkdtemp(tmpl);
    BMF_CHECK(base != NULL, 1, "mkdtemp");
    BMF_CHECK(bmf_child_cache16() == 0, 2, "cap16 cache init");

    char target[PATH_MAX];
    snprintf(target, sizeof(target), "%s/stale.bm", base);
    BMF_CHECK(bmf_make_old(target) == 0, 3, "old content create");

    BmfHolder h;
    BMF_CHECK(bmf_holder_start(&h, target) == 0, 4, "holder must hold");
    bmf_stage("holder-live");

    BMF_CHECK(bmf_publish_replacement(base, target, "new") == 0, 5,
              "publication must succeed");
    bmf_stage("published");

    errno = 0;
    BitmapShard *rm = bm_open(target, 0, 0, 0, 0, 0);
    if (rm != NULL) {
        int cached = bm_handle_cached(rm);
        bm_close(rm);
        BMF_CHECK(cached == 1, 6, "busy stale entry must not yield an uncached handle");
        BMF_CHECK(0, 7, "reacquire must fail while the old entry is busy");
    }
    BMF_CHECK(errno == EBUSY, 8, "stale-busy reacquire must report EBUSY");
    BMF_CHECK(atomic_load(&h.holding) == 1, 9,
              "the hold must still be live after the failed acquire");
    bmf_stage("reacquire-bounded");

    bmf_holder_release(&h);
    rm = bm_open(target, 0, 0, 0, 0, 0);
    BMF_CHECK(rm != NULL, 10, "post-release retry must succeed");
    BMF_CHECK(bm_handle_cached(rm) == 1, 11, "post-release retry must be cached");
    BMF_CHECK(bm_test(rm, (const uint8_t *)"new", 3, 0) == 1, 12,
              "retry must serve the new inode's content");
    if (rm) bm_close(rm);
    bmf_stage("retry-new-inode");

    bmf_child_cache_teardown();
    bmf_rmtree_flat(base);
    return 0;
}

/* Writer that blocks on the stale entry's hit-wr acquisition behind the
 * holder, then validates and closes its own handle in this thread. */
typedef struct {
    const char *path;
    pthread_t   tid;
    atomic_int  done;
    int         got;        /* 1 = handle returned */
    int         cached;
    int         reads_new;
    int         err;
} BmfStaleWriter;

static void *bmf_stale_writer_main(void *arg) {
    BmfStaleWriter *w = (BmfStaleWriter *)arg;
    g_db = bmf_child_db;
    errno = 0;
    BitmapShard *wm = bm_open(w->path, 0, 0, 0, 0, 1);
    w->err = errno;
    if (wm) {
        w->got = 1;
        w->cached = bm_handle_cached(wm);
        w->reads_new = bm_test(wm, (const uint8_t *)"new", 3, 0) == 1;
        bm_close(wm);
    }
    atomic_store(&w->done, 1);
    return NULL;
}

static int bmf_stale_writer_child(void) {
    char tmpl[128];
    snprintf(tmpl, sizeof(tmpl), "/tmp/shard-db-bmf-stalew-%d-XXXXXX", getpid());
    char *base = mkdtemp(tmpl);
    BMF_CHECK(base != NULL, 1, "mkdtemp");
    BMF_CHECK(bmf_child_cache16() == 0, 2, "cap16 cache init");

    char target[PATH_MAX];
    snprintf(target, sizeof(target), "%s/stalew.bm", base);
    BMF_CHECK(bmf_make_old(target) == 0, 3, "old content create");

    BmfHolder h;
    BMF_CHECK(bmf_holder_start(&h, target) == 0, 4, "holder must hold");

    BMF_CHECK(bmf_publish_replacement(base, target, "new") == 0, 5,
              "publication must succeed");
    bmf_stage("published-writer");

    /* The writer's hit-wr acquisition waits behind the holder's rdlock
     * (ordinary blocking contention). Release the holder only after that
     * real wait entry is visible; the writer must then return a cached
     * handle on the new inode — not a timed EBUSY. */
    int baseline = bm_test_lock_entered(BM_TEST_LOCK_HIT_WR);
    BmfStaleWriter w;
    memset(&w, 0, sizeof(w));
    w.path = target;
    atomic_init(&w.done, 0);
    BMF_CHECK(pthread_create(&w.tid, NULL, bmf_stale_writer_main, &w) == 0, 6,
              "writer thread start");
    for (int i = 0; i < 5000 &&
         bm_test_lock_entered(BM_TEST_LOCK_HIT_WR) <= baseline; i++)
        bmf_sleep_ms(2);
    int entered = bm_test_lock_entered(BM_TEST_LOCK_HIT_WR) > baseline;
    bmf_holder_release(&h);
    pthread_join(w.tid, NULL);
    BMF_CHECK(entered, 7, "writer hit-wr wait must be observable");
    BMF_CHECK(w.got == 1, 8, "writer open after release must succeed");
    BMF_CHECK(w.cached == 1, 9,
              "writer open must be cached, not an uncached new-inode handle");
    BMF_CHECK(w.reads_new == 1, 10, "writer handle must serve the new inode");
    bmf_stage("writer-new-inode-cached");

    bmf_child_cache_teardown();
    bmf_rmtree_flat(base);
    return 0;
}

static int bmf_stale_droptry_child(void) {
    char tmpl[128];
    snprintf(tmpl, sizeof(tmpl), "/tmp/shard-db-bmf-staledrop-%d-XXXXXX", getpid());
    char *base = mkdtemp(tmpl);
    BMF_CHECK(base != NULL, 1, "mkdtemp");
    BMF_CHECK(bmf_child_cache16() == 0, 2, "cap16 cache init");

    char target[PATH_MAX];
    snprintf(target, sizeof(target), "%s/staled.bm", base);
    BMF_CHECK(bmf_make_old(target) == 0, 3, "old content create");

    BmfHolder h;
    BMF_CHECK(bmf_holder_start(&h, target) == 0, 4, "holder must hold");

    BMF_CHECK(bmf_publish_replacement(base, target, "new") == 0, 5,
              "publication must succeed");
    BMF_CHECK(bm_test_lock_failure_consumed(BM_TEST_LOCK_DROP_TRY) == 0, 6,
              "no DROP_TRY fault may be consumed during publication");
    bm_test_fail_lock_next(BM_TEST_LOCK_DROP_TRY, EINVAL);
    bmf_stage("fault-armed");

    errno = 0;
    BitmapShard *rm = bm_open(target, 0, 0, 0, 0, 0);
    if (rm != NULL) {
        int cached = bm_handle_cached(rm);
        bm_close(rm);
        BMF_CHECK(cached == 1, 7, "no uncached handle may be returned");
        BMF_CHECK(0, 8, "injected retirement failure must fail the open");
    }
    BMF_CHECK(errno == EINVAL, 9, "injected DROP_TRY error must surface exactly");
    BMF_CHECK(bm_test_lock_failure_consumed(BM_TEST_LOCK_DROP_TRY) == 1, 10,
              "the fault must be consumed exactly once");
    BMF_CHECK(atomic_load(&h.holding) == 1, 11, "hold stays live");
    bmf_stage("fault-consumed");

    bmf_holder_release(&h);
    bm_test_reset_lock_failure();
    rm = bm_open(target, 0, 0, 0, 0, 0);
    BMF_CHECK(rm != NULL && bm_handle_cached(rm) == 1, 12,
              "cleared fault: retry succeeds cached");
    if (rm) bm_close(rm);

    bmf_child_cache_teardown();
    bmf_rmtree_flat(base);
    return 0;
}

static int test_bitmap_open_stale_publication_run(void) {
    BmfChildResult r;
    const char *names[3] = { "stale-reader", "stale-writer", "stale-droptry" };
    BmfChildFn fns[3] = { bmf_stale_reader_child, bmf_stale_writer_child,
                          bmf_stale_droptry_child };
    for (int i = 0; i < 3; i++) {
        if (bmf_run_child(names[i], fns[i], &r) != 1 ||
            r.ran != 1 || r.exit_code != 0 || r.timed_out || r.termsig) {
            TAP_DIAG("# %s child: ran=%d exit=%d timed_out=%d termsig=%d\n%s",
                     names[i], r.ran, r.exit_code, r.timed_out, r.termsig,
                     r.stages);
            ASSERT_TRUE(0, "0");
            return 1;
        }
        ASSERT_EQ_INT(0, r.exit_code, "child exits 0");
        if (i == 0) {
            ASSERT_TRUE(strstr(r.stages, "holder-live") != NULL, "strstr(r.stages, 'holder-live') != NULL");
            ASSERT_TRUE(strstr(r.stages, "published") != NULL, "strstr(r.stages, 'published') != NULL");
            ASSERT_TRUE(strstr(r.stages, "reacquire-bounded") != NULL, "strstr(r.stages, 'reacquire-bounded') != NULL");
            ASSERT_TRUE(strstr(r.stages, "retry-new-inode") != NULL, "strstr(r.stages, 'retry-new-inode') != NULL");
        }
        if (i == 2) {
            ASSERT_TRUE(strstr(r.stages, "fault-armed") != NULL, "strstr(r.stages, 'fault-armed') != NULL");
            ASSERT_TRUE(strstr(r.stages, "fault-consumed") != NULL, "strstr(r.stages, 'fault-consumed') != NULL");
        }
    }
    return 0;
}

/* ═══════════════════ Case 3: damaged-shard reads ═══════════════════ */

static int test_bitmap_damaged_shard_reads_run(void) {
    BmfDaemon d;
    int failures = 0;
    int src = bmf_daemon_setup(&d, NULL, "d3", "t3", 1200, 64, 500);
    if (src != 0) {
        TAP_DIAG("# setup failed rc=%d\n", src);
        bmf_daemon_stop(&d);
        ASSERT_TRUE(0, "setup");
        return 1;
    }

    failures += bmf_check_count(&d, &d.o, NULL, bmf_oracle_count(&d.o),
                                BMF_WANT_EXACT, "pre total");
    failures += bmf_check_count(&d, &d.o,
        "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}]",
        bmf_oracle_count_field(&d.o, 0, 1), BMF_WANT_EXACT, "pre f0=1");
    failures += bmf_check_count(&d, &d.o,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]",
        bmf_oracle_count_field(&d.o, 1, 2), BMF_WANT_EXACT, "pre f1=2");

    int shard = -1;
    if (bmf_shard_by_live(&d.o, 0, &shard) != 0) {
        TAP_DIAG("# no target shard\n");
        failures++;
    } else {
        BmfKfShard shards[MAX_SPLITS];
        int n = bmf_kf_shards(&d.o, shards, MAX_SPLITS);
        BmfKfShard *t = NULL;
        for (int i = 0; i < n; i++)
            if (shards[i].shard == shard) t = &shards[i];
        if (!t || t->deleted == 0 || (t->total - t->deleted) == 0) {
            TAP_DIAG("# target shard lacks live rows or tombstones\n");
            failures++;
        }
    }

    char target_bm[PATH_MAX] = "";
    char marker[PATH_MAX] = "";
    if (failures == 0) {
        tc_close(d.tc);
        d.tc = NULL;
        test_env_stop_keep(&d.env);
        bmf_bm_path(target_bm, sizeof(target_bm), &d.o, "f0", shard);
        bmf_marker_path(marker, sizeof(marker), &d.o, shard);
        unlink(marker);
        if (truncate(target_bm, 12) != 0) failures++;
        int64_t dl = bmf_monotonic_ms() + 30000;
        int64_t prev = test_fixture_set_start_deadline_ms(dl);
        int src = test_env_start_at(&d.env, d.o.root, d.env.port);
        test_fixture_set_start_deadline_ms(prev);
        if (src != 0) failures++;
        if (failures == 0) {
            TestClientCfg cfg = { .port = d.env.port };
            d.tc = tc_connect(&cfg);
            d.deadline_ms = bmf_monotonic_ms() + bmf_work_remaining_ms()
                            - BMF_CLEANUP_RESERVE_MS;
            if (!d.tc) failures++;
        }
    }
    ASSERT_TRUE(failures == 0, "failures == 0");
    if (failures != 0) { bmf_daemon_stop(&d); return 1; }

    /* Vacuum: must succeed while skipping the damaged shard; its marker is
     * retained for the startup rebuild. */
    {
        char req[512], *resp = NULL;
        bmf_vacuum_req(&d.o, req, sizeof(req));
        int rc = bmf_req(&d, req, &resp);
        if (rc != 0 || bmf_validate_no_error(resp) != 0) {
            TAP_DIAG("# vacuum must succeed while skipping the damaged shard\n");
            failures++;
        }
        free(resp);
        if (!bmf_file_exists(marker)) {
            TAP_DIAG("# bmfix marker must be retained after a corrupt skip\n");
            failures++;
        }
    }

    /* Damaged-field reads: exact errors or exact whole-oracle answers. */
    failures += bmf_check_count(&d, &d.o,
        "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}]",
        bmf_oracle_count_field(&d.o, 0, 1), BMF_WANT_EXACT_OR_ERROR, "f0=1 count");
    failures += bmf_check_find(&d, &d.o,
        "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}]", 0, 1, 5000,
        BMF_WANT_EXACT_OR_ERROR, "f0=1 find");
    {
        long counts[4];
        bmf_oracle_group_counts(&d.o, 0, counts);
        failures += bmf_check_group_counts(&d, &d.o,
            "\"aggregates\":[{\"fn\":\"count\"}],\"group_by\":\"f0\"",
            "f0", counts, BMF_WANT_EXACT_OR_ERROR, "f0 group-by");
    }

    /* Healthy-field and metadata reads stay exact. */
    failures += bmf_check_count(&d, &d.o,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]",
        bmf_oracle_count_field(&d.o, 1, 2), BMF_WANT_EXACT, "f1=2 healthy");
    failures += bmf_check_count(&d, &d.o, NULL, bmf_oracle_count(&d.o),
                                BMF_WANT_EXACT, "total healthy");
    ASSERT_TRUE(failures == 0, "failures == 0");
    if (failures != 0) { bmf_daemon_stop(&d); return 1; }

    /* Restart WITH the marker: startup repair runs, removes the marker, and
     * the damaged field becomes exact again. */
    tc_close(d.tc);
    d.tc = NULL;
    test_env_stop_keep(&d.env);
    {
        int64_t dl = bmf_monotonic_ms() + 60000;
        int64_t prev = test_fixture_set_start_deadline_ms(dl);
        int src = test_env_start_at(&d.env, d.o.root, d.env.port);
        test_fixture_set_start_deadline_ms(prev);
        if (src != 0) {
            TAP_DIAG("# restart with marker failed rc=%d errno=%d\n", src, errno);
            bmf_dump_daemon_log(&d.env);
            failures++;
        }
        if (failures == 0) {
            TestClientCfg cfg = { .port = d.env.port };
            d.tc = tc_connect(&cfg);
            d.deadline_ms = bmf_monotonic_ms() + bmf_work_remaining_ms()
                            - BMF_CLEANUP_RESERVE_MS;
            if (!d.tc) failures++;
        }
    }
    ASSERT_TRUE(failures == 0, "failures == 0");
    if (failures != 0) { bmf_daemon_stop(&d); return 1; }

    if (bmf_file_exists(marker)) {
        TAP_DIAG("# marker must be removed by the startup repair\n");
        failures++;
    }
    failures += bmf_check_count(&d, &d.o,
        "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}]",
        bmf_oracle_count_field(&d.o, 0, 1), BMF_WANT_EXACT, "repaired f0=1");
    {
        long counts[4];
        bmf_oracle_group_counts(&d.o, 0, counts);
        failures += bmf_check_group_counts(&d, &d.o,
            "\"aggregates\":[{\"fn\":\"count\"}],\"group_by\":\"f0\"",
            "f0", counts, BMF_WANT_EXACT, "repaired f0 group-by");
    }
    failures += bmf_check_find(&d, &d.o,
        "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}]", 0, 1, 5000,
        BMF_WANT_EXACT, "repaired f0=1 find");

    int kf_fail = 0;
    {
        BmfKfShard shards[MAX_SPLITS];
        int n = bmf_kf_shards(&d.o, shards, MAX_SPLITS);
        long live = 0, deleted = 0;
        for (int i = 0; i < n; i++) {
            live += (long)(shards[i].total - shards[i].deleted);
            deleted += (long)shards[i].deleted;
        }
        if (live != bmf_oracle_count(&d.o)) kf_fail = 1;
        if (deleted != 0) kf_fail = 1;   /* vacuum compaction dropped tombstones */
    }
    if (kf_fail) {
        TAP_DIAG("# keyfile oracle must show compacted live set, zero tombstones\n");
        failures++;
    }

    bmf_daemon_stop(&d);
    ASSERT_TRUE(failures == 0, "failures == 0");
    return failures != 0;
}

/* ═══════════════════ Case 4: corrupt-cache retirement ═══════════════════ */

typedef struct {
    ShardDb *db;
    char     root[256];
    BmfOracle o;
} BmfPdb;

static int bmf_pdb_req(ShardDb *db, const char *json, char **out_resp) {
    size_t out_len = 0;
    *out_resp = NULL;
    return shard_db_query(db, json, out_resp, &out_len);
}

static void bmf_pdb_free(char *resp) {
    shard_db_free_result(resp);
}

static int bmf_pdb_create(BmfPdb *p, const char *dir, const char *object) {
    char req[1024], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"%s\",\"object\":\"%s\","
             "\"splits\":8,\"max_key\":64,"
             "\"fields\":[\"id:int\",\"f0:enum(0,1,2,3)\","
             "\"f1:enum(0,1,2,3)\",\"f2:enum(0,1,2,3)\",\"pay:varchar:64\"],"
             "\"indexes\":[\"id\",\"f0:bitmap\",\"f1:bitmap\",\"f2:bitmap\"]}",
             dir, object);
    int rc = bmf_pdb_req(p->db, req, &resp);
    if (rc == 0) rc = bmf_validate_no_error(resp);
    bmf_pdb_free(resp);
    return rc;
}

static int bmf_pdb_seed(BmfPdb *p, long from, long to, int chunk) {
    char *req = malloc((size_t)chunk * 220 + 512);
    if (!req) return -1;
    long cursor = from;
    int rc = 0;
    while (cursor <= to) {
        int built = bmf_build_bulk_insert(&p->o, &cursor, to, chunk, 0,
                                          req, (size_t)chunk * 220 + 512);
        if (built <= 0) { rc = built < 0 ? -1 : 0; break; }
        char *resp = NULL;
        int rrc = bmf_pdb_req(p->db, req, &resp);
        if (rrc == 0) rrc = bmf_validate_no_error(resp);
        bmf_pdb_free(resp);
        if (rrc != 0) { rc = -1; break; }
    }
    free(req);
    return rc;
}

static int bmf_pdb_delete_tenth(BmfPdb *p, long from, long to) {
    char req[1 << 16];
    long cursor = from;
    while (cursor <= to) {
        int built = bmf_build_bulk_delete_tenth(&p->o, &cursor, to, req,
                                                sizeof(req));
        if (built <= 0) return built < 0 ? -1 : 0;
        char *resp = NULL;
        int rc = bmf_pdb_req(p->db, req, &resp);
        if (rc == 0) rc = bmf_validate_no_error(resp);
        bmf_pdb_free(resp);
        if (rc != 0) return -1;
    }
    return 0;
}

static int bmf_pdb_count(BmfPdb *p, const char *criteria, long long *out) {
    char req[1024], *resp = NULL;
    char dir_part[64], obj_part[64];
    bmf_split_obj(p->o.obj_dir, dir_part, sizeof(dir_part),
                  obj_part, sizeof(obj_part));
    if (criteria)
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\","
                 "\"criteria\":%s}", dir_part, obj_part, criteria);
    else
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\"}",
                 dir_part, obj_part);
    int rc = bmf_pdb_req(p->db, req, &resp);
    if (rc == 0) rc = bmf_parse_bare_int(resp, out);
    bmf_pdb_free(resp);
    return rc;
}

static int bmf_pdb_vacuum(BmfPdb *p, char *out_resp, size_t cap) {
    char req[512], *resp = NULL;
    bmf_vacuum_req(&p->o, req, sizeof(req));
    int rc = bmf_pdb_req(p->db, req, &resp);
    if (rc == 0 && out_resp) snprintf(out_resp, cap, "%s", resp);
    if (rc == 0) rc = bmf_validate_no_error(resp);
    bmf_pdb_free(resp);
    return rc;
}

static int bmf_retire_warm_child(void) {
    BMF_CHECK(bmf_child_db != NULL, 1, "process db must exist");
    BmfPdb p;
    memset(&p, 0, sizeof(p));
    p.db = bmf_child_db;
    snprintf(p.root, sizeof(p.root), "%s", test_get_process_db_root());
    bmf_oracle_init(&p.o, p.root, "bmf_d4", "t4", 400);
    BMF_CHECK(bmf_pdb_create(&p, "bmf_d4", "t4") == 0, 2, "create object");
    BMF_CHECK(bmf_pdb_seed(&p, 1, 400, 400) == 0, 3, "seed");
    BMF_CHECK(bmf_pdb_delete_tenth(&p, 1, 400) == 0, 4, "delete tenth");
    long long before = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}]", &before) == 0, 5,
        "warm count");
    BMF_CHECK(before == bmf_oracle_count_field(&p.o, 0, 1), 6, "warm oracle");
    bmf_stage("warm");

    int shard = -1;
    BMF_CHECK(bmf_shard_by_live(&p.o, 0, &shard) == 0, 7, "target shard");
    char target_bm[PATH_MAX];
    bmf_bm_path(target_bm, sizeof(target_bm), &p.o, "f0", shard);
    BMF_CHECK(bm_test_damage_header_bounds(target_bm) == 0, 8,
              "in-place header damage must succeed");
    BMF_CHECK(bm_file_is_corrupt(target_bm) == 1, 9,
              "cold probe must classify the file corrupt");
    bmf_stage("damaged-warm");

    /* The rebuild must skip the damaged bitmap only together with checked
     * retirement of its cached entry. */
    char vac[256] = "";
    BMF_CHECK(bmf_pdb_vacuum(&p, vac, sizeof(vac)) == 0, 10,
              "vacuum must succeed with a skipped corrupt bitmap");
    bmf_stage("vacuum-skipped");

    char marker[PATH_MAX];
    bmf_marker_path(marker, sizeof(marker), &p.o, shard);
    BMF_CHECK(bmf_file_exists(marker) == 1, 11, "marker retained for the skip");

    /* Retirement proof (independent of read-error propagation): with the
     * cached entry retired, a fresh open of the damaged path must cold-open
     * the file and fail EUCLEAN. A surviving stale entry would hand back a
     * successful cached handle instead. */
    errno = 0;
    BitmapShard *probe = bm_open(target_bm, 0, 0, 0, 0, 0);
    if (probe != NULL) {
        bm_close(probe);
        bmf_stage("FAIL stale cached entry survived the corrupt skip");
        return 18;
    }
    BMF_CHECK(errno == EUCLEAN, 19, "retired damaged path must cold-fail EUCLEAN");
    bmf_stage("entry-retired");

    /* Propagation: the next read cold-opens the damaged file and must fail
     * closed rather than return a silently partial or stale answer. */
    long long after = -1;
    int crc = bmf_pdb_count(&p,
        "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}]", &after);
    if (crc == 0 && after >= 0) {
        bmf_stage("FAIL damaged-field read returned a successful count");
        return 12;
    }
    bmf_stage("no-stale-success");

    /* Healthy field and keyfile oracle stay exact; the vacuum compacted the
     * tombstones away and did not lose live records. */
    long long f1 = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]", &f1) == 0, 13,
        "healthy f1 count");
    BMF_CHECK(f1 == bmf_oracle_count_field(&p.o, 1, 2), 14, "healthy f1 oracle");
    {
        BmfKfShard shards[MAX_SPLITS];
        int n = bmf_kf_shards(&p.o, shards, MAX_SPLITS);
        long live = 0, deleted = 0;
        for (int i = 0; i < n; i++) {
            live += (long)(shards[i].total - shards[i].deleted);
            deleted += (long)shards[i].deleted;
        }
        BMF_CHECK(live == bmf_oracle_count(&p.o), 15, "kf live oracle");
        BMF_CHECK(deleted == 0, 16, "vacuum compacted the tombstones");
    }
    bmf_stage("kf-oracle-ok");

    char drop[512], *resp = NULL;
    snprintf(drop, sizeof(drop),
             "{\"mode\":\"drop-object\",\"dir\":\"bmf_d4\",\"object\":\"t4\","
             "\"if_exists\":true}");
    BMF_CHECK(bmf_pdb_req(p.db, drop, &resp) == 0, 17, "drop object");
    bmf_pdb_free(resp);
    return 0;
}

static int bmf_retire_fault_child(void) {
    BMF_CHECK(bmf_child_db != NULL, 1, "process db must exist");
    BmfPdb p;
    memset(&p, 0, sizeof(p));
    p.db = bmf_child_db;
    snprintf(p.root, sizeof(p.root), "%s", test_get_process_db_root());
    bmf_oracle_init(&p.o, p.root, "bmf_d4", "t4b", 300);
    BMF_CHECK(bmf_pdb_create(&p, "bmf_d4", "t4b") == 0, 2, "create object");
    BMF_CHECK(bmf_pdb_seed(&p, 1, 300, 300) == 0, 3, "seed");
    BMF_CHECK(bmf_pdb_delete_tenth(&p, 1, 300) == 0, 4, "delete tenth");
    long long warm = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"2\"}]", &warm) == 0, 5,
        "warm count");

    int shard = -1;
    BMF_CHECK(bmf_shard_by_live(&p.o, 0, &shard) == 0, 6, "target shard");
    char target_bm[PATH_MAX];
    bmf_bm_path(target_bm, sizeof(target_bm), &p.o, "f0", shard);
    BMF_CHECK(bm_test_damage_header_bounds(target_bm) == 0, 7, "damage");

    /* Snapshot the target shard's keyfile (header totals and capacity)
     * before the faulted rebuild. Vacuum compacts shards independently, so
     * only the refused shard is required to stay untouched. */
    BmfKfShard before_kf = {0};
    struct stat before_st;
    char target_kf[PATH_MAX];
    snprintf(target_kf, sizeof(target_kf), "%s/%s/data/kf/%03d.kf",
             p.o.root, p.o.obj_dir, shard);
    {
        BmfKfShard shards[MAX_SPLITS];
        int n = bmf_kf_shards(&p.o, shards, MAX_SPLITS);
        for (int i = 0; i < n; i++)
            if (shards[i].shard == shard) before_kf = shards[i];
        BMF_CHECK(before_kf.valid && before_kf.deleted > 0, 22,
                  "target shard must carry tombstones before the vacuum");
        BMF_CHECK(stat(target_kf, &before_st) == 0, 23, "target kf stat");
    }

    /* Checked retirement failure must refuse the rebuild: the vacuum aborts
     * with the existing compaction error and the refused shard's keyfile is
     * unchanged. */
    bm_test_fail_invalidate_next(1);
    char vac[256] = "";
    int vrc = bmf_pdb_vacuum(&p, vac, sizeof(vac));
    if (vrc == 0) {
        bmf_stage("FAIL vacuum succeeded despite injected retirement failure");
        return 8;
    }
    BMF_CHECK(vrc != 0, 8, "vacuum must refuse when retirement fails");
    BMF_CHECK(bmf_validate_error_exact(vac, "keyfile compaction failed") == 0, 9,
              "refusal must use the existing compaction error");
    bmf_stage("vacuum-refused");
    bm_test_fail_reset();

    /* The refused attempt accepted no skip: the marker it created was
     * removed by the abort path. */
    char marker[PATH_MAX];
    bmf_marker_path(marker, sizeof(marker), &p.o, shard);
    BMF_CHECK(bmf_file_exists(marker) == 0, 20,
              "refused rebuild must not leave a skip marker");

    {
        BmfKfShard shards[MAX_SPLITS];
        int n = bmf_kf_shards(&p.o, shards, MAX_SPLITS);
        long live = 0;
        BmfKfShard after_kf = {0};
        for (int i = 0; i < n; i++) {
            live += (long)(shards[i].total - shards[i].deleted);
            if (shards[i].shard == shard) after_kf = shards[i];
        }
        BMF_CHECK(live == bmf_oracle_count(&p.o), 10, "kf live unchanged");
        struct stat after_st;
        BMF_CHECK(stat(target_kf, &after_st) == 0, 24, "target kf stat after");
        if (!after_kf.valid || after_kf.total != before_kf.total ||
            after_kf.deleted != before_kf.deleted ||
            after_st.st_size != before_st.st_size) {
            char msg[160];
            snprintf(msg, sizeof(msg),
                     "FAIL refused shard keyfile changed: total %llu->%llu "
                     "deleted %llu->%llu size %lld->%lld",
                     (unsigned long long)before_kf.total,
                     (unsigned long long)after_kf.total,
                     (unsigned long long)before_kf.deleted,
                     (unsigned long long)after_kf.deleted,
                     (long long)before_st.st_size, (long long)after_st.st_size);
            bmf_stage(msg);
            return 21;
        }
        BMF_CHECK(1, 11, "refused shard keyfile unchanged");
    }

    /* Clearing the fault: the rebuild succeeds with the skip and keeps the
     * marker. */
    BMF_CHECK(bmf_pdb_vacuum(&p, vac, sizeof(vac)) == 0, 12,
              "vacuum succeeds after the fault clears");
    BMF_CHECK(bmf_file_exists(marker) == 1, 13, "marker retained for the skip");
    bmf_stage("recovery-ok");

    char drop[512], *resp = NULL;
    snprintf(drop, sizeof(drop),
             "{\"mode\":\"drop-object\",\"dir\":\"bmf_d4\",\"object\":\"t4b\","
             "\"if_exists\":true}");
    BMF_CHECK(bmf_pdb_req(p.db, drop, &resp) == 0, 14, "drop object");
    bmf_pdb_free(resp);
    return 0;
}

static int test_bitmap_corrupt_cache_retirement_run(void) {
    /* Independent children: retirement-failure refusal runs first and does
     * not depend on read-error propagation; the warm-skip child then proves
     * retirement (cold EUCLEAN reopen) and fail-closed reads. */
    BmfChildResult r;
    int failures = 0;
    int rc = bmf_run_child("retire-fault", bmf_retire_fault_child, &r);
    if (rc != 1 || r.ran != 1 || r.exit_code != 0 || r.timed_out || r.termsig) {
        TAP_DIAG("# retire-fault child: ran=%d exit=%d timed_out=%d termsig=%d\n%s",
                 r.ran, r.exit_code, r.timed_out, r.termsig, r.stages);
        failures++;
    } else {
        ASSERT_TRUE(strstr(r.stages, "vacuum-refused") != NULL,
                    "retirement failure refuses the rebuild");
        ASSERT_TRUE(strstr(r.stages, "recovery-ok") != NULL,
                    "cleared fault: rebuild succeeds with the skip");
    }

    rc = bmf_run_child("retire-warm", bmf_retire_warm_child, &r);
    if (rc != 1 || r.ran != 1 || r.exit_code != 0 || r.timed_out || r.termsig) {
        TAP_DIAG("# retire-warm child: ran=%d exit=%d timed_out=%d termsig=%d\n%s",
                 r.ran, r.exit_code, r.timed_out, r.termsig, r.stages);
        failures++;
    } else {
        ASSERT_TRUE(strstr(r.stages, "vacuum-skipped") != NULL,
                    "warm damaged bitmap skipped by the rebuild");
        ASSERT_TRUE(strstr(r.stages, "entry-retired") != NULL,
                    "damaged cache entry retired before the skip");
        ASSERT_TRUE(strstr(r.stages, "no-stale-success") != NULL,
                    "damaged-field read fails closed after the skip");
    }
    ASSERT_TRUE(failures == 0, "both retirement children pass");
    return failures != 0;
}

/* ═══════════════════ Case 5: read-error propagation ═══════════════════ */

static int test_bitmap_read_error_propagation_run(void) {
    BmfDaemon d;
    int failures = 0;
    if (bmf_daemon_setup(&d, NULL, "d5", "t5", 1200, 64, 500) != 0) {
        TAP_DIAG("# setup failed\n");
        bmf_daemon_stop(&d);
        ASSERT_TRUE(0, "0");
        return 1;
    }

    /* ENOENT control: the f2 field was never written; every shard's file is
     * legitimately absent, contributing logical emptiness without error. */
    failures += bmf_check_count(&d, &d.o,
        "[{\"field\":\"f2\",\"op\":\"eq\",\"value\":\"1\"}]", 0,
        BMF_WANT_EXACT, "ENOENT f2=1");

    int shard = -1;
    if (bmf_shard_by_live(&d.o, 0, &shard) != 0) {
        TAP_DIAG("# no target shard\n");
        failures++;
    }
    ASSERT_TRUE(failures == 0, "failures == 0");
    if (failures != 0) { bmf_daemon_stop(&d); return 1; }

    tc_close(d.tc);
    d.tc = NULL;
    test_env_stop_keep(&d.env);
    char target_bm[PATH_MAX];
    bmf_bm_path(target_bm, sizeof(target_bm), &d.o, "f0", shard);
    if (truncate(target_bm, 12) != 0) failures++;
    {
        int64_t dl = bmf_monotonic_ms() + 30000;
        int64_t prev = test_fixture_set_start_deadline_ms(dl);
        int src = test_env_start_at(&d.env, d.o.root, d.env.port);
        test_fixture_set_start_deadline_ms(prev);
        if (src != 0) failures++;
        if (failures == 0) {
            TestClientCfg cfg = { .port = d.env.port };
            d.tc = tc_connect(&cfg);
            d.deadline_ms = bmf_monotonic_ms() + bmf_work_remaining_ms()
                            - BMF_CLEANUP_RESERVE_MS;
            if (!d.tc) failures++;
        }
    }
    ASSERT_TRUE(failures == 0, "failures == 0");
    if (failures != 0) { bmf_daemon_stop(&d); return 1; }

    /* Damaged-source consumer matrix. Every damaged-source answer must be a
     * complete exact-error object or the exact whole-oracle answer. */
    char crit[256];

    snprintf(crit, sizeof(crit), "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}]");
    failures += bmf_check_count(&d, &d.o, crit,
        bmf_oracle_count_field(&d.o, 0, 1), BMF_WANT_EXACT_OR_ERROR, "eq");
    failures += bmf_check_find(&d, &d.o, crit, 0, 1, 5000,
        BMF_WANT_EXACT_OR_ERROR, "eq find");

    snprintf(crit, sizeof(crit),
             "[{\"field\":\"f0\",\"op\":\"in\",\"value\":\"1,2\"}]");
    long in_oracle = bmf_oracle_count_field(&d.o, 0, 1) +
                     bmf_oracle_count_field(&d.o, 0, 2);
    failures += bmf_check_count(&d, &d.o, crit, in_oracle,
        BMF_WANT_EXACT_OR_ERROR, "in");

    snprintf(crit, sizeof(crit), "[{\"field\":\"f0\",\"op\":\"neq\",\"value\":\"0\"}]");
    long neq_oracle = bmf_oracle_count_field(&d.o, 0, 1) +
                      bmf_oracle_count_field(&d.o, 0, 2) +
                      bmf_oracle_count_field(&d.o, 0, 3);
    failures += bmf_check_count(&d, &d.o, crit, neq_oracle,
        BMF_WANT_EXACT_OR_ERROR, "generic neq");

    snprintf(crit, sizeof(crit),
             "{\"and\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"},"
             "{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]}");
    long and_oracle = 0;
    for (long i = 1; i <= d.o.seeded; i++)
        if (!bmf_is_deleted(i) && bmf_f0_for(i) == 1 && bmf_f1_for(i) == 2)
            and_oracle++;
    failures += bmf_check_count(&d, &d.o, crit, and_oracle,
        BMF_WANT_EXACT_OR_ERROR, "and");

    snprintf(crit, sizeof(crit),
             "{\"or\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"},"
             "{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"3\"}]}");
    long or_oracle = 0;
    for (long i = 1; i <= d.o.seeded; i++)
        if (!bmf_is_deleted(i) &&
            (bmf_f0_for(i) == 1 || bmf_f1_for(i) == 3))
            or_oracle++;
    failures += bmf_check_count(&d, &d.o, crit, or_oracle,
        BMF_WANT_EXACT_OR_ERROR, "or");

    /* Empty first AND input legitimately establishes emptiness. */
    snprintf(crit, sizeof(crit),
             "{\"and\":[{\"field\":\"f2\",\"op\":\"eq\",\"value\":\"1\"},"
             "{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]}");
    failures += bmf_check_count(&d, &d.o, crit, 0, BMF_WANT_EXACT,
        "empty-and-input");

    /* Complement prefilter (majority criteria) via top-N aggregate. */
    {
        long comp[4];
        bmf_oracle_group_counts(&d.o, 1, comp);
        failures += bmf_check_group_counts(&d, &d.o,
            "\"aggregates\":[{\"fn\":\"count\"}],"
            "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"}],"
            "\"group_by\":\"f1\",\"order_by\":\"count\",\"limit\":1000",
            "f1", comp, BMF_WANT_EXACT_OR_ERROR, "complement prefilter");
    }

    /* Ordered prefilter: the optional probe must fall back to the exact
     * whole-record walk (a complete error is not expected here, but a
     * silently-partial success is the defect). */
    {
        char req[1024];
        char dir_part[64], obj_part[64];
        bmf_split_obj(d.o.obj_dir, dir_part, sizeof(dir_part),
                      obj_part, sizeof(obj_part));
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\","
                 "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}],"
                 "\"order_by\":\"id\",\"order\":\"asc\",\"limit\":5000}",
                 dir_part, obj_part);
        char *resp = NULL;
        int rc = bmf_req(&d, req, &resp);
        int bad = 0;
        long prev_i = 1;
        if (rc != 0) bad = 1;
        else {
            int is_err = bmf_is_error_object(resp, NULL, NULL);
            char **keys = NULL; int n = 0;
            if (is_err == 1) bad = 0;   /* permitted: hard pre-emission error */
            else if (bmf_parse_find_keys(resp, &keys, &n) != 0) {
                TAP_DIAG("# ordered prefilter response: %.120s\n", resp);
                bad = 1;
            } else {
                {
                    long expect = bmf_oracle_count_field(&d.o, 0, 1);
                    int mismatch = n != expect;
                    for (int j = 0; j < n && !mismatch; j++) {
                        long i = bmf_next_matching(&d.o, j == 0 ? 1 : prev_i,
                                                   0, 1);
                        if (i == 0) { mismatch = 1; break; }
                        char key[BMF_KEY_MAX];
                        bmf_key_for(key, sizeof(key), i);
                        if (strcmp(keys[j], key) != 0) mismatch = 1;
                        prev_i = i + 1;
                    }
                    if (mismatch) bad = 1;
                    bmf_free_keys(keys, n);
                }
            }
        }
        free(resp);
        if (bad) {
            TAP_DIAG("# ordered prefilter: neither error nor exact ordered oracle\n");
            failures++;
        }
    }

    /* Slot-intersect group-by (criteria on healthy field). */
    {
        long slot_oracle[4] = {0, 0, 0, 0};
        for (long i = 1; i <= d.o.seeded; i++)
            if (!bmf_is_deleted(i) && bmf_f1_for(i) == 2)
                slot_oracle[bmf_f0_for(i)]++;
        failures += bmf_check_group_counts(&d, &d.o,
            "\"aggregates\":[{\"fn\":\"count\"}],"
            "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}],"
            "\"group_by\":\"f0\"",
            "f0", slot_oracle, BMF_WANT_EXACT_OR_ERROR, "slot intersect");
    }

    /* Group popcount (no criteria) and HBM emit (count + sum). */
    {
        long g[4];
        bmf_oracle_group_counts(&d.o, 0, g);
        failures += bmf_check_group_counts(&d, &d.o,
            "\"aggregates\":[{\"fn\":\"count\"}],\"group_by\":\"f0\"",
            "f0", g, BMF_WANT_EXACT_OR_ERROR, "group popcount");
        failures += bmf_check_group_counts(&d, &d.o,
            "\"aggregates\":[{\"fn\":\"count\"},{\"fn\":\"sum\",\"field\":\"id\"}],"
            "\"group_by\":\"f0\"",
            "f0", g, BMF_WANT_EXACT_OR_ERROR, "hbm emit");
    }

    /* Healthy-field and metadata controls stay exact. */
    failures += bmf_check_count(&d, &d.o,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]",
        bmf_oracle_count_field(&d.o, 1, 2), BMF_WANT_EXACT, "healthy f1");
    failures += bmf_check_count(&d, &d.o, NULL, bmf_oracle_count(&d.o),
                                BMF_WANT_EXACT, "healthy total");
    /* Absent value control: an unknown enum literal is rejected at
     * validation on every build (compatibility behavior, not a bitmap hard
     * error), or answers zero through a scan. */
    {
        long long got = -1;
        char req[1024], *resp = NULL;
        char dir_part[64], obj_part[64];
        bmf_split_obj(d.o.obj_dir, dir_part, sizeof(dir_part),
                      obj_part, sizeof(obj_part));
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\","
                 "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"9\"}]}",
                 dir_part, obj_part);
        int rc = bmf_req(&d, req, &resp);
        int is_err = rc == 0 ? bmf_is_error_object(resp, NULL, NULL) : -1;
        int ok_ctrl = (rc == 0) &&
                      (is_err == 1 ||
                       (is_err == 0 && bmf_parse_bare_int(resp, &got) == 0 &&
                        got == 0));
        free(resp);
        if (!ok_ctrl) {
            TAP_DIAG("# absent value control failed\n");
            failures++;
        }
    }

    /* Empty valid bitmap: write one f2 record, delete it; the file exists
     * with a cleared bitmap and must answer exactly zero. */
    {
        char req[1024], *resp = NULL;
        char dir_part[64], obj_part[64];
        bmf_split_obj(d.o.obj_dir, dir_part, sizeof(dir_part),
                      obj_part, sizeof(obj_part));
        snprintf(req, sizeof(req),
                 "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
                 "\"key\":\"k999999\",\"value\":{\"id\":\"999999\","
                 "\"f0\":\"0\",\"f1\":\"0\",\"f2\":\"1\",\"pay\":\"tmp\"}}",
                 dir_part, obj_part);
        int rc = bmf_req(&d, req, &resp);
        if (rc == 0) rc = bmf_validate_no_error(resp);
        free(resp);
        if (rc != 0) failures++;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"delete\",\"dir\":\"%s\",\"object\":\"%s\","
                 "\"key\":\"k999999\"}", dir_part, obj_part);
        resp = NULL;
        rc = bmf_req(&d, req, &resp);
        if (rc == 0) rc = bmf_validate_no_error(resp);
        free(resp);
        if (rc != 0) failures++;
        failures += bmf_check_count(&d, &d.o,
            "[{\"field\":\"f2\",\"op\":\"eq\",\"value\":\"1\"}]", 0,
            BMF_WANT_EXACT, "empty valid bitmap");
        failures += bmf_check_count(&d, &d.o, NULL, bmf_oracle_count(&d.o),
                                    BMF_WANT_EXACT, "total after f2 cycle");
    }

    ASSERT_TRUE(failures == 0, "failures == 0");
    bmf_daemon_stop(&d);
    return failures != 0;
}

/* ═══════════════════ Case 6: find-error framing ═══════════════════ */

/* Raw find request with full framing validation + a healthy follow-up on the
 * same connection. */
static int bmf_check_find_raw(BmfDaemon *d, const BmfOracle *o,
                              const char *req_tail,
                              const char *followup_criteria, long followup_oracle,
                              const char *label) {
    char req[1600];
    char dir_part[64], obj_part[64];
    bmf_split_obj(o->obj_dir, dir_part, sizeof(dir_part),
                  obj_part, sizeof(obj_part));
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\",%s",
             dir_part, obj_part, req_tail);
    char *resp = NULL;
    int rc = bmf_req(d, req, &resp);
    if (rc != 0) {
        TAP_DIAG("# %s: request failed rc=%d\n", label, rc);
        free(resp);
        return 1;
    }
    int is_err = bmf_is_error_object(resp, NULL, NULL);
    int bad = 0;
    if (is_err == 1) {
        /* Exactly one error object, nothing else. */
        bad = 0;
    } else if (is_err == -1) {
        bad = 1;
    } else {
        char **keys = NULL; int n = 0;
        if (bmf_parse_find_keys(resp, &keys, &n) != 0) bad = 1;
        else bmf_free_keys(keys, n);
    }
    free(resp);
    if (bad) {
        TAP_DIAG("# %s: response is not one complete error object or one complete array\n",
                 label);
        return 1;
    }
    /* Framing must remain usable: a healthy follow-up on the SAME connection. */
    long long got = -1;
    char freq[1024];
    snprintf(freq, sizeof(freq),
             "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\","
             "\"criteria\":%s}", dir_part, obj_part, followup_criteria);
    resp = NULL;
    rc = bmf_req(d, freq, &resp);
    if (rc != 0 || bmf_parse_bare_int(resp, &got) != 0 ||
        got != followup_oracle) {
        TAP_DIAG("# %s: healthy follow-up failed (framing broken)\n", label);
        free(resp);
        return 1;
    }
    free(resp);
    return 0;
}

static int test_bitmap_find_error_framing_run(void) {
    BmfDaemon d;
    int failures = 0;
    if (bmf_daemon_setup(&d, NULL, "d6", "t6", 900, 64, 450) != 0) {
        TAP_DIAG("# setup failed\n");
        bmf_daemon_stop(&d);
        ASSERT_TRUE(0, "0");
        return 1;
    }

    /* Join target: a plain btree object with the same ids. */
    {
        char req[16384], *resp = NULL;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"create-object\",\"dir\":\"d6\",\"object\":\"t6b\","
                 "\"splits\":8,\"max_key\":64,"
                 "\"fields\":[\"id:int\"],\"indexes\":[\"id\"]}");
        int rc = bmf_req(&d, req, &resp);
        if (rc != 0 || bmf_validate_no_error(resp) != 0) {
            TAP_DIAG("# t6b create failed rc=%d resp=%.120s\n", rc,
                     resp ? resp : "(null)");
            failures++;
        }
        free(resp);
        for (long i = 1; i <= 900 && failures == 0; i += 450) {
            long end = i + 449 > 900 ? 900 : i + 449;
            size_t off = (size_t)snprintf(req, sizeof(req),
                "{\"mode\":\"bulk-insert\",\"dir\":\"d6\",\"object\":\"t6b\","
                "\"records\":{");
            for (long j = i; j <= end; j++) {
                int n = snprintf(req + off, sizeof(req) - off,
                    "%s\"k%06ld\":{\"id\":\"%ld\"}", j > i ? "," : "", j, j);
                if (n < 0 || (size_t)n >= sizeof(req) - off) {
                    TAP_DIAG("# t6b seed builder overflow at %ld\n", j);
                    failures++;
                    break;
                }
                off += (size_t)n;
            }
            if (failures) break;
            int n = snprintf(req + off, sizeof(req) - off, "}}");
            if (n < 0 || (size_t)n >= sizeof(req) - off) failures++;
            resp = NULL;
            rc = bmf_req(&d, req, &resp);
            if (rc != 0 || bmf_validate_no_error(resp) != 0) {
                TAP_DIAG("# t6b seed chunk at %ld failed rc=%d resp=%.120s\n",
                         i, rc, resp ? resp : "(null)");
                failures++;
            }
            free(resp);
        }
    }
    ASSERT_TRUE(failures == 0, "failures == 0");
    if (failures != 0) { bmf_daemon_stop(&d); return 1; }

    int shard = -1;
    if (bmf_shard_highest_index(&d.o, &shard) != 0) {
        TAP_DIAG("# no late shard\n");
        failures++;
    }
    ASSERT_TRUE(failures == 0, "failures == 0");
    if (failures != 0) { bmf_daemon_stop(&d); return 1; }

    tc_close(d.tc);
    d.tc = NULL;
    test_env_stop_keep(&d.env);
    char target_bm[PATH_MAX];
    bmf_bm_path(target_bm, sizeof(target_bm), &d.o, "f0", shard);
    if (truncate(target_bm, 12) != 0) failures++;
    {
        int64_t dl = bmf_monotonic_ms() + 30000;
        int64_t prev = test_fixture_set_start_deadline_ms(dl);
        int src = test_env_start_at(&d.env, d.o.root, d.env.port);
        test_fixture_set_start_deadline_ms(prev);
        if (src != 0) failures++;
        if (failures == 0) {
            TestClientCfg cfg = { .port = d.env.port };
            d.tc = tc_connect(&cfg);
            d.deadline_ms = bmf_monotonic_ms() + bmf_work_remaining_ms()
                            - BMF_CLEANUP_RESERVE_MS;
            if (!d.tc) failures++;
        }
    }
    ASSERT_TRUE(failures == 0, "failures == 0");
    if (failures != 0) { bmf_daemon_stop(&d); return 1; }

    long f1_oracle = bmf_oracle_count_field(&d.o, 1, 2);
    const char *f1_crit = "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]";

    /* Default format, full result: one request — a complete error object is
     * required, or the exact full membership. */
    {
        char req[1024];
        char dir_part[64], obj_part[64];
        bmf_split_obj(d.o.obj_dir, dir_part, sizeof(dir_part),
                      obj_part, sizeof(obj_part));
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\","
                 "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}],"
                 "\"limit\":5000}", dir_part, obj_part);
        char *resp = NULL;
        int rc = bmf_req(&d, req, &resp);
        int bad = 0;
        if (rc != 0) bad = 1;
        else {
            int is_err = bmf_is_error_object(resp, NULL, NULL);
            if (is_err == 1) {
                /* complete single error object — the fixed contract */
            } else if (is_err == -1) bad = 1;
            else {
                char **keys = NULL; int n = 0;
                if (bmf_parse_find_keys(resp, &keys, &n) != 0) bad = 1;
                else {
                    long expect = bmf_oracle_count_field(&d.o, 0, 1);
                    if (n != expect) bad = 1;
                    bmf_free_keys(keys, n);
                }
            }
        }
        free(resp);
        if (bad) {
            TAP_DIAG("# default full: neither complete error nor exact membership\n");
            failures++;
        }
    }

    /* Small-limit shapes must still surface the damaged required source. */
    {
        char tail[512];
        snprintf(tail, sizeof(tail),
                 "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}],"
                 "\"limit\":1}");
        failures += bmf_check_find_raw(&d, &d.o, tail, f1_crit, f1_oracle,
                                       "small limit");
        snprintf(tail, sizeof(tail),
                 "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}],"
                 "\"limit\":1,\"format\":\"rows\"}");
        failures += bmf_check_find_raw(&d, &d.o, tail, f1_crit, f1_oracle,
                                       "rows small limit");
        snprintf(tail, sizeof(tail),
                 "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}],"
                 "\"limit\":1,\"format\":\"csv\"}");
        failures += bmf_check_find_raw(&d, &d.o, tail, f1_crit, f1_oracle,
                                       "csv small limit");
        snprintf(tail, sizeof(tail),
                 "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}],"
                 "\"limit\":5,\"join\":[{\"object\":\"t6b\",\"local\":\"id\","
                 "\"remote\":\"id\",\"as\":\"b\",\"type\":\"inner\"}]}");
        failures += bmf_check_find_raw(&d, &d.o, tail, f1_crit, f1_oracle,
                                       "join");
        snprintf(tail, sizeof(tail),
                 "\"criteria\":{\"or\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"},"
                 "{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"1\"}]},\"limit\":5000}");
        failures += bmf_check_find_raw(&d, &d.o, tail, f1_crit, f1_oracle,
                                       "union");
        snprintf(tail, sizeof(tail),
                 "\"criteria\":{\"and\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"},"
                 "{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"1\"}]},\"limit\":5}");
        failures += bmf_check_find_raw(&d, &d.o, tail, f1_crit, f1_oracle,
                                       "intersection");
        snprintf(tail, sizeof(tail),
                 "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}],"
                 "\"order_by\":\"id\",\"order\":\"asc\",\"limit\":100,"
                 "\"cursor\":null,\"total\":true}");
        failures += bmf_check_find_raw(&d, &d.o, tail, f1_crit, f1_oracle,
                                       "cursor+total");
    }

    ASSERT_TRUE(failures == 0, "failures == 0");
    bmf_daemon_stop(&d);
    return failures != 0;
}

/* ═══════════════════ Case 7: prepared small-limit ═══════════════════ */

static int bmf_check_small_find(BmfDaemon *d, const BmfOracle *o,
                                const char *req_tail,
                                int (*row_ok)(long id, void *ctx), void *ctx,
                                int expect_rows, const char *label) {
    char req[1600];
    char dir_part[64], obj_part[64];
    bmf_split_obj(o->obj_dir, dir_part, sizeof(dir_part),
                  obj_part, sizeof(obj_part));
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\",%s",
             dir_part, obj_part, req_tail);
    char *resp = NULL;
    int rc = bmf_req(d, req, &resp);
    if (rc != 0) {
        TAP_DIAG("# %s: request failed rc=%d\n", label, rc);
        free(resp);
        return 1;
    }
    /* Row array first; only a non-array response is checked as an error
     * object. */
    int bad = 0;
    {
        const char *p = resp;
        bmf_json_ws(&p);
        if (*p != '[') {
            int is_err = bmf_is_error_object(resp, NULL, NULL);
            if (is_err == 1)
                TAP_DIAG("# %s: unexpected error response: %.160s\n", label,
                         resp);
            else
                TAP_DIAG("# %s: unparseable response: %.160s\n", label, resp);
            free(resp);
            return 1;
        }
    }
    {
        const char *p = resp;
        bmf_json_ws(&p);
        if (*p != '[') bad = 1;
        else {
            p++;
            int n = 0;
            for (;;) {
                bmf_json_ws(&p);
                if (*p == ']') { p++; break; }
                if (*p == ',') { p++; continue; }
                if (*p != '{') { bad = 1; break; }
                p++;
                const char *key = NULL; size_t klen = 0;
                for (;;) {
                    bmf_json_ws(&p);
                    if (*p == '}') { p++; break; }
                    if (*p == ',') { p++; continue; }
                    const char *mk; size_t mlen;
                    if (bmf_json_string(&p, &mk, &mlen) != 0) { bad = 1; break; }
                    bmf_json_ws(&p);
                    if (*p != ':') { bad = 1; break; }
                    p++;
                    if (mlen == 3 && strncmp(mk, "key", 3) == 0) {
                        if (bmf_json_string(&p, &key, &klen) != 0) { bad = 1; break; }
                    } else if (bmf_json_skip_value(&p, 1) != 0) { bad = 1; break; }
                }
                if (bad || !key || klen < 2) { bad = 1; break; }
                long id = strtol(key + 1, NULL, 10);
                if (id < 1 || !row_ok(id, ctx)) { bad = 1; break; }
                n++;
            }
            bmf_json_ws(&p);
            if (bad || *p != '\0' || n != expect_rows) bad = 1;
        }
    }
    free(resp);
    if (bad) {
        TAP_DIAG("# %s: small-limit result is not the exact bounded answer\n",
                 label);
        return 1;
    }
    return 0;
}

static int bmf_row_f0_zero(long id, void *ctx) {
    (void)ctx;
    return bmf_f0_for(id) == 0 && !bmf_is_deleted(id);
}

static int bmf_row_f0_zero_id_ge(long id, void *ctx) {
    long floor = *(long *)ctx;
    return bmf_f0_for(id) == 0 && !bmf_is_deleted(id) && id >= floor;
}

/* In-process observer check: the find runs on this thread, so the
 * thread-local observer sees exactly this request's prepared iteration. */
static int bmf_small_observer_child(void) {
    BMF_CHECK(bmf_child_db != NULL, 1, "process db must exist");
    BmfPdb p;
    memset(&p, 0, sizeof(p));
    p.db = bmf_child_db;
    snprintf(p.root, sizeof(p.root), "%s", test_get_process_db_root());
    bmf_oracle_init(&p.o, p.root, "bmf_d7", "t7p", 150);
    BMF_CHECK(bmf_pdb_create(&p, "bmf_d7", "t7p") == 0, 2, "create object");
    BMF_CHECK(bmf_pdb_seed(&p, 1, 150, 150) == 0, 3, "seed");

    /* limit 1 over many matching candidates: one fetch, one live ref. */
    query_test_prepared_stream_observe(1);
    char *resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"find\",\"dir\":\"bmf_d7\",\"object\":\"t7p\","
        "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"}],"
        "\"limit\":1}", &resp) == 0, 4, "find limit 1");
    char **keys = NULL; int n = 0;
    int prc = bmf_parse_find_keys(resp, &keys, &n);
    bmf_pdb_free(resp);
    BMF_CHECK(prc == 0 && n == 1, 5, "exactly one row");
    long id = strtol(keys[0] + 1, NULL, 10);
    bmf_free_keys(keys, n);
    BMF_CHECK(bmf_f0_for(id) == 0, 6, "row matches the criteria");
    unsigned calls = 0, fetches = 0, live = 0, peak = 0;
    query_test_prepared_stream_stats(&calls, &fetches, &live, &peak);
    BMF_CHECK(calls == 1, 7, "prepared small-limit iterator served the find");
    BMF_CHECK(fetches == 1, 8, "no payload fetch beyond the limit boundary");
    BMF_CHECK(peak == 1, 9, "at most one live record ref");
    BMF_CHECK(live == 0, 10, "record ref released on exit");
    bmf_stage("observer-limit1-ok");

    /* Rejected candidates before the survivor: fetches stop at the first
       survivor and never exceed rejected-candidates + 1. */
    long rejectable = 0;
    for (long i = 1; i < 120; i++) if (bmf_f0_for(i) == 0) rejectable++;
    query_test_prepared_stream_observe(1);
    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"find\",\"dir\":\"bmf_d7\",\"object\":\"t7p\","
        "\"criteria\":{\"and\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"},"
        "{\"field\":\"pay\",\"op\":\"starts\",\"value\":\"p1\"},"
        "{\"field\":\"id\",\"op\":\"gte\",\"value\":\"120\"}]},"
        "\"limit\":1}", &resp) == 0, 11, "find with rejections");
    keys = NULL; n = 0;
    prc = bmf_parse_find_keys(resp, &keys, &n);
    bmf_pdb_free(resp);
    BMF_CHECK(prc == 0 && n == 1, 12, "exactly one survivor row");
    id = strtol(keys[0] + 1, NULL, 10);
    bmf_free_keys(keys, n);
    BMF_CHECK(id >= 120 && bmf_f0_for(id) == 0, 13, "survivor matches");
    query_test_prepared_stream_stats(&calls, &fetches, &live, &peak);
    BMF_CHECK(calls == 1, 14, "prepared iterator served the post-filtered find");
    BMF_CHECK(fetches >= 1 && fetches <= (unsigned)rejectable + 1, 15,
              "fetches bounded by rejected candidates + 1");
    BMF_CHECK(peak == 1 && live == 0, 16, "one live ref, released");
    query_test_prepared_stream_observe(0);
    bmf_stage("observer-rejections-ok");

    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"drop-object\",\"dir\":\"bmf_d7\",\"object\":\"t7p\","
        "\"if_exists\":true}", &resp) == 0, 17, "drop object");
    bmf_pdb_free(resp);
    return 0;
}

static int test_bitmap_prepared_small_limit_run(void) {
    BmfDaemon d;
    int failures = 0;
    {
        int src = bmf_daemon_setup(&d, "4", "d7", "t7", 150, 8192, 25);
        if (src != 0) {
            TAP_DIAG("# setup failed rc=%d\n", src);
            bmf_daemon_stop(&d);
            ASSERT_TRUE(0, "0");
            return 1;
        }
    }

    /* Exact bounded result with many large matching candidates under a 2 MB
     * query buffer: limit 1 over an f0=="0" majority (~192 candidates). */
    failures += bmf_check_small_find(&d, &d.o,
        "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"}],"
        "\"limit\":1}",
        bmf_row_f0_zero, NULL, 1, "limit1 exact");

    long floor = 120;
    failures += bmf_check_small_find(&d, &d.o,
        "\"criteria\":{\"and\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"},"
        "{\"field\":\"id\",\"op\":\"gte\",\"value\":\"120\"}]},\"limit\":1}",
        bmf_row_f0_zero_id_ge, &floor, 1, "rejected-then-survivor");

    ASSERT_TRUE(failures == 0, "failures == 0");
    bmf_daemon_stop(&d);
    {
        BmfChildResult r;
        int orc = bmf_run_child("small-observer", bmf_small_observer_child, &r);
        int ok = orc == 1 && r.ran == 1 && r.exit_code == 0 && !r.timed_out &&
                 !r.termsig;
        if (!ok)
            TAP_DIAG("# small-observer child: exit=%d timed_out=%d\n%s",
                     r.exit_code, r.timed_out, r.stages);
        ASSERT_TRUE(ok, "prepared iterator: one fetch at limit 1, bounded fetches, one live ref");
        if (!ok) failures++;
    }
    return failures != 0;
}

/* ═══════════════════ Case 8: keyset capacity race ═══════════════════ */

typedef struct {
    ShardDb    *db;
    const BmfOracle *o;
    const char *f1_value;
    const char *f0_value;
    long        from;
    long        to;
    int         past_cap;
    pthread_t   tid;
    atomic_int  done;
    atomic_int  abort;   /* query returned without parking at the barrier */
    const char *damage_path; /* optional: damage this bitmap after inserts */
    int         drop_cache; /* optional: shut the bitmap cache down after
                              inserts/damage so the resumed enumeration
                              cold-opens every shard (writer opens need an
                              initialized cache, so this cannot arm before
                              the inserts) */
    int         rc;
} BmfRaceWriter;

/* Wait for the query thread to park at the sizing/enumeration barrier
 * (sizing complete, no handles held), then insert the overflow records and
 * release the gate. */
static void *bmf_race_writer_main(void *arg) {
    BmfRaceWriter *w = (BmfRaceWriter *)arg;
    /* g_db is thread-local: adopt the child's db before dispatching. */
    g_db = bmf_child_db;
    for (int i = 0; i < 5000 && !query_bm_test_race_reached() &&
                    !atomic_load(&w->abort); i++)
        bmf_sleep_ms(2);
    if (!query_bm_test_race_reached()) {
        w->rc = 8;   /* barrier never reached: fail the scenario */
        return NULL;
    }
    if (w->past_cap)
        w->to = w->from + (long)query_bm_test_race_allocated_cap();
    char *req = malloc(1 << 20);
    if (!req) { w->rc = 9; query_bm_test_race_open_gate(); return NULL; }
    char dir_part[64], obj_part[64];
    bmf_split_obj(w->o->obj_dir, dir_part, sizeof(dir_part),
                  obj_part, sizeof(obj_part));
    long cursor = w->from;
    int rc = 0;
    while (cursor <= w->to) {
        size_t off = (size_t)snprintf(req, 1 << 20,
            "{\"mode\":\"bulk-insert\",\"dir\":\"%s\",\"object\":\"%s\","
            "\"records\":{", dir_part, obj_part);
        long end = cursor + 299 > w->to ? w->to : cursor + 299;
        for (long j = cursor; j <= end; j++) {
            char key[BMF_KEY_MAX];
            snprintf(key, sizeof(key), "w%07ld", j);
            int n = snprintf(req + off, (size_t)(1 << 20) - off,
                "%s\"%s\":{\"id\":\"%ld\",\"f0\":\"%s\",\"f1\":\"%s\","
                "\"pay\":\"race\"}", j > cursor ? "," : "", key, j,
                w->f0_value, w->f1_value);
            if (n < 0 || (size_t)n >= (size_t)(1 << 20) - off) { rc = 10; break; }
            off += (size_t)n;
        }
        if (rc) break;
        int n = snprintf(req + off, (size_t)(1 << 20) - off, "}}");
        if (n < 0 || (size_t)n >= (size_t)(1 << 20) - off) { rc = 10; break; }
        char *resp = NULL;
        size_t out_len = 0;
        if (shard_db_query(w->db, req, &resp, &out_len) != 0) { rc = 11; break; }
        if (bmf_validate_no_error(resp) != 0) rc = 12;
        shard_db_free_result(resp);
        if (rc) break;
        cursor = end + 1;
    }
    free(req);
    w->rc = rc;
    if (w->rc == 0 && w->damage_path && w->damage_path[0])
        if (bm_test_damage_header_bounds(w->damage_path) != 0) w->rc = 13;
    if (w->rc == 0 && w->drop_cache)
        bm_cache_shutdown();   /* parked query holds no handles here */
    query_bm_test_race_open_gate();
    return NULL;
}

/* Arm the barrier, start the writer, run the count on this thread (parking
 * between the sizing and enumeration passes), then join and report. */
static int bmf_race_scenario(BmfPdb *p, int stage, const char *count_criteria,
                             long long *out_count, int *out_err,
                             const char *f0_value, const char *f1_value,
                             long wfrom, long *out_added, int scenario_code) {
    query_bm_test_race_reset();
    BMF_CHECK(query_bm_test_arm_keyset_race(stage) == 0, scenario_code,
              "arm race barrier");

    BmfRaceWriter w;
    memset(&w, 0, sizeof(w));
    w.db = p->db;
    w.o = &p->o;
    w.f0_value = f0_value;
    w.f1_value = f1_value;
    w.from = wfrom;
    w.past_cap = 1;
    atomic_init(&w.done, 0);
    atomic_init(&w.abort, 0);
    BMF_CHECK(pthread_create(&w.tid, NULL, bmf_race_writer_main, &w) == 0,
              scenario_code + 1, "writer thread start");

    *out_err = bmf_pdb_count(p, count_criteria, out_count);
    atomic_store(&w.abort, 1);
    pthread_join(w.tid, NULL);
    BMF_CHECK(query_bm_test_race_reached() == 1, scenario_code + 2,
              "barrier must be reached");
    BMF_CHECK(query_bm_test_race_allocated_cap() > 0, scenario_code + 3,
              "allocated capacity must be observed");
    BMF_CHECK(w.rc == 0, scenario_code + 4, "writer inserts must be accepted");
    BMF_CHECK(query_bm_test_race_gate_opened() == 1, scenario_code + 5,
              "gate must have been opened");
    BMF_CHECK(query_bm_test_race_insert_fails() > 0, scenario_code + 6,
              "insertion failures must be observed");
    *out_added = w.to - w.from + 1;
    query_bm_test_race_reset();
    return 0;
}

static int bmf_race_child(void) {
    BMF_CHECK(bmf_child_db != NULL, 1, "process db must exist");
    BmfPdb p;
    memset(&p, 0, sizeof(p));
    p.db = bmf_child_db;
    snprintf(p.root, sizeof(p.root), "%s", test_get_process_db_root());
    bmf_oracle_init(&p.o, p.root, "bmf_d8", "t8", 2200);
    BMF_CHECK(bmf_pdb_create(&p, "bmf_d8", "t8") == 0, 2, "create object");
    BMF_CHECK(bmf_pdb_seed(&p, 1, 2200, 550) == 0, 3, "seed");
    bmf_stage("setup-ok");

    long long c_f1_2_before = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]",
        &c_f1_2_before) == 0, 4, "pre count f1=2");
    long long c_neq_before = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f1\",\"op\":\"neq\",\"value\":\"2\"}]",
        &c_neq_before) == 0, 5, "pre count f1 neq 2");
    long long c_complement_before = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f0\",\"op\":\"neq\",\"value\":\"0\"}]",
        &c_complement_before) == 0, 6, "pre complement size");

    /* eq collector: overflow the sized KeySet with same-value additions. */
    long long got = -1;
    int err = 0;
    long added = 0;
    BMF_CHECK(bmf_race_scenario(&p, BMKS_RACE_EQ,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]", &got, &err,
        "0", "2", 300001, &added, 20) == 0, 26, "eq scenario milestones");
    BMF_CHECK(err == 0 && got == c_f1_2_before + added, 27,
              "eq overflow returns the complete frozen oracle");
    bmf_stage("eq-scenario-done");

    /* generic adapter: overflow with complementing additions. */
    got = -1; err = 0;
    BMF_CHECK(bmf_race_scenario(&p, BMKS_RACE_GENERIC,
        "[{\"field\":\"f1\",\"op\":\"neq\",\"value\":\"2\"}]", &got, &err,
        "0", "0", 400001, &added, 30) == 0, 36, "generic scenario milestones");
    BMF_CHECK(err == 0 && got == c_neq_before + added, 37,
              "generic overflow returns the complete frozen oracle");
    bmf_stage("generic-scenario-done");

    /* complement collector: direct smaller-side build; the added records
     * (f0=3) join the complement and overflow it. */
    query_bm_test_race_reset();
    BMF_CHECK(query_bm_test_arm_keyset_race(BMKS_RACE_COMPLEMENT) == 0, 40,
              "arm complement barrier");
    {
        BmfRaceWriter w;
        memset(&w, 0, sizeof(w));
        w.db = p.db;
        w.o = &p.o;
        w.f0_value = "3";
        w.f1_value = "0";
        w.from = 500001;
        w.past_cap = 1;
        atomic_init(&w.done, 0);
    atomic_init(&w.abort, 0);
        BMF_CHECK(pthread_create(&w.tid, NULL, bmf_race_writer_main, &w) == 0,
                  41, "writer thread start");
        /* The builder parks at the barrier between sizing and enumeration;
           the writer fills past the complement's capacity and opens the
           gate. */
        const char *jerr = NULL;
        CriteriaNode *tree = parse_criteria_tree(
            "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"}]", &jerr);
        BMF_CHECK(tree != NULL, 42, "criteria parse");
        CriteriaNode *leaf_node = tree;
        if (leaf_node->kind == CNODE_AND && leaf_node->n_children == 1)
            leaf_node = leaf_node->children[0];
        BMF_CHECK(leaf_node->kind == CNODE_LEAF, 48, "single criteria leaf");
        char eff_root[PATH_MAX];
        snprintf(eff_root, sizeof(eff_root), "%s/bmf_d8", p.root);
        Schema sc = load_schema(eff_root, "t8");
        TypedSchema *ts = load_typed_schema(eff_root, "t8");
        const TypedField *tf = resolve_idx_field(ts, "f0");
        QueryDeadline dl = { now_ms_coarse(), 0, 0 };
        int inverted = 0;
        KeySet *ks = build_smaller_bitmap_keyset(eff_root, "t8",
                                                 sc.splits, &leaf_node->leaf, tf,
                                                 &dl, &inverted);
        int build_errno = errno;
        if (ks) keyset_free(ks);
        free_criteria_tree(tree);
        atomic_store(&w.abort, 1);
        pthread_join(w.tid, NULL);
        BMF_CHECK(w.rc == 0, 43, "writer inserts must be accepted");
        BMF_CHECK(query_bm_test_race_reached() == 1, 44, "barrier reached");
        BMF_CHECK(query_bm_test_race_gate_opened() == 1, 45, "gate opened");
        BMF_CHECK(query_bm_test_race_insert_fails() > 0, 46,
                  "insertion failures observed");
        BMF_CHECK(c_complement_before > 0, 47, "complement has seeded members");
        BMF_CHECK(ks == NULL && build_errno == ENOBUFS, 49,
                  "low-level complement overflow reports capacity failure");
    }
    query_bm_test_race_reset();
    bmf_stage("complement-scenario-done");

    char drop[512], *resp = NULL;
    snprintf(drop, sizeof(drop),
             "{\"mode\":\"drop-object\",\"dir\":\"bmf_d8\",\"object\":\"t8\","
             "\"if_exists\":true}");
    BMF_CHECK(bmf_pdb_req(p.db, drop, &resp) == 0, 50, "drop object");
    bmf_pdb_free(resp);
    return 0;
}

static int test_bitmap_keyset_capacity_race_run(void) {
    BmfChildResult r;
    if (bmf_run_child("keyset-race", bmf_race_child, &r) != 1 ||
        r.ran != 1 || r.exit_code != 0 || r.timed_out || r.termsig) {
        TAP_DIAG("# keyset-race child: ran=%d exit=%d timed_out=%d termsig=%d\n%s",
                 r.ran, r.exit_code, r.timed_out, r.termsig, r.stages);
        ASSERT_TRUE(0, "0");
        return 1;
    }
    ASSERT_EQ_INT(0, r.exit_code, "child exits 0");
    ASSERT_TRUE(strstr(r.stages, "eq-scenario-done") != NULL, "strstr(r.stages, 'eq-scenario-done') != NULL");
    ASSERT_TRUE(strstr(r.stages, "generic-scenario-done") != NULL, "strstr(r.stages, 'generic-scenario-done') != NULL");
    ASSERT_TRUE(strstr(r.stages, "complement-scenario-done") != NULL, "strstr(r.stages, 'complement-scenario-done') != NULL");
    return 0;
}

/* ═══════════════════ Case 9: candidate budget ═══════════════════ */

/* Request budget unit checks (pure KeySet API, isolated child). */
typedef struct {
    KeySetBudget *budget;
    size_t        hint;
    atomic_int   *over;     /* set if the budget was ever over its limit */
    int           ok;
    int           rejected;
} BmfBudgetRacer;

static void *bmf_budget_racer_main(void *arg) {
    BmfBudgetRacer *r = (BmfBudgetRacer *)arg;
    for (int i = 0; i < 200; i++) {
        KeySet *ks = keyset_new_budgeted(r->hint, r->budget);
        if (atomic_load(&r->budget->used) > r->budget->limit)
            atomic_store(r->over, 1);
        if (ks) { r->ok++; keyset_free(ks); }
        else if (errno == ENOBUFS) r->rejected++;
        else atomic_store(r->over, 2);
    }
    return NULL;
}

static int bmf_budget_unit_child(void) {
    size_t b = 0;
    size_t per = sizeof(uint8_t[16]) + sizeof(uint32_t);
    /* Exact rounded sizing: 200 000 matches round to 524 288 slots. */
    BMF_CHECK(keyset_allocation_bytes(200000, &b) == 0, 1, "sizing 200000");
    BMF_CHECK(b == sizeof(KeySet) + 524288 * per, 2, "exact rounded charge");
    /* The example inequality: the hint estimate fits 8 MiB, the actual
       arrays do not. */
    BMF_CHECK((size_t)200000 * 2 * per <= (size_t)8 << 20, 3, "hint fits");
    BMF_CHECK(b > (size_t)8 << 20, 4, "actual exceeds");
    BMF_CHECK(keyset_allocation_bytes(0, &b) == 0 &&
              b == sizeof(KeySet) + 8 * per, 5, "minimum eight slots");
    /* Checked arithmetic near SIZE_MAX. */
    errno = 0;
    BMF_CHECK(keyset_allocation_bytes(SIZE_MAX / 2 + 1, &b) == -1 &&
              errno == EOVERFLOW, 6, "hint doubling overflow");
    errno = 0;
    BMF_CHECK(keyset_allocation_bytes(SIZE_MAX / 4, &b) == -1 &&
              errno == EOVERFLOW, 7, "byte multiplication overflow");
    BMF_CHECK(keyset_new(SIZE_MAX / 2 + 1) == NULL, 8, "keyset_new rejects overflow");

    /* Simultaneous charges and exact release. */
    size_t c1 = 0;
    BMF_CHECK(keyset_allocation_bytes(1000, &c1) == 0, 9, "sizing 1000");
    KeySetBudget bud = { .limit = 2 * c1 + c1 / 2 };
    atomic_init(&bud.used, 0);
    atomic_init(&bud.peak, 0);
    KeySet *a = keyset_new_budgeted(1000, &bud);
    KeySet *bset = keyset_new_budgeted(1000, &bud);
    BMF_CHECK(a && bset, 10, "two sets fit");
    BMF_CHECK(atomic_load(&bud.used) == 2 * c1, 11, "combined charge");
    errno = 0;
    BMF_CHECK(keyset_new_budgeted(1000, &bud) == NULL && errno == ENOBUFS, 12,
              "third set exceeds the combined budget");
    BMF_CHECK(atomic_load(&bud.used) == 2 * c1, 13, "rejection charges nothing");
    keyset_free(a);
    BMF_CHECK(atomic_load(&bud.used) == c1, 14, "free releases exactly once");
    keyset_free(bset);
    BMF_CHECK(atomic_load(&bud.used) == 0, 15, "all released");
    BMF_CHECK(atomic_load(&bud.peak) == 2 * c1, 16, "peak is the actual maximum");

    /* Allocation failure rolls the reservation back. */
    keyset_test_fail_budgeted_alloc_next(1);
    errno = 0;
    BMF_CHECK(keyset_new_budgeted(1000, &bud) == NULL && errno == ENOMEM, 17,
              "injected allocation failure");
    BMF_CHECK(atomic_load(&bud.used) == 0, 18, "reservation rolled back");

    /* Parallel reservations racing near the cap never exceed it. */
    KeySetBudget race = { .limit = 3 * c1 };
    atomic_init(&race.used, 0);
    atomic_init(&race.peak, 0);
    atomic_int over;
    atomic_init(&over, 0);
    BmfBudgetRacer rs[8];
    pthread_t tids[8];
    for (int i = 0; i < 8; i++) {
        rs[i] = (BmfBudgetRacer){ .budget = &race, .hint = 1000, .over = &over };
        BMF_CHECK(pthread_create(&tids[i], NULL, bmf_budget_racer_main, &rs[i]) == 0,
                  19, "racer start");
    }
    int ok = 0;
    for (int i = 0; i < 8; i++) { pthread_join(tids[i], NULL); ok += rs[i].ok; }
    BMF_CHECK(atomic_load(&over) == 0, 20, "budget never over its limit");
    BMF_CHECK(atomic_load(&race.peak) <= race.limit, 21, "peak within limit");
    BMF_CHECK(atomic_load(&race.used) == 0, 22, "racers released everything");
    BMF_CHECK(ok > 0, 23, "some reservations succeeded");
    bmf_stage("budget-unit-ok");
    return 0;
}

static int test_bitmap_candidate_budget_run(void) {
    BmfDaemon d;
    int failures = 0;
    {
        BmfChildResult r;
        if (bmf_run_child("budget-unit", bmf_budget_unit_child, &r) != 1 ||
            r.ran != 1 || r.exit_code != 0 || r.timed_out || r.termsig) {
            TAP_DIAG("# budget-unit child: exit=%d timed_out=%d\n%s",
                     r.exit_code, r.timed_out, r.stages);
            failures++;
        }
        ASSERT_TRUE(failures == 0, "exact sizing, simultaneous charges, rollback, racing reservations");
    }
    if (bmf_daemon_setup(&d, "8", "d9", "t9", 60000, 8, 2000) != 0) {
        TAP_DIAG("# setup failed\n");
        bmf_daemon_stop(&d);
        ASSERT_TRUE(0, "0");
        return 1;
    }

    /* Fitting control: intersect of two bitmap leaves whose source sets and
     * result coexist within the 8 MB buffer. */
    {
        long and_oracle = 0;
        for (long i = 1; i <= d.o.seeded; i++)
            if (!bmf_is_deleted(i) && bmf_f0_for(i) == 0 && bmf_f1_for(i) == 0)
                and_oracle++;
        char req[1024], *resp = NULL;
        char dir_part[64], obj_part[64];
        bmf_split_obj(d.o.obj_dir, dir_part, sizeof(dir_part),
                      obj_part, sizeof(obj_part));
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\","
                 "\"criteria\":{\"and\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"},"
                 "{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]},\"limit\":100000}",
                 dir_part, obj_part);
        int rc = bmf_req(&d, req, &resp);
        if (rc != 0) failures++;
        else {
            int is_err = bmf_is_error_object(resp, NULL, NULL);
            if (is_err == 1) {
                TAP_DIAG("# fitting intersect must be admitted\n");
                failures++;
            } else {
                char **keys = NULL; int n = 0;
                if (bmf_parse_find_keys(resp, &keys, &n) != 0) failures++;
                else {
                    if (n != and_oracle) failures++;
                    bmf_free_keys(keys, n);
                }
            }
        }
        free(resp);
        TAP_DIAG("# fitting control done (failures=%d)\n", failures);
    }

    /* Over-budget case: add 140000 records so each leaf's actual rounded
     * KeySet arrays exceed the 8 MB buffer even though the approximate hint
     * estimate still fits. The response must be the existing buffer error
     * with no response prefix, and the connection must stay healthy. */
    {
        long cursor = 500001;
        long to = 500000 + 140000;
        char *req = malloc((size_t)2000 * 220 + 512);
        ASSERT_TRUE(req != NULL, "req != NULL");
        BmfOracle add_o;
        memcpy(&add_o, &d.o, sizeof(add_o));
        while (cursor <= to) {
            int built = bmf_build_bulk_insert(&add_o, &cursor, to, 2000, 0,
                                              req, (size_t)2000 * 220 + 512);
            if (built <= 0) { failures++; break; }
            char *resp = NULL;
            int rc = bmf_req(&d, req, &resp);
            if (rc == 0) rc = bmf_validate_no_error(resp);
            free(resp);
            if (rc != 0) { failures++; break; }
        }
        free(req);
    }
    ASSERT_TRUE(failures == 0, "failures == 0");
    if (failures != 0) { bmf_daemon_stop(&d); return 1; }

    {
        char req[1024], *resp = NULL;
        char dir_part[64], obj_part[64];
        bmf_split_obj(d.o.obj_dir, dir_part, sizeof(dir_part),
                      obj_part, sizeof(obj_part));
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\","
                 "\"criteria\":{\"and\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"},"
                 "{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]},\"limit\":100000}",
                 dir_part, obj_part);
        int rc = bmf_req(&d, req, &resp);
        if (rc != 0) {
            TAP_DIAG("# over-budget request failed rc=%d\n", rc);
            failures++;
        } else if (bmf_validate_error_prefix(resp, BMF_ERR_QUERY_BUFFER) != 0) {
            int is_err = bmf_is_error_object(resp, NULL, NULL);
            if (is_err == 1)
                TAP_DIAG("# over-budget candidates must be rejected with the buffer error\n");
            else
                TAP_DIAG("# over-budget candidates were silently admitted\n");
            failures++;
        }
        free(resp);
    }
    /* Healthy follow-up on the same connection (oracle covers the seeded
     * range plus the added records, minus deletions). */
    {
        long f1_1_oracle = 0;
        for (long i = 1; i <= d.o.seeded; i++)
            if (!bmf_is_deleted(i) && bmf_f1_for(i) == 1) f1_1_oracle++;
        for (long i = 500001; i <= 500000 + 140000; i++)
            if (bmf_f1_for(i) == 1) f1_1_oracle++;
        failures += bmf_check_count(&d, &d.o,
            "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"1\"}]", f1_1_oracle,
            BMF_WANT_EXACT, "healthy follow-up");
    }

    ASSERT_TRUE(failures == 0, "failures == 0");
    bmf_daemon_stop(&d);
    return failures != 0;
}

/* ═══════════════════ Case 10: candidate floor + capacity recovery ═══════════════════ */

/* Sizing policy boundary table (R1): the preferred actual slot count for an
 * initial distinct-match estimate m is next_power_of_two(max(8192, 2*m)). */
static const struct { size_t m; size_t want_slots; } bmf_floor_table[] = {
    { 0, 8192 }, { 10, 8192 }, { 1000, 8192 }, { 4096, 8192 },
    { 4097, 16384 }, { 5000, 16384 }, { 100000, 262144 },
};

/* In-process: preferred sizing + the sizing barrier (locks released), an
 * initially small candidate set absorbing a concurrent burst below the
 * floor, and the frozen-write oracle comparisons. */
static int bmf_total_value(const char *resp, long long *out) {
    BmfStrictJsonObject obj = {0};
    if (!resp) return -1;
    const char *end = bmf_strict_json_object(bmf_strict_json_ws(resp), 0, &obj);
    int rc = -1;
    if (end && *bmf_strict_json_ws(end) == '\0') {
        for (size_t i = 0; i < obj.count; i++) {
            if (strcmp(obj.members[i].name, "total") != 0) continue;
            size_t len = (size_t)(obj.members[i].end - obj.members[i].value);
            char *value = strndup(obj.members[i].value, len);
            if (value) { rc = bmf_parse_bare_int(value, out); free(value); }
        }
    }
    bmf_strict_json_object_free(&obj);
    return rc;
}

static int bmf_floor_postfilter_totals(BmfPdb *p) {
    long expect = 0;
    for (long i = 1; i <= p->o.seeded; i++)
        if (bmf_f1_for(i) == 0) expect++;
    size_t one_set = 0;
    BMF_CHECK(keyset_allocation_bytes(4096, &one_set) == 0, 80, "exact floor charge");
    const char *formats[] = {"", ",\"format\":\"rows\"", ""};
    const int limits[] = {1100, 1100, 1};
    for (size_t i = 0; i < 3; i++) {
        char req[1024], *resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"bmf_d10\",\"object\":\"t10\","
            "\"criteria\":{\"and\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"},"
            "{\"field\":\"pay\",\"op\":\"starts\",\"value\":\"p\"}]},"
            "\"limit\":%d,\"total\":true%s}", limits[i], formats[i]);
        query_test_prepared_budget_limit(one_set);
        int rc = bmf_pdb_req(p->db, req, &resp);
        query_test_prepared_budget_limit(0);
        long long total = -1;
        int parsed = bmf_total_value(resp, &total);
        if (rc != 0 || parsed != 0 || total != expect) {
            char diagnostic[600];
            snprintf(diagnostic, sizeof(diagnostic),
                     "postfilter-total shape=%zu rc=%d parsed=%d got=%lld expected=%ld response=%.320s",
                     i, rc, parsed, total, expect, resp ? resp : "(null)");
            bmf_stage(diagnostic);
        }
        bmf_pdb_free(resp);
        BMF_CHECK(rc == 0 && parsed == 0 && total == expect, 81 + (int)i,
                  "total filters the prepared source through the complete criteria");
    }
    bmf_stage("postfilter-total-one-set-budget-ok");
    return 0;
}

static int bmf_floor_race_child(void) {
    BMF_CHECK(bmf_child_db != NULL, 1, "process db must exist");
    BmfPdb p;
    memset(&p, 0, sizeof(p));
    p.db = bmf_child_db;
    snprintf(p.root, sizeof(p.root), "%s", test_get_process_db_root());
    bmf_oracle_init(&p.o, p.root, "bmf_d10", "t10", 100);
    BMF_CHECK(bmf_pdb_create(&p, "bmf_d10", "t10") == 0, 2, "create object");
    BMF_CHECK(bmf_pdb_seed(&p, 1, 100, 100) == 0, 3, "seed");

    /* Sizing policy: preferred actual slots per the boundary table. */
    for (size_t i = 0; i < sizeof(bmf_floor_table) / sizeof(bmf_floor_table[0]);
         i++) {
        size_t slots = 0;
        KeySet *ks = query_test_candidate_keyset_alloc(bmf_floor_table[i].m,
                                                       &slots);
        BMF_CHECK(slots == bmf_floor_table[i].want_slots, 10 + (int)i,
                  "preferred sizing table row");
        BMF_CHECK(ks != NULL, 20 + (int)i, "floor allocation admitted");
        if (ks) keyset_free(ks);
    }
    bmf_stage("sizing-table-ok");

    /* Race: the sizing barrier parks the count after its sizing pass with
     * every lock released; a concurrent burst brings the stable matching
     * total to 2,549 records; writes freeze before the gate releases;
     * enumeration then succeeds inside the 8,192-slot floor. */
    long long pre = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]", &pre) == 0, 29,
        "pre-burst count");
    BMF_CHECK(pre >= 0 && pre < 2549, 29, "pre-burst under the 2,549 target");
    long long burst = 2549 - pre;
    query_test_candidate_observe(1);
    BMF_CHECK(query_bm_test_arm_keyset_race(BMKS_RACE_EQ) == 0, 30,
              "arm race barrier");
    BmfRaceWriter w;
    memset(&w, 0, sizeof(w));
    w.db = p.db;
    w.o = &p.o;
    w.f0_value = "0";
    w.f1_value = "0";
    w.from = 100001;
    w.to = 100000 + burst;
    atomic_init(&w.done, 0);
    atomic_init(&w.abort, 0);
    BMF_CHECK(pthread_create(&w.tid, NULL, bmf_race_writer_main, &w) == 0, 31,
              "writer thread start");

    long long got = -1;
    int err = bmf_pdb_count(&p,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]", &got);
    atomic_store(&w.abort, 1);
    pthread_join(w.tid, NULL);
    BMF_CHECK(query_bm_test_race_reached() == 1, 32, "barrier reached");
    BMF_CHECK(w.rc == 0, 33, "writer inserts accepted");
    BMF_CHECK(query_bm_test_race_gate_opened() == 1, 34, "gate opened");
    BMF_CHECK(err == 0 && got == 2549, 35,
              "count equals the frozen 2,549-record oracle");
    unsigned last_slots = 0, overflows = 0, fallbacks = 0, peak = 0;
    query_test_candidate_stats(&last_slots, &overflows, &fallbacks, &peak);
    BMF_CHECK(last_slots == 8192, 36,
              "8,192-slot floor actually admitted (not fallback)");
    BMF_CHECK(overflows == 0, 37, "no overflow below the floor");
    BMF_CHECK(fallbacks == 0, 38, "no fallback below the floor");

    /* Frozen-write membership oracle on find, and aggregate buckets. */
    char *resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"find\",\"dir\":\"bmf_d10\",\"object\":\"t10\","
        "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}],"
        "\"limit\":5000}", &resp) == 0, 39, "find round-trip");
    char **keys = NULL; int n = 0;
    BMF_CHECK(bmf_parse_find_keys(resp, &keys, &n) == 0 && n == 2549, 40,
              "find membership cardinality after freeze (2,549)");
    bmf_free_keys(keys, n);
    bmf_pdb_free(resp);
    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"aggregate\",\"dir\":\"bmf_d10\",\"object\":\"t10\","
        "\"aggregates\":[{\"fn\":\"count\"}],\"group_by\":\"f1\"}",
        &resp) == 0, 41, "aggregate round-trip");
    char labels[8][8];
    long counts[8];
    int nb = 0;
    BMF_CHECK(bmf_parse_group_counts(resp, "f1", labels, counts, 8, &nb) == 0 &&
              nb == 4, 42, "aggregate buckets parse");
    for (int v = 0; v < 4; v++) {
        char want[8];
        snprintf(want, sizeof(want), "%d", v);
        long expect = 0;
        for (long i = 1; i <= p.o.seeded; i++)
            if (bmf_f1_for(i) == v) expect++;
        for (long i = 100001; i <= 100000 + burst; i++)
            if (v == 0) expect++;   /* writer records carry f1="0" */
        int found = 0;
        for (int b = 0; b < nb; b++)
            if (strcmp(labels[b], want) == 0) { found = 1;
                BMF_CHECK(counts[b] == expect, 50 + v, "bucket oracle"); }
        BMF_CHECK(found, 60 + v, "bucket present");
    }
    bmf_pdb_free(resp);
    BMF_CHECK(bmf_floor_postfilter_totals(&p) == 0, 79, "postfilter totals");
    query_test_candidate_observe(0);

    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"drop-object\",\"dir\":\"bmf_d10\",\"object\":\"t10\","
        "\"if_exists\":true}", &resp) == 0, 70, "drop object");
    bmf_pdb_free(resp);
    return 0;
}

/* In-process: force overflow beyond the OBSERVED actual capacity, then the
 * frozen-write oracle across count/find/aggregate and positive/generic/
 * complement/AND/OR consumers, with exactly one whole-record recovery. */
static int bmf_floor_overflow_child(void) {
    BMF_CHECK(bmf_child_db != NULL, 1, "process db must exist");
    BmfPdb p;
    memset(&p, 0, sizeof(p));
    p.db = bmf_child_db;
    snprintf(p.root, sizeof(p.root), "%s", test_get_process_db_root());
    bmf_oracle_init(&p.o, p.root, "bmf_d10", "t10o", 5000);
    BMF_CHECK(bmf_pdb_create(&p, "bmf_d10", "t10o") == 0, 2, "create object");
    BMF_CHECK(bmf_pdb_seed(&p, 1, 5000, 500) == 0, 3, "seed 5000");

    /* --- positive eq: barrier after sizing, burst past the observed
     * actual capacity even with the floor. m0 = matches of f1=="0" before
     * the burst; the policy admits next_pow2(max(8192, 2*m0)) slots and the
     * 12,000-record burst exceeds that. */
    long long m0 = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]", &m0) == 0, 9,
        "pre-burst count");
    BMF_CHECK(m0 > 0 && (unsigned long long)m0 * 2 <= 8192, 9,
              "pre-burst sizing sits under the floor");
    query_test_candidate_observe(1);
    BMF_CHECK(query_bm_test_arm_keyset_race(BMKS_RACE_EQ) == 0, 10,
              "arm eq barrier");
    BmfRaceWriter w;
    memset(&w, 0, sizeof(w));
    w.db = p.db;
    w.o = &p.o;
    w.f0_value = "0";
    w.f1_value = "0";
    w.from = 200001;
    w.to = 200000 + 12000;
    atomic_init(&w.done, 0);
    atomic_init(&w.abort, 0);
    BMF_CHECK(pthread_create(&w.tid, NULL, bmf_race_writer_main, &w) == 0, 11,
              "writer thread start");
    long long got = -1;
    int err = bmf_pdb_count(&p,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]", &got);
    atomic_store(&w.abort, 1);
    pthread_join(w.tid, NULL);
    BMF_CHECK(query_bm_test_race_reached() == 1, 12, "eq barrier reached");
    BMF_CHECK(w.rc == 0, 13, "writer inserts accepted");
    BMF_CHECK(query_bm_test_race_gate_opened() == 1, 14, "gate opened");
    unsigned last_slots = 0, overflows = 0, fallbacks = 0, peak = 0;
    query_test_candidate_stats(&last_slots, &overflows, &fallbacks, &peak);
    BMF_CHECK(last_slots == 8192, 15, "floor capacity 8192 actually admitted");
    BMF_CHECK(overflows > 0, 16, "overflow actually observed");
    BMF_CHECK(err == 0 && got == m0 + 12000, 17,
              "count recovered once to the frozen oracle");
    BMF_CHECK(fallbacks == 1, 18, "exactly one record fallback");

    /* Frozen find membership + AND intersect + OR union oracles (writes are
     * frozen; no snapshot-isolation claim is made or needed). */
    char *resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"find\",\"dir\":\"bmf_d10\",\"object\":\"t10o\","
        "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}],"
        "\"limit\":20000}", &resp) == 0, 19, "find round-trip");
    char **keys = NULL; int n = 0;
    BMF_CHECK(bmf_parse_find_keys(resp, &keys, &n) == 0 && n == m0 + 12000, 20,
              "find membership cardinality");
    bmf_free_keys(keys, n);
    bmf_pdb_free(resp);
    got = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "{\"and\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"},"
        "{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]}", &got) == 0, 21,
        "AND count round-trip");
    {
        long and_oracle = 0;
        for (long i = 200001; i <= 200000 + 12000; i++) and_oracle++;
        for (long i = 1; i <= p.o.seeded; i++)
            if (bmf_f0_for(i) == 0 && bmf_f1_for(i) == 0) and_oracle++;
        BMF_CHECK(got == and_oracle, 22, "AND count oracle");
    }
    got = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "{\"or\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"},"
        "{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]}", &got) == 0, 23,
        "OR count round-trip");
    {
        long oracle = 0;
        for (long i = 1; i <= p.o.seeded; i++)
            if (bmf_f0_for(i) == 1 || bmf_f1_for(i) == 2) oracle++;
        BMF_CHECK(got == oracle, 24, "OR count oracle (unchanged range)");
    }

    /* --- generic (neq) source overflow with its own barrier burst. */
    long long neq_before = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f0\",\"op\":\"neq\",\"value\":\"0\"}]", &neq_before) == 0,
        25, "neq pre count");
    query_test_candidate_observe(1);
    BMF_CHECK(query_bm_test_arm_keyset_race(BMKS_RACE_GENERIC) == 0, 26,
              "arm generic barrier");
    memset(&w, 0, sizeof(w));
    w.db = p.db;
    w.o = &p.o;
    w.f0_value = "3";
    w.f1_value = "3";
    w.from = 300001;
    w.past_cap = 1;   /* freeze a burst larger than the observed allocation */
    atomic_init(&w.done, 0);
    atomic_init(&w.abort, 0);
    BMF_CHECK(pthread_create(&w.tid, NULL, bmf_race_writer_main, &w) == 0, 27,
              "generic writer start");
    got = -1;
    err = bmf_pdb_count(&p,
        "[{\"field\":\"f0\",\"op\":\"neq\",\"value\":\"0\"}]", &got);
    atomic_store(&w.abort, 1);
    pthread_join(w.tid, NULL);
    BMF_CHECK(query_bm_test_race_reached() == 1, 28, "generic barrier reached");
    BMF_CHECK(query_bm_test_race_gate_opened() == 1, 29, "generic gate opened");
    query_test_candidate_stats(&last_slots, &overflows, &fallbacks, &peak);
    BMF_CHECK(overflows > 0, 30, "generic overflow observed");
    BMF_CHECK(err == 0 && got == neq_before + w.to - w.from + 1, 31,
              "generic count recovered to the frozen oracle");
    BMF_CHECK(fallbacks == 1, 32, "exactly one generic record fallback");

    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"add-index\",\"dir\":\"bmf_d10\",\"object\":\"t10o\","
        "\"field\":\"f1:btree\"}", &resp) == 0, 33, "build top-N group btree");
    {
        int ok = bmf_validate_no_error(resp) == 0;
        bmf_pdb_free(resp);
        BMF_CHECK(ok, 33, "top-N group btree accepted");
    }

    /* --- complement source: majority criteria through the aggregate top-N
     * consumer. Overflow in the complement build must end in exactly one
     * whole-record aggregate fallback starting from empty groups. */
    query_test_candidate_observe(1);
    BMF_CHECK(query_bm_test_arm_keyset_race(BMKS_RACE_COMPLEMENT) == 0, 33,
              "arm complement barrier");
    memset(&w, 0, sizeof(w));
    w.db = p.db;
    w.o = &p.o;
    w.f0_value = "3";
    w.f1_value = "0";
    w.from = 400001;
    w.past_cap = 1;   /* complement must exceed its observed allocation */
    atomic_init(&w.done, 0);
    atomic_init(&w.abort, 0);
    BMF_CHECK(pthread_create(&w.tid, NULL, bmf_race_writer_main, &w) == 0, 34,
              "complement writer start");
    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"aggregate\",\"dir\":\"bmf_d10\",\"object\":\"t10o\","
        "\"aggregates\":[{\"fn\":\"count\"}],"
        "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"}],"
        "\"group_by\":\"f1\",\"order_by\":\"count\",\"limit\":1000}",
        &resp) == 0, 35, "complement aggregate round-trip");
    char labels[8][8];
    long counts[8];
    int nb = 0;
    BMF_CHECK(bmf_parse_group_counts(resp, "f1", labels, counts, 8, &nb) == 0,
              36, "aggregate buckets parse");
    bmf_pdb_free(resp);
    pthread_join(w.tid, NULL);
    BMF_CHECK(query_bm_test_race_reached() == 1, 37,
              "complement barrier reached");
    query_test_candidate_stats(&last_slots, &overflows, &fallbacks, &peak);
    BMF_CHECK(overflows > 0, 38, "complement overflow observed");
    BMF_CHECK(fallbacks == 1, 39, "exactly one aggregate record fallback");
    for (int v = 0; v < 4; v++) {
        char want[8];
        snprintf(want, sizeof(want), "%d", v);
        long expect = 0;
        for (long i = 1; i <= p.o.seeded; i++)
            if (bmf_f0_for(i) == 0 && bmf_f1_for(i) == v) expect++;
        if (v == 0) expect += 12000;
        /* Both later bursts carry f0="3", outside the original criterion. */
        int found = 0;
        for (int b = 0; b < nb; b++)
            if (strcmp(labels[b], want) == 0) {
                found = 1;
                if (counts[b] != expect) {
                    char msg[128];
                    snprintf(msg, sizeof(msg),
                             "FAIL complement bucket %s: %ld != %ld",
                             want, counts[b], expect);
                    bmf_stage(msg);
                    return 40;
                }
            }
        BMF_CHECK(found, 41 + v, "complement bucket present");
    }
    query_test_candidate_observe(0);

    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"drop-object\",\"dir\":\"bmf_d10\",\"object\":\"t10o\","
        "\"if_exists\":true}", &resp) == 0, 50, "drop object");
    bmf_pdb_free(resp);
    return 0;
}

/* Budget, hard-error, timeout and mixed-leaf scenarios (R1). The budget
 * override seam is TEST_BUILD-only test support; production keeps the
 * query-buffer default. */
static int bmf_floor_budget_child(void) {
    BMF_CHECK(bmf_child_db != NULL, 1, "process db must exist");
    BmfPdb p;
    memset(&p, 0, sizeof(p));
    p.db = bmf_child_db;
    snprintf(p.root, sizeof(p.root), "%s", test_get_process_db_root());
    bmf_oracle_init(&p.o, p.root, "bmf_d10", "t10b", 200);
    BMF_CHECK(bmf_pdb_create(&p, "bmf_d10", "t10b") == 0, 2, "create object");
    BMF_CHECK(bmf_pdb_seed(&p, 1, 200, 200) == 0, 3, "seed");

    {
        char effective[PATH_MAX];
        snprintf(effective, sizeof(effective), "%s/bmf_d10", p.root);
        Schema sch = load_schema(effective, "t10b");
        FieldSchema fs;
        init_field_schema(&fs, effective, "t10b");
        const char *error = NULL;
        CriteriaNode *tree = parse_criteria_tree(
            "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]", &error);
        BMF_CHECK(tree != NULL, 4, "deadline criteria parse");
        char compile_error[256] = {0};
        BMF_CHECK(compile_criteria_tree_checked(tree, fs.ts, compile_error,
                                                sizeof(compile_error)) == 0,
                  4, "deadline criteria compile");
        long long expected = -1;
        BMF_CHECK(bmf_pdb_count(&p,
            "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]",
            &expected) == 0 && expected > 0, 5, "deadline positive oracle");
        QueryDeadline fresh = { now_ms_coarse(), 0, 0 };
        size_t count = 0;
        int good = count_full_record_checked(effective, "t10b", tree, &fs,
                                             &sch, &fresh, &count);
        if (good != 0 || count != (size_t)expected) {
            char diagnostic[256];
            snprintf(diagnostic, sizeof(diagnostic),
                     "fresh-count rc=%d got=%zu expected=%lld errno=%d",
                     good, count, expected, errno);
            bmf_stage(diagnostic);
        }
        BMF_CHECK(good == 0 && count == (size_t)expected, 6,
                  "fresh full-record scan evaluates original criteria");
        QueryDeadline elapsed = { now_ms_coarse() - 100, 1, 0 };
        count = SIZE_MAX;
        int expired = count_full_record_checked(effective, "t10b", tree, &fs,
                                                &sch, &elapsed, &count);
        free_criteria_tree(tree);
        BMF_CHECK(expired == 0 && count == 0 && elapsed.timed_out, 7,
                  "expired fallback never starts a short scan");
        bmf_stage("expired-scan-entry-ok");
    }

    /* Shared-budget: floor does not fit the remaining budget, ordinary does.
     * Floor charge for m=100: 8192 slots; ordinary: 256 slots. A limit
     * between them refuses only the floor. */
    size_t per = sizeof(uint8_t[16]) + sizeof(uint32_t) + sizeof(KeySet);
    size_t floor_charge = sizeof(KeySet) + 8192 * per;
    size_t ordinary_charge = sizeof(KeySet) + 256 * per;
    (void)floor_charge; (void)ordinary_charge;
    query_test_candidate_observe(1);
    query_test_prepared_budget_limit(ordinary_charge + 64);
    char *resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"find\",\"dir\":\"bmf_d10\",\"object\":\"t10b\","
        "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}],"
        "\"limit\":1}", &resp) == 0, 10, "budgeted find round-trip");
    char **keys = NULL; int n = 0;
    BMF_CHECK(bmf_parse_find_keys(resp, &keys, &n) == 0 && n == 1, 11,
              "ordinary sizing succeeds under a floor-refusing budget");
    bmf_free_keys(keys, n);
    bmf_pdb_free(resp);
    unsigned last_slots = 0, overflows = 0, fallbacks = 0, peak = 0;
    query_test_candidate_stats(&last_slots, &overflows, &fallbacks, &peak);
    BMF_CHECK(fallbacks == 0, 12, "no fallback: ordinary sizing is not overflow");
    query_test_prepared_budget_limit(0);

    /* Neither size fits: the existing pre-emission buffer error. */
    query_test_prepared_budget_limit(1);
    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"find\",\"dir\":\"bmf_d10\",\"object\":\"t10b\","
        "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}],"
        "\"limit\":1}", &resp) == 0, 13, "no-fit find round-trip");
    BMF_CHECK(bmf_validate_error_prefix(resp, BMF_ERR_QUERY_BUFFER) == 0, 14,
              "neither size fitting keeps the buffer error");
    bmf_pdb_free(resp);
    query_test_prepared_budget_limit(0);

    /* True allocation failure: reservation rolled back, hard error, not a
     * fallback and not a budget error. */
    query_test_prepared_budget_limit((size_t)1 << 30);
    keyset_test_fail_budgeted_alloc_next(1);
    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"find\",\"dir\":\"bmf_d10\",\"object\":\"t10b\","
        "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}],"
        "\"limit\":1}", &resp) == 0, 15, "alloc-failure find round-trip");
    BMF_CHECK(bmf_validate_no_error(resp) != 0, 16,
              "allocation failure is an error, not a fallback");
    bmf_pdb_free(resp);
    query_test_candidate_stats(&last_slots, &overflows, &fallbacks, &peak);
    BMF_CHECK(fallbacks == 0, 17, "allocation failure is not a fallback");
    query_test_candidate_observe(0);

    /* Three workers encounter distinct outcomes. The exact lock fault is
     * confined to f2's worker; f1 releases its sizing locks before the
     * checked writer grows beyond its observed allocation. */
    {
        BMF_CHECK(parallel_pool_size() == 0, 40, "isolated child starts without a pool");
        parallel_pool_init(4);
        BMF_CHECK(parallel_pool_size() == 4, 41, "four actual pool workers");
        const char *error = NULL;
        CriteriaNode *tree = parse_criteria_tree(
            "{\"and\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"},"
            "{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"},"
            "{\"field\":\"f2\",\"op\":\"eq\",\"value\":\"1\"}]}", &error);
        BMF_CHECK(tree && tree->kind == CNODE_AND && tree->n_children == 3,
                  42, "three indexed leaves parse");
        SearchCriterion *leaves[3];
        for (int i = 0; i < 3; i++) {
            BMF_CHECK(tree->children[i]->kind == CNODE_LEAF, 43, "parallel leaf shape");
            leaves[i] = &tree->children[i]->leaf;
        }
        BMF_CHECK(query_test_parallel_candidate_race("f1", BMKS_RACE_EQ) == 0,
                  44, "target f1 worker barrier");
        BMF_CHECK(query_test_parallel_candidate_fault("f2", BM_TEST_LOCK_HIT_RD, EIO) == 0,
                  45, "target f2 real lock fault");
        query_test_candidate_observe(1);
        BmfRaceWriter w = {0};
        w.db = p.db; w.o = &p.o;
        w.f0_value = "0"; w.f1_value = "0";
        w.from = 700001; w.past_cap = 1;
        atomic_init(&w.done, 0); atomic_init(&w.abort, 0);
        BMF_CHECK(pthread_create(&w.tid, NULL, bmf_race_writer_main, &w) == 0,
                  46, "mixed writer start");
        char effective[PATH_MAX];
        snprintf(effective, sizeof(effective), "%s/bmf_d10", p.root);
        Schema sch = load_schema(effective, "t10b");
        QueryDeadline dl = { now_ms_coarse(), 0, 0 };
        KeySetBudget budget = { .limit = (size_t)32 << 20 };
        atomic_init(&budget.used, 0);
        atomic_init(&budget.peak, 0);
        CandidateBuildOut outcome;
        int small_primary = 0;
        KeySet *ks = intersect_indexed_leaves_ck(effective, "t10b", sch.splits,
            leaves, 3, &dl, &small_primary, &budget, &outcome);
        atomic_store(&w.abort, 1);
        pthread_join(w.tid, NULL);
        unsigned workers = 0, joined = 0, capacity = 0, hard = 0, offthread = 0, consumed = 0;
        query_test_parallel_candidate_stats(&workers, &joined, &capacity, &hard,
                                            &offthread, &consumed);
        if (ks) keyset_free(ks);
        free_criteria_tree(tree);
        query_test_parallel_candidate_race(NULL, 0);
        query_test_parallel_candidate_fault(NULL, 0, 0);
        parallel_pool_shutdown();
        BMF_CHECK(w.rc == 0 && query_bm_test_race_reached() &&
                  query_bm_test_race_gate_opened(), 47, "mixed writer and barrier consumed");
        BMF_CHECK(workers == 3 && joined == 3 && offthread > 0, 48,
                  "all three real worker outcomes joined before cleanup");
        BMF_CHECK(capacity > 0 && hard > 0 && consumed == 1, 49,
                  "capacity and exact hard lock fault both encountered");
        BMF_CHECK(ks == NULL && outcome.reason == CBF_HARD &&
                  outcome.saved_errno == EIO, 50, "hard outcome wins over capacity");
        BMF_CHECK(atomic_load(&budget.used) == 0, 51,
                  "all parallel candidate reservations released");
        unsigned last = 0, over = 0, fallbacks = 0, peak = 0;
        query_test_candidate_stats(&last, &over, &fallbacks, &peak);
        BMF_CHECK(fallbacks == 0, 52, "hard failure cannot become capacity recovery");
        query_test_candidate_observe(0);
        bmf_stage("parallel-mixed-hard-precedence-cleanup-ok");
    }

    /* Hard read failure (damaged bitmap): retained error behavior. */
    {
        int shard = -1;
        BMF_CHECK(bmf_shard_by_live(&p.o, 0, &shard) == 0, 20, "target shard");
        char target_bm[PATH_MAX];
        bmf_bm_path(target_bm, sizeof(target_bm), &p.o, "f0", shard);
        BMF_CHECK(bm_test_damage_header_bounds(target_bm) == 0, 21, "damage");
        /* create-object preopened every shard, so probe the damaged file
         * with the process-local cache explicitly down (isolated child). */
        bm_cache_shutdown();
        errno = 0;
        BitmapShard *probe = bm_open(target_bm, 0, 0, 0, 0, 0);
        if (probe) { bm_close(probe); }
        BMF_CHECK(probe == NULL && errno == EUCLEAN, 22,
                  "damaged bitmap cold-opens EUCLEAN");
        bm_cache_init(1024);
        long long got = -1;
        BMF_CHECK(bmf_pdb_count(&p,
            "[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"}]", &got) != 0, 23,
            "hard read failure stays an error");
        resp = NULL;
        BMF_CHECK(bmf_pdb_req(p.db,
            "{\"mode\":\"find\",\"dir\":\"bmf_d10\",\"object\":\"t10b\","
            "\"criteria\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"0\"}],"
            "\"limit\":1}", &resp) == 0, 24, "hard find round-trip");
        BMF_CHECK(bmf_validate_error_exact(resp, "bitmap index unavailable") == 0,
                  25, "hard read failure is not a fallback");
        bmf_pdb_free(resp);
    }

    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"drop-object\",\"dir\":\"bmf_d10\",\"object\":\"t10b\","
        "\"if_exists\":true}", &resp) == 0, 50, "drop object");
    bmf_pdb_free(resp);
    return 0;
}

/* Daemon scenario (R1): sizing barrier parked through the find-flush gate
 * hook, writes frozen before release, oracle comparisons on the wire, log
 * needles for admission and (second scenario) the capacity recovery. */
typedef struct {
    TestEnv        *env;
    const char     *req;
    char          **resp;
    pthread_t       tid;
    TuJoinSignal    js;
    int             rc;
} BmfAsyncReq;

static void *bmf_async_req_main(void *arg) {
    BmfAsyncReq *r = (BmfAsyncReq *)arg;
    TestClientCfg cfg = { .port = r->env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { r->rc = -1; tu_join_signal_mark_done(&r->js); return NULL; }
    r->rc = tc_request(tc, r->req, r->resp);
    tc_close(tc);
    tu_join_signal_mark_done(&r->js);
    return NULL;
}

typedef struct {
    TestEnv      *env;
    const char   *f0_value;
    const char   *f1_value;
    long          from;
    long          to;
    const char   *damage_rel;  /* unused on the wire path */
    pthread_t     tid;
    TuJoinSignal  js;
    int           rc;
} BmfWireWriter;

static void *bmf_wire_writer_main(void *arg) {
    BmfWireWriter *w = (BmfWireWriter *)arg;
    TestClientCfg cfg = { .port = w->env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { w->rc = 8; tu_join_signal_mark_done(&w->js); return NULL; }
    char *req = malloc(1 << 19);
    if (!req) { w->rc = 9; tc_close(tc); tu_join_signal_mark_done(&w->js); return NULL; }
    long cursor = w->from;
    int rc = 0;
    while (cursor <= w->to && rc == 0) {
        size_t off = (size_t)snprintf(req, 1 << 19,
            "{\"mode\":\"bulk-insert\",\"dir\":\"d10w\",\"object\":\"t10w\","
            "\"records\":{");
        long end = cursor + 299 > w->to ? w->to : cursor + 299;
        for (long j = cursor; j <= end; j++) {
            char key[32];
            snprintf(key, sizeof(key), "k%06ld", j);
            int n = snprintf(req + off, (size_t)(1 << 19) - off,
                "%s\"%s\":{\"id\":\"%ld\",\"f0\":\"%s\",\"f1\":\"%s\","
                "\"pay\":\"w\"}", j > cursor ? "," : "", key, j,
                w->f0_value, w->f1_value);
            if (n < 0 || (size_t)n >= (size_t)(1 << 19) - off) { rc = 10; break; }
            off += (size_t)n;
        }
        if (rc) break;
        int n = snprintf(req + off, (size_t)(1 << 19) - off, "}}");
        if (n < 0 || (size_t)n >= (size_t)(1 << 19) - off) { rc = 10; break; }
        char *resp = NULL;
        if (tc_request(tc, req, &resp) != 0) { rc = 11; break; }
        if (bmf_validate_no_error(resp) != 0) rc = 12;
        free(resp);
        cursor = end + 1;
    }
    free(req);
    tc_close(tc);
    w->rc = rc;
    tu_join_signal_mark_done(&w->js);
    return NULL;
}

static int bmf_parser_selfcheck(void);

static int bmf_floor_join(pthread_t tid, TuJoinSignal *js, int seconds) {
    int rc = tu_timed_join(tid, js, seconds);
    if (rc == 0) {
        pthread_cond_destroy(&js->cond);
        pthread_mutex_destroy(&js->lock);
    }
    return rc;
}

static int bmf_floor_run_child(void) {
    BMF_CHECK(bmf_parser_selfcheck() == 0, 119, "parser controls");
    BmfChildResult r;
    int failures = 0;

    if (bmf_run_child("floor-race", bmf_floor_race_child, &r) != 1 ||
        r.ran != 1 || r.exit_code != 0 || r.timed_out || r.termsig) {
        TAP_DIAG("# floor-race child: exit=%d timed_out=%d\n%s",
                 r.exit_code, r.timed_out, r.stages);
        failures++;
    } else {
        ASSERT_TRUE(strstr(r.stages, "sizing-table-ok") != NULL,
                    "preferred sizing boundary table holds");
    }

    if (bmf_run_child("floor-overflow", bmf_floor_overflow_child, &r) != 1 ||
        r.ran != 1 || r.exit_code != 0 || r.timed_out || r.termsig) {
        TAP_DIAG("# floor-overflow child: exit=%d timed_out=%d\n%s",
                 r.exit_code, r.timed_out, r.stages);
        failures++;
    }

    if (bmf_run_child("floor-budget", bmf_floor_budget_child, &r) != 1 ||
        r.ran != 1 || r.exit_code != 0 || r.timed_out || r.termsig) {
        TAP_DIAG("# floor-budget child: exit=%d timed_out=%d\n%s",
                 r.exit_code, r.timed_out, r.stages);
        failures++;
    }

    /* ─── Daemon scenario: barrier + freeze + wire oracles ─── */
    BmfDaemon d;
    int dsrc = bmf_daemon_setup(&d, NULL, "d10w", "t10w", 100, 64, 100);
    if (dsrc != 0) {
        TAP_DIAG("# daemon setup failed rc=%d\n", dsrc);
        bmf_daemon_stop(&d);
        failures++;
    } else {
        /* Arm diagnostics + the daemon-side barrier, then park the count at
         * the sizing/enumeration boundary through the find-flush gate. */
        ASSERT_EQ_INT(test_env_ctl_set_bmlock_log(&d.env, 1), 0,
                      "arm candidate alloc log");
        long long dpre = -1;
        {
            char preq[512];
            snprintf(preq, sizeof(preq),
                     "{\"mode\":\"count\",\"dir\":\"d10w\",\"object\":\"t10w\","
                     "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\","
                     "\"value\":\"0\"}]}");
            char *pr = NULL;
            ASSERT_EQ_INT(bmf_req(&d, preq, &pr), 0, "daemon pre-count");
            ASSERT_EQ_INT(bmf_parse_bare_int(pr, &dpre), 0,
                          "daemon pre-count parses");
            free(pr);
            ASSERT_TRUE(dpre >= 0 && dpre < 2549,
                        "daemon pre-count under the 2,549 target");
        }
        long long dburst = 2549 - dpre;
        ASSERT_EQ_INT(test_env_test_hook_install_kind(&d.env, 2), 0,
                      "arm find-flush gate");
        ASSERT_EQ_INT(test_env_ctl_set_bmks_race(&d.env, BMKS_RACE_EQ), 0,
                      "arm daemon barrier");
        char creq[512];
        snprintf(creq, sizeof(creq),
                 "{\"mode\":\"count\",\"dir\":\"d10w\",\"object\":\"t10w\","
                 "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\","
                 "\"value\":\"0\"}]}");
        char *resp = NULL;
        BmfAsyncReq ar = { .env = &d.env, .req = creq, .resp = &resp };
        tu_join_signal_init(&ar.js);
        BMF_CHECK(pthread_create(&ar.tid, NULL, bmf_async_req_main, &ar) == 0,
                  110, "spawn parked count");
        int phase = -1;
        int wrc = test_env_test_hook_wait(&d.env, &phase);
        ASSERT_EQ_INT(wrc, 0, "barrier parked (gate hook reached)");
        if (wrc == 0) ASSERT_EQ_INT(phase, 3, "gate phase = batch-fetch park");
        if (!t_ctx->failed) {
            /* Freeze writes before releasing enumeration. */
            BmfWireWriter ww;
            memset(&ww, 0, sizeof(ww));
            ww.env = &d.env;
            ww.f0_value = "0";
            ww.f1_value = "0";
            ww.from = 500001;
            ww.to = 500000 + dburst;
            tu_join_signal_init(&ww.js);
            BMF_CHECK(pthread_create(&ww.tid, NULL, bmf_wire_writer_main,
                                     &ww) == 0, 111, "spawn frozen burst writer");
            if (bmf_floor_join(ww.tid, &ww.js, 30) != 0) {
                bmf_stage("FAIL frozen burst writer join deadline");
                _exit(120);
            }
            ASSERT_EQ_INT(ww.rc, 0, "frozen burst to 2,549 accepted");
            test_env_test_hook_release(&d.env);
            if (bmf_floor_join(ar.tid, &ar.js, 30) != 0) {
                bmf_stage("FAIL parked count join deadline");
                _exit(120);
            }
            ASSERT_EQ_INT(ar.rc, 0, "parked count completed");
            ASSERT_NOT_NULL(resp, "parked count response");
            if (resp) {
                long long got = -1;
                ASSERT_EQ_INT(bmf_parse_bare_int(resp, &got), 0,
                              "count parses");
                if (got != 2549) {
                    TAP_DIAG("# floor-held wire count: %lld (want 2549)\n",
                             got);
                    char p2[512], *r2 = NULL;
                    long long g2 = -1;
                    snprintf(p2, sizeof(p2),
                             "{\"mode\":\"count\",\"dir\":\"d10w\","
                             "\"object\":\"t10w\",\"criteria\":"
                             "[{\"field\":\"f0\",\"op\":\"eq\","
                             "\"value\":\"0\"}]}");
                    if (bmf_req(&d, p2, &r2) == 0 &&
                        bmf_parse_bare_int(r2, &g2) == 0)
                        TAP_DIAG("# f0=0 count: %lld (burst+seed should "
                                 "dominate)\n", g2);
                    free(r2);
                    snprintf(p2, sizeof(p2),
                             "{\"mode\":\"count\",\"dir\":\"d10w\","
                             "\"object\":\"t10w\"}");
                    if (bmf_req(&d, p2, &r2) == 0 &&
                        bmf_parse_bare_int(r2, &g2) == 0)
                        TAP_DIAG("# total live: %lld (want 2549+90-deletes "
                                 "rounded)\n", g2);
                    free(r2);
                    bmf_dump_daemon_log(&d.env);
                }
                ASSERT_TRUE(got == 2549,
                            "wire count equals the frozen oracle (floor held: "
                            "no candidate-capacity recovery logged)");
                free(resp);
                resp = NULL;
            }
            /* Full membership + aggregate oracles on the wire. */
            char freq[512];
            snprintf(freq, sizeof(freq),
                     "{\"mode\":\"find\",\"dir\":\"d10w\",\"object\":\"t10w\","
                     "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\","
                     "\"value\":\"0\"}],\"limit\":5000}");
            resp = NULL;
            ASSERT_EQ_INT(bmf_req(&d, freq, &resp), 0, "find round-trip");
            char **keys = NULL; int n = 0;
            ASSERT_EQ_INT(bmf_parse_find_keys(resp, &keys, &n), 0,
                          "find parses");
            ASSERT_TRUE(n == 2549, "find membership cardinality");
            bmf_free_keys(keys, n);
            free(resp);
            resp = NULL;
            char areq[512];
            snprintf(areq, sizeof(areq),
                     "{\"mode\":\"aggregate\",\"dir\":\"d10w\","
                     "\"object\":\"t10w\",\"aggregates\":[{\"fn\":\"count\"}],"
                     "\"group_by\":\"f1\"}");
            ASSERT_EQ_INT(bmf_req(&d, areq, &resp), 0, "aggregate round-trip");
            char labels[8][8];
            long counts[8];
            int nb = 0;
            ASSERT_EQ_INT(bmf_parse_group_counts(resp, "f1", labels, counts,
                                                 8, &nb), 0, "buckets parse");
            for (int v = 0; v < 4; v++) {
                char want[8];
                snprintf(want, sizeof(want), "%d", v);
                long expect = bmf_oracle_count_field(&d.o, 1, v);
                if (v == 0) expect += dburst;
                int found = 0;
                for (int b = 0; b < nb; b++)
                    if (strcmp(labels[b], want) == 0) {
                        found = 1;
                        ASSERT_TRUE(counts[b] == expect, "bucket oracle");
                    }
                ASSERT_TRUE(found, "bucket present");
            }
            free(resp);
            resp = NULL;
            test_env_test_hook_clear(&d.env);
        } else {
            test_env_test_hook_clear(&d.env);
            if (bmf_floor_join(ar.tid, &ar.js, 30) != 0) {
                bmf_stage("FAIL count cleanup join deadline");
                _exit(120);
            }
            free(resp);
            resp = NULL;
        }

        /* Second scenario: burst past the observed 8,192-slot floor; the
         * capacity recovery must return the frozen oracle (log shows the
         * recovery; the concurrent-reader no-error guarantee stays with its
         * own unchanged case). */
        if (!t_ctx->failed) {
            ASSERT_EQ_INT(test_env_test_hook_install_kind(&d.env, 2), 0,
                          "re-arm find-flush gate");
            ASSERT_EQ_INT(test_env_ctl_set_bmks_race(&d.env, BMKS_RACE_EQ), 0,
                          "re-arm daemon barrier");
            snprintf(creq, sizeof(creq),
                     "{\"mode\":\"count\",\"dir\":\"d10w\",\"object\":\"t10w\","
                     "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\","
                     "\"value\":\"0\"}]}");
            ar.resp = &resp;
            tu_join_signal_init(&ar.js);
            BMF_CHECK(pthread_create(&ar.tid, NULL, bmf_async_req_main, &ar) == 0,
                      112, "spawn parked count 2");
            int phase2 = -1;
            ASSERT_EQ_INT(test_env_test_hook_wait(&d.env, &phase2), 0,
                          "barrier 2 parked");
            BmfWireWriter ww2;
            memset(&ww2, 0, sizeof(ww2));
            ww2.env = &d.env;
            ww2.f0_value = "0";
            ww2.f1_value = "0";
            ww2.from = 600001;
            ww2.to = 600000 + 8000;
            tu_join_signal_init(&ww2.js);
            BMF_CHECK(pthread_create(&ww2.tid, NULL, bmf_wire_writer_main,
                                     &ww2) == 0, 113, "spawn past-floor writer");
            if (bmf_floor_join(ww2.tid, &ww2.js, 60) != 0) {
                bmf_stage("FAIL past-floor writer join deadline");
                _exit(120);
            }
            ASSERT_EQ_INT(ww2.rc, 0,
                          "burst past the floor accepted (10,549 total)");
            test_env_test_hook_release(&d.env);
            if (bmf_floor_join(ar.tid, &ar.js, 60) != 0) {
                bmf_stage("FAIL parked count 2 join deadline");
                _exit(120);
            }
            ASSERT_EQ_INT(ar.rc, 0, "parked count 2 completed");
            ASSERT_NOT_NULL(resp, "count 2 response");
            if (resp) {
                long long got = -1;
                ASSERT_EQ_INT(bmf_parse_bare_int(resp, &got), 0,
                              "count 2 parses");
                ASSERT_TRUE(got == 10549,
                            "capacity recovery equals the frozen oracle");
                free(resp);
                resp = NULL;
            }
            test_env_test_hook_clear(&d.env);
            /* Recovery needle in the daemon log. */
            char logbase[300], lpath[400];
            snprintf(logbase, sizeof(logbase), "%s", d.env.db_root);
            char *sl = strrchr(logbase, '/');
            if (sl) *sl = '\0';
            DIR *ldh;
            snprintf(lpath, sizeof(lpath), "%s/logs", logbase);
            int needle = 0, barrier_seen = 0;
            if ((ldh = opendir(lpath))) {
                struct dirent *de;
                while ((de = readdir(ldh))) {
                    if (!strstr(de->d_name, ".log")) continue;
                    snprintf(lpath, sizeof(lpath), "%s/logs/%s", logbase,
                             de->d_name);
                    char *body = tu_read_file(lpath);
                    if (body) {
                        if (strstr(body, "candidate capacity exceeded"))
                            needle = 1;
                        if (strstr(body, "BMKS-BARRIER-REACHED"))
                            barrier_seen = 1;
                        free(body);
                    }
                }
                closedir(ldh);
            }
            ASSERT_TRUE(barrier_seen, "barrier milestones in the daemon log");
            ASSERT_TRUE(needle, "capacity recovery logged on overflow");
        }
        bmf_daemon_stop(&d);
    }

    ASSERT_TRUE(failures == 0, "floor sizing + overflow recovery + budget/hard/timeout");
    return failures != 0 || t_ctx->failed != 0;
}

static int bmf_parser_selfcheck(void) {
    const char *bad[] = {
        "{,\"error\":\"boom\"}", "{\"error\":\"boom\",}",
        "{\"error\":\"boom\",,\"x\":1}", "{\"a\":1 \"b\":2}",
        "[1,]", "[,1]", "[1 2]", "[+]", "[--1]", "[1e]",
        "[01]", "[1.]", "[truefalse]", "{\"a\":1,\"a\":2}",
        "{\"a\":1,\"\\u0061\":2}", "{\"error\":\"boom\"}[]",
        "{\"a\":\"\\q\"}", "{\"a\":\"unterminated}"
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++)
        BMF_CHECK(bmf_validate_no_error(bad[i]) != 0, 121,
                  "strict parser rejects malformed responses");
    BMF_CHECK(bmf_validate_no_error(
        "{\"a\":[-1.25e+2,true,false,null,\"ok\"],\"b\":{}}") == 0, 122,
        "strict parser accepts nested valid JSON");
    BMF_CHECK(bmf_validate_error_exact("{\"error\":\"boom\"}", "boom") == 0,
              123, "standalone error accepted");
    BMF_CHECK(bmf_validate_error_exact(
        "{\"error\":\"boom\",\"extra\":1}", "boom") != 0, 124,
        "error envelope has exactly one member");
    char labels[8][8]; long counts[8]; int n = 0;
    BMF_CHECK(bmf_parse_group_counts(
        "[{\"f0\":\"0\",\"count\":1},{\"f0\":\"0\",\"count\":2}]",
        "f0", labels, counts, 8, &n) != 0, 125, "duplicate groups rejected");
    BMF_CHECK(bmf_parse_group_counts(
        "[{\"f0\":\"0\",\"count\":1},{\"f0\":\"1\",\"count\":2}]",
        "f0", labels, counts, 8, &n) == 0 && n == 2, 126,
        "distinct groups accepted");
    long long value = -1;
    BMF_CHECK(bmf_parse_bare_int("9223372036854775808", &value) != 0, 127,
              "integer overflow rejected");
    bmf_stage("strict-parser-ok");
    return 0;
}

static int test_bitmap_candidate_floor_run(void) {
    BmfChildResult r;
    int rc = bmf_run_child_bounded("candidate-floor", bmf_floor_run_child, &r,
                                   BMF_WORK_BUDGET_MS + BMF_CLEANUP_RESERVE_MS);
    int ok = rc == 1 && r.ran && !r.timed_out && !r.termsig && r.exit_code == 0;
    ASSERT_TRUE(ok, "bounded floor sizing, wire oracles and overflow recovery");
    return ok ? 0 : 1;
}

typedef struct {
    char **keys;
    size_t n;
    size_t cap;
} BmfShapeKeys;

static int bmf_shape_key_id(const char *key, long *id) {
    if ((key[0] != 'k' && key[0] != 'w') || key[1] < '0' || key[1] > '9')
        return 0;
    char *end = NULL;
    errno = 0;
    *id = strtol(key + 1, &end, 10);
    return errno == 0 && *end == '\0';
}

static int bmf_shape_add_key(BmfShapeKeys *keys, const char *key) {
    long id;
    if (!bmf_shape_key_id(key, &id)) return 0;
    if (keys->n == keys->cap) {
        size_t next = keys->cap ? keys->cap * 2 : 64;
        char **items = realloc(keys->keys, next * sizeof(*items));
        if (!items) return -1;
        keys->keys = items;
        keys->cap = next;
    }
    keys->keys[keys->n] = strdup(key);
    if (!keys->keys[keys->n]) return -1;
    keys->n++;
    return 0;
}

/* Fixture values contain no key-shaped strings. Capture driver identities
 * in the default, dict and tabular encodings; cursor metadata repeats a
 * delivered key, so it is deliberately excluded. */
static int bmf_shape_json_keys(const char **pos, BmfShapeKeys *keys, int depth) {
    if (depth > 32) return -1;
    const char *p = bmf_strict_json_ws(*pos);
    if (*p == '"') {
        char *value = NULL;
        const char *end = bmf_strict_json_string(p, &value);
        if (!end) return -1;
        int rc = bmf_shape_add_key(keys, value);
        free(value);
        *pos = end;
        return rc;
    }
    if (*p == '{') {
        p = bmf_strict_json_ws(p + 1);
        if (*p == '}') { *pos = p + 1; return 0; }
        for (;;) {
            char *name = NULL;
            const char *end = bmf_strict_json_string(p, &name);
            if (!end) return -1;
            int cursor = strcmp(name, "cursor") == 0;
            int rc = bmf_shape_add_key(keys, name);
            free(name);
            if (rc != 0) return -1;
            p = bmf_strict_json_ws(end);
            if (*p++ != ':') return -1;
            if (cursor) {
                p = bmf_strict_json_skip_value(p, depth + 1);
                if (!p) return -1;
            } else if (bmf_shape_json_keys(&p, keys, depth + 1) != 0) return -1;
            p = bmf_strict_json_ws(p);
            if (*p == '}') { *pos = p + 1; return 0; }
            if (*p++ != ',') return -1;
            p = bmf_strict_json_ws(p);
        }
    }
    if (*p == '[') {
        p = bmf_strict_json_ws(p + 1);
        if (*p == ']') { *pos = p + 1; return 0; }
        for (;;) {
            if (bmf_shape_json_keys(&p, keys, depth + 1) != 0) return -1;
            p = bmf_strict_json_ws(p);
            if (*p == ']') { *pos = p + 1; return 0; }
            if (*p++ != ',') return -1;
        }
    }
    p = bmf_strict_json_skip_value(p, depth);
    if (!p) return -1;
    *pos = p;
    return 0;
}

static int bmf_shape_collect_keys(const char *resp, int csv, BmfShapeKeys *keys) {
    if (!resp) return -1;
    if (!csv) {
        if (bmf_validate_json_complete(resp) != 0) return -1;
        const char *p = resp;
        return bmf_shape_json_keys(&p, keys, 0);
    }
    const char *p = strchr(resp, '\n');
    if (!p) return -1;
    p++;
    while (*p) {
        const char *end = strchr(p, '\n');
        if (!end) return -1;
        const char *comma = memchr(p, ',', (size_t)(end - p));
        if (!comma) return -1;
        char *key = strndup(p, (size_t)(comma - p));
        if (!key) return -1;
        long id;
        int rc = bmf_shape_key_id(key, &id) ? bmf_shape_add_key(keys, key) : -1;
        free(key);
        if (rc != 0) return -1;
        p = end + 1;
    }
    return 0;
}

enum {
    BMF_SHAPE_PAGE,
    BMF_SHAPE_JOIN,
    BMF_SHAPE_CURSOR,
    BMF_SHAPE_SORT_DICT,
    BMF_SHAPE_AGG,
    BMF_SHAPE_OR,
    BMF_SHAPE_CSV,
    BMF_SHAPE_COUNT
};

static int bmf_shape_case;

static int bmf_capacity_shape_child(void) {
    BmfPdb p = {0};
    p.db = bmf_child_db;
    BMF_CHECK(p.db != NULL, 1, "shape process db");
    snprintf(p.root, sizeof(p.root), "%s", test_get_process_db_root());
    bmf_oracle_init(&p.o, p.root, "bmf_shapes", "driver", 100);
    BMF_CHECK(bmf_pdb_create(&p, "bmf_shapes", "driver") == 0, 2, "shape object");
    BMF_CHECK(bmf_pdb_seed(&p, 1, 100, 100) == 0, 3, "shape seed");
    char *resp = NULL;
    if (bmf_shape_case == BMF_SHAPE_JOIN) {
        BMF_CHECK(bmf_pdb_req(p.db,
            "{\"mode\":\"create-object\",\"dir\":\"bmf_shapes\",\"object\":\"remote\","
            "\"fields\":[\"id:int\",\"pay:varchar:32\"],\"indexes\":[\"id\"]}", &resp) == 0,
            4, "join helper create");
        int ok = bmf_validate_no_error(resp) == 0;
        bmf_pdb_free(resp); resp = NULL;
        BMF_CHECK(ok, 5, "join helper accepted");
        BMF_CHECK(bmf_pdb_req(p.db,
            "{\"mode\":\"insert\",\"dir\":\"bmf_shapes\",\"object\":\"remote\","
            "\"key\":\"remote\",\"value\":{\"id\":0,\"pay\":\"joined\"}}", &resp) == 0,
            6, "join helper insert");
        ok = bmf_validate_no_error(resp) == 0;
        bmf_pdb_free(resp); resp = NULL;
        BMF_CHECK(ok, 7, "join helper seeded");
    }
    if (bmf_shape_case == BMF_SHAPE_AGG) {
        BMF_CHECK(bmf_pdb_req(p.db,
            "{\"mode\":\"add-index\",\"dir\":\"bmf_shapes\",\"object\":\"driver\","
            "\"field\":\"f0:btree\"}", &resp) == 0, 8, "shape top-N group btree");
        int ok = bmf_validate_no_error(resp) == 0;
        bmf_pdb_free(resp); resp = NULL;
        BMF_CHECK(ok, 9, "shape top-N group btree accepted");
    }
    const char *eq = "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]";
    const char *criteria = eq;
    const char *mode = "find";
    const char *tail = "\"limit\":20000";
    if (bmf_shape_case == BMF_SHAPE_PAGE) {
        criteria = "{\"and\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"},"
                   "{\"field\":\"pay\",\"op\":\"eq\",\"value\":\"race\"}]}";
        tail = "\"excludedKeys\":[\"w0100001\"],\"offset\":3,\"limit\":17,\"total\":true";
    } else if (bmf_shape_case == BMF_SHAPE_JOIN) {
        tail = "\"limit\":20000,\"join\":[{\"object\":\"remote\",\"local\":\"f1\","
               "\"remote\":\"id\",\"as\":\"joined\",\"type\":\"inner\",\"fields\":[\"pay\"]}]";
    } else if (bmf_shape_case == BMF_SHAPE_CURSOR) {
        tail = "\"limit\":20000,\"order_by\":\"id\",\"order\":\"asc\",\"cursor\":null,\"total\":true";
    } else if (bmf_shape_case == BMF_SHAPE_SORT_DICT) {
        tail = "\"limit\":20000,\"order_by\":\"pay\",\"format\":\"dict\"";
    } else if (bmf_shape_case == BMF_SHAPE_AGG) {
        mode = "aggregate";
        tail = "\"aggregates\":[{\"fn\":\"count\"}],\"group_by\":\"f0\",\"order_by\":\"count\",\"limit\":10";
    } else if (bmf_shape_case == BMF_SHAPE_OR) {
        criteria = "{\"or\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"},"
                   "{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"1\"}]}";
    } else if (bmf_shape_case == BMF_SHAPE_CSV) {
        tail = "\"limit\":20000,\"format\":\"csv\"";
    }
    char req[2048];
    snprintf(req, sizeof(req),
        "{\"mode\":\"%s\",\"dir\":\"bmf_shapes\",\"object\":\"driver\",\"criteria\":%s,%s}",
        mode, criteria, tail);
    query_test_candidate_observe(1);
    BMF_CHECK(query_bm_test_arm_keyset_race(
        bmf_shape_case == BMF_SHAPE_OR ? BMKS_RACE_OR : BMKS_RACE_EQ) == 0,
        10, "shape barrier arm");
    BmfRaceWriter w = {0};
    w.db = p.db; w.o = &p.o; w.f0_value = "0"; w.f1_value = "0";
    w.from = 100001; w.past_cap = 1;
    atomic_init(&w.done, 0); atomic_init(&w.abort, 0);
    BMF_CHECK(pthread_create(&w.tid, NULL, bmf_race_writer_main, &w) == 0,
              11, "shape writer");
    int rc = bmf_pdb_req(p.db, req, &resp);
    atomic_store(&w.abort, 1);
    pthread_join(w.tid, NULL);
    unsigned slots = 0, overflows = 0, fallbacks = 0, peak = 0;
    query_test_candidate_stats(&slots, &overflows, &fallbacks, &peak);
    BMF_CHECK(w.rc == 0 && rc == 0 && query_bm_test_race_reached() &&
              query_bm_test_race_gate_opened(), 12, "shape barrier and frozen writes");
    BMF_CHECK(query_bm_test_race_allocated_cap() > 0 &&
              w.to - w.from + 1 > query_bm_test_race_allocated_cap(), 13,
              "shape burst exceeds observed actual capacity");
    BMF_CHECK(overflows > 0 && fallbacks == 1, 14,
              "shape observes insertion failure and exactly one recovery");
    long burst = w.to - w.from + 1;
    long expected = burst;
    for (long i = 1; i <= p.o.seeded; i++)
        if (bmf_f1_for(i) == 0 ||
            (bmf_shape_case == BMF_SHAPE_OR && bmf_f0_for(i) == 1)) expected++;
    if (bmf_shape_case == BMF_SHAPE_AGG) {
        char labels[8][8]; long counts[8]; int n = 0;
        BMF_CHECK(bmf_parse_group_counts(resp, "f0", labels, counts, 8, &n) == 0,
                  15, "shape complete aggregate");
        long wanted[4] = {burst, 0, 0, 0};
        for (long i = 1; i <= p.o.seeded; i++)
            if (bmf_f1_for(i) == 0) wanted[bmf_f0_for(i)]++;
        int groups = 0;
        for (int v = 0; v < 4; v++) if (wanted[v]) groups++;
        BMF_CHECK(n == groups, 16, "all aggregate buckets and no extras");
        for (int b = 0; b < n; b++) {
            int v = labels[b][0] - '0';
            BMF_CHECK(labels[b][1] == '\0' && v >= 0 && v < 4 &&
                      counts[b] == wanted[v], 17, "aggregate frozen bucket oracle");
        }
    } else {
        BmfShapeKeys keys = {0};
        BMF_CHECK(bmf_shape_collect_keys(resp, bmf_shape_case == BMF_SHAPE_CSV,
                                        &keys) == 0, 18, "shape complete framing");
        BMF_CHECK(keys.n == (size_t)(bmf_shape_case == BMF_SHAPE_PAGE ? 17 : expected),
                  19, "shape exact page size or complete membership size");
        long previous = -1;
        for (size_t i = 0; i < keys.n; i++) {
            long id = 0;
            BMF_CHECK(bmf_shape_key_id(keys.keys[i], &id), 20, "driver identity parses");
            int seed = keys.keys[i][0] == 'k' && id >= 1 && id <= p.o.seeded &&
                (bmf_f1_for(id) == 0 ||
                 (bmf_shape_case == BMF_SHAPE_OR && bmf_f0_for(id) == 1));
            int inserted = keys.keys[i][0] == 'w' && id >= w.from && id <= w.to;
            if (bmf_shape_case == BMF_SHAPE_PAGE) {
                seed = 0;
                if (id == w.from) inserted = 0;
            }
            BMF_CHECK(seed || inserted, 21, "every delivered key belongs to frozen oracle");
            if (bmf_shape_case == BMF_SHAPE_CURSOR)
                BMF_CHECK(id > previous, 22, "cursor row order preserved");
            previous = id;
        }
        qsort(keys.keys, keys.n, sizeof(*keys.keys), bmf_key_cmp);
        for (size_t i = 1; i < keys.n; i++)
            BMF_CHECK(strcmp(keys.keys[i - 1], keys.keys[i]) != 0, 23,
                      "shape emits every key at most once");
        bmf_free_keys(keys.keys, (int)keys.n);
        if (bmf_shape_case == BMF_SHAPE_PAGE || bmf_shape_case == BMF_SHAPE_CURSOR) {
            long long total = -1;
            BMF_CHECK(bmf_total_value(resp, &total) == 0 &&
                      total == (bmf_shape_case == BMF_SHAPE_PAGE ? burst : expected),
                      24, "shape total uses complete original criteria");
        }
    }
    bmf_pdb_free(resp); resp = NULL;
    query_test_candidate_observe(0);
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"drop-object\",\"dir\":\"bmf_shapes\",\"object\":\"driver\"}", &resp) == 0,
        25, "shape drop driver");
    bmf_pdb_free(resp); resp = NULL;
    if (bmf_shape_case == BMF_SHAPE_JOIN) {
        BMF_CHECK(bmf_pdb_req(p.db,
            "{\"mode\":\"drop-object\",\"dir\":\"bmf_shapes\",\"object\":\"remote\"}", &resp) == 0,
            26, "shape drop remote");
        bmf_pdb_free(resp);
    }
    bmf_stage("forced-shape-complete");
    return 0;
}

static int test_bitmap_capacity_fallback_shapes_run(void) {
    static const char *names[] = {"page", "join", "cursor", "sort-dict", "aggregate", "or", "csv"};
    bmf_budget_anchor = bmf_monotonic_ms();
    bmf_budget_total_ms = BMF_WORK_BUDGET_MS + BMF_CLEANUP_RESERVE_MS;
    int failures = 0;
    for (int i = 0; i < BMF_SHAPE_COUNT; i++) {
        bmf_shape_case = i;
        BmfChildResult r;
        int rc = bmf_run_child(names[i], bmf_capacity_shape_child, &r);
        int ok = rc == 1 && r.ran && !r.timed_out && !r.termsig && r.exit_code == 0;
        ASSERT_TRUE(ok, names[i]);
        if (!ok) { failures++; break; }
    }
    bmf_budget_anchor = 0;
    bmf_budget_total_ms = BMF_WORK_BUDGET_MS;
    return failures != 0;
}


/* ═══════ R1: same-OrChildWorkerCtx overflow + hard-error precedence ═══════ */

/* In-process: one OR union build whose f1 child genuinely overflows the
 * shared KeySet through or_collect_cb after a barrier-gated concurrent
 * burst, and — in the same OrChildWorkerCtx's dispatch — encounters an
 * actual damaged-bitmap read error (EUCLEAN cold open). The hard outcome
 * must win with its exact errno, every reservation must be released, and
 * the capacity recovery must not run. The f2 child is deliberately
 * value-empty (no records carry f2), so every observed overflow and the
 * hard error provably belong to the same f1 worker context. */
static int bmf_or_precedence_child(void) {
    BMF_CHECK(bmf_child_db != NULL, 1, "process db must exist");
    BMF_CHECK(parallel_pool_size() == 0 && parallel_io_pool_size() == 0, 2,
              "isolated child starts without pools");
    BmfPdb p;
    memset(&p, 0, sizeof(p));
    p.db = bmf_child_db;
    snprintf(p.root, sizeof(p.root), "%s", test_get_process_db_root());
    bmf_oracle_init(&p.o, p.root, "bmf_d11", "t11or", 300);
    BMF_CHECK(bmf_pdb_create(&p, "bmf_d11", "t11or") == 0, 3, "create object");
    BMF_CHECK(bmf_pdb_seed(&p, 1, 300, 300) == 0, 4, "seed");

    /* Damage the lowest-numbered existing f1 bitmap shard. Healthy shards
     * before it must not cross the KeySet capacity before the damaged
     * shard's open records its read error: with ~759 candidates per shard
     * and a 2,048-slot OR KeySet, shards 0-3 contribute at most ~3,036
     * cumulative inserts before shard 4 — but overflow fires at 2,049, so
     * a damage target past index 3 could be skipped by the stop flag and
     * would make this fixture degenerate rather than proving precedence. */
    int dmg = -1;
    for (int s = 0; s < 8 && dmg < 0; s++) {
        char bp[PATH_MAX];
        bmf_bm_path(bp, sizeof(bp), &p.o, "f1", s);
        if (bmf_file_exists(bp)) dmg = s;
    }
    BMF_CHECK(dmg >= 0 && dmg <= 3, 5, "existing f1 shard within prefix guard");
    char dmg_path[PATH_MAX];
    bmf_bm_path(dmg_path, sizeof(dmg_path), &p.o, "f1", dmg);

    const char *error = NULL;
    CriteriaNode *tree = parse_criteria_tree(
        "{\"or\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"},"
        "{\"field\":\"f2\",\"op\":\"eq\",\"value\":\"1\"}]}", &error);
    BMF_CHECK(tree && tree->kind == CNODE_OR && tree->n_children == 2, 6,
              "two-child OR parses");
    char effective[PATH_MAX];
    snprintf(effective, sizeof(effective), "%s/bmf_d11", p.root);
    Schema sch = load_schema(effective, "t11or");

    query_test_candidate_observe(1);
    BMF_CHECK(query_bm_test_arm_keyset_race(BMKS_RACE_OR) == 0, 7,
              "arm OR barrier");
    BmfRaceWriter w = {0};
    w.db = p.db; w.o = &p.o;
    w.f0_value = "0"; w.f1_value = "0";
    /* 3,000 burst records: ~3,075 f1="0" matches, ~2,691 insertable after
     * the damaged shard's lost share — comfortably past the 2,048-slot cap,
     * while the pre-damage prefix stays below it. */
    w.from = 100001;
    w.to = w.from + 3000;
    w.damage_path = dmg_path;
    w.drop_cache = 1;   /* damaged shard must cold-open EUCLEAN at walk time */
    atomic_init(&w.done, 0); atomic_init(&w.abort, 0);
    BMF_CHECK(pthread_create(&w.tid, NULL, bmf_race_writer_main, &w) == 0,
              8, "writer thread start");

    QueryDeadline dl = { now_ms_coarse(), 0, 0 };
    KeySetBudget budget = { .limit = (size_t)32 << 20 };
    atomic_init(&budget.used, 0);
    atomic_init(&budget.peak, 0);
    CandidateBuildOut outcome;
    int budget_exceeded = 0;
    KeySet *ks = build_or_keyset_ck(effective, "t11or", sch.splits, tree,
                                    &dl, &budget_exceeded, 0, &budget,
                                    &outcome);
    atomic_store(&w.abort, 1);
    pthread_join(w.tid, NULL);

    unsigned last_slots = 0, overflows = 0, fallbacks = 0, peak = 0;
    query_test_candidate_stats(&last_slots, &overflows, &fallbacks, &peak);
    BMF_CHECK(query_bm_test_race_reached() == 1, 9, "OR barrier reached");
    BMF_CHECK(w.rc == 0, 10, "writer inserts accepted and damage applied");
    BMF_CHECK(query_bm_test_race_gate_opened() == 1, 11, "gate opened");
    BMF_CHECK(query_bm_test_race_allocated_cap() > 0, 12, "capacity observed");
    BMF_CHECK(query_bm_test_race_insert_fails() > 0, 13,
              "real or_collect_cb insertion overflow observed");
    BMF_CHECK(overflows > 0, 14, "overflow observation matches the wrapper");
    BMF_CHECK(ks == NULL, 15, "hard failure returns no KeySet");
    BMF_CHECK(outcome.reason == CBF_HARD && outcome.saved_errno == EUCLEAN, 16,
              "hard read error wins over insertion capacity (same ctx)");
    BMF_CHECK(budget_exceeded == 0, 17, "outcome is not a budget refusal");
    BMF_CHECK(atomic_load(&budget.used) == 0, 18,
              "all candidate reservations released");
    BMF_CHECK(fallbacks == 0, 19, "hard failure cannot become capacity recovery");
    if (ks) keyset_free(ks);
    free_criteria_tree(tree);
    query_test_candidate_observe(0);

    /* Request-state health: the unwritten f2 field still reads exactly 0,
     * and the damaged f1 field keeps failing as a hard error. */
    long long f2_count = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f2\",\"op\":\"eq\",\"value\":\"1\"}]",
        &f2_count) == 0 && f2_count == 0, 20,
        "healthy follow-up read is exact");
    long long f1_count = -1;
    BMF_CHECK(bmf_pdb_count(&p,
        "[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}]",
        &f1_count) != 0, 21,
        "damaged bitmap read stays a hard error");

    char *dresp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"drop-object\",\"dir\":\"bmf_d11\",\"object\":\"t11or\","
        "\"if_exists\":true}", &dresp) == 0, 30, "drop object");
    bmf_pdb_free(dresp);
    bmf_stage("or-precedence-complete");
    return 0;
}

/* ═══════ R1: aggregate top-N capacity handoff meets an expired deadline ══ */

/* In-process: a streaming top-N aggregate whose smaller-bitmap prefilter
 * genuinely overflows through the EQ barrier burst. At the capacity
 * handoff the TEST_BUILD trap lets the request's ORIGINAL absolute
 * deadline elapse (bounded spin, no deadline write or restart), so the
 * caller must observe rc==-2 with an expired deadline, answer
 * query_timeout, and never start the recovery scan. */
static int bmf_topn_deadline_child(void) {
    BMF_CHECK(bmf_child_db != NULL, 1, "process db must exist");
    BMF_CHECK(parallel_pool_size() == 0 && parallel_io_pool_size() == 0, 2,
              "isolated child starts without pools");
    BmfPdb p;
    memset(&p, 0, sizeof(p));
    p.db = bmf_child_db;
    snprintf(p.root, sizeof(p.root), "%s", test_get_process_db_root());
    bmf_oracle_init(&p.o, p.root, "bmf_d11", "t11ag", 300);
    BMF_CHECK(bmf_pdb_create(&p, "bmf_d11", "t11ag") == 0, 3, "create object");
    BMF_CHECK(bmf_pdb_seed(&p, 1, 300, 300) == 0, 4, "seed");
    char *resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"add-index\",\"dir\":\"bmf_d11\",\"object\":\"t11ag\","
        "\"field\":\"f0:btree\"}", &resp) == 0, 5, "top-N group btree");
    {
        int ok = bmf_validate_no_error(resp) == 0;
        bmf_pdb_free(resp); resp = NULL;
        BMF_CHECK(ok, 6, "top-N group btree accepted");
    }

    query_test_candidate_observe(1);
    query_test_agg_topn_arm_deadline_trap(20000);
    BMF_CHECK(query_test_agg_topn_deadline_trap_consumed() == 0, 7,
              "trap starts idle");
    BMF_CHECK(query_bm_test_arm_keyset_race(BMKS_RACE_EQ) == 0, 8,
              "arm EQ barrier");
    BmfRaceWriter w = {0};
    w.db = p.db; w.o = &p.o;
    w.f0_value = "0"; w.f1_value = "0";
    w.from = 200001; w.past_cap = 1;   /* burst = observed cap + 1 record */
    atomic_init(&w.done, 0); atomic_init(&w.abort, 0);
    BMF_CHECK(pthread_create(&w.tid, NULL, bmf_race_writer_main, &w) == 0,
              9, "writer thread start");

    resp = NULL;
    int rc = bmf_pdb_req(p.db,
        "{\"mode\":\"aggregate\",\"dir\":\"bmf_d11\",\"object\":\"t11ag\","
        "\"timeout_ms\":4000,"
        "\"aggregates\":[{\"fn\":\"count\"}],"
        "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"0\"}],"
        "\"group_by\":\"f0\",\"order_by\":\"count\",\"limit\":10}", &resp);
    atomic_store(&w.abort, 1);
    pthread_join(w.tid, NULL);

    unsigned last_slots = 0, overflows = 0, fallbacks = 0, peak = 0;
    query_test_candidate_stats(&last_slots, &overflows, &fallbacks, &peak);
    BMF_CHECK(rc == 0, 10, "aggregate request round-trip");
    BMF_CHECK(query_bm_test_race_reached() == 1, 11, "EQ barrier reached");
    BMF_CHECK(w.rc == 0, 12, "writer inserts accepted");
    BMF_CHECK(query_bm_test_race_gate_opened() == 1, 13, "gate opened");
    BMF_CHECK(query_bm_test_race_allocated_cap() > 0, 14, "capacity observed");
    BMF_CHECK(w.to - w.from + 1 > (long)query_bm_test_race_allocated_cap(), 15,
              "burst exceeds observed actual capacity");
    BMF_CHECK(query_bm_test_race_insert_fails() > 0, 16,
              "real insertion overflow observed");
    BMF_CHECK(overflows > 0, 17, "overflow observation matches the wrapper");
    BMF_CHECK(query_test_agg_topn_deadline_trap_consumed() == 1, 18,
              "capacity handoff reached; trap consumed exactly once");
    BMF_CHECK(bmf_validate_error_exact(resp, "query_timeout") == 0, 19,
              "expired original deadline returns the timeout error");
    BMF_CHECK(fallbacks == 0, 20,
              "no recovery scan started after the expired deadline");
    bmf_pdb_free(resp);
    query_test_candidate_observe(0);

    resp = NULL;
    BMF_CHECK(bmf_pdb_req(p.db,
        "{\"mode\":\"drop-object\",\"dir\":\"bmf_d11\",\"object\":\"t11ag\","
        "\"if_exists\":true}", &resp) == 0, 30, "drop object");
    bmf_pdb_free(resp);
    bmf_stage("topn-deadline-complete");
    return 0;
}


/* ═══════════════════ registrations ═══════════════════ */

static int test_bitmap_or_hard_precedence_run(void) {
    bmf_budget_anchor = bmf_monotonic_ms();
    bmf_budget_total_ms = BMF_WORK_BUDGET_MS + BMF_CLEANUP_RESERVE_MS;
    BmfChildResult r;
    int rc = bmf_run_child("or-precedence", bmf_or_precedence_child, &r);
    bmf_budget_anchor = 0;
    bmf_budget_total_ms = BMF_WORK_BUDGET_MS;
    int ok = rc == 1 && r.ran == 1 && r.exit_code == 0 && !r.timed_out &&
             !r.termsig;
    if (!ok)
        TAP_DIAG("# or-precedence child: rc=%d exit=%d timed_out=%d\n%s",
                 rc, r.exit_code, r.timed_out, r.stages);
    ASSERT_TRUE(ok, "same-ctx OR overflow + hard read error ends CBF_HARD/EUCLEAN");
    if (!ok) return 1;
    ASSERT_TRUE(strstr(r.stages, "or-precedence-complete") != NULL,
                "OR precedence scenario completed");
    return 0;
}

static int test_bitmap_agg_topn_deadline_run(void) {
    bmf_budget_anchor = bmf_monotonic_ms();
    bmf_budget_total_ms = BMF_WORK_BUDGET_MS + BMF_CLEANUP_RESERVE_MS;
    BmfChildResult r;
    int rc = bmf_run_child("topn-deadline", bmf_topn_deadline_child, &r);
    bmf_budget_anchor = 0;
    bmf_budget_total_ms = BMF_WORK_BUDGET_MS;
    int ok = rc == 1 && r.ran == 1 && r.exit_code == 0 && !r.timed_out &&
             !r.termsig;
    if (!ok)
        TAP_DIAG("# topn-deadline child: rc=%d exit=%d timed_out=%d\n%s",
                 rc, r.exit_code, r.timed_out, r.stages);
    ASSERT_TRUE(ok, "top-N capacity handoff with expired deadline returns timeout, no scan");
    if (!ok) return 1;
    ASSERT_TRUE(strstr(r.stages, "topn-deadline-complete") != NULL,
                "top-N deadline scenario completed");
    return 0;
}


TEST_REGISTER("test-bitmap-open-saturation", test_bitmap_open_saturation_run)
TEST_REGISTER("test-bitmap-open-stale-publication", test_bitmap_open_stale_publication_run)
TEST_REGISTER("test-bitmap-damaged-shard-reads", test_bitmap_damaged_shard_reads_run)
TEST_REGISTER("test-bitmap-corrupt-cache-retirement", test_bitmap_corrupt_cache_retirement_run)
TEST_REGISTER("test-bitmap-read-error-propagation", test_bitmap_read_error_propagation_run)
TEST_REGISTER("test-bitmap-find-error-framing", test_bitmap_find_error_framing_run)
TEST_REGISTER("test-bitmap-prepared-small-limit", test_bitmap_prepared_small_limit_run)
TEST_REGISTER("test-bitmap-keyset-capacity-race", test_bitmap_keyset_capacity_race_run)
TEST_REGISTER("test-bitmap-candidate-budget", test_bitmap_candidate_budget_run)
TEST_REGISTER("test-bitmap-candidate-floor", test_bitmap_candidate_floor_run)
TEST_REGISTER("test-bitmap-capacity-fallback-shapes", test_bitmap_capacity_fallback_shapes_run)
TEST_REGISTER("test-bitmap-or-hard-precedence", test_bitmap_or_hard_precedence_run)
TEST_REGISTER("test-bitmap-agg-topn-deadline", test_bitmap_agg_topn_deadline_run)
