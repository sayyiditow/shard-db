/* libFuzzer harness for the daemon's request-framing loop.
 *
 * Every TCP request is framed by the worker loop in server.c: fgets into
 * a g_max_request_size buffer, the "filled buffer without newline"
 * oversized-request check, the drain, and the {"error":"Request too
 * large"} response with its \0\n command separator. A framing bug here
 * desynchronizes every client (response merged into the next request's
 * frame) or wedges the connection (a drain that never terminates), so
 * the loop is fuzzed END-TO-END: the real worker_thread() runs against
 * the CLIENT end of a real loopback TCP pair, fed by the fuzzer, on a
 * throwaway empty DB_ROOT (initialized via embedded.c's
 * shard_db_open_internal), with the REAL dispatcher executing behind it
 * — so the framing logic, auth, AND server_process_fast's parser all
 * run under ASan/UBSan. (server.c is included for its statics; the
 * dispatcher runs for real — no stub, because a stub cannot intercept a
 * call site that is baked into the included text.)
 *
 * The connection MUST be a real AF_INET loopback connection: the worker
 * getpeername()s it and inet_ntop()s it as AF_INET for auth. A
 * socketpair(AF_UNIX) read as sockaddr_in yields 0.0.0.0 — an untrusted
 * peer — so every request used to die at auth and dispatch never ran.
 * Loopback is implicitly trusted (the same peer every local client
 * presents), so dispatch actually executes here.
 *
 * The daemon's async logger is not initialized in this harness, so every
 * LOG_AUDIT/LOG_WARN line would fall through to stderr (tens of
 * thousands of AUTH/dispatch lines per run); a no-op log handler is
 * installed at init to keep the fuzz output clean.
 *
 * Contract asserted per connection, from the wire:
 *   1. every oversized chunk (strlen == buf_size-1, no newline) produces
 *      exactly one "Request too large" response frame;
 *   2. every response frame ends with the \0\n separator;
 *   3. the number of frames equals the number of chunks that are not
 *      oversized, not empty, and not QUIT (empty lines are skipped,
 *      QUIT closes before any response; dispatch answers every other
 *      line with exactly one response frame);
 *   4. the connection always reaches EOF on the harness side within a
 *      bounded time (the drain always terminates — a wedged worker
 *      aborts the run).
 *
 * Inputs containing NUL bytes are skipped: fgets + strlen semantics read
 * stale buffer bytes past an embedded NUL (the connection buffer is
 * reused across chunks), which makes exact expectations
 * nondeterministic; embedded-NUL JSON behavior is fuzzed via
 * fuzz_json/fuzz_cursor instead.
 *
 * Build: see fuzz/build.sh (engine + server.c via the harness include;
 * embedded.c + nql.c provide the real g_db lifecycle and NQL symbols;
 * tls.c linked for symbol resolution, never entered).
 *
 * Run:
 *   ./fuzz_frame -max_total_time=60 fuzz/corpora/frame/
 */

#include <stdint.h>
#include <stddef.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>
#include <strings.h>

#include "../src/db/server.c"

/* Declared in shard_db.h, which this TU does not include (server.c is
   the include); the definition links from embedded.c. */
void shard_db_set_log_handler_global(
    void (*fn)(int type, const char *msg, void *userdata), void *userdata);

/* ---- worker lifecycle (one worker for the whole fuzz process) ---- */
static pthread_t g_frame_worker;
static int       g_frame_worker_up = 0;
static char      g_frame_tmpdir[PATH_MAX];

/* No-op log sink: the async logger never starts here, so without a
   handler every LOG_AUDIT / LOG_WARN line goes to stderr (47k+ lines in
   one corpus pass). */
static void frame_log_sink(int level, const char *msg, void *ud) {
    (void)level; (void)msg; (void)ud;
}

/* The instance opened at init; g_shard_db_instance and every thread's
   g_db alias must point HERE (both are per-thread or rebound per
   connection, so a new thread's defaults are NULL/zero). */
static ShardDb *frame_live_db = NULL;

static void *frame_worker_trampoline(void *arg) {
    (void)arg;
    g_shard_db_instance = frame_live_db;
    g_db = frame_live_db;

    /* WorkerArg is defined by the included server.c; worker_thread()
       takes ownership and free()s it on exit (as the real server's
       spawner does) — heap-allocate it, never pass a static. */
    WorkerArg *wa = (WorkerArg *)malloc(sizeof(*wa));
    if (!wa) return NULL;
    snprintf(wa->db_root, sizeof(wa->db_root), "%s", g_frame_tmpdir);
    wa->id = 0;
    return worker_thread(wa);
}

__attribute__((constructor))
static void frame_harness_init(void) {
    /* Request-path knobs are macros over g_db fields
       (shard_db_internal.h), so g_db must point at a real ShardDb. Open
       a throwaway EMPTY DB_ROOT through embedded.c's lifecycle: an
       empty root initializes directly (version gate passes), the legacy
       fast path answers every line, and nothing persisted survives the
       run. */
    snprintf(g_frame_tmpdir, sizeof(g_frame_tmpdir),
             "/tmp/shard-db-fuzzframe-XXXXXX");
    if (!mkdtemp(g_frame_tmpdir)) {
        fprintf(stderr, "frame: mkdtemp failed\n");
        exit(1);
    }
    ShardDb *db = shard_db_open_internal(g_frame_tmpdir);
    if (!db) {
        fprintf(stderr, "frame: embedded open of the throwaway root "
                "failed\n");
        exit(1);
    }
    /* Silence the logger before any request runs (see header). */
    shard_db_set_log_handler_global(frame_log_sink, NULL);
    /* rebind main-thread aliases onto the opened instance: the macros
       (g_tls_enable, g_max_request_size, ...) read g_db->* */
    frame_live_db = db;
    g_db = db;
    g_shard_db_instance = db;

    /* Worker infrastructure the loop touches. */
    wq_init(&g_work_queue, 64);
    g_worker_cfds = (int *)calloc(1, sizeof(int));
    g_worker_cfds[0] = -1;
    g_worker_cfds_n = 1;
    g_max_request_size = 64;       /* small: oversize cases stay cheap */
    server_running = 1;
    pthread_create(&g_frame_worker, NULL, frame_worker_trampoline, NULL);
    g_frame_worker_up = 1;
}

__attribute__((destructor))
static void frame_harness_shutdown(void) {
    if (g_frame_worker_up) {
        server_running = 0;
        pthread_mutex_lock(&g_work_queue.lock);
        pthread_cond_broadcast(&g_work_queue.not_empty);
        pthread_mutex_unlock(&g_work_queue.lock);
        pthread_join(g_frame_worker, NULL);
    }
    if (g_frame_tmpdir[0]) {
        char cmd[PATH_MAX + 32];
        snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", g_frame_tmpdir);
        if (system(cmd) != 0) { /* best effort */ }
    }
}

/* Wait until the worker finished processing the pushed connection
   (it clears its g_worker_cfds slot before closing the handles). */
static void frame_wait_idle(void) {
    /* give the worker a moment to pick the connection up, then wait
       for it to drop it (slot cleared before handle close) */
    usleep(100);
    for (int i = 0; i < 100000; i++) {
        if (__atomic_load_n(&g_worker_cfds[0], __ATOMIC_ACQUIRE) == -1)
            return;
        usleep(50);
    }
    /* Worker wedged past ~5s on one connection: that IS a framing bug
       (the drain must always terminate). */
    fprintf(stderr, "frame: worker wedged on one connection\n");
    abort();
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    /* NUL-containing inputs: skipped (see header). Also cap at a few
       times the 64-byte request buffer. */
    if (size == 0 || size > 512) return 0;
    if (memchr(data, '\0', size)) return 0;

    /* Real loopback TCP pair (see header): the worker's getpeername +
       inet_ntop(AF_INET) must see 127.0.0.1 — the implicitly trusted
       peer — or every request dies at auth before dispatch. */
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) return 0;
    struct sockaddr_in laddr;
    memset(&laddr, 0, sizeof(laddr));
    laddr.sin_family = AF_INET;
    laddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    laddr.sin_port = 0; /* ephemeral */
    if (bind(lfd, (struct sockaddr *)&laddr, sizeof(laddr)) != 0 ||
        listen(lfd, 1) != 0) {
        close(lfd);
        return 0;
    }
    /* Read the ephemeral port back BEFORE connecting: connecting to
       literal port 0 is ECONNREFUSED and accept() blocks forever. */
    socklen_t lalen = sizeof(laddr);
    if (getsockname(lfd, (struct sockaddr *)&laddr, &lalen) != 0) {
        close(lfd);
        return 0;
    }
    int cfd = socket(AF_INET, SOCK_STREAM, 0);
    if (cfd < 0) { close(lfd); return 0; }
    if (connect(cfd, (struct sockaddr *)&laddr, sizeof(laddr)) != 0) {
        close(cfd);
        close(lfd);
        return 0;
    }
    int pfd = accept(lfd, NULL, NULL);
    close(lfd);
    if (pfd < 0) { close(cfd); return 0; }

    /* Re-pin the buffer size per input: a dispatched request may reload
       config and reset g_db->max_request_size (the value is read once
       per connection, so setting it here keeps every connection at 64
       and the expectations deterministic). */
    g_max_request_size = 64;

    /* Feed the connection, then half-close so the worker's fgets sees
       EOF after the last line. */
    wq_push(&g_work_queue, cfd);
    if (write(pfd, data, size) != (ssize_t)size) {
        close(pfd);
        return 0;
    }
    shutdown(pfd, SHUT_WR);

    /* Simulate fgets/strlen/oversize semantics to build expectations.
       buf_size = 64 (g_max_request_size). Chunk = up to 63 bytes or
       through '\n'; oversized iff strlen(chunk) == buf_size-1 and no
       '\n' in the chunk bytes. NUL-containing inputs are skipped above,
       so strlen == chunk length. */
    int expect_frames = 0, expect_oversize = 0;
    {
        const size_t buf_size = 64;
        size_t i = 0;
        while (i < size) {
            size_t chunk = 0;
            int saw_nl = 0;
            while (i + chunk < size && chunk < buf_size - 1) {
                if (data[i + chunk] == '\n') { chunk++; saw_nl = 1; break; }
                chunk++;
            }
            int oversized = (!saw_nl && chunk == buf_size - 1);
            int quit = 0;
            if (oversized) {
                expect_oversize++;
                expect_frames++;
                /* drain the rest of the line, if any */
                while (i + chunk < size && data[i + chunk] != '\n') chunk++;
                if (i + chunk < size) chunk++;  /* the newline */
            } else {
                /* bound the newline scan to the buffer (data is not
                   NUL-terminated — strcspn here read past the end) */
                const uint8_t *rest = data + i;
                size_t restlen = size - i;
                const uint8_t *nl =
                    (const uint8_t *)memchr(rest, '\n', restlen);
                size_t eff = nl ? (size_t)(nl - rest) : restlen;
                if (eff > chunk) eff = chunk;
                int empty = (eff == 0);
                /* the worker strcasecmp()es QUIT — any case closes */
                quit = (eff == 4 &&
                        strncasecmp((const char *)(data + i), "QUIT", 4) == 0);
                if (!empty && !quit) expect_frames++;
                if (!saw_nl) chunk = eff;  /* EOF-terminated last line */
            }
            i += chunk;
            if (quit) break;  /* QUIT closes the connection: nothing after
                                 it is read or answered */
        }
    }

    /* Read the wire side until EOF (worker closes cfd after the loop). */
    static char resp[262144];
    size_t got = 0;
    /* Bound the drain: a worker that neither answers nor closes would
       otherwise block this read until libFuzzer's own (20-minute) timeout
       eats the CI job. A 5 s silence IS the framing bug the wedge check
       below reports. */
    struct timeval rcv_to = { .tv_sec = 5, .tv_usec = 0 };
    (void)setsockopt(pfd, SOL_SOCKET, SO_RCVTIMEO, &rcv_to, sizeof(rcv_to));
    for (;;) {
        ssize_t r = read(pfd, resp + got, sizeof(resp) - got);
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            fprintf(stderr, "frame: worker wedged on one connection "
                    "(no data or close for 5s)\n");
            abort();
        }
        if (r <= 0) break;
        got += (size_t)r;
        if (got >= sizeof(resp)) break;
    }
    frame_wait_idle();
    close(pfd);

    /* Contract 2 + 1: count frames; every frame ends with \0\n; every
       oversized chunk produced exactly one "Request too large". */
    int frames = 0, too_large = 0;
    size_t frame_start = 0;
    for (size_t i = 0; i + 1 < got; i++) {
        if (resp[i] == '\0' && resp[i + 1] == '\n') {
            frames++;
            /* the oversize error is a fixed JSON envelope at the pinned
               64-byte buffer size — match the EXACT frame. Substring
               search would false-positive on attacker-echoed text (the
               Unknown-dir error echoes the line bytes verbatim). */
            {
                static const char os_err[] =
                    "{\"error\":\"Request too large (max 63 bytes)\"}";
                size_t flen = i - frame_start;
                /* the emitted body carries a trailing \n before the
                   \0\n separator (OUT writes it), so content is
                   os_len + 1 with the newline last */
                if (flen == sizeof(os_err) &&
                    memcmp(resp + frame_start, os_err,
                           sizeof(os_err) - 1) == 0 &&
                    resp[frame_start + flen - 1] == '\n')
                    too_large++;
            }
            frame_start = i + 2;
            i++;
        }
    }
    if (got > 0 && frames == 0) {
        fprintf(stderr, "frame: %zu response bytes with no complete "
                "frame\n", got);
        abort();
    }
    if (too_large != expect_oversize) {
        fprintf(stderr, "frame: oversize responses %d != expected %d "
                "got=%zu\n", too_large, expect_oversize, got);
        for (size_t i = 0; i < got; i++)
            fputc(resp[i] >= 32 && resp[i] < 127 ? resp[i] : '.',
                  stderr);
        fputc('\n', stderr);
        abort();
    }
    if (frames != expect_frames) {
        fprintf(stderr, "frame: response frames %d != expected %d "
                "(got %zu bytes)\n", frames, expect_frames, got);
        abort();
    }
    return 0;
}
