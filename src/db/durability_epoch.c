/* src/db/durability_epoch.c — B3a path-keyed durability epochs.

   Coalesces concurrent fdatasync/fsync calls that target one path.
   Every caller registers a barrier AFTER its writes; the first caller
   with no round in flight claims target = requested and performs the
   raw sync, which covers every request registered up to the claim.
   Concurrent registrants sleep until that round's result covers or
   fails them. Full-file sync semantics make any successful round cover
   every earlier registration on the path, so `completed >= my` is
   durable truth; monotone result high-waters (never reset) mean a
   failed round fails exactly the requests it claimed. A group-failed
   waiter receives an errno from a failed round that claimed it (under
   sustained mixed failures it may be a later round's errno — every
   consumer treats it as diagnostic; see "Protocol semantics"). The
   registry key is (domain, path): different raw-op families on one
   path never coalesce into each other's rounds. Table full / path too
   long → plain uncoalesced sync, which is always correct. */

#include "types.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define DUREPOCH_CAP 256

typedef struct {
    char            path[PATH_MAX];
    unsigned        domain;       /* DurEpochDomain — part of the key     */
    int             used;         /* registry-level: entry installed      */
    pthread_mutex_t mu;           /* guards everything below              */
    pthread_cond_t  cv;
    uint64_t        requested;    /* barriers registered (monotone)       */
    uint64_t        completed;    /* covered by a SUCCESSFUL full-file sync;
                                     monotone, never reset                */
    uint64_t        failed_upto;  /* claimed by a FAILED sync (monotone,
                                     never reset)                         */
    int             last_errno;   /* an errno from the latest failed round */
    int             in_flight;
} DurEpoch;

static DurEpoch g_durepo[DUREPOCH_CAP];
static pthread_mutex_t g_durepo_registry_mu = PTHREAD_MUTEX_INITIALIZER;

/* Entry mutexes/conds are initialized exactly once, up front, under a
   private init mutex — re-installing a recycled entry later must never
   re-run pthread_mutex_init on a live mutex (UB), and per-entry lazy
   init would race the registry scan. ~1.1 MB BSS at CAP=256. */
static pthread_mutex_t g_durepo_init_mu = PTHREAD_MUTEX_INITIALIZER;
static _Atomic int g_durepo_mu_ready;

static void durepo_init_all(void) {
    /* Acquire-load fast path: after the first init, sync calls pay no
       init-mutex traffic. */
    if (atomic_load_explicit(&g_durepo_mu_ready, memory_order_acquire))
        return;
    pthread_mutex_lock(&g_durepo_init_mu);
    if (!g_durepo_mu_ready) {
        for (int i = 0; i < DUREPOCH_CAP; i++) {
            pthread_mutex_init(&g_durepo[i].mu, NULL);
            pthread_cond_init(&g_durepo[i].cv, NULL);
        }
        atomic_store_explicit(&g_durepo_mu_ready, 1, memory_order_release);
    }
    pthread_mutex_unlock(&g_durepo_init_mu);
}

#ifdef TEST_BUILD
static pthread_mutex_t g_durepo_test_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_durepo_test_reg_cv = PTHREAD_COND_INITIALIZER;
static int g_durepo_test_delay_ms;
static int g_durepo_test_fail_remaining;
static int g_durepo_test_fail_errno;
static int g_durepo_test_file_syncs;
static int g_durepo_test_dir_syncs;

static void durepo_test_delay(void) {
    int ms;
    pthread_mutex_lock(&g_durepo_test_mu);
    ms = g_durepo_test_delay_ms;
    pthread_mutex_unlock(&g_durepo_test_mu);
    if (ms > 0) usleep((useconds_t)ms * 1000);
}

/* Count one attempted module raw op (file or dir), successful or not. */
static void durepo_test_count(int is_dir) {
    pthread_mutex_lock(&g_durepo_test_mu);
    if (is_dir) g_durepo_test_dir_syncs++;
    else        g_durepo_test_file_syncs++;
    pthread_mutex_unlock(&g_durepo_test_mu);
}

/* One-shot failure injection for the module FILE raw op only. Returns 1
   with *out_err set when this attempt must fail. */
static int durepo_test_consume_fail(int *out_err) {
    pthread_mutex_lock(&g_durepo_test_mu);
    if (g_durepo_test_fail_remaining > 0) {
        g_durepo_test_fail_remaining--;
        *out_err = g_durepo_test_fail_errno;
        pthread_mutex_unlock(&g_durepo_test_mu);
        return 1;
    }
    pthread_mutex_unlock(&g_durepo_test_mu);
    return 0;
}

void durability_test_epoch_set_delay_ms(int ms) {
    pthread_mutex_lock(&g_durepo_test_mu);
    g_durepo_test_delay_ms = ms > 0 ? ms : 0;
    pthread_mutex_unlock(&g_durepo_test_mu);
}

/* Test-only: make the next N registrants of ANY epoch sync wait until
   all N have registered, before any of them evaluates/claims. Turns
   "N concurrent callers join one round" into a deterministic property
   (a plain start gate cannot: the first thread to grab the entry mutex
   claims target before its peers have registered). */
static int g_durepo_test_reg_hold;
static int g_durepo_test_reg_arrived;

static void durepo_test_reg_hold_wait(void) {
    pthread_mutex_lock(&g_durepo_test_mu);
    if (g_durepo_test_reg_hold <= 0) {
        pthread_mutex_unlock(&g_durepo_test_mu);
        return;
    }
    if (++g_durepo_test_reg_arrived >= g_durepo_test_reg_hold)
        pthread_cond_broadcast(&g_durepo_test_reg_cv);
    else
        while (g_durepo_test_reg_arrived < g_durepo_test_reg_hold)
            pthread_cond_wait(&g_durepo_test_reg_cv, &g_durepo_test_mu);
    pthread_mutex_unlock(&g_durepo_test_mu);
}

void durability_test_epoch_set_register_hold(int n) {
    pthread_mutex_lock(&g_durepo_test_mu);
    g_durepo_test_reg_hold = n > 0 ? n : 0;
    g_durepo_test_reg_arrived = 0;
    pthread_mutex_unlock(&g_durepo_test_mu);
}

void durability_test_epoch_fail_next(int count, int err) {
    pthread_mutex_lock(&g_durepo_test_mu);
    g_durepo_test_fail_remaining = count > 0 ? count : 0;
    g_durepo_test_fail_errno = err > 0 ? err : EIO;
    pthread_mutex_unlock(&g_durepo_test_mu);
}

int durability_test_epoch_file_sync_count(void) {
    pthread_mutex_lock(&g_durepo_test_mu);
    int n = g_durepo_test_file_syncs;
    pthread_mutex_unlock(&g_durepo_test_mu);
    return n;
}

int durability_test_epoch_dir_sync_count(void) {
    pthread_mutex_lock(&g_durepo_test_mu);
    int n = g_durepo_test_dir_syncs;
    pthread_mutex_unlock(&g_durepo_test_mu);
    return n;
}

/* Test-only: wipes the registry and all test knobs/counters. Call only
   when no durability_epoch_* call is in flight anywhere — the harness's
   default separate-worker mode (and --jobs 1, which runs cases one at a
   time) guarantees that for a case that owns the table. */
void durability_test_epoch_reset(void) {
    pthread_mutex_lock(&g_durepo_test_mu);
    g_durepo_test_delay_ms = 0;
    g_durepo_test_fail_remaining = 0;
    g_durepo_test_fail_errno = 0;
    g_durepo_test_file_syncs = 0;
    g_durepo_test_dir_syncs = 0;
    g_durepo_test_reg_hold = 0;
    g_durepo_test_reg_arrived = 0;
    pthread_mutex_unlock(&g_durepo_test_mu);
    durepo_init_all();
    /* Only place that holds registry_mu while taking an entry mu; no
       protocol path ever takes registry_mu under an entry mu. */
    pthread_mutex_lock(&g_durepo_registry_mu);
    for (int i = 0; i < DUREPOCH_CAP; i++) {
        DurEpoch *e = &g_durepo[i];
        pthread_mutex_lock(&e->mu);
        e->requested = 0;
        e->completed = 0;
        e->failed_upto = 0;
        e->last_errno = 0;
        e->in_flight = 0;
        e->used = 0;
        pthread_mutex_unlock(&e->mu);
    }
    pthread_mutex_unlock(&g_durepo_registry_mu);
}
#endif  /* TEST_BUILD */

/* Registry lookup/install. The registry mutex is the ONLY guard for
   used/domain/path; entry mutexes guard protocol state only. Never
   called while holding any entry mutex. Returns NULL when the path
   does not fit or the table is full — the caller falls back to a plain
   uncoalesced raw sync, which is always correct. Lookup is O(CAP)
   strcmp — noise next to any fdatasync. */
static DurEpoch *durepo_find_or_install(const char *path,
                                        unsigned domain) {
    if (!path || strlen(path) >= PATH_MAX) return NULL;
    DurEpoch *free_slot = NULL;
    pthread_mutex_lock(&g_durepo_registry_mu);
    for (int i = 0; i < DUREPOCH_CAP; i++) {
        DurEpoch *e = &g_durepo[i];
        if (!e->used) {
            if (!free_slot) free_slot = e;
            continue;
        }
        if (e->domain == domain && strcmp(e->path, path) == 0) {
            pthread_mutex_unlock(&g_durepo_registry_mu);
            return e;
        }
    }
    if (free_slot) {
        snprintf(free_slot->path, PATH_MAX, "%s", path);
        free_slot->domain = domain;
        free_slot->requested = 0;
        free_slot->completed = 0;
        free_slot->failed_upto = 0;
        free_slot->last_errno = 0;
        free_slot->in_flight = 0;
        free_slot->used = 1;   /* publish last, under the registry mutex */
    }
    pthread_mutex_unlock(&g_durepo_registry_mu);
    return free_slot;          /* NULL → table full */
}

/* Shared epoch protocol. `raw` performs the actual durability op on the
   CURRENT inode at `path` and owns whatever mutation discipline it
   needs (for btrees: the file's writer rwlock held across fdatasync,
   exactly as on the pre-epoch path). The flusher invokes it while
   holding NO epoch mutex. `domain` separates raw-op families in the
   registry: a btree registration can never be covered by a flusher
   that would sync without the btree writer rwlock. *out_synced (may be
   NULL) is set to 1 iff THIS caller attempted the raw op. */
static int durepo_sync(const char *path, unsigned domain,
                       int (*raw)(const char *path, void *ctx),
                       void *ctx, int *out_synced) {
    if (out_synced) *out_synced = 0;
    if (!path) {                   /* reject before the registry OR the
                                      raw callback — a fallback raw op
                                      must never see a null path */
        errno = EINVAL;
        return -1;
    }
    durepo_init_all();
    DurEpoch *e = durepo_find_or_install(path, domain);
    if (!e) {                          /* table full / path too long */
        int rc = raw(path, ctx);
        if (out_synced) *out_synced = 1;
        return rc;
    }
    pthread_mutex_lock(&e->mu);
    uint64_t my = ++e->requested;
    pthread_mutex_unlock(&e->mu);
#ifdef TEST_BUILD
    /* Registration hold: wait until the test's quota of registrants has
       landed, so the first claimer's target covers all of them. Entered
       WITHOUT any epoch mutex — no deadlock with the protocol. */
    durepo_test_reg_hold_wait();
#endif
    /* The claim/wait loop ALWAYS runs under e->mu in every build. The
       release above is only so the TEST-only hold cannot deadlock other
       registrants; production re-locks immediately. */
    pthread_mutex_lock(&e->mu);
    while (1) {
        if (e->completed >= my) {      /* a successful round covered us  */
            pthread_mutex_unlock(&e->mu);
            return 0;
        }
        if (e->failed_upto >= my) {    /* a failed round claimed us      */
            int err = e->last_errno ? e->last_errno : EIO;
            pthread_mutex_unlock(&e->mu);
            errno = err;               /* an errno from A failed round
                                          that claimed us — see the
                                          weakened errno invariant in
                                          "Protocol semantics" */
            return -1;
        }
        if (!e->in_flight) {
            e->in_flight = 1;
            uint64_t target = e->requested;  /* claim AFTER our own
                                                registration: every
                                                request ≤ target had its
                                                writes before its
                                                registration, hence
                                                before the raw op */
            pthread_mutex_unlock(&e->mu);
#ifdef TEST_BUILD
            durepo_test_delay();
#endif
            int rc = raw(path, ctx);
            int raw_errno = errno;
            if (out_synced) *out_synced = 1;
            pthread_mutex_lock(&e->mu);
            e->in_flight = 0;
            if (rc == 0) {
                if (target > e->completed) e->completed = target;
            } else {
                if (target > e->failed_upto) e->failed_upto = target;
                e->last_errno = raw_errno ? raw_errno : EIO;
            }
            pthread_cond_broadcast(&e->cv);
            continue;                  /* re-evaluate our own my */
        }
        pthread_cond_wait(&e->cv, &e->mu);
    }
}

/* Module raw ops. The dir raw preserves the exact pre-epoch behavior of
   its call sites (errno from the failing syscall reaches the caller;
   close() errors are not surfaced, matching slotcask's fsync_dir
   pattern). The FILE raw encodes the bitmap commit leg's documented
   rationale — "A missing file means nothing was ever written — not an
   error" — by converting ENOENT to success: there are no bytes to
   flush, so a covering round is vacuously true. This also structurally
   insulates the one errno-branching consumer (the bitmap call site's
   `errno != ENOENT` skip) from the weakened group-failure errno: an
   epoch failure on a file path can never deliver ENOENT. */

static int durepo_raw_fdatasync_file(const char *path, void *ctx) {
    (void)ctx;
#ifdef TEST_BUILD
    durepo_test_count(0);
    int injected;
    if (durepo_test_consume_fail(&injected)) {
        errno = injected;
        return -1;
    }
#endif
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT) return 0;   /* nothing was ever written */
        return -1;
    }
    int rc = fdatasync(fd);
    int saved = errno;
    close(fd);
    if (rc != 0) errno = saved;
    return rc;
}

static int durepo_raw_fsync_dir(const char *path, void *ctx) {
    (void)ctx;
#ifdef TEST_BUILD
    durepo_test_count(1);
#endif
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return -1;
    int rc = fsync(fd);
    int saved = errno;
    close(fd);
    if (rc != 0) errno = saved;
    return rc;
}

/* Public API. `domain` must be a distinct DurEpochDomain per raw-op
   family; coalescing only ever happens within one domain. */

int durability_epoch_sync_cb(const char *path, unsigned domain,
                             int (*raw)(const char *path, void *ctx),
                             void *ctx, int *out_synced) {
    if (!raw || domain == DUREPOCH_DOMAIN_NONE) {
        errno = EINVAL;
        return -1;
    }
    return durepo_sync(path, domain, raw, ctx, out_synced);
}

int durability_epoch_fdatasync_path_ex(const char *path, int *out_synced) {
    return durepo_sync(path, DUREPOCH_DOMAIN_FILE,
                       durepo_raw_fdatasync_file, NULL, out_synced);
}

int durability_epoch_fsync_dir(const char *path) {
    return durepo_sync(path, DUREPOCH_DOMAIN_DIR,
                       durepo_raw_fsync_dir, NULL, NULL);
}
