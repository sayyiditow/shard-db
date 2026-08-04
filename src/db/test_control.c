/* src/db/test_control.c
 *
 * TEST_BUILD-only control channel for the deterministic
 * test-update-partial-concurrent seam. Compiled only into
 * shard-db-test-server; the production shard-db binary never links it.
 *
 * The test runner inherits one end of an anonymous AF_UNIX/SOCK_STREAM
 * socketpair across exec (`server --test-control-fd <fd>`) and drives the
 * daemon-side slotcask pause hook with fixed-size INSTALL/RELEASE/CLEAR
 * messages. The control thread installs/clears the hook and signals a
 * request thread parked inside it; the only time anything waits while a
 * kf-shard lock is held is the intentional callback pause itself.
 */
#ifdef TEST_BUILD

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

#include "slotcask.h"
#include "test_control.h"

/* Message kinds — private duplicated constants; the runner (fixtures.c)
   defines the same layout independently. */
enum {
    TEST_HOOK_INSTALL = 1,
    TEST_HOOK_RELEASE = 2,
    TEST_HOOK_CLEAR   = 3,
    TEST_HOOK_ACK     = 4,
    TEST_HOOK_REACHED = 5,
};

typedef struct {
    uint32_t kind;   /* INSTALL=1, RELEASE=2, CLEAR=3, ACK=4, REACHED=5 */
    int32_t  phase;  /* REACHED: 0=stale snapshot, 1=under kf wrlock; else 0 */
} TestHookMessage;

typedef struct {
    int             fd;
    pthread_t       thread;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    int             running;
    int             waiting_for_release;
    int             release;
} TestControl;

static TestControl g_test_control;

static int test_control_write_full(int fd, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    while (n > 0) {
        ssize_t w = write(fd, p, n);
        if (w > 0) {
            p += w;
            n -= (size_t)w;
            continue;
        }
        if (w < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

static int test_control_read_full(int fd, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    while (n > 0) {
        ssize_t r = read(fd, p, n);
        if (r > 0) {
            p += r;
            n -= (size_t)r;
            continue;
        }
        if (r < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

/* Runs on the request thread inside the pause hook: reports the call-site
   phase (under_kf_wrlock) and blocks until the control thread broadcasts a
   release. A failed REACHED write releases immediately. */
static void test_control_after_old(int under_kf_wrlock, void *ctx_ptr) {
    TestControl *c = ctx_ptr;
    TestHookMessage reached = { .kind = TEST_HOOK_REACHED,
                                .phase = under_kf_wrlock };
    pthread_mutex_lock(&c->lock);
    c->waiting_for_release = 1;
    c->release = 0;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->lock);

    if (test_control_write_full(c->fd, &reached, sizeof(reached)) != 0) {
        pthread_mutex_lock(&c->lock);
        c->release = 1;
        pthread_cond_broadcast(&c->cond);
        c->waiting_for_release = 0;
        pthread_mutex_unlock(&c->lock);
        return;
    }

    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}

static void *test_control_thread_main(void *arg) {
    TestControl *c = arg;
    for (;;) {
        TestHookMessage msg;
        if (test_control_read_full(c->fd, &msg, sizeof(msg)) != 0)
            break; /* EOF or I/O error */

        switch (msg.kind) {
        case TEST_HOOK_INSTALL:
            slotcask_test_set_after_old_hook(test_control_after_old, c);
            break;
        case TEST_HOOK_RELEASE:
            pthread_mutex_lock(&c->lock);
            c->release = 1;
            pthread_cond_broadcast(&c->cond);
            pthread_mutex_unlock(&c->lock);
            break;
        case TEST_HOOK_CLEAR:
            slotcask_test_set_after_old_hook(NULL, NULL);
            pthread_mutex_lock(&c->lock);
            c->release = 1;
            pthread_cond_broadcast(&c->cond);
            pthread_mutex_unlock(&c->lock);
            break;
        default:
            break; /* malformed message — stop the control thread */
        }

        if (msg.kind == TEST_HOOK_INSTALL || msg.kind == TEST_HOOK_RELEASE ||
            msg.kind == TEST_HOOK_CLEAR) {
            TestHookMessage ack = { .kind = TEST_HOOK_ACK, .phase = 0 };
            if (test_control_write_full(c->fd, &ack, sizeof(ack)) != 0) break;
        } else {
            break;
        }
    }
    return NULL;
}

int shard_db_test_control_start(int fd) {
    if (fd < 0) return 0; /* idempotent */
    memset(&g_test_control, 0, sizeof(g_test_control));
    g_test_control.fd = fd;
    pthread_mutex_init(&g_test_control.lock, NULL);
    pthread_cond_init(&g_test_control.cond, NULL);
    g_test_control.running = 1;
    if (pthread_create(&g_test_control.thread, NULL,
                       test_control_thread_main, &g_test_control) != 0) {
        g_test_control.running = 0;
        close(fd);
        pthread_mutex_destroy(&g_test_control.lock);
        pthread_cond_destroy(&g_test_control.cond);
        return -1;
    }
    return 0;
}

void shard_db_test_control_stop(void) {
    if (g_test_control.fd < 0 || !g_test_control.running) return;
    /* Release a parked callback and prevent any future fire, then wake the
       control thread's blocking read. */
    slotcask_test_set_after_old_hook(NULL, NULL);
    pthread_mutex_lock(&g_test_control.lock);
    g_test_control.running = 0;
    g_test_control.release = 1;
    pthread_cond_broadcast(&g_test_control.cond);
    pthread_mutex_unlock(&g_test_control.lock);
    shutdown(g_test_control.fd, SHUT_RDWR);
    pthread_join(g_test_control.thread, NULL);
    close(g_test_control.fd);
    g_test_control.fd = -1;
    pthread_mutex_destroy(&g_test_control.lock);
    pthread_cond_destroy(&g_test_control.cond);
}

#endif /* TEST_BUILD */
