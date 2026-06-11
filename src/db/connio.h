#ifndef SHARD_DB_CONNIO_H
#define SHARD_DB_CONNIO_H

/* Shared TCP+TLS send/recv loops for both binaries.
 *
 * shard-db's server (src/db/server.c) and the CLI client (src/cli/conn.c)
 * each own their own connection struct, but the actual on-wire I/O is
 * byte-for-byte identical: a write/SSL_write loop that handles partial
 * sends + EINTR + SSL_write's signed-int chunk cap. Same shape for the
 * read side. Header-only (static inline) so the CLI keeps its
 * "no daemon source linked" contract — including this header inlines
 * the bodies without introducing any extern linkage.
 *
 * Header is self-contained — the includes below are pulled in at every
 * call site already, but listing them here makes the file readable in
 * isolation by tooling and avoids implicit-decl warnings. */

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

#ifndef EMBED_NO_TLS
#include <openssl/ssl.h>
#else
/* In embedded/npm builds TLS is never enabled (g_tls_enable = 0),
 * so ssl is always NULL. Provide OpenSSL-free stubs. */
struct ssl_st; typedef struct ssl_st SSL;
#endif

static inline int connio_send_all(int fd, SSL *ssl, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t w = 0;
    while (w < len) {
        int chunk = (int)((len - w) > 0x7FFFFFFF ? 0x7FFFFFFF : (len - w));
        int n;
#ifndef EMBED_NO_TLS
        if (ssl) {
            n = SSL_write(ssl, p + w, chunk);
            if (n <= 0) return -1;
        } else {
#endif
            ssize_t r = write(fd, p + w, (size_t)chunk);
            if (r < 0) { if (errno == EINTR) continue; return -1; }
            if (r == 0) return -1;
            n = (int)r;
#ifndef EMBED_NO_TLS
        }
#endif
        w += (size_t)n;
    }
    return 0;
}

static inline ssize_t connio_recv(int fd, SSL *ssl, void *buf, size_t len) {
#ifndef EMBED_NO_TLS
    if (ssl) {
        int chunk = (int)(len > 0x7FFFFFFF ? 0x7FFFFFFF : len);
        int n = SSL_read(ssl, buf, chunk);
        if (n > 0) return n;
        int err = SSL_get_error(ssl, n);
        if (err == SSL_ERROR_ZERO_RETURN) return 0;
        if (err == SSL_ERROR_SYSCALL && n == 0) return 0;
        return -1;
    }
#else
    (void)ssl;
#endif
    return read(fd, buf, len);
}

#endif
