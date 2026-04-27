#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "tls.h"

#include <errno.h>
#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

SSL_CTX *g_tls_server_ctx = NULL;
SSL_CTX *g_tls_client_ctx = NULL;

static void tls_log_err(const char *what) {
    unsigned long e = ERR_get_error();
    char buf[256] = {0};
    if (e) ERR_error_string_n(e, buf, sizeof(buf));
    fprintf(stderr, "tls: %s%s%s\n", what,
            buf[0] ? ": " : "",
            buf[0] ? buf : "");
}

int tls_server_init(const char *cert_path, const char *key_path) {
    SSL_load_error_strings();
    OpenSSL_add_ssl_algorithms();

    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) { tls_log_err("SSL_CTX_new(server)"); return -1; }

    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        tls_log_err("set_min_proto_version(1.3)"); SSL_CTX_free(ctx); return -1;
    }
    if (SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION) != 1) {
        tls_log_err("set_max_proto_version(1.3)"); SSL_CTX_free(ctx); return -1;
    }

    if (SSL_CTX_use_certificate_chain_file(ctx, cert_path) != 1) {
        tls_log_err("use_certificate_chain_file"); SSL_CTX_free(ctx); return -1;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, key_path, SSL_FILETYPE_PEM) != 1) {
        tls_log_err("use_PrivateKey_file"); SSL_CTX_free(ctx); return -1;
    }
    if (SSL_CTX_check_private_key(ctx) != 1) {
        tls_log_err("check_private_key (cert and key do not match)"); SSL_CTX_free(ctx); return -1;
    }

    SSL_CTX_set_mode(ctx, SSL_MODE_AUTO_RETRY | SSL_MODE_ENABLE_PARTIAL_WRITE);

    g_tls_server_ctx = ctx;
    return 0;
}

int tls_client_init(const char *ca_path, int skip_verify) {
    SSL_load_error_strings();
    OpenSSL_add_ssl_algorithms();

    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) { tls_log_err("SSL_CTX_new(client)"); return -1; }

    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        tls_log_err("set_min_proto_version(1.3)"); SSL_CTX_free(ctx); return -1;
    }

    if (skip_verify) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
    } else {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
        if (ca_path && ca_path[0]) {
            if (SSL_CTX_load_verify_locations(ctx, ca_path, NULL) != 1) {
                tls_log_err("load_verify_locations"); SSL_CTX_free(ctx); return -1;
            }
        } else {
            /* Fall back to OS default trust store. */
            if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
                tls_log_err("set_default_verify_paths"); SSL_CTX_free(ctx); return -1;
            }
        }
    }

    SSL_CTX_set_mode(ctx, SSL_MODE_AUTO_RETRY | SSL_MODE_ENABLE_PARTIAL_WRITE);

    g_tls_client_ctx = ctx;
    return 0;
}

void tls_shutdown(void) {
    if (g_tls_server_ctx) { SSL_CTX_free(g_tls_server_ctx); g_tls_server_ctx = NULL; }
    if (g_tls_client_ctx) { SSL_CTX_free(g_tls_client_ctx); g_tls_client_ctx = NULL; }
}

SSL *tls_accept(int fd) {
    if (!g_tls_server_ctx) return NULL;
    SSL *ssl = SSL_new(g_tls_server_ctx);
    if (!ssl) { tls_log_err("SSL_new(accept)"); return NULL; }
    if (SSL_set_fd(ssl, fd) != 1) { tls_log_err("SSL_set_fd(accept)"); SSL_free(ssl); return NULL; }
    /* Blocking handshake — server worker is blocking I/O via fgets/fwrite. */
    if (SSL_accept(ssl) != 1) {
        tls_log_err("SSL_accept");
        SSL_free(ssl);
        return NULL;
    }
    return ssl;
}

SSL *tls_connect(int fd, const char *server_name) {
    if (!g_tls_client_ctx) return NULL;
    SSL *ssl = SSL_new(g_tls_client_ctx);
    if (!ssl) { tls_log_err("SSL_new(connect)"); return NULL; }
    if (SSL_set_fd(ssl, fd) != 1) { tls_log_err("SSL_set_fd(connect)"); SSL_free(ssl); return NULL; }
    if (server_name && server_name[0]) {
        SSL_set_tlsext_host_name(ssl, server_name);
        SSL_set1_host(ssl, server_name);
    }
    if (SSL_connect(ssl) != 1) {
        tls_log_err("SSL_connect");
        SSL_free(ssl);
        return NULL;
    }
    return ssl;
}

/* ---------- FILE* wrapping ----------
   The codebase routes ~460 OUT() / fgets() calls through stdio FILE*. Wrapping
   the SSL* as a custom FILE via fopencookie (Linux) / funopen (macOS) keeps
   every call site untouched — stdio reads/writes flow through SSL_read /
   SSL_write transparently. */

/* Shared dispose: shutdown notify + SSL_free + close fd. Used by the cookie
   close hook (after tls_fopen) and by tls_close() (failed-handshake path). */
static void tls_dispose(SSL *ssl) {
    if (!ssl) return;
    int fd = SSL_get_fd(ssl);
    int rc = SSL_shutdown(ssl);
    if (rc == 0) SSL_shutdown(ssl);  /* one extra round if peer not yet sent */
    SSL_free(ssl);
    if (fd >= 0) close(fd);
}

#ifdef __APPLE__

static int tls_cookie_read_apple(void *cookie, char *buf, int size) {
    int n = SSL_read((SSL *)cookie, buf, size);
    if (n > 0) return n;
    int err = SSL_get_error((SSL *)cookie, n);
    if (err == SSL_ERROR_ZERO_RETURN) return 0;
    if (err == SSL_ERROR_SYSCALL && n == 0) return 0;
    errno = EIO; return -1;
}

static int tls_cookie_write_apple(void *cookie, const char *buf, int size) {
    int n = SSL_write((SSL *)cookie, buf, size);
    if (n > 0) return n;
    errno = EIO; return -1;
}

static int tls_cookie_close_apple(void *cookie) {
    tls_dispose((SSL *)cookie);
    return 0;
}

FILE *tls_fopen(SSL *ssl) {
    return funopen(ssl, tls_cookie_read_apple, tls_cookie_write_apple, NULL, tls_cookie_close_apple);
}

#else  /* Linux / glibc */

static ssize_t tls_cookie_read(void *cookie, char *buf, size_t size) {
    int n = SSL_read((SSL *)cookie, buf, size > INT_MAX ? INT_MAX : (int)size);
    if (n > 0) return n;
    int err = SSL_get_error((SSL *)cookie, n);
    if (err == SSL_ERROR_ZERO_RETURN) return 0;
    if (err == SSL_ERROR_SYSCALL && n == 0) return 0;
    errno = EIO; return -1;
}

static ssize_t tls_cookie_write(void *cookie, const char *buf, size_t size) {
    int n = SSL_write((SSL *)cookie, buf, size > INT_MAX ? INT_MAX : (int)size);
    if (n > 0) return n;
    errno = EIO; return -1;
}

static int tls_cookie_close(void *cookie) {
    tls_dispose((SSL *)cookie);
    return 0;
}

static cookie_io_functions_t tls_cookie_funcs = {
    .read  = tls_cookie_read,
    .write = tls_cookie_write,
    .seek  = NULL,
    .close = tls_cookie_close,
};

FILE *tls_fopen(SSL *ssl) {
    return fopencookie(ssl, "r+", tls_cookie_funcs);
}

#endif

void tls_close(SSL *ssl, int fd) {
    if (!ssl) { if (fd >= 0) close(fd); return; }
    tls_dispose(ssl);
    /* tls_dispose closes ssl's fd via SSL_get_fd; the fd parameter is used
       only when ssl is NULL (caller sets it explicitly). Avoid double-close. */
    (void)fd;
}
