/* TCP+TLS connection helper for shard-cli. Mirrors server.c::ClientConn but
   self-contained so shard-cli doesn't link against the daemon's full source. */

#define _GNU_SOURCE
#include "cli.h"

#include <unistd.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

char g_cli_host[128]   = "127.0.0.1";
int  g_cli_port        = 9199;
int  g_cli_tls_enable  = 0;
char g_cli_token[256]  = "";

static SSL_CTX *g_ssl_ctx = NULL;

struct CliConn {
    int  fd;
    SSL *ssl;
};

void cli_load_env(void) {
    const char *host = getenv("HOST");
    if (host && *host) {
        strncpy(g_cli_host, host, sizeof(g_cli_host) - 1);
        g_cli_host[sizeof(g_cli_host) - 1] = '\0';
    }
    const char *port = getenv("PORT");
    if (port && *port) g_cli_port = atoi(port);
    const char *tls  = getenv("TLS_ENABLE");
    if (tls && *tls)  g_cli_tls_enable = atoi(tls);
    const char *tok  = getenv("TOKEN");
    if (tok && *tok) {
        strncpy(g_cli_token, tok, sizeof(g_cli_token) - 1);
        g_cli_token[sizeof(g_cli_token) - 1] = '\0';
    }
}

static int ensure_ssl_ctx(void) {
    if (g_ssl_ctx) return 0;
    SSL_load_error_strings();
    OpenSSL_add_ssl_algorithms();
    g_ssl_ctx = SSL_CTX_new(TLS_client_method());
    if (!g_ssl_ctx) return -1;
    SSL_CTX_set_min_proto_version(g_ssl_ctx, TLS1_3_VERSION);
    const char *ca = getenv("TLS_CA");
    int skip_verify = 0;
    const char *sv = getenv("TLS_SKIP_VERIFY");
    if (sv && atoi(sv)) skip_verify = 1;
    if (skip_verify) {
        SSL_CTX_set_verify(g_ssl_ctx, SSL_VERIFY_NONE, NULL);
    } else {
        SSL_CTX_set_verify(g_ssl_ctx, SSL_VERIFY_PEER, NULL);
        if (ca && *ca) {
            if (SSL_CTX_load_verify_locations(g_ssl_ctx, ca, NULL) != 1) {
                SSL_CTX_free(g_ssl_ctx); g_ssl_ctx = NULL;
                return -1;
            }
        } else {
            SSL_CTX_set_default_verify_paths(g_ssl_ctx);
        }
    }
    return 0;
}

/* Conservative loopback check: host strings that are unambiguously the
   local machine. Used to gate plaintext-token sends. We deliberately
   only allowlist literal forms (no DNS lookup): a remote that resolves
   to 127.0.0.1 via /etc/hosts is still treated as remote so the token
   can't leak via misconfigured DNS. */
static int is_loopback_host(const char *host) {
    if (!host || !*host) return 0;
    return strcmp(host, "127.0.0.1") == 0 ||
           strcmp(host, "::1")       == 0 ||
           strcmp(host, "localhost") == 0;
}

CliConn *cli_connect(const char *host, int port) {
    /* Refuse to send a bearer token over plaintext to a non-loopback peer.
       The token would otherwise traverse the network in clear text — any
       on-path observer (a forwarding proxy, an intermediate router with
       traffic capture, an attacker on the same Wi-Fi) would see it.
       Either flip TLS_ENABLE=1 + provide TLS_CA, or accept the loopback
       restriction. CodeQL flags the env-var-token → write() dataflow as
       "system data exposure" — this guard makes that dataflow safe by
       construction for the only environment where plaintext is sane. */
    if (g_cli_token[0] != '\0' &&
        !g_cli_tls_enable &&
        !is_loopback_host(host)) {
        fprintf(stderr,
                "shard-cli: refusing to send TOKEN to %s without TLS — "
                "set TLS_ENABLE=1 (and TLS_CA) or connect to localhost\n",
                host);
        return NULL;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return NULL;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        struct hostent *he = gethostbyname(host);
        if (!he) { close(fd); return NULL; }
        memcpy(&addr.sin_addr, he->h_addr, sizeof(addr.sin_addr));
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd); return NULL;
    }

    CliConn *c = calloc(1, sizeof(*c));
    if (!c) { close(fd); return NULL; }
    c->fd = fd;
    c->ssl = NULL;

    if (g_cli_tls_enable) {
        if (ensure_ssl_ctx() != 0) { close(fd); free(c); return NULL; }
        SSL *ssl = SSL_new(g_ssl_ctx);
        if (!ssl) { close(fd); free(c); return NULL; }
        SSL_set_fd(ssl, fd);
        const char *sni = getenv("TLS_SERVER_NAME");
        if (!sni || !*sni) sni = host;
        SSL_set_tlsext_host_name(ssl, sni);
        SSL_set1_host(ssl, sni);
        if (SSL_connect(ssl) != 1) {
            SSL_free(ssl); close(fd); free(c); return NULL;
        }
        c->ssl = ssl;
    }
    return c;
}

void cli_close(CliConn *c) {
    if (!c) return;
    if (c->ssl) {
        SSL_shutdown(c->ssl);
        SSL_free(c->ssl);
    }
    if (c->fd >= 0) close(c->fd);
    free(c);
}

static int conn_send_all(CliConn *c, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t w = 0;
    while (w < len) {
        int chunk = (int)((len - w) > 0x7FFFFFFF ? 0x7FFFFFFF : (len - w));
        int n;
        if (c->ssl) {
            n = SSL_write(c->ssl, p + w, chunk);
            if (n <= 0) return -1;
        } else {
            ssize_t r = write(c->fd, p + w, (size_t)chunk);
            if (r < 0) { if (errno == EINTR) continue; return -1; }
            if (r == 0) return -1;
            n = (int)r;
        }
        w += (size_t)n;
    }
    return 0;
}

static ssize_t conn_recv(CliConn *c, void *buf, size_t len) {
    if (c->ssl) {
        int n = SSL_read(c->ssl, buf, (int)(len > 0x7FFFFFFF ? 0x7FFFFFFF : len));
        if (n > 0) return n;
        int err = SSL_get_error(c->ssl, n);
        if (err == SSL_ERROR_ZERO_RETURN) return 0;
        if (err == SSL_ERROR_SYSCALL && n == 0) return 0;
        return -1;
    }
    return read(c->fd, buf, len);
}

/* If g_cli_token is set, splice "auth":"..." into the request before the
   closing brace. Caller-provided JSON must NOT already contain "auth". */
static char *inject_auth(const char *request_json, size_t *out_len) {
    size_t inlen = strlen(request_json);
    if (g_cli_token[0] == '\0' || inlen < 2 || request_json[inlen - 1] != '}') {
        char *copy = strdup(request_json);
        *out_len = inlen;
        return copy;
    }
    /* Wedge "auth":"<token>", in just before the closing }. Strip trailing }
       then re-append. Naive but works for the well-formed JSON we send. */
    size_t tlen = strlen(g_cli_token);
    size_t cap = inlen + tlen + 16;
    char *out = malloc(cap);
    if (!out) { *out_len = 0; return NULL; }
    /* Find last }. */
    size_t end = inlen - 1;
    /* If the body is "{}" we want {"auth":"..."}.
       Otherwise insert `,"auth":"..."` before the last }. */
    int empty_body = (inlen == 2 && request_json[0] == '{' && request_json[1] == '}');
    int n;
    if (empty_body) {
        n = snprintf(out, cap, "{\"auth\":\"%s\"}", g_cli_token);
    } else {
        n = snprintf(out, cap, "%.*s,\"auth\":\"%s\"}",
                     (int)end, request_json, g_cli_token);
    }
    *out_len = (size_t)n;
    return out;
}

int cli_query(CliConn *c, const char *request_json,
              char **out_resp, size_t *out_len) {
    if (!c || !request_json) return -1;

    size_t req_len;
    char *req = inject_auth(request_json, &req_len);
    if (!req) return -1;

    if (conn_send_all(c, req, req_len) != 0 ||
        conn_send_all(c, "\n", 1) != 0) {
        free(req); return -1;
    }
    free(req);

    /* Accumulate response up to first \0. */
    size_t cap = 8192, len = 0;
    char *buf = malloc(cap);
    if (!buf) return -1;

    char rbuf[8192];
    ssize_t n;
    while ((n = conn_recv(c, rbuf, sizeof(rbuf))) > 0) {
        for (ssize_t j = 0; j < n; j++) {
            if (rbuf[j] == '\0') {
                if (len + j > cap) {
                    while (cap < len + j) cap *= 2;
                    char *nb = realloc(buf, cap);
                    if (!nb) { free(buf); return -1; }
                    buf = nb;
                }
                memcpy(buf + len, rbuf, j);
                len += j;
                if (len + 1 >= cap) buf = realloc(buf, len + 1);
                buf[len] = '\0';
                *out_resp = buf; *out_len = len;
                return 0;
            }
        }
        if (len + (size_t)n + 1 > cap) {
            while (cap < len + (size_t)n + 1) cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); return -1; }
            buf = nb;
        }
        memcpy(buf + len, rbuf, n);
        len += (size_t)n;
    }
    if (len + 1 > cap) buf = realloc(buf, len + 1);
    buf[len] = '\0';
    *out_resp = buf; *out_len = len;
    return 0;
}
