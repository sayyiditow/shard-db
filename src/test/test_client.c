/* src/test/test_client.c */
#include "test_client.h"
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

struct TestClient {
    int fd;
    SSL_CTX *ctx;
    SSL *ssl;
    int io_timeout_ms;
};

static void apply_io_timeout(int fd, int ms) {
    struct timeval tv = { .tv_sec = ms / 1000, .tv_usec = (ms % 1000) * 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

TestClient *tc_connect(const TestClientCfg *cfg) {
    if (!cfg || cfg->port <= 0) return NULL;
    const char *host = cfg->host ? cfg->host : "127.0.0.1";
    int connect_ms = cfg->connect_timeout_ms > 0 ? cfg->connect_timeout_ms : 2000;
    int io_ms = cfg->io_timeout_ms > 0 ? cfg->io_timeout_ms : 5000;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", cfg->port);

    struct addrinfo hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM };
    struct addrinfo *res = NULL;
    if (getaddrinfo(host, port_str, &hints, &res) != 0) return NULL;

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { freeaddrinfo(res); return NULL; }
    apply_io_timeout(fd, connect_ms);

    if (connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        freeaddrinfo(res); close(fd); return NULL;
    }
    freeaddrinfo(res);
    apply_io_timeout(fd, io_ms);

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    TestClient *tc = calloc(1, sizeof(*tc));
    if (!tc) { close(fd); return NULL; }
    tc->fd = fd;
    tc->io_timeout_ms = io_ms;

    if (cfg->use_tls) {
        tc->ctx = SSL_CTX_new(TLS_client_method());
        if (!tc->ctx) { close(fd); free(tc); return NULL; }
        SSL_CTX_set_min_proto_version(tc->ctx, TLS1_3_VERSION);
        if (cfg->tls_ca) SSL_CTX_load_verify_locations(tc->ctx, cfg->tls_ca, NULL);
        else SSL_CTX_set_default_verify_paths(tc->ctx);
        if (cfg->tls_skip_verify) SSL_CTX_set_verify(tc->ctx, SSL_VERIFY_NONE, NULL);
        else SSL_CTX_set_verify(tc->ctx, SSL_VERIFY_PEER, NULL);

        tc->ssl = SSL_new(tc->ctx);
        SSL_set_fd(tc->ssl, fd);
        SSL_set_tlsext_host_name(tc->ssl,
            cfg->tls_server_name ? cfg->tls_server_name : "localhost");
        if (SSL_connect(tc->ssl) != 1) {
            SSL_free(tc->ssl); SSL_CTX_free(tc->ctx); close(fd); free(tc);
            return NULL;
        }
    }
    return tc;
}

static int tc_write_all(TestClient *tc, const char *buf, size_t len) {
    while (len > 0) {
        ssize_t n = tc->ssl ? SSL_write(tc->ssl, buf, (int)len)
                            : send(tc->fd, buf, len, 0);
        if (n <= 0) return -1;
        buf += n;
        len -= (size_t)n;
    }
    return 0;
}

int tc_send(TestClient *tc, const char *json) {
    if (!tc || !json) return -1;
    size_t len = strlen(json);
    int rc = tc_write_all(tc, json, len);
    if (rc != 0) return rc;
    /* Daemon expects newline-terminated requests. */
    if (len == 0 || json[len - 1] != '\n')
        return tc_write_all(tc, "\n", 1);
    return 0;
}

int tc_recv(TestClient *tc, char **out_response) {
    if (!tc || !out_response) return -1;
    *out_response = NULL;
    size_t cap = 4096, pos = 0;
    char *buf = malloc(cap);
    if (!buf) return -1;
    /* Read until we see the daemon's \0\n separator. */
    while (1) {
        if (pos + 1 >= cap) {
            cap *= 2;
            char *t = realloc(buf, cap);
            if (!t) { free(buf); return -1; }
            buf = t;
        }
        ssize_t n = tc->ssl ? SSL_read(tc->ssl, buf + pos, (int)(cap - pos - 1))
                            : recv(tc->fd, buf + pos, cap - pos - 1, 0);
        if (n <= 0) { free(buf); return -1; }
        pos += (size_t)n;
        /* Detect terminator. */
        for (size_t i = (pos > (size_t)n + 1 ? pos - (size_t)n - 1 : 0); i + 1 < pos; i++) {
            if (buf[i] == '\0' && buf[i + 1] == '\n') {
                buf[i] = '\0';
                *out_response = buf;
                return 0;
            }
        }
    }
}

int tc_request(TestClient *tc, const char *json, char **out_response) {
    if (tc_send(tc, json) != 0) return -1;
    return tc_recv(tc, out_response);
}

void tc_close(TestClient *tc) {
    if (!tc) return;
    if (tc->ssl) { SSL_shutdown(tc->ssl); SSL_free(tc->ssl); }
    if (tc->ctx) SSL_CTX_free(tc->ctx);
    if (tc->fd >= 0) close(tc->fd);
    free(tc);
}
