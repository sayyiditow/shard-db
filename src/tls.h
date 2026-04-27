#ifndef SHARD_DB_TLS_H
#define SHARD_DB_TLS_H

#include <stdio.h>
#include <openssl/ssl.h>

/* Native TLS 1.3 wrapper — opaque SSL_CTX globals + helpers.

   Server: tls_server_init() loads cert/key from g_tls_cert / g_tls_key, builds
   a TLS 1.3-only context. Worker thread calls tls_accept(fd) to drive handshake.

   Client: tls_client_init() loads CA bundle from g_tls_ca (or sets verify-none
   if g_tls_skip_verify). CLI calls tls_connect(fd, host) per outbound request.

   FILE* wrapping: tls_fopen(SSL*) returns a buffered FILE* whose stdio reads
   and writes flow through SSL_read/SSL_write transparently — every existing
   OUT() / fgets() call site stays untouched. fclose() on that FILE* drives
   the full teardown (shutdown notify + SSL_free + close fd); callers must
   not also call tls_close() on the same SSL.

   tls_close(ssl, fd) is for the failed-handshake case where SSL exists but
   was never wrapped via tls_fopen. Pass ssl=NULL to just close the fd. */

extern SSL_CTX *g_tls_server_ctx;
extern SSL_CTX *g_tls_client_ctx;

int tls_server_init(const char *cert_path, const char *key_path);
int tls_client_init(const char *ca_path, int skip_verify);
void tls_shutdown(void);

SSL *tls_accept(int fd);
SSL *tls_connect(int fd, const char *server_name);

FILE *tls_fopen(SSL *ssl);
void tls_close(SSL *ssl, int fd);

#endif
