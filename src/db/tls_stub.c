/* src/db/tls_stub.c — no-op TLS stubs for builds that exclude OpenSSL.
 * Used by the npm/N-API package; the daemon uses the real tls.c instead. */
#include <stdio.h>
#include <stddef.h>

/* Match tls.h's extern declarations without pulling in <openssl/ssl.h>. */
typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st     SSL;

SSL_CTX *g_tls_server_ctx = NULL;
SSL_CTX *g_tls_client_ctx = NULL;

int   tls_server_init(const char *cert_path, const char *key_path) { (void)cert_path; (void)key_path; return -1; }
int   tls_client_init(const char *ca_path, int skip_verify)        { (void)ca_path; (void)skip_verify; return -1; }
void  tls_shutdown(void) {}
SSL  *tls_accept(int fd)                         { (void)fd; return NULL; }
SSL  *tls_connect(int fd, const char *sn)        { (void)fd; (void)sn; return NULL; }
FILE *tls_fopen(SSL *ssl)                        { (void)ssl; return NULL; }
void  tls_close(SSL *ssl, int fd)                { (void)ssl; (void)fd; }
