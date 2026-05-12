#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "tls.h"
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>

static const char g_test_cert[] =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIBfTCCASOgAwIBAgIUPTOZ4cIloNZv1Fzb7jSZ5jlpzoswCgYIKoZIzj0EAwIw\n"
    "FDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDUxMjE4MjkyM1oXDTI2MDUxMzE4\n"
    "MjkyM1owFDESMBAGA1UEAwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0D\n"
    "AQcDQgAECOW/v4pEoIsV7btW8TKBH3UoCBbrfGJIm8kUz9p8gp9u2O6y1C2SAq46\n"
    "R1MsrK3cZioN0F5f4140a9gn+QVWPKNTMFEwHQYDVR0OBBYEFF2yq+d9day6zagk\n"
    "XY0VlsuNPgIyMB8GA1UdIwQYMBaAFF2yq+d9day6zagkXY0VlsuNPgIyMA8GA1Ud\n"
    "EwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSAAwRQIgPuprCfgC3w9W0KEzDzuXMr/m\n"
    "so0LILWfg4Ohs+cmDHICIQCkcwfdbiUCZ8Yp+nZ1Q3XyDx827WakaUpQqhFnbaL5\n"
    "5g==\n"
    "-----END CERTIFICATE-----\n";

static const char g_test_key[] =
    "-----BEGIN EC PRIVATE KEY-----\n"
    "MHcCAQEEIEtzGoTP/E+E5bJ/p7iCv6Xw0XKBAYRwTUzzKc0UcDwvoAoGCCqGSM49\n"
    "AwEHoUQDQgAECOW/v4pEoIsV7btW8TKBH3UoCBbrfGJIm8kUz9p8gp9u2O6y1C2S\n"
    "Aq46R1MsrK3cZioN0F5f4140a9gn+QVWPA==\n"
    "-----END EC PRIVATE KEY-----\n";

typedef struct {
    SSL *ssl;
    const char *host;
    int fd;
} TlsThreadCtx;

static void *connect_worker(void *arg) {
    TlsThreadCtx *ctx = (TlsThreadCtx *)arg;
    ctx->ssl = tls_connect(ctx->fd, ctx->host);
    return NULL;
}

static int test_tls_unit_run(void) {
    char cert_path[256], key_path[256];
    snprintf(cert_path, sizeof(cert_path), "/tmp/shard-db-tls-cert-%d.pem", getpid());
    snprintf(key_path, sizeof(key_path), "/tmp/shard-db-tls-key-%d.pem", getpid());

    FILE *f = fopen(cert_path, "w");
    ASSERT_NOT_NULL(f, "open cert file");
    if (f) { fputs(g_test_cert, f); fclose(f); }
    f = fopen(key_path, "w");
    ASSERT_NOT_NULL(f, "open key file");
    if (f) { fputs(g_test_key, f); fclose(f); }

    int ret = tls_server_init(cert_path, key_path);
    ASSERT_EQ_INT(ret, 0, "tls_server_init");
    ASSERT_NOT_NULL(g_tls_server_ctx, "server ctx");

    ret = tls_client_init(cert_path, 1);
    ASSERT_EQ_INT(ret, 0, "tls_client_init");
    ASSERT_NOT_NULL(g_tls_client_ctx, "client ctx");

    int sv[2];
    ASSERT_EQ_INT(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0, "socketpair");

    TlsThreadCtx cctx = { NULL, "localhost", sv[1] };
    pthread_t thr;
    pthread_create(&thr, NULL, connect_worker, &cctx);
    SSL *srv_ssl = tls_accept(sv[0]);
    ASSERT_NOT_NULL(srv_ssl, "tls_accept");
    pthread_join(thr, NULL);
    SSL *cli_ssl = cctx.ssl;
    ASSERT_NOT_NULL(cli_ssl, "tls_connect");

    FILE *srv_f = tls_fopen(srv_ssl);
    FILE *cli_f = tls_fopen(cli_ssl);
    ASSERT_NOT_NULL(srv_f, "tls_fopen server");
    ASSERT_NOT_NULL(cli_f, "tls_fopen client");

    fprintf(cli_f, "ping\n");
    fflush(cli_f);
    char buf[64] = {0};
    ASSERT_NOT_NULL(fgets(buf, sizeof(buf), srv_f), "tls read");
    ASSERT_EQ_STR(buf, "ping\n", "tls roundtrip");

    fclose(srv_f);
    fclose(cli_f);
    tls_shutdown();
    ASSERT_TRUE(g_tls_server_ctx == NULL, "server ctx freed");
    ASSERT_TRUE(g_tls_client_ctx == NULL, "client ctx freed");

    ret = tls_server_init("/nonexistent/cert.pem", key_path);
    ASSERT_EQ_INT(ret, -1, "tls_server_init bad cert");
    ret = tls_client_init("/nonexistent/ca.pem", 0);
    ASSERT_EQ_INT(ret, -1, "tls_client_init bad ca");
    tls_shutdown();
    tls_close(NULL, -1);
    unlink(cert_path); unlink(key_path);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-tls-unit", test_tls_unit_run)