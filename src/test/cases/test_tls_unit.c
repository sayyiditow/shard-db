#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "tls.h"
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

static int test_tls_unit_run(void) {
    char cert[256], key[256];
    snprintf(cert, sizeof(cert), "/tmp/shard-db-tls-cert-%d.pem", getpid());
    snprintf(key, sizeof(key), "/tmp/shard-db-tls-key-%d.pem", getpid());

    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "openssl ecparam -genkey -name prime256v1 -out %s 2>/dev/null", key);
    ASSERT_EQ_INT(system(cmd), 0, "generate key");
    snprintf(cmd, sizeof(cmd), "openssl req -x509 -new -key %s -out %s -days 1 -subj '/CN=localhost' 2>/dev/null", key, cert);
    ASSERT_EQ_INT(system(cmd), 0, "generate cert");
    ASSERT_TRUE(access(cert, R_OK) == 0, "cert file");
    ASSERT_TRUE(access(key, R_OK) == 0, "key file");

    int ret = tls_server_init(cert, key);
    ASSERT_EQ_INT(ret, 0, "tls_server_init");
    ASSERT_NOT_NULL(g_tls_server_ctx, "server ctx");

    ret = tls_client_init(cert, 1);
    ASSERT_EQ_INT(ret, 0, "tls_client_init");
    ASSERT_NOT_NULL(g_tls_client_ctx, "client ctx");

    tls_shutdown();
    ASSERT_TRUE(g_tls_server_ctx == NULL, "server ctx freed");
    ASSERT_TRUE(g_tls_client_ctx == NULL, "client ctx freed");

    ret = tls_server_init("/nonexistent/cert.pem", key);
    ASSERT_EQ_INT(ret, -1, "tls_server_init bad cert");
    ret = tls_client_init("/nonexistent/ca.pem", 0);
    ASSERT_EQ_INT(ret, -1, "tls_client_init bad ca");
    tls_shutdown();

    tls_close(NULL, -1);

    unlink(cert); unlink(key);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-tls-unit", test_tls_unit_run)