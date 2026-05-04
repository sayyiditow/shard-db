/* src/test/test_client.h
 *
 * Minimal TCP+TLS client used by both shard-db-test and shard-db-bench.
 * Designed for "send one JSON request, get one JSON response" — the
 * daemon's wire protocol terminates each response with \0\n. Connections
 * are persistent: one TestClient handle = one TCP socket = one daemon
 * conversation.
 */
#ifndef TEST_CLIENT_H
#define TEST_CLIENT_H
#include <stddef.h>
#include <stdint.h>

typedef struct TestClient TestClient;

typedef struct {
    const char *host;       /* default "127.0.0.1" if NULL */
    int         port;
    int         use_tls;    /* 1 = TLS 1.3, 0 = plaintext */
    const char *tls_ca;     /* CA bundle path; NULL = OS trust store */
    int         tls_skip_verify; /* dev/test only */
    const char *tls_server_name; /* SNI override; NULL = "localhost" */
    int         connect_timeout_ms; /* default 2000 if 0 */
    int         io_timeout_ms;      /* default 5000 if 0 */
} TestClientCfg;

/* Connect; returns NULL on failure. */
TestClient *tc_connect(const TestClientCfg *cfg);

/* Send a single newline-terminated request, read one \0\n-terminated
   response. Returns 0 on success and writes a heap-allocated NUL-terminated
   string into *out_response (caller frees). On failure returns -1 and
   *out_response is NULL. */
int tc_request(TestClient *tc, const char *json, char **out_response);

/* Send-only and recv-only halves for benches that pipeline. */
int tc_send(TestClient *tc, const char *json);
int tc_recv(TestClient *tc, char **out_response);

void tc_close(TestClient *tc);

#endif
