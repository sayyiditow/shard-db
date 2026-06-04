#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

extern long order_walk_scanned_for_test(void);
extern void order_walk_scanned_reset_for_test(void);
extern int composite_prefix_walk_for_test(const char *db_root, const char *object,
                                           const char *criteria_json,
                                           const char *order_by, int order_desc,
                                           int limit);
extern int composite_prefix_bound_for_test(const char *db_root, const char *object,
                                            const char *criteria_json,
                                            const char *order_by,
                                            uint8_t *out_hi, size_t *out_hi_len,
                                            uint8_t *out_lo, size_t *out_lo_len,
                                            int *out_min_excl, int *out_max_excl);

static int test_composite_varchar_bound_verify(void) {
    TestEnv env = {0};
    TestClient *tc = NULL; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg); ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"v\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"v\",\"object\":\"ob\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:16\",\"t:long\"],"
        "\"indexes\":[\"name\",\"t\",\"name+t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create"); free(resp); resp=NULL;

    for (int i = 0; i < 2000; i++) {
        char req[256];
        const char *nm = (i % 400 == 0) ? "rare" : "rarf";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"v\",\"object\":\"ob\",\"key\":\"k%04d\","
            "\"value\":{\"name\":\"%s\",\"t\":%d}}", i, nm, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    uint8_t hi[1024+8]; size_t hi_len = 0;
    uint8_t lo[1024+8]; size_t lo_len = 0;
    int me = 0, xe = 0;
    int r = composite_prefix_bound_for_test(env.db_root, "v/ob",
        "[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"rare\"}]", "t",
        hi, &hi_len, lo, &lo_len, &me, &xe);
    ASSERT_TRUE(r > 0, "bound computation succeeded");
    /* Lo must be the encoded "rare": 4 bytes 'r','a','r','e' */
    ASSERT_EQ_INT(lo_len, 4, "lower bound length = 4");
    ASSERT_EQ_INT(lo[0], 'r', "lo[0]=r");
    ASSERT_EQ_INT(lo[1], 'a', "lo[1]=a");
    ASSERT_EQ_INT(lo[2], 'r', "lo[2]=r");
    ASSERT_EQ_INT(lo[3], 'e', "lo[3]=e");
    /* Hi must be the successor: "raref" (4 bytes, last byte incremented to 'f').
       The old 0xff*4 approach would produce 8 bytes (r,a,r,e,0xff,0xff,0xff,0xff).
       The successor approach produces 4 bytes (r,a,r,f). */
    ASSERT_TRUE(hi_len < 6, "upper bound is tight (< 6 bytes), not the 8-byte 0xff*4 padding");
    ASSERT_EQ_INT(hi[0], 'r', "hi[0]=r");
    ASSERT_EQ_INT(hi[1], 'a', "hi[1]=a");
    ASSERT_EQ_INT(hi[2], 'r', "hi[2]=r");
    ASSERT_EQ_INT(hi[3], 'f', "hi[3]=f (successor of 'e')");

    /* Also validate correctness through the daemon: query returns only rare rows. */
    order_walk_scanned_reset_for_test();
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"v\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"rare\"}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":5}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"rare\"", "returns rare rows");
    ASSERT_TRUE(strstr(resp, "\"name\":\"rarf\"") == NULL, "no rarf rows returned");
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-composite-varchar-bound", test_composite_varchar_bound_verify)
