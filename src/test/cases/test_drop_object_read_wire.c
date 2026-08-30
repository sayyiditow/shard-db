#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define DELAY_MS 2000

static long dow_now_ms(void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L; }
static int dow_env(const char *p, const char *root, int port, int delay) {
    FILE *f = fopen(p, "w"); if (!f) return -1;
    fprintf(f, "DB_ROOT=%s\nPORT=%d\nTIMEOUT=0\nTHREADS=4\nFCACHE_MAX=4096\nTLS_ENABLE=0\n", root, port);
    if (delay) fprintf(f, "SCHEMA_WRLOCK_TEST_DELAY_MS=%d\n", DELAY_MS);
    return fclose(f);
}
static pid_t dow_spawn(const char *base, const char *bin) { pid_t p = fork(); if (p == 0) { if (chdir(base)) _exit(126); execl(bin, bin, "server", (char *)NULL); _exit(127); } return p; }
static void dow_stop(pid_t p) { if (p <= 0) return; kill(p, SIGTERM); for (int i=0;i<100;i++) { if (waitpid(p, NULL, WNOHANG)==p) return; usleep(100000); } kill(p,SIGKILL); waitpid(p,NULL,0); }
static int dow_ready(int port) { for (int i=0;i<100;i++) { TestClientCfg c={.port=port,.connect_timeout_ms=200}; TestClient *t=tc_connect(&c); if(t) { char *r=NULL; int ok=tc_request(t,"{\"mode\":\"db-dirs\"}",&r)==0; free(r); tc_close(t); if(ok)return 1; } usleep(50000); } return 0; }
static int dow_marker(const char *p) { for(int i=0;i<100;i++) { FILE *f=fopen(p,"r"); if(f) { char b[128]={0}; int ok=fgets(b,sizeof b,f)&&strstr(b,"mode=drop-object"); fclose(f); if(ok)return 1; } usleep(100000); } return 0; }

static int test_drop_object_read_wire_run(void) {
    char base[]="/tmp/shard-db-drop-uaf-XXXXXX"; if(!mkdtemp(base)){ASSERT_TRUE(0,"mkdtemp");return 1;}
    int port=test_pick_port(); char root[PATH_MAX], env[PATH_MAX], marker[PATH_MAX], bin[PATH_MAX];
    snprintf(root,sizeof root,"%s/root",base); mkdir(root,0755); snprintf(env,sizeof env,"%s/db.env",base);
    const char *rel=access("./build/bin/shard-db",X_OK)==0?"./build/bin/shard-db":"./shard-db";
    if(!realpath(rel,bin)){ASSERT_TRUE(0,"shard-db available");return 1;}
    ASSERT_EQ_INT(dow_env(env,root,port,0),0,"write initial env"); pid_t pid=dow_spawn(base,bin); ASSERT_TRUE(dow_ready(port),"round one ready");
    TestClientCfg cfg={.port=port,.io_timeout_ms=30000}; TestClient *t=tc_connect(&cfg); ASSERT_NOT_NULL(t,"round one connect"); if(!t){dow_stop(pid);return 1;}
    char *r=NULL; tc_request(t,"{\"mode\":\"add-dir\",\"dir\":\"default\"}",&r); free(r);
    tc_request(t,"{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"dropuaf\",\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}",&r); free(r);
    tc_request(t,"{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dropuaf\",\"key\":\"k1\",\"value\":{\"name\":\"alice\"}}",&r); free(r); tc_close(t); dow_stop(pid);
    ASSERT_EQ_INT(dow_env(env,root,port,1),0,"write delayed env"); snprintf(marker,sizeof marker,"%s/default/.schema-wrlock-test-delay-dropuaf.active",root); pid=dow_spawn(base,bin); ASSERT_TRUE(dow_ready(port),"round two ready");
    TestClient *drop=tc_connect(&cfg); ASSERT_NOT_NULL(drop,"drop connect"); if(!drop){dow_stop(pid);return 1;}
    ASSERT_EQ_INT(tc_send(drop,"{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"dropuaf\"}"),0,"drop sent");
    int seen=dow_marker(marker); ASSERT_TRUE(seen,"drop-object wrlock marker observed");
    if(seen) {
        TestClient *get=tc_connect(&cfg), *find=tc_connect(&cfg); long start=dow_now_ms();
        ASSERT_TRUE(get&&find,"read clients connect"); if(get) ASSERT_EQ_INT(tc_send(get,"{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"dropuaf\",\"key\":\"k1\"}"),0,"get sent"); if(find) ASSERT_EQ_INT(tc_send(find,"{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"dropuaf\"}"),0,"find sent");
        for(int i=0;i<2;i++){TestClient *c=i?find:get; char *q=NULL; if(c){ASSERT_EQ_INT(tc_recv(c,&q),0,"read response"); ASSERT_TRUE(access(marker,F_OK)!=0,"response after marker gone"); ASSERT_TRUE(dow_now_ms()-start>=DELAY_MS/2,"read blocked behind drop"); ASSERT_TRUE(q&&SAFE_STRSTR(q,"error"),"read reports dropped object"); free(q);tc_close(c);}}
    }
    char *dr=NULL; tc_recv(drop,&dr); free(dr); tc_close(drop); dow_stop(pid); char cmd[PATH_MAX+16]; snprintf(cmd,sizeof cmd,"rm -rf %s",base); system(cmd); return t_ctx->failed?1:0;
}
TEST_REGISTER("test-drop-object-read-wire", test_drop_object_read_wire_run)
