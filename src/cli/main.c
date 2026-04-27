/* shard-cli main entry — parses env (db.env should be sourced), opens a
   connection (lazy on first action), drives the top-level menu. */

#define _GNU_SOURCE
#include "cli.h"
#include <unistd.h>
#include <sys/wait.h>

/* ---- helpers ---- */

static CliConn *g_conn = NULL;

static CliConn *get_conn(void) {
    if (g_conn) return g_conn;
    g_conn = cli_connect(g_cli_host, g_cli_port);
    if (!g_conn)
        tui_alert("connection failed",
                  "Could not connect. Is the daemon running on the expected host:port?\n"
                  "Source db.env (or set HOST/PORT/TLS_ENABLE) and run ./shard-db start.");
    return g_conn;
}

static void show_response(const char *title, const char *json) {
    if (!json || !*json) { tui_alert(title, "(empty response)"); return; }
    /* Show as kv-pairs if it parses as object; else raw. */
    if (json[0] == '{') tui_show_object(title, json);
    else                tui_show_text(title, json);
}

/* Run an external command and capture output via popen. */
static char *run_capture(const char *cmd) {
    FILE *f = popen(cmd, "r");
    if (!f) return strdup("(popen failed)");
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) { pclose(f); return NULL; }
    int ch;
    while ((ch = fgetc(f)) != EOF) {
        if (len + 1 >= cap) {
            cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); pclose(f); return NULL; }
            buf = nb;
        }
        buf[len++] = (char)ch;
    }
    pclose(f);
    buf[len] = '\0';
    return buf;
}

/* ---- Server menu ---- */

static void menu_server(void) {
    MenuItem items[] = {
        { "start",   "Daemonize ./shard-db on PORT" },
        { "stop",    "SIGTERM the running daemon (drains in-flight writes)" },
        { "status",  "Print PID/port if running" },
    };
    int choice = tui_menu("Server", items, 3);
    if (choice < 0) return;
    const char *cmd = NULL;
    switch (choice) {
        case 0: cmd = "./shard-db start  2>&1"; break;
        case 1: cmd = "./shard-db stop   2>&1"; break;
        case 2: cmd = "./shard-db status 2>&1"; break;
    }
    char *out = run_capture(cmd);
    /* Re-open connection on next request — the daemon may have just stopped/started. */
    if (g_conn) { cli_close(g_conn); g_conn = NULL; }
    tui_alert(items[choice].label, out ? out : "(no output)");
    free(out);
}

/* ---- Browse menu ---- */

static void browse_object(CliConn *c, const char *dir, const char *object) {
    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"describe-object\",\"dir\":\"%s\",\"object\":\"%s\"}",
             dir, object);
    char *resp = NULL;
    size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) {
        tui_alert("error", "describe-object failed");
        return;
    }
    char title[128];
    snprintf(title, sizeof(title), "%s/%s", dir, object);
    show_response(title, resp);
    free(resp);
}

static int collect_string_array(const char *resp, const char *key,
                                char ***out_arr, int *out_n) {
    /* Parse {"key":"...","key2":[...]} where key2 is an array of strings.
       Or top-level array if key is NULL. */
    const char *arr = resp;
    if (key) {
        size_t vlen;
        extern const char *json_find_key_extern(const char *, const char *, size_t *);
        /* Inline parse — duplicate of json_find_key in views.c kept private. */
        const char *p = resp;
        while (*p && *p != '[') {
            if (strncmp(p, key, strlen(key)) == 0 &&
                p > resp && p[-1] == '"') {
                p += strlen(key);
                while (*p && *p != '[' && *p != '{') p++;
                arr = p;
                break;
            }
            p++;
        }
        (void)vlen;
    }
    if (!arr || *arr != '[') return -1;
    /* Walk array. */
    int cap = 16, n = 0;
    char **out = malloc(cap * sizeof(char *));
    const char *p = arr + 1;
    while (*p) {
        while (*p == ' ' || *p == ',' || *p == '\n' || *p == '\t') p++;
        if (*p == ']' || *p == '\0') break;
        if (*p != '"') break;
        p++;
        const char *s = p;
        while (*p && *p != '"') p++;
        size_t L = p - s;
        if (n >= cap) { cap *= 2; out = realloc(out, cap * sizeof(char *)); }
        out[n] = malloc(L + 1);
        memcpy(out[n], s, L);
        out[n][L] = '\0';
        n++;
        if (*p == '"') p++;
    }
    *out_arr = out;
    *out_n = n;
    return 0;
}

static void menu_browse_object_list(CliConn *c, const char *dir) {
    char req[256];
    snprintf(req, sizeof(req), "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dir);
    char *resp = NULL;
    size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) {
        tui_alert("error", "list-objects failed");
        return;
    }
    char **objs;
    int n = 0;
    if (collect_string_array(resp, "objects", &objs, &n) != 0 || n == 0) {
        if (n == 0) tui_alert(dir, "(no objects in this tenant)");
        else        tui_alert("parse error", resp);
        free(resp);
        return;
    }
    free(resp);
    char title[128];
    snprintf(title, sizeof(title), "%s — pick object", dir);
    /* Make a const view of objs for tui_pick. */
    const char **view = malloc(n * sizeof(*view));
    for (int i = 0; i < n; i++) view[i] = objs[i];
    const char *picked = tui_pick(title, view, n);
    free(view);
    if (picked) browse_object(c, dir, picked);
    for (int i = 0; i < n; i++) free(objs[i]);
    free(objs);
}

static void menu_browse(void) {
    CliConn *c = get_conn();
    if (!c) return;
    char *resp = NULL;
    size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"db-dirs\"}", &resp, &rlen) != 0) {
        tui_alert("error", "db-dirs failed");
        return;
    }
    char **dirs;
    int n = 0;
    if (collect_string_array(resp, NULL, &dirs, &n) != 0 || n == 0) {
        if (n == 0) tui_alert("Browse", "(no allowed tenants — add to dirs.conf)");
        else        tui_alert("parse error", resp);
        free(resp);
        return;
    }
    free(resp);
    const char **view = malloc(n * sizeof(*view));
    for (int i = 0; i < n; i++) view[i] = dirs[i];
    const char *picked = tui_pick("Browse — pick tenant", view, n);
    free(view);
    if (picked) menu_browse_object_list(c, picked);
    for (int i = 0; i < n; i++) free(dirs[i]);
    free(dirs);
}

/* ---- Query menu ---- */

/* Pick a tenant + object via two list pickers; populate ObjectInfo. Returns
   0 on success, -1 if the user backs out at any step. */
static int pick_object(CliConn *c, ObjectInfo *oi) {
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"db-dirs\"}", &resp, &rlen) != 0) {
        tui_alert("error", "db-dirs failed");
        return -1;
    }
    char **dirs = NULL; int ndirs = 0;
    collect_string_array(resp, NULL, &dirs, &ndirs);
    free(resp);
    if (ndirs == 0) { tui_alert("query", "no tenants — add to dirs.conf"); return -1; }

    const char **vd = malloc(ndirs * sizeof(*vd));
    for (int i = 0; i < ndirs; i++) vd[i] = dirs[i];
    const char *dir = tui_pick("pick tenant", vd, ndirs);
    free(vd);
    if (!dir) {
        for (int i = 0; i < ndirs; i++) free(dirs[i]);
        free(dirs);
        return -1;
    }
    char dir_copy[64];
    snprintf(dir_copy, sizeof(dir_copy), "%s", dir);
    for (int i = 0; i < ndirs; i++) free(dirs[i]);
    free(dirs);

    char req[256];
    snprintf(req, sizeof(req), "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dir_copy);
    if (cli_query(c, req, &resp, &rlen) != 0) {
        tui_alert("error", "list-objects failed");
        return -1;
    }
    char **objs = NULL; int nobjs = 0;
    collect_string_array(resp, "objects", &objs, &nobjs);
    free(resp);
    if (nobjs == 0) { tui_alert(dir_copy, "(no objects)"); return -1; }

    const char **vo = malloc(nobjs * sizeof(*vo));
    for (int i = 0; i < nobjs; i++) vo[i] = objs[i];
    const char *obj = tui_pick("pick object", vo, nobjs);
    free(vo);
    char obj_copy[64] = "";
    if (obj) snprintf(obj_copy, sizeof(obj_copy), "%s", obj);
    for (int i = 0; i < nobjs; i++) free(objs[i]);
    free(objs);
    if (!obj) return -1;

    if (describe_object(c, dir_copy, obj_copy, oi) != 0) {
        tui_alert("error", "describe-object failed");
        return -1;
    }
    return 0;
}

static void query_insert(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    /* Form: key + one row per writable field. */
    int nf = oi.nfields + 1;
    FormField *fs = calloc(nf, sizeof(*fs));
    fs[0].label = "key";
    fs[0].kind  = FF_TEXT;
    for (int i = 0; i < oi.nfields; i++) {
        fs[1 + i].label = oi.fields[i].name;
        fs[1 + i].kind  = FF_TEXT;
    }
    char title[128];
    snprintf(title, sizeof(title), "insert into %s/%s", oi.dir, oi.object);
    int rc = tui_form(title, fs, nf);
    if (rc != 0) { free(fs); return; }
    if (!fs[0].value[0]) { tui_alert("insert", "key required"); free(fs); return; }

    /* Build {"mode":"insert","dir":"...","object":"...","key":"...","data":{...}} */
    size_t cap = 4096; char *req = malloc(cap); size_t off = 0;
    off += snprintf(req + off, cap - off,
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\",\"key\":\"%s\",\"data\":{",
        oi.dir, oi.object, fs[0].value);
    int emitted = 0;
    for (int i = 0; i < oi.nfields; i++) {
        if (!fs[1 + i].value[0]) continue;  /* skip empty — defaults apply */
        if (off + 256 + strlen(fs[1 + i].value) >= cap) {
            cap *= 2;
            req = realloc(req, cap);
        }
        off += snprintf(req + off, cap - off,
            "%s\"%s\":\"%s\"",
            emitted ? "," : "",
            oi.fields[i].name, fs[1 + i].value);
        emitted++;
    }
    off += snprintf(req + off, cap - off, "}}");
    free(fs);

    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) {
        tui_alert("error", "insert failed");
    } else {
        show_response("insert result", resp);
        free(resp);
    }
    free(req);
}

static void query_get(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    FormField fs[1] = {0};
    fs[0].label = "key";
    fs[0].kind  = FF_TEXT;
    char title[128];
    snprintf(title, sizeof(title), "get from %s/%s", oi.dir, oi.object);
    if (tui_form(title, fs, 1) != 0) return;
    if (!fs[0].value[0]) { tui_alert("get", "key required"); return; }

    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"%s\",\"object\":\"%s\",\"key\":\"%s\"}",
        oi.dir, oi.object, fs[0].value);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) { tui_alert("error", "get failed"); return; }
    show_response("get result", resp);
    free(resp);
}

static void query_exists(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    FormField fs[1] = {0};
    fs[0].label = "key";
    fs[0].kind  = FF_TEXT;
    char title[128];
    snprintf(title, sizeof(title), "exists in %s/%s", oi.dir, oi.object);
    if (tui_form(title, fs, 1) != 0) return;
    if (!fs[0].value[0]) { tui_alert("exists", "key required"); return; }

    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"exists\",\"dir\":\"%s\",\"object\":\"%s\",\"key\":\"%s\"}",
        oi.dir, oi.object, fs[0].value);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) { tui_alert("error", "exists failed"); return; }
    show_response("exists", resp);
    free(resp);
}

static void query_count(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    char *crit = NULL;
    if (tui_criteria_builder(&oi, &crit) != 0) return;

    char req[8192];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\",\"criteria\":%s}",
        oi.dir, oi.object, crit);
    free(crit);

    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) { tui_alert("error", "count failed"); return; }
    show_response("count result", resp);
    free(resp);
}

static void query_find(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    char *crit = NULL;
    if (tui_criteria_builder(&oi, &crit) != 0) return;

    /* Second form: offset, limit, fields (CSV). */
    FormField fs[3] = {0};
    fs[0].label = "offset"; fs[0].kind = FF_NUMBER; snprintf(fs[0].value, sizeof(fs[0].value), "0");
    fs[1].label = "limit";  fs[1].kind = FF_NUMBER; snprintf(fs[1].value, sizeof(fs[1].value), "50");
    fs[2].label = "fields"; fs[2].kind = FF_TEXT;   /* comma-separated, blank=all */
    if (tui_form("find — paging", fs, 3) != 0) { free(crit); return; }

    /* Build fields JSON array if user typed any. */
    char fields_json[1024] = "[]";
    if (fs[2].value[0]) {
        size_t off = 0;
        off += snprintf(fields_json + off, sizeof(fields_json) - off, "[");
        const char *p = fs[2].value;
        int first = 1;
        while (*p) {
            while (*p == ' ' || *p == ',') p++;
            if (!*p) break;
            const char *s = p;
            while (*p && *p != ',') p++;
            size_t L = (size_t)(p - s);
            off += snprintf(fields_json + off, sizeof(fields_json) - off,
                "%s\"%.*s\"", first ? "" : ",", (int)L, s);
            first = 0;
        }
        snprintf(fields_json + off, sizeof(fields_json) - off, "]");
    }

    int offv = atoi(fs[0].value);
    int limv = atoi(fs[1].value);
    if (limv <= 0) limv = 50;

    /* "format":"rows" — flat tabular shape easy for tui_show_table. */
    char *req = malloc(strlen(crit) + 1024);
    sprintf(req,
        "{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\",\"criteria\":%s,"
        "\"offset\":%d,\"limit\":%d,\"fields\":%s}",
        oi.dir, oi.object, crit, offv, limv, fields_json);
    free(crit);

    char *resp = NULL; size_t rlen = 0;
    int rc = cli_query(c, req, &resp, &rlen);
    free(req);
    if (rc != 0 || !resp) { tui_alert("error", "find failed"); free(resp); return; }

    /* If response is an array of objects, show as table; otherwise raw. */
    if (resp[0] == '[') tui_show_table("find result", resp);
    else                show_response("find result", resp);
    free(resp);
}

static void menu_query(void) {
    CliConn *c = get_conn();
    if (!c) return;
    MenuItem items[] = {
        { "insert", "single insert by key with field values" },
        { "get",    "fetch a single record by key" },
        { "exists", "check whether a key is present" },
        { "find",   "criteria builder + offset/limit, table output" },
        { "count",  "criteria builder, returns matching count" },
    };
    int sel = tui_menu("Query", items, 5);
    if (sel < 0) return;
    switch (sel) {
        case 0: query_insert(c); break;
        case 1: query_get(c);    break;
        case 2: query_exists(c); break;
        case 3: query_find(c);   break;
        case 4: query_count(c);  break;
    }
}

/* ---- Stats menu ---- */

static void menu_stats(void) {
    CliConn *c = get_conn();
    if (!c) return;
    tui_stats_live(c, 5);
}

/* ---- Top-level ---- */

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    /* Allow --token X to override env-supplied token. */
    for (int i = 1; i < argc; i++) {
        if ((strcmp(argv[i], "--token") == 0 || strcmp(argv[i], "-t") == 0) && i + 1 < argc) {
            snprintf(g_cli_token, sizeof(g_cli_token), "%s", argv[++i]);
        } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            snprintf(g_cli_host, sizeof(g_cli_host), "%s", argv[++i]);
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            g_cli_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            fprintf(stderr,
                    "shard-cli — ncurses TUI for shard-db\n"
                    "Reads HOST/PORT/TLS_ENABLE/TLS_CA/TOKEN from env (source db.env).\n"
                    "Usage: shard-cli [--host H] [--port P] [--token T]\n");
            return 0;
        }
    }
    cli_load_env();

    tui_init();
    atexit(tui_shutdown);
    tui_status("connected to %s:%d  tls=%s",
               g_cli_host, g_cli_port, g_cli_tls_enable ? "on" : "off");

    for (;;) {
        MenuItem items[] = {
            { "Server",   "start / stop / status" },
            { "Browse",   "tenants → objects → describe" },
            { "Query",    "insert / get / find / count / exists" },
            { "Stats",    "live counters, refreshes every 5s" },
            { "Quit",     "exit shard-cli" },
        };
        int choice = tui_menu("main menu", items, 5);
        if (choice < 0 || choice == 4) break;
        switch (choice) {
            case 0: menu_server(); break;
            case 1: menu_browse(); break;
            case 2: menu_query();  break;
            case 3: menu_stats();  break;
        }
    }

    if (g_conn) cli_close(g_conn);
    return 0;
}
