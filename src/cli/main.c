/* shard-cli main entry — parses env (db.env should be sourced), opens a
   connection (lazy on first action), drives the top-level menu. */

#define _GNU_SOURCE
#include "cli.h"
#include <unistd.h>
#include <sys/wait.h>

/* Stub for criteria builder until the next commit wires up the form. */
int tui_criteria_builder(const ObjectInfo *oi, char **criteria_out) {
    (void)oi;
    *criteria_out = strdup("[]");
    tui_alert("not yet", "criteria builder lands in the next commit — empty criteria for now");
    return 0;
}

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
            { "Query",    "(coming next commit) insert / get / find / count / exists" },
            { "Stats",    "live counters, refreshes every 5s" },
            { "Quit",     "exit shard-cli" },
        };
        int choice = tui_menu("main menu", items, 5);
        if (choice < 0 || choice == 4) break;
        switch (choice) {
            case 0: menu_server(); break;
            case 1: menu_browse(); break;
            case 2: tui_alert("Query",
                              "insert / get / find / count / exists land in the next commit.\n"
                              "For now, use ./shard-db query '{...}' from the shell."); break;
            case 3: menu_stats(); break;
        }
    }

    if (g_conn) cli_close(g_conn);
    return 0;
}
