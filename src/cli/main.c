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
    /* tui_show_table is the unified renderer for every JSON response shape
       the daemon emits — array of objects, array of strings, columns+rows
       envelope, results-wrapped fetch, scalar object, etc. Same UX (header,
       horizontal scroll, j/k navigation) for find / fetch / keys / count /
       aggregate / exists / get. */
    tui_show_table(title, json);
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
    int sel = 0;
    for (;;) {
        MenuItem items[] = {
            { "start",   "Daemonize ./shard-db on PORT" },
            { "stop",    "SIGTERM the running daemon (drains in-flight writes)" },
            { "status",  "Print PID/port if running" },
        };
        int choice = tui_menu_at("Server", items, 3, &sel);
        if (choice < 0) return;
        /* Close our connection BEFORE start/stop. Stop especially needs this:
           the daemon's shutdown waits for worker threads to exit, and a
           worker blocked on fgets reading our idle TUI connection would
           wedge pthread_join. Daemon also closes our fd on SIGTERM (see
           handle_shutdown), so this is belt-and-suspenders. */
        if (g_conn) { cli_close(g_conn); g_conn = NULL; }
        const char *cmd = NULL;
        const char *stat = NULL;
        switch (choice) {
            case 0: cmd = "./shard-db start  2>&1"; stat = "starting daemon..."; break;
            case 1: cmd = "./shard-db stop   2>&1"; stat = "stopping daemon (drains writes)..."; break;
            case 2: cmd = "./shard-db status 2>&1"; stat = "checking status...";  break;
        }
        tui_status("%s", stat);
        /* Quick repaint so the status bar shows the in-progress message
           before run_capture blocks. */
        int rows, cols; getmaxyx(stdscr, rows, cols);
        attron(COLOR_PAIR(3));
        mvprintw(rows - 1, 0, " %.*s", cols - 1, stat);
        attroff(COLOR_PAIR(3));
        refresh();
        char *out = run_capture(cmd);
        tui_status("connected to %s:%d  tls=%s",
                   g_cli_host, g_cli_port, g_cli_tls_enable ? "on" : "off");
        tui_alert(items[choice].label, out ? out : "(no output)");
        free(out);
    }
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

/* Loop at the object-pick level: ←/q goes back to tenant pick; describe view
   closes back to the object pick. Each level loops until user backs out. */
static void menu_browse_object_list(CliConn *c, const char *dir) {
    for (;;) {
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
        const char **view = malloc(n * sizeof(*view));
        for (int i = 0; i < n; i++) view[i] = objs[i];
        const char *picked = tui_pick(title, view, n);
        free(view);
        char obj_copy[64] = "";
        if (picked) snprintf(obj_copy, sizeof(obj_copy), "%s", picked);
        for (int i = 0; i < n; i++) free(objs[i]);
        free(objs);
        if (!obj_copy[0]) return;  /* user backed out → return to tenant pick */
        browse_object(c, dir, obj_copy);
        /* describe view dismissed → loop back to object pick */
    }
}

static void menu_browse(void) {
    CliConn *c = get_conn();
    if (!c) return;
    /* Tenant-pick loop: ← in tenant pick returns to the main menu. */
    for (;;) {
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
        char dir_copy[64] = "";
        if (picked) snprintf(dir_copy, sizeof(dir_copy), "%s", picked);
        for (int i = 0; i < n; i++) free(dirs[i]);
        free(dirs);
        if (!dir_copy[0]) return;  /* ← in tenant pick → back to main menu */
        menu_browse_object_list(c, dir_copy);
    }
}

/* ---- Query menu ---- */

/* Pick a tenant + object via two list pickers; populate ObjectInfo.
   Loops at each level so ←/q in object pick re-shows the tenant pick;
   ←/q in tenant pick returns -1 (caller drops back to its menu). */
static int pick_object(CliConn *c, ObjectInfo *oi) {
    for (;;) {
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
        char dir_copy[64] = "";
        if (dir) snprintf(dir_copy, sizeof(dir_copy), "%s", dir);
        for (int i = 0; i < ndirs; i++) free(dirs[i]);
        free(dirs);
        if (!dir_copy[0]) return -1;  /* ← in tenant pick → back to caller's menu */

        /* Object pick loop: ← here re-shows tenant pick. */
        for (;;) {
            char req[256];
            snprintf(req, sizeof(req), "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dir_copy);
            if (cli_query(c, req, &resp, &rlen) != 0) {
                tui_alert("error", "list-objects failed");
                return -1;
            }
            char **objs = NULL; int nobjs = 0;
            collect_string_array(resp, "objects", &objs, &nobjs);
            free(resp);
            if (nobjs == 0) { tui_alert(dir_copy, "(no objects)"); break; }

            const char **vo = malloc(nobjs * sizeof(*vo));
            for (int i = 0; i < nobjs; i++) vo[i] = objs[i];
            const char *obj = tui_pick("pick object", vo, nobjs);
            free(vo);
            char obj_copy[64] = "";
            if (obj) snprintf(obj_copy, sizeof(obj_copy), "%s", obj);
            for (int i = 0; i < nobjs; i++) free(objs[i]);
            free(objs);
            if (!obj_copy[0]) break;  /* ← in object pick → re-show tenant pick */

            if (describe_object(c, dir_copy, obj_copy, oi) != 0) {
                tui_alert("error", "describe-object failed");
                continue;  /* let user pick a different object */
            }
            return 0;
        }
    }
}

static void query_insert(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    /* Form: key + one row per writable field. Sticky across re-runs so the
       user can change just the key (or one field) and submit again. */
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

    for (;;) {
        if (tui_form(title, fs, nf) != 0) { free(fs); return; }
        if (!fs[0].value[0]) { tui_alert("insert", "key required"); continue; }

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

        char *resp = NULL; size_t rlen = 0;
        if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "insert failed");
        else { show_response("insert result", resp); free(resp); }
        free(req);
        /* result dismissed → loop back to form with sticky values */
    }
}

/* Single-key form for get/exists. Loops on form ↔ result so ← in result
   re-shows the form with the previous key pre-filled. ← in form exits. */
static void query_single_key(CliConn *c, const char *mode, const char *result_title) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    FormField fs[1] = {0};
    fs[0].label = "key";
    fs[0].kind  = FF_TEXT;
    char title[128];
    snprintf(title, sizeof(title), "%s in %s/%s", mode, oi.dir, oi.object);

    for (;;) {
        if (tui_form(title, fs, 1) != 0) return;  /* ← in form → exit op */
        if (!fs[0].value[0]) { tui_alert(mode, "key required"); continue; }

        char req[1024];
        snprintf(req, sizeof(req),
            "{\"mode\":\"%s\",\"dir\":\"%s\",\"object\":\"%s\",\"key\":\"%s\"}",
            mode, oi.dir, oi.object, fs[0].value);
        char *resp = NULL; size_t rlen = 0;
        if (cli_query(c, req, &resp, &rlen) != 0) { tui_alert("error", mode); continue; }
        show_response(result_title, resp);
        free(resp);
        /* result dismissed → loop back to form with sticky value */
    }
}

static void query_get(CliConn *c)    { query_single_key(c, "get",    "get result"); }
static void query_exists(CliConn *c) { query_single_key(c, "exists", "exists");     }

/* Multi-key wrapper for get/exists. Both daemon modes accept a `keys` array
   and return shapes the unified renderer handles. */
static void query_keys_op(CliConn *c, const char *mode, const char *result_title) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    /* Sticky list across re-runs — comes back from the result with the
       same keys still entered. */
    char keys[64][LIST_ITEM_MAX];
    memset(keys, 0, sizeof(keys));
    int n_keys = 0;

    char title[128];
    snprintf(title, sizeof(title), "%s many in %s/%s — TAB adds, ⏎ submits",
             mode, oi.dir, oi.object);

    for (;;) {
        if (tui_list_input(title, "key", keys, 64, &n_keys) != 0) return;

        /* Build keys JSON array from the accumulated list. */
        char keys_json[8192];
        size_t off = 0;
        off += snprintf(keys_json + off, sizeof(keys_json) - off, "[");
        for (int i = 0; i < n_keys; i++) {
            /* Basic JSON-string escape for embedded quotes/backslashes. */
            const char *s = keys[i];
            off += snprintf(keys_json + off, sizeof(keys_json) - off,
                "%s\"", i ? "," : "");
            for (; *s && off + 6 < sizeof(keys_json); s++) {
                if (*s == '"' || *s == '\\') {
                    keys_json[off++] = '\\';
                    keys_json[off++] = *s;
                } else {
                    keys_json[off++] = *s;
                }
            }
            keys_json[off++] = '"';
        }
        keys_json[off++] = ']';
        keys_json[off]   = '\0';

        char req[16384];
        snprintf(req, sizeof(req),
            "{\"mode\":\"%s\",\"dir\":\"%s\",\"object\":\"%s\",\"keys\":%s}",
            mode, oi.dir, oi.object, keys_json);

        char preview_title[128];
        snprintf(preview_title, sizeof(preview_title),
                 "%s many — query JSON (r=run  ←=back to edit)", mode);
        int act = tui_preview_json(preview_title, req);
        if (act != 1) continue;

        char *resp = NULL; size_t rlen = 0;
        if (cli_query(c, req, &resp, &rlen) != 0) {
            tui_alert("error", mode); continue;
        }
        tui_show_table(result_title, resp);
        free(resp);
    }
}

static void query_get_many(CliConn *c)    { query_keys_op(c, "get",    "get many");    }
static void query_exists_many(CliConn *c) { query_keys_op(c, "exists", "exists many"); }

static void query_delete(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    FormField fs[1] = {0};
    fs[0].label = "key"; fs[0].kind = FF_TEXT;
    char title[128];
    snprintf(title, sizeof(title), "delete from %s/%s", oi.dir, oi.object);
    for (;;) {
        if (tui_form(title, fs, 1) != 0) return;
        if (!fs[0].value[0]) { tui_alert("delete", "key required"); continue; }
        char prompt[256];
        snprintf(prompt, sizeof(prompt), "delete '%s' from %s/%s?",
                 fs[0].value, oi.dir, oi.object);
        if (!tui_confirm(prompt)) continue;
        char req[1024];
        snprintf(req, sizeof(req),
            "{\"mode\":\"delete\",\"dir\":\"%s\",\"object\":\"%s\",\"key\":\"%s\"}",
            oi.dir, oi.object, fs[0].value);
        char *resp = NULL; size_t rlen = 0;
        if (cli_query(c, req, &resp, &rlen) != 0) { tui_alert("error", "delete failed"); continue; }
        show_response("delete result", resp);
        free(resp);
    }
}

/* Update: form with key + one row per field. Empty fields are skipped on
   submit (server keeps the existing value), so the user only fills in what
   they want to change. */
static void query_update(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    int nf = oi.nfields + 1;
    FormField *fs = calloc(nf, sizeof(*fs));
    fs[0].label = "key"; fs[0].kind = FF_TEXT;
    for (int i = 0; i < oi.nfields; i++) {
        fs[1 + i].label = oi.fields[i].name;
        fs[1 + i].kind  = FF_TEXT;
    }
    char title[128];
    snprintf(title, sizeof(title), "update %s/%s", oi.dir, oi.object);

    for (;;) {
        if (tui_form(title, fs, nf) != 0) { free(fs); return; }
        if (!fs[0].value[0]) { tui_alert("update", "key required"); continue; }

        size_t cap = 4096; char *req = malloc(cap); size_t off = 0;
        off += snprintf(req + off, cap - off,
            "{\"mode\":\"update\",\"dir\":\"%s\",\"object\":\"%s\",\"key\":\"%s\",\"value\":{",
            oi.dir, oi.object, fs[0].value);
        int emitted = 0;
        for (int i = 0; i < oi.nfields; i++) {
            if (!fs[1 + i].value[0]) continue;
            if (off + 256 + strlen(fs[1 + i].value) >= cap) { cap *= 2; req = realloc(req, cap); }
            off += snprintf(req + off, cap - off,
                "%s\"%s\":\"%s\"",
                emitted ? "," : "", oi.fields[i].name, fs[1 + i].value);
            emitted++;
        }
        off += snprintf(req + off, cap - off, "}}");

        char *resp = NULL; size_t rlen = 0;
        if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "update failed");
        else { show_response("update result", resp); free(resp); }
        free(req);
    }
}

/* Keys: paginated key listing (no values). Server always returns
   ["k1","k2",...]; tui_show_table renders as a single-column table. */
static void query_keys(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    FormField fs[2] = {0};
    fs[0].label = "offset"; fs[0].kind = FF_NUMBER; snprintf(fs[0].value, sizeof(fs[0].value), "0");
    fs[1].label = "limit";  fs[1].kind = FF_NUMBER; snprintf(fs[1].value, sizeof(fs[1].value), "100");
    for (;;) {
        if (tui_form("keys — paging", fs, 2) != 0) return;
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"keys\",\"dir\":\"%s\",\"object\":\"%s\","
            "\"offset\":%d,\"limit\":%d}",
            oi.dir, oi.object, atoi(fs[0].value), atoi(fs[1].value));
        int act = tui_preview_json("keys — query JSON (r=run  ←=back to edit)", req);
        if (act != 1) continue;
        char *resp = NULL; size_t rlen = 0;
        if (cli_query(c, req, &resp, &rlen) != 0) { tui_alert("error", "keys failed"); continue; }
        tui_show_table("keys", resp);
        free(resp);
    }
}

/* Fetch: paginated record listing without criteria. Same UX as find but
   without the criteria builder (and the server walks insertion order). */
static void query_fetch(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    FormField fs[2] = {0};
    fs[0].label = "offset"; fs[0].kind = FF_NUMBER; snprintf(fs[0].value, sizeof(fs[0].value), "0");
    fs[1].label = "limit";  fs[1].kind = FF_NUMBER; snprintf(fs[1].value, sizeof(fs[1].value), "50");

    int *fld_sel = calloc(oi.nfields, sizeof(int));
    if (fld_sel) for (int i = 0; i < oi.nfields; i++) fld_sel[i] = 1;
    const char *fld_choices[MAX_FIELDS_CACHED + 1];
    for (int i = 0; i < oi.nfields; i++) fld_choices[i] = oi.fields[i].name;
    fld_choices[oi.nfields] = NULL;

    for (;;) {
        if (tui_form("fetch — paging", fs, 2) != 0) { free(fld_sel); return; }
        if (tui_multi_pick("fields to project (space toggle, ⏎ confirm)",
                           fld_choices, oi.nfields, fld_sel) != 0) continue;

        char fields_json[1024] = "[]";
        int picked = 0;
        for (int i = 0; i < oi.nfields; i++) if (fld_sel[i]) picked++;
        if (picked > 0 && picked < oi.nfields) {
            size_t off = 0;
            off += snprintf(fields_json + off, sizeof(fields_json) - off, "[");
            int first = 1;
            for (int i = 0; i < oi.nfields; i++) {
                if (!fld_sel[i]) continue;
                off += snprintf(fields_json + off, sizeof(fields_json) - off,
                    "%s\"%s\"", first ? "" : ",", oi.fields[i].name);
                first = 0;
            }
            snprintf(fields_json + off, sizeof(fields_json) - off, "]");
        }

        char req[2048];
        snprintf(req, sizeof(req),
            "{\"mode\":\"fetch\",\"dir\":\"%s\",\"object\":\"%s\","
            "\"offset\":%d,\"limit\":%d,\"fields\":%s}",
            oi.dir, oi.object, atoi(fs[0].value), atoi(fs[1].value), fields_json);
        int act = tui_preview_json("fetch — query JSON (r=run  ←=back to edit)", req);
        if (act != 1) continue;
        char *resp = NULL; size_t rlen = 0;
        if (cli_query(c, req, &resp, &rlen) != 0) { tui_alert("error", "fetch failed"); continue; }
        tui_show_table("fetch result", resp);
        free(resp);
    }
}

/* Aggregate spec — one row per (fn, field, alias). count and exists don't
   need a field; sum/avg/min/max do. List-with-actions UX same as criteria. */
typedef struct {
    char fn[16];
    char field[64];
    char alias[64];
} AggRow;

#define MAX_AGG_ROWS 8

static const char *const AGG_FNS[] = { "count", "sum", "avg", "min", "max", NULL };

static int prompt_one_agg(const ObjectInfo *oi, AggRow *out) {
    const char *field_choices[MAX_FIELDS_CACHED + 2];
    field_choices[0] = "(none)";
    for (int i = 0; i < oi->nfields; i++) field_choices[1 + i] = oi->fields[i].name;
    field_choices[1 + oi->nfields] = NULL;

    FormField fs[3] = {0};
    fs[0].label = "function"; fs[0].kind = FF_CHOICE; fs[0].choices = AGG_FNS;
    snprintf(fs[0].value, sizeof(fs[0].value), "count");
    fs[1].label = "field";    fs[1].kind = FF_CHOICE; fs[1].choices = field_choices;
    snprintf(fs[1].value, sizeof(fs[1].value), "(none)");
    fs[2].label = "alias";    fs[2].kind = FF_TEXT;
    snprintf(fs[2].value, sizeof(fs[2].value), "n");

    if (tui_form("add aggregate (sum/avg/min/max need field; count is optional)", fs, 3) != 0)
        return -1;
    snprintf(out->fn,    sizeof(out->fn),    "%s", fs[0].value);
    snprintf(out->field, sizeof(out->field), "%s", fs[1].value);
    snprintf(out->alias, sizeof(out->alias), "%s", fs[2].value[0] ? fs[2].value : "n");
    return 0;
}

static char *pack_aggs(const AggRow *rows, int n) {
    if (n <= 0) return strdup("[]");
    size_t cap = 1024;
    char *out = malloc(cap);
    size_t off = 0;
    off += snprintf(out + off, cap - off, "[");
    for (int i = 0; i < n; i++) {
        if (off + 256 >= cap) { cap *= 2; out = realloc(out, cap); }
        if (strcmp(rows[i].field, "(none)") == 0 || rows[i].field[0] == '\0') {
            off += snprintf(out + off, cap - off,
                "%s{\"fn\":\"%s\",\"alias\":\"%s\"}",
                i ? "," : "", rows[i].fn, rows[i].alias);
        } else {
            off += snprintf(out + off, cap - off,
                "%s{\"fn\":\"%s\",\"field\":\"%s\",\"alias\":\"%s\"}",
                i ? "," : "", rows[i].fn, rows[i].field, rows[i].alias);
        }
    }
    off += snprintf(out + off, cap - off, "]");
    return out;
}

/* Sticky variant — caller owns rows[]/n_io so the spec list survives
   across re-runs (same pattern as tui_criteria_builder). */
static int build_agg_specs(const ObjectInfo *oi,
                           AggRow *rows, int *n_io,
                           char **specs_out) {
    int sel = 0;
    for (;;) {
        int n = *n_io;
        MenuItem items[MAX_AGG_ROWS + 2];
        char labels[MAX_AGG_ROWS + 2][128];
        int total = 0;
        for (int i = 0; i < n; i++) {
            const char *f = (rows[i].field[0] && strcmp(rows[i].field, "(none)") != 0)
                            ? rows[i].field : "*";
            snprintf(labels[total], sizeof(labels[0]),
                     "%-6s %-20s as %s", rows[i].fn, f, rows[i].alias);
            items[total].label = labels[total];
            items[total].hint  = "⏎/→ to remove";
            total++;
        }
        snprintf(labels[total], sizeof(labels[0]), "[+] add aggregate");
        items[total].label = labels[total];
        items[total].hint  = "open form for the next fn/field/alias";
        int idx_add = total;
        total++;
        snprintf(labels[total], sizeof(labels[0]), "[✓] submit (%d aggregate%s)",
                 n, n == 1 ? "" : "s");
        items[total].label = labels[total];
        items[total].hint  = "build the JSON aggregates array and continue";
        int idx_submit = total;
        total++;

        int choice = tui_menu_at("aggregate spec builder", items, total, &sel);
        if (choice < 0) { *specs_out = NULL; return -1; }
        if (choice == idx_submit) { *specs_out = pack_aggs(rows, n); return 0; }
        if (choice == idx_add) {
            if (n >= MAX_AGG_ROWS) { tui_alert("aggregate", "max specs reached"); continue; }
            AggRow row;
            if (prompt_one_agg(oi, &row) == 0) {
                rows[n++] = row;
                *n_io = n;
                sel = idx_add;
            }
            continue;
        }
        if (tui_confirm("remove this aggregate?")) {
            for (int i = choice; i < n - 1; i++) rows[i] = rows[i + 1];
            n--;
            *n_io = n;
            if (sel >= n) sel = n;
        }
    }
}

/* Extract aggregate aliases from a packed specs JSON, e.g.
   [{"fn":"sum","field":"amount","alias":"total"},...] → ["total","..."].
   Used to populate the order_by dropdown so the user picks an alias by name
   instead of typing it. Returns count; out is filled with NULL-term'd
   pointers into the heap (caller frees each + the array). */
static int extract_agg_aliases(const char *specs, char ***out) {
    int cap = 8, n = 0;
    char **arr = malloc(cap * sizeof(char *));
    const char *p = specs;
    while (*p) {
        const char *q = strstr(p, "\"alias\"");
        if (!q) break;
        q = strchr(q, ':');
        if (!q) break;
        q++;
        while (*q == ' ' || *q == '\t') q++;
        if (*q != '"') { p = q; continue; }
        q++;
        const char *s = q;
        while (*q && *q != '"') q++;
        size_t L = (size_t)(q - s);
        if (n >= cap) { cap *= 2; arr = realloc(arr, cap * sizeof(char *)); }
        arr[n] = malloc(L + 1);
        memcpy(arr[n], s, L);
        arr[n][L] = '\0';
        n++;
        p = q + 1;
    }
    *out = arr;
    return n;
}

static void query_aggregate(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    /* Sticky group_by selection across re-runs. */
    int *grp_sel = calloc(oi.nfields, sizeof(int));
    const char *fld_choices[MAX_FIELDS_CACHED + 1];
    for (int i = 0; i < oi.nfields; i++) fld_choices[i] = oi.fields[i].name;
    fld_choices[oi.nfields] = NULL;

    /* Sticky criteria + agg-spec rows across re-runs. */
    CritRow crit_rows[MAX_CRIT_ROWS];
    int crit_n = 0;
    AggRow agg_rows[MAX_AGG_ROWS];
    int agg_n = 0;

    /* Sticky form fields for limit / order_by / order — kept outside all
       loops so the user's choices persist when they ← from the result. */
    static const char *const ord_dir[] = { "asc", "desc", NULL };
    FormField fs[3] = {0};
    fs[0].label = "limit";    fs[0].kind = FF_NUMBER;
    snprintf(fs[0].value, sizeof(fs[0].value), "100");
    fs[1].label = "order_by"; fs[1].kind = FF_CHOICE;
    snprintf(fs[1].value, sizeof(fs[1].value), "(none)");
    fs[2].label = "order";    fs[2].kind = FF_CHOICE;
    fs[2].choices = ord_dir;
    snprintf(fs[2].value, sizeof(fs[2].value), "desc");

    /* Outer loop: criteria. ← in criteria → exit op. */
    for (;;) {
        char *crit = NULL;
        if (tui_criteria_builder(&oi, crit_rows, &crit_n, &crit) != 0) { free(grp_sel); return; }

        /* Spec loop: ← in specs → re-show criteria. */
        for (;;) {
            char *specs = NULL;
            if (build_agg_specs(&oi, agg_rows, &agg_n, &specs) != 0) { free(crit); crit = NULL; break; }

            /* Aliases drive order_by dropdown — rebuilt each time the spec
               list changes. */
            char **aliases;
            int n_aliases = extract_agg_aliases(specs, &aliases);
            const char **ord_choices = malloc((n_aliases + 2) * sizeof(*ord_choices));
            ord_choices[0] = "(none)";
            for (int i = 0; i < n_aliases; i++) ord_choices[1 + i] = aliases[i];
            ord_choices[1 + n_aliases] = NULL;
            fs[1].choices = ord_choices;

            /* If the previously-chosen order_by alias no longer exists in
               the rebuilt list, reset it to (none). */
            int found = 0;
            for (int i = 0; ord_choices[i]; i++)
                if (strcmp(ord_choices[i], fs[1].value) == 0) { found = 1; break; }
            if (!found) snprintf(fs[1].value, sizeof(fs[1].value), "(none)");

            /* group_by + form loop: ← in form → re-show group_by;
               ← in group_by → re-show specs. */
            for (;;) {
                if (tui_multi_pick(
                        "group_by — fields to group on (skip → no grouping)",
                        fld_choices, oi.nfields, grp_sel) != 0) break;

                /* Form ↔ result inner loop. */
                for (;;) {
                    if (tui_form("aggregate — limit / order_by / order", fs, 3) != 0)
                        goto next_group;  /* ← in form → re-show group_by */

                    /* Build group_by JSON array. */
                    char grp_json[512] = "[]";
                    int grp_picked = 0;
                    for (int i = 0; i < oi.nfields; i++) if (grp_sel[i]) grp_picked++;
                    if (grp_picked > 0) {
                        size_t off = 0;
                        off += snprintf(grp_json + off, sizeof(grp_json) - off, "[");
                        int first = 1;
                        for (int i = 0; i < oi.nfields; i++) {
                            if (!grp_sel[i]) continue;
                            off += snprintf(grp_json + off, sizeof(grp_json) - off,
                                "%s\"%s\"", first ? "" : ",", oi.fields[i].name);
                            first = 0;
                        }
                        snprintf(grp_json + off, sizeof(grp_json) - off, "]");
                    }

                    int limv = atoi(fs[0].value);
                    if (limv < 0) limv = 0;

                    /* order_by is optional. (none) → omit the field. */
                    char ob_clause[128] = "";
                    if (strcmp(fs[1].value, "(none)") != 0 && fs[1].value[0])
                        snprintf(ob_clause, sizeof(ob_clause),
                                 ",\"order_by\":\"%s\"", fs[1].value);

                    char *req = malloc(strlen(crit) + strlen(specs) + 1024);
                    sprintf(req,
                        "{\"mode\":\"aggregate\",\"dir\":\"%s\",\"object\":\"%s\","
                        "\"criteria\":%s,\"aggregates\":%s,\"group_by\":%s%s,"
                        "\"order\":\"%s\",\"limit\":%d}",
                        oi.dir, oi.object, crit, specs, grp_json, ob_clause,
                        fs[2].value, limv);

                    int act = tui_preview_json(
                        "aggregate — query JSON (r=run  ←=back to edit)", req);
                    if (act != 1) { free(req); continue; }

                    char *resp = NULL; size_t rlen = 0;
                    int rc = cli_query(c, req, &resp, &rlen);
                    free(req);
                    if (rc != 0 || !resp) {
                        tui_alert("error", "aggregate failed");
                        free(resp); continue;
                    }
                    if (resp[0] == '[' || (resp[0] == '{' && strstr(resp, "rows")))
                        tui_show_table("aggregate result", resp);
                    else
                        show_response("aggregate result", resp);
                    free(resp);
                    /* result dismissed → loop back to form */
                }
            next_group:
                /* group_by/form layer exited — break to re-show spec builder. */
                break;
            }
            for (int i = 0; i < n_aliases; i++) free(aliases[i]);
            free(aliases);
            free(ord_choices);
            free(specs);
        }
        if (crit) free(crit);
    }
}

static void query_count(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    /* Caller-owned criteria state — sticky across runs so ← from result
       returns to a builder still showing the rows you typed. */
    CritRow crit_rows[MAX_CRIT_ROWS];
    int crit_n = 0;

    /* Loop: criteria builder → preview → result. ← in builder exits the op,
       ← in preview returns to criteria, ← in result returns to criteria. */
    for (;;) {
        char *crit = NULL;
        if (tui_criteria_builder(&oi, crit_rows, &crit_n, &crit) != 0) return;

        char *req = malloc(strlen(crit) + 256);
        sprintf(req,
            "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\",\"criteria\":%s}",
            oi.dir, oi.object, crit);
        free(crit);

        int act = tui_preview_json("count — query JSON (r=run  ←=back to edit)", req);
        if (act != 1) { free(req); continue; }

        char *resp = NULL; size_t rlen = 0;
        int rc = cli_query(c, req, &resp, &rlen);
        free(req);
        if (rc != 0) { tui_alert("error", "count failed"); continue; }
        show_response("count result", resp);
        free(resp);
    }
}

static void query_find(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;

    /* Sticky paging form values across re-runs. */
    FormField fs[2] = {0};
    fs[0].label = "offset"; fs[0].kind = FF_NUMBER; snprintf(fs[0].value, sizeof(fs[0].value), "0");
    fs[1].label = "limit";  fs[1].kind = FF_NUMBER; snprintf(fs[1].value, sizeof(fs[1].value), "50");

    /* Sticky field selection — start with all fields selected. */
    int *fld_sel = calloc(oi.nfields, sizeof(int));
    if (fld_sel) for (int i = 0; i < oi.nfields; i++) fld_sel[i] = 1;
    const char *fld_choices[MAX_FIELDS_CACHED + 1];
    for (int i = 0; i < oi.nfields; i++) fld_choices[i] = oi.fields[i].name;
    fld_choices[oi.nfields] = NULL;

    /* Sticky criteria — survive across re-runs of the find loop. */
    CritRow crit_rows[MAX_CRIT_ROWS];
    int crit_n = 0;

    /* Outer loop: criteria → form → result → form → result …
       ← in form → re-show criteria. ← in criteria → exit op. */
    for (;;) {
        char *crit = NULL;
        if (tui_criteria_builder(&oi, crit_rows, &crit_n, &crit) != 0) { free(fld_sel); return; }

        for (;;) {
            if (tui_form("find — paging", fs, 2) != 0) {
                /* ← in paging form → back to criteria builder */
                free(crit);
                break;
            }

            /* Field picker: multi-select from oi.fields. ← here → back to form. */
            if (tui_multi_pick("fields to project (space toggle, ⏎ confirm, ← back)",
                               fld_choices, oi.nfields, fld_sel) != 0) {
                /* user cancelled field pick → re-show form */
                continue;
            }

            /* Build fields JSON array. Empty selection → empty array (= all
               fields in the response). */
            char fields_json[1024] = "[]";
            int picked = 0;
            for (int i = 0; i < oi.nfields; i++) if (fld_sel[i]) picked++;
            if (picked > 0 && picked < oi.nfields) {
                size_t off = 0;
                off += snprintf(fields_json + off, sizeof(fields_json) - off, "[");
                int first = 1;
                for (int i = 0; i < oi.nfields; i++) {
                    if (!fld_sel[i]) continue;
                    off += snprintf(fields_json + off, sizeof(fields_json) - off,
                        "%s\"%s\"", first ? "" : ",", oi.fields[i].name);
                    first = 0;
                }
                snprintf(fields_json + off, sizeof(fields_json) - off, "]");
            }

            int offv = atoi(fs[0].value);
            int limv = atoi(fs[1].value);
            if (limv <= 0) limv = 50;

            /* No format hint — tui_show_table handles every shape the
               daemon emits today (default array-of-objects, format=rows
               envelope, scalars, plain objects). One render path for all
               data-returning queries. */
            char *req = malloc(strlen(crit) + 1024);
            sprintf(req,
                "{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\",\"criteria\":%s,"
                "\"offset\":%d,\"limit\":%d,\"fields\":%s}",
                oi.dir, oi.object, crit, offv, limv, fields_json);

            int act = tui_preview_json("find — query JSON (r=run  ←=back to edit)", req);
            if (act != 1) { free(req); continue; }

            char *resp = NULL; size_t rlen = 0;
            int rc = cli_query(c, req, &resp, &rlen);
            free(req);
            if (rc != 0 || !resp) { tui_alert("error", "find failed"); free(resp); continue; }
            tui_show_table("find result", resp);
            free(resp);
            /* result dismissed → loop back to paging form */
        }
        /* form loop exited → re-show criteria builder */
    }
}

static void menu_query(void) {
    CliConn *c = get_conn();
    if (!c) return;
    int sel = 0;
    for (;;) {
        MenuItem items[] = {
            { "insert",       "single insert by key with field values" },
            { "get",          "fetch a single record by key" },
            { "get many",     "fetch multiple records by comma-separated keys" },
            { "update",       "update fields on an existing record" },
            { "delete",       "delete a single record by key (confirms)" },
            { "exists",       "check whether a key is present" },
            { "exists many",  "check presence of multiple keys at once" },
            { "find",         "criteria builder + offset/limit, table output" },
            { "count",        "criteria builder, returns matching count" },
            { "aggregate",    "sum/avg/min/max/count + group_by + criteria" },
            { "keys",         "paginated key listing (no values)" },
            { "fetch",        "paginated record listing (no criteria)" },
        };
        int choice = tui_menu_at("Query", items, 12, &sel);
        if (choice < 0) return;
        switch (choice) {
            case 0:  query_insert(c);       break;
            case 1:  query_get(c);          break;
            case 2:  query_get_many(c);     break;
            case 3:  query_update(c);       break;
            case 4:  query_delete(c);       break;
            case 5:  query_exists(c);       break;
            case 6:  query_exists_many(c);  break;
            case 7:  query_find(c);         break;
            case 8:  query_count(c);        break;
            case 9:  query_aggregate(c);    break;
            case 10: query_keys(c);         break;
            case 11: query_fetch(c);        break;
        }
    }
}

/* ---- Schema menu ---- */

/* Pick a tenant alone (no object). Returns malloc'd dir or NULL when user
   backs out (←/q at the picker). Single-level pick — caller handles its own
   loop / fallback if it wants to repeat. */
static char *pick_tenant(CliConn *c) {
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"db-dirs\"}", &resp, &rlen) != 0) return NULL;
    char **dirs = NULL; int n = 0;
    collect_string_array(resp, NULL, &dirs, &n);
    free(resp);
    if (n == 0) { tui_alert("(none)", "no tenants in dirs.conf"); return NULL; }
    const char **vd = malloc(n * sizeof(*vd));
    for (int i = 0; i < n; i++) vd[i] = dirs[i];
    const char *picked = tui_pick("pick tenant", vd, n);
    free(vd);
    char *out = picked ? strdup(picked) : NULL;
    for (int i = 0; i < n; i++) free(dirs[i]);
    free(dirs);
    return out;
}

static void schema_create_object(CliConn *c) {
    char *dir = pick_tenant(c);
    if (!dir) return;

    FormField fs[5] = {0};
    fs[0].label = "object";  fs[0].kind = FF_TEXT;
    fs[1].label = "splits";  fs[1].kind = FF_NUMBER; snprintf(fs[1].value, sizeof(fs[1].value), "16");
    fs[2].label = "max_key"; fs[2].kind = FF_NUMBER; snprintf(fs[2].value, sizeof(fs[2].value), "64");
    fs[3].label = "fields";  fs[3].kind = FF_TEXT;   /* comma-separated, e.g. "name:varchar:64,age:int" */
    fs[4].label = "indexes"; fs[4].kind = FF_TEXT;   /* comma-separated */
    if (tui_form("create-object", fs, 5) != 0) { free(dir); return; }
    if (!fs[0].value[0] || !fs[3].value[0]) {
        tui_alert("create-object", "object name and fields are required");
        free(dir); return;
    }

    /* Build fields[] and indexes[] arrays from CSV inputs. */
    char fields_json[4096] = "[", indexes_json[1024] = "[";
    {
        size_t off = 1;
        const char *p = fs[3].value; int first = 1;
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
    {
        size_t off = 1;
        const char *p = fs[4].value; int first = 1;
        while (*p) {
            while (*p == ' ' || *p == ',') p++;
            if (!*p) break;
            const char *s = p;
            while (*p && *p != ',') p++;
            size_t L = (size_t)(p - s);
            off += snprintf(indexes_json + off, sizeof(indexes_json) - off,
                "%s\"%.*s\"", first ? "" : ",", (int)L, s);
            first = 0;
        }
        snprintf(indexes_json + off, sizeof(indexes_json) - off, "]");
    }

    char req[8192];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"splits\":%s,\"max_key\":%s,\"fields\":%s,\"indexes\":%s}",
        dir, fs[0].value, fs[1].value, fs[2].value, fields_json, indexes_json);
    free(dir);

    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "create-object failed");
    else { show_response("create-object", resp); free(resp); }
}

static void schema_drop_object(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    char prompt[128];
    snprintf(prompt, sizeof(prompt), "drop %s/%s — delete all data?", oi.dir, oi.object);
    if (!tui_confirm(prompt)) return;
    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"drop-object\",\"dir\":\"%s\",\"object\":\"%s\"}", oi.dir, oi.object);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "drop failed");
    else { show_response("drop-object", resp); free(resp); }
}

static void schema_add_field(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    FormField fs[1] = {0};
    fs[0].label = "fields"; fs[0].kind = FF_TEXT;  /* CSV, e.g. "city:varchar:32,age:int" */
    if (tui_form("add-field (CSV: name:type[:size])", fs, 1) != 0) return;
    if (!fs[0].value[0]) return;

    char fields_json[2048] = "[";
    size_t off = 1;
    const char *p = fs[0].value; int first = 1;
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

    char req[3072];
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-field\",\"dir\":\"%s\",\"object\":\"%s\",\"fields\":%s}",
        oi.dir, oi.object, fields_json);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "add-field failed");
    else { show_response("add-field", resp); free(resp); }
}

static const char *pick_field(const ObjectInfo *oi, const char *title) {
    if (oi->nfields == 0) { tui_alert(title, "(no fields)"); return NULL; }
    const char **vf = malloc(oi->nfields * sizeof(*vf));
    for (int i = 0; i < oi->nfields; i++) vf[i] = oi->fields[i].name;
    const char *picked = tui_pick(title, vf, oi->nfields);
    free(vf);
    return picked;  /* points into oi — valid until oi changes */
}

static void schema_remove_field(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    const char *fld = pick_field(&oi, "remove field");
    if (!fld) return;
    char prompt[128];
    snprintf(prompt, sizeof(prompt), "tombstone field '%s'?", fld);
    if (!tui_confirm(prompt)) return;

    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"remove-field\",\"dir\":\"%s\",\"object\":\"%s\",\"fields\":[\"%s\"]}",
        oi.dir, oi.object, fld);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "remove-field failed");
    else { show_response("remove-field", resp); free(resp); }
}

static void schema_rename_field(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    const char *old = pick_field(&oi, "rename — old name");
    if (!old) return;
    char old_copy[64]; snprintf(old_copy, sizeof(old_copy), "%s", old);

    FormField fs[1] = {0};
    fs[0].label = "new name"; fs[0].kind = FF_TEXT;
    if (tui_form("rename-field", fs, 1) != 0) return;
    if (!fs[0].value[0]) return;

    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"rename-field\",\"dir\":\"%s\",\"object\":\"%s\",\"old\":\"%s\",\"new\":\"%s\"}",
        oi.dir, oi.object, old_copy, fs[0].value);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "rename-field failed");
    else { show_response("rename-field", resp); free(resp); }
}

static void schema_add_index(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    FormField fs[1] = {0};
    fs[0].label = "field (or a+b)"; fs[0].kind = FF_TEXT;
    if (tui_form("add-index", fs, 1) != 0) return;
    if (!fs[0].value[0]) return;

    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-index\",\"dir\":\"%s\",\"object\":\"%s\",\"field\":\"%s\"}",
        oi.dir, oi.object, fs[0].value);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "add-index failed");
    else { show_response("add-index", resp); free(resp); }
}

static void schema_reindex(CliConn *c) {
    MenuItem items[] = {
        { "all",      "rebuild every index in every tenant (slow on big DBs)" },
        { "tenant",   "rebuild every index in one tenant" },
        { "object",   "rebuild indexes for a single object" },
    };
    int sel = tui_menu("reindex scope", items, 3);
    if (sel < 0) return;

    char req[1024];
    if (sel == 0) {
        if (!tui_confirm("reindex EVERYTHING — this can take minutes")) return;
        snprintf(req, sizeof(req), "{\"mode\":\"reindex\"}");
    } else if (sel == 1) {
        char *dir = pick_tenant(c);
        if (!dir) return;
        snprintf(req, sizeof(req), "{\"mode\":\"reindex\",\"dir\":\"%s\"}", dir);
        free(dir);
    } else {
        ObjectInfo oi;
        if (pick_object(c, &oi) != 0) return;
        snprintf(req, sizeof(req),
            "{\"mode\":\"reindex\",\"dir\":\"%s\",\"object\":\"%s\"}",
            oi.dir, oi.object);
    }
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "reindex failed");
    else { show_response("reindex", resp); free(resp); }
}

static void schema_remove_index(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    if (oi.nindexes == 0) { tui_alert("remove-index", "(no indexes)"); return; }
    const char **vi = malloc(oi.nindexes * sizeof(*vi));
    for (int i = 0; i < oi.nindexes; i++) vi[i] = oi.indexes[i];
    const char *picked = tui_pick("remove which index", vi, oi.nindexes);
    free(vi);
    if (!picked) return;
    char picked_copy[64]; snprintf(picked_copy, sizeof(picked_copy), "%s", picked);

    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"remove-index\",\"dir\":\"%s\",\"object\":\"%s\",\"field\":\"%s\"}",
        oi.dir, oi.object, picked_copy);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "remove-index failed");
    else { show_response("remove-index", resp); free(resp); }
}

static void menu_schema(void) {
    CliConn *c = get_conn();
    if (!c) return;
    /* Loop the schema menu so ← inside a sub-action returns here, not all
       the way to the main menu. ← in this menu returns to the main menu.
       sel persists across iterations so back-nav lands on the previously-
       used row, not at the top. */
    int sel = 0;
    for (;;) {
        MenuItem items[] = {
            { "create-object", "name + splits + max_key + fields/indexes CSV" },
            { "drop-object",   "delete an object and all its data (confirm prompt)" },
            { "add-field",     "extend schema by appending typed fields" },
            { "remove-field",  "tombstone a field (data reclaimed on vacuum)" },
            { "rename-field",  "metadata-only rename, preserves data" },
            { "add-index",     "build a new index (single or composite)" },
            { "remove-index",  "drop an existing index" },
            { "reindex",       "rebuild indexes (all / tenant / object)" },
        };
        int choice = tui_menu_at("Schema", items, 8, &sel);
        if (choice < 0) return;
        switch (choice) {
            case 0: schema_create_object(c); break;
            case 1: schema_drop_object(c);   break;
            case 2: schema_add_field(c);     break;
            case 3: schema_remove_field(c);  break;
            case 4: schema_rename_field(c);  break;
            case 5: schema_add_index(c);     break;
            case 6: schema_remove_index(c);  break;
            case 7: schema_reindex(c);       break;
        }
    }
}

/* ---- Maintenance menu ---- */

static void simple_op(CliConn *c, const char *mode, const char *title, int confirm) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    if (confirm) {
        char p[128];
        snprintf(p, sizeof(p), "%s %s/%s — proceed?", mode, oi.dir, oi.object);
        if (!tui_confirm(p)) return;
    }
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"%s\",\"dir\":\"%s\",\"object\":\"%s\"}",
        mode, oi.dir, oi.object);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", title);
    else { show_response(title, resp); free(resp); }
}

static void menu_maintenance(void) {
    CliConn *c = get_conn();
    if (!c) return;
    int sel = 0;
    for (;;) {
        MenuItem items[] = {
            { "vacuum",   "compact tombstones + (optional) reshard" },
            { "recount",  "rebuild record count from on-disk slots" },
            { "truncate", "delete all records (confirm)" },
            { "backup",   "snapshot copy of the object's data" },
        };
        int choice = tui_menu_at("Maintenance", items, 4, &sel);
        if (choice < 0) return;
        switch (choice) {
            case 0: simple_op(c, "vacuum",   "vacuum",   0); break;
            case 1: simple_op(c, "recount",  "recount",  0); break;
            case 2: simple_op(c, "truncate", "truncate", 1); break;
            case 3: simple_op(c, "backup",   "backup",   0); break;
        }
    }
}

/* ---- Auth menu ---- */

static void auth_list_tokens(CliConn *c) {
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"list-tokens\"}", &resp, &rlen) != 0) {
        tui_alert("error", "list-tokens failed"); return;
    }
    tui_show_table("tokens", resp);
    free(resp);
}

static void auth_list_ips(CliConn *c) {
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"list-ips\"}", &resp, &rlen) != 0) {
        tui_alert("error", "list-ips failed"); return;
    }
    show_response("allowed IPs", resp);
    free(resp);
}

static const char *const SCOPE_CHOICES[] = { "global", "tenant", "object", NULL };
static const char *const PERM_CHOICES[]  = { "r", "rw", "rwx", NULL };

/* Read 16 random bytes from /dev/urandom and emit as 32 hex chars. */
static int gen_random_token(char *out, size_t out_sz) {
    if (out_sz < 33) return -1;
    FILE *f = fopen("/dev/urandom", "rb");
    if (!f) return -1;
    unsigned char raw[16];
    size_t n = fread(raw, 1, sizeof(raw), f);
    fclose(f);
    if (n != sizeof(raw)) return -1;
    static const char *hex = "0123456789abcdef";
    for (int i = 0; i < 16; i++) {
        out[i*2]     = hex[raw[i] >> 4];
        out[i*2 + 1] = hex[raw[i] & 0x0f];
    }
    out[32] = '\0';
    return 0;
}

static void auth_add_token(CliConn *c) {
    /* Step 1: scope */
    FormField fs0[1] = {0};
    fs0[0].label = "scope"; fs0[0].kind = FF_CHOICE; fs0[0].choices = SCOPE_CHOICES;
    snprintf(fs0[0].value, sizeof(fs0[0].value), "global");
    if (tui_form("add-token: pick scope (←→ cycle)", fs0, 1) != 0) return;

    char chosen_dir[64] = "", chosen_obj[64] = "";

    /* Step 2: tenant + (optionally) object pickers when scope demands. */
    if (strcmp(fs0[0].value, "tenant") == 0) {
        char *d = pick_tenant(c);
        if (!d) return;
        snprintf(chosen_dir, sizeof(chosen_dir), "%s", d);
        free(d);
    } else if (strcmp(fs0[0].value, "object") == 0) {
        ObjectInfo oi;
        if (pick_object(c, &oi) != 0) return;
        snprintf(chosen_dir, sizeof(chosen_dir), "%s", oi.dir);
        snprintf(chosen_obj, sizeof(chosen_obj), "%s", oi.object);
    }

    /* Step 3: perm */
    FormField fs1[1] = {0};
    fs1[0].label = "perm"; fs1[0].kind = FF_CHOICE; fs1[0].choices = PERM_CHOICES;
    snprintf(fs1[0].value, sizeof(fs1[0].value), "rw");
    if (tui_form("add-token: pick perm (r=read, rw=write, rwx=admin)", fs1, 1) != 0) return;

    /* Step 4: auto-generate the token. */
    char tok[64];
    if (gen_random_token(tok, sizeof(tok)) != 0) {
        tui_alert("add-token", "could not read /dev/urandom"); return;
    }

    char req[2048];
    if (chosen_dir[0] && chosen_obj[0]) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"add-token\",\"token\":\"%s\",\"dir\":\"%s\",\"object\":\"%s\",\"perm\":\"%s\"}",
            tok, chosen_dir, chosen_obj, fs1[0].value);
    } else if (chosen_dir[0]) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"add-token\",\"token\":\"%s\",\"dir\":\"%s\",\"perm\":\"%s\"}",
            tok, chosen_dir, fs1[0].value);
    } else {
        snprintf(req, sizeof(req),
            "{\"mode\":\"add-token\",\"token\":\"%s\",\"perm\":\"%s\"}",
            tok, fs1[0].value);
    }
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) {
        tui_alert("error", "add-token failed"); return;
    }
    if (strstr(resp, "\"error\"")) {
        tui_alert("add-token", resp);
    } else {
        char body[1024];
        snprintf(body, sizeof(body),
            "Token created. SAVE THIS — it is shown only once:\n\n"
            "  %s\n\n"
            "scope: %s%s%s%s\n"
            "perm : %s\n",
            tok,
            chosen_dir[0] ? chosen_dir : "global",
            chosen_obj[0] ? "/" : "",
            chosen_obj[0] ? chosen_obj : "",
            chosen_dir[0] ? "" : "",
            fs1[0].value);
        tui_alert("add-token — copy this token", body);
    }
    free(resp);
}

/* Pick a token row from the list-tokens response. Returns the chosen
   fingerprint (caller frees) or NULL. */
static char *pick_token_fingerprint(CliConn *c) {
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"list-tokens\"}", &resp, &rlen) != 0) {
        tui_alert("error", "list-tokens failed"); return NULL;
    }
    /* Walk array, build one menu line per token: "fp   scope   perm". */
    int max = 256;
    char (*labels)[160] = malloc(max * sizeof(*labels));
    char (*fps)[64]     = malloc(max * sizeof(*fps));
    int n = 0;
    const char *p = resp;
    while (*p && *p != '[') p++;
    if (*p != '[') { free(labels); free(fps); free(resp); tui_alert("tokens", "(empty)"); return NULL; }
    p++;
    while (*p && n < max) {
        while (*p == ' ' || *p == ',' || *p == '\n' || *p == '\t') p++;
        if (*p == ']' || !*p) break;
        if (*p != '{') break;
        const char *start = p;
        int depth = 0;
        do {
            if (*p == '{') depth++;
            else if (*p == '}') depth--;
            if (*p) p++;
        } while (depth > 0 && *p);
        size_t L = (size_t)(p - start);
        /* Pull fp/scope/perm from this object slice. */
        char fp[64] = "", scope[64] = "", perm[8] = "";
        const char *q = start;
        const char *end = start + L;
        while (q < end) {
            if (*q != '"') { q++; continue; }
            q++;
            const char *ks = q;
            while (q < end && *q != '"') q++;
            size_t klen = (size_t)(q - ks);
            if (q < end) q++;
            while (q < end && (*q == ' ' || *q == ':' || *q == '\t')) q++;
            if (*q != '"') continue;
            q++;
            const char *vs = q;
            while (q < end && *q != '"') { if (*q == '\\' && q+1<end) q++; q++; }
            size_t vlen = (size_t)(q - vs);
            if (q < end) q++;
            if (klen == 5 && memcmp(ks, "token", 5) == 0)
                snprintf(fp, sizeof(fp), "%.*s", (int)vlen, vs);
            else if (klen == 5 && memcmp(ks, "scope", 5) == 0)
                snprintf(scope, sizeof(scope), "%.*s", (int)vlen, vs);
            else if (klen == 4 && memcmp(ks, "perm", 4) == 0)
                snprintf(perm, sizeof(perm), "%.*s", (int)vlen, vs);
        }
        snprintf(labels[n], sizeof(labels[0]), "%-15s  %-30s  %s", fp, scope, perm);
        snprintf(fps[n], sizeof(fps[0]), "%s", fp);
        n++;
    }
    free(resp);
    if (n == 0) {
        free(labels); free(fps);
        tui_alert("tokens", "(no tokens registered)");
        return NULL;
    }
    MenuItem *items = calloc(n, sizeof(*items));
    for (int i = 0; i < n; i++) items[i].label = labels[i];
    int idx = tui_menu("pick token to remove (fingerprint  scope  perm)", items, n);
    free(items);
    char *out = NULL;
    if (idx >= 0) out = strdup(fps[idx]);
    free(labels); free(fps);
    return out;
}

static void auth_remove_token(CliConn *c) {
    char *fp = pick_token_fingerprint(c);
    if (!fp) return;
    char prompt[128];
    snprintf(prompt, sizeof(prompt), "remove token '%s' ?", fp);
    if (!tui_confirm(prompt)) { free(fp); return; }
    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"remove-token\",\"fingerprint\":\"%s\"}", fp);
    free(fp);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "remove-token failed");
    else { show_response("remove-token", resp); free(resp); }
}

static void auth_add_ip(CliConn *c) {
    FormField fs[1] = {0};
    fs[0].label = "ip"; fs[0].kind = FF_TEXT;
    if (tui_form("add-ip (v4 or v6)", fs, 1) != 0) return;
    if (!fs[0].value[0]) return;
    char req[1024];
    snprintf(req, sizeof(req), "{\"mode\":\"add-ip\",\"ip\":\"%s\"}", fs[0].value);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "add-ip failed");
    else { show_response("add-ip", resp); free(resp); }
}

static void auth_remove_ip(CliConn *c) {
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"list-ips\"}", &resp, &rlen) != 0) {
        tui_alert("error", "list-ips failed"); return;
    }
    char **ips = NULL; int n = 0;
    collect_string_array(resp, NULL, &ips, &n);
    free(resp);
    if (n == 0) { tui_alert("remove-ip", "(no allowed IPs)"); return; }
    const char **vi = malloc(n * sizeof(*vi));
    for (int i = 0; i < n; i++) vi[i] = ips[i];
    const char *picked = tui_pick("remove which IP", vi, n);
    free(vi);
    char picked_copy[64] = "";
    if (picked) snprintf(picked_copy, sizeof(picked_copy), "%s", picked);
    for (int i = 0; i < n; i++) free(ips[i]);
    free(ips);
    if (!picked_copy[0]) return;
    if (!tui_confirm("remove this IP?")) return;
    char req[1024];
    snprintf(req, sizeof(req), "{\"mode\":\"remove-ip\",\"ip\":\"%s\"}", picked_copy);
    char *resp2 = NULL; size_t rlen2 = 0;
    if (cli_query(c, req, &resp2, &rlen2) != 0) tui_alert("error", "remove-ip failed");
    else { show_response("remove-ip", resp2); free(resp2); }
}

static void menu_auth(void) {
    CliConn *c = get_conn();
    if (!c) return;
    int sel = 0;
    for (;;) {
        MenuItem items[] = {
            { "list-tokens",  "fingerprint + scope + perm of every token" },
            { "add-token",    "global / tenant / object scope, r|rw|rwx perm" },
            { "remove-token", "paste full token to remove" },
            { "list-ips",     "trusted IPs (bypass token check)" },
            { "add-ip",       "add a trusted IP (v4 or v6)" },
            { "remove-ip",    "pick a trusted IP to remove" },
        };
        int choice = tui_menu_at("Auth", items, 6, &sel);
        if (choice < 0) return;
        switch (choice) {
            case 0: auth_list_tokens(c);  break;
            case 1: auth_add_token(c);    break;
            case 2: auth_remove_token(c); break;
            case 3: auth_list_ips(c);     break;
            case 4: auth_add_ip(c);       break;
            case 5: auth_remove_ip(c);    break;
        }
    }
}

/* ---- Migrate menu ---- schema export/import for local→prod bootstrap.
   No data, no tokens — just object/dir/index definitions. Roundtrip uses
   the existing db-dirs, list-objects, describe-object, and create-object
   modes; no server-side changes needed. */

/* Reconstruct the "name:type[:size][:scale]" spec string the daemon expects
   when it parses a fields.conf entry. describe-object emits the on-disk
   varchar size (= N+2 for the length prefix), so we strip 2 to recover the
   user-facing N. Numeric P (precision) isn't preserved — describe-object
   only round-trips S (scale) — so we default P=18 (max int64 digits) on
   export. P is informational; it doesn't affect storage. */
static void cached_field_to_spec(const CachedField *fd, char *out, size_t out_sz) {
    if (strcmp(fd->type, "varchar") == 0) {
        int n = fd->size - 2;
        if (n < 0) n = 0;
        snprintf(out, out_sz, "%s:%s:%d", fd->name, fd->type, n);
    } else if (strcmp(fd->type, "numeric") == 0) {
        snprintf(out, out_sz, "%s:%s:18,%d", fd->name, fd->type, fd->scale);
    } else {
        snprintf(out, out_sz, "%s:%s", fd->name, fd->type);
    }
}

static void migrate_export(CliConn *c) {
    FormField fs[1] = {0};
    fs[0].label = "output path"; fs[0].kind = FF_TEXT;
    snprintf(fs[0].value, sizeof(fs[0].value), "schema.json");
    if (tui_form("export-schema → file", fs, 1) != 0) return;
    if (!fs[0].value[0]) { tui_alert("export", "output path required"); return; }

    FILE *out = fopen(fs[0].value, "w");
    if (!out) { tui_alert("export", "cannot open output file for writing"); return; }

    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"db-dirs\"}", &resp, &rlen) != 0) {
        tui_alert("error", "db-dirs failed"); fclose(out); return;
    }
    char **dirs = NULL; int ndirs = 0;
    collect_string_array(resp, NULL, &dirs, &ndirs);
    free(resp);

    fprintf(out, "{\n");
    fprintf(out, "  \"version\": \"2026.05\",\n");
    fprintf(out, "  \"dirs\": [");
    for (int i = 0; i < ndirs; i++) fprintf(out, "%s\"%s\"", i ? "," : "", dirs[i]);
    fprintf(out, "],\n");
    fprintf(out, "  \"objects\": [\n");

    int total = 0;
    int first = 1;
    for (int i = 0; i < ndirs; i++) {
        char req[256];
        snprintf(req, sizeof(req), "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dirs[i]);
        char *list_resp = NULL;
        if (cli_query(c, req, &list_resp, &rlen) != 0) continue;
        char **objs = NULL; int nobjs = 0;
        collect_string_array(list_resp, "objects", &objs, &nobjs);
        free(list_resp);
        for (int j = 0; j < nobjs; j++) {
            ObjectInfo oi;
            if (describe_object(c, dirs[i], objs[j], &oi) != 0) continue;
            if (!first) fprintf(out, ",\n");
            fprintf(out,
                "    {\"dir\":\"%s\",\"object\":\"%s\",\"splits\":%d,\"max_key\":%d,\"fields\":[",
                oi.dir, oi.object, oi.splits, oi.max_key);
            for (int f = 0; f < oi.nfields; f++) {
                char spec[256];
                cached_field_to_spec(&oi.fields[f], spec, sizeof(spec));
                fprintf(out, "%s\"%s\"", f ? "," : "", spec);
            }
            fprintf(out, "],\"indexes\":[");
            for (int x = 0; x < oi.nindexes; x++)
                fprintf(out, "%s\"%s\"", x ? "," : "", oi.indexes[x]);
            fprintf(out, "]}");
            first = 0;
            total++;
        }
        for (int j = 0; j < nobjs; j++) free(objs[j]);
        free(objs);
    }
    fprintf(out, "\n  ]\n}\n");
    fclose(out);

    for (int i = 0; i < ndirs; i++) free(dirs[i]);
    free(dirs);

    char msg[512];
    snprintf(msg, sizeof(msg),
        "exported %d object%s across %d tenant%s to %s\n\n"
        "no data and no tokens were copied — only schema/index definitions.\n"
        "use migrate import on the prod machine to recreate.",
        total, total == 1 ? "" : "s",
        ndirs, ndirs == 1 ? "" : "s", fs[0].value);
    tui_alert("export-schema", msg);
}

/* Walk a JSON array of object entries and dispatch one create-object per row.
   Each row has dir/object/splits/max_key/fields[]/indexes[]. We pass the
   entry's `fields` and `indexes` arrays through verbatim — the manifest's
   shape exactly matches what create-object expects. */
typedef struct {
    CliConn *c;
    int      created;
    int      skipped;   /* if_not_exists hit */
    int      failed;
    int      if_not_exists;
} ImportCtx;

static int import_object_cb(const char *elem, size_t len, void *ctx) {
    ImportCtx *ic = (ImportCtx *)ctx;

    /* Wedge "if_not_exists":true into the create-object request between
       its closing brace and the elem's bytes. Easiest: copy elem verbatim,
       prepend mode + suffix-inject the flag. */
    char dir[64] = "", object[64] = "";
    /* We need dir+object to log progress; everything else passed through. */
    /* Tiny manual scan for our two keys. */
    const char *p = elem, *end = elem + len;
    while (p < end) {
        const char *k = strchr(p, '"');
        if (!k || k >= end) break;
        k++;
        const char *kend = strchr(k, '"');
        if (!kend || kend >= end) break;
        size_t klen = (size_t)(kend - k);
        const char *colon = strchr(kend, ':');
        if (!colon || colon >= end) break;
        const char *vs = colon + 1;
        while (vs < end && (*vs == ' ' || *vs == '\t')) vs++;
        if (vs >= end) break;
        if (klen == 3 && memcmp(k, "dir", 3) == 0 && *vs == '"') {
            const char *ve = strchr(vs + 1, '"');
            if (ve && (size_t)(ve - vs - 1) < sizeof(dir)) {
                memcpy(dir, vs + 1, ve - vs - 1);
                dir[ve - vs - 1] = '\0';
            }
        } else if (klen == 6 && memcmp(k, "object", 6) == 0 && *vs == '"') {
            const char *ve = strchr(vs + 1, '"');
            if (ve && (size_t)(ve - vs - 1) < sizeof(object)) {
                memcpy(object, vs + 1, ve - vs - 1);
                object[ve - vs - 1] = '\0';
            }
        }
        p = colon + 1;
        /* Skip past this value naively — fine since we only care about two
           keys and any miss just means we move on. */
        const char *next = strchr(p, ',');
        if (!next || next >= end) break;
        p = next + 1;
    }

    /* Build request: prepend mode, append if_not_exists if requested. */
    size_t req_cap = len + 256;
    char *req = malloc(req_cap);
    /* The manifest entry is {"dir":"...","object":"...","splits":...,...}.
       create-object expects the same shape under mode "create-object", so
       we just prepend "mode":"create-object", into the entry. */
    if (len < 2) { free(req); ic->failed++; return 0; }
    int n;
    if (ic->if_not_exists) {
        n = snprintf(req, req_cap,
            "{\"mode\":\"create-object\",\"if_not_exists\":true,%.*s",
            (int)(len - 1), elem + 1);
    } else {
        n = snprintf(req, req_cap,
            "{\"mode\":\"create-object\",%.*s",
            (int)(len - 1), elem + 1);
    }
    (void)n;

    char *resp = NULL; size_t rlen = 0;
    int rc = cli_query(ic->c, req, &resp, &rlen);
    free(req);
    if (rc != 0 || !resp) { ic->failed++; if (resp) free(resp); return 0; }
    /* Parse status/error from response. */
    if (strstr(resp, "\"status\":\"created\""))    ic->created++;
    else if (strstr(resp, "\"status\":\"exists\""))ic->skipped++;
    else if (strstr(resp, "\"error\":"))           ic->failed++;
    else                                           ic->created++;
    free(resp);
    return 0;
}

/* Walk the JSON manifest array. Reuses the json_array_iter logic from views.c
   indirectly via collect_string_array isn't quite the right shape — we need
   the per-element raw slice. So mini-walker inline. */
static int import_walk_objects(const char *manifest, ImportCtx *ic) {
    /* Find "objects": [ ... ]. */
    const char *p = strstr(manifest, "\"objects\"");
    if (!p) return -1;
    p = strchr(p, '[');
    if (!p) return -1;
    p++;
    int depth = 0;
    const char *elem_start = NULL;
    while (*p) {
        if (*p == '"') {
            p++;
            while (*p && *p != '"') {
                if (*p == '\\' && p[1]) p++;
                p++;
            }
            if (*p == '"') p++;
            continue;
        }
        if (*p == '{') {
            if (depth == 0) elem_start = p;
            depth++;
        } else if (*p == '}') {
            depth--;
            if (depth == 0 && elem_start) {
                size_t len = (size_t)(p - elem_start + 1);
                import_object_cb(elem_start, len, ic);
                elem_start = NULL;
            }
        } else if (*p == ']' && depth == 0) {
            return 0;
        }
        p++;
    }
    return 0;
}

static void migrate_import(CliConn *c) {
    FormField fs[2] = {0};
    fs[0].label = "input path"; fs[0].kind = FF_TEXT;
    snprintf(fs[0].value, sizeof(fs[0].value), "schema.json");
    static const char *const ne_choices[] = { "skip-existing", "fail-on-collision", NULL };
    fs[1].label = "on collision"; fs[1].kind = FF_CHOICE; fs[1].choices = ne_choices;
    snprintf(fs[1].value, sizeof(fs[1].value), "skip-existing");
    if (tui_form("import-schema ← file", fs, 2) != 0) return;
    if (!fs[0].value[0]) { tui_alert("import", "input path required"); return; }

    FILE *in = fopen(fs[0].value, "r");
    if (!in) { tui_alert("import", "cannot open file for reading"); return; }
    fseek(in, 0, SEEK_END);
    long sz = ftell(in);
    fseek(in, 0, SEEK_SET);
    if (sz <= 0 || sz > 100*1024*1024) {
        tui_alert("import", "manifest is empty or larger than 100 MB");
        fclose(in); return;
    }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { tui_alert("import", "out of memory"); fclose(in); return; }
    if (fread(buf, 1, (size_t)sz, in) != (size_t)sz) {
        tui_alert("import", "short read");
        free(buf); fclose(in); return;
    }
    buf[sz] = '\0';
    fclose(in);

    ImportCtx ic = { c, 0, 0, 0,
                     strcmp(fs[1].value, "skip-existing") == 0 ? 1 : 0 };
    if (import_walk_objects(buf, &ic) != 0) {
        tui_alert("import", "manifest is missing 'objects' array — not a valid export");
        free(buf); return;
    }
    free(buf);

    char msg[512];
    snprintf(msg, sizeof(msg),
        "import complete:\n"
        "  created : %d\n"
        "  skipped : %d   (already existed)\n"
        "  failed  : %d\n\n"
        "no data was copied — only schema/index definitions.",
        ic.created, ic.skipped, ic.failed);
    tui_alert("import-schema", msg);
}

static void menu_migrate(void) {
    CliConn *c = get_conn();
    if (!c) return;
    int sel = 0;
    for (;;) {
        MenuItem items[] = {
            { "export schema", "write all dir/object/index definitions to a JSON file (no data)" },
            { "import schema", "replay an exported manifest against this DB (recreate objects)" },
        };
        int choice = tui_menu_at("Migrate", items, 2, &sel);
        if (choice < 0) return;
        switch (choice) {
            case 0: migrate_export(c); break;
            case 1: migrate_import(c); break;
        }
    }
}

/* ---- Tenants menu ---- add/remove tenant directories. */

static void tenants_list(CliConn *c) {
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"db-dirs\"}", &resp, &rlen) != 0) {
        tui_alert("error", "db-dirs failed"); return;
    }
    tui_show_text("tenants", resp);
    free(resp);
}

static void tenants_add(CliConn *c) {
    FormField fs[1] = {0};
    fs[0].label = "tenant name"; fs[0].kind = FF_TEXT;
    if (tui_form("add-dir (no /, \\, .., control chars; max 64 bytes)", fs, 1) != 0) return;
    if (!fs[0].value[0]) { tui_alert("add-dir", "name required"); return; }
    char req[256];
    snprintf(req, sizeof(req), "{\"mode\":\"add-dir\",\"dir\":\"%s\"}", fs[0].value);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "add-dir failed");
    else { show_response("add-dir", resp); free(resp); }
}

static void tenants_remove(CliConn *c) {
    char *dir = pick_tenant(c);
    if (!dir) return;

    /* Default: empty-check enforced. Offer a "force" choice. */
    static const char *const force_choices[] = { "no (refuse if non-empty)", "yes (force)", NULL };
    FormField fs[1] = {0};
    fs[0].label = "force"; fs[0].kind = FF_CHOICE; fs[0].choices = force_choices;
    snprintf(fs[0].value, sizeof(fs[0].value), "no (refuse if non-empty)");
    if (tui_form("remove-dir options", fs, 1) != 0) { free(dir); return; }
    int force = (fs[0].value[0] == 'y');

    char prompt[128];
    snprintf(prompt, sizeof(prompt), "remove tenant '%s'?%s",
             dir, force ? " (force, even if non-empty)" : "");
    if (!tui_confirm(prompt)) { free(dir); return; }

    char req[256];
    if (force) snprintf(req, sizeof(req),
        "{\"mode\":\"remove-dir\",\"dir\":\"%s\",\"check_empty\":\"false\"}", dir);
    else       snprintf(req, sizeof(req),
        "{\"mode\":\"remove-dir\",\"dir\":\"%s\"}", dir);
    free(dir);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "remove-dir failed");
    else { show_response("remove-dir", resp); free(resp); }
}

static void menu_tenants(void) {
    CliConn *c = get_conn();
    if (!c) return;
    int sel = 0;
    for (;;) {
        MenuItem items[] = {
            { "list",       "show all tenant directories" },
            { "add-dir",    "register a new tenant (creates the on-disk dir too)" },
            { "remove-dir", "remove a tenant from dirs.conf (data preserved on disk)" },
        };
        int choice = tui_menu_at("Tenants", items, 3, &sel);
        if (choice < 0) return;
        switch (choice) {
            case 0: tenants_list(c);   break;
            case 1: tenants_add(c);    break;
            case 2: tenants_remove(c); break;
        }
    }
}

/* ---- Files menu ---- put/get shell out to ./shard-db (file I/O + base64
   already there); delete-file is a plain JSON request via cli_query. */

static void files_put(CliConn *c) {
    (void)c;
    ObjectInfo oi;
    if (pick_object(get_conn(), &oi) != 0) return;
    FormField fs[1] = {0};
    fs[0].label = "local file path"; fs[0].kind = FF_TEXT;
    for (;;) {
        if (tui_form("put-file ← local path", fs, 1) != 0) return;
        if (!fs[0].value[0]) { tui_alert("put-file", "path required"); continue; }
        char cmd[2048];
        snprintf(cmd, sizeof(cmd),
            "./shard-db put-file '%s' '%s' '%s' 2>&1",
            oi.dir, oi.object, fs[0].value);
        char *out = run_capture(cmd);
        tui_alert("put-file", out ? out : "(no output)");
        free(out);
    }
}

static void files_get(CliConn *c) {
    (void)c;
    ObjectInfo oi;
    if (pick_object(get_conn(), &oi) != 0) return;
    FormField fs[2] = {0};
    fs[0].label = "filename";    fs[0].kind = FF_TEXT;
    fs[1].label = "save-as path"; fs[1].kind = FF_TEXT;  /* blank = stdout */
    for (;;) {
        if (tui_form("get-file → local path", fs, 2) != 0) return;
        if (!fs[0].value[0]) { tui_alert("get-file", "filename required"); continue; }
        char cmd[2048];
        if (fs[1].value[0]) {
            snprintf(cmd, sizeof(cmd),
                "./shard-db get-file '%s' '%s' '%s' '%s' 2>&1",
                oi.dir, oi.object, fs[0].value, fs[1].value);
        } else {
            snprintf(cmd, sizeof(cmd),
                "./shard-db get-file '%s' '%s' '%s' 2>&1 | head -c 4096",
                oi.dir, oi.object, fs[0].value);
        }
        char *out = run_capture(cmd);
        tui_alert("get-file", out ? out : "(no output)");
        free(out);
    }
}

static void files_delete(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    FormField fs[1] = {0};
    fs[0].label = "filename"; fs[0].kind = FF_TEXT;
    for (;;) {
        if (tui_form("delete-file", fs, 1) != 0) return;
        if (!fs[0].value[0]) { tui_alert("delete-file", "filename required"); continue; }
        char prompt[256];
        snprintf(prompt, sizeof(prompt), "delete '%s' from %s/%s?",
                 fs[0].value, oi.dir, oi.object);
        if (!tui_confirm(prompt)) continue;
        char req[1024];
        snprintf(req, sizeof(req),
            "{\"mode\":\"delete-file\",\"dir\":\"%s\",\"object\":\"%s\",\"filename\":\"%s\"}",
            oi.dir, oi.object, fs[0].value);
        char *resp = NULL; size_t rlen = 0;
        if (cli_query(c, req, &resp, &rlen) != 0) tui_alert("error", "delete-file failed");
        else { show_response("delete-file", resp); free(resp); }
    }
}

static void menu_files(void) {
    CliConn *c = get_conn();
    if (!c) return;
    int sel = 0;
    for (;;) {
        MenuItem items[] = {
            { "put-file",    "upload a local file to <dir>/<obj>/files/" },
            { "get-file",    "download a stored file to a local path" },
            { "delete-file", "remove a stored file by name (confirms)" },
        };
        int choice = tui_menu_at("Files", items, 3, &sel);
        if (choice < 0) return;
        switch (choice) {
            case 0: files_put(c);    break;
            case 1: files_get(c);    break;
            case 2: files_delete(c); break;
        }
    }
}

/* ---- Diagnostics menu ---- per-shard, vacuum advisory, quick size. */

static void diag_shard_stats(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"shard-stats\",\"dir\":\"%s\",\"object\":\"%s\"}",
        oi.dir, oi.object);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) {
        tui_alert("error", "shard-stats failed"); return;
    }
    char title[128];
    snprintf(title, sizeof(title), "shard-stats %s/%s", oi.dir, oi.object);
    /* Response is a JSON array of {shard, slots, used, ...} per shard. */
    if (resp[0] == '[') tui_show_table(title, resp);
    else                show_response(title, resp);
    free(resp);
}

static void diag_vacuum_check(CliConn *c) {
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"vacuum-check\"}", &resp, &rlen) != 0) {
        tui_alert("error", "vacuum-check failed"); return;
    }
    if (resp[0] == '[') {
        /* Empty array → "no objects need vacuum". */
        if (strncmp(resp, "[]", 2) == 0)
            tui_alert("vacuum-check", "no objects currently need vacuum");
        else
            tui_show_table("vacuum-check", resp);
    } else {
        show_response("vacuum-check", resp);
    }
    free(resp);
}

static void diag_size(CliConn *c) {
    ObjectInfo oi;
    if (pick_object(c, &oi) != 0) return;
    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"size\",\"dir\":\"%s\",\"object\":\"%s\"}",
        oi.dir, oi.object);
    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) {
        tui_alert("error", "size failed"); return;
    }
    char title[128];
    snprintf(title, sizeof(title), "size %s/%s", oi.dir, oi.object);
    show_response(title, resp);
    free(resp);
}

static void menu_diagnostics(void) {
    CliConn *c = get_conn();
    if (!c) return;
    int sel = 0;
    for (;;) {
        MenuItem items[] = {
            { "shard-stats",  "per-shard load table for one object (find hot shards)" },
            { "vacuum-check", "objects with high tombstone ratios" },
            { "size",         "quick record count for one object" },
        };
        int choice = tui_menu_at("Diagnostics", items, 3, &sel);
        if (choice < 0) return;
        switch (choice) {
            case 0: diag_shard_stats(c);  break;
            case 1: diag_vacuum_check(c); break;
            case 2: diag_size(c);         break;
        }
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

    int top_sel = 0;
    for (;;) {
        MenuItem items[] = {
            { "Server",      "start / stop / status" },
            { "Browse",      "tenants → objects → describe" },
            { "Query",       "insert / get / update / delete / find / count / aggregate / keys / fetch / exists" },
            { "Schema",      "create/drop object, fields, indexes, reindex" },
            { "Maintenance", "vacuum / recount / truncate / backup" },
            { "Tenants",     "list / add / remove tenant directories" },
            { "Auth",        "tokens and trusted-IP allowlist" },
            { "Files",       "put-file / get-file / delete-file (per object)" },
            { "Migrate",     "export/import schema to bootstrap another DB (no data)" },
            { "Diagnostics", "shard-stats / vacuum-check / size" },
            { "Stats",       "live counters, refreshes every 5s" },
            { "Quit",        "exit shard-cli" },
        };
        int choice = tui_menu_at("main menu", items, 12, &top_sel);
        if (choice < 0 || choice == 11) break;
        switch (choice) {
            case 0:  menu_server();      break;
            case 1:  menu_browse();      break;
            case 2:  menu_query();       break;
            case 3:  menu_schema();      break;
            case 4:  menu_maintenance(); break;
            case 5:  menu_tenants();     break;
            case 6:  menu_auth();        break;
            case 7:  menu_files();       break;
            case 8:  menu_migrate();     break;
            case 9:  menu_diagnostics(); break;
            case 10: menu_stats();       break;
        }
    }

    if (g_conn) cli_close(g_conn);
    return 0;
}
