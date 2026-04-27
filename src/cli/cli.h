#ifndef SHARD_CLI_H
#define SHARD_CLI_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ncurses.h>

/* ============================================================
   shard-cli — ncurses TUI client for shard-db.

   Talks to a running shard-db daemon over the same TCP/JSON wire
   the existing CLI uses, with optional native TLS. Reads PORT,
   TLS_*, and TOKEN params from environment (db.env should be
   sourced before launch).

   Phase 1 menus: Server (start/stop/status), Browse (db-dirs →
   list-objects → describe-object), Query (insert/get/find/count/
   exists with field-by-field criteria builder), Stats (5s live
   refresh).
   ============================================================ */

/* ---- Connection ---- */

typedef struct CliConn CliConn;

/* Open a TCP+optionally-TLS connection to host:port. token is sent on every
   request as an "auth" field; pass NULL/"" for trusted-IP / no-auth setups.
   Returns NULL on failure (errno set or ssl error logged to stderr). */
CliConn *cli_connect(const char *host, int port);
void cli_close(CliConn *c);

/* Send one JSON request, accumulate the response up to the \0 sentinel.
   Caller frees *out_resp. Returns 0 on success, -1 on connection error.
   Connection is reused across calls. token (if set) is injected as an
   "auth" key automatically — caller's request_json should NOT include auth. */
int cli_query(CliConn *c, const char *request_json,
              char **out_resp, size_t *out_len);

/* ---- Connection params (loaded once at startup from environment) ---- */

extern char g_cli_host[128];
extern int  g_cli_port;
extern int  g_cli_tls_enable;
extern char g_cli_token[256];

void cli_load_env(void);

/* ---- TUI primitives ---- */

void tui_init(void);
void tui_shutdown(void);

/* Status bar at the bottom — updated when actions report back. */
void tui_status(const char *fmt, ...);

/* Modal message — single key to dismiss. */
void tui_alert(const char *title, const char *body);

/* Yes/no confirm prompt. Returns 1 for yes, 0 for no/escape. */
int  tui_confirm(const char *prompt);

/* ---- Menu widget ---- */

typedef struct {
    const char *label;        /* shown in the list */
    const char *hint;         /* one-line description shown beneath the menu */
} MenuItem;

/* Show a vertical menu, return index of selection (-1 if user pressed q/ESC). */
int tui_menu(const char *title, const MenuItem *items, int nitems);

/* Same as tui_menu but uses *sel as the initial highlight position and writes
   the user's last position back. Use this in section menus that loop on their
   own dispatch so back-navigation returns to the previously-highlighted row
   instead of jumping to the top. */
int tui_menu_at(const char *title, const MenuItem *items, int nitems, int *sel);

/* Pick a single value from a list of strings — used for tenant/object/field
   selection. Returns the chosen string (caller must NOT free) or NULL. */
const char *tui_pick(const char *title, const char *const *values, int nvalues);

/* Multi-pick: caller-owned `selected[i]` is in/out (0/1 for each value).
   Space toggles the highlighted row, ⏎ confirms, ←/q/ESC cancels.
   Returns 0 on confirm, -1 on cancel. */
int tui_multi_pick(const char *title, const char *const *values, int nvalues,
                   int *selected);

/* ---- Form widget ---- */

typedef enum {
    FF_TEXT,    /* single-line text */
    FF_NUMBER,  /* digits only */
    FF_CHOICE,  /* dropdown: choices[] is an array of strings ending with NULL */
} FormFieldKind;

typedef struct {
    const char    *label;
    FormFieldKind  kind;
    char           value[1024];      /* in/out — preset shown, user edits */
    const char *const *choices;      /* for FF_CHOICE; NULL otherwise */
} FormField;

/* Render a form, let the user edit. Returns 0 on submit, -1 on cancel. */
int tui_form(const char *title, FormField *fields, int nfields);

/* ---- Output views ---- */

/* Display a multi-line text body in a scrollable pane. */
void tui_show_text(const char *title, const char *body);

/* Preview a JSON request and let the user copy / run / back-out. Returns
   1 = run, 0 = back to the previous form (re-show with sticky values).
   Body is shown scrollable and unmodified so terminal-select copies the
   exact wire format. */
int tui_preview_json(const char *title, const char *body);

/* Render a JSON object as key:value pairs in a pane. Naive parser — handles
   the response shapes shard-db emits. */
void tui_show_object(const char *title, const char *json);

/* Render a JSON array of objects as a table. Caller picks columns, or we
   auto-detect from the first row's keys. */
void tui_show_table(const char *title, const char *json_array);

/* Live-refresh stats: poll {"mode":"stats"} every interval_sec until user
   presses q/ESC. */
void tui_stats_live(CliConn *c, int interval_sec);

/* ---- Schema cache (filled by describe-object on demand) ----

   Used by the criteria builder to populate field/op dropdowns. */

#define MAX_FIELDS_CACHED 32

typedef struct {
    char  name[64];
    char  type[16];   /* "varchar","int","long","short","double","bool","byte",
                         "numeric","date","datetime" */
    int   size;
    int   scale;
} CachedField;

typedef struct {
    char        dir[64];
    char        object[64];
    int         splits;
    int         max_key;
    int         max_value;
    int         record_count;
    CachedField fields[MAX_FIELDS_CACHED];
    int         nfields;
    char        indexes[MAX_FIELDS_CACHED][64];
    int         nindexes;
} ObjectInfo;

/* Fetch describe-object into oi. Returns 0 on success, -1 on error. */
int describe_object(CliConn *c, const char *dir, const char *object, ObjectInfo *oi);

/* ---- Criteria builder ----

   Build a JSON criteria array via a row-per-leaf form. Returns 0 on submit
   with criteria_out set to a malloc'd JSON array (caller frees), or -1 on
   cancel. AND-only for Phase 1 — OR/nested via raw JSON in v1.1. */
int tui_criteria_builder(const ObjectInfo *oi, char **criteria_out);

#endif
