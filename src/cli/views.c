/* Output panels: text view, JSON object as kv-pairs, JSON array as table,
   live stats refresh, describe-object helper, criteria builder.

   Tiny JSON parser tailored to shard-db response shapes — handles strings,
   numbers, booleans, null, nested objects/arrays. Not RFC 8259 perfect; enough
   for menu population and result display. */

#define _GNU_SOURCE
#include "cli.h"

#include <stdarg.h>
#include <unistd.h>

/* ============================================================
   Tiny JSON parser
   ============================================================ */

static const char *skip_ws(const char *p) {
    while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    return p;
}

/* If *p is at a JSON value, advance *p past the entire value (string/number/
   bool/null/object/array). Returns the start byte offset (so caller can copy
   the slice). Updates *p to one past the value. */
static const char *skip_value(const char **p) {
    *p = skip_ws(*p);
    const char *start = *p;
    if (**p == '"') {
        (*p)++;
        while (**p && **p != '"') {
            if (**p == '\\' && *(*p + 1)) (*p)++;
            (*p)++;
        }
        if (**p == '"') (*p)++;
    } else if (**p == '{' || **p == '[') {
        char open = **p, close = (open == '{' ? '}' : ']');
        int depth = 0;
        do {
            if (**p == '"') {
                (*p)++;
                while (**p && **p != '"') {
                    if (**p == '\\' && *(*p + 1)) (*p)++;
                    (*p)++;
                }
                if (**p == '"') (*p)++;
                continue;
            }
            if (**p == open) depth++;
            else if (**p == close) depth--;
            if (**p) (*p)++;
        } while (depth > 0 && **p);
    } else {
        while (**p && **p != ',' && **p != '}' && **p != ']' &&
               **p != ' ' && **p != '\n' && **p != '\t' && **p != '\r')
            (*p)++;
    }
    return start;
}

/* Find a top-level "key" inside a JSON object string. Returns pointer to the
   value start (just past the colon's whitespace), or NULL. *value_len is set
   to the length of the value's verbatim slice. */
static const char *json_find_key(const char *json, const char *key, size_t *value_len) {
    size_t klen = strlen(key);
    const char *p = skip_ws(json);
    if (*p != '{') return NULL;
    p++;
    while (*p) {
        p = skip_ws(p);
        if (*p == '}') return NULL;
        if (*p != '"') return NULL;
        p++;
        const char *kstart = p;
        while (*p && *p != '"') p++;
        size_t this_klen = p - kstart;
        if (*p == '"') p++;
        p = skip_ws(p);
        if (*p != ':') return NULL;
        p++;
        p = skip_ws(p);
        const char *vstart = p;
        skip_value(&p);
        if (this_klen == klen && memcmp(kstart, key, klen) == 0) {
            *value_len = (size_t)(p - vstart);
            return vstart;
        }
        p = skip_ws(p);
        if (*p == ',') p++;
    }
    return NULL;
}

/* Copy an unescaped JSON string value into out (max out_sz-1 bytes). The
   value at vstart is `"...."` with quotes; this strips them and unescapes
   common escapes. Returns 0 on success. */
static int json_string_into(const char *vstart, size_t vlen, char *out, size_t out_sz) {
    if (vlen < 2 || vstart[0] != '"') {
        /* Not a string — emit verbatim slice. */
        size_t n = vlen < out_sz - 1 ? vlen : out_sz - 1;
        memcpy(out, vstart, n);
        out[n] = '\0';
        return 0;
    }
    const char *p = vstart + 1;
    const char *end = vstart + vlen - 1; /* points at closing quote */
    size_t o = 0;
    while (p < end && o < out_sz - 1) {
        if (*p == '\\' && p + 1 < end) {
            switch (p[1]) {
                case 'n': out[o++] = '\n'; break;
                case 't': out[o++] = '\t'; break;
                case 'r': out[o++] = '\r'; break;
                case '"': out[o++] = '"';  break;
                case '\\': out[o++] = '\\'; break;
                case '/': out[o++] = '/';  break;
                default:  out[o++] = p[1]; break;
            }
            p += 2;
        } else {
            out[o++] = *p++;
        }
    }
    out[o] = '\0';
    return 0;
}

/* Iterate elements of a JSON array slice (vstart includes the [ ]). Calls
   cb for each element with its verbatim slice. Stops when cb returns nonzero. */
static void json_array_iter(const char *vstart, size_t vlen,
                            int (*cb)(const char *elem, size_t len, void *ctx),
                            void *ctx) {
    if (vlen < 2 || vstart[0] != '[') return;
    const char *p = vstart + 1;
    const char *end = vstart + vlen - 1; /* points at closing ] */
    while (p < end) {
        p = skip_ws(p);
        if (p >= end || *p == ']') break;
        const char *estart = p;
        skip_value(&p);
        size_t elen = (size_t)(p - estart);
        if (cb(estart, elen, ctx) != 0) return;
        p = skip_ws(p);
        if (*p == ',') p++;
    }
}

/* ============================================================
   Text + JSON object display
   ============================================================ */

int tui_preview_json(const char *title, const char *body) {
    int rows, cols;
    int top = 0;
    int blen = (int)strlen(body);
    int line_starts[4096];
    int nlines = 0;
    int pos = 0;
    line_starts[nlines++] = 0;
    while (pos < blen && nlines < 4095) {
        if (body[pos] == '\n') line_starts[nlines++] = pos + 1;
        pos++;
    }
    line_starts[nlines] = blen;

    for (;;) {
        getmaxyx(stdscr, rows, cols);
        erase();
        attron(COLOR_PAIR(1) | A_BOLD);
        mvprintw(0, 0, " %s ", title ? title : "");
        attroff(COLOR_PAIR(1) | A_BOLD);
        mvhline(1, 0, ACS_HLINE, cols);

        int view_rows = rows - 5;
        for (int i = 0; i < view_rows && top + i < nlines; i++) {
            int s = line_starts[top + i];
            int e = line_starts[top + i + 1];
            int len = e - s;
            if (len > 0 && body[s + len - 1] == '\n') len--;
            if (len > cols - 4) len = cols - 4;
            mvprintw(2 + i, 2, "%.*s", len, body + s);
        }
        attron(COLOR_PAIR(3));
        mvprintw(rows - 3, 4,
            "↑↓/jk scroll   r/⏎ run   ←/q/ESC back to edit   (line %d/%d)",
            top + 1, nlines);
        attroff(COLOR_PAIR(3));
        refresh();

        int ch = getch();
        switch (ch) {
            case KEY_UP: case 'k':   if (top > 0) top--; break;
            case KEY_DOWN: case 'j': if (top < nlines - 1) top++; break;
            case KEY_PPAGE: case 'b':
                top -= rows - 6;
                if (top < 0) top = 0;
                break;
            case KEY_NPAGE: case ' ': case 'f':
                top += rows - 6;
                if (top > nlines - 1) top = nlines - 1;
                break;
            case 'g': case KEY_HOME: top = 0; break;
            case 'G': case KEY_END:  top = nlines - 1; break;
            case 'r': case '\n': case '\r': case KEY_ENTER:
                return 1;
            case 'q': case 27: case KEY_LEFT: case 'h':
                return 0;
        }
    }
}

void tui_show_text(const char *title, const char *body) {
    int rows, cols;
    getmaxyx(stdscr, rows, cols);
    int top = 0;
    /* Pre-split the body into lines for vertical scroll. */
    int line_starts[4096];
    int nlines = 0;
    int pos = 0;
    line_starts[nlines++] = 0;
    int blen = (int)strlen(body);
    while (pos < blen && nlines < 4095) {
        if (body[pos] == '\n') line_starts[nlines++] = pos + 1;
        pos++;
    }
    line_starts[nlines] = blen;

    for (;;) {
        erase();
        attron(COLOR_PAIR(1) | A_BOLD);
        mvprintw(0, 0, " %s ", title ? title : "");
        attroff(COLOR_PAIR(1) | A_BOLD);
        mvhline(1, 0, ACS_HLINE, cols);

        int view_rows = rows - 4;
        for (int i = 0; i < view_rows && top + i < nlines; i++) {
            int s = line_starts[top + i];
            int e = line_starts[top + i + 1];
            int len = e - s;
            if (len > 0 && body[s + len - 1] == '\n') len--;
            if (len > cols - 4) len = cols - 4;
            mvprintw(2 + i, 2, "%.*s", len, body + s);
        }
        attron(COLOR_PAIR(3));
        mvprintw(rows - 2, 4, "↑↓/jk scroll   q/ESC close   line %d/%d",
                 top + 1, nlines);
        attroff(COLOR_PAIR(3));
        refresh();

        int ch = getch();
        switch (ch) {
            case KEY_UP: case 'k':   if (top > 0) top--; break;
            case KEY_DOWN: case 'j': if (top < nlines - 1) top++; break;
            case KEY_PPAGE: case 'b':
                top -= rows - 6;
                if (top < 0) top = 0;
                break;
            case KEY_NPAGE: case ' ': case 'f':
                top += rows - 6;
                if (top > nlines - 1) top = nlines - 1;
                break;
            case 'g': case KEY_HOME: top = 0; break;
            case 'G': case KEY_END:  top = nlines - 1; break;
            case 'q': case 27: return;
        }
    }
}

void tui_show_object(const char *title, const char *json) {
    /* Pretty-print: walk top-level keys, emit as "key: value" lines. */
    char buf[16384];
    size_t bo = 0;
    const char *p = skip_ws(json);
    if (*p != '{') {
        tui_show_text(title, json);
        return;
    }
    p++;
    while (*p && bo < sizeof(buf) - 1) {
        p = skip_ws(p);
        if (*p == '}' || *p == '\0') break;
        if (*p != '"') break;
        p++;
        const char *kstart = p;
        while (*p && *p != '"') p++;
        size_t klen = (size_t)(p - kstart);
        if (*p == '"') p++;
        p = skip_ws(p);
        if (*p != ':') break;
        p++;
        p = skip_ws(p);
        const char *vstart = p;
        skip_value(&p);
        size_t vlen = (size_t)(p - vstart);

        bo += snprintf(buf + bo, sizeof(buf) - bo, "%-14.*s : ", (int)klen, kstart);
        if (vlen > 0 && vstart[0] == '"') {
            char tmp[2048];
            json_string_into(vstart, vlen, tmp, sizeof(tmp));
            bo += snprintf(buf + bo, sizeof(buf) - bo, "%s\n", tmp);
        } else {
            size_t n = vlen < (sizeof(buf) - bo - 2) ? vlen : (sizeof(buf) - bo - 2);
            memcpy(buf + bo, vstart, n); bo += n;
            if (bo < sizeof(buf) - 1) buf[bo++] = '\n';
        }

        p = skip_ws(p);
        if (*p == ',') p++;
    }
    buf[bo] = '\0';
    tui_show_text(title, buf);
}

/* ============================================================
   Table rendering — JSON array of objects → columnar
   ============================================================ */

#define MAX_COLS 16
#define MAX_ROWS 4096

typedef struct {
    char  cols[MAX_COLS][32];     /* column names */
    int   ncols;
    char *cells[MAX_ROWS][MAX_COLS];  /* malloc'd */
    int   nrows;
    int   widths[MAX_COLS];
} Table;

static int table_col_index(Table *t, const char *name, size_t len) {
    if (len >= sizeof(t->cols[0])) len = sizeof(t->cols[0]) - 1;
    for (int i = 0; i < t->ncols; i++) {
        if (strncmp(t->cols[i], name, len) == 0 && t->cols[i][len] == '\0')
            return i;
    }
    if (t->ncols >= MAX_COLS) return -1;
    int idx = t->ncols++;
    memcpy(t->cols[idx], name, len);
    t->cols[idx][len] = '\0';
    t->widths[idx] = (int)len;
    return idx;
}

static int row_collect_cb(const char *elem, size_t len, void *ctx) {
    Table *t = (Table *)ctx;
    if (t->nrows >= MAX_ROWS) return 1;
    if (len < 2 || elem[0] != '{') return 0;
    /* For "key" responses (find): {"key":"..","value":{...}} — flatten value
       into top-level columns. */
    int row = t->nrows++;
    for (int i = 0; i < MAX_COLS; i++) t->cells[row][i] = NULL;

    /* Walk keys */
    const char *p = elem + 1;
    const char *end = elem + len - 1;
    while (p < end) {
        p = skip_ws(p);
        if (*p != '"') break;
        p++;
        const char *kstart = p;
        while (p < end && *p != '"') p++;
        size_t klen = (size_t)(p - kstart);
        if (p < end) p++;
        p = skip_ws(p);
        if (*p != ':') break;
        p++;
        p = skip_ws(p);
        const char *vstart = p;
        skip_value(&p);
        size_t vlen = (size_t)(p - vstart);

        if (klen == 5 && memcmp(kstart, "value", 5) == 0 &&
            vlen > 0 && vstart[0] == '{') {
            /* Recurse into value object as flat columns. */
            const char *vp = vstart + 1;
            const char *vend = vstart + vlen - 1;
            while (vp < vend) {
                vp = skip_ws(vp);
                if (*vp != '"') break;
                vp++;
                const char *kk = vp;
                while (vp < vend && *vp != '"') vp++;
                size_t kkl = (size_t)(vp - kk);
                if (vp < vend) vp++;
                vp = skip_ws(vp);
                if (*vp != ':') break;
                vp++;
                vp = skip_ws(vp);
                const char *vv = vp;
                skip_value(&vp);
                size_t vvl = (size_t)(vp - vv);
                int col = table_col_index(t, kk, kkl);
                if (col >= 0) {
                    char *cell;
                    if (vvl > 0 && vv[0] == '"') {
                        cell = malloc(vvl);
                        json_string_into(vv, vvl, cell, vvl);
                    } else {
                        cell = malloc(vvl + 1);
                        memcpy(cell, vv, vvl);
                        cell[vvl] = '\0';
                    }
                    t->cells[row][col] = cell;
                    int w = (int)strlen(cell);
                    if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
                }
                vp = skip_ws(vp);
                if (*vp == ',') vp++;
            }
        } else {
            int col = table_col_index(t, kstart, klen);
            if (col >= 0) {
                char *cell;
                if (vlen > 0 && vstart[0] == '"') {
                    cell = malloc(vlen);
                    json_string_into(vstart, vlen, cell, vlen);
                } else {
                    cell = malloc(vlen + 1);
                    memcpy(cell, vstart, vlen);
                    cell[vlen] = '\0';
                }
                t->cells[row][col] = cell;
                int w = (int)strlen(cell);
                if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
            }
        }
        p = skip_ws(p);
        if (*p == ',') p++;
    }
    return 0;
}

static void table_free(Table *t) {
    for (int r = 0; r < t->nrows; r++)
        for (int c = 0; c < t->ncols; c++)
            free(t->cells[r][c]);
}

/* Callback: row entry is a single scalar (string/number/bool). Used by
   shapes like `["k1","k2",...]` (keys mode) — emits a one-column "value"
   table. Header is set by tui_show_table before iterating. */
static int row_collect_string_cb(const char *elem, size_t len, void *ctx) {
    Table *t = (Table *)ctx;
    if (t->nrows >= MAX_ROWS) return 1;
    int row = t->nrows++;
    for (int i = 0; i < MAX_COLS; i++) t->cells[row][i] = NULL;
    char *cell;
    if (len > 0 && elem[0] == '"') {
        cell = malloc(len);
        json_string_into(elem, len, cell, len);
    } else {
        cell = malloc(len + 1);
        memcpy(cell, elem, len);
        cell[len] = '\0';
    }
    t->cells[row][0] = cell;
    int w = (int)strlen(cell);
    if (w > t->widths[0]) t->widths[0] = (w > 64 ? 64 : w);
    return 0;
}

/* Callback: row entry is a JSON ARRAY of scalar values aligned to t.cols
   (filled in advance by tui_show_table's columns parser). One element per
   column; mismatched lengths fill missing cells with empty strings. */
static int row_collect_array_cb(const char *elem, size_t len, void *ctx) {
    Table *t = (Table *)ctx;
    if (t->nrows >= MAX_ROWS) return 1;
    if (len < 2 || elem[0] != '[') return 0;
    int row = t->nrows++;
    for (int i = 0; i < MAX_COLS; i++) t->cells[row][i] = NULL;

    const char *p = elem + 1;
    const char *end = elem + len - 1;
    int col = 0;
    while (p < end && col < t->ncols) {
        p = skip_ws(p);
        if (*p == ']') break;
        const char *vstart = p;
        skip_value(&p);
        size_t vlen = (size_t)(p - vstart);
        char *cell;
        if (vlen > 0 && vstart[0] == '"') {
            cell = malloc(vlen);
            json_string_into(vstart, vlen, cell, vlen);
        } else {
            cell = malloc(vlen + 1);
            memcpy(cell, vstart, vlen);
            cell[vlen] = '\0';
        }
        t->cells[row][col] = cell;
        int w = (int)strlen(cell);
        if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
        col++;
        p = skip_ws(p);
        if (*p == ',') p++;
    }
    return 0;
}

/* tui_show_table returns 0 on close (q/←/ESC), 1 if user pressed 'e' to
   request CSV export. Callers that pass a JSON response can re-issue the
   underlying request with format:"csv" and write the result to a file. */
int tui_show_table(const char *title, const char *json) {
    Table t;
    memset(&t, 0, sizeof(t));

    /* tui_show_table is the single render path for every data-returning
       JSON response. Five shapes the daemon emits today, all handled here:

         1. {"columns":[...],"rows":[[v1,v2],...]}     — find/fetch format=rows
         2. {"results":[{...}], "cursor":...}          — fetch default
         3. {"rows":[...]}                             — generic envelope
         4. [{...},{...}]                              — find default, agg group_by
         5. ["s1","s2"]                                — keys default
         6. {"k1":"v1","k2":"v2"}                      — count, exists, agg scalar

       (1) drives column names + array rows. (2)/(3) just unwrap and recurse.
       (4) auto-detects columns from each object's keys (special-cases "value"
       to flatten a nested record). (5) becomes a single-column "value" table.
       (6) becomes a two-column "metric/value" table. */

    const char *arr = json;
    int rendered = 0;

    if (*json == '{') {
        size_t cols_len, rows_len, res_len;
        const char *cols_v = json_find_key(json, "columns", &cols_len);
        const char *rows_v = json_find_key(json, "rows", &rows_len);
        const char *res_v  = json_find_key(json, "results", &res_len);

        if (cols_v && rows_v) {
            /* Shape 1: explicit columns + array rows. */
            const char *cp = cols_v + 1;
            const char *cend = cols_v + cols_len - 1;
            while (cp < cend && t.ncols < MAX_COLS) {
                cp = skip_ws(cp);
                if (*cp == ']') break;
                const char *vstart = cp;
                skip_value(&cp);
                size_t vlen = (size_t)(cp - vstart);
                char tmp[64];
                if (vlen > 0 && vstart[0] == '"')
                    json_string_into(vstart, vlen, tmp, sizeof(tmp));
                else
                    snprintf(tmp, sizeof(tmp), "%.*s", (int)vlen, vstart);
                int idx = t.ncols++;
                snprintf(t.cols[idx], sizeof(t.cols[0]), "%s", tmp);
                t.widths[idx] = (int)strlen(tmp);
                cp = skip_ws(cp);
                if (*cp == ',') cp++;
            }
            json_array_iter(rows_v, rows_len, row_collect_array_cb, &t);
            rendered = 1;
        } else if (res_v) {       /* Shape 2: unwrap and re-point. */
            arr = res_v;
        } else if (rows_v) {       /* Shape 3: unwrap and re-point. */
            arr = rows_v;
        } else {
            /* Shape 4 (single): {"key":"...","value":{...}} — single record
               from `get`. Treat as a 1-element array so the same column-
               extraction logic find/fetch use kicks in (key + nested value
               fields flattened into one row). */
            size_t klen, vvl;
            const char *k_v = json_find_key(json, "key", &klen);
            const char *v_v = json_find_key(json, "value", &vvl);
            if (k_v && v_v && vvl > 0 && v_v[0] == '{') {
                row_collect_cb(json, strlen(json), &t);
                rendered = 1;
            }
        }
        if (!rendered && *json == '{' && !cols_v && !rows_v && !res_v) {
            /* Shape 6: plain object → 2-column metric/value table. */
            snprintf(t.cols[0], sizeof(t.cols[0]), "metric");
            snprintf(t.cols[1], sizeof(t.cols[0]), "value");
            t.widths[0] = 6; t.widths[1] = 5;
            t.ncols = 2;

            const char *p = skip_ws(json) + 1;  /* past leading { */
            while (*p && t.nrows < MAX_ROWS) {
                p = skip_ws(p);
                if (*p == '}') break;
                if (*p != '"') break;
                p++;
                const char *ks = p;
                while (*p && *p != '"') p++;
                size_t klen = (size_t)(p - ks);
                if (*p == '"') p++;
                p = skip_ws(p);
                if (*p != ':') break;
                p++;
                p = skip_ws(p);
                const char *vs = p;
                skip_value(&p);
                size_t vlen = (size_t)(p - vs);

                int row = t.nrows++;
                t.cells[row][0] = malloc(klen + 1);
                memcpy(t.cells[row][0], ks, klen);
                t.cells[row][0][klen] = '\0';
                if ((int)klen > t.widths[0]) t.widths[0] = klen > 32 ? 32 : (int)klen;

                t.cells[row][1] = malloc(vlen + 1);
                if (vlen > 0 && vs[0] == '"') {
                    json_string_into(vs, vlen, t.cells[row][1], vlen + 1);
                } else {
                    memcpy(t.cells[row][1], vs, vlen);
                    t.cells[row][1][vlen] = '\0';
                }
                int w = (int)strlen(t.cells[row][1]);
                if (w > t.widths[1]) t.widths[1] = (w > 64 ? 64 : w);

                p = skip_ws(p);
                if (*p == ',') p++;
            }
            rendered = 1;
        }
    }

    if (!rendered && *arr == '[') {
        /* Peek the first element to choose object-row vs string-row path. */
        const char *q = arr + 1;
        while (*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r') q++;
        if (*q == '{') {
            /* Shape 4: array of objects — auto-detect columns from keys. */
            json_array_iter(arr, strlen(arr), row_collect_cb, &t);
        } else if (*q == ']') {
            /* Empty array. */
        } else {
            /* Shape 5: array of scalars → single-column "value" table. */
            snprintf(t.cols[0], sizeof(t.cols[0]), "value");
            t.widths[0] = 5;
            t.ncols = 1;
            json_array_iter(arr, strlen(arr), row_collect_string_cb, &t);
        }
    }

    if (t.ncols == 0 || t.nrows == 0) {
        tui_show_text(title, "(no rows)");
        table_free(&t);
        return 0;
    }

    int rows, cols;
    int top = 0, hscroll = 0;
    for (;;) {
        getmaxyx(stdscr, rows, cols);
        erase();
        attron(COLOR_PAIR(1) | A_BOLD);
        mvprintw(0, 0, " %s [%d row%s × %d col%s] ",
                 title ? title : "", t.nrows, t.nrows == 1 ? "" : "s",
                 t.ncols, t.ncols == 1 ? "" : "s");
        attroff(COLOR_PAIR(1) | A_BOLD);
        mvhline(1, 0, ACS_HLINE, cols);

        /* Header */
        int x = 2 - hscroll;
        attron(A_BOLD | COLOR_PAIR(1));
        for (int c = 0; c < t.ncols; c++) {
            int w = t.widths[c] + 2;
            if (x >= 0 && x < cols)
                mvprintw(2, x, " %-*.*s|", t.widths[c], t.widths[c], t.cols[c]);
            x += w + 1;
        }
        attroff(A_BOLD | COLOR_PAIR(1));
        mvhline(3, 0, ACS_HLINE, cols);

        /* Rows */
        int view = rows - 6;
        for (int i = 0; i < view && top + i < t.nrows; i++) {
            int row = top + i;
            x = 2 - hscroll;
            for (int c = 0; c < t.ncols; c++) {
                int w = t.widths[c] + 2;
                const char *cell = t.cells[row][c] ? t.cells[row][c] : "";
                if (x >= 0 && x < cols)
                    mvprintw(4 + i, x, " %-*.*s|", t.widths[c], t.widths[c], cell);
                x += w + 1;
            }
        }

        attron(COLOR_PAIR(3));
        mvprintw(rows - 2, 4,
                 "↑↓/jk row  ←→/hl scroll-x  e export-csv  q/ESC close   row %d/%d",
                 top + 1, t.nrows);
        attroff(COLOR_PAIR(3));
        refresh();

        int ch = getch();
        switch (ch) {
            case KEY_UP: case 'k':    if (top > 0) top--; break;
            case KEY_DOWN: case 'j':  if (top < t.nrows - 1) top++; break;
            case KEY_PPAGE: case 'b': top -= view; if (top < 0) top = 0; break;
            case KEY_NPAGE: case ' ': top += view;
                                       if (top > t.nrows - 1) top = t.nrows - 1; break;
            case 'g': case KEY_HOME:  top = 0; break;
            case 'G': case KEY_END:   top = t.nrows - 1; break;
            case KEY_LEFT: case 'h':  hscroll -= 8; if (hscroll < 0) hscroll = 0; break;
            case KEY_RIGHT: case 'l': hscroll += 8; break;
            case 'e':                 table_free(&t); return 1;
            case 'q': case 27:        table_free(&t); return 0;
        }
    }
}

/* ============================================================
   Stats live refresh
   ============================================================ */

/* Render a JSON object's top-level keys as a 2-column table (metric / value).
   Used by stats live refresh. Keeps drift-friendly column widths so values
   are right-aligned for easy diff-spotting. */
typedef struct {
    char metric[64];
    char value[64];
} StatRow;

/* Emit one StatRow with `prefix.child = value` formatting, scalar values
   only — caller flattens nested objects manually. */
static int emit_row(StatRow *rows, int n, int max_rows,
                    const char *prefix,
                    const char *kstart, size_t klen,
                    const char *vstart, size_t vlen) {
    if (n >= max_rows) return n;
    char metric[64];
    if (prefix && prefix[0])
        snprintf(metric, sizeof(metric), "%s.%.*s", prefix, (int)klen, kstart);
    else
        snprintf(metric, sizeof(metric), "%.*s", (int)klen, kstart);
    snprintf(rows[n].metric, sizeof(rows[n].metric), "%s", metric);
    if (vlen > 0 && vstart[0] == '"')
        json_string_into(vstart, vlen, rows[n].value, sizeof(rows[n].value));
    else if (vlen > 0 && vstart[0] == '[')
        snprintf(rows[n].value, sizeof(rows[n].value), "<array>");
    else {
        size_t L = vlen < sizeof(rows[n].value) - 1 ? vlen : sizeof(rows[n].value) - 1;
        memcpy(rows[n].value, vstart, L);
        rows[n].value[L] = '\0';
    }
    return n + 1;
}

static int parse_stats_into(const char *json, StatRow *rows, int max_rows) {
    int n = 0;
    const char *p = skip_ws(json);
    if (*p != '{') return 0;
    p++;
    while (*p && n < max_rows) {
        p = skip_ws(p);
        if (*p == '}') break;
        if (*p != '"') break;
        p++;
        const char *ks = p;
        while (*p && *p != '"') p++;
        size_t klen = (size_t)(p - ks);
        if (*p == '"') p++;
        p = skip_ws(p);
        if (*p != ':') break;
        p++;
        p = skip_ws(p);
        const char *vs = p;
        skip_value(&p);
        size_t vlen = (size_t)(p - vs);

        /* Flatten one level of nested object: ucache → ucache.used,
           ucache.total, … (much more readable than dumping the JSON). */
        if (vlen > 0 && vs[0] == '{') {
            char prefix[64];
            snprintf(prefix, sizeof(prefix), "%.*s", (int)klen, ks);
            const char *np = vs + 1;
            const char *nend = vs + vlen - 1;
            while (np < nend && n < max_rows) {
                np = skip_ws(np);
                if (*np == '}' || np >= nend) break;
                if (*np != '"') break;
                np++;
                const char *nks = np;
                while (np < nend && *np != '"') np++;
                size_t nklen = (size_t)(np - nks);
                if (np < nend) np++;
                np = skip_ws(np);
                if (*np != ':') break;
                np++;
                np = skip_ws(np);
                const char *nvs = np;
                skip_value(&np);
                size_t nvlen = (size_t)(np - nvs);
                n = emit_row(rows, n, max_rows, prefix, nks, nklen, nvs, nvlen);
                np = skip_ws(np);
                if (*np == ',') np++;
            }
        } else {
            n = emit_row(rows, n, max_rows, NULL, ks, klen, vs, vlen);
        }
        p = skip_ws(p);
        if (*p == ',') p++;
    }
    return n;
}

void tui_stats_live(CliConn *c, int interval_sec) {
    int rows, cols;
    halfdelay(interval_sec * 10);  /* deciseconds */
    int tick = 0;
    StatRow stat_rows[64];
    int top = 0;

    for (;;) {
        char *resp = NULL; size_t rlen = 0;
        int rc = cli_query(c, "{\"mode\":\"stats\"}", &resp, &rlen);

        getmaxyx(stdscr, rows, cols);
        erase();
        attron(COLOR_PAIR(1) | A_BOLD);
        mvprintw(0, 0, " stats — refresh every %ds (tick %d) ", interval_sec, tick++);
        attroff(COLOR_PAIR(1) | A_BOLD);
        mvhline(1, 0, ACS_HLINE, cols);

        if (rc != 0 || !resp) {
            attron(COLOR_PAIR(4));
            mvprintw(3, 4, "connection error");
            attroff(COLOR_PAIR(4));
        } else {
            int n = parse_stats_into(resp, stat_rows, 64);
            free(resp);

            /* Compute column widths. */
            int wm = (int)strlen("metric"), wv = (int)strlen("value");
            for (int i = 0; i < n; i++) {
                int lm = (int)strlen(stat_rows[i].metric);
                int lv = (int)strlen(stat_rows[i].value);
                if (lm > wm) wm = lm;
                if (lv > wv) wv = lv;
            }
            if (wm > 32) wm = 32;
            if (wv > 32) wv = 32;

            /* Header row. ASCII | column separator (some terminals/locales
               render Unicode box-drawing chars as garbage like ~T~B). */
            attron(A_BOLD | COLOR_PAIR(1));
            mvprintw(2, 4, " %-*s | %*s ", wm, "metric", wv, "value");
            attroff(A_BOLD | COLOR_PAIR(1));
            mvhline(3, 4, ACS_HLINE, wm + wv + 7);

            int view = rows - 7;
            for (int i = 0; i < view && top + i < n; i++) {
                int r = top + i;
                mvprintw(4 + i, 4, " %-*.*s | %*.*s ",
                         wm, wm, stat_rows[r].metric,
                         wv, wv, stat_rows[r].value);
            }
            attron(COLOR_PAIR(3));
            mvprintw(rows - 3, 4, "%d metric%s", n, n == 1 ? "" : "s");
            attroff(COLOR_PAIR(3));
        }

        attron(COLOR_PAIR(3));
        mvprintw(rows - 2, 4, "↑↓/jk scroll   q/ESC/← close");
        attroff(COLOR_PAIR(3));
        refresh();

        int ch = getch();
        if (ch == 'q' || ch == 27 || ch == KEY_LEFT || ch == 'h') break;
        if (ch == KEY_UP   || ch == 'k') { if (top > 0) top--; }
        if (ch == KEY_DOWN || ch == 'j') top++;
    }
    cbreak();  /* leave halfdelay mode */
}

/* ============================================================
   describe-object → ObjectInfo cache
   ============================================================ */

int describe_object(CliConn *c, const char *dir, const char *object, ObjectInfo *oi) {
    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"describe-object\",\"dir\":\"%s\",\"object\":\"%s\"}",
             dir, object);
    char *resp = NULL;
    size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0 || !resp) {
        if (resp) free(resp);
        return -1;
    }
    /* Reject error responses early. */
    size_t vlen;
    if (json_find_key(resp, "error", &vlen)) { free(resp); return -1; }

    memset(oi, 0, sizeof(*oi));
    snprintf(oi->dir,    sizeof(oi->dir),    "%s", dir);
    snprintf(oi->object, sizeof(oi->object), "%s", object);

    const char *v;
    if ((v = json_find_key(resp, "splits", &vlen)))    oi->splits = atoi(v);
    if ((v = json_find_key(resp, "max_key", &vlen)))   oi->max_key = atoi(v);
    if ((v = json_find_key(resp, "max_value", &vlen))) oi->max_value = atoi(v);
    if ((v = json_find_key(resp, "record_count", &vlen))) oi->record_count = atoi(v);

    /* fields[] */
    if ((v = json_find_key(resp, "fields", &vlen)) && vlen > 0 && v[0] == '[') {
        const char *p = v + 1;
        const char *end = v + vlen - 1;
        while (p < end && oi->nfields < MAX_FIELDS_CACHED) {
            p = skip_ws(p);
            if (*p == ']') break;
            if (*p != '{') break;
            const char *fstart = p;
            skip_value(&p);
            size_t flen = (size_t)(p - fstart);
            CachedField *fld = &oi->fields[oi->nfields];
            const char *fv;
            size_t fvlen;
            char tmp[256];
            if ((fv = json_find_key(fstart, "name", &fvlen))) {
                /* json_find_key here works because skip_value left fstart..p as
                   a complete object; passing fstart with NUL after isn't safe,
                   but json_find_key only looks for keys, stops at }. Need a
                   bounded variant — write our own walk below. */
                (void)fvlen;
            }
            /* Walk fstart..fstart+flen as an object directly. */
            const char *fp = fstart + 1;
            const char *fend = fstart + flen - 1;
            while (fp < fend) {
                fp = skip_ws(fp);
                if (*fp != '"') break;
                fp++;
                const char *kk = fp;
                while (fp < fend && *fp != '"') fp++;
                size_t kkl = (size_t)(fp - kk);
                if (fp < fend) fp++;
                fp = skip_ws(fp);
                if (*fp != ':') break;
                fp++;
                fp = skip_ws(fp);
                const char *vv = fp;
                skip_value(&fp);
                size_t vvl = (size_t)(fp - vv);
                if (kkl == 4 && memcmp(kk, "name", 4) == 0)
                    json_string_into(vv, vvl, fld->name, sizeof(fld->name));
                else if (kkl == 4 && memcmp(kk, "type", 4) == 0)
                    json_string_into(vv, vvl, fld->type, sizeof(fld->type));
                else if (kkl == 4 && memcmp(kk, "size", 4) == 0) {
                    json_string_into(vv, vvl, tmp, sizeof(tmp));
                    fld->size = atoi(tmp);
                }
                else if (kkl == 5 && memcmp(kk, "scale", 5) == 0) {
                    json_string_into(vv, vvl, tmp, sizeof(tmp));
                    fld->scale = atoi(tmp);
                }
                fp = skip_ws(fp);
                if (*fp == ',') fp++;
            }
            oi->nfields++;
            p = skip_ws(p);
            if (*p == ',') p++;
        }
    }

    /* indexes[] (array of strings) */
    if ((v = json_find_key(resp, "indexes", &vlen)) && vlen > 0 && v[0] == '[') {
        const char *p = v + 1;
        const char *end = v + vlen - 1;
        while (p < end && oi->nindexes < MAX_FIELDS_CACHED) {
            p = skip_ws(p);
            if (*p == ']') break;
            const char *estart = p;
            skip_value(&p);
            size_t elen = (size_t)(p - estart);
            json_string_into(estart, elen,
                             oi->indexes[oi->nindexes],
                             sizeof(oi->indexes[0]));
            oi->nindexes++;
            p = skip_ws(p);
            if (*p == ',') p++;
        }
    }

    free(resp);
    return 0;
}

/* ============================================================
   Criteria builder — list-with-actions UX
   ============================================================

   See the accumulated criteria as a menu. [+] add opens a 3-field form
   (field dropdown, op dropdown, value text). ⏎ on an existing row offers
   to delete it. [✓] submit packs the rows into a JSON array. */

/* CritRow + MAX_CRIT_ROWS now declared in cli.h so callers can own the
   builder state and have it survive across re-runs. */

static const char *const OPS_ALL[] = {
    "eq","neq","lt","gt","lte","gte","between","in","not_in",
    "like","not_like","contains","not_contains","starts","ends",
    "exists","not_exists", NULL
};

/* Open a single-criterion form. *out is in/out: pre-filled values turn this
   into an edit dialog; empty values get sensible defaults for "add new".
   Returns 0 on submit, -1 on cancel. */
static int prompt_one_criterion(const ObjectInfo *oi, CritRow *out) {
    int nfc = oi->nfields;
    if (nfc <= 0) { tui_alert("criteria", "object has no fields"); return -1; }
    const char *field_choices[MAX_FIELDS_CACHED + 1];
    for (int i = 0; i < nfc; i++) field_choices[i] = oi->fields[i].name;
    field_choices[nfc] = NULL;

    int editing = (out->field[0] != '\0');

    FormField fs[3] = {0};
    fs[0].label   = "field"; fs[0].kind = FF_CHOICE; fs[0].choices = field_choices;
    snprintf(fs[0].value, sizeof(fs[0].value), "%s",
             editing ? out->field : oi->fields[0].name);
    fs[1].label   = "op";    fs[1].kind = FF_CHOICE; fs[1].choices = OPS_ALL;
    snprintf(fs[1].value, sizeof(fs[1].value), "%s",
             editing ? out->op : "eq");
    fs[2].label   = "value"; fs[2].kind = FF_TEXT;
    snprintf(fs[2].value, sizeof(fs[2].value), "%s",
             editing ? out->value : "");

    const char *title = editing
        ? "edit criterion (←→ cycle dropdowns)"
        : "add criterion (←→ cycle dropdowns)";
    if (tui_form(title, fs, 3) != 0) return -1;
    if (!fs[0].value[0]) return -1;
    snprintf(out->field, sizeof(out->field), "%s", fs[0].value);
    snprintf(out->op,    sizeof(out->op),    "%s", fs[1].value);
    snprintf(out->value, sizeof(out->value), "%s", fs[2].value);
    return 0;
}

/* Pack accumulated rows into a JSON array string. Caller frees. */
static char *pack_criteria(const CritRow *rows, int n) {
    size_t cap = 1024;
    char *out = malloc(cap);
    if (!out) return NULL;
    size_t off = 0;
    off += snprintf(out + off, cap - off, "[");
    for (int i = 0; i < n; i++) {
        const char *fld = rows[i].field;
        const char *op  = rows[i].op;
        const char *val = rows[i].value;
        if (off + 512 + strlen(val) >= cap) {
            cap *= 2;
            char *nb = realloc(out, cap);
            if (!nb) { free(out); return NULL; }
            out = nb;
        }
        if (i) off += snprintf(out + off, cap - off, ",");

        if (strcmp(op, "between") == 0) {
            char vbuf[1024];
            snprintf(vbuf, sizeof(vbuf), "%s", val);
            char *comma = strchr(vbuf, ',');
            const char *lo = vbuf, *hi = "";
            if (comma) { *comma = '\0'; hi = comma + 1; }
            off += snprintf(out + off, cap - off,
                "{\"field\":\"%s\",\"op\":\"between\",\"value\":\"%s\",\"value2\":\"%s\"}",
                fld, lo, hi);
        } else if (strcmp(op, "exists") == 0 || strcmp(op, "not_exists") == 0) {
            off += snprintf(out + off, cap - off,
                "{\"field\":\"%s\",\"op\":\"%s\"}", fld, op);
        } else if (strcmp(op, "in") == 0 || strcmp(op, "not_in") == 0) {
            off += snprintf(out + off, cap - off,
                "{\"field\":\"%s\",\"op\":\"%s\",\"value\":[", fld, op);
            const char *p = val;
            int first = 1;
            while (*p) {
                while (*p == ' ' || *p == ',') p++;
                if (!*p) break;
                const char *s = p;
                while (*p && *p != ',') p++;
                size_t L = (size_t)(p - s);
                if (off + L + 8 >= cap) {
                    cap *= 2;
                    char *nb = realloc(out, cap);
                    if (!nb) { free(out); return NULL; }
                    out = nb;
                }
                off += snprintf(out + off, cap - off,
                    "%s\"%.*s\"", first ? "" : ",", (int)L, s);
                first = 0;
            }
            off += snprintf(out + off, cap - off, "]}");
        } else {
            off += snprintf(out + off, cap - off,
                "{\"field\":\"%s\",\"op\":\"%s\",\"value\":\"%s\"}",
                fld, op, val);
        }
    }
    off += snprintf(out + off, cap - off, "]");
    return out;
}

int tui_criteria_builder(const ObjectInfo *oi,
                         CritRow *rows, int *n_io,
                         char **criteria_out) {
    int sel = 0;

    for (;;) {
        /* Build the menu from current rows + [+ add] + [✓ submit]. */
        int n = *n_io;
        MenuItem items[MAX_CRIT_ROWS + 2];
        char labels[MAX_CRIT_ROWS + 2][160];
        int total = 0;
        for (int i = 0; i < n; i++) {
            snprintf(labels[total], sizeof(labels[0]),
                     "%-20s  %-6s  %s", rows[i].field, rows[i].op, rows[i].value);
            items[total].label = labels[total];
            items[total].hint  = "⏎ edit   d/DEL delete";
            total++;
        }
        snprintf(labels[total], sizeof(labels[0]), "[+] add criterion");
        items[total].label = labels[total];
        items[total].hint  = (n < MAX_CRIT_ROWS)
                              ? "open a form for the next field/op/value"
                              : "(max criteria reached)";
        int idx_add = total;
        total++;
        snprintf(labels[total], sizeof(labels[0]),
                 "[✓] submit  (%d criteri%s, AND'd)",
                 n, n == 1 ? "on" : "a");
        items[total].label = labels[total];
        items[total].hint  = "build the JSON array and continue";
        int idx_submit = total;
        total++;

        char title[128];
        snprintf(title, sizeof(title), "criteria for %s/%s", oi->dir, oi->object);
        int choice = tui_menu_at(title, items, total, &sel);
        if (choice < 0) {
            *criteria_out = NULL;
            return -1;
        }
        if (choice == idx_submit) {
            *criteria_out = pack_criteria(rows, n);
            return *criteria_out ? 0 : -1;
        }
        if (choice == idx_add) {
            if (n >= MAX_CRIT_ROWS) continue;
            CritRow row = {0};  /* empty → prompt uses defaults */
            if (prompt_one_criterion(oi, &row) == 0) {
                rows[n++] = row;
                *n_io = n;
                sel = idx_add;  /* keep highlight on [+ add] for fast repeat */
            }
            continue;
        }
        /* Existing row — small action menu: edit, delete. */
        MenuItem actions[] = {
            { "edit",   "change field, op, or value" },
            { "delete", "remove this criterion from the list" },
        };
        int act = tui_menu("criterion action", actions, 2);
        if (act == 0) {
            CritRow edit_row = rows[choice];
            if (prompt_one_criterion(oi, &edit_row) == 0)
                rows[choice] = edit_row;
        } else if (act == 1) {
            if (tui_confirm("delete this criterion?")) {
                for (int i = choice; i < n - 1; i++) rows[i] = rows[i + 1];
                n--;
                *n_io = n;
                if (sel >= n) sel = n;
            }
        }
    }
}
