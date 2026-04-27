/* Primitive ncurses widgets — menu, pick, alert, confirm, form, status bar.
   Stays generic; views.c composes these for shard-db-specific output. */

#define _GNU_SOURCE
#include "cli.h"

#include <stdarg.h>

static char g_status_buf[256] = "ready";

void tui_init(void) {
    initscr();
    cbreak();
    noecho();
    keypad(stdscr, TRUE);
    curs_set(0);
    /* ncurses default ESCDELAY is 1000ms — way too slow for "ESC = back".
       25ms is the standard recommendation; fast enough that single ESC
       feels instant, slow enough that real escape sequences (arrow keys
       etc) still arrive together. */
    set_escdelay(25);
    if (has_colors()) {
        start_color();
        use_default_colors();
        init_pair(1, COLOR_CYAN,   -1);   /* titles */
        init_pair(2, COLOR_GREEN,  -1);   /* selected row */
        init_pair(3, COLOR_YELLOW, -1);   /* hints / status */
        init_pair(4, COLOR_RED,    -1);   /* errors */
    }
}

void tui_shutdown(void) {
    endwin();
}

void tui_status(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(g_status_buf, sizeof(g_status_buf), fmt, ap);
    va_end(ap);
}

static void draw_status_bar(void) {
    int rows, cols;
    getmaxyx(stdscr, rows, cols);
    move(rows - 1, 0);
    clrtoeol();
    attron(COLOR_PAIR(3));
    mvprintw(rows - 1, 0, " %.*s", cols - 1, g_status_buf);
    attroff(COLOR_PAIR(3));
}

static void draw_title(const char *title) {
    int cols;
    cols = getmaxx(stdscr);
    attron(COLOR_PAIR(1) | A_BOLD);
    mvprintw(0, 0, "  shard-cli  ");
    attroff(A_BOLD);
    mvprintw(0, 14, "| %.*s", cols - 16, title);
    attroff(COLOR_PAIR(1));
    mvhline(1, 0, ACS_HLINE, cols);
}

void tui_alert(const char *title, const char *body) {
    int rows, cols;
    getmaxyx(stdscr, rows, cols);
    int w = cols * 3 / 4;
    if (w < 40) w = (cols < 40 ? cols - 4 : 40);
    int h = 0;
    /* Count body lines (with wrap by w). */
    const char *p = body;
    int lc = 1, ll = 0;
    while (*p) {
        if (*p == '\n' || ll >= w - 4) { lc++; ll = 0; if (*p != '\n') continue; }
        else ll++;
        p++;
    }
    h = lc + 4;
    if (h > rows - 4) h = rows - 4;
    int y = (rows - h) / 2;
    int x = (cols - w) / 2;

    WINDOW *win = newwin(h, w, y, x);
    box(win, 0, 0);
    wattron(win, COLOR_PAIR(1) | A_BOLD);
    mvwprintw(win, 0, 2, " %s ", title ? title : "");
    wattroff(win, COLOR_PAIR(1) | A_BOLD);

    /* Render body with simple wrap. */
    p = body;
    int row = 1, col = 2;
    while (*p && row < h - 2) {
        if (*p == '\n') { row++; col = 2; p++; continue; }
        if (col >= w - 2) { row++; col = 2; }
        if (row < h - 2) mvwaddch(win, row, col++, *p);
        p++;
    }
    mvwprintw(win, h - 2, 2, "[press any key]");
    wrefresh(win);
    wgetch(win);
    delwin(win);
    touchwin(stdscr);
    refresh();
}

int tui_confirm(const char *prompt) {
    int rows, cols;
    getmaxyx(stdscr, rows, cols);
    int w = (int)strlen(prompt) + 16;
    if (w > cols - 4) w = cols - 4;
    int h = 5;
    int y = (rows - h) / 2;
    int x = (cols - w) / 2;
    WINDOW *win = newwin(h, w, y, x);
    box(win, 0, 0);
    wattron(win, COLOR_PAIR(3) | A_BOLD);
    mvwprintw(win, 0, 2, " confirm ");
    wattroff(win, COLOR_PAIR(3) | A_BOLD);
    mvwprintw(win, 1, 2, "%.*s", w - 4, prompt);
    mvwprintw(win, 3, 2, "[y]es / [n]o");
    wrefresh(win);
    int rc = 0;
    for (;;) {
        int ch = wgetch(win);
        if (ch == 'y' || ch == 'Y') { rc = 1; break; }
        if (ch == 'n' || ch == 'N' || ch == 27 /* ESC */ || ch == 'q') { rc = 0; break; }
    }
    delwin(win);
    touchwin(stdscr);
    refresh();
    return rc;
}

int tui_menu_at(const char *title, const MenuItem *items, int nitems, int *sel_io) {
    if (nitems == 0) return -1;
    int sel = (sel_io && *sel_io >= 0 && *sel_io < nitems) ? *sel_io : 0;
    for (;;) {
        int rows, cols;
        getmaxyx(stdscr, rows, cols);
        erase();
        draw_title(title);

        int top = 3;
        for (int i = 0; i < nitems; i++) {
            int row = top + i;
            if (row >= rows - 3) break;
            if (i == sel) attron(COLOR_PAIR(2) | A_REVERSE);
            mvprintw(row, 4, " %2d. %-30s ", i + 1, items[i].label);
            if (i == sel) attroff(COLOR_PAIR(2) | A_REVERSE);
        }

        if (items[sel].hint) {
            attron(COLOR_PAIR(3));
            mvprintw(rows - 3, 4, "%.*s", cols - 8, items[sel].hint);
            attroff(COLOR_PAIR(3));
        }
        mvprintw(rows - 2, 4, "↑↓/jk select   →/⏎ open   ←/q/ESC back");
        draw_status_bar();
        refresh();

        int ch = getch();
        switch (ch) {
            case KEY_UP:    case 'k': if (sel > 0) sel--; break;
            case KEY_DOWN:  case 'j': if (sel < nitems - 1) sel++; break;
            case KEY_HOME:  case 'g': sel = 0; break;
            case KEY_END:   case 'G': sel = nitems - 1; break;
            case '\n': case '\r': case KEY_ENTER:
            case KEY_RIGHT: case 'l':
                if (sel_io) *sel_io = sel;
                return sel;
            case 'q': case 27: case KEY_LEFT: case 'h':
                if (sel_io) *sel_io = sel;
                return -1;
            default:
                if (ch >= '1' && ch <= '9') {
                    int n = ch - '1';
                    if (n < nitems) {
                        sel = n;
                        if (sel_io) *sel_io = sel;
                        return n;
                    }
                }
                break;
        }
    }
}

int tui_menu(const char *title, const MenuItem *items, int nitems) {
    int local = 0;
    return tui_menu_at(title, items, nitems, &local);
}

int tui_multi_pick(const char *title, const char *const *values, int nvalues,
                   int *selected) {
    if (nvalues <= 0) {
        tui_alert(title, "(no items)");
        return -1;
    }
    int sel = 0;
    for (;;) {
        int rows, cols;
        getmaxyx(stdscr, rows, cols);
        erase();
        attron(COLOR_PAIR(1) | A_BOLD);
        mvprintw(0, 0, " %s ", title ? title : "");
        attroff(COLOR_PAIR(1) | A_BOLD);
        mvhline(1, 0, ACS_HLINE, cols);

        int top = 3;
        for (int i = 0; i < nvalues; i++) {
            int row = top + i;
            if (row >= rows - 3) break;
            const char *check = selected[i] ? "[x]" : "[ ]";
            if (i == sel) attron(COLOR_PAIR(2) | A_REVERSE);
            mvprintw(row, 4, " %s  %-30s ", check, values[i]);
            if (i == sel) attroff(COLOR_PAIR(2) | A_REVERSE);
        }

        attron(COLOR_PAIR(3));
        mvprintw(rows - 2, 4, "↑↓/jk move   space toggle   ⏎ confirm   ←/q/ESC cancel");
        attroff(COLOR_PAIR(3));
        refresh();

        int ch = getch();
        switch (ch) {
            case KEY_UP: case 'k':   if (sel > 0) sel--; break;
            case KEY_DOWN: case 'j': if (sel < nvalues - 1) sel++; break;
            case KEY_HOME: case 'g': sel = 0; break;
            case KEY_END:  case 'G': sel = nvalues - 1; break;
            case ' ':                selected[sel] = !selected[sel]; break;
            case '\n': case '\r': case KEY_ENTER:
                return 0;
            case 'q': case 27: case KEY_LEFT: case 'h':
                return -1;
        }
    }
}

const char *tui_pick(const char *title, const char *const *values, int nvalues) {
    if (nvalues <= 0) {
        tui_alert(title, "(no items)");
        return NULL;
    }
    /* Reuse menu by adapting the MenuItem array. */
    MenuItem *items = calloc(nvalues, sizeof(MenuItem));
    for (int i = 0; i < nvalues; i++) items[i].label = values[i];
    int idx = tui_menu(title, items, nvalues);
    free(items);
    return idx >= 0 ? values[idx] : NULL;
}

int tui_list_input(const char *title, const char *item_label,
                   char items[][LIST_ITEM_MAX], int max, int *n_io) {
    char input[LIST_ITEM_MAX] = "";
    int sel = -1;  /* -1 = focus on input; 0..n-1 = focus on a row */

    for (;;) {
        int rows, cols;
        getmaxyx(stdscr, rows, cols);
        erase();
        attron(COLOR_PAIR(1) | A_BOLD);
        mvprintw(0, 0, "  shard-cli  ");
        attroff(A_BOLD);
        mvprintw(0, 14, "| %.*s", cols - 16, title ? title : "");
        attroff(COLOR_PAIR(1));
        mvhline(1, 0, ACS_HLINE, cols);

        int top = 3;
        int n = *n_io;
        for (int i = 0; i < n; i++) {
            int row = top + i;
            if (row >= rows - 6) break;
            if (i == sel) attron(COLOR_PAIR(2) | A_REVERSE);
            mvprintw(row, 4, " %2d. %-*s ",
                     i + 1, cols - 12, items[i]);
            if (i == sel) attroff(COLOR_PAIR(2) | A_REVERSE);
        }
        if (n == 0) {
            attron(COLOR_PAIR(3));
            mvprintw(top, 4, "(empty — type a %s and press TAB to add)",
                     item_label ? item_label : "value");
            attroff(COLOR_PAIR(3));
        }

        int input_row = rows - 5;
        attron(sel < 0 ? (COLOR_PAIR(2) | A_REVERSE) : A_UNDERLINE);
        mvprintw(input_row, 4, " %s: %-*s ",
                 item_label ? item_label : "value",
                 cols - 12 - (int)strlen(item_label ? item_label : "value"),
                 input);
        attroff(sel < 0 ? (COLOR_PAIR(2) | A_REVERSE) : A_UNDERLINE);

        attron(COLOR_PAIR(3));
        if (sel < 0)
            mvprintw(rows - 3, 4,
                "type to enter   TAB add to list   ⏎ submit (%d item%s)   "
                "BACKSPACE pop last   ESC back",
                n, n == 1 ? "" : "s");
        else
            mvprintw(rows - 3, 4,
                "↑↓ navigate   d/DEL remove   ⏎ edit   TAB back to input   "
                "←/q/ESC cancel");
        attroff(COLOR_PAIR(3));
        refresh();

        int ch = getch();

        /* Item-row focus mode. */
        if (sel >= 0) {
            switch (ch) {
                case KEY_UP: case 'k':   if (sel > 0) sel--; break;
                case KEY_DOWN: case 'j': if (sel < n - 1) sel++; break;
                case '\t':                sel = -1; break;
                case 'd': case KEY_DC: case 127: case 8: case KEY_BACKSPACE:
                    /* Remove highlighted. */
                    for (int i = sel; i < n - 1; i++)
                        memcpy(items[i], items[i + 1], LIST_ITEM_MAX);
                    items[n - 1][0] = '\0';
                    n--;
                    *n_io = n;
                    if (sel >= n) sel = n - 1;
                    if (n == 0) sel = -1;
                    break;
                case '\n': case '\r': case KEY_ENTER:
                    /* Pull the row back into the input for editing. */
                    snprintf(input, sizeof(input), "%s", items[sel]);
                    for (int i = sel; i < n - 1; i++)
                        memcpy(items[i], items[i + 1], LIST_ITEM_MAX);
                    items[n - 1][0] = '\0';
                    n--;
                    *n_io = n;
                    sel = -1;
                    break;
                case 'q': case 27: case KEY_LEFT: case 'h':
                    return -1;
            }
            continue;
        }

        /* Input focus mode. */
        size_t L = strlen(input);
        switch (ch) {
            case '\t':
                if (L > 0) {
                    if (n >= max) { tui_alert(title, "list is full"); break; }
                    snprintf(items[n], LIST_ITEM_MAX, "%s", input);
                    *n_io = ++n;
                    input[0] = '\0';
                }
                break;
            case '\n': case '\r': case KEY_ENTER:
                if (L > 0) {
                    if (n >= max) { tui_alert(title, "list is full"); break; }
                    snprintf(items[n], LIST_ITEM_MAX, "%s", input);
                    *n_io = ++n;
                    input[0] = '\0';
                }
                if (*n_io == 0) {
                    tui_alert(title, "list is empty — type a value and TAB or ⏎ to add");
                    break;
                }
                return 0;
            case KEY_BACKSPACE: case 127: case 8:
                if (L > 0) {
                    input[L - 1] = '\0';
                } else if (n > 0) {
                    /* Pop last item back into input for re-edit. */
                    snprintf(input, sizeof(input), "%s", items[n - 1]);
                    items[n - 1][0] = '\0';
                    *n_io = --n;
                }
                break;
            case KEY_UP:
                if (n > 0) sel = n - 1;
                break;
            case 27:
                /* Only ESC cancels in input mode — q is a valid key character
                   the user might type, and ←/h could be cursor navigation
                   (not supported yet, but reserved). To exit, press ↑ to
                   move focus to a list row and use q/←/ESC there. */
                return -1;
            default:
                if (ch >= 32 && ch < 127 && L < LIST_ITEM_MAX - 1) {
                    input[L] = (char)ch;
                    input[L + 1] = '\0';
                }
                break;
        }
    }
}

int tui_form(const char *title, FormField *fields, int nfields) {
    int sel = 0;
    if (nfields <= 0) return -1;

    for (;;) {
        int rows, cols;
        getmaxyx(stdscr, rows, cols);
        erase();
        draw_title(title);

        int top = 3;
        for (int i = 0; i < nfields; i++) {
            int row = top + i * 2;
            if (row >= rows - 5) break;
            if (i == sel) attron(A_BOLD);
            mvprintw(row, 4, "%-16s :", fields[i].label);
            if (i == sel) attroff(A_BOLD);

            /* Field value box */
            int xv = 24;
            int wv = cols - xv - 4;
            if (i == sel) attron(COLOR_PAIR(2) | A_REVERSE);
            else          attron(A_UNDERLINE);
            char shown[256];
            snprintf(shown, sizeof(shown), "%-*.*s", wv, wv, fields[i].value);
            mvprintw(row, xv, " %s ", shown);
            if (i == sel) attroff(COLOR_PAIR(2) | A_REVERSE);
            else          attroff(A_UNDERLINE);

            if (fields[i].kind == FF_CHOICE && fields[i].choices) {
                /* Hint listing all choices */
                attron(COLOR_PAIR(3));
                int x = 4;
                int row2 = row + 1;
                for (int j = 0; fields[i].choices[j]; j++) {
                    int len = (int)strlen(fields[i].choices[j]) + 2;
                    if (x + len > cols - 4) break;
                    mvprintw(row2, x, " %s ", fields[i].choices[j]);
                    x += len + 1;
                }
                attroff(COLOR_PAIR(3));
            }
        }

        const char *help = sel < nfields && fields[sel].kind == FF_CHOICE
                       ? "↑↓ choose row  ←→ cycle choice  ⏎ submit  ESC cancel"
                       : "↑↓ choose row  type to edit  ⏎ submit  ESC cancel";
        mvprintw(rows - 2, 4, "%s", help);
        draw_status_bar();
        refresh();

        int ch = getch();
        if (ch == 27) return -1;
        if (ch == '\n' || ch == '\r' || ch == KEY_ENTER) return 0;
        if (ch == KEY_UP || ch == KEY_BTAB) {
            if (sel > 0) sel--;
            continue;
        }
        if (ch == KEY_DOWN || ch == '\t') {
            if (sel < nfields - 1) sel++;
            continue;
        }

        FormField *f = &fields[sel];

        /* Choice cycling */
        if (f->kind == FF_CHOICE && f->choices) {
            if (ch == KEY_LEFT || ch == KEY_RIGHT) {
                int cur = -1, ncho = 0;
                for (int j = 0; f->choices[j]; j++) {
                    if (strcmp(f->choices[j], f->value) == 0) cur = j;
                    ncho++;
                }
                if (cur < 0) cur = 0;
                if (ch == KEY_LEFT)  cur = (cur == 0 ? ncho - 1 : cur - 1);
                if (ch == KEY_RIGHT) cur = (cur + 1) % ncho;
                snprintf(f->value, sizeof(f->value), "%s", f->choices[cur]);
                continue;
            }
        }

        /* Text editing */
        if (ch == KEY_BACKSPACE || ch == 127 || ch == 8) {
            size_t L = strlen(f->value);
            if (L > 0) f->value[L - 1] = '\0';
            continue;
        }
        if (ch >= 32 && ch < 127) {
            if (f->kind == FF_NUMBER && !((ch >= '0' && ch <= '9') || ch == '-' || ch == '.')) continue;
            size_t L = strlen(f->value);
            if (L < sizeof(f->value) - 1) {
                f->value[L]     = (char)ch;
                f->value[L + 1] = '\0';
            }
        }
    }
}
