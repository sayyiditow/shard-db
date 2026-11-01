#include "scalar_grammar.h"
#include <arpa/inet.h>
#include <string.h>

static int sp(char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; }
static int strip(const char **s, size_t *n) {
    while (*n > 0 && sp((*s)[0])) { (*s)++; (*n)--; }
    while (*n > 0 && sp((*s)[*n - 1])) (*n)--;
    return *n > 0 && !memchr(*s, '\0', *n);
}
static int dg(char c) { return c >= '0' && c <= '9'; }
static int digs(const char *s, size_t i, size_t w) {
    for (size_t k = 0; k < w; k++) if (!dg(s[i + k])) return 0;
    return 1;
}
static int num(const char *s, size_t i, size_t w) {
    int v = 0;
    for (size_t k = 0; k < w; k++) v = v * 10 + (s[i + k] - '0');
    return v;
}
static int leap(int y) { return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0; }
static int ymd_ok(int y, int m, int d) {
    static const int dm[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (y < 1 || y > 9999 || m < 1 || m > 12 || d < 1) return 0;
    return d <= dm[m - 1] + (m == 2 && leap(y));
}
static int hms_ok(int h, int m, int s) { return h <= 23 && m <= 59 && s <= 59; }

int sg_bool(const char *s, size_t n, uint8_t *out) {
    static const char *const T[] = {"1", "t", "tr", "tru", "true", "y", "ye", "yes", "on"};
    static const char *const F[] = {"0", "f", "fa", "fal", "fals", "false", "n", "no", "of", "off"};
    char b[8];
    if (!strip(&s, &n) || n >= sizeof b) return -1;
    for (size_t i = 0; i < n; i++) b[i] = (s[i] >= 'A' && s[i] <= 'Z') ? (char)(s[i] + 32) : s[i];
    b[n] = '\0';
    for (size_t i = 0; i < sizeof T / sizeof *T; i++) if (!strcmp(b, T[i])) { *out = 1; return 0; }
    for (size_t i = 0; i < sizeof F / sizeof *F; i++) if (!strcmp(b, F[i])) { *out = 0; return 0; }
    return -1;
}

/* date part at s[0..]: compact (8) when iso==0, dashed (10) when iso==1. */
static int ymd_at(const char *s, int iso, int *y, int *m, int *d) {
    if (iso) {
        if (s[4] != '-' || s[7] != '-' || !digs(s, 0, 4) || !digs(s, 5, 2) || !digs(s, 8, 2)) return -1;
        *y = num(s, 0, 4); *m = num(s, 5, 2); *d = num(s, 8, 2);
    } else {
        if (!digs(s, 0, 8)) return -1;
        *y = num(s, 0, 4); *m = num(s, 4, 2); *d = num(s, 6, 2);
    }
    return ymd_ok(*y, *m, *d) ? 0 : -1;
}
/* HH:MM:SS at s[0..8] */
static int hms_at(const char *s, int *h, int *m, int *sec) {
    if (s[2] != ':' || s[5] != ':' || !digs(s, 0, 2) || !digs(s, 3, 2) || !digs(s, 6, 2)) return -1;
    *h = num(s, 0, 2); *m = num(s, 3, 2); *sec = num(s, 6, 2);
    return hms_ok(*h, *m, *sec) ? 0 : -1;
}

int sg_date(const char *s, size_t n, int32_t *out) {
    int y, m, d;
    if (!strip(&s, &n) || (n != 8 && n != 10)) return -1;
    if (ymd_at(s, n == 10, &y, &m, &d)) return -1;
    *out = y * 10000 + m * 100 + d;
    return 0;
}
int sg_datetime(const char *s, size_t n, int32_t *date, uint32_t *secs) {
    int y, m, d, h, mi, sec;
    if (!strip(&s, &n) || (n != 14 && n != 19)) return -1;
    if (n == 14) {
        if (ymd_at(s, 0, &y, &m, &d) || !digs(s, 8, 6)) return -1;
        h = num(s, 8, 2); mi = num(s, 10, 2); sec = num(s, 12, 2);
        if (!hms_ok(h, mi, sec)) return -1;
    } else {
        if ((s[10] != ' ' && s[10] != 'T') || ymd_at(s, 1, &y, &m, &d) || hms_at(s + 11, &h, &mi, &sec)) return -1;
    }
    *date = y * 10000 + m * 100 + d;
    *secs = (uint32_t)(h * 3600 + mi * 60 + sec);
    return 0;
}
int sg_datetimems(const char *s, size_t n, int32_t *date, uint32_t *ms) {
    int y, m, d, h, mi, sec, f;
    if (!strip(&s, &n) || (n != 17 && n != 23)) return -1;
    if (n == 17) {
        if (ymd_at(s, 0, &y, &m, &d) || !digs(s, 8, 9)) return -1;
        h = num(s, 8, 2); mi = num(s, 10, 2); sec = num(s, 12, 2); f = num(s, 14, 3);
        if (!hms_ok(h, mi, sec)) return -1;
    } else {
        if ((s[10] != ' ' && s[10] != 'T') || s[19] != '.' || !digs(s, 20, 3) ||
            ymd_at(s, 1, &y, &m, &d) || hms_at(s + 11, &h, &mi, &sec)) return -1;
        f = num(s, 20, 3);
    }
    *date = y * 10000 + m * 100 + d;
    *ms = (uint32_t)((h * 3600 + mi * 60 + sec) * 1000 + f);
    return 0;
}
int sg_time(const char *s, size_t n, uint32_t *secs) {
    int h, m, sec;
    if (!strip(&s, &n) || n != 8 || hms_at(s, &h, &m, &sec)) return -1;
    *secs = (uint32_t)(h * 3600 + m * 60 + sec);
    return 0;
}
static int hexv(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}
int sg_uuid(const char *s, size_t n, uint8_t out[16]) {
    uint8_t buf[16];
    int pos = 0;
    if (!strip(&s, &n)) return -1;
    if (n >= 2 && s[0] == '{' && s[n - 1] == '}') { s++; n -= 2; }
    if (n != 32 && n != 36) return -1;
    for (size_t i = 0; i < n; i++) {
        if (n == 36 && (i == 8 || i == 13 || i == 18 || i == 23)) {
            if (s[i] != '-') return -1;
            continue;
        }
        int v = hexv(s[i]);
        if (v < 0) return -1;
        if (pos % 2 == 0) buf[pos / 2] = (uint8_t)(v << 4); else buf[pos / 2] |= (uint8_t)v;
        pos++;
    }
    if (pos != 32) return -1;
    memcpy(out, buf, 16);
    return 0;
}
int sg_ipv4(const char *s, size_t n, uint8_t out[4]) {
    char b[16];
    uint8_t t[4];
    if (!strip(&s, &n) || n > 15) return -1;
    memcpy(b, s, n); b[n] = '\0';
    if (inet_pton(AF_INET, b, t) != 1) return -1;
    memcpy(out, t, 4);
    return 0;
}
int sg_ipv6(const char *s, size_t n, uint8_t out[16]) {
    char b[46];
    uint8_t t[16];
    if (!strip(&s, &n) || n > 45) return -1;
    memcpy(b, s, n); b[n] = '\0';
    if (inet_pton(AF_INET6, b, t) != 1) return -1;
    memcpy(out, t, 16);
    return 0;
}
