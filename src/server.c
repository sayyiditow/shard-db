#include "types.h"

/* Forward decls for monitoring counters (defined lower in this file). */
extern volatile int active_threads;
extern volatile int in_flight_writes;

/* Commands that mutate data (insert/delete/update/bulk/add-index/put-file/sequence).
   Take per-object rdlock during dispatch so rebuild (wrlock) blocks them briefly. */
static int mode_is_write(const char *m) {
    if (!m) return 0;
    return strcasecmp(m, "insert") == 0 || strcasecmp(m, "update") == 0 ||
           strcasecmp(m, "delete") == 0 || strcasecmp(m, "bulk-insert") == 0 ||
           strcasecmp(m, "bulk-insert-delimited") == 0 || strcasecmp(m, "bulk-delete") == 0 ||
           strcasecmp(m, "bulk-update") == 0 || strcasecmp(m, "bulk-update-delimited") == 0 ||
           strcasecmp(m, "add-index") == 0 || strcasecmp(m, "remove-index") == 0 ||
           strcasecmp(m, "put-file") == 0 ||
           strcasecmp(m, "delete-file") == 0 ||
           strcasecmp(m, "sequence") == 0;
}
/* Schema/rebuild commands — take exclusive wrlock. */
static int mode_is_schema(const char *m) {
    if (!m) return 0;
    return strcasecmp(m, "rename-field") == 0 || strcasecmp(m, "remove-field") == 0 ||
           strcasecmp(m, "add-field") == 0 || strcasecmp(m, "vacuum") == 0 ||
           strcasecmp(m, "truncate") == 0;
}

/* ========== Auth: IP allowlist + token set ========== */

static uint32_t str_hash(const char *s) {
    uint32_t h = 5381;
    while (*s) h = h * 33 + (unsigned char)*s++;
    return h;
}

/* --- IP allowlist: hash set, loaded from $DB_ROOT/allowed_ips.conf --- */
#define IP_SET_BUCKETS 128
static char g_ip_set[IP_SET_BUCKETS][46];
static int g_ip_set_used[IP_SET_BUCKETS];
int g_ip_set_count = 0;
static pthread_mutex_t g_ip_lock = PTHREAD_MUTEX_INITIALIZER;

static void ip_set_add(const char *ip) {
    uint32_t idx = str_hash(ip) % IP_SET_BUCKETS;
    for (int i = 0; i < IP_SET_BUCKETS; i++) {
        int slot = (idx + i) % IP_SET_BUCKETS;
        if (!g_ip_set_used[slot]) {
            strncpy(g_ip_set[slot], ip, 45);
            g_ip_set[slot][45] = '\0';
            g_ip_set_used[slot] = 1;
            g_ip_set_count++;
            return;
        }
        if (strcmp(g_ip_set[slot], ip) == 0) return;
    }
}

static int ip_set_remove(const char *ip) {
    uint32_t idx = str_hash(ip) % IP_SET_BUCKETS;
    for (int i = 0; i < IP_SET_BUCKETS; i++) {
        int slot = (idx + i) % IP_SET_BUCKETS;
        if (!g_ip_set_used[slot]) return 0;
        if (strcmp(g_ip_set[slot], ip) == 0) {
            g_ip_set_used[slot] = 0;
            g_ip_set[slot][0] = '\0';
            g_ip_set_count--;
            return 1;
        }
    }
    return 0;
}

void load_allowed_ips_conf(const char *db_root) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/allowed_ips.conf", db_root);
    FILE *f = fopen(path, "r");
    if (!f) return;
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n\r")] = '\0';
        char *p = line; while (*p == ' ') p++;
        if (*p && *p != '#') ip_set_add(p);
    }
    fclose(f);
}

static void save_allowed_ips_conf(const char *db_root) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/allowed_ips.conf", db_root);
    FILE *f = fopen(path, "w");
    if (!f) return;
    for (int i = 0; i < IP_SET_BUCKETS; i++) {
        if (g_ip_set_used[i]) fprintf(f, "%s\n", g_ip_set[i]);
    }
    fclose(f);
}

int is_ip_trusted(const char *ip) {
    /* Loopback is implicitly trusted by default (shard-db typically sits behind
       a localhost-connecting proxy that does TLS + auth upstream). Strict
       deployments set DISABLE_LOCALHOST_TRUST=1 in db.env to require an
       explicit token even for same-host callers — useful for testing the auth
       path and for production setups that don't front the DB with a proxy. */
    if (!g_disable_localhost_trust &&
        (strcmp(ip, "127.0.0.1") == 0 || strcmp(ip, "::1") == 0)) return 1;
    if (g_ip_set_count == 0) return 0;
    uint32_t idx = str_hash(ip) % IP_SET_BUCKETS;
    for (int i = 0; i < IP_SET_BUCKETS; i++) {
        int slot = (idx + i) % IP_SET_BUCKETS;
        if (!g_ip_set_used[slot]) return 0;
        if (strcmp(g_ip_set[slot], ip) == 0) return 1;
    }
    return 0;
}

/* --- Token store: hash set, three scope tiers + r/rw/rwx permissions.
   Files:
     $DB_ROOT/tokens.conf                 — global admin tokens
     $DB_ROOT/<dir>/tokens.conf           — tenant-scoped tokens for <dir>
     $DB_ROOT/<dir>/<obj>/tokens.conf     — object-scoped tokens for <dir>/<obj>
   Line format: `token[:perm]` where perm ∈ {r, rw, rwx}. Empty suffix or no
   colon = rwx (admin) — preserves backward compat with pre-perm tokens.conf
   files from before 2026.05.
   The hash is keyed on the token string. Each slot carries its (dir, obj,
   perm) — on lookup we match the request's (dir, obj) against the slot's
   scope after the string compare. Five parallel heap arrays sized at startup
   from g_token_cap. --- */
static char   (*g_token_set)[256]       = NULL;
static char   (*g_token_scope)[256]     = NULL;   /* "" = global, else dir */
static char   (*g_token_scope_obj)[256] = NULL;   /* "" = dir-or-global, else object */
static uint8_t *g_token_perm            = NULL;   /* PERM_R / PERM_RW / PERM_RWX */
static int     *g_token_set_used        = NULL;
int g_token_count = 0;
static pthread_mutex_t g_token_lock = PTHREAD_MUTEX_INITIALIZER;

static void token_store_init(void) {
    if (g_token_set) return;
    g_token_set       = calloc(g_token_cap, sizeof(*g_token_set));
    g_token_scope     = calloc(g_token_cap, sizeof(*g_token_scope));
    g_token_scope_obj = calloc(g_token_cap, sizeof(*g_token_scope_obj));
    g_token_perm      = calloc(g_token_cap, sizeof(*g_token_perm));
    g_token_set_used  = calloc(g_token_cap, sizeof(*g_token_set_used));
    if (!g_token_set || !g_token_scope || !g_token_scope_obj ||
        !g_token_perm || !g_token_set_used) {
        fprintf(stderr, "token_store_init: out of memory (TOKEN_CAP=%d)\n", g_token_cap);
        exit(1);
    }
}

static void token_set_add_full(const char *token,
                               const char *dir_scope,
                               const char *obj_scope,
                               uint8_t perm) {
    uint32_t idx = str_hash(token) % g_token_cap;
    const char *ds = dir_scope ? dir_scope : "";
    const char *os = obj_scope ? obj_scope : "";
    for (int i = 0; i < g_token_cap; i++) {
        int slot = (idx + i) % g_token_cap;
        if (!g_token_set_used[slot]) {
            strncpy(g_token_set[slot], token, 255);       g_token_set[slot][255] = '\0';
            strncpy(g_token_scope[slot], ds, 255);        g_token_scope[slot][255] = '\0';
            strncpy(g_token_scope_obj[slot], os, 255);    g_token_scope_obj[slot][255] = '\0';
            g_token_perm[slot] = perm;
            __atomic_store_n(&g_token_set_used[slot], 1, __ATOMIC_RELEASE);
            g_token_count++;
            return;
        }
        if (strcmp(g_token_set[slot], token) == 0) {
            /* Token already present. Duplicate tokens must be unique across
               scopes; later writes silently overwrite scope+perm. In practice
               tokens are 32 random bytes so duplicates don't happen except on
               explicit add-token of an existing value, which is caller error. */
            strncpy(g_token_scope[slot], ds, 255);        g_token_scope[slot][255] = '\0';
            strncpy(g_token_scope_obj[slot], os, 255);    g_token_scope_obj[slot][255] = '\0';
            g_token_perm[slot] = perm;
            return;
        }
    }
}

/* Legacy convenience: plain-string add always means global admin (rwx). */
static void token_set_add(const char *token) {
    token_set_add_full(token, NULL, NULL, PERM_RWX);
}
static void token_set_add_scoped(const char *token, const char *dir_scope) {
    token_set_add_full(token, dir_scope, NULL, PERM_RWX);
}

static int token_set_remove(const char *token) {
    uint32_t idx = str_hash(token) % g_token_cap;
    for (int i = 0; i < g_token_cap; i++) {
        int slot = (idx + i) % g_token_cap;
        if (!g_token_set_used[slot]) return 0;
        if (strcmp(g_token_set[slot], token) == 0) {
            g_token_set[slot][0] = '\0';
            g_token_scope[slot][0] = '\0';
            g_token_scope_obj[slot][0] = '\0';
            g_token_perm[slot] = 0;
            __atomic_store_n(&g_token_set_used[slot], 0, __ATOMIC_RELEASE);
            g_token_count--;
            return 1;
        }
    }
    return 0;
}

/* Parse a line of the form "<token>[:r|rw|rwx]" into (token_out, perm_out).
   No suffix (or :rwx) => PERM_RWX. Unknown suffix => returns 0 (reject). */
static int parse_token_line(const char *line, char *token_out, size_t tok_sz,
                            uint8_t *perm_out) {
    const char *colon = strchr(line, ':');
    if (!colon) {
        size_t len = strlen(line);
        if (len >= tok_sz) return 0;
        memcpy(token_out, line, len); token_out[len] = '\0';
        *perm_out = PERM_RWX;
        return 1;
    }
    size_t tok_len = colon - line;
    if (tok_len == 0 || tok_len >= tok_sz) return 0;
    memcpy(token_out, line, tok_len); token_out[tok_len] = '\0';
    const char *p = colon + 1;
    if (p[0] == '\0' || strcmp(p, "rwx") == 0) { *perm_out = PERM_RWX; return 1; }
    if (strcmp(p, "r") == 0)  { *perm_out = PERM_R;  return 1; }
    if (strcmp(p, "rw") == 0) { *perm_out = PERM_RW; return 1; }
    return 0;  /* bad perm suffix */
}

static int token_compare(const char *a, const char *b) {
    size_t la = strlen(a), lb = strlen(b);
    volatile int diff = la ^ lb;
    size_t n = la < lb ? la : lb;
    for (size_t i = 0; i < n; i++) diff |= a[i] ^ b[i];
    return diff == 0;
}

/* Look up a token. Returns slot index on match, -1 on miss. */
static int token_find_slot(const char *token) {
    if (!token || !token[0] || !g_token_set) return -1;
    uint32_t idx = str_hash(token) % g_token_cap;
    for (int i = 0; i < g_token_cap; i++) {
        int slot = (idx + i) % g_token_cap;
        uint32_t used = __atomic_load_n(&g_token_set_used[slot], __ATOMIC_ACQUIRE);
        if (!used) return -1;
        if (token_compare(g_token_set[slot], token)) return slot;
    }
    return -1;
}

/* Legacy wrapper: "is this a global admin token?" Used by the auth-management
   early gate (add-token / remove-token / list-tokens / add-ip / etc.) which
   is strictly server-admin regardless of the token being managed. */
int is_token_valid(const char *token) {
    int slot = token_find_slot(token);
    if (slot < 0) return 0;
    return g_token_scope[slot][0] == '\0' &&
           g_token_scope_obj[slot][0] == '\0' &&
           g_token_perm[slot] == PERM_RWX;
}

/* Admin level a mode requires.
     ADMIN_NONE   — data operation; permission check is read-vs-write only.
     ADMIN_OBJECT — narrow admin on one object (add-field, vacuum, add-index).
     ADMIN_TENANT — admin that targets a whole dir (create-object).
     ADMIN_SERVER — admin that touches server state (stats, auth management). */
typedef enum {
    ADMIN_NONE   = 0,
    ADMIN_OBJECT = 1,
    ADMIN_TENANT = 2,
    ADMIN_SERVER = 3
} AdminLevel;

static AdminLevel mode_admin_level(const char *mode) {
    if (!mode) return ADMIN_NONE;
    static const char *srv[] = {
        "stats", "db-dirs", "vacuum-check", "shard-stats",
        "add-token", "remove-token", "list-tokens",
        "add-ip", "remove-ip", "list-ips",
        "reindex",
        NULL
    };
    for (int i = 0; srv[i]; i++)
        if (strcmp(mode, srv[i]) == 0) return ADMIN_SERVER;
    if (strcmp(mode, "create-object") == 0) return ADMIN_TENANT;
    static const char *obj[] = {
        "truncate", "vacuum", "backup", "recount",
        "add-field", "remove-field", "rename-field",
        "add-index", "remove-index",
        "drop-object",
        NULL
    };
    for (int i = 0; obj[i]; i++)
        if (strcmp(mode, obj[i]) == 0) return ADMIN_OBJECT;
    return ADMIN_NONE;
}

/* Data-write modes (not admin). Reads are everything else that isn't admin. */
static int mode_is_data_write(const char *mode) {
    if (!mode) return 0;
    static const char *w[] = {
        "insert", "update", "delete",
        "bulk-insert", "bulk-insert-delimited", "bulk-delete", "bulk-update", "bulk-update-delimited",
        "put-file", "delete-file", "sequence",
        NULL
    };
    for (int i = 0; w[i]; i++)
        if (strcmp(mode, w[i]) == 0) return 1;
    return 0;
}

/* Full authorization check.
   Returns 1 if this token is allowed to run `mode` against (req_dir, req_obj).
   Returns 0 otherwise — caller emits {"error":"auth failed"}.
   Admin-management modes (add-token etc.) bypass this and go through the
   early gate using is_token_valid(). */
int is_authorized(const char *token, const char *req_dir, const char *req_obj,
                  const char *mode) {
    int slot = token_find_slot(token);
    if (slot < 0) return 0;

    const char *tok_dir = g_token_scope[slot];
    const char *tok_obj = g_token_scope_obj[slot];
    uint8_t perm = g_token_perm[slot];

    int scope_is_global = (tok_dir[0] == '\0');
    int scope_is_tenant = (!scope_is_global && tok_obj[0] == '\0');
    int scope_is_object = (!scope_is_global && tok_obj[0] != '\0');

    /* Scope match against the request's (dir, object). */
    if (scope_is_tenant) {
        if (!req_dir || strcmp(req_dir, tok_dir) != 0) return 0;
    } else if (scope_is_object) {
        if (!req_dir || !req_obj) return 0;
        if (strcmp(req_dir, tok_dir) != 0) return 0;
        if (strcmp(req_obj, tok_obj) != 0) return 0;
    }
    /* Global scope: matches any (dir, object). */

    /* Permission check against the mode. */
    AdminLevel req = mode_admin_level(mode);
    switch (req) {
    case ADMIN_NONE:
        if (mode_is_data_write(mode)) return perm >= PERM_RW;
        return perm >= PERM_R;
    case ADMIN_OBJECT:
        /* Any rwx token whose scope covers this object can run it. */
        return perm == PERM_RWX;
    case ADMIN_TENANT:
        /* rwx at tenant or global scope; object-scoped rwx is too narrow. */
        return perm == PERM_RWX && !scope_is_object;
    case ADMIN_SERVER:
        /* rwx at global scope only. */
        return perm == PERM_RWX && scope_is_global;
    }
    return 0;
}

/* Read one tokens.conf file into the token store with the given scope. */
static void load_one_tokens_file(const char *path, const char *dir_scope,
                                 const char *obj_scope) {
    FILE *f = fopen(path, "r");
    if (!f) return;
    char line[512];
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n\r")] = '\0';
        char *p = line; while (*p == ' ' || *p == '\t') p++;
        if (!*p || *p == '#') continue;
        char tokbuf[256]; uint8_t perm;
        if (parse_token_line(p, tokbuf, sizeof(tokbuf), &perm)) {
            token_set_add_full(tokbuf, dir_scope, obj_scope, perm);
        } else {
            fprintf(stderr, "load_tokens_conf: skipping malformed line '%s' in %s\n", p, path);
        }
    }
    fclose(f);
}

void load_tokens_conf(const char *db_root) {
    token_store_init();

    /* Global tokens. */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/tokens.conf", db_root);
    load_one_tokens_file(path, NULL, NULL);

    /* Per-tenant tokens. */
    char dirs_path[PATH_MAX];
    snprintf(dirs_path, sizeof(dirs_path), "%s/dirs.conf", db_root);
    FILE *df = fopen(dirs_path, "r");
    if (df) {
        char dirline[256];
        while (fgets(dirline, sizeof(dirline), df)) {
            dirline[strcspn(dirline, "\n\r")] = '\0';
            char *dp = dirline; while (*dp == ' ') dp++;
            if (!*dp || *dp == '#') continue;
            char tpath[PATH_MAX];
            snprintf(tpath, sizeof(tpath), "%s/%s/tokens.conf", db_root, dp);
            load_one_tokens_file(tpath, dp, NULL);
        }
        fclose(df);
    }

    /* Per-object tokens: schema.conf lists `dir:object:splits:...`. */
    char schema_path[PATH_MAX];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
    FILE *sf = fopen(schema_path, "r");
    if (sf) {
        char sline[512];
        while (fgets(sline, sizeof(sline), sf)) {
            sline[strcspn(sline, "\n\r")] = '\0';
            char *p = sline; while (*p == ' ') p++;
            if (!*p || *p == '#') continue;
            char *c1 = strchr(p, ':'); if (!c1) continue;
            char *c2 = strchr(c1 + 1, ':'); if (!c2) continue;
            size_t dlen = c1 - p;
            size_t olen = c2 - c1 - 1;
            char sdir[256], sobj[256];
            if (dlen == 0 || dlen >= sizeof(sdir)) continue;
            if (olen == 0 || olen >= sizeof(sobj)) continue;
            memcpy(sdir, p, dlen); sdir[dlen] = '\0';
            memcpy(sobj, c1 + 1, olen); sobj[olen] = '\0';
            char tpath[PATH_MAX];
            snprintf(tpath, sizeof(tpath), "%s/%s/%s/tokens.conf", db_root, sdir, sobj);
            load_one_tokens_file(tpath, sdir, sobj);
        }
        fclose(sf);
    }
}

/* Emit one token line to `f`: plain token for rwx (backward-compat), or
   `token:r` / `token:rw` for narrower perms. */
static void save_token_line(FILE *f, int slot) {
    if (g_token_perm[slot] == PERM_RWX) {
        fprintf(f, "%s\n", g_token_set[slot]);
    } else {
        const char *p = (g_token_perm[slot] == PERM_R) ? "r" : "rw";
        fprintf(f, "%s:%s\n", g_token_set[slot], p);
    }
}

/* Rewrite the tokens.conf file for a specific scope (global, tenant, or
   object). Writes only tokens whose (dir, obj) matches the requested scope. */
static void save_tokens_conf_full(const char *db_root,
                                  const char *dir_scope,
                                  const char *obj_scope) {
    char path[PATH_MAX];
    if (!dir_scope || !dir_scope[0])
        snprintf(path, sizeof(path), "%s/tokens.conf", db_root);
    else if (!obj_scope || !obj_scope[0])
        snprintf(path, sizeof(path), "%s/%s/tokens.conf", db_root, dir_scope);
    else
        snprintf(path, sizeof(path), "%s/%s/%s/tokens.conf",
                 db_root, dir_scope, obj_scope);
    FILE *f = fopen(path, "w");
    if (!f) return;
    const char *wd = dir_scope ? dir_scope : "";
    const char *wo = obj_scope ? obj_scope : "";
    for (int i = 0; i < g_token_cap; i++) {
        if (!g_token_set_used[i]) continue;
        if (strcmp(g_token_scope[i], wd) != 0) continue;
        if (strcmp(g_token_scope_obj[i], wo) != 0) continue;
        save_token_line(f, i);
    }
    fclose(f);
}

/* Legacy wrappers used elsewhere in this file. */
static void save_tokens_conf_scoped(const char *db_root, const char *dir_scope) {
    save_tokens_conf_full(db_root, dir_scope, NULL);
}
static void save_tokens_conf(const char *db_root) {
    save_tokens_conf_full(db_root, NULL, NULL);
}

/* ========== JSON QUERY DISPATCH ========== */

/* Dispatch a JSON query object: {"mode":"get","object":"users","key":"k1",...} */
void dispatch_json_query(const char *raw_db_root, const char *json, const char *client_ip) {
    /* Parse the request JSON top-level fields in a single pass. Every
       json_obj_strdup() below is an O(n) lookup over this ~10-20 entry
       array instead of an O(|json|) walk from the beginning — the whole
       request is parsed exactly once regardless of how many fields
       subsequent code extracts. The JsonObj holds spans into `json`,
       which lives for the full dispatch. */
    JsonObj req;
    json_parse_object(json, strlen(json), &req);

    char *mode = json_obj_strdup(&req, "mode");

    /* Per-request statement timeout. Set unconditionally at the start of every
       dispatch so the next request on this worker thread never inherits a
       stale value. "timeout_ms":0 or absent → fall back to global g_timeout
       at QueryDeadline creation time (resolve_timeout_ms). */
    g_request_timeout_ms = (uint32_t)json_obj_int(&req, "timeout_ms", 0);

    /* Auth management modes — require trusted IP or valid token */
    if (mode && (strcmp(mode, "add-token") == 0 || strcmp(mode, "remove-token") == 0 ||
                 strcmp(mode, "add-ip") == 0 || strcmp(mode, "remove-ip") == 0 ||
                 strcmp(mode, "list-tokens") == 0 || strcmp(mode, "list-ips") == 0)) {
        int authorized = is_ip_trusted(client_ip);
        if (!authorized) {
            char *auth = json_obj_strdup(&req, "auth");
            authorized = auth && is_token_valid(auth);
            free(auth);
        }
        if (!authorized) {
            OUT("{\"error\":\"unauthorized\"}\n");
            free(mode); return;
        }

        if (strcmp(mode, "add-token") == 0) {
            char *token = json_obj_strdup(&req, "token");
            char *dir_scope = json_obj_strdup(&req, "dir");     /* optional */
            char *obj_scope = json_obj_strdup(&req, "object");  /* optional, needs dir */
            char *perm_str  = json_obj_strdup(&req, "perm");    /* r | rw | rwx (default rw) */

            /* Validate dir if present */
            if (dir_scope && dir_scope[0] && !is_valid_dir(dir_scope)) {
                OUT("{\"error\":\"Unknown dir: %s\"}\n", dir_scope);
                free(token); free(dir_scope); free(obj_scope); free(perm_str); free(mode);
                return;
            }
            /* Object scope requires dir scope */
            if (obj_scope && obj_scope[0] && !(dir_scope && dir_scope[0])) {
                OUT("{\"error\":\"object scope requires dir\"}\n");
                free(token); free(dir_scope); free(obj_scope); free(perm_str); free(mode);
                return;
            }
            /* Validate object exists under dir (schema.conf check is fs stat for
               <dir>/<object>/fields.conf — reuses existing invariant). */
            if (obj_scope && obj_scope[0]) {
                char ocheck[PATH_MAX];
                snprintf(ocheck, sizeof(ocheck), "%s/%s/%s/fields.conf",
                         raw_db_root, dir_scope, obj_scope);
                struct stat ost;
                if (stat(ocheck, &ost) != 0) {
                    OUT("{\"error\":\"object not found: %s/%s\"}\n", dir_scope, obj_scope);
                    free(token); free(dir_scope); free(obj_scope); free(perm_str); free(mode);
                    return;
                }
            }
            /* Parse perm: default is rw (least privilege for new tokens;
               admins explicitly opt into rwx). Empty or missing => rw. */
            uint8_t perm = PERM_RW;
            if (perm_str && perm_str[0]) {
                if (strcmp(perm_str, "r") == 0)        perm = PERM_R;
                else if (strcmp(perm_str, "rw") == 0)  perm = PERM_RW;
                else if (strcmp(perm_str, "rwx") == 0) perm = PERM_RWX;
                else {
                    OUT("{\"error\":\"invalid perm: must be r, rw, or rwx\"}\n");
                    free(token); free(dir_scope); free(obj_scope); free(perm_str); free(mode);
                    return;
                }
            }
            if (token && token[0]) {
                const char *ds = (dir_scope && dir_scope[0]) ? dir_scope : NULL;
                const char *os = (obj_scope && obj_scope[0]) ? obj_scope : NULL;
                pthread_mutex_lock(&g_token_lock);
                token_set_add_full(token, ds, os, perm);
                save_tokens_conf_full(raw_db_root, ds, os);
                pthread_mutex_unlock(&g_token_lock);
                const char *pstr = (perm == PERM_R) ? "r" : (perm == PERM_RW) ? "rw" : "rwx";
                log_msg(3, "AUTH add-token scope=%s/%s perm=%s from %s",
                        ds ? ds : "global", os ? os : "", pstr, client_ip);
                OUT("{\"status\":\"token_added\",\"scope\":\"%s%s%s\",\"perm\":\"%s\"}\n",
                    ds ? ds : "global",
                    os ? "/" : "",
                    os ? os : "",
                    pstr);
            } else {
                OUT("{\"error\":\"Missing token\"}\n");
            }
            free(token); free(dir_scope); free(obj_scope); free(perm_str);
        } else if (strcmp(mode, "remove-token") == 0) {
            char *token = json_obj_strdup(&req, "token");
            if (token && token[0]) {
                pthread_mutex_lock(&g_token_lock);
                /* Remember scope (dir + obj) before removal so we know which
                   file to rewrite. */
                char rm_dir[256] = "", rm_obj[256] = "";
                int found = 0;
                uint32_t idx = str_hash(token) % g_token_cap;
                for (int i = 0; i < g_token_cap; i++) {
                    int slot = (idx + i) % g_token_cap;
                    if (!g_token_set_used[slot]) break;
                    if (strcmp(g_token_set[slot], token) == 0) {
                        strncpy(rm_dir, g_token_scope[slot],     sizeof(rm_dir) - 1);
                        strncpy(rm_obj, g_token_scope_obj[slot], sizeof(rm_obj) - 1);
                        found = 1;
                        break;
                    }
                }
                int removed = token_set_remove(token);
                if (removed && found)
                    save_tokens_conf_full(raw_db_root,
                                          rm_dir[0] ? rm_dir : NULL,
                                          rm_obj[0] ? rm_obj : NULL);
                pthread_mutex_unlock(&g_token_lock);
                OUT("{\"status\":\"%s\"}\n", removed ? "token_removed" : "token_not_found");
            } else {
                OUT("{\"error\":\"Missing token\"}\n");
            }
            free(token);
        } else if (strcmp(mode, "add-ip") == 0) {
            char *ip = json_obj_strdup(&req, "ip");
            if (ip && ip[0]) {
                pthread_mutex_lock(&g_ip_lock);
                ip_set_add(ip);
                save_allowed_ips_conf(raw_db_root);
                pthread_mutex_unlock(&g_ip_lock);
                log_msg(3, "AUTH add-ip %s from %s", ip, client_ip);
                OUT("{\"status\":\"ip_added\"}\n");
            } else {
                OUT("{\"error\":\"Missing ip\"}\n");
            }
            free(ip);
        } else if (strcmp(mode, "remove-ip") == 0) {
            char *ip = json_obj_strdup(&req, "ip");
            if (ip && ip[0]) {
                pthread_mutex_lock(&g_ip_lock);
                int removed = ip_set_remove(ip);
                if (removed) save_allowed_ips_conf(raw_db_root);
                pthread_mutex_unlock(&g_ip_lock);
                OUT("{\"status\":\"%s\"}\n", removed ? "ip_removed" : "ip_not_found");
            } else {
                OUT("{\"error\":\"Missing ip\"}\n");
            }
            free(ip);
        } else if (strcmp(mode, "list-tokens") == 0) {
            /* Emit {"token":"fp","scope":"global"|"<dir>"|"<dir>/<obj>","perm":"r|rw|rwx"} */
            OUT("[");
            int printed = 0;
            for (int i = 0; i < g_token_cap; i++) {
                if (!g_token_set_used[i]) continue;
                int tlen = strlen(g_token_set[i]);
                char fp[32];
                if (tlen > 10)
                    snprintf(fp, sizeof(fp), "%.4s...%s",
                             g_token_set[i], g_token_set[i] + tlen - 4);
                else
                    snprintf(fp, sizeof(fp), "****");
                char scope_buf[520];
                if (g_token_scope[i][0] == '\0')
                    snprintf(scope_buf, sizeof(scope_buf), "global");
                else if (g_token_scope_obj[i][0] == '\0')
                    snprintf(scope_buf, sizeof(scope_buf), "%s", g_token_scope[i]);
                else
                    snprintf(scope_buf, sizeof(scope_buf), "%s/%s",
                             g_token_scope[i], g_token_scope_obj[i]);
                const char *pstr = (g_token_perm[i] == PERM_R) ? "r"
                                 : (g_token_perm[i] == PERM_RW) ? "rw" : "rwx";
                OUT("%s{\"token\":\"%s\",\"scope\":\"%s\",\"perm\":\"%s\"}",
                    printed ? "," : "", fp, scope_buf, pstr);
                printed++;
            }
            OUT("]\n");
        } else if (strcmp(mode, "list-ips") == 0) {
            OUT("[");
            int printed = 0;
            for (int i = 0; i < IP_SET_BUCKETS; i++) {
                if (!g_ip_set_used[i]) continue;
                OUT("%s\"%s\"", printed ? "," : "", g_ip_set[i]);
                printed++;
            }
            OUT("]\n");
        }
        free(mode);
        return;
    }

    /* Auth check — order of precedence:
         1. Trusted IP → allow anything (bypass token check).
         2. Global/admin token → allow anything.
         3. Tenant token whose scope matches the request's `dir` → allow data
            commands for that dir only; admin commands rejected.
         4. Otherwise reject. */
    if (!is_ip_trusted(client_ip)) {
        char *auth = json_obj_strdup(&req, "auth");
        char *req_dir = json_obj_strdup(&req, "dir");
        char *req_obj = json_obj_strdup(&req, "object");
        int ok = auth && is_authorized(auth, req_dir, req_obj, mode);
        free(auth); free(req_dir); free(req_obj);
        if (!ok) {
            free(mode);
            OUT("{\"error\":\"auth failed\"}\n");
            return;
        }
    }

    /* db-dirs doesn't need dir or object */
    if (mode && strcmp(mode, "db-dirs") == 0) {
        OUT("[");
        int printed = 0;
        for (int i = 0; i < DIRS_BUCKETS; i++) {
            if (!g_dirs_used[i]) continue;
            OUT("%s\"%s\"", printed ? "," : "", g_dirs[i]);
            printed++;
        }
        OUT("]\n");
        free(mode);
        return;
    }

    /* stats — monitoring snapshot, no dir/object needed */
    if (mode && strcmp(mode, "stats") == 0) {
        char *fmt = json_obj_strdup(&req, "format");
        int as_table = (fmt && strcmp(fmt, "table") == 0);
        free(fmt);

        int uc_used = 0, uc_total = 0; size_t uc_bytes = 0;
        int bc_used = 0, bc_total = 0; size_t bc_bytes = 0;
        ucache_stats(&uc_used, &uc_total, &uc_bytes);
        bt_cache_stats(&bc_used, &bc_total, &bc_bytes);
        uint64_t u_hits   = __atomic_load_n(&g_ucache_hits,    __ATOMIC_RELAXED);
        uint64_t u_miss   = __atomic_load_n(&g_ucache_misses,  __ATOMIC_RELAXED);
        uint64_t b_hits   = __atomic_load_n(&g_bt_cache_hits,  __ATOMIC_RELAXED);
        uint64_t b_miss   = __atomic_load_n(&g_bt_cache_misses,__ATOMIC_RELAXED);
        uint64_t slow_n   = __atomic_load_n(&g_slow_query_count,__ATOMIC_RELAXED);
        uint64_t uptime   = now_ms() - g_server_start_ms;
        /* Subtract 1 for this stats request itself (occupies one worker thread). */
        int at = active_threads > 0 ? active_threads - 1 : 0;

        if (as_table) {
            double u_hit_pct = (u_hits + u_miss) ? 100.0 * u_hits / (u_hits + u_miss) : 0.0;
            double b_hit_pct = (b_hits + b_miss) ? 100.0 * b_hits / (b_hits + b_miss) : 0.0;
            OUT("uptime          %.1fs\n", uptime / 1000.0);
            OUT("active_threads  %d\n", at);
            OUT("in_flight_wr    %d\n", in_flight_writes);
            OUT("ucache          used=%d/%d bytes=%zu hit=%.1f%% (%lu/%lu)\n",
                uc_used, uc_total, uc_bytes, u_hit_pct, u_hits, u_miss);
            OUT("bt_cache        used=%d/%d bytes=%zu hit=%.1f%% (%lu/%lu)\n",
                bc_used, bc_total, bc_bytes, b_hit_pct, b_hits, b_miss);
            OUT("slow_query      count=%lu threshold=%dms\n", slow_n, g_slow_query_ms);
            pthread_mutex_lock(&g_slow_query_lock);
            int printed = 0;
            for (int i = 0; i < SLOW_QUERY_RING && printed < 5; i++) {
                int idx = (g_slow_query_head - 1 - i + SLOW_QUERY_RING) % SLOW_QUERY_RING;
                SlowQueryEntry *e = &g_slow_queries[idx];
                if (e->duration_ms == 0 && e->ts_ms == 0) break;
                OUT("  %-20s %ums  %s/%s\n", e->mode, e->duration_ms, e->dir, e->object);
                printed++;
            }
            pthread_mutex_unlock(&g_slow_query_lock);
        } else {
            OUT("{\"uptime_ms\":%lu,\"active_threads\":%d,\"in_flight_writes\":%d,"
                "\"ucache\":{\"used\":%d,\"total\":%d,\"bytes\":%zu,\"hits\":%lu,\"misses\":%lu},"
                "\"bt_cache\":{\"used\":%d,\"total\":%d,\"bytes\":%zu,\"hits\":%lu,\"misses\":%lu},"
                "\"slow_query\":{\"threshold_ms\":%d,\"count\":%lu,\"recent\":[",
                uptime, at, in_flight_writes,
                uc_used, uc_total, uc_bytes, u_hits, u_miss,
                bc_used, bc_total, bc_bytes, b_hits, b_miss,
                g_slow_query_ms, slow_n);
            pthread_mutex_lock(&g_slow_query_lock);
            int printed = 0;
            for (int i = 0; i < SLOW_QUERY_RING; i++) {
                int idx = (g_slow_query_head - 1 - i + SLOW_QUERY_RING) % SLOW_QUERY_RING;
                SlowQueryEntry *e = &g_slow_queries[idx];
                if (e->duration_ms == 0 && e->ts_ms == 0) break;
                OUT("%s{\"ts_ms\":%lu,\"duration_ms\":%u,\"mode\":\"%s\",\"dir\":\"%s\",\"object\":\"%s\"}",
                    printed ? "," : "", e->ts_ms, e->duration_ms, e->mode, e->dir, e->object);
                printed++;
            }
            pthread_mutex_unlock(&g_slow_query_lock);
            OUT("]}}\n");
        }
        free(mode);
        return;
    }

    /* vacuum-check scans all objects — no dir/object needed */
    if (mode && strcmp(mode, "vacuum-check") == 0) {
        OUT("[");
        int printed = 0;
        for (int di = 0; di < DIRS_BUCKETS; di++) {
            if (!g_dirs_used[di]) continue;
            char dir_path[PATH_MAX];
            snprintf(dir_path, sizeof(dir_path), "%s/%s", g_db_root, g_dirs[di]);
            DIR *dd = opendir(dir_path);
            if (!dd) continue;
            struct dirent *de;
            while ((de = readdir(dd))) {
                if (de->d_name[0] == '.') continue;
                char obj_check[PATH_MAX];
                snprintf(obj_check, sizeof(obj_check), "%s/%s/fields.conf", dir_path, de->d_name);
                struct stat ost;
                if (stat(obj_check, &ost) != 0) continue;
                /* Build effective root for this dir */
                char eff[PATH_MAX];
                snprintf(eff, sizeof(eff), "%s/%s", g_db_root, g_dirs[di]);
                /* Read counts (single file, single read) */
                int count = get_live_count(eff, de->d_name);
                int deleted = get_deleted_count(eff, de->d_name);
                /* Recommend vacuum: deleted >= 10% of (count+deleted) AND deleted >= 1000 */
                int total = count + deleted;
                int recommend = (deleted >= 1000 && total > 0 && deleted * 10 >= total);
                if (deleted > 0) {
                    OUT("%s{\"dir\":\"%s\",\"object\":\"%s\",\"count\":%d,\"orphaned\":%d,\"vacuum\":%s}",
                        printed ? "," : "", g_dirs[di], de->d_name, count, deleted,
                        recommend ? "true" : "false");
                    printed++;
                }
            }
            closedir(dd);
        }
        OUT("]\n");
        free(mode);
        return;
    }

    /* reindex — walk schema.conf, optionally filtered by dir/object, and
       rebuild every index. Server-admin scope regardless of filter because
       the no-filter form crosses tenants. */
    if (mode && strcmp(mode, "reindex") == 0) {
        char *dir_f = json_obj_strdup(&req, "dir");
        char *obj_f = json_obj_strdup(&req, "object");
        const char *df = (dir_f && dir_f[0]) ? dir_f : NULL;
        const char *of = (obj_f && obj_f[0]) ? obj_f : NULL;
        cmd_reindex(g_db_root, df, of);
        free(dir_f); free(obj_f); free(mode);
        return;
    }

    char *dir = json_obj_strdup(&req, "dir");
    char *object = json_obj_strdup(&req, "object");

    /* shard-stats: object (and dir) are optional. When omitted, walk every
       dir/object. format="table" switches from JSON to a human ASCII table. */
    if (mode && strcmp(mode, "shard-stats") == 0 && !object) {
        char *fmt = json_obj_strdup(&req, "format");
        int as_table = (fmt && strcmp(fmt, "table") == 0);
        free(fmt);
        if (as_table) OUT("%-24s %-24s\n", "DIR", "OBJECT");
        else OUT("[");
        int printed = 0;
        for (int di = 0; di < DIRS_BUCKETS; di++) {
            if (!g_dirs_used[di]) continue;
            if (dir && strcmp(dir, g_dirs[di]) != 0) continue;
            char dir_path[PATH_MAX];
            snprintf(dir_path, sizeof(dir_path), "%s/%s", g_db_root, g_dirs[di]);
            DIR *dd = opendir(dir_path);
            if (!dd) continue;
            struct dirent *de;
            while ((de = readdir(dd))) {
                if (de->d_name[0] == '.') continue;
                char obj_check[PATH_MAX];
                snprintf(obj_check, sizeof(obj_check), "%s/%s/fields.conf", dir_path, de->d_name);
                struct stat ost;
                if (stat(obj_check, &ost) != 0) continue;
                if (as_table) {
                    OUT("\n%-24s %-24s\n", g_dirs[di], de->d_name);
                    cmd_shard_stats(dir_path, de->d_name, 1);
                } else {
                    if (printed) OUT(",");
                    OUT("{\"dir\":\"%s\",\"object\":\"%s\",\"stats\":", g_dirs[di], de->d_name);
                    cmd_shard_stats(dir_path, de->d_name, 0);
                    OUT("}");
                }
                printed++;
            }
            closedir(dd);
        }
        if (!as_table) OUT("]\n");
        free(mode); free(dir); free(object);
        return;
    }

    if (!mode || !dir || !object) {
        OUT("{\"error\":\"Missing mode, dir, or object\"}\n");
        free(mode); free(dir); free(object);
        return;
    }

    /* create-object bypasses dir validation (dir may not exist yet) */
    if (strcmp(mode, "create-object") == 0) {
        char *fields_j = json_obj_strdup_raw(&req, "fields");
        char *indexes_j = json_obj_strdup_raw(&req, "indexes");
        char *splits_s = json_obj_strdup(&req, "splits");
        char *max_key_s = json_obj_strdup(&req, "max_key");
        char *ine_s = json_obj_strdup(&req, "if_not_exists");
        int if_not_exists = ine_s && (strcmp(ine_s, "true") == 0 || strcmp(ine_s, "1") == 0);
        cmd_create_object(g_db_root, dir, object,
                          fields_j, indexes_j,
                          splits_s ? atoi(splits_s) : 0,
                          max_key_s ? atoi(max_key_s) : 0,
                          if_not_exists);
        free(fields_j); free(indexes_j);
        free(splits_s); free(max_key_s); free(ine_s);
        free(mode); free(dir); free(object);
        return;
    }

    /* drop-object also bypasses the fields.conf pre-check below — the
       command itself handles the "not found" case (idempotent with
       if_exists:true, errors otherwise). */
    if (strcmp(mode, "drop-object") == 0) {
        char *ie_s = json_obj_strdup(&req, "if_exists");
        int if_exists = ie_s && (strcmp(ie_s, "true") == 0 || strcmp(ie_s, "1") == 0);
        cmd_drop_object(g_db_root, dir, object, if_exists);
        free(ie_s);
        free(mode); free(dir); free(object);
        return;
    }

    if (!is_valid_dir(dir)) {
        OUT("{\"error\":\"Unknown dir: %s\"}\n", dir);
        free(mode); free(dir); free(object);
        return;
    }

    /* Build effective root: $DB_ROOT/<dir> */
    char db_root[PATH_MAX];
    build_effective_root(db_root, sizeof(db_root), dir);

    /* Validate object exists (must be created via create-object first) */
    char obj_check[PATH_MAX];
    snprintf(obj_check, sizeof(obj_check), "%s/%s/fields.conf", db_root, object);
    struct stat obj_st;
    if (stat(obj_check, &obj_st) != 0) {
        OUT("{\"error\":\"Object [%s] not found. Use create-object first.\"}\n", object);
        free(mode); free(dir); free(object);
        return;
    }

    /* Per-object locking: wrlock for schema/rebuild, rdlock for writes, none for reads. */
    int took_wrlock = mode_is_schema(mode);
    int took_rdlock = !took_wrlock && mode_is_write(mode);
    if (took_wrlock) objlock_wrlock(db_root, object);
    else if (took_rdlock) objlock_rdlock(db_root, object);

    if (strcmp(mode, "get") == 0) {
        char *key = json_obj_strdup(&req, "key");
        char *keys = json_obj_strdup_raw(&req, "keys");
        char *fields = json_obj_string_or_array(&req, "fields");
        if (keys) {
            char *fmt = json_obj_strdup(&req, "format");
            char *delim = json_obj_strdup(&req, "delimiter");
            cmd_get_multi(db_root, object, keys, fmt, delim);
            free(keys); free(fmt); free(delim);
        } else if (key) {
            if (fields && fields[0]) {
                /* Get with projection — uses ucache */
                Schema sc = load_schema(db_root, object);
                uint8_t hash[16]; int shard_id, start_slot;
                size_t klen = strlen(key);
                compute_addr(key, klen, sc.splits, hash, &shard_id, &start_slot);
                char shard[PATH_MAX];
                build_shard_path(shard, sizeof(shard), db_root, object, shard_id);
                FcacheRead fc = fcache_get_read(shard);
                if (fc.map) {
                    uint32_t slots = fc.slots_per_shard;
                    uint32_t mask = slots - 1;
                    int found = 0;
                    for (uint32_t i = 0; i < slots; i++) {
                        uint32_t s = ((uint32_t)start_slot + i) & mask;
                        SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
                        if (h->flag == 0 && h->key_len == 0) break;
                        if (h->flag == 2) continue;
                        if (h->flag == 1 && memcmp(h->hash, hash, 16) == 0 &&
                            h->key_len == klen &&
                            memcmp(fc.map + zoneB_off(s, slots, sc.slot_size), key, klen) == 0) {
                            const char *raw = (const char *)(fc.map + zoneB_off(s, slots, sc.slot_size) + h->key_len);
                            FieldSchema pfs; init_field_schema(&pfs, db_root, object);
                            char proj_buf[MAX_LINE];
                            strncpy(proj_buf, fields, MAX_LINE - 1);
                            const char *flds[MAX_FIELDS];
                            int nf = 0;
                            char *_tok_save = NULL; char *tok = strtok_r(proj_buf, ",", &_tok_save);
                            while (tok && nf < MAX_FIELDS) { flds[nf++] = tok; tok = strtok_r(NULL, ",", &_tok_save); }
                            OUT("{\"key\":\"%s\",\"value\":{", key);
                            int first = 1;
                            for (int fi = 0; fi < nf; fi++) {
                                char *pv = decode_field(raw, h->value_len, flds[fi],
                                    (pfs.ts || pfs.nfields > 0) ? &pfs : NULL);
                                if (!pv) continue;
                                OUT("%s\"%s\":\"%s\"", first ? "" : ",", flds[fi], pv);
                                first = 0; free(pv);
                            }
                            OUT("}}\n");
                            found = 1; break;
                        }
                    }
                    if (!found) OUT("{\"error\":\"Not found\"}\n");
                    fcache_release(fc);
                } else OUT("{\"error\":\"Not found\"}\n");
            } else {
                cmd_get(db_root, object, key);
            }
        } else {
            OUT("{\"error\":\"Missing key or keys\"}\n");
        }
        free(key); free(fields);
    } else if (strcmp(mode, "insert") == 0) {
        char *key = json_obj_strdup(&req, "key");
        char *value = json_obj_strdup_raw(&req, "value");
        char *if_cond = json_obj_strdup_raw(&req, "if");
        char *ine_raw = json_obj_strdup(&req, "if_not_exists");
        int if_not_exists = (ine_raw && strcmp(ine_raw, "true") == 0);
        free(ine_raw);
        if (key && value)
            cmd_insert(db_root, object, key, value, if_cond, if_not_exists);
        else
            OUT("{\"error\":\"Missing key or value\"}\n");
        free(key); free(value); free(if_cond);
    } else if (strcmp(mode, "update") == 0) {
        char *key = json_obj_strdup(&req, "key");
        char *value = json_obj_strdup_raw(&req, "value");
        char *if_cond = json_obj_strdup_raw(&req, "if");
        char *dry_s = json_obj_strdup(&req, "dry_run");
        int dry = (dry_s && strcmp(dry_s, "true") == 0);
        if (key && value)
            cmd_update(db_root, object, key, value, if_cond, dry);
        else
            OUT("{\"error\":\"Missing key or value\"}\n");
        free(key); free(value); free(if_cond); free(dry_s);
    } else if (strcmp(mode, "delete") == 0) {
        char *key = json_obj_strdup(&req, "key");
        char *if_cond = json_obj_strdup_raw(&req, "if");
        char *dry_s = json_obj_strdup(&req, "dry_run");
        int dry = (dry_s && strcmp(dry_s, "true") == 0);
        if (key) cmd_delete(db_root, object, key, if_cond, dry);
        else OUT("{\"error\":\"Missing key\"}\n");
        free(key); free(if_cond); free(dry_s);
    } else if (strcmp(mode, "exists") == 0) {
        char *key = json_obj_strdup(&req, "key");
        char *keys_json = json_obj_strdup_raw(&req, "keys");
        if (keys_json) {
            char *fmt = json_obj_strdup(&req, "format");
            char *delim = json_obj_strdup(&req, "delimiter");
            cmd_exists_multi(db_root, object, keys_json, fmt, delim);
            free(keys_json); free(fmt); free(delim);
        } else if (key) {
            cmd_exists(db_root, object, key);
        } else {
            OUT("{\"error\":\"Missing key or keys\"}\n");
        }
        free(key);
    } else if (strcmp(mode, "not-exists") == 0) {
        char *keys_json = json_obj_strdup_raw(&req, "keys");
        if (keys_json) {
            cmd_not_exists(db_root, object, keys_json);
            free(keys_json);
        } else {
            OUT("{\"error\":\"Missing keys\"}\n");
        }
    } else if (strcmp(mode, "size") == 0) {
        cmd_size(db_root, object);
    } else if (strcmp(mode, "count") == 0) {
        char *criteria = json_obj_strdup_raw(&req, "criteria");
        cmd_count(db_root, object, criteria);
        free(criteria);
    } else if (strcmp(mode, "find") == 0) {
        char *criteria = json_obj_strdup_raw(&req, "criteria");
        char *off_s = json_obj_strdup(&req, "offset");
        char *lim_s = json_obj_strdup(&req, "limit");
        char *fields = json_obj_string_or_array(&req, "fields");
        char *excl = json_obj_string_or_array(&req, "excludedKeys");
        char *fmt = json_obj_strdup(&req, "format");
        char *delim = json_obj_strdup(&req, "delimiter");
        char *join = json_obj_strdup_raw(&req, "join");
        char *ob = json_obj_strdup(&req, "order_by");
        char *od = json_obj_strdup(&req, "order");
        int off = off_s ? atoi(off_s) : 0;
        int lim = lim_s ? atoi(lim_s) : 0;
        if (criteria || join)
            cmd_find(db_root, object, criteria ? criteria : "[]",
                     off, lim, fields, excl, fmt, delim, join, ob, od);
        else OUT("{\"error\":\"Missing criteria\"}\n");
        free(criteria); free(off_s); free(lim_s); free(fields); free(excl); free(fmt);
        free(delim); free(join); free(ob); free(od);
    } else if (strcmp(mode, "keys") == 0) {
        char *off_s = json_obj_strdup(&req, "offset");
        char *lim_s = json_obj_strdup(&req, "limit");
        char *fmt = json_obj_strdup(&req, "format");
        char *delim = json_obj_strdup(&req, "delimiter");
        cmd_keys(db_root, object, off_s ? atoi(off_s) : 0, lim_s ? atoi(lim_s) : 0, fmt, delim);
        free(off_s); free(lim_s); free(fmt); free(delim);
    } else if (strcmp(mode, "fetch") == 0) {
        char *off_s = json_obj_strdup(&req, "offset");
        char *lim_s = json_obj_strdup(&req, "limit");
        char *fields = json_obj_string_or_array(&req, "fields");
        char *cur = json_obj_strdup(&req, "cursor");
        char *fmt = json_obj_strdup(&req, "format");
        char *delim = json_obj_strdup(&req, "delimiter");
        cmd_fetch(db_root, object, off_s ? atoi(off_s) : 0, lim_s ? atoi(lim_s) : 0, fields, cur, fmt, delim);
        free(off_s); free(lim_s); free(fields); free(cur); free(fmt); free(delim);
    } else if (strcmp(mode, "add-index") == 0) {
        char *field = json_obj_strdup(&req, "field");
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        char *force = json_obj_strdup(&req, "force");
        int f = force && strcmp(force, "true") == 0;
        if (fields_arr)
            cmd_add_indexes(db_root, object, fields_arr, f);
        else if (field)
            cmd_add_index(db_root, object, field, f);
        free(field); free(fields_arr); free(force);
    } else if (strcmp(mode, "remove-index") == 0) {
        char *field = json_obj_strdup(&req, "field");
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        if (fields_arr)
            cmd_remove_indexes(db_root, object, fields_arr);
        else if (field)
            cmd_remove_index(db_root, object, field);
        else
            OUT("{\"error\":\"Missing field or fields\"}\n");
        free(field); free(fields_arr);
    } else if (strcmp(mode, "bulk-insert") == 0) {
        char *file = json_obj_strdup(&req, "file");
        char *records = json_obj_strdup_raw(&req, "records");
        if (records) {
            /* Inline records — pass string directly, no temp file */
            cmd_bulk_insert_string(db_root, object, records);
            free(records);
        } else {
            cmd_bulk_insert(db_root, object, file);
        }
        free(file);
    } else if (strcmp(mode, "bulk-insert-delimited") == 0) {
        char *file = json_obj_strdup(&req, "file");
        char *delim = json_obj_strdup(&req, "delimiter");
        char d = (delim && delim[0]) ? delim[0] : '|'; /* default pipe */
        cmd_bulk_insert_delimited(db_root, object, file, d);
        free(file); free(delim);
    } else if (strcmp(mode, "bulk-delete") == 0) {
        char *crit_json = json_obj_strdup_raw(&req, "criteria");
        if (crit_json) {
            /* Criteria-based bulk delete */
            char *lim_s = json_obj_strdup(&req, "limit");
            char *dry_s = json_obj_strdup(&req, "dry_run");
            int lim = lim_s ? atoi(lim_s) : 0;
            int dry = (dry_s && strcmp(dry_s, "true") == 0);
            cmd_bulk_delete_criteria(db_root, object, crit_json, lim, dry);
            free(crit_json); free(lim_s); free(dry_s);
        } else {
            /* Key-list bulk delete (existing path) */
            char *file = json_obj_strdup(&req, "file");
            char *keys = json_obj_strdup_raw(&req, "keys");
            if (keys) {
                char tmp[PATH_MAX];
                snprintf(tmp, sizeof(tmp), "/tmp/shard-db_bdel_%d.json", getpid());
                FILE *tf = fopen(tmp, "w");
                if (tf) { fputs(keys, tf); fclose(tf); }
                cmd_bulk_delete(db_root, object, tmp);
                unlink(tmp);
                free(keys);
            } else {
                cmd_bulk_delete(db_root, object, file);
            }
            free(file);
        }
    } else if (strcmp(mode, "bulk-update") == 0) {
        char *crit_json = json_obj_strdup_raw(&req, "criteria");
        char *value = json_obj_strdup_raw(&req, "value");
        char *lim_s = json_obj_strdup(&req, "limit");
        char *dry_s = json_obj_strdup(&req, "dry_run");
        int lim = lim_s ? atoi(lim_s) : 0;
        int dry = (dry_s && strcmp(dry_s, "true") == 0);
        if (crit_json && value)
            cmd_bulk_update(db_root, object, crit_json, value, lim, dry);
        else
            OUT("{\"error\":\"Missing criteria or value\"}\n");
        free(crit_json); free(value); free(lim_s); free(dry_s);
    } else if (strcmp(mode, "bulk-update-delimited") == 0) {
        char *file = json_obj_strdup(&req, "file");
        char *delim = json_obj_strdup(&req, "delimiter");
        char d = (delim && delim[0]) ? delim[0] : ',';
        if (file) cmd_bulk_update_delimited(db_root, object, file, d);
        else OUT("{\"error\":\"Missing file\"}\n");
        free(file); free(delim);
    } else if (strcmp(mode, "vacuum") == 0) {
        /* Optional flags: "compact":true and "splits":N route to rebuild_object;
           no flags means fast in-place tombstone reclaim. */
        char *compact_s = json_obj_strdup(&req, "compact");
        char *splits_s  = json_obj_strdup(&req, "splits");
        int compact = compact_s && strcmp(compact_s, "true") == 0;
        int new_splits = splits_s ? atoi(splits_s) : 0;
        cmd_vacuum(db_root, object, compact, new_splits);
        free(compact_s); free(splits_s);
    } else if (strcmp(mode, "rename-field") == 0) {
        char *oldn = json_obj_strdup(&req, "old");
        char *newn = json_obj_strdup(&req, "new");
        if (oldn && newn) cmd_rename_field(db_root, object, oldn, newn);
        else OUT("{\"error\":\"Missing 'old' or 'new' field name\"}\n");
        free(oldn); free(newn);
    } else if (strcmp(mode, "add-field") == 0) {
        /* fields is a JSON array of spec lines, e.g. ["phone:varchar:20","dob:date"] */
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        if (!fields_arr) { OUT("{\"error\":\"Missing 'fields' array\"}\n"); }
        else {
            char lines[MAX_FIELDS][256];
            int nlines = 0;
            const char *p = fields_arr;
            while (*p && nlines < MAX_FIELDS) {
                while (*p == '[' || *p == ',' || *p == ' ' || *p == '\t') p++;
                if (*p == ']' || *p == '\0') break;
                if (*p == '"') {
                    p++;
                    const char *start = p;
                    while (*p && *p != '"') p++;
                    size_t l = p - start;
                    if (l > 0 && l < 256) {
                        memcpy(lines[nlines], start, l);
                        lines[nlines][l] = '\0';
                        nlines++;
                    }
                    if (*p == '"') p++;
                } else p++;
            }
            if (nlines == 0) OUT("{\"error\":\"No fields in 'fields' array\"}\n");
            else cmd_add_fields(db_root, object, lines, nlines);
            free(fields_arr);
        }
    } else if (strcmp(mode, "remove-field") == 0) {
        /* fields is a JSON array of field names, e.g. ["email","age"] */
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        if (!fields_arr) { OUT("{\"error\":\"Missing 'fields' array\"}\n"); }
        else {
            char names[MAX_FIELDS][256];
            int nnames = 0;
            const char *p = fields_arr;
            while (*p && nnames < MAX_FIELDS) {
                while (*p == '[' || *p == ',' || *p == ' ' || *p == '\t') p++;
                if (*p == ']' || *p == '\0') break;
                if (*p == '"') {
                    p++;
                    const char *start = p;
                    while (*p && *p != '"') p++;
                    size_t l = p - start;
                    if (l > 0 && l < 256) {
                        memcpy(names[nnames], start, l);
                        names[nnames][l] = '\0';
                        nnames++;
                    }
                    if (*p == '"') p++;
                } else p++;
            }
            if (nnames == 0) OUT("{\"error\":\"No fields in 'fields' array\"}\n");
            else cmd_remove_fields(db_root, object, names, nnames);
            free(fields_arr);
        }
    } else if (strcmp(mode, "recount") == 0) {
        cmd_recount(db_root, object);
    } else if (strcmp(mode, "shard-stats") == 0) {
        char *fmt = json_obj_strdup(&req, "format");
        cmd_shard_stats(db_root, object, fmt && strcmp(fmt, "table") == 0);
        free(fmt);
    } else if (strcmp(mode, "truncate") == 0) {
        cmd_truncate(db_root, object);
    } else if (strcmp(mode, "backup") == 0) {
        cmd_backup(db_root, object);
    } else if (strcmp(mode, "put-file") == 0) {
        char *data = json_obj_strdup(&req, "data");
        if (data) {
            char *filename = json_obj_strdup(&req, "filename");
            char *ine = json_obj_strdup(&req, "if_not_exists");
            int if_not_exists = ine && strcmp(ine, "true") == 0;
            if (!filename)
                OUT("{\"error\":\"filename is required when uploading via data\"}\n");
            else
                cmd_put_file_b64(db_root, object, filename, data, strlen(data), if_not_exists);
            free(filename); free(ine);
        } else {
            char *path = json_obj_strdup(&req, "path");
            if (path) cmd_put_file(db_root, object, path);
            else OUT("{\"error\":\"put-file requires \\\"data\\\" (base64) or \\\"path\\\" (server-local)\"}\n");
            free(path);
        }
        free(data);
    } else if (strcmp(mode, "get-file") == 0) {
        char *filename = json_obj_strdup(&req, "filename");
        if (filename) cmd_get_file_b64(db_root, object, filename);
        else OUT("{\"error\":\"filename is required\"}\n");
        free(filename);
    } else if (strcmp(mode, "get-file-path") == 0) {
        char *filename = json_obj_strdup(&req, "filename");
        if (filename) cmd_get_file_path(db_root, object, filename);
        free(filename);
    } else if (strcmp(mode, "delete-file") == 0) {
        char *filename = json_obj_strdup(&req, "filename");
        if (filename) cmd_delete_file(db_root, object, filename);
        else OUT("{\"error\":\"filename is required\"}\n");
        free(filename);
    } else if (strcmp(mode, "aggregate") == 0) {
        char *crit = json_obj_strdup_raw(&req, "criteria");
        char *grp = json_obj_strdup_raw(&req, "group_by");
        char *aggs = json_obj_strdup_raw(&req, "aggregates");
        char *hav = json_obj_strdup_raw(&req, "having");
        char *ob = json_obj_strdup(&req, "order_by");
        char *od = json_obj_strdup(&req, "order");
        char *lim_s = json_obj_strdup(&req, "limit");
        char *fmt = json_obj_strdup(&req, "format");
        char *delim = json_obj_strdup(&req, "delimiter");
        int desc = (od && strcmp(od, "desc") == 0);
        int lim = lim_s ? atoi(lim_s) : 0;
        cmd_aggregate(db_root, object, crit, grp, aggs, hav, ob, desc, lim, fmt, delim);
        free(crit); free(grp); free(aggs); free(hav);
        free(ob); free(od); free(lim_s); free(fmt); free(delim);
    } else if (strcmp(mode, "sequence") == 0) {
        char *name = json_obj_strdup(&req, "name");
        char *action = json_obj_strdup(&req, "action");
        char *batch_s = json_obj_strdup(&req, "batch");
        if (name && action)
            cmd_sequence(db_root, object, name, action, batch_s ? atoi(batch_s) : 1);
        else
            OUT("{\"error\":\"Missing name or action\"}\n");
        free(name); free(action); free(batch_s);
    } else {
        OUT("{\"error\":\"Unknown mode: %s\"}\n", mode);
    }

    if (took_wrlock) objlock_wrunlock(db_root, object);
    else if (took_rdlock) objlock_rdunlock(db_root, object);

    free(mode); free(dir); free(object);
}

/* ========== SERVER MODE (epoll + thread pool) ========== */

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/epoll.h>

volatile int server_running = 1;
volatile int active_threads = 0;
volatile int in_flight_writes = 0;    /* write/schema modes; shutdown waits for these */
pthread_mutex_t thread_count_lock = PTHREAD_MUTEX_INITIALIZER;

/* Work queue for thread pool */
typedef struct {
    int *queue;
    int capacity;
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} WorkQueue;

void wq_init(WorkQueue *wq, int capacity) {
    wq->queue = malloc(capacity * sizeof(int));
    wq->capacity = capacity;
    wq->head = wq->tail = wq->count = 0;
    pthread_mutex_init(&wq->lock, NULL);
    pthread_cond_init(&wq->not_empty, NULL);
    pthread_cond_init(&wq->not_full, NULL);
}

void wq_push(WorkQueue *wq, int fd) {
    pthread_mutex_lock(&wq->lock);
    while (wq->count >= wq->capacity && server_running)
        pthread_cond_wait(&wq->not_full, &wq->lock);
    if (!server_running) { pthread_mutex_unlock(&wq->lock); close(fd); return; }
    wq->queue[wq->tail] = fd;
    wq->tail = (wq->tail + 1) % wq->capacity;
    wq->count++;
    pthread_cond_signal(&wq->not_empty);
    pthread_mutex_unlock(&wq->lock);
}

int wq_pop(WorkQueue *wq) {
    pthread_mutex_lock(&wq->lock);
    while (wq->count == 0 && server_running)
        pthread_cond_wait(&wq->not_empty, &wq->lock);
    if (wq->count == 0) { pthread_mutex_unlock(&wq->lock); return -1; }
    int fd = wq->queue[wq->head];
    wq->head = (wq->head + 1) % wq->capacity;
    wq->count--;
    pthread_cond_signal(&wq->not_full);
    pthread_mutex_unlock(&wq->lock);
    return fd;
}

void wq_destroy(WorkQueue *wq) {
    /* Drain remaining connections */
    while (wq->count > 0) {
        int fd = wq->queue[wq->head];
        wq->head = (wq->head + 1) % wq->capacity;
        wq->count--;
        close(fd);
    }
    free(wq->queue);
    pthread_mutex_destroy(&wq->lock);
    pthread_cond_destroy(&wq->not_empty);
    pthread_cond_destroy(&wq->not_full);
}

WorkQueue g_work_queue;

void handle_shutdown(int sig) {
    (void)sig;
    server_running = 0;
    g_scan_stop = 1;  /* abort any in-flight shard scans, vacuum, index builds */
    /* Wake all waiting workers */
    pthread_cond_broadcast(&g_work_queue.not_empty);
    pthread_cond_broadcast(&g_work_queue.not_full);
}

/* Fast process — stdout already redirected to client by worker thread */
#define FIELD_SEP "\x1F"
#define MAX_ARGS 8
void server_process_fast(const char *db_root, const char *line, const char *client_ip) {
    const char *trimmed = line;
    while (*trimmed == ' ') trimmed++;
    uint64_t t0 = (g_slow_query_ms > 0) ? now_ms() : 0;
    int is_json = (*trimmed == '{');

    /* Reset any stale per-request timeout from a previous request on this
       worker thread. dispatch_json_query will overwrite it for JSON requests
       that carry "timeout_ms"; the legacy \x1F-delimited path doesn't carry
       it at all, so we just zero it here. */
    g_request_timeout_ms = 0;

    /* Detect mode early so we can track writes for graceful-shutdown drain.
       Writes must complete before the server exits; reads are safe to drop. */
    char *mode_for_write = NULL;
    if (is_json) {
        JsonObj tmp;
        json_parse_object(trimmed, strlen(trimmed), &tmp);
        mode_for_write = json_obj_strdup(&tmp, "mode");
    } else {
        /* legacy: first field is the command */
        const char *p = trimmed;
        size_t n = 0;
        while (p[n] && p[n] != '\x1F' && p[n] != '\n' && n < 31) n++;
        mode_for_write = strndup(p, n);
    }
    int is_write = mode_for_write && (mode_is_write(mode_for_write) || mode_is_schema(mode_for_write));
    if (is_write) {
        pthread_mutex_lock(&thread_count_lock);
        in_flight_writes++;
        pthread_mutex_unlock(&thread_count_lock);
    }

    if (is_json) {
        dispatch_json_query(db_root, trimmed, client_ip);
        fflush(g_out);
        goto timing;
    }

    /* Legacy \x1F protocol */
    char *args[MAX_ARGS];
    char linecopy[MAX_LINE * 2];
    strncpy(linecopy, line, sizeof(linecopy) - 1);
    linecopy[sizeof(linecopy) - 1] = '\0';

    int nargs = 0;
    char *_tok_save = NULL; char *tok = strtok_r(linecopy, FIELD_SEP, &_tok_save);
    while (tok && nargs < MAX_ARGS) {
        args[nargs++] = tok;
        tok = strtok_r(NULL, FIELD_SEP, &_tok_save);
    }
    /* Pad with empty strings */
    for (int i = nargs; i < MAX_ARGS; i++) args[i] = "";

    const char *cmd = args[0];
    const char *dir_arg = args[1];
    const char *object = args[2];
    const char *arg1 = args[3];
    const char *arg2 = args[4];
    const char *arg3 = args[5];
    const char *arg4 = args[6];

    /* Validate dir. Must `goto timing;` (not bare return) so is_write's
       in_flight_writes++ at the top is paired with the matching decrement;
       otherwise errored writes leak the counter and `stop` waits 30s for
       phantom drains. */
    if (!is_valid_dir(dir_arg)) {
        OUT("{\"error\":\"Unknown dir: %s\"}\n", dir_arg);
        fflush(g_out);
        goto timing;
    }
    char eff_root[PATH_MAX];
    build_effective_root(eff_root, sizeof(eff_root), dir_arg);

    /* Validate object exists — every fast-path command operates on a created object.
       Without this, missing objects reach cmd_insert/cmd_get/... and null-deref on
       the schema, killing the worker thread. */
    if (!object || !object[0]) {
        OUT("{\"error\":\"object is required\"}\n");
        fflush(g_out);
        goto timing;
    }
    char obj_check[PATH_MAX];
    snprintf(obj_check, sizeof(obj_check), "%s/%s/fields.conf", eff_root, object);
    struct stat obj_st;
    if (stat(obj_check, &obj_st) != 0) {
        OUT("{\"error\":\"Object [%s] not found. Use create-object first.\"}\n", object);
        fflush(g_out);
        goto timing;
    }

    /* All args are \x1F separated:
       get\x1Fdir\x1Fobj\x1Fkey
       insert\x1Fdir\x1Fobj\x1Fkey\x1Fvalue
       delete\x1Fdir\x1Fobj\x1Fkey
       exists\x1Fdir\x1Fobj\x1Fkey
       size\tobj
       find\tobj\tcriteria\toffset\tlimit\tfields
       keys\tobj\toffset\tlimit
       fetch\tobj\toffset\tlimit\tfields
       add-index\tobj\tfield\t-f
       remove-index\tobj\tfield
       bulk-insert\tobj\tfile
       bulk-delete\tobj\tfile
       vacuum\tobj / recount\tobj / truncate\tobj / backup\tobj
       put-file\tobj\tpath / get-file-path\tobj\tfilename
    */
    /* Per-object locking for this dispatch — same policy as JSON mode. */
    int fast_wr = mode_is_schema(cmd);
    int fast_rd = !fast_wr && mode_is_write(cmd);
    if (fast_wr) objlock_wrlock(eff_root, object);
    else if (fast_rd) objlock_rdlock(eff_root, object);

    if (strcasecmp(cmd, "get") == 0) {
        cmd_get(eff_root, object, arg1);
    } else if (strcasecmp(cmd, "insert") == 0) {
        cmd_insert(eff_root, object, arg1, arg2, NULL, 0);
    } else if (strcasecmp(cmd, "delete") == 0) {
        cmd_delete(eff_root, object, arg1, NULL, 0);
    } else if (strcasecmp(cmd, "size") == 0) {
        cmd_size(eff_root, object);
    } else if (strcasecmp(cmd, "exists") == 0) {
        cmd_exists(eff_root, object, arg1);
    } else if (strcasecmp(cmd, "keys") == 0) {
        cmd_keys(eff_root, object, arg1[0] ? atoi(arg1) : 0, arg2[0] ? atoi(arg2) : 0, NULL, NULL);
    } else if (strcasecmp(cmd, "fetch") == 0) {
        /* fetch\tobj\toff\tlim\tfields */
        cmd_fetch(eff_root, object, arg1[0] ? atoi(arg1) : 0,
                  arg2[0] ? atoi(arg2) : 0, arg3[0] ? arg3 : NULL, NULL, NULL, NULL);
    } else if (strcasecmp(cmd, "find") == 0) {
        /* find\tobj\tcriteria\toff\tlim\tfields (excludedKeys/join/order_by via JSON mode only) */
        cmd_find(eff_root, object, arg1,
                 arg2[0] ? atoi(arg2) : 0, arg3[0] ? atoi(arg3) : 0,
                 arg4[0] ? arg4 : NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    } else if (strcasecmp(cmd, "backup") == 0) {
        cmd_backup(eff_root, object);
    } else if (strcasecmp(cmd, "add-index") == 0) {
        int force = (arg2[0] && strcmp(arg2, "-f") == 0);
        cmd_add_index(eff_root, object, arg1, force);
    } else if (strcasecmp(cmd, "remove-index") == 0) {
        cmd_remove_index(eff_root, object, arg1);
    } else if (strcasecmp(cmd, "bulk-insert") == 0) {
        cmd_bulk_insert(eff_root, object, arg1[0] ? arg1 : NULL);
    } else if (strcasecmp(cmd, "bulk-delete") == 0) {
        cmd_bulk_delete(eff_root, object, arg1[0] ? arg1 : NULL);
    } else if (strcasecmp(cmd, "vacuum") == 0) {
        /* Legacy fast path always does fast in-place vacuum; JSON mode carries the flags. */
        cmd_vacuum(eff_root, object, 0, 0);
    } else if (strcasecmp(cmd, "recount") == 0) {
        cmd_recount(eff_root, object);
    } else if (strcasecmp(cmd, "truncate") == 0) {
        cmd_truncate(eff_root, object);
    } else if (strcasecmp(cmd, "put-file") == 0) {
        cmd_put_file(eff_root, object, arg1);
    } else if (strcasecmp(cmd, "get-file-path") == 0) {
        cmd_get_file_path(eff_root, object, arg1);
    } else if (strcasecmp(cmd, "sequence") == 0) {
        /* sequence\tobj\tname\taction[\tbatch] */
        cmd_sequence(eff_root, object, arg1, arg2, arg3[0] ? atoi(arg3) : 1);
    } else {
        OUT("Error: Unknown command: %s\n", cmd);
    }

    if (fast_wr) objlock_wrunlock(eff_root, object);
    else if (fast_rd) objlock_rdunlock(eff_root, object);

    fflush(stdout); fflush(stderr);

timing:
    if (is_write) {
        pthread_mutex_lock(&thread_count_lock);
        in_flight_writes--;
        pthread_mutex_unlock(&thread_count_lock);
    }
    free(mode_for_write);

    if (g_slow_query_ms > 0) {
        uint64_t dt = now_ms() - t0;
        if (dt > (uint64_t)g_slow_query_ms) {
            char *mode = NULL, *dir_s = NULL, *obj_s = NULL;
            if (is_json) {
                JsonObj tmp;
                json_parse_object(trimmed, strlen(trimmed), &tmp);
                mode  = json_obj_strdup(&tmp, "mode");
                dir_s = json_obj_strdup(&tmp, "dir");
                obj_s = json_obj_strdup(&tmp, "object");
            }
            log_slow_query(mode ? mode : (is_json ? "" : "legacy"),
                           dir_s ? dir_s : "",
                           obj_s ? obj_s : "",
                           (uint32_t)dt);
            free(mode); free(dir_s); free(obj_s);
        }
    }
}

/* Worker thread — pulls connections from work queue */
typedef struct {
    char db_root[PATH_MAX];
    int id;
} WorkerArg;

void *worker_thread(void *arg) {
    WorkerArg *wa = (WorkerArg *)arg;

    while (server_running) {
        int cfd = wq_pop(&g_work_queue);
        if (cfd < 0) break;

        pthread_mutex_lock(&thread_count_lock);
        active_threads++;
        pthread_mutex_unlock(&thread_count_lock);

        /* Get client IP for auth decisions */
        struct sockaddr_in peer_addr;
        socklen_t peer_len = sizeof(peer_addr);
        char client_ip[INET_ADDRSTRLEN] = "127.0.0.1";
        if (getpeername(cfd, (struct sockaddr *)&peer_addr, &peer_len) == 0)
            inet_ntop(AF_INET, &peer_addr.sin_addr, client_ip, sizeof(client_ip));

        /* Per-connection output stream — no dup2, thread-safe */
        int out_fd = dup(cfd);  /* separate fd for writing */
        FILE *out = fdopen(out_fd, "w");
        FILE *cf = fdopen(cfd, "r");
        if (cf && out) {
            g_out = out;  /* thread-local: all OUT()/fprintf(g_out,...) goes to this client */
            int buf_size = g_max_request_size > 0 ? g_max_request_size : MAX_LINE;
            char *line = malloc(buf_size);
            while (line && fgets(line, buf_size, cf)) {
                /* Check for oversized request: fgets filled buffer without finding newline */
                int len = strlen(line);
                if (len > 0 && line[len - 1] != '\n' && len >= buf_size - 1) {
                    /* Drain the rest of this oversized line */
                    int c;
                    while ((c = fgetc(cf)) != EOF && c != '\n');
                    OUT("{\"error\":\"Request too large (max %d bytes)\"}\n", buf_size - 1);
                    fflush(g_out);
                    /* Emit command separator (\0\n) so the client read loop unblocks. */
                    fputc('\0', g_out);
                    fputc('\n', g_out);
                    fflush(g_out);
                    continue;
                }
                line[strcspn(line, "\n")] = '\0';
                if (line[0] == '\0') continue;
                if (strcasecmp(line, "QUIT") == 0) break;

                if (!server_running) {
                    OUT("{\"error\":\"Server shutting down\"}\n");
                    fflush(g_out);
                    fputc('\0', g_out);
                    fputc('\n', g_out);
                    fflush(g_out);
                    break;
                }

                server_process_fast(wa->db_root, line, client_ip);
                fflush(g_out);

                /* Write null terminator + newline as command separator */
                fputc('\0', g_out);
                fputc('\n', g_out);
                fflush(g_out);
            }
            free(line);
            g_out = stdout;  /* restore for safety */
            fclose(cf);  /* closes cfd */
            fclose(out); /* closes out_fd */
        } else {
            if (out) fclose(out); else close(out_fd);
            if (cf) fclose(cf); else close(cfd);
        }

        pthread_mutex_lock(&thread_count_lock);
        active_threads--;
        pthread_mutex_unlock(&thread_count_lock);
    }

    free(wa);
    return NULL;
}

void write_pid_file(const char *db_root, int port) {
    (void)db_root;
    char pidpath[PATH_MAX];
    snprintf(pidpath, sizeof(pidpath), "%s/shard-db.pid", g_log_dir);
    FILE *f = fopen(pidpath, "w");
    if (f) { fprintf(f, "%d\n%d\n", getpid(), port); fclose(f); }
}

void remove_pid_file(const char *db_root) {
    (void)db_root;
    char pidpath[PATH_MAX];
    snprintf(pidpath, sizeof(pidpath), "%s/shard-db.pid", g_log_dir);
    unlink(pidpath);
}

int cmd_server(const char *db_root, int daemonize) {
    int port = g_port;

    /* Raise the file-descriptor soft limit to the hard limit. ucache holds 1
       fd per cached shard and briefly 2 during ucache_grow_shard (new + retired
       for grace-period). At FCACHE_MAX=4096 defaults, peak need is ~8k fds —
       well above the 1024 default on many distros. Shell-default limits cause
       EMFILE inside ucache_grow_shard at high split counts. Soft → hard needs
       no privilege. If the hard limit itself is below a practical floor, warn
       with actionable guidance. */
    {
        struct rlimit rl;
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
            if (rl.rlim_cur < rl.rlim_max) {
                struct rlimit rl_new = { rl.rlim_max, rl.rlim_max };
                if (setrlimit(RLIMIT_NOFILE, &rl_new) == 0)
                    rl = rl_new;
            }
            /* Practical floor: 2 × FCACHE_MAX + 64 slack for sockets, logs,
               index fds, stdin/out/err. At the default FCACHE_MAX=4096 this is
               8256. */
            rlim_t needed = (rlim_t)((g_fcache_cap > 0 ? g_fcache_cap : 4096) * 2 + 64);
            if (rl.rlim_cur < needed) {
                const char *user = getenv("USER");
                if (!user) user = "your-user";
                fprintf(stderr,
                    "WARN: RLIMIT_NOFILE soft=%llu hard=%llu is below the "
                    "practical floor %llu for FCACHE_MAX=%d. Bulk inserts at "
                    "high split counts may hit EMFILE. Raise the hard limit:\n"
                    "  /etc/security/limits.conf:\n"
                    "    %s soft nofile %llu\n"
                    "    %s hard nofile %llu\n"
                    "  …then log out + back in. Or add 'LimitNOFILE=%llu' to "
                    "the systemd unit file.\n",
                    (unsigned long long)rl.rlim_cur,
                    (unsigned long long)rl.rlim_max,
                    (unsigned long long)needed,
                    g_fcache_cap > 0 ? g_fcache_cap : 4096,
                    user, (unsigned long long)needed,
                    user, (unsigned long long)needed,
                    (unsigned long long)needed);
            }
        }
    }

    /* Single-instance guard. flock on a per-DB_ROOT lock file prevents a
       second shard-db process from attaching to the same data directory
       (which would corrupt shared mmap state — the per-object rwlocks in
       objlock.c are in-process only). The lock is held by the kernel for
       the server process lifetime and released automatically on exit or
       crash; fork() carries the open-file-description across so the
       daemon child inherits the held lock after the parent returns. */
    char lockpath[PATH_MAX];
    snprintf(lockpath, sizeof(lockpath), "%s/.shard-db.lock", db_root);
    int lock_fd = open(lockpath, O_CREAT | O_RDWR, 0644);
    if (lock_fd < 0) {
        fprintf(stderr, "Error: cannot open lock file %s: %s\n", lockpath, strerror(errno));
        return 1;
    }
    if (flock(lock_fd, LOCK_EX | LOCK_NB) < 0) {
        fprintf(stderr,
                "Error: another shard-db instance is already running on DB_ROOT=%s "
                "(lock held on %s). Stop it first with './shard-db stop'.\n",
                db_root, lockpath);
        close(lock_fd);
        return 1;
    }

    if (daemonize) {
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); close(lock_fd); return 1; }
        if (pid > 0) {
            usleep(200000);
            OUT("shard-db started (pid %d, port %d)\n", pid, port);
            /* Parent exits; child's fd keeps the OFD (and the flock) alive. */
            return 0;
        }
        setsid();
        freopen("/dev/null", "r", stdin);
        freopen("/dev/null", "w", stdout);
        freopen("/dev/null", "w", stderr);
    }

    /* Record the running PID inside the lock file for operator visibility
       (lsof / cat .shard-db.lock). Not load-bearing — the lock is what
       enforces exclusion. */
    char pidbuf[32];
    int pidlen = snprintf(pidbuf, sizeof(pidbuf), "%d\n", (int)getpid());
    if (ftruncate(lock_fd, 0) == 0) {
        ssize_t _ignored = pwrite(lock_fd, pidbuf, pidlen, 0);
        (void)_ignored;
    }

    /* TCP socket */
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in saddr;
    memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_addr.s_addr = INADDR_ANY;
    saddr.sin_port = htons(port);

    if (bind(sfd, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
        perror("bind"); close(sfd); return 1;
    }
    if (listen(sfd, 128) < 0) { perror("listen"); close(sfd); return 1; }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_shutdown;
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    log_init(db_root);
    write_pid_file(db_root, port);
    g_server_start_ms = now_ms();
    bt_page_size = g_index_page_size;
    fcache_init(g_fcache_cap);
    bt_cache_init(g_btcache_cap);
    /* Pool size: explicit THREADS wins; otherwise 4× cores.
       Oversubscription hides shard-rwlock stalls that a thread-per-core
       pool can't overlap — measured ~18% faster on parallel bulk-insert. */
    int pool_sz = g_max_threads > 0
                  ? g_max_threads
                  : (int)sysconf(_SC_NPROCESSORS_ONLN) * 4;
    if (pool_sz < 4) pool_sz = 4;
    parallel_pool_init(pool_sz);
    load_dirs();
    load_tokens_conf(db_root);
    load_allowed_ips_conf(db_root);
    objlock_init();
    rebuild_recovery(db_root);
    grow_recovery(db_root);

    int nthreads = g_workers > 0 ? g_workers : (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads < 4) nthreads = 4; /* minimum pool size */

    /* Init work queue and thread pool */
    wq_init(&g_work_queue, nthreads * 64);
    pthread_t *pool = malloc(nthreads * sizeof(pthread_t));
    for (int i = 0; i < nthreads; i++) {
        WorkerArg *wa = malloc(sizeof(WorkerArg));
        strncpy(wa->db_root, db_root, PATH_MAX - 1);
        wa->id = i;
        pthread_create(&pool[i], NULL, worker_thread, wa);
    }

    fprintf(stdout, "shard-db listening on port %d (pid=%d, workers=%d, timeout=%us)\n",
            port, getpid(), nthreads, g_timeout);
    fflush(stdout);
    log_msg(3, "SERVER START port=%d pid=%d workers=%d", port, getpid(), nthreads);

    /* epoll-based accept loop */
    int epfd = epoll_create1(0);
    struct epoll_event ev = { .events = EPOLLIN, .data.fd = sfd };
    epoll_ctl(epfd, EPOLL_CTL_ADD, sfd, &ev);

    while (server_running) {
        struct epoll_event events[16];
        int n = epoll_wait(epfd, events, 16, 500); /* 500ms timeout for shutdown check */
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }
        for (int i = 0; i < n; i++) {
            struct sockaddr_in caddr;
            socklen_t clen = sizeof(caddr);
            int cfd = accept(sfd, (struct sockaddr *)&caddr, &clen);
            if (cfd < 0) continue;

            /* TCP_NODELAY — reduce latency for small responses */
            int flag = 1;
            setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

            wq_push(&g_work_queue, cfd);
        }
    }

    /* Graceful shutdown */
    close(sfd);
    close(epfd);
    log_msg(3, "SHUTDOWN: waiting for %d in-flight writes, %d active connections",
            in_flight_writes, active_threads);

    /* Wake workers and wait for them */
    pthread_cond_broadcast(&g_work_queue.not_empty);
    for (int i = 0; i < nthreads; i++)
        pthread_join(pool[i], NULL);
    free(pool);
    wq_destroy(&g_work_queue);

    /* Wait for any remaining in-flight writes (up to 30s) */
    for (int i = 0; i < 300 && in_flight_writes > 0; i++) usleep(100000);

    remove_pid_file(db_root);
    parallel_pool_shutdown();
    fcache_shutdown();
    bt_cache_shutdown();
    log_msg(3, "SERVER STOP pid=%d", getpid());
    log_shutdown();
    fprintf(stdout, "shard-db stopped (pid=%d)\n", getpid());
    fflush(stdout);
    return 0;
}

int cmd_stop(const char *db_root) {
    (void)db_root;
    char pidpath[PATH_MAX];
    snprintf(pidpath, sizeof(pidpath), "%s/shard-db.pid", g_log_dir);
    FILE *f = fopen(pidpath, "r");
    if (!f) { fprintf(stderr, "Error: No running server (no pid file)\n"); return 1; }
    int pid, port;
    fscanf(f, "%d\n%d", &pid, &port);
    fclose(f);

    if (kill(pid, 0) != 0) {
        fprintf(stderr, "Server (pid %d) not running. Cleaning up.\n", pid);
        unlink(pidpath);
        return 0;
    }

    OUT("Stopping shard-db (pid %d, port %d)...\n", pid, port);
    kill(pid, SIGTERM);

    /* Wait up to 30s for graceful shutdown (in-flight writes must complete) */
    for (int i = 0; i < 300; i++) {
        usleep(100000);
        if (kill(pid, 0) != 0) { OUT("Server stopped.\n"); return 0; }
    }

    /* NEVER force kill — writes in progress would be lost.
       Tell the user to retry SIGTERM or investigate the workload. */
    fprintf(stderr,
        "Server (pid %d) still busy after 30s — writes in progress.\n"
        "Data safety: NOT force-killing. Options:\n"
        "  1. Wait longer and retry: ./shard-db stop\n"
        "  2. Check active work: ./shard-db status\n"
        "  3. If truly stuck, manual SIGKILL with: kill -9 %d (DATA LOSS RISK)\n",
        pid, pid);
    return 1;
}

int cmd_status(const char *db_root) {
    (void)db_root;
    char pidpath[PATH_MAX];
    snprintf(pidpath, sizeof(pidpath), "%s/shard-db.pid", g_log_dir);
    FILE *f = fopen(pidpath, "r");
    if (!f) { OUT("stopped\n"); return 1; }
    int pid, port;
    fscanf(f, "%d\n%d", &pid, &port);
    fclose(f);
    if (kill(pid, 0) == 0) {
        OUT("running (pid %d, port %d)\n", pid, port);
        return 0;
    }
    OUT("stopped (stale pid file)\n");
    unlink(pidpath);
    return 1;
}

/* TCP client — connect to port */
int cmd_query(int port, int argc, char **argv) {
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return 1; }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(port);

    if (connect(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Error: Cannot connect to port %d\n", port);
        close(sfd); return 1;
    }

    /* Protocol: all args separated by Unit Separator (0x1F) */
    char buf[MAX_LINE * 2];
    int pos = 0;
    for (int i = 0; i < argc; i++) {
        if (i > 0) buf[pos++] = '\x1F';
        pos += snprintf(buf + pos, sizeof(buf) - pos, "%s", argv[i]);
    }
    buf[pos++] = '\n';
    write(sfd, buf, pos);

    char rbuf[8192];
    ssize_t n;
    while ((n = read(sfd, rbuf, sizeof(rbuf))) > 0) {
        for (ssize_t j = 0; j < n; j++) {
            if (rbuf[j] == '\0') {
                write(STDOUT_FILENO, rbuf, j);
                close(sfd);
                return 0;
            }
        }
        write(STDOUT_FILENO, rbuf, n);
    }
    close(sfd);
    return 0;
}

/* Send raw JSON query to server */
int cmd_query_json(int port, const char *json) {
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return 1; }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(port);
    if (connect(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "{\"error\":\"Cannot connect to port %d\"}\n", port);
        close(sfd); return 1;
    }
    write(sfd, json, strlen(json));
    write(sfd, "\n", 1);
    char rbuf[8192];
    ssize_t n;
    while ((n = read(sfd, rbuf, sizeof(rbuf))) > 0) {
        for (ssize_t j = 0; j < n; j++) {
            if (rbuf[j] == '\0') { write(STDOUT_FILENO, rbuf, j); close(sfd); return 0; }
        }
        write(STDOUT_FILENO, rbuf, n);
    }
    close(sfd);
    return 0;
}

/* ========== File upload/download client helpers ========== */

/* Blocking write of all bytes. -1 on error. */
static int write_all(int fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t w = 0;
    while (w < len) {
        ssize_t n = write(fd, p + w, len - w);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        if (n == 0) return -1;
        w += (size_t)n;
    }
    return 0;
}

/* Connect to local server, send JSON line, return accumulated response (up to first \0).
   Caller frees *out. Returns 0 on success, -1 on error. */
static int query_collect(int port, const char *json, size_t json_len, char **out, size_t *out_len) {
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) return -1;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(port);
    if (connect(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { close(sfd); return -1; }

    if (write_all(sfd, json, json_len) != 0) { close(sfd); return -1; }
    if (write_all(sfd, "\n", 1) != 0) { close(sfd); return -1; }

    size_t cap = 8192, len = 0;
    char *buf = malloc(cap);
    if (!buf) { close(sfd); return -1; }

    char rbuf[8192];
    ssize_t n;
    while ((n = read(sfd, rbuf, sizeof(rbuf))) > 0) {
        for (ssize_t j = 0; j < n; j++) {
            if (rbuf[j] == '\0') {
                if (len + j > cap) {
                    while (cap < len + j) cap *= 2;
                    char *nb = realloc(buf, cap);
                    if (!nb) { free(buf); close(sfd); return -1; }
                    buf = nb;
                }
                memcpy(buf + len, rbuf, j);
                len += j;
                close(sfd);
                *out = buf; *out_len = len;
                return 0;
            }
        }
        if (len + (size_t)n > cap) {
            while (cap < len + (size_t)n) cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); close(sfd); return -1; }
            buf = nb;
        }
        memcpy(buf + len, rbuf, n);
        len += (size_t)n;
    }
    close(sfd);
    /* EOF with no \0 sentinel — still return what we got. */
    *out = buf; *out_len = len;
    return 0;
}

/* CLI: read local file, base64-encode, send put-file JSON, print server response. */
int cmd_put_file_tcp(int port, const char *dir, const char *object,
                     const char *local_path, int if_not_exists) {
    struct stat st;
    if (stat(local_path, &st) != 0) {
        fprintf(stderr, "{\"error\":\"source file not found: %s\"}\n", local_path);
        return 1;
    }
    size_t raw_len = (size_t)st.st_size;

    int fd = open(local_path, O_RDONLY);
    if (fd < 0) { fprintf(stderr, "{\"error\":\"cannot open %s\"}\n", local_path); return 1; }
    uint8_t *raw = malloc(raw_len ? raw_len : 1);
    if (!raw) { close(fd); fprintf(stderr, "{\"error\":\"out of memory\"}\n"); return 1; }
    size_t r = 0;
    while (r < raw_len) {
        ssize_t n = read(fd, raw + r, raw_len - r);
        if (n <= 0) break;
        r += (size_t)n;
    }
    close(fd);
    if (r != raw_len) { free(raw); fprintf(stderr, "{\"error\":\"read failed\"}\n"); return 1; }

    size_t enc_sz = b64_encoded_size(raw_len);
    char *enc = malloc(enc_sz + 1);
    if (!enc) { free(raw); fprintf(stderr, "{\"error\":\"out of memory\"}\n"); return 1; }
    b64_encode(raw, raw_len, enc);
    free(raw);

    const char *filename = strrchr(local_path, '/');
    filename = filename ? filename + 1 : local_path;

    /* Build JSON: {"mode":"put-file","dir":"D","object":"O","filename":"F","data":"B64"[,"if_not_exists":true]} */
    size_t json_cap = enc_sz + 256 + strlen(dir) + strlen(object) + strlen(filename);
    char *json = malloc(json_cap);
    if (!json) { free(enc); fprintf(stderr, "{\"error\":\"out of memory\"}\n"); return 1; }
    int jl = snprintf(json, json_cap,
        "{\"mode\":\"put-file\",\"dir\":\"%s\",\"object\":\"%s\",\"filename\":\"%s\",%s\"data\":\"%s\"}",
        dir, object, filename, if_not_exists ? "\"if_not_exists\":true," : "", enc);
    free(enc);
    if (jl < 0 || (size_t)jl >= json_cap) {
        free(json); fprintf(stderr, "{\"error\":\"json build failed\"}\n"); return 1;
    }

    char *resp = NULL; size_t resp_len = 0;
    int rc = query_collect(port, json, (size_t)jl, &resp, &resp_len);
    free(json);
    if (rc != 0) {
        fprintf(stderr, "{\"error\":\"cannot connect to port %d\"}\n", port);
        return 1;
    }
    write(STDOUT_FILENO, resp, resp_len);
    write(STDOUT_FILENO, "\n", 1);
    free(resp);
    return 0;
}

/* CLI: send get-file JSON, parse response, decode base64, write to out_path (NULL=stdout). */
int cmd_get_file_tcp(int port, const char *dir, const char *object,
                     const char *filename, const char *out_path) {
    char json[1024];
    int jl = snprintf(json, sizeof(json),
        "{\"mode\":\"get-file\",\"dir\":\"%s\",\"object\":\"%s\",\"filename\":\"%s\"}",
        dir, object, filename);
    if (jl < 0 || jl >= (int)sizeof(json)) {
        fprintf(stderr, "{\"error\":\"request too long\"}\n"); return 1;
    }

    char *resp = NULL; size_t resp_len = 0;
    if (query_collect(port, json, (size_t)jl, &resp, &resp_len) != 0) {
        fprintf(stderr, "{\"error\":\"cannot connect to port %d\"}\n", port);
        return 1;
    }

    /* NUL-terminate for JSON parsing. */
    char *resp_z = malloc(resp_len + 1);
    if (!resp_z) { free(resp); fprintf(stderr, "{\"error\":\"out of memory\"}\n"); return 1; }
    memcpy(resp_z, resp, resp_len);
    resp_z[resp_len] = '\0';
    free(resp);

    /* Extract "data" (base64 payload) + "status" from the server response. */
    JsonObj resp_obj;
    json_parse_object(resp_z, resp_len, &resp_obj);
    char *data = json_obj_strdup(&resp_obj, "data");
    if (!data) {
        /* Server returned error or unexpected shape — surface it verbatim. */
        fprintf(stderr, "%s\n", resp_z);
        free(resp_z);
        return 1;
    }
    char *status = json_obj_strdup(&resp_obj, "status");

    size_t b64_len = strlen(data);
    size_t cap = b64_decoded_maxsize(b64_len);
    uint8_t *raw = malloc(cap ? cap : 1);
    size_t raw_len = 0;
    if (!raw || b64_decode(data, b64_len, raw, &raw_len) != 0) {
        free(raw); free(data); free(status); free(resp_z);
        fprintf(stderr, "{\"error\":\"invalid base64 in response\"}\n");
        return 1;
    }

    int ofd = STDOUT_FILENO;
    int close_out = 0;
    if (out_path) {
        ofd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (ofd < 0) {
            free(raw); free(data); free(status); free(resp_z);
            fprintf(stderr, "{\"error\":\"cannot open output %s\"}\n", out_path);
            return 1;
        }
        close_out = 1;
    }
    int werr = write_all(ofd, raw, raw_len);
    if (close_out) close(ofd);
    free(raw); free(data); free(status); free(resp_z);
    if (werr != 0) { fprintf(stderr, "{\"error\":\"write failed\"}\n"); return 1; }
    if (out_path) fprintf(stderr, "{\"status\":\"ok\",\"filename\":\"%s\",\"bytes\":%zu,\"out\":\"%s\"}\n",
                          filename, raw_len, out_path);
    return 0;
}

