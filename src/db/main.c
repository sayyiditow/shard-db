#include "types.h"

/* ========== MAIN ========== */

/* Read port from PID file (for auto-connect) */
int read_server_port(const char *db_root) {
    (void)db_root;
    char pidpath[PATH_MAX];
    snprintf(pidpath, sizeof(pidpath), "%s/shard-db.pid", g_log_dir);
    FILE *f = fopen(pidpath, "r");
    if (!f) return -1;
    int pid, port;
    if (fscanf(f, "%d\n%d", &pid, &port) != 2) { fclose(f); return -1; }
    fclose(f);
    if (kill(pid, 0) != 0) return -1; /* not running */
    return port;
}

int main(int argc, char *argv[]) {
    g_out = stdout; /* CLI mode — output to terminal */
    if (argc < 2) {
        fprintf(stderr, "Usage: shard-db <command> [args...]\n");
        fprintf(stderr, "\nLifecycle:\n");
        fprintf(stderr, "  start                                Start server (PORT from db.env)\n");
        fprintf(stderr, "  stop                                 Graceful shutdown\n");
        fprintf(stderr, "  status                               Check if running\n");
        fprintf(stderr, "  server                               Start foreground (debug)\n");
        fprintf(stderr, "\nAll commands below require the server to be running:\n");
        fprintf(stderr, "  insert <object> <key> <value>        Insert/update a record\n");
        fprintf(stderr, "  get <object> <key>                   Get a record\n");
        fprintf(stderr, "  delete <object> <key>                Delete a record\n");
        fprintf(stderr, "  exists <object> <key>                Check if key exists\n");
        fprintf(stderr, "  size <object>                        Record count\n");
        fprintf(stderr, "  find <object> <criteria> [off] [lim] [fields]\n");
        fprintf(stderr, "  count <dir> <obj> [criteria_json]    Count records (criteria optional)\n");
        fprintf(stderr, "  aggregate <dir> <obj> <aggregates_json> [group_by_csv] [criteria_json] [having_json]\n");
        fprintf(stderr, "  keys <object> [offset] [limit]       List keys\n");
        fprintf(stderr, "  fetch <object> [off] [lim] [fields]  Paginated scan\n");
        fprintf(stderr, "  add-index <object> <field> [-f]      Build index\n");
        fprintf(stderr, "  remove-index <object> <field>        Drop index\n");
        fprintf(stderr, "  bulk-insert <object> [file]          Bulk insert JSON array\n");
        fprintf(stderr, "  bulk-delete <object> [file]          Bulk delete\n");
        fprintf(stderr, "  vacuum <object>                      Clean tombstones\n");
        fprintf(stderr, "  recount <object>                     Recalculate count\n");
        fprintf(stderr, "  truncate <object>                    Delete all data\n");
        fprintf(stderr, "  backup <object>                      Backup data\n");
        fprintf(stderr, "  put-file <dir> <object> <local-path> [--if-not-exists]\n");
        fprintf(stderr, "                                       Upload file (base64 over TCP)\n");
        fprintf(stderr, "  get-file <dir> <object> <filename> [<out-path>]\n");
        fprintf(stderr, "                                       Download file (base64 over TCP)\n");
        fprintf(stderr, "  delete-file <dir> <object> <filename> Remove a stored file\n");
        fprintf(stderr, "  get-file-path <object> <filename>    Get server-local file path\n");
        fprintf(stderr, "  export-schema [out_path]             Write JSON manifest of all schemas\n");
        fprintf(stderr, "  import-schema <in_path> [--if-not-exists]\n");
        fprintf(stderr, "                                       Replay manifest as create-object calls\n");
        fprintf(stderr, "\nSchema mutations (via JSON query):\n");
        fprintf(stderr, "  query '{\"mode\":\"rename-field\",\"dir\":\"...\",\"object\":\"...\",\"old\":\"...\",\"new\":\"...\"}'\n");
        fprintf(stderr, "  query '{\"mode\":\"remove-field\",\"dir\":\"...\",\"object\":\"...\",\"fields\":[\"f1\",\"f2\"]}'\n");
        fprintf(stderr, "  query '{\"mode\":\"add-field\",\"dir\":\"...\",\"object\":\"...\",\"fields\":[\"name:type[:param]\"]}'\n");
        fprintf(stderr, "  query '{\"mode\":\"vacuum\",\"dir\":\"...\",\"object\":\"...\",\"compact\":true,\"splits\":128}'\n");
        fprintf(stderr, "\nObject management (via JSON query):\n");
        fprintf(stderr, "  query '{\"mode\":\"create-object\",\"dir\":\"...\",\"object\":\"...\",\n");
        fprintf(stderr, "          \"fields\":[...],\"indexes\":[...],\n");
        fprintf(stderr, "          \"splits\":N,\"max_key\":N}'\n");
        fprintf(stderr, "\nJSON query mode:\n");
        fprintf(stderr, "  query '{\"mode\":\"get\",\"object\":\"users\",\"key\":\"k1\"}'\n");
        fprintf(stderr, "  query '{\"mode\":\"get\",\"object\":\"users\",\"keys\":[\"k1\",\"k2\"]}'\n");
        fprintf(stderr, "  query '{\"mode\":\"find\",\"object\":\"users\",\"criteria\":[...]}'\n");
        return 1;
    }

    const char *cmd = argv[1];

    /* JSON query mode */
    if (strcmp(cmd, "query") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: shard-db query '{\"mode\":\"...\", ...}'\n"); return 1; }
        char db_root[PATH_MAX];
        if (load_db_root(db_root, sizeof(db_root)) != 0) return 1;
        int port = read_server_port(db_root);
        if (port < 0) { fprintf(stderr, "{\"error\":\"Server not running\"}\n"); return 1; }
        return cmd_query_json(port, argv[2]);
    }

    /* Lifecycle commands — handled directly */
    if (strcmp(cmd, "start") == 0 || strcmp(cmd, "server") == 0 ||
        strcmp(cmd, "stop") == 0 || strcmp(cmd, "status") == 0) {
        char db_root[PATH_MAX];
        if (load_db_root(db_root, sizeof(db_root)) != 0) return 1;
        if (strcmp(cmd, "start") == 0) return cmd_server(db_root, 1);
        if (strcmp(cmd, "server") == 0) return cmd_server(db_root, 0);
        if (strcmp(cmd, "stop") == 0) return cmd_stop(db_root);
        if (strcmp(cmd, "status") == 0) return cmd_status(db_root);
    }

    /* All other commands — route through server via TCP */
    char db_root[PATH_MAX];
    if (load_db_root(db_root, sizeof(db_root)) != 0) return 1;

    int port = read_server_port(db_root);
    if (port < 0) {
        fprintf(stderr, "Error: Server not running. Start with: shard-db start\n");
        return 1;
    }

    /* Diagnostic shortcuts — build JSON query with format=table.
       Usage:
         shard-db shard-stats [dir] [object]
         shard-db stats
         shard-db db-dirs
         shard-db vacuum-check                   */
    if (strcmp(cmd, "shard-stats") == 0) {
        char json[512];
        if (argc >= 4)
            snprintf(json, sizeof(json),
                "{\"mode\":\"shard-stats\",\"dir\":\"%s\",\"object\":\"%s\",\"format\":\"table\"}",
                argv[2], argv[3]);
        else if (argc == 3)
            snprintf(json, sizeof(json),
                "{\"mode\":\"shard-stats\",\"dir\":\"%s\",\"format\":\"table\"}", argv[2]);
        else
            snprintf(json, sizeof(json), "{\"mode\":\"shard-stats\",\"format\":\"table\"}");
        return cmd_query_json(port, json);
    }

    /* reindex — rebuild every index for matching objects.
         shard-db reindex                   (all dirs × all objects)
         shard-db reindex <dir>             (all objects in one tenant)
         shard-db reindex <dir> <obj>       (one object) */
    if (strcmp(cmd, "reindex") == 0) {
        char json[512];
        if (argc >= 4)
            snprintf(json, sizeof(json),
                "{\"mode\":\"reindex\",\"dir\":\"%s\",\"object\":\"%s\"}",
                argv[2], argv[3]);
        else if (argc == 3)
            snprintf(json, sizeof(json),
                "{\"mode\":\"reindex\",\"dir\":\"%s\"}", argv[2]);
        else
            snprintf(json, sizeof(json), "{\"mode\":\"reindex\"}");
        return cmd_query_json(port, json);
    }
    /* drop <dir> <obj>  [--if-exists]   — remove object data + config entirely */
    if (strcmp(cmd, "drop") == 0 || strcmp(cmd, "drop-object") == 0) {
        if (argc < 4) {
            fprintf(stderr, "Usage: shard-db drop <dir> <obj> [--if-exists]\n");
            return 1;
        }
        int if_exists = (argc >= 5 && strcmp(argv[4], "--if-exists") == 0);
        char json[512];
        snprintf(json, sizeof(json),
            "{\"mode\":\"drop-object\",\"dir\":\"%s\",\"object\":\"%s\"%s}",
            argv[2], argv[3], if_exists ? ",\"if_exists\":true" : "");
        return cmd_query_json(port, json);
    }

    if (strcmp(cmd, "stats") == 0)
        return cmd_query_json(port, "{\"mode\":\"stats\",\"format\":\"table\"}");
    if (strcmp(cmd, "db-dirs") == 0)
        return cmd_query_json(port, "{\"mode\":\"db-dirs\"}");
    if (strcmp(cmd, "vacuum-check") == 0)
        return cmd_query_json(port, "{\"mode\":\"vacuum-check\"}");

    /* count <dir> <obj> [criteria_json] — debugging shortcut for the JSON query.
       criteria_json must be a JSON array like '[{"field":"age","op":"gt","value":"30"}]'.
       Empty/absent criteria returns the O(1) live count from metadata. */
    if (strcmp(cmd, "count") == 0) {
        if (argc < 4) {
            fprintf(stderr, "Usage: shard-db count <dir> <obj> [criteria_json]\n");
            return 1;
        }
        const char *crit = (argc >= 5 && argv[4][0]) ? argv[4] : NULL;
        size_t cap = strlen(argv[2]) + strlen(argv[3]) + (crit ? strlen(crit) : 0) + 128;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        if (crit)
            snprintf(json, cap,
                "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\",\"criteria\":%s}",
                argv[2], argv[3], crit);
        else
            snprintf(json, cap,
                "{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\"}",
                argv[2], argv[3]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }

    /* aggregate <dir> <obj> <aggregates_json> [group_by_csv] [criteria_json] [having_json]
       group_by accepts comma-separated field names ("status,region"); use empty string to
       skip a positional slot. */
    if (strcmp(cmd, "aggregate") == 0) {
        if (argc < 5) {
            fprintf(stderr,
                "Usage: shard-db aggregate <dir> <obj> <aggregates_json> "
                "[group_by_csv] [criteria_json] [having_json]\n");
            return 1;
        }
        const char *aggs = argv[4];
        const char *gb   = (argc >= 6 && argv[5][0]) ? argv[5] : NULL;
        const char *crit = (argc >= 7 && argv[6][0]) ? argv[6] : NULL;
        const char *hav  = (argc >= 8 && argv[7][0]) ? argv[7] : NULL;

        char *gb_json = NULL;
        if (gb) {
            size_t gblen = strlen(gb);
            gb_json = malloc(gblen * 2 + 16);
            if (!gb_json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
            char *p = gb_json;
            *p++ = '[';
            int first = 1;
            const char *s = gb;
            while (*s) {
                const char *end = strchr(s, ',');
                if (!end) end = s + strlen(s);
                while (s < end && (*s == ' ' || *s == '\t')) s++;
                const char *fe = end;
                while (fe > s && (fe[-1] == ' ' || fe[-1] == '\t')) fe--;
                if (fe > s) {
                    if (!first) *p++ = ',';
                    first = 0;
                    *p++ = '"';
                    memcpy(p, s, fe - s);
                    p += fe - s;
                    *p++ = '"';
                }
                if (!*end) break;
                s = end + 1;
            }
            *p++ = ']';
            *p = '\0';
        }

        size_t cap = strlen(argv[2]) + strlen(argv[3]) + strlen(aggs)
                   + (gb_json ? strlen(gb_json) : 0)
                   + (crit ? strlen(crit) : 0)
                   + (hav  ? strlen(hav)  : 0) + 256;
        char *json = malloc(cap);
        if (!json) { free(gb_json); fprintf(stderr, "Error: out of memory\n"); return 1; }
        int n = snprintf(json, cap,
            "{\"mode\":\"aggregate\",\"dir\":\"%s\",\"object\":\"%s\",\"aggregates\":%s",
            argv[2], argv[3], aggs);
        if (gb_json) n += snprintf(json + n, cap - n, ",\"group_by\":%s", gb_json);
        if (crit)    n += snprintf(json + n, cap - n, ",\"criteria\":%s", crit);
        if (hav)     n += snprintf(json + n, cap - n, ",\"having\":%s", hav);
        snprintf(json + n, cap - n, "}");

        int rc = cmd_query_json(port, json);
        free(json); free(gb_json);
        return rc;
    }

    /* File upload/download: route through dedicated TCP helpers (base64 in JSON).
       Usage:
         put-file <dir> <object> <local-path> [--if-not-exists]
         get-file <dir> <object> <filename>   [<out-path>] */
    if (strcmp(cmd, "put-file") == 0) {
        if (argc < 5) {
            fprintf(stderr, "Usage: shard-db put-file <dir> <object> <local-path> [--if-not-exists]\n");
            return 1;
        }
        int ine = 0;
        for (int i = 5; i < argc; i++) if (strcmp(argv[i], "--if-not-exists") == 0) ine = 1;
        return cmd_put_file_tcp(port, argv[2], argv[3], argv[4], ine);
    }
    if (strcmp(cmd, "get-file") == 0) {
        if (argc < 5) {
            fprintf(stderr, "Usage: shard-db get-file <dir> <object> <filename> [<out-path>]\n");
            return 1;
        }
        const char *out_path = (argc >= 6) ? argv[5] : NULL;
        return cmd_get_file_tcp(port, argv[2], argv[3], argv[4], out_path);
    }
    /* Schema migration — local→prod bootstrap. No data, no tokens.
         shard-db export-schema [out_path]                 (stdout if absent or "-")
         shard-db import-schema <in_path> [--if-not-exists] */
    if (strcmp(cmd, "export-schema") == 0) {
        const char *out = (argc >= 3) ? argv[2] : NULL;
        return cmd_export_schema(port, out);
    }
    if (strcmp(cmd, "import-schema") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: shard-db import-schema <in_path> [--if-not-exists]\n");
            return 1;
        }
        int ine = 0;
        for (int i = 3; i < argc; i++)
            if (strcmp(argv[i], "--if-not-exists") == 0) ine = 1;
        return cmd_import_schema(port, argv[2], ine);
    }
    if (strcmp(cmd, "delete-file") == 0) {
        if (argc < 5) {
            fprintf(stderr, "Usage: shard-db delete-file <dir> <object> <filename>\n");
            return 1;
        }
        size_t cap = strlen(argv[2]) + strlen(argv[3]) + strlen(argv[4]) + 128;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap,
            "{\"mode\":\"delete-file\",\"dir\":\"%s\",\"object\":\"%s\",\"filename\":\"%s\"}",
            argv[2], argv[3], argv[4]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }

    return cmd_query(port, argc - 1, argv + 1);
}
