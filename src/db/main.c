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
        fprintf(stderr, "  size <object>                        Live record count (O(1) metadata)\n");
        fprintf(stderr, "  orphaned <object>                    Tombstoned slot count (vacuum reclaims)\n");
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
        fprintf(stderr, "  restore <object> <timestamp> [--force]\n");
        fprintf(stderr, "                                       Restore from <obj>/backup/<timestamp>/\n");
        fprintf(stderr, "  put-file <dir> <object> <local-path> [--if-not-exists]\n");
        fprintf(stderr, "                                       Upload file (base64 over TCP)\n");
        fprintf(stderr, "  get-file <dir> <object> <filename> [<out-path>]\n");
        fprintf(stderr, "                                       Download file (base64 over TCP)\n");
        fprintf(stderr, "  delete-file <dir> <object> <filename> Remove a stored file\n");
        fprintf(stderr, "  list-files <dir> <object> [pattern] [offset] [limit] [--match=<mode>]\n");
        fprintf(stderr, "                                       List stored files (alphabetical, paginated)\n");
        fprintf(stderr, "                                       --match=prefix (default) | suffix | contains | glob\n");
        fprintf(stderr, "  get-file-path <object> <filename>    Get server-local file path\n");
        fprintf(stderr, "  export-schema [out_path]             Write JSON manifest of all schemas\n");
        fprintf(stderr, "  import-schema <in_path> [--if-not-exists]\n");
        fprintf(stderr, "                                       Replay manifest as create-object calls\n");
        fprintf(stderr, "\nSchema mutations:\n");
        fprintf(stderr, "  edit-field <dir> <object> <name:type[:param]>   Same-type single-field edit\n");
        fprintf(stderr, "  remove-field <dir> <object> <field>             Soft-remove (tombstone)\n");
        fprintf(stderr, "  rename-field <dir> <object> <old> <new>         Rename a field\n");
        fprintf(stderr, "  query '{\"mode\":\"add-field\",\"dir\":\"...\",\"object\":\"...\",\"fields\":[\"name:type[:param]\"]}'\n");
        fprintf(stderr, "                                                   (add-field is JSON-only; batch + computed defaults)\n");
        fprintf(stderr, "  query '{\"mode\":\"vacuum\",\"dir\":\"...\",\"object\":\"...\",\"compact\":true,\"splits\":128}'\n");
        fprintf(stderr, "\nDir / object management:\n");
        fprintf(stderr, "  add-dir <dir>                        Register a tenant directory\n");
        fprintf(stderr, "  remove-dir <dir> [--force]           Remove a tenant directory (--force = ignore non-empty)\n");
        fprintf(stderr, "  list-objects <dir>                   List objects in a dir\n");
        fprintf(stderr, "  describe <dir> <object>              Show object schema\n");
        fprintf(stderr, "  query '{\"mode\":\"create-object\",\"dir\":\"...\",\"object\":\"...\",\n");
        fprintf(stderr, "          \"fields\":[...],\"indexes\":[...],\"splits\":N,\"max_key\":N}'\n");
        fprintf(stderr, "                                                   (JSON-only; create-object has too many params for a CLI shortcut)\n");
        fprintf(stderr, "\nAuth admin:\n");
        fprintf(stderr, "  add-token <token> [--dir <d>] [--object <o>] [--perm r|rw|rwx]\n");
        fprintf(stderr, "                                       Add a token (global / tenant / object scope)\n");
        fprintf(stderr, "  remove-token <token>                 Remove a token\n");
        fprintf(stderr, "  list-tokens [--dir <d>] [--object <o>]   List tokens at the given scope\n");
        fprintf(stderr, "  add-ip <ip>                          Add trusted IP\n");
        fprintf(stderr, "  remove-ip <ip>                       Remove trusted IP\n");
        fprintf(stderr, "  list-ips                             List trusted IPs\n");
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

    /* migrate-files moved to the standalone ./migrate binary in 2026.05.1.
       Redirect before the server check so the message lands whether the
       daemon is running or not. */
    if (strcmp(cmd, "migrate-files") == 0) {
        fprintf(stderr,
            "shard-db: 'migrate-files' moved to ./migrate in 2026.05.1.\n"
            "          Stop the daemon and run ./migrate instead.\n");
        return 1;
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
         shard-db stats-prom                       (Prometheus text-format exposition)
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
         shard-db reindex <dir> <obj>       (one object)
         --composites-only                  only rebuild composite indexes (fields containing '+') */
    if (strcmp(cmd, "reindex") == 0) {
        /* Pre-scan for --composites-only flag */
        int composites_only = 0;
        for (int i = 2; i < argc; i++) {
            if (strcmp(argv[i], "--composites-only") == 0) {
                composites_only = 1;
                break;
            }
        }
        /* Collect positional arguments (non-flag) */
        const char *pos[2] = {NULL, NULL};
        int npos = 0;
        for (int i = 2; i < argc && npos < 2; i++) {
            if (argv[i][0] == '-' && argv[i][1] == '-') continue;
            pos[npos++] = argv[i];
        }
        char json[512];
        if (npos == 2)
            snprintf(json, sizeof(json),
                "{\"mode\":\"reindex\",\"dir\":\"%s\",\"object\":\"%s\"%s}",
                pos[0], pos[1],
                composites_only ? ",\"composites_only\":true" : "");
        else if (npos == 1)
            snprintf(json, sizeof(json),
                "{\"mode\":\"reindex\",\"dir\":\"%s\"%s}",
                pos[0],
                composites_only ? ",\"composites_only\":true" : "");
        else
            snprintf(json, sizeof(json),
                "{\"mode\":\"reindex\"%s}",
                composites_only ? ",\"composites_only\":true" : "");
        return cmd_query_json(port, json);
    }
    /* explain find|count|aggregate <dir> <obj> <criteria> [order_by]
       Sends explain:true on the named mode to the running server.
       shard-db explain find  <dir> <obj> '[{"field":"score","op":"gt","value":"50"}]'
       shard-db explain count <dir> <obj> '[{"field":"active","op":"eq","value":"true"}]'
       shard-db explain aggregate <dir> <obj> '[...]' [order_by_field] */
    if (strcmp(cmd, "explain") == 0) {
        if (argc < 6) {
            fprintf(stderr, "Usage: shard-db explain find|count|aggregate <dir> <obj> <criteria> [order_by]\n");
            return 1;
        }
        const char *subcmd  = argv[2];
        const char *dir     = argv[3];
        const char *object  = argv[4];
        const char *criteria = argv[5];
        const char *order_by = (argc > 6) ? argv[6] : NULL;

        if (strcmp(subcmd, "find") != 0 && strcmp(subcmd, "count") != 0 &&
            strcmp(subcmd, "aggregate") != 0) {
            fprintf(stderr, "Unknown explain subcommand: %s (expected find, count, or aggregate)\n", subcmd);
            return 1;
        }

        char json[4096];
        if (order_by) {
            snprintf(json, sizeof(json),
                "{\"mode\":\"%s\",\"dir\":\"%s\",\"object\":\"%s\","
                "\"criteria\":%s,\"order_by\":\"%s\",\"explain\":true}",
                subcmd, dir, object, criteria, order_by);
        } else {
            snprintf(json, sizeof(json),
                "{\"mode\":\"%s\",\"dir\":\"%s\",\"object\":\"%s\","
                "\"criteria\":%s,\"explain\":true}",
                subcmd, dir, object, criteria);
        }
        return cmd_query_json(port, json);
    }
    /* estimate-index <dir> <obj> <field>:trigram — sample 1024 records,
       project on-disk size for a hypothetical trigram index. Lets ops
       budget honestly before committing to add-index. */
    if (strcmp(cmd, "estimate-index") == 0) {
        if (argc < 5) {
            fprintf(stderr, "Usage: shard-db estimate-index <dir> <obj> <field>:trigram\n");
            return 1;
        }
        char json[1024];
        snprintf(json, sizeof(json),
            "{\"mode\":\"estimate-index\",\"dir\":\"%s\",\"object\":\"%s\",\"spec\":\"%s\"}",
            argv[2], argv[3], argv[4]);
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
    if (strcmp(cmd, "stats-prom") == 0)
        return cmd_query_json(port, "{\"mode\":\"stats-prom\"}");
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
    /* Schema mutation shortcut: single-field edit-field. JSON form
       covers batch edits. */
    if (strcmp(cmd, "edit-field") == 0) {
        if (argc < 5) {
            fprintf(stderr,
                "Usage: shard-db edit-field <dir> <object> <field-spec>\n"
                "       (spec uses the same form as add-field, e.g. 'name:varchar:200')\n"
                "       Batch edits: use 'shard-db query' with mode=edit-field + fields:[...].\n");
            return 1;
        }
        size_t cap = strlen(argv[2]) + strlen(argv[3]) + strlen(argv[4]) + 128;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap,
            "{\"mode\":\"edit-field\",\"dir\":\"%s\",\"object\":\"%s\",\"fields\":[\"%s\"]}",
            argv[2], argv[3], argv[4]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
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
    if (strcmp(cmd, "list-files") == 0) {
        if (argc < 4) {
            fprintf(stderr,
                "Usage: shard-db list-files <dir> <object> [pattern] [offset] [limit] "
                "[--match=<prefix|suffix|contains|glob>]\n");
            return 1;
        }
        const char *match = "prefix";
        const char *pos[3] = {"", "0", "0"}; /* pattern, offset, limit */
        int npos = 0;
        for (int i = 4; i < argc; i++) {
            if (strncmp(argv[i], "--match=", 8) == 0) match = argv[i] + 8;
            else if (npos < 3) pos[npos++] = argv[i];
        }
        size_t cap = strlen(argv[2]) + strlen(argv[3]) + strlen(pos[0])
                   + strlen(match) + strlen(pos[1]) + strlen(pos[2]) + 192;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap,
            "{\"mode\":\"list-files\",\"dir\":\"%s\",\"object\":\"%s\","
            "\"pattern\":\"%s\",\"match\":\"%s\",\"offset\":%s,\"limit\":%s}",
            argv[2], argv[3], pos[0], match, pos[1], pos[2]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }

    /* ===== Admin shortcuts — operations that were previously only
       reachable via `shard-db query '{"mode":"..."}'`.  Localhost is
       trusted by default, so the CLI sends the JSON and echoes the
       daemon's response.  Remote operators still need to craft JSON
       with an `auth` field via `query`. */

    if (strcmp(cmd, "add-dir") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: shard-db add-dir <dir>\n"); return 1; }
        size_t cap = strlen(argv[2]) + 64;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap, "{\"mode\":\"add-dir\",\"dir\":\"%s\"}", argv[2]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }
    if (strcmp(cmd, "remove-dir") == 0) {
        if (argc < 3) {
            fprintf(stderr,
                "Usage: shard-db remove-dir <dir> [--force]\n"
                "       --force removes even if the dir still has objects\n");
            return 1;
        }
        int force = 0;
        for (int i = 3; i < argc; i++)
            if (strcmp(argv[i], "--force") == 0) force = 1;
        size_t cap = strlen(argv[2]) + 96;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap,
            "{\"mode\":\"remove-dir\",\"dir\":\"%s\",\"check_empty\":%s}",
            argv[2], force ? "false" : "true");
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }
    if (strcmp(cmd, "list-objects") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: shard-db list-objects <dir>\n"); return 1; }
        size_t cap = strlen(argv[2]) + 64;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap, "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", argv[2]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }
    if (strcmp(cmd, "describe-object") == 0 || strcmp(cmd, "describe") == 0) {
        if (argc < 4) { fprintf(stderr, "Usage: shard-db describe <dir> <object>\n"); return 1; }
        size_t cap = strlen(argv[2]) + strlen(argv[3]) + 96;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap,
            "{\"mode\":\"describe-object\",\"dir\":\"%s\",\"object\":\"%s\"}",
            argv[2], argv[3]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }

    if (strcmp(cmd, "add-token") == 0) {
        if (argc < 3) {
            fprintf(stderr,
                "Usage: shard-db add-token <token> [--dir <d>] [--object <o>] [--perm r|rw|rwx]\n"
                "       --dir omitted = global token; --object requires --dir\n"
                "       --perm default rwx\n");
            return 1;
        }
        const char *token = argv[2];
        const char *dir = NULL, *obj = NULL, *perm = NULL;
        for (int i = 3; i < argc - 1; i++) {
            if (strcmp(argv[i], "--dir") == 0)         dir  = argv[++i];
            else if (strcmp(argv[i], "--object") == 0) obj  = argv[++i];
            else if (strcmp(argv[i], "--perm") == 0)   perm = argv[++i];
        }
        char dir_part[256] = "", obj_part[256] = "", perm_part[64] = "";
        if (dir)  snprintf(dir_part,  sizeof(dir_part),  ",\"dir\":\"%s\"", dir);
        if (obj)  snprintf(obj_part,  sizeof(obj_part),  ",\"object\":\"%s\"", obj);
        if (perm) snprintf(perm_part, sizeof(perm_part), ",\"perm\":\"%s\"", perm);
        size_t cap = strlen(token) + sizeof(dir_part) + sizeof(obj_part) + sizeof(perm_part) + 64;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap,
            "{\"mode\":\"add-token\",\"token\":\"%s\"%s%s%s}",
            token, dir_part, obj_part, perm_part);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }
    if (strcmp(cmd, "remove-token") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: shard-db remove-token <token>\n"); return 1; }
        size_t cap = strlen(argv[2]) + 64;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap, "{\"mode\":\"remove-token\",\"token\":\"%s\"}", argv[2]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }
    if (strcmp(cmd, "list-tokens") == 0) {
        const char *dir = NULL, *obj = NULL;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--dir") == 0)         dir = argv[++i];
            else if (strcmp(argv[i], "--object") == 0) obj = argv[++i];
        }
        char dir_part[256] = "", obj_part[256] = "";
        if (dir) snprintf(dir_part, sizeof(dir_part), ",\"dir\":\"%s\"", dir);
        if (obj) snprintf(obj_part, sizeof(obj_part), ",\"object\":\"%s\"", obj);
        size_t cap = sizeof(dir_part) + sizeof(obj_part) + 64;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap, "{\"mode\":\"list-tokens\"%s%s}", dir_part, obj_part);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }

    if (strcmp(cmd, "add-ip") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: shard-db add-ip <ip>\n"); return 1; }
        size_t cap = strlen(argv[2]) + 64;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap, "{\"mode\":\"add-ip\",\"ip\":\"%s\"}", argv[2]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }
    if (strcmp(cmd, "remove-ip") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: shard-db remove-ip <ip>\n"); return 1; }
        size_t cap = strlen(argv[2]) + 64;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap, "{\"mode\":\"remove-ip\",\"ip\":\"%s\"}", argv[2]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }
    if (strcmp(cmd, "list-ips") == 0) {
        return cmd_query_json(port, "{\"mode\":\"list-ips\"}");
    }

    if (strcmp(cmd, "remove-field") == 0) {
        if (argc < 5) {
            fprintf(stderr,
                "Usage: shard-db remove-field <dir> <object> <field>\n"
                "       Soft-removes (tombstones) the field. Batch via JSON query.\n");
            return 1;
        }
        size_t cap = strlen(argv[2]) + strlen(argv[3]) + strlen(argv[4]) + 96;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap,
            "{\"mode\":\"remove-field\",\"dir\":\"%s\",\"object\":\"%s\",\"fields\":[\"%s\"]}",
            argv[2], argv[3], argv[4]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }
    if (strcmp(cmd, "rename-field") == 0) {
        if (argc < 6) {
            fprintf(stderr, "Usage: shard-db rename-field <dir> <object> <old> <new>\n");
            return 1;
        }
        size_t cap = strlen(argv[2]) + strlen(argv[3]) + strlen(argv[4]) + strlen(argv[5]) + 96;
        char *json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory\n"); return 1; }
        snprintf(json, cap,
            "{\"mode\":\"rename-field\",\"dir\":\"%s\",\"object\":\"%s\",\"old\":\"%s\",\"new\":\"%s\"}",
            argv[2], argv[3], argv[4], argv[5]);
        int rc = cmd_query_json(port, json);
        free(json);
        return rc;
    }

    return cmd_query(port, argc - 1, argv + 1);
}
