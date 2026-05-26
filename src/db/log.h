/* shard-db logging framework — see
   docs/superpowers/specs/2026-05-24-logging-framework-design.md

   Every log line shape:
     YYYY-MM-DD HH:MM:SS LEVEL [subsystem] body

   File routing (`$LOG_DIR/<file>`):
     YYYY-MM-DD-error.log     LOG_ERROR
     YYYY-MM-DD-warn.log      LOG_WARN
     YYYY-MM-DD-info.log      LOG_INFO
     YYYY-MM-DD-debug.log     LOG_DEBUG (only when LOG_LEVEL=4)
     YYYY-MM-DD-audit.log     LOG_AUDIT — bypasses LOG_LEVEL filter
     slow-YYYY-MM-DD.log      log_slow_query — bypasses LOG_LEVEL filter
*/

#ifndef SHARD_DB_LOG_H
#define SHARD_DB_LOG_H

#include <stdint.h>

typedef enum {
    LOG_LVL_ERROR = 1,
    LOG_LVL_WARN  = 2,
    LOG_LVL_INFO  = 3,
    LOG_LVL_DEBUG = 4
} LogLevel;

/* Subsystem registry. Add #define here when introducing a new
   subsystem; nothing else needs updating. Keep values lowercase
   and stable — operators grep for `[subsystem]`. */
#define LOG_SUB_SERVER    "server"
#define LOG_SUB_SLOTCASK  "slotcask"
#define LOG_SUB_BTREE     "btree"
#define LOG_SUB_BITMAP    "bitmap"
#define LOG_SUB_TRIGRAM   "trigram"
#define LOG_SUB_INDEX     "index"
#define LOG_SUB_QUERY     "query"
#define LOG_SUB_PLANNER   "planner"
#define LOG_SUB_VACUUM    "vacuum"
#define LOG_SUB_WARMUP    "warmup"
#define LOG_SUB_AUTH      "auth"
#define LOG_SUB_TLS       "tls"
#define LOG_SUB_CONFIG    "config"
#define LOG_SUB_REINDEX   "reindex"
#define LOG_SUB_SLOW      "slow"

/* Call-site macros. Compiler enforces level + subsystem at every site. */
#define LOG_ERROR(sub, fmt, ...) log_msg_sub(LOG_LVL_ERROR, (sub), (fmt), ##__VA_ARGS__)
#define LOG_WARN(sub, fmt, ...)  log_msg_sub(LOG_LVL_WARN,  (sub), (fmt), ##__VA_ARGS__)
#define LOG_INFO(sub, fmt, ...)  log_msg_sub(LOG_LVL_INFO,  (sub), (fmt), ##__VA_ARGS__)
#define LOG_DEBUG(sub, fmt, ...) log_msg_sub(LOG_LVL_DEBUG, (sub), (fmt), ##__VA_ARGS__)
#define LOG_AUDIT(sub, fmt, ...) log_audit_sub((sub), (fmt), ##__VA_ARGS__)

void log_msg_sub(int level, const char *subsystem, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));
void log_audit_sub(const char *subsystem, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

/* Slow-query log — public signature kept identical (callers don't
   know it goes through the ring buffer now). Bypasses LOG_LEVEL
   because the SLOW_QUERY_MS threshold is the filter. */
void log_slow_query(const char *mode, const char *dir,
                    const char *object, const char *query, uint32_t duration_ms);


#endif /* SHARD_DB_LOG_H */
