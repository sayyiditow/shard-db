#ifndef MIGRATE_H
#define MIGRATE_H

/* migrate-files: lift pre-2026.05.2 XX/XX hash-bucketed file storage to flat
   <obj>/files/<name>. Walks every (dir, object) in <db_root>/schema.conf.
   Idempotent — re-running after a successful migration finds nothing to move.
   Emits a one-line JSON summary to stdout.
   Returns 0 on success, nonzero on a fatal error (cannot read schema.conf). */
int migrate_files(const char *db_root);

#endif
