#ifndef SHARD_DB_VERSION_H
#define SHARD_DB_VERSION_H

/* Compiled-in release metadata, CalVer yyyy.mm.N — see
   docs/reference/changelog.md for the scheme. Bump these in the same
   commit as every new docs/release-notes/<ver>.md + changelog.md entry.
   This release records a minimum supported source version for operators,
   but treats it as informational and does not enforce it: earlier releases
   did not write .version. */
#define SHARD_DB_VERSION "2026.08.1"
#define SHARD_DB_MIN_VERSION "2026.07.3"
#define SHARD_DB_ENFORCE_MIN_VERSION 0
#define SHARD_DB_HAS_STARTUP_MIGRATION 1

#define SHARD_DB_VERSION_FILE_OK      0
#define SHARD_DB_VERSION_FILE_MISSING 1
#define SHARD_DB_VERSION_FILE_ERROR  (-1)

enum ShardDbVersionDecision {
    SHARD_DB_VERSION_NOOP = 0,
    SHARD_DB_VERSION_RUN_MIGRATION = 1,
    SHARD_DB_VERSION_STAMP_ONLY = 2,
    SHARD_DB_VERSION_TOO_OLD = -3,
    SHARD_DB_VERSION_DOWNGRADE = -2,
    SHARD_DB_VERSION_INVALID = -4
};

#endif
