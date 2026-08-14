#ifndef SHARD_DB_VERSION_H
#define SHARD_DB_VERSION_H

/* Compiled-in release metadata, CalVer yyyy.mm.N — see
   docs/reference/changelog.md for the scheme. */
#define SHARD_DB_VERSION "2026.08.2"
#define SHARD_DB_REQUIRED_SOURCE_VERSION "2026.08.1"

#define SHARD_DB_VERSION_FILE_OK       0
#define SHARD_DB_VERSION_FILE_MISSING  1
#define SHARD_DB_VERSION_FILE_ERROR   (-1)

enum ShardDbVersionStampResult {
    SHARD_DB_VERSION_STAMP_OK = 0,
    SHARD_DB_VERSION_STAMP_FAILED = -1,
    SHARD_DB_VERSION_STAMP_UNCERTAIN = -2
};

enum ShardDbVersionDecision {
    SHARD_DB_VERSION_NOOP = 0,
    SHARD_DB_VERSION_STAMP = 1,
    SHARD_DB_VERSION_TOO_OLD = -3,
    SHARD_DB_VERSION_DOWNGRADE = -2,
    SHARD_DB_VERSION_INVALID = -4
};

#endif
