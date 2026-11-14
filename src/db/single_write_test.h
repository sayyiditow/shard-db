#ifndef SHARD_SINGLE_WRITE_TEST_H
#define SHARD_SINGLE_WRITE_TEST_H
#ifdef TEST_BUILD
#include "types.h"
#include "slotcask.h"
/* Caller owns immutable schema/db until this synchronous call returns.
   No loader, registry publication, schema-cache mutation or activation. */
int shard_test_single_insert(const char *root, const char *object,
    const char *key, size_t klen, const char *json, const char *condition,
    int only_new, int return_record, const Schema *sc, TypedSchema *ts,
    SlotcaskDb *db);
int shard_test_single_update(const char *root, const char *object,
    const char *key, size_t klen, const char *json, const char *condition,
    int dry_run, int return_record, const Schema *sc, TypedSchema *ts,
    SlotcaskDb *db);
#endif
#endif
