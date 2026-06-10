# PLAN_NOTES.md — Corrections applied during embedded mode implementation

## Struct definition fixes (shard_db_internal.h)

These struct definitions in the plan did not match the actual source code.
Corrected to match source:

1. **KfCacheEntry** (slotcask.c):
   - Field `base` (uint8_t*) not `map` — matches source
   - Added `size_t capacity` field — present in source

2. **RegEntry** (slotcask.c):
   - `key[PATH_MAX]` not `key[512]` — matches source
   - Added `SlotcaskDb *db` field — present in source

3. **CountsCacheEntry** (storage.c):
   - Fields: `_Atomic int64_t live, deleted`, `_Atomic uint64_t pending_writes`, `_Atomic int used`
   - Plan had `int live, int deleted, int valid, uint64_t last_access` — completely wrong

4. **FieldsCache** (config.c):
   - `name[256]` not `name[512]` — matches source
   - Field order: `name`, `fields`, `nfields`, `used` — not `name`, `nfields`, `fields`, `used`

5. **IdxCache** (config.c):
   - Fields: `char name[512]`, `char fields[MAX_FIELDS][256]`, `enum IndexType types[MAX_FIELDS]`, `int nfields`, `int used`
   - Plan had `index_names[MAX_FIELDS][256]` and `index_count` — completely wrong struct

6. **TypedCacheEntry** (config.c):
   - Field name `schema` not `ts` — matches source

## Function call fixes (embedded.c)

1. **`load_dirs()`** takes no arguments (declared `void load_dirs(void)`)
   - Plan called `load_dirs(db->db_root)` — fixed to `load_dirs()`

## Extern declaration caveat (types.h, Task 2)

1. **Anchor block 1** in the plan includes `g_max_threads` and `g_pool_chunk` in the removal list,
   but these are process-global (parallel.c) and NOT moved to ShardDb. Their externs must remain.
   - Only `g_timeout`, `g_port`, `g_workers`, `g_io_threads`, `g_index_page_size`,
     `g_global_limit`, `g_max_concurrent_queries` are removed.
   - `g_max_threads` and `g_pool_chunk` are kept.
