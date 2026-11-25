# Phase 1 steps 1-2 — lint + analyzer triage (2026-11-25)

Raw output: `lint-raw.log` (`BUILD_MODE=lint ./build.sh`: -Wall -Wextra
-Wconversion -Wshadow, no -Wno-*), `clang-tidy-util-config.log`,
`gcc-fanalyzer-util-config-server.log`. Nothing here is a confirmed
data-loss defect. CI history (CodeQL, cppcheck, scan-build) is green on
f19b17d; Coverity is fully triaged by the human (0 open).

## Lint build: 584 unique warnings (3774 raw lines)

| Class | Count | Disposition rule |
|---|---|---|
| -Wsign-conversion | 468 (config.c 83, query_bulk.c 82, btree.c 37) | **benign-by-design**: int/size_t mixing on bounded lengths. Sampled 20 across config.c, query_bulk.c, btree.c: all index/length values bounded by page size, MAX_* constants or buffer sizes. |
| -Wconversion | 78 | **benign-by-design** (BE byte packing into uint8_t, page-bounded uint16 slot offsets, strlen into bounded buffers, ms/sec double math). Sampled btree.c:1368-1408/1723-1733, index.c:1169/1224/1260, query_find.c:372/398, query_aggregate.c:1468/2957, query_plan.c:1177. |
| -Wunused-parameter | 18 | benign (callbacks / seams). |
| -Wshadow | 16 | benign: inner-scope reuse (query_bulk.c:1739/4479/5122, query_schema.c:1457, slotcask.c:7012/7016/8586); no semantic bug found. |
| -Wmaybe-uninitialized | 2 (index.c:1301) | **false positive**: `sync_fields/sync_types` are filled for i<used before the call; the call is guarded by `if(used)`. |
| -Wdeprecated-declarations | 2 | OpenSSL 4.0 `SSL_set1_host` (tls.c:126, cli/conn.c:186): future-compat item, not a defect. |

## Analyzer runs (parser/dispatch TUs: util.c, config.c, server.c)

gcc -fanalyzer: util 0, config 3, server 1.
- FIXED (this commit): config.c `typed_get_field_str` varchar case, unchecked `malloc` (NULL returned on OOM); server.c `g_worker_cfds` unchecked `malloc` (startup exits with a message).
- config.c:4979/5088 `rd_line` "uninitialized": false positive (set by `conf_read_line` on every path that reaches the use).

clang-tidy (`bugprone-*,clang-analyzer-*,concurrency-*,cert-*`) on util.c/config.c: 1181 raw; noise classes (signed-bitwise 429, DeprecatedOrUnsafeBufferHandling 320, cert-err33-c 216, mt-unsafe 30, easily-swappable 38) dispositioned **benign-by-design**. ArrayBound x10 on `line[strcspn(...)]` after bounded `fgets` and `read_file` (`buf[got]`): false positives; `BlockInCriticalSection` x3: dirs.conf `fgets` under `g_dirs_lock` is intentional (load-time). `unix.Malloc` x5 (config.c:2131, 4986, 5009, 5105): paths free via the `fail:` label; no leak found on read.
60x `cert-err34-c` (atoi/strtol without error checks on config values): **needs-look** in the dispatch/auth slice.

clang-tidy clang-analyzer subset on server.c, nql.c, schema_wire.c, query_bulk.c:
- **needs-look, bulk-insert slice (OOM-only, not DL):** query_bulk.c `arena_new` result (1678, 2444) and `calloc(records)` unchecked; `arena_alloc` results used unchecked (1792 memset, 1763/1779/2487/2502/2520 ids, 1812/2577 payload); analyzer null-derefs at 2006, 2789, 3368, 4221, 4790, 5989, 6465. A failed allocation crashes the daemon instead of returning an error.
- query_bulk.c:548 `free` "offset by 32" — `bw_dealloc` deliberate header offset; false positive. 5597/5819 dead stores `obj_heap`: cleanup candidate.
- nql.c:494/505/556/562 "garbage value" on `ferr[0]`: `ferr` is only read when `nql_parse_filter` returned NULL, which writes it on that path — verify in the NQL slice.
- server.c:2748 stream position after failed op; server.c:118 ArrayBound on bounded line: false positives pending the dispatch slice.
