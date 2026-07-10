#!/bin/bash
set -e

# OpenSSL detection — Linux uses system paths; macOS uses Homebrew openssl@3
# (Apple's bundled LibreSSL has no public headers and is deprecated for app use).
# Override with OPENSSL_PREFIX=... if you installed elsewhere (MacPorts, source).
OSSL_CFLAGS=""
OSSL_LDFLAGS=""
case "$(uname)" in
    Darwin)
        if [ -n "$OPENSSL_PREFIX" ]; then
            OSSL="$OPENSSL_PREFIX"
        elif command -v brew >/dev/null 2>&1; then
            OSSL="$(brew --prefix openssl@3 2>/dev/null || true)"
        fi
        if [ -z "$OSSL" ] || [ ! -d "$OSSL/include" ]; then
            echo "build.sh: OpenSSL not found. brew install openssl@3 or set OPENSSL_PREFIX=/path/to/openssl" >&2
            exit 1
        fi
        OSSL_CFLAGS="-I$OSSL/include"
        OSSL_LDFLAGS="-L$OSSL/lib"
        ;;
    *)
        # Linux — rely on system libssl-dev / openssl-devel
        :
        ;;
esac

# BUILD_MODE selects compilation flavour. Default `release` is what ships;
# the others are for CI sanitizer runs and never produce a stripped binary.
#   release - -O2 -flto, stripped (default; what users get)
#   asan    - -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
#   tsan    - -O1 -g -fsanitize=thread        -fno-omit-frame-pointer
#   debug   - -O0 -g (no sanitizers; just for stepping in gdb)
# The sanitizer modes use -O1 (not -O2) because aggressive optimisation
# sometimes hides the very bugs the sanitizer is meant to find.
# Common warning flags: -Wall -Wextra catch a wide net of real bugs (typos
# in conditionals, sign-mixing in arithmetic, dead stores). We disable
# -Wformat-truncation because GCC fires it conservatively on every
# `snprintf(buf[PATH_MAX], "%s/x", path)` even though snprintf truncates
# safely and we never act on the truncated result. The CodeQL +
# scan-build + cppcheck CI workflows catch real format-string issues
# more reliably than -Wformat-truncation does.
WARN_CFLAGS="-Wall -Wextra -Wno-format-truncation -Wno-unused-parameter -Wno-address-of-packed-member -Wno-unused-result"

BUILD_MODE="${BUILD_MODE:-release}"
# BUILD_MARCH (opt-in): pass through as -march=$BUILD_MARCH. Defaults to
# unset (portable baseline) so prebuilt binaries shipped from CI run on
# any x86-64 / aarch64 host. Self-built deployments can set
# BUILD_MARCH=native (or e.g. x86-64-v3) for MOVBE/BMI2/AVX2 codegen at
# the cost of CPU-microarchitecture portability.
MARCH_CFLAGS=""
[ -n "$BUILD_MARCH" ] && MARCH_CFLAGS="-march=$BUILD_MARCH"
case "$BUILD_MODE" in
    release)
        # -ffunction-sections/-fdata-sections put each symbol in its own
        # section so the linker can drop unreferenced ones. Linux uses
        # --gc-sections; macOS ld64 uses -dead_strip (the section flags are
        # harmless there). This is on top of -flto + strip.
        GC_CFLAGS="-ffunction-sections -fdata-sections"
        if [ "$(uname)" = "Darwin" ]; then
            GC_LDFLAGS="-Wl,-dead_strip"
        else
            GC_LDFLAGS="-Wl,--gc-sections"
        fi
        MODE_CFLAGS="-O2 -g -fno-omit-frame-pointer -flto=auto $GC_CFLAGS $MARCH_CFLAGS $WARN_CFLAGS"
        MODE_LDFLAGS="-flto=auto $GC_LDFLAGS"
        DO_STRIP=${DO_STRIP:-1}
        ;;
    asan)
        MODE_CFLAGS="-O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined -fno-PIE -no-pie $WARN_CFLAGS"
        MODE_LDFLAGS="-fsanitize=address,undefined -no-pie"
        DO_STRIP=0
        ;;
    tsan)
        MODE_CFLAGS="-O1 -g -fno-omit-frame-pointer -fsanitize=thread -fno-PIE -no-pie $WARN_CFLAGS"
        MODE_LDFLAGS="-fsanitize=thread -no-pie"
        DO_STRIP=0
        ;;
    debug)
        MODE_CFLAGS="-O0 -g -fno-PIE -no-pie $WARN_CFLAGS"
        MODE_LDFLAGS="-no-pie"
        DO_STRIP=0
        ;;
    coverage)
        # gcov-style line/branch coverage. Each compiled .o gets a sibling
        # .gcno (control-flow graph); each test run produces .gcda counters.
        # Codecov collects both via lcov.
        #
        # -fprofile-update=atomic is required because the daemon spawns
        # parallel-for worker threads that hit the same .gcda counters
        # concurrently. Without atomics, racing increments produce
        # impossible negative deltas and gcov refuses to read the data
        # ("Unexpected negative count for branch ..." in lcov).
        MODE_CFLAGS="-O0 -g --coverage -fprofile-arcs -ftest-coverage -fprofile-update=atomic $WARN_CFLAGS"
        MODE_LDFLAGS="--coverage"
        DO_STRIP=0
        ;;
    *)
        echo "build.sh: unknown BUILD_MODE=$BUILD_MODE (release|asan|tsan|debug|coverage)" >&2
        exit 1
        ;;
esac

# -flto: link-time optimization (cross-TU inlining; usually helps perf, definitely
#        shrinks the binary by eliminating dead code visible only across files).
# strip: remove symbol/debug tables from the shipped binary (~25K cut). Skipped
#        for sanitizer/debug builds — symbols are needed for readable stack traces.
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db

# libshard-db.a — embedded mode static library (all daemon sources except main.c)
LIB_SRCS="src/db/util.c src/db/parallel.c src/db/storage.c src/db/index.c \
          src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c \
          src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c \
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/type_desc.c \
          src/db/nql.c src/db/embedded.c"
LIB_OBJS=""
mkdir -p build/bin build/obj
for f in $LIB_SRCS; do
    obj="build/obj/$(basename "${f%.c}").o"
    gcc $MODE_CFLAGS -c "$f" -Isrc/db $OSSL_CFLAGS -o "$obj"
    LIB_OBJS="$LIB_OBJS $obj"
done
ar rcs build/bin/libshard-db.a $LIB_OBJS
cp src/db/shard_db.h build/bin/shard_db.h
echo "  -> build/bin/libshard-db.a + build/bin/shard_db.h"

# shard-cli — separate ncurses TUI client. Links the same OpenSSL but no
# pthread/daemon code. Self-contained connection helper in src/cli/conn.c.
# ncurses lib name: Linux has libncursesw (wide-char); macOS's bundled
# ncurses is built with wide-char baked into -lncurses, no -w suffix.
NCURSES_LDFLAGS="-lncursesw"
case "$(uname)" in
    Darwin) NCURSES_LDFLAGS="-lncurses" ;;
esac
gcc $MODE_CFLAGS -o shard-cli src/cli/main.c src/cli/widgets.c src/cli/views.c src/cli/conn.c -Isrc/cli $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS $NCURSES_LDFLAGS -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-cli

# shard-db-test — TAP-style C test runner. Links daemon's JSON helpers
# (src/db/util.c) for response parsing; otherwise self-contained (TCP/TLS
# client + assertion macros + per-test daemon fixtures). Future test cases
# under src/test/cases/ get listed here.
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test \
    src/test/shard-db-test.c \
    src/test/test_client.c \
    src/test/test_runner.c \
    src/test/fixtures.c \
    src/test/cases/test_or_logic.c \
    src/test/cases/test_crash_safety.c \
    src/test/cases/test_rename_field.c \
    src/test/cases/test_edit_field.c \
    src/test/cases/test_edit_field_polish.c \
    src/test/cases/test_add_field_computed_defaults.c \
    src/test/cases/test_auto_key.c \
    src/test/cases/test_auto_create.c \
    src/test/cases/test_object_name_validation.c \
    src/test/cases/test_auto_key_multi.c \
    src/test/cases/test_varchar_overflow.c \
    src/test/cases/test_bulk_upsert.c \
    src/test/cases/test_joins.c \
    src/test/cases/test_describe.c \
    src/test/cases/test_tenant_mgmt.c \
    src/test/cases/test_regex.c \
    src/test/cases/test_bare_shapes.c \
    src/test/cases/test_list_files.c \
    src/test/cases/test_bulk_update_json.c \
    src/test/cases/test_bulk_update_delimited.c \
    src/test/cases/test_remove_field.c \
    src/test/cases/test_length_ops.c \
    src/test/cases/test_unknown_field_validation.c \
    src/test/cases/test_idx_cache_tenants.c \
    src/test/cases/test_odirect_single_shot.c \
    src/test/cases/test_cursor_with_total.c \
    src/test/cases/test_small_prefilter_orderby.c \
    src/test/cases/test_cursor_bitmap_intersect.c \
    src/test/cases/test_float_field_type.c \
    src/test/cases/test_all_field_types.c \
    src/test/cases/test_case_sensitivity.c \
    src/test/cases/test_objlock.c \
    src/test/cases/test_field_vs_field.c \
    src/test/cases/test_binary_index.c \
    src/test/cases/test_stats_prom.c \
    src/test/cases/test_parallel_index_integrity.c \
    src/test/cases/test_cli_shortcuts.c \
    src/test/cases/test_agg_neq_shortcut.c \
    src/test/cases/test_agg_indexed_groupby.c \
    src/test/cases/test_agg_int_groupby_multi.c \
    src/test/cases/test_agg_int_groupby_resize.c \
    src/test/cases/test_agg_leaf_only_walk.c \
    src/test/cases/test_agg_varchar_groupby_limit.c \
    src/test/cases/test_agg_varchar_groupby_sum.c \
    src/test/cases/test_find_indexed_orderby.c \
    src/test/cases/test_find_filter_first_orderby.c \
    src/test/cases/test_find_timestamp_criteria.c \
    src/test/cases/test_or_keyset_cap.c \
    src/test/cases/test_agg_walk_fetch_check.c \
    src/test/cases/test_regex_anchor_prefilter.c \
    src/test/cases/test_request_timeout.c \
    src/test/cases/test_and_intersection.c \
    src/test/cases/test_find_cursor.c \
    src/test/cases/test_schema_export.c \
    src/test/cases/test_bulk_cas.c \
    src/test/cases/test_token_perms.c \
    src/test/cases/test_csv_export.c \
    src/test/cases/test_cursor_sparse_prefetch.c \
    src/test/cases/test_vacuum_addfield.c \
    src/test/cases/test_vacuum_streams_mismatch.c \
    src/test/cases/test_rebuild_kf.c \
    src/test/cases/test_rebuild_recovery.c \
    src/test/cases/test_slotcask_resplit.c \
    src/test/cases/test_per_tenant_auth.c \
    src/test/cases/test_stress_no_hang.c \
    src/test/cases/test_tls.c \
    src/test/cases/test_restore.c \
    src/test/cases/test_auto_vacuum.c \
    src/test/cases/test_startup_validator.c \
    src/test/cases/test_index_splits_curve.c \
    src/test/cases/test_range_coalesce.c \
    src/test/cases/test_count_varchar_field.c \
    src/test/cases/test_slotcask_basic.c \
    src/test/cases/test_slotcask_v2_object.c \
    src/test/cases/test_slotcask_cas.c \
    src/test/cases/test_slotcask_v2_wire.c \
    src/test/cases/test_slotcask_v2_query.c \
    src/test/cases/test_slotcask_v2_bulk.c \
    src/test/cases/test_bulk_delete_criteria_indexed.c \
    src/test/cases/test_slotcask_v2_parity.c \
    src/test/cases/test_slotcask_v2_schema.c \
    src/test/cases/test_slotcask_v2_crash.c \
    src/test/cases/test_slotcask_v2_concurrent.c \
    src/test/cases/test_v2_index_leak_on_clear.c \
    src/test/cases/test_btree.c \
    src/test/cases/test_btree_inplace_leaf.c \
    src/test/cases/test_btree_value_hash_sort.c \
    src/test/cases/test_json_escape.c \
    src/test/cases/test_timestamp.c \
    src/test/cases/test_datetimems.c \
    src/test/cases/test_bitmap_index.c \
    src/test/cases/test_bm_intersect_count.c \
    src/test/cases/test_enum.c \
    src/test/cases/test_ipv4.c \
    src/test/cases/test_ipv6.c \
    src/test/cases/test_trigram_index.c \
    src/test/cases/test_config_encode.c \
    src/test/cases/test_error_paths.c \
    src/test/cases/test_explain.c \
    src/test/cases/test_keyset.c \
    src/test/cases/test_objlock_unit.c \
    src/test/cases/test_parallel.c \
    src/test/cases/test_planner_edge_cases.c \
    src/test/cases/test_signal_handling.c \
    src/test/cases/test_simd.c \
    src/test/cases/test_slotcask_api.c \
    src/test/cases/test_tls_unit.c \
    src/test/cases/test_variable_length.c \
    src/test/cases/test_nql.c \
    src/test/cases/test_nql_joins.c \
    src/test/cases/test_util.c \
    src/test/cases/test_agg_topn_stream.c \
    src/test/cases/test_find_orderby_selective.c \
    src/test/cases/test_planner_trigram_selectivity.c \
    src/test/cases/test_cardinality_estimate.c \
    src/test/cases/test_order_walk_range_bounds.c \
    src/test/cases/test_planner_broad_ordered_walk.c \
    src/test/cases/test_planner_sort_vs_walk.c \
    src/test/cases/test_planner_cost_model.c \
    src/test/cases/test_planner_materialization_guard.c \
    src/test/cases/test_planner_op_capability.c \
    src/test/cases/test_d1_composite_executor.c \
    src/test/cases/test_composite_varchar_bound.c \
    src/test/cases/test_composite_in_fold.c \
    src/test/cases/test_composite_typed.c \
    src/test/cases/test_composite_selectivity_guard.c \
    src/test/cases/test_composite_range_fold.c \
    src/test/cases/test_composite_prefix_routing.c \
    src/test/cases/test_d3_order_walk_executor.c \
    src/test/cases/test_ft_float_consistency.c \
    src/test/cases/test_reindex_spill_collision.c \
    src/test/cases/test_slow_query_log.c \
    src/test/cases/test_find_with_total.c \
    src/test/cases/test_o_direct_scan.c \
    src/test/cases/test_registry_single_flight.c \
    src/test/cases/test_add_indexes_single_scan.c \
    src/test/cases/test_secure_random_keys.c \
    src/test/cases/test_coverity_disk_corruption_segments.c \
    src/test/cases/test_coverity_disk_corruption_btree.c \
    src/test/cases/test_coverity_disk_corruption_bitmap.c \
    src/test/cases/test_coverity_seg_scan_varlen_overflow.c \
    src/test/cases/test_coverity_encode_criterion_overflow.c \
    src/test/cases/test_coverity_group_by_overflow.c \
    src/test/cases/test_coverity_join_buf_overflow.c \
    src/test/cases/test_nql_input_validation.c \
    src/test/cases/test_criteria_operator_alias.c \
    src/test/cases/test_criteria_op_tree_error.c \
    src/test/cases/test_criteria_field_value_validation.c \
    src/test/cases/test_agg_having_or.c \
    src/test/cases/test_agg_having_nested.c \
    src/test/cases/test_agg_having_json_or.c \
    src/test/cases/test_agg_having_json_and.c \
    src/test/cases/test_agg_having_no_group_regression.c \
    src/db/util.c \
    src/db/slotcask.c \
    src/db/parallel.c \
    src/db/storage.c \
    src/db/index.c \
    src/db/keyset.c \
    src/db/simd.c \
    src/db/tls.c \
    src/db/btree.c \
    src/db/bitmap.c \
    src/db/trigram.c \
    src/db/objlock.c \
    src/db/io_direct.c \
    src/db/query.c \
    src/db/query_aggregate.c \
    src/db/query_join.c \
    src/db/query_plan.c \
    src/db/query_maint.c \
    src/db/query_schema.c \
    src/db/query_bulk.c \
    src/db/query_find.c \
    src/db/server.c \
    src/db/config.c \
    src/db/type_desc.c \
    src/db/embedded.c \
    src/db/nql.c \
    -Isrc/db -Isrc/test \
    $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db-test

# shard-db-bench — bench runner. Same TCP+TLS client as shard-db-test,
# different output format (latency histograms + throughput). Future bench
# files under src/bench/ get listed here.
gcc $MODE_CFLAGS -o shard-db-bench \
    src/bench/shard-db-bench.c \
    src/bench/bench_common.c \
    src/bench/bench_stats.c \
    src/bench/bench_table.c \
    src/bench/bench_kv.c \
    src/bench/bench_kv_parallel.c \
    src/bench/bench_grow.c \
    src/bench/bench_invoice.c \
    src/bench/bench_parallel.c \
    src/bench/bench_queries.c \
    src/bench/bench_cache_pollution.c \
    src/bench/bench_joins.c \
    src/bench/bench_incremental.c \
    src/bench/bench_btree.c \
    src/bench/bench_bitmap_vs_btree.c \
    src/bench/bench_trigram_sizing.c \
    src/test/test_client.c \
    src/test/test_runner.c \
    src/test/fixtures.c \
    src/db/util.c \
    src/db/parallel.c \
    src/db/storage.c \
    src/db/index.c \
    src/db/keyset.c \
    src/db/btree.c \
    src/db/bitmap.c \
    src/db/trigram.c \
    src/db/objlock.c \
    src/db/tls.c \
    src/db/slotcask.c \
    src/db/simd.c \
    src/db/io_direct.c \
    src/db/query.c \
    src/db/query_aggregate.c \
    src/db/query_join.c \
    src/db/query_plan.c \
    src/db/query_maint.c \
    src/db/query_schema.c \
    src/db/query_bulk.c \
    src/db/query_find.c \
    src/db/server.c \
    src/db/config.c \
    src/db/type_desc.c \
    src/db/embedded.c \
    src/db/nql.c \
    -Isrc/db -Isrc/test -Isrc/bench \
    $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db-bench

# migrate — one-shot upgrade orchestrator. Spawns ./shard-db start, runs
# ./shard-db reindex (idempotent — rewrites btrees in the (value, hash)
# sort 2026.05.5 expects), then stops the daemon. Standalone binary; no
# daemon source linkage.
gcc $MODE_CFLAGS -o migrate src/migrate/main.c
[ "$DO_STRIP" = 1 ] && strip migrate

mkdir -p build/bin

# Purge any dev-run artifacts so `./build.sh` always emits a clean tree.
# When the daemon is exercised from build/bin during local testing it
# creates build/db (DB_ROOT="../db") + build/logs (LOG_DIR="../logs") +
# possibly a build/bin/db.env if the operator copied from the example.
# Strip those here so `cp build/bin/ <prod>:/opt/shard-db/bin/` upgrades
# don't accidentally drag dev state along.
rm -rf build/db build/logs
rm -f  build/bin/db.env

cp shard-db shard-cli shard-db-test shard-db-bench migrate build/bin/
cp src/db/shard_db.h build/bin/

# Ship as db.env.example — operator copies to db.env on first deploy. Avoids
# overwriting the existing config when an upgrade tarball lands on top.
cat > build/bin/db.env.example << 'EOF'
# Server identity + networking
export DB_ROOT="../db"
export PORT=9199
export TIMEOUT=0

# Logging
export LOG_DIR="../logs"
export LOG_LEVEL=3
export LOG_RETAIN_DAYS=7
export SLOW_QUERY_MS=500
# Cost-model: random-read penalty vs sequential (fetch-and-check beats full
# scan when matches < live_rows / RANDOM_SEQ_COST_RATIO). Higher = prefer scan.
export RANDOM_SEQ_COST_RATIO=8

# Threading + concurrency
export THREADS=0
export POOL_CHUNK=0
export WORKERS=0
# I/O thread pool size. 0 = auto = 4 × nproc. Separate from the CPU pool
# (THREADS) — I/O threads wait on page faults and benefit from
# oversubscription without starving CPU-bound queries.
export IO_THREADS=0
# Max queries in flight. 0 = auto = max(4, min(nproc, 32)).
# Worst-case query-buffer RAM = MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB.
# Clients hitting the cap get {"error":"server at capacity"} immediately
# and should retry. Lower to leave more headroom for OS page cache;
# raise on a beefy box with many short-lived queries.
export MAX_CONCURRENT_QUERIES=0

# Request + query limits
export GLOBAL_LIMIT=100000
export MAX_REQUEST_SIZE=33554432
# Per-query intermediate buffer. With MAX_CONCURRENT_QUERIES bounding
# fan-in, worst-case RAM stays predictable. 256 MB is sized for ~16
# concurrent queries on a 16 GB host; auto-tunes upward on big-RAM
# hosts with low slot counts.
export QUERY_BUFFER_MB=256

# Indexes + cache
export INDEX_PAGE_SIZE=4096
export FCACHE_MAX=4096
# BT_CACHE_MAX is no longer configurable — derived as FCACHE_MAX/4 since 2026.05.1.
# Per-pass memory budget for cmd_add_indexes / reindex. Multi-field builds
# group fields into passes that fit under this budget; an oversized single
# field still runs alone. Crank up on big-RAM hosts to fit more fields per
# pass (faster) or down on small VPS to cap peak.
export INDEX_BUILD_BUDGET_MB=1024

# Full-scan O_DIRECT chunk size (MB). Each parallel worker reads shard data
# in chunks of this size using O_DIRECT (cache-bypassing pread). Larger chunks
# reduce syscall overhead on fast NVMe; smaller chunks reduce peak RAM.
# Default 32 MB if unset. Peak O_DIRECT RAM ~ 2 × DB_ODIRECT_BUF_MB × workers.
export DB_ODIRECT_BUF_MB=32

# Auth + access
export TOKEN_CAP=1024
export DISABLE_LOCALHOST_TRUST=0

# Startup warmup — primes userspace caches (schema, slotcask registry,
# kfcache) and the OS page cache for kf + index shards so the first
# user query is O(1).
#   async (default)  detached thread races first queries
#   sync             block startup until warmup completes
#   off              skip entirely (rely on lazy cache populate)
# Warmup itself fans out per-shard / per-file through the global pool
# (THREADS), so its parallelism scales with pool size.
export WARMUP=async

# Auto-vacuum — opt-in background thread that runs plain `vacuum` on
# objects exceeding the recommendation thresholds below. Same thresholds
# drive the manual `vacuum-check` command's "vacuum":true output, so
# operator and daemon agree on what needs cleaning.
#   AUTO_VACUUM=1                         to enable
#   AUTO_VACUUM_INTERVAL_SEC>=60          poll cadence (1-min floor)
#   VACUUM_RECOMMEND_TOMBSTONE_PCT=N      vacuum when deleted/total >= N%
#   VACUUM_RECOMMEND_MIN_DELETED=N        absolute floor on deleted count
# Defaults match the historic hardcoded values (10% / 1000) so existing
# operators see no behaviour change.
export AUTO_VACUUM=0
export AUTO_VACUUM_INTERVAL_SEC=3600
export VACUUM_RECOMMEND_TOMBSTONE_PCT=10
export VACUUM_RECOMMEND_MIN_DELETED=1000

# Native TLS — set TLS_ENABLE=1 + TLS_CERT/TLS_KEY (server) and TLS_CA (client)
# to require TLS 1.3 on PORT. Default 0 = plaintext TCP. Front the daemon
# with nginx/HAProxy if you already have an existing cert pipeline.
export TLS_ENABLE=0
export TLS_CERT=""
export TLS_KEY=""
export TLS_CA=""
export TLS_SKIP_VERIFY=0
EOF

echo "Built: build/bin/"

# Run the C test suite after every build unless explicitly skipped.
# Each test self-spawns its own daemon (fork+exec) on a free port and
# tears down on exit, so they're safe to run from anywhere. Set
# SKIP_TESTS=1 to suppress (e.g. during quick edit/compile loops).
if [ -z "$SKIP_TESTS" ]; then
    echo ""
    echo "Running C tests..."
    if ! ./build/bin/shard-db-test run-all 2>&1 | tee /tmp/shard-db-build-tests.log; then
        echo ""
        echo "BUILD FAILED: tests reported failures (see output above and /tmp/shard-db-build-tests.log)" >&2
        exit 1
    fi
    # `run-all` exits 0 even when individual cases fail unless we check the
    # final summary. Grep for failures in the log to be safe.
    if grep -q "not ok " /tmp/shard-db-build-tests.log; then
        echo ""
        echo "BUILD FAILED: tests reported assertion failures (see /tmp/shard-db-build-tests.log)" >&2
        exit 1
    fi
fi

echo "Deploy: copy build/bin/ contents to your install dir (e.g. /opt/shard-db/bin/)."
echo "First-time setup: cp db.env.example db.env, edit, then ./shard-db start."
echo "Upgrades: replace build/bin/ contents (shard-db + shard-cli + migrate), then run ./migrate once to rebuild B+ trees into 2026.05.5's (value, hash)-sorted layout. If still on v1 (pre-2026.05.5), first upgrade to 2026.05.4 and run that release's ./migrate."
