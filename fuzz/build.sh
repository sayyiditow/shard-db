#!/bin/bash
# Build the libFuzzer harnesses with ASan + UBSan instrumentation.
#
# Each harness compiles only the source it strictly needs — this isn't
# the full daemon. We want fast fuzz cycles and tight crash localization.
#
# Requires Clang (libFuzzer is bundled with Clang's compiler-rt). The
# resulting binaries run via:
#     ./fuzz_<target> -max_total_time=N corpora/<target>/
#
# Set FUZZ_OUT to control the output directory (defaults to fuzz/build).
set -e

OUT="${FUZZ_OUT:-fuzz/build}"
mkdir -p "$OUT"

CFLAGS="-O1 -g -fno-omit-frame-pointer -fsanitize=fuzzer,address,undefined"
INC="-Isrc/db"

# -latomic resolution: on Ubuntu/Debian libatomic sits in the default
# linker path; on Arch it lives only in the gcc toolchain dir, which
# clang's default search misses (slotcask.c's misaligned kf-header magic
# atomics emit __atomic_load/__atomic_store libcalls). Locate it once.
ATOMIC_LD=""
if [ ! -e /usr/lib/libatomic.so ] && [ ! -e /usr/lib/x86_64-linux-gnu/libatomic.so ]; then
    ATOMIC_DIR="$(ls -d /usr/lib/gcc/*-linux-gnu/[0-9]* /usr/lib/gcc/x86_64-*/[0-9]* 2>/dev/null | sort -V | tail -1)"
    [ -n "$ATOMIC_DIR" ] && ATOMIC_LD="-L$ATOMIC_DIR"
fi

echo "==> fuzz_json"
clang $CFLAGS $INC -o "$OUT/fuzz_json" \
    fuzz/fuzz_json.c \
    src/db/util.c

echo "==> fuzz_b64"
clang $CFLAGS $INC -o "$OUT/fuzz_b64" \
    fuzz/fuzz_b64.c \
    src/db/util.c

# fuzz_criteria pulls in parse_criteria_tree from query.c. query.c brings
# a long transitive dep tree (storage, btree, index, server globals, ...)
# but we only need the parser. We compile a minimal subset:
#   util.c          — JSON helpers parse_criteria_tree calls
#   keyset.c        — KeySet definitions referenced by criteria.h
#   query.c + query_*.c — parse_criteria_tree itself (query.c was split
#       2026.07 into per-concern TUs; all seven query_*.c files are
#       linked for symbol resolution alongside the remainder)
#   storage.c, btree.c, index.c, config.c — pulled in transitively;
#       linked but never executed because the harness only calls the
#       parser entry point. Their globals are zero-initialised which is
#       fine for our use.
#   slotcask.c, simd.c — added 2026.06: query.c and storage.c now have
#       v2-engine call sites and SIMD scan helpers. Linker needs the
#       symbol bodies even though the fuzzer never enters those paths.
#   io_direct.c — added 2026.06: query.c references seg_scan_o_direct
#       and seg_scan_o_direct_match for the O_DIRECT full-scan path.
#       Same rationale — linked for symbol resolution, never entered.
#   bitmap.c, trigram.c — added 2026.05.7: query.c and index.c reference
#       bm_* / tg_* symbols for the bitmap + trigram index types. Same
#       rationale as above — linked for symbol resolution, never entered
#       by the fuzzer.
#   type_desc.c — added 2026.07: query_schema.c and query_aggregate.c
#       call type_desc() for the field-type descriptor table.
#   typed_row.c, typed_index.c, typed_read_command.c, typed_bulk_insert.c,
#       packed_record.c, decnum.c, agg_exact.c, scalar_grammar.c,
#       schema_wire.c — added 2026.11: the typed/packed record layer
#       (exact typed records, packed layout) is now referenced from
#       query.c, query_find.c, query_bulk.c, config.c, storage.c and
#       index.c call sites, so the fuzz link resolves those symbols the
#       same way — linked for symbol resolution, never entered.
# server.c, tls.c are NOT linked: the parser never reaches them and
# tls.c requires OpenSSL. Instead, fuzz/stubs.c provides zero-valued
# stub definitions for the two globals they export (g_db,
# g_shard_db_instance) so the linker resolves the references from
# objlock.c and parallel.c without pulling in TLS dependencies.
echo "==> fuzz_criteria"
# -latomic: storage.c does sub-word atomics on packed-struct members, which
# clang emits as a libcall instead of an inline op (gcc inlines it). The
# atomic libcall lives in libatomic.
clang $CFLAGS $INC -o "$OUT/fuzz_criteria" \
    fuzz/fuzz_criteria.c \
    fuzz/stubs.c \
    src/db/util.c src/db/keyset.c src/db/query.c \
    src/db/query_find.c src/db/query_aggregate.c src/db/query_join.c \
    src/db/query_plan.c src/db/query_bulk.c src/db/query_maint.c \
    src/db/query_schema.c src/db/type_desc.c \
    src/db/config.c src/db/storage.c src/db/index.c src/db/btree.c \
    src/db/objlock.c src/db/parallel.c src/db/slotcask.c src/db/simd.c \
    src/db/io_direct.c \
    src/db/bitmap.c src/db/trigram.c \
    src/db/durability.c src/db/durability_epoch.c \
    src/db/typed_row.c src/db/typed_index.c src/db/typed_read_command.c \
    src/db/typed_bulk_insert.c src/db/packed_record.c src/db/decnum.c \
    src/db/agg_exact.c src/db/scalar_grammar.c src/db/schema_wire.c \
    src/db/decimal_order.c src/db/packed_csv_command.c \
    -lpthread $ATOMIC_LD -latomic

# fuzz_filename hammers the three filesystem-name validators (util.c +
# config.c). config.c is linked for dir_name_ok; its transitive refs
# resolve through the same subset fuzz_criteria uses.
echo "==> fuzz_filename"
# Same TU subset as fuzz_criteria: config.c pulls in the typed-record
# layer, scalar grammar, storage/btree/index and rebuild_object
# (query_find.c), which drag most of the engine in for symbol
# resolution. Linked but never executed — the harness only calls the
# three validators.
clang $CFLAGS $INC -o "$OUT/fuzz_filename" \
    fuzz/fuzz_filename.c \
    fuzz/stubs.c \
    src/db/util.c src/db/keyset.c \
    src/db/query.c \
    src/db/query_find.c src/db/query_aggregate.c src/db/query_join.c \
    src/db/query_plan.c src/db/query_bulk.c src/db/query_maint.c \
    src/db/query_schema.c src/db/type_desc.c \
    src/db/config.c src/db/storage.c src/db/index.c src/db/btree.c \
    src/db/objlock.c src/db/parallel.c src/db/slotcask.c src/db/simd.c \
    src/db/io_direct.c \
    src/db/bitmap.c src/db/trigram.c \
    src/db/durability.c src/db/durability_epoch.c \
    src/db/typed_row.c src/db/typed_index.c src/db/typed_read_command.c \
    src/db/typed_bulk_insert.c src/db/packed_record.c src/db/decnum.c \
    src/db/agg_exact.c src/db/scalar_grammar.c src/db/schema_wire.c \
    src/db/decimal_order.c src/db/packed_csv_command.c \
    -lpthread $ATOMIC_LD -latomic

# fuzz_cursor reaches parse_cursor_object (static in query.c) via
# #include "query.c" in the harness TU — so query.c is NOT linked again
# here; every other TU matches the fuzz_criteria subset.
echo "==> fuzz_cursor"
clang $CFLAGS $INC -o "$OUT/fuzz_cursor" \
    fuzz/fuzz_cursor.c \
    fuzz/stubs.c \
    src/db/util.c src/db/keyset.c \
    src/db/query_find.c src/db/query_aggregate.c src/db/query_join.c \
    src/db/query_plan.c src/db/query_bulk.c src/db/query_maint.c \
    src/db/query_schema.c src/db/type_desc.c \
    src/db/config.c src/db/storage.c src/db/index.c src/db/btree.c \
    src/db/objlock.c src/db/parallel.c src/db/slotcask.c src/db/simd.c \
    src/db/io_direct.c \
    src/db/bitmap.c src/db/trigram.c \
    src/db/durability.c src/db/durability_epoch.c \
    src/db/typed_row.c src/db/typed_index.c src/db/typed_read_command.c \
    src/db/typed_bulk_insert.c src/db/packed_record.c src/db/decnum.c \
    src/db/agg_exact.c src/db/scalar_grammar.c src/db/schema_wire.c \
    src/db/decimal_order.c src/db/packed_csv_command.c \
    -lpthread $ATOMIC_LD -latomic

# fuzz_frame drives the daemon's real request-framing worker loop over a
# loopback TCP pair. server.c enters via #include in the harness TU (statics
# visible), and the REAL legacy fast-path dispatcher executes behind it —
# no stub. tls.c is linked for symbol resolution only (the TLS branch
# never runs: g_tls_enable=0); OpenSSL is required at link time.
echo "==> fuzz_frame"
clang $CFLAGS $INC \
    -o "$OUT/fuzz_frame" \
    fuzz/fuzz_frame.c \
    src/db/nql.c src/db/embedded.c \
    src/db/util.c src/db/keyset.c src/db/query.c \
    src/db/query_find.c src/db/query_aggregate.c src/db/query_join.c \
    src/db/query_plan.c src/db/query_bulk.c src/db/query_maint.c \
    src/db/query_schema.c src/db/type_desc.c \
    src/db/config.c src/db/storage.c src/db/index.c src/db/btree.c \
    src/db/objlock.c src/db/parallel.c src/db/slotcask.c src/db/simd.c \
    src/db/io_direct.c \
    src/db/bitmap.c src/db/trigram.c \
    src/db/durability.c src/db/durability_epoch.c \
    src/db/typed_row.c src/db/typed_index.c src/db/typed_read_command.c \
    src/db/typed_bulk_insert.c src/db/packed_record.c src/db/decnum.c \
    src/db/agg_exact.c src/db/scalar_grammar.c src/db/schema_wire.c \
    src/db/decimal_order.c src/db/packed_csv_command.c \
    src/db/tls.c \
    -lpthread $ATOMIC_LD -latomic -lssl -lcrypto

# fuzz_export: export / get-file response re-parse steps (server.c via
# #include, same link set as fuzz_frame).
echo "==> fuzz_export"
clang $CFLAGS $INC \
    -o "$OUT/fuzz_export" \
    fuzz/fuzz_export.c \
    src/db/nql.c src/db/embedded.c \
    src/db/util.c src/db/keyset.c src/db/query.c \
    src/db/query_find.c src/db/query_aggregate.c src/db/query_join.c \
    src/db/query_plan.c src/db/query_bulk.c src/db/query_maint.c \
    src/db/query_schema.c src/db/type_desc.c \
    src/db/config.c src/db/storage.c src/db/index.c src/db/btree.c \
    src/db/objlock.c src/db/parallel.c src/db/slotcask.c src/db/simd.c \
    src/db/io_direct.c \
    src/db/bitmap.c src/db/trigram.c \
    src/db/durability.c src/db/durability_epoch.c \
    src/db/typed_row.c src/db/typed_index.c src/db/typed_read_command.c \
    src/db/typed_bulk_insert.c src/db/packed_record.c src/db/decnum.c \
    src/db/agg_exact.c src/db/scalar_grammar.c src/db/schema_wire.c \
    src/db/decimal_order.c src/db/packed_csv_command.c \
    src/db/tls.c \
    -lpthread $ATOMIC_LD -latomic -lssl -lcrypto

echo
echo "Built fuzzers in $OUT/:"
ls -la "$OUT"
echo
echo "Run with:"
echo "  $OUT/fuzz_json -max_total_time=60 fuzz/corpora/json/ $OUT/corpus-growth/json/"
echo "  $OUT/fuzz_b64 -max_total_time=60 fuzz/corpora/b64/ $OUT/corpus-growth/b64/"
echo "  $OUT/fuzz_criteria -max_total_time=60 fuzz/corpora/criteria/ $OUT/corpus-growth/criteria/"
echo "  $OUT/fuzz_filename -max_total_time=60 fuzz/corpora/filename/ $OUT/corpus-growth/filename/"
echo "  $OUT/fuzz_cursor -max_total_time=60 fuzz/corpora/cursor/ $OUT/corpus-growth/cursor/"
echo "  $OUT/fuzz_frame -max_total_time=60 fuzz/corpora/frame/ $OUT/corpus-growth/frame/"
