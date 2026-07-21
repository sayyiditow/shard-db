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
    src/db/durability.c \
    -lpthread -latomic

echo
echo "Built fuzzers in $OUT/:"
ls -la "$OUT"
echo
echo "Run with:"
echo "  $OUT/fuzz_json -max_total_time=60 fuzz/corpora/json/"
echo "  $OUT/fuzz_b64 -max_total_time=60 fuzz/corpora/b64/"
echo "  $OUT/fuzz_criteria -max_total_time=60 fuzz/corpora/criteria/"
