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
#   query.c         — parse_criteria_tree itself
#   storage.c, btree.c, index.c, config.c — pulled in transitively;
#       linked but never executed because the harness only calls the
#       parser entry point. Their globals are zero-initialised which is
#       fine for our use.
# objlock.c, parallel.c, server.c, tls.c are NOT linked: the parser
# never reaches them; trying to link tls.c without OpenSSL would fail
# the cleaner subset.
echo "==> fuzz_criteria"
# -latomic: storage.c does sub-word atomics on packed-struct members, which
# clang emits as a libcall instead of an inline op (gcc inlines it). The
# atomic libcall lives in libatomic.
clang $CFLAGS $INC -o "$OUT/fuzz_criteria" \
    fuzz/fuzz_criteria.c \
    src/db/util.c src/db/keyset.c src/db/query.c \
    src/db/config.c src/db/storage.c src/db/index.c src/db/btree.c \
    src/db/objlock.c src/db/parallel.c \
    -lpthread -latomic

echo
echo "Built fuzzers in $OUT/:"
ls -la "$OUT"
echo
echo "Run with:"
echo "  $OUT/fuzz_json -max_total_time=60 fuzz/corpora/json/"
echo "  $OUT/fuzz_b64 -max_total_time=60 fuzz/corpora/b64/"
echo "  $OUT/fuzz_criteria -max_total_time=60 fuzz/corpora/criteria/"
