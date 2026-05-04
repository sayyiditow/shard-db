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
WARN_CFLAGS="-Wall -Wextra -Wno-format-truncation -Wno-unused-parameter -Wno-address-of-packed-member"

BUILD_MODE="${BUILD_MODE:-release}"
case "$BUILD_MODE" in
    release)
        MODE_CFLAGS="-O2 -flto=auto $WARN_CFLAGS"
        MODE_LDFLAGS="-flto=auto"
        DO_STRIP=1
        ;;
    asan)
        MODE_CFLAGS="-O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined $WARN_CFLAGS"
        MODE_LDFLAGS="-fsanitize=address,undefined"
        DO_STRIP=0
        ;;
    tsan)
        MODE_CFLAGS="-O1 -g -fno-omit-frame-pointer -fsanitize=thread $WARN_CFLAGS"
        MODE_LDFLAGS="-fsanitize=thread"
        DO_STRIP=0
        ;;
    debug)
        MODE_CFLAGS="-O0 -g $WARN_CFLAGS"
        MODE_LDFLAGS=""
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
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/config.c src/db/storage.c src/db/index.c src/db/query.c src/db/server.c src/db/main.c src/db/btree.c src/db/objlock.c src/db/keyset.c src/db/parallel.c src/db/tls.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db

# shard-cli — separate ncurses TUI client. Links the same OpenSSL but no
# pthread/daemon code. Self-contained connection helper in src/cli/conn.c.
gcc $MODE_CFLAGS -o shard-cli src/cli/main.c src/cli/widgets.c src/cli/views.c src/cli/conn.c -Isrc/cli $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lncursesw -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-cli

# migrate — one-shot per-release upgrade runner. Pure FS ops + system()
# calls to shard-db; no daemon code, no OpenSSL, no ncurses.
gcc $MODE_CFLAGS -o migrate src/migrate/main.c src/migrate/migrate_files.c -Isrc/migrate $MODE_LDFLAGS
[ "$DO_STRIP" = 1 ] && strip migrate

# shard-db-test — TAP-style C test runner. Links daemon's JSON helpers
# (src/db/util.c) for response parsing; otherwise self-contained (TCP/TLS
# client + assertion macros + per-test daemon fixtures). Future test cases
# under src/test/cases/ get listed here.
gcc $MODE_CFLAGS -o shard-db-test \
    src/test/shard-db-test.c \
    src/test/test_client.c \
    src/test/test_runner.c \
    src/test/fixtures.c \
    src/test/cases/test_or_logic.c \
    src/test/cases/test_crash_safety.c \
    src/test/cases/test_rename_field.c \
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
    src/test/cases/test_case_sensitivity.c \
    src/test/cases/test_objlock.c \
    src/test/cases/test_migrate_binary.c \
    src/test/cases/test_field_vs_field.c \
    src/test/cases/test_binary_index.c \
    src/test/cases/test_stats_prom.c \
    src/test/cases/test_parallel_index_integrity.c \
    src/test/cases/test_cli_shortcuts.c \
    src/test/cases/test_agg_neq_shortcut.c \
    src/test/cases/test_request_timeout.c \
    src/test/cases/test_and_intersection.c \
    src/test/cases/test_find_cursor.c \
    src/test/cases/test_schema_export.c \
    src/test/cases/test_bulk_cas.c \
    src/test/cases/test_token_perms.c \
    src/test/cases/test_csv_export.c \
    src/db/util.c \
    -Isrc/db -Isrc/test \
    $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db-test

# shard-db-bench — bench runner. Same TCP+TLS client as shard-db-test,
# different output format (latency histograms + throughput). Future bench
# files under src/bench/ get listed here.
gcc $MODE_CFLAGS -o shard-db-bench \
    src/bench/shard-db-bench.c \
    src/bench/bench_stats.c \
    src/bench/bench_kv.c \
    src/bench/bench_kv_parallel.c \
    src/bench/bench_grow.c \
    src/bench/bench_invoice.c \
    src/bench/bench_parallel.c \
    src/bench/bench_queries.c \
    src/bench/bench_joins.c \
    src/bench/bench_incremental.c \
    src/test/test_client.c \
    src/test/test_runner.c \
    src/test/fixtures.c \
    src/db/util.c \
    -Isrc/db -Isrc/test -Isrc/bench \
    $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db-bench

mkdir -p build/bin

# Purge any dev-run artifacts so `./build.sh` always emits a clean tree.
# When the daemon is exercised from build/bin during local testing it
# creates build/db (DB_ROOT="../db") + build/logs (LOG_DIR="../logs") +
# possibly a build/bin/db.env if the operator copied from the example.
# Strip those here so `cp build/bin/ <prod>:/opt/shard-db/bin/` upgrades
# don't accidentally drag dev state along.
rm -rf build/db build/logs
rm -f  build/bin/db.env

cp shard-db shard-cli migrate shard-db-test shard-db-bench build/bin/

# Ship as db.env.example — operator copies to db.env on first deploy. Avoids
# overwriting the existing config when an upgrade tarball lands on top.
cat > build/bin/db.env.example << 'EOF'
export DB_ROOT="../db"
export PORT=9199
export TIMEOUT=0
export LOG_DIR="../logs"
export LOG_LEVEL=3
export LOG_RETAIN_DAYS=7
export INDEX_PAGE_SIZE=4096
export THREADS=0
export POOL_CHUNK=0
export WORKERS=0
export GLOBAL_LIMIT=100000
export MAX_REQUEST_SIZE=33554432
export FCACHE_MAX=4096
export BT_CACHE_MAX=256
export QUERY_BUFFER_MB=500
export TOKEN_CAP=1024
export DISABLE_LOCALHOST_TRUST=0
export SLOW_QUERY_MS=500

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
echo "Upgrades: replace build/bin/ contents (shard-db + shard-cli + migrate). Run ./migrate before starting the new daemon — db.env / DB_ROOT / logs are untouched."
