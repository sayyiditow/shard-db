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
BUILD_MODE="${BUILD_MODE:-release}"
case "$BUILD_MODE" in
    release)
        MODE_CFLAGS="-O2 -flto"
        MODE_LDFLAGS=""
        DO_STRIP=1
        ;;
    asan)
        MODE_CFLAGS="-O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined"
        MODE_LDFLAGS="-fsanitize=address,undefined"
        DO_STRIP=0
        ;;
    tsan)
        MODE_CFLAGS="-O1 -g -fno-omit-frame-pointer -fsanitize=thread"
        MODE_LDFLAGS="-fsanitize=thread"
        DO_STRIP=0
        ;;
    debug)
        MODE_CFLAGS="-O0 -g"
        MODE_LDFLAGS=""
        DO_STRIP=0
        ;;
    *)
        echo "build.sh: unknown BUILD_MODE=$BUILD_MODE (release|asan|tsan|debug)" >&2
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

mkdir -p build/bin

cp shard-db shard-cli build/bin/

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

echo "Built: build/"
echo "Deploy: copy build/ to server, cd build/bin, ./shard-db start"
echo "Note: \$DB_ROOT is created lazily on first start; existing data dirs are not touched."
