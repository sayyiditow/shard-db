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

# -flto: link-time optimization (cross-TU inlining; usually helps perf, definitely
#        shrinks the binary by eliminating dead code visible only across files).
# strip: remove symbol/debug tables from the shipped binary (~25K cut).
gcc -O2 -flto -o shard-db src/db/util.c src/db/config.c src/db/storage.c src/db/index.c src/db/query.c src/db/server.c src/db/main.c src/db/btree.c src/db/objlock.c src/db/keyset.c src/db/parallel.c src/db/tls.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS -lpthread -lssl -lcrypto
strip shard-db

# shard-cli — separate ncurses TUI client. Links the same OpenSSL but no
# pthread/daemon code. Self-contained connection helper in src/cli/conn.c.
gcc -O2 -o shard-cli src/cli/main.c src/cli/widgets.c src/cli/views.c src/cli/conn.c -Isrc/cli $OSSL_CFLAGS $OSSL_LDFLAGS -lncursesw -lssl -lcrypto
strip shard-cli

mkdir -p build/bin build/db

cp shard-db shard-cli build/bin/
cp start.sh stop.sh status.sh build/bin/ 2>/dev/null || true

cat > build/bin/db.env << 'EOF'
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

cat > build/db/schema.conf << 'EOF'
# Format: dir:object:splits:max_key:max_value
# Auto-managed by create-object — do not edit manually
EOF

echo "Built: build/"
echo "Deploy: copy build/ to server, cd build/bin, ./shard-db start"
