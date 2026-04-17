#!/bin/bash
gcc -O2 -o shard-db src/util.c src/config.c src/storage.c src/index.c src/query.c src/server.c src/main.c src/btree.c src/objlock.c -Isrc -lpthread
mkdir -p build/bin build/db

cp shard-db build/bin/
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
export WORKERS=0
export GLOBAL_LIMIT=100000
export MAX_REQUEST_SIZE=33554432
export FCACHE_MAX=4096
export BT_CACHE_MAX=256
export SLOW_QUERY_MS=500
EOF

cat > build/db/schema.conf << 'EOF'
# Format: dir:object:splits:max_key:max_value
# Auto-managed by create-object — do not edit manually
EOF

echo "Built: build/"
echo "Deploy: copy build/ to server, cd build/bin, ./shard-db start"
