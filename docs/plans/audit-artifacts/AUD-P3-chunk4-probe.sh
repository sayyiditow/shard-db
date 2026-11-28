#!/bin/bash
# Phase 3 slice 1 chunk 4: reply shape when the kf marker gate fails (retained/invalid marker)
R=/tmp/shard-db-p3c4; rm -rf $R; mkdir -p $R/db
PORT=52982
BIN=$(readlink -f "$(dirname "$0")/../../../build/bin/shard-db")
cat > $R/db.env <<ENV
export DB_ROOT="$R/db"
export PORT=$PORT
export TIMEOUT=30
export THREADS=2
export TLS_ENABLE=0
ENV
cd $R
$BIN server > $R/stdout.log 2>&1 &
DPID=$!
for i in $(seq 1 60); do sleep 0.25; python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),1).close()" 2>/dev/null && break; done
python3 - "$PORT" "$R" <<'PY'
import socket, sys, json, glob, os
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=15)
def q(o):
    s.sendall((json.dumps(o)+"\n").encode()); b=b""
    while b"\0\n" not in b:
        d=s.recv(65536)
        if not d: break
        b+=d
    return b.replace(b"\0\n",b"").decode().strip()
def show(l,o): print(f"{l}: {q(o)}")
show("create", {"mode":"create-object","dir":"d","object":"t","splits":8,"max_key":16,"fields":["n:int"],"indexes":["n"]})
show("insert ok", {"mode":"insert","dir":"d","object":"t","key":"a","value":{"n":1}})
kf = glob.glob(sys.argv[2]+"/db/d/t/data/kf")[0]
for i in range(8): open(f"{kf}/bogus{i}_marker.dat","w").write("junk")
show("insert w/ bad marker", {"mode":"insert","dir":"d","object":"t","key":"b","value":{"n":2}})
show("update w/ bad marker", {"mode":"update","dir":"d","object":"t","key":"a","value":{"n":5}})
show("delete w/ bad marker", {"mode":"delete","dir":"d","object":"t","key":"a"})
PY
kill $DPID; wait $DPID 2>/dev/null
