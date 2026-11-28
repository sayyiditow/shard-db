#!/bin/bash
# Phase 3 slice 1 chunk 3: v2_insert_owned / typed_insert_alloc probes
R=/tmp/shard-db-p3c3; rm -rf $R; mkdir -p $R/db
PORT=52981
BIN=$(dirname "$0")/../../../build/bin/shard-db
BIN=$(readlink -f "$BIN")
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
python3 - "$PORT" <<'PY'
import socket, sys, json
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=15)
def q(o):
    s.sendall((json.dumps(o)+"\n").encode()); b=b""
    while b"\0\n" not in b:
        d=s.recv(65536)
        if not d: break
        b+=d
    return b.replace(b"\0\n",b"").decode().strip()
def show(l,o): print(f"{l}: {q(o)}")
show("create", {"mode":"create-object","dir":"d","object":"t","splits":8,"max_key":16,
 "fields":["name:varchar:8","age:int:default=7","req:varchar:8:not_null","created:timestamp:auto_create","note:varchar:20"]})
show("ins typo-field", {"mode":"insert","dir":"d","object":"t","key":"k1","value":{"nmae":"x","req":"r"}})
show("get k1", {"mode":"get","dir":"d","object":"t","key":"k1"})
show("ins missing req", {"mode":"insert","dir":"d","object":"t","key":"k2","value":{"name":"x"}})
show("ins null req", {"mode":"insert","dir":"d","object":"t","key":"k3","value":{"req":None}})
show("ins oversize name", {"mode":"insert","dir":"d","object":"t","key":"k4","value":{"req":"r","name":"123456789"}})
show("ins dup field", {"mode":"insert","dir":"d","object":"t","key":"k5","value":{}} )
s.sendall(b'{"mode":"insert","dir":"d","object":"t","key":"k5","value":{"req":"a","req":"b"}}\n'); b=b""
while b"\0\n" not in b: b+=s.recv(65536)
print("dup field:", b.replace(b"\0\n",b"").decode().strip())
show("get k5", {"mode":"get","dir":"d","object":"t","key":"k5"})
show("ins nested obj", {"mode":"insert","dir":"d","object":"t","key":"k6","value":{"req":"r","name":{"$incr":1}}})
show("ins age str", {"mode":"insert","dir":"d","object":"t","key":"k7","value":{"req":"r","age":"abc"}})
show("ins age null", {"mode":"insert","dir":"d","object":"t","key":"k8","value":{"req":"r","age":None}})
show("get k8", {"mode":"get","dir":"d","object":"t","key":"k8"})
show("ins created explicit", {"mode":"insert","dir":"d","object":"t","key":"k9","value":{"req":"r","created":1}})
show("get k9", {"mode":"get","dir":"d","object":"t","key":"k9"})
show("upsert k9 again", {"mode":"insert","dir":"d","object":"t","key":"k9","value":{"req":"r2"}})
show("get k9 after", {"mode":"get","dir":"d","object":"t","key":"k9"})
show("value as string", {"mode":"insert","dir":"d","object":"t","key":"k10","value":"{\"req\":\"r\"}"})
show("value as array", {"mode":"insert","dir":"d","object":"t","key":"k11","value":[1]})
show("value empty obj", {"mode":"insert","dir":"d","object":"t","key":"k12","value":{}})
PY
kill $DPID; wait $DPID 2>/dev/null
