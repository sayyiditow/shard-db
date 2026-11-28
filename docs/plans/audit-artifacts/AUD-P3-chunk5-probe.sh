#!/bin/bash
# Phase 3 slice 1 chunk 5: cmd_update (partial update, dry_run, if, computed)
R=/tmp/shard-db-p3c5; rm -rf $R; mkdir -p $R/db
PORT=52983
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
python3 - "$PORT" <<'PY'
import socket, sys, json
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=15)
def raw(t):
    s.sendall((t+"\n").encode()); b=b""
    while b"\0\n" not in b:
        d=s.recv(65536)
        if not d: break
        b+=d
    return b.replace(b"\0\n",b"").decode().strip()
def q(o): return raw(json.dumps(o))
def show(l,o): print(f"{l}: {q(o) if isinstance(o,dict) else raw(o)}")
U=lambda k,v,**x: dict({"mode":"update","dir":"d","object":"t","key":k,"value":v},**x)
G=lambda k: {"mode":"get","dir":"d","object":"t","key":k}
show("create", {"mode":"create-object","dir":"d","object":"t","splits":8,"max_key":16,
 "fields":["name:varchar:8","n:int","m:int","req:varchar:8:not_null","upd:timestamp:auto_update","bal:numeric:10,2"],"indexes":["n"]})
show("ins", {"mode":"insert","dir":"d","object":"t","key":"a","value":{"name":"x","n":1,"m":5,"req":"r","bal":"1.50"}})
show("get a", G("a"))
show("upd missing key", U("zz",{"n":1}))
show("upd typo field", U("a",{"nn":9}))
show("get a (upd stamped?)", G("a"))
show("upd bad int", U("a",{"n":"abc"}))
show("upd null req", U("a",{"req":None}))
show("upd null n", U("a",{"n":None}))
show("get a", G("a"))
show("upd empty {}", U("a",{}))
show("dry_run valid", U("a",{"n":2},dry_run=True))
show("dry_run bad int", U("a",{"n":"abc"},dry_run=True))
show("dry_run null req", U("a",{"req":None},dry_run=True))
show("dry_run bad json", '{"mode":"update","dir":"d","object":"t","key":"a","value":"notjson","dry_run":true}')
show("dry_run missing", U("zz",{"n":2},dry_run=True))
show("get a unchanged", G("a"))
show("if fail", U("a",{"n":3},**{"if":[{"field":"m","op":"eq","value":"99"}]}))
show("if ok", U("a",{"n":3},**{"if":[{"field":"m","op":"eq","value":"5"}]}))
show("incr", U("a",{"m":{"$incr":2}}))
show("incr neg", U("a",{"m":{"$incr":-100}}))
show("incr overflow", U("a",{"m":{"$incr":2147483647}}))
show("incr on null n", U("a",{"n":{"$incr":1}}))
show("incr numeric", U("a",{"bal":{"$incr":"0.75"}}))
show("incr varchar", U("a",{"name":{"$incr":1}}))
show("copy", U("a",{"m":{"$copy":"n"}}))
show("copy from null", U("a",{"m":{"$copy":"n"}}))
show("copy self", U("a",{"m":{"$copy":"m"}}))
show("copy+incr same field", U("a",{"m":{"$incr":1,"$copy":"n"}}))
show("get a", G("a"))
show("find n=3 via index", {"mode":"find","dir":"d","object":"t","criteria":[{"field":"n","op":"eq","value":"3"}]})
show("set n NULL", U("a",{"n":None}))
show("incr on NULL n", U("a",{"n":{"$incr":1}}))
show("get a", G("a"))
show("copy from NULL n", U("a",{"n":None,"m":{"$copy":"n"}}))
show("set m max", U("a",{"m":2147483647}))
show("incr m overflow", U("a",{"m":{"$incr":1}}))
show("get a", G("a"))
show("return_record", U("a",{"n":7},return_record=True))
PY
kill $DPID; wait $DPID 2>/dev/null
