#!/bin/bash
# Phase 3 slice 1 chunk 6: cmd_delete (if, return_record, dry_run, index/count effects)
R=/tmp/shard-db-p3c6; rm -rf $R; mkdir -p $R/db
PORT=52984
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
def show(l,o): print(f"{l}: {q(o)}")
D=lambda k,**x: dict({"mode":"delete","dir":"d","object":"t","key":k},**x)
I=lambda k,n,b: {"mode":"insert","dir":"d","object":"t","key":k,"value":{"n":n,"b":b,"tg":"hello"+str(n)}}
C={"mode":"count","dir":"d","object":"t"}
F=lambda f,v: {"mode":"find","dir":"d","object":"t","criteria":[{"field":f,"op":"eq","value":v}]}
show("create", {"mode":"create-object","dir":"d","object":"t","splits":8,"max_key":16,
 "fields":["n:int","b:varchar:8","tg:varchar:20"],"indexes":["n","b:bitmap","tg:trigram"]})
for k,n,b in (("a",1,"x"),("b",2,"x"),("c",3,"y")): show("ins "+k, I(k,n,b))
show("count", C)
show("del missing", D("zz"))
show("del missing + if", D("zz",**{"if":[{"field":"n","op":"eq","value":"1"}]}))
show("del if mismatch", D("a",**{"if":[{"field":"n","op":"eq","value":"99"}]}))
show("del bad if", D("a",**{"if":[{"field":"nosuch","op":"eq","value":"1"}]}))
show("del dry_run", D("a",dry_run=True))
show("count after dry", C)
show("del return_record", D("a",return_record=True))
show("del again", D("a"))
show("count", C)
show("find n=1 (btree)", F("n","1"))
show("find b=x (bitmap)", F("b","x"))
show("find tg contains (trigram)", {"mode":"find","dir":"d","object":"t","criteria":[{"field":"tg","op":"contains","value":"hello1"}]})
show("get a", {"mode":"get","dir":"d","object":"t","key":"a"})
show("reinsert a", I("a",1,"x"))
show("find n=1", F("n","1"))
show("del ok if", D("b",**{"if":[{"field":"n","op":"eq","value":"2"}]}))
show("count", C)
show("orphaned", {"mode":"orphaned","dir":"d","object":"t"})
show("del bad object", {"mode":"delete","dir":"d","object":"nope","key":"a"})
show("del no key", {"mode":"delete","dir":"d","object":"t"})
PY
kill $DPID; wait $DPID 2>/dev/null
