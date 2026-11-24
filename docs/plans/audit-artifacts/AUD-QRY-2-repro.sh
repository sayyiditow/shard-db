#!/bin/bash
# probe 3: aggregate scalar vs count/find under hybrid leaf+OR criteria
R=/tmp/shard-db-aprobe
rm -rf $R; mkdir -p $R/db
PORT=52977
BIN=/home/hashim/junk-system/git-workspace/sayyiditow/shard-db/build/bin/shard-db
cat > $R/db.env <<ENV
export DB_ROOT="$R/db"
export PORT=$PORT
export TIMEOUT=30
export THREADS=2
export MAX_CONCURRENT_QUERIES=32
export TLS_ENABLE=0
ENV
cd $R
$BIN server > $R/stdout.log 2>&1 &
DPID=$!
for i in $(seq 1 60); do
  sleep 0.25
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),1).close()" 2>/dev/null && break
done
python3 - "$PORT" <<'EOF'
import socket, sys, json
port = int(sys.argv[1])
s = socket.create_connection(("127.0.0.1", port), timeout=15)
def raw(txt):
    s.sendall((txt + "\n").encode())
    buf = b""
    while b"\0\n" not in buf:
        d = s.recv(65536)
        if not d: break
        buf += d
    return buf.replace(b"\0\n", b"").decode().strip()
def q(obj):
    return raw(json.dumps(obj))
q({"mode":"add-dir","dir":"default"})
for obj, idx in (("o_plain", None), ("o_btree", '["v","t","w"]'), ("o_bitmap", '["v:bitmap","w:bitmap","t"]')):
    req = {"mode":"create-object","dir":"default","object":obj,"splits":8,
           "max_key":16,"fields":["v:varchar:48","u:int","t:long","w:varchar:16"]}
    if idx: req["indexes"] = json.loads(idx)
    q(req)
    recs = ",".join(json.dumps({"key":"k%03d"%i,"value":{"v":"cat_%d"%(i%5),"u":i%13,"t":i,"w":"w_%d"%(i%3)}}) for i in range(100))
    raw('{"mode":"bulk-insert","dir":"default","object":"%s","records":[%s]}' % (obj, recs))

# hybrid: leaf + OR subtree. t 50..75 ∩ (v=cat_1 OR u=7)
crit = [{"field":"t","op":"between","value":50,"value2":75},
        {"or":[{"field":"v","op":"eq","value":"cat_1"},{"field":"u","op":"eq","value":7}]}]
for o in ("o_plain","o_btree","o_bitmap"):
    c = q({"mode":"count","dir":"default","object":o,"criteria":crit})
    agg = raw(json.dumps({"mode":"aggregate","dir":"default","object":o,"criteria":crit,
                          "aggregates":[{"fn":"count","alias":"c"}]})).replace("\n"," ")
    f = q({"mode":"find","dir":"default","object":o,"criteria":crit,"offset":0,"limit":200})
    n = f.count('"key"')
    print(o, "count:", c, "| agg-count:", agg, "| find rows:", n)
# control: same leaf + AND postfilter (no OR)
crit2 = [{"field":"t","op":"between","value":50,"value2":75},
         {"field":"v","op":"eq","value":"cat_1"}]
for o in ("o_plain","o_btree","o_bitmap"):
    c = q({"mode":"count","dir":"default","object":o,"criteria":crit2})
    agg = raw(json.dumps({"mode":"aggregate","dir":"default","object":o,"criteria":crit2,
                          "aggregates":[{"fn":"count","alias":"c"}]})).replace("\n"," ")
    print(o, "AND-control count:", c, "| agg:", agg)
EOF
kill $DPID 2>/dev/null
sleep 0.5
cd /
rm -rf $R
