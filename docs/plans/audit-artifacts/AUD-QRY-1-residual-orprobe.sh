#!/bin/bash
# AUD-QRY-1 residual probe: OR-bearing group_by on the bitmap twin AFTER
# the QRY-1 fix. Harness criterion-10 class, hand-verifiable 60-row fixture.
R=/tmp/shard-db-qry1res
rm -rf $R; mkdir -p $R/db
PORT=52991
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
UP=0
for i in $(seq 1 60); do
  sleep 0.25
  if python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),1).close()" 2>/dev/null; then UP=1; break; fi
done
echo "up=$UP"
python3 - "$PORT" <<'PYEOF'
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
def q(obj): return raw(json.dumps(obj))
q({"mode":"add-dir","dir":"default"})
# 60 rows: v=cat_(i%3), w=w_(i%3), t=i, u=i%7
N = 60
for obj, idx in (("r_plain", None), ("r_bitmap", '["v:bitmap","w:bitmap","t"]')):
    req = {"mode":"create-object","dir":"default","object":obj,"splits":8,
           "max_key":16,"fields":["v:varchar:48","u:int","t:long","w:varchar:16"]}
    if idx: req["indexes"] = json.loads(idx)
    q(req)
    recs = ",".join(json.dumps({"key":"k%03d"%i,"value":{"v":"cat_%d"%(i%3),"u":i%7,"t":i,"w":"w_%d"%(i%3)}}) for i in range(N))
    print(obj, "insert ->", raw('{"mode":"bulk-insert","dir":"default","object":"%s","records":[%s]}' % (obj, recs))[:60])

crit = [{"field":"w","op":"eq","value":"w_0"},
        {"or":[{"field":"v","op":"eq","value":"cat_2"},
               {"field":"u","op":"between","value":2,"value2":9}]}]
def gb(obj, c, label):
    body = '{"mode":"aggregate","dir":"default","object":"%s","group_by":"v","aggregates":[{"fn":"count","alias":"n"}],"criteria":%s}' % (obj, json.dumps(c))
    print(label, "->", raw(body))
# Oracle: w_0 rows are i%3==0 (i=0,3,...,57 -> 20 rows). OR true iff
# v==cat_2 (i%3==2, disjoint from w_0) OR u=i%7 in [2,9] i.e. i%7 != 0,1.
# i=3k, k=0..19: 3k%7 in {0,1} -> k%7 in {0,5} -> k=0,5,12,17 -> 4 excluded.
# TRUE COUNT = 16, all in group v=cat_(3k%3)=cat_0.
gb("r_plain",  crit, "plain  crit10")
gb("r_bitmap", crit, "bitmap crit10")
gb("r_plain",  None, "plain  nocrit ")
gb("r_bitmap", None, "bitmap nocrit ")
# count() cross-check of the same criteria
print("count plain  ->", raw('{"mode":"count","dir":"default","object":"r_plain","criteria":%s}' % json.dumps(crit)))
print("count bitmap ->", raw('{"mode":"count","dir":"default","object":"r_bitmap","criteria":%s}' % json.dumps(crit)))
PYEOF
kill $DPID 2>/dev/null
wait $DPID 2>/dev/null
cd /
rm -rf /tmp/shard-db-qry1res
echo done
