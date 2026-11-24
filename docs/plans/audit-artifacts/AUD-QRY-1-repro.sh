#!/bin/bash
# probe 2: isolate the group_by-empty-on-bitmap behavior
R=/tmp/shard-db-gbcheck2
rm -rf $R; mkdir -p $R/db
PORT=52975
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
python3 - "$PORT" <<'EOF'
import socket, sys, json
port = int(sys.argv[1])
s = socket.create_connection(("127.0.0.1", port), timeout=15)
def q(obj):
    s.sendall((json.dumps(obj) + "\n").encode())
    buf = b""
    while b"\0\n" not in buf:
        d = s.recv(65536)
        if not d: break
        buf += d
    return buf.replace(b"\0\n", b"").decode().strip()
def raw(txt):
    s.sendall((txt + "\n").encode())
    buf = b""
    while b"\0\n" not in buf:
        d = s.recv(65536)
        if not d: break
        buf += d
    return buf.replace(b"\0\n", b"").decode().strip()
q({"mode":"add-dir","dir":"default"})
for obj, idx in (("o_plain", None), ("o_btree", '["v","t","w"]'),
                 ("o_bitmap", '["v:bitmap","w:bitmap","t"]'), ("o_comp", '["u+v"]')):
    req = {"mode":"create-object","dir":"default","object":obj,"splits":8,
           "max_key":16,"fields":["v:varchar:48","u:int","t:long","w:varchar:16"]}
    if idx: req["indexes"] = json.loads(idx)
    q(req)
    recs = ",".join(json.dumps({"key":"k%03d"%i,"value":{"v":"cat_%d"%(i%5),"u":i%13,"t":i,"w":"w_%d"%(i%3)}}) for i in range(100))
    raw('{"mode":"bulk-insert","dir":"default","object":"%s","records":[%s]}' % (obj, recs))

def gb(obj, crit, label):
    body = '{"mode":"aggregate","dir":"default","object":"%s","group_by":"v","aggregates":[{"fn":"count","alias":"n"}]' % obj
    if crit is not None:
        body += ',"criteria":%s' % json.dumps(crit)
    print(label, "->", raw(body + "}"))

gb("o_plain", None, "plain nocrit  ")
gb("o_btree", None, "btree nocrit  ")
gb("o_bitmap", None, "bitmap nocrit ")
gb("o_comp", None, "comp nocrit   ")
crit_t = [{"field":"t","op":"between","value":50,"value2":75}]
for o in ("o_plain","o_btree","o_bitmap","o_comp"):
    gb(o, crit_t, o + " t-range   ")
crit_v = [{"field":"v","op":"eq","value":"cat_1"}]
for o in ("o_plain","o_btree","o_bitmap","o_comp"):
    gb(o, crit_v, o + " v-eq      ")
# scalar aggregate sanity on bitmap shape
print("bitmap scalar:", q({"mode":"aggregate","dir":"default","object":"o_bitmap","criteria":crit_t,"aggregates":[{"fn":"count","alias":"c"},{"fn":"sum","field":"t","alias":"s"}]}))
# explain the group-by plans
for o in ("o_btree","o_bitmap"):
    print(o, "explain t-range:", q({"mode":"find","dir":"default","object":o,"criteria":crit_t,"explain":true} if False else {"mode":"find","dir":"default","object":o,"criteria":crit_t,"explain":True}))
EOF
kill $DPID 2>/dev/null
sleep 0.5
cd /
rm -rf $R
