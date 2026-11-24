#!/bin/bash
# AUD-QRY-5 live wire repro: bitmap(300) group field, 900 rows over 300
# distinct values — group_by caps at 256 labels while count reports 900.
R=/tmp/shard-db-qry5repro
rm -rf $R; mkdir -p $R/db
PORT=52997
BIN=/home/hashim/junk-system/git-workspace/sayyiditow/shard-db/build/bin/shard-db
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
python3 - "$PORT" <<'PYEOF'
import socket, json, sys
port = int(sys.argv[1])
s = socket.create_connection(("127.0.0.1", port), timeout=30)
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
print("create:", q({"mode":"create-object","dir":"default","object":"cap_demo","splits":8,"max_key":16,"fields":["v:varchar:16","u:int"],"indexes":["v:bitmap(300)","u"]}))
recs = [{"key":"k%04d"%i,"value":{"v":"v%03d"%(i%300),"u":i%13}} for i in range(900)]
# chunked bulk (keep bodies moderate)
for base in range(0, 900, 300):
    r = q({"mode":"bulk-insert","dir":"default","object":"cap_demo","records":recs[base:base+300]})
    print("bulk chunk:", r[:60])
print("count:", raw('{"mode":"count","dir":"default","object":"cap_demo"}'))
gb = raw('{"mode":"aggregate","dir":"default","object":"cap_demo","group_by":"v","aggregates":[{"fn":"count","alias":"n"}]}')
labels = gb.count('"v":"v')
print("group_by label count:", labels, "(expected 300; the cap truncates at 256)")
print("group_by head:", gb[:160], "...")
print("group_by tail:", gb[-120:])
PYEOF
kill $DPID 2>/dev/null; wait $DPID 2>/dev/null
cd /; rm -rf /tmp/shard-db-qry5repro
echo done
