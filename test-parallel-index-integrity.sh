#!/bin/bash
# Verify every record inserted via parallel bulk-insert is findable via
# single and composite indexes. This is the scenario where the old strtok
# race would silently drop index entries.

BIN="./shard-db"
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
PASS=0
FAIL=0

pass() { PASS=$((PASS+1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL+1)); echo "  FAIL: $1"; }

$BIN stop 2>/dev/null; sleep 0.5
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
$BIN start
sleep 0.3

# Fresh object with both single and composite indexes
$BIN query '{"mode":"truncate","dir":"default","object":"idxtest"}' 2>/dev/null || true
rm -rf "$DB_ROOT/default/idxtest"
sed -i '/^default:idxtest:/d' "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN query '{"mode":"create-object","dir":"default","object":"idxtest","splits":64,"max_key":32,"fields":["status:varchar:16","region:varchar:16","tier:varchar:8","amount:int"],"indexes":["status","region","status+region","region+tier","status+region+tier"]}'

echo "=== Generating 5 chunks × 20K = 100K records ==="
python3 << 'PY'
import json, random
statuses = ['PAID','PENDING','REFUNDED','CANCELLED']
regions  = ['EU','US','APAC','LATAM','ME']
tiers    = ['GOLD','SILVER','BRONZE']
random.seed(42)
for c in range(5):
    recs = []
    for i in range(c*20000, (c+1)*20000):
        recs.append({'id': f'k{i}',
                     'data': {'status': statuses[i%4],
                              'region': regions[i%5],
                              'tier':   tiers[i%3],
                              'amount': i}})
    with open(f'/tmp/idx_{c}.json','w') as f:
        json.dump(recs, f)
PY

echo "=== Parallel bulk insert 5×20K ==="
for i in 0 1 2 3 4; do
    $BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"idxtest\",\"file\":\"/tmp/idx_$i.json\"}" > /tmp/idx_out_$i.log 2>&1 &
done
wait

GOT=$($BIN query '{"mode":"size","dir":"default","object":"idxtest"}')
COUNT=$(echo "$GOT" | grep -o '"count":[0-9]*' | grep -o '[0-9]*')
if [ "$COUNT" = "100000" ]; then pass "100000 records present"; else
    fail "count=$COUNT"
    echo "    bulk insert outputs:"
    for i in 0 1 2 3 4; do echo "      $i: $(cat /tmp/idx_out_$i.log | head -1)"; done
fi

# ---- Distribution counts per value ----
# 4 statuses x 25000 each = 100000
# 5 regions  x 20000 each = 100000
# 3 tiers    — uneven because 100000/3 isn't integer
# Composites: status+region = 4*5 = 20 combos. Most should have ~5000 each.

echo ""
echo "=== Single-field index: status ==="
# With limit large enough to return all, count results per value and compare against expected
for s in PAID PENDING REFUNDED CANCELLED; do
    GOT=$($BIN query "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"idxtest\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"$s\"}],\"limit\":200000}")
    N=$(echo "$GOT" | grep -o '"key"' | wc -l)
    if [ "$N" = "25000" ]; then pass "status=$s → 25000 records via index"; else fail "status=$s returned $N, expected 25000"; fi
done

echo ""
echo "=== Single-field index: region ==="
for r in EU US APAC LATAM ME; do
    GOT=$($BIN query "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"idxtest\",\"criteria\":[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"$r\"}],\"limit\":200000}")
    N=$(echo "$GOT" | grep -o '"key"' | wc -l)
    if [ "$N" = "20000" ]; then pass "region=$r → 20000 records via index"; else fail "region=$r returned $N, expected 20000"; fi
done

echo ""
echo "=== Composite index: status+region ==="
# Iterate each status×region combo, sum results, expect total = 100000
TOTAL=0
FAILURES=0
for s in PAID PENDING REFUNDED CANCELLED; do
    for r in EU US APAC LATAM ME; do
        GOT=$($BIN query "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"idxtest\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"$s\"},{\"field\":\"region\",\"op\":\"eq\",\"value\":\"$r\"}],\"limit\":200000}")
        N=$(echo "$GOT" | grep -o '"key"' | wc -l)
        TOTAL=$((TOTAL + N))
    done
done
if [ "$TOTAL" = "100000" ]; then pass "status+region composite: all 100000 records reachable"; else fail "composite status+region returned total $TOTAL, expected 100000"; fi

echo ""
echo "=== Composite index: region+tier ==="
TOTAL=0
for r in EU US APAC LATAM ME; do
    for t in GOLD SILVER BRONZE; do
        GOT=$($BIN query "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"idxtest\",\"criteria\":[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"$r\"},{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"$t\"}],\"limit\":200000}")
        N=$(echo "$GOT" | grep -o '"key"' | wc -l)
        TOTAL=$((TOTAL + N))
    done
done
if [ "$TOTAL" = "100000" ]; then pass "region+tier composite: all 100000 records reachable"; else fail "composite region+tier returned total $TOTAL, expected 100000"; fi

echo ""
echo "=== Composite index: status+region+tier (3-way) ==="
TOTAL=0
for s in PAID PENDING REFUNDED CANCELLED; do
    for r in EU US APAC LATAM ME; do
        for t in GOLD SILVER BRONZE; do
            GOT=$($BIN query "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"idxtest\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"$s\"},{\"field\":\"region\",\"op\":\"eq\",\"value\":\"$r\"},{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"$t\"}],\"limit\":200000}")
            N=$(echo "$GOT" | grep -o '"key"' | wc -l)
            TOTAL=$((TOTAL + N))
        done
    done
done
if [ "$TOTAL" = "100000" ]; then pass "status+region+tier 3-way composite: all 100000 records reachable"; else fail "3-way composite returned total $TOTAL, expected 100000"; fi

echo ""
echo "=== Sample spot checks: random records findable via each index ==="
# Pick 10 random records, verify each is returned by the composite search matching its exact values
for i in 0 7777 15000 33333 49999 60000 77777 88888 95000 99999; do
    GOT=$($BIN get default idxtest k$i)
    S=$(echo "$GOT" | python3 -c "import json,sys; r=json.loads(sys.stdin.read()); print(r['value']['status'])" 2>/dev/null || echo "")
    R=$(echo "$GOT" | python3 -c "import json,sys; r=json.loads(sys.stdin.read()); print(r['value']['region'])" 2>/dev/null || echo "")
    T=$(echo "$GOT" | python3 -c "import json,sys; r=json.loads(sys.stdin.read()); print(r['value']['tier'])" 2>/dev/null || echo "")
    [ -z "$S" ] && { fail "k$i not retrievable"; continue; }
    GOT2=$($BIN query "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"idxtest\",\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"$S\"},{\"field\":\"region\",\"op\":\"eq\",\"value\":\"$R\"},{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"$T\"}],\"limit\":200000}")
    if echo "$GOT2" | grep -q "\"k$i\""; then pass "k$i findable via 3-way composite"; else fail "k$i NOT findable via 3-way composite (s=$S r=$R t=$T)"; fi
done

echo ""
$BIN stop 2>/dev/null || true
rm -f /tmp/idx_*.json

echo ""
echo "=== RESULT: $PASS passed, $FAIL failed ==="
[ $FAIL -eq 0 ]
