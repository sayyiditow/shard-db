#!/usr/bin/env bash
# measure-shapes.sh — true cold/warm cost of each catalogued query shape,
# in ISOLATION (page cache dropped between shapes so timings aren't
# confounded by an earlier scan). Run on the shard-db host.
#   sudo ./bench/measure-shapes.sh
# Requires root (drop_caches). Sources db.env for PORT/DB_ROOT/TOKEN.
# NOTE: drops the page cache repeatedly -> degrades the live site briefly.
# Run during low traffic, or against a non-production copy.
set -u
BIN=${BIN:-/usr/local/bin/shard-db}
cd "${DB_DIR:-/var/lib/shard-db}" && set -a && . ./db.env && set +a

drop_cache() { sync; echo 3 > /proc/sys/vm/drop_caches; }
# time one query (ms); prints "<ms>"
timeq() { local t0 t1; t0=$(date +%s%3N); "$BIN" query "$1" >/dev/null 2>&1; t1=$(date +%s%3N); echo $((t1-t0)); }
# cold (after drop) + 3 warm; prints "cold=<>ms warm=<min>ms"
measure() {
  local label="$1" q="$2"
  drop_cache
  local cold; cold=$(timeq "$q")
  local w1 w2 w3; w1=$(timeq "$q"); w2=$(timeq "$q"); w3=$(timeq "$q")
  local warm=$w1; [ "$w2" -lt "$warm" ] && warm=$w2; [ "$w3" -lt "$warm" ] && warm=$w3
  printf "%-46s cold=%6sms  warm=%6sms\n" "$label" "$cold" "$warm"
}

TO='"timeout_ms":120000'
echo "=== isolated per-shape cost (cold = after drop_caches, warm = min of 3) ==="
measure "title starts 'Show HN' (count)" \
  "{\"mode\":\"count\",\"dir\":\"hn\",\"object\":\"stories\",\"criteria\":[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"story\"},{\"field\":\"title\",\"op\":\"starts\",\"value\":\"Show HN\"}],$TO}"
measure "by=aikah order_by time desc (profile find)" \
  "{\"mode\":\"find\",\"dir\":\"hn\",\"object\":\"comments\",\"criteria\":[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"aikah\"}],\"order_by\":\"time\",\"order\":\"desc\",\"limit\":25,$TO}"
measure "by=aikah (no order_by) (baseline)" \
  "{\"mode\":\"find\",\"dir\":\"hn\",\"object\":\"comments\",\"criteria\":[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"aikah\"}],\"limit\":25,$TO}"
measure "type=story group_by author (aggregate)" \
  "{\"mode\":\"aggregate\",\"dir\":\"hn\",\"object\":\"stories\",\"group_by\":[\"by\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],\"criteria\":[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"story\"}],\"order_by\":\"n\",\"order\":\"desc\",\"limit\":20,$TO}"
measure "comment-tree story_root (find limit 500)" \
  "{\"mode\":\"find\",\"dir\":\"hn\",\"object\":\"comments\",\"criteria\":[{\"field\":\"story_root\",\"op\":\"eq\",\"value\":9987679}],\"order_by\":\"time\",\"order\":\"asc\",\"limit\":500,$TO}"

echo; echo "=== pollution probe: does a full scan evict a warm shape? ==="
AGG="{\"mode\":\"aggregate\",\"dir\":\"hn\",\"object\":\"stories\",\"group_by\":[\"by\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],\"criteria\":[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"story\"}],\"order_by\":\"n\",\"order\":\"desc\",\"limit\":20,$TO}"
SCAN="{\"mode\":\"count\",\"dir\":\"hn\",\"object\":\"stories\",\"criteria\":[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"story\"},{\"field\":\"title\",\"op\":\"starts\",\"value\":\"Show HN\"}],$TO}"
timeq "$AGG" >/dev/null; timeq "$AGG" >/dev/null   # warm it
echo "  agg warm:        $(timeq "$AGG")ms"
echo "  full scan:       $(timeq "$SCAN")ms"
echo "  agg after scan:  $(timeq "$AGG")ms   (ratio vs warm = pollution impact)"
