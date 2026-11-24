#!/bin/bash
# audit/scripts/soak.sh <duration-seconds>
#
# Soak prep (campaign Phase 1 step 9): mixed-workload overnight soak —
# concurrent writers, readers, vacuum/reindex interleaves, and periodic
# clean restarts against ONE daemon on a throwaway DB_ROOT. The HUMAN
# runs this (normal build for the overnight soak; the sanitizer variant
# per AGENTS: BUILD_MODE=tsan then soak.sh — keep concurrency low under
# TSan).
#
# Usage:
#   ./audit/scripts/soak.sh 28800            # 8h against the normal build
#   TSAN=1 ./audit/scripts/soak.sh 3600      # 1h; REQUIRES a BUILD_MODE=tsan
#                                            # build and drops THREADS/MAX_CONCURRENT
#                                            # (TSan is resource-constrained)
#
# TSAN=1 is enforced: the script refuses to run unless build/bin/shard-db
# actually contains the TSan runtime (a plain binary soaked under
# TSAN=1 would look clean and prove nothing).
#
# Stop with Ctrl-C; the trap tears the daemon and the root down. Logs
# (daemon output, request failures) are written OUTSIDE the soak root,
# to a per-run directory under /tmp/shard-db-audit-logs/, and SURVIVE
# the cleanup.
set -u
DUR=${1:?usage: soak.sh <duration-seconds>}
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN="$REPO/build/bin/shard-db"
[ -x "$BIN" ] || { echo "build/bin/shard-db missing (build it first)" >&2; exit 1; }

TSAN=${TSAN:-0}
if [ "$TSAN" = "1" ]; then
  if ! nm "$BIN" 2>/dev/null | grep -q __tsan_init; then
    echo "TSAN=1 but $BIN has no TSan runtime — rebuild with BUILD_MODE=tsan" >&2
    exit 1
  fi
fi

# Logs live OUTSIDE the soak root so the cleanup cannot delete them.
mkdir -p /tmp/shard-db-audit-logs
LOG_DIR=/tmp/shard-db-audit-logs/soak-$(date +%s)-$$
mkdir -p "$LOG_DIR"

SOAK_DIR=$(mktemp -d /tmp/shard-db-soak-XXXXXX)
DB_ROOT="$SOAK_DIR/db"
mkdir -p "$DB_ROOT"
PORT=$(( 20000 + RANDOM % 20000 ))
{
  echo "export DB_ROOT=\"$DB_ROOT\""
  echo "export PORT=$PORT"
  echo "export TIMEOUT=30"
  if [ "$TSAN" = "1" ]; then
    echo "export THREADS=2"
    echo "export MAX_CONCURRENT_QUERIES=4"
  else
    echo "export THREADS=4"
    echo "export MAX_CONCURRENT_QUERIES=16"
  fi
  echo "export TLS_ENABLE=0"
} > "$SOAK_DIR/db.env"

# Export the soak config so BOTH the daemon and the `query` CLI helper
# see it (the CLI discovers ./db.env from its CWD too; without this the
# requests would talk to whatever the invoking shell's config points at).
. "$SOAK_DIR/db.env"

start_daemon() {
  # The daemon discovers ./db.env from its CWD (the old script relied on
  # a bare cd; keep that contract without breaking the absolute $BIN):
  # spawn from $SOAK_DIR in a subshell and exec so $! IS the daemon.
  ( cd "$SOAK_DIR" && exec "$BIN" server ) >> "$LOG_DIR/soak-daemon.log" 2>&1 &
  DPID=$!
}
wait_ready() {
  for i in $(seq 1 120); do
    sleep 0.25
    python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),1).close()" 2>/dev/null && return 0
  done
  return 1
}

DPID=""
cleanup() {
  if [ -n "$DPID" ]; then
    kill "$DPID" 2>/dev/null
    wait "$DPID" 2>/dev/null
  fi
  rm -rf "$SOAK_DIR"
}
# Installed BEFORE the daemon spawns: nothing between here and the
# server backgrounding can leak a process.
trap cleanup EXIT

start_daemon
if ! wait_ready; then
  echo "soak: daemon did not become ready in 30s; see $LOG_DIR/soak-daemon.log" >&2
  exit 1
fi

# wire helper: one request per invocation
REQ_ERR="$LOG_DIR/soak-req-errors.log"
# query takes the JSON as an ARGUMENT (piped stdin prints a usage error)
# and discovers ./db.env + the daemon's pid file from its CWD — so run it
# from $SOAK_DIR like the daemon does.
req() {
  out=$( ( cd "$SOAK_DIR" && timeout 20 "$BIN" query "$2" ) 2>> "$REQ_ERR" ) \
    || echo "REQFAIL[$1] $2" >> "$REQ_ERR"
  # the CLI exits 0 on an {"error":...} reply; count those as failures too
  case "$out" in '{"error"'*) echo "REQFAIL[$1] $2 -> $out" >> "$REQ_ERR" ;; esac
}

echo "soak: root=$SOAK_DIR port=$PORT duration=${DUR}s pid=$DPID tsan=$TSAN"
echo "soak: logs in $LOG_DIR (kept after exit)"
END=$(( SECONDS + DUR ))
ROUND=0
while [ $SECONDS -lt $END ]; do
  ROUND=$((ROUND+1))
  # setup objects once per 50 rounds (schema churn is exercised separately)
  if [ $((ROUND % 50)) -eq 1 ]; then
    req setup '{"mode":"add-dir","dir":"default"}'
    req setup '{"mode":"create-object","dir":"default","object":"soak_kv","splits":8,"max_key":16,"fields":["v:varchar:64","u:int","t:long"]}'
    req setup '{"mode":"create-object","dir":"default","object":"soak_ix","splits":8,"max_key":16,"fields":["v:varchar:64","u:int","t:long"],"indexes":["v","u"]}'
  fi
  # writers: insert + bulk + update + delete mix on both objects
  req w1 "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"soak_kv\",\"key\":\"w$ROUND\",\"value\":{\"v\":\"v$ROUND\",\"u\":$((ROUND%97)),\"t\":$ROUND}}"
  req w2 "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"soak_kv\",\"key\":\"w$((ROUND/2))\",\"value\":{\"v\":\"upd$ROUND\",\"u\":$((ROUND%13)),\"t\":$ROUND}}"
  req w3 "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"soak_ix\",\"records\":[{\"key\":\"b${ROUND}a\",\"value\":{\"v\":\"cat$((ROUND%7))\",\"u\":$ROUND,\"t\":$ROUND}},{\"key\":\"b${ROUND}b\",\"value\":{\"v\":\"cat$((ROUND%5))\",\"u\":$((ROUND+1)),\"t\":$ROUND}}]}"
  req w4 "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"soak_kv\",\"key\":\"w$((ROUND/3))\"}"
  # readers: find / count / aggregate / cursor-ish
  req r1 "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"soak_ix\"}"
  req r2 "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"soak_ix\",\"criteria\":[{\"field\":\"u\",\"op\":\"between\",\"value\":1,\"value2\":40}],\"offset\":0,\"limit\":50}"
  req r3 "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"soak_ix\",\"criteria\":[{\"field\":\"t\",\"op\":\"between\",\"value\":0,\"value2\":$((ROUND+50))}],\"group_by\":\"v\",\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}"
  # maintenance interleave
  if [ $((ROUND % 25)) -eq 0 ]; then
    req m1 '{"mode":"vacuum","dir":"default","object":"soak_kv"}'
  fi
  if [ $((ROUND % 200)) -eq 0 ]; then
    req m2 '{"mode":"reindex","dir":"default","object":"soak_ix"}'
  fi
  if [ $((ROUND % 500)) -eq 0 ]; then
    echo "soak: clean restart at round $ROUND"
    kill "$DPID" 2>/dev/null; wait "$DPID" 2>/dev/null
    start_daemon
    if ! wait_ready; then
      echo "soak: daemon did not come back after restart; see $LOG_DIR/soak-daemon.log" >&2
      exit 1
    fi
  fi
  sleep 0.3
done

echo "soak: duration elapsed; final state:"
req final '{"mode":"count","dir":"default","object":"soak_ix"}'
req final2 '{"mode":"count","dir":"default","object":"soak_kv"}'
# Verdict: request failures (query CLI errors / timeouts) and any
# sanitizer or assertion report in the daemon log make the soak FAIL —
# a clean exit status must mean a clean soak.
FAILS=0
[ -f "$REQ_ERR" ] && FAILS=$(grep -c REQFAIL "$REQ_ERR")
echo "soak: request failures: $FAILS${FAILS:+ (see $REQ_ERR)}"
STATUS=0
[ "$FAILS" -gt 0 ] && STATUS=1
if grep -qE "ThreadSanitizer|AddressSanitizer|UndefinedBehaviorSanitizer|runtime error:|Assertion .* failed" \
     "$LOG_DIR/soak-daemon.log" 2>/dev/null; then
  echo "soak: SANITIZER/ASSERTION report in $LOG_DIR/soak-daemon.log" >&2
  STATUS=1
fi
echo "soak: logs in $LOG_DIR (kept)"
exit $STATUS
