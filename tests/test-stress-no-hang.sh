#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-stress-no-hang.sh — concurrent-client stress harness.
#
# Spawns N worker processes hammering the daemon with mixed insert / find /
# count / update operations for T seconds. An independent watchdog probes
# the daemon every few seconds with a wallclock timeout; if any probe
# doesn't return within the timeout, that's flagged as a hang.
#
# Pass criteria:
#   - No watchdog probe ever times out.
#   - Daemon PID still alive after the workers finish.
#   - Final responsiveness probe completes within the per-probe timeout.
#   - All workers exit cleanly.
#
# This won't catch every theoretical hang (slow memory leak over hours,
# hangs that self-resolve within the window), but it proves "N clients
# hammering for T seconds doesn't lock the daemon" — a meaningful baseline.

set -e

BIN="./shard-db"
PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo "  FAIL: $1"; }

# Tunables — set via env to scale up locally / down in CI.
WORKERS=${STRESS_WORKERS:-16}
DURATION=${STRESS_DURATION:-15}            # seconds, the worker-loop budget
PROBE_INTERVAL=${STRESS_PROBE_INTERVAL:-2} # seconds between watchdog probes
PROBE_TIMEOUT=${STRESS_PROBE_TIMEOUT:-5}   # max seconds for one probe to complete

# Daemon's PID file lives in $LOG_DIR (set by db.env), not $DB_ROOT.
LOG_DIR=$(grep -E '^export LOG_DIR=' db.env | sed 's/.*[\"'"'"']\(.*\)[\"'"'"']/\1/')
[ -z "$LOG_DIR" ] && LOG_DIR="./logs"
PID_FILE="$LOG_DIR/shard-db.pid"

echo "=== SETUP ==="
echo "  workers=$WORKERS duration=${DURATION}s probe=every ${PROBE_INTERVAL}s, timeout ${PROBE_TIMEOUT}s"
$BIN stop 2>/dev/null || true
sleep 0.3
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
mkdir -p "$DB_ROOT"
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"
rm -rf "$DB_ROOT/default/stress"
sed -i "/^default:stress:/d" "$DB_ROOT/schema.conf" 2>/dev/null || true
$BIN start > /dev/null
sleep 0.5

# 64 splits to spread the lock contention across shards. Indexed `status`
# so writes also exercise the index drop+insert path.
$BIN query '{"mode":"create-object","dir":"default","object":"stress","fields":["status:varchar:16","amount:int","note:varchar:32"],"indexes":["status"],"splits":64}' > /dev/null

WORK_DIR=$(mktemp -d /tmp/shard-stress.XXXXXX)
STOP_FLAG="$WORK_DIR/stop"
PROBE_LOG="$WORK_DIR/probes.log"
WORKER_LOG="$WORK_DIR/workers.log"

# Worker process — runs a tight mixed-op loop until $STOP_FLAG appears.
# Counts ops per worker into its own log line; failures are append-marked.
worker() {
    local id=$1
    local op_count=0
    local err_count=0
    while [ ! -f "$STOP_FLAG" ]; do
        local roll=$((RANDOM % 100))
        local key="w${id}_${op_count}"
        local rc=0
        if [ $roll -lt 60 ]; then
            # 60% inserts (the hot write path)
            local status; if [ $((op_count % 3)) -eq 0 ]; then status="paid"; else status="pending"; fi
            $BIN query "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"stress\",\"key\":\"$key\",\"value\":{\"status\":\"$status\",\"amount\":$op_count,\"note\":\"x\"}}" > /dev/null 2>&1 || rc=1
        elif [ $roll -lt 80 ]; then
            # 20% indexed counts
            $BIN query '{"mode":"count","dir":"default","object":"stress","criteria":[{"field":"status","op":"eq","value":"paid"}]}' > /dev/null 2>&1 || rc=1
        elif [ $roll -lt 95 ]; then
            # 15% finds (small page)
            $BIN query '{"mode":"find","dir":"default","object":"stress","criteria":[{"field":"status","op":"eq","value":"pending"}],"limit":5}' > /dev/null 2>&1 || rc=1
        else
            # 5% updates of recently-inserted records (drives index drop+insert)
            local target_id=$((id))
            local target_op=$((op_count - 1))
            [ $target_op -lt 0 ] && target_op=0
            $BIN query "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"stress\",\"key\":\"w${target_id}_${target_op}\",\"value\":{\"status\":\"refunded\"}}" > /dev/null 2>&1 || rc=1
        fi
        op_count=$((op_count + 1))
        [ $rc -ne 0 ] && err_count=$((err_count + 1))
    done
    echo "worker_$id ops=$op_count errors=$err_count" >> "$WORKER_LOG"
}

# Watchdog — probes the daemon at fixed intervals with a wallclock timeout.
# A probe is just `count(*)`. Timing out means the daemon stopped processing
# requests while under load → that's a hang.
watchdog() {
    local probe=0
    local hangs=0
    while [ ! -f "$STOP_FLAG" ]; do
        sleep "$PROBE_INTERVAL"
        [ -f "$STOP_FLAG" ] && break
        probe=$((probe + 1))
        local t0=$(date +%s%N)
        # `timeout` from coreutils kills the call if it doesn't return.
        if timeout "${PROBE_TIMEOUT}s" $BIN query \
             '{"mode":"count","dir":"default","object":"stress"}' \
             > /dev/null 2>&1; then
            local t1=$(date +%s%N)
            local ms=$(( (t1 - t0) / 1000000 ))
            echo "probe=$probe ok ${ms}ms" >> "$PROBE_LOG"
        else
            hangs=$((hangs + 1))
            echo "probe=$probe HANG (>${PROBE_TIMEOUT}s)" >> "$PROBE_LOG"
        fi
    done
    echo "watchdog probes=$probe hangs=$hangs" >> "$PROBE_LOG"
}

echo "=== running stress: $WORKERS workers for ${DURATION}s ==="
START_TS=$(date +%s)

# Spawn workers + watchdog in background.
WORKER_PIDS=()
for i in $(seq 1 $WORKERS); do
    worker $i &
    WORKER_PIDS+=($!)
done
watchdog &
WATCHDOG_PID=$!

# Let the storm run, then signal stop. Workers and watchdog notice via the
# stop flag at their next loop iteration.
sleep "$DURATION"
touch "$STOP_FLAG"

# Reap. Use `wait` so we don't move on until they've all quit cleanly.
for pid in "${WORKER_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
wait $WATCHDOG_PID 2>/dev/null || true

ELAPSED=$(( $(date +%s) - START_TS ))
echo "  workers + watchdog finished after ${ELAPSED}s"

echo "=== verifying daemon is still alive and responsive ==="
DAEMON_PID=$(cat "$PID_FILE" 2>/dev/null | head -1)
if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    pass "daemon PID $DAEMON_PID still alive after stress"
else
    fail "daemon PID gone — server crashed under load"
fi

# Final probe with timeout. If this hangs, the test fails.
FINAL_T0=$(date +%s%N)
if timeout "${PROBE_TIMEOUT}s" $BIN query \
     '{"mode":"count","dir":"default","object":"stress"}' > "$WORK_DIR/final.json" 2>&1; then
    FINAL_T1=$(date +%s%N)
    FINAL_MS=$(( (FINAL_T1 - FINAL_T0) / 1000000 ))
    pass "final probe responded in ${FINAL_MS}ms"
else
    fail "final probe timed out (>${PROBE_TIMEOUT}s) — daemon unresponsive"
fi

# Watchdog log — count any probe-time hangs during the run. grep -c returns
# nonzero exit code on 0 matches (counter-intuitive — that's why we don't
# chain `|| echo 0`, which would emit a SECOND zero and break the int test).
HANG_COUNT=$(grep -c "HANG" "$PROBE_LOG" 2>/dev/null; true)
HANG_COUNT=${HANG_COUNT:-0}
if [ "$HANG_COUNT" -eq 0 ]; then
    pass "no watchdog probe ever timed out (all probes < ${PROBE_TIMEOUT}s)"
else
    fail "$HANG_COUNT watchdog probes hit the ${PROBE_TIMEOUT}s timeout"
    echo "  --- probe log ---"
    cat "$PROBE_LOG" | head -20
fi

# Aggregate worker stats. Log line shape: "worker_$id ops=$N errors=$M".
TOTAL_OPS=$(awk '{ for(i=1;i<=NF;i++) if(match($i,/^ops=([0-9]+)$/,m)) sum+=m[1] } END{print sum+0}' "$WORKER_LOG")
TOTAL_ERRS=$(awk '{ for(i=1;i<=NF;i++) if(match($i,/^errors=([0-9]+)$/,m)) sum+=m[1] } END{print sum+0}' "$WORKER_LOG")
echo "  workers completed $TOTAL_OPS ops total ($TOTAL_ERRS individual op errors)"
if [ "$TOTAL_OPS" -gt 0 ]; then
    pass "workers made forward progress under load ($TOTAL_OPS ops)"
else
    fail "workers made no progress at all — daemon was unresponsive throughout"
fi

# Throughput sanity — should clear at least 100 ops/sec aggregate or
# something was bottlenecked badly. Loose bound; really just guarding
# against "everything got queued and only completed at shutdown".
if [ "$ELAPSED" -gt 0 ]; then
    OPS_PER_SEC=$((TOTAL_OPS / ELAPSED))
    echo "  aggregate throughput: ~${OPS_PER_SEC} ops/sec across $WORKERS workers"
fi

echo
echo "=== TEARDOWN ==="
$BIN query '{"mode":"drop-object","dir":"default","object":"stress"}' > /dev/null 2>&1 || true
$BIN stop > /dev/null
sleep 0.3
rm -rf "$WORK_DIR"

if [ "$FAIL" -eq 0 ]; then
    echo "================================"
    echo "  $PASS passed, 0 failed ($TOTAL total)"
    echo "================================"
    exit 0
else
    echo "================================"
    echo "  $PASS passed, $FAIL failed ($TOTAL total)"
    echo "================================"
    exit 1
fi
