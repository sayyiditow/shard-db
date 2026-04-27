#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# test-tls.sh — native TLS 1.3 (single port, db.env toggle, OpenSSL backend)

set -u

BIN="./shard-db"
PASS=0
FAIL=0
TOTAL=0
TMPDIR_TLS=$(mktemp -d /tmp/shard-tls.XXXXXX)
DBENV_BAK="$TMPDIR_TLS/db.env.bak"
trap 'cleanup' EXIT

cleanup() {
    $BIN stop >/dev/null 2>&1 || true
    [ -f "$DBENV_BAK" ] && cp "$DBENV_BAK" db.env
    rm -rf "$TMPDIR_TLS"
}

pass() { PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1)); echo "  ok: $1"; }
fail() { FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1)); echo "  FAIL: $1"; }

if ! command -v openssl >/dev/null 2>&1; then
    echo "SKIP: openssl CLI not present — required for cert generation"
    exit 0
fi

# Snapshot current db.env so the suite can mutate freely.
cp db.env "$DBENV_BAK"
$BIN stop >/dev/null 2>&1 || true
sleep 0.2

CERT="$TMPDIR_TLS/cert.pem"
KEY="$TMPDIR_TLS/key.pem"
WRONG_CERT="$TMPDIR_TLS/wrong-cert.pem"
WRONG_KEY="$TMPDIR_TLS/wrong-key.pem"

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$KEY" -out "$CERT" -days 30 \
    -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    >/dev/null 2>&1

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WRONG_KEY" -out "$WRONG_CERT" -days 30 \
    -subj "/CN=other" -addext "subjectAltName=DNS:other" \
    >/dev/null 2>&1

set_env() {
    local enable="$1" cert="$2" key="$3" ca="$4" skip="$5"
    cp "$DBENV_BAK" db.env
    cat >> db.env <<EOF
export TLS_ENABLE=$enable
export TLS_CERT="$cert"
export TLS_KEY="$key"
export TLS_CA="$ca"
export TLS_SKIP_VERIFY=$skip
EOF
}

# ---------- positive path: TLS server accepts CLI over TLS ----------
echo "=== TLS_ENABLE=1 round-trip ==="
set_env 1 "$CERT" "$KEY" "$CERT" 0
$BIN start >/dev/null 2>&1
sleep 0.4

if $BIN status 2>/dev/null | grep -q running; then
    pass "server started in TLS mode"
else
    fail "server did not start in TLS mode"
fi

resp=$($BIN query '{"mode":"db-dirs"}' 2>&1)
if echo "$resp" | grep -q '\['; then
    pass "CLI TLS round-trip returns dirs JSON"
else
    fail "CLI TLS round-trip got: $resp"
fi

# put-file / get-file over TLS — exercises query_collect path.
mkdir -p $(grep DB_ROOT "$DBENV_BAK" | sed "s/.*[\"']\(.*\)[\"']/\1/")
DB_ROOT=$(grep DB_ROOT "$DBENV_BAK" | sed "s/.*[\"']\(.*\)[\"']/\1/")
grep -q '^default$' "$DB_ROOT/dirs.conf" 2>/dev/null || echo default >> "$DB_ROOT/dirs.conf"
$BIN query '{"mode":"create-object","dir":"default","object":"tlsfiles","fields":["body:varchar:64"]}' >/dev/null 2>&1 || true
echo "tls-test-payload-$(date +%s)" > "$TMPDIR_TLS/in.txt"
put_resp=$($BIN put-file default tlsfiles "$TMPDIR_TLS/in.txt" 2>&1)
if echo "$put_resp" | grep -q '"stored"'; then
    pass "put-file over TLS"
else
    fail "put-file over TLS got: $put_resp"
fi
get_resp=$($BIN get-file default tlsfiles in.txt "$TMPDIR_TLS/out.txt" 2>&1)
if echo "$get_resp" | grep -q '"ok"'; then
    pass "get-file over TLS"
else
    fail "get-file over TLS got: $get_resp"
fi
if diff -q "$TMPDIR_TLS/in.txt" "$TMPDIR_TLS/out.txt" >/dev/null 2>&1; then
    pass "put/get-file round-trip byte-identical over TLS"
else
    fail "put/get-file round-trip mismatch"
fi

# ---------- negative paths against the running TLS server ----------
echo "=== TLS server rejects bad clients ==="

tls12_out=$(echo 'QUIT' | timeout 3 openssl s_client -connect 127.0.0.1:9199 \
        -tls1_2 -CAfile "$CERT" -servername localhost 2>&1)
if echo "$tls12_out" | grep -qE 'protocol version|alert number 70|tlsv1 alert'; then
    pass "server rejects TLS 1.2 ClientHello with protocol-version alert"
else
    fail "server did not reject TLS 1.2 cleanly: $(echo "$tls12_out" | head -3)"
fi

# Plain TCP write to a TLS port — server should drop the connection.
plain_resp=$(timeout 3 bash -c 'exec 3<>/dev/tcp/127.0.0.1/9199; echo "{\"mode\":\"db-dirs\"}" >&3; cat <&3' 2>&1 | head -2)
if echo "$plain_resp" | grep -qiE 'reset|connection|broken|EOF' || [ -z "$plain_resp" ]; then
    pass "server drops plaintext bytes on TLS port"
else
    fail "server unexpectedly responded to plaintext: $plain_resp"
fi

# Wrong-CA verification — point CLI at WRONG cert as CA, expect failure.
set_env 1 "$CERT" "$KEY" "$WRONG_CERT" 0
wrong_ca_resp=$($BIN query '{"mode":"db-dirs"}' 2>&1)
if [ -z "$wrong_ca_resp" ] || echo "$wrong_ca_resp" | grep -qiE 'tls:|verify|certificate|error'; then
    pass "client with wrong CA rejects connection"
else
    fail "client with wrong CA unexpectedly succeeded: $wrong_ca_resp"
fi

# Skip-verify with the same wrong CA — should succeed and emit warning.
set_env 1 "$CERT" "$KEY" "$WRONG_CERT" 1
skip_resp=$($BIN query '{"mode":"db-dirs"}' 2>&1)
if echo "$skip_resp" | grep -q '\[' && echo "$skip_resp" | grep -q 'TLS_SKIP_VERIFY=1'; then
    pass "TLS_SKIP_VERIFY=1 succeeds with warning"
else
    fail "TLS_SKIP_VERIFY=1 unexpected: $skip_resp"
fi

$BIN stop >/dev/null 2>&1
sleep 0.2

# ---------- server-side misconfig refusal ----------
echo "=== server refuses bad TLS config ==="

# Missing TLS_CERT
set_env 1 "" "$KEY" "$CERT" 0
miss_cert=$($BIN start 2>&1; $BIN status 2>&1)
if echo "$miss_cert" | grep -qE 'TLS_CERT and/or TLS_KEY not set|stopped'; then
    pass "server refuses TLS_ENABLE=1 with empty TLS_CERT"
else
    fail "server unexpectedly handled empty TLS_CERT: $miss_cert"
fi
$BIN stop >/dev/null 2>&1

# Cert path that doesn't exist
set_env 1 "$TMPDIR_TLS/missing-cert.pem" "$KEY" "$CERT" 0
no_file=$($BIN start 2>&1; $BIN status 2>&1)
if echo "$no_file" | grep -qE 'not readable|stopped'; then
    pass "server refuses missing TLS_CERT path"
else
    fail "server unexpectedly handled missing TLS_CERT path: $no_file"
fi
$BIN stop >/dev/null 2>&1

# Mismatched cert/key (cert from one keypair, key from another)
set_env 1 "$CERT" "$WRONG_KEY" "$CERT" 0
mismatch=$($BIN start 2>&1; $BIN status 2>&1)
if echo "$mismatch" | grep -qE 'check_private_key|cert.*do not match|TLS context init|stopped'; then
    pass "server refuses mismatched cert/key"
else
    fail "server unexpectedly handled mismatched cert/key: $mismatch"
fi
$BIN stop >/dev/null 2>&1

# ---------- summary ----------
echo
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
