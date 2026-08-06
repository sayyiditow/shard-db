# Deployment

Getting shard-db into a real environment. Covers systemd, native TLS (built in) and reverse-proxy TLS (HAProxy / nginx) as options, bind-address hardening, logs, and health checks.

## Prerequisites

- Linux x86_64 / ARM64 or macOS Apple Silicon (see [Install](../getting-started/install.md)).
- A non-root user to run the daemon.
- Sufficient disk — plan for `record_count × value_size × 2` as a rough envelope (includes indexes). With slotcask (v2) you'll also accumulate seg-file slack as tombstones build up; either run `vacuum` periodically (or enable `AUTO_VACUUM=1`) or add headroom for the slack.

## Directory layout

Put the binary and config where you want them, e.g. `/opt/shard-db/`:

```
/opt/shard-db/
  shard-db            # the binary
  db.env              # runtime config
  db/                 # $DB_ROOT — data + indexes + metadata
    tokens.conf
    allowed_ips.conf
    dirs.conf
    default/...
  logs/               # $LOG_DIR
```

Set ownership:

```bash
sudo useradd -r -s /usr/sbin/nologin shard-db
sudo chown -R shard-db:shard-db /opt/shard-db
```

## systemd unit

`/etc/systemd/system/shard-db.service`:

```ini
[Unit]
Description=shard-db
After=network.target

[Service]
Type=simple
User=shard-db
Group=shard-db
WorkingDirectory=/opt/shard-db
ExecStart=/opt/shard-db/shard-db server
ExecStop=/opt/shard-db/shard-db stop
Restart=on-failure
RestartSec=2s
TimeoutStopSec=35s    # give graceful shutdown 30s drain + slack
LimitNOFILE=65536
# Hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/shard-db

[Install]
WantedBy=multi-user.target
```

- `Type=simple` + `server` — foreground mode; systemd manages the lifecycle.
- `ExecStop=... stop` — graceful drain of in-flight writes.
- `TimeoutStopSec=35s` — gives the built-in 30 s drain a buffer before SIGKILL.
- `LimitNOFILE=65536` — per-connection fds add up; raise if you expect many concurrent clients.

Enable + start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now shard-db
sudo systemctl status shard-db
sudo journalctl -u shard-db -f
```

## Bind address

shard-db listens on all interfaces by default (the `PORT` in db.env). What you do with that depends on which TLS option (below) you pick:

- **Native TLS (`TLS_ENABLE=1`)**: leaving the default bind is fine — clients connect directly to `PORT` over TLS 1.3. Token-based auth handles client identity; trusted-IP / per-tenant tokens scope access.
- **Reverse-proxy TLS** or **plaintext-only behind a private network**: bind to `127.0.0.1` so only the proxy (or local clients) can reach the daemon, and let the proxy enforce TLS at the edge.

The bind address is currently hard-coded to all-interfaces in `src/db/server.c`. For the localhost-only case, run shard-db inside a network namespace, behind a host firewall rule blocking external traffic to `PORT`, or wrap with a proxy.

## TLS

shard-db has **native TLS 1.3** built in (since 2026.05.1, OpenSSL-backed). For most deployments that's all you need — single binary, single port, no extra processes. Reverse-proxy termination (HAProxy / nginx) remains fully supported if you prefer to consolidate TLS at the edge.

### Option 1: Native TLS (recommended for most deployments)

Enable in `db.env`:

```bash
export TLS_ENABLE=1
export TLS_CERT="/opt/shard-db/certs/cert.pem"
export TLS_KEY="/opt/shard-db/certs/key.pem"
# Optional — only set if your CA isn't in the OS trust store
export TLS_CA="/opt/shard-db/certs/ca.pem"
```

Get a certificate:

```bash
# Self-signed (dev / internal)
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj '/CN=shard-db.internal'

mkdir -p /opt/shard-db/certs
mv cert.pem key.pem /opt/shard-db/certs/
chown shard-db:shard-db /opt/shard-db/certs/*.pem
chmod 600 /opt/shard-db/certs/key.pem
chmod 644 /opt/shard-db/certs/cert.pem
```

Production: use Let's Encrypt or your corporate CA. Same `mv` + `chown` + `chmod`.

Restart and verify:

```bash
sudo systemctl restart shard-db
echo '{"mode":"db-dirs"}' | openssl s_client -connect localhost:9199 -quiet 2>/dev/null
```

`PORT` becomes TLS-only when `TLS_ENABLE=1` — plaintext clients are rejected at handshake. Client identity is enforced via tokens (not mTLS). See [Configuration → TLS knobs](../getting-started/configuration.md) for `TLS_SKIP_VERIFY`, `TLS_SERVER_NAME`, and the full client-side options.

**Cert rotation:** native TLS does **not** hot-reload certs — replace the cert files and restart the daemon. The single-instance lock plus systemd's restart hooks make this clean. If you need rotation without restart, use Option 2 instead.

**Why use native TLS:**

- Single binary, single process, single port — nothing else to install or supervise.
- TLS 1.3 only (modern ciphersuites, forward secrecy).
- Server refuses to start if cert/key are missing, unreadable, or mismatched — fail-fast vs. quietly serving plaintext.

### Option 2: Reverse-proxy TLS (HAProxy / nginx)

Use a reverse proxy when you already have a TLS pipeline (e.g., one HAProxy fronting a fleet of services), need cert hot-reload without restarting the daemon, or want to combine TLS termination with rate limiting / IP gating at the edge. Set `TLS_ENABLE=0` (the default) and bind shard-db to `127.0.0.1` so only the proxy reaches it.

#### HAProxy

```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj '/CN=shard-db.internal'
cat cert.pem key.pem > /etc/haproxy/shard-db.pem
chmod 600 /etc/haproxy/shard-db.pem

apt install haproxy   # or pacman -S haproxy / dnf install haproxy
```

`/etc/haproxy/haproxy.cfg`:

```
global
    maxconn 20000
    tune.ssl.default-dh-param 2048

defaults
    mode tcp
    timeout connect 5s
    timeout client  30s
    timeout server  30s

frontend shard_db_tls
    bind *:9200 ssl crt /etc/haproxy/shard-db.pem
    default_backend shard_db

backend shard_db
    server db1 127.0.0.1:9199 check
```

```bash
sudo systemctl enable --now haproxy
ss -tlnp | grep 9200
echo '{"mode":"db-dirs"}' | openssl s_client -connect localhost:9200 -quiet 2>/dev/null
```

Client connects to port 9200; HAProxy decrypts and forwards to shard-db on 127.0.0.1:9199. nginx `stream` module is an equivalent option if you already run nginx.

See the README for Python / Node.js / Java TLS client snippets — they're transport-agnostic and work against either option.

## Authentication

Once reachable over TLS, enforce auth. Options:

### IP allowlist

```bash
echo "203.0.113.5" | sudo tee -a /opt/shard-db/db/allowed_ips.conf
sudo systemctl reload shard-db   # or use add-ip JSON mode
```

Entries auto-load on server start. Localhost (`127.0.0.1`, `::1`) is trusted implicitly.

### Tokens

Generate and register:

```bash
openssl rand -hex 32 > /opt/shard-db/db/tokens.conf.new
sudo mv /opt/shard-db/db/tokens.conf.new /opt/shard-db/db/tokens.conf
sudo chown shard-db:shard-db /opt/shard-db/db/tokens.conf
chmod 600 /opt/shard-db/db/tokens.conf

# Or at runtime (from a trusted IP):
./shard-db query '{"mode":"add-token","token":"<token-value>"}'
```

Clients include `"auth":"<token>"` in every request from non-allowlisted IPs.

See [Configuration → Authentication](../getting-started/configuration.md) for the full model.

## Log rotation

shard-db auto-prunes logs older than `LOG_RETAIN_DAYS` (default 7). If you want the OS to manage rotation instead, set `LOG_RETAIN_DAYS=0` and configure logrotate:

`/etc/logrotate.d/shard-db`:
```
/opt/shard-db/logs/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
```

`copytruncate` is important — shard-db keeps the current day's file open.

## Health check

For load balancers / uptime monitors, use `db-dirs` or `stats` as a liveness probe:

```bash
# Returns the tenant list — fast, no disk I/O
echo '{"mode":"db-dirs"}' | nc -q1 localhost 9199
```

Any response other than a connect error = server is alive. For deeper health, parse `stats` and alert on `in_flight_writes` staying high or cache hit rates collapsing.

## Backup strategy

Two layers:

1. **File-system snapshots** — ZFS / LVM / btrfs snapshots of `$DB_ROOT` give you crash-consistent point-in-time backups. Schedule hourly/daily, retain N.
2. **Per-object `backup` command** — logical copy of one object's data/indexes/files to a timestamped dir under the same root. Good for pre-upgrade checkpoints.

See [Operations → Backup](backup.md).

## Upgrades

Standard upgrade flow (already on v2 / slotcask):

```bash
systemctl stop shard-db
# replace /opt/shard-db/shard-db and /opt/shard-db/shard-cli
systemctl start shard-db
```

As of 2026.08.1, startup migration is automatic — the daemon compares `$DB_ROOT/.version` against its compiled-in version and runs the full index rebuild in-process on start. The standalone `./migrate` binary is removed. The minimum supported source release is 2026.07.3; this is recorded for operators but is informational and not enforced in this release because earlier releases did not write `.version`. `./shard-db reindex` remains available for on-demand use. Once your objects are on the slotcask engine and compact format, point-release upgrades are a binary swap. On startup the daemon sweeps stale `.new` rebuild artifacts from interrupted resplits/vacuum runs before accepting connections. Operators who upgraded from a pre-2026.07.1 build affected by kf corruption must run the 2026.07.1 release's `rebuild-kf` against a backup before upgrading past this release.

**Upgrading from a pre-2026.05.5 install with legacy v1 (probe-into-slot) objects.** This binary refuses v1 objects at load. Run the migration on the previous release first:

```bash
# step 1: install 2026.05.4 (for legacy v1→v2 conversion)
systemctl stop shard-db
# replace shard-db / shard-cli / migrate with the 2026.05.4 artifacts
sudo -u shard-db /opt/shard-db/migrate   # converts v1 objects → slotcask v2
systemctl start shard-db
# verify by checking schema.conf — every line should now end with `:2:<streams>`
systemctl stop shard-db

# step 2: upgrade to 2026.05.5+
# replace shard-db / shard-cli with the new artifacts; migrate binary is gone
systemctl start shard-db
```

## Resource sizing

Rough rules:

| Workload | `WORKERS` | `FCACHE_MAX` | `MAX_REQUEST_SIZE` |
|---|---|---|---|
| Small (< 1 M records, internal tool) | auto | 4096 (default) | 32 MB (default) |
| Medium (1–10 M records, mixed read/write) | 16 | 8192 | 64 MB |
| Large (10 M+, read-heavy) | 32 | 16384 | 128 MB |
| File-heavy (large uploads) | — | — | 128–256 MB |

`BT_CACHE_MAX` is **derived** as `FCACHE_MAX / 4` since 2026.05.1 — no separate knob. `FCACHE_MAX` accepts the strict allow-list `{4096, 8192, 12288, 16384}`.

See [Operations → Tuning](tuning.md) for detail.
