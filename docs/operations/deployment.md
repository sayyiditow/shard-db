# Deployment

Getting shard-db into a real environment. Covers systemd, reverse proxy for TLS, bind-address hardening, logs, and health checks.

## Prerequisites

- Linux x86_64 or ARM64 (see [Install](../getting-started/install.md)).
- A non-root user to run the daemon.
- Sufficient disk — plan for `record_count × value_size × 2` as a rough envelope (includes indexes).

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

shard-db listens on all interfaces by default (the `PORT` in db.env). **Bind to localhost only and put a reverse proxy in front** for any non-trivial deployment:

- Bind to `127.0.0.1`: prevents direct access from outside, forces traffic through the proxy, keeps TLS at the edge.
- Today, the bind address is hard-coded in `src/db/server.c`. For production, wrap with a proxy even if you want it "open" — IP/TLS gating is easier there.

## TLS via HAProxy (recommended)

shard-db speaks plaintext TCP only. Terminate TLS at a reverse proxy.

### Why HAProxy

- Purpose-built for TCP+TLS termination.
- Reloads certs without dropping connections.
- Simplest config for a single TCP backend.

Alternatives: nginx `stream` module (fine — pick it if you already run nginx), stunnel (tiny, good for dev).

### 1. Get a certificate

```bash
# Self-signed (dev / internal)
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj '/CN=shard-db.internal'

# HAProxy wants cert + key in one file
cat cert.pem key.pem > /etc/haproxy/shard-db.pem
chmod 600 /etc/haproxy/shard-db.pem
```

Production: use Let's Encrypt or your corporate CA.

### 2. Install HAProxy

```bash
# Debian/Ubuntu
apt install haproxy
# Arch
pacman -S haproxy
# RHEL/CentOS
dnf install haproxy
```

### 3. Configure

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

### 4. Start

```bash
sudo systemctl enable --now haproxy
```

### 5. Verify

```bash
ss -tlnp | grep 9200
echo '{"mode":"db-dirs"}' | openssl s_client -connect localhost:9200 -quiet 2>/dev/null
```

Client connects to port 9200 with TLS; HAProxy decrypts and forwards to shard-db on 127.0.0.1:9199. See the README for Python / Node.js / Java TLS client snippets.

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

- Stop the server (`systemctl stop shard-db` — calls graceful shutdown).
- Replace the binary.
- Start (`systemctl start shard-db`).
- On startup, any stale `.new`/`.old` rebuild files from the previous run are swept.

No DB migration step — schemas are per-object and live in `fields.conf`; the binary format is stable within a major version.

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
