# Audit — Phase 2 area 3: connection lifecycle (2026-11-25)

Read-only audit; symbols in `src/db/server.c` (`cmd_server`, `worker_thread`,
`server_process_fast`, `tls.c`).

## Model

`cmd_server` runs a poll-based accept loop on one `AF_INET` socket bound to
`INADDR_ANY`. Accepted fds go to a work queue; `WORKERS` (default CPU count,
min 4, max 1024) worker threads pop an fd and **serve that connection until
EOF**, reading newline-framed requests with `fgets` into a
`MAX_REQUEST_SIZE` buffer and answering each with `\0\n`.

## AUD-CONN-1 — idle connections starve the worker pool (major, confirmed)

- Only `SO_REUSEADDR` and `TCP_NODELAY` are set; there is no `SO_RCVTIMEO`,
  no keep-alive and no idle limit. The per-query `TIMEOUT` is a query
  deadline, not a socket timeout.
- A worker is bound to a connection for its whole life, accept needs no
  authentication, and TLS mode runs `tls_accept` (blocking handshake) inside
  the worker too.
- So `WORKERS` idle or half-open connections from any peer (even
  unauthenticated, even sending nothing or a partial line) make the server
  unresponsive to everyone; further connections wait in the queue.
- Live check (WORKERS=4): with 4 idle unauthenticated sockets open, an
  authenticated `db-dirs` timed out; it answered again once they closed.
- Repro: `audit-repro-idle-connections` (`--include-audit`), red.
- Fix (Phase 4): socket read timeout / idle limit (new `db.env` key with a
  safe default), handshake timeout for TLS, optionally a per-IP connection
  cap and a reserved worker for trusted addresses. Breaking: idle clients
  beyond the limit get disconnected (document; `shard-cli` and pooled
  clients must reconnect).

## AUD-CONN-2 (minor) — peer address defaults to loopback

`worker_thread` initialises `client_ip` to `"127.0.0.1"` and only overwrites
it when `getpeername` succeeds. A failed `getpeername` therefore leaves the
connection treated as the trusted loopback address while localhost trust is
on. In practice a connection whose peer is already gone cannot carry request
data, so this is not exploitable today, but the default should fail closed
(use a non-trusted placeholder).

## Checked, no defect

- Oversized request: `fgets` filling the buffer without a newline triggers an
  error response and the rest of the line is drained to the newline; the
  stream stays framed (`\0\n` is still emitted). The drain loop is unbounded
  in time, which is the AUD-CONN-1 problem again, not a separate bug.
- A NUL byte inside a request line truncates the request at the NUL (the
  rest of that line is ignored); no memory issue.
- `QUIT` ends the connection; shutdown wakes idle workers by
  `shutdown(SHUT_RDWR)` on registered fds (`g_worker_cfds`); a worker
  allocation failure now aborts startup cleanly (this campaign's fix).
- IPv6 is unsupported by design (`AF_INET`).

## Not covered

TLS handshake failure and teardown paths line by line (`tls.c`,
`tls_fopen` cookie functions); partial writes on slow readers (response side
backpressure); fd exhaustion behavior at `accept`.
