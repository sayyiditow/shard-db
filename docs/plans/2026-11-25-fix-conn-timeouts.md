# Fix plan 4 — idle and slow connections must not starve the worker pool (AUD-CONN-1, AUD-CONN-2)

## Brief
**Problem and root cause.** A worker serves one connection until EOF, sockets have no timeouts, accept needs no
authentication, and TLS handshakes run inside the worker. `WORKERS` idle or half-open connections stop the server
(repro `audit-repro-idle-connections`). `client_ip` defaults to `127.0.0.1` when `getpeername` fails.

**Logical flow.**
1. New `db.env` keys `IDLE_TIMEOUT_SEC` (default 300; 0 = off) and `HANDSHAKE_TIMEOUT_SEC` (default 10).
2. After accept, the worker sets `SO_RCVTIMEO` to the idle timeout (and `SO_SNDTIMEO` to a send timeout) on the
   socket; `fgets` returning EOF/EAGAIN on timeout closes the connection with a log line. For TLS, `SO_RCVTIMEO` is
   set to the handshake timeout before `tls_accept` and raised to the idle timeout after it succeeds.
3. A request in progress is not interrupted: the timeout applies only while waiting for the next byte.
4. `client_ip` defaults to `"0.0.0.0"` (never trusted); if `getpeername` fails the connection is closed.

**Decisions.**
| Decision | Alternatives | Why | Tag |
|---|---|---|---|
| Idle default 300 s | off by default | an unauthenticated peer can otherwise stall the server | NEEDS YOU (pooled clients idling longer than 300 s are disconnected and must reconnect; `shard-cli` too) |
| Handshake default 10 s | 30 s | handshakes are sub-second | DEFAULT |
| Close on `getpeername` failure | keep going | fail closed | DEFAULT |
| No per-IP connection cap now | add cap | separate hardening; timeouts fix the starvation | DEFAULT |

**Blast radius.** `worker_thread`, `tls_accept` path, `config.c` db.env parsing, `db.env.example`, docs
(`configuration.md`); `src/cli` reconnect behavior; benches and test clients that hold idle connections
(`test_client` keeps persistent connections: tests that sleep > timeout need the key set).

**Tests.** `audit-repro-idle-connections` green; connection idle past the timeout is closed and a new request works;
slow-loris partial line is closed after the timeout; a long-running query is not killed by the idle timeout; TLS
handshake stall times out (if TLS test fixtures exist, else documented gap); `0` disables.

**Out of scope.** Per-IP caps, reserved workers for trusted addresses.

**Review log.** none yet.

## Plan (executor)
Branch `fix/conn-timeouts`. 1. Red test pre-exists; add the idle-close and long-query cases. 2. config.c: parse the
two keys with range checks (`g_idle_timeout_sec`, `g_handshake_timeout_sec` on ShardDb). 3. server.c: set socket
options in `worker_thread` before the TLS/plain branch; handle `EAGAIN`/`EWOULDBLOCK` from `fgets` as idle timeout;
fail-closed `client_ip`. 4. `db.env.example`, `docs/getting-started/configuration.md`, changelog. 5. Full suite
(tests that idle long: set `IDLE_TIMEOUT_SEC=0` in their env, record which). 6. Uncommitted.
