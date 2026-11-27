# Audit threat model — shard-db campaign 2026-11-19

Status: **provisional draft — awaiting human approval** (plan Phase 0 step 2; Phase 1
may proceed against this draft meanwhile). Basis: `audit-base` = f19b17d (2026.11.2).
Companion docs: campaign plan `2026-11-19-audit-campaign.md`, index
`2026-11-19-audit-index.md`. All symbols cited at audit-base.

## Assets (what the campaign must show is not lost, corrupted, or exposed)

1. **Acknowledged records** — durability and byte-correctness of every acknowledged
   write; no resurrection of deleted records; no torn/clobbered reads. This is the
   DL (data-loss) class; it outranks everything below regardless of adversary.
2. **Index/bitmap/trigram consistency with records** — a query that silently omits
   data the records contain is DL per the plan's Priority section.
3. **Tenant isolation** — one tenant's data is invisible and unmodifiable to
   another tenant's tokens.
4. **Auth secrets** — token strings in `tokens.conf` (any level); token-management
   modes must not leak or allow forging them beyond admin scope.
5. **Backup/restore fidelity** — a backup restores to exactly the backed-up state.
6. **Availability** — secondary per the human directive; relevant only as
   *documented-cap* conformance (caps that silently don't apply are a finding;
   best-effort DoS resistance beyond documented caps is not in scope).

## Adversaries

| # | Adversary | Capability | Primary goals |
|---|---|---|---|
| A1 | Unauthenticated network peer | Any bytes/frames on `PORT` (TCP; TLS when enabled), no token | Read/write data without a token; crash or corrupt via parser bugs |
| A2 | Authenticated tenant token (scope = one dir) | Valid `r`/`rw`/`rwx` token at `$DB_ROOT/<dir>/tokens.conf` | Escalate to other dirs (tenant isolation), to server-admin modes, or above perm; path traversal out of its dir |
| A3 | Authenticated object token (scope = one object) | Valid token at `$DB_ROOT/<dir>/<obj>/tokens.conf` | Same as A2, one level narrower |
| A4 | Authenticated read-only token (perm = `r`) | Any scope | Perform writes/mutations above perm |
| A5 | Local user with `$DB_ROOT` file access | Can read/modify/create conf and data files as its own user | Not defendable against for confidentiality/integrity *by design* (root-trust model); in scope only as: daemon must parse adversarial/malformed conf files fail-closed without memory unsafety or privilege amplification |

## Trust boundaries and invariants

1. **Wire framing** (A1–A4): newline-delimited requests; three request grammars —
   JSON (`dispatch_json_query`, server.c), legacy `\x1F`-delimited
   (`server_process_fast`, server.c), NQL text (`dispatch_nql_query`/`nql.c`);
   response framed by `\0\n`. Invariants: unauthenticated peer reaches *no* data;
   every grammar parse-fails to an error, never to memory unsafety or a crash;
   caps hold under adversarial framing (`MAX_REQUEST_SIZE` mid-frame, `MAX_LINE`,
   `MAX_ARGS=8` fast path).
2. **TLS** (A1): optional, off by default; `TLS_ENABLE=1` makes `PORT` TLS-only,
   TLS 1.3; client identity = tokens (no mTLS by design); SNI/verify default
   `localhost` (`TLS_SERVER_NAME` override); no cert hot-reload (restart).
   Invariants: handshake/teardown failure paths fail the connection cleanly; no
   plaintext fallback on the TLS port; `tls_fopen` FILE* wrapper never
   double-frees or leaks on error paths.
3. **Auth: scope × perm × admin level** (A2–A4): `mode_admin_level` (NONE /
   OBJECT / TENANT / SERVER), `mode_is_data_write`, and the early admin gate
   (server.c `add-token`/`remove-token`/`add-ip`/`remove-ip`/`list-tokens`/
   `list-ips`/`add-dir`/`remove-dir`); trusted-IP bypass via
   `allowed_ips.conf` (global only); localhost trusted by default,
   `DISABLE_LOCALHOST_TRUST=1` for strict. Invariant: **every mode** (diagnostics
   included) enforces its declared level for the token's scope; no mode is
   reachable above its token's perm; the early-gate modes never grant more than
   their documented admin surface.
4. **File paths** (A2, A3): `valid_filename` (util.c), `is_valid_object` (util.c),
   `is_valid_dir` (config.c), `build_effective_root` (config.c). Invariant: no
   traversal out of the token's dir/object/file scope via any request field
   (`dir`, `object`, filename in file modes, sequence names, field names in
   index paths).
5. **Conf-file parsers** (A5, plus A2/A3 for effects of files they may write at
   their own scope): `tokens.conf` (3 levels), `dirs.conf`, `schema.conf`
   (incl. `auto_key=` tail scan), `fields.conf`, `db.env`, `allowed_ips.conf`.
   Invariants: malformed content fails closed (no auth grant, no start with
   silently degraded security), no memory unsafety, no crash loop.
6. **Platform** (none — conformance): macOS differences (`sem_init`
   unsupported, `F_FULLFSYNC`, `SEEK_DATA`) must not silently disable a
   documented protection (e.g. `MAX_CONCURRENT_QUERIES`).

## Severity definitions per boundary

**DL class always outranks these**: any finding in the plan's DL definition is
DL regardless of boundary or adversary, needs a repro, and is fixed first.

- **Wire**: data read/write or memory-unsafety reachable without a token (A1) =
  blocker. Bounded, documented reject = pass. Undocumented/unbounded resource
  consumption from a single connection = major. Response-shape drift vs docs on
  any grammar = major (clients branch on JSON type).
- **Auth**: any scope, perm, or admin-level escalation for any mode (incl.
  diagnostics and the early-gate set) = blocker. Behavior contradicting
  docs/concepts/multi-tenancy.md = major. Trusted-IP/localhost semantics
  differing from docs = major.
- **Paths**: any traversal outside the token's scope via a request field =
  blocker. Correctly rejected near-miss = note only.
- **Conf parsers**: memory unsafety = blocker; fail-open (malformed input
  grants auth, weakens caps, or starts with degraded security) = blocker;
  safe-but-crashy (refuses start on benign input) = major.
- **TLS**: plaintext fallback / handshake failure leaking memory = blocker;
  error-path fd/FILE* leaks = major.
- **Platform**: silently disabled documented cap = major.

## Out of scope

- shard-cli internals (deferred by approved decision).
- DoS resistance beyond the documented caps (secondary per human directive).
- Defending data files against A5 (local file access is root-trust by design);
  only parser robustness for A5-supplied conf content is in scope.
- npm/embedded consumers as a separate surface (covered inside mode slices per
  plan).
