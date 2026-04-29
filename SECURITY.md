# Security policy

## Supported versions

shard-db follows a `yyyy.mm.N` release scheme. Security fixes are
backported only to the **latest release line in the current calendar
month**. Older releases are end-of-life as soon as a newer one ships.

| Version line | Supported |
|---|---|
| 2026.05.x (latest) | ✓ |
| 2026.04.x | end-of-life |
| < 2026.04 | end-of-life |

## Reporting a vulnerability

Please report security vulnerabilities **privately**, not through public GitHub Issues.

**Preferred:** GitHub's private vulnerability reporting:
<https://github.com/sayyiditow/shard-db/security/advisories/new>

**Alternative:** email **hashim.sayyidinyo@gmail.com** with subject line `shard-db security`. PGP welcome but not required.

Please include:

- A clear description of the vulnerability.
- Reproduction steps (input, expected vs. observed behaviour, daemon log if available).
- Affected version(s) — the output of `./shard-db --version` or the git SHA you tested.
- Whether the issue is exploitable from an unauthenticated TCP byte, an authenticated read-only token, or only by an admin token.

## What to expect

| Step | Target |
|---|---|
| Initial acknowledgement | within 72 hours |
| Triage + severity assessment | within 7 days |
| Fix or remediation plan | within 30 days for critical / high; 60 days for medium / low |
| Public advisory + patched release | coordinated with the reporter |

We follow the convention that **critical** issues block release work until fixed; **high** issues land in the next monthly release at the latest.

## Out of scope

- Denial-of-service via legitimate-but-expensive queries (the planner can't bound that — use the `TIMEOUT` / `timeout_ms` / `QUERY_BUFFER_MB` knobs to mitigate).
- Disk-exhaustion via legitimate-but-large bulk inserts (operate behind a quota or rate-limit).
- Cleartext token transmission with `TLS_ENABLE=0` to a non-loopback peer — the CLI now refuses this fail-closed, but if you bypass that guard with a custom client, plaintext credentials over plaintext TCP is operating-it-wrong, not a vulnerability.
- Issues in code paths that require admin (`rwx`) access — admins by definition can already do the relevant operation legitimately.

## Hall of fame

Security researchers who report valid issues are credited (with their consent) in the release advisory. No bug bounty program at present.
