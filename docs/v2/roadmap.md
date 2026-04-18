# Roadmap — v2

Features on the v2 backlog. Scope is provisional until features get committed into a release branch.

## Native TLS

**Today:** terminate at a reverse proxy (HAProxy / nginx / stunnel). See [Deployment → TLS](../operations/deployment.md#tls-via-haproxy-recommended).

**v2 target:** OpenSSL-backed TLS directly in the server — `tls_enable=true` in `db.env`, paths to `tls_cert` and `tls_key`. Removes the reverse-proxy requirement for simple deployments.

**Why it's a v2 (not v1) thing:** TLS termination at the proxy is already production-quality. Doing it in-process adds a dependency (OpenSSL / BoringSSL), a build-time choice, and config surface. For most shops, the proxy is the right answer.

## Replication

**Today:** single-node only. For HA, use block-level replication (DRBD, SAN) or filesystem replication (ZFS send/recv).

**v2 target:** leader/follower streaming replication. Writes replicate asynchronously to one or more followers; reads can fan out to followers with eventual consistency.

Open design questions:
- Protocol shape (log-shipping vs logical apply).
- Recovery semantics (follower crash → rebuild from leader or wait for WAL gap).
- Topology: single-leader, multi-leader, or gossip.

## Streaming binary file protocol

**Today:** files ride inside JSON as base64. Simple, remote-safe, but bounded by `MAX_REQUEST_SIZE` (~24 MB effective file size).

**v2 target:** length-prefixed binary body after the JSON header. Unbounded file size, no base64 overhead.

Shape (provisional):

```
{"mode":"put-file","dir":"...","object":"...","filename":"...","size":N}\n
<N raw bytes>
```

Server reads the JSON header line, parses, then reads exactly `N` bytes from the socket. Symmetric for download.

**Why v2, not v1:** needed only for files > ~24 MB. Base64 covers invoices, images, PDFs, and most practical blobs. The binary framing path is a meaningful protocol extension that clients have to implement.

## Unlimited fields

**Today:** `MAX_FIELDS = 256` per schema (bumped from 64 in 2026.04.2).

**v2 target:** remove the hard cap. The limit is internal: several per-object structures allocate arrays of `MAX_FIELDS` entries. Dynamic allocation is straightforward; the cost is a lot of small refactors.

Practical concern: 256 typed columns is already a lot for a flat-schema database. This is a roadmap item but not a pressing one.

## Delete / list files

**Today:** no `delete-file` or `list-files` mode. Delete by removing the file directly (`rm <db_root>/<dir>/<obj>/files/XX/XX/<filename>`). List by walking the directory yourself.

**v2 target:**
- `{"mode":"delete-file","dir":"...","object":"...","filename":"..."}`
- `{"mode":"list-files","dir":"...","object":"...","prefix":"...","limit":N}` with pagination.

Low-complexity additions to the existing file-storage module.

## Per-tenant auth

**Today:** a single token pool (`tokens.conf`) grants access to every tenant. Isolation is at the filesystem/validation layer, not at the credential layer.

**v2 target:** per-tenant tokens — each `dir` gets its own allowlist, tokens scoped to one or more tenants. Structure:

```
$DB_ROOT/
  tokens.conf                 # global admin tokens
  <dir>/
    tokens.conf               # tenant-scoped tokens
```

Useful for multi-customer deployments where one token shouldn't cross tenants.

## OR in criteria

**Today:** `criteria` is AND-combined. Emulate OR with multiple queries + dedupe or `in` sets.

**v2 target:** explicit `"or": [...]` branch in criteria:

```json
{
  "criteria": [
    {"field": "status", "op": "eq", "value": "paid"},
    {"or": [
      {"field": "priority", "op": "eq", "value": "high"},
      {"field": "total", "op": "gte", "value": "1000"}
    ]}
  ]
}
```

Requires planner changes — picking an index across OR branches is harder than across AND conjuncts.

## Right / full outer joins

**Today:** inner and left only.

**v2 target:** right (equivalent to left with reversed driver) and full outer (union of left + right). Implementation-heavy because of the "driver" model — the driver is always the outermost object.

## Paid tier differentiation

Separate from open-source v2:

- Encryption at rest.
- Fine-grained per-tenant quotas (CPU, disk).
- Managed backup / restore UI.
- Enterprise support SLAs.

Details TBD — not feature work today.

## What's explicitly not planned

- **Windows / macOS native** — the `epoll`+`mmap` model is deliberate. Containers cover the cross-platform case.
- **Multi-master replication with strong consistency** — beyond scope. Use a different database if you need that.
- **Query language (SQL)** — the JSON protocol is the language. If you want SQL, parse it client-side and emit JSON.
- **In-process embedded mode** — always a server, always over TCP. Simpler mental model, simpler auth story.

## How to propose changes

Open an issue on the [GitHub repo](https://github.com/sayyiditow/shard-db/issues) with:

- The problem you're solving.
- The workaround you're currently using.
- A rough proposal for the wire-protocol change (if any).

v2 is a working list, not a commitment. Priorities shift based on what users actually hit.
