# Query protocol overview

shard-db speaks one wire protocol: **newline-delimited JSON over TCP**. Every request is a JSON object on a single line terminated by `\n`; every response is terminated by `\0\n` (NUL byte + newline).

## Wire format

### Request

```
{"mode":"...","dir":"...",...}\n
```

- One JSON object per line. No comments, no trailing comma.
- Maximum per-request size = `MAX_REQUEST_SIZE` (default 32 MB). Oversized requests are rejected with `{"error":"Request too large (max N bytes)"}`.
- You can pipeline multiple requests on the same connection — the server processes them in order.

### Response

```
{"status":"ok",...}\n\0\n
```

- JSON object (or array) followed by `\0\n`.
- Clients **read until `\0`** to frame a response. The trailing `\n` is separator between responses; most clients can ignore it.
- For pipelined requests, responses come back in order, each terminated by `\0\n`.

## Minimal request shape

Every request has:

```json
{
  "mode": "<command-name>",
  "dir": "<tenant>",
  "object": "<object-name>",
  "...": "mode-specific fields"
}
```

Except for global modes (`stats`, `db-dirs`, `vacuum-check`, auth admin, `create-object`) that don't need `object`, or modes that explicitly accept `keys:[...]` instead of a single `key`.

## Authentication

From trusted IPs (`allowed_ips.conf` + default localhost), no credential is required. Otherwise, include a token:

```json
{"mode":"get","dir":"default","object":"users","key":"u1","auth":"<token>"}
```

Tokens are stored in `$DB_ROOT/tokens.conf`. See [Getting started → Configuration](../getting-started/configuration.md).

## Modes at a glance

### Data operations
- [`get`](#get), `insert`, `update`, `delete`, `exists`, `not-exists`, `size` — per-record CRUD and existence checks
- [`find`](find.md), [`count`](count.md), `keys`, `fetch` — queries

### Aggregation and joins
- [`aggregate`](aggregate.md) — count/sum/avg/min/max with group_by + having
- [Joins in `find`](joins.md) — inner/left, by PK or indexed field

### Bulk
- [`bulk-insert`](bulk.md), `bulk-insert-delimited`, [`bulk-delete`](bulk.md), [`bulk-update`](bulk.md) — all with optional `dry_run`

### Conditional writes
- [CAS](cas.md) — `if_not_exists`, `if:[...]`

### Schema
- [`create-object`](schema-mutations.md), [`add-field`](schema-mutations.md), [`remove-field`](schema-mutations.md), [`rename-field`](schema-mutations.md), [`vacuum`](schema-mutations.md), `truncate`, `recount`, `backup`

### Indexes
- [`add-index`](index-management.md), [`remove-index`](index-management.md)

### Files
- [`put-file`](files.md), [`get-file`](files.md), `get-file-path`

### Sequences (monotonic counters)
- `sequence` — `action: init | next | current | reset`; optional `batch` for bulk allocation

### Diagnostics
- [`stats`](diagnostics.md), `shard-stats`, `db-dirs`, `vacuum-check`

### Auth admin (trusted-IP only)
- `add-token`, `remove-token`, `list-tokens`, `add-ip`, `remove-ip`, `list-ips`

## `get` — the simplest round-trip

```json
// Request
{"mode":"get","dir":"default","object":"users","key":"u1"}

// Response
{"key":"u1","value":{"name":"Alice","email":"a@x.com","age":30}}
```

Multi-get:

```json
// Request
{"mode":"get","dir":"default","object":"users","keys":["u1","u2","u3"]}

// Response
[{"key":"u1","value":{...}},{"key":"u2","value":{...}}]
// (missing keys are omitted)
```

With field projection:

```json
// Request
{"mode":"get","dir":"default","object":"users","key":"u1","fields":"name,email"}

// Response
{"key":"u1","value":{"name":"Alice","email":"a@x.com"}}
```

## Pagination patterns

| Pattern | Use when | Speed |
|---|---|---|
| `offset` + `limit` | UI-style page numbers | O(matches) — fine for shallow pages, expensive deep |
| `cursor` (returned by `fetch`) | Stream through an entire object | O(limit) — constant cost per page |

## Error shape

```json
{"error":"<machine-readable-string>","..."}
```

Common errors:

| Error | Cause |
|---|---|
| `Unknown dir: <name>` | `dir` not in `dirs.conf`. |
| `Object [<name>] not found. Use create-object first.` | Object doesn't exist. |
| `Request too large (max N bytes)` | Request exceeded `MAX_REQUEST_SIZE`. |
| `condition_not_met` | CAS check on insert/update/delete failed. |
| `invalid filename` | Upload/download filename violated rules. |
| `file exists` | Upload with `if_not_exists:true` hit an existing file. |

See [Reference → Error codes](../reference/error-codes.md) for a full list.

## Clients

### Python

```python
import socket, json

def query(request, host="localhost", port=9199):
    s = socket.socket()
    s.connect((host, port))
    s.sendall((json.dumps(request) + "\n").encode())
    s.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        chunk = s.recv(65536)
        if not chunk: break
        data += chunk
    s.close()
    text = data.decode().strip().rstrip("\x00").strip()
    return json.loads(text) if text else None
```

### Node.js

```javascript
const net = require("net");

function query(request, host = "localhost", port = 9199) {
  return new Promise((resolve, reject) => {
    const s = net.connect(port, host);
    let data = "";
    s.on("connect", () => { s.write(JSON.stringify(request) + "\n"); s.end(); });
    s.on("data", chunk => data += chunk.toString());
    s.on("end", () => {
      try { resolve(JSON.parse(data.replace(/\0/g, "").trim())); }
      catch { resolve(data); }
    });
    s.on("error", reject);
  });
}
```

### Pipelining

Send multiple requests on one socket — responses come back in order, each terminated by `\0\n`. See the README's Python/JavaScript sections for a `pipeline()` helper.

### Shell (diagnostic only)

```bash
echo '{"mode":"get","dir":"default","object":"users","key":"u1"}' | nc -q1 localhost 9199
```

## Where to go

- [`find`](find.md) — the most-used query mode; all 17 operators, joins, sorting, projection.
- [`aggregate`](aggregate.md) — group-by + having.
- [CAS](cas.md) — conditional writes.
- [Bulk](bulk.md) — bulk-insert, bulk-update, bulk-delete.
- [Schema mutations](schema-mutations.md) — evolve objects without downtime.
