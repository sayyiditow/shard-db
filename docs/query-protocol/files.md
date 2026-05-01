# File storage

Upload and download arbitrary files (PDFs, images, CSVs, blobs) keyed by filename. Files live flat under `<obj>/files/<filename>` — the basename is the lookup key.

> **Upgrading from pre-2026.05.2?** Storage was previously bucketed at `<obj>/files/<XX>/<XX>/<filename>`. Run `./shard-db migrate-files` once after upgrading the binary — it walks every (dir, object) and lifts each file into the flat layout. Idempotent, so safe to re-run.

Two variants for both upload and download:

1. **Bytes-in-JSON (base64) — remote-safe.** The default. Works from any host with TCP access to the server.
2. **Server-local path — zero-copy admin fast path.** Only useful when the caller can touch the server's filesystem.

Pick #1 unless you have a specific reason.

## Where files live

```
$DB_ROOT/<dir>/<obj>/files/
  hello.txt
  medium.bin
  small.bin
```

Flat — basename is the lookup key. No bucketing or sub-directories. `get-file` / `delete-file` / `get-file-path` all open `<files_dir>/<filename>` directly, no hashing.

## put-file — bytes in JSON (remote-safe)

### Shape

```json
{
  "mode": "put-file",
  "dir": "<dir>",
  "object": "<obj>",
  "filename": "invoice.pdf",
  "data": "<base64-encoded-bytes>",
  "if_not_exists": true
}
```

- `filename` — plain basename. No `/`, `\`, `..`, control chars, ≤255 bytes. Path-traversal attempts return `{"error":"invalid filename"}`.
- `data` — standard RFC 4648 base64 (with `+/=`). Whitespace inside the string is tolerated.
- `if_not_exists` (optional) — CAS. Fails with `{"error":"file exists",...}` if a file with this name already exists in the same `(dir, obj)`.

### Response

```json
{"status":"stored","filename":"invoice.pdf","bytes":12345}
```

On CAS conflict:
```json
{"error":"file exists","filename":"invoice.pdf"}
```

### Atomicity

Writes go to `<dest>.tmp.<pid>`, `fsync`ed, then `rename`d onto `<dest>`. A mid-upload crash leaves no partial file. Default is silent overwrite; add `if_not_exists:true` to refuse.

### Size cap

Inherited from `MAX_REQUEST_SIZE` (default 32 MB). Base64 inflates by 4/3, so the effective file-size cap is **~24 MB** at default config. Raise `MAX_REQUEST_SIZE` in `db.env` to lift it.

Every connection allocates a read buffer of `MAX_REQUEST_SIZE` — see [Operations → Tuning](../operations/tuning.md) before setting very high values.

### CLI

```bash
./shard-db put-file <dir> <obj> <local-path> [--if-not-exists]
```

The CLI reads the file, base64-encodes, sends the JSON. Works from any host with TCP access to the server.

## put-file — server-local path (admin fast path)

### Shape

```json
{"mode":"put-file","dir":"<dir>","object":"<obj>","path":"/srv/incoming/invoice.pdf"}
```

- `path` is read **on the server's filesystem** — not the client's. The server opens the path and copies it into `<obj>/files/<filename>` (filename = basename of the path).
- No base64 overhead. Good for batch ingestion from a shared volume.
- **Only useful for same-host callers** — a remote client has no way to place a file on the server's filesystem without a separate transport (scp, rsync, shared FS).

Response: `{"status":"stored","path":"<dest-path>"}`.

No CAS (`if_not_exists`) on this variant. Silent overwrite.

## get-file — bytes in JSON (remote-safe)

### Shape

```json
{"mode":"get-file","dir":"<dir>","object":"<obj>","filename":"invoice.pdf"}
```

### Response

```json
{"status":"ok","filename":"invoice.pdf","bytes":12345,"data":"<base64>"}
```

Not found:
```json
{"error":"file not found","filename":"invoice.pdf"}
```

### CLI

```bash
./shard-db get-file <dir> <obj> <filename> [<out-path>]
```

- With `<out-path>`: decodes base64 and writes raw bytes to the file.
- Without: writes raw bytes to stdout.

## get-file-path — server path (admin fast path)

### Shape

```json
{"mode":"get-file-path","dir":"<dir>","object":"<obj>","filename":"invoice.pdf"}
```

### Response

```json
{"path":"<db_root>/<dir>/<obj>/files/invoice.pdf"}
```

No bytes on the wire. The caller is expected to read the returned path directly from the server's filesystem. Useful for:

- Admin/debug: "where is this file?"
- Colocated services with shared-FS access.
- Large files where you want to stream via `cat` / `sendfile` instead of base64 over the socket.

## delete-file

### Shape

```json
{"mode":"delete-file","dir":"<dir>","object":"<obj>","filename":"invoice.pdf"}
```

### Response

```json
{"status":"deleted","filename":"invoice.pdf"}
```

Not found:
```json
{"error":"file not found","filename":"invoice.pdf"}
```

Same filename rules as `put-file` / `get-file` — `{"error":"invalid filename"}` on `/`, `\`, `..`, control chars, or > 255 bytes.

### CLI

```bash
./shard-db delete-file <dir> <obj> <filename>
```

## list-files

Paginated, alphabetical inventory of stored files for one object. Optional pattern match, returns total + page.

### Shape

```json
{
  "mode":"list-files",
  "dir":"<dir>",
  "object":"<obj>",
  "pattern":"2026-",
  "match":"prefix",
  "offset":0,
  "limit":100
}
```

- `pattern` — optional. Pattern to match filenames against (byte-exact for prefix/suffix/contains, glob syntax for `glob`). Missing or empty = match all.
- `match` — optional, one of `prefix` (default), `suffix`, `contains`, `glob`. `glob` uses `fnmatch(3)` — supports `*`, `?`, `[abc]` character classes.
- `prefix` — legacy field, still accepted; equivalent to `pattern:"..."` + `match:"prefix"`. Use `pattern` + `match` for new code.
- `offset` / `limit` — standard pagination. `limit` defaults to `GLOBAL_LIMIT` when absent or 0.

### Examples

```json
{"mode":"list-files","dir":"acme","object":"invoices","pattern":".pdf","match":"suffix"}
{"mode":"list-files","dir":"acme","object":"invoices","pattern":"2026-Q4","match":"contains"}
{"mode":"list-files","dir":"acme","object":"invoices","pattern":"INV-*.pdf","match":"glob"}
```

### CLI

```bash
./shard-db list-files <dir> <obj> [pattern] [offset] [limit] [--match=<mode>]
```

### Response

```json
{
  "files": ["2026-01-summary.pdf","2026-02-summary.pdf", ...],
  "total": 245,
  "offset": 0,
  "limit": 100
}
```

`total` is the unpaginated match count (after pattern filtering, before pagination). Single `opendir(<obj>/files)` + `readdir` loop — O(file count). Comfortable up to several million files on ext4/XFS; past that, maintain your own index in a regular object.

Invalid match mode returns `{"error":"invalid match mode (use prefix|suffix|contains|glob)"}`.

## Filename rules

Enforced by `valid_filename()`:

- Non-empty, ≤ 255 bytes.
- No `/` or `\` (plain basename only).
- No literal `..` as the whole name.
- No control characters (bytes `< 0x20` or `0x7F`).

Invalid filenames get `{"error":"invalid filename"}`.

## Choosing a variant

| Scenario | Recommendation |
|---|---|
| Remote client (Python, JS, Java, anywhere over TCP) | `put-file` with `data`, `get-file`. |
| Same-host admin script, batch ingestion | `put-file` with `path` — no base64 overhead. |
| Need to stream a large existing server-side file to another process | `get-file-path`, then `sendfile()`/`cat` the path. |
| Very large files (> 24 MB at default config) | Raise `MAX_REQUEST_SIZE` **or** use server-local path. |

## CLI examples

### Upload a PDF from a remote machine

```bash
./shard-db put-file acme invoices /home/me/Invoice-001.pdf
# → {"status":"stored","filename":"Invoice-001.pdf","bytes":183721}
```

### Download and verify

```bash
./shard-db get-file acme invoices Invoice-001.pdf /tmp/invoice.pdf
md5sum /home/me/Invoice-001.pdf /tmp/invoice.pdf
```

### CAS upload (refuse overwrite)

```bash
./shard-db put-file acme invoices /home/me/Invoice-001.pdf --if-not-exists
# → {"error":"file exists","filename":"Invoice-001.pdf"}
```

### Admin fast path (same host)

```bash
./shard-db query '{"mode":"put-file","dir":"acme","object":"invoices","path":"/srv/ingest/new.pdf"}'
./shard-db query '{"mode":"get-file-path","dir":"acme","object":"invoices","filename":"new.pdf"}'
# → {"path":"../db/acme/invoices/files/6b/7a/new.pdf"}
```

## Limitations

- File content isn't indexed or queryable — it's opaque storage. Use [`list-files`](#list-files) for inventory by filename prefix.
- No ranged reads — every `get-file` returns the whole file.
