# Quick start

Five minutes from zero to querying typed data. Assumes you've already [installed](install.md) and built shard-db.

## 1. Start the server

```bash
cd build/bin
./shard-db start
./shard-db status
```

You should see `running (pid ..., port 9199)`.

## 2. Create an object

An **object** is shard-db's equivalent of a table: a typed schema (from `fields.conf`), per-object keyfile shards under `data/kf/`, append-only segment files under `data/streams/`, and optional indexes.

```bash
./shard-db query '{
  "mode": "create-object",
  "dir": "default",
  "object": "users",
  "splits": 16,
  "max_key": 128,
  "fields": [
    "name:varchar:100",
    "email:varchar:200",
    "age:int",
    "balance:numeric:19,2",
    "active:bool",
    "created:datetime:auto_create"
  ],
  "indexes": ["email", "age", "active"]
}'
```

Returns:
```json
{"status":"created","object":"users","dir":"default","splits":16,"max_key":128,"storage_version":2,"streams":8,"value_size":...,"fields":6}
```

- `splits: 16` → 16 keyfile shards (`data/kf/000.kf`..`00f.kf`). Each holds a packed array of 24-byte slot headers; the values live separately in `data/streams/`.
- `streams: 8` → derived from `nproc` (≤ 8 → nproc; ≤ 16 → 8; else 16). Inserts hash to one stream and append to its active segment file — parallel writers contend per stream, not per shard.
- `max_key: 128` → keys up to 128 bytes. Stored inline with the value record in the segment file. UUIDs fit in 36 bytes.
- `storage_version: 2` → slotcask engine version slot, kept in the response for forward compatibility. The slotcask engine is the only supported layout as of 2026.05.5; legacy v1 (probe-into-slot) was removed.
- `indexes: ["email","age","active"]` → three indexes built on first insert: `email` and `age` as B+ trees (4 idx-shard files each at `splits=16`); `active` declared bare on a `bool` field promotes to a bitmap index (16 `.bm` shard files, 1:1 with data shards).
- `created:datetime:auto_create` → server fills in the current datetime on every INSERT.

See [Concepts → Storage model](../concepts/storage-model.md) for what's actually on disk.

## 3. Insert a record

```bash
./shard-db insert default users u1 '{
  "name":"Alice",
  "email":"alice@example.com",
  "age":30,
  "balance":"1500.75",
  "active":true
}'
```

`created` is filled by the server (not shown here). Numeric fixed-point values like `balance` are passed as strings so you don't lose precision through a JSON parser.

## 4. Read it back

```bash
./shard-db get default users u1
```
```json
{"name":"Alice","email":"alice@example.com","age":30,"balance":"1500.75","active":true,"created":"20260418153012"}
```

## 5. Find by indexed field

```bash
# NQL — human-readable filter string (recommended for interactive use)
./shard-db find default users 'age > 25'

# Equivalent JSON criteria (for programmatic use or advanced options)
./shard-db find default users '[{"field":"age","op":"gt","value":"25"}]'
```

Returns a JSON array of matching records. Because `age` is indexed, this is a 1–3 ms B+ tree range scan rather than a full shard scan.

NQL supports the full filter grammar — AND/OR, BETWEEN, IN, string ops, and more. See [Query protocol → NQL](../query-protocol/nql.md) for the complete reference, or [Query protocol → find](../query-protocol/find.md) for JSON mode options.

## 6. Aggregate

```bash
# NQL — inline aggregate spec
./shard-db aggregate default users count(),avg(age) --group-by active

# JSON mode — full control over aliases and criteria
./shard-db query '{
  "mode":"aggregate","dir":"default","object":"users",
  "group_by":["active"],
  "aggregates":[
    {"fn":"count","alias":"n"},
    {"fn":"avg","field":"age","alias":"avg_age"}
  ]
}'
```

## 7. Bulk insert (JSON)

```bash
cat > /tmp/users.json <<'EOF'
[
  {"key":"u2","value":{"name":"Bob","email":"b@x.com","age":22,"balance":"10.00","active":true}},
  {"key":"u3","value":{"name":"Carol","email":"c@x.com","age":45,"balance":"999.99","active":false}}
]
EOF

./shard-db bulk-insert default users /tmp/users.json
```

Indexes on `email` and `age` are maintained inline by the pre-commit hook (no separate "build" step). Bulk-insert is an upsert — overwriting an existing key drops its stale index entries before writing the new value. Pass `if_not_exists:true` to keep the old idempotent behaviour. Throughput on a modern laptop tops 5 M rows/sec single-connection, 7.5 M/sec at 5 parallel connections (key/value workload); the wide-record invoice schema with 14 indexes lands in the 300–500 k rows/sec band.

The dict form `{"u2":{...},"u3":{...}}` is equivalent — round-trips with `get-multi` and `bulk-update`.

## 8. Upload a file

```bash
./shard-db put-file default users /tmp/avatar.png
./shard-db get-file default users avatar.png /tmp/got.png
```

Files ride the same TCP socket as queries — no separate upload protocol. See [Query protocol → file storage](../query-protocol/files.md) for size caps and the server-local zero-copy variant.

## 9. Graceful shutdown

```bash
./shard-db stop
```

`stop` waits for in-flight writes to drain before exiting. Crash-safe: each write commits via an atomic 8-byte store into the keyfile only *after* the value has been written to the active segment file, so a crash mid-write leaves the segment slot orphaned (reclaimed on next vacuum) — never a torn record. Any `.new` rebuild artifacts from interrupted resplits or vacuum runs get swept on the next start.

## Where to go next

- [Query protocol → NQL](../query-protocol/nql.md) — human-readable filter syntax for find / count / aggregate.
- [Query protocol → Overview](../query-protocol/overview.md) — the full JSON API shape.
- [Concepts → Typed records](../concepts/typed-records.md) — every field type + how defaults work.
- [Concepts → Indexes](../concepts/indexes.md) — when a query uses an index, composite indexes, cost.
- [CLI reference](../cli/index.md) — one page listing every command.
- [Operations → Deployment](../operations/deployment.md) — systemd unit, native TLS or reverse-proxy TLS, auth.
