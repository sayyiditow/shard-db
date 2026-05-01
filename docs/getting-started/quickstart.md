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

An **object** is shard-db's equivalent of a table: a typed schema (from `fields.conf`), one or more shard files under `data/`, and optional indexes.

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
  "indexes": ["email", "age"]
}'
```

Returns:
```json
{"status":"created","object":"users","dir":"default","splits":16,"max_key":128,"value_size":..., "fields":6}
```

- `splits: 16` → 16 shard files (`data/000.bin`..`00f.bin`).
- `max_key: 128` → keys up to 128 bytes. Keys larger than this bloat slot size; UUIDs fit in 36 bytes.
- `indexes: ["email","age"]` → two B+ tree indexes built on first insert.
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
./shard-db find default users '[{"field":"age","op":"gt","value":"25"}]'
```

Returns a JSON array of matching records. Because `age` is indexed, this is a 1–3 ms B+ tree range scan rather than a full shard scan. See [Query protocol → find](../query-protocol/find.md) for every option.

## 6. Aggregate

```bash
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
  {"id":"u2","data":{"name":"Bob","email":"b@x.com","age":22,"balance":"10.00","active":true}},
  {"id":"u3","data":{"name":"Carol","email":"c@x.com","age":45,"balance":"999.99","active":false}}
]
EOF

./shard-db bulk-insert default users /tmp/users.json
```

Indexes on `email` and `age` are built in parallel. Expect ~130 k inserts/sec for single-request bulk loads on a typical laptop.

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

`stop` waits for in-flight writes to drain before exiting. Crash-safe: any `.new` / `.old` rebuild artifacts get swept on the next start.

## Where to go next

- [Query protocol → Overview](../query-protocol/overview.md) — the full JSON API shape.
- [Concepts → Typed records](../concepts/typed-records.md) — every field type + how defaults work.
- [Concepts → Indexes](../concepts/indexes.md) — when a query uses an index, composite indexes, cost.
- [CLI reference](../cli/index.md) — one page listing every command.
- [Operations → Deployment](../operations/deployment.md) — systemd unit, reverse proxy for TLS, auth.
