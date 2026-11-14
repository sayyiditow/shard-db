# Backup

Three overlapping strategies, each covering a different failure mode. Pick at least two.

## 1. Per-object logical backup

Use the built-in `backup` command.

```bash
./shard-db backup <dir> <object>
```

Or via JSON:

```json
{"mode":"backup","dir":"<dir>","object":"<obj>"}
```

### What it copies

Everything for that object: `data/` (kf + streams), `indexes/`, `metadata/`, `files/` (the stored files written by `put-file`), the object's `tokens.conf`, `fields.conf`, and an `object.json` manifest carrying the splits/max_key/storage_version/streams the operator would need to recreate the object from scratch (plus a `"files":1` marker — see Restore below). A backup is therefore enough to recreate the same object — records, indexes, sequences, stored files and access tokens — in another database later (the object directory must exist there first, and the backup folder goes inside it, as `restore` requires). Destination is a timestamped subdirectory under the object's own `backup/` directory:

```
$DB_ROOT/<dir>/<obj>/backup/20260418_153012/
  object.json          # schema manifest (splits, max_key, storage_version, streams, layout, files)
  fields.conf
  tokens.conf          # the object's access tokens (copied 0600)
  data/
    kf/
    streams/
  indexes/
  metadata/
  files/               # put-file store (absent if the object has no stored files)
```

`tokens.conf` holds token secrets in plaintext — the backup copy is created with mode `0600`, but protecting the backup directory itself is the operator's responsibility. Backup runs under the object's exclusive lock and the copy time now grows with the stored-file volume, so backups of objects holding large stored files take correspondingly longer.

### Known windows (files/)

- `copy_file`/`cprf` errors are not reported: a backup whose `files/` copy partly failed (ENOSPC/EIO) still carries the `"files":1` marker, and a later `restore --force` wipes live `files/` and copies back the partial set.
- If an operator pruned `<backup>/files/` but kept the manifest, `restore --force` wipes live `files/` and copies nothing back.
- If a `restore --force` crashes after wiping live `files/` but before the copy completes, live `files/` stays empty (or partial) until `restore --force` is re-run; the backup itself is intact.

### When to use

- Pre-schema-migration checkpoint (before `add-field` / `remove-field` on production data).
- Before a risky `vacuum --splits N` or manual shard edit.
- One-off snapshots of a single object for export.

### Consistency

`backup` holds the object's **exclusive lock for the whole copy** — every request on that object, reads included, waits until the backup finishes, so the copy cannot observe a write, resplit, bitmap-repair marker or `sequence` update in flight. Backup waits for in-flight operations to drain exactly as `vacuum` does and, because the object lock is a default-attribute (reader-preferring on glibc) rwlock, may itself wait a long time under continuous overlapping traffic — quiesce clients on busy objects (requests parked behind the backup occupy their dispatcher workers, as behind `vacuum`). `./shard-db stop` waits for a running backup like it waits for writes. Requests waiting behind the backup are not subject to the per-request `timeout_ms` while queued (same as behind `vacuum`), so clients with short I/O timeouts (shard-cli included) may time out and should retry. An object already flagged bitmap-disabled is copied as-is (flag and markers included) and `restore` re-arms the escape until `reindex`. Backups taken by releases before this change were not fenced — if one was taken on a live object, run `reindex` after restoring it. Filesystem snapshots remain the recommendation for very large or always-busy objects.

### Restore

Use the `restore` mode (shipped 2026.05.1):

```bash
./shard-db query '{
  "mode":"restore",
  "dir":"<dir>",
  "object":"<obj>",
  "from":"20260418_153012",
  "force":true
}'
```

- `from` is the timestamp directory name under `<obj>/backup/`.
- `force:true` is required if the live object isn't empty — without it `restore` refuses to overwrite live data and tells you to pass `force=true` to proceed.
- **Stored files (`files/`):** a backup carrying the `"files":1` manifest marker replaces the live `files/` directory (with `force`, after the same non-empty refusal as `data/`); a backup taken before files support (no marker) leaves live `files/` untouched.
- **Tokens:** restore **adds** the backup's object tokens to the running daemon immediately (no restart) and rewrites the object's `tokens.conf`. It adds only tokens that are not already present — an existing token is never changed — and never removes any. Consequence: a token revoked after the backup was taken comes back if the backup contains it. Live tokens absent from the backup are kept. The object's `tokens.conf` is rewritten from the in-memory token set, so hand edits made after startup are lost — same as `add-token`. A full token table (`TOKEN_CAP`) makes the merge skip the remaining tokens and log it. This merge runs on the JSON protocol only.
- **Authorization:** `restore` requires an object-admin (`rwx`) token whose scope covers the object on the JSON protocol. The legacy text-wire protocol has no authentication at all (pre-existing), never restores tokens (a text-path restore leaves the object's `tokens.conf` and the running token set untouched — use the JSON `restore` to bring tokens back), and is not covered by that requirement.
- The schema in the backup's `object.json` is reconciled with `schema.conf` and `fields.conf`. If the live versions were deleted (operator wiped metadata), restore recreates them from the backup manifest.
- Caches are invalidated post-restore so subsequent reads see the restored state — no daemon restart required.

Listed backup directories:

```bash
ls $DB_ROOT/<dir>/<obj>/backup/
```

Errors you might see:

- `{"error":"backup not found ..."}` — bad `from` value.
- `{"error":"invalid from ..."}` — `from` contained `/`, `..`, or other unsafe characters.
- `{"error":"files/ is not empty (use force=true to overwrite)"}` — the backup governs `files/` and live `files/` is non-empty; pass `force:true` to overwrite.
- `{"error":"refusing to overwrite non-empty live object; pass force=true to proceed"}` — exactly that.

## 2. Filesystem snapshots

The strongest option for whole-database backup. Use ZFS, LVM, btrfs, or a SAN snapshot.

### ZFS example

```bash
# Hourly snapshot retained for 24 hours
zfs snapshot tank/shard-db@hourly-$(date +%Y%m%dT%H)
zfs list -t snapshot tank/shard-db

# Prune snapshots older than a day
zfs list -H -o name -t snapshot tank/shard-db \
  | while read snap; do
      ts=$(echo "$snap" | awk -F@ '{print $2}' | sed 's/hourly-//')
      if [[ "$ts" < "$(date -d '24 hours ago' +%Y%m%dT%H)" ]]; then
        zfs destroy "$snap"
      fi
    done
```

### Why it's the gold standard

- **Crash-consistent** — the snapshot captures whatever state the kernel had flushed at the moment, which is exactly what shard-db recovers from on crash anyway.
- **Instant** — COW, no copy cost.
- **Cross-object** — captures every tenant + object + log state atomically.

### Restore

Revert via `zfs rollback` (destroys later snapshots) or clone to a new filesystem for selective restore:

```bash
zfs clone tank/shard-db@hourly-20260418T14 tank/shard-db-restore
# mount tank/shard-db-restore and copy the bits you want
```

## 3. Offsite replication (cold)

For disaster recovery beyond a single host.

### Option A — `rsync` the whole root

Cheap, simple, good enough when a few minutes of data loss is acceptable:

```bash
rsync -aAX --delete \
  /opt/shard-db/db/ \
  backup-host:/var/backups/shard-db/
```

Run nightly from cron. `-aAX` preserves permissions, ACLs, xattrs. `--delete` prunes removed objects on the destination.

Caveat: `rsync` during active writes can race with kf resplits or vacuum (`.new` staging files mid-rename). Either pause writes, run on a filesystem snapshot, or accept occasional `.new` artifacts on the destination (they get swept on server startup).

### Option B — block-level replication

DRBD, GlusterFS replicated volume, or a SAN replication product. Higher complexity, near-zero RPO.

shard-db has **no native replication** — there's no leader/follower streaming protocol. If you need that, layer it at the block or filesystem level.

## Schedule

Suggested baseline:

| Layer | Cadence | Retention |
|---|---|---|
| Per-object `backup` | Ad-hoc, pre-migration | Keep until migration confirmed |
| Filesystem snapshot | Hourly | 24 h |
| Filesystem snapshot | Daily | 14 d |
| Offsite `rsync` | Daily | 30 d |
| Full cold archive | Weekly | 52 w |

Tune to your RPO and budget.

## Testing restores

A backup you haven't restored is not a backup.

- Once a month, spin up a scratch instance on a restore of yesterday's snapshot.
- Run a handful of reads to verify object integrity.
- Run `vacuum-check` — orphan counts should match what the live system reports.

## What shard-db already does on its own

- **Atomic writes (v2)** — value bytes land in the segment file first, then a single atomic 8-byte store on the keyfile slot publishes the record. A crash before the store orphans the segment bytes but never leaves a torn record visible to readers.
- **Atomic rebuilds** — `.new` + `rename()` for kf resplits, vacuum, and schema mutations.
- **Startup sweep** — stale `.new` artifacts (interrupted resplits, abandoned vacuum runs) are removed before accepting connections.

These protect against single-process crashes. They do **not** protect against disk failure, data center loss, or administrator error — hence the need for explicit backups above.
