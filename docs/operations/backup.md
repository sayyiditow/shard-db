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

Everything for that object: `data/`, `indexes/`, `metadata/`, `files/`, and `fields.conf`. Destination is a timestamped sibling directory under `$DB_ROOT/<dir>/`:

```
$DB_ROOT/<dir>/<obj>.backup-20260418T153012/
  fields.conf
  data/
  indexes/
  metadata/
  files/
```

### When to use

- Pre-schema-migration checkpoint (before `add-field` / `remove-field` on production data).
- Before a risky `vacuum --splits N` or manual shard edit.
- One-off snapshots of a single object for export.

### Consistency

Copy happens under a read lock, so concurrent reads see a consistent view. Writes during the copy **may or may not** be included — it's "recent" but not a fenced snapshot. For strict point-in-time, pause writes first or use filesystem snapshots.

### Restore

No automatic restore command. To restore:

1. Stop the server.
2. Remove or rename the current `<obj>/` directory.
3. `mv <obj>.backup-TIMESTAMP <obj>`.
4. Start the server.

The timestamped directory is a drop-in replacement — same layout.

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

Caveat: `rsync` during active writes can race with shard growth (`.new` files mid-rename). Either pause writes, run on a filesystem snapshot, or accept occasional `.new` artifacts on the destination (they get swept on server startup).

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

- **Atomic writes** — every slot flip is two-phase (write payload → flip flag).
- **Atomic rebuilds** — `.new` + `rename()` for growth, vacuum, schema mutations.
- **Startup sweep** — stale `.new`/`.old` artifacts are removed before accepting connections.

These protect against single-process crashes. They do **not** protect against disk failure, data center loss, or administrator error — hence the need for explicit backups above.
