# Changelog

The maintained changelog lives at [`docs/reference/changelog.md`](docs/reference/changelog.md) — a per-release summary with links to the detail.

For the rich per-release notes (motivation, migration steps, code references), see [`docs/release-notes/`](docs/release-notes/).

Versions follow `yyyy.mm.N` — year-month, with `N` as the counter within that month.

## Recent releases

- **[2026.07.1](docs/release-notes/2026.07.1.md)** — **Compact VARCHAR storage** (run `./migrate`): VARIABLE-format segments trim trailing zero bytes on every write; `./shard-db compact <dir> <obj>` (or JSON `{"mode":"compact"}`) rewrites existing segments to reclaim the space. **Natural Query Language (NQL)**: human-readable `find / count / aggregate` filter syntax accepted on the same TCP port as JSON; server auto-detects by first character. **`rebuild-kf`** kf-corruption recovery command; `add-index`/`remove-index` now take the exclusive object lock, fixing a concurrent-write race that could corrupt index files during a rebuild. **Run `./migrate`** after upgrading.
- **[2026.05.8](docs/release-notes/2026.05.8.md)** — 4800× speedup on selective filter + order_by, logging framework reshape, unknown-field validation, several planner correctness fixes
- **[2026.05.7.1](docs/release-notes/2026.05.7.1.md)** — `find` correctness fix on busy daemons, FT_TIMESTAMP criteria fix, edit-field polish, add-field computed defaults
- **[2026.05.7](docs/release-notes/2026.05.7.md)** — filter-first planner, trigram + bitmap + enum, regex on indexed varchar
- **[2026.05.6](docs/release-notes/2026.05.6.md)** — FT_TIMESTAMP type
- **[2026.05.5](docs/release-notes/2026.05.5.md)** — v1 retirement, btree (value, hash) sort, slotcask CRUD perf

Older releases (2026.05.4 → 2026.04.1): see the full version files in [`docs/release-notes/`](docs/release-notes/). The historical Keep-a-Changelog content that lived here previously is preserved in git history (`git log -- CHANGELOG.md`).
