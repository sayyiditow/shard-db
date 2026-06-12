# Plan: shard-db-embedded-sveltekit-starter

**Goal:** Create a new GitHub repo `sayyiditow/shard-db-embedded-sveltekit-starter` that mirrors
`sayyiditow/shard-db-svelte-starter` (same Items app, same cursor pagination demo) but uses the
`shard-db` npm package (embedded, in-process) instead of a TCP daemon.

**Execution rules:**
- Work in a fresh local directory: `/tmp/shard-db-embedded-sveltekit-starter`
- Do NOT branch from any existing repo — this is a brand new repo
- After all files are created, run `bun install` to verify dependencies resolve
- Run `bun run check` to verify TypeScript is clean (requires `.svelte-kit/` to exist, so run `bun run prepare` first)
- Do not start the dev server (no shard-db data dir available)
- Create the GitHub repo via `gh repo create` and push
- Do not claim success without showing the final `gh repo view` URL

---

## Task 1 — Create directory and git repo

```bash
mkdir -p /tmp/shard-db-embedded-sveltekit-starter
cd /tmp/shard-db-embedded-sveltekit-starter
git init
```

---

## Task 2 — `package.json`

Create `/tmp/shard-db-embedded-sveltekit-starter/package.json`:

```json
{
	"name": "shard-db-embedded-sveltekit-starter",
	"private": true,
	"version": "0.0.1",
	"type": "module",
	"scripts": {
		"dev": "bun --bun vite dev",
		"build": "bun --bun vite build",
		"preview": "bun --bun vite preview",
		"prepare": "svelte-kit sync || echo ''",
		"check": "svelte-kit sync && svelte-check --tsconfig ./tsconfig.json",
		"setup": "bun run scripts/setup-schema.ts"
	},
	"dependencies": {
		"shard-db": "^1.0.1"
	},
	"devDependencies": {
		"@sveltejs/adapter-node": "^5.5.4",
		"@sveltejs/kit": "^2.57.0",
		"@sveltejs/vite-plugin-svelte": "^7.0.0",
		"@types/bun": "^1.3.14",
		"@types/node": "^25.9.0",
		"svelte": "^5.55.2",
		"svelte-check": "^4.4.6",
		"typescript": "^6.0.2",
		"vite": "^8.0.7"
	}
}
```

---

## Task 3 — `svelte.config.js`

```js
import adapter from '@sveltejs/adapter-node';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	compilerOptions: {
		runes: ({ filename }) => (filename.split(/[/\\]/).includes('node_modules') ? undefined : true)
	},
	kit: {
		adapter: adapter()
	}
};

export default config;
```

---

## Task 4 — `vite.config.ts`

```ts
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [sveltekit()]
});
```

---

## Task 5 — `tsconfig.json`

```json
{
	"extends": "./.svelte-kit/tsconfig.json",
	"compilerOptions": {
		"rewriteRelativeImportExtensions": true,
		"allowJs": true,
		"checkJs": true,
		"esModuleInterop": true,
		"forceConsistentCasingInFileNames": true,
		"resolveJsonModule": true,
		"skipLibCheck": true,
		"sourceMap": true,
		"strict": true,
		"moduleResolution": "bundler",
		"types": ["bun"]
	}
}
```

---

## Task 6 — `.gitignore`

```
node_modules/
build/
.svelte-kit/
.env
*.local
data/
```

Note: `data/` is added so the default `SHARD_DB_ROOT=./data` dir isn't committed.

---

## Task 7 — `.env.example`

```
# Copy to .env and fill in your values.
# Absolute or relative path to the shard-db data directory.
# Will be created automatically on first run.
SHARD_DB_ROOT=./data
```

---

## Task 8 — `db.env.example`

```
# Optional shard-db tuning. Copy to db.env in the project root.
# DB_ROOT is ignored — the SHARD_DB_ROOT env var controls the data path.
# Settings take effect on next startup.
SLOW_QUERY_MS=500
GLOBAL_LIMIT=10000
LOG_LEVEL=3
```

---

## Task 9 — `src/app.html`

```html
<!doctype html>
<html lang="en">
	<head>
		<meta charset="utf-8" />
		<link rel="icon" href="%sveltekit.assets%/favicon.png" />
		<meta name="viewport" content="width=device-width, initial-scale=1" />
		%sveltekit.head%
	</head>
	<body data-sveltekit-preload-data="hover">
		<div style="display: contents">%sveltekit.body%</div>
	</body>
</html>
```

---

## Task 10 — `src/app.d.ts`

```ts
declare global {
	namespace App {
		// interface Error {}
		// interface Locals {}
		// interface PageData {}
		// interface Platform {}
	}
}

export {};
```

---

## Task 11 — `src/hooks.server.ts`

This is new vs the TCP starter. Registers SIGTERM/SIGINT handlers so systemd can stop the
process cleanly (without the C worker threads holding it open for 90s).

```ts
import type { Handle } from '@sveltejs/kit';
import { db } from '$lib/db/client';

const shutdown = () => { db.close(); process.exit(0); };
process.on('SIGTERM', shutdown);
process.on('SIGINT',  shutdown);

export const handle: Handle = async ({ event, resolve }) => resolve(event);
```

---

## Task 12 — `src/lib/db/client.ts`

This replaces the TCP client entirely. Same exported shape (`db`, `isError`) so routes are
unchanged.

```ts
import ShardDb from 'shard-db';

export interface ShardDbError {
	error: string;
}

function getDbRoot(): string {
	const root = process.env.SHARD_DB_ROOT;
	if (!root) throw new Error('SHARD_DB_ROOT environment variable is not set');
	return root;
}

const native = new ShardDb(getDbRoot());

// Forward engine logs to the console.
// The binding passes numeric types (1–6) despite LogHandler typing them as strings.
// Cast through unknown to satisfy strict mode without changing the runtime behaviour.
// type: 1=error  2=warn  3=info  4=debug  5=audit  6=slow
native.setLogHandler(((type: number, msg: string) => {
	const text = msg.trimEnd();
	if (type === 1) console.error(text);
	else if (type === 2 || type === 6) console.warn(text);
	else console.log(text);
}) as unknown as ShardDb.LogHandler);

export const db = {
	query<T = unknown>(body: ShardDb.QueryBody): Promise<T | ShardDbError> {
		return native.query(body).then(raw => JSON.parse(raw) as T | ShardDbError);
	},
	close() { native.close(); }
};

export function isError(resp: unknown): resp is ShardDbError {
	return typeof resp === 'object' && resp !== null && 'error' in resp;
}
```

---

## Task 13 — `src/lib/schema.ts`

Unchanged from the TCP starter. Copy verbatim:

```ts
/** Matches the `items` object created by scripts/setup-schema.ts. */
export interface Item {
	key: string;
	title: string;
	body: string;
	status: 'draft' | 'published' | 'archived';
	score: number;
	created_at: string;
}

/** Shape of a find-with-cursor response row. */
export interface Row {
	key: string;
	value: Omit<Item, 'key'>;
}

/** Shape of a find-with-cursor + want_total response envelope. */
export interface FindEnvelope {
	rows: Row[];
	cursor: Record<string, unknown> | null;
	total: number;
}

export function rowToItem(r: Row): Item {
	return { key: r.key, ...r.value };
}
```

---

## Task 14 — `src/routes/+layout.svelte`

Unchanged from the TCP starter. Copy verbatim:

```svelte
<script lang="ts">
	let { children } = $props();
</script>

<nav>
	<a href="/">Items</a>
</nav>

{@render children()}

<style>
	nav {
		padding: 1rem;
		border-bottom: 1px solid #e5e7eb;
		margin-bottom: 1.5rem;
	}
	nav a {
		font-weight: 600;
		text-decoration: none;
		color: #111;
	}
	:global(body) {
		font-family: system-ui, sans-serif;
		max-width: 720px;
		margin: 0 auto;
		padding: 0 1rem;
		color: #111;
	}
</style>
```

---

## Task 15 — `src/routes/+page.server.ts`

Unchanged from the TCP starter. Copy verbatim (imports `db` and `isError` from `$lib/db/client`,
which now resolves to the embedded client — no other change needed):

```ts
import type { PageServerLoad } from './$types';
import { db, isError } from '$lib/db/client';
import type { FindEnvelope, Item } from '$lib/schema';
import { rowToItem } from '$lib/schema';

const PAGE_SIZE = 25;
const VALID_STATUSES = new Set(['draft', 'published', 'archived']);

export const load: PageServerLoad = async ({ url }) => {
	const statusParam = url.searchParams.get('status') ?? '';
	const after       = url.searchParams.get('after')  ?? '';

	const status = VALID_STATUSES.has(statusParam) ? statusParam : null;

	const criteria: object[] = [];
	if (status) {
		criteria.push({ field: 'status', op: 'eq', value: status });
	}

	let cursor: object | null = null;
	if (after) {
		try { cursor = JSON.parse(decodeURIComponent(after)); } catch { /* start from page 1 */ }
	}

	const resp = await db.query<FindEnvelope>({
		mode:     'find',
		dir:      'myapp',
		object:   'items',
		criteria,
		order_by: 'score',
		order:    'desc',
		limit:    PAGE_SIZE,
		cursor:   cursor ?? null,
		total:    true
	});

	if (isError(resp)) {
		return { items: [] as Item[], total: 0, nextCursor: null, status, error: resp.error };
	}

	const items      = resp.rows.map(rowToItem);
	const nextCursor = resp.cursor ? encodeURIComponent(JSON.stringify(resp.cursor)) : null;

	return { items, total: resp.total, nextCursor, status };
};
```

---

## Task 16 — `src/routes/+page.svelte`

Unchanged from the TCP starter. Copy verbatim:

```svelte
<script lang="ts">
	import type { PageData } from './$types';
	let { data }: { data: PageData } = $props();

	function cursorUrl(cursor: string | null, status: string | null): string {
		const p = new URLSearchParams();
		if (status) p.set('status', status);
		if (cursor) p.set('after', cursor);
		const qs = p.toString();
		return qs ? `/?${qs}` : '/';
	}
</script>

<svelte:head><title>Items</title></svelte:head>

<div class="toolbar">
	<span class="total">{data.total} items</span>
	<div class="filters">
		<a href="/" class:active={!data.status}>All</a>
		<a href="/?status=published" class:active={data.status === 'published'}>Published</a>
		<a href="/?status=draft"     class:active={data.status === 'draft'}>Draft</a>
		<a href="/?status=archived"  class:active={data.status === 'archived'}>Archived</a>
	</div>
</div>

{#if data.error}
	<p class="error">{data.error}</p>
{:else if data.items.length === 0}
	<p class="empty">No items found.</p>
{:else}
	<ul class="list">
		{#each data.items as item (item.key)}
			<li>
				<a href="/items/{item.key}" class="title">{item.title}</a>
				<div class="meta">
					<span class="badge {item.status}">{item.status}</span>
					<span>score {item.score}</span>
					<span>{item.created_at}</span>
				</div>
			</li>
		{/each}
	</ul>

	{#if data.nextCursor}
		<a class="next" href={cursorUrl(data.nextCursor, data.status)}>Next →</a>
	{/if}
{/if}

<style>
	.toolbar { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; flex-wrap: wrap; }
	.total   { color: #6b7280; font-size: 0.875rem; }
	.filters { display: flex; gap: 0.5rem; }
	.filters a { padding: 0.25rem 0.75rem; border-radius: 9999px; border: 1px solid #d1d5db; font-size: 0.875rem; text-decoration: none; color: #374151; }
	.filters a.active { background: #111; color: #fff; border-color: #111; }

	.list { list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: 0.75rem; }
	.list li { padding: 0.75rem 1rem; border: 1px solid #e5e7eb; border-radius: 8px; }
	.list li:hover { border-color: #9ca3af; }

	.title { font-weight: 600; text-decoration: none; color: #111; display: block; margin-bottom: 0.35rem; }
	.meta  { display: flex; gap: 0.75rem; font-size: 0.8rem; color: #6b7280; align-items: center; }

	.badge           { padding: 0.1rem 0.5rem; border-radius: 9999px; font-size: 0.75rem; font-weight: 500; }
	.badge.published { background: #dcfce7; color: #166534; }
	.badge.draft     { background: #fef9c3; color: #854d0e; }
	.badge.archived  { background: #f3f4f6; color: #374151; }

	.next  { display: inline-block; margin-top: 1.25rem; padding: 0.5rem 1.25rem; border: 1px solid #d1d5db; border-radius: 6px; text-decoration: none; color: #374151; }
	.next:hover { border-color: #9ca3af; }

	.error { color: #dc2626; }
	.empty { color: #6b7280; }
</style>
```

---

## Task 17 — `src/routes/items/[key]/+page.server.ts`

Unchanged from the TCP starter. Copy verbatim:

```ts
import type { PageServerLoad } from './$types';
import { db, isError } from '$lib/db/client';
import { error } from '@sveltejs/kit';
import type { Item } from '$lib/schema';

export const load: PageServerLoad = async ({ params }) => {
	const resp = await db.query<Omit<Item, 'key'>>({
		mode:   'get',
		dir:    'myapp',
		object: 'items',
		key:    params.key
	});

	if (isError(resp)) {
		error(404, resp.error);
	}

	return { item: { key: params.key, ...resp } as Item };
};
```

---

## Task 18 — `src/routes/items/[key]/+page.svelte`

Unchanged from the TCP starter. Copy verbatim:

```svelte
<script lang="ts">
	import type { PageData } from './$types';
	let { data }: { data: PageData } = $props();
	const { item } = data;
</script>

<svelte:head><title>{item.title}</title></svelte:head>

<a class="back" href="/">← Back</a>

<article>
	<div class="header">
		<h1>{item.title}</h1>
		<span class="badge {item.status}">{item.status}</span>
	</div>
	<div class="meta">
		<span>Score: {item.score}</span>
		<span>Created: {item.created_at}</span>
		<span>Key: {item.key}</span>
	</div>
	<p class="body">{item.body}</p>
</article>

<style>
	.back { display: inline-block; margin-bottom: 1.5rem; color: #6b7280; text-decoration: none; font-size: 0.875rem; }
	.back:hover { color: #111; }

	article { }
	.header { display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.5rem; }
	h1 { margin: 0; font-size: 1.5rem; }

	.badge           { padding: 0.15rem 0.6rem; border-radius: 9999px; font-size: 0.8rem; font-weight: 500; white-space: nowrap; }
	.badge.published { background: #dcfce7; color: #166534; }
	.badge.draft     { background: #fef9c3; color: #854d0e; }
	.badge.archived  { background: #f3f4f6; color: #374151; }

	.meta { display: flex; gap: 1rem; font-size: 0.8rem; color: #6b7280; margin-bottom: 1.25rem; flex-wrap: wrap; }
	.body { line-height: 1.6; white-space: pre-wrap; }
</style>
```

---

## Task 19 — `scripts/setup-schema.ts`

Simplified vs TCP starter: no host/port/token options. Creates a ShardDb directly.
Note: this script is run via `bun run setup`, so bun automatically loads `.env` —
`SHARD_DB_ROOT` is available.

```ts
/**
 * Creates the `myapp/items` object in shard-db and seeds a few example records.
 *
 * Run once before starting the app:
 *   bun run setup          (via package.json script)
 *   bun run scripts/setup-schema.ts   (directly)
 *
 * Reads SHARD_DB_ROOT from .env (bun loads it automatically).
 */
import ShardDb from 'shard-db';

const dbRoot = process.env.SHARD_DB_ROOT ?? '';
if (!dbRoot) {
	console.error('SHARD_DB_ROOT is not set. Copy .env.example to .env and set the path.');
	process.exit(1);
}

const db = new ShardDb(dbRoot);

function isError(r: unknown): r is { error: string } {
	return typeof r === 'object' && r !== null && 'error' in r;
}

async function run() {
	const create = await db.query({
		mode:    'create-object',
		dir:     'myapp',
		object:  'items',
		splits:  8,
		max_key: 8,
		auto_key: 'seq(items_seq)',
		fields: [
			'title:varchar:200',
			'body:varchar:2000',
			'status:varchar:20:default=draft',
			'score:int:default=0',
			'created_at:datetime:default=auto_create'
		],
		indexes: ['status', 'score', 'created_at']
	});

	const parsed = JSON.parse(create);
	if (isError(parsed) && !parsed.error.includes('already exists')) {
		console.error('create-object failed:', parsed.error);
		db.close();
		process.exit(1);
	}

	console.log('Schema ready.');

	const seeds = [
		{ title: 'Getting started with shard-db embedded', body: 'shard-db runs entirely in-process — no daemon, no TCP. Just npm install and go.', status: 'published', score: 42 },
		{ title: 'Cursor pagination with want_total',       body: 'Pass "cursor": null and "total": true to get both the page results and full match count in one round-trip.', status: 'published', score: 38 },
		{ title: 'Draft item example',                      body: 'This item has status=draft. Filter by status using the pills on the list page.', status: 'draft', score: 5 },
		{ title: 'Archived item example',                   body: 'Archived items are still queryable — just hidden from the default view.', status: 'archived', score: 1 }
	];

	for (const seed of seeds) {
		// key is omitted intentionally — auto_key=seq generates it server-side.
		// Cast through unknown because QueryBody.insert requires key: string.
		const body = { mode: 'insert', dir: 'myapp', object: 'items', value: seed } as unknown as Parameters<typeof db.query>[0];
		const r = JSON.parse(await db.query(body));
		if (isError(r)) { console.error('insert failed:', r.error); } else { console.log('inserted:', seed.title); }
	}

	db.close();
}

run().catch((err) => { console.error(err); db.close(); process.exit(1); });
```

---

## Task 20 — `README.md`

```markdown
# shard-db-embedded-sveltekit-starter

SvelteKit + [shard-db](https://github.com/sayyiditow/shard-db) starter template — **embedded mode**.

Runs shard-db entirely in-process via the [`shard-db` npm package](https://www.npmjs.com/package/shard-db).
No daemon, no TCP socket, no external dependencies. The database opens when the app starts and closes on shutdown.

For the TCP variant (separate daemon process), see [shard-db-svelte-starter](https://github.com/sayyiditow/shard-db-svelte-starter).

Demonstrates:
- Embedded shard-db via `shard-db` npm package
- Cursor pagination with `total` (page + total count in one request)
- Log handler forwarding (errors, warnings, slow queries → console)
- Graceful shutdown on SIGTERM (clean db close, no zombie threads)
- Typed schema and row helpers

See [shard-db-hn-explorer](https://github.com/sayyiditow/shard-db-hn-explorer) for a full production example.

## Quick start

**Prerequisites:** [Bun](https://bun.sh) installed. Linux x64/arm64 or macOS Apple Silicon.

```bash
# 1. Clone and install
git clone https://github.com/sayyiditow/shard-db-embedded-sveltekit-starter
cd shard-db-embedded-sveltekit-starter
bun install

# 2. Configure
cp .env.example .env
# SHARD_DB_ROOT=./data is fine for local dev — no further edits needed

# 3. Create schema + seed data
bun run setup

# 4. Dev server
bun run dev
```

Open http://localhost:5173.

## Project structure

```
src/
  hooks.server.ts          — SIGTERM/SIGINT handler for clean shutdown
  lib/
    db/client.ts           — embedded ShardDb singleton + query wrapper
    schema.ts              — Item interface, FindEnvelope, rowToItem helper
  routes/
    +page.server.ts        — list: cursor find + total
    +page.svelte           — list UI with status filter + pagination
    items/[key]/
      +page.server.ts      — single item get
      +page.svelte         — item detail
scripts/
  setup-schema.ts          — create-object + seed data (run once)
```

## Replacing the starter schema

Edit `scripts/setup-schema.ts` to change the `fields` and `indexes` arrays, then re-run `bun run setup`.
Update `src/lib/schema.ts` to match your types.

## Environment variables

| Variable        | Default  | Purpose                                  |
|----------------|----------|------------------------------------------|
| `SHARD_DB_ROOT` | required | Path to the data directory (auto-created)|

## Tuning (optional)

Copy `db.env.example` to `db.env` in the project root. Settings are read once at startup by the
shard-db engine. `DB_ROOT` is always ignored — `SHARD_DB_ROOT` controls the path.

## Platform support

Prebuilt binaries are included for Linux x64, Linux arm64, and macOS Apple Silicon.
Windows is not supported.

## Deployment

Build with Bun, deploy the `build/` directory:

```bash
bun run build
# copy build/ package.json bun.lockb to server
# on server:
bun install --production
bun pm trust shard-db   # allow node-gyp-build install script
NODE_ENV=production SHARD_DB_ROOT=/path/to/data bun ./build/index.js
```

For systemd deployment, add a `TimeoutStopSec=15` to your service unit — the SIGTERM handler
closes the database and exits within milliseconds, so 15s is a generous safety net.
```

---

## Task 21 — Install, check, commit, push

```bash
cd /tmp/shard-db-embedded-sveltekit-starter

# Install dependencies
bun install

# Generate .svelte-kit/ so check works
bun run prepare

# TypeScript check (expect 0 errors)
bun run check

# Commit
git add -A
git commit -m "$(cat <<'EOF'
feat: initial embedded sveltekit starter

SvelteKit starter using shard-db npm package (embedded mode).
Same Items demo as shard-db-svelte-starter but no daemon required.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"

# Create GitHub repo and push
gh repo create sayyiditow/shard-db-embedded-sveltekit-starter \
  --public \
  --description "SvelteKit starter template for shard-db (embedded mode — no daemon)" \
  --source . \
  --remote origin \
  --push

gh repo view sayyiditow/shard-db-embedded-sveltekit-starter
```

---

## Invariants and edge cases

- `client.ts` calls `getDbRoot()` at module load time. If `SHARD_DB_ROOT` is not set, it throws
  immediately with a clear message — do not silently fall back to a default path.
- `hooks.server.ts` imports `db` from `$lib/db/client`. This triggers module load and therefore
  `new ShardDb(getDbRoot())`. This is intentional — db opens on startup, not on first request.
  This means `SHARD_DB_ROOT` must be set before the server starts.
- `setup-schema.ts` does NOT import from `$lib/db/client` (which would create a second db
  instance). It creates its own `ShardDb` instance and closes it explicitly at the end.
- The `db.query()` wrapper in `client.ts` always `JSON.parse`s the result string. shard-db
  always returns valid JSON on success or `{"error":"..."}` on failure — never an empty string
  in normal operation.
- `bun run setup` automatically loads `.env` — no explicit dotenv import needed.
- `bun pm trust shard-db` is required on the server for the `node-gyp-build` postinstall script
  to run (Bun's postinstall security sandbox blocks it otherwise). Document this in README.
