# Hardening: secure random source + error propagation for generated keys/defaults

## Nature of this plan

Security + perf fix in one change. Every random byte in the daemon today comes
from a per-call `fopen("/dev/urandom")`, and failures are swallowed in the
worst places:

- `gen_uuid4_raw` (config.c) returns `void` and **zero-fills on failure**,
  skipping even the v4/variant stamping. Under fd exhaustion (realistic at
  high connection counts — every auto-key insert opens a fresh fd), **every
  insert on an `auto_key=uuid` object gets the same all-zero key** and
  silently upserts over the previous record. Silent data loss.
- Both bulk-insert call sites of `gen_uuid4_batch` **ignore its `-1` return**;
  on failure the keys come from **uninitialized heap memory** (whatever
  `malloc` returned — a heap-content leak into stored, readable keys).
- The bulk seq path ignores `seq_next_val_batch`'s `-1` the same way: keys
  become `-1, 0, 1, ...`, colliding with existing records.
- `generate_default`'s caller in `typed_encode_defaults` treats NULL as
  "leave field zero", so a failed `seq()` / `uuid()` / `random(N)` default is
  silently stored as zeros.
- The v2 rebuild backfill (`v2_rebuild_walk_cb` in query.c) writes zero UUIDs
  / empty random fields on the same failure.

The fix: one `fill_random()` helper backed by `getentropy(2)` (no fd, immune
to fd exhaustion, works in chroot; one syscall instead of
fopen+fread+fclose — this is also the perf win on the single-insert auto-key
path), with a `/dev/urandom` read-loop fallback, and **error propagation at
every generation site** so a failed random source refuses the write instead
of corrupting it.

Also includes the CLI request-builder buffer fix found in the same review
(three `malloc`+`sprintf` sites whose size budget omits components).

## Execution rules (read first)

- Branch off `main`: `git checkout -b hardening/secure-random`.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run-all`
  after every phase. Not green → STOP and write `PLAN_NOTES.md`.
- Every insertion/edit locates its site by **quoted anchor text**. If an
  anchor is not found exactly, STOP and write `PLAN_NOTES.md` — do not guess.
  If plan 05 (split-query-c) has already been executed, query.c anchors may
  live in a `src/db/query_*.c` file instead — locate them with
  `grep -rn '<anchor>' src/db/` and apply the edit wherever the anchor lives.
- Leave all work uncommitted. Never claim a step passed without real output.

## Phase 0 — Baseline

1. `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all` — record the
   `# total:` line. Behavior-preserving phases must match it; phases that add
   error paths must not *reduce* the pass count (new tests may raise it).

## Phase 1 — `fill_random` helper

**1a.** In `src/db/util.c`, add near the top of the file (after the existing
`#include` block; also add `#include <sys/random.h>`, `#include <fcntl.h>`,
`#include <errno.h>`, `#include <unistd.h>` to that block if not already
present):

```c
/* Fill buf with n cryptographically-secure random bytes. Primary source is
   getentropy(2) — no fd, so it cannot fail from fd exhaustion and works in
   chroot/sandbox; chunked at 256 bytes (the getentropy per-call cap).
   Fallback is a /dev/urandom read loop for libcs without getentropy.
   Returns 0 on success, -1 if no random source is available. Callers MUST
   check the return: on -1 the buffer contents are unspecified and must not
   be used. */
int fill_random(void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    size_t off = 0;
    while (off < n) {
        size_t chunk = (n - off > 256) ? 256 : (n - off);
        if (getentropy(p + off, chunk) != 0) break;
        off += chunk;
    }
    if (off == n) return 0;

    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    off = 0;
    while (off < n) {
        ssize_t r = read(fd, p + off, n - off);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) { close(fd); return -1; }
        off += (size_t)r;
    }
    close(fd);
    return 0;
}
```

**1b.** In `src/db/types.h`, anchor on the existing declaration
`void gen_uuid4_raw(uint8_t out[16]);` and add directly above it:

```c
int fill_random(void *buf, size_t n);
```

Build + `run-all`: green (nothing consumes it yet).

## Phase 2 — UUID generators return failure

**2a.** In `src/db/config.c`, replace the whole function whose anchor is
`void gen_uuid4_raw(uint8_t out[16]) {` with:

```c
int gen_uuid4_raw(uint8_t out[16]) {
    if (fill_random(out, 16) != 0) {
        memset(out, 0, 16);   /* defense in depth — callers must check rc */
        return -1;
    }
    out[6] = (out[6] & 0x0F) | 0x40;  /* version 4 */
    out[8] = (out[8] & 0x3F) | 0x80;  /* variant 1 */
    return 0;
}
```

**2b.** In `src/db/types.h`, change the declaration
`void gen_uuid4_raw(uint8_t out[16]);` to
`int gen_uuid4_raw(uint8_t out[16]);`.

**2c.** In query.c (or its split successor), anchor on
`extern void gen_uuid4_raw(uint8_t out[16]);` and change to
`extern int gen_uuid4_raw(uint8_t out[16]);`.

**2d.** In `src/db/config.c`, in `gen_uuid4_batch`, replace the body between
`if (n == 0) return 0;` and the stamping `for` loop — i.e. the lines

```c
    FILE *f = fopen("/dev/urandom", "r");
    if (!f) return -1;
    size_t got = fread(out, 1, n * 16, f);
    fclose(f);
    if (got != n * 16) return -1;
```

with:

```c
    if (n > SIZE_MAX / 16) return -1;
    if (fill_random(out, n * 16) != 0) return -1;
```

**2e.** In `src/db/config.c`, in the wrapper anchored by
`static void gen_uuid4(char *buf, size_t bufsz) {`, replace its body:

```c
static void gen_uuid4(char *buf, size_t bufsz) {
    if (bufsz < 37) { buf[0] = '\0'; return; }
    uint8_t raw[16];
    if (gen_uuid4_raw(raw) != 0) { buf[0] = '\0'; return; }
    format_uuid_string(raw, buf);
}
```

(Empty string is this file's established generator-failure signal — see
`gen_random_hex` and the `return gen_buf[0] ? gen_buf : NULL;` consumers.)

**2f.** In `src/db/config.c`, in `gen_random_hex`, replace the lines

```c
    FILE *f = fopen("/dev/urandom", "r");
    if (!f || (int)fread(raw, 1, nbytes, f) != nbytes) { buf[0] = '\0'; if (f) fclose(f); return; }
    fclose(f);
```

with:

```c
    if (fill_random(raw, (size_t)nbytes) != 0) { buf[0] = '\0'; return; }
```

Build + `run-all`: must match Phase 0 (failure paths are unreachable when the
random source works).

## Phase 3 — defaults path: generator failure refuses the write

In `src/db/config.c`, inside `typed_encode_defaults`, anchor on:

```c
        const char *dv = generate_default(&ts->fields[i], gen_buf, sizeof(gen_buf),
                                          db_root, object);
        if (dv) encode_field(&ts->fields[i], dv, out + ts->fields[i].offset);
```

and replace those lines with:

```c
        const char *dv = generate_default(&ts->fields[i], gen_buf, sizeof(gen_buf),
                                          db_root, object);
        if (!dv) {
            /* The loop already skipped DK_NONE, so NULL here always means the
               generator failed (seq lock/filesystem error, random source
               unavailable). Silently storing zeros was the old behavior —
               refuse the write instead, using the same -2 + err_buf contract
               as enum validation. */
            if (err_buf && err_buf_size > 0)
                snprintf(err_buf, err_buf_size,
                         "default generator failed for field [%s]",
                         ts->fields[i].name);
            return -2;
        }
        encode_field(&ts->fields[i], dv, out + ts->fields[i].offset);
```

**Behavior change (intended, record in the branch message):** a failing
`seq()` default previously stored zeros silently; it now rejects the insert
with an error. If any existing test asserts the silent-zero behavior, STOP
and write `PLAN_NOTES.md`.

Build + `run-all`: must match Phase 0.

## Phase 4 — single-insert auto-key path (server.c)

In `src/db/server.c`, inside `auto_key_generate`, anchor on:

```c
        char *buf = malloc(16);
        if (!buf) { OUT("{\"error\":\"oom\"}\n"); return -1; }
        gen_uuid4_raw((uint8_t *)buf);
```

and replace the `gen_uuid4_raw` line with:

```c
        if (gen_uuid4_raw((uint8_t *)buf) != 0) {
            free(buf);
            OUT("{\"error\":\"random source unavailable (uuid key generation failed)\"}\n");
            return -1;
        }
```

(The caller already handles `-1`; the seq branch of the same function is the
error-shape template.)

Build + `run-all`: must match Phase 0.

## Phase 5 — bulk-insert paths: refuse the batch on generation failure

There are **two** sibling sites (JSON bulk-insert and delimited bulk-insert).
Each currently reads:

```c
        if (auto_key_mode == AK_UUID) {
            uuid_pool = malloc((size_t)n_omits * 16);
            if (uuid_pool) gen_uuid4_batch(uuid_pool, n_omits);
        } else {
            seq_start = seq_next_val_batch(db_root, object, sc.auto_key_seq_name, n_omits);
        }
```

Locate both with `grep -rn 'gen_uuid4_batch(uuid_pool, n_omits)' src/db/`.
For **each** site, apply the same transformation:

**5a.** Replace the block above with:

```c
        int keygen_failed = 0;
        if (auto_key_mode == AK_UUID) {
            uuid_pool = malloc((size_t)n_omits * 16);
            if (!uuid_pool || gen_uuid4_batch(uuid_pool, n_omits) != 0)
                keygen_failed = 1;
        } else {
            seq_start = seq_next_val_batch(db_root, object, sc.auto_key_seq_name, n_omits);
            if (seq_start < 0) keygen_failed = 1;
        }
        if (keygen_failed) {
            free(uuid_pool);
            /* CLEANUP: duplicate — statement for statement — the exact free
               sequence of the "validation failed at record %d: malformed key
               for auto_key mode" error block found earlier in THIS SAME
               function (it frees some of: wire_keys[i] loop, wire_keys,
               records, arena, idx_pairs loop, idx_pairs, idx_pair_counts,
               idx_pair_caps — copy precisely what that block frees, in the
               same order). Then: */
            OUT("{\"error\":\"bulk-insert key generation failed: %s\"}\n",
                auto_key_mode == AK_UUID ? "random source unavailable"
                                         : "sequence unavailable");
            return 1;
        }
```

The two functions' validation-failure blocks differ slightly (one loops
`nfields`, the other `nidx`; one frees `wire_keys`/`records`, check each) —
that is why the cleanup must be **copied from the adjacent block in the same
function**, not from this plan. Do not invent frees and do not skip any.

**5b.** In the omit-fill loop just below each site, anchor on:

```c
                if (uuid_pool) memcpy(r->id, uuid_pool + omit_idx * 16, 16);
                else gen_uuid4_raw((uint8_t *)r->id);
```

(the second line may carry a `/* fallback per-record */` comment at one site)
and replace both lines with:

```c
                memcpy(r->id, uuid_pool + omit_idx * 16, 16);
```

(`uuid_pool` is now guaranteed non-NULL past the keygen_failed gate.)

Build + `run-all`: must match Phase 0.

## Phase 6 — rebuild/backfill path (v2_rebuild_walk_cb)

In query.c (or its split successor), inside `v2_rebuild_walk_cb`:

**6a.** Anchor on:

```c
            case DK_UUID: {
                uint8_t raw[16];
                gen_uuid4_raw(raw);
```

and change the `gen_uuid4_raw(raw);` line to:

```c
                if (gen_uuid4_raw(raw) != 0) { free(buf); ctx->error = 1; return 1; }
```

(This matches the function's existing calloc-failure convention:
`if (!buf) { ctx->error = 1; return 1; }`.)

**6b.** Anchor on:

```c
                uint8_t raw[256];
                FILE *rf = fopen("/dev/urandom", "r");
                if (!rf) break;
                if ((int)fread(raw, 1, (size_t)nbytes, rf) != nbytes) {
                    fclose(rf);
                    break;
                }
                fclose(rf);
```

and replace the `FILE *rf ... fclose(rf);` lines (keep `uint8_t raw[256];`)
with:

```c
                if (fill_random(raw, (size_t)nbytes) != 0) {
                    free(buf); ctx->error = 1; return 1;
                }
```

**Behavior change (intended):** a random-source failure now aborts the
rebuild (which is transactional — `.new` files only activate on success)
instead of silently backfilling zero/empty values.

Build + `run-all`: must match Phase 0.

## Phase 7 — CLI request-builder size budgets (separate bug, same review)

Three `malloc`+`sprintf` sites in `src/cli/main.c` under-budget the buffer.
Minimal fix: correct the size expression only — no restructuring.

**7a.** Anchor: `char *req = malloc(strlen(crit) + strlen(specs) + strlen(having_json) + 1024);`
Replace with:

```c
                    char *req = malloc(strlen(crit) + strlen(specs) + strlen(having_json)
                                       + strlen(grp_json) + strlen(ob_clause)
                                       + strlen(oi.dir) + strlen(oi.object) + 1024);
```

**7b.** Anchor: `char *req = malloc(strlen(crit) + 256);`
Replace with:

```c
        char *req = malloc(strlen(crit) + strlen(oi.dir) + strlen(oi.object) + 256);
```

**7c.** Anchor: `char *req = malloc(strlen(crit) + 1024);`
Replace with:

```c
            char *req = malloc(strlen(crit) + strlen(fields_json) + strlen(ob_clause)
                               + strlen(oi.dir) + strlen(oi.object) + 1024);
```

Build (shard-cli builds as part of `./build.sh`) + `run-all`: must match
Phase 0.

## Phase 8 — test coverage

Add assertions to whichever existing auto-key test case covers bulk-insert
with `auto_key=uuid` (find it via `./build/bin/shard-db-test list` and
`grep -rl auto_key src/test/cases/`): after a bulk-insert of ≥100 omitted-key
records, assert every returned key is 36 chars, dashed at 8/13/18/23, has
`'4'` at index 14 (version) and one of `8/9/a/b` at index 19 (variant), and
that all keys are pairwise unique. If no existing case fits, add
`src/test/cases/test_secure_random_keys.c` modeled on the nearest auto-key
case (same TEST_REGISTER + fixture pattern) and register it in `build.sh`'s
test-source list.

The failure paths (random source unavailable) are not testable without fault
injection — do not attempt to simulate fd exhaustion in the suite; the code
review of the diff is the verification for those branches.

Final gate: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all` —
`0 failed`, total ≥ Phase 0.

## Definition of done

- No `fopen("/dev/urandom")` remains in `src/db/`
  (`grep -rn 'urandom' src/db/*.c` shows only comments and `fill_random`'s
  fallback `open`).
- Every `gen_uuid4_raw` / `gen_uuid4_batch` / `seq_next_val_batch` call site
  checks the return value (`grep -n` each symbol and inspect).
- `run-all` green; uuid-key assertions from Phase 8 in place.
- Branch message notes the two intended behavior changes (Phase 3, Phase 6).
- Leave uncommitted.
