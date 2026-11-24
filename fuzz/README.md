# Wire-format fuzzers

libFuzzer harnesses for shard-db's network-facing parsers. Every
inbound TCP byte ends up in one of these before any auth check fires,
so memory-safety bugs here are reachable from anywhere on the network.

## Targets

| Harness | What it fuzzes | Source |
|---|---|---|
| `fuzz_json` | `json_parse_object()` plus every `json_obj_*` getter | `src/db/util.c` |
| `fuzz_b64` | `b64_decode()` | `src/db/util.c` |
| `fuzz_criteria` | `parse_criteria_tree()` (the AND/OR/op nesting walker) | `src/db/query_plan.c` (linked via query.c's TU set) |
| `fuzz_filename` | the filesystem-name validators — `valid_filename()`, `is_valid_object()` (util.c), `dir_name_ok()` (config.c) | `src/db/util.c`, `src/db/config.c` |
| `fuzz_cursor` | `parse_cursor_object()` — static in query.c, reached by `#include "query.c"` in the harness TU (campaign step-3 decision) | `src/db/query.c` |
| `fuzz_frame` | the request-framing worker loop: fgets chunking vs `g_max_request_size`, the oversized check + drain, `{"error":"Request too large (max 63 bytes)"}` + `\0\n` separator, empty-line skip, case-insensitive QUIT. Drives the REAL `worker_thread()` over a real loopback TCP pair (the worker `getpeername()`s the peer as AF_INET — a socketpair reads as 0.0.0.0, untrusted, and every request would die at auth) against a throwaway empty DB_ROOT (embedded open), with the real dispatcher — auth included — executing behind it | `src/db/server.c` (harness includes it for the statics), `src/db/nql.c`, `src/db/embedded.c` |

## Framing + re-parse symbol identification (campaign Phase 1 step 3, first sub-step)

Request-framing logic (fuzzed by `fuzz_frame`):
- The framing loop is INLINE in the static `worker_thread()` (`server.c`,
  "Worker thread — pulls connections from work queue"): `fgets` into a
  `g_max_request_size` buffer (`g_max_request_size > 0 ? g_max_request_size
  : MAX_LINE`), the oversized check `strlen(line) == buf_size-1 &&
  line[strlen-1] != '\n'`, the `fgetc` drain, the
  `{"error":"Request too large (max N bytes)"}` emission with its `\0\n`
  separator, empty-line skip, and the case-insensitive `QUIT` comparison.
- Reached in the harness by including server.c (statics visible), one
  persistent `worker_thread` on a loopback TCP pair (the client end of a
  real 127.0.0.1 connection — the peer address auth needs), `g_db`/`g_shard_db_instance`
  bound per thread to the embedded-opened throwaway root, and the
  `g_max_request_size` knob re-pinned per input (a dispatched request can
  reload config and reset it mid-connection-set).

Response re-parse sites (server.c — identified; NOT yet harness-covered):
- `dispatch_nql_query` → NQL parse of the request line (covered incidentally
  by `fuzz_frame`'s real dispatcher — this is where AUD-RES-1 was found).
- The export/snapshot paths re-parse their OWN client responses as JSON:
  describe-object export (`json_parse_object(resp, ...)` in the
  describe-export collect callback), list-objects snapshot parse, and
  get-file's base64 `"data"` extraction (all server.c, all fed by
  `query_collect` round-trips). These run only when the server acts as its
  own client (export/snapshot with a live port) — a harness needs a
  connected two-role setup or `query_collect` interception. Deferred to the
  next step-3 session with this note as the identified symbol list (the
  campaign differential does not reach them either).

## Build

Requires Clang (libFuzzer ships with compiler-rt). On Ubuntu:

```bash
sudo apt-get install -y clang libssl-dev libatomic1
./fuzz/build.sh
```

Outputs binaries to `fuzz/build/`. Each is built with
`-fsanitize=fuzzer,address,undefined`, so crashes are reported with full
stack traces.

## Run locally

```bash
# Grown units go to the LAST corpus dir — keep it under fuzz/build/
# (gitignored) so the seed corpora stay clean:
./fuzz/build/fuzz_json     -max_total_time=60 fuzz/corpora/json/ fuzz/build/corpus-growth/json/
./fuzz/build/fuzz_b64      -max_total_time=60 fuzz/corpora/b64/ fuzz/build/corpus-growth/b64/
./fuzz/build/fuzz_criteria -max_total_time=60 fuzz/corpora/criteria/ fuzz/build/corpus-growth/criteria/
./fuzz/build/fuzz_filename -max_total_time=60 fuzz/corpora/filename/ fuzz/build/corpus-growth/filename/
./fuzz/build/fuzz_cursor    -max_total_time=60 fuzz/corpora/cursor/ fuzz/build/corpus-growth/cursor/
./fuzz/build/fuzz_frame     -max_total_time=60 fuzz/corpora/frame/ fuzz/build/corpus-growth/frame/
```

Found a crash? The artifact lands in the working directory as
`crash-<sha>`. Replay:

```bash
./fuzz/build/fuzz_json crash-abc123...
```

## CI

`.github/workflows/fuzz.yml` runs each harness for 60 seconds per push
and PR. Crash artifacts are uploaded on failure for triage.

## Bugs found by these fuzzers

The first three runs of these harnesses found two real heap-buffer-
overflow bugs in the wire path:

1. `json_skip_value` advanced the cursor past NUL when an unclosed
   `{` or `[` value contained an unclosed inner `"` — fixed at
   `src/db/util.c` with an `if (!*p) break;` guard.
2. `parse_one_criterion`'s IN-list parser walked off the end of a
   strdup'd buffer when the value array had a trailing comma and no
   closing `]` (reachable when an embedded NUL truncates the upstream
   value span) — fixed at `src/db/query.c` with the same guard
   pattern.

Keep this CI green; if it breaks, you've added a parser bug.

## Going further: OSS-Fuzz

For continuous deeper fuzzing on Google's infrastructure (free for
open-source projects), apply at <https://github.com/google/oss-fuzz>
with these harnesses pointed at — they're already in the right shape.
