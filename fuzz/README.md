# Wire-format fuzzers

libFuzzer harnesses for shard-db's network-facing parsers. Every
inbound TCP byte ends up in one of these before any auth check fires,
so memory-safety bugs here are reachable from anywhere on the network.

## Targets

| Harness | What it fuzzes | Source |
|---|---|---|
| `fuzz_json` | `json_parse_object()` plus every `json_obj_*` getter | `src/db/util.c` |
| `fuzz_b64` | `b64_decode()` | `src/db/util.c` |
| `fuzz_criteria` | `parse_criteria_tree()` (the AND/OR/op nesting walker) | `src/db/query.c` |

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
./fuzz/build/fuzz_json     -max_total_time=60 fuzz/corpora/json/
./fuzz/build/fuzz_b64      -max_total_time=60 fuzz/corpora/b64/
./fuzz/build/fuzz_criteria -max_total_time=60 fuzz/corpora/criteria/
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
