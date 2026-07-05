# Build: garbage-collect dead sections to shrink the shipped binary

## Execution rules (read first)

- Branch off `main`: `git checkout -b build/gc-sections`.
- This changes only `build.sh`. Verify by building **and** running the full
  suite (the linker change must not drop anything live) and by comparing binary
  size before/after.
- Anchors are quoted exact text. Not found → STOP + `PLAN_NOTES.md`. Leave
  uncommitted.

## Background

The `release` build already does `-O2 -flto=auto` + `strip`. It does **not**
compile with `-ffunction-sections -fdata-sections` or link with section GC, so
unreferenced functions/data (plenty, given how much of `query.c` is cold admin
code) stay in the binary. Adding section GC typically reclaims real space at
zero behavioral risk.

macOS `ld64` and GNU `ld`/`lld` spell this differently:
- Linux: compile `-ffunction-sections -fdata-sections`, link `-Wl,--gc-sections`.
- macOS: `ld64` does dead-strip via `-Wl,-dead_strip` (section flags are a no-op
  but harmless).

## Task 1 — Record the baseline

```
SKIP_TESTS=1 ./build.sh
ls -l build/bin/shard-db
size build/bin/shard-db 2>/dev/null || true
```

Paste the `ls -l` size (bytes) — this is the before number.

## Task 2 — Add the flags to the release build

In `build.sh`, anchor on this exact block:

```bash
    release)
        MODE_CFLAGS="-O2 -g -fno-omit-frame-pointer -flto=auto $MARCH_CFLAGS $WARN_CFLAGS"
        MODE_LDFLAGS="-flto=auto"
        DO_STRIP=${DO_STRIP:-1}
        ;;
```

Replace it with:

```bash
    release)
        # -ffunction-sections/-fdata-sections put each symbol in its own
        # section so the linker can drop unreferenced ones. Linux uses
        # --gc-sections; macOS ld64 uses -dead_strip (the section flags are
        # harmless there). This is on top of -flto + strip.
        GC_CFLAGS="-ffunction-sections -fdata-sections"
        if [ "$(uname)" = "Darwin" ]; then
            GC_LDFLAGS="-Wl,-dead_strip"
        else
            GC_LDFLAGS="-Wl,--gc-sections"
        fi
        MODE_CFLAGS="-O2 -g -fno-omit-frame-pointer -flto=auto $GC_CFLAGS $MARCH_CFLAGS $WARN_CFLAGS"
        MODE_LDFLAGS="-flto=auto $GC_LDFLAGS"
        DO_STRIP=${DO_STRIP:-1}
        ;;
```

## Task 3 — Rebuild, measure, verify correctness

```
SKIP_TESTS=1 ./build.sh
ls -l build/bin/shard-db          # after number — compare to Task 1
./build/bin/shard-db-test run-all
```

Paste before/after sizes and the suite tail. Requirements:
- Suite ends `# total: N passed, 0 failed` (nothing live was stripped).
- Record the byte delta. If the after-size is not smaller, keep the change
  anyway (it's harmless) but note the null result — LTO may already be doing
  most of the work on this toolchain.

## Notes

- Do not add these flags to the `asan`/`tsan`/`debug`/`coverage` cases — those
  intentionally keep everything for tooling.
- Leave uncommitted.
