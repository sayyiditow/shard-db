# Harden the CLI: replace shell-string `popen` with `posix_spawn` + argv; sweep TODO markers

## Execution rules (read first)

- Branch off `main`: `git checkout -b hardening/cli-posix-spawn`.
- Build: `SKIP_TESTS=1 ./build.sh` (builds `shard-cli` too). There is no daemon
  test harness for the TUI; verification is a clean build + the manual smoke in
  Task 4. CodeQL/scan-build in CI is the durable check.
- Anchors are quoted exact text. Not found → STOP + `PLAN_NOTES.md`. Leave
  uncommitted.

## Background

`src/cli/main.c` builds shell command strings from user input and runs them via
`popen`:

```c
snprintf(cmd, sizeof(cmd), "./shard-db put-file '%s' '%s' '%s' 2>&1",
         oi.dir, oi.object, fs[0].value);
char *out = run_capture(cmd);
```

A single quote in a path/filename breaks out of the quoting → arbitrary shell.
Operator-only (local TUI), so severity is bounded — but it is exactly the
`system()`/shell class already removed elsewhere for CodeQL (see the
`system("cp -r")` / `rm -rf` / `sort -u` replacements in `query.c` and
`index.c`). This reintroduces it.

## Fix strategy

Add `run_capture_argv(char *const argv[])` that spawns `./shard-db` with an
argument vector via `posix_spawn` (no shell, no quoting, no injection),
redirecting the child's stdout+stderr into a pipe. Convert every `run_capture`
call site that interpolates user input to build an argv array instead. The
static server commands (`start`/`stop`/`status`) convert too — they carry no
user input but the `2>&1` is handled by the pipe redirect.

---

## Task 1 — Add `run_capture_argv`

In `src/cli/main.c`, anchor on the existing `run_capture` definition header:

```c
/* Run an external command and capture output via popen. */
static char *run_capture(const char *cmd) {
```

Insert **immediately before** it:

```c
/* Run ./shard-db with an explicit argv (no shell, no injection) and capture
   its stdout+stderr. argv[0] should be "./shard-db"; argv must be NULL-
   terminated. Returns a heap NUL-terminated string (caller frees), or NULL. */
static char *run_capture_argv(char *const argv[]) {
    int pipefd[2];
    if (pipe(pipefd) != 0) return strdup("(pipe failed)");

    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_adddup2(&fa, pipefd[1], STDOUT_FILENO);
    posix_spawn_file_actions_adddup2(&fa, pipefd[1], STDERR_FILENO);
    posix_spawn_file_actions_addclose(&fa, pipefd[0]);
    posix_spawn_file_actions_addclose(&fa, pipefd[1]);

    extern char **environ;
    pid_t pid;
    int rc = posix_spawn(&pid, argv[0], &fa, NULL, argv, environ);
    posix_spawn_file_actions_destroy(&fa);
    close(pipefd[1]);
    if (rc != 0) { close(pipefd[0]); return strdup("(spawn failed)"); }

    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) { close(pipefd[0]); return NULL; }
    ssize_t n;
    char tmp[4096];
    while ((n = read(pipefd[0], tmp, sizeof(tmp))) > 0) {
        if (len + (size_t)n + 1 > cap) {
            while (len + (size_t)n + 1 > cap) cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); close(pipefd[0]); return NULL; }
            buf = nb;
        }
        memcpy(buf + len, tmp, (size_t)n);
        len += (size_t)n;
    }
    close(pipefd[0]);
    int status; waitpid(pid, &status, 0);
    buf[len] = '\0';
    return buf;
}
```

`src/cli/main.c` already includes `<unistd.h>` and `<sys/wait.h>` (top of
file). It does **not** include `<spawn.h>` — `posix_spawn`/
`posix_spawn_file_actions_*` need it. Anchor on the exact top-of-file block:

```c
#define _GNU_SOURCE
#include "cli.h"
#include <unistd.h>
#include <sys/wait.h>
```

Replace with:

```c
#define _GNU_SOURCE
#include "cli.h"
#include <unistd.h>
#include <sys/wait.h>
#include <spawn.h>
```

Add it unconditionally — this is not a "see if the build complains" case;
`posix_spawn_file_actions_init` etc. are declared in `<spawn.h>` and Task 1's
code above will not compile without it.

---

## Task 2 — Convert the server lifecycle commands

In `src/cli/main.c`, anchor on this exact block:

```c
            case 0: cmd = "./shard-db start  2>&1"; stat = "starting daemon..."; break;
            case 1: cmd = "./shard-db stop   2>&1"; stat = "stopping daemon (drains writes)..."; break;
            case 2: cmd = "./shard-db status 2>&1"; stat = "checking status...";  break;
```

Replace with (map the choice to a subcommand string, spawn via argv):

```c
            case 0: cmd = "start";  stat = "starting daemon..."; break;
            case 1: cmd = "stop";   stat = "stopping daemon (drains writes)..."; break;
            case 2: cmd = "status"; stat = "checking status...";  break;
```

Then find the matching `char *out = run_capture(cmd);` in that same function and
replace it with:

```c
        char *sub = (char *)cmd;
        char *argv[] = { "./shard-db", sub, NULL };
        char *out = run_capture_argv(argv);
```

> If `cmd` is declared `const char *`, the `(char *)cmd` cast is required because
> `posix_spawn` takes `char *const argv[]`. This is safe — the child does not
> mutate argv.

---

## Task 3 — Convert the file commands (the injectable ones)

Convert every remaining `run_capture` call that builds a string with `snprintf`
from user input. Locate them with:

```
grep -n "run_capture(" src/cli/main.c
```

This should show exactly 2 remaining call sites after Task 2 (put-file,
get-file — Task 2 already converted the server-lifecycle one). **There is no
delete-file call site to convert**: `files_delete` (near the file-command
block) already sends a plain JSON request via `cli_query`, not a shell
command via `popen`/`run_capture` — it was never affected by this bug. Do not
go looking for a delete-file `run_capture` anchor; it does not exist. If your
grep shows a 4th `run_capture(` site anywhere, STOP and write
`PLAN_NOTES.md` — that would mean the code has drifted from this plan's
assumptions.

For put-file and get-file (both the save-as and stdout variants), replace the
`snprintf(cmd, ...)` + `run_capture(cmd)` pair with a direct argv build.
Templates:

**put-file** (anchor `"./shard-db put-file '%s' '%s' '%s' 2>&1"`):

```c
        char *argv[] = { "./shard-db", "put-file", oi.dir, oi.object, fs[0].value, NULL };
        char *out = run_capture_argv(argv);
```

**get-file with save-as** (anchor `"./shard-db get-file '%s' '%s' '%s' 2>&1"`):

```c
        char *argv[] = { "./shard-db", "get-file", oi.dir, oi.object, fs[0].value, fs[1].value, NULL };
        char *out = run_capture_argv(argv);
```

**get-file to stdout** (anchor the `"... get-file '%s' '%s' '%s' 2>&1 | head -c 4096"`
variant): drop the shell `| head -c 4096` — spawn without it and truncate in C:

```c
        char *argv[] = { "./shard-db", "get-file", oi.dir, oi.object, fs[0].value, NULL };
        char *out = run_capture_argv(argv);
        if (out && strlen(out) > 4096) out[4096] = '\0';  /* preview cap */
```

Once no `run_capture(` call sites remain (put-file and get-file only —
there is no delete-file site, per the note above), **delete** the old `run_capture`
function (the `popen`-based one) entirely. If any static-only call site remains
that you chose not to convert, keep `run_capture` and note why in the final
message.

---

## Task 4 — Verify

```
SKIP_TESTS=1 ./build.sh
grep -n "popen\|run_capture(" src/cli/main.c   # expect: no popen; ideally no run_capture(
./build/bin/shard-db-test run-all
```

Manual injection smoke (optional but recommended): start a daemon, launch
`./build/bin/shard-cli`, and in the put-file form enter a path containing a
single quote and a `; touch /tmp/pwned` suffix. Confirm no `/tmp/pwned` is
created (with argv, the whole string is one literal argument). Paste the suite
output; note the manual result.

---

## Task 5 — Sweep TODO/FIXME/XXX/HACK markers (separate, lightweight)

```
grep -rn "TODO\|FIXME\|XXX\|HACK" src/db/*.c src/db/*.h
```

This returns 15 hits, but almost none are actionable markers — verify before
doing anything:

- **12 hits are in `src/db/xxhash.h`** — this is a vendored third-party
  header (the xxHash library), not project code. **Skip it entirely.** Do
  not edit vendored code as part of this sweep, and do not list its markers
  in the `PLAN_NOTES.md` inventory — they're upstream's tech debt, not
  ours.
- **The remaining 3 hits are false positives**, not real markers — they're
  substring matches of `XXX` inside unrelated text: `embedded.c`'s
  `/tmp/shard-db-unit-XXXXXX` mktemp template, and `\uXXXX` inside comments
  describing JSON escape sequences (`types.h`, `server.c`, `config.c`).
  None of these are actual TODO/FIXME/XXX/HACK markers left by a developer.

**Net result: there is nothing actionable in `src/db` for this task.** Do
not invent cleanup work to fill it. If a `PLAN_NOTES.md` tech-debt inventory
already exists from other tasks, add a one-line note that this sweep found
zero actionable project-code markers (all real hits are vendored
xxhash.h); otherwise skip writing one for this task. Skip the build/test
step for this task since no code changes are expected — only run it if you
did in fact change something.
