# Fix: NQL has no way to express a quoted string value containing a space or an embedded quote

> **Execution rules:** branch off `main` as `fix/nql-quoted-literal-escaping` (fresh
> feature branch, do not work on `main`); do
> tasks in order; leave everything **uncommitted** for review; locate every insertion/edit
> by searching the **quoted anchor text** given in each task (never line numbers — they
> drift, especially under concurrent work on other branches); build with
> `rtk env SKIP_TESTS=1 ./build.sh`; run individual cases with
> `rtk ./build/bin/shard-db-test run <name>` and the full suite with
> `rtk ./build/bin/shard-db-test run-all` before calling this plan done; never claim a step
> passed without pasting the real command output; if a quoted anchor isn't found exactly as
> given, **STOP and write `PLAN_NOTES.md`** describing the mismatch and halt the entire
> execution run immediately — do not guess, reinterpret, or continue to any further task,
> even an unrelated one. Resuming requires the human (or the planning model, re-engaged) to
> read `PLAN_NOTES.md`, decide whether it's a stale-anchor problem (re-derive and patch the
> plan) or a wrong-assumption problem (rethink the plan), and hand back either a patched or
> a fresh plan — execution never resumes on its own initiative. If you hit a decision this
> plan doesn't cover, **stop and ask** — do not improvise.

Source: discovered while scoping `docs/plans/2026-07-22-index-key-json-unescape.md` (a
JSON-unescape bug in write/comparison paths). NQL is a text query language, not JSON — it
was checked as a possible parallel surface for that bug and found to have its own,
unrelated defect instead: it isn't a decoding bug (nothing gets *mis*-decoded), it's a
missing capability — NQL's quoting has no escape convention at all, so certain values
that JSON criteria can already express (a value with a space, or a value containing a
literal `'`) cannot be written as an NQL filter through *any* real caller — not the raw
TCP wire, and not the CLI, which independently reconstructs NQL text with its own
(also-unescaped) wrapper.

**Round 2 revision note:** a review of the round-1 plan found it fixed only the two
in-process decoders (`nql.c`) and missed that `src/db/main.c` has four *producers* that
build NQL text with their own single-quote wrapper, that the raw-TCP docs already show a
double-quote top-level syntax the code didn't support, that the tests didn't exercise any
real caller (dispatch, CLI binary, raw TCP), and several process-compliance gaps. This
revision addresses all of that; see the "Round 2 changes" callout at the end of each
affected section.

## Background: NQL's two quoting layers, and two kinds of caller

NQL commands reach the server as one raw text line (`server.c:596`,
`dispatch_nql_query` → `nql_parse_command(line, &cmd)`). Parsing happens in two layers:

1. **`cmd_split`** (`nql.c:361-379`) splits the whole command line into whitespace-
   separated arguments, the same way a shell would — except its only quoting mechanism
   is `'...'`, used so a multi-word filter clause (`name eq Alice`) can be passed as a
   single argument: `find default users 'name eq Alice'`. This is **the only way** a
   multi-word filter reaches `nql_parse_filter`, since `nql_parse_command` (`nql.c:394`)
   takes the filter as a single `argv[i]` string produced by this split.
2. **`nql_parse_filter`**'s own lexer, `lex_scan` (`nql.c:22-93`), tokenizes *that*
   string. Its own string-literal syntax is *also* `'...'` (`nql.c:31-40`, `TOK_STRING`
   — "content stored without surrounding quotes" per `nql.h:10`), used for a field
   value that needs to contain something a bare identifier can't (a space, for
   instance — `TOK_IDENT` only accepts `[A-Za-z0-9_.+]`).

Both layers use the identical delimiter character, and **neither has any escape
convention**: each just scans for the next `'` and stops there, full stop.

There are also two kinds of caller that *produce* NQL text and must therefore wrap
values correctly for layer 1 to hand off cleanly to layer 2:

3. **The raw TCP wire** — a client sends a pre-built command line directly (e.g. via
   `nc`, a socket library, or a hand-written client). `docs/query-protocol/nql.md` and
   `docs/query-protocol/overview.md` already show these examples using a **double-quoted**
   top-level wrapper (`find default users "age > 18" --limit 5`) — a syntax `cmd_split`
   does not currently recognize at all (round-1 finding).
4. **`src/db/main.c`'s CLI dispatch** — `explain`, `find`, `count`, and `aggregate` each
   take shell-already-split `argv` positional arguments and *re-serialize* them into one
   NQL line to forward to the server, adding their own space-detecting single-quote
   wrapper (`main.c:370-386`, `430-448`, `460-472`, `502-514` — four near-identical
   copies of the same loop). **This wrapper has the identical unescaped-collision bug as
   layer 1**, and today it is the only thing standing between a user's shell invocation
   and the wire text — so even after `nql.c`'s two decoders are fixed, the documented CLI
   path (`./shard-db find default users "name eq 'Alice Smith'"`) remains broken, because
   `main.c` reconstructs it as `find default users 'name eq 'Alice Smith''`, and the inner
   opening quote closes the wrapper before layer 1 ever sees the whole filter.

## Root cause — three distinct, confirmed defects

**Defect 1 — the two in-process layers collide.** Because layer 1's wrapper and layer
2's string delimiter are the same character with no escaping, any attempt to put a
quoted string value inside the filter clause breaks layer 1's own wrapper before layer 2
ever sees it.

Confirmed via a direct call (temporary test, not committed — reverted after
confirming; `git diff --stat` showed a clean `src/test/cases/test_nql.c` before and
after):

```c
NqlCommand cmd;
int r = nql_parse_command("find default users 'name eq 'Alice Smith''", &cmd);
```

Output:
```
SCRATCH-NQL rc=-1 err=expected value, got '' filter=(nil)
```

`cmd_split` treats the *first* `'` after `users ` as the wrapper-open, then stops the
wrapper at the very next `'` — i.e. it captures `name eq ` (with the value not yet
started) as the entire filter clause, discarding `Alice Smith''` as leftover text.
`nql_parse_filter` then correctly reports a parse error on `name eq ` because there's no
value at all. **This means no multi-word or apostrophe-containing string value can be
expressed through the raw wire path today** — not a silent wrong-match (like the JSON
bug), an outright parse failure.

**Defect 2 — even bypassing layer 1, layer 2's own quoting has no escape either.**
Confirmed by calling the filter lexer directly, skipping `cmd_split` entirely:

```c
CriteriaNode *tree = nql_parse_filter("name eq 'Alice Smith'", err, sizeof(err));
/* → succeeds: tree->leaf.value == "Alice Smith" (defect 1 doesn't apply here since
   cmd_split wasn't involved — this confirms layer 2 alone works fine for a value with
   a space and NO embedded quote) */

CriteriaNode *tree2 = nql_parse_filter("name eq 'O'Brien'", err2, sizeof(err2));
```

Output:
```
SCRATCH-NQL embedded-apostrophe tree=(nil) err=unexpected token at end of filter
```

A value containing a literal `'` (a last name like `O'Brien` — realistic, unremarkable
data any JSON-driven `insert` can already store) cannot be expressed as an NQL string
literal at all, even when layer 1 isn't in the picture. `lex_scan`'s scan
(`nql.c:34`, `while (l->src[l->pos] && l->src[l->pos] != '\'' ...)`) stops at the first
`'`, full stop, no lookahead.

**Defect 3 (round 2) — `main.c`'s CLI serializers have the same collision as layer 1, one
level up.** Each of the four command handlers (`explain`, `find`, `count`, `aggregate`)
rebuilds an NQL line from `argv` by wrapping any token containing a space in `'...'`,
with no escaping. Confirmed by inspection of all four sites (identical logic, minor
indentation/context differences — see Call-site inventory):

```c
if (strchr(argv[i], ' ')) {
    strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
    strncat(nql, argv[i], sizeof(nql) - strlen(nql) - 1);
    strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
} else {
    strncat(nql, argv[i], sizeof(nql) - strlen(nql) - 1);
}
```

Given the shell invocation `./shard-db find default users "name eq 'Alice Smith'"` (a
single shell argument, `argv[3] = name eq 'Alice Smith'`), this loop sees a space in
`argv[3]` and wraps the *whole thing* in a fresh pair of single quotes:
`'name eq 'Alice Smith''` — colliding with the embedded single quotes exactly as in
Defect 1, and unfixable by patching `nql.c` alone. **This is why Defect 1's fix alone is
insufficient: the documented CLI path stays broken until `main.c`'s builders are also
fixed.**

## Design decisions (human-approved before Task 1)

### D1 — Escape convention: doubled quote (`''`/`""` → literal), not backslash

Considered and rejected: backslash-escaping (`\'`, mirroring the JSON plan's `\"`) —
introduces a *second* special character (`\`) that would then itself need its own escape
(`\\`) for a value containing a literal backslash, adding complexity neither layer
currently has any need for. Doubling only ever needs to special-case the one character
that's already special at each layer, and it's the same convention SQL/CSV use for
exactly this situation.

### D2 — Top-level wrapper: add double-quote support, keep single-quote (human-approved)

Round-1 considered swapping layer 1's delimiter to `"` and rejected it as breaking.
Round-2 review pointed out the docs (`nql.md:11`, `nql.md:251`, `overview.md:14`)
*already* show double-quoted top-level examples that don't work today, and asked for an
explicit decision. **Decision: add `"..."` as a second top-level wrapper delimiter in
`cmd_split`, alongside the existing `'...'` one, both using the same doubling-escape
convention as their own delimiter.** This is non-breaking (every existing single-quote
caller/test keeps working byte-for-byte) and makes the docs' existing examples correct
without a doc rewrite. It also gives `main.c`'s CLI builders (Task 3) a wrapper that
never collides with layer 2's single-quote literals in the common case — no nested
doubling needed there at all, since `"name eq 'O''Brien'"` has no `"` in it.

### D3 — Unterminated literal handling differs by layer (human-approved)

- **Layer 2 (`lex_scan`, filter string literal): becomes a hard parse error
  (`TOK_ERR`).** Today, `name eq 'Alice` (no closing quote) silently succeeds with
  whatever was scanned as the value — an accepted-but-wrong-mask bug, not a feature.
  **This is a deliberate, approved, externally-observable behavior change** — flag it in
  the PR description as a breaking/tightening change requiring the human's sign-off at
  merge time (per this repo's breaking-change review rule), and cover it with an
  asserted regression test (Task 1g), not silently.
- **Layer 1 (`cmd_split`, top-level wrapper): stays tolerant**, degrading to
  "everything decoded up to end of input" — unchanged from today, and consistent with
  the fact that `cmd_split` has no channel to report a parse error back to its caller
  (it only returns an argument count). Covered by an asserted test (Task 1h) that pins
  this exact, unchanged behavior (not just "doesn't crash").

## The shared decoder function — `nql_read_quoted`

New, `static`, file-local to `nql.c` (no other translation unit needs it — see Call-site
inventory). Parameterized by delimiter so both `'` and `"` reuse one implementation
(needed for D2).

```c
/* Reads a quoted literal delimited by `delim` (either '\'' or '"'),
   starting just after the opening delimiter at *pp, decoding the
   SQL-style doubled-delimiter escape (delim,delim -> one literal
   delim) -- the only escape either quoting layer in this file needs.
   Writes the decoded content into out (bounded by out_sz - 1), always
   NUL-terminating whatever was decoded so far, and advances *pp to
   just past the closing delimiter (or to the terminating NUL if the
   literal never closes). A doubled delimiter never ends the literal;
   a single one does. Returns 0 if a genuine closing delimiter was
   found before filling out_sz, -1 if the input ran out first
   (unterminated literal) or out_sz was too small -- callers decide
   whether that's a hard parse error (lex_scan does, per D3) or a
   tolerant best-effort read (cmd_split does, per D3, matching its
   pre-existing "no closing quote found" tolerance since it has no
   channel to report a parse error back to its caller). This is the
   one place any quote-scanning site in this file decodes a quoted
   span -- cmd_split (top-level command-argument splitting, both '
   and " delimiters) and lex_scan (filter-expression string tokens,
   ' only) both call it instead of each hand-rolling their own scan
   loop. */
static int nql_read_quoted(const char **pp, char *out, size_t out_sz, char delim) {
    const char *p = *pp;
    size_t o = 0;
    int ok = 0;
    while (*p) {
        if (*p == delim) {
            if (p[1] == delim) {
                if (o + 1 >= out_sz) break;
                out[o++] = delim;
                p += 2;
                continue;
            }
            p++;
            ok = 1;
            break;
        }
        if (o + 1 >= out_sz) break;
        out[o++] = *p++;
    }
    out[o] = '\0';
    *pp = p;
    return ok ? 0 : -1;
}
```

Both in-process defects are fixed by the same mechanism, applied at each layer
independently:

- Fixing layer 2 alone fixes Defect 2 (a value with an embedded apostrophe, parsed
  directly via `nql_parse_filter`, or reached through a layer-1 double-quote wrapper
  that never collides with it at all — the D2 common case).
- Fixing layer 1 alone (applying the same doubling rule to both `'` and `"` in
  `cmd_split`'s wrapper scan) fixes Defect 1 for callers that still use the single-quote
  wrapper (e.g. existing `test_nql.c` cases, or a raw-TCP client that prefers `'`) — a
  quoted string value can now be nested inside it, *provided* every `'` meant to survive
  into the decoded filter text is itself doubled at the wire level:
  `find default users 'name eq ''O''''Brien'''`. This nested-doubling form keeps
  working and is tested (Task 1c/1d) as a permanent regression guard, but is **not**
  the form `main.c` will generate going forward — Task 3 uses the double-quote wrapper
  (D2), which needs no nesting for this same value:
  `find default users "name eq 'O''Brien'"`.
- Fixing Defect 3 requires a *third*, separate change: `main.c`'s four CLI builders
  stop hand-rolling their own space-detecting single-quote wrap and instead call a new
  shared public encoder, `nql_append_arg` (Task 2e), which wraps in `"..."` (doubling
  any embedded `"`) whenever an argument needs quoting, preserves empty argv elements,
  and rejects CR/LF or insufficient capacity atomically. See Task 3.

## Call-site inventory

**Decoders (consume NQL text) — both patched in Task 2:**

| # | File:line | Function | Current scan (before) | After |
|---|-----------|----------|------------------------|-------|
| 1 | `nql.c:368-371` | `cmd_split` (top-level command-argument split) | `while (*p && *p != '\'') p++;` (single-quote only) | recognizes `'` **or** `"` as opener; calls `nql_read_quoted` with the matching delimiter, in-place (decoded length is always ≤ raw length, so writing the decoded bytes back into the same span `nql_read_quoted` reads from is safe — no extra allocation, matches `cmd_split`'s existing zero-copy pointer-slicing style) |
| 2 | `nql.c:31-40` | `lex_scan` (filter-expression `TOK_STRING` literal) | `while (l->src[l->pos] && l->src[l->pos] != '\'' && i < ...) t->text[i++] = ...;` | calls `nql_read_quoted` (delim `'`) into `t->text` (separate fixed buffer, no aliasing concern); unterminated now yields `TOK_ERR` per D3 |

**Producers (build NQL text from other input) — all four patched in Task 3, round 2
addition:**

| # | File:line | Function | Current wrap logic (before) | After |
|---|-----------|----------|------------------------------|-------|
| 3 | `main.c:370-386` (inside the `strncat(nql, " '", ...)` block and the `for (int i = 6; ...)` flags loop) | `explain` handler | hand-rolled: unescaped `'...'` wrap for the filter arg; unescaped `'...'` wrap per space-containing flag token | all arguments use checked `nql_append_arg`; failure aborts before sending |
| 4 | `main.c:437-448` | `find` handler | hand-rolled per-token loop, unescaped `'...'` wrap on space | checked `nql_append_arg` loop |
| 5 | `main.c:461-472` | `count` handler, NQL branch | same hand-rolled loop | checked `nql_append_arg` loop |
| 6 | `main.c:503-514` | `aggregate` handler, NQL branch | same hand-rolled loop | checked `nql_append_arg` loop |

No other call site in the repository does its own quote scanning or wrapping for NQL
text. Confirmed by search:
- `read_value`/`read_list`/`parse_predicate`/`parse_one_criterion` (`query_plan.c`, the
  JSON criteria path) all consume already-tokenized JSON values and are untouched by
  this plan — separate wire format, covered instead by
  `docs/plans/2026-07-22-index-key-json-unescape.md`.
- `main.c`'s other `cmd_query_json` callers (`stats`, `db-dirs`, `drop`, `estimate-index`,
  `reindex`, etc.) build pure JSON, not NQL text, and don't touch quoting at all.
- No other file under `src/` matches `argv[i]` + `strchr(..., ' ')` NQL-wrapping logic
  (grepped for the `strchr(argv` pattern repo-wide; only the four `main.c` sites match).
- `--filter` and `--having` are not separately hand-coded in `main.c` — they arrive as
  ordinary `argv[i]` tokens inside the same generic per-argument loop as every other
  flag, so the same fix covers them automatically. Task 1k adds an explicit test proving
  this (round-2 addition, per review finding 3).

## Task 1 — Test-first: unit and real-caller regression tests

Add these as new registered cases (`TEST_REGISTER`), not modifications to existing
passing tests — the existing bare-identifier-value tests
(`test_nql_find_command`, `test_nql_count_command`, etc.) must keep passing byte-for-byte
unchanged, since layer 1's single-quote path is purely additive (D2 adds a second,
independent wrapper option; it doesn't change the first one's behavior).

Insertion anchor — append the new function bodies just above this exact line (last line
of the file):

```c
TEST_REGISTER("nql-flags",            test_nql_flags);
```

### 1a. Layer 2 alone: embedded apostrophe in a directly-parsed filter

```c
static int test_nql_filter_embedded_quote(void) {
    char err[256];
    CriteriaNode *tree = nql_parse_filter("name eq 'O''Brien'", err, sizeof(err));
    ASSERT_TRUE(tree != NULL, "nql_parse_filter parses a doubled-quote literal");
    if (!tree) return t_ctx->failed > 0 ? 1 : 0;
    ASSERT_EQ_STR(tree->leaf.value, "O'Brien", "value decodes the doubled quote to a literal '");
    free_criteria_tree(tree);
    return t_ctx->failed > 0 ? 1 : 0;
}
```

Run this *before* Task 2's fix and paste the failing output (today: `tree == NULL`,
`err == "unexpected token at end of filter"`).

### 1b. Layer 2 alone: value with a space, no embedded quote (control — already works)

```c
static int test_nql_filter_quoted_space_value(void) {
    char err[256];
    CriteriaNode *tree = nql_parse_filter("name eq 'Alice Smith'", err, sizeof(err));
    ASSERT_TRUE(tree != NULL, "nql_parse_filter parses a space-containing literal");
    if (!tree) return t_ctx->failed > 0 ? 1 : 0;
    ASSERT_EQ_STR(tree->leaf.value, "Alice Smith", "value is unchanged (no doubling in this input)");
    free_criteria_tree(tree);
    return t_ctx->failed > 0 ? 1 : 0;
}
```

This already passes today (confirmed above) — included as a control to prove the fix
doesn't disturb the no-escaping-needed case, and as a permanent regression guard.

### 1c. Layer 1 (single-quote wrapper) + layer 2 together: nested doubling

```c
static int test_nql_command_nested_quote_escape(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default users 'name eq ''O''''Brien'''", &cmd);
    ASSERT_EQ_INT(r, 0, "nql_parse_command succeeds on nested-doubled literal");
    ASSERT_TRUE(cmd.filter != NULL, "filter parsed");
    if (cmd.filter) {
        ASSERT_EQ_STR(cmd.filter->leaf.field, "name", "field == name");
        ASSERT_EQ_STR(cmd.filter->leaf.value, "O'Brien", "value decodes through both layers to O'Brien");
    }
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}
```

Run before Task 2's fix and paste the failing output (today: `r == -1`,
`cmd.err == "expected value, got ''"`, matching Defect 1's repro above).

### 1d. Layer 1 (single-quote wrapper) + layer 2 together: plain multi-word value, no embedded quote (control)

```c
static int test_nql_command_quoted_space_value(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default users 'name eq ''Alice Smith'''", &cmd);
    ASSERT_EQ_INT(r, 0, "nql_parse_command succeeds");
    ASSERT_TRUE(cmd.filter != NULL, "filter parsed");
    if (cmd.filter) ASSERT_EQ_STR(cmd.filter->leaf.value, "Alice Smith", "value == Alice Smith");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}
```

Also failing before Task 2 (same Defect 1 mechanism as 1c, one level of doubling instead
of two since there's no embedded apostrophe in the value itself — only the two structural
apostrophes layer 2 needs around the value have to survive layer 1, hence single
doubling: `''Alice Smith''` inside the outer wrapper).

### 1e (round 2, new). Layer 1 double-quote wrapper (D2): the documented common case, no nesting needed

```c
static int test_nql_command_double_quote_wrapper(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default users \"name eq 'O''Brien'\"", &cmd);
    ASSERT_EQ_INT(r, 0, "nql_parse_command succeeds with a double-quoted top-level wrapper");
    ASSERT_TRUE(cmd.filter != NULL, "filter parsed");
    if (cmd.filter) {
        ASSERT_EQ_STR(cmd.filter->leaf.field, "name", "field == name");
        ASSERT_EQ_STR(cmd.filter->leaf.value, "O'Brien", "value decodes to O'Brien with no nested doubling needed");
    }
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}
```

Fails before Task 2 (today `cmd_split` doesn't recognize `"` as an opener at all — the
whole double-quoted span is instead split on internal whitespace as several bare
identifier tokens, so `nql_parse_command` will report a different, argument-count-driven
parse error). Paste the actual failing output.

### 1f (round 2, new). Layer 1 double-quote wrapper: embedded literal `"` doubles the same way

```c
static int test_nql_command_double_quote_embedded_quote(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default users \"name eq 'Say \"\"hi\"\"'\"", &cmd);
    ASSERT_EQ_INT(r, 0, "nql_parse_command succeeds with an embedded doubled double-quote");
    ASSERT_TRUE(cmd.filter != NULL, "filter parsed");
    if (cmd.filter) ASSERT_EQ_STR(cmd.filter->leaf.value, "Say \"hi\"", "embedded \"\" decodes to a literal \"");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}
```

### 1g (round 2, new; supersedes round 1's unasserted 1e). Layer 2 unterminated literal — now a hard error (D3)

```c
static int test_nql_filter_unterminated_string_is_error(void) {
    char err[256] = {0};
    CriteriaNode *tree = nql_parse_filter("name eq 'Alice", err, sizeof(err));
    ASSERT_TRUE(tree == NULL, "unterminated string literal is now rejected, not silently truncated");
    ASSERT_TRUE(err[0] != '\0', "a parse error message is produced");
    if (tree) free_criteria_tree(tree);
    return t_ctx->failed > 0 ? 1 : 0;
}
```

This is the deliberate, approved behavior tightening from D3 — before Task 2, this
currently *passes* the old (wrong) way: `tree != NULL` and `tree->leaf.value == "Alice"`
(silently truncated, no error). Run before Task 2 and paste that output, confirming the
old silent-acceptance behavior, then confirm this new assertion passes after Task 2.

### 1h (round 2, new; supersedes round 1's unasserted 1e). Layer 1 unterminated literal — stays tolerant (D3)

```c
static int test_nql_command_unterminated_quote_tolerant(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default users 'name eq Alice", &cmd);
    /* cmd_split has no error channel; it degrades to "everything decoded to end of
       input" both before and after this fix -- pin that the captured filter argument
       is exactly the tail after the opening quote, unchanged from today. */
    ASSERT_EQ_INT(r, 0, "unterminated top-level wrapper remains a tolerant success");
    ASSERT_TRUE(cmd.filter != NULL, "captured filter tail is parsed");
    if (cmd.filter) {
        ASSERT_EQ_STR(cmd.filter->leaf.field, "name", "captured field == name");
        ASSERT_EQ_STR(cmd.filter->leaf.value, "Alice", "captured value == Alice");
    }
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}
```

This passes both before and after Task 2: layer 1 strips the unmatched opening wrapper
and captures `name eq Alice` through end-of-input; layer 2 then parses that captured
text as a valid bare-identifier predicate. It is a compatibility control, not one of
the regression cases expected to go red before the fix.

### 1i (round 2, new). Empty string literal — explicit, not left to the executor's judgment

```c
static int test_nql_filter_empty_string_literal(void) {
    char err[256];
    CriteriaNode *tree = nql_parse_filter("name eq ''", err, sizeof(err));
    ASSERT_TRUE(tree != NULL, "empty '' is a valid, immediately-closed literal, not unterminated");
    if (!tree) return t_ctx->failed > 0 ? 1 : 0;
    ASSERT_EQ_STR(tree->leaf.value, "", "value decodes to the empty string");
    free_criteria_tree(tree);
    return t_ctx->failed > 0 ? 1 : 0;
}
```

### 1j (round 3, new). Encoder contract: exact escaping, empty arguments, and atomic failure

This test names the new public function before its body exists. Add the declaration to
`src/db/nql.h` first, then add the test. The resulting pre-fix build must fail at the
link step with an undefined `nql_append_arg`; paste that failure as this test's red
proof. Task 2e supplies the body and makes the same test pass.

Anchor in `src/db/nql.h`:

```c
CriteriaNode *nql_parse_filter(const char *src, char *err_out, size_t err_sz);
```

Insert:

```c

/* Appends one argv element to an NQL wire command. Returns 0 on success.
   Returns -1 without modifying dst if the complete encoded argument plus
   separator and terminating NUL will not fit, if dst is not NUL-terminated
   within dst_sz, or if arg contains CR/LF (which cannot occur inside the
   newline-delimited wire protocol). Empty args are encoded as "". */
int nql_append_arg(char *dst, size_t dst_sz, const char *arg);
```

Add this function with the other Task 1 functions above the registration block:

```c
static int test_nql_append_arg_contract(void) {
    char quoted[128] = "find";
    ASSERT_EQ_INT(nql_append_arg(quoted, sizeof quoted,
                                 "name eq 'Say \"hi\"'"),
                  0, "encoder accepts a quoted argument");
    ASSERT_EQ_STR(quoted, "find \"name eq 'Say \"\"hi\"\"'\"",
                  "encoder doubles the top-level wrapper delimiter");

    char empty[32] = "find";
    ASSERT_EQ_INT(nql_append_arg(empty, sizeof empty, ""), 0,
                  "encoder preserves an empty argv element");
    ASSERT_EQ_STR(empty, "find \"\"", "empty argv element is encoded as double quotes");

    char small[12] = "find";
    char before[sizeof small];
    memcpy(before, small, sizeof small);
    ASSERT_EQ_INT(nql_append_arg(small, sizeof small, "too long here"), -1,
                  "encoder rejects an argument that cannot fit completely");
    ASSERT_EQ_STR(small, before, "capacity failure leaves dst unchanged");

    char newline[32] = "find";
    ASSERT_EQ_INT(nql_append_arg(newline, sizeof newline, "x\ny"), -1,
                  "encoder rejects an embedded wire line break");
    ASSERT_EQ_STR(newline, "find", "line-break rejection leaves dst unchanged");
    return t_ctx->failed > 0 ? 1 : 0;
}
```

Complete `TEST_REGISTER` block to append (all ten new cases, exact names):

```c
TEST_REGISTER("nql-filter-embedded-quote",          test_nql_filter_embedded_quote);
TEST_REGISTER("nql-filter-quoted-space-value",      test_nql_filter_quoted_space_value);
TEST_REGISTER("nql-command-nested-quote-escape",    test_nql_command_nested_quote_escape);
TEST_REGISTER("nql-command-quoted-space-value",     test_nql_command_quoted_space_value);
TEST_REGISTER("nql-command-double-quote-wrapper",   test_nql_command_double_quote_wrapper);
TEST_REGISTER("nql-command-double-quote-embedded",  test_nql_command_double_quote_embedded_quote);
TEST_REGISTER("nql-filter-unterminated-is-error",   test_nql_filter_unterminated_string_is_error);
TEST_REGISTER("nql-command-unterminated-tolerant",  test_nql_command_unterminated_quote_tolerant);
TEST_REGISTER("nql-filter-empty-string-literal",    test_nql_filter_empty_string_literal);
TEST_REGISTER("nql-append-arg-contract",            test_nql_append_arg_contract);
```

### 1k (round 3, new). Real raw-wire and CLI callers in one dedicated case

Create `src/test/cases/test_nql_quoted_literals.c` with this complete content. This
single, named case owns its isolated daemon/object and exercises raw TCP dispatch, the
real `shard-db` binary, the documented aggregate `--filter` form, embedded double
quotes through the CLI encoder, and an empty argv element. Wanted-value assertions are
paired with unwanted-value assertions so an ignored filter/full-table result cannot
pass accidentally.

```c
/* Regression: quoted NQL literals must survive raw-wire parsing and the
 * CLI's argv -> NQL serialization without widening the match set. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void nql_quote_base_of(const char *db_root, char *out, size_t out_sz) {
    const char *slash = strrchr(db_root, '/');
    if (!slash || slash == db_root) { out[0] = '\0'; return; }
    size_t n = (size_t)(slash - db_root);
    if (n + 1 > out_sz) { out[0] = '\0'; return; }
    memcpy(out, db_root, n);
    out[n] = '\0';
}

static int test_nql_quoted_literals_real_callers(void) {
    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db binary not found");
        return 1;
    }

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char base[256];
    nql_quote_base_of(env.db_root, base, sizeof base);

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "add-dir succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"nql_quote_t\"," 
        "\"fields\":[\"name:varchar:32\"],\"splits\":8}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create-object succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"nql_quote_t\",\"records\":{" 
        "\"k1\":{\"name\":\"Alice Smith\"},"
        "\"k2\":{\"name\":\"O'Brien\"},"
        "\"k3\":{\"name\":\"Say \\\"hi\\\"\"}}}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "bulk-insert succeeds");
    free(resp); resp = NULL;

    tc_request(tc, "find default nql_quote_t \"name eq 'O''Brien'\"", &resp);
    ASSERT_CONTAINS(resp, "O'Brien", "raw NQL matches apostrophe record");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "Alice Smith") == NULL,
                "raw apostrophe query excludes Alice Smith");
    free(resp); resp = NULL;

    tc_request(tc, "find default nql_quote_t \"name eq 'Alice Smith'\"", &resp);
    ASSERT_CONTAINS(resp, "Alice Smith", "raw NQL matches space-containing record");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "O'Brien") == NULL,
                "raw space query excludes apostrophe record");
    free(resp); resp = NULL;

    char *out = tu_capture_cmd(
        "cd %s && %s find default nql_quote_t \"name eq 'O''Brien'\" 2>&1",
        base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "O'Brien") != NULL,
                "CLI find matches apostrophe record");
    ASSERT_TRUE(out && strstr(out, "Alice Smith") == NULL,
                "CLI find does not widen to Alice Smith");
    free(out);

    out = tu_capture_cmd(
        "cd %s && %s find default nql_quote_t \"name eq 'Say \\\"hi\\\"'\" 2>&1",
        base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "Say \\\"hi\\\"") != NULL,
                "CLI encoder doubles embedded top-level double quotes");
    ASSERT_TRUE(out && strstr(out, "Alice Smith") == NULL,
                "CLI double-quote query does not widen to Alice Smith");
    free(out);

    out = tu_capture_cmd(
        "cd %s && %s aggregate default nql_quote_t 'count()' "
        "--filter \"name eq 'O''Brien'\" 2>&1",
        base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "\"count\":1") != NULL,
                "CLI aggregate --filter matches exactly one record");
    ASSERT_TRUE(out && strstr(out, "\"count\":2") == NULL,
                "CLI aggregate --filter does not widen the match set");
    free(out);

    out = tu_capture_cmd(
        "cd %s && %s find default nql_quote_t --fields \"\" --limit 1 2>&1",
        base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "\"error\"") == NULL,
                "CLI encoder preserves an empty argv element");
    free(out);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-quoted-literals-real-callers",
              test_nql_quoted_literals_real_callers)
```

Wire the new file into `build.sh`. Anchor:

```bash
    src/test/cases/test_nql.c \
    src/test/cases/test_nql_joins.c \
```

Replace with:

```bash
    src/test/cases/test_nql.c \
    src/test/cases/test_nql_quoted_literals.c \
    src/test/cases/test_nql_joins.c \
```

### Verification (before implementing Task 2)

Execute Task 1 in two explicit phases so the new encoder contract can be tested without
preventing the parser/real-caller regression cases from linking and running first.

1. **Phase A:** add 1a-1i and 1k, including the first nine registration lines (through
   `nql-filter-empty-string-literal`) and the `build.sh` entry. Do not add 1j's header
   declaration, function, or tenth registration line yet.
2. `rtk env SKIP_TESTS=1 ./build.sh`.
3. Run each new Phase-A case individually and paste the real output:
   `rtk ./build/bin/shard-db-test run nql-filter-embedded-quote`,
   `rtk ./build/bin/shard-db-test run nql-command-nested-quote-escape`,
   `rtk ./build/bin/shard-db-test run nql-command-quoted-space-value`,
   `rtk ./build/bin/shard-db-test run nql-command-double-quote-wrapper`,
   `rtk ./build/bin/shard-db-test run nql-command-double-quote-embedded`,
   `rtk ./build/bin/shard-db-test run nql-filter-unterminated-is-error` (the new
   assertion fails because the old parser returns a non-NULL tree; paste that real
   failure), and
   `rtk ./build/bin/shard-db-test run test-nql-quoted-literals-real-callers`.
4. `nql-command-unterminated-tolerant`, `nql-filter-quoted-space-value`, and
   `nql-filter-empty-string-literal` are compatibility controls that pass before the
   fix. Run and paste:
   `rtk ./build/bin/shard-db-test run nql-command-unterminated-tolerant`,
   `rtk ./build/bin/shard-db-test run nql-filter-quoted-space-value`, and
   `rtk ./build/bin/shard-db-test run nql-filter-empty-string-literal`; then run the
   same three exact commands after Task 2 and confirm they remain green.
5. **Phase B:** add 1j's `nql.h` declaration, test function, and tenth registration
   line exactly as specified.
6. `rtk env SKIP_TESTS=1 ./build.sh` — paste the expected undefined-reference failure
   for `nql_append_arg`. Do not weaken or remove the test; Task 2e supplies the missing
   implementation.

## Task 2 — Implement the fix

### 2a. Add `nql_read_quoted`

File: `src/db/nql.c`. Anchor — insert immediately after this exact block (so the new
function sits above both call sites; `lex_scan` is defined right after it and `cmd_split`
is further down the file, so **neither call site needs a forward declaration** — this
supersedes round 1's incorrect claim that `cmd_split` might need one):

```c
static void skip_ws(NqlLexer *l) {
    while (l->src[l->pos] && isspace((unsigned char)l->src[l->pos]))
        l->pos++;
}
```

Insert the complete `nql_read_quoted` function (full body given in the shared-decoder
section above) directly after this block, before `static void lex_scan(NqlLexer *l) {`.

### 2b. Wire into `lex_scan` (`nql.c:31-40`)

Anchor:

```c
    /* Single-quoted string: content stored without quotes */
    if (c == '\'') {
        l->pos++;
        int i = 0;
        while (l->src[l->pos] && l->src[l->pos] != '\'' && i < (int)sizeof(t->text)-1)
            t->text[i++] = l->src[l->pos++];
        if (l->src[l->pos] == '\'') l->pos++;
        t->text[i] = '\0';
        t->type = TOK_STRING;
        return;
    }
```

Replace with:

```c
    /* Single-quoted string: content stored without quotes. A doubled quote
       ('') decodes to one literal ' -- see nql_read_quoted. Per D3, an
       unterminated literal is now a hard parse error (TOK_ERR), a
       deliberate tightening from the old silent-truncation behavior. */
    if (c == '\'') {
        l->pos++;
        const char *p = &l->src[l->pos];
        if (nql_read_quoted(&p, t->text, sizeof t->text, '\'') != 0) {
            snprintf(l->err, sizeof l->err, "unterminated string literal");
            t->type = TOK_ERR;
            l->pos = (int)(p - l->src);
            return;
        }
        l->pos = (int)(p - l->src);
        t->type = TOK_STRING;
        return;
    }
```

### 2c. Wire into `cmd_split` (`nql.c:368-371`), adding the D2 double-quote wrapper

Anchor:

```c
        if (*p == '\'') {
            p++; argv[n++] = p;
            while (*p && *p != '\'') p++;
            if (*p) *p++ = '\0';
        } else {
```

Replace with:

```c
        if (*p == '\'' || *p == '"') {
            char delim = *p;
            p++;
            char *start = p;
            const char *cursor = p;
            nql_read_quoted(&cursor, start, (size_t)(buf + bufsz - start), delim);
            p = (char *)cursor;
            argv[n++] = start;
        } else {
```

Note the return value is intentionally ignored here (unlike 2b, per D3) — `cmd_split`
has no channel to report a parse error to its caller (it only returns an argument
count), so an unterminated literal keeps degrading to "everything decoded up to the end
of input," matching its behavior before this change, for **both** delimiters.
`nql_read_quoted` always NUL-terminates `start` in place regardless of its return value,
so `argv[n]` is always a valid C string either way.

### 2d. Update the stale comment in `nql.h`

Anchor (`nql.h:10`):

```c
    TOK_STRING,     /* 'value' — content stored without surrounding quotes */
```

Replace with:

```c
    TOK_STRING,     /* 'value' — content stored without surrounding quotes;
                        '' inside a literal decodes to one literal '; an
                        unterminated literal is a hard parse error (TOK_ERR) */
```

### 2e (round 2, new). Add the public CLI-argument encoder `nql_append_arg`

This is the function Task 3 uses so `main.c` stops hand-rolling its own (buggy)
wrap-if-space loop four times. Public because `main.c` is a different translation unit;
Task 1j already added its complete `nql.h` declaration as the test-first interface
scaffold. The implementation preflights the entire encoded argument before writing a
byte, so capacity and CR/LF failures are atomic rather than silently sending a truncated
or multi-line command. Empty argv elements are represented as `""`.

Anchor in `nql.c` — insert the function definition directly after `cmd_split`'s closing
brace (the `}` that ends the function whose body was just modified in 2c), i.e. right
before this exact line:

```c
/* Normalize an order-direction token to canonical lowercase "asc"/"desc"
```

Insert:

```c
int nql_append_arg(char *dst, size_t dst_sz, const char *arg) {
    if (!dst || !arg || dst_sz == 0) return -1;
    const char *dst_end = memchr(dst, '\0', dst_sz);
    if (!dst_end) return -1;
    size_t len = (size_t)(dst_end - dst);

    int quoted = (*arg == '\0');
    for (const unsigned char *s = (const unsigned char *)arg; *s; s++) {
        if (*s == '\r' || *s == '\n') return -1;
        if (isspace(*s) || *s == '"') quoted = 1;
    }

    size_t remaining = dst_sz - len - 1; /* excludes the existing NUL */
    size_t needed = len > 0 ? 1 : 0;     /* separator */
    if (quoted) {
        if (needed > remaining || 2 > remaining - needed) return -1;
        needed += 2;                     /* opening + closing wrapper */
    }
    for (const char *s = arg; *s; s++) {
        size_t add = (quoted && *s == '"') ? 2 : 1;
        if (needed > remaining || add > remaining - needed) return -1;
        needed += add;
    }

    char *w = dst + len;
    if (len > 0) *w++ = ' ';
    if (quoted) *w++ = '"';
    for (const char *s = arg; *s; s++) {
        if (quoted && *s == '"') *w++ = '"';
        *w++ = *s;
    }
    if (quoted) *w++ = '"';
    *w = '\0';
    return 0;
}
```

`nql.c` already defines public `nql_parse_filter` and `nql_parse_command`; this exact
anchor deterministically places the new public function before them. No placement or
signature decision is left to the executor.

## Task 3 (round 2, new) — Fix `main.c`'s four CLI builders

File: `src/db/main.c`.

### 3a. Add the include

Anchor:

```c
#include "types.h"
#include "slotcask.h"
```

Replace with:

```c
#include "types.h"
#include "slotcask.h"
#include "nql.h"
```

### 3b. `explain` handler

Anchor:

```c
        /* Build NQL string: "<subcmd> <dir> <obj> '<filter>' --explain [remaining flags]" */
        char nql[8192];
        snprintf(nql, sizeof(nql), "%s %s %s", subcmd, dir, object);
        if (filter && filter[0]) {
            strncat(nql, " '", sizeof(nql) - strlen(nql) - 1);
            strncat(nql, filter, sizeof(nql) - strlen(nql) - 1);
            strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
        }
        strncat(nql, " --explain", sizeof(nql) - strlen(nql) - 1);
        for (int i = 6; i < argc; i++) {
            strncat(nql, " ", sizeof(nql) - strlen(nql) - 1);
            if (strchr(argv[i], ' ')) {
                strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
                strncat(nql, argv[i], sizeof(nql) - strlen(nql) - 1);
                strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
            } else {
                strncat(nql, argv[i], sizeof(nql) - strlen(nql) - 1);
            }
        }
        return cmd_query_json(port, nql);
```

Replace with:

```c
        /* Build NQL from already-split argv without allowing a partial argument. */
        char nql[8192] = {0};
        if (nql_append_arg(nql, sizeof nql, subcmd) != 0 ||
            nql_append_arg(nql, sizeof nql, dir) != 0 ||
            nql_append_arg(nql, sizeof nql, object) != 0 ||
            (filter && filter[0] && nql_append_arg(nql, sizeof nql, filter) != 0) ||
            nql_append_arg(nql, sizeof nql, "--explain") != 0) {
            fprintf(stderr, "Error: NQL command is too long or contains a line break\n");
            return 1;
        }
        for (int i = 6; i < argc; i++) {
            if (nql_append_arg(nql, sizeof nql, argv[i]) != 0) {
                fprintf(stderr, "Error: NQL command is too long or contains a line break\n");
                return 1;
            }
        }
        return cmd_query_json(port, nql);
```

### 3c. `find` handler

Anchor:

```c
        char nql[8192] = {0};
        for (int i = 1; i < argc; i++) {
            if (i > 1) strncat(nql, " ", sizeof(nql) - strlen(nql) - 1);
            if (strchr(argv[i], ' ')) {
                strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
                strncat(nql, argv[i], sizeof(nql) - strlen(nql) - 1);
                strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
            } else {
                strncat(nql, argv[i], sizeof(nql) - strlen(nql) - 1);
            }
        }
        return cmd_query_json(port, nql);
    }

    /* count <dir> <obj> [criteria_json] — debugging shortcut for the JSON query.
```

Replace with:

```c
        char nql[8192] = {0};
        for (int i = 1; i < argc; i++) {
            if (nql_append_arg(nql, sizeof nql, argv[i]) != 0) {
                fprintf(stderr, "Error: NQL command is too long or contains a line break\n");
                return 1;
            }
        }
        return cmd_query_json(port, nql);
    }

    /* count <dir> <obj> [criteria_json] — debugging shortcut for the JSON query.
```

(The trailing context line disambiguates this occurrence from the identical-looking
loop body in 3d/3e, which are indented one level deeper — verify the anchor matches
exactly, including indentation, before editing.)

### 3d. `count` handler, NQL branch

Anchor:

```c
            char nql[8192] = {0};
            for (int i = 1; i < argc; i++) {
                if (i > 1) strncat(nql, " ", sizeof(nql) - strlen(nql) - 1);
                if (strchr(argv[i], ' ')) {
                    strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
                    strncat(nql, argv[i], sizeof(nql) - strlen(nql) - 1);
                    strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
                } else {
                    strncat(nql, argv[i], sizeof(nql) - strlen(nql) - 1);
                }
            }
            return cmd_query_json(port, nql);
        }
        const char *crit = (argc >= 5 && argv[4][0]) ? argv[4] : NULL;
```

Replace with:

```c
            char nql[8192] = {0};
            for (int i = 1; i < argc; i++) {
                if (nql_append_arg(nql, sizeof nql, argv[i]) != 0) {
                    fprintf(stderr, "Error: NQL command is too long or contains a line break\n");
                    return 1;
                }
            }
            return cmd_query_json(port, nql);
        }
        const char *crit = (argc >= 5 && argv[4][0]) ? argv[4] : NULL;
```

### 3e. `aggregate` handler, NQL branch

Anchor:

```c
        /* NQL path: aggregates arg contains '(' not '[' — e.g. sum(amount),count() */
        if (argv[4][0] != '[') {
            char nql[8192] = {0};
            for (int i = 1; i < argc; i++) {
                if (i > 1) strncat(nql, " ", sizeof(nql) - strlen(nql) - 1);
                if (strchr(argv[i], ' ')) {
                    strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
                    strncat(nql, argv[i], sizeof(nql) - strlen(nql) - 1);
                    strncat(nql, "'", sizeof(nql) - strlen(nql) - 1);
                } else {
                    strncat(nql, argv[i], sizeof(nql) - strlen(nql) - 1);
                }
            }
            return cmd_query_json(port, nql);
        }
```

Replace with:

```c
        /* NQL path: aggregates arg contains '(' not '[' — e.g. sum(amount),count() */
        if (argv[4][0] != '[') {
            char nql[8192] = {0};
            for (int i = 1; i < argc; i++) {
                if (nql_append_arg(nql, sizeof nql, argv[i]) != 0) {
                    fprintf(stderr, "Error: NQL command is too long or contains a line break\n");
                    return 1;
                }
            }
            return cmd_query_json(port, nql);
        }
```

## Task 4 (round 3, expanded) — Full regression suite

1. `rtk env SKIP_TESTS=1 ./build.sh` (final rebuild after Task 2 and Task 3).
2. Derive the complete, current set of NQL-related cases rather than trusting a
   hand-typed list (round-1's list was incomplete per review):
   `rtk ./build/bin/shard-db-test list | rtk grep -i nql`
   Paste the actual output. As of this plan's writing that set is (for reference only —
   **use the live output, not this list**, since new cases from this plan itself must
   also appear): `nql-simple-filter`, `nql-find-command`, `nql-count-command`,
   `nql-aggregate-command`, `nql-and-or`, `nql-between`, `nql-in`, `nql-parse-errors`,
   `nql-flags`, `nql-join-parse-basic`, `nql-join-parse-implicit`, `nql-join-parse-fields`,
   `nql-join-parse-left`, `nql-join-parse-multi`, `nql-join-parse-bad`, `nql-join-inner`,
   `nql-join-left`, `nql-join-implicit-alias`, `nql-join-fields`,
   `test-nql-agg-filter-flag`, `test-nql-agg-filter-override-leak`,
   `test-nql-input-validation`, `test-nql-order-by-direction`, plus every new case added
   by Task 1 above, including `test-nql-quoted-literals-real-callers`.
3. `rtk ./build/bin/shard-db-test run-all --filter nql` — paste output; every case in
   the derived list must pass, including all pre-existing ones unchanged.
4. **Revert/fail/reapply/pass proof.** Keep every Task 1 test and `build.sh` entry in
   place throughout:
   a. Temporarily reverse only Task 2a-2d and Task 3's `main.c` changes, while retaining
      Task 2e's `nql_append_arg` body so the test binary remains linkable.
   b. `rtk env SKIP_TESTS=1 ./build.sh`.
   c. Run and paste the expected failures for
      `nql-filter-embedded-quote`, `nql-command-nested-quote-escape`,
      `nql-command-double-quote-wrapper`, `nql-command-double-quote-embedded`,
      `nql-filter-unterminated-is-error`, and
      `test-nql-quoted-literals-real-callers`. Also run the three compatibility controls
      from Task 1 verification and paste that they remain green.
   d. Re-apply Task 2a-2d and Task 3 exactly, rebuild, and paste the same cases passing.
   e. Separately reverse only Task 2e's function body (leave its declaration/test), run
      `rtk env SKIP_TESTS=1 ./build.sh`, and paste the expected undefined-reference
      failure for `nql_append_arg`—the same failure mechanism proved test-first in Task
      1 Phase B.
   f. Re-apply Task 2e, rebuild, run
      `rtk ./build/bin/shard-db-test run nql-append-arg-contract`, and paste the pass.
5. `rtk ./build/bin/shard-db-test run-all` — full suite, no filter, paste output. Every
   case must pass, not just the `nql`-filtered subset — this catches any regression in
   an unrelated area from the `main.c` include/signature changes.

## Edge cases / invariants

- **No `'` or `"` in the input at all** (the overwhelming common case: `age eq 42`, bare
  identifiers, `find default users`): `nql_read_quoted` never enters its doubling
  branch; behaves identically to the old scan loop, byte for byte.
- **A single delimiter with no doubling** (today's only "working" quoted case, e.g.
  `'Alice'`): still terminates at that first delimiter exactly as before — no behavior
  change for existing single-quote-only callers.
- **`''`/`""` alone** (empty string literal, e.g. `name eq ''`): the *opening* quote is
  already consumed by the caller before `nql_read_quoted` is invoked (both call sites
  advance past the leading delimiter first), so `nql_read_quoted` sees a lone delimiter
  as its very first character with nothing after it to pair with — resolves to a
  genuine (immediate) close, decoding to an empty string, not an unterminated literal.
  Covered by Task 1i (asserted, not left to executor discretion).
- **`out_sz` overflow**: `lex_scan`'s buffer is the existing fixed `t->text[1024]`;
  `cmd_split`'s bound is however much of its `buf[8192]` remains from the start of the
  current argument to the end of the buffer. Either overflow returns -1 (hard error at
  layer 2 per D3, tolerant truncation at layer 1 per D3) rather than silently truncating
  past the caller-provided bound — no new buffer overflow risk versus the original
  bounded loops.
- **Unterminated literal at true end-of-input** (hits `\0` before ever finding an
  unescaped closing delimiter): `nql_read_quoted` still NUL-terminates whatever was
  decoded so far and advances `*pp` to the `\0`, so neither caller reads past the input
  buffer, regardless of which layer's D3 policy applies.
- **Mismatched delimiters don't close each other**: `"name eq 'Alice"` (double-quote
  wrapper containing an unclosed single quote) — the double-quote wrapper closes on the
  final `"`, handing `name eq 'Alice` to layer 2, which per D3 now hard-errors on the
  unterminated `'`. This is intentional and already covered by the general D3 layer-2
  behavior; no separate test required beyond 1g.
- **`main.c`'s `nql_append_arg` is all-or-nothing**: it preflights separator, wrapper,
  doubled delimiters, payload, and terminating NUL before writing. Empty argv elements
  encode as `""`; capacity failure or CR/LF returns `-1` with `dst` byte-for-byte
  unchanged, and every CLI caller reports an error instead of sending partial wire text.
  Task 1j covers the helper contract and Task 1k covers empty argv end-to-end.
- **This does not touch the JSON criteria path at all** — `parse_one_criterion` in
  `query_plan.c` and its `json_obj_strdup`/`json_get_fields` call sites are a completely
  separate code path (JSON wire protocol, not NQL text), covered instead by
  `docs/plans/2026-07-22-index-key-json-unescape.md`. Nothing in this plan overlaps with
  that one.

## Documentation sync (required, not conditional)

`docs/query-protocol/nql.md` is the authoritative NQL syntax document (confirmed by
inspection — it has a full grammar section) and needs updating in this same branch/PR,
per this repo's definition-of-done rule that a documented command/protocol change syncs
docs in the same PR, not as a follow-up.

### Anchor 1 — `nql.md`, add the escape-convention note to the grammar section

Anchor (exact existing text):

```
value         = single-quoted | number | boolean | bare-word

single-quoted = "'" chars "'"
number        = "-"? [0-9]+ ("." [0-9]+)?
boolean       = "true" | "false"
bare-word     = [a-zA-Z0-9_.-]+   # no spaces; use single-quoted for spaces
```

Append immediately after this block (same section):

```

A literal `'` inside a single-quoted value is written doubled: `''` decodes to one
literal `'` (SQL-style). Example: to filter on the value `O'Brien`,
write `name eq 'O''Brien'`.
```

### Anchor 2 — `nql.md`, document the top-level wrapper's own escaping and the two
delimiter choice

Anchor (exact existing text):

```
`dir` and `obj` are bare words (no spaces). `filter` is a single shell-quoted (or double-quoted)
string. When `filter` is absent, all records match — same as empty criteria `[]` in JSON.
```

Replace with:

```
`dir` and `obj` are bare words (no spaces). Quoting differs slightly by caller:

- **CLI:** pass the whole filter as one ordinary shell argument. The CLI performs the
  NQL wire-level wrapping and delimiter doubling automatically; do not pre-double its
  wrapper quotes yourself. For example:
  `./shard-db find default users "name eq 'O''Brien'"`.
- **Raw TCP:** include one NQL top-level wrapper around the filter, using either
  `'...'` or `"..."`. A literal instance of that wrapper's own delimiter inside the
  filter text is doubled: `''` inside a `'...'` wrapper, or `""` inside a `"..."`
  wrapper. Prefer the `"..."` top-level wrapper when filter values use single quotes:
  `find default users "name eq 'O''Brien'"`.

The doubled `''` in both examples belongs to the filter value itself and decodes to the
apostrophe in `O'Brien`; it is independent of shell/top-level wrapping. When `filter`
is absent, all records match — same as empty criteria `[]` in JSON.
```

### Anchor 3 (verify only) — CLI/TCP examples at `nql.md:6-11` and `overview.md:8-14`
already use the double-quoted top-level form implemented by Task 2. They are currently
broken on the raw wire because pre-fix `cmd_split` does not recognize `"` as an opener;
Task 1k and Task 2 make that documented syntax real. No edit is required at these
anchors: Task 1k is the mandatory executable verification of both raw TCP and CLI forms,
so there is no discretionary hand spot-check or conditional documentation edit left to
the executor.

### Breaking-change flag for the human (merge-time decision, not this plan's to make)

D3's layer-2 tightening (unterminated string literal → hard error) is an externally-
observable contract change. Per this repo's review checklist, call this out explicitly
in the PR description as a breaking/tightening change and let the human decide whether
it warrants a note in the next release's release notes
(`docs/release-notes/`) — this plan does not presume a specific release-notes edit,
since the next unreleased version file isn't yet cut as of this plan's writing.
