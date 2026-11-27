# Plan: AUD-QRY-22 — wire shape of a find that fails before any row is
# emitted

**Status: PROPOSED — a design ruling, not a defect. Written 2026-11-24 at
the round-4 fix work, after a wider change was built, found to contradict
the documented contract, and reverted in the same session. Severity:
minor. No product code changes until the human picks an option.**

## The question

`cmd_find_do_impl` writes the response envelope prefix first (`[`,
`{"rows":[`, `{"columns":[…],"rows":[`, …) and only then runs the chosen
route. When the route fails **before the first row** — a KeySet build, a
candidate collect, a join lookup or an iterator open that hits a storage
error — what should the client receive?

Today the answer depends on the route, not on the failure:

| Route / exit | Wire shape on a pre-row failure | Since |
|---|---|---|
| 17 bare `return -1` exits of `cmd_find_do_impl` (rows / dict / default single-leaf, OR union, composite-prefix, order-index walk, fetch-sort, streaming, trigram, full scan, totals) | unterminated: the prefix alone (`{"columns":[…],"rows":[`, `[`) | always |
| `rc == -2` budget, OR-union budget | closed bare envelope, then `QUERY_BUFFER_ERR` | `main` |
| ordered-fallback / small-prefilter collect errors | closed bare envelope, then the error object | `main` |
| AND-intersect **build** failure | closed bare envelope, then the error object | round-4 M2 |
| composite-exact walk | closed bare envelope, then the error object | round-4 M3 |

All six shapes are fail-closed — none can pass for a complete page. The
inconsistency is the finding.

## What the documentation says

`docs/query-protocol/find.md`:

- "Bitmap-source errors and partial-result semantics": bitmap sources are
  prepared *before* the response starts, "so a bitmap read failure …
  yields one complete `{"error":"bitmap index unavailable"}` object before
  any response prefix", and "a hard total error is an error response, not
  `"total":null`". Then: "This is a pre-emission boundary, not full output
  atomicity: record or serialization errors after rows have been emitted
  keep their existing behavior."
- "Unordered indexed pages": allocation failures, a missing storage
  handle and join failures "fail the query instead of returning a shorter
  page, and, like other record errors after the response has started,
  leave the response unterminated."

So the document states two rules and a deliberate reason: a failure after
the response has started is unterminated because a closed short page can
pass for success and a truncated one cannot. It does not address a failure
after the *prefix* but before any *row*. `test_kef-trigram-starts-error`
pins the unterminated shape for one mid-stream route.

The round-4 M2 and M3 items directed the closing shape for two pre-row
exits (the AND-intersect build failure and the composite-exact walk).
That is where the inconsistency entered.

## Evidence

`test-idx-absent-shard` Part C (31 request shapes, walker seam armed on
the shard holding the record). Pre-row failures that return a bare
prefix: the five rows-format finds, both join shapes and the default
format OR find. Diag lines in that case name the class for every shape.

Two facts about clients, both verifiable from those diags:

- a client that parses the whole response as JSON cannot tell a pre-row
  failure from a mid-stream cut — both are a parse error, with no reason;
- a client that streams the prefix cannot tell an error from a slow page
  until the connection frame ends.

## Options

**(a) Keep as documented — close as `wontfix`.** No code. One sentence in
find.md naming the closing exits as deliberate exceptions, so the next
reader does not re-derive this question.

**(b) One rule for every route:** while nothing but the envelope prefix
is on the wire, close it and append the error object (budget →
`QUERY_BUFFER_ERR`, anything else names its errno); once a row is out,
leave the response unterminated. This is the rule the round-4 M2/M3 exits
already follow and the rule the bitmap design states in the same
document.

## Recommendation

(b). A pre-row failure is the one case where the server still knows the
page is empty and still has a reason to report; throwing both away buys
nothing, and three routes already do it. The reason the document gives
for unterminated — a closed short page passing for success — does not
apply to a page with zero rows followed by an error object.

The cost is honest: a client that reads the first complete JSON document
and ignores trailing bytes would read that empty page as success. Such a
client already misreads the budget exits, the bitmap errors and the two
round-4 exits, so (b) widens an existing hazard rather than creating one;
if it matters, it is a client-contract note for find.md, not a reason to
keep three shapes.

## If (b) is chosen — what the work is

1. **An exact "rows emitted" signal per executor.** The reverted sweep
   closed unconditionally and was caught by `test-kef-trigram-starts-error`
   and by the two mid-stream cases. The signal must come from the
   executor, not from a byte count: the response stream is a socket
   (`fdopen`) or a TLS cookie stream, so `ftell` is unavailable, and
   several emitters run on IO-pool workers, where thread-local state does
   not reach the request thread. `t_find_rows_emitted` (query.c) is the
   pattern — the executor stores its emitted-row count on the request
   thread before its `-1` return. It is set today in `keyset_emit_find`
   and `find_via_composite_key` only; (b) needs it in
   `idx_find_streaming`, `find_via_composite_prefix`,
   `find_via_order_index_walk`, `find_via_trigram_starts_with`,
   `prepared_bitmap_find_stream` and `find_via_fetch_sort`, plus the
   inline contexts (`cc.printed`, `rctx.printed`, the ordered fallback).
   An executor that does not report must read as "started" (unterminated),
   never as "safe to close".
2. **Route every bare exit through the close** once the signal says zero
   rows. Keep the mid-stream exits exactly as they are.
3. **Tests.** `test-kef-trigram-starts-error`, case 10 of the same file
   and Part D of `test-idx-absent-shard` must stay green unchanged — they
   are the mid-stream half. Tighten Part C to reject an unterminated
   response that carries zero rows.
4. **Docs.** find.md: replace the two quoted sentences with the single
   rule and keep the reason for the mid-stream half. Reference in the
   `QUERY_BUFFER_MB` paragraph of AGENTS.md stays (it describes a
   mid-stream failure).

Gates: test-first red on the current tree for at least the rows-format
and join shapes; full suite `--jobs 8`; sanitizers once at hand-back (the
diff touches no locks or shared state).

## If (a) is chosen

Close AUD-QRY-22 as `wontfix`; add the find.md sentence; no branch.
