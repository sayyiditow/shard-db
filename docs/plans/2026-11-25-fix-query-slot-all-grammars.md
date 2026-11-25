# Fix plan 5 — one concurrency slot for every request grammar (AUD-CAP-1)

## Brief
**Problem and root cause.** `slot_try_acquire`/`slot_cleanup` run only in `dispatch_json_query`. NQL
(`dispatch_nql_query`: find/count/aggregate) and the legacy text path take no slot, so the documented bound
`MAX_CONCURRENT_QUERIES x QUERY_BUFFER_MB` does not hold.

**Logical flow.** `server_process_fast` takes the slot once, before choosing a grammar, and releases it on every
return path (cleanup attribute); `dispatch_json_query` stops taking its own slot (it is always entered through
`server_process_fast`; check the other callers listed below). At capacity every grammar answers
`{"error":"server at capacity","max_concurrent_queries":N}`.

**Decisions.**
| Decision | Alternatives | Why | Tag |
|---|---|---|---|
| Acquire in `server_process_fast` | acquire in each dispatcher | single point, cannot be forgotten by a new grammar | DEFAULT |
| Keep "before auth" ordering | after auth | unchanged behavior; slot is released at once on auth failure | DEFAULT |

**Blast radius.** `dispatch_json_query` callers (grep before editing: `server_process_fast`, the embedded API,
`cas_guard`/other internal callers if any must still work without a double acquire); error shape for NQL/text at
capacity is new (JSON error). Not otherwise breaking.

**Tests.** Hold all slots with blocked JSON requests (existing capacity test style), then an NQL find and a text
`get` each return `server at capacity`; after release they succeed; JSON behavior unchanged; no slot leak after auth
failures (loop 1000 bad requests, then a good one succeeds).

**Out of scope.** Per-query memory accounting changes.

**Review log.** none yet.

## Plan (executor)
Branch `fix/query-slot-all-grammars`. 1. Red tests (NQL and text at capacity). 2. Grep all callers of
`dispatch_json_query` and `dispatch_nql_query`; if any caller bypasses `server_process_fast`, keep the JSON-local
acquire for it (halt rule: write `PLAN_NOTES.md` if the call graph differs). 3. Move acquire/cleanup into
`server_process_fast` with the `__attribute__((cleanup))` pattern; make `dispatch_json_query` not re-acquire when
entered from it (flag parameter or static wrapper). 4. Full suite; revert-prove; changelog; uncommitted.
