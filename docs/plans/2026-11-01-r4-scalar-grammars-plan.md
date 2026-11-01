# R4 — strict scalar grammars (bool / calendar / UUID / IP / enum / string)

Status: **revision 2 (after plan-blind review) — no execution before the human approves §2 (D1–D7 decided (D7: varchar NUL unchanged, audit deferred to R7)) and names the executor.** Review fixes folded in: empty enum / legacy `""` route (B1), indexed bool NEQ in `query.c` (B2), packed varchar NUL route (B3), task order (M1), data-derived index probes (M2, approved: strict reject ⇒ "no match" for probes), brace-UUID vs computed-update gate (M3, approved: exempt when the value is a valid UUID), operator scope (M4), class-E list (M5), exact anchors (M6).
Base: `fix/type-correctness-recovery` at `b32bc23` (R3 committed locally). Branch: continue on it.
Scope owner: recovery plan row R4 / Phase 7 (`docs/plans/2026-10-19-type-correctness-recovery-plan.md`, "Task 0.2 — Freeze lexical and numerical contracts", T10–T14, T18). R1–R3 are done; R5 (metadata/migration/recovery) and R6 (activation, docs/clients, **sanitizers**) are out of scope. **No ASan/UBSan/TSan and no benchmarks in this plan** — the human runs sanitizers once, in R6.

## 1. Problem and root causes

Malformed non-NULL input becomes a valid-looking different value, or NULL, on write and query routes.

| ID | Mechanism (root cause) | Observed effect |
|---|---|---|
| T10 | `encode_field_len` `case FT_BOOL` tests only `val[0]` ∈ {t,T,1}; query compile (`query_plan.c` `case FT_BOOL`, IN-list branch) repeats the first-byte test | `"trash"`, `"10"`, `"yes"`, `"on"` store/compare as true/false by first byte (`"yes"` → false!) |
| T11 | `encode_field_len` `FT_DATE/DATETIME/DATETIMEMS` copy only digit characters (separators/suffix/timezone discarded), zero-pad short datetime/ms on the right, never range-check; `query_plan.c` `parse_date_i32/parse_datetime/parse_datetimems` do the same | `2024-02-30`, `20241399`, `2024-01-05garbage`, `2024-01-05T25:61:61Z` store as unrelated valid or out-of-range values; `20240105` as a datetime becomes midnight |
| T12 | `encode_field_len` `FT_TIME` and `query_plan.c parse_time` encode malformed input as `00:00:00` | invalid time == midnight |
| T13 | `encode_field_len` `FT_UUID` skips every `-` anywhere and stops at 32 digits (trailing text ignored); `FT_IPV4/6` truncate to a stack buffer then `inet_pton`; failure zero-fills; `encode_json_field_checked`/`typed_row.c` then convert a malformed uuid/ip to **NULL** via `sentinel_span_parses` | `uuid`+`"…garbage"` accepted; 40-char "ip" truncated into a different valid address; malformed → silent NULL (or zero) |
| T14 | `FT_ENUM` unknown member: `encode_field_len` zero-fills (index 0 = first member); only some routes pre-check (`typed_row.c` does; `encode_field_checked` does not) | unknown enum silently becomes member 0 on routes that skip the pre-check |
| T18-adjacent | varchar: embedded NUL accepted; bounds enforced only in `encode_field_checked` | NUL-containing varchar content (R3 D6 left this policy to R4) |

**Choke point (verified):** every checked write and every R2 query literal funnels through `encode_field_checked` (`config.c`): `encode_json_field_checked` → `encode_field_checked` → `encode_field_len` (unchecked). R2's `predicate_validate_value` (`query_plan.c`) already calls `encode_json_field_checked` for every non-varchar/enum/numeric criterion literal. So one strict converter set inserted into `encode_field_checked` fixes writes and query-literal *validation* together. The remaining weakness on the query side is the second, lenient parse in `compile_criterion` (the static `parse_*` helpers) that runs *after* validation — it must use the same grammar, otherwise a literal that validates (e.g. a brace-wrapped UUID or ISO date) is mis-compiled.

## 2. Decisions needed from the human (do not pick silently)

| # | Decision | Recommendation |
|---|---|---|
| D1 | **Compatibility break accepted?** Strict rejection changes behaviour for clients that send lenient forms (`2024-1-5`, `"yes"`→false, trailing junk, date-only datetime). Needs release-note + "breaking" flag. | Accept: it is the stated goal (T10–T13). CHANGELOG entry titled as a breaking change; no migration (stored bytes unchanged). |
| D2 | **Date-only / short input to datetime and datetimems** (currently zero-padded: `20240105` → midnight). Contract lists only full forms. | Reject (contract). Clients wanting midnight send `20240105000000`. |
| D3 | **Empty string** for bool/date/datetime/datetimems/time/uuid/ipv4/ipv6/enum. Contract: invalid on JSON `""` and query literals; explicit JSON `null` stays NULL. Open: bulk **CSV/delimited empty cells** today mean "missing/NULL" on some routes. | JSON `""` and criteria: error. Delimited/CSV empty cell: keep its current meaning (Task 0 records it per route); the call site tests emptiness *before* calling the converter. |
| D4 | **Enum unknown member in a query literal** (`eq`, `neq`, `in`, `not_in`): today returns no rows. PostgreSQL errors. | Error (parity with writes; message names the field). Flag in CHANGELOG. |
| D5 | **Leading/trailing ASCII whitespace** on the new scalar types: contract Task 0.2 says permit consistently. | Permit outer ASCII whitespace for bool/date/datetime/datetimems/time/uuid/ip; not for varchar/enum (byte-exact, unchanged). |
| D6 | **`on`/`of`/`off` and one-letter bool prefixes.** Contract spellings: true `1 t tr tru true y ye yes on`; false `0 f fa fal fals false n no of off`; `o` is ambiguous → error. | As in the contract. |

| D7 | **Varchar embedded NUL — scope finding (planner audit, 2026-11-01, live scratch daemon + source).** The *public* object layout is legacy fixed-width and **C-string based end to end**: field values travel as `char*` (`json_get_typed_fields` → `field_vals[i]`, `strlen` in `storage.c`/`query_bulk.c`), criteria values are `char*` (`in_values[i]`, `strlen`), and `json_unescape_cstring` **deliberately rejects** any decoded NUL ("malformed JSON escape in value for field 's'" on single insert, verified live). The *packed* layout (R1, not publicly activatable until R6) is already NUL-exact on write/read/CSV (tests: `test_packed_csv`, `test_typed_bulk_insert`, `test_single_write_integration`, `test_r1_routes`). Making the whole public DB NUL-transparent means converting the legacy write, criteria, index-key, aggregate/group, join and CSV paths to length-carrying text — a separate project, not a scalar-grammar change. | **Recommended: R4 does not change varchar NUL behaviour.** Legacy public routes keep rejecting escaped NUL (documented); packed keeps its exact behaviour; a database-wide NUL audit is deferred to a new R7 after R6 activation (decided by the human). |

Everything else (accepted spellings, forms, ranges) is frozen by the recovery plan's Task 0.2 and is restated, not re-decided, in §3.

## 3. Frozen grammar (restated from Task 0.2; outer ASCII whitespace stripped first, case-insensitive only for bool and hex)

- **bool**: exactly the spellings in D6, case-insensitive. Anything else (`o`, `10`, `trash`, embedded NUL) is an error.
- **date**: `YYYYMMDD` or `YYYY-MM-DD`; year 1..9999; real Gregorian day limits (leap year: ÷4, not ÷100 unless ÷400).
- **datetime**: `YYYYMMDDHHMMSS`, `YYYY-MM-DD HH:MM:SS`, `YYYY-MM-DDTHH:MM:SS`; HH 0..23, MM 0..59, SS 0..59 (no leap second).
- **datetimems**: `YYYYMMDDHHMMSSfff`, `YYYY-MM-DD HH:MM:SS.fff`, `YYYY-MM-DDTHH:MM:SS.fff`; exactly 3 fraction digits.
- **time**: exactly `HH:MM:SS`, same ranges.
- **uuid**: 32 hex digits, or 36-char 8-4-4-4-12 dashed; one matching `{…}` pair may enclose either. No other dash positions, no trailing text.
- **ipv4 / ipv6**: whole input must be accepted by `inet_pton`; length checked **before** copying (v4 ≤ 15, v6 ≤ 45); embedded NUL rejected. Valid zeros (`0.0.0.0`, `::`) and the nil UUID / midnight stay real, non-NULL values.
- **enum**: member must exist (byte-exact); unknown member is an error on every route; empty string is an error (not member 0).
- **varchar**: bounds unchanged (bytes); NUL policy is unchanged by R4 (D7): legacy public routes reject decoded NUL, packed is NUL-exact. Scalar types reject NUL.
- **Operator scope of query-literal validation**: strict grammar validation applies to the comparison ops only (`eq neq lt gt lte gte between in not_in`). Pattern/substring ops (`starts ends contains like` and i-variants, regex, length, `exists`, `*_field`) are not value-validated for these types (unchanged: they operate on rendered text).
- **Whitespace set** is the same 4 characters as the existing numeric `is_ascii_space` (space, `\t`, `\n`, `\r`), everywhere.
- **timestamp** stays signed Unix ms (untouched). Stored bytes and layouts are **unchanged** — no migration.

## 4. Consumer census (verified at `b32bc23`)

Strict converters go in `encode_field_checked`, so these call sites inherit the new behaviour. **Each must be confirmed by the executor against D3 (empty cells) in Task 0**:

| Entry point | Sites |
|---|---|
| `encode_field_checked` callers | `query_bulk.c` (4: lines near `encode_field_checked(&ts->fields[i], vals[i].ptr`, `…[fi]…`, `cellbuf`, `json_rec->field_values[i]`), `query_find.c` (1, default backfill), `schema_wire.c` (1, default validation), `storage.c` (1, field_vals), `typed_index.c` (1, key encode), `config.c` (self: `encode_json_field_checked`, `typed_encode*`) |
| `encode_json_field_checked` callers | `typed_row.c` (`row_encode_fixed`), `query_plan.c` (`predicate_validate_value`), `config.c` |
| `sentinel_span_parses` (to be deleted) | `config.c` two call sites in the JSON encoders (`if (!sentinel_span_parses(ts->fields[i].type, ev, el))` ×2), `typed_row.c` (1), decl in `typed_row.h` |
| Lenient compile parsers (to be replaced by the grammar module) | `query_plan.c`: `parse_date_i32`, `parse_datetime`, `parse_datetimems`, `parse_uuid`, `parse_ipv4`, `parse_ipv6`, `parse_time`, bool first-byte tests (2 places) |
| Routes that bypass `encode_field_checked` (found by review; each gets an explicit hunk) | `config.c` `typed_encode_defaults` and `typed_encode` wrap the checked call in `if (el > 0) {` ⇒ JSON `""` never reaches the converter on legacy-layout objects (also called from `query_bulk.c`; hunk 2c). `typed_row.c` `typed_prepared_set` `PKR_VARCHAR` branch does `memcpy` with no NUL check (hunk 2d). `query.c` ×2 `int is_true = (search_criterion_value(pc)[0] == 't' …` in the indexed `OP_NOT_EQUAL` bool shortcut (btree dispatch and chunk walker) (Task 4). |
| Computed-update `{` gate (brace-wrapped UUID) | `storage.c` `if (f->type != FT_VARCHAR && field_vals[i][0] == '{') return -2;`; `query_bulk.c` `if (w->ts->fields[tidx].type != FT_VARCHAR && *cell_start == '{')` (CSV cell) and `json_rec->field_values[i][0] == '{'` (hunk 2e). |
| `typed_index_text_key_into` callers (probe keys; return 1 = key, 0 = no match, −1 = error) | `query.c` lines calling it at: helper wrappers (`return typed_index_text_key_into(ts,(size_t)index,text,len,out,cap,out_len);`), composite order bound, cursor bound (`texts[side]`), `m->schema` index probes, `c->in_values[i]` index key building, `ospec.parts[i].idx` order-by probes; `query_join.c` (`j->remote_fs.ts`, 2 calls). Join/cursor/order-by feed **data-derived** text (a driver varchar joined to a remote bool/date/uuid; legacy stored zero/invalid dates). Client literals are already rejected earlier by `predicate_validate`. ⇒ strict rejection here must mean "no match" (return 0), never an error (hunk 2f). |
| Lenient on purpose | `config.c encode_field_text_for_index` (index build/sync from **stored** text; keeps old data readable) — named so nobody "fixes" it. |
| Unchecked `encode_field` callers (stay) | `storage.c` ×2 (legacy auto_create/auto_update stamps), `query_find.c` ×3 (seq/uuid/random default backfill). **Their inputs are server-generated and in-grammar** (`auto_now_str`, `%04d%02d%02d…`, canonical dashed UUID, hex); Task 5 asserts that. Note the live **packed** stamp path (`typed_prepared_stamp` → `typed_prepared_set`) is already strict. |
| Known gap, not fixed here | composite criteria (`a+b`) return early in `compile_one_checked` before `predicate_validate`; malformed literals there are not rejected (pre-existing). `typed_json_string_type()` unescapes only some types on the legacy route (pre-existing, recorded by Task 0). |
| Also check, not expected to change | `server.c`/`query_bulk.c`/`query_join.c`/`typed_bulk_insert.c`/`packed_csv_command.c`/`typed_read_command.c` use `parse_uuid_string` (auto-key/key normalisation — strict already, leave); `nql.c` literal handoff (verify it passes strings through untouched); CLI `src/cli/conn.c` (`inet_pton` for connection only). |

## 5. Execution rules

- Branch: current `fix/type-correctness-recovery`; leave **all work uncommitted** for raw-diff review (AGENTS.md standing exception).
- Build `SKIP_TESTS=1 ./build.sh`; test `./build/bin/shard-db-test run <name>` (targeted cases after each task; the **suite is expected red between Tasks 2 and 6** because lenient-form fixtures are class-E and are fixed in Task 6); full `./build/bin/shard-db-test run-all` fresh at the end of Task 6 and Task 7 only (◆). Zero warnings. **No sanitizers.**
- Every task is test-first: write the test, run it RED against the current tree and paste the failure, implement, paste GREEN. For Tasks 1, 3, 4, 5: also revert the implementation, show RED again, restore.
- Anchors are quoted text (never line numbers). If an anchor is not found exactly once: write `PLAN_NOTES.md` describing the mismatch and **halt the whole run**.
- Existing tests that use lenient forms will legitimately change (class E: expectation updated to the strict result). Each such edit is listed in the hand-back with the old/new value; any other failure is a stop-and-report.
- Decision D1–D6 not yet approved ⇒ do not start.
- Housekeeping before ◆ runs: `rm -rf /tmp/shard-agg-packed-* /tmp/shard-db-test-*` leftovers (RAM-backed `/tmp` pressure caused spurious failures in R3).

## 6. Tasks

### Task 0 — Characterization matrix (diagnostic, no production change)

Create `src/test/cases/test_scalar_grammar_routes.c` (daemon-per-case style; copy `test_agg_empty_avg_order.c`'s fixture pattern). One object `sg` with fields `b:bool, d:date, dt:datetime, dm:datetimems, t:time, u:uuid, i4:ipv4, i6:ipv6, e:enum(red,green,blue), s:varchar:8`, all nullable. Route list per type: direct `insert`, `update`, `bulk-insert` (JSON), `bulk-insert` delimited/CSV, `bulk-update`, `default` clause (`add-field … :default=`), `find` criterion `eq`, `in`, `between`, `get` unaffected. For every (type × route × input class) record what the tree does today (`accepted`, `stored-as-NULL`, `stored-as-zero`, `error`) in a table inside the test as a comment and as assertions that **pass on the current tree** (characterization). Input classes: valid; invalid-calendar (`2024-02-30`); trailing junk; wrong separators; empty string; whitespace-only; embedded NUL (`\u0000`); over-long ip; bool `yes`/`trash`; unknown enum.

Register the file in `build.sh` (anchor: the line `src/test/cases/test_scalar_csv_update.c \` — insert `src/test/cases/test_scalar_grammar_routes.c \` directly after it; verify the anchor occurs exactly once, else halt). Also record per route: legacy-layout object (non-packed) **and** packed object (create `sg` twice), pattern ops (`starts`, `contains`, `like`) on each new type, indexed vs full-scan `neq` on bool, join of a varchar driver to a remote bool/date/uuid, cursor/order_by on a date with a stored zero date, default-clause literals (`default=yes`, `default=2024-1-5`, empty `default=`), and `\u0000` in varchar on both layouts.

**Varchar NUL (D7): no R4 work.** Do not add or remove any NUL handling for varchar. Task 0 only records, in the route table, whether each route accepts `a\u0000b` today (informational).

**Class-E census (deliverable, before Task 2):** run `grep -ln` over `src/test/cases/*.c` for lenient forms (non-ISO dates, short datetimes, bool words, `not-an-ip`/`999.` ips, malformed uuids, `purple`-style unknown enums, `\u0000`, empty-string values) and paste the list of every test expected to change with its old/new expectation. Known so far (not exhaustive): `test_ipv4.c`, `test_ipv6.c` (insert `"not-an-ip"` expects inserted/NULL), `test_enum.c` (`eq "purple"` expects 0 ⇒ error, D4), `test_not_null_writes.c` (`nope`/`bad`/`999.999.1.1` now error on nullable fields, message text changes), `test_packed_json_projection.c`, `test_packed_named_projection.c` (store `\u0000`), plus `\u0000` users to classify: `test_index_varchar_unescape.c`, `test_packed_csv.c`, `test_packed_csv_commands.c`, `test_r1_routes.c`, `test_scalar_csv_update.c`, `test_scalar_routes_checked.c`, `test_single_write_integration.c`, `test_typed_bulk_insert.c`.

Deliverable: the characterization test (green on base) plus the filled table pasted in the hand-back. **Stop and ask the human** if the delimited/CSV empty-cell row contradicts D3's assumption.

### Task 1 — Grammar module `scalar_grammar.[ch]` (pure functions)

Test first: `src/test/cases/test_scalar_grammar_core.c` (no daemon; links like `test_decnum_core.c`). Register in `build.sh` (anchor: `src/test/cases/test_decnum_core.c \`) after that line. Tables (valid and invalid, each asserted both ways with exact outputs): bool all 19 spellings × {lower, UPPER, Mixed}, plus `o`, `10`, `tru e`, `t\0x`, ``, `  `, and `\vtrue` / `true\f` (vertical-tab/form-feed are NOT whitespace) ; date valid `20240229`, `2000-02-29`, `0001-01-01`, `9999-12-31`; invalid `19000229`, `2023-02-29`, `20241301`, `20240100`, `2024-1-5`, `20240105x`, `0000-01-01`, `2024/01/05`; datetime/datetimems/time valid and the range limits (`23:59:59`, `.999`) and invalid (`24:00:00`, `12:60:00`, `12:00:60`, `.12`, `.1234`, `20240105`); uuid 32-hex, dashed, braced, upper-case, nil; invalid dashes at wrong positions, 31/33 digits, trailing text, mismatched braces; ipv4 `0.0.0.0`, `255.255.255.255` valid / `256.1.1.1`, `1.2.3`, `1.2.3.4.5`, 16-char input, NUL; ipv6 `::`, `::1`, `2001:db8::1` valid / 46-char input, `:::`, NUL. RED = link failure (functions missing).

Create `src/db/scalar_grammar.h`:
```c
#ifndef SCALAR_GRAMMAR_H
#define SCALAR_GRAMMAR_H
#include <stddef.h>
#include <stdint.h>
/* Strict, length-aware scalar grammars (R4). Each returns 0 and fills the
   output, or -1 leaving outputs untouched. Outer ASCII whitespace is
   stripped first; embedded NUL is always invalid. */
int sg_bool(const char *s, size_t n, uint8_t *out);
int sg_date(const char *s, size_t n, int32_t *yyyymmdd);
int sg_datetime(const char *s, size_t n, int32_t *yyyymmdd, uint32_t *secs);
int sg_datetimems(const char *s, size_t n, int32_t *yyyymmdd, uint32_t *ms);
int sg_time(const char *s, size_t n, uint32_t *secs);
int sg_uuid(const char *s, size_t n, uint8_t out[16]);
int sg_ipv4(const char *s, size_t n, uint8_t out[4]);
int sg_ipv6(const char *s, size_t n, uint8_t out[16]);
#endif
```
Create `src/db/scalar_grammar.c`:
```c
#include "scalar_grammar.h"
#include <arpa/inet.h>
#include <string.h>

static int sp(char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; }
static int strip(const char **s, size_t *n) {
    while (*n > 0 && sp((*s)[0])) { (*s)++; (*n)--; }
    while (*n > 0 && sp((*s)[*n - 1])) (*n)--;
    return *n > 0 && !memchr(*s, '\0', *n);
}
static int dg(char c) { return c >= '0' && c <= '9'; }
static int digs(const char *s, size_t i, size_t w) {
    for (size_t k = 0; k < w; k++) if (!dg(s[i + k])) return 0;
    return 1;
}
static int num(const char *s, size_t i, size_t w) {
    int v = 0;
    for (size_t k = 0; k < w; k++) v = v * 10 + (s[i + k] - '0');
    return v;
}
static int leap(int y) { return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0; }
static int ymd_ok(int y, int m, int d) {
    static const int dm[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (y < 1 || y > 9999 || m < 1 || m > 12 || d < 1) return 0;
    return d <= dm[m - 1] + (m == 2 && leap(y));
}
static int hms_ok(int h, int m, int s) { return h <= 23 && m <= 59 && s <= 59; }

int sg_bool(const char *s, size_t n, uint8_t *out) {
    static const char *const T[] = {"1", "t", "tr", "tru", "true", "y", "ye", "yes", "on"};
    static const char *const F[] = {"0", "f", "fa", "fal", "fals", "false", "n", "no", "of", "off"};
    char b[8];
    if (!strip(&s, &n) || n >= sizeof b) return -1;
    for (size_t i = 0; i < n; i++) b[i] = (s[i] >= 'A' && s[i] <= 'Z') ? (char)(s[i] + 32) : s[i];
    b[n] = '\0';
    for (size_t i = 0; i < sizeof T / sizeof *T; i++) if (!strcmp(b, T[i])) { *out = 1; return 0; }
    for (size_t i = 0; i < sizeof F / sizeof *F; i++) if (!strcmp(b, F[i])) { *out = 0; return 0; }
    return -1;
}

/* date part at s[0..]: compact (8) when iso==0, dashed (10) when iso==1. */
static int ymd_at(const char *s, int iso, int *y, int *m, int *d) {
    if (iso) {
        if (s[4] != '-' || s[7] != '-' || !digs(s, 0, 4) || !digs(s, 5, 2) || !digs(s, 8, 2)) return -1;
        *y = num(s, 0, 4); *m = num(s, 5, 2); *d = num(s, 8, 2);
    } else {
        if (!digs(s, 0, 8)) return -1;
        *y = num(s, 0, 4); *m = num(s, 4, 2); *d = num(s, 6, 2);
    }
    return ymd_ok(*y, *m, *d) ? 0 : -1;
}
/* HH:MM:SS at s[0..7] */
static int hms_at(const char *s, int *h, int *m, int *sec) {
    if (s[2] != ':' || s[5] != ':' || !digs(s, 0, 2) || !digs(s, 3, 2) || !digs(s, 6, 2)) return -1;
    *h = num(s, 0, 2); *m = num(s, 3, 2); *sec = num(s, 6, 2);
    return hms_ok(*h, *m, *sec) ? 0 : -1;
}

int sg_date(const char *s, size_t n, int32_t *out) {
    int y, m, d;
    if (!strip(&s, &n) || (n != 8 && n != 10)) return -1;
    if (ymd_at(s, n == 10, &y, &m, &d)) return -1;
    *out = y * 10000 + m * 100 + d;
    return 0;
}
int sg_datetime(const char *s, size_t n, int32_t *date, uint32_t *secs) {
    int y, m, d, h, mi, sec;
    if (!strip(&s, &n) || (n != 14 && n != 19)) return -1;
    if (n == 14) {
        if (ymd_at(s, 0, &y, &m, &d) || !digs(s, 8, 6)) return -1;
        h = num(s, 8, 2); mi = num(s, 10, 2); sec = num(s, 12, 2);
        if (!hms_ok(h, mi, sec)) return -1;
    } else {
        if ((s[10] != ' ' && s[10] != 'T') || ymd_at(s, 1, &y, &m, &d) || hms_at(s + 11, &h, &mi, &sec)) return -1;
    }
    *date = y * 10000 + m * 100 + d;
    *secs = (uint32_t)(h * 3600 + mi * 60 + sec);
    return 0;
}
int sg_datetimems(const char *s, size_t n, int32_t *date, uint32_t *ms) {
    int y, m, d, h, mi, sec, f;
    if (!strip(&s, &n) || (n != 17 && n != 23)) return -1;
    if (n == 17) {
        if (ymd_at(s, 0, &y, &m, &d) || !digs(s, 8, 9)) return -1;
        h = num(s, 8, 2); mi = num(s, 10, 2); sec = num(s, 12, 2); f = num(s, 14, 3);
        if (!hms_ok(h, mi, sec)) return -1;
    } else {
        if ((s[10] != ' ' && s[10] != 'T') || s[19] != '.' || !digs(s, 20, 3) ||
            ymd_at(s, 1, &y, &m, &d) || hms_at(s + 11, &h, &mi, &sec)) return -1;
        f = num(s, 20, 3);
    }
    *date = y * 10000 + m * 100 + d;
    *ms = (uint32_t)((h * 3600 + mi * 60 + sec) * 1000 + f);
    return 0;
}
int sg_time(const char *s, size_t n, uint32_t *secs) {
    int h, m, sec;
    if (!strip(&s, &n) || n != 8 || hms_at(s, &h, &m, &sec)) return -1;
    *secs = (uint32_t)(h * 3600 + m * 60 + sec);
    return 0;
}
static int hexv(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}
int sg_uuid(const char *s, size_t n, uint8_t out[16]) {
    uint8_t buf[16];
    int pos = 0;
    if (!strip(&s, &n)) return -1;
    if (n >= 2 && s[0] == '{' && s[n - 1] == '}') { s++; n -= 2; }
    if (n != 32 && n != 36) return -1;
    for (size_t i = 0; i < n; i++) {
        if (n == 36 && (i == 8 || i == 13 || i == 18 || i == 23)) {
            if (s[i] != '-') return -1;
            continue;
        }
        int v = hexv(s[i]);
        if (v < 0) return -1;
        if (pos % 2 == 0) buf[pos / 2] = (uint8_t)(v << 4); else buf[pos / 2] |= (uint8_t)v;
        pos++;
    }
    if (pos != 32) return -1;
    memcpy(out, buf, 16);
    return 0;
}
int sg_ipv4(const char *s, size_t n, uint8_t out[4]) {
    char b[16];
    uint8_t t[4];
    if (!strip(&s, &n) || n > 15) return -1;
    memcpy(b, s, n); b[n] = '\0';
    if (inet_pton(AF_INET, b, t) != 1) return -1;
    memcpy(out, t, 4);
    return 0;
}
int sg_ipv6(const char *s, size_t n, uint8_t out[16]) {
    char b[46];
    uint8_t t[16];
    if (!strip(&s, &n) || n > 45) return -1;
    memcpy(b, s, n); b[n] = '\0';
    if (inet_pton(AF_INET6, b, t) != 1) return -1;
    memcpy(out, t, 16);
    return 0;
}
```
Register `src/db/scalar_grammar.c` in `build.sh` next to `src/db/decnum.c` on **every** link line (daemon, `LIB_SRCS`, test-server, test, bench — exactly where R3 added `src/db/agg_exact.c`; anchor each `src/db/decimal_order.c src/db/agg_exact.c` → `src/db/decimal_order.c src/db/agg_exact.c src/db/scalar_grammar.c`). `grep -c scalar_grammar.c build.sh` must equal the count of `agg_exact.c` occurrences in link lines.

### Task 2 — Checked converters in `encode_field_checked` (+ the routes that bypass it)

**Tasks 2 and 3 are one RED/GREEN unit** (the sentinel downgrade turns malformed uuid/ip into NULL before the converter is reached, so Task 2's uuid/ip assertions cannot pass until Task 3). Write both tests first (RED on base), implement hunks 2a–2f then Task 3, and run GREEN only at the end of Task 3. Revert-proof both: `git stash` the production hunks, show RED, restore, show GREEN, paste both.

Test first (extend Task 0's file into the RED set): for each type, through `insert`/`update`/`bulk-insert`(JSON)/default: the invalid classes from Task 0 now return `{"error":"value for field '<f>' is not a valid <type>: …"}` (exact field-named message asserted), the stored record is untouched on update failure, and each valid grammar form stores the same bytes as before for forms the old code handled correctly (`20240105`, `2024-01-05`, `12:30:45`, canonical UUID, `10.0.0.1`, `::1`, `true`/`false`/`1`/`0`).

In `src/db/config.c` add `#include "scalar_grammar.h"` beside the existing includes (anchor: the `#include "types.h"` line — verify exactly-once; otherwise use the first `#include "` line). In `encode_field_checked`, anchor:
```c
    switch (f->type) {
    case FT_SHORT: case FT_INT: case FT_LONG: case FT_TIMESTAMP:
    case FT_BYTE: case FT_NUMERIC: case FT_DOUBLE: case FT_FLOAT:
        while (vlen > 0 && is_ascii_space(val[0])) { val++; vlen--; }
```
leave unchanged (the new `sg_*` strip internally). Then replace the block:
```c
    if (vlen == 0) {
        /* Preserve the historic zero-filled representation for empty/NULL
           spans. */
        memset(out, 0, f->size);
        return 0;
    }
```
with:
```c
    if (vlen == 0) {
        switch (f->type) {
        case FT_BOOL: case FT_DATE: case FT_DATETIME: case FT_DATETIMEMS:
        case FT_TIME: case FT_UUID: case FT_IPV4: case FT_IPV6:
            /* R4: an empty span is not a value of these types; NULL is
               expressed by the caller (JSON null / omitted / delimited
               empty cell), never by a zero-length string reaching here. */
            fmt_conv_err(err, errsz, f, "empty value");
            return -1;
        case FT_ENUM:
            /* Empty is invalid unless it is an explicit declared member. */
            if (enum_value_index(f, val, 0) < 0) {
                fmt_conv_err(err, errsz, f, "unknown enum member");
                return -1;
            }
            break;
        default:
            break;
        }
        /* Historic zero-filled representation for the remaining types. */
        memset(out, 0, f->size);
        return 0;
    }
```
(Only after the D3-confirmed audit of the call sites in Task 0: any site that legitimately passes a zero-length span for "missing" must test `len == 0` itself before calling — add that guard in the same task and name the site in the hand-back.)

Then, in the second `switch (f->type)`, before `default:`, add:
```c
    case FT_BOOL: {
        uint8_t b;
        if (sg_bool(val, vlen, &b)) {
            fmt_conv_err(err, errsz, f, "expected true/false (t, f, yes, no, on, off, 1, 0)");
            return -1;
        }
        out[0] = b;
        return 0;
    }
    case FT_DATE: {
        int32_t d;
        if (sg_date(val, vlen, &d)) {
            fmt_conv_err(err, errsz, f, "expected YYYYMMDD or YYYY-MM-DD (a real calendar date)");
            return -1;
        }
        out[0] = (uint8_t)(d >> 24); out[1] = (uint8_t)(d >> 16);
        out[2] = (uint8_t)(d >> 8);  out[3] = (uint8_t)d;
        return 0;
    }
    case FT_DATETIME: {
        int32_t d; uint32_t t;
        if (sg_datetime(val, vlen, &d, &t)) {
            fmt_conv_err(err, errsz, f, "expected YYYYMMDDHHMMSS, 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS'");
            return -1;
        }
        out[0] = (uint8_t)(d >> 24); out[1] = (uint8_t)(d >> 16);
        out[2] = (uint8_t)(d >> 8);  out[3] = (uint8_t)d;
        out[4] = (uint8_t)(t >> 16); out[5] = (uint8_t)(t >> 8); out[6] = (uint8_t)t;
        return 0;
    }
    case FT_DATETIMEMS: {
        int32_t d; uint32_t ms;
        if (sg_datetimems(val, vlen, &d, &ms)) {
            fmt_conv_err(err, errsz, f, "expected YYYYMMDDHHMMSSfff or 'YYYY-MM-DD HH:MM:SS.fff' (exactly 3 fraction digits)");
            return -1;
        }
        out[0] = (uint8_t)(d >> 24);  out[1] = (uint8_t)(d >> 16);
        out[2] = (uint8_t)(d >> 8);   out[3] = (uint8_t)d;
        out[4] = (uint8_t)(ms >> 24); out[5] = (uint8_t)(ms >> 16);
        out[6] = (uint8_t)(ms >> 8);  out[7] = (uint8_t)ms;
        return 0;
    }
    case FT_TIME: {
        uint32_t s;
        if (sg_time(val, vlen, &s)) {
            fmt_conv_err(err, errsz, f, "expected HH:MM:SS (00:00:00..23:59:59)");
            return -1;
        }
        out[0] = (uint8_t)(s >> 16); out[1] = (uint8_t)(s >> 8); out[2] = (uint8_t)s;
        return 0;
    }
    case FT_UUID:
        if (sg_uuid(val, vlen, out)) {
            fmt_conv_err(err, errsz, f, "expected 32 hex digits or the 36-character dashed form");
            return -1;
        }
        return 0;
    case FT_IPV4:
        if (sg_ipv4(val, vlen, out)) {
            fmt_conv_err(err, errsz, f, "not a valid IPv4 address");
            return -1;
        }
        return 0;
    case FT_IPV6:
        if (sg_ipv6(val, vlen, out)) {
            fmt_conv_err(err, errsz, f, "not a valid IPv6 address");
            return -1;
        }
        return 0;
    case FT_ENUM:
        if (enum_value_index(f, val, vlen) < 0) {
            fmt_conv_err(err, errsz, f, "unknown enum member");
            return -1;
        }
        break; /* encode_field_len writes the index */
```
and in the existing `case FT_VARCHAR:` block of `encode_field_checked`, before the overlength test, add nothing for NUL (D7: varchar NUL policy unchanged). (`typed_type_name` must already name ipv4/ipv6/uuid/bool/date/time types — verify; if a name is missing add it in the same hunk.) Datetime stored byte layout must equal the pre-existing encoder's (7 bytes: BE int32 date + BE uint24 seconds; datetimems 8 bytes: BE int32 + BE uint32 ms) — the Task 2 test compares against `encode_field_len` output for every valid corpus entry (equivalence test, both directions of assertion).

**Hunk 2c — legacy-layout `""` never reaches the converter** (`src/db/config.c`, functions `typed_encode_defaults` and `typed_encode`). Add this helper immediately before `int sentinel_span_parses(` (anchor: that exact line; it is deleted in Task 3, so place the helper above where it stood):
```c
/* R4: types for which a zero-length string is an invalid value (it must
   reach encode_field_checked, which rejects it) rather than the historic
   "no value" no-op. */
static int empty_span_invalid(enum FieldType t) {
    switch (t) {
    case FT_BOOL: case FT_DATE: case FT_DATETIME: case FT_DATETIMEMS:
    case FT_TIME: case FT_UUID: case FT_IPV4: case FT_IPV6: case FT_ENUM:
        return 1;
    default:
        return 0;
    }
}
```
Then in `typed_encode_defaults` change the line `            if (el > 0) {` that is immediately followed by `                if (!sentinel_span_parses(ts->fields[i].type, ev, el)) {\n                    if (ts->fields[i].not_null) return -2;` to `            if (el > 0 || empty_span_invalid(ts->fields[i].type)) {`, and in `typed_encode` change the line `                if (el > 0) {` immediately followed by `                    if (!sentinel_span_parses(ts->fields[i].type, ev, el)) {\n                        if (ts->fields[i].not_null) {\n                            char message[320];` to `                if (el > 0 || empty_span_invalid(ts->fields[i].type)) {`. (Each anchor is unique by its following line; verify exactly once or halt. Task 3 then removes the sentinel blocks inside them.) Because the helper sits above its two callers and uses only enum values, no prototype is needed. On success path `encode_json_field_checked` is called with `el == 0` and returns `-2` with the field-named error already in `err_buf` — Task 2's test asserts `{"d":""}` errors on a **legacy-layout** object and on `bulk-insert` JSON that routes through `query_bulk.c`.

**Hunk 2d — packed route `""` (no varchar NUL change; D7).** Empty `""` for the new types and enum on the packed route is already rejected once the sentinel block is gone (the `n&&enum_value_index` guard is bypassed at `n==0`, and `row_encode_fixed` → `encode_json_field_checked` → `encode_field_checked` rejects the empty span); Task 2's test asserts it on a packed object explicitly. The two fixtures that store `\u0000` on purpose (`test_packed_json_projection.c`, `test_packed_named_projection.c`) stay unchanged.

**Hunk 2e — brace-wrapped UUID vs the computed-update gate.** Add to `src/db/config.c` (after `encode_field_checked`) and declare in `src/db/types.h` immediately after the `encode_field_checked` declaration:
```c
/* 1 iff f is a uuid field and v is a valid uuid literal (including the
   brace-wrapped form). Used to exempt such values from the "starts with
   '{' ⇒ computed-update object" gate. */
int value_is_braced_uuid(const TypedField *f, const char *v, size_t n) {
    uint8_t tmp[16];
    return f->type == FT_UUID && n && v[0] == '{' && sg_uuid(v, n, tmp) == 0;
}
```
Apply at the three gates (each is a distinct line; verify exactly once):
- `src/db/storage.c`: `            if (f->type != FT_VARCHAR && field_vals[i][0] == '{') return -2;` → `            if (f->type != FT_VARCHAR && field_vals[i][0] == '{' &&\n                !value_is_braced_uuid(f, field_vals[i], strlen(field_vals[i]))) return -2;`
- `src/db/query_bulk.c`: `                if (w->ts->fields[tidx].type != FT_VARCHAR && *cell_start == '{')` → `                if (w->ts->fields[tidx].type != FT_VARCHAR && *cell_start == '{' &&\n                    !value_is_braced_uuid(&w->ts->fields[tidx], cell_start, cell_len))`
- `src/db/query_bulk.c`: `                json_rec->field_values[i][0] == '{') {ctx->error=errno?errno:EINVAL;return -1;}` → `                json_rec->field_values[i][0] == '{' &&\n                !value_is_braced_uuid(&w->ts->fields[tidx], json_rec->field_values[i], strlen(json_rec->field_values[i]))) {ctx->error=errno?errno:EINVAL;return -1;}`

Test: `update`, `bulk-update` (JSON and CSV) of a uuid field with `{550e8400-e29b-41d4-a716-446655440000}` stores the canonical UUID, while `{"$incr":1}`-style objects on a uuid field still error.

**Hunk 2f — data-derived index probes mean "no match", never an error** (`src/db/typed_index.c`, `typed_index_text_key_into`). Anchor (exactly once):
```c
  uint8_t raw[16];
  errno = 0;
  if (encode_field_checked(f, text, n, raw, NULL, 0))
    return fail(errno == ENOMEM ? ENOMEM : EINVAL);
```
replace with:
```c
  uint8_t raw[16];
  errno = 0;
  if (encode_field_checked(f, text, n, raw, NULL, 0)) {
    /* R4: a probe text that is not in the field's grammar (a join driver
       value of another type, a legacy stored zero/invalid date) cannot
       equal any stored value: no match, not an error. Client literals are
       rejected earlier by predicate_validate. */
    if (errno == ENOMEM)
      return fail(ENOMEM);
    return 0;
  }
```
Verify each caller of `typed_index_text_key_into` treats `0` as "no match" (the enum branch above already returns 0): `query.c` helper wrappers, composite/cursor bound, `m->schema` probes, `in_values` key building, order-by `ospec.parts[i].idx` (2 calls), `query_join.c` (2 calls). Any caller that treats `!= 1` as an error must be changed to treat `0` as an empty probe — list each in the hand-back. Tests: (a) join a varchar driver value `"yes"` to a remote bool, `"x"` to a remote date/uuid ⇒ no matched rows, not an error, for inner and left; (b) cursor/order_by with a stored zero date does not error; (c) a client `eq` literal that is invalid still errors.

### Task 3 — Remove the sentinel downgrade (malformed ⇒ error, never NULL)

Test first (RED on the Task 2 tree): insert/update/bulk of `{"u":"not-a-uuid"}`, `{"i4":"999.1.1.1"}`, `{"i6":"::g"}` into a **nullable** field return the same field-named error as a not_null field; the record is not written; `null` still stores NULL; `0.0.0.0`, `::`, nil UUID store as bit-clear real values and read back as themselves.

Remove the downgrade branches: in `src/db/typed_row.c` delete the block
```c
        if(!sentinel_span_parses(f->type,text,n)){
            if(f->not_null){fmt_not_null_err(f,err,cap);errno=EINVAL;goto done;}
            rc=replace_cell(p,i,NULL,NULL,0,1,err,cap);goto done;
        }
```
and in `src/db/config.c` the two blocks inside the `el > 0 || empty_span_invalid(...)` guards of Task 2 hunk 2c. In `typed_encode_defaults` delete exactly:
```c
                if (!sentinel_span_parses(ts->fields[i].type, ev, el)) {
                    if (ts->fields[i].not_null) return -2;
                    /* Malformed uuid/ipv4/ipv6 — the historic unset
                       sentinel. Store NULL (bit set) rather than a
                       visible zero value on nullmap records. */
                    typed_set_null(out + 1, ts->nullmap_prefix - 1, i);
                    break;
                }
```
and in `typed_encode` delete exactly:
```c
                    if (!sentinel_span_parses(ts->fields[i].type, ev, el)) {
                        if (ts->fields[i].not_null) {
                            char message[320];
                            fmt_not_null_err(&ts->fields[i], message, sizeof(message));
                            LOG_WARN(LOG_SUB_CONFIG, "%s/%s: %s (malformed sentinel)",
                                     db_root ? db_root : "?", object ? object : "?", message);
                            fmt_not_null_err(&ts->fields[i], err_buf, err_buf_size);
                            return -2;
                        }
                        /* Malformed uuid/ipv4/ipv6 — the historic unset
                           sentinel. Store NULL (bit set) rather than a
                           visible zero value on nullmap records. */
                        typed_set_null(out + 1, ts->nullmap_prefix - 1, i);
                        break;
                    }
```
(copy the text from the file as it stands; if either block differs from the quote, write `PLAN_NOTES.md` and halt). The remaining code in each guard falls through to the checked encoder, which now returns the error. Then delete `sentinel_span_parses` from `config.c` and its declaration `int sentinel_span_parses(enum FieldType type, const char *value, size_t len);` from `typed_row.h`; `grep -rn sentinel_span_parses src` must print nothing.

### Task 4 ◆ — Query side: one grammar, strict compile

Test first (RED): through `find`/`count`/`aggregate`-criteria: `eq`/`neq`/`lt`/`between`/`in`/`not_in` on each new type with (a) valid ISO and compact date forms returning identical result sets, (b) brace-wrapped UUID matching the stored canonical UUID, (c) invalid literals returning the same field-named error as writes, (d) bool `yes`/`off`/`TRUE` matching correctly (old first-byte bug: `"yes"` compiled as false), (e) enum unknown literal erroring (D4), (f) the same results indexed vs full-scan (add an index on each field).

In `src/db/query_plan.c`:
1. Add `#include "scalar_grammar.h"` with the other local includes.
2. Replace the bodies of `parse_date_i32`, `parse_datetime`, `parse_datetimems`, `parse_uuid`, `parse_ipv4`, `parse_ipv6`, `parse_time` with thin wrappers (signatures unchanged so the many call sites need no edit):
```c
static int32_t parse_date_i32(const char *s) {
    int32_t d = 0;
    if (s && sg_date(s, strlen(s), &d)) d = 0;
    return d;
}
static void parse_datetime(const char *s, int32_t *out_date, uint32_t *out_time) {
    *out_date = 0; *out_time = 0;
    if (s) (void)sg_datetime(s, strlen(s), out_date, out_time);
}
static void parse_datetimems(const char *s, int32_t *out_date, uint32_t *out_ms) {
    *out_date = 0; *out_ms = 0;
    if (s) (void)sg_datetimems(s, strlen(s), out_date, out_ms);
}
static void parse_uuid(const char *s, uint8_t out[16]) {
    memset(out, 0, 16);
    if (s) (void)sg_uuid(s, strlen(s), out);
}
static void parse_ipv4(const char *s, uint8_t out[4]) {
    memset(out, 0, 4);
    if (s) (void)sg_ipv4(s, strlen(s), out);
}
static void parse_ipv6(const char *s, uint8_t out[16]) {
    memset(out, 0, 16);
    if (s) (void)sg_ipv6(s, strlen(s), out);
}
static void parse_time(const char *s, uint8_t out[3]) {
    uint32_t secs = 0;
    memset(out, 0, 3);
    if (s && sg_time(s, strlen(s), &secs) == 0) {
        out[0] = (secs >> 16) & 0xFF; out[1] = (secs >> 8) & 0xFF; out[2] = secs & 0xFF;
    }
}
```
(These run only after `predicate_validate` has accepted the literal, or for the unused `value2` of non-BETWEEN ops, where failure-to-zero is harmless. Delete the old bodies completely, including their comments.)
3. Bool — one shared helper. Add to `src/db/config.c` (after `value_is_braced_uuid`) and declare in `src/db/types.h` after its declaration:
```c
/* Truth value of an already-validated bool literal (grammar: sg_bool);
   0 for anything the grammar rejects. */
uint8_t scalar_bool_value(const char *s) {
    uint8_t b = 0;
    if (s && sg_bool(s, strlen(s), &b)) b = 0;
    return b;
}
```
Then in `query_plan.c` replace `cc->b1 = (search_criterion_value(c)[0] == 't' || search_criterion_value(c)[0] == 'T' || search_criterion_value(c)[0] == '1') ? 1 : 0;` with `cc->b1 = scalar_bool_value(search_criterion_value(c));`, and replace the exact two-statement IN-list text
```c
                if (cc->ftype == FT_BOOL && (c->in_values[i][0] == 't' ||
                    c->in_values[i][0] == 'T' || c->in_values[i][0] == '1'))
                    cc->in_i64[i] = 1;
                else
                    cc->in_i64[i] = (int64_t)strtoll(c->in_values[i], NULL, 10);
```
with
```c
                cc->in_i64[i] = cc->ftype == FT_BOOL
                    ? (int64_t)scalar_bool_value(c->in_values[i])
                    : (int64_t)strtoll(c->in_values[i], NULL, 10);
```
In `src/db/query.c` there are **two** identical statements (indexed `OP_NOT_EQUAL` bool shortcut: the btree dispatch and the chunk walker):
```c
                int is_true = (search_criterion_value(pc)[0] == 't' || search_criterion_value(pc)[0] == 'T' ||
                               search_criterion_value(pc)[0] == '1');
```
Replace **both** (use `replace_all` and confirm exactly 2 hits) with `                int is_true = scalar_bool_value(search_criterion_value(pc));`. Root cause being fixed: the first-byte test makes `neq "yes"` search the inverse of the wrong bucket on an indexed bool. Test (f) covers it (indexed vs full-scan `neq` on bool for `yes`/`off`/`TRUE`).
4. Validation scope and enum (D4). Replace in `predicate_validate_value`
```c
    if(f->type==FT_VARCHAR||f->type==FT_ENUM||f->type==FT_NUMERIC)return 0;
    if(c->op==OP_REGEX||c->op==OP_NOT_REGEX||predicate_is_length(c->op))return 0;
```
with
```c
    if(f->type==FT_VARCHAR||f->type==FT_NUMERIC)return 0;
    /* R4: strict grammar validation applies to comparison ops only.
       Pattern/substring ops operate on rendered text and stay unvalidated. */
    if(!predicate_numeric_comparison(c->op))return 0;
```
(`predicate_numeric_comparison` is defined above it and covers eq/neq/lt/gt/lte/gte/between/in/not_in; regex and length ops are excluded by it, so the old line is subsumed.) Executor: list every `SearchOp` the planner has beyond those (e.g. case-insensitive eq variants) in the hand-back as "not validated" — do not add them. Enum literals now reach `encode_json_field_checked`, which checks membership via `encode_field_checked` (Task 2). Test one op per class on each new type: comparison (strict), `starts`/`contains`/`like` (accepted as before, no error), `regex`/`len_*` (unchanged).
5. Index/bitmap key route: after Task 2 hunk 2f, `find` with an invalid client literal on an indexed field must still **error** (rejected earlier by `predicate_validate`, before any index walk) — RED→GREEN test; data-derived probes are covered by 2f.

### Task 5 — Remaining unchecked/derived paths and server-generated values

Test first: (a) generated values stay in-grammar — for each of date/datetime/datetimems/timestamp `auto_create`/`auto_update`, `default=uuid()`, `default=random(N)` on uuid/varchar, `default=seq(x)`, run an insert and a `add-field … :default=…` backfill and assert success plus that the stored value round-trips through `sg_*` (call the grammar function on the rendered value); (b) `edit-field` between same-type declarations and `vacuum` do not reinterpret bytes (stored values unchanged).
No code change is expected for `storage.c` ×2 and `query_find.c` ×3 (they stay on unchecked `encode_field` with server-generated input). If the test finds any generated string outside the grammar (e.g. `auto_now_str` output), fix the generator, not the grammar.
Also route the **default clause validation** (`schema_wire.c`, `encode_field_checked(f,f->default_val,…)`) and `parse_default_modifier_checked` for the new types: `create-object`/`add-field` with `:default=2024-02-30` on a date field must now fail at declaration time with the grammar error (RED→GREEN test); a valid default is stored as the strict encoding.

### Task 6 ◆ — Updating existing tests (class E) and docs

Run the full suite after Tasks 2–5 and work from the Task 0 class-E census (the list is authoritative; the suite run confirms nothing is missing). Failures that are lenient-form fixtures (e.g. date-only datetimes, trailing-junk dates, `"yes"` expecting false, malformed uuid/ip expecting NULL/zero) are class E: change the *input* to a valid form only when the test's purpose is unrelated, or change the *expectation* to the new error when the test is about the lenient behaviour. List every edit (file, old, new) in the hand-back. Do not delete, skip or loosen a case. Any failure outside this class stops the run.

Docs (same branch): `docs/concepts/typed-records.md` (the `time` row says "Malformed input encodes 0" — rewrite; add a grammar table for bool/date/datetime/datetimems/time/uuid/ip/enum), `docs/query-protocol/*` pages that show date/uuid examples (grep `yyyyMMdd`, `uuid`, `ipv4` under `docs/`), `docs/reference/error-codes.md` (new messages), and `docs/reference/changelog.md` "Unreleased": a **breaking-change** paragraph covering D1–D5 (rejected forms, empty string, whitespace, enum query error, malformed uuid/ip no longer NULL/zero, varchar NUL rejection). npm: check `npm/README.md`/`npm/index.d.ts` mention of date/uuid string forms; update only if they describe lenient forms.

### Task 7 ◆ — Route-equivalence and final checks

One consolidated route matrix test (extend Task 0's file): for every type, the same invalid input is rejected with the same message on insert, update, bulk-insert JSON, bulk delimited (non-empty bad cell), default declaration, and `find` criterion; the same valid inputs are accepted identically on every route; explicit `null` stays NULL on every write route; `0.0.0.0`, `::`, nil UUID, `00:00:00`, empty varchar, numeric zero round-trip as real values. Final: `SKIP_TESTS=1 ./build.sh` zero warnings; `git diff --check`; full `run-all` fresh (0 failed); `grep` audits: `sentinel_span_parses` absent; no `atoi`/`strtoll` remains in a *date/bool/time/uuid/ip data-parsing* path of `query_plan.c` (the numeric ones stay); no new `TODO`/debug prints. Hand back with: RED/GREEN/revert outputs for Tasks 1, 3, 4, 5, the Task 0 table, the class-E edit list, every decision taken (should be none beyond D1–D6), and any call site where a D3 empty-guard was added.

## 7. Edge cases and invariants (explicit)

- **Existing `:default=` literals are re-read at insert time** (no startup validation exists): `default=yes` on a bool previously stored false and now stores true; `default=2024-1-5` or an empty `:default=` on date/uuid/ip now makes inserts that rely on the default fail with the grammar error. Declaration-time validation (Task 5) covers only new/edited definitions. Task 6's changelog states this; Task 0 records the current behaviour.
- CI `scan-build.yml` / `coverity.yml` list sources explicitly and already omit `agg_exact.c`; executor checks whether they link (not just compile) and adds `scalar_grammar.c` only if so.
- Stored bytes and layouts are unchanged; **no migration**, no version bump of on-disk format. A previously stored invalid-looking value (e.g. a date stored as 20241399 by the old encoder) is still **read** as-is; only new input is validated (recovery plan: never rewrite old data or declare zero-valued records corrupt).
- Nil UUID, `0.0.0.0`, `::`, `00:00:00`, empty varchar and numeric zero remain real non-NULL values; explicit JSON `null` is NULL; omission follows insert/default/update rules; none of these are changed.
- `encode_field_len`/`encode_field` (unchecked) keep their lenient behaviour for server-generated inputs only; they must not gain new callers (Task 7 grep: the call-site list in §4 is exhaustive).
- Per-record bulk skip/error contracts are preserved (no invented atomicity).
- `grep -rn "sg_"` consumers: only `config.c` and `query_plan.c`; the module has no global state, so it is thread-safe by construction (TSan is not run in this plan).
- Resource handling: no allocation in the grammar module; `inet_pton` buffers are fixed and length-checked before copy (the T13 truncation bug).

## 8. Open items deliberately left to later work

R5: schema conversion/migration of existing stored invalid values and durable-metadata publication. R6: public activation, clients/docs acceptance, sanitizer gate. Not in R4: changing varchar from bytes to Unicode characters; timestamp semantics; time zones; fractional seconds beyond ms.

**R7 (new, post-activation; human-approved 2026-11-01): database-wide varchar NUL audit.** After R6 activates packed storage, plan and run an audit of every remaining route and C-string site for embedded-NUL handling (writes, criteria, index keys, aggregates/group keys, joins, CSV, cursors, files/keys), fix what truncates or mismatches, and document the final NUL contract. R4 changes nothing about varchar NUL.


## 9. Approval requested

D1–D6 approved; M2 (probes ⇒ no match) and M3 (valid braced UUID exempt from the `{` gate) approved. D7 decided: R4 leaves varchar NUL unchanged; database-wide NUL audit is R7 after R6. Still needed: confirm the executor, and confirm that class-E test edits are pre-authorised under §6 Task 6.
