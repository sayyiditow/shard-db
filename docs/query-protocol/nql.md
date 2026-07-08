# Natural Query Language (NQL)

NQL is shard-db's human-readable filter syntax. It replaces the JSON criteria array in the
three read commands — `find`, `count`, and `aggregate` — both at the CLI and over the raw TCP wire.

```bash
# CLI
./shard-db find default users 'age > 18 and status = active' --order-by age:desc --limit 20

# TCP wire (same string, server auto-detects)
echo 'find default users "age > 18 and status = active" --limit 20' | nc localhost 9199
```

The existing JSON wire protocol is **unchanged** — JSON and NQL are accepted on the same port,
distinguished by the first character of the line (`{` = JSON, `f`/`c`/`a` = NQL).

---

## Command shapes

```
find      ::= "find"      dir obj [filter] [find-flags]
count     ::= "count"     dir obj [filter]
aggregate ::= "aggregate" dir obj [filter] agg-list [agg-flags]
```

`dir` and `obj` are bare words (no spaces). `filter` is a single shell-quoted (or double-quoted)
string. When `filter` is absent, all records match — same as empty criteria `[]` in JSON.

For `aggregate`, the parser tells `filter` from `agg-list` by checking whether the token contains
`(`. If it does, it is `agg-list`; if not, it is `filter` and the next positional is `agg-list`.

---

## Flags

### find flags

| Flag | Argument | Meaning |
|---|---|---|
| `--order-by` | `field[:dir]` | Sort by `field`; `dir` is `asc` (default) or `desc`. |
| `--limit` | integer | Max records returned (default: `GLOBAL_LIMIT`). |
| `--offset` | integer | Skip the first N matches. |
| `--fields` | `f1,f2,...` | Comma-separated field projection. |
| `--format` | `json\|rows\|csv\|dict` | Output shape (default: JSON array). |
| `--cursor` | JSON value | Opt into keyset cursor pagination. Pass `null` for page 1, or a JSON cursor object for subsequent pages. Requires indexed `order_by`. |
| `--auth` | token | Auth token for TCP wire use. (CLI uses trusted-localhost; not needed there.) |

### count flags

No flags (the filter is the only optional argument).

### aggregate flags

| Flag | Argument | Meaning |
|---|---|---|
| `--group-by` | `f1,f2,...` | Grouping keys. |
| `--having` | `filter` | Post-aggregation filter (same grammar as the main filter). |
| `--order-by` | `alias[:dir]` | Sort groups by an aggregate alias or group-by field. |
| `--limit` | integer | Max groups returned. |
| `--auth` | token | Auth token for TCP wire use. |

---

## Filter grammar

```
filter      = or-expr

or-expr     = and-expr ("or" and-expr)*

and-expr    = atom ("and" atom)*

atom        = "(" or-expr ")"
            | predicate

predicate   = field no-val-op
            | field "between" value "and" value
            | field in-op list
            | field bin-op value
```

AND binds tighter than OR. Parentheses override precedence. Max nesting depth: 16 (matches the engine limit).

### Operators

```
no-val-op  = "exists" | "not_exists" | "nexists"

in-op      = "in" | "not_in" | "not" "in"

bin-op     = eq-op | cmp-op | str-op | len-op | field-op

eq-op      = "eq" | "=" | "neq" | "!="

cmp-op     = "gt" | ">" | "lt" | "<" | "gte" | ">=" | "greater_eq"
           | "lte" | "<=" | "less_eq"

str-op     = "contains"    | "not_contains"  | "not" "contains"
           | "starts"      | "not_starts"    | "not" "starts"
           | "ends"        | "not_ends"      | "not" "ends"
           | "like"        | "not_like"      | "not" "like"
           | "icontains"   | "not_icontains" | "not" "icontains"
           | "istarts"     | "iends"
           | "ilike"       | "not_ilike"     | "not" "ilike"
           | "regex"       | "not_regex"     | "not" "regex"

len-op     = "len_eq" | "len_gt" | "len_lt" | "len_gte" | "len_lte" | "len_between"

field-op   = "eq_field" | "neq_field" | "lt_field" | "lte_field"
           | "gt_field" | "gte_field"
```

`not <op>` (two tokens) normalises to `not_<op>` before reaching the engine.
Symbolic aliases (`=`, `!=`, `>`, `<`, `>=`, `<=`) are valid inside quoted strings because shell
metacharacter expansion does not apply inside single or double quotes.

### BETWEEN disambiguation

`between` consumes `value`, then the literal token `and`, then the second `value`. That `and` is
consumed by `between` and does **not** count as a logical AND connector.

### Values

```
value         = single-quoted | number | boolean | bare-word

single-quoted = "'" chars "'"
number        = "-"? [0-9]+ ("." [0-9]+)?
boolean       = "true" | "false"
bare-word     = [a-zA-Z0-9_.-]+   # no spaces; use single-quoted for spaces
```

Values with spaces must be single-quoted: `name starts 'john doe'`.

### List (IN / NOT_IN)

```
list = "(" value ("," value)* ")"
     | "[" value ("," value)* "]"
```

Both bracket styles accepted. Spaces around commas are fine.

---

## Aggregate spec grammar

```
agg-list = agg-spec ("," agg-spec)*
agg-spec = agg-fn "(" [field] ")"
agg-fn   = "count" | "sum" | "avg" | "min" | "max"
```

Output aliases are auto-generated: `sum(amount)` → alias `sum_amount`, `count()` → alias `count`.
`--having` and `--order-by` reference these auto-generated names.

---

## Examples

### find

```bash
# Equality
./shard-db find default users 'username = alice'

# Range (symbolic operators)
./shard-db find default users 'age > 18 and age < 65'

# OR with parentheses
./shard-db find default users '(status = active or status = pending) and age > 18'

# IN list
./shard-db find default users 'status in (active, pending, trial)'

# BETWEEN
./shard-db find default users 'age between 18 and 65'

# String ops
./shard-db find default users "username starts 'john'"
./shard-db find default users "bio contains 'rust'"

# Existence
./shard-db find default users 'phone exists'
./shard-db find default users 'deleted_at not_exists'

# With flags
./shard-db find default users 'age > 18' --order-by age:desc --limit 20 --offset 40 --fields name,email

# No filter (all records)
./shard-db find default users --limit 50
```

### count

```bash
./shard-db count default users 'status = active'
./shard-db count default users 'age between 18 and 65 and status != deleted'
./shard-db count default users   # all records — O(1) metadata path
```

### aggregate

```bash
# Whole-table count
./shard-db aggregate default orders count()

# Sum + avg with filter
./shard-db aggregate default orders 'status = active' sum(amount),avg(amount)

# Group by with having and order
./shard-db aggregate default orders sum(amount),count() \
  --group-by status \
  --having 'count > 100' \
  --order-by sum_amount:desc \
  --limit 10

# Multi-field group by
./shard-db aggregate default orders 'created_at > 20260101000000' \
  sum(amount),avg(amount),min(amount),max(amount) \
  --group-by status,currency \
  --order-by sum_amount:desc
```

---

## NQL over TCP

The server auto-detects NQL vs. JSON by the first character of each request line:
- `{` → JSON query (existing protocol)
- `f`, `c`, or `a` → NQL (`find` / `count` / `aggregate`)

NQL requests are framed and terminated the same way as JSON: one line per request, response
terminated by `\0\n`. Pipelining works identically.

```bash
# netcat example
echo 'find default users "age > 18" --limit 5' | nc localhost 9199

# Python
import socket
s = socket.socket()
s.connect(("localhost", 9199))
s.sendall(b'find default users "age > 18" --limit 5\n')
s.shutdown(socket.SHUT_WR)
data = s.makefile().read()
s.close()
```

When sending NQL over TCP from a non-trusted IP, include `--auth <token>`:

```bash
echo 'find tenant users "age > 18" --auth mysecrettoken --limit 5' | nc remotehost 9199
```

---

## Operator normalisation table

| Written | Sent to engine |
|---|---|
| `=` | `eq` |
| `!=` | `neq` |
| `>` | `gt` |
| `<` | `lt` |
| `>=` | `gte` |
| `<=` | `lte` |
| `less_eq` | `lte` |
| `greater_eq` | `gte` |
| `nexists` | `not_exists` |
| `not contains` | `not_contains` |
| `not starts` | `not_starts` |
| `not ends` | `not_ends` |
| `not like` | `not_like` |
| `not icontains` | `not_icontains` |
| `not ilike` | `not_ilike` |
| `not regex` | `not_regex` |
| `not in` | `not_in` |

---

## Planner behaviour

NQL filters produce the same `CriteriaNode *` tree that the JSON criteria path does — they
hit identical planner paths (PRIMARY_INTERSECT, PRIMARY_LEAF, PRIMARY_KEYSET, PRIMARY_NONE).
See [find → Execution paths](find.md#execution-paths) for details.

---

## Scope

NQL covers `find`, `count`, and `aggregate`. Joins, bulk operations, CAS,
schema mutations, and file storage are separate commands available via the
JSON protocol only.
