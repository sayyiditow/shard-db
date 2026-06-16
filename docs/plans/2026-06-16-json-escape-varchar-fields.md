# Fix: unescaped varchar field values break find/fetch/aggregate/file JSON responses

## Execution rules

- Branch off `main`: `git checkout -b fix/json-escape-field-values`. **Tasks
  1–12 below were already executed once** on this branch (uncommitted,
  verified: build clean, `./build/bin/shard-db-test run-all` →
  `4442 passed, 0 failed`, and the real reproducing query — `title contains
  "Cogs"` against production-pulled HN data — now returns valid JSON). If
  you're starting fresh on a clean `main`, do tasks 1–12 first, then 13+. If
  the branch already has tasks 1–12 applied (check `git diff src/db/query.c`
  for `json_escape_field`), skip straight to Task 13.
- Do tasks in order. Each task is a self-contained edit.
- Build with `SKIP_TESTS=1 ./build.sh`. Test with `./build/bin/shard-db-test run-all`.
- Never claim a step passed without showing the real command output.
- If a quoted anchor is not found exactly as written, STOP — do not guess or
  reinterpret. Write `PLAN_NOTES.md` describing the mismatch and stop.
- Leave the work **uncommitted** when done. Do not run git commit/push.

## Scope of this revision (Tasks 13–20)

Review of the Task 1–12 execution found the same unescaped-JSON bug class in
two more places that weren't in the original scope (find/fetch/get only):

1. **Aggregate `group_by` output** — grouping by a varchar field (e.g. `by`
   username, or `title`) with an embedded `"` breaks `aggregate` JSON the
   same way `find` was broken. Two emission sites in query.c.
2. **Stored filenames** — `valid_filename()` (util.c) rejects `/`, `\`,
   control characters, and `..`, but not `"`. A filename like `my"file.txt`
   passes validation, gets stored via `put-file`, and then breaks JSON in
   `list-files` (and, for any file stored before this fix ships, potentially
   `get-file`/`delete-file`/`get-file-path` too).

## Root cause

`typed_get_field_str()` (src/db/config.c) is the single function that decodes
a stored field's raw bytes to a string. Its `FT_VARCHAR` case returns the
**raw, unescaped** content:

```c
case FT_VARCHAR: {
    const uint8_t *p = data + f->offset;
    int slen = ((int)p[0] << 8) | (int)p[1];
    int content_max = f->size - 2;
    if (slen > content_max) slen = content_max;
    if (slen == 0) return NULL;
    char *out = malloc(slen + 1);
    memcpy(out, p + 2, slen);
    out[slen] = '\0';
    return out;
}
```

This is correct behavior for `typed_get_field_str`'s CSV callers (CSV does
its own RFC-4180 escaping) and its sort-key/cursor-comparison callers (raw
bytes are what `memcmp` needs). But `decode_field()` in query.c is a thin
wrapper around `typed_get_field_str` for non-composite fields, and **several
JSON-emitting code paths in query.c call `decode_field()` /
`typed_get_field_str()` directly and splice the result into a quoted JSON
string with no escaping**:

```c
char *pv = decode_field(raw, hdr->value_len, proj_fields[i], fs);
OUT(",\"%s\"", pv ? pv : "");   /* pv may itself contain unescaped " */
```

Any varchar field whose content contains a literal `"` (or `\`, or a control
character) breaks the JSON syntax of the response. This was reproduced
locally: HN story 19336924's title is `Honda's award winning commercial
"Cogs" [video]`. Every `find` response (cursor or not, with or without
`fields` projection, with or without `order_by`) that includes this row
produces invalid JSON — confirmed with `python3 -c "import json;
json.load(...)"` failing with `Expecting ',' delimiter`. This is the actual
root cause of the HN Explorer "honda" search returning truncated/wrong
results: the client's `JSON.parse()` on the response either throws or
recovers unpredictably depending on where in the stream the bad quote
landed, which is exactly the "9,878 vs 1 vs inconsistent count" symptom
chain. It is **not** a cursor bug, **not** an index bug, and **not** related
to vacuum/compaction — `count`, full scans, and `get` were all returning
correct data the whole time; only the JSON serialization of `find`/`fetch`
rows was broken.

The already-safe path: `decode_value()` → `typed_decode()` →
`decode_field_to_buf()` (config.c) DOES escape FT_VARCHAR correctly via
`json_escape_into()`. That's the "no projected fields, no rows/dict format"
default branch (full-record decode) and it must NOT be changed.

## Fix strategy

Do **not** change `typed_get_field_str()` itself — it has non-JSON callers
(CSV, sort-key/cursor-tiebreak comparisons) that need raw bytes. Instead, add
one small helper in query.c that JSON-escapes a malloc'd string in place
(consuming the input, returning a new escaped string), and wrap every call
site where a `decode_field()`/`typed_get_field_str()` result is spliced
directly into JSON output via `OUT(...)`.

Do **not** touch:
- CSV call sites (`csv_emit_row`, `buf_driver_values`/`csv_cell_to_buf`) —
  those already apply their own RFC-4180 escaping on raw bytes.
- Sort-key/cursor-tiebreak extraction in `ordered_collect_cb` (the `sv`
  variable at the "Extract sort key" comment) — used only for in-memory
  comparison, never emitted as JSON.
- Any call site already going through `decode_value()`/`typed_decode()` —
  already escaped.

## Task 1 — add the escaping helper

In `src/db/query.c`, find this anchor (the end of `decode_field`):

```c
        int idx = typed_field_index(fs->ts, field);
        return typed_get_field_str(fs->ts, (const uint8_t *)raw, idx);
    }
    return NULL;
}

/* ========== Compiled criteria & Joins structs (definitions below) ========== */
```

Replace it with (inserting the new helper between `decode_field` and the
"Compiled criteria" comment):

```c
        int idx = typed_field_index(fs->ts, field);
        return typed_get_field_str(fs->ts, (const uint8_t *)raw, idx);
    }
    return NULL;
}

/* JSON-escape a malloc'd field-value string for inline embedding inside a
   quoted JSON string. decode_field()/typed_get_field_str() return raw field
   bytes (CSV and sort-key callers need that); JSON emitters must escape
   before printing or an embedded '"'/'\' byte breaks the response. Frees
   v; returns a new malloc'd escaped string, or NULL if v was NULL or the
   escape failed. */
static char *json_escape_field(char *v) {
    if (!v) return NULL;
    size_t len = strlen(v);
    char *esc = malloc(len * 6 + 1);
    if (!esc) { free(v); return NULL; }
    int n = json_escape_into(esc, len * 6 + 1, v, len);
    free(v);
    if (n < 0) { free(esc); return NULL; }
    esc[n] = '\0';
    return esc;
}

/* ========== Compiled criteria & Joins structs (definitions below) ========== */
```

## Task 2 — `print_record_row`

Anchor:

```c
    OUT("%s[\"%s\"", *printed ? "," : "", key);
    if (proj_count > 0) {
        for (int i = 0; i < proj_count; i++) {
            char *pv = decode_field(raw, hdr->value_len, proj_fields[i], fs);
            OUT(",\"%s\"", pv ? pv : "");
            free(pv);
        }
    } else if (fs && fs->ts) {
        for (int i = 0; i < fs->ts->nfields; i++) {
            if (fs->ts->fields[i].removed) continue;
            char *pv = typed_get_field_str(fs->ts, (const uint8_t *)raw, i);
            OUT(",\"%s\"", pv ? pv : "");
            free(pv);
        }
    }
    OUT("]");
    (*printed)++;
}
```

Replace with:

```c
    OUT("%s[\"%s\"", *printed ? "," : "", key);
    if (proj_count > 0) {
        for (int i = 0; i < proj_count; i++) {
            char *pv = json_escape_field(decode_field(raw, hdr->value_len, proj_fields[i], fs));
            OUT(",\"%s\"", pv ? pv : "");
            free(pv);
        }
    } else if (fs && fs->ts) {
        for (int i = 0; i < fs->ts->nfields; i++) {
            if (fs->ts->fields[i].removed) continue;
            char *pv = json_escape_field(typed_get_field_str(fs->ts, (const uint8_t *)raw, i));
            OUT(",\"%s\"", pv ? pv : "");
            free(pv);
        }
    }
    OUT("]");
    (*printed)++;
}
```

## Task 3 — `adv_search_cb`: rows_fmt branch

Anchor:

```c
                } else if (sc->rows_fmt) {
                    OUT("%s[\"%s\"", sc->printed ? "," : "", key);
                    if (sc->proj_count > 0) {
                        for (int i = 0; i < sc->proj_count; i++) {
                            char *pv = decode_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs);
                            OUT(",\"%s\"", pv ? pv : "");
                            free(pv);
                        }
                    } else if (sc->fs && sc->fs->ts) {
                        for (int i = 0; i < sc->fs->ts->nfields; i++) {
                            if (sc->fs->ts->fields[i].removed) continue;
                            char *pv = typed_get_field_str(sc->fs->ts, (const uint8_t *)raw, i);
                            OUT(",\"%s\"", pv ? pv : "");
                            free(pv);
                        }
                    }
```

Replace with:

```c
                } else if (sc->rows_fmt) {
                    OUT("%s[\"%s\"", sc->printed ? "," : "", key);
                    if (sc->proj_count > 0) {
                        for (int i = 0; i < sc->proj_count; i++) {
                            char *pv = json_escape_field(decode_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs));
                            OUT(",\"%s\"", pv ? pv : "");
                            free(pv);
                        }
                    } else if (sc->fs && sc->fs->ts) {
                        for (int i = 0; i < sc->fs->ts->nfields; i++) {
                            if (sc->fs->ts->fields[i].removed) continue;
                            char *pv = json_escape_field(typed_get_field_str(sc->fs->ts, (const uint8_t *)raw, i));
                            OUT(",\"%s\"", pv ? pv : "");
                            free(pv);
                        }
                    }
```

## Task 4 — `adv_search_cb`: dict_fmt + default proj branches

Anchor:

```c
                } else if (sc->dict_fmt) {
                    OUT("%s\"%s\":", sc->printed ? "," : "", key);
                    if (sc->proj_count > 0) {
                        OUT("{");
                        int first = 1;
                        for (int i = 0; i < sc->proj_count; i++) {
                            char *pv = decode_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs);
                            if (!pv) continue;
                            OUT("%s\"%s\":\"%s\"", first ? "" : ",", sc->proj_fields[i], pv);
                            first = 0;
                            free(pv);
                        }
                        OUT("}");
                    } else {
                        char *val = decode_value(raw, hdr->value_len, sc->fs);
                        OUT("%s", val);
                        free(val);
                    }
                } else if (sc->proj_count > 0) {
                    OUT("%s{\"key\":\"%s\",\"value\":{", sc->printed ? "," : "", key);
                    int first = 1;
                    for (int i = 0; i < sc->proj_count; i++) {
                        char *pv = decode_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs);
                        if (!pv) continue;
                        OUT("%s\"%s\":\"%s\"", first ? "" : ",", sc->proj_fields[i], pv);
                        first = 0;
                        free(pv);
                    }
                    OUT("}}");
                } else {
                    char *val = decode_value(raw, hdr->value_len, sc->fs);
                    OUT("%s{\"key\":\"%s\",\"value\":%s}", sc->printed ? "," : "", key, val);
                    free(val);
                }
                sc->printed++;
            }
```

Replace with (only the two `decode_field(...)` lines inside the
`proj_count > 0` sub-branches change; the `decode_value` branches are
untouched since they're already escaped):

```c
                } else if (sc->dict_fmt) {
                    OUT("%s\"%s\":", sc->printed ? "," : "", key);
                    if (sc->proj_count > 0) {
                        OUT("{");
                        int first = 1;
                        for (int i = 0; i < sc->proj_count; i++) {
                            char *pv = json_escape_field(decode_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs));
                            if (!pv) continue;
                            OUT("%s\"%s\":\"%s\"", first ? "" : ",", sc->proj_fields[i], pv);
                            first = 0;
                            free(pv);
                        }
                        OUT("}");
                    } else {
                        char *val = decode_value(raw, hdr->value_len, sc->fs);
                        OUT("%s", val);
                        free(val);
                    }
                } else if (sc->proj_count > 0) {
                    OUT("%s{\"key\":\"%s\",\"value\":{", sc->printed ? "," : "", key);
                    int first = 1;
                    for (int i = 0; i < sc->proj_count; i++) {
                        char *pv = json_escape_field(decode_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs));
                        if (!pv) continue;
                        OUT("%s\"%s\":\"%s\"", first ? "" : ",", sc->proj_fields[i], pv);
                        first = 0;
                        free(pv);
                    }
                    OUT("}}");
                } else {
                    char *val = decode_value(raw, hdr->value_len, sc->fs);
                    OUT("%s{\"key\":\"%s\",\"value\":%s}", sc->printed ? "," : "", key, val);
                    free(val);
                }
                sc->printed++;
            }
```

## Task 5 — `keyset_emit_find`: rows_fmt branch

Anchor:

```c
                        } else if (rows_fmt) {
                            OUT("%s[\"%s\"", printed ? "," : "", keybuf);
                            if (proj_count > 0) {
                                for (int j = 0; j < proj_count; j++) {
                                    char *pv = decode_field((const char *)raw, value_len,
                                                            proj_fields[j], fs);
                                    OUT(",\"%s\"", pv ? pv : "");
                                    free(pv);
                                }
                            } else if (fs && fs->ts) {
                                for (int j = 0; j < fs->ts->nfields; j++) {
                                    if (fs->ts->fields[j].removed) continue;
                                    char *pv = typed_get_field_str(fs->ts, raw, j);
                                    OUT(",\"%s\"", pv ? pv : "");
                                    free(pv);
                                }
                            }
                            OUT("]");
```

Replace with:

```c
                        } else if (rows_fmt) {
                            OUT("%s[\"%s\"", printed ? "," : "", keybuf);
                            if (proj_count > 0) {
                                for (int j = 0; j < proj_count; j++) {
                                    char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                                            proj_fields[j], fs));
                                    OUT(",\"%s\"", pv ? pv : "");
                                    free(pv);
                                }
                            } else if (fs && fs->ts) {
                                for (int j = 0; j < fs->ts->nfields; j++) {
                                    if (fs->ts->fields[j].removed) continue;
                                    char *pv = json_escape_field(typed_get_field_str(fs->ts, raw, j));
                                    OUT(",\"%s\"", pv ? pv : "");
                                    free(pv);
                                }
                            }
                            OUT("]");
```

## Task 6 — `keyset_emit_find`: dict_fmt + default proj branches

Anchor:

```c
                        } else if (dict_fmt) {
                            OUT("%s\"%s\":", printed ? "," : "", keybuf);
                            if (proj_count > 0) {
                                OUT("{");
                                int first = 1;
                                for (int j = 0; j < proj_count; j++) {
                                    char *pv = decode_field((const char *)raw, value_len,
                                                            proj_fields[j], fs);
                                    if (!pv) continue;
                                    OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                                    first = 0;
                                    free(pv);
                                }
                                OUT("}");
                            } else {
                                char *v = decode_value((const char *)raw, value_len, fs);
                                OUT("%s", v);
                                free(v);
                            }
                        } else if (proj_count > 0) {
                            OUT("%s{\"key\":\"%s\",\"value\":{", printed ? "," : "", keybuf);
                            int first = 1;
                            for (int j = 0; j < proj_count; j++) {
                                char *pv = decode_field((const char *)raw, value_len,
                                                        proj_fields[j], fs);
                                if (!pv) continue;
                                OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                                first = 0;
                                free(pv);
                            }
                            OUT("}}");
                        } else {
                            char *v = decode_value((const char *)raw, value_len, fs);
                            OUT("%s{\"key\":\"%s\",\"value\":%s}", printed ? "," : "", keybuf, v);
                            free(v);
```

Replace with:

```c
                        } else if (dict_fmt) {
                            OUT("%s\"%s\":", printed ? "," : "", keybuf);
                            if (proj_count > 0) {
                                OUT("{");
                                int first = 1;
                                for (int j = 0; j < proj_count; j++) {
                                    char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                                            proj_fields[j], fs));
                                    if (!pv) continue;
                                    OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                                    first = 0;
                                    free(pv);
                                }
                                OUT("}");
                            } else {
                                char *v = decode_value((const char *)raw, value_len, fs);
                                OUT("%s", v);
                                free(v);
                            }
                        } else if (proj_count > 0) {
                            OUT("%s{\"key\":\"%s\",\"value\":{", printed ? "," : "", keybuf);
                            int first = 1;
                            for (int j = 0; j < proj_count; j++) {
                                char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                                        proj_fields[j], fs));
                                if (!pv) continue;
                                OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                                first = 0;
                                free(pv);
                            }
                            OUT("}}");
                        } else {
                            char *v = decode_value((const char *)raw, value_len, fs);
                            OUT("%s{\"key\":\"%s\",\"value\":%s}", printed ? "," : "", keybuf, v);
                            free(v);
```

## Task 7 — `cursor_find_cb`: rows_fmt branch

Anchor:

```c
    if (c->rows_fmt) {
        OUT("%s[\"%s\"", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = decode_field((const char *)raw, value_len,
                                        c->proj_fields[i], c->fs);
                OUT(",\"%s\"", pv ? pv : "");
                free(pv);
            }
        } else if (c->fs && c->fs->ts) {
            for (int i = 0; i < c->fs->ts->nfields; i++) {
                if (c->fs->ts->fields[i].removed) continue;
                char *pv = typed_get_field_str(c->fs->ts, raw, i);
                OUT(",\"%s\"", pv ? pv : "");
                free(pv);
            }
        }
        OUT("]");
    } else if (c->dict_fmt) {
```

Replace with:

```c
    if (c->rows_fmt) {
        OUT("%s[\"%s\"", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                        c->proj_fields[i], c->fs));
                OUT(",\"%s\"", pv ? pv : "");
                free(pv);
            }
        } else if (c->fs && c->fs->ts) {
            for (int i = 0; i < c->fs->ts->nfields; i++) {
                if (c->fs->ts->fields[i].removed) continue;
                char *pv = json_escape_field(typed_get_field_str(c->fs->ts, raw, i));
                OUT(",\"%s\"", pv ? pv : "");
                free(pv);
            }
        }
        OUT("]");
    } else if (c->dict_fmt) {
```

## Task 8 — `cursor_find_cb`: dict_fmt + default proj branches

Anchor:

```c
    } else if (c->dict_fmt) {
        OUT("%s\"%s\":", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            OUT("{");
            int first = 1;
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = decode_field((const char *)raw, value_len,
                                        c->proj_fields[i], c->fs);
                if (!pv) continue;
                OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
                first = 0;
                free(pv);
            }
            OUT("}");
        } else {
            char *dv = decode_value((const char *)raw, value_len, c->fs);
            OUT("%s", dv ? dv : "{}");
            free(dv);
        }
    } else if (c->proj_count > 0) {
        OUT("%s{\"key\":\"%s\",\"value\":{", c->printed ? "," : "", key_buf);
        int first = 1;
        for (int i = 0; i < c->proj_count; i++) {
            char *pv = decode_field((const char *)raw, value_len,
                                    c->proj_fields[i], c->fs);
            if (!pv) continue;
            OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
            first = 0;
            free(pv);
        }
        OUT("}}");
    } else {
        char *dv = decode_value((const char *)raw, value_len, c->fs);
        OUT("%s{\"key\":\"%s\",\"value\":%s}",
            c->printed ? "," : "", key_buf, dv ? dv : "{}");
        free(dv);
    }
```

Replace with:

```c
    } else if (c->dict_fmt) {
        OUT("%s\"%s\":", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            OUT("{");
            int first = 1;
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                        c->proj_fields[i], c->fs));
                if (!pv) continue;
                OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
                first = 0;
                free(pv);
            }
            OUT("}");
        } else {
            char *dv = decode_value((const char *)raw, value_len, c->fs);
            OUT("%s", dv ? dv : "{}");
            free(dv);
        }
    } else if (c->proj_count > 0) {
        OUT("%s{\"key\":\"%s\",\"value\":{", c->printed ? "," : "", key_buf);
        int first = 1;
        for (int i = 0; i < c->proj_count; i++) {
            char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                    c->proj_fields[i], c->fs));
            if (!pv) continue;
            OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
            first = 0;
            free(pv);
        }
        OUT("}}");
    } else {
        char *dv = decode_value((const char *)raw, value_len, c->fs);
        OUT("%s{\"key\":\"%s\",\"value\":%s}",
            c->printed ? "," : "", key_buf, dv ? dv : "{}");
        free(dv);
    }
```

## Task 9 — `cursor_find_cb`: cursor token's `last_value_str`

This is a second, independent exposure: when `order_by` is itself a varchar
field containing `"`, the **cursor token** (`{"cursor":{"<order_by>":"...",
"key":"..."}}`) breaks the same way, because `last_value_str` is embedded
unescaped at the two `OUT(",\"cursor\":{...}` sites later in query.c. Fixing
it once at the assignment point fixes every downstream emission.

Anchor:

```c
    c->last_value_str = (c->order_tf && c->fs && c->fs->ts)
        ? typed_get_field_str(c->fs->ts, raw, c->order_field_idx)
        : NULL;
    c->last_key_str = strndup(key_buf, klen);
```

Replace with:

```c
    c->last_value_str = (c->order_tf && c->fs && c->fs->ts)
        ? json_escape_field(typed_get_field_str(c->fs->ts, raw, c->order_field_idx))
        : NULL;
    c->last_key_str = strndup(key_buf, klen);
```

## Task 10 — `cmd_find`: join driver-rows rows_fmt branch

Anchor:

```c
            if (rows_fmt) {
                OUT("%s[\"%s\"", printed ? "," : "", r->key);
                if (proj_count > 0) {
                    for (int j = 0; j < proj_count; j++) {
                        char *pv = decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs);
                        OUT(",\"%s\"", pv ? pv : "");
                        free(pv);
                    }
                } else if (driver_fs.ts) {
                    for (int j = 0; j < driver_fs.ts->nfields; j++) {
                        if (driver_fs.ts->fields[j].removed) continue;
                        char *pv = typed_get_field_str(driver_fs.ts, val, j);
                        OUT(",\"%s\"", pv ? pv : "");
                        free(pv);
                    }
                }
                OUT("]");
```

Replace with:

```c
            if (rows_fmt) {
                OUT("%s[\"%s\"", printed ? "," : "", r->key);
                if (proj_count > 0) {
                    for (int j = 0; j < proj_count; j++) {
                        char *pv = json_escape_field(decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs));
                        OUT(",\"%s\"", pv ? pv : "");
                        free(pv);
                    }
                } else if (driver_fs.ts) {
                    for (int j = 0; j < driver_fs.ts->nfields; j++) {
                        if (driver_fs.ts->fields[j].removed) continue;
                        char *pv = json_escape_field(typed_get_field_str(driver_fs.ts, val, j));
                        OUT(",\"%s\"", pv ? pv : "");
                        free(pv);
                    }
                }
                OUT("]");
```

## Task 11 — `cmd_find`: join driver-rows dict_fmt + default proj branches

Anchor:

```c
            } else if (dict_fmt) {
                OUT("%s\"%s\":", printed ? "," : "", r->key);
                if (proj_count > 0) {
                    OUT("{");
                    int first = 1;
                    for (int j = 0; j < proj_count; j++) {
                        char *pv = decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs);
                        if (!pv) continue;
                        OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                        first = 0;
                        free(pv);
                    }
                    OUT("}");
                } else {
                    char *v = decode_value((const char *)val, r->value_len, &driver_fs);
                    OUT("%s", v);
                    free(v);
                }
            } else if (proj_count > 0) {
                OUT("%s{\"key\":\"%s\",\"value\":{", printed ? "," : "", r->key);
                int first = 1;
                for (int j = 0; j < proj_count; j++) {
                    char *pv = decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs);
                    if (!pv) continue;
                    OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                    first = 0;
                    free(pv);
                }
                OUT("}}");
            } else {
                char *v = decode_value((const char *)val, r->value_len, &driver_fs);
                OUT("%s{\"key\":\"%s\",\"value\":%s}", printed ? "," : "", r->key, v);
                free(v);
            }
            printed++;
        }
```

Replace with:

```c
            } else if (dict_fmt) {
                OUT("%s\"%s\":", printed ? "," : "", r->key);
                if (proj_count > 0) {
                    OUT("{");
                    int first = 1;
                    for (int j = 0; j < proj_count; j++) {
                        char *pv = json_escape_field(decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs));
                        if (!pv) continue;
                        OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                        first = 0;
                        free(pv);
                    }
                    OUT("}");
                } else {
                    char *v = decode_value((const char *)val, r->value_len, &driver_fs);
                    OUT("%s", v);
                    free(v);
                }
            } else if (proj_count > 0) {
                OUT("%s{\"key\":\"%s\",\"value\":{", printed ? "," : "", r->key);
                int first = 1;
                for (int j = 0; j < proj_count; j++) {
                    char *pv = json_escape_field(decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs));
                    if (!pv) continue;
                    OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                    first = 0;
                    free(pv);
                }
                OUT("}}");
            } else {
                char *v = decode_value((const char *)val, r->value_len, &driver_fs);
                OUT("%s{\"key\":\"%s\",\"value\":%s}", printed ? "," : "", r->key, v);
                free(v);
            }
            printed++;
        }
```

## Task 12 — audit for any remaining unescaped site

After completing Tasks 1–11, run:

```bash
grep -n "typed_get_field_str\|decode_field(" src/db/query.c
```

For every match NOT already changed by Tasks 2–11, inspect ±10 lines:

- If it feeds a `csv_emit_row`/`csv_cell_to_buf`/`buf_driver_values`/CSV
  buffer → leave it (CSV escaping already applies).
- If it feeds an in-memory comparison/sort/dedup (no `OUT(...)` with the
  value in the same statement) → leave it.
- If it feeds an `OUT(...)` call that splices the value inside a JSON
  quoted string (`\"%s\"`) → wrap it with `json_escape_field(...)` the same
  way as the tasks above and note the added site in `PLAN_NOTES.md` (file,
  function name, line) so the reviewer can re-check it.

If Task 12 finds nothing new, write `PLAN_NOTES.md` with the single line
`Task 12: audit clean, no additional sites found.`

## Verification

1. `SKIP_TESTS=1 ./build.sh` — must compile clean.
2. `./build/bin/shard-db-test run-all` — must print `# total: N passed, 0 failed`.
3. Manual repro check — from a scratch DB:

```bash
TESTDIR=$(mktemp -d)
cat > $TESTDIR/db.env << 'EOF'
export DB_ROOT=TESTDIR_PLACEHOLDER/db
export PORT=19401
EOF
sed -i "s#TESTDIR_PLACEHOLDER#$TESTDIR#" $TESTDIR/db.env
mkdir -p $TESTDIR/db
cd $TESTDIR && /path/to/build/bin/shard-db server &
sleep 1
BIN=/path/to/build/bin/shard-db
$BIN query '{"mode":"create-object","dir":"default","object":"t","splits":8,"max_key":16,"fields":["title:varchar:128"]}'
$BIN query '{"mode":"insert","dir":"default","object":"t","key":"1","value":{"title":"He said \"hi\" to me"}}'
$BIN query '{"mode":"find","dir":"default","object":"t","criteria":[],"fields":["title"]}' | python3 -c "import json,sys; json.load(sys.stdin); print('VALID JSON')"
$BIN stop
```

Expect `VALID JSON` printed, not a traceback. Also re-run the same query
with `"cursor":null` and `"format":"rows"`/`"format":"dict"` added, each
piped through `python3 -c "import json,sys; json.load(sys.stdin)"`, to
confirm all four output formats are fixed.

Report the actual output of all three verification steps — do not claim
success without it.

## Task 13 — add `json_escape_const`, a non-freeing escape helper

`json_escape_field` (Task 1) takes ownership of its input and frees it —
right for `decode_field`/`typed_get_field_str` results (always a fresh
malloc). The two remaining bug classes below hand us strings we do **not**
own: `b->group_vals[g]` is arena-owned (freed when the arena is freed, not
per-call), and `filename` is the caller's `const char *` request argument.
We need a variant that escapes without taking ownership.

Anchor (the helper added by Task 1):

```c
static char *json_escape_field(char *v) {
    if (!v) return NULL;
    size_t len = strlen(v);
    char *esc = malloc(len * 6 + 1);
    if (!esc) { free(v); return NULL; }
    int n = json_escape_into(esc, len * 6 + 1, v, len);
    free(v);
    if (n < 0) { free(esc); return NULL; }
    esc[n] = '\0';
    return esc;
}
```

Replace with (adding the new helper immediately after):

```c
static char *json_escape_field(char *v) {
    if (!v) return NULL;
    size_t len = strlen(v);
    char *esc = malloc(len * 6 + 1);
    if (!esc) { free(v); return NULL; }
    int n = json_escape_into(esc, len * 6 + 1, v, len);
    free(v);
    if (n < 0) { free(esc); return NULL; }
    esc[n] = '\0';
    return esc;
}

/* Like json_escape_field but does not take ownership of v — v may be
   arena-owned (AggBucket.group_vals), a request argument (filename), or
   any other string the caller still owns. Returns a new malloc'd escaped
   string (caller frees), or NULL if v is NULL or allocation fails. */
static char *json_escape_const(const char *v) {
    if (!v) return NULL;
    size_t len = strlen(v);
    char *esc = malloc(len * 6 + 1);
    if (!esc) return NULL;
    int n = json_escape_into(esc, len * 6 + 1, v, len);
    if (n < 0) { free(esc); return NULL; }
    esc[n] = '\0';
    return esc;
}
```

Also add the declaration to `src/db/types.h`. Anchor:

```c
char *decode_field(const char *raw, size_t raw_len, const char *field, FieldSchema *fs);
char *decode_value(const char *raw, size_t raw_len, FieldSchema *fs);
char *json_escape_field(char *v);
```

Replace with:

```c
char *decode_field(const char *raw, size_t raw_len, const char *field, FieldSchema *fs);
char *decode_value(const char *raw, size_t raw_len, FieldSchema *fs);
char *json_escape_field(char *v);
char *json_escape_const(const char *v);
```

## Task 14 — aggregate top-N heap path: escape varchar group_by value

This is the `order_by` + `limit` aggregate path (top-N heap). `gb_is_varchar`
true branch and the `else` (unknown-type) fallback branch both build
`val_buf` via raw `memcpy` from the index key and print it unescaped.

Anchor:

```c
        /* Emit the group_by field value. */
        char val_buf[1032];
        int  vl = 0;
        if (gb_is_varchar) {
            /* Index stores raw string content for varchar. */
            vl = (int)gklens[i];
            if (vl > (int)(sizeof(val_buf) - 1)) vl = (int)(sizeof(val_buf) - 1);
            memcpy(val_buf, gkeys[i], (size_t)vl);
            val_buf[vl] = '\0';
            OUT("\"%s\":\"%.*s\"", group_by_field, vl, val_buf);
        } else if (ctx.gb_tf) {
            /* Numeric index key — decode to double and format as quoted
             * string to match the existing IGB/scan-path output shape
             * where group_vals are always emitted as JSON strings. */
            double dv = 0.0;
            if (decode_index_key_to_double(ctx.gb_tf, (const uint8_t *)gkeys[i],
                                            gklens[i], &dv)) {
                char dbuf[64];
                fmt_double(dbuf, sizeof(dbuf), dv);
                OUT("\"%s\":\"%s\"", group_by_field, dbuf);
            } else {
                OUT("\"%s\":null", group_by_field);
            }
        } else {
            /* Unknown type — emit raw bytes as string (safe for ASCII). */
            vl = (int)gklens[i];
            if (vl > (int)(sizeof(val_buf) - 1)) vl = (int)(sizeof(val_buf) - 1);
            memcpy(val_buf, gkeys[i], (size_t)vl);
            val_buf[vl] = '\0';
            OUT("\"%s\":\"%.*s\"", group_by_field, vl, val_buf);
        }
```

Replace with:

```c
        /* Emit the group_by field value. */
        char val_buf[1032];
        int  vl = 0;
        if (gb_is_varchar) {
            /* Index stores raw string content for varchar. */
            vl = (int)gklens[i];
            if (vl > (int)(sizeof(val_buf) - 1)) vl = (int)(sizeof(val_buf) - 1);
            memcpy(val_buf, gkeys[i], (size_t)vl);
            val_buf[vl] = '\0';
            char *esc = json_escape_const(val_buf);
            OUT("\"%s\":\"%s\"", group_by_field, esc ? esc : "");
            free(esc);
        } else if (ctx.gb_tf) {
            /* Numeric index key — decode to double and format as quoted
             * string to match the existing IGB/scan-path output shape
             * where group_vals are always emitted as JSON strings. */
            double dv = 0.0;
            if (decode_index_key_to_double(ctx.gb_tf, (const uint8_t *)gkeys[i],
                                            gklens[i], &dv)) {
                char dbuf[64];
                fmt_double(dbuf, sizeof(dbuf), dv);
                OUT("\"%s\":\"%s\"", group_by_field, dbuf);
            } else {
                OUT("\"%s\":null", group_by_field);
            }
        } else {
            /* Unknown type — emit raw bytes as string (safe for ASCII). */
            vl = (int)gklens[i];
            if (vl > (int)(sizeof(val_buf) - 1)) vl = (int)(sizeof(val_buf) - 1);
            memcpy(val_buf, gkeys[i], (size_t)vl);
            val_buf[vl] = '\0';
            char *esc = json_escape_const(val_buf);
            OUT("\"%s\":\"%s\"", group_by_field, esc ? esc : "");
            free(esc);
        }
```

## Task 15 — aggregate standard bucket path: escape group_vals

This is the path used by both the full-scan aggregate and the post-merge
indexed-group-by output. `b->group_vals[g]` is an arena-owned C string
(never individually freed — owned by `ctx->arena`), raw content straight
from `decode_field`/`typed_field_to_buf_raw`.

Anchor:

```c
            for (int g = 0; g < ctx.ngroups; g++) {
                if (!first) OUT(",");
                OUT("\"%s\":\"%s\"", ctx.group_fields[g], b->group_vals[g]);
                first = 0;
            }
```

Replace with:

```c
            for (int g = 0; g < ctx.ngroups; g++) {
                if (!first) OUT(",");
                char *gv = json_escape_const(b->group_vals[g]);
                OUT("\"%s\":\"%s\"", ctx.group_fields[g], gv ? gv : "");
                free(gv);
                first = 0;
            }
```

## Task 16 — harden `valid_filename()` against embedded quotes

`valid_filename()` (src/db/util.c) already rejects `/`, `\`, control
characters, and `..`, but not `"`. Since stored filenames are echoed back
verbatim in `put-file`/`get-file`/`delete-file`/`get-file-path` JSON
responses, a filename containing `"` breaks those responses the same way an
unescaped varchar field does. Reject it at the validation boundary so no
new file can be stored with a JSON-breaking name.

Anchor:

```c
int valid_filename(const char *name) {
    if (!name || !name[0]) return 0;
    size_t n = strlen(name);
    if (n > 255) return 0;
    if (name[0] == '.' && (n == 1 || (n == 2 && name[1] == '.'))) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)name[i];
        if (c == '/' || c == '\\' || c < 0x20 || c == 0x7F) return 0;
    }
```

Replace with:

```c
int valid_filename(const char *name) {
    if (!name || !name[0]) return 0;
    size_t n = strlen(name);
    if (n > 255) return 0;
    if (name[0] == '.' && (n == 1 || (n == 2 && name[1] == '.'))) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)name[i];
        if (c == '/' || c == '\\' || c == '"' || c < 0x20 || c == 0x7F) return 0;
    }
```

This protects every *future* `put-file` call (and, transitively, the
`filename`-echoing `OUT()` sites in `cmd_put_file_b64`, `cmd_get_file_b64`,
and `cmd_delete_file`, all of which call `valid_filename` before emitting
`filename`). It does **not** protect files already stored on disk with a
bad name before this fix ships — `list-files` (Task 18) handles that case
separately since it reads names directly off the filesystem and can't
reject what's already there.

## Task 17 — validate filename in `cmd_get_file_path`

Unlike `cmd_put_file_b64`/`cmd_get_file_b64`/`cmd_delete_file`,
`cmd_get_file_path` currently has no `valid_filename` check at all, so it
echoes the raw request `filename` straight into JSON with zero validation.

Anchor:

```c
int cmd_get_file_path(const char *db_root, const char *object, const char *filename) {
    OUT("{\"path\":\"%s/%s/files/%s\"}\n", db_root, object, filename);
    return 0;
}
```

Replace with:

```c
int cmd_get_file_path(const char *db_root, const char *object, const char *filename) {
    if (!valid_filename(filename)) {
        OUT("{\"error\":\"invalid filename\"}\n");
        return 1;
    }
    OUT("{\"path\":\"%s/%s/files/%s\"}\n", db_root, object, filename);
    return 0;
}
```

## Task 18 — escape filenames in `list-files` output

`list-files` reads filenames directly off the filesystem (`readdir`), so it
can encounter names stored before Task 16 shipped (or placed on disk by any
other means) that contain `"`. Escape at emission rather than relying on
upstream validation.

Anchor:

```c
    /* Emit page [offset, offset+limit) plus the unfiltered total. */
    OUT("{\"files\":[");
    int emitted = 0;
    for (size_t i = (size_t)offset; i < count && emitted < limit; i++) {
        if (emitted) OUT(",");
        OUT("\"%s\"", names[i]);
        emitted++;
    }
```

Replace with:

```c
    /* Emit page [offset, offset+limit) plus the unfiltered total. */
    OUT("{\"files\":[");
    int emitted = 0;
    for (size_t i = (size_t)offset; i < count && emitted < limit; i++) {
        if (emitted) OUT(",");
        char *esc = json_escape_const(names[i]);
        OUT("\"%s\"", esc ? esc : names[i]);
        free(esc);
        emitted++;
    }
```

## Task 19 — audit for any remaining unescaped aggregate/file sites

Run:

```bash
grep -n "group_vals\[\|gkeys\[i\]" src/db/query.c
grep -n "OUT(.*filename" src/db/query.c src/db/server.c
```

Confirm every `OUT(...)` hit either already routes through
`json_escape_const`/`json_escape_field` (post Task 14/15/18) or is a
`csv_emit_cell` call (CSV path, leave alone — line ~27185's
`csv_emit_cell(b->group_vals[g], csv_delim)` is correct as-is). If you find
a JSON-emitting site not covered, fix it the same way and note it in
`PLAN_NOTES.md`.

## Task 20 — tests

Add to `src/test/cases/test_json_escape.c` (the test file Task 1–12 already
added/extended) — follow that file's existing setup/teardown and assertion
helper conventions:

1. **Aggregate group_by escaping**: create an object with a varchar field
   (e.g. `category:varchar:64`), insert at least two records where
   `category` contains an embedded `"` (e.g. `He said "hi"` and `Plain`).
   Run `{"mode":"aggregate","aggregates":[{"fn":"count","alias":"n"}],
   "group_by":["category"]}` and assert the raw response parses as valid
   JSON (use the same JSON-validity assertion pattern already in this file
   for the find/fetch cases) and that one group's `category` value
   round-trips to the original unescaped string after parsing.
2. **Aggregate top-N escaping**: same data, run the aggregate with
   `"order_by":"n","limit":10` added (forces the top-N heap path at Task 14)
   and assert valid JSON the same way.
3. **valid_filename rejects embedded quotes**: call `put-file` with a
   filename containing `"` (e.g. `bad"name.txt`) and assert the response is
   `{"error":"invalid filename"}`.
4. **list-files escapes pre-existing bad filenames**: since `put-file` now
   rejects quoted filenames (Task 16), simulate a pre-existing bad file by
   creating it directly on disk (bypass the API — e.g. `mkdir -p
   <db_root>/<dir>/<object>/files && touch '<...>/bad"name.txt'` from the
   test, mirroring how this test file already manages tmpdirs/daemons), then
   call `list-files` and assert the raw response parses as valid JSON and
   contains a file entry equal to `bad"name.txt` after parsing.

## Updated verification

In addition to the original Verification section:

1. `SKIP_TESTS=1 ./build.sh` and `./build/bin/shard-db-test run-all` — must
   still print `# total: N passed, 0 failed` (N now includes the Task 20
   cases).
2. Manual aggregate check:

```bash
$BIN query '{"mode":"create-object","dir":"default","object":"agg_t","splits":8,"max_key":16,"fields":["category:varchar:64"]}'
$BIN query '{"mode":"insert","dir":"default","object":"agg_t","key":"1","value":{"category":"He said \"hi\""}}'
$BIN query '{"mode":"insert","dir":"default","object":"agg_t","key":"2","value":{"category":"Plain"}}'
$BIN query '{"mode":"aggregate","dir":"default","object":"agg_t","aggregates":[{"fn":"count","alias":"n"}],"group_by":["category"]}' | python3 -c "import json,sys; json.load(sys.stdin); print('VALID JSON')"
```

3. Manual filename check:

```bash
$BIN query '{"mode":"put-file","dir":"default","object":"agg_t","filename":"bad\"name.txt","data":"aGVsbG8="}'
# expect: {"error":"invalid filename"}
```

Report the actual output of all manual checks — do not claim success
without it.
