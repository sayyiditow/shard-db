# PLAN_NOTES.md — 2026-06-10-json-bool-flags.md

## util.c anchor mismatch

**Anchor specified:**
```
    if (val)  *val  = v;
    if (vlen) *vlen = vl;
    return 1;
}

char *json_obj_strdup(const JsonObj *o, const char *key) {
```

**Actual code:**
`json_obj_unquoted` ends at line 344 (`}`). Between it and `json_obj_strdup` (line 445) there are three intermediate functions: `hex_nibble_u8` (347-352), `json_unescape_string` (354-414), `json_obj_strdup_unescaped` (416-423), `json_obj_int` (425-432), and `json_obj_copy` (434-443).

**Action taken:**
Inserted `json_obj_is_true` between `json_obj_copy` (ends line 443) and `json_obj_strdup` (starts line 445) — the most natural location adjacent to the other json helper utilities.
