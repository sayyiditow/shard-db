# R5b evidence — 2026-11-03-r5b-packed-recompose-plan.md

## Task 0 — baseline and branch

1. `git status --short` (on `fix/type-correctness-recovery`, HEAD b1cc3b7):
```
 M docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md
?? docs/plans/2026-11-03-r5b-packed-recompose-plan.md
?? docs/plans/2026-11-03-r5b-plan-review.md
```
   Branch created: `git checkout -b feat/r5b-packed-recompose` → `feat/r5b-packed-recompose`.
2. §3.0 inventory grep — every hit in the table (20 hits: defs/decls + rebuild_object callers
   `config.c:4777` (cmd_add_fields), `query_maint.c:193` (cmd_vacuum), `rebuild_object` ×2 →
   `rebuild_object_v2` at `query_find.c:1960` (verbatim packed copy) and `:2043` (legacy recompose),
   `cmd_edit_fields` ×1 at `query_schema.c:934`; `parse_field_line` callers `query_schema.c:645`,
   `query_find.c:1990` (legacy rebuild only); `packed_schema_same` def `query_find.c:1435` + sole
   caller `layout_migrate_object` `:1598`; `validate_packed_field_type` def `:990` + sole caller
   `cmd_create_object` `:1153`; `rewrite_fields_conf_for_edit` def `:232` + `edit_finalize_metadata`
   `:594` + no-rebuild path `:865`). No extra callers → no halt.
3. `SKIP_TESTS=1 ./build.sh` tail:
```
  -> build/bin/libshard-db.a + build/bin/shard_db.h
Built: build/bin/
```
   Warning count across the full build: **0**.

## Task 1 — RED (tests first, no production change)

`SKIP_TESTS=1 ./build.sh` after adding the three test files/build registration: no errors, no
warnings (`grep -E "error:|warning:"` empty). Individual case runs below, verbatim.

### $ ./build/bin/shard-db-test run test-packed-add-field
# test-packed-add-field
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
not ok 4 - add-field succeeds on a packed object
#   '"status":"rebuilt"' not found in '{"error":"add-field is not supported on packed-layout objects yet"}
'
ok 5 - old row k1 intact, new nullable field without default reads NULL (A-1)
ok 6 - old row k1 intact, new nullable field without default reads NULL (A-1)
ok 7 - bare object response
not ok 8 - old row k1 intact, new nullable field without default reads NULL (A-1)
#   '{"f1":1,"f2":null,"f3":null,"f4":null,"f5":null,"f6":null,"f7":"hi","f8":1.50,"f9":null}' not found in '{"f1":1,"f2":null,"f3":null,"f4":null,"f5":null,"f6":null,"f7":"hi","f8":1.50}
'
ok 9 - NULLs preserved across the bitmap resize
ok 10 - NULLs preserved across the bitmap resize
ok 11 - bare object response
not ok 12 - NULLs preserved across the bitmap resize
#   '{"f1":2,"f2":null,"f3":null,"f4":null,"f5":null,"f6":null,"f7":null,"f8":null,"f9":null}' not found in '{"f1":2,"f2":null,"f3":null,"f4":null,"f5":null,"f6":null,"f7":null,"f8":null}
'
ok 13 - 9-field insert works
not ok 14 - fields.conf has the appended line
#   assertion failed: c && strstr(c, "f9:int")
ok 15 - record count
ok 16 - existing index still answers after add-field
ok 17 - no transaction residue
ok 18 - layout token preserved
# test-packed-add-field: 14 passed, 4 failed
exit=0
### $ ./build/bin/shard-db-test run test-packed-add-field-grammar
# test-packed-add-field-grammar
ok 1 - create packed object
ok 2 - insert
not ok 3 - bare varchar/numeric, currency, numeric(10,3), numeric:12 (scale 0)
#   '"status":"rebuilt"' not found in '{"error":"add-field is not supported on packed-layout objects yet"}
'
not ok 4 - default-less nullable bare varchar/numerics read NULL on old rows (A-1)
#   assertion failed: g && strstr(g, "\"note\":null") && strstr(g, "\"amt\":null") && strstr(g, "\"w\":null")
not ok 5 - add-field with a default
#   '"status":"rebuilt"' not found in '{"error":"add-field is not supported on packed-layout objects yet"}
'
not ok 6 - old row backfilled with the declared default
#   assertion failed: g && strstr(g, "\"lvl\":7")
ok 7 - NOT NULL without a default is refused when rows exist
not ok 8 - refusal names the cause
#   'NOT NULL and has no default' not found in '{"error":"add-field is not supported on packed-layout objects yet"}
'
ok 9 - fields.conf unchanged after the refusal
not ok 10 - NOT NULL with a default is accepted
#   '"status":"rebuilt"' not found in '{"error":"add-field is not supported on packed-layout objects yet"}
'
ok 11 - varchar:0 refused
ok 12 - numeric:0 refused
ok 13 - varchar:4194305 refused
ok 14 - duplicate refused
ok 15 - refusals leave fields.conf untouched
# test-packed-add-field-grammar: 9 passed, 6 failed
exit=0
### $ ./build/bin/shard-db-test run test-packed-add-field-defaults
# test-packed-add-field-defaults
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
ok 4 - insert
ok 5 - insert
ok 6 - insert
ok 7 - insert
ok 8 - insert
ok 9 - insert
ok 10 - insert
ok 11 - insert
ok 12 - insert
ok 13 - insert
ok 14 - insert
ok 15 - insert
ok 16 - insert
ok 17 - insert
ok 18 - insert
ok 19 - insert
ok 20 - insert
ok 21 - insert
ok 22 - insert
ok 23 - insert
ok 24 - insert
ok 25 - insert
ok 26 - insert
ok 27 - insert
ok 28 - insert
ok 29 - insert
ok 30 - insert
ok 31 - insert
ok 32 - insert
ok 33 - insert
ok 34 - insert
ok 35 - insert
ok 36 - insert
ok 37 - insert
ok 38 - insert
ok 39 - insert
ok 40 - insert
ok 41 - insert
not ok 42 - add-field with every default kind
#   '"status":"rebuilt"' not found in '{"error":"add-field is not supported on packed-layout objects yet"}
'
not ok 43 - literal defaults stamped
#   assertion failed: g && strstr(g, "\"lit\":7") && strstr(g, "\"txt\":\"hey\"")
not ok 44 - rid present
#   assertion failed: s != NULL
not ok 45 - rid present
#   assertion failed: s != NULL
not ok 46 - rid present
#   assertion failed: s != NULL
not ok 47 - rid present
#   assertion failed: s != NULL
not ok 48 - rid present
#   assertion failed: s != NULL
not ok 49 - rid present
#   assertion failed: s != NULL
not ok 50 - rid present
#   assertion failed: s != NULL
not ok 51 - rid present
#   assertion failed: s != NULL
not ok 52 - rid present
#   assertion failed: s != NULL
not ok 53 - rid present
#   assertion failed: s != NULL
not ok 54 - rid present
#   assertion failed: s != NULL
not ok 55 - rid present
#   assertion failed: s != NULL
not ok 56 - rid present
#   assertion failed: s != NULL
not ok 57 - rid present
#   assertion failed: s != NULL
not ok 58 - rid present
#   assertion failed: s != NULL
not ok 59 - rid present
#   assertion failed: s != NULL
not ok 60 - rid present
#   assertion failed: s != NULL
not ok 61 - rid present
#   assertion failed: s != NULL
not ok 62 - rid present
#   assertion failed: s != NULL
not ok 63 - rid present
#   assertion failed: s != NULL
not ok 64 - rid present
#   assertion failed: s != NULL
not ok 65 - rid present
#   assertion failed: s != NULL
not ok 66 - rid present
#   assertion failed: s != NULL
not ok 67 - rid present
#   assertion failed: s != NULL
not ok 68 - rid present
#   assertion failed: s != NULL
not ok 69 - rid present
#   assertion failed: s != NULL
not ok 70 - rid present
#   assertion failed: s != NULL
not ok 71 - rid present
#   assertion failed: s != NULL
not ok 72 - rid present
#   assertion failed: s != NULL
not ok 73 - rid present
#   assertion failed: s != NULL
not ok 74 - rid present
#   assertion failed: s != NULL
not ok 75 - rid present
#   assertion failed: s != NULL
not ok 76 - rid present
#   assertion failed: s != NULL
not ok 77 - rid present
#   assertion failed: s != NULL
not ok 78 - rid present
#   assertion failed: s != NULL
not ok 79 - rid present
#   assertion failed: s != NULL
not ok 80 - rid present
#   assertion failed: s != NULL
not ok 81 - rid present
#   assertion failed: s != NULL
not ok 82 - rid present
#   assertion failed: s != NULL
not ok 83 - rid present
#   assertion failed: s != NULL
not ok 84 - seq values are exactly 1..40 (order = shard scan order, documented)
#   expected 820 got 0
ok 85 - insert after backfill
not ok 86 - sequence resumes at start+live_count
#   assertion failed: g && strstr(g, "\"rid\":41")
not ok 87 - uuid() differs per row, dashed form
#   assertion failed: t0 && t1 && strncmp(t0, t1, 7 + 36) != 0 && t0[7 + 8] == '-'
not ok 88 - random(4) = 8 hex chars, differs per row
#   assertion failed: n0 && n1 && n0[9 + 8] == '"' && strncmp(n0, n1, 9 + 8) != 0
ok 89 - random(5) into varchar:8 refused
ok 90 - random into int refused
ok 91 - uuid() into varchar:20 refused
ok 92 - unstorable literal refused
ok 93 - refusals leave fields.conf untouched
ok 94 - no transaction residue
# test-packed-add-field-defaults: 48 passed, 46 failed
exit=0
### $ ./build/bin/shard-db-test run test-packed-add-field-atomic
# test-packed-add-field-atomic
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
ok 4 - insert
ok 5 - insert
ok 6 - insert
ok 7 - insert
ok 8 - insert
ok 9 - insert
ok 10 - insert
ok 11 - insert
ok 12 - insert
ok 13 - insert
ok 14 - insert
ok 15 - insert
ok 16 - insert
ok 17 - insert
ok 18 - insert
ok 19 - insert
ok 20 - insert
ok 21 - insert
ok 22 - insert
ok 23 - insert
ok 24 - insert
ok 25 - insert
ok 26 - insert
ok 27 - insert
ok 28 - insert
ok 29 - insert
ok 30 - insert
ok 31 - insert
ok 32 - insert
ok 33 - insert
ok 34 - insert
ok 35 - insert
ok 36 - insert
ok 37 - insert
ok 38 - insert
ok 39 - insert
ok 40 - insert
ok 41 - insert
ok 42 - insert
ok 43 - insert
ok 44 - insert
ok 45 - insert
ok 46 - insert
ok 47 - insert
ok 48 - insert
ok 49 - insert
ok 50 - insert
ok 51 - insert
ok 52 - insert
ok 53 - insert
ok 54 - insert
ok 55 - insert
ok 56 - insert
ok 57 - insert
ok 58 - insert
ok 59 - insert
ok 60 - insert
ok 61 - insert
ok 62 - insert
ok 63 - insert
ok 64 - insert
ok 65 - insert
ok 66 - insert
ok 67 - insert
ok 68 - insert
ok 69 - insert
ok 70 - insert
ok 71 - insert
ok 72 - insert
ok 73 - insert
ok 74 - insert
ok 75 - insert
ok 76 - insert
ok 77 - insert
ok 78 - insert
ok 79 - insert
ok 80 - insert
ok 81 - insert
ok 82 - insert
ok 83 - insert
ok 84 - insert
ok 85 - insert
ok 86 - insert
ok 87 - insert
ok 88 - insert
ok 89 - insert
ok 90 - insert
ok 91 - insert
ok 92 - insert
ok 93 - insert
ok 94 - insert
ok 95 - insert
ok 96 - insert
ok 97 - insert
ok 98 - insert
ok 99 - insert
ok 100 - insert
ok 101 - insert
ok 102 - insert
ok 103 - insert
ok 104 - insert
ok 105 - insert
ok 106 - insert
ok 107 - insert
ok 108 - insert
ok 109 - insert
ok 110 - insert
ok 111 - insert
ok 112 - insert
ok 113 - insert
ok 114 - insert
ok 115 - insert
ok 116 - insert
ok 117 - insert
ok 118 - insert
ok 119 - insert
ok 120 - insert
ok 121 - insert
ok 122 - insert
ok 123 - insert
ok 124 - insert
ok 125 - insert
ok 126 - insert
ok 127 - insert
ok 128 - insert
ok 129 - insert
ok 130 - insert
ok 131 - insert
ok 132 - insert
ok 133 - insert
ok 134 - insert
ok 135 - insert
ok 136 - insert
ok 137 - insert
ok 138 - insert
ok 139 - insert
ok 140 - insert
ok 141 - insert
ok 142 - insert
ok 143 - insert
ok 144 - insert
ok 145 - insert
ok 146 - insert
ok 147 - insert
ok 148 - insert
ok 149 - insert
ok 150 - insert
ok 151 - insert
ok 152 - insert
ok 153 - insert
ok 154 - insert
ok 155 - insert
ok 156 - insert
ok 157 - insert
ok 158 - insert
ok 159 - insert
ok 160 - insert
ok 161 - insert
ok 162 - insert
ok 163 - insert
ok 164 - insert
ok 165 - insert
ok 166 - insert
ok 167 - insert
ok 168 - insert
ok 169 - insert
ok 170 - insert
ok 171 - insert
ok 172 - insert
ok 173 - insert
ok 174 - insert
ok 175 - insert
ok 176 - insert
ok 177 - insert
ok 178 - insert
ok 179 - insert
ok 180 - insert
ok 181 - insert
ok 182 - insert
ok 183 - insert
ok 184 - insert
ok 185 - insert
ok 186 - insert
ok 187 - insert
ok 188 - insert
ok 189 - insert
ok 190 - insert
ok 191 - insert
ok 192 - insert
ok 193 - insert
ok 194 - insert
ok 195 - insert
ok 196 - insert
ok 197 - insert
ok 198 - insert
ok 199 - insert
ok 200 - insert
ok 201 - insert
ok 202 - insert
ok 203 - insert
ok 204 - insert
ok 205 - insert
ok 206 - insert
ok 207 - insert
ok 208 - insert
ok 209 - insert
ok 210 - insert
ok 211 - insert
ok 212 - insert
ok 213 - insert
ok 214 - insert
ok 215 - insert
ok 216 - insert
ok 217 - insert
ok 218 - insert
ok 219 - insert
ok 220 - insert
ok 221 - insert
ok 222 - insert
ok 223 - insert
ok 224 - insert
ok 225 - insert
ok 226 - insert
ok 227 - insert
ok 228 - insert
ok 229 - insert
ok 230 - insert
ok 231 - insert
ok 232 - insert
ok 233 - insert
ok 234 - insert
ok 235 - insert
ok 236 - insert
ok 237 - insert
ok 238 - insert
ok 239 - insert
ok 240 - insert
ok 241 - insert
ok 242 - insert
ok 243 - insert
ok 244 - insert
ok 245 - insert
ok 246 - insert
ok 247 - insert
ok 248 - insert
ok 249 - insert
ok 250 - insert
ok 251 - insert
ok 252 - insert
ok 253 - insert
ok 254 - insert
ok 255 - insert
ok 256 - insert
ok 257 - insert
ok 258 - insert
ok 259 - insert
ok 260 - insert
ok 261 - insert
ok 262 - insert
ok 263 - insert
ok 264 - insert
ok 265 - insert
ok 266 - insert
ok 267 - insert
ok 268 - insert
ok 269 - insert
ok 270 - insert
ok 271 - insert
ok 272 - insert
ok 273 - insert
ok 274 - insert
ok 275 - insert
ok 276 - insert
ok 277 - insert
ok 278 - insert
ok 279 - insert
ok 280 - insert
ok 281 - insert
ok 282 - insert
ok 283 - insert
ok 284 - insert
ok 285 - insert
ok 286 - insert
ok 287 - insert
ok 288 - insert
ok 289 - insert
ok 290 - insert
ok 291 - insert
ok 292 - insert
ok 293 - insert
ok 294 - insert
ok 295 - insert
ok 296 - insert
ok 297 - insert
ok 298 - insert
ok 299 - insert
ok 300 - insert
ok 301 - insert
ok 302 - seq overflowing a byte refuses the whole add
ok 303 - fields.conf unchanged
ok 304 - count unchanged
ok 305 - every row byte-identical after the refused add
ok 306 - transaction fully rolled back
ok 307 - index still consistent
ok 308 - create packed object
not ok 309 - 256th field accepted
#   '"status":"rebuilt"' not found in '{"error":"add-field is not supported on packed-layout objects yet"}
'
ok 310 - 257th field refused
# test-packed-add-field-atomic: 309 passed, 1 failed
exit=1
### $ ./build/bin/shard-db-test run test-packed-edit-field-varchar
# test-packed-edit-field-varchar
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
not ok 4 - grow rebuilds
#   '"rebuilt":true' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 5 - value intact after grow
not ok 6 - shrink below an existing value refused by the pre-flight
#   'Pre-flight failed on field [name]' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
not ok 7 - dry_run predicts the same refusal
#   'Pre-flight failed on field [name]' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 8 - refused shrink changed nothing
not ok 9 - shrink to exactly the longest value is allowed
#   '"rebuilt":true' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
not ok 10 - bounded -> bare
#   '"status":"edited"' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
not ok 11 - bare -> bounded when all rows fit
#   '"status":"edited"' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 12 - bare -> too-small bounded refused
ok 13 - count
ok 14 - varchar index rebuilt and consistent
not ok 15 - default-only edit is metadata only
#   '"rebuilt":false' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 16 - no residue
# test-packed-edit-field-varchar: 9 passed, 7 failed
exit=7
### $ ./build/bin/shard-db-test run test-packed-edit-field-numeric
# test-packed-edit-field-numeric
ok 1 - create packed object
ok 2 - create packed object
ok 3 - insert
ok 4 - insert
ok 5 - insert
ok 6 - insert
ok 7 - insert
ok 8 - insert
ok 9 - insert
ok 10 - insert
not ok 11 - numeric(12,4) -> numeric(12,2)
#   '"rebuilt":true' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
not ok 12 - 1.0050 rounds to 1.01
#   assertion failed: g && strstr(g, "1.01") && !strstr(g, "1.0050")
not ok 13 - -1.0050 rounds to -1.01 (ties away from zero)
#   assertion failed: g && strstr(g, "-1.01")
ok 14 - 2.5000 -> 2.50
not ok 15 - index rebuilt: eq on the rounded value
#   'k1' not found in '[]
'
not ok 16 - dry_run predicts the overflow refusal
#   'Pre-flight failed on field [a]' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
not ok 17 - overflow after rounding refused by the pre-flight
#   'Pre-flight failed on field [a]' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 18 - fields.conf unchanged
ok 19 - every row (including the ones that would have fit) is unchanged
ok 20 - every row (including the ones that would have fit) is unchanged
ok 21 - every row (including the ones that would have fit) is unchanged
ok 22 - every row (including the ones that would have fit) is unchanged
not ok 23 - bounded -> bare
#   '"status":"edited"' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
not ok 24 - bare -> currency (numeric(19,4))
#   '"status":"edited"' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 25 - count
ok 26 - no residue
# test-packed-edit-field-numeric: 18 passed, 8 failed
exit=8
### $ ./build/bin/shard-db-test run test-packed-edit-field-fixed
# test-packed-edit-field-fixed
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
not ok 4 - int->long, float->double
#   '"rebuilt":true' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 5 - negatives sign-extend; float widens exactly
ok 6 - narrow refused: 70000 does not fit a short
not ok 7 - short->int
#   '"rebuilt":true' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
not ok 8 - enum append (same width) is metadata only
#   '"rebuilt":false' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 9 - rename without allow_rename refused
not ok 10 - enum 1B -> 2B widen rebuilds
#   '"rebuilt":true' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 11 - enum indexes preserved through the widen
ok 12 - NULL row still readable
# test-packed-edit-field-fixed: 8 passed, 4 failed
exit=4
### $ ./build/bin/shard-db-test run test-packed-edit-field-gates
# test-packed-edit-field-gates
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
ok 4 - adding :not_null over a NULL row is refused
not ok 5 - adding :not_null when no NULL exists succeeds
#   '"status":"edited"' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
not ok 6 - fields.conf carries the constraint
#   assertion failed: c && strstr(c, "a:int:not_null")
not ok 7 - dry_run success
#   '"dry_run":true' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 8 - dry_run wrote nothing
# test-packed-edit-field-gates: 5 passed, 3 failed
exit=3
### $ ./build/bin/shard-db-test run test-packed-bitmap-after-rebuild
# test-packed-bitmap-after-rebuild
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
ok 4 - insert
ok 5 - insert
ok 6 - insert
ok 7 - insert
ok 8 - insert
ok 9 - insert
ok 10 - insert
ok 11 - insert
ok 12 - insert
ok 13 - insert
ok 14 - insert
ok 15 - insert
ok 16 - insert
ok 17 - insert
ok 18 - insert
ok 19 - insert
ok 20 - insert
ok 21 - insert
ok 22 - insert
ok 23 - insert
ok 24 - insert
ok 25 - insert
ok 26 - insert
ok 27 - insert
ok 28 - insert
ok 29 - insert
ok 30 - insert
ok 31 - insert
ok 32 - delete
ok 33 - delete
ok 34 - delete
ok 35 - delete
ok 36 - delete
ok 37 - delete
ok 38 - delete
ok 39 - delete
ok 40 - delete
ok 41 - delete
not ok 42 - recompose
#   '"status"' not found in '{"error":"add-field is not supported on packed-layout objects yet"}
'
ok 43 - bool bitmap count stable after recompose
ok 44 - enum bitmap count stable after recompose
not ok 45 - recompose
#   '"status"' not found in '{"error":"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet"}
'
ok 46 - bool bitmap count stable after recompose
ok 47 - enum bitmap count stable after recompose
not ok 48 - recompose
#   '"status"' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
ok 49 - bool bitmap count stable after recompose
ok 50 - enum bitmap count stable after recompose
# test-packed-bitmap-after-rebuild: 47 passed, 3 failed
exit=3
### $ ./build/bin/shard-db-test run test-packed-edit-field-tombstone-multi
# test-packed-edit-field-tombstone-multi
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
ok 4 - tombstone
not ok 5 - multi-field edit incl. rename
#   '"rebuilt":true' not found in '{"error":"edit-field is not supported on packed-layout objects yet"}
'
not ok 6 - values converted, rename relabels, tombstone stays hidden
#   assertion failed: g && strstr(g, "1.23") && strstr(g, "\"e\":\"z\"") && !strstr(g, "\"gone\"")
ok 7 - NULL numeric row readable
ok 8 - tombstoned field cannot be edited
not ok 9 - add-field with a :not_null field next to a tombstone
#   '"status":"rebuilt"' not found in '{"error":"add-field is not supported on packed-layout objects yet"}
'
not ok 10 - tombstone line kept, new line appended
#   assertion failed: c && strstr(c, "gone:int:removed") && strstr(c, "nn:int:not_null")
# test-packed-edit-field-tombstone-multi: 6 passed, 4 failed
exit=4
### $ ./build/bin/shard-db-test run test-packed-compact
# test-packed-compact
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
ok 4 - insert
ok 5 - insert
ok 6 - insert
ok 7 - insert
ok 8 - insert
not ok 9 - compact with no tombstones still rebuilds
#   '"status":"rebuilt"' not found in '{"error":"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet"}
'
ok 10 - delete k2
ok 11 - delete k3
ok 12 - two deleted records pending
ok 13 - tombstone c
not ok 14 - compact reports compact
#   '"compact":true' not found in '{"error":"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet"}
'
not ok 15 - slot_size shrinks when a field is dropped
#   assertion failed: s1 && atoi(s1 + 12) < slot_before
not ok 16 - tombstoned line dropped from fields.conf
#   assertion failed: conf && !strstr(conf, ":removed") && !strstr(conf, "c:numeric")
not ok 17 - deleted-record space reclaimed
#   expected 0 got 2
ok 18 - live rows preserved
ok 19 - row intact, dropped field gone
ok 20 - NULL row survives the bitmap shrink
ok 21 - index preserved and consistent
not ok 22 - a name freed by compact can be re-added
#   '"status":"rebuilt"' not found in '{"error":"add-field is not supported on packed-layout objects yet"}
'
not ok 23 - compact + splits
#   '"splits":16' not found in '{"error":"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet"}
'
not ok 24 - splits changed, layout token preserved
#   assertion failed: sconf && strstr(sconf, "default:o:16:") && strstr(sconf, "layout=packed")
ok 25 - count after compact+splits
ok 26 - index consistent after reshard
ok 27 - no residue
# test-packed-compact: 19 passed, 8 failed
exit=8
### $ ./build/bin/shard-db-test run test-compact-removed-literal
# test-compact-removed-literal
ok 1 - create
ok 2 - insert
ok 3 - tombstone gone
not ok 4 - compact
#   '"status":"rebuilt"' not found in '{"error":"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet"}
'
not ok 5 - live line kept verbatim, tombstoned line dropped
#   assertion failed: conf && strstr(conf, "note:varchar:20:default=a:removed:x") && !strstr(conf, "gone:")
ok 6 - row readable
ok 7 - create
ok 8 - insert
ok 9 - tombstone gone
ok 10 - compact
not ok 11 - live line kept verbatim, tombstoned line dropped
#   assertion failed: conf && strstr(conf, "note:varchar:20:default=a:removed:x") && !strstr(conf, "gone:")
not ok 12 - row readable
#   assertion failed: g && strstr(g, "\"note\":\"hello\"")
# test-compact-removed-literal: 8 passed, 4 failed
exit=4
### $ ./build/bin/shard-db-test run test-packed-recompose-ops-supported
# test-packed-recompose-ops-supported
ok 1 - mkdtemp layout root
ok 2 - mkdir db_root
ok 3 - pick port
ok 4 - write db.env
shard-db listening on port 47031 (pid=688842, workers=16, timeout=0s, tls=off)
ok 5 - start
not ok 6 - add-field succeeds on a packed object
#   assertion failed: a && strstr(a, "\"status\":\"rebuilt\"")
not ok 7 - edit-field succeeds on a packed object
#   assertion failed: e && strstr(e, "\"status\":\"edited\"")
not ok 8 - compact succeeds on a packed object
#   assertion failed: c && strstr(c, "\"status\":\"rebuilt\"")
ok 9 - vacuum splits
ok 10 - splits change allowed
ok 11 - get
ok 12 - record intact after verbatim reshard
shard-db stopped (pid=688842)
ok 13 - reshard preserved the layout token
# test-packed-recompose-ops-supported: 10 passed, 3 failed
exit=3
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-add-after-stage
# test-rebuild-txn-packed-add-after-stage
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
ok 10 - rollback restores fields.conf byte-for-byte
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-add-after-walk
# test-rebuild-txn-packed-add-after-walk
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
ok 10 - rollback restores fields.conf byte-for-byte
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-add-after-metadata
# test-rebuild-txn-packed-add-after-metadata
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
ok 10 - rollback restores fields.conf byte-for-byte
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-add-after-commit
# test-rebuild-txn-packed-add-after-commit
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
not ok 10 - committed add-field is complete
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-edit-after-stage
# test-rebuild-txn-packed-edit-after-stage
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
ok 10 - rollback restores fields.conf byte-for-byte
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-edit-after-walk
# test-rebuild-txn-packed-edit-after-walk
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
ok 10 - rollback restores fields.conf byte-for-byte
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-edit-after-metadata
# test-rebuild-txn-packed-edit-after-metadata
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
ok 10 - rollback restores fields.conf byte-for-byte
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-edit-after-commit
# test-rebuild-txn-packed-edit-after-commit
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
not ok 10 - committed edit-field is complete, index rebuilt (7.1250 -> 7.13)
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-compact-after-stage
# test-rebuild-txn-packed-compact-after-stage
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
ok 10 - rollback restores fields.conf byte-for-byte
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-compact-after-walk
# test-rebuild-txn-packed-compact-after-walk
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
ok 10 - rollback restores fields.conf byte-for-byte
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-compact-after-metadata
# test-rebuild-txn-packed-compact-after-metadata
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
ok 10 - rollback restores fields.conf byte-for-byte
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-compact-after-commit
# test-rebuild-txn-packed-compact-after-commit
ok 1 - create packed crash fixture
ok 2 - baseline captured
ok 3 - enable pause
ok 4 - restart with pause hook
ok 5 - spawn request
not ok 6 - rebuild reaches the pause
#   expected 0 got -1
ok 7 - restart recovers
ok 8 - all rows present
ok 9 - no active transaction after recovery
not ok 10 - committed compact is complete
RED summary (all expected, none GREEN on the unmodified tree → no halt):
- `test-packed-add-field`, `-grammar`, `-defaults`, `-atomic`, `-edit-field-*`, `-bitmap-after-rebuild`,
  `-compact`, `test-packed-recompose-ops-supported`: every failing assertion's response is
  `{"error":"... not supported on packed-layout objects yet"}` (or, for the refusal-assertions
  that only require `"error"`, they pass trivially in RED and stay green post-fix).
- `test-compact-removed-literal`: packed half → compact refused (RED); **legacy half → compact
  succeeds but the live `note:varchar:20:default=a:removed:x` line is DROPPED from fields.conf
  (not ok 11) and the row is no longer readable (not ok 12)** — this is the A-4 latent data-loss
  bug; pasted above as the regression proof for Task 4 / H5.
- 12 `test-rebuild-txn-packed-*`: all fail at `not ok 6 - rebuild reaches the pause` (20 s
  wait_for_path timeout — the request is refused before any rebuild starts). On the three
  `after-commit` cases the additional `not ok 10` ("committed ... is complete") is the same root
  cause downstream: no rebuild ever ran, so there is no committed state to verify.

## Task 2 — shared machinery + add-field (D-2, D-4)

Applied: §3.1, §3.2a–f, §3.3 H1–H6, §3.4 H7 (+ `rebuild_object_packed`), §3.5 H8–H9,
§3.6 H10–H11, §3.8 H15 and H17. `SKIP_TESTS=1 ./build.sh`: **zero warnings**
(`grep -E "error:|warning:"` empty, twice).

### $ ./build/bin/shard-db-test run test-packed-add-field
ok 17 - no transaction residue
ok 18 - layout token preserved
# test-packed-add-field: 18 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-add-field-grammar
ok 14 - duplicate refused
ok 15 - refusals leave fields.conf untouched
# test-packed-add-field-grammar: 15 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-add-field-defaults
ok 93 - refusals leave fields.conf untouched
ok 94 - no transaction residue
# test-packed-add-field-defaults: 94 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-add-field-atomic
ok 309 - 256th field accepted
ok 310 - 257th field refused
# test-packed-add-field-atomic: 310 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-add-after-stage
ok 11 - rollback restores the original record
ok 12 - index consistent after rollback
shard-db stopped (pid=698887)
# test-rebuild-txn-packed-add-after-stage: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-add-after-walk
ok 11 - rollback restores the original record
ok 12 - index consistent after rollback
shard-db stopped (pid=699171)
# test-rebuild-txn-packed-add-after-walk: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-add-after-metadata
ok 11 - rollback restores the original record
ok 12 - index consistent after rollback
shard-db stopped (pid=699454)
# test-rebuild-txn-packed-add-after-metadata: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-add-after-commit
ok 9 - no active transaction after recovery
ok 10 - committed add-field is complete
shard-db stopped (pid=699740)
# test-rebuild-txn-packed-add-after-commit: 10 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-recompose-ops-supported   (add half green; edit/compact halves stay RED until Tasks 3-4)
ok 1 - mkdtemp layout root
ok 2 - mkdir db_root
ok 3 - pick port
ok 4 - write db.env
ok 5 - start
ok 6 - add-field succeeds on a packed object
not ok 7 - edit-field succeeds on a packed object
ok 8 - compact succeeds on a packed object
ok 9 - vacuum splits
ok 10 - splits change allowed
ok 11 - get
ok 12 - record intact after verbatim reshard
ok 13 - reshard preserved the layout token
# test-packed-recompose-ops-supported: 12 passed, 1 failed
### Invariant (a): git diff --stat
 build.sh                                           |   1 +
 ...26-11-02-r5-metadata-migration-recovery-plan.md |   7 +
 src/db/config.c                                    |  31 +-
 src/db/objlock.c                                   |   6 +
 src/db/query_find.c                                | 699 ++++++++++++++++++++-
 src/db/query_internal.h                            |   5 +
 src/db/query_schema.c                              |   3 +-
 src/db/types.h                                     |  14 +
 src/test/cases/test_layout_manifest.c              |  29 +-
 src/test/cases/test_rebuild_txn_recovery.c         | 150 +++++
 10 files changed, 889 insertions(+), 56 deletions(-)
### Invariant (b): mid-walk failure case = test-packed-add-field-atomic (310 passed, 0 failed above): fields.conf/rows/index unchanged, no residue — all its assertions passed.
### $ ./build/bin/shard-db-test run-all --filter add-field --jobs 4
ok 16 - existing index still answers after add-field
ok 17 - no transaction residue
ok 18 - layout token preserved
# test-packed-add-field: 18 passed, 0 failed
1..5
# total: 471 passed, 0 failed across 5 cases

## Task 3 — edit-field (D-1, D-3)

Applied §3.6 H12–H14 (H16 is inside H13's replacement). `SKIP_TESTS=1 ./build.sh`: **zero warnings**.

GREEN targets:
### $ ./build/bin/shard-db-test run test-packed-edit-field-varchar
ok 16 - no residue
# test-packed-edit-field-varchar: 16 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-edit-field-numeric
ok 26 - no residue
# test-packed-edit-field-numeric: 26 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-edit-field-fixed
ok 12 - NULL row still readable
# test-packed-edit-field-fixed: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-edit-field-gates
ok 8 - dry_run wrote nothing
# test-packed-edit-field-gates: 8 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-edit-field-tombstone-multi
#   assertion failed: c && strstr(c, "gone:int:removed") && strstr(c, "nn:int:not_null")
# test-packed-edit-field-tombstone-multi: 8 passed, 2 failed
### $ ./build/bin/shard-db-test run test-packed-bitmap-after-rebuild
ok 50 - enum bitmap count stable after recompose
# test-packed-bitmap-after-rebuild: 50 passed, 0 failed

**Finding during Task 3 GREEN (test bug, root-caused, not papered over):**
`test-packed-edit-field-tombstone-multi` expected `add-field ["nn:int:not_null","t:varchar:5"]`
to SUCCEED on an object holding rows k1/k2. That contradicts the plan's own human-decided A-1
(a `:not_null` field with no usable default is refused while the object has any record) — the
engine's refusal was correct: `{"error":"Rebuild walk failed: field 'nn' key 'k2': Field 'nn'
is NOT NULL and has no default; existing rows cannot be backfilled; original data restored"}`.
In the Task 1 RED run this assertion was never reached (the case died earlier at the packed
edit refusal), so the bad expectation surfaced only now. Fix (test-only): give the field the
default A-1 requires — `nn:int:default=5:not_null` — which preserves the case's intent
("a :not_null add next to a tombstone succeeds") and additionally exercises the
defaulted-:not_null backfill path. The bare-`:not_null` refusal itself stays covered by
`test-packed-add-field-grammar` ("NOT NULL without a default is refused when rows exist").

### \$ ./build/bin/shard-db-test run test-packed-edit-field-tombstone-multi  (after the test fix)
ok 8 - tombstoned field cannot be edited
ok 9 - add-field with a defaulted :not_null field next to a tombstone
ok 10 - tombstone line kept, new line appended
# test-packed-edit-field-tombstone-multi: 10 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-recompose-ops-supported  (add+edit green; compact half stays RED until Task 4)
ok 1 - mkdtemp layout root
ok 2 - mkdir db_root
ok 3 - pick port
ok 4 - write db.env
ok 5 - start
ok 6 - add-field succeeds on a packed object
ok 7 - edit-field succeeds on a packed object
ok 8 - compact succeeds on a packed object
ok 9 - vacuum splits
ok 10 - splits change allowed
ok 11 - get
ok 12 - record intact after verbatim reshard
ok 13 - reshard preserved the layout token
# test-packed-recompose-ops-supported: 13 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-edit-after-stage
shard-db stopped (pid=707868)
# test-rebuild-txn-packed-edit-after-stage: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-edit-after-walk
shard-db stopped (pid=708158)
# test-rebuild-txn-packed-edit-after-walk: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-edit-after-metadata
shard-db stopped (pid=708449)
# test-rebuild-txn-packed-edit-after-metadata: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-edit-after-commit
shard-db stopped (pid=708747)
# test-rebuild-txn-packed-edit-after-commit: 10 passed, 0 failed

### Revert proof 1 — packed NOT NULL gate (H12). Temporary edit: delete the
`if (c->ts && c->ts->packed.nfields) { … }` block in `not_null_scan_cb`. Rebuild, run:
### $ ./build/bin/shard-db-test run test-packed-edit-field-gates   (reverted gate)
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
ok 4 - adding :not_null over a NULL row is refused
not ok 5 - adding :not_null when no NULL exists succeeds
not ok 6 - fields.conf carries the constraint
#   assertion failed: c && strstr(c, "a:int:not_null")
ok 7 - dry_run success
ok 8 - dry_run wrote nothing
# test-packed-edit-field-gates: 6 passed, 2 failed
ok 4 - adding :not_null over a NULL row is refused
not ok 5 - adding :not_null when no NULL exists succeeds
not ok 6 - fields.conf carries the constraint
#   assertion failed: c && strstr(c, "a:int:not_null")
ok 7 - dry_run success
ok 8 - dry_run wrote nothing
# test-packed-edit-field-gates: 6 passed, 2 failed
(fails at 'adding :not_null when no NULL exists succeeds' + its fields.conf follow-up — the packed NULL scan reports broken → the gate refuses a legal edit)
Restore; rerun:
### $ ./build/bin/shard-db-test run test-packed-edit-field-gates   (restored)
ok 8 - dry_run wrote nothing
# test-packed-edit-field-gates: 8 passed, 0 failed

### Revert proof 2 — packed pre-flight (H14g). Temporary edit: the packed branch uses the
legacy `edit_preflight_walk_cb` walker (which skips packed records via `return 0` on
`typed_record_view` failure). Rebuild, run:
### $ ./build/bin/shard-db-test run test-packed-edit-field-numeric   (reverted pre-flight)
ok 1 - create packed object
ok 2 - create packed object
ok 3 - insert
ok 4 - insert
ok 5 - insert
ok 6 - insert
ok 7 - insert
ok 8 - insert
ok 9 - insert
ok 10 - insert
ok 11 - numeric(12,4) -> numeric(12,2)
ok 12 - 1.0050 rounds to 1.01
ok 13 - -1.0050 rounds to -1.01 (ties away from zero)
ok 14 - 2.5000 -> 2.50
ok 15 - index rebuilt: eq on the rounded value
not ok 16 - dry_run predicts the overflow refusal
not ok 17 - overflow after rounding refused by the pre-flight
ok 18 - fields.conf unchanged
ok 19 - every row (including the ones that would have fit) is unchanged
ok 20 - every row (including the ones that would have fit) is unchanged
ok 21 - every row (including the ones that would have fit) is unchanged
ok 22 - every row (including the ones that would have fit) is unchanged
ok 23 - bounded -> bare
ok 24 - bare -> currency (numeric(19,4))
ok 25 - count
ok 26 - no residue
# test-packed-edit-field-numeric: 24 passed, 2 failed
### $ ./build/bin/shard-db-test run test-packed-edit-field-varchar   (reverted pre-flight)
ok 1 - create packed object
ok 2 - insert
ok 3 - insert
ok 4 - grow rebuilds
ok 5 - value intact after grow
not ok 6 - shrink below an existing value refused by the pre-flight
not ok 7 - dry_run predicts the same refusal
ok 8 - refused shrink changed nothing
ok 9 - shrink to exactly the longest value is allowed
ok 10 - bounded -> bare
ok 11 - bare -> bounded when all rows fit
ok 12 - bare -> too-small bounded refused
ok 13 - count
ok 14 - varchar index rebuilt and consistent
ok 15 - default-only edit is metadata only
ok 16 - no residue
# test-packed-edit-field-varchar: 14 passed, 2 failed
Restore; rerun both:
### $ ./build/bin/shard-db-test run test-packed-edit-field-numeric   (restored)
# test-packed-edit-field-numeric: 26 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-edit-field-varchar   (restored)
# test-packed-edit-field-varchar: 16 passed, 0 failed
### $ ./build/bin/shard-db-test run-all --filter edit-field --jobs 4
1..8
# total: 149 passed, 0 failed across 8 cases
### $ ./build/bin/shard-db-test run-all --filter not-null --jobs 4
1..6
# total: 183 passed, 0 failed across 6 cases
### $ ./build/bin/shard-db-test run-all --filter enum --jobs 4
1..2
# total: 57 passed, 0 failed across 2 cases
### $ ./build/bin/shard-db-test run-all --filter long-field --jobs 4
1..1
# total: 53 passed, 0 failed across 1 cases

## Task 4 — vacuum compact (D-5) and the tombstone-filter fix (A-4)

(H5 and H7 were applied in Tasks 2–3.) GREEN targets:
### $ ./build/bin/shard-db-test run test-packed-compact
ok 27 - no residue
# test-packed-compact: 27 passed, 0 failed
### $ ./build/bin/shard-db-test run test-compact-removed-literal   (both halves)
ok 12 - row readable
# test-compact-removed-literal: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-compact-after-stage
shard-db stopped (pid=719942)
# test-rebuild-txn-packed-compact-after-stage: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-compact-after-walk
shard-db stopped (pid=720227)
# test-rebuild-txn-packed-compact-after-walk: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-compact-after-metadata
shard-db stopped (pid=720509)
# test-rebuild-txn-packed-compact-after-metadata: 12 passed, 0 failed
### $ ./build/bin/shard-db-test run test-rebuild-txn-packed-compact-after-commit
shard-db stopped (pid=720791)
# test-rebuild-txn-packed-compact-after-commit: 10 passed, 0 failed

### Revert proof A-4 — temporarily restore `strstr(line, ":removed")` in H5's compaction filter. Run:
### $ ./build/bin/shard-db-test run test-compact-removed-literal   (reverted filter)
ok 1 - create
ok 2 - insert
ok 3 - tombstone gone
not ok 4 - compact
not ok 5 - live line kept verbatim, tombstoned line dropped
#   assertion failed: conf && strstr(conf, "note:varchar:20:default=a:removed:x") && !strstr(conf, "gone:")
ok 6 - row readable
ok 7 - create
ok 8 - insert
ok 9 - tombstone gone
ok 10 - compact
not ok 11 - live line kept verbatim, tombstoned line dropped
#   assertion failed: conf && strstr(conf, "note:varchar:20:default=a:removed:x") && !strstr(conf, "gone:")
not ok 12 - row readable
#   assertion failed: g && strstr(g, "\"note\":\"hello\"")
# test-compact-removed-literal: 8 passed, 4 failed
Restore H5; rerun:
### $ ./build/bin/shard-db-test run test-compact-removed-literal   (restored)
# test-compact-removed-literal: 12 passed, 0 failed
(Reverted run: the substring filter drops the live `note:...:default=a:removed:x` line on BOTH
layouts — packed: `not ok 4 compact` + `not ok 5` (with the note line gone the compact even
mis-detects "every field tombstoned"); legacy: `not ok 11` (line dropped) + `not ok 12` (row
unreadable). Restored run: 12 passed, 0 failed. The exact-suffix filter is what keeps the line.)
### $ ./build/bin/shard-db-test run-all --filter vacuum --jobs 4
1..9
# total: 136 passed, 0 failed across 9 cases
### $ ./build/bin/shard-db-test run-all --filter heavy-vacuum --jobs 4
1..1
# total: 3 passed, 0 failed across 1 cases

## Task 5 — full suite and hygiene

1. `SKIP_TESTS=1 ./build.sh`: **zero warnings** (grep empty — pasted each rebuild above; final
   rebuild after the H5 restore also clean).
3. Hygiene:
### $ git diff --check
(clean)
### $ grep -rn "TODO\|FIXME" src/db/types.h src/db/query_find.c src/db/query_schema.c src/db/config.c src/db/objlock.c src/test/cases/test_packed_recompose.c | grep -v PLAN
(none)
### $ grep -rn "not supported on packed" src/
(empty — no matches)
2. Full suite (fresh, once):
### \$ ./build/bin/shard-db-test run-all --jobs 4
```
1..592
# total: 2403395 passed, 0 failed across 592 cases
```
Exit code 0. Baseline at b1cc3b7 was 2,402,659 passing / 0 failing across 568 cases → the delta is
exactly the 24 new cases (+736 assertions), 0 failing. `/tmp/shard-db-*` cleared immediately after.
4. Nothing committed — the working tree is left for review.

## Task 6 — documentation (§6) — applied to the working tree (uncommitted)

- 6.1(i)–(iv) → docs/query-protocol/schema-mutations.md (packed add-field bullets after the
  "Sequence allocation" bullet; packed edit rules table before "Other cross-type edits are
  hard-refused"; item-7 packed-converter sentence; vacuum compact flavor row rewritten).
- 6.2 → docs/reference/error-codes.md (six new rows after the `invalid field name` row).
- 6.3 → docs/reference/changelog.md (paragraph after the layout-token sentence) and
  docs/release-notes/2026.11.1.md (bullet appended).
- 6.4 → docs/concepts/typed-records.md (packed sentence appended to the add-field NULL paragraph).
- 6.4b → docs/reference/limits.md (edit-field scope row's last cell extended).

### 6.5 Instructions for R6 (recorded here per the plan)

`docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md` Task 12 contains docs hunks that
would *add* "not supported on packed-layout"/"R5b-superseding" text: the changelog sentence
"`add-field`, `edit-field` and `vacuum compact` are not yet supported on packed objects…", the
release-note bullet beginning "- `add-field`, `edit-field` and `vacuum compact` on a migrated
object are not yet supported", the three `error-codes.md` rows ("… are not supported on
packed-layout objects yet"), and `schema-mutations.md` hunks (i)–(iii) (the three
"> **Not supported on packed-layout objects…**" notes). **R6 must not apply these.** Also remove
the R5-plan "FOLLOW-UP NOTE — R5b" once R5b is merged.

### DoD: docs grep
### $ grep -rn "not supported on packed" src/ docs/ --include=*.c --include=*.h --include=*.md
### $ grep -rn 'not supported on packed' src/ docs/ --include='*.c' --include='*.h' --include='*.md'
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:74:   `docs/release-notes/2026.11.1.md` and `docs/reference/changelog.md` at `b1cc3b7` say nothing about "not supported on packed-layout".
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:77:   *adds* the R5b documentation, and **R6 must drop every "not supported on packed" hunk from the R5 plan's Task 12** (listed in §6.4).
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:103:| `src/db/config.c`, `cmd_add_fields` | `add-field is not supported on packed-layout objects yet` | `test_layout_manifest.c` (`test_packed_recompose_ops_refused`, match substring `not supported on packed`) |
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:104:| `src/db/query_schema.c`, `cmd_edit_fields` | `edit-field is not supported on packed-layout objects yet` | same test |
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:105:| `src/db/query_find.c`, `rebuild_object_v2` (guard at the top) | `vacuum compact / add-field / edit-field are not supported on packed-layout objects yet` | same test (compact) |
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:807:        OUT("{\"error\":\"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet\"}\n");
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:937:            OUT("{\"error\":\"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet\"}\n");
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:1081:        OUT("{\"error\":\"add-field is not supported on packed-layout objects yet\"}\n");
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:1426:        OUT("{\"error\":\"edit-field is not supported on packed-layout objects yet\"}\n");
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:2260:Replace the three refusal assertions in `test_packed_recompose_ops_refused` (anchors: the three `ASSERT_TRUE(lm_expect_error(tc, ... "not supported on packed"), "... refused");` statements, quoted exactly in the file at the strings
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:2278:After the flip `lm_expect_error` has **no remaining caller** and `-Wall -Wextra` would warn (unused static): **delete the `lm_expect_error` function** (definition begins `static int lm_expect_error(TestClient *tc, const char *req, const char *needle) {`, once). Quoted old text to replace in the case body: the nine lines from `    ASSERT_TRUE(lm_expect_error(tc,` (the `"add-field refused"` one) through `        "not supported on packed"), "compact refused");`.
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:2455:   Expected RED reason for all (except the 12 crash cases, which time out waiting for the pause marker): the response contains `not supported on packed-layout objects yet`
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:2484:3. (new cases `test-packed-bitmap-after-rebuild`, `test-packed-edit-field-tombstone-multi` included in the Task 2–4 runs.) `git diff --check` clean; no `TODO`/`FIXME`/debug prints; `grep -rn "not supported on packed" src/` → only matches inside **test strings that assert the absence** (none expected) — must be empty.
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:2549:`docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md` Task 12 contains docs hunks that would *add* "not supported on packed-layout"/"R5b" text:
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:2550:the changelog sentence "`add-field`, `edit-field` and `vacuum compact` are not yet supported on packed objects…", the release-note bullet beginning "- `add-field`, `edit-field` and `vacuum compact` on a migrated object are not yet supported", the three `error-codes.md` rows ("… are not supported on packed-layout objects yet"), and `schema-mutations.md` hunks (i)–(iii) (the three "> **Not supported on packed-layout objects…**" notes). **R6 must not apply these.** Also remove the R5-plan "FOLLOW-UP NOTE — R5b" once R5b is merged.
docs/plans/2026-11-03-r5b-packed-recompose-plan.md:2576:- [ ] `grep -rn "not supported on packed" src/ docs/ --include=*.c --include=*.h --include=*.md` finds nothing outside `docs/plans/`.
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:1027:| D9 — **CONFIRMED by the human (sequencing only: nothing ships before R5b/R6/R7, so no functionality is lost)** | For **packed objects**, `add-field`, `edit-field` and `vacuum --compact` (the three operations that *recompose record bytes* with a changed schema) are **fail-closed refused** in R5 with the error `"<op> is not supported on packed-layout objects yet"`. Splits/streams changes (`vacuum` with `splits`, auto-reshard), `remove-field`, `rename-field`, `add-index`/`remove-index`, `reindex`, `backup`/`restore` are supported and tested. The recompose of those three is a follow-up plan **R5b, which must land before R6**. | Each needs a semantic decision the human has not made (numeric rescale on edit, computed-default backfill — seq/uuid()/random — on packed, bare-vs-bounded edits, re-pointing the legacy-grammar validators `validate_field_type`/`typed_legacy_decl_size` at the packed grammar). Writing them blind would be guessing. Because every migrated object is packed, **until R5b the only way to add/edit a field is to recreate the object**. If you want them in R5 say so and I will write Part C (tasks numbered 13+) before execution. |
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:1471:        "not supported on packed"), "add-field refused");
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:1474:        "not supported on packed"), "edit-field refused");
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:1477:        "not supported on packed"), "compact refused");
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:1520:        OUT("{\"error\":\"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet\"}\n");
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:1537:            OUT("{\"error\":\"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet\"}\n");
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:1573:        OUT("{\"error\":\"add-field is not supported on packed-layout objects yet\"}\n");
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:1591:        OUT("{\"error\":\"edit-field is not supported on packed-layout objects yet\"}\n");
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:1596:Error texts: test needle is `not supported on packed` — all three messages contain it (`…are not supported on packed-layout…`, `add-field is not supported on packed-layout…`, `edit-field is not supported on packed-layout…`) ✓.
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:3180:| `vacuum compact / add-field / edit-field are not supported on packed-layout objects yet` | Recomposing operation on a packed object. | Recreate the object, or wait for R5b. |
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:3181:| `add-field is not supported on packed-layout objects yet` | Same, `add-field`. | Same. |
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:3182:| `edit-field is not supported on packed-layout objects yet` | Same, `edit-field`. | Same. |
docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md:3224:Deferred by D9: packed-layout **recompose** of `add-field`, `edit-field` and `vacuum compact` (also `compact`+`splits`). In R5 they are refused with `"... not supported on packed-layout objects yet"` (Task 6). R5b needs the human's decisions on: (1) numeric rescale/precision edits (text re-encode via `pkr_numeric_encode`, refuse on loss vs round); (2) computed-default backfill (`seq`, `uuid()`, `random`) on packed rows; (3) bare vs bounded `varchar`/`numeric` edits; (4) re-pointing `validate_field_type`/`typed_legacy_decl_size`/`parse_field_line` at the packed grammar for packed objects; and implementation of the `slot_changed` branch of `v2_rebuild_walk_cb` for packed (the `layout_convert_record` shape in Task 8 is the template). Until R5b, the only way to add/edit a field on a migrated object is to recreate it. R6 (acceptance, docs, clients, sanitizers, push/PR) is blocked on R5b.
docs/plans/2026-11-02-r5-evidence.md:416:not ok 6 - add-field refused       (errors later with other text; no "not supported on packed")
(no matches outside docs/plans/ — actually none anywhere; the plan file is .md but grep found nothing?)
### $ grep -rn 'not supported on packed' src/ docs/ (excluding docs/plans/)
(no matches outside docs/plans/)
### $ SKIP_TESTS=1 ./build.sh (final)
(zero warnings/errors)

## Blind-review pass (DoD) — findings recorded and fixed

Reviewer verdict: FIX-FIRST; transactional/crash-recovery core verified clean (H6 cache aliasing,
H15↔rebuild_txn_abort restore paths, H17 commit durability, edit_merge_line equivalence, cell/
buffer ownership, integer bounds, JSON-envelope sanitisation, pause phases, test buffers).

Fixed:
1. MINOR — enum-list leak on the packed pre-flight hard-error return (query_schema.c H14g
   `prc < 0` path): added the `free_enum_values(&parsed[_i])` loop every sibling return has.
2. MINOR — packed edit-field accepted computed defaults legacy refuses (A-3 gap): an edit like
   `b:varchar:8:default=random(16)` (or a seq/uuid/literal that cannot be encoded) passed the
   metadata-only path and left fields.conf carrying a default every later insert fails to
   encode. Fix: `packed_probe_text` is now exported (query_internal.h) and cmd_edit_fields'
   packed branch probes every edited field's declared default (literal itself, seq "1",
   sample UUID, 2N hex for random(N) with the 1..256 gate) before the edit applies — same
   machinery add-field already uses. Pinned by two new assertions in test-packed-edit-field-gates
   (refusal for random(16) into varchar:10; metadata-only success for random(4)).
3. NIT — the new duplicate-name echo in rebuild_object_packed now sanitises the raw conf name
   via packed_json_safe (NITs 4/5 recorded as accepted: truncation is bounded and documented;
   crash-test teardown matches the plan verbatim and the file's existing shape).

Post-fix verification:
### $ ./build/bin/shard-db-test run test-packed-edit-field-gates
# test-packed-edit-field-gates: 10 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-edit-field-varchar
# test-packed-edit-field-varchar: 16 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-edit-field-numeric
# test-packed-edit-field-numeric: 26 passed, 0 failed
### $ ./build/bin/shard-db-test run test-packed-edit-field-tombstone-multi
# test-packed-edit-field-tombstone-multi: 10 passed, 0 failed
### \$ ./build/bin/shard-db-test run-all --jobs 4   (final, after review fixes — fresh, once)
```
1..592
# total: 2403397 passed, 0 failed across 592 cases
```
Exit 0 (+2 assertions vs the pre-fix run = the two new A-3 edit-path assertions). `/tmp` cleared.
`SKIP_TESTS=1 ./build.sh` after the fixes: zero warnings. Nothing committed — tree left for review.

## Task 7 (human-approved: round-half-away-from-zero)

Unpinned `declaration_layout:"legacy"` in: test_edit_field, test_edit_field_polish, test_edit_long_default, test_not_null_edit_gate, test_not_null_edit_modifiers, test_not_null_schema, test_long_field_def, test_enum, test_add_field_computed_defaults, test_slotcask_v2_schema, test_vacuum_addfield, test_vacuum_streams_mismatch.

Expectation edits (no assertion weakened):
- test_edit_field: scale-down 3.1499 → 3.15 (round-half-away); shrink-overflow reason text now `exceeds new cap`.
- test_vacuum_addfield / test_slotcask_v2_schema: new nullable fields backfill NULL on packed (A-1), was zero/"".
- test_vacuum_streams_mismatch: fixture bug — the schema.conf rewrite helper dropped the trailing `:layout=packed` token, turning the object into legacy; helper now preserves the tail.

Re-pinned (legacy-specific): test_not_null_edit_gate object `gl` (legacy hdr2=0 surgery), test_bitmap_kfcache_lock_order fixture.

**Finding (pre-existing, not R5b):** `add-index {"field":"kind:bitmap","force":true}` on a packed object, then `count kind eq alpha` returns 0 instead of 5. Reproduced on the base commit b1cc3b7 in a clean worktree. Test re-pinned; needs its own fix (R6 or separate plan).

Full suite after Task 7: 2,403,397 passed / 0 failed, 592 cases (--jobs 4).

## Blind review round 1 — all findings fixed (full suite 2,403,484 passed / 0 failed, 598 cases, --jobs 4)

1. **Bitmap bits go stale after any recompose (confirmed, medium).** New `test-packed-bitmap-collision-growth` (6000 rows, every 3rd deleted) failed after add-field/compact/edit-field (e.g. bitmap=998 vs scan=1000). Fix: `rebuild_object_v2` rebuilds every bitmap spec (explicit or auto-promoted bool/enum) after the schema swap, and sets `indexes_may_change` on the txn when a bitmap exists so abort/recovery re-reindexes. Revert proofs: (A) txn flag off → `test-rebuild-txn-packed-bitmap-after-indexes` fails 3 assertions; (B) post-recompose rebuild off → packed and legacy collision tests fail. New pause phase `after-indexes`.
2. **Forced single-field bitmap build on packed produced an empty bitmap (the earlier "pre-existing" finding).** Root cause: `bm_rebuild_cb` decoded with the legacy `typed_record_view`/offset path. Now uses `build_index_payload_from_record` (layout-aware, same payload as insert and the seg-scan rebuild); malformed rows fail the build instead of being skipped. `test_bitmap_kfcache_lock_order` unpinned again.
3. **Metadata-only edit reported failure after the file was live (prc==1).** `rewrite_fields_conf_for_edit` now returns 1 for "applied, parent sync failed"; `cmd_edit_fields` invalidates caches and says durability is unconfirmed. No fault-injection seam exists for the parent fsync, so this has no automated test (code review only).
4. **Enum-list leaks** on the `!sdb`, pre-flight-failed, rebuild-failed and rewrite-failed returns of `cmd_edit_fields` (LSan-visible; no sanitizer run until R6 — flagged for the R6 gate).
5. **Legacy coverage restored:** new `test_legacy_recompose.c` (truncate-toward-zero, "varchar shrink" text, zero-fill add-field, compact, legacy bitmap collision/growth).
6. **errno hygiene:** `errno = 0` before the three ENOMEM classifications in the packed converter.
7. **dry_run accuracy:** `packed_preflight_walk` uses `slotcask_walk_live_strict` (unreadable live entry → pre-flight error, not a false pass).
8. **`:removed` exact-suffix** now shared (`fields_line_is_removed`, config.c) by remove-field, rename-field, add-field and edit-field; new `test-removed-literal-remove-rename-edit` (both layouts).
9. **Docs:** typed-records sentence rewritten; "refused before any data moves" corrected (rolled back); `:not_null` without a default now refused *before* the transaction starts.
10. **Rebuild txn staging is power-loss durable:** rollback copy and meta are fsynced, directory entries synced, `active` made durable before the data tree is moved (a sync failure un-publishes and fails the begin). SIGKILL tests cannot exercise this; code review only.
11. **JSON echoes** of request text in add-field/edit-field errors are escaped (`OUT_ERR_ESC`). The wire parser already rejects quotes/backslashes in field lines before these sites, so no test can reach them; defense in depth.

## Blind review round 2 — all findings addressed (full suite 2,403,488 passed / 0 failed, 598 cases, --jobs 4)

- **H1 (HIGH, reproduced then fixed).** `build_bitmap_pass` sized every temporary bitmap from the default tier, but shards auto-grow past it, so `bm_set` failed ("index build failed … Input/output error") and every recompose/streams rebuild of a grown object with a bitmap would have failed. Reproduced first by shrinking the kf tier (`test_env_ctl_set_kf_seams(env, 64, 0)`) and making the builder seam-aware; fix: `bm_shard_walk_worker` opens each bitmap at that shard's actual `kh.capacity` (as the seg-scan rebuild already does).
- **H2 (tests too weak).** A popcount cannot see a misplaced bit. Tests now shrink the kf tier (real growth, ~73% load) and resolve slots back to records with `find` (every returned row must carry the queried value; none missing). Revert proof: with the post-recompose rebuild disabled the packed/legacy tests fail hard (e.g. matching=306 of 1000). `test-rebuild-txn-packed-bitmap-after-indexes` now resolves via `find` too; with the late txn marker disabled it fails 3 assertions.
- **M1.** `indexes_may_change` is no longer set at transaction begin; `rebuild_txn_set_indexes_may_change` rewrites the txn meta durably immediately before the first bitmap file is touched, so aborts/recoveries that never reached the bitmap step no longer force a full reindex.
- **L1.** Bitmaps are rebuilt through `cmd_add_indexes(force)` on exactly the bitmap specs (not every index on the field); response `indexes_rebuilt` no longer double counts.
- **L2.** `edit_merge_line` refuses a merged line of `MAX_FIELD_DEF` bytes or more (the loader would refuse the whole schema).
- **L3.** `edit_merge_line` uses exact trailing-token tests for `:not_null`/`:nullable`. Not reachable today (the declaration grammar rejects `:not_null` inside a default literal), so no test; consistency fix.
- **L4.** `rebuild_txn_begin` also syncs the directories touched by the data-tree move.
- **L5.** Fixture allocation and bulk-insert response are asserted.

## Blind review round 3 — verdict FIX-FIRST (tests only); addressed

- **M1.** The crash test now runs under a small kf tier that persists across daemon restarts (TEST_BUILD-only `KF_TEST_INITIAL_SLOTS` db.env knob, since the control channel cannot reach a restarting daemon). Revert proof: late txn marker disabled → 4 assertions fail; restored → 20/20 pass.
- **M2.** `slotcask_default_slots_for_splits` honours the W1 test seam under TEST_BUILD, so bitmap sizing is consistent with the keyfile tier. Revert proof: sizing the temporary bitmap from the default tier again → `forced bitmap rebuild` (packed) and the recompose step (legacy) fail with the I/O error; restored → pass.
- **Low fixes:** a missing /dev/null now fails the helper instead of leaking its reply into the client stream; stale index specs for non-existent fields are skipped; abort fsyncs the data-restore and done renames; `test_layout_manifest` function renamed `..._supported`; `bm_rebuild_cb` comment corrected.
- **Accepted, not changed:** (a) `cmd_add_indexes` canonicalises bare bool/enum index.conf lines to `:bitmap` (same meaning) and index.conf is outside the rollback; (b) a bitmap on a field edit-field also marks dirty is rebuilt twice and `indexes_rebuilt` counts only the edit's pass (correct result, extra work); (c) `fields.conf.rollback.tmp.<pid>` can survive a crash inside abort; (d) 64 KB `line_copy` stack buffer in `edit_merge_line` (worst case < 1 MB of an 8 MB stack).
