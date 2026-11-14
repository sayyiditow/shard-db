# R1 amendment — restore keyfile-level fetch skipping

User authorization (2026-10-28): “Please revert to previous behaviour.” This
amends revision 3's skipped-segment validation policy after execution; it does
not rewrite the frozen revision-3 checkpoints or its historical evidence.

Root cause: both ordinary fetch and private packed CSV decrement their offset
inside the record callback. Checked traversal has already opened the segment
and validated its header by that point. Move offset consumption into keyfile
traversal before segment access, matching the former skip optimization. This
is not evidence of a production corruption incident: the regression deliberately
injects an unavailable reference solely to prove that skipped locations are
never accessed. Returned records retain checked validation.

Continue the current uncommitted feature branch; preserve all R1 changes and
planning artifacts. Do not stage, commit, push, run benchmarks, start R2 or
activate packed storage. Existing KF/segment read locks and worker scheduling
stay unchanged. The original slotcask prefix remains untouched. Sanitizers keep
the AGENTS.md branch-final timing. The user explicitly authorized this amendment;
no separate approval pause is required.

## Task 1 — regression first

In src/test/cases/test_r1_routes.c add #include <fcntl.h> immediately after the
quoted anchor #include <errno.h>. Insert the following complete test before
`static int test_r1_routes(void)`. Registering in that existing test translation
unit requires no build-list change. Fixtures use unique temporary directories,
no environment mutations, and restore their injected offset before cleanup.
Run an ordinary warning-free build and test-r1-fetch-skip; capture genuine RED
on current R1. The skipped offset/cursor cases must fail while the explicit
returned-record error controls pass. Do not accept infrastructure failures.

```c
static int r_fetch_page(CFixture *f, const char *format, int offset,
    const char *cursor, char **text) {
    size_t length = 0;
    FILE *out = open_memstream(text, &length);
    ASSERT_NOT_NULL(out, "pagination output stream");
    if (!out) return -1;
    TypedReadOptions options = {
        .kind = TYPED_READ_FETCH, .format = format, .fields = "f0",
        .offset = offset, .limit = 1, .cursor = cursor,
        .budget = 65536, .delimiter = ','
    };
    int rc = typed_read_command(&f->db, &f->fs, &f->sc, &options, out);
    ASSERT_EQ_INT(fclose(out), 0, "pagination output close");
    return rc;
}

static void r_fetch_skip_layout(int packed) {
    CFixture f;
    if (c_open(&f, 0)) { c_close(&f); return; }
    if (!packed) {
        memset(f.ts, 0, sizeof(*f.ts));
        f.ts->typed = 1;
        f.ts->nfields = 1;
        f.ts->nullmap_prefix = 2;
        f.ts->total_size = 6;
        strcpy(f.ts->fields[0].name, "f0");
        f.ts->fields[0].name_len = 2;
        f.ts->fields[0].type = FT_INT;
        f.ts->fields[0].size = 4;
        f.sc.max_value = f.ts->total_size;
        f.fs.auto_key_schema_snapshot = f.sc;
    }
    uint8_t flags = packed ? PKR_FLAGS : SREC_FLAG_NULLMAP;
    if (c_seed(&f, "a", 1, "{\"f0\":7}", flags) ||
        c_seed(&f, "b", 1, "{\"f0\":7}", flags)) {
        c_close(&f); return;
    }
    char path[PATH_MAX];
    int length = snprintf(path, sizeof(path), "%s/data/kf/000.kf", f.root);
    ASSERT_TRUE(length > 0 && (size_t)length < sizeof(path), "pagination keyfile path");
    if (length < 1 || (size_t)length >= sizeof(path)) { c_close(&f); return; }
    int fd = open(path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "pagination keyfile open");
    if (fd < 0) { c_close(&f); return; }
    struct stat st;
    int rc = fstat(fd, &st);
    ASSERT_EQ_INT(rc, 0, "pagination keyfile size");
    if (rc) { close(fd); c_close(&f); return; }
    SlotcaskKfEntry first = {0}, second = {0};
    off_t first_at = 0;
    int live = 0;
    for (off_t at = SLOTCASK_KF_HDR_SIZE;
         at <= st.st_size - (off_t)sizeof(SlotcaskKfEntry);
         at += sizeof(SlotcaskKfEntry)) {
        SlotcaskKfEntry entry;
        ssize_t got = pread(fd, &entry, sizeof(entry), at);
        if (got != sizeof(entry)) { live = -1; break; }
        if (entry.flag != 1) continue;
        if (!live) { first = entry; first_at = at; }
        else if (live == 1) second = entry;
        live++;
    }
    ASSERT_EQ_INT(live, 2, "two pagination records in traversal order");
    if (live != 2) { close(fd); c_close(&f); return; }
    uint8_t a_hash[16];
    compute_hash_raw("a", 1, a_hash);
    const char *survivor = memcmp(second.hash, a_hash, 16) ? "b" : "a";
    uint32_t unavailable = UINT32_MAX;
    off_t offset_at = first_at + offsetof(SlotcaskKfEntry, offset);
    ssize_t written = pwrite(fd, &unavailable, sizeof(unavailable), offset_at);
    ASSERT_EQ_INT(written, sizeof(unavailable), "inject unavailable skipped location");
    if (written != sizeof(unavailable)) { close(fd); c_close(&f); return; }
    const char *formats[] = {NULL, "rows", "dict", "csv"};
    for (size_t i = 0; i < sizeof(formats) / sizeof(formats[0]); i++) {
        char *text = NULL;
        rc = r_fetch_page(&f, formats[i], 1, NULL, &text);
        ASSERT_EQ_INT(rc, 0, "offset skips segment access");
        char expected[16];
        snprintf(expected, sizeof(expected), formats[i] && !strcmp(formats[i], "csv") ?
                 "%s," : "\"%s\"", survivor);
        ASSERT_CONTAINS(text, expected, "page returns the next live key");
        free(text); text = NULL;
        rc = r_fetch_page(&f, formats[i], 0, "1", &text);
        ASSERT_EQ_INT(rc, 0, "cursor skips segment access");
        ASSERT_CONTAINS(text, expected, "cursor returns the next live key");
        free(text); text = NULL;
        rc = r_fetch_page(&f, formats[i], 1, "1", &text);
        ASSERT_EQ_INT(rc, 0, "skip beyond records avoids segment access");
        ASSERT_TRUE(text && !strstr(text, "\"error\""), "empty page is successful");
        free(text); text = NULL;
        rc = r_fetch_page(&f, formats[i], 0, NULL, &text);
        ASSERT_EQ_INT(rc, -1, "returned records still require valid locations");
        ASSERT_TRUE(text && !strncmp(text, "{\"error\":", 9), "returned-record error has no success prefix");
        free(text);
    }
    written = pwrite(fd, &first.offset, sizeof(first.offset), offset_at);
    ASSERT_EQ_INT(written, sizeof(first.offset), "restore injected location");
    ASSERT_EQ_INT(close(fd), 0, "pagination keyfile close");
    ASSERT_EQ_INT(slotcask_write_reservation_held(&f.db), 0, "pagination takes no writer reservation");
    c_close(&f);
}

static int test_r1_fetch_skip(void) {
    r_fetch_skip_layout(0);
    r_fetch_skip_layout(1);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-r1-fetch-skip", test_r1_fetch_skip)

```

## Task 2 — restore skip placement

In src/db/slotcask.c replace the complete quoted function beginning
`int slotcask_walk_live_checked(SlotcaskDb *db,SlotcaskScanCb callback,void *ctx){`
with this complete replacement:

```c
int slotcask_walk_live_skip_checked(SlotcaskDb *db, int64_t skip_n,
    SlotcaskScanCb callback, void *ctx) {
    if (!db || !callback || db->num_shards < 1) { errno = EINVAL; return -1; }
    for (int shard = 0; shard < db->num_shards; shard++) {
        char path[PATH_MAX];
        kf_path_for(path, db->data_dir, shard);
        SlotcaskKfHandle keyfile;
        if (kfcache_acquire(&keyfile, path, db->slots_per_shard, 0)) return -1;
        int rc = 0;
        for (size_t slot = 0; slot < keyfile.capacity; slot++) {
            const SlotcaskKfEntry *entry = &keyfile.map[slot];
            uint8_t flag = __atomic_load_n(&entry->flag, __ATOMIC_ACQUIRE);
            if (!flag || flag == 2) continue;
            if (flag != 1) { errno = EUCLEAN; rc = -1; break; }
            if (skip_n > 0) { skip_n--; continue; }
            rc = slotcask_emit_checked(db, entry, callback, ctx);
            if (rc) break;
        }
        int saved = errno;
        kfcache_release(&keyfile);
        errno = saved;
        if (rc) return rc;
    }
    return 0;
}

int slotcask_walk_live_checked(SlotcaskDb *db, SlotcaskScanCb callback, void *ctx) {
    return slotcask_walk_live_skip_checked(db, 0, callback, ctx);
}
```

In src/db/slotcask.h retain the quoted existing declaration
`int slotcask_walk_live_checked(SlotcaskDb *db,SlotcaskScanCb cb,void *ctx);`
and insert immediately after it:

```c
/* Skip live keyfile entries before accessing segments; validate emitted records. */
int slotcask_walk_live_skip_checked(SlotcaskDb *db, int64_t skip_n,
    SlotcaskScanCb cb, void *ctx);
```

In read_walk (src/db/typed_read_command.c), delete only the complete hunk:

```c
    if(c->skip){c->skip--;return 0;}
```

In typed_read_command replace the complete hunk:

```c
            int rc=slotcask_walk_live_checked(db,read_walk,&c);
```

with:

```c
            int rc=slotcask_walk_live_skip_checked(db,c.skip,read_walk,&c);
```

In csv_fetch_record (src/db/packed_csv_command.c), delete only the identical
callback-local skip hunk quoted above. In packed_csv_fetch replace:

```c
    c.limit=(size_t)limit;    int rc=slotcask_walk_live_checked(db,csv_fetch_record,&c);
```

with:

```c
    c.limit=(size_t)limit;
    int rc=slotcask_walk_live_skip_checked(db,c.skip,csv_fetch_record,&c);
```

Consumer census: slotcask_walk_live_checked has only the two command consumers
above and its header declaration/definition. Keep its skip-zero wrapper API for
internal compatibility. The new skip API is consumed by ordinary fetch/keys and
private packed CSV fetch only. Multi-key readers, insert/update/delete, indexes,
public wire envelopes and offset/cursor arithmetic are unchanged. Keyfile flags
still distinguish live/tombstone/empty; tombstones do not consume offset.
Skipped entries need no segment access; limits stop after enough returned rows;
negative offsets retain existing normalization. No new locks or writer gates.

## Task 3 — verify and document

Use rtk-prefixed commands: rtk proxy env SKIP_TESTS=1 ./build.sh;
rtk proxy ./build/bin/shard-db-test run test-r1-fetch-skip;
rtk proxy ./build/bin/shard-db-test run test-r1-routes;
rtk proxy ./build/bin/shard-db-test run-all. Capture zero warnings, real totals
and exit codes. Keep builds and suite runs sequential. Run the existing frozen
allocation/concurrency/client checks as compatibility coverage against the amended
source (do not claim the old whole-file checkpoint hashes still match). The
scratch pre-correction slotcask suffix predates the new internal skip API: derive
a temporary checks script that appends the complete final
slotcask_walk_live_skip_checked function to that scratch baseline source only.
This resolves the new library symbol while preserving the baseline's original
bulk-corruption defect and all its expected RED/GREEN assertions. Do not change
the frozen historical plan or its tools. Record the derived compatibility script
and the narrowly scoped adaptation in the amendment evidence.

For reversal evidence retain the pre-fix RED binary/source evidence in /tmp;
never revert the live working tree. Synchronize docs/concepts/packed-csv-commands.md
and ordinary-type-integration.md: offsets skip in the keyfile before segment
access; only returned records need backing-reference validation. Append actual
results to a separate amendment evidence document, preserving revision-3 history.
Perform raw-diff review of the amendment. Leave everything unstaged/uncommitted.
If an anchor is missing or a decision falls outside this amendment, write
PLAN_NOTES.md and halt; do not improvise or weaken assertions.
