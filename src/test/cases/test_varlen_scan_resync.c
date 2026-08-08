#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "seg_scan_varlen.h"
#include "varlen_scan_fixture.h"
#include <string.h>
#include <stdint.h>

/* Reproduces the exact real-world desync shape: record A occupies a
   64-byte capacity slot but only needs 32 bytes naturally (32 bytes of
   real zero-fill gap, non-24-aligned relative to A's own size), then
   record C starts immediately at the old slot's 64-byte capacity
   boundary — a scanner that advances by A's own natural size (32 bytes)
   lands 32 bytes into the gap instead of at C. */
static int test_varlen_scan_resync_run(void) {
    uint8_t buf[512];
    memset(buf, 0, sizeof(buf));

    size_t a_natural = varlen_fixture_write_bytes(buf, 1, "ka", 2, "v", 1); /* 24+2+1=27 -> 32 */
    ASSERT_EQ_INT((int)a_natural, 32, "record A natural padded size");

    const size_t A_SLOT_CAPACITY = 64; /* reused freed slot was 64 bytes */
    /* buf[32..64) is already zero from the memset above, matching the
       real seg_record_emit() zero-pad-to-slot_size behavior exactly. */

    size_t c_off = A_SLOT_CAPACITY;
    size_t c_natural = varlen_fixture_write_bytes(buf + c_off, 1, "kc", 2, "cc", 2); /* 24+2+2=28 -> 32 */
    ASSERT_EQ_INT((int)c_natural, 32, "record C natural padded size");

    /* A scanner that advances by A's own natural size (32) lands at
       offset 32 — inside the zero-filled gap, not at C (offset 64). */
    ASSERT_TRUE(a_natural < A_SLOT_CAPACITY,
                "A's natural size is smaller than its old slot capacity (the gap exists)");

    size_t desync_off = a_natural; /* where a naive scanner would land: 32 */
    const size_t max_slot_size = A_SLOT_CAPACITY;

    size_t resume_off = 0;
    int found = seg_scan_varlen_resync(buf, sizeof(buf), desync_off,
                                        max_slot_size, max_slot_size,
                                        &resume_off);
    ASSERT_TRUE(found, "resync finds a valid record within the window");
    ASSERT_EQ_INT((int)resume_off, (int)c_off,
                  "resync lands exactly on record C, not on padding inside the gap");

    /* Confirm what it found really is C: struct_ok + hash_ok both pass,
       and it's C's key, not some other offset the padding also happens
       to structurally satisfy. */
    size_t rec_size;
    uint8_t flag = 0;
    uint16_t klen = 0;
    uint32_t vlen = 0;
    int sok = seg_scan_varlen_struct_ok(buf, sizeof(buf), resume_off,
                                         max_slot_size,
                                         &rec_size, &flag, &klen, &vlen);
    ASSERT_TRUE(sok, "resync target is structurally valid");
    ASSERT_EQ_INT((int)flag, 1, "resync target has flag==1 (live)");
    ASSERT_EQ_INT((int)klen, 2, "resync target key length matches C");
    ASSERT_TRUE(memcmp(buf + resume_off + 24, "kc", 2) == 0,
                "resync target key content is C's key");
    ASSERT_TRUE(seg_scan_varlen_hash_ok(buf, resume_off, klen),
                "resync target hash verifies");

    /* Unaligned-pos regression: desync_off + 1 (unaligned) must still
       resync to the same 8-byte-aligned target, proving the search
       floors to the true 8-byte grid instead of stepping from the
       unaligned start (the alignment issue called out in review). */
    size_t resume_off2 = 0;
    int found2 = seg_scan_varlen_resync(buf, sizeof(buf), desync_off + 1,
                                         max_slot_size, max_slot_size,
                                         &resume_off2);
    ASSERT_TRUE(found2, "resync from an unaligned offset still finds a record");
    ASSERT_EQ_INT((int)resume_off2, (int)c_off,
                  "resync from an unaligned start still lands on the true 8-byte-aligned target");

    /* A pure zero region (no record ever follows) must fail, not hang or
       falsely accept the first flag==0 header as a target. */
    uint8_t all_zero[256];
    memset(all_zero, 0, sizeof(all_zero));
    size_t unused;
    int found3 = seg_scan_varlen_resync(all_zero, sizeof(all_zero), 0,
                                         max_slot_size, sizeof(all_zero),
                                         &unused);
    ASSERT_TRUE(!found3, "resync over pure padding with no real record finds nothing");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-scan-resync", test_varlen_scan_resync_run)
