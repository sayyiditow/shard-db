#ifndef SHARD_DB_TEST_VARLEN_SCAN_FIXTURE_H
#define SHARD_DB_TEST_VARLEN_SCAN_FIXTURE_H

#include "types.h"
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static inline size_t varlen_fixture_record_size(uint16_t klen, uint32_t vlen) {
    size_t raw = 24 + (size_t)klen + (size_t)vlen;
    return (raw + 7) & ~(size_t)7;
}

static inline size_t varlen_fixture_write_bytes(uint8_t *buf, uint8_t flag,
                                                const char *key, uint16_t klen,
                                                const char *value, uint32_t vlen) {
    uint8_t hash[16];
    compute_hash_raw(key, klen, hash);
    memcpy(buf, hash, sizeof(hash));
    memcpy(buf + 16, &klen, sizeof(klen));
    buf[18] = flag;
    buf[19] = 0;
    memcpy(buf + 20, &vlen, sizeof(vlen));
    memcpy(buf + 24, key, klen);
    memcpy(buf + 24 + klen, value, vlen);
    size_t padded = varlen_fixture_record_size(klen, vlen);
    memset(buf + 24 + klen + vlen, 0,
           padded - (24 + (size_t)klen + (size_t)vlen));
    return padded;
}

static inline size_t varlen_fixture_write_file(FILE *f, uint8_t flag,
                                               const char *key, uint16_t klen,
                                               const char *value, uint32_t vlen) {
    size_t size = varlen_fixture_record_size(klen, vlen);
    uint8_t *record = malloc(size);
    if (!record) return 0;
    varlen_fixture_write_bytes(record, flag, key, klen, value, vlen);
    fwrite(record, 1, size, f);
    free(record);
    return size;
}

#endif
