#include "typed_index.h"
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#define ORDERED_KEY_CAP 544
static int fail(int e) {
  errno = e;
  return -1;
}

#ifdef TEST_BUILD
/* Deterministic allocation-failure injection for the allocating
   typed-index wrapper (same precedent as g_bulk_upd_test_fail_alloc):
   armed N fails the Nth wrapper allocation; 0 = never. */
static int g_typed_index_test_fail_alloc;

void typed_index_test_set_fail_alloc(int fail_n) {
  g_typed_index_test_fail_alloc = fail_n;
}
#endif
static int check(const TypedSchema *ts, size_t i) {
  if (!ts || i >= (size_t)ts->nfields)
    return fail(EINVAL);
  if (ts->fields[i].type <= FT_NONE || ts->fields[i].type >= FT_COUNT)
    return fail(EILSEQ);
  return 0;
}
/* Historical index unit fixtures supply legacy fields without setting
   ts->typed. Preserve that wrapper contract; packed views stay strict. */
static int index_row_open(const TypedSchema *ts, const uint8_t *value,
                          size_t len, uint8_t flags, TypedRowView *out) {
  if (!ts || ts->typed || ts->packed.nfields)
    return typed_row_open(ts, value, len, flags, out);
  memset(out, 0, sizeof(*out));
  if (!value || ts->nfields < 1 || ts->nfields > MAX_FIELDS)
    return fail(EINVAL);
  if (typed_record_view(value, len, flags, &out->base, &out->bitmap,
                        &out->bitmap_bytes, &out->region))
    return fail(EILSEQ);
  out->schema = ts;
  out->value = value;
  out->len = len;
  out->hdr2 = flags;
  return 0;
}
int typed_index_fixed_width(const TypedSchema *ts, size_t i, size_t *width) {
  if (!width)
    return fail(EINVAL);
  *width = 0;
  if (check(ts, i))
    return -1;
  const TypedField *f = &ts->fields[i];
  if (f->type == FT_VARCHAR || (ts->packed.nfields && f->type == FT_NUMERIC))
    return 0;
  size_t n = 0;
  switch (f->type) {
  case FT_BOOL:
  case FT_BYTE:
    n = 1;
    break;
  case FT_SHORT:
    n = 2;
    break;
  case FT_TIME:
    n = 3;
    break;
  case FT_INT:
  case FT_DATE:
  case FT_FLOAT:
  case FT_IPV4:
    n = 4;
    break;
  case FT_DATETIME:
    n = 7;
    break;
  case FT_LONG:
  case FT_TIMESTAMP:
  case FT_NUMERIC:
  case FT_DOUBLE:
  case FT_DATETIMEMS:
    n = 8;
    break;
  case FT_UUID:
  case FT_IPV6:
    n = 16;
    break;
  case FT_ENUM:
    n = f->enum_width == 2 ? 2 : 1;
    break;
  default:
    return fail(EILSEQ);
  }
  if (f->size != (int)n)
    return fail(EILSEQ);
  *width = n;
  return 1;
}
/* A complete fixed span has already been generation-validated. Preserve
   every historical fixed byte transformation, including FT_TIME. */
static int fixed_payload(const TypedField *f, const uint8_t *src, size_t n,
                         uint8_t *out) {
  if (!out)
    return 0;
  memcpy(out, src, n);
  switch (f->type) {
  case FT_LONG:
  case FT_NUMERIC:
  case FT_TIMESTAMP:
  case FT_INT:
  case FT_DATE:
  case FT_SHORT:
  case FT_DATETIME:
  case FT_DATETIMEMS:
  case FT_TIME:
    out[0] ^= 128;
    break;
  case FT_DOUBLE: {
    uint64_t u;
    memcpy(&u, src, 8);
    u = (u >> 63) ? ~u : u ^ UINT64_C(0x8000000000000000);
    for (size_t j = 0; j < 8; j++)
      out[j] = (uint8_t)(u >> (56 - 8 * j));
    break;
  }
  case FT_FLOAT: {
    uint32_t u;
    memcpy(&u, src, 4);
    u = (u >> 31) ? ~u : u ^ UINT32_C(0x80000000);
    for (size_t j = 0; j < 4; j++)
      out[j] = (uint8_t)(u >> (24 - 8 * j));
    break;
  }
  default:
    break;
  }
  return 0;
}
static int span_payload(const TypedSchema *ts, size_t i,
                        const TypedRowSpan *span, uint8_t *out, size_t cap,
                        size_t *len) {
  *len = 0;
  size_t width = 0;
  int fixed = typed_index_fixed_width(ts, i, &width);
  if (fixed < 0)
    return -1;
  const uint8_t *src = NULL;
  if (typed_span_read(ts, i, span, &src))
    return -1;
  const TypedField *f = &ts->fields[i];
  if (ts->packed.nfields && f->type == FT_NUMERIC) {
    dec_t d;
    dec_init(&d);
    dec_status st = dec_unpack(src, span->len, &d);
    if (st != DEC_OK) {
      dec_free(&d);
      return fail(st == DEC_ERR_NOMEM ? ENOMEM : EILSEQ);
    }
    int rc = decimal_order_into(&d, out, cap, len);
    dec_free(&d);
    return rc;
  }
  if (f->type == FT_VARCHAR) {
    size_t n = span->len;
    if (!ts->packed.nfields) {
      if (f->size < 2)
        return fail(EILSEQ);
      n = ((size_t)src[0] << 8) | src[1];
      if (n > (size_t)f->size - 2)
        n = (size_t)f->size - 2;
      src += 2;
    }
    if (out && n > cap)
      return fail(ENOBUFS);
    if (out && n)
      memcpy(out, src, n);
    *len = n;
    return 0;
  }
  if (!fixed)
    return fail(EILSEQ);
  if (out && width > cap)
    return fail(ENOBUFS);
  fixed_payload(f, src, width, out);
  *len = width;
  return 0;
}
static int field_view_key(const TypedRowView *view, size_t i, int nonfinal,
                          uint8_t *out, size_t cap, size_t *len) {
  *len = 0;
  TypedRowSpan span;
  size_t width = 0;
  if (typed_row_span(view, i, &span))
    return -1;
  const TypedSchema *ts = view->schema;
  int fixed = typed_index_fixed_width(ts, i, &width);
  if (fixed < 0)
    return -1;
  /* Legacy NULL varchar bytes retain its old data-derived padding. */
  size_t payload = 0;
  if (span.is_null) {
    if (fixed)
      payload = width;
    else if (!ts->packed.nfields) {
      const TypedField *f = &ts->fields[i];
      if (span.complete && f->size >= 2) {
        payload = ((size_t)span.data[0] << 8) | span.data[1];
        if (payload > (size_t)f->size - 2)
          payload = (size_t)f->size - 2;
      }
    }
    if (out && 1 + payload > cap)
      return fail(ENOBUFS);
    if (out) {
      out[0] = 1;
      if (payload)
        memset(out + 1, 0, payload);
    }
    *len = 1 + payload;
    return 1;
  }
  if (span_payload(ts, i, &span, NULL, 0, &payload))
    return -1;
  int escape =
      ts->packed.nfields && nonfinal && ts->fields[i].type == FT_VARCHAR;
  size_t extra = 0;
  if (escape) {
    extra = 2;
    for (size_t j = 0; j < span.len; j++)
      extra += span.data[j] == 0;
  }
  if (payload > SIZE_MAX - 1 - extra)
    return fail(EOVERFLOW);
  size_t need = 1 + payload + extra;
  if (out && need > cap)
    return fail(ENOBUFS);
  if (out) {
    out[0] = 0;
    if (escape) {
      size_t pos = 1;
      for (size_t j = 0; j < span.len; j++) {
        out[pos++] = span.data[j];
        if (!span.data[j])
          out[pos++] = 255;
      }
      out[pos++] = 0;
      out[pos] = 0;
    } else {
      size_t n = 0;
      if (span_payload(ts, i, &span, out + 1, cap - 1, &n))
        return -1;
    }
  }
  *len = need;
  return 1;
}
int typed_index_field_key_into(const TypedSchema *ts, size_t i,
                               const uint8_t *v, size_t n, uint8_t flags,
                               uint8_t *out, size_t cap, size_t *len) {
  if (!len)
    return fail(EINVAL);
  *len = 0;
  if (check(ts, i))
    return -1;
  TypedRowView view;
  if (index_row_open(ts, v, n, flags, &view))
    return -1;
  return field_view_key(&view, i, 0, out, cap, len);
}
int typed_index_field_key_alloc(const TypedSchema *ts, size_t i,
                                const uint8_t *v, size_t n, uint8_t flags,
                                size_t budget, uint8_t **out, size_t *len) {
  if (out)
    *out = NULL;
  if (len)
    *len = 0;
  if (!out || !len)
    return fail(EINVAL);
  size_t need = 0;
  int rc = typed_index_field_key_into(ts, i, v, n, flags, NULL, 0, &need);
  if (rc != 1)
    return rc;
  if (need > budget)
    return fail(ENOBUFS);
  uint8_t *buf = malloc(need ? need : 1);
  if (!buf)
    return fail(ENOMEM);
  rc = typed_index_field_key_into(ts, i, v, n, flags, buf, need, &need);
  if (rc != 1) {
    free(buf);
    return rc;
  }
  *out = buf;
  *len = need;
  return 1;
}
int typed_index_text_key_into(const TypedSchema *ts, size_t i, const char *text,
                              size_t n, uint8_t *out, size_t cap, size_t *len) {
  if (!len)
    return fail(EINVAL);
  *len = 0;
  if (check(ts, i) || (!text && n))
    return fail(EINVAL);
  const TypedField *f = &ts->fields[i];
  if (f->type == FT_NUMERIC) {
    dec_t d;
    dec_init(&d);
    dec_status st = dec_parse(text, n, &d, NULL, 0);
    if (st != DEC_OK) {
      dec_free(&d);
      return fail(st == DEC_ERR_NOMEM ? ENOMEM : EINVAL);
    }
    if (ts->packed.nfields) {
      int rc = decimal_order_into(&d, out, cap, len);
      dec_free(&d);
      return rc < 0 ? -1 : 1;
    }
    int64_t scaled = 0;
    if (conv_numeric_text(f, text, n, &scaled, NULL, 0)) {
      dec_free(&d);
      return fail(EDOM);
    }
    char assigned[96];
    int an = snprintf(assigned, sizeof(assigned), "%" PRId64 "e-%d", scaled,
                      f->numeric_scale);
    dec_t exact;
    dec_init(&exact);
    st = dec_parse(assigned, (size_t)an, &exact, NULL, 0);
    int equal = st == DEC_OK && !dec_cmp(&d, &exact);
    dec_free(&d);
    dec_free(&exact);
    if (!equal)
      return fail(st == DEC_ERR_NOMEM ? ENOMEM : EDOM);
    if (out && cap < 8)
      return fail(ENOBUFS);
    if (out) {
      uint64_t u = (uint64_t)scaled ^ UINT64_C(0x8000000000000000);
      for (size_t j = 0; j < 8; j++)
        out[j] = (uint8_t)(u >> (56 - 8 * j));
    }
    *len = 8;
    return 1;
  }
  if (f->type == FT_VARCHAR) {
    /* Query literals are not assignments: retain all bytes. Larger
       bounds remain meaningful even when no stored row can equal them. */
    if (out && n > cap)
      return fail(ENOBUFS);
    if (out && n)
      memcpy(out, text, n);
    *len = n;
    return 1;
  }
  size_t width = 0;
  if (typed_index_fixed_width(ts, i, &width) != 1)
    return -1;
  if (out && width > cap)
    return fail(ENOBUFS);
  if (f->type == FT_ENUM && enum_value_index(f, text, n) < 0)
    return 0;
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
  fixed_payload(f, raw, width, out);
  *len = width;
  return 1;
}
/* Parse the whole spec without strtok's empty-component elision/truncation. */
static int components(const TypedSchema *ts, const char *spec,
                      size_t ids[MAX_FIELDS], size_t *count) {
  *count = 0;
  if (!spec || !*spec || strnlen(spec,256)==256)
    return fail(EINVAL);
  const char *p = spec;
  while (*p) {
    const char *q = strchr(p, '+');
    size_t n = q ? (size_t)(q - p) : strlen(p);
    if (!n || n >= sizeof(ts->fields[0].name) || *count == MAX_FIELDS)
      return fail(EINVAL);
    char name[sizeof(ts->fields[0].name)];
    memcpy(name, p, n);
    name[n] = 0;
    int fi = typed_field_index(ts, name);
    if (fi < 0)
      return fail(EINVAL);
    ids[(*count)++] = (size_t)fi;
    if (!q)
      break;
    p = q + 1;
    if (!*p)
      return fail(EINVAL);
  }
  return 0;
}
int typed_index_record_key_into(const TypedSchema *ts, const uint8_t *v,
                                size_t n, uint8_t flags, const char *spec,
                                uint8_t *out, size_t cap, size_t *len) {
  if (!len)
    return fail(EINVAL);
  *len = 0;
  if (!ts)
    return fail(EINVAL);
  size_t ids[MAX_FIELDS], count = 0;
  if (components(ts, spec, ids, &count))
    return -1;
  TypedRowView view;
  if (index_row_open(ts, v, n, flags, &view))
    return -1;
  size_t need = 0;
  /* Measure all components and enforce admission before the first write. */
  for (size_t j = 0; j < count; j++) {
    size_t part = 0;
    if (field_view_key(&view, ids[j], j + 1 < count, NULL, 0, &part) < 0)
      return -1;
    if (part > ORDERED_KEY_CAP - need)
      return fail(E2BIG);
    need += part;
  }
  if (out && need > cap)
    return fail(ENOBUFS);
  if (out) {
    size_t pos = 0;
    for (size_t j = 0; j < count; j++) {
      size_t part = 0;
      if (field_view_key(&view, ids[j], j + 1 < count, out + pos, cap - pos,
                         &part) < 0)
        return -1;
      pos += part;
    }
  }
  *len = need;
  return 1;
}
int typed_index_record_payload_into(const TypedSchema *ts, const uint8_t *v,
                                    size_t n, uint8_t flags, const char *spec,
                                    uint8_t *out, size_t cap, size_t *len) {
  if (!len)
    return fail(EINVAL);
  *len = 0;
  if (!ts)
    return fail(EINVAL);
  size_t ids[MAX_FIELDS], count = 0;
  if (components(ts, spec, ids, &count))
    return -1;
  if (count != 1)
    return fail(EINVAL);
  TypedRowView view;
  TypedRowSpan span;
  if (index_row_open(ts, v, n, flags, &view) ||
      typed_row_span(&view, ids[0], &span))
    return -1;
  if (span.is_null)
    return 0;
  /* Bitmap/trigram data retains its untiered raw policy. Numeric bitmap
     dictionaries continue using the existing sortable numeric encoding. */
  if (span_payload(ts, ids[0], &span, out, cap, len))
    return -1;
  return *len ? 1 : 0;
}
int typed_index_record_alloc(const TypedSchema *ts, const uint8_t *v, size_t n,
                             uint8_t flags, const char *spec, int payload,
                             size_t budget, uint8_t **out, size_t *len) {
  if (out)
    *out = NULL;
  if (len)
    *len = 0;
  if (!out || !len)
    return fail(EINVAL);
  int (*fn)(const TypedSchema *, const uint8_t *, size_t, uint8_t, const char *,
            uint8_t *, size_t, size_t *) =
      payload ? typed_index_record_payload_into : typed_index_record_key_into;
  size_t need = 0;
  int rc = fn(ts, v, n, flags, spec, NULL, 0, &need);
  if (rc != 1)
    return rc;
  if (need > budget)
    return fail(ENOBUFS);
  uint8_t *buf = malloc(need ? need : 1);
#ifdef TEST_BUILD
  if (buf && g_typed_index_test_fail_alloc > 0 &&
      --g_typed_index_test_fail_alloc == 0) {
    free(buf);
    buf = NULL;
  }
#endif
  if (!buf)
    return fail(ENOMEM);
  rc = fn(ts, v, n, flags, spec, buf, need, &need);
  if (rc != 1) {
    free(buf);
    return rc;
  }
  *out = buf;
  *len = need;
  return 1;
}

int typed_index_validate_new(const TypedSchema *ts, const uint8_t *value,
                             size_t n, uint8_t flags, const char fields[][256],
                             const enum IndexType *types, int nidx) {
  if (nidx < 0 || nidx > MAX_FIELDS || (nidx && !fields))
    return fail(EINVAL);
  TypedRowView view;
  if (index_row_open(ts, value, n, flags, &view))
    return -1;
  for (int i = 0; i < nidx; i++) {
    if (strnlen(fields[i], 256) == 256)
      return fail(EINVAL);
    size_t len = 0;
    int rc;
    if (!types || types[i] == IT_BTREE)
      rc = typed_index_record_key_into(ts, value, n, flags, fields[i], NULL, 0,
                                       &len);
    else if (types[i] == IT_BITMAP || types[i] == IT_TRIGRAM)
      rc = typed_index_record_payload_into(ts, value, n, flags, fields[i], NULL,
                                           0, &len);
    else
      return fail(EINVAL);
    if (rc < 0)
      return -1;
    if (types && types[i] == IT_BITMAP && len > UINT16_MAX)
      return fail(EOVERFLOW);
  }
  return 0;
}
int typed_index_old_record_key_into(const TypedSchema *ts, const uint8_t *value,
                                    size_t n, uint8_t flags, const char *spec,
                                    uint8_t *out, size_t cap, size_t *len) {
  /* Distinguish historical key oversize from the caller's small buffer:
     only a validating 544-cap admission failure is an OLD no-op. */
  if (!len)
    return fail(EINVAL);
  *len = 0;
  size_t need = 0;
  int rc =
      typed_index_record_key_into(ts, value, n, flags, spec, NULL, 0, &need);
  if (rc < 0 && errno == E2BIG) {
    errno = 0;
    return 0;
  }
  if (rc != 1)
    return rc;
  return typed_index_record_key_into(ts, value, n, flags, spec, out, cap, len);
}

int typed_index_old_record_key(const TypedSchema *ts, const uint8_t *value,
                               size_t n, uint8_t flags, const char *spec,
                               uint8_t **out, size_t *len) {
  if (out)
    *out = NULL;
  if (len)
    *len = 0;
  if (!out || !len)
    return fail(EINVAL);
  size_t need = 0;
  int rc = typed_index_old_record_key_into(ts, value, n, flags, spec, NULL, 0,
                                           &need);
  if (rc != 1)
    return rc;
  return typed_index_record_alloc(ts, value, n, flags, spec, 0, 544, out, len);
}

/* Untiered component payload for exact composite seeks. The first tier is
   supplied by the Btree wrapper; callers interleave later tiers. */
int typed_index_text_component_key_into(const TypedSchema *ts, size_t field,
                                        const char *text, size_t n,
                                        int nonfinal, uint8_t *out, size_t cap,
                                        size_t *len) {
  if (!len)
    return fail(EINVAL);
  *len = 0;
  size_t raw = 0;
  int rc = typed_index_text_key_into(ts, field, text, n, NULL, 0, &raw);
  if (rc != 1)
    return rc;
  if (!ts->packed.nfields || ts->fields[field].type != FT_VARCHAR || !nonfinal)
    return typed_index_text_key_into(ts, field, text, n, out, cap, len);
  size_t need = raw + 2;
  if (need < raw)
    return fail(EOVERFLOW);
  for (size_t i = 0; i < n; i++)
    if (!text[i]) {
      if (need == SIZE_MAX)
        return fail(EOVERFLOW);
      need++;
    }
  if (out && need > cap)
    return fail(ENOBUFS);
  if (out) {
    size_t at = 0;
    for (size_t i = 0; i < n; i++) {
      out[at++] = (uint8_t)text[i];
      if (!text[i])
        out[at++] = 255;
    }
    out[at++] = 0;
    out[at] = 0;
  }
  *len = need;
  return 1;
}
