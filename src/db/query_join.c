#include "types.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include "typed_index.h"
#include <math.h>
#include <dirent.h>
#include <stdarg.h>
/* ========== Joins (find-side) ==========
 * Parse, resolve, lookup and emit join results. Joined queries always return
 * tabular {"columns":[...],"rows":[[...]]} with fully-namespaced column names.
 */

/* typed_field_to_buf_raw and decode_idx_to_buf are declared in query_internal.h */

static int parse_one_join(const char *obj_buf, JoinSpec *j) {
    memset(j, 0, sizeof(*j));
    j->type = JOIN_INNER;

    JsonObj jobj;
    json_parse_object(obj_buf, strlen(obj_buf), &jobj);

    char *o   = json_obj_strdup(&jobj, "object");
    char *l   = json_obj_strdup(&jobj, "local");
    char *r   = json_obj_strdup(&jobj, "remote");
    char *as  = json_obj_strdup(&jobj, "as");
    char *t   = json_obj_strdup(&jobj, "type");
    char *f   = json_obj_strdup_raw(&jobj, "fields");

    if (o)  { strncpy(j->object, o, 255); free(o); }
    if (l)  { strncpy(j->local_field, l, 255); free(l); }
    if (r)  { strncpy(j->remote_field, r, 255); free(r); }
    if (as) { strncpy(j->as_name, as, 255); free(as); }
    else    { strncpy(j->as_name, j->object, 255); }
    if (t)  { if (strcmp(t, "left") == 0) j->type = JOIN_LEFT; free(t); }

    if (f) {
        const char *p = f;
        while (*p && j->proj_count < MAX_FIELDS) {
            while (*p && *p != '"') p++;
            if (!*p) break;
            p++;
            const char *s = p;
            while (*p && *p != '"') p++;
            int len = (int)(p - s);
            if (len > 0 && len < 255) {
                if (len == 3 && strncmp(s, "key", 3) == 0) {
                    j->include_remote_key = 1;
                } else {
                    memcpy(j->proj_fields[j->proj_count], s, len);
                    j->proj_fields[j->proj_count][len] = '\0';
                    j->proj_count++;
                }
            }
            if (*p == '"') p++;
        }
        free(f);
    }

    return (j->object[0] && j->local_field[0] && j->remote_field[0] && j->as_name[0]) ? 1 : 0;
}

int parse_joins_json(const char *json, JoinSpec **out, int *count) {
    *out = NULL; *count = 0;
    if (!json || !json[0]) return 0;
    const char *p = json_skip(json);
    if (*p != '[') return -1;

    int cap = 8;
    JoinSpec *arr = calloc(cap, sizeof(JoinSpec));
    int n = 0;
    p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p != '{') { p++; continue; }

        if (n >= cap) {
            cap *= 2;
            JoinSpec *t = xrealloc_or_free(arr, cap * sizeof(*t));
            if (!t) { arr = NULL; break; }
            arr = t;
            memset(&arr[n], 0, (cap - n) * sizeof(JoinSpec));
        }

        const char *obj_start = p;
        const char *obj_end = json_skip_value(p);
        size_t obj_len = obj_end - obj_start;
        char obj_buf[MAX_LINE];
        if (obj_len >= sizeof(obj_buf)) { p = obj_end; continue; }
        memcpy(obj_buf, obj_start, obj_len);
        obj_buf[obj_len] = '\0';

        if (parse_one_join(obj_buf, &arr[n])) n++;
        p = obj_end;
    }
    *out = arr; *count = n;
    return 0;
}

void free_joins(JoinSpec *arr, int n) {
    (void)n;
    free(arr);
}

/* Validate join specs against driver schema + load remote schemas + pre-resolve
   field pointers. Writes {"error":...} to OUT on failure. */
int resolve_joins(JoinSpec *joins, int n, const char *db_root,
                         const char *driver_object, FieldSchema *driver_fs) {
    for (int i = 0; i < n; i++) {
        if (strcmp(joins[i].as_name, driver_object) == 0) {
            OUT("{\"error\":\"join 'as' [%s] collides with driver object name\"}\n",
                joins[i].as_name);
            return -1;
        }
        for (int k = 0; k < i; k++) {
            if (strcmp(joins[i].as_name, joins[k].as_name) == 0) {
                OUT("{\"error\":\"duplicate join 'as' [%s]\"}\n", joins[i].as_name);
                return -1;
            }
        }
    }

    for (int i = 0; i < n; i++) {
        JoinSpec *j = &joins[i];

        j->remote_sch = load_schema(db_root, j->object);
        if (j->remote_sch.splits <= 0) {
            OUT("{\"error\":\"join remote object [%s] not found\"}\n", j->object);
            return -1;
        }
        init_field_schema(&j->remote_fs, db_root, j->object);

        j->local_is_composite = (strchr(j->local_field, '+') != NULL);
        if (!j->local_is_composite && driver_fs && driver_fs->ts) {
            int idx = typed_field_index(driver_fs->ts, j->local_field);
            if (idx >= 0) j->local_tf = &driver_fs->ts->fields[idx];
        }

        if (strcmp(j->remote_field, "key") == 0) {
            j->remote_is_key = 1;
        } else {
            j->remote_is_key = 0;
            if (!btree_idx_exists(db_root, j->object, j->remote_field, j->remote_sch.splits)) {
                OUT("{\"error\":\"join remote field [%s.%s] must be 'key' or indexed\"}\n",
                    j->object, j->remote_field);
                return -1;
            }
        }

        if (j->proj_count == 0) {
            /* No explicit fields → all non-tombstoned remote fields */
            if (j->remote_fs.ts) {
                for (int k = 0; k < j->remote_fs.ts->nfields && j->proj_count < MAX_FIELDS; k++) {
                    if (j->remote_fs.ts->fields[k].removed) continue;
                    strncpy(j->proj_fields[j->proj_count],
                            j->remote_fs.ts->fields[k].name, 255);
                    j->proj_fields[j->proj_count][255] = '\0';
                    j->proj_tfs[j->proj_count] = &j->remote_fs.ts->fields[k];
                    j->proj_count++;
                }
            }
        } else {
            /* Belt-and-suspenders re-term — proj_fields[] is null-terminated
               at population (parse_one_join via memcpy + explicit '\0', and
               the auto-fill above via strncpy + explicit '\0'), but Coverity
               can't propagate the post-condition to consumers. */
            for (int k = 0; k < j->proj_count; k++) j->proj_fields[k][255] = '\0';
            for (int k = 0; k < j->proj_count; k++) {
                if (strchr(j->proj_fields[k], '+')) {
                    OUT("{\"error\":\"composite projection field [%s] not supported\"}\n",
                        j->proj_fields[k]);
                    return -1;
                }
                if (j->remote_fs.ts) {
                    int idx = typed_field_index(j->remote_fs.ts, j->proj_fields[k]);
                    if (idx >= 0) {
                        j->proj_tfs[k] = &j->remote_fs.ts->fields[idx];
                    } else {
                        OUT("{\"error\":\"join field [%s.%s] not found\"}\n",
                            j->object, j->proj_fields[k]);
                        return -1;
                    }
                }
            }
        }
    }
    return 0;
}

/* Extract the local field value from a driver record into buf.
   Handles composite (a+b → concat values). Returns written length (0 = empty).
   A NULL local field also returns 0: NULL joins nothing, and probing the
   zero-encoded bytes would falsely match a remote zero-valued key. */


/* Forward decl — definition lives near btree_dispatch below. */




/* Btree search callback — captures the first hash hit. _Atomic on `found`
   because btree_idx_search fans out across idx_shards in parallel and
   multiple shard workers can race to invoke this cb on the shared hit
   struct. Compare-exchange ensures exactly one winner records its hash. */
typedef struct { uint8_t hash[16]; _Atomic int found; } JoinBtHit;
static int join_bt_first_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    JoinBtHit *h = (JoinBtHit *)ctx;
    int expected = 0;
    if (!atomic_compare_exchange_strong_explicit(
            &h->found, &expected, 1,
            memory_order_acq_rel, memory_order_relaxed)) {
        return -1;  /* another worker already recorded the hash */
    }
    memcpy(h->hash, hash16, 16);
    return -1;  /* stop after first match */
}

typedef struct {
    uint8_t (*hashes)[16];size_t cap;
    _Atomic size_t count;
} JoinCandidates;
typedef struct {size_t field,offset,length;} JoinComponent;
static int join_candidate_cb(const char *value,size_t length,const uint8_t *hash,void *arg){
    (void)value;(void)length;JoinCandidates *c=arg;
    size_t at=atomic_fetch_add_explicit(&c->count,1,memory_order_relaxed);
    if(c->hashes&&at<c->cap)memcpy(c->hashes[at],hash,16);
    return 0;
}
/* Legacy variable composite bytes can collide across component boundaries.
   The shared codec stays unchanged. Validate components after all index
   callbacks have returned, never fetching while a Btree lock is held. */
static int join_candidate_components(const JoinSpec *j,const char *root,
    const uint8_t *key,size_t keylen,const JoinComponent *parts,size_t count,
    size_t budget,RecordRef *out){
    JoinCandidates candidates={0};
    btree_idx_search(root,j->object,j->remote_field,j->remote_sch.splits,
                     (const char*)key+1,keylen-1,join_candidate_cb,&candidates);
    size_t capacity=atomic_load_explicit(&candidates.count,memory_order_relaxed);
    if(!capacity)return 0;
    if(capacity>budget/sizeof(*candidates.hashes)){errno=ENOBUFS;return -1;}
    candidates.hashes=malloc(capacity*sizeof(*candidates.hashes));
    if(!candidates.hashes)return -1;
    candidates.cap=capacity;atomic_store_explicit(&candidates.count,0,memory_order_relaxed);
    btree_idx_search(root,j->object,j->remote_field,j->remote_sch.splits,
                     (const char*)key+1,keylen-1,join_candidate_cb,&candidates);
    size_t actual=atomic_load_explicit(&candidates.count,memory_order_relaxed);
    if(actual>capacity){free(candidates.hashes);errno=ENOBUFS;return -1;}
    int found=0;
    for(size_t i=0;i<actual;i++){
        if(read_record_ref(root,j->object,&j->remote_sch,candidates.hashes[i],out))continue;
        int equal=1;
        for(size_t p=0;p<count;p++){
            size_t n=0;
            int rc=typed_index_field_key_into(j->remote_fs.ts,parts[p].field,
                out->val,out->vlen,out->hdr2,NULL,0,&n);
            if(rc<0){found=-1;equal=0;break;}
            if(!rc||n!=parts[p].length){equal=0;break;}
            uint8_t component[544];size_t written=0;
            rc=typed_index_field_key_into(j->remote_fs.ts,parts[p].field,
                out->val,out->vlen,out->hdr2,component,sizeof(component),&written);
            if(rc<0){found=-1;equal=0;break;}
            if(rc!=1||written!=n||memcmp(component,key+parts[p].offset,n)){equal=0;break;}
        }
        if(equal){found=1;break;}
        int error=errno;release_record_ref(out);memset(out,0,sizeof(*out));errno=error;
        if(found<0)break;
    }
    int error=errno;free(candidates.hashes);errno=error;return found;
}

/* Look up the remote record for one join. Returns 1 if found, 0 otherwise.
   On success, *out_rr is populated; caller must release_record_ref(out_rr).
   Storage-version-agnostic: read_record_ref dispatches v1/v2 internally. */
/* Resolve one join from complete logical values, not a fixed-size display
   buffer or bytes encoded for the driver's potentially different layout. */
int resolve_join_record(const JoinSpec *j,const char *root,
    const TypedSchema *driver_ts,const uint8_t *raw,size_t len,uint8_t flags,
    size_t budget,RecordRef *out){
    if(!j||!driver_ts||!out){errno=EINVAL;return -1;}
    memset(out,0,sizeof(*out));
    if(j->remote_is_key){
        char *text=NULL;size_t n=0;
        int rc=typed_query_named_text(driver_ts,raw,len,flags,j->local_field,budget,&text,&n);
        if(rc<=0)return rc;
        const uint8_t *key=(const uint8_t*)text;size_t keylen=n;uint8_t binary[16];
        if(j->remote_sch.auto_key==AK_UUID){
            if(n!=36||parse_uuid_string(text,binary)){free(text);return 0;}
            key=binary;keylen=16;
        }else if(j->remote_sch.auto_key==AK_SEQ){
            int64_t number=0;
            if(memchr(text,0,n)||parse_seq_key(text,&number)){free(text);return 0;}
            for(size_t i=0;i<8;i++)binary[i]=(uint8_t)((uint64_t)number>>(56-8*i));
            key=binary;keylen=8;
        }
        if(keylen>(size_t)j->remote_sch.max_key){free(text);return 0;}
        uint8_t hash[16];compute_hash_raw((const char*)key,keylen,hash);
        int found=read_record_ref(root,j->object,&j->remote_sch,hash,out)==0;
        if(found&&(out->klen!=keylen||memcmp(out->key,key,keylen))){release_record_ref(out);found=0;}
        free(text);return found;
    }
    if(!j->remote_fs.ts){errno=EINVAL;return -1;}
    TypedRowView view;
    if(typed_row_open(driver_ts,raw,len,flags,&view))return -1;
    char local[256],remote[256];
    memcpy(local,j->local_field,sizeof(local));local[255]=0;
    memcpy(remote,j->remote_field,sizeof(remote));remote[255]=0;
    char *ls=NULL,*rs=NULL,*l=strtok_r(local,"+",&ls),*r=strtok_r(remote,"+",&rs);
    uint8_t key[544];size_t used=0,count=0;int ambiguous=0;
    JoinComponent parts[MAX_FIELDS];
    while(l&&r){
        int li=typed_field_index(driver_ts,l),ri=typed_field_index(j->remote_fs.ts,r);
        if(li<0||ri<0)return 0;
        TypedRowSpan span;if(typed_row_span(&view,(size_t)li,&span))return -1;
        if(span.is_null)return 0;
        char *text=NULL;size_t n=0;
        int rc=typed_query_named_text(driver_ts,raw,len,flags,l,budget,&text,&n);
        if(rc<=0)return rc;
        size_t width=0;
        rc=typed_index_text_key_into(j->remote_fs.ts,(size_t)ri,text,n,NULL,0,&width);
        if(rc<=0){int error=errno;free(text);if(!rc||error==EDOM)return 0;errno=error;return -1;}
        char *next_l=strtok_r(NULL,"+",&ls),*next_r=strtok_r(NULL,"+",&rs);
        int escape=next_r&&j->remote_fs.ts->packed.nfields&&
                   j->remote_fs.ts->fields[ri].type==FT_VARCHAR;
        size_t extra=escape?2:0;
        if(escape)for(size_t i=0;i<n;i++)extra+=text[i]==0;
        /* No admitted remote key can match a larger complete probe. */
        if(used>=sizeof(key)||width>sizeof(key)-used-1||
           extra>sizeof(key)-used-1-width){free(text);return 0;}
        if(count==MAX_FIELDS){free(text);errno=EINVAL;return -1;}
        parts[count++]=(JoinComponent){(size_t)ri,used,width+1};
        if(next_r&&!j->remote_fs.ts->packed.nfields&&j->remote_fs.ts->fields[ri].type==FT_VARCHAR)ambiguous=1;
        key[used++]=0;
        if(escape){
            for(size_t i=0;i<n;i++){key[used++]=(uint8_t)text[i];if(!text[i])key[used++]=255;}
            key[used++]=0;key[used++]=0;
        }else{
            size_t written=0;
            rc=typed_index_text_key_into(j->remote_fs.ts,(size_t)ri,text,n,
                                          key+used,sizeof(key)-used,&written);
            if(rc!=1||written!=width){int error=errno;free(text);errno=error?error:EILSEQ;return -1;}
            used+=written;
        }
        free(text);l=next_l;r=next_r;
    }
    if(l||r||!used)return 0;
    if(ambiguous)return join_candidate_components(j,root,key,used,parts,count,budget,out);
    JoinBtHit hit={{0},0};
    btree_idx_search(root,j->object,j->remote_field,j->remote_sch.splits,
                     (const char*)key+1,used-1,join_bt_first_cb,&hit);
    if(!atomic_load_explicit(&hit.found,memory_order_acquire))return 0;
    return read_record_ref(root,j->object,&j->remote_sch,hit.hash,out)==0;
}



/* Retained bounded-buffer compatibility entry for the regression rig. */
int buf_driver_values(const uint8_t *raw,size_t len,FieldSchema *fs,const char **names,int count,uint8_t flags,char *out,size_t cap){
    return typed_query_columns_into(raw,len,flags,fs,names,count,out,cap);
}

/* Emit the CSV header line for a joined query (driver + joins, prefixed columns). */
void emit_joined_csv_header(const char *driver_object,
                                   FieldSchema *driver_fs,
                                   const JoinSpec *joins, int njoins,
                                   const char **driver_proj, int driver_proj_count,
                                   char delim) {
    OUT("%s.key", driver_object);
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++) {
            char d[2] = { delim, '\0' }; OUT("%s%s.%s", d, driver_object, driver_proj[i]);
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            char d[2] = { delim, '\0' }; OUT("%s%s.%s", d, driver_object, driver_fs->ts->fields[i].name);
        }
    }
    for (int i = 0; i < njoins; i++) {
        const JoinSpec *j = &joins[i];
        if (j->include_remote_key) { char d[2] = { delim, '\0' }; OUT("%s%s.key", d, j->as_name); }
        for (int k = 0; k < j->proj_count; k++) {
            char d[2] = { delim, '\0' }; OUT("%s%s.%s", d, j->as_name, j->proj_fields[k]);
        }
    }
    OUT("\n");
}

/* Emit the columns header for a joined query (main thread only). */
void emit_joined_columns(const char *driver_object,
                                FieldSchema *driver_fs,
                                const JoinSpec *joins, int njoins,
                                const char **driver_proj, int driver_proj_count) {
    OUT("{\"columns\":[\"%s.key\"", driver_object);
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++)
            OUT(",\"%s.%s\"", driver_object, driver_proj[i]);
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            OUT(",\"%s.%s\"", driver_object, driver_fs->ts->fields[i].name);
        }
    }
    for (int i = 0; i < njoins; i++) {
        const JoinSpec *j = &joins[i];
        if (j->include_remote_key) OUT(",\"%s.key\"", j->as_name);
        for (int k = 0; k < j->proj_count; k++)
            OUT(",\"%s.%s\"", j->as_name, j->proj_fields[k]);
    }
    OUT("],\"rows\":[");
}

int adv_search_cb(const SlotHeader *hdr,const uint8_t *block,void *ctx){
    AdvSearchCtx *sc=ctx;
    if(sc->limit>0&&sc->printed>=sc->limit)return 1;
    if(query_deadline_tick(sc->deadline,&sc->dl_counter))return 1;
    char key[1100];
    const Schema *schema=sc->fs&&sc->fs->auto_key!=AK_NONE?&sc->fs->auto_key_schema_snapshot:NULL;
    if(format_wire_key(schema,(const char*)block,hdr->key_len,key,sizeof(key))<0){
        pthread_mutex_lock(&sc->lock);sc->predicate_error=EILSEQ;pthread_mutex_unlock(&sc->lock);return 1;
    }
    if(is_excluded(&sc->excluded,key))return 0;
    const uint8_t *raw=block+hdr->key_len;uint8_t flags=(uint8_t)(hdr->flag>>8);
    int matched=0;
    int rc=sc->fast_cc?match_typed_checked(raw,hdr->value_len,sc->fast_cc,sc->fs,flags,&matched):
        criteria_match_tree_checked(raw,hdr->value_len,sc->tree,sc->fs,flags,&matched);
    if(rc){int error=errno?errno:EILSEQ;pthread_mutex_lock(&sc->lock);
        sc->predicate_error=error;pthread_mutex_unlock(&sc->lock);return 1;}
    if(!matched)return 0;
    RecordRef *refs=NULL;int dropped=0,error=0;
    if(sc->njoins){
        size_t bytes=(size_t)sc->njoins*sizeof(*refs);
        if(bytes>g_query_buffer_max_bytes){error=ENOBUFS;goto done;}
        refs=calloc((size_t)sc->njoins,sizeof(*refs));
        if(!refs){error=ENOMEM;goto done;}
        for(int i=0;i<sc->njoins;i++){
            int found=resolve_join_record(&sc->joins[i],sc->db_root,sc->fs->ts,
                                          raw,hdr->value_len,flags,
                                          g_query_buffer_max_bytes-bytes,&refs[i]);
            if(found<0){error=errno?errno:EILSEQ;goto done;}
            if(!found&&sc->joins[i].type==JOIN_INNER){dropped=1;break;}
        }
    }
    if(!dropped){
        /* Keep the existing emit critical section and limit recheck. */
        pthread_mutex_lock(&sc->lock);
        if(sc->limit<=0||sc->printed<sc->limit){
            sc->count++;
            if(sc->count>sc->offset){
                size_t charge=(size_t)sc->njoins*sizeof(*refs);
                if(typed_query_emit_row(g_out?g_out:stdout,key,strlen(key),raw,hdr->value_len,
                    flags,sc->fs,sc->proj_fields,sc->proj_count,sc->rows_fmt,sc->dict_fmt,
                    sc->csv_delim,sc->printed>0,sc->joins,sc->njoins,refs,
                    g_query_buffer_max_bytes-charge))error=errno?errno:EILSEQ;
                else sc->printed++;
            }
        }
        if(error)sc->predicate_error=error;
        pthread_mutex_unlock(&sc->lock);
    }
done:
    if(refs){for(int i=0;i<sc->njoins;i++)release_record_ref(&refs[i]);free(refs);}
    if(error){pthread_mutex_lock(&sc->lock);sc->predicate_error=error;
        pthread_mutex_unlock(&sc->lock);return 1;}
    return sc->limit>0&&sc->printed>=sc->limit;
}
