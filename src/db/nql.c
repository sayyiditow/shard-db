#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stddef.h>
#include "nql.h"
#include "types.h"
#include "query_internal.h" /* JoinSpec, free_joins, MAX_FIELDS */

/* ── Lexer ────────────────────────────────────────────────────────── */

static void lex_init(NqlLexer *l, const char *src) {
    memset(l, 0, sizeof *l);
    l->src = src;
}

static void skip_ws(NqlLexer *l) {
    while (l->src[l->pos] && isspace((unsigned char)l->src[l->pos]))
        l->pos++;
}

static void lex_scan(NqlLexer *l) {
    skip_ws(l);
    NqlToken *t = &l->cur;
    memset(t, 0, sizeof *t);
    char c = l->src[l->pos];

    if (c == '\0') { t->type = TOK_EOF; return; }

    /* Single-quoted string: content stored without quotes */
    if (c == '\'') {
        l->pos++;
        int i = 0;
        while (l->src[l->pos] && l->src[l->pos] != '\'' && i < (int)sizeof(t->text)-1)
            t->text[i++] = l->src[l->pos++];
        if (l->src[l->pos] == '\'') l->pos++;
        t->text[i] = '\0';
        t->type = TOK_STRING;
        return;
    }

    /* Two-char symbols */
    if (c == '!' && l->src[l->pos+1] == '=') { t->type = TOK_SYM_NEQ; strcpy(t->text,"!="); l->pos+=2; return; }
    if (c == '>' && l->src[l->pos+1] == '=') { t->type = TOK_SYM_GTE; strcpy(t->text,">="); l->pos+=2; return; }
    if (c == '<' && l->src[l->pos+1] == '=') { t->type = TOK_SYM_LTE; strcpy(t->text,"<="); l->pos+=2; return; }

    /* Single-char symbols */
    if (c == '=') { t->type=TOK_SYM_EQ;  t->text[0]='='; l->pos++; return; }
    if (c == '>') { t->type=TOK_SYM_GT;  t->text[0]='>'; l->pos++; return; }
    if (c == '<') { t->type=TOK_SYM_LT;  t->text[0]='<'; l->pos++; return; }
    if (c == '(') { t->type=TOK_LPAREN;  t->text[0]='('; l->pos++; return; }
    if (c == ')') { t->type=TOK_RPAREN;  t->text[0]=')'; l->pos++; return; }
    if (c == '[') { t->type=TOK_LBRACKET;t->text[0]='['; l->pos++; return; }
    if (c == ']') { t->type=TOK_RBRACKET;t->text[0]=']'; l->pos++; return; }
    if (c == ',') { t->type=TOK_COMMA;   t->text[0]=','; l->pos++; return; }

    /* Negative number */
    if (c == '-' && isdigit((unsigned char)l->src[l->pos+1])) {
        int i = 0;
        t->text[i++] = l->src[l->pos++];
        while (l->src[l->pos] && (isdigit((unsigned char)l->src[l->pos]) || l->src[l->pos] == '.') && i < (int)sizeof(t->text)-1)
            t->text[i++] = l->src[l->pos++];
        t->text[i] = '\0'; t->type = TOK_NUMBER; return;
    }

    /* Positive number */
    if (isdigit((unsigned char)c)) {
        int i = 0;
        while (l->src[l->pos] && (isdigit((unsigned char)l->src[l->pos]) || l->src[l->pos] == '.') && i < (int)sizeof(t->text)-1)
            t->text[i++] = l->src[l->pos++];
        t->text[i] = '\0'; t->type = TOK_NUMBER; return;
    }

    /* Identifier / keyword */
    if (isalpha((unsigned char)c) || c == '_') {
        int i = 0;
        while (l->src[l->pos] &&
               (isalnum((unsigned char)l->src[l->pos]) || l->src[l->pos]=='_' ||
                l->src[l->pos]=='.' || l->src[l->pos]=='+') &&
               i < (int)sizeof(t->text)-1)
            t->text[i++] = l->src[l->pos++];
        t->text[i] = '\0';
        if (strcmp(t->text,"true")==0 || strcmp(t->text,"false")==0)
            t->type = TOK_BOOL;
        else
            t->type = TOK_IDENT;
        return;
    }

    snprintf(l->err, sizeof l->err, "unexpected character '%c' at pos %d", c, l->pos);
    t->type = TOK_ERR;
    l->pos++;
}

static NqlToken lex_peek(NqlLexer *l) {
    if (!l->ready) { lex_scan(l); l->ready = 1; }
    return l->cur;
}

static NqlToken lex_next(NqlLexer *l) {
    if (!l->ready) lex_scan(l);
    NqlToken t = l->cur;
    l->ready = 0;
    return t;
}

static int lex_peek_kw(NqlLexer *l, const char *kw) {
    NqlToken t = lex_peek(l);
    return t.type == TOK_IDENT && strcasecmp(t.text, kw) == 0;
}

/* ── Filter parser ────────────────────────────────────────────────── */

static CriteriaNode *make_and(CriteriaNode *a, CriteriaNode *b) {
    CriteriaNode *n = calloc(1, sizeof *n);
    n->kind = CNODE_AND;
    n->children = malloc(2 * sizeof(CriteriaNode *));
    n->children[0] = a; n->children[1] = b;
    n->n_children = 2;
    return n;
}

static CriteriaNode *make_or(CriteriaNode *a, CriteriaNode *b) {
    CriteriaNode *n = calloc(1, sizeof *n);
    n->kind = CNODE_OR;
    n->children = malloc(2 * sizeof(CriteriaNode *));
    n->children[0] = a; n->children[1] = b;
    n->n_children = 2;
    return n;
}

static int read_value(NqlLexer *l, char *buf, size_t bufsz) {
    NqlToken t = lex_peek(l);
    if (t.type==TOK_STRING || t.type==TOK_NUMBER ||
        t.type==TOK_BOOL   || t.type==TOK_IDENT) {
        lex_next(l);
        snprintf(buf, bufsz, "%s", t.text);
        return 0;
    }
    snprintf(l->err, sizeof l->err, "expected value, got '%s'", t.text);
    return -1;
}

static int read_list(NqlLexer *l, SearchCriterion *c) {
    NqlToken open = lex_next(l);
    NqlTokenType close = (open.type == TOK_LPAREN) ? TOK_RPAREN : TOK_RBRACKET;
    c->in_cap = 8; c->in_count = 0;
    c->in_values = malloc(c->in_cap * sizeof(char *));
    if (!c->in_values) return -1;
    while (1) {
        NqlToken t = lex_peek(l);
        if (t.type == close) { lex_next(l); break; }
        if (t.type == TOK_EOF) { snprintf(l->err, sizeof l->err, "unterminated list"); return -1; }
        if (t.type == TOK_COMMA) { lex_next(l); continue; }
        char vbuf[1024];
        if (read_value(l, vbuf, sizeof vbuf) < 0) return -1;
        if (c->in_count >= c->in_cap) {
            c->in_cap *= 2;
            char **tmp = realloc(c->in_values, c->in_cap * sizeof(char *));
            if (!tmp) return -1;
            c->in_values = tmp;
        }
        c->in_values[c->in_count++] = strdup(vbuf);
    }
    return 0;
}

static CriteriaNode *parse_or_expr(NqlLexer *l);  /* forward decl */

static CriteriaNode *parse_predicate(NqlLexer *l) {
    NqlToken ft = lex_next(l);
    if (ft.type != TOK_IDENT) {
        snprintf(l->err, sizeof l->err, "expected field name, got '%s'", ft.text);
        return NULL;
    }

    CriteriaNode *node = calloc(1, sizeof *node);
    node->kind = CNODE_LEAF;
    snprintf(node->leaf.field, sizeof node->leaf.field, "%s", ft.text);

    NqlToken op = lex_peek(l);
    char op_str[64] = {0};

    if      (op.type == TOK_SYM_EQ)  { lex_next(l); strcpy(op_str,"eq"); }
    else if (op.type == TOK_SYM_NEQ) { lex_next(l); strcpy(op_str,"neq"); }
    else if (op.type == TOK_SYM_GT)  { lex_next(l); strcpy(op_str,"gt"); }
    else if (op.type == TOK_SYM_LT)  { lex_next(l); strcpy(op_str,"lt"); }
    else if (op.type == TOK_SYM_GTE) { lex_next(l); strcpy(op_str,"gte"); }
    else if (op.type == TOK_SYM_LTE) { lex_next(l); strcpy(op_str,"lte"); }
    else if (op.type == TOK_IDENT) {
        char kw[64];
        snprintf(kw, sizeof kw, "%s", op.text);
        for (char *p = kw; *p; p++) *p = (char)tolower((unsigned char)*p);

        if (strcmp(kw, "not") == 0) {
            lex_next(l);
            NqlToken nxt = lex_next(l);
            if (nxt.type != TOK_IDENT) {
                snprintf(l->err, sizeof l->err, "expected op after 'not', got '%s'", nxt.text);
                free(node); return NULL;
            }
            snprintf(op_str, sizeof op_str, "not_%s", nxt.text);
            for (char *p = op_str; *p; p++) *p = (char)tolower((unsigned char)*p);
        } else {
            lex_next(l);
            if      (strcmp(kw,"nexists")==0)    strcpy(op_str,"not_exists");
            else if (strcmp(kw,"less_eq")==0)    strcpy(op_str,"lte");
            else if (strcmp(kw,"greater_eq")==0) strcpy(op_str,"gte");
            else                                  snprintf(op_str,sizeof op_str,"%s",kw);
        }
    } else {
        snprintf(l->err, sizeof l->err, "expected operator after field '%s'", ft.text);
        free(node); return NULL;
    }

    /* Map string → enum via the existing parse_op() in query.c */
    node->leaf.op = parse_op(op_str);
    if (node->leaf.op == OP_UNKNOWN) {
        snprintf(l->err, sizeof l->err, "unknown operator '%s'", op_str);
        free(node);
        return NULL;
    }

    /* No-value ops */
    if (node->leaf.op == OP_EXISTS || node->leaf.op == OP_NOT_EXISTS)
        return node;

    /* IN / NOT_IN: expect ( list ) or [ list ] */
    if (node->leaf.op == OP_IN || node->leaf.op == OP_NOT_IN) {
        NqlToken nx = lex_peek(l);
        if (nx.type != TOK_LPAREN && nx.type != TOK_LBRACKET) {
            snprintf(l->err, sizeof l->err, "expected '(' or '[' after in/not_in");
            free(node); return NULL;
        }
        if (read_list(l, &node->leaf) < 0) { free(node); return NULL; }
        return node;
    }

    /* BETWEEN / LEN_BETWEEN: value1 "and" value2 */
    if (node->leaf.op == OP_BETWEEN || node->leaf.op == OP_LEN_BETWEEN) {
        if (read_value(l, node->leaf.value, sizeof node->leaf.value) < 0) { free(node); return NULL; }
        if (!lex_peek_kw(l,"and")) {
            snprintf(l->err, sizeof l->err, "expected 'and' between BETWEEN bounds");
            free(node); return NULL;
        }
        lex_next(l); /* consume 'and' — NOT a logical AND */
        if (read_value(l, node->leaf.value2, sizeof node->leaf.value2) < 0) { free(node); return NULL; }
        if (node->leaf.op == OP_LEN_BETWEEN) {
            node->leaf.len_target  = atoll(node->leaf.value);
            node->leaf.len_target2 = atoll(node->leaf.value2);
        }
        return node;
    }

    /* OP_LEN_* scalar ops: pre-parse len_target */
    if (node->leaf.op >= OP_LEN_EQ && node->leaf.op <= OP_LEN_GREATER_EQ) {
        if (read_value(l, node->leaf.value, sizeof node->leaf.value) < 0) { free(node); return NULL; }
        node->leaf.len_target = atoll(node->leaf.value);
        return node;
    }

    /* All other binary ops: single value */
    if (read_value(l, node->leaf.value, sizeof node->leaf.value) < 0) { free(node); return NULL; }
    return node;
}

static CriteriaNode *parse_atom(NqlLexer *l) {
    if (lex_peek(l).type == TOK_LPAREN) {
        lex_next(l);
        CriteriaNode *inner = parse_or_expr(l);
        if (!inner) return NULL;
        if (lex_next(l).type != TOK_RPAREN) {
            snprintf(l->err, sizeof l->err, "expected ')'");
            free_criteria_tree(inner); return NULL;
        }
        return inner;
    }
    return parse_predicate(l);
}

static CriteriaNode *parse_and_expr(NqlLexer *l) {
    CriteriaNode *left = parse_atom(l);
    if (!left) return NULL;
    while (lex_peek_kw(l,"and")) {
        lex_next(l);
        CriteriaNode *right = parse_atom(l);
        if (!right) { free_criteria_tree(left); return NULL; }
        left = make_and(left, right);
    }
    return left;
}

static CriteriaNode *parse_or_expr(NqlLexer *l) {
    CriteriaNode *left = parse_and_expr(l);
    if (!left) return NULL;
    while (lex_peek_kw(l,"or")) {
        lex_next(l);
        CriteriaNode *right = parse_and_expr(l);
        if (!right) { free_criteria_tree(left); return NULL; }
        left = make_or(left, right);
    }
    return left;
}

CriteriaNode *nql_parse_filter(const char *src, char *err_out, size_t err_sz) {
    if (!src || !*src) return NULL;
    NqlLexer l;
    lex_init(&l, src);
    CriteriaNode *tree = parse_or_expr(&l);
    if (lex_peek(&l).type != TOK_EOF) {
        const char *msg = l.err[0] ? l.err : "unexpected token at end of filter";
        if (err_out) snprintf(err_out, err_sz, "%s", msg);
        free_criteria_tree(tree);
        return NULL;
    }
    if (!tree && l.err[0] && err_out)
        snprintf(err_out, err_sz, "%s", l.err);
    return tree;
}

/* ── Aggregate spec parser ────────────────────────────────────────── */

static int parse_nql_aggs(const char *src, NqlAggSpec **out) {
    int cap = 8, n = 0;
    NqlAggSpec *specs = calloc(cap, sizeof *specs);
    const char *p = src;
    while (*p) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;
        char fn[32] = {0}; int fi = 0;
        while (*p && isalpha((unsigned char)*p) && fi < (int)sizeof(fn)-1)
            fn[fi++] = (char)tolower((unsigned char)*p++);
        while (*p && isspace((unsigned char)*p)) p++;
        if (*p != '(') { free(specs); return -1; }
        p++;
        char field[256] = {0}; int gi = 0;
        while (*p && isspace((unsigned char)*p)) p++;
        while (*p && *p != ')' && gi < (int)sizeof(field)-1)
            field[gi++] = *p++;
        while (gi > 0 && isspace((unsigned char)field[gi-1])) field[--gi] = '\0';
        if (*p != ')') { free(specs); return -1; }
        p++;
        if (n >= cap) {
            cap *= 2;
            NqlAggSpec *t = realloc(specs, cap * sizeof *t);
            if (!t) { free(specs); return -1; }
            specs = t;
        }
        snprintf(specs[n].fn,    sizeof specs[n].fn,    "%s", fn);
        snprintf(specs[n].field, sizeof specs[n].field, "%s", field);
        n++;
        while (*p && isspace((unsigned char)*p)) p++;
        if (*p == ',') p++;
    }
    *out = specs;
    return n;
}

/* ── Command tokeniser (whitespace-split, respects single quotes) ── */

static int cmd_split(const char *src, char *buf, size_t bufsz,
                     char **argv, int maxargs) {
    snprintf(buf, bufsz, "%s", src);
    int n = 0; char *p = buf;
    while (*p && n < maxargs) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;
        if (*p == '\'') {
            p++; argv[n++] = p;
            while (*p && *p != '\'') p++;
            if (*p) *p++ = '\0';
        } else {
            argv[n++] = p;
            while (*p && !isspace((unsigned char)*p)) p++;
            if (*p) *p++ = '\0';
        }
    }
    return n;
}

/* ── nql_parse_command ────────────────────────────────────────────── */

int nql_parse_command(const char *src, NqlCommand *out) {
    memset(out, 0, sizeof *out);
    char buf[8192]; char *argv[256];
    int argc = cmd_split(src, buf, sizeof buf, argv, 256);
    if (argc < 3) {
        snprintf(out->err, sizeof out->err, "NQL needs at least: <mode> <dir> <obj>");
        return -1;
    }
    int i = 0;
    if      (strcasecmp(argv[i],"find")==0)      out->mode = NQL_FIND;
    else if (strcasecmp(argv[i],"count")==0)     out->mode = NQL_COUNT;
    else if (strcasecmp(argv[i],"aggregate")==0) out->mode = NQL_AGGREGATE;
    else {
        snprintf(out->err, sizeof out->err, "NQL: unknown mode '%s'", argv[i]);
        return -1;
    }
    i++;
    snprintf(out->dir, sizeof out->dir, "%s", argv[i++]);
    snprintf(out->obj, sizeof out->obj, "%s", argv[i++]);

    if (out->mode == NQL_AGGREGATE) {
        if (!out->filter && i < argc && argv[i][0] != '-' && strchr(argv[i],'(') == NULL) {
            char ferr[256];
            out->filter = nql_parse_filter(argv[i], ferr, sizeof ferr);
            if (!out->filter && ferr[0]) { snprintf(out->err,sizeof out->err,"%s",ferr); return -1; }
            i++;
        }
        if (i < argc && argv[i][0] != '-') {
            int n = parse_nql_aggs(argv[i], &out->aggs);
            if (n < 0) { snprintf(out->err,sizeof out->err,"invalid agg spec '%s'",argv[i]); return -1; }
            out->naggs = n; i++;
        }
    } else if (i < argc && argv[i][0] != '-') {
        char ferr[256];
        out->filter = nql_parse_filter(argv[i], ferr, sizeof ferr);
        if (!out->filter && ferr[0]) { snprintf(out->err,sizeof out->err,"%s",ferr); return -1; }
        i++;
    }

    while (i < argc) {
        if      (!strcmp(argv[i],"--limit")    && i+1<argc) {
            char *endp;
            long v = strtol(argv[++i], &endp, 10);
            if (endp == argv[i] || *endp != '\0' || v < 0) { snprintf(out->err, sizeof out->err, "--limit must be a non-negative integer, got '%s'", argv[i]); return -1; }
            out->limit = (int)v; i++;
        }
        else if (!strcmp(argv[i],"--offset")   && i+1<argc) {
            char *endp;
            long v = strtol(argv[++i], &endp, 10);
            if (endp == argv[i] || *endp != '\0' || v < 0) { snprintf(out->err, sizeof out->err, "--offset must be a non-negative integer, got '%s'", argv[i]); return -1; }
            out->offset = (int)v; i++;
        }
        else if (!strcmp(argv[i],"--fields")   && i+1<argc) { snprintf(out->fields,  sizeof out->fields,  "%s",argv[++i]); i++; }
        else if (!strcmp(argv[i],"--format")   && i+1<argc) { snprintf(out->format,  sizeof out->format,  "%s",argv[++i]); i++; }
        else if (!strcmp(argv[i],"--group-by") && i+1<argc) { snprintf(out->group_by,sizeof out->group_by,"%s",argv[++i]); i++; }
        else if (!strcmp(argv[i],"--auth")     && i+1<argc) { snprintf(out->auth,    sizeof out->auth,    "%s",argv[++i]); i++; }
        else if (!strcmp(argv[i],"--order-by") && i+1<argc) {
            char *spec = argv[++i]; i++;
            char *colon = strrchr(spec, ':');
            if (colon) {
                *colon = '\0';
                snprintf(out->order_dir,sizeof out->order_dir,"%s",colon+1);
                if (spec[0] == '\0') {
                    snprintf(out->err, sizeof out->err, "--order-by requires a field name before ':'");
                    return -1;
                }
            }
            else         snprintf(out->order_dir,sizeof out->order_dir,"asc");
            snprintf(out->order_by, sizeof out->order_by, "%s", spec);
        }
        else if (!strcmp(argv[i],"--filter") && i+1<argc) {
            char ferr[256];
            out->filter = nql_parse_filter(argv[++i], ferr, sizeof ferr);
            if (!out->filter && ferr[0]) { snprintf(out->err,sizeof out->err,"%s",ferr); return -1; }
            i++;
        }
        else if (!strcmp(argv[i],"--having") && i+1<argc) {
            char ferr[256];
            out->having = nql_parse_filter(argv[++i], ferr, sizeof ferr);
            if (!out->having && ferr[0]) { snprintf(out->err,sizeof out->err,"%s",ferr); return -1; }
            i++;
        }
        else if (!strcmp(argv[i],"--order")   && i+1<argc) {
            const char *d = argv[++i];
            if (strcmp(d,"desc")==0 || strcmp(d,"DESC")==0 || strcmp(d,"Desc")==0)
                snprintf(out->order_dir,sizeof out->order_dir,"desc");
            else if (strcmp(d,"asc")==0 || strcmp(d,"ASC")==0 || strcmp(d,"Asc")==0)
                snprintf(out->order_dir,sizeof out->order_dir,"asc");
            else { snprintf(out->err, sizeof out->err, "invalid order direction '%s'; use 'asc' or 'desc'", d); return -1; }
            i++;
        }
        else if (!strcmp(argv[i],"--cursor")  && i+1<argc) { snprintf(out->cursor,   sizeof out->cursor,   "%s",argv[++i]); i++; }
        else if (!strcmp(argv[i],"--explain"))              { out->explain = 1; i++; }
        else if (!strcmp(argv[i],"--join") && i+1<argc) {
            /* --join object local=remote [as alias] [fields f1,f2] [left] */
            i++;
            if (i >= argc) { snprintf(out->err,sizeof out->err,"NQL: --join requires arguments"); return -1; }
            const char *object = argv[i++];
            if (i >= argc) { snprintf(out->err,sizeof out->err,"NQL: --join requires local=remote"); return -1; }
            const char *local_eq_remote = argv[i++];
            const char *eq = strchr(local_eq_remote, '=');
            if (!eq) { snprintf(out->err,sizeof out->err,"NQL: --join expected local=remote, got '%s'",local_eq_remote); return -1; }
            char local[256] = {0}, remote[256] = {0};
            int llen = (int)(eq - local_eq_remote);
            if (llen > 255) llen = 255;
            memcpy(local, local_eq_remote, (size_t)llen);
            strncpy(remote, eq + 1, 255);
            char as_name[256] = {0};
            char fields[1024] = {0};
            int left_join = 0;
            /* Consume optional tokens. Stop at anything that looks like a
               flag (-), a quote ('), or end of args. */
            while (i < argc && argv[i][0] != '-' && argv[i][0] != '\'') {
                if (!strcmp(argv[i],"as") && i+2<=argc) {
                    strncpy(as_name, argv[i+1], 255);
                    i += 2;
                } else if (!strcmp(argv[i],"fields") && i+2<=argc) {
                    strncpy(fields, argv[i+1], 1023);
                    i += 2;
                } else if (!strcmp(argv[i],"left")) {
                    left_join = 1;
                    i++;
                } else {
                    break;
                }
            }
            /* Grow joins array and populate the new slot */
            int idx = out->njoins;
            JoinSpec *tmp = realloc(out->joins, (size_t)(idx + 1) * sizeof(JoinSpec));
            if (!tmp) { snprintf(out->err,sizeof out->err,"NQL: join alloc failed"); return -1; }
            out->joins = tmp;
            JoinSpec *j = &out->joins[idx];
            memset(j, 0, sizeof(*j));
            strncpy(j->object, object, 255);
            strncpy(j->local_field, local, 255);
            strncpy(j->remote_field, remote, 255);
            if (as_name[0]) strncpy(j->as_name, as_name, 255);
            else            strncpy(j->as_name, object, 255);
            j->type = left_join ? JOIN_LEFT : JOIN_INNER;
            /* Parse fields CSV into proj_fields[] */
            if (fields[0]) {
                const char *fp = fields;
                while (*fp && j->proj_count < MAX_FIELDS) {
                    while (*fp == ' ' || *fp == '\t') fp++;
                    if (!*fp) break;
                    const char *fstart = fp;
                    while (*fp && *fp != ',') fp++;
                    int flen = (int)(fp - fstart);
                    if (flen > 255) flen = 255;
                    memcpy(j->proj_fields[j->proj_count], fstart, (size_t)flen);
                    j->proj_fields[j->proj_count][flen] = '\0';
                    j->proj_count++;
                    if (*fp == ',') fp++;
                }
            }
            out->njoins++;
        }
        else if (out->mode == NQL_AGGREGATE && out->naggs == 0 && argv[i][0] != '-') {
            int n = parse_nql_aggs(argv[i], &out->aggs);
            if (n < 0) { snprintf(out->err,sizeof out->err,"invalid agg spec '%s'",argv[i]); return -1; }
            out->naggs = n; i++;
        }
        else { snprintf(out->err,sizeof out->err,"NQL: unknown flag '%s'",argv[i]); return -1; }
    }
    return 0;
}

void nql_free_command(NqlCommand *cmd) {
    free_criteria_tree(cmd->filter);
    free_criteria_tree(cmd->having);
    free(cmd->aggs);
    free_joins(cmd->joins, cmd->njoins);
    memset(cmd, 0, sizeof *cmd);
}
