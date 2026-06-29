#pragma once
#include <stddef.h>
#include "types.h"   /* CriteriaNode, NqlAggSpec */

/* ── Token types ────────────────────────────────────────────────── */
typedef enum {
    TOK_IDENT,      /* field names and keywords (and/or/in/not/between…) */
    TOK_STRING,     /* 'value' — content stored without surrounding quotes */
    TOK_NUMBER,     /* 123, -45, 3.14                                      */
    TOK_BOOL,       /* true / false                                        */
    TOK_SYM_EQ,    /* =                                                   */
    TOK_SYM_NEQ,   /* !=                                                  */
    TOK_SYM_GT,    /* >                                                   */
    TOK_SYM_LT,    /* <                                                   */
    TOK_SYM_GTE,   /* >=                                                  */
    TOK_SYM_LTE,   /* <=                                                  */
    TOK_LPAREN,    /* (  */
    TOK_RPAREN,    /* )  */
    TOK_LBRACKET,  /* [  */
    TOK_RBRACKET,  /* ]  */
    TOK_COMMA,     /* ,  */
    TOK_EOF,
    TOK_ERR,
} NqlTokenType;

typedef struct {
    NqlTokenType type;
    char         text[1024];
} NqlToken;

/* ── Lexer state ─────────────────────────────────────────────────── */
typedef struct {
    const char *src;
    int         pos;
    NqlToken    cur;
    int         ready;   /* 1 = cur is valid (not yet consumed) */
    char        err[256];
} NqlLexer;

/* ── Parsed command ──────────────────────────────────────────────── */
typedef enum { NQL_FIND, NQL_COUNT, NQL_AGGREGATE } NqlMode;

typedef struct {
    NqlMode       mode;
    char          dir[256];
    char          obj[256];
    char          auth[512];    /* value of --auth flag; empty = no token */
    CriteriaNode *filter;       /* owned — free with free_criteria_tree() */
    /* find / count flags */
    int           offset;
    int           limit;
    char          fields[1024]; /* comma-separated projection */
    char          format[32];
    char          order_by[256];
    char          order_dir[8]; /* "asc" or "desc"; default "asc" */
    char          cursor[4096]; /* raw JSON cursor object; empty = no cursor */
    int           explain;      /* 1 = emit plan only, no execution */
    /* aggregate-only */
    NqlAggSpec   *aggs;         /* heap-allocated; free() directly */
    int           naggs;
    char          group_by[1024];
    CriteriaNode *having;       /* owned — free with free_criteria_tree() */
    /* error */
    char          err[256];
} NqlCommand;

/* ── Public API ──────────────────────────────────────────────────── */

/* Parse a filter expression string into a CriteriaNode tree.
   Returns NULL on error and writes into err_out (may be NULL).
   On success the caller owns the tree; free with free_criteria_tree(). */
CriteriaNode *nql_parse_filter(const char *src, char *err_out, size_t err_sz);

/* Parse a full NQL command line — "find dir obj [filter] [--flags…]".
   Returns 0 on success, -1 on error (message in out->err). */
int nql_parse_command(const char *src, NqlCommand *out);

/* Free the two CriteriaNode trees and the aggs array owned by cmd.
   Does not free cmd itself. */
void nql_free_command(NqlCommand *cmd);
