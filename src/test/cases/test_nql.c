#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "nql.h"
#include "types.h"
#include <string.h>
#include <stdlib.h>

static int test_nql_simple_filter(void) {
    char err[256];
    CriteriaNode *tree = nql_parse_filter("age eq 42", err, sizeof(err));
    ASSERT_TRUE(tree != NULL, "nql_parse_filter returns non-NULL");
    if (!tree) return t_ctx->failed > 0 ? 1 : 0;
    ASSERT_EQ_INT((int)tree->kind, (int)CNODE_LEAF, "kind == CNODE_LEAF");
    ASSERT_EQ_STR(tree->leaf.field, "age", "field == age");
    ASSERT_EQ_INT((int)tree->leaf.op, (int)OP_EQUAL, "op == OP_EQUAL");
    free_criteria_tree(tree);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_find_command(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    /* Format: find <dir> <obj> [filter] [--flags...] */
    int r = nql_parse_command("find default users 'name eq Alice'", &cmd);
    ASSERT_TRUE(r == 0, "nql_parse_command succeeds");
    ASSERT_EQ_INT((int)cmd.mode, (int)NQL_FIND, "mode == NQL_FIND");
    ASSERT_EQ_STR(cmd.dir, "default", "dir == default");
    ASSERT_EQ_STR(cmd.obj, "users", "obj == users");
    ASSERT_TRUE(cmd.filter != NULL, "filter != NULL");
    if (cmd.filter) {
        ASSERT_EQ_INT((int)cmd.filter->leaf.op, (int)OP_EQUAL, "filter op == eq");
    }
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_count_command(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("count default orders 'status eq active'", &cmd);
    ASSERT_TRUE(r == 0, "nql_parse_command succeeds");
    ASSERT_EQ_INT((int)cmd.mode, (int)NQL_COUNT, "mode == NQL_COUNT");
    ASSERT_EQ_STR(cmd.dir, "default", "dir == default");
    ASSERT_EQ_STR(cmd.obj, "orders", "obj == orders");
    ASSERT_TRUE(cmd.filter != NULL, "filter != NULL");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_aggregate_command(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    /* Format: aggregate <dir> <obj> [filter] <aggs> --group-by <group> */
    int r = nql_parse_command("aggregate default sales sum(amount) --group-by region", &cmd);
    ASSERT_TRUE(r == 0, "nql_parse_command aggregate succeeds");
    ASSERT_EQ_INT((int)cmd.mode, (int)NQL_AGGREGATE, "mode == NQL_AGGREGATE");
    ASSERT_EQ_STR(cmd.dir, "default", "dir == default");
    ASSERT_EQ_STR(cmd.obj, "sales", "obj == sales");
    ASSERT_EQ_INT(cmd.naggs, 1, "naggs == 1");
    if (cmd.naggs > 0) {
        ASSERT_EQ_STR(cmd.aggs[0].fn, "sum", "agg fn == sum");
        ASSERT_EQ_STR(cmd.aggs[0].field, "amount", "agg field == amount");
    }
    ASSERT_EQ_STR(cmd.group_by, "region", "group_by == region");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_and_or(void) {
    char err[256];

    /* AND: age > 18 and status = active → CNODE_AND with two LEAF children */
    CriteriaNode *and_tree = nql_parse_filter("age > 18 and status = active", err, sizeof(err));
    ASSERT_TRUE(and_tree != NULL, "AND filter parses");
    if (and_tree) {
        ASSERT_EQ_INT((int)and_tree->kind, (int)CNODE_AND, "AND kind == CNODE_AND");
        ASSERT_EQ_INT(and_tree->n_children, 2, "AND has 2 children");
        if (and_tree->n_children == 2) {
            ASSERT_EQ_INT((int)and_tree->children[0]->kind, (int)CNODE_LEAF, "left is LEAF");
            ASSERT_EQ_STR(and_tree->children[0]->leaf.field, "age", "left field == age");
            ASSERT_EQ_INT((int)and_tree->children[0]->leaf.op, (int)OP_GREATER, "left op == gt");
            ASSERT_EQ_INT((int)and_tree->children[1]->kind, (int)CNODE_LEAF, "right is LEAF");
            ASSERT_EQ_STR(and_tree->children[1]->leaf.field, "status", "right field == status");
            ASSERT_EQ_INT((int)and_tree->children[1]->leaf.op, (int)OP_EQUAL, "right op == eq");
        }
        free_criteria_tree(and_tree);
    }

    /* OR: score < 5 or score > 95 → CNODE_OR with two LEAF children */
    CriteriaNode *or_tree = nql_parse_filter("score < 5 or score > 95", err, sizeof(err));
    ASSERT_TRUE(or_tree != NULL, "OR filter parses");
    if (or_tree) {
        ASSERT_EQ_INT((int)or_tree->kind, (int)CNODE_OR, "OR kind == CNODE_OR");
        ASSERT_EQ_INT(or_tree->n_children, 2, "OR has 2 children");
        free_criteria_tree(or_tree);
    }

    /* AND binds tighter: a = 1 or b = 2 and c = 3 → OR(LEAF, AND(LEAF,LEAF)) */
    CriteriaNode *prec = nql_parse_filter("a = 1 or b = 2 and c = 3", err, sizeof(err));
    ASSERT_TRUE(prec != NULL, "precedence filter parses");
    if (prec) {
        ASSERT_EQ_INT((int)prec->kind, (int)CNODE_OR, "top node is OR");
        if (prec->n_children == 2)
            ASSERT_EQ_INT((int)prec->children[1]->kind, (int)CNODE_AND, "right child is AND");
        free_criteria_tree(prec);
    }

    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_between(void) {
    char err[256];
    CriteriaNode *tree = nql_parse_filter("price between 10 and 100", err, sizeof(err));
    ASSERT_TRUE(tree != NULL, "BETWEEN filter parses");
    if (tree) {
        ASSERT_EQ_INT((int)tree->kind, (int)CNODE_LEAF, "kind == CNODE_LEAF");
        ASSERT_EQ_STR(tree->leaf.field, "price", "field == price");
        ASSERT_EQ_INT((int)tree->leaf.op, (int)OP_BETWEEN, "op == OP_BETWEEN");
        ASSERT_EQ_STR(tree->leaf.value,  "10",  "value == 10");
        ASSERT_EQ_STR(tree->leaf.value2, "100", "value2 == 100");
        free_criteria_tree(tree);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_in(void) {
    char err[256];

    /* IN with parentheses */
    CriteriaNode *in_tree = nql_parse_filter("status in (active,pending)", err, sizeof(err));
    ASSERT_TRUE(in_tree != NULL, "IN filter parses");
    if (in_tree) {
        ASSERT_EQ_INT((int)in_tree->leaf.op, (int)OP_IN, "op == OP_IN");
        ASSERT_EQ_INT(in_tree->leaf.in_count, 2, "in_count == 2");
        if (in_tree->leaf.in_count >= 2) {
            ASSERT_EQ_STR(in_tree->leaf.in_values[0], "active",  "in_values[0] == active");
            ASSERT_EQ_STR(in_tree->leaf.in_values[1], "pending", "in_values[1] == pending");
        }
        free_criteria_tree(in_tree);
    }

    /* NOT_IN */
    CriteriaNode *nin_tree = nql_parse_filter("role not in [admin,root]", err, sizeof(err));
    ASSERT_TRUE(nin_tree != NULL, "NOT_IN filter parses");
    if (nin_tree) {
        ASSERT_EQ_INT((int)nin_tree->leaf.op, (int)OP_NOT_IN, "op == OP_NOT_IN");
        ASSERT_EQ_INT(nin_tree->leaf.in_count, 2, "in_count == 2");
        free_criteria_tree(nin_tree);
    }

    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_parse_errors(void) {
    char err[256];

    /* Missing value after operator */
    err[0] = '\0';
    CriteriaNode *t1 = nql_parse_filter("age >", err, sizeof(err));
    ASSERT_TRUE(t1 == NULL, "missing value → NULL");
    ASSERT_TRUE(err[0] != '\0', "missing value → error message set");

    /* Missing operator */
    err[0] = '\0';
    CriteriaNode *t2 = nql_parse_filter("age", err, sizeof(err));
    ASSERT_TRUE(t2 == NULL, "missing op → NULL");

    /* Empty string → NULL, no error (valid: no filter) */
    err[0] = '\0';
    CriteriaNode *t3 = nql_parse_filter("", err, sizeof(err));
    ASSERT_TRUE(t3 == NULL, "empty string → NULL");

    /* Too few args to nql_parse_command */
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default", &cmd);
    ASSERT_TRUE(r < 0, "too-few-args → error");
    ASSERT_TRUE(cmd.err[0] != '\0', "too-few-args → err message set");

    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_flags(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command(
        "find tenant items 'qty > 0' --order-by price:desc --limit 20 --offset 5 --format csv",
        &cmd);
    ASSERT_TRUE(r == 0, "flags command parses");
    ASSERT_EQ_INT(cmd.limit,  20,  "limit == 20");
    ASSERT_EQ_INT(cmd.offset,  5,  "offset == 5");
    ASSERT_EQ_STR(cmd.order_by,  "price", "order_by == price");
    ASSERT_EQ_STR(cmd.order_dir, "desc",  "order_dir == desc");
    ASSERT_EQ_STR(cmd.format,    "csv",   "format == csv");
    ASSERT_TRUE(cmd.filter != NULL, "filter != NULL");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_filter_embedded_quote(void) {
    char err[256];
    CriteriaNode *tree = nql_parse_filter("name eq 'O''Brien'", err, sizeof(err));
    ASSERT_TRUE(tree != NULL, "nql_parse_filter parses a doubled-quote literal");
    if (!tree) return t_ctx->failed > 0 ? 1 : 0;
    ASSERT_EQ_STR(tree->leaf.value, "O'Brien", "value decodes the doubled quote to a literal '");
    free_criteria_tree(tree);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_filter_quoted_space_value(void) {
    char err[256];
    CriteriaNode *tree = nql_parse_filter("name eq 'Alice Smith'", err, sizeof(err));
    ASSERT_TRUE(tree != NULL, "nql_parse_filter parses a space-containing literal");
    if (!tree) return t_ctx->failed > 0 ? 1 : 0;
    ASSERT_EQ_STR(tree->leaf.value, "Alice Smith", "value is unchanged (no doubling in this input)");
    free_criteria_tree(tree);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_command_nested_quote_escape(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default users 'name eq ''O''''Brien'''", &cmd);
    ASSERT_EQ_INT(r, 0, "nql_parse_command succeeds on nested-doubled literal");
    ASSERT_TRUE(cmd.filter != NULL, "filter parsed");
    if (cmd.filter) {
        ASSERT_EQ_STR(cmd.filter->leaf.field, "name", "field == name");
        ASSERT_EQ_STR(cmd.filter->leaf.value, "O'Brien", "value decodes through both layers to O'Brien");
    }
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_command_quoted_space_value(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default users 'name eq ''Alice Smith'''", &cmd);
    ASSERT_EQ_INT(r, 0, "nql_parse_command succeeds");
    ASSERT_TRUE(cmd.filter != NULL, "filter parsed");
    if (cmd.filter) ASSERT_EQ_STR(cmd.filter->leaf.value, "Alice Smith", "value == Alice Smith");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_command_double_quote_wrapper(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default users \"name eq 'O''Brien'\"", &cmd);
    ASSERT_EQ_INT(r, 0, "nql_parse_command succeeds with a double-quoted top-level wrapper");
    ASSERT_TRUE(cmd.filter != NULL, "filter parsed");
    if (cmd.filter) {
        ASSERT_EQ_STR(cmd.filter->leaf.field, "name", "field == name");
        ASSERT_EQ_STR(cmd.filter->leaf.value, "O'Brien", "value decodes to O'Brien with no nested doubling needed");
    }
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_command_double_quote_embedded_quote(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default users \"name eq 'Say \"\"hi\"\"'\"", &cmd);
    ASSERT_EQ_INT(r, 0, "nql_parse_command succeeds with an embedded doubled double-quote");
    ASSERT_TRUE(cmd.filter != NULL, "filter parsed");
    if (cmd.filter) ASSERT_EQ_STR(cmd.filter->leaf.value, "Say \"hi\"", "embedded \"\" decodes to a literal \"");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_filter_unterminated_string_is_error(void) {
    char err[256] = {0};
    CriteriaNode *tree = nql_parse_filter("name eq 'Alice", err, sizeof(err));
    ASSERT_TRUE(tree == NULL, "unterminated string literal is now rejected, not silently truncated");
    ASSERT_TRUE(err[0] != '\0', "a parse error message is produced");
    if (tree) free_criteria_tree(tree);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_command_unterminated_quote_tolerant(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command("find default users 'name eq Alice", &cmd);
    /* cmd_split has no error channel; an unterminated top-level wrapper
       degrades to "everything decoded to end of input" -- pin that the
       captured filter argument is exactly the tail after the opening quote. */
    ASSERT_EQ_INT(r, 0, "unterminated top-level wrapper remains a tolerant success");
    ASSERT_TRUE(cmd.filter != NULL, "captured filter tail is parsed");
    if (cmd.filter) {
        ASSERT_EQ_STR(cmd.filter->leaf.field, "name", "captured field == name");
        ASSERT_EQ_STR(cmd.filter->leaf.value, "Alice", "captured value == Alice");
    }
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_filter_empty_string_literal(void) {
    char err[256];
    CriteriaNode *tree = nql_parse_filter("name eq ''", err, sizeof(err));
    ASSERT_TRUE(tree != NULL, "empty '' is a valid, immediately-closed literal, not unterminated");
    if (!tree) return t_ctx->failed > 0 ? 1 : 0;
    ASSERT_EQ_STR(tree->leaf.value, "", "value decodes to the empty string");
    free_criteria_tree(tree);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_append_arg_contract(void) {
    char quoted[128] = "find";
    ASSERT_EQ_INT(nql_append_arg(quoted, sizeof quoted,
                                 "name eq 'Say \"hi\"'"),
                  0, "encoder accepts a quoted argument");
    ASSERT_EQ_STR(quoted, "find \"name eq 'Say \"\"hi\"\"'\"",
                  "encoder doubles the top-level wrapper delimiter");

    char empty[32] = "find";
    ASSERT_EQ_INT(nql_append_arg(empty, sizeof empty, ""), 0,
                  "encoder preserves an empty argv element");
    ASSERT_EQ_STR(empty, "find \"\"", "empty argv element is encoded as double quotes");

    char small[12] = "find";
    char before[sizeof small];
    memcpy(before, small, sizeof small);
    ASSERT_EQ_INT(nql_append_arg(small, sizeof small, "too long here"), -1,
                  "encoder rejects an argument that cannot fit completely");
    ASSERT_EQ_STR(small, before, "capacity failure leaves dst unchanged");

    char newline[32] = "find";
    ASSERT_EQ_INT(nql_append_arg(newline, sizeof newline, "x\ny"), -1,
                  "encoder rejects an embedded wire line break");
    ASSERT_EQ_STR(newline, "find", "line-break rejection leaves dst unchanged");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("nql-simple-filter",    test_nql_simple_filter);
TEST_REGISTER("nql-find-command",     test_nql_find_command);
TEST_REGISTER("nql-count-command",    test_nql_count_command);
TEST_REGISTER("nql-aggregate-command",test_nql_aggregate_command);
TEST_REGISTER("nql-and-or",           test_nql_and_or);
TEST_REGISTER("nql-between",          test_nql_between);
TEST_REGISTER("nql-in",               test_nql_in);
TEST_REGISTER("nql-parse-errors",     test_nql_parse_errors);
TEST_REGISTER("nql-flags",            test_nql_flags);
TEST_REGISTER("nql-filter-embedded-quote",          test_nql_filter_embedded_quote);
TEST_REGISTER("nql-filter-quoted-space-value",      test_nql_filter_quoted_space_value);
TEST_REGISTER("nql-command-nested-quote-escape",    test_nql_command_nested_quote_escape);
TEST_REGISTER("nql-command-quoted-space-value",     test_nql_command_quoted_space_value);
TEST_REGISTER("nql-command-double-quote-wrapper",   test_nql_command_double_quote_wrapper);
TEST_REGISTER("nql-command-double-quote-embedded",  test_nql_command_double_quote_embedded_quote);
TEST_REGISTER("nql-filter-unterminated-is-error",   test_nql_filter_unterminated_string_is_error);
TEST_REGISTER("nql-command-unterminated-tolerant",  test_nql_command_unterminated_quote_tolerant);
TEST_REGISTER("nql-filter-empty-string-literal",    test_nql_filter_empty_string_literal);
TEST_REGISTER("nql-append-arg-contract",            test_nql_append_arg_contract);
