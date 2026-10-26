#include <gtest/gtest.h>
#include "irods/rodsGenQuery.h"
#include "irods/rodsKeyWdDef.h"
#include "irods/rcMisc.h"
#include "irods/genquery1_ast_adapter.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/genquery2_ast_types.hpp"
#include <cstring>

using namespace irods::catalog;

TEST(Gq1BridgeTest, ParentOfConditionParsesWithoutCrashing) {
    genQueryInp_t inp{};
    memset(&inp, 0, sizeof(genQueryInp_t));

    // Simulate simple condition
    inp.sqlCondInp.len = 1;
    inp.sqlCondInp.inx = (int*)malloc(sizeof(int));
    inp.sqlCondInp.inx[0] = COL_COLL_NAME;
    inp.sqlCondInp.value = (char**)malloc(sizeof(char*));
    inp.sqlCondInp.value[0] = strdup("parent_of '/tempZone/home/rods/new_coll'");

    inp.selectInp.len = 1;
    inp.selectInp.inx = (int*)malloc(sizeof(int));
    inp.selectInp.inx[0] = COL_COLL_NAME;
    inp.selectInp.value = (int*)malloc(sizeof(int));
    inp.selectInp.value[0] = 1;

    auto ast = irods::experimental::genquery2::genquery1_to_genquery2_ast(inp);

    EXPECT_EQ(ast.conditions.size(), 1u);

    free(inp.sqlCondInp.inx);
    free(inp.sqlCondInp.value[0]);
    free(inp.sqlCondInp.value);
    free(inp.selectInp.inx);
    free(inp.selectInp.value);
}

TEST(Gq1BridgeTest, ResultSetBindBufferZeroCopyAccess) {
    lite3cpp::Buffer raw_buf(256);
    raw_buf.init_array();
    size_t r0 = raw_buf.arr_append_obj(0);
    size_t p0 = raw_buf.set_arr(r0, "proj");
    raw_buf.arr_append_str(p0, "val1");
    raw_buf.arr_append_str(p0, "100");
    size_t r1 = raw_buf.arr_append_obj(0);
    size_t p1 = raw_buf.set_arr(r1, "proj");
    raw_buf.arr_append_str(p1, "val2");
    raw_buf.arr_append_str(p1, "200");

    ResultSet results;
    results.bind_buffer(std::move(raw_buf));

    ASSERT_EQ(results.row_count(), 2u);
    EXPECT_EQ(results.get_field(0, 0), "val1");
    EXPECT_EQ(results.get_field(0, 1), "100");
    EXPECT_EQ(results.get_field(1, 0), "val2");
    EXPECT_EQ(results.get_field(1, 1), "200");
}

TEST(Gq1BridgeTest, RowsToGenQueryOutFromLite3Buffer) {
    lite3cpp::Buffer raw_buf(256);
    raw_buf.init_array();
    size_t r0 = raw_buf.arr_append_arr(0);
    raw_buf.arr_append_str(r0, "/tempZone/home/rods");
    size_t r1 = raw_buf.arr_append_arr(0);
    raw_buf.arr_append_str(r1, "/tempZone/home/public");

    const int col_ids[] = {COL_COLL_NAME};
    genQueryOut_t* out = nullptr;
    ASSERT_EQ(irods::experimental::genquery2::rows_buffer_to_genquery_out(raw_buf, std::span<const int>{col_ids}, out), 0);
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(out->rowCnt, 2);
    EXPECT_EQ(out->attriCnt, 1);
    EXPECT_STREQ(out->sqlResult[0].value, "/tempZone/home/rods");
    EXPECT_STREQ(out->sqlResult[0].value + out->sqlResult[0].len, "/tempZone/home/public");

    freeGenQueryOut(&out);
}

TEST(Gq1BridgeTest, ZeroHopDataObjectColumnMapping) {
    namespace gq2 = irods::experimental::genquery2;
    gq2::select sel;
    sel.from_entity = "DataObject";
    sel.projections.push_back(gq2::column{"DATA_NAME"});
    sel.projections.push_back(gq2::column{"COLL_NAME"});
    sel.projections.push_back(gq2::column{"DATA_COLL_ID"});
    sel.conditions.push_back(gq2::condition{gq2::column{"COLL_NAME"}, gq2::condition_equal{"/tempZone/home/rods"}});
    sel.conditions.push_back(gq2::condition{gq2::column{"COLL_ID"}, gq2::condition_equal{"1001"}});

    irods::catalog::compiler::Gq2ToL3kvgCompiler compiler;
    lite3cpp::Buffer buf = compiler.compile(sel);

    EXPECT_EQ(buf.get_str(0, "root_alias"), "DataObject");

    for (const auto& t : compiler.get_target_types()) {
        EXPECT_NE(t, "Collection");
    }

    size_t projs = buf.get_arr(0, "projections");
    size_t p0 = buf.arr_get_obj(projs, 0);
    EXPECT_EQ(buf.get_str(p0, "alias"), "DataObject");
    EXPECT_EQ(buf.get_str(p0, "property"), "n");

    size_t p1 = buf.arr_get_obj(projs, 1);
    EXPECT_EQ(buf.get_str(p1, "alias"), "DataObject");
    EXPECT_EQ(buf.get_str(p1, "property"), "pn");

    size_t p2 = buf.arr_get_obj(projs, 2);
    EXPECT_EQ(buf.get_str(p2, "alias"), "DataObject");
    EXPECT_EQ(buf.get_str(p2, "property"), "cid");

    size_t filters = buf.get_arr(0, "filters");
    size_t f0 = buf.arr_get_obj(filters, 0);
    EXPECT_EQ(buf.get_str(f0, "alias"), "DataObject");
    EXPECT_EQ(buf.get_str(f0, "key"), "pn");
    EXPECT_EQ(buf.get_str(f0, "value"), "/tempZone/home/rods");

    size_t f1 = buf.arr_get_obj(filters, 1);
    EXPECT_EQ(buf.get_str(f1, "alias"), "DataObject");
    EXPECT_EQ(buf.get_str(f1, "key"), "cid");
    EXPECT_EQ(buf.get_str(f1, "value"), "1001");
}

TEST(Gq1BridgeTest, SynthesizeGq2AstEqualityIntegration) {
    namespace gq2 = irods::experimental::genquery2;

    genQueryInp_t inp{};
    memset(&inp, 0, sizeof(genQueryInp_t));

    inp.selectInp.len = 2;
    inp.selectInp.inx = (int*)malloc(2 * sizeof(int));
    inp.selectInp.inx[0] = COL_DATA_NAME;
    inp.selectInp.inx[1] = COL_DATA_SIZE;
    inp.selectInp.value = (int*)malloc(2 * sizeof(int));
    inp.selectInp.value[0] = 1;
    inp.selectInp.value[1] = 1;

    inp.sqlCondInp.len = 3;
    inp.sqlCondInp.inx = (int*)malloc(3 * sizeof(int));
    inp.sqlCondInp.inx[0] = COL_COLL_NAME;
    inp.sqlCondInp.inx[1] = COL_DATA_NAME;
    inp.sqlCondInp.inx[2] = COL_DATA_SIZE;

    inp.sqlCondInp.value = (char**)malloc(3 * sizeof(char*));
    inp.sqlCondInp.value[0] = strdup("= '/tempZone/home/rods'");
    inp.sqlCondInp.value[1] = strdup("= 'my_file.txt'");
    inp.sqlCondInp.value[2] = strdup("= '1024'");

    auto ast = gq2::genquery1_to_genquery2_ast(inp);

    ASSERT_EQ(ast.conditions.size(), 3u);

    const auto* cond0 = std::get_if<gq2::condition>(&ast.conditions[0]);
    ASSERT_NE(cond0, nullptr);
    const auto* col0 = std::get_if<gq2::column>(&cond0->lhs);
    ASSERT_NE(col0, nullptr);
    EXPECT_EQ(col0->name, "COLL_NAME");
    const auto* eq0 = std::get_if<gq2::condition_equal>(&cond0->expression);
    ASSERT_NE(eq0, nullptr);
    EXPECT_EQ(eq0->string_literal, "/tempZone/home/rods");

    const auto* and1 = std::get_if<gq2::logical_and>(&ast.conditions[1]);
    ASSERT_NE(and1, nullptr);
    ASSERT_EQ(and1->condition.size(), 1u);
    const auto* cond1 = std::get_if<gq2::condition>(&and1->condition[0]);
    ASSERT_NE(cond1, nullptr);
    const auto* col1 = std::get_if<gq2::column>(&cond1->lhs);
    ASSERT_NE(col1, nullptr);
    EXPECT_EQ(col1->name, "DATA_NAME");
    const auto* eq1 = std::get_if<gq2::condition_equal>(&cond1->expression);
    ASSERT_NE(eq1, nullptr);
    EXPECT_EQ(eq1->string_literal, "my_file.txt");

    const auto* and2 = std::get_if<gq2::logical_and>(&ast.conditions[2]);
    ASSERT_NE(and2, nullptr);
    ASSERT_EQ(and2->condition.size(), 1u);
    const auto* cond2 = std::get_if<gq2::condition>(&and2->condition[0]);
    ASSERT_NE(cond2, nullptr);
    const auto* col2 = std::get_if<gq2::column>(&cond2->lhs);
    ASSERT_NE(col2, nullptr);
    EXPECT_EQ(col2->name, "DATA_SIZE");
    const auto* eq2 = std::get_if<gq2::condition_equal>(&cond2->expression);
    ASSERT_NE(eq2, nullptr);
    EXPECT_EQ(eq2->string_literal, "1024");

    free(inp.selectInp.inx);
    free(inp.selectInp.value);
    free(inp.sqlCondInp.inx);
    free(inp.sqlCondInp.value[0]);
    free(inp.sqlCondInp.value[1]);
    free(inp.sqlCondInp.value[2]);
    free(inp.sqlCondInp.value);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
