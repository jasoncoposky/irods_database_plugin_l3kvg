#include <gtest/gtest.h>
#include "irods/rodsGenQuery.h"
#include "irods/rodsKeyWdDef.h"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/private/genquery2_ast_types.hpp"
#include <cstring>

namespace irods::catalog::bridge {
    irods::experimental::genquery2::select synthesize_gq2_ast(genQueryInp_t* _inp, CatalogFacade* _catalog, std::vector<uint64_t>& _starting_nodes);
    void pack_gq1_results(const ResultSet& _results, genQueryInp_t* _inp, genQueryOut_t* _out);
}

using namespace irods::catalog;
using namespace irods::catalog::bridge;

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

    std::vector<snowflake_id_t> starting_nodes;

    auto ast = synthesize_gq2_ast(&inp, nullptr, starting_nodes);

    EXPECT_EQ(ast.conditions.size(), 1);

    free(inp.sqlCondInp.inx);
    free(inp.sqlCondInp.value[0]);
    free(inp.sqlCondInp.value);
    free(inp.selectInp.inx);
    free(inp.selectInp.value);
}

TEST(Gq1BridgeTest, PaginationFieldsAreParsedCorrectly) {
    genQueryInp_t inp{};
    memset(&inp, 0, sizeof(genQueryInp_t));
    inp.maxRows = 500;
    inp.continueInx = 1000;

    // We must provide at least one select to set the root alias
    inp.selectInp.len = 1;
    inp.selectInp.inx = (int*)malloc(sizeof(int));
    inp.selectInp.inx[0] = COL_COLL_NAME;
    inp.selectInp.value = (int*)malloc(sizeof(int));
    inp.selectInp.value[0] = 1;

    std::vector<snowflake_id_t> starting_nodes;
    auto ast = synthesize_gq2_ast(&inp, nullptr, starting_nodes);

    EXPECT_EQ(ast.range.number_of_rows, "500");
    EXPECT_EQ(ast.range.offset, "1000");

    free(inp.selectInp.inx);
    free(inp.selectInp.value);
}

TEST(Gq1BridgeTest, PackGq1ResultsSetsContinueInx) {
    genQueryInp_t inp{};
    memset(&inp, 0, sizeof(genQueryInp_t));
    inp.maxRows = 2; // Request 2 rows
    inp.continueInx = 5; // Current offset

    inp.selectInp.len = 1;
    inp.selectInp.inx = (int*)malloc(sizeof(int));
    inp.selectInp.inx[0] = COL_COLL_NAME;
    inp.selectInp.value = (int*)malloc(sizeof(int));
    inp.selectInp.value[0] = 1;

    ResultSet results;
    // Simulate getting 2 rows (meaning there might be more)
    l3kvg::Query::ResultRow r1; r1.fields["idx_0"] = "val1";
    l3kvg::Query::ResultRow r2; r2.fields["idx_0"] = "val2";
    results.rows.push_back(r1);
    results.rows.push_back(r2);

    genQueryOut_t out{};
    memset(&out, 0, sizeof(genQueryOut_t));

    pack_gq1_results(results, &inp, &out);

    // Should be continueInx (5) + rows returned (2) = 7
    EXPECT_EQ(out.continueInx, 7);

    free(inp.selectInp.inx);
    free(inp.selectInp.value);
    if (out.sqlResult && out.sqlResult[0].value) free(out.sqlResult[0].value);
}

TEST(Gq1BridgeTest, PackGq1ResultsEndsPaginationWhenFewerRowsReturned) {
    genQueryInp_t inp{};
    memset(&inp, 0, sizeof(genQueryInp_t));
    inp.maxRows = 2; // Request 2 rows
    inp.continueInx = 5;

    inp.selectInp.len = 1;
    inp.selectInp.inx = (int*)malloc(sizeof(int));
    inp.selectInp.inx[0] = COL_COLL_NAME;
    inp.selectInp.value = (int*)malloc(sizeof(int));
    inp.selectInp.value[0] = 1;

    ResultSet results;
    // Simulate getting 1 row (less than maxRows, meaning end of results)
    l3kvg::Query::ResultRow r1; r1.fields["idx_0"] = "val1";
    results.rows.push_back(r1);

    genQueryOut_t out{};
    memset(&out, 0, sizeof(genQueryOut_t));

    pack_gq1_results(results, &inp, &out);

    // Should be 0 because we hit the end
    EXPECT_EQ(out.continueInx, 0);

    free(inp.selectInp.inx);
    free(inp.selectInp.value);
    if (out.sqlResult && out.sqlResult[0].value) free(out.sqlResult[0].value);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}