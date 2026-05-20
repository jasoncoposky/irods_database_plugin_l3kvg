#include <gtest/gtest.h>
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"

using namespace irods::catalog;

TEST(GroupByTest, CountPerCollection) {
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"COLL_ID"});
    
    gq::function count_func;
    count_func.name = "COUNT";
    count_func.arguments.push_back(gq::column{"DATA_ID"});
    ast.projections.push_back(count_func);

    compiler::Gq2ToL3kvgCompiler compiler;
    std::string cypher = compiler.compile(ast);

    std::cerr << "COMPILED CYPHER: " << cypher << std::endl;
    EXPECT_TRUE(cypher.find("\"agg\":") != std::string::npos || cypher.find("COUNT") != std::string::npos);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
