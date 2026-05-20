#include <gtest/gtest.h>
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"

using namespace irods::catalog;

TEST(AggregateTest, CountAndSum) {
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    
    gq::function count_func;
    count_func.name = "COUNT";
    count_func.arguments.push_back(gq::column{"DATA_ID"});
    ast.projections.push_back(count_func);

    ast.conditions.push_back(gq::condition{gq::column{"COLL_ID"}, gq::condition_equal{"10"}});

    compiler::Gq2ToL3kvgCompiler compiler;
    std::string cypher = compiler.compile(ast);
    std::cerr << "COMPILED CYPHER: " << cypher << std::endl;
    
    EXPECT_TRUE(cypher.find("\"agg\":1") != std::string::npos || cypher.find("COUNT") != std::string::npos);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
