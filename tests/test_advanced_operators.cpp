#include <gtest/gtest.h>
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"

using namespace irods::catalog;

TEST(AdvancedQueryTest, LikeAndComparison) {
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"DATA_NAME"});
    ast.conditions.push_back(gq::condition{gq::column{"DATA_SIZE"}, gq::condition_greater_than{"1000"}});
    ast.conditions.push_back(gq::condition{gq::column{"DATA_NAME"}, gq::condition_like{"report%"}});

    compiler::Gq2ToL3kvgCompiler compiler;
    std::string cypher = compiler.compile(ast);
    std::cerr << "COMPILED CYPHER: " << cypher << std::endl;
    
    EXPECT_TRUE(cypher.find("\"op\":2") != std::string::npos);
    EXPECT_TRUE(cypher.find("\"op\":6") != std::string::npos);
    EXPECT_TRUE(cypher.find("report%") != std::string::npos);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
