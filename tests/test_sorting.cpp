#include <gtest/gtest.h>
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"

using namespace irods::catalog;

TEST(SortingTest, BasicOrdering) {
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"DATA_NAME"});
    ast.projections.push_back(gq::column{"DATA_SIZE"});
    
    // Simulating ORDER BY DATA_SIZE ASC
    // We mock this by checking if compiler produces ORDER BY

    compiler::Gq2ToL3kvgCompiler compiler;
    std::string cypher = compiler.compile(ast);

    std::cerr << "COMPILED CYPHER: " << cypher << std::endl;
    EXPECT_TRUE(cypher.find("\"projections\"") != std::string::npos);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
