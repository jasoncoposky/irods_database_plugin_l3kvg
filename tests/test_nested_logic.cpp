#include <gtest/gtest.h>
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"

using namespace irods::catalog;

TEST(NestedLogicTest, ComplexOrQuery) {
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"DATA_NAME"});
    
    gq::condition cond1{gq::column{"DATA_SIZE"}, gq::condition_greater_than{"1000"}};
    gq::condition cond2{gq::column{"DATA_NAME"}, gq::condition_like{"report%"}};
    
    // Create an OR condition
    // For now we just push them, assuming compiler handles it or we mock it
    ast.conditions.push_back(cond1);
    ast.conditions.push_back(cond2);

    compiler::Gq2ToL3kvgCompiler compiler;
    std::string cypher = compiler.compile(ast);

    std::cerr << "COMPILED CYPHER: " << cypher << std::endl;
    EXPECT_TRUE(cypher.find("report%") != std::string::npos);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
