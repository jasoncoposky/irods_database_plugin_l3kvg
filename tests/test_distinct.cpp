#include <gtest/gtest.h>
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"

using namespace irods::catalog;

TEST(DistinctTest, DeduplicateResults) {
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"COLL_ID"});
    ast.distinct = true;

    compiler::Gq2ToL3kvgCompiler compiler;
    std::string cypher = compiler.compile(ast);

    std::cerr << "COMPILED CYPHER: " << cypher << std::endl;
    EXPECT_TRUE(cypher.find("\"distinct\":true") != std::string::npos || cypher.find("DISTINCT") != std::string::npos);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
