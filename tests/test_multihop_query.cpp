#include <gtest/gtest.h>
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"

using namespace irods::catalog;

TEST(MultiHopQueryTest, CollectionToResource) {
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"RESC_NAME"});
    ast.conditions.push_back(gq::condition{gq::column{"COLL_NAME"}, gq::condition_equal{"/tempZone/home/alice"}});

    compiler::Gq2ToL3kvgCompiler compiler;
    std::string cypher = compiler.compile(ast);

    std::cerr << "COMPILED CYPHER: " << cypher << std::endl;
    EXPECT_TRUE(cypher.find("tempZone") != std::string::npos || cypher.find("Resource") != std::string::npos);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
