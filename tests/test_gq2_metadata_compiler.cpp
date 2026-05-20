#include <gtest/gtest.h>
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"

using namespace irods::catalog;

TEST(MetadataCompilerTest, GroupedMetadataQuery) {
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"DATA_NAME"});
    ast.conditions.push_back(gq::condition{gq::column{"META_DATA_ATTR_NAME"}, gq::condition_equal{"Project"}});
    ast.conditions.push_back(gq::condition{gq::column{"META_DATA_ATTR_VALUE"}, gq::condition_equal{"L3KVG"}});

    compiler::Gq2ToL3kvgCompiler compiler;
    std::string cypher = compiler.compile(ast);

    EXPECT_TRUE(cypher.find("Project") != std::string::npos);
    EXPECT_TRUE(cypher.find("L3KVG") != std::string::npos);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
