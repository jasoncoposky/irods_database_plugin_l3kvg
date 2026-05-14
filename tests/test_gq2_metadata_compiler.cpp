#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/private/genquery2_ast_types.hpp"
#include <iostream>

using namespace irods::catalog;
namespace gq = irods::experimental::genquery2;

class MetadataCompilerTest : public ::testing::Test {
protected:
    void SetUp() override {
        Config cfg;
        cfg.db_path = "test_metadata_compiler.l3kvg";
        cfg.node_id = 1;
        system("rm -rf test_metadata_compiler.l3kvg");
        ASSERT_TRUE(catalog.init(cfg).ok());

        // Setup some test data
        data_object obj;
        obj.id = 1001;
        obj.coll_id = 500;
        obj.name = "test_file.txt";
        data_id_t out_id;
        catalog.register_data_object(obj, out_id);

        catalog.add_avu_metadata("DataObject", "1001", {"Project", "L3KVG", ""});

        // Ensure all async operations are completed
        catalog.get_engine()->flush();
    }

    CatalogFacade catalog;
};

TEST_F(MetadataCompilerTest, GroupedMetadataQuery) {
    gq::select ast;
    ast.projections.push_back(gq::column{"DATA_NAME"});
    
    // WHERE META_DATA_ATTR_NAME = 'Project' AND META_DATA_ATTR_VALUE = 'L3KVG'
    ast.conditions.push_back(gq::condition{gq::column{"META_DATA_ATTR_NAME"}, gq::condition_equal{"Project"}});
    ast.conditions.push_back(gq::condition{gq::column{"META_DATA_ATTR_VALUE"}, gq::condition_equal{"L3KVG"}});

    ResultSet results;
    auto ret = catalog.execute_query(ast, results);
    ASSERT_TRUE(ret.ok()) << ret.code() << ": " << ret.result();
    
    // Should return "test_file.txt"
    ASSERT_EQ(results.row_count(), 1);
    EXPECT_EQ(results.get_field(0, 0), "test_file.txt");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
