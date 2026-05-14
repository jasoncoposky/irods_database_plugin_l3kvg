#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"

using namespace irods::catalog;

TEST(AdvancedQueryTest, LikeAndComparison) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "advanced.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Setup Data
    data_object obj1;
    obj1.id = 701;
    obj1.name = "report_v1.pdf";
    obj1.size = 5000;
    data_id_t out1;
    catalog.register_data_object(obj1, out1);

    data_object obj2;
    obj2.id = 702;
    obj2.name = "notes.txt";
    obj2.size = 500;
    data_id_t out2;
    catalog.register_data_object(obj2, out2);

    catalog.get_engine()->get_store()->wait_all_shards();

    // 2. Query: SELECT DATA_NAME WHERE DATA_SIZE > 1000 AND DATA_NAME LIKE 'report%'
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"DATA_NAME"});
    ast.conditions.push_back(gq::condition{gq::column{"DATA_SIZE"}, gq::condition_greater_than{"1000"}});
    ast.conditions.push_back(gq::condition{gq::column{"DATA_NAME"}, gq::condition_like{"report%"}});

    ResultSet results;
    ASSERT_TRUE(catalog.execute_query(ast, results).ok());
    
    // 3. Validate: Only obj1 should match
    ASSERT_EQ(results.row_count(), 1);
    EXPECT_EQ(results.get_field(0, "DataObject.name"), "report_v1.pdf");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
