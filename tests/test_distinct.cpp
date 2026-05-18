#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"
#include <set>

using namespace irods::catalog;

TEST(DistinctTest, DeduplicateResults) {
    Config cfg;
    cfg.db_path = "distinct.l3kvg";
    cfg.node_id = 1;
    system("rm -rf distinct.l3kvg"); // Clean start
    
    CatalogFacade catalog;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Setup Data
    // Register Collection first
    collection coll; coll.id = 10; coll.name = "/tempZone/home/alice/c10";
    coll_id_t c_out;
    catalog.register_collection(coll, c_out);

    // Multiple objects in the same collection (ID 10)
    for (int i = 1; i <= 5; ++i) {
        data_object obj;
        obj.id = 700 + i;
        obj.name = "file" + std::to_string(i) + ".txt";
        obj.coll_id = 10;
        data_id_t out;
        catalog.register_data_object(obj, out);
    }

    catalog.get_engine()->get_store()->wait_all_shards();

    namespace gq = irods::experimental::genquery2;

    // 2. Query: SELECT DISTINCT COLL_ID
    {
        gq::select ast;
        ast.distinct = true;
        ast.projections.push_back(gq::column{"COLL_ID"});

        ResultSet results;
        ASSERT_TRUE(catalog.execute_query(ast, results).ok());
        
        // Expected exactly 1 row (Collection ID 10)
        ASSERT_EQ(results.row_count(), 1);
        EXPECT_EQ(results.get_field(0, 0), "10");
    }

    // 3. Query: SELECT COLL_ID (Non-Distinct)
    {
        gq::select ast;
        ast.distinct = false;
        ast.projections.push_back(gq::column{"COLL_ID"});

        ResultSet results;
        ASSERT_TRUE(catalog.execute_query(ast, results).ok());
        
        // Expected 5 rows (all objects)
        ASSERT_EQ(results.row_count(), 5);
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
