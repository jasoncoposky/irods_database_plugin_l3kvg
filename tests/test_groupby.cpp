#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"
#include <map>

using namespace irods::catalog;

TEST(GroupByTest, CountPerCollection) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "groupby.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Setup Data
    // Register Collections first
    collection c10; c10.id = 10; c10.name = "/tempZone/home/alice/c10";
    collection c20; c20.id = 20; c20.name = "/tempZone/home/alice/c20";
    coll_id_t c_out;
    catalog.register_collection(c10, c_out);
    catalog.register_collection(c20, c_out);

    // Collection 10 has 2 objects
    // Collection 20 has 1 object
    for (int i = 1; i <= 2; ++i) {
        data_object obj;
        obj.id = 400 + i;
        obj.name = "c10_file" + std::to_string(i) + ".txt";
        obj.coll_id = 10;
        data_id_t out;
        catalog.register_data_object(obj, out);
    }
    {
        data_object obj;
        obj.id = 403;
        obj.name = "c20_file1.txt";
        obj.coll_id = 20;
        data_id_t out;
        catalog.register_data_object(obj, out);
    }

    catalog.get_engine()->get_store()->wait_all_shards();

    namespace gq = irods::experimental::genquery2;

    // 2. Query: SELECT COLL_ID, COUNT(DATA_ID) GROUP BY COLL_ID
    {
        gq::select ast;
        ast.projections.push_back(gq::column{"COLL_ID"});
        
        gq::function fn;
        fn.name = "COUNT";
        fn.arguments.push_back(gq::column{"DATA_ID"});
        ast.projections.push_back(fn);

        ast.group_by.expressions.push_back(gq::column{"COLL_ID"});

        ResultSet results;
        ASSERT_TRUE(catalog.execute_query(ast, results).ok());
        
        // Expected 2 rows: {10, 2} and {20, 1}
        ASSERT_EQ(results.row_count(), 2);
        
        std::map<std::string, std::string> counts;
        counts[std::string(results.get_field(0, 0))] = std::string(results.get_field(0, 1));
        counts[std::string(results.get_field(1, 0))] = std::string(results.get_field(1, 1));
        
        EXPECT_EQ(counts["10"], "2");
        EXPECT_EQ(counts["20"], "1");
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
