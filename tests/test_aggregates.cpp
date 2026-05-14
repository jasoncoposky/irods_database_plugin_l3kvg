#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"

using namespace irods::catalog;

TEST(AggregateTest, CountAndSum) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "agg.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Setup Data: 3 objects of sizes 100, 200, 300
    for (int i = 1; i <= 3; ++i) {
        data_object obj;
        obj.id = 200 + i;
        obj.name = "file" + std::to_string(i) + ".txt";
        obj.size = i * 100;
        data_id_t out;
        catalog.register_data_object(obj, out);
    }

    catalog.get_engine()->get_store()->wait_all_shards();

    namespace gq = irods::experimental::genquery2;

    // 2. Query 1: SELECT COUNT(DATA_ID)
    {
        gq::select ast;
        gq::function fn;
        fn.name = "COUNT";
        fn.arguments.push_back(gq::column{"DATA_ID"});
        ast.projections.push_back(fn);

        ResultSet results;
        ASSERT_TRUE(catalog.execute_query(ast, results).ok());
        ASSERT_EQ(results.row_count(), 1);
        EXPECT_EQ(results.get_field(0, 0), "3");
    }

    // 3. Query 2: SELECT SUM(DATA_SIZE)
    {
        gq::select ast;
        gq::function fn;
        fn.name = "SUM";
        fn.arguments.push_back(gq::column{"DATA_SIZE"});
        ast.projections.push_back(fn);

        ResultSet results;
        ASSERT_TRUE(catalog.execute_query(ast, results).ok());
        ASSERT_EQ(results.row_count(), 1);
        EXPECT_EQ(results.get_field(0, 0), "600");
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
