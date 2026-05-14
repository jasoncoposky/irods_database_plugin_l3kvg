#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"

using namespace irods::catalog;

TEST(SortingTest, BasicOrdering) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "sort.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Setup Data: 3 objects with specific names and sizes
    // objA: size 300, name "z_file.txt"
    // objB: size 100, name "a_file.txt"
    // objC: size 200, name "m_file.txt"
    
    data_object objA; objA.id = 301; objA.name = "z_file.txt"; objA.size = 300;
    data_object objB; objB.id = 302; objB.name = "a_file.txt"; objB.size = 100;
    data_object objC; objC.id = 303; objC.name = "m_file.txt"; objC.size = 200;
    
    data_id_t out;
    catalog.register_data_object(objA, out);
    catalog.register_data_object(objB, out);
    catalog.register_data_object(objC, out);

    catalog.get_engine()->get_store()->wait_all_shards();

    namespace gq = irods::experimental::genquery2;

    // 2. Query: ORDER BY DATA_SIZE ASC
    {
        gq::select ast;
        ast.projections.push_back(gq::column{"DATA_NAME"});
        ast.order_by.sort_expressions.push_back({gq::column{"DATA_SIZE"}, true});

        ResultSet results;
        ASSERT_TRUE(catalog.execute_query(ast, results).ok());
        ASSERT_EQ(results.row_count(), 3);
        EXPECT_EQ(results.get_field(0, 0), "a_file.txt"); // size 100
        EXPECT_EQ(results.get_field(1, 0), "m_file.txt"); // size 200
        EXPECT_EQ(results.get_field(2, 0), "z_file.txt"); // size 300
    }

    // 3. Query: ORDER BY DATA_NAME ASC LIMIT 2 OFFSET 1
    {
        gq::select ast;
        ast.projections.push_back(gq::column{"DATA_NAME"});
        ast.order_by.sort_expressions.push_back({gq::column{"DATA_NAME"}, true});
        ast.range.number_of_rows = "2";
        ast.range.offset = "1";

        ResultSet results;
        ASSERT_TRUE(catalog.execute_query(ast, results).ok());
        
        // Full order: a_file, m_file, z_file
        // Offset 1 -> m_file, z_file
        // Limit 2 -> m_file, z_file
        ASSERT_EQ(results.row_count(), 2);
        EXPECT_EQ(results.get_field(0, 0), "m_file.txt");
        EXPECT_EQ(results.get_field(1, 0), "z_file.txt");
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
