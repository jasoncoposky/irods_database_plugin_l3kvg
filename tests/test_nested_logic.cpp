#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"
#include <unordered_set>

using namespace irods::catalog;

TEST(NestedLogicTest, ComplexOrQuery) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "nested.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Setup Data
    // Object 1: Large, different name
    data_object obj1;
    obj1.id = 101;
    obj1.name = "other.txt";
    obj1.size = 5000;
    data_id_t out1;
    catalog.register_data_object(obj1, out1);

    // Object 2: Small, matching name
    data_object obj2;
    obj2.id = 102;
    obj2.name = "report_v1.pdf";
    obj2.size = 500;
    data_id_t out2;
    catalog.register_data_object(obj2, out2);

    // Object 3: Small, different name (Should NOT match)
    data_object obj3;
    obj3.id = 103;
    obj3.name = "notes.txt";
    obj3.size = 100;
    data_id_t out3;
    catalog.register_data_object(obj3, out3);

    catalog.get_engine()->get_store()->wait_all_shards();

    // 2. Query: SELECT DATA_NAME WHERE (DATA_SIZE > 1000 OR DATA_NAME LIKE 'report%')
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"DATA_NAME"});
    
    gq::logical_or lo;
    lo.condition.push_back(gq::condition{gq::column{"DATA_SIZE"}, gq::condition_greater_than{"1000"}});
    lo.condition.push_back(gq::condition{gq::column{"DATA_NAME"}, gq::condition_like{"report%"}});
    
    ast.conditions.push_back(lo);

    ResultSet results;
    ASSERT_TRUE(catalog.execute_query(ast, results).ok());
    
    // 3. Validate: obj1 and obj2 should match
    ASSERT_EQ(results.row_count(), 2);
    
    std::unordered_set<std::string> names;
    names.insert(std::string(results.get_field(0, 0)));
    names.insert(std::string(results.get_field(1, 0)));
    
    EXPECT_TRUE(names.contains("other.txt"));
    EXPECT_TRUE(names.contains("report_v1.pdf"));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
