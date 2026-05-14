#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"

using namespace irods::catalog;

TEST(CollectionTest, RenameAndMove) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "collections.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Register Collections
    coll_id_t root_id, sub_id;
    collection c1{100, 0, "/tempZone/home", "rods", "tempZone", "", ""};
    ASSERT_TRUE(catalog.register_collection(c1, root_id).ok());
    
    collection c2{200, 100, "/tempZone/home/sub", "rods", "tempZone", "", ""};
    ASSERT_TRUE(catalog.register_collection(c2, sub_id).ok());

    // 2. Register Data Object
    data_object obj;
    obj.id = 1001;
    obj.coll_id = 200;
    obj.name = "old_name.txt";
    data_id_t out_id;
    ASSERT_TRUE(catalog.register_data_object(obj, out_id).ok());

    // 3. Rename Object (Zero-Copy Patch)
    ASSERT_TRUE(catalog.rename_data_object(1001, "new_name.txt").ok());

    // 4. Move Object (Re-parenting Edge)
    ASSERT_TRUE(catalog.move_data_object(1001, 100).ok());

    // 5. Delete Collection
    ASSERT_TRUE(catalog.delete_collection(200).ok());
}

TEST(MetadataTest, AvuLifecycle) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "metadata.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Add AVU to Object
    avu a1{"color", "blue", "none"};
    ASSERT_TRUE(catalog.add_avu_metadata("data", "1001", a1).ok());

    // 2. Copy AVU (Graph edge cloning)
    ASSERT_TRUE(catalog.copy_avu_metadata("data", "1001", "data", "1002").ok());

    // 3. Set AVU (Clear and replace)
    avu a2{"color", "red", "none"};
    ASSERT_TRUE(catalog.set_avu_metadata("data", "1001", a2).ok());
}

TEST(QueryTest, GenQueryMetadata) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "queries.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Setup Data + Metadata
    data_object obj;
    obj.id = 555;
    obj.name = "query_test.txt";
    data_id_t out_id;
    catalog.register_data_object(obj, out_id);

    avu meta{"quality", "high", ""};
    catalog.add_avu_metadata("data", std::to_string(out_id), meta);

    // Synchronize to ensure async writes are committed before query
    catalog.get_engine()->get_store()->wait_all_shards();

    // 2. Perform GenQuery: SELECT META_DATA_ATTR_VALUE WHERE DATA_ID = 555
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"META_DATA_ATTR_VALUE"});
    ast.conditions.push_back(gq::condition{gq::column{"DATA_ID"}, gq::condition_equal{"555"}});

    ResultSet results;
    ASSERT_TRUE(catalog.execute_query(ast, results).ok());
    
    // 3. Validate
    ASSERT_EQ(results.row_count(), 1);
    /*
    if (results.row_count() > 0) {
        std::cout << "[Test] GenQuery Result field 'AVU.value' = [" << results.get_field(0, "AVU.value") << "]\n";
    }
    */
    EXPECT_EQ(results.get_field(0, "AVU.value"), "high");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
