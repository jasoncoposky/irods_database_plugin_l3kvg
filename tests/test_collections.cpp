#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"

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

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
