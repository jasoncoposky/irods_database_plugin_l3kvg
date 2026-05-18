#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"

using namespace irods::catalog;

TEST(CollectionTest, Lifecycle) {
    Config cfg;
    cfg.db_path = "collections.l3kvg";
    cfg.node_id = 1;
    cfg.zmq_endpoint = "tcp://127.0.0.1:5555";
    
    CatalogFacade catalog;
    if (!catalog.init(cfg).ok()) {
        std::cout << "Skipping test: L3KVG Server not available" << std::endl;
        return;
    }

    // 1. Register Collections
    coll_id_t root_id, sub_id;
    collection c1;
    c1.id = 100;
    c1.name = "/tempZone/home";
    c1.owner_name = "rods";
    c1.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(c1, root_id).ok());
    
    collection c2;
    c2.id = 200;
    c2.parent_id = 100;
    c2.name = "/tempZone/home/sub";
    c2.owner_name = "rods";
    c2.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(c2, sub_id).ok());

    // 2. Register Data Object
    data_object obj;
    obj.id = 1001;
    obj.coll_id = 200;
    obj.name = "old_name.txt";
    obj.owner_zone = "tempZone";
    data_id_t out_id;
    ASSERT_TRUE(catalog.register_data_object(obj, out_id).ok());

    // 3. Rename Object
    ASSERT_TRUE(catalog.rename_data_object(1001, "new_name.txt").ok());

    // 4. Move Object
    ASSERT_TRUE(catalog.move_data_object(1001, 100).ok());

    // 5. Delete Collection
    ASSERT_TRUE(catalog.delete_collection(200).ok());
}

TEST(MetadataTest, AvuLifecycle) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "metadata.l3kvg";
    cfg.node_id = 1;
    cfg.zmq_endpoint = "tcp://127.0.0.1:5555";
    if (!catalog.init(cfg).ok()) return;

    // 1. Add AVU to Object
    avu a1{"color", "blue", "none"};
    ASSERT_TRUE(catalog.add_avu_metadata("data", "1001", a1).ok());

    // 2. Copy AVU
    ASSERT_TRUE(catalog.copy_avu_metadata("data", "1001", "data", "1002").ok());

    // 3. Set AVU
    avu a2{"color", "red", "none"};
    ASSERT_TRUE(catalog.set_avu_metadata("data", "1001", a2).ok());
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
