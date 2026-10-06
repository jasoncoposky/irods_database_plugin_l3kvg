#include "plugin_test_fixture.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/rodsErrorTable.h"

using namespace irods::catalog;
using namespace irods::catalog::test;

class CollectionTest : public PluginTestFixture {};

TEST_F(CollectionTest, Lifecycle) {
    // 1. Setup Config
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    CatalogFacade catalog;
    Config cfg;
    cfg.node_id = 1;
    cfg.zmq_endpoint = endpoint();
    ASSERT_TRUE(catalog.init(cfg, "tempZone").ok());

    // 1. Register Collections
    coll_id_t root_id, sub_id;
    collection c1;
    c1.id = 100;
    c1.name = "/tempZone/home/testcoll";
    c1.owner_name = "rods";
    c1.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(c1, root_id).ok());
    
    collection c2;
    c2.id = 200;
    c2.parent_id = 100;
    c2.name = "/tempZone/home/testcoll/sub";
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

TEST_F(CollectionTest, SubtreeHierarchyScan) {
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test_subtree.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    CatalogFacade catalog;
    Config cfg;
    cfg.node_id = 1;
    cfg.zmq_endpoint = endpoint();
    ASSERT_TRUE(catalog.init(cfg, "tempZone").ok());

    coll_id_t dummy_id;

    collection c100;
    c100.id = 100;
    c100.name = "/tempZone/home/testcoll";
    c100.owner_name = "rods";
    c100.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(c100, dummy_id).ok());

    collection c101;
    c101.id = 101;
    c101.parent_id = 100;
    c101.name = "/tempZone/home/testcoll/sub1";
    c101.owner_name = "rods";
    c101.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(c101, dummy_id).ok());

    collection c102;
    c102.id = 102;
    c102.parent_id = 101;
    c102.name = "/tempZone/home/testcoll/sub1/deep";
    c102.owner_name = "rods";
    c102.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(c102, dummy_id).ok());

    collection c103;
    c103.id = 103;
    c103.parent_id = 100;
    c103.name = "/tempZone/home/testcoll/sub2";
    c103.owner_name = "rods";
    c103.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(c103, dummy_id).ok());

    collection c200;
    c200.id = 200;
    c200.name = "/tempZone/home/other";
    c200.owner_name = "rods";
    c200.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(c200, dummy_id).ok());

    snowflake_id_t sid_100 = catalog.make_id(EntityType::Collection, 100);
    snowflake_id_t sid_101 = catalog.make_id(EntityType::Collection, 101);
    snowflake_id_t sid_102 = catalog.make_id(EntityType::Collection, 102);
    snowflake_id_t sid_103 = catalog.make_id(EntityType::Collection, 103);
    snowflake_id_t sid_200 = catalog.make_id(EntityType::Collection, 200);

    std::vector<snowflake_id_t> out_ids;
    ASSERT_TRUE(catalog.get_collection_subtree_ids(sid_100, out_ids).ok());

    EXPECT_EQ(out_ids.size(), 4);
    EXPECT_NE(std::find(out_ids.begin(), out_ids.end(), sid_100), out_ids.end());
    EXPECT_NE(std::find(out_ids.begin(), out_ids.end(), sid_101), out_ids.end());
    EXPECT_NE(std::find(out_ids.begin(), out_ids.end(), sid_102), out_ids.end());
    EXPECT_NE(std::find(out_ids.begin(), out_ids.end(), sid_103), out_ids.end());
    EXPECT_EQ(std::find(out_ids.begin(), out_ids.end(), sid_200), out_ids.end());
}

TEST_F(CollectionTest, NameCollisionWithDataObjectRejection) {
    nlohmann::json config;
    config["zone_name"] = "testZone";
    config["zone_user"] = "testuser";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test_collision.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    CatalogFacade catalog;
    Config cfg;
    cfg.node_id = 1;
    cfg.zmq_endpoint = endpoint();
    ASSERT_TRUE(catalog.init(cfg, "testZone").ok());

    coll_id_t coll_id;
    collection coll;
    coll.id = 300;
    coll.name = "/testZone/home/testuser/collision_test_dir";
    coll.owner_name = "testuser";
    coll.owner_zone = "testZone";
    ASSERT_TRUE(catalog.register_collection(coll, coll_id).ok());

    data_object obj;
    obj.id = 3001;
    obj.full_path = "/testZone/home/testuser/collision_test_dir";
    obj.name = "collision_test_dir";
    obj.owner_name = "testuser";
    obj.owner_zone = "testZone";
    data_id_t out_id;
    auto ret = catalog.register_data_object(obj, out_id);
    EXPECT_FALSE(ret.ok());
    EXPECT_EQ(ret.code(), CAT_NAME_EXISTS_AS_COLLECTION);
}

TEST_F(CollectionTest, CollectionCacheReset) {
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test_cache_reset.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    CatalogFacade catalog;
    Config cfg;
    cfg.node_id = 1;
    cfg.zmq_endpoint = endpoint();
    ASSERT_TRUE(catalog.init(cfg, "tempZone").ok());

    CatalogFacade::reset_collection_cache();

    coll_id_t out_coll_id;
    collection c;
    c.id = 400;
    c.name = "/tempZone/home/cached_coll";
    c.owner_name = "rods";
    c.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(c, out_coll_id).ok());

    snowflake_id_t sid = 0;
    EntityType et;
    ASSERT_TRUE(catalog.resolve_path("/tempZone/home/cached_coll", sid, et).ok());
    EXPECT_EQ(et, EntityType::Collection);

    CatalogFacade::reset_collection_cache();
    ASSERT_TRUE(catalog.resolve_path("/tempZone/home/cached_coll", sid, et).ok());
    EXPECT_EQ(et, EntityType::Collection);
}

class MetadataTest : public PluginTestFixture {};

TEST_F(MetadataTest, AvuLifecycle) {
    // 1. Setup Config
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    CatalogFacade catalog;
    Config cfg;
    cfg.node_id = 1;
    cfg.zmq_endpoint = endpoint();
    ASSERT_TRUE(catalog.init(cfg, "tempZone").ok());

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
