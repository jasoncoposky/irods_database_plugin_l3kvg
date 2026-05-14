#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"

using namespace irods::catalog;

TEST(IdentityTest, AuthCheckZeroCopy) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "identity.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Register a rodsadmin
    user admin;
    admin.id = 1;
    admin.name = "rods";
    admin.zone = "tempZone";
    admin.type = "rodsadmin";
    
    user_id_t out_id;
    ASSERT_TRUE(catalog.register_user(admin, out_id).ok());
    catalog.get_engine()->get_store()->wait_all_shards();

    // 2. Check Auth (Retrieves view from shard)
    int priv = 0;
    ASSERT_TRUE(catalog.check_auth("rods", "tempZone", priv).ok());
    EXPECT_EQ(priv, 5); // rodsadmin mapping

    // 3. Register a regular user
    user alice;
    alice.id = 2;
    alice.name = "alice";
    alice.zone = "tempZone";
    alice.type = "rodsuser";
    ASSERT_TRUE(catalog.register_user(alice, out_id).ok());
    catalog.get_engine()->get_store()->wait_all_shards();

    // 4. Check Auth for regular user
    ASSERT_TRUE(catalog.check_auth("alice", "tempZone", priv).ok());
    EXPECT_EQ(priv, 1); // rodsuser mapping
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
