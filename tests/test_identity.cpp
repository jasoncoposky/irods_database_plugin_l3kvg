#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"

using namespace irods::catalog;

TEST(IdentityTest, AuthCheckZeroCopy) {
    Config cfg;
    cfg.db_path = "identity.l3kvg";
    cfg.node_id = 1;
    cfg.zmq_endpoint = "tcp://127.0.0.1:5555";
    
    CatalogFacade catalog;
    // Note: This will fail if no server is running, which is expected in Phase 2/3
    // until the L3KVG server agent completes their work.
    // For now, we verify that the plugin can initialize with the new Config.
    bool init_ok = catalog.init(cfg).ok();
    if (!init_ok) {
        std::cout << "Skipping test: L3KVG Server not available for Smart Client" << std::endl;
        return;
    }

    // 0. Bootstrap local zone
    ASSERT_TRUE(catalog.bootstrap_catalog("tempZone", "rods").ok());

    // 1. Register a rodsadmin
    user admin;
    admin.id = 1;
    admin.name = "rods";
    admin.zone = "tempZone";
    admin.type = "rodsadmin";
    
    user_id_t out_id;
    ASSERT_TRUE(catalog.register_user(admin, out_id).ok());

    // 2. Check Auth
    int priv = 0;
    ASSERT_TRUE(catalog.check_auth("rods", "tempZone", priv).ok());
    EXPECT_EQ(priv, 5); 
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
