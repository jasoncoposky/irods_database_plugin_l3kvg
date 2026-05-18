#include "plugin_test_fixture.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/rodsLog.h"
#include <cstdlib>

using namespace irods::catalog;
using namespace irods::catalog::test;

class IdentityPluginTest : public PluginTestFixture {};

TEST_F(IdentityPluginTest, BootstrapAndAuth) {
    // 1. Setup Mock iRODS Config File
    irods::server_properties::instance().init("./server_config.json");

    // 2. Start Plugin
    auto ret = plugin()->call(nullptr, irods::DATABASE_OP_START, nullptr);
    if (!ret.ok()) {
        std::cerr << "DATABASE_OP_START failed: " << ret.result() << std::endl;
    }
    ASSERT_TRUE(ret.ok());

    // 3. Verify Bootstrap Nodes
    uint16_t local_cid = SnowflakeID::calculate_cluster_id("tempZone");
    snowflake_id_t zid = SnowflakeID::create(local_cid, "1:1");
    snowflake_id_t uid = SnowflakeID::create(local_cid, "2:1");

    std::cout << "[Test] Expecting Zone Snowflake ID: [" << std::hex << zid << "]" << std::endl;
    std::cout << "[Test] Expecting Admin Snowflake ID: [" << std::hex << uid << "]" << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ASSERT_TRUE(server()->has_node(zid));
    ASSERT_TRUE(server()->has_node(uid));
    
    // Verify the HAS_USER edge exists in the mock server
    bool edge_found = false;
    for (const auto& edge : server()->get_node(zid).edges) {
        if (edge.first == "HAS_USER" && edge.second == uid) {
            edge_found = true;
            break;
        }
    }
    ASSERT_TRUE(edge_found);

    // 4. Register a new user
    userInfo_t user;
    std::memset(&user, 0, sizeof(user));
    std::strncpy(user.userName, "alice", NAME_LEN);
    std::strncpy(user.rodsZone, "tempZone", NAME_LEN);
    std::strncpy(user.userType, "rodsuser", NAME_LEN);
    user.sysUid = 1002;
    
    ASSERT_TRUE(plugin()->call<userInfo_t*>(nullptr, irods::DATABASE_OP_REG_USER_RE, nullptr, &user).ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    snowflake_id_t alice_id = SnowflakeID::create(local_cid, "2:1002");
    ASSERT_TRUE(server()->has_node(alice_id));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
