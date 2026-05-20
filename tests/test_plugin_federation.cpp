#include "plugin_test_fixture.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/rodsLog.h"
#include <fstream>
#include <cstdlib>

using namespace irods::catalog;
using namespace irods::catalog::test;

class FederationPluginTest : public PluginTestFixture {};

TEST_F(FederationPluginTest, RemoteZoneAnchors) {
    // 1. Setup Config File
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    
    nlohmann::json fed_zone;
    fed_zone["name"] = "eu-west";
    fed_zone["id"] = 100;
    fed_zone["endpoint"] = "tcp://127.0.0.1:5566";
    
    nlohmann::json db_config;
    db_config["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()},
        {"federation", {fed_zone}}
    };
    config["plugin_configuration"]["database"] = db_config;

    irods::server_properties::instance().set_configuration(config);
    
    // 2. Start Plugin
    auto ret = plugin()->call(nullptr, irods::DATABASE_OP_START, nullptr);
    ASSERT_TRUE(ret.ok());

    // 3. Verify Remote Zone Anchor
    uint64_t remote_hash = XXH3_64bits("eu-west", 7);
    snowflake_id_t remote_zid = (static_cast<uint64_t>(100) << 48) | (remote_hash & SnowflakeID::LOCAL_HASH_MASK);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ASSERT_TRUE(server()->has_node(remote_zid));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
