#include "plugin_test_fixture.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/rodsLog.h"
#include <cstdlib>

using namespace irods::catalog;
using namespace irods::catalog::test;

class IdentityPluginTest : public PluginTestFixture {
protected:
    snowflake_id_t resolve_id_from_index(EntityType type, std::string_view attr, std::string_view value) {
        uint16_t local_cid = SnowflakeID::calculate_cluster_id("tempZone");
        std::string type_str;
        switch(type) {
            case EntityType::Zone: type_str = "Zone"; break;
            case EntityType::User: type_str = "User"; break;
            case EntityType::Collection: type_str = "Collection"; break;
            case EntityType::DataObject: type_str = "DataObject"; break;
            case EntityType::Resource: type_str = "Resource"; break;
            default: type_str = "Unknown"; break;
        }
        std::string idx_key = "idx:" + type_str + ":" + std::string(attr) + ":" + std::string(value);
        std::string payload = server()->get_generic_value(idx_key);
        if (payload.empty()) return 0;
        return std::stoull(payload, nullptr, 16);
    }
};

TEST_F(IdentityPluginTest, BootstrapAndAuth) {
    // 1. Setup Mock iRODS Config
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    // 2. Start Plugin
    auto ret = plugin()->call(nullptr, irods::DATABASE_OP_START, nullptr);
    ASSERT_TRUE(ret.ok());

    // Bootstrap
    ret = plugin()->call(nullptr, "database_initialize_catalog", nullptr);
    ASSERT_TRUE(ret.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

TEST_F(IdentityPluginTest, GroupOperations) {
    // 1. Setup Mock iRODS Config
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test_group.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    // 2. Start Plugin and Bootstrap
    ASSERT_TRUE(plugin()->call(nullptr, irods::DATABASE_OP_START, nullptr).ok());
    ASSERT_TRUE(plugin()->call(nullptr, "database_initialize_catalog", nullptr).ok());

    // 3. Register a new group
    userInfo_t group;
    std::memset(&group, 0, sizeof(group));
    std::strncpy(group.userName, "devs", NAME_LEN);
    std::strncpy(group.rodsZone, "tempZone", NAME_LEN);
    std::strncpy(group.userType, "rodsgroup", NAME_LEN);
    group.sysUid = 2001;
    
    ASSERT_TRUE(plugin()->call<userInfo_t*>(nullptr, irods::DATABASE_OP_REG_USER_RE, nullptr, &group).ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    
    // 4. Add user to group
    ASSERT_TRUE((plugin()->call<const char*, const char*, const char*, const char*>(
        nullptr, irods::DATABASE_OP_MOD_GROUP, nullptr, "devs", "add", "rods", "tempZone").ok()));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Verify edge exists: rods -> MEMBER_OF -> devs
    snowflake_id_t gid = resolve_id_from_index(EntityType::User, "n", "devs");
    snowflake_id_t rid = resolve_id_from_index(EntityType::User, "n", "rods");

    ASSERT_NE(gid, 0);
    ASSERT_NE(rid, 0);

    bool edge_found = false;
    for (const auto& edge : server()->get_node(rid).edges) {
        if (edge.first == "MEMBER_OF" && edge.second == gid) {
            edge_found = true;
            break;
        }
    }
    ASSERT_TRUE(edge_found);
}

TEST_F(IdentityPluginTest, NullSafety) {
    // 1. Setup Mock iRODS Config
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test_null.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    // 2. Start Plugin
    ASSERT_TRUE(plugin()->call(nullptr, irods::DATABASE_OP_START, nullptr).ok());

    // 3. Test various operations with NULL pointers - Should NOT crash
    
    plugin()->call<const char*, const char*, const char*>(
        nullptr, irods::DATABASE_OP_MOD_USER, nullptr, nullptr, nullptr, nullptr);

    plugin()->call<collInfo_t*>(
        nullptr, irods::DATABASE_OP_REG_COLL, nullptr, nullptr);

    plugin()->call<const char*, const char*, const char*, const char*>(
        nullptr, irods::DATABASE_OP_MOD_GROUP, nullptr, nullptr, nullptr, nullptr, nullptr);

    plugin()->call<const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(
        nullptr, irods::DATABASE_OP_ADD_AVU_METADATA, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
