#include "plugin_test_fixture.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/irods_database_constants.hpp"

using namespace irods::catalog;
using namespace irods::catalog::test;

class ResourcePluginTest : public PluginTestFixture {};

TEST_F(ResourcePluginTest, LifecycleAndHierarchy) {
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

    ASSERT_TRUE(plugin()->call(nullptr, irods::DATABASE_OP_START, nullptr).ok());

    uint16_t local_cid = SnowflakeID::calculate_cluster_id("tempZone");

    // 2. Register Resource
    std::map<std::string, std::string> info;
    info["resc_id"] = "501";
    info["resc_name"] = "ufs";
    info["resc_type"] = "unixfilesystem";
    info["resc_net"] = "localhost";
    info["resc_def_path"] = "/tmp";
    ASSERT_TRUE((plugin()->call<std::map<std::string, std::string>*>(
        nullptr, irods::DATABASE_OP_REG_RESC, nullptr, &info).ok()));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    snowflake_id_t rid = SnowflakeID::create(local_cid, "5:501");
    ASSERT_TRUE(server()->has_node(rid));

    // 3. Modify Resource
    ASSERT_TRUE((plugin()->call<const char*, const char*, const char*>(
        nullptr, irods::DATABASE_OP_MOD_RESC, nullptr, "ufs", "status", "down").ok()));
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Note: status is stored as int64 's' in the facade
    // For now we just verify it exists or check value if we know mapping
    
    // 4. Resource Hierarchy
    // Register Child
    std::map<std::string, std::string> child_info;
    child_info["resc_id"] = "502";
    child_info["resc_name"] = "ufs_child";
    child_info["resc_type"] = "unixfilesystem";
    ASSERT_TRUE((plugin()->call<std::map<std::string, std::string>*>(
        nullptr, irods::DATABASE_OP_REG_RESC, nullptr, &child_info).ok()));
    
    ASSERT_TRUE((plugin()->call<const char*, const char*, const char*>(
        nullptr, irods::DATABASE_OP_ADD_CHILD_RESC, nullptr, "ufs", "ufs_child", "").ok()));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    snowflake_id_t crid = SnowflakeID::create(local_cid, "5:502");
    
    // Verify HAS_CHILD edge
    bool edge_found = false;
    for (const auto& edge : server()->get_node(rid).edges) {
        if (edge.first == "HAS_CHILD" && edge.second == crid) {
            edge_found = true;
            break;
        }
    }
    ASSERT_TRUE(edge_found);

    // 5. Get Hierarchy
    char* hier = nullptr;
    ASSERT_TRUE((plugin()->call<const char*, char**>(
        nullptr, irods::DATABASE_OP_GET_HIERARCHY_FOR_RESC, nullptr, "ufs_child", &hier).ok()));
    ASSERT_STREQ(hier, "ufs;ufs_child");
    if (hier) free(hier);

    // 6. Delete Resource
    ASSERT_TRUE((plugin()->call<const char*, int>(
        nullptr, irods::DATABASE_OP_DEL_RESC, nullptr, "ufs_child", 0).ok()));
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_FALSE(server()->has_node(crid));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
