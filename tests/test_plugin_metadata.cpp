#include "plugin_test_fixture.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/irods_database_constants.hpp"

using namespace irods::catalog;
using namespace irods::catalog::test;

class MetadataPluginTest : public PluginTestFixture {};

TEST_F(MetadataPluginTest, AvuLifecycle) {
    // 1. Setup Config
    irods::server_properties::instance().init("./server_config.json");
    irods::plugin_property_map prop_map;
    irods::plugin_context ctx(nullptr, prop_map);
    ASSERT_TRUE(plugin()->call(nullptr, irods::DATABASE_OP_START, nullptr).ok());

    uint16_t local_cid = SnowflakeID::calculate_cluster_id("tempZone");

    // 2. Add AVU to Data Object
    auto ret = plugin()->call<const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(
        nullptr, irods::DATABASE_OP_ADD_AVU_METADATA, nullptr, 
        "data", "1001", "color", "red", "none", nullptr);
    ASSERT_TRUE(ret.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // AVU ID: XXH3(color:red:none)
    std::string avu_uuid = "color:red:none";
    snowflake_id_t aid = SnowflakeID::create(local_cid, avu_uuid);
    snowflake_id_t tid = SnowflakeID::create(local_cid, "4:1001"); // DataObject

    ASSERT_TRUE(server()->has_node(aid));
    
    // Verify ANNOTATED_WITH edge
    bool edge_found = false;
    for (const auto& edge : server()->get_node(tid).edges) {
        if (edge.first == "ANNOTATED_WITH" && edge.second == aid) {
            edge_found = true;
            break;
        }
    }
    ASSERT_TRUE(edge_found);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
