#include "plugin_test_fixture.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/rodsLog.h"

using namespace irods::catalog;
using namespace irods::catalog::test;

class DataPluginTest : public PluginTestFixture {};

TEST_F(DataPluginTest, DataObjectLifecycle) {
    // 1. Setup Config
    irods::server_properties::instance().init("./server_config.json");
    ASSERT_TRUE(plugin()->call(nullptr, irods::DATABASE_OP_START, nullptr).ok());

    uint16_t local_cid = SnowflakeID::calculate_cluster_id("tempZone");

    // 2. Register Collection
    collInfo_t coll;
    std::memset(&coll, 0, sizeof(coll));
    coll.collId = 100;
    std::strncpy(coll.collName, "/tempZone/home/rods", NAME_LEN);
    std::strncpy(coll.collOwnerName, "rods", NAME_LEN);
    std::strncpy(coll.collOwnerZone, "tempZone", NAME_LEN);
    ASSERT_TRUE(plugin()->call<collInfo_t*>(nullptr, irods::DATABASE_OP_REG_COLL, nullptr, &coll).ok());

    // 3. Register Data Object
    dataObjInfo_t obj;
    std::memset(&obj, 0, sizeof(obj));
    obj.dataId = 1001;
    obj.collId = 100;
    std::strncpy(obj.objPath, "/tempZone/home/rods/test.txt", MAX_NAME_LEN);
    std::strncpy(obj.dataOwnerName, "rods", NAME_LEN);
    std::strncpy(obj.dataOwnerZone, "tempZone", NAME_LEN);
    ASSERT_TRUE(plugin()->call<dataObjInfo_t*>(nullptr, irods::DATABASE_OP_REG_DATA_OBJ, nullptr, &obj).ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    snowflake_id_t sid = SnowflakeID::create(local_cid, "4:1001"); // EntityType::DataObject = 4
    snowflake_id_t cid = SnowflakeID::create(local_cid, "3:100");  // EntityType::Collection = 3
    ASSERT_TRUE(server()->has_node(sid));
    ASSERT_TRUE(server()->has_node(cid));

    // Verify CONTAINS edge
    bool edge_found = false;
    for (const auto& edge : server()->get_node(cid).edges) {
        if (edge.first == "CONTAINS" && edge.second == sid) {
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
