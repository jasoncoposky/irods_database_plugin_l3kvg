#include "plugin_test_fixture.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/irods_database_constants.hpp"

using namespace irods::catalog;
using namespace irods::catalog::test;

class CollectionPluginTest : public PluginTestFixture {};

TEST_F(CollectionPluginTest, Lifecycle) {
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

    // 1. Register Collection
    collInfo_t coll;
    std::memset(&coll, 0, sizeof(coll));
    coll.collId = 100;
    std::strncpy(coll.collName, "/tempZone/home/rods", NAME_LEN);
    std::strncpy(coll.collOwnerName, "rods", NAME_LEN);
    std::strncpy(coll.collOwnerZone, "tempZone", NAME_LEN);
    ASSERT_TRUE(plugin()->call<collInfo_t*>(nullptr, irods::DATABASE_OP_REG_COLL, nullptr, &coll).ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    snowflake_id_t cid = SnowflakeID::create(local_cid, "3:100");
    ASSERT_TRUE(server()->has_node(cid));

    // 2. Modify Collection
    std::strncpy(coll.collOwnerName, "alice", NAME_LEN);
    // Simulation of condInput
    coll.condInput.len = 1;
    coll.condInput.keyWord = (char**)malloc(sizeof(char*));
    coll.condInput.value = (char**)malloc(sizeof(char*));
    coll.condInput.keyWord[0] = strdup("o");
    coll.condInput.value[0] = strdup("alice");
    
    ASSERT_TRUE(plugin()->call<collInfo_t*>(nullptr, irods::DATABASE_OP_MOD_COLL, nullptr, &coll).ok());
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(server()->get_node(cid).get_attribute("o"), "alice");

    // 3. Rename Collection
    ASSERT_TRUE(plugin()->call<const char*, const char*>(
        nullptr, irods::DATABASE_OP_RENAME_COLL, nullptr, "/tempZone/home/rods", "/tempZone/home/alice").ok());
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(server()->get_node(cid).get_attribute("n"), "/tempZone/home/alice");

    // 4. Delete Collection
    ASSERT_TRUE(plugin()->call<collInfo_t*>(nullptr, irods::DATABASE_OP_DEL_COLL, nullptr, &coll).ok());
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_FALSE(server()->has_node(cid));

    free(coll.condInput.keyWord[0]);
    free(coll.condInput.value[0]);
    free(coll.condInput.keyWord);
    free(coll.condInput.value);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
