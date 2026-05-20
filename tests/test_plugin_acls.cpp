#include "plugin_test_fixture.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/irods_database_constants.hpp"

using namespace irods::catalog;
using namespace irods::catalog::test;

class AclPluginTest : public PluginTestFixture {};

TEST_F(AclPluginTest, GroupAccessModel) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    uint16_t local_cid = SnowflakeID::calculate_cluster_id("tempZone");

    // 2. Register Alice and Group Research (Creates Proxy Indexes)
    userInfo_t u_alice;
    std::memset(&u_alice, 0, sizeof(u_alice));
    std::strncpy(u_alice.userName, "alice", NAME_LEN);
    std::strncpy(u_alice.rodsZone, "tempZone", NAME_LEN);
    u_alice.sysUid = 100;
    ASSERT_TRUE(plugin()->call<userInfo_t*>(nullptr, irods::DATABASE_OP_REG_USER_RE, nullptr, &u_alice).ok());

    userInfo_t g_research;
    std::memset(&g_research, 0, sizeof(g_research));
    std::strncpy(g_research.userName, "research", NAME_LEN);
    std::strncpy(g_research.rodsZone, "tempZone", NAME_LEN);
    std::strncpy(g_research.userType, "rodsgroup", NAME_LEN);
    g_research.sysUid = 200;
    ASSERT_TRUE(plugin()->call<userInfo_t*>(nullptr, irods::DATABASE_OP_REG_USER_RE, nullptr, &g_research).ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 3. Add Alice to Research Group
    auto ret_group = plugin()->call<const char*, const char*, const char*, const char*>(
        nullptr, irods::DATABASE_OP_MOD_GROUP, nullptr, "research", "add", "alice", "tempZone");
    ASSERT_TRUE(ret_group.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    snowflake_id_t aid = SnowflakeID::create(local_cid, "2:100"); // alice
    snowflake_id_t gid = SnowflakeID::create(local_cid, "2:200"); // research

    ASSERT_TRUE(server()->has_node(aid));
    ASSERT_TRUE(server()->has_node(gid));

    // Verify MEMBER_OF edge: Alice --(MEMBER_OF)--> Research
    bool member_found = false;
    for (const auto& edge : server()->get_node(aid).edges) {
        if (edge.first == "MEMBER_OF" && edge.second == gid) {
            member_found = true;
            break;
        }
    }
    ASSERT_TRUE(member_found);

    // 4. Grant Research Group Access to a Collection
    collInfo_t coll;
    std::memset(&coll, 0, sizeof(coll));
    coll.collId = 500;
    std::strncpy(coll.collName, "/tempZone/home/research_data", NAME_LEN);
    ASSERT_TRUE(plugin()->call<collInfo_t*>(nullptr, irods::DATABASE_OP_REG_COLL, nullptr, &coll).ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // recursive, level, user, zone, path
    auto ret_acl = plugin()->call<int, const char*, const char*, const char*, const char*>(
        nullptr, irods::DATABASE_OP_MOD_ACCESS_CONTROL, nullptr, 0, "own", "research", "tempZone", "/tempZone/home/research_data");
    ASSERT_TRUE(ret_acl.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    snowflake_id_t cid = SnowflakeID::create(local_cid, "3:500"); // collection
    snowflake_id_t access_id = SnowflakeID::create(local_cid, std::to_string(gid) + ":" + std::to_string(cid));

    ASSERT_TRUE(server()->has_node(access_id));
    
    // Verify edges: Research --(HAS_ACCESS)--> Access --(FOR_OBJECT)--> Collection
    bool has_access_found = false;
    for (const auto& edge : server()->get_node(gid).edges) {
        if (edge.first == "HAS_ACCESS" && edge.second == access_id) {
            has_access_found = true;
            break;
        }
    }
    ASSERT_TRUE(has_access_found);

    bool for_object_found = false;
    for (const auto& edge : server()->get_node(access_id).edges) {
        if (edge.first == "FOR_OBJECT" && edge.second == cid) {
            for_object_found = true;
            break;
        }
    }
    ASSERT_TRUE(for_object_found);

    // 5. Remove User from Group
    ret_group = plugin()->call<const char*, const char*, const char*, const char*>(
        nullptr, irods::DATABASE_OP_MOD_GROUP, nullptr, "research", "remove", "alice", "tempZone");
    ASSERT_TRUE(ret_group.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    
    // Verify MEMBER_OF edge is gone
    bool removed_member_found = false;
    snowflake_id_t local_alice_id = SnowflakeID::create(local_cid, "2:100");
    for (const auto& edge : server()->get_node(local_alice_id).edges) {
        if (edge.first == "MEMBER_OF" && edge.second == gid) {
            removed_member_found = true;
            break;
        }
    }
    ASSERT_FALSE(removed_member_found);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
