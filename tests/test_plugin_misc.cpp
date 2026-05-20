#include "plugin_test_fixture.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/irods_database_constants.hpp"

using namespace irods::catalog;
using namespace irods::catalog::test;

class MiscPluginTest : public PluginTestFixture {};

TEST_F(MiscPluginTest, TokensAndQuotas) {
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

    // Register User for Quota test
    userInfo_t u_alice;
    std::memset(&u_alice, 0, sizeof(u_alice));
    std::strncpy(u_alice.userName, "alice", NAME_LEN);
    std::strncpy(u_alice.rodsZone, "tempZone", NAME_LEN);
    ASSERT_TRUE((plugin()->call<userInfo_t*>(nullptr, irods::DATABASE_OP_REG_USER_RE, nullptr, &u_alice).ok()));

    // 2. Tokens
    ASSERT_TRUE((plugin()->call<const char*, const char*, const char*>(
        nullptr, irods::DATABASE_OP_REG_TOKEN, nullptr, "test_token", "value123", "ns1").ok()));
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    snowflake_id_t sid = SnowflakeID::create(local_cid, "ns1:test_token");
    ASSERT_TRUE(server()->has_node(sid));

    ASSERT_TRUE((plugin()->call<const char*, const char*>(
        nullptr, irods::DATABASE_OP_DEL_TOKEN, nullptr, "test_token", "ns1").ok()));
    
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_FALSE(server()->has_node(sid));

    // 3. Quotas
    ASSERT_TRUE((plugin()->call<const char*, const char*, rodsLong_t>(
        nullptr, irods::DATABASE_OP_SET_QUOTA, nullptr, "alice", "ufs", 1000000).ok()));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    rodsLong_t usage = 0, limit = 0;
    ASSERT_TRUE((plugin()->call<const char*, const char*, rodsLong_t*, rodsLong_t*>(
        nullptr, irods::DATABASE_OP_CHECK_QUOTA, nullptr, "alice", "ufs", &usage, &limit).ok()));
    ASSERT_EQ(limit, 1000000);

    // 4. Logical Quotas
    // Register Collection for Logical Quota test
    collInfo_t coll;
    std::memset(&coll, 0, sizeof(coll));
    coll.collId = 100;
    std::strncpy(coll.collName, "/tempZone/home/alice", NAME_LEN);
    ASSERT_TRUE((plugin()->call<collInfo_t*>(nullptr, irods::DATABASE_OP_REG_COLL, nullptr, &coll).ok()));

    ASSERT_TRUE((plugin()->call<const char*, rodsLong_t>(
        nullptr, irods::DATABASE_OP_SET_LOGICAL_QUOTA, nullptr, "/tempZone/home/alice", 5000000).ok()));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_TRUE((plugin()->call<const char*, rodsLong_t*, rodsLong_t*>(
        nullptr, irods::DATABASE_OP_CHECK_LOGICAL_QUOTA, nullptr, "/tempZone/home/alice", &usage, &limit).ok()));
    ASSERT_EQ(limit, 5000000);
}

TEST_F(MiscPluginTest, RuleExecutionAndGridConfig) {
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

    // 2. Rule Execution
    ruleExecSubmitInp_t re;
    std::memset(&re, 0, sizeof(re));
    std::strncpy(re.ruleName, "test_rule", NAME_LEN);
    std::strncpy(re.exeTime, "2026-05-18", TIME_LEN);
    std::strncpy(re.priority, "5", NAME_LEN);
    
    ASSERT_TRUE((plugin()->call<ruleExecSubmitInp_t*>(
        nullptr, irods::DATABASE_OP_REG_RULE_EXEC, nullptr, &re).ok()));

    // 3. Grid Config
    ASSERT_TRUE((plugin()->call<const char*, const char*>(
        nullptr, irods::DATABASE_OP_SET_GRID_CONFIGURATION_VALUE, nullptr, "cleanup_interval", "3600").ok()));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    char* val = nullptr;
    ASSERT_TRUE((plugin()->call<const char*, char**>(
        nullptr, irods::DATABASE_OP_GET_GRID_CONFIGURATION_VALUE, nullptr, "cleanup_interval", &val).ok()));
    ASSERT_STREQ(val, "3600");
    if (val) free(val);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
