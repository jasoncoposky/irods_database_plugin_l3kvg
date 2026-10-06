#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsErrorTable.h"
#include "mock_l3kvg.hpp"
#include "buffer.hpp"
#include <random>

using namespace irods::catalog;
using namespace irods::catalog::compiler;
using namespace irods::catalog::test;

class CatalogFacadeDmlTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(5600, 7600);
        int port = dis(gen);
        endpoint_ = "tcp://127.0.0.1:" + std::to_string(port);
        mock_server_ = std::make_unique<MockL3KVGServer>(endpoint_);
        mock_server_->start();

        Config cfg;
        cfg.node_id = 1;
        cfg.cluster_id = 1;
        cfg.zmq_endpoint = endpoint_;

        auto err = facade_.init(cfg, "tempZone");
        ASSERT_TRUE(err.ok()) << err.result();
    }

    void TearDown() override {
        if (mock_server_) {
            mock_server_->stop();
        }
    }

    std::string endpoint_;
    std::unique_ptr<MockL3KVGServer> mock_server_;
    CatalogFacade facade_;
};

TEST_F(CatalogFacadeDmlTest, ExecuteDmlLifecycle) {
    // 1. Test Insert: plan with action Insert, entity_type "DataObject", properties "n" -> "test_file.dat", "s" -> "1024".
    DmlPlan insert_plan;
    insert_plan.action = DmlAction::Insert;
    insert_plan.entity_type = "DataObject";
    insert_plan.properties["n"] = "test_file.dat";
    insert_plan.properties["s"] = "1024";

    lite3cpp::Buffer insert_res;
    auto insert_err = facade_.execute_dml(insert_plan, insert_res);
    ASSERT_TRUE(insert_err.ok()) << insert_err.result();
    EXPECT_EQ(insert_res.get_i64(0, "rows_affected"), 1);
    EXPECT_EQ(insert_res.get_str(0, "status"), "SUCCESS");

    // 2. Test Secondary Condition Mismatch: update with correct "n" but mismatched "s" (9999 != 1024)
    DmlPlan mismatch_update;
    mismatch_update.action = DmlAction::Update;
    mismatch_update.entity_type = "DataObject";
    mismatch_update.properties["s"] = "2048";
    mismatch_update.conditions.push_back(DmlCondition{"n", 0, "test_file.dat"});
    mismatch_update.conditions.push_back(DmlCondition{"s", 0, "9999"});

    lite3cpp::Buffer mismatch_res;
    auto mismatch_err = facade_.execute_dml(mismatch_update, mismatch_res);
    ASSERT_TRUE(mismatch_err.ok()) << mismatch_err.result();
    EXPECT_EQ(mismatch_res.get_i64(0, "rows_affected"), 0);

    // 2b. Test Inequality Operator: s < 500 fails (actual is 1024)
    DmlPlan ineq_fail;
    ineq_fail.action = DmlAction::Update;
    ineq_fail.entity_type = "DataObject";
    ineq_fail.properties["s"] = "2048";
    ineq_fail.conditions.push_back(DmlCondition{"n", 0, "test_file.dat"});
    ineq_fail.conditions.push_back(DmlCondition{"s", 4, "500"}); // op 4 is '<'

    lite3cpp::Buffer ineq_fail_res;
    ASSERT_TRUE(facade_.execute_dml(ineq_fail, ineq_fail_res).ok());
    EXPECT_EQ(ineq_fail_res.get_i64(0, "rows_affected"), 0);

    // 2c. Test Inequality Operator: s > 500 succeeds (actual is 1024)
    DmlPlan ineq_pass;
    ineq_pass.action = DmlAction::Update;
    ineq_pass.entity_type = "DataObject";
    ineq_pass.properties["s"] = "1500";
    ineq_pass.conditions.push_back(DmlCondition{"n", 0, "test_file.dat"});
    ineq_pass.conditions.push_back(DmlCondition{"s", 2, "500"}); // op 2 is '>'

    lite3cpp::Buffer ineq_pass_res;
    ASSERT_TRUE(facade_.execute_dml(ineq_pass, ineq_pass_res).ok());
    EXPECT_EQ(ineq_pass_res.get_i64(0, "rows_affected"), 1);

    // 3. Test Update with "name" attribute and rename: change "name" -> "test_file_renamed.dat" and "s" -> "2048"
    DmlPlan update_plan;
    update_plan.action = DmlAction::Update;
    update_plan.entity_type = "DataObject";
    update_plan.properties["name"] = "test_file_renamed.dat";
    update_plan.properties["s"] = "2048";
    update_plan.conditions.push_back(DmlCondition{"n", 0, "test_file.dat"});

    lite3cpp::Buffer update_res;
    auto update_err = facade_.execute_dml(update_plan, update_res);
    ASSERT_TRUE(update_err.ok()) << update_err.result();
    EXPECT_EQ(update_res.get_i64(0, "rows_affected"), 1);
    EXPECT_EQ(update_res.get_str(0, "status"), "SUCCESS");

    // 4. Test Update targeting new name index:
    DmlPlan update2_plan;
    update2_plan.action = DmlAction::Update;
    update2_plan.entity_type = "DataObject";
    update2_plan.properties["s"] = "4096";
    update2_plan.conditions.push_back(DmlCondition{"name", 0, "test_file_renamed.dat"});

    lite3cpp::Buffer update2_res;
    auto update2_err = facade_.execute_dml(update2_plan, update2_res);
    ASSERT_TRUE(update2_err.ok()) << update2_err.result();
    EXPECT_EQ(update2_res.get_i64(0, "rows_affected"), 1);

    // 5. Test Secondary Condition Mismatch on Remove:
    DmlPlan mismatch_remove;
    mismatch_remove.action = DmlAction::Remove;
    mismatch_remove.entity_type = "DataObject";
    mismatch_remove.conditions.push_back(DmlCondition{"n", 0, "test_file_renamed.dat"});
    mismatch_remove.conditions.push_back(DmlCondition{"s", 0, "9999"});

    lite3cpp::Buffer mismatch_rem_res;
    auto mismatch_rem_err = facade_.execute_dml(mismatch_remove, mismatch_rem_res);
    ASSERT_TRUE(mismatch_rem_err.ok()) << mismatch_rem_err.result();
    EXPECT_EQ(mismatch_rem_res.get_i64(0, "rows_affected"), 0);

    // 6. Test Remove: plan with action Remove, entity_type "DataObject", condition on "name" == "test_file_renamed.dat".
    DmlPlan remove_plan;
    remove_plan.action = DmlAction::Remove;
    remove_plan.entity_type = "DataObject";
    remove_plan.conditions.push_back(DmlCondition{"name", 0, "test_file_renamed.dat"});

    lite3cpp::Buffer remove_res;
    auto remove_err = facade_.execute_dml(remove_plan, remove_res);
    ASSERT_TRUE(remove_err.ok()) << remove_err.result();
    EXPECT_EQ(remove_res.get_i64(0, "rows_affected"), 1);
    EXPECT_EQ(remove_res.get_str(0, "status"), "SUCCESS");

    // 7. Test Repeated Remove: asserting rows_affected == 0 confirming complete deletion
    lite3cpp::Buffer repeat_res;
    auto repeat_err = facade_.execute_dml(remove_plan, repeat_res);
    ASSERT_TRUE(repeat_err.ok()) << repeat_err.result();
    EXPECT_EQ(repeat_res.get_i64(0, "rows_affected"), 0);
}

TEST_F(CatalogFacadeDmlTest, ExecuteDmlInsertWithParentCollAndPath) {
    // Verify root collection and user exist from init()
    snowflake_id_t root_id = 0;
    EntityType et;
    ASSERT_TRUE(facade_.resolve_path("/tempZone/home/rods", root_id, et).ok());
    ASSERT_NE(facade_.resolve_user("rods", "tempZone"), 0);

    // Insert DataObject with parent_coll and owner
    DmlPlan insert_plan;
    insert_plan.action = DmlAction::Insert;
    insert_plan.entity_type = "DataObject";
    insert_plan.properties["n"] = "sub_file.txt";
    insert_plan.properties["parent_coll"] = "/tempZone/home/rods";
    insert_plan.properties["owner"] = "rods";
    insert_plan.properties["s"] = "4096";

    lite3cpp::Buffer insert_res;
    auto insert_err = facade_.execute_dml(insert_plan, insert_res);
    ASSERT_TRUE(insert_err.ok()) << insert_err.result();
    EXPECT_EQ(insert_res.get_i64(0, "rows_affected"), 1);

    // Update using path condition and rename basename without explicit path
    DmlPlan update_plan;
    update_plan.action = DmlAction::Update;
    update_plan.entity_type = "DataObject";
    update_plan.properties["name"] = "sub_file_renamed.txt";
    update_plan.properties["s"] = "8192";
    update_plan.conditions.push_back(DmlCondition{"path", 0, "/tempZone/home/rods/sub_file.txt"});

    lite3cpp::Buffer update_res;
    auto update_err = facade_.execute_dml(update_plan, update_res);
    ASSERT_TRUE(update_err.ok()) << update_err.result();
    EXPECT_EQ(update_res.get_i64(0, "rows_affected"), 1);

    // Remove using recomputed path condition
    DmlPlan remove_plan;
    remove_plan.action = DmlAction::Remove;
    remove_plan.entity_type = "DataObject";
    remove_plan.conditions.push_back(DmlCondition{"path", 0, "/tempZone/home/rods/sub_file_renamed.txt"});

    lite3cpp::Buffer remove_res;
    auto remove_err = facade_.execute_dml(remove_plan, remove_res);
    ASSERT_TRUE(remove_err.ok()) << remove_err.result();
    EXPECT_EQ(remove_res.get_i64(0, "rows_affected"), 1);

    // Repeated remove on newly removed path yields 0
    lite3cpp::Buffer repeat_res;
    auto repeat_err = facade_.execute_dml(remove_plan, repeat_res);
    ASSERT_TRUE(repeat_err.ok()) << repeat_err.result();
    EXPECT_EQ(repeat_res.get_i64(0, "rows_affected"), 0);
}

TEST_F(CatalogFacadeDmlTest, ExecuteDmlUnknownEntityType) {
    DmlPlan plan;
    plan.action = DmlAction::Insert;
    plan.entity_type = "InvalidUnknownType";
    plan.properties["n"] = "file.dat";

    lite3cpp::Buffer res;
    auto err = facade_.execute_dml(plan, res);
    EXPECT_FALSE(err.ok());
}

TEST_F(CatalogFacadeDmlTest, ModifyDataObjectAndReplicaUnified) {
    // 1. Register a collection
    collection coll;
    coll.id = 500;
    coll.name = "/tempZone/home/rods/modcoll";
    coll.owner_name = "rods";
    coll.owner_zone = "tempZone";
    coll_id_t coll_id = 0;
    ASSERT_TRUE(facade_.register_collection(coll, coll_id).ok());

    // 2. Register a Data Object with initial replica
    data_object obj;
    obj.id = 5001;
    obj.coll_id = coll_id;
    obj.name = "test_mod.dat";
    obj.owner_name = "rods";
    obj.owner_zone = "tempZone";
    obj.size = 100;
    obj.create_ts = "0100000000";
    obj.modify_ts = "0100000000";

    replica repl;
    repl.data_id = 5001;
    repl.replica_number = 0;
    repl.resc_hier = "demoResc";
    repl.physical_path = "/var/lib/irods/Vault/test_mod.dat";
    repl.status = "0";
    repl.size = 100;
    repl.modify_ts = "0100000000";

    data_id_t registered_data_id = 0;
    ASSERT_TRUE(facade_.register_data_object(obj, registered_data_id, &repl).ok());
    EXPECT_EQ(registered_data_id, 5001);

    // 3. Perform unified modification:
    // Update dataSize to 4096, dataModify to "0170000000", chksum to "sha2:xyz",
    // filePath to "/new/path/test_mod.dat", dataComments to "updated comment",
    // and all_repl_status = true (which should mark good replica status "1")
    std::vector<std::pair<std::string, std::string>> updates = {
        {"dataSize", "4096"},
        {"dataModify", "0170000000"},
        {"chksum", "sha2:xyz"},
        {"filePath", "/new/path/test_mod.dat"},
        {"dataComments", "updated comment"}
    };

    auto mod_err = facade_.modify_data_object_and_replica(
        5001,
        0,
        "demoResc",
        updates,
        true,   // all_repl_status
        false   // all_replicas
    );
    ASSERT_TRUE(mod_err.ok()) << mod_err.result();

    // 4. Verify updates on mock server
    uint16_t cluster_id = facade_.get_cluster_id();
    snowflake_id_t sid = facade_.make_id(EntityType::DataObject, 5001);
    std::string local_uuid = "5001:0";
    snowflake_id_t rid = SnowflakeID::create(cluster_id, local_uuid);

    ASSERT_TRUE(mock_server_->has_node(sid));
    ASSERT_TRUE(mock_server_->has_node(rid));

    // DataObject node checks:
    auto d_node = mock_server_->get_node(sid);
    EXPECT_EQ(d_node.get_attribute<int64_t>("s"), 4096);
    EXPECT_EQ(d_node.get_attribute<std::string>("mt"), "0170000000");
    EXPECT_EQ(d_node.get_attribute<std::string>("c"), "updated comment");

    // Replica node checks:
    auto r_node = mock_server_->get_node(rid);
    EXPECT_EQ(r_node.get_attribute<int64_t>("s"), 4096);
    EXPECT_EQ(r_node.get_attribute<std::string>("mt"), "0170000000");
    EXPECT_EQ(r_node.get_attribute<std::string>("cs"), "sha2:xyz");
    EXPECT_EQ(r_node.get_attribute<std::string>("p"), "/new/path/test_mod.dat");
    EXPECT_EQ(r_node.get_attribute<std::string>("st"), "1");

    // 5. Test non-existent data object
    auto non_exist_err = facade_.modify_data_object_and_replica(
        99999,
        0,
        "demoResc",
        updates,
        false
    );
    EXPECT_FALSE(non_exist_err.ok());
    EXPECT_EQ(non_exist_err.code(), CAT_UNKNOWN_FILE);
}

TEST_F(CatalogFacadeDmlTest, ModifyDataObjectAndReplicaMultiReplica) {
    // 1. Register a collection
    collection coll;
    coll.id = 600;
    coll.name = "/tempZone/home/rods/multicoll";
    coll.owner_name = "rods";
    coll.owner_zone = "tempZone";
    coll_id_t coll_id = 0;
    ASSERT_TRUE(facade_.register_collection(coll, coll_id).ok());

    // 2. Register resources demoResc and otherResc
    resource r1;
    r1.name = "demoResc";
    r1.type = "unixfilesystem";
    resc_id_t r1_id = 40001;
    facade_.register_resource(r1, r1_id);

    resource r2;
    r2.name = "otherResc";
    r2.type = "unixfilesystem";
    resc_id_t r2_id = 40002;
    facade_.register_resource(r2, r2_id);

    // 3. Register Data Object with initial replica 0 on demoResc
    data_object obj;
    obj.id = 6001;
    obj.coll_id = coll_id;
    obj.name = "multi_mod.dat";
    obj.owner_name = "rods";
    obj.owner_zone = "tempZone";
    obj.size = 100;
    obj.create_ts = "0100000000";
    obj.modify_ts = "0100000000";

    replica repl0;
    repl0.data_id = 6001;
    repl0.replica_number = 0;
    repl0.resc_hier = "demoResc";
    repl0.physical_path = "/var/lib/irods/Vault/multi_mod_0.dat";
    repl0.status = "1";
    repl0.size = 100;
    repl0.modify_ts = "0100000000";

    data_id_t registered_data_id = 0;
    ASSERT_TRUE(facade_.register_data_object(obj, registered_data_id, &repl0).ok());
    EXPECT_EQ(registered_data_id, 6001);

    // 4. Register replica 1 on otherResc
    replica repl1;
    repl1.data_id = 6001;
    repl1.replica_number = 1;
    repl1.resc_hier = "otherResc";
    repl1.physical_path = "/var/lib/irods/Vault/multi_mod_1.dat";
    repl1.status = "1";
    repl1.size = 100;
    repl1.modify_ts = "0100000000";
    ASSERT_TRUE(facade_.register_replica(repl1).ok());

    uint16_t cluster_id = facade_.get_cluster_id();
    snowflake_id_t sid = facade_.make_id(EntityType::DataObject, 6001);
    snowflake_id_t rid0 = SnowflakeID::create(cluster_id, "6001:0");
    snowflake_id_t rid1 = SnowflakeID::create(cluster_id, "6001:1");

    ASSERT_TRUE(mock_server_->has_node(sid));
    ASSERT_TRUE(mock_server_->has_node(rid0));
    ASSERT_TRUE(mock_server_->has_node(rid1));

    // 5. Update only replica 1 with all_repl_status = true, all_replicas = false
    std::vector<std::pair<std::string, std::string>> updates1 = {
        {"dataSize", "8192"},
        {"dataModify", "0175000000"},
        {"chksum", "sha2:repl1chksum"},
        {"filePath", "/new/path/multi_mod_1.dat"}
    };

    auto mod_err1 = facade_.modify_data_object_and_replica(
        6001,
        0, // default repl_num should be overridden by resc_hier matching
        "otherResc",
        updates1,
        /*all_repl_status=*/true,
        /*all_replicas=*/false
    );
    ASSERT_TRUE(mod_err1.ok()) << mod_err1.result();

    // Verify Data Object updated
    auto d_node1 = mock_server_->get_node(sid);
    EXPECT_EQ(d_node1.get_attribute<int64_t>("s"), 8192);
    EXPECT_EQ(d_node1.get_attribute<std::string>("mt"), "0175000000");

    // Verify Replica 1 updated and marked GOOD ("1")
    auto r1_node = mock_server_->get_node(rid1);
    EXPECT_EQ(r1_node.get_attribute<int64_t>("s"), 8192);
    EXPECT_EQ(r1_node.get_attribute<std::string>("mt"), "0175000000");
    EXPECT_EQ(r1_node.get_attribute<std::string>("cs"), "sha2:repl1chksum");
    EXPECT_EQ(r1_node.get_attribute<std::string>("p"), "/new/path/multi_mod_1.dat");
    EXPECT_EQ(r1_node.get_attribute<std::string>("st"), "1");

    // Verify Replica 0 marked STALE ("0") and original size preserved
    auto r0_node = mock_server_->get_node(rid0);
    EXPECT_EQ(r0_node.get_attribute<std::string>("st"), "0");
    EXPECT_EQ(r0_node.get_attribute<int64_t>("s"), 100);

    // 6. Test with all_replicas == true and verify both replicas receive update
    std::vector<std::pair<std::string, std::string>> updates_all = {
        {"dataSize", "16384"},
        {"dataModify", "0180000000"}
    };

    auto mod_err2 = facade_.modify_data_object_and_replica(
        6001,
        0,
        "",
        updates_all,
        /*all_repl_status=*/false,
        /*all_replicas=*/true
    );
    ASSERT_TRUE(mod_err2.ok()) << mod_err2.result();

    auto r0_node_after = mock_server_->get_node(rid0);
    EXPECT_EQ(r0_node_after.get_attribute<int64_t>("s"), 16384);
    EXPECT_EQ(r0_node_after.get_attribute<std::string>("mt"), "0180000000");

    auto r1_node_after = mock_server_->get_node(rid1);
    EXPECT_EQ(r1_node_after.get_attribute<int64_t>("s"), 16384);
    EXPECT_EQ(r1_node_after.get_attribute<std::string>("mt"), "0180000000");
}
