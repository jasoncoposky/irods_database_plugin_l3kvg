#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
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
