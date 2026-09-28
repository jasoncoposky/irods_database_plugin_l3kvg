#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "mock_l3kvg.hpp"
#include <nlohmann/json.hpp>
#include <random>

using namespace irods::catalog;
using namespace irods::catalog::compiler;
using namespace irods::catalog::test;

class CatalogFacadeDmlTest : public ::testing::Test {
protected:
    void SetUp() override {
        int port = 5600 + (rand() % 2000);
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

    nlohmann::json insert_res;
    auto insert_err = facade_.execute_dml(insert_plan, insert_res);
    ASSERT_TRUE(insert_err.ok()) << insert_err.result();
    EXPECT_EQ(insert_res["rows_affected"], 1);
    EXPECT_EQ(insert_res["status"], "SUCCESS");

    // 2. Test Update: plan with action Update, entity_type "DataObject", properties "s" -> "2048", condition on "n" == "test_file.dat".
    DmlPlan update_plan;
    update_plan.action = DmlAction::Update;
    update_plan.entity_type = "DataObject";
    update_plan.properties["s"] = "2048";
    update_plan.conditions.push_back(DmlCondition{"n", 0, "test_file.dat"});

    nlohmann::json update_res;
    auto update_err = facade_.execute_dml(update_plan, update_res);
    ASSERT_TRUE(update_err.ok()) << update_err.result();
    EXPECT_EQ(update_res["rows_affected"], 1);
    EXPECT_EQ(update_res["status"], "SUCCESS");

    // 3. Test Remove: plan with action Remove, entity_type "DataObject", condition on "n" == "test_file.dat".
    DmlPlan remove_plan;
    remove_plan.action = DmlAction::Remove;
    remove_plan.entity_type = "DataObject";
    remove_plan.conditions.push_back(DmlCondition{"n", 0, "test_file.dat"});

    nlohmann::json remove_res;
    auto remove_err = facade_.execute_dml(remove_plan, remove_res);
    ASSERT_TRUE(remove_err.ok()) << remove_err.result();
    EXPECT_EQ(remove_res["rows_affected"], 1);
    EXPECT_EQ(remove_res["status"], "SUCCESS");
}

TEST_F(CatalogFacadeDmlTest, ExecuteDmlInsertWithParentCollAndPath) {
    // Bootstrap root collection first
    coll_id_t root_id;
    collection c;
    c.id = 100;
    c.name = "/tempZone/home/rods";
    c.owner_name = "rods";
    c.owner_zone = "tempZone";
    ASSERT_TRUE(facade_.register_collection(c, root_id).ok());

    // Insert DataObject with parent_coll
    DmlPlan insert_plan;
    insert_plan.action = DmlAction::Insert;
    insert_plan.entity_type = "DataObject";
    insert_plan.properties["n"] = "sub_file.txt";
    insert_plan.properties["parent_coll"] = "/tempZone/home/rods";
    insert_plan.properties["s"] = "4096";

    nlohmann::json insert_res;
    auto insert_err = facade_.execute_dml(insert_plan, insert_res);
    ASSERT_TRUE(insert_err.ok()) << insert_err.result();
    EXPECT_EQ(insert_res["rows_affected"], 1);

    // Update using path condition
    DmlPlan update_plan;
    update_plan.action = DmlAction::Update;
    update_plan.entity_type = "DataObject";
    update_plan.properties["s"] = "8192";
    update_plan.conditions.push_back(DmlCondition{"path", 0, "/tempZone/home/rods/sub_file.txt"});

    nlohmann::json update_res;
    auto update_err = facade_.execute_dml(update_plan, update_res);
    ASSERT_TRUE(update_err.ok()) << update_err.result();
    EXPECT_EQ(update_res["rows_affected"], 1);

    // Remove using path condition
    DmlPlan remove_plan;
    remove_plan.action = DmlAction::Remove;
    remove_plan.entity_type = "DataObject";
    remove_plan.conditions.push_back(DmlCondition{"path", 0, "/tempZone/home/rods/sub_file.txt"});

    nlohmann::json remove_res;
    auto remove_err = facade_.execute_dml(remove_plan, remove_res);
    ASSERT_TRUE(remove_err.ok()) << remove_err.result();
    EXPECT_EQ(remove_res["rows_affected"], 1);
}
