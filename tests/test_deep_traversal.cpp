#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"

using namespace irods::catalog;

TEST(DeepTraversalTest, UserToDataObject) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "deep.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Setup Data
    // Alice -> Access(read) -> report.txt
    user alice;
    alice.id = 501;
    alice.name = "alice";
    alice.zone = "tempZone";
    user_id_t u_out;
    catalog.register_user(alice, u_out);

    data_object report;
    report.id = 601;
    report.name = "report.txt";
    report.coll_id = 10;
    data_id_t d_out;
    catalog.register_data_object(report, d_out);

    catalog.set_access(501, 601, "read");

    catalog.get_engine()->get_store()->wait_all_shards();

    namespace gq = irods::experimental::genquery2;

    // 2. Query: SELECT DATA_NAME WHERE USER_NAME = 'alice' AND DATA_ACCESS_NAME = 'read'
    {
        gq::select ast;
        ast.projections.push_back(gq::column{"DATA_NAME"});
        ast.conditions.push_back(gq::condition{gq::column{"USER_NAME"}, gq::condition_equal{"alice"}});
        ast.conditions.push_back(gq::condition{gq::column{"DATA_ACCESS_NAME"}, gq::condition_equal{"read"}});

        ResultSet results;
        ASSERT_TRUE(catalog.execute_query(ast, results).ok());
        
        // Path should be User(alice) -> Access(read) -> DataObject(report.txt)
        ASSERT_EQ(results.row_count(), 1);
        EXPECT_EQ(results.get_field(0, 0), "report.txt");
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
