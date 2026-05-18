#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"

using namespace irods::catalog;

TEST(MultiHopQueryTest, CollectionToResource) {
    Config cfg;
    cfg.db_path = "multihop.l3kvg";
    cfg.node_id = 1;
    system("rm -rf multihop.l3kvg"); // Clean start
    
    CatalogFacade catalog;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Setup Hierarchy: Collection -> DataObject -> Resource
    // Register Collection
    coll_id_t coll_id;
    collection c{10, 0, "/tempZone/home/alice", "alice", "tempZone", "", ""};
    ASSERT_TRUE(catalog.register_collection(c, coll_id).ok());

    // Register DataObject in that Collection
    data_object obj;
    obj.id = 1001;
    obj.coll_id = 10;
    obj.name = "report.pdf";
    data_id_t out_obj_id;
    ASSERT_TRUE(catalog.register_data_object(obj, out_obj_id).ok());

    // Register a Resource
    resource resc;
    resc.id = 301;
    resc.name = "demoResc";
    resc.type = "unixfilesystem";
    resc.location = "localhost";
    resc.vault_path = "/tmp";
    resc_id_t out_resc_id;
    ASSERT_TRUE(catalog.register_resource(resc, out_resc_id).ok());

    // Register DataObject -> Resource relationship (using replica)
    replica repl;
    repl.data_id = 1001;
    repl.resource_id = 301;
    repl.physical_path = "/vault/report.pdf";
    repl.replica_number = 0;
    repl.status = "good";
    ASSERT_TRUE(catalog.register_replica(repl).ok());

    // Synchronize
    catalog.get_engine()->flush();
    catalog.get_engine()->get_store()->wait_all_shards();

    // 2. Perform Multi-Hop GenQuery using Secondary Index:
    // SELECT RESC_NAME WHERE COLL_NAME = '/tempZone/home/alice'
    
    namespace gq = irods::experimental::genquery2;
    gq::select ast;
    ast.projections.push_back(gq::column{"RESC_NAME"});
    ast.conditions.push_back(gq::condition{gq::column{"COLL_NAME"}, gq::condition_equal{"/tempZone/home/alice"}});

    ResultSet results;
    ASSERT_TRUE(catalog.execute_query(ast, results).ok());
    
    // 3. Validate
    ASSERT_EQ(results.row_count(), 1);
    EXPECT_EQ(results.get_field(0, "Resource.name"), "demoResc");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
